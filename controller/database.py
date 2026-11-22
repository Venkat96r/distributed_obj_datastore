import sqlite3
import json
import time
import copy
import threading
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime, timezone
import logging

from common.lru import LRUCache
from common.models import META_CACHE_MAX_ENTRIES, META_CACHE_TTL

logger = logging.getLogger("controller.database")


class Database:
    def __init__(
        self,
        db_path: str,
        cache_max_entries: int = META_CACHE_MAX_ENTRIES,
        cache_ttl: float = META_CACHE_TTL
    ):
        self.db_path = Path(db_path).resolve()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self.meta_cache = LRUCache(max_entries=cache_max_entries, ttl_seconds=cache_ttl)
        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), timeout=20.0, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON;")
        conn.execute("PRAGMA journal_mode = WAL;")
        return conn

    def _init_db(self):
        with self._lock, self._get_connection() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS objects (
                    object_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    size INTEGER NOT NULL,
                    content_type TEXT NOT NULL,
                    checksum TEXT NOT NULL,
                    num_chunks INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    modified_at TEXT NOT NULL,
                    custom_metadata TEXT DEFAULT '{}',
                    state TEXT NOT NULL DEFAULT 'COMMITTED' CHECK(state IN ('PENDING', 'COMMITTED')),
                    idempotency_key TEXT
                );

                CREATE TABLE IF NOT EXISTS chunks (
                    chunk_id TEXT PRIMARY KEY,
                    object_id TEXT NOT NULL,
                    idx INTEGER NOT NULL,
                    size INTEGER NOT NULL,
                    checksum TEXT NOT NULL,
                    FOREIGN KEY (object_id) REFERENCES objects(object_id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS workers (
                    worker_id TEXT PRIMARY KEY,
                    url TEXT NOT NULL,
                    last_heartbeat REAL NOT NULL,
                    status TEXT NOT NULL CHECK(status IN ('alive', 'dead'))
                );

                CREATE TABLE IF NOT EXISTS replicas (
                    chunk_id TEXT NOT NULL,
                    worker_id TEXT NOT NULL,
                    PRIMARY KEY (chunk_id, worker_id),
                    FOREIGN KEY (chunk_id) REFERENCES chunks(chunk_id) ON DELETE CASCADE,
                    FOREIGN KEY (worker_id) REFERENCES workers(worker_id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS uploads (
                    object_id TEXT PRIMARY KEY,
                    state TEXT NOT NULL CHECK(state IN ('STREAMING', 'STAGED', 'COMMITTED', 'FAILED')),
                    staging_path TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    last_error TEXT,
                    FOREIGN KEY (object_id) REFERENCES objects(object_id) ON DELETE CASCADE
                );

                CREATE INDEX IF NOT EXISTS idx_chunks_object ON chunks(object_id);
                CREATE INDEX IF NOT EXISTS idx_chunks_idx ON chunks(object_id, idx);
                CREATE INDEX IF NOT EXISTS idx_replicas_chunk ON replicas(chunk_id);
                CREATE INDEX IF NOT EXISTS idx_replicas_worker ON replicas(worker_id);
                CREATE INDEX IF NOT EXISTS idx_uploads_state ON uploads(state);
            """)

            # Backward-compatible schema migration for existing persistent volumes
            cols = {row["name"] for row in conn.execute("PRAGMA table_info(objects)").fetchall()}
            if "state" not in cols:
                conn.execute("ALTER TABLE objects ADD COLUMN state TEXT NOT NULL DEFAULT 'COMMITTED'")
                logger.info("Migrated objects table: added 'state' column")
            if "idempotency_key" not in cols:
                conn.execute("ALTER TABLE objects ADD COLUMN idempotency_key TEXT")
                logger.info("Migrated objects table: added 'idempotency_key' column")

            conn.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_objects_idempotency
                ON objects(idempotency_key)
                WHERE idempotency_key IS NOT NULL
            """)
            conn.commit()
            logger.info(f"Database initialized at {self.db_path}")

    def get_cache_stats(self) -> Dict[str, Any]:
        return self.meta_cache.stats()

    def _object_id_from_chunk(self, chunk_id: str) -> str:
        return chunk_id.rsplit("_", 1)[0] if "_" in chunk_id else chunk_id

    # --- Worker Management ---

    def register_worker(self, worker_id: str, url: str) -> None:
        with self._lock, self._get_connection() as conn:
            now = time.time()
            conn.execute("""
                INSERT INTO workers (worker_id, url, last_heartbeat, status)
                VALUES (?, ?, ?, 'alive')
                ON CONFLICT(worker_id) DO UPDATE SET
                    url = excluded.url,
                    last_heartbeat = excluded.last_heartbeat,
                    status = 'alive'
            """, (worker_id, url, now))
            conn.commit()
            self.meta_cache.invalidate_all()
            logger.info(f"Worker registered: {worker_id} at {url}")

    def record_heartbeat(self, worker_id: str) -> bool:
        with self._lock, self._get_connection() as conn:
            now = time.time()
            prev = conn.execute(
                "SELECT status FROM workers WHERE worker_id = ?", (worker_id,)
            ).fetchone()
            if not prev:
                return False
            was_dead = prev["status"] != "alive"
            cur = conn.execute("""
                UPDATE workers
                SET last_heartbeat = ?, status = 'alive'
                WHERE worker_id = ?
            """, (now, worker_id))
            conn.commit()
            if was_dead:
                self.meta_cache.invalidate_all()
            return cur.rowcount > 0

    def mark_worker_status(self, worker_id: str, status: str) -> None:
        with self._lock, self._get_connection() as conn:
            conn.execute("UPDATE workers SET status = ? WHERE worker_id = ?", (status, worker_id))
            conn.commit()
            self.meta_cache.invalidate_all()

    def get_all_workers(self) -> List[Dict[str, Any]]:
        with self._lock, self._get_connection() as conn:
            rows = conn.execute("""
                SELECT w.worker_id, w.url, w.last_heartbeat, w.status,
                       COUNT(r.chunk_id) as chunk_count
                FROM workers w
                LEFT JOIN replicas r ON w.worker_id = r.worker_id
                GROUP BY w.worker_id
                ORDER BY w.worker_id ASC
            """).fetchall()
            now = time.time()
            return [
                {
                    "worker_id": row["worker_id"],
                    "url": row["url"],
                    "last_heartbeat": row["last_heartbeat"],
                    "status": row["status"],
                    "chunk_count": row["chunk_count"],
                    "seconds_since_heartbeat": round(max(0.0, now - row["last_heartbeat"]), 1)
                }
                for row in rows
            ]

    def get_alive_workers(self) -> List[Dict[str, Any]]:
        workers = self.get_all_workers()
        return [w for w in workers if w["status"] == "alive"]

    def get_worker(self, worker_id: str) -> Optional[Dict[str, Any]]:
        with self._lock, self._get_connection() as conn:
            row = conn.execute("""
                SELECT w.worker_id, w.url, w.last_heartbeat, w.status,
                       COUNT(r.chunk_id) as chunk_count
                FROM workers w
                LEFT JOIN replicas r ON w.worker_id = r.worker_id
                WHERE w.worker_id = ?
                GROUP BY w.worker_id
            """, (worker_id,)).fetchone()
            if not row:
                return None
            now = time.time()
            return {
                "worker_id": row["worker_id"],
                "url": row["url"],
                "last_heartbeat": row["last_heartbeat"],
                "status": row["status"],
                "chunk_count": row["chunk_count"],
                "seconds_since_heartbeat": round(max(0.0, now - row["last_heartbeat"]), 1)
            }

    # --- Staged Uploads & Idempotency ---

    def get_object_by_idempotency_key(self, idempotency_key: str) -> Optional[Dict[str, Any]]:
        """
        Looks up an object (PENDING or COMMITTED) and its upload record by Idempotency-Key.
        """
        with self._lock, self._get_connection() as conn:
            row = conn.execute("""
                SELECT o.object_id, o.state as object_state,
                       u.state as upload_state, u.staging_path, u.created_at as upload_created_at
                FROM objects o
                LEFT JOIN uploads u ON o.object_id = u.object_id
                WHERE o.idempotency_key = ?
            """, (idempotency_key,)).fetchone()
            if not row:
                return None
            return {
                "object_id": row["object_id"],
                "object_state": row["object_state"],
                "upload_state": row["upload_state"],
                "staging_path": row["staging_path"],
                "upload_created_at": row["upload_created_at"],
            }

    def begin_staged_upload(
        self,
        object_id: str,
        staging_path: str,
        idempotency_key: Optional[str] = None
    ) -> None:
        """
        DB-first step: inserts a PENDING object placeholder and STREAMING upload row
        in a single atomic transaction. Raises sqlite3.IntegrityError on duplicate Idempotency-Key.
        """
        now_iso = datetime.now(timezone.utc).isoformat()
        now_ts = time.time()
        with self._lock, self._get_connection() as conn:
            try:
                conn.execute("""
                    INSERT INTO objects (
                        object_id, name, size, content_type, checksum,
                        num_chunks, created_at, modified_at, custom_metadata,
                        state, idempotency_key
                    ) VALUES (?, '', 0, 'application/octet-stream', '', 0, ?, ?, '{}', 'PENDING', ?)
                """, (object_id, now_iso, now_iso, idempotency_key))

                conn.execute("""
                    INSERT INTO uploads (object_id, state, staging_path, created_at, updated_at, last_error)
                    VALUES (?, 'STREAMING', ?, ?, ?, NULL)
                """, (object_id, staging_path, now_ts, now_ts))
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    def mark_upload_staged(
        self,
        object_id: str,
        name: str,
        size: int,
        content_type: str,
        checksum: str,
        num_chunks: int,
        custom_metadata: Dict[str, Any],
        staging_path: str
    ) -> None:
        """
        The durability commit point of an upload: atomically records the real object metadata
        and transitions uploads.state from STREAMING -> STAGED.
        """
        now_iso = datetime.now(timezone.utc).isoformat()
        now_ts = time.time()
        with self._lock, self._get_connection() as conn:
            try:
                conn.execute("""
                    UPDATE objects
                    SET name = ?, size = ?, content_type = ?, checksum = ?,
                        num_chunks = ?, modified_at = ?, custom_metadata = ?
                    WHERE object_id = ?
                """, (
                    name, size, content_type, checksum,
                    num_chunks, now_iso, json.dumps(custom_metadata), object_id
                ))

                conn.execute("""
                    UPDATE uploads
                    SET state = 'STAGED', staging_path = ?, updated_at = ?, last_error = NULL
                    WHERE object_id = ?
                """, (staging_path, now_ts, object_id))
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    def ensure_chunk_record(
        self,
        chunk_id: str,
        object_id: str,
        idx: int,
        size: int,
        checksum: str
    ) -> None:
        """
        Idempotently records a chunk row before pushing to workers so reconcile_worker
        never treats an in-flight chunk as an orphan.
        """
        with self._lock, self._get_connection() as conn:
            conn.execute("""
                INSERT OR IGNORE INTO chunks (chunk_id, object_id, idx, size, checksum)
                VALUES (?, ?, ?, ?, ?)
            """, (chunk_id, object_id, idx, size, checksum))
            conn.commit()

    def commit_staged_upload(self, object_id: str) -> None:
        """
        Atomically transitions a staged upload to COMMITTED so it becomes visible to clients.
        """
        now_iso = datetime.now(timezone.utc).isoformat()
        now_ts = time.time()
        with self._lock, self._get_connection() as conn:
            try:
                conn.execute("""
                    UPDATE objects SET state = 'COMMITTED', modified_at = ?
                    WHERE object_id = ?
                """, (now_iso, object_id))
                conn.execute("""
                    UPDATE uploads SET state = 'COMMITTED', updated_at = ?, last_error = NULL
                    WHERE object_id = ?
                """, (now_ts, object_id))
                conn.commit()
                self.meta_cache.invalidate(object_id)
                logger.info(f"Committed staged object {object_id}.")
            except Exception:
                conn.rollback()
                raise

    def update_upload_error(self, object_id: str, error_msg: str) -> None:
        with self._lock, self._get_connection() as conn:
            conn.execute("""
                UPDATE uploads SET last_error = ?, updated_at = ?
                WHERE object_id = ?
            """, (error_msg[:500], time.time(), object_id))
            conn.commit()

    def abort_upload(self, object_id: str) -> Optional[List[Dict[str, Any]]]:
        """
        Deletes a PENDING object (cascading to uploads, chunks, and replicas).
        Returns any chunk replica records that may need cleanup on workers.
        """
        with self._lock, self._get_connection() as conn:
            obj = self.get_object(object_id, include_pending=True)
            if not obj or obj.get("state") == "COMMITTED":
                return None
            chunks = obj["chunks"]
            conn.execute("DELETE FROM objects WHERE object_id = ? AND state = 'PENDING'", (object_id,))
            conn.commit()
            self.meta_cache.invalidate(object_id)
            return chunks

    def get_uploads_by_state(self, state: str) -> List[Dict[str, Any]]:
        with self._lock, self._get_connection() as conn:
            rows = conn.execute("""
                SELECT u.object_id, u.state, u.staging_path, u.created_at, u.updated_at, u.last_error,
                       o.name, o.size, o.num_chunks, o.idempotency_key
                FROM uploads u
                JOIN objects o ON u.object_id = o.object_id
                WHERE u.state = ?
                ORDER BY u.created_at ASC
            """, (state,)).fetchall()
            return [dict(r) for r in rows]

    def get_pending_uploads_count(self) -> int:
        with self._lock, self._get_connection() as conn:
            row = conn.execute(
                "SELECT COUNT(*) as c FROM uploads WHERE state IN ('STREAMING', 'STAGED')"
            ).fetchone()
            return row["c"] if row else 0

    # --- Object & Chunk Storage ---

    def create_object_transaction(
        self,
        object_id: str,
        name: str,
        size: int,
        content_type: str,
        checksum: str,
        num_chunks: int,
        custom_metadata: Dict[str, Any],
        chunks_data: List[Dict[str, Any]],
        idempotency_key: Optional[str] = None
    ) -> None:
        """
        Atomically records a COMMITTED object, all chunks, and replicas in a single transaction.
        """
        now = datetime.now(timezone.utc).isoformat()
        custom_meta_str = json.dumps(custom_metadata)

        with self._lock, self._get_connection() as conn:
            try:
                conn.execute("""
                    INSERT INTO objects (
                        object_id, name, size, content_type, checksum,
                        num_chunks, created_at, modified_at, custom_metadata,
                        state, idempotency_key
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'COMMITTED', ?)
                """, (
                    object_id, name, size, content_type, checksum,
                    num_chunks, now, now, custom_meta_str, idempotency_key
                ))

                for c in chunks_data:
                    conn.execute("""
                        INSERT INTO chunks (chunk_id, object_id, idx, size, checksum)
                        VALUES (?, ?, ?, ?, ?)
                    """, (c["chunk_id"], object_id, c["idx"], c["size"], c["checksum"]))

                    for worker_id in c["workers"]:
                        conn.execute("""
                            INSERT INTO replicas (chunk_id, worker_id)
                            VALUES (?, ?)
                        """, (c["chunk_id"], worker_id))

                conn.commit()
                self.meta_cache.invalidate(object_id)
                logger.info(f"Committed object {object_id} with {num_chunks} chunks.")
            except Exception as e:
                conn.rollback()
                logger.error(f"Failed to record object {object_id}: {e}")
                raise

    def get_object(self, object_id: str, include_pending: bool = False) -> Optional[Dict[str, Any]]:
        if not include_pending:
            cached = self.meta_cache.get(object_id)
            if cached is not None:
                return copy.deepcopy(cached)
            token = self.meta_cache.begin_load(object_id)
        else:
            token = None

        with self._lock, self._get_connection() as conn:
            if include_pending:
                obj_row = conn.execute(
                    "SELECT * FROM objects WHERE object_id = ?", (object_id,)
                ).fetchone()
            else:
                obj_row = conn.execute(
                    "SELECT * FROM objects WHERE object_id = ? AND state = 'COMMITTED'", (object_id,)
                ).fetchone()

            if not obj_row:
                return None

            chunk_rows = conn.execute("""
                SELECT c.chunk_id, c.idx, c.size, c.checksum,
                       GROUP_CONCAT(r.worker_id) as worker_ids
                FROM chunks c
                LEFT JOIN replicas r ON c.chunk_id = r.chunk_id
                WHERE c.object_id = ?
                GROUP BY c.chunk_id
                ORDER BY c.idx ASC
            """, (object_id,)).fetchall()

            chunks = []
            for cr in chunk_rows:
                workers = cr["worker_ids"].split(",") if cr["worker_ids"] else []
                chunks.append({
                    "chunk_id": cr["chunk_id"],
                    "index": cr["idx"],
                    "size": cr["size"],
                    "checksum": cr["checksum"],
                    "workers": workers
                })

            try:
                custom_meta = json.loads(obj_row["custom_metadata"])
            except Exception:
                custom_meta = {}

            result = {
                "object_id": obj_row["object_id"],
                "name": obj_row["name"],
                "size": obj_row["size"],
                "content_type": obj_row["content_type"],
                "checksum": obj_row["checksum"],
                "num_chunks": obj_row["num_chunks"],
                "created_at": obj_row["created_at"],
                "modified_at": obj_row["modified_at"],
                "custom_metadata": custom_meta,
                "chunks": chunks,
            }
            if include_pending:
                result["state"] = obj_row["state"]
                result["idempotency_key"] = obj_row["idempotency_key"]

        if not include_pending and token is not None:
            self.meta_cache.put_if_fresh(object_id, copy.deepcopy(result), token)

        return result

    def list_objects(self, limit: int = 50, offset: int = 0) -> Tuple[List[Dict[str, Any]], int]:
        with self._lock, self._get_connection() as conn:
            total = conn.execute(
                "SELECT COUNT(*) as count FROM objects WHERE state = 'COMMITTED'"
            ).fetchone()["count"]
            rows = conn.execute("""
                SELECT object_id, name, size, content_type, checksum, num_chunks,
                       created_at, modified_at, custom_metadata
                FROM objects
                WHERE state = 'COMMITTED'
                ORDER BY created_at DESC
                LIMIT ? OFFSET ?
            """, (limit, offset)).fetchall()

            objects = []
            for r in rows:
                try:
                    custom_meta = json.loads(r["custom_metadata"])
                except Exception:
                    custom_meta = {}
                objects.append({
                    "object_id": r["object_id"],
                    "name": r["name"],
                    "size": r["size"],
                    "content_type": r["content_type"],
                    "checksum": r["checksum"],
                    "num_chunks": r["num_chunks"],
                    "created_at": r["created_at"],
                    "modified_at": r["modified_at"],
                    "custom_metadata": custom_meta
                })
            return objects, total

    def update_object_metadata(self, object_id: str, new_custom_metadata: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        with self._lock, self._get_connection() as conn:
            obj = self.get_object(object_id)
            if not obj:
                return None

            merged_meta = obj["custom_metadata"].copy()
            merged_meta.update(new_custom_metadata)
            now = datetime.now(timezone.utc).isoformat()

            conn.execute("""
                UPDATE objects
                SET custom_metadata = ?, modified_at = ?
                WHERE object_id = ? AND state = 'COMMITTED'
            """, (json.dumps(merged_meta), now, object_id))
            conn.commit()
            self.meta_cache.invalidate(object_id)

            return self.get_object(object_id)

    def delete_object(self, object_id: str) -> Optional[List[Dict[str, Any]]]:
        """
        Deletes a COMMITTED object and cascades to chunks & replicas.
        Returns list of chunks with worker assignments so controller can delete worker files.
        """
        with self._lock, self._get_connection() as conn:
            obj = self.get_object(object_id)
            if not obj:
                return None

            chunks_to_delete = obj["chunks"]
            conn.execute("DELETE FROM objects WHERE object_id = ? AND state = 'COMMITTED'", (object_id,))
            conn.commit()
            self.meta_cache.invalidate(object_id)
            logger.info(f"Deleted object {object_id} from database.")
            return chunks_to_delete

    # --- Replica & Repair Management ---

    def add_replica(self, chunk_id: str, worker_id: str) -> None:
        with self._lock, self._get_connection() as conn:
            conn.execute("""
                INSERT OR IGNORE INTO replicas (chunk_id, worker_id)
                VALUES (?, ?)
            """, (chunk_id, worker_id))
            conn.commit()
            self.meta_cache.invalidate(self._object_id_from_chunk(chunk_id))
            logger.info(f"Added replica for chunk {chunk_id} on worker {worker_id}")

    def remove_replica(self, chunk_id: str, worker_id: str) -> None:
        with self._lock, self._get_connection() as conn:
            conn.execute("""
                DELETE FROM replicas
                WHERE chunk_id = ? AND worker_id = ?
            """, (chunk_id, worker_id))
            conn.commit()
            self.meta_cache.invalidate(self._object_id_from_chunk(chunk_id))
            logger.warning(f"Removed replica for chunk {chunk_id} from worker {worker_id}")

    def get_chunk_info(self, chunk_id: str) -> Optional[Dict[str, Any]]:
        with self._lock, self._get_connection() as conn:
            row = conn.execute("""
                SELECT c.chunk_id, c.object_id, c.idx, c.size, c.checksum, o.state as object_state,
                       GROUP_CONCAT(r.worker_id) as worker_ids
                FROM chunks c
                JOIN objects o ON c.object_id = o.object_id
                LEFT JOIN replicas r ON c.chunk_id = r.chunk_id
                WHERE c.chunk_id = ?
                GROUP BY c.chunk_id
            """, (chunk_id,)).fetchone()
            if not row:
                return None
            workers = row["worker_ids"].split(",") if row["worker_ids"] else []
            return {
                "chunk_id": row["chunk_id"],
                "object_id": row["object_id"],
                "object_state": row["object_state"],
                "idx": row["idx"],
                "size": row["size"],
                "checksum": row["checksum"],
                "workers": workers
            }

    def get_chunk_alive_workers(self, chunk_id: str) -> List[str]:
        with self._lock, self._get_connection() as conn:
            rows = conn.execute("""
                SELECT r.worker_id
                FROM replicas r
                JOIN workers w ON r.worker_id = w.worker_id
                WHERE r.chunk_id = ? AND w.status = 'alive'
            """, (chunk_id,)).fetchall()
            return [r["worker_id"] for r in rows]

    def get_worker_chunks(self, worker_id: str) -> List[str]:
        """
        Retrieve all chunk IDs associated with a specific worker.
        """
        with self._lock, self._get_connection() as conn:
            rows = conn.execute(
                "SELECT chunk_id FROM replicas WHERE worker_id = ? ORDER BY chunk_id ASC",
                (worker_id,)
            ).fetchall()
            return [r["chunk_id"] for r in rows]

    def get_under_replicated_chunks(self, required_replicas: int = 2) -> List[Dict[str, Any]]:
        """
        Finds all chunks of COMMITTED objects that have fewer than `required_replicas`
        on currently ALIVE workers (using LEFT JOIN so 0-replica chunks are included).
        """
        with self._lock, self._get_connection() as conn:
            rows = conn.execute("""
                SELECT c.chunk_id, c.object_id, c.idx, c.size, c.checksum,
                       GROUP_CONCAT(CASE WHEN w.status = 'alive' THEN r.worker_id END) as alive_workers,
                       COUNT(CASE WHEN w.status = 'alive' THEN r.worker_id END) as alive_replica_count
                FROM chunks c
                JOIN objects o ON c.object_id = o.object_id
                LEFT JOIN replicas r ON c.chunk_id = r.chunk_id
                LEFT JOIN workers w ON r.worker_id = w.worker_id
                WHERE o.state = 'COMMITTED'
                GROUP BY c.chunk_id
                HAVING alive_replica_count < ?
            """, (required_replicas,)).fetchall()

            under_replicated = []
            for r in rows:
                alive = [w for w in (r["alive_workers"].split(",") if r["alive_workers"] else []) if w]
                under_replicated.append({
                    "chunk_id": r["chunk_id"],
                    "object_id": r["object_id"],
                    "idx": r["idx"],
                    "size": r["size"],
                    "checksum": r["checksum"],
                    "alive_workers": alive,
                    "alive_count": r["alive_replica_count"]
                })
            return under_replicated

    def get_over_replicated_chunks(self, required_replicas: int = 2) -> List[Dict[str, Any]]:
        """
        Finds all chunks of COMMITTED objects that have MORE than `required_replicas`
        on currently ALIVE workers (e.g., after a previously dead worker returns).
        """
        with self._lock, self._get_connection() as conn:
            rows = conn.execute("""
                SELECT c.chunk_id, c.object_id, c.idx,
                       GROUP_CONCAT(CASE WHEN w.status = 'alive' THEN r.worker_id END) as alive_workers,
                       COUNT(CASE WHEN w.status = 'alive' THEN r.worker_id END) as alive_replica_count
                FROM chunks c
                JOIN objects o ON c.object_id = o.object_id
                JOIN replicas r ON c.chunk_id = r.chunk_id
                JOIN workers w ON r.worker_id = w.worker_id
                WHERE o.state = 'COMMITTED'
                GROUP BY c.chunk_id
                HAVING alive_replica_count > ?
            """, (required_replicas,)).fetchall()

            over_replicated = []
            for r in rows:
                alive = [w for w in (r["alive_workers"].split(",") if r["alive_workers"] else []) if w]
                over_replicated.append({
                    "chunk_id": r["chunk_id"],
                    "object_id": r["object_id"],
                    "idx": r["idx"],
                    "alive_workers": alive,
                    "alive_count": r["alive_replica_count"]
                })
            return over_replicated

    def get_cluster_stats(self) -> Dict[str, Any]:
        with self._lock, self._get_connection() as conn:
            obj_count = conn.execute(
                "SELECT COUNT(*) as c FROM objects WHERE state = 'COMMITTED'"
            ).fetchone()["c"]
            chunk_count = conn.execute("""
                SELECT COUNT(*) as c FROM chunks c
                JOIN objects o ON c.object_id = o.object_id
                WHERE o.state = 'COMMITTED'
            """).fetchone()["c"]
            return {
                "total_objects": obj_count,
                "total_chunks": chunk_count
            }
