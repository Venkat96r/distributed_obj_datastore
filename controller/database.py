import sqlite3
import json
import time
import os
import threading
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime, timezone
import logging

logger = logging.getLogger("controller.database")


class Database:
    def __init__(self, db_path: str):
        self.db_path = Path(db_path).resolve()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
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
                    custom_metadata TEXT DEFAULT '{}'
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

                CREATE INDEX IF NOT EXISTS idx_chunks_object ON chunks(object_id);
                CREATE INDEX IF NOT EXISTS idx_chunks_idx ON chunks(object_id, idx);
                CREATE INDEX IF NOT EXISTS idx_replicas_chunk ON replicas(chunk_id);
                CREATE INDEX IF NOT EXISTS idx_replicas_worker ON replicas(worker_id);
            """)
            conn.commit()
            logger.info(f"Database initialized at {self.db_path}")

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
            logger.info(f"Worker registered: {worker_id} at {url}")

    def record_heartbeat(self, worker_id: str) -> bool:
        with self._lock, self._get_connection() as conn:
            now = time.time()
            cur = conn.execute("""
                UPDATE workers
                SET last_heartbeat = ?, status = 'alive'
                WHERE worker_id = ?
            """, (now, worker_id))
            conn.commit()
            return cur.rowcount > 0

    def mark_worker_status(self, worker_id: str, status: str) -> None:
        with self._lock, self._get_connection() as conn:
            conn.execute("UPDATE workers SET status = ? WHERE worker_id = ?", (status, worker_id))
            conn.commit()

    def get_all_workers(self) -> List[Dict[str, Any]]:
        with self._lock, self._get_connection() as conn:
            # Query workers along with chunk count
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
                    "seconds_since_heartbeat": round(now - row["last_heartbeat"], 1)
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
                "seconds_since_heartbeat": round(now - row["last_heartbeat"], 1)
            }

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
        chunks_data: List[Dict[str, Any]]
    ) -> None:
        """
        Atomically records object, all chunks, and replicas in a single transaction.
        """
        now = datetime.now(timezone.utc).isoformat()
        custom_meta_str = json.dumps(custom_metadata)

        with self._lock, self._get_connection() as conn:
            try:
                # 1. Insert object
                conn.execute("""
                    INSERT INTO objects (
                        object_id, name, size, content_type, checksum,
                        num_chunks, created_at, modified_at, custom_metadata
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    object_id, name, size, content_type, checksum,
                    num_chunks, now, now, custom_meta_str
                ))

                # 2. Insert chunks and replicas
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
                logger.info(f"Committed object {object_id} with {num_chunks} chunks.")
            except Exception as e:
                conn.rollback()
                logger.error(f"Failed to record object {object_id}: {e}")
                raise

    def get_object(self, object_id: str) -> Optional[Dict[str, Any]]:
        with self._lock, self._get_connection() as conn:
            obj_row = conn.execute("SELECT * FROM objects WHERE object_id = ?", (object_id,)).fetchone()
            if not obj_row:
                return None

            # Get chunks ordered by idx
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

            return {
                "object_id": obj_row["object_id"],
                "name": obj_row["name"],
                "size": obj_row["size"],
                "content_type": obj_row["content_type"],
                "checksum": obj_row["checksum"],
                "num_chunks": obj_row["num_chunks"],
                "created_at": obj_row["created_at"],
                "modified_at": obj_row["modified_at"],
                "custom_metadata": custom_meta,
                "chunks": chunks
            }

    def list_objects(self, limit: int = 50, offset: int = 0) -> Tuple[List[Dict[str, Any]], int]:
        with self._lock, self._get_connection() as conn:
            total = conn.execute("SELECT COUNT(*) as count FROM objects").fetchone()["count"]
            rows = conn.execute("""
                SELECT object_id, name, size, content_type, checksum, num_chunks,
                       created_at, modified_at, custom_metadata
                FROM objects
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

            # Merge or overwrite custom metadata
            merged_meta = obj["custom_metadata"].copy()
            merged_meta.update(new_custom_metadata)
            now = datetime.now(timezone.utc).isoformat()

            conn.execute("""
                UPDATE objects
                SET custom_metadata = ?, modified_at = ?
                WHERE object_id = ?
            """, (json.dumps(merged_meta), now, object_id))
            conn.commit()

            return self.get_object(object_id)

    def delete_object(self, object_id: str) -> Optional[List[Dict[str, Any]]]:
        """
        Deletes object and cascades to chunks & replicas.
        Returns list of chunks with worker assignments so controller can tell workers to delete chunk files.
        """
        with self._lock, self._get_connection() as conn:
            obj = self.get_object(object_id)
            if not obj:
                return None

            chunks_to_delete = obj["chunks"]
            conn.execute("DELETE FROM objects WHERE object_id = ?", (object_id,))
            conn.commit()
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
            logger.info(f"Added replica for chunk {chunk_id} on worker {worker_id}")

    def remove_replica(self, chunk_id: str, worker_id: str) -> None:
        with self._lock, self._get_connection() as conn:
            conn.execute("""
                DELETE FROM replicas
                WHERE chunk_id = ? AND worker_id = ?
            """, (chunk_id, worker_id))
            conn.commit()
            logger.warning(f"Removed replica for chunk {chunk_id} from worker {worker_id}")

    def get_chunk_info(self, chunk_id: str) -> Optional[Dict[str, Any]]:
        with self._lock, self._get_connection() as conn:
            row = conn.execute("""
                SELECT c.chunk_id, c.object_id, c.idx, c.size, c.checksum,
                       GROUP_CONCAT(r.worker_id) as worker_ids
                FROM chunks c
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
                "idx": row["idx"],
                "size": row["size"],
                "checksum": row["checksum"],
                "workers": workers
            }

    def get_under_replicated_chunks(self, required_replicas: int = 2) -> List[Dict[str, Any]]:
        """
        Finds all chunks that have fewer than `required_replicas` on currently ALIVE workers.
        """
        with self._lock, self._get_connection() as conn:
            rows = conn.execute("""
                SELECT c.chunk_id, c.object_id, c.idx, c.size, c.checksum,
                       GROUP_CONCAT(CASE WHEN w.status = 'alive' THEN r.worker_id END) as alive_workers,
                       COUNT(CASE WHEN w.status = 'alive' THEN r.worker_id END) as alive_replica_count
                FROM chunks c
                JOIN replicas r ON c.chunk_id = r.chunk_id
                JOIN workers w ON r.worker_id = w.worker_id
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

    def get_cluster_stats(self) -> Dict[str, Any]:
        with self._lock, self._get_connection() as conn:
            obj_count = conn.execute("SELECT COUNT(*) as c FROM objects").fetchone()["c"]
            chunk_count = conn.execute("SELECT COUNT(*) as c FROM chunks").fetchone()["c"]
            return {
                "total_objects": obj_count,
                "total_chunks": chunk_count
            }
