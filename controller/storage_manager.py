import os
import io
import uuid
import time
import sqlite3
import hashlib
import asyncio
from pathlib import Path
from typing import AsyncGenerator, Dict, Any, List, Optional, Tuple
import httpx
from fastapi import HTTPException, UploadFile, status
import logging

from common.models import (
    CHUNK_SIZE,
    REPLICATION_FACTOR,
    UPLOAD_ABANDON_SECONDS,
    UPLOAD_CONCURRENCY,
)
from controller.database import Database

logger = logging.getLogger("controller.storage")


def _read_chunk_slice(file_path: Path, offset: int, size: int) -> bytes:
    """Cross-platform positional read of a single chunk from a staged file."""
    with open(file_path, "rb") as f:
        f.seek(offset)
        return f.read(size)


class StorageManager:
    def __init__(self, db: Database, staging_dir: Optional[str] = None):
        self.db = db
        self.staging_dir = Path(staging_dir).resolve() if staging_dir else (db.db_path.parent / "staging")
        self.staging_dir.mkdir(parents=True, exist_ok=True)
        self.http_client = httpx.AsyncClient(timeout=10.0)
        # Tracks object_ids actively streaming or committing inside this controller process
        self.active_uploads: set[str] = set()
        self._commit_locks: Dict[str, asyncio.Lock] = {}

    async def close(self) -> None:
        await self.http_client.aclose()

    def _get_commit_lock(self, object_id: str) -> asyncio.Lock:
        lock = self._commit_locks.get(object_id)
        if lock is None:
            lock = asyncio.Lock()
            self._commit_locks[object_id] = lock
        return lock

    def _uploading_path(self, object_id: str) -> Path:
        return self.staging_dir / f"{object_id}.uploading"

    def _staged_path(self, object_id: str) -> Path:
        return self.staging_dir / f"{object_id}.staged"

    def _safe_unlink(self, path: Path) -> None:
        try:
            if path.is_file():
                path.unlink()
        except OSError as e:
            logger.warning(f"Failed to delete staging file {path}: {e}")

    # --- Startup Crash Recovery ---

    def recover_staging(self) -> Dict[str, int]:
        """
        Called once at controller startup before the resume loop starts:
        1. Drops incomplete STREAMING rows and deletes their partial .uploading files.
        2. Finishes any .uploading -> .staged rename interrupted right after DB commit point.
        3. Drops STAGED rows whose disk file is missing (for non-empty objects).
        4. Removes unreferenced orphan files from the staging directory.
        """
        summary = {"streaming_aborted": 0, "renames_finished": 0, "missing_aborted": 0, "orphans_deleted": 0}

        # 1. Drop dead STREAMING rows (never reached commit point)
        for row in self.db.get_uploads_by_state("STREAMING"):
            oid = row["object_id"]
            self._safe_unlink(self._uploading_path(oid))
            self._safe_unlink(self._staged_path(oid))
            self.db.abort_upload(oid)
            summary["streaming_aborted"] += 1
            logger.info(f"[RECOVERY] Aborted incomplete STREAMING upload {oid}")

        # 2. Verify STAGED rows and finish interrupted renames
        valid_staged_filenames: set[str] = set()
        for row in self.db.get_uploads_by_state("STAGED"):
            oid = row["object_id"]
            up_path = self._uploading_path(oid)
            st_path = self._staged_path(oid)

            if up_path.is_file() and not st_path.is_file():
                os.replace(up_path, st_path)
                summary["renames_finished"] += 1
                logger.info(f"[RECOVERY] Completed interrupted rename for staged upload {oid}")

            if row["num_chunks"] == 0 or st_path.is_file():
                valid_staged_filenames.add(st_path.name)
            else:
                self.db.abort_upload(oid)
                summary["missing_aborted"] += 1
                logger.warning(f"[RECOVERY] Staged file missing for {oid}; aborted pending upload")

        # 3. Delete orphan staging files not referenced by any STAGED row
        if self.staging_dir.exists():
            for p in self.staging_dir.iterdir():
                if p.is_file() and p.name not in valid_staged_filenames:
                    self._safe_unlink(p)
                    summary["orphans_deleted"] += 1
                    logger.info(f"[RECOVERY] Deleted orphan staging file {p.name}")

        logger.info(f"[RECOVERY] Staging recovery complete: {summary}")
        return summary

    # --- Staged, Crash-Safe & Idempotent Upload ---

    async def handle_upload(
        self,
        filename: str,
        content_type: str,
        upload_file: UploadFile,
        custom_metadata: Dict[str, Any],
        idempotency_key: Optional[str] = None
    ) -> Tuple[Dict[str, Any], bool]:
        """
        Streams an incoming UploadFile straight to persistent staging disk (bounded ~1 MiB RAM),
        enforces Idempotency-Key deduplication, and commits chunks across 2 workers.
        Returns (object_metadata_dict, is_idempotent_replay).
        """
        # 1. Idempotency-Key check
        if idempotency_key:
            existing = self.db.get_object_by_idempotency_key(idempotency_key)
            if existing:
                oid = existing["object_id"]
                if existing["object_state"] == "COMMITTED":
                    logger.info(f"[IDEMPOTENCY] Replaying committed object {oid} for key '{idempotency_key}'")
                    return self.db.get_object(oid), True

                if oid in self.active_uploads:
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail=f"An upload with Idempotency-Key '{idempotency_key}' is already in progress."
                    )

                if existing["upload_state"] == "STAGED":
                    # Already durably staged on disk! Resume committing directly without re-staging.
                    logger.info(f"[RESUME] Resuming staged upload {oid} via Idempotency-Key '{idempotency_key}'")
                    committed = await self.commit_upload(oid, raise_on_unavailable=True)
                    return committed, False

                if existing["upload_state"] == "STREAMING":
                    # Dead STREAMING row (not in active_uploads -> left by a crash): clear it so retry succeeds
                    logger.info(f"[IDEMPOTENCY] Clearing stale STREAMING upload {oid} for key '{idempotency_key}'")
                    self._safe_unlink(self._uploading_path(oid))
                    self._safe_unlink(self._staged_path(oid))
                    self.db.abort_upload(oid)

        # 2. DB-first: reserve PENDING object + STREAMING upload row
        object_id = uuid.uuid4().hex[:12]
        up_path = self._uploading_path(object_id)
        st_path = self._staged_path(object_id)

        try:
            self.db.begin_staged_upload(object_id, str(st_path), idempotency_key=idempotency_key)
        except sqlite3.IntegrityError:
            # Simultaneous race with identical Idempotency-Key
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Concurrent upload conflict for Idempotency-Key '{idempotency_key}'."
            )

        self.active_uploads.add(object_id)
        staged_committed = False

        try:
            # 3. Stream multipart body in 1 MiB slices straight to staging disk
            whole_hasher = hashlib.sha256()
            file_size = 0

            with open(up_path, "wb") as f:
                while True:
                    piece = await upload_file.read(CHUNK_SIZE)
                    if not piece:
                        break
                    f.write(piece)
                    whole_hasher.update(piece)
                    file_size += len(piece)
                f.flush()
                os.fsync(f.fileno())

            whole_checksum = whole_hasher.hexdigest()
            num_chunks = 0 if file_size == 0 else (file_size + CHUNK_SIZE - 1) // CHUNK_SIZE

            # Commit point of staging: atomic DB update -> STAGED
            self.db.mark_upload_staged(
                object_id=object_id,
                name=filename,
                size=file_size,
                content_type=content_type,
                checksum=whole_checksum,
                num_chunks=num_chunks,
                custom_metadata=custom_metadata,
                staging_path=str(st_path)
            )
            staged_committed = True

            # Housekeeping rename (.uploading -> .staged)
            os.replace(up_path, st_path)
            logger.info(
                f"[STAGING] Staged object {object_id} ({file_size} bytes, {num_chunks} chunks, "
                f"sha256: {whole_checksum[:8]}...)"
            )
        except Exception as e:
            self.active_uploads.discard(object_id)
            if not staged_committed:
                self._safe_unlink(up_path)
                self._safe_unlink(st_path)
                self.db.abort_upload(object_id)
            if isinstance(e, HTTPException):
                raise e
            logger.error(f"Staging failed for object {object_id}: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to stage upload: {str(e)}"
            )

        # 4. Replicate chunks from staged file to workers & commit
        try:
            committed = await self.commit_upload(object_id, raise_on_unavailable=True, already_active=True)
            return committed, False
        finally:
            self.active_uploads.discard(object_id)

    async def upload_object(
        self,
        filename: str,
        content_type: str,
        file_bytes: bytes,
        custom_metadata: Dict[str, Any],
        idempotency_key: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Convenience wrapper around handle_upload for in-memory bytes (used by tests & internal callers).
        """
        fake_upload = UploadFile(file=io.BytesIO(file_bytes), filename=filename)
        obj, _ = await self.handle_upload(
            filename=filename,
            content_type=content_type,
            upload_file=fake_upload,
            custom_metadata=custom_metadata,
            idempotency_key=idempotency_key
        )
        return obj

    async def commit_upload(
        self,
        object_id: str,
        raise_on_unavailable: bool = True,
        already_active: bool = False
    ) -> Optional[Dict[str, Any]]:
        """
        Idempotently distributes 1 MiB chunks from the persistent staged file to 2 distinct
        alive workers with bounded concurrency. Skips chunks that already have 2 healthy
        replicas (crash-safe resume). Marks COMMITTED and removes staged file once complete.
        """
        lock = self._get_commit_lock(object_id)
        async with lock:
            if not already_active:
                self.active_uploads.add(object_id)
            try:
                obj = self.db.get_object(object_id, include_pending=True)
                if not obj:
                    return None
                if obj.get("state") == "COMMITTED":
                    self._safe_unlink(self._staged_path(object_id))
                    return self.db.get_object(object_id)

                st_path = self._staged_path(object_id)
                up_path = self._uploading_path(object_id)
                if not st_path.is_file() and up_path.is_file():
                    os.replace(up_path, st_path)

                num_chunks = obj["num_chunks"]

                # Handle 0-byte empty objects cleanly (Fix #1)
                if num_chunks == 0:
                    self.db.commit_staged_upload(object_id)
                    self._safe_unlink(st_path)
                    return self.db.get_object(object_id)

                if not st_path.is_file():
                    msg = f"Staging file missing for object '{object_id}'."
                    self.db.update_upload_error(object_id, msg)
                    if raise_on_unavailable:
                        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=msg)
                    return None

                alive_workers = self.db.get_alive_workers()
                if len(alive_workers) < REPLICATION_FACTOR:
                    msg = (
                        f"Upload staged as '{object_id}', but only {len(alive_workers)}/{REPLICATION_FACTOR} "
                        f"storage workers are alive. It will resume automatically once workers recover."
                    )
                    self.db.update_upload_error(object_id, msg)
                    if raise_on_unavailable:
                        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=msg)
                    return None

                # Shared mutable load counter across chunk tasks
                worker_counts = {w["worker_id"]: w["chunk_count"] for w in alive_workers}
                worker_map = {w["worker_id"]: w for w in alive_workers}
                sem = asyncio.Semaphore(UPLOAD_CONCURRENCY)

                async def process_chunk(idx: int) -> bool:
                    async with sem:
                        chunk_id = f"{object_id}_{idx}"
                        existing_alive = self.db.get_chunk_alive_workers(chunk_id)
                        if len(existing_alive) >= REPLICATION_FACTOR:
                            # Already replicated prior to crash/retry -> idempotent skip!
                            return True

                        chunk_bytes = await asyncio.to_thread(
                            _read_chunk_slice, st_path, idx * CHUNK_SIZE, CHUNK_SIZE
                        )
                        c_checksum = hashlib.sha256(chunk_bytes).hexdigest()

                        # Record chunk row BEFORE writing to workers so reconcile_worker never deletes it (Fix #4)
                        self.db.ensure_chunk_record(
                            chunk_id=chunk_id,
                            object_id=object_id,
                            idx=idx,
                            size=len(chunk_bytes),
                            checksum=c_checksum
                        )

                        needed = REPLICATION_FACTOR - len(existing_alive)
                        candidates = [
                            w for wid, w in worker_map.items()
                            if wid not in existing_alive
                        ]
                        # Sort least-loaded first with deterministic distribution tie-breaker
                        candidates.sort(
                            key=lambda w: (worker_counts.get(w["worker_id"], 0), (hash(w["worker_id"]) + idx) % 1000)
                        )

                        stored_count = 0
                        # Try primary picks concurrently first, then fall back to remaining healthy candidates
                        primary = candidates[:needed]
                        fallbacks = candidates[needed:]

                        # Reserve load synchronously BEFORE awaiting network I/O so concurrent chunk tasks
                        # distribute evenly across all 4 workers instead of all picking the same pair!
                        for w in primary:
                            worker_counts[w["worker_id"]] = worker_counts.get(w["worker_id"], 0) + 1

                        if primary:
                            tasks = [
                                self._put_chunk_to_worker(
                                    f"{w['url']}/internal/chunks/{chunk_id}",
                                    chunk_bytes,
                                    expected_checksum=c_checksum
                                )
                                for w in primary
                            ]
                            results = await asyncio.gather(*tasks, return_exceptions=True)
                            for w, res in zip(primary, results):
                                if res is True:
                                    self.db.add_replica(chunk_id, w["worker_id"])
                                    stored_count += 1
                                else:
                                    worker_counts[w["worker_id"]] = max(0, worker_counts.get(w["worker_id"], 1) - 1)
                                    logger.warning(
                                        f"[UPLOAD] Failed to store {chunk_id} on {w['worker_id']}: {res}"
                                    )

                        # Retry on any remaining healthy fallback workers if a primary worker failed
                        for w in fallbacks:
                            if stored_count >= needed:
                                break
                            try:
                                ok = await self._put_chunk_to_worker(
                                    f"{w['url']}/internal/chunks/{chunk_id}",
                                    chunk_bytes,
                                    expected_checksum=c_checksum
                                )
                                if ok:
                                    self.db.add_replica(chunk_id, w["worker_id"])
                                    worker_counts[w["worker_id"]] = worker_counts.get(w["worker_id"], 0) + 1
                                    stored_count += 1
                            except Exception as e:
                                logger.warning(f"[UPLOAD] Fallback worker {w['worker_id']} failed for {chunk_id}: {e}")

                        return (len(existing_alive) + stored_count) >= REPLICATION_FACTOR

                chunk_results = await asyncio.gather(*(process_chunk(i) for i in range(num_chunks)))

                if all(chunk_results):
                    self.db.commit_staged_upload(object_id)
                    self._safe_unlink(st_path)
                    return self.db.get_object(object_id)
                else:
                    msg = (
                        f"Upload durably staged as '{object_id}', but could not reach {REPLICATION_FACTOR} "
                        f"workers for all chunks. It will resume automatically or on retry with the same Idempotency-Key."
                    )
                    self.db.update_upload_error(object_id, msg)
                    if raise_on_unavailable:
                        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=msg)
                    return None
            finally:
                if not already_active:
                    self.active_uploads.discard(object_id)

    async def resume_staged_uploads(self) -> None:
        """
        Background loop helper: resumes STAGED uploads left pending by a controller restart
        or temporary worker outage, and cleans up uploads abandoned past UPLOAD_ABANDON_SECONDS.
        """
        staged_rows = self.db.get_uploads_by_state("STAGED")
        if not staged_rows:
            return

        now = time.time()
        alive_count = len(self.db.get_alive_workers())

        for row in staged_rows:
            oid = row["object_id"]
            if oid in self.active_uploads:
                continue

            age = now - row["created_at"]
            if age > UPLOAD_ABANDON_SECONDS:
                logger.warning(f"[STAGING] Abandoning expired staged upload {oid} (age {age:.0f}s)")
                self._safe_unlink(self._staged_path(oid))
                self._safe_unlink(self._uploading_path(oid))
                chunks = self.db.abort_upload(oid)
                if chunks:
                    await self._cleanup_worker_chunks(chunks)
                continue

            if alive_count >= REPLICATION_FACTOR or row["num_chunks"] == 0:
                logger.info(f"[RESUME LOOP] Resuming staged upload {oid} ({row['name']})")
                try:
                    committed = await self.commit_upload(oid, raise_on_unavailable=False)
                    if committed:
                        logger.info(f"[RESUME LOOP] Successfully committed staged upload {oid}!")
                except Exception as e:
                    logger.error(f"[RESUME LOOP] Error resuming upload {oid}: {e}")

    async def _put_chunk_to_worker(
        self,
        url: str,
        data: bytes,
        expected_checksum: Optional[str] = None
    ) -> bool:
        headers = {"Content-Type": "application/octet-stream"}
        if expected_checksum:
            headers["X-Chunk-Checksum"] = expected_checksum
        resp = await self.http_client.put(url, content=data, headers=headers)
        return resp.status_code in (200, 201)

    async def _delete_chunk_from_worker(self, worker_base_url: str, chunk_id: str) -> None:
        try:
            url = f"{worker_base_url}/internal/chunks/{chunk_id}"
            await self.http_client.delete(url)
        except Exception as e:
            logger.warning(f"Failed to delete chunk {chunk_id} from {worker_base_url}: {e}")

    async def _cleanup_worker_chunks(self, chunks: List[Dict[str, Any]]) -> None:
        all_workers = {w["worker_id"]: w for w in self.db.get_all_workers()}
        delete_tasks = []
        for c in chunks:
            chunk_id = c["chunk_id"]
            for wid in c.get("workers", []):
                worker = all_workers.get(wid)
                if worker:
                    delete_tasks.append(self._delete_chunk_from_worker(worker["url"], chunk_id))
        if delete_tasks:
            await asyncio.gather(*delete_tasks, return_exceptions=True)

    # --- Download & Integrity Verification ---

    def verify_object_readable(self, obj: Dict[str, Any]) -> None:
        """
        Pre-flight check before sending HTTP 200 headers (Fix #8):
        Ensures every chunk has at least one replica on an alive worker.
        Raises 503 Service Unavailable if any chunk is currently unavailable.
        """
        if obj["num_chunks"] == 0:
            return
        alive_ids = {w["worker_id"] for w in self.db.get_alive_workers()}
        for chunk_meta in obj["chunks"]:
            alive_holders = [wid for wid in chunk_meta["workers"] if wid in alive_ids]
            if not alive_holders:
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail=f"Object '{obj['object_id']}' is temporarily unavailable: chunk '{chunk_meta['chunk_id']}' has no alive replicas."
                )

    async def stream_object_chunks(self, object_id: str) -> AsyncGenerator[bytes, None]:
        """
        Streams object chunks in order. For each chunk:
        - Tries alive replica workers.
        - Verifies SHA-256 checksum!
        - Silently falls back to another replica if corruption or network failure occurs.
        - Discards and unlinks corrupted replica.
        """
        obj = self.db.get_object(object_id)
        if not obj:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Object '{object_id}' not found."
            )

        if obj["num_chunks"] == 0:
            return

        all_workers = {w["worker_id"]: w for w in self.db.get_all_workers()}

        for chunk_meta in obj["chunks"]:
            chunk_id = chunk_meta["chunk_id"]
            expected_checksum = chunk_meta["checksum"]
            worker_ids = chunk_meta["workers"]

            chunk_data: Optional[bytes] = None
            success = False

            # Sort workers: prioritize alive workers first
            candidates = sorted(
                worker_ids,
                key=lambda wid: 0 if all_workers.get(wid, {}).get("status") == "alive" else 1
            )

            for wid in candidates:
                worker = all_workers.get(wid)
                if not worker:
                    continue

                try:
                    url = f"{worker['url']}/internal/chunks/{chunk_id}"
                    resp = await self.http_client.get(url)
                    if resp.status_code == 200:
                        data = resp.content
                        calculated_checksum = hashlib.sha256(data).hexdigest()

                        if calculated_checksum == expected_checksum:
                            chunk_data = data
                            success = True
                            logger.debug(f"Retrieved chunk {chunk_id} from {wid} with valid checksum.")
                            break
                        else:
                            # CORRUPTION DETECTED!
                            logger.error(
                                f"DATA INTEGRITY CORRUPTION DETECTED on worker '{wid}' for chunk '{chunk_id}'. "
                                f"Expected: {expected_checksum}, Got: {calculated_checksum}. Falling back to next replica..."
                            )
                            # Remove corrupt replica row to trigger repair loop self-healing!
                            self.db.remove_replica(chunk_id, wid)
                            # Purge corrupt file from worker disk
                            asyncio.create_task(self._delete_chunk_from_worker(worker["url"], chunk_id))
                    else:
                        logger.warning(f"Worker '{wid}' returned status {resp.status_code} for chunk '{chunk_id}'")
                except Exception as e:
                    logger.warning(f"Failed to fetch chunk '{chunk_id}' from worker '{wid}': {e}. Trying alternative...")

            if not success or chunk_data is None:
                logger.critical(f"FATAL: All replicas failed or corrupted for chunk '{chunk_id}'!")
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Data integrity error: could not retrieve valid copy for chunk '{chunk_id}'."
                )

            yield chunk_data

    async def delete_object(self, object_id: str) -> bool:
        """
        Deletes object and instructs workers to remove their chunk files.
        """
        chunks_to_delete = self.db.delete_object(object_id)
        if chunks_to_delete is None:
            return False

        self._safe_unlink(self._staged_path(object_id))
        self._safe_unlink(self._uploading_path(object_id))
        await self._cleanup_worker_chunks(chunks_to_delete)
        return True
