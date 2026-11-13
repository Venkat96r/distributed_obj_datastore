import uuid
import hashlib
import asyncio
from typing import AsyncGenerator, Dict, Any, List, Optional
import httpx
from fastapi import HTTPException, status
import logging

from common.models import CHUNK_SIZE, REPLICATION_FACTOR
from controller.database import Database

logger = logging.getLogger("controller.storage")


class StorageManager:
    def __init__(self, db: Database):
        self.db = db
        self.http_client = httpx.AsyncClient(timeout=10.0)

    def _select_workers_for_chunk(
        self,
        alive_workers: List[Dict[str, Any]],
        worker_chunk_counts: Dict[str, int],
        idx: int
    ) -> List[Dict[str, Any]]:
        """
        Selects 2 distinct alive workers for a chunk, balancing load across workers.
        """
        if len(alive_workers) < REPLICATION_FACTOR:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"Cannot maintain replication factor of {REPLICATION_FACTOR}. Only {len(alive_workers)} storage worker(s) alive."
            )

        # Sort alive workers by current chunk count (least loaded first), then by worker_id + idx for distribution
        sorted_workers = sorted(
            alive_workers,
            key=lambda w: (worker_chunk_counts.get(w["worker_id"], 0), (hash(w["worker_id"]) + idx) % 1000)
        )

        selected = sorted_workers[:REPLICATION_FACTOR]
        for w in selected:
            worker_chunk_counts[w["worker_id"]] = worker_chunk_counts.get(w["worker_id"], 0) + 1

        return selected

    async def upload_object(
        self,
        filename: str,
        content_type: str,
        file_bytes: bytes,
        custom_metadata: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Cuts file into 1 MB chunks, stores with replication factor 2 across alive workers,
        and records metadata in a single database transaction.
        """
        alive_workers = self.db.get_alive_workers()
        if len(alive_workers) < REPLICATION_FACTOR:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"Insufficient active storage workers. Minimum required: {REPLICATION_FACTOR}, alive: {len(alive_workers)}"
            )

        object_id = uuid.uuid4().hex[:12]
        file_size = len(file_bytes)
        whole_hasher = hashlib.sha256()
        whole_hasher.update(file_bytes)
        whole_checksum = whole_hasher.hexdigest()

        # Track worker loads during this upload
        worker_counts = {w["worker_id"]: w["chunk_count"] for w in alive_workers}

        chunks_data: List[Dict[str, Any]] = []
        uploaded_chunks_record: List[tuple[str, str]] = []  # (worker_url, chunk_id) for rollback

        chunk_idx = 0
        offset = 0

        # Handle empty file edge-case: 1 empty chunk
        if file_size == 0:
            chunk_slices = [b""]
        else:
            chunk_slices = []
            while offset < file_size:
                chunk_slices.append(file_bytes[offset:offset + CHUNK_SIZE])
                offset += CHUNK_SIZE

        try:
            for idx, chunk_bytes in enumerate(chunk_slices):
                chunk_id = f"{object_id}_{idx}"
                chunk_size = len(chunk_bytes)
                c_hasher = hashlib.sha256()
                c_hasher.update(chunk_bytes)
                c_checksum = c_hasher.hexdigest()

                selected_workers = self._select_workers_for_chunk(alive_workers, worker_counts, idx)
                assigned_worker_ids = [w["worker_id"] for w in selected_workers]

                # Concurrently upload chunk to both workers
                upload_tasks = []
                for w in selected_workers:
                    url = f"{w['url']}/internal/chunks/{chunk_id}"
                    upload_tasks.append(self._put_chunk_to_worker(url, chunk_bytes))

                results = await asyncio.gather(*upload_tasks, return_exceptions=True)

                # Check if all replicas stored successfully
                for i, res in enumerate(results):
                    w = selected_workers[i]
                    if isinstance(res, Exception) or not res:
                        err_msg = str(res) if isinstance(res, Exception) else "Non-201 response"
                        logger.error(f"Failed to store chunk {chunk_id} on {w['worker_id']}: {err_msg}")
                        raise HTTPException(
                            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Storage failure on worker {w['worker_id']} for chunk {chunk_id}."
                        )
                    uploaded_chunks_record.append((w["url"], chunk_id))

                chunks_data.append({
                    "chunk_id": chunk_id,
                    "idx": idx,
                    "size": chunk_size,
                    "checksum": c_checksum,
                    "workers": assigned_worker_ids
                })

            # Atomic commit to metadata DB
            self.db.create_object_transaction(
                object_id=object_id,
                name=filename,
                size=file_size,
                content_type=content_type,
                checksum=whole_checksum,
                num_chunks=len(chunks_data),
                custom_metadata=custom_metadata,
                chunks_data=chunks_data
            )

            return self.db.get_object(object_id)

        except Exception as e:
            logger.error(f"Upload failed for object {object_id}: {e}. Rolling back chunks...")
            # Cleanup uploaded chunks across workers
            rollback_tasks = [
                self._delete_chunk_from_worker(url, c_id)
                for url, c_id in uploaded_chunks_record
            ]
            if rollback_tasks:
                await asyncio.gather(*rollback_tasks, return_exceptions=True)
            if isinstance(e, HTTPException):
                raise e
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Upload failed: {str(e)}"
            )

    async def _put_chunk_to_worker(self, url: str, data: bytes) -> bool:
        resp = await self.http_client.put(
            url,
            content=data,
            headers={"Content-Type": "application/octet-stream"}
        )
        return resp.status_code in (200, 201)

    async def _delete_chunk_from_worker(self, worker_base_url: str, chunk_id: str) -> None:
        try:
            url = f"{worker_base_url}/internal/chunks/{chunk_id}"
            await self.http_client.delete(url)
        except Exception as e:
            logger.warning(f"Failed to delete chunk {chunk_id} during rollback from {worker_base_url}: {e}")

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
                        hasher = hashlib.sha256()
                        hasher.update(data)
                        calculated_checksum = hasher.hexdigest()

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

        all_workers = {w["worker_id"]: w for w in self.db.get_all_workers()}

        # Fire delete requests to workers asynchronously
        delete_tasks = []
        for c in chunks_to_delete:
            chunk_id = c["chunk_id"]
            for wid in c["workers"]:
                worker = all_workers.get(wid)
                if worker:
                    url = f"{worker['url']}/internal/chunks/{chunk_id}"
                    delete_tasks.append(self.http_client.delete(url))

        if delete_tasks:
            await asyncio.gather(*delete_tasks, return_exceptions=True)

        return True
