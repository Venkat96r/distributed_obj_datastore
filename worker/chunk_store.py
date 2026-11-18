import os
import uuid
import hashlib
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple
import logging

from common.lru import LRUCache
from common.models import WORKER_CACHE_MAX_BYTES

logger = logging.getLogger("worker.chunk_store")


class ChecksumMismatchError(ValueError):
    """Raised when uploaded chunk bytes do not match expected SHA-256."""


class ChunkStore:
    def __init__(self, data_dir: str, cache_max_bytes: int = WORKER_CACHE_MAX_BYTES):
        self.data_dir = Path(data_dir).resolve()
        self.chunks_dir = self.data_dir / "chunks"
        self.chunks_dir.mkdir(parents=True, exist_ok=True)
        self.cache = LRUCache(max_entries=None, max_bytes=cache_max_bytes, ttl_seconds=None)
        self._cleanup_stale_tmp_files()
        logger.info(
            f"Initialized ChunkStore at {self.chunks_dir} "
            f"(RAM cache budget: {cache_max_bytes / (1024 * 1024):.1f} MiB)"
        )

    def _cleanup_stale_tmp_files(self) -> None:
        """Removes any partial .tmp files left behind by a prior crash."""
        if not self.chunks_dir.exists():
            return
        for p in self.chunks_dir.iterdir():
            if p.is_file() and (p.name.endswith(".tmp") or ".tmp." in p.name):
                try:
                    p.unlink()
                    logger.info(f"Cleaned up stale temp file {p.name}")
                except OSError:
                    pass

    def _chunk_path(self, chunk_id: str) -> Path:
        # Prevent directory traversal
        safe_id = Path(chunk_id).name
        return self.chunks_dir / safe_id

    def save_chunk(
        self,
        chunk_id: str,
        data: bytes,
        expected_checksum: Optional[str] = None
    ) -> Tuple[int, str]:
        """
        Verifies optional checksum, atomically saves chunk data using a unique
        temporary file + fsync + atomic rename, and writes through to cache
        ONLY after durable disk commit.
        """
        hasher = hashlib.sha256()
        hasher.update(data)
        checksum = hasher.hexdigest()
        size = len(data)

        if expected_checksum and checksum != expected_checksum:
            raise ChecksumMismatchError(
                f"Checksum mismatch for chunk '{chunk_id}': "
                f"expected {expected_checksum}, got {checksum}"
            )

        final_path = self._chunk_path(chunk_id)
        temp_path = self.chunks_dir / f"{final_path.name}.tmp.{os.getpid()}.{uuid.uuid4().hex[:8]}"

        # Write to unique temporary file, sync to disk, then atomically rename
        try:
            with open(temp_path, "wb") as f:
                f.write(data)
                f.flush()
                os.fsync(f.fileno())
            os.replace(temp_path, final_path)

            # Write-through to bounded LRU cache only AFTER durable disk commit
            self.cache.put(chunk_id, data, size_bytes=size)

            logger.info(f"Saved chunk {chunk_id} ({size} bytes, sha256: {checksum[:8]}...)")
            return size, checksum
        except Exception as e:
            if temp_path.exists():
                try:
                    temp_path.unlink()
                except OSError:
                    pass
            logger.error(f"Failed to save chunk {chunk_id}: {e}")
            raise

    def get_chunk(self, chunk_id: str) -> Optional[bytes]:
        """
        Reads chunk binary data from bounded LRU cache or disk.
        Uses stale-fill protection so concurrent deletes/corruptions are never overwritten.
        """
        cached = self.cache.get(chunk_id)
        if cached is not None:
            return cached

        token = self.cache.begin_load(chunk_id)
        path = self._chunk_path(chunk_id)
        if not path.is_file():
            return None
        with open(path, "rb") as f:
            data = f.read()

        self.cache.put_if_fresh(chunk_id, data, token, size_bytes=len(data))
        return data

    def delete_chunk(self, chunk_id: str) -> bool:
        """
        Invalidates cache and deletes chunk file from disk.
        Returns True if deleted, False if not found.
        """
        self.cache.invalidate(chunk_id)
        path = self._chunk_path(chunk_id)
        if path.is_file():
            try:
                path.unlink()
                logger.info(f"Deleted chunk {chunk_id}")
                return True
            except OSError as e:
                logger.error(f"Error deleting chunk {chunk_id}: {e}")
                raise
        return False

    def list_chunks(self) -> List[Dict[str, Any]]:
        """
        Returns list of chunks present on this worker.
        """
        chunks = []
        if not self.chunks_dir.exists():
            return chunks

        for p in self.chunks_dir.iterdir():
            if p.is_file() and not p.name.endswith(".tmp") and ".tmp." not in p.name:
                stat = p.stat()
                chunks.append({
                    "chunk_id": p.name,
                    "size": stat.st_size,
                    "modified_at": stat.st_mtime
                })
        return chunks

    def corrupt_chunk(self, chunk_id: str) -> bool:
        """
        Intentionally flips/mutates bytes of chunk on disk for integrity testing,
        and evicts the chunk from RAM cache so subsequent reads hit disk.
        """
        self.cache.invalidate(chunk_id)
        path = self._chunk_path(chunk_id)
        if not path.is_file():
            return False
        with open(path, "r+b") as f:
            data = f.read()
            if len(data) > 0:
                # Invert first byte
                corrupted_first_byte = bytes([data[0] ^ 0xFF])
                f.seek(0)
                f.write(corrupted_first_byte)
            else:
                f.write(b"CORRUPTED_DATA")
            f.flush()
            os.fsync(f.fileno())
        self.cache.invalidate(chunk_id)
        logger.warning(f"Intentionally corrupted chunk {chunk_id} on disk (cache evicted)!")
        return True

    def cache_stats(self) -> Dict[str, Any]:
        return self.cache.stats()
