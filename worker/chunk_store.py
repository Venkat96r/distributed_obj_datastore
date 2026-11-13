import os
import hashlib
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple
import logging

logger = logging.getLogger("worker.chunk_store")


class ChunkStore:
    def __init__(self, data_dir: str):
        self.data_dir = Path(data_dir).resolve()
        self.chunks_dir = self.data_dir / "chunks"
        self.chunks_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Initialized ChunkStore at {self.chunks_dir}")

    def _chunk_path(self, chunk_id: str) -> Path:
        # Prevent directory traversal
        safe_id = Path(chunk_id).name
        return self.chunks_dir / safe_id

    def save_chunk(self, chunk_id: str, data: bytes) -> Tuple[int, str]:
        """
        Atomically saves chunk data using temporary file and atomic rename.
        Computes and returns (size, sha256_checksum).
        """
        final_path = self._chunk_path(chunk_id)
        temp_path = self.chunks_dir / f"{final_path.name}.tmp.{os.getpid()}"

        hasher = hashlib.sha256()
        hasher.update(data)
        checksum = hasher.hexdigest()
        size = len(data)

        # Write to temporary file, sync to disk, then atomically rename
        try:
            with open(temp_path, "wb") as f:
                f.write(data)
                f.flush()
                os.fsync(f.fileno())
            os.replace(temp_path, final_path)
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
        Reads chunk binary data if file exists, returns None if not found.
        """
        path = self._chunk_path(chunk_id)
        if not path.is_file():
            return None
        with open(path, "rb") as f:
            return f.read()

    def delete_chunk(self, chunk_id: str) -> bool:
        """
        Deletes chunk file from disk. Returns True if deleted, False if not found.
        """
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
            if p.is_file() and not p.name.endswith(".tmp") and not ".tmp." in p.name:
                stat = p.stat()
                chunks.append({
                    "chunk_id": p.name,
                    "size": stat.st_size,
                    "modified_at": stat.st_mtime
                })
        return chunks

    def corrupt_chunk(self, chunk_id: str) -> bool:
        """
        Intentionally flips/mutates bytes of chunk for integrity testing.
        """
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
        logger.warning(f"Intentionally corrupted chunk {chunk_id} on disk!")
        return True
