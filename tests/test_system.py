import os
import sys
import shutil
import hashlib
import tempfile
import json
import pytest
from pathlib import Path
from starlette.testclient import TestClient
import httpx

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from common.models import CHUNK_SIZE, REPLICATION_FACTOR
from worker.chunk_store import ChunkStore
from controller.database import Database
from controller.storage_manager import StorageManager
from controller.repair_manager import RepairManager


@pytest.fixture
def temp_dir():
    d = tempfile.mkdtemp()
    yield d
    shutil.rmtree(d, ignore_errors=True)


@pytest.fixture
def anyio_backend():
    return 'asyncio'


def test_chunk_store_atomic_operations(temp_dir):
    store = ChunkStore(temp_dir)
    test_data = b"Hello, Distributed Cloud Storage! 12345"
    chunk_id = "test_chunk_0"

    # Save chunk
    size, checksum = store.save_chunk(chunk_id, test_data)
    assert size == len(test_data)
    assert checksum == hashlib.sha256(test_data).hexdigest()

    # Read chunk
    retrieved = store.get_chunk(chunk_id)
    assert retrieved == test_data

    # List chunks
    chunks = store.list_chunks()
    assert len(chunks) == 1
    assert chunks[0]["chunk_id"] == chunk_id
    assert chunks[0]["size"] == len(test_data)

    # Intentional corruption
    assert store.corrupt_chunk(chunk_id) is True
    corrupted_data = store.get_chunk(chunk_id)
    assert corrupted_data != test_data
    assert hashlib.sha256(corrupted_data).hexdigest() != checksum

    # Delete chunk
    assert store.delete_chunk(chunk_id) is True
    assert store.get_chunk(chunk_id) is None
    assert len(store.list_chunks()) == 0


def test_database_crud_and_transactions(temp_dir):
    db_path = os.path.join(temp_dir, "test_metadata.db")
    db = Database(db_path)

    # Register 4 workers
    for i in range(1, 5):
        db.register_worker(f"worker{i}", f"http://worker{i}:800{i}")

    workers = db.get_all_workers()
    assert len(workers) == 4
    assert all(w["status"] == "alive" for w in workers)

    # Record Heartbeat
    assert db.record_heartbeat("worker1") is True
    assert db.record_heartbeat("nonexistent") is False

    # Insert object with 2 chunks, each replicated to 2 workers
    obj_id = "obj_abc123"
    chunks_data = [
        {
            "chunk_id": f"{obj_id}_0",
            "idx": 0,
            "size": 1024,
            "checksum": "hash0",
            "workers": ["worker1", "worker2"]
        },
        {
            "chunk_id": f"{obj_id}_1",
            "idx": 1,
            "size": 512,
            "checksum": "hash1",
            "workers": ["worker3", "worker4"]
        }
    ]

    db.create_object_transaction(
        object_id=obj_id,
        name="test_file.txt",
        size=1536,
        content_type="text/plain",
        checksum="whole_hash",
        num_chunks=2,
        custom_metadata={"author": "alice", "tag": "test"},
        chunks_data=chunks_data
    )

    # Retrieve object
    obj = db.get_object(obj_id)
    assert obj is not None
    assert obj["name"] == "test_file.txt"
    assert obj["size"] == 1536
    assert obj["num_chunks"] == 2
    assert obj["custom_metadata"]["author"] == "alice"
    assert len(obj["chunks"]) == 2
    assert obj["chunks"][0]["workers"] == ["worker1", "worker2"]

    # PATCH custom metadata
    updated = db.update_object_metadata(obj_id, {"status": "reviewed", "version": "2.0"})
    assert updated["custom_metadata"]["status"] == "reviewed"
    assert updated["custom_metadata"]["author"] == "alice"
    assert updated["custom_metadata"]["version"] == "2.0"

    # List objects
    objs, total = db.list_objects(limit=10, offset=0)
    assert total == 1
    assert objs[0]["object_id"] == obj_id

    # Under-replicated check: all alive so under_replicated should be 0
    under_rep = db.get_under_replicated_chunks(required_replicas=2)
    assert len(under_rep) == 0

    # Mark worker2 DEAD -> chunk 0 now has only 1 alive replica (worker1)
    db.mark_worker_status("worker2", "dead")
    under_rep = db.get_under_replicated_chunks(required_replicas=2)
    assert len(under_rep) == 1
    assert under_rep[0]["chunk_id"] == f"{obj_id}_0"
    assert under_rep[0]["alive_workers"] == ["worker1"]

    # Repair: add replica on worker3
    db.add_replica(f"{obj_id}_0", "worker3")
    under_rep = db.get_under_replicated_chunks(required_replicas=2)
    assert len(under_rep) == 0

    # Delete object
    deleted_chunks = db.delete_object(obj_id)
    assert deleted_chunks is not None
    assert len(deleted_chunks) == 2
    assert db.get_object(obj_id) is None
    objs, total = db.list_objects()
    assert total == 0


@pytest.mark.anyio
async def test_end_to_end_cluster_flow(temp_dir):
    """
    Simulates complete distributed workflow in-process:
    1. Creates 4 worker chunk stores.
    2. Uses custom HTTP transport to route requests between controller and workers.
    3. Uploads 2.5 MB object (3 chunks).
    4. Downloads and validates SHA-256 bit-for-bit.
    5. Tests worker failure & retrieval from replica.
    6. Tests intentional corruption & silent fallback.
    7. Tests self-healing repair.
    8. Tests deletion and chunk removal.
    """
    db_path = os.path.join(temp_dir, "metadata.db")
    db = Database(db_path)

    # Setup 4 worker chunk stores
    worker_stores = {}
    for wid in ["worker1", "worker2", "worker3", "worker4"]:
        wdir = os.path.join(temp_dir, wid)
        worker_stores[wid] = ChunkStore(wdir)
        db.register_worker(wid, f"http://{wid}:8000")

    # Custom mock transport routing to worker chunk stores
    def mock_handler(request: httpx.Request) -> httpx.Response:
        url_str = str(request.url)
        host = request.url.host  # e.g. worker1
        wid = host
        store = worker_stores.get(wid)
        if not store:
            return httpx.Response(500, content=b"Unknown worker host")

        path = request.url.path
        if path.startswith("/internal/chunks/"):
            chunk_id = path.replace("/internal/chunks/", "")
            if request.method == "PUT":
                size, checksum = store.save_chunk(chunk_id, request.content)
                return httpx.Response(201, json={"status": "stored", "size": size, "checksum": checksum})
            elif request.method == "GET":
                data = store.get_chunk(chunk_id)
                if data is None:
                    return httpx.Response(404, json={"detail": "Not found"})
                return httpx.Response(200, content=data)
            elif request.method == "DELETE":
                store.delete_chunk(chunk_id)
                return httpx.Response(204)
        elif path.startswith(f"/internal/workers/{wid}/chunks"):
            chunks = store.list_chunks()
            return httpx.Response(200, json={"chunks": chunks})

        return httpx.Response(404, json={"detail": "Not found"})

    mock_client = httpx.AsyncClient(transport=httpx.MockTransport(mock_handler))

    # Initialize StorageManager & RepairManager with mock_client
    sm = StorageManager(db)
    sm.http_client = mock_client
    rm = RepairManager(db)
    rm.http_client = mock_client

    # 1. Upload a 2.5 MB file (3 chunks: 1MB, 1MB, 0.5MB)
    file_size = int(2.5 * 1024 * 1024)
    file_bytes = b"X" * file_size
    orig_hash = hashlib.sha256(file_bytes).hexdigest()

    obj_meta = await sm.upload_object(
        filename="big_document.bin",
        content_type="application/octet-stream",
        file_bytes=file_bytes,
        custom_metadata={"environment": "testing", "priority": "high"}
    )

    assert obj_meta["num_chunks"] == 3
    assert obj_meta["size"] == file_size
    assert obj_meta["checksum"] == orig_hash
    assert len(obj_meta["chunks"]) == 3

    # Verify each chunk has exactly 2 distinct replica workers
    for c in obj_meta["chunks"]:
        assert len(c["workers"]) == 2
        assert c["workers"][0] != c["workers"][1]

    # 2. Download and verify bit-for-bit integrity
    downloaded_chunks = []
    async for chunk_data in sm.stream_object_chunks(obj_meta["object_id"]):
        downloaded_chunks.append(chunk_data)

    reassembled = b"".join(downloaded_chunks)
    assert len(reassembled) == file_size
    assert hashlib.sha256(reassembled).hexdigest() == orig_hash

    # 3. Simulate Worker Failure: mark worker2 DEAD
    db.mark_worker_status("worker2", "dead")
    # File download should STILL succeed seamlessly because every chunk has another replica!
    downloaded_chunks_after_fail = []
    async for chunk_data in sm.stream_object_chunks(obj_meta["object_id"]):
        downloaded_chunks_after_fail.append(chunk_data)
    assert b"".join(downloaded_chunks_after_fail) == file_bytes

    # 4. Run Self-Healing Repair
    await rm.repair_under_replicated_chunks()
    # Check that any under-replicated chunk was restored to 2 alive workers
    under_rep = db.get_under_replicated_chunks(required_replicas=2)
    assert len(under_rep) == 0

    # 5. Simulate Data Corruption
    # Corrupt chunk 0 on one worker
    chunk_0_id = obj_meta["chunks"][0]["chunk_id"]
    chunk_0_info = db.get_chunk_info(chunk_0_id)
    corrupted_worker = chunk_0_info["workers"][0]
    worker_stores[corrupted_worker].corrupt_chunk(chunk_0_id)

    # Download should detect corruption, discard bad replica, and fall back to healthy replica
    downloaded_after_corruption = []
    async for chunk_data in sm.stream_object_chunks(obj_meta["object_id"]):
        downloaded_after_corruption.append(chunk_data)
    assert b"".join(downloaded_after_corruption) == file_bytes

    # Now the chunk is under-replicated again (because bad replica was discarded)
    under_rep = db.get_under_replicated_chunks(required_replicas=2)
    assert len(under_rep) >= 1

    # Revive worker2 and run repair loop to heal the corrupted chunk
    db.mark_worker_status("worker2", "alive")
    await rm.repair_under_replicated_chunks()
    under_rep_after_repair = db.get_under_replicated_chunks(required_replicas=2)
    assert len(under_rep_after_repair) == 0

    # 6. Delete Object
    deleted = await sm.delete_object(obj_meta["object_id"])
    assert deleted is True
    assert db.get_object(obj_meta["object_id"]) is None

    # Verify chunks removed from worker stores
    for wid, store in worker_stores.items():
        assert len(store.list_chunks()) == 0

    await mock_client.aclose()
