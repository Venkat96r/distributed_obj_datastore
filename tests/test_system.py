import os
import io
import sys
import time
import shutil
import hashlib
import tempfile
import asyncio
import pytest
from pathlib import Path
from fastapi import HTTPException, UploadFile
import httpx

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from common.lru import LRUCache
from common.models import CHUNK_SIZE, REPLICATION_FACTOR
from worker.chunk_store import ChunkStore, ChecksumMismatchError
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
    return "asyncio"


def _make_mock_cluster(temp_dir):
    """Helper creating a Database, 4 ChunkStores, StorageManager, RepairManager, and MockTransport."""
    db_path = os.path.join(temp_dir, "metadata.db")
    db = Database(db_path)

    worker_stores = {}
    worker_online = {}
    for wid in ["worker1", "worker2", "worker3", "worker4"]:
        wdir = os.path.join(temp_dir, wid)
        worker_stores[wid] = ChunkStore(wdir, cache_max_bytes=8 * 1024 * 1024)
        worker_online[wid] = True
        db.register_worker(wid, f"http://{wid}:8000")

    def mock_handler(request: httpx.Request) -> httpx.Response:
        wid = request.url.host
        store = worker_stores.get(wid)
        if not store or not worker_online.get(wid, False):
            raise httpx.ConnectError(f"Worker {wid} unreachable")

        path = request.url.path
        if path == "/health":
            return httpx.Response(200, json={"status": "ok", "worker_id": wid, "cache": store.cache_stats()})
        if path.startswith("/internal/chunks/"):
            chunk_id = path.replace("/internal/chunks/", "")
            if request.method == "PUT":
                expected_cs = request.headers.get("x-chunk-checksum")
                try:
                    size, checksum = store.save_chunk(chunk_id, request.content, expected_checksum=expected_cs)
                except ChecksumMismatchError as e:
                    return httpx.Response(400, json={"detail": str(e)})
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
            return httpx.Response(200, json={"chunks": store.list_chunks()})

        return httpx.Response(404, json={"detail": "Not found"})

    mock_client = httpx.AsyncClient(transport=httpx.MockTransport(mock_handler))
    sm = StorageManager(db, staging_dir=os.path.join(temp_dir, "staging"))
    sm.http_client = mock_client
    rm = RepairManager(db, storage_manager=sm)
    rm.http_client = mock_client

    return db, worker_stores, worker_online, sm, rm, mock_client


# --- 1. Bounded LRU Cache & Stale-Fill Protection Tests ---

def test_lru_cache_bounds_ttl_and_stale_fill():
    # Entry bound & LRU eviction
    cache = LRUCache(max_entries=2, ttl_seconds=60.0)
    cache.put("a", 1)
    cache.put("b", 2)
    assert cache.get("a") == 1  # promotes 'a', so 'b' is LRU
    cache.put("c", 3)           # evicts 'b'
    assert cache.get("b") is None
    assert cache.get("a") == 1
    assert cache.get("c") == 3
    assert cache.stats()["evictions"] == 1

    # Stale-fill protection (per-key invalidation)
    tok = cache.begin_load("x")
    cache.invalidate("x")  # concurrent writer invalidated 'x' while reader was loading
    assert cache.put_if_fresh("x", "stale_value", tok) is False
    assert cache.get("x") is None
    assert cache.stats()["stale_fills_rejected"] == 1

    # Stale-fill protection (global invalidation)
    tok2 = cache.begin_load("y")
    cache.invalidate_all()
    assert cache.put_if_fresh("y", "stale_global", tok2) is False
    assert cache.get("y") is None

    # Byte budget bound
    bcache = LRUCache(max_entries=None, max_bytes=10)
    assert bcache.put("k1", b"123456", size_bytes=6) is True
    assert bcache.put("k2", b"123456", size_bytes=6) is True  # evicts k1 (6+6 > 10)
    assert bcache.get("k1") is None
    assert bcache.get("k2") == b"123456"
    assert bcache.put("huge", b"0" * 20, size_bytes=20) is False  # larger than budget

    # TTL expiration
    tcache = LRUCache(max_entries=10, ttl_seconds=0.05)
    tcache.put("temp", "val")
    assert tcache.get("temp") == "val"
    time.sleep(0.08)
    assert tcache.get("temp") is None


# --- 2. ChunkStore Atomic Ops, Checksum Verification & Cache ---

def test_chunk_store_atomic_operations_checksum_and_cache(temp_dir):
    # Pre-create a stale temp file to verify cleanup on startup
    chunks_dir = Path(temp_dir) / "chunks"
    chunks_dir.mkdir(parents=True, exist_ok=True)
    stale_tmp = chunks_dir / "old_chunk.tmp.9999.abcd1234"
    stale_tmp.write_bytes(b"partial garbage")

    store = ChunkStore(temp_dir, cache_max_bytes=1024 * 1024)
    assert not stale_tmp.exists()

    test_data = b"Hello, Distributed Cloud Storage! 12345"
    chunk_id = "test_chunk_0"
    good_hash = hashlib.sha256(test_data).hexdigest()

    # Reject mismatched checksum on write (Fix #9)
    with pytest.raises(ChecksumMismatchError):
        store.save_chunk(chunk_id, test_data, expected_checksum="0" * 64)
    assert store.get_chunk(chunk_id) is None

    # Save chunk with matching checksum (populates cache write-through after fsync)
    size, checksum = store.save_chunk(chunk_id, test_data, expected_checksum=good_hash)
    assert size == len(test_data)
    assert checksum == good_hash

    # Read chunk -> should be a RAM cache HIT!
    retrieved = store.get_chunk(chunk_id)
    assert retrieved == test_data
    assert store.cache_stats()["hits"] == 1

    # List chunks
    chunks = store.list_chunks()
    assert len(chunks) == 1
    assert chunks[0]["chunk_id"] == chunk_id

    # Intentional corruption MUST evict RAM cache so read hits disk!
    assert store.corrupt_chunk(chunk_id) is True
    corrupted_data = store.get_chunk(chunk_id)
    assert corrupted_data != test_data
    assert hashlib.sha256(corrupted_data).hexdigest() != checksum

    # Delete chunk
    assert store.delete_chunk(chunk_id) is True
    assert store.get_chunk(chunk_id) is None
    assert len(store.list_chunks()) == 0


# --- 3. Database CRUD, Metadata Cache & LEFT JOIN Under-Replicated ---

def test_database_crud_and_transactions(temp_dir):
    db_path = os.path.join(temp_dir, "test_metadata.db")
    db = Database(db_path)

    for i in range(1, 5):
        db.register_worker(f"worker{i}", f"http://worker{i}:800{i}")

    workers = db.get_all_workers()
    assert len(workers) == 4
    assert all(w["status"] == "alive" for w in workers)
    assert db.record_heartbeat("worker1") is True
    assert db.record_heartbeat("nonexistent") is False

    obj_id = "obj_abc123"
    chunks_data = [
        {"chunk_id": f"{obj_id}_0", "idx": 0, "size": 1024, "checksum": "hash0", "workers": ["worker1", "worker2"]},
        {"chunk_id": f"{obj_id}_1", "idx": 1, "size": 512, "checksum": "hash1", "workers": ["worker3", "worker4"]},
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

    # First read = cache miss; second read = cache hit
    obj1 = db.get_object(obj_id)
    assert db.get_cache_stats()["misses"] == 1
    obj2 = db.get_object(obj_id)
    assert db.get_cache_stats()["hits"] == 1
    assert obj1 == obj2

    # PATCH invalidates cache cleanly
    updated = db.update_object_metadata(obj_id, {"status": "reviewed", "version": "2.0"})
    assert updated["custom_metadata"]["status"] == "reviewed"
    assert updated["custom_metadata"]["author"] == "alice"

    # Under-replicated check: 0 replicas still detected via LEFT JOIN (Fix #7)
    db.remove_replica(f"{obj_id}_0", "worker1")
    db.remove_replica(f"{obj_id}_0", "worker2")
    under_rep = db.get_under_replicated_chunks(required_replicas=2)
    assert len(under_rep) == 1
    assert under_rep[0]["chunk_id"] == f"{obj_id}_0"
    assert under_rep[0]["alive_count"] == 0

    # Restore replicas + add 3rd surplus replica -> detected by get_over_replicated_chunks
    db.add_replica(f"{obj_id}_0", "worker1")
    db.add_replica(f"{obj_id}_0", "worker2")
    db.add_replica(f"{obj_id}_0", "worker3")
    over_rep = db.get_over_replicated_chunks(required_replicas=2)
    assert len(over_rep) == 1
    assert over_rep[0]["chunk_id"] == f"{obj_id}_0"

    # Delete object
    deleted_chunks = db.delete_object(obj_id)
    assert deleted_chunks is not None and len(deleted_chunks) == 2
    assert db.get_object(obj_id) is None


# --- 4. Fix #1: 0-Byte Empty Object Upload & Download ---

@pytest.mark.anyio
async def test_empty_zero_byte_object_upload_and_download(temp_dir):
    db, worker_stores, worker_online, sm, rm, mock_client = _make_mock_cluster(temp_dir)
    try:
        meta = await sm.upload_object(
            filename="empty.txt",
            content_type="text/plain",
            file_bytes=b"",
            custom_metadata={"empty": True}
        )
        assert meta["size"] == 0
        assert meta["num_chunks"] == 0
        assert meta["checksum"] == hashlib.sha256(b"").hexdigest()

        sm.verify_object_readable(meta)
        chunks = [c async for c in sm.stream_object_chunks(meta["object_id"])]
        assert b"".join(chunks) == b""
    finally:
        await mock_client.aclose()


# --- 5. Persistent Staging, Crash Recovery, Resumable Uploads & Idempotency ---

@pytest.mark.anyio
async def test_persistent_staging_crash_recovery_and_idempotency(temp_dir):
    db, worker_stores, worker_online, sm, rm, mock_client = _make_mock_cluster(temp_dir)
    try:
        payload = b"RESUMABLE_STAGED_DATA_" * 70000  # ~1.5 MiB -> 2 chunks
        orig_hash = hashlib.sha256(payload).hexdigest()

        # A. Normal upload with Idempotency-Key + Replay returns identical object without duplicating
        upload1 = UploadFile(file=io.BytesIO(payload), filename="report.bin")
        obj1, replay1 = await sm.handle_upload(
            "report.bin", "application/octet-stream", upload1, {"v": 1}, idempotency_key="idem-key-001"
        )
        assert replay1 is False
        assert obj1["checksum"] == orig_hash
        assert obj1["num_chunks"] == 2
        # Staged file must be cleaned up after COMMITTED
        assert list(sm.staging_dir.iterdir()) == []

        upload2 = UploadFile(file=io.BytesIO(payload), filename="report.bin")
        obj2, replay2 = await sm.handle_upload(
            "report.bin", "application/octet-stream", upload2, {"v": 1}, idempotency_key="idem-key-001"
        )
        assert replay2 is True
        assert obj2["object_id"] == obj1["object_id"]
        _, total = db.list_objects()
        assert total == 1

        # B. Concurrent duplicate Idempotency-Key -> 409 Conflict
        db.begin_staged_upload("in_flight_id", str(sm._staged_path("in_flight_id")), idempotency_key="idem-busy")
        sm.active_uploads.add("in_flight_id")
        with pytest.raises(HTTPException) as exc_info:
            await sm.handle_upload(
                "dup.bin", "application/octet-stream",
                UploadFile(file=io.BytesIO(b"abc"), filename="dup.bin"),
                {}, idempotency_key="idem-busy"
            )
        assert exc_info.value.status_code == 409
        sm.active_uploads.discard("in_flight_id")

        # C. Dead STREAMING row (from controller crash before commit point) -> cleared cleanly on retry!
        sm._uploading_path("in_flight_id").write_bytes(b"partial crash bytes")
        obj_retry, replay_retry = await sm.handle_upload(
            "retry.bin", "application/octet-stream",
            UploadFile(file=io.BytesIO(b"fresh data"), filename="retry.bin"),
            {}, idempotency_key="idem-busy"
        )
        assert replay_retry is False
        assert obj_retry["object_id"] != "in_flight_id"
        assert not sm._uploading_path("in_flight_id").exists()

        # D. Worker outage during upload -> durably STAGED (returns 503), then RESUMED via Idempotency-Key!
        for wid in ["worker2", "worker3", "worker4"]:
            db.mark_worker_status(wid, "dead")
            worker_online[wid] = False

        with pytest.raises(HTTPException) as exc_503:
            await sm.handle_upload(
                "resumable.bin", "application/octet-stream",
                UploadFile(file=io.BytesIO(payload), filename="resumable.bin"),
                {"resume": True}, idempotency_key="idem-resume-99"
            )
        assert exc_503.value.status_code == 503

        # Pending staged upload exists on disk, NOT visible in list_objects yet
        staged_rows = db.get_uploads_by_state("STAGED")
        assert len(staged_rows) == 1
        staged_oid = staged_rows[0]["object_id"]
        assert sm._staged_path(staged_oid).is_file()
        assert db.get_object(staged_oid) is None  # not committed yet

        # Bring workers back online and retry with SAME Idempotency-Key -> resumes directly from staged file!
        for wid in ["worker2", "worker3", "worker4"]:
            db.mark_worker_status(wid, "alive")
            worker_online[wid] = True

        resumed_obj, is_replay = await sm.handle_upload(
            "resumable.bin", "application/octet-stream",
            UploadFile(file=io.BytesIO(b""), filename="resumable.bin"),  # doesn't even need re-reading!
            {"resume": True}, idempotency_key="idem-resume-99"
        )
        assert is_replay is False
        assert resumed_obj["object_id"] == staged_oid
        assert resumed_obj["checksum"] == orig_hash
        assert not sm._staged_path(staged_oid).exists()  # cleaned up after commit

        # Verify downloaded bytes match original payload
        downloaded = b"".join([c async for c in sm.stream_object_chunks(staged_oid)])
        assert hashlib.sha256(downloaded).hexdigest() == orig_hash

        # E. Simulate Controller Crash RIGHT after STAGED commit point (before rename) + orphan cleanup
        crash_oid = "crash_oid_01"
        crash_payload = b"CRASH_SURVIVAL_DATA_" * 60000
        crash_hash = hashlib.sha256(crash_payload).hexdigest()
        crash_chunks = (len(crash_payload) + CHUNK_SIZE - 1) // CHUNK_SIZE

        db.begin_staged_upload(crash_oid, str(sm._staged_path(crash_oid)), idempotency_key="idem-crash-01")
        sm._uploading_path(crash_oid).write_bytes(crash_payload)
        db.mark_upload_staged(
            crash_oid, "crashed.bin", len(crash_payload), "application/octet-stream",
            crash_hash, crash_chunks, {}, str(sm._staged_path(crash_oid))
        )
        # Leave an orphan file & a dead STREAMING row to test full recover_staging
        (sm.staging_dir / "random_orphan.tmp").write_bytes(b"orphan")
        db.begin_staged_upload("dead_stream_01", str(sm._staged_path("dead_stream_01")))
        sm._uploading_path("dead_stream_01").write_bytes(b"half")

        rec = sm.recover_staging()
        assert rec["streaming_aborted"] == 1
        assert rec["renames_finished"] == 1
        assert rec["orphans_deleted"] == 1
        assert sm._staged_path(crash_oid).is_file()
        assert not (sm.staging_dir / "random_orphan.tmp").exists()

        # Background resume loop commits the crashed upload automatically!
        await sm.resume_staged_uploads()
        committed_crash = db.get_object(crash_oid)
        assert committed_crash is not None
        assert committed_crash["checksum"] == crash_hash
        assert not sm._staged_path(crash_oid).exists()
    finally:
        await mock_client.aclose()


# --- 6. Fix #3 (Startup Probe), Fix #6 (Surplus Trimming) & Fix #8 (Pre-stream 503) ---

@pytest.mark.anyio
async def test_startup_probe_surplus_trimming_and_prestream_503(temp_dir):
    db, worker_stores, worker_online, sm, rm, mock_client = _make_mock_cluster(temp_dir)
    try:
        # Fix #3: Simulate aged heartbeats after controller downtime (> 15s)
        with db._lock, db._get_connection() as conn:
            conn.execute("UPDATE workers SET last_heartbeat = ?", (time.time() - 60.0,))
            conn.commit()
        worker_online["worker4"] = False  # worker4 is genuinely offline

        await rm.probe_workers_on_startup()
        await rm.check_heartbeats()

        alive_ids = {w["worker_id"] for w in db.get_alive_workers()}
        assert alive_ids == {"worker1", "worker2", "worker3"}
        assert db.get_worker("worker4")["status"] == "dead"

        # Revive worker4
        worker_online["worker4"] = True
        db.register_worker("worker4", "http://worker4:8000")

        # Upload a file, kill one holder, repair (creates 3rd replica), bring holder back, trim surplus!
        obj = await sm.upload_object("trim.bin", "application/octet-stream", b"A" * 5000, {})
        cid = obj["chunks"][0]["chunk_id"]
        holders = obj["chunks"][0]["workers"]
        victim = holders[0]

        db.mark_worker_status(victim, "dead")
        worker_online[victim] = False
        await rm.repair_under_replicated_chunks()

        # Bring victim back -> now chunk has 3 alive replicas
        worker_online[victim] = True
        db.register_worker(victim, f"http://{victim}:8000")
        assert len(db.get_chunk_alive_workers(cid)) == 3

        # Surplus trimmer brings it cleanly back to 2 (Fix #6)
        await rm.trim_surplus_replicas()
        assert len(db.get_chunk_alive_workers(cid)) == 2
        assert len(db.get_over_replicated_chunks(REPLICATION_FACTOR)) == 0

        # Fix #8: Pre-flight 503 if ALL replicas of a chunk are on dead workers
        for wid in db.get_chunk_alive_workers(cid):
            db.mark_worker_status(wid, "dead")
        with pytest.raises(HTTPException) as exc_unavail:
            sm.verify_object_readable(db.get_object(obj["object_id"]))
        assert exc_unavail.value.status_code == 503
    finally:
        await mock_client.aclose()


# --- 7. Full End-to-End Cluster Flow ---

@pytest.mark.anyio
async def test_end_to_end_cluster_flow(temp_dir):
    db, worker_stores, worker_online, sm, rm, mock_client = _make_mock_cluster(temp_dir)
    try:
        # 1. Upload 2.5 MiB file (3 chunks: 1 MiB, 1 MiB, 0.5 MiB)
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
        for c in obj_meta["chunks"]:
            assert len(c["workers"]) == 2
            assert c["workers"][0] != c["workers"][1]

        # 2. Download & verify bit-for-bit integrity
        reassembled = b"".join([c async for c in sm.stream_object_chunks(obj_meta["object_id"])])
        assert hashlib.sha256(reassembled).hexdigest() == orig_hash

        # 3. Worker failure + seamless download + self-healing repair
        db.mark_worker_status("worker2", "dead")
        worker_online["worker2"] = False
        after_fail = b"".join([c async for c in sm.stream_object_chunks(obj_meta["object_id"])])
        assert after_fail == file_bytes

        await rm.repair_under_replicated_chunks()
        assert len(db.get_under_replicated_chunks(required_replicas=2)) == 0

        # 4. Data corruption detection (with cache eviction!) + fallback + self-healing
        chunk_0_id = obj_meta["chunks"][0]["chunk_id"]
        alive_holders = db.get_chunk_alive_workers(chunk_0_id)
        corrupted_worker = alive_holders[0]
        worker_stores[corrupted_worker].corrupt_chunk(chunk_0_id)

        after_corrupt = b"".join([c async for c in sm.stream_object_chunks(obj_meta["object_id"])])
        assert after_corrupt == file_bytes
        assert len(db.get_under_replicated_chunks(required_replicas=2)) >= 1

        worker_online["worker2"] = True
        db.mark_worker_status("worker2", "alive")
        await rm.repair_under_replicated_chunks()
        assert len(db.get_under_replicated_chunks(required_replicas=2)) == 0

        # Trim any surplus copies before final delete check
        await rm.trim_surplus_replicas()

        # 5. Delete object and verify clean worker removal
        assert await sm.delete_object(obj_meta["object_id"]) is True
        assert db.get_object(obj_meta["object_id"]) is None
        for wid, store in worker_stores.items():
            assert len(store.list_chunks()) == 0
    finally:
        await mock_client.aclose()


# --- 8. Multi-Client Concurrent Uploads & Batch Uploads ---

@pytest.mark.anyio
async def test_concurrent_multi_client_and_batch_uploads(temp_dir):
    """
    Verifies that multiple independent clients can upload simultaneously without
    deadlocks, data corruption, worker collision, or race conditions.
    """
    db, worker_stores, worker_online, sm, rm, mock_client = _make_mock_cluster(temp_dir)
    try:
        # 1. Simulate 4 clients uploading files at the exact same moment
        num_clients = 4
        client_files = [
            (f"client_{i}_file.bin", f"PAYLOAD_FROM_CLIENT_{i}_DATA_".encode() * 25000)
            for i in range(num_clients)
        ]

        async def _client_upload_task(fname, data):
            uf = UploadFile(file=io.BytesIO(data), filename=fname)
            obj, _ = await sm.handle_upload(fname, "application/octet-stream", uf, {"client": fname})
            return obj

        # Fire all 4 client uploads concurrently
        uploaded_objs = await asyncio.gather(*(
            _client_upload_task(fname, data) for fname, data in client_files
        ))

        assert len(uploaded_objs) == num_clients
        # Verify unique object IDs
        oids = [obj["object_id"] for obj in uploaded_objs]
        assert len(set(oids)) == num_clients

        # Verify each client's object downloads bit-for-bit
        for (fname, orig_bytes), obj in zip(client_files, uploaded_objs):
            downloaded = b"".join([c async for c in sm.stream_object_chunks(obj["object_id"])])
            assert downloaded == orig_bytes
            assert obj["checksum"] == hashlib.sha256(orig_bytes).hexdigest()
            # Every chunk must have 2 distinct worker replicas
            for ch in obj["chunks"]:
                assert len(ch["workers"]) == 2
                assert ch["workers"][0] != ch["workers"][1]

        # Verify cluster total objects count
        objs, total = db.list_objects()
        assert total == num_clients

    finally:
        await mock_client.aclose()
