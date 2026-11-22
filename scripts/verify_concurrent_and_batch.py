import requests
import asyncio
import httpx
import hashlib
import time
import json

BASE_URL = "http://localhost:8000"

async def test_live_concurrent_and_batch():
    print("=" * 65)
    print("LIVE VERIFICATION: MULTI-FILE BATCH & CONCURRENT CLIENT UPLOADS")
    print("=" * 65)

    # -----------------------------------------------------------------
    # PART 1: NATIVE BATCH UPLOAD (Multiple files in 1 HTTP request)
    # -----------------------------------------------------------------
    print("\n[TEST 1] Testing Multi-File Batch API (POST /api/v1/objects/batch)...")
    batch_files = [
        ("batch_file_1.txt", b"First batch text content " * 1000, "text/plain"),
        ("batch_file_2.png", b"\x89PNG\r\n\x1a\nbatch image binary " * 500, "image/png"),
        ("batch_file_3.bin", b"RAW_BINARY_DATA_" * 50000, "application/octet-stream")  # ~800 KB
    ]

    files_payload = [
        ("files", (fname, data, ctype))
        for fname, data, ctype in batch_files
    ]

    async with httpx.AsyncClient(timeout=30.0) as client:
        res = await client.post(
            f"{BASE_URL}/api/v1/objects/batch",
            files=files_payload,
            data={"metadata": json.dumps({"batch_test": True, "created_by": "multi_client_demo"})}
        )

        assert res.status_code == 201, f"Batch upload failed: {res.text}"
        batch_resp = res.json()
        total_uploaded = batch_resp.get("total_uploaded")
        objs = batch_resp.get("objects", [])

        print(f" -> Batch upload succeeded! Total files uploaded: {total_uploaded}")
        assert total_uploaded == len(batch_files)
        assert len(objs) == len(batch_files)

        # Verify each object from the batch is stored and reassembled correctly
        for (fname, orig_bytes, _), obj in zip(batch_files, objs):
            oid = obj["object_id"]
            dl_res = await client.get(f"{BASE_URL}/api/v1/objects/{oid}")
            assert dl_res.status_code == 200
            assert dl_res.content == orig_bytes
            print(f"    * Stored '{fname}' (ID: {oid}) verified bit-for-bit!")

    # -----------------------------------------------------------------
    # PART 2: CONCURRENT MULTI-CLIENT UPLOADS (5 clients at the same millisecond)
    # -----------------------------------------------------------------
    print("\n[TEST 2] Testing 5 Independent Clients Uploading Simultaneously...")
    num_clients = 5
    client_tasks_data = [
        (f"client_{i}_concurrent.bin", f"CLIENT_{i}_UNIQUE_PAYLOAD_".encode() * 60000)  # ~1.4 MB each -> 2 chunks
        for i in range(1, num_clients + 1)
    ]

    async def _simulated_client(client_id, fname, payload):
        start_t = time.time()
        async with httpx.AsyncClient(timeout=30.0) as client:
            res = await client.post(
                f"{BASE_URL}/api/v1/objects",
                files={"file": (fname, payload, "application/octet-stream")},
                data={"metadata": json.dumps({"client_id": client_id})}
            )
            elapsed = round(time.time() - start_t, 2)
            assert res.status_code == 201, f"Client {client_id} failed: {res.text}"
            obj = res.json()
            print(f" -> Client {client_id} finished in {elapsed}s: Object ID '{obj['object_id']}', {obj['num_chunks']} chunks")
            return obj, payload

    # Fire all 5 clients concurrently
    start_total = time.time()
    results = await asyncio.gather(*(
        _simulated_client(i, fname, payload)
        for i, (fname, payload) in enumerate(client_tasks_data, 1)
    ))
    total_time = round(time.time() - start_total, 2)
    print(f"\nAll {num_clients} client uploads completed in parallel in {total_time}s!")

    # Verify each client's object download
    async with httpx.AsyncClient(timeout=30.0) as client:
        for obj, orig_bytes in results:
            oid = obj["object_id"]
            dl = await client.get(f"{BASE_URL}/api/v1/objects/{oid}")
            assert dl.status_code == 200
            assert hashlib.sha256(dl.content).hexdigest() == hashlib.sha256(orig_bytes).hexdigest()
            # Verify 2x replication across distinct workers
            for chunk in obj["chunks"]:
                assert len(chunk["workers"]) == 2
                assert chunk["workers"][0] != chunk["workers"][1]

    print(f"All {num_clients} objects verified with exact SHA-256 and 2x worker replication!")

    print("\n" + "=" * 65)
    print("ALL TESTS PASSED: MULTI-FILE & CONCURRENT CLIENTS ARE 100% OPERATIONAL!")
    print("=" * 65)

if __name__ == "__main__":
    asyncio.run(test_live_concurrent_and_batch())
