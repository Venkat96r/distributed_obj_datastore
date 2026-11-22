import requests
import hashlib
import json
import time

BASE_URL = "http://localhost:8000"

def test_multifile_upload():
    print("=" * 65)
    print("TESTING MULTI-FILE BATCH UPLOAD (POST /api/v1/objects/batch)")
    print("=" * 65)

    # 1. Prepare 4 distinct files (small, medium, large > 1MB, and binary)
    files_data = [
        ("report_doc.pdf", b"%PDF-1.4 MULTIFILE_TEST_DOC_" * 500, "application/pdf"),
        ("notes.txt", b"Hello from multifile upload batch test! " * 300, "text/plain"),
        ("large_dataset.bin", b"LARGE_BINARY_CHUNK_DATA_" * 120000, "application/octet-stream"),  # ~2.88 MB -> 3 chunks
        ("avatar.png", b"\x89PNG\r\n\x1a\nSAMPLE_IMAGE_BYTES_" * 400, "image/png")
    ]

    print("\n[STEP 1] Preparing 4 files for batch upload:")
    hashes = {}
    for name, data, ctype in files_data:
        h = hashlib.sha256(data).hexdigest()
        hashes[name] = h
        print(f" -> '{name}': {len(data)} bytes ({len(data)/1024:.1f} KB), Type: {ctype}")

    # 2. Issue the multi-file batch upload request
    multipart_files = [
        ("files", (name, data, ctype))
        for name, data, ctype in files_data
    ]

    custom_meta = {"uploaded_via": "batch_api", "batch_run_id": f"run_{int(time.time())}"}

    print("\n[STEP 2] Sending POST /api/v1/objects/batch with all 4 files...")
    start_time = time.time()
    res = requests.post(
        f"{BASE_URL}/api/v1/objects/batch",
        files=multipart_files,
        data={"metadata": json.dumps(custom_meta)}
    )
    elapsed = round(time.time() - start_time, 2)

    assert res.status_code == 201, f"Batch upload failed ({res.status_code}): {res.text}"
    response_data = res.json()
    print(f" -> SUCCESS! Received HTTP 201 Created in {elapsed}s")
    print(f" -> Total files uploaded: {response_data.get('total_uploaded')}")

    uploaded_objects = response_data.get("objects", [])
    assert len(uploaded_objects) == 4, f"Expected 4 objects, got {len(uploaded_objects)}"

    # 3. Inspect metadata and chunk replication for each uploaded object
    print("\n[STEP 3] Inspecting metadata & chunk replication for each object:")
    for obj in uploaded_objects:
        oid = obj["object_id"]
        oname = obj["name"]
        osize = obj["size"]
        ochunks = obj["num_chunks"]
        expected_hash = hashes[oname]

        print(f"\n  * Object '{oname}' (ID: {oid}):")
        print(f"    - Size: {osize} bytes")
        print(f"    - Chunks: {ochunks}")
        print(f"    - Checksum: {obj['checksum'][:16]}... (matches expected: {obj['checksum'] == expected_hash})")
        assert obj["checksum"] == expected_hash, f"Checksum mismatch for {oname}"

        # Verify chunks have 2 distinct worker replicas
        for chunk in obj["chunks"]:
            w_holders = chunk["workers"]
            print(f"    - Chunk {chunk['chunk_id']}: {chunk['size']} bytes -> Workers: {w_holders}")
            assert len(w_holders) == 2, f"Chunk {chunk['chunk_id']} has {len(w_holders)} replicas instead of 2"
            assert w_holders[0] != w_holders[1], f"Chunk replicas placed on identical worker!"

    # 4. Download each object individually and verify byte-for-byte integrity
    print("\n[STEP 4] Downloading each object to verify bit-for-bit file integrity:")
    for name, orig_data, ctype in files_data:
        obj = next((o for o in uploaded_objects if o["name"] == name), None)
        assert obj is not None, f"Object {name} missing from uploaded objects list"
        oid = obj["object_id"]

        dl_res = requests.get(f"{BASE_URL}/api/v1/objects/{oid}")
        assert dl_res.status_code == 200, f"Download failed for {oid}: {dl_res.status_code}"
        assert dl_res.content == orig_data, f"Downloaded content mismatch for {name}"
        assert ctype in dl_res.headers.get("Content-Type", ""), f"Content-Type mismatch"
        print(f"  [PASS] '{name}' downloaded successfully and matches original bit-for-bit!")

    # 5. Test Error Handling for Batch API
    print("\n[STEP 5] Testing Batch API Error Handling:")
    # Invalid JSON metadata
    bad_meta_res = requests.post(
        f"{BASE_URL}/api/v1/objects/batch",
        files=[("files", ("test.txt", b"abc", "text/plain"))],
        data={"metadata": "{not_valid_json}"}
    )
    assert bad_meta_res.status_code == 400, f"Expected 400 for bad metadata, got {bad_meta_res.status_code}"
    print(f"  [PASS] Invalid JSON metadata correctly rejected with 400 Bad Request.")

    # 6. Verify Objects appear in Object Explorer List
    print("\n[STEP 6] Verifying Stored Objects List (GET /api/v1/objects):")
    list_res = requests.get(f"{BASE_URL}/api/v1/objects")
    assert list_res.status_code == 200
    all_stored_ids = {o["object_id"] for o in list_res.json()["objects"]}
    for obj in uploaded_objects:
        assert obj["object_id"] in all_stored_ids
    print(f"  [PASS] All {len(uploaded_objects)} batch-uploaded objects visible in Object Explorer.")

    print("\n" + "=" * 65)
    print("RESULT: MULTI-FILE UPLOAD IS FULLY FUNCTIONAL AND 100% VERIFIED!")
    print("=" * 65)

if __name__ == "__main__":
    test_multifile_upload()
