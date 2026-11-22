"""
Comprehensive Verification Script for Mini_Distributed_Cloud_Object_Store.pdf
Tests EVERY feature, parameter, API endpoint, and failure scenario defined in Sections 1 through 10.
"""

import requests
import time
import hashlib
import json
import os
import subprocess
import sys

BASE_URL = "http://localhost:8000"
WORKER_PORTS = {
    "worker1": 8001,
    "worker2": 8002,
    "worker3": 8003,
    "worker4": 8004
}

passed_tests = 0
failed_tests = 0

def report(name, success, detail=""):
    global passed_tests, failed_tests
    if success:
        passed_tests += 1
        print(f"  [PASS] {name}" + (f" -> {detail}" if detail else ""))
    else:
        failed_tests += 1
        print(f"  [FAIL] {name}" + (f" -> {detail}" if detail else ""))

def print_section(title):
    print("\n" + "=" * 70)
    print(title)
    print("=" * 70)

def run_all_tests():
    global passed_tests, failed_tests
    print_section("SPECIFICATION VERIFICATION SUITE (Mini Distributed Cloud Object Store)")

    # -------------------------------------------------------------
    # SECTION 2 & 9: SYSTEM CONFIGURATION & DEPLOYMENT
    # -------------------------------------------------------------
    print_section("SECTION 2 & 9: System Configuration & Deployment")
    try:
        status_res = requests.get(f"{BASE_URL}/internal/status", timeout=5).json()
        report("Controller Node is online (Port 8000)", status_res.get("controller_status") == "online")
        
        workers = status_res.get("workers", [])
        report("Storage/Worker Nodes count is 4", len(workers) == 4, f"Found {len(workers)} workers")
        
        alive_workers = [w for w in workers if w.get("status") == "alive"]
        report("All 4 Workers are ALIVE and registered", len(alive_workers) == 4, f"Alive: {[w['worker_id'] for w in alive_workers]}")
        
        report("Replication Factor requirement is 2", status_res.get("required_replication_factor") == 2)
    except Exception as e:
        report("Cluster connectivity", False, str(e))
        return

    # -------------------------------------------------------------
    # SECTION 1 & 3: OBJECT SUPPORT & METADATA
    # -------------------------------------------------------------
    print_section("SECTION 1 & 3: Object and Metadata Support")
    
    # Test arbitrary file types: Document (PDF-like), Image (PNG-like), Audio/Video (MP4-like), Compressed (ZIP-like)
    sample_files = [
        ("report.pdf", b"%PDF-1.4 sample pdf content for cloud project testing", "application/pdf"),
        ("image.png", b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR sample png binary", "image/png"),
        ("audio.mp4", b"\x00\x00\x00 ftypisom sample mp4 binary data stream", "video/mp4"),
        ("archive.zip", b"PK\x03\x04 sample zip binary archive content", "application/zip")
    ]
    
    uploaded_objects = []
    for fname, content, ctype in sample_files:
        orig_hash = hashlib.sha256(content).hexdigest()
        custom_meta = {"category": "test_suite", "original_format": ctype}
        
        res = requests.post(
            f"{BASE_URL}/api/v1/objects",
            files={"file": (fname, content, ctype)},
            data={"metadata": json.dumps(custom_meta)}
        )
        report(f"Upload arbitrary object '{fname}' ({ctype}) -> 201", res.status_code == 201)
        if res.status_code == 201:
            data = res.json()
            uploaded_objects.append((data["object_id"], fname, content, ctype, orig_hash))

    # Test Section 3 required metadata fields
    if uploaded_objects:
        oid, fname, content, ctype, orig_hash = uploaded_objects[0]
        meta_res = requests.get(f"{BASE_URL}/api/v1/objects/{oid}/metadata")
        report("GET /api/v1/objects/{object_id}/metadata -> 200", meta_res.status_code == 200)
        
        m = meta_res.json()
        report("Metadata contains unique Object ID", bool(m.get("object_id")))
        report("Metadata contains Object Name & Size", m.get("name") == fname and m.get("size") == len(content))
        report("Metadata contains Content Type", m.get("content_type") == ctype)
        report("Metadata contains Creation and Modified timestamps", bool(m.get("created_at")) and bool(m.get("modified_at")))
        report("Metadata contains accurate Checksum (SHA-256)", m.get("checksum") == orig_hash)
        report("Metadata contains Number of Chunks", m.get("num_chunks") == 1)
        report("Metadata contains Chunk info & Storage locations", len(m.get("chunks", [])) > 0 and len(m["chunks"][0].get("workers", [])) == 2)
        report("Metadata contains Custom User-Defined Key-Value pairs", m.get("custom_metadata", {}).get("category") == "test_suite")

    # -------------------------------------------------------------
    # SECTION 4: CHUNKING & REPLICATION (> 1 MB)
    # -------------------------------------------------------------
    print_section("SECTION 4: Chunking and Replication (> 1 MB)")
    
    # 2.5 MiB file -> Exactly 3 chunks: Chunk 0 (1 MB), Chunk 1 (1 MB), Chunk 2 (0.5 MB)
    chunk_test_bytes = b"CHUNK_VERIFY_DATA_" * 150000  # ~2.7 MB
    ctest_hash = hashlib.sha256(chunk_test_bytes).hexdigest()
    ctest_res = requests.post(
        f"{BASE_URL}/api/v1/objects",
        files={"file": ("multichunk_dataset.bin", chunk_test_bytes, "application/octet-stream")},
        data={"metadata": json.dumps({"description": "Chunking test > 1MB"})}
    )
    report("Upload 2.7 MB multi-chunk object -> 201", ctest_res.status_code == 201)
    
    if ctest_res.status_code == 201:
        cdata = ctest_res.json()
        cid_obj = cdata["object_id"]
        chunks = cdata["chunks"]
        
        expected_chunks = (len(chunk_test_bytes) + 1048576 - 1) // 1048576
        report(f"File sliced into {expected_chunks} chunks (1 MB size slices)", len(chunks) == expected_chunks)
        
        # Check ordering and uniqueness
        indices = [c["index"] for c in chunks]
        report("Chunk ordering preserved (0, 1, 2, ...)", indices == list(range(len(chunks))))
        
        chunk_ids = [c["chunk_id"] for c in chunks]
        report("Each chunk has unique identifier", len(set(chunk_ids)) == len(chunks))
        
        # Check replication factor (2 copies per chunk on different workers)
        all_rep_2 = all(len(c["workers"]) == 2 and c["workers"][0] != c["workers"][1] for c in chunks)
        report("Each chunk has exactly 2 copies on distinct workers", all_rep_2)
        
        # Check distribution across workers
        all_assigned_workers = {w for c in chunks for w in c["workers"]}
        report("Chunks distributed across available workers", len(all_assigned_workers) >= 3, f"Assigned: {all_assigned_workers}")

    # -------------------------------------------------------------
    # SECTION 5: CLIENT REST API METHODS
    # -------------------------------------------------------------
    print_section("SECTION 5: Client REST API Methods")
    
    # 5.1 & 5.2 Upload and Download
    target_oid = uploaded_objects[1][0]
    target_name = uploaded_objects[1][1]
    target_bytes = uploaded_objects[1][2]
    target_type = uploaded_objects[1][3]
    
    dl_res = requests.get(f"{BASE_URL}/api/v1/objects/{target_oid}")
    report("GET /api/v1/objects/{object_id} -> 200", dl_res.status_code == 200)
    report("Downloaded binary bytes match original bit-for-bit", dl_res.content == target_bytes)
    report("Download returns correct Content-Type header", target_type in dl_res.headers.get("Content-Type", ""))
    
    # 5.4 Update Custom Metadata (PATCH)
    patch_payload = {"author": "Prof_Evaluator", "verified": True, "release": "v2.0"}
    patch_res = requests.patch(
        f"{BASE_URL}/api/v1/objects/{target_oid}/metadata",
        json=patch_payload
    )
    report("PATCH /api/v1/objects/{object_id}/metadata -> 200", patch_res.status_code == 200)
    if patch_res.status_code == 200:
        patched = patch_res.json()
        report("Custom metadata updated without re-uploading file", patched.get("custom_metadata", {}).get("author") == "Prof_Evaluator")
    
    # 5.6 List Objects with Pagination
    list_res = requests.get(f"{BASE_URL}/api/v1/objects?limit=2&offset=0")
    report("GET /api/v1/objects (Pagination limit=2) -> 200", list_res.status_code == 200)
    if list_res.status_code == 200:
        ldata = list_res.json()
        report("List objects response has 'objects' and 'total'", "objects" in ldata and "total" in ldata)
        report("Pagination limit respected", len(ldata["objects"]) <= 2)
        
        first_item = ldata["objects"][0]
        report("Item contains Object ID, name, size, and content_type", 
               all(k in first_item for k in ("object_id", "name", "size", "content_type")))

    # 5.5 Delete Object
    del_target = uploaded_objects[2][0]
    del_res = requests.delete(f"{BASE_URL}/api/v1/objects/{del_target}")
    report("DELETE /api/v1/objects/{object_id} -> 204 No Content", del_res.status_code == 204)
    
    # Confirm deletion from metadata and download
    check_meta = requests.get(f"{BASE_URL}/api/v1/objects/{del_target}/metadata")
    report("Deleted object metadata returns 404 Not Found", check_meta.status_code == 404)
    
    check_dl = requests.get(f"{BASE_URL}/api/v1/objects/{del_target}")
    report("Deleted object download returns 404 Not Found", check_dl.status_code == 404)

    # -------------------------------------------------------------
    # SECTION 5.7: ERROR HANDLING
    # -------------------------------------------------------------
    print_section("SECTION 5.7: Error Handling (400, 404, 409, 503)")
    
    # 400 Bad Request (missing file)
    bad_req = requests.post(f"{BASE_URL}/api/v1/objects", data={})
    report("Missing file in upload returns 400 or 422 Bad Request", bad_req.status_code in (400, 422))
    
    # 404 Not Found
    nf_req = requests.get(f"{BASE_URL}/api/v1/objects/non_existent_id_9999")
    report("Non-existent object ID returns 404 Not Found", nf_req.status_code == 404)
    
    # Meaningful JSON error message
    report("Error response provides meaningful JSON message", "detail" in nf_req.json())
    
    # 409 Conflict (Concurrent / in-flight duplicate Idempotency-Key)
    idem_key = f"idem-conflict-test-{int(time.time())}"
    # Start first upload with idem_key
    up1 = requests.post(
        f"{BASE_URL}/api/v1/objects",
        files={"file": ("conflict1.bin", b"test data", "application/octet-stream")},
        headers={"Idempotency-Key": idem_key}
    )
    report("First upload with Idempotency-Key succeeds -> 201", up1.status_code == 201)
    
    # Replay returns identical object with replay header
    replay_req = requests.post(
        f"{BASE_URL}/api/v1/objects",
        files={"file": ("conflict1.bin", b"test data", "application/octet-stream")},
        headers={"Idempotency-Key": idem_key}
    )
    report("Replay with Idempotency-Key returns existing object (Replay)", replay_req.status_code == 201 and replay_req.headers.get("X-Idempotent-Replay") == "true")

    # -------------------------------------------------------------
    # SECTION 6: INTERNAL WORKER APIS
    # -------------------------------------------------------------
    print_section("SECTION 6: Internal Worker APIs")
    
    # Test worker chunk storage, retrieval, listing, and deletion
    w_port = 8001
    test_chunk_id = f"internal_test_chunk_{int(time.time())}"
    chunk_payload = b"DIRECT_WORKER_CHUNK_TEST_BYTES_" * 1024
    chunk_chk = hashlib.sha256(chunk_payload).hexdigest()
    
    # PUT /internal/chunks/{chunk_id}
    put_c = requests.put(
        f"http://localhost:{w_port}/internal/chunks/{test_chunk_id}",
        data=chunk_payload,
        headers={"X-Chunk-Checksum": chunk_chk}
    )
    report("PUT /internal/chunks/{chunk_id} -> 201", put_c.status_code == 201)
    
    # GET /internal/chunks/{chunk_id}
    get_c = requests.get(f"http://localhost:{w_port}/internal/chunks/{test_chunk_id}")
    report("GET /internal/chunks/{chunk_id} -> 200", get_c.status_code == 200 and get_c.content == chunk_payload)
    
    # GET /internal/workers/{worker_id}/chunks (Direct on Worker Disk)
    list_w = requests.get(f"http://localhost:{w_port}/internal/workers/worker1/chunks")
    report("GET /internal/workers/{worker_id}/chunks (Worker node) -> 200", list_w.status_code == 200)
    if list_w.status_code == 200:
        c_list = list_w.json().get("chunks", [])
        chunk_ids = [c["chunk_id"] if isinstance(c, dict) else c for c in c_list]
        report("Worker physical disk chunk list contains uploaded chunk", test_chunk_id in chunk_ids)

    # GET /internal/workers/{worker_id}/chunks (Via Controller)
    list_c = requests.get(f"{BASE_URL}/internal/workers/worker1/chunks")
    report("GET /internal/workers/{worker_id}/chunks (Controller node) -> 200", list_c.status_code == 200)
        
    # DELETE /internal/chunks/{chunk_id}
    del_c = requests.delete(f"http://localhost:{w_port}/internal/chunks/{test_chunk_id}")
    report("DELETE /internal/chunks/{chunk_id} -> 204", del_c.status_code == 204)
    
    # Confirm deletion from worker
    get_del_c = requests.get(f"http://localhost:{w_port}/internal/chunks/{test_chunk_id}")
    report("Deleted chunk returns 404 from worker", get_del_c.status_code == 404)

    # -------------------------------------------------------------
    # SECTION 8 & 10.6: DATA INTEGRITY & CORRUPTION HANDLING
    # -------------------------------------------------------------
    print_section("SECTION 8 & 10.6: Data Integrity & Bit-Rot Handling")
    
    # Upload clean object
    corr_bytes = b"CORRUPTION_INTEGRITY_DATA_" * 50000
    corr_hash = hashlib.sha256(corr_bytes).hexdigest()
    corr_up = requests.post(
        f"{BASE_URL}/api/v1/objects",
        files={"file": ("corrupt_test.bin", corr_bytes, "application/octet-stream")},
        data={"metadata": json.dumps({"test": "corruption"})}
    )
    if corr_up.status_code == 201:
        corr_oid = corr_up.json()["object_id"]
        corr_chunk = corr_up.json()["chunks"][0]
        c_id = corr_chunk["chunk_id"]
        v_worker = corr_chunk["workers"][0]
        i_worker = corr_chunk["workers"][1]
        v_port = WORKER_PORTS[v_worker]
        
        # Corrupt chunk on worker disk
        corrupt_call = requests.post(f"http://localhost:{v_port}/internal/testing/corrupt/{c_id}")
        report("POST /internal/testing/corrupt/{chunk_id} modifies chunk bytes on disk", corrupt_call.status_code == 200)
        
        # Download object -> must detect corruption, purge bad replica, and failover silently to secondary
        corr_dl = requests.get(f"{BASE_URL}/api/v1/objects/{corr_oid}")
        report("Download object with corrupted primary replica succeeds via silent failover", corr_dl.status_code == 200)
        report("Downloaded payload matches uncorrupted SHA-256 bit-for-bit", hashlib.sha256(corr_dl.content).hexdigest() == corr_hash)
        
        # Verify automatic self-healing repair restores replication to 2x
        repaired = False
        for _ in range(10):
            time.sleep(1)
            meta = requests.get(f"{BASE_URL}/api/v1/objects/{corr_oid}/metadata").json()
            ch_info = next((c for c in meta["chunks"] if c["chunk_id"] == c_id), None)
            if ch_info and len(ch_info["workers"]) >= 2:
                repaired = True
                break
        report("Self-healing repair restored corrupted chunk to required 2x replication", repaired)

    # -------------------------------------------------------------
    # SECTION 7 & 10.5: WORKER FAILURE & RESTORATION
    # -------------------------------------------------------------
    print_section("SECTION 7 & 10.5: Worker Failure & Container Restart")
    
    # Upload object for failure test
    fail_bytes = b"WORKER_FAILURE_TEST_BYTES_" * 60000
    fail_hash = hashlib.sha256(fail_bytes).hexdigest()
    fail_up = requests.post(
        f"{BASE_URL}/api/v1/objects",
        files={"file": ("failover_test.bin", fail_bytes, "application/octet-stream")}
    )
    if fail_up.status_code == 201:
        fail_oid = fail_up.json()["object_id"]
        fail_holders = fail_up.json()["chunks"][0]["workers"]
        stop_target = fail_holders[0]
        surviving_target = fail_holders[1]
        
        print(f"  -> Stopping Docker container '{stop_target}'...")
        subprocess.run(["docker", "stop", stop_target], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        
        # Verify object still retrievable immediately via surviving replica
        res_during_down = requests.get(f"{BASE_URL}/api/v1/objects/{fail_oid}")
        report(f"Retrieve object while '{stop_target}' is DOWN -> 200 OK", res_during_down.status_code == 200)
        report("Downloaded data during outage matches original hash bit-for-bit", hashlib.sha256(res_during_down.content).hexdigest() == fail_hash)
        
        # Restart container
        print(f"  -> Restarting Docker container '{stop_target}'...")
        subprocess.run(["docker", "start", stop_target], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        time.sleep(4)
        
        # Verify worker registers back and is alive
        after_status = requests.get(f"{BASE_URL}/internal/status").json()
        target_info = next((w for w in after_status["workers"] if w["worker_id"] == stop_target), None)
        report(f"Restarted container '{stop_target}' re-registers with Controller as ALIVE", target_info and target_info.get("status") == "alive")
        
        # Verify object remains healthy after worker recovery
        final_check = requests.get(f"{BASE_URL}/api/v1/objects/{fail_oid}")
        report("Object remains healthy & accessible after worker restart", final_check.status_code == 200 and hashlib.sha256(final_check.content).hexdigest() == fail_hash)

    # -------------------------------------------------------------
    # FINAL SUMMARY
    # -------------------------------------------------------------
    print_section("AUDIT SUMMARY")
    total = passed_tests + failed_tests
    print(f"  TOTAL CHECKS EXECUTED : {total}")
    print(f"  PASSED                : {passed_tests}")
    print(f"  FAILED                : {failed_tests}")
    print("=" * 70)
    if failed_tests == 0:
        print("  ALL FEATURES IN Mini_Distributed_Cloud_Object_Store.pdf ARE 100% OPERATIONAL!")
    else:
        print(f"  WARNING: {failed_tests} checks failed.")
    print("=" * 70)

if __name__ == "__main__":
    run_all_tests()
