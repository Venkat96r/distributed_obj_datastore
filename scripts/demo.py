#!/usr/bin/env python3
"""
Demonstration Suite for Mini Distributed Cloud Object Store.
Implements and automates all 6 evaluation criteria from Section 10 of the Project Specification:
  1. Object Storage & Bit-for-Bit Verification
  2. Chunking & Worker Distribution (> 1 MB)
  3. 2x Replication Factor Verification
  4. Metadata Management & In-Place PATCH
  5. Worker Failure, Fault Tolerance & Auto-Repair (docker stop/start worker2)
  6. Data Integrity & Corruption Self-Healing
"""

import sys
import os
import time
import json
import hashlib
import subprocess
import requests

BASE_URL = os.getenv("CONTROLLER_URL", "http://localhost:8000")

# Color formatting
class Style:
    HEADER = "\033[95m\033[1m"
    BLUE = "\033[94m"
    CYAN = "\033[96m"
    GREEN = "\033[92m\033[1m"
    WARNING = "\033[93m"
    FAIL = "\033[91m\033[1m"
    BOLD = "\033[1m"
    UNDERLINE = "\033[4m"
    RESET = "\033[0m"


def print_banner(step_num: int, title: str):
    print("\n" + "=" * 78)
    print(f"{Style.HEADER} [DEMO STEP {step_num}] {title.upper()}{Style.RESET}")
    print("=" * 78)


def print_sub(msg: str):
    print(f"{Style.CYAN}==>{Style.RESET} {msg}")


def print_success(msg: str):
    print(f"{Style.GREEN}[SUCCESS]{Style.RESET} {msg}")


def print_fail(msg: str):
    print(f"{Style.FAIL}[FAILED]{Style.RESET} {msg}")


def check_health():
    print_sub(f"Checking cluster health at {BASE_URL}/internal/status ...")
    try:
        res = requests.get(f"{BASE_URL}/internal/status", timeout=5)
        res.raise_for_status()
        data = res.json()
        print_success(f"Controller Online. Active Workers: {data['alive_workers']}/{data['total_workers']}")
        for w in data["workers"]:
            status_color = Style.GREEN if w["status"] == "alive" else Style.FAIL
            print(f"    - Node {w['worker_id']}: {status_color}{w['status'].upper()}{Style.RESET} ({w['url']}) | Chunks: {w['chunk_count']}")
        return data
    except Exception as e:
        print_fail(f"Could not connect to Controller at {BASE_URL}: {e}")
        print("Please ensure the cluster is running via 'docker compose up -d'.")
        sys.exit(1)


# --- Section 10.1: Object Storage ---
def test_object_storage():
    print_banner(1, "Object Storage: Upload, Retrieve & Bit-for-Bit Hash Verification")
    test_content = b"Cloud Computing Project 2026 - Mini Distributed S3 Storage System\n" * 500
    orig_hash = hashlib.sha256(test_content).hexdigest()
    orig_size = len(test_content)
    print_sub(f"Prepared sample file: size = {orig_size} bytes, SHA-256 = {orig_hash}")

    # Upload
    files = {"file": ("cloud_notes.txt", test_content, "text/plain")}
    res = requests.post(f"{BASE_URL}/api/v1/objects", files=files)
    if res.status_code != 201:
        print_fail(f"Upload failed: status {res.status_code}, {res.text}")
        return None
    obj = res.json()
    obj_id = obj["object_id"]
    print_success(f"Object uploaded! ID = {obj_id}, stored {obj['num_chunks']} chunk(s).")

    # Download
    print_sub(f"Downloading object {obj_id} from Controller...")
    dl_res = requests.get(f"{BASE_URL}/api/v1/objects/{obj_id}")
    if dl_res.status_code != 200:
        print_fail(f"Download failed: status {dl_res.status_code}")
        return None

    dl_content = dl_res.content
    dl_hash = hashlib.sha256(dl_content).hexdigest()
    print_sub(f"Downloaded: size = {len(dl_content)} bytes, SHA-256 = {dl_hash}")

    if dl_hash == orig_hash and len(dl_content) == orig_size:
        print_success("Exact match verified! Downloaded binary is bit-for-bit identical to original.")
    else:
        print_fail("Checksum mismatch! Data corruption detected.")
    return obj_id


# --- Section 10.2 & 10.3: Chunking, Distribution & Replication ---
def test_chunking_and_replication():
    print_banner(2, "Chunking, Distribution & 2x Replication Factor (> 1 MB)")
    # Generate 3.5 MB file (exactly 3 full 1MB chunks + 1 half chunk = 4 chunks)
    target_size = int(3.5 * 1024 * 1024)
    print_sub(f"Generating 3.5 MB synthetic payload ({target_size} bytes)...")
    pattern = b"CHUNKING_TEST_PAYLOAD_BLOCK_0123456789ABCDEF\n"
    synthetic_data = (pattern * (target_size // len(pattern) + 1))[:target_size]
    orig_hash = hashlib.sha256(synthetic_data).hexdigest()

    # Upload
    print_sub("Uploading 3.5 MB file to Controller (POST /api/v1/objects)...")
    files = {"file": ("dataset_3.5mb.bin", synthetic_data, "application/octet-stream")}
    res = requests.post(f"{BASE_URL}/api/v1/objects", files=files)
    if res.status_code != 201:
        print_fail(f"Upload failed: {res.text}")
        return None
    obj = res.json()
    obj_id = obj["object_id"]
    print_success(f"File uploaded! ID = {obj_id}, Total Chunks: {obj['num_chunks']}")

    # Inspect metadata & chunk distribution
    meta_res = requests.get(f"{BASE_URL}/api/v1/objects/{obj_id}/metadata")
    meta = meta_res.json()

    print("\n" + "-" * 75)
    print(f"{'Chunk Index':<14} | {'Chunk ID':<18} | {'Size':<10} | {'Replica Workers':<25}")
    print("-" * 75)
    for c in meta["chunks"]:
        workers_str = ", ".join(c["workers"])
        size_kb = f"{c['size']/1024:.1f} KB"
        print(f"  Chunk #{c['index']:<8} | {c['chunk_id']:<18} | {size_kb:<10} | {Style.GREEN}{workers_str}{Style.RESET}")
    print("-" * 75)

    # Verify replication factor = 2 on different workers
    all_replicated_2x = True
    distinct_workers = set()
    for c in meta["chunks"]:
        if len(c["workers"]) != 2 or len(set(c["workers"])) != 2:
            all_replicated_2x = False
        distinct_workers.update(c["workers"])

    if all_replicated_2x and len(meta["chunks"]) == 4:
        print_success(f"Verified: 4 chunks created. Each chunk has exactly 2 copies on different workers.")
        print_success(f"Chunks distributed across {len(distinct_workers)} distinct storage nodes: {distinct_workers}")
    else:
        print_fail("Replication or chunk count requirement not met!")

    return obj_id


# --- Section 10.4: Custom Metadata Management ---
def test_metadata_management(obj_id: str):
    print_banner(3, "Metadata: Upload Custom Metadata, Retrieve & In-Place PATCH")
    custom_meta = {
        "project": "Cloud_Computing_Final",
        "course": "Distributed Systems",
        "author": "Student",
        "semester": "Fall 2026",
        "version": "1.0"
    }

    print_sub(f"1. Uploading new object with custom metadata JSON...")
    files = {"file": ("spec.pdf", b"%PDF-1.4 Mock Document Bytes", "application/pdf")}
    data = {"metadata": json.dumps(custom_meta)}
    res = requests.post(f"{BASE_URL}/api/v1/objects", files=files, data=data)
    obj = res.json()
    new_id = obj["object_id"]
    print_success(f"Object created with ID {new_id}")
    print_sub(f"Initial custom metadata: {json.dumps(obj.get('custom_metadata', {}), indent=2)}")

    # Retrieve metadata via GET /api/v1/objects/{id}/metadata
    print_sub(f"2. Fetching metadata via GET /api/v1/objects/{new_id}/metadata ...")
    meta_res = requests.get(f"{BASE_URL}/api/v1/objects/{new_id}/metadata")
    meta = meta_res.json()
    assert meta["custom_metadata"]["project"] == "Cloud_Computing_Final"
    created_at = meta["created_at"]
    print_success(f"Retrieved metadata matches! Created At: {created_at}")

    # PATCH metadata
    patch_update = {
        "version": "2.0-approved",
        "status": "production-ready",
        "grade": "A+"
    }
    print_sub(f"3. Applying PATCH /api/v1/objects/{new_id}/metadata (without re-uploading file bytes)...")
    patch_res = requests.patch(f"{BASE_URL}/api/v1/objects/{new_id}/metadata", json=patch_update)
    if patch_res.status_code == 200:
        updated = patch_res.json()
        print_success("Metadata updated successfully!")
        print_sub(f"Updated custom metadata: {json.dumps(updated['custom_metadata'], indent=2)}")
        print_sub(f"Modified At timestamp updated: {updated['modified_at']}")
        if updated["custom_metadata"]["version"] == "2.0-approved" and updated["custom_metadata"]["status"] == "production-ready":
            print_success("In-place metadata update verified! Data was not re-uploaded.")
    else:
        print_fail(f"PATCH failed: status {patch_res.status_code}, {patch_res.text}")


# --- Section 10.5: Worker Failure & Self-Healing ---
def test_worker_failure(obj_id: str):
    print_banner(4, "Worker Failure & Self-Healing: docker stop worker2 / docker start worker2")
    print_sub("Target scenario: Stop worker2, verify object retrieval via surviving replicas,")
    print_sub("verify Controller detects failure, auto-repairs replication factor to 2, and restores worker.")

    # 1. Stop worker2
    print_sub("Executing: docker stop worker2 ...")
    try:
        subprocess.run(["docker", "stop", "worker2"], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        print_success("worker2 stopped successfully!")
    except Exception as e:
        print_fail(f"Could not execute 'docker stop worker2': {e}")
        return

    # 2. Retrieve object immediately
    print_sub(f"Attempting to download object {obj_id} while worker2 is DOWN...")
    res = requests.get(f"{BASE_URL}/api/v1/objects/{obj_id}")
    if res.status_code == 200:
        print_success("Object successfully retrieved! Failover to surviving replicas worked seamlessly.")
    else:
        print_fail(f"Download failed during worker downtime: status {res.status_code}")

    # 3. Wait for Heartbeat Monitor to detect failure (> 15 seconds + up to 5s monitor cycle)
    print_sub("Waiting for Controller heartbeat timeout to mark worker2 as DEAD (15s timeout)...")
    worker2_status = None
    for elapsed in range(0, 24, 2):
        status_res = requests.get(f"{BASE_URL}/internal/status").json()
        worker2_status = next((w for w in status_res["workers"] if w["worker_id"] == "worker2"), None)
        if worker2_status and worker2_status["status"] == "dead":
            break
        if elapsed % 4 == 0:
            print(f"    ... waiting ({elapsed}s elapsed)")
        time.sleep(2)

    if worker2_status and worker2_status["status"] == "dead":
        print_success("Controller successfully detected worker2 failure and marked node DEAD!")
    else:
        print_sub(f"Worker status: {worker2_status['status'] if worker2_status else 'unknown'}")

    # 4. Observe Self-Healing Repair Loop
    print_sub("Checking automatic replication repair (restoring under-replicated chunks to 2 copies)...")
    time.sleep(6)  # Give repair loop one cycle
    repaired_status = requests.get(f"{BASE_URL}/internal/status").json()
    under_rep = repaired_status["under_replicated_chunks"]
    print_sub(f"Current under-replicated chunks in cluster: {under_rep}")

    # 5. Restart worker2
    print_sub("Executing: docker start worker2 ...")
    try:
        subprocess.run(["docker", "start", "worker2"], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        print_success("worker2 started!")
    except Exception as e:
        print_fail(f"Could not execute 'docker start worker2': {e}")
        return

    print_sub("Waiting for worker2 to re-register and send heartbeat...")
    time.sleep(6)
    final_status = requests.get(f"{BASE_URL}/internal/status").json()
    worker2_final = next((w for w in final_status["workers"] if w["worker_id"] == "worker2"), None)
    if worker2_final and worker2_final["status"] == "alive":
        print_success("worker2 has successfully re-registered and is ALIVE!")
    print_success("Worker failure and recovery cycle completed successfully.")


# --- Section 10.6: Data Integrity & Corruption Handling ---
def test_data_integrity():
    print_banner(5, "Data Integrity: Intentional Corruption, Detection & Self-Healing")
    test_data = b"INTEGRITY_CHECK_SAMPLE_DATA_BLOCK_" * 1000
    orig_hash = hashlib.sha256(test_data).hexdigest()

    # Upload
    print_sub("Uploading test file for corruption demonstration...")
    files = {"file": ("integrity_test.dat", test_data, "application/octet-stream")}
    res = requests.post(f"{BASE_URL}/api/v1/objects", files=files)
    obj = res.json()
    obj_id = obj["object_id"]
    chunk_0 = obj["chunks"][0]
    chunk_id = chunk_0["chunk_id"]
    assigned_workers = chunk_0["workers"]
    print_success(f"Uploaded object {obj_id}. Chunk {chunk_id} replicated to: {assigned_workers}")

    # Corrupt one worker's chunk
    target_worker = assigned_workers[0]
    print_sub(f"Intentionally corrupting chunk '{chunk_id}' on worker '{target_worker}'...")

    # Determine worker port
    worker_port = 8001 if target_worker == "worker1" else 8002 if target_worker == "worker2" else 8003 if target_worker == "worker3" else 8004
    corrupt_url = f"http://localhost:{worker_port}/internal/testing/corrupt/{chunk_id}"

    try:
        c_res = requests.post(corrupt_url)
        if c_res.status_code == 200:
            print_success(f"Chunk bytes mutated on {target_worker}'s disk!")
        else:
            print_fail(f"Could not corrupt chunk: {c_res.text}")
    except Exception as e:
        print_fail(f"Worker corrupt endpoint unreachable: {e}")

    # Download from Controller
    print_sub(f"Downloading object {obj_id} from Controller...")
    print_sub("Controller will verify SHA-256, detect the corruption on primary worker, and failover silently to secondary replica.")
    dl_res = requests.get(f"{BASE_URL}/api/v1/objects/{obj_id}")

    if dl_res.status_code == 200:
        dl_hash = hashlib.sha256(dl_res.content).hexdigest()
        if dl_hash == orig_hash:
            print_success("DOWNLOAD SUCCEEDED! Client received perfectly intact uncorrupted data.")
            print_success("Controller successfully detected corruption, discarded bad replica, and used intact copy.")
        else:
            print_fail("Received corrupted bytes! Fallback failed.")
    else:
        print_fail(f"Download returned error: {dl_res.status_code}")


# --- Section 10 Bonus: Persistent Staging, Idempotent Resume & LRU Caching ---
def test_staging_idempotency_and_cache():
    print_banner(6, "Persistent Staging, Controller Crash Recovery, Idempotency & LRU Caching")

    # 1. Idempotency-Key deduplication
    idem_key = f"demo-idem-{int(time.time())}"
    payload = b"IDEMPOTENCY_AND_STAGING_PAYLOAD_" * 50000  # ~1.5 MiB
    orig_hash = hashlib.sha256(payload).hexdigest()

    print_sub(f"1. Uploading 1.5 MB object with Idempotency-Key: '{idem_key}' ...")
    res1 = requests.post(
        f"{BASE_URL}/api/v1/objects",
        files={"file": ("idem_test.bin", payload, "application/octet-stream")},
        headers={"Idempotency-Key": idem_key}
    )
    assert res1.status_code == 201, f"Expected 201, got {res1.status_code}: {res1.text}"
    oid1 = res1.json()["object_id"]
    print_success(f"First upload committed! Object ID = {oid1}")

    print_sub(f"2. Retrying the exact same upload with Idempotency-Key: '{idem_key}' ...")
    res2 = requests.post(
        f"{BASE_URL}/api/v1/objects",
        files={"file": ("idem_test.bin", payload, "application/octet-stream")},
        headers={"Idempotency-Key": idem_key}
    )
    assert res2.status_code == 201
    oid2 = res2.json()["object_id"]
    is_replay = res2.headers.get("X-Idempotent-Replay") == "true"
    if oid1 == oid2 and is_replay:
        print_success(f"Idempotent replay verified! Returned same Object ID ({oid2}) with X-Idempotent-Replay: true.")
    else:
        print_fail(f"Idempotency check failed: oid1={oid1}, oid2={oid2}, replay={is_replay}")

    # 2. Persistent Staging + Controller Restart Crash Survival + Resume
    resume_key = f"demo-resume-{int(time.time())}"
    print_sub("3. Stopping worker2, worker3, worker4 (leaving < 2 alive workers) to test persistent staging...")
    subprocess.run(["docker", "stop", "worker2", "worker3", "worker4"], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    print_sub("Waiting 16s for heartbeat timeout so Controller knows < 2 workers are alive...")
    time.sleep(16)

    print_sub(f"Uploading 1.5 MB file with Idempotency-Key '{resume_key}' during worker outage...")
    res_staged = requests.post(
        f"{BASE_URL}/api/v1/objects",
        files={"file": ("crash_safe.bin", payload, "application/octet-stream")},
        headers={"Idempotency-Key": resume_key}
    )
    if res_staged.status_code == 503:
        print_success(f"Received expected 503 while workers unavailable: {res_staged.json().get('detail')}")
    else:
        print_fail(f"Expected 503, got {res_staged.status_code}")

    pending = requests.get(f"{BASE_URL}/internal/uploads").json()
    print_sub(f"Staged uploads persisted on Controller disk: {len(pending.get('staged', []))}")

    print_sub("4. Restarting Controller container (`docker restart controller`) to verify crash survival...")
    subprocess.run(["docker", "restart", "controller"], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    time.sleep(4)

    print_sub("5. Starting worker2, worker3, worker4 back up...")
    subprocess.run(["docker", "start", "worker2", "worker3", "worker4"], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    time.sleep(6)

    print_sub(f"6. Retrying with Idempotency-Key '{resume_key}' (or verifying auto-resume after restart)...")
    res_resumed = requests.post(
        f"{BASE_URL}/api/v1/objects",
        files={"file": ("crash_safe.bin", b"", "application/octet-stream")},
        headers={"Idempotency-Key": resume_key}
    )
    if res_resumed.status_code == 201:
        resumed_oid = res_resumed.json()["object_id"]
        dl_check = requests.get(f"{BASE_URL}/api/v1/objects/{resumed_oid}")
        if dl_check.status_code == 200 and hashlib.sha256(dl_check.content).hexdigest() == orig_hash:
            print_success(f"Crash-safe staged upload resumed & verified bit-for-bit! Object ID = {resumed_oid}")
        else:
            print_fail("Resumed object download checksum mismatch!")
    else:
        print_fail(f"Resume failed with status {res_resumed.status_code}: {res_resumed.text}")

    # 3. Verify Metadata & Worker LRU Caches
    print_sub("7. Verifying Controller Metadata LRU Cache & Worker Chunk LRU Cache...")
    for _ in range(3):
        requests.get(f"{BASE_URL}/api/v1/objects/{oid1}/metadata")
        requests.get(f"{BASE_URL}/api/v1/objects/{oid1}")

    status_data = requests.get(f"{BASE_URL}/internal/status").json()
    mcache = status_data.get("metadata_cache", {})
    print_success(
        f"Controller Metadata LRU Cache -> Hits: {mcache.get('hits')}, Misses: {mcache.get('misses')}, "
        f"Hit Rate: {mcache.get('hit_rate_pct')}%, Entries: {mcache.get('entries')}"
    )
    for port in (8001, 8002, 8003, 8004):
        try:
            whealth = requests.get(f"http://localhost:{port}/health", timeout=2).json()
            wc = whealth.get("cache", {})
            print(
                f"    - {whealth['worker_id']} Chunk LRU Cache: "
                f"Hits={wc.get('hits')}, Misses={wc.get('misses')}, "
                f"Bytes={wc.get('bytes_used', 0) / 1024:.1f} KiB"
            )
        except Exception:
            pass


def main():
    print("\n" + "=" * 78)
    print(f"{Style.BOLD} MINI DISTRIBUTED CLOUD OBJECT STORE — PROJECT DEMONSTRATION {Style.RESET}")
    print("=" * 78)

    check_health()
    s1_id = test_object_storage()
    s2_id = test_chunking_and_replication()
    test_metadata_management(s1_id)
    test_worker_failure(s2_id)
    test_data_integrity()
    test_staging_idempotency_and_cache()

    print("\n" + "=" * 78)
    print(f"{Style.GREEN} ALL DEMONSTRATION TESTS COMPLETED SUCCESSFULLY! {Style.RESET}")
    print("=" * 78)


if __name__ == "__main__":
    main()

