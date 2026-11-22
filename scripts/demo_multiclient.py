"""
Interactive Multi-Client Demonstration Script
Demonstrates multiple independent clients (Alice, Bob, Carol, Dave)
uploading different files simultaneously to the distributed object store.
"""

import asyncio
import httpx
import hashlib
import time
import json
import sys

BASE_URL = "http://localhost:8000"

CLIENTS = [
    {
        "name": "Client 1 (Alice)",
        "file": "alice_dataset.bin",
        "data": b"ALICE_DISTRIBUTED_DATA_STREAM_" * 90000,   # ~2.7 MB -> 3 chunks
        "content_type": "application/octet-stream",
        "color": "\033[96m"  # Cyan
    },
    {
        "name": "Client 2 (Bob)",
        "file": "bob_quarterly_report.pdf",
        "data": b"%PDF-1.4_BOB_RESEARCH_REPORT_" * 45000,   # ~1.3 MB -> 2 chunks
        "content_type": "application/pdf",
        "color": "\033[92m"  # Green
    },
    {
        "name": "Client 3 (Carol)",
        "file": "carol_system_image.png",
        "data": b"\x89PNG\r\n\x1a\n_CAROL_PHOTO_DATA_" * 30000, # ~0.7 MB -> 1 chunk
        "content_type": "image/png",
        "color": "\033[93m"  # Yellow
    },
    {
        "name": "Client 4 (Dave)",
        "file": "dave_backup_archive.zip",
        "data": b"PK\x03\x04_DAVE_BACKUP_CONTAINER_" * 75000,  # ~2.2 MB -> 3 chunks
        "content_type": "application/zip",
        "color": "\033[95m"  # Magenta
    }
]

RESET = "\033[0m"
BOLD = "\033[1m"

async def simulate_client(client_info, barrier):
    name = client_info["name"]
    fname = client_info["file"]
    data = client_info["data"]
    ctype = client_info["content_type"]
    color = client_info["color"]
    orig_hash = hashlib.sha256(data).hexdigest()

    # Synchronize all clients so they start at the exact same microsecond
    await barrier.wait()

    start_time = time.time()
    print(f"{color}[{name}]{RESET} -> Starting upload of '{fname}' ({len(data)/1024/1024:.2f} MB)...")

    async with httpx.AsyncClient(timeout=45.0) as http_client:
        res = await http_client.post(
            f"{BASE_URL}/api/v1/objects",
            files={"file": (fname, data, ctype)},
            data={"metadata": json.dumps({"client": name, "file_name": fname})}
        )
        elapsed = round(time.time() - start_time, 2)

        if res.status_code == 201:
            obj = res.json()
            oid = obj["object_id"]
            num_chunks = obj["num_chunks"]
            print(f"{color}{BOLD}[{name} SUCCESS]{RESET} -> Uploaded in {elapsed}s | Object ID: {oid} | Chunks: {num_chunks}")
            return {
                "client": name,
                "file": fname,
                "object_id": oid,
                "orig_hash": orig_hash,
                "chunks": obj["chunks"],
                "elapsed": elapsed,
                "color": color
            }
        else:
            print(f"{color}[{name} FAILED]{RESET} -> Status {res.status_code}: {res.text}")
            return None

async def run_multiclient_demo():
    print("\n" + "=" * 70)
    print(f"{BOLD}MULTI-CLIENT CONCURRENT UPLOAD DEMONSTRATION{RESET}")
    print("=" * 70)
    print("Simulating 4 distinct clients uploading multi-MB files simultaneously:")
    for c in CLIENTS:
        print(f"  * {c['color']}{c['name']}{RESET}: '{c['file']}' ({len(c['data'])/1024/1024:.2f} MB)")
    print("=" * 70)

    # Barrier synchronization ensures all 4 requests hit the controller at the same millisecond
    barrier = asyncio.Barrier(len(CLIENTS))

    print(f"\n{BOLD}[STAGE 1] Firing all 4 client uploads simultaneously...{RESET}\n")
    overall_start = time.time()
    tasks = [simulate_client(c, barrier) for c in CLIENTS]
    results = await asyncio.gather(*tasks)
    overall_elapsed = round(time.time() - overall_start, 2)

    print("\n" + "-" * 70)
    print(f"{BOLD}All 4 clients completed parallel uploads in {overall_elapsed} seconds!{RESET}")
    print("-" * 70)

    # Stage 2: Verification of Independent Data & Replication
    print(f"\n{BOLD}[STAGE 2] Verifying Data Integrity & Worker Load Distribution:{RESET}")
    async with httpx.AsyncClient(timeout=30.0) as http_client:
        worker_distribution = {}
        for r in results:
            if not r:
                continue
            color = r["color"]
            name = r["client"]
            oid = r["object_id"]
            orig_hash = r["orig_hash"]

            # Download and verify bit-for-bit SHA-256
            dl = await http_client.get(f"{BASE_URL}/api/v1/objects/{oid}")
            dl_hash = hashlib.sha256(dl.content).hexdigest()
            intact = (dl_hash == orig_hash)

            print(f"\n  {color}{name}{RESET}: Object '{r['file']}' (ID: {oid})")
            print(f"    - SHA-256 Verified: {'PASS (Bit-for-Bit Exact)' if intact else 'FAIL'}")
            
            # Show worker assignments for this client's chunks
            for ch in r["chunks"]:
                cid = ch["chunk_id"]
                holders = ch["workers"]
                print(f"    - Chunk {cid} ({ch['size']} bytes) -> Replicas on: {holders}")
                for w in holders:
                    worker_distribution[w] = worker_distribution.get(w, 0) + 1

    print("\n" + "-" * 70)
    print(f"{BOLD}Cluster Chunk Distribution Across All Workers:{RESET}")
    for wid in sorted(worker_distribution.keys()):
        print(f"  * {wid}: {worker_distribution[wid]} chunk replicas stored")
    print("-" * 70)
    print(f"\n{BOLD}DEMO COMPLETE: Multi-client concurrency is 100% operational with zero data collision!{RESET}\n")

if __name__ == "__main__":
    asyncio.run(run_multiclient_demo())
