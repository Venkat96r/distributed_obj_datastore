import asyncio
import time
import hashlib
from typing import Dict, Any, List
import httpx
import logging

from common.models import (
    HEARTBEAT_TIMEOUT,
    REPAIR_INTERVAL,
    REPLICATION_FACTOR
)
from controller.database import Database

logger = logging.getLogger("controller.repair")


class RepairManager:
    def __init__(self, db: Database):
        self.db = db
        self.http_client = httpx.AsyncClient(timeout=10.0)
        self.is_running = False

    async def start(self):
        self.is_running = True
        logger.info("Repair and Heartbeat Monitor loop started.")
        while self.is_running:
            try:
                await self.check_heartbeats()
                await self.repair_under_replicated_chunks()
            except Exception as e:
                logger.error(f"Error in repair manager cycle: {e}", exc_info=True)

            await asyncio.sleep(REPAIR_INTERVAL)

    async def stop(self):
        self.is_running = False
        await self.http_client.aclose()
        logger.info("Repair and Heartbeat Monitor loop stopped.")

    async def check_heartbeats(self):
        """
        Detects worker failures when last heartbeat exceeds HEARTBEAT_TIMEOUT (15s).
        """
        now = time.time()
        workers = self.db.get_all_workers()

        for w in workers:
            elapsed = now - w["last_heartbeat"]
            if w["status"] == "alive" and elapsed > HEARTBEAT_TIMEOUT:
                logger.warning(
                    f"[HEARTBEAT MONITOR] Worker '{w['worker_id']}' missed heartbeat (last seen {elapsed:.1f}s ago). "
                    f"Marking worker as DEAD."
                )
                self.db.mark_worker_status(w["worker_id"], "dead")

    async def repair_under_replicated_chunks(self):
        """
        Finds under-replicated chunks and restores replication factor to REPLICATION_FACTOR.
        """
        alive_workers = {w["worker_id"]: w for w in self.db.get_alive_workers()}
        if len(alive_workers) < REPLICATION_FACTOR:
            # Cannot restore replication factor if fewer than 2 workers are alive
            return

        under_replicated = self.db.get_under_replicated_chunks(required_replicas=REPLICATION_FACTOR)
        if not under_replicated:
            return

        logger.info(f"[REPAIR] Found {len(under_replicated)} under-replicated chunk(s). Initiating self-healing...")

        for chunk in under_replicated:
            chunk_id = chunk["chunk_id"]
            expected_checksum = chunk["checksum"]
            alive_sources = chunk["alive_workers"]

            if not alive_sources:
                logger.error(f"[REPAIR] Cannot repair chunk {chunk_id}: No alive workers hold a valid copy!")
                continue

            # Pick source worker
            source_wid = alive_sources[0]
            source_worker = alive_workers.get(source_wid)
            if not source_worker:
                continue

            # Pick target worker: alive worker that doesn't already have this chunk
            candidate_targets = [
                w for wid, w in alive_workers.items()
                if wid not in chunk["alive_workers"]
            ]
            if not candidate_targets:
                continue

            # Prefer target with least chunks
            target_worker = min(candidate_targets, key=lambda w: w["chunk_count"])
            target_wid = target_worker["worker_id"]

            try:
                # 1. Fetch chunk from healthy source
                fetch_url = f"{source_worker['url']}/internal/chunks/{chunk_id}"
                resp = await self.http_client.get(fetch_url)
                if resp.status_code != 200:
                    logger.warning(f"[REPAIR] Failed to fetch chunk {chunk_id} from source {source_wid}: status {resp.status_code}")
                    continue

                chunk_bytes = resp.content
                # 2. Verify integrity
                hasher = hashlib.sha256()
                hasher.update(chunk_bytes)
                if hasher.hexdigest() != expected_checksum:
                    logger.error(f"[REPAIR] Source chunk {chunk_id} on {source_wid} failed checksum verification! Marking corrupt.")
                    self.db.remove_replica(chunk_id, source_wid)
                    continue

                # 3. Store to target worker
                put_url = f"{target_worker['url']}/internal/chunks/{chunk_id}"
                put_resp = await self.http_client.put(
                    put_url,
                    content=chunk_bytes,
                    headers={"Content-Type": "application/octet-stream"}
                )

                if put_resp.status_code in (200, 201):
                    # 4. Record new replica in DB
                    self.db.add_replica(chunk_id, target_wid)
                    logger.info(
                        f"[REPAIR SUCCESS] Restored chunk '{chunk_id}' replication factor to {REPLICATION_FACTOR}! "
                        f"Copied from {source_wid} -> {target_wid}."
                    )
                else:
                    logger.warning(f"[REPAIR] Failed to store chunk {chunk_id} to {target_wid}: status {put_resp.status_code}")

            except Exception as e:
                logger.error(f"[REPAIR ERROR] Failed self-healing for chunk {chunk_id}: {e}")

    async def reconcile_worker(self, worker_id: str):
        """
        Reconciles chunks on a re-registered worker: cleans up orphaned chunks.
        """
        worker = self.db.get_worker(worker_id)
        if not worker or worker["status"] != "alive":
            return

        try:
            url = f"{worker['url']}/internal/workers/{worker_id}/chunks"
            resp = await self.http_client.get(url, timeout=5.0)
            if resp.status_code != 200:
                return

            worker_chunks = resp.json().get("chunks", [])
            for c in worker_chunks:
                cid = c["chunk_id"]
                info = self.db.get_chunk_info(cid)
                if not info:
                    # Orphan chunk: delete from worker disk!
                    logger.info(f"[RECONCILE] Deleting orphan chunk {cid} from {worker_id}")
                    await self.http_client.delete(f"{worker['url']}/internal/chunks/{cid}")
                elif worker_id not in info["workers"]:
                    # Chunk exists in DB but this replica was superseded/repaired
                    logger.info(f"[RECONCILE] Cleaning up superseded chunk {cid} from {worker_id}")
                    await self.http_client.delete(f"{worker['url']}/internal/chunks/{cid}")
        except Exception as e:
            logger.warning(f"[RECONCILE] Error during reconcile for {worker_id}: {e}")
