import asyncio
import time
import hashlib
from typing import Optional
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
    def __init__(self, db: Database, storage_manager=None):
        self.db = db
        self.storage_manager = storage_manager
        self.http_client = httpx.AsyncClient(timeout=10.0)
        self.is_running = False
        self._tasks: list[asyncio.Task] = []

    async def probe_workers_on_startup(self):
        """
        Fix #3: On controller restart, heartbeat timestamps in SQLite may have aged past
        HEARTBEAT_TIMEOUT while the controller was down. Probe every known worker's /health
        endpoint first and refresh the ones that respond so healthy workers aren't falsely marked DEAD.
        """
        workers = self.db.get_all_workers()
        if not workers:
            return

        async def _probe(w):
            try:
                resp = await self.http_client.get(f"{w['url']}/health", timeout=2.5)
                if resp.status_code == 200:
                    self.db.record_heartbeat(w["worker_id"])
                    logger.info(f"[STARTUP PROBE] Worker '{w['worker_id']}' responded healthy; refreshed heartbeat.")
            except Exception as e:
                logger.warning(f"[STARTUP PROBE] Worker '{w['worker_id']}' unreachable on startup: {e}")

        await asyncio.gather(*(_probe(w) for w in workers), return_exceptions=True)

    async def _heartbeat_loop(self):
        """
        Independent heartbeat monitor loop (runs every 2s with zero network I/O)
        so slow repair/trim network operations can NEVER delay worker failure detection.
        """
        while self.is_running:
            try:
                await self.check_heartbeats()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Error in heartbeat monitor loop: {e}", exc_info=True)
            await asyncio.sleep(2.0)

    async def _maintenance_loop(self):
        """
        Periodic maintenance loop: resumes staged uploads, repairs under-replicated chunks,
        and trims surplus replicas once replication is healthy.
        """
        while self.is_running:
            try:
                if self.storage_manager is not None:
                    await self.storage_manager.resume_staged_uploads()
                repaired_any = await self.repair_under_replicated_chunks()
                if not repaired_any:
                    await self.trim_surplus_replicas()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Error in maintenance loop: {e}", exc_info=True)
            await asyncio.sleep(REPAIR_INTERVAL)

    async def start(self):
        self.is_running = True
        logger.info("Independent Heartbeat Monitor and Repair/Resume loops started.")
        hb_task = asyncio.create_task(self._heartbeat_loop())
        maint_task = asyncio.create_task(self._maintenance_loop())
        self._tasks = [hb_task, maint_task]
        try:
            await asyncio.gather(hb_task, maint_task)
        except asyncio.CancelledError:
            for t in self._tasks:
                t.cancel()
            await asyncio.gather(*self._tasks, return_exceptions=True)
            raise

    async def stop(self):
        self.is_running = False
        for t in self._tasks:
            t.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
            self._tasks.clear()
        await self.http_client.aclose()
        logger.info("Repair and Heartbeat Monitor loops stopped.")

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

    async def repair_under_replicated_chunks(self) -> bool:
        """
        Finds under-replicated chunks of COMMITTED objects and restores replication factor
        to REPLICATION_FACTOR. Returns True if any under-replicated chunks were found.
        """
        alive_workers = {w["worker_id"]: w for w in self.db.get_alive_workers()}
        if len(alive_workers) < REPLICATION_FACTOR:
            return False

        under_replicated = self.db.get_under_replicated_chunks(required_replicas=REPLICATION_FACTOR)
        if not under_replicated:
            return False

        logger.info(f"[REPAIR] Found {len(under_replicated)} under-replicated chunk(s). Initiating self-healing...")
        unreachable_workers: set[str] = set()

        for chunk in under_replicated:
            chunk_id = chunk["chunk_id"]
            expected_checksum = chunk["checksum"]
            alive_sources = [wid for wid in chunk["alive_workers"] if wid not in unreachable_workers]

            if not alive_sources:
                logger.error(f"[REPAIR] Cannot repair chunk {chunk_id}: No reachable alive workers hold a valid copy!")
                continue

            source_wid = alive_sources[0]
            source_worker = alive_workers.get(source_wid)
            if not source_worker:
                continue

            candidate_targets = [
                w for wid, w in alive_workers.items()
                if wid not in chunk["alive_workers"] and wid not in unreachable_workers
            ]
            if not candidate_targets:
                continue

            target_worker = min(candidate_targets, key=lambda w: w["chunk_count"])
            target_wid = target_worker["worker_id"]

            try:
                # 1. Fetch chunk from healthy source
                fetch_url = f"{source_worker['url']}/internal/chunks/{chunk_id}"
                resp = await self.http_client.get(fetch_url, timeout=4.0)
                if resp.status_code != 200:
                    logger.warning(
                        f"[REPAIR] Failed to fetch chunk {chunk_id} from source {source_wid}: status {resp.status_code}"
                    )
                    continue

                chunk_bytes = resp.content
                # 2. Verify integrity
                if hashlib.sha256(chunk_bytes).hexdigest() != expected_checksum:
                    logger.error(
                        f"[REPAIR] Source chunk {chunk_id} on {source_wid} failed checksum verification! Marking corrupt."
                    )
                    self.db.remove_replica(chunk_id, source_wid)
                    try:
                        await self.http_client.delete(fetch_url, timeout=2.0)
                    except Exception:
                        pass
                    continue

                # 3. Store to target worker with checksum verification header
                put_url = f"{target_worker['url']}/internal/chunks/{chunk_id}"
                try:
                    put_resp = await self.http_client.put(
                        put_url,
                        content=chunk_bytes,
                        headers={
                            "Content-Type": "application/octet-stream",
                            "X-Chunk-Checksum": expected_checksum,
                        },
                        timeout=4.0
                    )
                except httpx.RequestError as e:
                    unreachable_workers.add(target_wid)
                    logger.warning(f"[REPAIR] Target worker {target_wid} unreachable: {e}")
                    continue

                if put_resp.status_code in (200, 201):
                    # 4. Record new replica in DB
                    self.db.add_replica(chunk_id, target_wid)
                    target_worker["chunk_count"] += 1
                    logger.info(
                        f"[REPAIR SUCCESS] Restored chunk '{chunk_id}' replication factor to {REPLICATION_FACTOR}! "
                        f"Copied from {source_wid} -> {target_wid}."
                    )
                else:
                    logger.warning(
                        f"[REPAIR] Failed to store chunk {chunk_id} to {target_wid}: status {put_resp.status_code}"
                    )

            except httpx.RequestError as e:
                unreachable_workers.add(source_wid)
                logger.warning(f"[REPAIR] Source worker {source_wid} unreachable: {e}")
            except Exception as e:
                logger.error(f"[REPAIR ERROR] Failed self-healing for chunk {chunk_id}: {e}")

        return True

    async def trim_surplus_replicas(self):
        """
        Fix #6: Trims surplus replicas (> REPLICATION_FACTOR alive copies) that accumulate
        when a previously dead worker recovers after its chunks were re-replicated elsewhere.
        Removes copies from the most-loaded reachable worker first and skips unreachable nodes.
        """
        over_replicated = self.db.get_over_replicated_chunks(required_replicas=REPLICATION_FACTOR)
        if not over_replicated:
            return

        alive_workers = {w["worker_id"]: w for w in self.db.get_alive_workers()}
        unreachable_workers: set[str] = set()
        logger.info(f"[TRIM] Found {len(over_replicated)} over-replicated chunk(s). Trimming surplus copies...")

        for chunk in over_replicated:
            chunk_id = chunk["chunk_id"]
            holders = [
                alive_workers[wid] for wid in chunk["alive_workers"]
                if wid in alive_workers and wid not in unreachable_workers
            ]
            excess = len(holders) - REPLICATION_FACTOR
            if excess <= 0:
                continue

            # Remove from most-loaded reachable worker(s) first
            holders.sort(key=lambda w: w["chunk_count"], reverse=True)
            trimmed = 0
            for victim in holders:
                if trimmed >= excess:
                    break
                wid = victim["worker_id"]
                if wid in unreachable_workers:
                    continue
                try:
                    del_resp = await self.http_client.delete(
                        f"{victim['url']}/internal/chunks/{chunk_id}",
                        timeout=2.0
                    )
                    if del_resp.status_code in (200, 204, 404):
                        self.db.remove_replica(chunk_id, wid)
                        victim["chunk_count"] = max(0, victim["chunk_count"] - 1)
                        trimmed += 1
                        logger.info(f"[TRIM] Removed surplus copy of chunk '{chunk_id}' from {wid}")
                except httpx.RequestError as e:
                    unreachable_workers.add(wid)
                    logger.warning(f"[TRIM] Skipping unreachable worker {wid}: {e}")
                except Exception as e:
                    logger.warning(f"[TRIM] Failed deleting surplus chunk '{chunk_id}' on {wid}: {e}")

    async def reconcile_worker(self, worker_id: str):
        """
        Fix #4: Reconciles chunks on a re-registered worker: cleans up orphaned chunks,
        while NEVER deleting chunks belonging to active PENDING staged uploads.
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
                    # Orphan chunk: delete from worker disk
                    logger.info(f"[RECONCILE] Deleting orphan chunk {cid} from {worker_id}")
                    await self.http_client.delete(f"{worker['url']}/internal/chunks/{cid}", timeout=2.0)
                elif info.get("object_state") == "PENDING":
                    # In-flight upload chunk: do not touch!
                    continue
                elif worker_id not in info["workers"]:
                    # Chunk exists in DB but this replica was superseded/removed
                    logger.info(f"[RECONCILE] Cleaning up superseded chunk {cid} from {worker_id}")
                    await self.http_client.delete(f"{worker['url']}/internal/chunks/{cid}", timeout=2.0)
        except Exception as e:
            logger.warning(f"[RECONCILE] Error during reconcile for {worker_id}: {e}")
