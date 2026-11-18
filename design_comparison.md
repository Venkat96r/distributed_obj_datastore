# Architecture & Feature Comparison: Your Project vs. Friend's Design

## Executive Summary

- **Course / PDF Specification Coverage**: **Both projects cover 100% of the core requirements** in [`Mini_Distributed_Cloud_Object_Store.pdf`](Mini_Distributed_Cloud_Object_Store.pdf) (1 Controller, 4 Workers, 1 MB chunks, 2x replication, full REST + Internal APIs, Docker Compose with persistent volumes, worker failure recovery, and corruption detection/healing).
- **Key Difference in Scope**:
  - **Your Project** is a **clean, lean, presentation-ready distributed object store** (5 containers, SQLite WAL, interactive Web Dashboard, Swagger UI, automated demo & pytest suite). It directly mirrors [`object_store_walkthrough.md`](object_store_walkthrough.md) and is easy to run, debug, and explain.
  - **Your Friend's Design** goes far beyond the base PDF specification into **production-grade distributed systems engineering** (PostgreSQL state machines, crash-safe disk staging, chain replication, hedged reads, read-ahead, HTTP range requests, worker-to-worker direct repair, rebalancing & draining, background bit-rot scrubbing, LRU caching, backpressure gates, and Prometheus/Grafana/Jaeger observability).

---

## Side-by-Side Comparison Matrix

| Feature / Capability | PDF Spec Required? | Your Implementation | Friend's Design |
|---|:---:|---|---|
| **1 Controller + 4 Workers (Docker Compose)** | Yes | ✅ 5 containers, persistent volumes | ✅ 5 core + Postgres + Monitoring containers |
| **1 MB Chunking & Ordering** | Yes | ✅ `1,048,576` B, `{object_id}_{idx}` | ✅ `1,048,576` B, `{object_id}-{6-digit idx}` |
| **2x Replication on Distinct Workers** | Yes | ✅ Least-loaded placement | ✅ Least-loaded among healthy, non-draining |
| **All Client REST APIs (Upload/Download/Meta/PATCH/Delete/List)** | Yes | ✅ Full FastAPI + `Idempotency-Key` + OpenAPI `/docs` | ✅ Full FastAPI + Idempotency & Range support |
| **All Internal Worker APIs** | Yes | ✅ Register, heartbeat, chunk CRUD, list, `/health` cache stats | ✅ Extended with `/send`, `/drain`, `/scrub-results` |
| **Atomic Worker Disk Writes (`fsync` + rename)** | Recommended | ✅ Unique `.tmp.{pid}.{uuid}` + `X-Chunk-Checksum` verify + `fsync` + `os.replace` | ✅ Temp file + checksum verify + `fsync` + rename |
| **Heartbeat Failure Detection (5s / 15s)** | Yes | ✅ Marks worker `dead` after 15s + startup `/health` probe | ✅ Plus startup `/healthz` probe to prevent false deaths |
| **Auto-Repair of Under-Replicated Chunks** | Yes | ✅ Controller fetches & pushes chunk (`LEFT JOIN`, checksum verified) | ✅ Durable queue (`repair_tasks`) + direct worker-to-worker push |
| **Read-Time Corruption Detection & Failover** | Yes | ✅ SHA-256 check, purges bad copy, falls back, pre-stream `503` | ✅ Size + SHA-256 check, marks `CORRUPT`, queues repair |
| **Interactive Web Dashboard UI** | No (Bonus) | ✅ Built-in UI (upload + Idempotency-Key, download, chunk matrix, PATCH, live cache stats) | ❌ Uses Grafana for metrics (no file browser UI) |
| **Metadata Database** | Any | SQLite (WAL mode, 5 tables incl. `uploads`, `PENDING → COMMITTED`) | PostgreSQL (8+ tables, SQL migrations, state machines) |
| **Bounded Memory on Upload** | No (Bonus) | ✅ Streams multipart in 1 MB slices straight to persistent `/data/staging` | ✅ Streams multipart directly to persistent staging disk |
| **Crash-Safe Resumable Uploads & Idempotency** | No (Bonus) | ✅ `uploads` staging table + startup `recover_staging` + background resume + `Idempotency-Key` | ✅ `uploads` staging table + startup resume + `Idempotency-Key` |
| **Upload Replication Path** | No (Bonus) | Controller → Worker A & Worker B (bounded 4-chunk concurrency + fallback) | ✅ **Chain Replication** (`X-Forward-To`, 1x controller egress) |
| **Hedged Reads (Slow Worker Protection)** | No (Bonus) | ❌ Waits for 10s HTTP timeout on frozen worker | ✅ Adaptive p95 hedge delay (20ms–1s) |
| **Read-Ahead Pipelining** | No (Bonus) | ❌ Sequential chunk streaming | ✅ Fetches 2 chunks ahead while streaming |
| **HTTP Range Requests (`206 Partial Content`)** | No (Bonus) | ❌ Full-object downloads only | ✅ Full RFC 9110 `Range`, `If-Range`, `Accept-Ranges` |
| **Worker-to-Worker Direct Repair** | No (Bonus) | ❌ Repair bytes pass through Controller | ✅ Source worker pushes directly to target (`POST /send`) |
| **Surplus Replica Trimming** | No (Bonus) | ✅ `trim_surplus_replicas()` trims excess copies on most-loaded worker | ✅ Scanner trims excess copies on most-loaded worker |
| **Rebalancing & Dynamic Scale-In (`drain`)** | No (Bonus) | ❌ New chunks balance; old chunks stay put | ✅ Automatic rebalancer + `/drain` & safe worker removal |
| **Background Bit-Rot Scrubber** | No (Bonus) | ❌ Reactive only (detected on read) | ✅ Rate-limited worker scrubber + durable `/data/report_queue` |
| **Caching (Metadata + Chunk LRU)** | No (Bonus) | ✅ Controller LRU (1000 entries, 30s TTL, stale-fill protection) + Worker LRU (64 MiB) | ✅ Controller LRU (1000 entries) + Worker LRU (128 MiB) |
| **Tombstones & Retryable Async Deletion** | No (Bonus) | ⚠️ Immediate DB delete + best-effort worker delete | ✅ `DELETING` state + 24h `tombstones` + background reaper |
| **Backpressure & Concurrency Limits** | No (Bonus) | ❌ No explicit semaphore gates | ✅ `Gate` semaphores + `503 server_busy` + `Retry-After` |
| **Prometheus, Grafana & OpenTelemetry Tracing** | No (Bonus) | ❌ Standard logs + `/internal/status` JSON | ✅ Full Prometheus + Grafana + Jaeger distributed tracing |

---

## Detailed Section-by-Section Breakdown

### 1. Database & State Machine
- **Yours (`controller/database.py`)**: 4 tables (`objects`, `chunks`, `workers`, `replicas`) in SQLite WAL. Objects appear only once fully stored.
- **Friend's**: PostgreSQL with explicit lifecycle states:
  - Objects: `PENDING → COMMITTED → DELETING → DELETED`
  - Replicas: `PENDING / HEALTHY / CORRUPT / MISSING / DELETING`
  - Durable `repair_tasks` queue with a partial unique index (prevents duplicate repairs for the same chunk) and `UNRECOVERABLE` state.
  - `uploads`, `tombstones`, and `scrub_results` tables.

### 2. Upload Flow & Memory Usage
- **Yours (`controller/main.py` & `controller/storage_manager.py`)**:
  - Currently calls `file_bytes = await file.read()`, which buffers the entire uploaded file in Controller RAM before slicing it into 1 MB chunks.
  - Uploads each chunk to both workers concurrently from the Controller (2× network bandwidth out of Controller).
  - If the Controller crashes mid-upload, the upload is lost and the client must retry.
- **Friend's**:
  - Parses multipart incrementally and streams straight to a persistent staging disk file (`<id>.uploading` → `*.staged`). Memory stays bounded even for 384+ MB uploads.
  - Supports `Idempotency-Key` so retrying clients never create duplicate objects.
  - Uses **Chain Replication** (`Controller → Worker 1 → Worker 2` via `X-Forward-To`), cutting controller upload bandwidth in half.
  - Survives controller container restarts during upload via `recover_staging`.

### 3. Download Flow & Tail Latency
- **Yours (`controller/storage_manager.py`)**:
  - Streams 1 chunk at a time (`StreamingResponse` — constant ~1 MB RAM).
  - Checks SHA-256 on every chunk; on error or mismatch, removes the bad replica and fetches the second copy.
- **Friend's**:
  - **Hedged Reads**: Handles *slow/frozen* workers (e.g., `docker pause worker2`) by querying the second replica after an adaptive p95 latency threshold without falsely marking the slow worker dead.
  - **Read-Ahead**: Prefetches the next 2 chunks concurrently while streaming the current chunk to the client.
  - **Range Requests**: Supports `Range: bytes=a-b` (`206 Partial Content`), allowing video seeking and download resume while still validating full chunk checksums.

### 4. Failure Handling, Repair & Rebalancing
- **Yours (`controller/repair_manager.py`)**:
  - Heartbeat monitor marks workers `dead` after 15s.
  - Repair loop runs every 5s, pulling surviving chunk bytes **through the Controller** and uploading them to a healthy worker.
  - Re-registering workers trigger orphan cleanup (`reconcile_worker`).
- **Friend's**:
  - Probes workers on controller startup (`GET /healthz`) so a controller restart doesn't falsely declare all workers dead.
  - **Direct Worker-to-Worker Repair**: Controller tells source worker `POST /internal/chunks/{id}/send` to push straight to the target worker (keeps controller network free).
  - **Periodic Inventory Reconciliation** (every 30s) detects silent file deletions on worker disks.
  - **Rebalancer & Draining**: Automatically rebalances existing chunks when a new worker (`worker5`) joins, and supports graceful scale-in via `POST /internal/workers/{id}/drain` before stopping a node.

### 5. Data Integrity (Proactive vs. Reactive)
- **Yours**: Detects corruption **reactively** when a chunk is downloaded (or read during repair).
- **Friend's**: Adds a **proactive background scrubber** on every worker that continuously hashes cold/unread chunks on disk at a throttled MB/s rate and reports bad files via a durable disk queue (`/data/report_queue`).

### 6. Deletion & Orphan Safety
- **Yours**: Deletes the object from SQLite immediately and sends async `DELETE` calls to workers. If a worker is down during deletion, its stale chunk is cleaned up when it re-registers.
- **Friend's**: Uses soft-delete (`DELETING`), writes **24-hour tombstones**, and runs a background **Reaper** that keeps retrying deletions until every replica confirms removal—eliminating edge-case races where a returning worker's inventory could resurrect deleted chunks.

### 7. Caching & Backpressure
- **Yours**: No caching or concurrency semaphores (simple and always consistent).
- **Friend's**: Adds bounded LRU caches (controller metadata + 128 MiB worker chunk cache with stale-fill protection) and `Gate` concurrency semaphores returning `503` + `Retry-After` under overload.

### 8. Observability & User Experience
- **Yours**: Includes a sleek, custom-built **Web Dashboard** (`http://localhost:8000/dashboard`) where evaluators can visually upload/download files, inspect/PATCH metadata, and see the live chunk-to-worker replica matrix.
- **Friend's**: Uses an industry-standard observability stack (**Prometheus** metrics, **Grafana** auto-provisioned dashboards, and **OpenTelemetry + Jaeger** end-to-end distributed tracing).

---

## Where Your Project Already Shines

1. **Zero-Friction Setup & Presentation**: 5 lightweight containers, zero external DB setup, and an immediate visual Web UI Dashboard for live demos.
2. **100% Spec Alignment**: Every single endpoint, error code, and Section 10 demo requirement is cleanly implemented and verified with automated tests (`pytest` + `demo.py`).
3. **Clean, Readable Codebase**: Easy to walk a professor/interviewer through every line of code without getting lost in infrastructure boilerplate.

---

## Recommended Upgrade Roadmap (If You Want to Extend Yours)

If you want to bridge the biggest gaps and make your main project stand out even more—without overcomplicating everything at once—here is the best order to add features:

### Tier 1 — High Impact, Quick Wins (Great for Viva / Demo)
1. **True Streaming Uploads** — Replace `await file.read()` with chunked streaming reads (`await file.read(CHUNK_SIZE)`) so uploading a 500 MB file uses ~1 MB RAM instead of 500 MB RAM.
2. **Proactive Background Bit-Rot Scrubber** — Add a periodic background task on workers to scan cold chunks, detect silent disk corruption automatically, and report to the controller (aligns well with your automation plans!).
3. **HTTP Range Requests (`206 Partial Content`)** — Enable streaming video/audio directly in the browser from your object store.
4. **Worker-to-Worker Direct Repair** — Add `POST /internal/chunks/{id}/send` on workers so repair traffic bypasses the controller.

### Tier 2 — Impressive Distributed Systems Upgrades
5. **Rebalancing & Graceful Worker Drain (`POST /internal/workers/{id}/drain`)** — Demonstrate scaling from 4 → 5 workers (auto-rebalance) and draining back to 4 with zero downtime.
6. **Hedged Reads** — Handle `docker pause worker2` (slow/frozen worker) in milliseconds instead of waiting for timeouts.
7. **Chain Replication on Upload** — Controller sends to Worker 1, Worker 1 forwards to Worker 2 (`X-Forward-To`).
8. **Soft-Delete Tombstones + Background Reaper** — Guarantee crash-safe deletion even when multiple workers are offline.

### Tier 3 — Production Infrastructure & Observability
9. **Concurrency Gates / Backpressure** — Add `asyncio.Semaphore` limits with `503 Retry-After`.
10. **Prometheus + Grafana Metrics** (or expose live latency/throughput charts right inside your existing Web Dashboard!).
11. **PostgreSQL Migration** — Swap SQLite for PostgreSQL if you want multi-connection concurrency and `SELECT ... FOR UPDATE SKIP LOCKED` durable queues.
