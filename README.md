# Mini Distributed Cloud Object Store (Tiny S3)

A resilient, high-performance distributed object storage system inspired by Amazon S3 and Google Cloud Storage. Built with **Python 3.11**, **FastAPI**, **SQLite (WAL mode)**, and **Docker Compose**.

Designed for cloud computing coursework, production-style demonstrations, and infrastructure evaluation.

---

## Architecture Overview

```
                      ┌────────────────────────────────────────┐
                      │              Client REST API           │
                      │       (/api/v1/objects, /dashboard)    │
                      └───────────────────┬────────────────────┘
                                          │ HTTP (Streaming)
                                          ▼
                                ┌──────────────────┐
                                │    CONTROLLER    │  (Port 8000)
                                │  (Metadata Brain)│  metadata.db (SQLite WAL)
                                └─────────┬────────┘
                                          │
                      Internal Chunk REST │ & Heartbeats (every 5s)
          ┌───────────────────┬───────────┴───────────┬───────────────────┐
          ▼                   ▼                       ▼                   ▼
    ┌───────────┐       ┌───────────┐           ┌───────────┐       ┌───────────┐
    │  worker1  │       │  worker2  │           │  worker3  │       │  worker4  │
    │(Port 8001)│       │(Port 8002)│           │(Port 8003)│       │(Port 8004)│
    │  /data    │       │  /data    │           │  /data    │       │  /data    │
    └───────────┘       └───────────┘           └───────────┘       └───────────┘
     worker1_data        worker2_data            worker3_data        worker4_data
       (Volume)            (Volume)                (Volume)            (Volume)
```

### Core Design Principles
1. **Brain vs. Muscle Separation**:
   - The **Controller** stores only metadata (object records, chunk mappings, worker health). It holds zero payload bytes permanently.
   - The **Workers** are autonomous storage nodes that persist 1 MB chunk files on isolated, persistent Docker volumes.
2. **Chunking & Order Preservation**: Files are sliced into **1 MB chunks** (`{object_id}_{idx}`). Chunks are reassembled on download in exact index order.
3. **2x Replication Factor**: Every chunk is stored across **two distinct workers**. If any worker node crashes or is killed, no data is lost.
4. **Active Self-Healing**: Workers send heartbeats every 5 seconds. If a worker goes silent for >15 seconds, the Controller marks it `DEAD`, identifies all under-replicated chunks, and immediately copies them from surviving replicas to other healthy workers to restore the 2x redundancy.
5. **Data Integrity & Silent Failover**: Every chunk has a SHA-256 fingerprint calculated during upload and verified on read. If corruption occurs (disk bit-rot or tampering), the Controller detects the checksum mismatch, purges the bad replica, silently streams from the healthy secondary replica, and schedules self-healing.
6. **Constant Memory Streaming**: Downloads and uploads use chunked streaming generators (`StreamingResponse`). Controller memory usage stays around ~1 MB regardless of whether a file is 10 MB or 10 GB.
7. **Atomic Disk I/O**: Workers write incoming chunks to temporary files (`.tmp.{pid}`) with `fsync`, followed by an atomic `os.replace` rename, preventing partial or torn writes.

---

## System Configuration

| Parameter | Specification | Implementation |
|---|---|---|
| **Controller Nodes** | 1 | FastAPI daemon managing SQLite WAL metadata DB |
| **Storage / Worker Nodes** | 4 | `worker1`, `worker2`, `worker3`, `worker4` |
| **Clients** | 1 or more | REST API clients (`curl`, Python, Web UI) |
| **Chunk Size** | 1 MB (1,048,576 bytes) | Configurable via `CHUNK_SIZE` |
| **Replication Factor** | 2 | Configurable via `REPLICATION_FACTOR` |
| **Deployment** | Docker & Docker Compose | `docker-compose.yml` with custom bridge network |
| **Worker Storage** | Persistent Storage | Named Docker volumes (`worker1_data` .. `worker4_data`) |
| **Heartbeat Frequency** | Every 5 seconds | Configurable via `HEARTBEAT_INTERVAL` |
| **Heartbeat Timeout** | 15 seconds | Node marked `DEAD` after 15s without ping |

---

## Interactive Web Dashboard

The Controller serves a real-time web dashboard at **`http://localhost:8000/dashboard`** (or **`http://localhost:8000/`**):
- **Live Cluster Topology**: Real-time worker node health badges (`ALIVE` / `DEAD`), last heartbeat timestamps, and chunk counts.
- **Object Explorer**: Upload files via drag-and-drop or file picker, inspect file sizes, and download reassembled files.
- **Visual Chunk Replica Matrix**: Interactive grid visualizing every object's chunks (0, 1, 2, ...) and the exact worker nodes holding replicas.
- **Metadata Inspector & In-Place PATCH**: Modal to inspect system-maintained metadata and update custom key-value JSON tags in place.
- **OpenAPI / Swagger**: Interactive API documentation at **`http://localhost:8000/docs`**.

---

## Client REST API Reference

Structured data uses JSON. Binary data uses the appropriate `Content-Type`.

### 1. Upload Object
- **Method**: `POST /api/v1/objects`
- **Content-Type**: `multipart/form-data`
- **Fields**:
  - `file` *(required)*: Binary file data.
  - `metadata` *(optional)*: JSON string with custom key-value pairs.
- **Response** (`201 Created`):
```json
{
  "object_id": "3116e843d97b",
  "name": "dataset.bin",
  "size": 3670016,
  "content_type": "application/octet-stream",
  "checksum": "81a3d9454db68273a99e5b5d6cad793efd6e5fdcaa0112ebdb117a1d23be7821",
  "num_chunks": 4,
  "created_at": "2026-11-13T10:07:48.726564+00:00",
  "modified_at": "2026-11-13T10:07:48.726564+00:00",
  "custom_metadata": { "author": "Alice", "project": "Cloud" },
  "chunks": [
    { "chunk_id": "3116e843d97b_0", "index": 0, "size": 1048576, "checksum": "...", "workers": ["worker1", "worker4"] },
    { "chunk_id": "3116e843d97b_1", "index": 1, "size": 1048576, "checksum": "...", "workers": ["worker2", "worker3"] },
    { "chunk_id": "3116e843d97b_2", "index": 2, "size": 1048576, "checksum": "...", "workers": ["worker1", "worker4"] },
    { "chunk_id": "3116e843d97b_3", "index": 3, "size": 524288,  "checksum": "...", "workers": ["worker2", "worker3"] }
  ]
}
```

### 2. Download Object
- **Method**: `GET /api/v1/objects/{object_id}`
- **Response** (`200 OK`): Streams original binary content with original `Content-Type` and `Content-Disposition`. Returns `404 Not Found` if missing.

### 3. Get Object Metadata
- **Method**: `GET /api/v1/objects/{object_id}/metadata`
- **Response** (`200 OK`): Full metadata JSON including system metadata and custom attributes without transferring file bytes.

### 4. Update Custom Metadata (In-Place)
- **Method**: `PATCH /api/v1/objects/{object_id}/metadata`
- **Body**: `{"version": "2.0", "status": "approved"}`
- **Response** (`200 OK`): Returns updated metadata object. Data bytes are untouched.

### 5. Delete Object
- **Method**: `DELETE /api/v1/objects/{object_id}`
- **Response** (`204 No Content`): Removes database records and sends asynchronous delete commands to worker nodes. Returns `404 Not Found` if missing.

### 6. List Stored Objects
- **Method**: `GET /api/v1/objects?limit=50&offset=0`
- **Response** (`200 OK`):
```json
{
  "total": 1,
  "limit": 50,
  "offset": 0,
  "objects": [
    {
      "object_id": "3116e843d97b",
      "name": "dataset.bin",
      "size": 3670016,
      "content_type": "application/octet-stream",
      "num_chunks": 4,
      "created_at": "2026-11-13T10:07:48+00:00",
      "modified_at": "2026-11-13T10:07:48+00:00"
    }
  ]
}
```

---

## Internal Worker APIs

| Method | Endpoint | Purpose |
|---|---|---|
| `POST` | `/internal/workers/register` | Register a worker node (`worker_id`, `url`) with the Controller |
| `POST` | `/internal/workers/heartbeat` | Send periodic liveness heartbeat |
| `PUT` | `/internal/chunks/{chunk_id}` | Atomically store a 1 MB chunk on worker disk |
| `GET` | `/internal/chunks/{chunk_id}` | Retrieve chunk binary data |
| `DELETE` | `/internal/chunks/{chunk_id}` | Delete a chunk from worker disk |
| `GET` | `/internal/workers/{worker_id}/chunks` | List chunks stored on worker disk (for reconciliation) |
| `POST` | `/internal/testing/corrupt/{chunk_id}` | Intentionally mutate chunk bytes on disk (integrity demo) |
| `GET` | `/internal/status` | Expose real-time cluster health and chunk replication status |

---

## Quickstart & Deployment

### 1. Prerequisites
- Docker Engine & Docker Compose
- Python 3.10+ (for running the demo client or tests locally)

### 2. Start the Cluster
```bash
docker compose up -d
```

Verify that all 5 containers are running:
```bash
docker compose ps
```

You should see:
- `controller` (port 8000)
- `worker1` (port 8001)
- `worker2` (port 8002)
- `worker3` (port 8003)
- `worker4` (port 8004)

### 3. Open the Web Dashboard
Open your browser to:
```
http://localhost:8000/dashboard
```

---

## Automated Demonstration Suite (Section 10 Evaluation)

The project includes an automated demonstration script `scripts/demo.py` that executes and verifies all 6 evaluation items from **Section 10 of the Project Specification**:

```bash
python scripts/demo.py
```

### What `demo.py` Evaluates:
1. **Object Storage**:
   - Uploads binary data and downloads it.
   - Computes whole-file SHA-256 and proves bit-for-bit exact identity.
2. **Chunking and Distribution (> 1 MB)**:
   - Slices a 3.5 MB file into 4 distinct chunks (1MB, 1MB, 1MB, 0.5MB).
   - Shows chunks distributed across all 4 worker nodes.
3. **Replication**:
   - Verifies each chunk has exactly 2 copies on two different storage nodes.
4. **Metadata Management**:
   - Uploads custom metadata, retrieves it, and issues a `PATCH` request.
   - Proves metadata updates in place without re-uploading file bytes.
5. **Worker Failure & Self-Healing**:
   - Stops `worker2` (`docker stop worker2`).
   - Retrieves object successfully from surviving replicas.
   - Observes Controller detecting `worker2` as `DEAD` after heartbeat timeout.
   - Observes automatic self-healing repair restoring replication factor back to 2 across remaining nodes.
   - Restarts `worker2` (`docker start worker2`) and verifies reconciliation.
6. **Data Integrity & Bit-Rot Protection**:
   - Intentionally mutates bytes in a chunk file on one worker.
   - Downloads object: Controller detects checksum mismatch, discards bad replica, silently delivers valid file from intact replica, and repairs the damage.

---

## Automated Pytest Suite

Run the full automated test suite:
```bash
python -m pytest tests/test_system.py -v
```

Tests cover:
- Atomic file writes and directory traversal protection in `ChunkStore`.
- SQLite ACID transactions and foreign-key cascading in `Database`.
- End-to-end distributed cluster upload, download, failover, and healing.

---

## Project Structure

```
cloud_project/
├── controller/
│   ├── __init__.py
│   ├── main.py              # Controller FastAPI app & REST APIs
│   ├── database.py          # SQLite schema, WAL mode, transactions
│   ├── storage_manager.py   # Chunking, placement, streaming, failover
│   ├── repair_manager.py    # Heartbeat monitoring & self-healing repair loop
│   └── templates/
│       └── index.html       # Modern real-time dark-mode Web Dashboard
├── worker/
│   ├── __init__.py
│   ├── main.py              # Worker node FastAPI daemon & heartbeats
│   └── chunk_store.py       # Atomic file operations (.tmp -> rename), integrity
├── common/
│   ├── __init__.py
│   └── models.py            # Pydantic models, shared constants
├── scripts/
│   └── demo.py              # Section 10 automated demonstration script
├── tests/
│   ├── __init__.py
│   └── test_system.py       # Pytest unit and integration test suite
├── Dockerfile.controller    # Lightweight Python 3.11 image for Controller
├── Dockerfile.worker        # Lightweight Python 3.11 image for Workers
├── docker-compose.yml       # 1 Controller, 4 Workers, named persistent volumes
├── requirements.txt         # Dependencies
├── Mini_Distributed_Cloud_Object_Store.pdf
├── object_store_walkthrough.md
└── README.md
```

---

## Stopping the Cluster

To stop the containers:
```bash
docker compose down
```

To stop containers and wipe persistent data volumes:
```bash
docker compose down -v
```
