import os
import sys
import json
import asyncio
import logging
from pathlib import Path
from contextlib import asynccontextmanager
from typing import Optional, Dict, Any

from fastapi import (
    FastAPI,
    File,
    UploadFile,
    Form,
    Header,
    Request,
    Response,
    HTTPException,
    Query,
    status
)
from fastapi.responses import StreamingResponse, HTMLResponse, JSONResponse
from fastapi.exceptions import RequestValidationError

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from common.models import (
    REPLICATION_FACTOR,
    WorkerRegisterRequest,
    WorkerHeartbeatRequest,
    ClusterStatusResponse,
    WorkerInfo
)
from controller.database import Database
from controller.storage_manager import StorageManager
from controller.repair_manager import RepairManager

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] (%(name)s) %(message)s"
)
logger = logging.getLogger("controller")

# Configuration
PORT = int(os.getenv("PORT", 8000))
DATA_DIR = os.getenv("DATA_DIR", "./data/controller")
DB_PATH = os.path.join(DATA_DIR, "metadata.db")
STAGING_DIR = os.path.join(DATA_DIR, "staging")

db = Database(DB_PATH)
storage_manager = StorageManager(db, staging_dir=STAGING_DIR)
repair_manager = RepairManager(db, storage_manager=storage_manager)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 1. Crash recovery of persistent staging directory
    storage_manager.recover_staging()
    # 2. Probe known workers so controller restarts don't falsely mark healthy workers DEAD (Fix #3)
    await repair_manager.probe_workers_on_startup()
    # 3. Start background heartbeat checker, staged-upload resume loop & self-healing repair loop
    repair_task = asyncio.create_task(repair_manager.start())
    logger.info("Controller initialized; staging recovery, startup probe, and repair monitor active.")
    try:
        yield
    finally:
        await repair_manager.stop()
        repair_task.cancel()
        try:
            await repair_task
        except asyncio.CancelledError:
            pass
        await storage_manager.close()
        logger.info("Controller shutdown cleanly.")


app = FastAPI(
    title="Mini Distributed Cloud Object Store - Controller",
    description="S3-compatible distributed object storage controller with persistent staging, idempotent resumable uploads, LRU caching, and self-healing",
    version="2.0.0",
    lifespan=lifespan
)


# Standardize JSON error responses according to spec (Section 5.7)
@app.exception_handler(HTTPException)
async def custom_http_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": "Request Error",
            "status_code": exc.status_code,
            "detail": exc.detail
        }
    )


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content={
            "error": "Invalid Request",
            "status_code": 400,
            "detail": str(exc)
        }
    )


# --- Dashboard Route ---

@app.get("/", response_class=HTMLResponse)
@app.get("/dashboard", response_class=HTMLResponse)
async def get_dashboard():
    template_path = Path(__file__).parent / "templates" / "index.html"
    if template_path.is_file():
        return HTMLResponse(content=template_path.read_text(encoding="utf-8"))
    return HTMLResponse("<h1>Mini Cloud Object Store Controller Online</h1>")


# --- Client REST API (Section 5) ---

@app.post("/api/v1/objects", status_code=status.HTTP_201_CREATED)
async def upload_object(
    file: UploadFile = File(...),
    metadata: Optional[str] = Form(None),
    idempotency_key: Optional[str] = Header(None, alias="Idempotency-Key")
):
    """
    5.1 Upload Object:
    Streams multipart/form-data to persistent staging disk (bounded ~1 MiB RAM),
    supports Idempotency-Key for safe retries and crash-safe resumption,
    slices into 1 MiB chunks, replicates to 2 alive workers, and commits metadata.
    """
    custom_metadata: Dict[str, Any] = {}
    if metadata:
        try:
            custom_metadata = json.loads(metadata)
            if not isinstance(custom_metadata, dict):
                raise ValueError("Metadata must be a JSON object")
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid JSON metadata: {str(e)}"
            )

    content_type = file.content_type or "application/octet-stream"
    filename = file.filename or "unnamed_object"
    clean_key = idempotency_key.strip() if idempotency_key and idempotency_key.strip() else None

    result, is_replay = await storage_manager.handle_upload(
        filename=filename,
        content_type=content_type,
        upload_file=file,
        custom_metadata=custom_metadata,
        idempotency_key=clean_key
    )

    headers = {}
    if is_replay:
        headers["X-Idempotent-Replay"] = "true"

    return JSONResponse(status_code=status.HTTP_201_CREATED, content=result, headers=headers)


@app.get("/api/v1/objects/{object_id}")
async def download_object(object_id: str):
    """
    5.2 Download Object:
    Pre-checks replica availability (returns 503 if unreadable) and streams the
    reassembled original object from chunks with SHA-256 verification and silent failover.
    """
    obj = db.get_object(object_id)
    if not obj:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Object '{object_id}' not found."
        )

    # Pre-flight readability check before sending 200 headers (Fix #8)
    storage_manager.verify_object_readable(obj)

    headers = {
        "Content-Disposition": f'inline; filename="{obj["name"]}"',
        "Content-Length": str(obj["size"]),
        "X-Object-ID": obj["object_id"],
        "X-Object-Checksum": obj["checksum"]
    }

    return StreamingResponse(
        storage_manager.stream_object_chunks(object_id),
        media_type=obj["content_type"],
        headers=headers
    )


@app.get("/api/v1/objects/{object_id}/metadata")
async def get_object_metadata(object_id: str):
    """
    5.3 Get Object Metadata:
    Returns system-maintained and custom metadata without downloading data bytes.
    """
    obj = db.get_object(object_id)
    if not obj:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Object '{object_id}' not found."
        )
    return obj


@app.patch("/api/v1/objects/{object_id}/metadata")
async def update_object_metadata(object_id: str, request: Request):
    """
    5.4 Update Object Metadata:
    Updates custom metadata without re-uploading object data.
    """
    try:
        body = await request.json()
        if not isinstance(body, dict):
            raise ValueError("Payload must be a JSON dictionary")
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid JSON payload: {str(e)}"
        )

    updated_obj = db.update_object_metadata(object_id, body)
    if not updated_obj:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Object '{object_id}' not found."
        )
    return updated_obj


@app.delete("/api/v1/objects/{object_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_object(object_id: str):
    """
    5.5 Delete Object:
    Deletes object and associated stored chunks across workers.
    """
    deleted = await storage_manager.delete_object(object_id)
    if not deleted:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Object '{object_id}' not found."
        )
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@app.get("/api/v1/objects")
async def list_objects(
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0)
):
    """
    5.6 List Objects:
    Returns committed stored objects with pagination.
    """
    objects, total = db.list_objects(limit=limit, offset=offset)
    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "objects": objects
    }


# --- Internal Worker & Observability APIs (Section 6) ---

@app.post("/internal/workers/register", status_code=status.HTTP_200_OK)
async def register_worker(req: WorkerRegisterRequest):
    """
    Register a worker with the Controller.
    """
    db.register_worker(req.worker_id, req.url)
    # Trigger background reconciliation for this worker
    asyncio.create_task(repair_manager.reconcile_worker(req.worker_id))
    return {
        "status": "registered",
        "worker_id": req.worker_id,
        "url": req.url
    }


@app.post("/internal/workers/heartbeat")
async def worker_heartbeat(req: WorkerHeartbeatRequest):
    """
    Report worker availability.
    """
    success = db.record_heartbeat(req.worker_id)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Worker '{req.worker_id}' not recognized. Please register first."
        )
    return {"status": "heartbeat_acknowledged", "worker_id": req.worker_id}


@app.get("/internal/workers/{worker_id}/chunks")
async def get_worker_chunks_endpoint(worker_id: str):
    """
    Retrieve information about chunks stored by a worker (Section 6).
    """
    worker = db.get_worker(worker_id)
    if not worker:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Worker '{worker_id}' not found.")
    chunks = db.get_worker_chunks(worker_id)
    return {
        "worker_id": worker_id,
        "total_chunks": len(chunks),
        "chunks": chunks
    }


@app.get("/internal/uploads")
async def get_pending_uploads():
    """
    Inspect staged / streaming uploads awaiting completion.
    """
    return {
        "streaming": db.get_uploads_by_state("STREAMING"),
        "staged": db.get_uploads_by_state("STAGED"),
    }


@app.get("/internal/status", response_model=ClusterStatusResponse)
async def get_cluster_status():
    """
    Returns full cluster topology, node health, chunk replication status,
    pending staged uploads, and metadata LRU cache metrics.
    """
    workers_raw = db.get_all_workers()
    stats = db.get_cluster_stats()
    under_rep = db.get_under_replicated_chunks(required_replicas=REPLICATION_FACTOR)

    alive_count = sum(1 for w in workers_raw if w["status"] == "alive")
    workers = [WorkerInfo(**w) for w in workers_raw]

    return ClusterStatusResponse(
        controller_status="online",
        total_objects=stats["total_objects"],
        total_chunks=stats["total_chunks"],
        required_replication_factor=REPLICATION_FACTOR,
        alive_workers=alive_count,
        total_workers=len(workers_raw),
        under_replicated_chunks=len(under_rep),
        pending_uploads=db.get_pending_uploads_count(),
        metadata_cache=db.get_cache_stats(),
        workers=workers
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("controller.main:app", host="0.0.0.0", port=PORT, log_level="info")
