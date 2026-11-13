import os
import sys
import asyncio
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, Response, HTTPException, status
import httpx

# Ensure parent directory is in python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from common.models import (
    CHUNK_SIZE,
    HEARTBEAT_INTERVAL,
    WorkerRegisterRequest,
    WorkerHeartbeatRequest
)
from worker.chunk_store import ChunkStore

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] (%(name)s) %(message)s"
)
logger = logging.getLogger("worker")

# Worker Configuration
WORKER_ID = os.getenv("WORKER_ID", "worker1")
WORKER_PORT = int(os.getenv("PORT", os.getenv("WORKER_PORT", 8001)))
WORKER_HOST = os.getenv("WORKER_HOST", "0.0.0.0")
WORKER_URL = os.getenv("WORKER_URL", f"http://{WORKER_ID}:{WORKER_PORT}")
CONTROLLER_URL = os.getenv("CONTROLLER_URL", "http://controller:8000")
DATA_DIR = os.getenv("DATA_DIR", f"./data/{WORKER_ID}")

chunk_store = ChunkStore(DATA_DIR)


async def heartbeat_loop():
    """
    Background loop that registers the worker and sends heartbeats to the controller every 5 seconds.
    """
    logger.info(f"Starting heartbeat loop for {WORKER_ID} pointing to controller at {CONTROLLER_URL}")
    client = httpx.AsyncClient(timeout=4.0)
    registered = False

    while True:
        try:
            if not registered:
                reg_payload = WorkerRegisterRequest(worker_id=WORKER_ID, url=WORKER_URL).model_dump()
                res = await client.post(f"{CONTROLLER_URL}/internal/workers/register", json=reg_payload)
                if res.status_code in (200, 201):
                    logger.info(f"Registered successfully with controller at {CONTROLLER_URL}")
                    registered = True
                else:
                    logger.warning(f"Registration returned status {res.status_code}: {res.text}")

            if registered:
                hb_payload = WorkerHeartbeatRequest(worker_id=WORKER_ID).model_dump()
                res = await client.post(f"{CONTROLLER_URL}/internal/workers/heartbeat", json=hb_payload)
                if res.status_code == 200:
                    logger.debug(f"Heartbeat sent successfully from {WORKER_ID}")
                elif res.status_code == 404:
                    # Controller restarted or does not recognize worker; trigger re-register
                    logger.info("Controller requested re-registration")
                    registered = False
        except httpx.RequestError as e:
            logger.warning(f"Could not reach controller at {CONTROLLER_URL}: {e}")
            registered = False
        except Exception as e:
            logger.error(f"Unexpected error in heartbeat loop: {e}")

        await asyncio.sleep(HEARTBEAT_INTERVAL)


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(heartbeat_loop())
    try:
        yield
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        logger.info(f"Worker {WORKER_ID} shutdown cleanly.")


app = FastAPI(
    title=f"Mini Object Store - Worker ({WORKER_ID})",
    description="Storage node handling chunk persistence and heartbeats",
    version="1.0.0",
    lifespan=lifespan
)


@app.get("/health")
async def health_check():
    chunks = chunk_store.list_chunks()
    return {
        "status": "ok",
        "worker_id": WORKER_ID,
        "url": WORKER_URL,
        "chunk_count": len(chunks)
    }


@app.put("/internal/chunks/{chunk_id}", status_code=status.HTTP_201_CREATED)
async def store_chunk(chunk_id: str, request: Request):
    """
    Stores a chunk on this worker. Uses atomic file rename.
    """
    body = await request.body()
    if not body:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Chunk payload cannot be empty."
        )

    try:
        size, checksum = chunk_store.save_chunk(chunk_id, body)
        return {
            "chunk_id": chunk_id,
            "worker_id": WORKER_ID,
            "size": size,
            "checksum": checksum,
            "status": "stored"
        }
    except Exception as e:
        logger.error(f"Error storing chunk {chunk_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to store chunk: {str(e)}"
        )


@app.get("/internal/chunks/{chunk_id}")
async def get_chunk(chunk_id: str):
    """
    Retrieves a chunk from this worker.
    """
    data = chunk_store.get_chunk(chunk_id)
    if data is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Chunk '{chunk_id}' not found on worker '{WORKER_ID}'."
        )
    return Response(content=data, media_type="application/octet-stream")


@app.delete("/internal/chunks/{chunk_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_chunk(chunk_id: str):
    """
    Deletes a chunk from this worker.
    """
    deleted = chunk_store.delete_chunk(chunk_id)
    if not deleted:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Chunk '{chunk_id}' not found on worker '{WORKER_ID}'."
        )
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@app.get("/internal/workers/{worker_id}/chunks")
async def list_worker_chunks(worker_id: str):
    """
    Lists all chunks currently stored on this worker.
    """
    chunks = chunk_store.list_chunks()
    return {
        "worker_id": WORKER_ID,
        "total_chunks": len(chunks),
        "chunks": chunks
    }


@app.post("/internal/testing/corrupt/{chunk_id}")
async def corrupt_chunk_endpoint(chunk_id: str):
    """
    Testing endpoint: intentionally flips bytes in the chunk file to simulate bit-rot or disk corruption.
    Demonstrates Section 10 Item 6 of the project requirement.
    """
    success = chunk_store.corrupt_chunk(chunk_id)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Chunk '{chunk_id}' not found to corrupt."
        )
    return {
        "status": "corrupted",
        "chunk_id": chunk_id,
        "worker_id": WORKER_ID,
        "message": "Chunk data intentionally modified on disk."
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("worker.main:app", host=WORKER_HOST, port=WORKER_PORT, log_level="info")
