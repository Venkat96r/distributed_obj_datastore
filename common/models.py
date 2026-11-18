from pydantic import BaseModel, Field
from typing import Dict, Any, List, Optional
import os

# System Parameters according to project specification
CHUNK_SIZE: int = int(os.getenv("CHUNK_SIZE", 1024 * 1024))  # 1 MiB
REPLICATION_FACTOR: int = int(os.getenv("REPLICATION_FACTOR", 2))
HEARTBEAT_INTERVAL: float = float(os.getenv("HEARTBEAT_INTERVAL", 5.0))  # seconds
HEARTBEAT_TIMEOUT: float = float(os.getenv("HEARTBEAT_TIMEOUT", 15.0))  # seconds
REPAIR_INTERVAL: float = float(os.getenv("REPAIR_INTERVAL", 5.0))  # seconds

# Staging & Resumable Upload Configuration
UPLOAD_ABANDON_SECONDS: float = float(os.getenv("UPLOAD_ABANDON_SECONDS", 3600.0))  # 1 hour
UPLOAD_CONCURRENCY: int = int(os.getenv("UPLOAD_CONCURRENCY", 4))

# Cache Configuration
META_CACHE_MAX_ENTRIES: int = int(os.getenv("META_CACHE_MAX_ENTRIES", 1000))
META_CACHE_TTL: float = float(os.getenv("META_CACHE_TTL", 30.0))  # seconds
WORKER_CACHE_MAX_BYTES: int = int(os.getenv("WORKER_CACHE_MAX_BYTES", 64 * 1024 * 1024))  # 64 MiB


class WorkerRegisterRequest(BaseModel):
    worker_id: str
    url: str


class WorkerHeartbeatRequest(BaseModel):
    worker_id: str


class ChunkInfo(BaseModel):
    chunk_id: str
    index: int
    size: int
    checksum: str
    workers: List[str] = Field(default_factory=list)


class ObjectMetadataResponse(BaseModel):
    object_id: str
    name: str
    size: int
    content_type: str
    checksum: str
    num_chunks: int
    created_at: str
    modified_at: str
    custom_metadata: Dict[str, Any] = Field(default_factory=dict)
    chunks: List[ChunkInfo] = Field(default_factory=list)


class ObjectSummary(BaseModel):
    object_id: str
    name: str
    size: int
    content_type: str
    checksum: str
    num_chunks: int
    created_at: str
    modified_at: str
    custom_metadata: Dict[str, Any] = Field(default_factory=dict)


class ListObjectsResponse(BaseModel):
    objects: List[ObjectSummary]
    total: int
    limit: int
    offset: int


class WorkerInfo(BaseModel):
    worker_id: str
    url: str
    last_heartbeat: float
    status: str  # "alive" | "dead"
    chunk_count: int = 0
    seconds_since_heartbeat: float = 0.0


class ClusterStatusResponse(BaseModel):
    controller_status: str
    total_objects: int
    total_chunks: int
    required_replication_factor: int
    alive_workers: int
    total_workers: int
    under_replicated_chunks: int
    pending_uploads: int = 0
    metadata_cache: Dict[str, Any] = Field(default_factory=dict)
    workers: List[WorkerInfo]
