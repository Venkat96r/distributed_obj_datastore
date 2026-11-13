from pydantic import BaseModel, Field
from typing import Dict, Any, List, Optional
import os

# System Parameters according to project specification
CHUNK_SIZE: int = int(os.getenv("CHUNK_SIZE", 1024 * 1024))  # 1 MB
REPLICATION_FACTOR: int = int(os.getenv("REPLICATION_FACTOR", 2))
HEARTBEAT_INTERVAL: float = float(os.getenv("HEARTBEAT_INTERVAL", 5.0))  # seconds
HEARTBEAT_TIMEOUT: float = float(os.getenv("HEARTBEAT_TIMEOUT", 15.0))  # seconds
REPAIR_INTERVAL: float = float(os.getenv("REPAIR_INTERVAL", 5.0))  # seconds


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
    workers: List[WorkerInfo]
