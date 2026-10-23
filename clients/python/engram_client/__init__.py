"""engram-client — official Python client."""

from engram_client.client import EngramClient, EngramError
from engram_client.models import (
    IngestResponse,
    ModelTokenCall,
    QueryResponse,
    QueryTokenUsage,
    RetrievalMetadata,
    SessionState,
    TenantPayload,
)

__all__ = [
    "EngramClient",
    "EngramError",
    "IngestResponse",
    "ModelTokenCall",
    "QueryResponse",
    "QueryTokenUsage",
    "RetrievalMetadata",
    "SessionState",
    "TenantPayload",
]

__version__ = "0.1.0"
