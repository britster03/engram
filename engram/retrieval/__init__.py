"""Adaptive retrieval domain and orchestration APIs."""

from engram.retrieval.domain import (
    AnswerabilityState,
    DiscoveryCandidate,
    EvidenceAssessment,
    MemoryRepository,
    RetrievalCandidate,
    RetrievalIntent,
    RetrievalPlan,
    RetrievalRoute,
    TemporalKind,
    TemporalScope,
    VerifiedEvidence,
)
from engram.retrieval.evidence import CanonicalVerifier, EvidenceGate
from engram.retrieval.orchestrator import OrchestratorContext, QueryResult, run_query
from engram.retrieval.planner import AdaptivePlanner

__all__ = [
    "AdaptivePlanner",
    "AnswerabilityState",
    "CanonicalVerifier",
    "DiscoveryCandidate",
    "EvidenceAssessment",
    "EvidenceGate",
    "MemoryRepository",
    "OrchestratorContext",
    "QueryResult",
    "RetrievalCandidate",
    "RetrievalIntent",
    "RetrievalPlan",
    "RetrievalRoute",
    "TemporalKind",
    "TemporalScope",
    "VerifiedEvidence",
    "run_query",
]
