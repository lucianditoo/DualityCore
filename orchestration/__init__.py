"""Duality Multi-Agent Orchestration Layer (DMAOL) - Public Package API."""
from orchestration.errors import (
    DualityError,
    TransientError,
    PermanentError,
    IllegalStateTransitionError,
    FencingTokenMismatchError,
    LeaseExpiredError,
    ResourceConflictError,
    QuarantineRequiredError,
    SecurityViolationError,
    PathTraversalError,
    IdempotencyConflictError,
)
from orchestration.models import (
    TaskPhase,
    TaskLease,
    ExecutionResult,
    HumanApproval,
    GitMergeRecord,
    RetryPolicy,
    TaskEnvelope,
    compute_payload_digest,
    compute_approval_signature,
)
from orchestration.lock import CrossPlatformFileLock
from orchestration.path_security import PathSecurity
from orchestration.persistence import TaskStore
from orchestration.lease_manager import LeaseManager
from orchestration.git_policy import GitPolicy
from orchestration.contracts import (
    SessionLifecycleDriver,
    SimulatedSessionDriver,
    AgentAdapter,
    SimulatedAgentAdapter,
    HumanGateProvider,
    CryptographicHumanGate,
)

__version__ = "0.1.1-alpha"

__all__ = [
    "DualityError",
    "TransientError",
    "PermanentError",
    "IllegalStateTransitionError",
    "FencingTokenMismatchError",
    "LeaseExpiredError",
    "ResourceConflictError",
    "QuarantineRequiredError",
    "SecurityViolationError",
    "PathTraversalError",
    "IdempotencyConflictError",
    "TaskPhase",
    "TaskLease",
    "ExecutionResult",
    "HumanApproval",
    "GitMergeRecord",
    "RetryPolicy",
    "TaskEnvelope",
    "compute_payload_digest",
    "compute_approval_signature",
    "CrossPlatformFileLock",
    "PathSecurity",
    "TaskStore",
    "LeaseManager",
    "GitPolicy",
    "SessionLifecycleDriver",
    "SimulatedSessionDriver",
    "AgentAdapter",
    "SimulatedAgentAdapter",
    "HumanGateProvider",
    "CryptographicHumanGate",
]
