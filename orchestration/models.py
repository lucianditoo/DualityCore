"""Duality Multi-Agent Orchestration Layer (DMAOL) - Data Models and State Machine."""
from dataclasses import dataclass, field, asdict
from enum import Enum
import hashlib
import hmac
import json
import time
from typing import List, Dict, Optional, Any, Set
import uuid

from orchestration.errors import (
    IllegalStateTransitionError,
    FencingTokenMismatchError,
    LeaseExpiredError,
    SecurityViolationError,
    PermanentError,
)


class TaskPhase(str, Enum):
    PENDING = "PENDING"
    ASSIGNED = "ASSIGNED"
    RUNNING = "RUNNING"
    CANCELLING = "CANCELLING"
    CANCELLED = "CANCELLED"
    AWAITING_REVIEW = "AWAITING_REVIEW"
    APPROVED = "APPROVED"
    MERGING = "MERGING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    QUARANTINED = "QUARANTINED"


# Explicit, verifiable transition table
# Any transition not declared here is rejected with IllegalStateTransitionError.
# NOTE: RUNNING -> PENDING is strictly FORBIDDEN to prevent duplicate execution of active workers.
VALID_TRANSITIONS: Dict[TaskPhase, Set[TaskPhase]] = {
    TaskPhase.PENDING: {
        TaskPhase.ASSIGNED,
        TaskPhase.CANCELLING,
        TaskPhase.QUARANTINED,
    },
    TaskPhase.ASSIGNED: {
        TaskPhase.RUNNING,
        TaskPhase.PENDING,      # Allowed ONLY after previous executor is proven dead
        TaskPhase.CANCELLING,
        TaskPhase.QUARANTINED,  # Liveness of executor uncertain after lease expiration
    },
    TaskPhase.RUNNING: {
        TaskPhase.AWAITING_REVIEW, # Agent completed execution with valid fencing token
        TaskPhase.FAILED,          # Permanent failure or retries exhausted
        TaskPhase.CANCELLING,      # Cancellation requested; executor must be stopped
        TaskPhase.QUARANTINED,     # Uncertain crash, heartbeat timeout, or transient error requiring audit
    },
    TaskPhase.CANCELLING: {
        TaskPhase.CANCELLED,       # Graceful cleanup and executor termination confirmed
        TaskPhase.QUARANTINED,     # Cleanup timed out or resources stuck
    },
    TaskPhase.AWAITING_REVIEW: {
        TaskPhase.APPROVED,        # Authenticated human reviewer approved
        TaskPhase.PENDING,         # Human rejected; re-opened for modifications
        TaskPhase.CANCELLING,
        TaskPhase.QUARANTINED,
    },
    TaskPhase.APPROVED: {
        TaskPhase.MERGING,         # Merge process initiated
        TaskPhase.AWAITING_REVIEW, # Approval revoked
        TaskPhase.CANCELLING,
        TaskPhase.QUARANTINED,
    },
    TaskPhase.MERGING: {
        TaskPhase.COMPLETED,       # Merge confirmed
        TaskPhase.FAILED,          # Merge conflict / verification failure
        TaskPhase.QUARANTINED,     # Merge state uncertain
    },
    TaskPhase.QUARANTINED: {
        TaskPhase.PENDING,         # Operator resolved quarantine and verified executor is dead
        TaskPhase.AWAITING_REVIEW, # Operator resolved quarantine and verified execution result
        TaskPhase.CANCELLED,       # Operator aborted quarantined task
    },
    TaskPhase.COMPLETED: set(),    # Terminal state
    TaskPhase.FAILED: set(),       # Terminal state
    TaskPhase.CANCELLED: set(),    # Terminal state
}


@dataclass
class TaskLease:
    """Represents a temporary, exclusive lease with a monotonically increasing fencing token."""
    lease_id: str
    holder_agent_id: str
    generation: int  # Fencing token: strictly increments on every lease acquisition
    acquired_at: float
    expires_at: float
    ttl_seconds: float = 300.0

    def is_expired(self, current_time: Optional[float] = None) -> bool:
        now = time.time() if current_time is None else current_time
        return now > self.expires_at

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TaskLease":
        return cls(**data)


@dataclass
class ExecutionResult:
    """Separated result produced solely by the agent's work."""
    status: str  # "SUCCESS", "FAILURE", "PARTIAL"
    summary: str
    commit_sha: Optional[str]
    generated_artifacts: List[str]
    execution_time_seconds: float
    completed_at: float
    fencing_token_used: int  # Must match the task's active generation

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ExecutionResult":
        return cls(**data)


def compute_approval_signature(
    approver_id: str,
    task_id: str,
    decision: str,
    timestamp: float,
    secret_key: bytes
) -> str:
    """Compute cryptographic HMAC-SHA256 signature for authentic human approvals."""
    message = f"{approver_id}:{task_id}:{decision}:{timestamp:.4f}".encode('utf-8')
    return hmac.new(secret_key, message, hashlib.sha256).hexdigest()


@dataclass
class HumanApproval:
    """Cryptographically verifiable human authorization record."""
    approver_id: str
    decision: str  # "APPROVED" or "REJECTED"
    approval_token: str
    comments: str
    timestamp: float
    signature: str  # HMAC-SHA256 authenticating human gate decision

    def verify_signature(self, task_id: str, secret_key: bytes) -> bool:
        """Verify that the approval signature matches the authentic operator secret."""
        expected = compute_approval_signature(
            self.approver_id, task_id, self.decision, self.timestamp, secret_key
        )
        return hmac.compare_digest(self.signature, expected)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "HumanApproval":
        return cls(**data)


@dataclass
class GitMergeRecord:
    """Separated record documenting the final Git merge into the target branch."""
    source_branch: str
    target_branch: str
    merged_commit_sha: str
    merge_strategy: str
    merged_by: str
    merged_at: float

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "GitMergeRecord":
        return cls(**data)


@dataclass
class RetryPolicy:
    max_retries: int = 3
    initial_backoff_seconds: float = 2.0
    backoff_factor: float = 2.0
    current_retry_count: int = 0

    def next_backoff(self) -> float:
        return self.initial_backoff_seconds * (self.backoff_factor ** self.current_retry_count)

    def can_retry(self) -> bool:
        return self.current_retry_count < self.max_retries

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RetryPolicy":
        return cls(**data)


def compute_payload_digest(
    title: str,
    instructions: str,
    target_branch: str,
    base_commit: str,
    affected_paths: List[str]
) -> str:
    """Compute deterministic SHA-256 digest of task payload for idempotency verification."""
    payload = {
        "title": title.strip(),
        "instructions": instructions.strip(),
        "target_branch": target_branch.strip(),
        "base_commit": base_commit.strip(),
        "affected_paths": sorted(affected_paths),
    }
    canonical = json.dumps(payload, sort_keys=True, separators=(',', ':')).encode('utf-8')
    return hashlib.sha256(canonical).hexdigest()


@dataclass
class TaskEnvelope:
    """Core aggregate for multi-agent tasks in DUALITY CORE."""
    task_id: str
    idempotency_key: str
    payload_digest: str
    correlation_id: str
    title: str
    instructions: str
    target_branch: str
    base_commit: str
    affected_paths: List[str]
    phase: TaskPhase = TaskPhase.PENDING
    assigned_agent: Optional[str] = None
    lease: Optional[TaskLease] = None
    fencing_token: int = 0
    retry_policy: RetryPolicy = field(default_factory=RetryPolicy)
    execution_result: Optional[ExecutionResult] = None
    human_approval: Optional[HumanApproval] = None
    git_merge: Optional[GitMergeRecord] = None
    errors: List[Dict[str, Any]] = field(default_factory=list)
    quarantine_reason: Optional[str] = None
    state_version: int = 1
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)

    @classmethod
    def create(
        cls,
        idempotency_key: str,
        title: str,
        instructions: str,
        target_branch: str,
        base_commit: str,
        affected_paths: List[str],
        correlation_id: Optional[str] = None,
        retry_policy: Optional[RetryPolicy] = None,
    ) -> "TaskEnvelope":
        digest = compute_payload_digest(title, instructions, target_branch, base_commit, affected_paths)
        tid = f"task-{uuid.uuid4().hex[:16]}"
        cid = correlation_id or f"corr-{uuid.uuid4().hex[:16]}"
        now = time.time()
        return cls(
            task_id=tid,
            idempotency_key=idempotency_key,
            payload_digest=digest,
            correlation_id=cid,
            title=title,
            instructions=instructions,
            target_branch=target_branch,
            base_commit=base_commit,
            affected_paths=affected_paths,
            phase=TaskPhase.PENDING,
            retry_policy=retry_policy or RetryPolicy(),
            created_at=now,
            updated_at=now,
        )

    def transition_to(self, next_phase: TaskPhase, reason: str = "") -> None:
        """Validate and apply state transition."""
        allowed = VALID_TRANSITIONS.get(self.phase, set())
        if next_phase not in allowed:
            raise IllegalStateTransitionError(
                from_phase=self.phase.value,
                to_phase=next_phase.value,
                reason=reason or f"Valid targets from {self.phase.value} are {[p.value for p in allowed]}"
            )
        self.phase = next_phase
        self.state_version += 1
        self.updated_at = time.time()
        if next_phase == TaskPhase.QUARANTINED and reason:
            self.quarantine_reason = reason

    def verify_fencing_token(self, token: int) -> None:
        """Verify that a received fencing token matches the active generation."""
        if token != self.fencing_token:
            raise FencingTokenMismatchError(
                expected_token=self.fencing_token,
                received_token=token,
                task_id=self.task_id
            )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "task_id": self.task_id,
            "idempotency_key": self.idempotency_key,
            "payload_digest": self.payload_digest,
            "correlation_id": self.correlation_id,
            "title": self.title,
            "instructions": self.instructions,
            "target_branch": self.target_branch,
            "base_commit": self.base_commit,
            "affected_paths": self.affected_paths,
            "phase": self.phase.value,
            "assigned_agent": self.assigned_agent,
            "lease": self.lease.to_dict() if self.lease else None,
            "fencing_token": self.fencing_token,
            "retry_policy": self.retry_policy.to_dict(),
            "execution_result": self.execution_result.to_dict() if self.execution_result else None,
            "human_approval": self.human_approval.to_dict() if self.human_approval else None,
            "git_merge": self.git_merge.to_dict() if self.git_merge else None,
            "errors": self.errors,
            "quarantine_reason": self.quarantine_reason,
            "state_version": self.state_version,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TaskEnvelope":
        copied = dict(data)
        copied["phase"] = TaskPhase(copied["phase"])
        if copied.get("lease"):
            copied["lease"] = TaskLease.from_dict(copied["lease"])
        if copied.get("retry_policy"):
            copied["retry_policy"] = RetryPolicy.from_dict(copied["retry_policy"])
        if copied.get("execution_result"):
            copied["execution_result"] = ExecutionResult.from_dict(copied["execution_result"])
        if copied.get("human_approval"):
            copied["human_approval"] = HumanApproval.from_dict(copied["human_approval"])
        if copied.get("git_merge"):
            copied["git_merge"] = GitMergeRecord.from_dict(copied["git_merge"])
        return cls(**copied)
