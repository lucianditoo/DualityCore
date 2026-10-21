"""Duality Multi-Agent Orchestration Layer (DMAOL) - Core Exceptions."""
from typing import Optional


class DualityError(Exception):
    """Base class for all Duality Core orchestration exceptions."""
    def __init__(self, code: str, message: str, retryable: bool = False, details: Optional[dict] = None):
        self.code = code
        self.message = message
        self.retryable = retryable
        self.details = details or {}
        super().__init__(f"[{code}] {message}")


class TransientError(DualityError):
    """An operation failed due to a transient condition and may be retried."""
    def __init__(self, code: str, message: str, details: Optional[dict] = None):
        super().__init__(code=code, message=message, retryable=True, details=details)


class PermanentError(DualityError):
    """An operation failed due to a non-recoverable condition. Retries will fail."""
    def __init__(self, code: str, message: str, details: Optional[dict] = None):
        super().__init__(code=code, message=message, retryable=False, details=details)


class IllegalStateTransitionError(PermanentError):
    """Attempted an invalid state transition in the task lifecycle state machine."""
    def __init__(self, from_phase: str, to_phase: str, reason: str = ""):
        super().__init__(
            code="ILLEGAL_STATE_TRANSITION",
            message=f"Transition from {from_phase} to {to_phase} is illegal. {reason}".strip(),
            details={"from_phase": from_phase, "to_phase": to_phase, "reason": reason}
        )


class FencingTokenMismatchError(PermanentError):
    """Action rejected because lease generation / fencing token is stale."""
    def __init__(self, expected_token: int, received_token: int, task_id: str):
        super().__init__(
            code="FENCING_TOKEN_MISMATCH",
            message=f"Stale fencing token {received_token} for task {task_id}. Current generation is {expected_token}.",
            details={"task_id": task_id, "expected_token": expected_token, "received_token": received_token}
        )


class LeaseExpiredError(TransientError):
    """Operation failed because the active lease expired."""
    def __init__(self, task_id: str, lease_id: str):
        super().__init__(
            code="LEASE_EXPIRED",
            message=f"Lease {lease_id} for task {task_id} has expired.",
            details={"task_id": task_id, "lease_id": lease_id}
        )


class ResourceConflictError(TransientError):
    """A resource (path or branch) is already leased exclusively by another task."""
    def __init__(self, resource: str, current_holder: str):
        super().__init__(
            code="RESOURCE_CONFLICT",
            message=f"Resource '{resource}' is currently leased by agent '{current_holder}'.",
            details={"resource": resource, "current_holder": current_holder}
        )


class QuarantineRequiredError(PermanentError):
    """Ambiguous executor or task state requires moving the task to quarantine for human review."""
    def __init__(self, task_id: str, reason: str):
        super().__init__(
            code="QUARANTINE_REQUIRED",
            message=f"Task {task_id} requires quarantine: {reason}",
            details={"task_id": task_id, "reason": reason}
        )


class SecurityViolationError(PermanentError):
    """Action violated an operating system, git, or access policy."""
    def __init__(self, rule: str, detail: str):
        super().__init__(
            code="SECURITY_VIOLATION",
            message=f"Security violation [{rule}]: {detail}",
            details={"rule": rule, "detail": detail}
        )


class PathTraversalError(SecurityViolationError):
    """Attempted path traversal or escape from the authorized workspace."""
    def __init__(self, path: str, boundary: str):
        super().__init__(
            rule="WORKSPACE_BOUNDARY_ENFORCEMENT",
            detail=f"Path '{path}' escapes workspace boundary '{boundary}'."
        )


class IdempotencyConflictError(PermanentError):
    """Idempotency key reused with a conflicting payload."""
    def __init__(self, idempotency_key: str):
        super().__init__(
            code="IDEMPOTENCY_CONFLICT",
            message=f"Idempotency key '{idempotency_key}' already exists with a different payload.",
            details={"idempotency_key": idempotency_key}
        )
