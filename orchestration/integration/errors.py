"""Integration errors and contractual error definitions for DMAOL <-> Executor."""
from typing import Optional, Dict, Any


class AdapterError(Exception):
    """Base exception for all integration adapter errors."""
    def __init__(
        self,
        code: str,
        message: str,
        operation: str = "",
        effect: str = "NONE",
        retry: str = "DO_NOT_RETRY",
        operator_review_required: bool = False,
        executor_error_code: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ):
        self.code = code
        self.message = message
        self.operation = operation
        self.effect = effect
        self.retry = retry
        self.operator_review_required = operator_review_required
        self.executor_error_code = executor_error_code
        self.details = details or {}
        super().__init__(f"[{code}] {message}")

    def to_contract_error_dict(self) -> Dict[str, Any]:
        """Convert into the contractual error schema specified in docs/DMAOL_EXECUTOR_CONTRACT.md."""
        res: Dict[str, Any] = {
            "code": self.code,
            "operation": self.operation or "unknown",
            "effect": self.effect,
            "retry": self.retry,
            "operator_review_required": self.operator_review_required,
        }
        if self.executor_error_code:
            res["executor_error_code"] = self.executor_error_code
        return res


class ContractValidationError(AdapterError):
    """Message schema, size, or identity field violation."""
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(
            code="INVALID_REQUEST",
            message=message,
            effect="NONE",
            retry="DO_NOT_RETRY",
            operator_review_required=False,
            details=details,
        )


class UnauthorizedError(AdapterError):
    """Operation rejected due to invalid authorization or scope mismatch."""
    def __init__(self, message: str, operation: str = "", details: Optional[Dict[str, Any]] = None):
        super().__init__(
            code="UNAUTHORIZED",
            message=message,
            operation=operation,
            effect="NONE",
            retry="DO_NOT_RETRY",
            operator_review_required=False,
            details=details,
        )


class StaleGenerationError(AdapterError):
    """Operation or result rejected due to obsolete generation/fencing token."""
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(
            code="STALE_GENERATION",
            message=message,
            effect="NONE",
            retry="DO_NOT_RETRY",
            operator_review_required=False,
            details=details,
        )


class IdempotencyConflictError(AdapterError):
    """Same request_id supplied with conflicting operation parameters."""
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(
            code="IDEMPOTENCY_CONFLICT",
            message=message,
            effect="NONE",
            retry="DO_NOT_RETRY",
            operator_review_required=False,
            details=details,
        )


class AlreadyTerminalError(AdapterError):
    """Operation cannot be performed because the task is already terminal."""
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(
            code="ALREADY_TERMINAL",
            message=message,
            effect="NONE",
            retry="DO_NOT_RETRY",
            operator_review_required=False,
            details=details,
        )


class OutcomeUnknownError(AdapterError):
    """State or outcome of operation could not be determined with certainty."""
    def __init__(self, message: str, operation: str = "", executor_error_code: Optional[str] = None, details: Optional[Dict[str, Any]] = None):
        super().__init__(
            code="OUTCOME_UNKNOWN",
            message=message,
            operation=operation,
            effect="MAY_HAVE_OCCURRED",
            retry="OBSERVE_SAME_OPERATION",
            operator_review_required=True,
            executor_error_code=executor_error_code,
            details=details,
        )


class ResourceIdentityUnverifiableError(AdapterError):
    """Session or resource identity mismatch or unverified."""
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(
            code="RESOURCE_IDENTITY_UNVERIFIABLE",
            message=message,
            effect="NONE",
            retry="DO_NOT_RETRY",
            operator_review_required=True,
            details=details,
        )
