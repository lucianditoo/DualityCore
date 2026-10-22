"""Strict schema parsing, serialization, and validation for dmaol-executor/1 contract."""
import json
import re
from typing import Dict, Any, Optional, Tuple, Set

from orchestration.integration.errors import (
    ContractValidationError,
)

CONTRACT_VERSION = "dmaol-executor/1"
MAX_CONTROL_MESSAGE_BYTES = 16384
MAX_GENERATION = 9007199254740991

UUID_REGEX = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
HEX32_REGEX = re.compile(r"^[0-9a-f]{32}$")
HEX64_REGEX = re.compile(r"^[0-9a-f]{64}$")
SESSION_ID_REGEX = re.compile(r"^s-[0-9a-f]{32}$")

VALID_OPERATIONS: Set[str] = {"start", "observe", "cancel", "recover"}
VALID_TASK_STATES: Set[str] = {
    "ACCEPTED", "DISPATCHING", "RUNNING", "CANCEL_REQUESTED",
    "SUCCEEDED", "FAILED", "CANCELLED", "UNKNOWN"
}
VALID_RESOURCE_STATES: Set[str] = {
    "NOT_CREATED", "CREATING", "ACTIVE_OBSERVED", "STOPPING",
    "CLOSED_CONFIRMED", "UNKNOWN"
}
VALID_CERTAINTIES: Set[str] = {"CONFIRMED", "OBSERVED", "UNKNOWN"}
VALID_ERROR_CODES: Set[str] = {
    "INVALID_REQUEST", "UNAUTHORIZED", "APPROVAL_EXPIRED",
    "DEPENDENCY_UNVERIFIED", "STALE_GENERATION", "IDEMPOTENCY_CONFLICT",
    "BUSY", "ALREADY_TERMINAL", "OUTCOME_UNKNOWN", "RESOURCE_IDENTITY_UNVERIFIABLE"
}


def _reject_duplicates_pairs_hook(pairs):
    res = {}
    for k, v in pairs:
        if k in res:
            raise ContractValidationError(f"Duplicate JSON key: '{k}'")
        res[k] = v
    return res


def parse_strict_json(raw: bytes) -> Dict[str, Any]:
    """Parse raw bytes into a JSON dictionary adhering to strict contract limits."""
    if not isinstance(raw, (bytes, bytearray)):
        raise ContractValidationError("Message payload must be raw bytes")
    if len(raw) > MAX_CONTROL_MESSAGE_BYTES:
        raise ContractValidationError(
            f"Message size {len(raw)} exceeds maximum allowed {MAX_CONTROL_MESSAGE_BYTES} bytes"
        )
    try:
        decoded = raw.decode("utf-8")
    except UnicodeDecodeError as e:
        raise ContractValidationError(f"Invalid UTF-8 encoding in message: {e}")

    try:
        parsed = json.loads(
            decoded,
            object_pairs_hook=_reject_duplicates_pairs_hook,
            parse_constant=lambda c: (_ for _ in ()).throw(ValueError(f"Invalid constant {c}"))
        )
    except Exception as e:
        raise ContractValidationError(f"Malformed JSON: {e}")

    if not isinstance(parsed, dict):
        raise ContractValidationError("JSON payload must be an object")
    return parsed


def dump_strict_json(obj: Dict[str, Any]) -> bytes:
    """Serialize dictionary into canonical UTF-8 JSON bytes."""
    serialized = json.dumps(obj, sort_keys=True, separators=(",", ":")).encode("utf-8")
    if len(serialized) > MAX_CONTROL_MESSAGE_BYTES:
        raise ContractValidationError(
            f"Serialized message size {len(serialized)} exceeds limit of {MAX_CONTROL_MESSAGE_BYTES} bytes"
        )
    return serialized


def _validate_uuid(val: Any, field_name: str) -> str:
    if not isinstance(val, str) or not UUID_REGEX.match(val):
        raise ContractValidationError(f"Field '{field_name}' must be a canonical lowercase UUID, got: {val!r}")
    return val


def _validate_hex32(val: Any, field_name: str) -> str:
    if not isinstance(val, str) or not HEX32_REGEX.match(val):
        raise ContractValidationError(f"Field '{field_name}' must be 32 lowercase hex characters, got: {val!r}")
    return val


def _validate_hex64(val: Any, field_name: str) -> str:
    if not isinstance(val, str) or not HEX64_REGEX.match(val):
        raise ContractValidationError(f"Field '{field_name}' must be 64 lowercase hex characters, got: {val!r}")
    return val


def _validate_session_id(val: Any, field_name: str) -> str:
    if not isinstance(val, str) or not SESSION_ID_REGEX.match(val):
        raise ContractValidationError(f"Field '{field_name}' must match pattern 's-[0-9a-f]{{32}}', got: {val!r}")
    return val


def _validate_generation(val: Any, field_name: str = "generation") -> int:
    if isinstance(val, bool) or not isinstance(val, int) or val < 1 or val > MAX_GENERATION:
        raise ContractValidationError(
            f"Field '{field_name}' must be an integer between 1 and {MAX_GENERATION}, got: {val!r}"
        )
    return val


def _validate_positive_int(val: Any, field_name: str) -> int:
    if isinstance(val, bool) or not isinstance(val, int) or val <= 0:
        raise ContractValidationError(f"Field '{field_name}' must be a positive integer, got: {val!r}")
    return val


def _validate_string(val: Any, field_name: str, max_len: int = 256) -> str:
    if not isinstance(val, str) or not val.strip() or len(val) > max_len:
        raise ContractValidationError(f"Field '{field_name}' must be a non-empty string <= {max_len} chars")
    return val.strip()


def validate_request_message(data: Dict[str, Any]) -> Dict[str, Any]:
    """Strictly validate an incoming contract request message."""
    if data.get("version") != CONTRACT_VERSION:
        raise ContractValidationError(
            f"Unsupported contract version '{data.get('version')}'. Expected '{CONTRACT_VERSION}'"
        )

    op = data.get("op")
    if op not in VALID_OPERATIONS:
        raise ContractValidationError(f"Invalid operation '{op}'. Supported: {sorted(VALID_OPERATIONS)}")

    _validate_uuid(data.get("request_id"), "request_id")

    if op == "start":
        allowed_fields = {
            "version", "op", "request_id", "deployment_id", "task_id",
            "attempt_id", "generation", "session_id", "authorization_ref",
            "profile_ref", "input_ref", "input_sha256", "execution_timeout_ms"
        }
        unknown = set(data.keys()) - allowed_fields
        if unknown:
            raise ContractValidationError(f"Unknown fields in 'start' request: {unknown}")

        missing = allowed_fields - set(data.keys())
        if missing:
            raise ContractValidationError(f"Missing required fields in 'start' request: {missing}")

        _validate_hex32(data["deployment_id"], "deployment_id")
        _validate_uuid(data["task_id"], "task_id")
        _validate_uuid(data["attempt_id"], "attempt_id")
        _validate_generation(data["generation"], "generation")

        if data["session_id"] is not None:
            raise ContractValidationError(
                f"'session_id' in 'start' request must be null (assigned by adapter), got: {data['session_id']!r}"
            )

        _validate_string(data["authorization_ref"], "authorization_ref")
        _validate_string(data["profile_ref"], "profile_ref")
        _validate_string(data["input_ref"], "input_ref")
        _validate_hex64(data["input_sha256"], "input_sha256")
        _validate_positive_int(data["execution_timeout_ms"], "execution_timeout_ms")

    elif op == "observe":
        allowed_fields = {
            "version", "op", "request_id", "authorization_ref",
            "lookup_request_id", "deployment_id", "task_id", "attempt_id",
            "generation", "session_id"
        }
        unknown = set(data.keys()) - allowed_fields
        if unknown:
            raise ContractValidationError(f"Unknown fields in 'observe' request: {unknown}")

        _validate_string(data.get("authorization_ref"), "authorization_ref")

        if "lookup_request_id" in data:
            _validate_uuid(data["lookup_request_id"], "lookup_request_id")
        else:
            required_tuple = {"deployment_id", "task_id", "attempt_id", "generation", "session_id"}
            missing = required_tuple - set(data.keys())
            if missing:
                raise ContractValidationError(
                    f"'observe' requires either 'lookup_request_id' or full tuple {required_tuple}, missing: {missing}"
                )
            _validate_hex32(data["deployment_id"], "deployment_id")
            _validate_uuid(data["task_id"], "task_id")
            _validate_uuid(data["attempt_id"], "attempt_id")
            _validate_generation(data["generation"], "generation")
            _validate_session_id(data["session_id"], "session_id")

    elif op == "cancel":
        allowed_fields = {
            "version", "op", "request_id", "deployment_id", "task_id",
            "attempt_id", "generation", "session_id", "authorization_ref",
            "reason_code"
        }
        unknown = set(data.keys()) - allowed_fields
        if unknown:
            raise ContractValidationError(f"Unknown fields in 'cancel' request: {unknown}")

        missing = allowed_fields - set(data.keys())
        if missing:
            raise ContractValidationError(f"Missing required fields in 'cancel' request: {missing}")

        _validate_hex32(data["deployment_id"], "deployment_id")
        _validate_uuid(data["task_id"], "task_id")
        _validate_uuid(data["attempt_id"], "attempt_id")
        _validate_generation(data["generation"], "generation")
        _validate_session_id(data["session_id"], "session_id")
        _validate_string(data["authorization_ref"], "authorization_ref")
        _validate_string(data["reason_code"], "reason_code")

    elif op == "recover":
        allowed_fields = {
            "version", "op", "request_id", "deployment_id", "task_id",
            "attempt_id", "generation", "session_id", "authorization_ref"
        }
        unknown = set(data.keys()) - allowed_fields
        if unknown:
            raise ContractValidationError(f"Unknown fields in 'recover' request: {unknown}")

        missing = allowed_fields - set(data.keys())
        if missing:
            raise ContractValidationError(f"Missing required fields in 'recover' request: {missing}")

        _validate_hex32(data["deployment_id"], "deployment_id")
        _validate_uuid(data["task_id"], "task_id")
        _validate_uuid(data["attempt_id"], "attempt_id")
        _validate_generation(data["generation"], "generation")
        _validate_session_id(data["session_id"], "session_id")
        _validate_string(data["authorization_ref"], "authorization_ref")

    return data


def validate_response_message(data: Dict[str, Any]) -> Dict[str, Any]:
    """Strictly validate an outgoing or incoming contract response message."""
    if data.get("version") != CONTRACT_VERSION:
        raise ContractValidationError(f"Invalid response version: {data.get('version')}")

    required_fields = {
        "version", "request_id", "operation_id", "deployment_id", "task_id",
        "attempt_id", "generation", "session_id", "executor_instance",
        "state_version", "task_state", "resource_state", "certainty",
        "observed_at", "source", "error"
    }
    unknown = set(data.keys()) - required_fields
    if unknown:
        raise ContractValidationError(f"Unknown fields in response message: {unknown}")

    missing = required_fields - set(data.keys())
    if missing:
        raise ContractValidationError(f"Missing fields in response message: {missing}")

    err = data["error"]
    if err is None:
        _validate_uuid(data["request_id"], "request_id")
        _validate_uuid(data["operation_id"], "operation_id")
        _validate_hex32(data["deployment_id"], "deployment_id")
        _validate_uuid(data["task_id"], "task_id")
        _validate_uuid(data["attempt_id"], "attempt_id")
        _validate_generation(data["generation"], "generation")
        _validate_session_id(data["session_id"], "session_id")
        if data["executor_instance"] is not None:
            _validate_hex32(data["executor_instance"], "executor_instance")
        _validate_positive_int(data["state_version"], "state_version")
    else:
        if not isinstance(err, dict):
            raise ContractValidationError("Response 'error' must be a dict or null")
        code = err.get("code")
        if code not in VALID_ERROR_CODES:
            raise ContractValidationError(f"Invalid error code '{code}' in response error")
        if err.get("operation") is not None:
            _validate_string(err.get("operation"), "error.operation")
        if err.get("effect") not in {"NONE", "MAY_HAVE_OCCURRED", "CONFIRMED"}:
            raise ContractValidationError(f"Invalid error effect: {err.get('effect')}")
        if not isinstance(err.get("operator_review_required"), bool):
            raise ContractValidationError("error.operator_review_required must be a boolean")

        # Identity fields can be None if error occurred before assignment or during auth failure
        if data["request_id"] is not None:
            _validate_uuid(data["request_id"], "request_id")
        if data["operation_id"] is not None:
            _validate_uuid(data["operation_id"], "operation_id")
        if data["deployment_id"] is not None:
            _validate_hex32(data["deployment_id"], "deployment_id")
        if data["task_id"] is not None:
            _validate_uuid(data["task_id"], "task_id")
        if data["attempt_id"] is not None:
            _validate_uuid(data["attempt_id"], "attempt_id")
        if data["generation"] is not None:
            _validate_generation(data["generation"], "generation")
        if data["session_id"] is not None:
            _validate_session_id(data["session_id"], "session_id")
        if data["executor_instance"] is not None:
            _validate_hex32(data["executor_instance"], "executor_instance")
        if data["state_version"] is not None:
            _validate_positive_int(data["state_version"], "state_version")

    if data["task_state"] not in VALID_TASK_STATES:
        raise ContractValidationError(f"Invalid task_state '{data['task_state']}'")
    if data["resource_state"] not in VALID_RESOURCE_STATES:
        raise ContractValidationError(f"Invalid resource_state '{data['resource_state']}'")
    if data["certainty"] not in VALID_CERTAINTIES:
        raise ContractValidationError(f"Invalid certainty '{data['certainty']}'")

    _validate_string(data["observed_at"], "observed_at")
    _validate_string(data["source"], "source")
    return data


def validate_result_envelope(data: Dict[str, Any]) -> Dict[str, Any]:
    """Strictly validate an incoming task execution result envelope."""
    if "version" in data and data["version"] != CONTRACT_VERSION:
        raise ContractValidationError(f"Invalid result version: {data.get('version')}")

    required_fields = {
        "task_id", "attempt_id", "generation", "session_id",
        "result_id", "outcome", "artifact_ref", "artifact_sha256"
    }
    allowed_fields = required_fields | {
        "version", "deployment_id", "completed_at", "signature",
        "commit_sha", "execution_time_seconds"
    }

    unknown = set(data.keys()) - allowed_fields
    if unknown:
        raise ContractValidationError(f"Unknown fields in result envelope: {unknown}")

    missing = required_fields - set(data.keys())
    if missing:
        raise ContractValidationError(f"Missing fields in result envelope: {missing}")

    _validate_uuid(data["task_id"], "task_id")
    _validate_uuid(data["attempt_id"], "attempt_id")
    _validate_generation(data["generation"], "generation")
    _validate_session_id(data["session_id"], "session_id")
    _validate_uuid(data["result_id"], "result_id")

    if data.get("deployment_id") is not None:
        _validate_hex32(data["deployment_id"], "deployment_id")

    if data["outcome"] not in ("SUCCEEDED", "FAILED"):
        raise ContractValidationError(f"Invalid result outcome '{data['outcome']}'. Expected SUCCEEDED or FAILED")

    _validate_string(data["artifact_ref"], "artifact_ref")
    _validate_hex64(data["artifact_sha256"], "artifact_sha256")

    if "completed_at" in data:
        if not isinstance(data["completed_at"], (int, float)) or data["completed_at"] <= 0:
            raise ContractValidationError(f"Invalid completed_at timestamp: {data['completed_at']!r}")

    if "signature" in data:
        _validate_string(data["signature"], "signature")
    return data
