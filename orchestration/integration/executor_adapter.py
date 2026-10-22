"""DMAOL-side integration adapter translating TaskEnvelope lifecycle into dmaol-executor/1."""
import hashlib
import time
from typing import Dict, Any, Optional, Tuple
import uuid

from orchestration.models import (
    TaskEnvelope,
    ExecutionResult,
)
from orchestration.integration.errors import (
    AdapterError,
    ContractValidationError,
    StaleGenerationError,
    OutcomeUnknownError,
)
from orchestration.integration.contract_messages import (
    CONTRACT_VERSION,
    UUID_REGEX,
    dump_strict_json,
    parse_strict_json,
    validate_request_message,
    validate_response_message,
)
from orchestration.integration.simulated_transport import SimulatedExecutorTransport


def canonical_uuid_for_task(task_id: str) -> str:
    """Ensure task_id conforms to canonical UUID syntax required by the contract."""
    if UUID_REGEX.match(task_id):
        return task_id
    # Deterministic UUID mapping for DMAOL string IDs
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, task_id))


class DmaolExecutorAdapter:
    """High-level integration adapter bridging DMAOL orchestrator and the Executor contract."""

    def __init__(self, transport: SimulatedExecutorTransport, deployment_id: str):
        if not isinstance(deployment_id, str) or len(deployment_id) != 32:
            raise ContractValidationError("deployment_id must be exactly 32 lowercase hex characters")
        self.transport = transport
        self.deployment_id = deployment_id

    def start_attempt(
        self,
        task: TaskEnvelope,
        authorization_ref: str = "approval-handshake-001",
        profile_ref: str = "isolated-task-v1",
        input_ref: str = "artifact-input-001",
        input_sha256: Optional[str] = None,
        execution_timeout_ms: int = 60000,
        request_id: Optional[str] = None,
        attempt_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Initiate execution of a task attempt via the contract start operation."""
        if task.fencing_token < 1:
            raise StaleGenerationError(
                f"Task '{task.task_id}' has fencing_token={task.fencing_token}. A valid lease must be acquired first."
            )

        c_task_id = canonical_uuid_for_task(task.task_id)
        c_attempt_id = attempt_id or str(uuid.uuid4())
        c_req_id = request_id or str(uuid.uuid4())
        c_input_hash = input_sha256 or hashlib.sha256(task.payload_digest.encode()).hexdigest()

        start_req = {
            "version": CONTRACT_VERSION,
            "op": "start",
            "request_id": c_req_id,
            "deployment_id": self.deployment_id,
            "task_id": c_task_id,
            "attempt_id": c_attempt_id,
            "generation": task.fencing_token,
            "session_id": None,
            "authorization_ref": authorization_ref,
            "profile_ref": profile_ref,
            "input_ref": input_ref,
            "input_sha256": c_input_hash,
            "execution_timeout_ms": execution_timeout_ms,
        }

        req_bytes = dump_strict_json(start_req)
        resp_bytes = self.transport.send_request(req_bytes)
        resp_dict = parse_strict_json(resp_bytes)
        return validate_response_message(resp_dict)

    def observe_lost_ack(self, original_request_id: str, authorization_ref: str = "auth-observe-001") -> Dict[str, Any]:
        """Query state of an operation when the original ACK was lost in transit."""
        obs_req = {
            "version": CONTRACT_VERSION,
            "op": "observe",
            "request_id": str(uuid.uuid4()),
            "authorization_ref": authorization_ref,
            "lookup_request_id": original_request_id,
        }
        req_bytes = dump_strict_json(obs_req)
        resp_bytes = self.transport.send_request(req_bytes)
        resp_dict = parse_strict_json(resp_bytes)
        return validate_response_message(resp_dict)

    def observe_tuple(
        self,
        task: TaskEnvelope,
        attempt_id: str,
        session_id: str,
        generation: Optional[int] = None,
        authorization_ref: str = "auth-observe-001",
        request_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Query state using the bound 5-tuple."""
        obs_req = {
            "version": CONTRACT_VERSION,
            "op": "observe",
            "request_id": request_id or str(uuid.uuid4()),
            "authorization_ref": authorization_ref,
            "deployment_id": self.deployment_id,
            "task_id": canonical_uuid_for_task(task.task_id),
            "attempt_id": attempt_id,
            "generation": generation or task.fencing_token,
            "session_id": session_id,
        }
        req_bytes = dump_strict_json(obs_req)
        resp_bytes = self.transport.send_request(req_bytes)
        resp_dict = parse_strict_json(resp_bytes)
        return validate_response_message(resp_dict)

    def cancel_attempt(
        self,
        task: TaskEnvelope,
        attempt_id: str,
        session_id: str,
        generation: Optional[int] = None,
        reason_code: str = "OPERATOR_REQUEST",
        authorization_ref: str = "auth-cancel-001",
        request_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Request cancellation of an active attempt."""
        cancel_req = {
            "version": CONTRACT_VERSION,
            "op": "cancel",
            "request_id": request_id or str(uuid.uuid4()),
            "deployment_id": self.deployment_id,
            "task_id": canonical_uuid_for_task(task.task_id),
            "attempt_id": attempt_id,
            "generation": generation or task.fencing_token,
            "session_id": session_id,
            "authorization_ref": authorization_ref,
            "reason_code": reason_code,
        }
        req_bytes = dump_strict_json(cancel_req)
        resp_bytes = self.transport.send_request(req_bytes)
        resp_dict = parse_strict_json(resp_bytes)
        return validate_response_message(resp_dict)

    def recover_attempt(
        self,
        task: TaskEnvelope,
        attempt_id: str,
        session_id: str,
        generation: Optional[int] = None,
        authorization_ref: str = "auth-recover-001",
        request_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Recover and reconcile state non-destructively."""
        rec_req = {
            "version": CONTRACT_VERSION,
            "op": "recover",
            "request_id": request_id or str(uuid.uuid4()),
            "deployment_id": self.deployment_id,
            "task_id": canonical_uuid_for_task(task.task_id),
            "attempt_id": attempt_id,
            "generation": generation or task.fencing_token,
            "session_id": session_id,
            "authorization_ref": authorization_ref,
        }
        req_bytes = dump_strict_json(rec_req)
        resp_bytes = self.transport.send_request(req_bytes)
        resp_dict = parse_strict_json(resp_bytes)
        return validate_response_message(resp_dict)

    def translate_to_dmaol_execution_result(self, result_envelope: Dict[str, Any]) -> ExecutionResult:
        """Convert a contractual result envelope into DMAOL's ExecutionResult aggregate."""
        outcome = result_envelope["outcome"]
        status = "SUCCESS" if outcome == "SUCCEEDED" else "FAILED"
        return ExecutionResult(
            status=status,
            summary=f"Result from session {result_envelope['session_id']} (attempt {result_envelope['attempt_id']})",
            commit_sha=result_envelope["artifact_sha256"][:40],
            generated_artifacts=[result_envelope["artifact_ref"]],
            execution_time_seconds=10.0,
            completed_at=result_envelope["completed_at"],
            fencing_token_used=result_envelope["generation"]
        )
