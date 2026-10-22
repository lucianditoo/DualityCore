"""DMAOL Live Client (Phase 3D-E).

Experimental high-level client wrapping DmaolExecutorAdapter to interact
with FORGE LINK / ExecutionLedger under dmaol-executor/1 without creating
an auxiliary execution authority or altering existing DMAOL models.
"""
import hashlib
import time
from typing import Dict, Any, Optional

from orchestration.models import (
    TaskEnvelope,
    ExecutionResult,
)
from orchestration.integration.errors import (
    AdapterError,
    StaleGenerationError,
    ContractValidationError,
)
from orchestration.integration.executor_adapter import (
    DmaolExecutorAdapter,
    canonical_uuid_for_task,
)
from lab.crosswire_live.transport import ForgeLinkDirectTransport


# Canonical fixed inputs mandated by Forge Link / Ignition
FIXED_PROFILE = "isolated-echo-v1"
FIXED_INPUT_REF = "ignition-fixed-input-v1"
FIXED_INPUT_HASH = hashlib.sha256(b"").hexdigest()


class DmaolLiveClient:
    """High-level client connecting DMAOL orchestrator tasks to Forge Link."""

    def __init__(self, transport: ForgeLinkDirectTransport, deployment_id: str):
        self.adapter = DmaolExecutorAdapter(transport=transport, deployment_id=deployment_id)
        self.deployment_id = deployment_id

    def start_task(
        self,
        task: TaskEnvelope,
        attempt_id: Optional[str] = None,
        authorization_ref: str = "approval-lab-001",
        timeout_ms: int = 2000,
        request_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Dispatch task execution start to Forge Link."""
        return self.adapter.start_attempt(
            task=task,
            authorization_ref=authorization_ref,
            profile_ref=FIXED_PROFILE,
            input_ref=FIXED_INPUT_REF,
            input_sha256=FIXED_INPUT_HASH,
            execution_timeout_ms=timeout_ms,
            request_id=request_id,
            attempt_id=attempt_id,
        )

    def observe_task(
        self,
        task: TaskEnvelope,
        attempt_id: str,
        session_id: str,
        generation: Optional[int] = None,
        authorization_ref: str = "approval-lab-001",
        request_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Query task state using the bound 5-tuple."""
        return self.adapter.observe_tuple(
            task=task,
            attempt_id=attempt_id,
            session_id=session_id,
            generation=generation,
            authorization_ref=authorization_ref,
            request_id=request_id,
        )

    def cancel_task(
        self,
        task: TaskEnvelope,
        attempt_id: str,
        session_id: str,
        generation: Optional[int] = None,
        reason_code: str = "OPERATOR_REQUEST",
        authorization_ref: str = "approval-lab-001",
        request_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Request audited cancellation of an active attempt."""
        return self.adapter.cancel_attempt(
            task=task,
            attempt_id=attempt_id,
            session_id=session_id,
            generation=generation,
            reason_code=reason_code,
            authorization_ref=authorization_ref,
            request_id=request_id,
        )

    def recover_task(
        self,
        task: TaskEnvelope,
        attempt_id: str,
        session_id: str,
        generation: Optional[int] = None,
        authorization_ref: str = "approval-lab-001",
        request_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Execute non-destructive recovery and reconcile uncertainty."""
        return self.adapter.recover_attempt(
            task=task,
            attempt_id=attempt_id,
            session_id=session_id,
            generation=generation,
            authorization_ref=authorization_ref,
            request_id=request_id,
        )

    def process_execution_outcome(
        self,
        task: TaskEnvelope,
        result_envelope: Dict[str, Any],
        raw_artifact_bytes: Optional[bytes] = None,
    ) -> ExecutionResult:
        """Convert a contractual result envelope into DMAOL's ExecutionResult.

        Guarantees:
        1. Fencing validation: generation must match task.fencing_token.
        2. Outcome preservation: outcome UNKNOWN remains UNKNOWN.
        3. Artifact validation: if raw bytes provided, verifies SHA-256 matches evidence.
        4. No synthetic Git commits or execution times.
        """
        generation = int(result_envelope.get("generation", 0))
        if generation != task.fencing_token:
            raise StaleGenerationError(
                f"Result generation {generation} does not match active task fencing token {task.fencing_token}"
            )

        outcome = result_envelope.get("outcome", "FAILED")
        artifact_sha256 = result_envelope.get("artifact_sha256")

        # Verify raw bytes integrity if available
        if raw_artifact_bytes is not None and artifact_sha256:
            actual_sha = hashlib.sha256(raw_artifact_bytes).hexdigest()
            if actual_sha != artifact_sha256:
                raise AdapterError(
                    "ARTIFACT_INTEGRITY_MISMATCH",
                    f"Artifact content hash {actual_sha} does not match contractual evidence {artifact_sha256}",
                    operation="validate_artifact",
                )

        if outcome == "SUCCEEDED":
            status = "SUCCESS"
        elif outcome == "UNKNOWN":
            status = "UNKNOWN"
        else:
            status = "FAILED"

        artifacts = []
        if result_envelope.get("artifact_ref"):
            artifacts.append(result_envelope["artifact_ref"])

        # Preserve exact duration if measured, else sentinel 0.0
        duration = float(result_envelope.get("duration_seconds", 0.0))

        return ExecutionResult(
            status=status,
            summary=f"Result from Forge Link session {result_envelope.get('session_id', 'unknown')} (outcome={outcome})",
            commit_sha=result_envelope.get("commit_sha"),  # None unless explicitly provided
            generated_artifacts=artifacts,
            execution_time_seconds=duration,
            completed_at=time.time(),
            fencing_token_used=generation,
        )
