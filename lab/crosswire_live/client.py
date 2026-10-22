"""DMAOL Live Client (Phase 3D-G Hardened & Verified).

Experimental high-level client wrapping DmaolExecutorAdapter to interact
with FORGE LINK / ExecutionLedger under dmaol-executor/1 without creating
an auxiliary execution authority or altering existing DMAOL models.
"""
from __future__ import annotations

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
    ResourceIdentityUnverifiableError,
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
MAX_CONTRACT_INT = 9007199254740991


class DmaolLiveClient:
    """High-level client connecting DMAOL orchestrator tasks to Forge Link."""

    def __init__(self, transport: ForgeLinkDirectTransport, deployment_id: str):
        self.adapter = DmaolExecutorAdapter(transport=transport, deployment_id=deployment_id)
        self.transport = transport
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
        attempt_id: Optional[str] = None,
        session_id: Optional[str] = None,
        raw_artifact_bytes: Optional[bytes] = None,
    ) -> ExecutionResult:
        """Convert a contractual result envelope into DMAOL's ExecutionResult.

        Guarantees:
        1. Strict Generation Types: rejects booleans, floats, strings, nulls, and out-of-range values.
        2. Contractual Identity: validates deployment_id, task_id, attempt_id, and session_id against
           the expected task/attempt/session and client configuration.
        3. Mandatory Artifact Authority & Integrity: for SUCCEEDED outcomes, requires valid artifact_ref,
           artifact_sha256, retrieves artifact bytes from the authentic authority (rejecting alien
           session references), and verifies SHA-256 hash. Caller-provided bytes are never trusted
           without verification against authority bytes.
        4. Outcome Preservation: outcome UNKNOWN remains UNKNOWN (never converted to SUCCESS);
           FAILED and CANCELLED remain non-successful.
        5. No Synthetic Data: no fabricated Git commits or execution duration.
        """
        if not isinstance(result_envelope, dict):
            raise ContractValidationError("Result envelope must be a dictionary")

        # -------------------------------------------------------------------
        # ETAPA 2: Tipos estrictos de generación (rechaza bool, str, float, null, fuera de rango)
        # -------------------------------------------------------------------
        if "generation" not in result_envelope:
            raise ContractValidationError("Missing 'generation' in result envelope")

        gen_raw = result_envelope["generation"]
        if gen_raw is None:
            raise ContractValidationError("Generation cannot be None")

        # In Python, bool is a subclass of int (isinstance(True, int) is True).
        # We must explicitly reject booleans.
        if type(gen_raw) is not int or isinstance(gen_raw, bool):
            raise ContractValidationError(
                f"Invalid generation type: {type(gen_raw).__name__} (value: {gen_raw!r}). Generation must be a strict integer."
            )

        if gen_raw < 1 or gen_raw > MAX_CONTRACT_INT:
            raise ContractValidationError(
                f"Generation must be an integer between 1 and {MAX_CONTRACT_INT}, got {gen_raw}"
            )

        if gen_raw != task.fencing_token:
            raise StaleGenerationError(
                f"Result generation {gen_raw} does not match active task fencing token {task.fencing_token}"
            )

        # -------------------------------------------------------------------
        # ETAPA 1: Identidad contractual completa y contexto obligatorio
        # -------------------------------------------------------------------
        # Contexto esperado obligatorio
        if attempt_id is None or not isinstance(attempt_id, str) or not attempt_id.strip():
            raise ResourceIdentityUnverifiableError(
                "Missing required trusted context: 'attempt_id' must be provided to verify execution outcome"
            )
        if session_id is None or not isinstance(session_id, str) or not session_id.strip():
            raise ResourceIdentityUnverifiableError(
                "Missing required trusted context: 'session_id' must be provided to verify execution outcome"
            )

        # Deployment identity validation
        if "deployment_id" not in result_envelope or result_envelope["deployment_id"] is None:
            raise ResourceIdentityUnverifiableError("Missing or null 'deployment_id' in result envelope")
        res_deployment = result_envelope["deployment_id"]
        if not isinstance(res_deployment, str) or not res_deployment.strip():
            raise ResourceIdentityUnverifiableError("Invalid 'deployment_id' type or empty value in result envelope")
        if res_deployment != self.deployment_id:
            raise ResourceIdentityUnverifiableError(
                f"Result deployment_id '{res_deployment}' does not match client deployment_id '{self.deployment_id}'"
            )

        # Task identity validation
        expected_task_id = canonical_uuid_for_task(task.task_id)
        res_task_id = result_envelope.get("task_id")
        if res_task_id is None or not isinstance(res_task_id, str):
            raise ResourceIdentityUnverifiableError("Missing or invalid 'task_id' in result envelope")
        if res_task_id != expected_task_id:
            raise ResourceIdentityUnverifiableError(
                f"Result task_id '{res_task_id}' does not match expected task_id '{expected_task_id}'"
            )

        # Attempt identity validation
        res_attempt_id = result_envelope.get("attempt_id")
        if res_attempt_id is None or not isinstance(res_attempt_id, str) or not res_attempt_id.strip():
            raise ResourceIdentityUnverifiableError("Missing or invalid 'attempt_id' in result envelope")
        if res_attempt_id != attempt_id:
            raise ResourceIdentityUnverifiableError(
                f"Result attempt_id '{res_attempt_id}' does not match expected attempt_id '{attempt_id}'"
            )

        # Session identity validation
        res_session_id = result_envelope.get("session_id")
        if res_session_id is None or not isinstance(res_session_id, str) or not res_session_id.strip():
            raise ResourceIdentityUnverifiableError("Missing or invalid 'session_id' in result envelope")
        if res_session_id != session_id:
            raise ResourceIdentityUnverifiableError(
                f"Result session_id '{res_session_id}' does not match expected session_id '{session_id}'"
            )

        # Result ID format and presence validation
        res_result_id = result_envelope.get("result_id")
        if res_result_id is None or not isinstance(res_result_id, str) or not res_result_id.strip():
            raise ContractValidationError("Missing or invalid 'result_id' in result envelope")
        import uuid as _uuid
        try:
            _parsed_uuid = _uuid.UUID(res_result_id)
        except (ValueError, TypeError, AttributeError) as exc:
            raise ContractValidationError(f"Invalid 'result_id' UUID format: {res_result_id!r}") from exc

        # -------------------------------------------------------------------
        # ETAPA 3: Integridad obligatoria y verificación de artefacto desde autoridad
        # -------------------------------------------------------------------
        outcome = result_envelope.get("outcome")
        if outcome is None:
            raise ContractValidationError("Missing 'outcome' in result envelope")

        if outcome not in ("SUCCEEDED", "FAILED", "UNKNOWN", "CANCELLED"):
            raise ContractValidationError(f"Invalid outcome: '{outcome}'")

        if outcome == "SUCCEEDED":
            artifact_ref = result_envelope.get("artifact_ref")
            if not artifact_ref or not isinstance(artifact_ref, str) or not artifact_ref.strip():
                raise ContractValidationError("Successful result requires a non-empty string 'artifact_ref'")

            expected_sha = result_envelope.get("artifact_sha256")
            if not expected_sha or not isinstance(expected_sha, str) or len(expected_sha) != 64:
                raise ContractValidationError("Successful result requires a valid 64-char hex 'artifact_sha256'")

        # Correlación obligatoria de result_id con autoridad de ejecución para todos los estados
        if not hasattr(self.transport, "get_execution_identity") or not callable(getattr(self.transport, "get_execution_identity")):
            raise ResourceIdentityUnverifiableError(
                "Transport does not support authentic execution identity verification; cannot authenticate outcome"
            )

        authority_exec_id = self.transport.get_execution_identity(res_session_id)
        if authority_exec_id is None:
            # Si la autoridad no tiene registro de ejecución para esta sesión, la identidad no puede verificarse
            raise ResourceIdentityUnverifiableError(
                f"Authority has no execution record for session '{res_session_id}'; cannot verify outcome '{outcome}'"
            )

        if res_result_id != authority_exec_id:
            raise ResourceIdentityUnverifiableError(
                f"Result result_id '{res_result_id}' does not match authority execution_id '{authority_exec_id}' for session '{res_session_id}'"
            )

        if outcome == "SUCCEEDED":
            # Obtain raw artifact bytes strictly from the authentic authority
            if not hasattr(self.transport, "get_artifact") or not callable(getattr(self.transport, "get_artifact")):
                raise AdapterError(
                    "ARTIFACT_AUTHORITY_UNSUPPORTED",
                    "Transport does not support authentic artifact retrieval; cannot verify SUCCEEDED outcome",
                    operation="verify_artifact",
                )

            # Retrieve from authority, verifying session binding and result_id
            authority_bytes, authority_sha = self.transport.get_artifact(
                artifact_ref,
                session_id=res_session_id,
                result_id=res_result_id,
            )

            if authority_bytes is None:
                raise AdapterError(
                    "ARTIFACT_BYTES_MISSING",
                    f"Authentic authority returned no bytes for artifact '{artifact_ref}'",
                    operation="verify_artifact",
                )

            # If caller passed raw bytes as well, ensure strict match with authority bytes
            if raw_artifact_bytes is not None and raw_artifact_bytes != authority_bytes:
                raise AdapterError(
                    "ARTIFACT_CONTENT_MISMATCH",
                    "Supplied raw_artifact_bytes does not match authentic artifact bytes stored in authority",
                    operation="verify_artifact",
                )

            # Verify cryptographic hash against contractual evidence
            actual_sha = hashlib.sha256(authority_bytes).hexdigest()
            if actual_sha != expected_sha:
                raise AdapterError(
                    "ARTIFACT_INTEGRITY_MISMATCH",
                    f"Computed artifact SHA-256 '{actual_sha}' does not match contractual evidence '{expected_sha}'",
                    operation="verify_artifact",
                )

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
            summary=f"Result from Forge Link session {res_session_id} (outcome={outcome})",
            commit_sha=result_envelope.get("commit_sha"),  # None unless explicitly provided
            generated_artifacts=artifacts,
            execution_time_seconds=duration,
            completed_at=time.time(),
            fencing_token_used=gen_raw,
        )
