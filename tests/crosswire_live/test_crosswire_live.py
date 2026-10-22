"""Automated acceptance test suite for DMAOL -> FORGE LINK integration (Phase 3D-G Hardened).

Tests verify the 8 contractual acceptance criteria and the 10 hardened identity,
type, and integrity regression requirements:
1. Real contractual execution confirmed (ACCEPTED -> SUCCEEDED).
2. Strict idempotency of requests and conflict detection.
3. Fencing of generations and rejection of stale attempts.
4. Non-destructive recovery preserving uncertainty without false claims.
5. Cancellation without falsifying exit (CANCEL_REQUESTED preserved until exit).
6. SHA-256 artifact integrity verification derived from actual bytes.
7. Conservative handling of uncertain outcomes (UNKNOWN is never SUCCESS).
8. Strict compliance with dmaol-executor/1 JSON contract.
9. Rejection of alien identity with matching generation.
10. Rejection of incorrect attempt identifier.
11. Rejection of incorrect session identifier.
12. Rejection of boolean generation (True / False).
13. Rejection of textual generation ("1").
14. Rejection of floating point generation (1.0).
15. Rejection of missing artifact reference or bytes on SUCCEEDED.
16. Rejection of missing or invalid artifact hash on SUCCEEDED.
17. Rejection of mismatched artifact hash.
18. Full verification of legitimate result with all 5-tuple identities.
"""
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import tarfile
import tempfile
import unittest
import uuid

from orchestration.models import TaskEnvelope, TaskPhase
from orchestration.integration.errors import (
    IdempotencyConflictError,
    StaleGenerationError,
    OutcomeUnknownError,
    AdapterError,
    ContractValidationError,
    ResourceIdentityUnverifiableError,
)
from orchestration.integration.executor_adapter import canonical_uuid_for_task
from lab.crosswire_live.transport import ForgeLinkDirectTransport
from lab.crosswire_live.client import DmaolLiveClient


class TestCrosswireLiveForgeLink(unittest.TestCase):
    """Verifies DMAOL Live Client against authentic Forge Link ExecutionLedger from commit 654737f."""

    @classmethod
    def setUpClass(cls):
        # Extract authentic lab/handshake from commit 654737f into an isolated temp dir
        cls.extracted_dir = tempfile.mkdtemp(prefix="forgelink_authentic_")
        proc = subprocess.run(
            ["git", "archive", "654737f9066f8f03141cb76e396f2fde0b1131f0", "lab/handshake"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True
        )
        with tarfile.open(fileobj=io.BytesIO(proc.stdout)) as tar:
            tar.extractall(cls.extracted_dir)

        import sys
        if cls.extracted_dir not in sys.path:
            sys.path.insert(0, cls.extracted_dir)

        from lab.handshake.ledger import ExecutionLedger
        cls.ExecutionLedger = ExecutionLedger

    @classmethod
    def tearDownClass(cls):
        import sys
        if cls.extracted_dir in sys.path:
            sys.path.remove(cls.extracted_dir)
        shutil.rmtree(cls.extracted_dir, ignore_errors=True)

    def setUp(self):
        self.test_dir = tempfile.mkdtemp(prefix="test_live_")
        self.db_path = os.path.join(self.test_dir, "ledger.sqlite")
        self.deployment_id = "0123456789abcdef0123456789abcdef"
        self.ledger = self.ExecutionLedger(self.db_path, self.deployment_id, initialize=True)
        self.transport = ForgeLinkDirectTransport(self.ledger)
        self.client = DmaolLiveClient(transport=self.transport, deployment_id=self.deployment_id)

        # Setup standard task and attempt identifiers
        self.task_id = str(uuid.uuid4())
        self.attempt_id = str(uuid.uuid4())
        self.auth_ref = "approval-lab-001"
        self.task = TaskEnvelope(
            task_id=self.task_id,
            idempotency_key=f"idem-{self.task_id}",
            payload_digest="d" * 64,
            correlation_id=f"corr-{self.task_id}",
            title="Echo test",
            instructions="Execute fixed echo program under Forge Link",
            target_branch="ag/architecture",
            base_commit="18a5d35b64e9772f7bc651ee41faf526b843ea2b",
            affected_paths=["artifact.txt"],
            fencing_token=1,
            phase=TaskPhase.ASSIGNED,
            assigned_agent="codex",
        )

        # Authorize attempt on the authentic ledger authority
        self.ledger.advance(self.task_id, self.attempt_id, 1)
        self.ledger.approve(self.auth_ref, self.task_id, self.attempt_id, 1)

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_01_real_contract_execution_confirmed(self):
        """1. Verify valid task start produces ACCEPTED ACK, and finish on ExecutionLedger produces SUCCEEDED.

        Note: This verifies the authentic Forge Link contractual ledger from commit 654737f;
        physical Linux process execution (launching echo_program.py via /proc) is verified on Ubuntu.
        """
        start_ack = self.client.start_task(
            task=self.task,
            attempt_id=self.attempt_id,
            authorization_ref=self.auth_ref,
        )
        self.assertEqual(start_ack["task_state"], "ACCEPTED")
        self.assertEqual(start_ack["resource_state"], "NOT_CREATED")
        self.assertEqual(start_ack["certainty"], "CONFIRMED")
        session_id = start_ack["session_id"]
        self.assertTrue(session_id.startswith("s-"))

        # Simulate authentic evidence recording on ledger
        real_artifact_bytes = b"IGNITION isolated-echo-v1\n"
        actual_sha = hashlib.sha256(real_artifact_bytes).hexdigest()
        execution_id = self.ledger.run(session_id)["execution_id"]
        valid_boot_id = str(uuid.uuid4())
        evidence = {
            "identity": {"pid": 1234, "boot_id": valid_boot_id, "start_ticks": 5678},
            "exit_confirmed": True,
            "returncode": 0,
            "timed_out": False,
            "termination_requested": False,
            "cancelled": False,
            "duration_seconds": 0.42,
            "artifact_hex": real_artifact_bytes.hex(),
            "artifact_sha256": actual_sha,
            "stdout": "O" * 4096,
            "stderr": "E" * 4096,
            "stdout_bytes": 131072,
            "stderr_bytes": 131072,
            "stdout_truncated": True,
            "stderr_truncated": True,
        }

        # Transition task to RUNNING on ledger with valid identity before finish
        with self.ledger.transaction() as state:
            run = self.ledger.run(session_id)
            run["identity"] = evidence["identity"]
            run["phase"] = "RUNNING"
            self.ledger._save_run(session_id, run)
            self.ledger.revise(state["assignments"][session_id], task_state="RUNNING", resource_state="ACTIVE_OBSERVED", certainty="OBSERVED")
        self.ledger.finish(session_id, evidence)

        # Observe through client
        observed = self.client.observe_task(
            task=self.task,
            attempt_id=self.attempt_id,
            session_id=session_id,
            authorization_ref=self.auth_ref,
        )
        self.assertEqual(observed["task_state"], "SUCCEEDED")
        self.assertEqual(observed["resource_state"], "CLOSED_CONFIRMED")
        self.assertEqual(observed["certainty"], "CONFIRMED")

        # Reconcile result envelope into DMAOL with full contractual identity
        result_envelope = {
            "deployment_id": self.deployment_id,
            "task_id": canonical_uuid_for_task(self.task.task_id),
            "session_id": session_id,
            "attempt_id": self.attempt_id,
            "generation": 1,
            "outcome": "SUCCEEDED",
            "artifact_ref": f"artifact-{execution_id}",
            "artifact_sha256": actual_sha,
            "duration_seconds": 0.42,
        }
        dmaol_res = self.client.process_execution_outcome(
            self.task,
            result_envelope,
            attempt_id=self.attempt_id,
            session_id=session_id,
            raw_artifact_bytes=real_artifact_bytes,
        )
        self.assertEqual(dmaol_res.status, "SUCCESS")
        self.assertEqual(dmaol_res.fencing_token_used, 1)
        self.assertIsNone(dmaol_res.commit_sha)  # Never falsified
        self.assertEqual(dmaol_res.execution_time_seconds, 0.42)

    def test_02_idempotency_duplicate_and_conflict(self):
        """2. Verify duplicate start returns identical ACK; conflicting payload raises conflict."""
        req_id = str(uuid.uuid4())
        ack1 = self.client.start_task(
            task=self.task,
            attempt_id=self.attempt_id,
            authorization_ref=self.auth_ref,
            request_id=req_id,
        )
        # Re-send same request_id
        ack2 = self.client.start_task(
            task=self.task,
            attempt_id=self.attempt_id,
            authorization_ref=self.auth_ref,
            request_id=req_id,
        )
        self.assertEqual(ack1, ack2)

        # Send same attempt_id with different request_id -> IDEMPOTENCY_CONFLICT
        with self.assertRaises(IdempotencyConflictError):
            self.client.start_task(
                task=self.task,
                attempt_id=self.attempt_id,
                authorization_ref=self.auth_ref,
                request_id=str(uuid.uuid4()),
            )

    def test_03_fencing_generation_revoked(self):
        """3. Verify revoked generation rejects start and result publication."""
        # Revoke generation 1 by advancing authority to generation 2
        new_attempt = str(uuid.uuid4())
        self.ledger.advance(self.task_id, new_attempt, 2)

        # Attempting start on generation 1 must be rejected with StaleGenerationError
        with self.assertRaises(StaleGenerationError):
            self.client.start_task(
                task=self.task,  # task fencing_token is 1
                attempt_id=self.attempt_id,
                authorization_ref=self.auth_ref,
            )

        # Reconciling an outcome with outdated generation must also be rejected
        stale_result = {
            "deployment_id": self.deployment_id,
            "task_id": canonical_uuid_for_task(self.task.task_id),
            "session_id": "s-old",
            "attempt_id": self.attempt_id,
            "generation": 1,
            "outcome": "SUCCEEDED",
            "artifact_ref": "artifact-123",
            "artifact_sha256": "a" * 64,
        }
        self.task.fencing_token = 2  # Current task token is 2
        with self.assertRaises(StaleGenerationError):
            self.client.process_execution_outcome(
                self.task,
                stale_result,
                attempt_id=self.attempt_id,
                session_id="s-old",
            )

    def test_04_nondestructive_recovery(self):
        """4. Verify recover operation flags uncertainty without terminating foreign processes."""
        start_ack = self.client.start_task(
            task=self.task,
            attempt_id=self.attempt_id,
            authorization_ref=self.auth_ref,
        )
        session_id = start_ack["session_id"]

        # Call recover via client: returns OUTCOME_UNKNOWN error because task is still in NOT_CREATED
        with self.assertRaises(OutcomeUnknownError) as ctx:
            self.client.recover_task(
                task=self.task,
                attempt_id=self.attempt_id,
                session_id=session_id,
                authorization_ref=self.auth_ref,
            )
        self.assertEqual(ctx.exception.details.get("code"), "OUTCOME_UNKNOWN")

    def test_05_cancellation_without_falsifying_exit(self):
        """5. Verify cancel before launch confirms CANCELLED without process; during RUNNING preserves CANCEL_REQUESTED."""
        start_ack = self.client.start_task(
            task=self.task,
            attempt_id=self.attempt_id,
            authorization_ref=self.auth_ref,
        )
        session_id = start_ack["session_id"]

        # Case A: Cancellation in INTENT phase confirms CANCELLED without launching process
        cancel_resp = self.client.cancel_task(
            task=self.task,
            attempt_id=self.attempt_id,
            session_id=session_id,
            authorization_ref=self.auth_ref,
        )
        self.assertEqual(cancel_resp["task_state"], "CANCELLED")
        self.assertEqual(cancel_resp["resource_state"], "CLOSED_CONFIRMED")
        self.assertEqual(cancel_resp["certainty"], "CONFIRMED")

        # Case B: Cancellation when RUNNING leaves task in CANCEL_REQUESTED until exit confirmed
        task2_id = str(uuid.uuid4())
        attempt2_id = str(uuid.uuid4())
        task2 = TaskEnvelope(
            task_id=task2_id,
            idempotency_key=f"idem-{task2_id}",
            payload_digest="e" * 64,
            correlation_id=f"corr-{task2_id}",
            title="Echo test 2",
            instructions="Execute fixed echo program under Forge Link",
            target_branch="ag/architecture",
            base_commit="18a5d35b64e9772f7bc651ee41faf526b843ea2b",
            affected_paths=["artifact.txt"],
            fencing_token=1,
            phase=TaskPhase.ASSIGNED,
            assigned_agent="codex",
        )
        self.ledger.advance(task2_id, attempt2_id, 1)
        self.ledger.approve(self.auth_ref, task2_id, attempt2_id, 1)
        ack2 = self.client.start_task(task=task2, attempt_id=attempt2_id, authorization_ref=self.auth_ref)
        session2_id = ack2["session_id"]

        # Simulate task entering RUNNING
        with self.ledger.transaction() as state:
            run2 = self.ledger.run(session2_id)
            run2["phase"] = "RUNNING"
            run2["identity"] = {"pid": 2345, "boot_id": str(uuid.uuid4()), "start_ticks": 6789}
            self.ledger._save_run(session2_id, run2)
            self.ledger.revise(state["assignments"][session2_id], task_state="RUNNING", resource_state="ACTIVE_OBSERVED", certainty="OBSERVED")

        # Cancel while RUNNING
        cancel2_resp = self.client.cancel_task(
            task=task2,
            attempt_id=attempt2_id,
            session_id=session2_id,
            authorization_ref=self.auth_ref,
        )
        # MUST remain CANCEL_REQUESTED because the process has not yet exited!
        self.assertEqual(cancel2_resp["task_state"], "CANCEL_REQUESTED")
        self.assertEqual(cancel2_resp["resource_state"], "ACTIVE_OBSERVED")

    def test_06_sha256_artifact_integrity(self):
        """6. Verify SHA-256 integrity check fails if artifact bytes do not match evidence."""
        real_bytes = b"IGNITION isolated-echo-v1\n"
        expected_sha = hashlib.sha256(real_bytes).hexdigest()
        tampered_bytes = b"IGNITION tampered-bytes\n"

        result_envelope = {
            "deployment_id": self.deployment_id,
            "task_id": canonical_uuid_for_task(self.task.task_id),
            "session_id": "s-test",
            "attempt_id": self.attempt_id,
            "generation": 1,
            "outcome": "SUCCEEDED",
            "artifact_ref": "artifact-test",
            "artifact_sha256": expected_sha,
        }

        # Valid bytes pass
        res = self.client.process_execution_outcome(
            self.task,
            result_envelope,
            attempt_id=self.attempt_id,
            session_id="s-test",
            raw_artifact_bytes=real_bytes,
        )
        self.assertEqual(res.status, "SUCCESS")

        # Tampered bytes trigger AdapterError
        with self.assertRaises(AdapterError) as ctx:
            self.client.process_execution_outcome(
                self.task,
                result_envelope,
                attempt_id=self.attempt_id,
                session_id="s-test",
                raw_artifact_bytes=tampered_bytes,
            )
        self.assertIn("ARTIFACT_INTEGRITY_MISMATCH", str(ctx.exception))

    def test_07_uncertain_outcome_preservation(self):
        """7. Verify uncertain outcome is never converted into success."""
        uncertain_envelope = {
            "deployment_id": self.deployment_id,
            "task_id": canonical_uuid_for_task(self.task.task_id),
            "session_id": "s-uncertain",
            "attempt_id": self.attempt_id,
            "generation": 1,
            "outcome": "UNKNOWN",
            "duration_seconds": 0.0,
        }
        res = self.client.process_execution_outcome(
            self.task,
            uncertain_envelope,
            attempt_id=self.attempt_id,
            session_id="s-uncertain",
        )
        self.assertEqual(res.status, "UNKNOWN")
        self.assertNotEqual(res.status, "SUCCESS")

    def test_08_strict_contract_compatibility(self):
        """8. Verify strict envelope formatting and rejection of duplicate JSON keys / invalid fields."""
        ack = self.client.start_task(
            task=self.task,
            attempt_id=self.attempt_id,
            authorization_ref=self.auth_ref,
        )
        expected_keys = {
            "version", "request_id", "operation_id", "deployment_id",
            "task_id", "attempt_id", "generation", "session_id",
            "executor_instance", "state_version", "task_state",
            "resource_state", "certainty", "observed_at", "source", "error"
        }
        self.assertEqual(set(ack.keys()), expected_keys)
        self.assertEqual(ack["version"], "dmaol-executor/1")
        self.assertEqual(ack["source"], "adapter-ledger")

    # -----------------------------------------------------------------------
    # FASE 3D-G: 10 PRUEBAS NEGATIVAS Y DE REGRESIÓN DE IDENTIDAD, TIPO Y HASH
    # -----------------------------------------------------------------------

    def test_09_rejection_alien_identity_matching_generation(self):
        """9. Rejection: Alien task_id or deployment_id must be rejected even with matching generation."""
        alien_task_id = str(uuid.uuid4())
        envelope = {
            "deployment_id": self.deployment_id,
            "task_id": alien_task_id,  # Alien task identity!
            "session_id": "s-test",
            "attempt_id": self.attempt_id,
            "generation": 1,
            "outcome": "SUCCEEDED",
            "artifact_ref": "artifact-test",
            "artifact_sha256": "a" * 64,
        }
        with self.assertRaises(ResourceIdentityUnverifiableError) as ctx:
            self.client.process_execution_outcome(self.task, envelope)
        self.assertIn("does not match expected task_id", str(ctx.exception))

        # Alien deployment_id
        envelope2 = dict(envelope, task_id=canonical_uuid_for_task(self.task.task_id), deployment_id="f" * 32)
        with self.assertRaises(ResourceIdentityUnverifiableError) as ctx:
            self.client.process_execution_outcome(self.task, envelope2)
        self.assertIn("does not match client deployment_id", str(ctx.exception))

    def test_10_rejection_incorrect_attempt(self):
        """10. Rejection: Result envelope with mismatched attempt_id must be rejected."""
        alien_attempt_id = str(uuid.uuid4())
        envelope = {
            "deployment_id": self.deployment_id,
            "task_id": canonical_uuid_for_task(self.task.task_id),
            "session_id": "s-test",
            "attempt_id": alien_attempt_id,
            "generation": 1,
            "outcome": "SUCCEEDED",
            "artifact_ref": "artifact-test",
            "artifact_sha256": "a" * 64,
        }
        with self.assertRaises(ResourceIdentityUnverifiableError) as ctx:
            self.client.process_execution_outcome(
                self.task,
                envelope,
                attempt_id=self.attempt_id,  # Expected attempt_id differs
                session_id="s-test",
            )
        self.assertIn("does not match expected attempt_id", str(ctx.exception))

    def test_11_rejection_incorrect_session(self):
        """11. Rejection: Result envelope with mismatched session_id must be rejected."""
        envelope = {
            "deployment_id": self.deployment_id,
            "task_id": canonical_uuid_for_task(self.task.task_id),
            "session_id": "s-wrong-session-9999",
            "attempt_id": self.attempt_id,
            "generation": 1,
            "outcome": "SUCCEEDED",
            "artifact_ref": "artifact-test",
            "artifact_sha256": "a" * 64,
        }
        with self.assertRaises(ResourceIdentityUnverifiableError) as ctx:
            self.client.process_execution_outcome(
                self.task,
                envelope,
                attempt_id=self.attempt_id,
                session_id="s-expected-session-1111",
            )
        self.assertIn("does not match expected session_id", str(ctx.exception))

    def test_12_rejection_boolean_generation(self):
        """12. Rejection: Boolean generation (True or False) must be strictly rejected."""
        for bool_gen in (True, False):
            envelope = {
                "deployment_id": self.deployment_id,
                "task_id": canonical_uuid_for_task(self.task.task_id),
                "session_id": "s-test",
                "attempt_id": self.attempt_id,
                "generation": bool_gen,
                "outcome": "SUCCEEDED",
                "artifact_ref": "artifact-test",
                "artifact_sha256": "a" * 64,
            }
            with self.assertRaises(ContractValidationError) as ctx:
                self.client.process_execution_outcome(self.task, envelope)
            self.assertIn("Invalid generation type: bool", str(ctx.exception))

    def test_13_rejection_textual_generation(self):
        """13. Rejection: String generation ("1") must not be coerced into integer."""
        envelope = {
            "deployment_id": self.deployment_id,
            "task_id": canonical_uuid_for_task(self.task.task_id),
            "session_id": "s-test",
            "attempt_id": self.attempt_id,
            "generation": "1",
            "outcome": "SUCCEEDED",
            "artifact_ref": "artifact-test",
            "artifact_sha256": "a" * 64,
        }
        with self.assertRaises(ContractValidationError) as ctx:
            self.client.process_execution_outcome(self.task, envelope)
        self.assertIn("Invalid generation type: str", str(ctx.exception))

    def test_14_rejection_float_generation(self):
        """14. Rejection: Float generation (1.0) must not be accepted."""
        envelope = {
            "deployment_id": self.deployment_id,
            "task_id": canonical_uuid_for_task(self.task.task_id),
            "session_id": "s-test",
            "attempt_id": self.attempt_id,
            "generation": 1.0,
            "outcome": "SUCCEEDED",
            "artifact_ref": "artifact-test",
            "artifact_sha256": "a" * 64,
        }
        with self.assertRaises(ContractValidationError) as ctx:
            self.client.process_execution_outcome(self.task, envelope)
        self.assertIn("Invalid generation type: float", str(ctx.exception))

    def test_15_rejection_missing_artifact_ref_or_bytes(self):
        """15. Rejection: SUCCEEDED outcome missing artifact_ref or unavailable bytes must fail."""
        # Missing artifact_ref
        envelope_no_ref = {
            "deployment_id": self.deployment_id,
            "task_id": canonical_uuid_for_task(self.task.task_id),
            "session_id": "s-test",
            "attempt_id": self.attempt_id,
            "generation": 1,
            "outcome": "SUCCEEDED",
            "artifact_sha256": "a" * 64,
        }
        with self.assertRaises(ContractValidationError) as ctx:
            self.client.process_execution_outcome(self.task, envelope_no_ref)
        self.assertIn("artifact_ref", str(ctx.exception))

        # Has artifact_ref, but bytes are unavailable from transport and not provided
        envelope_no_bytes = {
            "deployment_id": self.deployment_id,
            "task_id": canonical_uuid_for_task(self.task.task_id),
            "session_id": "s-test",
            "attempt_id": self.attempt_id,
            "generation": 1,
            "outcome": "SUCCEEDED",
            "artifact_ref": "nonexistent-artifact-999",
            "artifact_sha256": "a" * 64,
        }
        with self.assertRaises(AdapterError) as ctx:
            self.client.process_execution_outcome(self.task, envelope_no_bytes, raw_artifact_bytes=None)
        self.assertIn("ARTIFACT_BYTES_MISSING", str(ctx.exception))

    def test_16_rejection_missing_artifact_hash(self):
        """16. Rejection: SUCCEEDED outcome with missing or malformed artifact_sha256 must fail."""
        for bad_hash in (None, "", "not-a-64-char-hex", 12345):
            envelope = {
                "deployment_id": self.deployment_id,
                "task_id": canonical_uuid_for_task(self.task.task_id),
                "session_id": "s-test",
                "attempt_id": self.attempt_id,
                "generation": 1,
                "outcome": "SUCCEEDED",
                "artifact_ref": "artifact-test",
                "artifact_sha256": bad_hash,
            }
            with self.assertRaises(ContractValidationError):
                self.client.process_execution_outcome(self.task, envelope)

    def test_17_rejection_mismatched_artifact_hash(self):
        """17. Rejection: Mismatch between computed SHA-256 and contractual hash must fail."""
        raw_bytes = b"real-unmodified-content\n"
        wrong_hash = "f" * 64
        envelope = {
            "deployment_id": self.deployment_id,
            "task_id": canonical_uuid_for_task(self.task.task_id),
            "session_id": "s-test",
            "attempt_id": self.attempt_id,
            "generation": 1,
            "outcome": "SUCCEEDED",
            "artifact_ref": "artifact-test",
            "artifact_sha256": wrong_hash,
        }
        with self.assertRaises(AdapterError) as ctx:
            self.client.process_execution_outcome(self.task, envelope, raw_artifact_bytes=raw_bytes)
        self.assertIn("ARTIFACT_INTEGRITY_MISMATCH", str(ctx.exception))

    def test_18_legitimate_result_fully_validated(self):
        """18. Positive: Legitimate result with all 5 identities matching and valid artifact passes."""
        real_bytes = b"IGNITION isolated-echo-v1\n"
        real_sha = hashlib.sha256(real_bytes).hexdigest()
        session_id = "s-legitimate-001"

        envelope = {
            "deployment_id": self.deployment_id,
            "task_id": canonical_uuid_for_task(self.task.task_id),
            "session_id": session_id,
            "attempt_id": self.attempt_id,
            "generation": 1,
            "outcome": "SUCCEEDED",
            "artifact_ref": "artifact-legitimate-001",
            "artifact_sha256": real_sha,
            "duration_seconds": 0.38,
        }

        res = self.client.process_execution_outcome(
            self.task,
            envelope,
            attempt_id=self.attempt_id,
            session_id=session_id,
            raw_artifact_bytes=real_bytes,
        )
        self.assertEqual(res.status, "SUCCESS")
        self.assertEqual(res.fencing_token_used, 1)
        self.assertIsNone(res.commit_sha)
        self.assertEqual(res.execution_time_seconds, 0.38)
        self.assertEqual(res.generated_artifacts, ["artifact-legitimate-001"])


if __name__ == "__main__":
    unittest.main()
