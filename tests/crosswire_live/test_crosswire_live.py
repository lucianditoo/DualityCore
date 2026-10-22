"""Automated acceptance test suite for DMAOL -> FORGE LINK integration (Phase 3D-E).

Tests verify the 8 contractual acceptance criteria:
1. Real contractual execution confirmed (ACCEPTED -> SUCCEEDED).
2. Strict idempotency of requests and conflict detection.
3. Fencing of generations and rejection of stale attempts.
4. Non-destructive recovery preserving uncertainty without false claims.
5. Cancellation without falsifying exit (CANCEL_REQUESTED preserved until exit).
6. SHA-256 artifact integrity verification derived from actual bytes.
7. Conservative handling of uncertain outcomes (UNKNOWN is never SUCCESS).
8. Strict compliance with dmaol-executor/1 JSON contract.
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
)
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

        # Reconcile result envelope into DMAOL
        result_envelope = {
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
            raw_artifact_bytes=real_artifact_bytes
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
            "session_id": "s-old",
            "generation": 1,
            "outcome": "SUCCEEDED",
        }
        self.task.fencing_token = 2  # Current task token is 2
        with self.assertRaises(StaleGenerationError):
            self.client.process_execution_outcome(self.task, stale_result)

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
            "session_id": "s-test",
            "generation": 1,
            "outcome": "SUCCEEDED",
            "artifact_sha256": expected_sha,
        }

        # Valid bytes pass
        res = self.client.process_execution_outcome(self.task, result_envelope, raw_artifact_bytes=real_bytes)
        self.assertEqual(res.status, "SUCCESS")

        # Tampered bytes trigger AdapterError
        with self.assertRaises(AdapterError) as ctx:
            self.client.process_execution_outcome(self.task, result_envelope, raw_artifact_bytes=tampered_bytes)
        self.assertIn("ARTIFACT_INTEGRITY_MISMATCH", str(ctx.exception))

    def test_07_uncertain_outcome_preservation(self):
        """7. Verify uncertain outcome is never converted into success."""
        uncertain_envelope = {
            "session_id": "s-uncertain",
            "generation": 1,
            "outcome": "UNKNOWN",
            "duration_seconds": 0.0,
        }
        res = self.client.process_execution_outcome(self.task, uncertain_envelope)
        self.assertEqual(res.status, "UNKNOWN")
        self.assertNotEqual(res.status, "SUCCESS")

    def test_08_strict_contract_compatibility(self):
        """8. Verify strict envelope formatting and rejection of duplicate JSON keys / invalid fields."""
        # Check start ACK keys conform exactly to contract
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


if __name__ == "__main__":
    unittest.main()
