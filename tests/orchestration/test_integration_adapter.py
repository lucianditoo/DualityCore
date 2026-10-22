"""Automated tests for DMAOL <-> Executor Integration Adapter (Fase 3B-A)."""
import hashlib
from pathlib import Path
import tempfile
import time
import unittest
import uuid

from orchestration.models import TaskEnvelope, TaskPhase
from orchestration.persistence import TaskStore
from orchestration.lease_manager import LeaseManager
from orchestration.integration.errors import (
    ContractValidationError,
    StaleGenerationError,
    IdempotencyConflictError,
    AlreadyTerminalError,
    OutcomeUnknownError,
    ResourceIdentityUnverifiableError,
)
from orchestration.integration.contract_messages import (
    CONTRACT_VERSION,
    MAX_CONTROL_MESSAGE_BYTES,
    dump_strict_json,
    parse_strict_json,
    validate_request_message,
)
from orchestration.integration.adapter_ledger import AdapterLedger
from orchestration.integration.simulated_transport import SimulatedExecutorTransport
from orchestration.integration.executor_adapter import DmaolExecutorAdapter, canonical_uuid_for_task
from orchestration.integration.demo_handshake import run_handshake_demo


class TestIntegrationAdapter(unittest.TestCase):
    """Test suite verifying contract compliance, negative scenarios, and edge cases."""

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.base_path = Path(self.temp_dir.name)
        self.store = TaskStore(str(self.base_path / "store"))
        self.lease_mgr = LeaseManager(self.store)
        self.ledger = AdapterLedger(str(self.base_path / "ledger"))
        self.transport = SimulatedExecutorTransport(self.ledger)
        self.deployment_id = "0123456789abcdef0123456789abcdef"
        self.adapter = DmaolExecutorAdapter(self.transport, deployment_id=self.deployment_id)

    def tearDown(self):
        self.temp_dir.cleanup()

    def _create_and_lease_task(self, key: str = "t-key-1") -> TaskEnvelope:
        task = TaskEnvelope.create(
            idempotency_key=key,
            title="Test integration task",
            instructions="Execute test instructions",
            target_branch="duality/task-int/01",
            base_commit="commit-sha-01",
            affected_paths=["src/module.py"]
        )
        self.store.save_task(task)
        self.lease_mgr.acquire_lease(task, agent_id="executor-agent-01")
        return task

    def test_handshake_demo_end_to_end(self):
        """Verify the full reproducible handshake demonstration runs successfully."""
        report = run_handshake_demo(self.temp_dir.name, verbose=False)
        self.assertIn("TASK_CREATED", report["steps_completed"])
        self.assertIn("START_ACCEPTED", report["steps_completed"])
        self.assertIn("DMAOL_RESULT_ACCEPTED", report["steps_completed"])
        self.assertIn("PERSISTENCE_RECOVERY_VERIFIED", report["steps_completed"])

    def test_duplicate_response_idempotency(self):
        """Duplicate request_id returns the identical response; conflicting payload raises error."""
        task = self._create_and_lease_task("idempotency-key-01")
        req_id = str(uuid.uuid4())
        attempt_id = str(uuid.uuid4())

        # First request
        resp1 = self.adapter.start_attempt(
            task=task,
            request_id=req_id,
            attempt_id=attempt_id,
            profile_ref="profile-v1",
        )

        # Duplicate identical request returns exact same response
        resp2 = self.adapter.start_attempt(
            task=task,
            request_id=req_id,
            attempt_id=attempt_id,
            profile_ref="profile-v1",
        )
        self.assertEqual(resp1["session_id"], resp2["session_id"])
        self.assertEqual(resp1["operation_id"], resp2["operation_id"])

        # Same request_id with different payload raises IdempotencyConflictError
        with self.assertRaises(IdempotencyConflictError):
            self.adapter.start_attempt(
                task=task,
                request_id=req_id,
                attempt_id=attempt_id,
                profile_ref="conflicting-profile-v2",
            )

    def test_lost_ack_recovery(self):
        """Client recovers from lost ACK by querying observe with lookup_request_id."""
        task = self._create_and_lease_task("lost-ack-key-01")
        req_id = str(uuid.uuid4())

        self.transport.drop_next_ack = True
        with self.assertRaises(OutcomeUnknownError):
            self.adapter.start_attempt(task=task, request_id=req_id)

        # Query observe using lookup_request_id
        recovered = self.adapter.observe_lost_ack(original_request_id=req_id)
        self.assertEqual(recovered["request_id"], req_id)
        self.assertEqual(recovered["task_state"], "ACCEPTED")
        self.assertTrue(recovered["session_id"].startswith("s-"))

    def test_stale_generation_rejected(self):
        """Stale generation in requests or results is rejected."""
        task = self._create_and_lease_task("stale-gen-01")
        # Task is leased at generation 1
        resp = self.adapter.start_attempt(task=task)

        # Release and re-lease to increment generation to 2
        self.lease_mgr.release_lease(task.task_id, fencing_token=1, is_terminal=False)
        fresh_task = self.store.load_task(task.task_id)
        fresh_task.transition_to(TaskPhase.PENDING)
        self.store.save_task(fresh_task)

        gen2 = self.lease_mgr.acquire_lease(fresh_task, agent_id="executor-agent-02")
        self.assertEqual(gen2, 2)

        # Attempt to start with stale generation 1
        with self.assertRaises(StaleGenerationError):
            self.adapter.start_attempt(
                task=task, # task has generation 1
                attempt_id=str(uuid.uuid4())
            )

    def test_attempt_id_mismatch_rejected(self):
        """Result envelope referencing a different attempt_id is rejected."""
        task = self._create_and_lease_task("attempt-mismatch-01")
        attempt_id = str(uuid.uuid4())
        resp = self.adapter.start_attempt(task=task, attempt_id=attempt_id)

        stale_attempt = str(uuid.uuid4())
        mismatched_result = {
            "version": CONTRACT_VERSION,
            "task_id": canonical_uuid_for_task(task.task_id),
            "attempt_id": stale_attempt,
            "generation": task.fencing_token,
            "session_id": resp["session_id"],
            "result_id": str(uuid.uuid4()),
            "outcome": "SUCCEEDED",
            "artifact_ref": "art-01",
            "artifact_sha256": "a" * 64,
            "completed_at": time.time(),
            "signature": "sig-01",
        }

        with self.assertRaises(ContractValidationError):
            self.transport.dispatch_execution_result(dump_strict_json(mismatched_result))

        # Verify preserved as segregated evidence
        segregated = list(self.ledger.results_dir.glob("segregated_*.json"))
        self.assertTrue(len(segregated) >= 1)

    def test_session_id_mismatch_rejected(self):
        """Result envelope referencing a tampered or changed session_id is rejected."""
        task = self._create_and_lease_task("session-mismatch-01")
        attempt_id = str(uuid.uuid4())
        resp = self.adapter.start_attempt(task=task, attempt_id=attempt_id)

        tampered_result = {
            "version": CONTRACT_VERSION,
            "task_id": canonical_uuid_for_task(task.task_id),
            "attempt_id": attempt_id,
            "generation": task.fencing_token,
            "session_id": f"s-{'f' * 32}", # different session_id
            "result_id": str(uuid.uuid4()),
            "outcome": "SUCCEEDED",
            "artifact_ref": "art-01",
            "artifact_sha256": "b" * 64,
            "completed_at": time.time(),
            "signature": "sig-02",
        }

        with self.assertRaises(ResourceIdentityUnverifiableError):
            self.transport.dispatch_execution_result(dump_strict_json(tampered_result))

    def test_late_cancellation_rejected(self):
        """Cancellation of an already terminal task raises AlreadyTerminalError."""
        task = self._create_and_lease_task("late-cancel-01")
        attempt_id = str(uuid.uuid4())
        resp = self.adapter.start_attempt(task=task, attempt_id=attempt_id)
        session_id = resp["session_id"]

        # Complete the task
        valid_result = {
            "version": CONTRACT_VERSION,
            "task_id": canonical_uuid_for_task(task.task_id),
            "attempt_id": attempt_id,
            "generation": task.fencing_token,
            "session_id": session_id,
            "result_id": str(uuid.uuid4()),
            "outcome": "SUCCEEDED",
            "artifact_ref": "art-01",
            "artifact_sha256": "c" * 64,
            "completed_at": time.time(),
            "signature": "sig-03",
        }
        self.transport.dispatch_execution_result(dump_strict_json(valid_result))

        # Attempt to cancel already SUCCEEDED task
        with self.assertRaises(AlreadyTerminalError):
            self.adapter.cancel_attempt(
                task=task,
                attempt_id=attempt_id,
                session_id=session_id,
            )

    def test_restart_between_intent_and_response(self):
        """Crash between intent write and response delivery allows non-destructive recovery."""
        task = self._create_and_lease_task("crash-intent-01")
        req_id = str(uuid.uuid4())

        self.transport.crash_before_ack = True
        with self.assertRaises(OutcomeUnknownError):
            self.adapter.start_attempt(task=task, request_id=req_id)

        # Restart simulation: instantiate fresh ledger and transport
        fresh_ledger = AdapterLedger(str(self.base_path / "ledger"))
        fresh_transport = SimulatedExecutorTransport(fresh_ledger)
        fresh_adapter = DmaolExecutorAdapter(fresh_transport, deployment_id=self.deployment_id)

        # Recovery query via lookup_request_id proves intent was safely committed
        recovered = fresh_adapter.observe_lost_ack(original_request_id=req_id)
        self.assertEqual(recovered["request_id"], req_id)
        self.assertEqual(recovered["task_state"], "ACCEPTED")
        self.assertEqual(recovered["resource_state"], "NOT_CREATED")

    def test_result_from_previous_attempt_rejected(self):
        """Result from a superseded attempt is rejected with StaleGenerationError."""
        task = self._create_and_lease_task("superseded-attempt-01")
        att1 = str(uuid.uuid4())
        resp1 = self.adapter.start_attempt(task=task, attempt_id=att1)

        # Reassign to generation 2
        self.lease_mgr.release_lease(task.task_id, fencing_token=1, is_terminal=False)
        fresh_task = self.store.load_task(task.task_id)
        fresh_task.transition_to(TaskPhase.PENDING)
        self.store.save_task(fresh_task)

        gen2 = self.lease_mgr.acquire_lease(fresh_task, agent_id="executor-agent-02")
        self.assertEqual(gen2, 2)
        att2 = str(uuid.uuid4())
        resp2 = self.adapter.start_attempt(task=fresh_task, attempt_id=att2)

        # Old attempt 1 tries to submit result with generation 1
        stale_result = {
            "version": CONTRACT_VERSION,
            "task_id": canonical_uuid_for_task(task.task_id),
            "attempt_id": att1,
            "generation": 1,
            "session_id": resp1["session_id"],
            "result_id": str(uuid.uuid4()),
            "outcome": "SUCCEEDED",
            "artifact_ref": "art-01",
            "artifact_sha256": "d" * 64,
            "completed_at": time.time(),
            "signature": "sig-old",
        }

        with self.assertRaises(StaleGenerationError):
            self.transport.dispatch_execution_result(dump_strict_json(stale_result))

    def test_strict_message_limits_and_schema(self):
        """Payload exceeding size limit or containing duplicate keys is rejected."""
        # Over 16384 bytes
        large_payload = b"{" + b'"a":1,' * 3000 + b'"version":"dmaol-executor/1"}'
        self.assertGreater(len(large_payload), MAX_CONTROL_MESSAGE_BYTES)
        with self.assertRaises(ContractValidationError):
            self.transport.send_request(large_payload)

        # Duplicate JSON keys
        dup_payload = b'{"version":"dmaol-executor/1","op":"start","op":"observe"}'
        with self.assertRaises(ContractValidationError):
            self.transport.send_request(dup_payload)


if __name__ == "__main__":
    unittest.main()
