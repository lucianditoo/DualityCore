"""DUALITY CORE - Fase 3C: Pruebas automatizadas de interoperabilidad cruzada (CROSSWIRE).

Verifica la comunicacion contractual real entre DmaolExecutorAdapter (Antigravity)
y Simulator (Codex commit c44af038c54135a03fb1e695a375b1d13f57c456).
"""
import hashlib
import importlib.util
from pathlib import Path
import subprocess
import tempfile
import time
import unittest
import uuid

from orchestration.models import TaskEnvelope
from orchestration.persistence import TaskStore
from orchestration.lease_manager import LeaseManager
from orchestration.integration.errors import (
    ContractValidationError,
    UnauthorizedError,
    StaleGenerationError,
    IdempotencyConflictError,
    AlreadyTerminalError,
    OutcomeUnknownError,
    ResourceIdentityUnverifiableError,
)
from orchestration.integration.contract_messages import (
    CONTRACT_VERSION,
    dump_strict_json,
    parse_strict_json,
    validate_response_message,
)
from orchestration.integration.executor_adapter import DmaolExecutorAdapter, canonical_uuid_for_task

CODEX_SIMULATOR_COMMIT = "c44af038c54135a03fb1e695a375b1d13f57c456"


class CodexSimulatorInMemoryTransport:
    """In-memory transport bridging DmaolExecutorAdapter and Codex Simulator.handle()."""

    def __init__(self, simulator):
        self.simulator = simulator
        self.drop_next_ack: bool = False

    def send_request(self, raw_request_bytes: bytes) -> bytes:
        if self.drop_next_ack:
            self.drop_next_ack = False
            # Simulate dispatch and server commit, but network drops the response
            self.simulator.handle(raw_request_bytes)
            raise OutcomeUnknownError(
                "Network timeout: response lost in transit (lost ACK)",
                operation="start"
            )

        resp_dict = self.simulator.handle(raw_request_bytes)
        # Test wire JSON serialization strictly
        return dump_strict_json(resp_dict)


class TestCrosswireInteroperability(unittest.TestCase):
    """Interoperability test suite connecting Antigravity DMAOL Adapter to Codex Simulator."""

    @classmethod
    def setUpClass(cls):
        # Extract Codex Simulator code directly from Git into isolated temporary workspace
        cls.sim_tmp = tempfile.TemporaryDirectory()
        sim_code_path = Path(cls.sim_tmp.name) / "codex_simulator.py"
        cmd = ["git", "show", f"{CODEX_SIMULATOR_COMMIT}:lab/handshake/simulator.py"]
        code = subprocess.check_output(cmd, text=True)
        sim_code_path.write_text(code, encoding="utf-8")

        spec = importlib.util.spec_from_file_location("codex_simulator", str(sim_code_path))
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        cls.CodexSimulator = mod.Simulator

    @classmethod
    def tearDownClass(cls):
        cls.sim_tmp.cleanup()

    def setUp(self):
        self.test_dir = tempfile.TemporaryDirectory()
        self.base_path = Path(self.test_dir.name)

        # Setup DMAOL store & lease manager
        self.store = TaskStore(str(self.base_path / "dmaol_store"))
        self.lease_mgr = LeaseManager(self.store)

        # Setup Codex Simulator with SQLite in test_dir
        self.db_path = str(self.base_path / "codex_sim.db")
        self.deployment_id = "0123456789abcdef0123456789abcdef"
        self.simulator = self.CodexSimulator(self.db_path, self.deployment_id, simulated_channel=True)

        self.transport = CodexSimulatorInMemoryTransport(self.simulator)
        self.adapter = DmaolExecutorAdapter(self.transport, deployment_id=self.deployment_id)

    def tearDown(self):
        # Close SQLite database connection in Codex Simulator to release file locks on Windows
        self.simulator.close()
        self.test_dir.cleanup()

    def _create_task_and_fixture(self, key: str = "t-crosswire-1", auth_ref: str = "app-crosswire-01"):
        task = TaskEnvelope.create(
            idempotency_key=key,
            title="Crosswire test task",
            instructions="Execute interoperability test",
            target_branch="duality/task-crosswire/01",
            base_commit="commit-sha-crosswire-01",
            affected_paths=["src/crosswire.py"]
        )
        self.store.save_task(task)
        gen = self.lease_mgr.acquire_lease(task, agent_id="codex-executor-sim")
        c_task_id = canonical_uuid_for_task(task.task_id)
        attempt_id = str(uuid.uuid4())

        # Configure trusted authority and approval in Codex Simulator
        self.simulator.advance(c_task_id, attempt_id, gen)
        self.simulator.approve(auth_ref, c_task_id, attempt_id, gen, actions=("start", "observe", "cancel", "recover"))
        return task, attempt_id, gen, auth_ref

    def test_crosswire_start_and_session_allocation(self):
        """1. Verify start operation allocates a valid session_id and transitions task_state to ACCEPTED."""
        task, attempt_id, gen, auth_ref = self._create_task_and_fixture("t-start-01", "app-start-01")

        resp = self.adapter.start_attempt(
            task=task,
            authorization_ref=auth_ref,
            attempt_id=attempt_id,
            profile_ref="isolated-task-v1",
            input_ref="input-01",
            input_sha256="a" * 64,
            execution_timeout_ms=60000
        )

        self.assertEqual(resp["version"], CONTRACT_VERSION)
        self.assertEqual(resp["task_state"], "ACCEPTED")
        self.assertEqual(resp["resource_state"], "NOT_CREATED")
        self.assertEqual(resp["certainty"], "CONFIRMED")
        self.assertEqual(resp["state_version"], 1)
        self.assertIsNone(resp["error"])
        self.assertTrue(resp["session_id"].startswith("s-"))
        self.assertEqual(len(resp["session_id"]), 34)

    def test_crosswire_observe_by_tuple(self):
        """2. Verify observe by bound 5-tuple queries active session state cleanly."""
        task, attempt_id, gen, auth_ref = self._create_task_and_fixture("t-obs-01", "app-obs-01")
        start_resp = self.adapter.start_attempt(task=task, authorization_ref=auth_ref, attempt_id=attempt_id)
        session_id = start_resp["session_id"]

        obs_resp = self.adapter.observe_tuple(
            task=task,
            attempt_id=attempt_id,
            session_id=session_id,
            generation=gen,
            authorization_ref=auth_ref
        )

        self.assertEqual(obs_resp["task_state"], "ACCEPTED")
        self.assertEqual(obs_resp["session_id"], session_id)
        self.assertEqual(obs_resp["state_version"], 1)
        self.assertIsNone(obs_resp["error"])

    def test_crosswire_lost_ack_and_lookup_recovery(self):
        """3. Verify lost ACK recovery querying observe with lookup_request_id retrieves session without duplicate effects."""
        task, attempt_id, gen, auth_ref = self._create_task_and_fixture("t-lost-ack-01", "app-lost-ack-01")
        req_id = str(uuid.uuid4())

        self.transport.drop_next_ack = True
        with self.assertRaises(OutcomeUnknownError):
            self.adapter.start_attempt(
                task=task,
                authorization_ref=auth_ref,
                attempt_id=attempt_id,
                request_id=req_id
            )

        # Recover using lookup_request_id
        recovered = self.adapter.observe_lost_ack(
            original_request_id=req_id,
            authorization_ref=auth_ref
        )

        self.assertEqual(recovered["task_state"], "ACCEPTED")
        self.assertTrue(recovered["session_id"].startswith("s-"))
        self.assertEqual(recovered["state_version"], 1)
        self.assertIsNone(recovered["error"])

    def test_crosswire_cancellation(self):
        """4. Verify cancellation transitions task_state to CANCEL_REQUESTED and increments state_version."""
        task, attempt_id, gen, auth_ref = self._create_task_and_fixture("t-cancel-01", "app-cancel-01")
        start_resp = self.adapter.start_attempt(task=task, authorization_ref=auth_ref, attempt_id=attempt_id)
        session_id = start_resp["session_id"]

        cancel_resp = self.adapter.cancel_attempt(
            task=task,
            attempt_id=attempt_id,
            session_id=session_id,
            generation=gen,
            authorization_ref=auth_ref,
            reason_code="OPERATOR_REQUEST"
        )

        self.assertEqual(cancel_resp["task_state"], "CANCEL_REQUESTED")
        self.assertEqual(cancel_resp["state_version"], 2)
        self.assertIsNone(cancel_resp["error"])

    def test_crosswire_conservative_recover(self):
        """5. Verify conservative recover operation flags OUTCOME_UNKNOWN and task_state UNKNOWN for operator audit."""
        task, attempt_id, gen, auth_ref = self._create_task_and_fixture("t-recover-01", "app-rec-01")
        start_resp = self.adapter.start_attempt(task=task, authorization_ref=auth_ref, attempt_id=attempt_id)
        session_id = start_resp["session_id"]

        # Call recover with raise_on_error=False to inspect the raw contractual response
        rec_resp = self.adapter.recover_attempt(
            task=task,
            attempt_id=attempt_id,
            session_id=session_id,
            generation=gen,
            authorization_ref=auth_ref,
            raise_on_error=False
        )

        self.assertEqual(rec_resp["task_state"], "UNKNOWN")
        self.assertEqual(rec_resp["resource_state"], "UNKNOWN")
        self.assertEqual(rec_resp["certainty"], "UNKNOWN")
        self.assertEqual(rec_resp["state_version"], 2)
        self.assertIsNotNone(rec_resp["error"])
        self.assertEqual(rec_resp["error"]["code"], "OUTCOME_UNKNOWN")
        self.assertTrue(rec_resp["error"]["operator_review_required"])

    def test_crosswire_duplicate_request_idempotency(self):
        """6. Verify duplicate request returns identical response, while conflicting payload returns IDEMPOTENCY_CONFLICT."""
        task, attempt_id, gen, auth_ref = self._create_task_and_fixture("t-idemp-01", "app-idemp-01")
        req_id = str(uuid.uuid4())

        # First request
        resp1 = self.adapter.start_attempt(
            task=task,
            authorization_ref=auth_ref,
            attempt_id=attempt_id,
            request_id=req_id,
            input_ref="input-v1"
        )

        # Duplicate request with exact same payload
        resp2 = self.adapter.start_attempt(
            task=task,
            authorization_ref=auth_ref,
            attempt_id=attempt_id,
            request_id=req_id,
            input_ref="input-v1"
        )
        self.assertEqual(resp1["session_id"], resp2["session_id"])
        self.assertEqual(resp1["operation_id"], resp2["operation_id"])

        # Conflicting request with same request_id but altered payload raises IdempotencyConflictError
        with self.assertRaises(IdempotencyConflictError):
            self.adapter.start_attempt(
                task=task,
                authorization_ref=auth_ref,
                attempt_id=attempt_id,
                request_id=req_id,
                input_ref="conflicting-input-v2"
            )

    def test_crosswire_stale_generation_rejected(self):
        """7. Verify stale generation in start or cancel is rejected with STALE_GENERATION."""
        task, attempt_id, gen, auth_ref = self._create_task_and_fixture("t-stale-01", "app-stale-01")
        c_task_id = canonical_uuid_for_task(task.task_id)

        # Advance authority in Codex Simulator to generation 2
        new_attempt = str(uuid.uuid4())
        self.simulator.advance(c_task_id, new_attempt, generation=2)

        # Attempt to start with stale generation 1
        with self.assertRaises(StaleGenerationError):
            self.adapter.start_attempt(
                task=task, # task has generation 1
                authorization_ref=auth_ref,
                attempt_id=attempt_id
            )

    def test_crosswire_incorrect_attempt_and_session(self):
        """8. Verify incorrect session_id or attempt_id is rejected with RESOURCE_IDENTITY_UNVERIFIABLE or UNAUTHORIZED."""
        task, attempt_id, gen, auth_ref = self._create_task_and_fixture("t-tamper-01", "app-tamper-01")
        start_resp = self.adapter.start_attempt(task=task, authorization_ref=auth_ref, attempt_id=attempt_id)
        session_id = start_resp["session_id"]

        # 8a: Tampered session_id
        with self.assertRaises(ResourceIdentityUnverifiableError):
            self.adapter.observe_tuple(
                task=task,
                attempt_id=attempt_id,
                session_id=f"s-{'f' * 32}",
                generation=gen,
                authorization_ref=auth_ref
            )

        # 8b: Tampered attempt_id (rejected by approval/identity check)
        with self.assertRaises((UnauthorizedError, ResourceIdentityUnverifiableError, ContractValidationError)):
            self.adapter.observe_tuple(
                task=task,
                attempt_id=str(uuid.uuid4()),
                session_id=session_id,
                generation=gen,
                authorization_ref=auth_ref
            )

    def test_crosswire_execution_result_conversion_integrity(self):
        """9. Verify execution result conversion never falsifies a Git commit or duration when unmeasured."""
        c_task_id = str(uuid.uuid4())
        attempt_id = str(uuid.uuid4())
        session_id = f"s-{'a' * 32}"
        artifact_hash = "e" * 64

        contractual_result = {
            "deployment_id": self.deployment_id,
            "task_id": c_task_id,
            "attempt_id": attempt_id,
            "generation": 1,
            "session_id": session_id,
            "result_id": str(uuid.uuid4()),
            "outcome": "SUCCEEDED",
            "artifact_ref": "output-report.json",
            "artifact_sha256": artifact_hash,
            "completed_at": 1700000000.0,
        }

        dmaol_res = self.adapter.translate_to_dmaol_execution_result(contractual_result)

        # 1. Status is SUCCESS
        self.assertEqual(dmaol_res.status, "SUCCESS")
        # 2. Commit SHA is strictly None (artifact hash is NOT a git commit)
        self.assertIsNone(dmaol_res.commit_sha)
        # 3. Execution time is 0.0 (unmeasured duration preserved as unknown, not invented)
        self.assertEqual(dmaol_res.execution_time_seconds, 0.0)
        # 4. Artifact reference preserved in generated_artifacts
        self.assertIn("output-report.json", dmaol_res.generated_artifacts)
        # 5. Completed at and fencing token intact
        self.assertEqual(dmaol_res.completed_at, 1700000000.0)
        self.assertEqual(dmaol_res.fencing_token_used, 1)


if __name__ == "__main__":
    unittest.main()
