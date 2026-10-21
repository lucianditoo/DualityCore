"""End-to-end simulated lifecycle and split-brain defense tests for DMAOL."""
import tempfile
import time
import unittest

from orchestration.errors import (
    FencingTokenMismatchError,
    SecurityViolationError,
    PermanentError,
)
from orchestration.models import (
    TaskEnvelope,
    TaskPhase,
    ExecutionResult,
    GitMergeRecord,
)
from orchestration.persistence import TaskStore
from orchestration.lease_manager import LeaseManager
from orchestration.contracts import (
    SimulatedSessionDriver,
    SimulatedAgentAdapter,
    CryptographicHumanGate,
)
from orchestration.git_policy import GitPolicy


class TestSimulatedPipeline(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.store = TaskStore(self.temp_dir.name)
        self.mgr = LeaseManager(self.store)
        self.driver = SimulatedSessionDriver()
        self.human_gate = CryptographicHumanGate()
        self.operator_secret = b"authentic-operator-secret-32bytes!"

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_full_simulated_multiagent_lifecycle(self):
        task = TaskEnvelope.create(
            idempotency_key="pipeline-task-001",
            title="Implement resilient session manager",
            instructions="Add pidfd verification to avoid pid reuse",
            target_branch="duality/task-001/agent-codex",
            base_commit="commit-root-111",
            affected_paths=["executor/sessions.py"]
        )
        GitPolicy.validate_agent_branch(task.target_branch)
        self.store.save_task(task)
        self.assertEqual(task.phase, TaskPhase.PENDING)

        session_id = "codex-sess-001"
        self.driver.create_session(session_id)
        fencing_token = self.mgr.acquire_lease(task, agent_id="codex-worker-1", ttl_seconds=300.0)
        self.assertEqual(fencing_token, 1)

        task.transition_to(TaskPhase.RUNNING)
        self.store.save_task(task)

        # Agent yields ExecutionResult
        execution_result = ExecutionResult(
            status="SUCCESS",
            summary="Implemented pidfd_open and signal checks",
            commit_sha="a" * 40,
            generated_artifacts=["test_report.json"],
            execution_time_seconds=42.0,
            completed_at=time.time(),
            fencing_token_used=fencing_token
        )

        # Atomic submission verifies version, lease, and fencing token in one transaction
        task = self.mgr.submit_execution_result(
            task.task_id,
            result=execution_result,
            expected_fencing_token=fencing_token,
            expected_version=task.state_version
        )
        self.assertEqual(task.phase, TaskPhase.AWAITING_REVIEW)

        # Agent cannot self-approve
        with self.assertRaises(SecurityViolationError):
            self.human_gate.submit_decision(
                task, "agent-codex", "APPROVED", "I self approve", self.operator_secret
            )

        # Forged approval with wrong key fails verification
        forged_approval = self.human_gate.submit_decision(
            task, "lead-architect", "APPROVED", "Approved", b"attacker-key-16bytes!"
        )
        with self.assertRaises(SecurityViolationError):
            self.human_gate.verify_decision(task, forged_approval, self.operator_secret)

        # Authentic human approval
        authentic_approval = self.human_gate.submit_decision(
            task, "lead-architect", "APPROVED", "Verified", self.operator_secret
        )
        self.assertTrue(self.human_gate.verify_decision(task, authentic_approval, self.operator_secret))

        task.human_approval = authentic_approval
        task.transition_to(TaskPhase.APPROVED)
        self.store.save_task(task)

        # Merge process
        task.transition_to(TaskPhase.MERGING)
        target_final_branch = GitPolicy.validate_target_branch("main", allow_protected=True)
        merge_record = GitMergeRecord(
            source_branch=task.target_branch,
            target_branch=target_final_branch,
            merged_commit_sha="b" * 40,
            merge_strategy="squash",
            merged_by=authentic_approval.approver_id,
            merged_at=time.time()
        )
        task.git_merge = merge_record
        task.transition_to(TaskPhase.COMPLETED)
        self.store.save_task(task)

        self.driver.destroy_session(session_id)
        self.assertTrue(self.driver.is_confirmed_dead(session_id))

        reloaded = self.store.load_task(task.task_id)
        self.assertEqual(reloaded.phase, TaskPhase.COMPLETED)
        self.assertIsNotNone(reloaded.execution_result)
        self.assertIsNotNone(reloaded.human_approval)
        self.assertIsNotNone(reloaded.git_merge)

    def test_split_brain_stale_fencing_token_rejection(self):
        task = TaskEnvelope.create(
            idempotency_key="split-brain-001",
            title="Refactor database",
            instructions="Migrate schema",
            target_branch="duality/task-split/agent-1",
            base_commit="c00",
            affected_paths=["db/schema.sql"]
        )
        self.store.save_task(task)

        token_agent1 = self.mgr.acquire_lease(task, agent_id="agent-slow", ttl_seconds=10.0, now=100.0)
        self.assertEqual(token_agent1, 1)

        # Lease expires at t=200, executor is confirmed dead
        reconciled = self.mgr.reconcile_expired_lease(
            task.task_id,
            is_executor_confirmed_dead_fn=lambda aid: True,
            now=200.0
        )
        self.assertEqual(reconciled.phase, TaskPhase.PENDING)

        # Agent 2 acquires lease (Generation = 2)
        token_agent2 = self.mgr.acquire_lease(reconciled, agent_id="agent-fast", ttl_seconds=100.0, now=210.0)
        self.assertEqual(token_agent2, 2)
        reconciled.transition_to(TaskPhase.RUNNING)
        self.store.save_task(reconciled)

        # Agent 1 attempts atomic submission with stale token 1 -> REJECTED!
        stale_result = ExecutionResult(
            status="SUCCESS",
            summary="Late commit",
            commit_sha="x" * 40,
            generated_artifacts=[],
            execution_time_seconds=100.0,
            completed_at=220.0,
            fencing_token_used=token_agent1
        )
        with self.assertRaises(FencingTokenMismatchError):
            self.mgr.submit_execution_result(
                task.task_id,
                result=stale_result,
                expected_fencing_token=token_agent1,
                expected_version=reconciled.state_version,
                now=220.0
            )

        # Agent 2 submits with active token 2 -> SUCCEEDS!
        valid_result = ExecutionResult(
            status="SUCCESS",
            summary="Valid commit by Agent 2",
            commit_sha="y" * 40,
            generated_artifacts=[],
            execution_time_seconds=20.0,
            completed_at=230.0,
            fencing_token_used=token_agent2
        )
        final_task = self.mgr.submit_execution_result(
            task.task_id,
            result=valid_result,
            expected_fencing_token=token_agent2,
            expected_version=reconciled.state_version,
            now=230.0
        )
        self.assertEqual(final_task.phase, TaskPhase.AWAITING_REVIEW)
        self.assertEqual(final_task.execution_result.commit_sha, "y" * 40)


if __name__ == "__main__":
    unittest.main()
