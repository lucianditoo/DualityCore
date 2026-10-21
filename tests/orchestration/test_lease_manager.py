"""Unit tests for LeaseManager, atomic result submission, and split-brain prevention."""
import tempfile
import time
import unittest

from orchestration.errors import (
    ResourceConflictError,
    FencingTokenMismatchError,
    LeaseExpiredError,
    PermanentError,
)
from orchestration.models import TaskEnvelope, TaskPhase, ExecutionResult
from orchestration.persistence import TaskStore
from orchestration.lease_manager import LeaseManager


class TestLeaseManager(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.store = TaskStore(self.temp_dir.name)
        self.mgr = LeaseManager(self.store)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_monotonic_fencing_token_generation(self):
        task = TaskEnvelope.create("key-fence-1", "Task", "Inst", "b", "c", ["file1.py"])
        self.store.save_task(task)

        gen1 = self.mgr.acquire_lease(task, "agent-codex", ttl_seconds=100.0)
        self.assertEqual(gen1, 1)
        self.assertEqual(task.fencing_token, 1)
        self.assertEqual(task.phase, TaskPhase.ASSIGNED)

        self.mgr.release_lease(task.task_id, fencing_token=1, is_terminal=False)
        fresh = self.store.load_task(task.task_id)
        fresh.transition_to(TaskPhase.PENDING)
        self.store.save_task(fresh)

        gen2 = self.mgr.acquire_lease(fresh, "agent-antigravity", ttl_seconds=100.0)
        self.assertEqual(gen2, 2)
        self.assertEqual(fresh.fencing_token, 2)

    def test_persistent_path_conflict_detection(self):
        task1 = TaskEnvelope.create("key-pc-1", "Task 1", "Inst", "b", "c", ["src/auth.py", "src/user.py"])
        self.store.save_task(task1)

        task2 = TaskEnvelope.create("key-pc-2", "Task 2", "Inst", "b", "c", ["src/user.py", "src/dashboard.py"])
        self.store.save_task(task2)

        # Agent 1 leases task1
        self.mgr.acquire_lease(task1, "agent-codex")

        # Second manager or process checking task2 detects conflict persistently
        mgr2 = LeaseManager(self.store)
        with self.assertRaises(ResourceConflictError) as ctx:
            mgr2.acquire_lease(task2, "agent-antigravity")
        self.assertEqual(ctx.exception.details["current_holder"], "agent-codex")

        # After releasing task1, task2 can be leased
        self.mgr.release_lease(task1.task_id, fencing_token=1, is_terminal=True)
        gen = mgr2.acquire_lease(task2, "agent-antigravity")
        self.assertEqual(gen, 1)

    def test_atomic_submit_execution_result(self):
        task = TaskEnvelope.create("key-submit-1", "Task", "Inst", "b", "c", ["f.py"])
        self.store.save_task(task)

        token = self.mgr.acquire_lease(task, "agent-codex", ttl_seconds=300.0)
        task.transition_to(TaskPhase.RUNNING)
        self.store.save_task(task)

        result = ExecutionResult(
            status="SUCCESS",
            summary="Completed refactoring",
            commit_sha="a" * 40,
            generated_artifacts=[],
            execution_time_seconds=15.0,
            completed_at=time.time(),
            fencing_token_used=token
        )

        # Atomic submission succeeds
        updated = self.mgr.submit_execution_result(
            task.task_id,
            result=result,
            expected_fencing_token=token,
            expected_version=task.state_version
        )
        self.assertEqual(updated.phase, TaskPhase.AWAITING_REVIEW)
        self.assertEqual(updated.execution_result.commit_sha, "a" * 40)
        self.assertIsNone(updated.lease)

    def test_atomic_submit_rejects_stale_fencing_token(self):
        task = TaskEnvelope.create("key-submit-stale", "Task", "Inst", "b", "c", ["f.py"])
        self.store.save_task(task)

        token = self.mgr.acquire_lease(task, "agent-codex", ttl_seconds=300.0)
        task.transition_to(TaskPhase.RUNNING)
        self.store.save_task(task)

        # Result with obsolete token
        stale_result = ExecutionResult(
            status="SUCCESS",
            summary="Late commit",
            commit_sha="x" * 40,
            generated_artifacts=[],
            execution_time_seconds=15.0,
            completed_at=time.time(),
            fencing_token_used=999  # Stale!
        )

        with self.assertRaises(FencingTokenMismatchError):
            self.mgr.submit_execution_result(
                task.task_id,
                result=stale_result,
                expected_fencing_token=999,
                expected_version=task.state_version
            )

    def test_conservative_expired_lease_reconciliation(self):
        task_unconfirmed = TaskEnvelope.create("key-exp-1", "Task", "Inst", "b", "c", ["f1.py"])
        self.store.save_task(task_unconfirmed)
        self.mgr.acquire_lease(task_unconfirmed, "agent-codex", ttl_seconds=10.0, now=100.0)

        # Death not confirmed -> QUARANTINED
        reconciled = self.mgr.reconcile_expired_lease(
            task_unconfirmed.task_id,
            is_executor_confirmed_dead_fn=lambda aid: False,
            now=200.0
        )
        self.assertEqual(reconciled.phase, TaskPhase.QUARANTINED)
        self.assertIn("SPLIT_BRAIN_GUARD", reconciled.quarantine_reason)

        # Death confirmed -> PENDING
        task_dead = TaskEnvelope.create("key-exp-2", "Task", "Inst", "b", "c", ["f2.py"])
        self.store.save_task(task_dead)
        self.mgr.acquire_lease(task_dead, "agent-codex", ttl_seconds=10.0, now=100.0)

        reconciled_dead = self.mgr.reconcile_expired_lease(
            task_dead.task_id,
            is_executor_confirmed_dead_fn=lambda aid: True,
            now=200.0
        )
        self.assertEqual(reconciled_dead.phase, TaskPhase.PENDING)
        self.assertIsNone(reconciled_dead.assigned_agent)


if __name__ == "__main__":
    unittest.main()
