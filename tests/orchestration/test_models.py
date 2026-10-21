"""Unit tests for DMAOL models, transitions, and fencing invariants."""
import time
import unittest

from orchestration.errors import (
    IllegalStateTransitionError,
    FencingTokenMismatchError,
)
from orchestration.models import (
    TaskEnvelope,
    TaskPhase,
    TaskLease,
    ExecutionResult,
    HumanApproval,
    GitMergeRecord,
    RetryPolicy,
    compute_payload_digest,
    compute_approval_signature,
    VALID_TRANSITIONS,
)


class TestTaskModels(unittest.TestCase):

    def test_compute_payload_digest_is_deterministic(self):
        digest1 = compute_payload_digest(
            title="Refactor auth",
            instructions="Implement OAuth2 token verification",
            target_branch="feature/auth",
            base_commit="abc1234",
            affected_paths=["src/auth.py", "src/models.py"]
        )
        digest2 = compute_payload_digest(
            title=" Refactor auth ",
            instructions="Implement OAuth2 token verification",
            target_branch="feature/auth",
            base_commit="abc1234",
            affected_paths=["src/models.py", "src/auth.py"]
        )
        self.assertEqual(digest1, digest2)

        digest3 = compute_payload_digest(
            title="Refactor auth",
            instructions="Implement SAML verification",
            target_branch="feature/auth",
            base_commit="abc1234",
            affected_paths=["src/auth.py", "src/models.py"]
        )
        self.assertNotEqual(digest1, digest3)

    def test_task_creation_defaults(self):
        task = TaskEnvelope.create(
            idempotency_key="key-001",
            title="Fix bug",
            instructions="Fix memory leak",
            target_branch="duality/task-fix/agent-1",
            base_commit="commit-123",
            affected_paths=["src/leak.py"]
        )
        self.assertEqual(task.phase, TaskPhase.PENDING)
        self.assertEqual(task.fencing_token, 0)
        self.assertEqual(task.state_version, 1)
        self.assertIsNone(task.assigned_agent)
        self.assertIsNone(task.lease)
        self.assertIsNone(task.execution_result)
        self.assertIsNone(task.human_approval)
        self.assertIsNone(task.git_merge)

    def test_legal_state_transitions(self):
        task = TaskEnvelope.create("key-002", "T", "I", "b", "c", ["p"])
        
        task.transition_to(TaskPhase.ASSIGNED)
        self.assertEqual(task.phase, TaskPhase.ASSIGNED)
        self.assertEqual(task.state_version, 2)

        task.transition_to(TaskPhase.RUNNING)
        self.assertEqual(task.phase, TaskPhase.RUNNING)
        self.assertEqual(task.state_version, 3)

        task.transition_to(TaskPhase.AWAITING_REVIEW)
        self.assertEqual(task.phase, TaskPhase.AWAITING_REVIEW)

        task.transition_to(TaskPhase.APPROVED)
        self.assertEqual(task.phase, TaskPhase.APPROVED)

        task.transition_to(TaskPhase.MERGING)
        self.assertEqual(task.phase, TaskPhase.MERGING)

        task.transition_to(TaskPhase.COMPLETED)
        self.assertEqual(task.phase, TaskPhase.COMPLETED)

    def test_running_to_pending_is_strictly_forbidden(self):
        """Verify that RUNNING -> PENDING is illegal to prevent active executor duplication."""
        task = TaskEnvelope.create("key-no-run-pend", "T", "I", "b", "c", ["p"])
        task.transition_to(TaskPhase.ASSIGNED)
        task.transition_to(TaskPhase.RUNNING)

        with self.assertRaises(IllegalStateTransitionError) as ctx:
            task.transition_to(TaskPhase.PENDING)
        self.assertIn("RUNNING to PENDING is illegal", str(ctx.exception))

        # Must go through QUARANTINED or CANCELLING instead
        task.transition_to(TaskPhase.QUARANTINED, reason="Executor failure detected")
        self.assertEqual(task.phase, TaskPhase.QUARANTINED)

    def test_illegal_state_transitions_rejected(self):
        task = TaskEnvelope.create("key-003", "T", "I", "b", "c", ["p"])

        with self.assertRaises(IllegalStateTransitionError) as ctx:
            task.transition_to(TaskPhase.COMPLETED)
        self.assertIn("PENDING to COMPLETED is illegal", str(ctx.exception))

        with self.assertRaises(IllegalStateTransitionError):
            task.transition_to(TaskPhase.RUNNING)

        task.transition_to(TaskPhase.ASSIGNED)
        task.transition_to(TaskPhase.RUNNING)
        task.transition_to(TaskPhase.FAILED)
        self.assertEqual(task.phase, TaskPhase.FAILED)

        with self.assertRaises(IllegalStateTransitionError):
            task.transition_to(TaskPhase.PENDING)

    def test_quarantine_transition_and_reason(self):
        task = TaskEnvelope.create("key-004", "T", "I", "b", "c", ["p"])
        task.transition_to(TaskPhase.QUARANTINED, reason="Heartbeat timed out unexpectedly")
        self.assertEqual(task.phase, TaskPhase.QUARANTINED)
        self.assertEqual(task.quarantine_reason, "Heartbeat timed out unexpectedly")

        task.transition_to(TaskPhase.PENDING, reason="Operator verified executor is dead and cleared quarantine")
        self.assertEqual(task.phase, TaskPhase.PENDING)

    def test_fencing_token_verification(self):
        task = TaskEnvelope.create("key-005", "T", "I", "b", "c", ["p"])
        task.fencing_token = 5

        task.verify_fencing_token(5)

        with self.assertRaises(FencingTokenMismatchError) as ctx:
            task.verify_fencing_token(4)
        self.assertEqual(ctx.exception.details["expected_token"], 5)
        self.assertEqual(ctx.exception.details["received_token"], 4)

    def test_human_approval_hmac_signature(self):
        secret = b"super-secret-operator-key-32bytes"
        now = time.time()
        sig = compute_approval_signature("lead-eng", "task-99", "APPROVED", now, secret)
        
        approval = HumanApproval(
            approver_id="lead-eng",
            decision="APPROVED",
            approval_token="tok-1",
            comments="Looks good",
            timestamp=now,
            signature=sig
        )

        # Valid signature matches
        self.assertTrue(approval.verify_signature("task-99", secret))

        # Tampered decision fails
        approval_tampered = HumanApproval(
            approver_id="lead-eng",
            decision="REJECTED",  # Tampered!
            approval_token="tok-1",
            comments="Looks good",
            timestamp=now,
            signature=sig
        )
        self.assertFalse(approval_tampered.verify_signature("task-99", secret))

        # Wrong secret key fails
        self.assertFalse(approval.verify_signature("task-99", b"wrong-secret-key-32bytes"))

    def test_serialization_roundtrip(self):
        task = TaskEnvelope.create("key-006", "T", "I", "b", "c", ["p"])
        task.lease = TaskLease("l-1", "agent-x", 1, 100.0, 400.0, 300.0)
        task.execution_result = ExecutionResult(
            status="SUCCESS",
            summary="All tests pass",
            commit_sha="a" * 40,
            generated_artifacts=["test.log"],
            execution_time_seconds=12.5,
            completed_at=time.time(),
            fencing_token_used=1
        )
        secret = b"auth-key-at-least-16-bytes!"
        now = time.time()
        sig = compute_approval_signature("lead-engineer", task.task_id, "APPROVED", now, secret)
        task.human_approval = HumanApproval(
            approver_id="lead-engineer",
            decision="APPROVED",
            approval_token="token-xyz",
            comments="Looks solid",
            timestamp=now,
            signature=sig
        )
        task.git_merge = GitMergeRecord(
            source_branch="duality/task/ag",
            target_branch="feature/cool",
            merged_commit_sha="b" * 40,
            merge_strategy="merge-commit",
            merged_by="lead-engineer",
            merged_at=time.time()
        )

        d = task.to_dict()
        restored = TaskEnvelope.from_dict(d)

        self.assertEqual(task.task_id, restored.task_id)
        self.assertEqual(task.payload_digest, restored.payload_digest)
        self.assertEqual(task.lease.lease_id, restored.lease.lease_id)
        self.assertEqual(task.execution_result.commit_sha, restored.execution_result.commit_sha)
        self.assertEqual(task.human_approval.signature, restored.human_approval.signature)
        self.assertEqual(task.git_merge.merged_commit_sha, restored.git_merge.merged_commit_sha)


if __name__ == "__main__":
    unittest.main()
