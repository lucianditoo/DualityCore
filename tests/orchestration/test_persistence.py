"""Unit tests for atomic persistence, SHA-256 envelope, and crash recovery."""
import json
from pathlib import Path
import tempfile
import time
import unittest

from orchestration.errors import (
    PermanentError,
    IdempotencyConflictError,
)
from orchestration.models import TaskEnvelope, TaskPhase, TaskLease
from orchestration.persistence import (
    TaskStore,
    canonical_bytes,
    decode_canonical_json,
)


class TestPersistence(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.store = TaskStore(self.temp_dir.name)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_save_and_load_task_intact(self):
        task = TaskEnvelope.create("key-p1", "Test Task", "Do something", "branch", "commit", ["file.py"])
        self.store.save_task(task)

        loaded = self.store.load_task(task.task_id)
        self.assertEqual(loaded.task_id, task.task_id)
        self.assertEqual(loaded.payload_digest, task.payload_digest)
        self.assertEqual(loaded.phase, TaskPhase.PENDING)

    def test_tampered_file_triggers_integrity_failure(self):
        task = TaskEnvelope.create("key-p2", "Test Task", "Do something", "branch", "commit", ["file.py"])
        self.store.save_task(task)

        task_file = self.store.tasks_dir / f"{task.task_id}.json"
        data = json.loads(task_file.read_text(encoding="utf-8"))
        data["record"]["title"] = "Maliciously Altered Title"
        task_file.write_text(json.dumps(data), encoding="utf-8")

        with self.assertRaises(PermanentError) as ctx:
            self.store.load_task(task.task_id)
        self.assertEqual(ctx.exception.code, "INTEGRITY_CHECK_FAILED")

    def test_duplicate_json_keys_rejected(self):
        malformed_bytes = b'{"a": 1, "a": 2}'
        with self.assertRaises(PermanentError) as ctx:
            decode_canonical_json(malformed_bytes)
        self.assertEqual(ctx.exception.code, "DUPLICATE_JSON_KEY")

    def test_idempotency_lookup_and_conflict_detection(self):
        task = TaskEnvelope.create("key-idem-1", "Task 1", "Instructions", "b", "c", ["f"])
        self.store.save_task(task)

        found = self.store.find_by_idempotency_key("key-idem-1")
        self.assertIsNotNone(found)
        self.assertEqual(found.task_id, task.task_id)

        conflicting_task = TaskEnvelope.create("key-idem-1", "Different Task", "Diff Instructions", "b", "c", ["f"])
        with self.assertRaises(IdempotencyConflictError):
            self.store.save_task(conflicting_task)

    def test_optimistic_concurrency_control(self):
        task = TaskEnvelope.create("key-occ", "Task OCC", "Inst", "b", "c", ["f"])
        self.store.save_task(task)

        self.assertEqual(task.state_version, 1)

        with self.assertRaises(PermanentError) as ctx:
            self.store.save_task(task, expected_version=2)
        self.assertEqual(ctx.exception.code, "CONCURRENCY_CONFLICT")

        task.transition_to(TaskPhase.ASSIGNED)
        self.store.save_task(task, expected_version=1)

    def test_idempotency_interruption_recovery_reconciliation(self):
        """Test that if crash occurs between task write and idempotency index write, recovery reconciles."""
        task = TaskEnvelope.create("key-interrupted", "Task Interrupted", "Inst", "b", "c", ["f"])
        self.store.save_task(task)

        # Intentionally delete the idempotency entry to simulate interruption/crash
        safe_key = task.idempotency_key
        import hashlib
        h = hashlib.sha256(safe_key.encode('utf-8')).hexdigest()
        key_file = self.store.idempotency_dir / f"{h}.json"
        self.assertTrue(key_file.exists())
        key_file.unlink()

        # Task store find_by_idempotency_key would now return None
        self.assertIsNone(self.store.find_by_idempotency_key(safe_key))

        # Run recovery
        recovered, quarantined = self.store.recover_on_startup()
        self.assertIn(task.task_id, recovered)

        # Idempotency index must be reconstructed!
        self.assertTrue(key_file.exists())
        found_after = self.store.find_by_idempotency_key(safe_key)
        self.assertIsNotNone(found_after)
        self.assertEqual(found_after.task_id, task.task_id)

    def test_conservative_crash_recovery(self):
        running_task = TaskEnvelope.create("key-run", "Running Task", "Inst", "b", "c", ["f"])
        running_task.transition_to(TaskPhase.ASSIGNED)
        running_task.transition_to(TaskPhase.RUNNING)
        self.store.save_task(running_task)

        completed_task = TaskEnvelope.create("key-comp", "Done Task", "Inst", "b", "c", ["f"])
        completed_task.transition_to(TaskPhase.ASSIGNED)
        completed_task.transition_to(TaskPhase.RUNNING)
        completed_task.transition_to(TaskPhase.AWAITING_REVIEW)
        completed_task.transition_to(TaskPhase.APPROVED)
        completed_task.transition_to(TaskPhase.MERGING)
        completed_task.transition_to(TaskPhase.COMPLETED)
        self.store.save_task(completed_task)

        dangling_tmp = self.store.tasks_dir / ".tmp-dangling-123.json"
        dangling_tmp.write_text("temporary junk", encoding="utf-8")

        recovered, quarantined = self.store.recover_on_startup()

        self.assertIn(completed_task.task_id, recovered)
        self.assertFalse(dangling_tmp.exists(), "Dangling temp file should have been cleaned up")

        quarantined_ids = [q.split(":")[0] for q in quarantined]
        self.assertIn(running_task.task_id, quarantined_ids)

        reloaded_running = self.store.load_task(running_task.task_id)
        self.assertEqual(reloaded_running.phase, TaskPhase.QUARANTINED)
        self.assertIn("CRASH_RECOVERY", reloaded_running.quarantine_reason)


if __name__ == "__main__":
    unittest.main()
