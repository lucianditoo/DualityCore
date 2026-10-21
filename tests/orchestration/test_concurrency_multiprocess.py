"""Multiprocess concurrency tests verifying cross-process locking and OCC under contention."""
import multiprocessing as mp
import os
from pathlib import Path
import tempfile
import time
import unittest

from orchestration.errors import (
    ResourceConflictError,
    PermanentError,
)
from orchestration.models import TaskEnvelope, TaskPhase
from orchestration.persistence import TaskStore
from orchestration.lease_manager import LeaseManager


def _worker_acquire_lease(store_dir: str, task_id: str, agent_id: str, result_queue: mp.Queue):
    """Worker executed in an independent OS process to acquire lease."""
    try:
        store = TaskStore(store_dir)
        mgr = LeaseManager(store)
        task = store.load_task(task_id)
        token = mgr.acquire_lease(task, agent_id=agent_id, ttl_seconds=60.0)
        result_queue.put(("SUCCESS", agent_id, token))
    except Exception as e:
        result_queue.put(("ERROR", agent_id, type(e).__name__, str(e)))


def _worker_update_task(store_dir: str, task_id: str, worker_id: str, expected_version: int, result_queue: mp.Queue):
    """Worker executed in an independent OS process attempting optimistic concurrency update."""
    try:
        store = TaskStore(store_dir)
        task = store.load_task(task_id)
        task.title = f"Updated by {worker_id}"
        task.state_version += 1
        store.save_task(task, expected_version=expected_version)
        result_queue.put(("SUCCESS", worker_id))
    except Exception as e:
        result_queue.put(("ERROR", worker_id, type(e).__name__, str(e)))


class TestMultiprocessConcurrency(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.store = TaskStore(self.temp_dir.name)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_two_processes_competing_for_same_resource(self):
        """Two independent processes attempt to acquire leases overlapping on the same path."""
        task1 = TaskEnvelope.create("mp-key-1", "Task 1", "Inst", "b", "c", ["shared/module.py"])
        self.store.save_task(task1)

        task2 = TaskEnvelope.create("mp-key-2", "Task 2", "Inst", "b", "c", ["shared/module.py", "other/file.py"])
        self.store.save_task(task2)

        ctx = mp.get_context("spawn")
        queue = ctx.Queue()

        p1 = ctx.Process(target=_worker_acquire_lease, args=(self.temp_dir.name, task1.task_id, "agent-proc-1", queue))
        p2 = ctx.Process(target=_worker_acquire_lease, args=(self.temp_dir.name, task2.task_id, "agent-proc-2", queue))

        p1.start()
        p2.start()

        p1.join(timeout=10)
        p2.join(timeout=10)

        results = []
        while not queue.empty():
            results.append(queue.get())

        self.assertEqual(len(results), 2)
        statuses = [r[0] for r in results]

        # Exactly one must succeed, and one must be rejected with ResourceConflictError
        self.assertIn("SUCCESS", statuses)
        self.assertIn("ERROR", statuses)

        error_res = next(r for r in results if r[0] == "ERROR")
        self.assertEqual(error_res[2], "ResourceConflictError")

    def test_two_processes_competing_update_occ_collision(self):
        """Two independent processes attempt to update the same task with expected_version=1."""
        task = TaskEnvelope.create("mp-occ-key", "Task OCC", "Inst", "b", "c", ["file.py"])
        self.store.save_task(task)
        self.assertEqual(task.state_version, 1)

        ctx = mp.get_context("spawn")
        queue = ctx.Queue()

        p1 = ctx.Process(target=_worker_update_task, args=(self.temp_dir.name, task.task_id, "worker-1", 1, queue))
        p2 = ctx.Process(target=_worker_update_task, args=(self.temp_dir.name, task.task_id, "worker-2", 1, queue))

        p1.start()
        p2.start()

        p1.join(timeout=10)
        p2.join(timeout=10)

        results = []
        while not queue.empty():
            results.append(queue.get())

        self.assertEqual(len(results), 2)
        statuses = [r[0] for r in results]

        # Exactly one succeeds, the other gets CONCURRENCY_CONFLICT
        self.assertIn("SUCCESS", statuses)
        self.assertIn("ERROR", statuses)

        error_res = next(r for r in results if r[0] == "ERROR")
        self.assertEqual(error_res[2], "PermanentError")
        self.assertIn("CONCURRENCY_CONFLICT", error_res[3])


if __name__ == "__main__":
    unittest.main()
