import concurrent.futures
import os
from pathlib import Path
import sqlite3
import time
import unittest
import uuid
from unittest.mock import patch

from lab.ignition.backend import Laboratory, Backend


class SupervisionTests(unittest.TestCase):
    def setUp(self):
        self.lab = Laboratory()
        self.addCleanup(self.lab.close)
        self.backend = self.lab.backend
        self.request = str(uuid.uuid4())
        self.task, self.attempt = str(uuid.uuid4()), str(uuid.uuid4())
        self.backend.advance(self.task, self.attempt, 1)

    def start(self, timeout=2000):
        return self.backend.start(request_id=self.request, task_id=self.task,
                                  attempt_id=self.attempt, generation=1, timeout_ms=timeout)

    def until(self, condition):
        deadline = time.monotonic() + 4
        while time.monotonic() < deadline:
            value = condition()
            if value:
                return value
            time.sleep(0.005)
        self.fail('condition timed out')

    def test_recover_reopen_never_signals_or_relaunches_live_child(self):
        self.start()
        other = Backend(self.lab.root)
        self.addCleanup(other.close)
        with patch('subprocess.Popen.terminate', side_effect=AssertionError('recover signalled')), patch('subprocess.Popen.kill', side_effect=AssertionError('SIGKILL forbidden')):
            with concurrent.futures.ThreadPoolExecutor() as pool:
                f = pool.submit(self.backend.execute, self.request)
                self.until(lambda: self.backend.facts(self.request)['phase'] == 'RUNNING')
                identity = self.backend.facts(self.request)['identity']
                unknown = other.recover(self.request)
                self.assertEqual(unknown['state'], 'UNKNOWN')
                self.assertIsNone(other.facts(self.request)['evidence'])
                self.assertEqual(other.facts(self.request)['identity'], identity)
                self.assertEqual(other.execute(self.request), unknown)
                done = f.result(timeout=5)
        self.assertEqual(done['state'], 'SUCCEEDED')
        self.assertEqual(done['result']['returncode'], 0)
        self.assertEqual(other.observe(self.request), done)

    def test_timeout_of_resistant_child_preserves_unconfirmed_exit(self):
        self.start(200)
        fixture = Path(__file__).with_name('resistant_program.py')
        with patch('lab.ignition.process_runner.PROGRAM', fixture), patch('subprocess.Popen.kill', side_effect=AssertionError('SIGKILL forbidden')):
            with concurrent.futures.ThreadPoolExecutor() as pool:
                future = pool.submit(self.backend.execute, self.request)
                self.until(lambda: list(self.lab.root.glob('s-*/ready')))
                self.until(lambda: self.backend.facts(self.request)['timed_out'])
                facts = self.backend.facts(self.request)
                self.assertIsNotNone(facts['identity'])
                self.assertTrue(Path('/proc', str(facts['identity']['pid'])).exists())
                self.assertIsNone(facts['evidence'])
                self.assertFalse(future.done())
                self.assertEqual(self.backend.observe(self.request)['state'], 'UNKNOWN')
                done = future.result(timeout=5)
        self.assertEqual(done['state'], 'TIMED_OUT')
        self.assertTrue(done['result']['exit_confirmed'])
        self.assertEqual(done['result']['returncode'], 0)  # Natural exit after ignoring TERM.
        self.assertFalse(done['result']['accepted_as_current'])

    def test_cancel_resistant_child_never_escalates_or_claims_early_exit(self):
        self.start()
        fixture = Path(__file__).with_name('resistant_program.py')
        with patch('lab.ignition.process_runner.PROGRAM', fixture), patch('subprocess.Popen.kill', side_effect=AssertionError('SIGKILL forbidden')):
            with concurrent.futures.ThreadPoolExecutor() as pool:
                future = pool.submit(self.backend.execute, self.request)
                self.until(lambda: list(self.lab.root.glob('s-*/ready')))
                requested = self.backend.cancel(self.request)
                self.assertEqual(requested['state'], 'CANCELLING')
                self.assertIsNone(self.backend.facts(self.request)['evidence'])
                self.assertFalse(future.done())
                done = future.result(timeout=5)
        self.assertEqual(done['state'], 'UNKNOWN')  # No proof of cancellation order.
        self.assertTrue(done['result']['exit_confirmed'])
        self.assertEqual(done['result']['returncode'], 0)
        self.assertTrue(self.backend.facts(self.request)['cancel_requested'])

    def test_standalone_backend_has_no_second_generation_authority(self):
        self.start()
        with sqlite3.connect(self.backend.ledger) as db:
            tables = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        self.assertEqual(tables, {'model', 'runs', 'artifacts', 'execution_policy'})
        with self.backend._db() as ledger:
            current = ledger.snapshot()['authority'][self.task]
        self.assertEqual(current, dict(attempt_id=self.attempt, generation=1))


if __name__ == '__main__':
    unittest.main()
