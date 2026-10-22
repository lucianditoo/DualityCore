import concurrent.futures
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import time
import unittest
import uuid
from unittest.mock import patch

from lab.ignition.backend import ARTIFACT, Backend, CAPTURE_LIMIT, Laboratory, LedgerError, PROFILE


class IgnitionTests(unittest.TestCase):
    def setUp(self):
        self.lab = Laboratory()
        self.addCleanup(self.lab.close)
        self.backend = self.lab.backend
        self.q = dict(request_id=str(uuid.uuid4()), task_id=str(uuid.uuid4()), attempt_id=str(uuid.uuid4()), generation=1)
        self.backend.advance(self.q['task_id'], self.q['attempt_id'], 1)

    def start(self, **changes):
        return self.backend.start(**dict(self.q, **changes))

    def wait_state(self, state):
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            value = self.backend.observe(self.q['request_id'])
            if value['state'] == state:
                return value
            time.sleep(0.005)
        self.fail('state did not appear: ' + state)

    def test_real_process_artifact_hash_duration_and_bounded_streams(self):
        initial = self.start()
        launched = subprocess.Popen
        with patch('lab.ignition.backend.subprocess.Popen', wraps=launched) as spawn:
            done = self.backend.execute(self.q['request_id'])
        self.assertEqual(spawn.call_count, 1)
        args, options = spawn.call_args
        self.assertEqual(args[0][1:3], ['-I', '-B'])
        self.assertEqual(Path(args[0][-1]).name, 'echo_program.py')
        self.assertFalse(options['shell'])
        self.assertEqual(options['env'], {'LANG': 'C.UTF-8', 'LC_ALL': 'C.UTF-8'})
        self.assertEqual(Path(options['cwd']).parent, self.lab.root)
        self.assertEqual(done['state'], 'SUCCEEDED')
        result = done['result']
        self.assertEqual(initial['execution_id'], result['execution_id'])
        self.assertTrue(result['accepted_as_current'])
        self.assertTrue(result['exit_confirmed'])
        self.assertEqual(result['returncode'], 0)
        artifact = self.lab.root / result['workspace_name'] / result['artifact_name']
        self.assertEqual(artifact.read_bytes(), ARTIFACT)
        self.assertEqual(result['artifact_sha256'], hashlib.sha256(artifact.read_bytes()).hexdigest())
        self.assertGreaterEqual(result['duration_seconds'], 0.4)
        for stream in ('stdout', 'stderr'):
            self.assertLessEqual(len(result[stream].encode()), CAPTURE_LIMIT)
            self.assertTrue(result[stream + '_truncated'])
            self.assertGreaterEqual(result[stream + '_bytes'], 131072)

    def test_real_nonzero_exit_does_not_publish_success(self):
        self.start()
        with concurrent.futures.ThreadPoolExecutor() as pool:
            future = pool.submit(self.backend.execute, self.q['request_id'])
            self.wait_state('RUNNING')
            # Deliberately obstruct only the scratch artifact of our own child.
            workspace = next(self.lab.root.glob('run-*'))
            (workspace / 'artifact.txt').mkdir()
            done = future.result(timeout=5)
        self.assertEqual(done['state'], 'FAILED')
        self.assertTrue(done['result']['exit_confirmed'])
        self.assertNotEqual(done['result']['returncode'], 0)
        self.assertIsNone(done['result']['artifact_sha256'])
        self.assertFalse(done['result']['accepted_as_current'])

    def test_real_timeout_waits_for_exit(self):
        self.start(timeout_ms=30)
        done = self.backend.execute(self.q['request_id'])
        self.assertEqual(done['state'], 'TIMED_OUT')
        self.assertTrue(done['result']['exit_confirmed'])
        self.assertNotEqual(done['result']['returncode'], 0)
        self.assertFalse(done['result']['accepted_as_current'])
        self.assertIsNone(done['result']['artifact_sha256'])
        self.assertLess(done['result']['duration_seconds'], 3)

    def test_cancel_before_execute_never_spawns(self):
        self.start()
        self.assertEqual(self.backend.cancel(self.q['request_id'])['state'], 'CANCELLED')
        with patch('lab.ignition.backend.subprocess.Popen', side_effect=AssertionError('no spawn')):
            self.assertEqual(self.backend.execute(self.q['request_id'])['state'], 'CANCELLED')

    def test_real_cancel_from_reopened_backend_waits_for_owned_child(self):
        self.start()
        other = Backend(self.lab.root)
        self.addCleanup(other.close)
        with concurrent.futures.ThreadPoolExecutor() as pool:
            future = pool.submit(self.backend.execute, self.q['request_id'])
            self.wait_state('RUNNING')
            self.assertEqual(other.cancel(self.q['request_id'])['state'], 'CANCELLING')
            done = future.result(timeout=5)
        self.assertEqual(done['state'], 'CANCELLED')
        self.assertTrue(done['result']['exit_confirmed'])
        self.assertFalse(done['result']['accepted_as_current'])
        self.assertIsNone(done['result']['artifact_sha256'])

    def test_terminal_cancel_preserves_result(self):
        self.start()
        done = self.backend.execute(self.q['request_id'])
        self.assertEqual(self.backend.cancel(self.q['request_id']), done)
        self.assertEqual(self.backend.recover(self.q['request_id']), done)

    def test_reopen_idempotency_without_second_launch(self):
        self.start()
        done = self.backend.execute(self.q['request_id'])
        reopened = Backend(self.lab.root)
        self.addCleanup(reopened.close)
        with patch('lab.ignition.backend.subprocess.Popen', side_effect=AssertionError('no relaunch')):
            self.assertEqual(reopened.start(**self.q), done)
            self.assertEqual(reopened.execute(self.q['request_id']), done)
        with self.assertRaisesRegex(ValueError, 'IDEMPOTENCY_CONFLICT'):
            reopened.start(**dict(self.q, timeout_ms=1000))
        with self.assertRaisesRegex(ValueError, 'IDEMPOTENCY_CONFLICT'):
            reopened.start(**dict(self.q, request_id=str(uuid.uuid4())))

    def test_simultaneous_execute_claims_once(self):
        self.start()
        other = Backend(self.lab.root)
        self.addCleanup(other.close)
        real_spawn = subprocess.Popen
        with patch('lab.ignition.backend.subprocess.Popen', wraps=real_spawn) as spy:
            with concurrent.futures.ThreadPoolExecutor() as pool:
                futures = [pool.submit(backend.execute, self.q['request_id']) for backend in (self.backend, other)]
                replies = [f.result(timeout=5) for f in futures]
        self.assertEqual(spy.call_count, 1)
        self.assertEqual(len({r['execution_id'] for r in replies}), 1)
        self.assertEqual(self.backend.observe(self.q['request_id'])['state'], 'SUCCEEDED')

    def test_recovery_of_all_incomplete_phases_never_spawns_or_signals(self):
        initial = self.start()
        for phase in ('INTENT', 'SPAWNING', 'RUNNING', 'CANCELLING'):
            with self.subTest(phase=phase):
                with sqlite3.connect(self.backend.ledger) as db:
                    db.execute('UPDATE attempts SET state=? WHERE request=?', (phase, self.q['request_id']))
                other = Backend(self.lab.root)
                try:
                    with patch('lab.ignition.backend.subprocess.Popen', side_effect=AssertionError('no launch')), patch('os.kill', side_effect=AssertionError('no signal')):
                        unknown = other.recover(self.q['request_id'])
                        self.assertEqual(unknown['state'], 'UNKNOWN')
                        self.assertEqual(unknown['execution_id'], initial['execution_id'])
                        self.assertEqual(other.execute(self.q['request_id']), unknown)
                        self.assertEqual(other.cancel(self.q['request_id']), unknown)
                finally:
                    other.close()

    def test_recovery_during_live_execution_blocks_publication(self):
        self.start()
        other = Backend(self.lab.root)
        self.addCleanup(other.close)
        with concurrent.futures.ThreadPoolExecutor() as pool:
            future = pool.submit(self.backend.execute, self.q['request_id'])
            self.wait_state('RUNNING')
            self.assertEqual(other.recover(self.q['request_id'])['state'], 'UNKNOWN')
            done = future.result(timeout=5)
        self.assertEqual(done['state'], 'UNKNOWN')
        self.assertFalse(done['result']['accepted_as_current'])
        self.assertTrue(done['result']['exit_confirmed'])

    def test_generation_advance_blocks_old_result_and_cancel(self):
        self.start()
        with concurrent.futures.ThreadPoolExecutor() as pool:
            future = pool.submit(self.backend.execute, self.q['request_id'])
            self.wait_state('RUNNING')
            attempt = str(uuid.uuid4())
            self.backend.advance(self.q['task_id'], attempt, 2)
            with self.assertRaisesRegex(ValueError, 'STALE_GENERATION'):
                self.backend.cancel(self.q['request_id'])
            next_q = dict(self.q, request_id=str(uuid.uuid4()), attempt_id=attempt, generation=2)
            new = self.backend.start(**next_q)
            done = future.result(timeout=5)
        self.assertEqual(done['state'], 'UNKNOWN')
        self.assertFalse(done['result']['accepted_as_current'])
        self.assertEqual(self.backend.observe(next_q['request_id']), new)
        current = self.backend.execute(next_q['request_id'])
        self.assertEqual(current['state'], 'SUCCEEDED')
        self.assertNotEqual(done['execution_id'], current['execution_id'])
        self.assertEqual(self.backend.execute(self.q['request_id']), done)
        self.assertEqual(self.backend.observe(next_q['request_id']), current)

    def test_stale_unstarted_execution_is_not_launched(self):
        self.start()
        self.backend.advance(self.q['task_id'], str(uuid.uuid4()), 2)
        with patch('lab.ignition.backend.subprocess.Popen', side_effect=AssertionError('no launch')):
            self.assertEqual(self.backend.execute(self.q['request_id'])['state'], 'UNKNOWN')

    def test_validation_and_no_arbitrary_payload_or_environment(self):
        for changes in ({'profile': 'arbitrary'}, {'timeout_ms': True}, {'timeout_ms': 0}, {'timeout_ms': 5001}, {'generation': True}, {'request_id': 'x'}, {'task_id': '../path'}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                self.start(**changes)
        for changes in ({'command': 'id'}, {'env': {'X': 'Y'}}, {'cwd': '/tmp'}, {'args': []}, {'input_ref': 'external'}):
            with self.subTest(changes=changes), self.assertRaises(TypeError):
                self.start(**changes)
        with self.assertRaisesRegex(ValueError, 'STALE_GENERATION'):
            self.start(generation=2)
        with self.assertRaises(PermissionError), patch('os.geteuid', return_value=0):
            Backend(self.lab.root)

    def test_missing_corrupt_ledger_fails_without_reinitializing(self):
        self.start()
        with sqlite3.connect(self.backend.ledger) as db:
            db.execute('DELETE FROM meta')
        with self.assertRaises(LedgerError):
            Backend(self.lab.root)
        with self.assertRaises(LedgerError):
            self.backend.execute(self.q['request_id'])
        self.backend.ledger.unlink()
        with self.assertRaises(sqlite3.OperationalError):
            Backend(self.lab.root)
        self.assertFalse(self.backend.ledger.exists())

    def test_spawn_failure_is_durable_unknown(self):
        self.start()
        with patch('lab.ignition.backend.subprocess.Popen', side_effect=OSError('injected launch failure')):
            done = self.backend.execute(self.q['request_id'])
        self.assertEqual(done['state'], 'UNKNOWN')
        self.assertFalse(done['result']['exit_confirmed'])
        self.assertIsNone(done['result']['returncode'])
        self.assertIsNone(done['result']['artifact_sha256'])
        self.assertEqual(self.backend.execute(self.q['request_id']), done)

    def test_close_while_child_is_owned_is_rejected(self):
        self.start()
        with concurrent.futures.ThreadPoolExecutor() as pool:
            future = pool.submit(self.backend.execute, self.q['request_id'])
            self.wait_state('RUNNING')
            with self.assertRaises(RuntimeError):
                self.backend.close()
            self.backend.cancel(self.q['request_id'])
            future.result(timeout=5)

    def crash_worker(self, point, code):
        worker = Path(__file__).with_name('crash_worker.py')
        result = subprocess.run([sys.executable, '-B', str(worker), str(self.lab.root), self.q['request_id'], point],
                                cwd=self.lab.root, env={'LANG': 'C.UTF-8', 'LC_ALL': 'C.UTF-8'},
                                stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
                                timeout=5, check=False)
        self.assertEqual(result.returncode, code, result.stderr.decode())

    def test_real_owner_crash_before_result_commit_recovers_unknown(self):
        initial = self.start()
        self.crash_worker('before', 41)
        other = Backend(self.lab.root)
        self.addCleanup(other.close)
        # Artifact exists and is correct, but there is no committed result evidence.
        artifacts = list(self.lab.root.glob('run-*/artifact.txt'))
        self.assertEqual(len(artifacts), 1)
        self.assertEqual(artifacts[0].read_bytes(), ARTIFACT)
        self.assertEqual(other.observe(self.q['request_id'])['state'], 'RUNNING')
        unknown = other.recover(self.q['request_id'])
        self.assertEqual(unknown['state'], 'UNKNOWN')
        self.assertIsNone(unknown['result'])
        self.assertEqual(unknown['execution_id'], initial['execution_id'])
        with patch('lab.ignition.backend.subprocess.Popen', side_effect=AssertionError('no relaunch')):
            self.assertEqual(other.execute(self.q['request_id']), unknown)

    def test_real_owner_crash_after_result_commit_replays_success(self):
        self.start()
        self.crash_worker('after', 42)
        other = Backend(self.lab.root)
        self.addCleanup(other.close)
        done = other.observe(self.q['request_id'])
        self.assertEqual(done['state'], 'SUCCEEDED')
        self.assertTrue(done['result']['exit_confirmed'])
        with patch('lab.ignition.backend.subprocess.Popen', side_effect=AssertionError('no relaunch')):
            self.assertEqual(other.start(**self.q), done)
            self.assertEqual(other.execute(self.q['request_id']), done)

    def test_corrupt_result_and_fingerprint_are_not_replayed(self):
        self.start()
        done = self.backend.execute(self.q['request_id'])
        result = dict(done['result'], generation=2)
        with sqlite3.connect(self.backend.ledger) as db:
            db.execute('UPDATE attempts SET result=?', (json.dumps(result),))
        with self.assertRaises(LedgerError):
            self.backend.observe(self.q['request_id'])
        with sqlite3.connect(self.backend.ledger) as db:
            db.execute('UPDATE attempts SET result=?,fingerprint=?', (json.dumps(done['result']), 'corrupt'))
        with self.assertRaises(LedgerError):
            Backend(self.lab.root)


if __name__ == '__main__':
    unittest.main()
