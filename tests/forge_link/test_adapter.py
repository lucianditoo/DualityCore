import concurrent.futures
import hashlib
import itertools
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import threading
import time
import unittest
import uuid
from unittest.mock import patch

from lab.forge_link.adapter import Adapter, ForgeLab, fixed_start
from lab.handshake.ledger import ExecutionLedger, TUPLE, LedgerCorrupt
from lab.ignition.process_runner import ARTIFACT


class AdapterTests(unittest.TestCase):
    def setUp(self):
        self.lab = ForgeLab()
        self.addCleanup(lambda: self.lab.__exit__(None, None, None))
        self.adapter = self.lab.adapter
        self.q = fixed_start(self.lab.deployment)
        self.adapter.advance(self.q['task_id'], self.q['attempt_id'], 1)
        self.adapter.approve(self.q['authorization_ref'], self.q['task_id'], self.q['attempt_id'], 1)

    def send(self, q, adapter=None):
        return (adapter or self.adapter).handle(json.dumps(q))

    def pending(self):
        # Trusted test staging: commit the start intent, don't launch yet.
        with self.adapter.backend._db() as ledger:
            return ledger.handle(json.dumps(self.q))

    def command(self, a, op='observe', approval=None):
        q = dict(version=self.q['version'], op=op, request_id=str(uuid.uuid4()),
                 authorization_ref=approval or self.q['authorization_ref'], **{k: a[k] for k in TUPLE})
        if op == 'cancel':
            q['reason_code'] = 'OPERATOR_REQUEST'
        return q

    def wait_running(self):
        deadline = time.monotonic() + 4
        while time.monotonic() < deadline:
            try:
                facts = self.adapter.backend.facts(self.q['request_id'])
                if facts['phase'] == 'RUNNING':
                    with self.adapter.backend._db() as ledger:
                        return ledger.snapshot()['assignments'][facts['workspace_name']]
            except KeyError:
                pass
            time.sleep(0.005)
        self.fail('no RUNNING evidence')

    def test_real_contract_start_observe_result_and_artifact(self):
        accepted = self.send(self.q)
        observed = self.send(self.command(accepted))
        self.assertEqual(accepted['task_state'], 'ACCEPTED')
        self.assertEqual(observed['task_state'], 'SUCCEEDED')
        self.assertEqual(observed['resource_state'], 'CLOSED_CONFIRMED')
        self.assertEqual(set(observed), set(accepted))
        self.assertEqual(observed['version'], 'dmaol-executor/1')
        self.assertEqual(observed['source'], 'adapter-ledger')
        result = self.adapter.published_result(accepted['session_id'])
        self.assertEqual(set(result), TUPLE | {'result_id', 'outcome', 'artifact_ref', 'artifact_sha256'})
        content, sha = self.adapter.artifact(result['artifact_ref'])
        self.assertEqual(content, ARTIFACT)
        self.assertEqual(hashlib.sha256(content).hexdigest(), sha)
        self.assertEqual(sha, result['artifact_sha256'])
        # Mutating the workspace cannot substitute the published byte snapshot.
        (self.lab.root / accepted['session_id'] / 'artifact.txt').write_bytes(b'modified after verification')
        self.assertEqual(self.adapter.artifact(result['artifact_ref']), (content, sha))

    def test_simultaneous_duplicate_and_restart_have_one_process(self):
        real = subprocess.Popen
        other = Adapter(self.lab.root, self.lab.deployment)
        self.addCleanup(other.close)
        with patch('subprocess.Popen', wraps=real) as spawn:
            with concurrent.futures.ThreadPoolExecutor() as pool:
                futures = [pool.submit(self.send, self.q, a) for a in (self.adapter, other)]
                replies = [f.result(timeout=6) for f in futures]
            self.assertEqual(spawn.call_count, 1)
        self.assertEqual(replies[0], replies[1])
        with patch('subprocess.Popen', side_effect=AssertionError('duplicate launch')):
            self.assertEqual(self.send(self.q, other), replies[0])
        lookup = dict(version=self.q['version'], op='observe', request_id=str(uuid.uuid4()),
                      authorization_ref=self.q['authorization_ref'], lookup_request_id=self.q['request_id'])
        self.assertEqual(self.send(lookup, other)['task_state'], 'SUCCEEDED')
        self.assertEqual(len(list(self.lab.root.glob('s-*'))), 1)

    def test_invalid_profile_input_commands_and_scope_have_no_effect(self):
        bad = [dict(profile_ref='isolated-task-v1'), dict(input_ref='external'), dict(input_sha256='a' * 64),
               dict(execution_timeout_ms=5001), dict(authorization_ref='missing'), dict(deployment_id='f' * 32)]
        with patch('subprocess.Popen', side_effect=AssertionError('invalid request launched')):
            for change in bad:
                q = dict(self.q, request_id=str(uuid.uuid4()), **change)
                self.assertIn(self.send(q)['error']['code'], ('UNAUTHORIZED', 'INVALID_REQUEST'))
            for extra in ({'command': 'id'}, {'cwd': '/tmp'}, {'env': {}}, {'args': []}, {'code': 'print(1)'}):
                self.assertEqual(self.send(dict(self.q, **extra))['error']['code'], 'INVALID_REQUEST')
            self.assertEqual(self.adapter.handle('{"op":"start","op":"cancel"}')['error']['code'], 'INVALID_REQUEST')
        with self.adapter.backend._db() as ledger:
            self.assertFalse(ledger.snapshot()['assignments'])

    def test_conflicts_and_wrong_session_cannot_cancel(self):
        a = self.send(self.q)
        self.assertEqual(self.send(dict(self.q, execution_timeout_ms=1))['error']['code'], 'IDEMPOTENCY_CONFLICT')
        self.assertEqual(self.send(dict(self.q, request_id=str(uuid.uuid4())))['error']['code'], 'IDEMPOTENCY_CONFLICT')
        q = self.command(a, 'cancel')
        q['session_id'] = 's-' + '0' * 32
        self.assertEqual(self.send(q)['error']['code'], 'RESOURCE_IDENTITY_UNVERIFIABLE')
        self.assertEqual(self.send(self.command(a))['task_state'], 'SUCCEEDED')

    def test_recovery_during_live_execution_is_read_only_for_process(self):
        other = Adapter(self.lab.root, self.lab.deployment)
        self.addCleanup(other.close)
        with patch('subprocess.Popen.terminate', side_effect=AssertionError('recover terminated')), patch('subprocess.Popen.kill', side_effect=AssertionError('SIGKILL')):
            with concurrent.futures.ThreadPoolExecutor() as pool:
                future = pool.submit(self.send, self.q)
                a = self.wait_running()
                unknown = self.send(self.command(a, 'recover'), other)
                self.assertEqual(unknown['error']['code'], 'OUTCOME_UNKNOWN')
                self.assertEqual(unknown['resource_state'], 'UNKNOWN')
                self.assertIsNone(other.backend.facts(self.q['request_id'])['evidence'])
                future.result(timeout=5)
        self.assertEqual(self.send(self.command(a), other)['task_state'], 'SUCCEEDED')

    def test_timeout_does_not_signal_or_publish_success(self):
        self.q['execution_timeout_ms'] = 40
        with patch('subprocess.Popen.terminate', side_effect=AssertionError('timeout signalled')), patch('subprocess.Popen.kill', side_effect=AssertionError('SIGKILL')):
            with concurrent.futures.ThreadPoolExecutor() as pool:
                future = pool.submit(self.send, self.q)
                a = self.wait_running()
                deadline = time.monotonic() + 2
                while not self.adapter.backend.facts(self.q['request_id'])['timed_out']:
                    self.assertLess(time.monotonic(), deadline)
                    time.sleep(0.005)
                self.assertIsNone(self.adapter.backend.facts(self.q['request_id'])['evidence'])
                self.assertFalse(future.done())
                future.result(timeout=5)
        observed = self.send(self.command(a))
        self.assertEqual(observed['task_state'], 'UNKNOWN')
        self.assertEqual(observed['resource_state'], 'CLOSED_CONFIRMED')
        self.assertIsNone(self.adapter.published_result(a['session_id']))

    def test_explicit_cancel_waits_for_exit_and_is_idempotent(self):
        with concurrent.futures.ThreadPoolExecutor() as pool:
            future = pool.submit(self.send, self.q)
            a = self.wait_running()
            q = self.command(a, 'cancel')
            requested = self.send(q)
            self.assertEqual(requested['task_state'], 'CANCEL_REQUESTED')
            self.assertEqual(self.send(q), requested)
            future.result(timeout=5)
        observed = self.send(self.command(a))
        self.assertEqual(observed['task_state'], 'CANCELLED')
        self.assertEqual(observed['resource_state'], 'CLOSED_CONFIRMED')
        self.assertTrue(self.adapter.backend.facts(self.q['request_id'])['evidence']['exit_confirmed'])

    def test_reassignment_never_signals_old_child_or_publishes_late_result(self):
        with patch('subprocess.Popen.terminate', side_effect=AssertionError('revocation signalled')):
            with concurrent.futures.ThreadPoolExecutor() as pool:
                future = pool.submit(self.send, self.q)
                a = self.wait_running()
                attempt = str(uuid.uuid4())
                self.adapter.advance(a['task_id'], attempt, 2)
                future.result(timeout=5)
        self.assertIsNone(self.adapter.published_result(a['session_id']))
        with self.adapter.backend._db() as ledger:
            state = ledger.snapshot()
            self.assertEqual(len(state['late_results']), 1)
            self.assertEqual(ledger.db.execute('SELECT count(*) FROM artifacts').fetchone()[0], 0)
        newer = dict(self.q, request_id=str(uuid.uuid4()), attempt_id=attempt, generation=2, authorization_ref='next')
        self.adapter.approve('next', newer['task_id'], attempt, 2)
        b = self.send(newer)
        terminal = self.send(self.command(b, approval='next'))
        self.assertEqual(terminal['task_state'], 'SUCCEEDED')
        self.assertEqual(self.send(self.command(a, 'cancel'))['error']['code'], 'STALE_GENERATION')
        self.send(self.q)
        self.assertEqual(self.send(self.command(b, approval='next'))['state_version'], terminal['state_version'])

    def test_all_orders_of_cancel_revoke_and_real_publication(self):
        for order in itertools.permutations(('cancel', 'revoke', 'publish')):
            with self.subTest(order=order), ForgeLab() as lab:
                adapter, q = lab.adapter, fixed_start(lab.deployment)
                adapter.advance(q['task_id'], q['attempt_id'], 1)
                adapter.approve(q['authorization_ref'], q['task_id'], q['attempt_id'], 1)
                ready, release = threading.Event(), threading.Event()
                original = ExecutionLedger.finish
                def blocked(ledger, session, evidence):
                    ready.set()
                    if not release.wait(5):
                        raise TimeoutError('test publication barrier')
                    return original(ledger, session, evidence)
                with patch.object(ExecutionLedger, 'finish', blocked), concurrent.futures.ThreadPoolExecutor() as pool:
                    future = pool.submit(adapter.handle, json.dumps(q))
                    try:
                        self.assertTrue(ready.wait(4))  # Child has really exited.
                        with adapter.backend._db() as ledger:
                            a = next(iter(ledger.snapshot()['assignments'].values()))
                        for action in order:
                            if action == 'publish':
                                release.set()
                                future.result(timeout=5)
                            elif action == 'revoke':
                                adapter.advance(q['task_id'], str(uuid.uuid4()), 2)
                            else:
                                adapter.handle(json.dumps(self.command(a, 'cancel')))
                    finally:
                        release.set()
                        future.result(timeout=5)
                with adapter.backend._db() as ledger:
                    state = ledger.snapshot()
                    accepted = [r for r in state['results'].values() if r['status'] == 'ACCEPTED']
                    expected = order[0] == 'publish'
                    self.assertEqual(len(accepted), int(expected))
                    self.assertEqual(ledger.db.execute('SELECT count(*) FROM artifacts').fetchone()[0], int(expected))
                self.assertIsNone(adapter.published_result(a['session_id']))  # Revoked now in every ordering.

    def test_expired_approval_is_rechecked_before_real_launch(self):
        a = self.pending()
        self.adapter.approve(self.q['authorization_ref'], self.q['task_id'], self.q['attempt_id'], 1, expired=True)
        with patch('subprocess.Popen', side_effect=AssertionError('expired approval launched')):
            self.assertEqual(self.send(self.q), a)  # Historical ACK, no new effect.
        self.assertEqual(self.send(self.command(a))['error']['code'], 'APPROVAL_EXPIRED')
        with self.adapter.backend._db() as ledger:
            self.assertEqual(ledger.run(a['session_id'])['phase'], 'UNKNOWN')

    def test_missing_published_bytes_fails_closed(self):
        a = self.send(self.q)
        with sqlite3.connect(self.adapter.backend.ledger) as db:
            db.execute('DELETE FROM artifacts')
        with self.assertRaises(LedgerCorrupt):
            self.send(self.command(a))

    def test_independent_deployments_cannot_reopen_or_cross_cancel(self):
        a = self.send(self.q)
        with self.assertRaises(LedgerCorrupt):
            Adapter(self.lab.root, 'f' * 32)
        with ForgeLab() as other:
            response = other.adapter.handle(json.dumps(self.command(a, 'cancel')))
            self.assertEqual(response['error']['code'], 'UNAUTHORIZED')

    def worker(self, mode, expected_code):
        spec = self.lab.root / 'worker.json'
        spec.write_text(json.dumps(dict(root=str(self.lab.root), deployment=self.lab.deployment, request=self.q, mode=mode)))
        result = subprocess.run([sys.executable, '-B', str(Path(__file__).with_name('restart_worker.py')), str(spec)],
                                cwd=self.lab.root, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, timeout=8)
        self.assertEqual(result.returncode, expected_code, result.stderr.decode())

    def test_owner_crash_before_publication_conserves_uncertainty(self):
        a = self.pending()
        self.worker('before-commit', 51)
        other = Adapter(self.lab.root, self.lab.deployment)
        self.addCleanup(other.close)
        unknown = self.send(self.command(a, 'recover'), other)
        self.assertEqual(unknown['error']['code'], 'OUTCOME_UNKNOWN')
        with patch('subprocess.Popen', side_effect=AssertionError('uncertain retry launched')):
            self.assertEqual(self.send(self.q, other), a)
        self.assertIsNone(other.published_result(a['session_id']))
        self.assertEqual(len(list(self.lab.root.glob('s-*'))), 1)

    def test_owner_crash_after_publication_replays_ack_and_result(self):
        a = self.pending()
        self.worker('after-commit', 52)
        other = Adapter(self.lab.root, self.lab.deployment)
        self.addCleanup(other.close)
        result = other.published_result(a['session_id'])
        self.assertIsNotNone(result)
        with patch('subprocess.Popen', side_effect=AssertionError('duplicate launch')):
            self.assertEqual(self.send(self.q, other), a)
        self.assertEqual(self.send(self.command(a), other)['task_state'], 'SUCCEEDED')

    def test_interpreter_restart_while_child_alive_never_adopts_or_relaunches(self):
        a = self.pending()
        self.worker('exec-during-run', 0)
        self.assertEqual(self.send(self.command(a))['task_state'], 'UNKNOWN')
        self.assertIsNone(self.adapter.published_result(a['session_id']))
        with patch('subprocess.Popen', side_effect=AssertionError('restart launched again')):
            self.assertEqual(self.send(self.q), a)
        self.assertEqual(len(list(self.lab.root.glob('s-*'))), 1)


if __name__ == '__main__':
    unittest.main()
