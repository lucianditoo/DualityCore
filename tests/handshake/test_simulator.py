import copy
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
import uuid
from unittest.mock import patch

from lab.handshake.simulator import Simulator, InvalidRequest, TUPLE, decode

FIXTURES = Path(__file__).resolve().parents[2] / 'lab/handshake/fixtures'


class SimulatorTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / 'ledger.sqlite'
        self.q = json.loads((FIXTURES / 'start.json').read_text())
        self.sim = Simulator(self.path, self.q['deployment_id'], simulated_channel=True)
        self.addCleanup(lambda: self.sim.close())
        self.sim.advance(self.q['task_id'], self.q['attempt_id'], 7)
        self.sim.approve(self.q['authorization_ref'], self.q['task_id'], self.q['attempt_id'], 7)

    def send(self, q):
        return self.sim.handle(json.dumps(q))

    def command(self, a, op='observe'):
        q = {k: a[k] for k in TUPLE}
        q.update(version=self.q['version'], op=op, request_id=str(uuid.uuid4()), authorization_ref=self.q['authorization_ref'])
        if op == 'cancel':
            q['reason_code'] = 'OPERATOR_REQUEST'
        return q

    def lookup(self):
        return dict(version=self.q['version'], op='observe', request_id=str(uuid.uuid4()), authorization_ref=self.q['authorization_ref'], lookup_request_id=self.q['request_id'])

    def result(self, a):
        return dict(**{k: a[k] for k in TUPLE}, result_id=str(uuid.uuid4()), outcome='SUCCEEDED', artifact_ref='fixture-output', artifact_sha256='c' * 64)

    def evidence(self, a, task='DISPATCHING', resource='CREATING', certainty='CONFIRMED'):
        current = self.sim.snapshot()['assignments'][a['session_id']]
        self.sim.simulate_evidence(a['session_id'], task_state=task, resource_state=resource, certainty=certainty, state_version=current['state_version'])

    def test_default_dependency_block_and_restart(self):
        self.sim.channel = False
        r = self.send(self.q)
        self.assertEqual(r['error']['code'], 'DEPENDENCY_UNVERIFIED')
        self.assertEqual(self.sim.snapshot()['assignments'], {})
        self.sim.close()
        self.sim = Simulator(self.path, self.q['deployment_id'], simulated_channel=True)
        self.assertEqual(self.send(self.q), r)
        self.assertEqual(self.sim.snapshot()['assignments'], {})

    def test_lost_ack_restart_and_new_request_no_duplicate(self):
        r = self.send(self.q)  # Deliberately discard ACK at client boundary.
        self.sim.close()
        self.sim = Simulator(self.path, self.q['deployment_id'], simulated_channel=True)
        self.assertEqual(self.send(self.q), r)
        found = self.send(self.lookup())
        self.assertEqual(found['session_id'], r['session_id'])
        q = dict(self.q, request_id=str(uuid.uuid4()))
        self.assertEqual(self.send(q)['error']['code'], 'IDEMPOTENCY_CONFLICT')
        self.assertEqual(len(self.sim.snapshot()['assignments']), 1)

    def test_request_conflict_after_restart(self):
        self.send(self.q)
        self.assertEqual(self.send(dict(self.q, execution_timeout_ms=1))['error']['code'], 'IDEMPOTENCY_CONFLICT')

    def test_strict_json_and_schema(self):
        for raw in ('{}', '[]', 'null', '{"op":1,"op":2}', '{"x":NaN}', b'\xff', '{}'.encode('utf-16'), 'x' * 16385):
            with self.subTest(raw=str(raw)[:30]):
                self.assertEqual(self.sim.handle(raw)['error']['code'], 'INVALID_REQUEST')
        mutations = {'generation': [True, 0, -1, 9007199254740992, 7.0, '7'],
                     'task_id': [None, '0' * 32, str(uuid.UUID(int=0))],
                     'attempt_id': ['bad'], 'deployment_id': ['A' * 32],
                     'session_id': ['s-' + 'c' * 32], 'request_id': ['bad'],
                     'execution_timeout_ms': [False, 1.0], 'input_sha256': ['x' * 64],
                     'input_ref': ['/etc/passwd'], 'profile_ref': ['$(id)']}
        for key, values in mutations.items():
            for value in values:
                with self.subTest(key=key, value=value):
                    self.assertEqual(self.send(dict(self.q, **{key: value}))['error']['code'], 'INVALID_REQUEST')
        for field in self.q:
            q = dict(self.q)
            del q[field]
            self.assertEqual(self.send(q)['error']['code'], 'INVALID_REQUEST')
        for field in ('state_version', 'task_state', 'resource_state', 'certainty', 'command'):
            self.assertEqual(self.send(dict(self.q, **{field: 1}))['error']['code'], 'INVALID_REQUEST')
        self.assertEqual(self.sim.snapshot()['assignments'], {})

    def test_authorization_scope_expiry_and_profile(self):
        self.assertEqual(self.send(dict(self.q, authorization_ref='missing'))['error']['code'], 'UNAUTHORIZED')
        self.sim.approve('expired', self.q['task_id'], self.q['attempt_id'], 7, expired=True)
        self.assertEqual(self.send(dict(self.q, request_id=str(uuid.uuid4()), authorization_ref='expired'))['error']['code'], 'APPROVAL_EXPIRED')
        for change in ({'execution_timeout_ms': 60001}, {'profile_ref': 'other'}, {'deployment_id': 'd' * 32}):
            self.assertEqual(self.send(dict(self.q, request_id=str(uuid.uuid4()), **change))['error']['code'], 'UNAUTHORIZED')

    def test_stale_generation_not_largest_received(self):
        self.assertEqual(self.send(dict(self.q, generation=8))['error']['code'], 'UNAUTHORIZED')
        self.sim.approve('future', self.q['task_id'], self.q['attempt_id'], 8)
        q = dict(self.q, request_id=str(uuid.uuid4()), generation=8, authorization_ref='future')
        self.assertEqual(self.send(q)['error']['code'], 'STALE_GENERATION')
        self.assertFalse(self.sim.snapshot()['assignments'])

    def test_wrong_session_identity_and_delayed_cancel(self):
        a = self.send(self.q)
        q = self.command(a, 'cancel')
        q['session_id'] = 's-' + '0' * 32
        self.assertEqual(self.send(q)['error']['code'], 'RESOURCE_IDENTITY_UNVERIFIABLE')
        self.sim.advance(a['task_id'], str(uuid.uuid4()), 8)
        self.assertEqual(self.send(self.command(a, 'cancel'))['error']['code'], 'STALE_GENERATION')
        self.assertEqual(self.sim.snapshot()['assignments'][a['session_id']]['task_state'], 'ACCEPTED')

    def test_observe_no_mutation_recover_uncertain_idempotent(self):
        a = self.send(self.q)
        before = copy.deepcopy(self.sim.snapshot()['assignments'])
        self.send(self.command(a))
        self.assertEqual(before, self.sim.snapshot()['assignments'])
        q = self.command(a, 'recover')
        r = self.send(q)
        self.assertEqual((r['task_state'], r['resource_state'], r['certainty']), ('UNKNOWN', 'UNKNOWN', 'UNKNOWN'))
        self.assertEqual(self.send(q), r)
        self.assertEqual(self.send(self.command(a, 'cancel'))['error']['code'], 'OUTCOME_UNKNOWN')
        self.assertEqual(len(self.sim.snapshot()['assignments']), 1)

    def test_cancel_requires_confirmation(self):
        a = self.send(self.q)
        r = self.send(self.command(a, 'cancel'))
        self.assertEqual(r['task_state'], 'CANCEL_REQUESTED')
        self.assertEqual(r['resource_state'], 'NOT_CREATED')
        with self.assertRaises(ValueError):
            self.evidence(a, 'CANCELLED', 'STOPPING')
        self.evidence(a, 'CANCELLED', 'CLOSED_CONFIRMED')
        self.assertEqual(self.send(self.command(a, 'cancel'))['error']['code'], 'ALREADY_TERMINAL')

    def test_state_versions_and_states(self):
        a = self.send(self.q)
        self.evidence(a)
        for change in ({'state_version': 1}, {'state_version': True}, {'task_state': 'BOGUS'}, {'resource_state': 'LIVE'}, {'certainty': 'YES'}, {'resource_state': 'UNKNOWN'}):
            args = dict(task_state='DISPATCHING', resource_state='CREATING', certainty='CONFIRMED', state_version=2)
            args.update(change)
            with self.subTest(change=change), self.assertRaises(ValueError):
                self.sim.simulate_evidence(a['session_id'], **args)
        self.evidence(a, 'RUNNING', 'ACTIVE_OBSERVED', 'OBSERVED')
        self.assertEqual(self.send(self.command(a))['state_version'], 3)

    def test_results_immutable_and_terminal_cancel(self):
        a = self.send(self.q)
        self.evidence(a)
        self.evidence(a, 'RUNNING', 'ACTIVE_OBSERVED', 'OBSERVED')
        r = self.result(a)
        self.assertEqual(self.sim.simulate_result(json.dumps(r)), 'ACCEPTED')
        self.assertEqual(self.sim.simulate_result(json.dumps(r)), 'ACCEPTED')
        self.assertEqual(self.sim.simulate_result(json.dumps(dict(r, outcome='FAILED'))), 'IDEMPOTENCY_CONFLICT')
        self.assertEqual(self.send(self.command(a, 'cancel'))['error']['code'], 'ALREADY_TERMINAL')
        observed = self.send(self.command(a, 'recover'))
        self.assertEqual(observed['task_state'], 'SUCCEEDED')
        self.assertEqual(observed['resource_state'], 'UNKNOWN')
        with self.assertRaises(ValueError):
            self.evidence(a, 'UNKNOWN', 'UNKNOWN', 'UNKNOWN')

    def test_result_after_cancel_order_unknown(self):
        a = self.send(self.q)
        self.send(self.command(a, 'cancel'))
        self.assertEqual(self.sim.simulate_result(json.dumps(self.result(a))), 'OUTCOME_UNKNOWN')
        self.assertEqual(self.send(self.command(a))['task_state'], 'UNKNOWN')

    def test_old_result_segregated_after_reassignment(self):
        a = self.send(self.q)
        attempt = str(uuid.uuid4())
        self.sim.advance(a['task_id'], attempt, 8)
        self.sim.approve('new', a['task_id'], attempt, 8)
        b = self.send(dict(self.q, request_id=str(uuid.uuid4()), attempt_id=attempt, generation=8, authorization_ref='new'))
        self.assertNotEqual(a['session_id'], b['session_id'])
        self.assertEqual(self.sim.simulate_result(json.dumps(self.result(a))), 'STALE_GENERATION')
        state = self.sim.snapshot()
        self.assertEqual(state['assignments'][b['session_id']]['task_state'], 'ACCEPTED')
        self.assertEqual(len(state['late_results']), 1)
        with self.assertRaises(ValueError):
            self.sim.advance(a['task_id'], a['attempt_id'], 9)

    def test_missing_corrupt_and_wrong_deployment_ledgers_fail_closed(self):
        self.assertEqual(self.send(self.lookup())['error']['code'], 'UNAUTHORIZED')
        with self.assertRaises(ValueError):
            Simulator(self.path, 'b' * 32)
        corrupt = Path(self.tmp.name) / 'corrupt.sqlite'
        corrupt.write_bytes(b'not a database')
        with self.assertRaises(sqlite3.DatabaseError):
            Simulator(corrupt, self.q['deployment_id'])

    def test_no_infrastructure_calls(self):
        with patch('subprocess.Popen', side_effect=AssertionError('process forbidden')), patch('socket.socket', side_effect=AssertionError('socket forbidden')), patch('os.kill', side_effect=AssertionError('signal forbidden')):
            a = self.send(self.q)
            self.send(self.command(a))
            self.send(self.command(a, 'cancel'))
            self.send(self.command(a, 'recover'))

    def test_transaction_rollback_before_ack(self):
        with self.assertRaises(RuntimeError):
            with self.sim.transaction() as state:
                state['authority'].clear()
                raise RuntimeError('simulated crash before commit')
        self.assertIn(self.q['task_id'], self.sim.snapshot()['authority'])


    def test_consumable_exchange_fixture(self):
        scenario = json.loads((FIXTURES / 'lost_ack_cancel_recover.json').read_text())
        responses = {}
        session = None
        for index, step in enumerate(scenario['steps']):
            if step.get('restart'):
                self.sim.close()
                self.sim = Simulator(self.path, self.q['deployment_id'], simulated_channel=True)
                continue
            raw = json.dumps(step['request']).replace('$session_id', session or '$session_id')
            response = self.sim.handle(raw)
            if session is None:
                session = response['session_id']
            responses[index] = response
            if 'same_as_step' in step:
                self.assertEqual(response, responses[step['same_as_step']])
            expected = json.loads(json.dumps(step.get('expect', {})).replace('$session_id', session))
            for key, value in expected.items():
                self.assertEqual(response[key], value, (index, key))
            if 'error_code' in step:
                self.assertEqual(response['error']['code'], step['error_code'])

    def test_concurrent_retries_and_competing_requests(self):
        from concurrent.futures import ThreadPoolExecutor
        import threading
        barrier = threading.Barrier(2)
        def send(q):
            sim = Simulator(self.path, self.q['deployment_id'], simulated_channel=True)
            try:
                barrier.wait(timeout=5)
                return sim.handle(json.dumps(q))
            finally:
                sim.close()
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(send, self.q) for _ in range(2)]
            results = [f.result(timeout=10) for f in futures]
        self.assertEqual(results[0], results[1])
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(send, dict(self.q, request_id=str(uuid.uuid4()))) for _ in range(2)]
            results = [f.result(timeout=10) for f in futures]
        self.assertTrue(all(r['error']['code'] == 'IDEMPOTENCY_CONFLICT' for r in results))
        self.assertEqual(len(self.sim.snapshot()['assignments']), 1)

    def test_revoked_unused_attempt_never_reused(self):
        old = self.q['attempt_id']
        self.sim.advance(self.q['task_id'], str(uuid.uuid4()), 8)
        with self.assertRaises(ValueError):
            self.sim.advance(self.q['task_id'], old, 9)

    def test_rejected_recover_preserves_assignment(self):
        a = self.send(self.q)
        before = self.sim.snapshot()['assignments']
        q = self.command(a, 'recover')
        q['authorization_ref'] = 'missing'
        self.assertEqual(self.send(q)['error']['code'], 'UNAUTHORIZED')
        self.assertEqual(before, self.sim.snapshot()['assignments'])

    def test_operation_specific_schema_and_result_identity(self):
        a = self.send(self.q)
        for op in ('observe', 'recover', 'cancel'):
            q = self.command(a, op)
            self.assertEqual(self.send(dict(q, input_ref='unexpected'))['error']['code'], 'INVALID_REQUEST')
            self.assertEqual(self.send(dict(q, session_id=None))['error']['code'], 'INVALID_REQUEST')
        q = self.lookup()
        self.assertEqual(self.send(dict(q, session_id=a['session_id']))['error']['code'], 'INVALID_REQUEST')
        r = self.result(a)
        r['session_id'] = 's-' + 'f' * 32
        self.assertEqual(self.sim.simulate_result(json.dumps(r)), 'RESOURCE_IDENTITY_UNVERIFIABLE')
        with self.assertRaises(InvalidRequest):
            self.sim.simulate_result(json.dumps(dict(r, generation=True)))


if __name__ == '__main__':
    unittest.main()
