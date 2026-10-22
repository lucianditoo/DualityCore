"""CROSSWIRE: file-only process races and conservative local-ledger failure tests."""
import copy
import itertools
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
import uuid

from lab.handshake.simulator import LedgerCorrupt, Simulator, TUPLE, canonical

HERE = Path(__file__).resolve().parent
FIXTURE = HERE.parents[1] / 'lab/handshake/fixtures/start.json'


class HardeningTests(unittest.TestCase):
    def setUp(self):
        self.scratch = tempfile.TemporaryDirectory()
        self.addCleanup(self.scratch.cleanup)
        self.root = Path(self.scratch.name)
        self.q = json.loads(FIXTURE.read_text())
        self.path = self.root / 'ledger.sqlite'
        self.sim = self.bootstrap(self.path)
        self.addCleanup(lambda: self.sim.close())

    def bootstrap(self, path):
        sim = Simulator(path, self.q['deployment_id'], simulated_channel=True)
        sim.advance(self.q['task_id'], self.q['attempt_id'], 7)
        sim.approve(self.q['authorization_ref'], self.q['task_id'], self.q['attempt_id'], 7)
        return sim

    def send(self, q):
        return self.sim.handle(json.dumps(q))

    def command(self, a, op='observe', approval=None):
        q = dict(**{k: a[k] for k in TUPLE}, version=self.q['version'], op=op,
                 request_id=str(uuid.uuid4()), authorization_ref=approval or self.q['authorization_ref'])
        if op == 'cancel':
            q['reason_code'] = 'OPERATOR_REQUEST'
        return q

    def running(self, a):
        self.sim.simulate_evidence(a['session_id'], task_state='DISPATCHING', resource_state='CREATING', certainty='CONFIRMED', state_version=1)
        self.sim.simulate_evidence(a['session_id'], task_state='RUNNING', resource_state='ACTIVE_OBSERVED', certainty='OBSERVED', state_version=2)

    def result(self, a, outcome='SUCCEEDED'):
        return dict(**{k: a[k] for k in TUPLE}, result_id=str(uuid.uuid4()), outcome=outcome,
                    artifact_ref='fixture-output', artifact_sha256='c' * 64)

    def processes(self, actions, *, expected_exit=None):
        """Barrier after independent opens; no inherited SQLite connections/IPC sockets."""
        group = self.root / uuid.uuid4().hex
        group.mkdir()
        gate = group / 'go'
        children = []
        try:
            for i, action in enumerate(actions):
                base = group / str(i)
                spec = dict(ledger=str(self.path), deployment=self.q['deployment_id'],
                            ready=str(base.with_suffix('.ready')), output=str(base.with_suffix('.json')),
                            gate=str(gate), **action)
                specfile = base.with_suffix('.spec')
                specfile.write_text(json.dumps(spec))
                log = base.with_suffix('.log').open('w')
                try:
                    child = subprocess.Popen([sys.executable, '-B', str(HERE / 'process_worker.py'), str(specfile)],
                                             stdin=subprocess.DEVNULL, stdout=log, stderr=log)
                finally:
                    log.close()
                children.append((child, base))
            deadline = time.monotonic() + 15
            while not all(base.with_suffix('.ready').exists() for _, base in children):
                if any(child.poll() is not None for child, _ in children) or time.monotonic() >= deadline:
                    self.fail('worker failed before barrier: ' + '\n'.join(base.with_suffix('.log').read_text() for _, base in children))
                time.sleep(0.01)
            pids = [int(base.with_suffix('.ready').read_text()) for _, base in children]
            self.assertEqual(len(set(pids)), len(children))
            self.assertNotIn(os.getpid(), pids)
            gate.write_text('release')
            results = []
            for index, (child, base) in enumerate(children):
                code = child.wait(timeout=15)
                self.assertEqual(code, (expected_exit or {}).get(index, 0), base.with_suffix('.log').read_text())
                output = base.with_suffix('.json')
                results.append(json.loads(output.read_text())['result'] if output.exists() else None)
            return results
        finally:
            for child, _ in children:
                if child.poll() is None:
                    child.kill()  # Only the disposable Python test worker just created.
                child.wait(timeout=5)

    def reopen(self):
        self.sim.close()
        self.sim = Simulator(self.path, self.q['deployment_id'], simulated_channel=True)

    def test_process_simultaneous_start_and_restart_replay(self):
        actions = [dict(kind='handle', request=self.q) for _ in range(4)]
        replies = self.processes(actions)
        self.assertTrue(all(r == replies[0] for r in replies))
        self.reopen()
        self.assertEqual(self.send(self.q), replies[0])
        self.assertEqual(self.processes(actions), replies)
        self.assertEqual(len(self.sim.snapshot()['assignments']), 1)

    def test_process_new_keys_compete_for_one_attempt(self):
        requests = [dict(self.q, request_id=str(uuid.uuid4())) for _ in range(4)]
        replies = self.processes([dict(kind='handle', request=q) for q in requests])
        self.assertEqual(sum(r['error'] is None for r in replies), 1)
        self.assertTrue(all(r['error'] is None or r['error']['code'] == 'IDEMPOTENCY_CONFLICT' for r in replies))
        self.reopen()
        for q, r in zip(requests, replies):
            self.assertEqual(self.send(q), r)
        self.assertEqual(len(self.sim.snapshot()['assignments']), 1)

    def test_process_same_key_different_payload_has_one_winner(self):
        replies = self.processes([dict(kind='handle', request=self.q),
                                 dict(kind='handle', request=dict(self.q, execution_timeout_ms=1))])
        self.assertEqual(sum(r['error'] is None for r in replies), 1)
        self.assertEqual([r['error']['code'] for r in replies if r['error']], ['IDEMPOTENCY_CONFLICT'])
        self.assertEqual(len(self.sim.snapshot()['assignments']), 1)

    def test_process_exit_before_commit_rolls_back_and_after_commit_replays(self):
        before = self.sim.snapshot()
        self.processes([dict(kind='crash_before_commit', request=self.q)], expected_exit={0: 31})
        self.reopen()
        self.assertEqual(self.sim.snapshot(), before)
        self.processes([dict(kind='crash_after_commit', request=self.q)], expected_exit={0: 32})
        self.reopen()
        committed = self.sim.snapshot()['requests'][self.q['request_id']]['response']
        replies = self.processes([dict(kind='handle', request=self.q) for _ in range(3)])
        self.assertTrue(all(r == committed for r in replies))
        self.assertEqual(len(self.sim.snapshot()['assignments']), 1)

    def test_process_duplicate_and_competing_results_are_immutable(self):
        a = self.send(self.q)
        self.running(a)
        r = self.result(a)
        self.assertEqual(self.processes([dict(kind='result', result=r) for _ in range(3)]), ['ACCEPTED'] * 3)
        frozen = copy.deepcopy(self.sim.snapshot()['assignments'])
        other = self.result(a, 'FAILED')
        self.assertEqual(self.processes([dict(kind='result', result=other), dict(kind='result', result=dict(r, outcome='FAILED'))]), ['ALREADY_TERMINAL', 'IDEMPOTENCY_CONFLICT'])
        self.reopen()
        self.assertEqual(self.sim.snapshot()['assignments'], frozen)

    def test_process_conflicting_terminal_results_have_one_winner(self):
        a = self.send(self.q)
        self.running(a)
        success, failure = self.result(a), self.result(a, 'FAILED')
        replies = self.processes([dict(kind='result', result=success), dict(kind='result', result=failure)])
        self.assertCountEqual(replies, ['ACCEPTED', 'ALREADY_TERMINAL'])
        winner = (success, failure)[replies.index('ACCEPTED')]
        self.assertEqual(self.sim.snapshot()['assignments'][a['session_id']]['task_state'], winner['outcome'])

    def race_setup(self):
        a = self.send(self.q)
        self.running(a)
        next_q = dict(self.q, request_id=str(uuid.uuid4()), attempt_id=str(uuid.uuid4()), generation=8, authorization_ref='next')
        self.sim.approve('next', next_q['task_id'], next_q['attempt_id'], 8)
        actions = dict(cancel=dict(kind='handle', request=self.command(a, 'cancel')),
                       reassign=dict(kind='reassign', request=next_q), result=dict(kind='result', result=self.result(a)))
        return a, next_q, actions

    def assert_race(self, a, next_q, actions, replies):
        self.assertIn(replies['result'], ('ACCEPTED', 'STALE_GENERATION', 'OUTCOME_UNKNOWN'))
        error = replies['cancel']['error']
        self.assertIn(error['code'] if error else None, (None, 'STALE_GENERATION', 'ALREADY_TERMINAL'))
        new = replies['reassign']
        self.assertIsNone(new['error'])
        self.assertEqual(new['task_state'], 'ACCEPTED')
        self.running(new)
        self.assertEqual(self.sim.simulate_result(json.dumps(self.result(new))), 'ACCEPTED')
        frozen = copy.deepcopy(self.sim.snapshot()['assignments'][new['session_id']])
        # Replaying a formerly accepted result is historical ACK, no new publication.
        self.sim.simulate_result(json.dumps(actions['result']['result']))
        late = self.result(a, 'FAILED')
        self.assertEqual(self.sim.simulate_result(json.dumps(late)), 'STALE_GENERATION')
        late_cancel = self.command(a, 'cancel')
        self.assertEqual(self.send(late_cancel)['error']['code'], 'STALE_GENERATION')
        self.reopen()
        self.assertEqual(self.sim.snapshot()['assignments'][new['session_id']], frozen)
        self.assertEqual(self.sim.snapshot()['authority'][a['task_id']], dict(attempt_id=next_q['attempt_id'], generation=8))
        if replies['result'] == 'ACCEPTED':
            self.assertEqual(self.sim.snapshot()['assignments'][a['session_id']]['task_state'], 'SUCCEEDED')

    def test_process_three_way_race(self):
        a, q, actions = self.race_setup()
        names = list(actions)
        replies = dict(zip(names, self.processes([actions[name] for name in names])))
        self.assert_race(a, q, actions, replies)

    def test_all_serial_orders_cancel_reassign_result(self):
        # Deterministic coverage of every linearization, in addition to the race.
        for order in itertools.permutations(('cancel', 'reassign', 'result')):
            with self.subTest(order=order):
                self.sim.close()
                self.path = self.root / ('-'.join(order) + '.sqlite')
                self.sim = self.bootstrap(self.path)
                a, q, actions = self.race_setup()
                replies = {name: self.processes([actions[name]])[0] for name in order}
                expected = ('ACCEPTED' if order.index('result') < min(order.index('cancel'), order.index('reassign'))
                            else 'STALE_GENERATION' if order.index('reassign') < order.index('result') else 'OUTCOME_UNKNOWN')
                self.assertEqual(replies['result'], expected)
                self.assert_race(a, q, actions, replies)

    def store_body(self, body):
        with sqlite3.connect(self.path) as db:
            db.execute('UPDATE model SET body=? WHERE id=1', (body,))

    def test_missing_row_is_not_reinitialized(self):
        self.send(self.q)
        with sqlite3.connect(self.path) as db:
            db.execute('DELETE FROM model')
        before = self.path.read_bytes()
        with self.assertRaises(LedgerCorrupt):
            Simulator(self.path, self.q['deployment_id'])
        with self.assertRaises(LedgerCorrupt):
            self.send(self.q)
        self.assertEqual(self.path.read_bytes(), before)
        with sqlite3.connect(self.path) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM model').fetchone()[0], 0)

    def test_empty_and_truncated_existing_files_never_bootstrap(self):
        for content in (b'', b'SQLite format 3\x00' + b'\x00' * 64, b'not sqlite'):
            path = self.root / uuid.uuid4().hex
            path.write_bytes(content)
            with self.subTest(content=content[:20]), self.assertRaises((sqlite3.DatabaseError, LedgerCorrupt)):
                Simulator(path, self.q['deployment_id'])
            self.assertEqual(path.read_bytes(), content)

    def test_malformed_json_and_inconsistent_state_block_all_actions(self):
        a = self.send(self.q)
        self.running(a)
        self.sim.simulate_result(json.dumps(self.result(a)))
        good = self.sim.snapshot()
        def changed(edit):
            state = copy.deepcopy(good)
            edit(state)
            return canonical(state)
        sid = a['session_id']
        malformed = ['{', 'null', '[]', '{"deployment":1,"deployment":2}', canonical(good).replace('"state_version":4', '"state_version":NaN')]
        edits = [lambda s: s.pop('requests'), lambda s: s.update(deployment='d' * 32),
                 lambda s: s['assignments'][sid].update(state_version=True),
                 lambda s: s['assignments'][sid].update(task_state='RUNNING'),
                 lambda s: s['assignments'][sid].update(certainty='BOGUS'),
                 lambda s: s['assignments'][sid].update(generation=8),
                 lambda s: s['assignments'][sid].update(session_id='s-' + '0' * 32),
                 lambda s: s['requests'].clear(), lambda s: s['results'].clear(),
                 lambda s: s['used_attempts'].clear(), lambda s: s.update(late_results=[{}]),
                 lambda s: s['requests'][self.q['request_id']].update(session='s-' + '0' * 32)]
        for raw in malformed + [changed(edit) for edit in edits]:
            with self.subTest(raw=raw[:80]):
                self.store_body(raw)
                before = self.path.read_bytes()
                for operation in (lambda: Simulator(self.path, self.q['deployment_id']),
                                  lambda: self.send(self.q), lambda: self.send(self.command(a, 'recover')),
                                  lambda: self.sim.simulate_result(json.dumps(self.result(a)))):
                    with self.assertRaises(LedgerCorrupt):
                        operation()
                self.assertEqual(self.path.read_bytes(), before)
        # Explicit test-operator restoration of the exact intact data; no auto repair.
        self.store_body(canonical(good))
        self.reopen()
        self.assertEqual(self.send(self.q), good['requests'][self.q['request_id']]['response'])
        self.assertEqual(self.sim.snapshot()['assignments'], good['assignments'])

    def test_lock_timeout_is_fail_closed_and_retry_keeps_identity(self):
        self.sim.db.execute('PRAGMA busy_timeout=1')
        before = self.sim.snapshot()
        blocker = sqlite3.connect(self.path, isolation_level=None)
        try:
            blocker.execute('BEGIN IMMEDIATE')
            with self.assertRaises(sqlite3.OperationalError):
                self.send(self.q)
        finally:
            blocker.close()
        self.assertEqual(self.sim.snapshot(), before)
        ack = self.send(self.q)
        self.reopen()
        self.assertEqual(self.send(self.q), ack)

    def test_unrelated_sqlite_schema_is_not_repurposed(self):
        path = self.root / 'unrelated.sqlite'
        with sqlite3.connect(path) as db:
            db.execute('CREATE TABLE unrelated (value TEXT)')
            db.execute("INSERT INTO unrelated VALUES ('fixture-only')")
        before = path.read_bytes()
        with self.assertRaises(sqlite3.DatabaseError):
            Simulator(path, self.q['deployment_id'])
        self.assertEqual(path.read_bytes(), before)
        with sqlite3.connect(path) as db:
            self.assertEqual(db.execute('SELECT name FROM sqlite_master WHERE type="table"').fetchall(), [('unrelated',)])

    def test_independent_deployments_do_not_share_ids_approvals_or_mutations(self):
        a = self.send(self.q)
        other_path = self.root / 'other.sqlite'
        other_deployment = 'd' * 32
        other = Simulator(other_path, other_deployment, simulated_channel=True)
        self.addCleanup(other.close)
        other.advance(self.q['task_id'], self.q['attempt_id'], 7)
        other.approve(self.q['authorization_ref'], self.q['task_id'], self.q['attempt_id'], 7)
        q = dict(self.q, deployment_id=other_deployment)
        b = other.handle(json.dumps(q))
        self.assertIsNone(b['error'])
        self.assertNotEqual(a['session_id'], b['session_id'])
        before = self.sim.snapshot()['assignments']
        self.assertEqual(self.send(self.command(b, 'cancel'))['error']['code'], 'UNAUTHORIZED')
        self.assertEqual(self.sim.simulate_result(json.dumps(self.result(b))), 'RESOURCE_IDENTITY_UNVERIFIABLE')
        self.assertEqual(self.sim.snapshot()['assignments'], before)
        raw = self.path.read_bytes()
        with self.assertRaises(LedgerCorrupt):
            Simulator(self.path, other_deployment)
        self.assertEqual(self.path.read_bytes(), raw)

    def test_attempt_identity_cannot_cross_bind_sessions_or_lookup(self):
        a = self.send(self.q)
        next_q = dict(self.q, request_id=str(uuid.uuid4()), attempt_id=str(uuid.uuid4()), generation=8, authorization_ref='next')
        self.sim.advance(next_q['task_id'], next_q['attempt_id'], 8)
        self.sim.approve('next', next_q['task_id'], next_q['attempt_id'], 8)
        b = self.send(next_q)
        before = self.sim.snapshot()['assignments']
        q = self.command(b, 'cancel', 'next')
        q['session_id'] = a['session_id']
        self.assertEqual(self.send(q)['error']['code'], 'RESOURCE_IDENTITY_UNVERIFIABLE')
        lookup = dict(version=self.q['version'], op='observe', request_id=str(uuid.uuid4()), authorization_ref='next', lookup_request_id=self.q['request_id'])
        self.assertEqual(self.send(lookup)['error']['code'], 'UNAUTHORIZED')
        r = self.result(b)
        r['session_id'] = a['session_id']
        self.assertEqual(self.sim.simulate_result(json.dumps(r)), 'RESOURCE_IDENTITY_UNVERIFIABLE')
        self.assertEqual(self.sim.snapshot()['assignments'], before)


if __name__ == '__main__':
    unittest.main()
