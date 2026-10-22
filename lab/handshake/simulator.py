"""Offline contract model. No Executor imports, transport, processes or resources."""
import copy
import datetime
import hashlib
import json
import re
import sqlite3
import uuid
from contextlib import contextmanager
from pathlib import Path

VERSION = 'dmaol-executor/1'
MAX_INT = 9007199254740991
TASKS = {'ACCEPTED', 'DISPATCHING', 'RUNNING', 'CANCEL_REQUESTED', 'SUCCEEDED', 'FAILED', 'CANCELLED', 'UNKNOWN'}
RESOURCES = {'NOT_CREATED', 'CREATING', 'ACTIVE_OBSERVED', 'STOPPING', 'CLOSED_CONFIRMED', 'UNKNOWN'}
CERTAINTIES = {'CONFIRMED', 'OBSERVED', 'UNKNOWN'}
TERMINAL = {'SUCCEEDED', 'FAILED', 'CANCELLED'}
TUPLE = {'deployment_id', 'task_id', 'attempt_id', 'generation', 'session_id'}
COMMON = {'version', 'op', 'request_id', 'authorization_ref'}
START = {'profile_ref', 'input_ref', 'input_sha256', 'execution_timeout_ms'}


class InvalidRequest(ValueError):
    pass


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)


def uid(value):
    try:
        return type(value) is str and str(uuid.UUID(value)) == value and uuid.UUID(value).int != 0
    except (ValueError, AttributeError):
        return False


def integer(value):
    return type(value) is int and 1 <= value <= MAX_INT


def token(value, size):
    return type(value) is str and re.fullmatch('[0-9a-f]{%d}' % size, value) is not None


def reference(value):
    return type(value) is str and re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,127}', value) is not None


def decode(raw):
    if type(raw) not in (str, bytes):
        raise InvalidRequest('JSON text required')
    try:
        if len(raw.encode('utf-8') if isinstance(raw, str) else raw) > 16384:
            raise InvalidRequest('message too large')
        def pairs(items):
            result = {}
            for key, value in items:
                if key in result:
                    raise InvalidRequest('duplicate key')
                result[key] = value
            return result
        def constant(_):
            raise InvalidRequest('nonfinite number')
        if type(raw) is bytes:
            raw = raw.decode('utf-8', errors='strict')
        return json.loads(raw, object_pairs_hook=pairs, parse_constant=constant)
    except (ValueError, UnicodeError, RecursionError) as exc:
        raise InvalidRequest('invalid JSON') from exc


def validate(q):
    if type(q) is not dict:
        raise InvalidRequest('object required')
    op = q.get('op')
    if op not in ('start', 'observe', 'cancel', 'recover'):
        raise InvalidRequest('invalid operation')
    lookup = op == 'observe' and 'lookup_request_id' in q
    expected = COMMON | ({'lookup_request_id'} if lookup else TUPLE)
    if op == 'start':
        expected |= START
    if op == 'cancel':
        expected |= {'reason_code'}
    if set(q) != expected or q['version'] != VERSION or not uid(q['request_id']) or not reference(q['authorization_ref']):
        raise InvalidRequest('invalid envelope')
    if lookup:
        if not uid(q['lookup_request_id']):
            raise InvalidRequest('invalid lookup')
        return
    if not token(q['deployment_id'], 32) or not uid(q['task_id']) or not uid(q['attempt_id']) or not integer(q['generation']):
        raise InvalidRequest('invalid identity')
    if op == 'start':
        if q['session_id'] is not None or not all(reference(q[k]) for k in ('profile_ref', 'input_ref')) or not token(q['input_sha256'], 64) or not integer(q['execution_timeout_ms']):
            raise InvalidRequest('invalid start')
    elif type(q['session_id']) is not str or re.fullmatch(r's-[0-9a-f]{32}', q['session_id']) is None:
        raise InvalidRequest('invalid session')
    if op == 'cancel' and q['reason_code'] != 'OPERATOR_REQUEST':
        raise InvalidRequest('invalid reason')


class LedgerCorrupt(ValueError):
    """Local storage failure. Never translated into a successful wire reply."""


def validate_ledger(s, deployment):
    """Check structure and retained links, not authenticity of local storage."""
    def require(condition):
        if not condition:
            raise LedgerCorrupt('inconsistent ledger')

    def exact(value, keys):
        require(type(value) is dict and set(value) == set(keys))

    def identity(value):
        require(value['deployment_id'] == deployment)
        require(uid(value['task_id']) and uid(value['attempt_id']) and integer(value['generation']))
        require(type(value['session_id']) is str and re.fullmatch(r's-[0-9a-f]{32}', value['session_id']) is not None)

    def state(value):
        require(type(value['task_state']) is str and value['task_state'] in TASKS)
        require(type(value['resource_state']) is str and value['resource_state'] in RESOURCES)
        require(type(value['certainty']) is str and value['certainty'] in CERTAINTIES)
        require(type(value['observed_at']) is str)
        require(datetime.datetime.fromisoformat(value['observed_at']).utcoffset() is not None)
        require(value['executor_instance'] is None)

    exact(s, {'deployment', 'authority', 'approvals', 'requests', 'assignments', 'results', 'late_results', 'used_attempts'})
    require(s['deployment'] == deployment)
    for field in ('authority', 'approvals', 'requests', 'assignments', 'results'):
        require(type(s[field]) is dict)
    require(type(s['used_attempts']) is list and all(uid(x) for x in s['used_attempts']))
    require(len(set(s['used_attempts'])) == len(s['used_attempts']))
    require(type(s['late_results']) is list)
    authority_attempts = set()
    for task, current in s['authority'].items():
        exact(current, {'attempt_id', 'generation'})
        require(uid(task) and uid(current['attempt_id']) and integer(current['generation']))
        require(current['attempt_id'] in s['used_attempts'] and current['attempt_id'] not in authority_attempts)
        authority_attempts.add(current['attempt_id'])
    for ref, approval in s['approvals'].items():
        exact(approval, {'task_id', 'attempt_id', 'generation', 'actions', 'expired'})
        require(reference(ref) and uid(approval['task_id']) and uid(approval['attempt_id']) and integer(approval['generation']))
        require(type(approval['expired']) is bool and type(approval['actions']) is list)
        require(all(type(x) is str and x in ('start', 'observe', 'cancel', 'recover') for x in approval['actions']))
    assignment_keys = TUPLE | {'operation_id', 'executor_instance', 'state_version', 'task_state', 'resource_state', 'certainty', 'observed_at'}
    attempts, operations = set(), set()
    for session, a in s['assignments'].items():
        exact(a, assignment_keys)
        identity(a)
        state(a)
        require(session == a['session_id'] and uid(a['operation_id']) and integer(a['state_version']))
        require(a['attempt_id'] in s['used_attempts'] and a['attempt_id'] not in attempts and a['operation_id'] not in operations)
        attempts.add(a['attempt_id'])
        operations.add(a['operation_id'])
        current = s['authority'].get(a['task_id'])
        require(current is not None and current['generation'] >= a['generation'])
        require(current['generation'] != a['generation'] or current['attempt_id'] == a['attempt_id'])
    response_keys = assignment_keys | {'version', 'request_id', 'source', 'error'}
    starts = set()
    for request, record in s['requests'].items():
        exact(record, {'hash', 'response', 'op', 'session'})
        require(uid(request) and token(record['hash'], 64) and record['op'] in ('start', 'observe', 'cancel', 'recover'))
        r = record['response']
        exact(r, response_keys)
        state(r)
        require(r['version'] == VERSION and r['source'] == 'simulator-ledger' and r['request_id'] == request and r['deployment_id'] == deployment)
        if r['error'] is not None:
            exact(r['error'], {'code', 'operation', 'effect', 'retry', 'operator_review_required', 'executor_error_code'})
            error = r['error']
            require(type(error['code']) is str and error['code'] in {'INVALID_REQUEST', 'UNAUTHORIZED', 'APPROVAL_EXPIRED', 'DEPENDENCY_UNVERIFIED', 'STALE_GENERATION', 'IDEMPOTENCY_CONFLICT', 'BUSY', 'ALREADY_TERMINAL', 'OUTCOME_UNKNOWN', 'RESOURCE_IDENTITY_UNVERIFIABLE'})
            require(error == Simulator.error(error['code'], record['op']))
        if record['session'] is None:
            require(all(r[k] is None for k in ('task_id', 'attempt_id', 'generation', 'session_id', 'operation_id', 'state_version')))
            require(r['error'] is not None)
        else:
            a = s['assignments'].get(record['session'])
            require(a is not None)
            identity(r)
            require(all(r[k] == a[k] for k in TUPLE | {'operation_id'}))
            require(integer(r['state_version']) and r['state_version'] <= a['state_version'])
            if record['op'] == 'start' and r['error'] is None:
                require(record['session'] not in starts)
                starts.add(record['session'])
    require(starts == set(s['assignments']))
    accepted = set()
    for result_id, record in s['results'].items():
        exact(record, {'body', 'status'})
        r = record['body']
        exact(r, TUPLE | {'result_id', 'outcome', 'artifact_ref', 'artifact_sha256'})
        # Rejected identities may belong to another deployment; their syntax is
        # still checked, but they must never become accepted assignments.
        require(uid(result_id) and r['result_id'] == result_id and token(r['deployment_id'], 32))
        require(uid(r['task_id']) and uid(r['attempt_id']) and integer(r['generation']))
        require(type(r['session_id']) is str and re.fullmatch(r's-[0-9a-f]{32}', r['session_id']) is not None)
        require(r['outcome'] in ('SUCCEEDED', 'FAILED') and reference(r['artifact_ref']) and token(r['artifact_sha256'], 64))
        require(record['status'] in ('ACCEPTED', 'RESOURCE_IDENTITY_UNVERIFIABLE', 'STALE_GENERATION', 'ALREADY_TERMINAL', 'OUTCOME_UNKNOWN'))
        if record['status'] == 'ACCEPTED':
            a = s['assignments'].get(r['session_id'])
            require(a is not None and all(a[k] == r[k] for k in TUPLE))
            require(a['task_state'] == r['outcome'] and r['session_id'] not in accepted)
            accepted.add(r['session_id'])
    require(accepted == {session for session, a in s['assignments'].items() if a['task_state'] in ('SUCCEEDED', 'FAILED')})
    late_ids = set()
    for r in s['late_results']:
        require(type(r) is dict and uid(r.get('result_id')))
        record = s['results'].get(r['result_id'])
        require(record is not None and record['body'] == r and record['status'] == 'STALE_GENERATION' and r['result_id'] not in late_ids)
        late_ids.add(r['result_id'])
    require(late_ids == {key for key, r in s['results'].items() if r['status'] == 'STALE_GENERATION'})


class Simulator:
    """Caller supplies an explicit scratch ledger path; no default production paths.

    SQLite serializes the fixture authority, requests, assignments and results.
    All knobs below are trusted test controls, never wire operations.
    """
    def __init__(self, ledger, deployment_id, *, simulated_channel=False):
        if not token(deployment_id, 32) or type(simulated_channel) is not bool:
            raise ValueError('invalid simulator configuration')
        self.deployment = deployment_id
        self.channel = simulated_channel
        # Existing storage must never be silently reinitialized. Bootstrap one
        # fresh scratch path before opening concurrent workers.
        existing = Path(ledger).exists()
        self.db = sqlite3.connect(ledger, isolation_level=None)
        try:
            self.db.execute('PRAGMA synchronous=FULL')
            self.db.execute('BEGIN IMMEDIATE')
            if not existing:
                self.db.execute('CREATE TABLE model (id INTEGER PRIMARY KEY CHECK(id=1), body TEXT NOT NULL)')
                initial = {'deployment': deployment_id, 'authority': {}, 'approvals': {}, 'requests': {}, 'assignments': {}, 'results': {}, 'late_results': [], 'used_attempts': []}
                self.db.execute('INSERT INTO model VALUES (1, ?)', (canonical(initial),))
            if self.db.execute('PRAGMA quick_check').fetchall() != [('ok',)]:
                raise LedgerCorrupt('SQLite integrity check failed')
            self.snapshot()
            self.db.execute('COMMIT')
        except BaseException:
            if self.db.in_transaction:
                self.db.execute('ROLLBACK')
            self.db.close()
            raise

    def close(self):
        self.db.close()

    def snapshot(self):
        try:
            rows = self.db.execute('SELECT id, body FROM model').fetchall()
            if len(rows) != 1 or rows[0][0] != 1:
                raise LedgerCorrupt('missing or inconsistent model row')
            def pairs(items):
                result = {}
                for key, value in items:
                    if key in result:
                        raise LedgerCorrupt('duplicate ledger key')
                    result[key] = value
                return result
            def constant(_):
                raise LedgerCorrupt('nonfinite ledger number')
            state = json.loads(rows[0][1], object_pairs_hook=pairs, parse_constant=constant)
            validate_ledger(state, self.deployment)
            return state
        except (ValueError, TypeError, KeyError, OverflowError, RecursionError) as exc:
            raise LedgerCorrupt('ledger invalid; preserve for operator review') from exc

    @contextmanager
    def transaction(self):
        self.db.execute('BEGIN IMMEDIATE')
        try:
            state = self.snapshot()
            yield state
            validate_ledger(state, self.deployment)
            self.db.execute('UPDATE model SET body=? WHERE id=1', (canonical(state),))
            self.db.execute('COMMIT')
        except BaseException:
            self.db.execute('ROLLBACK')
            raise

    def approve(self, ref, task_id, attempt_id, generation, *, actions=('start', 'observe', 'cancel', 'recover'), expired=False):
        """Trusted fixture approval, scoped to this deployment and exact attempt."""
        if not reference(ref) or not uid(task_id) or not uid(attempt_id) or not integer(generation) or not set(actions) <= {'start', 'observe', 'cancel', 'recover'} or type(expired) is not bool:
            raise ValueError('invalid approval')
        with self.transaction() as s:
            s['approvals'][ref] = dict(task_id=task_id, attempt_id=attempt_id, generation=generation, actions=list(actions), expired=expired)

    def advance(self, task_id, attempt_id, generation):
        """Fixture authority: atomic revocation/advance, never inferred from input."""
        if not uid(task_id) or not uid(attempt_id) or not integer(generation):
            raise ValueError('invalid authority')
        with self.transaction() as s:
            old = s['authority'].get(task_id)
            if old and generation <= old['generation']:
                raise ValueError('authority must advance')
            if attempt_id in s['used_attempts']:
                raise ValueError('attempt reused')
            s['used_attempts'].append(attempt_id)
            s['authority'][task_id] = dict(attempt_id=attempt_id, generation=generation)

    @staticmethod
    def error(code, op):
        uncertain = code == 'OUTCOME_UNKNOWN'
        return dict(code=code, operation=op, effect='MAY_HAVE_OCCURRED' if uncertain else 'NONE', retry='OBSERVE_SAME_OPERATION' if uncertain else 'DO_NOT_RETRY', operator_review_required=uncertain or code == 'RESOURCE_IDENTITY_UNVERIFIABLE', executor_error_code=None)

    def response(self, q, a=None, code=None):
        r = dict(version=VERSION, request_id=q.get('request_id') if uid(q.get('request_id')) else None,
                 operation_id=None, deployment_id=self.deployment, task_id=None, attempt_id=None,
                 generation=None, session_id=None, executor_instance=None, state_version=None,
                 task_state='UNKNOWN', resource_state='UNKNOWN', certainty='UNKNOWN',
                 observed_at=datetime.datetime.now(datetime.timezone.utc).isoformat(), source='simulator-ledger', error=None)
        if a:
            r.update(copy.deepcopy(a))
        if code:
            r['error'] = self.error(code, q.get('op') if q.get('op') in ('start', 'observe', 'cancel', 'recover') else None)
        return r

    def handle(self, raw):
        """Strict wire entry point. Returns JSON-compatible response; commits before ACK."""
        q = {}
        try:
            q = decode(raw)
            validate(q)
        except InvalidRequest:
            return self.response(q if type(q) is dict else {}, code='INVALID_REQUEST')
        fingerprint = hashlib.sha256(canonical(q).encode()).hexdigest()
        with self.transaction() as s:
            prior = s['requests'].get(q['request_id'])
            if prior:
                if prior['hash'] != fingerprint:
                    return self.response(q, code='IDEMPOTENCY_CONFLICT')
                return copy.deepcopy(prior['response'])
            a = None
            if 'lookup_request_id' in q:
                original = s['requests'].get(q['lookup_request_id'])
                if original and original['op'] == 'start':
                    a = s['assignments'].get(original['session'])
            elif q['op'] != 'start':
                a = s['assignments'].get(q['session_id'])
            identity = a if 'lookup_request_id' in q else q
            approval = s['approvals'].get(q['authorization_ref'])
            code = None
            if not identity or not approval or q['op'] not in approval['actions'] or any(identity[k] != approval[k] for k in ('task_id', 'attempt_id', 'generation')) or identity['deployment_id'] != self.deployment:
                code = 'UNAUTHORIZED'
            elif approval['expired']:
                code = 'APPROVAL_EXPIRED'
            elif 'lookup_request_id' not in q and q['op'] != 'start' and (not a or any(a[k] != q[k] for k in TUPLE)):
                code = 'RESOURCE_IDENTITY_UNVERIFIABLE'
            elif q['op'] in ('start', 'cancel') and s['authority'].get(identity['task_id']) != {k: identity[k] for k in ('attempt_id', 'generation')}:
                code = 'STALE_GENERATION'
            elif q['op'] == 'start':
                if any(x['attempt_id'] == q['attempt_id'] for x in s['assignments'].values()):
                    code = 'IDEMPOTENCY_CONFLICT'
                elif not self.channel:
                    code = 'DEPENDENCY_UNVERIFIED'
                elif q['profile_ref'] != 'isolated-task-v1' or q['execution_timeout_ms'] > 60000:
                    code = 'UNAUTHORIZED'
                else:
                    a = {k: q[k] for k in TUPLE}
                    a.update(session_id='s-' + uuid.uuid4().hex, operation_id=str(uuid.uuid4()), executor_instance=None,
                             observed_at=datetime.datetime.now(datetime.timezone.utc).isoformat(), state_version=1, task_state='ACCEPTED', resource_state='NOT_CREATED', certainty='CONFIRMED')
                    if a['session_id'] in s['assignments']:
                        raise ValueError('session identifier collision')
                    s['assignments'][a['session_id']] = a
            elif q['op'] == 'cancel':
                if a['task_state'] in TERMINAL:
                    code = 'ALREADY_TERMINAL'
                elif a['certainty'] == 'UNKNOWN' or a['task_state'] == 'UNKNOWN':
                    code = 'OUTCOME_UNKNOWN'
                else:
                    a.update(task_state='CANCEL_REQUESTED', state_version=a['state_version'] + 1)
            elif q['op'] == 'recover':
                # No local ledger entry is evidence of live processes or task results.
                if a['task_state'] not in TERMINAL:
                    a['task_state'] = 'UNKNOWN'
                a.update(resource_state='UNKNOWN', certainty='UNKNOWN', state_version=a['state_version'] + 1)
                code = 'OUTCOME_UNKNOWN'
            if a and ((q['op'] == 'cancel' and code is None) or (q['op'] == 'recover' and code == 'OUTCOME_UNKNOWN')):
                a['observed_at'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
            # Do not disclose unrelated assignment on identity/authorization failure.
            visible = a if code not in ('UNAUTHORIZED', 'APPROVAL_EXPIRED', 'RESOURCE_IDENTITY_UNVERIFIABLE') else None
            r = self.response(q, visible, code)
            s['requests'][q['request_id']] = dict(hash=fingerprint, response=r, op=q['op'], session=a['session_id'] if visible else None)
            return r

    def simulate_evidence(self, session_id, *, task_state, resource_state, certainty, state_version):
        """Trusted scenario input, NOT an Executor observation or wire API.

        Only model forward nonterminal transitions; cancellation needs a simulated
        result-channel reconciliation as well as confirmed resource closure.
        """
        if task_state not in TASKS or resource_state not in RESOURCES or certainty not in CERTAINTIES or not integer(state_version):
            raise ValueError('invalid evidence')
        transitions = {'ACCEPTED': {'DISPATCHING', 'UNKNOWN'}, 'DISPATCHING': {'RUNNING', 'UNKNOWN'}, 'RUNNING': {'UNKNOWN'}, 'CANCEL_REQUESTED': {'CANCELLED', 'UNKNOWN'}, 'UNKNOWN': {'UNKNOWN'}}
        with self.transaction() as s:
            a = s['assignments'][session_id]
            if state_version != a['state_version']:
                raise ValueError('stale state_version')
            if task_state != a['task_state'] and task_state not in transitions.get(a['task_state'], set()):
                raise ValueError('invalid transition')
            if task_state == 'CANCELLED' and (resource_state != 'CLOSED_CONFIRMED' or certainty != 'CONFIRMED'):
                raise ValueError('cancellation unconfirmed')
            if resource_state == 'UNKNOWN' and certainty != 'UNKNOWN':
                raise ValueError('inconsistent certainty')
            if resource_state == 'ACTIVE_OBSERVED' and certainty != 'OBSERVED':
                raise ValueError('snapshot requires OBSERVED')
            a.update(task_state=task_state, resource_state=resource_state, certainty=certainty, state_version=state_version + 1, observed_at=datetime.datetime.now(datetime.timezone.utc).isoformat())

    def simulate_result(self, raw):
        """Trusted simulated authenticated result source; never accepts real artifacts."""
        r = decode(raw)
        keys = TUPLE | {'result_id', 'outcome', 'artifact_ref', 'artifact_sha256'}
        if type(r) is not dict or set(r) != keys or not uid(r['result_id']) or r['outcome'] not in ('SUCCEEDED', 'FAILED') or not reference(r['artifact_ref']) or not token(r['artifact_sha256'], 64):
            raise InvalidRequest('invalid result')
        validate(dict(version=VERSION, op='observe', request_id=r['result_id'], authorization_ref='fixture', **{k: r[k] for k in TUPLE}))
        with self.transaction() as s:
            prior = s['results'].get(r['result_id'])
            if prior:
                return prior['status'] if prior['body'] == r else 'IDEMPOTENCY_CONFLICT'
            a = s['assignments'].get(r['session_id'])
            if not a or any(a[k] != r[k] for k in TUPLE):
                status = 'RESOURCE_IDENTITY_UNVERIFIABLE'
            elif s['authority'].get(r['task_id']) != {k: r[k] for k in ('attempt_id', 'generation')}:
                status = 'STALE_GENERATION'
                s['late_results'].append(r)
            elif a['task_state'] in TERMINAL:
                status = 'ALREADY_TERMINAL'
            elif a['task_state'] != 'RUNNING':
                a.update(task_state='UNKNOWN', certainty='UNKNOWN', state_version=a['state_version'] + 1, observed_at=datetime.datetime.now(datetime.timezone.utc).isoformat())
                status = 'OUTCOME_UNKNOWN'
            else:
                a.update(task_state=r['outcome'], state_version=a['state_version'] + 1, observed_at=datetime.datetime.now(datetime.timezone.utc).isoformat())
                status = 'ACCEPTED'
            s['results'][r['result_id']] = dict(body=r, status=status)
            return status
