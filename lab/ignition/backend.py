"""Trusted local facade over HANDSHAKE authority and fixed-process supervision."""
import os
import json
from pathlib import Path
import subprocess  # Compatibility for existing test instrumentation; never shell.
import tempfile
import threading
import uuid
from contextlib import contextmanager

from lab.handshake.ledger import ExecutionLedger, LedgerCorrupt, TUPLE
from .process_runner import ARTIFACT, CAPTURE_LIMIT, PROFILE, ProcessRunner

LedgerError = LedgerCorrupt
LOCAL_DEPLOYMENT = '0' * 32


class Laboratory:
    def __init__(self):
        if os.geteuid() == 0:
            raise PermissionError('root is forbidden')
        self.directory = tempfile.TemporaryDirectory(prefix='ignition-')
        self.root = Path(self.directory.name)
        self.backend = Backend(self.root, initialize=True)

    def close(self):
        self.backend.close()
        self.directory.cleanup()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


class Backend:
    authority_type = ExecutionLedger

    def __init__(self, root, *, initialize=False, deployment_id=LOCAL_DEPLOYMENT, timeout_termination=True):
        if os.geteuid() == 0:
            raise PermissionError('root is forbidden')
        root = Path(root).absolute()
        if (root.is_symlink() or root.resolve() != root or root.parent != Path(tempfile.gettempdir()).resolve()
                or not root.name.startswith('ignition-') or root.stat().st_uid != os.geteuid()):
            raise ValueError('a dedicated owned ignition temporary directory is required')
        self.root = root
        self.ledger = root / 'ledger.sqlite'
        self.deployment = deployment_id
        self.timeout_termination = timeout_termination
        self._condition = threading.Condition()
        self._active = 0
        self._closed = False
        store = self.authority_type(self.ledger, deployment_id, initialize=initialize, timeout_termination=timeout_termination)
        store.close()

    @contextmanager
    def _db(self):
        if self._closed:
            raise RuntimeError('backend closed')
        store = self.authority_type(self.ledger, self.deployment, timeout_termination=self.timeout_termination)
        try:
            yield store
        finally:
            store.close()

    def close(self):
        with self._condition:
            if self._active:
                raise RuntimeError('wait for owned execution before closing')
            self._closed = True

    def advance(self, task_id, attempt_id, generation):
        with self._db() as store:
            store.advance(task_id, attempt_id, generation)

    @staticmethod
    def _session(store, request):
        record = store.snapshot()['requests'].get(request)
        if not record or record['op'] != 'start' or record['session'] is None:
            raise KeyError('unknown start request')
        return record['session']

    @staticmethod
    def _view(store, session):
        a = store.snapshot()['assignments'][session]
        run = store.run(session)
        e = run['evidence']
        result = None
        state = run['phase']
        if a['task_state'] == 'CANCELLED':
            state = 'CANCELLED'
        elif a['task_state'] == 'CANCEL_REQUESTED' and e is None:
            state = 'CANCELLING'
        elif a['task_state'] == 'UNKNOWN':
            state = 'UNKNOWN'
        if e is not None:
            state = ('SUCCEEDED' if a['task_state'] == 'SUCCEEDED' else 'CANCELLED' if a['task_state'] == 'CANCELLED'
                     else 'TIMED_OUT' if run['timed_out'] else 'FAILED' if e['returncode'] != 0 and not run['cancel_requested'] else 'UNKNOWN')
            result = dict(execution_id=run['execution_id'], attempt_id=a['attempt_id'], generation=a['generation'],
                          outcome=state, accepted_as_current=a['task_state'] == 'SUCCEEDED',
                          artifact_sha256=e['artifact_sha256'], artifact_name='artifact.txt' if e['artifact_sha256'] else None,
                          workspace_name=run['workspace_name'], duration_seconds=e['duration_seconds'],
                          returncode=e['returncode'], exit_confirmed=e['exit_confirmed'])
            for k in ('stdout', 'stderr', 'stdout_bytes', 'stderr_bytes', 'stdout_truncated', 'stderr_truncated'):
                result[k] = e[k]
        return dict(request_id=run['request_id'], task_id=a['task_id'], attempt_id=a['attempt_id'],
                    generation=a['generation'], execution_id=run['execution_id'], state=state, result=result)

    def start(self, *, request_id, task_id, attempt_id, generation, profile=PROFILE, timeout_ms=2000):
        # Reuse the contractual validator, authority, request hash and assignment.
        from lab.handshake.ledger import uid, integer
        if not all(uid(x) for x in (request_id, task_id, attempt_id)) or not integer(generation) or profile != PROFILE or not integer(timeout_ms) or timeout_ms > 5000:
            raise ValueError('invalid fixed-profile request')
        with self._db() as store:
            approval = 'local-' + request_id
            store.approve(approval, task_id, attempt_id, generation)
            response = store.handle(json.dumps(dict(version='dmaol-executor/1', op='start',
                request_id=request_id, deployment_id=self.deployment, task_id=task_id, attempt_id=attempt_id,
                generation=generation, session_id=None, authorization_ref=approval, profile_ref=profile,
                input_ref=store.INPUT_REF, input_sha256=store.INPUT_HASH, execution_timeout_ms=timeout_ms)))
            if response['error']:
                raise ValueError(response['error']['code'])
            return self._view(store, response['session_id'])

    def observe(self, request_id):
        with self._db() as store:
            return self._view(store, self._session(store, request_id))

    def facts(self, request_id):
        with self._db() as store:
            return store.run(self._session(store, request_id))

    def cancel(self, request_id):
        import json
        with self._db() as store:
            session = self._session(store, request_id)
            a, run = store.snapshot()['assignments'][session], store.run(session)
            response = store.handle(json.dumps(dict(version='dmaol-executor/1', op='cancel', request_id=str(uuid.uuid4()),
                authorization_ref=run['authorization_ref'], reason_code='OPERATOR_REQUEST', **{k: a[k] for k in TUPLE})))
            if response['error'] and response['error']['code'] == 'STALE_GENERATION':
                raise ValueError('STALE_GENERATION')
            return self._view(store, session)

    def recover(self, request_id):
        with self._db() as store:
            session = self._session(store, request_id)
            store.uncertain(session)  # Knowledge only, NEVER cancellation.
            return self._view(store, session)

    def execute(self, request_id):
        with self._db() as store:
            session = self._session(store, request_id)
        self.execute_session(session)
        return self.observe(request_id)

    def execute_session(self, session):
        with self._condition:
            if self._closed:
                raise RuntimeError('backend closed')
            self._active += 1
        runner = ProcessRunner()
        try:
            with self._db() as store:
                if not store.claim(session):
                    return
            workspace = self.root / session
            try:
                workspace.mkdir()  # Never reuse an old workspace.
                with self._db() as store:
                    launched = store.launch(session, lambda: runner.launch(workspace))
                if not launched:
                    return
                def cancelled():
                    with self._db() as store:
                        return store.run(session)['cancel_requested']
                def timed_out():
                    with self._db() as store:
                        store.timeout(session)
                with self._db() as store:
                    timeout = store.run(session)['timeout_ms']
                evidence = runner.supervise(timeout_ms=timeout, cancel_requested=cancelled,
                                            on_timeout=timed_out, allow_timeout_termination=self.timeout_termination)
                with self._db() as store:
                    store.finish(session, evidence)
            except Exception:
                if runner.proc is not None and runner.proc.poll() is None:
                    # No kill or implicit cancel on storage/observation errors.
                    runner.supervise(timeout_ms=5000, cancel_requested=lambda: False,
                                     on_timeout=lambda: None, allow_timeout_termination=False)
                with self._db() as store:
                    store.uncertain(session)
        finally:
            with self._condition:
                self._active -= 1
                self._condition.notify_all()
