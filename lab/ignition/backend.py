"""Local-only real-process experiment. Deliberately NOT a DMAOL adapter."""
import hashlib
import json
import math
import os
from pathlib import Path
import selectors
import sqlite3
import stat
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from contextlib import contextmanager

PROFILE = 'isolated-echo-v1'
PROGRAM = Path(__file__).with_name('echo_program.py').resolve()
INTERPRETER = str(Path(sys.executable).resolve())
ARTIFACT = b'IGNITION isolated-echo-v1\n'
CAPTURE_LIMIT = 4096
MAX_INT = 9007199254740991
FINAL = {'SUCCEEDED', 'FAILED', 'TIMED_OUT', 'CANCELLED', 'UNKNOWN'}


class LedgerError(ValueError):
    pass


def identifier(value):
    try:
        return type(value) is str and str(uuid.UUID(value)) == value and uuid.UUID(value).int != 0
    except (ValueError, AttributeError):
        return False


def integer(value):
    return type(value) is int and 1 <= value <= MAX_INT


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)


class Laboratory:
    """Creates only a fresh temporary lab; caller controls its explicit lifetime."""
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
    """Trusted local Python API. No JSON endpoint, arbitrary payload or CLI runner.

    Connections are short lived and serialized with SQLite. The lock/condition
    coordinate only directly owned children in this object, never adopted PIDs.
    """
    def __init__(self, root, *, initialize=False):
        if os.geteuid() == 0:
            raise PermissionError('root is forbidden')
        root = Path(root).absolute()
        if root.is_symlink() or root.resolve() != root or root.parent != Path(tempfile.gettempdir()).resolve() or not root.name.startswith('ignition-'):
            raise ValueError('a dedicated ignition temporary directory is required')
        info = root.stat()
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid():
            raise ValueError('invalid lab owner or directory')
        self.root = root
        self.ledger = root / 'ledger.sqlite'
        self._condition = threading.Condition()
        self._active = 0
        self._closed = False
        if initialize:
            if self.ledger.exists():
                raise LedgerError('refusing to overwrite existing ledger')
            with sqlite3.connect(self.ledger) as db:
                db.executescript('''
                    CREATE TABLE meta (id INTEGER PRIMARY KEY CHECK(id=1), version INTEGER NOT NULL CHECK(version=1));
                    INSERT INTO meta VALUES (1, 1);
                    CREATE TABLE generations (task TEXT NOT NULL, generation INTEGER NOT NULL,
                        attempt TEXT NOT NULL UNIQUE, PRIMARY KEY(task, generation));
                    CREATE TABLE attempts (request TEXT PRIMARY KEY, fingerprint TEXT NOT NULL,
                        task TEXT NOT NULL, attempt TEXT NOT NULL UNIQUE, generation INTEGER NOT NULL,
                        execution TEXT NOT NULL UNIQUE, timeout_ms INTEGER NOT NULL,
                        state TEXT NOT NULL CHECK(state IN ('INTENT','SPAWNING','RUNNING','CANCELLING','SUCCEEDED','FAILED','TIMED_OUT','CANCELLED','UNKNOWN')),
                        cancel INTEGER NOT NULL DEFAULT 0 CHECK(cancel IN (0,1)), result TEXT);
                ''')
        with self._db() as db:
            if [tuple(r) for r in db.execute('PRAGMA quick_check')] != [('ok',)]:
                raise LedgerError('corrupt SQLite')
            self._validate(db)

    @contextmanager
    def _db(self):
        if self._closed:
            raise RuntimeError('backend closed')
        # rw forbids accidental creation of an empty ledger after deletion.
        db = sqlite3.connect(self.ledger.as_uri() + '?mode=rw', uri=True, isolation_level=None)
        db.row_factory = sqlite3.Row
        try:
            db.execute('PRAGMA synchronous=FULL')
            db.execute('BEGIN IMMEDIATE')
            self._validate(db)
            yield db
            self._validate(db)
            db.execute('COMMIT')
        except BaseException:
            if db.in_transaction:
                db.execute('ROLLBACK')
            raise
        finally:
            db.close()

    @staticmethod
    def _validate(db):
        if [tuple(r) for r in db.execute('SELECT * FROM meta')] != [(1, 1)]:
            raise LedgerError('missing ledger identity; no automatic repair')
        for generation in db.execute('SELECT * FROM generations'):
            if not identifier(generation['task']) or not identifier(generation['attempt']) or not integer(generation['generation']):
                raise LedgerError('invalid authority history')
        for row in db.execute('SELECT * FROM attempts'):
            if not all(identifier(row[k]) for k in ('request', 'task', 'attempt', 'execution')) or not integer(row['generation']) or not integer(row['timeout_ms']) or row['timeout_ms'] > 5000:
                raise LedgerError('invalid attempt')
            expected_hash = hashlib.sha256(canonical(dict(task_id=row['task'], attempt_id=row['attempt'], generation=row['generation'], profile=PROFILE, timeout_ms=row['timeout_ms'])).encode()).hexdigest()
            if row['fingerprint'] != expected_hash:
                raise LedgerError('request fingerprint mismatch')
            authority = db.execute('SELECT 1 FROM generations WHERE task=? AND attempt=? AND generation=?', (row['task'], row['attempt'], row['generation'])).fetchone()
            if authority is None:
                raise LedgerError('missing authority history')
            if row['result'] is not None:
                try:
                    result = json.loads(row['result'])
                    if type(result) is not dict or result['execution_id'] != row['execution'] or result['attempt_id'] != row['attempt'] or result['generation'] != row['generation']:
                        raise LedgerError('result identity mismatch')
                    if result['outcome'] != row['state'] or row['state'] not in FINAL or type(result['accepted_as_current']) is not bool or result['accepted_as_current'] != (row['state'] == 'SUCCEEDED'):
                        raise LedgerError('inconsistent result state')
                    if not isinstance(result['duration_seconds'], (int, float)) or isinstance(result['duration_seconds'], bool) or not math.isfinite(result['duration_seconds']) or result['duration_seconds'] < 0:
                        raise LedgerError('invalid duration')
                    if any(type(result[k]) is not str or len(result[k].encode('utf-8')) > CAPTURE_LIMIT for k in ('stdout', 'stderr')):
                        raise LedgerError('invalid capture')
                    if row['state'] == 'SUCCEEDED' and (result['artifact_sha256'] != hashlib.sha256(ARTIFACT).hexdigest() or result['returncode'] != 0 or result['exit_confirmed'] is not True):
                        raise LedgerError('missing success evidence')
                except (ValueError, TypeError, KeyError) as exc:
                    raise LedgerError('invalid stored result') from exc
            elif row['state'] in ('SUCCEEDED', 'FAILED', 'TIMED_OUT'):
                raise LedgerError('terminal evidence missing')

    def close(self):
        with self._condition:
            if self._active:
                raise RuntimeError('wait for owned execution before closing')
            self._closed = True

    def advance(self, task_id, attempt_id, generation):
        """Trusted lab authority, NOT a generation supplied by an external client."""
        if not identifier(task_id) or not identifier(attempt_id) or not integer(generation):
            raise ValueError('invalid authority')
        with self._db() as db:
            current = db.execute('SELECT max(generation) FROM generations WHERE task=?', (task_id,)).fetchone()[0]
            if current is not None and generation <= current:
                raise ValueError('generation must advance')
            db.execute('INSERT INTO generations VALUES (?,?,?)', (task_id, generation, attempt_id))

    @staticmethod
    def _current(db, row):
        current = db.execute('SELECT attempt, generation FROM generations WHERE task=? ORDER BY generation DESC LIMIT 1', (row['task'],)).fetchone()
        return current is not None and tuple(current) == (row['attempt'], row['generation'])

    @staticmethod
    def _row(db, request_id):
        if not identifier(request_id):
            raise ValueError('invalid request_id')
        row = db.execute('SELECT * FROM attempts WHERE request=?', (request_id,)).fetchone()
        if row is None:
            raise KeyError('unknown request')
        return row

    @staticmethod
    def _view(row):
        return dict(request_id=row['request'], task_id=row['task'], attempt_id=row['attempt'],
                    generation=row['generation'], execution_id=row['execution'], state=row['state'],
                    result=json.loads(row['result']) if row['result'] is not None else None)

    def start(self, *, request_id, task_id, attempt_id, generation, profile=PROFILE, timeout_ms=2000):
        if not all(identifier(x) for x in (request_id, task_id, attempt_id)) or not integer(generation) or profile != PROFILE or not integer(timeout_ms) or timeout_ms > 5000:
            raise ValueError('invalid fixed-profile request')
        fingerprint = hashlib.sha256(canonical(dict(task_id=task_id, attempt_id=attempt_id, generation=generation, profile=profile, timeout_ms=timeout_ms)).encode()).hexdigest()
        with self._db() as db:
            prior = db.execute('SELECT * FROM attempts WHERE request=?', (request_id,)).fetchone()
            if prior:
                if prior['fingerprint'] != fingerprint:
                    raise ValueError('IDEMPOTENCY_CONFLICT')
                return self._view(prior)
            candidate = dict(task=task_id, attempt=attempt_id, generation=generation)
            if not self._current(db, candidate):
                raise ValueError('STALE_GENERATION')
            if db.execute('SELECT 1 FROM attempts WHERE attempt=?', (attempt_id,)).fetchone():
                raise ValueError('IDEMPOTENCY_CONFLICT')
            db.execute('INSERT INTO attempts (request,fingerprint,task,attempt,generation,execution,timeout_ms,state) VALUES (?,?,?,?,?,?,?,?)',
                       (request_id, fingerprint, task_id, attempt_id, generation, str(uuid.uuid4()), timeout_ms, 'INTENT'))
            return self._view(self._row(db, request_id))

    def observe(self, request_id):
        with self._db() as db:
            return self._view(self._row(db, request_id))

    def cancel(self, request_id):
        with self._db() as db:
            row = self._row(db, request_id)
            if not self._current(db, row):
                raise ValueError('STALE_GENERATION')
            if row['state'] == 'INTENT':
                db.execute("UPDATE attempts SET state='CANCELLED',cancel=1 WHERE request=?", (request_id,))
            elif row['state'] in ('SPAWNING', 'RUNNING', 'CANCELLING'):
                db.execute("UPDATE attempts SET state='CANCELLING',cancel=1 WHERE request=?", (request_id,))
            # UNKNOWN is never a license to signal a persisted PID.
            return self._view(self._row(db, request_id))

    def recover(self, request_id):
        with self._db() as db:
            row = self._row(db, request_id)
            if row['state'] in ('INTENT', 'SPAWNING', 'RUNNING', 'CANCELLING'):
                db.execute("UPDATE attempts SET state='UNKNOWN' WHERE request=?", (request_id,))
            return self._view(self._row(db, request_id))

    def execute(self, request_id):
        """One trusted local invocation; claim is durable before any subprocess."""
        with self._condition:
            if self._closed:
                raise RuntimeError('backend closed')
            self._active += 1
        try:
            return self._execute(request_id)
        finally:
            with self._condition:
                self._active -= 1
                self._condition.notify_all()

    def _execute(self, request_id):
        with self._db() as db:
            row = self._row(db, request_id)
            if row['state'] != 'INTENT':
                return self._view(row)
            if not self._current(db, row):
                db.execute("UPDATE attempts SET state='UNKNOWN' WHERE request=?", (request_id,))
                return self._view(self._row(db, request_id))
            db.execute("UPDATE attempts SET state='SPAWNING' WHERE request=?", (request_id,))
        started = time.monotonic()
        proc = None
        stdout, stderr = bytearray(), bytearray()
        totals = [0, 0]
        cause = None
        artifact_hash = None
        workspace = None
        try:
            # Workspace is generated here, never derived from client-supplied text.
            workspace = Path(tempfile.mkdtemp(prefix='run-', dir=self.root))
            proc = subprocess.Popen([INTERPRETER, '-I', '-B', str(PROGRAM)], cwd=workspace,
                                    env={'LANG': 'C.UTF-8', 'LC_ALL': 'C.UTF-8'}, stdin=subprocess.DEVNULL,
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True,
                                    shell=False)
            with self._db() as db:
                live = self._row(db, request_id)
                if live['state'] == 'SPAWNING':
                    db.execute("UPDATE attempts SET state='RUNNING' WHERE request=?", (request_id,))
            with selectors.DefaultSelector() as selector:
                for index, stream in enumerate((proc.stdout, proc.stderr)):
                    os.set_blocking(stream.fileno(), False)
                    selector.register(stream, selectors.EVENT_READ, index)
                stop_at = None
                while selector.get_map() or proc.poll() is None:
                    now = time.monotonic()
                    with self._db() as db:
                        live = self._row(db, request_id)
                        if cause is None and live['cancel']:
                            cause = 'CANCELLED'
                        elif cause is None and (live['state'] == 'UNKNOWN' or not self._current(db, live)):
                            cause = 'UNKNOWN'
                    if cause is None and now - started >= row['timeout_ms'] / 1000:
                        cause = 'TIMED_OUT'
                    if cause and proc.poll() is None:
                        if stop_at is None:
                            proc.terminate()  # Direct child still owned by this Popen.
                            stop_at = now
                        elif now - stop_at >= 0.25:
                            proc.kill()
                    for key, _ in selector.select(0.01):
                        data = os.read(key.fileobj.fileno(), 16384)
                        if not data:
                            selector.unregister(key.fileobj)
                            key.fileobj.close()
                            continue
                        index = key.data
                        totals[index] += len(data)
                        target = (stdout, stderr)[index]
                        target.extend(data[:max(0, CAPTURE_LIMIT - len(target))])
                proc.wait()
            artifact = workspace / 'artifact.txt'
            if cause is None:
                if proc.returncode == 0:
                    fd = os.open(artifact, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
                    try:
                        if not stat.S_ISREG(os.fstat(fd).st_mode):
                            raise ValueError('invalid artifact type')
                        content = os.read(fd, len(ARTIFACT) + 1)
                        if content != ARTIFACT:
                            raise ValueError('unexpected artifact')
                        os.fsync(fd)
                    finally:
                        os.close(fd)
                    directory_fd = os.open(workspace, os.O_RDONLY | os.O_DIRECTORY)
                    try:
                        os.fsync(directory_fd)
                    finally:
                        os.close(directory_fd)
                    artifact_hash = hashlib.sha256(content).hexdigest()
                    cause = 'SUCCEEDED'
                else:
                    cause = 'FAILED'
        except Exception:
            # Unrecognized launch/I/O/storage failure does not justify success or retry.
            cause = 'UNKNOWN'
        finally:
            if proc is not None:
                if proc.poll() is None:
                    proc.terminate()
                    try:
                        proc.wait(timeout=0.25)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                        proc.wait()
                for stream in (proc.stdout, proc.stderr):
                    if stream is not None:
                        stream.close()
        duration = time.monotonic() - started
        with self._db() as db:
            live = self._row(db, request_id)
            # Publication and generation check share the same write transaction.
            accepted = cause == 'SUCCEEDED' and self._current(db, live) and live['state'] == 'RUNNING' and not live['cancel']
            state = cause
            if not self._current(db, live) or live['state'] == 'UNKNOWN':
                state = 'UNKNOWN'
            elif live['cancel'] and cause == 'SUCCEEDED':
                state = 'UNKNOWN'  # Cannot establish result/cancellation order.
            result = dict(execution_id=live['execution'], attempt_id=live['attempt'], generation=live['generation'],
                          outcome=state, accepted_as_current=accepted,
                          artifact_sha256=artifact_hash, artifact_name='artifact.txt' if artifact_hash else None,
                          workspace_name=workspace.name if workspace else None,
                          duration_seconds=duration, returncode=proc.returncode if proc else None,
                          exit_confirmed=proc is not None and proc.returncode is not None,
                          stdout=stdout.decode('utf-8', errors='replace'), stderr=stderr.decode('utf-8', errors='replace'),
                          stdout_bytes=totals[0], stderr_bytes=totals[1],
                          stdout_truncated=totals[0] > CAPTURE_LIMIT, stderr_truncated=totals[1] > CAPTURE_LIMIT)
            db.execute('UPDATE attempts SET state=?,result=? WHERE request=?', (state, canonical(result), request_id))
            return self._view(self._row(db, request_id))
