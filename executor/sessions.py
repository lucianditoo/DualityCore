#!/usr/bin/env python3
"""Administrative v3 session lifecycle. Never expose directly to agents.

All filesystem mutations are relative to validated, pinned directory descriptors.
The CLI requires an operator-owned configuration attesting legacy fencing.
"""
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import select
import signal
import stat
import subprocess
import sys
import time
import uuid
from contextlib import contextmanager, ExitStack

CONFIG = Path('/opt/duality-core/executor/config.v3.json')
STATE_ROOT = Path('/var/lib/duality-core/sessions-v3')
GATEWAY_ROOT = Path('/run/duality-core/gateways-v3')
GATEWAY_SERVER = '/opt/duality-core/gateway/gateway.py'
SESSION_RE = re.compile(r'[a-z0-9][a-z0-9-]{0,39}')
HEX_ID = re.compile(r'[0-9a-f]{64}')
TOKEN = re.compile(r'[0-9a-f]{32}')
LOCK_TIMEOUT = 5.0
GATEWAY_TIMEOUT = 5.0
DOCKER_TIMEOUT = 15
MAX_RECORD = 16384
PHASES = {'prepared', 'spawning', 'gateway', 'container', 'starting', 'active', 'stopping', 'closed'}
# The launcher cannot exec the gateway until its kernel identity is durable.
# EOF on Bridge/executor death or a 5 s release timeout exits without exec.
GATEWAY_LAUNCHER = """import os, select, sys
fd = int(sys.argv[1])
poller = select.poll()
poller.register(fd, select.POLLIN | select.POLLHUP)
events = poller.poll(5000)
permit = os.read(fd, 1) if events else b''
os.close(fd)
if permit != b'1':
    raise SystemExit(125)
os.execv(sys.argv[2], sys.argv[2:])
"""
FIELDS = {'schema_version', 'deployment_id', 'session_id', 'name', 'instance',
          'phase', 'gateway', 'gateway_exit_confirmed', 'container_id',
          'docker_uncertain', 'start_attempted'}


def fail(message):
    raise RuntimeError(message)


def valid_token(value):
    return type(value) is str and TOKEN.fullmatch(value) is not None


def validate_boot_id(value):
    try:
        parsed = uuid.UUID(value) if type(value) is str else None
    except ValueError:
        parsed = None
    if parsed is None or str(parsed) != value or parsed.int == 0:
        fail('BOOT_ID_INVALID')
    return value


def current_boot_id():
    return validate_boot_id(Path('/proc/sys/kernel/random/boot_id').read_text().strip())


def validate_identity(identity):
    if (type(identity) is not dict
            or set(identity) != {'pid', 'boot_id', 'process_start_ticks'}
            or type(identity['pid']) is not int or not 0 < identity['pid'] <= 2**31-1
            or type(identity['process_start_ticks']) is not int
            or not 0 < identity['process_start_ticks'] < 2**64):
        fail('GATEWAY_IDENTITY_RECORD_INVALID')
    validate_boot_id(identity['boot_id'])


def read_process_identity(pid):
    if type(pid) is not int or not 0 < pid <= 2**31-1:
        fail('PROCESS_PID_INVALID')
    boot = current_boot_id()
    raw = Path(f'/proc/{pid}/stat').read_text()
    closing = raw.rfind(')')
    if closing < 0 or raw.split(' ', 1)[0] != str(pid):
        fail('PROCESS_STAT_INVALID')
    tail = raw[closing + 2:].split()
    if len(tail) < 20:
        fail('PROCESS_STAT_INVALID')
    try:
        ticks = int(tail[19])
    except ValueError:
        fail('PROCESS_STAT_INVALID')
    identity = dict(pid=pid, boot_id=boot, process_start_ticks=ticks)
    validate_identity(identity)
    return identity


def gateway_command(sock):
    return ['/usr/bin/python3', '-B', GATEWAY_SERVER, str(sock),
            '--upstream-port', '18767', '--admin-key-file',
            '/etc/duality-core/auth/gateway-admin.key']


def pidfd_exited(fd, timeout_ms=0):
    poller = select.poll()
    poller.register(fd, select.POLLIN)
    events = poller.poll(timeout_ms)
    if any(flags & (select.POLLNVAL | select.POLLERR) for _, flags in events):
        fail('PIDFD_POLL_ERROR')
    return any(flags & select.POLLIN for _, flags in events)


class Gateway:
    """Keep the verified pidfd open across Docker preflight and shutdown."""
    def __init__(self, identity, sock):
        self.identity, self.sock, self.fd = identity, sock, None
        self.status = None

    def __enter__(self):
        validate_identity(self.identity)
        if self.identity['boot_id'] != current_boot_id():
            self.status = 'EXITED_PREVIOUS_BOOT'
            return self
        try:
            self.fd = os.pidfd_open(self.identity['pid'], 0)
            if pidfd_exited(self.fd):
                self.status = 'EXITED'
                return self
            actual = read_process_identity(self.identity['pid'])
            if actual != self.identity:
                fail('GATEWAY_PROCESS_IDENTITY_MISMATCH')
            args = Path(f'/proc/{self.identity["pid"]}/cmdline').read_bytes().split(b'\0')
            if args and args[-1] == b'':
                args.pop()
            if args != [arg.encode() for arg in gateway_command(self.sock)]:
                if pidfd_exited(self.fd):
                    self.status = 'EXITED'
                    return self
                fail('GATEWAY_COMMAND_MISMATCH')
            if pidfd_exited(self.fd):
                self.status = 'EXITED'
            else:
                self.status = 'LIVE_VERIFIED'
            return self
        except ProcessLookupError:
            self.status = 'EXITED'
            return self
        except FileNotFoundError:
            try:
                if self.fd is not None and pidfd_exited(self.fd):
                    self.status = 'EXITED'
                    return self
                fail('GATEWAY_IDENTITY_UNVERIFIABLE')
            except BaseException:
                self.__exit__(None, None, None)
                raise
        except BaseException:
            self.__exit__(None, None, None)
            raise

    def terminate(self):
        if self.status == 'LIVE_VERIFIED':
            try:
                signal.pidfd_send_signal(self.fd, signal.SIGTERM)
            except ProcessLookupError:
                # ESRCH confirms the pinned target is gone; never signal by PID.
                self.status = 'EXITED'
                return self.status
            if not pidfd_exited(self.fd, int(GATEWAY_TIMEOUT * 1000)):
                fail('GATEWAY_TERMINATION_TIMEOUT')
            self.status = 'EXITED'
        return self.status

    def __exit__(self, *exc):
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None


def terminate_verified_gateway(pid, sock, identity):
    validate_identity(identity)
    if type(pid) is not int or pid != identity['pid']:
        fail('GATEWAY_PID_MISMATCH')
    with Gateway(identity, sock) as gateway:
        return gateway.terminate()


def _check_directory(st, leaf=False):
    # A root-owned sticky ancestor (e.g. /tmp in tests) cannot have our owned
    # next component replaced by other users. It is never accepted as a root leaf.
    sticky_ancestor = (not leaf and st.st_uid == 0 and st.st_mode & stat.S_ISVTX)
    owners = {os.geteuid()} if leaf else {0, os.geteuid()}
    if (not stat.S_ISDIR(st.st_mode) or st.st_uid not in owners
            or (st.st_mode & 0o022 and not sticky_ancestor)):
        fail('UNTRUSTED_DIRECTORY')


class Directory:
    def __init__(self, fd, path):
        self.fd, self.path = fd, Path(path)

    @classmethod
    def open(cls, path):
        raw = os.fspath(path)
        if not raw.startswith('/') or raw.startswith('//') or any(p in ('.', '..') for p in raw.split('/')) or ',' in raw:
            fail('PATH_INVALID')
        fd = os.open('/', os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
        try:
            _check_directory(os.fstat(fd))
            parts = [p for p in raw.split('/') if p]
            for index, component in enumerate(parts):
                child = os.open(component, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                                dir_fd=fd)
                os.close(fd)
                fd = child
                _check_directory(os.fstat(fd), leaf=index == len(parts)-1)
            if not parts:
                _check_directory(os.fstat(fd), leaf=True)
            return cls(fd, raw)
        except BaseException:
            os.close(fd)
            raise

    @staticmethod
    def component(name):
        if type(name) is not str or name in ('', '.', '..') or '/' in name or '\0' in name:
            fail('PATH_COMPONENT_INVALID')
        return name

    def child(self, name):
        name = self.component(name)
        fd = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                     dir_fd=self.fd)
        try:
            _check_directory(os.fstat(fd), leaf=True)
            return Directory(fd, self.path / name)
        except BaseException:
            os.close(fd)
            raise

    def mkdir(self, name, mode=0o700):
        os.mkdir(self.component(name), mode, dir_fd=self.fd)
        os.fsync(self.fd)
        return self.child(name)

    def entries(self):
        return sorted(os.listdir(self.fd))

    def stat(self, name):
        return os.stat(self.component(name), dir_fd=self.fd, follow_symlinks=False)

    def unlink(self, name, missing_ok=False):
        try:
            os.unlink(self.component(name), dir_fd=self.fd)
        except FileNotFoundError:
            if not missing_ok:
                raise
        os.fsync(self.fd)

    def rmdir(self, name):
        os.rmdir(self.component(name), dir_fd=self.fd)
        os.fsync(self.fd)

    def close(self):
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            fail('DUPLICATE_JSON_KEY')
        result[key] = value
    return result


def _decode(raw):
    return json.loads(raw, object_pairs_hook=_unique_object,
                      parse_constant=lambda value: fail('NONFINITE_JSON_NUMBER'))


def read_private_json(directory, name):
    fd = os.open(directory.component(name), os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC,
                 dir_fd=directory.fd)
    try:
        st = os.fstat(fd)
        if (not stat.S_ISREG(st.st_mode) or st.st_uid != os.geteuid()
                or stat.S_IMODE(st.st_mode) != 0o600 or st.st_nlink != 1 or st.st_size > MAX_RECORD):
            fail('UNTRUSTED_METADATA')
        raw = b''
        while len(raw) <= MAX_RECORD:
            chunk = os.read(fd, MAX_RECORD + 1 - len(raw))
            if not chunk:
                break
            raw += chunk
        if len(raw) > MAX_RECORD:
            fail('METADATA_TOO_LARGE')
        return _decode(raw)
    finally:
        os.close(fd)


def canonical(record):
    return json.dumps(record, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def write_record(directory, record):
    validate_record(record, record['session_id'], record['name'], record['deployment_id'])
    data = canonical(dict(record=record, sha256=hashlib.sha256(canonical(record)).hexdigest())) + b'\n'
    if len(data) > MAX_RECORD:
        fail('METADATA_TOO_LARGE')
    name = 'record-' + uuid.uuid4().hex + '.tmp'
    fd = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC,
                 0o600, dir_fd=directory.fd)
    try:
        with os.fdopen(fd, 'wb') as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(name, 'session.json', src_dir_fd=directory.fd, dst_dir_fd=directory.fd)
        os.fsync(directory.fd)
    finally:
        directory.unlink(name, missing_ok=True)


def read_record(directory, session_id, name, deployment_id):
    envelope = read_private_json(directory, 'session.json')
    if (type(envelope) is not dict or set(envelope) != {'record', 'sha256'}
            or type(envelope['sha256']) is not str or not HEX_ID.fullmatch(envelope['sha256'])
            or hashlib.sha256(canonical(envelope['record'])).hexdigest() != envelope['sha256']):
        fail('RECORD_INTEGRITY_INVALID')
    record = envelope['record']
    validate_record(record, session_id, name, deployment_id)
    return record


def session_name(cfg, session_id):
    if type(session_id) is not str or not SESSION_RE.fullmatch(session_id):
        fail('SESSION_ID_INVALID')
    # The suffix exceeds legacy SESSION_RE's entire 40-character budget.
    return f'{cfg["container_prefix"]}-lifecycle-v3-{cfg["deployment_id"]}-{session_id}'


def validate_config(cfg):
    required = {'lifecycle_schema', 'deployment_id', 'legacy_executor_fenced',
                'legacy_container_prefix', 'legacy_workspace_root', 'container_prefix', 'workspace_root',
                'agent_uid', 'agent_gid', 'pids_limit', 'memory', 'cpus', 'image'}
    if type(cfg) is not dict or not required.issubset(cfg):
        fail('CONFIG_INVALID')
    if type(cfg['lifecycle_schema']) is not int or cfg['lifecycle_schema'] != 3:
        fail('CONFIG_SCHEMA_INVALID')
    if cfg['legacy_executor_fenced'] is not True or not valid_token(cfg['deployment_id']):
        fail('LEGACY_FENCING_REQUIRED')
    for key in ('container_prefix', 'legacy_container_prefix'):
        if type(cfg[key]) is not str or not SESSION_RE.fullmatch(cfg[key]):
            fail('CONTAINER_PREFIX_INVALID')
    for key in ('agent_uid', 'agent_gid', 'pids_limit'):
        if type(cfg[key]) is not int or not 0 < cfg[key] < 2**31:
            fail('CONFIG_INTEGER_INVALID')
    if cfg['agent_uid'] == os.geteuid():
        fail('AGENT_OPERATOR_UID_MUST_DIFFER')
    if (type(cfg['workspace_root']) is not str or not cfg['workspace_root'].startswith('/')
            or any(c in cfg['workspace_root'] for c in (',', '\0'))
            or type(cfg['memory']) is not str or not re.fullmatch(r'[1-9][0-9]*[kmg]?', cfg['memory'])
            or type(cfg['cpus']) not in (int, float) or not 0 < cfg['cpus'] <= 1024
            or type(cfg['image']) is not str or not cfg['image'] or cfg['image'].startswith('-')):
        fail('CONFIG_INVALID')
    paths = []
    for key in ('workspace_root', 'legacy_workspace_root'):
        value = cfg[key]
        if (type(value) is not str or not value.startswith('/') or value.startswith('//') or str(Path(value)) != value
                or '..' in Path(value).parts or ',' in value or '\0' in value):
            fail('WORKSPACE_ROOT_INVALID')
        paths.append(Path(value))
    all_paths = paths + [Path(STATE_ROOT), Path(GATEWAY_ROOT)]
    for index, first in enumerate(all_paths):
        for second in all_paths[index+1:]:
            if first.is_relative_to(second) or second.is_relative_to(first):
                fail('WORKSPACE_NAMESPACE_OVERLAP')
    new = session_name(cfg, 'x')
    old_prefix = cfg['legacy_container_prefix'] + '-'
    if new.startswith(old_prefix) and SESSION_RE.fullmatch(new[len(old_prefix):]):
        fail('LEGACY_NAMESPACE_COLLISION')


def validate_layout(cfg, roots):
    """Reject symlinked legacy roots and bind aliases before session operations."""
    with Directory.open(cfg['workspace_root']) as workspace, Directory.open(cfg['legacy_workspace_root']) as legacy:
        descriptors = [root.fd for root in roots] + [workspace.fd, legacy.fd]
        identities = [(os.fstat(fd).st_dev, os.fstat(fd).st_ino) for fd in descriptors]
        if len(set(identities)) != len(identities):
            fail('LIFECYCLE_ROOT_ALIAS')


def validate_record(record, session_id, name, deployment_id):
    if (type(record) is not dict or set(record) != FIELDS
            or type(record['schema_version']) is not int or record['schema_version'] != 3
            or not valid_token(record['deployment_id']) or record['deployment_id'] != deployment_id
            or type(record['session_id']) is not str or not SESSION_RE.fullmatch(record['session_id'])
            or record['session_id'] != session_id or type(record['name']) is not str or record['name'] != name
            or not valid_token(record['instance']) or type(record['phase']) is not str
            or record['phase'] not in PHASES
            or any(type(record[k]) is not bool for k in
                   ('docker_uncertain', 'start_attempted', 'gateway_exit_confirmed'))
            or (record['container_id'] is not None and
                (type(record['container_id']) is not str or not HEX_ID.fullmatch(record['container_id'])))):
        fail('SESSION_RECORD_INVALID')
    gateway, cid, phase = record['gateway'], record['container_id'], record['phase']
    if gateway is not None:
        validate_identity(gateway)
    if gateway is None and not record['gateway_exit_confirmed'] and phase != 'spawning':
        fail('GATEWAY_IDENTITY_UNAVAILABLE')
    if record['docker_uncertain'] and (cid is not None or record['start_attempted']):
        fail('SESSION_STATE_INCONSISTENT')
    if record['start_attempted'] and cid is None and phase != 'closed':
        fail('SESSION_STATE_INCONSISTENT')
    if phase == 'prepared' and (gateway is not None or not record['gateway_exit_confirmed']
                               or cid is not None or record['docker_uncertain'] or record['start_attempted']):
        fail('SESSION_STATE_INCONSISTENT')
    if phase == 'spawning' and (gateway is not None or record['gateway_exit_confirmed']
                               or cid is not None or record['docker_uncertain'] or record['start_attempted']):
        fail('SESSION_STATE_INCONSISTENT')
    if phase in ('gateway', 'container', 'starting', 'active') and (
            gateway is None or record['gateway_exit_confirmed']):
        fail('SESSION_STATE_INCONSISTENT')
    if phase == 'gateway' and cid is not None:
        fail('SESSION_STATE_INCONSISTENT')
    if phase in ('container', 'starting', 'active') and cid is None:
        fail('SESSION_STATE_INCONSISTENT')
    if phase in ('starting', 'active') and not record['start_attempted']:
        fail('SESSION_STATE_INCONSISTENT')
    if phase in ('gateway', 'container') and record['start_attempted']:
        fail('SESSION_STATE_INCONSISTENT')
    if phase == 'closed' and (cid is not None or record['docker_uncertain'] or not record['gateway_exit_confirmed']):
        fail('SESSION_STATE_INCONSISTENT')


@contextmanager
def lifecycle_lock(timeout=LOCK_TIMEOUT, read_only=False):
    # Roots must be provisioned by the operator; recover never creates roots/locks.
    with Directory.open(STATE_ROOT) as state, Directory.open(GATEWAY_ROOT) as runtime:
        state_st, runtime_st = os.fstat(state.fd), os.fstat(runtime.fd)
        if (state_st.st_dev, state_st.st_ino) == (runtime_st.st_dev, runtime_st.st_ino):
            fail('STATE_RUNTIME_ROOT_ALIAS')
        flags = os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC
        fd = os.open('.lifecycle.lock', flags | (0 if read_only else os.O_CREAT), 0o600, dir_fd=state.fd)
        try:
            st = os.fstat(fd)
            if (not stat.S_ISREG(st.st_mode) or st.st_uid != os.geteuid()
                    or stat.S_IMODE(st.st_mode) != 0o600 or st.st_nlink != 1):
                fail('UNTRUSTED_LOCK')
            deadline = time.monotonic() + timeout
            while True:
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if time.monotonic() >= deadline:
                        fail('LIFECYCLE_LOCK_TIMEOUT')
                    time.sleep(min(0.05, max(0, deadline - time.monotonic())))
            yield state, runtime
        finally:
            os.close(fd)


def docker(*args):
    return subprocess.run(['/usr/bin/docker', *args], check=True, text=True,
                          capture_output=True, timeout=DOCKER_TIMEOUT).stdout.strip()


def inspect_managed_container(name, session_id, instance=None, expected_id=None, running=None):
    if name == 'duality-sol-sandbox':
        fail('PROTECTED_CONTAINER')
    listing = docker('container', 'ls', '-a', '--no-trunc', '--format', '{{json .}}')
    matches = []
    for line in listing.splitlines():
        item = _decode(line)
        if type(item) is not dict:
            fail('CONTAINER_LIST_INVALID')
        if item.get('Names') == name or (expected_id is not None and item.get('ID') == expected_id):
            matches.append(item)
    if not matches:
        return None
    if len(matches) != 1:
        fail('CONTAINER_IDENTITY_AMBIGUOUS')
    cid = matches[0].get('ID')
    if type(cid) is not str or not HEX_ID.fullmatch(cid):
        fail('CONTAINER_ID_INVALID')
    data = _decode(docker('container', 'inspect', cid))
    if type(data) is not list or len(data) != 1 or type(data[0]) is not dict:
        fail('CONTAINER_INSPECT_INVALID')
    info = data[0]
    if info.get('Id') != cid or (expected_id is not None and cid != expected_id):
        fail('CONTAINER_ID_MISMATCH')
    if info.get('Name') != '/' + name:
        fail('CONTAINER_NAME_MISMATCH')
    config = info.get('Config')
    if type(config) is not dict:
        fail('CONTAINER_CONFIG_INVALID')
    labels = config.get('Labels')
    expected = {'org.duality-core.managed': 'true', 'org.duality-core.component': 'agent-session',
                'org.duality-core.session-id': session_id, 'org.duality-core.lifecycle-schema': '3'}
    if instance is not None:
        expected['org.duality-core.instance'] = instance
    if type(labels) is not dict or any(labels.get(k) != v for k, v in expected.items()):
        fail('CONTAINER_LABEL_MISMATCH')
    state = info.get('State')
    if (type(state) is not dict or any(type(state.get(k)) is not bool for k in ('Running', 'Paused', 'Restarting'))
            or state.get('Status') not in ('created', 'running', 'paused', 'restarting', 'removing', 'exited', 'dead')):
        fail('CONTAINER_STATE_INVALID')
    if running is True and (not state['Running'] or state['Paused'] or state['Restarting'] or state['Status'] != 'running'):
        fail('CONTAINER_NOT_RUNNING')
    if running is False and (state['Running'] or state['Paused'] or state['Restarting'] or state['Status'] not in ('created', 'exited')):
        fail('CONTAINER_NOT_STOPPED')
    return info


def contents(directory, runtime=False):
    if directory is None:
        return []
    unexpected = []
    for name in directory.entries():
        st = directory.stat(name)
        okay = (name == 'gateway.sock' and stat.S_ISSOCK(st.st_mode) and st.st_uid == os.geteuid()) if runtime else (
            name == 'session.json' and stat.S_ISREG(st.st_mode))
        if not okay:
            unexpected.append(name)
    return unexpected


def optional_child(directory, session_id):
    try:
        return directory.child(session_id)
    except FileNotFoundError:
        return None


def prepare_workspace(cfg, session_id):
    with Directory.open(cfg['workspace_root']) as parent:
        try:
            os.mkdir(session_id, 0o700, dir_fd=parent.fd)
            created = True
        except FileExistsError:
            created = False
        fd = os.open(session_id, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=parent.fd)
        try:
            if created:
                os.fchown(fd, cfg['agent_uid'], cfg['agent_gid'])
                os.fsync(parent.fd)
            st = os.fstat(fd)
            if (not stat.S_ISDIR(st.st_mode) or st.st_uid != cfg['agent_uid']
                    or st.st_gid != cfg['agent_gid'] or st.st_mode & 0o077):
                fail('WORKSPACE_OWNER_OR_MODE_INVALID')
        finally:
            os.close(fd)
    return Path(cfg['workspace_root']) / session_id


def _inspect(record, running=None):
    return inspect_managed_container(record['name'], record['session_id'], record['instance'],
                                     record['container_id'], running=running)


def _stop_container(record):
    info = _inspect(record)
    if info is None:
        if record['docker_uncertain']:
            fail('DOCKER_CREATE_OUTCOME_UNCERTAIN')
        return
    cid = info['Id']
    if info['State']['Paused'] or info['State']['Restarting']:
        fail('CONTAINER_SHUTDOWN_STATE_UNSAFE')
    if info['State']['Running']:
        docker('kill', '--signal=TERM', cid)
        result = docker('wait', cid)
        if not re.fullmatch(r'[0-9]{1,3}', result) or int(result) > 255:
            fail('CONTAINER_EXIT_UNCONFIRMED')
    if _inspect(record, running=False) is None:
        fail('CONTAINER_EXIT_UNCONFIRMED')


def cleanup(cfg, session_id, roots):
    state, runtime = roots
    name = session_name(cfg, session_id)
    with ExitStack() as stack:
        journal = optional_child(state, session_id)
        if journal is not None:
            stack.enter_context(journal)
        gateway_dir = optional_child(runtime, session_id)
        if gateway_dir is not None:
            stack.enter_context(gateway_dir)
        if journal is None:
            if gateway_dir is not None or inspect_managed_container(name, session_id) is not None:
                fail('SESSION_METADATA_MISSING')
            return
        record = read_record(journal, session_id, name, cfg['deployment_id'])
        if contents(journal) or contents(gateway_dir, runtime=True):
            fail('UNEXPECTED_SESSION_FILE')
        if record['phase'] == 'spawning':
            fail('GATEWAY_SPAWN_OUTCOME_UNCERTAIN')
        # Validate both resources before the first destructive action, retaining
        # the process descriptor while graceful container shutdown runs.
        handle = None
        if not record['gateway_exit_confirmed']:
            handle = stack.enter_context(Gateway(record['gateway'], GATEWAY_ROOT / session_id / 'gateway.sock'))
        info = _inspect(record)
        if info is None and record['docker_uncertain']:
            fail('DOCKER_CREATE_OUTCOME_UNCERTAIN')
        if info is not None and record['container_id'] is None:
            record['container_id'] = info['Id']
            record['docker_uncertain'] = False
        already_closed = record['phase'] == 'closed'
        if already_closed and info is not None:
            fail('CLOSED_SESSION_HAS_CONTAINER')
        if not already_closed:
            record['phase'] = 'stopping'
            write_record(journal, record)
            _stop_container(record)
        if handle is not None:
            handle.terminate()
            if handle.status not in ('EXITED', 'EXITED_PREVIOUS_BOOT'):
                fail('GATEWAY_EXIT_UNCONFIRMED')
        record['gateway_exit_confirmed'] = True
        write_record(journal, record)
        # No forced deletion: rm itself also refuses a concurrently running ID.
        if info is not None:
            stopped = _inspect(record, running=False)
            if stopped is not None:
                docker('rm', record['container_id'])
            if _inspect(record) is not None:
                fail('CONTAINER_REMOVAL_UNCONFIRMED')
        record['container_id'] = None
        record['docker_uncertain'] = False
        record['phase'] = 'closed'
        write_record(journal, record)
        if contents(journal) or contents(gateway_dir, runtime=True):
            fail('UNEXPECTED_SESSION_FILE')
        if gateway_dir is not None:
            gateway_dir.unlink('gateway.sock', missing_ok=True)
            runtime.rmdir(session_id)
        journal.unlink('session.json')
        state.rmdir(session_id)


def create_container(cfg, record, journal, workspace, gateway_dir):
    record['docker_uncertain'] = True
    write_record(journal, record)
    cid = docker(
        'create', '--name', record['name'], '--restart', 'no',
        '--label', 'org.duality-core.managed=true',
        '--label', 'org.duality-core.component=agent-session',
        '--label', 'org.duality-core.lifecycle-schema=3',
        '--label', f'org.duality-core.session-id={record["session_id"]}',
        '--label', f'org.duality-core.instance={record["instance"]}',
        '--network', 'none', '--read-only', '--cap-drop', 'ALL',
        '--security-opt', 'no-new-privileges:true',
        '--pids-limit', str(cfg['pids_limit']), '--memory', cfg['memory'], '--cpus', str(cfg['cpus']),
        '--user', f'{cfg["agent_uid"]}:{cfg["agent_gid"]}',
        '--tmpfs', '/tmp:rw,nosuid,nodev,size=64m',
        '--tmpfs', f'/home/agent/.codex:rw,nosuid,nodev,uid={cfg["agent_uid"]},gid={cfg["agent_gid"]},size=64m',
        '--mount', 'type=bind,src=/opt/duality-core/executor/profiles/duality_developer.config.toml,dst=/home/agent/.codex/duality_developer.config.toml,readonly',
        '--mount', f'type=bind,src={workspace},dst=/workspace',
        '--mount', f'type=bind,src={gateway_dir.path},dst=/gateway,readonly',
        '--workdir', '/workspace',
        '--mount', 'type=bind,src=/opt/duality-core/executor/bridge.js,dst=/opt/duality/bridge.js,readonly',
        cfg['image'], 'node', '/opt/duality/bridge.js')
    if type(cid) is not str or not HEX_ID.fullmatch(cid):
        fail('CONTAINER_CREATE_RESPONSE_INVALID')
    record['container_id'] = cid
    record['docker_uncertain'] = False
    record['phase'] = 'container'
    write_record(journal, record)
    if _inspect(record) is None:
        fail('CONTAINER_CREATE_UNCONFIRMED')
    record['start_attempted'] = True
    record['phase'] = 'starting'
    write_record(journal, record)
    docker('start', cid)


def verify_survival(proc, record, socket):
    if proc.poll() is not None:
        fail('GATEWAY_EXITED_DURING_CREATE')
    with Gateway(record['gateway'], socket) as handle:
        if handle.status != 'LIVE_VERIFIED':
            fail('GATEWAY_EXITED_DURING_CREATE')
    if _inspect(record, running=True) is None:
        fail('CONTAINER_NOT_RUNNING')
    if proc.poll() is not None:
        fail('GATEWAY_EXITED_DURING_CREATE')


def create(cfg, session_id, roots):
    state, runtime = roots
    name = session_name(cfg, session_id)
    if session_id in state.entries() or session_id in runtime.entries():
        fail('SESSION_EXISTS')
    if inspect_managed_container(name, session_id) is not None:
        fail('CONTAINER_SESSION_EXISTS')
    workspace = prepare_workspace(cfg, session_id)
    record = dict(schema_version=3, deployment_id=cfg['deployment_id'], session_id=session_id,
                  name=name, instance=uuid.uuid4().hex, phase='prepared', gateway=None,
                  gateway_exit_confirmed=True, container_id=None, docker_uncertain=False,
                  start_attempted=False)
    proc = None
    journal = state.mkdir(session_id)
    gateway_dir = None
    try:
        write_record(journal, record)
        gateway_dir = runtime.mkdir(session_id)
        os.fchown(gateway_dir.fd, os.geteuid(), cfg['agent_gid'])
        os.fchmod(gateway_dir.fd, 0o710)
        os.fsync(gateway_dir.fd)
        record['phase'] = 'spawning'
        record['gateway_exit_confirmed'] = False
        write_record(journal, record)
        sock = gateway_dir.path / 'gateway.sock'
        gate_read, gate_write = os.pipe2(os.O_CLOEXEC)
        try:
            proc = subprocess.Popen(
                ['/usr/bin/python3', '-B', '-c', GATEWAY_LAUNCHER, str(gate_read), *gateway_command(sock)],
                stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                start_new_session=True, close_fds=True, pass_fds=(gate_read,))
            os.close(gate_read)
            gate_read = None
            record['gateway'] = read_process_identity(proc.pid)
            record['phase'] = 'gateway'
            write_record(journal, record)
            if os.write(gate_write, b'1') != 1:
                fail('GATEWAY_RELEASE_FAILED')
        finally:
            if gate_read is not None:
                os.close(gate_read)
            os.close(gate_write)
        deadline = time.monotonic() + GATEWAY_TIMEOUT
        while True:
            if proc.poll() is not None:
                fail('GATEWAY_FAILED_TO_START')
            try:
                st = gateway_dir.stat('gateway.sock')
                if not stat.S_ISSOCK(st.st_mode) or st.st_uid != os.geteuid():
                    fail('GATEWAY_SOCKET_INVALID')
                break
            except FileNotFoundError:
                if time.monotonic() >= deadline:
                    fail('GATEWAY_START_TIMEOUT')
                time.sleep(0.05)
        # Verify process before granting the socket mount to Docker.
        with Gateway(record['gateway'], sock) as handle:
            if handle.status != 'LIVE_VERIFIED':
                fail('GATEWAY_FAILED_TO_START')
        create_container(cfg, record, journal, workspace, gateway_dir)
        verify_survival(proc, record, sock)
        record['phase'] = 'active'
        write_record(journal, record)
        verify_survival(proc, record, sock)
    except BaseException as original:
        if record['start_attempted']:
            # A task may already be writing workspace data. Never auto-destroy it
            # merely because acknowledgement or metadata persistence failed.
            raise RuntimeError('CREATE_INCOMPLETE_REVIEW_REQUIRED') from original
        try:
            if proc is not None:
                if proc.poll() is None:
                    proc.terminate()  # Direct unreaped child, never an adopted PID.
                proc.wait(timeout=GATEWAY_TIMEOUT)
            record['gateway_exit_confirmed'] = True
            record['phase'] = 'stopping'
            write_record(journal, record)
            cleanup(cfg, session_id, roots)
        except BaseException as rollback:
            raise RuntimeError('CREATE_ROLLBACK_INCOMPLETE') from rollback
        raise
    finally:
        journal.close()
        if gateway_dir is not None:
            gateway_dir.close()


def recovery_plan(cfg, session_id, roots):
    state, runtime = roots
    name = session_name(cfg, session_id)
    plan = dict(session_id=session_id, status='quarantine', bridge_liveness='unknown',
                issues=[], gateway='unknown', container='unknown')
    with ExitStack() as stack:
        try:
            journal = optional_child(state, session_id)
            if journal is not None:
                stack.enter_context(journal)
            gateway_dir = optional_child(runtime, session_id)
            if gateway_dir is not None:
                stack.enter_context(gateway_dir)
            if journal is None:
                info = inspect_managed_container(name, session_id)
                plan['container'] = 'absent' if info is None else info['Id']
                if info is None and gateway_dir is None:
                    plan['status'] = 'absent'
                else:
                    plan['issues'].append('SESSION_METADATA_MISSING')
                return plan
            for entry in contents(journal):
                plan['issues'].append('UNEXPECTED_STATE_FILE:' + entry)
            for entry in contents(gateway_dir, runtime=True):
                plan['issues'].append('UNEXPECTED_RUNTIME_FILE:' + entry)
            record = read_record(journal, session_id, name, cfg['deployment_id'])
            plan.update(phase=record['phase'], docker_uncertain=record['docker_uncertain'],
                        start_attempted=record['start_attempted'])
            try:
                info = _inspect(record)
                plan['container'] = 'absent' if info is None else dict(id=info['Id'], state=info['State'])
                if info is None and record['docker_uncertain']:
                    plan['issues'].append('DOCKER_CREATE_OUTCOME_UNCERTAIN')
            except (RuntimeError, OSError, ValueError, subprocess.SubprocessError) as exc:
                plan['issues'].append('CONTAINER_UNVERIFIABLE:' + type(exc).__name__)
            if record['gateway_exit_confirmed']:
                plan['gateway'] = 'exit-confirmed-in-journal'
            elif record['gateway'] is None:
                plan['issues'].append('GATEWAY_SPAWN_OUTCOME_UNCERTAIN')
            else:
                try:
                    with Gateway(record['gateway'], GATEWAY_ROOT / session_id / 'gateway.sock') as handle:
                        plan['gateway'] = handle.status
                except (RuntimeError, OSError, ValueError) as exc:
                    plan['issues'].append('GATEWAY_UNVERIFIABLE:' + str(exc))
            if plan['gateway'] == 'LIVE_VERIFIED':
                if gateway_dir is None:
                    plan['issues'].append('RUNTIME_DIRECTORY_MISSING')
                else:
                    try:
                        socket_st = gateway_dir.stat('gateway.sock')
                        if not stat.S_ISSOCK(socket_st.st_mode) or socket_st.st_uid != os.geteuid():
                            plan['issues'].append('GATEWAY_SOCKET_INVALID')
                    except FileNotFoundError:
                        plan['issues'].append('GATEWAY_SOCKET_MISSING')
            if record['phase'] == 'active':
                container = plan['container']
                healthy_container = (type(container) is dict and container['state']['Running']
                                     and not container['state']['Paused'] and not container['state']['Restarting']
                                     and container['state']['Status'] == 'running')
                if not healthy_container or plan['gateway'] != 'LIVE_VERIFIED':
                    plan['issues'].append('RECORDED_ACTIVE_RESOURCES_NOT_LIVE')
            if not plan['issues']:
                plan['status'] = 'operator-review'
            return plan
        except (RuntimeError, OSError, ValueError, subprocess.SubprocessError) as exc:
            plan['issues'].append('METADATA_OR_PATH_UNVERIFIABLE:' + type(exc).__name__)
            return plan


def main():
    if len(sys.argv) != 3 or sys.argv[1] not in ('create', 'destroy', 'recover'):
        raise SystemExit('Uso: sessions.py create|destroy|recover SESSION_ID')
    action, session_id = sys.argv[1:]
    if not SESSION_RE.fullmatch(session_id):
        raise SystemExit('SESSION_ID_INVALID')
    with Directory.open(CONFIG.parent) as directory:
        cfg = read_private_json(directory, CONFIG.name)
    validate_config(cfg)
    with lifecycle_lock(read_only=action == 'recover') as roots:
        validate_layout(cfg, roots)
        if action == 'recover':
            result = recovery_plan(cfg, session_id, roots)
        elif action == 'create':
            create(cfg, session_id, roots)
        else:
            cleanup(cfg, session_id, roots)
    # No potentially blocking output while holding the lifecycle lock.
    if action == 'recover':
        print(json.dumps(result, sort_keys=True))
    else:
        print(f'SESSION_{"CREATED" if action == "create" else "DESTROYED"}:{session_name(cfg, session_id)}')


if __name__ == '__main__':
    main()
