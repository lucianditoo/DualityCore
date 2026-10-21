"""Isolated regression suite: Docker, gateways and all process signals are mocked.

Filesystem and flock checks operate only inside TemporaryDirectory. Distinct-user
permissions, real pidfds, Docker and systemd semantics require disposable lab tests.
"""
import copy
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
from contextlib import redirect_stdout
import select
import signal
import stat
import subprocess
import tempfile
import threading
import time
import types
import unittest
from unittest.mock import Mock, patch

spec = importlib.util.spec_from_file_location('sessions', Path(__file__).resolve().parents[1] / 'executor/sessions.py')
s = importlib.util.module_from_spec(spec)
spec.loader.exec_module(s)
RealGateway = s.Gateway
RealCurrentBoot = s.current_boot_id
BOOT = '12345678-1234-4234-9234-123456789abc'
OTHER_BOOT = '22345678-1234-4234-9234-123456789abc'
IDENTITY = dict(pid=123, boot_id=BOOT, process_start_ticks=456)
CID = 'a' * 64


class FakeDocker:
    def __init__(self):
        self.db, self.calls = {}, []
        self.on_create = self.on_start = self.on_kill = self.on_rm = None

    def add(self, name, session_id='test', instance='b'*32, running=False, cid=CID):
        self.db[cid] = dict(Id=cid, Name='/' + name,
            Config=dict(Labels={'org.duality-core.managed': 'true',
                'org.duality-core.component': 'agent-session',
                'org.duality-core.lifecycle-schema': '3',
                'org.duality-core.session-id': session_id,
                'org.duality-core.instance': instance}),
            State=dict(Running=running, Paused=False, Restarting=False,
                       Status='running' if running else 'created'))
        return self.db[cid]

    def __call__(self, *args):
        self.calls.append(args)
        if args[:2] == ('container', 'ls'):
            return '\n'.join(json.dumps(dict(Names=v['Name'][1:], ID=k)) for k, v in self.db.items())
        if args[:2] == ('container', 'inspect'):
            return json.dumps([self.db[args[2]]])
        if args[0] == 'create':
            labels = dict(args[i+1].split('=', 1) for i, value in enumerate(args) if value == '--label')
            self.add(args[args.index('--name')+1], labels['org.duality-core.session-id'], labels['org.duality-core.instance'])
            if self.on_create:
                return self.on_create(args)
            return CID
        if args[0] == 'start':
            self.db[args[1]]['State'].update(Running=True, Status='running')
            if self.on_start:
                return self.on_start(args)
            return args[1]
        if args[0] == 'kill':
            if self.on_kill:
                return self.on_kill(args)
            self.db[args[-1]]['State'].update(Running=False, Status='exited')
            return args[-1]
        if args[0] == 'wait':
            if self.db[args[-1]]['State']['Running']:
                raise subprocess.TimeoutExpired('fake Docker wait', s.DOCKER_TIMEOUT)
            return '0'
        if args[0] == 'rm':
            if self.db[args[-1]]['State']['Running']:
                raise subprocess.CalledProcessError(1, 'fake rm: running')
            del self.db[args[-1]]
            if self.on_rm:
                return self.on_rm(args)
            return args[-1]
        raise AssertionError('Unsupported fake Docker call: ' + repr(args))


class Isolated(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        # This sandbox maps host root to UID 65534. Normalize ONLY the existing
        # / and /tmp inode ownership for the test environment; production keeps
        # its root/effective-UID policy. Never normalize test-created ancestors.
        ancestor_keys = {(os.stat(p).st_dev, os.stat(p).st_ino) for p in ('/', '/tmp')}
        check = s._check_directory
        def sandbox_ancestor_check(st, leaf=False):
            if (st.st_dev, st.st_ino) in ancestor_keys and st.st_uid == 65534:
                st = types.SimpleNamespace(st_uid=0, st_mode=st.st_mode)
            return check(st, leaf=leaf)
        self.enterContext(patch.object(s, '_check_directory', side_effect=sandbox_ancestor_check))
        self.state, self.runtime, self.workspaces = [self.root / p for p in ('state', 'runtime', 'workspaces')]
        self.legacy_workspaces = self.root / 'legacy-workspaces'
        for p in (self.state, self.runtime, self.workspaces, self.legacy_workspaces):
            p.mkdir(mode=0o700)
        self.enterContext(patch.object(s, 'STATE_ROOT', self.state))
        self.enterContext(patch.object(s, 'GATEWAY_ROOT', self.runtime))
        for obj, name in ((s.subprocess, 'run'), (s.subprocess, 'Popen'), (s.os, 'pidfd_open'),
                          (s.signal, 'pidfd_send_signal'), (s.os, 'kill')):
            self.enterContext(patch.object(obj, name, side_effect=AssertionError('REAL INFRASTRUCTURE BLOCKED')))
        self.enterContext(patch.object(s, 'current_boot_id', return_value=BOOT))
        self.cfg = dict(lifecycle_schema=3, deployment_id='d'*32, legacy_executor_fenced=True,
                        legacy_container_prefix='duality', legacy_workspace_root=str(self.root / 'legacy-workspaces'),
                        container_prefix='duality',
                        workspace_root=str(self.workspaces), agent_uid=os.geteuid(),
                        agent_gid=os.getegid(), pids_limit=32, memory='64m', cpus=1, image='test-image')
        # Core filesystem tests run as one unprivileged user. The CLI rejects
        # equal operator/agent UIDs; different-user chown is a lab requirement.
        self.name = s.session_name(self.cfg, 'test')
        self.fake = FakeDocker()
        self.enterContext(patch.object(s, 'docker', side_effect=self.fake))
        self.lock_context = s.lifecycle_lock()
        self.roots = self.enterContext(self.lock_context)
        self.proc = Mock(pid=123)
        self.proc.poll.return_value = None
        self.proc.terminate.side_effect = lambda: setattr(self.proc.poll, 'return_value', -15)
        self.proc.wait.return_value = -15
        self.gateway_status = 'LIVE_VERIFIED'
        self.gateway_error = self.gateway_termination_error = None
        self.events = []
        outer = self
        class FakeGateway:
            def __init__(self, identity, sock):
                s.validate_identity(identity)
                self.status = outer.gateway_status
            def __enter__(self):
                outer.events.append('gateway-verified')
                if outer.gateway_error:
                    raise outer.gateway_error
                return self
            def terminate(self):
                outer.events.append('gateway-terminated')
                if outer.gateway_termination_error:
                    raise outer.gateway_termination_error
                self.status = 'EXITED'
                return self.status
            def __exit__(self, *args):
                pass
        self.enterContext(patch.object(s, 'Gateway', FakeGateway))

    def record(self, **changes):
        record = dict(schema_version=3, deployment_id=self.cfg['deployment_id'], session_id='test',
                      name=self.name, instance='b'*32, phase='prepared', gateway=None,
                      gateway_exit_confirmed=True, container_id=None, docker_uncertain=False,
                      start_attempted=False)
        record.update(changes)
        return record

    def live_record(self, **changes):
        record = self.record(phase='active', gateway=copy.deepcopy(IDENTITY), gateway_exit_confirmed=False,
                             container_id=CID, start_attempted=True)
        record.update(changes)
        return record

    def journal(self, record=None, runtime=True):
        with self.roots[0].mkdir('test') as journal:
            s.write_record(journal, self.record() if record is None else record)
        if runtime:
            self.roots[1].mkdir('test').close()

    def read(self):
        with self.roots[0].child('test') as directory:
            return s.read_record(directory, 'test', self.name, self.cfg['deployment_id'])

    def creation(self):
        self.gate_readers = []
        def popen(*args, **kwargs):
            # Simulate the child's inherited descriptor, without spawning.
            reader = os.dup(kwargs['pass_fds'][0])
            self.gate_readers.append(reader)
            self.addCleanup(os.close, reader)
            return self.proc
        self.enterContext(patch.object(s.subprocess, 'Popen', side_effect=popen))
        self.enterContext(patch.object(s, 'read_process_identity', return_value=copy.deepcopy(IDENTITY)))
        original = s.Directory.stat
        def mock_socket(directory, name):
            if name == 'gateway.sock' and directory.path.parent == self.runtime and not (directory.path / name).exists():
                return types.SimpleNamespace(st_mode=stat.S_IFSOCK | 0o600, st_uid=os.geteuid())
            return original(directory, name)
        self.enterContext(patch.object(s.Directory, 'stat', mock_socket))

    def mutate_envelope(self, update, checksum=True):
        path = self.state / 'test/session.json'
        data = json.loads(path.read_text())
        update(data['record'])
        if checksum:
            data['sha256'] = hashlib.sha256(s.canonical(data['record'])).hexdigest()
        path.write_text(json.dumps(data))


class StrictMetadata(Isolated):
    def test_all_fields_required(self):
        for field in s.FIELDS:
            with self.subTest(field=field):
                record = self.record()
                del record[field]
                with self.assertRaises(RuntimeError):
                    s.validate_record(record, 'test', self.name, self.cfg['deployment_id'])

    def test_strict_types(self):
        for key, value in [('schema_version', 3.0), ('schema_version', True),
                           ('container_id', int('1'*64)), ('docker_uncertain', 0),
                           ('gateway_exit_confirmed', 1), ('start_attempted', 'false'),
                           ('phase', []), ('instance', None), ('deployment_id', 123),
                           ('session_id', '../test'), ('name', None)]:
            with self.subTest(key=key, value=value), self.assertRaises(RuntimeError):
                s.validate_record(self.record(**{key: value}), 'test', self.name, self.cfg['deployment_id'])

    def test_boot_uuid_validation(self):
        for value in ('-'*36, '0'*36, '00000000-0000-0000-0000-000000000000', BOOT.upper(),
                      BOOT.replace('-', ''), None, 123, ''):
            with self.subTest(value=value), self.assertRaises(RuntimeError):
                s.validate_identity(dict(IDENTITY, boot_id=value))
        s.validate_identity(IDENTITY)

    def test_identity_integer_validation(self):
        for changes in (dict(pid=True), dict(pid=0), dict(pid=2**31), dict(process_start_ticks=True),
                        dict(process_start_ticks=0), dict(process_start_ticks=2**64), dict(extra=1)):
            with self.subTest(changes=changes), self.assertRaises(RuntimeError):
                s.validate_identity(dict(IDENTITY, **changes))

    def test_phase_invariants(self):
        for changes in (dict(phase='active'), dict(phase='spawning'), dict(phase='gateway'),
                        dict(phase='container'), dict(start_attempted=True), dict(docker_uncertain=True),
                        dict(phase='closed', gateway_exit_confirmed=False)):
            with self.subTest(changes=changes), self.assertRaises(RuntimeError):
                s.validate_record(self.record(**changes), 'test', self.name, self.cfg['deployment_id'])

    def test_record_roundtrip(self):
        self.journal()
        self.assertEqual(self.read(), self.record())
        self.assertEqual((self.state / 'test/session.json').stat().st_mode & 0o777, 0o600)

    def test_checksum_rejects_corruption(self):
        self.journal()
        self.mutate_envelope(lambda r: r.update(instance='c'*32), checksum=False)
        with self.assertRaisesRegex(RuntimeError, 'INTEGRITY'):
            self.read()

    def test_missing_uncertain_cannot_delete_evidence(self):
        self.journal(self.record(phase='stopping', docker_uncertain=True))
        self.mutate_envelope(lambda r: r.pop('docker_uncertain'))
        with self.assertRaisesRegex(RuntimeError, 'RECORD_INVALID'):
            s.cleanup(self.cfg, 'test', self.roots)
        self.assertTrue((self.state / 'test/session.json').exists())
        self.assertEqual(self.fake.calls, [])

    def test_malformed_boot_cannot_delete_evidence(self):
        self.journal(self.live_record())
        self.mutate_envelope(lambda r: r['gateway'].update(boot_id='-'*36))
        with self.assertRaisesRegex(RuntimeError, 'BOOT_ID'):
            s.cleanup(self.cfg, 'test', self.roots)
        self.assertTrue((self.state / 'test/session.json').exists())
        self.assertEqual(self.fake.calls, [])

    def test_duplicate_and_nonfinite_json_rejected(self):
        for value in ('{"x":1,"x":2}', '{"x":NaN}', '{"x":Infinity}'):
            with self.subTest(value=value), self.assertRaises(RuntimeError):
                s._decode(value)

    def test_private_metadata_rejections(self):
        self.journal()
        path = self.state / 'test/session.json'
        for content in ('bad', '[]', '{}'):
            path.write_text(content)
            with self.subTest(content=content), self.assertRaises((RuntimeError, ValueError)):
                self.read()
        path.chmod(0o644)
        with self.assertRaisesRegex(RuntimeError, 'UNTRUSTED_METADATA'):
            self.read()

    def test_metadata_hardlink(self):
        self.journal()
        os.link(self.state / 'test/session.json', self.root / 'copy')
        with self.assertRaisesRegex(RuntimeError, 'UNTRUSTED_METADATA'):
            self.read()

    def test_metadata_size_bound(self):
        self.journal()
        (self.state / 'test/session.json').write_text(' '*(s.MAX_RECORD+1))
        with self.assertRaisesRegex(RuntimeError, 'UNTRUSTED_METADATA'):
            self.read()

    def test_replace_failure_preserves_previous_record(self):
        self.journal()
        with self.roots[0].child('test') as directory, patch.object(s.os, 'replace', side_effect=OSError('disk')):
            with self.assertRaises(OSError):
                s.write_record(directory, self.record(phase='stopping'))
        self.assertEqual(self.read()['phase'], 'prepared')
        self.assertEqual(sorted(p.name for p in (self.state / 'test').iterdir()), ['session.json'])

    def test_fsync_failure_after_replace_keeps_parseable_evidence(self):
        self.journal()
        real_sync = s.os.fsync
        with self.roots[0].child('test') as directory:
            def fsync(fd):
                if fd == directory.fd:
                    raise OSError('directory durability uncertain')
                return real_sync(fd)
            with patch.object(s.os, 'fsync', side_effect=fsync), self.assertRaises(OSError):
                s.write_record(directory, self.record(phase='stopping'))
        self.assertEqual(self.read()['phase'], 'stopping')


class Paths(Isolated):
    def test_symlink_leaf_rejected(self):
        alias = self.root / 'alias'
        alias.symlink_to(self.state)
        with self.assertRaises(OSError):
            s.Directory.open(alias)

    def test_symlink_ancestor_rejected(self):
        alias = self.root / 'alias'
        alias.symlink_to(self.root)
        with self.assertRaises(OSError):
            s.Directory.open(alias / 'state')

    def test_writable_ancestor_rejected(self):
        parent = self.root / 'writable'
        parent.mkdir(mode=0o777)
        parent.chmod(0o777)
        (parent / 'leaf').mkdir(mode=0o700)
        with self.assertRaisesRegex(RuntimeError, 'UNTRUSTED_DIRECTORY'):
            s.Directory.open(parent / 'leaf')

    def test_pinned_descriptor_survives_path_replacement(self):
        self.journal()
        with self.roots[0].child('test') as pinned:
            (self.state / 'test').rename(self.state / 'moved')
            (self.state / 'test').mkdir(mode=0o700)
            (self.state / 'test/session.json').write_text('unrelated')
            s.write_record(pinned, self.record(phase='stopping'))
        self.assertEqual((self.state / 'test/session.json').read_text(), 'unrelated')
        self.assertEqual(json.loads((self.state / 'moved/session.json').read_text())['record']['phase'], 'stopping')

    def test_metadata_symlink_rejected(self):
        self.journal()
        path = self.state / 'test/session.json'
        path.unlink()
        victim = self.root / 'victim'
        victim.write_text('keep')
        path.symlink_to(victim)
        with self.assertRaises(OSError):
            self.read()
        self.assertEqual(victim.read_text(), 'keep')

    def test_state_symlink_rejected(self):
        (self.state / 'test').symlink_to(self.workspaces)
        with self.assertRaises(OSError):
            s.cleanup(self.cfg, 'test', self.roots)

    def test_runtime_open_failure_closes_journal_descriptor(self):
        self.journal(runtime=False)
        (self.runtime / 'test').symlink_to(self.workspaces)
        original = s.Directory.child
        opened = []
        def capture(directory, name):
            child = original(directory, name)
            opened.append(child)
            return child
        with patch.object(s.Directory, 'child', capture), self.assertRaises(OSError):
            s.cleanup(self.cfg, 'test', self.roots)
        self.assertNotEqual(opened, [])
        self.assertTrue(all(d.fd is None for d in opened))

    def test_recovery_runtime_open_failure_closes_journal(self):
        self.journal(runtime=False)
        (self.runtime / 'test').symlink_to(self.workspaces)
        original = s.Directory.child
        opened = []
        def capture(directory, name):
            child = original(directory, name)
            opened.append(child)
            return child
        with patch.object(s.Directory, 'child', capture):
            plan = s.recovery_plan(self.cfg, 'test', self.roots)
        self.assertEqual(plan['status'], 'quarantine')
        self.assertNotEqual(opened, [])
        self.assertTrue(all(d.fd is None for d in opened))

    def test_workspace_symlink_rejected(self):
        (self.workspaces / 'test').symlink_to(self.root)
        with self.assertRaises(OSError):
            s.prepare_workspace(self.cfg, 'test')

    def test_workspace_mode_rejected(self):
        (self.workspaces / 'test').mkdir(mode=0o755)
        with self.assertRaisesRegex(RuntimeError, 'WORKSPACE'):
            s.prepare_workspace(self.cfg, 'test')

    def test_workspace_owner_checked(self):
        cfg = dict(self.cfg, agent_uid=os.geteuid()+1)
        (self.workspaces / 'test').mkdir(mode=0o700)
        with self.assertRaisesRegex(RuntimeError, 'WORKSPACE'):
            s.prepare_workspace(cfg, 'test')

    def test_workspace_chown_is_descriptor_relative(self):
        with patch.object(s.os, 'fchown', wraps=s.os.fchown) as change:
            s.prepare_workspace(self.cfg, 'test')
        self.assertIs(type(change.call_args.args[0]), int)

    def test_path_components_rejected(self):
        for name in ('../test', '.', '..', '', '/etc', 'bad\0name'):
            with self.subTest(name=name), self.assertRaises(RuntimeError):
                self.roots[0].unlink(name)

    def test_absolute_traversal_rejected(self):
        for path in ('relative/path', str(self.root) + '/../state', str(self.root) + '/comma,path'):
            with self.subTest(path=path), self.assertRaises(RuntimeError):
                s.Directory.open(path)


class DockerIdentity(Isolated):
    def test_verified_complete_id_and_labels(self):
        self.fake.add(self.name)
        info = s.inspect_managed_container(self.name, 'test', 'b'*32, CID)
        self.assertEqual(info['Id'], CID)
        self.assertEqual(self.fake.calls[-1], ('container', 'inspect', CID))

    def test_absent(self):
        self.assertIsNone(s.inspect_managed_container(self.name, 'test'))

    def test_label_mismatches(self):
        info = self.fake.add(self.name)
        original = copy.deepcopy(info['Config']['Labels'])
        for key in original:
            info['Config']['Labels'] = dict(original, **{key: 'wrong'})
            with self.subTest(key=key), self.assertRaisesRegex(RuntimeError, 'LABEL'):
                s.inspect_managed_container(self.name, 'test', 'b'*32, CID)

    def test_id_mismatch(self):
        self.fake.add(self.name)
        with self.assertRaisesRegex(RuntimeError, 'ID_MISMATCH'):
            s.inspect_managed_container(self.name, 'test', 'b'*32, 'c'*64)

    def test_renamed_id_is_not_absence(self):
        self.fake.add('renamed')
        with self.assertRaisesRegex(RuntimeError, 'NAME_MISMATCH'):
            s.inspect_managed_container(self.name, 'test', 'b'*32, CID)

    def test_ambiguous_listing(self):
        self.fake.add(self.name)
        self.fake.add('renamed', cid='c'*64)
        with self.assertRaisesRegex(RuntimeError, 'AMBIGUOUS'):
            s.inspect_managed_container(self.name, 'test', 'b'*32, 'c'*64)

    def test_invalid_listing_and_details(self):
        for output in ('bad', '[]', json.dumps(dict(Names=self.name, ID='short'))):
            with self.subTest(output=output), patch.object(s, 'docker', return_value=output), self.assertRaises((ValueError, RuntimeError)):
                s.inspect_managed_container(self.name, 'test')
        listing = json.dumps(dict(Names=self.name, ID=CID))
        for output in ('{}', '[]', '[1]', '[{},{}]'):
            with self.subTest(output=output), patch.object(s, 'docker', side_effect=[listing, output]), self.assertRaises(RuntimeError):
                s.inspect_managed_container(self.name, 'test')

    def test_container_state_types(self):
        info = self.fake.add(self.name)
        info['State']['Running'] = 0
        with self.assertRaisesRegex(RuntimeError, 'STATE_INVALID'):
            s.inspect_managed_container(self.name, 'test')

    def test_malformed_config_is_fail_closed(self):
        info = self.fake.add(self.name)
        for config in (None, [], 'bad'):
            info['Config'] = config
            with self.subTest(config=config), self.assertRaisesRegex(RuntimeError, 'CONFIG_INVALID'):
                s.inspect_managed_container(self.name, 'test')

    def test_running_and_stopped_are_verified(self):
        info = self.fake.add(self.name)
        with self.assertRaisesRegex(RuntimeError, 'NOT_RUNNING'):
            s.inspect_managed_container(self.name, 'test', running=True)
        info['State'].update(Running=True, Status='running')
        with self.assertRaisesRegex(RuntimeError, 'NOT_STOPPED'):
            s.inspect_managed_container(self.name, 'test', running=False)

    def test_protected_container(self):
        with self.assertRaisesRegex(RuntimeError, 'PROTECTED'):
            s.inspect_managed_container('duality-sol-sandbox', 'sandbox')
        self.assertEqual(self.fake.calls, [])

    def test_docker_timeout_not_absence(self):
        with patch.object(s, 'docker', side_effect=subprocess.TimeoutExpired('fake', 15)), self.assertRaises(subprocess.TimeoutExpired):
            s.inspect_managed_container(self.name, 'test')

    def test_docker_wrapper_has_timeout(self):
        # Exercise the real wrapper only against a blocked-and-replaced run().
        spec2 = importlib.util.spec_from_file_location('wrapper_only', spec.origin)
        module = importlib.util.module_from_spec(spec2)
        spec2.loader.exec_module(module)
        with patch.object(s.subprocess, 'run', return_value=Mock(stdout=' output ')) as run:
            self.assertEqual(module.docker('version'), 'output')
            self.assertEqual(run.call_args.kwargs['timeout'], s.DOCKER_TIMEOUT)
            self.assertEqual(run.call_args.args[0], ['/usr/bin/docker', 'version'])


class GatewayIdentity(Isolated):
    def gateway(self, events=None, actual=None, command=None, boot=BOOT):
        self.enterContext(patch.object(s, 'current_boot_id', return_value=boot))
        self.enterContext(patch.object(s.os, 'pidfd_open', return_value=999))
        real_close = os.close
        self.close = self.enterContext(patch.object(s.os, 'close', side_effect=lambda fd: None if fd == 999 else real_close(fd)))
        self.send = self.enterContext(patch.object(s.signal, 'pidfd_send_signal'))
        self.enterContext(patch.object(s, 'read_process_identity', return_value=IDENTITY if actual is None else actual))
        self.enterContext(patch.object(Path, 'read_bytes', return_value=command if command is not None else
                                     b'\0'.join(x.encode() for x in s.gateway_command('/socket')) + b'\0'))
        self.poll = Mock()
        self.poll.poll.side_effect = events if events is not None else [[], [], [(999, select.POLLIN)]]
        self.enterContext(patch.object(s.select, 'poll', return_value=self.poll))
        return RealGateway(IDENTITY, Path('/socket'))

    def test_verified_signal_and_wait(self):
        with self.gateway() as gateway:
            self.assertEqual(gateway.status, 'LIVE_VERIFIED')
            self.assertEqual(gateway.terminate(), 'EXITED')
        self.send.assert_called_once_with(999, signal.SIGTERM)
        self.assertIn(unittest.mock.call(999), self.close.call_args_list)

    def test_pid_reuse_no_signal(self):
        with self.assertRaisesRegex(RuntimeError, 'IDENTITY_MISMATCH'):
            with self.gateway(actual=dict(IDENTITY, process_start_ticks=999)):
                self.fail('adopted reused PID')
        self.send.assert_not_called()

    def test_boot_change_does_not_open_pidfd(self):
        with self.gateway(boot=OTHER_BOOT) as gateway:
            self.assertEqual(gateway.terminate(), 'EXITED_PREVIOUS_BOOT')
        s.os.pidfd_open.assert_not_called()
        self.send.assert_not_called()

    def test_bad_current_boot_blocks_operation(self):
        for raw in ('-'*36, '', '0'*36):
            with self.subTest(raw=raw), patch.object(Path, 'read_text', return_value=raw):
                with self.assertRaisesRegex(RuntimeError, 'BOOT_ID_INVALID'):
                    RealCurrentBoot()

    def test_dead_pidfd_before_cmdline(self):
        with self.gateway(events=[[(999, select.POLLIN)]], command=b'') as gateway:
            self.assertEqual(gateway.terminate(), 'EXITED')
        self.send.assert_not_called()

    def test_exit_during_empty_cmdline_is_accepted(self):
        with self.gateway(events=[[], [(999, select.POLLIN)]], command=b'') as gateway:
            self.assertEqual(gateway.status, 'EXITED')
        self.send.assert_not_called()

    def test_live_empty_cmdline_rejected(self):
        with self.assertRaisesRegex(RuntimeError, 'COMMAND_MISMATCH'):
            with self.gateway(events=[[], []], command=b''):
                pass
        self.send.assert_not_called()

    def test_full_command_must_match(self):
        command = b'\0'.join(x.encode() for x in s.gateway_command('/socket') + ['--extra']) + b'\0'
        with self.assertRaisesRegex(RuntimeError, 'COMMAND_MISMATCH'):
            with self.gateway(events=[[], []], command=command):
                pass

    def test_timeout_retains_no_false_confirmation(self):
        with self.gateway(events=[[], [], []]) as gateway:
            with self.assertRaisesRegex(RuntimeError, 'TIMEOUT'):
                gateway.terminate()
            self.assertEqual(gateway.status, 'LIVE_VERIFIED')

    def test_poll_errors_are_not_exit(self):
        for flag in (select.POLLERR, select.POLLNVAL, select.POLLERR | select.POLLIN):
            with self.subTest(flag=flag), self.assertRaisesRegex(RuntimeError, 'POLL_ERROR'):
                with self.gateway(events=[[(999, flag)]]):
                    pass

    def test_no_pidfd_fallback(self):
        with patch.object(s.os, 'pidfd_open', side_effect=OSError('unsupported')), self.assertRaises(OSError):
            with RealGateway(IDENTITY, Path('/socket')):
                pass

    def test_missing_process(self):
        with patch.object(s.os, 'pidfd_open', side_effect=ProcessLookupError):
            with RealGateway(IDENTITY, Path('/socket')) as gateway:
                self.assertEqual(gateway.terminate(), 'EXITED')

    def test_missing_proc_requires_exit_evidence(self):
        context = self.gateway(events=[[], []])
        with patch.object(s, 'read_process_identity', side_effect=FileNotFoundError), self.assertRaisesRegex(RuntimeError, 'UNVERIFIABLE'):
            with context:
                pass

    def test_poll_error_during_missing_proc_closes_pidfd(self):
        context = self.gateway(events=[[], [(999, select.POLLERR)]])
        with patch.object(s, 'read_process_identity', side_effect=FileNotFoundError), self.assertRaisesRegex(RuntimeError, 'POLL_ERROR'):
            with context:
                pass
        self.assertIsNone(context.fd)
        self.assertIn(unittest.mock.call(999), self.close.call_args_list)

    def test_signal_esrch_confirms_exit(self):
        with self.gateway() as gateway, patch.object(s.signal, 'pidfd_send_signal', side_effect=ProcessLookupError):
            self.assertEqual(gateway.terminate(), 'EXITED')

    def test_stat_parentheses_and_pid(self):
        raw = '123 (name (strange)) ' + ' '.join(['S'] + ['0']*18 + ['987'])
        with patch.object(Path, 'read_text', return_value=raw):
            self.assertEqual(s.read_process_identity(123)['process_start_ticks'], 987)
        for raw in ('bad', '321 (name) ' + ' '.join(['0']*20), '123 (name) S 0'):
            with self.subTest(raw=raw), patch.object(Path, 'read_text', return_value=raw), self.assertRaises(RuntimeError):
                s.read_process_identity(123)


class Creation(Isolated):
    def test_success_and_dynamic_uid_tmpfs(self):
        self.creation()
        s.create(self.cfg, 'test', self.roots)
        self.assertEqual(self.read()['phase'], 'active')
        self.assertEqual(self.read()['gateway'], IDENTITY)
        create_call = next(c for c in self.fake.calls if c[0] == 'create')
        self.assertIn(f'/home/agent/.codex:rw,nosuid,nodev,uid={os.geteuid()},gid={os.getegid()},size=64m', create_call)
        running_inspections = [c for c in self.fake.calls if c[:2] == ('container', 'inspect')]
        self.assertGreaterEqual(len(running_inspections), 3)

    def test_gateway_death_during_start_never_acknowledged(self):
        self.creation()
        self.fake.on_start = lambda args: setattr(self.proc.poll, 'return_value', 1)
        with self.assertRaisesRegex(RuntimeError, 'REVIEW_REQUIRED'):
            s.create(self.cfg, 'test', self.roots)
        self.assertNotEqual(self.read()['phase'], 'active')
        self.assertIn(CID, self.fake.db)
        self.assertFalse(any(c[0] == 'rm' for c in self.fake.calls))

    def test_container_dies_during_start(self):
        self.creation()
        def died(args):
            self.fake.db[CID]['State'].update(Running=False, Status='exited')
        self.fake.on_start = died
        with self.assertRaisesRegex(RuntimeError, 'REVIEW_REQUIRED'):
            s.create(self.cfg, 'test', self.roots)
        self.assertEqual(self.read()['phase'], 'starting')
        self.proc.terminate.assert_not_called()

    def test_failure_after_start_keeps_workload_and_gateway(self):
        self.creation()
        original = s.write_record
        def fail_active(directory, record):
            if record['phase'] == 'active':
                raise OSError('persistence failed after workload start')
            return original(directory, record)
        with patch.object(s, 'write_record', side_effect=fail_active), self.assertRaisesRegex(RuntimeError, 'REVIEW_REQUIRED'):
            s.create(self.cfg, 'test', self.roots)
        self.assertTrue(self.fake.db[CID]['State']['Running'])
        self.proc.terminate.assert_not_called()
        self.assertFalse(any(c[0] in ('kill', 'rm') for c in self.fake.calls))

    def test_start_timeout_keeps_resources(self):
        self.creation()
        def timeout(args):
            raise subprocess.TimeoutExpired('fake start', 15)
        self.fake.on_start = timeout
        with self.assertRaisesRegex(RuntimeError, 'REVIEW_REQUIRED'):
            s.create(self.cfg, 'test', self.roots)
        self.assertTrue(self.read()['start_attempted'])
        self.assertTrue(self.fake.db[CID]['State']['Running'])

    def test_partial_create_visible_is_reconciled(self):
        self.creation()
        def timeout(args):
            raise subprocess.TimeoutExpired('fake create', 15)
        self.fake.on_create = timeout
        with self.assertRaises(subprocess.TimeoutExpired):
            s.create(self.cfg, 'test', self.roots)
        self.assertNotIn(CID, self.fake.db)
        self.assertFalse((self.state / 'test').exists())
        self.assertTrue((self.workspaces / 'test').exists())
        self.assertIn(('rm', CID), self.fake.calls)

    def test_uncertain_invisible_create_preserves_journal(self):
        self.creation()
        def timeout(args):
            self.fake.db.clear()
            raise subprocess.TimeoutExpired('fake create', 15)
        self.fake.on_create = timeout
        with self.assertRaisesRegex(RuntimeError, 'ROLLBACK_INCOMPLETE'):
            s.create(self.cfg, 'test', self.roots)
        record = self.read()
        self.assertTrue(record['docker_uncertain'])
        self.assertTrue(record['gateway_exit_confirmed'])
        self.fake.add(self.name, instance=record['instance'])
        s.cleanup(self.cfg, 'test', self.roots)
        self.assertFalse((self.state / 'test').exists())

    def test_invalid_create_output_reconciles_owned_id(self):
        self.creation()
        self.fake.on_create = lambda args: 'not-an-id'
        with self.assertRaisesRegex(RuntimeError, 'RESPONSE_INVALID'):
            s.create(self.cfg, 'test', self.roots)
        self.assertNotIn(CID, self.fake.db)

    def test_popen_failure_cleans_without_pid(self):
        with patch.object(s.subprocess, 'Popen', side_effect=OSError('spawn failed')):
            with self.assertRaisesRegex(OSError, 'spawn failed'):
                s.create(self.cfg, 'test', self.roots)
        self.assertFalse((self.state / 'test').exists())

    def test_identity_failure_and_unconfirmed_exit_quarantines(self):
        self.creation()
        self.proc.wait.side_effect = subprocess.TimeoutExpired('fake gateway', 5)
        with patch.object(s, 'read_process_identity', side_effect=PermissionError), self.assertRaisesRegex(RuntimeError, 'ROLLBACK_INCOMPLETE'):
            s.create(self.cfg, 'test', self.roots)
        self.assertEqual(self.read()['phase'], 'spawning')
        self.assertFalse(self.read()['gateway_exit_confirmed'])

    def test_permission_failure_before_spawn_cleans_journal(self):
        # Reuse workspace to inject only the runtime ownership failure.
        s.prepare_workspace(self.cfg, 'test')
        with patch.object(s.os, 'fchown', side_effect=PermissionError('runtime owner')):
            with self.assertRaises(PermissionError):
                s.create(self.cfg, 'test', self.roots)
        self.assertFalse((self.state / 'test').exists())
        self.assertFalse((self.runtime / 'test').exists())

    def test_gateway_release_follows_durable_identity(self):
        self.creation()
        original = os.write
        observed = []
        def release(fd, data):
            observed.append(self.read())
            return original(fd, data)
        with patch.object(s.os, 'write', side_effect=release):
            s.create(self.cfg, 'test', self.roots)
        self.assertEqual(len(observed), 1)
        self.assertEqual(observed[0]['gateway'], IDENTITY)
        self.assertEqual(observed[0]['phase'], 'gateway')
        self.assertEqual(os.read(self.gate_readers[0], 1), b'1')

    def test_no_release_if_identity_persistence_fails(self):
        self.creation()
        original = s.write_record
        def persist(directory, record):
            if record['phase'] == 'gateway':
                raise OSError('identity durability failed')
            return original(directory, record)
        with patch.object(s, 'write_record', side_effect=persist), patch.object(s.os, 'write') as release:
            with self.assertRaises(OSError):
                s.create(self.cfg, 'test', self.roots)
        release.assert_not_called()
        self.assertEqual(os.read(self.gate_readers[0], 1), b'')

    def test_existing_container_preflight(self):
        self.fake.add(self.name)
        with self.assertRaisesRegex(RuntimeError, 'CONTAINER_SESSION_EXISTS'):
            s.create(self.cfg, 'test', self.roots)
        self.assertFalse((self.workspaces / 'test').exists())

    def test_existing_session_not_overwritten(self):
        self.journal()
        with self.assertRaisesRegex(RuntimeError, 'SESSION_EXISTS'):
            s.create(self.cfg, 'test', self.roots)
        self.assertEqual(self.read(), self.record())

    def test_gateway_socket_must_not_be_symlink(self):
        self.creation()
        original = s.Directory.stat
        def bad_socket(directory, name):
            if name == 'gateway.sock':
                return types.SimpleNamespace(st_mode=stat.S_IFLNK, st_uid=os.geteuid())
            return original(directory, name)
        with patch.object(s.Directory, 'stat', bad_socket), self.assertRaisesRegex(RuntimeError, 'SOCKET_INVALID'):
            s.create(self.cfg, 'test', self.roots)

    def test_readiness_timeout_is_bounded(self):
        self.creation()
        original = s.Directory.stat
        def no_socket(directory, name):
            if name == 'gateway.sock':
                raise FileNotFoundError
            return original(directory, name)
        with patch.object(s.Directory, 'stat', no_socket), patch.object(s, 'GATEWAY_TIMEOUT', 0), self.assertRaisesRegex(RuntimeError, 'START_TIMEOUT'):
            s.create(self.cfg, 'test', self.roots)


class Destruction(Isolated):
    def active(self, **changes):
        record = self.live_record(**changes)
        self.journal(record)
        self.fake.add(self.name, instance=record['instance'], running=True)

    def test_graceful_order_no_force_and_preserved_workspace(self):
        self.active()
        project = self.workspaces / 'test'
        project.mkdir()
        (project / 'data').write_text('keep')
        self.fake.on_kill = lambda args: (self.events.append('container-term'), self.fake.db[CID]['State'].update(Running=False, Status='exited'))[-1] or CID
        s.cleanup(self.cfg, 'test', self.roots)
        self.assertLess(self.events.index('gateway-verified'), self.events.index('container-term'))
        self.assertLess(self.events.index('container-term'), self.events.index('gateway-terminated'))
        self.assertIn(('kill', '--signal=TERM', CID), self.fake.calls)
        self.assertIn(('wait', CID), self.fake.calls)
        self.assertIn(('rm', CID), self.fake.calls)
        self.assertFalse(any('-f' in c or '-v' in c or 'KILL' in c for c in self.fake.calls))
        self.assertEqual((project / 'data').read_text(), 'keep')
        s.cleanup(self.cfg, 'test', self.roots)

    def test_gateway_mismatch_prevents_all_mutations(self):
        self.active()
        self.gateway_error = RuntimeError('GATEWAY_PROCESS_IDENTITY_MISMATCH')
        with self.assertRaisesRegex(RuntimeError, 'IDENTITY_MISMATCH'):
            s.cleanup(self.cfg, 'test', self.roots)
        self.assertEqual(self.fake.calls, [])
        self.assertTrue(self.fake.db[CID]['State']['Running'])

    def test_foreign_container_prevents_gateway_termination(self):
        self.active()
        self.fake.db[CID]['Config']['Labels']['org.duality-core.instance'] = 'foreign'
        with self.assertRaisesRegex(RuntimeError, 'LABEL'):
            s.cleanup(self.cfg, 'test', self.roots)
        self.assertNotIn('gateway-terminated', self.events)
        self.assertTrue((self.state / 'test').exists())

    def test_gateway_timeout_preserves_files_and_container(self):
        self.active()
        self.gateway_termination_error = RuntimeError('GATEWAY_TERMINATION_TIMEOUT')
        with self.assertRaisesRegex(RuntimeError, 'TIMEOUT'):
            s.cleanup(self.cfg, 'test', self.roots)
        self.assertIn(CID, self.fake.db)
        self.assertFalse(self.fake.db[CID]['State']['Running'])
        self.assertFalse(self.read()['gateway_exit_confirmed'])
        self.assertFalse(any(c[0] == 'rm' for c in self.fake.calls))

    def test_container_timeout_does_not_kill_gateway(self):
        self.active()
        self.fake.on_kill = lambda args: CID
        with self.assertRaises(subprocess.TimeoutExpired):
            s.cleanup(self.cfg, 'test', self.roots)
        self.assertNotIn('gateway-terminated', self.events)
        self.assertTrue((self.state / 'test/session.json').exists())
        self.assertFalse(any(c[0] == 'rm' for c in self.fake.calls))

    def test_uncertain_rm_result_is_retryable(self):
        self.active()
        def timeout(args):
            raise subprocess.TimeoutExpired('fake rm', 15)
        self.fake.on_rm = timeout
        with self.assertRaises(subprocess.TimeoutExpired):
            s.cleanup(self.cfg, 'test', self.roots)
        self.assertTrue(self.read()['gateway_exit_confirmed'])
        self.assertNotIn(CID, self.fake.db)
        s.cleanup(self.cfg, 'test', self.roots)
        self.assertFalse((self.state / 'test').exists())

    def test_runtime_missing_after_host_reboot(self):
        self.journal(self.live_record(), runtime=False)
        self.gateway_status = 'EXITED_PREVIOUS_BOOT'
        self.fake.add(self.name, running=False)
        s.cleanup(self.cfg, 'test', self.roots)
        self.assertFalse((self.state / 'test').exists())

    def test_missing_journal_never_removes_container(self):
        self.fake.add(self.name)
        with self.assertRaisesRegex(RuntimeError, 'METADATA_MISSING'):
            s.cleanup(self.cfg, 'test', self.roots)
        self.assertIn(CID, self.fake.db)
        self.assertFalse(any(c[0] == 'rm' for c in self.fake.calls))

    def test_unknown_files_preserved(self):
        self.journal()
        for parent in (self.state, self.runtime):
            path = parent / 'test/unknown'
            path.write_text('keep')
            with self.subTest(parent=parent), self.assertRaisesRegex(RuntimeError, 'UNEXPECTED'):
                s.cleanup(self.cfg, 'test', self.roots)
            self.assertEqual(path.read_text(), 'keep')
            path.unlink()

    def test_spawning_without_identity_preserved(self):
        self.journal(self.record(phase='spawning', gateway_exit_confirmed=False))
        with self.assertRaisesRegex(RuntimeError, 'SPAWN_OUTCOME_UNCERTAIN'):
            s.cleanup(self.cfg, 'test', self.roots)
        self.assertTrue((self.state / 'test').exists())

    def test_empty_journal_not_mistaken_for_exit(self):
        self.roots[0].mkdir('test').close()
        with self.assertRaises(FileNotFoundError):
            s.cleanup(self.cfg, 'test', self.roots)
        self.assertTrue((self.state / 'test').exists())

    def test_closed_record_is_retryable(self):
        self.journal(self.live_record(phase='closed', container_id=None, gateway_exit_confirmed=True))
        s.cleanup(self.cfg, 'test', self.roots)
        self.assertFalse((self.state / 'test').exists())


class Recovery(Isolated):
    def snapshot(self):
        return {str(p.relative_to(self.root)): p.read_bytes() for p in self.root.rglob('*') if p.is_file()}

    def test_read_only_full_diagnostics(self):
        self.creation()  # A mocked socket; no Unix socket is created.
        self.journal(self.live_record())
        self.fake.add(self.name, running=True)
        before = self.snapshot()
        plan = s.recovery_plan(self.cfg, 'test', self.roots)
        self.assertEqual(plan['status'], 'operator-review')
        self.assertEqual(plan['gateway'], 'LIVE_VERIFIED')
        self.assertEqual(plan['container']['id'], CID)
        self.assertEqual(before, self.snapshot())
        self.assertNotIn('gateway-terminated', self.events)
        self.assertFalse(any(c[0] in ('kill', 'rm', 'start', 'create') for c in self.fake.calls))

    def test_unknown_files_are_reported(self):
        self.journal()
        (self.state / 'test/extra').write_text('keep')
        (self.runtime / 'test/extra').write_text('keep')
        plan = s.recovery_plan(self.cfg, 'test', self.roots)
        self.assertEqual(plan['status'], 'quarantine')
        self.assertIn('UNEXPECTED_STATE_FILE:extra', plan['issues'])
        self.assertIn('UNEXPECTED_RUNTIME_FILE:extra', plan['issues'])

    def test_uncertain_create_is_reported(self):
        self.journal(self.record(phase='stopping', docker_uncertain=True))
        plan = s.recovery_plan(self.cfg, 'test', self.roots)
        self.assertIn('DOCKER_CREATE_OUTCOME_UNCERTAIN', plan['issues'])
        self.assertTrue(plan['docker_uncertain'])

    def test_spawn_gap_is_reported(self):
        self.journal(self.record(phase='spawning', gateway_exit_confirmed=False))
        self.assertIn('GATEWAY_SPAWN_OUTCOME_UNCERTAIN', s.recovery_plan(self.cfg, 'test', self.roots)['issues'])

    def test_missing_metadata_and_corruption_reported(self):
        self.fake.add(self.name)
        self.assertIn('SESSION_METADATA_MISSING', s.recovery_plan(self.cfg, 'test', self.roots)['issues'])
        self.journal()
        (self.state / 'test/session.json').write_text('broken')
        self.assertEqual(s.recovery_plan(self.cfg, 'test', self.roots)['status'], 'quarantine')

    def test_gateway_mismatch_reported(self):
        self.journal(self.live_record())
        self.gateway_error = RuntimeError('GATEWAY_PROCESS_IDENTITY_MISMATCH')
        plan = s.recovery_plan(self.cfg, 'test', self.roots)
        self.assertTrue(any('IDENTITY_MISMATCH' in issue for issue in plan['issues']))

    def test_absent(self):
        self.assertEqual(s.recovery_plan(self.cfg, 'test', self.roots)['status'], 'absent')

    def test_active_missing_socket_is_reported(self):
        self.journal(self.live_record())
        self.fake.add(self.name, running=True)
        plan = s.recovery_plan(self.cfg, 'test', self.roots)
        self.assertIn('GATEWAY_SOCKET_MISSING', plan['issues'])
        self.assertEqual(plan['status'], 'quarantine')

    def test_missing_runtime_is_reported(self):
        self.journal(self.live_record(), runtime=False)
        self.fake.add(self.name, running=True)
        plan = s.recovery_plan(self.cfg, 'test', self.roots)
        self.assertIn('RUNTIME_DIRECTORY_MISSING', plan['issues'])

    def test_no_journal_and_docker_unavailable_returns_quarantine(self):
        with patch.object(s, 'docker', side_effect=subprocess.TimeoutExpired('fake', 15)):
            plan = s.recovery_plan(self.cfg, 'test', self.roots)
        self.assertEqual(plan['status'], 'quarantine')
        self.assertNotEqual(plan['issues'], [])

    def test_no_mutation_when_docker_unavailable(self):
        self.journal()
        before = self.snapshot()
        with patch.object(s, 'docker', side_effect=subprocess.TimeoutExpired('fake', 15)):
            plan = s.recovery_plan(self.cfg, 'test', self.roots)
        self.assertEqual(plan['status'], 'quarantine')
        self.assertEqual(before, self.snapshot())


class MigrationAndConfig(Isolated):
    def deployment_config(self, **changes):
        return dict(self.cfg, agent_uid=os.geteuid()+10001, **changes)

    def test_config_requires_fencing_and_schema(self):
        for changes in (dict(legacy_executor_fenced=False), dict(legacy_executor_fenced=1),
                        dict(lifecycle_schema=3.0), dict(deployment_id='bad')):
            with self.subTest(changes=changes), self.assertRaises(RuntimeError):
                s.validate_config(self.deployment_config(**changes))
        cfg = self.deployment_config()
        s.validate_config(cfg)
        for key in cfg:
            copy_cfg = dict(cfg)
            del copy_cfg[key]
            with self.subTest(missing=key), self.assertRaises(RuntimeError):
                s.validate_config(copy_cfg)

    def test_agent_must_differ_from_operator_and_root(self):
        for uid in (0, os.geteuid()):
            cfg = dict(self.cfg, agent_uid=uid)
            with self.subTest(uid=uid), self.assertRaises(RuntimeError):
                s.validate_config(cfg)

    def test_strict_config_types(self):
        for key, value in [('agent_gid', True), ('pids_limit', 0), ('cpus', True), ('cpus', float('nan')),
                           ('memory', 'bad'), ('workspace_root', '/tmp/a,b'), ('image', '--bad')]:
            cfg = self.deployment_config()
            cfg[key] = value
            with self.subTest(key=key), self.assertRaises(RuntimeError):
                s.validate_config(cfg)

    def test_all_legacy_session_lengths_cannot_target_new_name(self):
        cfg = self.deployment_config()
        prefix = cfg['legacy_container_prefix'] + '-'
        for length in range(1, 41):
            name = s.session_name(cfg, 'x'*length)
            self.assertTrue(name.startswith(prefix))
            self.assertIsNone(s.SESSION_RE.fullmatch(name[len(prefix):]))
        self.assertNotEqual(s.GATEWAY_ROOT, Path('/run/duality-core/gateways'))
        self.assertNotEqual(s.STATE_ROOT, s.GATEWAY_ROOT)

    def test_original_executor_rejects_v3_target_before_reading_config(self):
        old = types.ModuleType('legacy_reference_audit')
        reference = Path(__file__).resolve().parents[1] / 'executor/sessions.py.production'
        exec(compile(reference.read_text(), str(reference), 'exec'), old.__dict__)
        target = self.name[len(self.cfg['legacy_container_prefix'])+1:]
        self.assertGreater(len(target), 40)
        fake_config = Mock()
        fake_config.read_text.side_effect = AssertionError('LEGACY CONFIG ACCESS BLOCKED')
        with patch.object(old, 'CONFIG', fake_config), patch.object(s.sys, 'argv', ['sessions.py', 'destroy', target]):
            with self.assertRaisesRegex(SystemExit, 'SESSION_ID_INVALID'):
                old.main()
        fake_config.read_text.assert_not_called()
        self.assertEqual(self.fake.calls, [])

    def test_declared_legacy_prefix_collision_rejected(self):
        # A different old prefix could consume part of the new namespace.
        cfg = self.deployment_config(legacy_container_prefix='duality-lifecycle-v3')
        with self.assertRaisesRegex(RuntimeError, 'COLLISION'):
            s.validate_config(cfg)

    def test_workspace_namespace_overlap_rejected(self):
        for path in (self.cfg['legacy_workspace_root'], str(self.state), str(self.runtime),
                     str(self.root), self.cfg['legacy_workspace_root'] + '/child'):
            cfg = self.deployment_config()
            cfg['workspace_root'] = path
            with self.subTest(path=path), self.assertRaisesRegex(RuntimeError, 'OVERLAP'):
                s.validate_config(cfg)

    def test_v3_config_does_not_replace_legacy_config(self):
        self.assertEqual(s.CONFIG.name, 'config.v3.json')
        self.assertNotEqual(s.CONFIG.name, 'config.json')

    def test_v2_record_not_adopted(self):
        record = self.record(schema_version=2)
        with self.assertRaisesRegex(RuntimeError, 'RECORD_INVALID'):
            s.validate_record(record, 'test', self.name, self.cfg['deployment_id'])

    def test_deployment_mismatch_rejected(self):
        with self.assertRaisesRegex(RuntimeError, 'RECORD_INVALID'):
            s.validate_record(self.record(), 'test', self.name, 'e'*32)

    def test_layout_checks_legacy_symlinks(self):
        alias = self.root / 'legacy-alias'
        alias.symlink_to(self.legacy_workspaces)
        cfg = self.deployment_config(legacy_workspace_root=str(alias))
        with self.assertRaises(OSError):
            s.validate_layout(cfg, self.roots)

    def test_main_recover_is_read_only_and_outputs_json(self):
        self.creation()
        self.journal(self.live_record())
        self.fake.add(self.name, running=True)
        cfg = self.deployment_config()
        config_path = self.root / 'config.v3.json'
        config_path.write_text(json.dumps(cfg))
        config_path.chmod(0o600)
        self.lock_context.__exit__(None, None, None)
        before = {str(p): p.read_bytes() for p in self.root.rglob('*') if p.is_file()}
        output = io.StringIO()
        with patch.object(s, 'CONFIG', config_path), patch.object(s.sys, 'argv', ['sessions.py', 'recover', 'test']), redirect_stdout(output):
            s.main()
        after = {str(p): p.read_bytes() for p in self.root.rglob('*') if p.is_file()}
        self.assertEqual(json.loads(output.getvalue())['status'], 'operator-review')
        self.assertEqual(before, after)
        self.assertFalse(any(c[0] in ('create', 'start', 'kill', 'rm') for c in self.fake.calls))

    def test_main_rejects_old_config_before_infrastructure(self):
        cfg_path = self.root / 'config.json'
        cfg_path.write_text(json.dumps(dict(container_prefix='duality')))
        cfg_path.chmod(0o600)
        with patch.object(s, 'CONFIG', cfg_path), patch.object(s.sys, 'argv', ['sessions.py', 'create', 'test']):
            with self.assertRaisesRegex(RuntimeError, 'CONFIG_INVALID'):
                s.main()
        self.assertEqual(self.fake.calls, [])


class Launcher(Isolated):
    def run_launcher(self, permit, events=True):
        poll = Mock()
        poll.poll.return_value = [(44, select.POLLIN)] if events else []
        close = Mock()
        execute = Mock()
        with patch.object(s.select, 'poll', return_value=poll), \
             patch.object(s.os, 'read', return_value=permit), \
             patch.object(s.os, 'close', close), \
             patch.object(s.os, 'execv', execute), \
             patch.object(s.sys, 'argv', ['-c', '44', '/usr/bin/python3', '-B', '/fixture-gateway']):
            if permit == b'1' and events:
                exec(compile(s.GATEWAY_LAUNCHER, '<launcher>', 'exec'), {})
            else:
                with self.assertRaises(SystemExit) as error:
                    exec(compile(s.GATEWAY_LAUNCHER, '<launcher>', 'exec'), {})
                self.assertEqual(error.exception.code, 125)
        return execute, close

    def test_eof_never_executes_gateway(self):
        execute, close = self.run_launcher(b'')
        execute.assert_not_called()
        close.assert_called_once_with(44)

    def test_timeout_never_executes_gateway(self):
        execute, _ = self.run_launcher(b'', events=False)
        execute.assert_not_called()

    def test_wrong_permit_never_executes_gateway(self):
        execute, _ = self.run_launcher(b'x')
        execute.assert_not_called()

    def test_release_executes_exact_command_same_process(self):
        execute, _ = self.run_launcher(b'1')
        execute.assert_called_once_with('/usr/bin/python3', ['/usr/bin/python3', '-B', '/fixture-gateway'])


class Locking(Isolated):
    def test_contended_lock_has_deadline(self):
        started = time.monotonic()
        with self.assertRaisesRegex(RuntimeError, 'LOCK_TIMEOUT'):
            with s.lifecycle_lock(timeout=0.05):
                self.fail('overlapping lock')
        self.assertLess(time.monotonic()-started, 1.0)

    def test_threads_do_not_overlap(self):
        # Exercise lifecycle_lock itself with independent opens of a temp lock.
        other = self.root / 'thread-state'
        other.mkdir(mode=0o700)
        active = 0
        maximum = 0
        gate = threading.Barrier(3)
        errors = []
        def worker():
            nonlocal active, maximum
            try:
                gate.wait(timeout=2)
                with s.lifecycle_lock(timeout=2):
                    active += 1
                    maximum = max(maximum, active)
                    time.sleep(0.01)
                    active -= 1
            except BaseException as exc:
                errors.append(repr(exc))
        with patch.object(s, 'STATE_ROOT', other):
            threads = [threading.Thread(target=worker) for _ in range(2)]
            for thread in threads:
                thread.start()
            gate.wait(timeout=2)
            for thread in threads:
                thread.join(timeout=3)
        self.assertFalse(any(t.is_alive() for t in threads))
        self.assertEqual(errors, [])
        self.assertEqual(maximum, 1)

    def test_lock_symlink_and_hardlink_rejected(self):
        # Use a separate state root because setup holds its own lock.
        other = self.root / 'other-state'
        other.mkdir(mode=0o700)
        victim = self.root / 'victim'
        victim.write_text('keep')
        victim.chmod(0o600)
        lock = other / '.lifecycle.lock'
        with patch.object(s, 'STATE_ROOT', other):
            lock.symlink_to(victim)
            with self.assertRaises(OSError):
                with s.lifecycle_lock():
                    pass
            lock.unlink()
            os.link(victim, lock)
            with self.assertRaisesRegex(RuntimeError, 'UNTRUSTED_LOCK'):
                with s.lifecycle_lock():
                    pass
        self.assertEqual(victim.read_text(), 'keep')

    def test_exception_releases_lock(self):
        other = self.root / 'exception-state'
        other.mkdir(mode=0o700)
        with patch.object(s, 'STATE_ROOT', other):
            with self.assertRaisesRegex(RuntimeError, 'injected'):
                with s.lifecycle_lock():
                    raise RuntimeError('injected')
            with s.lifecycle_lock(timeout=0.05):
                pass

    def test_same_root_alias_rejected(self):
        with patch.object(s, 'GATEWAY_ROOT', self.state), self.assertRaisesRegex(RuntimeError, 'ROOT_ALIAS'):
            with s.lifecycle_lock():
                pass

    def test_concurrent_create_only_one_wins(self):
        self.creation()
        other = self.root / 'concurrent-state'
        other.mkdir(mode=0o700)
        barrier = threading.Barrier(3)
        outcomes = []
        def worker():
            try:
                barrier.wait(timeout=2)
                with s.lifecycle_lock(timeout=2) as roots:
                    s.create(self.cfg, 'test', roots)
                outcomes.append('created')
            except RuntimeError as exc:
                outcomes.append(str(exc))
            except BaseException as exc:
                outcomes.append(type(exc).__name__)
        with patch.object(s, 'STATE_ROOT', other):
            threads = [threading.Thread(target=worker) for _ in range(2)]
            for thread in threads:
                thread.start()
            barrier.wait(timeout=2)
            for thread in threads:
                thread.join(timeout=3)
        self.assertFalse(any(t.is_alive() for t in threads))
        self.assertCountEqual(outcomes, ['created', 'SESSION_EXISTS'])
        self.assertEqual(sum(c[0] == 'create' for c in self.fake.calls), 1)

    def test_read_only_lock_does_not_create_file(self):
        other = self.root / 'other-state'
        other.mkdir(mode=0o700)
        with patch.object(s, 'STATE_ROOT', other), self.assertRaises(FileNotFoundError):
            with s.lifecycle_lock(read_only=True):
                pass
        self.assertEqual(list(other.iterdir()), [])


if __name__ == '__main__':
    unittest.main()
