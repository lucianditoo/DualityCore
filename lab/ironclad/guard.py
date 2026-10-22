"""Prepared guards; not executed or experimentally validated in Phase 2.

No Docker client, network connection, subprocess, system change or secret lookup.
Guest observations establish consistency with an operator's out-of-band attestation,
not independent proof of a trusted hypervisor or absence of a malicious root.
"""
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import time
import uuid

BASELINE_COMMIT = 'ccc54e6f25f37f3541d52d14a8fe295beb95e8e7'
EXECUTOR_SHA256 = '00ea3fb806afc56b80f45a34c72fb252a6345ef71ffad54102807d9917a75ce0'
TESTS_SHA256 = '6ab3f38ab58c6797663a79c1abfe431e4efafd24eec379ef9646ef3052aa727d'
BUNDLE_ROOT = Path(__file__).absolute().parents[2]
# Process scopes remain unavailable until independent supervision is prepared.
SCOPES = {'L00'}
FIXED_ROOTS = {name: '/srv/ironclad/' + name for name in ('state', 'runtime', 'evidence')}

def validate_roots(roots):
    keys(roots, 'state runtime evidence')
    need(roots == FIXED_ROOTS, 'only fixed laboratory write roots are permitted')
BUNDLE_FILES = {
    'executor/sessions.py', 'tests/test_sessions.py',
    'lab/ironclad/baseline.json', 'lab/ironclad/guard.py',
    'lab/ironclad/evidence.py', 'lab/ironclad/harness.py',
    'lab/ironclad/test_harness_unit.py',
    'lab/ironclad/fixtures/process_fixture.py',
    'lab/ironclad/fixtures/gateway_fixture.py',
    'lab/ironclad/fixtures/launcher_contract.json',
}
HEX64 = re.compile(r'[0-9a-f]{64}')
SAFE_FS = {'ext4', 'xfs', 'btrfs', 'tmpfs', 'devtmpfs', 'proc', 'sysfs',
           'cgroup2', 'cgroup', 'devpts', 'mqueue', 'securityfs', 'debugfs',
           'tracefs', 'pstore', 'efivarfs', 'configfs', 'hugetlbfs', 'ramfs'}


class Refusal(RuntimeError):
    pass


def need(condition, reason):
    if not condition:
        raise Refusal(reason)


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def keys(obj, expected):
    need(type(obj) is dict and set(obj) == set(expected.split()), 'invalid object fields')


def uuid_text(value):
    try:
        parsed = uuid.UUID(value) if type(value) is str else None
    except ValueError:
        parsed = None
    need(parsed is not None and parsed.int != 0 and str(parsed) == value, 'invalid UUID')
    return value


def hash_text(value):
    need(type(value) is str and HEX64.fullmatch(value) is not None, 'missing/invalid digest')
    return value


def checked_path(path):
    need(type(path) is str and path.startswith('/') and not path.startswith('//')
         and '\0' not in path and ',' not in path and str(Path(path)) == path
         and '..' not in Path(path).parts, 'noncanonical absolute path')
    return Path(path)


def open_directory(path, private=False):
    """Pin every component; reject writable/sticky ancestors, symlinks and aliases.

    Unlike unit-test fixtures, this guard never normalizes owner IDs.
    """
    path = checked_path(str(path))
    fd = os.open('/', os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        components = path.parts[1:]
        for index in range(len(components) + 1):
            st = os.fstat(fd)
            need(stat.S_ISDIR(st.st_mode) and st.st_uid in {0, os.geteuid()}
                 and not st.st_mode & 0o022, 'untrusted directory or ancestor')
            if index == len(components):
                if private:
                    need(st.st_uid == os.geteuid() and stat.S_IMODE(st.st_mode) == 0o700,
                         'private root must be euid-owned 0700')
                return fd
            child = os.open(components[index], os.O_RDONLY | os.O_DIRECTORY |
                            os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=fd)
            os.close(fd)
            fd = child
    except BaseException:
        os.close(fd)
        raise


def read_checked(path, private=False, limit=1048576):
    path = checked_path(str(path))
    parent = open_directory(path.parent)
    try:
        fd = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC,
                     dir_fd=parent)
    finally:
        os.close(parent)
    try:
        st = os.fstat(fd)
        need(stat.S_ISREG(st.st_mode) and st.st_nlink == 1 and st.st_uid in {0, os.geteuid()}
             and not st.st_mode & 0o022 and st.st_size <= limit, 'untrusted/oversized file')
        if private:
            need(st.st_uid == os.geteuid() and stat.S_IMODE(st.st_mode) == 0o600,
                 'control/evidence file must be euid-owned 0600')
        raw = bytearray()
        while len(raw) <= limit:
            part = os.read(fd, min(65536, limit + 1 - len(raw)))
            if not part:
                break
            raw.extend(part)
        need(len(raw) <= limit, 'file exceeds byte limit')
        return bytes(raw)
    finally:
        os.close(fd)


def _unique(pairs):
    obj = {}
    for key, value in pairs:
        need(key not in obj, 'duplicate JSON key')
        obj[key] = value
    return obj


def decode(raw):
    return json.loads(raw, object_pairs_hook=_unique,
                      parse_constant=lambda value: need(False, 'nonfinite JSON number'))


def kernel_bytes(path, limit=1048576):
    # Fixed, non-secret kernel paths only; never arbitrary manifest-selected files.
    with open(path, 'rb', buffering=0) as stream:
        raw = stream.read(limit + 1)
    need(len(raw) <= limit, 'kernel observation exceeds limit')
    return raw


def verify_bundle(lock_hash):
    lock_path = BUNDLE_ROOT / 'lab/ironclad/artifacts.lock.json'
    raw = read_checked(lock_path)
    need(digest(raw) == hash_text(lock_hash), 'artifact lock not approved')
    lock = decode(raw)
    keys(lock, 'schema_version baseline_commit files')
    need(type(lock['schema_version']) is int and lock['schema_version'] == 1
         and lock['baseline_commit'] == BASELINE_COMMIT, 'invalid artifact lock')
    need(type(lock['files']) is dict and set(lock['files']) == BUNDLE_FILES,
         'artifact lock path allowlist mismatch')
    for relative in sorted(BUNDLE_FILES):
        raw = read_checked(BUNDLE_ROOT / relative)
        need(digest(raw) == hash_text(lock['files'][relative]), 'artifact changed: ' + relative)
    need(lock['files']['executor/sessions.py'] == EXECUTOR_SHA256
         and lock['files']['tests/test_sessions.py'] == TESTS_SHA256, 'baseline bytes changed')
    baseline = decode(read_checked(BUNDLE_ROOT / 'lab/ironclad/baseline.json'))
    need(baseline['commit'] == BASELINE_COMMIT and baseline['executor_sha256'] == EXECUTOR_SHA256
         and baseline['tests_sha256'] == TESTS_SHA256, 'baseline identity changed')
    return lock


def authorize(manifest_path, approval_path, scope, execute=False):
    # No kernel identity observations, evidence writes or process creation before
    # these gates. Examples intentionally have execution_enabled/active=false.
    need(execute is True and os.environ.get('IRONCLAD_ENABLE_EXECUTION') == 'OPERATOR_APPROVED',
         'execution disabled; a future operator authorization is required')
    need(scope in SCOPES, 'unsupported scope')
    need(BUNDLE_ROOT == Path('/srv/ironclad/bundle'), 'not the dedicated guest artifact path')
    need(str(manifest_path) == '/srv/ironclad/control/manifest.json'
         and str(approval_path) == '/srv/ironclad/control/authorization.json',
         'only fixed guest control paths are permitted')
    manifest_raw = read_checked(manifest_path, private=True)
    m = decode(manifest_raw)
    keys(m, 'schema_version execution_enabled run_id baseline artifact_root vm roots docker resources limits')
    need(type(m['schema_version']) is int and m['schema_version'] == 1
         and m['execution_enabled'] is True, 'manifest disabled or invalid')
    need(type(m['run_id']) is str and re.fullmatch(r'[0-9a-f]{32}', m['run_id']) is not None,
         'invalid run ID')
    keys(m['baseline'], 'commit executor_sha256 tests_sha256 artifact_lock_sha256')
    need(m['baseline']['commit'] == BASELINE_COMMIT
         and m['baseline']['executor_sha256'] == EXECUTOR_SHA256
         and m['baseline']['tests_sha256'] == TESTS_SHA256, 'unapproved baseline')
    need(checked_path(m['artifact_root']) == BUNDLE_ROOT, 'wrong artifact location')
    approval_raw = read_checked(approval_path, private=True)
    a = decode(approval_raw)
    keys(a, 'schema_version active authorization_id issued_unix expires_unix manifest_sha256 artifact_lock_sha256 scopes vm_uuid boot_id operator_attestations l00_result_sha256')
    need(type(a['schema_version']) is int and a['schema_version'] == 1 and a['active'] is True,
         'authorization disabled')
    uuid_text(a['authorization_id'])
    now = time.time()
    need(type(a['issued_unix']) is int and type(a['expires_unix']) is int
         and a['issued_unix'] <= now < a['expires_unix']
         and 0 < a['expires_unix'] - a['issued_unix'] <= 3600, 'expired/invalid authorization window')
    need(a['manifest_sha256'] == digest(manifest_raw), 'manifest not approved')
    need(a['artifact_lock_sha256'] == m['baseline']['artifact_lock_sha256'], 'bundle approval mismatch')
    need(type(a['scopes']) is list and all(type(x) is str for x in a['scopes'])
         and set(a['scopes']).issubset(SCOPES) and len(set(a['scopes'])) == len(a['scopes'])
         and scope in a['scopes'], 'scope not authorized')
    keys(a['operator_attestations'], 'independent_disposable_vm no_production_access no_host_mounts exclusive_resources')
    need(all(v is True for v in a['operator_attestations'].values()), 'independent operator attestation missing')
    verify_bundle(a['artifact_lock_sha256'])
    keys(m['vm'], 'product_uuid boot_id operator_uid pid_namespace_inode mount_namespace_inode root_device root_inode mountinfo_sha256 network_interfaces')
    uuid_text(m['vm']['product_uuid'])
    uuid_text(m['vm']['boot_id'])
    need(a['vm_uuid'] == m['vm']['product_uuid'] and a['boot_id'] == m['vm']['boot_id'],
         'authorization not bound to VM and boot')
    for key in ('operator_uid', 'pid_namespace_inode', 'mount_namespace_inode', 'root_device', 'root_inode'):
        need(type(m['vm'][key]) is int and m['vm'][key] >= 0, 'unprovisioned VM identity')
    need(m['vm']['network_interfaces'] == ['lo'], 'this phase requires a VM with no external NIC')
    hash_text(m['vm']['mountinfo_sha256'])
    validate_roots(m['roots'])
    paths = [checked_path(m['artifact_root'])] + [checked_path(p) for p in m['roots'].values()]
    for index, first in enumerate(paths):
        for second in paths[index + 1:]:
            need(not first.is_relative_to(second) and not second.is_relative_to(first), 'root overlap')
    keys(m['docker'], 'execution_enabled endpoint daemon_id socket_device socket_inode socket_uid data_root')
    need(m['docker']['execution_enabled'] is False, 'Docker execution is not implemented/authorized in this phase')
    need(type(m['docker']['endpoint']) is str and m['docker']['endpoint'].startswith('unix:///run/ironclad/'),
         'remote/default Docker endpoint prohibited')
    # daemon_id/socket identity are expected operator pins, NOT observed by this
    # process-only guard. Nothing connects to or opens that socket.
    keys(m['resources'], 'max_direct_children docker_mutations_allowed allowed_container_ids allowed_container_names fixture_modes')
    need(type(m['resources']['max_direct_children']) is int and m['resources']['max_direct_children'] == 1
         and m['resources']['docker_mutations_allowed'] is False
         and m['resources']['allowed_container_ids'] == []
         and m['resources']['allowed_container_names'] == [], 'resource policy must be process-only')
    need(m['resources']['fixture_modes'] == [], 'active fixtures are disabled in Phase 2C')
    keys(m['limits'], 'child_timeout_seconds max_stream_bytes max_events max_event_bytes')
    for key, lower, upper in (('child_timeout_seconds', 7, 10), ('max_stream_bytes', 1024, 8192),
                              ('max_events', 16, 1024), ('max_event_bytes', 1024, 65536)):
        need(type(m['limits'][key]) is int and lower <= m['limits'][key] <= upper, 'invalid hard limit')
    need(m['limits']['max_event_bytes'] >= 6*m['limits']['max_stream_bytes'] + 4096,
         'event budget cannot preserve bounded child output')
    return dict(manifest=m, authorization=a, manifest_sha256=digest(manifest_raw),
                authorization_sha256=digest(approval_raw), scope=scope,
                environment_verified=False, observed=None)


def assert_authorization_current(ctx):
    a = ctx['authorization']
    need(a['issued_unix'] <= time.time() < a['expires_unix'], 'authorization expired before action')


def verify_environment(ctx):
    m, expected = ctx['manifest'], ctx['manifest']['vm']
    product = uuid_text(kernel_bytes('/sys/class/dmi/id/product_uuid', 128).decode().strip().lower())
    boot = uuid_text(kernel_bytes('/proc/sys/kernel/random/boot_id', 128).decode().strip())
    need(product == expected['product_uuid'] and boot == expected['boot_id'], 'wrong VM or boot')
    need(os.geteuid() == expected['operator_uid'], 'wrong execution identity')
    need(os.stat('/proc/self/ns/pid').st_ino == expected['pid_namespace_inode']
         and os.stat('/proc/self/ns/mnt').st_ino == expected['mount_namespace_inode'], 'namespace changed')
    root = os.stat('/')
    need((root.st_dev, root.st_ino) == (expected['root_device'], expected['root_inode']), 'root filesystem changed')
    mounts = kernel_bytes('/proc/self/mountinfo')
    need(digest(mounts) == expected['mountinfo_sha256'], 'mount topology changed')
    for line in mounts.decode().splitlines():
        fields = line.split(' - ')
        need(len(fields) == 2 and fields[1].split()[0] in SAFE_FS, 'unknown/shared/container filesystem')
    need(sorted(os.listdir('/sys/class/net')) == ['lo'], 'network is not offline')
    artifact_fd = open_directory(BUNDLE_ROOT)
    try:
        artifact_st = os.fstat(artifact_fd)
        identities = [(artifact_st.st_dev, artifact_st.st_ino)]
    finally:
        os.close(artifact_fd)
    for name in ('state', 'runtime', 'evidence'):
        fd = open_directory(m['roots'][name], private=True)
        try:
            st = os.fstat(fd)
            identities.append((st.st_dev, st.st_ino))
            if name == 'evidence':
                need(not {'.IRONCLAD_STOP', '.IRONCLAD_IN_PROGRESS'} & set(os.listdir(fd)),
                     'stop latch or unfinished operation is present')
        finally:
            os.close(fd)
    need(len(set(identities)) == len(identities), 'aliased roots')
    ctx['observed'] = dict(product_uuid=product, boot_id=boot, operator_uid=os.geteuid(),
                           mountinfo_sha256=digest(mounts), network_interfaces=['lo'],
                           daemon_identity='NOT_CONTACTED_NOT_VERIFIED',
                           independence='OPERATOR_ATTESTED_NOT_PROVEN_BY_GUEST')
    ctx['environment_verified'] = True
    return ctx['observed']


def verify_previous_l00(ctx, path):
    need(path is not None, 'L01 requires an approved L00 result')
    evidence = checked_path(ctx['manifest']['roots']['evidence'])
    path = checked_path(str(path))
    need(path.is_relative_to(evidence) and path.name == 'summary.json', 'invalid preflight evidence path')
    raw = read_checked(path, private=True)
    need(digest(raw) == hash_text(ctx['authorization']['l00_result_sha256']), 'L00 result not approved')
    report = decode(raw)
    need(report['scope'] == 'L00' and report['result'] == 'PROCESS_PREFLIGHT_ONLY'
         and report['manifest_sha256'] == ctx['manifest_sha256']
         and report['artifact_lock_sha256'] == ctx['manifest']['baseline']['artifact_lock_sha256']
         and report['observed']['boot_id'] == ctx['manifest']['vm']['boot_id']
         and report['observed']['product_uuid'] == ctx['manifest']['vm']['product_uuid'],
         'L00 does not match this experiment')
