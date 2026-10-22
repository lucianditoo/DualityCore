"""Prepared evidence collector. Importing defines code only; no collection runs.

Exclusive, private files; bounded JSONL; chained hashes detect accidental changes,
not an administrator rewriting evidence. No broad snapshots or environment dumps.
"""
import fcntl
import json
import os
import stat
import time
from guard import need, open_directory, digest, assert_authorization_current


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def write_all(fd, raw):
    view = memoryview(raw)
    while view:
        written = os.write(fd, view)
        need(written > 0, 'evidence write failed')
        view = view[written:]


class Evidence:
    def __init__(self, ctx):
        need(ctx.get('environment_verified') is True, 'environment must be verified first')
        self.ctx = ctx
        self.root = self.lock = self.directory = self.events = None
        self.sequence = 0
        self.previous = '0' * 64
        self.finished = False
        self.marker_created = False
        self.name = ctx['manifest']['run_id'] + '-' + ctx['scope']
        need(all(c.isalnum() or c in '-_' for c in self.name), 'invalid evidence name')

    def __enter__(self):
        try:
            assert_authorization_current(self.ctx)
            need(self.ctx['scope'] == 'L00', 'only L00 evidence execution is enabled')
            self.root = open_directory(self.ctx['manifest']['roots']['evidence'], private=True)
            self.lock = os.open('.ironclad.lock', os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW |
                                os.O_CLOEXEC | os.O_NONBLOCK, 0o600, dir_fd=self.root)
            st = os.fstat(self.lock)
            need(stat.S_ISREG(st.st_mode) and st.st_uid == os.geteuid()
                 and stat.S_IMODE(st.st_mode) == 0o600 and st.st_nlink == 1, 'untrusted evidence lock')
            fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            need(not {'.IRONCLAD_STOP', '.IRONCLAD_IN_PROGRESS'} & set(os.listdir(self.root)),
                 'stop latch or unfinished operation present')
            assert_authorization_current(self.ctx)
            marker = os.open('.IRONCLAD_IN_PROGRESS', os.O_WRONLY | os.O_CREAT | os.O_EXCL |
                             os.O_NOFOLLOW | os.O_CLOEXEC, 0o600, dir_fd=self.root)
            self.marker_created = True
            try:
                write_all(marker, encoded(dict(run=self.name, scope=self.ctx['scope'])) + b'\n')
                os.fsync(marker)
                os.fsync(self.root)
            finally:
                os.close(marker)
            os.mkdir(self.name, 0o700, dir_fd=self.root)  # Never overwrite/reuse a run.
            self.directory = os.open(self.name, os.O_RDONLY | os.O_DIRECTORY |
                                     os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=self.root)
            os.fsync(self.root)
            self.events = os.open('events.jsonl', os.O_WRONLY | os.O_CREAT | os.O_EXCL |
                                  os.O_APPEND | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600,
                                  dir_fd=self.directory)
            self.event('begin', dict(scope=self.ctx['scope'],
                                     manifest_sha256=self.ctx['manifest_sha256'],
                                     authorization_sha256=self.ctx['authorization_sha256'],
                                     artifact_lock_sha256=self.ctx['manifest']['baseline']['artifact_lock_sha256'],
                                     observed=self.ctx['observed']))
            return self
        except BaseException:
            try:
                if self.marker_created:
                    self.stop('evidence setup incomplete')
            finally:
                self.close()
            raise

    def event(self, name, payload):
        limits = self.ctx['manifest']['limits']
        need(self.sequence < limits['max_events'], 'evidence event count exceeded')
        body = dict(sequence=self.sequence, monotonic_ns=time.monotonic_ns(),
                    utc=time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
                    event=name, payload=payload, previous_sha256=self.previous)
        checksum = digest(encoded(body))
        raw = encoded(dict(body=body, sha256=checksum)) + b'\n'
        need(len(raw) <= limits['max_event_bytes'], 'evidence event size exceeded')
        write_all(self.events, raw)
        os.fsync(self.events)
        self.previous = checksum
        self.sequence += 1

    def stop(self, reason):
        # No deletion, signaling, daemon calls or automatic removal of this latch.
        try:
            fd = os.open('.IRONCLAD_STOP', os.O_WRONLY | os.O_CREAT | os.O_EXCL |
                         os.O_NOFOLLOW | os.O_CLOEXEC, 0o600, dir_fd=self.root)
        except FileExistsError:
            return
        try:
            write_all(fd, encoded(dict(run=self.name, reason=reason)) + b'\n')
            os.fsync(fd)
            os.fsync(self.root)
        finally:
            os.close(fd)

    def finish(self, result, details):
        self.event('finish', dict(result=result, details=details))
        summary = dict(scope=self.ctx['scope'], result=result, details=details,
                       run_id=self.ctx['manifest']['run_id'],
                       manifest_sha256=self.ctx['manifest_sha256'],
                       authorization_sha256=self.ctx['authorization_sha256'],
                       artifact_lock_sha256=self.ctx['manifest']['baseline']['artifact_lock_sha256'],
                       baseline=self.ctx['manifest']['baseline'], observed=self.ctx['observed'],
                       events=self.sequence, final_event_sha256=self.previous,
                       limitations=['NO_DOCKER_CONNECTION', 'NO_EXECUTOR_CREATE_DESTROY',
                                    'NO_POWERLOSS_TEST', 'NO_PID_REUSE_TEST',
                                    'GUEST_IDENTITY_REQUIRES_OPERATOR_ATTESTATION'])
        fd = os.open('summary.json', os.O_WRONLY | os.O_CREAT | os.O_EXCL |
                     os.O_NOFOLLOW | os.O_CLOEXEC, 0o600, dir_fd=self.directory)
        try:
            write_all(fd, encoded(summary) + b'\n')
            os.fsync(fd)
            os.fsync(self.directory)
        finally:
            os.close(fd)
        self.finished = True
        return summary

    def close(self):
        for name in ('events', 'directory', 'lock', 'root'):
            fd = getattr(self, name)
            if fd is not None:
                os.close(fd)
                setattr(self, name, None)

    def __exit__(self, kind, value, tb):
        try:
            if self.marker_created:
                if self.finished and kind is None:
                    # Summary and its directory were fsynced before clearing intent.
                    os.unlink('.IRONCLAD_IN_PROGRESS', dir_fd=self.root)
                    os.fsync(self.root)
                else:
                    self.stop('unfinished experiment; operator review required')
        finally:
            self.close()
