"""Fixed-process supervisor: no ledger, fencing, contractual results or publication."""
import hashlib
import os
from pathlib import Path
import selectors
import stat
import subprocess
import sys
import time

PROFILE = 'isolated-echo-v1'
PROGRAM = Path(__file__).with_name('echo_program.py').resolve()
INTERPRETER = str(Path(sys.executable).resolve())
ARTIFACT = b'IGNITION isolated-echo-v1\n'
CAPTURE_LIMIT = 4096


def identity(pid):
    # Only called on the directly owned, unreaped child. Never used to signal a PID.
    text = Path(f'/proc/{pid}/stat').read_text()
    return dict(pid=pid, boot_id=Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
                start_ticks=int(text[text.rfind(')') + 2:].split()[19]))


class ProcessRunner:
    def __init__(self):
        self.proc = None
        self.identity = None
        self.workspace = None

    def launch(self, workspace):
        if os.geteuid() == 0:
            raise PermissionError('root is forbidden')
        if self.proc is not None:
            raise RuntimeError('runner is single-use')
        self.workspace = Path(workspace)
        self.proc = subprocess.Popen([INTERPRETER, '-I', '-B', str(PROGRAM)], cwd=workspace,
                                     env={'LANG': 'C.UTF-8', 'LC_ALL': 'C.UTF-8'},
                                     stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                     stderr=subprocess.PIPE, close_fds=True, shell=False)
        self.identity = identity(self.proc.pid)
        return self.identity

    def supervise(self, *, timeout_ms, cancel_requested, on_timeout, allow_timeout_termination=False):
        """Timeout reports uncertainty immediately; waits for actual exit, without kill.

        cancel_requested returns only an explicitly approved intent. UNKNOWN and
        revocation are not cancellation requests. Handles remain owned until wait.
        """
        proc = self.proc
        started = time.monotonic()
        buffers = [bytearray(), bytearray()]
        totals = [0, 0]
        timed_out = False
        termination_requested = False
        cancelled = False
        with selectors.DefaultSelector() as selector:
            for index, stream in enumerate((proc.stdout, proc.stderr)):
                os.set_blocking(stream.fileno(), False)
                selector.register(stream, selectors.EVENT_READ, index)
            try:
                while selector.get_map() or proc.poll() is None:
                    if not timed_out and (time.monotonic() - started) * 1000 >= timeout_ms:
                        timed_out = True
                        on_timeout()  # Durable knowledge, not a claim of process exit.
                    requested = cancel_requested()
                    if requested:
                        cancelled = True
                    if (requested or (timed_out and allow_timeout_termination)) and not termination_requested and proc.poll() is None:
                        proc.terminate()  # Only directly owned Popen, never a saved PID.
                        termination_requested = True
                    for key, _ in selector.select(0.01):
                        data = os.read(key.fileobj.fileno(), 16384)
                        if not data:
                            selector.unregister(key.fileobj)
                            key.fileobj.close()
                            continue
                        index = key.data
                        totals[index] += len(data)
                        buffers[index].extend(data[:max(0, CAPTURE_LIMIT - len(buffers[index]))])
                proc.wait()
            finally:
                # No signal or SIGKILL on recovery, exceptions or scope exit.
                # Even on an error, retain ownership and drain until the fixed child
                # exits; this may wait indefinitely for a different/stuck program.
                while selector.get_map() or proc.poll() is None:
                    for key, _ in selector.select(0.01):
                        data = os.read(key.fileobj.fileno(), 16384)
                        if not data:
                            selector.unregister(key.fileobj)
                            key.fileobj.close()
                        else:
                            index = key.data
                            totals[index] += len(data)
                            buffers[index].extend(data[:max(0, CAPTURE_LIMIT - len(buffers[index]))])
                proc.wait()
                for stream in (proc.stdout, proc.stderr):
                    stream.close()
        content = None
        if proc.returncode == 0 and not timed_out and not cancelled:
            fd = os.open(self.workspace / 'artifact.txt', os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
            try:
                if not stat.S_ISREG(os.fstat(fd).st_mode):
                    raise ValueError('invalid artifact')
                content = os.read(fd, len(ARTIFACT) + 1)
                if content != ARTIFACT:
                    raise ValueError('unexpected artifact bytes')
                os.fsync(fd)
            finally:
                os.close(fd)
        return dict(identity=self.identity, exit_confirmed=True, returncode=proc.returncode,
                    timed_out=timed_out, termination_requested=termination_requested,
                    cancelled=cancelled, duration_seconds=time.monotonic() - started,
                    artifact_hex=content.hex() if content is not None else None,
                    artifact_sha256=hashlib.sha256(content).hexdigest() if content is not None else None,
                    stdout=buffers[0].decode('ascii', errors='replace'),
                    stderr=buffers[1].decode('ascii', errors='replace'),
                    stdout_bytes=totals[0], stderr_bytes=totals[1],
                    stdout_truncated=totals[0] > CAPTURE_LIMIT, stderr_truncated=totals[1] > CAPTURE_LIMIT)
