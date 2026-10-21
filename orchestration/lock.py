"""Cross-platform advisory file locking for inter-process synchronization."""
import os
import sys
import time
from typing import Optional

from orchestration.errors import PermanentError

if sys.platform == 'win32':
    import msvcrt
else:
    import fcntl


class CrossPlatformFileLock:
    """Inter-process file lock working on POSIX (fcntl.flock) and Windows (msvcrt.locking)."""

    def __init__(self, lock_path: str, timeout: float = 10.0, retry_interval: float = 0.02):
        self.lock_path = lock_path
        self.timeout = timeout
        self.retry_interval = retry_interval
        self._fd: Optional[int] = None

    def acquire(self) -> None:
        flags = os.O_RDWR | os.O_CREAT
        if hasattr(os, 'O_CLOEXEC'):
            flags |= os.O_CLOEXEC
        
        mode = 0o600 if os.name == 'posix' else 0o666
        self._fd = os.open(self.lock_path, flags, mode)

        deadline = time.monotonic() + self.timeout
        while True:
            try:
                if sys.platform == 'win32':
                    # Lock 1 byte from position 0
                    os.lseek(self._fd, 0, os.SEEK_SET)
                    msvcrt.locking(self._fd, msvcrt.LK_NBLCK, 1)
                else:
                    fcntl.flock(self._fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return  # Acquired successfully
            except (OSError, BlockingIOError):
                if time.monotonic() >= deadline:
                    if self._fd is not None:
                        try:
                            os.close(self._fd)
                        except OSError:
                            pass
                        self._fd = None
                    raise PermanentError(
                        "LOCK_ACQUISITION_TIMEOUT",
                        f"Failed to acquire lock on '{self.lock_path}' within {self.timeout}s timeout"
                    )
                time.sleep(self.retry_interval)

    def release(self) -> None:
        if self._fd is not None:
            try:
                if sys.platform == 'win32':
                    try:
                        os.lseek(self._fd, 0, os.SEEK_SET)
                        msvcrt.locking(self._fd, msvcrt.LK_UNLCK, 1)
                    except OSError:
                        pass
                else:
                    try:
                        fcntl.flock(self._fd, fcntl.LOCK_UN)
                    except OSError:
                        pass
                os.close(self._fd)
            finally:
                self._fd = None

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()
