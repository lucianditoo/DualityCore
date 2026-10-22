"""Test-only owner crash after the fixed child has exited and been reaped."""
import os
from pathlib import Path
import sys
from contextlib import contextmanager

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from lab.ignition.backend import Backend


class BeforeResultCommit(Backend):
    @contextmanager
    def _db(self):
        with super()._db() as db:
            yield db
            if db.execute('SELECT 1 FROM attempts WHERE result IS NOT NULL').fetchone():
                os._exit(41)  # UPDATE result is pending, owned child already reaped.


if __name__ == '__main__':
    root, request, point = sys.argv[1:]
    backend = (BeforeResultCommit if point == 'before' else Backend)(root)
    try:
        backend.execute(request)
        os._exit(42)  # Result committed, ACK deliberately lost.
    finally:
        backend.close()
