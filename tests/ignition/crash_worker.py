"""Owner crash around the shared authority's final commit, after child reaping."""
import os
from pathlib import Path
import sys
from contextlib import contextmanager

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from lab.ignition.backend import Backend
from lab.handshake.ledger import ExecutionLedger


class BeforeResultCommit(ExecutionLedger):
    @contextmanager
    def transaction(self):
        with super().transaction() as state:
            yield state
            if self.db.execute("SELECT 1 FROM runs WHERE json_extract(body, '$.evidence') IS NOT NULL").fetchone():
                os._exit(41)


if __name__ == '__main__':
    root, request, point = sys.argv[1:]
    if point == 'before':
        Backend.authority_type = BeforeResultCommit
    backend = Backend(root)
    try:
        backend.execute(request)
        os._exit(42)
    finally:
        backend.close()
