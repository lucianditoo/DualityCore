"""Independent test interpreter; IPC uses only regular scratch files, no sockets."""
import json
import os
from pathlib import Path
import sys
import time
from contextlib import contextmanager

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from lab.handshake.simulator import Simulator, canonical


class CrashBeforeCommit(Simulator):
    @contextmanager
    def transaction(self):
        with super().transaction() as state:
            yield state
            # Exercise SQLite rollback of actual dirty pages, not only Python data.
            self.db.execute('UPDATE model SET body=? WHERE id=1', (canonical(state),))
            os._exit(31)


def main():
    spec = json.loads(Path(sys.argv[1]).read_text())
    kind = spec['kind']
    cls = CrashBeforeCommit if kind == 'crash_before_commit' else Simulator
    sim = cls(spec['ledger'], spec['deployment'], simulated_channel=True)
    try:
        Path(spec['ready']).write_text(str(os.getpid()))
        deadline = time.monotonic() + 15
        while not Path(spec['gate']).exists():
            if time.monotonic() >= deadline:
                raise TimeoutError('test gate not released')
            time.sleep(0.01)
        if kind in ('handle', 'crash_before_commit', 'crash_after_commit'):
            result = sim.handle(json.dumps(spec['request']))
            if kind == 'crash_after_commit':
                os._exit(32)  # Committed operation, no ACK delivered.
        elif kind == 'result':
            result = sim.simulate_result(json.dumps(spec['result']))
        elif kind == 'reassign':
            q = spec['request']
            sim.advance(q['task_id'], q['attempt_id'], q['generation'])
            result = sim.handle(json.dumps(q))
        else:
            raise ValueError('unknown test action')
        Path(spec['output']).write_text(json.dumps(dict(pid=os.getpid(), result=result)))
    finally:
        sim.close()


if __name__ == '__main__':
    main()
