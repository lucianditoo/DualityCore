"""Test-only interpreter crashes/restart. Only the fixed lab payload is launched."""
import concurrent.futures
import json
import os
from pathlib import Path
import sys
import time
from contextlib import contextmanager

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from lab.forge_link.adapter import Adapter
from lab.ignition.backend import Backend
from lab.handshake.ledger import ExecutionLedger, TUPLE


class BeforeCommit(ExecutionLedger):
    @contextmanager
    def transaction(self):
        with super().transaction() as state:
            yield state
            if self.db.execute("SELECT 1 FROM runs WHERE json_extract(body, '$.evidence') IS NOT NULL").fetchone():
                os._exit(51)


def main():
    spec = json.loads(Path(sys.argv[1]).read_text())
    q = spec['request']
    if spec['mode'] == 'before-commit':
        Backend.authority_type = BeforeCommit
    adapter = Adapter(spec['root'], spec['deployment'])
    if len(sys.argv) == 3:
        # Same test worker PID after exec, still the child's direct parent.
        # Reaping our child is test teardown only; no recovered task is signalled.
        pid = int(sys.argv[2])
        with adapter.backend._db() as ledger:
            a = next(iter(ledger.snapshot()['assignments'].values()))
        response = adapter.handle(json.dumps(dict(version=q['version'], op='recover',
            request_id='aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa', authorization_ref=q['authorization_ref'],
            **{k: a[k] for k in TUPLE})))
        assert response['error']['code'] == 'OUTCOME_UNKNOWN'
        adapter.handle(json.dumps(q))
        os.waitpid(pid, 0)  # No adoption/PID signalling; direct child survives exec.
        adapter.close()
        return
    if spec['mode'] == 'exec-during-run':
        pool = concurrent.futures.ThreadPoolExecutor()
        future = pool.submit(adapter.handle, json.dumps(q))
        deadline = time.monotonic() + 4
        while time.monotonic() < deadline:
            facts = adapter.backend.facts(q['request_id'])
            if facts['phase'] == 'RUNNING':
                os.execv(sys.executable, [sys.executable, '-B', str(Path(__file__).resolve()), sys.argv[1], str(facts['identity']['pid'])])
            if future.done():
                raise AssertionError('child finished before restart barrier')
            time.sleep(0.005)
        raise TimeoutError('no running child')
    adapter.handle(json.dumps(q))
    os._exit(52)


if __name__ == '__main__':
    main()
