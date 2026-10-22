"""In-process experimental dmaol-executor/1 adapter; no external transport."""
import copy
import json
import os
from pathlib import Path
import tempfile
import uuid

from lab.handshake.ledger import decode, ExecutionLedger
from lab.ignition.backend import Backend


class Adapter:
    def __init__(self, root, deployment_id, *, initialize=False):
        # Contractual timeout is knowledge only, never implicit permission to kill.
        self.backend = Backend(root, deployment_id=deployment_id, initialize=initialize,
                               timeout_termination=False)

    def close(self):
        self.backend.close()

    def approve(self, *args, **kwargs):
        """Trusted lab control; JSON authorization_ref alone grants no permission."""
        with self.backend._db() as ledger:
            ledger.approve(*args, **kwargs)

    def advance(self, task_id, attempt_id, generation):
        self.backend.advance(task_id, attempt_id, generation)

    def handle(self, raw):
        with self.backend._db() as ledger:
            response = ledger.handle(raw)
        if response['error'] is None and decode(raw)['op'] == 'start':
            # Durable assignment + execution claim ensure at most one launch.
            # A replay returns the same acceptance ACK; observe reads progress.
            self.backend.execute_session(response['session_id'])
        return response

    def published_result(self, session_id):
        """Local authenticated result-channel stand-in; exact normative result fields.

        Returns only a currently fenced publication. No logs/paths in wire control
        responses. Historical artifacts remain stored but are not republished.
        """
        with self.backend._db() as ledger:
            with ledger.transaction() as state:
                a = state['assignments'].get(session_id)
                if not a or not ledger.current(state, a):
                    return None
                run = ledger.run(session_id)
                record = state['results'].get(run['execution_id'])
                if not record or record['status'] != 'ACCEPTED':
                    return None
                return copy.deepcopy(record['body'])

    def artifact(self, ref):
        """Trusted local retrieval of bytes, not a message/transport operation."""
        with self.backend._db() as ledger:
            return ledger.artifact(ref)


class ForgeLab:
    def __init__(self):
        if os.geteuid() == 0:
            raise PermissionError('root is forbidden')
        self.directory = tempfile.TemporaryDirectory(prefix='ignition-')
        self.root = Path(self.directory.name)
        self.deployment = uuid.uuid4().hex
        self.adapter = Adapter(self.root, self.deployment, initialize=True)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.adapter.close()
        self.directory.cleanup()


def fixed_start(deployment_id):
    """Demo fixture builder. No variable task input or executable selection."""
    return dict(version='dmaol-executor/1', op='start', request_id=str(uuid.uuid4()),
                deployment_id=deployment_id, task_id=str(uuid.uuid4()), attempt_id=str(uuid.uuid4()),
                generation=1, session_id=None, authorization_ref='approval-lab-001',
                profile_ref='isolated-echo-v1', input_ref=ExecutionLedger.INPUT_REF,
                input_sha256=ExecutionLedger.INPUT_HASH, execution_timeout_ms=2000)
