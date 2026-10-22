"""One controlled real execution via strict contractual JSON messages."""
import hashlib
import json
import sys
import uuid

from lab.handshake.ledger import TUPLE
from .adapter import ForgeLab, fixed_start


def main():
    if len(sys.argv) != 1:
        raise SystemExit('no arguments accepted')
    with ForgeLab() as lab:
        q = fixed_start(lab.deployment)
        adapter = lab.adapter
        adapter.advance(q['task_id'], q['attempt_id'], q['generation'])
        adapter.approve(q['authorization_ref'], q['task_id'], q['attempt_id'], q['generation'])
        accepted = adapter.handle(json.dumps(q))
        observed = adapter.handle(json.dumps(dict(version=q['version'], op='observe',
            request_id=str(uuid.uuid4()), authorization_ref=q['authorization_ref'], **{k: accepted[k] for k in TUPLE})))
        result = adapter.published_result(accepted['session_id'])
        if not result or observed['task_state'] != 'SUCCEEDED':
            raise SystemExit('no verified result')
        content, digest = adapter.artifact(result['artifact_ref'])
        assert hashlib.sha256(content).hexdigest() == digest == result['artifact_sha256']
        print(json.dumps(dict(acceptance=accepted, observation=observed, result=result), indent=2))


if __name__ == '__main__':
    main()
