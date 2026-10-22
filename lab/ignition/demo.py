"""Run one fixed local test and verify its artifact; no task input accepted."""
import hashlib
import json
import sys
import uuid

from .backend import Laboratory


def main():
    if len(sys.argv) != 1:
        raise SystemExit('This local demo accepts no arguments')
    with Laboratory() as lab:
        task, attempt, request = (str(uuid.uuid4()) for _ in range(3))
        lab.backend.advance(task, attempt, 1)
        lab.backend.start(request_id=request, task_id=task, attempt_id=attempt, generation=1)
        response = lab.backend.execute(request)
        result = response['result']
        verified = False
        if response['state'] == 'SUCCEEDED':
            artifact = lab.root / result['workspace_name'] / result['artifact_name']
            verified = hashlib.sha256(artifact.read_bytes()).hexdigest() == result['artifact_sha256']
        print(json.dumps(dict(execution_id=response['execution_id'], state=response['state'],
                              artifact_sha256=result['artifact_sha256'], hash_verified=verified,
                              duration_seconds=result['duration_seconds'], exit_confirmed=result['exit_confirmed']), indent=2))
        if response['state'] != 'SUCCEEDED' or not verified:
            raise SystemExit(1)
    # The explicit temporary-lab context removes only its own test artifacts.


if __name__ == '__main__':
    main()
