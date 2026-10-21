# DUALITY CORE — Engineering Assignment

You are the lead software engineer responsible for hardening the
session lifecycle and orphan recovery subsystem of DUALITY CORE.

## Environment

- Ubuntu 24.04 VM
- Python 3.12
- Docker
- systemd
- Unix domain sockets
- Codex CLI
- Agent sessions managed by a Python executor

## Files

- executor/sessions.py: development candidate; you may modify it.
- executor/sessions.py.production: reference; DO NOT modify it.
- tests/: create automated tests here.
- docs/: create design and security documentation here.

## Objectives

1. Audit the current session lifecycle implementation.
2. Fix creation and destruction failure paths.
3. Verify Docker ownership using immutable container IDs and labels.
4. Persist gateway identity using PID, boot ID and process start ticks.
5. Use pidfd to prevent PID reuse vulnerabilities.
6. Validate gateway identity before termination.
7. Wait for process exit before deleting gateway files.
8. Handle missing, malformed and inconsistent metadata safely.
9. Handle partial Docker creation failures.
10. Make cleanup idempotent.
11. Design safe orphan recovery after Bridge crashes.
12. Preserve workspaces and unrelated containers.
13. Create comprehensive automated tests.
14. Document security guarantees and remaining risks.

## Critical security restrictions

NEVER modify /opt/duality-core or production files.

NEVER restart, stop or reconfigure:
- Ubuntu
- Docker
- duality-bridge.service
- duality-cao.service
- duality-auth-proxy.service
- existing containers or gateways

NEVER touch duality-sol-sandbox.

NEVER access or modify authentication credentials or OAuth secrets.

NEVER execute create/destroy against real infrastructure.

NEVER use sudo or request elevated privileges.

NEVER deploy changes automatically.

Work ONLY inside this development workspace.

Use mocks and temporary directories for tests.
Do not interact with the real Docker daemon or production sockets.

## Workflow

First inspect the available source code.

Explain the architecture and identify security risks.

Implement the required changes in the development candidate.

Create and execute automated tests without touching production.

Produce:
- docs/SECURITY_REVIEW.md
- docs/RECOVERY_DESIGN.md
- docs/DEPLOYMENT_PLAN.md

The deployment plan must include rollback procedures.

Do not deploy.

## Completion

When finished, report:
- files changed
- tests executed and results
- security issues fixed
- remaining risks
- decisions requiring human approval

Stop and ask for authorization if an action would violate these rules.
