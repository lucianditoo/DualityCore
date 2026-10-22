#!/usr/bin/env python3
"""IRONCLAD preparation artifact; execution disabled unless explicitly authorized.

Only process preflight and launcher EOF/timeout cases exist. No Docker commands,
container operations, installation, system configuration or numeric PID signaling.
This module and its prepared dependencies have NOT been run in Phase 2.
"""
import sys
if __name__ == '__main__' and not (sys.flags.isolated and sys.dont_write_bytecode):
    raise SystemExit('IRONCLAD requires python3 -I -B')

import argparse
import ast
import json
import os
from pathlib import Path
import select
import subprocess
import sys
import time

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).absolute().parent))

from guard import (authorize, verify_environment, verify_previous_l00, assert_authorization_current, need,
                   read_checked, decode, digest, BUNDLE_ROOT, EXECUTOR_SHA256, Refusal)
from evidence import Evidence


def launcher_source():
    raw = read_checked(BUNDLE_ROOT / 'executor/sessions.py')
    need(digest(raw) == EXECUTOR_SHA256, 'baseline source changed')
    # Extract a literal only; do not import or exec the Executor module.
    found = []
    for node in ast.parse(raw).body:
        if isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and
                t.id == 'GATEWAY_LAUNCHER' for t in node.targets):
            need(isinstance(node.value, ast.Constant) and type(node.value.value) is str,
                 'launcher must be a literal string')
            found.append(node.value.value)
    need(len(found) == 1, 'ambiguous launcher source')
    contract = decode(read_checked(BUNDLE_ROOT / 'lab/ironclad/fixtures/launcher_contract.json'))
    need(digest(found[0].encode()) == contract['launcher_sha256'], 'launcher contract mismatch')
    return found[0]


def child_identity(pid):
    # Only the direct child's PID, never a manifest-selected/adopted process.
    raw = Path(f'/proc/{pid}/stat').read_text()
    need(raw.split(' ', 1)[0] == str(pid), 'child identity invalid')
    tail = raw[raw.rfind(')') + 2:].split()
    need(len(tail) >= 20 and int(tail[19]) > 0, 'child stat invalid')
    return dict(pid=pid, process_start_ticks=int(tail[19]))


def collect_child(proc, deadline, limit):
    buffers = {proc.stdout: bytearray(), proc.stderr: bytearray()}
    streams = list(buffers)
    for stream in streams:
        os.set_blocking(stream.fileno(), False)
    while streams or proc.poll() is None:
        if time.monotonic() >= deadline:
            return buffers, 'DEADLINE_EXCEEDED'
        ready, _, _ = select.select(streams, [], [], min(0.05, max(0, deadline-time.monotonic())))
        for stream in ready:
            chunk = os.read(stream.fileno(), 4096)
            if not chunk:
                streams.remove(stream)
                continue
            if sum(map(len, buffers.values())) + len(chunk) > limit:
                return buffers, 'OUTPUT_LIMIT_EXCEEDED'
            buffers[stream].extend(chunk)
    return buffers, None


def l01(ctx, evidence, scenario):
    need(False, 'L01 disabled: independent bounded supervision is not prepared')
    source = launcher_source()
    reader, writer = os.pipe2(os.O_CLOEXEC)
    proc = None
    pidfd = None
    identity = None
    observed_problem = None
    started = None
    buffers = None
    gate_closed_elapsed = None
    try:
        command = ['/usr/bin/python3', '-B', '-c', source, str(reader),
                   '/usr/bin/python3', '-I', '-B',
                   str(BUNDLE_ROOT / 'lab/ironclad/fixtures/process_fixture.py'),
                   '--unexpected-exec']
        # Exactly one real direct child, only after all gates and evidence setup.
        # No production paths/configuration, HOME, Docker context or credentials.
        assert_authorization_current(ctx)
        verify_environment(ctx)
        assert_authorization_current(ctx)
        started = time.monotonic()
        proc = subprocess.Popen(command, stdin=subprocess.DEVNULL,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                close_fds=True, pass_fds=(reader,), start_new_session=True,
                                cwd='/',
                                env={'PATH': '/usr/bin:/bin', 'LANG': 'C.UTF-8',
                                     'PYTHONNOUSERSITE': '1', 'PYTHONDONTWRITEBYTECODE': '1'})
        os.close(reader)
        reader = None
        try:
            pidfd = os.pidfd_open(proc.pid, 0)
            identity = child_identity(proc.pid)
        except (OSError, RuntimeError, ValueError):
            observed_problem = 'CHILD_IDENTITY_OR_PIDFD_UNAVAILABLE'
        evidence.event('child-started', dict(pid=proc.pid, identity=identity,
                                            boot_id=ctx['observed']['boot_id'], scenario=scenario,
                                            pidfd_opened=pidfd is not None))
        if scenario == 'eof':
            os.close(writer)
            writer = None
            gate_closed_elapsed = time.monotonic() - started
            if gate_closed_elapsed >= 4.0:
                observed_problem = observed_problem or 'EOF_BOUNDARY_TOO_CLOSE_TO_LAUNCHER_TIMEOUT'
            evidence.event('gate-closed-without-permit', dict(elapsed_seconds=gate_closed_elapsed))
        # Timeout scenario deliberately holds the writer open without writing.
        buffers, problem = collect_child(proc, started + ctx['manifest']['limits']['child_timeout_seconds'],
                                         ctx['manifest']['limits']['max_stream_bytes'])
        observed_problem = observed_problem or problem
        elapsed = time.monotonic() - started
        code = proc.poll()
        out = bytes(buffers[proc.stdout]).decode('utf-8', errors='replace')
        err = bytes(buffers[proc.stderr]).decode('utf-8', errors='replace')
        details = dict(pid=proc.pid, identity=identity, returncode=code, elapsed_seconds=elapsed,
                       stdout=out, stderr=err, issue=observed_problem,
                       gate_closed_elapsed_seconds=gate_closed_elapsed,
                       release_bytes_sent=0, daemon_contacted=False)
        if code != 125 or out or err:
            observed_problem = observed_problem or 'UNEXPECTED_LAUNCHER_RESULT'
        # A lower timing bound is meaningful only for timeout, never EOF.
        if scenario == 'timeout' and elapsed < 5.0:
            observed_problem = observed_problem or 'TIMEOUT_NOT_OBSERVED'
        if code is None:
            observed_problem = 'CHILD_STILL_ALIVE_OPERATOR_REVIEW'
        details['issue'] = observed_problem
        if observed_problem:
            evidence.stop(observed_problem)
            return evidence.finish('INCONCLUSIVE_OR_FAIL', details), 2
        result = 'L01_EOF_COMPONENT_PASS' if scenario == 'eof' else 'L01_TIMEOUT_COMPONENT_PASS'
        return evidence.finish(result, details), 0
    finally:
        # Closing the pipe requests no execution and permits EOF; never writes 1.
        # No terminate/kill fallback. A surviving child is quarantined, not signaled.
        if reader is not None:
            os.close(reader)
        if writer is not None:
            os.close(writer)
        if pidfd is not None:
            os.close(pidfd)
        if proc is not None:
            if proc.poll() is not None:
                proc.wait(timeout=0)
            for stream in (proc.stdout, proc.stderr):
                if stream is not None:
                    stream.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--case', choices=('L00', 'L01'), required=True)
    parser.add_argument('--scenario', choices=('eof', 'timeout'))
    parser.add_argument('--manifest', required=True)
    parser.add_argument('--authorization', required=True)
    parser.add_argument('--preflight-result')
    parser.add_argument('--execute', action='store_true', default=False)
    args = parser.parse_args()
    need((args.case == 'L00' and args.scenario is None) or
         (args.case == 'L01' and args.scenario is not None), 'invalid case/scenario pair')
    scope = 'L00' if args.case == 'L00' else 'L01_' + args.scenario.upper()
    ctx = authorize(args.manifest, args.authorization, scope, args.execute)
    verify_environment(ctx)
    if args.case == 'L01':
        verify_previous_l00(ctx, args.preflight_result)
    assert_authorization_current(ctx)
    with Evidence(ctx) as evidence:
        if args.case == 'L00':
            summary = evidence.finish('PROCESS_PREFLIGHT_ONLY', dict(
                daemon_identity_verified=False, docker_connection_attempted=False,
                docker_execution_allowed=False, independent_vm_operator_attested=True,
                note='NOT a completed Docker isolation qualification or Executor validation'))
            code = 3  # Partial preflight, never a full L00/Docker qualification.
        else:
            summary, code = l01(ctx, evidence, args.scenario)
    # No output while holding the evidence lock.
    print(json.dumps(summary, sort_keys=True))
    return code


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except (Refusal, OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError) as exc:
        print('IRONCLAD_REFUSED_OR_ABORTED:' + type(exc).__name__, file=sys.stderr)
        raise SystemExit(2)
