"""Isolated unit checks: temporary evidence only; no kernel observations or children."""
import json
import os
from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).absolute().parent))
import guard
import evidence
import harness


class EvidenceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.ctx = dict(environment_verified=True, scope='L00',
                        manifest_sha256='a'*64, authorization_sha256='b'*64,
                        authorization=dict(issued_unix=int(time.time())-1,
                                           expires_unix=int(time.time())+300),
                        observed={}, manifest=dict(run_id='c'*32,
                            roots=dict(evidence=str(self.root)), baseline=dict(artifact_lock_sha256='d'*64),
                            limits=dict(max_events=16, max_event_bytes=32768)))
        # Only the evidence directory opener is replaced. Production path and
        # ancestor guards remain strict; no /srv paths are opened by these tests.
        self.opener = patch.object(evidence, 'open_directory',
            side_effect=lambda path, private=False: os.open(self.root, os.O_RDONLY | os.O_DIRECTORY))
        self.opener.start()
        self.forbid_child = patch.object(harness.subprocess, 'Popen', side_effect=AssertionError('external child forbidden'))
        self.forbid_child.start()

    def tearDown(self):
        self.forbid_child.stop()
        self.opener.stop()
        self.tmp.cleanup()

    def test_success_clears_intent_after_summary(self):
        with evidence.Evidence(self.ctx) as collector:
            self.assertTrue((self.root/'.IRONCLAD_IN_PROGRESS').exists())
            collector.finish('PROCESS_PREFLIGHT_ONLY', {})
            self.assertTrue((self.root/'.IRONCLAD_IN_PROGRESS').exists())
        self.assertFalse((self.root/'.IRONCLAD_IN_PROGRESS').exists())
        self.assertEqual(json.loads((self.root/collector.name/'summary.json').read_text())['result'], 'PROCESS_PREFLIGHT_ONLY')

    def test_controller_loss_keeps_intent_and_blocks_next(self):
        collector = evidence.Evidence(self.ctx).__enter__()
        collector.close()  # Models lost controller without invoking __exit__.
        self.ctx['manifest']['run_id'] = 'e'*32
        with self.assertRaises(guard.Refusal):
            with evidence.Evidence(self.ctx):
                self.fail('unfinished run admitted')
        self.assertTrue((self.root/'.IRONCLAD_IN_PROGRESS').exists())

    def test_exclusion_while_owner_holds_lock(self):
        collector = evidence.Evidence(self.ctx).__enter__()
        try:
            with self.assertRaises(BlockingIOError):
                evidence.Evidence(self.ctx).__enter__()
        finally:
            collector.close()

    def test_exception_retains_intent_and_stop(self):
        with self.assertRaises(ValueError):
            with evidence.Evidence(self.ctx):
                raise ValueError('injected')
        self.assertTrue((self.root/'.IRONCLAD_IN_PROGRESS').exists())
        self.assertTrue((self.root/'.IRONCLAD_STOP').exists())

    def test_exception_after_finish_retains_intent(self):
        with self.assertRaises(ValueError):
            with evidence.Evidence(self.ctx) as collector:
                collector.finish('PROCESS_PREFLIGHT_ONLY', {})
                raise ValueError('after summary')
        self.assertTrue((self.root/'.IRONCLAD_IN_PROGRESS').exists())

    def test_marker_write_failure_is_fail_closed(self):
        with patch.object(evidence, 'write_all', side_effect=OSError('injected disk failure')):
            with self.assertRaises(OSError):
                evidence.Evidence(self.ctx).__enter__()
        self.assertTrue((self.root/'.IRONCLAD_IN_PROGRESS').exists())

    def test_expired_authorization_writes_nothing(self):
        self.ctx['authorization']['expires_unix'] = 0
        with self.assertRaises(guard.Refusal):
            evidence.Evidence(self.ctx).__enter__()
        self.assertEqual(list(self.root.iterdir()), [])

    def test_fixture_scope_cannot_enter_collector(self):
        self.ctx['scope'] = 'FIXTURE_GATEWAY'
        with self.assertRaises(guard.Refusal):
            evidence.Evidence(self.ctx).__enter__()
        self.assertEqual(list(self.root.iterdir()), [])

    def test_l01_rejected_before_any_child(self):
        with self.assertRaises(guard.Refusal):
            harness.l01(self.ctx, None, 'eof')


class GuardTests(unittest.TestCase):
    def test_only_l00_is_enabled(self):
        self.assertEqual(guard.SCOPES, {'L00'})
        with patch.dict(os.environ, IRONCLAD_ENABLE_EXECUTION='OPERATOR_APPROVED'):
            for scope in ['L01_EOF', 'L01_TIMEOUT', 'FIXTURE_PROCESS', 'FIXTURE_GATEWAY']:
                with self.subTest(scope=scope), self.assertRaises(guard.Refusal):
                    guard.authorize('unused', 'unused', scope, True)

    def test_fixed_roots(self):
        guard.validate_roots(dict(guard.FIXED_ROOTS))
        for role in guard.FIXED_ROOTS:
            roots = dict(guard.FIXED_ROOTS)
            roots[role] = '/home/other-private-directory'
            with self.subTest(role=role), self.assertRaises(guard.Refusal):
                guard.validate_roots(roots)

    def test_development_workspace_refused_before_reads(self):
        with patch.dict(os.environ, IRONCLAD_ENABLE_EXECUTION='OPERATOR_APPROVED'):
            with patch.object(guard, 'read_checked', side_effect=AssertionError('must not read controls')):
                with self.assertRaises(guard.Refusal):
                    guard.authorize('unused', 'unused', 'L00', True)


if __name__ == '__main__':
    unittest.main()
