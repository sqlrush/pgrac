"""Propagate the real collector's post-capture identity refusal to its judge."""
from contextlib import redirect_stdout
import io
import json
import subprocess
import sys
import unittest
from unittest.mock import patch

import m2_macos_observer
import test_m2_run15_observer as fixtures


class MacComponentJudgement(unittest.TestCase):
    def exercise(self, change):
        fixture = fixtures.Run15Observation()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        (fixture.f.run / 'm2-context-8.json').unlink()

        def debugger(command, **kwargs):
            if change == 'context':
                context = fixture.f.run / 'm2-context-4.json'
                value = json.loads(context.read_text())
                value['changed_after_attach'] = True
                context.write_text(json.dumps(value))
            elif change == 'birth':
                fixture.f.birth(1011, 999)
            return subprocess.CompletedProcess(command, 0,
                b'Process 1011 stopped\nframe #0: PostgresMain\nProcess 1011 detached\n')

        fixture.debugger = debugger
        observer = fixture.observer()
        observer.step()
        fixture.f.now = 30
        argv = ['mac-observer', '--base', str(fixture.f.base), '--manifest-sha', 'unused-by-factory']
        # Only the OS-specific constructor is replaced. The original collector
        # performs timeline, capture and identity rechecks; the real judge runs.
        with patch.object(m2_macos_observer, 'MacObserver', return_value=observer), \
                patch.object(sys, 'argv', argv), redirect_stdout(io.StringIO()):
            m2_macos_observer.main()
        return json.loads((fixture.f.base / 'OBSERVER-RESULT.json').read_text()), observer

    def test_context_changed_after_successful_attach_is_never_a_positive(self):
        result, observer = self.exercise('context')
        self.assertEqual(observer.records[0]['status'], 'IDENTITY_CHANGED_UNKNOWN')
        self.assertFalse(result['positive_attach'])

    def test_process_birth_changed_after_successful_attach_is_never_a_positive(self):
        result, observer = self.exercise('birth')
        self.assertEqual(observer.records[0]['status'], 'IDENTITY_CHANGED_UNKNOWN')
        self.assertFalse(result['positive_attach'])

    def test_same_identity_with_a_stack_keeps_receipt_unknown(self):
        result, observer = self.exercise(None)
        self.assertEqual(observer.records[0]['status'], 'PARTIAL_UNKNOWN')
        self.assertTrue(result['positive_attach'])
        self.assertEqual(observer.records[0]['receipt_identity'], 'UNKNOWN_UNINTERPRETED')


if __name__ == '__main__':
    unittest.main()
