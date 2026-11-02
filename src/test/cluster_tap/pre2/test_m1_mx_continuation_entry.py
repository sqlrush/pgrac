"""Native MultiXact continuation selection; no database success is simulated.

Author: SqlRush <sqlrush@gmail.com>
Copyright (c) 2026, pgrac contributors
"""
from pathlib import Path
import subprocess
import sys
import unittest

import run_multixact_lifecycle as native


class MultiXactContinuationEntryTests(unittest.TestCase):
    def test_continuation_uses_original_native_lifecycle_without_case_substitution(self):
        try:
            runs = native.case_runs('m1-mx-continuation', None)
        except (KeyError, AttributeError) as error:
            self.fail('native MultiXact continuation entry missing: '+str(error))
        self.assertEqual(runs, [dict(case='t/486_pre2_multixact_continuation.pl',
            clients=None, timeout=1200, directory='486_pre2_multixact_continuation')])
        with self.assertRaises(ValueError):
            native.case_runs('m1-mx-continuation', ['t/447_pre2_multixact_restart.pl'])
        with self.assertRaises(ValueError):
            native.case_runs('m1', ['t/486_pre2_multixact_continuation.pl'])
        reply = subprocess.run([sys.executable, '-B', str(Path(native.__file__)), '--help'],
                               text=True, capture_output=True)
        self.assertEqual(reply.returncode, 0, reply.stderr)
        self.assertIn('m1-mx-continuation', reply.stdout)

    def test_existing_suites_and_case_envelopes_stay_unchanged(self):
        self.assertEqual(native.select_cases('m1', None), [
            't/431_pre2_start_stop_restart.pl', 't/445_pre2_multixact_maintenance.pl',
            't/447_pre2_multixact_restart.pl', 't/448_pre2_multixact_cleared_votes.pl',
            't/453_pre2_vacuum_snapshot_cancel.pl', 't/482_pre2_undo_active_slots.pl'])
        self.assertEqual(native.select_cases('multixact', None), [
            't/447_pre2_multixact_restart.pl', 't/448_pre2_multixact_cleared_votes.pl'])
        for suite, case in [('m1-retention', 't/484_pre2_retained_snapshot.pl'),
                            ('m1-idle-freeze', 't/485_pre2_idle_freeze_restart.pl')]:
            self.assertEqual(native.case_runs(suite, None), [dict(
                case=case, clients=None, timeout=1200, directory=Path(case).stem)])
        self.assertEqual([(case['clients'], case['timeout'])
                         for case in native.case_runs('m1-soak-workload', None)],
                         [(16, 5400), (48, 5400), (64, 5400)])


if __name__ == '__main__':
    unittest.main()
