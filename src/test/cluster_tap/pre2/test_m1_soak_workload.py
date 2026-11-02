"""Test workload planning only; no database or native acceptance is simulated.

Author: SqlRush <sqlrush@gmail.com>
Copyright (c) 2026, pgrac contributors
"""
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import run_multixact_lifecycle as native


class SoakWorkloadTests(unittest.TestCase):
    def test_plan_preserves_one_hour_per_case_without_starting_a_cohort(self):
        script = Path(__file__).with_name('m1_soak_workload.py')
        with tempfile.TemporaryDirectory() as directory:
            reply = subprocess.run([sys.executable, '-B', str(script)],
                cwd=directory, text=True, capture_output=True)
            self.assertEqual(reply.returncode, 0, reply.stderr)
            plan = json.loads(reply.stdout)
            self.assertEqual(list(Path(directory).iterdir()), [])
        self.assertEqual(plan['status'], 'NOT_RUN')
        self.assertEqual(plan['p07'], 'NOT_READY')
        self.assertEqual(plan['stage_acceptance'], 'NOT_READY')
        self.assertEqual([case['clients'] for case in plan['cases']], [16, 48, 64])
        for case in plan['cases']:
            self.assertEqual(case['pgbench_arguments'], [
                '-n', '-M', 'simple', '-j', '2', '-c', str(case['clients']),
                '-T', '3600', '--max-tries=1', '-l'])
        self.assertEqual(plan['setup_sql'].count('CREATE TABLE '), 64)
        self.assertEqual(plan['setup_sql'].count('INSERT INTO '), 64)
        self.assertIn('CREATE TABLE m1_soak_0 ', plan['setup_sql'])
        self.assertIn('INSERT INTO m1_soak_63 VALUES (63, 0);', plan['setup_sql'])
        self.assertEqual(plan['row_cut_sql'].count('SELECT client_id, value FROM '), 64)
        self.assertTrue(plan['row_cut_sql'].endswith(' ORDER BY client_id;'))
        self.assertNotIn('cluster_dump_state', json.dumps(plan))
        self.assertEqual(plan['writer_node'], 0)
        self.assertEqual(plan['readers'], [0, 1, 2, 3])

    def test_native_matrix_uses_fresh_case_roots_and_separate_deadlines(self):
        try:
            runs = native.case_runs('m1-soak-workload', None)
        except (AttributeError, KeyError) as error:
            self.fail('native controller has no workload matrix: '+str(error))
        self.assertEqual([(run['clients'], run['timeout']) for run in runs],
                         [(16, 5400), (48, 5400), (64, 5400)])
        self.assertEqual([run['directory'] for run in runs], [
            '483_pre2_m1_soak_workload-16', '483_pre2_m1_soak_workload-48',
            '483_pre2_m1_soak_workload-64'])
        self.assertEqual({run['case'] for run in runs},
                         {'t/483_pre2_m1_soak_workload.pl'})
        legacy = native.case_runs('multixact', None)
        self.assertEqual([run['case'] for run in legacy], [
            't/447_pre2_multixact_restart.pl', 't/448_pre2_multixact_cleared_votes.pl'])
        self.assertTrue(all(run['clients'] is None and run['timeout'] == 1200
                            for run in legacy))
        self.assertEqual(legacy[0]['directory'], '447_pre2_multixact_restart')
        m1 = native.case_runs('m1', None)
        self.assertEqual(len(m1), 6)
        self.assertTrue(all(run['timeout'] == 1200 for run in m1))
        with self.assertRaises(ValueError):
            native.case_runs('m1', ['t/483_pre2_m1_soak_workload.pl'])


if __name__ == '__main__':
    unittest.main()
