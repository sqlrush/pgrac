"""Regression-controller checks only; these do not run a database.

Author: SqlRush <sqlrush@gmail.com>
Copyright (c) 2026, pgrac contributors
"""
import json
import argparse
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import m1_regression as m1
import run_multixact_lifecycle as native


class RegressionEntryTests(unittest.TestCase):
    def test_plan_uses_original_controllers_without_creating_output(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            argv = [sys.executable, '-B', str(Path(m1.__file__)), '--plan',
                    '--source', str(Path(m1.__file__).resolve().parents[4]),
                    '--build', str(root/'build'), '--install', str(root/'install'),
                    '--fixture-install', str(root/'fixtures'), '--vendor', str(root/'vendor'),
                    '--output', str(root/'results'), '--uid', '501', '--gid', '20',
                    '--expected-postgres-sha256', 'a'*64]
            reply = subprocess.run(argv, text=True, capture_output=True)
            self.assertEqual(reply.returncode, 0, reply.stderr)
            plan = json.loads(reply.stdout)
            self.assertEqual(plan['status'], 'NOT_RUN')
            self.assertEqual(plan['stage_acceptance'], 'NOT_READY')
            self.assertFalse((root/'results').exists())
            self.assertEqual([p['name'] for p in plan['phases']],
                             ['legacy', 'four-member', 'single-member'])
            self.assertEqual(plan['phases'][0]['cases'], [
                't/222_cluster_3_13_undo_cleaner.pl',
                't/270_cluster_4_12a_undo_record_segment_reclaim.pl',
                't/402_undo_cleaner_stall_retry_2node.pl'])
            self.assertEqual(plan['phases'][1]['cases'], [
                't/431_pre2_start_stop_restart.pl',
                't/445_pre2_multixact_maintenance.pl',
                't/447_pre2_multixact_restart.pl',
                't/448_pre2_multixact_cleared_votes.pl',
                't/453_pre2_vacuum_snapshot_cancel.pl',
                't/482_pre2_undo_active_slots.pl'])
            self.assertEqual(plan['phases'][2]['cases'],
                             ['t/461_pre2_single_shared_vacuum.pl'])
            self.assertIn('run_legacy.py', ' '.join(plan['phases'][0]['command']))
            self.assertIn('run_multixact_lifecycle.py', ' '.join(plan['phases'][1]['command']))
            self.assertIn('run_single_shared_vacuum.py', ' '.join(plan['phases'][2]['command']))
            self.assertNotIn('270_cluster_4_8ab_writeback_boundary_guards.pl', reply.stdout)

    def test_native_selection_retains_default_and_requires_447_before_448(self):
        self.assertEqual(native.select_cases('multixact', None), [
            't/447_pre2_multixact_restart.pl', 't/448_pre2_multixact_cleared_votes.pl'])
        self.assertEqual(native.select_cases('m1', None), m1.FOUR_MEMBER)
        for cases in ([], ['t/431_pre2_start_stop_restart.pl']*2,
                      ['t/448_pre2_multixact_cleared_votes.pl'],
                      ['t/448_pre2_multixact_cleared_votes.pl',
                       't/447_pre2_multixact_restart.pl'],
                      ['t/270_cluster_4_8ab_writeback_boundary_guards.pl']):
            with self.subTest(cases=cases), self.assertRaises(ValueError):
                native.select_cases('m1', cases)
        with self.assertRaises(ValueError):
            native.select_cases('multixact', ['t/482_pre2_undo_active_slots.pl'])

    def test_adapter_and_python_optimization_cannot_supply_acceptance(self):
        for name in ('PGRAC_PRE2_TEST_ADAPTER', 'PGRAC_PRE2_ENTRY_FILE',
                     'PGRAC_SINGLE_SHARED_FIXTURE', 'PYTHONOPTIMIZE'):
            with self.subTest(name=name), self.assertRaises(ValueError):
                m1.validate_environment({name: '/unqualified'})
        m1.validate_environment({})

    def test_strict_judge_reads_actual_prove_results(self):
        scripts = {
            'pass': 'use Test::More; pass("real assertion"); done_testing();',
            'fail': 'use Test::More; fail("real failure"); done_testing();',
            'skip': 'use Test::More; plan skip_all => "missing dependency";',
            'todo': 'use Test::More; { local $TODO="unfinished"; fail("pending"); } done_testing();',
            'bail': 'use Test::More; pass("before bailout"); BAIL_OUT("stopped");',
            'no-plan': 'print "ok 1 - incomplete\\n";',
        }
        with tempfile.TemporaryDirectory() as directory:
            for name, body in scripts.items():
                path = Path(directory)/(name+'.pl')
                path.write_text(body)
                reply = subprocess.run(['prove', '-v', str(path)], text=True,
                                       stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                with self.subTest(name=name):
                    self.assertEqual(m1.strict_tap(reply.stdout, reply.returncode), name == 'pass')
                    self.assertFalse(m1.strict_tap(reply.stdout, 124))

    def test_missing_phase_results_and_cleanup_debt_cannot_pass(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaises((ValueError, OSError)):
                m1.phase_passed('four-member', root, 0)
            (root/'result.json').write_text(json.dumps(dict(complete=True,
                error=None, cleanup_errors=[], cases=[])))
            self.assertFalse(m1.phase_passed('four-member', root, 0))
            (root/'result.json').write_text(json.dumps(dict(rc=0,
                error=None, cleanup_errors=['live DATA required error cleanup'])))
            self.assertFalse(m1.phase_passed('single-member', root, 0))

    def test_existing_output_and_wrong_binary_refuse_before_output_creation(self):
        # Filesystem preflight only: these bytes are never executed as a server.
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root/'install/bin').mkdir(parents=True)
            (root/'install/bin/postgres').write_bytes(b'identity input, not a server')
            args = argparse.Namespace(output=root/'results', install=root/'install',
                                      expected_postgres_sha256='a'*64)
            with self.assertRaisesRegex(ValueError, 'identity mismatch'):
                m1.preflight(args, {})
            self.assertFalse(args.output.exists())
            args.output.mkdir()
            marker = args.output/'retained'
            marker.write_bytes(b'keep')
            with self.assertRaisesRegex(ValueError, 'new directory'):
                m1.preflight(args, {})
            self.assertEqual(marker.read_bytes(), b'keep')


if __name__ == '__main__':
    unittest.main()
