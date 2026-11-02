"""Native offset-page entry checks only; no database result is simulated.

Author: SqlRush <sqlrush@gmail.com>
Copyright (c) 2026, pgrac contributors
"""
import argparse
import json
import re
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import m1_regression as regression
import run_multixact_lifecycle as native


class MultiXactPagesEntryTests(unittest.TestCase):
    def lock_plan(self, rows):
        # Run the actual pure fixture planner, without SQL/client substitutes.
        tap = Path(__file__).resolve().parent.parent/'t/487_pre2_multixact_offset_pages.pl'
        match = re.search(r'(?ms)^sub page_lock_plan\n\{.*?^\}', tap.read_text())
        self.assertIsNotNone(match, 'native lock schedule must retain both endpoint partners')
        program = ('use strict; use warnings; use JSON::PP; '+match.group(0)
                   +'; print JSON::PP->new->encode(page_lock_plan(decode_json($ARGV[0])));')
        return subprocess.run(['perl', '-e', program, '--', json.dumps(rows)],
                              text=True, capture_output=True)

    def test_lock_schedule_keeps_two_live_endpoints_across_the_crossing(self):
        for rows in (2, 3, 129, 513):
            with self.subTest(rows=rows):
                reply = self.lock_plan(rows)
                self.assertEqual(reply.returncode, 0, reply.stderr)
                steps = json.loads(reply.stdout)
                self.assertEqual([step['row'] for step in steps], list(range(rows)))
                active = set()
                checked = []
                for step in steps:
                    partner = step['partner']
                    self.assertNotIn(partner, active, 'BEGIN cannot replace a live endpoint transaction')
                    active.add(partner)
                    if step['check_members']:
                        checked.append((step['row'], partner))
                        self.assertFalse(step['commit'], 'an endpoint must still have two active members')
                    if step['commit']:
                        active.remove(partner)
                self.assertEqual(checked, [(0, 'anchor'), (rows-1, 'rolling')])
                self.assertEqual(active, {'anchor', 'rolling'})

    def test_lock_schedule_rejects_missing_or_impossible_crossing_lengths(self):
        for rows in (None, 0, 1, -1, 2.5, 'bad', 514):
            with self.subTest(rows=rows):
                reply = self.lock_plan(rows)
                self.assertNotEqual(reply.returncode, 0)
                self.assertIn('invalid native page-lock row count', reply.stderr)
                self.assertEqual(reply.stdout, '')

    def test_member_expectations_sort_raw_xids_before_appending_the_mode(self):
        # Execute each actual TAP expression; no database/holder is simulated.
        tap = Path(__file__).resolve().parent.parent/'t'
        for name in ('486_pre2_multixact_continuation.pl', '487_pre2_multixact_offset_pages.pl'):
            line = next(line.strip() for line in (tap/name).read_text().splitlines()
                        if 'expected_members => join(' in line)
            expression = re.fullmatch(r'expected_members => (.+)\};', line).group(1)
            for first, second, expected in [(64, 640, '64:keysh\n640:keysh'),
                                             (640, 64, '64:keysh\n640:keysh'),
                                             (16, 160, '16:keysh\n160:keysh')]:
                with self.subTest(name=name, first=first, second=second):
                    program = ('my @raw=('+str(first)+','+str(second)+'); '
                        'my $a={raw_xid=>$raw[0]}; my $b={raw_xid=>$raw[1]}; print '+expression+';')
                    reply = subprocess.run(['perl', '-e', program], text=True, capture_output=True)
                    self.assertEqual(reply.returncode, 0, reply.stderr)
                    self.assertEqual(reply.stdout, expected)

    def test_page_case_uses_original_native_budget_without_case_substitution(self):
        try:
            runs = native.case_runs('m1-mx-pages', None)
        except (KeyError, AttributeError) as error:
            self.fail('native occupied offset-page entry missing: '+str(error))
        self.assertEqual(runs, [dict(case='t/487_pre2_multixact_offset_pages.pl',
            clients=None, timeout=1200, directory='487_pre2_multixact_offset_pages')])
        for suite, cases in [('m1-mx-pages', ['t/486_pre2_multixact_continuation.pl']),
                             ('m1', ['t/487_pre2_multixact_offset_pages.pl'])]:
            with self.assertRaises(ValueError):
                native.case_runs(suite, cases)
        reply = subprocess.run([sys.executable, '-B', str(Path(native.__file__)), '--help'],
                               text=True, capture_output=True)
        self.assertEqual(reply.returncode, 0, reply.stderr)
        self.assertIn('m1-mx-pages', reply.stdout)

    def test_extended_includes_original_page_case_and_core_stays_unchanged(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            args = argparse.Namespace(source=Path(__file__).resolve().parents[4],
                build=root/'build', install=root/'install', fixture_install=root/'fixtures',
                vendor=root/'vendor', output=root/'output', uid=501, gid=20,
                expected_postgres_sha256='a'*64, suite='extended')
            plan = regression.plan(args)
            self.assertIn('mx-pages', [p['name'] for p in plan['phases']],
                          'extended regression must consume the native page-preservation case')
            phase = next(p for p in plan['phases'] if p['name'] == 'mx-pages')
            self.assertEqual(phase['cases'], ['t/487_pre2_multixact_offset_pages.pl'])
            self.assertEqual(phase['scope'], 'multixact-offset-page-live-members')
            self.assertEqual(phase['command'][phase['command'].index('--suite')+1], 'm1-mx-pages')
            self.assertEqual(plan['subcheck_gates']['x04'], 'NOT_READY')
            args.suite = 'core'
            self.assertEqual([p['name'] for p in regression.plan(args)['phases']],
                             ['legacy', 'four-member', 'single-member'])
            self.assertFalse(args.output.exists())


if __name__ == '__main__':
    unittest.main()
