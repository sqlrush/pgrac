"""Offline capacity evidence and native selection checks; no database is simulated.

Author: SqlRush <sqlrush@gmail.com>
Copyright (c) 2026, pgrac contributors
"""
import copy
import json
from pathlib import Path
import subprocess
import sys
import unittest

import run_multixact_lifecycle as native


def cuts():
    before = [dict(node=n, postmaster_start='2026-11-25 00:00:00+08',
        stats=dict(capacity=2048, live_entries='100', peak_live_entries='100',
                   binding_generation='100', capacity_wait_count='0',
                   reclaim_success_count='0')) for n in range(4)]
    after = copy.deepcopy(before)
    after[0]['stats'].update(peak_live_entries='2048', binding_generation='10000',
                            capacity_wait_count='1', reclaim_success_count='8000')
    return before, after


class CapacityEvidenceTests(unittest.TestCase):
    def judge(self, before, after):
        from pcm_capacity_evidence import judge
        return judge(dict(before=before, after=after))

    def test_complete_same_lifetime_counter_evidence(self):
        result = self.judge(*cuts())
        self.assertTrue(result['passed'], result)
        self.assertEqual(result['wait_delta'], '1')

    def test_uint64_delta_is_exact_above_double_precision(self):
        before, after = cuts()
        before[0]['stats']['capacity_wait_count'] = str(2**64 - 2)
        after[0]['stats']['capacity_wait_count'] = str(2**64 - 1)
        self.assertEqual(self.judge(before, after)['wait_delta'], '1')

    def test_native_cli_consumes_the_same_json_cuts(self):
        before, after = cuts()
        reply = subprocess.run([sys.executable, '-B',
            str(Path(__file__).with_name('pcm_capacity_evidence.py'))],
            input=json.dumps(dict(before=before, after=after)), text=True, capture_output=True)
        self.assertEqual(reply.returncode, 0, reply.stderr)
        self.assertEqual(json.loads(reply.stdout), self.judge(before, after))

    def test_no_wait_or_no_reclaim_cannot_qualify(self):
        for field in ('capacity_wait_count', 'reclaim_success_count'):
            with self.subTest(field=field):
                before, after = cuts()
                after[0]['stats'][field] = '0'
                self.assertFalse(self.judge(before, after)['passed'])

    def test_not_full_and_wrong_capacity_are_rejected(self):
        for field, value in (('peak_live_entries', '2047'), ('capacity', 65536)):
            with self.subTest(field=field):
                before, after = cuts()
                after[0]['stats'][field] = value
                self.assertFalse(self.judge(before, after)['passed'])

    def test_birth_change_and_counter_reset_are_rejected(self):
        before, after = cuts()
        after[0]['postmaster_start'] = '2026-11-25 00:01:00+08'
        self.assertFalse(self.judge(before, after)['passed'])
        before, after = cuts()
        before[0]['stats']['capacity_wait_count'] = '2'
        self.assertFalse(self.judge(before, after)['passed'])

    def test_missing_duplicate_or_reordered_members_are_rejected(self):
        before, after = cuts()
        for members in (after[:3], [after[0]] * 4, list(reversed(after))):
            self.assertFalse(self.judge(before, members)['passed'])
        del after[0]['stats']['capacity_wait_count']
        self.assertFalse(self.judge(before, after)['passed'])

    def test_malformed_or_out_of_range_counter_is_rejected(self):
        for value in (None, True, 1.5, '-1', '01', '+1', str(2**64)):
            with self.subTest(value=value):
                before, after = cuts()
                after[0]['stats']['capacity_wait_count'] = value
                self.assertFalse(self.judge(before, after)['passed'])

    def test_live_entries_cannot_exceed_fixed_capacity(self):
        before, after = cuts()
        after[0]['stats']['live_entries'] = '2049'
        self.assertFalse(self.judge(before, after)['passed'])


class CapacityEntryTests(unittest.TestCase):
    def test_capacity_has_its_own_single_case(self):
        try:
            runs = native.case_runs('p145-grd-capacity', None)
        except KeyError:
            self.fail('ordinary INSERT capacity case has no native entry')
        self.assertEqual(runs, [dict(case='t/488_pre2_grd_capacity_insert.pl',
            clients=None, timeout=1200, directory='488_pre2_grd_capacity_insert')])
        with self.assertRaises(ValueError):
            native.case_runs('p145-grd-capacity', ['t/482_pre2_undo_active_slots.pl'])
        with self.assertRaises(ValueError):
            native.case_runs('m1', ['t/488_pre2_grd_capacity_insert.pl'])
        reply = subprocess.run([sys.executable, '-B', str(Path(native.__file__)), '--help'],
                               text=True, capture_output=True)
        self.assertEqual(reply.returncode, 0, reply.stderr)
        self.assertIn('p145-grd-capacity', reply.stdout)


if __name__ == '__main__':
    unittest.main()
