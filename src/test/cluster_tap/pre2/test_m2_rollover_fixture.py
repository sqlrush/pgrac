"""Exercise the ordinary rollover workload and its preflight without a VM."""
import json
from pathlib import Path
import subprocess
import unittest

HERE = Path(__file__).resolve().parent


def invoke(action, limits=None, stats=None):
    code = r'''use M2CanonicalRollover qw(rollover_workload check_rollover_budget);
use JSON::PP;
my $input=decode_json(do { local $/; <STDIN> });
my $result=eval {
 $input->{action} eq 'workload' ? rollover_workload()
  : check_rollover_budget($input->{limits},$input->{stats});
};
print encode_json({result=>$result,error=>$@});
'''
    result = subprocess.run(['perl', '-I'+str(HERE), '-e', code],
                            input=json.dumps(dict(action=action, limits=limits, stats=stats)),
                            text=True, capture_output=True, timeout=10)
    if result.returncode:
        raise AssertionError(result.stderr)
    return json.loads(result.stdout)


class RolloverFixture(unittest.TestCase):
    def setUp(self):
        # Independent, hand-computed examples. Native runs must supply their
        # compiled sizes/current GUC and actual PLAIN table tuple statistics.
        self.limits = dict(segment_bytes=67108864, inline_capacity_bytes=1024,
                           update_payload_header_bytes=12, record_header_bytes=64,
                           history_trailer_bytes=136, record_hard_cap_bytes=8000)
        self.stats = dict(rows=65536, min_tuple_bytes=932, max_tuple_bytes=932)

    def test_workload_fits_one_receipt_and_budgets_more_than_one_segment(self):
        workload = invoke('workload')['result']
        # PLAIN text, two non-null int4 values: aligned heap header24 +8 +4.
        old_tuple = 36 + 32 * workload['pad_repeats']
        self.assertLessEqual(12 + old_tuple, 1024)
        self.assertGreater(workload['rows'] * (64 + 12 + old_tuple + 136), 67108864)

    def test_preflight_returns_measured_payload_and_conservative_record_budget(self):
        response = invoke('check', self.limits, self.stats)
        self.assertEqual(response['error'], '')
        self.assertEqual(response['result'], dict(max_payload_bytes=944,
                         max_record_bytes=1144, min_total_record_bytes=74973184))

    def test_old_large_plain_tuple_refuses_before_the_update(self):
        response = invoke('check', self.limits,
                          dict(rows=10000, min_tuple_bytes=6916, max_tuple_bytes=6916))
        self.assertIn('payload', response['error'])
        self.assertIsNone(response['result'])

    def test_small_rows_require_enough_ordinary_records_for_a_full_segment(self):
        stats = dict(self.stats, rows=10000)
        response = invoke('check', self.limits, stats)
        self.assertIn('segment', response['error'])
        # An exactly full byte budget does not prove a subsequent allocation.
        limits = dict(self.limits, segment_bytes=11440000)
        response = invoke('check', limits, stats)
        self.assertIn('segment', response['error'])

    def test_limits_use_the_current_session_and_include_the_history_trailer(self):
        response = invoke('check', dict(self.limits, inline_capacity_bytes=943), self.stats)
        self.assertIn('payload', response['error'])
        response = invoke('check', dict(self.limits, record_hard_cap_bytes=1143), self.stats)
        self.assertIn('record', response['error'])
        response = invoke('check', dict(self.limits, inline_capacity_bytes=944,
                                       record_hard_cap_bytes=1144), self.stats)
        self.assertEqual(response['error'], '')

    def test_missing_or_inconsistent_runtime_facts_are_rejected(self):
        cases = [dict(self.stats, rows=0), dict(self.stats, min_tuple_bytes=933),
                 dict(self.stats, max_tuple_bytes=None), dict(self.stats, rows='unknown')]
        for stats in cases:
            with self.subTest(stats=stats):
                self.assertIn('facts', invoke('check', self.limits, stats)['error'])
        for field in self.limits:
            limits = dict(self.limits)
            del limits[field]
            with self.subTest(missing=field):
                self.assertIn('facts', invoke('check', limits, self.stats)['error'])


if __name__ == '__main__':
    unittest.main()
