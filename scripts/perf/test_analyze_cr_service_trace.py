#!/usr/bin/env python3
"""Author: SqlRush. Exact request linkage and non-overlapping CR slot cost."""
import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from analyze_cr_service_trace import analyze_events


class CrServiceTraceTests(unittest.TestCase):
    def test_exact_link_and_state_conservation(self):
        def event(t, kind, value=0):
            return dict(node=1, pid=20, requester=0, backend=7, request=8,
                        slot=2, generation=4, stamp_ns=t, kind=kind, value=value, op=0)
        link = dict(event(0, 1), node=0, pid=10, op=3)
        events = [link, event(10, 2, 5), event(20, 2, 6), event(21, 3, 6),
                  event(23, 4), event(24, 2, 7), event(30, 2, 8),
                  event(60, 2, 9), event(70, 3, 9), event(73, 4),
                  event(74, 2, 10), event(76, 2, 14), event(80, 5)]
        report = analyze_events(events, {(0, 3)})
        self.assertEqual(report['complete_episodes'], 1)
        self.assertEqual(report['slot_wall_ns'], 70)
        self.assertEqual(sum(report['phases_ns'].values()), 70)
        self.assertEqual(report['phases_ns']['queued'], 10)
        self.assertEqual(report['phases_ns']['undo_inflight'], 30)
        self.assertEqual(report['phases_ns']['undo_ready_wait'], 10)
        self.assertEqual(report['phases_ns']['build_call'], 5)
        self.assertEqual(analyze_events(events, {(0, 4)})['complete_episodes'], 0)
        # Only the same LMS process can block this builder; another worker
        # has independent progress even though it is on the same node.
        blocked = events + [event(12, 7, 2), event(18, 8, 2),
                            dict(event(60, 7, 2), pid=21), dict(event(69, 8, 2), pid=21)]
        report = analyze_events(blocked, {(0, 3)})
        self.assertEqual(report['eligible_wait_inside_legacy_serve_ns'], 6)

    def test_missing_end_is_not_silently_counted_complete(self):
        events = [dict(node=1, pid=20, requester=0, backend=7, request=8,
                       slot=2, generation=4, stamp_ns=10, kind=2, value=5, op=0)]
        report = analyze_events(events, None)
        self.assertEqual(report['incomplete_episodes'], 1)
        self.assertEqual(report['slot_wall_ns'], 0)

    def test_terminal_state_inside_failed_build_keeps_call_time(self):
        def event(t, kind, value=0):
            return dict(node=1, pid=20, requester=0, backend=7, request=8,
                        slot=2, generation=4, stamp_ns=t, kind=kind, value=value, op=0)
        events = [event(10, 2, 5), event(20, 3, 6), event(25, 2, 12),
                  event(30, 4), event(40, 2, 14), event(45, 5)]
        report = analyze_events(events, None)
        self.assertEqual(report['phases_ns']['build_call'], 10)
        self.assertEqual(report['phases_ns']['terminal_wait'], 10)
        self.assertEqual(sum(report['phases_ns'].values()), 35)

    def test_conflicting_request_link_is_rejected(self):
        event = dict(node=0, pid=20, requester=0, backend=7, request=8,
                     slot=0, generation=0, stamp_ns=10, kind=1, value=0, op=1)
        with self.assertRaisesRegex(ValueError, 'conflicting'):
            analyze_events([event, dict(event, op=2)], None)

    def test_origin_residence_joins_dependency_without_cross_node_clock_subtraction(self):
        def event(t, kind, value=0, **kwargs):
            e = dict(node=1, pid=20, requester=0, backend=7, request=8,
                     slot=2, generation=4, stamp_ns=t, kind=kind, value=value, op=0)
            e.update(kwargs)
            return e
        events = [event(0, 1, node=0, op=3),
                  event(10, 10, 4294967294, dependency=99),
                  event(900000, 9, 0x401, node=2, requester=1, backend=-2,
                        request=99, duration_ns=200, work_ns=20),
                  event(900010, 9, 0x410, node=2, requester=1, backend=-2,
                        request=99, duration_ns=10, work_ns=8)]
        report = analyze_events(events, {(0, 3)})
        self.assertEqual(report['origin_phase_ns'], {'4:1': 200, '4:16': 10})
        self.assertEqual(report['origin_work_ns'], {'4:1': 20, '4:16': 8})
        self.assertEqual(report['origin_dependency_requests'], 1)
        self.assertEqual(report['origin_completed_requests'], 1)
        self.assertEqual(report['complete_episodes'], 0)
        self.assertEqual(analyze_events(events, {(0, 4)})['origin_phase_ns'], {})
        with self.assertRaisesRegex(ValueError, 'origin.*work'):
            analyze_events(events+[event(900020, 9, 0x401, node=2, requester=1,
                backend=65535, request=99, duration_ns=10, work_ns=20)], None)

    def test_abandoned_origin_is_not_a_completed_phase(self):
        common = dict(node=1, pid=20, requester=0, backend=7, request=8,
                      slot=2, generation=4, stamp_ns=10, op=0)
        events = [dict(common, kind=10, value=65535, dependency=99),
                  dict(common, kind=11, value=0x401, node=2, requester=1,
                       backend=65535, request=99)]
        report = analyze_events(events)
        self.assertEqual(report['origin_incomplete_intervals'], 1)
        self.assertEqual(report['origin_phase_ns'], {})
        self.assertEqual(report['origin_completed_requests'], 0)


if __name__ == '__main__':
    unittest.main()
