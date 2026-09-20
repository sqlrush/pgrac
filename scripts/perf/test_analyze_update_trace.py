#!/usr/bin/env python3
"""Tests for the update phase trace analyzer.

Author: SqlRush <sqlrush@gmail.com>

The trace format is intentionally plain JSONL so an archived diagnostic run
can be analyzed without a running server or a matching PostgreSQL binary.
"""

import json
import pathlib
import sys
import unittest


SCRIPT_DIR = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

from analyze_update_trace import analyze_records, load_jsonl  # noqa: E402


class UpdateTraceAnalyzerTests(unittest.TestCase):
    def test_exclusive_partition_and_real_event_counts(self):
        records = [{"op_id": 1, "total_ns": 100, "status": "ok",
                    "phases": {"parent": 80, "child": 60},
                    "exclusive_phases": {"parent": 20, "child": 60},
                    "phase_events": {"parent": 1, "child": 3},
                    "unattributed_ns": 20, "accounting_errors": 0}]
        report = analyze_records(records)
        self.assertEqual(report["exclusive_phase_rank"][0]["phase"], "child")
        self.assertEqual(report["phase_rank"][1]["events"], 3)
        self.assertEqual(report["unattributed_ns"], 20)
        self.assertEqual(report["accounted_ops"], 1)
        self.assertEqual(report["latency_basis"], "server_executor")

    def test_broken_conservation_is_not_a_valid_ranking(self):
        with self.assertRaisesRegex(ValueError, "conservation"):
            analyze_records([{"op_id": 1, "total_ns": 100, "status": "ok",
                              "exclusive_phases": {"cr": 110}, "unattributed_ns": 0}])

    def test_accounting_fault_preserved_and_excluded_from_exclusive_rank(self):
        report = analyze_records([{"op_id": 1, "total_ns": 100, "status": "ok",
                                   "accounting_errors": 1, "exclusive_phases": {"cr": 50},
                                   "unattributed_ns": 50}])
        self.assertEqual(report["accounting_fault_ops"], 1)
        self.assertEqual(report["completed_ops"], 1)
        self.assertEqual(report["exclusive_phase_rank"], [])

    def test_p80_tail_and_phase_rank(self):
        records = [
            {"op_id": i, "total_ns": i * 100, "status": "ok",
             "phases": {"cr": i * 10, "undo": i * 2}}
            for i in range(1, 11)
        ]

        report = analyze_records(records)

        self.assertEqual(report["completed_ops"], 10)
        self.assertEqual(report["p80_total_ns"], 800)
        self.assertEqual(report["tail_ops"], 2)
        self.assertEqual(report["tail_wall_ns"], 1900)
        self.assertEqual(report["phase_rank"][0]["phase"], "cr")
        self.assertEqual(report["phase_rank"][0]["total_ns"], 550)
        self.assertAlmostEqual(report["tail_wall_share"], 1900 / 5500)

    def test_non_ok_records_are_retained_but_not_in_latency_denominator(self):
        records = [
            {"op_id": 1, "total_ns": 100, "status": "ok", "phases": {}},
            {"op_id": 2, "total_ns": 200, "status": "error", "phases": {}},
            {"op_id": 3, "total_ns": 300, "status": "ok", "phases": {}},
        ]

        report = analyze_records(records)

        self.assertEqual(report["records_seen"], 3)
        self.assertEqual(report["completed_ops"], 2)
        self.assertEqual(report["status_counts"], {"ok": 2, "error": 1})
        self.assertEqual(report["p80_total_ns"], 300)

    def test_jsonl_loader_rejects_malformed_phase_value(self):
        path = SCRIPT_DIR / ".tmp-update-trace-malformed.jsonl"
        try:
            path.write_text(json.dumps({"op_id": 1, "total_ns": 1,
                                        "status": "ok", "phases": []}) + "\n")
            with self.assertRaises(ValueError):
                load_jsonl(path)
        finally:
            path.unlink(missing_ok=True)


if __name__ == "__main__":
    unittest.main()
