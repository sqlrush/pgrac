#!/usr/bin/env python3
"""Author: SqlRush. Tests for the pg_cluster_state update-trace adapter."""

import json
import pathlib
import sys
import tempfile
import unittest


SCRIPT_DIR = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

from collect_update_trace import collect, collect_window, load_dump  # noqa: E402


class UpdateTraceCollectorTests(unittest.TestCase):
    def test_second_window_excludes_prior_records_without_reset(self):
        def row(op, start, status=0):
            return {"category": "update_trace", "key": f"op.{op}", "value":
                    f"node=0;backend=1;pid=10;status={status};start_ns={start};end_ns={start+5}"}
        before = self._dump([row(1, 10)])
        after = self._dump([row(1, 10), row(2, 20, 1), row(3, 30)])
        self.assertEqual([r['source_op_id'] for r in collect_window([before], [after])], [2, 3])

    def test_window_rejects_reset_or_an_inflight_start_boundary(self):
        def row(op, start, status=0):
            return {"category": "update_trace", "key": f"op.{op}", "value":
                    f"node=0;backend=1;pid=10;status={status};start_ns={start};end_ns={start+5}"}
        before = self._dump([row(1, 10)])
        reset = self._dump([row(1, 50)])
        with self.assertRaisesRegex(ValueError, 'changed'):
            collect_window([before], [reset])
        inflight = self._dump([row(1, 10, 2)])
        with self.assertRaisesRegex(ValueError, 'incomplete'):
            collect_window([inflight], [before])

    def test_incomplete_record_and_exclusive_fields(self):
        path = self._dump([
            {"category": "update_trace", "key": "op.1", "value":
             "node=0;backend=1;pid=10;status=2;start_ns=100;end_ns=0;"
             "backend_sequence=1;affected_rows=0;unattributed_ns=0;accounting_errors=0"},
            {"category": "update_trace", "key": "op.2", "value":
             "node=0;backend=1;pid=10;status=0;start_ns=200;end_ns=300;"
             "backend_sequence=2;affected_rows=1;unattributed_ns=20;accounting_errors=0;"
             "phase.cr=80,3;exclusive.cr=80"},
        ])
        rows = load_dump(path)
        self.assertEqual(rows[0]["status"], "incomplete")
        self.assertEqual(rows[1]["exclusive_phases"], {"cr": 80})
        self.assertEqual(rows[1]["backend_sequence"], 2)

    def _dump(self, rows):
        handle = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
        with handle:
            json.dump(rows, handle)
        self.addCleanup(lambda: pathlib.Path(handle.name).unlink(missing_ok=True))
        return pathlib.Path(handle.name)

    def test_normalizes_node_local_ids_and_tm_updated(self):
        path = self._dump([
            {"category": "update_trace", "key": "record_capacity", "value": "32768"},
            {"category": "update_trace", "key": "op.7", "value":
             "node=2;backend=3;pid=91;status=103;start_ns=10;end_ns=35;"
             "phase.r_gcs_s_request=20,1;phase.i_tx_wait=2,1"},
        ])
        records = load_dump(path)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["op_id"], (2 << 32) | 7)
        self.assertEqual(records[0]["status"], "retry_tm_updated")
        self.assertEqual(records[0]["result"], "TM_Updated")
        self.assertEqual(records[0]["total_ns"], 25)
        self.assertEqual(records[0]["phases"]["r_gcs_s_request"], 20)

    def test_collect_sorts_by_start_time(self):
        first = self._dump([
            {"category": "update_trace", "key": "op.1", "value":
             "node=0;backend=1;pid=10;status=0;start_ns=20;end_ns=30"},
        ])
        second = self._dump([
            {"category": "update_trace", "key": "op.1", "value":
             "node=1;backend=1;pid=11;status=0;start_ns=10;end_ns=15"},
        ])
        records = collect([first, second])
        self.assertEqual([record["node_id"] for record in records], [1, 0])

    def test_rejects_malformed_phase(self):
        path = self._dump([
            {"category": "update_trace", "key": "op.1", "value":
             "node=0;backend=1;pid=10;status=0;start_ns=1;end_ns=2;phase.bad=7"},
        ])
        with self.assertRaises(ValueError):
            load_dump(path)


if __name__ == "__main__":
    unittest.main()
