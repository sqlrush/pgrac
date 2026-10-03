"""Harness-only status tests; no mock adapter can earn acceptance credit."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from run_legacy import classify
from acceptance import Blocked, run_case, members


class StatusTests(unittest.TestCase):
    def test_leave_cannot_pass_with_blocked_reply_or_another_operations_trace(self):
        request = dict(version=1, operation_kind="leave", target_node=1,
                       guest_uuid="11111111-1111-1111-1111-111111111111",
                       expected_formation=3, operation_generation=4,
                       expected_old_incarnation=5, reserved_new_incarnation=0)
        class FixtureDouble:
            def __init__(self, status):
                self.status = status
            def sql(self, node, sql):
                return "30000" if sql.startswith("SHOW") else "3|1,2,"
            def call(self, op, **args):
                if op == "membership_request":
                    return {"request": request}
                if op == "membership_trace":
                    kinds = ["reserve", "drain", "peer_failed", "abort", "abort_closed",
                             "fail_stop_reserve", "fail_stop_complete"]
                    return dict(pi_obligations_retired=True, leave_key="other", failure_key="other_failure",
                                request=dict(request, target_node=0), failed={"node": 0},
                                recovery_budget_ms=1000,
                                events=[dict(kind=k, request="other" if i < 5 else "other_failure", at_ms=i)
                                        for i, k in enumerate(kinds)])
                return {}
            def barrier(self, *args):
                pass
            def async_sql(self, *args):
                return None
            def hit(self, *args):
                return {}
            def crash(self, *args):
                return {"node": 2, "old_incarnation": 8, "duty": "actual-failure"}
            def release(self, *args):
                pass
            def finish(self, *args):
                return json.dumps(dict(version=1, action="execute", status=self.status,
                                       request=request, reason=""))
        for status in ("blocked", "accepted"):
            with self.subTest(status=status), self.assertRaisesRegex(AssertionError, "member execution|trace belongs"):
                members(FixtureDouble(status), "MEM-LEAVE-FAILURE")

    def test_no_skip_bailout_failure_or_timeout_counts_as_pass(self):
        self.assertEqual(classify(0, "ok 1\nResult: PASS", "")[0], "PASS")
        cases = [
            (0, "ok 1 # SKIP unavailable\nResult: PASS", "", "FAIL"),
            (0, "1..0 # skip no feature\nResult: PASS", "", "FAIL"),
            (255, "Bailout called", "FATAL:  could not prepare cluster HW startup authority\nDETAIL: reason=SNAPSHOT_MISSING", "FAIL"),
            (255, "Bailout called", "FATAL:  PRE1 data directory is not supported by PRE2 shared mode", "FAIL"),
            (1, "not ok 1\n", "FATAL:  could not prepare cluster HW startup authority", "FAIL"),
            (124, "", "", "FAIL"),
            (255, "Bailout called", "FATAL: unexpected corruption", "FAIL"),
        ]
        for rc, text, log, expected in cases:
            with self.subTest(expected=expected, text=text):
                self.assertEqual(classify(rc, text, log)[0], expected)

    def test_absent_adapter_has_nonzero_blocked_result_for_every_case(self):
        with tempfile.TemporaryDirectory() as root:
            output = Path(root) / "report"
            env = dict(os.environ)
            env.pop("PGRAC_PRE2_TEST_ADAPTER", None)
            result = subprocess.run(
                [sys.executable, str(Path(__file__).with_name("acceptance.py")),
                 "--output", str(output), "--bindir", str(Path(root) / "absent-bin")],
                env=env, capture_output=True, text=True)
            self.assertEqual(result.returncode, 1, result.stderr)
            rows = json.loads((output / "results.json").read_text())
            self.assertEqual(len(rows), 14)
            self.assertTrue(all(row["status"] == "BLOCKED" for row in rows))
            self.assertFalse(any(output.rglob("pg_control")))

    def test_missing_case_capability_cannot_initialize_any_node(self):
        class ReadOnlyDriver:
            def call(self, op):
                if op != "describe":
                    raise AssertionError("preflight started a database")
                return {"capabilities": ["fresh_init", "shared_start", "shared_stop", "root_observation"]}
        with tempfile.TemporaryDirectory() as root:
            with self.assertRaises(Blocked):
                run_case("W06-HOLDER-EIO", ReadOnlyDriver(), Path(root), Path(root))
            self.assertFalse(list(Path(root).iterdir()))


if __name__ == "__main__":
    unittest.main()
