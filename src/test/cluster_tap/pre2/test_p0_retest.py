"""Offline checks for the targeted runner and retained-output judge."""
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
from types import SimpleNamespace

import p0_retest as retest


class RetestTests(unittest.TestCase):
    logs = ["LOG: member ready\n"] * 4
    def capture(self, case="P0-10"):
        return {"version": 1, "case": case, "rc": 0,
                "sql_completed": True, "normal_stop": True,
                "cleanup_errors": [], "nodes": [0, 1, 2, 3],
                "source_commit": "4a57a5851550b5d7e7ca1e54c5471b78ae71786f"}

    def test_empty_capture_never_passes(self):
        for case in retest.CASES:
            with self.subTest(case=case):
                self.assertEqual(retest.judge(case, {}, "", [""] * 4)["status"], "NOT_REACHED")

    def test_no_errors_is_only_workload_evidence(self):
        result = retest.judge("P0-10", self.capture(), "1..1\nok 1 - SQL\n", self.logs)
        self.assertEqual(result["status"], "WORKLOAD_PASS")
        self.assertEqual(result["original_path"], "UNKNOWN")
        self.assertFalse(result["p0_closed"])

    def test_empty_member_logs_are_not_completion_evidence(self):
        self.assertEqual(retest.judge("P0-10", self.capture(), "1..1\nok 1\n", [""] * 4)["status"], "FAIL")

    def test_every_original_symptom_fails_with_raw_line(self):
        samples = {"P0-5": "ERROR: 53R97: cluster TT status unknown: RECYCLED_AUTHORITY_UNPROVABLE",
                   "P0-8": "ERROR: cluster ITL slot exhaustion: UNKNOWN slot 6 xid 5282704",
                   "P0-10": "ERROR: could not obtain read image from X holder 3 status=-1",
                   "P0-12": "ERROR: R4 HOT logical-row proof unprovable"}
        for case, line in samples.items():
            with self.subTest(case=case):
                result = retest.judge(case, self.capture(case), "1..1\nok 1 - SQL\n", [line, "", "", ""])
                self.assertEqual(result["status"], "FAIL")
                self.assertEqual(result["errors"][0]["text"], line)

    def test_failure_cannot_be_overridden_by_later_pass(self):
        for text in ("not ok 1 - failed\n1..1\n", "ok 1 # SKIP unavailable\n1..1\n",
                     "ok 1 # TODO later\n1..1\n", "Bail out! bad\n", "", "ok 1\n1..2\n"):
            with self.subTest(text=text):
                self.assertNotEqual(retest.judge("P0-10", self.capture(), text, self.logs)["status"], "WORKLOAD_PASS")

    def test_nonzero_rc_cleanup_stop_and_missing_node_fail(self):
        for patch in ({"rc": 1}, {"normal_stop": False}, {"cleanup_errors": ["quit failed"]},
                      {"nodes": [0, 1, 2]}, {"sql_completed": False}, {"case": "P0-12"}):
            with self.subTest(patch=patch):
                self.assertNotEqual(retest.judge("P0-10", self.capture() | patch,
                                                "ok 1\n1..1\n", self.logs)["status"], "WORKLOAD_PASS")

    def test_wire_timeout_is_not_a_recycled_result(self):
        line = "LOG: PGRAC ordinary TT exchange refused: received=0 status=-1"
        result = retest.judge("P0-5", self.capture("P0-5"), "ok 1\n1..1\n", [line, "", "", ""])
        self.assertEqual(result["status"], "FAIL")
        self.assertEqual(result["original_path"], "UNKNOWN")

    def test_four_prepare_commands_run_without_database(self):
        with tempfile.TemporaryDirectory() as tmp:
            for case in retest.CASES:
                out = Path(tmp) / case
                run = subprocess.run([sys.executable, str(Path(retest.__file__).with_name(
                    case.lower().replace("-", "_") + "_retest.py")), "prepare", "--output", str(out)],
                    capture_output=True, text=True)
                self.assertEqual(run.returncode, 0, run.stderr)
                plan = json.loads((out / "preparation.json").read_text())
                self.assertEqual(plan["case"], case)
                self.assertEqual(plan["source_commit"], retest.source_commit())
                self.assertFalse(plan["database_started"])
                self.assertTrue((out / "run.sh").read_text().startswith("#!/bin/sh\n"))

    def test_prepare_will_not_overwrite_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(FileExistsError):
                retest.prepare("P0-10", Path(tmp))

    def test_native_route_keeps_child_failure_and_node_ordinals(self):
        with tempfile.TemporaryDirectory() as tmp:
            build = Path(tmp) / "build"
            for name in ("install/bin/postgres", "install/bin/psql", "src/test/regress/pg_regress"):
                path = build / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.touch()
            entry = Path(tmp) / "entry.json"
            entry.write_text("{}")
            output = Path(tmp) / "result"
            def child(argv, **kwargs):
                self.assertEqual(kwargs["env"]["PGRAC_P0_RETEST_CASE"], "P0-10")
                self.assertEqual(kwargs["env"]["PGRAC_PRE2_ENTRY_FILE"], str(entry.resolve()))
                self.assertNotIn("PGRAC_PRE2_TEST_ADAPTER", kwargs["env"])
                kwargs["stdout"].write(b"ok 1 - SQL\n1..1\n")
                (output / "log/p0-result.json").write_text(json.dumps(self.capture()))
                # Only node 2 was exported. Its error must stay on node 2.
                (output / "log/node2.log").write_text("ERROR: original failure\n")
                return SimpleNamespace(returncode=1)
            with patch.object(retest, "source_commit", return_value=self.capture()["source_commit"]), \
                    patch.object(retest.subprocess, "run", side_effect=child):
                result = retest.run_case("P0-10", SimpleNamespace(
                    build=build, entry_file=entry, output=output))
            self.assertEqual(result["status"], "FAIL")
            self.assertEqual(result["errors"][0]["node"], 2)
            self.assertFalse(result["checks"]["client_rc"])
            self.assertFalse(result["checks"]["member_logs_retained"])


if __name__ == "__main__":
    unittest.main()
