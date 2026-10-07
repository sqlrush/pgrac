#!/usr/bin/env python3
"""Check the optional, real-file WAL publisher component benchmark.

Author: SqlRush <sqlrush@gmail.com>
"""

import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


class PublishCostTest(unittest.TestCase):
    def invoke(self, parent, mode, groups="3", records="8"):
        return subprocess.run(
            [BINARY, str(parent), mode, groups, records],
            capture_output=True, text=True, timeout=120,
        )

    def test_invalid_arguments_do_not_create_files(self):
        with tempfile.TemporaryDirectory() as parent:
            for mode, groups, records in (
                ("bad", "3", "8"), ("pgwp", "0", "8"),
                ("pgwp", "3x", "8"), ("pgwp", "3", "0"),
                ("pgwp", "100001", "8"), ("pgwp", "3", "1025"),
            ):
                result = self.invoke(parent, mode, groups, records)
                self.assertEqual(result.returncode, 2, result.stderr)
                self.assertEqual(list(Path(parent).iterdir()), [])

    def test_actual_publisher_progress_and_fsync_breakdown(self):
        with tempfile.TemporaryDirectory() as parent:
            result = self.invoke(parent, "pgwp")
            self.assertEqual(result.returncode, 0, result.stderr)
            packet = json.loads(result.stdout)
            self.assertEqual(packet["boundary"], "production_publisher_fixture")
            self.assertEqual(packet["mode"], "pgwp")
            self.assertFalse(packet["sql_commit_measurement"])
            self.assertEqual(len(packet["samples"]), 3)
            prior_end = 0
            for sample in packet["samples"]:
                self.assertEqual(sample["records"], 8)
                self.assertGreater(sample["covered_end"], prior_end)
                prior_end = sample["covered_end"]
                self.assertEqual(sample["publish_fsync_calls"], 4)
                self.assertGreater(sample["wal_fsync_us"], 0)
                self.assertGreater(sample["publish_us"], 0)
                self.assertGreaterEqual(
                    sample["publish_us"], sample["publish_fsync_us"]
                )
            self.assertEqual(list(Path(parent).iterdir()), [])

    def test_short_alias_cannot_overflow_expanded_path(self):
        with tempfile.TemporaryDirectory() as parent:
            target = Path(parent)
            for _ in range(7):
                target /= "x" * 120
                target.mkdir()
            alias = Path(parent) / "short"
            alias.symlink_to(target, target_is_directory=True)
            result = self.invoke(alias, "pgwp")
            self.assertEqual(result.returncode, 2, result.stderr)
            self.assertEqual(list(target.iterdir()), [])

    def test_wal_only_comparator_does_not_claim_commit_or_proof(self):
        with tempfile.TemporaryDirectory() as parent:
            result = self.invoke(parent, "wal-only")
            self.assertEqual(result.returncode, 0, result.stderr)
            packet = json.loads(result.stdout)
            self.assertFalse(packet["sql_commit_measurement"])
            self.assertEqual(packet["mode"], "wal-only")
            self.assertEqual(len(packet["samples"]), 3)
            for sample in packet["samples"]:
                self.assertEqual(sample["covered_end"], 0)
                self.assertEqual(sample["publish_fsync_calls"], 0)
                self.assertEqual(sample["publish_us"], 0)
                self.assertEqual(sample["publish_fsync_us"], 0)
                self.assertGreater(sample["wal_fsync_us"], 0)
            self.assertEqual(list(Path(parent).iterdir()), [])


if __name__ == "__main__":
    BINARY = str(Path(sys.argv.pop(1)).resolve(strict=True))
    unittest.main()
