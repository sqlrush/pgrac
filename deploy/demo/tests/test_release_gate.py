"""Exact-commit publishing gate tests. Author: SqlRush <sqlrush@gmail.com>"""

import importlib.util
from pathlib import Path
import unittest

MODULE = Path(__file__).resolve().parents[1] / "release_gate.py"
gate = None
if MODULE.exists():
    spec = importlib.util.spec_from_file_location("release_gate", MODULE)
    gate = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(gate)


class ReleaseGate(unittest.TestCase):
    def test_missing_failed_or_newer_running_check_blocks_publish(self):
        self.assertIsNotNone(gate, "release gate missing")
        good = [{"name": name, "conclusion": "success", "started_at": "2026-09-20T00:00:00Z"}
                for name in gate.REQUIRED]
        self.assertEqual(gate.failures(good), [])
        self.assertTrue(gate.failures(good[:-1]))
        for conclusion in (None, "failure", "cancelled", "skipped"):
            changed = good + [dict(good[0], conclusion=conclusion, started_at="2026-09-20T01:00:00Z")]
            self.assertTrue(gate.failures(changed))
