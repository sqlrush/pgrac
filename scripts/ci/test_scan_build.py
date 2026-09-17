#!/usr/bin/env python3
"""CI analyzer execution contract. Author: SqlRush <sqlrush@gmail.com>."""

import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


class ScanBuildExecution(unittest.TestCase):
    def run_helper(self, *, clean_rc=0, analysis_rc=0):
        with tempfile.TemporaryDirectory(prefix="pgrac-scan-build-test-") as root:
            root = Path(root)
            ci = root / "scripts" / "ci"
            ci.mkdir(parents=True)
            shutil.copyfile(Path(__file__).with_name("run-scan-build.sh"), ci / "run-scan-build.sh")
            (root / "src" / "backend" / "cluster").mkdir(parents=True)
            commands = root / "commands"
            commands.mkdir()
            for name, body in {
                "make": f"exit {clean_rc}\n",
                "scan-build": (
                    'if [ "$1" = --help ]; then echo "fixture scan-build"; exit 0; fi\n'
                    'printf "%s\\n" "$@" > "$CAPTURE_ARGS"\n'
                    f"exit {analysis_rc}\n"
                ),
            }.items():
                command = commands / name
                command.write_text("#!/bin/sh\n" + body)
                command.chmod(0o755)
            capture = root / "arguments"
            env = dict(os.environ, PATH=f"{commands}:{os.environ['PATH']}",
                       CAPTURE_ARGS=str(capture), CI_ANALYZE_JOBS="2")
            result = subprocess.run(["bash", str(ci / "run-scan-build.sh")],
                                    env=env, text=True, capture_output=True, check=False)
            return result.returncode, capture.read_text() if capture.exists() else ""

    def test_analysis_build_failure_is_not_success(self):
        rc, _ = self.run_helper(analysis_rc=7)
        self.assertEqual(rc, 7)

    def test_clean_failure_prevents_analysis(self):
        rc, arguments = self.run_helper(clean_rc=9)
        self.assertEqual(rc, 9)
        self.assertEqual(arguments, "")

    def test_success_uses_parallel_build_without_changing_bug_policy(self):
        rc, arguments = self.run_helper()
        self.assertEqual(rc, 0)
        self.assertIn("-j2\n", arguments)
        self.assertNotIn("--status-bugs", arguments)


if __name__ == "__main__":
    unittest.main()
