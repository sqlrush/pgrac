"""Installed-bundle CLI boundary on Linux; never starts live target operations.

Author: SqlRush <sqlrush@gmail.com>
Uses only a new root-owned temporary installation and invalid configuration.
"""

import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


class ServiceCliTests(unittest.TestCase):
    def setUp(self):
        if sys.platform != "linux" or os.geteuid() != 0:
            self.fail("run this fixture test as Linux root")
        self.source = Path(__file__).resolve().parents[1]
        self.assertTrue((self.source / "pgrac-target-service").is_file(), "protected target CLI is missing")
        temporary = tempfile.TemporaryDirectory(prefix="pgrac-service-cli-")
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.bundle = self.directory / "bundle"
        self.bundle.mkdir(mode=0o700)
        for path in [self.source / "pgrac-target-service", *self.source.glob("target_*.py")]:
            shutil.copyfile(path, self.bundle / path.name)
        self.cli = self.bundle / "pgrac-target-service"

    def run_cli(self, *arguments, isolated=True, env=None):
        return subprocess.run([sys.executable, *(["-I"] if isolated else []), "-B", str(self.cli), *arguments],
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=5, env=env)

    def test_protected_bundle_imports_real_runtime_and_sanitizes_config_error(self):
        result = self.run_cli("--config", str(self.directory / "private-secret-name"))
        self.assertEqual(result.returncode, 1)
        self.assertEqual(result.stdout, b"")
        self.assertEqual(result.stderr, b"TARGET_SERVICE_CONFIG\n")

    def test_isolated_interpreter_is_mandatory_and_help_is_finite(self):
        result = self.run_cli("--help", isolated=False)
        self.assertEqual(result.returncode, 1)
        self.assertEqual(result.stderr, b"TARGET_SERVICE_PROCESS\n")
        result = self.run_cli("--help")
        self.assertEqual(result.returncode, 0)
        self.assertIn(b"--config /absolute/protected/service.json", result.stdout)

    def test_pythonpath_cannot_inject_application_module(self):
        poison = self.directory / "poison"
        poison.mkdir()
        (poison / "target_service.py").write_text('raise RuntimeError("SECRET-INJECTION")\n')
        result = self.run_cli("--config", str(self.directory / "missing"),
                              env={**os.environ, "PYTHONPATH": str(poison)})
        self.assertEqual(result.returncode, 1)
        self.assertEqual(result.stderr, b"TARGET_SERVICE_CONFIG\n")

    def test_untrusted_code_or_symlink_refuses_before_import(self):
        module = self.bundle / "target_service.py"
        original = module.read_bytes()
        module.chmod(0o666)
        result = self.run_cli("--help")
        self.assertEqual(result.stderr, b"TARGET_SERVICE_CODE\n")
        self.assertEqual(result.returncode, 1)
        module.chmod(0o644)
        module.unlink()
        real = self.directory / "real.py"
        real.write_bytes(original)
        module.symlink_to(real)
        self.assertEqual(self.run_cli("--help").stderr, b"TARGET_SERVICE_CODE\n")

    def test_cache_or_package_directory_is_not_an_alternate_code_source(self):
        (self.bundle / "__pycache__").mkdir()
        result = self.run_cli("--help")
        self.assertEqual(result.returncode, 1)
        self.assertEqual(result.stderr, b"TARGET_SERVICE_CODE\n")


if __name__ == "__main__":
    unittest.main(verbosity=2)
