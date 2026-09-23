"""Standalone helper tests; device authority requires real Linux block I/O.

Author: SqlRush <sqlrush@gmail.com>
"""

import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from voting import canonical_image

SOURCE = Path(__file__).resolve().parents[1] / "voting_io.c"


class VotingIoTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory(prefix="pre1-vote-build-")
        cls.binary = Path(cls.temp.name) / "voting_io"
        subprocess.run([os.environ.get("CC", "cc"), "-std=c11", "-Wall", "-Wextra",
                        "-Werror", "-O2", str(SOURCE), "-o", str(cls.binary)], check=True)

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    def run_helper(self, *args):
        return subprocess.run([str(self.binary), *args], capture_output=True, timeout=5)

    def test_c_images_equal_independently_checked_python_images(self):
        for index in range(3):
            result = self.run_helper("image", str(index))
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stdout, canonical_image(index))
            self.assertEqual(result.stderr, b"")

    def test_real_bounded_write_core_and_faults(self):
        source = SOURCE.parent / "tests" / "test_voting_write_core.c"
        binary = Path(self.temp.name) / "write_core"
        subprocess.run([os.environ.get("CC", "cc"), "-std=c11", "-Wall", "-Wextra",
                        "-Werror", "-O2", str(source), "-o", str(binary)], check=True)
        result = subprocess.run([str(binary)], capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("cases PASS", result.stdout)

    def test_native_runtime_member_observer(self):
        source = SOURCE.parent / "tests" / "test_voting_member_core.c"
        binary = Path(self.temp.name) / "member_core"
        subprocess.run([os.environ.get("CC", "cc"), "-std=c11", "-Wall", "-Wextra",
                        "-Werror", "-O2", str(source), "-o", str(binary)], check=True)
        result = subprocess.run([str(binary)], capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("native member observer cases PASS", result.stdout)

    def test_format_regular_file_rejects_without_any_write(self):
        with tempfile.TemporaryDirectory(prefix="pre1-format-reject-") as temp:
            path = Path(temp) / "fresh"
            payload = bytes(IMAGE_BYTES := len(canonical_image(0)))
            path.write_bytes(payload)
            result = self.run_helper("format-fresh", str(path), "360014056bfe000009214000800000000",
                                     "0", str(IMAGE_BYTES), "8", "0")
            self.assertNotEqual(result.returncode, 0)
            # A recognized write command must reject its target, not masquerade
            # as an unknown CLI option (which would leave this path untested).
            reason = json.loads(result.stdout)["reason"]
            self.assertIn(reason, ("DEVICE_TYPE_OR_NUMBER", "LINUX_BLOCK_DEVICE_REQUIRED"))
            self.assertEqual(path.read_bytes(), payload)

    def test_invalid_commands_and_numbers_do_not_echo_values(self):
        for args in ((), ("DO_NOT_PRINT_SECRET",), ("format-fresh",), ("image", "-1"),
                     ("image", "00"), ("image", "3"), ("image", "0", "extra"),
                     ("inspect", "/dev/sda", "36001400000", "0", "512", "8", "0"),
                     ("inspect", "/dev/sda", "36001400000", "0", "1048577", "8", "0"),
                     ("inspect", "/dev/sda", "36001400000", "0", "1048576", "8", "-1")):
            result = self.run_helper(*args)
            self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
            self.assertNotIn(b"DO_NOT_PRINT_SECRET", result.stdout + result.stderr)
            self.assertEqual(json.loads(result.stdout)["status"], "BLOCKED")

    def test_regular_files_and_symlinks_never_become_device_authority(self):
        with tempfile.TemporaryDirectory(prefix="pre1-vote-not-device-") as temp:
            path = Path(temp) / "image"
            payload = canonical_image(0)
            path.write_bytes(payload)
            alias = Path(temp) / "alias"
            alias.symlink_to(path)
            for selected in (path, alias):
                result = self.run_helper("inspect", str(selected), "360014056bfe000009214000800000000", "0",
                                         str(len(payload)), "8", "0")
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse(json.loads(result.stdout)["strict_authority"])
                self.assertEqual(path.read_bytes(), payload)
            self.assertEqual(hashlib.sha256(path.read_bytes()).digest(), hashlib.sha256(payload).digest())


if __name__ == "__main__":
    unittest.main()
