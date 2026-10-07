"""Real protected-file/pinned-process signing; test-only seeds and no target action.

Author: SqlRush <sqlrush@gmail.com>
Usage: python3 run_target_signing.py /absolute/pgrac-fenced-drain-sign
"""

from dataclasses import replace
import hashlib
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

from run_drain_sign_cli import KEY, packet

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from target_journal import TargetJournalError
from target_mapping import VerifierPin
from target_signing import SigningPin, sign_drain_body


class SigningTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="pgrac-sign-owner-")
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name).resolve()
        self.binary, self.seed = self.directory / "signer", self.directory / "seed"
        shutil.copyfile(EXECUTABLE, self.binary)
        self.binary.chmod(0o700)
        self.seed.write_bytes(b"\x42" * 32)
        self.seed.chmod(0o600)
        executable = VerifierPin(str(self.binary), hashlib.sha256(self.binary.read_bytes()).hexdigest(), os.geteuid())
        self.pin = SigningPin(executable, str(self.seed), bytes.fromhex(KEY))
        self.body = packet()[32:]

    def sign(self, pin=None, body=None, deadline=None):
        return sign_drain_body(self.pin if pin is None else pin, self.body if body is None else body,
                               time.monotonic_ns() + 5_000_000_000 if deadline is None else deadline)

    def sign_ok(self):
        try:
            return self.sign()
        except TargetJournalError as error:
            self.fail(f"valid protected signing invocation refused: {error}")

    def test_exact_signature_without_private_bytes_read_by_parent(self):
        expected = subprocess.run([EXECUTABLE, KEY], input=packet(), capture_output=True, check=True).stdout
        native_read = os.read
        seed_info = self.seed.stat()
        def no_seed_read(fd, count):
            info = os.fstat(fd)
            if (info.st_dev, info.st_ino) == (seed_info.st_dev, seed_info.st_ino):
                raise AssertionError("owner copied the private seed")
            return native_read(fd, count)
        with patch("os.read", side_effect=no_seed_read):
            self.assertEqual(self.sign_ok(), expected)
        self.assertEqual(expected[:-64], self.body)
        self.assertEqual(len(expected), 272)

    def test_seed_mode_size_links_symlinks_and_unsafe_ancestor_refuse(self):
        for mode in (0o644, 0o640, 0o400, 0o1600):
            self.seed.chmod(mode)
            with self.subTest(mode=mode), self.assertRaises(TargetJournalError):
                self.sign()
        self.seed.chmod(0o600)
        for size in (0, 31, 33):
            self.seed.write_bytes(b"\x42" * size)
            with self.subTest(size=size), self.assertRaises(TargetJournalError):
                self.sign()
        self.seed.write_bytes(b"\x42" * 32)
        alias = self.directory / "alias"
        os.link(self.seed, alias)
        with self.assertRaises(TargetJournalError):
            self.sign()
        alias.unlink()
        self.seed.rename(alias)
        self.seed.symlink_to(alias)
        with self.assertRaises(TargetJournalError):
            self.sign()
        self.seed.unlink()
        alias.rename(self.seed)
        self.directory.chmod(0o770)
        try:
            with self.assertRaises(TargetJournalError):
                self.sign()
        finally:
            self.directory.chmod(0o700)
        self.assertEqual(len(self.sign_ok()), 272)

    def test_wrong_seed_public_key_or_binary_identity_refuses(self):
        self.seed.write_bytes(b"\x43" * 32)
        with self.assertRaises(TargetJournalError):
            self.sign()
        self.seed.write_bytes(b"\x42" * 32)
        for pin in (replace(self.pin, public_key=b"\x01" * 32),
                    replace(self.pin, executable=replace(self.pin.executable, sha256="aa" * 32)),
                    replace(self.pin, executable=replace(self.pin.executable, owner_uid=os.geteuid() + 1))):
            with self.subTest(pin=pin.executable.owner_uid), self.assertRaises(TargetJournalError):
                self.sign(pin)

    def test_replacement_after_pin_uses_exact_opened_executable_and_seed(self):
        native_run = subprocess.run
        def replace_paths(*args, **kwargs):
            self.binary.rename(self.directory / "pinned-binary")
            self.binary.write_text("#!/bin/sh\nexit 99\n")
            self.binary.chmod(0o700)
            self.seed.rename(self.directory / "pinned-seed")
            self.seed.write_bytes(b"\x43" * 32)
            self.seed.chmod(0o600)
            return native_run(*args, **kwargs)
        with patch("subprocess.run", side_effect=replace_paths):
            self.assertEqual(self.sign_ok()[:-64], self.body)

    def test_bad_body_or_expiry_refuses_and_does_not_leak_fds(self):
        before = set(os.listdir("/proc/self/fd"))
        for body in (b"", self.body[:-1], self.body + b"x", b"x" * 1201, bytearray(self.body)):
            with self.subTest(size=len(body)), self.assertRaises(TargetJournalError):
                self.sign(body=body)
        with self.assertRaises(TargetJournalError):
            self.sign(deadline=time.monotonic_ns() - 1)
        self.assertEqual(self.sign_ok()[:-64], self.body)
        self.assertEqual(set(os.listdir("/proc/self/fd")), before)

    def test_timeout_output_or_stderr_error_is_value_free_and_closes_handles(self):
        before = set(os.listdir("/proc/self/fd"))
        cases = (subprocess.TimeoutExpired("private-key-never-echo", 1),
                 subprocess.CompletedProcess([], 0, self.body + bytes(63), b""),
                 subprocess.CompletedProcess([], 0, self.body + bytes(64), b"private-key-never-echo"),
                 subprocess.CompletedProcess([], 77, self.body + bytes(64), b""))
        for result in cases:
            options = {"side_effect": result} if isinstance(result, Exception) else {"return_value": result}
            with patch("subprocess.run", **options), self.assertRaises(TargetJournalError) as caught:
                self.sign()
            self.assertNotIn("private-key", str(caught.exception))
            self.assertTrue(caught.exception.__suppress_context__)
            self.assertEqual(set(os.listdir("/proc/self/fd")), before)


if __name__ == "__main__":
    if len(sys.argv) != 2 or not Path(sys.argv[1]).is_absolute():
        raise SystemExit("expected absolute signer path")
    EXECUTABLE = sys.argv[1]
    unittest.main(argv=[sys.argv[0]], verbosity=2)
