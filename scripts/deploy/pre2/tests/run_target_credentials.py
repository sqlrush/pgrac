"""Linux protected TLS loader and actual channel, using ephemeral test keys.

Author: SqlRush <sqlrush@gmail.com>
Usage: python3 run_target_credentials.py
Runs in a dedicated process: the production loader disables process core dumps.
"""

import ctypes
from dataclasses import replace
import os
from pathlib import Path
import resource
import shutil
import ssl
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

import test_target_transport as transport_tests
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from target_journal import TargetJournalError
try:
    from target_credentials import TlsFiles, load_server_context
except ModuleNotFoundError:
    TlsFiles = load_server_context = None


class TargetCredentialTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if sys.platform != "linux":
            raise AssertionError("this profile requires Linux, not a skip")
        transport_tests.TargetTransportTests.setUpClass.__func__(cls)

    def setUp(self):
        self.assertTrue(callable(load_server_context), "protected TLS credential loader is missing")
        temporary = tempfile.TemporaryDirectory(prefix="pgrac-tls-files-")
        self.addCleanup(temporary.cleanup)
        self.local = Path(temporary.name).resolve()
        for name in ("server.pem", "server.key", "client.pem"):
            shutil.copyfile(self.directory / name, self.local / name)
            (self.local / name).chmod(0o600)
        self.files = TlsFiles(str(self.local / "client.pem"), str(self.local / "server.pem"),
                              str(self.local / "server.key"), os.geteuid())

    def load(self, files=None, deadline=None):
        return load_server_context(self.files if files is None else files,
                                   time.monotonic_ns() + 5_000_000_000 if deadline is None else deadline)

    def test_loaded_context_completes_real_authenticated_exchange(self):
        # Reuse the real TCP/TLS driver, not a mocked OpenSSL response.
        driver = transport_tests.TargetTransportTests()
        driver.directory, driver.pin = self.directory, self.pin
        calls, errors, response, _ = driver.exchange(b"\0\0\0\1x", server=self.load())
        self.assertEqual(errors, [])
        self.assertEqual(len(calls), 1)
        self.assertEqual(response[4:], b"reply-not-a-proof")

    def test_native_dump_protection_precedes_credential_open(self):
        import target_credentials
        native_open = target_credentials._open_registry
        libc = ctypes.CDLL(None)
        libc.prctl.argtypes = (ctypes.c_int, ctypes.c_ulong, ctypes.c_ulong, ctypes.c_ulong, ctypes.c_ulong)
        libc.prctl.restype = ctypes.c_int
        opened = []
        def observe(path, uid):
            self.assertEqual(libc.prctl(3, 0, 0, 0, 0), 0)
            self.assertEqual(resource.getrlimit(resource.RLIMIT_CORE), (0, 0))
            opened.append(path)
            return native_open(path, uid)
        with patch("target_credentials._open_registry", side_effect=observe):
            self.load()
        self.assertEqual(len(opened), 3)

    def test_dump_protection_failure_does_not_open_credentials(self):
        with patch("target_credentials._lock_secrets", side_effect=OSError("private-guard-error")), \
                patch("target_credentials._open_registry") as opened:
            with self.assertRaises(TargetJournalError) as caught:
                self.load()
        opened.assert_not_called()
        self.assertNotIn("private", str(caught.exception))

    def test_modes_links_symlink_and_unsafe_parent_refuse(self):
        key = self.local / "server.key"
        before = set(os.listdir("/proc/self/fd"))
        for mode in (0o644, 0o640, 0o400, 0o1600):
            key.chmod(mode)
            with self.subTest(mode=mode), self.assertRaises(TargetJournalError):
                self.load()
        key.chmod(0o600)
        alias = self.local / "alias"
        os.link(key, alias)
        with self.assertRaises(TargetJournalError):
            self.load()
        alias.unlink()
        key.rename(alias)
        key.symlink_to(alias)
        with self.assertRaises(TargetJournalError):
            self.load()
        key.unlink()
        alias.rename(key)
        self.local.chmod(0o770)
        try:
            with self.assertRaises(TargetJournalError):
                self.load()
        finally:
            self.local.chmod(0o700)
        self.assertEqual(set(os.listdir("/proc/self/fd")), before)

    def test_wrong_owner_empty_oversized_and_mismatched_key_refuse(self):
        with self.assertRaises(TargetJournalError):
            self.load(replace(self.files, owner_uid=os.geteuid() + 1))
        key = self.local / "server.key"
        for body in (b"", b"x" * 65537, (self.directory / "other.key").read_bytes()):
            key.write_bytes(body)
            with self.subTest(size=len(body)), self.assertRaises(TargetJournalError):
                self.load()

    def test_encrypted_key_refuses_without_interactive_password(self):
        encrypted = self.local / "encrypted"
        result = subprocess.run(["openssl", "pkey", "-in", str(self.local / "server.key"),
                                 "-aes256", "-passout", "pass:test-only", "-out", str(encrypted)],
                                capture_output=True, timeout=5)
        self.assertEqual(result.returncode, 0)
        encrypted.chmod(0o600)
        start = time.monotonic()
        with self.assertRaises(TargetJournalError):
            self.load(replace(self.files, private_key=str(encrypted)))
        self.assertLess(time.monotonic() - start, 2)

    def test_changed_key_metadata_and_expiry_close_descriptors(self):
        native_load = ssl.SSLContext.load_cert_chain
        def mutate(context, *args, **kwargs):
            result = native_load(context, *args, **kwargs)
            key = self.local / "server.key"
            key.write_bytes(key.read_bytes() + b"\n")
            return result
        before = set(os.listdir("/proc/self/fd"))
        with patch.object(ssl.SSLContext, "load_cert_chain", new=mutate):
            with self.assertRaises(TargetJournalError):
                self.load()
        with self.assertRaises(TargetJournalError):
            self.load(deadline=time.monotonic_ns() - 1)
        self.assertEqual(set(os.listdir("/proc/self/fd")), before)

    def test_keylog_environment_does_not_enable_key_logging(self):
        path = self.local / "keylog-must-not-exist"
        with patch.dict(os.environ, {"SSLKEYLOGFILE": str(path)}):
            context = self.load()
        self.assertIsNone(context.keylog_filename)
        self.assertFalse(path.exists())
        self.assertEqual(context.num_tickets, 0)

    def test_special_file_does_not_block_and_native_errors_are_value_free(self):
        fifo = self.local / "secret-fifo-name"
        os.mkfifo(fifo, 0o600)
        with self.assertRaises(TargetJournalError) as caught:
            self.load(replace(self.files, private_key=str(fifo)))
        self.assertNotIn("secret", str(caught.exception))
        self.assertRegex(str(caught.exception), r"^TARGET_TLS_[A-Z_]+$")
        self.assertTrue(caught.exception.__suppress_context__)


if __name__ == "__main__":
    unittest.main(verbosity=2)
