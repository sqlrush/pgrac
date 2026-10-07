"""Linux protected client against the actual target service and C map verifier.

Author: SqlRush <sqlrush@gmail.com>
Native libvirt/configfs handles alone are fixtures. No live guest/export writes.
Usage: sudo python3 run_target_client_runtime.py /absolute/map-verifier
"""

from dataclasses import asdict
import fcntl
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import unittest
from unittest.mock import patch

import run_target_service_runtime as server_tests
import run_target_service_cli as cli_tests
import run_target_registry as registry_tests
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from target_journal import TargetJournal, TargetJournalError, _canonical
try:
    import target_client_runtime as runtime
except ImportError:
    runtime = None


class ClientRuntimeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        server_tests.ServiceRuntimeTests.setUpClass()

    def setUp(self):
        self.assertTrue(callable(getattr(runtime, "run_client_request", None)),
                        "protected target client runtime is missing")
        self.s = server_tests.ServiceRuntimeTests()
        self.addCleanup(self.s.doCleanups)
        self.s.setUp()
        self.path = self.s.r.directory / "client.json"
        self.document = dict(version=1, registry_path=str(self.s.r.registry),
            endpoint=asdict(self.s.endpoint), tls=dict(
                ca_certificate=str(self.s.tls_directory / "server.pem"),
                certificate=str(self.s.tls_directory / "client.pem"),
                private_key=str(self.s.tls_directory / "client.key")))
        self.write()

    def write(self):
        self.path.write_text(json.dumps(self.document))
        self.path.chmod(0o600)

    def command(self, action="identity"):
        result = dict(version=1, action=action, challenge="ab" * 16)
        if action != "identity":
            mapping = self.s.registry.node(2).mapping
            result.update(node_id=2, system_identifier=mapping.system_identifier,
                mapping_generation=mapping.mapping_generation, protected_set_digest=mapping.protected_set_digest,
                operation_id="ad" * 16, attempt=1, daemon_boot_id="ae" * 16, target_boot_id=self.s.boot_id)
        return result

    def request(self, document=None, *, deadline=None):
        return runtime.run_client_request(str(self.path), _canonical(self.command() if document is None else document),
            time.monotonic_ns() + 3_000_000_000 if deadline is None else deadline)

    def installed_cli(self):
        fixture = cli_tests.ServiceCliTests()
        self.addCleanup(fixture.doCleanups)
        fixture.setUp()
        return fixture.cli

    def test_real_signed_registry_tls_and_durable_prepare(self):
        self.s.start()
        result = json.loads(self.request())
        self.assertEqual(result["status"], "IDENTITY_ONLY")
        self.assertEqual(result["target_boot_id"], self.s.boot_id)
        result = json.loads(self.request(self.command("prepare_deny")))
        self.assertEqual(result["status"], "DENY_RECORDED")
        self.assertNotIn("certificate", result)
        self.assertEqual(self.s.stop(), 0)
        with TargetJournal(str(self.s.state), self.s.registry.inventory_digest) as journal:
            self.assertEqual(journal.denied()[0].identity.operation_id, "ad" * 16)

    def test_wrong_registry_or_request_binding_never_connects(self):
        with patch("target_client.socket.socket", side_effect=AssertionError("must not connect")) as network:
            for key, value in (("node_id", 3), ("system_identifier", 987654321),
                               ("mapping_generation", 99), ("protected_set_digest", "ef" * 32)):
                document = self.command("prepare_deny")
                document[key] = value
                with self.subTest(key=key), self.assertRaisesRegex(TargetJournalError, "^TARGET_CLIENT_UNPROVEN$"):
                    self.request(document)
            self.document["endpoint"]["inventory_digest"] = "ef" * 32
            self.write()
            with self.assertRaises(TargetJournalError):
                self.request()
        network.assert_not_called()

    def test_bad_signature_and_expired_deadline_leave_target_unopened(self):
        with self.assertRaises(TargetJournalError):
            self.request(deadline=time.monotonic_ns() - 1)
        self.s.r.document["nodes"][0]["signed_map"] = self.s.r.packet[:-1].hex() + "00"
        self.s.r.write()
        with self.assertRaises(TargetJournalError):
            self.request()
        self.assertFalse((self.s.state / "native-opened").exists())

    def test_wrong_server_pin_never_dispatches_or_emits_payload(self):
        self.s.start()
        self.document["endpoint"]["certificate_sha256"] = "ef" * 32
        self.write()
        with self.assertRaisesRegex(TargetJournalError, "^TARGET_CLIENT_UNPROVEN$"):
            self.request()
        self.assertFalse((self.s.state / "native-opened").exists())
        self.assertEqual(self.s.stop(), 0)

    def test_installed_isolated_cli_returns_one_exact_reply(self):
        cli = self.installed_cli()
        self.s.start()
        result = subprocess.run([sys.executable, "-I", "-B", str(cli), "--request", "--config", str(self.path),
            "--deadline-ns", str(time.monotonic_ns() + 3_000_000_000)], input=_canonical(self.command()),
            capture_output=True, timeout=5)
        self.assertEqual((result.returncode, result.stderr), (0, b""))
        response = json.loads(result.stdout)
        self.assertEqual(response["status"], "IDENTITY_ONLY")
        self.assertEqual(response["challenge"], "ab" * 16)
        self.assertEqual(response["target_boot_id"], self.s.boot_id)
        self.assertEqual(self.s.stop(), 0)

    def test_held_open_input_expires_without_network_or_new_deadline(self):
        cli = self.installed_cli()
        start = time.monotonic()
        process = subprocess.Popen([sys.executable, "-I", "-B", str(cli), "--request", "--config", str(self.path),
            "--deadline-ns", str(time.monotonic_ns() + 200_000_000)],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            process.stdin.write(b'{"action":')
            process.stdin.flush()
            status = process.wait(timeout=3)
            self.assertNotEqual(status, 0)
            self.assertLess(time.monotonic() - start, 2)
            self.assertEqual(process.stdout.read(), b"")
            self.assertEqual(process.stderr.read(), b"TARGET_CLIENT_UNPROVEN\n")
        finally:
            if process.poll() is None:
                process.kill()
                process.wait()
            for stream in (process.stdin, process.stdout, process.stderr):
                stream.close()

    def test_cli_oversized_or_noncanonical_input_never_connects(self):
        cli = self.installed_cli()
        for payload in (b"x" * 4097, _canonical(self.command()) + b" ", b"", b"{}\n{}\n"):
            result = subprocess.run([sys.executable, "-I", "-B", str(cli), "--request", "--config", str(self.path),
                "--deadline-ns", str(time.monotonic_ns() + 3_000_000_000)], input=payload,
                capture_output=True, timeout=5)
            self.assertEqual(result.returncode, 1)
            self.assertEqual(result.stdout, b"")
            self.assertEqual(result.stderr, b"TARGET_CLIENT_UNPROVEN\n")
        self.assertFalse((self.s.state / "native-opened").exists())

    def test_full_output_pipe_expires_but_does_not_undo_durable_deny(self):
        cli = self.installed_cli()
        self.s.start()
        read_fd, write_fd = os.pipe()
        try:
            capacity = fcntl.fcntl(write_fd, fcntl.F_SETPIPE_SZ, 4096)
            self.assertEqual(os.write(write_fd, b"x" * capacity), capacity)
            result = subprocess.run([sys.executable, "-I", "-B", str(cli), "--request", "--config", str(self.path),
                "--deadline-ns", str(time.monotonic_ns() + 800_000_000)],
                input=_canonical(self.command("prepare_deny")), stdout=write_fd, stderr=subprocess.PIPE, timeout=3)
            self.assertEqual(result.returncode, 1)
            self.assertEqual(result.stderr, b"TARGET_CLIENT_UNPROVEN\n")
            self.assertEqual(os.read(read_fd, capacity), b"x" * capacity)
        finally:
            os.close(read_fd)
            os.close(write_fd)
        self.assertEqual(self.s.stop(), 0)
        with TargetJournal(str(self.s.state), self.s.registry.inventory_digest) as journal:
            self.assertEqual(journal.denied()[0].identity.operation_id, "ad" * 16)

    def test_closed_output_pipe_is_nonzero_after_remote_prepare(self):
        cli = self.installed_cli()
        self.s.start()
        read_fd, write_fd = os.pipe()
        os.close(read_fd)
        try:
            result = subprocess.run([sys.executable, "-I", "-B", str(cli), "--request", "--config", str(self.path),
                "--deadline-ns", str(time.monotonic_ns() + 3_000_000_000)],
                input=_canonical(self.command("prepare_deny")), stdout=write_fd, stderr=subprocess.PIPE, timeout=5)
            self.assertEqual(result.returncode, 1)
            self.assertEqual(result.stderr, b"TARGET_CLIENT_UNPROVEN\n")
        finally:
            os.close(write_fd)
        self.assertEqual(self.s.stop(), 0)
        with TargetJournal(str(self.s.state), self.s.registry.inventory_digest) as journal:
            self.assertEqual(journal.denied()[0].identity.operation_id, "ad" * 16)


if __name__ == "__main__":
    if len(sys.argv) != 2 or not Path(sys.argv[1]).is_absolute():
        raise SystemExit("expected absolute verifier path")
    registry_tests.EXECUTABLE = sys.argv[1]
    unittest.main(argv=[sys.argv[0]], verbosity=2)
