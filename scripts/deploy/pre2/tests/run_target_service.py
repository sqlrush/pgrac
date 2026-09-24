"""Actual Linux listener/worker/TLS/journal lifecycle, with native fixtures.

Author: SqlRush <sqlrush@gmail.com>
No target exports, guests or stable data are modified by these tests.
"""

from dataclasses import replace
import hashlib
import os
from pathlib import Path
import signal
import socket
import ssl
import sys
import time
import unittest
from unittest.mock import patch

import run_target_worker as worker_tests
import test_target_service_config as config_tests
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from target_client import TargetEndpoint, request_target
from target_journal import TargetJournalError
from target_worker import TargetWorker
import target_service


class TargetServiceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        worker_tests.TargetWorkerTests.setUpClass.__func__(cls)

    def setUp(self):
        self.assertTrue(callable(getattr(target_service, "serve_listener", None)),
                        "sequential target listener is missing")
        self.driver = worker_tests.TargetWorkerTests()
        self.driver.tls_directory, self.driver.pin = self.tls_directory, self.pin
        self.addCleanup(self.driver.doCleanups)
        self.driver.setUp()
        self.cfg = config_tests.ServiceConfigTests()
        self.addCleanup(self.cfg.doCleanups)
        self.cfg.setUp()
        self.config = replace(self.cfg.load(), state_directory=str(self.driver.f.directory),
                              target_boot_id=self.driver.f.identity.target_boot_id, command_timeout_ms=500)
        self.server_pid = None
        self.addCleanup(self.stop_server)
        self.pin_server = hashlib.sha256(ssl.PEM_cert_to_DER_cert(
            (self.tls_directory / "server.pem").read_text())).hexdigest()

    def start_server(self, *, timeout=500, boot=None, expect_start=True):
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.bind(("127.0.0.1", 0))
        listener.listen(4)
        self.endpoint = TargetEndpoint("127.0.0.1", listener.getsockname()[1], "server",
                                       self.pin_server, self.driver.f.registry.inventory_digest)
        ready_read, ready_write = os.pipe()
        self.server_pid = os.fork()
        if self.server_pid == 0:
            os.close(ready_read)
            status = 2
            try:
                with self.driver.open_owner() as owner:
                    config = replace(self.config, command_timeout_ms=timeout,
                                     target_boot_id=boot or self.config.target_boot_id)
                    target_service.check_service_boot(owner, config.target_boot_id)
                    os.write(ready_write, b"READY")
                    os.close(ready_write)
                    target_service.serve_listener(listener, owner, config)
                status = 0
            except BaseException:
                pass
            finally:
                listener.close()
                os._exit(status)
        listener.close()
        os.close(ready_write)
        try:
            result = worker_tests.read_pipe(ready_read)
            if expect_start:
                self.assertEqual(result, b"READY")
            else:
                self.assertEqual(result, b"")
        finally:
            os.close(ready_read)

    def stop_server(self):
        if self.server_pid is not None:
            found, status = os.waitpid(self.server_pid, os.WNOHANG)
            if not found:
                os.kill(self.server_pid, signal.SIGTERM)
                status = worker_tests.reap(self.server_pid)
            self.server_pid = None
            return status

    def request(self, command=None, *, context=None):
        if context is None:
            context = self.driver.tls.context(server=False)
        context.hostname_checks_common_name = False
        return request_target(self.endpoint, context, command or self.driver.f.document,
                              time.monotonic_ns() + 3_000_000_000, route_count=4)

    def test_sequential_connections_replay_same_durable_operation(self):
        self.start_server()
        first = self.request()
        self.assertEqual(first["status"], "DENY_RECORDED")
        second = self.request()
        self.assertEqual(first["journal_sequence"], second["journal_sequence"])
        self.assertEqual(self.stop_server(), 0)
        self.assertEqual(self.driver.states()[0].identity, self.driver.f.identity)

    def test_unauthenticated_connection_does_not_stop_valid_next_connection(self):
        self.start_server()
        no_certificate = self.driver.tls.context(server=False, certificate=False)
        with self.assertRaises(TargetJournalError):
            self.request(context=no_certificate)
        self.assertEqual(self.request()["status"], "DENY_RECORDED")
        self.assertEqual(self.stop_server(), 0)

    def test_wrong_current_boot_never_starts_service_or_changes_journal(self):
        before = (self.driver.f.directory / "deny.journal").read_bytes()
        self.start_server(boot="ef" * 16, expect_start=False)
        self.assertEqual(self.stop_server(), 2 << 8)
        self.assertEqual((self.driver.f.directory / "deny.journal").read_bytes(), before)

    def test_old_boot_journal_requires_reconciliation_without_reset(self):
        self.driver.exchange()
        self.driver.close_owner()
        before = (self.driver.f.directory / "deny.journal").read_bytes()
        with patch("target_operation._kernel_boot_id", return_value="ef" * 16):
            self.start_server(boot="ef" * 16, expect_start=False)
        self.assertEqual(self.stop_server(), 2 << 8)
        self.assertEqual((self.driver.f.directory / "deny.journal").read_bytes(), before)

    def test_expired_native_operation_keeps_deny_and_accepts_later_inspection(self):
        def blocked(_path):
            signal.pause()
        with patch("target_drain.os.rmdir", side_effect=blocked):
            self.start_server(timeout=250)
        self.assertEqual(self.request()["status"], "DENY_RECORDED")
        self.driver.f.document["action"] = "complete_off"
        with self.assertRaises(TargetJournalError):
            self.request()
        result = self.request({"version": 1, "action": "identity", "challenge": "ab" * 16})
        self.assertEqual(result["status"], "IDENTITY_ONLY")
        self.assertEqual(self.stop_server(), 0)
        self.assertEqual(self.driver.states()[0].phases, (1, 1, 1, 1))

    def test_shutdown_during_native_work_preserves_deny_and_releases_only_dead_worker(self):
        marker = self.driver.f.directory / "native-entered"
        def blocked(_path):
            marker.write_text("entered")
            signal.pause()
        with patch("target_drain.os.rmdir", side_effect=blocked):
            self.start_server(timeout=3000)
        self.assertEqual(self.request()["status"], "DENY_RECORDED")
        self.driver.f.document["action"] = "complete_off"
        client = os.fork()
        if client == 0:
            try:
                self.request()
                os._exit(2)
            except TargetJournalError:
                os._exit(0)
        try:
            deadline = time.monotonic() + 2
            while not marker.exists() and time.monotonic() < deadline:
                time.sleep(0.005)
            self.assertTrue(marker.exists())
            self.assertEqual(self.stop_server(), 0)
            self.assertEqual(worker_tests.reap(client), 0)
            client = None
            with self.driver.open_owner():
                self.assertEqual(self.driver.states()[0].phases, (1, 1, 1, 1))
        finally:
            if client is not None:
                found, _ = os.waitpid(client, os.WNOHANG)
                if not found:
                    os.kill(client, signal.SIGKILL)
                    worker_tests.reap(client)


if __name__ == "__main__":
    unittest.main(verbosity=2)
