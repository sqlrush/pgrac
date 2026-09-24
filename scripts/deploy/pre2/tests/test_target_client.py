"""Actual TLS client to target owner, with exact operation-bound responses.

Author: SqlRush <sqlrush@gmail.com>
Only native LIO/libvirt calls are injected; certificates and files are fixtures.
"""

from dataclasses import replace
import hashlib
import json
from pathlib import Path
import socket
import ssl
import struct
import sys
import threading
import time
import unittest

import test_target_operation as operation_tests
import test_target_transport as transport_tests
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from target_dispatch import dispatch_target
from target_journal import TargetJournalError, _canonical
from target_transport import serve_one
try:
    from target_client import TargetEndpoint, request_target
except ModuleNotFoundError:
    TargetEndpoint = request_target = None


class TargetClientTests(unittest.TestCase):
    setUp_inventory = operation_tests.OperationTests.setUp_inventory
    read_config = operation_tests.OperationTests.read_config
    remove_acl = operation_tests.OperationTests.remove_acl
    context = transport_tests.TargetTransportTests.context

    @classmethod
    def setUpClass(cls):
        transport_tests.TargetTransportTests.setUpClass.__func__(cls)
        cls.tls_directory = cls.directory
        cls.server_pin = hashlib.sha256(ssl.PEM_cert_to_DER_cert(
            (cls.directory / "server.pem").read_text())).hexdigest()

    def setUp(self):
        self.assertTrue(callable(request_target), "target management client is missing")
        operation_tests.OperationTests.setUp(self)
        self.command = {"version": 1, "action": "prepare_deny", "node_id": 2,
                        "system_identifier": 123, "mapping_generation": self.mapping.mapping_generation,
                        "protected_set_digest": self.mapping.protected_set_digest,
                        "operation_id": self.identity.operation_id, "attempt": self.identity.attempt,
                        "daemon_boot_id": self.identity.daemon_boot_id,
                        "target_boot_id": self.identity.target_boot_id, "challenge": "ab" * 16}
        self.tls = transport_tests.TargetTransportTests()
        self.tls.directory, self.tls.pin = self.tls_directory, self.pin

    def client_context(self):
        context = self.tls.context(server=False)
        context.hostname_checks_common_name = False
        return context

    def exchange(self, *, command=None, handler=None, context=None, endpoint_change=None,
                 raw_reply=None, seconds=2, routes=4):
        errors, received = [], []
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        listener.settimeout(0.3)
        endpoint = TargetEndpoint("127.0.0.1", listener.getsockname()[1], "server",
                                  self.server_pin, self.registry.inventory_digest)
        if endpoint_change:
            endpoint = replace(endpoint, **endpoint_change)
        def dispatch(request):
            received.append(request)
            if handler:
                return handler(request)
            return dispatch_target(request, self.registry, self.journal, self.root, self.connection)
        def server():
            try:
                plain, _ = listener.accept()
                if raw_reply is None:
                    serve_one(plain, self.tls.context(server=True), (self.pin,),
                              time.monotonic_ns() + 3_000_000_000, dispatch)
                else:
                    with self.tls.context(server=True).wrap_socket(plain, server_side=True) as secured:
                        secured.settimeout(1)
                        secured.recv(8192)
                        if callable(raw_reply):
                            raw_reply(secured)
                        else:
                            secured.sendall(raw_reply)
            except BaseException as error:
                errors.append(error)
        thread = threading.Thread(target=server, daemon=True)
        thread.start()
        try:
            return request_target(endpoint, context or self.client_context(),
                                  self.command if command is None else command,
                                  time.monotonic_ns() + int(seconds * 1_000_000_000),
                                  route_count=routes)
        finally:
            thread.join(4)
            listener.close()
            self.assertFalse(thread.is_alive(), "test target worker exceeded envelope")
            self.last_received, self.last_errors = received, errors

    def test_real_prepare_and_native_completion(self):
        self.assertEqual(self.exchange()["status"], "DENY_RECORDED")
        self.command["action"] = "complete_off"
        result = self.exchange()
        self.assertEqual(result["status"], "OFF_DRAIN_UNCERTIFIED")
        self.assertEqual(result["route_phases"], [3] * 4)
        self.assertEqual(self.events, ["teardown"])
        self.assertFalse("certificate" in result)

    def test_identity_reply_is_pinned_to_inventory_and_challenge(self):
        command = {"version": 1, "action": "identity", "challenge": "ab" * 16}
        reply = self.exchange(command=command)
        self.assertEqual(reply["status"], "IDENTITY_ONLY")
        self.assertEqual(reply["target_boot_id"], self.identity.target_boot_id)
        with self.assertRaises(TargetJournalError):
            self.exchange(command=command, endpoint_change={"inventory_digest": "ee" * 32})
        self.assertEqual(self.journal.denied(), ())

    def test_wrong_server_pin_san_or_trust_never_dispatches(self):
        for change in ({"certificate_sha256": "ef" * 32}, {"server_name": "wrong-name"}):
            with self.subTest(change=change), self.assertRaises(TargetJournalError):
                self.exchange(endpoint_change=change)
            self.assertEqual(self.last_received, [])
        # A new explicit context with only unrelated trust cannot verify server.
        other = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        other.minimum_version = other.maximum_version = ssl.TLSVersion.TLSv1_3
        other.hostname_checks_common_name = False
        other.options |= ssl.OP_NO_TICKET
        other.load_verify_locations(str(self.tls_directory / "other.pem"))
        other.load_cert_chain(str(self.tls_directory / "client.pem"), str(self.tls_directory / "client.key"))
        with self.assertRaises(TargetJournalError):
            self.exchange(context=other)
        self.assertEqual(self.last_received, [])

    def test_mismatched_or_extra_response_fields_never_acknowledge(self):
        changes = ({"attempt": True}, {"operation_id": "ef" * 16}, {"challenge": "ee" * 16},
                   {"status": "PROVEN"}, {"node_id": 1}, {"version": True},
                   {"journal_sequence": 0}, {"journal_digest": "00" * 32}, {"extra": 1})
        for change in changes:
            def changed(request):
                value = json.loads(dispatch_target(request, self.registry, self.journal,
                                                    self.root, self.connection))
                return _canonical({**value, **change})
            with self.subTest(change=change), self.assertRaises(TargetJournalError):
                self.exchange(handler=changed)
            self.assertEqual(self.journal.denied()[0].identity, self.identity)

    def test_incomplete_wrong_size_or_noncanonical_replies_refuse(self):
        for reply in (b"", b"\0\0", struct.pack("!I", 0), struct.pack("!I", 4097),
                      struct.pack("!I", 6) + b"{}\n", struct.pack("!I", 2) + b"{}",
                      struct.pack("!I", 3) + b"[]\n"):
            with self.subTest(reply=reply), self.assertRaises(TargetJournalError):
                self.exchange(raw_reply=reply)
        with self.assertRaises(TargetJournalError):
            self.exchange(handler=lambda request: b" " + dispatch_target(
                request, self.registry, self.journal, self.root, self.connection))

    def test_extra_frame_is_not_ignored(self):
        def reply(secured):
            body = {**self.command, "status": "DENY_RECORDED", "journal_sequence": 1,
                    "journal_digest": "ee" * 32}
            del body["action"]
            payload = _canonical(body)
            secured.sendall(struct.pack("!I", len(payload)) + payload + b"extra")
        with self.assertRaises(TargetJournalError):
            self.exchange(raw_reply=reply)

    def test_partial_trickle_does_not_refresh_total_deadline(self):
        def reply(secured):
            secured.sendall(struct.pack("!I", 8))
            for _ in range(8):
                time.sleep(0.04)
                secured.sendall(b"x")
        start = time.monotonic()
        with self.assertRaises(TargetJournalError):
            self.exchange(raw_reply=reply, seconds=0.12)
        self.assertLess(time.monotonic() - start, 0.8)

    def test_invalid_endpoint_command_policy_and_expiry_do_not_dispatch(self):
        changes = ({"address": "localhost"}, {"address": "127.1"}, {"port": True},
                   {"port": 0}, {"server_name": "server/path"})
        for change in changes:
            with self.subTest(change=change), self.assertRaises(TargetJournalError):
                self.exchange(endpoint_change=change)
            self.assertEqual(self.last_received, [])
        with self.assertRaises(TargetJournalError):
            self.exchange(command={**self.command, "unknown": "/private"})
        self.assertEqual(self.last_received, [])
        context = self.client_context()
        context.check_hostname = False
        with self.assertRaises(TargetJournalError):
            self.exchange(context=context)
        self.assertEqual(self.last_received, [])
        with self.assertRaises(TargetJournalError):
            self.exchange(seconds=-1)
        self.assertEqual(self.last_received, [])

    def test_route_count_and_phase_are_not_inferred_from_reply(self):
        self.exchange()
        self.command["action"] = "complete_off"
        for phases in ([3] * 3, [3] * 5, [3, 3, 3, 2], [True] * 4, [3.0] * 4):
            def changed(request):
                value = json.loads(dispatch_target(request, self.registry, self.journal,
                                                    self.root, self.connection))
                return _canonical({**value, "route_phases": phases})
            with self.subTest(phases=phases), self.assertRaises(TargetJournalError):
                self.exchange(handler=changed)
        with self.assertRaises(TargetJournalError):
            self.exchange(routes=0)
        self.assertEqual(self.events, ["teardown"])

    def test_maximum_route_reply_fits_frame_without_becoming_certificate(self):
        self.command["action"] = "complete_off"
        def maximum(_request):
            body = {**self.command, "status": "OFF_DRAIN_UNCERTIFIED", "route_phases": [3] * 128,
                    "journal_sequence": (1 << 64) - 1, "journal_digest": "ef" * 32}
            del body["action"]
            return _canonical(body)
        reply = self.exchange(handler=maximum, routes=128)
        self.assertEqual(len(reply["route_phases"]), 128)
        self.assertEqual(reply["status"], "OFF_DRAIN_UNCERTIFIED")

    def test_lost_reply_keeps_actual_durable_deny_and_does_not_retry(self):
        def lost(request):
            dispatch_target(request, self.registry, self.journal, self.root, self.connection)
            raise OSError("private native diagnostic")
        with self.assertRaises(TargetJournalError) as caught:
            self.exchange(handler=lost)
        self.assertEqual(len(self.last_received), 1)
        self.assertEqual(self.journal.denied()[0].identity, self.identity)
        self.assertEqual(self.events, [])
        self.assertEqual(str(caught.exception), "TARGET_CLIENT_UNPROVEN")
        self.assertTrue(caught.exception.__suppress_context__)


if __name__ == "__main__":
    unittest.main()
