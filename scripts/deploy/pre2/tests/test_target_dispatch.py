"""Authenticated command binding to the actual target journal and operation.

Author: SqlRush <sqlrush@gmail.com>
Native libvirt/LIO boundaries use the existing isolated operation fixture.
"""

from dataclasses import replace
import json
from pathlib import Path
import struct
import sys
import time
import unittest
from unittest.mock import patch

import test_target_operation as operation_tests
import test_target_transport as transport_tests
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from target_journal import TargetJournalError, _canonical
from target_transport import AuthenticatedTargetRequest
try:
    from target_dispatch import dispatch_target
except ModuleNotFoundError:
    dispatch_target = None


class TargetDispatchTests(unittest.TestCase):
    setUp_inventory = operation_tests.OperationTests.setUp_inventory
    read_config = operation_tests.OperationTests.read_config
    remove_acl = operation_tests.OperationTests.remove_acl

    def setUp(self):
        self.assertTrue(callable(dispatch_target), "authenticated target dispatcher is missing")
        operation_tests.OperationTests.setUp(self)
        self.document = {"version": 1, "action": "prepare_deny", "node_id": 2,
                         "system_identifier": 123, "mapping_generation": self.mapping.mapping_generation,
                         "protected_set_digest": self.mapping.protected_set_digest,
                         "operation_id": self.identity.operation_id, "attempt": self.identity.attempt,
                         "daemon_boot_id": self.identity.daemon_boot_id,
                         "target_boot_id": self.identity.target_boot_id, "challenge": "ab" * 16}

    def request(self, document=None, payload=None):
        return AuthenticatedTargetRequest("cd" * 32,
                                          _canonical(document or self.document) if payload is None else payload,
                                          time.monotonic_ns() + 5_000_000_000)

    def dispatch(self, request=None):
        return dispatch_target(request or self.request(), self.registry, self.journal,
                               self.root, self.connection)

    def test_identity_is_read_only_and_not_certification(self):
        before = (self.directory / "deny.journal").read_bytes()
        result = json.loads(self.dispatch(self.request({"version": 1, "action": "identity",
                                                       "challenge": "ab" * 16})))
        self.assertEqual(result, {"version": 1, "status": "IDENTITY_ONLY", "challenge": "ab" * 16,
                                  "target_boot_id": self.identity.target_boot_id,
                                  "inventory_digest": self.registry.inventory_digest})
        self.assertEqual((self.directory / "deny.journal").read_bytes(), before)
        self.assertEqual(self.events, [])

    def test_prepare_while_guest_active_is_durable_without_power_or_acl_action(self):
        self.connection.domain.active = 1
        result = json.loads(self.dispatch())
        self.assertEqual(result["status"], "DENY_RECORDED")
        self.assertEqual(result["challenge"], "ab" * 16)
        self.assertEqual(result["operation_id"], self.identity.operation_id)
        self.assertEqual(self.journal.denied()[0].identity, self.identity)
        self.assertEqual(self.journal.denied()[0].phases, (0, 0, 0, 0))
        self.assertEqual(self.connection.domain.active, 1)
        self.assertEqual(len(self.tpg.node_acls), 1)
        self.assertEqual(self.events, [])
        before = (self.directory / "deny.journal").read_bytes()
        self.dispatch()
        self.assertEqual((self.directory / "deny.journal").read_bytes(), before)

    def test_complete_requires_prepare_then_returns_uncertified_exact_observation(self):
        self.document["action"] = "complete_off"
        with self.assertRaises(TargetJournalError):
            self.dispatch()
        self.assertEqual(self.journal.denied(), ())
        self.assertEqual(self.events, [])
        self.document["action"] = "prepare_deny"
        self.dispatch()
        self.document["action"] = "complete_off"
        result = json.loads(self.dispatch())
        self.assertEqual(result["status"], "OFF_DRAIN_UNCERTIFIED")
        self.assertEqual(result["route_phases"], [3, 3, 3, 3])
        self.assertEqual(result["operation_id"], self.identity.operation_id)
        self.assertEqual(result["target_boot_id"], self.identity.target_boot_id)
        self.assertEqual(self.events, ["teardown"])
        self.assertFalse("certificate" in result or "proven" in result)
        self.assertEqual(json.loads(self.dispatch())["status"], "OFF_DRAIN_UNCERTIFIED")
        self.assertEqual(self.events, ["teardown"])

    def test_live_guest_does_not_complete_and_keeps_prepared_obligation(self):
        self.dispatch()
        self.document["action"] = "complete_off"
        self.connection.domain.active = 1
        with self.assertRaises(TargetJournalError):
            self.dispatch()
        self.assertEqual(self.events, [])
        self.assertEqual(self.journal.denied()[0].identity, self.identity)

    def test_wrong_binding_or_types_never_mutate(self):
        before = (self.directory / "deny.journal").read_bytes()
        for key, value in (("node_id", 1), ("node_id", True), ("system_identifier", 124),
                           ("mapping_generation", self.identity.mapping_generation + 1),
                           ("protected_set_digest", "ef" * 32), ("target_boot_id", "ef" * 16),
                           ("operation_id", "00" * 16), ("attempt", 0), ("attempt", True),
                           ("daemon_boot_id", "00" * 16), ("challenge", "not-hex"),
                           ("action", "authorize_on"), ("version", True)):
            with self.subTest(field=key), self.assertRaises(TargetJournalError):
                self.dispatch(self.request({**self.document, key: value}))
            self.assertEqual((self.directory / "deny.journal").read_bytes(), before)
        self.assertEqual(self.events, [])

    def test_duplicate_unknown_noncanonical_and_oversized_json_refuse(self):
        original = _canonical(self.document)
        payloads = (original.rstrip(), b" " + original, original + b"\n",
                    b'{"version":1,"version":1}\n', b"[1]\n", b"x" * 4097,
                    _canonical({**self.document, "path": "/private/disk"}),
                    _canonical({key: value for key, value in self.document.items() if key != "attempt"}))
        for payload in payloads:
            with self.subTest(length=len(payload)), self.assertRaises(TargetJournalError):
                self.dispatch(self.request(payload=payload))
        self.assertEqual(self.journal.denied(), ())
        self.assertEqual(self.events, [])

    def test_failed_durable_prepare_never_acknowledges_or_touches_acl(self):
        with patch("target_journal.os.fsync", side_effect=OSError("private disk path")):
            with self.assertRaises(TargetJournalError) as caught:
                self.dispatch()
        self.assertTrue(self.journal.poisoned)
        self.assertNotIn("private", str(caught.exception))
        self.assertEqual(self.events, [])

    def test_changed_prepare_tuple_or_new_operation_cannot_complete(self):
        self.dispatch()
        original = (self.directory / "deny.journal").read_bytes()
        for change in ({"attempt": 2}, {"daemon_boot_id": "ef" * 16}, {"operation_id": "ee" * 16}):
            with self.subTest(change=change), self.assertRaises(TargetJournalError):
                self.dispatch(self.request({**self.document, "action": "complete_off", **change}))
            self.assertEqual((self.directory / "deny.journal").read_bytes(), original)
        self.assertEqual(self.events, [])

    def test_expired_or_untyped_request_never_mutates(self):
        for request in (replace(self.request(), deadline_mono_ns=time.monotonic_ns() - 1),
                        replace(self.request(), peer_sha256=""), self.document):
            with self.assertRaises(TargetJournalError):
                self.dispatch(request)
        self.assertEqual(self.journal.denied(), ())

    def test_real_tls_delivers_exact_prepare_and_completion(self):
        driver = transport_tests.TargetTransportTests()
        transport_tests.TargetTransportTests.setUpClass.__func__(type(driver))
        try:
            for action, status in (("prepare_deny", "DENY_RECORDED"),
                                   ("complete_off", "OFF_DRAIN_UNCERTIFIED")):
                payload = _canonical({**self.document, "action": action})
                calls, errors, response, _ = driver.exchange(struct.pack("!I", len(payload)) + payload,
                                                              handler=self.dispatch)
                self.assertEqual(errors, [])
                self.assertEqual(len(calls), 1)
                size = struct.unpack("!I", response[:4])[0]
                self.assertEqual(len(response[4:]), size)
                self.assertEqual(json.loads(response[4:])["status"], status)
            self.assertEqual(self.events, ["teardown"])
        finally:
            type(driver).doClassCleanups()


if __name__ == "__main__":
    unittest.main()
