"""Authenticated rejoin commands through real journal and native composition.

Author: SqlRush <sqlrush@gmail.com>
Native fixture calls never operate a real VM or target.
"""

from dataclasses import replace
import json
from pathlib import Path
import sys
import time
import unittest
from unittest.mock import patch

import test_target_permissions as permission_tests
import test_target_client as client_tests
import test_target_transport as transport_tests
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from target_dispatch import dispatch_target
from target_journal import TargetJournalError, _canonical
from target_transport import AuthenticatedTargetRequest


class RejoinDispatchTests(unittest.TestCase):
    exchange = client_tests.TargetClientTests.exchange
    client_context = client_tests.TargetClientTests.client_context

    @classmethod
    def setUpClass(cls):
        client_tests.TargetClientTests.setUpClass.__func__(cls)

    def setUp(self):
        self.f = permission_tests.PermissionTests()
        self.f.arm_initial = False
        self.addCleanup(self.f.doCleanups)
        self.f.setUp()
        self.f.patch_native()
        for name in ("registry", "journal", "root", "connection"):
            setattr(self, name, getattr(self.f, name))
        self.f.identity = replace(self.f.identity, attempt=self.f.identity.attempt + 1)
        boot = patch("target_operation._kernel_boot_id", return_value=self.f.identity.target_boot_id)
        boot.start()
        self.addCleanup(boot.stop)
        mapping = self.registry.nodes[0].mapping
        identity, intent = self.f.identity, self.f.intent
        self.command = {"version": 1, "action": "rejoin_restore", "node_id": intent.old_node_id,
                        "system_identifier": intent.system_identifier, "mapping_generation": mapping.mapping_generation,
                        "protected_set_digest": mapping.protected_set_digest,
                        "operation_id": identity.operation_id, "attempt": identity.attempt,
                        "daemon_boot_id": identity.daemon_boot_id, "target_boot_id": identity.target_boot_id,
                        "challenge": "ab" * 16, "old_incarnation": intent.old_incarnation,
                        "candidate_incarnation": intent.candidate_incarnation,
                        "rejoin_gate_digest": intent.rejoin_gate_digest}
        self.tls = transport_tests.TargetTransportTests()
        self.tls.directory, self.tls.pin = self.tls_directory, self.pin

    def dispatch(self, request=None):
        if request is None:
            request = AuthenticatedTargetRequest(self.pin, _canonical(self.command),
                                                 time.monotonic_ns() + 5_000_000_000)
        return dispatch_target(request, self.registry, self.journal, self.root, self.connection,
                               startup_config=self.f.template)

    def send(self, **kwargs):
        return self.exchange(handler=self.dispatch, **kwargs)

    def running(self):
        self.connection.domain.active, self.connection.domain.status = 1, (1, 1)
        self.connection.domain.ID = lambda: 7

    def off(self):
        self.connection.domain.active, self.connection.domain.status = 0, (5, 2)

    def test_restore_authenticates_intent_and_is_not_power_or_certificate(self):
        result = self.send()
        self.assertEqual(result["status"], "ACCESS_READY_UNCERTIFIED")
        self.assertEqual(result["candidate_incarnation"], 8)
        self.assertEqual(result["attempt"], 2)
        self.assertEqual(self.journal.denied()[0].identity, self.f.identity)
        self.assertEqual(self.journal.denied()[0].rejoin.phase, 3)
        self.assertEqual(self.connection.domain.active, 0)
        self.assertEqual(self.f.events, ["create-acl", "create-lun-0", "create-lun-1"])
        before = (self.journal.sequence, list(self.f.events))
        self.send()
        self.assertEqual((self.journal.sequence, self.f.events), before)
        self.assertNotIn("certificate", result)

    def test_running_authorize_then_refresh_then_new_attempt_compensation(self):
        self.send()
        self.running()
        self.command.update(action="rejoin_running", owner_phase="authorize")
        self.assertEqual(self.send()["runtime_id"], 7)
        self.command.update(owner_phase="refresh", attempt=3)
        self.assertEqual(self.send()["status"], "REJOIN_RUNNING_UNCERTIFIED")
        self.assertTrue(self.journal.denied()[0].rejoin.refresh_started)
        self.command.pop("owner_phase")
        self.command.update(action="rejoin_prepare_revoke", attempt=4, daemon_boot_id="ee" * 16)
        self.assertEqual(self.send()["status"], "REJOIN_REVOKED")
        self.assertEqual(self.connection.domain.active, 1)
        self.assertEqual(len(self.f.tpg.node_acls), 1)
        self.command["action"] = "rejoin_complete_off"
        with self.assertRaises(TargetJournalError):
            self.send()
        self.off()
        self.assertEqual(self.send()["route_phases"], [3, 3, 3, 3])
        self.assertEqual(self.send()["status"], "OFF_DRAIN_UNCERTIFIED")
        self.assertEqual(self.f.tpg.node_acls, [])

    def test_live_guest_cannot_arm_or_restore(self):
        self.running()
        before = self.journal.sequence
        with self.assertRaises(TargetJournalError):
            self.dispatch()
        self.assertEqual(self.journal.sequence, before)
        self.assertEqual(self.f.events, [])
        self.assertIsNone(self.journal.denied()[0].rejoin)

    def test_invalid_or_extra_intent_fields_do_not_mutate(self):
        before = self.journal.sequence
        original = dict(self.command)
        for change in ({"candidate_incarnation": 7}, {"old_incarnation": True},
                       {"rejoin_gate_digest": "00" * 32}, {"candidate_incarnation": 1 << 64},
                       {"secret_path": "/not-allowed"}, {"owner_phase": "authorize"}):
            self.command = {**original, **change}
            with self.subTest(change=change), self.assertRaises(TargetJournalError):
                self.dispatch()
        self.command = {key: value for key, value in original.items() if key != "old_incarnation"}
        with self.assertRaises(TargetJournalError):
            self.dispatch()
        self.assertEqual(self.journal.sequence, before)
        self.assertEqual(self.f.events, [])

    def test_wrong_running_phase_or_stale_authorize_never_changes_access(self):
        self.send()
        restore = dict(self.command)
        self.running()
        self.command.update(action="rejoin_running", owner_phase="refresh", attempt=3)
        self.send()
        before = (self.journal.sequence, list(self.f.events))
        with self.assertRaises(TargetJournalError):
            self.send(command={**self.command, "owner_phase": "authorize"})
        with self.assertRaises(TargetJournalError):
            self.send(command=restore)
        self.assertEqual((self.journal.sequence, self.f.events), before)

    def test_runtime_change_between_permission_censuses_is_unproven(self):
        self.send()
        self.running()
        runtime = iter((7, 7, 8, 8))
        self.connection.domain.ID = lambda: next(runtime)
        self.command.update(action="rejoin_running", owner_phase="authorize")
        with self.assertRaises(TargetJournalError):
            self.send()
        self.assertEqual(self.journal.denied()[0].rejoin.phase, 3)

    def test_running_with_wrong_chap_or_partial_routes_is_not_ready(self):
        self.send()
        self.running()
        self.f.acl.chap_password = "wrong-test-secret"
        self.command.update(action="rejoin_running", owner_phase="authorize")
        with self.assertRaises(TargetJournalError):
            self.send()
        self.f.acl.chap_password = "test-only-secret"
        self.f.acl.mapped_luns.pop()
        with self.assertRaises(TargetJournalError):
            self.send()

    def test_complete_off_requires_prepared_current_revocation(self):
        self.send()
        self.command["action"] = "rejoin_complete_off"
        before = self.journal.sequence
        with self.assertRaises(TargetJournalError):
            self.send()
        self.assertEqual(self.journal.sequence, before)
        self.assertEqual(len(self.f.tpg.node_acls), 1)

    def test_lost_restore_reply_keeps_durable_ready_and_exact_retry_is_read_only(self):
        def lose(request):
            self.dispatch(request)
            raise OSError("test lost reply")
        with self.assertRaises(TargetJournalError):
            self.exchange(handler=lose)
        self.assertEqual(self.journal.denied()[0].rejoin.phase, 3)
        before = (self.journal.sequence, list(self.f.events))
        self.assertEqual(self.send()["status"], "ACCESS_READY_UNCERTIFIED")
        self.assertEqual((self.journal.sequence, self.f.events), before)

    def test_partial_restore_is_owned_revocation_not_fake_ready(self):
        actual = self.f.create_lun
        def fail_second(acl, index, tpg_lun):
            if index == 1:
                raise OSError("test-secret")
            return actual(acl, index, tpg_lun)
        with patch("target_permissions._create_lun", side_effect=fail_second), self.assertRaises(TargetJournalError):
            self.send()
        self.assertEqual(self.journal.denied()[0].rejoin.phase, 4)
        self.command.update(action="rejoin_prepare_revoke", attempt=3)
        self.assertEqual(self.send()["status"], "REJOIN_REVOKED")
        self.command["action"] = "rejoin_complete_off"
        self.assertEqual(self.send()["route_phases"], [3, 3, 3, 3])

    def test_running_reply_must_echo_all_identity_and_runtime_fields(self):
        self.send()
        self.running()
        self.command.update(action="rejoin_running", owner_phase="authorize")
        for key, value in (("candidate_incarnation", 9), ("attempt", 99),
                           ("owner_phase", "refresh"), ("runtime_id", True),
                           ("runtime_id", 0), ("runtime_id", (1 << 32) - 1),
                           ("status", "PROVEN")):
            def changed(request, key=key, value=value):
                reply = json.loads(self.dispatch(request))
                return _canonical({**reply, key: value})
            with self.subTest(field=key, value=value), self.assertRaises(TargetJournalError):
                self.exchange(handler=changed)

    def test_compensating_off_reply_requires_all_route_phases(self):
        self.send()
        self.command.update(action="rejoin_prepare_revoke", attempt=3)
        self.send()
        self.command["action"] = "rejoin_complete_off"
        for phases in ([3] * 3, [3, 3, 3, True], [3, 3, 3, 2]):
            def changed(request, phases=phases):
                reply = json.loads(self.dispatch(request))
                return _canonical({**reply, "route_phases": phases})
            with self.subTest(phases=phases), self.assertRaises(TargetJournalError):
                self.exchange(handler=changed)
        self.assertEqual(self.send()["route_phases"], [3] * 4)

    def test_native_running_does_not_erase_revocation_during_readback(self):
        self.send()
        self.running()
        self.command.update(action="rejoin_running", owner_phase="authorize")
        actual = self.f.api()._access_readback
        def revoke(*args):
            actual(*args)
            self.journal.revoke_rejoin(self.f.identity, self.f.intent)
        with patch("target_rejoin_operation._access_readback", side_effect=revoke), self.assertRaises(TargetJournalError):
            self.send()
        self.assertEqual(self.journal.denied()[0].rejoin.phase, 4)
        self.assertEqual(self.connection.domain.active, 1)

    def test_failed_arming_never_creates_permission_and_hides_native_details(self):
        with patch("target_journal.os.fsync", side_effect=OSError("test secret path")):
            with self.assertRaises(TargetJournalError) as error:
                self.dispatch()
        self.assertTrue(self.journal.poisoned)
        self.assertEqual(self.f.events, [])
        self.assertEqual(str(error.exception), "TARGET_COMMAND_UNPROVEN")


if __name__ == "__main__":
    unittest.main()
