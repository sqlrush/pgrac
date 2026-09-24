"""Authenticated closed-mode command boundary over the real reconciliation.

Author: SqlRush <sqlrush@gmail.com>
Native edges only are fixtures; old boot never grants access or a certificate.
"""

import json
import time
import unittest
from unittest.mock import patch

import test_target_closed_reconcile as closed_tests
from target_dispatch import dispatch_target
from target_journal import TargetJournalError, _canonical
from target_transport import AuthenticatedTargetRequest


class ClosedDispatchTests(unittest.TestCase):
    def setUp(self):
        self.c = closed_tests.ClosedReconcileTests()
        self.addCleanup(self.c.doCleanups)
        self.c.setUp()
        self.f = self.c.f
        self.boot = patch("target_operation._kernel_boot_id", return_value=self.f.identity.target_boot_id)
        self.boot.start()
        self.addCleanup(self.boot.stop)
        i, m = self.f.identity, self.f.registry.node(2).mapping
        self.command = dict(version=1, action="prepare_deny", challenge="ab" * 16,
            node_id=2, system_identifier=m.system_identifier, mapping_generation=m.mapping_generation,
            protected_set_digest=m.protected_set_digest, operation_id=i.operation_id,
            attempt=i.attempt, daemon_boot_id=i.daemon_boot_id, target_boot_id=i.target_boot_id)

    def dispatch(self, command=None, *, closed=True):
        request = AuthenticatedTargetRequest("ab" * 32, _canonical(command or self.command),
                                              time.monotonic_ns() + 5_000_000_000)
        return json.loads(dispatch_target(request, self.f.registry, self.f.journal, self.f.root,
            self.f.connection, startup_config=self.f.template, closed_reconcile=closed))

    def with_intent(self, action):
        i = self.f.intent
        return dict(self.command, action=action, old_incarnation=i.old_incarnation,
                    candidate_incarnation=i.candidate_incarnation, rejoin_gate_digest=i.rejoin_gate_digest)

    def test_closed_prepare_then_complete_has_exact_original_reply_shape(self):
        first = self.dispatch()
        self.assertEqual(first["status"], "DENY_RECORDED")
        self.assertEqual(first["target_boot_id"], self.f.identity.target_boot_id)
        self.command["action"] = "complete_off"
        result = self.dispatch()
        self.assertEqual(result["status"], "OFF_DRAIN_UNCERTIFIED")
        self.assertEqual(result["route_phases"], [3, 3, 3, 3])
        self.assertNotIn("certificate", result)

    def test_closed_identity_sees_current_boot_but_keeps_old_obligation_untouched(self):
        before = self.f.journal.sequence
        result = self.dispatch(dict(version=1, action="identity", challenge="ab" * 16))
        self.assertEqual(result["target_boot_id"], self.f.identity.target_boot_id)
        self.assertEqual(self.f.journal.sequence, before)
        self.assertEqual(self.f.journal.denied()[0].identity, self.c.old)

    def test_closed_rejoin_only_explicit_revocation_can_cross_boot(self):
        self.f.journal.arm_rejoin(self.c.old, self.f.intent)
        self.assertEqual(self.dispatch(self.with_intent("rejoin_prepare_revoke"))["status"], "REJOIN_REVOKED")
        result = self.dispatch(self.with_intent("rejoin_complete_off"))
        self.assertEqual(result["route_phases"], [3] * 4)

    def test_restore_running_and_nonboolean_mode_never_mutate(self):
        before = self.f.journal.sequence
        for command in (self.with_intent("rejoin_restore"),
                        dict(self.with_intent("rejoin_running"), owner_phase="authorize")):
            with self.assertRaises(TargetJournalError):
                self.dispatch(command)
        with self.assertRaises(TargetJournalError):
            self.dispatch(closed=1)
        self.assertEqual(self.f.journal.sequence, before)
        self.assertEqual(self.f.events, [])

    def test_even_identity_refuses_an_open_export_in_closed_mode(self):
        self.f.config[self.f.tpg.path + "/enable"] = "1\n"
        with self.assertRaises(TargetJournalError):
            self.dispatch(dict(version=1, action="identity", challenge="ab" * 16))
        self.assertEqual(self.f.journal.denied()[0].identity, self.c.old)

    def test_ordinary_mode_cannot_inherit_closed_partial_route_exception(self):
        with self.assertRaises(TargetJournalError):
            self.dispatch(closed=False)
        self.assertEqual(self.f.journal.denied()[0].identity, self.c.old)


if __name__ == "__main__":
    unittest.main()
