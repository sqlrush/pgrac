"""Explicit authenticated-owner boot reconciliation, never physical proof.

Author: SqlRush <sqlrush@gmail.com>
Real journal, ordered transitions, fsync and replay; no target actions.
"""

from dataclasses import replace
import unittest
from unittest.mock import patch

import test_target_rejoin_journal as rejoin_tests
from target_journal import TargetJournal, TargetJournalError


class BootJournalTests(unittest.TestCase):
    open = rejoin_tests.RejoinJournalTests.open
    create = rejoin_tests.RejoinJournalTests.create
    drain = rejoin_tests.RejoinJournalTests.drain
    prepared = rejoin_tests.RejoinJournalTests.prepared

    def setUp(self):
        self.assertTrue(callable(getattr(TargetJournal, "reconcile_boot", None)),
                        "explicit closed boot transition is missing")
        rejoin_tests.RejoinJournalTests.setUp(self)
        self.new = replace(self.identity, attempt=2, daemon_boot_id="dd" * 16, target_boot_id="ee" * 16)

    def test_new_boot_keeps_identity_history_and_resets_all_completion(self):
        with self.open() as journal:
            self.prepared(journal)
            previous_bytes = self.path.read_bytes()
            journal.reconcile_boot(self.new)
            self.assertTrue(self.path.read_bytes().startswith(previous_bytes))
            self.assertEqual(journal.denied()[0].identity, self.new)
            self.assertEqual(journal.denied()[0].phases, (0, 0))
            for boot in (self.identity.target_boot_id, self.new.target_boot_id):
                self.assertFalse(journal.completion_recorded(self.new.operation_id, boot))
            journal.advance(self.new.operation_id, self.new.target_boot_id, 0, 1)
            before = self.path.read_bytes()
            journal.reconcile_boot(self.new)
            self.assertEqual(self.path.read_bytes(), before)
        with self.open() as journal:
            self.assertEqual(journal.denied()[0].identity, self.new)
            self.assertEqual(journal.denied()[0].phases, (1, 0))

    def test_every_rejoin_phase_requires_exact_revoke_intent_and_never_rearms(self):
        for old_phase in (1, 2, 3, 4):
            with self.subTest(phase=old_phase), self.open() as journal:
                identity = replace(self.identity, operation_id=f"{old_phase + 30:032x}",
                                   guest_uuid=f"{old_phase + 30:032x}")
                journal.deny(identity)
                for ordinal in range(2):
                    for phase in (1, 2, 3):
                        journal.advance(identity.operation_id, identity.target_boot_id, ordinal, phase)
                journal.arm_rejoin(identity, self.intent)
                for phase in range(2, old_phase + 1):
                    journal.advance_rejoin(identity, self.intent, phase)
                requested = replace(identity, attempt=2, target_boot_id=self.new.target_boot_id)
                with self.assertRaises(TargetJournalError):
                    journal.reconcile_boot(requested)
                with self.assertRaises(TargetJournalError):
                    journal.reconcile_boot(requested, intent=replace(self.intent, candidate_incarnation=9))
                journal.reconcile_boot(requested, intent=self.intent)
                state = journal.states[identity.operation_id]
                self.assertEqual(state.phases, (0, 0))
                self.assertEqual((state.rejoin.intent, state.rejoin.phase), (self.intent, 4))
                with self.assertRaises(TargetJournalError):
                    journal.advance_rejoin(identity, self.intent, 3)
                with self.assertRaises(TargetJournalError):
                    journal.authorize_rejoin(replace(requested, attempt=3), self.intent)
                before = self.path.read_bytes()
                journal.reconcile_boot(requested, intent=self.intent)
                self.assertEqual(self.path.read_bytes(), before)

    def test_attempt_boot_mapping_route_and_operation_must_be_exact(self):
        with self.open() as journal:
            self.prepared(journal)
            before = self.path.read_bytes()
            bad = (replace(self.new, attempt=1), replace(self.new, target_boot_id=self.identity.target_boot_id),
                   replace(self.new, operation_id="ff" * 16), replace(self.new, guest_uuid="ff" * 16),
                   replace(self.new, mapping_generation=6), replace(self.new, protected_set_digest="ff" * 32),
                   replace(self.new, route_digests=tuple(reversed(self.new.route_digests))))
            for requested in bad:
                with self.subTest(requested=requested), self.assertRaises(TargetJournalError):
                    journal.reconcile_boot(requested)
                self.assertEqual(self.path.read_bytes(), before)
            with self.assertRaises(TargetJournalError):
                journal.reconcile_boot(self.new, intent=self.intent)

    def test_tombstoned_owner_and_delayed_old_route_callbacks_refuse(self):
        with self.open() as journal:
            self.prepared(journal)
            successor = replace(self.identity, operation_id="ab" * 16)
            journal.handoff(self.identity, successor)
            with self.assertRaises(TargetJournalError):
                journal.reconcile_boot(self.new)
            journal.reconcile_boot(replace(successor, attempt=2, target_boot_id=self.new.target_boot_id))
            for identity in (self.identity, successor):
                with self.assertRaises(TargetJournalError):
                    journal.advance(identity.operation_id, identity.target_boot_id, 0, 1)

    def test_failed_fsync_cannot_publish_reconciled_identity(self):
        with self.open() as journal:
            self.prepared(journal)
            with patch("target_journal.os.fsync", side_effect=OSError("secret native path")):
                with self.assertRaises(TargetJournalError):
                    journal.reconcile_boot(self.new)
            self.assertTrue(journal.poisoned)
            self.assertEqual(journal.states[self.identity.operation_id].identity, self.identity)
            with self.assertRaises(TargetJournalError):
                journal.completion_recorded(self.identity.operation_id, self.identity.target_boot_id)

    def test_maximum_route_record_replays_without_reset_or_truncation(self):
        self.identity = replace(self.identity, route_digests=tuple(f"{i + 100:064x}" for i in range(128)))
        requested = replace(self.identity, attempt=2, target_boot_id=self.new.target_boot_id)
        with self.open() as journal:
            journal.deny(self.identity)
            journal.reconcile_boot(requested)
        with self.open() as journal:
            self.assertEqual(journal.denied()[0].identity, requested)
            self.assertEqual(journal.denied()[0].phases, (0,) * 128)


if __name__ == "__main__":
    unittest.main()
