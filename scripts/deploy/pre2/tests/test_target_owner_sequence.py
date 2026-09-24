"""Actual durable target ownership across C coordinator phase identities.

Author: SqlRush <sqlrush@gmail.com>
Only scratch journal/files; no live power or storage actions.
"""

from dataclasses import replace
import hashlib
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

import test_target_journal as base
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from target_journal import RejoinIntent, TargetJournalError, _canonical


class OwnerSequenceTests(unittest.TestCase):
    open = base.TargetJournalTests.open
    create = base.TargetJournalTests.create

    def setUp(self):
        base.TargetJournalTests.setUp(self)
        self.create()
        self.admin = base.identity()
        self.authorize = replace(self.admin, attempt=2)
        self.refresh = replace(self.admin, attempt=3)
        self.intent = RejoinIntent(123, 2, 7, 8, "ab" * 32)

    def drain(self, journal, identity):
        for ordinal in range(len(identity.route_digests)):
            for phase in range(1, 4):
                journal.advance(identity.operation_id, identity.target_boot_id, ordinal, phase)

    def prepared(self, journal):
        journal.deny(self.admin)
        self.drain(journal, self.admin)

    def ready(self, journal):
        self.prepared(journal)
        journal.authorize_rejoin(self.authorize, self.intent)
        journal.advance_rejoin(self.authorize, self.intent, 2)
        journal.advance_rejoin(self.authorize, self.intent, 3)

    def test_authorize_adopts_attempt_and_arms_atomically(self):
        with self.open() as journal:
            self.prepared(journal)
            before = journal.sequence
            journal.authorize_rejoin(self.authorize, self.intent)
            self.assertEqual(journal.sequence, before + 1)
            state = journal.denied()[0]
            self.assertEqual(state.identity, self.authorize)
            self.assertEqual((state.rejoin.intent, state.rejoin.phase), (self.intent, 1))
            self.assertFalse(journal.completion_recorded(self.admin.operation_id, self.admin.target_boot_id))
            raw = self.path.read_bytes()
            journal.authorize_rejoin(self.authorize, self.intent)
            self.assertEqual(self.path.read_bytes(), raw)
        with self.open() as journal:
            self.assertEqual(journal.denied()[0], state)

    def test_refresh_retains_access_but_retires_authorize_identity(self):
        with self.open() as journal:
            self.ready(journal)
            journal.refresh_rejoin(self.refresh, self.intent)
            state = journal.denied()[0]
            self.assertEqual(state.identity, self.refresh)
            self.assertEqual(state.rejoin.phase, 3)
            self.assertTrue(state.rejoin.refresh_started)
            raw = self.path.read_bytes()
            journal.refresh_rejoin(self.refresh, self.intent)
            for action in (lambda: journal.authorize_rejoin(self.authorize, self.intent),
                           lambda: journal.authorize_rejoin(self.refresh, self.intent),
                           lambda: journal.arm_rejoin(self.refresh, self.intent),
                           lambda: journal.refresh_rejoin(replace(self.refresh, attempt=4), self.intent),
                           lambda: journal.advance_rejoin(self.authorize, self.intent, 3)):
                with self.assertRaises(TargetJournalError):
                    action()
            self.assertEqual(self.path.read_bytes(), raw)
        with self.open() as journal:
            self.assertEqual(journal.denied()[0], state)

    def test_authorize_requires_completed_drain_and_exact_later_identity(self):
        with self.open() as journal:
            journal.deny(self.admin)
            with self.assertRaises(TargetJournalError):
                journal.authorize_rejoin(self.authorize, self.intent)
            self.drain(journal, self.admin)
            raw = self.path.read_bytes()
            variants = (self.admin, replace(self.authorize, daemon_boot_id="ee" * 16),
                        replace(self.authorize, target_boot_id="ee" * 16),
                        replace(self.authorize, mapping_generation=99),
                        replace(self.authorize, operation_id="ee" * 16),
                        replace(self.authorize, route_digests=("ee" * 32,)))
            for identity in variants:
                with self.subTest(identity=identity), self.assertRaises(TargetJournalError):
                    journal.authorize_rejoin(identity, self.intent)
            self.assertEqual(self.path.read_bytes(), raw)

    def test_refresh_before_ready_or_changed_intent_refuses(self):
        with self.open() as journal:
            self.prepared(journal)
            for phase in (0, 1, 2):
                if phase == 1:
                    journal.authorize_rejoin(self.authorize, self.intent)
                if phase == 2:
                    journal.advance_rejoin(self.authorize, self.intent, 2)
                with self.assertRaises(TargetJournalError):
                    journal.refresh_rejoin(self.refresh, self.intent)
            journal.advance_rejoin(self.authorize, self.intent, 3)
            for intent in (replace(self.intent, candidate_incarnation=9),
                           replace(self.intent, rejoin_gate_digest="ee" * 32)):
                with self.assertRaises(TargetJournalError):
                    journal.refresh_rejoin(self.refresh, intent)

    def test_restart_compensation_adopts_new_daemon_attempt_before_native_work(self):
        with self.open() as journal:
            self.ready(journal)
        cleanup = replace(self.refresh, daemon_boot_id="ee" * 16)
        with self.open() as journal:
            journal.revoke_rejoin(cleanup, self.intent)
            state = journal.denied()[0]
            self.assertEqual(state.identity, cleanup)
            self.assertEqual(state.rejoin.phase, 4)
            self.assertEqual(state.phases, (0, 0))
            journal.advance(cleanup.operation_id, cleanup.target_boot_id, 0, 1)
            later = replace(cleanup, attempt=4)
            journal.revoke_rejoin(later, self.intent)
            self.assertEqual(journal.denied()[0].phases, (1, 0))
            with self.assertRaises(TargetJournalError):
                journal.revoke_rejoin(cleanup, self.intent)
            with self.assertRaises(TargetJournalError):
                journal.advance_rejoin(self.authorize, self.intent, 3)
        with self.open() as journal:
            self.assertEqual(journal.denied()[0].identity, later)
            self.assertEqual(journal.denied()[0].phases, (1, 0))

    def test_compensation_before_authorize_delivery_never_opens_permissions(self):
        with self.open() as journal:
            self.prepared(journal)
            cleanup = replace(self.refresh, daemon_boot_id="ee" * 16)
            journal.revoke_rejoin(cleanup, self.intent)
            self.assertEqual(journal.denied()[0].rejoin.phase, 4)
            self.assertEqual(journal.denied()[0].phases, (0, 0))
            with self.assertRaises(TargetJournalError):
                journal.authorize_rejoin(self.authorize, self.intent)

    def test_same_attempt_changed_daemon_or_target_boot_cannot_be_adopted(self):
        with self.open() as journal:
            self.ready(journal)
            raw = self.path.read_bytes()
            for identity in (replace(self.authorize, daemon_boot_id="ee" * 16),
                             replace(self.refresh, target_boot_id="ee" * 16)):
                with self.assertRaises(TargetJournalError):
                    journal.revoke_rejoin(identity, self.intent)
            self.assertEqual(self.path.read_bytes(), raw)
            journal.revoke_rejoin(self.refresh, self.intent)
            with self.assertRaises(TargetJournalError):
                journal.deny(replace(self.refresh, attempt=4, target_boot_id="ee" * 16))

    def test_future_fence_tombstones_old_owner_and_requires_fresh_drain(self):
        successor = replace(self.admin, operation_id="dd" * 16, daemon_boot_id="ee" * 16)
        with self.open() as journal:
            self.ready(journal)
            journal.refresh_rejoin(self.refresh, self.intent)
            journal.supersede_rejoin(successor, self.intent.candidate_incarnation)
            state = journal.denied()[0]
            self.assertEqual(state.identity, successor)
            self.assertEqual(state.rejoin.phase, 4)
            self.assertEqual(state.phases, (0, 0))
            self.assertEqual(journal.states[self.admin.operation_id].successor_operation_id,
                             successor.operation_id)
            raw = self.path.read_bytes()
            journal.supersede_rejoin(successor, self.intent.candidate_incarnation)
            self.assertEqual(self.path.read_bytes(), raw)
            with self.assertRaises(TargetJournalError):
                journal.revoke_rejoin(replace(self.refresh, attempt=4), self.intent)
            self.drain(journal, successor)
            self.assertTrue(journal.completion_recorded(successor.operation_id, successor.target_boot_id))
            self.assertFalse(journal.completion_recorded(self.refresh.operation_id, self.refresh.target_boot_id))
        with self.open() as journal:
            self.assertEqual(journal.denied()[0].identity, successor)

    def test_future_fence_wrong_incarnation_or_mapping_keeps_existing_owner(self):
        successor = replace(self.admin, operation_id="dd" * 16)
        with self.open() as journal:
            self.ready(journal)
            raw = self.path.read_bytes()
            for identity, victim in ((successor, 7), (successor, True),
                                     (replace(successor, target_boot_id="ee" * 16), 8),
                                     (replace(successor, mapping_generation=99), 8),
                                     (self.refresh, 8)):
                with self.subTest(victim=victim), self.assertRaises(TargetJournalError):
                    journal.supersede_rejoin(identity, victim)
            self.assertEqual(self.path.read_bytes(), raw)

    def test_failed_adoption_fsync_never_publishes_new_owner(self):
        with self.open() as journal:
            self.prepared(journal)
            before = journal.denied()[0]
            with patch("target_journal.os.fsync", side_effect=OSError("injected")):
                with self.assertRaises(TargetJournalError):
                    journal.authorize_rejoin(self.authorize, self.intent)
            self.assertTrue(journal.poisoned)
            self.assertEqual(journal.states[self.admin.operation_id], before)

    def test_full_route_identity_fits_record_bound_after_adoption(self):
        self.admin = replace(self.admin, route_digests=tuple(f"{i + 1:064x}" for i in range(128)))
        self.authorize = replace(self.admin, attempt=2)
        self.refresh = replace(self.admin, attempt=3)
        with self.open() as journal:
            self.ready(journal)
            journal.refresh_rejoin(self.refresh, self.intent)
        self.assertLessEqual(max(map(len, self.path.read_bytes().splitlines(keepends=True))), 16384)
        with self.open() as journal:
            self.assertEqual(journal.denied()[0].identity, self.refresh)

    def test_each_successor_fsync_failure_keeps_previous_published_owner(self):
        for action in ("refresh", "revoke", "fence"):
            # A fresh real journal per failure; a poisoned one cannot be reused.
            with self.subTest(action=action):
                self.setUp()
                with self.open() as journal:
                    self.ready(journal)
                    before = dict(journal.states)
                    successor = replace(self.admin, operation_id="dd" * 16)
                    apply = {"refresh": lambda: journal.refresh_rejoin(self.refresh, self.intent),
                             "revoke": lambda: journal.revoke_rejoin(self.refresh, self.intent),
                             "fence": lambda: journal.supersede_rejoin(successor, 8)}[action]
                    with patch("target_journal.os.fsync", side_effect=OSError("injected")):
                        with self.assertRaises(TargetJournalError):
                            apply()
                    self.assertTrue(journal.poisoned)
                    self.assertEqual(journal.states, before)

    def test_checksum_valid_but_invalid_successor_refuses_replay(self):
        with self.open() as journal:
            self.ready(journal)
            journal.refresh_rejoin(self.refresh, self.intent)
        original = self.path.read_bytes().splitlines(keepends=True)
        for change in ("boot", "attempt", "action", "intent", "predecessor"):
            record = json.loads(original[-1])
            payload = record["payload"]
            if change == "boot":
                payload["successor"]["target_boot_id"] = "ee" * 16
            elif change == "attempt":
                payload["successor"]["attempt"] = 1
            elif change == "action":
                payload["action"] = "authorize"
            elif change == "intent":
                payload["intent"]["candidate_incarnation"] = 99
            else:
                payload["previous"]["identity_digest"] = "ee" * 32
            del record["digest"]
            record["digest"] = hashlib.sha256(_canonical(record)).hexdigest()
            changed = b"".join(original[:-1]) + _canonical(record)
            self.path.write_bytes(changed)
            with self.subTest(change=change), self.assertRaises(TargetJournalError):
                self.open()
            self.assertEqual(self.path.read_bytes(), changed)

    def test_later_admin_cannot_reuse_candidate_after_new_fence(self):
        with self.open() as journal:
            self.ready(journal)
            fenced = replace(self.admin, operation_id="dd" * 16)
            journal.supersede_rejoin(fenced, 8)
            self.drain(journal, fenced)
            admin = replace(fenced, operation_id="ee" * 16)
            journal.handoff(fenced, admin)
            authorize = replace(admin, attempt=2)
            with self.assertRaises(TargetJournalError):
                journal.authorize_rejoin(authorize, self.intent)
            fresh = replace(self.intent, old_incarnation=8, candidate_incarnation=9,
                            rejoin_gate_digest="ef" * 32)
            journal.authorize_rejoin(authorize, fresh)
            self.assertEqual(journal.denied()[0].rejoin.intent, fresh)


if __name__ == "__main__":
    unittest.main()
