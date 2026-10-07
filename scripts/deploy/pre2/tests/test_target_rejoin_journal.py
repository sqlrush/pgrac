"""Production journal transitions for controlled rejoin, not native permission.

Author: SqlRush <sqlrush@gmail.com>
Temporary durable files only; no VM, export or database mutation.
"""

from dataclasses import replace
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import patch

import test_target_journal as journal_tests
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from target_journal import TargetJournalError, _canonical
try:
    from target_journal import RejoinIntent
except ImportError:
    RejoinIntent = None


class RejoinJournalTests(unittest.TestCase):
    open = journal_tests.TargetJournalTests.open
    create = journal_tests.TargetJournalTests.create

    def setUp(self):
        self.assertTrue(callable(RejoinIntent), "durable target rejoin intent is missing")
        journal_tests.TargetJournalTests.setUp(self)
        self.create()
        self.identity = journal_tests.identity()
        self.intent = RejoinIntent(123, 2, 7, 8, "ab" * 32)

    def drain(self, journal):
        for ordinal in range(len(self.identity.route_digests)):
            for phase in range(1, 4):
                journal.advance(self.identity.operation_id, self.identity.target_boot_id, ordinal, phase)

    def prepared(self, journal):
        journal.deny(self.identity)
        self.drain(journal)

    def test_arm_retains_deny_but_retires_old_completion_and_replays(self):
        with self.open() as journal:
            self.prepared(journal)
            self.assertTrue(journal.completion_recorded(self.identity.operation_id, self.identity.target_boot_id))
            journal.arm_rejoin(self.identity, self.intent)
            self.assertEqual(journal.denied()[0].rejoin.intent, self.intent)
            self.assertEqual(journal.denied()[0].rejoin.phase, 1)
            self.assertFalse(journal.completion_recorded(self.identity.operation_id, self.identity.target_boot_id))
            before = self.path.read_bytes()
            journal.arm_rejoin(self.identity, self.intent)
            self.assertEqual(self.path.read_bytes(), before)
        with self.open() as journal:
            self.assertEqual(journal.denied()[0].identity, self.identity)
            self.assertEqual(journal.denied()[0].rejoin.intent, self.intent)
            self.assertFalse(journal.completion_recorded(self.identity.operation_id, self.identity.target_boot_id))

    def test_only_ordered_access_edges_and_exact_duplicates(self):
        with self.open() as journal:
            self.prepared(journal)
            journal.arm_rejoin(self.identity, self.intent)
            with self.assertRaises(TargetJournalError):
                journal.advance_rejoin(self.identity, self.intent, 3)
            for phase in (2, 3):
                journal.advance_rejoin(self.identity, self.intent, phase)
                before = self.path.read_bytes()
                journal.advance_rejoin(self.identity, self.intent, phase)
                journal.arm_rejoin(self.identity, self.intent)
                self.assertEqual(self.path.read_bytes(), before)
                self.assertFalse(journal.completion_recorded(self.identity.operation_id, self.identity.target_boot_id))
            with self.assertRaises(TargetJournalError):
                journal.advance_rejoin(self.identity, self.intent, 2)

    def test_revocation_resets_drain_and_refuses_late_restore(self):
        for initial in (1, 2, 3):
            identity = replace(self.identity, operation_id=f"{initial + 20:032x}", guest_uuid=f"{initial + 20:032x}")
            with self.open() as journal:
                previous = self.identity
                self.identity = identity
                self.prepared(journal)
                journal.arm_rejoin(identity, self.intent)
                for phase in range(2, initial + 1):
                    journal.advance_rejoin(identity, self.intent, phase)
                journal.advance_rejoin(identity, self.intent, 4)
                state = journal.states[identity.operation_id]
                self.assertEqual(state.phases, (0, 0))
                before = self.path.read_bytes()
                for phase in (2, 3):
                    with self.assertRaises(TargetJournalError):
                        journal.advance_rejoin(identity, self.intent, phase)
                with self.assertRaises(TargetJournalError):
                    journal.arm_rejoin(identity, self.intent)
                self.assertEqual(self.path.read_bytes(), before)
                self.drain(journal)
                self.assertTrue(journal.completion_recorded(identity.operation_id, identity.target_boot_id))
                before = self.path.read_bytes()
                journal.advance_rejoin(identity, self.intent, 4)
                self.assertEqual(self.path.read_bytes(), before)
                self.assertEqual(journal.states[identity.operation_id].phases, (3, 3))
                self.identity = previous

    def test_active_intent_cannot_be_erased_by_generic_deny_route_or_handoff(self):
        with self.open() as journal:
            self.prepared(journal)
            journal.arm_rejoin(self.identity, self.intent)
            before = self.path.read_bytes()
            actions = (lambda: journal.deny(self.identity),
                       lambda: journal.deny(replace(self.identity, attempt=2)),
                       lambda: journal.advance(self.identity.operation_id, self.identity.target_boot_id, 0, 1),
                       lambda: journal.handoff(self.identity, replace(self.identity, operation_id="ef" * 16)))
            for action in actions:
                with self.assertRaises(TargetJournalError):
                    action()
                self.assertEqual(self.path.read_bytes(), before)

    def test_revoke_survives_new_attempt_and_old_attempt_cannot_publish(self):
        with self.open() as journal:
            self.prepared(journal)
            journal.arm_rejoin(self.identity, self.intent)
            journal.advance_rejoin(self.identity, self.intent, 4)
            new = replace(self.identity, attempt=2, daemon_boot_id="ee" * 16)
            journal.deny(new)
            self.assertEqual(journal.denied()[0].rejoin.phase, 4)
            with self.assertRaises(TargetJournalError):
                journal.advance_rejoin(self.identity, self.intent, 4)
        with self.open() as journal:
            self.assertEqual(journal.denied()[0].identity, new)
            self.assertEqual(journal.denied()[0].rejoin.intent, self.intent)

    def test_new_operation_after_revoke_requires_new_candidate_and_keeps_tombstone(self):
        with self.open() as journal:
            self.prepared(journal)
            journal.arm_rejoin(self.identity, self.intent)
            journal.advance_rejoin(self.identity, self.intent, 4)
            self.drain(journal)
            new = replace(self.identity, operation_id="ef" * 16)
            journal.handoff(self.identity, new)
            with self.assertRaises(TargetJournalError):
                journal.arm_rejoin(new, self.intent)
            newer = replace(self.intent, old_incarnation=8, candidate_incarnation=9, rejoin_gate_digest="ef" * 32)
            journal.arm_rejoin(new, newer)
            self.assertEqual(journal.states[self.identity.operation_id].rejoin.intent, self.intent)
            self.assertEqual(journal.denied()[0].rejoin.intent, newer)
            with self.assertRaises(TargetJournalError):
                journal.advance_rejoin(self.identity, self.intent, 4)

    def test_no_partial_drain_wrong_identity_or_invalid_intent_can_arm(self):
        with self.open() as journal:
            journal.deny(self.identity)
            with self.assertRaises(TargetJournalError):
                journal.arm_rejoin(self.identity, self.intent)
            self.drain(journal)
            with self.assertRaises(TargetJournalError):
                journal.arm_rejoin(replace(self.identity, attempt=2), self.intent)
            for change in ({"old_node_id": True}, {"old_node_id": -1}, {"system_identifier": 0},
                           {"old_incarnation": 0}, {"candidate_incarnation": 7},
                           {"candidate_incarnation": 1 << 64}, {"rejoin_gate_digest": "00" * 32}):
                with self.subTest(change=change), self.assertRaises(TargetJournalError):
                    journal.arm_rejoin(self.identity, replace(self.intent, **change))

    def test_changed_intent_or_phase_types_never_overwrite(self):
        with self.open() as journal:
            self.prepared(journal)
            journal.arm_rejoin(self.identity, self.intent)
            before = self.path.read_bytes()
            for change in ({"system_identifier": 124}, {"old_node_id": 3},
                           {"candidate_incarnation": 9}, {"rejoin_gate_digest": "ee" * 32}):
                intent = replace(self.intent, **change)
                with self.subTest(change=change), self.assertRaises(TargetJournalError):
                    journal.arm_rejoin(self.identity, intent)
                with self.assertRaises(TargetJournalError):
                    journal.advance_rejoin(self.identity, intent, 2)
            for phase in (True, 0, 1, 2.0, 5):
                with self.subTest(phase=phase), self.assertRaises(TargetJournalError):
                    journal.advance_rejoin(self.identity, self.intent, phase)
            self.assertEqual(self.path.read_bytes(), before)

    def test_fsync_failure_poisoning_never_publishes_intent(self):
        with self.open() as journal:
            self.prepared(journal)
            with patch("target_journal.os.fsync", side_effect=OSError("private native path")):
                with self.assertRaises(TargetJournalError):
                    journal.arm_rejoin(self.identity, self.intent)
            self.assertTrue(journal.poisoned)
            self.assertIsNone(journal.states[self.identity.operation_id].rejoin)
            with self.assertRaises(TargetJournalError):
                journal.denied()

    def test_128_routes_fit_record_bound_and_replay_full_identity(self):
        self.identity = replace(self.identity, route_digests=tuple(f"{i + 1:064x}" for i in range(128)))
        with self.open() as journal:
            self.prepared(journal)
            journal.arm_rejoin(self.identity, self.intent)
        self.assertLessEqual(len(self.path.read_bytes().splitlines()[-1]) + 1, 16384)
        with self.open() as journal:
            self.assertEqual(journal.denied()[0].identity, self.identity)
            self.assertEqual(journal.denied()[0].rejoin.intent, self.intent)

    def test_semantically_invalid_record_with_valid_checksum_still_refuses_replay(self):
        with self.open() as journal:
            self.prepared(journal)
            journal.arm_rejoin(self.identity, self.intent)
        original = self.path.read_bytes().splitlines(keepends=True)
        for change in ("wrong_identity", "invalid_candidate", "skipped_phase"):
            record = json.loads(original[-1])
            if change == "wrong_identity":
                record["payload"]["identity"]["identity_digest"] = "ef" * 32
            elif change == "invalid_candidate":
                record["payload"]["intent"]["candidate_incarnation"] = 7
            else:
                record["kind"], record["payload"]["phase"] = "REJOIN_PHASE", 3
            del record["digest"]
            record["digest"] = hashlib.sha256(_canonical(record)).hexdigest()
            changed = b"".join(original[:-1]) + _canonical(record)
            self.path.write_bytes(changed)
            with self.subTest(change=change), self.assertRaises(TargetJournalError):
                self.open()
            self.assertEqual(self.path.read_bytes(), changed)

    def test_each_phase_fsync_failure_poisoning_preserves_last_published_state(self):
        for failed_phase in (2, 3, 4):
            identity = replace(self.identity, operation_id=f"{failed_phase + 40:032x}",
                               guest_uuid=f"{failed_phase + 40:032x}")
            with self.open() as journal:
                self.identity = identity
                self.prepared(journal)
                journal.arm_rejoin(identity, self.intent)
                if failed_phase == 3:
                    journal.advance_rejoin(identity, self.intent, 2)
                previous = journal.states[identity.operation_id]
                with patch("target_journal.os.fsync", side_effect=OSError("injected")):
                    with self.assertRaises(TargetJournalError):
                        journal.advance_rejoin(identity, self.intent, failed_phase)
                self.assertEqual(journal.states[identity.operation_id], previous)
                self.assertTrue(journal.poisoned)

    def test_arm_survives_abrupt_process_exit_without_clearing_deny(self):
        with self.open() as journal:
            self.prepared(journal)
        code = ("import os,sys; sys.path[:0]=sys.argv[1:3]; "
                "from target_journal import TargetJournal,RejoinIntent; from test_target_journal import identity; "
                "j=TargetJournal(sys.argv[3],sys.argv[4],owner_uid=os.geteuid()); "
                "j.arm_rejoin(identity(),RejoinIntent(123,2,7,8,'ab'*32)); os._exit(0)")
        result = subprocess.run([sys.executable, "-c", code, str(Path(__file__).resolve().parents[1]),
                                 str(Path(__file__).resolve().parent), str(self.directory), self.inventory],
                                capture_output=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        with self.open() as journal:
            self.assertEqual(journal.denied()[0].rejoin.phase, 1)
            self.assertFalse(journal.completion_recorded(self.identity.operation_id, self.identity.target_boot_id))


if __name__ == "__main__":
    unittest.main()
