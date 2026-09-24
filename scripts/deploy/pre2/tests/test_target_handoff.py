"""Durable completed-deny ownership, never permission to access storage.

Author: SqlRush <sqlrush@gmail.com>
"""

from dataclasses import replace
import os
from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from target_journal import TargetJournalError
import test_target_journal as journal_tests


class HandoffTests(unittest.TestCase):
    setUp_journal = journal_tests.TargetJournalTests.setUp
    open = journal_tests.TargetJournalTests.open
    create = journal_tests.TargetJournalTests.create

    def setUp(self):
        self.setUp_journal()
        self.first = journal_tests.identity()
        self.next = replace(self.first, operation_id="ab" * 16, daemon_boot_id="cd" * 16)
        self.create()

    def drain(self, journal):
        journal.deny(self.first)
        for ordinal in range(len(self.first.route_digests)):
            for phase in range(1, 4):
                journal.advance(self.first.operation_id, self.first.target_boot_id, ordinal, phase)

    def transfer(self, journal, previous=None, successor=None):
        try:
            journal.handoff(previous or self.first, successor or self.next)
        except TargetJournalError as error:
            self.fail(f"complete handoff refused: {error}")

    def test_handoff_and_restart_keep_denied_guest_with_new_owner_only(self):
        with self.open() as journal:
            self.drain(journal)
            before = self.path.read_bytes()
            self.transfer(journal)
            self.assertTrue(self.path.read_bytes().startswith(before))
            self.assertEqual([state.identity for state in journal.denied()], [self.next])
            self.assertFalse(journal.completion_recorded(self.first.operation_id, self.first.target_boot_id))
        with self.open() as journal:
            self.assertEqual([state.identity for state in journal.denied()], [self.next])
            self.assertEqual(journal.denied()[0].phases, (3, 3))
            self.assertTrue(journal.completion_recorded(self.next.operation_id, self.next.target_boot_id))

    def test_maximum_128_route_handoff_fits_existing_record_bound(self):
        self.first = replace(self.first, route_digests=tuple(f"{i + 1:064x}" for i in range(128)))
        self.next = replace(self.next, route_digests=self.first.route_digests)
        with self.open() as journal:
            self.drain(journal)
            self.transfer(journal)
        self.assertLessEqual(len(self.path.read_bytes().splitlines()[-1]) + 1, 16384)
        with self.open() as journal:
            self.assertEqual(journal.denied()[0].identity, self.next)
            self.assertEqual(journal.denied()[0].phases, (3,) * 128)

    def test_duplicate_handoff_does_not_append_or_rewind_successor(self):
        with self.open() as journal:
            self.drain(journal)
            self.transfer(journal)
            original = self.path.read_bytes()
            self.transfer(journal)
            self.assertEqual(self.path.read_bytes(), original)
            journal.deny(replace(self.next, attempt=2))
            original = self.path.read_bytes()
            with self.assertRaises(TargetJournalError):
                journal.handoff(self.first, self.next)
            self.assertEqual(self.path.read_bytes(), original)

    def test_partial_or_wrong_predecessor_does_not_append(self):
        with self.open() as journal:
            journal.deny(self.first)
            for phase in range(1, 4):
                journal.advance(self.first.operation_id, self.first.target_boot_id, 0, phase)
            original = self.path.read_bytes()
            with self.assertRaises(TargetJournalError):
                journal.handoff(self.first, self.next)
            self.assertEqual(self.path.read_bytes(), original)
            for phase in range(1, 4):
                journal.advance(self.first.operation_id, self.first.target_boot_id, 1, phase)
            original = self.path.read_bytes()
            with self.assertRaises(TargetJournalError):
                journal.handoff(replace(self.first, attempt=2), self.next)
            self.assertEqual(self.path.read_bytes(), original)

    def test_boot_guest_mapping_or_route_change_is_not_a_handoff(self):
        with self.open() as journal:
            self.drain(journal)
            original = self.path.read_bytes()
            for change in ({"operation_id": self.first.operation_id}, {"target_boot_id": "ee" * 16},
                           {"guest_uuid": "ef" * 16}, {"mapping_generation": 8},
                           {"protected_set_digest": "ba" * 32},
                           {"route_digests": tuple(reversed(self.next.route_digests))}):
                with self.subTest(change=change), self.assertRaises(TargetJournalError):
                    journal.handoff(self.first, replace(self.next, **change))
                self.assertEqual(self.path.read_bytes(), original)

    def test_retired_uuid_cannot_deny_advance_or_take_back_later(self):
        third = replace(self.next, operation_id="ed" * 16)
        with self.open() as journal:
            self.drain(journal)
            self.transfer(journal)
            self.transfer(journal, self.next, third)
        with self.open() as journal:
            original = self.path.read_bytes()
            actions = [lambda: journal.deny(self.first),
                       lambda: journal.deny(replace(self.first, attempt=99)),
                       lambda: journal.advance(self.first.operation_id, self.first.target_boot_id, 0, 1),
                       lambda: journal.handoff(self.first, self.next),
                       lambda: journal.handoff(third, self.first)]
            for action in actions:
                with self.assertRaises(TargetJournalError):
                    action()
                self.assertEqual(self.path.read_bytes(), original)
            self.assertEqual([state.identity for state in journal.denied()], [third])

    def test_handoff_fsync_failure_never_publishes_new_owner(self):
        with self.open() as journal:
            self.drain(journal)
            with patch("target_journal.os.fsync", side_effect=OSError("injected")):
                with self.assertRaises(TargetJournalError):
                    journal.handoff(self.first, self.next)
            self.assertTrue(journal.poisoned)
            with self.assertRaises(TargetJournalError):
                journal.denied()

    def test_used_successor_and_full_history_do_not_discard_tombstones(self):
        with self.open() as journal:
            self.drain(journal)
            occupied = replace(self.next, guest_uuid="ef" * 16)
            journal.deny(occupied)
            original = self.path.read_bytes()
            with self.assertRaises(TargetJournalError):
                journal.handoff(self.first, self.next)
            self.assertEqual(self.path.read_bytes(), original)
            for i in range(126):
                journal.deny(replace(self.first, operation_id=f"{i + 1:032x}", guest_uuid=f"{i + 1:032x}"))
            original = self.path.read_bytes()
            with self.assertRaises(TargetJournalError):
                journal.handoff(self.first, replace(self.next, operation_id="ed" * 16))
            self.assertEqual(self.path.read_bytes(), original)
        with self.open() as journal:
            self.assertEqual(len(journal.denied()), 128)

    def test_replay_valid_hash_does_not_accept_wrong_handoff_predecessor(self):
        import hashlib
        import json

        with self.open() as journal:
            self.drain(journal)
            self.transfer(journal)
        lines = self.path.read_bytes().splitlines()
        record = json.loads(lines[-1])
        record["payload"]["previous"]["identity_digest"] = "fe" * 32
        del record["digest"]
        canonical = lambda value: json.dumps(value, sort_keys=True, separators=(",", ":")).encode() + b"\n"
        record["digest"] = hashlib.sha256(canonical(record)).hexdigest()
        changed = b"\n".join(lines[:-1]) + b"\n" + canonical(record)
        self.path.write_bytes(changed)
        with self.assertRaises(TargetJournalError):
            self.open()
        self.assertEqual(self.path.read_bytes(), changed)

    def test_successful_handoff_survives_exit_without_cleanup(self):
        with self.open() as journal:
            self.drain(journal)
        code = ("import os,sys; sys.path[:0]=sys.argv[1:3]; "
                "from target_journal import TargetJournal; from test_target_journal import identity; "
                "from dataclasses import replace; j=TargetJournal(sys.argv[3],sys.argv[4],owner_uid=os.geteuid()); "
                "j.handoff(identity(),replace(identity(),operation_id='ab'*16,daemon_boot_id='cd'*16)); os._exit(0)")
        result = subprocess.run([sys.executable, "-c", code, str(Path(__file__).resolve().parents[1]),
                                 str(Path(__file__).resolve().parent), str(self.directory), self.inventory],
                                capture_output=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        with self.open() as journal:
            self.assertEqual([state.identity for state in journal.denied()], [self.next])


if __name__ == "__main__":
    unittest.main()
