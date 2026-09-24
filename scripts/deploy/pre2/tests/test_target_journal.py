"""Real-file deny/replay tests. No target, VM or database is operated.

Author: SqlRush <sqlrush@gmail.com>
"""

from dataclasses import replace
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from target_journal import DrainIdentity, TargetJournal, TargetJournalError


def identity(**changes):
    return replace(DrainIdentity("01" * 16, 1, "02" * 16, "03" * 16,
                                 "04" * 16, 5, "06" * 32,
                                 ("07" * 32, "08" * 32)), **changes)


class TargetJournalTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="pgrac-target-journal-")
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name).resolve()
        self.path = self.directory / "deny.journal"
        self.inventory = "09" * 32

    def open(self, **kwargs):
        return TargetJournal(self.directory, self.inventory,
                             owner_uid=os.geteuid(), **kwargs)

    def create(self):
        with self.open(initialize=True):
            pass

    def test_missing_and_empty_do_not_mean_no_denies(self):
        with self.assertRaises(TargetJournalError):
            self.open()
        self.assertFalse(self.path.exists())
        self.path.touch(mode=0o600)
        with self.assertRaises(TargetJournalError):
            self.open()

    def test_explicit_initialization_is_exclusive_and_persistent(self):
        with self.open(initialize=True) as journal:
            self.assertEqual(journal.denied(), ())
        original = self.path.read_bytes()
        self.assertTrue(original.endswith(b"\n"))
        with self.assertRaises(TargetJournalError):
            self.open(initialize=True)
        self.assertEqual(self.path.read_bytes(), original)
        with self.open() as journal:
            self.assertEqual(journal.denied(), ())
        with self.assertRaises(TargetJournalError):
            TargetJournal(self.directory, "10" * 32, owner_uid=os.geteuid())

    def test_exact_deny_replay_is_not_completion(self):
        self.create()
        request = identity()
        with self.open() as journal:
            journal.deny(request)
            durable = self.path.read_bytes()
            journal.deny(request)
            self.assertEqual(self.path.read_bytes(), durable)
            self.assertFalse(journal.completion_recorded(request.operation_id, request.target_boot_id))
        with self.open() as journal:
            self.assertEqual(journal.denied()[0].identity, request)
            self.assertEqual(journal.denied()[0].phases, (0, 0))

    def test_every_route_requires_ordered_completion(self):
        self.create()
        request = identity()
        with self.open() as journal:
            journal.deny(request)
            for n in range(2):
                with self.assertRaises(TargetJournalError):
                    journal.advance(request.operation_id, request.target_boot_id, n, 2)
                for phase in range(1, 4):
                    journal.advance(request.operation_id, request.target_boot_id, n, phase)
                    self.assertEqual(journal.completion_recorded(request.operation_id, request.target_boot_id),
                                     n == 1 and phase == 3)
        with self.open() as journal:
            self.assertTrue(journal.completion_recorded(request.operation_id, request.target_boot_id))
            self.assertFalse(journal.completion_recorded(request.operation_id, "ff" * 16))

    def test_restart_after_begin_keeps_ambiguous_obligation(self):
        self.create()
        request = identity()
        with self.open() as journal:
            journal.deny(request)
            journal.advance(request.operation_id, request.target_boot_id, 0, 1)
        with self.open() as journal:
            self.assertEqual(journal.denied()[0].phases, (1, 0))
            self.assertFalse(journal.completion_recorded(request.operation_id, request.target_boot_id))

    def test_attempt_change_retains_facts_only_for_same_target_boot(self):
        self.create()
        request = identity()
        with self.open() as journal:
            journal.deny(request)
            for route in range(2):
                for phase in range(1, 4):
                    journal.advance(request.operation_id, request.target_boot_id, route, phase)
            journal.deny(replace(request, attempt=7, daemon_boot_id="0a" * 16))
            self.assertTrue(journal.completion_recorded(request.operation_id, request.target_boot_id))
            changed = replace(request, attempt=8, target_boot_id="0b" * 16)
            journal.deny(changed)
            self.assertEqual(journal.denied()[0].phases, (0, 0))
            self.assertFalse(journal.completion_recorded(request.operation_id, changed.target_boot_id))
            with self.assertRaises(TargetJournalError):
                journal.advance(request.operation_id, request.target_boot_id, 0, 1)
        with self.open() as journal:
            self.assertEqual(journal.denied()[0].identity, changed)

    def test_attempt_identity_or_route_drift_cannot_replace_a_deny(self):
        self.create()
        request = identity(attempt=2)
        changes = [{"attempt": 1}, {"daemon_boot_id": "ab" * 16},
                   {"target_boot_id": "ac" * 16}, {"guest_uuid": "ae" * 16},
                   {"mapping_generation": 6}, {"protected_set_digest": "af" * 32},
                   {"route_digests": tuple(reversed(request.route_digests))}]
        with self.open() as journal:
            journal.deny(request)
            original = self.path.read_bytes()
            for change in changes:
                with self.subTest(change=change), self.assertRaises(TargetJournalError):
                    journal.deny(replace(request, **change))
                self.assertEqual(self.path.read_bytes(), original)
            with self.assertRaises(TargetJournalError):
                journal.deny(replace(request, operation_id="ff" * 16))
            self.assertEqual(self.path.read_bytes(), original)

    def test_distinct_guests_remain_independently_owned(self):
        self.create()
        first, second = identity(), identity(operation_id="ab" * 16, guest_uuid="cd" * 16)
        with self.open() as journal:
            journal.deny(first)
            journal.deny(second)
            journal.advance(first.operation_id, first.target_boot_id, 0, 1)
        with self.open() as journal:
            self.assertEqual([s.phases for s in journal.denied()], [(1, 0), (0, 0)])

    def test_bad_identity_and_ordinal_never_append(self):
        self.create()
        with self.open() as journal:
            for change in ({"operation_id": "00" * 16}, {"attempt": True},
                           {"attempt": 1 << 64}, {"mapping_generation": 0},
                           {"guest_uuid": "../escape"}, {"route_digests": ()},
                           {"route_digests": ("07" * 32,) * 2}):
                with self.subTest(change=change), self.assertRaises(TargetJournalError):
                    journal.deny(identity(**change))
            request = identity()
            journal.deny(request)
            original = self.path.read_bytes()
            for ordinal, phase in ((-1, 1), (2, 1), (True, 1), (0, 0), (0, 4), (0, True)):
                with self.assertRaises(TargetJournalError):
                    journal.advance(request.operation_id, request.target_boot_id, ordinal, phase)
            self.assertEqual(self.path.read_bytes(), original)

    def test_second_process_cannot_own_open_journal(self):
        self.create()
        script = ("import sys; sys.path.insert(0, sys.argv[1]); "
                  "from target_journal import TargetJournal; "
                  "TargetJournal(sys.argv[2], sys.argv[3], owner_uid=int(sys.argv[4]))")
        with self.open():
            result = subprocess.run([sys.executable, "-c", script,
                                     str(Path(__file__).resolve().parents[1]),
                                     str(self.directory), self.inventory, str(os.geteuid())],
                                    capture_output=True, timeout=10)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn(b"TARGET_JOURNAL_BUSY", result.stderr)

    def test_unsafe_file_directory_links_and_mode_refuse(self):
        self.create()
        original = self.path.read_bytes()
        linked = self.directory / "copy"
        os.link(self.path, linked)
        with self.assertRaises(TargetJournalError):
            self.open()
        linked.unlink()
        self.path.chmod(0o644)
        with self.assertRaises(TargetJournalError):
            self.open()
        self.path.chmod(0o600)
        self.path.rename(linked)
        self.path.symlink_to(linked)
        with self.assertRaises(TargetJournalError):
            self.open()
        self.assertEqual(linked.read_bytes(), original)
        self.path.unlink()
        linked.rename(self.path)
        self.directory.chmod(0o755)
        with self.assertRaises(TargetJournalError):
            self.open()
        self.directory.chmod(0o700)
        with self.assertRaises(TargetJournalError):
            TargetJournal(self.directory, self.inventory, owner_uid=os.geteuid() + 1)

    def test_symlinked_directory_is_not_followed(self):
        self.create()
        with tempfile.TemporaryDirectory(prefix="pgrac-target-link-") as temp:
            link = Path(temp).resolve() / "alias"
            link.symlink_to(self.directory, target_is_directory=True)
            with self.assertRaises(TargetJournalError):
                TargetJournal(link, self.inventory, owner_uid=os.geteuid())

    def test_partial_corrupt_duplicate_and_oversize_journal_remain_untouched(self):
        self.create()
        with self.open() as journal:
            journal.deny(identity())
        original = self.path.read_bytes()
        corruptions = [original[:-1], original[:-10], original + b"{}\n",
                       original.replace(b'"sequence":1', b'"sequence":9', 1),
                       original.replace(b'"version":1', b'"version":1,"version":1', 1),
                       b"x" * (4 * 1024 * 1024 + 1)]
        for raw in corruptions:
            with self.subTest(length=len(raw)):
                self.path.write_bytes(raw)
                with self.assertRaises(TargetJournalError):
                    self.open()
                self.assertEqual(self.path.read_bytes(), raw)

    def test_valid_hash_does_not_allow_illegal_transition(self):
        self.create()
        with self.open() as journal:
            journal.deny(identity())
            journal.advance(identity().operation_id, identity().target_boot_id, 0, 1)
        lines = self.path.read_bytes().splitlines()
        record = json.loads(lines[-1])
        record["payload"]["phase"] = 3
        del record["digest"]
        canonical = lambda doc: json.dumps(doc, sort_keys=True, separators=(",", ":")).encode() + b"\n"
        record["digest"] = hashlib.sha256(canonical(record)).hexdigest()
        raw = b"\n".join(lines[:-1]) + b"\n" + canonical(record)
        self.path.write_bytes(raw)
        with self.assertRaises(TargetJournalError):
            self.open()
        self.assertEqual(self.path.read_bytes(), raw)

    def test_fsync_failure_poisoned_owner_never_publishes_state(self):
        self.create()
        with self.open() as journal:
            with patch("target_journal.os.fsync", side_effect=OSError("injected fsync failure")):
                with self.assertRaises(TargetJournalError):
                    journal.deny(identity())
            with self.assertRaises(TargetJournalError):
                journal.denied()
            with self.assertRaises(TargetJournalError):
                journal.deny(identity())
        # A complete but unacknowledged DENY may survive. It still cannot be proof.
        with self.open() as journal:
            self.assertFalse(journal.completion_recorded(identity().operation_id, identity().target_boot_id))

    def test_state_is_not_published_before_real_fsync(self):
        self.create()
        actual_fsync = os.fsync
        with self.open() as journal:
            def observe_then_sync(fd):
                self.assertEqual(journal.denied(), ())
                self.assertIn(b'"kind":"DENY"', self.path.read_bytes())
                actual_fsync(fd)
            with patch("target_journal.os.fsync", side_effect=observe_then_sync):
                journal.deny(identity())
            self.assertEqual(len(journal.denied()), 1)

    def test_partial_write_leaves_torn_journal_and_poisoned_owner(self):
        self.create()
        actual_write = os.write
        calls = []
        with self.open() as journal:
            def partial_then_fail(fd, raw):
                calls.append(len(raw))
                if len(calls) == 1:
                    return actual_write(fd, raw[:17])
                raise OSError("injected full device")
            with patch("target_journal.os.write", side_effect=partial_then_fail):
                with self.assertRaises(TargetJournalError):
                    journal.deny(identity())
            with self.assertRaises(TargetJournalError):
                journal.completion_recorded(identity().operation_id, identity().target_boot_id)
        raw = self.path.read_bytes()
        with self.assertRaises(TargetJournalError):
            self.open()
        self.assertEqual(self.path.read_bytes(), raw)

    def test_abrupt_process_exit_preserves_begin_without_completion(self):
        self.create()
        script = ("import os,sys; sys.path.insert(0, sys.argv[1]); "
                  "from target_journal import TargetJournal,DrainIdentity; "
                  "j=TargetJournal(sys.argv[2],sys.argv[3],owner_uid=int(sys.argv[4])); "
                  "i=DrainIdentity('01'*16,1,'02'*16,'03'*16,'04'*16,5,'06'*32,('07'*32,'08'*32)); "
                  "j.deny(i); j.advance(i.operation_id,i.target_boot_id,0,1); os._exit(29)")
        result = subprocess.run([sys.executable, "-c", script,
                                 str(Path(__file__).resolve().parents[1]), str(self.directory),
                                 self.inventory, str(os.geteuid())], capture_output=True, timeout=10)
        self.assertEqual(result.returncode, 29, result.stderr)
        with self.open() as journal:
            self.assertEqual(journal.denied()[0].phases, (1, 0))
            self.assertFalse(journal.completion_recorded(identity().operation_id, identity().target_boot_id))


if __name__ == "__main__":
    unittest.main()
