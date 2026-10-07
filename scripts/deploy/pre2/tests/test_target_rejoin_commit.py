"""Durable rejoin retirement and real startup consumers, not DB admission.

Author: SqlRush <sqlrush@gmail.com>
Temporary real journals/files only; configfs/boot are native-boundary fixtures.
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
import test_target_rejoin_journal as rejoin_tests
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import target_journal as api
from target_journal import TargetJournalError, RejoinIntent, _canonical


class CommitTests(unittest.TestCase):
    open = journal_tests.TargetJournalTests.open
    create = journal_tests.TargetJournalTests.create
    drain = rejoin_tests.RejoinJournalTests.drain
    prepared = rejoin_tests.RejoinJournalTests.prepared

    def setUp(self):
        journal_tests.TargetJournalTests.setUp(self)
        self.create()
        self.identity = journal_tests.identity()
        self.intent = RejoinIntent(123, 2, 7, 8, "ab" * 32)

    def completion(self, **changes):
        cls = getattr(api, "RejoinCommit", None)
        self.assertTrue(callable(cls), "durable rejoin completion type is missing")
        return replace(cls(4, 15, 23, "cd" * 32), **changes)

    def commit(self, journal, identity, *, intent=None, completion=None):
        action = getattr(journal, "commit_rejoin", None)
        self.assertTrue(callable(action), "successful rejoin retirement is missing")
        action(identity, self.intent if intent is None else intent,
               self.completion() if completion is None else completion)

    def ready(self, journal, *, refresh=True):
        self.prepared(journal)
        authorized = replace(self.identity, attempt=2)
        journal.authorize_rejoin(authorized, self.intent)
        for phase in (2, 3):
            journal.advance_rejoin(authorized, self.intent, phase)
        if not refresh:
            return authorized
        refreshed = replace(self.identity, attempt=3)
        journal.refresh_rejoin(refreshed, self.intent)
        return refreshed

    def test_committed_rejoin_survives_replay_without_active_deny(self):
        with self.open() as journal:
            identity = self.ready(journal)
            self.commit(journal, identity)
            self.assertEqual(journal.denied(), ())
            self.assertFalse(journal.completion_recorded(identity.operation_id, identity.target_boot_id))
            self.assertEqual(len(journal.states), 1, "audit lineage must not be erased")
        with self.open() as journal:
            self.assertEqual(journal.denied(), ())
            self.assertEqual(journal.states[identity.operation_id].completion, self.completion())
            self.assertEqual(journal.states[identity.operation_id].rejoin.intent, self.intent)

    def test_exact_lost_ack_repeat_is_read_only_but_changed_decision_refuses(self):
        with self.open() as journal:
            identity = self.ready(journal)
            self.commit(journal, identity)
        before = self.path.read_bytes()
        with self.open() as journal:
            self.commit(journal, identity)
            for change in ({"database_incarnation": 5}, {"formation_epoch": 16},
                           {"root_generation": 24}, {"decision_digest": "ef" * 32}):
                with self.subTest(change=change), self.assertRaises(TargetJournalError):
                    self.commit(journal, identity, completion=self.completion(**change))
            self.assertEqual(self.path.read_bytes(), before)

    def test_access_ready_without_refresh_cannot_retire(self):
        with self.open() as journal:
            identity = self.ready(journal, refresh=False)
            before = self.path.read_bytes()
            with self.assertRaises(TargetJournalError):
                self.commit(journal, identity)
            self.assertEqual(self.path.read_bytes(), before)
            self.assertEqual(len(journal.denied()), 1)

    def test_incomplete_and_revoked_rejoin_cannot_retire(self):
        with self.open() as journal:
            self.prepared(journal)
            for phase in (0, 1, 2, 4):
                if phase == 1:
                    journal.arm_rejoin(self.identity, self.intent)
                elif phase in (2, 4):
                    journal.advance_rejoin(self.identity, self.intent, phase)
                before = self.path.read_bytes()
                with self.subTest(phase=phase), self.assertRaises(TargetJournalError):
                    self.commit(journal, self.identity)
                self.assertEqual(self.path.read_bytes(), before)

    def test_wrong_identity_or_intent_cannot_clear_current_obligation(self):
        with self.open() as journal:
            identity = self.ready(journal)
            before = self.path.read_bytes()
            for change in ({"operation_id": "ef" * 16}, {"attempt": 4}, {"daemon_boot_id": "ee" * 16},
                           {"target_boot_id": "dd" * 16}, {"guest_uuid": "cc" * 16},
                           {"mapping_generation": 6}, {"protected_set_digest": "dd" * 32},
                           {"route_digests": identity.route_digests[::-1]}):
                with self.subTest(change=change), self.assertRaises(TargetJournalError):
                    self.commit(journal, replace(identity, **change))
            for change in ({"system_identifier": 124}, {"old_node_id": 3}, {"old_incarnation": 6},
                           {"candidate_incarnation": 9}, {"rejoin_gate_digest": "ef" * 32}):
                with self.subTest(change=change), self.assertRaises(TargetJournalError):
                    self.commit(journal, identity, intent=replace(self.intent, **change))
            self.assertEqual(self.path.read_bytes(), before)

    def test_malformed_completion_is_not_published(self):
        with self.open() as journal:
            identity = self.ready(journal)
            before = self.path.read_bytes()
            for name in ("database_incarnation", "formation_epoch", "root_generation"):
                for value in (0, -1, True, 1.0, "1", 1 << 64):
                    with self.subTest(name=name, value=value), self.assertRaises(TargetJournalError):
                        self.commit(journal, identity, completion=self.completion(**{name: value}))
            for value in ("00" * 32, "CD" * 32, "cd" * 31, "cd" * 33, 1, b"cd" * 32):
                with self.subTest(value=value), self.assertRaises(TargetJournalError):
                    self.commit(journal, identity, completion=self.completion(decision_digest=value))
            with self.assertRaises(TargetJournalError):
                self.commit(journal, identity, completion={"root_generation": 23})
            self.assertEqual(self.path.read_bytes(), before)

    def test_retired_owner_cannot_be_mutated_or_rebased_to_new_boot(self):
        with self.open() as journal:
            identity = self.ready(journal)
            self.commit(journal, identity)
            newer = replace(identity, attempt=4)
            actions = (lambda: journal.deny(identity), lambda: journal.deny(newer),
                       lambda: journal.advance(identity.operation_id, identity.target_boot_id, 0, 1),
                       lambda: journal.arm_rejoin(identity, self.intent),
                       lambda: journal.advance_rejoin(identity, self.intent, 4),
                       lambda: journal.authorize_rejoin(newer, self.intent),
                       lambda: journal.refresh_rejoin(newer, self.intent),
                       lambda: journal.revoke_rejoin(newer, self.intent),
                       lambda: journal.reconcile_boot(replace(newer, target_boot_id="dd" * 16), intent=self.intent),
                       lambda: journal.handoff(identity, replace(newer, operation_id="ef" * 16)))
            before = self.path.read_bytes()
            for n, action in enumerate(actions):
                with self.subTest(action=n), self.assertRaises(TargetJournalError):
                    action()
            self.assertEqual(self.path.read_bytes(), before)
            self.assertEqual(journal.denied(), ())

    def test_new_fence_wins_before_delayed_commit(self):
        with self.open() as journal:
            identity = self.ready(journal)
            newer = replace(identity, operation_id="ef" * 16, attempt=1)
            journal.supersede_rejoin(newer, self.intent.candidate_incarnation)
            before = self.path.read_bytes()
            with self.assertRaises(TargetJournalError):
                self.commit(journal, identity)
            self.assertEqual(journal.denied()[0].identity, newer)
            self.assertEqual(self.path.read_bytes(), before)

    def test_commit_then_new_fence_survives_old_commit_replay(self):
        with self.open() as journal:
            identity = self.ready(journal)
            self.commit(journal, identity)
            newer = replace(identity, operation_id="ef" * 16, attempt=1, target_boot_id="dd" * 16)
            journal.deny(newer)
            before = self.path.read_bytes()
            with self.assertRaises(TargetJournalError):
                self.commit(journal, identity)
            self.assertEqual(journal.denied()[0].identity, newer)
            self.assertEqual(self.path.read_bytes(), before)
        with self.open() as journal:
            self.assertEqual(journal.denied()[0].identity, newer)
            self.assertEqual(journal.states[identity.operation_id].completion, self.completion())

    def test_retirement_preserves_candidate_incarnation_high_water(self):
        with self.open() as journal:
            identity = self.ready(journal)
            self.commit(journal, identity)
            newer = replace(identity, operation_id="ef" * 16, attempt=1)
            journal.deny(newer)
            for ordinal in range(2):
                for phase in (1, 2, 3):
                    journal.advance(newer.operation_id, newer.target_boot_id, ordinal, phase)
            with self.assertRaises(TargetJournalError):
                journal.arm_rejoin(newer, self.intent)
            next_intent = replace(self.intent, old_incarnation=8, candidate_incarnation=9,
                                  rejoin_gate_digest="de" * 32)
            journal.arm_rejoin(newer, next_intent)
            self.assertEqual(journal.denied()[0].rejoin.intent.candidate_incarnation, 9)

    def test_fsync_failure_never_publishes_or_acknowledges_retirement(self):
        with self.open() as journal:
            identity = self.ready(journal)
            with patch("target_journal.os.fsync", side_effect=OSError("private path")):
                with self.assertRaises(TargetJournalError):
                    self.commit(journal, identity)
            self.assertIsNone(journal.states[identity.operation_id].completion)
            self.assertTrue(journal.poisoned)
            with self.assertRaises(TargetJournalError):
                journal.denied()
        # A complete write whose fsync failed is uncertain, not safe to erase.
        # Recovery must sync the visible record and its directory before it can
        # expose retirement or acknowledge the exact repeated decision.
        before = self.path.read_bytes()
        with patch("target_journal.os.fsync", wraps=os.fsync) as sync:
            with self.open() as journal:
                self.assertEqual([call.args[0] for call in sync.call_args_list],
                                 [journal.fd, journal.directory_fd])
                self.assertEqual(journal.denied(), ())
                self.commit(journal, identity)
        self.assertEqual(self.path.read_bytes(), before)

    def test_replay_sync_failure_cannot_publish_a_retired_obligation(self):
        with self.open() as journal:
            identity = self.ready(journal)
            self.commit(journal, identity)
        before = self.path.read_bytes()
        actual = os.fsync
        for fail_at in (1, 2):
            calls = []
            def failing_sync(fd):
                calls.append(fd)
                if len(calls) == fail_at:
                    raise OSError("uncertain replay durability")
                actual(fd)
            with self.subTest(fail_at=fail_at):
                with patch("target_journal.os.fsync", side_effect=failing_sync):
                    with self.assertRaisesRegex(TargetJournalError, "TARGET_JOURNAL_IO"):
                        with self.open() as journal:
                            self.commit(journal, identity)
                self.assertEqual(self.path.read_bytes(), before)
        # A later successful recovery may acknowledge, but never erase the
        # uncertain terminal or fabricate a new membership decision.
        with self.open() as journal:
            self.commit(journal, identity)
            self.assertEqual(journal.denied(), ())

    def test_short_write_bad_tail_does_not_become_empty_deny(self):
        with self.open() as journal:
            identity = self.ready(journal)
            actual, count = os.write, [0]
            def partial(fd, raw):
                count[0] += 1
                if count[0] == 1:
                    return actual(fd, raw[:19])
                raise OSError("interrupted append")
            with patch("target_journal.os.write", side_effect=partial):
                with self.assertRaises(TargetJournalError):
                    self.commit(journal, identity)
            self.assertTrue(journal.poisoned)
        with self.assertRaises(TargetJournalError):
            self.open()

    def test_complete_append_survives_immediate_process_exit(self):
        with self.open() as journal:
            self.ready(journal)
        code = "\n".join([
            "import os,sys", "sys.path.insert(0,sys.argv[1])",
            "from target_journal import TargetJournal,RejoinCommit",
            "j=TargetJournal(sys.argv[2], '09'*32, owner_uid=os.geteuid())",
            "s=j.denied()[0]", "j.commit_rejoin(s.identity,s.rejoin.intent,RejoinCommit(4,15,23,'cd'*32))",
            "os._exit(0)"])
        self.assertTrue(callable(getattr(api.TargetJournal, "commit_rejoin", None)), "retirement is missing")
        result = subprocess.run([sys.executable, "-c", code, str(Path(api.__file__).parent), str(self.directory)],
                                capture_output=True, timeout=5)
        self.assertEqual(result.returncode, 0, result.stderr.decode())
        with self.open() as journal:
            self.assertEqual(journal.denied(), ())

    def test_replay_checks_completion_schema_not_only_record_checksum(self):
        with self.open() as journal:
            identity = self.ready(journal)
            self.commit(journal, identity)
        original = self.path.read_bytes()
        lines = original.splitlines(keepends=True)
        for change in ({"root_generation": 0}, {"decision_digest": "00" * 32}, {"extra": 1}):
            record = json.loads(lines[-1])
            record["payload"]["completion"].update(change)
            del record["digest"]
            record["digest"] = hashlib.sha256(_canonical(record)).hexdigest()
            self.path.write_bytes(b"".join(lines[:-1]) + _canonical(record))
            with self.subTest(change=change), self.assertRaises(TargetJournalError):
                self.open()
        self.path.write_bytes(original)
        with self.open() as journal:
            self.assertEqual(journal.denied(), ())

    def test_maximum_routes_and_uint64_completion_preserve_exact_identity(self):
        self.identity = replace(self.identity, route_digests=tuple(f"{n + 1:064x}" for n in range(128)))
        with self.open() as journal:
            identity = self.ready(journal)
            largest = self.completion(database_incarnation=(1 << 64) - 1,
                                      formation_epoch=(1 << 64) - 1, root_generation=(1 << 64) - 1)
            self.commit(journal, identity, completion=largest)
            self.assertEqual(journal.denied(), ())
        with self.open() as journal:
            self.assertEqual(journal.states[identity.operation_id].completion, largest)


class CommitStartupTests(unittest.TestCase):
    def setUp(self):
        from test_target_admission import AdmissionTests
        self.f = AdmissionTests()
        self.addCleanup(self.f.doCleanups)
        self.f.setUp()
        self.intent = RejoinIntent(123, 2, 7, 8, "ab" * 32)

    def ready(self):
        journal, old = self.f.journal, self.f.identity
        journal.deny(old)
        for ordinal in range(4):
            for phase in (1, 2, 3):
                journal.advance(old.operation_id, old.target_boot_id, ordinal, phase)
        authorized = replace(old, attempt=2)
        journal.authorize_rejoin(authorized, self.intent)
        for phase in (2, 3):
            journal.advance_rejoin(authorized, self.intent, phase)
        identity = replace(old, attempt=3)
        journal.refresh_rejoin(identity, self.intent)
        return identity

    def restart(self):
        self.f.journal.close()
        self.f.journal = api.TargetJournal(self.f.directory, self.f.registry.inventory_digest,
                                          owner_uid=os.geteuid())
        self.addCleanup(self.f.journal.close)
        self.f.root.targets, self.f.root.storage_objects = [], []
        self.f.staged = self.f.stage_ok()

    def commit(self, identity):
        self.assertTrue(callable(getattr(api, "RejoinCommit", None)), "retirement type is missing")
        self.f.journal.commit_rejoin(identity, self.intent, api.RejoinCommit(4, 15, 23, "cd" * 32))

    def test_new_boot_opens_exact_completed_guest_without_old_drain_or_off(self):
        identity = self.ready()
        self.commit(identity)
        self.restart()
        self.assertNotEqual(identity.target_boot_id, self.f.staged.target_boot_id)
        before = (self.f.directory / "deny.journal").read_bytes()
        self.commit(identity)  # Lost-ACK replay keeps the original REFRESH tuple.
        result = self.f.open_ok()
        self.assertEqual(result.excluded_guests, ())
        self.assertEqual(self.f.tpg.node_acls, [self.f.acl])
        self.assertTrue(self.f.tpg.enable)
        self.assertEqual(self.f.journal.denied(), ())
        self.assertEqual((self.f.directory / "deny.journal").read_bytes(), before)
        self.assertFalse(self.f.journal.completion_recorded(identity.operation_id, identity.target_boot_id))

    def test_ready_without_committed_decision_still_refuses_after_restart(self):
        self.ready()
        self.restart()
        self.f.assert_closed_refusal()
        self.assertEqual(len(self.f.journal.denied()), 1)

    def test_retired_history_never_hides_new_fence_after_restart(self):
        identity = self.ready()
        self.commit(identity)
        newer = replace(identity, operation_id="ef" * 16, attempt=1, target_boot_id="dd" * 16)
        self.f.journal.deny(newer)
        self.restart()
        self.f.assert_closed_refusal()
        with self.assertRaises(TargetJournalError):
            self.commit(identity)
        self.assertEqual(self.f.journal.denied()[0].identity, newer)

    def test_terminal_does_not_excuse_missing_survivor_acl_or_automatic_access(self):
        self.commit(self.ready())
        self.restart()
        self.f.tpg.node_acls = []
        self.f.assert_closed_refusal()
        self.f.tpg.node_acls = [self.f.acl]
        self.f.attributes["generate_node_acls"] = "1"
        self.f.assert_closed_refusal()
        self.assertFalse(self.f.tpg.enable)

    def test_authenticated_observation_cannot_request_commit(self):
        from target_command import decode_command
        self.ready()
        before = (self.f.directory / "deny.journal").read_bytes()
        raw = _canonical({"version": 1, "action": "rejoin_commit", "committed": True,
                          "decision_digest": "cd" * 32})
        with self.assertRaises(TargetJournalError):
            decode_command(raw)
        self.assertEqual((self.f.directory / "deny.journal").read_bytes(), before)
        self.assertEqual(len(self.f.journal.denied()), 1)


if __name__ == "__main__":
    unittest.main()
