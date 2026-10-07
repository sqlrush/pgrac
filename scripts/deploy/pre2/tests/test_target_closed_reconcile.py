"""Closed reboot/partial-drain composition using real journal and file pins.

Author: SqlRush <sqlrush@gmail.com>
Native libvirt/configfs boundaries are fixtures, not physical certification.
"""

from dataclasses import replace
import importlib
import os
import time
from types import SimpleNamespace as Object
import unittest
from unittest.mock import patch

import test_target_permissions as permission_tests
from target_journal import TargetJournal, TargetJournalError


class ClosedReconcileTests(unittest.TestCase):
    def setUp(self):
        try:
            self.api = importlib.import_module("target_closed")
        except ImportError:
            self.fail("closed reconciliation composition is missing")
        self.f = permission_tests.PermissionTests()
        self.f.arm_initial = False
        self.addCleanup(self.f.doCleanups)
        self.f.setUp()
        self.old = self.f.identity
        self.f.identity = replace(self.old, attempt=2, target_boot_id="dd" * 16)
        self.f.config[self.f.tpg.path + "/enable"] = "0\n"
        self.f.remove_acl = self.remove_acl
        self.f.patch_native()
        boot = patch("target_closed._kernel_boot_id", return_value=self.f.identity.target_boot_id)
        boot.start()
        self.addCleanup(boot.stop)

    def present(self):
        self.f.tpg.node_acls = [self.f.acl]
        self.f.acl.mapped_luns = [Object(mapped_lun=lun.lun, tpg_lun=lun,
            path=self.f.acl.path + f"/lun_{lun.lun}") for lun in self.f.tpg.luns]

    def remove_acl(self, name, fd):
        self.assertEqual(name, self.f.acl.node_wwn)
        self.assertEqual(self.f.journal.denied()[0].identity, self.f.identity)
        self.assertEqual(self.f.journal.denied()[0].phases, (1,) * 4)
        self.assertEqual(self.f.config[self.f.tpg.path + "/enable"].strip(), "0")
        self.assertEqual(self.f.connection.domain.active, 0)
        self.assertEqual(self.f.acl.mapped_luns, [])
        os.fstat(fd)
        self.f.events.append("rmdir-acl")
        if self.f.acl not in self.f.tpg.node_acls:
            raise FileNotFoundError
        self.f.tpg.node_acls.remove(self.f.acl)

    def prepare(self, *, identity=None, intent=None):
        return self.api.prepare_closed_deny(self.f.registry, self.f.journal, self.f.root,
            identity or self.f.identity, 2, self.f.template, time.monotonic_ns() + 5_000_000_000, intent=intent)

    def complete(self, *, intent=None, deadline=None):
        return self.api.complete_closed_drain(self.f.registry, self.f.journal, self.f.root, self.f.connection,
            self.f.identity, 2, self.f.template, time.monotonic_ns() + 5_000_000_000 if deadline is None else deadline,
            intent=intent)

    def test_full_closed_restart_gets_fresh_teardown_flush_not_old_completion(self):
        self.present()
        before = self.f.journal.sequence
        self.prepare()
        self.assertGreater(self.f.journal.sequence, before)
        self.assertEqual(self.f.journal.denied()[0].phases, (0,) * 4)
        self.assertEqual(self.complete(), (3,) * 4)
        self.assertEqual(self.f.events, ["unlink-lun", "unlink-lun", "rmdir-acl"])
        before = (self.f.journal.sequence, list(self.f.events))
        self.prepare()
        self.assertEqual(self.complete(), (3,) * 4)
        self.assertEqual((self.f.journal.sequence, self.f.events), before)
        self.assertEqual(self.f.config[self.f.tpg.path + "/enable"].strip(), "0")

    def test_absence_requires_actual_serialized_syscall_and_flush(self):
        self.prepare()
        flushes = []
        actual_sync = os.fsync
        def sync(fd):
            if fd != self.f.journal.fd:
                self.assertIn("rmdir-acl", self.f.events)
                flushes.append(os.fstat(fd).st_ino)
            actual_sync(fd)
        with patch("target_closed.os.fsync", side_effect=sync):
            self.assertEqual(self.complete(), (3,) * 4)
        self.assertEqual(self.f.events, ["rmdir-acl"])
        self.assertEqual(len(set(flushes)), 2)

    def test_partial_deletion_retry_never_recreates_access(self):
        self.present()
        self.prepare()
        removed = self.f.remove_lun
        def stop_after_one(path, expected):
            removed(path, expected)
            raise OSError("interrupted configfs")
        with patch("target_permissions._remove_mapped_lun", side_effect=stop_after_one):
            with self.assertRaises(TargetJournalError):
                self.complete()
        self.assertEqual(len(self.f.acl.mapped_luns), 1)
        self.assertEqual(self.f.journal.denied()[0].phases, (1,) * 4)
        self.assertEqual(self.complete(), (3,) * 4)
        self.assertEqual(self.f.events, ["unlink-lun", "unlink-lun", "rmdir-acl"])

    def test_active_rejoin_requires_explicit_exact_revoke_and_stays_retired(self):
        self.f.journal.arm_rejoin(self.old, self.f.intent)
        self.f.journal.advance_rejoin(self.old, self.f.intent, 2)
        self.f.journal.advance_rejoin(self.old, self.f.intent, 3)
        self.present()
        before = self.f.journal.sequence
        with self.assertRaises(TargetJournalError):
            self.prepare()
        with self.assertRaises(TargetJournalError):
            self.prepare(intent=replace(self.f.intent, candidate_incarnation=9))
        self.assertEqual(self.f.journal.sequence, before)
        self.prepare(intent=self.f.intent)
        self.assertEqual(self.f.journal.denied()[0].rejoin.phase, 4)
        self.assertEqual(self.complete(intent=self.f.intent), (3,) * 4)
        with self.assertRaises(TargetJournalError):
            self.f.journal.advance_rejoin(self.old, self.f.intent, 3)

    def test_live_guest_can_prepare_but_cannot_prove_drain(self):
        self.f.connection.domain.active = 1
        self.prepare()
        with self.assertRaises(TargetJournalError):
            self.complete()
        self.assertEqual(self.f.events, [])
        self.assertEqual(self.f.journal.denied()[0].phases, (0,) * 4)

    def test_enabled_missing_or_extra_native_export_refuses_before_journal_write(self):
        before = self.f.journal.sequence
        for value in ("1\n", "bad"):
            self.f.config[self.f.tpg.path + "/enable"] = value
            with self.assertRaises(TargetJournalError):
                self.prepare()
            self.assertEqual(self.f.journal.sequence, before)
        del self.f.config[self.f.tpg.path + "/enable"]
        with self.assertRaises(TargetJournalError):
            self.prepare()
        self.f.config[self.f.tpg.path + "/enable"] = "0"
        self.f.root.targets.append(Object(wwn="iqn.2026-09.test:unknown", tpgs=[]))
        with self.assertRaises(TargetJournalError):
            self.prepare()
        self.assertEqual(self.f.journal.sequence, before)

    def test_missing_nondenied_guest_routes_are_not_covered_by_closed_mode(self):
        fresh = self.f.directory / "fresh-journal"
        fresh.mkdir(mode=0o700)
        with TargetJournal(fresh, self.f.registry.inventory_digest, initialize=True, owner_uid=os.geteuid()) as journal:
            old, self.f.journal = self.f.journal, journal
            try:
                with self.assertRaises(TargetJournalError):
                    self.prepare()
                self.assertEqual(journal.denied(), ())
            finally:
                self.f.journal = old

    def test_old_boot_and_same_attempt_are_not_silently_rebased(self):
        before = self.f.journal.sequence
        for identity in (self.old, replace(self.f.identity, attempt=self.old.attempt)):
            with self.assertRaises(TargetJournalError):
                self.prepare(identity=identity)
            self.assertEqual(self.f.journal.sequence, before)

    def test_failed_flush_leaves_real_teardown_and_retry_completes(self):
        self.prepare()
        actual_sync = os.fsync
        def fail_data(fd):
            if fd != self.f.journal.fd:
                raise OSError("private path")
            actual_sync(fd)
        with patch("target_closed.os.fsync", side_effect=fail_data), self.assertRaises(TargetJournalError):
            self.complete()
        self.assertEqual(self.f.journal.denied()[0].phases, (2,) * 4)
        self.assertEqual(self.complete(), (3,) * 4)
        self.assertEqual(self.f.events, ["rmdir-acl"])

    def test_unprepared_expired_or_changed_boot_never_returns_completion(self):
        with self.assertRaises(TargetJournalError):
            self.complete()
        self.prepare()
        with self.assertRaises(TargetJournalError):
            self.complete(deadline=time.monotonic_ns() - 1)
        with patch("target_closed._kernel_boot_id", return_value="ff" * 16), self.assertRaises(TargetJournalError):
            self.complete()
        self.assertEqual(self.f.events, [])

    def test_fresh_closed_stage_drains_before_separate_open_and_keeps_guest_denied(self):
        from target_admission import open_staged_exports
        from target_startup import stage_closed_exports
        from test_target_admission import NativeTpg
        self.present()
        self.f.tpg = NativeTpg(self.f.tpg, self.f.config)
        self.f.target.tpgs = [self.f.tpg]
        self.f.root.targets, self.f.root.storage_objects = [], []
        deadline = time.monotonic_ns() + 5_000_000_000
        stage = stage_closed_exports(self.f.registry, self.f.journal, self.f.root, self.f.template, deadline)
        with patch("target_admission._kernel_boot_id", return_value=self.f.identity.target_boot_id):
            with self.assertRaises(TargetJournalError):
                open_staged_exports(self.f.registry, self.f.journal, self.f.root, stage, deadline)
            self.prepare()
            self.complete()
            result = open_staged_exports(self.f.registry, self.f.journal, self.f.root, stage, deadline)
        self.assertEqual(result.excluded_guests, (self.f.identity.guest_uuid,))
        self.assertEqual(self.f.tpg.node_acls, [])
        self.assertEqual(self.f.journal.denied()[0].identity, self.f.identity)
        self.assertTrue(self.f.tpg.enable)

    def test_export_opened_during_flush_is_not_a_completed_reconciliation(self):
        self.prepare()
        actual_sync = os.fsync
        def open_during_flush(fd):
            actual_sync(fd)
            if fd != self.f.journal.fd:
                self.f.config[self.f.tpg.path + "/enable"] = "1"
        with patch("target_closed.os.fsync", side_effect=open_during_flush), self.assertRaises(TargetJournalError):
            self.complete()
        self.assertEqual(self.f.journal.denied()[0].phases, (2,) * 4)
        self.assertFalse(self.f.journal.completion_recorded(self.f.identity.operation_id, self.f.identity.target_boot_id))


if __name__ == "__main__":
    unittest.main()
