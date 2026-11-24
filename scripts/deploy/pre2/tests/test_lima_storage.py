"""Compose LIO lookup with the real resolver, durable journal and drain.

Author: SqlRush <sqlrush@gmail.com>
Only native boundaries are injected. No power or old-writer I/O proof is made.
"""

from dataclasses import FrozenInstanceError, replace
import importlib.util
import os
from pathlib import Path
import sys
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import target_native
from target_journal import TargetJournalError
import test_target_inventory as inventory_tests

if importlib.util.find_spec("lima_storage") is not None:
    import lima_storage
else:
    lima_storage = None


class LimaStorageTests(unittest.TestCase):
    setUp_inventory = inventory_tests.TargetInventoryTests.setUp
    read_config = inventory_tests.TargetInventoryTests.read_config

    def setUp(self):
        self.setUp_inventory()
        self.events = []
        self.deadline = time.monotonic_ns() + 5_000_000_000
        self.inventory = self.journal.inventory
        patches = (
            patch.object(target_native.sys, "platform", "linux"),
            patch.object(target_native.os, "geteuid", return_value=0),
            patch.object(target_native, "_configfs_identity", return_value=(4, 12)),
            patch.object(target_native, "_lookup_root", return_value=self.root),
            patch("target_operation._kernel_boot_id", return_value=self.identity.target_boot_id),
            patch.dict(sys.modules, {"rtslib_fb": SimpleNamespace(__version__="2.1.74"),
                                     "libvirt": None}),
            patch("target_drain.os.rmdir", side_effect=self.remove_acl),
        )
        self.boundaries = [item.start() for item in patches]
        for item in patches:
            self.addCleanup(item.stop)
        for lun in self.acl.mapped_luns:
            lun.delete = lambda lun=lun: self.acl.mapped_luns.remove(lun)

    def remove_acl(self, path):
        self.assertEqual(path, self.acl.path)
        self.assertEqual(self.journal.denied()[0].identity, self.identity)
        self.assertEqual(self.journal.denied()[0].phases, (1, 1, 1, 1))
        self.assertEqual(self.acl.mapped_luns, [])
        self.events.append("teardown")
        self.tpg.node_acls.remove(self.acl)

    def deny(self):
        self.assertIsNotNone(lima_storage, "Lima storage-only drain is not implemented")
        return lima_storage.deny_storage(self.journal, self.inventory, self.identity,
                                         self.bindings, self.deadline)

    def test_exact_drain_and_replay_without_power_or_isolation_claim(self):
        result = self.deny()
        self.assertEqual(result.identity, self.identity)
        self.assertEqual(result.phases, (3, 3, 3, 3))
        self.assertEqual(self.tpg.node_acls, [])
        self.assertEqual(self.deny(), result)
        self.assertEqual(self.events, ["teardown"])
        self.assertEqual(set(vars(result)), {"identity", "phases"})
        with self.assertRaises(FrozenInstanceError):
            result.phases = ()

    def test_wrong_inventory_refuses_before_any_deny(self):
        self.inventory = "ff" * 32
        with self.assertRaises(TargetJournalError):
            self.deny()
        self.assertEqual(self.journal.denied(), ())
        self.assertEqual(self.events, [])

    def test_invalid_identity_refuses_before_any_deny(self):
        self.identity = replace(self.identity, attempt=True)
        with self.assertRaises(TargetJournalError):
            self.deny()
        self.assertEqual(self.journal.denied(), ())

    def test_expired_or_invalid_deadline_never_starts_drain(self):
        for deadline in (time.monotonic_ns() - 1, True, 0, 1 << 64):
            self.deadline = deadline
            with self.subTest(deadline=deadline), self.assertRaises(TargetJournalError):
                self.deny()
        self.assertEqual(self.events, [])
        self.assertEqual(self.journal.denied(), ())

    def test_missing_acl_without_journal_completion_is_not_success(self):
        self.tpg.node_acls = []
        with self.assertRaises(TargetJournalError):
            self.deny()
        self.assertEqual(self.journal.denied(), ())

    def test_wrong_file_identity_is_not_relabelled_or_drained(self):
        self.bindings = tuple(replace(b, inode=b.inode + 1) if b.mapped_lun == 1 else b
                              for b in self.bindings)
        with self.assertRaises(TargetJournalError):
            self.deny()
        self.assertEqual(self.events, [])
        self.assertEqual(self.journal.denied(), ())

    def test_failed_unmap_preserves_begin_debt(self):
        def fail():
            raise OSError("private backend path")
        self.acl.mapped_luns[0].delete = fail
        with self.assertRaises(TargetJournalError) as caught:
            self.deny()
        self.assertNotIn("private", str(caught.exception))
        self.assertEqual(self.journal.denied()[0].phases, (1, 1, 1, 1))
        self.assertEqual(self.events, [])

    def test_failed_flush_keeps_removed_acl_and_unfinished_debt(self):
        real_sync = os.fsync
        def sync(fd):
            if fd != self.journal.fd:
                raise OSError("private backend path")
            real_sync(fd)
        with patch("target_drain.os.fsync", side_effect=sync), self.assertRaises(TargetJournalError):
            self.deny()
        self.assertEqual(self.tpg.node_acls, [])
        self.assertEqual(self.journal.denied()[0].phases, (2, 2, 2, 2))

    def test_persistence_failure_prevents_first_mutation(self):
        with patch.object(self.journal, "deny", side_effect=TargetJournalError("TEST_DURABILITY")):
            with self.assertRaises(TargetJournalError):
                self.deny()
        self.assertEqual(self.events, [])
        self.assertEqual(len(self.acl.mapped_luns), 2)

    def test_post_drain_full_census_rejects_new_automatic_route(self):
        real_remove = self.remove_acl
        def remove(path):
            real_remove(path)
            self.attributes["generate_node_acls"] = "1"
        with patch("target_drain.os.rmdir", side_effect=remove), self.assertRaises(TargetJournalError):
            self.deny()
        self.assertEqual(self.journal.denied()[0].phases, (3, 3, 3, 3))

    def test_root_replacement_after_teardown_prevents_observation(self):
        real_remove = self.remove_acl
        def remove(path):
            real_remove(path)
            self.boundaries[2].return_value = (4, 13)
        with patch("target_drain.os.rmdir", side_effect=remove), self.assertRaises(TargetJournalError):
            self.deny()
        self.assertEqual(self.journal.denied()[0].phases, (3, 3, 3, 3))

    def test_target_boot_change_keeps_deny_without_reusable_observation(self):
        real_remove = self.remove_acl
        def remove(path):
            real_remove(path)
            self.boundaries[4].return_value = "ef" * 16
        with patch("target_drain.os.rmdir", side_effect=remove), self.assertRaises(TargetJournalError):
            self.deny()
        self.assertEqual(self.journal.denied()[0].phases, (3, 3, 3, 3))

    def test_new_boot_cannot_reuse_absent_acl_completion(self):
        self.deny()
        original = self.journal.denied()
        self.identity = replace(self.identity, attempt=2, target_boot_id="ef" * 16)
        self.boundaries[4].return_value = self.identity.target_boot_id
        with self.assertRaises(TargetJournalError):
            self.deny()
        self.assertEqual(self.journal.denied(), original)
        self.assertEqual(self.events, ["teardown"])

    def test_deadline_after_drain_is_not_a_successful_observation(self):
        self.assertIsNotNone(lima_storage, "Lima storage-only drain is not implemented")
        with patch.object(lima_storage.time, "monotonic_ns", side_effect=[self.deadline - 1,
                                                                         self.deadline + 1]):
            with self.assertRaises(TargetJournalError):
                self.deny()
        self.assertEqual(self.journal.denied()[0].phases, (3, 3, 3, 3))

    def test_deadline_during_native_handle_exit_is_not_success(self):
        self.assertIsNotNone(lima_storage, "Lima storage-only drain is not implemented")
        with patch.object(lima_storage.time, "monotonic_ns", side_effect=[self.deadline - 2,
                                                                         self.deadline - 1,
                                                                         self.deadline]):
            with self.assertRaises(TargetJournalError):
                self.deny()
        self.assertEqual(self.journal.denied()[0].phases, (3, 3, 3, 3))


if __name__ == "__main__":
    unittest.main()
