"""Compose the real resolver, OFF observer and durable executor.

Author: SqlRush <sqlrush@gmail.com>
Only native LIO/libvirt/syscall boundaries are injected; no isolation claim.
"""

from dataclasses import replace
import os
from pathlib import Path
import sys
import time
import unittest
from unittest.mock import patch
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from target_journal import TargetJournalError
from target_mapping import ProtectedMap, ProtectedRoute
from target_operation import complete_off_drain
from target_registry import NodeRegistry, TargetRegistry
from test_target_guest import Connection
import test_target_inventory as inventory_tests


class OperationTests(unittest.TestCase):
    setUp_inventory = inventory_tests.TargetInventoryTests.setUp
    read_config = inventory_tests.TargetInventoryTests.read_config

    def setUp(self):
        self.setUp_inventory()
        self.connection = Connection()
        self.connection.domain.uuid = str(uuid.UUID(hex=self.identity.guest_uuid))
        routes = tuple(ProtectedRoute(b.ordinal, b.digest, 1, b.initiator, "cred-1", b.target,
                                      f"{b.portal}:{b.port}", b.tpg, "wwid-1", b.serial,
                                      "ab" * 16, 1, "bc" * 16, 1) for b in self.bindings)
        self.mapping = ProtectedMap(123, 2, self.identity.mapping_generation,
                                    self.identity.protected_set_digest, "01" * 16, "02" * 16,
                                    "03" * 16, "pre2-kvm-gfs2-v1", "04" * 16,
                                    self.identity.guest_uuid, routes)
        self.registry = TargetRegistry(self.journal.inventory, (NodeRegistry(self.mapping, self.bindings),))
        self.events = []
        self.boot = patch("target_operation._kernel_boot_id", return_value=self.identity.target_boot_id)
        self.boot.start()
        self.addCleanup(self.boot.stop)
        for lun in self.acl.mapped_luns:
            lun.delete = lambda lun=lun: self.acl.mapped_luns.remove(lun)
        self.rmdir = patch("target_drain.os.rmdir", side_effect=self.remove_acl)
        self.rmdir.start()
        self.addCleanup(self.rmdir.stop)

    def remove_acl(self, path):
        self.assertEqual(path, self.acl.path)
        self.assertEqual(len(self.journal.denied()), 1)
        self.assertEqual(self.journal.denied()[0].identity, self.identity)
        self.assertEqual(self.journal.denied()[0].phases, (1, 1, 1, 1))
        self.assertEqual(self.connection.domain.active, 0)
        self.events.append("teardown")
        self.tpg.node_acls.remove(self.acl)

    def complete(self):
        i = self.identity
        return complete_off_drain(self.registry, self.journal, self.root, self.connection, 2,
                                  i.operation_id, i.attempt, i.daemon_boot_id, i.target_boot_id,
                                  "dd" * 16, time.monotonic_ns() + 5_000_000_000)

    def complete_ok(self):
        try:
            return self.complete()
        except TargetJournalError as error:
            self.fail(f"complete real composition refused: {error}")

    def test_complete_and_exact_replay_retain_all_physical_obligations(self):
        before = time.monotonic_ns()
        result = self.complete_ok()
        self.assertEqual(result.identity, self.identity)
        self.assertEqual(result.challenge, "dd" * 16)
        self.assertEqual(result.phases, (3, 3, 3, 3))
        self.assertGreaterEqual(result.guest.observed_mono_ns, before)
        self.assertEqual(self.events, ["teardown"])
        self.assertEqual(self.complete_ok().identity, result.identity)
        self.assertEqual(self.events, ["teardown"])
        self.assertTrue(self.journal.completion_recorded(self.identity.operation_id, self.identity.target_boot_id))
        self.assertFalse(hasattr(result, "unsolicited_on_blocked"))

    def test_on_guest_is_not_drained_and_no_returned_completion(self):
        self.connection.domain.active = 1
        with self.assertRaises(TargetJournalError):
            self.complete()
        self.assertEqual(self.events, [])
        self.assertEqual(len(self.tpg.node_acls), 1)
        self.assertEqual(len(self.journal.denied()), 1)
        self.assertEqual(self.journal.denied()[0].phases, (0, 0, 0, 0))

    def test_off_readback_after_flush_is_required(self):
        real_sync = os.fsync
        def sync(fd):
            real_sync(fd)
            if fd != self.journal.fd:
                self.connection.domain.active = 1
        with patch("target_drain.os.fsync", side_effect=sync), self.assertRaises(TargetJournalError):
            self.complete()
        self.assertEqual(self.events, ["teardown"])
        self.assertEqual(self.journal.denied()[0].phases, (3, 3, 3, 3))

    def test_new_route_after_teardown_is_not_covered_by_old_census(self):
        real_remove = self.remove_acl
        def remove(path):
            real_remove(path)
            self.attributes["generate_node_acls"] = "1"
        with patch("target_drain.os.rmdir", side_effect=remove), self.assertRaises(TargetJournalError):
            self.complete()
        self.assertEqual(self.events, ["teardown"])

    def test_failed_flush_keeps_durable_unfinished_deny(self):
        real_sync = os.fsync
        def sync(fd):
            if fd != self.journal.fd:
                raise OSError("private backend path")
            return real_sync(fd)
        with patch("target_drain.os.fsync", side_effect=sync), self.assertRaises(TargetJournalError):
            self.complete()
        self.assertEqual(len(self.journal.denied()), 1)
        self.assertEqual(self.journal.denied()[0].phases, (2, 2, 2, 2))
        self.assertEqual(self.tpg.node_acls, [])

    def test_target_boot_drift_keeps_completion_unusable(self):
        with patch("target_operation._kernel_boot_id", side_effect=[self.identity.target_boot_id, "ef" * 16]), \
                self.assertRaises(TargetJournalError):
            self.complete()
        self.assertEqual(self.events, ["teardown"])

    def test_wrong_inventory_or_guest_relabelling_prevents_any_teardown(self):
        self.registry = replace(self.registry, inventory_digest="ff" * 32)
        with self.assertRaises(TargetJournalError):
            self.complete()
        self.assertEqual(self.events, [])
        self.assertEqual(self.journal.denied(), ())

    def test_resolve_failure_does_not_create_deny_or_leak_pins(self):
        node = self.registry.node(2)
        broken = tuple(replace(b, inode=b.inode + 1) if b.mapped_lun == 1 else b for b in node.bindings)
        self.registry = replace(self.registry, nodes=(replace(node, bindings=broken),))
        with self.assertRaises(TargetJournalError):
            self.complete()
        self.assertEqual(self.events, [])
        self.assertEqual(self.journal.denied(), ())


if __name__ == "__main__":
    unittest.main()
