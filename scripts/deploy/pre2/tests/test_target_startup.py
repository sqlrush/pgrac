"""Closed native startup ordering with real journal and backing-file validation.

Author: SqlRush <sqlrush@gmail.com>
Only native restore/configfs and boot ID are injected; not live certification.
"""

from copy import deepcopy
from dataclasses import replace
from pathlib import Path
import sys
import time
from types import SimpleNamespace as Object
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from target_journal import TargetJournalError
from target_mapping import ProtectedMap, ProtectedRoute
from target_registry import NodeRegistry, TargetRegistry
from target_startup import stage_closed_exports
import test_target_inventory as inventory_tests


class StartupTests(unittest.TestCase):
    setUp_inventory = inventory_tests.TargetInventoryTests.setUp
    read_config = inventory_tests.TargetInventoryTests.read_config

    def setUp(self):
        self.setUp_inventory()
        routes = tuple(ProtectedRoute(b.ordinal, b.digest, 1, b.initiator, "cred-1", b.target,
                                      f"{b.portal}:{b.port}", b.tpg, "wwid-1", b.serial,
                                      "ab" * 16, 1, "bc" * 16, 1) for b in self.bindings)
        mapping = ProtectedMap(123, 2, self.identity.mapping_generation,
                               self.identity.protected_set_digest, "01" * 16, "02" * 16,
                               "03" * 16, "pre2-kvm-gfs2-v1", "04" * 16, self.identity.guest_uuid, routes)
        self.registry = TargetRegistry(self.journal.inventory, (NodeRegistry(mapping, self.bindings),))
        self.template = {
            "storage_objects": [dict(plugin="fileio", name=f"disk{n}", index=n, dev=s.udev_path,
                                     size=4096, wwn=s.wwn, write_back=False, aio=False,
                                     attributes={"emulate_write_cache": 0}) for n, s in enumerate(self.stores)],
            "targets": [dict(wwn=self.target.wwn, fabric="iscsi", tpgs=[dict(
                tag=1, enable=True,
                attributes=dict(generate_node_acls=0, cache_dynamic_acls=0, demo_mode_write_protect=1,
                                authentication=0), parameters={"ErrorRecoveryLevel": "0"},
                luns=[dict(index=n, storage_object=f"/backstores/fileio/disk{n}") for n in range(2)],
                portals=[dict(ip_address=f"192.0.2.{n + 1}", port=3260, iser=False, offload=False)
                         for n in range(2)],
                node_acls=[dict(node_wwn=self.acl.node_wwn,
                                mapped_luns=[dict(index=n, tpg_lun=n, write_protect=False) for n in range(2)])])])]}
        self.calls = []
        self.tpg.enable = False
        self.root = Object(targets=[], storage_objects=[], restore=self.restore)
        self.boot = patch("target_startup._kernel_boot_id", return_value="dd" * 16, create=True)
        self.boot.start()
        self.addCleanup(self.boot.stop)

    def restore(self, config, *, clear_existing, abort_on_error):
        self.assertIs(clear_existing, False)
        self.assertIs(abort_on_error, True)
        self.assertIs(config["targets"][0]["tpgs"][0]["enable"], False)
        self.calls.append(deepcopy(config))
        self.tpg.enable = False
        self.root.targets, self.root.storage_objects = [self.target], self.stores
        return []

    def stage(self, deadline=None):
        return stage_closed_exports(self.registry, self.journal, self.root, self.template,
                                    time.monotonic_ns() + 5_000_000_000 if deadline is None else deadline)

    def stage_ok(self):
        try:
            return self.stage()
        except TargetJournalError as error:
            self.fail(f"complete closed stage refused: {error}")

    def test_cold_stage_preserves_deny_template_and_disables_every_export(self):
        self.journal.deny(self.identity)
        self.journal.advance(self.identity.operation_id, self.identity.target_boot_id, 0, 1)
        original, history = deepcopy(self.template), self.journal.denied()
        self.journal.close()
        from target_journal import TargetJournal
        import os
        self.journal = TargetJournal(self.directory, self.registry.inventory_digest, owner_uid=os.geteuid())
        self.addCleanup(self.journal.close)
        result = self.stage_ok()
        self.assertEqual(result.target_boot_id, "dd" * 16)
        self.assertEqual(result.tpg_keys, ((self.target.wwn, 1),))
        self.assertEqual(result.inventory_digest, self.registry.inventory_digest)
        self.assertEqual(len(self.calls), 1)
        self.assertIs(self.tpg.enable, False)
        self.assertEqual(self.template, original)
        self.assertEqual(self.journal.denied(), history)
        self.assertFalse(self.journal.completion_recorded(self.identity.operation_id, "dd" * 16))

    def test_existing_objects_or_stale_inventory_never_restore_or_clear(self):
        for name in ("targets", "storage_objects"):
            setattr(self.root, name, [Object()])
            with self.subTest(name=name), self.assertRaises(TargetJournalError):
                self.stage()
            setattr(self.root, name, [])
        self.registry = replace(self.registry, inventory_digest="ff" * 32)
        with self.assertRaises(TargetJournalError):
            self.stage()
        self.assertEqual(self.calls, [])

    def test_unknown_denied_guest_cannot_be_forgotten_on_restart(self):
        self.journal.deny(replace(self.identity, guest_uuid="ef" * 16))
        with self.assertRaises(TargetJournalError):
            self.stage()
        self.assertEqual(self.calls, [])
        self.assertEqual(len(self.journal.denied()), 1)

    def test_extra_route_or_automatic_access_refuses_before_native_mutation(self):
        original = deepcopy(self.template)
        def tpg():
            return self.template["targets"][0]["tpgs"][0]
        changes = [lambda: tpg()["portals"].append(dict(ip_address="192.0.2.99", port=3260, iser=False, offload=False)),
                   lambda: tpg()["node_acls"][0]["mapped_luns"].append(dict(index=2, tpg_lun=1, write_protect=False)),
                   lambda: tpg()["attributes"].update(generate_node_acls=1),
                   lambda: tpg()["attributes"].update(cache_dynamic_acls=1),
                   lambda: tpg()["parameters"].update(ErrorRecoveryLevel="1"),
                   lambda: self.template["targets"].append(deepcopy(self.template["targets"][0])),
                   lambda: self.template["storage_objects"][0].update(write_back=True),
                   lambda: self.template["storage_objects"][0].update(dev="/not/the/registered/file")]
        for change in changes:
            self.template = deepcopy(original)
            change()
            with self.assertRaises(TargetJournalError):
                self.stage()
        self.assertEqual(self.calls, [])

    def test_changed_backing_inode_is_not_recreated_or_resized(self):
        file = Path(self.stores[0].udev_path)
        file.rename(file.with_name("preserved-original"))
        file.write_bytes(b"y" * 4096)
        with self.assertRaises(TargetJournalError):
            self.stage()
        self.assertEqual(self.calls, [])
        self.assertEqual(file.read_bytes(), b"y" * 4096)

    def test_backing_name_cannot_inject_a_native_control_parameter(self):
        file = Path(self.stores[0].udev_path)
        changed = file.with_name("disk0,fd_buffered_io=1")
        file.rename(changed)
        node = self.registry.nodes[0]
        bindings = tuple(replace(b, file_path=str(changed)) if b.file_path == str(file) else b
                         for b in node.bindings)
        self.registry = replace(self.registry, nodes=(replace(node, bindings=bindings),))
        self.template["storage_objects"][0]["dev"] = str(changed)
        with self.assertRaises(TargetJournalError):
            self.stage()
        self.assertEqual(len(self.calls), 0, "native control string must never contain an injected parameter")

    def test_native_fileio_name_truncation_is_rejected_before_restore(self):
        file = Path(self.stores[0].udev_path)
        parent = self.directory / ("a" * 120)
        parent.mkdir()
        changed = parent / ("b" * 120)
        file.rename(changed)
        node = self.registry.nodes[0]
        bindings = tuple(replace(b, file_path=str(changed)) if b.file_path == str(file) else b
                         for b in node.bindings)
        self.registry = replace(self.registry, nodes=(replace(node, bindings=bindings),))
        self.template["storage_objects"][0]["dev"] = str(changed)
        with self.assertRaises(TargetJournalError):
            self.stage()
        self.assertEqual(len(self.calls), 0, "kernel path truncation must not select another file")

    def test_native_errors_or_enabled_export_never_return_staged(self):
        def failure(*args, **kwargs):
            self.restore(*args, **kwargs)
            raise OSError("private-chap-contents")
        self.root.restore = failure
        with self.assertRaises(TargetJournalError) as caught:
            self.stage()
        self.assertNotIn("private-chap", str(caught.exception))
        self.assertIs(self.tpg.enable, False)
        self.root.targets, self.root.storage_objects = [], []
        def enabled(*args, **kwargs):
            self.restore(*args, **kwargs)
            self.tpg.enable = True
            return []
        self.root.restore = enabled
        with self.assertRaises(TargetJournalError):
            self.stage()

    def test_partial_native_inventory_and_returned_errors_are_not_ready(self):
        def partial(*args, **kwargs):
            self.restore(*args, **kwargs)
            self.tpg.node_acls = []
            return []
        self.root.restore = partial
        with self.assertRaises(TargetJournalError):
            self.stage()
        self.root.targets, self.root.storage_objects = [], []
        self.root.restore = lambda *a, **k: ["private native diagnostic"]
        with self.assertRaises(TargetJournalError):
            self.stage()

    def test_expiry_or_journal_change_cannot_yield_staging_token(self):
        with self.assertRaises(TargetJournalError):
            self.stage(time.monotonic_ns() - 1)
        self.assertEqual(self.calls, [])
        def add_deny(*args, **kwargs):
            self.restore(*args, **kwargs)
            self.journal.deny(self.identity)
            return []
        self.root.restore = add_deny
        with self.assertRaises(TargetJournalError):
            self.stage()
        self.assertEqual(len(self.journal.denied()), 1)


if __name__ == "__main__":
    unittest.main()
