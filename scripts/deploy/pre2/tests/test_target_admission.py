"""Startup admits only registered non-denied guests after complete checks.

Author: SqlRush <sqlrush@gmail.com>
Real journal/backing files/inventory; only native configfs and boot are injected.
"""

from dataclasses import replace
from pathlib import Path
import sys
import time
from types import SimpleNamespace as Object
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from target_admission import open_staged_exports
from target_journal import TargetJournalError
from target_registry import NodeRegistry
import test_target_startup as startup_tests


class NativeTpg:
    def __init__(self, source, config):
        self.__dict__.update(source.__dict__)
        self.config, self.writes, self.after_write = config, [], None
        self.fail_enable = self.ignore_enable = self.fail_disable = False
        self.config[self.path + "/enable"] = "0\n"

    @property
    def enable(self):
        return self.config.get(self.path + "/enable", "1").strip() == "1"

    @enable.setter
    def enable(self, value):
        self.writes.append(value)
        if (value and self.fail_enable) or (not value and self.fail_disable):
            raise OSError("private native path")
        if value and self.ignore_enable:
            return
        self.config[self.path + "/enable"] = "1\n" if value else "0\n"
        if self.after_write:
            self.after_write(value)


class AdmissionTests(unittest.TestCase):
    setUp_startup = startup_tests.StartupTests.setUp
    setUp_inventory = startup_tests.StartupTests.setUp_inventory
    read_config = startup_tests.StartupTests.read_config
    restore = startup_tests.StartupTests.restore
    stage = startup_tests.StartupTests.stage
    stage_ok = startup_tests.StartupTests.stage_ok

    def setUp(self):
        self.setUp_startup()
        self.staged = self.stage_ok()
        self.tpg = NativeTpg(self.tpg, self.config)
        self.target.tpgs = [self.tpg]
        self.boot_read = patch("target_admission._kernel_boot_id", return_value="dd" * 16, create=True)
        self.boot_read.start()
        self.addCleanup(self.boot_read.stop)

    def open_exports(self, deadline=None):
        return open_staged_exports(self.registry, self.journal, self.root, self.staged,
                                   time.monotonic_ns() + 5_000_000_000 if deadline is None else deadline)

    def open_ok(self):
        try:
            return self.open_exports()
        except TargetJournalError as error:
            self.fail(f"complete startup refused: {error}")

    def deny_complete(self):
        denied = replace(self.identity, target_boot_id="dd" * 16)
        self.journal.deny(denied)
        for ordinal in range(4):
            for phase in range(1, 4):
                self.journal.advance(denied.operation_id, denied.target_boot_id, ordinal, phase)
        self.tpg.node_acls = []
        return denied

    def assert_closed_refusal(self):
        with self.assertRaises(TargetJournalError):
            self.open_exports()
        self.assertFalse(self.tpg.enable)

    def test_exact_non_denied_bootstrap_opens_after_full_census(self):
        before = time.monotonic_ns()
        result = self.open_ok()
        self.assertEqual(result.target_boot_id, "dd" * 16)
        self.assertEqual(result.excluded_guests, ())
        self.assertEqual(result.tpg_keys, ((self.target.wwn, 1),))
        self.assertGreaterEqual(result.observed_mono_ns, before)
        self.assertTrue(self.tpg.enable)
        self.assertEqual(self.tpg.writes, [True])
        self.assertEqual(self.journal.denied(), ())

    def test_completed_deny_remains_absent_and_durable_after_startup(self):
        denied = self.deny_complete()
        original = (self.directory / "deny.journal").read_bytes()
        result = self.open_ok()
        self.assertEqual(result.excluded_guests, (denied.guest_uuid,))
        self.assertEqual(self.tpg.node_acls, [])
        self.assertEqual(self.journal.denied()[0].identity, denied)
        self.assertEqual((self.directory / "deny.journal").read_bytes(), original)

    def test_survivor_acl_remains_available_while_denied_guest_is_excluded(self):
        node = self.registry.nodes[0]
        iqn = "iqn.2026-09.test:survivor"
        survivor = Object(node_wwn=iqn, parent_tpg=self.tpg, path=self.tpg.path + "/acls/" + iqn)
        survivor.mapped_luns = [Object(mapped_lun=old.mapped_lun, tpg_lun=old.tpg_lun,
                                      path=survivor.path + f"/lun_{old.mapped_lun}")
                               for old in self.acl.mapped_luns]
        bindings = tuple(replace(b, initiator=iqn, digest=f"{b.ordinal + 30:02x}" * 32)
                         for b in node.bindings)
        routes = tuple(replace(r, initiator=iqn, digest=f"{r.ordinal + 30:02x}" * 32)
                       for r in node.mapping.routes)
        mapping = replace(node.mapping, node_id=3, guest_uuid="ef" * 16, routes=routes)
        self.registry = replace(self.registry, nodes=(node, NodeRegistry(mapping, bindings)))
        denied = self.deny_complete()
        self.tpg.node_acls = [survivor]
        result = self.open_ok()
        self.assertEqual(result.excluded_guests, (denied.guest_uuid,))
        self.assertEqual([acl.node_wwn for acl in self.tpg.node_acls], [iqn])
        self.assertTrue(self.tpg.enable)

    def test_incomplete_or_old_boot_deny_never_enables(self):
        denied = replace(self.identity, target_boot_id="dd" * 16)
        self.journal.deny(denied)
        self.assert_closed_refusal()
        for ordinal in range(4):
            for phase in range(1, 4):
                self.journal.advance(denied.operation_id, denied.target_boot_id, ordinal, phase)
        self.tpg.node_acls = []
        self.journal.deny(replace(denied, attempt=2, target_boot_id="ee" * 16))
        self.assert_closed_refusal()
        self.assertEqual(self.tpg.writes, [])

    def test_recreated_denied_acl_and_unregistered_guest_refuse_before_enable(self):
        self.deny_complete()
        self.tpg.node_acls = [self.acl]
        self.assert_closed_refusal()
        self.tpg.node_acls = [Object(node_wwn="iqn.2026-09.test:unregistered")]
        self.assert_closed_refusal()
        self.assertEqual(self.tpg.writes, [])

    def test_stale_stage_and_extra_native_namespace_are_not_touched(self):
        for staged in (replace(self.staged, target_boot_id="ab" * 16),
                       replace(self.staged, inventory_digest="cd" * 32),
                       replace(self.staged, tpg_keys=())):
            saved, self.staged = self.staged, staged
            self.assert_closed_refusal()
            self.staged = saved
        self.root.targets.append(Object(wwn="iqn.2026-09.test:extra", tpgs=[]))
        self.assert_closed_refusal()
        self.assertEqual(self.tpg.writes, [])

    def test_ignored_enable_requires_native_readback(self):
        self.tpg.ignore_enable = True
        self.assert_closed_refusal()
        self.assertEqual(self.tpg.writes, [True, False])

    def test_failed_enable_never_returns_and_cleanup_is_verified(self):
        self.tpg.fail_enable = True
        self.assert_closed_refusal()
        self.assertEqual(self.tpg.writes, [True, False])

    def test_second_tpg_failure_recloses_already_enabled_first_tpg(self):
        from copy import deepcopy

        extra = NativeTpg(self.tpg, self.config)
        extra.tag, extra.path = 2, self.target.path + "/tpgt_2"
        self.config[extra.path + "/enable"] = "0\n"
        extra.writes = []
        extra.luns = [Object(**{**lun.__dict__, "path": extra.path + f"/lun/lun_{lun.lun}"})
                      for lun in self.tpg.luns]
        acl = Object(node_wwn=self.acl.node_wwn, parent_tpg=extra,
                     path=extra.path + "/acls/" + self.acl.node_wwn)
        acl.mapped_luns = [Object(mapped_lun=lun.lun, tpg_lun=lun,
                                 path=acl.path + f"/lun_{lun.lun}") for lun in extra.luns]
        extra.node_acls, extra.network_portals = [acl], []
        for old in self.tpg.network_portals:
            path = extra.path + f"/np/{old.ip_address}:{old.port}"
            extra.network_portals.append(Object(ip_address=old.ip_address, port=old.port,
                                                 parent_tpg=extra, path=path))
            self.config[path + "/iser"] = self.config[path + "/cxgbit"] = "0\n"
        node = self.registry.nodes[0]
        bindings = node.bindings + tuple(replace(b, tpg=2, ordinal=b.ordinal + 4,
                                                 digest=f"{b.ordinal + 30:02x}" * 32) for b in node.bindings)
        routes = node.mapping.routes + tuple(replace(r, tpg=2, ordinal=r.ordinal + 4,
                                                      digest=f"{r.ordinal + 30:02x}" * 32) for r in node.mapping.routes)
        self.registry = replace(self.registry, nodes=(replace(node, bindings=bindings,
                                                               mapping=replace(node.mapping, routes=routes)),))
        self.target.tpgs.append(extra)
        template = deepcopy(self.template["targets"][0]["tpgs"][0])
        template["tag"] = 2
        self.template["targets"][0]["tpgs"].append(template)
        self.root.targets, self.root.storage_objects = [], []
        self.staged = self.stage_ok()
        self.tpg.writes.clear()
        extra.fail_enable = True
        self.assert_closed_refusal()
        self.assertFalse(extra.enable)
        self.assertEqual(self.tpg.writes, [True, False])
        self.assertEqual(extra.writes, [True, False])

    def test_journal_change_after_enable_recloses_export(self):
        def change(enabled):
            if enabled:
                self.journal.deny(replace(self.identity, target_boot_id="dd" * 16))
        self.tpg.after_write = change
        self.assert_closed_refusal()
        self.assertEqual(self.tpg.writes, [True, False])
        self.assertEqual(len(self.journal.denied()), 1)

    def test_backing_replacement_after_enable_is_detected_and_reclosed(self):
        file = Path(self.stores[0].udev_path)
        def change(enabled):
            if enabled:
                file.rename(file.with_name("preserved"))
                file.write_bytes(b"y" * 4096)
        self.tpg.after_write = change
        self.assert_closed_refusal()
        self.assertEqual(file.read_bytes(), b"y" * 4096)

    def test_boot_change_after_enable_never_yields_startup_observation(self):
        with patch("target_admission._kernel_boot_id", side_effect=["dd" * 16, "dd" * 16, "ef" * 16]):
            self.assert_closed_refusal()
        self.assertEqual(self.tpg.writes, [True, False])

    def test_cancel_after_enable_recloses_before_propagating(self):
        def cancel(enabled):
            if enabled:
                raise KeyboardInterrupt()
        self.tpg.after_write = cancel
        with self.assertRaises(KeyboardInterrupt):
            self.open_exports()
        self.assertFalse(self.tpg.enable)
        self.assertEqual(self.tpg.writes, [True, False])

    def test_native_attribute_disappearing_is_not_successful_enable(self):
        def change(enabled):
            if enabled:
                del self.config[self.tpg.path + "/enable"]
        self.tpg.after_write = change
        self.assert_closed_refusal()
        self.assertEqual(self.tpg.writes, [True, False])

    def test_failed_disable_is_reported_not_hidden_as_closed(self):
        def change(enabled):
            if enabled:
                self.tpg.fail_disable = True
                self.attributes["generate_node_acls"] = "1"
        self.tpg.after_write = change
        with self.assertRaises(TargetJournalError) as caught:
            self.open_exports()
        self.assertEqual(str(caught.exception), "TARGET_ADMISSION_ROLLBACK_UNPROVEN")
        self.assertTrue(self.tpg.enable)
        self.assertEqual(self.tpg.writes, [True, False])

    def test_expired_envelope_and_missing_enable_file_do_not_mutate(self):
        with self.assertRaises(TargetJournalError):
            self.open_exports(time.monotonic_ns() - 1)
        del self.config[self.tpg.path + "/enable"]
        with self.assertRaises(TargetJournalError):
            self.open_exports()
        self.assertEqual(self.tpg.writes, [])


if __name__ == "__main__":
    unittest.main()
