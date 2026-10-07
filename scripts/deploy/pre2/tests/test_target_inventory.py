"""Native inventory boundary tests with real pinned backing files.

Author: SqlRush <sqlrush@gmail.com>
The configfs/rtslib fixtures below are not physical isolation certificates.
"""

from dataclasses import replace
import itertools
import os
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace as Object
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import target_inventory
from target_inventory import RouteBinding, resolve_routes
from target_journal import DrainIdentity, TargetJournal, TargetJournalError


class TargetInventoryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="pgrac-inventory-")
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name).resolve()
        self.journal = TargetJournal(self.directory, "09" * 32, initialize=True,
                                     owner_uid=os.geteuid())
        self.addCleanup(self.journal.close)
        self.identity = DrainIdentity("01" * 16, 1, "02" * 16, "03" * 16,
                                      "04" * 16, 5, "06" * 32,
                                      tuple(f"{i:02x}" * 32 for i in range(7, 11)))
        self.target = Object(wwn="iqn.2026-09.test:target", fabric_module=Object(name="iscsi"))
        self.target.path = "/sys/kernel/config/target/iscsi/" + self.target.wwn
        self.attributes = {"generate_node_acls": "0", "cache_dynamic_acls": "0",
                           "demo_mode_write_protect": "1"}
        self.tpg = Object(tag=1, parent_target=self.target, path=self.target.path + "/tpgt_1",
                          get_attribute=lambda key: self.attributes[key],
                          get_parameter=lambda key: {"ErrorRecoveryLevel": "0"}[key])
        self.target.tpgs = [self.tpg]
        self.acl = Object(node_wwn="iqn.2026-09.test:guest", parent_tpg=self.tpg)
        self.acl.path = self.tpg.path + "/acls/" + self.acl.node_wwn
        self.tpg.node_acls = [self.acl]
        self.tpg.network_portals = []
        self.tpg.luns = []
        self.acl.mapped_luns = []
        self.config = {}
        self.stores = []
        for index in range(2):
            portal = Object(ip_address=f"192.0.2.{index + 1}", port=3260, parent_tpg=self.tpg)
            portal.path = self.tpg.path + f"/np/{portal.ip_address}:3260"
            self.tpg.network_portals.append(portal)
            self.config[portal.path + "/iser"] = "0\n"
            self.config[portal.path + "/cxgbit"] = "0\n"
            file = self.directory / f"disk{index}"
            file.write_bytes(b"x" * 4096)
            storage = Object(path=f"/sys/kernel/config/target/core/fileio_{index}/disk{index}",
                             plugin="fileio", wwn=f"serial-{index}", udev_path=str(file),
                             size=4096, write_back=False, aio=False)
            self.stores.append(storage)
            self.config[storage.path + "/info"] = (
                f"TCM FILEIO ID: {index}        File: {file}  Size: 4096  Mode: O_DSYNC Async: 0\n")
            self.config[storage.path + "/attrib/emulate_write_cache"] = "0\n"
            lun = Object(lun=index, path=self.tpg.path + f"/lun/lun_{index}", storage_object=storage)
            self.tpg.luns.append(lun)
            self.acl.mapped_luns.append(Object(mapped_lun=index, tpg_lun=lun,
                                               path=self.acl.path + f"/lun_{index}"))
        bindings = []
        for portal in self.tpg.network_portals:
            for lun in self.tpg.luns:
                store = lun.storage_object
                status = os.stat(store.udev_path)
                ordinal = len(bindings)
                bindings.append(RouteBinding(
                    ordinal, self.identity.route_digests[ordinal], self.target.wwn, 1,
                    portal.ip_address, 3260, self.acl.node_wwn, lun.lun, lun.lun,
                    store.path, store.wwn, store.udev_path, status.st_dev, status.st_ino, 4096))
        self.bindings = tuple(bindings)
        self.root = Object(targets=[self.target])
        self.reader = patch("target_inventory._read_config", side_effect=self.read_config)
        self.reader.start()
        self.addCleanup(self.reader.stop)

    def read_config(self, path):
        if path not in self.config:
            raise FileNotFoundError(path)
        return self.config[path]

    def resolve(self):
        return resolve_routes(self.root, self.journal, self.identity, self.bindings)

    def resolve_ok(self):
        try:
            return self.resolve()
        except TargetJournalError as error:
            self.fail(f"complete native inventory was refused: {error}")

    def assert_refused(self, reason=None):
        with self.assertRaises(TargetJournalError) as caught:
            self.resolve()
        if reason:
            self.assertEqual(str(caught.exception), reason)

    def test_all_portal_routes_survive_grouping_and_pins_close(self):
        # Dropping the second portal from the inventory or flushing per-route
        # instead of per-file breaks these independently counted obligations.
        result = None
        try:
            result = self.resolve()
        except TargetJournalError:
            pass
        self.assertIsNotNone(result, "complete native routes must resolve to pinned handles")
        with result:
            self.assertEqual(len(result.acl_groups), 1)
            self.assertEqual(result.acl_groups[0].ordinals, (0, 1, 2, 3))
            self.assertEqual(result.acl_groups[0].mapped_lun_paths,
                             (self.acl.path + "/lun_0", self.acl.path + "/lun_1"))
            self.assertEqual([group.ordinals for group in result.flush_groups], [(0, 2), (1, 3)])
            fds = [group.fd for group in result.flush_groups]
            self.assertEqual([os.fstat(fd).st_size for fd in fds], [4096, 4096])
            self.assertEqual(self.journal.denied(), ())
        for fd in fds:
            with self.assertRaises(OSError):
                os.fstat(fd)

    def test_unlisted_portal_is_not_silently_deduplicated(self):
        portal = Object(ip_address="192.0.2.3", port=3260, parent_tpg=self.tpg,
                        path=self.tpg.path + "/np/192.0.2.3:3260")
        self.config[portal.path + "/iser"] = self.config[portal.path + "/cxgbit"] = "0"
        self.tpg.network_portals.append(portal)
        self.assert_refused()

    def test_extra_mapped_lun_or_second_target_alias_refuses(self):
        self.acl.mapped_luns.append(Object(mapped_lun=2, path=self.acl.path + "/lun_2",
                                           tpg_lun=self.tpg.luns[0]))
        self.assert_refused()
        self.acl.mapped_luns.pop()
        # The root census must not stop after it has found the expected target.
        second = Object(wwn="iqn.2026-09.test:alias", fabric_module=Object(name="iscsi"))
        second.path = "/sys/kernel/config/target/iscsi/" + second.wwn
        second.tpgs = [Object(**{**self.tpg.__dict__, "parent_target": second,
                               "path": second.path + "/tpgt_1"})]
        self.root.targets.append(second)
        self.assert_refused()

    def test_extra_tpg_with_victim_and_no_portal_refuses(self):
        extra = Object(**{**self.tpg.__dict__, "tag": 2, "path": self.target.path + "/tpgt_2",
                           "network_portals": []})
        self.target.tpgs.append(extra)
        self.assert_refused()

    def test_another_guest_does_not_add_victim_routes(self):
        other = Object(node_wwn="iqn.2026-09.test:other", parent_tpg=self.tpg,
                       path=self.tpg.path + "/acls/iqn.2026-09.test:other", mapped_luns=[])
        self.tpg.node_acls.append(other)
        with self.resolve_ok() as result:
            self.assertEqual(len(result.acl_groups), 1)

    def test_automatic_access_is_rejected_even_on_unlisted_tpg(self):
        for key, value in (("generate_node_acls", "1"), ("cache_dynamic_acls", "1"),
                           ("demo_mode_write_protect", "0")):
            previous = self.attributes[key]
            self.attributes[key] = value
            with self.subTest(key=key):
                self.assert_refused()
            self.attributes[key] = previous
        self.tpg.get_parameter = lambda key: "1"
        self.assert_refused()

    def test_non_iscsi_fabric_and_offload_or_missing_mode_refuse(self):
        self.target.fabric_module.name = "loopback"
        self.assert_refused()
        self.target.fabric_module.name = "iscsi"
        for field in ("iser", "cxgbit"):
            path = self.tpg.network_portals[0].path + "/" + field
            self.config[path] = "1"
            self.assert_refused()
            del self.config[path]
            self.assert_refused()
            self.config[path] = "0"

    def test_fileio_mode_must_be_explicit_and_synchronous(self):
        path = self.stores[0].path + "/info"
        valid = self.config[path]
        for text in (valid.replace("O_DSYNC", "Buffered-WCE"),
                     valid.replace("Async: 0", "Async: 1"), valid.replace(" Async: 0", ""),
                     valid + valid, "not a fileio configuration"):
            self.config[path] = text
            with self.subTest(text=text):
                self.assert_refused()
        self.config[path] = valid
        self.config[self.stores[0].path + "/attrib/emulate_write_cache"] = "1"
        self.assert_refused()

    def test_native_storage_identity_drift_refuses(self):
        store = self.stores[0]
        for field, bad in (("wwn", "different-serial"), ("plugin", "iblock"),
                           ("udev_path", self.stores[1].udev_path), ("size", 8192),
                           ("path", "/sys/kernel/config/target/core/fileio_4/rebound")):
            previous = getattr(store, field)
            setattr(store, field, bad)
            with self.subTest(field=field):
                self.assert_refused()
            setattr(store, field, previous)

    def test_secure_file_identity_checks_close_every_acquired_fd(self):
        opened = []
        real_open = os.open
        def track(path, flags, *args, **kwargs):
            fd = real_open(path, flags, *args, **kwargs)
            opened.append(fd)
            return fd
        self.bindings = tuple(replace(item, inode=item.inode + 1) if item.mapped_lun == 1 else item
                              for item in self.bindings)
        with patch("target_inventory.os.open", side_effect=track):
            self.assert_refused()
        self.assertTrue(opened)
        for fd in opened:
            with self.assertRaises(OSError):
                os.fstat(fd)

    def test_replaced_file_and_symlink_never_supply_flush_handle(self):
        file = Path(self.stores[0].udev_path)
        retained = self.directory / "retained"
        file.rename(retained)
        file.write_bytes(b"z" * 4096)
        self.assert_refused()
        file.unlink()
        file.symlink_to(retained)
        self.assert_refused()

    def test_symlink_directory_and_short_file_are_refused(self):
        alias = self.directory / "alias"
        alias.symlink_to(self.directory, target_is_directory=True)
        original = self.stores[0].udev_path
        new_path = str(alias / "disk0")
        self.stores[0].udev_path = new_path
        self.config[self.stores[0].path + "/info"] = self.config[
            self.stores[0].path + "/info"].replace(original, new_path)
        self.bindings = tuple(replace(item, file_path=new_path) if item.mapped_lun == 0 else item
                              for item in self.bindings)
        self.assert_refused()
        alias.unlink()
        self.stores[0].udev_path = original
        Path(original).write_bytes(b"short")
        self.assert_refused()

    def test_bindings_cannot_omit_duplicate_or_relabel_a_route(self):
        original = self.bindings
        for bindings in (original[:3], original + (original[0],),
                         (replace(original[0], ordinal=True),) + original[1:],
                         (replace(original[0], digest="ad" * 32),) + original[1:],
                         (replace(original[1], ordinal=0),) + original[1:]):
            self.bindings = bindings
            with self.subTest(bindings=len(bindings)):
                self.assert_refused()
        self.bindings = original

    def complete_journal(self):
        self.journal.deny(self.identity)
        for ordinal in range(4):
            for phase in (1, 2, 3):
                self.journal.advance(self.identity.operation_id, self.identity.target_boot_id, ordinal, phase)

    def test_absent_acl_resume_requires_exact_same_boot_durable_teardown(self):
        self.tpg.node_acls = []
        self.assert_refused()
        self.journal.deny(self.identity)
        self.journal.advance(self.identity.operation_id, self.identity.target_boot_id, 0, 1)
        self.assert_refused()
        for ordinal, start in ((0, 2), (1, 1), (2, 1), (3, 1)):
            for phase in range(start, 4):
                self.journal.advance(self.identity.operation_id, self.identity.target_boot_id, ordinal, phase)
        with self.resolve_ok() as result:
            self.assertEqual(result.acl_groups[0].ordinals, (0, 1, 2, 3))
        self.identity = replace(self.identity, attempt=2, target_boot_id="ef" * 16)
        self.assert_refused()

    def test_journal_completion_cannot_hide_recreated_acl_or_identity_drift(self):
        self.complete_journal()
        self.assert_refused()
        self.tpg.node_acls = []
        for field, bad in (("protected_set_digest", "bc" * 32), ("guest_uuid", "bc" * 16),
                           ("daemon_boot_id", "bc" * 16), ("mapping_generation", 6)):
            original = self.identity
            self.identity = replace(self.identity, **{field: bad})
            with self.subTest(field=field):
                self.assert_refused()
            self.identity = original

    def test_new_daemon_attempt_can_continue_same_boot_obligation(self):
        self.complete_journal()
        self.tpg.node_acls = []
        self.identity = replace(self.identity, attempt=2, daemon_boot_id="ab" * 16)
        with self.resolve_ok() as result:
            self.assertEqual(len(result.flush_groups), 2)
        # Resolution is read-only; only the executor appends the successor intent.
        self.assertEqual(self.journal.denied()[0].identity.attempt, 1)

    def test_native_errors_do_not_disclose_configuration(self):
        self.tpg.get_attribute = lambda key: (_ for _ in ()).throw(RuntimeError("secret=do-not-log"))
        self.assert_refused("TARGET_INVENTORY_UNPROVEN")

    def test_two_declared_luns_sharing_one_file_need_one_pin_and_all_ordinals(self):
        self.tpg.luns[1].storage_object = self.stores[0]
        first = self.bindings[0]
        self.bindings = tuple(replace(item, storage_path=first.storage_path, serial=first.serial,
                                      file_path=first.file_path, device=first.device, inode=first.inode)
                              for item in self.bindings)
        with self.resolve_ok() as result:
            self.assertEqual(len(result.flush_groups), 1)
            self.assertEqual(result.flush_groups[0].ordinals, (0, 1, 2, 3))
            self.assertEqual(len(result.acl_groups[0].mapped_lun_paths), 2)

    def test_backing_open_failure_closes_prior_pin(self):
        opened = []
        real_open = os.open
        def fail_second(path, flags, *args, **kwargs):
            if path == "disk1":
                raise PermissionError("secret configuration")
            fd = real_open(path, flags, *args, **kwargs)
            opened.append(fd)
            return fd
        with patch("target_inventory.os.open", side_effect=fail_second):
            self.assert_refused("TARGET_INVENTORY_UNPROVEN")
        for fd in opened:
            with self.assertRaises(OSError):
                os.fstat(fd)

    def test_enumeration_has_a_finite_bound(self):
        def many_acls():
            for ordinal in itertools.count():
                name = f"iqn.2026-09.test:other{ordinal}"
                yield Object(node_wwn=name, parent_tpg=self.tpg,
                             path=self.tpg.path + "/acls/" + name, mapped_luns=[])
        self.tpg.node_acls = many_acls()
        self.assert_refused("TARGET_INVENTORY_TOO_LARGE")

    def test_completed_journal_does_not_hide_added_portal(self):
        self.complete_journal()
        self.tpg.node_acls = []
        extra = Object(ip_address="192.0.2.3", port=3260, parent_tpg=self.tpg,
                       path=self.tpg.path + "/np/192.0.2.3:3260")
        self.config[extra.path + "/iser"] = self.config[extra.path + "/cxgbit"] = "0"
        self.tpg.network_portals.append(extra)
        self.assert_refused()

    def test_real_config_reader_is_bounded_and_does_not_follow_links(self):
        self.reader.stop()
        path = self.directory / "config"
        path.write_text("0\n")
        self.assertEqual(target_inventory._read_config(str(path)), "0\n")
        path.write_bytes(b"x" * 4097)
        with self.assertRaises(TargetJournalError):
            target_inventory._read_config(str(path))
        link = self.directory / "config-link"
        link.symlink_to(path)
        with self.assertRaises(OSError):
            target_inventory._read_config(str(link))

    def test_resolved_handles_drive_existing_executor_and_then_absent_replay(self):
        from target_drain import drain_routes
        for lun in self.acl.mapped_luns:
            lun.delete = lambda lun=lun: self.acl.mapped_luns.remove(lun)
        def rmdir(path):
            self.assertEqual(path, self.acl.path)
            self.assertEqual(self.journal.denied()[0].phases, (1, 1, 1, 1))
            self.tpg.node_acls.remove(self.acl)
        with self.resolve_ok() as result, patch("target_drain.os.rmdir", side_effect=rmdir):
            self.assertEqual(drain_routes(self.journal, self.identity,
                                         result.acl_groups, result.flush_groups), (3, 3, 3, 3))
        with self.resolve_ok() as result:
            self.assertEqual(drain_routes(self.journal, self.identity,
                                         result.acl_groups, result.flush_groups), (3, 3, 3, 3))


if __name__ == "__main__":
    unittest.main()
