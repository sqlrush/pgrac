"""Real durable rejoin lifecycle with only native kernel/libvirt edges injected.

Author: SqlRush <sqlrush@gmail.com>
This suite is not a physical fencing or deployment certificate.
"""

from dataclasses import replace
import importlib
import os
from pathlib import Path
import sys
import tempfile
import time
from types import SimpleNamespace as Object
import unittest
from unittest.mock import patch
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from target_journal import RejoinIntent, TargetJournal, TargetJournalError
from test_target_guest import Connection
import test_target_startup as startup_tests


class PermissionTests(unittest.TestCase):
    setUp_startup = startup_tests.StartupTests.setUp
    setUp_inventory = startup_tests.StartupTests.setUp_inventory
    read_config = startup_tests.StartupTests.read_config
    restore = startup_tests.StartupTests.restore

    def setUp(self):
        self.setUp_startup()
        self.root.targets, self.root.storage_objects = [self.target], self.stores
        self.attributes["authentication"] = "1"
        config = self.template["targets"][0]["tpgs"][0]
        config["attributes"]["authentication"] = 1
        config["node_acls"][0].update(chap_userid="test-user", chap_password="test-only-secret")
        self.connection = Connection()
        self.connection.domain.uuid = str(uuid.UUID(hex=self.identity.guest_uuid))
        self.journal.deny(self.identity)
        for n in range(4):
            for phase in (1, 2, 3):
                self.journal.advance(self.identity.operation_id, self.identity.target_boot_id, n, phase)
        self.intent = RejoinIntent(123, 2, 7, 8, "ab" * 32)
        self.journal.arm_rejoin(self.identity, self.intent)
        self.tpg.node_acls = []
        self.acl.mapped_luns = []
        self.events = []
        self.native_parent = self.directory / "native-parent"
        self.native_parent.mkdir()
        self.native_fds = []

    def api(self):
        try:
            return importlib.import_module("target_permissions")
        except ImportError:
            self.fail("controlled native rejoin permissions are not implemented")

    def create_acl(self, tpg, iqn):
        self.assertEqual(self.journal.denied()[0].rejoin.phase, 2)
        self.assertIs(tpg, self.tpg)
        self.assertEqual(iqn, self.acl.node_wwn)
        self.assertEqual(tpg.node_acls, [])
        self.events.append("create-acl")
        for name in ("chap_userid", "chap_password", "chap_mutual_userid", "chap_mutual_password"):
            setattr(self.acl, name, "")
        tpg.node_acls.append(self.acl)
        return self.acl

    def create_lun(self, acl, index, tpg_lun):
        self.assertEqual((acl.chap_userid, acl.chap_password), ("test-user", "test-only-secret"))
        self.events.append(f"create-lun-{index}")
        lun = Object(mapped_lun=index, tpg_lun=self.tpg.luns[tpg_lun],
                     path=acl.path + f"/lun_{index}", write_protect=False)
        acl.mapped_luns.append(lun)
        return lun

    def patch_native(self):
        api = self.api()
        patches = [patch.object(api, "_kernel_boot_id", return_value=self.identity.target_boot_id),
                   patch.object(api, "_create_acl", side_effect=self.create_acl),
                   patch.object(api, "_create_lun", side_effect=self.create_lun),
                   patch("target_inventory._mapped_link", side_effect=self.mapped_link),
                   patch.object(api, "_open_acl_parent", side_effect=self.open_parent),
                   patch.object(api, "_same_parent", side_effect=self.same_parent),
                   patch.object(api, "_remove_mapped_lun", side_effect=self.remove_lun),
                   patch.object(api, "_rmdir_acl", side_effect=self.remove_acl)]
        for item in patches:
            item.start()
            self.addCleanup(item.stop)
        return api

    def mapped_link(self, path, expected):
        lun = next(lun for lun in self.acl.mapped_luns if lun.path == path)
        if lun.tpg_lun is None:
            return None
        if lun.tpg_lun.path != expected:
            raise TargetJournalError("TARGET_LUN_IDENTITY")
        return "test-link"

    def open_parent(self, path):
        self.assertEqual(path, self.acl.path)
        fd = os.open(self.native_parent, os.O_RDONLY | os.O_DIRECTORY)
        self.native_fds.append(fd)
        return fd, os.fstat(fd)

    def same_parent(self, path, fd, initial):
        current = os.fstat(fd)
        self.assertEqual((current.st_dev, current.st_ino), (initial.st_dev, initial.st_ino))

    def remove_lun(self, path, expected):
        lun = next(lun for lun in self.acl.mapped_luns if lun.path == path)
        self.mapped_link(path, expected)
        self.events.append("unlink-lun")
        self.acl.mapped_luns.remove(lun)

    def remove_acl(self, name, fd):
        self.assertEqual(name, self.acl.node_wwn)
        self.assertEqual(self.journal.denied()[0].rejoin.phase, 4)
        self.assertEqual(self.journal.denied()[0].phases, (1,) * 4)
        self.assertEqual(self.connection.domain.active, 0)
        self.assertEqual(self.acl.mapped_luns, [])
        os.fstat(fd)
        self.events.append("rmdir-acl")
        if self.acl not in self.tpg.node_acls:
            raise FileNotFoundError
        self.tpg.node_acls.remove(self.acl)

    def restore_access(self, api, deadline=None):
        return api.restore_rejoin_access(self.registry, self.journal, self.root, self.connection,
                                         self.identity, self.intent, self.template,
                                         time.monotonic_ns() + 5_000_000_000 if deadline is None else deadline)

    def revoke(self, api):
        return api.revoke_rejoin_access(self.registry, self.journal, self.root, self.connection,
                                        self.identity, self.intent, time.monotonic_ns() + 5_000_000_000)

    def test_restore_is_durable_before_native_change_and_exact_repeat_is_read_only(self):
        api = self.patch_native()
        result = self.restore_access(api)
        self.assertEqual(result.identity, self.identity)
        self.assertEqual(result.intent, self.intent)
        self.assertEqual(self.journal.denied()[0].rejoin.phase, 3)
        self.assertEqual(self.events, ["create-acl", "create-lun-0", "create-lun-1"])
        before = self.journal.sequence
        self.restore_access(api)
        self.assertEqual(before, self.journal.sequence)
        self.assertEqual(len(self.events), 3)
        self.assertFalse(self.journal.completion_recorded(self.identity.operation_id, self.identity.target_boot_id))

    def test_refresh_cannot_repeat_permission_restore(self):
        api = self.patch_native()
        self.restore_access(api)
        self.identity = replace(self.identity, attempt=self.identity.attempt + 1)
        self.journal.refresh_rejoin(self.identity, self.intent)
        before = (self.journal.sequence, list(self.events))
        with self.assertRaises(TargetJournalError):
            self.restore_access(api)
        self.assertEqual((self.journal.sequence, self.events), before)
        self.assertTrue(self.journal.denied()[0].rejoin.refresh_started)

    def test_later_owner_can_compensate_native_access_but_old_one_cannot_restore(self):
        api = self.patch_native()
        self.restore_access(api)
        old = self.identity
        self.identity = replace(old, attempt=old.attempt + 1, daemon_boot_id="ee" * 16)
        self.journal.revoke_rejoin(self.identity, self.intent)
        result = self.revoke(api)
        self.assertEqual(result.identity, self.identity)
        self.assertEqual(self.journal.denied()[0].phases, (3, 3, 3, 3))
        self.assertEqual(self.tpg.node_acls, [])
        self.identity = old
        with self.assertRaises(TargetJournalError):
            self.restore_access(api)

    def test_refreshed_owner_compensation_reaches_native_cleanup_and_exact_retry(self):
        api = self.patch_native()
        self.restore_access(api)
        self.identity = replace(self.identity, attempt=self.identity.attempt + 1)
        self.journal.refresh_rejoin(self.identity, self.intent)
        self.identity = replace(self.identity, attempt=self.identity.attempt + 1, daemon_boot_id="ee" * 16)
        self.journal.revoke_rejoin(self.identity, self.intent)
        self.assertTrue(self.journal.denied()[0].rejoin.refresh_started)
        self.assertEqual(self.revoke(api).phase, 4)
        before = (self.journal.sequence, list(self.events))
        self.assertEqual(self.revoke(api).phase, 4)
        self.assertEqual((self.journal.sequence, self.events), before)
        self.assertEqual(self.journal.denied()[0].phases, (3, 3, 3, 3))

    def test_partial_restore_failure_revokes_and_can_be_drained(self):
        api = self.patch_native()
        create = self.create_lun
        def fail_second(acl, index, tpg_lun):
            if index == 1:
                raise OSError("test-only-secret")
            return create(acl, index, tpg_lun)
        with patch.object(api, "_create_lun", side_effect=fail_second), self.assertRaises(TargetJournalError) as error:
            self.restore_access(api)
        self.assertNotIn("test-only-secret", str(error.exception))
        self.assertEqual(self.journal.denied()[0].rejoin.phase, 4)
        self.assertEqual(len(self.acl.mapped_luns), 1)
        self.revoke(api)
        self.assertEqual(self.tpg.node_acls, [])
        self.assertTrue(self.journal.completion_recorded(self.identity.operation_id, self.identity.target_boot_id))

    def test_armed_revoke_requires_serialized_native_negative_and_real_flush(self):
        api = self.patch_native()
        real_sync = os.fsync
        def sync(fd):
            real_sync(fd)
            if fd != self.journal.fd:
                self.events.append("flush")
        with patch("target_permissions.os.fsync", side_effect=sync):
            self.revoke(api)
        self.assertEqual(self.events, ["rmdir-acl", "flush", "flush"])
        for fd in self.native_fds:
            with self.assertRaises(OSError):
                os.fstat(fd)

    def test_restoring_with_unlinked_native_lun_converges_without_reopening(self):
        api = self.patch_native()
        self.journal.advance_rejoin(self.identity, self.intent, 2)
        self.create_acl(self.tpg, self.acl.node_wwn)
        self.acl.mapped_luns = [Object(mapped_lun=0, tpg_lun=None, path=self.acl.path + "/lun_0")]
        with self.assertRaises(TargetJournalError):
            self.restore_access(api)
        self.revoke(api)
        self.assertEqual(self.events, ["create-acl", "unlink-lun", "rmdir-acl"])

    def test_access_ready_revoke_requires_off_and_rejects_late_restore(self):
        api = self.patch_native()
        self.restore_access(api)
        before = list(self.events)
        self.connection.domain.active = 1
        with self.assertRaises(TargetJournalError):
            self.revoke(api)
        self.assertEqual(self.events, before)
        self.assertEqual(self.journal.denied()[0].rejoin.phase, 4)
        self.connection.domain.active = 0
        self.revoke(api)
        with self.assertRaises(TargetJournalError):
            self.restore_access(api)

    def test_interrupted_after_native_rmdir_reconciles_without_forging_a_return(self):
        api = self.patch_native()
        self.restore_access(api)
        advance = self.journal.advance
        def interrupted(op, boot, ordinal, phase):
            if phase == 2:
                raise RuntimeError("simulated stopped worker before journal append")
            return advance(op, boot, ordinal, phase)
        with patch.object(self.journal, "advance", side_effect=interrupted), self.assertRaises(TargetJournalError):
            self.revoke(api)
        self.assertEqual(self.tpg.node_acls, [])
        self.assertEqual(self.journal.denied()[0].phases, (1,) * 4)
        self.journal.close()
        self.journal = TargetJournal(self.directory, self.registry.inventory_digest, owner_uid=os.geteuid())
        self.addCleanup(self.journal.close)
        self.revoke(api)
        self.assertEqual(self.events.count("rmdir-acl"), 2)

    def test_wrong_identity_deadline_native_on_or_auth_never_creates_acl(self):
        api = self.patch_native()
        original = self.intent
        self.intent = replace(original, candidate_incarnation=9)
        with self.assertRaises(TargetJournalError):
            self.restore_access(api)
        self.intent = original
        with self.assertRaises(TargetJournalError):
            self.restore_access(api, deadline=1)
        self.connection.domain.active = 1
        with self.assertRaises(TargetJournalError):
            self.restore_access(api)
        self.connection.domain.active = 0
        self.attributes["authentication"] = "0"
        with self.assertRaises(TargetJournalError):
            self.restore_access(api)
        self.assertEqual(self.events, [])

    def test_unknown_route_drift_is_not_a_permitted_partial_restore(self):
        api = self.patch_native()
        self.restore_access(api)
        self.acl.mapped_luns.append(Object(mapped_lun=99, path=self.acl.path + "/lun_99",
                                           tpg_lun=self.tpg.luns[0]))
        before = list(self.events)
        with self.assertRaises(TargetJournalError):
            self.revoke(api)
        self.assertEqual(self.events, before)
        self.assertEqual(self.journal.denied()[0].phases, (0,) * 4)

    def test_failed_flush_retains_drained_but_unfinished_deny(self):
        api = self.patch_native()
        real_sync = os.fsync
        def sync(fd):
            if fd != self.journal.fd:
                raise OSError("test-only-secret")
            real_sync(fd)
        with patch("target_permissions.os.fsync", side_effect=sync), self.assertRaises(TargetJournalError):
            self.revoke(api)
        self.assertEqual(self.journal.denied()[0].phases, (2,) * 4)
        self.assertFalse(self.journal.completion_recorded(self.identity.operation_id, self.identity.target_boot_id))
        self.revoke(api)
        self.assertEqual(self.events, ["rmdir-acl"])

    def test_journal_failure_prevents_first_acl_create(self):
        api = self.patch_native()
        with patch("target_journal.os.fsync", side_effect=OSError("disk-full")), self.assertRaises(TargetJournalError):
            self.restore_access(api)
        self.assertEqual(self.events, [])

    def test_revocation_during_native_create_prevents_late_mapping(self):
        api = self.patch_native()
        def revoke_after_create(tpg, iqn):
            acl = self.create_acl(tpg, iqn)
            self.journal.advance_rejoin(self.identity, self.intent, 4)
            return acl
        with patch.object(api, "_create_acl", side_effect=revoke_after_create), self.assertRaises(TargetJournalError):
            self.restore_access(api)
        self.assertEqual(self.events, ["create-acl"])
        self.assertEqual(self.acl.mapped_luns, [])

    def test_final_off_failure_keeps_rejoin_revoked_not_ready(self):
        api = self.patch_native()
        create = self.create_lun
        def goes_on(acl, index, tpg_lun):
            result = create(acl, index, tpg_lun)
            if index == 1:
                self.connection.domain.active = 1
            return result
        with patch.object(api, "_create_lun", side_effect=goes_on), self.assertRaises(TargetJournalError):
            self.restore_access(api)
        self.assertEqual(self.journal.denied()[0].rejoin.phase, 4)
        self.assertFalse(self.journal.completion_recorded(self.identity.operation_id, self.identity.target_boot_id))

    def test_parent_drift_and_failed_rmdir_are_not_serialized_negative_success(self):
        api = self.patch_native()
        with patch.object(api, "_same_parent", side_effect=TargetJournalError("TARGET_ACL_IDENTITY")), \
                self.assertRaises(TargetJournalError):
            self.revoke(api)
        self.assertEqual(self.events, [])
        self.assertEqual(self.journal.denied()[0].phases, (1,) * 4)
        with patch.object(api, "_rmdir_acl", side_effect=PermissionError("no native authority")), \
                self.assertRaises(TargetJournalError):
            self.revoke(api)
        self.assertEqual(self.journal.denied()[0].phases, (1,) * 4)
        self.assertFalse(self.journal.completion_recorded(self.identity.operation_id, self.identity.target_boot_id))

    def test_wrong_boot_and_stale_attempt_do_not_revoke_or_mutate(self):
        api = self.patch_native()
        before = self.journal.sequence
        with patch.object(api, "_kernel_boot_id", return_value="ef" * 16), self.assertRaises(TargetJournalError):
            self.revoke(api)
        self.identity = replace(self.identity, attempt=2)
        with self.assertRaises(TargetJournalError):
            self.revoke(api)
        self.assertEqual(self.journal.sequence, before)
        self.assertEqual(self.events, [])


class NativeBoundaryTests(unittest.TestCase):
    """Exercise actual user-space syscalls on scratch, not LIO certification."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="pgrac-permission-native-")
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name).resolve()

    def test_mapped_link_allows_unlinked_partial_but_not_wrong_or_extra_route(self):
        from target_inventory import _mapped_link
        path = self.directory / "lun_0"
        path.mkdir()
        expected = self.directory / "backing-lun"
        expected.mkdir()
        self.assertIsNone(_mapped_link(str(path), str(expected)))
        (path / "exact").symlink_to(expected)
        self.assertEqual(_mapped_link(str(path), str(expected)), "exact")
        with self.assertRaises(TargetJournalError):
            _mapped_link(str(path), str(self.directory / "different"))
        (path / "extra").symlink_to(expected)
        with self.assertRaises(TargetJournalError):
            _mapped_link(str(path), str(expected))

    def test_real_exact_lun_unlink_and_parent_relative_acl_rmdir(self):
        import target_permissions as api
        parent = self.directory / "acls"
        parent.mkdir()
        acl = parent / "initiator"
        acl.mkdir()
        fd, identity = api._open_acl_parent(str(acl))
        try:
            expected = self.directory / "tpg-lun"
            expected.mkdir()
            for index in (0, 1):
                lun = acl / f"lun_{index}"
                lun.mkdir()
                if index == 0:
                    (lun / "exact").symlink_to(expected)
                api._remove_mapped_lun(str(lun), str(expected))
                self.assertFalse(lun.exists())
            api._same_parent(str(acl), fd, identity)
            api._rmdir_acl(acl.name, fd)
            api._same_parent(str(acl), fd, identity)
            with self.assertRaises(FileNotFoundError):
                api._rmdir_acl(acl.name, fd)
            self.assertTrue(expected.exists())
        finally:
            os.close(fd)

    def test_changed_or_symlinked_parent_is_not_the_pinned_parent(self):
        import target_permissions as api
        parent = self.directory / "acls"
        parent.mkdir()
        acl = parent / "initiator"
        fd, identity = api._open_acl_parent(str(acl))
        try:
            parent.rename(self.directory / "old-acls")
            parent.mkdir()
            with self.assertRaises(TargetJournalError):
                api._same_parent(str(acl), fd, identity)
            link = self.directory / "linked-acls"
            link.symlink_to(parent, target_is_directory=True)
            with self.assertRaises(OSError):
                api._open_acl_parent(str(link / "initiator"))
        finally:
            os.close(fd)


if __name__ == "__main__":
    unittest.main()
