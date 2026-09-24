"""Exercise actual journal ordering with injected LIO syscall boundaries.

Author: SqlRush <sqlrush@gmail.com>
No fake LIO call below is a kernel drain witness or deployment certification.
"""

from dataclasses import replace
import os
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from target_drain import AclGroup, FlushGroup, drain_routes
from target_journal import DrainIdentity, TargetJournal, TargetJournalError


class TargetDrainTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="pgrac-drain-order-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.journal = TargetJournal(self.root, "09" * 32, initialize=True, owner_uid=os.geteuid())
        self.addCleanup(self.journal.close)
        self.request = DrainIdentity("01" * 16, 1, "02" * 16, "03" * 16,
                                     "04" * 16, 5, "06" * 32, ("07" * 32, "08" * 32))
        self.events = []
        self.path = "/sys/kernel/config/target/iscsi/iqn.2026-09.test:target/tpgt_1/acls/iqn.2026-09.test:guest"
        self.luns = [SimpleNamespace(path=f"{self.path}/lun_{n}",
                                     delete=lambda n=n: self.events.append(("unmap", n))) for n in range(2)]
        self.acl = SimpleNamespace(path=self.path, mapped_luns=self.luns)
        self.group = AclGroup(self.acl, self.path, tuple(l.path for l in self.luns), (0, 1))
        self.fd = os.open(self.root / "backstore", os.O_RDWR | os.O_CREAT | os.O_EXCL, 0o600)
        self.addCleanup(os.close, self.fd)
        st = os.fstat(self.fd)
        self.flush = FlushGroup(self.fd, st.st_dev, st.st_ino, (0, 1))

    def phases(self):
        return self.journal.denied()[0].phases

    def teardown(self, path):
        self.assertEqual(path, self.path)
        self.assertEqual(self.phases(), (1, 1))
        raw = (self.root / "deny.journal").read_bytes()
        self.assertEqual(raw.count(b'"phase":1'), 2)
        self.events.append(("rmdir-returned", path))

    def run_drain(self, *, groups=None, flushes=None):
        return drain_routes(self.journal, self.request,
                            (self.group,) if groups is None else groups,
                            (self.flush,) if flushes is None else flushes)

    def test_deny_and_all_begins_precede_actual_teardown_then_flush(self):
        actual_fsync = os.fsync
        def fsync(fd):
            if fd == self.fd:
                self.assertEqual(self.phases(), (2, 2))
                self.events.append(("flush", fd))
            actual_fsync(fd)
        with patch("target_drain.os.rmdir", side_effect=self.teardown), \
                patch("target_drain.os.fsync", side_effect=fsync):
            self.assertEqual(self.run_drain(), (3, 3))
        self.assertEqual([event[0] for event in self.events], ["unmap", "unmap", "rmdir-returned", "flush"])

    def test_missing_acl_is_not_a_returned_teardown(self):
        with patch("target_drain.os.rmdir", side_effect=FileNotFoundError), \
                self.assertRaises(TargetJournalError):
            self.run_drain()
        self.assertEqual(self.phases(), (1, 1))
        self.assertFalse(self.journal.completion_recorded(self.request.operation_id, self.request.target_boot_id))

    def test_unmap_failure_does_not_advance_any_route(self):
        self.luns[1].delete = lambda: (_ for _ in ()).throw(OSError("unmap failed"))
        with patch("target_drain.os.rmdir") as rmdir, self.assertRaises(TargetJournalError):
            self.run_drain()
        rmdir.assert_not_called()
        self.assertEqual(self.phases(), (1, 1))

    def test_flush_failure_preserves_drained_but_not_flushed(self):
        actual_fsync = os.fsync
        def fsync(fd):
            if fd == self.fd:
                raise OSError("flush failed")
            actual_fsync(fd)
        with patch("target_drain.os.rmdir", side_effect=self.teardown), \
                patch("target_drain.os.fsync", side_effect=fsync), self.assertRaises(TargetJournalError):
            self.run_drain()
        self.assertEqual(self.phases(), (2, 2))
        # Previously completed teardown can resume only while the ACL stays absent.
        with patch("target_drain.os.lstat", side_effect=FileNotFoundError), \
                patch("target_drain.os.rmdir") as rmdir:
            self.assertEqual(self.run_drain(), (3, 3))
        rmdir.assert_not_called()

    def test_partial_or_duplicate_route_groups_never_mutate(self):
        for groups, flushes in (((replace(self.group, ordinals=(0,)),), (self.flush,)),
                                ((self.group, self.group), (self.flush,)),
                                ((self.group,), (replace(self.flush, ordinals=(0,)),)),
                                ((self.group,), (self.flush, self.flush)),
                                ((replace(self.group, ordinals=(True, 0)),), (self.flush,))):
            with self.subTest(groups=groups), patch("target_drain.os.rmdir") as rmdir, \
                    self.assertRaises(TargetJournalError):
                self.run_drain(groups=groups, flushes=flushes)
            rmdir.assert_not_called()
            self.assertEqual(self.journal.denied(), ())
        self.assertEqual(self.events, [])

    def test_unexpected_lun_path_or_replaced_file_refuses_before_teardown(self):
        cases = [(replace(self.group, path="/tmp/not-configfs"), self.flush),
                 (replace(self.group, mapped_lun_paths=(self.luns[0].path,)), self.flush),
                 (self.group, replace(self.flush, inode=self.flush.inode + 1))]
        for group, flush in cases:
            with self.subTest(group=group.path), patch("target_drain.os.rmdir") as rmdir, \
                    self.assertRaises(TargetJournalError):
                self.run_drain(groups=(group,), flushes=(flush,))
            rmdir.assert_not_called()
            self.assertEqual(self.journal.denied(), ())

    def test_recreated_acl_cannot_reuse_old_completion(self):
        with patch("target_drain.os.rmdir", side_effect=self.teardown):
            self.run_drain()
        with patch("target_drain.os.lstat", return_value=object()), \
                patch("target_drain.os.rmdir") as rmdir, self.assertRaises(TargetJournalError):
            self.run_drain()
        rmdir.assert_not_called()

    def test_fresh_target_boot_cannot_reuse_old_absent_acl(self):
        with patch("target_drain.os.rmdir", side_effect=self.teardown):
            self.run_drain()
        self.request = replace(self.request, attempt=2, target_boot_id="bc" * 16)
        with patch("target_drain.os.rmdir", side_effect=FileNotFoundError), \
                self.assertRaises(TargetJournalError):
            self.run_drain()
        self.assertEqual(self.phases(), (1, 1))

    def test_deny_persistence_failure_never_calls_lio(self):
        with patch("target_journal.os.fsync", side_effect=OSError), \
                patch("target_drain.os.rmdir") as rmdir, self.assertRaises(TargetJournalError):
            self.run_drain()
        rmdir.assert_not_called()
        self.assertEqual(self.events, [])

    def test_absence_permission_failure_cannot_reuse_completed_record(self):
        with patch("target_drain.os.rmdir", side_effect=self.teardown):
            self.run_drain()
        with patch("target_drain.os.lstat", side_effect=PermissionError), \
                patch("target_drain.os.rmdir") as rmdir, self.assertRaises(TargetJournalError):
            self.run_drain()
        rmdir.assert_not_called()

    def test_late_acl_reappearance_never_returns_complete_result(self):
        with patch("target_drain.os.rmdir", side_effect=self.teardown), \
                patch("target_drain.os.lstat", return_value=object()), \
                self.assertRaises(TargetJournalError):
            self.run_drain()
        # Physical history is retained, but final verification rejected admission.
        self.assertEqual(self.phases(), (3, 3))

    def test_missing_second_acl_does_not_flush_first_or_mark_whole_set(self):
        other_path = self.path.replace("tpgt_1", "tpgt_2")
        other_lun = SimpleNamespace(path=other_path + "/lun_1", delete=lambda: None)
        other_acl = SimpleNamespace(path=other_path, mapped_luns=[other_lun])
        first = replace(self.group, ordinals=(0,))
        second = AclGroup(other_acl, other_path, (other_lun.path,), (1,))
        actual_fsync = os.fsync
        flushed = []
        def fsync(fd):
            if fd == self.fd:
                flushed.append(fd)
            actual_fsync(fd)
        def teardown(path):
            if path == other_path:
                raise FileNotFoundError
        with patch("target_drain.os.rmdir", side_effect=teardown), \
                patch("target_drain.os.fsync", side_effect=fsync), self.assertRaises(TargetJournalError):
            self.run_drain(groups=(first, second))
        self.assertEqual(self.phases(), (2, 1))
        self.assertEqual(flushed, [])

    def test_native_api_error_details_are_not_exposed(self):
        self.luns[0].delete = lambda: (_ for _ in ()).throw(RuntimeError("secret=do-not-log"))
        with self.assertRaises(TargetJournalError) as context:
            self.run_drain()
        self.assertEqual(str(context.exception), "TARGET_DRAIN_UNPROVEN")
        self.assertTrue(context.exception.__suppress_context__)


if __name__ == "__main__":
    unittest.main()
