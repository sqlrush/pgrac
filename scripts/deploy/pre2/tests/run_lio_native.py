"""Read-only tests against initialized Linux LIO and the installed rtslib.

Author: SqlRush <sqlrush@gmail.com>
Requires exclusive target ownership; never initializes configfs or exports.
This does not exercise physical route drain or establish isolation authority.
"""

import os
from pathlib import Path
import stat
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from target_journal import TargetJournalError
from target_native import CONFIGFS_ROOT, DBROOT, RTSLIB_VERSION, lio_native_handles
from target_operation import _kernel_boot_id


class InstalledLioTests(unittest.TestCase):
    def setUp(self):
        self.assertEqual(sys.platform, "linux", "requires Linux, not an emulated native boundary")
        self.assertEqual(os.geteuid(), 0, "requires the exclusive root target owner")
        import rtslib_fb
        self.assertEqual(rtslib_fb.__version__, RTSLIB_VERSION)
        self.boot = _kernel_boot_id()

    def snapshot(self):
        result = {}
        for path in (Path(CONFIGFS_ROOT), Path(CONFIGFS_ROOT) / "core", Path(CONFIGFS_ROOT) / "iscsi"):
            info = path.stat(follow_symlinks=False)
            self.assertTrue(stat.S_ISDIR(info.st_mode))
            with os.scandir(path) as entries:
                names = []
                for entry in entries:
                    self.assertLess(len(names), 4096, "native census exceeds selected profile")
                    names.append(entry.name)
            result[str(path)] = (info.st_dev, info.st_ino, info.st_mode, sorted(names))
        result["dbroot"] = (Path(CONFIGFS_ROOT) / "dbroot").read_bytes()
        return result

    def test_real_initialized_lookup_preserves_native_configuration(self):
        before = self.snapshot()
        with lio_native_handles(self.boot) as root:
            self.assertEqual(root.path, CONFIGFS_ROOT)
            self.assertEqual(root.dbroot, DBROOT)
        self.assertEqual(self.snapshot(), before)

    def test_upstream_initializer_is_never_called(self):
        from rtslib_fb.root import RTSRoot
        with patch.object(RTSRoot, "__init__", side_effect=AssertionError("implicit initialization")):
            with lio_native_handles(self.boot) as root:
                self.assertEqual(root.path, CONFIGFS_ROOT)

    def test_fresh_boot_is_required(self):
        wrong = "ff" * 16 if self.boot != "ff" * 16 else "ee" * 16
        with self.assertRaises(TargetJournalError):
            with lio_native_handles(wrong):
                self.fail("foreign target boot admitted")

    def test_zero_boot_is_not_an_unknown_identity(self):
        with self.assertRaises(TargetJournalError):
            with lio_native_handles("0" * 32):
                self.fail("unknown target boot admitted")

    def test_nonselected_library_version_refuses(self):
        import rtslib_fb
        with patch.object(rtslib_fb, "__version__", "unsupported"):
            with self.assertRaises(TargetJournalError):
                with lio_native_handles(self.boot):
                    self.fail("unsupported library admitted")

    def test_consumer_failure_stays_value_free(self):
        with self.assertRaises(TargetJournalError) as caught:
            with lio_native_handles(self.boot):
                raise OSError("private-consumer-detail")
        self.assertEqual(str(caught.exception), "TARGET_NATIVE_UNPROVEN")

    def test_libvirt_is_not_needed_for_real_native_lookup(self):
        with patch.dict(sys.modules, {"libvirt": None}):
            with lio_native_handles(self.boot) as root:
                self.assertEqual(root.path, CONFIGFS_ROOT)


if __name__ == "__main__":
    unittest.main(verbosity=2)
