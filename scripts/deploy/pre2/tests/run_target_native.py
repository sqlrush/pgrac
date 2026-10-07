"""Linux installed-rtslib tests for initialized-only native handle construction.

Author: SqlRush <sqlrush@gmail.com>
Scratch paths and libvirt fixture only; no VM, export or module is changed.
"""

from contextlib import ExitStack
from pathlib import Path
import os
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from target_journal import TargetJournalError
try:
    import target_native
except ModuleNotFoundError:
    target_native = None


class Connection:
    def __init__(self):
        self.closed = 0
        self.uri = "qemu:///system"
        self.version = 10000000

    def getURI(self):
        return self.uri

    def getType(self):
        return "QEMU"

    def getLibVersion(self):
        return self.version

    def isAlive(self):
        return 1

    def close(self):
        self.closed += 1
        return 0


class NativeHandleTests(unittest.TestCase):
    def setUp(self):
        self.assertIsNotNone(target_native, "initialized-only native handles are missing")
        import libvirt
        import rtslib_fb
        self.assertEqual(rtslib_fb.__version__, "2.1.74")
        self.assertEqual(libvirt.getVersion(), 10000000)
        temporary = tempfile.TemporaryDirectory(prefix="pgrac-native-lookup-")
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name).resolve()
        self.root = self.directory / "target"
        self.root.mkdir()
        (self.root / "core").mkdir()
        (self.root / "iscsi").mkdir()
        (self.root / "dbroot").write_text("/etc/rtslib-fb-target\n")
        self.connection = Connection()
        self.opened = []
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.stack.enter_context(patch("target_native.CONFIGFS_ROOT", str(self.root)))
        self.stack.enter_context(patch("rtslib_fb.node.CFSNode.configfs_dir", str(self.root)))
        # Calling the upstream initializer is specifically the behavior forbidden
        # here: it may mount, load modules and create/change host configuration.
        self.stack.enter_context(patch("rtslib_fb.root.RTSRoot.__init__",
                                       side_effect=AssertionError("implicit host setup")))
        self.stack.enter_context(patch("target_native.os.geteuid", return_value=0))
        def connect(uri):
            self.assertEqual(uri, "qemu:///system")
            self.opened.append(uri)
            return self.connection
        self.stack.enter_context(patch("libvirt.openReadOnly", side_effect=connect))
        self.filesystem = self.stack.enter_context(patch("target_native._configfs_identity", return_value=(4, 12)))

    def test_actual_rtslib_lookup_is_read_only_and_connection_closed(self):
        before = {str(p.relative_to(self.root)): p.stat().st_mtime_ns for p in self.root.rglob("*")}
        with target_native.native_handles() as (root, connection):
            self.assertIs(connection, self.connection)
            self.assertEqual(root.dbroot, "/etc/rtslib-fb-target")
            self.assertEqual(tuple(root.targets), ())
            self.assertEqual(tuple(root.tpgs), ())
            self.assertEqual(tuple(root.storage_objects), ())
        self.assertEqual(self.opened, ["qemu:///system"])
        self.assertEqual(self.connection.closed, 1)
        self.assertEqual({str(p.relative_to(self.root)): p.stat().st_mtime_ns for p in self.root.rglob("*")}, before)

    def test_absent_root_or_dbroot_never_autocreates_or_opens_libvirt(self):
        (self.root / "dbroot").unlink()
        with self.assertRaises(TargetJournalError):
            with target_native.native_handles():
                self.fail("absent dbroot yielded handles")
        self.assertFalse((self.root / "dbroot").exists())
        self.assertEqual(self.opened, [])

    def test_wrong_versions_refuse_before_opening_connection(self):
        for variable, value in (("rtslib_fb.__version__", "2.1.75"),
                                ("libvirt.getVersion", lambda: 9000000)):
            with self.subTest(variable=variable), patch(variable, value), self.assertRaises(TargetJournalError):
                with target_native.native_handles():
                    self.fail("wrong version yielded handles")
        self.assertEqual(self.opened, [])

    def test_wrong_server_identity_closes_without_yielding(self):
        for uri, version in (("qemu:///session", 10000000), ("qemu:///system", 9000000)):
            self.connection.uri, self.connection.version = uri, version
            with self.subTest(uri=uri, version=version), self.assertRaises(TargetJournalError):
                with target_native.native_handles():
                    self.fail("wrong server yielded handles")
        self.assertEqual(self.connection.closed, 2)

    def test_root_replacement_during_open_is_unproven_and_closes(self):
        self.filesystem.side_effect = [(4, 12), (4, 13)]
        with self.assertRaises(TargetJournalError):
            with target_native.native_handles():
                self.fail("changed root yielded handles")
        self.assertEqual(self.connection.closed, 1)

    def test_native_failure_does_not_echo_details_or_leak_connection(self):
        with self.assertRaises(TargetJournalError) as caught:
            with target_native.native_handles():
                raise OSError("private-password-value")
        self.assertEqual(str(caught.exception), "TARGET_NATIVE_UNPROVEN")
        self.assertEqual(self.connection.closed, 1)

    def test_non_configfs_root_is_rejected_by_actual_syscall(self):
        self.stack.close()
        with patch("target_native.CONFIGFS_ROOT", str(self.root)), self.assertRaises(TargetJournalError):
            target_native._configfs_identity()

    def test_dbroot_is_not_silently_changed(self):
        (self.root / "dbroot").write_text("/var/target\n")
        with self.assertRaises(TargetJournalError):
            with target_native.native_handles():
                self.fail("different dbroot yielded handles")
        self.assertEqual((self.root / "dbroot").read_text(), "/var/target\n")
        self.assertEqual(self.opened, [])


if __name__ == "__main__":
    if sys.platform != "linux":
        raise SystemExit("This installed-native suite requires Linux, not a skip")
    unittest.main(verbosity=2)
