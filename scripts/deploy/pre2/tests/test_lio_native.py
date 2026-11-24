"""LIO-only handle boundaries; native configfs lookup is tested on Linux.

Author: SqlRush <sqlrush@gmail.com>
These injected boundaries do not certify physical storage isolation.
"""

from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import target_native
from target_journal import TargetJournalError


class LioNativeTests(unittest.TestCase):
    boot = "ab" * 16

    def setUp(self):
        self.root = object()
        self.version = SimpleNamespace(__version__=target_native.RTSLIB_VERSION)
        patches = (
            patch.object(target_native.sys, "platform", "linux"),
            patch.object(target_native.os, "geteuid", return_value=0),
            patch.object(target_native, "_configfs_identity", return_value=(4, 12)),
            patch.object(target_native, "_lookup_root", return_value=self.root),
            patch("target_operation._kernel_boot_id", return_value=self.boot),
            patch.dict(sys.modules, {"rtslib_fb": self.version, "libvirt": None}),
        )
        self.boundaries = [item.start() for item in patches]
        for item in patches:
            self.addCleanup(item.stop)

    def handles(self, boot=None):
        entry = getattr(target_native, "lio_native_handles", None)
        self.assertTrue(callable(entry), "LIO-only native lookup is not implemented")
        return entry(self.boot if boot is None else boot)

    def test_lookup_does_not_require_libvirt(self):
        with self.handles() as root:
            self.assertIs(root, self.root)

    def test_wrong_version_refuses_before_lookup(self):
        self.version.__version__ = "2.1.75"
        with self.assertRaises(TargetJournalError):
            with self.handles():
                self.fail("unsupported LIO library admitted")
        self.boundaries[3].assert_not_called()

    def test_nonroot_refuses_before_lookup(self):
        self.boundaries[1].return_value = 1000
        with self.assertRaises(TargetJournalError):
            with self.handles():
                self.fail("unprivileged lookup admitted")
        self.boundaries[3].assert_not_called()

    def test_nonlinux_refuses_before_lookup(self):
        with patch.object(target_native.sys, "platform", "darwin"):
            with self.assertRaises(TargetJournalError):
                with self.handles():
                    self.fail("non-Linux native lookup admitted")
        self.boundaries[3].assert_not_called()

    def test_invalid_boot_is_rejected_without_native_lookup(self):
        for boot in ("0" * 32, "AB" * 16, "short", None, 1):
            with self.subTest(boot=boot), self.assertRaises(TargetJournalError):
                # Pass None explicitly, rather than this test helper's default.
                entry = getattr(target_native, "lio_native_handles", None)
                self.assertTrue(callable(entry), "LIO-only native lookup is not implemented")
                with entry(boot):
                    self.fail("invalid boot admitted")
        self.boundaries[3].assert_not_called()

    def test_wrong_initial_boot_refuses_without_lookup(self):
        self.boundaries[4].return_value = "cd" * 16
        with self.assertRaises(TargetJournalError):
            with self.handles():
                self.fail("wrong boot admitted")
        self.boundaries[3].assert_not_called()

    def test_boot_change_during_lookup_refuses_before_yield(self):
        self.boundaries[4].side_effect = [self.boot, "cd" * 16]
        with self.assertRaises(TargetJournalError):
            with self.handles():
                self.fail("boot drift admitted")

    def test_root_change_during_lookup_refuses_before_yield(self):
        self.boundaries[2].side_effect = [(4, 12), (4, 13)]
        with self.assertRaises(TargetJournalError):
            with self.handles():
                self.fail("replacement configfs admitted")

    def test_boot_change_in_consumer_prevents_returned_observation(self):
        with self.assertRaises(TargetJournalError):
            with self.handles():
                self.boundaries[4].return_value = "cd" * 16

    def test_root_change_in_consumer_prevents_returned_observation(self):
        with self.assertRaises(TargetJournalError):
            with self.handles():
                self.boundaries[2].return_value = (4, 13)

    def test_native_lookup_errors_are_value_free(self):
        self.boundaries[3].side_effect = RuntimeError("private-path chap-secret")
        with self.assertRaises(TargetJournalError) as caught:
            with self.handles():
                self.fail("failed lookup admitted")
        self.assertNotIn("private-path", str(caught.exception))
        self.assertNotIn("chap-secret", str(caught.exception))

    def test_consumer_error_does_not_turn_into_success(self):
        with self.assertRaises(TargetJournalError) as caught:
            with self.handles():
                raise RuntimeError("private-path chap-secret")
        self.assertNotIn("chap-secret", str(caught.exception))


if __name__ == "__main__":
    unittest.main()
