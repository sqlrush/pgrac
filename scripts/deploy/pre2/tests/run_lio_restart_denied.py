"""Native restart regression for a retained lab-only LIO deny journal.

Author: SqlRush <sqlrush@gmail.com>
Run explicitly as Linux root after the target has restarted closed. The case
directory is the sole operator's existing component-test fixture, not a signed
provider map. This test never restores exports, clears debt or admits a guest.
"""

from dataclasses import replace
import os
from pathlib import Path
import sys
import time
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from lima_storage import deny_storage
from target_inventory import RouteBinding
from target_journal import DrainIdentity, TargetJournal, TargetJournalError, _identity
from target_native import lio_native_handles
from target_operation import _kernel_boot_id
from target_registry import _keys, _read_owned

CASE = Path(sys.argv.pop(1)) if __name__ == "__main__" and len(sys.argv) > 1 else None


class RestartDeniedTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if sys.platform != "linux" or os.geteuid() != 0 or CASE is None:
            raise RuntimeError("requires Linux root and an explicit retained case directory")
        fixture = _read_owned(str(CASE / "fixture.json"), 0)
        _keys(fixture, {"inventory_digest", "identity", "inventory", "bindings"})
        if fixture["inventory"].get("kind") != "LAB_NATIVE_REGRESSION_ONLY_NO_AUTHENTICATED_PROVIDER":
            raise RuntimeError("requires the retained lab component-test fixture")
        raw = dict(fixture["identity"])
        raw["route_digests"] = tuple(raw["route_digests"])
        cls.identity = _identity(DrainIdentity(**raw))
        cls.bindings = tuple(RouteBinding(**x) for x in fixture["bindings"])
        cls.inventory = fixture["inventory_digest"]
        cls.boot = _kernel_boot_id()
        if cls.boot == cls.identity.target_boot_id:
            raise RuntimeError("target has not actually restarted")

    def setUp(self):
        self.journal = TargetJournal(CASE / "journal", self.inventory)
        self.addCleanup(self.journal.close)
        states = self.journal.denied()
        self.assertEqual(len(states), 1)
        self.assertEqual(states[0].identity, self.identity)
        self.assertEqual(states[0].phases, (3,) * len(self.identity.route_digests))
        self.before = self.snapshot()
        self.assert_closed()

    def snapshot(self):
        return self.journal.sequence, self.journal.digest, self.journal.denied()

    def assert_closed(self):
        with lio_native_handles(self.boot) as root:
            self.assertEqual(tuple(root.targets), ())
            self.assertEqual(tuple(root.storage_objects), ())

    def tearDown(self):
        self.assertEqual(self.snapshot(), self.before, "observation changed durable deny debt")
        self.assert_closed()

    def test_old_completion_is_not_current_boot_completion(self):
        self.assertTrue(self.journal.completion_recorded(self.identity.operation_id,
                                                        self.identity.target_boot_id))
        self.assertFalse(self.journal.completion_recorded(self.identity.operation_id, self.boot))

    def test_old_boot_drain_cannot_reuse_native_handles(self):
        with self.assertRaises(TargetJournalError) as caught:
            deny_storage(self.journal, self.inventory, self.identity, self.bindings,
                         time.monotonic_ns() + 5_000_000_000)
        self.assertEqual(str(caught.exception), "TARGET_NATIVE_BOOT")

    def test_new_boot_attempt_cannot_infer_drain_from_absent_exports(self):
        current = replace(self.identity, attempt=self.identity.attempt + 1,
                          target_boot_id=self.boot)
        with self.assertRaises(TargetJournalError) as caught:
            deny_storage(self.journal, self.inventory, current, self.bindings,
                         time.monotonic_ns() + 5_000_000_000)
        self.assertEqual(str(caught.exception), "TARGET_ROUTE_COVERAGE")

    def test_reopening_journal_preserves_the_original_owner_and_debt(self):
        self.journal.close()
        self.journal = TargetJournal(CASE / "journal", self.inventory)
        self.addCleanup(self.journal.close)
        self.assertEqual(self.snapshot(), self.before)


if __name__ == "__main__":
    unittest.main(verbosity=2)
