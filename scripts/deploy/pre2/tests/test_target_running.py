"""Native RUNNING readback is distinct from isolation and DB admission.

Author: SqlRush <sqlrush@gmail.com>
Libvirt calls use isolated boundary fixtures, never guest power changes.
"""

from dataclasses import replace
from pathlib import Path
import sys
import time
import unittest

import test_target_guest as guest_tests
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from target_journal import TargetJournalError
from target_mapping import ProtectedMap
try:
    from target_guest import observe_running, GuestRunningObservation
except ImportError:
    observe_running = GuestRunningObservation = None


class RunningTests(unittest.TestCase):
    def setUp(self):
        self.assertTrue(callable(observe_running), "native rejoin running observer is missing")
        self.connection = guest_tests.Connection()
        self.connection.domain.active, self.connection.domain.status = 1, (1, 1)
        self.connection.domain.ID = lambda: 7
        self.mapping = ProtectedMap(123, 2, 7, "aa" * 32, "01" * 16, "02" * 16,
                                    "03" * 16, "pre2-kvm-gfs2-v1", "04" * 16, "05" * 16, ())

    def observe(self, deadline=None):
        return observe_running(self.connection, self.mapping,
                               time.monotonic_ns() + 5_000_000_000 if deadline is None else deadline)

    def test_exact_running_guest_is_not_drain_or_new_incarnation_proof(self):
        before = time.monotonic_ns()
        result = self.observe()
        self.assertIs(type(result), GuestRunningObservation)
        self.assertEqual(result.guest_uuid, "05" * 16)
        self.assertEqual(result.hypervisor_uuid, "04" * 16)
        self.assertEqual((result.state, result.reason, result.runtime_id), (1, 1, 7))
        self.assertGreaterEqual(result.observed_mono_ns, before)
        for field in ("io_drain_state", "incarnation", "rejoin_gate", "admission"):
            self.assertFalse(hasattr(result, field))
        self.assertEqual(self.connection.lookups, [guest_tests.GUEST] * 2)

    def test_nonrunning_or_malformed_state_is_not_ready(self):
        for state in (0, 2, 3, 4, 5, 6, 7, 8, -1, True):
            self.connection.domain.status = (state, 0)
            with self.subTest(state=state), self.assertRaises(TargetJournalError):
                self.observe()
        for status in (None, (1,), (1, 0, 3), (1, True), (1, -1)):
            self.connection.domain.status = status
            with self.subTest(status=status), self.assertRaises(TargetJournalError):
                self.observe()

    def test_absent_changed_or_malformed_runtime_id_refuses(self):
        for runtime in (-1, 0, (1 << 32) - 1, 1 << 32, True, 7.0, None):
            self.connection.domain.ID = lambda: runtime
            with self.subTest(runtime=runtime), self.assertRaises(TargetJournalError):
                self.observe()
        identities = iter((7, 8))
        self.connection.domain.ID = lambda: next(identities)
        with self.assertRaises(TargetJournalError):
            self.observe()

    def test_native_permission_and_liveness_guards_still_apply(self):
        for field, value in (("active", 0), ("active", True), ("persistent", 0),
                             ("auto", 1), ("saved", 1)):
            original = getattr(self.connection.domain, field)
            setattr(self.connection.domain, field, value)
            with self.subTest(field=field), self.assertRaises(TargetJournalError):
                self.observe()
            setattr(self.connection.domain, field, original)
        self.connection.alive = 0
        with self.assertRaises(TargetJournalError):
            self.observe()

    def test_host_guest_and_state_changes_refuse(self):
        original = self.mapping
        for field in ("hypervisor_uuid", "guest_uuid", "profile"):
            self.mapping = replace(original, **{field: "ef" * 16})
            with self.subTest(field=field), self.assertRaises(TargetJournalError):
                self.observe()
        self.mapping = original
        self.connection.domain.states = [(1, 1), (3, 1)]
        with self.assertRaises(TargetJournalError):
            self.observe()
        self.connection.capabilities = [self.connection.caps, self.connection.caps.replace("0404", "0606")]
        with self.assertRaises(TargetJournalError):
            self.observe()

    def test_deadline_and_native_exception_do_not_yield_running(self):
        for deadline in (0, True, time.monotonic_ns() - 1):
            with self.subTest(deadline=deadline), self.assertRaises(TargetJournalError):
                self.observe(deadline)
        self.connection.domain = None
        with self.assertRaises(TargetJournalError) as caught:
            self.observe()
        self.assertNotIn("sensitive", str(caught.exception))
        self.assertEqual(str(caught.exception), "TARGET_GUEST_UNPROVEN")


if __name__ == "__main__":
    unittest.main()
