"""Native API boundary tests without power/storage operations.

Author: SqlRush <sqlrush@gmail.com>
"""

from dataclasses import replace
from pathlib import Path
import sys
import time
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from target_guest import observe_off
from target_journal import TargetJournalError
from target_mapping import ProtectedMap

HOST = "04040404-0404-0404-0404-040404040404"
GUEST = "05050505-0505-0505-0505-050505050505"


class Domain:
    def __init__(self):
        self.uuid = GUEST
        self.active, self.persistent = 0, 1
        self.auto, self.saved, self.status = 0, 0, (5, 2)
        self.states = []

    def UUIDString(self):
        return self.uuid

    def isActive(self):
        return self.active

    def isPersistent(self):
        return self.persistent

    def autostart(self):
        return self.auto

    def hasManagedSaveImage(self, flags):
        assert flags == 0
        return self.saved

    def state(self, flags):
        assert flags == 0
        return self.states.pop(0) if self.states else self.status


class Connection:
    def __init__(self):
        self.domain = Domain()
        self.lookups = []
        self.uri, self.driver, self.alive, self.version = "qemu:///system", "QEMU", 1, 10000000
        self.caps = f"<capabilities><host><uuid>{HOST}</uuid></host></capabilities>"
        self.capabilities = []

    def getURI(self):
        return self.uri

    def getType(self):
        return self.driver

    def isAlive(self):
        return self.alive

    def getLibVersion(self):
        return self.version

    def getCapabilities(self):
        return self.capabilities.pop(0) if self.capabilities else self.caps

    def lookupByUUIDString(self, value):
        self.lookups.append(value)
        if self.domain is None:
            raise RuntimeError("native error with sensitive path")
        return self.domain


class GuestTests(unittest.TestCase):
    def setUp(self):
        self.connection = Connection()
        self.mapping = ProtectedMap(123, 2, 7, "aa" * 32, "01" * 16, "02" * 16,
                                    "03" * 16, "pre2-kvm-gfs2-v1", "04" * 16, "05" * 16, ())

    def observe(self, deadline=None):
        return observe_off(self.connection, self.mapping,
                           deadline if deadline is not None else time.monotonic_ns() + 5_000_000_000)

    def test_exact_persistent_off_guest_is_only_an_off_observation(self):
        before = time.monotonic_ns()
        try:
            answer = self.observe()
        except TargetJournalError as error:
            self.fail(f"exact native OFF refused: {error}")
        self.assertEqual(answer.guest_uuid, "05" * 16)
        self.assertEqual(answer.hypervisor_uuid, "04" * 16)
        self.assertEqual(answer.libvirt_version, 10000000)
        self.assertEqual((answer.state, answer.reason), (5, 2))
        self.assertGreaterEqual(answer.observed_mono_ns, before)
        self.assertFalse(hasattr(answer, "io_drain_state"))
        self.assertEqual(set(self.connection.lookups), {GUEST})
        self.assertGreaterEqual(len(self.connection.lookups), 2)

    def test_every_non_off_or_malformed_state_refuses(self):
        for state in (0, 1, 2, 3, 4, 6, 7, 8, -1, True):
            self.connection.domain.status = (state, 0)
            with self.subTest(state=state), self.assertRaises(TargetJournalError):
                self.observe()
        for status in (None, (5,), (5, 2, 3), (5, -1), (5, True)):
            self.connection.domain.status = status
            with self.subTest(status=status), self.assertRaises(TargetJournalError):
                self.observe()

    def test_no_absence_or_saved_memory_is_treated_as_power_off(self):
        for field, value in (("active", 1), ("persistent", 0), ("auto", 1), ("saved", 1),
                             ("active", False), ("persistent", True), ("auto", -1), ("saved", -1)):
            original = getattr(self.connection.domain, field)
            setattr(self.connection.domain, field, value)
            with self.subTest(field=field, value=value), self.assertRaises(TargetJournalError):
                self.observe()
            setattr(self.connection.domain, field, original)
        self.connection.domain = None
        with self.assertRaisesRegex(TargetJournalError, "^TARGET_GUEST_UNPROVEN$"):
            self.observe()

    def test_driver_endpoint_liveness_and_version_are_profile_bound(self):
        for field, value in (("uri", "qemu:///session"), ("uri", "qemu+ssh://elsewhere/system"),
                             ("driver", "TEST"), ("alive", 0), ("alive", True), ("version", 11000000)):
            old = getattr(self.connection, field)
            setattr(self.connection, field, value)
            with self.subTest(field=field), self.assertRaises(TargetJournalError):
                self.observe()
            setattr(self.connection, field, old)

    def test_hypervisor_and_guest_identity_cannot_be_relabelled(self):
        for field in ("hypervisor_uuid", "guest_uuid"):
            old = self.mapping
            self.mapping = replace(self.mapping, **{field: "ab" * 16})
            with self.subTest(field=field), self.assertRaises(TargetJournalError):
                self.observe()
            self.mapping = old
        self.connection.domain.uuid = "06060606-0606-0606-0606-060606060606"
        with self.assertRaises(TargetJournalError):
            self.observe()

    def test_final_observation_rejects_restarted_guest_and_host_drift(self):
        self.connection.domain.states = [(5, 2), (1, 1)]
        with self.assertRaises(TargetJournalError):
            self.observe()
        self.connection.capabilities = [self.connection.caps, self.connection.caps.replace("0404", "0606")]
        with self.assertRaises(TargetJournalError):
            self.observe()

    def test_capabilities_xml_must_have_one_exact_host_uuid(self):
        original = self.connection.caps
        for caps in ("<", "<!DOCTYPE capabilities>" + original, " " * (1048576 + 1),
                     original.replace("</host>", f"<uuid>{HOST}</uuid></host>"),
                     "<capabilities/>", original.replace("capabilities", "another")):
            self.connection.caps = caps
            with self.subTest(size=len(caps)), self.assertRaises(TargetJournalError):
                self.observe()

    def test_expired_envelope_or_invalid_mapping_never_yields_off(self):
        for deadline in (0, True, time.monotonic_ns() - 1):
            with self.subTest(deadline=deadline), self.assertRaises(TargetJournalError):
                self.observe(deadline)
        self.mapping = replace(self.mapping, profile="not-certified")
        with self.assertRaises(TargetJournalError):
            self.observe()


if __name__ == "__main__":
    unittest.main()
