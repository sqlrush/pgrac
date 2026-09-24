"""Real Linux verifier-to-target-owner integration, no storage mutation.

Author: SqlRush <sqlrush@gmail.com>
Usage: python3 run_target_mapping.py /absolute/path/pgrac-fenced-map-verify
"""

from dataclasses import replace
import ast
import hashlib
import os
from pathlib import Path
import re
import shutil
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import target_mapping
from target_mapping import MapExpected, VerifierPin, load_verified_map
from target_journal import TargetJournalError


class TargetMappingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        repo = Path(__file__).resolve().parents[4]
        text = (repo / "src/test/cluster_unit/data/pgrac_fence_map_v2_fixture.h").read_text()
        literals = text.split("static const char fenced_config_v2[]", 1)[1]
        config = "".join(ast.literal_eval(value) for value in re.findall(r'"(?:[^"\\]|\\.)*"', literals))
        cls.packet = bytes.fromhex(dict(line.split("=", 1) for line in config.splitlines())["node.2.adapter_data"])

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="pgrac-map-owner-")
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name).resolve()
        self.binary = self.directory / "verifier"
        shutil.copyfile(EXECUTABLE, self.binary)
        self.binary.chmod(0o700)
        self.verifier = VerifierPin(str(self.binary), hashlib.sha256(self.binary.read_bytes()).hexdigest(),
                                    os.geteuid())
        self.expected = MapExpected(123456789, 2, 7,
                                   "84dc62a01725f0361b3fa2de5615632a8fb9a95d07e564fbb3a661d51fd06828",
                                   bytes.fromhex("c050c5637a44fa8629fff3cccce2300cb362a63d99d95fc54145266f4332445a"))

    def load(self):
        return load_verified_map(self.verifier, self.expected, self.packet,
                                 time.monotonic_ns() + 5_000_000_000)

    def load_ok(self):
        try:
            return self.load()
        except TargetJournalError as error:
            self.fail(f"authentic selected map refused: {error}")

    def test_real_verifier_yields_complete_immutable_metadata(self):
        result = self.load_ok()
        self.assertEqual((result.system_identifier, result.node_id, result.mapping_generation),
                         (123456789, 2, 7))
        self.assertEqual(result.protected_set_digest, self.expected.protected_set_digest)
        self.assertEqual(result.database_uuid, "01" * 16)
        self.assertEqual(result.storage_uuid, "02" * 16)
        self.assertEqual(result.authority_uuid, "03" * 16)
        self.assertEqual(result.hypervisor_uuid, "04" * 16)
        self.assertEqual(result.guest_uuid, "05" * 16)
        self.assertEqual(result.profile, "pre2-kvm-gfs2-v1")
        self.assertEqual(len(result.routes), 4)
        self.assertEqual(tuple(route.ordinal for route in result.routes), (0, 1, 2, 3))
        self.assertEqual(tuple(route.roles for route in result.routes), (1, 2, 4, 8))
        self.assertEqual(tuple(route.tpg for route in result.routes), (1, 2, 3, 4))
        self.assertEqual(tuple(route.media_kind for route in result.routes), (1, 1, 1, 2))
        self.assertEqual(tuple(route.backstore_uuid for route in result.routes),
                         ("10" * 16, "11" * 16, "12" * 16, "13" * 16))
        self.assertEqual(tuple(route.filesystem_uuid for route in result.routes),
                         ("20" * 16, "21" * 16, "22" * 16, "00" * 16))
        for route in result.routes:
            self.assertEqual(route.initiator, "iqn.2026-09.test:guest0")
            self.assertEqual(route.credential_ref, "cred-1")
            self.assertEqual(route.target, "iqn.2026-09.test:target")
            self.assertEqual(route.endpoint, "10.0.0.10:3260")
            self.assertEqual(route.lun_wwid, "wwid-1")
            self.assertEqual(route.lun_serial, "serial-1")
            self.assertRegex(route.digest, r"^[0-9a-f]{64}$")
            # Golden fixture: 174 payload-header bytes, four 176-byte routes.
            # Counted independently of the production field reader.
            start = 32 + 174 + route.ordinal * 176
            golden = hashlib.sha256(b"PGRAC-TARGET-ROUTE-V1\0" + route.ordinal.to_bytes(4, "little")
                                     + self.packet[start:start + 176]).hexdigest()
            self.assertEqual(route.digest, golden)
        self.assertEqual(len({route.digest for route in result.routes}), 4)
        self.assertEqual(result, self.load_ok())

    def test_tampered_input_or_independent_expectation_is_unproven(self):
        for field, value in (("system_identifier", 9), ("node_id", 1), ("mapping_generation", 8),
                             ("protected_set_digest", "01" * 32), ("public_key", b"\x01" * 32)):
            original = self.expected
            self.expected = replace(original, **{field: value})
            with self.subTest(field=field), self.assertRaises(TargetJournalError):
                self.load()
            self.expected = original
        self.packet = self.packet[:-1] + bytes([self.packet[-1] ^ 1])
        with self.assertRaises(TargetJournalError):
            self.load()

    def test_wrong_fingerprint_mode_owner_or_symlink_cannot_execute(self):
        self.verifier = replace(self.verifier, sha256="ab" * 32)
        with self.assertRaises(TargetJournalError):
            self.load()
        self.verifier = replace(self.verifier, sha256=hashlib.sha256(self.binary.read_bytes()).hexdigest())
        self.binary.chmod(0o722)
        with self.assertRaises(TargetJournalError):
            self.load()
        self.binary.chmod(0o700)
        original = self.verifier
        self.verifier = replace(original, owner_uid=os.geteuid() + 1)
        with self.assertRaises(TargetJournalError):
            self.load()
        self.verifier = original
        self.binary.rename(self.directory / "saved")
        self.binary.symlink_to(self.directory / "saved")
        with self.assertRaises(TargetJournalError):
            self.load()

    def test_expired_envelope_and_bad_arguments_never_yield_map(self):
        with self.assertRaises(TargetJournalError):
            load_verified_map(self.verifier, self.expected, self.packet, time.monotonic_ns() - 1)
        for packet in (b"", self.packet + b"x" * 65537, bytearray(self.packet)):
            with self.subTest(size=len(packet)), self.assertRaises(TargetJournalError):
                load_verified_map(self.verifier, self.expected, packet, time.monotonic_ns() + 5_000_000_000)
        for field, value in (("system_identifier", True), ("node_id", 128), ("mapping_generation", 0),
                             ("protected_set_digest", ""), ("public_key", b"")):
            original = self.expected
            self.expected = replace(original, **{field: value})
            with self.subTest(field=field), self.assertRaises(TargetJournalError):
                self.load()
            self.expected = original

    def test_path_replacement_after_pin_cannot_select_replacement_program(self):
        import subprocess
        original_run = subprocess.run
        def replace_path(*args, **kwargs):
            self.binary.rename(self.directory / "retained-verifier")
            self.binary.write_text("#!/bin/sh\nexit 99\n")
            self.binary.chmod(0o700)
            return original_run(*args, **kwargs)
        with patch("target_mapping.subprocess.run", side_effect=replace_path):
            self.assertEqual(self.load_ok().guest_uuid, "05" * 16)

    def test_repeated_success_and_failure_do_not_leak_binary_descriptors(self):
        before = set(os.listdir("/proc/self/fd"))
        self.load_ok()
        self.expected = replace(self.expected, node_id=1)
        for _ in range(3):
            with self.assertRaises(TargetJournalError):
                self.load()
        self.assertEqual(set(os.listdir("/proc/self/fd")), before)

    def test_reading_binary_can_change_access_time_without_identity_drift(self):
        info = self.binary.stat()
        os.utime(self.binary, ns=(1, info.st_mtime_ns))
        self.assertEqual(self.load_ok().node_id, 2)

    def test_native_timeout_does_not_echo_request_or_leak_pin(self):
        before = set(os.listdir("/proc/self/fd"))
        with patch("target_mapping.subprocess.run",
                   side_effect=target_mapping.subprocess.TimeoutExpired("secret=never-echo", 1)):
            with self.assertRaises(TargetJournalError) as error:
                self.load()
        self.assertEqual(str(error.exception), "TARGET_MAP_UNPROVEN")
        self.assertTrue(error.exception.__suppress_context__)
        self.assertEqual(set(os.listdir("/proc/self/fd")), before)


if __name__ == "__main__":
    if len(sys.argv) != 2 or not Path(sys.argv[1]).is_absolute():
        raise SystemExit("expected absolute verifier path")
    EXECUTABLE = sys.argv[1]
    unittest.main(argv=[sys.argv[0]], verbosity=2)
