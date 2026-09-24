"""Exercise the actual non-setuid signing process with a test-only seed.

Author: SqlRush <sqlrush@gmail.com>
Usage: python3 run_drain_sign_cli.py /absolute/path/pgrac-fenced-drain-sign [--no-ssl]
"""

from pathlib import Path
import struct
import subprocess
import sys
import unittest

KEY = "2152f8d19b791d24453242e15f2eab6cb7cffa7b6a5ed30097960e069881db12"


def packet():
    frame = bytearray(176 + 32)
    frame[:8] = b"PGRDRN01"
    struct.pack_into("<HHI", frame, 8, 1, 176, len(frame) + 64)
    for offset, value in ((16, 1), (40, 2), (56, 3), (72, 4), (88, 5)):
        frame[offset:offset + 16] = bytes([value]) * 16
    struct.pack_into("<Q", frame, 32, 17)
    struct.pack_into("<Q", frame, 104, 7)
    frame[112:144] = b"\x06" * 32
    struct.pack_into("<IIII", frame, 144, 4, 1, 3, 4)
    for ordinal in range(4):
        struct.pack_into("<II", frame, 176 + ordinal * 8, ordinal, 31)
    return b"\x42" * 32 + frame


class CliTests(unittest.TestCase):
    def invoke(self, data, *arguments):
        return subprocess.run([EXECUTABLE, *arguments], input=data, capture_output=True, timeout=5, check=False)

    def test_actual_process_emits_only_complete_signed_body(self):
        data = packet()
        result = self.invoke(data, KEY)
        if NO_SSL:
            self.assertEqual((result.returncode, result.stdout, result.stderr), (77, b"", b"PGRAC_DRAIN_UNSIGNED\n"))
        else:
            self.assertEqual((result.returncode, result.stderr), (0, b""))
            self.assertEqual(result.stdout[:-64], data[32:])
            self.assertEqual(len(result.stdout), 272)
            self.assertNotEqual(result.stdout[-64:], b"\0" * 64)

    def test_wrong_seed_and_incomplete_or_extra_frame_return_no_bytes(self):
        data = packet()
        bad_flags = bytearray(data)
        bad_flags[32 + 152] = 1
        for invalid in (b"", data[:-1], data + b"x", b"\x43" * 32 + data[32:], bytes(bad_flags)):
            result = self.invoke(invalid, KEY)
            self.assertEqual((result.returncode, result.stdout, result.stderr), (77, b"", b"PGRAC_DRAIN_UNSIGNED\n"))

    def test_bad_arguments_never_echo_supplied_content(self):
        for args in ((), ("sensitive-input",), (KEY, "unexpected")):
            result = self.invoke(packet(), *args)
            self.assertEqual((result.returncode, result.stdout, result.stderr), (2, b"", b"PGRAC_DRAIN_UNSIGNED\n"))


if __name__ == "__main__":
    if not 2 <= len(sys.argv) <= 3 or not Path(sys.argv[1]).is_absolute():
        raise SystemExit("expected absolute binary path [--no-ssl]")
    EXECUTABLE, NO_SSL = sys.argv[1], len(sys.argv) == 3 and sys.argv[2] == "--no-ssl"
    if len(sys.argv) == 3 and not NO_SSL:
        raise SystemExit("unknown mode")
    unittest.main(argv=[sys.argv[0]], verbosity=2)
