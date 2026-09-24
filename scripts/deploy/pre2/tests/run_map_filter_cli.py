"""Run the installed-shape C map filter, not a Python authentication substitute.

Author: SqlRush <sqlrush@gmail.com>
Usage: python3 run_map_filter_cli.py /absolute/path/pgrac-fenced-map-verify [--no-ssl]
"""

import ast
from pathlib import Path
import re
import subprocess
import sys
import unittest


class MapFilterCliTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        repo = Path(__file__).resolve().parents[4]
        fixture = (repo / "src/test/cluster_unit/data/pgrac_fence_map_v2_fixture.h").read_text()
        literals = fixture.split("static const char fenced_config_v2[]", 1)[1]
        config = "".join(ast.literal_eval(value) for value in re.findall(r'"(?:[^"\\]|\\.)*"', literals))
        cls.packet = bytes.fromhex(dict(line.split("=", 1) for line in config.splitlines())["node.2.adapter_data"])
        cls.arguments = ["123456789", "2", "7",
                         "84dc62a01725f0361b3fa2de5615632a8fb9a95d07e564fbb3a661d51fd06828",
                         "c050c5637a44fa8629fff3cccce2300cb362a63d99d95fc54145266f4332445a"]

    def invoke(self, packet, arguments=None):
        return subprocess.run([EXECUTABLE] + (self.arguments if arguments is None else arguments),
                              input=packet, capture_output=True, timeout=10, check=False)

    def test_valid_payload_or_explicit_no_crypto_refusal(self):
        result = self.invoke(self.packet)
        self.assertEqual(result.returncode, 77 if NO_SSL else 0)
        self.assertEqual(result.stdout, b"" if NO_SSL else self.packet[32:-64])
        self.assertEqual(result.stderr, b"PGRAC_MAP_UNVERIFIED\n" if NO_SSL else b"")

    def test_signature_and_binding_rejection_never_output_payload(self):
        result = self.invoke(self.packet[:-1] + bytes([self.packet[-1] ^ 1]))
        self.assertEqual(result.returncode, 77)
        self.assertEqual(result.stdout, b"")
        args = self.arguments.copy()
        args[1] = "1"
        result = self.invoke(self.packet, args)
        self.assertEqual(result.returncode, 77)
        self.assertEqual(result.stdout, b"")

    def test_bad_args_and_oversized_input_do_not_echo_values(self):
        result = self.invoke(self.packet, ["secret-must-not-be-echoed"])
        self.assertEqual(result.returncode, 2)
        self.assertEqual(result.stdout, b"")
        self.assertEqual(result.stderr, b"PGRAC_MAP_UNVERIFIED\n")
        result = self.invoke(self.packet + b"x" * 65537)
        self.assertEqual(result.returncode, 77)
        self.assertEqual(result.stdout, b"")


if __name__ == "__main__":
    if len(sys.argv) not in (2, 3) or not Path(sys.argv[1]).is_absolute():
        raise SystemExit("expected absolute verifier path [--no-ssl]")
    EXECUTABLE = sys.argv[1]
    NO_SSL = len(sys.argv) == 3 and sys.argv[2] == "--no-ssl"
    if len(sys.argv) == 3 and not NO_SSL:
        raise SystemExit("unknown test mode")
    unittest.main(argv=[sys.argv[0]], verbosity=2)
