"""Linux real-executable dumpability at first stdin read, with a test-only key.

Author: SqlRush <sqlrush@gmail.com>
Usage: python3 run_drain_sign_dump_probe.py /absolute/signer /absolute/probe.so
"""

import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from run_drain_sign_cli import KEY, packet


class DumpProbeTests(unittest.TestCase):
    def invoke(self, fail=False, descriptor=False):
        reader, writer = os.pipe()
        try:
            env = dict(os.environ, LD_PRELOAD=PROBE, PGRAC_TEST_DUMP_FD=str(writer))
            env.pop("PGRAC_TEST_DUMP_FAIL", None)
            env.pop("PGRAC_TEST_SEED_FD", None)
            if fail:
                env["PGRAC_TEST_DUMP_FAIL"] = "1"
            with tempfile.NamedTemporaryFile(prefix="pgrac-dump-key-") as seed:
                seed.write(b"\x42" * 32)
                seed.flush()
                args, data, inherited = [EXECUTABLE, KEY], packet(), (writer,)
                if descriptor:
                    args.append(str(seed.fileno()))
                    env["PGRAC_TEST_SEED_FD"] = str(seed.fileno())
                    data, inherited = data[32:], (writer, seed.fileno())
                result = subprocess.run(args, input=data, capture_output=True,
                                        env=env, pass_fds=inherited, timeout=5, check=False)
            os.close(writer)
            writer = None
            return result, os.read(reader, 32)
        finally:
            os.close(reader)
            if writer is not None:
                os.close(writer)

    def test_private_input_is_not_read_while_process_is_dumpable(self):
        for descriptor in (False, True):
            with self.subTest(descriptor=descriptor):
                result, marker = self.invoke(descriptor=descriptor)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(marker, b"0")

    def test_failed_nondumpable_transition_refuses_before_reading_secret(self):
        for descriptor in (False, True):
            with self.subTest(descriptor=descriptor):
                result, marker = self.invoke(fail=True, descriptor=descriptor)
                self.assertEqual((result.returncode, result.stdout, result.stderr), (77, b"", b"PGRAC_DRAIN_UNSIGNED\n"))
                self.assertEqual(marker, b"")


if __name__ == "__main__":
    if len(sys.argv) != 3 or any(not Path(arg).is_absolute() for arg in sys.argv[1:]):
        raise SystemExit("expected absolute signer and probe paths")
    EXECUTABLE, PROBE = sys.argv[1:]
    unittest.main(argv=[sys.argv[0]], verbosity=2)
