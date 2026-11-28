"""Forced-command readback. Only the native Lima inventory and host PID are simulated in the protocol tests.

Author: SqlRush <sqlrush@gmail.com>
"""

import importlib.util
import io
import os
import plistlib
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
import test_lima_power as native

if importlib.util.find_spec("lima_power_observer"):
    import lima_power_observer as observer
else:
    observer = None

TOKEN = "01" * 16
PREFIX = f"pgrac-lima-v1 0 {native.UUID} {TOKEN}"


class ObserverTests(unittest.TestCase):
    def setUp(self):
        self.assertIsNotNone(observer, "host has no restricted observation endpoint")
        fixture = native.PowerTests()
        self.addCleanup(fixture.doCleanups)
        fixture.setUp()
        self.f = fixture

        def inventory(argv, **kwargs):
            if argv[0] == "/usr/bin/sandbox-exec":
                self.assertEqual(argv[:3], ["/usr/bin/sandbox-exec", "-p",
                                            "(version 1)(allow default)(deny file-write*)"])
                self.assertEqual(argv[3:5], [native.power.LIMACTL, "list"])
                argv = argv[3:]
            return fixture.command(argv, **kwargs)

        native.power.subprocess.run.side_effect = inventory
        self.lock = fixture.vm / "pgrac-power.lock"
        self.lock.touch(mode=0o600)

    def request(self, action="state"):
        return f"pgrac-lima-v1 {action} 0 {native.UUID} {TOKEN} 10000"

    def run_request(self, request=None, data=b"", transition=None):
        class Output(io.StringIO):
            def flush(out):
                super().flush()
                if transition and " RUNNING " in out.getvalue().splitlines()[-1]:
                    transition()

        output = Output()
        read_fd, write_fd = os.pipe()
        with os.fdopen(read_fd, "rb") as source:
            with os.fdopen(write_fd, "wb") as sink:
                sink.write(data)
            observer.serve(self.request() if request is None else request, self.f.home,
                           source, output)
        return output.getvalue().splitlines()

    def test_state_has_only_bound_facts(self):
        self.assertEqual(self.run_request(), [PREFIX + " STOPPED"])
        self.f.running()
        self.assertEqual(self.run_request(), [PREFIX + " RUNNING 45678 123 456"])
        self.assertEqual([args[1] for args, _ in self.f.calls], ["list"] * 4)

    def test_track_exit_uses_original_target_pid_with_owner_unlocked(self):
        self.f.running()
        def stop():
            # External management must be able to own the same file at this point.
            self.assertEqual(self.f.adapter.observe(self.f.target, self.f.deadline()).state,
                             "Running")
            self.f.record.update(status="Stopped", hostAgentPID=0, driverPID=0)
            self.f.processes.clear()
        result = self.run_request(self.request("track-exit"), b"RECHECK\n", stop)
        self.assertEqual(result, [PREFIX + " RUNNING 45678 123 456",
                                  PREFIX + " EXITED 45678 123 456"])
        self.assertTrue(all(cmd[1] == "list" for cmd, _ in self.f.calls))

    def test_missing_original_or_live_reused_pid_cannot_report_exit(self):
        with self.assertRaises(native.power.LimaPowerError):
            self.run_request(self.request("track-exit"), b"RECHECK\n")
        self.f.running()
        def stopped_with_pid():
            self.f.record.update(status="Stopped", hostAgentPID=0, driverPID=0)
        with self.assertRaises(native.power.LimaPowerError):
            self.run_request(self.request("track-exit"), b"RECHECK\n", stopped_with_pid)

    def test_replaced_lock_with_same_guest_uuid_revokes_sample(self):
        self.f.running()
        def replace():
            self.lock.rename(self.f.vm / "old-lock")
            self.lock.touch(mode=0o600)
            self.f.record.update(status="Stopped", hostAgentPID=0, driverPID=0)
            self.f.processes.clear()
        with self.assertRaises(native.power.LimaPowerError):
            self.run_request(self.request("track-exit"), b"RECHECK\n", replace)

    def test_writable_instance_directory_after_first_cut_revokes_sample(self):
        self.f.running()
        def change():
            self.f.vm.chmod(0o777)
            self.f.record.update(status="Stopped", hostAgentPID=0, driverPID=0)
            self.f.processes.clear()
        with self.assertRaises(native.power.LimaPowerError):
            self.run_request(self.request("track-exit"), b"RECHECK\n", change)

    def test_nonregular_identifier_refuses_without_blocking_the_power_owner(self):
        self.f.identifier.unlink()
        os.mkfifo(self.f.identifier, 0o600)
        # Run in a child so a blocking open cannot strand the test or its lock.
        check = """import fcntl, sys, time
from pathlib import Path
from lima_power import LimaPower, LimaPowerError, LimaTarget
try:
    LimaPower(sys.argv[1], read_only=True).observe(LimaTarget(0, sys.argv[2]),
                                                 time.monotonic_ns() + 500_000_000)
except LimaPowerError as error:
    if str(error) != 'LIMA_PATH_UNTRUSTED':
        sys.exit(1)
    with (Path(sys.argv[1]) / 'pgrac-lab4-node0' / 'pgrac-power.lock').open('rb') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    sys.exit(0)
sys.exit(1)
"""
        self.f.run_patch.stop()
        result = subprocess.run([sys.executable, "-B", "-c", check, str(self.f.home), native.UUID],
                                cwd=Path(observer.__file__).parent, capture_output=True, timeout=2)
        self.assertEqual(result.returncode, 0)

    def test_query_never_creates_a_missing_lock(self):
        self.lock.unlink()
        with self.assertRaises(native.power.LimaPowerError):
            self.run_request()
        self.assertFalse(self.lock.exists())
        self.assertFalse(self.f.calls)

    def test_exact_allowlist_rejects_commands_arguments_and_noncanonical_fields(self):
        good = self.request()
        for value in ("", good + "\n", good + " 10", good.replace("state", "stop"),
                      good.replace(" 0 ", " 4 "), good.replace(" 0 ", "  0 "),
                      good.replace(" 0 ", " ../ "), good.replace(native.UUID, "0"*32),
                      good.replace(TOKEN, "0"*32), good.replace(native.UUID, native.UUID.upper()),
                      good.replace("10000", "0001"), good.replace("10000", "120001")):
            with self.subTest(value=value), self.assertRaises(native.power.LimaPowerError):
                self.run_request(value)
        self.assertFalse(self.f.calls)

    def test_disconnect_trailing_data_or_timeout_cannot_confirm(self):
        self.f.running()
        for data in (b"", b"RECHECK", b"RECHECK\n\n", b"RECHECK\nother", b"recheck\n"):
            with self.subTest(data=data), self.assertRaises(native.power.LimaPowerError):
                self.run_request(self.request("track-exit"), data)
        with patch.object(observer.select, "select", return_value=([], [], [])):
            with self.assertRaises(native.power.LimaPowerError):
                self.run_request(self.request("track-exit"), b"RECHECK\n")


class EntryTests(unittest.TestCase):
    @unittest.skipUnless(sys.platform == "darwin" and os.path.exists(native.power.LIMACTL),
                         "installed Darwin Lima required")
    def test_native_inventory_preserves_stale_hostagent_pid_file(self):
        with tempfile.TemporaryDirectory(dir="/tmp") as tmp:
            home = Path(tmp).resolve() / ".lima-pre2-single"
            vm = home / "pgrac-lab4-node0"
            vm.mkdir(parents=True)
            (vm / "pgrac-power.lock").touch(mode=0o600)
            (vm / "vz-identifier").write_bytes(plistlib.dumps({"UUID": bytes.fromhex(native.UUID)}))
            (vm / "lima.yaml").write_text("vmType: vz\narch: aarch64\nimages:\n- location: /dev/null\n  arch: aarch64\n")
            child = subprocess.Popen(["/usr/bin/true"])
            child.wait(timeout=2)
            pidfile = vm / "ha.pid"
            original = str(child.pid).encode()
            pidfile.write_bytes(original)
            out = io.StringIO()
            observer.serve(f"pgrac-lima-v1 state 0 {native.UUID} {TOKEN} 5000", home, None, out)
            self.assertEqual(out.getvalue().strip(), PREFIX + " STOPPED")
            self.assertTrue(pidfile.exists(), "the observation deleted the stale host PID file")
            self.assertEqual(pidfile.read_bytes(), original)

    def test_real_entry_refuses_extra_argv_and_never_echoes_input(self):
        self.assertIsNotNone(observer, "host has no restricted observation endpoint")
        for extra in ([], ["state"]):
            with self.subTest(extra=extra):
                result = subprocess.run([sys.executable, "-EsB", observer.__file__, *extra],
                                        env={"SSH_ORIGINAL_COMMAND": "private-invalid-input"},
                                        capture_output=True, timeout=3)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(result.stdout, b"")
                self.assertEqual(result.stderr, b"LIMA_OBSERVATION_UNPROVEN\n")


if __name__ == "__main__":
    unittest.main()
