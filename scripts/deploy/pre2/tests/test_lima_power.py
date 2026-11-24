"""Host power boundary tests; no test starts or stops a virtual machine.

Author: SqlRush <sqlrush@gmail.com>
"""

from dataclasses import replace
import importlib
import json
import os
from pathlib import Path
import plistlib
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
power = importlib.import_module("lima_power") if importlib.util.find_spec("lima_power") else None
UUID = "8eb8c80f9bc842c3a78bc2379a745fb8"


class PowerTests(unittest.TestCase):
    def setUp(self):
        self.assertIsNotNone(power, "the Lima host power adapter is not implemented")
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.home = Path(self.tmp.name).resolve() / ".lima-pre2-single"
        self.home.mkdir(mode=0o700)
        self.vm = self.home / "pgrac-lab4-node0"
        self.vm.mkdir(mode=0o700)
        self.identifier = self.vm / "vz-identifier"
        self.write_uuid()
        self.target = power.LimaTarget(0, UUID)
        self.adapter = power.LimaPower(self.home)
        self.record = dict(name=self.vm.name, dir=str(self.vm), status="Stopped",
                           vmType="vz", arch="aarch64")
        self.calls = []
        self.after_stop = None
        self.processes = {}
        self.run_patch = patch.object(power.subprocess, "run", side_effect=self.command)
        self.proc_patch = patch.object(power, "process_identity", side_effect=self.processes.get)
        self.run_patch.start()
        self.proc_patch.start()
        self.addCleanup(self.run_patch.stop)
        self.addCleanup(self.proc_patch.stop)

    def write_uuid(self, value=UUID):
        self.identifier.write_bytes(plistlib.dumps({"UUID": bytes.fromhex(value)},
                                                  fmt=plistlib.FMT_BINARY))

    def deadline(self):
        return time.monotonic_ns() + 10_000_000_000

    def command(self, argv, **kwargs):
        self.calls.append((argv, kwargs))
        self.assertEqual(kwargs["env"]["LIMA_HOME"], str(self.home))
        self.assertFalse(kwargs.get("shell", False))
        self.assertGreater(kwargs["timeout"], 0)
        if argv[1:] == ["list", self.vm.name, "--json"]:
            return subprocess.CompletedProcess(argv, 0, json.dumps(self.record), "")
        self.assertEqual(argv[1:], ["stop", "--force", self.vm.name])
        if self.after_stop:
            self.after_stop()
        else:
            self.record.update(status="Stopped", hostAgentPID=0, driverPID=0)
            self.processes.clear()
        return subprocess.CompletedProcess(argv, 0, "", "")

    def running(self):
        pid = 45678
        self.record.update(status="Running", hostAgentPID=pid, driverPID=pid)
        cmd = (f"/opt/homebrew/bin/limactl hostagent --pidfile {self.vm}/ha.pid "
               f"--socket {self.vm}/ha.sock --guestagent "
               f"/opt/homebrew/share/lima/lima-guestagent.Linux-aarch64.gz {self.vm.name}")
        self.processes[pid] = power.ProcessIdentity(pid, os.getuid(), 123, 456,
                                                   os.path.realpath(power.LIMACTL), cmd)
        return self.processes[pid]

    def test_native_uuid_and_stopped_state_are_only_power_observations(self):
        answer = self.adapter.observe(self.target, self.deadline())
        self.assertEqual((answer.node_id, answer.guest_uuid, answer.state), (0, UUID, "Stopped"))
        self.assertIsNone(answer.host_process)
        self.assertFalse(hasattr(answer, "io_drain_state"))
        self.assertFalse(hasattr(answer, "write_excluded"))
        self.assertTrue(all(call[0][1] == "list" for call in self.calls))

    def test_running_observation_binds_native_host_process_and_birth(self):
        original = self.running()
        self.assertEqual(self.adapter.observe(self.target, self.deadline()).host_process, original)

    def test_force_off_requires_original_process_exit_and_native_stopped_readback(self):
        original = self.running()
        answer = self.adapter.force_off(self.target, self.deadline())
        self.assertEqual(answer.before.host_process, original)
        self.assertEqual(answer.after.state, "Stopped")
        self.assertEqual(sum(call[0][1] == "stop" for call in self.calls), 1)
        self.assertFalse(hasattr(answer, "write_excluded"))

    def test_already_stopped_cannot_supply_a_missing_original_process(self):
        with self.assertRaises(power.LimaPowerError):
            self.adapter.force_off(self.target, self.deadline())
        self.assertFalse(any(call[0][1] == "stop" for call in self.calls))

    def test_only_four_integer_database_node_ids_and_nonzero_native_uuids(self):
        for node in (True, -1, 4, "0", "pgrac-e-perf-20261018", "pgrac-lab4-storage"):
            with self.subTest(node=node), self.assertRaises(power.LimaPowerError):
                self.adapter.observe(power.LimaTarget(node, UUID), self.deadline())
        for value in (None, "00" * 16, UUID.upper(), UUID + "0", "../vz-identifier"):
            with self.subTest(uuid=value), self.assertRaises(power.LimaPowerError):
                self.adapter.observe(power.LimaTarget(0, value), self.deadline())
        self.assertEqual(self.calls, [])

    def test_uuid_mismatch_and_malformed_plist_refuse_before_power_command(self):
        for data in (plistlib.dumps({"UUID": bytes.fromhex("ab" * 16)}), b"not a plist",
                     plistlib.dumps({"UUID": UUID}), plistlib.dumps({"UUID": b"\0" * 16}),
                     b"x" * 4097):
            self.identifier.write_bytes(data)
            with self.subTest(size=len(data)), self.assertRaises(power.LimaPowerError):
                self.adapter.force_off(self.target, self.deadline())
        self.assertEqual(self.calls, [])

    def test_symlink_identifier_or_instance_directory_is_refused(self):
        saved = self.vm / "saved"
        self.identifier.rename(saved)
        self.identifier.symlink_to(saved)
        with self.assertRaises(power.LimaPowerError):
            self.adapter.observe(self.target, self.deadline())
        self.identifier.unlink()
        saved.rename(self.identifier)
        elsewhere = self.home / "elsewhere"
        self.vm.rename(elsewhere)
        self.vm.symlink_to(elsewhere, target_is_directory=True)
        with self.assertRaises(power.LimaPowerError):
            self.adapter.observe(self.target, self.deadline())
        self.assertEqual(self.calls, [])

    def test_writable_identity_or_instance_is_not_trusted(self):
        for path in (self.identifier, self.vm, self.home):
            old = path.stat().st_mode & 0o777
            path.chmod(old | 0o020)
            with self.subTest(path=path.name), self.assertRaises(power.LimaPowerError):
                self.adapter.observe(self.target, self.deadline())
            path.chmod(old)

    def test_inventory_is_exactly_one_same_vz_arm_instance(self):
        for key, value in (("name", "pgrac-e-perf-20261018"), ("dir", "/wrong"),
                           ("vmType", "qemu"), ("arch", "x86_64"), ("status", "Broken"),
                           ("hostAgentPID", True), ("driverPID", 99)):
            original = dict(self.record)
            self.record[key] = value
            with self.subTest(key=key), self.assertRaises(power.LimaPowerError):
                self.adapter.observe(self.target, self.deadline())
            self.record = original

    def test_running_requires_two_identical_positive_host_pids(self):
        self.running()
        for key, value in (("hostAgentPID", None), ("hostAgentPID", True),
                           ("hostAgentPID", -1), ("driverPID", 99), ("driverPID", 0)):
            old = self.record[key]
            self.record[key] = value
            with self.subTest(key=key, value=value), self.assertRaises(power.LimaPowerError):
                self.adapter.force_off(self.target, self.deadline())
            self.record[key] = old
        self.assertFalse(any(call[0][1] == "stop" for call in self.calls))

    def test_wrong_process_owner_binary_or_exact_hostagent_target_refuses(self):
        original = self.running()
        for changes in (dict(uid=os.getuid() + 1), dict(executable="/bin/sh"),
                        dict(command=original.command.replace(self.vm.name, "pgrac-lab4-storage")),
                        dict(command=original.command.replace("--pidfile", "--other")),
                        dict(command=original.command + " pgrac-lab4-node1")):
            self.processes[original.pid] = replace(original, **changes)
            with self.subTest(changes=changes), self.assertRaises(power.LimaPowerError):
                self.adapter.force_off(self.target, self.deadline())
        self.assertFalse(any(call[0][1] == "stop" for call in self.calls))

    def test_process_birth_change_between_reads_refuses_before_stop(self):
        original = self.running()
        with patch.object(power, "process_identity", side_effect=[original, replace(original, start_usec=999)]):
            with self.assertRaises(power.LimaPowerError):
                self.adapter.force_off(self.target, self.deadline())
        self.assertFalse(any(call[0][1] == "stop" for call in self.calls))

    def test_uuid_replaced_during_command_does_not_yield_power_evidence(self):
        self.running()
        def change():
            self.record.update(status="Stopped", hostAgentPID=0, driverPID=0)
            self.processes.clear()
            self.write_uuid("ab" * 16)
        self.after_stop = change
        with self.assertRaises(power.LimaPowerError):
            self.adapter.force_off(self.target, self.deadline())

    def test_stopped_metadata_does_not_override_a_still_live_original_process(self):
        self.running()
        self.after_stop = lambda: self.record.update(status="Stopped", hostAgentPID=0, driverPID=0)
        with self.assertRaises(power.LimaPowerError):
            self.adapter.force_off(self.target, self.deadline())

    def test_restarted_instance_does_not_yield_off(self):
        self.running()
        self.after_stop = lambda: None
        with self.assertRaises(power.LimaPowerError):
            self.adapter.force_off(self.target, self.deadline())

    def test_reused_original_pid_is_not_treated_as_an_exit_proof(self):
        original = self.running()
        def reuse():
            self.record.update(status="Stopped", hostAgentPID=0, driverPID=0)
            self.processes[original.pid] = replace(original, start_usec=999)
        self.after_stop = reuse
        with self.assertRaises(power.LimaPowerError):
            self.adapter.force_off(self.target, self.deadline())

    def test_failed_stop_never_uses_a_stopped_readback_as_success(self):
        self.running()
        original = self.command
        def failed(argv, **kwargs):
            result = original(argv, **kwargs)
            if argv[1] == "stop":
                result.returncode = 1
            return result
        with patch.object(power.subprocess, "run", side_effect=failed):
            with self.assertRaises(power.LimaPowerError):
                self.adapter.force_off(self.target, self.deadline())
        self.assertEqual(self.record["status"], "Stopped")

    def test_instance_directory_replacement_after_stop_is_refused(self):
        self.running()
        def change():
            self.vm.rename(self.home / "retired-node0")
            self.vm.mkdir(mode=0o700)
            self.write_uuid()
            self.record.update(status="Stopped", hostAgentPID=0, driverPID=0)
            self.processes.clear()
        self.after_stop = change
        with self.assertRaises(power.LimaPowerError):
            self.adapter.force_off(self.target, self.deadline())

    def test_symlink_lock_is_refused_before_inventory_or_stop(self):
        destination = self.vm / "another-lock"
        destination.touch(mode=0o600)
        (self.vm / "pgrac-power.lock").symlink_to(destination)
        with self.assertRaises(power.LimaPowerError):
            self.adapter.force_off(self.target, self.deadline())
        self.assertEqual(self.calls, [])

    def test_home_must_be_explicit_absolute_and_canonical(self):
        link = self.home.parent / "alias"
        link.symlink_to(self.home, target_is_directory=True)
        for value in (Path(".lima-pre2-single"), self.home.parent / "code-sandbox", link):
            with self.subTest(value=value), self.assertRaises(power.LimaPowerError):
                power.LimaPower(value)

    def test_deadline_expiring_during_stop_does_not_yield_off(self):
        self.running()
        def change():
            self.record.update(status="Stopped", hostAgentPID=0, driverPID=0)
            self.processes.clear()
            self.expired = patch.object(power.time, "monotonic_ns", return_value=1 << 63)
            self.expired.start()
            self.addCleanup(self.expired.stop)
        self.after_stop = change
        with self.assertRaises(power.LimaPowerError):
            self.adapter.force_off(self.target, self.deadline())

    def test_cli_failure_timeout_and_invalid_inventory_do_not_yield_off(self):
        self.running()
        for result in (subprocess.CompletedProcess([], 1, "", "private error"),
                       subprocess.CompletedProcess([], 0, "{}\n{}", ""),
                       subprocess.CompletedProcess([], 0, "[]", "")):
            with patch.object(power.subprocess, "run", return_value=result):
                with self.subTest(result=result), self.assertRaises(power.LimaPowerError):
                    self.adapter.force_off(self.target, self.deadline())
        with patch.object(power.subprocess, "run", side_effect=subprocess.TimeoutExpired([], 1)):
            with self.assertRaises(power.LimaPowerError):
                self.adapter.force_off(self.target, self.deadline())

    def test_expired_or_invalid_deadline_never_dispatches(self):
        for value in (True, 0, time.monotonic_ns() - 1):
            with self.subTest(value=value), self.assertRaises(power.LimaPowerError):
                self.adapter.force_off(self.target, value)
        self.assertEqual(self.calls, [])

    def test_independent_owner_cannot_enter_a_locked_operation(self):
        import fcntl
        with (self.vm / "pgrac-power.lock").open("w") as lock:
            os.chmod(lock.name, 0o600)
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with self.assertRaises(power.LimaPowerError):
                self.adapter.observe(self.target, self.deadline())
        self.assertEqual(self.calls, [])


class NativeProcessTests(unittest.TestCase):
    @unittest.skipUnless(sys.platform == "darwin", "native Darwin process API")
    def test_own_real_child_has_precise_birth_and_becomes_absent_after_wait(self):
        self.assertIsNotNone(power, "the Lima host power adapter is not implemented")
        child = subprocess.Popen([sys.executable, "-c",
                                  "import sys; print('ready', flush=True); sys.stdin.buffer.read()"],
                                 stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        try:
            self.assertEqual(child.stdout.readline(), b"ready\n")
            identity = power.process_identity(child.pid)
            self.assertEqual(identity.pid, child.pid)
            self.assertEqual(identity.uid, os.getuid())
            self.assertGreater(identity.start_sec, 0)
            self.assertTrue(0 <= identity.start_usec < 1_000_000)
            self.assertEqual(power.process_identity(child.pid), identity)
        finally:
            child.communicate(timeout=5)
        self.assertIsNone(power.process_identity(child.pid))


if __name__ == "__main__":
    unittest.main()
