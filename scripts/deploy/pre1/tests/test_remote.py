"""Read-only remote lifecycle tests; synthetic observations are not qualification.

Author: SqlRush <sqlrush@gmail.com>
"""

import contextlib
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import PreflightError, document_sha
from test_profile import fixture


class GuestObservationTests(unittest.TestCase):
    def setUp(self):
        # Missing implementation is an explicit behavior failure, not an import error.
        self.assertIsNotNone(importlib.util.find_spec("guest_status"),
                             "read-only guest lifecycle collector is missing")
        import guest_status
        self.guest = guest_status
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()

    def test_control_state_requires_unambiguous_native_output(self):
        output = ("pg_control version number:            1300\n"
                  "Database system identifier:           7654321000123456789\n"
                  "Database cluster state:               shut down\n")
        self.assertEqual(self.guest.parse_control(output),
                         {"system_identifier": "7654321000123456789", "state": "shut down"})
        for bad in (output + "Database cluster state: in production\n",
                    output.replace("shut down", "unknown"),
                    output.replace("7654321000123456789", "0"),
                    output.replace("Database cluster state:", "Wrong field:")):
            with self.subTest(output=bad):
                with self.assertRaises(self.guest.ObservationError):
                    self.guest.parse_control(bad)

    def test_unsafe_pgdata_symlink_is_not_followed(self):
        target = self.root / "retained-data"
        target.mkdir()
        alias = self.root / "pgdata"
        alias.symlink_to(target, target_is_directory=True)
        with self.assertRaises(self.guest.ObservationError) as error:
            self.guest.checked_directory(str(alias), allow_absent=True)
        self.assertEqual(error.exception.reason, "PATH_NOT_CANONICAL")
        self.assertEqual(list(target.iterdir()), [])

    def test_proc_stat_handles_spaces_and_parentheses_in_comm(self):
        # Fields 4..21 followed by literal field 22 (starttime).
        raw = "417 (postgres: app ) worker) S " + " ".join(["1"] * 18) + " 987654 0\n"
        self.assertEqual(self.guest.parse_proc_stat(raw, 417),
                         {"pid": 417, "ppid": 1, "starttime": 987654, "state": "S"})
        with self.assertRaises(self.guest.ObservationError):
            self.guest.parse_proc_stat(raw, 418)

    def test_process_reuse_during_read_is_not_a_stopped_process(self):
        first = {"pid": 417, "ppid": 1, "starttime": 987654, "state": "S"}
        second = {**first, "starttime": 987655}
        with self.assertRaises(self.guest.ObservationError) as error:
            self.guest.require_same_process(first, second)
        self.assertEqual(error.exception.reason, "PROCESS_IDENTITY_CHANGED")

    def test_wrong_boot_is_rejected_before_reading_database(self):
        node = fixture()["nodes"][0]
        request = {"action": "status", "node": {k: node[k] for k in
                   ("node_id", "vm_uuid", "machine_id", "boot_id", "pgdata",
                    "install_root", "uid", "gid")}, "binary_sha256": "a" * 64}
        observed = {k: node[k] for k in ("vm_uuid", "machine_id", "boot_id")}
        observed["boot_id"] = "00000000-0000-0000-0000-000000000000"
        with patch.object(self.guest, "read_identity", return_value=observed):
            with self.assertRaises(self.guest.ObservationError) as error:
                self.guest.collect(request)
        self.assertEqual(error.exception.reason, "GUEST_IDENTITY_MISMATCH")

    def test_arbitrary_action_and_unknown_field_are_rejected(self):
        for request in ({"action": "sh -c secret"},
                        {"action": "status", "node": {}, "binary_sha256": "a" * 64,
                         "command": ["rm", "not-authorized"]}):
            with self.subTest(request=request):
                with self.assertRaises(self.guest.ObservationError) as error:
                    self.guest.validate_request(request)
                self.assertEqual(error.exception.reason, "REQUEST_INVALID")

    def test_regular_file_hash_and_control_error_preserve_real_rc(self):
        binary = self.root / "postgres"
        binary.write_bytes(b"synthetic binary bytes")
        self.assertEqual(self.guest.file_hash(binary),
                         hashlib.sha256(b"synthetic binary bytes").hexdigest())
        result = self.guest.control_result(subprocess.CompletedProcess(
            [], 1, "", "could not read control file"))
        self.assertEqual(result["rc"], 1)
        self.assertEqual(result["stderr"], "could not read control file")
        self.assertIsNone(result["parsed"])

    def test_census_failure_cannot_be_reported_as_empty(self):
        with patch.object(self.guest.os, "scandir", side_effect=PermissionError):
            with self.assertRaises(self.guest.ObservationError) as error:
                self.guest.scan_processes(os.getuid())
        self.assertEqual(error.exception.reason, "PROCESS_CENSUS_UNAVAILABLE")

    def local_request(self):
        node = fixture()["nodes"][0]
        install = self.root / "install"
        (install / "bin").mkdir(parents=True)
        (install / "bin/postgres").write_bytes(b"synthetic postgres")
        pgdata = self.root / "pgdata"
        pgdata.mkdir(mode=0o700)
        request = {"action": "status", "node": {k: node[k] for k in
                   ("node_id", "vm_uuid", "machine_id", "boot_id", "pgdata",
                    "install_root", "uid", "gid")},
                   "binary_sha256": hashlib.sha256(b"synthetic postgres").hexdigest()}
        request["node"].update({"pgdata": str(pgdata), "install_root": str(install),
                                "uid": os.getuid(), "gid": os.getgid()})
        return request, pgdata

    def local_collect(self, request):
        identity = {k: request["node"][k] for k in ("vm_uuid", "machine_id", "boot_id")}
        with patch.object(self.guest, "read_identity", return_value=identity), \
                patch.object(self.guest, "scan_processes", return_value=[]):
            return self.guest.collect(request)

    def test_missing_or_partial_data_is_observed_not_a_clean_database(self):
        request, pgdata = self.local_request()
        self.assertEqual(self.local_collect(request)["pgdata_state"], "EMPTY")
        (pgdata / "partial-init").write_text("retained partial setup")
        partial = self.local_collect(request)
        self.assertEqual(partial["pgdata_state"], "PARTIAL")
        self.assertIsNone(partial["control"])
        request["node"]["pgdata"] = str(self.root / "absent")
        self.assertEqual(self.local_collect(request)["pgdata_state"], "ABSENT")

    def test_wrong_binary_does_not_claim_node_observed(self):
        request, _ = self.local_request()
        request["binary_sha256"] = "b" * 64
        with self.assertRaises(self.guest.ObservationError) as error:
            self.local_collect(request)
        self.assertEqual(error.exception.reason, "DATABASE_BINARY_MISMATCH")

    def test_control_symlink_is_rejected_before_native_tool_reads_it(self):
        request, pgdata = self.local_request()
        (pgdata / "PG_VERSION").write_text("16\n")
        (pgdata / "global").mkdir()
        retained = self.root / "other-control"
        retained.write_bytes(b"do not follow this file")
        (pgdata / "global/pg_control").symlink_to(retained)
        with patch.object(self.guest, "read_control") as read:
            with self.assertRaises(self.guest.ObservationError) as error:
                self.local_collect(request)
        self.assertEqual(error.exception.reason, "CONTROL_PATH_INVALID")
        read.assert_not_called()

    @unittest.skipUnless(sys.platform == "linux" and os.geteuid() == 0,
                         "full procfs census requires the deployed Linux root collector")
    def test_linux_census_retains_exact_running_postgres_process(self):
        import pwd
        import shutil
        import time
        owner = pwd.getpwnam("nobody")
        self.root.chmod(0o711)
        executable = self.root / "postgres"
        shutil.copyfile("/usr/bin/sleep", executable)
        executable.chmod(0o755)
        child = subprocess.Popen([str(executable), "15"], user=owner.pw_uid,
                                 group=owner.pw_gid, extra_groups=[])
        try:
            expected = hashlib.sha256(executable.read_bytes()).hexdigest()
            # The OS exec boundary is not a product readiness assertion.
            limit = time.monotonic() + 5
            while os.readlink(f"/proc/{child.pid}/exe") != str(executable):
                if time.monotonic() > limit:
                    self.fail("test child did not exec")
                time.sleep(0.01)
            records = self.guest.scan_processes(owner.pw_uid)
            observed = next(record for record in records if record["pid"] == child.pid)
            self.assertGreater(observed["starttime"], 0)
            self.assertEqual(observed["exe_sha256"], expected)
        finally:
            child.terminate()
            child.wait(timeout=5)
        self.assertFalse(any(record["pid"] == child.pid
                             for record in self.guest.scan_processes(owner.pw_uid)))


class RemoteCommandTests(unittest.TestCase):
    def setUp(self):
        self.assertIsNotNone(importlib.util.find_spec("remote"),
                             "read-only remote action controller is missing")
        import remote
        self.remote = remote
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.node = fixture()["nodes"][0]
        key = self.root / "identity"
        key.write_text("synthetic non-secret key for mocked SSH")
        key.chmod(0o600)
        self.node["admin_endpoint"]["identity_file"] = str(key)

    def test_transport_preserves_remote_rc_without_claiming_process_exit(self):
        with patch("subprocess.run", return_value=subprocess.CompletedProcess(
                [], 255, "partial output", "connection closed")):
            result = self.remote.observe(self.node, "a" * 64)
        self.assertEqual(result["transport"]["rc"], 255)
        self.assertEqual(result["transport"]["stdout"], "partial output")
        self.assertEqual(result["transport"]["stderr"], "connection closed")
        self.assertEqual(result["reason"], "SSH_COMMAND_FAILED")
        self.assertIsNone(result["observation"])
        self.assertFalse(result["deployment_qualified"])

    def test_timeout_preserves_partial_evidence_but_not_a_fake_rc(self):
        with patch("subprocess.run", side_effect=subprocess.TimeoutExpired(
                [], 30, output=b"partial", stderr=b"pending")):
            result = self.remote.observe(self.node, "a" * 64)
        self.assertEqual(result["reason"], "SSH_TIMEOUT")
        self.assertIsNone(result["transport"]["rc"])
        self.assertTrue(result["transport"]["timed_out"])
        self.assertIsNone(result["observation"])

    def test_ssh_has_fixed_program_and_structured_stdin(self):
        def run(argv, **kwargs):
            self.assertNotIn("shell", kwargs)
            self.assertIn("StrictHostKeyChecking=yes", argv)
            self.assertIn("IdentityAgent=none", argv)
            self.assertIn("IdentityFile=none", argv)
            self.assertIn("/dev/null", argv)
            self.assertNotIn(self.node["pgdata"], argv[-1])
            self.assertTrue(argv[-1].startswith("sudo -n /usr/bin/python3 -I -c "))
            request = json.loads(kwargs["input"])
            self.assertEqual(request["action"], "status")
            self.assertEqual(request["node"]["pgdata"], "/srv/pgrac/node/pgdata")
            self.assertNotIn("admin_endpoint", request["node"])
            return subprocess.CompletedProcess(argv, 0, "{}", "")
        with patch("subprocess.run", side_effect=run):
            result = self.remote.observe(self.node, "a" * 64)
        self.assertEqual(result["reason"], "REMOTE_OUTPUT_INVALID")

    def test_output_requires_same_request_binding_and_no_extra_json(self):
        for output in ('{"status":"PASS"}', '{}\n{}', '"secret"'):
            with self.subTest(output=output):
                with patch("subprocess.run", return_value=subprocess.CompletedProcess([], 0, output, "")):
                    result = self.remote.observe(self.node, "a" * 64)
                self.assertEqual(result["status"], "ERROR")
                self.assertIsNone(result["observation"])

    def response(self):
        request = self.remote.request_for(self.node, "a" * 64)
        return {"status": "PASS", "reason": "NODE_OBSERVED", "schema_version": 1,
                "kind": "pre1-node-observation", "request_sha256": document_sha(request),
                "deployment_qualified": False, "observation": {
                    "node_id": 0, "identity": {key: self.node[key] for key in
                        ("vm_uuid", "machine_id", "boot_id")},
                    "binary_sha256": "a" * 64, "pgdata": "/srv/pgrac/node/pgdata",
                    "pgdata_state": "EMPTY", "control": None, "pidfile": None,
                    "processes": []}}

    def test_complete_empty_observation_is_not_start_qualification(self):
        response = self.response()
        with patch("subprocess.run", return_value=subprocess.CompletedProcess([], 0, json.dumps(response), "")):
            result = self.remote.observe(self.node, "a" * 64)
        self.assertEqual(result["status"], "PASS")
        self.assertEqual(result["observation"]["pgdata_state"], "EMPTY")
        self.assertFalse(result["deployment_qualified"])
        self.assertEqual(result["scope"], "NODE_OBSERVATION_ONLY")

    def test_fake_process_or_contradictory_control_is_not_accepted(self):
        changes = [{"processes": [False]},
                   {"control": {"rc": 0, "stdout": "garbage", "stderr": "", "parsed": None}},
                   {"pidfile": {"pid": True, "pgdata": "/other/data", "start_epoch": 1}},
                   {"pgdata_state": "INITIALIZED", "control": None}]
        for change in changes:
            response = self.response()
            response["observation"].update(change)
            with self.subTest(change=change), patch("subprocess.run", return_value=
                    subprocess.CompletedProcess([], 0, json.dumps(response), "")):
                result = self.remote.observe(self.node, "a" * 64)
            self.assertEqual(result["reason"], "REMOTE_OUTPUT_INVALID")
            self.assertIsNone(result["observation"])

    def test_cli_does_not_overwrite_evidence_or_send_a_command(self):
        path = self.root / "node.json"
        path.write_text(json.dumps(self.node))
        out = self.root / "status.json"
        out.write_bytes(b"retained evidence")
        with patch.object(self.remote, "observe") as observe:
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                rc = self.remote.main(["status", "--node", str(path),
                                       "--binary-sha256", "a" * 64, "--out", str(out)])
        self.assertEqual(rc, 3)
        self.assertEqual(json.loads(stdout.getvalue())["reason"], "ARTIFACT_EXISTS")
        observe.assert_not_called()
        self.assertEqual(out.read_bytes(), b"retained evidence")

    def test_unsafe_node_never_starts_ssh(self):
        self.node["pgdata"] = "/srv/pgrac/../other"
        with patch("subprocess.run") as execute:
            with self.assertRaises(PreflightError) as error:
                self.remote.observe(self.node, "a" * 64)
        self.assertEqual(error.exception.reason, "UNSAFE_PATH")
        execute.assert_not_called()


if __name__ == "__main__":
    unittest.main()
