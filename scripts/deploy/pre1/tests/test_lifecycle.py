"""Lifecycle observer tests; fixtures never authorize database startup.

Author: SqlRush <sqlrush@gmail.com>
"""

import contextlib
import copy
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import PreflightError, document_sha
import remote
import guest_status
from test_profile import fixture


class LifecycleTests(unittest.TestCase):
    def setUp(self):
        self.assertIsNotNone(importlib.util.find_spec("lifecycle"), "four-node reconciliation is missing")
        import lifecycle
        self.lifecycle = lifecycle
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        key = self.root / "key"
        key.write_text("synthetic test key, never sent to SSH")
        key.chmod(0o600)
        self.nodes = fixture()["nodes"]
        for node in self.nodes:
            node["admin_endpoint"]["identity_file"] = str(key)
        self.binary = "a" * 64

    def observations(self, state="EMPTY", system_identifier="7654321000123456789", processes=None,
                     control_warning="", control_stderr=""):
        records = []
        for node in self.nodes:
            control = None
            if state not in ("EMPTY", "ABSENT", "PARTIAL"):
                control = guest_status.control_result(subprocess.CompletedProcess(
                    [], 0, control_warning + f"Database system identifier: {system_identifier}\n"
                    f"Database cluster state: {state}\n", control_stderr))
            facts = {"node_id": node["node_id"], "identity": {key: node[key] for key in
                     ("vm_uuid", "machine_id", "boot_id")}, "binary_sha256": self.binary,
                     "pgdata": node["pgdata"], "pgdata_state": state if control is None else "INITIALIZED",
                     "control": control, "pidfile": None, "processes": processes or []}
            response = {"schema_version": 1, "kind": "pre1-node-observation",
                        "request_sha256": document_sha(remote.request_for(node, self.binary)),
                        "status": "PASS", "reason": "NODE_OBSERVED", "observation": facts,
                        "deployment_qualified": False}
            transport = {"rc": 0, "stdout": json.dumps(response), "stderr": "",
                         "timed_out": False, "truncated": False}
            with patch.object(remote, "run_ssh", return_value=transport):
                records.append(remote.observe(node, self.binary))
        return records

    def reconcile(self, records, **kwargs):
        return self.lifecycle.reconcile(self.nodes, self.binary, records, **kwargs)

    def test_empty_is_not_bootstrap_or_restart_authority(self):
        result = self.reconcile(self.observations())
        self.assertEqual(result["state"], "EMPTY_NOT_INITIALIZED")
        self.assertFalse(result["data_clean"])
        self.assertFalse(result["restart_allowed"])
        self.assertFalse(result["deployment_qualified"])

    def test_clean_controls_do_not_prove_protocol_or_persistent_closure(self):
        result = self.reconcile(self.observations("shut down"))
        self.assertEqual(result["state"], "DATA_CLEAN_CLOSURE_UNPROVEN")
        self.assertTrue(result["data_clean"])
        self.assertFalse(result["restart_allowed"])
        self.assertEqual(result["pending"], ["PROTOCOL_CLOSED", "PERSISTENT_CLOSURE"])

    def test_no_pidfile_does_not_make_unclean_data_clean(self):
        for state in ("in production", "in crash recovery", "shut down in recovery", "shutting down"):
            with self.subTest(state=state):
                result = self.reconcile(self.observations(state))
                self.assertEqual(result["state"], "UNCLEAN_OR_UNKNOWN")
                self.assertFalse(result["data_clean"])
                self.assertFalse(result["restart_allowed"])

    def test_native_control_warnings_cannot_prove_data_clean_even_with_rc_zero(self):
        # pg_controldata emits these diagnostics on stdout but still exits zero.
        for warning in (
                "WARNING: Calculated CRC checksum does not match value stored in file.\n"
                "Either the file is corrupt, or it has a different layout than this program\n"
                "is expecting.  The results below are untrustworthy.\n\n",
                "WARNING: invalid WAL segment size\n"):
            with self.subTest(warning=warning):
                result = self.reconcile(self.observations("shut down", control_warning=warning))
                self.assertFalse(result["data_clean"])
                self.assertFalse(result["restart_allowed"])
                self.assertEqual(result["state"], "OBSERVATION_INCOMPLETE")
                self.assertEqual(result["status"], "ERROR")
                control = result["observations"][0]["observation"]["control"]
                self.assertEqual(control["rc"], 0)
                self.assertIn(warning, control["stdout"])
                self.assertIsNone(control["parsed"])
        result = self.reconcile(self.observations("shut down", control_stderr="untrusted diagnostic\n"))
        self.assertFalse(result["data_clean"])
        self.assertEqual(result["observations"][0]["observation"]["control"]["stderr"],
                         "untrusted diagnostic\n")

    def test_live_auxiliary_blocks_process_gone_even_with_clean_controls(self):
        processes = [{"pid": 123, "ppid": 1, "starttime": 1234, "state": "S",
                      "exe": "/opt/pgrac/bin/postgres", "exe_sha256": self.binary, "uid": 10001}]
        result = self.reconcile(self.observations("shut down", processes=processes))
        self.assertEqual(result["state"], "PROCESSES_PRESENT")
        self.assertTrue(result["data_clean"])
        self.assertFalse(result["process_gone"])
        self.assertFalse(result["restart_allowed"])

    def test_partial_and_mixed_dataset_are_preserved(self):
        records = self.observations("shut down")
        records[0] = self.observations("PARTIAL")[0]
        self.assertEqual(self.reconcile(records)["state"], "PARTIAL_DATASET")
        self.assertFalse(self.reconcile(records)["restart_allowed"])

    def test_four_matching_system_identifiers_are_required(self):
        records = self.observations("shut down")
        records[0] = self.observations("shut down", "123456789")[0]
        result = self.reconcile(records)
        self.assertEqual(result["state"], "IDENTITY_MISMATCH")
        self.assertFalse(result["data_clean"])
        result = self.reconcile(self.observations("shut down"), expected_system_identifier="123456789")
        self.assertEqual(result["state"], "IDENTITY_MISMATCH")

    def test_missing_duplicate_and_boolean_node_ids_are_rejected(self):
        records = self.observations()
        for bad in (records[:3], [records[0], records[0], records[2], records[3]]):
            with self.assertRaises(PreflightError):
                self.reconcile(bad)
        bad = copy.deepcopy(records)
        bad[0]["node_id"] = False
        with self.assertRaises(PreflightError):
            self.reconcile(bad)

    def test_stale_request_and_changed_boot_cannot_bind_new_observation(self):
        records = self.observations()
        for field in ("node_sha256", "request_sha256", "program_sha256"):
            bad = copy.deepcopy(records)
            bad[0][field] = "f" * 64
            with self.subTest(field=field), self.assertRaises(PreflightError):
                self.reconcile(bad)
        self.nodes[0]["boot_id"] = "00000000-0000-4000-8000-000000000099"
        with self.assertRaises(PreflightError):
            self.reconcile(records)

    def test_ssh_failure_is_not_process_absence(self):
        records = self.observations()
        records[0].update({"status": "ERROR", "reason": "SSH_COMMAND_FAILED", "observation": None})
        records[0]["transport"].update({"rc": 255, "stdout": "", "stderr": "connection lost"})
        result = self.reconcile(records)
        self.assertEqual(result["state"], "OBSERVATION_INCOMPLETE")
        self.assertFalse(result["process_gone"])
        self.assertEqual(result["observations"][0]["transport"]["rc"], 255)

    def test_modified_summary_cannot_disagree_with_raw_observation(self):
        records = self.observations()
        records[0]["observation"]["pgdata_state"] = "ABSENT"
        with self.assertRaises(PreflightError):
            self.reconcile(records)

    def test_reconcile_cli_publishes_actual_four_observations_without_overwrite(self):
        paths = []
        for node in self.nodes:
            path = self.root / (str(node["node_id"]) + ".json")
            path.write_text(json.dumps(node))
            paths.append(str(path))
        out = self.root / "observed.json"
        args = ["reconcile", "--nodes", *paths, "--binary-sha256", self.binary, "--out", str(out)]
        records = self.observations()
        with patch.object(remote, "observe", side_effect=records) as observe:
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                self.assertEqual(self.lifecycle.main(args), 0)
            self.assertEqual(observe.call_count, 4)
        saved = out.read_bytes()
        self.assertEqual(json.loads(saved)["state"], "EMPTY_NOT_INITIALIZED")
        with patch.object(remote, "observe") as observe, contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(self.lifecycle.main(args), 3)
            observe.assert_not_called()
        self.assertEqual(out.read_bytes(), saved)


if __name__ == "__main__":
    unittest.main()
