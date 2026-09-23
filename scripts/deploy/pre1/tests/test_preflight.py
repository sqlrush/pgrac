"""CLI and read-only identity collection tests; no live certification.

Author: SqlRush <sqlrush@gmail.com>
"""

import contextlib
import copy
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
from test_profile import fixture
import preflight


def observation(node):
    return {"node_id": node["node_id"], "vm_uuid": node["vm_uuid"],
            "machine_id": node["machine_id"], "boot_id": node["boot_id"],
            "virtualization": "kvm", "kernel": "unit-kernel", "arch": "x86_64"}


class PreflightTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.profile = fixture()

    def call(self, args):
        stdout = io.StringIO()
        stderr = io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            rc = preflight.main(args)
        return rc, json.loads(stdout.getvalue()), stderr.getvalue()

    def test_profile_check_is_explicitly_not_live_qualification(self):
        path = self.root / "profile.json"
        path.write_text(json.dumps(self.profile))
        rc, result, _ = self.call(["check-profile", "--profile", str(path)])
        self.assertEqual(rc, 0)
        self.assertEqual(result["status"], "PASS")
        self.assertEqual(result["scope"], "PROFILE_ONLY")
        self.assertFalse(result["deployment_qualified"])

    def test_errors_are_one_safe_json_not_secret_tracebacks(self):
        path = self.root / "profile.json"
        self.profile["DO_NOT_PRINT_SECRET"] = "DO_NOT_PRINT_SECRET"
        path.write_text(json.dumps(self.profile))
        rc, result, stderr = self.call(["check-profile", "--profile", str(path)])
        self.assertEqual(rc, 2)
        self.assertEqual(result["reason"], "UNKNOWN_FIELD")
        self.assertNotIn("DO_NOT_PRINT_SECRET", json.dumps(result) + stderr)

    def test_inventory_does_not_claim_storage_or_fencing_qualification(self):
        path = self.root / "profile.json"
        path.write_text(json.dumps(self.profile))
        out = self.root / "inventory.json"
        with patch.object(preflight, "collect_node", side_effect=lambda n: observation(n)):
            rc, result, _ = self.call(["inventory", "--profile", str(path), "--out", str(out)])
        self.assertEqual(rc, 2)
        self.assertEqual(result["reason"], "QUALIFICATION_PENDING")
        saved = json.loads(out.read_text())
        self.assertEqual(saved["profile_sha256"], document_sha(self.profile))
        self.assertEqual(len(saved["nodes"]), 4)
        self.assertIn("STORAGE_IDENTITY", saved["pending_checks"])
        self.assertFalse(saved["deployment_qualified"])
        rc, result, _ = self.call(["verify", "--profile", str(path), "--inventory", str(out)])
        self.assertEqual(rc, 2)
        self.assertEqual(result["reason"], "QUALIFICATION_PENDING")

    def test_inventory_timeout_never_publishes_success(self):
        path = self.root / "profile.json"
        path.write_text(json.dumps(self.profile))
        out = self.root / "inventory.json"
        with patch.object(preflight, "collect_node", side_effect=PreflightError("SSH_TIMEOUT", status="ERROR")):
            rc, result, _ = self.call(["inventory", "--profile", str(path), "--out", str(out)])
        self.assertEqual(rc, 3)
        self.assertEqual(result["reason"], "SSH_TIMEOUT")
        self.assertFalse(out.exists())

    def test_changed_manifest_inventory_cannot_be_reused(self):
        inventory = preflight.build_inventory(self.profile, [observation(n) for n in self.profile["nodes"]])
        changed = copy.deepcopy(self.profile)
        changed["binary_sha256"] = "d" * 64
        with self.assertRaises(PreflightError) as got:
            preflight.verify_inventory(changed, inventory)
        self.assertEqual(got.exception.reason, "INVENTORY_PROFILE_MISMATCH")

    def test_container_and_wrong_observed_uuid_are_rejected(self):
        for key, value, reason in (("virtualization", "docker", "INDEPENDENT_KERNEL_UNPROVEN"),
                                   ("vm_uuid", "f" * 36, "OBSERVED_IDENTITY_MISMATCH")):
            with self.subTest(key=key):
                observations = [observation(n) for n in self.profile["nodes"]]
                observations[0][key] = value
                with self.assertRaises(PreflightError) as got:
                    preflight.build_inventory(self.profile, observations)
                self.assertEqual(got.exception.reason, reason)

    def test_reloaded_observation_rejects_boolean_node_id(self):
        nodes = [observation(n) for n in self.profile["nodes"]]
        nodes[0]["node_id"] = False
        with self.assertRaises(PreflightError) as got:
            preflight.build_inventory(self.profile, nodes)
        self.assertEqual(got.exception.reason, "OBSERVATION_INVALID")

    def test_malformed_command_still_returns_single_json(self):
        rc, result, stderr = self.call(["check-profile", "--private-DO_NOT_PRINT_SECRET"])
        self.assertEqual(rc, 2)
        self.assertEqual(result["reason"], "CLI_ARGUMENTS")
        self.assertNotIn("DO_NOT_PRINT_SECRET", json.dumps(result) + stderr)

    def test_ssh_uses_pinned_key_and_no_inherited_configuration(self):
        node = self.profile["nodes"][0]
        key = self.root / "private-key"
        key.write_text("synthetic non-secret key used only with mocked SSH")
        key.chmod(0o600)
        node["admin_endpoint"]["identity_file"] = str(key)
        def run(argv, **kwargs):
            self.assertIsInstance(argv, list)
            self.assertNotIn("shell", kwargs)
            self.assertIn("StrictHostKeyChecking=yes", argv)
            self.assertIn("GlobalKnownHostsFile=/dev/null", argv)
            self.assertIn("/dev/null", argv)
            self.assertIn("IdentityAgent=none", argv)
            self.assertIn("IdentityFile=none", argv)
            self.assertNotIn("accept-new", " ".join(argv))
            known = next(a.split("=", 1)[1] for a in argv if a.startswith("UserKnownHostsFile="))
            self.assertEqual(Path(known).stat().st_mode & 0o777, 0o600)
            self.assertIn(node["ssh_host_key"], Path(known).read_text())
            return subprocess.CompletedProcess(argv, 0, json.dumps(observation(node)), "")
        with patch("subprocess.run", side_effect=run):
            self.assertEqual(preflight.collect_node(node), observation(node))

    def test_ssh_failures_do_not_echo_remote_output(self):
        node = self.profile["nodes"][0]
        key = self.root / "private-key"
        key.write_text("synthetic non-secret key used only with mocked SSH")
        key.chmod(0o600)
        node["admin_endpoint"]["identity_file"] = str(key)
        errors = [subprocess.CompletedProcess([], 255, "DO_NOT_PRINT_SECRET", "key changed DO_NOT_PRINT_SECRET"),
                  subprocess.TimeoutExpired([], 20, output="DO_NOT_PRINT_SECRET")]
        for err in errors:
            with self.subTest(err=type(err).__name__):
                kwargs = {"side_effect": err} if isinstance(err, Exception) else {"return_value": err}
                with patch("subprocess.run", **kwargs):
                    with self.assertRaises(PreflightError) as got:
                        preflight.collect_node(node)
                self.assertEqual(got.exception.status, "ERROR")
                self.assertNotIn("DO_NOT_PRINT_SECRET", str(got.exception))

    def test_missing_identity_does_not_fall_back_to_controller_default_key(self):
        node = self.profile["nodes"][0]
        node["admin_endpoint"]["identity_file"] = str(self.root / "missing")
        with patch("subprocess.run") as run:
            with self.assertRaises(PreflightError) as got:
                preflight.collect_node(node)
        self.assertEqual(got.exception.reason, "SSH_IDENTITY_UNAVAILABLE")
        run.assert_not_called()

    def test_world_readable_identity_is_rejected(self):
        node = self.profile["nodes"][0]
        key = self.root / "private-key"
        key.write_text("synthetic non-secret key")
        key.chmod(0o644)
        node["admin_endpoint"]["identity_file"] = str(key)
        with patch("subprocess.run") as run:
            with self.assertRaises(PreflightError) as got:
                preflight.collect_node(node)
        self.assertEqual(got.exception.reason, "SSH_IDENTITY_UNAVAILABLE")
        run.assert_not_called()


if __name__ == "__main__":
    unittest.main()
