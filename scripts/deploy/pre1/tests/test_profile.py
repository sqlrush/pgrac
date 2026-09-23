"""Synthetic unit fixtures do not certify a live deployment.

Author: SqlRush <sqlrush@gmail.com>
"""

import copy
import fcntl
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import PreflightError, load_json, publish_artifact, validate_profile


def fixture():
    """A complete but deliberately synthetic planned deployment identity."""
    sha = "a" * 64
    nodes = []
    for n in range(4):
        nodes.append({
            "node_id": n,
            "vm_uuid": f"00000000-0000-4000-8000-{n + 1:012d}",
            "machine_id": f"{n + 1:032x}",
            "boot_id": f"10000000-0000-4000-8000-{n + 1:012d}",
            "admin_endpoint": {"host": f"192.0.2.{n + 10}", "port": 22,
                               "user": "operator", "identity_file": "/keys/lab"},
            "ssh_host_key": "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIEZha2VGb3JVbml0VGVzdHNPbmx5MDAwMDAwMDAwMDAw",
            "sql_addr": f"192.0.2.{n + 10}:5432",
            "control_addr": f"192.0.2.{n + 10}:6540",
            "data_base_addr": f"192.0.2.{n + 10}:6541", "data_workers": 2,
            "pgdata": "/srv/pgrac/node/pgdata", "install_root": "/opt/pgrac",
            "log_root": "/var/log/pgrac", "uid": 10001, "gid": 10001,
        })
    votes = [{"wwid": f"naa.600000000000000{n}", "size": 16777216,
              "logical_sector": 512, "index": n} for n in range(3)]
    return {
        "schema_version": 1, "profile_id": "pre1-gfs2-v1",
        "campaign_id": "synthetic-unit-only", "source_commit": "b" * 40,
        "source_tree": "c" * 40, "binary_sha256": sha,
        "configure_args": ["--enable-cluster"], "compiler": "gcc synthetic",
        "package_versions": [{"name": "gfs2-utils", "version": "synthetic"}],
        "block_size": 8192, "encoding": "UTF8", "locale": "C",
        "collation_version": "C", "nodes": nodes,
        "data_lun_wwid": "naa.6000000000000003", "pv_uuid": "synthetic-pv",
        "vg_uuid": "synthetic-vg", "lv_uuid": "synthetic-lv",
        "fs_uuid": "20000000-0000-4000-8000-000000000001",
        "mountpoint": "/srv/pgrac/shared", "votes": votes,
        "fencing": {"vm_map": [{"node_id": n["node_id"], "vm_uuid": n["vm_uuid"]}
                               for n in nodes], "agent_version": "synthetic",
                    "credential_ref": "/keys/fencing"},
        "guc_file_hash": sha, "guc_runtime_hash": sha, "pgrac_conf_hash": sha,
        "seed_schema_hash": sha, "seed_backup_hash": sha, "dataset_id": "main-1",
        "test_contract_hash": sha, "judge_sha": sha, "workload_sha": sha,
        "fixture_inventory": {
            "MAIN": {"root": "/srv/pgrac/cases/main", "vote_wwids": [v["wwid"] for v in votes]},
            "NEGATIVE": {"root": "/srv/pgrac/cases/negative",
                         "vote_wwids": [f"naa.700000000000000{n}" for n in range(3)]}},
        "clock_offset": [{"node_id": n, "offset_ms": 0} for n in range(4)],
        "power_policy": "ac-awake", "host_boot_id": "30000000-0000-4000-8000-000000000001",
        "authorization": {"device_allowlist": [
            {"wwid": "naa.6000000000000003", "size": 10737418240,
             "purpose": "data", "fresh": True},
            *[{"wwid": v["wwid"], "size": v["size"], "purpose": "voting", "fresh": True}
              for v in votes],
            *[{"wwid": w, "size": 16777216, "purpose": "negative-voting", "fresh": True}
              for w in [f"naa.700000000000000{n}" for n in range(3)]]]},
    }


class ProfileTests(unittest.TestCase):
    def setUp(self):
        self.profile = fixture()

    def rejects(self, reason, field=None):
        with self.assertRaises(PreflightError) as got:
            validate_profile(self.profile)
        self.assertEqual(got.exception.reason, reason)
        if field:
            self.assertEqual(got.exception.field, field)
        self.assertNotIn("DO_NOT_PRINT_SECRET", str(got.exception))

    def test_complete_fixture_retained_without_mutation(self):
        original = copy.deepcopy(self.profile)
        self.assertEqual(validate_profile(self.profile), original)
        self.assertEqual(self.profile, original)

    def test_same_kernel_is_not_four_vms(self):
        self.profile["nodes"][1]["boot_id"] = self.profile["nodes"][0]["boot_id"]
        self.rejects("IDENTITY_DUPLICATE", "nodes.boot_id")

    def test_duplicate_node_machine_and_domain(self):
        for key in ("node_id", "machine_id", "vm_uuid"):
            with self.subTest(key=key):
                self.profile = fixture()
                self.profile["nodes"][1][key] = self.profile["nodes"][0][key]
                self.rejects("IDENTITY_DUPLICATE", f"nodes.{key}")

    def test_unknown_or_missing_fields_fail_closed_without_value_echo(self):
        self.profile["password"] = "DO_NOT_PRINT_SECRET"
        self.rejects("UNKNOWN_FIELD", "profile")
        self.profile = fixture()
        del self.profile["binary_sha256"]
        self.rejects("REQUIRED_FIELD", "profile.binary_sha256")

    def test_nested_unknown_fields_rejected(self):
        self.profile["nodes"][0]["password"] = "DO_NOT_PRINT_SECRET"
        self.rejects("UNKNOWN_FIELD", "profile.nodes[0]")

    def test_strict_types_reject_bool_as_integer(self):
        self.profile["nodes"][0]["node_id"] = False
        self.rejects("FIELD_TYPE", "profile.nodes[0].node_id")

    def test_missing_vote_and_bad_sector(self):
        self.profile["votes"].pop()
        self.rejects("FIELD_RANGE")
        self.profile = fixture()
        self.profile["votes"][0]["logical_sector"] = 4096
        self.rejects("FIELD_VALUE")

    def test_data_vote_alias_collision(self):
        self.profile["votes"][0]["wwid"] = self.profile["data_lun_wwid"]
        self.rejects("DEVICE_OVERLAP")

    def test_authorization_is_exact_not_a_device_glob(self):
        self.profile["authorization"]["device_allowlist"][0]["wwid"] = "/dev/sd*"
        self.rejects("FIELD_VALUE")

    def test_authorization_size_and_role_must_match(self):
        self.profile["authorization"]["device_allowlist"][1]["size"] += 512
        self.rejects("DEVICE_AUTHORIZATION_MISMATCH")

    def test_negative_fixture_must_not_share_main_votes(self):
        self.profile["fixture_inventory"]["NEGATIVE"]["vote_wwids"][0] = self.profile["votes"][0]["wwid"]
        self.rejects("DEVICE_OVERLAP")

    def test_fence_mapping_must_match_exact_domain(self):
        self.profile["fencing"]["vm_map"][0]["vm_uuid"] = self.profile["nodes"][1]["vm_uuid"]
        self.rejects("FENCE_MAPPING_MISMATCH")

    def test_unsafe_remote_path_rejected_lexically(self):
        for value in ("/", "/home", "/root", "/srv/../root", "~/pgdata", "/Users/operator", "/srv/pgrac/node/../pgdata"):
            with self.subTest(value=value):
                self.profile = fixture()
                self.profile["nodes"][0]["pgdata"] = value
                self.rejects("UNSAFE_PATH")

    def test_local_and_shared_roots_cannot_overlap(self):
        self.profile["nodes"][0]["pgdata"] = "/srv/pgrac/shared/pgdata"
        self.rejects("PATH_OVERLAP")

    def test_double_slash_cannot_alias_main_and_negative(self):
        self.profile["fixture_inventory"]["NEGATIVE"]["root"] = "//srv/pgrac/cases/main"
        self.rejects("UNSAFE_PATH")

    def test_endpoint_must_not_be_loopback_or_shell_text(self):
        for value in ("127.0.0.1:5432", "0.0.0.0:5432", "host;id:5432", "192.0.2.1:99999"):
            with self.subTest(value=value):
                self.profile = fixture()
                self.profile["nodes"][0]["sql_addr"] = value
                self.rejects("ENDPOINT_INVALID")

    def test_worker_port_range_cannot_overlap_control_or_sql(self):
        self.profile["nodes"][0]["data_base_addr"] = "192.0.2.10:6539"
        self.rejects("PORT_OVERLAP")


class ArtifactTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def test_duplicate_json_members_rejected_without_echo(self):
        path = self.root / "input.json"
        path.write_text('{"password":"DO_NOT_PRINT_SECRET","password":"x"}')
        with self.assertRaises(PreflightError) as got:
            load_json(path)
        self.assertEqual(got.exception.reason, "JSON_DUPLICATE_KEY")
        self.assertNotIn("DO_NOT_PRINT_SECRET", str(got.exception))

    def test_json_nan_rejected(self):
        path = self.root / "input.json"
        path.write_text('{"value":NaN}')
        with self.assertRaises(PreflightError) as got:
            load_json(path)
        self.assertEqual(got.exception.reason, "JSON_INVALID")

    def test_publish_is_private_and_never_overwrites(self):
        path = self.root / "result.json"
        document = {"status": "BLOCKED", "reason": "NOT_QUALIFIED"}
        publish_artifact(path, document)
        self.assertEqual(json.loads(path.read_text()), document)
        self.assertEqual(path.stat().st_mode & 0o777, 0o600)
        before = path.read_bytes()
        with self.assertRaises(PreflightError) as got:
            publish_artifact(path, {"status": "PASS"})
        self.assertEqual(got.exception.reason, "ARTIFACT_EXISTS")
        self.assertEqual(path.read_bytes(), before)

    def test_failed_fsync_does_not_publish(self):
        path = self.root / "result.json"
        with patch("os.fsync", side_effect=OSError("DO_NOT_PRINT_SECRET")):
            with self.assertRaises(PreflightError) as got:
                publish_artifact(path, {"status": "PASS"})
        self.assertEqual(got.exception.reason, "ARTIFACT_IO")
        self.assertFalse(path.exists())
        self.assertNotIn("DO_NOT_PRINT_SECRET", str(got.exception))

    def test_publish_failure_does_not_leave_temporary(self):
        path = self.root / "result.json"
        with patch("os.link", side_effect=OSError("DO_NOT_PRINT_SECRET")):
            with self.assertRaises(PreflightError):
                publish_artifact(path, {"status": "PASS"})
        self.assertFalse(path.exists())
        self.assertEqual(list(self.root.glob(".pre1-????????")), [])

    def test_noncooperating_late_writer_cannot_be_overwritten(self):
        path = self.root / "result.json"
        real_fsync = os.fsync
        def write_after_check(fd):
            path.write_text("prior evidence")
            return real_fsync(fd)
        with patch("os.fsync", side_effect=write_after_check):
            with self.assertRaises(PreflightError) as got:
                publish_artifact(path, {"status": "PASS"})
        self.assertEqual(got.exception.reason, "ARTIFACT_EXISTS")
        self.assertEqual(path.read_text(), "prior evidence")

    def test_directory_fsync_failure_leaves_unqualified_orphan_not_overwrite(self):
        path = self.root / "result.json"
        with patch("os.fsync", side_effect=[None, OSError("disk lost")]):
            with self.assertRaises(PreflightError):
                publish_artifact(path, {"status": "BLOCKED"})
        self.assertTrue(path.exists())
        with self.assertRaises(PreflightError) as got:
            publish_artifact(path, {"status": "PASS"})
        self.assertEqual(got.exception.reason, "ARTIFACT_EXISTS")

    def test_concurrent_controller_is_refused(self):
        lock = self.root / ".pre1-evidence.lock"
        fd = os.open(lock, os.O_CREAT | os.O_RDWR, 0o600)
        self.addCleanup(os.close, fd)
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with self.assertRaises(PreflightError) as got:
            publish_artifact(self.root / "result.json", {})
        self.assertEqual(got.exception.reason, "CONTROLLER_BUSY")

    def test_symlink_destination_not_followed(self):
        target = self.root / "original"
        target.write_text("preserve")
        path = self.root / "result.json"
        path.symlink_to(target)
        with self.assertRaises(PreflightError):
            publish_artifact(path, {"status": "PASS"})
        self.assertEqual(target.read_text(), "preserve")


if __name__ == "__main__":
    unittest.main()
