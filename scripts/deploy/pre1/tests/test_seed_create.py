"""Seed creation guards; synthetic unit evidence is not deployment qualification.

Author: SqlRush <sqlrush@gmail.com>
"""

import hashlib
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import seed
from common import PreflightError


class SeedCreateTests(unittest.TestCase):
    def setUp(self):
        self.assertTrue(hasattr(seed, "validate_seed_create"),
                        "identity-bound seed creator is missing")
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        for name in ("data", "mount", "mount/main", "logs", "install", "install/bin"):
            (self.root / name).mkdir(mode=0o700)
        self.node = dict(node_id=0, vm_uuid="00000000-0000-4000-8000-000000000001",
                         boot_id="00000000-0000-4000-8000-000000000002",
                         machine_id="a" * 32, pgdata=str(self.root / "data"),
                         install_root=str(self.root / "install"),
                         uid=os.getuid() or 10001, gid=os.getgid() or 10001)
        self.sql = self.root / "schema.sql"
        self.sql.write_text("CREATE TABLE probe(id integer PRIMARY KEY);\n")
        self.request = dict(schema_version=1, action="create-seed", node=self.node,
                            binary_sha256="a" * 64, dataset_id="seed-test-1",
                            shared_mount=str(self.root / "mount"),
                            shared_root=str(self.root / "mount/main"),
                            fs_uuid="00000000-0000-4000-8000-000000000003",
                            backup=str(self.root / "backup"),
                            log=str(self.root / "logs/seed.log"),
                            schema_path=str(self.sql),
                            schema_sha256=hashlib.sha256(self.sql.read_bytes()).hexdigest())
        self.out = self.root / "result.json"
        self.facts = dict(node_id=0, pgdata_state="EMPTY", pidfile=None, processes=[],
                          control=None, identity={k: self.node[k] for k in
                                                 ("vm_uuid", "boot_id", "machine_id")})
        self.stop_result = dict(argv=["pidfd-fast-stop"], rc=0, timed_out=False,
                                truncated=False, stdout="", stderr="")

    def validate(self):
        with patch.object(seed, "seed_guest_observation", return_value=self.facts), \
             patch.object(seed, "require_seed_mount"):
            return seed.validate_seed_create(self.request, self.out)

    def test_valid_seed_plan_preserves_native_initialization_and_wal_checks(self):
        plan = self.validate()
        self.assertIn("--pgrac-hw-snapshot-owner=0", plan["initdb"])
        self.assertIn("--pgrac-hw-snapshot-root=" + self.request["shared_root"], plan["initdb"])
        self.assertIn("--wal-method=stream", plan["backup"])
        self.assertIn("--manifest-checksums=SHA256", plan["backup"])
        self.assertNotIn("--no-sync", plan["backup"])
        self.assertIn("cluster.enabled = off", plan["configuration"])
        self.assertIn("cluster.shared_catalog = off", plan["configuration"])
        self.assertIn("cluster.relation_extend_lock_enabled = off", plan["configuration"])
        self.assertNotIn("cluster.enabled = on", plan["configuration"])

    def test_nonempty_targets_and_wrong_guest_are_rejected_before_initdb(self):
        for field in ("backup", "log"):
            path = Path(self.request[field])
            path.write_text("preserve")
            with self.subTest(field=field), self.assertRaises(PreflightError):
                self.validate()
            self.assertEqual(path.read_text(), "preserve")
            path.unlink()
        (self.root / "data/retained").write_text("old")
        with self.assertRaises(PreflightError):
            self.validate()
        (self.root / "data/retained").unlink()
        self.facts["processes"] = [{"pid": 123}]
        with self.assertRaises(PreflightError):
            self.validate()
        self.facts["processes"] = []
        self.request["node"]["node_id"] = 1
        with self.assertRaises(PreflightError):
            self.validate()

    def test_overlapping_paths_missing_mount_and_changed_schema_are_rejected(self):
        original = self.request["backup"]
        for value in (self.node["pgdata"], self.request["shared_root"],
                      str(self.root / "data/backup")):
            self.request["backup"] = value
            with self.subTest(value=value), self.assertRaises(PreflightError):
                self.validate()
        self.request["backup"] = original
        with patch.object(seed, "seed_guest_observation", return_value=self.facts), \
             patch.object(seed, "require_seed_mount", side_effect=PreflightError("MOUNT_MISMATCH")):
            with self.assertRaises(PreflightError):
                seed.validate_seed_create(self.request, self.out)
        self.sql.write_text("changed")
        with self.assertRaises(PreflightError):
            self.validate()

    def test_existing_output_refuses_before_any_database_action(self):
        self.out.write_text("retained")
        with patch.object(seed, "seed_guest_observation") as observe:
            with self.assertRaises(PreflightError):
                seed.validate_seed_create(self.request, self.out)
            observe.assert_not_called()
        self.assertEqual(self.out.read_text(), "retained")

    def test_shared_root_and_output_cannot_overlap_any_input(self):
        for out in (self.root / "data/result.json", self.root / "mount/main/result.json",
                    self.sql, self.root / "backup/result.json"):
            with self.subTest(out=out), self.assertRaises(PreflightError):
                with patch.object(seed, "seed_guest_observation", return_value=self.facts), \
                     patch.object(seed, "require_seed_mount"):
                    seed.validate_seed_create(self.request, out)

    def test_failed_schema_still_normally_stops_its_exact_seed(self):
        self.assertTrue(hasattr(seed, "execute_seed_create"), "seed executor is missing")
        plan = self.validate()
        calls = []
        def command(argv, node):
            calls.append(argv)
            if argv == plan["initdb"]:
                (self.root / "data/postgresql.conf").write_text("# native config\n")
            return dict(argv=argv, rc=1 if argv == plan["schema"] else 0,
                        timed_out=False, truncated=False, stdout="", stderr="")
        live = dict(self.facts, pgdata_state="INITIALIZED",
                    control={"parsed": {"system_identifier": "12345", "state": "in production"}},
                    pidfile={"pid": 123, "pgdata": self.node["pgdata"], "start_epoch": 200},
                    processes=[dict(pid=123, starttime=500, exe_sha256="a" * 64)])
        clean = dict(self.facts, pgdata_state="INITIALIZED",
                     control={"parsed": {"system_identifier": "12345", "state": "shut down"}})
        with patch.object(seed, "run_seed_native", side_effect=command), \
             patch.object(seed, "stop_seed_exact", return_value=self.stop_result, create=True) as stop, \
             patch.object(seed, "seed_guest_observation", side_effect=[live, live, clean]), \
             patch.object(seed, "verify_backup") as verify, \
             patch.object(seed.time, "time", return_value=199):
            result = seed.execute_seed_create(self.request, plan)
        self.assertEqual(result["status"], "ERROR")
        self.assertEqual(result["state"], "SEED_SCHEMA_FAILED")
        self.assertTrue(result["seed_clean_stop"])
        stop.assert_called_once()
        self.assertNotIn(plan["backup"], calls)
        verify.assert_not_called()

    def test_changed_process_identity_is_not_signalled(self):
        self.assertTrue(hasattr(seed, "execute_seed_create"), "seed executor is missing")
        plan = self.validate()
        calls = []
        def command(argv, node):
            calls.append(argv)
            if argv == plan["initdb"]:
                (self.root / "data/postgresql.conf").write_text("# native config\n")
            return dict(argv=argv, rc=1 if argv == plan["schema"] else 0,
                        timed_out=False, truncated=False, stdout="", stderr="")
        live = dict(self.facts, pgdata_state="INITIALIZED",
                    control={"parsed": {"system_identifier": "12345", "state": "in production"}},
                    pidfile={"pid": 123, "pgdata": self.node["pgdata"], "start_epoch": 200},
                    processes=[dict(pid=123, starttime=500, exe_sha256="a" * 64)])
        changed = dict(live, processes=[dict(pid=123, starttime=999, exe_sha256="a" * 64)])
        with patch.object(seed, "run_seed_native", side_effect=command), \
             patch.object(seed, "stop_seed_exact", return_value=self.stop_result, create=True) as stop, \
             patch.object(seed, "seed_guest_observation", side_effect=[live, changed]), \
             patch.object(seed.time, "time", return_value=199):
            result = seed.execute_seed_create(self.request, plan)
        self.assertEqual(result["status"], "ERROR")
        self.assertFalse(result["seed_clean_stop"])
        stop.assert_not_called()
        self.assertEqual(result["cleanup_error"], "SEED_PROCESS_CHANGED")

    def test_stop_failure_never_qualifies_a_backup(self):
        self.assertTrue(hasattr(seed, "execute_seed_create"), "seed executor is missing")
        plan = self.validate()
        def command(argv, node):
            if argv == plan["initdb"]:
                (self.root / "data/postgresql.conf").write_text("# native config\n")
            return dict(argv=argv, rc=0,
                        timed_out=False, truncated=False, stdout="", stderr="")
        live = dict(self.facts, pgdata_state="INITIALIZED",
                    control={"parsed": {"system_identifier": "12345", "state": "in production"}},
                    pidfile={"pid": 123, "pgdata": self.node["pgdata"], "start_epoch": 200},
                    processes=[dict(pid=123, starttime=500, exe_sha256="a" * 64)])
        with patch.object(seed, "run_seed_native", side_effect=command), \
             patch.object(seed, "stop_seed_exact", return_value=dict(self.stop_result, rc=1), create=True), \
             patch.object(seed, "seed_guest_observation", return_value=live), \
             patch.object(seed, "verify_backup") as verify, \
             patch.object(seed.time, "time", return_value=199):
            result = seed.execute_seed_create(self.request, plan)
        self.assertEqual(result["status"], "ERROR")
        self.assertFalse(result["seed_clean_stop"])
        self.assertEqual(result["cleanup_error"], "SEED_NORMAL_STOP_FAILED")
        verify.assert_not_called()

    def test_success_requires_native_backup_and_actual_clean_control(self):
        self.assertTrue(hasattr(seed, "execute_seed_create"), "seed executor is missing")
        plan = self.validate()
        def command(argv, node):
            if argv == plan["initdb"]:
                (self.root / "data/postgresql.conf").write_text("# native config\n")
            if argv == plan["backup"]:
                (self.root / "backup").mkdir()
                (self.root / "backup/backup_manifest").write_text("manifest")
            return dict(argv=argv, rc=0, timed_out=False, truncated=False, stdout="", stderr="")
        live = dict(self.facts, pgdata_state="INITIALIZED",
                    control={"parsed": {"system_identifier": "12345", "state": "in production"}},
                    pidfile={"pid": 123, "pgdata": self.node["pgdata"], "start_epoch": 200},
                    processes=[dict(pid=123, starttime=500, exe_sha256="a" * 64)])
        clean = dict(self.facts, pgdata_state="INITIALIZED",
                     control={"parsed": {"system_identifier": "12345", "state": "shut down"}})
        with patch.object(seed, "run_seed_native", side_effect=command), \
             patch.object(seed, "stop_seed_exact", return_value=self.stop_result, create=True), \
             patch.object(seed, "seed_guest_observation", side_effect=[live, live, clean]), \
             patch.object(seed, "verify_backup", return_value={"status": "PASS"}) as verify, \
             patch.object(seed.time, "time", return_value=199):
            result = seed.execute_seed_create(self.request, plan)
        self.assertEqual(result["status"], "PASS")
        self.assertEqual(result["state"], "SEED_BACKUP_READY")
        self.assertTrue(result["seed_clean_stop"])
        self.assertFalse(result["bootstrap_ready"])
        self.assertFalse(result["restart_allowed"])
        verify.assert_called_once()

    def test_pidfd_is_bound_before_signal_and_changed_identity_is_refused(self):
        self.assertTrue(hasattr(seed, "stop_seed_exact"), "identity-bound stop is missing")
        expected = dict(pid=123, starttime=500, exe_sha256="a" * 64)
        with patch.object(os, "pidfd_open", return_value=7, create=True) as opened, \
             patch.object(os, "close") as closed, \
             patch.object(seed.guest_status, "read_identity", return_value=self.facts["identity"]), \
             patch.object(seed.guest_status, "process_record", return_value=dict(expected, starttime=999)), \
             patch.object(seed.signal, "pidfd_send_signal", create=True) as send:
            with self.assertRaises(PreflightError):
                seed.stop_seed_exact(self.request, expected)
        opened.assert_called_once_with(123, 0)
        closed.assert_called_once_with(7)
        send.assert_not_called()

    def test_start_failure_without_bound_postmaster_never_sends_stop(self):
        plan = self.validate()
        def command(argv, node):
            if argv == plan["initdb"]:
                (self.root / "data/postgresql.conf").write_text("# native config\n")
            return dict(argv=argv, rc=1 if argv == plan["start"] else 0,
                        timed_out=False, truncated=False, stdout="", stderr="")
        with patch.object(seed, "run_seed_native", side_effect=command), \
             patch.object(seed, "stop_seed_exact", return_value=self.stop_result, create=True) as stop:
            result = seed.execute_seed_create(self.request, plan)
        stop.assert_not_called()
        self.assertEqual(result["state"], "SEED_START_FAILED")
        self.assertFalse(result["seed_clean_stop"])


if __name__ == "__main__":
    unittest.main()
