"""Seed-backup prerequisite tests, not permission to start a cloned database.

Author: SqlRush <sqlrush@gmail.com>
"""

import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import PreflightError


class SeedBackupTests(unittest.TestCase):
    def setUp(self):
        self.assertIsNotNone(importlib.util.find_spec("seed"), "native backup verification is missing")
        import seed
        self.seed = seed
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.backup = self.root / "backup"
        self.backup.mkdir(mode=0o700)
        for directory in ("global", "pg_wal", "pg_tblspc"):
            (self.backup / directory).mkdir()
        for name, value in {"PG_VERSION": "16\n", "backup_label": "synthetic label\n",
                            "global/pg_control": "control",
                            "pg_wal/000000010000000000000001": "synthetic WAL"}.items():
            (self.backup / name).write_text(value)
        self.manifest_document = {"PostgreSQL-Backup-Manifest-Version": 1,
                                  "Files": [{"Path": name, "Size": (self.backup / name).stat().st_size,
                                             "Checksum-Algorithm": "SHA256",
                                             "Checksum": self.sha(self.backup / name)}
                                            for name in ("PG_VERSION", "backup_label", "global/pg_control")]}
        (self.backup / "backup_manifest").write_text(json.dumps(self.manifest_document))
        self.install = self.root / "install"
        (self.install / "bin").mkdir(parents=True)
        for name in ("postgres", "pg_verifybackup", "pg_controldata", "pg_waldump"):
            (self.install / "bin" / name).write_text("synthetic " + name)
            (self.install / "bin" / name).chmod(0o700)
        self.binary = self.sha(self.install / "bin/postgres")
        self.manifest = self.sha(self.backup / "backup_manifest")
        self.system_id = "7654321000123456789"

    @staticmethod
    def sha(path):
        return hashlib.sha256(path.read_bytes()).hexdigest()

    def native(self, argv):
        output = "backup successfully verified\n"
        if Path(argv[0]).name == "pg_controldata":
            output = (f"Database system identifier: {self.system_id}\n"
                      "Database cluster state: in production\n")
        return {"argv": argv, "rc": 0, "stdout": output, "stderr": "",
                "timed_out": False, "truncated": False}

    def verify(self):
        return self.seed.verify_backup(self.backup, self.install, self.binary,
                                       self.manifest, self.system_id)

    def test_checksums_and_wal_are_not_skipped_and_success_is_not_start_authority(self):
        with patch.object(self.seed, "run_native", side_effect=self.native) as native:
            result = self.verify()
        self.assertEqual(result["status"], "PASS")
        self.assertEqual(result["state"], "BACKUP_CONTENT_VERIFIED")
        self.assertFalse(result["bootstrap_ready"])
        self.assertFalse(result["restart_allowed"])
        self.assertEqual(native.call_args_list[0].args[0],
                         [str(self.install / "bin/pg_verifybackup"), str(self.backup)])
        self.assertIn("SEED_CLEAN_STOP", result["pending"])

    def test_missing_required_files_wrong_hash_and_used_backup_are_rejected(self):
        for name in ("backup_label", "backup_manifest", "global/pg_control", "PG_VERSION"):
            path = self.backup / name
            saved = path.read_bytes()
            path.unlink()
            with self.subTest(name=name), patch.object(self.seed, "run_native") as native:
                with self.assertRaises(PreflightError):
                    self.verify()
                native.assert_not_called()
            path.write_bytes(saved)
        self.manifest = "f" * 64
        with patch.object(self.seed, "run_native") as native:
            with self.assertRaises(PreflightError):
                self.verify()
            native.assert_not_called()
        self.manifest = self.sha(self.backup / "backup_manifest")
        (self.backup / "postmaster.pid").write_text("123\n")
        with self.assertRaises(PreflightError):
            self.verify()

    def test_links_special_files_and_external_tablespaces_are_refused(self):
        other = self.root / "retained"
        other.write_bytes(b"preserve")
        target = self.backup / "alias"
        for make in (lambda: target.symlink_to(other), lambda: os.link(other, target),
                     lambda: os.mkfifo(target)):
            make()
            with patch.object(self.seed, "run_native") as native:
                with self.assertRaises(PreflightError):
                    self.verify()
                native.assert_not_called()
            target.unlink()
        (self.backup / "pg_tblspc/12345").mkdir()
        with self.assertRaises(PreflightError):
            self.verify()
        self.assertEqual(other.read_bytes(), b"preserve")

    def test_native_failure_timeout_and_stderr_remain_incomplete(self):
        for change in ({"rc": 1, "stderr": "WAL missing"},
                       {"rc": None, "timed_out": True},
                       {"stderr": "untrusted warning"}, {"truncated": True}):
            def fail(argv):
                return {**self.native(argv), **change}
            with self.subTest(change=change), patch.object(self.seed, "run_native", side_effect=fail):
                result = self.verify()
            self.assertEqual(result["status"], "ERROR")
            self.assertFalse(result["bootstrap_ready"])
            for key, value in change.items():
                self.assertEqual(result["commands"][0][key], value)

    def test_wrong_system_identity_and_crc_warning_cannot_verify_backup(self):
        for change in ("wrong-id", "crc-warning"):
            def native(argv):
                result = self.native(argv)
                if Path(argv[0]).name == "pg_controldata":
                    if change == "wrong-id":
                        result["stdout"] = result["stdout"].replace(self.system_id, "123456")
                    else:
                        result["stdout"] = "WARNING: Calculated CRC checksum does not match value stored in file.\n" + result["stdout"]
                return result
            with self.subTest(change=change), patch.object(self.seed, "run_native", side_effect=native):
                result = self.verify()
            self.assertNotEqual(result["status"], "PASS")
            self.assertFalse(result["bootstrap_ready"])

    def test_mutation_during_native_verification_invalidates_result(self):
        def native(argv):
            result = self.native(argv)
            if Path(argv[0]).name == "pg_verifybackup":
                (self.backup / "global/pg_control").write_text("changed during verification")
            return result
        with patch.object(self.seed, "run_native", side_effect=native):
            result = self.verify()
        self.assertEqual(result["state"], "BACKUP_CHANGED")
        self.assertNotEqual(result["status"], "PASS")
        self.assertFalse(result["bootstrap_ready"])

    def test_checksum_free_manifest_is_refused_even_if_native_would_succeed(self):
        for change in ({"Checksum-Algorithm": "NONE"}, {"Checksum": ""},
                       {"Checksum-Algorithm": "SHA256", "Checksum": "z" * 64}):
            document = json.loads(json.dumps(self.manifest_document))
            document["Files"][0].update(change)
            (self.backup / "backup_manifest").write_text(json.dumps(document))
            self.manifest = self.sha(self.backup / "backup_manifest")
            with self.subTest(change=change), patch.object(self.seed, "run_native", side_effect=self.native) as native:
                with self.assertRaisesRegex(PreflightError, "BACKUP_CHECKSUMS_REQUIRED"):
                    self.verify()
                native.assert_not_called()
        document["Files"][0].pop("Checksum-Algorithm")
        document["Files"][0].pop("Checksum")
        (self.backup / "backup_manifest").write_text(json.dumps(document))
        self.manifest = self.sha(self.backup / "backup_manifest")
        with patch.object(self.seed, "run_native", side_effect=self.native) as native:
            with self.assertRaisesRegex(PreflightError, "BACKUP_CHECKSUMS_REQUIRED"):
                self.verify()
            native.assert_not_called()

    def test_output_inside_backup_is_refused_without_native_calls_or_mutation(self):
        before = self.seed.inventory(self.backup)
        for output in (self.backup / "result.json", self.backup / "global/result.json"):
            with self.subTest(output=output), patch.object(self.seed, "run_native", side_effect=self.native) as native, \
                    patch("sys.stdout", new_callable=io.StringIO) as stdout:
                rc = self.seed.main(["verify-backup", "--backup", str(self.backup),
                                     "--install-root", str(self.install), "--binary-sha256", self.binary,
                                     "--manifest-sha256", self.manifest, "--system-identifier", self.system_id,
                                     "--out", str(output)])
                self.assertEqual(rc, 2)
                self.assertEqual(json.loads(stdout.getvalue())["reason"], "BACKUP_OUTPUT_OVERLAP")
                native.assert_not_called()
            self.assertEqual(self.seed.inventory(self.backup), before)


if __name__ == "__main__":
    unittest.main()
