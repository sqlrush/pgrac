"""New-target seed distribution tests; no live deployment qualification.

Author: SqlRush <sqlrush@gmail.com>
"""

import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import seed
from common import PreflightError, document_sha


class SeedCloneTests(unittest.TestCase):
    def setUp(self):
        path = Path(__file__).resolve().parents[1] / "seed_clone.py"
        self.assertTrue(path.exists(), "guarded seed clone is missing")
        spec = importlib.util.spec_from_file_location("pre1_seed_clone", path)
        self.clone = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.clone)
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        for name in ("data", "backup", "install", "mount"):
            (self.root / name).mkdir(mode=0o700)
        self.node = dict(node_id=1, vm_uuid="00000000-0000-4000-8000-000000000001",
                         boot_id="00000000-0000-4000-8000-000000000002",
                         machine_id="a" * 32, pgdata=str(self.root / "data"),
                         install_root=str(self.root / "install"),
                         uid=os.getuid() or 10001, gid=os.getgid() or 10001)
        source_node = dict(self.node, node_id=0, vm_uuid="00000000-0000-4000-8000-000000001001",
                           boot_id="00000000-0000-4000-8000-000000001002", machine_id="b" * 32)
        self.source = dict(node=source_node, binary_sha256="a" * 64,
                           dataset_id="seed-test", shared_mount=str(self.root / "mount"),
                           fs_uuid="00000000-0000-4000-8000-000000000003")
        self.source_path = self.root / "source.json"
        self.source_path.write_text(json.dumps(self.source))
        self.artifact = dict(kind="pre1-seed-create", status="PASS", state="SEED_BACKUP_READY",
                             request_sha256=document_sha(self.source), dataset_id="seed-test",
                             seed_clean_stop=True, system_identifier="12345",
                             backup_verification=dict(status="PASS", manifest_sha256="b" * 64,
                                                      tree_sha256="c" * 64))
        self.artifact_path = self.root / "seed.json"
        self.artifact_path.write_text(json.dumps(self.artifact))
        self.request = dict(schema_version=1, action="clone-seed", node=self.node,
                            binary_sha256="a" * 64, backup=str(self.root / "backup"),
                            seed_request_path=str(self.source_path),
                            seed_request_sha256=seed.checked_hash(self.source_path)["sha256"],
                            seed_artifact_path=str(self.artifact_path),
                            seed_artifact_sha256=seed.checked_hash(self.artifact_path)["sha256"])
        self.out = self.root / "result.json"
        self.facts = dict(node_id=1, identity={k: self.node[k] for k in
                          ("vm_uuid", "boot_id", "machine_id")}, pgdata_state="EMPTY",
                          control=None, pidfile=None, processes=[])

    def validate(self):
        with patch.object(seed, "seed_guest_observation", return_value=self.facts), \
             patch.object(seed, "require_seed_mount"), \
             patch.object(seed, "verify_backup", return_value=self.artifact["backup_verification"]):
            return self.clone.validate_clone(self.request, self.out)

    def test_guarded_plain_backup_is_not_bootstrap_permission(self):
        context = self.validate()
        self.assertEqual(context["system_identifier"], "12345")
        self.assertEqual(context["dataset_id"], "seed-test")

    def test_existing_target_wrong_node_or_process_refuses_before_copy(self):
        (self.root / "data/preserve").write_text("old")
        with self.assertRaises(PreflightError):
            self.validate()
        self.assertEqual((self.root / "data/preserve").read_text(), "old")
        (self.root / "data/preserve").unlink()
        self.node["node_id"] = 0
        with self.assertRaises(PreflightError):
            self.validate()
        self.node["node_id"] = 1
        self.facts["processes"] = [{"pid": 123}]
        with self.assertRaises(PreflightError):
            self.validate()

    def test_changed_or_unclean_source_cannot_authorize_clone(self):
        self.artifact["seed_clean_stop"] = False
        self.artifact_path.write_text(json.dumps(self.artifact))
        with self.assertRaises(PreflightError):
            self.validate()
        self.request["seed_artifact_sha256"] = seed.checked_hash(self.artifact_path)["sha256"]
        with self.assertRaises(PreflightError):
            self.validate()

    def test_mismatched_backup_or_overlap_does_not_touch_target(self):
        with patch.object(seed, "seed_guest_observation", return_value=self.facts), \
             patch.object(seed, "require_seed_mount"), \
             patch.object(seed, "verify_backup", return_value=dict(status="ERROR")):
            with self.assertRaises(PreflightError):
                self.clone.validate_clone(self.request, self.out)
        self.request["backup"] = str(self.root / "data")
        with self.assertRaises(PreflightError):
            self.validate()
        self.assertFalse(any((self.root / "data").iterdir()))

    def test_copy_never_overwrites_or_follows_links(self):
        source, target = self.root / "from", self.root / "to"
        source.write_bytes(b"native bytes")
        self.clone.copy_file_new(source, target, self.node)
        self.assertEqual(target.read_bytes(), b"native bytes")
        with self.assertRaises((OSError, PreflightError)):
            self.clone.copy_file_new(source, target, self.node)
        link = self.root / "link"
        link.symlink_to(source)
        with self.assertRaises((OSError, PreflightError)):
            self.clone.copy_file_new(link, self.root / "new", self.node)
        self.assertEqual(source.read_bytes(), b"native bytes")

    def test_directory_links_and_preexisting_children_are_never_merged(self):
        source, target = self.root / "backup", self.root / "data"
        (source / "base").mkdir()
        (source / "base/page").write_bytes(b"native page")
        external = self.root / "external"
        external.mkdir(mode=0o750)
        (external / "preserve").write_bytes(b"retained")
        before = external.stat()
        (target / "base").symlink_to(external)
        # Exercise the new descriptor-based primitive at the race boundary,
        # after higher-level empty-directory checks would already have run.
        self.assertTrue(hasattr(self.clone, "copy_tree_new"), "directory-bound copy is missing")
        with self.assertRaises((OSError, PreflightError)):
            self.clone.copy_tree_new(source, target, self.node)
        self.assertEqual((external / "preserve").read_bytes(), b"retained")
        self.assertFalse((external / "page").exists())
        self.assertEqual(external.stat().st_mode, before.st_mode)
        self.assertEqual(external.stat().st_mtime_ns, before.st_mtime_ns)

    def test_plain_tree_copy_preserves_bytes_and_rejects_a_second_copy(self):
        source, target = self.root / "backup", self.root / "data"
        (source / "base").mkdir()
        (source / "base/page").write_bytes(b"native page")
        self.assertTrue(hasattr(self.clone, "copy_tree_new"), "directory-bound copy is missing")
        self.clone.copy_tree_new(source, target, self.node)
        self.assertEqual((target / "base/page").read_bytes(), b"native page")
        with self.assertRaises((OSError, PreflightError)):
            self.clone.copy_tree_new(source, target, self.node)

    def test_link_inserted_after_empty_check_cannot_escape(self):
        source, target = self.root / "backup", self.root / "data"
        (source / "base").mkdir()
        (source / "base/page").write_bytes(b"native page")
        external = self.root / "external"
        external.mkdir()
        target_inode, original, injected = target.stat().st_ino, os.listdir, [False]
        def interleave(fd):
            result = original(fd)
            if type(fd) is int and os.fstat(fd).st_ino == target_inode and not injected[0]:
                injected[0] = True
                (target / "base").symlink_to(external)
            return result
        with patch.object(os, "listdir", side_effect=interleave):
            with self.assertRaises((OSError, PreflightError)):
                self.clone.copy_tree_new(source, target, self.node)
        self.assertTrue(injected[0])
        self.assertFalse(any(external.iterdir()))


if __name__ == "__main__":
    unittest.main()
