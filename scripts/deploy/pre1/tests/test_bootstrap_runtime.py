"""Initial-start guards do not authorize later recovery or restart.

Author: SqlRush <sqlrush@gmail.com>
"""

import importlib.util
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import PreflightError, document_sha


class BootstrapRuntimeTests(unittest.TestCase):
    def setUp(self):
        path = Path(__file__).resolve().parents[1] / "bootstrap_runtime.py"
        self.assertTrue(path.exists(), "guarded initial-start adapter is missing")
        spec = importlib.util.spec_from_file_location("pre1_bootstrap_runtime", path)
        self.module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.module)
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.node = dict(uid=os.getuid(), gid=os.getgid())

    def test_exclusive_configuration_never_overwrites_or_follows_link(self):
        target = self.root / "config"
        self.module.write_new(target, b"fsync = on\n", self.node)
        with self.assertRaises((OSError, PreflightError)):
            self.module.write_new(target, b"replacement", self.node)
        link = self.root / "link"
        link.symlink_to(target)
        with self.assertRaises((OSError, PreflightError)):
            self.module.write_new(link, b"replacement", self.node)
        self.assertEqual(target.read_bytes(), b"fsync = on\n")

    def test_config_delta_cannot_touch_native_control_or_wal(self):
        before = {"global/pg_control": {"sha256": "control"},
                  "postgresql.conf": {"sha256": "native"}}
        files = {name: "value" for name in self.module.CONFIG_NAMES}
        after = dict(before)
        after.update(self.module.config_entries(files))
        self.module.require_config_only(before, after, files)
        after["global/pg_control"] = {"sha256": "changed"}
        with self.assertRaises(PreflightError):
            self.module.require_config_only(before, after, files)

    def test_existing_runtime_configuration_is_not_a_new_attempt(self):
        files = {name: "value" for name in self.module.CONFIG_NAMES}
        before = self.module.config_entries(files)
        with self.assertRaises(PreflightError):
            self.module.require_config_only(before, before, files)

    def test_cold_inventory_rejects_socket_links_and_stale_pidfile(self):
        (self.root / "PG_VERSION").write_text("16\n")
        expected = self.module.cold_tree(self.root)
        self.assertEqual(expected["PG_VERSION"]["size"], 3)
        (self.root / "postmaster.pid").write_text("123\n")
        with self.assertRaises(PreflightError):
            self.module.cold_tree(self.root)
        (self.root / "postmaster.pid").unlink()
        (self.root / "linked").symlink_to(self.root / "PG_VERSION")
        with self.assertRaises(PreflightError):
            self.module.cold_tree(self.root)

    def test_fresh_clone_and_native_clean_seed_are_distinct(self):
        facts = dict(processes=[], pidfile=None, pgdata_state="INITIALIZED",
                     control=dict(parsed=dict(system_identifier="123", state="in production")))
        self.module.require_stopped_control(facts, "123", "in production")
        with self.assertRaises(PreflightError):
            self.module.require_stopped_control(facts, "123", "shut down")
        facts["processes"] = [{"pid": 123}]
        with self.assertRaises(PreflightError):
            self.module.require_stopped_control(facts, "123", "in production")

    def test_exact_stop_accepts_clean_restart_but_not_unproven_start(self):
        log = self.root / 'server.log'
        log.write_text('started\n')
        node = dict(node_id=0, vm_uuid='vm', machine_id='machine', boot_id='boot',
                    pgdata=str(self.root / 'data'), install_root='/install', **self.node)
        for kind in ('pre1-initial-start', 'pre1-clean-start', 'unproven'):
            artifact = dict(kind=kind, node=node, log=str(log),
                            process_identity=dict(pid=123, starttime=50, exe_sha256='a'*64),
                            request=dict(binary_sha256='a'*64))
            path = self.root / (kind+'.json')
            path.write_text(json.dumps(artifact))
            reference = dict(path=str(path), sha256=hashlib.sha256(path.read_bytes()).hexdigest())
            out = self.root / (kind+'-stop.json')
            with self.subTest(kind=kind), \
                 patch.object(self.module, 'observation', return_value=dict(pidfile=dict(pid=123))), \
                 patch.object(self.module.seed, 'stop_seed_exact',
                              return_value=dict(rc=0, timed_out=False, truncated=False)):
                if kind == 'unproven':
                    with self.assertRaises(PreflightError):
                        self.module.stop_exact(reference, out)
                    self.assertFalse(out.exists())
                else:
                    result = self.module.stop_exact(reference, out)
                    self.assertEqual(result['state'], 'EXACT_PROCESS_EXITED_CLOSURE_NOT_YET_PROVEN')
                    self.assertFalse(json.loads(out.read_text())['restart_allowed'])

    def test_attempt_marker_cannot_be_reused_after_failure(self):
        marker = self.root / "initial-attempt.json"
        self.module.write_new(marker, b"attempted", self.node)
        with self.assertRaises((OSError, PreflightError)):
            self.module.write_new(marker, b"retry", self.node)
        self.assertEqual(marker.read_bytes(), b"attempted")

    def test_output_cannot_alias_files_created_before_native_start(self):
        root, logs, shared = [self.root / name for name in ("data", "logs", "shared")]
        for path in (root, logs, shared):
            path.mkdir()
        node = dict(self.node, pgdata=str(root), log_root=str(logs))
        request = dict(binary_sha256="a" * 64)
        files = {name: "value" for name in self.module.CONFIG_NAMES}
        artifact = dict(kind="pre1-initial-config", status="PASS", state="CONFIGURED_NOT_STARTED",
                        request=request, request_sha256=document_sha(request), node=node,
                        after_tree_sha256=document_sha({}), files=self.module.config_entries(files))
        ctx = dict(node=node, source=dict(shared_mount=str(shared)), files=files)
        for name in ("initial-start-attempt.json", "initial-start.log"):
            with self.subTest(name=name), \
                 patch.object(self.module, "read_bound", return_value=artifact), \
                 patch.object(self.module, "context", return_value=ctx), \
                 patch.object(self.module, "write_new") as write, \
                 patch.object(self.module.seed, "run_native") as start:
                with self.assertRaises(PreflightError):
                    self.module.start_initial({}, logs / name)
                write.assert_not_called()
                start.assert_not_called()

    def test_replaced_socket_directory_cannot_follow_chown(self):
        external = self.root / "external"
        external.mkdir(mode=0o755)
        logs = self.root / "logs"
        logs.mkdir()
        real_mkdir = os.mkdir
        def swap(path, mode=0o777, **kwargs):
            real_mkdir(path, mode, **kwargs)
            if path == "socket":
                (logs / "socket").rmdir()
                (logs / "socket").symlink_to(external)
        self.assertTrue(hasattr(self.module, "make_socket_new"), "nofollow socket creation is missing")
        with patch.object(os, "mkdir", side_effect=swap), \
             patch.object(os, "fchown") as owner, patch.object(os, "chown") as path_owner:
            with self.assertRaises((OSError, PreflightError)):
                self.module.make_socket_new(logs, self.node)
            owner.assert_not_called()
            path_owner.assert_not_called()


if __name__ == "__main__":
    unittest.main()
