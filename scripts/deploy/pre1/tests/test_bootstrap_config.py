"""Fixed PRE1 configuration rendering is not live admission.

Author: SqlRush <sqlrush@gmail.com>
"""

import copy
import importlib.util
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import PreflightError
from test_profile import fixture


class BootstrapConfigTests(unittest.TestCase):
    def setUp(self):
        path = Path(__file__).resolve().parents[1] / "bootstrap_config.py"
        self.assertTrue(path.exists(), "fixed runtime configuration renderer is missing")
        spec = importlib.util.spec_from_file_location("pre1_bootstrap_config", path)
        self.module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.module)
        self.request = dict(schema_version=1, profile_id="pre1-gfs2-arm64-lab-v1",
                            nodes=fixture()["nodes"], cluster_name="pre1_main",
                            shared_root="/srv/pgrac/shared/main", controller_addr="192.0.2.1",
                            voting_wwids=["360014056bfe500009214000800000000",
                                          "360014056bfe600009214000800000000",
                                          "360014056bfe700009214000800000000"])

    def test_canonical_settings_and_three_private_planes(self):
        result = self.module.render(self.request)
        self.assertEqual(len(result), 4)
        for i, files in enumerate(result):
            conf = files["pre1-runtime.conf"]
            for required in ("cluster.quorum_poll_interval_ms = 2000", "cluster.write_fence_lease_ms = 60000",
                             "cluster.controlfile_shared_authority = off", "cluster.shared_catalog = off",
                             "cluster.wal_threads_dir = ''", "fsync = on", "cluster.lms_workers = 2",
                             "max_connections = 512", "shared_buffers = 1GB", "restart_after_crash = off"):
                self.assertIn(required, conf)
            self.assertIn(f"cluster.node_id = {i}", conf)
            self.assertIn(f"[node.{i}]", files["pgrac.conf"])
            self.assertIn("192.0.2.1/32", files["pre1-hba.conf"])
            self.assertNotIn("0.0.0.0", "".join(files.values()))
            self.assertNotIn("127.0.0.1", files["pgrac.conf"])
            self.assertNotIn("/dev/sd", conf)
            self.assertNotIn("include", conf)

    def test_unknown_fields_injection_and_global_trust_are_rejected(self):
        for field, value in (("shared_root", "/srv/pgrac/data'\nfsync=off"),
                             ("cluster_name", "x\n[node.8]"), ("controller_addr", "0.0.0.0"),
                             ("profile_id", "unreviewed"), ("timeout", 99999)):
            request = copy.deepcopy(self.request)
            request[field] = value
            with self.subTest(field=field), self.assertRaises(PreflightError):
                self.module.render(request)

    def test_duplicate_nodes_devices_and_overlap_are_refused(self):
        for kind in ("node", "device", "port", "shared"):
            request = copy.deepcopy(self.request)
            if kind == "node":
                request["nodes"][1]["vm_uuid"] = request["nodes"][0]["vm_uuid"]
            elif kind == "device":
                request["voting_wwids"][1] = request["voting_wwids"][0]
            elif kind == "port":
                request["nodes"][1]["control_addr"] = request["nodes"][1]["data_base_addr"]
            else:
                request["shared_root"] = request["nodes"][0]["pgdata"]
            with self.subTest(kind=kind), self.assertRaises(PreflightError):
                self.module.render(request)


if __name__ == "__main__":
    unittest.main()
