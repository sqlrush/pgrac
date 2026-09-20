"""Behavior tests for customer demo safety and evidence.

Author: SqlRush <sqlrush@gmail.com>
"""

import importlib.util
import io
import json
import socket
from pathlib import Path
import sys
import tempfile
import unittest

MODULE = Path(__file__).resolve().parents[1] / "demo_lib.py"
core = None
if MODULE.exists():
    spec = importlib.util.spec_from_file_location("demo_lib", MODULE)
    core = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = core
    spec.loader.exec_module(core)


class DemoPolicies(unittest.TestCase):
    def setUp(self):
        self.assertIsNotNone(core, "demo safety policy is not implemented")

    def test_names_cannot_escape_storage_or_become_options(self):
        for name in ("../data", "", "-rf", "a/b", "a b", "x;id", "X", "a" * 33):
            with self.subTest(name=name), self.assertRaises(ValueError):
                core.validate_name(name)
        self.assertEqual(core.validate_name("customer-01"), "customer-01")

    def test_symlink_parent_or_target_is_rejected_without_touching_it(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            real = base / "real"
            real.mkdir()
            link = base / "link"
            link.symlink_to(real, target_is_directory=True)
            with self.assertRaises(ValueError):
                core.safe_root(link, "demo")
            (real / "demo").symlink_to(base, target_is_directory=True)
            with self.assertRaises(ValueError):
                core.safe_root(real, "demo")
            self.assertTrue((real / "demo").is_symlink())

    def test_container_paths_cannot_escape_owned_storage(self):
        for path in ("/etc/passwd", "/demo/../other", "/demo", "/demo/a//b"):
            with self.assertRaises(ValueError):
                core.storage_relative(path)
        self.assertEqual(core.storage_relative("/demo/tap/node/pgdata"),
                         Path("tap/node/pgdata"))

    def test_repeat_up_does_not_initialize_or_restart(self):
        self.assertEqual(core.start_action("READY", [0, 1, 2, 3], [], True), "verify")

    def test_only_clean_four_member_shutdown_allows_restart(self):
        self.assertEqual(core.start_action("CLEAN", [], [True] * 4, True), "restart")
        for state, running, clean in (("CLEAN", [], [True, True, False, True]),
                                      ("READY", [0, 1, 2], []),
                                      ("STARTING", [], [True] * 4),
                                      ("INITIALIZING", [], []),
                                      ("CLEAN", [1], [True] * 4)):
            with self.subTest(state=state), self.assertRaises(ValueError):
                core.start_action(state, running, clean, True)

    def test_new_image_cannot_silently_take_over_old_data(self):
        with self.assertRaises(ValueError):
            core.start_action("CLEAN", [], [True] * 4, False)

    def test_first_start_is_distinct_from_restart(self):
        self.assertEqual(core.start_action("INITIALIZED", [], [], True), "first")

    def test_reused_loop_device_is_not_owned(self):
        self.assertTrue(core.exact_loop("/dev/loop7", "/owned/vote0", "7:7", 525824,
                                        "/dev/loop7", "/owned/vote0", "7:7", 525824))
        self.assertFalse(core.exact_loop("/dev/loop7", "/owned/vote0", "7:7", 525824,
                                         "/dev/loop7", "/other/vote0", "7:7", 525824))

    def test_pgbench_output_preserves_failed_transactions_and_rc(self):
        text = ("number of transactions actually processed: 123\n"
                "number of failed transactions: 2 (1.6%)\n"
                "tps = 12.300000 (without initial connection time)\n")
        self.assertEqual(core.parse_pgbench(text, 3),
                         {"transactions": 123, "failed": 2, "tps": 12.3, "rc": 3})

    def test_missing_or_duplicate_benchmark_fields_are_not_zero_errors(self):
        for text in ("", "tps = 5.0\n", "number of transactions actually processed: 12\n",
                     self.output() + self.output()):
            with self.assertRaises(ValueError):
                core.parse_pgbench(text, 0)

    @staticmethod
    def output():
        return ("number of transactions actually processed: 10\n"
                "number of failed transactions: 0 (0.0%)\n"
                "tps = 1.000000 (without initial connection time)\n")

    @staticmethod
    def snapshots(total=0, digest="aaa"):
        return [{"rows": 100, "sum": total, "sha256": digest,
                 "key_payload_sha256": "bbb"} for _ in range(4)]

    def test_benchmark_pass_requires_actual_committed_sum_and_full_rows(self):
        nodes = [core.parse_pgbench(self.output(), 0) for _ in range(4)]
        result = core.benchmark_verdict(nodes, self.snapshots(), self.snapshots(40, "ccc"),
                                        [0] * 4, [True] * 4)
        self.assertEqual(result["verdict"], "PASS")
        self.assertEqual(result["transactions"], 40)
        self.assertEqual(result["reported_tps_sum"], 4.0)
        self.assertFalse(result["is_formal_pre"])

    def test_errors_bad_sums_missing_nodes_and_server_errors_fail(self):
        nodes = [core.parse_pgbench(self.output(), 0) for _ in range(4)]
        cases = [([dict(nodes[0], rc=2)] + nodes[1:], self.snapshots(40, "ccc"), [0]*4),
                 ([dict(nodes[0], failed=1)] + nodes[1:], self.snapshots(40, "ccc"), [0]*4),
                 (nodes, self.snapshots(39, "ccc"), [0]*4),
                 (nodes[:3], self.snapshots(40, "ccc"), [0]*4),
                 (nodes, self.snapshots(40, "ccc"), [0, 1, 0, 0])]
        for clients, after, errors in cases:
            with self.subTest(clients=clients, errors=errors):
                result = core.benchmark_verdict(clients, self.snapshots(), after,
                                                errors, [True] * 4)
                self.assertEqual(result["verdict"], "FAIL")
                self.assertTrue(result["reasons"])

    def test_equal_totals_do_not_hide_cross_node_different_rows(self):
        nodes = [core.parse_pgbench(self.output(), 0) for _ in range(4)]
        after = self.snapshots(40, "ccc")
        after[3]["sha256"] = "ddd"
        self.assertEqual(core.benchmark_verdict(nodes, self.snapshots(), after,
                                                [0]*4, [True]*4)["verdict"], "FAIL")

    def test_four_pods_have_no_automatic_restart_or_privileged_database(self):
        manifests = core.pod_documents("demo", "localhost/pgrac:test", "/owned/storage",
                                        ["/dev/loop7", "/dev/loop8", "/dev/loop9"])
        self.assertEqual(len(manifests), 4)
        self.assertEqual(len({p["metadata"]["name"] for p in manifests}), 4)
        for i, pod in enumerate(manifests):
            spec = pod["spec"]
            self.assertEqual(spec["restartPolicy"], "Never")
            db = spec["containers"][0]
            self.assertEqual(db["args"], ["node", str(i)])
            self.assertEqual(db["securityContext"]["runAsUser"], 10001)
            self.assertFalse(db["securityContext"].get("privileged", False))
            self.assertFalse(db["securityContext"]["allowPrivilegeEscalation"])
            devices = [v["hostPath"]["path"] for v in spec["volumes"]
                       if v.get("hostPath", {}).get("type") == "BlockDevice"]
            self.assertEqual(devices, ["/dev/loop7", "/dev/loop8", "/dev/loop9"])
            self.assertEqual(json.loads(json.dumps(pod)), pod)

    def test_population_refuses_existing_data_and_unbounded_geometry(self):
        self.assertTrue(callable(getattr(core, "population_rows", None)), "population guard missing")
        self.assertEqual(core.population_rows(0, 10000), 10000)
        for count, rows in ((1, 10000), (0, 0), (0, -1), (0, 1000001)):
            with self.assertRaises(ValueError):
                core.population_rows(count, rows)

    def test_row_snapshot_reads_all_rows_and_rejects_duplicate_keys(self):
        self.assertTrue(callable(getattr(core, "row_snapshot", None)), "full-row verifier missing")
        value = core.row_snapshot(io.BytesIO(b"1\t2\tx\n2\t4\ty\n"))
        self.assertEqual((value["rows"], value["sum"]), (2, 6))
        for data in (b"1\t2\tx\n1\t4\ty\n", b"2\t3\tx\n1\t4\ty\n", b"1\tx\ty\n"):
            with self.assertRaises(ValueError):
                core.row_snapshot(io.BytesIO(data))

    def test_port_check_allows_time_wait_but_rejects_a_listener(self):
        self.assertTrue(callable(getattr(core, "check_port_free", None)), "port check missing")
        with socket.socket() as server:
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind(("127.0.0.1", 0))
            port = server.getsockname()[1]
            server.listen()
            with self.assertRaises(OSError):
                core.check_port_free(port)
            with socket.create_connection(("127.0.0.1", port)) as client:
                accepted, _ = server.accept()
                accepted.close()
                self.assertEqual(client.recv(1), b"")
        core.check_port_free(port)

    def test_foreign_or_running_pod_is_not_removable(self):
        self.assertTrue(callable(getattr(core, "removable_pod", None)), "pod owner guard missing")
        owned = {"Name": "pgrac-demo-0", "State": "Exited", "Labels": {"pgrac-demo": "demo"}}
        self.assertTrue(core.removable_pod(owned, "demo", 0))
        self.assertFalse(core.removable_pod(dict(owned, Labels={}), "demo", 0))
        self.assertFalse(core.removable_pod(dict(owned, Name="other"), "demo", 0))
        self.assertFalse(core.removable_pod(dict(owned, State="Running"), "demo", 0))


if __name__ == "__main__":
    unittest.main()
