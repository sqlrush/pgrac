"""Driver unit tests use a fake command boundary; never count as acceptance."""
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from blackbox import BlackBox, Dependency


class BlackBoxTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.calls = []
        self.layout = dict(root=str(self.root), name="test", nodes=[
            dict(id=n, data_dir=str(self.root / f"node{n}"), port=21000+n,
                 host="localhost", logfile=str(self.root / f"node{n}.log"))
            for n in range(4)])

    def runner(self, argv, **kwargs):
        self.calls.append((argv, kwargs))
        return subprocess.CompletedProcess(argv, 0, "--cluster-seed --cluster-join --join-from-backup\n", "")

    def driver(self, profile=None):
        return BlackBox(self.layout, self.root, profile, runner=self.runner)

    def profile(self):
        return dict(version=1, initialize=[dict(tool="pgrac-init", argv=[
            "--cluster-seed", "-D", "${node0_data_dir}", "--topology=${layout_file}"])],
            operations={"open_observation": dict(node=0, sql="SELECT product_observation()")})

    def test_missing_profile_probes_actual_product_entry(self):
        with self.assertRaisesRegex(Dependency, "S11/S12"):
            self.driver().describe()
        self.assertEqual(Path(self.calls[0][0][0]).name, "pgrac-init")
        self.assertEqual(self.calls[0][0][1:], ["--help"])
        self.assertEqual(len(self.calls), 1)

    def test_no_shell_backup_or_literal_observation(self):
        for bad in (dict(tool="sh", argv=["-c", "echo yes"]),
                    dict(tool="pgrac-init", argv=["--cluster-seed", "--join-from=x"]),
                    dict(tool="pgrac-init", argv=["--cluster-seed", "--force"])):
            profile = self.profile()
            profile["initialize"] = [bad]
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                self.driver(profile).describe()
        profile = self.profile()
        profile["operations"]["open_observation"] = dict(result={"phase": "OPEN"})
        with self.assertRaises(ValueError):
            self.driver(profile).describe()

    def test_existing_data_rejected_before_mutation(self):
        (self.root / "node0").mkdir()
        (self.root / "node0/PG_VERSION").write_text("18\n")
        driver = self.driver(self.profile())
        with self.assertRaisesRegex(ValueError, "not empty"):
            driver.fresh_init()
        self.assertEqual(self.calls, [])

    def test_expansion_is_argv_not_shell(self):
        driver = self.driver(self.profile())
        self.assertEqual(driver.expand(["--x=${args_json}"], {"value": "$(false); 'quoted'"}),
                         ["--x=" + json.dumps({"value": "$(false); 'quoted'"}, sort_keys=True)])
        with self.assertRaisesRegex(ValueError, "unknown placeholder"):
            driver.expand(["${missing}"], {})

    def test_initializer_cannot_name_an_existing_external_tree(self):
        profile = self.profile()
        profile["initialize"][0]["argv"] = ["--cluster-seed", "-D", "/unrelated-database"]
        with self.assertRaisesRegex(ValueError, "outside this disposable fixture"):
            self.driver(profile).fresh_init()
        self.assertEqual(self.calls, [])

    def test_status_must_be_native_exit_three(self):
        driver = self.driver(self.profile())
        self.assertFalse(driver.is_stopped(self.layout["nodes"][0]))
        self.assertEqual(self.calls[-1][0][1], "status")

    def test_late_sql_success_is_not_ready(self):
        driver = self.driver(self.profile())
        clock = [0]
        def runner(argv, **kwargs):
            if Path(argv[0]).name == "psql" and str(self.layout["nodes"][3]["port"]) in argv:
                clock[0] = 61
            return subprocess.CompletedProcess(argv, 0, "1\n", "")
        driver.runner = runner
        with patch("blackbox.time.monotonic", side_effect=lambda: clock[0]):
            with self.assertRaisesRegex(RuntimeError, "deadline"):
                driver.shared_start()

    def prepare_stop(self):
        driver = self.driver(self.profile())
        for node in self.layout["nodes"]:
            data = Path(node["data_dir"])
            data.mkdir()
            (data / "postmaster.pid").write_text(str(100 + node["id"]) + "\n")
            Path(node["logfile"]).write_text("database system is shut down\n")
        driver.state_path.write_text(json.dumps({"log_offsets": {str(n): 0 for n in range(4)}}))
        stopped = set()
        def runner(argv, **kwargs):
            self.calls.append((argv, kwargs))
            node = argv[argv.index("-D") + 1]
            if argv[1] == "stop":
                stopped.add(node)
            return subprocess.CompletedProcess(argv, 3 if argv[1] == "status" and node in stopped else 0, "", "")
        driver.runner = runner
        processes = {100 + n: (1, f"postgres-{n}") for n in range(4)}
        processes[200] = (100, "cluster-worker")
        return driver, processes

    def test_orphan_worker_prevents_normal_stop(self):
        driver, processes = self.prepare_stop()
        clock = [0]
        def process_snapshot():
            if any(call[0][1] == "stop" for call in self.calls):
                clock[0] = 61
                return {200: (1, "cluster-worker")}
            return processes
        with patch.object(driver, "processes", side_effect=process_snapshot), \
             patch("blackbox.time.monotonic", side_effect=lambda: clock[0]):
            with self.assertRaisesRegex(RuntimeError, "deadline"):
                driver.shared_stop({})
        stops = [call[0] for call in self.calls if call[0][1] == "stop"]
        self.assertEqual(len(stops), 4)
        self.assertTrue(all("fast" in argv and "immediate" not in argv for argv in stops))

    def test_normal_stop_and_crash_log(self):
        for crash in (False, True):
            # Recreate this unit-only filesystem fixture for each subcase.
            if crash:
                for node in self.layout["nodes"]:
                    (Path(node["data_dir"]) / "postmaster.pid").unlink()
                    Path(node["data_dir"]).rmdir()
            driver, processes = self.prepare_stop()
            if crash:
                Path(self.layout["nodes"][0]["logfile"]).write_text(
                    "server process was terminated by signal 6\ndatabase system is shut down\n")
            with patch.object(driver, "processes", side_effect=[processes, {}]):
                if crash:
                    with self.assertRaisesRegex(RuntimeError, "abnormal process exit"):
                        driver.shared_stop({})
                else:
                    self.assertTrue(driver.shared_stop({})["all_processes_exited"])


if __name__ == "__main__":
    unittest.main()
