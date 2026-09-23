"""Real-syscall probe tests on a local filesystem, not GFS2 qualification.

Author: SqlRush <sqlrush@gmail.com>
"""

import json
import os
from pathlib import Path
import selectors
import subprocess
import sys
import tempfile
import unittest


SOURCE = Path(__file__).resolve().parents[1] / "storage_probe.c"
TOKEN = "0123456789abcdef0123456789abcdef"


class StorageProbeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.build = tempfile.TemporaryDirectory(prefix="pre1-probe-build-")
        cls.binary = Path(cls.build.name) / "storage_probe"
        subprocess.run([os.environ.get("CC", "cc"), "-std=c11", "-Wall", "-Wextra",
                        "-Werror", "-O2", str(SOURCE), "-o", str(cls.binary)], check=True)

    @classmethod
    def tearDownClass(cls):
        cls.build.cleanup()

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="pre1-storage-tests-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve() / ("pre1-probe-" + TOKEN)
        self.run_case("init")

    def argv(self, case, *extra, root=None):
        return [str(self.binary), "--case", case, "--root", str(root or self.root),
                "--token", TOKEN, "--node", "0", *extra]

    def run_case(self, case, *extra, rc=0, root=None):
        result = subprocess.run(self.argv(case, *extra, root=root), capture_output=True,
                                text=True, timeout=5)
        self.assertEqual(result.returncode, rc, result.stdout + result.stderr)
        self.assertEqual(result.stderr, "")
        events = [json.loads(line) for line in result.stdout.splitlines()]
        self.assertTrue(events)
        for event in events:
            for key in ("syscall", "errno", "offset", "length", "crc32", "token", "node", "mono_ns"):
                self.assertIn(key, event)
        return events

    def start(self, case, *extra):
        process = subprocess.Popen(self.argv(case, *extra), stdin=subprocess.PIPE,
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                                   bufsize=1)
        def cleanup():
            if process.poll() is None:
                process.kill()
            process.communicate()
        self.addCleanup(cleanup)
        # Read one byte at a time below: TextIOWrapper can prefetch lines,
        # which would make select() miss buffered READY records.
        raw = bytearray()
        with selectors.DefaultSelector() as selector:
            selector.register(process.stdout, selectors.EVENT_READ)
            while True:
                self.assertTrue(selector.select(5), "probe did not reach READY")
                byte = os.read(process.stdout.fileno(), 1)
                self.assertTrue(byte, "probe exited before READY: " + raw.decode())
                raw += byte
                if byte == b"\n":
                    event = json.loads(raw)
                    raw.clear()
                    if event.get("status") == "READY":
                        break
        return process

    def release(self, process, command=None):
        stdout, stderr = process.communicate(command or f"GO {TOKEN}\n", timeout=5)
        self.assertEqual(process.returncode, 0, stdout + stderr)
        self.assertEqual(stderr, "")
        return [json.loads(line) for line in stdout.splitlines()]

    def test_create_write_read_and_no_reinitialization(self):
        self.run_case("write", "--sequence", "7")
        events = self.run_case("read", "--sequence", "7")
        self.assertEqual(events[-1]["status"], "PASS")
        self.run_case("read", "--sequence", "8", rc=1)
        self.run_case("init", rc=1)
        self.run_case("read", "--sequence", "7")

    def test_both_lock_apis_conflict_then_handoff(self):
        for api in ("flock", "fcntl"):
            with self.subTest(api=api):
                holder = self.start(api + "-hold")
                blocked = self.run_case(api + "-try", rc=4)
                self.assertEqual(blocked[-1]["status"], "CONFLICT")
                self.release(holder)
                self.run_case(api + "-try")

    def test_fcntl_disjoint_ranges_can_coexist(self):
        holder = self.start("fcntl-hold", "--offset", "0", "--length", "16")
        self.run_case("fcntl-try", "--offset", "8", "--length", "16", rc=4)
        self.run_case("fcntl-try", "--offset", "16", "--length", "16")
        self.release(holder)

    def test_normal_exit_without_unlock_releases_lock(self):
        holder = self.start("flock-exit")
        self.run_case("flock-try", rc=4)
        self.release(holder)
        self.run_case("flock-try")

    def test_cached_existing_fd_and_new_fd_see_remote_write_after_barrier(self):
        self.run_case("write", "--sequence", "1")
        reader = self.start("cache-reader", "--sequence", "1")
        self.run_case("write", "--sequence", "2", "--writer", "1")
        events = self.release(reader, f"GO {TOKEN} 2 1\n")
        self.assertEqual(sum(e["syscall"] == "verify" for e in events), 2)

    def test_extend_and_truncate_visibility(self):
        self.run_case("write")
        self.run_case("resize", "--length", "16384")
        self.run_case("read", "--length", "16384")
        self.run_case("resize", "--length", "256")
        self.run_case("read", "--length", "256")

    def test_rename_is_new_object_but_existing_fd_keeps_old_object(self):
        self.run_case("write", "--sequence", "1")
        reader = self.start("rename-reader", "--sequence", "1")
        self.run_case("rename", "--sequence", "2", "--writer", "1")
        self.release(reader, f"GO {TOKEN} 2 1\n")
        self.run_case("read", "--sequence", "2", "--writer", "1")

    def test_unlink_preserves_open_fd_and_removes_name(self):
        self.run_case("write", "--sequence", "1")
        reader = self.start("unlink-reader", "--sequence", "1")
        self.run_case("unlink")
        self.release(reader)
        self.assertFalse((self.root / "data").exists())

    def test_missing_or_wrong_marker_never_writes(self):
        (self.root / ".pre1-manifest").write_text("wrong")
        self.run_case("write", rc=1)
        self.assertFalse((self.root / "data").exists())

    def test_symlink_files_and_paths_are_rejected_without_modifying_target(self):
        target = Path(self.temp.name) / "precious"
        target.write_bytes(b"keep")
        (self.root / "data").symlink_to(target)
        self.run_case("write", rc=1)
        self.assertEqual(target.read_bytes(), b"keep")
        alias = Path(self.temp.name) / "alias"
        alias.symlink_to(self.root, target_is_directory=True)
        self.run_case("write", rc=2, root=alias)

    def test_hardlinked_file_is_rejected(self):
        target = Path(self.temp.name) / "precious"
        target.write_bytes(b"keep")
        os.link(target, self.root / "data")
        self.run_case("write", rc=1)
        self.assertEqual(target.read_bytes(), b"keep")

    def test_capacity_requires_exact_small_filesystem_before_creating_data(self):
        events = self.run_case("capacity-fill", "--capacity-bytes", "2097152",
                               "--capacity-device", str(self.root.stat().st_dev), rc=1)
        self.assertEqual(events[-1]["syscall"], "capacity-filesystem-identity")
        self.assertFalse((self.root / "data").exists())

    def test_capacity_missing_or_extra_authority_arguments_are_rejected(self):
        self.run_case("capacity-fill", rc=2)
        self.run_case("capacity-fill", "--capacity-bytes", "1073741825",
                      "--capacity-device", "1", rc=2)
        self.run_case("write", "--capacity-bytes", "2097152",
                      "--capacity-device", "1", rc=2)
        self.assertFalse((self.root / "data").exists())

    @unittest.skipUnless(sys.platform == "linux" and os.getuid() == 0
                         and os.environ.get("PGRAC_PRE1_CAPACITY_TEST") == "1",
                         "explicit privileged isolated-tmpfs capacity test")
    def test_capacity_real_enospc_and_no_overwrite(self):
        mount = Path(tempfile.mkdtemp(prefix="pre1-capacity-test-"))
        subprocess.run(["mount", "-t", "tmpfs", "-o", "size=2m,mode=0700",
                        "pre1-capacity-test", str(mount)], check=True)
        def cleanup():
            subprocess.run(["umount", str(mount)], check=True)
            mount.rmdir()
        self.addCleanup(cleanup)
        root = mount / ("pre1-probe-" + TOKEN)
        self.run_case("init", root=root)
        geometry = os.statvfs(root)
        args = ("--capacity-bytes", str(geometry.f_blocks * geometry.f_frsize),
                "--capacity-device", str(root.stat().st_dev))
        # Even on a small eligible filesystem, wrong device identity writes nothing.
        self.run_case("capacity-fill", "--capacity-bytes", args[1],
                      "--capacity-device", str(root.stat().st_dev + 1), root=root, rc=1)
        self.assertFalse((root / "data").exists())
        events = self.run_case("capacity-fill", *args, root=root, rc=4)
        self.assertEqual(events[-1]["status"], "EXPECTED_INJECTION")
        exhausted = [e for e in events if e["status"] == "EXPECTED_ENOSPC"]
        self.assertTrue(exhausted)
        self.assertEqual(exhausted[0]["errno"], 28)
        self.assertGreater(exhausted[0]["offset"], 0)
        self.assertFalse(any(e["status"] == "PASS" for e in events))
        saved = (root / "data").read_bytes()
        self.run_case("capacity-fill", *args, root=root, rc=1)
        self.assertEqual((root / "data").read_bytes(), saved)

    def test_invalid_arguments_are_safe_and_do_not_create_directories(self):
        for extra in (("--sequence", "-1"), ("--length", "4294967296"),
                      ("--node", "4"), ("--case", "DO_NOT_PRINT_SECRET")):
            result = subprocess.run(self.argv("write", *extra), capture_output=True, text=True)
            self.assertEqual(result.returncode, 2)
            self.assertNotIn("DO_NOT_PRINT_SECRET", result.stdout + result.stderr)
        self.run_case("write", rc=2, root=Path(self.temp.name) / "existing-data")

    def test_eof_and_wrong_barrier_token_are_incomplete_not_success(self):
        for command in ("", "GO wrong\n"):
            holder = self.start("flock-hold")
            stdout, stderr = holder.communicate(command, timeout=5)
            self.assertEqual(holder.returncode, 3, stdout + stderr)
            self.assertEqual(json.loads(stdout.splitlines()[-1])["status"], "INCOMPLETE")
            self.run_case("flock-try")

    def test_unchanged_cache_or_rename_version_is_not_a_witness(self):
        for case in ("cache-reader", "rename-reader"):
            self.run_case("write", "--sequence", "1")
            reader = self.start(case, "--sequence", "1")
            stdout, stderr = reader.communicate(f"GO {TOKEN} 1 0\n", timeout=5)
            self.assertEqual(reader.returncode, 3, stdout + stderr)
            self.assertEqual(json.loads(stdout.splitlines()[-1])["status"], "INCOMPLETE")

    def test_offset_is_not_misrepresented_for_data_operations(self):
        self.run_case("write", "--offset", "4096", rc=2)
        held = self.start("fcntl-hold", "--offset", "16", "--length", "16")
        events = self.run_case("fcntl-try", "--offset", "16", "--length", "16", rc=4)
        self.assertEqual(events[-1]["offset"], 16)
        self.assertTrue(all(e["offset"] == 0 for e in events if e["syscall"] == "pread"))
        self.release(held)

    def test_distinct_versions_cannot_alias_the_generated_payload(self):
        # These tuples collided in a seed-only XOR encoding. No write occurs
        # between READY and GO; neither tuple advancement nor CRC alone proves it.
        self.run_case("write", "--sequence", "133446002", "--writer", "2")
        reader = self.start("cache-reader", "--sequence", "133446002", "--writer", "2")
        stdout, stderr = reader.communicate(f"GO {TOKEN} 1000000000 0\n", timeout=5)
        self.assertEqual(reader.returncode, 1, stdout + stderr)
        self.assertEqual(json.loads(stdout.splitlines()[-1])["syscall"], "content-mismatch")
        self.run_case("cache-reader", "--length", "0", rc=2)

    def test_fence_writer_retains_lock_and_syncs_progress_without_timeout_pass(self):
        holder = self.start("fence-writer", "--iterations", "4")
        self.run_case("flock-try", rc=4)
        stdout, stderr = holder.communicate(timeout=5)
        self.assertEqual(holder.returncode, 3, stdout + stderr)
        events = [json.loads(line) for line in stdout.splitlines()]
        progress = [e["result"] for e in events if e["syscall"] == "fence-progress"]
        self.assertEqual(progress, [2, 3, 4])
        self.assertEqual(events[-1]["syscall"], "witness-budget-exhausted")
        self.assertEqual(events[-1]["status"], "INCOMPLETE")
        self.run_case("flock-try")
        seen = self.run_case("observe")
        self.assertEqual(next(e["result"] for e in seen if e["syscall"] == "observed-version"), 4)

    def test_fence_writer_kill_is_not_itself_an_isolation_certificate(self):
        holder = self.start("fence-writer")
        self.run_case("flock-try", rc=4)
        holder.kill()  # Exact local scratch child only; not a VM/database test.
        stdout, _ = holder.communicate(timeout=5)
        self.assertLess(holder.returncode, 0)
        self.assertNotIn('"status":"PASS"', stdout)
        self.run_case("flock-try")
        self.run_case("observe")

    def test_fence_writer_reentry_does_not_overwrite_existing_scratch_payload(self):
        self.run_case("write", "--sequence", "7")
        before = (self.root / "data").read_bytes()
        self.run_case("fence-writer", "--iterations", "1", rc=1)
        self.assertEqual((self.root / "data").read_bytes(), before)

    def test_fence_arguments_and_observed_corruption_are_rejected(self):
        for extra in (("--iterations", "0"), ("--iterations", "1201"),
                      ("--writer", "1"), ("--sequence", "1000000000")):
            self.run_case("fence-writer", *extra, rc=2)
        self.run_case("write", "--iterations", "1", rc=2)
        self.run_case("write", "--sequence", "2")
        payload = bytearray((self.root / "data").read_bytes())
        payload[47] ^= 1
        (self.root / "data").write_bytes(payload)
        self.run_case("observe", rc=1)


if __name__ == "__main__":
    unittest.main()
