"""Actual C protected invocation with process boundaries and real TLS/registry.

Author: SqlRush <sqlrush@gmail.com>
Run on Linux as root, providing real C client driver and map verifier paths.
No real guest/export changes: only libvirt/configfs are fixture boundaries.
"""

import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import unittest

import run_target_client_runtime as client_tests
import run_target_registry as registry_tests
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from target_journal import TargetJournal, _canonical


class CClientTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if sys.platform != "linux" or os.geteuid() != 0:
            raise AssertionError("Linux root is required, not skipped")
        client_tests.ClientRuntimeTests.setUpClass()

    def setUp(self):
        self.c = client_tests.ClientRuntimeTests()
        self.addCleanup(self.c.doCleanups)
        self.c.setUp()
        self.cli = self.c.installed_cli()

    def call(self, payload=None, *, bundle=None, config=None, deadline=None, pass_fds=(), env=None):
        return subprocess.run([EXECUTABLE, str(self.cli.parent if bundle is None else bundle),
            str(self.c.path if config is None else config),
            str(time.monotonic_ns() + 3_000_000_000 if deadline is None else deadline)],
            input=_canonical(self.c.command()) if payload is None else payload,
            capture_output=True, timeout=6, pass_fds=pass_fds, env=env)

    def refused(self, result):
        self.assertEqual((result.returncode, result.stdout, result.stderr), (1, b"", b""))

    def test_actual_root_tls_and_durable_prepare(self):
        self.c.s.start()
        result = self.call()
        self.assertEqual((result.returncode, result.stderr), (0, b""))
        self.assertEqual(json.loads(result.stdout)["status"], "IDENTITY_ONLY")
        result = self.call(_canonical(self.c.command("prepare_deny")))
        self.assertEqual((result.returncode, result.stderr), (0, b""))
        self.assertEqual(json.loads(result.stdout)["status"], "DENY_RECORDED")
        self.assertEqual(self.c.s.stop(), 0)
        with TargetJournal(str(self.c.s.state), self.c.s.registry.inventory_digest) as journal:
            self.assertEqual(journal.denied()[0].identity.operation_id, "ad" * 16)

    def test_wrong_peer_and_malformed_command_are_empty_local_failure(self):
        self.c.s.start()
        for payload in (b"x" * 1025, b"", b"{}\n", b"{}\n{}\n"):
            self.refused(self.call(payload))
        self.c.document["endpoint"]["certificate_sha256"] = "ef" * 32
        self.c.write()
        self.refused(self.call())
        self.assertFalse((self.c.s.state / "native-opened").exists())
        self.assertEqual(self.c.s.stop(), 0)

    def test_unsafe_config_or_bundle_never_connects(self):
        link = self.c.s.r.directory / "bundle-link"
        link.symlink_to(self.cli.parent, target_is_directory=True)
        self.refused(self.call(bundle=link))
        self.refused(self.call(bundle="relative"))
        self.refused(self.call(config="relative"))
        self.c.path.chmod(0o644)
        self.refused(self.call())
        self.c.path.chmod(0o600)
        self.cli.chmod(0o777)
        self.refused(self.call())
        self.assertFalse((self.c.s.state / "native-opened").exists())

    def fake_bundle(self, body):
        directory = self.c.s.r.directory / "process-fixture"
        directory.mkdir(mode=0o700)
        entry = directory / "pgrac-target-service"
        entry.write_text(body)
        entry.chmod(0o700)
        return directory

    def test_fixed_argv_clean_environment_and_descriptors(self):
        # Only this trusted native-process boundary is a fixture. It cannot
        # certify anything; the actual TLS path is exercised above.
        fd = os.open(self.c.path, os.O_RDONLY)
        try:
            body = ("import os,sys\n"
                    "assert sys.flags.isolated and sys.flags.dont_write_bytecode\n"
                    "assert sys.argv[1:3] == ['--request','--config']\n"
                    "assert sys.argv[4] == '--deadline-ns'\n"
                    "assert 'PYTHONPATH' not in os.environ and 'SSLKEYLOGFILE' not in os.environ\n"
                    f"assert not os.path.exists('/proc/self/fd/{fd}')\n"
                    "assert os.getpgrp() == os.getppid()\n"
                    "data = sys.stdin.buffer.read()\n"
                    "assert data == b'{}\\n'\n"
                    "sys.stdout.buffer.write(b'fixture-only\\n')\n")
            bundle = self.fake_bundle(body)
            result = self.call(b"{}\n", bundle=bundle, pass_fds=(fd,),
                               env={"PYTHONPATH": "/untrusted", "SSLKEYLOGFILE": "/not-allowed"})
            self.assertEqual((result.returncode, result.stdout, result.stderr), (0, b"fixture-only\n", b""))
        finally:
            os.close(fd)

    def test_nonzero_partial_oversize_or_lingering_child_never_succeeds(self):
        bundle = self.fake_bundle("")
        entry = bundle / "pgrac-target-service"
        cases = (
            "import sys; sys.stdout.write('partial'); sys.exit(1)",
            "import os; os.write(1,b'x'*4097)",
            "import os,time; os.write(1,b'partial'); time.sleep(30)",
            "import os,time; os.close(1); time.sleep(30)",
            "import time; time.sleep(30)",
        )
        for body in cases:
            with self.subTest(body=body):
                entry.write_text(body + "\n")
                start = time.monotonic()
                self.refused(self.call(b"{}\n", bundle=bundle,
                                      deadline=time.monotonic_ns() + 250_000_000))
                self.assertLess(time.monotonic() - start, 2)

    def test_expired_before_launch_has_no_child_side_effect(self):
        marker = self.c.s.r.directory / "must-not-exist"
        bundle = self.fake_bundle(f"open({str(marker)!r},'w').write('bad')\n")
        self.refused(self.call(b"{}\n", bundle=bundle, deadline=time.monotonic_ns() - 1))
        self.assertFalse(marker.exists())

    def assert_not_running(self, pid):
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            try:
                # A just-adopted zombie is stopped, not an active helper.
                state = Path(f"/proc/{pid}/stat").read_text().rsplit(") ", 1)[1].split()[0]
                if state in ("Z", "X"):
                    return
            except FileNotFoundError:
                return
            time.sleep(0.005)
        self.fail("the exact fixture client survived its owner/deadline")

    def test_deadline_kills_client_without_extending_its_budget(self):
        marker = self.c.s.r.directory / "owned-pid"
        bundle = self.fake_bundle("import os,time\n"
            f"open({str(marker)!r},'w').write(str(os.getpid()))\n"
            "time.sleep(30)\n")
        self.refused(self.call(b"{}\n", bundle=bundle, deadline=time.monotonic_ns() + 250_000_000))
        self.assertTrue(marker.exists(), "must exercise a launched child, not preflight refusal")
        self.assert_not_running(int(marker.read_text()))

    def test_worker_death_kills_its_client(self):
        marker = self.c.s.r.directory / "owned-pid"
        bundle = self.fake_bundle("import os,time\n"
            f"open({str(marker)!r},'w').write(str(os.getpid()))\n"
            "time.sleep(30)\n")
        process = subprocess.Popen([EXECUTABLE, str(bundle), str(self.c.path),
            str(time.monotonic_ns() + 4_000_000_000)], stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            process.stdin.write(b"{}\n")
            process.stdin.close()
            process.stdin = None
            deadline = time.monotonic() + 2
            while not marker.exists() and time.monotonic() < deadline:
                time.sleep(0.005)
            self.assertTrue(marker.exists(), "client did not launch")
            pid = int(marker.read_text())
            process.kill()
            output, error = process.communicate(timeout=2)
            self.assertEqual((process.returncode, output, error), (-signal.SIGKILL, b"", b""))
            self.assert_not_running(pid)
        finally:
            if process.poll() is None:
                process.kill()
                process.communicate(timeout=2)
            for stream in (process.stdin, process.stdout, process.stderr):
                if stream is not None:
                    stream.close()

    def test_exact_output_bound_and_unavailable_reaper(self):
        bundle = self.fake_bundle("import sys\n"
            "assert sys.stdin.buffer.read() == b'{}\\n'\n"
            "sys.stderr.write('private-fixture-message')\n"
            "sys.stdout.buffer.write(b'x'*4096)\n")
        result = self.call(b"{}\n", bundle=bundle)
        self.assertEqual((result.returncode, result.stdout, result.stderr), (0, b"x" * 4096, b""))
        result = subprocess.run([EXECUTABLE, str(bundle), str(self.c.path),
            str(time.monotonic_ns() + 3_000_000_000)], input=b"{}\n", capture_output=True, timeout=5,
            preexec_fn=lambda: signal.signal(signal.SIGCHLD, signal.SIG_IGN))
        self.refused(result)


if __name__ == "__main__":
    if len(sys.argv) != 3 or not all(Path(path).is_absolute() for path in sys.argv[1:]):
        raise SystemExit("expected absolute C client driver and map verifier")
    EXECUTABLE, registry_tests.EXECUTABLE = sys.argv[1:]
    unittest.main(argv=[sys.argv[0]], verbosity=2)
