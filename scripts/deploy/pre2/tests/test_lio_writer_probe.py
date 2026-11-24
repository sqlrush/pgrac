"""Check the held-descriptor witness, without claiming native I/O isolation.

Author: SqlRush <sqlrush@gmail.com>
"""

import errno
import importlib.util
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))
if importlib.util.find_spec("lio_writer_probe") is not None:
    import lio_writer_probe as probe
else:
    probe = None


class CommandStream(io.StringIO):
    def __init__(self, commands, before_read):
        super().__init__(commands)
        self.before_read = before_read

    def readline(self, *args):
        action, self.before_read = self.before_read, None
        if action:
            action()
        return super().readline(*args)


class HeldWriterTests(unittest.TestCase):
    def setUp(self):
        self.assertIsNotNone(probe, "held-descriptor witness is not implemented")
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / "scratch"
        self.fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600)
        self.addCleanup(os.close, self.fd)
        os.ftruncate(self.fd, 1 << 20)
        self.out = io.StringIO()
        self.identity = {"pid": os.getpid(), "inode": os.fstat(self.fd).st_ino}

    def run_probe(self, commands):
        code = probe.serve_fd(self.fd, commands, self.out, self.identity)
        return code, [json.loads(line) for line in self.out.getvalue().splitlines()]

    def test_ready_requires_a_completed_first_write_and_flush(self):
        code, events = self.run_probe(io.StringIO(""))
        self.assertEqual(code, 0)
        self.assertEqual([x["event"] for x in events], ["READY", "CLOSED"])
        self.assertEqual(events[0]["sequence"], 0)
        self.assertEqual(events[0]["identity"], self.identity)
        page = os.pread(self.fd, 4096, 524288)
        self.assertTrue(page.startswith(b"PGRAC LAB SCRATCH HELD FD sequence=0\n"))
        self.assertEqual(len(page), 4096)
        self.assertEqual(os.pread(self.fd, 4096, 0), b"\0" * 4096)

    def test_next_write_keeps_the_original_descriptor_after_path_replacement(self):
        old = self.path.with_name("original-inode")
        def replace():
            self.path.rename(old)
            self.path.write_bytes(b"replacement must stay unchanged")
        code, events = self.run_probe(CommandStream("WRITE\nCLOSE\n", replace))
        self.assertEqual(code, 0)
        self.assertEqual([x["event"] for x in events], ["READY", "WROTE", "CLOSED"])
        self.assertEqual([x["sequence"] for x in events[:2]], [0, 1])
        self.assertEqual(self.path.read_bytes(), b"replacement must stay unchanged")
        self.assertEqual(old.stat().st_ino, self.identity["inode"])
        self.assertTrue(os.pread(self.fd, 4096, 524288).startswith(
            b"PGRAC LAB SCRATCH HELD FD sequence=1\n"))

    def test_initial_write_error_is_not_ready_or_denial_success(self):
        read_only = os.open(self.path, os.O_RDONLY)
        self.addCleanup(os.close, read_only)
        code = probe.serve_fd(read_only, io.StringIO("WRITE\n"), self.out, self.identity)
        events = [json.loads(line) for line in self.out.getvalue().splitlines()]
        self.assertEqual(code, 1)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["event"], "IO_ERROR")
        self.assertEqual(events[0]["operation"], "pwrite")
        self.assertEqual(events[0]["errno"], errno.EBADF)
        self.assertEqual(events[0]["sequence"], 0)
        self.assertNotIn("write_excluded", events[0])

    def test_invalid_command_does_not_issue_another_write(self):
        code, events = self.run_probe(io.StringIO("REOPEN\nWRITE\n"))
        self.assertEqual(code, 2)
        self.assertEqual([x["event"] for x in events], ["READY", "REFUSED"])
        self.assertTrue(os.pread(self.fd, 4096, 524288).startswith(
            b"PGRAC LAB SCRATCH HELD FD sequence=0\n"))

    def test_close_does_not_consume_a_later_write(self):
        commands = io.StringIO("CLOSE\nWRITE\n")
        code, events = self.run_probe(commands)
        self.assertEqual(code, 0)
        self.assertEqual([x["event"] for x in events], ["READY", "CLOSED"])
        self.assertEqual(commands.readline(), "WRITE\n")

    def test_probe_does_not_close_the_callers_descriptor(self):
        self.run_probe(io.StringIO("CLOSE\n"))
        self.assertEqual(os.fstat(self.fd).st_ino, self.identity["inode"])

    def test_only_an_explicit_lab_scratch_route_and_serial_are_selected(self):
        route = ("/dev/disk/by-path/ip-192.168.117.1:3260-iscsi-"
                 "iqn.2026-11.org.pgrac.lab4:g20261125-" + "a" * 32 + "-lun-4")
        serial = "05fb1263-2743-45fc-af36-87f078ad0c38"
        probe.check_request(route, serial)
        for path, value in (("/dev/sda", serial), (route[:-1] + "0", serial),
                            (route.replace("lab4", "code-sandbox"), serial),
                            (route, ""), (route, "0" * 36), (route, True)):
            with self.subTest(path=path, serial=value), self.assertRaises(ValueError):
                probe.check_request(path, value)


if __name__ == "__main__":
    unittest.main(verbosity=2)
