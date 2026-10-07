"""Linux process-owner tests using real fork, TLS and durable target records.

Author: SqlRush <sqlrush@gmail.com>
Only native libvirt/LIO handles are fixtures; no VM or export is changed.
Invoke explicitly on Linux, not as a silently skipped portable test suite.
"""

from contextlib import contextmanager, nullcontext
import json
import os
from pathlib import Path
import select
import signal
import socket
import struct
import sys
import time
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import test_target_dispatch as dispatch_tests
import test_target_transport as transport_tests
from target_journal import TargetJournal, TargetJournalError, _canonical
try:
    from target_worker import TargetWorker
except ModuleNotFoundError:
    TargetWorker = None


def read_pipe(fd, seconds=3):
    if not select.select([fd], [], [], seconds)[0]:
        raise AssertionError("test child exceeded envelope")
    return os.read(fd, 8192)


def reap(pid, seconds=3):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        found, status = os.waitpid(pid, os.WNOHANG)
        if found:
            return status
        time.sleep(0.005)
    os.kill(pid, signal.SIGKILL)
    os.waitpid(pid, 0)
    raise AssertionError("test child did not terminate")


class TargetWorkerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if sys.platform != "linux":
            raise AssertionError("run_target_worker.py requires actual Linux")
        transport_tests.TargetTransportTests.setUpClass.__func__(cls)
        cls.tls_directory = cls.directory

    def setUp(self):
        self.assertTrue(callable(TargetWorker), "exclusive bounded target owner is missing")
        self.f = dispatch_tests.TargetDispatchTests()
        self.addCleanup(self.f.doCleanups)
        self.f.setUp()
        self.f.journal.close()
        self.tls = transport_tests.TargetTransportTests()
        self.tls.directory, self.tls.pin = self.tls_directory, self.pin
        self.server_context = self.tls.context(server=True)
        self.client_context = self.tls.context(server=False)
        self.owner = None
        self.addCleanup(self.close_owner)

    def close_owner(self):
        if self.owner is not None:
            self.owner.close()
            self.owner = None

    @contextmanager
    def native(self):
        yield self.f.root, self.f.connection

    def open_owner(self):
        return TargetWorker(str(self.f.directory), self.f.registry, self.server_context,
                            (self.pin,), self.native, owner_uid=os.geteuid())

    def states(self):
        with TargetJournal(str(self.f.directory), self.f.registry.inventory_digest,
                           owner_uid=os.geteuid()) as journal:
            return journal.denied()

    def exchange(self, *, seconds=2, pins=None, payload=None, serve_patch=None):
        if self.owner is None:
            self.owner = self.open_owner()
        server, client = socket.socketpair()
        output_read, output_write = os.pipe()
        pid = os.fork()
        if pid == 0:
            # This is an independent client, not a target owner's descendant.
            server.close()
            os.close(output_read)
            os.close(self.owner.lock_fd)
            os.close(self.owner.directory_fd)
            wire = b""
            try:
                with self.client_context.wrap_socket(client, server_hostname="server") as secured:
                    secured.settimeout(4)
                    body = _canonical(self.f.document) if payload is None else payload
                    secured.sendall(struct.pack("!I", len(body)) + body)
                    while True:
                        part = secured.recv(8192)
                        if not part:
                            break
                        wire += part
            except Exception:
                pass
            os.write(output_write, wire or b"NO_REPLY")
            os._exit(0)
        client.close()
        os.close(output_write)
        error = interrupted = None
        try:
            if pins is not None:
                self.owner.peer_pins = pins
            with serve_patch or nullcontext():
                self.owner.serve(server, time.monotonic_ns() + int(seconds * 1e9))
        except TargetJournalError as caught:
            error = str(caught)
        except BaseException as caught:
            interrupted = caught
        finally:
            server.close()
        try:
            wire = read_pipe(output_read, 5)
            self.assertEqual(reap(pid), 0)
        finally:
            os.close(output_read)
        if interrupted is not None:
            raise interrupted
        if wire == b"NO_REPLY":
            return error, None
        self.assertEqual(struct.unpack("!I", wire[:4])[0], len(wire) - 4)
        return error, json.loads(wire[4:])

    def test_authentic_prepare_and_exact_retry_replay_in_fresh_workers(self):
        error, reply = self.exchange()
        self.assertIsNone(error)
        self.assertEqual(reply["status"], "DENY_RECORDED")
        self.assertEqual(self.states()[0].identity, self.f.identity)
        before = (self.f.directory / "deny.journal").read_bytes()
        self.assertEqual(self.exchange()[1]["journal_sequence"], reply["journal_sequence"])
        self.assertEqual((self.f.directory / "deny.journal").read_bytes(), before)
        self.close_owner()
        self.assertEqual(self.exchange()[1]["status"], "DENY_RECORDED")

    def test_competing_owner_refused_without_truncating_lock_or_journal(self):
        self.owner = self.open_owner()
        original = (self.f.directory / "deny.journal").read_bytes()
        inode = (self.f.directory / "owner.lock").stat().st_ino
        with self.assertRaisesRegex(TargetJournalError, "TARGET_WORKER_BUSY"):
            self.open_owner()
        self.assertEqual((self.f.directory / "owner.lock").stat().st_ino, inode)
        self.assertEqual((self.f.directory / "deny.journal").read_bytes(), original)
        self.close_owner()
        with self.open_owner():
            self.assertEqual((self.f.directory / "owner.lock").stat().st_ino, inode)

    def test_unlisted_peer_never_opens_native_handles_or_changes_journal(self):
        @contextmanager
        def forbidden():
            (self.f.directory / "native-opened").touch()
            yield self.f.root, self.f.connection
        self.native = forbidden
        error, reply = self.exchange(pins=("ff" * 32,))
        self.assertIsNone(reply)
        self.assertEqual(error, "TARGET_WORKER_UNPROVEN")
        self.assertFalse((self.f.directory / "native-opened").exists())
        self.assertEqual(self.states(), ())

    def test_blocked_native_teardown_expires_without_faking_drain_or_losing_deny(self):
        self.assertIsNone(self.exchange()[0])
        self.f.document["action"] = "complete_off"
        def blocked(_path):
            signal.pause()
            raise AssertionError("native fixture must be killed")
        start = time.monotonic()
        with patch("target_drain.os.rmdir", side_effect=blocked):
            error, reply = self.exchange(seconds=0.35)
        self.assertEqual(error, "TARGET_WORKER_EXPIRED")
        self.assertIsNone(reply)
        self.assertLess(time.monotonic() - start, 2)
        state = self.states()[0]
        self.assertEqual(state.identity, self.f.identity)
        self.assertEqual(state.phases, (1, 1, 1, 1))
        self.assertIsNone(self.owner.active_pid)
        # A fresh owner may inspect/retry only after the old real process died.
        self.close_owner()
        self.f.document["action"] = "prepare_deny"
        self.assertEqual(self.exchange()[1]["status"], "DENY_RECORDED")
        self.assertEqual(self.states()[0].phases, (1, 1, 1, 1))

    def test_invalid_lock_object_and_permissions_never_start_worker(self):
        lock = self.f.directory / "owner.lock"
        lock.symlink_to("deny.journal")
        with self.assertRaises(TargetJournalError):
            self.open_owner()
        lock.unlink()
        lock.write_bytes(b"")
        lock.chmod(0o644)
        with self.assertRaises(TargetJournalError):
            self.open_owner()
        lock.chmod(0o600)
        lock.write_bytes(b"unexpected owner data")
        with self.assertRaises(TargetJournalError):
            self.open_owner()
        self.assertEqual(lock.read_bytes(), b"unexpected owner data")

    def test_interrupt_at_fork_return_still_reaps_the_owned_child(self):
        actual_fork = os.fork
        children = []
        def interrupt_fork():
            pid = actual_fork()
            if pid:
                children.append(pid)
                os.kill(os.getpid(), signal.SIGINT)
            return pid
        try:
            with self.assertRaises(KeyboardInterrupt):
                self.exchange(serve_patch=patch("target_worker.os.fork", side_effect=interrupt_fork))
            self.assertEqual(len(children), 1)
            with self.assertRaises(ChildProcessError):
                os.waitpid(children[0], os.WNOHANG)
            self.assertIsNone(self.owner.active_pid)
        finally:
            for pid in children:
                try:
                    found, _ = os.waitpid(pid, os.WNOHANG)
                    if not found:
                        os.kill(pid, signal.SIGKILL)
                        reap(pid)
                except ChildProcessError:
                    pass

    def test_unkillable_worker_poison_holds_lock_and_refuses_a_second_attempt(self):
        self.assertIsNone(self.exchange()[0])
        self.f.document["action"] = "complete_off"
        def blocked(_path):
            signal.pause()
        actual_kill, actual_killpg = os.kill, os.killpg
        def no_kill(pid, sig):
            if sig != signal.SIGKILL:
                return actual_kill(pid, sig)
        def no_killpg(pid, sig):
            if sig != signal.SIGKILL:
                return actual_killpg(pid, sig)
        @contextmanager
        def signals_refused():
            with patch("target_worker.os.kill", side_effect=no_kill), \
                    patch("target_worker.os.killpg", side_effect=no_killpg):
                yield
        try:
            with patch("target_drain.os.rmdir", side_effect=blocked):
                error, reply = self.exchange(seconds=0.2, serve_patch=signals_refused())
            self.assertEqual(error, "TARGET_WORKER_UNREAPED")
            self.assertIsNone(reply)
            self.assertTrue(self.owner.poisoned)
            self.assertIsNotNone(self.owner.active_pid)
            with self.assertRaises(TargetJournalError):
                self.open_owner()
            with self.assertRaises(TargetJournalError):
                self.owner.close()
            self.assertEqual(self.exchange()[0], "TARGET_WORKER_UNUSABLE")
        finally:
            if self.owner.active_pid is not None:
                actual_killpg(self.owner.active_pid, signal.SIGKILL)
                reap(self.owner.active_pid)
                self.owner.active_pid = None  # Test fixture cleanup, not a recovery API.
        self.assertEqual(self.states()[0].phases, (1, 1, 1, 1))

    def test_group_check_failure_never_signals_an_already_reaped_pid(self):
        actual_waitpid, actual_kill, actual_killpg = os.waitpid, os.kill, os.killpg
        reaped, late_signals = set(), []
        def waitpid(pid, flags):
            result = actual_waitpid(pid, flags)
            if result[0]:
                reaped.add(result[0])
            return result
        def kill(pid, sig):
            if sig and pid in reaped:
                late_signals.append(pid)
            return actual_kill(pid, sig)
        def killpg(pid, sig):
            if sig and pid in reaped:
                late_signals.append(pid)
            return actual_killpg(pid, sig)
        @contextmanager
        def census_failed():
            with patch("target_worker.os.waitpid", side_effect=waitpid), \
                    patch("target_worker.os.kill", side_effect=kill), \
                    patch("target_worker.os.killpg", side_effect=killpg), \
                    patch("target_worker._group_exists", side_effect=OSError("private host detail")):
                yield
        try:
            error, _reply = self.exchange(serve_patch=census_failed())
            self.assertEqual(error, "TARGET_WORKER_UNPROVEN")
            self.assertEqual(late_signals, [])
            self.assertTrue(self.owner.poisoned)
        finally:
            self.owner.active_pid = None  # Real child was reaped; fixture-only cleanup.

    def test_surviving_worker_descendant_keeps_owner_poisoned_and_locked(self):
        grandchild_file = self.f.directory / "native-descendant"
        @contextmanager
        def descendant():
            pid = os.fork()
            if pid == 0:
                while True:
                    signal.pause()
            grandchild_file.write_text(str(pid))
            yield self.f.root, self.f.connection
        self.native = descendant
        actual_killpg = os.killpg
        def refuse_group_kill(pid, sig):
            if sig != signal.SIGKILL:
                return actual_killpg(pid, sig)
        try:
            error, reply = self.exchange(serve_patch=patch("target_worker.os.killpg",
                                                          side_effect=refuse_group_kill))
            self.assertEqual(reply["status"], "DENY_RECORDED")
            self.assertEqual(error, "TARGET_WORKER_UNREAPED")
            self.assertTrue(self.owner.poisoned)
            grandchild = int(grandchild_file.read_text())
            self.assertEqual(os.getpgid(grandchild), self.owner.active_pid)
            self.assertEqual(self.exchange()[0], "TARGET_WORKER_UNUSABLE")
            with self.assertRaises(TargetJournalError):
                self.open_owner()
        finally:
            if grandchild_file.exists():
                grandchild = int(grandchild_file.read_text())
                os.kill(grandchild, signal.SIGKILL)
            self.owner.active_pid = None  # The leader was already reaped by production.

    def test_foreign_reaper_poison_never_signals_a_now_unowned_pid(self):
        actual_waitid, actual_waitpid, actual_kill = os.waitid, os.waitpid, os.kill
        reaped, signals = set(), []
        def foreign_reap(*args):
            result = actual_waitid(*args)
            if result is not None:
                actual_waitpid(result.si_pid, 0)
                reaped.add(result.si_pid)
                raise ChildProcessError("test foreign reaper")
            return result
        def kill(pid, sig):
            if pid in reaped:
                signals.append(pid)
            return actual_kill(pid, sig)
        @contextmanager
        def stolen():
            with patch("target_worker.os.waitid", side_effect=foreign_reap), \
                    patch("target_worker.os.kill", side_effect=kill):
                yield
        try:
            error, _reply = self.exchange(serve_patch=stolen())
            self.assertEqual(error, "TARGET_WORKER_UNPROVEN")
            self.assertTrue(self.owner.poisoned)
            self.assertEqual(signals, [])
            self.assertEqual(self.exchange()[0], "TARGET_WORKER_UNUSABLE")
        finally:
            self.owner.active_pid = None  # Test foreign reaper already collected the child.

    def test_parent_death_kills_native_worker_but_durable_deny_survives(self):
        self.assertIsNone(self.exchange()[0])
        self.close_owner()
        self.f.document["action"] = "complete_off"
        server, client = socket.socketpair()
        arrived_read, arrived_write = os.pipe()
        def blocked(_path):
            os.write(arrived_write, str(os.getpid()).encode())
            signal.pause()
        with patch("target_drain.os.rmdir", side_effect=blocked):
            supervisor = os.fork()
            if supervisor == 0:
                client.close()
                os.close(arrived_read)
                try:
                    with self.open_owner() as owner:
                        owner.serve(server, time.monotonic_ns() + 5_000_000_000)
                finally:
                    os._exit(2)
            server.close()
            os.close(arrived_write)
            supervisor_reaped = False
            try:
                with self.client_context.wrap_socket(client, server_hostname="server") as secured:
                    secured.settimeout(3)
                    payload = _canonical(self.f.document)
                    secured.sendall(struct.pack("!I", len(payload)) + payload)
                    worker_pid = int(read_pipe(arrived_read))
                    self.assertNotEqual(supervisor, worker_pid)
                    os.kill(supervisor, signal.SIGKILL)
                    status = reap(supervisor)
                    supervisor_reaped = True
                    self.assertEqual(status, signal.SIGKILL)
                    self.assertEqual(secured.recv(8192), b"")
                # The lock is released by process exit, not a fabricated timeout.
                with self.open_owner():
                    self.assertEqual(self.states()[0].phases, (1, 1, 1, 1))
            finally:
                os.close(arrived_read)
                if not supervisor_reaped:
                    found, _ = os.waitpid(supervisor, os.WNOHANG)
                    if not found:
                        os.kill(supervisor, signal.SIGKILL)
                        reap(supervisor)


if __name__ == "__main__":
    unittest.main(verbosity=2)
