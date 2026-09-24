"""Single Linux execution owner for bounded, authenticated target operations.

Author: SqlRush <sqlrush@gmail.com>
Worker exit and watchdog expiry are never OFF, drain or admission evidence.
The listener supplies protected, frozen configuration. Native handles must be
opened by its factory *inside* the authenticated worker, never in the parent.
"""

from contextlib import contextmanager
import ctypes
import fcntl
import os
import signal
import stat
import sys
import threading
import time

from target_dispatch import dispatch_target
from target_journal import TargetJournal, TargetJournalError, _directory_fd, _uint
from target_registry import TargetRegistry
from target_transport import _policy, serve_one

KILL_GRACE_NS = 1_000_000_000


@contextmanager
def _blocked_signals():
    # Publishing/reaping a child identity must be atomic against Python signal
    # handlers. Otherwise an interrupt can orphan a live operation or let a
    # cleanup retry signal a PID that waitpid has already made reusable.
    previous = signal.pthread_sigmask(signal.SIG_BLOCK,
                                      signal.valid_signals() - {signal.SIGKILL, signal.SIGSTOP})
    try:
        yield previous
    finally:
        signal.pthread_sigmask(signal.SIG_SETMASK, previous)


def _parent_guard(parent_pid):
    os.setsid()
    libc = ctypes.CDLL(None, use_errno=True)
    prctl = libc.prctl
    prctl.argtypes = [ctypes.c_int, ctypes.c_ulong, ctypes.c_ulong,
                      ctypes.c_ulong, ctypes.c_ulong]
    prctl.restype = ctypes.c_int
    if (prctl(1, signal.SIGKILL, 0, 0, 0) != 0  # PR_SET_PDEATHSIG
            or os.getppid() != parent_pid):
        raise TargetJournalError("TARGET_WORKER_PARENT")


def _group_exists(pid):
    try:
        os.killpg(pid, 0)
        return True
    except ProcessLookupError:
        return False


class TargetWorker:
    """One operation at a time; no second attempt while an old worker survives.

    The parent must be single-threaded and the sole SIGCHLD reaper. The native
    factory returns a context manager yielding (RTSRoot, read-only libvirt).
    It must not initialize host configuration or spawn detached native work.
    This internal callable is not selected by network input.
    """

    def __init__(self, directory, registry, context, peer_pins, native_factory,
                 *, startup_config=None, owner_uid=0):
        self.lock_fd = self.directory_fd = self.active_pid = None
        self._reaped_status = None
        self.poisoned = False
        self.parent_pid = os.getpid()
        self.directory, self.owner_uid = directory, owner_uid
        self.registry, self.context, self.peer_pins = registry, context, peer_pins
        self.native_factory, self.startup_config = native_factory, startup_config
        if (sys.platform != "linux" or type(registry) is not TargetRegistry
                or not callable(native_factory) or type(owner_uid) is not int or owner_uid < 0):
            raise TargetJournalError("TARGET_WORKER_ARGUMENT")
        _policy(context, peer_pins)
        self._process_policy()
        try:
            self.directory_fd = _directory_fd(directory, owner_uid)
            self.lock_fd = os.open("owner.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW
                                   | os.O_NONBLOCK | os.O_CLOEXEC, 0o600, dir_fd=self.directory_fd)
            self._lock_status()
            try:
                fcntl.flock(self.lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise TargetJournalError("TARGET_WORKER_BUSY") from None
            # Existing initialization is a separate closed-start operation.
            # Do not manufacture, repair or discard a journal here.
            with TargetJournal(directory, registry.inventory_digest, owner_uid=owner_uid):
                pass
        except BaseException as error:
            self.close()
            if isinstance(error, OSError):
                raise TargetJournalError("TARGET_WORKER_FILE") from None
            raise

    def _process_policy(self):
        if (os.getpid() != self.parent_pid or threading.active_count() != 1
                or threading.current_thread() is not threading.main_thread()
                or signal.getsignal(signal.SIGCHLD) != signal.SIG_DFL
                or len(os.listdir("/proc/self/task")) != 1):
            raise TargetJournalError("TARGET_WORKER_PROCESS")

    def _lock_status(self):
        status = os.fstat(self.lock_fd)
        named = os.stat("owner.lock", dir_fd=self.directory_fd, follow_symlinks=False)
        if (not stat.S_ISREG(status.st_mode) or status.st_uid != self.owner_uid
                or stat.S_IMODE(status.st_mode) != 0o600 or status.st_nlink != 1
                or status.st_size != 0 or (status.st_dev, status.st_ino) != (named.st_dev, named.st_ino)):
            raise TargetJournalError("TARGET_WORKER_FILE")

    def _usable(self):
        self._process_policy()
        if self.lock_fd is None or self.poisoned or self.active_pid is not None:
            raise TargetJournalError("TARGET_WORKER_UNUSABLE")
        self._lock_status()
        check = _directory_fd(self.directory, self.owner_uid)
        try:
            current, pinned = os.fstat(check), os.fstat(self.directory_fd)
            if (current.st_dev, current.st_ino) != (pinned.st_dev, pinned.st_ino):
                raise TargetJournalError("TARGET_WORKER_FILE")
        finally:
            os.close(check)

    def __enter__(self):
        self._usable()
        return self

    def __exit__(self, *_args):
        self.close()

    def close(self):
        if self.active_pid is not None:
            # An unkillable worker remains an execution obligation. Never
            # release its parent lock as a pretend recovery/timeout action.
            raise TargetJournalError("TARGET_WORKER_UNREAPED")
        for name in ("lock_fd", "directory_fd"):
            fd = getattr(self, name, None)
            if fd is not None:
                setattr(self, name, None)
                # No LOCK_UN: a forked worker shares this open description.
                os.close(fd)

    def _child(self, connection, deadline, signal_mask):
        result = 1
        try:
            _parent_guard(self.parent_pid)
            # Keep ownership even if a native library launches an owned helper.
            # Detached helpers remain prohibited by the fixed native profile.
            os.set_inheritable(self.lock_fd, True)
            signal.pthread_sigmask(signal.SIG_SETMASK, signal_mask)
            def dispatch(request):
                with TargetJournal(self.directory, self.registry.inventory_digest,
                                   owner_uid=self.owner_uid) as journal:
                    with self.native_factory() as (root, native_connection):
                        return dispatch_target(request, self.registry, journal, root, native_connection,
                                               startup_config=self.startup_config)
            serve_one(connection, self.context, self.peer_pins, deadline, dispatch)
            result = 0
        except BaseException:
            # No TLS/request/native diagnostic material in process output.
            pass
        finally:
            os._exit(result)

    def _finish(self, pid):
        """Signal only the unreaped child identity; never kill a reused PID."""
        try:
            if self._reaped_status is None:
                with _blocked_signals():
                    try:
                        os.killpg(pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass  # Child might not yet have called setsid().
                    try:
                        os.kill(pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
            deadline = time.monotonic_ns() + KILL_GRACE_NS
            while True:
                if self._reaped_status is None:
                    with _blocked_signals():
                        found, value = os.waitpid(pid, os.WNOHANG)
                        if found:
                            self._reaped_status = value
                if self._reaped_status is not None and not _group_exists(pid):
                    self.active_pid = None
                    return self._reaped_status
                if time.monotonic_ns() >= deadline:
                    raise TargetJournalError("TARGET_WORKER_UNREAPED")
                time.sleep(0.005)
        except BaseException:
            self.poisoned = True
            raise

    def serve(self, connection, deadline_mono_ns):
        """Consume one socket; return only when the exact worker has exited.

        A completed reply is an operation-bound observation. The return value
        is deliberately not a certificate. Expiry preserves durable obligations.
        """
        expired = False
        try:
            self._usable()
            _policy(self.context, self.peer_pins)
            if not _uint(deadline_mono_ns) or time.monotonic_ns() >= deadline_mono_ns:
                raise TargetJournalError("TARGET_WORKER_EXPIRED")
            with _blocked_signals() as signal_mask:
                pid = os.fork()
                if pid == 0:
                    self._child(connection, deadline_mono_ns, signal_mask)
                self.active_pid = pid
                self._reaped_status = None
            while True:
                # WNOWAIT keeps the child identity reserved until group cleanup.
                result = os.waitid(os.P_PID, pid, os.WEXITED | os.WNOHANG | os.WNOWAIT)
                if result is not None:
                    break
                if time.monotonic_ns() >= deadline_mono_ns:
                    expired = True
                    break
                time.sleep(0.005)
            status = self._finish(pid)
            if expired:
                raise TargetJournalError("TARGET_WORKER_EXPIRED")
            if status != 0:
                raise TargetJournalError("TARGET_WORKER_UNPROVEN")
        except BaseException as error:
            if isinstance(error, ChildProcessError):
                # Somebody else reaped this child: numeric PID ownership is
                # unproven. Never send it another signal or start a new worker.
                self.poisoned = True
            if self.active_pid is not None and not self.poisoned:
                try:
                    self._finish(self.active_pid)
                except BaseException:
                    self.poisoned = True
            if isinstance(error, OSError):
                raise TargetJournalError("TARGET_WORKER_UNPROVEN") from None
            raise
        finally:
            connection.close()
