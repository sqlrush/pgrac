"""Darwin Lima lab power observations, without storage or isolation receipts.

Author: SqlRush <sqlrush@gmail.com>
"""

from contextlib import contextmanager
import ctypes
from dataclasses import dataclass
import errno
import fcntl
import json
import os
from pathlib import Path
import plistlib
import re
import shlex
import stat
import subprocess
import sys
import time

LIMACTL = "/opt/homebrew/bin/limactl"


class LimaPowerError(RuntimeError):
    pass


@dataclass(frozen=True)
class LimaTarget:
    node_id: int
    guest_uuid: str


@dataclass(frozen=True)
class ProcessIdentity:
    pid: int
    uid: int
    start_sec: int
    start_usec: int
    executable: str
    command: str


@dataclass(frozen=True)
class PowerObservation:
    node_id: int
    guest_uuid: str
    state: str
    host_process: object
    observed_mono_ns: int


@dataclass(frozen=True)
class PowerOffEvidence:
    before: PowerObservation
    after: PowerObservation


class _BsdInfo(ctypes.Structure):
    # Darwin sys/proc_info.h: PROC_PIDTBSDINFO, including microsecond birth.
    _fields_ = [(name, ctypes.c_uint32) for name in (
        "flags", "status", "xstatus", "pid", "ppid", "uid", "gid", "ruid",
        "rgid", "svuid", "svgid", "reserved")]
    _fields_ += [("comm", ctypes.c_char * 16), ("name", ctypes.c_char * 32)]
    _fields_ += [(name, ctypes.c_uint32) for name in (
        "nfiles", "pgid", "pjobc", "tdev", "tpgid")]
    _fields_ += [("nice", ctypes.c_int32), ("start_sec", ctypes.c_uint64),
                 ("start_usec", ctypes.c_uint64)]


def process_identity(pid):
    """None means ESRCH, never an unreadable, foreign or unsupported process."""
    if sys.platform != "darwin" or type(pid) is not int or not 1 < pid < 1 << 31:
        raise LimaPowerError("LIMA_PROCESS_ARGUMENT")
    try:
        os.kill(pid, 0)
    except OSError as error:
        if error.errno == errno.ESRCH:
            return None
        raise LimaPowerError("LIMA_PROCESS_UNPROVEN") from None
    try:
        native = ctypes.CDLL("/usr/lib/libproc.dylib", use_errno=True)
        native.proc_pidinfo.argtypes = (ctypes.c_int, ctypes.c_int, ctypes.c_uint64,
                                       ctypes.c_void_p, ctypes.c_int)
        native.proc_pidinfo.restype = ctypes.c_int
        native.proc_pidpath.argtypes = (ctypes.c_int, ctypes.c_void_p, ctypes.c_uint32)
        native.proc_pidpath.restype = ctypes.c_int

        def read():
            info = _BsdInfo()
            if (ctypes.sizeof(info) != 136
                    or native.proc_pidinfo(pid, 3, 0, ctypes.byref(info), 136) != 136
                    or info.pid != pid or info.start_sec == 0 or info.start_usec >= 1_000_000):
                raise LimaPowerError("LIMA_PROCESS_UNPROVEN")
            path = ctypes.create_string_buffer(4096)
            if native.proc_pidpath(pid, path, len(path)) <= 0:
                raise LimaPowerError("LIMA_PROCESS_UNPROVEN")
            return info.uid, info.start_sec, info.start_usec, os.fsdecode(path.value)

        before = read()
        result = subprocess.run(["/bin/ps", "-ww", "-p", str(pid), "-o", "command="],
                                capture_output=True, text=True, check=False, timeout=2)
        if result.returncode or not result.stdout.strip() or len(result.stdout) > 16384:
            raise LimaPowerError("LIMA_PROCESS_UNPROVEN")
        if before != read():
            raise LimaPowerError("LIMA_PROCESS_CHANGED")
        return ProcessIdentity(pid, *before, result.stdout.strip())
    except LimaPowerError:
        raise
    except (OSError, ValueError, subprocess.SubprocessError):
        raise LimaPowerError("LIMA_PROCESS_UNPROVEN") from None


def _remaining(deadline):
    if type(deadline) is not int or not 0 < deadline < 1 << 64:
        raise LimaPowerError("LIMA_DEADLINE")
    remaining = (deadline - time.monotonic_ns()) / 1_000_000_000
    if not 0 < remaining <= 120:
        raise LimaPowerError("LIMA_DEADLINE")
    return remaining


def _trusted(fd, *, directory=False):
    info = os.fstat(fd)
    predicate = stat.S_ISDIR if directory else stat.S_ISREG
    if not predicate(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o022:
        raise LimaPowerError("LIMA_PATH_UNTRUSTED")
    return info.st_dev, info.st_ino


class LimaPower:
    """Caller retains exclusive management ownership beyond a returned observation.

    No start/unfence operation is provided. Power evidence alone must never be
    used as an old-I/O drain, storage-denial proof or service permission.
    """

    def __init__(self, home):
        self.home = Path(home)
        if (not self.home.is_absolute() or self.home.name != ".lima-pre2-single"
                or self.home.resolve() != self.home):
            raise LimaPowerError("LIMA_HOME")

    @contextmanager
    def _owner(self, target, deadline):
        _remaining(deadline)
        if (type(target) is not LimaTarget or type(target.node_id) is not int
                or not 0 <= target.node_id <= 3 or type(target.guest_uuid) is not str
                or not re.fullmatch("[0-9a-f]{32}", target.guest_uuid)
                or target.guest_uuid == "0" * 32):
            raise LimaPowerError("LIMA_TARGET")
        name = f"pgrac-lab4-node{target.node_id}"
        fds = []
        try:
            root = os.open(self.home, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            fds.append(root)
            root_id = _trusted(root, directory=True)
            vm = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=root)
            fds.append(vm)
            vm_id = _trusted(vm, directory=True)
            lock = os.open("pgrac-power.lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW,
                           0o600, dir_fd=vm)
            fds.append(lock)
            lock_id = _trusted(lock)
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)

            def check():
                _remaining(deadline)
                for path, identity in ((self.home, root_id), (self.home / name, vm_id),
                                       (self.home / name / "pgrac-power.lock", lock_id)):
                    info = path.lstat()
                    if (info.st_dev, info.st_ino) != identity or stat.S_ISLNK(info.st_mode):
                        raise LimaPowerError("LIMA_PATH_CHANGED")
                fd = os.open("vz-identifier", os.O_RDONLY | os.O_NOFOLLOW, dir_fd=vm)
                try:
                    _trusted(fd)
                    data = os.read(fd, 4097)
                finally:
                    os.close(fd)
                if len(data) > 4096:
                    raise LimaPowerError("LIMA_UUID")
                document = plistlib.loads(data)
                if (type(document) is not dict or set(document) != {"UUID"}
                        or type(document["UUID"]) is not bytes or len(document["UUID"]) != 16
                        or document["UUID"].hex() != target.guest_uuid):
                    raise LimaPowerError("LIMA_UUID")

            check()
            yield name, check
            check()
        except LimaPowerError:
            raise
        except (OSError, ValueError, TypeError, plistlib.InvalidFileException):
            raise LimaPowerError("LIMA_BINDING_UNPROVEN") from None
        finally:
            for fd in reversed(fds):
                os.close(fd)

    def _command(self, arguments, deadline):
        try:
            result = subprocess.run([LIMACTL, *arguments],
                                    env={**os.environ, "LIMA_HOME": str(self.home)},
                                    capture_output=True, text=True, check=False,
                                    timeout=_remaining(deadline))
            _remaining(deadline)
            if result.returncode or len(result.stdout) > 2 * 1024 * 1024:
                raise LimaPowerError("LIMA_COMMAND_UNPROVEN")
            return result.stdout
        except (OSError, subprocess.SubprocessError):
            raise LimaPowerError("LIMA_COMMAND_UNPROVEN") from None

    def _sample(self, target, name, check, deadline):
        check()
        try:
            record = json.loads(self._command(["list", name, "--json"], deadline))
        except ValueError:
            raise LimaPowerError("LIMA_INVENTORY_UNPROVEN") from None
        if (type(record) is not dict or record.get("name") != name
                or record.get("dir") != str(self.home / name)
                or record.get("vmType") != "vz" or record.get("arch") != "aarch64"):
            raise LimaPowerError("LIMA_INVENTORY_UNPROVEN")
        state = record.get("status")
        pids = (record.get("hostAgentPID", 0), record.get("driverPID", 0))
        if any(type(pid) is not int for pid in pids):
            raise LimaPowerError("LIMA_PROCESS_UNPROVEN")
        host = None
        if state == "Stopped":
            if pids != (0, 0):
                raise LimaPowerError("LIMA_NOT_OFF")
        elif state == "Running":
            if not 1 < pids[0] < 1 << 31 or pids[0] != pids[1]:
                raise LimaPowerError("LIMA_PROCESS_UNPROVEN")
            host = process_identity(pids[0])
            expected = ["hostagent", "--pidfile", str(self.home / name / "ha.pid"),
                        "--socket", str(self.home / name / "ha.sock"), "--guestagent",
                        "/opt/homebrew/share/lima/lima-guestagent.Linux-aarch64.gz", name]
            argv = shlex.split(host.command) if host else []
            if (host is None or host.pid != pids[0] or host.uid != os.getuid()
                    or host.executable != os.path.realpath(LIMACTL) or len(argv) != 9
                    or os.path.realpath(argv[0]) != os.path.realpath(LIMACTL)
                    or argv[1:] != expected):
                raise LimaPowerError("LIMA_PROCESS_UNPROVEN")
        else:
            raise LimaPowerError("LIMA_STATE_UNPROVEN")
        check()
        return PowerObservation(target.node_id, target.guest_uuid, state, host, time.monotonic_ns())

    def _observe(self, target, name, check, deadline):
        before = self._sample(target, name, check, deadline)
        after = self._sample(target, name, check, deadline)
        if (before.state, before.host_process) != (after.state, after.host_process):
            raise LimaPowerError("LIMA_PROCESS_CHANGED")
        return after

    def observe(self, target, deadline_mono_ns):
        with self._owner(target, deadline_mono_ns) as (name, check):
            return self._observe(target, name, check, deadline_mono_ns)

    def force_off(self, target, deadline_mono_ns):
        with self._owner(target, deadline_mono_ns) as (name, check):
            before = self._observe(target, name, check, deadline_mono_ns)
            if before.state != "Running":
                raise LimaPowerError("LIMA_ORIGINAL_PROCESS_MISSING")
            check()
            self._command(["stop", "--force", name], deadline_mono_ns)
            after = self._observe(target, name, check, deadline_mono_ns)
            if after.state != "Stopped" or process_identity(before.host_process.pid) is not None:
                raise LimaPowerError("LIMA_ORIGINAL_PROCESS_NOT_EXITED")
            check()
            return PowerOffEvidence(before, after)
