"""Lab scratch witness: keep one FD open across an externally owned drain.

Author: SqlRush <sqlrush@gmail.com>
This test tool reports native write/flush results, never isolation authority.
It does not revoke routes, alter recovery timeouts, reopen a device or retry I/O.
"""

import json
import mmap
import os
from pathlib import Path
import re
import socket
import stat
import subprocess
import sys
import time
import uuid

PAGE = 4096
OFFSET = 524288


def check_request(route, serial):
    pattern = (r"/dev/disk/by-path/ip-192\.168\.117\.1:3260-iscsi-"
               r"iqn\.2026-11\.org\.pgrac\.lab4:g[0-9]{8}-[0-9a-f]{32}-lun-4")
    if (type(route) is not str or not re.fullmatch(pattern, route)
            or type(serial) is not str):
        raise ValueError("LAB_SCRATCH_IDENTITY")
    value = uuid.UUID(serial)
    if not value.int or str(value) != serial:
        raise ValueError("LAB_SCRATCH_IDENTITY")


def serve_fd(fd, commands, output, identity):
    """The caller keeps ownership of fd. EOF/CLOSE perform no additional I/O."""
    def emit(event, **fields):
        output.write(json.dumps(dict(event=event, identity=identity,
                                     monotonic_ns=time.monotonic_ns(),
                                     realtime_ns=time.time_ns(), **fields)) + "\n")
        output.flush()

    with mmap.mmap(-1, PAGE) as page:
        sequence = 0
        while True:
            page[:] = (f"PGRAC LAB SCRATCH HELD FD sequence={sequence}\n".encode()).ljust(PAGE, b"\0")
            operation = "pwrite"
            try:
                count = os.pwrite(fd, page, OFFSET)
                if count != PAGE:
                    emit("IO_INCOMPLETE", sequence=sequence, operation=operation, bytes=count)
                    return 1
                operation = "fsync"
                os.fsync(fd)
            except OSError as error:
                emit("IO_ERROR", sequence=sequence, operation=operation, errno=error.errno)
                return 1
            emit("READY" if sequence == 0 else "WROTE", sequence=sequence, bytes=count,
                 offset=OFFSET)
            command = commands.readline()
            if command in ("", "CLOSE\n"):
                emit("CLOSED")
                return 0
            if command != "WRITE\n":
                emit("REFUSED", reason="INVALID_COMMAND")
                return 2
            sequence += 1


def open_scratch(route, serial):
    check_request(route, serial)
    if (sys.platform != "linux" or os.geteuid() != 0
            or not re.fullmatch(r"lima-pgrac-lab4-node[0-3]", socket.gethostname())):
        raise ValueError("LAB_NATIVE_OWNER_REQUIRED")
    path = Path(route).resolve(strict=True)
    before = path.stat(follow_symlinks=False)
    if not stat.S_ISBLK(before.st_mode):
        raise ValueError("LAB_BLOCK_DEVICE_REQUIRED")
    devices = json.loads(subprocess.check_output(
        ["lsblk", "--json", "--bytes", "--output", "PATH,TYPE,SIZE,SERIAL,MOUNTPOINTS",
         "--", str(path)], text=True, timeout=10))["blockdevices"]
    if (len(devices) != 1 or devices[0]["path"] != str(path)
            or devices[0]["type"] != "disk" or devices[0]["size"] != 1073741824
            or devices[0]["serial"] != serial or devices[0].get("children")
            or not devices[0]["mountpoints"]
            or any(x is not None for x in devices[0]["mountpoints"])):
        raise ValueError("LAB_SCRATCH_IN_USE_OR_MISMATCH")
    major, minor = os.major(before.st_rdev), os.minor(before.st_rdev)
    device_number = f"{major}:{minor}"
    if (any(Path(f"/sys/dev/block/{device_number}/holders").iterdir())
            or any(line.split()[2] == device_number
                   for line in Path("/proc/self/mountinfo").read_text().splitlines())
            or any(Path(line.split()[0]).resolve() == path
                   for line in Path("/proc/swaps").read_text().splitlines()[1:])):
        raise ValueError("LAB_SCRATCH_IN_USE_OR_MISMATCH")
    fd = os.open(path, os.O_RDWR | os.O_DIRECT | os.O_DSYNC | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        after = os.fstat(fd)
        if ((before.st_dev, before.st_ino, before.st_rdev) !=
                (after.st_dev, after.st_ino, after.st_rdev)):
            raise ValueError("LAB_BLOCK_DEVICE_CHANGED")
        process = Path("/proc/self/stat").read_text()
        start_ticks = int(process[process.rindex(")") + 2:].split()[19])
        identity = dict(pid=os.getpid(), start_ticks=start_ticks,
                        boot_id=Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
                        hostname=socket.gethostname(), route=route, device=str(path),
                        serial=serial, major=major, minor=minor, inode=after.st_ino,
                        open_flags="O_RDWR|O_DIRECT|O_DSYNC|O_NOFOLLOW|O_CLOEXEC")
        return fd, identity
    except BaseException:
        os.close(fd)
        raise


def main():
    if len(sys.argv) != 3:
        return 2
    try:
        fd, identity = open_scratch(sys.argv[1], sys.argv[2])
    except Exception:
        print(json.dumps(dict(event="REFUSED", reason="LAB_SCRATCH_UNPROVEN")), flush=True)
        return 2
    try:
        return serve_fd(fd, sys.stdin, sys.stdout, identity)
    finally:
        os.close(fd)


if __name__ == "__main__":
    sys.exit(main())
