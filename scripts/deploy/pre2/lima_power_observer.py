"""Read-only SSH forced command for the four Lima database guests.

Author: SqlRush <sqlrush@gmail.com>
"""

import os
from pathlib import Path
import pwd
import re
import select
import sys
import time

from lima_power import LimaPower, LimaPowerError, LimaTarget

COMMAND = re.compile(
    r"pgrac-lima-v1 (state|track-exit) ([0-3]) ([0-9a-f]{32}) ([0-9a-f]{32}) ([1-9][0-9]{0,5})"
)


def read_recheck(source, deadline):
    expected = b"RECHECK\n"
    data = bytearray()
    fd = source.fileno()
    while len(data) <= len(expected):
        remaining = (deadline - time.monotonic_ns()) / 1_000_000_000
        if remaining <= 0 or not select.select([fd], [], [], remaining)[0]:
            raise LimaPowerError("LIMA_DEADLINE")
        part = os.read(fd, len(expected) + 1 - len(data))
        if not part:
            if data == expected:
                return
            break
        data.extend(part)
        if not expected.startswith(data):
            break
    raise LimaPowerError("LIMA_RECHECK_UNPROVEN")


def serve(command, home, source, output):
    match = COMMAND.fullmatch(command) if isinstance(command, str) else None
    if match is None:
        raise LimaPowerError("LIMA_QUERY")
    action, node, uuid, challenge, millis = match.groups()
    if uuid == "0" * 32 or challenge == "0" * 32 or int(millis) > 120_000:
        raise LimaPowerError("LIMA_QUERY")
    deadline = time.monotonic_ns() + int(millis) * 1_000_000
    target = LimaTarget(int(node), uuid)
    power = LimaPower(home, read_only=True)

    def emit(state, process):
        if time.monotonic_ns() >= deadline:
            raise LimaPowerError("LIMA_DEADLINE")
        row = f"pgrac-lima-v1 {node} {uuid} {challenge} {state}"
        if process is not None:
            row += f" {process.pid} {process.start_sec} {process.start_usec}"
        print(row, file=output, flush=True)

    if action == "state":
        sample = power.observe(target, deadline)
        emit(sample.state.upper(), sample.host_process)
    else:
        def await_recheck(sample):
            emit("RUNNING", sample.host_process)
            read_recheck(source, deadline)

        evidence = power.observe_external_exit(target, deadline, await_recheck)
        emit("EXITED", evidence.before.host_process)


def main():
    try:
        if sys.platform != "darwin" or len(sys.argv) != 1:
            raise LimaPowerError("LIMA_QUERY")
        command = os.environ.get("SSH_ORIGINAL_COMMAND")
        account = pwd.getpwuid(os.getuid())
        os.environ.clear()
        os.environ.update(HOME=account.pw_dir, USER=account.pw_name, LOGNAME=account.pw_name,
                          PATH="/opt/homebrew/bin:/usr/bin:/bin:/usr/sbin:/sbin", LC_ALL="C")
        serve(command, Path(account.pw_dir) / ".lima-pre2-single", sys.stdin.buffer, sys.stdout)
        return 0
    except Exception:
        # Native errors and the peer-supplied command may contain private data.
        print("LIMA_OBSERVATION_UNPROVEN", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
