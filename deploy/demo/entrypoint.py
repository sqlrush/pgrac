#!/usr/bin/env python3
"""Non-root image entry point. Author: SqlRush <sqlrush@gmail.com>"""

import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys

from demo_lib import security_status, storage_relative


def main():
    security = security_status(Path("/proc/self/status").read_text(), os.getuid())
    if sys.argv[1:] == ["runtime-check"]:
        # No data/device mount: prove kernel security and real TCP admission first.
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP) as sock:
            sock.bind(("127.0.0.1", 0))
            sock.listen()
        print(json.dumps({"runtime_check": "PASS", "uid": os.getuid(), "security": security}))
        return 0
    if sys.argv[1:] == ["bootstrap"]:
        os.execvp("perl", ["perl", "/opt/pgrac/demo/bootstrap.pl"])
    if len(sys.argv) != 3 or sys.argv[1] != "node" or sys.argv[2] not in ("0", "1", "2", "3"):
        raise ValueError("use runtime-check, bootstrap or node 0..3")
    node = json.loads(Path("/demo/bootstrap.json").read_text())["nodes"][int(sys.argv[2])]
    storage_relative(node["pgdata"])
    storage_relative(node["log"])
    with open(node["log"], "ab", buffering=0) as log:
        process = subprocess.Popen(["postgres", "-D", node["pgdata"]], stdout=log, stderr=log)

        def stop(_sig, _frame):
            # SIGINT is PostgreSQL's normal fast shutdown, not immediate/kill.
            if process.poll() is None:
                process.send_signal(signal.SIGINT)

        signal.signal(signal.SIGTERM, stop)
        signal.signal(signal.SIGINT, stop)
        return process.wait()


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (ValueError, OSError, KeyError) as error:
        print("demo entry point refused: " + str(error), file=sys.stderr)
        sys.exit(1)
