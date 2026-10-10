"""Local management transport for the four guests in the PRE2 appliance."""

import base64
import json
from pathlib import Path
import subprocess
import time

import libvirt
import libvirt_qemu

NAMES = tuple("pgrac-n%d" % n for n in range(4))


def command(argv, timeout=60, check=True):
    result = subprocess.run(argv, capture_output=True, text=True, timeout=timeout)
    if check and result.returncode:
        raise RuntimeError("command failed (%s): %s" % (argv[0], result.stderr[-1500:]))
    return result


def rpc(node, request):
    if type(node) is not int or node not in range(4):
        raise ValueError("invalid appliance node")
    # File RPCs can contain credentials. Keep the payload in memory, never argv.
    connection = libvirt.open("qemu:///system")
    if connection is None:
        raise RuntimeError("cannot connect to appliance hypervisor")
    try:
        domain = connection.lookupByName(NAMES[node])
        value = json.loads(libvirt_qemu.qemuAgentCommand(domain, json.dumps(request), 15, 0))
    finally:
        connection.close()
    if "error" in value:
        raise RuntimeError("guest agent refused operation: " + str(value["error"]))
    return value["return"]


def execute(node, argv, timeout=120, check=True):
    started = rpc(node, {"execute": "guest-exec", "arguments": {
        "path": argv[0], "arg": argv[1:], "capture-output": True}})
    until = time.monotonic() + timeout
    while time.monotonic() < until:
        status = rpc(node, {"execute": "guest-exec-status", "arguments": {"pid": started["pid"]}})
        if status.get("exited"):
            row = {"rc": status.get("exitcode", 128 + status.get("signal", 0)),
                   "stdout": base64.b64decode(status.get("out-data", "")).decode(errors="replace"),
                   "stderr": base64.b64decode(status.get("err-data", "")).decode(errors="replace")}
            if status.get("out-truncated") or status.get("err-truncated"):
                raise RuntimeError("guest output truncated")
            if check and row["rc"]:
                raise RuntimeError("node%d command failed: %s" % (node, row["stderr"][-1500:]))
            return row
        time.sleep(.2)
    raise TimeoutError("node%d guest command exceeded %ss" % (node, timeout))


def shell(node, script, timeout=120, check=True):
    return execute(node, ["/bin/bash", "-eu", "-o", "pipefail", "-c", script], timeout, check)


def write_file(node, path, content, mode=0o600):
    """Transport bytes through the agent file API, without logging secret argv."""
    parent = str(Path(path).parent)
    execute(node, ["/usr/bin/mkdir", "-p", parent])
    handle = rpc(node, {"execute": "guest-file-open", "arguments": {"path": path, "mode": "w"}})
    try:
        data = content.encode() if isinstance(content, str) else content
        for offset in range(0, len(data), 32768):
            block = data[offset:offset + 32768]
            result = rpc(node, {"execute": "guest-file-write", "arguments": {
                "handle": handle, "buf-b64": base64.b64encode(block).decode()}})
            if result.get("count") != len(block):
                raise RuntimeError("short guest file write")
        rpc(node, {"execute": "guest-file-flush", "arguments": {"handle": handle}})
    finally:
        rpc(node, {"execute": "guest-file-close", "arguments": {"handle": handle}})
    execute(node, ["/usr/bin/chmod", "%04o" % mode, path])


def read_file(node, path):
    handle = rpc(node, {"execute": "guest-file-open", "arguments": {"path": path, "mode": "r"}})
    chunks = []
    try:
        while True:
            row = rpc(node, {"execute": "guest-file-read", "arguments": {"handle": handle, "count": 32768}})
            chunks.append(base64.b64decode(row.get("buf-b64", "")))
            if row.get("eof") or not row.get("count"):
                return b"".join(chunks)
    finally:
        rpc(node, {"execute": "guest-file-close", "arguments": {"handle": handle}})
