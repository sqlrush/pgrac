#!/usr/bin/env python3
"""Collect read-only, identity-bound PRE1 guest status through pinned SSH.

Author: SqlRush <sqlrush@gmail.com>
"""

from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import stat
import subprocess
import sys
import tempfile
import time
import uuid

from common import (PreflightError, _schema_check, document_sha, ipv4,
                    load_json, paths_disjoint, publish_artifact, safe_remote_path)
from preflight import SafeParser
from guest_status import ObservationError, parse_control


def validate_node(node, binary_sha256):
    schema = load_json(Path(__file__).with_name("profile.schema.json"))
    _schema_check(node, schema["$defs"]["node"], schema["$defs"], "node")
    if type(binary_sha256) is not str or not re.fullmatch(r"[a-f0-9]{64}", binary_sha256):
        raise PreflightError("FIELD_VALUE", "binary_sha256")
    ipv4(node["admin_endpoint"]["host"], "node.admin_endpoint.host")
    paths_disjoint([safe_remote_path(node[name], f"node.{name}")
                    for name in ("pgdata", "install_root", "log_root")], "node")
    key = Path(node["admin_endpoint"]["identity_file"])
    try:
        metadata = key.lstat()
        if (not key.is_absolute() or not stat.S_ISREG(metadata.st_mode)
                or metadata.st_uid != os.getuid() or metadata.st_mode & 0o077
                or not os.access(key, os.R_OK)):
            raise OSError
    except OSError:
        raise PreflightError("SSH_IDENTITY_UNAVAILABLE", "node.admin_endpoint.identity_file", "ERROR") from None


def request_for(node, binary_sha256):
    return {"action": "status", "binary_sha256": binary_sha256,
            "node": {key: node[key] for key in ("node_id", "vm_uuid", "machine_id",
                     "boot_id", "pgdata", "install_root", "uid", "gid")}}


def limited_text(value):
    if value is None:
        return ""
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="replace")
    return value[:1048576]


def run_ssh(node, program, request):
    admin = node["admin_endpoint"]
    host, port = admin["host"], admin["port"]
    with tempfile.NamedTemporaryFile(mode="w", prefix="pre1-known-hosts-") as known:
        name = host if port == 22 else f"[{host}]:{port}"
        known.write(f"{name} {node['ssh_host_key']}\n")
        known.flush()
        argv = ["ssh", "-F", "/dev/null", "-T", "-p", str(port),
                "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes",
                "-o", f"UserKnownHostsFile={known.name}", "-o", "GlobalKnownHostsFile=/dev/null",
                "-o", "UpdateHostKeys=no", "-o", "ConnectTimeout=10", "-o", "ConnectionAttempts=1",
                "-o", "ForwardAgent=no", "-o", "ForwardX11=no", "-o", "IdentitiesOnly=yes",
                "-o", "IdentityAgent=none", "-o", "IdentityFile=none",
                "-i", admin["identity_file"], "-l", admin["user"], host,
                "sudo -n /usr/bin/python3 -I -c " + shlex.quote(program)]
        try:
            completed = subprocess.run(argv, input=json.dumps(request), capture_output=True,
                                       text=True, timeout=45, check=False)
            return {"rc": completed.returncode, "stdout": limited_text(completed.stdout),
                    "stderr": limited_text(completed.stderr), "timed_out": False,
                    "truncated": len(completed.stdout) > 1048576 or len(completed.stderr) > 1048576}
        except subprocess.TimeoutExpired as exc:
            return {"rc": None, "stdout": limited_text(exc.stdout), "stderr": limited_text(exc.stderr),
                    "timed_out": True, "truncated": False}
        except OSError:
            return {"rc": None, "stdout": "", "stderr": "", "timed_out": False, "truncated": False}


def validate_facts(observation, node):
    control, pidfile = observation["control"], observation["pidfile"]
    if observation["pgdata_state"] == "INITIALIZED":
        if (type(control) is not dict or set(control) != {"rc", "stdout", "stderr", "parsed"}
                or type(control["rc"]) is not int
                or any(type(control[k]) is not str or len(control[k]) > 16384 for k in ("stdout", "stderr"))):
            raise ValueError
        expected = parse_control(control["stdout"], control["stderr"]) if control["rc"] == 0 else None
        if control["parsed"] != expected:
            raise ValueError
    elif control is not None or pidfile is not None:
        raise ValueError
    if pidfile is not None:
        if (type(pidfile) is not dict or set(pidfile) != {"pid", "pgdata", "start_epoch"}
                or pidfile["pgdata"] != node["pgdata"]
                or any(type(pidfile[k]) is not int or pidfile[k] <= 0 for k in ("pid", "start_epoch"))):
            raise ValueError
    seen = set()
    for process in observation["processes"]:
        if (type(process) is not dict or set(process) != {"pid", "ppid", "starttime", "state", "exe", "exe_sha256", "uid"}
                or any(type(process[k]) is not int or process[k] < (0 if k == "ppid" else 1)
                       for k in ("pid", "ppid", "starttime", "uid"))
                or process["uid"] != node["uid"] or process["pid"] in seen
                or process["state"] not in ("R", "S", "D", "T", "t", "X", "Z", "P", "I")):
            raise ValueError
        seen.add(process["pid"])
        if process["state"] == "Z" and process["exe"] is None and process["exe_sha256"] is None:
            continue
        if (type(process["exe"]) is not str or not process["exe"].startswith("/")
                or type(process["exe_sha256"]) is not str
                or not re.fullmatch(r"[a-f0-9]{64}", process["exe_sha256"])):
            raise ValueError


def decode_observation(transport, request):
    if transport["timed_out"]:
        return "ERROR", "SSH_TIMEOUT", None
    if transport["truncated"]:
        return "ERROR", "REMOTE_OUTPUT_TOO_LARGE", None
    if transport["rc"] not in (0, 2, 3):
        return "ERROR", "SSH_COMMAND_FAILED", None
    try:
        answer = json.loads(transport["stdout"])
        expected = {"status", "reason", "observation", "schema_version", "kind",
                    "request_sha256", "deployment_qualified"}
        if (type(answer) is not dict or set(answer) != expected
                or type(answer["schema_version"]) is not int or answer["schema_version"] != 1
                or answer["kind"] != "pre1-node-observation" or answer["deployment_qualified"] is not False
                or answer["request_sha256"] != document_sha(request)
                or answer["status"] not in ("PASS", "BLOCKED", "ERROR")
                or {"PASS": 0, "BLOCKED": 2, "ERROR": 3}[answer["status"]] != transport["rc"]
                or type(answer["reason"]) is not str or not re.fullmatch(r"[A-Z_]{1,80}", answer["reason"])):
            raise ValueError
        observation = answer["observation"]
        if answer["status"] == "PASS":
            node = request["node"]
            keys = {"node_id", "identity", "binary_sha256", "pgdata", "pgdata_state",
                    "control", "pidfile", "processes"}
            if (type(observation) is not dict or set(observation) != keys
                    or type(observation["node_id"]) is not int or observation["node_id"] != node["node_id"]
                    or observation["identity"] != {k: node[k] for k in ("vm_uuid", "machine_id", "boot_id")}
                    or observation["binary_sha256"] != request["binary_sha256"]
                    or observation["pgdata"] != node["pgdata"]
                    or observation["pgdata_state"] not in ("ABSENT", "EMPTY", "PARTIAL", "INITIALIZED")
                    or type(observation["processes"]) is not list):
                raise ValueError
            validate_facts(observation, node)
        elif observation is not None:
            raise ValueError
        return answer["status"], answer["reason"], observation
    except (ValueError, TypeError, KeyError, RecursionError, ObservationError):
        return "ERROR", "REMOTE_OUTPUT_INVALID", None


def observe(node, binary_sha256):
    validate_node(node, binary_sha256)
    request = request_for(node, binary_sha256)
    program = Path(__file__).with_name("guest_status.py").read_text()
    begin, utc = time.monotonic_ns(), datetime.now(timezone.utc).isoformat()
    transport = run_ssh(node, program, request)
    status, reason, observation = decode_observation(transport, request)
    return {"schema_version": 1, "kind": "pre1-remote-status", "run_id": str(uuid.uuid4()),
            "node_id": node["node_id"], "status": status, "reason": reason,
            "scope": "NODE_OBSERVATION_ONLY", "deployment_qualified": False,
            "node_sha256": document_sha(node), "request_sha256": document_sha(request),
            "program_sha256": hashlib.sha256(program.encode()).hexdigest(),
            "begin_monotonic_ns": begin, "end_monotonic_ns": time.monotonic_ns(), "utc": utc,
            "transport": transport, "observation": observation}


def main(argv=None):
    try:
        parser = SafeParser(description=__doc__)
        parser.add_argument("action", choices=("status",))
        parser.add_argument("--node", type=Path, required=True)
        parser.add_argument("--binary-sha256", required=True)
        parser.add_argument("--out", type=Path, required=True)
        args = parser.parse_args(argv)
        if os.path.lexists(args.out):
            raise PreflightError("ARTIFACT_EXISTS", "out", "ERROR")
        artifact = observe(load_json(args.node), args.binary_sha256)
        checksum = publish_artifact(args.out, artifact)
        answer = {"status": artifact["status"], "reason": artifact["reason"],
                  "scope": artifact["scope"], "artifact_sha256": checksum}
    except PreflightError as exc:
        answer = {"status": exc.status, "reason": exc.reason, "field": exc.field}
    except (OSError, ValueError, KeyError, TypeError, RecursionError):
        answer = {"status": "ERROR", "reason": "OBSERVATION_UNAVAILABLE"}
    answer["deployment_qualified"] = False
    print(json.dumps(answer, sort_keys=True))
    return {"PASS": 0, "BLOCKED": 2, "ERROR": 3}[answer["status"]]


if __name__ == "__main__":
    sys.exit(main())
