#!/usr/bin/env python3
"""Read-only PRE1 preflight command.

Author: SqlRush <sqlrush@gmail.com>
"""

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import shlex
import stat
import subprocess
import sys
import tempfile
import time
import uuid

from common import (PreflightError, document_sha, load_json,
                    publish_artifact, validate_profile)

# This first deployment slice inventories kernel identity, not disk authority.
# No result from these checks is permission to mount, format or start a database.
PENDING_CHECKS = ["LIBVIRT_DOMAIN_MAP", "STORAGE_IDENTITY", "FENCING_WITNESS",
                  "INSTALLED_BINARY", "RUNTIME_CONFIGURATION", "SEED_IDENTITY"]
IDENTITY_KEYS = {"node_id", "vm_uuid", "machine_id", "boot_id",
                 "virtualization", "kernel", "arch"}

# Constant program, sent to the remote interpreter; no manifest value is shell
# source. Only node_id is sent on stdin. The program reads identity, never writes.
IDENTITY_PROGRAM = r'''
import json, pathlib, platform, subprocess, sys
def read(path):
    return pathlib.Path(path).read_text().strip().lower()
container = subprocess.run(["systemd-detect-virt", "--container"], capture_output=True, text=True, timeout=5)
if container.returncode not in (0, 1):
    sys.exit(3)
virtual = subprocess.run(["systemd-detect-virt", "--vm"], capture_output=True, text=True, timeout=5)
if virtual.returncode not in (0, 1):
    sys.exit(3)
identity = json.load(sys.stdin)
try:
    domain = read("/sys/class/dmi/id/product_uuid")
except PermissionError:
    value = subprocess.run(["sudo", "-n", "cat", "/sys/class/dmi/id/product_uuid"], capture_output=True, text=True, timeout=5, check=True)
    domain = value.stdout.strip().lower()
print(json.dumps({"node_id": identity["node_id"], "vm_uuid": domain,
    "machine_id": read("/etc/machine-id"),
    "boot_id": read("/proc/sys/kernel/random/boot_id"),
    "virtualization": container.stdout.strip() if container.returncode == 0 else virtual.stdout.strip(),
    "kernel": platform.release(), "arch": platform.machine()}))
'''


class SafeParser(argparse.ArgumentParser):
    def error(self, _message):
        raise PreflightError("CLI_ARGUMENTS", "arguments")


def result(status, reason, scope="IDENTITY_ONLY", **fields):
    return {"status": status, "reason": reason, "scope": scope,
            "deployment_qualified": False, **fields}


def main(argv=None):
    try:
        parser = SafeParser(description=__doc__)
        sub = parser.add_subparsers(dest="operation", required=True, parser_class=SafeParser)
        for name in ("check-profile", "inventory", "verify"):
            command = sub.add_parser(name)
            command.add_argument("--profile", required=True, type=Path)
            if name == "inventory":
                command.add_argument("--out", required=True, type=Path)
            if name == "verify":
                command.add_argument("--inventory", required=True, type=Path)
        args = parser.parse_args(argv)
        profile = validate_profile(load_json(args.profile))
        if args.operation == "check-profile":
            answer = result("PASS", "PROFILE_VALID", "PROFILE_ONLY", profile_sha256=document_sha(profile))
        elif args.operation == "inventory":
            # Fail before network work if publication would replace old evidence.
            if args.out.exists() or args.out.is_symlink():
                raise PreflightError("ARTIFACT_EXISTS", "out", "ERROR")
            observations, events = [], []
            run_id = str(uuid.uuid4())
            for node in profile["nodes"]:
                begin = time.monotonic_ns()
                utc = datetime.now(timezone.utc).isoformat()
                observed = collect_node(node)
                observations.append(observed)
                events.append({"run_id": run_id, "step_id": "GUEST_IDENTITY",
                               "node": node["node_id"], "begin_monotonic_ns": begin,
                               "end_monotonic_ns": time.monotonic_ns(), "utc": utc,
                               "rc": 0, "reason": "COLLECTED",
                               "command": "ssh pinned-host python3 identity-probe",
                               "probe_sha256": document_sha(IDENTITY_PROGRAM),
                               "artifact_sha": document_sha(observed)})
            inventory = build_inventory(profile, observations)
            inventory["events"] = events
            sha = publish_artifact(args.out, inventory)
            answer = result("BLOCKED", "QUALIFICATION_PENDING", artifact_sha256=sha,
                            pending_checks=PENDING_CHECKS)
        else:
            answer = verify_inventory(profile, load_json(args.inventory))
    except PreflightError as exc:
        answer = result(exc.status, exc.reason, field=exc.field)
    except (OSError, ValueError, KeyError, TypeError, RecursionError):
        # Raw OS, JSON and remote errors may contain credentials or input text.
        answer = result("ERROR", "EVIDENCE_INVALID")
    print(json.dumps(answer, sort_keys=True))
    return {"PASS": 0, "BLOCKED": 2, "ERROR": 3}[answer["status"]]


def collect_node(node):
    admin = node["admin_endpoint"]
    try:
        key = Path(admin["identity_file"])
        metadata = key.lstat()
        if (not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != os.getuid()
                or metadata.st_mode & 0o077 or not os.access(key, os.R_OK)):
            raise OSError
    except OSError as exc:
        raise PreflightError("SSH_IDENTITY_UNAVAILABLE", "admin_endpoint.identity_file", "ERROR") from exc
    host, port = admin["host"], admin["port"]
    known_name = host if port == 22 else f"[{host}]:{port}"
    with tempfile.NamedTemporaryFile(mode="w", prefix="pre1-known-hosts-") as known:
        known.write(f"{known_name} {node['ssh_host_key']}\n")
        known.flush()
        argv = ["ssh", "-F", "/dev/null", "-T", "-p", str(port),
                "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes",
                "-o", f"UserKnownHostsFile={known.name}",
                "-o", "GlobalKnownHostsFile=/dev/null", "-o", "UpdateHostKeys=no",
                "-o", "ConnectTimeout=10", "-o", "ConnectionAttempts=1",
                "-o", "ForwardAgent=no", "-o", "ForwardX11=no",
                "-o", "IdentitiesOnly=yes", "-o", "IdentityAgent=none", "-o", "IdentityFile=none",
                "-i", admin["identity_file"], "-l", admin["user"], host,
                "python3 -I -c " + shlex.quote(IDENTITY_PROGRAM)]
        try:
            completed = subprocess.run(argv, input=json.dumps({"node_id": node["node_id"]}),
                                       capture_output=True, text=True, timeout=25, check=False)
        except subprocess.TimeoutExpired as exc:
            raise PreflightError("SSH_TIMEOUT", "admin_endpoint", "ERROR") from exc
        except OSError as exc:
            raise PreflightError("SSH_EXECUTION", "admin_endpoint", "ERROR") from exc
    if completed.returncode:
        raise PreflightError("SSH_COMMAND_FAILED", "admin_endpoint", "ERROR")
    try:
        if len(completed.stdout) > 8192:
            raise ValueError
        observed = json.loads(completed.stdout)
        if type(observed) is not dict or set(observed) != IDENTITY_KEYS:
            raise ValueError
        if type(observed["node_id"]) is not int:
            raise ValueError
        if any(type(observed[k]) is not str or not 1 <= len(observed[k]) <= 128
               or any(ord(c) < 32 for c in observed[k]) for k in IDENTITY_KEYS - {"node_id"}):
            raise ValueError
        return observed
    except (ValueError, TypeError) as exc:
        raise PreflightError("SSH_OUTPUT_INVALID", "admin_endpoint", "ERROR") from exc


def build_inventory(profile, nodes):
    validate_profile(profile)
    if type(nodes) is not list or any(type(n) is not dict or set(n) != IDENTITY_KEYS
            or type(n["node_id"]) is not int
            or any(type(n[k]) is not str or not 1 <= len(n[k]) <= 128
                   or any(ord(c) < 32 for c in n[k]) for k in IDENTITY_KEYS - {"node_id"})
            for n in nodes):
        raise PreflightError("OBSERVATION_INVALID", "nodes", "ERROR")
    if len(nodes) != 4 or len({n["node_id"] for n in nodes}) != 4:
        raise PreflightError("OBSERVED_IDENTITY_MISMATCH", "nodes")
    expected = {n["node_id"]: n for n in profile["nodes"]}
    architecture = "aarch64" if profile["profile_id"] == "pre1-gfs2-arm64-lab-v1" else "x86_64"
    for observed in nodes:
        if set(observed) != IDENTITY_KEYS or observed["node_id"] not in expected:
            raise PreflightError("OBSERVED_IDENTITY_MISMATCH", "nodes")
        if observed["virtualization"] != "kvm":
            raise PreflightError("INDEPENDENT_KERNEL_UNPROVEN", "nodes.virtualization")
        if observed["arch"] != architecture:
            raise PreflightError("PLATFORM_MISMATCH", "nodes.arch")
        for key in ("vm_uuid", "machine_id", "boot_id"):
            if observed[key] != expected[observed["node_id"]][key]:
                raise PreflightError("OBSERVED_IDENTITY_MISMATCH", f"nodes.{key}")
    return {"schema_version": 1, "kind": "pre1-identity-inventory",
            "profile_sha256": document_sha(profile), "nodes": nodes, "events": [],
            "pending_checks": list(PENDING_CHECKS), "deployment_qualified": False}


def verify_inventory(profile, inventory):
    keys = {"schema_version", "kind", "profile_sha256", "nodes", "events",
            "pending_checks", "deployment_qualified"}
    if type(inventory) is not dict or set(inventory) != keys:
        raise PreflightError("INVENTORY_INVALID", "inventory", "ERROR")
    if inventory["profile_sha256"] != document_sha(profile):
        raise PreflightError("INVENTORY_PROFILE_MISMATCH", "inventory.profile_sha256")
    if (type(inventory["schema_version"]) is not int or inventory["schema_version"] != 1 or
            inventory["kind"] != "pre1-identity-inventory" or
            inventory["deployment_qualified"] is not False or
            inventory["pending_checks"] != PENDING_CHECKS):
        raise PreflightError("INVENTORY_INVALID", "inventory", "ERROR")
    build_inventory(profile, inventory["nodes"])
    return result("BLOCKED", "QUALIFICATION_PENDING", pending_checks=PENDING_CHECKS)


if __name__ == "__main__":
    sys.exit(main())
