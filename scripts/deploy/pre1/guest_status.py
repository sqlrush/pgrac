#!/usr/bin/env python3
"""Fixed, read-only Linux guest observation program for PRE1.

Author: SqlRush <sqlrush@gmail.com>

This program never starts, stops or signals a database. A successful observation
is not clean-stop, storage or deployment qualification.
"""

import hashlib
import json
import os
from pathlib import Path
import pwd
import re
import stat
import subprocess
import sys

NODE_KEYS = {"node_id", "vm_uuid", "machine_id", "boot_id", "pgdata",
             "install_root", "uid", "gid"}
CONTROL_STATES = {"starting up", "shut down", "shut down in recovery",
                  "shutting down", "in crash recovery", "in archive recovery",
                  "in production"}


class ObservationError(Exception):
    def __init__(self, reason):
        self.reason = reason
        super().__init__(reason)


def digest(value):
    raw = (json.dumps(value, sort_keys=True, ensure_ascii=True,
                      separators=(",", ":"), allow_nan=False) + "\n").encode()
    return hashlib.sha256(raw).hexdigest()


def validate_request(request):
    if (type(request) is not dict or set(request) != {"action", "node", "binary_sha256"}
            or request["action"] != "status" or type(request["node"]) is not dict
            or set(request["node"]) != NODE_KEYS):
        raise ObservationError("REQUEST_INVALID")
    node = request["node"]
    for key in ("node_id", "uid", "gid"):
        minimum, maximum = (0, 3) if key == "node_id" else (1, 2147483647)
        if type(node[key]) is not int or not minimum <= node[key] <= maximum:
            raise ObservationError("REQUEST_INVALID")
    patterns = {"vm_uuid": r"[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}",
                "boot_id": r"[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}",
                "machine_id": r"[a-f0-9]{32}"}
    for key, pattern in patterns.items():
        if type(node[key]) is not str or not re.fullmatch(pattern, node[key]):
            raise ObservationError("REQUEST_INVALID")
    if type(request["binary_sha256"]) is not str or not re.fullmatch(r"[a-f0-9]{64}", request["binary_sha256"]):
        raise ObservationError("REQUEST_INVALID")
    for key in ("pgdata", "install_root"):
        value = node[key]
        if type(value) is not str or not 1 <= len(value) <= 4096 or any(ord(c) < 32 for c in value):
            raise ObservationError("REQUEST_INVALID")
        path = Path(value)
        if (not path.is_absolute() or str(path) != value or ".." in path.parts
                or len(path.parts) < 3 or path.parts[1] in ("root", "home", "Users", "proc", "sys", "dev")):
            raise ObservationError("PATH_NOT_CANONICAL")
    return request


def read_small(path, limit=16384):
    with open(path, "rb") as source:
        value = source.read(limit + 1)
    if len(value) > limit:
        raise ObservationError("OBSERVATION_TOO_LARGE")
    return value.decode("utf-8", errors="strict")


def checked_directory(value, allow_absent=False):
    path = Path(value)
    if not path.is_absolute() or str(path) != value or ".." in path.parts:
        raise ObservationError("PATH_NOT_CANONICAL")
    try:
        actual = path.resolve(strict=True)
    except FileNotFoundError:
        if (allow_absent and not path.is_symlink()
                and path.parent.resolve(strict=True) == path.parent):
            return None
        raise ObservationError("PATH_UNAVAILABLE") from None
    if actual != path or not path.is_dir():
        raise ObservationError("PATH_NOT_CANONICAL")
    return path


def file_hash(path):
    checksum = hashlib.sha256()
    with open(path, "rb") as source:
        metadata = os.fstat(source.fileno())
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_size > 1024 * 1024 * 1024:
            raise ObservationError("FILE_INVALID")
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            checksum.update(chunk)
        after = os.fstat(source.fileno())
        if (metadata.st_size, metadata.st_mtime_ns, metadata.st_ctime_ns) != (
                after.st_size, after.st_mtime_ns, after.st_ctime_ns):
            raise ObservationError("FILE_CHANGED")
    return checksum.hexdigest()


def read_identity():
    detected = subprocess.run(["/usr/bin/systemd-detect-virt", "--vm"],
                              capture_output=True, text=True, timeout=5)
    container = subprocess.run(["/usr/bin/systemd-detect-virt", "--container"],
                               capture_output=True, text=True, timeout=5)
    if detected.returncode != 0 or detected.stdout.strip() != "kvm" or container.returncode != 1:
        raise ObservationError("INDEPENDENT_KERNEL_UNPROVEN")
    return {"vm_uuid": read_small("/sys/class/dmi/id/product_uuid").strip().lower(),
            "machine_id": read_small("/etc/machine-id").strip().lower(),
            "boot_id": read_small("/proc/sys/kernel/random/boot_id").strip().lower()}


def parse_control(output, stderr=""):
    # Native pg_controldata can exit zero after reporting corrupt/untrusted data.
    # Keep the raw command result, but never turn its printed state into proof.
    if stderr.strip() or any(line.startswith("WARNING:") for line in output.splitlines()):
        return None
    wanted = {"Database system identifier": "system_identifier", "Database cluster state": "state"}
    result = {}
    for line in output.splitlines():
        key, separator, value = line.partition(":")
        if separator and key in wanted:
            name = wanted[key]
            if name in result:
                raise ObservationError("CONTROL_OUTPUT_INVALID")
            result[name] = value.strip()
    if (set(result) != set(wanted.values()) or result["state"] not in CONTROL_STATES
            or not re.fullmatch(r"[1-9][0-9]{0,19}", result["system_identifier"])
            or int(result["system_identifier"]) > 18446744073709551615):
        raise ObservationError("CONTROL_OUTPUT_INVALID")
    return result


def control_result(completed):
    if len(completed.stdout) > 16384 or len(completed.stderr) > 16384:
        raise ObservationError("OBSERVATION_TOO_LARGE")
    return {"rc": completed.returncode, "stdout": completed.stdout, "stderr": completed.stderr,
            "parsed": parse_control(completed.stdout, completed.stderr) if completed.returncode == 0 else None}


def read_control(node):
    executable = Path(node["install_root"]) / "bin/pg_controldata"
    if executable.resolve(strict=True) != executable or not executable.is_file():
        raise ObservationError("CONTROL_EXECUTABLE_INVALID")
    argv = [str(executable), "-D", node["pgdata"]]
    # Native tools run as the dedicated database user, never as root.
    if os.getuid() == 0:
        username = pwd.getpwuid(node["uid"]).pw_name
        argv = ["/usr/sbin/runuser", "-u", username, "--"] + argv
    elif (os.getuid(), os.getgid()) != (node["uid"], node["gid"]):
        raise ObservationError("DATABASE_USER_MISMATCH")
    return control_result(subprocess.run(argv, capture_output=True, text=True, timeout=10,
                                        env={"PATH": "/usr/bin:/bin", "LC_ALL": "C"}))


def parse_proc_stat(raw, expected_pid):
    try:
        pid_text, rest = raw.split(" (", 1)
        fields = rest.rsplit(") ", 1)[1].split()
        result = {"pid": int(pid_text), "ppid": int(fields[1]),
                  "starttime": int(fields[19]), "state": fields[0]}
        if result["pid"] != expected_pid or result["starttime"] <= 0 or len(result["state"]) != 1:
            raise ValueError
        return result
    except (ValueError, IndexError):
        raise ObservationError("PROCESS_STAT_INVALID") from None


def require_same_process(before, after):
    if any(before[key] != after[key] for key in ("pid", "starttime")):
        raise ObservationError("PROCESS_IDENTITY_CHANGED")


def process_record(entry, uid):
    try:
        status = read_small(entry / "status")
    except FileNotFoundError:
        return None  # An unrelated process may exit before any identity was observed.
    owners = [line.split()[1:] for line in status.splitlines() if line.startswith("Uid:")]
    if len(owners) != 1 or len(owners[0]) != 4:
        raise ObservationError("PROCESS_CENSUS_UNAVAILABLE")
    if uid not in [int(value) for value in owners[0]]:
        return None
    before = parse_proc_stat(read_small(entry / "stat"), int(entry.name))
    try:
        executable = os.readlink(entry / "exe")
    except FileNotFoundError:
        # Zombies have no exe: they are still present, never evidence of absence.
        if before["state"] == "Z":
            return {**before, "exe": None, "exe_sha256": None, "uid": uid}
        raise ObservationError("PROCESS_CENSUS_CHANGED") from None
    if Path(executable.removesuffix(" (deleted)")).name not in ("postgres", "postmaster"):
        return None
    checksum = file_hash(entry / "exe")
    after = parse_proc_stat(read_small(entry / "stat"), int(entry.name))
    require_same_process(before, after)
    return {**after, "exe": executable, "exe_sha256": checksum, "uid": uid}


def scan_processes(uid):
    try:
        with os.scandir("/proc") as entries:
            pids = [Path(entry.path) for entry in entries if entry.name.isdigit()]
        records = [record for entry in pids if (record := process_record(entry, uid)) is not None]
        return sorted(records, key=lambda record: record["pid"])
    except (OSError, ValueError):
        raise ObservationError("PROCESS_CENSUS_UNAVAILABLE") from None


def read_pidfile(pgdata):
    path = pgdata / "postmaster.pid"
    if not os.path.lexists(path):
        return None
    if path.resolve(strict=True) != path or not path.is_file():
        raise ObservationError("PIDFILE_INVALID")
    lines = read_small(path, 8192).splitlines()
    try:
        if len(lines) < 3 or lines[1] != str(pgdata) or int(lines[0]) <= 0 or int(lines[2]) <= 0:
            raise ValueError
        return {"pid": int(lines[0]), "pgdata": lines[1], "start_epoch": int(lines[2])}
    except ValueError:
        raise ObservationError("PIDFILE_INVALID") from None


def collect(request):
    validate_request(request)
    node = request["node"]
    identity = read_identity()
    if any(identity[key] != node[key] for key in ("vm_uuid", "machine_id", "boot_id")):
        raise ObservationError("GUEST_IDENTITY_MISMATCH")
    install = checked_directory(node["install_root"])
    postgres = install / "bin/postgres"
    if postgres.resolve(strict=True) != postgres or not postgres.is_file():
        raise ObservationError("DATABASE_EXECUTABLE_INVALID")
    if file_hash(postgres) != request["binary_sha256"]:
        raise ObservationError("DATABASE_BINARY_MISMATCH")
    pgdata = checked_directory(node["pgdata"], allow_absent=True)
    state, control, pidfile = "ABSENT", None, None
    if pgdata is not None:
        metadata = pgdata.stat()
        if (metadata.st_uid, metadata.st_gid) != (node["uid"], node["gid"]) or metadata.st_mode & 0o027:
            raise ObservationError("PGDATA_OWNERSHIP_INVALID")
        if not any(pgdata.iterdir()):
            state = "EMPTY"
        else:
            state = "PARTIAL"
            version = pgdata / "PG_VERSION"
            if (version.exists() and version.resolve(strict=True) == version
                    and read_small(version, 16).strip() == "16"):
                control_path = pgdata / "global/pg_control"
                if control_path.resolve(strict=True) != control_path or not control_path.is_file():
                    raise ObservationError("CONTROL_PATH_INVALID")
                state, control = "INITIALIZED", read_control(node)
                pidfile = read_pidfile(pgdata)
    processes = scan_processes(node["uid"])
    second = scan_processes(node["uid"])
    if [(p["pid"], p["starttime"], p["exe_sha256"]) for p in processes] != [
            (p["pid"], p["starttime"], p["exe_sha256"]) for p in second]:
        raise ObservationError("PROCESS_CENSUS_CHANGED")
    if read_identity() != identity:
        raise ObservationError("GUEST_IDENTITY_CHANGED")
    return {"node_id": node["node_id"], "identity": identity,
            "binary_sha256": request["binary_sha256"], "pgdata": node["pgdata"],
            "pgdata_state": state, "control": control, "pidfile": pidfile,
            "processes": second}


def main():
    request_sha = None
    def no_duplicates(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ObservationError("REQUEST_INVALID")
            result[key] = value
        return result
    try:
        raw = sys.stdin.buffer.read(16385)
        if len(raw) > 16384:
            raise ObservationError("REQUEST_INVALID")
        request = validate_request(json.loads(raw, object_pairs_hook=no_duplicates))
        request_sha = digest(request)
        observation = collect(request)
        result = {"status": "PASS", "reason": "NODE_OBSERVED", "observation": observation}
    except ObservationError as exc:
        result = {"status": "BLOCKED", "reason": exc.reason, "observation": None}
    except (OSError, ValueError, KeyError, TypeError, RecursionError, subprocess.SubprocessError):
        result = {"status": "ERROR", "reason": "GUEST_OBSERVATION_UNAVAILABLE", "observation": None}
    result.update({"schema_version": 1, "kind": "pre1-node-observation",
                   "request_sha256": request_sha, "deployment_qualified": False})
    print(json.dumps(result, sort_keys=True))
    return {"PASS": 0, "BLOCKED": 2, "ERROR": 3}[result["status"]]


if __name__ == "__main__":
    sys.exit(main())
