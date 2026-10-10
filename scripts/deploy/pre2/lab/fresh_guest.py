#!/usr/bin/env python3
"""Create and verify one original native writer on this guest.

Author: SqlRush <sqlrush@gmail.com>

Reads one JSON request on stdin, writes one JSON result on stdout. It runs the
candidate's own initdb native-writer entry with data checksums enabled; the
founder (thread 1) also creates the shared DATA base. It verifies the result
from the candidate's own files and tools, and preserves every partial target
on failure: it never deletes, retries or overwrites.

Modes:
  lab               guest identity and GFS2 mount are verified first
  local-validation  single-host script validation; the result is never
                    deployment-qualified and records that it skipped them
"""

import hashlib
import json
import os
from pathlib import Path
import pwd
import re
import subprocess
import sys

# Run with python3 -I on guests: only this file's own directory is trusted for imports.
sys.path.insert(0, str(Path(__file__).resolve().parent))

from lab_common import (ERROR, FOUNDER_THREAD, PASS, PreflightError, file_sha256,
                        safe_remote_path)
from fresh_plan import initdb_argv
import guest_status
from guest_status import ObservationError, parse_control

KEYS = {"schema_version", "action", "mode", "node", "binary_sha256", "shared_mount", "fs_uuid",
        "dataset_root", "wal_threads_dir", "pgdata", "wal_dir", "thread", "shared_base",
        "storage_uuid", "database_incarnation", "system_identifier", "config_request",
        "config_path", "config_sha256"}
TOOLS = ("postgres", "initdb", "pg_controldata", "pg_checksums")
ROOT_NOT_PUBLISHED = "shared startup authority is not published"
OUTPUT_LIMIT = 65536


def validate(request):
    if type(request) is not dict or set(request) != KEYS or request["schema_version"] != 1 \
            or request["action"] != "create-native-writer" \
            or request["mode"] not in ("lab", "local-validation"):
        raise PreflightError("GUEST_REQUEST_INVALID")
    node = request["node"]
    if type(node) is not dict or type(node.get("uid")) is not int or type(node.get("gid")) is not int:
        raise PreflightError("GUEST_REQUEST_INVALID", "node")
    if type(request["binary_sha256"]) is not str or not re.fullmatch(r"[0-9a-f]{64}", request["binary_sha256"]):
        raise PreflightError("GUEST_REQUEST_INVALID", "binary_sha256")
    for key in ("shared_mount", "dataset_root", "wal_threads_dir", "pgdata", "wal_dir"):
        if type(request[key]) is not str:
            raise PreflightError("GUEST_REQUEST_INVALID", key)
        safe_remote_path(request[key], key)
    thread = request["thread"]
    if type(thread) is not int or not 1 <= thread <= 128 or thread != node.get("node_id", -1) + 1:
        raise PreflightError("GUEST_REQUEST_INVALID", "thread")
    if Path(request["wal_dir"]) != Path(request["wal_threads_dir"]) / ("thread_%d" % thread):
        raise PreflightError("GUEST_REQUEST_INVALID", "wal_dir")
    founder = thread == FOUNDER_THREAD
    if founder != (request["shared_base"] is not None):
        raise PreflightError("GUEST_REQUEST_INVALID", "founder")
    if type(request["system_identifier"]) is not str or not re.fullmatch(
            r"[1-9][0-9]{0,19}", request["system_identifier"]):
        raise PreflightError("GUEST_REQUEST_INVALID", "system_identifier")
    config = (request["config_request"], request["config_path"], request["config_sha256"])
    if any(v is not None for v in config):
        if (not founder or type(config[0]) is not str or type(config[1]) is not str
                or hashlib.sha256(config[0].encode("utf-8")).hexdigest() != config[2]
                or Path(config[1]).parent != Path(request["pgdata"]).parent):
            raise PreflightError("GUEST_REQUEST_INVALID", "config_request")
        safe_remote_path(config[1], "config_path")
    if founder:
        if (type(request["storage_uuid"]) is not str
                or not re.fullmatch(r"[0-9a-f]{32}", request["storage_uuid"])
                or type(request["database_incarnation"]) is not int or request["database_incarnation"] < 1):
            raise PreflightError("GUEST_REQUEST_INVALID", "namespace")
        safe_remote_path(request["shared_base"], "shared_base")
    return founder


def as_database_user(node, argv):
    """Native tools run as the database user, never as root."""
    if os.getuid() == 0:
        return ["/usr/sbin/runuser", "-u", pwd.getpwuid(node["uid"]).pw_name, "--"] + argv
    if (os.getuid(), os.getgid()) != (node["uid"], node["gid"]):
        raise PreflightError("DATABASE_USER_MISMATCH")
    return argv


def native_env(node):
    """Minimal environment; the candidate's own lib directory for installs built with another prefix."""
    return {"PATH": "/usr/bin:/bin", "LC_ALL": "C", "LD_LIBRARY_PATH": node["install_root"] + "/lib"}


def run(node, argv, timeout):
    try:
        completed = subprocess.run(as_database_user(node, argv), capture_output=True, text=True,
                                   timeout=timeout, check=False, env=native_env(node))
    except subprocess.TimeoutExpired:
        return {"argv": argv, "rc": None, "timed_out": True, "stdout": "", "stderr": ""}
    return {"argv": argv, "rc": completed.returncode, "timed_out": False,
            "stdout": completed.stdout[-OUTPUT_LIMIT:], "stderr": completed.stderr[-OUTPUT_LIMIT:]}


def require_owned_directory(path, node, field):
    try:
        metadata = os.lstat(path)
    except OSError:
        raise PreflightError("DIRECTORY_UNAVAILABLE", field) from None
    if (not Path(path).is_dir() or os.path.realpath(path) != str(path)
            or (metadata.st_uid, metadata.st_gid) != (node["uid"], node["gid"])
            or metadata.st_mode & 0o022):
        raise PreflightError("DIRECTORY_NOT_OWNED", field)


def require_new_target(path, node, field):
    """Absent, or an empty directory owned by the database user; parent must exist."""
    require_owned_directory(str(Path(path).parent), node, field + ".parent")
    if os.path.lexists(path):
        require_owned_directory(path, node, field)
        if any(os.scandir(path)):
            raise PreflightError("TARGET_NOT_EMPTY", field)


def make_layout_directory(path, node, field):
    """Create one new layout directory; an existing one must be empty and owned."""
    try:
        os.mkdir(path, 0o700)
    except FileExistsError:
        require_owned_directory(path, node, field)
        if any(os.scandir(path)):
            raise PreflightError("TARGET_NOT_EMPTY", field) from None
        return False
    except OSError:
        raise PreflightError("LAYOUT_CREATE_FAILED", field) from None
    if os.getuid() == 0:
        os.chown(path, node["uid"], node["gid"])
    return True


def require_mount(request):
    observed = subprocess.run(["/usr/bin/findmnt", "--json", "--mountpoint", request["shared_mount"],
                               "-o", "TARGET,FSTYPE,UUID,OPTIONS"],
                              capture_output=True, text=True, timeout=10, check=False)
    try:
        mounts = json.loads(observed.stdout)["filesystems"]
        mount = mounts[0]
        options = set(mount["options"].split(","))
        if (observed.returncode != 0 or len(mounts) != 1 or mount["target"] != request["shared_mount"]
                or mount["fstype"] != "gfs2" or mount["uuid"] != request["fs_uuid"]
                or "rw" not in options or options & {"ro", "localflocks", "lock_nolock"}):
            raise ValueError
    except (ValueError, KeyError, TypeError, IndexError):
        raise PreflightError("SHARED_MOUNT_UNPROVEN") from None


def control_facts(node, pgdata):
    command = run(node, [node["install_root"] + "/bin/pg_controldata", "-D", pgdata], 30)
    if command["rc"] != 0:
        raise PreflightError("CONTROL_READ_FAILED")
    parsed = parse_control(command["stdout"], command["stderr"])
    if parsed is None:
        raise PreflightError("CONTROL_OUTPUT_UNTRUSTED")
    fields = {}
    for line in command["stdout"].splitlines():
        key, _, value = line.partition(":")
        if key in ("Data page checksum version", "Catalog version number", "Latest checkpoint location"):
            fields[key] = value.strip()
    return dict(parsed, checksum_version=fields.get("Data page checksum version"),
                catalog_version=fields.get("Catalog version number"),
                checkpoint=fields.get("Latest checkpoint location"))


def verify(request, founder, initdb):
    node, pgdata, wal_dir = request["node"], request["pgdata"], request["wal_dir"]
    facts = {"initdb_reports_root_unpublished": ROOT_NOT_PUBLISHED in initdb["stdout"]}
    control = control_facts(node, pgdata)
    facts["control"] = control
    if control["state"] != "shut down":
        raise PreflightError("WRITER_NOT_SHUT_DOWN")
    if control["checksum_version"] != "1":
        raise PreflightError("DATA_CHECKSUMS_NOT_ENABLED")
    if control["system_identifier"] != request["system_identifier"]:
        raise PreflightError("SYSTEM_IDENTIFIER_MISMATCH")
    link = Path(pgdata) / "pg_wal"
    if not link.is_symlink() or os.path.realpath(link) != wal_dir:
        raise PreflightError("WAL_THREAD_ROUTE_INVALID")
    segments = sorted(p.name for p in Path(wal_dir).iterdir() if re.fullmatch(r"[0-9A-F]{24}", p.name))
    if not segments:
        raise PreflightError("WAL_THREAD_EMPTY")
    facts["wal_segments"] = segments
    facts["startup_binding_present"] = (Path(pgdata) / "global/pgrac_control_binding").exists()
    if founder:
        if not facts["initdb_reports_root_unpublished"]:
            raise PreflightError("CREATOR_OUTPUT_UNRECOGNIZED")
        base = Path(request["shared_base"])
        side = base / "native_side" / "origin_0"
        if not (base / "global").is_dir() or not (base / "base").is_dir() or not side.is_dir():
            raise PreflightError("SHARED_BASE_INCOMPLETE")
        facts["shared_base_root_present"] = (base / "global/pgrac_control_root").exists()
        if request["config_request"] is not None:
            facts["initial_config_object"] = verify_config_object(base, request)
        facts["founder_side"] = sorted(str(p.relative_to(side)) for p in side.rglob("*") if p.is_file())
        checks = run(node, [node["install_root"] + "/bin/pg_checksums", "--check", "-D", pgdata], 900)
        facts["pg_checksums_rc"] = checks["rc"]
        if checks["rc"] != 0:
            raise PreflightError("DATA_CHECKSUMS_INVALID")
    if facts["startup_binding_present"] or facts.get("shared_base_root_present"):
        # Present authority would be outside this step's contract; report, never adopt.
        raise PreflightError("UNEXPECTED_STARTUP_AUTHORITY")
    return facts


def write_config_request(request, node):
    """New 0600 file owned by the database user; the creator refuses anything else."""
    path, data = request["config_path"], request["config_request"].encode("utf-8")
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        if os.write(fd, data) != len(data):
            raise PreflightError("CONFIG_REQUEST_SHORT_WRITE")
        os.fsync(fd)
        if os.getuid() == 0:
            os.fchown(fd, node["uid"], node["gid"])
    finally:
        os.close(fd)
    if file_sha256(path) != request["config_sha256"]:
        raise PreflightError("CONFIG_REQUEST_CHANGED")


def verify_config_object(base, request):
    """The creator must publish the exact request bytes as an independent immutable object."""
    path = base / "global" / "config_images" / ("1-%s.conf" % request["config_sha256"])
    try:
        metadata = os.lstat(path)
    except OSError:
        raise PreflightError("INITIAL_CONFIG_OBJECT_MISSING") from None
    if not path.is_file() or path.is_symlink() or metadata.st_nlink != 1 \
            or path.read_bytes() != request["config_request"].encode("utf-8"):
        raise PreflightError("INITIAL_CONFIG_OBJECT_INVALID")
    return {"path": str(path), "sha256": request["config_sha256"], "bytes": metadata.st_size}


def create(request):
    founder = validate(request)
    node = request["node"]
    result = {"schema_version": 1, "kind": "pre2-lab-native-writer", "status": ERROR,
              "state": "NATIVE_WRITER_INCOMPLETE", "node_id": node["node_id"],
              "thread": request["thread"], "mode": request["mode"],
              "deployment_qualified": False, "created_layout": [], "commands": []}
    try:
        if request["mode"] == "lab":
            identity = guest_status.read_identity()
            if any(identity[k] != node[k] for k in ("vm_uuid", "machine_id", "boot_id")):
                raise PreflightError("GUEST_IDENTITY_MISMATCH")
            require_mount(request)
        hashes = {name: file_sha256(Path(node["install_root"]) / "bin" / name) for name in TOOLS}
        result["tool_sha256"] = hashes
        if hashes["postgres"] != request["binary_sha256"]:
            raise PreflightError("DATABASE_BINARY_MISMATCH")
        if founder:
            for key in ("dataset_root", "wal_threads_dir"):
                if make_layout_directory(request[key], node, key):
                    result["created_layout"].append(request[key])
            require_new_target(request["shared_base"], node, "shared_base")
        else:
            require_owned_directory(request["wal_threads_dir"], node, "wal_threads_dir")
        require_new_target(request["pgdata"], node, "pgdata")
        require_new_target(request["wal_dir"], node, "wal_dir")
        if request["config_request"] is not None:
            write_config_request(request, node)
            result["config_request"] = {"path": request["config_path"], "sha256": request["config_sha256"]}
        layout = {"pgdata": request["pgdata"], "wal_dir": request["wal_dir"], "thread": request["thread"],
                  "shared_base": request["shared_base"]}
        argv = initdb_argv(node["install_root"], pwd.getpwuid(node["uid"]).pw_name, layout,
                           {"storage_uuid": request["storage_uuid"],
                            "database_incarnation": request["database_incarnation"]},
                           request["system_identifier"], request["config_path"])
        initdb = run(node, argv, 1800)
        result["commands"].append(dict(stage="initdb", **initdb))
        if initdb["rc"] != 0 or initdb["timed_out"]:
            raise PreflightError("NATIVE_INITDB_FAILED")
        result["facts"] = verify(request, founder, initdb)
        result["system_identifier"] = result["facts"]["control"]["system_identifier"]
        if any(file_sha256(Path(node["install_root"]) / "bin" / n) != h for n, h in hashes.items()):
            raise PreflightError("TOOL_CHANGED")
        result.update(status=PASS, state="NATIVE_WRITER_CREATED",
                      deployment_qualified=request["mode"] == "lab")
    except (PreflightError, ObservationError) as exc:
        result["state"] = exc.reason
    except (OSError, ValueError, KeyError, subprocess.SubprocessError):
        result["state"] = "NATIVE_WRITER_UNAVAILABLE"
    return result


def main():
    """JSON request on stdin, or --request-b64 for actions whose stdin/stdout carry a tar stream."""
    import base64
    import cohort_guest
    if len(sys.argv) == 3 and sys.argv[1] == "--request-b64":
        try:
            request = json.loads(base64.b64decode(sys.argv[2]))
            action = cohort_guest.STREAM_ACTIONS[request["action"]]
        except (ValueError, KeyError, TypeError):
            sys.stderr.write("GUEST_REQUEST_INVALID\n")
            return 2
        try:
            return action(request)
        except PreflightError as exc:
            sys.stderr.write(exc.reason + "\n")
            return 3
    try:
        request = json.loads(sys.stdin.read(4 * 1024 * 1024))
        action = request.get("action") if type(request) is dict else None
        if action in cohort_guest.JSON_ACTIONS:
            result = cohort_guest.JSON_ACTIONS[action](request)
        else:
            result = create(request)
    except (ValueError, PreflightError, OSError) as exc:
        result = {"status": ERROR, "state": getattr(exc, "reason", "GUEST_REQUEST_INVALID")}
    sys.stdout.write(json.dumps(result, sort_keys=True) + "\n")
    return 0 if result.get("status") == PASS else 1


if __name__ == "__main__":
    sys.exit(main())
