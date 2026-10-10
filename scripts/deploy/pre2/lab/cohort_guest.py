"""Guest-side cohort actions: one original creation, exact PGDATA transfer, read-only bootstrap check.

Author: SqlRush <sqlrush@gmail.com>

All native tools run as the database user. Every target is new; a failure keeps
whatever was created for inspection and never deletes, retries or overwrites.
"""

import os
from pathlib import Path
import re
import subprocess
import sys

import cohort
from fresh_guest import (as_database_user, control_facts, file_sha256, make_layout_directory,
                         require_mount, require_owned_directory, run, write_config_request)
import guest_status
from lab_common import ERROR, PASS, PreflightError, safe_remote_path

COHORT_KEYS = {"schema_version", "action", "mode", "node", "binary_sha256", "shared_mount", "fs_uuid",
               "dataset_root", "shared_roots", "cache_root", "members", "system_identifier",
               "config_request", "config_path", "config_sha256", "initdb_argv"}
ROOT_FILES = ("pgrac_control_root", "pgrac_control_root.bak", "pgrac_catalog_authority", "pgrac_oid_authority",
              "pgrac_xid_authority", "pgrac_xid_authority.bak")
# pgrac_cf_contract is the original owner's storage contract; startup never writes a missing one.
MEMBER_FILES = ("global/pg_control", "global/pgrac_control_binding", "global/pgrac_cf_contract")


def lab_identity(request):
    if request["mode"] != "lab":
        return
    node = request["node"]
    identity = guest_status.read_identity()
    if any(identity[k] != node[k] for k in ("vm_uuid", "machine_id", "boot_id")):
        raise PreflightError("GUEST_IDENTITY_MISMATCH")


def tool_hashes(node, binary_sha256, names=("postgres", "initdb", "pg_controldata")):
    hashes = {name: file_sha256(Path(node["install_root"]) / "bin" / name) for name in names}
    if hashes["postgres"] != binary_sha256:
        raise PreflightError("DATABASE_BINARY_MISMATCH")
    return hashes


def root_hashes(shared_data):
    out = {}
    for name in ROOT_FILES:
        path = Path(shared_data) / "global" / name
        out[name] = file_sha256(path) if path.is_file() else None
    return out


def validate_cohort(request):
    if type(request) is not dict or set(request) != COHORT_KEYS or request["mode"] not in ("lab", "local-validation"):
        raise PreflightError("GUEST_REQUEST_INVALID")
    for key in ("shared_mount", "dataset_root", "cache_root", "config_path"):
        safe_remote_path(request[key], key)
    roots = request["shared_roots"]
    if type(roots) is not dict or set(roots) != {"data", "wal", "undo"}:
        raise PreflightError("GUEST_REQUEST_INVALID", "shared_roots")
    for key, path in roots.items():
        safe_remote_path(path, "shared_roots." + key)
        if Path(path).parent != Path(request["dataset_root"]):
            raise PreflightError("GUEST_REQUEST_INVALID", "shared_roots." + key)
    expected = cohort.initdb_argv(request["node"]["install_root"], request["cache_root"], request["config_path"])
    if request["initdb_argv"] != expected:
        raise PreflightError("GUEST_REQUEST_INVALID", "initdb_argv")
    if not re.fullmatch(r"[1-9][0-9]{0,19}", str(request["system_identifier"])):
        raise PreflightError("GUEST_REQUEST_INVALID", "system_identifier")


def create_cohort(request):
    validate_cohort(request)
    node = request["node"]
    result = {"schema_version": 1, "kind": "pre2-lab-cohort", "status": ERROR, "state": "COHORT_INCOMPLETE",
              "mode": request["mode"], "deployment_qualified": False, "created_layout": [], "commands": []}
    try:
        lab_identity(request)
        if request["mode"] == "lab":
            require_mount(request)
        result["tool_sha256"] = tool_hashes(node, request["binary_sha256"])
        if make_layout_directory(request["dataset_root"], node, "dataset_root"):
            result["created_layout"].append(request["dataset_root"])
        for key, path in sorted(request["shared_roots"].items()):
            if os.path.lexists(path):
                raise PreflightError("SHARED_ROOT_EXISTS", key)
        require_owned_directory(str(Path(request["cache_root"]).parent), node, "cache_root.parent")
        if os.path.lexists(request["cache_root"]):
            raise PreflightError("CACHE_ROOT_EXISTS")
        write_config_request(request, node)
        result["config_request"] = {"path": request["config_path"], "sha256": request["config_sha256"]}
        command = run(node, request["initdb_argv"], 3600)
        result["commands"].append(dict(stage="initdb-cohort", **command))
        if command["rc"] != 0 or command["timed_out"] or cohort.COHORT_CREATED not in command["stdout"]:
            raise PreflightError("COHORT_INITDB_FAILED")
        result["facts"] = verify_cohort(request)
        result.update(status=PASS, state="COHORT_CREATED", deployment_qualified=request["mode"] == "lab")
    except PreflightError as exc:
        result["state"] = exc.reason
    except (OSError, ValueError, KeyError, subprocess.SubprocessError):
        result["state"] = "COHORT_UNAVAILABLE"
    return result


def verify_cohort(request):
    node, cache = request["node"], Path(request["cache_root"])
    facts = {"root": root_hashes(request["shared_roots"]["data"]), "members": {}}
    if facts["root"]["pgrac_control_root"] is None:
        raise PreflightError("COHORT_ROOT_MISSING")
    for member in request["members"]:
        data = cache / ("node_%d" % member)
        if any(not (data / name).is_file() for name in MEMBER_FILES):
            raise PreflightError("COHORT_MEMBER_INCOMPLETE", "node_%d" % member)
        wal = os.readlink(data / "pg_wal")
        if not wal.startswith(request["shared_roots"]["wal"] + "/thread_%d/" % (member + 1)):
            raise PreflightError("COHORT_MEMBER_WAL_ROUTE", "node_%d" % member)
        control = control_facts(node, str(data))
        if (control["state"] != "shut down" or control["system_identifier"] != str(request["system_identifier"])
                or control["checksum_version"] != "1"):
            raise PreflightError("COHORT_MEMBER_CONTROL", "node_%d" % member)
        facts["members"][member] = {"wal": wal, "control": control,
                                    "side_archive": (data / "pgrac_initdb_native_side").is_dir()}
    return facts


def pgdata_manifest(request):
    safe_remote_path(request["path"], "path")
    return {"status": PASS, "manifest": cohort.manifest(request["path"])}


def install_pgdata(request):
    """Verify a received node_N against the source manifest, then make it this member's PGDATA."""
    node = request["node"]
    staging, pgdata = Path(request["staging"]), Path(request["pgdata"])
    received = staging / request["source_name"]
    for key in ("staging", "pgdata"):
        safe_remote_path(request[key], key)
    if staging.parent != pgdata.parent or os.path.lexists(pgdata):
        raise PreflightError("PGDATA_TARGET_INVALID")
    require_owned_directory(str(staging), node, "staging")
    actual = cohort.manifest(received)
    if actual["sha256"] != request["expected_manifest_sha256"]:
        return {"status": ERROR, "state": "PGDATA_MANIFEST_MISMATCH", "manifest_sha256": actual["sha256"]}
    os.rename(received, pgdata)
    os.rmdir(staging)
    fd = os.open(pgdata.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    if cohort.manifest(pgdata)["sha256"] != request["expected_manifest_sha256"]:
        return {"status": ERROR, "state": "PGDATA_CHANGED_AFTER_INSTALL"}
    return {"status": PASS, "state": "PGDATA_INSTALLED", "manifest_sha256": actual["sha256"],
            "entries": actual["count"]}


def bootstrap_check(request):
    """Candidate's read-only early preparation on the installed PGDATA (postgres -C)."""
    node = request["node"]
    before = root_hashes(request["shared_data_dir"])
    argv = [node["install_root"] + "/bin/postgres", "-D", request["pgdata"],
            "-c", "config_file=" + request["config_file"], "-C", "shared_memory_size"]
    command = run(node, argv, 300)
    after = root_hashes(request["shared_data_dir"])
    ok = command["rc"] == 0 and not command["timed_out"] and re.fullmatch(r"\d+\s*", command["stdout"] or "")
    return {"status": PASS if ok and before == after else ERROR,
            "state": "BOOTSTRAP_PREPARED" if ok else "BOOTSTRAP_REFUSED",
            "shared_memory_mb": int(command["stdout"]) if ok else None,
            "root_unchanged": before == after, "root": after, "command": command}


def export_pgdata(request):
    """Stream node_N as tar on stdout (binary; symlinks kept, hard links refused by the manifest)."""
    node = request["node"]
    argv = as_database_user(node, ["/usr/bin/tar", "-C", request["parent"], "-cpf", "-", request["name"]])
    return subprocess.run(argv, stdout=sys.stdout.buffer, timeout=1800, check=False).returncode


def import_pgdata(request):
    """Receive a tar stream from stdin into a new staging directory owned by the database user."""
    node = request["node"]
    staging = request["staging"]
    safe_remote_path(staging, "staging")
    require_owned_directory(str(Path(staging).parent), node, "staging.parent")
    if os.path.lexists(staging):
        return 17
    script = "umask 077; mkdir %s && exec /usr/bin/tar -C %s -xpf -" % (staging, staging)
    argv = as_database_user(node, ["/bin/sh", "-c", script])
    return subprocess.run(argv, stdin=sys.stdin.buffer, timeout=1800, check=False).returncode


JSON_ACTIONS = {"create-cohort": create_cohort, "pgdata-manifest": pgdata_manifest,
                "install-pgdata": install_pgdata, "bootstrap-check": bootstrap_check}
STREAM_ACTIONS = {"export-pgdata": export_pgdata, "import-pgdata": import_pgdata}
