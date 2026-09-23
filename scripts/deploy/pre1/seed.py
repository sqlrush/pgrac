#!/usr/bin/env python3
"""Create a guarded node0 seed or verify its plain native backup.

Author: SqlRush <sqlrush@gmail.com>
"""

import fcntl
import hashlib
import json
import os
from pathlib import Path
import pwd
import re
import select
import signal
import stat
import subprocess
import sys
import tempfile
import time

from common import (PreflightError, document_sha, publish_artifact, load_json,
                    paths_disjoint, safe_remote_path)
import guest_status
from guest_status import ObservationError, parse_control
from preflight import SafeParser


def canonical_directory(path):
    path = Path(path)
    if not path.is_absolute() or path.resolve(strict=True) != path or not path.is_dir():
        raise PreflightError("BACKUP_PATH_INVALID")
    return path


def checked_hash(path):
    """Refuse links/special files and a file that changes while being read."""
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except OSError:
        raise PreflightError("BACKUP_FILE_UNAVAILABLE") from None
    with os.fdopen(fd, "rb") as source:
        before = os.fstat(source.fileno())
        if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1:
            raise PreflightError("BACKUP_FILE_INVALID")
        digest = hashlib.sha256()
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
        after = os.fstat(source.fileno())
        fields = ("st_dev", "st_ino", "st_mode", "st_nlink", "st_uid", "st_gid",
                  "st_size", "st_mtime_ns", "st_ctime_ns")
        if any(getattr(before, field) != getattr(after, field) for field in fields):
            raise PreflightError("BACKUP_CHANGED")
    return {"sha256": digest.hexdigest(), "size": before.st_size}


def inventory(backup):
    entries = {}
    for directory, dirs, files in os.walk(backup, followlinks=False):
        relative = Path(directory).relative_to(backup)
        if len(relative.parts) > 16 or len(entries) + len(dirs) + len(files) > 100000:
            raise PreflightError("BACKUP_LAYOUT_UNSUPPORTED")
        for name in dirs:
            path = Path(directory) / name
            if path.is_symlink():
                raise PreflightError("BACKUP_LINK_REFUSED")
            entries[str(path.relative_to(backup))] = {"directory": True}
        for name in files:
            path = Path(directory) / name
            entries[str(path.relative_to(backup))] = checked_hash(path)
    required = {"PG_VERSION", "backup_label", "backup_manifest", "global/pg_control"}
    if (not required <= entries.keys() or any(entries[name].get("size", 0) == 0 for name in required)
            or entries["PG_VERSION"]["size"] != 3
            or entries["backup_label"]["size"] > 32768
            or entries["backup_manifest"]["size"] > 4 * 1024 * 1024
            or (backup / "PG_VERSION").read_bytes() != b"16\n"
            or entries.get("pg_wal") != {"directory": True}
            or not any(name.startswith("pg_wal/") for name in entries)
            or entries.get("pg_tblspc") != {"directory": True}
            or any(name.startswith("pg_tblspc/") for name in entries)
            or any(name in entries for name in ("postmaster.pid", "standby.signal", "recovery.signal"))):
        raise PreflightError("BACKUP_LAYOUT_UNSUPPORTED")
    return entries


def run_native(argv, node=None):
    """Fixed native tools with full checks and bounded, preserved output."""
    with tempfile.TemporaryFile() as stdout, tempfile.TemporaryFile() as stderr:
        rc, timed_out = None, False
        try:
            credentials = {}
            if node is not None:
                if os.getuid() == 0:
                    credentials = dict(user=node["uid"], group=node["gid"], extra_groups=[])
                elif (os.getuid(), os.getgid()) != (node["uid"], node["gid"]):
                    raise PreflightError("SEED_DATABASE_USER_MISMATCH")
            rc = subprocess.run(argv, stdout=stdout, stderr=stderr, timeout=600,
                                **credentials,
                                cwd=node["pgdata"] if node is not None else None,
                                env={"PATH": "/usr/bin:/bin", "LC_ALL": "C", "LANG": "C"}).returncode
        except subprocess.TimeoutExpired:
            timed_out = True
        stdout.seek(0)
        stderr.seek(0)
        out, err = stdout.read(1048577), stderr.read(1048577)
    return {"argv": argv, "rc": rc, "timed_out": timed_out,
            "truncated": len(out) > 1048576 or len(err) > 1048576,
            "stdout": out[:1048576].decode(errors="replace"),
            "stderr": err[:1048576].decode(errors="replace")}


def require_file_checksums(backup):
    """Native verification permits --manifest-checksums=NONE; this gate does not.

    The native tool still owns manifest authentication, file coverage, checksum
    calculation and WAL parsing. This only forbids its checksum-free option.
    """
    try:
        manifest = json.loads((backup / "backup_manifest").read_bytes())
        files = manifest["Files"]
        lengths = {"CRC32C": 8, "SHA224": 56, "SHA256": 64, "SHA384": 96, "SHA512": 128}
        if type(files) is not list or not files:
            raise ValueError
        for entry in files:
            algorithm, checksum = entry.get("Checksum-Algorithm"), entry.get("Checksum")
            if (type(algorithm) is not str or algorithm not in lengths
                    or type(checksum) is not str or len(checksum) != lengths[algorithm]
                    or re.fullmatch(r"[a-fA-F0-9]+", checksum) is None):
                raise ValueError
    except (ValueError, TypeError, KeyError, AttributeError):
        raise PreflightError("BACKUP_CHECKSUMS_REQUIRED") from None


def verify_backup(backup, install, binary_sha256, manifest_sha256, system_identifier):
    for digest in (binary_sha256, manifest_sha256):
        if type(digest) is not str or not re.fullmatch(r"[a-f0-9]{64}", digest):
            raise PreflightError("BACKUP_HASH_INVALID")
    if (type(system_identifier) is not str or not re.fullmatch(r"[1-9][0-9]{0,19}", system_identifier)
            or int(system_identifier) > 18446744073709551615):
        raise PreflightError("SYSTEM_IDENTIFIER_INVALID")
    backup, install = canonical_directory(backup), canonical_directory(install)
    before = inventory(backup)
    if before["backup_manifest"]["sha256"] != manifest_sha256:
        raise PreflightError("BACKUP_MANIFEST_MISMATCH")
    require_file_checksums(backup)
    tools = {}
    for name in ("postgres", "pg_verifybackup", "pg_controldata", "pg_waldump"):
        path = install / "bin" / name
        if path.resolve(strict=True) != path or not os.access(path, os.X_OK):
            raise PreflightError("BACKUP_TOOL_INVALID")
        tools[name] = checked_hash(path)["sha256"]
    if tools["postgres"] != binary_sha256:
        raise PreflightError("BINARY_IDENTITY_MISMATCH")
    result = {"schema_version": 1, "kind": "pre1-seed-backup-content",
              "status": "ERROR", "state": "BACKUP_VERIFICATION_INCOMPLETE",
              "bootstrap_ready": False, "restart_allowed": False, "deployment_qualified": False,
              "backup": str(backup), "system_identifier": system_identifier,
              "manifest_sha256": manifest_sha256, "tree_sha256": document_sha(before),
              "tool_sha256": tools, "commands": [],
              "pending": ["SOURCE_PROVENANCE", "SEED_CLEAN_STOP", "TARGET_GUARDS", "CLONE_IDENTITY"]}
    for argv in ([str(install / "bin/pg_verifybackup"), str(backup)],
                 [str(install / "bin/pg_controldata"), "-D", str(backup)]):
        observed = run_native(argv)
        result["commands"].append(observed)
        if observed["rc"] != 0 or observed["timed_out"] or observed["truncated"] or observed["stderr"].strip():
            return result
    try:
        control = parse_control(result["commands"][-1]["stdout"])
        if control is None or control["system_identifier"] != system_identifier:
            result["state"] = "BACKUP_CONTROL_UNTRUSTED"
            return result
        if inventory(backup) != before:
            result["state"] = "BACKUP_CHANGED"
            return result
        for name, digest in tools.items():
            if checked_hash(install / "bin" / name)["sha256"] != digest:
                result["state"] = "BACKUP_TOOL_CHANGED"
                return result
    except (ObservationError, PreflightError, OSError):
        result["state"] = "BACKUP_CHANGED_OR_UNTRUSTED"
        return result
    result.update(status="PASS", state="BACKUP_CONTENT_VERIFIED", control=control)
    return result


def seed_guest_observation(request):
    return guest_status.collect({"action": "status", "node": request["node"],
                                 "binary_sha256": request["binary_sha256"]})


def require_seed_mount(request):
    observed = subprocess.run(["/usr/bin/findmnt", "--json", "--mountpoint",
                              request["shared_mount"], "-o", "TARGET,FSTYPE,UUID,OPTIONS"],
                             capture_output=True, text=True, timeout=10, check=False)
    try:
        mounts = json.loads(observed.stdout)["filesystems"]
        if observed.returncode != 0 or len(mounts) != 1:
            raise ValueError
        mount = mounts[0]
        options = set(mount["options"].split(","))
        if (mount["target"] != request["shared_mount"] or mount["fstype"] != "gfs2"
                or mount["uuid"] != request["fs_uuid"] or "rw" not in options
                or options & {"ro", "localflocks", "lock_nolock"}):
            raise ValueError
    except (ValueError, KeyError, TypeError):
        raise PreflightError("SEED_MOUNT_UNPROVEN") from None


def validate_seed_create(request, output):
    if os.path.lexists(output):
        raise PreflightError("ARTIFACT_EXISTS", "out", "ERROR")
    keys = {"schema_version", "action", "node", "binary_sha256", "dataset_id",
            "shared_mount", "shared_root", "fs_uuid", "backup", "log",
            "schema_path", "schema_sha256"}
    if (type(request) is not dict or set(request) != keys
            or type(request["schema_version"]) is not int or request["schema_version"] != 1
            or request["action"] != "create-seed"):
        raise PreflightError("SEED_REQUEST_INVALID")
    guest_status.validate_request({"action": "status", "node": request["node"],
                                   "binary_sha256": request["binary_sha256"]})
    node = request["node"]
    if (node["node_id"] != 0 or type(request["dataset_id"]) is not str
            or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}", request["dataset_id"])
            or type(request["fs_uuid"]) is not str
            or not re.fullmatch(r"[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}", request["fs_uuid"])):
        raise PreflightError("SEED_REQUEST_INVALID")
    paths = {}
    for key in ("shared_mount", "shared_root", "backup", "log", "schema_path"):
        value = request[key]
        if type(value) is not str or not re.fullmatch(r"/[A-Za-z0-9_./-]+", value):
            raise PreflightError("SEED_PATH_INVALID", key)
        safe_remote_path(value, key)
        paths[key] = Path(value)
    for key in ("pgdata", "install_root"):
        if not re.fullmatch(r"/[A-Za-z0-9_./-]+", node[key]):
            raise PreflightError("SEED_PATH_INVALID", key)
        paths[key] = canonical_directory(node[key])
    paths_disjoint([paths[key] for key in ("pgdata", "install_root", "shared_mount",
                                           "backup", "log", "schema_path")], "seed")
    if paths["shared_mount"] not in paths["shared_root"].parents:
        raise PreflightError("SEED_SHARED_ROOT_INVALID")
    output = Path(output)
    paths_disjoint([output] + [paths[key] for key in
                    ("pgdata", "install_root", "shared_mount", "backup", "log", "schema_path")], "out")
    if (not output.is_absolute() or output.resolve() != output
            or output.parent.resolve(strict=True) != output.parent):
        raise PreflightError("SEED_OUTPUT_INVALID")
    for key in ("pgdata", "shared_root"):
        path = canonical_directory(paths[key])
        metadata = path.stat()
        if (any(path.iterdir()) or (metadata.st_uid, metadata.st_gid) != (node["uid"], node["gid"])
                or metadata.st_mode & 0o027):
            raise PreflightError("SEED_TARGET_NOT_EMPTY_OR_OWNED", key)
    for key in ("backup", "log"):
        path = paths[key]
        canonical_directory(path.parent)
        if os.path.lexists(path):
            raise PreflightError("SEED_TARGET_ALREADY_EXISTS", key)
        if (path.parent.stat().st_uid, path.parent.stat().st_gid) != (node["uid"], node["gid"]):
            raise PreflightError("SEED_TARGET_PARENT_NOT_OWNED", key)
    if checked_hash(paths["schema_path"])["sha256"] != request["schema_sha256"]:
        raise PreflightError("SEED_SCHEMA_MISMATCH")
    require_seed_mount(request)
    facts = seed_guest_observation(request)
    if (facts["identity"] != {k: node[k] for k in ("vm_uuid", "boot_id", "machine_id")}
            or facts["node_id"] != 0 or facts["pgdata_state"] != "EMPTY"
            or facts["processes"] or facts["pidfile"] is not None or facts["control"] is not None):
        raise PreflightError("SEED_EMPTY_GUEST_UNPROVEN")
    binary, pgdata = paths["install_root"] / "bin", str(paths["pgdata"])
    username = pwd.getpwuid(node["uid"]).pw_name
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_-]{0,62}", username):
        raise PreflightError("SEED_USERNAME_INVALID")
    configuration = (
        "\n# PGRAC: initial seed only; no cluster or shared catalog authority.\n"
        "listen_addresses = ''\nport = 5432\n"
        f"unix_socket_directories = '{pgdata}'\n"
        "shared_buffers = '16MB'\nmax_connections = 16\n"
        "wal_level = replica\nmax_wal_senders = 4\n"
        "fsync = on\nfull_page_writes = on\nsynchronous_commit = on\n"
        "autovacuum = off\ncluster.enabled = off\ncluster.lms_enabled = off\n"
        "cluster.node_id = 0\ncluster.shared_storage_backend = cluster_fs\n"
        f"cluster.shared_data_dir = '{paths['shared_root']}'\n"
        "cluster.smgr_user_relations = on\ncluster.relation_extend_lock_enabled = off\n"
        "cluster.controlfile_shared_authority = off\ncluster.shared_catalog = off\n"
        "cluster.merged_recovery = off\ncluster.wal_threads_dir = ''\n")
    connection = ["-h", pgdata, "-p", "5432", "-U", username]
    return {"configuration": configuration,
            "initdb": [str(binary / "initdb"), "-D", pgdata, "-U", username,
                       "--auth-local=peer", "--auth-host=reject", "--no-locale", "-E", "UTF8",
                       "--pgrac-hw-snapshot-root=" + str(paths["shared_root"]),
                       "--pgrac-hw-snapshot-owner=0"],
            "start": [str(binary / "pg_ctl"), "-D", pgdata, "-l", request["log"], "-w", "-t", "60", "start"],
            "schema": [str(binary / "psql"), "-X", "-v", "ON_ERROR_STOP=1"] + connection
                      + ["-d", "postgres", "-f", request["schema_path"]],
            "backup": [str(binary / "pg_basebackup")] + connection
                      + ["-D", request["backup"], "--format=plain", "--wal-method=stream",
                         "--checkpoint=fast", "--manifest-checksums=SHA256"]}


def run_seed_native(argv, node):
    return run_native(argv, node)


def native_succeeded(command):
    return command["rc"] == 0 and not command["timed_out"] and not command["truncated"]


def seed_process(facts, request, launched_at):
    """Bind the new postmaster, not an arbitrary PID read from a stale file."""
    node, pidfile = request["node"], facts["pidfile"]
    if (facts["identity"] != {key: node[key] for key in ("vm_uuid", "boot_id", "machine_id")}
            or facts["pgdata_state"] != "INITIALIZED" or not pidfile
            or pidfile["pgdata"] != node["pgdata"] or pidfile["start_epoch"] < launched_at
            or not facts["processes"]
            or any(p["exe_sha256"] != request["binary_sha256"] for p in facts["processes"])):
        raise PreflightError("SEED_PROCESS_UNPROVEN")
    matches = [p for p in facts["processes"] if p["pid"] == pidfile["pid"]]
    if len(matches) != 1:
        raise PreflightError("SEED_PROCESS_UNPROVEN")
    return {key: matches[0][key] for key in ("pid", "starttime", "exe_sha256")}


def stop_seed_exact(request, identity):
    """Native fast-stop SIGINT through a stable Linux process handle.

    pg_ctl reopens postmaster.pid before kill(2); that would discard the verified
    starttime and can target a reused PID. Open the pidfd first, then revalidate
    that exact process and boot before sending the same native shutdown signal.
    Timeout never escalates to immediate stop or kill.
    """
    if not hasattr(os, "pidfd_open") or not hasattr(signal, "pidfd_send_signal"):
        raise PreflightError("SEED_PIDFD_UNAVAILABLE")
    fd = os.pidfd_open(identity["pid"], 0)
    try:
        node = request["node"]
        actual = guest_status.process_record(Path("/proc") / str(identity["pid"]), node["uid"])
        if (actual is None or any(actual[key] != identity[key] for key in identity)
                or guest_status.read_identity() != {key: node[key] for key in
                                                    ("vm_uuid", "boot_id", "machine_id")}):
            raise PreflightError("SEED_PROCESS_CHANGED")
        waiter = select.poll()
        waiter.register(fd, select.POLLIN)
        signal.pidfd_send_signal(fd, signal.SIGINT)
        events = waiter.poll(600000)
        stopped = any(handle == fd and flags & (select.POLLIN | select.POLLHUP)
                      for handle, flags in events)
        return {"argv": ["pidfd_send_signal", str(identity["pid"]), "SIGINT"],
                "rc": 0 if stopped else None, "timed_out": not events, "truncated": False,
                "stdout": "EXACT_POSTMASTER_EXITED" if stopped else "", "stderr": "",
                "process_identity": identity}
    finally:
        os.close(fd)


def execute_seed_create(request, plan):
    """Execute an already guarded plan. Preserve partial data on every failure.

    The local controller lock and absence of database autostart are prerequisites.
    Only our newly launched, pidfd-bound seed can receive native fast stop.
    This creates no cluster admission or old-data recovery authority.
    """
    result = {"schema_version": 1, "kind": "pre1-seed-create", "status": "ERROR",
              "state": "SEED_CREATE_INCOMPLETE", "request_sha256": document_sha(request),
              "dataset_id": request["dataset_id"], "commands": [], "observations": [],
              "seed_clean_stop": False, "bootstrap_ready": False, "restart_allowed": False,
              "deployment_qualified": False}
    launched_at, identity, system_identifier = None, None, None
    def command(stage):
        value = run_seed_native(plan[stage], request["node"])
        result["commands"].append(dict(stage=stage, **value))
        if not native_succeeded(value):
            raise PreflightError("SEED_" + stage.upper() + "_FAILED")
    try:
        command("initdb")
        config = Path(request["node"]["pgdata"]) / "postgresql.conf"
        fd = os.open(config, os.O_WRONLY | os.O_APPEND | os.O_NOFOLLOW)
        with os.fdopen(fd, "a") as stream:
            metadata = os.fstat(stream.fileno())
            if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
                raise PreflightError("SEED_CONFIG_INVALID")
            stream.write(plan["configuration"])
            stream.flush()
            os.fsync(stream.fileno())
        result["configuration_sha256"] = checked_hash(config)["sha256"]
        launched_at = int(time.time())
        command("start")
        live = seed_guest_observation(request)
        result["observations"].append(live)
        identity = seed_process(live, request, launched_at)
        control = live["control"]["parsed"] if live["control"] else None
        if not control or control["state"] != "in production":
            raise PreflightError("SEED_CONTROL_UNTRUSTED")
        system_identifier = control["system_identifier"]
        result["system_identifier"] = system_identifier
        if checked_hash(request["schema_path"])["sha256"] != request["schema_sha256"]:
            raise PreflightError("SEED_SCHEMA_CHANGED")
        command("schema")
        command("backup")
        result["state"] = "SEED_BACKUP_CREATED"
    except (PreflightError, ObservationError) as exc:
        result["state"] = exc.reason
    except (OSError, ValueError, TypeError, KeyError, subprocess.SubprocessError):
        result["state"] = "SEED_COMMAND_UNAVAILABLE"
    finally:
        if launched_at is not None:
            try:
                if identity is None:
                    raise PreflightError("SEED_PROCESS_UNPROVEN")
                before_stop = seed_guest_observation(request)
                result["observations"].append(before_stop)
                current = seed_process(before_stop, request, launched_at)
                if current != identity:
                    raise PreflightError("SEED_PROCESS_CHANGED")
                stopped = stop_seed_exact(request, identity)
                result["commands"].append(dict(stage="stop", **stopped))
                if not native_succeeded(stopped):
                    raise PreflightError("SEED_NORMAL_STOP_FAILED")
                clean = seed_guest_observation(request)
                result["observations"].append(clean)
                control = clean["control"]["parsed"] if clean["control"] else None
                if (clean["processes"] or clean["pidfile"] is not None or not control
                        or control["state"] != "shut down"
                        or (system_identifier and control["system_identifier"] != system_identifier)):
                    raise PreflightError("SEED_CLEAN_STOP_UNPROVEN")
                result["seed_clean_stop"] = True
            except (PreflightError, ObservationError) as exc:
                result["cleanup_error"] = exc.reason
            except (OSError, ValueError, TypeError, KeyError, subprocess.SubprocessError):
                result["cleanup_error"] = "SEED_CLEAN_STOP_UNAVAILABLE"
    if result["state"] != "SEED_BACKUP_CREATED" or not result["seed_clean_stop"]:
        return result
    try:
        manifest = checked_hash(Path(request["backup"]) / "backup_manifest")["sha256"]
        verified = verify_backup(request["backup"], request["node"]["install_root"],
                                 request["binary_sha256"], manifest, system_identifier)
        result["backup_verification"] = verified
        if verified["status"] == "PASS":
            result.update(status="PASS", state="SEED_BACKUP_READY")
        else:
            result["state"] = "SEED_BACKUP_VERIFICATION_FAILED"
    except (PreflightError, ObservationError, OSError, ValueError, TypeError, KeyError):
        result["state"] = "SEED_BACKUP_VERIFICATION_UNAVAILABLE"
    return result


def create_seed(request, output):
    """One controller per local PGDATA parent; no overwrite or automatic retry."""
    validate_seed_create(request, output)
    lock = Path(request["node"]["pgdata"]).parent / ".pre1-seed.lock"
    paths_disjoint([Path(output), lock], "lock")
    fd = os.open(lock, os.O_WRONLY | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
    with os.fdopen(fd, "w") as stream:
        metadata = os.fstat(stream.fileno())
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
            raise PreflightError("SEED_LOCK_INVALID")
        try:
            fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise PreflightError("SEED_CONTROLLER_BUSY") from None
        plan = validate_seed_create(request, output)
        install = Path(request["node"]["install_root"]) / "bin"
        hashes = {}
        for name in ("postgres", "initdb", "pg_ctl", "psql", "pg_basebackup",
                     "pg_verifybackup", "pg_controldata", "pg_waldump"):
            executable = install / name
            if executable.resolve(strict=True) != executable or not os.access(executable, os.X_OK):
                raise PreflightError("SEED_TOOL_INVALID")
            hashes[name] = checked_hash(executable)["sha256"]
        if hashes["postgres"] != request["binary_sha256"]:
            raise PreflightError("SEED_BINARY_MISMATCH")
        result = execute_seed_create(request, plan)
        result["tool_sha256"] = hashes
        if any(checked_hash(install / name)["sha256"] != sha for name, sha in hashes.items()):
            result.update(status="ERROR", state="SEED_TOOL_CHANGED")
        result["artifact_sha256"] = publish_artifact(output, result)
        return result


def main(argv=None):
    try:
        parser = SafeParser(description=__doc__)
        actions = parser.add_subparsers(dest="action", required=True, parser_class=SafeParser)
        verify = actions.add_parser("verify-backup")
        verify.add_argument("--backup", type=Path, required=True)
        verify.add_argument("--install-root", type=Path, required=True)
        verify.add_argument("--binary-sha256", required=True)
        verify.add_argument("--manifest-sha256", required=True)
        verify.add_argument("--system-identifier", required=True)
        verify.add_argument("--out", type=Path, required=True)
        create = actions.add_parser("create-seed")
        create.add_argument("--request", type=Path, required=True)
        create.add_argument("--out", type=Path, required=True)
        args = parser.parse_args(argv)
        if os.path.lexists(args.out):
            raise PreflightError("ARTIFACT_EXISTS", "out", "ERROR")
        if args.action == "create-seed":
            result = create_seed(load_json(args.request), args.out)
            checksum = result["artifact_sha256"]
        else:
            backup = canonical_directory(args.backup)
            output = args.out.resolve()
            if output == backup or backup in output.parents:
                raise PreflightError("BACKUP_OUTPUT_OVERLAP", "out")
            result = verify_backup(args.backup, args.install_root, args.binary_sha256,
                                   args.manifest_sha256, args.system_identifier)
            checksum = publish_artifact(args.out, result)
        answer = {"status": result["status"], "state": result["state"], "artifact_sha256": checksum}
    except PreflightError as exc:
        answer = {"status": exc.status, "reason": exc.reason, "field": exc.field}
    except ObservationError as exc:
        answer = {"status": "BLOCKED", "reason": exc.reason}
    except (OSError, ValueError, TypeError, KeyError, subprocess.SubprocessError):
        answer = {"status": "ERROR", "reason": "BACKUP_VERIFICATION_UNAVAILABLE"}
    answer.update(bootstrap_ready=False, restart_allowed=False, deployment_qualified=False)
    print(json.dumps(answer, sort_keys=True))
    return {"PASS": 0, "BLOCKED": 2, "ERROR": 3}[answer["status"]]


if __name__ == "__main__":
    sys.exit(main())
