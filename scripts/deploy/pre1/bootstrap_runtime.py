#!/usr/bin/env python3
"""Prepare/start a proven unused PRE1 seed; stop only its exact postmaster.

Author: SqlRush <sqlrush@gmail.com>

Initial start is one-shot. This adapter grants no old-data restart or crash
recovery authority. The controller dispatches stop-exact to all four guests
concurrently; each guest signals before waiting, with no kill escalation.
"""

import fcntl
import hashlib
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import time

import bootstrap_config
from common import PreflightError, document_sha, load_json, paths_disjoint, publish_artifact
import guest_status
from preflight import SafeParser
import seed
from seed_clone import open_directory

CONFIG_NAMES = {"pre1-runtime.conf", "pgrac.conf", "pre1-hba.conf"}


def write_new(path, value, node):
    """Create only, anchored beneath a no-follow directory fd, with durable bytes."""
    path = Path(path)
    parent = open_directory(path.parent)
    try:
        fd = os.open(path.name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                     0o600, dir_fd=parent)
        with os.fdopen(fd, "wb") as stream:
            stream.write(value)
            stream.flush()
            os.fchown(stream.fileno(), node["uid"], node["gid"])
            os.fsync(stream.fileno())
        os.fsync(parent)
    finally:
        os.close(parent)


def make_socket_new(logroot, node):
    """Never chown a replaceable pathname as the administrative user."""
    parent, child = open_directory(logroot), None
    try:
        os.mkdir("socket", 0o700, dir_fd=parent)
        child = os.open("socket", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent)
        os.fchown(child, node["uid"], node["gid"])
        os.fsync(child)
        os.fsync(parent)
        held = os.fstat(child)
        actual = os.stat("socket", dir_fd=parent, follow_symlinks=False)
        if (held.st_dev, held.st_ino) != (actual.st_dev, actual.st_ino):
            raise PreflightError("INITIAL_SOCKET_CHANGED")
    finally:
        if child is not None:
            os.close(child)
        os.close(parent)


def cold_tree(root):
    root = seed.canonical_directory(root)
    entries = {}
    for directory, dirs, files in os.walk(root, followlinks=False):
        if len(Path(directory).relative_to(root).parts) > 16 or len(entries) > 100000:
            raise PreflightError("INITIAL_LAYOUT_UNSUPPORTED")
        for name in dirs:
            path = Path(directory) / name
            if path.is_symlink():
                raise PreflightError("INITIAL_LINK_REFUSED")
            entries[str(path.relative_to(root))] = {"directory": True}
        for name in files:
            path = Path(directory) / name
            entries[str(path.relative_to(root))] = seed.checked_hash(path)
    if any(name in entries for name in ("postmaster.pid", "standby.signal", "recovery.signal")):
        raise PreflightError("INITIAL_USED_OR_RECOVERY_TARGET")
    return entries


def config_entries(files):
    return {name: {"sha256": hashlib.sha256(text.encode()).hexdigest(), "size": len(text.encode())}
            for name, text in files.items()}


def require_config_only(before, after, files):
    if set(files) != CONFIG_NAMES or CONFIG_NAMES & before.keys() or after != dict(before, **config_entries(files)):
        raise PreflightError("INITIAL_CONFIG_DELTA_INVALID")


def read_bound(reference):
    if type(reference) is not dict or set(reference) != {"path", "sha256"}:
        raise PreflightError("INITIAL_REFERENCE_INVALID")
    path = Path(reference["path"])
    seed.canonical_directory(path.parent)
    if seed.checked_hash(path)["sha256"] != reference["sha256"]:
        raise PreflightError("INITIAL_REFERENCE_CHANGED")
    return load_json(path)


def observation(request):
    node = {key: request["node"][key] for key in guest_status.NODE_KEYS}
    return seed.seed_guest_observation(dict(node=node, binary_sha256=request["binary_sha256"]))


def require_stopped_control(facts, system_identifier, state):
    if (facts["processes"] or facts["pidfile"] is not None
            or facts["pgdata_state"] != "INITIALIZED" or not facts["control"]
            or facts["control"]["parsed"] != dict(system_identifier=system_identifier, state=state)):
        raise PreflightError("INITIAL_STOPPED_CONTROL_UNPROVEN")


def require_votes(config, node):
    """Identity/permissions only: fresh bytes are a separate direct-read gate."""
    devices = []
    for wwid in config["voting_wwids"]:
        path = Path("/dev/disk/by-id/scsi-" + wwid).resolve(strict=True)
        info = path.stat()
        if (not stat.S_ISBLK(info.st_mode) or info.st_uid != 0 or info.st_gid != node["gid"]
                or stat.S_IMODE(info.st_mode) != 0o660):
            raise PreflightError("INITIAL_VOTE_ACCESS_UNPROVEN")
        result = subprocess.run(["/usr/bin/udevadm", "info", "--query=property", "--name", str(path)],
                                capture_output=True, text=True, timeout=10)
        values = dict(line.split("=", 1) for line in result.stdout.splitlines() if "=" in line)
        if result.returncode or values.get("ID_SERIAL") != wwid or values.get("DEVTYPE") != "disk":
            raise PreflightError("INITIAL_VOTE_IDENTITY_UNPROVEN")
        devices.append(info.st_rdev)
    if len(set(devices)) != 3:
        raise PreflightError("INITIAL_VOTE_ALIAS")


def context(request):
    keys = {"schema_version", "action", "node_id", "binary_sha256", "config", "seed_request",
            "seed_artifact", "clone_request", "clone_artifact"}
    if (type(request) is not dict or set(request) != keys
            or type(request["schema_version"]) is not int or request["schema_version"] != 1
            or request["action"] != "configure-initial" or type(request["node_id"]) is not int
            or request["node_id"] not in range(4)):
        raise PreflightError("INITIAL_REQUEST_INVALID")
    config = read_bound(request["config"])
    files = bootstrap_config.render(config)[request["node_id"]]
    node = next(n for n in config["nodes"] if n["node_id"] == request["node_id"])
    source, artifact = read_bound(request["seed_request"]), read_bound(request["seed_artifact"])
    if (source["node"]["node_id"] != 0 or source["binary_sha256"] != request["binary_sha256"]
            or config["shared_root"] != source["shared_root"]
            or artifact["kind"] != "pre1-seed-create" or artifact["status"] != "PASS"
            or artifact["state"] != "SEED_BACKUP_READY" or artifact["seed_clean_stop"] is not True
            or artifact["request_sha256"] != document_sha(source)
            or artifact["backup_verification"]["status"] != "PASS"):
        raise PreflightError("INITIAL_SEED_UNPROVEN")
    if node["node_id"] == 0:
        if (request["clone_request"] is not None or request["clone_artifact"] is not None
                or source["node"] != {key: node[key] for key in source["node"]}):
            raise PreflightError("INITIAL_SEED_IDENTITY_CHANGED")
        proof = artifact
    else:
        clone_request, proof = read_bound(request["clone_request"]), read_bound(request["clone_artifact"])
        if (clone_request["node"] != {key: node[key] for key in clone_request["node"]}
                or clone_request["binary_sha256"] != request["binary_sha256"]
                or clone_request["seed_artifact_sha256"] != request["seed_artifact"]["sha256"]
                or clone_request["seed_request_sha256"] != request["seed_request"]["sha256"]
                or proof["request_sha256"] != document_sha(clone_request)
                or proof["kind"] != "pre1-seed-clone" or proof["status"] != "PASS"
                or proof["state"] != "CLONED_NOT_CONFIGURED"
                or proof["system_identifier"] != artifact["system_identifier"]
                or proof["backup_verification"]["tree_sha256"] != artifact["backup_verification"]["tree_sha256"]):
            raise PreflightError("INITIAL_CLONE_UNPROVEN")
    seed.require_seed_mount(source)
    require_votes(config, node)
    actual = observation(dict(node=node, binary_sha256=request["binary_sha256"]))
    expected_state = "shut down" if node["node_id"] == 0 else "in production"
    require_stopped_control(actual, artifact["system_identifier"], expected_state)
    return dict(node=node, source=source, artifact=artifact, proof=proof, files=files, observation=actual)


def configure(request, output):
    ctx = context(request)
    node, files = ctx["node"], ctx["files"]
    root = Path(node["pgdata"])
    socket = Path(node["log_root"]) / "socket"
    paths_disjoint([Path(output), root, Path(ctx["source"]["shared_mount"]), socket], "initial-output")
    if os.path.lexists(output):
        raise PreflightError("ARTIFACT_EXISTS")
    before = cold_tree(root)
    if CONFIG_NAMES & before.keys():
        raise PreflightError("INITIAL_ALREADY_CONFIGURED")
    if node["node_id"] == 0:
        if (before["postgresql.conf"]["sha256"] != ctx["artifact"]["configuration_sha256"]
                or ctx["observation"]["control"] != ctx["artifact"]["observations"][-1]["control"]):
            raise PreflightError("INITIAL_NATIVE_SEED_CHANGED")
    elif document_sha(before) != ctx["proof"]["backup_verification"]["tree_sha256"]:
        raise PreflightError("INITIAL_CLONE_CHANGED")
    seed.canonical_directory(socket.parent)
    make_socket_new(socket.parent, node)
    for name, value in files.items():
        write_new(root / name, value.encode(), node)
    after = cold_tree(root)
    require_config_only(before, after, files)
    result = dict(schema_version=1, kind="pre1-initial-config", status="PASS",
                  state="CONFIGURED_NOT_STARTED", request=request, node=node,
                  request_sha256=document_sha(request), system_identifier=ctx["artifact"]["system_identifier"],
                  before_tree_sha256=document_sha(before), after_tree_sha256=document_sha(after),
                  files=config_entries(files), bootstrap_ready=False, restart_allowed=False,
                  deployment_qualified=False)
    result["artifact_sha256"] = publish_artifact(output, result)
    return result


def start_initial(prepared, output):
    artifact = read_bound(prepared)
    if (artifact["kind"] != "pre1-initial-config" or artifact["status"] != "PASS"
            or artifact["state"] != "CONFIGURED_NOT_STARTED"
            or artifact["request_sha256"] != document_sha(artifact["request"])):
        raise PreflightError("INITIAL_CONFIGURATION_UNPROVEN")
    ctx = context(artifact["request"])
    node = ctx["node"]
    root, logroot = Path(node["pgdata"]), Path(node["log_root"])
    log = logroot / "initial-start.log"
    attempt = logroot / "initial-start-attempt.json"
    paths_disjoint([Path(output), root, Path(ctx["source"]["shared_mount"]),
                    log, attempt, logroot / "socket"], "initial-output")
    if (node != artifact["node"] or os.path.lexists(output)
            or document_sha(cold_tree(root)) != artifact["after_tree_sha256"]
            or artifact["files"] != config_entries(ctx["files"])):
        raise PreflightError("INITIAL_CONFIGURATION_CHANGED")
    # Retain the marker even if pg_ctl fails. An in-production clone is never
    # silently reinterpreted as a fresh backup on a second attempt.
    write_new(attempt, (json.dumps(prepared, sort_keys=True) + "\n").encode(), dict(uid=0, gid=0))
    write_new(log, b"", node)
    result = dict(schema_version=1, kind="pre1-initial-start", status="ERROR", state="START_INCOMPLETE",
                  prepared=prepared, request=artifact["request"], node=node, log=str(log),
                  process_identity=None, bootstrap_ready=False, restart_allowed=False,
                  deployment_qualified=False)
    launched_at = int(time.time())
    try:
        argv = [str(Path(node["install_root"]) / "bin/pg_ctl"), "-D", str(root), "-l", str(log),
                "-o", "-c config_file=" + str(root / "pre1-runtime.conf"), "-w", "-t", "60", "start"]
        result["command"] = seed.run_native(argv, node)
        facts = observation(dict(node=node, binary_sha256=artifact["request"]["binary_sha256"]))
        result["observation"] = facts
        result["process_identity"] = seed.seed_process(facts, dict(node=node,
                                    binary_sha256=artifact["request"]["binary_sha256"]), launched_at)
        if seed.native_succeeded(result["command"]):
            result.update(status="PASS", state="PROCESS_STARTED_NOT_ADMITTED")
    except (PreflightError, guest_status.ObservationError) as exc:
        result["reason"] = exc.reason
    except (OSError, ValueError, TypeError, KeyError, subprocess.SubprocessError):
        result["reason"] = "INITIAL_START_UNAVAILABLE"
    result["artifact_sha256"] = publish_artifact(output, result)
    return result


def stop_exact(started, output):
    artifact = read_bound(started)
    if (artifact["kind"] not in ("pre1-initial-start", "pre1-clean-start")
            or not artifact["process_identity"] or os.path.lexists(output)):
        raise PreflightError("INITIAL_PROCESS_UNPROVEN")
    node = artifact["node"]
    request = dict(node={key: node[key] for key in guest_status.NODE_KEYS},
                   binary_sha256=artifact["request"]["binary_sha256"])
    facts = observation(request)
    if not facts["pidfile"] or facts["pidfile"]["pid"] != artifact["process_identity"]["pid"]:
        raise PreflightError("INITIAL_POSTMASTER_CHANGED")
    # Offset precedes SIGINT. A native clean control alone is NOT closure proof.
    offset = os.stat(artifact["log"]).st_size
    result = dict(schema_version=1, kind="pre1-initial-stop", status="ERROR", state="STOP_INCOMPLETE",
                  started=started, node=node, log_offset=offset, restart_allowed=False,
                  deployment_qualified=False)
    result["command"] = seed.stop_seed_exact(request, artifact["process_identity"])
    result["observation"] = observation(request)
    if seed.native_succeeded(result["command"]):
        result.update(status="PASS", state="EXACT_PROCESS_EXITED_CLOSURE_NOT_YET_PROVEN")
    result["artifact_sha256"] = publish_artifact(output, result)
    return result


def main(argv=None):
    try:
        parser = SafeParser(description=__doc__)
        parser.add_argument("action", choices=("configure-initial", "start-initial", "stop-exact"))
        parser.add_argument("--request", required=True, type=Path)
        parser.add_argument("--sha256")
        parser.add_argument("--out", required=True, type=Path)
        args = parser.parse_args(argv)
        # One controller operation per guest; the retained lock is not admission.
        ref = dict(path=str(args.request), sha256=args.sha256)
        if args.action == "configure-initial":
            request = load_json(args.request)
            node = context(request)["node"]
        else:
            request = read_bound(ref)
            config = read_bound(request["request"]["config"])
            bootstrap_config.render(config)
            node = next(n for n in config["nodes"] if n["node_id"] == request["request"]["node_id"])
            if request["node"] != node:
                raise PreflightError("INITIAL_NODE_CHANGED")
            seed.canonical_directory(node["pgdata"])
        lock = Path(node["pgdata"]).parent / ".pre1-runtime.lock"
        fd = os.open(lock, os.O_WRONLY | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
        with os.fdopen(fd, "w") as stream:
            info = os.fstat(stream.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                raise PreflightError("INITIAL_CONTROLLER_LOCK_INVALID")
            fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
            if args.action == "configure-initial":
                result = configure(request, args.out)
            elif args.action == "start-initial":
                result = start_initial(ref, args.out)
            else:
                result = stop_exact(ref, args.out)
        answer = {key: result[key] for key in ("status", "state", "artifact_sha256")}
    except (PreflightError, guest_status.ObservationError) as exc:
        answer = dict(status="BLOCKED", reason=exc.reason)
    except (OSError, ValueError, TypeError, KeyError, subprocess.SubprocessError):
        answer = dict(status="ERROR", reason="INITIAL_OPERATION_UNAVAILABLE")
    answer.update(restart_allowed=False, deployment_qualified=False)
    print(json.dumps(answer, sort_keys=True))
    return {"PASS": 0, "BLOCKED": 2, "ERROR": 3}[answer["status"]]


if __name__ == "__main__":
    sys.exit(main())
