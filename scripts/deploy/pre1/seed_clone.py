#!/usr/bin/env python3
"""Copy one verified native seed backup into an unused joiner's local PGDATA.

Author: SqlRush <sqlrush@gmail.com>

No database is started, no native control/WAL field is rewritten, and no shared
user files are distributed. This is not bootstrap or crash-recovery authority.
"""

import fcntl
import json
import os
from pathlib import Path
import shutil
import stat
import sys

from common import PreflightError, document_sha, load_json, paths_disjoint, publish_artifact
from guest_status import ObservationError
from preflight import SafeParser
import seed


def validate_clone(request, output):
    if os.path.lexists(output):
        raise PreflightError("ARTIFACT_EXISTS", "out", "ERROR")
    keys = {"schema_version", "action", "node", "binary_sha256", "backup",
            "seed_request_path", "seed_request_sha256", "seed_artifact_path", "seed_artifact_sha256"}
    if (type(request) is not dict or set(request) != keys
            or type(request["schema_version"]) is not int or request["schema_version"] != 1
            or request["action"] != "clone-seed"):
        raise PreflightError("CLONE_REQUEST_INVALID")
    seed.guest_status.validate_request({"action": "status", "node": request["node"],
                                       "binary_sha256": request["binary_sha256"]})
    node = request["node"]
    if node["node_id"] not in (1, 2, 3):
        raise PreflightError("CLONE_JOINER_REQUIRED")
    target = seed.canonical_directory(node["pgdata"])
    backup = seed.canonical_directory(request["backup"])
    install = seed.canonical_directory(node["install_root"])
    evidence = {}
    for name in ("seed_request", "seed_artifact"):
        path = Path(request[name + "_path"])
        seed.canonical_directory(path.parent)
        if seed.checked_hash(path)["sha256"] != request[name + "_sha256"]:
            raise PreflightError("CLONE_SOURCE_HASH_MISMATCH")
        evidence[name] = load_json(path)
    source, artifact = evidence["seed_request"], evidence["seed_artifact"]
    verified = artifact["backup_verification"]
    if (source["node"]["node_id"] != 0 or source["binary_sha256"] != request["binary_sha256"]
            or any(source["node"][key] == node[key] for key in ("vm_uuid", "boot_id", "machine_id"))
            or artifact["kind"] != "pre1-seed-create" or artifact["status"] != "PASS"
            or artifact["state"] != "SEED_BACKUP_READY" or artifact["seed_clean_stop"] is not True
            or artifact["request_sha256"] != document_sha(source)
            or artifact["dataset_id"] != source["dataset_id"] or verified["status"] != "PASS"):
        raise PreflightError("CLONE_SOURCE_NOT_READY")
    output = Path(output)
    paths_disjoint([target, backup, install, Path(source["shared_mount"]), output,
                    Path(request["seed_request_path"]), Path(request["seed_artifact_path"])], "clone")
    if (not output.is_absolute() or output.resolve() != output
            or output.parent.resolve(strict=True) != output.parent):
        raise PreflightError("CLONE_OUTPUT_INVALID")
    metadata = target.stat()
    if (any(target.iterdir()) or (metadata.st_uid, metadata.st_gid) != (node["uid"], node["gid"])
            or metadata.st_mode & 0o027):
        raise PreflightError("CLONE_TARGET_NOT_EMPTY_OR_OWNED")
    seed.require_seed_mount(source)
    facts = seed.seed_guest_observation(request)
    if (facts["identity"] != {key: node[key] for key in ("vm_uuid", "boot_id", "machine_id")}
            or facts["node_id"] != node["node_id"] or facts["pgdata_state"] != "EMPTY"
            or facts["control"] is not None or facts["pidfile"] is not None or facts["processes"]):
        raise PreflightError("CLONE_TARGET_NOT_EMPTY_OR_STOPPED")
    local = seed.verify_backup(backup, install, request["binary_sha256"],
                               verified["manifest_sha256"], artifact["system_identifier"])
    if local["status"] != "PASS" or local["tree_sha256"] != verified["tree_sha256"]:
        raise PreflightError("CLONE_LOCAL_BACKUP_INVALID")
    return {"dataset_id": source["dataset_id"], "system_identifier": artifact["system_identifier"],
            "backup_verification": local, "source": source}


def copy_file_new(source, destination, node, source_dir_fd=None, destination_dir_fd=None):
    """Exclusive destinations and regular source fds; never follow a file link."""
    fd = os.open(source, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=source_dir_fd)
    with os.fdopen(fd, "rb") as incoming:
        metadata = os.fstat(incoming.fileno())
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
            raise PreflightError("CLONE_SOURCE_FILE_INVALID")
        fd = os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                     0o600, dir_fd=destination_dir_fd)
        with os.fdopen(fd, "wb") as outgoing:
            shutil.copyfileobj(incoming, outgoing, 1024 * 1024)
            outgoing.flush()
            os.fchown(outgoing.fileno(), node["uid"], node["gid"])
            os.fchmod(outgoing.fileno(), metadata.st_mode & 0o777)
            os.fsync(outgoing.fileno())
    return str(destination)


def open_directory(path):
    """Walk absolute components without following an intermediate directory link."""
    path = Path(path)
    if not path.is_absolute() or ".." in path.parts:
        raise PreflightError("CLONE_DIRECTORY_INVALID")
    fd = os.open("/", os.O_RDONLY | os.O_DIRECTORY)
    try:
        for part in path.parts[1:]:
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
            os.close(fd)
            fd = child
        return fd
    except BaseException:
        os.close(fd)
        raise


def copy_tree_new(source, destination, node):
    """Never merge a target child; every lookup/write stays under held dir fds.

    copytree(dirs_exist_ok=True) would follow a target directory replaced after
    the empty check. Here child directories are exclusively created, opened
    O_NOFOLLOW and held through recursion and fsync. A concurrent insertion is
    an error, not a directory to reuse. Only our pre-existing empty root is used.
    """
    source_fd = open_directory(source)
    destination_fd = None
    try:
        destination_fd = open_directory(destination)
        metadata = os.fstat(destination_fd)
        if (os.listdir(destination_fd)
                or (metadata.st_uid, metadata.st_gid) != (node["uid"], node["gid"])):
            raise PreflightError("CLONE_TARGET_NOT_EMPTY_OR_OWNED")
        count = [0]
        def descend(in_fd, out_fd, depth):
            if depth > 16:
                raise PreflightError("CLONE_LAYOUT_UNSUPPORTED")
            for name in sorted(os.listdir(in_fd)):
                count[0] += 1
                if count[0] > 100000:
                    raise PreflightError("CLONE_LAYOUT_UNSUPPORTED")
                info = os.stat(name, dir_fd=in_fd, follow_symlinks=False)
                if stat.S_ISDIR(info.st_mode):
                    incoming = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=in_fd)
                    outgoing = None
                    try:
                        os.mkdir(name, 0o700, dir_fd=out_fd)
                        outgoing = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=out_fd)
                        descend(incoming, outgoing, depth + 1)
                    finally:
                        os.close(incoming)
                        if outgoing is not None:
                            os.close(outgoing)
                elif stat.S_ISREG(info.st_mode):
                    copy_file_new(name, name, node, in_fd, out_fd)
                else:
                    raise PreflightError("CLONE_SOURCE_FILE_INVALID")
            os.fchown(out_fd, node["uid"], node["gid"])
            os.fchmod(out_fd, os.fstat(in_fd).st_mode & 0o777)
            os.fsync(out_fd)
        descend(source_fd, destination_fd, 0)
        actual = os.stat(destination, follow_symlinks=False)
        held = os.fstat(destination_fd)
        if (actual.st_dev, actual.st_ino) != (held.st_dev, held.st_ino):
            raise PreflightError("CLONE_TARGET_CHANGED")
    finally:
        os.close(source_fd)
        if destination_fd is not None:
            os.close(destination_fd)


def clone_seed(request, output):
    context = validate_clone(request, output)
    target, backup = Path(request["node"]["pgdata"]), Path(request["backup"])
    lock = target.parent / ".pre1-seed.lock"
    paths_disjoint([Path(output), lock], "lock")
    fd = os.open(lock, os.O_WRONLY | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
    with os.fdopen(fd, "w") as stream:
        metadata = os.fstat(stream.fileno())
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
            raise PreflightError("CLONE_LOCK_INVALID")
        try:
            fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise PreflightError("CLONE_CONTROLLER_BUSY") from None
        context = validate_clone(request, output)
        before = seed.inventory(backup)
        if document_sha(before) != context["backup_verification"]["tree_sha256"]:
            raise PreflightError("CLONE_SOURCE_CHANGED")
        result = {"schema_version": 1, "kind": "pre1-seed-clone", "status": "ERROR",
                  "state": "PARTIAL_CLONE", "request_sha256": document_sha(request),
                  "node": request["node"], "dataset_id": context["dataset_id"],
                  "system_identifier": context["system_identifier"],
                  "seed_artifact_sha256": request["seed_artifact_sha256"],
                  "bootstrap_ready": False, "restart_allowed": False, "deployment_qualified": False}
        try:
            copy_tree_new(backup, target, request["node"])
            verified = seed.verify_backup(target, request["node"]["install_root"],
                                           request["binary_sha256"],
                                           context["backup_verification"]["manifest_sha256"],
                                           context["system_identifier"])
            result["backup_verification"] = verified
            facts = seed.seed_guest_observation(request)
            result["observation"] = facts
            if (verified["status"] == "PASS" and verified["tree_sha256"] == document_sha(before)
                    and seed.inventory(backup) == before and not facts["processes"]
                    and facts["pidfile"] is None and facts["control"]["parsed"] is not None
                    and facts["control"]["parsed"]["system_identifier"] == context["system_identifier"]):
                result.update(status="PASS", state="CLONED_NOT_CONFIGURED")
        except (PreflightError, ObservationError) as exc:
            result["reason"] = exc.reason
        except (OSError, ValueError, TypeError, KeyError):
            result["reason"] = "CLONE_COPY_OR_VERIFICATION_FAILED"
        result["artifact_sha256"] = publish_artifact(output, result)
        return result


def main(argv=None):
    try:
        parser = SafeParser(description=__doc__)
        parser.add_argument("--request", type=Path, required=True)
        parser.add_argument("--out", type=Path, required=True)
        args = parser.parse_args(argv)
        result = clone_seed(load_json(args.request), args.out)
        answer = {key: result[key] for key in ("status", "state", "artifact_sha256")}
    except (PreflightError, ObservationError) as exc:
        answer = {"status": getattr(exc, "status", "BLOCKED"), "reason": exc.reason}
    except (OSError, ValueError, TypeError, KeyError):
        answer = {"status": "ERROR", "reason": "CLONE_UNAVAILABLE"}
    answer.update(bootstrap_ready=False, restart_allowed=False, deployment_qualified=False)
    print(json.dumps(answer, sort_keys=True))
    return {"PASS": 0, "BLOCKED": 2, "ERROR": 3}[answer["status"]]


if __name__ == "__main__":
    sys.exit(main())
