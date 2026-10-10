#!/usr/bin/env python3
"""Install these tools on the four guests under a new, versioned root.

Author: SqlRush <sqlrush@gmail.com>

    install_guest_tools.py --request REQ --dest /opt/pgrac-pre2-lab-tools-<id> --out RESULT [--dry-run]

Packs scripts/deploy/pre1 and scripts/deploy/pre2/lab (without tests or
bytecode) with a SHA256SUMS manifest, sends it over pinned SSH and unpacks it
as root into a staging directory next to DEST. The guest verifies every file
against the manifest before renaming the staging directory to DEST. An
existing DEST is never replaced; a failed node keeps its staging directory
for inspection and nothing else on the guest changes.
"""

import argparse
import hashlib
import io
import json
from pathlib import Path, PurePosixPath
import re
import shlex
import subprocess
import sys
import tarfile
import tempfile

from lab_common import ERROR, PASS, PreflightError, load_json, publish_artifact, safe_remote_path
import request as lab_request

DEPLOY = Path(__file__).resolve().parents[2]
PARTS = (("pre1", DEPLOY / "pre1"), ("pre2/lab", DEPLOY / "pre2" / "lab"))
SOURCE_FILES = {
    "pre1": ("bootstrap_config.py", "bootstrap_runtime.py", "clean_closure.py", "clean_restart.py",
             "common.py", "fencing.py", "guest_status.py", "lifecycle.py", "preflight.py",
             "profile.schema.json", "remote.py", "seed.py", "seed_clone.py", "snapshot.py",
             "storage_probe.c", "verify.py", "voting.py", "voting_io.c"),
    "pre2/lab": ("README.md", "cohort.py", "cohort_controller.py", "cohort_guest.py",
                 "fresh_controller.py", "fresh_guest.py", "fresh_plan.py", "install_guest_tools.py",
                 "lab_common.py", "render.py", "request.py", "source_contract.py"),
}


def collect_files():
    files = []
    for prefix, root in PARTS:
        # Never ship operator requests, keys or logs placed beside these scripts.
        for name in SOURCE_FILES[prefix]:
            path = root / name
            if path.is_symlink() or not path.is_file():
                raise PreflightError("TOOL_SOURCE_UNAVAILABLE", prefix + "/" + name)
            files.append((prefix + "/" + name, path))
    return files


def build_archive():
    """Deterministic tar (fixed owner/time) with SHA256SUMS last; returns (bytes, manifest)."""
    files = collect_files()
    manifest = {name: hashlib.sha256(path.read_bytes()).hexdigest() for name, path in files}
    sums = "".join("%s  %s\n" % (manifest[name], name) for name, _ in files).encode()
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w", format=tarfile.PAX_FORMAT) as tar:
        for name, path in files + [("SHA256SUMS", None)]:
            data = sums if path is None else path.read_bytes()
            info = tarfile.TarInfo(name)
            info.size, info.mtime, info.uid, info.gid = len(data), 0, 0, 0
            info.mode = 0o755 if path is not None and path.stat().st_mode & 0o111 else 0o644
            tar.addfile(info, io.BytesIO(data))
    return buffer.getvalue(), manifest


def validate_dest(dest):
    if not re.fullmatch(r"/opt/pgrac-pre2-lab-tools-[A-Za-z0-9._-]{1,64}", dest):
        raise PreflightError("INSTALL_DEST_INVALID", "dest")
    safe_remote_path(dest, "dest")
    return dest


def remote_command(dest):
    parent, name = str(PurePosixPath(dest).parent), PurePosixPath(dest).name
    staging = "%s/.%s.staging" % (parent, name)
    script = ("set -eu; umask 022; d=%s; s=%s; [ ! -e \"$d\" ] || { echo DEST_EXISTS; exit 17; }; "
              "[ ! -e \"$s\" ] || { echo STAGING_EXISTS; exit 18; }; mkdir -m 0755 \"$s\"; "
              "tar -x --no-same-owner -C \"$s\" -f -; cd \"$s\"; sha256sum --check --quiet --strict SHA256SUMS; "
              "chown -R root:root \"$s\"; mv -T \"$s\" \"$d\"; echo INSTALLED \"$d\"; "
              "sha256sum \"$d/SHA256SUMS\"") % (shlex.quote(dest), shlex.quote(staging))
    return "sudo -n sh -c " + shlex.quote(script)


def ssh_argv(node, known_hosts):
    admin = node["admin_endpoint"]
    return ["ssh", "-F", "/dev/null", "-T", "-p", str(admin["port"]), "-o", "BatchMode=yes",
            "-o", "StrictHostKeyChecking=yes", "-o", "UserKnownHostsFile=" + known_hosts,
            "-o", "GlobalKnownHostsFile=/dev/null", "-o", "ConnectTimeout=10", "-o", "ForwardAgent=no",
            "-o", "IdentitiesOnly=yes", "-o", "IdentityAgent=none", "-i", admin["identity_file"],
            "-l", admin["user"], admin["host"]]


def install(request, dest, dry_run):
    archive, manifest = build_archive()
    sums_sha = hashlib.sha256("".join("%s  %s\n" % (manifest[n], n) for n in manifest).encode()).hexdigest()
    result = {"schema_version": 1, "kind": "pre2-lab-tool-install", "dest": dest, "files": len(manifest),
              "archive_sha256": hashlib.sha256(archive).hexdigest(), "manifest_sha256": sums_sha,
              "status": ERROR, "nodes": []}
    for node in request["nodes"]:
        with tempfile.NamedTemporaryFile(mode="w", prefix="pre2-known-hosts-") as known:
            admin = node["admin_endpoint"]
            name = admin["host"] if admin["port"] == 22 else "[%s]:%d" % (admin["host"], admin["port"])
            known.write("%s %s\n" % (name, node["ssh_host_key"]))
            known.flush()
            argv = ssh_argv(node, known.name) + [remote_command(dest)]
            if dry_run:
                result["nodes"].append({"node_id": node["node_id"], "argv": argv})
                continue
            done = subprocess.run(argv, input=archive, capture_output=True, timeout=300, check=False)
            stdout = done.stdout.decode(errors="replace")
            ok = (done.returncode == 0 and ("INSTALLED " + dest) in stdout and sums_sha in stdout)
            result["nodes"].append({"node_id": node["node_id"], "rc": done.returncode, "ok": ok,
                                    "stdout": stdout[-2000:], "stderr": done.stderr.decode(errors="replace")[-2000:]})
            if not ok:
                result["state"] = "INSTALL_FAILED_NODE_%d" % node["node_id"]
                return result
    result.update(status=PASS, state="DRY_RUN" if dry_run else "INSTALLED")
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--request", required=True)
    parser.add_argument("--dest", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    try:
        request, _ = lab_request.validate(load_json(args.request))
        result = install(request, validate_dest(args.dest), args.dry_run)
        publish_artifact(args.out, result)
    except PreflightError as exc:
        print(json.dumps({"status": exc.status, "reason": exc.reason, "field": exc.field}))
        return 2
    print(json.dumps({"status": result["status"], "state": result["state"], "files": result["files"]}))
    return 0 if result["status"] == PASS else 1


if __name__ == "__main__":
    sys.exit(main())
