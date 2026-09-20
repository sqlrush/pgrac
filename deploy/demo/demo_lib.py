"""Pure safety and evidence policies for the single-host Podman demo.

Author: SqlRush <sqlrush@gmail.com>
"""

import hashlib
import math
from pathlib import Path, PurePosixPath
import re
import socket

SOURCE_REVISION = "83d8c5a002643581f153058b7a766afe1ffa1eb0"
DEMO_VERSION = "v0.131.0-demo.1"
DEFAULT_IMAGE = "ghcr.io/sqlrush/pgrac-demo:" + DEMO_VERSION
DATABASE_UID = 10001


def security_status(text, uid):
    fields = dict(re.findall(r"^(NoNewPrivs|CapEff|CapBnd|Seccomp):\s+(\S+)", text, re.M))
    if (uid != DATABASE_UID or fields.get("NoNewPrivs") != "1"
            or fields.get("CapEff") != "0000000000000000"
            or fields.get("CapBnd") != "0000000000000000" or fields.get("Seccomp") != "2"):
        raise ValueError("require UID 10001, no-new-privileges, zero capabilities and seccomp")
    return fields


def sql_startup_ready(logs):
    return len(logs) == 4 and all(re.search(
        r"LOG:\s+database system is ready to accept connections\s*$", text, re.M) for text in logs)


def check_port_free(port):
    # Match PostgreSQL's SO_REUSEADDR: old TIME_WAIT is not a live listener.
    with socket.socket() as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("127.0.0.1", port))
        sock.listen()


def removable_pod(info, name, node):
    return (info.get("Name") == "pgrac-%s-%d" % (name, node)
            and info.get("Labels", {}).get("pgrac-demo") == name
            and info.get("State") in ("Exited", "Stopped", "Created"))


def validate_name(name):
    if not re.fullmatch(r"[a-z][a-z0-9-]{0,31}", name):
        raise ValueError("name must be 1..32 lowercase letters/digits/hyphens, starting with a letter")
    return name


def safe_root(base, name):
    validate_name(name)
    path = Path(base) / name
    if not path.is_absolute() or ".." in path.parts:
        raise ValueError("storage root must be an absolute, non-traversing path")
    for part in [path, *path.parents]:
        if part.is_symlink():
            raise ValueError("symlink in storage root: " + str(part))
    return path


def storage_relative(path):
    if not isinstance(path, str) or not path.startswith("/demo/"):
        raise ValueError("path is outside the deployment storage")
    parsed = PurePosixPath(path)
    if str(parsed) != path or ".." in parsed.parts or len(parsed.parts) < 3:
        raise ValueError("non-canonical storage path")
    return Path(*parsed.parts[2:])


def start_action(state, running, clean, image_matches):
    if not image_matches:
        raise ValueError("image differs from initialized deployment; in-place upgrade is not supported")
    if state == "READY" and sorted(running) == [0, 1, 2, 3]:
        return "verify"
    if state == "INITIALIZED" and not running:
        return "first"
    if state == "CLEAN" and not running and clean == [True] * 4:
        return "restart"
    raise ValueError("deployment is not a complete running or proven clean cluster; preserve evidence")


def exact_loop(device, backing, major_minor, size, actual_device, actual_backing,
               actual_major_minor, actual_size):
    return ((device, backing, major_minor, size) ==
            (actual_device, actual_backing, actual_major_minor, actual_size))


def parse_pgbench(text, rc):
    values = {}
    for key, pattern in (
            ("transactions", r"^number of transactions actually processed: (\d+)(?:/\d+)?\s*$"),
            ("failed", r"^number of failed transactions: (\d+) \([^\n]+\)\s*$"),
            ("tps", r"^tps = ([0-9.]+) \([^\n]+\)\s*$")):
        matches = re.findall(pattern, text, flags=re.MULTILINE)
        if len(matches) != 1:
            raise ValueError("missing or ambiguous pgbench field: " + key)
        values[key] = float(matches[0]) if key == "tps" else int(matches[0])
    if not math.isfinite(values["tps"]):
        raise ValueError("non-finite throughput")
    values["rc"] = rc
    return values


def benchmark_verdict(nodes, before, after, server_errors, healthy):
    reasons = []
    if len(nodes) != 4 or any(n["rc"] != 0 or n["failed"] != 0 or n["transactions"] <= 0
                              for n in nodes):
        reasons.append("CLIENT_ERROR_OR_MISSING_NODE")
    if len(server_errors) != 4 or any(server_errors):
        reasons.append("SERVER_ERROR_OR_MISSING_LOG")
    if healthy != [True] * 4:
        reasons.append("HEALTH_NOT_PROVEN")
    total = sum(n["transactions"] for n in nodes)
    for label, snapshots in (("BEFORE", before), ("AFTER", after)):
        if len(snapshots) != 4 or any(s != snapshots[0] for s in snapshots):
            reasons.append(label + "_ROW_MISMATCH")
    if len(before) == 4 and len(after) == 4:
        if (before[0]["rows"] <= 0 or before[0]["rows"] != after[0]["rows"]
                or before[0]["key_payload_sha256"] != after[0]["key_payload_sha256"]):
            reasons.append("KEY_PAYLOAD_OR_COUNT_CHANGED")
        if after[0]["sum"] - before[0]["sum"] != total:
            reasons.append("COMMITTED_SUM_MISMATCH")
    return {"verdict": "FAIL" if reasons else "PASS", "reasons": reasons,
            "transactions": total, "reported_tps_sum": sum(n["tps"] for n in nodes),
            "nodes": nodes, "is_formal_pre": False}


def pod_documents(name, image, storage, loops):
    validate_name(name)
    if len(loops) != 3 or any(not re.fullmatch(r"/dev/loop\d+", d) for d in loops):
        raise ValueError("exactly three Linux loop devices are required")
    documents = []
    for i in range(4):
        volumes = [{"name": "storage", "hostPath": {"path": storage, "type": "Directory"}},
                   {"name": "shm", "emptyDir": {"medium": "Memory", "sizeLimit": "256Mi"}}]
        mounts = [{"name": "storage", "mountPath": "/demo"},
                  {"name": "shm", "mountPath": "/dev/shm"}]
        for j, device in enumerate(loops):
            volumes.append({"name": "vote" + str(j), "hostPath": {
                "path": device, "type": "BlockDevice"}})
            mounts.append({"name": "vote" + str(j), "mountPath": "/dev/pgrac-vote" + str(j)})
        documents.append({"apiVersion": "v1", "kind": "Pod", "metadata": {
            "name": "pgrac-" + name + "-" + str(i),
            "labels": {"app": "pgrac-demo", "pgrac-demo": name}}, "spec": {
                "hostNetwork": True, "restartPolicy": "Never", "terminationGracePeriodSeconds": 600,
                "containers": [{"name": "db", "image": image, "imagePullPolicy": "Never",
                                "args": ["node", str(i)], "securityContext": {
                                    "runAsUser": DATABASE_UID, "runAsGroup": DATABASE_UID,
                                    # The image's setpriv entry point applies NNP after
                                    # AppArmor admission (Ubuntu crun stub compatibility).
                                    "capabilities": {"drop": ["ALL"]}},
                                "volumeMounts": mounts}], "volumes": volumes}})
    return documents


def population_rows(existing, requested):
    if existing != 0:
        raise ValueError("demo_account already contains data; initialization never truncates/replaces it")
    if not isinstance(requested, int) or not 1000 <= requested <= 1000000:
        raise ValueError("rows must be in 1000..1000000")
    return requested


def row_snapshot(stream):
    full = hashlib.sha256()
    immutable = hashlib.sha256()
    rows = total = previous = 0
    for line in stream:
        columns = line.rstrip(b"\n").split(b"\t")
        if len(columns) != 3:
            raise ValueError("unexpected COPY row shape")
        key, value = int(columns[0]), int(columns[1])
        if key <= previous or value < 0:
            raise ValueError("duplicate/unsorted key or negative counter")
        previous = key
        rows += 1
        total += value
        full.update(line)
        immutable.update(columns[0]+b"\t"+columns[2]+b"\n")
    return {"rows": rows, "sum": total, "sha256": full.hexdigest(),
            "key_payload_sha256": immutable.hexdigest()}
