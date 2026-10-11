"""Controller commands for cohort releases: create once, distribute exact PGDATA, bootstrap check.

Author: SqlRush <sqlrush@gmail.com>
"""

import base64
import contextlib
import json
from pathlib import Path, PurePosixPath
import shlex
import subprocess
import sys
import tempfile

from lab_common import BLOCKED, ERROR, FOUNDER_NODE, PASS, READY, PreflightError, document_sha, publish_artifact

GUEST_TIMEOUT = 3900


@contextlib.contextmanager
def guest_argv(node, transport, tools_root, b64=None):
    """argv that runs fresh_guest.py for one node; JSON on stdin, or a stream action via b64."""
    script = "pre2/lab/fresh_guest.py"
    tail = ["--request-b64", b64] if b64 is not None else []
    if transport == "local":
        yield [sys.executable, "-I", str(Path(__file__).with_name("fresh_guest.py"))] + tail
        return
    admin = node["admin_endpoint"]
    with tempfile.NamedTemporaryFile(mode="w", prefix="pre2-known-hosts-") as known:
        name = admin["host"] if admin["port"] == 22 else "[%s]:%d" % (admin["host"], admin["port"])
        known.write("%s %s\n" % (name, node["ssh_host_key"]))
        known.flush()
        remote = "sudo -n /usr/bin/python3 -I " + shlex.quote(str(PurePosixPath(tools_root) / script))
        if tail:
            remote += " " + " ".join(shlex.quote(t) for t in tail)
        yield ["ssh", "-F", "/dev/null", "-T", "-p", str(admin["port"]), "-o", "BatchMode=yes",
               "-o", "StrictHostKeyChecking=yes", "-o", "UserKnownHostsFile=" + known.name,
               "-o", "GlobalKnownHostsFile=/dev/null", "-o", "ConnectTimeout=10", "-o", "ForwardAgent=no",
               "-o", "IdentitiesOnly=yes", "-o", "IdentityAgent=none", "-i", admin["identity_file"],
               "-l", admin["user"], admin["host"], remote]


def call(node, payload, transport, tools_root):
    with guest_argv(node, transport, tools_root) as argv:
        done = subprocess.run(argv, input=json.dumps(payload), capture_output=True, text=True,
                              timeout=GUEST_TIMEOUT, check=False)
    try:
        result = json.loads(done.stdout.strip().splitlines()[-1])
    except (ValueError, IndexError):
        return {"status": ERROR, "state": "GUEST_RESULT_UNREADABLE", "rc": done.returncode,
                "stderr": done.stderr[-4000:]}
    result["transport_rc"] = done.returncode
    return result


def encode(payload):
    return base64.b64encode(json.dumps(payload).encode()).decode()


def transfer(source_node, target_node, export, imported, transport, tools_root):
    """Pipe the founder's tar stream into the member's new staging directory."""
    with guest_argv(source_node, transport, tools_root, encode(export)) as src_argv, \
            guest_argv(target_node, transport, tools_root, encode(imported)) as dst_argv:
        src = subprocess.Popen(src_argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        dst = subprocess.Popen(dst_argv, stdin=src.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        src.stdout.close()
        dst_out, dst_err = dst.communicate(timeout=GUEST_TIMEOUT)
        src_err = src.stderr.read()
        src.wait(timeout=60)
    return {"export_rc": src.returncode, "import_rc": dst.returncode,
            "export_stderr": src_err.decode(errors="replace")[-2000:],
            "import_stderr": dst_err.decode(errors="replace")[-2000:]}


def mode_of(args, plan):
    if args.local_validation != plan["local_validation"]:
        raise PreflightError("PLAN_MODE_MISMATCH", "local_validation")
    return "local-validation" if args.local_validation else "lab"


def require_cohort(plan, stage="native_writers"):
    if plan.get("creation") != "cohort":
        raise PreflightError("PLAN_NOT_COHORT")
    if plan["stages"][stage]["status"] != READY:
        print(json.dumps({"status": BLOCKED, "stage": stage, "wait_points": plan["stages"][stage]["wait_points"]}))
        return False
    return True


def cmd_cohort(args, request, plan):
    if not require_cohort(plan):
        return 3
    mode = mode_of(args, plan)
    by_id = {n["node_id"]: n for n in request["nodes"]}
    c = plan["cohort"]
    payload = {"schema_version": 1, "action": "create-cohort", "mode": mode, "node": by_id[FOUNDER_NODE],
               "binary_sha256": plan["binary_sha256"], "shared_mount": request["shared_mount"],
               "fs_uuid": request["fs_uuid"], "dataset_root": plan["layout"]["shared"]["dataset_root"],
               "shared_roots": c["shared_roots"], "cache_root": c["cache_root"],
               "members": [m["node_id"] for m in c["members"]], "system_identifier": request["system_identifier"],
               "config_request": plan["config_request"]["text"], "config_path": c["config_request_path"],
               "config_sha256": plan["config_request"]["sha256"], "initdb_argv": c["initdb_argv"]}
    result = call(by_id[FOUNDER_NODE], payload, args.transport, args.tools_root)
    result["plan_sha256"] = document_sha(plan)
    publish_artifact(args.out, result)
    print(json.dumps({"status": result.get("status"), "state": result.get("state")}, sort_keys=True))
    return 0 if result.get("status") == PASS else 1


def cmd_distribute(args, request, plan):
    """Members 1..N first, then the founder's own copy; stop at the first failure."""
    if not require_cohort(plan):
        return 3
    mode = mode_of(args, plan)
    by_id = {n["node_id"]: n for n in request["nodes"]}
    founder = by_id[FOUNDER_NODE]
    cache = plan["cohort"]["cache_root"]
    outcome = {"schema_version": 1, "kind": "pre2-lab-cohort-distribution", "status": ERROR, "mode": mode,
               "plan_sha256": document_sha(plan), "members": []}
    order = sorted(plan["cohort"]["members"], key=lambda m: (m["node_id"] == FOUNDER_NODE, m["node_id"]))
    for member in order:
        n, name = member["node_id"], "node_%d" % member["node_id"]
        row = {"node_id": n, "source": member["source"], "pgdata": member["pgdata"]}
        outcome["members"].append(row)
        source = call(founder, {"action": "pgdata-manifest", "node": founder, "path": member["source"]},
                      args.transport, args.tools_root)
        if source.get("status") != PASS:
            row["state"] = "SOURCE_MANIFEST_FAILED"
            row["detail"] = source
            break
        row["source_manifest_sha256"] = source["manifest"]["sha256"]
        row["source_entries"] = source["manifest"]["count"]
        staging = str(PurePosixPath(member["pgdata"]).parent / (".cohort-staging-" + name))
        row["transfer"] = transfer(founder, by_id[n],
                                   {"action": "export-pgdata", "node": founder, "parent": cache, "name": name},
                                   {"action": "import-pgdata", "node": by_id[n], "staging": staging},
                                   args.transport, args.tools_root)
        if row["transfer"]["export_rc"] != 0 or row["transfer"]["import_rc"] != 0:
            row["state"] = "TRANSFER_FAILED"
            break
        installed = call(by_id[n], {"action": "install-pgdata", "node": by_id[n], "staging": staging,
                                    "source_name": name, "pgdata": member["pgdata"],
                                    "expected_manifest_sha256": row["source_manifest_sha256"]},
                         args.transport, args.tools_root)
        row.update(state=installed.get("state"), install=installed)
        if installed.get("status") != PASS:
            break
    else:
        outcome.update(status=PASS, state="PGDATA_DISTRIBUTED")
    outcome.setdefault("state", "DISTRIBUTION_STOPPED")
    publish_artifact(args.out, outcome)
    print(json.dumps({"status": outcome["status"], "state": outcome["state"],
                      "members": [(r["node_id"], r.get("state")) for r in outcome["members"]]}, sort_keys=True))
    return 0 if outcome["status"] == PASS else 1


def cmd_bootstrap_check(args, request, plan):
    """Each member's release early preparation (postgres -C); ROOT must stay byte-identical."""
    mode_of(args, plan)
    by_id = {n["node_id"]: n for n in request["nodes"]}
    budget_mb = int(request["guest_memory_gib"] * 1024 * 0.6)
    outcome = {"schema_version": 1, "kind": "pre2-lab-bootstrap-check", "status": ERROR,
               "plan_sha256": document_sha(plan), "budget_mb": budget_mb, "members": []}
    for node in plan["nodes"]:
        n = node["node_id"]
        result = call(by_id[n], {"action": "bootstrap-check", "node": by_id[n], "pgdata": node["pgdata"],
                                 "config_file": plan["layout"]["nodes"][str(n) if str(n) in plan["layout"]["nodes"]
                                                                       else n]["local_config"],
                                 "shared_data_dir": plan["layout"]["shared"]["shared_data_dir"]},
                      args.transport, args.tools_root)
        result["node_id"] = n
        outcome["members"].append(result)
        if result.get("status") != PASS:
            outcome["state"] = "BOOTSTRAP_FAILED_NODE_%d" % n
            break
        if result["shared_memory_mb"] > budget_mb:
            outcome["state"] = "SHARED_MEMORY_EXCEEDS_GUEST_BUDGET"
            break
    else:
        outcome.update(status=PASS, state="BOOTSTRAP_PREPARED_ALL")
    publish_artifact(args.out, outcome)
    print(json.dumps({"status": outcome["status"], "state": outcome["state"],
                      "shared_memory_mb": [m.get("shared_memory_mb") for m in outcome["members"]]}, sort_keys=True))
    return 0 if outcome["status"] == PASS else 1
