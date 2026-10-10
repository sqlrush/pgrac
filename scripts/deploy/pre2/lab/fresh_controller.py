#!/usr/bin/env python3
"""Controller for a fresh PRE2 four-VM laboratory database.

Author: SqlRush <sqlrush@gmail.com>

    fresh_controller.py plan    --request REQ --source SRC --binary-sha256 SHA --out PLAN
    fresh_controller.py render  --plan PLAN --request REQ --source SRC --out-dir DIR
    fresh_controller.py writers --plan PLAN --request REQ --transport local|ssh
                                [--tools-root DIR] [--local-validation] --out RESULT
    fresh_controller.py advance --plan PLAN
    fresh_controller.py new-identity
    fresh_controller.py memory-check --plan PLAN --request REQ --source SRC --install DIR --out RESULT
    fresh_controller.py cohort|distribute|bootstrap-check --plan PLAN --request REQ --source SRC
                                --transport local|ssh [--tools-root DIR] [--local-validation] --out RESULT

cohort       (cohort candidates) one original creator runs initdb once for all members.
distribute   copies each node_N unchanged to its member's new local PGDATA (manifest-checked).
bootstrap-check runs the candidate's read-only early preparation on every member.

plan     validates the request, reads the candidate source contract, renders
         and policy-checks the configuration, and records every wait point.
render   writes the per-node files for review; it does not install them.
writers  creates one original native writer per node: the founder first,
         then the others with the founder's system identifier. It stops at the
         first failure and never retries, deletes or overwrites.
advance  reports the first blocked stage after the writers. It exits 3 while
         any wait point is BLOCKED and never starts a server.

Evidence documents are published once and never replaced.
"""

import argparse
import json
import os
from pathlib import Path
import pwd
import shlex
import subprocess
import sys
import tempfile
import time
import secrets

from lab_common import (BLOCKED, ERROR, FOUNDER_NODE, PASS, READY, PreflightError, document_sha,
                        file_sha256, load_json, publish_artifact)
import cohort_controller
import fresh_plan
import render
import request as lab_request
import source_contract

GUEST_TIMEOUT = 2400
EXIT_BLOCKED = 3


def username_for(node, local):
    if local:
        return pwd.getpwuid(node["uid"]).pw_name
    # The guest resolves the name itself; the plan shows the conventional one.
    return "${DATABASE_USER}"


def load_inputs(args):
    request, derived = lab_request.validate(load_json(args.request))
    contract = source_contract.collect(args.source)
    return request, derived, contract


def verify_flush_evidence(request):
    """The declared evidence file must exist on the controller with the declared hash."""
    cache = request["storage_write_cache"]
    if cache["mode"] != "write-back":
        return
    evidence = cache["flush_verification"]
    try:
        actual = file_sha256(evidence["path"])
    except PreflightError:
        raise PreflightError("WRITE_CACHE_FLUSH_EVIDENCE_MISSING", "storage_write_cache") from None
    if actual != evidence["sha256"]:
        raise PreflightError("WRITE_CACHE_FLUSH_EVIDENCE_CHANGED", "storage_write_cache")


def cmd_plan(args):
    request, derived, contract = load_inputs(args)
    verify_flush_evidence(request)
    rendered = render.render(request, derived, contract)
    users = {n["node_id"]: username_for(n, args.local_validation) for n in request["nodes"]}
    plan = fresh_plan.build(request, derived, contract, rendered, users)
    plan["binary_sha256"] = args.binary_sha256
    plan["local_validation"] = bool(args.local_validation)
    publish_artifact(args.out, plan)
    print(json.dumps({"status": PASS, "stages": plan["stages"]}, sort_keys=True))
    return 0


def require_plan(args, request, contract):
    plan = load_json(args.plan)
    if plan.get("kind") != "pre2-lab-fresh-plan" or plan["request_sha256"] != document_sha(request) \
            or plan["contract_sha256"] != document_sha(contract):
        raise PreflightError("PLAN_STALE", "plan")
    return plan


def cmd_render(args):
    request, derived, contract = load_inputs(args)
    require_plan(args, request, contract)
    rendered = render.render(request, derived, contract)
    out = Path(args.out_dir)
    out.mkdir(mode=0o700)
    for node_id, files in rendered["files"].items():
        directory = out / ("node%d" % node_id)
        directory.mkdir(mode=0o700)
        for name, text in files.items():
            (directory / name).write_text(text)
    (out / "shared-entries.json").write_text(json.dumps(rendered["shared_entries"], indent=1) + "\n")
    print(json.dumps({"status": PASS, "files": sorted(str(p.relative_to(out)) for p in out.rglob("*") if p.is_file()),
                      "policy_refusals": rendered["policy_refusals"]}, sort_keys=True))
    return 0


def guest_request(plan, request, node, layout_node, mode):
    founder = node["node_id"] == FOUNDER_NODE
    config = plan.get("config_request") if founder and layout_node.get("config_request_path") else None
    return {"schema_version": 1, "action": "create-native-writer", "mode": mode, "node": node,
            "binary_sha256": plan["binary_sha256"], "shared_mount": request["shared_mount"],
            "fs_uuid": request["fs_uuid"], "dataset_root": plan["layout"]["shared"]["dataset_root"],
            "wal_threads_dir": plan["layout"]["shared"]["wal_threads_dir"],
            "pgdata": layout_node["pgdata"], "wal_dir": layout_node["wal_dir"],
            "thread": layout_node["thread"], "shared_base": layout_node["shared_base"],
            "storage_uuid": request["storage_uuid"] if founder else None,
            "database_incarnation": request["database_incarnation"] if founder else None,
            "system_identifier": request["system_identifier"],
            "config_request": config["text"] if config else None,
            "config_path": layout_node.get("config_request_path") if config else None,
            "config_sha256": config["sha256"] if config else None}


def run_guest_local(payload):
    script = Path(__file__).with_name("fresh_guest.py")
    completed = subprocess.run([sys.executable, "-I", str(script)], input=json.dumps(payload),
                               capture_output=True, text=True, timeout=GUEST_TIMEOUT, check=False)
    return completed.returncode, completed.stdout, completed.stderr


def run_guest_ssh(node, payload, tools_root):
    """Pinned host key, explicit identity, no agent; the guest runs the installed copy."""
    admin = node["admin_endpoint"]
    with tempfile.NamedTemporaryFile(mode="w", prefix="pre2-known-hosts-") as known:
        name = admin["host"] if admin["port"] == 22 else "[%s]:%d" % (admin["host"], admin["port"])
        known.write("%s %s\n" % (name, node["ssh_host_key"]))
        known.flush()
        argv = ["ssh", "-F", "/dev/null", "-T", "-p", str(admin["port"]),
                "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes",
                "-o", "UserKnownHostsFile=" + known.name, "-o", "GlobalKnownHostsFile=/dev/null",
                "-o", "ConnectTimeout=10", "-o", "ForwardAgent=no", "-o", "IdentitiesOnly=yes",
                "-o", "IdentityAgent=none", "-i", admin["identity_file"], "-l", admin["user"], admin["host"],
                "sudo -n /usr/bin/python3 -I " + shlex.quote(str(Path(tools_root) / "pre2/lab/fresh_guest.py"))]
        completed = subprocess.run(argv, input=json.dumps(payload), capture_output=True, text=True,
                                   timeout=GUEST_TIMEOUT, check=False)
        return completed.returncode, completed.stdout, completed.stderr


def decode_guest(rc, stdout, stderr):
    try:
        result = json.loads(stdout.strip().splitlines()[-1])
    except (ValueError, IndexError):
        return {"status": ERROR, "state": "GUEST_RESULT_UNREADABLE", "rc": rc, "stderr": stderr[-4096:]}
    if type(result) is not dict:
        return {"status": ERROR, "state": "GUEST_RESULT_UNREADABLE", "rc": rc}
    result["transport_rc"] = rc
    return result


def cmd_writers(args):
    request, derived, contract = load_inputs(args)
    plan = require_plan(args, request, contract)
    if plan.get("creation") == "cohort":
        print(json.dumps({"status": ERROR, "reason": "COHORT_CANDIDATE_USES_COHORT_AND_DISTRIBUTE"}))
        return 2
    if plan["stages"]["native_writers"]["status"] != READY:
        print(json.dumps({"status": BLOCKED, "stage": "native_writers",
                          "wait_points": plan["stages"]["native_writers"]["wait_points"]}))
        return EXIT_BLOCKED
    if args.local_validation != plan["local_validation"]:
        raise PreflightError("PLAN_MODE_MISMATCH", "local_validation")
    mode = "local-validation" if args.local_validation else "lab"
    by_id = {node["node_id"]: node for node in request["nodes"]}
    outcome = {"schema_version": 1, "kind": "pre2-lab-native-writers", "status": ERROR,
               "plan_sha256": document_sha(plan), "mode": mode, "deployment_qualified": False,
               "nodes": []}
    system_identifier = request["system_identifier"]
    order = [FOUNDER_NODE] + sorted(n for n in by_id if n != FOUNDER_NODE)
    for node_id in order:
        layout_node = next(n for n in plan["nodes"] if n["node_id"] == node_id)
        payload = guest_request(plan, request, by_id[node_id], layout_node, mode)
        if args.transport == "local":
            rc, stdout, stderr = run_guest_local(payload)
        else:
            rc, stdout, stderr = run_guest_ssh(by_id[node_id], payload, args.tools_root)
        result = decode_guest(rc, stdout, stderr)
        outcome["nodes"].append(result)
        if result.get("status") != PASS:
            outcome["state"] = "WRITER_FAILED_NODE_%d" % node_id
            break
        if result["system_identifier"] != system_identifier:
            outcome["state"] = "SYSTEM_IDENTIFIER_DIVERGED"
            break
    else:
        outcome.update(status=PASS, state="NATIVE_WRITERS_CREATED", system_identifier=system_identifier,
                       deployment_qualified=mode == "lab"
                       and all(r.get("deployment_qualified") for r in outcome["nodes"]),
                       next_stage=fresh_plan.stage_status(plan["wait_points"]))
    publish_artifact(args.out, outcome)
    print(json.dumps({"status": outcome["status"], "state": outcome["state"],
                      "system_identifier": system_identifier}, sort_keys=True))
    return 0 if outcome["status"] == PASS else 1


def new_identity(now_ns=None, pid=None):
    """Identity fields for a new database request.

    The system identifier follows PostgreSQL's own construction (seconds in the
    high 32 bits, microseconds and the low process-id bits below); the storage
    and authority identities are independent random 128-bit values.
    """
    now_ns = time.time_ns() if now_ns is None else now_ns
    pid = os.getpid() if pid is None else pid
    seconds, micros = divmod(now_ns // 1000, 1000000)
    sysid = ((seconds & 0xFFFFFFFF) << 32) | (micros << 12) | (pid & 0xFFF)
    def uuid4_hex():
        raw = bytearray(secrets.token_bytes(16))
        raw[6] = (raw[6] & 0x0F) | 0x40          # version 4
        raw[8] = (raw[8] & 0x3F) | 0x80          # RFC 4122 variant
        return raw.hex()
    storage, authority = uuid4_hex(), uuid4_hex()
    while authority == storage:
        storage, authority = uuid4_hex(), uuid4_hex()
    return {"system_identifier": str(sysid), "storage_uuid": storage, "authority_uuid": authority}


# Shared-mode, identity and path settings select startup authority; they are not
# shared memory sizing inputs and cannot be evaluated on a scratch directory.
MEMORY_CHECK_EXCLUDE = {"cluster.shared_config", "cluster.shared_catalog", "cluster.controlfile_shared_authority",
                        "cluster.merged_recovery", "cluster.shared_data_dir", "cluster.wal_threads_dir",
                        "cluster.undo_tablespace_path", "cluster.shared_storage_uuid", "cluster.config_file",
                        "listen_addresses", "unix_socket_directories", "port", "debug_io_direct"}


def memory_settings(plan, node_id=FOUNDER_NODE):
    values = {}
    for entry in plan["shared_entries"]:
        if entry["node_id"] in (None, node_id) and entry["name"] not in MEMORY_CHECK_EXCLUDE:
            values[entry["name"]] = entry["value"]
    return values


def cmd_memory_check(args):
    """Shared memory of the rendered profile, computed by the candidate itself."""
    request, derived, contract = load_inputs(args)
    plan = require_plan(args, request, contract)
    rendered = render.render(request, derived, contract)
    install = Path(args.install)
    budget_mb = int(request["guest_memory_gib"] * 1024 * render.SHMEM_FRACTION_OF_GUEST)
    with tempfile.TemporaryDirectory(prefix="pre2-memcheck-") as scratch:
        data = Path(scratch) / "data"
        made = subprocess.run([str(install / "bin/initdb"), "-D", str(data), "-A", "trust", "--no-locale",
                               "-E", "UTF8", "-N"], capture_output=True, text=True, timeout=600)
        if made.returncode:
            raise PreflightError("MEMORY_CHECK_SCRATCH_FAILED")
        (data / "pgrac.conf").write_text(rendered["files"][FOUNDER_NODE]["pgrac.conf"])
        argv = [str(install / "bin/postgres"), "-D", str(data), "-c", "cluster.config_file=%s/pgrac.conf" % data]
        settings = memory_settings(plan)
        for name, value in sorted(settings.items()):
            argv += ["-c", "%s=%s" % (name, value)]
        probe = subprocess.run(argv + ["-C", "shared_memory_size"], capture_output=True, text=True, timeout=120)
    try:
        shmem_mb = int(probe.stdout.strip())
    except ValueError:
        raise PreflightError("MEMORY_CHECK_PROBE_FAILED") from None
    result = {"schema_version": 1, "kind": "pre2-lab-memory-check", "plan_sha256": document_sha(plan),
              "memory_profile": request["memory_profile"], "guest_memory_gib": request["guest_memory_gib"],
              "shared_memory_mb": shmem_mb, "budget_mb": budget_mb, "settings": settings,
              "postgres_sha256": file_sha256(install / "bin/postgres"),
              "status": PASS if shmem_mb <= budget_mb else BLOCKED,
              "state": "SHARED_MEMORY_FITS" if shmem_mb <= budget_mb else "SHARED_MEMORY_EXCEEDS_GUEST_BUDGET"}
    publish_artifact(args.out, result)
    print(json.dumps({k: result[k] for k in ("status", "state", "shared_memory_mb", "budget_mb")}, sort_keys=True))
    return 0 if result["status"] == PASS else EXIT_BLOCKED


def cmd_cohort_step(args):
    request, _derived, contract = load_inputs(args)
    plan = require_plan(args, request, contract)
    step = {"cohort": cohort_controller.cmd_cohort, "distribute": cohort_controller.cmd_distribute,
            "bootstrap-check": cohort_controller.cmd_bootstrap_check}[args.command]
    return step(args, request, plan)


def cmd_new_identity(args):
    print(json.dumps(new_identity(), sort_keys=True))
    return 0


def cmd_advance(args):
    plan = load_json(args.plan)
    # Evidence documents use sorted keys; the stage order is the plan's fixed order.
    blocked = [(stage, plan["stages"][stage]["wait_points"]) for stage in fresh_plan.STAGES
               if stage not in ("render", "native_writers") and plan["stages"][stage]["status"] != READY]
    if blocked:
        stage, points = blocked[0]
        evidence = {p["id"]: p["evidence"] for p in plan["wait_points"] if p["id"] in points}
        print(json.dumps({"status": BLOCKED, "first_blocked_stage": stage, "wait_points": points,
                          "evidence": evidence, "all_blocked": [s for s, _ in blocked]}, sort_keys=True))
        return EXIT_BLOCKED
    # Unreachable with the current contract: SHARED_OPEN_ENTRY is always BLOCKED until
    # this tool is updated for a real product entry.
    print(json.dumps({"status": ERROR, "state": "TOOL_UPDATE_REQUIRED"}))
    return 2


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("plan")
    p.add_argument("--request", required=True)
    p.add_argument("--source", required=True)
    p.add_argument("--binary-sha256", required=True)
    p.add_argument("--local-validation", action="store_true")
    p.add_argument("--out", required=True)
    p = sub.add_parser("render")
    for flag in ("--plan", "--request", "--source", "--out-dir"):
        p.add_argument(flag, required=True)
    p = sub.add_parser("writers")
    for flag in ("--plan", "--request", "--source", "--out"):
        p.add_argument(flag, required=True)
    p.add_argument("--transport", choices=("local", "ssh"), required=True)
    p.add_argument("--tools-root", default="/opt/pgrac-pre2-lab-tools")
    p.add_argument("--local-validation", action="store_true")
    p = sub.add_parser("advance")
    p.add_argument("--plan", required=True)
    sub.add_parser("new-identity")
    for name in ("cohort", "distribute", "bootstrap-check"):
        p = sub.add_parser(name)
        for flag in ("--plan", "--request", "--source", "--out"):
            p.add_argument(flag, required=True)
        p.add_argument("--transport", choices=("local", "ssh"), required=True)
        p.add_argument("--tools-root", default="/opt/pgrac-pre2-lab-tools")
        p.add_argument("--local-validation", action="store_true")
    p = sub.add_parser("memory-check")
    for flag in ("--plan", "--request", "--source", "--install", "--out"):
        p.add_argument(flag, required=True)
    args = parser.parse_args(argv)
    try:
        return {"plan": cmd_plan, "render": cmd_render, "writers": cmd_writers,
                "advance": cmd_advance, "new-identity": cmd_new_identity,
                "memory-check": cmd_memory_check, "cohort": cmd_cohort_step,
                "distribute": cmd_cohort_step, "bootstrap-check": cmd_cohort_step}[args.command](args)
    except PreflightError as exc:
        print(json.dumps({"status": exc.status, "reason": exc.reason, "field": exc.field}))
        return 2
    except (OSError, subprocess.SubprocessError) as exc:
        print(json.dumps({"status": ERROR, "reason": "CONTROLLER_IO", "detail": type(exc).__name__}))
        return 2


if __name__ == "__main__":
    sys.exit(main())
