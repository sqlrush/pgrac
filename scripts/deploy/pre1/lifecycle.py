#!/usr/bin/env python3
"""Observe four-node lifecycle state without starting, stopping or recovering data.

Author: SqlRush <sqlrush@gmail.com>

Only status and reconcile are exposed. Native clean control files are one fact,
not permission to restart without current protocol and persistent closure.
"""

import hashlib
import json
import os
from pathlib import Path
import re
import sys

from common import PreflightError, document_sha, load_json, publish_artifact, unique
from preflight import SafeParser
import remote


def validate_nodes(nodes, binary_sha256):
    if type(nodes) is not list or len(nodes) != 4:
        raise PreflightError("FOUR_NODES_REQUIRED")
    for node in nodes:
        remote.validate_node(node, binary_sha256)
    for key in ("node_id", "vm_uuid", "machine_id", "boot_id"):
        unique([node[key] for node in nodes], "nodes." + key)
    if {node["node_id"] for node in nodes} != set(range(4)):
        raise PreflightError("FOUR_NODES_REQUIRED")


def bind_observations(nodes, binary_sha256, observations):
    """Recheck both the raw guest reply and its controller-side identity binding."""
    program_hash = hashlib.sha256(Path(remote.__file__).with_name("guest_status.py").read_bytes()).hexdigest()
    try:
        if (type(observations) is not list or len(observations) != 4
                or any(type(r["node_id"]) is not int for r in observations)
                or {r["node_id"] for r in observations} != set(range(4))):
            raise ValueError
        ordered = sorted(observations, key=lambda r: r["node_id"])
        for node, record in zip(sorted(nodes, key=lambda n: n["node_id"]), ordered):
            request = remote.request_for(node, binary_sha256)
            status, reason, facts = remote.decode_observation(record["transport"], request)
            if (record["node_sha256"] != document_sha(node)
                    or record["request_sha256"] != document_sha(request)
                    or record["program_sha256"] != program_hash
                    or (record["status"], record["reason"], record["observation"]) != (status, reason, facts)
                    or record["kind"] != "pre1-remote-status"
                    or record["deployment_qualified"] is not False
                    or any(type(record[k]) is not int or record[k] <= 0 for k in
                           ("begin_monotonic_ns", "end_monotonic_ns"))
                    or record["end_monotonic_ns"] < record["begin_monotonic_ns"]):
                raise ValueError
        return ordered
    except (KeyError, TypeError, ValueError):
        raise PreflightError("LIFECYCLE_OBSERVATION_MISMATCH") from None


def reconcile(nodes, binary_sha256, observations, expected_system_identifier=None):
    validate_nodes(nodes, binary_sha256)
    if expected_system_identifier is not None and (
            type(expected_system_identifier) is not str
            or not re.fullmatch(r"[1-9][0-9]{0,19}", expected_system_identifier)
            or int(expected_system_identifier) > 18446744073709551615):
        raise PreflightError("SYSTEM_IDENTIFIER_INVALID")
    records = bind_observations(nodes, binary_sha256, observations)
    result = {"schema_version": 1, "kind": "pre1-lifecycle-observation",
              "scope": "LIFECYCLE_OBSERVATION_ONLY", "status": "PASS",
              "state": "OBSERVATION_INCOMPLETE", "data_clean": False,
              "process_gone": False, "restart_allowed": False,
              "deployment_qualified": False, "system_identifier": None,
              "pending": [], "observations": records,
              "begin_monotonic_ns": min(r["begin_monotonic_ns"] for r in records),
              "end_monotonic_ns": max(r["end_monotonic_ns"] for r in records)}
    if any(record["status"] != "PASS" for record in records):
        result["status"] = "ERROR"
        return result
    facts = [record["observation"] for record in records]
    result["process_gone"] = all(not f["processes"] and f["pidfile"] is None for f in facts)
    states = {f["pgdata_state"] for f in facts}
    if states <= {"EMPTY", "ABSENT"}:
        result["state"] = "EMPTY_NOT_INITIALIZED" if result["process_gone"] else "PROCESSES_PRESENT"
        return result
    if states != {"INITIALIZED"}:
        result["state"] = "PARTIAL_DATASET"
        return result
    controls = [f["control"] for f in facts]
    if any(control["rc"] != 0 or control["parsed"] is None for control in controls):
        result["status"] = "ERROR"
        return result
    identifiers = {control["parsed"]["system_identifier"] for control in controls}
    if len(identifiers) != 1 or (expected_system_identifier is not None and identifiers != {expected_system_identifier}):
        result.update(status="BLOCKED", state="IDENTITY_MISMATCH")
        return result
    result["system_identifier"] = next(iter(identifiers))
    result["data_clean"] = all(control["parsed"]["state"] == "shut down" for control in controls)
    if not result["process_gone"]:
        result["state"] = "PROCESSES_PRESENT"
    elif not result["data_clean"]:
        result["state"] = "UNCLEAN_OR_UNKNOWN"
    else:
        result["state"] = "DATA_CLEAN_CLOSURE_UNPROVEN"
        result["pending"] = ["PROTOCOL_CLOSED", "PERSISTENT_CLOSURE"]
    return result


def main(argv=None):
    try:
        parser = SafeParser(description=__doc__)
        parser.add_argument("action", choices=("status", "reconcile"))
        parser.add_argument("--nodes", nargs=4, type=Path, required=True)
        parser.add_argument("--binary-sha256", required=True)
        parser.add_argument("--system-identifier")
        parser.add_argument("--out", type=Path, required=True)
        args = parser.parse_args(argv)
        if os.path.lexists(args.out):
            raise PreflightError("ARTIFACT_EXISTS", "out", "ERROR")
        nodes = [load_json(path) for path in args.nodes]
        validate_nodes(nodes, args.binary_sha256)
        records = [remote.observe(node, args.binary_sha256) for node in nodes]
        artifact = reconcile(nodes, args.binary_sha256, records, args.system_identifier)
        checksum = publish_artifact(args.out, artifact)
        answer = {k: artifact[k] for k in ("status", "scope", "state", "data_clean", "process_gone")}
        answer["artifact_sha256"] = checksum
    except PreflightError as exc:
        answer = {"status": exc.status, "reason": exc.reason, "field": exc.field}
    except (OSError, ValueError, TypeError, KeyError, RecursionError):
        answer = {"status": "ERROR", "reason": "LIFECYCLE_OBSERVATION_UNAVAILABLE"}
    answer.update(restart_allowed=False, deployment_qualified=False)
    print(json.dumps(answer, sort_keys=True))
    return {"PASS": 0, "BLOCKED": 2, "ERROR": 3}[answer["status"]]


if __name__ == "__main__":
    sys.exit(main())
