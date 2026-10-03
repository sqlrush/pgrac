#!/usr/bin/env python3
"""Submit or inspect an exact membership request through an admin service.

Copyright (c) 2026, PostgreSQL Global Development Group
Copyright (c) 2026, PGRAC contributors
Author: SqlRush <sqlrush@gmail.com>
IDENTIFICATION: scripts/deploy/pre2/membership.py
NOTES: The server owns execution; connection loss never cancels its operation.
"""
import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys


class ServerError(RuntimeError):
    pass


def unique_keys(pairs):
    result = {}
    for name, value in pairs:
        if name in result:
            raise ValueError("duplicate JSON key")
        result[name] = value
    return result


def read_json(stream):
    def constant(_value):
        raise ValueError("non-finite JSON value")
    return json.load(stream, object_pairs_hook=unique_keys, parse_constant=constant)


def validate_request(request):
    fields = {"version", "operation_kind", "target_node", "guest_uuid", "expected_formation",
              "operation_generation", "expected_old_incarnation", "reserved_new_incarnation"}
    if type(request) is not dict or request.keys() != fields:
        raise ValueError("exact membership request fields required")
    if type(request["version"]) is not int or request["version"] != 1:
        raise ValueError("unsupported membership request version")
    if request["operation_kind"] not in ("leave", "remove", "rejoin"):
        raise ValueError("unsupported membership operation")
    if type(request["target_node"]) is not int or not 0 <= request["target_node"] < 16:
        raise ValueError("invalid target node")
    guest = request["guest_uuid"]
    if (type(guest) is not str or not re.fullmatch(r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}", guest)
            or int(guest.replace("-", ""), 16) == 0):
        raise ValueError("canonical nonzero guest UUID required")
    for key in ("expected_formation", "operation_generation", "expected_old_incarnation", "reserved_new_incarnation"):
        value = request[key]
        if type(value) is not int or not (0 if key == "reserved_new_incarnation" else 1) <= value < 2**64:
            raise ValueError("invalid unsigned membership identity")
    new = request["reserved_new_incarnation"]
    if ((request["operation_kind"] == "rejoin" and new <= request["expected_old_incarnation"])
            or (request["operation_kind"] != "rejoin" and new != 0)):
        raise ValueError("invalid successor incarnation")
    return request


def validate_response(response, action, request):
    statuses = {
        "precheck": {"ready", "blocked", "conflict", "stale", "rejected"},
        "execute": {"accepted", "retry", "blocked", "conflict", "stale", "rejected"},
        "status": {"idle", "reserved", "running", "finished", "cancelled_unpublished", "blocked"},
    }
    if (type(response) is not dict or response.keys() != {"version", "action", "status", "request", "reason"}
            or type(response["version"]) is not int or response["version"] != 1
            or response["action"] != action or type(response["status"]) is not str
            or response["status"] not in statuses[action]
            or type(response["reason"]) is not str):
        raise ValueError("membership response shape")
    actual = response["request"]
    if actual is not None:
        validate_request(actual)
    if request is not None and actual != request:
        raise ValueError("membership response identity changed")
    if actual is None and (action != "status" or response["status"] not in ("idle", "blocked")):
        raise ValueError("membership response lacks operation identity")
    return response


def call_server(action, request, service, psql="psql"):
    if action not in ("precheck", "execute", "status"):
        raise ValueError("unknown membership action")
    if type(service) is not str or not re.fullmatch(r"[A-Za-z0-9_-]{1,100}", service):
        raise ValueError("invalid admin service name")
    if request is not None:
        validate_request(request)
    elif action != "status":
        raise ValueError("exact membership request required")
    env = {key: value for key, value in os.environ.items() if not key.startswith("PG")}
    for key in ("PGSERVICEFILE", "PGPASSFILE"):
        if key in os.environ:
            env[key] = os.environ[key]
    env.update(PGSERVICE=service, PGCONNECT_TIMEOUT="5", LC_ALL="C")
    argv = [str(psql), "-X", "-q", "-A", "-t", "-w", "-v", "ON_ERROR_STOP=1",
            "-v", "action=" + action,
            "-v", "request=" + json.dumps(request, sort_keys=True, separators=(",", ":"))]
    # psql's quoted variables preserve all uint64 digits without SQL interpolation.
    query = "SELECT pg_catalog.pg_cluster_membership_command(:'action', :'request'::jsonb);\n"
    try:
        reply = subprocess.run(argv, input=query, text=True, capture_output=True, env=env,
                               timeout=610, check=False)
        if reply.returncode:
            raise ServerError("membership outcome unknown; inspect status with the same request")
        import io
        return validate_response(read_json(io.StringIO(reply.stdout)), action, request)
    except (OSError, subprocess.TimeoutExpired, ValueError) as error:
        # Neither libpq's stderr nor command arguments belong in this diagnostic.
        raise ServerError("membership outcome unknown; inspect status with the same request") from None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("precheck", "execute", "status"))
    parser.add_argument("--service", required=True)
    parser.add_argument("--request", type=Path)
    parser.add_argument("--psql", default="psql")
    args = parser.parse_args()
    try:
        if args.request is None:
            request = None
        else:
            with args.request.open() as stream:
                request = read_json(stream)
        response = call_server(args.action, request, args.service, args.psql)
        print(json.dumps(response, sort_keys=True))
        return 2 if response["status"] in ("blocked", "conflict", "stale", "rejected") else 0
    except (ValueError, ServerError, OSError) as error:
        print(str(error), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
