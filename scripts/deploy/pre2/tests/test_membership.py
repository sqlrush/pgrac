#!/usr/bin/env python3
"""Tests for the membership CLI boundary, not cluster admission proofs.

Author: SqlRush <sqlrush@gmail.com>
"""
import copy
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import unittest
from unittest.mock import patch


SPEC = importlib.util.spec_from_file_location(
    "membership_cli", Path(__file__).resolve().parents[1] / "membership.py"
)
membership = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(membership)


def request():
    return {
        "version": 1,
        "operation_kind": "rejoin",
        "target_node": 4,
        "guest_uuid": "11111111-2222-3333-4444-555555555555",
        "expected_formation": 91,
        "operation_generation": 17,
        "expected_old_incarnation": 2**63 + 4,
        "reserved_new_incarnation": 2**63 + 5,
    }


class MembershipTests(unittest.TestCase):
    def test_valid_request_preserves_full_identity(self):
        original = request()
        self.assertEqual(membership.validate_request(original), original)
        for kind in ("leave", "remove"):
            candidate = request()
            candidate["operation_kind"] = kind
            candidate["reserved_new_incarnation"] = 0
            self.assertEqual(membership.validate_request(candidate), candidate)

    def test_missing_unknown_and_ambiguous_fields_reject(self):
        for key in request():
            candidate = request()
            del candidate[key]
            with self.subTest(missing=key), self.assertRaises(ValueError):
                membership.validate_request(candidate)
        for key, value in (
            ("version", 2), ("version", True), ("operation_kind", "join"),
            ("target_node", -1), ("target_node", 16), ("target_node", True),
            ("guest_uuid", "00000000-0000-0000-0000-000000000000"),
            ("guest_uuid", "node-four"), ("expected_formation", 0),
            ("operation_generation", 2**64), ("expected_old_incarnation", 0),
            ("reserved_new_incarnation", 2**63 + 4),
        ):
            candidate = request()
            candidate[key] = value
            with self.subTest(key=key, value=value), self.assertRaises(ValueError):
                membership.validate_request(candidate)
        candidate = request()
        candidate["force"] = True
        with self.assertRaises(ValueError):
            membership.validate_request(candidate)

    def test_duplicate_json_keys_reject(self):
        with self.assertRaises(ValueError):
            membership.read_json(io.StringIO('{"version":1,"version":2}'))

    def test_parameterized_sql_and_service_without_shell(self):
        response = {"version": 1, "action": "execute", "status": "accepted",
                    "request": request(), "reason": ""}
        completed = subprocess.CompletedProcess([], 0, json.dumps(response), "")
        with patch.object(membership.subprocess, "run", return_value=completed) as run:
            self.assertEqual(membership.call_server("execute", request(), "pgrac-admin"), response)
        args, kwargs = run.call_args
        command = args[0]
        self.assertNotIn("-c", command)
        self.assertNotIn("--dbname", command)
        self.assertNotIn("shell", kwargs)
        self.assertEqual(kwargs["env"]["PGSERVICE"], "pgrac-admin")
        self.assertIn("-X", command)
        self.assertIn("-w", command)
        self.assertIn("ON_ERROR_STOP=1", command)
        self.assertIn(":'request'::jsonb", kwargs["input"])
        self.assertNotIn(request()["guest_uuid"], kwargs["input"])

    def test_invalid_request_never_starts_psql(self):
        bad = request()
        bad["operation_kind"] = "join"
        with patch.object(membership.subprocess, "run") as run:
            with self.assertRaises(ValueError):
                membership.call_server("execute", bad, "pgrac-admin")
            run.assert_not_called()

    def test_wrong_response_identity_and_status_reject(self):
        response = {"version": 1, "action": "execute", "status": "accepted",
                    "request": request(), "reason": ""}
        for field in ("action", "version", "status", "request"):
            bad = copy.deepcopy(response)
            if field == "request":
                bad[field]["operation_generation"] += 1
            else:
                bad[field] = "unknown"
            completed = subprocess.CompletedProcess([], 0, json.dumps(bad), "")
            with patch.object(membership.subprocess, "run", return_value=completed):
                with self.subTest(field=field), self.assertRaises(membership.ServerError):
                    membership.call_server("execute", request(), "pgrac-admin")

    def test_transport_loss_reports_unknown_and_does_not_retry(self):
        completed = subprocess.CompletedProcess([], 2, "", "password=do-not-print")
        with patch.object(membership.subprocess, "run", return_value=completed) as run:
            with self.assertRaises(membership.ServerError) as caught:
                membership.call_server("execute", request(), "pgrac-admin")
            self.assertIn("unknown", str(caught.exception))
            self.assertNotIn("do-not-print", str(caught.exception))
            self.assertEqual(run.call_count, 1)

    def test_malformed_status_is_rejected_without_a_parser_traceback(self):
        for status in ([], {}, None, True):
            response = {"version": 1, "action": "status", "status": status,
                        "request": None, "reason": ""}
            completed = subprocess.CompletedProcess([], 0, json.dumps(response), "")
            with self.subTest(status=status), patch.object(membership.subprocess, "run", return_value=completed):
                with self.assertRaises(membership.ServerError):
                    membership.call_server("status", None, "pgrac-admin")

    def test_status_can_query_without_creating_request(self):
        response = {"version": 1, "action": "status", "status": "idle",
                    "request": None, "reason": ""}
        completed = subprocess.CompletedProcess([], 0, json.dumps(response), "")
        with patch.object(membership.subprocess, "run", return_value=completed):
            self.assertEqual(membership.call_server("status", None, "pgrac-admin"), response)
        with self.assertRaises(ValueError):
            membership.call_server("execute", None, "pgrac-admin")


if __name__ == "__main__":
    unittest.main()
