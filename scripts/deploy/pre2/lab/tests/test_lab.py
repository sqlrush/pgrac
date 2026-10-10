#!/usr/bin/env python3
"""Tests for the PRE2 deployment fresh-database tools.

Author: SqlRush <sqlrush@gmail.com>

    python3 tests/test_lab.py                    # units; no database or guest
    PRE2_LAB_LIVE=/path/to/install PRE2_LAB_ROOT=/var/tmp/new-dir python3 tests/test_lab.py
        # also runs the controller end to end on this host in local-validation
        # mode: four native writers, then advance (expected BLOCKED)

The source contract tests read this repository's own source tree.
"""

import copy
import hashlib
import json
import os
import re
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

LAB = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(LAB))

import fresh_controller  # noqa: E402
import fresh_guest  # noqa: E402
import fresh_plan  # noqa: E402
from lab_common import BLOCKED, READY, PreflightError  # noqa: E402
import render  # noqa: E402
import request as lab_request  # noqa: E402
import source_contract  # noqa: E402

SOURCE = LAB.parents[3]


def node(n, root, uid=None, gid=None):
    # Synthetic inventory follows the public guide; offline tests never contact it.
    return {"node_id": n, "vm_uuid": "0000000%d-0000-4000-8000-000000000000" % n,
            "machine_id": ("%x" % (n + 1)) * 32, "boot_id": "1000000%d-0000-4000-8000-000000000000" % n,
            "admin_endpoint": {"host": "10.20.0.%d" % (10 + n), "port": 22, "user": "deploy",
                               "identity_file": "/var/lib/pgrac-admin/example_ed25519"},
            "ssh_host_key": "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIExample",
            "sql_addr": "10.20.0.%d:5432" % (10 + n),
            "control_addr": "10.20.0.%d:6540" % (10 + n),
            "data_base_addr": "10.20.0.%d:6541" % (10 + n), "data_workers": 2,
            "pgdata": "%s/node%d/data" % (root, n), "install_root": "%s/install" % root,
            "log_root": "%s/node%d/log" % (root, n),
            "uid": uid if uid is not None else 10001, "gid": gid if gid is not None else 10001}


def make_request(root="/srv/pgrac/eval01", direct_io="off", uid=None, gid=None):
    return {"schema_version": 1, "profile_id": "pre2-gfs2-arm64-lab-v1", "cluster_name": "pre2eval",
            "dataset_id": "pre2-example", "controller_addr": "10.20.0.20",
            "shared_mount": root + "/shared", "fs_uuid": "11111111-2222-4333-8444-555555555555",
            "storage_uuid": "0123456789abcdef0123456789abcdef", "database_incarnation": 1,
            "authority_uuid": "123456789abc4ef0923456789abcdef0", "system_identifier": "1234567890123456789",
            "config_dir": root + "/conf", "direct_io": direct_io,
            "storage_write_cache": {"mode": "write-through", "flush_verification": None},
            "memory_profile": "lab-8g", "guest_memory_gib": 8,
            "storage_quorum": {"cluster": "pre2eval", "nodes": {"0": 1, "1": 2, "2": 3, "3": 4}},
            "voting_wwids": ["36001405aaaaaaaaaaaaaaaaaaaaaaaaa"[:33], "36001405bbbbbbbbbbbbbbbbbbbbbbbbb"[:33],
                             "36001405ccccccccccccccccccccccccc"[:33]],
            "nodes": [node(n, root, uid, gid) for n in range(4)]}


CONTRACT = source_contract.collect(SOURCE)
# The per-node writer path, still used by releases without the cohort entry.
LEGACY = copy.deepcopy(CONTRACT)
LEGACY["initdb"]["cohort_option"] = False


class SourceContract(unittest.TestCase):
    def test_native_writer_entry_and_messages(self):
        initdb = CONTRACT["initdb"]
        self.assertTrue(initdb["native_writer"])
        self.assertTrue(initdb["root_not_published_message"])
        self.assertTrue(initdb["native_requires_checksums"])
        if not fresh_plan.cohort_mode(CONTRACT):
            self.assertEqual(initdb["founder_side_origin"], "origin_0")
        else:
            self.assertTrue(CONTRACT["cohort"]["owner"] and CONTRACT["cohort"]["root_last"])
        self.assertEqual(CONTRACT["pgrac_init"]["thread_dir_pattern"], "thread_{node_id_plus_1}")

    def test_policy_and_bootstrap_tables(self):
        policy = CONTRACT["config_policy"]
        self.assertEqual(policy["cluster.node_id"], ["COLD", "INSTANCE"])
        self.assertIn("UUID", policy["cluster.shared_storage_uuid"])
        for name in ("cluster.shared_config", "cluster.shared_catalog",
                     "cluster.controlfile_shared_authority", "cluster.wal_threads_dir"):
            self.assertIn(name, CONTRACT["bootstrap_required"])
        self.assertIn("hba_file", CONTRACT["string_gucs"])
        self.assertIn("debug_io_direct", CONTRACT["string_gucs"])

    def test_unrecognized_policy_flag_refused(self):
        with self.assertRaises(PreflightError):
            source_contract.parse_policy_table('config_policies[] = {\n { "x", POLICY_NEW } };')

    def test_catalog_requirement_read_from_fixture(self):
        self.assertEqual(CONTRACT["required_catalog_version"], 202609290)
        self.assertIsInstance(CONTRACT["catalog_version"], int)


class Request(unittest.TestCase):
    def test_valid_layout(self):
        request, derived = lab_request.validate(make_request())
        self.assertEqual(derived["shared"]["shared_data_dir"], "/srv/pgrac/eval01/shared/pre2-example/data")
        self.assertEqual([derived["nodes"][n]["wal_dir"].rsplit("/", 1)[1] for n in range(4)],
                         ["thread_1", "thread_2", "thread_3", "thread_4"])
        self.assertEqual(derived["storage_uuid_dashed"], "01234567-89ab-cdef-0123-456789abcdef")

    def test_write_cache_requires_flush_evidence(self):
        bad = make_request()
        bad["storage_write_cache"] = {"mode": "write-back", "flush_verification": None}
        with self.assertRaises(PreflightError) as caught:
            lab_request.validate(bad)
        self.assertEqual(caught.exception.reason, "WRITE_CACHE_FLUSH_UNVERIFIED")
        good = make_request()
        good["storage_write_cache"] = {"mode": "write-back",
                                       "flush_verification": {"path": "/var/lib/x.json", "sha256": "e" * 64}}
        request, _ = lab_request.validate(good)
        with self.assertRaises(PreflightError) as caught:
            fresh_controller.verify_flush_evidence(request)       # declared file is absent here
        self.assertEqual(caught.exception.reason, "WRITE_CACHE_FLUSH_EVIDENCE_MISSING")
        with tempfile.TemporaryDirectory() as d:
            evidence = Path(d) / "flush.json"
            evidence.write_text("{}")
            good["storage_write_cache"]["flush_verification"] = {
                "path": str(evidence), "sha256": "0" * 63 + "1"}
            request, _ = lab_request.validate(good)
            with self.assertRaises(PreflightError) as caught:
                fresh_controller.verify_flush_evidence(request)
            self.assertEqual(caught.exception.reason, "WRITE_CACHE_FLUSH_EVIDENCE_CHANGED")
            import hashlib
            good["storage_write_cache"]["flush_verification"]["sha256"] = hashlib.sha256(b"{}").hexdigest()
            request, _ = lab_request.validate(good)
            fresh_controller.verify_flush_evidence(request)
        bad = make_request()
        bad["storage_write_cache"] = {"mode": "write-through", "flush_verification": {"path": "/x", "sha256": "e" * 64}}
        with self.assertRaises(PreflightError):
            lab_request.validate(bad)

    def test_refusals(self):
        cases = [("profile_id", "pre1-gfs2-arm64-lab-v1"), ("storage_uuid", "0" * 32),
                 ("storage_uuid", "0123"), ("direct_io", "wal"), ("database_incarnation", 0),
                 ("controller_addr", "8.8.8.8"), ("shared_mount", "/home/x/shared")]
        for key, value in cases:
            bad = make_request()
            bad[key] = value
            with self.assertRaises(PreflightError, msg=key):
                lab_request.validate(bad)
        bad = make_request()
        bad["nodes"] = bad["nodes"][:3]
        with self.assertRaises(PreflightError):
            lab_request.validate(bad)
        bad = make_request()
        bad["nodes"][1]["sql_addr"] = bad["nodes"][0]["sql_addr"]
        with self.assertRaises(PreflightError):
            lab_request.validate(bad)


class Render(unittest.TestCase):
    def render(self, direct_io="off", contract=CONTRACT):
        request, derived = lab_request.validate(make_request(direct_io=direct_io))
        return request, derived, render.render(request, derived, contract)

    def test_shared_entries_pass_candidate_policy(self):
        _, _, rendered = self.render()
        self.assertEqual(rendered["policy_refusals"], [])
        scoped = {(e["name"], e["node_id"]) for e in rendered["shared_entries"]}
        self.assertIn(("cluster.shared_config", None), scoped)
        self.assertIn(("shared_buffers", 2), scoped)          # instance-only in the policy
        self.assertNotIn(("shared_buffers", None), scoped)
        self.assertIn(("cluster.node_id", 3), scoped)
        values = {(e["name"], e["node_id"]): e["value"] for e in rendered["shared_entries"]}
        self.assertEqual(values[("cluster.controlfile_shared_authority", None)], "on")
        self.assertEqual(values[("cluster.merged_recovery", None)], "on")
        for name in render.LOCAL_ONLY_STRINGS:
            self.assertNotIn((name, None), scoped)
        self.assertNotIn(("log_min_messages", None), scoped)
        self.assertEqual(values[("cluster.storage_quorum_nodes", None)], "0:1,1:2,2:3,3:4")
        self.assertEqual(values[("cluster.storage_quorum_cluster", None)], "pre2eval")

    def test_remote_visibility_in_canonical_common_request(self):
        for profile in render.MEMORY_PROFILES:
            req = make_request()
            req["memory_profile"] = profile
            request, derived = lab_request.validate(req)
            rendered = render.render(request, derived, CONTRACT)
            self.assertEqual(rendered["policy_refusals"], [])
            text = rendered["config_request"]["text"]
            for name in render.REMOTE_VISIBILITY:
                self.assertIn("\ncommon.%s='on'\n" % name, text)
                self.assertNotRegex(text, r"\nnode\d+\.%s=" % re.escape(name))
                for node_files in rendered["files"].values():
                    self.assertNotIn(name, node_files["pre2-bootstrap.conf"])

    def test_lab_8g_max_connections_matches_initdb(self):
        _, _, rendered = self.render()
        self.assertIn("\ncommon.max_connections='100'\n", rendered["config_request"]["text"])

    def test_hba_does_not_authorize_benchmark_accounts(self):
        request, _ = lab_request.validate(make_request())
        rules = [line.split() for line in render.hba_file(request).splitlines()
                 if line and not line.startswith("#")]
        self.assertEqual(rules, [["local", "all", "all", "peer"],
                                 ["host", "postgres", "pgrac", "10.20.0.20/32", "trust"]])

    def test_storage_quorum_request(self):
        for bad in ({"cluster": "pre2eval", "nodes": {"0": 1, "1": 1, "2": 3, "3": 4}},
                    {"cluster": "pre2eval", "nodes": {"0": 1, "1": 2, "2": 3}},
                    {"cluster": "pre2eval", "nodes": {"0": 0, "1": 2, "2": 3, "3": 4}},
                    {"cluster": "", "nodes": {"0": 1, "1": 2, "2": 3, "3": 4}}):
            req = make_request()
            req["storage_quorum"] = bad
            with self.assertRaises(PreflightError):
                lab_request.validate(req)

    def test_bootstrap_file(self):
        request, derived, rendered = self.render()
        text = rendered["files"][1]["pre2-bootstrap.conf"]
        for name in render.BOOTSTRAP_DISCOVERY:
            self.assertIn("\n%s = " % name, text)
        # Only discovery and local-only strings; never copies of other shared settings.
        names = [line.split(" = ")[0] for line in text.splitlines() if " = " in line]
        self.assertEqual(set(names) - set(render.BOOTSTRAP_DISCOVERY),
                         {"cluster.node_id", "hba_file", "cluster_name", "cluster.voting_disks"})
        self.assertIn("cluster.node_id = 1\n", text)
        self.assertIn("cluster.wal_threads_dir = '%s'" % derived["shared"]["wal_threads_dir"], text)
        self.assertNotIn("cluster.shared_storage_uuid", text)
        self.assertIn("hba_file = '%s'" % derived["nodes"][1]["hba_file"], text)

    def test_direct_io_follows_candidate_policy(self):
        absent = copy.deepcopy(CONTRACT)
        absent["config_policy"].pop("debug_io_direct", None)
        _, _, rendered = self.render("data", absent)
        self.assertEqual([r["name"] for r in rendered["policy_refusals"]], ["debug_io_direct"])
        present = copy.deepcopy(CONTRACT)
        present["config_policy"]["debug_io_direct"] = ["COLD", "COMMON", "STRING"]
        _, _, rendered = self.render("data", present)
        self.assertEqual(rendered["policy_refusals"], [])
        self.assertIn(("debug_io_direct", None, "data"),
                      {(e["name"], e["node_id"], e["value"]) for e in rendered["shared_entries"]})
        # This repository's own release decides which case applies.
        _, _, rendered = self.render("data")
        self.assertEqual(bool(rendered["policy_refusals"]), "debug_io_direct" not in CONTRACT["config_policy"])

    def test_check_entry_refusals(self):
        with self.assertRaises(PreflightError):
            render.check_entry(CONTRACT, "port", "5432", None)                 # instance-only
        with self.assertRaises(PreflightError):
            render.check_entry(CONTRACT, "work_mem", "4MB", 1)                 # common-only
        with self.assertRaises(PreflightError):
            render.check_entry(CONTRACT, "cluster.shared_data_dir", "rel/path", None)
        with self.assertRaises(PreflightError):
            render.check_entry(CONTRACT, "hba_file", "/x/hba.conf", None)      # unclassified string
        with self.assertRaises(PreflightError):
            render.check_entry(CONTRACT, "track_commit_timestamp", "on", None)


class Plan(unittest.TestCase):
    def plan(self, direct_io="off", contract=CONTRACT):
        request, derived = lab_request.validate(make_request(direct_io=direct_io))
        rendered = render.render(request, derived, contract)
        return fresh_plan.build(request, derived, contract, rendered, {n: "pgrac" for n in range(4)})

    def test_cohort_plan(self):
        if not fresh_plan.cohort_mode(CONTRACT):
            self.skipTest("release has no cohort entry")
        plan = self.plan()
        self.assertEqual(plan["creation"], "cohort")
        c = plan["cohort"]
        self.assertEqual(c["initdb_argv"][1:], ["-D", "/srv/pgrac/eval01/cohort", "-k", "-A", "trust", "--no-locale",
                                                "--pgrac-initdb-cohort",
                                                "--pgrac-initdb-shared-config=" + c["config_request_path"]])
        self.assertEqual([m["source"] for m in c["members"]],
                         ["/srv/pgrac/eval01/cohort/node_%d" % n for n in range(4)])
        self.assertEqual(c["shared_roots"]["wal"], "/srv/pgrac/eval01/shared/pre2-example/wal")
        blocked = [p["id"] for p in plan["wait_points"] if p["status"] == BLOCKED]
        self.assertEqual(blocked, [])
        self.assertEqual(plan["stages"]["open"]["status"], READY)

    def test_writers_ready_and_root_blocked(self):
        if fresh_plan.cohort_mode(CONTRACT):
            contract = copy.deepcopy(CONTRACT)
            contract["initdb"]["cohort_option"] = False
            plan = self.plan(contract=contract)
            self.assertEqual(plan["stages"]["native_writers"]["status"], READY)
            return
        plan = self.plan()
        self.assertEqual(plan["stages"]["native_writers"]["status"], READY)
        self.assertEqual(plan["stages"]["publish_root"],
                         {"status": BLOCKED, "wait_points": ["FRESH_ROOT_PUBLICATION"]})
        for stage in ("relmap", "cohort_side", "publish_config", "format", "open"):
            self.assertEqual(plan["stages"][stage]["status"], BLOCKED)
        ids = [p["id"] for p in plan["wait_points"] if p["status"] == BLOCKED]
        expected = ["FRESH_ROOT_PUBLICATION", "RELMAP_AUTHORITY_INIT", "COHORT_NATIVE_SIDE",
                    "SHARED_CONFIG_PUBLICATION", "FORMAT_ACTIVATION", "SHARED_OPEN_ENTRY"]
        if CONTRACT["initdb"].get("initial_config_option"):
            expected.remove("SHARED_CONFIG_PUBLICATION")      # the founder creates the object
        self.assertEqual(ids, expected)

    def test_direct_io_wait_point(self):
        absent = copy.deepcopy(LEGACY)
        absent["config_policy"].pop("debug_io_direct", None)
        absent["direct_io"] = {"sharedfs_consumes_io_direct_data": False, "policy": None}
        plan = self.plan("data", absent)
        point = next(p for p in plan["wait_points"] if p["id"] == "DIRECT_IO_DATA")
        self.assertEqual(point["status"], BLOCKED)
        self.assertIn("POLICY_REFUSAL", plan["stages"]["publish_config"]["wait_points"])
        present = copy.deepcopy(LEGACY)
        present["config_policy"]["debug_io_direct"] = ["COLD", "COMMON", "STRING"]
        present["direct_io"] = {"sharedfs_consumes_io_direct_data": True, "policy": ["COLD", "COMMON", "STRING"]}
        plan = self.plan("data", present)
        point = next(p for p in plan["wait_points"] if p["id"] == "DIRECT_IO_DATA")
        self.assertEqual(point["status"], READY)
        # Direct I/O alone never unblocks the stage: its own or earlier wait points still apply.
        waits = plan["stages"]["publish_config"]["wait_points"]
        self.assertNotIn("DIRECT_IO_DATA", waits)
        self.assertNotIn("POLICY_REFUSAL", waits)
        self.assertEqual(plan["stages"]["publish_config"]["status"], BLOCKED)

    def test_missing_entry_blocks_writers(self):
        contract = copy.deepcopy(LEGACY)
        contract["initdb"]["native_writer"] = False
        plan = self.plan(contract=contract)
        self.assertEqual(plan["stages"]["native_writers"]["status"], BLOCKED)
        self.assertEqual(plan["stages"]["publish_root"]["status"], BLOCKED)

    def test_initdb_argv(self):
        plan = self.plan(contract=LEGACY)
        founder, other = plan["nodes"][0]["initdb_argv"], plan["nodes"][2]["initdb_argv"]
        for argv in (founder, other):
            self.assertIn("-k", argv)
            self.assertNotIn("-N", argv)
            self.assertFalse(any(a.startswith(("-c", "--set")) for a in argv))
        self.assertIn("--pgrac-initdb-thread=1", founder)
        self.assertIn("--pgrac-initdb-storage-uuid=0123456789abcdef0123456789abcdef", founder)
        self.assertTrue(any(a.startswith("--pgrac-initdb-shared-base=") for a in founder))
        self.assertIn("--pgrac-initdb-thread=3", other)
        for argv in (founder, other):
            self.assertIn("--pgrac-initdb-system-identifier=1234567890123456789", argv)
        self.assertFalse(any(a.startswith(("--pgrac-initdb-shared-base", "--pgrac-initdb-shared-config"))
                             for a in other))
        if LEGACY["initdb"].get("initial_config_option"):
            path = plan["nodes"][0]["config_request_path"]
            self.assertIn("--pgrac-initdb-shared-config=" + path, founder)
            self.assertTrue(path.endswith(plan["config_request"]["sha256"][:16] + ".conf"))
            self.assertEqual(Path(path).parent, Path(plan["nodes"][0]["pgdata"]).parent)
        with self.assertRaises(ValueError):
            fresh_plan.initdb_argv("/i", "u", {"pgdata": "/a", "wal_dir": "/b", "thread": 2}, {})
        with self.assertRaises(ValueError):
            fresh_plan.initdb_argv("/i", "u", {"pgdata": "/a", "wal_dir": "/b", "thread": 2}, {}, "1", "/c")

    def test_refusal_blocks_writers_when_founder_creates_config(self):
        contract = copy.deepcopy(CONTRACT)
        contract["initdb"]["initial_config_option"] = True
        contract["config_policy"].pop("debug_io_direct", None)
        contract["direct_io"] = {"sharedfs_consumes_io_direct_data": False, "policy": None}
        plan = self.plan("data", contract)
        self.assertEqual(plan["stages"]["native_writers"], {"status": BLOCKED, "wait_points": ["POLICY_REFUSAL"]})
        self.assertIsNone(plan["config_request"])


class MemoryProfile(unittest.TestCase):
    def test_profile_values_and_check_inputs(self):
        request, derived = lab_request.validate(make_request())
        rendered = render.render(request, derived, CONTRACT)
        plan = fresh_plan.build(request, derived, CONTRACT, rendered, {n: "pgrac" for n in range(4)})
        settings = fresh_controller.memory_settings(plan)
        self.assertEqual(settings["shared_buffers"], "512MB")                 # instance entry of node 0
        self.assertEqual(settings["cluster.pcm_grd_max_entries"], "65536")
        self.assertEqual(settings["cluster.undo_buffers"], "16384")
        for name in fresh_controller.MEMORY_CHECK_EXCLUDE:
            self.assertNotIn(name, settings)
        pre1 = make_request()
        pre1["memory_profile"] = "pre1"
        request, derived = lab_request.validate(pre1)
        values = {e["name"]: e["value"] for e in render.render(request, derived, CONTRACT)["shared_entries"]
                  if e["node_id"] in (None, 0)}
        self.assertEqual((values["shared_buffers"], values["cluster.pcm_grd_max_entries"]), ("1GB", "131072"))
        bad = make_request()
        bad["memory_profile"] = "huge"
        with self.assertRaises(PreflightError):
            lab_request.validate(bad)


class Identity(unittest.TestCase):
    def test_new_identity_is_valid_request_input(self):
        ident = fresh_controller.new_identity(now_ns=1790000000123456000, pid=0x12345)
        self.assertEqual(int(ident["system_identifier"]) >> 32, 1790000000)
        self.assertEqual((int(ident["system_identifier"]) >> 12) & 0xFFFFF, 123456)
        self.assertEqual(int(ident["system_identifier"]) & 0xFFF, 0x345)
        request = make_request()
        request.update(ident)
        lab_request.validate(request)
        self.assertNotEqual(ident["storage_uuid"], ident["authority_uuid"])
        for _ in range(50):
            a = fresh_controller.new_identity()["authority_uuid"]
            self.assertEqual(a[12], "4")
            self.assertIn(a[16], "89ab")
        bad = make_request()
        bad["authority_uuid"] = "123456789abcdef0123456789abcdef0"      # not version 4
        with self.assertRaises(PreflightError):
            lab_request.validate(bad)


class ConfigRequest(unittest.TestCase):
    """Byte-for-byte equal to the request the release's own initdb test builds."""

    def test_matches_candidate_tap_request(self):
        temp, base = "/var/tmp/t", "/var/tmp/t/valid-shared"
        entries = {"cluster.controlfile_shared_authority": "on", "cluster.enabled": "on",
                   "cluster.merged_recovery": "on", "cluster.shared_catalog": "on", "cluster.shared_config": "on",
                   "cluster.shared_data_dir": base, "cluster.shared_storage_backend": "cluster_fs",
                   "cluster.shared_storage_uuid": "01234567-89ab-cdef-0123-456789abcdef",
                   "cluster.smgr_user_relations": "on", "cluster.undo_tablespace_path": temp + "/undo",
                   "cluster.wal_threads_dir": temp + "/threads"}
        expected = ("@authority_uuid=123456789abc4ef0923456789abcdef0\n"
                    "@configured_0=000000000000000f\n@configured_1=0000000000000000\n"
                    "@database_incarnation=1\n@format=1\n@generation=1\n"
                    "@storage_uuid=0123456789abcdef0123456789abcdef\n@system_identifier=1234567890123456789\n")
        for key in sorted(entries):
            expected += "common.%s='%s'\n" % (key, entries[key].replace("'", "''"))
        for n in range(4):
            expected += "node%03d.cluster.node_id='%d'\n" % (n, n)
        request = make_request()
        rows = [{"name": k, "value": v, "node_id": None} for k, v in reversed(list(entries.items()))]
        rows += [{"name": "cluster.node_id", "value": str(n), "node_id": n} for n in (3, 1, 0, 2)]
        self.assertEqual(render.config_request_bytes(request, rows), expected.encode())

    def test_quotes_and_refusals(self):
        request = make_request()
        data = render.config_request_bytes(request, [{"name": "log_line_prefix", "value": "%m 'x' ", "node_id": 1}])
        self.assertTrue(data.endswith(b"node001.log_line_prefix='%m ''x'' '\n"))
        with self.assertRaises(PreflightError):
            render.config_request_bytes(request, [{"name": "a", "value": "x\ny", "node_id": None}])
        with self.assertRaises(PreflightError):
            render.config_request_bytes(request, [{"name": "a", "value": "1", "node_id": None}] * 2)

    def test_rendered_request_is_canonical(self):
        request, derived = lab_request.validate(make_request())
        rendered = render.render(request, derived, CONTRACT)
        text = rendered["config_request"]["text"]
        keys = [line.split("=", 1)[0] for line in text.splitlines() if not line.startswith("@")]
        self.assertEqual(keys, sorted(keys, key=str.encode))
        self.assertEqual(len(keys), len(set(keys)))
        for name in CONTRACT["bootstrap_required"]:
            self.assertIn("common.%s=" % name, text)
        for n in range(4):
            self.assertIn("node%03d.cluster.node_id='%d'" % (n, n), text)


class GuestValidation(unittest.TestCase):
    def payload(self, n=0):
        request, derived = lab_request.validate(make_request())
        layout = derived["nodes"][n]
        return {"schema_version": 1, "action": "create-native-writer", "mode": "local-validation",
                "node": request["nodes"][n], "binary_sha256": "a" * 64,
                "shared_mount": request["shared_mount"], "fs_uuid": request["fs_uuid"],
                "dataset_root": derived["shared"]["dataset_root"],
                "wal_threads_dir": derived["shared"]["wal_threads_dir"],
                "pgdata": layout["pgdata"], "wal_dir": layout["wal_dir"], "thread": layout["thread"],
                "shared_base": derived["shared"]["shared_data_dir"] if n == 0 else None,
                "storage_uuid": request["storage_uuid"] if n == 0 else None,
                "database_incarnation": 1 if n == 0 else None,
                "system_identifier": "7123456789", "config_request": None, "config_path": None,
                "config_sha256": None}

    def test_valid_payloads(self):
        self.assertTrue(fresh_guest.validate(self.payload(0)))
        self.assertFalse(fresh_guest.validate(self.payload(3)))

    def test_refusals(self):
        bad = self.payload(1)
        bad["wal_dir"] = bad["wal_dir"].replace("thread_2", "thread_3")
        bad2 = self.payload(1)
        bad2["shared_base"] = "/srv/pgrac/eval01/shared/x"
        bad3 = self.payload(0)
        bad3["system_identifier"] = None
        bad4 = self.payload(2)
        bad4["thread"] = 2
        bad5 = self.payload(0)
        bad5["mode"] = "production"
        bad6 = self.payload(1)
        bad6.update(config_request="x", config_path=bad6["pgdata"] + ".conf",
                    config_sha256=hashlib.sha256(b"x").hexdigest())          # non-founder
        bad7 = self.payload(0)
        bad7.update(config_request="x", config_path="/var/lib/elsewhere/c.conf",
                    config_sha256=hashlib.sha256(b"x").hexdigest())          # not beside PGDATA
        for payload in (bad, bad2, bad3, bad4, bad5, bad6, bad7):
            with self.assertRaises(PreflightError):
                fresh_guest.validate(payload)
        result = fresh_guest.create(dict(self.payload(1), node=dict(self.payload(1)["node"],
                                                                    install_root="/srv/pgrac/eval01/none")))
        self.assertEqual(result["status"], "ERROR")


class Installer(unittest.TestCase):
    def test_operator_files_are_not_in_guest_archive(self):
        import install_guest_tools as ig
        import shutil
        with tempfile.TemporaryDirectory() as directory:
            parts = []
            for prefix, source in ig.PARTS:
                target = Path(directory) / prefix
                shutil.copytree(source, target, ignore=shutil.ignore_patterns('tests', '__pycache__'))
                parts.append((prefix, target))
            lab = Path(directory) / 'pre2/lab'
            for name in ('request.json', 'id_ed25519', 'private-notes.md'):
                (lab / name).write_text('operator fixture; not a real credential')
            with mock.patch.object(ig, 'PARTS', tuple(parts)):
                _, manifest = ig.build_archive()
            for name in ('request.json', 'id_ed25519', 'private-notes.md'):
                self.assertNotIn('pre2/lab/' + name, manifest)

    def test_archive_and_manifest(self):
        import install_guest_tools as ig
        import io as _io
        import tarfile
        archive, manifest = ig.build_archive()
        names = tarfile.open(fileobj=_io.BytesIO(archive)).getnames()
        self.assertIn("pre1/common.py", names)
        self.assertIn("pre2/lab/fresh_guest.py", names)
        self.assertEqual(names[-1], "SHA256SUMS")
        self.assertFalse(any("/tests/" in n or n.startswith("pre2/lab/tests") or "__pycache__" in n for n in names))
        self.assertEqual(archive, ig.build_archive()[0], "archive must be deterministic")
        self.assertEqual(set(manifest), set(names) - {"SHA256SUMS"})

    def test_archive_contains_only_required_pre1_helpers_and_runs_offline(self):
        import install_guest_tools as ig
        import io
        import tarfile
        archive, manifest = ig.build_archive()
        self.assertEqual({name for name in manifest if name.startswith("pre1/")},
                         {"pre1/common.py", "pre1/guest_status.py", "pre1/profile.schema.json"})
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with tarfile.open(fileobj=io.BytesIO(archive)) as tar:
                tar.extractall(root, filter="data")
            request = root / "request.json"
            request.write_text(json.dumps(make_request()))
            controller = [sys.executable, "-B", str(root / "pre2/lab/fresh_controller.py")]
            common = ["--request", str(request), "--source", str(SOURCE)]
            plan = root / "plan.json"
            commands = [controller + ["plan"] + common + ["--binary-sha256", "e" * 64, "--out", str(plan)],
                        controller + ["render"] + common + ["--plan", str(plan), "--out-dir", str(root / "rendered")]]
            for command in commands:
                result = subprocess.run(command, capture_output=True, text=True, timeout=30)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertEqual(json.loads(plan.read_text())["creation"], "cohort")
            for action in ("cohort", "distribute", "bootstrap-check"):
                result = subprocess.run(controller + [action, "--help"], capture_output=True, text=True, timeout=10)
                self.assertEqual(result.returncode, 0, result.stderr)

    def test_dest_and_dry_run(self):
        import install_guest_tools as ig
        for bad in ("/opt/pgrac-pre1-tools", "/opt/pgrac-pre2-lab-tools-x/../y", "/tmp/pgrac-pre2-lab-tools-a"):
            with self.assertRaises(PreflightError):
                ig.validate_dest(bad)
        request, _ = lab_request.validate(make_request())
        result = ig.install(request, ig.validate_dest("/opt/pgrac-pre2-lab-tools-example"), dry_run=True)
        self.assertEqual(result["state"], "DRY_RUN")
        command = result["nodes"][0]["argv"][-1]
        self.assertIn("DEST_EXISTS", command)
        self.assertIn("sha256sum --check", command)
        self.assertIn("mv -T", command)


@unittest.skipUnless(os.environ.get("PRE2_LAB_LIVE"), "live single-host validation not requested")
class LiveSingleHost(unittest.TestCase):
    """Real initdb native writers on this host; no server is started."""

    def test_controller_end_to_end(self):
        install = Path(os.environ["PRE2_LAB_LIVE"])
        root = Path(os.environ["PRE2_LAB_ROOT"])
        root.mkdir(mode=0o700)
        req = make_request(str(root), uid=os.getuid(), gid=os.getgid())
        for item in req["nodes"]:
            item["install_root"] = str(install)
            Path(item["pgdata"]).parent.mkdir(mode=0o700)
        Path(req["shared_mount"]).mkdir(mode=0o700)
        (root / "request.json").write_text(json.dumps(req))
        sha = subprocess.run(["sha256sum", str(install / "bin/postgres")], capture_output=True,
                             text=True, check=True).stdout.split()[0]
        controller = [sys.executable, str(LAB / "fresh_controller.py")]
        common = ["--request", str(root / "request.json"), "--source", str(SOURCE)]

        def run(*argv):
            completed = subprocess.run(controller + list(argv), capture_output=True, text=True, timeout=3600)
            print(completed.stdout.strip())
            return completed.returncode, json.loads(completed.stdout.strip().splitlines()[-1])

        rc, out = run("plan", *common, "--binary-sha256", sha, "--local-validation",
                      "--out", str(root / "plan.json"))
        self.assertEqual(rc, 0, out)
        rc, out = run("render", "--plan", str(root / "plan.json"), *common, "--out-dir", str(root / "rendered"))
        self.assertEqual(rc, 0, out)
        rc, out = run("writers", "--plan", str(root / "plan.json"), *common, "--transport", "local",
                      "--local-validation", "--out", str(root / "writers.json"))
        self.assertEqual(rc, 0, out)
        result = json.loads((root / "writers.json").read_text())
        self.assertEqual(result["state"], "NATIVE_WRITERS_CREATED")
        self.assertFalse(result["deployment_qualified"])
        ids = {r["system_identifier"] for r in result["nodes"]}
        self.assertEqual(len(ids), 1)
        for r in result["nodes"]:
            self.assertEqual(r["facts"]["control"]["checksum_version"], "1")
            self.assertEqual(r["facts"]["control"]["state"], "shut down")
        founder = result["nodes"][0]["facts"]
        plan = json.loads((root / "plan.json").read_text())
        if plan["config_request"] is not None and CONTRACT["initdb"].get("initial_config_option"):
            self.assertEqual(founder["initial_config_object"]["sha256"], plan["config_request"]["sha256"])
        self.assertEqual(ids, {req["system_identifier"]})
        self.assertTrue(founder["initdb_reports_root_unpublished"])
        self.assertFalse(founder["shared_base_root_present"])
        self.assertGreater(len(founder["founder_side"]), 0)
        rc, out = run("writers", "--plan", str(root / "plan.json"), *common, "--transport", "local",
                      "--local-validation", "--out", str(root / "writers-again.json"))
        self.assertEqual(rc, 1, "a second creation must refuse existing targets")
        again = json.loads((root / "writers-again.json").read_text())
        self.assertEqual(again["nodes"][0]["state"], "TARGET_NOT_EMPTY")
        rc, out = run("advance", "--plan", str(root / "plan.json"))
        self.assertEqual(rc, 3)
        self.assertEqual(out["first_blocked_stage"], "publish_root")


@unittest.skipUnless(os.environ.get("PRE2_LAB_COHORT_LIVE"), "live cohort validation not requested")
class LiveCohort(unittest.TestCase):
    """The release's cohort creator, exact distribution and read-only preparation on this host."""

    def test_cohort_distribute_bootstrap(self):
        install = Path(os.environ["PRE2_LAB_COHORT_LIVE"])
        root = Path(os.environ["PRE2_LAB_ROOT"])
        root.mkdir(mode=0o700)
        req = make_request(str(root), direct_io="data", uid=os.getuid(), gid=os.getgid())
        for item in req["nodes"]:
            item["install_root"] = str(install)
            Path(item["pgdata"]).parent.mkdir(mode=0o700, parents=True)
        Path(req["shared_mount"]).mkdir(mode=0o700)
        (root / "request.json").write_text(json.dumps(req))
        sha = subprocess.run(["sha256sum", str(install / "bin/postgres")], capture_output=True,
                             text=True, check=True).stdout.split()[0]
        controller = [sys.executable, str(LAB / "fresh_controller.py")]
        source = os.environ.get("PRE2_LAB_COHORT_SOURCE", str(SOURCE))
        common = ["--request", str(root / "request.json"), "--source", source]
        local = ["--transport", "local", "--local-validation"]

        def run(*argv):
            completed = subprocess.run(controller + list(argv), capture_output=True, text=True, timeout=3600)
            print(completed.stdout.strip()[-1500:], completed.stderr.strip()[-1500:])
            return completed.returncode, json.loads(completed.stdout.strip().splitlines()[-1])

        rc, out = run("plan", *common, "--binary-sha256", sha, "--local-validation", "--out", str(root / "plan.json"))
        self.assertEqual(rc, 0, out)
        plan = json.loads((root / "plan.json").read_text())
        self.assertEqual(plan["creation"], "cohort")
        rc, out = run("render", "--plan", str(root / "plan.json"), *common, "--out-dir", str(root / "rendered"))
        self.assertEqual(rc, 0, out)
        conf = Path(req["config_dir"])
        conf.mkdir(mode=0o700)
        for name in ("pre2-bootstrap.conf", "pgrac.conf", "pre2-hba.conf"):
            # One host plays every member; each member reads its own bootstrap file below.
            (conf / name).write_text((root / "rendered/node0" / name).read_text())
        rc, out = run("cohort", "--plan", str(root / "plan.json"), *common, *local, "--out", str(root / "cohort.json"))
        self.assertEqual(rc, 0, out)
        cohort_result = json.loads((root / "cohort.json").read_text())
        self.assertEqual(cohort_result["state"], "COHORT_CREATED")
        self.assertIsNotNone(cohort_result["facts"]["root"]["pgrac_control_root"])
        for member in range(4):
            facts = cohort_result["facts"]["members"][str(member)]
            self.assertEqual(facts["control"]["system_identifier"], req["system_identifier"])
            self.assertTrue(facts["side_archive"])
        rc, out = run("distribute", "--plan", str(root / "plan.json"), *common, *local,
                      "--out", str(root / "distribute.json"))
        self.assertEqual(rc, 0, out)
        dist = json.loads((root / "distribute.json").read_text())
        self.assertEqual([m["state"] for m in dist["members"]], ["PGDATA_INSTALLED"] * 4)
        for member in range(4):
            pgdata = Path(req["nodes"][member]["pgdata"])
            self.assertTrue(os.path.islink(pgdata / "pg_wal"))
            self.assertTrue((pgdata / "pgrac_initdb_native_side").is_dir())
            self.assertTrue((pgdata / "global/pgrac_cf_contract").is_file())
        images = list((Path(plan["layout"]["shared"]["shared_data_dir"]) / "global/config_images").glob("1-*.conf"))
        self.assertEqual(len(images), 1)
        image = images[0].read_text()
        for name in ("cluster.crossnode_runtime_visibility", "cluster.undo_gcs_coherence"):
            self.assertIn("common.%s='on'" % name, image)
            self.assertTrue((Path(plan["cohort"]["cache_root"]) / ("node_%d" % member)).is_dir())
        # Each member's own discovery file (node_id differs), as on the guests.
        checks = []
        for member in range(4):
            (conf / "pre2-bootstrap.conf").write_text((root / ("rendered/node%d" % member) / "pre2-bootstrap.conf").read_text())
            sub = dict(plan)
            sub["nodes"] = [n for n in plan["nodes"] if n["node_id"] == member]
            (root / ("plan-node%d.json" % member)).write_text(json.dumps(sub))
            import cohort_controller
            import fresh_controller
            args = fresh_controller.argparse.Namespace(local_validation=True, transport="local", tools_root="/x",
                                                       out=str(root / ("bootstrap-%d.json" % member)))
            sub["local_validation"] = True
            rc = cohort_controller.cmd_bootstrap_check(args, json.loads((root / "request.json").read_text()), sub)
            checks.append((rc, json.loads((root / ("bootstrap-%d.json" % member)).read_text())))
        for rc, result in checks:
            self.assertEqual(rc, 0, result)
            self.assertTrue(result["members"][0]["root_unchanged"])
            self.assertLessEqual(result["members"][0]["shared_memory_mb"], result["budget_mb"])
        rc, out = run("cohort", "--plan", str(root / "plan.json"), *common, *local, "--out", str(root / "again.json"))
        self.assertEqual(rc, 1, "a second creation must refuse existing targets")


if __name__ == "__main__":
    unittest.main(verbosity=2)
