"""Offline retained-file tests, not native workload or process attestation."""
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


if importlib.util.find_spec("workload_manifest") is not None:
    import workload_manifest
else:
    workload_manifest = None


class WorkloadManifestTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.manifest = dict(version=1, run_id="run-A", files={},
                             source=dict(commit="a" * 40, tree="b" * 40, file="source"),
                             harness="harness", nodes=[])
        self.put("source", b"retained source snapshot, not proof of ELF origin\n")
        self.put("harness", b"retained harness snapshot\n")
        for node in range(4):
            entry = dict(node_id=node, boot_id="boot-" + str(node), cluster_epoch="0",
                         postgres="postgres" + str(node), config="config" + str(node),
                         stderr="stderr" + str(node), inventory="inventory" + str(node))
            self.manifest["nodes"].append(entry)
            self.put(entry["postgres"], b"same frozen postgres bytes\n")
            self.put(entry["config"], ("node_id=" + str(node) + "\n").encode())
            self.put(entry["stderr"], self.log(node))
            inventory = dict(run_id="run-A", node_id=node, boot_id=entry["boot_id"],
                             cluster_epoch="0", source_commit="a" * 40, source_tree="b" * 40,
                             postgres_sha256=self.digest(b"same frozen postgres bytes\n"),
                             config_sha256=self.manifest["files"][entry["config"]]["sha256"],
                             harness_sha256=self.manifest["files"]["harness"]["sha256"],
                             processes=[dict(pid=101, procno=7, backend_start="start-A",
                                             process_incarnation="inc-A", role="backend")])
            self.inventory(entry, inventory)

    @staticmethod
    def digest(data):
        return hashlib.sha256(data).hexdigest()

    @staticmethod
    def log(node, sequence="1"):
        return ("LOG:  PGRAC_FAMILY=HOT_FULL event=observed event_seq=" + sequence +
                " sequence_scope=process sequence_domain=HOT_FULL node_id=" + str(node) +
                " pid=101 process_incarnation_known=0 authoritative=0 "
                "trace_sequence_exhausted=0\n").encode()

    def put(self, name, data):
        path = self.root / (name + ".raw")
        path.write_bytes(data)
        self.manifest["files"][name] = dict(path=path.name, sha256=self.digest(data),
                                             size_bytes=len(data))

    def inventory(self, entry, value=None):
        name = entry["inventory"]
        if value is None:
            return json.loads((self.root / self.manifest["files"][name]["path"]).read_text())
        self.put(name, json.dumps(value).encode())

    def read(self):
        self.assertIsNotNone(workload_manifest, "retained manifest reader is missing")
        path = self.root / "manifest.json"
        path.write_text(json.dumps(self.manifest))
        return workload_manifest.read_capture(path)

    def test_verified_file_bytes_remain_separate_from_identity_and_build_claims(self):
        result = self.read()
        self.assertEqual(result["file_binding"], "VERIFIED")
        self.assertEqual(len(result["files"]), 18)
        self.assertEqual(result["run_id"], "run-A")
        self.assertEqual(result["manifest"]["sha256"], self.digest(
            (self.root / "manifest.json").read_bytes()))
        self.assertEqual(result["parse_status"], "PARSED")
        self.assertEqual(result["build_binding"], "DECLARED_ONLY")
        self.assertFalse(result["identity_bound"])
        self.assertFalse(result["ready_to_run"])
        self.assertEqual(result["field_root"], "OPEN")
        self.assertNotIn("events", result)
        self.assertNotIn("overall", result)
        for node in result["nodes"]:
            self.assertEqual(node["process_candidates"], [dict(inventory_indices=[0])])
            self.assertFalse(node["trace"]["identity_bound"])
            self.assertEqual(node["trace"]["observations"][0]["fields"]
                             ["process_incarnation_known"], "0")

    def test_changed_missing_and_wrong_length_files_are_rejected(self):
        saved = copy.deepcopy(self.manifest)
        for kind in ("digest", "length", "missing"):
            with self.subTest(kind=kind):
                self.manifest = copy.deepcopy(saved)
                descriptor = self.manifest["files"]["harness"]
                if kind == "digest":
                    descriptor["sha256"] = "0" * 64
                elif kind == "length":
                    descriptor["size_bytes"] += 1
                else:
                    descriptor["path"] = "absent.raw"
                with self.assertRaises(ValueError):
                    self.read()

    def test_paths_cannot_escape_or_alias_another_role(self):
        saved = copy.deepcopy(self.manifest)
        target = self.root / "outside-link"
        target.symlink_to(self.root.parent)
        for path in ("../outside.raw", "/etc/hosts", "outside-link/outside.raw",
                     self.manifest["files"]["source"]["path"]):
            with self.subTest(path=path):
                self.manifest = copy.deepcopy(saved)
                self.manifest["files"]["harness"]["path"] = path
                with self.assertRaises(ValueError):
                    self.read()

    def test_all_four_nodes_and_all_referenced_files_are_required(self):
        saved = copy.deepcopy(self.manifest)
        changes = [lambda m: m["nodes"].pop(),
                   lambda m: m["nodes"].append(copy.deepcopy(m["nodes"][0])),
                   lambda m: m["nodes"][1].update(node_id=0),
                   lambda m: m["nodes"][0].update(config="absent"),
                   lambda m: m["nodes"][0].update(config="harness"),
                   lambda m: m["files"].update(unused=m["files"]["source"])]
        for change in changes:
            self.manifest = copy.deepcopy(saved)
            change(self.manifest)
            with self.subTest(manifest=self.manifest), self.assertRaises(ValueError):
                self.read()

    def test_manifest_and_inventory_capture_declarations_must_agree(self):
        entry = self.manifest["nodes"][0]
        original = self.inventory(entry)
        wrong = dict(run_id="run-B", node_id=1, boot_id="boot-other", cluster_epoch="1",
                     source_commit="c" * 40, source_tree="d" * 40,
                     postgres_sha256="0" * 64, config_sha256="1" * 64,
                     harness_sha256="2" * 64)
        for field, value in wrong.items():
            with self.subTest(field=field):
                inventory = copy.deepcopy(original)
                inventory[field] = value
                self.inventory(entry, inventory)
                with self.assertRaises(ValueError):
                    self.read()

    def test_different_node_binary_bytes_cannot_share_a_frozen_capture(self):
        entry = self.manifest["nodes"][3]
        self.put(entry["postgres"], b"different postgres bytes\n")
        inventory = self.inventory(entry)
        inventory["postgres_sha256"] = self.digest(b"different postgres bytes\n")
        self.inventory(entry, inventory)
        with self.assertRaises(ValueError):
            self.read()

    def test_individually_consistent_inventory_cannot_mix_cluster_epochs(self):
        entry = self.manifest["nodes"][2]
        entry["cluster_epoch"] = "1"
        inventory = self.inventory(entry)
        inventory["cluster_epoch"] = "1"
        self.inventory(entry, inventory)
        with self.assertRaises(ValueError):
            self.read()

    def test_inventory_pid_reuse_is_ambiguous_and_never_selects_an_incarnation(self):
        entry = self.manifest["nodes"][0]
        inventory = self.inventory(entry)
        second = dict(pid=101, procno=8, backend_start="start-B",
                      process_incarnation="inc-B", role="backend")
        inventory["processes"].append(second)
        self.inventory(entry, inventory)
        node = self.read()["nodes"][0]
        self.assertEqual(node["process_candidates"], [dict(inventory_indices=[0, 1])])
        fields = node["trace"]["observations"][0]["fields"]
        self.assertNotIn("backend_start", fields)
        self.assertNotIn("procno", fields)
        self.assertNotIn("process_incarnation", fields)

    def test_missing_inventory_process_stays_unbound(self):
        entry = self.manifest["nodes"][0]
        inventory = self.inventory(entry)
        inventory["processes"] = []
        self.inventory(entry, inventory)
        result = self.read()
        self.assertEqual(result["nodes"][0]["process_candidates"],
                         [dict(inventory_indices=[])])
        self.assertFalse(result["identity_bound"])
        self.assertFalse(result["ready_to_run"])

    def test_invalid_or_duplicate_full_inventory_identities_are_rejected(self):
        entry = self.manifest["nodes"][0]
        original = self.inventory(entry)
        for field, value in (("pid", True), ("pid", 0), ("procno", -1),
                             ("process_incarnation", ""), ("backend_start", None),
                             ("role", "")):
            with self.subTest(field=field, value=value):
                inventory = copy.deepcopy(original)
                inventory["processes"][0][field] = value
                self.inventory(entry, inventory)
                with self.assertRaises(ValueError):
                    self.read()
        original["processes"].append(copy.deepcopy(original["processes"][0]))
        self.inventory(entry, original)
        with self.assertRaises(ValueError):
            self.read()

    def test_wrong_log_node_cannot_borrow_file_identity(self):
        self.put("stderr0", self.log(1))
        with self.assertRaises(ValueError):
            self.read()

    def test_parser_rejections_empty_logs_and_repeated_sequences_keep_original_meaning(self):
        self.put("stderr0", self.log(0, "9") + self.log(0, "9"))
        self.put("stderr1", self.log(1).rstrip(b"\n"))
        self.put("stderr2", b"unrelated\n")
        result = self.read()
        self.assertEqual(result["parse_status"], "REJECTED")
        node = result["nodes"][0]
        self.assertEqual([o["fields"]["event_seq"] for o in
                          node["trace"]["observations"]], ["9", "9"])
        self.assertEqual(result["nodes"][1]["trace"]["rejections"][0]["raw_hex"],
                         self.log(1).rstrip(b"\n").hex())
        self.assertEqual(result["nodes"][2]["trace"]["parse_status"], "NOT_OBSERVED")
        self.assertFalse(result["identity_bound"])

    def test_invalid_schema_hash_types_and_json_ambiguities_are_rejected(self):
        saved = copy.deepcopy(self.manifest)
        changes = [lambda m: m.update(version=True), lambda m: m.update(run_id=""),
                   lambda m: m["source"].update(commit="short"),
                   lambda m: m["files"]["source"].update(size_bytes=True),
                   lambda m: m["files"]["source"].update(sha256="X" * 64),
                   lambda m: m["nodes"][0].update(node_id=False),
                   lambda m: m["nodes"][0].update(cluster_epoch=0)]
        for change in changes:
            self.manifest = copy.deepcopy(saved)
            change(self.manifest)
            with self.subTest(manifest=self.manifest), self.assertRaises(ValueError):
                self.read()
        self.assertIsNotNone(workload_manifest, "retained manifest reader is missing")
        for raw in (b'{"version":1,"version":1}', b'{"version":NaN}', b'[]'):
            path = self.root / "invalid.json"
            path.write_bytes(raw)
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                workload_manifest.read_capture(path)
        self.manifest = copy.deepcopy(saved)
        self.put("inventory0", b'{"run_id":"run-A","run_id":"run-A"}')
        with self.assertRaises(ValueError):
            self.read()

    def assignment(self, node=0, process=None, index=0):
        entry = self.manifest["nodes"][node]
        inventory = self.inventory(entry)
        process = inventory["processes"][0] if process is None else process
        data = (self.root / self.manifest["files"][entry["stderr"]]["path"]).read_bytes()
        lines = data.splitlines(keepends=True)
        return dict(source=dict(stderr_file_id=entry["stderr"],
                                stderr_sha256=self.digest(data), observation_index=index,
                                line=index + 1, byte_offset=sum(map(len, lines[:index])),
                                byte_length=len(lines[index]), sha256=self.digest(lines[index])),
                    identity=dict(node_id=node, boot_id=entry["boot_id"],
                                  sequence_domain="HOT_FULL", **process))

    def assign(self, claims, node=0):
        entry = self.manifest["nodes"][node]
        inventory = self.inventory(entry)
        inventory["trace_identity_assignments"] = claims
        self.inventory(entry, inventory)

    def test_absent_assignment_cannot_promote_even_a_unique_pid_candidate(self):
        result = self.read()
        for node in result["nodes"]:
            self.assertEqual(node["trace_identity_assignments"],
                             [dict(status="UNKNOWN", identity_bound=False)])
        self.assertFalse(result["identity_bound"])

    def test_exact_raw_assignment_preserves_declared_only_and_producer_unknown(self):
        claim = self.assignment()
        self.assign([claim])
        result = self.read()
        node = result["nodes"][0]
        assignment = node["trace_identity_assignments"][0]
        self.assertEqual(assignment["status"], "DECLARED_ONLY")
        self.assertEqual(assignment["identity"], claim["identity"])
        self.assertEqual(assignment["source"], claim["source"])
        self.assertEqual(assignment["inventory_file_id"], "inventory0")
        self.assertEqual(assignment["inventory_sha256"],
                         self.manifest["files"]["inventory0"]["sha256"])
        fields = node["trace"]["observations"][0]["fields"]
        self.assertEqual(fields["process_incarnation_known"], "0")
        self.assertNotIn("process_incarnation", fields)
        self.assertFalse(assignment["identity_bound"])
        self.assertFalse(result["identity_bound"])
        self.assertFalse(result["ready_to_run"])
        self.assertEqual(result["field_root"], "OPEN")
        self.assertNotIn("events", result)
        self.assertNotIn("overall", result)

    def test_assignment_rejects_each_wrong_raw_locator_including_boolean_indices(self):
        original = self.assignment()
        wrong = dict(stderr_file_id="stderr1", stderr_sha256="0" * 64,
                     observation_index=1, line=2, byte_offset=1,
                     byte_length=original["source"]["byte_length"] + 1, sha256="1" * 64)
        for field, value in list(wrong.items()) + [("observation_index", False),
                                                  ("byte_offset", False), ("line", True)]:
            with self.subTest(field=field, value=value):
                claim = copy.deepcopy(original)
                claim["source"][field] = value
                self.assign([claim])
                with self.assertRaises(ValueError):
                    self.read()

    def test_assignment_rejects_incomplete_or_unrecognized_raw_provenance(self):
        original = self.assignment()
        for field in original["source"]:
            with self.subTest(field=field):
                claim = copy.deepcopy(original)
                del claim["source"][field]
                self.assign([claim])
                with self.assertRaises(ValueError):
                    self.read()
        claim = copy.deepcopy(original)
        claim["source"]["timestamp"] = "nearby is not provenance"
        self.assign([claim])
        with self.assertRaises(ValueError):
            self.read()

    def test_assignment_rejects_wrong_birth_role_boot_pid_or_sequence_domain(self):
        original = self.assignment()
        wrong = dict(node_id=1, boot_id="boot-other", pid=102, procno=8,
                     backend_start="start-other", process_incarnation="inc-other",
                     role="worker", sequence_domain="READ_IMAGE")
        for field, value in wrong.items():
            with self.subTest(field=field):
                claim = copy.deepcopy(original)
                claim["identity"][field] = value
                self.assign([claim])
                with self.assertRaises(ValueError):
                    self.read()

    def test_assignment_requires_complete_typed_identity_instead_of_inventory_fill(self):
        original = self.assignment()
        for field in original["identity"]:
            for operation in ("missing", "invalid"):
                with self.subTest(field=field, operation=operation):
                    claim = copy.deepcopy(original)
                    if operation == "missing":
                        del claim["identity"][field]
                    else:
                        claim["identity"][field] = (False if field in
                                                    ("node_id", "pid", "procno") else "")
                    self.assign([claim])
                    with self.assertRaises(ValueError):
                        self.read()

    def test_duplicate_or_malformed_assignments_cannot_silently_choose_one(self):
        claim = self.assignment()
        for claims in (None, {}, [None], [dict(source=claim["source"])],
                       [dict(identity=claim["identity"])], [claim, claim]):
            with self.subTest(claims=claims):
                self.assign(claims)
                with self.assertRaises(ValueError):
                    self.read()

    def test_pid_reuse_requires_the_explicit_complete_birth_tuple(self):
        entry = self.manifest["nodes"][0]
        inventory = self.inventory(entry)
        second = dict(pid=101, procno=7, backend_start="start-B",
                      process_incarnation="inc-B", role="backend")
        inventory["processes"].append(second)
        self.inventory(entry, inventory)
        self.put("stderr0", self.log(0) + self.log(0, "2"))
        claim = self.assignment(process=second)
        self.assign([claim])
        node = self.read()["nodes"][0]
        self.assertEqual(node["process_candidates"],
                         [dict(inventory_indices=[0, 1]), dict(inventory_indices=[0, 1])])
        self.assertEqual(node["trace_identity_assignments"][0]["identity"], claim["identity"])
        self.assertEqual(node["trace_identity_assignments"][1],
                         dict(status="UNKNOWN", identity_bound=False))

    def test_duplicate_sequence_raw_lines_keep_separate_assignments_without_run_credit(self):
        self.put("stderr0", self.log(0, "9") * 2)
        self.assign([self.assignment(index=1), self.assignment(index=0)])
        node = self.read()["nodes"][0]
        self.assertEqual([item["source"]["line"] for item in
                          node["trace_identity_assignments"]], [1, 2])
        self.assertEqual([item["fields"]["event_seq"] for item in
                          node["trace"]["observations"]], ["9", "9"])
        self.assertTrue(all(item["status"] == "DECLARED_ONLY" for item in
                            node["trace_identity_assignments"]))
        self.assertFalse(self.read()["ready_to_run"])

    def test_missing_raw_domain_cannot_be_invented_by_an_assignment(self):
        raw = (b"LOG: PGRAC_FAMILY=CTRC_RECEIPT event=observed event_seq=1 "
               b"sequence_scope=process node_id=0 pid=101 authoritative=0 "
               b"trace_sequence_exhausted=0\n")
        self.put("stderr0", raw)
        claim = self.assignment()
        claim["identity"]["sequence_domain"] = "CTRC_RECEIPT"
        self.assign([claim])
        with self.assertRaises(ValueError):
            self.read()

    def test_existing_raw_procno_cannot_contradict_a_declared_process(self):
        self.put("stderr0", self.log(0).replace(b"pid=101 ", b"pid=101 procno=8 "))
        self.assign([self.assignment()])
        with self.assertRaises(ValueError):
            self.read()

    def test_known_raw_birth_fields_must_agree_but_unknown_placeholders_stay_raw(self):
        for key, gate, value in (("backend_start", "backend_start_known", "start-A"),
                                 ("process_incarnation", "process_incarnation_known", "inc-A"),
                                 ("boot_id", "boot_known", "boot-0")):
            for known, raw_value in (("1", "other"), ("1", value), ("0", "unknown")):
                with self.subTest(key=key, known=known, raw_value=raw_value):
                    raw = self.log(0).replace(b"process_incarnation_known=0 ", b"")
                    raw = raw.replace(b"authoritative=0 ",
                                      (key + "=" + raw_value + " " + gate + "=" + known +
                                       " authoritative=0 ").encode())
                    self.put("stderr0", raw)
                    self.assign([self.assignment()])
                    if known == "1" and raw_value == "other":
                        with self.assertRaises(ValueError):
                            self.read()
                    else:
                        node = self.read()["nodes"][0]
                        self.assertEqual(node["trace"]["observations"][0]["fields"][key], raw_value)
                        self.assertEqual(node["trace_identity_assignments"][0]["status"],
                                         "DECLARED_ONLY")
                        self.assertFalse(node["trace_identity_assignments"][0]["identity_bound"])

    def test_assignment_uses_observation_index_and_exact_byte_position_not_line_index(self):
        raw = self.log(0)
        prefix = b"unrelated UTF-8: \xc3\xa9\n"
        self.put("stderr0", prefix + raw)
        claim = self.assignment()
        claim["source"].update(line=2, byte_offset=len(prefix), byte_length=len(raw),
                               sha256=self.digest(raw))
        self.assign([claim])
        node = self.read()["nodes"][0]
        self.assertEqual(node["trace_identity_assignments"][0]["source"], claim["source"])
        self.assertEqual(node["trace"]["ignored_lines"], 1)

    def test_assignment_cannot_reference_unhandled_or_rejected_raw_lines(self):
        for raw in (b"LOG: PGRAC_FAMILY=FUTURE event=x\n", self.log(0).rstrip(b"\n")):
            with self.subTest(raw=raw):
                self.put("stderr0", raw)
                self.assign([self.assignment()])
                with self.assertRaises(ValueError):
                    self.read()

    def test_cli_verifies_without_modifying_files_or_granting_run_credit(self):
        self.assign([self.assignment()])
        self.read()
        path = self.root / "manifest.json"
        saved = {p: p.read_bytes() for p in self.root.iterdir()}
        run = subprocess.run([sys.executable, workload_manifest.__file__, str(path)],
                             capture_output=True, text=True, check=False)
        self.assertEqual(run.returncode, 0, run.stderr)
        result = json.loads(run.stdout)
        self.assertEqual(result["file_binding"], "VERIFIED")
        self.assertEqual(result["nodes"][0]["trace_identity_assignments"][0]["status"],
                         "DECLARED_ONLY")
        self.assertFalse(result["ready_to_run"])
        self.assertEqual({p: p.read_bytes() for p in self.root.iterdir()}, saved)
        claim = self.assignment()
        claim["identity"]["backend_start"] = "not-in-inventory"
        self.assign([claim])
        path.write_text(json.dumps(self.manifest))
        run = subprocess.run([sys.executable, workload_manifest.__file__, str(path)],
                             capture_output=True, text=True, check=False)
        self.assertEqual(run.returncode, 1, run.stderr)
        self.assertEqual(json.loads(run.stdout)["file_binding"], "REJECTED")
        self.assign([self.assignment()])
        self.read()
        (self.root / "harness.raw").write_bytes(b"changed\n")
        run = subprocess.run([sys.executable, workload_manifest.__file__, str(path)],
                             capture_output=True, text=True, check=False)
        self.assertEqual(run.returncode, 1, run.stderr)
        rejected = json.loads(run.stdout)
        self.assertEqual(rejected["file_binding"], "REJECTED")
        self.assertFalse(rejected["ready_to_run"])
        self.assertNotIn("overall", rejected)


if __name__ == "__main__":
    unittest.main()
