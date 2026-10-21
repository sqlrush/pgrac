"""Offline producer-field fixtures; no native identity or workload acceptance."""
import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import unittest

import test_workload_manifest as manifest_fixture

if importlib.util.find_spec("workload_evidence") is not None:
    import workload_evidence
else:
    workload_evidence = None


KEY = dict(system_identifier="18446744073709551615", origin_node_id="1",
           owner_instance="2", origin_boot_incarnation="3", cluster_epoch="4",
           formation_epoch="5", admission_record_generation="6",
           root_descriptor_incarnation="7", root_id="8", root_generation="9",
           segment_id="10", segment_generation="11", slot_offset="12",
           slot_wrap="13", canonical_xid="14")
TAG = dict(spc="1663", db="16384", rel="17000", fork="0", block="137")
TARGET = dict(TAG, page_lsn="15", page_scn="16", itl_slot="17", itl_wrap="18",
              itl_xid="19", offset="20", itemid_flags="1", uba="a1" * 16)


def line(family, fields, seq=1):
    domain = {} if family == "CTRC_RECEIPT" else dict(sequence_domain=family)
    ending = "sequence_exhausted" if family == "CTRC_CONSUME" else "trace_sequence_exhausted"
    envelope = dict(PGRAC_FAMILY=family, event="observed", event_seq=str(seq),
                    sequence_scope="process", **domain, node_id="0", pid="101",
                    procno="7", backend_id="1", process_incarnation_known="0",
                    authoritative="0")
    envelope.update(fields)
    envelope[ending] = "0"
    return ("LOG:  " + " ".join(key + "=" + value for key, value in envelope.items()) + "\n").encode()


def receipt_fields():
    fields = dict(KEY, canonical_key_known="1", receipt_known="1",
                  reservation_seq="21", attempt="22", target_ordinal="0",
                  target_known="1", **{key: value for key, value in TARGET.items()
                                        if key not in ("spc", "db", "rel")})
    fields.update(spc_oid=TAG["spc"], db_oid=TAG["db"], rel_number=TAG["rel"])
    return fields


def lifecycle_fields():
    return dict(KEY, **TARGET, key_known="1", receipt_known="1", reservation_seq="21",
                attempt="22", target_ordinal="0", target_known="1", handle_known="1",
                handle_valid="1", participant_index="23", receipt_index="24",
                journal_slot_generation="25", publication_known="1", wire_request_id="26",
                operation_id="27", publication_attempt="28", publication_grant="29",
                publication_backend="30", publication_requester="1", publication_boot="31",
                publication_capability_generation="32", descriptor_hash="33", member_ordinal="0",
                member_role="1", reference_kind="2", target_kind="3",
                publication_journal_sequence="34", publication_key_sequence="35",
                publication_slot_generation="36")


def consume_fields():
    fields = {key: value for key, value in KEY.items()
              if key not in ("slot_offset", "slot_wrap", "canonical_xid")}
    fields.update(key_known="1", tt_slot_offset="12", tt_wrap="13", raw_xid="14",
                  participant_index="23", receipt_index="24", journal_slot_generation="25",
                  consumer_sequence="37", attempt="22", target_ordinal="0", route_retarget="1",
                  data_target_known="1", data_offset="20", consumed_uba_raw="b2" * 16)
    fields.update({"final_" + key: TARGET[key] for key in
                   ("spc", "db", "rel", "fork", "block", "itl_slot", "itl_wrap",
                    "itl_xid", "page_lsn", "page_scn")})
    fields.update({"data_" + key: value for key, value in TAG.items()})
    return fields


class WorkloadEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.fixture = manifest_fixture.WorkloadManifestTests("runTest")
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)

    def read(self, raw):
        self.assertIsNotNone(workload_evidence, "producer-field adapter is missing")
        self.fixture.put("stderr0", raw)
        path = self.fixture.root / "manifest.json"
        path.write_text(json.dumps(self.fixture.manifest))
        return workload_evidence.read_evidence(path)

    def projection(self, family, fields):
        return self.read(line(family, fields))["nodes"][0]["producer_fields"][0]

    def test_request_tag_and_reply_remain_distinct(self):
        fields = dict(TAG, operation_request_id="11", request_id="12", request_epoch="13",
                      attempt="2", transition_id="3", reply_known="1", reply_request_id="14",
                      reply_epoch="15", reply_sender="2", forwarding_master="1",
                      reply_backend="4", reply_transition="5", status="6", page_lsn="17")
        projected = self.projection("READ_IMAGE", fields)
        self.assertEqual(projected["groups"]["request"]["values"]["request_id"], 12)
        self.assertEqual(projected["groups"]["reply"]["values"]["request_id"], 14)
        self.assertEqual(projected["groups"]["tag"]["values"],
                         dict(spcOid=1663, dbOid=16384, relNumber=17000, forkNum=0, blockNum=137))
        self.assertEqual(projected["groups"]["reply"]["source_fields"]["request_id"],
                         "reply_request_id")
        self.assertNotIn("predecessor_event_id", projected)

    def test_hop_tag_requires_its_own_known_flag(self):
        fields = dict(TAG, tag_known="0", forward_known="1", reply_known="0",
                      request_id="1", request_epoch="2", requester_node="3",
                      requester_backend="4", master_node="0")
        projected = self.projection("READ_IMAGE_HOP", fields)
        self.assertEqual(projected["groups"]["tag"]["status"], "UNKNOWN")
        self.assertEqual(projected["groups"]["tag"]["values"], {})
        self.assertEqual(projected["groups"]["tag"]["raw_values"]["block"], "137")
        fields["tag_known"] = "1"
        self.assertEqual(self.projection("READ_IMAGE_HOP", fields)
                         ["groups"]["tag"]["values"]["blockNum"], 137)

    def test_receipt_legacy_domain_and_actual_native_aliases(self):
        projected = self.projection("CTRC_RECEIPT", receipt_fields())
        self.assertIsNone(projected["sequence_domain"])
        self.assertEqual(projected["groups"]["transaction_key"]["values"],
                         {key: int(value) for key, value in KEY.items()})
        self.assertEqual(projected["groups"]["target"]["source_fields"]["spcOid"], "spc_oid")
        self.assertEqual(projected["groups"]["target"]["values"]["uba"], "a1" * 16)
        self.assertEqual(projected["groups"]["receipt"]["values"]["reservation_seq"], 21)
        self.assertNotIn("receipt_key", projected)

    def test_lifecycle_publication_and_handle_are_independent_groups(self):
        fields = lifecycle_fields()
        projected = self.projection("CTRC_RECEIPT_LIFECYCLE", fields)
        self.assertEqual(projected["groups"]["publication"]["values"]["wire_request_id"], 26)
        self.assertEqual(projected["groups"]["handle"]["values"],
                         dict(participant_index=23, receipt_index=24, journal_slot_generation=25))
        fields.update(publication_known="0", handle_valid="0")
        projected = self.projection("CTRC_RECEIPT_LIFECYCLE", fields)
        for group in ("publication", "handle"):
            self.assertEqual(projected["groups"][group]["status"], "UNKNOWN")
            self.assertEqual(projected["groups"][group]["values"], {})
        self.assertEqual(projected["groups"]["transaction_key"]["status"], "OBSERVED")

    def test_consume_keeps_original_target_separate_from_retarget_and_duplicate_sequence(self):
        first = consume_fields()
        second = dict(first, target_ordinal="1", data_block="8", final_block="9")
        raw = line("CTRC_CONSUME", first, 7) + line("CTRC_CONSUME", second, 7)
        result = self.read(raw)
        projected = result["nodes"][0]["producer_fields"]
        self.assertEqual([item["event_seq"] for item in projected], [7, 7])
        for item in projected:
            key = item["groups"]["transaction_key"]
            self.assertEqual(key["values"]["slot_offset"], 12)
            self.assertEqual(key["source_fields"]["slot_wrap"], "tt_wrap")
        self.assertEqual(projected[1]["groups"]["data_target"]["values"]["blockNum"], 8)
        self.assertEqual(projected[1]["groups"]["final_target"]["values"]["blockNum"], 9)
        self.assertNotIn("events", result)

    def test_unknown_gate_does_not_promote_nonzero_or_malformed_payload(self):
        fields = receipt_fields()
        fields.update(canonical_key_known="0", system_identifier="bad", target_known="0", block="-1")
        projected = self.projection("CTRC_RECEIPT", fields)
        for group in ("transaction_key", "target"):
            self.assertEqual(projected["groups"][group]["status"], "UNKNOWN")
            self.assertEqual(projected["groups"][group]["values"], {})
        self.assertEqual(projected["groups"]["transaction_key"]["raw_values"]
                         ["system_identifier"], "bad")

    def test_known_gate_requires_every_key_component_without_inventory_backfill(self):
        for absent in KEY:
            with self.subTest(absent=absent):
                fields = receipt_fields()
                del fields[absent]
                group = self.projection("CTRC_RECEIPT", fields)["groups"]["transaction_key"]
                self.assertEqual(group["status"], "UNKNOWN")
                self.assertEqual(group["values"], {})
                self.assertIn(absent, group["missing"])

    def test_numeric_hex_and_known_flag_errors_are_disclosed_without_coercion(self):
        bad_values = dict(system_identifier="18446744073709551616", cluster_epoch="4294967296",
                          canonical_xid="-1", slot_offset="1.0", slot_wrap="true", uba="a1/22",
                          target_known="2", canonical_key_known="yes", block="1e2")
        for field, value in bad_values.items():
            with self.subTest(field=field):
                fields = receipt_fields()
                fields[field] = value
                result = self.read(line("CTRC_RECEIPT", fields))
                projected = result["nodes"][0]["producer_fields"][0]
                self.assertEqual(result["projection_status"], "INVALID")
                self.assertTrue(any(field in group["invalid"] for group in projected["groups"].values()))
                self.assertEqual(result["nodes"][0]["trace"]["observations"][0]["fields"][field], value)

    def test_missing_gate_is_unknown_even_when_all_values_are_present(self):
        fields = receipt_fields()
        del fields["canonical_key_known"]
        group = self.projection("CTRC_RECEIPT", fields)["groups"]["transaction_key"]
        self.assertEqual(group["status"], "UNKNOWN")
        self.assertIn("canonical_key_known", group["missing"])
        self.assertEqual(group["values"], {})

    def test_provenance_points_to_exact_verified_manifest_file_and_raw_line(self):
        raw = b"unrelated\n" + line("CTRC_RECEIPT", receipt_fields())
        result = self.read(raw)
        projected = result["nodes"][0]["producer_fields"][0]
        source = projected["source"]
        observation = result["nodes"][0]["trace"]["observations"][0]
        self.assertEqual(source["manifest_sha256"], result["manifest"]["sha256"])
        self.assertEqual(source["stderr_file_id"], "stderr0")
        self.assertEqual(source["stderr_sha256"], hashlib.sha256(raw).hexdigest())
        self.assertEqual(source["observation_index"], 0)
        for key in ("line", "byte_offset", "byte_length", "sha256"):
            self.assertEqual(source[key], observation[key])
        self.assertEqual(source["byte_offset"], len(b"unrelated\n"))

    def test_unique_and_reused_pid_candidates_never_supply_incarnation_or_join_credit(self):
        entry = self.fixture.manifest["nodes"][0]
        inventory = self.fixture.inventory(entry)
        for reuse in (False, True):
            with self.subTest(reuse=reuse):
                if reuse:
                    other = dict(inventory["processes"][0], process_incarnation="inc-B", procno=8)
                    inventory["processes"].append(other)
                self.fixture.inventory(entry, inventory)
                result = self.read(line("CTRC_RECEIPT", receipt_fields()))
                self.assertFalse(result["identity_bound"])
                self.assertFalse(result["ready_to_run"])
                self.assertEqual(result["field_root"], "OPEN")
                self.assertNotIn("overall", result)
                projected = result["nodes"][0]["producer_fields"][0]
                self.assertFalse(projected["identity_bound"])
                self.assertNotIn("process_incarnation", projected)
                self.assertNotIn("chain_id", projected)
                self.assertEqual(result["nodes"][0]["trace"]["observations"][0]
                                 ["fields"]["process_incarnation_known"], "0")

    def test_unsupported_and_malformed_observations_keep_their_original_meaning(self):
        raw = line("HOT_FULL", dict(read_scn="9")) + line("E_OTHER", {})
        result = self.read(raw)
        self.assertEqual(result["nodes"][0]["producer_fields"][0]["status"], "UNSUPPORTED")
        self.assertEqual(result["projection_status"], "NOT_OBSERVED")
        self.assertEqual(len(result["nodes"][0]["trace"]["unhandled"]), 1)
        malformed = line("CTRC_RECEIPT", receipt_fields()).rstrip(b"\n")
        result = self.read(malformed)
        self.assertEqual(result["parse_status"], "REJECTED")
        self.assertEqual(result["nodes"][0]["producer_fields"], [])
        self.assertEqual(result["nodes"][0]["trace"]["rejections"][0]["raw_hex"], malformed.hex())

    def test_cli_requires_verified_files_and_keeps_readiness_false(self):
        self.assertIsNotNone(workload_evidence, "producer-field adapter is missing")
        self.read(line("CTRC_RECEIPT", receipt_fields()))
        command = [sys.executable, str(Path(workload_evidence.__file__)),
                   str(self.fixture.root / "manifest.json")]
        for changed in (False, True):
            with self.subTest(changed=changed):
                if changed:
                    (self.fixture.root / "stderr0.raw").write_bytes(b"changed\n")
                process = subprocess.run(command, capture_output=True, text=True)
                result = json.loads(process.stdout)
                self.assertEqual(process.returncode, 1 if changed else 0)
                self.assertEqual(result["file_binding"], "REJECTED" if changed else "VERIFIED")
                self.assertFalse(result["identity_bound"])
                self.assertFalse(result["ready_to_run"])


if __name__ == "__main__":
    unittest.main()
