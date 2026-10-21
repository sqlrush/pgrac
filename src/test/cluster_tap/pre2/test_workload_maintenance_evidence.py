"""Offline prune/index-delete fixtures; local observations, no workload credit."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import unittest

import test_workload_evidence as fixture
import workload_evidence
from workload_log import read_trace


HEAP_ITEM = ("item_known=1 lp=1 tuple_known=1 off=7900 len=80 xmin=14 xmax=15 "
             "ctid=137/3 mask=256/512 raw_cid=4 combo=1 itl=2 slot_known=1 "
             "slot_xid=16 itl_wrap=17 flags=18 write_scn=19 commit_scn=20 "
             "first_change_lsn=21 uba_raw=22/23 full_xid_known=0 tt_wrap_known=0")
INDEX_ITEM = ("item_known=1 lp=1 tuple_known=1 storage_off=7800 storage_len=64 "
              "tuple_size=64 posting=1 posting_shape_known=1 posting_count=3 pivot=0 "
              "heap_tid_known=0 heap_tid=0/0 raw_index_tid=16777216/8192")


def line(family, event, fields, seq=1):
    envelope = dict(PGRAC_FAMILY=family, event=event, event_seq=str(seq),
                    node="0", pid="101")
    envelope.update(fields)
    envelope.update(authoritative="0", trace_sequence_exhausted="0")
    return ("LOG:  " + " ".join(key + "=" + value for key, value in envelope.items()) + "\n").encode()


def prune_fields():
    return dict(operation_seq="9", read_scn="32", epoch="33", relid="17000",
                requested_locator="1663/16384/17000", actual_tag_known="1",
                actual_tag="1663/16384/18000/-1/137", admission_feature="34",
                admission_generation="35", admission_epoch="36", admission_side="1",
                space_known="1", space_system="37", database_incarnation="38",
                space_incarnation="ab" * 16, space_sequence="39", space_operation="40",
                space_state="2", before_known="1", after_known="1", before_shape="1",
                after_shape="1", before_lsn="41", before_scn="42", after_lsn="43",
                after_scn="44", floor_known="1", floor_scn="45", ndeleted="2",
                nnewlpdead="1", changed_items="3", pcm_generation="46",
                reservation_token="47", writer_activation="48", resource_x_activation="49",
                pcm_state="2", pcm_flags="3", pin_scope="caller_retained",
                subpredicate_known="0", wal_flush_known="0", boot_known="0",
                process_incarnation_known="0", image_hash_known="0")


def prune_item_fields():
    return dict(operation_seq="9", offset="3", before="{" + HEAP_ITEM + "}",
                after="{" + HEAP_ITEM.replace("ctid=137/3", "ctid=138/4") + "}")


def index_fields():
    return dict(operation_seq="10", relid="17001", requested_locator="1663/16384/17001",
                actual_tag_known="1", actual_tag="1663/16384/18001/0/138", space_known="1",
                space_system="37", database_incarnation="38", storage_uuid="cd" * 16,
                space_incarnation="ab" * 16, space_sequence="39", space_operation="40",
                space_state="2", before_shape="1", after_shape="1", before_maxoff="20",
                after_maxoff="18", before_lsn="41", after_lsn="43", before_scn="42",
                after_scn="44", conflict_horizon_known="1", conflict_horizon="50",
                ndeleted="2", nupdated="1", captured="3", omitted="4", wal_required="1",
                inserted_lsn_known="1", inserted_lsn="51", caller_index_lock_retained="1",
                caller_cleanup_known="0", new_offset_mapping_known="0", wal_flush_known="0",
                boot_known="0", process_incarnation_known="0")


def index_item_fields():
    return dict(operation_seq="10", item_ordinal="0", item_events_expected="3",
                old_index_offset="5", change="posting_update", before="{" + INDEX_ITEM + "}",
                removed_count="4", positions_captured="3", positions_omitted="1",
                removed="{2:137/3,2:138/4,9:unknown}", new_offset_mapping_known="0")


class WorkloadMaintenanceEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.capture = fixture.WorkloadEvidenceTests("runTest")
        self.capture.setUp()
        self.addCleanup(self.capture.doCleanups)

    def read(self, raw):
        return self.capture.read(raw)

    def projection(self, family, event, fields):
        return self.read(line(family, event, fields))["nodes"][0]["producer_fields"][0]

    def test_legacy_envelope_retains_absent_metadata_and_original_bytes(self):
        for family, event in (("R4_HEAP_PRUNE", "prune.result"),
                              ("R4_BTREE_DELETE", "delete.applied")):
            with self.subTest(family=family):
                raw = b"unrelated\n" + line(family, event, {})
                result = read_trace(raw)
                self.assertEqual(result["parse_status"], "PARSED")
                observation = result["observations"][0]
                self.assertEqual(observation["fields"]["node"], "0")
                self.assertEqual(observation["byte_offset"], len(b"unrelated\n"))
                self.assertEqual(observation["raw"].encode(), raw[observation["byte_offset"]:])
                for key in ("node_id", "sequence_scope", "sequence_domain"):
                    self.assertNotIn(key, observation["fields"])

    def test_legacy_envelope_rejects_malformed_or_invented_identity(self):
        valid = line("R4_HEAP_PRUNE", "prune.item", {})
        for old, new in ((b"event_seq=1", b"event_seq=0"),
                         (b"event_seq=1", b"event_seq=18446744073709551616"),
                         (b"node=0", b"node_id=0"), (b"node=0", b"node=-1"),
                         (b"pid=101", b"pid=0"), (b"authoritative=0", b"authoritative=1"),
                         (b"node=0", b"node=0 node_id=0"),
                         (b"node=0", b"node=0 sequence_scope=process"),
                         (b"node=0", b"node=0 sequence_domain=R4_HEAP_PRUNE"),
                         (b"trace_sequence_exhausted=0", b"trace_sequence_exhausted=0 extra=1")):
            with self.subTest(new=new):
                result = read_trace(valid.replace(old, new))
                self.assertEqual(result["parse_status"], "REJECTED")
                self.assertEqual(bytes.fromhex(result["rejections"][0]["raw_hex"]),
                                 valid.replace(old, new))

    def test_modern_family_still_requires_its_original_envelope(self):
        raw = fixture.line("HOT_FULL", {})
        self.assertEqual(read_trace(raw)["parse_status"], "PARSED")
        for old, new in ((b"node_id=0", b"node=0"),
                         (b"sequence_scope=process ", b""),
                         (b"sequence_domain=HOT_FULL ", b"")):
            self.assertEqual(read_trace(raw.replace(old, new))["parse_status"], "REJECTED")

    def test_exact_legacy_summary_adjacency_is_parsed_without_rewriting_message(self):
        for event in ("delete.applied", "vacuum.applied"):
            raw = line("R4_BTREE_DELETE", event, index_fields()).replace(
                b"after_scn=44 conflict_horizon_known=1", b"after_scn=44conflict_horizon_known=1")
            result = read_trace(raw)
            self.assertEqual(result["parse_status"], "PARSED")
            observation = result["observations"][0]
            self.assertEqual(observation["fields"]["after_scn"], "44")
            self.assertEqual(observation["fields"]["conflict_horizon_known"], "1")
            self.assertEqual(observation["raw"].encode(), raw)
            self.assertIn("after_scn=44conflict_horizon_known=1", observation["message"])

    def test_other_missing_scalar_separators_are_rejected(self):
        summary = line("R4_BTREE_DELETE", "delete.applied", index_fields())
        adjacent = summary.replace(b"after_scn=44 conflict_horizon_known=1",
                                   b"after_scn=44conflict_horizon_known=1")
        for raw in (adjacent.replace(b"delete.applied", b"future.event"),
                    adjacent.replace(b"delete.applied", b"index.item"),
                    adjacent.replace(b"R4_BTREE_DELETE", b"R4_HEAP_PRUNE"),
                    summary.replace(b"before_lsn=41 after_lsn=43", b"before_lsn=41after_lsn=43"),
                    adjacent.replace(b"after_scn=44", b"after_scn=bad")):
            self.assertEqual(read_trace(raw)["parse_status"], "REJECTED")

    def test_manifest_checks_existing_node_without_binding_pid_candidate(self):
        result = self.read(line("R4_HEAP_PRUNE", "prune.item", prune_item_fields()))
        self.assertEqual(result["file_binding"], "VERIFIED")
        self.assertEqual(result["nodes"][0]["process_candidates"][0]["inventory_indices"], [0])
        self.assertEqual(len(result["nodes"][0]["producer_fields"]), 1)
        self.assertFalse(result["identity_bound"])
        self.assertFalse(result["ready_to_run"])

    def test_manifest_rejects_legacy_node_contradicting_file_role(self):
        raw = line("R4_BTREE_DELETE", "index.item", index_item_fields()).replace(b"node=0", b"node=1")
        with self.assertRaisesRegex(ValueError, "LOG node contradicts file role"):
            self.read(raw)

    def test_prune_summary_keeps_requested_locator_and_actual_pages_separate(self):
        projected = self.projection("R4_HEAP_PRUNE", "prune.result", prune_fields())
        self.assertEqual(projected["status"], "PROJECTED")
        groups = projected["groups"]
        self.assertEqual(groups["requested"]["values"]["locator"]["relNumber"], 17000)
        self.assertEqual(groups["actual_tag"]["values"]["tag"]["relNumber"], 18000)
        self.assertEqual(groups["actual_tag"]["values"]["tag"]["forkNum"], -1)
        self.assertEqual(groups["before_page"]["values"]["page_lsn"], 41)
        self.assertEqual(groups["after_page"]["values"]["page_lsn"], 43)
        self.assertEqual(groups["operation"]["values"]["read_scn"], 32)
        self.assertEqual(groups["counts"]["values"]["changed_items"], 3)

    def test_prune_false_page_or_shape_flags_do_not_expose_placeholder_values(self):
        for guard in ("before_known", "before_shape"):
            with self.subTest(guard=guard):
                fields = prune_fields()
                fields.update({guard: "0", "before_lsn": "not-a-number"})
                groups = self.projection("R4_HEAP_PRUNE", "prune.result", fields)["groups"]
                self.assertEqual(groups["before_page"]["status"], "UNKNOWN")
                self.assertEqual(groups["before_page"]["values"], {})
                self.assertEqual(groups["after_page"]["status"], "OBSERVED")

    def test_prune_item_uses_its_own_known_flags_and_itl_wrap_spelling(self):
        projected = self.projection("R4_HEAP_PRUNE", "prune.item", prune_item_fields())
        self.assertEqual(projected["status"], "PROJECTED")
        groups = projected["groups"]
        self.assertEqual(groups["before_slot"]["values"]["wrap"], 17)
        self.assertEqual(groups["before_slot"]["source_fields"]["wrap"], "before.itl_wrap")
        self.assertEqual(groups["before_tuple"]["values"]["ctid"]["blockNum"], 137)
        self.assertEqual(groups["after_tuple"]["values"]["ctid"]["blockNum"], 138)
        self.assertNotIn("tt_wrap", groups["before_slot"]["values"])
        self.assertNotIn("before_known", groups["before_slot"]["guards"])

    def test_prune_redirect_retains_storage_without_tuple_or_slot_credit(self):
        fields = prune_item_fields()
        fields["before"] = "{item_known=1 lp=2 redirect=4 off=4 len=0 tuple_known=0}"
        groups = self.projection("R4_HEAP_PRUNE", "prune.item", fields)["groups"]
        self.assertEqual(groups["before_redirect"]["values"]["redirect"], 4)
        self.assertEqual(groups["before_storage"]["values"], dict(off=4, len=0))
        for name in ("before_tuple", "before_slot"):
            self.assertEqual(groups[name]["status"], "UNKNOWN")
            self.assertEqual(groups[name]["values"], {})

    def test_missing_or_malformed_item_never_borrows_from_other_image_or_line(self):
        fields = prune_item_fields()
        fields["before"] = "{" + HEAP_ITEM.replace("ctid=137/3 ", "") + "}"
        raw = line("R4_HEAP_PRUNE", "prune.result", prune_fields())
        raw += line("R4_HEAP_PRUNE", "prune.item", fields, 2)
        groups = self.read(raw)["nodes"][0]["producer_fields"][1]["groups"]
        self.assertEqual(groups["before_tuple"]["status"], "UNKNOWN")
        self.assertEqual(groups["before_tuple"]["values"], {})
        self.assertEqual(groups["after_tuple"]["status"], "OBSERVED")
        fields["before"] = "{broken}"
        groups = self.projection("R4_HEAP_PRUNE", "prune.item", fields)["groups"]
        self.assertEqual(groups["before_item"]["status"], "INVALID")
        self.assertEqual(groups["before_item"]["raw_values"]["before"], "{broken}")

    def test_index_summary_events_keep_bounded_counts_and_inserted_wal_local(self):
        for event in ("delete.applied", "vacuum.applied"):
            with self.subTest(event=event):
                result = self.read(line("R4_BTREE_DELETE", event, index_fields()))
                projected = result["nodes"][0]["producer_fields"][0]
                self.assertEqual(projected["status"], "PROJECTED")
                self.assertEqual(projected["groups"]["counts"]["values"]["omitted"], 4)
                self.assertEqual(projected["groups"]["horizon"]["values"]["conflict_horizon"], 50)
                self.assertEqual(projected["groups"]["wal"]["values"]["inserted_lsn"], 51)
                self.assertFalse(result["ready_to_run"])
                self.assertEqual(result["nodes"][0]["trace"]["observations"][0]["fields"]["wal_flush_known"], "0")

    def test_posting_raw_index_tid_does_not_supply_a_heap_tid(self):
        projected = self.projection("R4_BTREE_DELETE", "index.item", index_item_fields())
        groups = projected["groups"]
        self.assertEqual(groups["before_tuple"]["values"]["raw_index_tid"],
                         dict(blockNum=16777216, offsetNumber=8192))
        self.assertEqual(groups["before_heap_tid"]["status"], "UNKNOWN")
        self.assertEqual(groups["before_heap_tid"]["values"], {})
        self.assertEqual(groups["before_posting"]["values"]["posting_count"], 3)
        fields = index_item_fields()
        fields["before"] = "{" + INDEX_ITEM.replace("posting=1", "posting=0").replace(
            "heap_tid_known=0 heap_tid=0/0", "heap_tid_known=1 heap_tid=137/3") + "}"
        groups = self.projection("R4_BTREE_DELETE", "index.item", fields)["groups"]
        self.assertEqual(groups["before_heap_tid"]["values"]["heap_tid"]["blockNum"], 137)
        self.assertEqual(groups["before_posting"]["status"], "UNKNOWN")

    def test_posting_positions_retain_order_duplicates_unknown_and_empty_list(self):
        groups = self.projection("R4_BTREE_DELETE", "index.item", index_item_fields())["groups"]
        positions = groups["removed_positions"]["values"]["positions"]
        self.assertEqual([entry["position"] for entry in positions], [2, 2, 9])
        self.assertEqual([entry["heap_tid_known"] for entry in positions], [True, True, False])
        self.assertEqual(positions[1]["heap_tid"], dict(blockNum=138, offsetNumber=4))
        self.assertIsNone(positions[2]["heap_tid"])
        fields = index_item_fields()
        fields.update(positions_captured="0", removed="{}")
        empty = self.projection("R4_BTREE_DELETE", "index.item", fields)["groups"]["removed_positions"]
        self.assertEqual(empty["status"], "OBSERVED")
        self.assertEqual(empty["values"]["positions"], [])

    def test_inactive_posting_shape_keeps_removed_text_unknown(self):
        fields = index_item_fields()
        fields.update(before="{" + INDEX_ITEM.replace("posting_shape_known=1", "posting_shape_known=0") + "}",
                      removed="{unknown}")
        groups = self.projection("R4_BTREE_DELETE", "index.item", fields)["groups"]
        for name in ("before_posting", "removed_positions"):
            self.assertEqual(groups[name]["status"], "UNKNOWN")
            self.assertEqual(groups[name]["values"], {})
        self.assertEqual(groups["removed_positions"]["raw_values"]["removed"], "{unknown}")

    def test_active_format_ranges_and_known_flags_are_checked_with_raw_values(self):
        for key, value in (("offset", "4294967296"),
                           ("before", "{" + HEAP_ITEM.replace("slot_known=1", "slot_known=2") + "}"),
                           ("before", "{" + HEAP_ITEM.replace("uba_raw=22/23", "uba_raw=22/23/24") + "}")):
            with self.subTest(key=key, value=value):
                fields = prune_item_fields()
                fields[key] = value
                self.assertEqual(self.projection("R4_HEAP_PRUNE", "prune.item", fields)["status"], "INVALID")
        for removed in ("{x:137/3}", "{2:137/4294967296}", "{2:137/3,}"):
            fields = index_item_fields()
            fields["removed"] = removed
            group = self.projection("R4_BTREE_DELETE", "index.item", fields)["groups"]["removed_positions"]
            self.assertEqual(group["status"], "INVALID")
            self.assertEqual(group["raw_values"]["removed"], removed)

    def test_unknown_legacy_event_is_raw_only_without_summary_schema(self):
        for family, fields in (("R4_HEAP_PRUNE", prune_fields()),
                               ("R4_BTREE_DELETE", index_fields())):
            projected = self.projection(family, "future.event", fields)
            self.assertEqual(projected["status"], "UNSUPPORTED")
            self.assertEqual(projected["groups"], {})

    def test_duplicate_sequences_keep_provenance_without_identity_or_offset_join(self):
        raw = b"unrelated\n" + line("R4_HEAP_PRUNE", "prune.item", prune_item_fields(), 7)
        raw += line("R4_BTREE_DELETE", "index.item", index_item_fields(), 7)
        result = self.read(raw)
        projected = result["nodes"][0]["producer_fields"]
        self.assertEqual([item["event_seq"] for item in projected], [7, 7])
        self.assertEqual([item["sequence_domain"] for item in projected], [None, None])
        self.assertEqual(result["field_root"], "OPEN")
        self.assertNotIn("events", result)
        self.assertFalse(result["ready_to_run"])
        for index, item in enumerate(projected):
            observation = result["nodes"][0]["trace"]["observations"][index]
            self.assertFalse(item["identity_bound"])
            self.assertEqual(item["source"]["stderr_sha256"], hashlib.sha256(raw).hexdigest())
            for key in ("line", "byte_offset", "byte_length", "sha256"):
                self.assertEqual(item["source"][key], observation[key])
            self.assertNotIn("predecessor_event_id", item)
            self.assertNotIn("after_offset", item["groups"])
            self.assertNotIn("procno", observation["fields"])

    def test_cli_rejects_invalid_active_projection_without_acceptance_credit(self):
        fields = index_item_fields()
        fields["removed"] = "{2:137/4294967296}"
        self.read(line("R4_BTREE_DELETE", "index.item", fields))
        process = subprocess.run([sys.executable, "-B", str(Path(workload_evidence.__file__)),
                                  str(self.capture.fixture.root / "manifest.json")],
                                 capture_output=True, text=True)
        result = json.loads(process.stdout)
        self.assertEqual(process.returncode, 1)
        self.assertEqual(result["projection_status"], "INVALID")
        self.assertFalse(result["ready_to_run"])
        self.assertEqual(result["field_root"], "OPEN")


if __name__ == "__main__":
    unittest.main()
