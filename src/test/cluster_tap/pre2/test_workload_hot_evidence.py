"""Offline native HOT/undo formatter fixtures; no SQL or workload verdict."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import unittest

import test_workload_evidence as fixture
import workload_evidence


ITEM = ("item_known=1 lp=1 tuple_known=1 off=7900 len=80 xmin=14 xmax=15 "
        "ctid=137/3 mask=256/512 raw_cid=4 combo=1 itl=2 slot_known=1 "
        "slot_xid=16 wrap=17 flags=18 write_scn=19 commit_scn=20 "
        "first_change_lsn=21 uba_raw=22/23")


def full_fields():
    return dict(fixture.TAG, cycle="31", buffer="-1", root_block="137", root_offset="1",
                queried_offset="3", read_scn="32", read_epoch="33", snapshot_curcid="4",
                build_known="1", build_result="2", build_reason="0", search_known="1",
                candidate_kind="1", before_known="1", before_lsn="34", before_scn="35",
                before_watermark="36", full_known="1", full_lsn="37", full_scn="38",
                full_watermark="39", before_root="{" + ITEM + "}",
                before_queried="{" + ITEM + "}", full_root="{" + ITEM + "}",
                full_queried="{" + ITEM + "}", decision_text_complete="0",
                decisions="{trace_known=0}", input_revalidated="0", image_hash_known="0",
                page_generation_known="0", full_xid_known="0", slot_incarnation_known="0")


def update_fields():
    fields = dict(raw_xid="14", canonical_xid="15", input_command_id="4", hot="1",
                  cross_page="0", requested_tid="137/1", old_tid="137/2", new_tid="137/3",
                  undo_uba_raw="18446744073709551615/0", wal_inserted="1", wal_lsn="45",
                  wal_flush_known="0", old_tag="1663/16384/17000/0/137",
                  new_tag="1663/16384/17000/0/138", after_old_tag="1663/16384/17000/0/139",
                  after_new_tag="1663/16384/17000/0/140")
    for index, name in enumerate(("before_old", "before_new", "after_old", "after_new")):
        fields.update({name + "_page_known": "1", name + "_offset": str(index + 1),
                       name + "_lsn": str(50 + index), name + "_scn": str(60 + index),
                       name: "{" + ITEM + "}"})
    return fields


def undo_fields():
    return dict(fixture.TAG, slot="1", slot_generation="41", builder_incarnation="42",
                builder_pid="101", builder_worker="0", edge_incarnation="43", edge_pid="102",
                edge_worker="1", edge_role="2", read_scn="32", formation_epoch="33",
                activation_generation="44", master_authority_generation="45", master_transition="46",
                real_master="0", selected_holder="1", expected_page_scn="47",
                copied_page_lsn="48", copied_page_scn="49", candidate_cursor="0",
                candidate_count="2", covered_mask="1", record_format="2", build_steps="3",
                max_steps="100", visited="3", have_previous="1", locator_known="1",
                locator_xid="14", locator_tt_wrap="15", locator_itl_kind="1", locator_itl_slot="2",
                locator_uba="22/23", uba_known="1", uba="24/25", uba_decoded="1",
                uba_segment="26", uba_block="27", uba_tt_offset="28", uba_row_offset="29",
                foreign_request_id="51", origin_formation_epoch="52", origin_tt_generation="53",
                origin_authority_scn="54", read_return_known="1", record_length="128",
                record_header_known="1", record_type="1", record_flags="2", record_payload_length="64",
                record_xid="14", record_origin="1", record_tt_segment="26", record_tt_slot="28",
                record_tt_wrap_plus1="16", record_write_scn="55", record_prev="56/57",
                target_spc="1663", target_db="16384", target_rel="17000", target_fork="0",
                target_block="141", target_offset="3", page_known="1", page_lsn="58",
                page_scn="59", page_watermark="60", page_itl_known="1", page_itl_xid="16",
                page_itl_wrap="17", page_itl_flags="18", page_itl_write_scn="19",
                page_itl_uba="22/23", target_item_known="1", target_item_flags="1",
                target_item_offset="7900", target_item_length="80", target_tuple_known="1",
                tuple_xmin="14", tuple_xmax="15", tuple_ctid="137/3", tuple_mask="256/512",
                apply_record="-1", locator_reason_known="1", locator_reason="0")


class WorkloadHotEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.capture = fixture.WorkloadEvidenceTests("runTest")
        self.capture.setUp()
        self.addCleanup(self.capture.doCleanups)

    def read(self, raw):
        return self.capture.read(raw)

    def projection(self, family, fields):
        return self.read(fixture.line(family, fields))["nodes"][0]["producer_fields"][0]

    def test_full_snapshot_and_each_owned_image_remain_distinct(self):
        projected = self.projection("HOT_FULL", full_fields())
        self.assertEqual(projected["status"], "PROJECTED")
        groups = projected["groups"]
        self.assertEqual(groups["snapshot"]["values"]["read_scn"], 32)
        self.assertEqual(groups["before_page"]["values"]["page_lsn"], 34)
        self.assertEqual(groups["full_page"]["values"]["page_lsn"], 37)
        self.assertEqual(groups["full_root_tuple"]["values"]["ctid"],
                         dict(blockNum=137, offsetNumber=3))
        self.assertEqual(groups["full_root_slot"]["values"]["uba_raw"], dict(word0=22, word1=23))
        self.assertEqual(groups["full_root_tuple"]["source_fields"]["ctid"], "full_root.ctid")

    def test_itemid_redirect_is_not_promoted_to_tuple_or_slot(self):
        fields = full_fields()
        fields["full_root"] = "{item_known=1 lp=2 off=7 len=0 tuple_known=0}"
        groups = self.projection("HOT_FULL", fields)["groups"]
        self.assertEqual(groups["full_root_item"]["values"], dict(lp=2))
        self.assertEqual(groups["full_root_storage"]["values"], dict(off=7, len=0))
        for name in ("full_root_tuple", "full_root_slot"):
            self.assertEqual(groups[name]["status"], "UNKNOWN")
            self.assertEqual(groups[name]["values"], {})
        self.assertEqual(groups["full_queried_tuple"]["status"], "OBSERVED")

    def test_item_without_valid_storage_retains_lp_only(self):
        fields = full_fields()
        fields["before_root"] = "{item_known=1 lp=1 tuple_known=0}"
        groups = self.projection("HOT_FULL", fields)["groups"]
        self.assertEqual(groups["before_root_item"]["values"], dict(lp=1))
        self.assertEqual(groups["before_root_storage"]["status"], "UNKNOWN")
        self.assertIn("before_root.off", groups["before_root_storage"]["missing"])

    def test_page_and_nested_known_flags_all_gate_their_own_payload(self):
        for flag in ("full_known", "item_known", "tuple_known", "slot_known"):
            with self.subTest(flag=flag):
                fields = full_fields()
                if flag == "full_known":
                    fields[flag] = "0"
                else:
                    fields["full_root"] = "{" + ITEM.replace(flag + "=1", flag + "=0") + "}"
                groups = self.projection("HOT_FULL", fields)["groups"]
                self.assertEqual(groups["full_root_slot"]["status"], "UNKNOWN")
                self.assertEqual(groups["full_root_slot"]["values"], {})
                self.assertEqual(groups["before_root_slot"]["status"], "OBSERVED")

    def test_build_and_search_flags_do_not_supply_each_other(self):
        fields = full_fields()
        fields.update(build_known="0", build_result="-1", search_known="0", candidate_kind="0")
        groups = self.projection("HOT_FULL", fields)["groups"]
        for name in ("build", "search"):
            self.assertEqual(groups[name]["status"], "UNKNOWN")
        self.assertEqual(groups["tag"]["status"], "OBSERVED")

    def test_nested_syntax_and_missing_or_malformed_guards_are_retained(self):
        for text in ("{item_known=1 lp=1 lp=2}", "{item_known=2 lp=1}",
                     "{item_known=1 lp=1 tuple_known=1 slot_known=2}"):
            with self.subTest(text=text):
                fields = full_fields()
                fields["full_root"] = text
                projected = self.projection("HOT_FULL", fields)
                self.assertEqual(projected["status"], "INVALID")
                self.assertEqual(projected["groups"]["full_root_slot"]["raw_values"]["full_root"], text)
        fields = full_fields()
        fields["full_root"] = "{" + ITEM.replace("slot_known=1 ", "") + "}"
        group = self.projection("HOT_FULL", fields)["groups"]["full_root_slot"]
        self.assertEqual(group["status"], "UNKNOWN")
        self.assertIn("full_root.slot_known", group["missing"])

    def test_update_tags_tids_page_versions_and_wal_are_local_projections(self):
        fields = update_fields()
        projected = self.projection("R4_HEAP_UPDATE", fields)
        self.assertEqual(projected["status"], "PROJECTED")
        groups = projected["groups"]
        self.assertEqual(groups["tids"]["values"]["new_tid"], dict(blockNum=137, offsetNumber=3))
        for name, block, lsn in (("before_old", 137, 50), ("before_new", 138, 51),
                                 ("after_old", 139, 52), ("after_new", 140, 53)):
            self.assertEqual(groups[name + "_page"]["values"]["tag"]["blockNum"], block)
            self.assertEqual(groups[name + "_page"]["values"]["page_lsn"], lsn)
        self.assertEqual(groups["undo"]["values"]["uba_raw"], dict(word0=2**64 - 1, word1=0))
        self.assertEqual(groups["wal"]["values"], dict(wal_lsn=45))
        fields["wal_inserted"] = "0"
        self.assertEqual(self.projection("R4_HEAP_UPDATE", fields)["groups"]["wal"]["status"], "UNKNOWN")
        self.assertNotIn("wal_durable", projected)

    def test_undo_record_target_locator_and_page_are_not_merged(self):
        projected = self.projection("R4_UNDO", undo_fields())
        self.assertEqual(projected["status"], "PROJECTED")
        groups = projected["groups"]
        self.assertEqual(groups["tag"]["values"]["blockNum"], 137)
        self.assertEqual(groups["record_target"]["values"]["blockNum"], 141)
        self.assertEqual(groups["locator"]["values"]["uba_raw"], dict(word0=22, word1=23))
        self.assertEqual(groups["undo"]["values"]["uba_raw"], dict(word0=24, word1=25))
        self.assertEqual(groups["record"]["values"]["prev_uba_raw"], dict(word0=56, word1=57))
        self.assertEqual(groups["target_tuple"]["values"]["mask"], dict(infomask=256, infomask2=512))

    def test_undo_known_flags_preserve_missing_evidence(self):
        for flag, names in (("locator_known", ("locator",)), ("uba_known", ("undo", "decoded_uba")),
                            ("record_header_known", ("record", "record_target")),
                            ("page_known", ("page", "page_itl", "target_item", "target_tuple")),
                            ("target_item_known", ("target_item", "target_tuple")),
                            ("target_tuple_known", ("target_tuple",)),
                            ("locator_reason_known", ("locator_reason",))):
            with self.subTest(flag=flag):
                fields = undo_fields()
                fields[flag] = "0"
                groups = self.projection("R4_UNDO", fields)["groups"]
                for name in names:
                    self.assertEqual(groups[name]["status"], "UNKNOWN")
                    self.assertEqual(groups[name]["values"], {})

    def test_slash_operands_require_exact_component_counts_and_native_ranges(self):
        for family, make, field, raw in (
                ("R4_HEAP_UPDATE", update_fields, "old_tag", "1663/16384/17000/0"),
                ("R4_HEAP_UPDATE", update_fields, "old_tag", "1663/16384/17000/-2147483649/137"),
                ("R4_HEAP_UPDATE", update_fields, "new_tid", "137/3/4"),
                ("R4_HEAP_UPDATE", update_fields, "new_tid", "-1/3"),
                ("R4_HEAP_UPDATE", update_fields, "undo_uba_raw", "18446744073709551616/0"),
                ("R4_UNDO", undo_fields, "tuple_mask", "256/4294967296"),
                ("R4_UNDO", undo_fields, "record_prev", "0x20/2")):
            with self.subTest(field=field, raw=raw):
                fields = make()
                fields[field] = raw
                projected = self.projection(family, fields)
                self.assertEqual(projected["status"], "INVALID")
                self.assertTrue(any(field in group["invalid"] for group in projected["groups"].values()))

    def test_missing_active_item_field_is_unknown_without_filling_from_another_image(self):
        fields = full_fields()
        fields["full_root"] = "{" + ITEM.replace("ctid=137/3 ", "") + "}"
        groups = self.projection("HOT_FULL", fields)["groups"]
        self.assertEqual(groups["full_root_tuple"]["status"], "UNKNOWN")
        self.assertEqual(groups["full_root_tuple"]["values"], {})
        self.assertEqual(groups["full_queried_tuple"]["status"], "OBSERVED")

    def test_duplicate_sequence_and_raw_provenance_never_bind_or_join(self):
        raw = b"unrelated\n" + fixture.line("HOT_FULL", full_fields(), 7)
        raw += fixture.line("R4_UNDO", undo_fields(), 7)
        result = self.read(raw)
        projected = result["nodes"][0]["producer_fields"]
        self.assertEqual([item["event_seq"] for item in projected], [7, 7])
        self.assertEqual([item["sequence_domain"] for item in projected], ["HOT_FULL", "R4_UNDO"])
        self.assertFalse(result["identity_bound"])
        self.assertFalse(result["ready_to_run"])
        self.assertEqual(result["field_root"], "OPEN")
        self.assertNotIn("events", result)
        for index, item in enumerate(projected):
            observation = result["nodes"][0]["trace"]["observations"][index]
            self.assertFalse(item["identity_bound"])
            self.assertEqual(item["source"]["stderr_sha256"], hashlib.sha256(raw).hexdigest())
            for key in ("line", "byte_offset", "byte_length", "sha256"):
                self.assertEqual(item["source"][key], observation[key])
            self.assertNotIn("predecessor_event_id", item)

    def test_cli_rejects_malformed_active_projection_without_acceptance_credit(self):
        fields = update_fields()
        fields["old_tag"] = "1663/16384/17000/0/4294967296"
        self.read(fixture.line("R4_HEAP_UPDATE", fields))
        process = subprocess.run([sys.executable, str(Path(workload_evidence.__file__)),
                                  str(self.capture.fixture.root / "manifest.json")],
                                 capture_output=True, text=True)
        result = json.loads(process.stdout)
        self.assertEqual(process.returncode, 1)
        self.assertEqual(result["projection_status"], "INVALID")
        self.assertFalse(result["ready_to_run"])
        self.assertEqual(result["field_root"], "OPEN")


if __name__ == "__main__":
    unittest.main()
