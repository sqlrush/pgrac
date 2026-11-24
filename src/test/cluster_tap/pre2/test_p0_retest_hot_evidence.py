"""HOT copies and SQL scalar matches do not establish a logical-row proof."""
import importlib.util
import unittest

import p0_retest

if importlib.util.find_spec("p0_retest_hot_evidence") is not None:
    from p0_retest_hot_evidence import hot_evidence
else:
    hot_evidence = None


def capture():
    return dict(version=1, case="P0-12", rc=0, sql_completed=True, normal_stop=True,
                cleanup_errors=[], nodes=[0, 1, 2, 3], source_commit="a" * 40,
                sql_evidence=dict(case="P0-12", reader=2,
                    reader_sql_identity=dict(node_id=2, pid=301, backend_start="birth",
                        postmaster_start="postmaster", database_oid=5, system_identifier="99"),
                    locator=dict(spcOid=1663, dbOid=5, relNumber=900, forkNum=0, blocks=2),
                    snapshot=dict(read_scn="9223372036854775806", read_epoch="9")))


MEMBER = ("item_known=1 lp=1 tuple_known=1 off=7000 len=40 xmin=1200 xmax=0 "
          "ctid=1/3 mask=1/2 raw_cid=1 combo=0 itl=6 slot_known=1 slot_xid=1200 "
          "wrap=7 flags=2 write_scn=40 commit_scn=50 first_change_lsn=60 uba_raw=100/200")


def line(family="HOT_FULL", **changes):
    fields = dict(PGRAC_FAMILY=family, event="full.search.end", event_seq="7",
        sequence_scope="process", sequence_domain=family, cycle="5", node_id="2", pid="301",
        wall_us="30", monotonic_us="20", identity_source="run_manifest", run_id_known="0",
        boot_identity_known="0", process_incarnation_known="0", request_id_known="0",
        full_xid_known="0", tx_origin_known="0", slot_incarnation_known="0", authoritative="0",
        buffer="1", spc="1663", db="5", rel="900", fork="0", block="1", root_block="1",
        root_offset="2", queried_offset="3", read_scn="9223372036854775806", read_epoch="9",
        snapshot_curcid="1", image_scope="owned_copy", input_revalidated="0", image_hash_known="0",
        page_generation_known="0", build_known="1", build_result="0", build_reason="0",
        search_known="1", candidate_kind="1", before_known="1", before_lsn="40",
        before_scn="50", before_watermark="10", full_known="1", full_lsn="20", full_scn="30",
        full_watermark="0", before_root="{" + MEMBER + "}", before_queried="{" + MEMBER + "}",
        full_root="{" + MEMBER + "}", full_queried="{" + MEMBER + "}",
        decision_text_complete="0", decisions="{stage=unknown}", trace_critical_skipped="0",
        trace_sequence_exhausted="0")
    fields.update(changes)
    ending = fields.pop("trace_sequence_exhausted")
    fields["trace_sequence_exhausted"] = ending
    return "LOG: 00000: " + " ".join(f"{key}={value}" for key, value in fields.items()) + "\n"


class HotEvidenceTests(unittest.TestCase):
    def collect(self, raw=None, case=None, node=2):
        self.assertIsNotNone(hot_evidence, "HOT retest projection is missing")
        logs = ["LOG: member ready\n"] * 4
        logs[node] = line() if raw is None else raw
        return hot_evidence(capture() if case is None else case, logs)

    def test_matching_scalar_values_do_not_prove_a_member_or_page_version(self):
        result = self.collect()
        record = result["records"][0]
        self.assertTrue(record["emitter_matches_sql_reader"])
        self.assertTrue(record["relation_matches_sql_target"])
        self.assertTrue(record["snapshot_values_match"])
        self.assertEqual(record["groups"]["snapshot"]["values"]["read_scn"], 9223372036854775806)
        self.assertEqual(record["groups"]["before_root_slot"]["values"]["wrap"], 7)
        self.assertEqual(record["fields"]["input_revalidated"], "0")
        self.assertEqual(record["fields"]["page_generation_known"], "0")
        for name in ("original_path", "member_binding", "page_version_binding", "sql_statement_binding"):
            self.assertEqual(result[name], "UNKNOWN")
        self.assertFalse(result["identity_bound"])

    def test_each_member_requires_its_own_copy_and_known_flags(self):
        record = self.collect(line(before_known="0", full_queried="{item_known=0}"))["records"][0]
        self.assertEqual(record["groups"]["before_root_tuple"]["status"], "UNKNOWN")
        self.assertEqual(record["groups"]["full_root_tuple"]["status"], "OBSERVED")
        self.assertEqual(record["groups"]["full_queried_tuple"]["values"], {})
        self.assertEqual(record["groups"]["before_root_slot"]["raw_values"]["before_root"], "{" + MEMBER + "}")

    def test_same_xid_with_different_itl_wraps_stays_in_separate_cuts(self):
        raw = line() + line(full_root="{" + MEMBER.replace("wrap=7", "wrap=8") + "}")
        result = self.collect(raw)
        self.assertEqual([r["groups"]["full_root_slot"]["values"]["wrap"] for r in result["records"]], [7, 8])
        self.assertEqual([r["line"] for r in result["records"]], [1, 2])
        self.assertEqual(result["member_binding"], "UNKNOWN")
        self.assertNotIn("groups", result)

    def test_scn_and_epoch_must_both_match_without_float_rounding(self):
        for patch in (dict(read_scn="9223372036854775807"), dict(read_epoch="10")):
            self.assertFalse(self.collect(line(**patch))["records"][0]["snapshot_values_match"])

    def test_missing_snapshot_is_not_backfilled_from_another_record(self):
        raw = line() + line(read_epoch="bad")
        self.assertIsNone(self.collect(raw)["records"][1]["snapshot_values_match"])
        case = capture()
        case["sql_evidence"]["snapshot"]["read_scn"] = True
        self.assertIsNone(self.collect(case=case)["records"][0]["snapshot_values_match"])

    def test_wrong_emitter_and_relation_are_not_accepted_as_sql_scope(self):
        self.assertFalse(self.collect(node=1)["records"][0]["source_node_matches"])
        self.assertFalse(self.collect(line(pid="302"))["records"][0]["emitter_matches_sql_reader"])
        self.assertFalse(self.collect(line(rel="901"))["records"][0]["relation_matches_sql_target"])

    def test_no_scope_is_filled_from_the_log(self):
        record = self.collect(case={})["records"][0]
        self.assertIsNone(record["emitter_matches_sql_reader"])
        self.assertIsNone(record["relation_matches_sql_target"])
        self.assertIsNone(record["snapshot_values_match"])

    def test_undo_line_stays_separate_without_borrowing_hot_snapshot_or_member(self):
        result = self.collect(line() + line("R4_UNDO", event="walk", locator_known="0", locator_xid="1200"))
        record = result["records"][1]
        self.assertEqual(record["family"], "R4_UNDO")
        self.assertIsNone(record["snapshot_values_match"])
        self.assertEqual(record["groups"]["locator"]["status"], "UNKNOWN")
        self.assertNotIn("full_root_slot", record["groups"])
        self.assertEqual(result["member_binding"], "UNKNOWN")

    def test_malformed_nested_value_and_loss_disclosures_are_preserved(self):
        raw = line(full_root="{" + MEMBER.replace("uba_raw=100/200", "uba_raw=bad") + "}",
                   trace_critical_skipped="4", trace_sequence_exhausted="1")
        record = self.collect(raw)["records"][0]
        self.assertEqual(record["groups"]["full_root_slot"]["status"], "INVALID")
        self.assertEqual(record["fields"]["trace_critical_skipped"], "4")
        self.assertEqual(record["raw"], raw)

    def test_bad_emitter_does_not_hide_a_later_observation(self):
        bad = line().replace("pid=301 ", "pid=301 pid=302 ")
        result = self.collect(bad + line())
        self.assertEqual(len(result["rejections"]), 1)
        self.assertEqual(result["records"][0]["line"], 2)
        self.assertFalse(result["atomic_graph"])

    def test_sql_success_without_hot_trace_keeps_the_path_unknown(self):
        result = p0_retest.judge("P0-12", capture(), "ok 1\n1..1\n", ["LOG: member ready\n"] * 4)
        self.assertEqual(result["status"], "WORKLOAD_PASS")
        self.assertIn("hot_evidence", result)
        self.assertFalse(result["hot_evidence"]["records"])
        self.assertEqual(result["original_path"], "UNKNOWN")

    def test_original_error_followed_by_full_copy_still_fails(self):
        logs = ["LOG: member ready\n"] * 4
        logs[2] = "ERROR: R4 HOT reconstruction cannot prove the logical row\n" + line()
        result = p0_retest.judge("P0-12", capture(), "ok 1\n1..1\n", logs)
        self.assertEqual(result["status"], "FAIL")
        self.assertIn("hot_evidence", result)
        self.assertEqual(len(result["hot_evidence"]["records"]), 1)
        self.assertFalse(result["p0_closed"])


if __name__ == "__main__":
    unittest.main()
