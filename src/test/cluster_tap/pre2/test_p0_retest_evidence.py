"""Same-carrier grouping and raw ITL copies never supply transaction proof."""
import json
from pathlib import Path
import tempfile
import unittest

from p0_retest import judge, judge_directory
from p0_retest_evidence import request_evidence, itl_evidence


def lines():
    key = "backend=7 request=18446744073709551614 xid=1200 tag=1663/5/900/0/2"
    return [
        f"LOG: PGRAC ordinary TT exchange refused: node=0 origin=3 {key} epoch=9 received=0 status=-1",
        f"LOG: PGRAC ordinary TT outbound refused: node=0 requester=0 destination=3 {key} epoch=9 send_result=3",
        f"LOG: PGRAC ordinary TT refused: node=3 requester=0 {key} epoch=9 predicate=TT_SCAN_UNAVAILABLE",
        f"LOG: PGRAC ordinary TT reply: path=queued node=3 requester=0 {key} request_epoch=9 send_result=0",
    ]


class RequestEvidenceTests(unittest.TestCase):
    def collect(self, rows=None):
        a, b, c, d = rows or lines()
        return request_evidence([a + "\n" + b, "", "", c + "\n" + d])

    def test_four_copied_boundaries_keep_the_actual_full_carrier_key(self):
        result = self.collect()
        self.assertEqual(len(result["groups"]), 1)
        group = result["groups"][0]
        self.assertEqual(group["key"]["request_id"], "18446744073709551614")
        self.assertEqual(group["key"]["tag"], [1663, 5, 900, 0, 2])
        self.assertEqual(group["observed_boundaries"], ["requester", "outbound", "serve_refusal", "reply_handoff"])
        self.assertFalse(group["identity_bound"])
        self.assertEqual(group["frontend_consumption"], "UNKNOWN")
        self.assertEqual(result["records"][0]["fields"]["received"], "0")
        self.assertEqual(result["records"][0]["fields"]["status"], "-1")

    def test_backend_origin_epoch_tag_and_xid_must_each_match(self):
        for before, after in (("backend=7", "backend=8"), ("epoch=9", "epoch=10"),
                              ("1663/5/900/0/2", "1663/5/900/0/3"),
                              ("xid=1200", "xid=1201"), ("node=3", "node=2")):
            with self.subTest(after=after):
                rows = lines()
                rows[2] = rows[2].replace(before, after)
                result = self.collect(rows)
                self.assertFalse(any(len(g["observed_boundaries"]) == 4 for g in result["groups"]))

    def test_legacy_missing_backend_is_retained_without_joining(self):
        rows = lines()
        rows[0] = rows[0].replace("backend=7 ", "")
        result = self.collect(rows)
        self.assertEqual(result["records"][0]["missing_fields"], ["backend"])
        self.assertIsNone(result["records"][0]["key"])
        self.assertEqual(result["records"][0]["raw"], rows[0])

    def test_ambiguous_or_invalid_keys_are_not_joined(self):
        for before, after in (("request=18446744073709551614", "request=18446744073709551616"),
                              ("backend=7", "backend=-1"), ("backend=7", "backend=7 backend=7"),
                              ("tag=1663/5/900/0/2", "tag=1663/5/900/0")):
            with self.subTest(after=after):
                rows = lines()
                rows[0] = rows[0].replace(before, after)
                record = self.collect(rows)["records"][0]
                self.assertIsNone(record["key"])
                self.assertTrue(record["error"])

    def test_copied_log_from_another_node_cannot_acquire_a_source_identity(self):
        result = request_evidence(["", "", "", lines()[0]])
        self.assertIsNone(result["records"][0]["key"])
        self.assertIn("source node", result["records"][0]["error"])

    def test_verbose_log_prefix_and_sql_quoting_are_distinct(self):
        line = "2026-01-01 01:02:03 [100] " + lines()[0].replace("LOG: ", "LOG: 00000: ", 1)
        result = request_evidence([line + "\nSTATEMENT: SELECT '" + lines()[0] + "';", "", "", ""])
        self.assertEqual(len(result["records"]), 1)
        self.assertEqual(result["records"][0]["source_line"], 1)

    def test_duplicate_cuts_remain_separate_and_never_prove_delivery(self):
        result = request_evidence([lines()[0] + "\n" + lines()[0], "", "", ""])
        self.assertEqual(len(result["records"]), 2)
        self.assertEqual(result["groups"][0]["repeated_boundaries"], ["requester"])
        self.assertEqual(result["groups"][0]["frontend_consumption"], "UNKNOWN")


def trace():
    return dict(available=True, enabled=True, export_complete=True, overflow=False, dropped=0,
                events=[dict(kind=4, sequence=20, node=0, pid=100, page_known=True,
                             authoritative=False, tag=[1663, 5, 900, 0, 2], page_lsn="300",
                             page_scn="400", slots=[dict(index=i, xid=1200+i, itl_wrap=5+i,
                                                        flags=1, lock_count=0, commit_scn="0",
                                                        write_scn="400", first_change_lsn="300",
                                                        tt_wrap_known=False, uba_bytes="01" * 16)
                                                    for i in range(8)])])


class ItlEvidenceTests(unittest.TestCase):
    def test_missing_or_invalid_slot_facts_never_make_a_complete_copy(self):
        for field in trace()["events"][0]["slots"][6]:
            sample = trace()
            del sample["events"][0]["slots"][6][field]
            self.assertFalse(itl_evidence(sample)["pages"][0]["copy_complete"], field)
        for field, value in (("xid", -1), ("itl_wrap", 65536), ("flags", 256),
                             ("lock_count", 256), ("commit_scn", "-1"),
                             ("write_scn", True), ("first_change_lsn", "18446744073709551616"),
                             ("uba_bytes", "gg" * 16), ("uba_bytes", "00"),
                             ("tt_wrap_known", True)):
            sample = trace()
            sample["events"][0]["slots"][6][field] = value
            result = itl_evidence(sample)
            self.assertFalse(result["pages"][0]["copy_complete"], (field, value))
            self.assertEqual(result["pages"][0]["copy"], sample["events"][0])

    def test_slot_number_is_an_index_not_array_position_or_tt_wrap(self):
        sample = trace()
        sample["events"][0]["slots"].reverse()
        result = itl_evidence(sample)
        self.assertTrue(result["prefix_complete"])
        self.assertEqual(result["pages"][0]["slot6"]["xid"], 1206)
        self.assertEqual(result["pages"][0]["slot6"]["itl_wrap"], 11)
        self.assertFalse(result["pages"][0]["slot6"]["tt_wrap_known"])
        self.assertEqual(result["producer_identity"], "UNKNOWN")

    def test_missing_duplicate_or_unknown_page_keeps_the_gap(self):
        for field in ("missing", "duplicate", "unknown", "no_lsn"):
            sample = trace()
            page = sample["events"][0]
            if field == "missing": page["slots"] = page["slots"][:6]
            if field == "duplicate": page["slots"][7]["index"] = 6
            if field == "unknown": page["page_known"] = False
            if field == "no_lsn": del page["page_lsn"]
            result = itl_evidence(sample)
            self.assertFalse(result["pages"][0]["copy_complete"], field)
            self.assertEqual(result["producer_identity"], "UNKNOWN")

    def test_matching_raw_xid_does_not_bind_a_tt_producer(self):
        sample = trace()
        sample["events"].append(dict(kind=2, xid=1206, key_known=True,
                                     key=dict(xid=1206, slot_wrap=55), pid=200))
        result = itl_evidence(sample)
        self.assertEqual(len(result["tt_publications"]), 1)
        self.assertEqual(result["producer_identity"], "UNKNOWN")
        self.assertNotIn("key", result["pages"][0]["slot6"])

    def test_dropped_or_incomplete_prefix_is_not_complete(self):
        for patch in ({"overflow": True}, {"export_complete": False}, {"dropped": 1},
                      {"enabled": False}, {"available": False}):
            result = itl_evidence(trace() | patch)
            self.assertFalse(result["prefix_complete"])
            self.assertEqual(result["producer_identity"], "UNKNOWN")


class JudgeEvidenceTests(unittest.TestCase):
    def test_missing_case_still_preserves_request_observations(self):
        result = judge("P0-5", {}, "", [lines()[0].replace("backend=7 ", ""), "", "", ""])
        self.assertEqual(result["status"], "NOT_REACHED")
        self.assertEqual(result["request_evidence"]["records"][0]["missing_fields"], ["backend"])

    def test_saved_native_copy_is_projected_without_awarding_path_credit(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp)
            (output / "log").mkdir()
            (output / "tap.log").write_text("not ok 1 - original failure\n1..1\n")
            (output / "log/m2-b07-candidate-latest.json").write_text(json.dumps({"trace": trace()}))
            result = judge_directory("P0-8", output)
            self.assertTrue(result["itl_evidence"]["pages"][0]["copy_complete"])
            self.assertEqual(result["status"], "NOT_REACHED")
            self.assertFalse(result["p0_closed"])

    def test_bad_optional_copy_cannot_erase_the_workload_judgement(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp)
            (output / "log").mkdir()
            (output / "tap.log").write_text("not ok 1 - original failure\n1..1\n")
            for raw in ("{", "[]"):
                (output / "log/m2-b07-candidate-latest.json").write_text(raw)
                result = judge_directory("P0-8", output)
                self.assertEqual(result["status"], "NOT_REACHED")
                self.assertFalse(result["itl_evidence"]["prefix_complete"])
                self.assertTrue(result["itl_evidence"]["error"])
                self.assertEqual(json.loads((output / "judgement.json").read_text()), result)


if __name__ == "__main__":
    unittest.main()
