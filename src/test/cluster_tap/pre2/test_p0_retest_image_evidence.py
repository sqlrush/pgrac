"""A local image projection never proves a SQL statement or snapshot binding."""
import importlib.util
import unittest

import p0_retest

SPEC = importlib.util.find_spec("p0_retest_image_evidence")
if SPEC is not None:
    from p0_retest_image_evidence import image_evidence
else:
    image_evidence = None


def capture():
    return dict(version=1, case="P0-10", rc=0, sql_completed=True, normal_stop=True,
                cleanup_errors=[], nodes=[0, 1, 2, 3], source_commit="a" * 40,
                sql_evidence=dict(case="P0-10", reader=1,
                    reader_sql_identity=dict(node_id=1, pid=201, backend_start="birth",
                        postmaster_start="postmaster", database_oid=5, system_identifier="99"),
                    locator=dict(spcOid=1663, dbOid=5, relNumber=900, forkNum=0, blocks=2),
                    snapshot=dict(read_scn="18446744073709551614", read_epoch="9")))


def line(family="READ_IMAGE", **changes):
    # Current formatter fields, including explicit missing identity/read-SCN facts.
    fields = dict(PGRAC_FAMILY=family, event="install.end", event_seq="7",
        sequence_scope="process", sequence_domain=family, node_id="1", pid="201",
        procno="7", backend_id="8", wall_us="30", monotonic_us="20",
        identity_source="run_manifest", run_id_known="0", process_incarnation_known="0",
        backend_type_known="0", transaction_identity_known="0", role="requester",
        master_node="1", holder_node="0", holder_procno="3", holder_epoch="9",
        holder_request_id="1", transition_count="0", pending_x_since_lsn="0",
        authority_state="0", authority_x_holder="0", authority_s_bitmap="0",
        authority_pending_x_requester="-1", authoritative="0",
        operation_request_id="18446744073709551614", request_id="11", request_epoch="9",
        attempt="1", transition_id="3", spc="1663", db="5", rel="900", fork="0", block="1",
        image_only="1", expected_pi_watermark="40", deadline_wall_us="90",
        reply_known="1", status="6", status_source="accepted_reply_header", reply_request_id="11",
        reply_epoch="9", reply_sender="0", forwarding_master="1", reply_backend="8",
        reply_transition="3", page_lsn="100", page_scn_known="0", read_scn_known="0",
        checksum_known="1", checksum_expected="41", checksum_actual="41", installed="1",
        durable_s="0", retry_denied="0", trace_critical_skipped="0", trace_sequence_exhausted="0")
    fields.update(changes)
    # The emitter ending remains last even when a test adds fields.
    ending = fields.pop("trace_sequence_exhausted")
    fields["trace_sequence_exhausted"] = ending
    return "LOG: 00000: " + " ".join(f"{key}={value}" for key, value in fields.items()) + "\n"


class ImageEvidenceTests(unittest.TestCase):
    def collect(self, raw=None, case=None, node=1):
        self.assertIsNotNone(image_evidence, "P0-10 image projection is missing")
        logs = ["LOG: member ready\n"] * 4
        logs[node] = raw if raw is not None else line()
        return image_evidence(capture() if case is None else case, logs)

    def test_same_page_and_reader_do_not_supply_snapshot_or_statement_identity(self):
        result = self.collect()
        record = result["records"][0]
        self.assertTrue(record["source_node_matches"])
        self.assertTrue(record["emitter_matches_sql_reader"])
        self.assertTrue(record["relation_matches_sql_target"])
        self.assertTrue(record["block_within_initial_size"])
        self.assertEqual(record["groups"]["request"]["values"]["operation_request_id"], 18446744073709551614)
        self.assertEqual(record["fields"]["read_scn_known"], "0")
        self.assertEqual(result["snapshot_binding"], "UNKNOWN")
        self.assertEqual(result["sql_statement_binding"], "UNKNOWN")
        self.assertFalse(result["identity_bound"])
        self.assertEqual(result["original_path"], "UNKNOWN")

    def test_wrong_emitter_or_page_is_preserved_without_a_match(self):
        for patch, field in ((dict(pid="202"), "emitter_matches_sql_reader"),
                             (dict(rel="901"), "relation_matches_sql_target"),
                             (dict(fork="1"), "relation_matches_sql_target"),
                             (dict(block="2"), "block_within_initial_size")):
            result = self.collect(line(**patch))
            self.assertFalse(result["records"][0][field])
            self.assertEqual(result["original_path"], "UNKNOWN")

    def test_other_source_node_does_not_acquire_reader_identity(self):
        record = self.collect(node=2)["records"][0]
        self.assertFalse(record["source_node_matches"])
        self.assertFalse(record["emitter_matches_sql_reader"])

    def test_reply_is_not_borrowed_from_a_previous_record(self):
        result = self.collect(line() + line(reply_known="0", status="-1", installed="0"))
        self.assertEqual(len(result["records"]), 2)
        reply = result["records"][1]["groups"]["reply"]
        self.assertEqual(reply["status"], "UNKNOWN")
        self.assertEqual(reply["values"], {})
        self.assertEqual(result["records"][1]["fields"]["status"], "-1")

    def test_ingress_unvalidated_tag_does_not_match_a_relation(self):
        record = self.collect(line("READ_IMAGE_INGRESS", header_validated="0"))["records"][0]
        self.assertIsNone(record["relation_matches_sql_target"])

    def test_hop_tag_uses_its_own_known_flag(self):
        record = self.collect(line("READ_IMAGE_HOP", tag_known="0", forward_known="1",
                                   requester_node="1", requester_backend="8"))["records"][0]
        self.assertEqual(record["groups"]["tag"]["status"], "UNKNOWN")
        self.assertIsNone(record["relation_matches_sql_target"])

    def test_repeated_sequences_and_loss_disclosures_remain_raw(self):
        raw = line(trace_critical_skipped="3")
        result = self.collect(raw + raw)
        self.assertEqual([r["line"] for r in result["records"]], [1, 2])
        self.assertEqual(result["records"][0]["fields"]["trace_critical_skipped"], "3")
        self.assertFalse(result["atomic_graph"])
        self.assertNotIn("groups", result)

    def test_malformed_emitter_is_retained_as_a_rejection(self):
        raw = line().replace("pid=201 ", "pid=201 pid=202 ")
        result = self.collect(raw)
        self.assertFalse(result["records"])
        self.assertIn("duplicate", result["rejections"][0]["reason"])
        self.assertEqual(bytes.fromhex(result["rejections"][0]["raw_hex"]).decode(), raw)

    def test_missing_or_invalid_sql_scope_is_not_filled_from_logs(self):
        for case in ({}, capture() | {"sql_evidence": None}):
            record = self.collect(case=case)["records"][0]
            self.assertIsNone(record["emitter_matches_sql_reader"])
            self.assertIsNone(record["relation_matches_sql_target"])
        case = capture()
        case["sql_evidence"]["reader_sql_identity"]["pid"] = True
        self.assertIsNone(self.collect(case=case)["records"][0]["emitter_matches_sql_reader"])

    def test_sql_workload_pass_without_trace_stays_unknown(self):
        result = p0_retest.judge("P0-10", capture(), "ok 1\n1..1\n", ["LOG: member ready\n"] * 4)
        self.assertEqual(result["status"], "WORKLOAD_PASS")
        self.assertIn("image_evidence", result)
        self.assertFalse(result["image_evidence"]["records"])
        self.assertEqual(result["original_path"], "UNKNOWN")
        self.assertFalse(result["p0_closed"])


if __name__ == "__main__":
    unittest.main()
