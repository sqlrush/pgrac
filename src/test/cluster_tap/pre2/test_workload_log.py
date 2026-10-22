"""Offline LOG intake tests; fixture parsing is not native acceptance."""
import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


SPEC = importlib.util.find_spec("workload_log")
if SPEC is not None:
    import workload_log
else:
    workload_log = None


FAMILIES = (
    "READ_IMAGE", "READ_IMAGE_INGRESS", "READ_IMAGE_HOP", "R4_UNDO",
    "R4_HEAP_UPDATE", "HOT_FULL", "CTRC_RECEIPT", "CTRC_RECEIPT_LIFECYCLE",
    "CTRC_CONSUME", "CTRC_ORIGIN_REPLY", "CTRC_ORIGIN_BATCH_REPLY",
    "CTRC_ORIGIN_DISPATCH",
)


def message(family="HOT_FULL", extra="", seq="1"):
    domain = "" if family == "CTRC_RECEIPT" else " sequence_domain=" + family
    ending = ("sequence_saturated" if family in
              ("CTRC_ORIGIN_REPLY", "CTRC_ORIGIN_BATCH_REPLY") else
              "sequence_exhausted" if family in
              ("CTRC_CONSUME", "CTRC_ORIGIN_DISPATCH") else
              "trace_sequence_exhausted")
    return ("PGRAC_FAMILY={} event=observed event_seq={} sequence_scope=process{} "
            "node_id=1 pid=101 process_incarnation_known=0 authoritative=0 {} "
            "{}=0".format(family, seq, domain, extra, ending))


def log(payload=None, prefix="2026-10-21 15:00:00 CST [101] LOG:  "):
    return (prefix + (message() if payload is None else payload) + "\n").encode()


class WorkloadLogTests(unittest.TestCase):
    def read(self, data):
        self.assertIsNotNone(workload_log, "raw structured LOG reader is missing")
        return workload_log.read_trace(data)

    def test_brace_values_and_unknown_fields_are_lossless(self):
        extra = ("before_root={known=0 nested={off=1 flags=2}} "
                 "decisions={a=1 b=2 -> c=unknown} uba=11/22 "
                 "read_scn=18446744073709551615 procno=-1 "
                 "decision_text_complete=0 trace_critical_skipped=7")
        raw = log(message(extra=extra))
        result = self.read(raw)
        self.assertEqual(result["parse_status"], "PARSED")
        fields = result["observations"][0]["fields"]
        self.assertEqual(fields["before_root"], "{known=0 nested={off=1 flags=2}}")
        self.assertEqual(fields["decisions"], "{a=1 b=2 -> c=unknown}")
        self.assertEqual(fields["uba"], "11/22")
        self.assertEqual(fields["read_scn"], "18446744073709551615")
        self.assertEqual(fields["procno"], "-1")
        self.assertEqual(fields["decision_text_complete"], "0")
        self.assertEqual(fields["trace_critical_skipped"], "7")
        self.assertEqual(fields["process_incarnation_known"], "0")
        self.assertEqual(fields["authoritative"], "0")
        self.assertTrue(all(isinstance(value, str) for value in fields.values()))
        self.assertEqual(result["observations"][0]["raw"].encode(), raw)

    def test_existing_family_envelopes_and_legacy_missing_domain(self):
        result = self.read(b"".join(log(message(family)) for family in FAMILIES))
        self.assertEqual(result["parse_status"], "PARSED")
        self.assertEqual(len(result["observations"]), len(FAMILIES))
        for family, observation in zip(FAMILIES, result["observations"]):
            fields = observation["fields"]
            self.assertEqual(fields["PGRAC_FAMILY"], family)
            if family == "CTRC_RECEIPT":
                self.assertNotIn("sequence_domain", fields)
            else:
                self.assertEqual(fields["sequence_domain"], family)
            self.assertNotIn("procno", fields)

    def test_source_and_line_hashes_bind_exact_byte_positions(self):
        first = "unrelated UTF-8: 观察\n".encode()
        raw = log() + b"\r\n"
        data = first + raw + log(message("READ_IMAGE"))
        result = self.read(data)
        self.assertEqual(result["source"], dict(sha256=hashlib.sha256(data).hexdigest(),
                                                 size_bytes=len(data)))
        observation = result["observations"][0]
        # The extra blank line is not part of the LOG observation.
        original = log()
        self.assertEqual(observation["line"], 2)
        self.assertEqual(observation["byte_offset"], len(first))
        self.assertEqual(observation["byte_length"], len(original))
        self.assertEqual(observation["sha256"], hashlib.sha256(original).hexdigest())
        self.assertEqual(result["observations"][1]["line"], 4)

    def test_order_and_repeated_sequences_are_not_causal_joins(self):
        payloads = [message("CTRC_CONSUME", "target_ordinal=1 emit_stage=late wall_us=9", "9"),
                    message("READ_IMAGE", "wall_us=1", "2"),
                    message("CTRC_CONSUME", "target_ordinal=0 emit_stage=late wall_us=0", "9")]
        result = self.read(b"".join(log(payload) for payload in payloads))
        self.assertEqual([item["message"] for item in result["observations"]], payloads)
        self.assertEqual(result["field_root"], "OPEN")
        self.assertFalse(result["identity_bound"])
        self.assertFalse(result["ready_to_run"])
        self.assertNotIn("events", result)
        self.assertNotIn("sequences", result)

    def test_unhandled_families_are_visible_and_other_severities_ignored(self):
        raw = (log(message("E_OTHER")) + log(prefix="DETAIL:  ") +
               log(prefix="STATEMENT: SELECT 'LOG:  ") +
               b"LOG: unrelated text PGRAC_FAMILY=HOT_FULL\n")
        result = self.read(raw)
        self.assertEqual(result["parse_status"], "NOT_OBSERVED")
        self.assertEqual(len(result["unhandled"]), 1)
        self.assertEqual(result["unhandled"][0]["family"], "E_OTHER")
        self.assertEqual(result["ignored_lines"], 3)
        self.assertFalse(result["rejections"])

    def test_verbose_sqlstate_is_lossless_log_decoration(self):
        for family in FAMILIES:
            for sqlstate in ("00000", "53R9G"):
                with self.subTest(family=family, sqlstate=sqlstate):
                    decorated = sqlstate + ": " + message(family)
                    raw = log(decorated).replace(b"\n", b"\r\n")
                    first = "unrelated UTF-8: 观察\n".encode()
                    result = self.read(first + raw)
                    self.assertEqual(result["parse_status"], "PARSED")
                    self.assertEqual(len(result["observations"]), 1)
                    observation = result["observations"][0]
                    self.assertEqual(observation["raw"].encode(), raw)
                    self.assertEqual(observation["message"], decorated)
                    self.assertEqual(observation["log_sqlstate"], sqlstate)
                    self.assertEqual(observation["line"], 2)
                    self.assertEqual(observation["byte_offset"], len(first))
                    self.assertEqual(observation["byte_length"], len(raw))
                    self.assertEqual(observation["sha256"], hashlib.sha256(raw).hexdigest())
                    self.assertEqual(observation["fields"]["PGRAC_FAMILY"], family)
                    self.assertEqual(observation["fields"]["authoritative"], "0")
                    self.assertNotIn("log_sqlstate", observation["fields"])
                    self.assertEqual(result["field_root"], "OPEN")
                    self.assertFalse(result["identity_bound"])
                    self.assertFalse(result["ready_to_run"])

    def test_verbose_prefix_keeps_cursor_and_legacy_emitter_boundaries(self):
        payloads = (message(),
                    "PGRAC_FAMILY=R4_HEAP_PRUNE event=observed event_seq=1 "
                    "node=1 pid=101 authoritative=0 trace_sequence_exhausted=0",
                    "PGRAC_FAMILY=R4_BTREE_DELETE event=delete.applied event_seq=1 "
                    "node=1 pid=101 authoritative=0 after_scn=44conflict_horizon_known=1 "
                    "trace_sequence_exhausted=0")
        for payload in payloads:
            with self.subTest(payload=payload):
                decorated = "00000: " + payload + " at character 14"
                result = self.read(log(decorated))
                self.assertEqual(result["parse_status"], "PARSED")
                observation = result["observations"][0]
                self.assertEqual(observation["message"], decorated)
                self.assertEqual(observation["log_sqlstate"], "00000")
                self.assertEqual(observation["log_cursor_position"], "14")
                fields = observation["fields"]
                self.assertNotIn("log_cursor_position", fields)
                if fields["PGRAC_FAMILY"] == "R4_BTREE_DELETE":
                    self.assertEqual(fields["after_scn"], "44")
                    self.assertEqual(fields["conflict_horizon_known"], "1")
                if fields["PGRAC_FAMILY"] != "HOT_FULL":
                    self.assertNotIn("node_id", fields)
                    self.assertNotIn("sequence_domain", fields)

    def test_verbose_prefix_must_be_exact_and_at_message_start(self):
        prefixes = ("0000: ", "000000: ", "53r9G: ", "０００００: ", "00000:",
                    "00000:\t", "00000:  ", "00000 : ", "00000: 00000: ",
                    "unrelated 00000: ")
        for prefix in prefixes:
            with self.subTest(prefix=prefix):
                result = self.read(log(prefix + message()))
                self.assertEqual(result["parse_status"], "NOT_OBSERVED")
                self.assertEqual(result["ignored_lines"], 1)
                self.assertFalse(result["observations"])
                self.assertFalse(result["unhandled"])
                self.assertFalse(result["rejections"])

    def test_verbose_prefix_keeps_unsupported_and_non_log_boundaries(self):
        unsupported = log("00000: PGRAC_FAMILY=E_OTHER event=observed")
        raw = unsupported + b"".join(
            log("00000: " + message(), prefix=severity + ":  ")
            for severity in ("DETAIL", "ERROR", "FATAL", "STATEMENT"))
        result = self.read(raw)
        self.assertEqual(result["parse_status"], "NOT_OBSERVED")
        self.assertEqual(len(result["unhandled"]), 1)
        self.assertEqual(result["unhandled"][0]["family"], "E_OTHER")
        self.assertEqual(result["unhandled"][0]["raw"].encode(), unsupported)
        self.assertEqual(result["ignored_lines"], 4)
        self.assertFalse(result["observations"])
        self.assertFalse(result["rejections"])

    def test_verbose_prefix_does_not_hide_invalid_or_partial_emitters(self):
        payloads = (message().replace("pid=101", "pid=bad"),
                    message().replace("authoritative=0", "authoritative=1"),
                    message().replace("trace_sequence_exhausted=0", ""),
                    message() + " extra=1 at character 14")
        records = [log("00000: " + payload) for payload in payloads]
        records.extend((log("00000: " + message()).rstrip(b"\n"),
                        log("00000: " + message()).replace(b"event=observed", b"event=\xff")))
        for raw in records:
            with self.subTest(raw=raw):
                result = self.read(raw)
                self.assertEqual(result["parse_status"], "REJECTED")
                self.assertFalse(result["observations"])
                self.assertEqual(result["rejections"][0]["raw_hex"], raw.hex())
                self.assertEqual(result["rejections"][0]["sha256"],
                                 hashlib.sha256(raw).hexdigest())

    def test_verbose_and_plain_records_keep_order_without_inventing_metadata(self):
        raw = (log("00000: " + message(seq="9")) + log(message(seq="2")) +
               log("53R9G: " + message(seq="9")))
        result = self.read(raw)
        self.assertEqual(result["parse_status"], "PARSED")
        observations = result["observations"]
        self.assertEqual([item["fields"]["event_seq"] for item in observations],
                         ["9", "2", "9"])
        self.assertEqual([item.get("log_sqlstate") for item in observations],
                         ["00000", None, "53R9G"])
        self.assertNotIn("log_sqlstate", observations[1])
        self.assertNotIn("events", result)
        self.assertNotIn("sequences", result)

    def test_malformed_records_retain_provenance_and_cannot_parse(self):
        valid = message()
        bad = [valid + " pid=102", valid.replace("pid=101", "pid="),
               valid.replace("pid=101", "pid=1node=2"),
               valid.replace("pid=101", "pid=wrong"),
               valid.replace("event_seq=1", "event_seq=0"),
               valid.replace("event_seq=1", "event_seq=18446744073709551616"),
               valid.replace("sequence_scope=process", "sequence_scope=global"),
               valid.replace("sequence_domain=HOT_FULL", "sequence_domain=READ_IMAGE"),
               valid.replace("sequence_domain=HOT_FULL ", ""),
               valid.replace("authoritative=0 ", ""),
               valid.replace("trace_sequence_exhausted=0", ""),
               valid + " trailing", valid + " broken={a=1", valid + " broken=x}",
               valid + " broken={a=1}suffix", valid.replace("pid=101", "pid=101\x00")]
        for payload in bad:
            with self.subTest(payload=payload):
                raw = log(payload)
                result = self.read(raw)
                self.assertEqual(result["parse_status"], "REJECTED")
                self.assertFalse(result["observations"])
                self.assertEqual(result["rejections"][0]["raw_hex"], raw.hex())
                self.assertEqual(result["rejections"][0]["sha256"],
                                 hashlib.sha256(raw).hexdigest())

    def test_end_of_record_marker_must_be_last(self):
        result = self.read(log(message() + " another=field"))
        self.assertEqual(result["parse_status"], "REJECTED")

    def test_postgres_cursor_suffix_preserves_raw_and_emitter_boundary(self):
        payloads = [message(family) for family in
                    ("HOT_FULL", "READ_IMAGE", "CTRC_CONSUME", "CTRC_ORIGIN_BATCH_REPLY")]
        payloads.append("PGRAC_FAMILY=R4_HEAP_PRUNE event=observed event_seq=1 "
                        "node=1 pid=101 authoritative=0 trace_sequence_exhausted=0")
        for payload in payloads:
            for position in ("14", "2147483647"):
                with self.subTest(payload=payload, position=position):
                    decorated = payload + " at character " + position
                    raw = log(decorated)
                    result = self.read(raw)
                    self.assertEqual(result["parse_status"], "PARSED")
                    observation = result["observations"][0]
                    self.assertEqual(observation["raw"].encode(), raw)
                    self.assertEqual(observation["message"], decorated)
                    self.assertEqual(observation["sha256"], hashlib.sha256(raw).hexdigest())
                    self.assertEqual(observation["byte_length"], len(raw))
                    self.assertEqual(observation["log_cursor_position"], position)
                    self.assertEqual(observation["fields"]["pid"], "101")
                    self.assertEqual(observation["fields"]["authoritative"], "0")
                    self.assertNotIn("log_cursor_position", observation["fields"])
                    self.assertFalse(result["identity_bound"])
                    self.assertFalse(result["ready_to_run"])

    def test_cursor_suffix_does_not_hide_malformed_emitter_or_trailing_text(self):
        valid = message()
        suffixes = (" at character 0", " at character -1", " at character 01",
                    " at character 2147483648", " at character 999999999999999",
                    " at character x", " at character 14 trailing",
                    " at character 14 another=field", " at character 14 at character 15",
                    " at character 14 ", "\tat character 14", " at character １４")
        payloads = [valid + suffix for suffix in suffixes]
        payloads.extend((valid.replace("trace_sequence_exhausted=0", "") +
                         " at character 14", valid + " another=field at character 14",
                         valid.replace("pid=101", "pid=bad") + " at character 14"))
        for payload in payloads:
            with self.subTest(payload=payload):
                raw = log(payload)
                result = self.read(raw)
                self.assertEqual(result["parse_status"], "REJECTED")
                self.assertFalse(result["observations"])
                self.assertEqual(result["rejections"][0]["raw_hex"], raw.hex())

    def test_partial_line_and_invalid_utf8_are_preserved(self):
        for raw in (log().rstrip(b"\n"), log().replace(b"event=observed", b"event=\xff")):
            with self.subTest(raw=raw):
                result = self.read(raw)
                self.assertEqual(result["parse_status"], "REJECTED")
                self.assertEqual(result["rejections"][0]["raw_hex"], raw.hex())

    def test_crlf_and_empty_brace_value(self):
        raw = log(message(extra="decisions={}")).replace(b"\n", b"\r\n")
        observation = self.read(raw)["observations"][0]
        self.assertEqual(observation["raw"].encode(), raw)
        self.assertEqual(observation["fields"]["decisions"], "{}")

    def test_empty_input_does_not_prove_anything(self):
        result = self.read(b"")
        self.assertEqual(result["parse_status"], "NOT_OBSERVED")
        self.assertEqual(result["field_root"], "OPEN")
        self.assertFalse(result["identity_bound"])
        self.assertFalse(result["ready_to_run"])

    def test_cli_reports_parse_status_separately_from_readiness(self):
        self.assertIsNotNone(workload_log, "raw structured LOG reader is missing")
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "server.log"
            for raw, rc, status in ((log(), 0, "PARSED"),
                                    (log("00000: " + message()), 0, "PARSED"),
                                    (log("00000: " + message()).rstrip(b"\n"), 1, "REJECTED"),
                                    (log().rstrip(b"\n"), 1, "REJECTED"),
                                    (b"nothing observed\n", 2, "NOT_OBSERVED")):
                with self.subTest(status=status):
                    path.write_bytes(raw)
                    run = subprocess.run([sys.executable, workload_log.__file__, str(path)],
                                         capture_output=True, text=True, check=False)
                    self.assertEqual(run.returncode, rc, run.stderr)
                    result = json.loads(run.stdout)
                    self.assertEqual(result["parse_status"], status)
                    self.assertFalse(result["ready_to_run"])
                    self.assertEqual(path.read_bytes(), raw)


if __name__ == "__main__":
    unittest.main()
