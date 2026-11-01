"""Check complete native empty-undo evidence without supplying product state.

Author: SqlRush <sqlrush@gmail.com>
"""
import copy
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import unittest

from empty_undo_preflight import evaluate


WRITES = ("zero_update", "empty_copy", "conflict_nothing", "zero_row_lock")


def perl_bridge(function, sample):
    # Execute only the original Perl function, never the native cluster body.
    source = Path(__file__).resolve().parents[1] / "t/482_pre2_undo_active_slots.pl"
    matches = re.findall(r"(?ms)^sub " + function + r"\n\{.*?^\}\n", source.read_text())
    if len(matches) != 1:
        raise AssertionError("native Perl function extraction is incomplete")
    prefix = r'''
use strict;
use warnings;
use JSON::PP;
use FindBin;
use IPC::Run qw(run timeout);
my $input = decode_json(do { local $/; <STDIN> });
my @saved;
sub save_json { push @saved, [@_]; }
sub counters { return $input->{counters}; }
sub empty_observation { return $input->{observation}; }
{
    package EmptyClient;
    sub query { return ($input->{output}, $input->{error}); }
}
'''
    if function == "evaluate_sample":
        suffix = r'''
print encode_json(evaluate_sample($input, 'empty_undo_preflight.py'));
'''
    else:
        suffix = r'''
my $client = bless {stderr => $input->{stderr}}, 'EmptyClient';
my $sample;
my $ok = eval { $sample = empty_statement('offline', {client => $client},
                                         'SELECT 1;', $input->{trigger}); 1; };
print encode_json({sample => $sample, failure => $ok ? '' : $@,
                   stderr_after => $client->{stderr}, saved => \@saved});
'''
    result = subprocess.run(["perl", "-e", prefix + matches[0] + suffix],
                            input=json.dumps(sample), text=True, capture_output=True,
                            cwd=source.parent, env=dict(os.environ, PYTHON=sys.executable),
                            timeout=20, check=True)
    if result.stderr:
        raise AssertionError(result.stderr)
    return json.loads(result.stdout)


def evidence(kind="zero_update", finish="commit"):
    # Literal native field contract. FullXid epoch 1 maps to raw xid 200.
    counters = dict(tt_capacity_stats_available="true", tt_capacity_origin_node_id="0",
                    tt_slot_active_full_wait_count="18446744073709551610",
                    tt_capacity_pool_wait_count="2", tt_capacity_cancel_count="3",
                    segment_switch_count="4", autoextend_count="5",
                    segment_reuse_count="6", record_alloc_count="100")
    before = dict(pid=7000, top="none",
                  binding=dict(available=False, wait_scope=False), counters=counters,
                  snapshot=dict(available=True, origin=0, segment_id=9,
                                binding_generation="18446744073709551614",
                                slots=[dict(offset=i, xid=0, wrap=7, status=0,
                                            commit_scn="0") for i in range(48)]))
    first = copy.deepcopy(before)
    after = copy.deepcopy(before)
    if kind not in ("pure_select", "explain_only"):
        first["top"] = "4294967496"
        first["binding"] = dict(available=True, wait_scope=False, pid=7000, xid=200,
                                origin=0, segment_id=9, segment_generation=0,
                                slot_offset=3, slot_wrap=7,
                                active_lsn="18446744073709551614")
        first["snapshot"]["slots"][3].update(xid=200, status=1)
        after["snapshot"]["slots"][3]["wrap"] = 8
    first["result"] = dict(error=0, sqlstate="00000")
    repeated = copy.deepcopy(first)
    if kind == "statement_trigger_error":
        first["result"] = dict(error=1, sqlstate="P0001")
        repeated = None
    return dict(kind=kind, finish=finish, pid=7000, before=before, first=first,
                repeated=repeated, after=after, data=["data|1|100"] * 4)


class EmptyUndoEvidenceTests(unittest.TestCase):
    def passes(self, sample):
        result = evaluate(sample)
        self.assertIs(result["passed"], True, result)

    def rejects(self, sample):
        result = evaluate(sample)
        self.assertIs(result["passed"], False, result)
        self.assertTrue(result["reason"])

    def test_all_four_success_entries_keep_one_binding_through_commit_and_abort(self):
        for kind in WRITES:
            for finish in ("commit", "abort"):
                with self.subTest(kind=kind, finish=finish):
                    self.passes(evidence(kind, finish))

    def test_trigger_observes_real_binding_before_expected_error_and_abort(self):
        self.passes(evidence("statement_trigger_error", "abort"))

    def test_pure_select_and_explain_never_assign_a_top_or_binding(self):
        for kind in ("pure_select", "explain_only"):
            self.passes(evidence(kind))
            sample = evidence(kind)
            sample["first"]["top"] = "4294967496"
            self.rejects(sample)
            sample = evidence(kind)
            sample["first"]["binding"]["available"] = True
            self.rejects(sample)

    def test_missing_wrong_or_replaced_top_binding_is_not_early_publication(self):
        faults = (("available", False), ("wait_scope", True), ("pid", 7001),
                  ("xid", 201), ("origin", 1), ("segment_id", 10),
                  ("segment_generation", 4294967295), ("slot_offset", 48),
                  ("slot_wrap", 65535), ("active_lsn", "0"))
        for field, value in faults:
            sample = evidence()
            sample["first"]["binding"][field] = value
            self.rejects(sample)
        for field, value in (("top", "200.0"), ("top", "none"), ("pid", 7001)):
            sample = evidence()
            sample["first"][field] = value
            self.rejects(sample)
        sample = evidence()
        sample["repeated"]["binding"]["active_lsn"] = "8192"
        self.rejects(sample)

    def test_only_the_exact_one_actual_slot_may_change_while_bound(self):
        for offset, changes in ((3, dict(xid=201)), (3, dict(wrap=8)),
                                (3, dict(status=2)), (3, dict(commit_scn="1")),
                                (4, dict(xid=201, status=1)), (4, dict(wrap=8)),
                                (47, dict(offset=0))):
            sample = evidence()
            sample["first"]["snapshot"]["slots"][offset].update(changes)
            self.rejects(sample)
        for field, value in (("available", False), ("origin", False),
                             ("segment_id", 9.0), ("segment_id", 10),
                             ("binding_generation", "2"), ("slots", [])):
            sample = evidence()
            sample["first"]["snapshot"][field] = value
            self.rejects(sample)

    def test_undo_emission_wait_supply_or_unavailable_counters_cannot_pass(self):
        for phase in ("first", "repeated", "after"):
            for key in ("record_alloc_count", "tt_slot_active_full_wait_count",
                        "tt_capacity_pool_wait_count", "tt_capacity_cancel_count",
                        "segment_switch_count", "autoextend_count", "segment_reuse_count"):
                sample = evidence()
                sample[phase]["counters"][key] = str(int(sample[phase]["counters"][key]) + 1)
                self.rejects(sample)
            for key, value in (("tt_capacity_stats_available", "false"),
                               ("tt_capacity_origin_node_id", "1"),
                               ("record_alloc_count", None),
                               ("record_alloc_count", "18446744073709551616")):
                sample = evidence()
                sample[phase]["counters"][key] = value
                self.rejects(sample)
        sample = evidence()
        del sample["before"]["counters"]["record_alloc_count"]
        self.rejects(sample)

    def test_terminal_cleanup_requires_same_backend_and_real_all_free_current(self):
        for field, value in (("top", "4294967496"), ("pid", 7001),
                             ("binding", dict(available=False, wait_scope=True))):
            sample = evidence()
            sample["after"][field] = value
            self.rejects(sample)
        for field, value in (("xid", 200), ("status", 1), ("status", 3),
                             ("commit_scn", "91")):
            sample = evidence()
            sample["after"]["snapshot"]["slots"][3][field] = value
            self.rejects(sample)
        sample = evidence()
        sample["after"]["snapshot"]["binding_generation"] = "2"
        self.rejects(sample)

    def test_expected_trigger_error_is_not_an_arbitrary_error_or_workload_success(self):
        for kind, error, state in (("zero_update", 1, "57014"),
                                    ("zero_update", 0, "0A000"),
                                    ("zero_update", True, "00000"),
                                    ("statement_trigger_error", 1, "57014"),
                                    ("statement_trigger_error", 0, "00000")):
            sample = evidence(kind, "abort")
            sample["first"]["result"] = dict(error=error, sqlstate=state)
            self.rejects(sample)
        sample = evidence("statement_trigger_error", "commit")
        self.rejects(sample)
        sample = evidence("statement_trigger_error", "abort")
        sample["repeated"] = copy.deepcopy(sample["first"])
        self.rejects(sample)

    def test_all_four_members_must_agree_with_the_unchanged_data_ledger(self):
        for value in ([], ["data|1|100"] * 3, ["data|1|100"] * 5,
                      ["data|1|100"] * 3 + ["data|1|101"],
                      ["data|1|100\ntrigger|2|100"] * 4):
            sample = evidence()
            sample["data"] = value
            self.rejects(sample)

    def test_unknown_missing_and_malformed_evidence_is_not_success(self):
        for value in (None, [], {}, dict(kind="unknown")):
            self.rejects(value)
        for field, value in (("before", None), ("first", []), ("repeated", None),
                             ("after", "done"), ("finish", "skip"), ("pid", True)):
            sample = evidence()
            sample[field] = value
            self.rejects(sample)

    def test_actual_perl_json_bridge_preserves_uint64_and_refuses_changed_undo_count(self):
        sample = evidence()
        self.assertIs(perl_bridge("evaluate_sample", sample)["passed"], True)
        sample["after"]["counters"]["record_alloc_count"] = "101"
        self.assertIs(perl_bridge("evaluate_sample", sample)["passed"], False)

    def test_actual_perl_statement_reader_requires_one_pre_error_notice_and_native_error(self):
        first = evidence("statement_trigger_error", "abort")["first"]
        notice = {key: value for key, value in first.items() if key not in ("result", "counters")}
        line = "NOTICE:  00000: G_EMPTY_OBSERVATION " + json.dumps(notice) + "\n"
        sample = dict(trigger=1, output="G_EMPTY_SQLSTATE P0001", error=1,
                      counters=first["counters"],
                      stderr=line + "ERROR:  P0001: G_EMPTY_EXPECTED_TRIGGER_ERROR\n")
        result = perl_bridge("empty_statement", sample)
        self.assertEqual(result["sample"], first)
        self.assertEqual(result["failure"], "")
        self.assertEqual(result["stderr_after"], "")
        self.assertEqual(result["saved"][0][1]["stderr"], sample["stderr"])
        for field, value in (("output", "G_EMPTY_SQLSTATE 57014"),
                             ("output", "G_EMPTY_SQLSTATE P0001\nG_EMPTY_SQLSTATE P0001"),
                             ("stderr", "ERROR:  P0001: G_EMPTY_EXPECTED_TRIGGER_ERROR\n"),
                             ("stderr", line * 2 + "ERROR:  P0001: G_EMPTY_EXPECTED_TRIGGER_ERROR\n"),
                             ("stderr", line + "ERROR:  57014: timeout\n")):
            bad = dict(sample)
            bad[field] = value
            result = perl_bridge("empty_statement", bad)
            self.assertTrue(result["failure"])
            self.assertIsNone(result["sample"])


if __name__ == "__main__":
    unittest.main()
