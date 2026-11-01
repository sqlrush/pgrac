"""Pin actual-slot resume, repeated binding and original retirement evidence.

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

from full_preflight_resume import evaluate
from test_full_preflight_cancel import KINDS, SQL
from test_undo_slot_wait import blocked_sample


def evidence(kind="zero_update", mode="commit"):
    blocked = blocked_sample()
    first = dict(pid=9000, top="500", counters=copy.deepcopy(blocked["counters_after"]),
                 binding=dict(available=True, wait_scope=False, pid=9000, xid=500,
                              origin=0, segment_id=9, segment_generation=0,
                              slot_offset=0, slot_wrap=8, active_lsn="9000"),
                 snapshot=copy.deepcopy(blocked["before"]["snapshot"]),
                 result=dict(error=0, sqlstate="00000"))
    first["snapshot"]["slots"][0].update(xid=500, wrap=8)
    repeated = copy.deepcopy(first)
    if kind == "statement_trigger_error":
        first["result"] = dict(error=1, sqlstate="P0001")
        repeated = None
    after = copy.deepcopy(first)
    after.update(top="none", binding=dict(available=False, wait_scope=False))
    after["snapshot"]["slots"][0].update(status=0, xid=0, wrap=9)
    after.pop("result")
    return dict(kind=kind, mode=mode, blocked=blocked, first=first, repeated=repeated,
                after=after, remaining=copy.deepcopy(blocked["before"]["bindings"][1:]),
                data=["data|1|100"] * 4)


def perl_controller(sample, fault="", poll=False):
    source = Path(__file__).resolve().parents[1] / "t/482_pre2_undo_active_slots.pl"
    text = source.read_text()
    names = (("wait_preflight_retirement",) if poll else
             ("end_owner", "empty_statement", "resume_full_preflight"))
    functions = []
    for name in names:
        matches = re.findall(r"(?ms)^sub " + name + r"\n\{.*?^\}\n", text)
        if len(matches) != 1:
            raise AssertionError("native function extraction incomplete: " + name)
        functions.append(matches[0])
    payload = dict(sample=sample, fault=fault, sql=SQL[KINDS.index(sample["kind"])])
    prefix = r'''
use strict;
use warnings;
use JSON::PP;
use FindBin;
use IPC::Run qw(run timeout);
use subs qw(sleep);
my $input = decode_json(do { local $/; <STDIN> });
my $writer;
my (@trace, @saved, @expected);
my @nodes;
my $observations = 0;
my $tick = 0;
use constant CLOCK_MONOTONIC => 1;
sub clock_gettime { $tick += $input->{fault} eq 'timeout' ? 16 : 1; return $tick; }
sub sleep { }
sub save_json { push @saved, [@_]; }
sub pass { push @trace, 'pass'; }
sub binding { return $_[0]{binding}; }
sub counters { return $input->{sample}{first}{counters}; }
sub empty_observation {
    ++$observations;
    return $input->{sample}{after} if $input->{poll} && $observations > 1;
    return $input->{sample}{first};
}
sub wait_preflight_retirement {
    die "OFFLINE_RETIREMENT_FAILURE\n" if $input->{fault} eq 'retirement';
    push @trace, 'retired';
    return $input->{sample}{after};
}
sub evaluate_sample {
    my ($sample, $script) = @_;
    die "wrong evaluator\n" unless $script eq 'full_preflight_resume.py';
    push @trace, 'evaluate';
    my ($out, $err) = ('', '');
    my $json = encode_json($sample);
    run([$ENV{PYTHON}, "$FindBin::RealBin/../pre2/$script"], '<', \$json,
        '>', \$out, '2>', \$err, timeout(10)) or die $err;
    return decode_json($out);
}
{
    package ResumeClient;
    sub query_safe {
        my ($self, $sql) = @_;
        push @trace, $self->{role} . ':' . $sql;
        return '';
    }
    sub query {
        my ($self, $sql) = @_;
        push @trace, $sql eq '' ? 'drain' : 'repeat:' . $sql;
        die "OFFLINE_DRAIN_FAILURE\n" if $input->{fault} eq 'drain';
        return ("G_EMPTY_SQLSTATE 57014\n", 1) if $input->{fault} eq 'state';
        if ($input->{sample}{kind} eq 'statement_trigger_error') {
            my %notice = %{$input->{sample}{first}};
            delete $notice{result}; delete $notice{counters};
            $self->{stderr} = 'NOTICE: G_EMPTY_OBSERVATION ' . JSON::PP::encode_json(\%notice)
              . "\nERROR: P0001: G_EMPTY_EXPECTED_TRIGGER_ERROR\n";
            return ("G_EMPTY_SQLSTATE P0001\n", 1);
        }
        return ("G_EMPTY_SQLSTATE 00000\n", 0);
    }
}
{
    package ResumeReader;
    sub safe_psql { return 'data|1|100'; }
}
'''
    # The poll test uses the real poll helper, not its completion-test IO leaf.
    if poll:
        prefix = re.sub(r"(?ms)^sub wait_preflight_retirement \{.*?^\}\n", "", prefix)
        suffix = r'''
$input->{poll} = 1;
my $ok = eval { wait_preflight_retirement('offline', {}, 0); 1; };
print encode_json({failure => $ok ? '' : $@, saved => \@saved, observations => $observations});
'''
    else:
        suffix = r'''
my $sample = $input->{sample};
my $chosen = {pid => 1000, table => 0, trial => 100, transaction => 1,
              client => bless({role => 'owner'}, 'ResumeClient')};
my $waiter = {pid => 9000, transaction => 1, pending => 1,
              client => bless({role => 'waiter', stderr => ''}, 'ResumeClient')};
my @owners = ($chosen, map {{pid => $_->{pid}, binding => $_}} @{$sample->{remaining}});
@nodes = map {bless {}, 'ResumeReader'} 0 .. 3;
my $ok = eval { resume_full_preflight('offline', $sample->{mode}, \@owners, $chosen,
                                    $waiter, $sample->{blocked},
                                    [$sample->{kind}, $input->{sql}]); 1; };
print encode_json({failure => $ok ? '' : $@, trace => \@trace, saved => \@saved,
                   expected => \@expected, owner_transaction => $chosen->{transaction},
                   pending => $waiter->{pending}, transaction => $waiter->{transaction}});
'''
    result = subprocess.run(["perl", "-e", prefix + "\n".join(functions) + suffix],
                            input=json.dumps(payload), text=True, capture_output=True,
                            cwd=source.parent, env=dict(os.environ, PYTHON=sys.executable),
                            timeout=20, check=True)
    if result.stderr:
        raise AssertionError(result.stderr)
    return json.loads(result.stdout)


class FullPreflightResumeTests(unittest.TestCase):
    def passes(self, sample):
        result = evaluate(sample)
        self.assertIs(result["passed"], True, result)

    def rejects(self, sample):
        result = evaluate(sample)
        self.assertIs(result["passed"], False, result)
        self.assertTrue(result["reason"])

    def test_all_five_callers_resume_after_both_owner_terminal_operations(self):
        for kind in KINDS:
            for mode in ("commit", "abort"):
                with self.subTest(kind=kind, mode=mode):
                    self.passes(evidence(kind, mode))

    def test_native_scheduler_reaches_all_ten_cases_with_distinct_original_trials(self):
        source = Path(__file__).resolve().parents[1] / "t/482_pre2_undo_active_slots.pl"
        functions = re.findall(r"(?ms)^sub run_full_preflight_resumes\n\{.*?^\}\n", source.read_text())
        self.assertEqual(len(functions), 1)
        program = ("use strict; use warnings; use JSON::PP; my @cases; "
                   "sub run_case { push @cases, [@_]; }\n" + functions[0] +
                   "run_full_preflight_resumes(); print encode_json(\\@cases);")
        result = subprocess.run(["perl", "-e", program], text=True, capture_output=True,
                                timeout=10, check=True)
        self.assertEqual(result.stderr, "")
        expected = [[mode, 100 + 2 * index + terminal, [kind, SQL[index]]]
                    for index, kind in enumerate(KINDS)
                    for terminal, mode in enumerate(("commit", "abort"))]
        self.assertEqual(json.loads(result.stdout), expected)

    def test_wait_must_be_original_exact_vxid_and_allocator_stays_full(self):
        for field, value in (("blockers", [1001]), ("wait_event", "Lock")):
            sample = evidence(); sample["blocked"]["activity"][field] = value
            self.rejects(sample)

    def test_only_selected_retired_slot_can_supply_same_native_xid(self):
        for field, value in (("slot_offset", 1), ("slot_wrap", 7), ("xid", 501),
                             ("segment_generation", 1), ("pid", 1000),
                             ("available", False), ("wait_scope", True)):
            sample = evidence(); sample["first"]["binding"][field] = value
            self.rejects(sample)
        for field, value in (("top", "501"), ("pid", True)):
            sample = evidence(); sample["first"][field] = value; self.rejects(sample)

    def test_other47_slots_bindings_and_current_identity_never_change(self):
        for phase in ("first", "repeated", "after"):
            for field, value in (("status", 0), ("xid", 333), ("wrap", 8)):
                sample = evidence(); sample[phase]["snapshot"]["slots"][1][field] = value
                self.rejects(sample)
            sample = evidence(); sample[phase]["snapshot"]["binding_generation"] = "1"
            self.rejects(sample)
        sample = evidence(); sample["remaining"][0]["active_lsn"] = "8192"
        self.rejects(sample)

    def test_repeated_preflight_keeps_original_top_and_entire_receipt(self):
        for field, value in (("top", "4294967796"), ("binding", {}), ("snapshot", {})):
            sample = evidence(); sample["repeated"][field] = value; self.rejects(sample)

    def test_waiter_retirement_requires_free_slot_and_no_binding_top_or_scope(self):
        for field, value in (("status", 1), ("status", 2), ("status", 3),
                             ("xid", 500), ("wrap", True), ("commit_scn", "1")):
            sample = evidence(); sample["after"]["snapshot"]["slots"][0][field] = value
            self.rejects(sample)
        for field, value in (("top", "500"), ("binding", dict(available=False, wait_scope=True))):
            sample = evidence(); sample["after"][field] = value; self.rejects(sample)

    def test_every_counter_is_exact_and_no_data_undo_or_supply_is_allowed(self):
        for phase in ("first", "repeated", "after"):
            for key, value in evidence()[phase]["counters"].items():
                sample = evidence()
                sample[phase]["counters"][key] = "false" if value == "true" else str(int(value) + 1)
                self.rejects(sample)

    def test_first_resume_allows_original_terminal_cv_then_pins_success_counters(self):
        for kind in KINDS:
            sample = evidence(kind)
            sample["blocked"]["counters_before"]["tt_capacity_pool_wait_count"] = str((1 << 64) - 5)
            sample["blocked"]["counters_after"]["tt_capacity_pool_wait_count"] = str((1 << 64) - 5)
            for phase in ("first", "repeated", "after"):
                if sample[phase] is not None:
                    sample[phase]["counters"]["tt_capacity_pool_wait_count"] = str((1 << 64) - 2)
            self.passes(sample)
            for phase in (("after",) if kind == "statement_trigger_error" else ("repeated", "after")):
                wrong = copy.deepcopy(sample)
                wrong[phase]["counters"]["tt_capacity_pool_wait_count"] = str((1 << 64) - 1)
                self.rejects(wrong)
        sample = evidence()
        for phase in ("first", "repeated", "after"):
            sample[phase]["counters"]["tt_capacity_pool_wait_count"] = "1"
        self.rejects(sample)

    def test_sqlstate_trigger_repeat_and_four_peer_data_are_mandatory(self):
        for kind in KINDS:
            sample = evidence(kind); sample["first"]["result"] = dict(error=0, sqlstate="57014")
            self.rejects(sample)
        sample = evidence("statement_trigger_error"); sample["repeated"] = sample["first"]
        self.rejects(sample)
        for data in ([], ["data|1|100"] * 3, ["data|1|999"] * 4):
            sample = evidence(); sample["data"] = data; self.rejects(sample)

    def test_malformed_and_ambiguous_evidence_is_refused(self):
        for sample in (None, [], {}, {"kind": []}):
            self.rejects(sample)
        for key in evidence():
            sample = evidence(); del sample[key]; self.rejects(sample)

    def test_actual_perl_completion_releases_owner_drains_repeats_and_retires(self):
        for kind in KINDS:
            for mode in ("commit", "abort"):
                with self.subTest(kind=kind, mode=mode):
                    result = perl_controller(evidence(kind, mode))
                    self.assertEqual(result["failure"], "")
                    finish = "COMMIT" if mode == "commit" else "ROLLBACK"
                    waiter_finish = "ROLLBACK" if kind == "statement_trigger_error" else finish
                    trace = ["owner:" + finish, "drain"]
                    if kind != "statement_trigger_error":
                        trace.append("repeat:" + SQL[KINDS.index(kind)] +
                                     "\n\\echo G_EMPTY_SQLSTATE :SQLSTATE\n")
                    trace += ["waiter:" + waiter_finish, "retired", "evaluate", "pass"]
                    self.assertEqual(result["trace"], trace)
                    self.assertEqual((result["pending"], result["transaction"],
                                      result["owner_transaction"]), (0, 0, 0))
                    self.assertEqual(result["expected"], ["0|100"] if mode == "commit" else [])
                    self.assertTrue(dict(result["saved"])["offline-resume-result"]["passed"])

    def test_completion_first_error_stops_without_repeating_or_hiding_cleanup(self):
        for fault, reason, pending, transaction in (
                ("drain", "OFFLINE_DRAIN_FAILURE", 1, 1),
                ("state", "empty statement failed", 0, 1),
                ("retirement", "OFFLINE_RETIREMENT_FAILURE", 0, 0)):
            with self.subTest(fault=fault):
                result = perl_controller(evidence(), fault)
                self.assertIn(reason, result["failure"])
                self.assertEqual((result["pending"], result["transaction"]), (pending, transaction))
                self.assertNotIn("evaluate", result["trace"])
        sample = evidence(); sample["after"]["snapshot"]["slots"][1]["wrap"] = 8
        result = perl_controller(sample)
        self.assertIn("NOT_REACHED offline", result["failure"])
        self.assertIn("remaining47 actual slots changed", result["failure"])

    def test_poll_uses_actual_free_slot_and_preserves_timeout_evidence(self):
        result = perl_controller(evidence(), poll=True)
        self.assertEqual(result["failure"], "")
        self.assertEqual(result["observations"], 2)
        self.assertIn("offline-retirement-first", dict(result["saved"]))
        result = perl_controller(evidence(), "timeout", poll=True)
        self.assertIn("original waiter slot cleanup did not finish", result["failure"])
        self.assertIn("offline-retirement-last", dict(result["saved"]))


if __name__ == "__main__":
    unittest.main()
