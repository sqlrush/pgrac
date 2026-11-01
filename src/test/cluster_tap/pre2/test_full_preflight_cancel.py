"""Exercise the native full-slot caller controller with explicit IO leaves.

Author: SqlRush <sqlrush@gmail.com>
"""
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import unittest

from test_undo_slot_wait import blocked_sample


SQL = (
    "UPDATE g_empty_undo SET value=999 WHERE false;",
    "COPY g_empty_undo FROM STDIN;\n\\.\n",
    "INSERT INTO g_empty_undo VALUES(1,999) ON CONFLICT(id) DO NOTHING;",
    "SELECT * FROM g_empty_undo WHERE false FOR UPDATE;",
    "UPDATE g_empty_trigger SET value=999 WHERE false;",
)
KINDS = ("zero_update", "empty_copy", "conflict_nothing", "zero_row_lock",
         "statement_trigger_error")


def controller(fault=""):
    source = Path(__file__).resolve().parents[1] / "t/482_pre2_undo_active_slots.pl"
    text = source.read_text()
    functions = []
    for name in ("evaluate_sample", "judge", "counters", "binding", "census",
                 "lock_rows", "end_owner", "stable_supply", "cancel_waiter",
                 "run_full_preflight_cancellations"):
        matches = re.findall(r"(?ms)^sub " + name + r"\n\{.*?^\}\n", text)
        if len(matches) != 1:
            raise AssertionError("native function extraction is incomplete: " + name)
        functions.append(matches[0])
    keys = re.findall(r"(?s)^my @counter_keys = qw\(.*?\);", text, re.MULTILINE)
    if len(keys) != 1:
        raise AssertionError("native counter list extraction is incomplete")
    sample = blocked_sample()
    frames = []
    for case in range(5):
        before = dict(sample["counters_before"],
                      tt_slot_active_full_wait_count=str((1 << 64) - 20 + case),
                      tt_capacity_cancel_count=str((1 << 64) - 30 + case))
        waiting = dict(before, tt_slot_active_full_wait_count=str((1 << 64) - 19 + case))
        cancelled = dict(waiting, tt_capacity_cancel_count=str((1 << 64) - 29 + case))
        if fault == "counter" and case == 2:
            cancelled["tt_capacity_cancel_count"] = str((1 << 64) - 28 + case)
        frames.extend((before, waiting, cancelled))
    payload = dict(sample=sample, frames=frames, fault=fault)
    prefix = r'''
use strict;
use warnings;
use JSON::PP;
use FindBin;
use IPC::Run qw(run timeout);
my $input = decode_json(do { local $/; <STDIN> });
my $writer;
my (@saved, @sent, @started, @signals, @judged);
my $index = -1;
my $entry;
sub save_json { push @saved, [@_]; }
sub pass { push @judged, $_[0]; }
sub is { die "offline equality failed: $_[2]\n" unless $_[0] eq $_[1]; }
sub is_deeply {
    my $json = JSON::PP->new->canonical;
    die "offline deep equality failed: $_[2]\n"
      unless $json->encode($_[0]) eq $json->encode($_[1]);
}
sub wait_witness {
    my ($name, $waiter) = @_;
    die "waiter was not pending\n" unless $waiter->{pending} && $waiter->{transaction};
    die "OFFLINE_WITNESS_FAILURE\n" if $input->{fault} eq 'witness' && $index == 2;
    my %activity = %{$input->{sample}{activity}};
    $activity{xid} = '' . (500 + $index);
    $activity{blockers} = [1001] if $input->{fault} eq 'judge' && $index == 2;
    return \%activity;
}
{
    package FullWriter;
    sub safe_psql {
        my ($self, $database, $sql) = @_;
        if ($sql =~ /^SELECT pg_cancel_backend\(9000\)$/) {
            push @signals, $index;
            return 'f' if $input->{fault} eq 'cancel' && $index == 2;
            return 't';
        }
        return JSON::PP::encode_json(shift @{$input->{frames}})
          if $sql =~ /^SELECT json_object_agg/;
        return JSON::PP::encode_json($input->{sample}{before}{snapshot})
          if $sql eq 'SELECT test_pgrac_undo_slot_snapshot()';
        return JSON::PP::encode_json($input->{sample}{locks}) if $sql =~ /FROM pg_locks/;
        die "unexpected observer SQL: $sql\n";
    }
}
{
    package FullClient;
    sub query_safe {
        my ($self, $sql) = @_;
        return JSON::PP::encode_json($self->{binding})
          if $sql eq 'SELECT test_pgrac_undo_slot_binding()';
        if ($sql eq 'BEGIN; SELECT pg_current_xact_id()::text') {
            die "previous caller not cleared\n" if $entry->{pending};
            ++$index;
            push @started, $self->{pid};
            return '' . (500 + $index);
        }
        return '' if $sql eq 'ROLLBACK';
        return '' . $self->{pid} if $sql eq 'SELECT pg_backend_pid()';
        die "unexpected caller SQL: $sql\n";
    }
    sub query_until {
        my ($self, $pattern, $sql) = @_;
        push @sent, {sql => $sql, pending => $entry->{pending},
                     transaction => $entry->{transaction}};
        return "G_SLOT_SENT\n";
    }
    sub query {
        my ($self, $sql) = @_;
        die "unexpected completion query\n" unless $sql eq '';
        $self->{stderr} = "ERROR: canceling statement due to user request\n";
        return ("G_SLOT_SQLSTATE 57014\n", 1);
    }
}
'''
    suffix = r'''
$writer = bless {}, 'FullWriter';
my @owners = map { {pid => $_->{pid}, client => bless({pid => $_->{pid}, binding => $_},
                                                   'FullClient')} }
             @{$input->{sample}{before}{bindings}};
my $client = bless {pid => 9000, stderr => '',
                   binding => {available => JSON::PP::false, wait_scope => JSON::PP::false}},
                   'FullClient';
$entry = {client => $client, pid => 9000, transaction => 0, pending => 0};
my $ok = eval { run_full_preflight_cancellations('native', \@owners, $owners[0],
                                               $entry, $input->{sample}{before}); 1; };
print encode_json({failure => $ok ? '' : $@, started => \@started, sent => \@sent,
                   signals => \@signals, judged => \@judged, saved => \@saved,
                   frames_left => scalar @{$input->{frames}},
                   pending => $entry->{pending}, transaction => $entry->{transaction}});
'''
    result = subprocess.run(["perl", "-e", prefix + keys[0] + "\n" +
                             "\n".join(functions) + suffix], input=json.dumps(payload),
                            text=True, capture_output=True, cwd=source.parent,
                            env=dict(os.environ, PYTHON=sys.executable), timeout=25,
                            check=True)
    if result.stderr:
        raise AssertionError(result.stderr)
    return json.loads(result.stdout)


class FullPreflightCancelTests(unittest.TestCase):
    def test_five_real_sql_callers_join_original_judge_and_exact_cancel_cleanup(self):
        result = controller()
        self.assertEqual(result["failure"], "")
        self.assertEqual(result["started"], [9000] * 5)
        self.assertEqual(result["signals"], list(range(5)))
        self.assertEqual(result["frames_left"], 0)
        self.assertEqual((result["pending"], result["transaction"]), (0, 0))
        self.assertEqual([row["sql"] for row in result["sent"]],
                         ["\\echo G_SLOT_SENT\n" + sql +
                          "\n\\echo G_SLOT_SQLSTATE :SQLSTATE\n" for sql in SQL])
        self.assertTrue(all(row["pending"] == row["transaction"] == 1
                            for row in result["sent"]))
        saved = dict(result["saved"])
        for kind in KINDS:
            name = "native-full-" + kind
            self.assertTrue(saved[name + "-blocked-result"]["passed"])
            self.assertEqual(saved[name + "-blocked-result"]["selected_pid"], 1000)
            completion = saved[name + "-completion"]
            self.assertEqual(completion["cancel"], "t")
            self.assertIn("canceling statement due to user request", completion["stderr"])

    def test_missing_wait_propagates_first_error_and_keeps_pending_for_cleanup(self):
        result = controller("witness")
        self.assertIn("OFFLINE_WITNESS_FAILURE", result["failure"])
        self.assertEqual(len(result["started"]), 3)
        self.assertEqual(result["signals"], [0, 1])
        self.assertEqual((result["pending"], result["transaction"]), (1, 1))

    def test_wrong_vxid_is_rejected_by_original_evaluator_before_cancel(self):
        result = controller("judge")
        self.assertIn("NOT_REACHED native-full-conflict_nothing-blocked", result["failure"])
        self.assertIn("not the exact native owner wait", result["failure"])
        self.assertEqual(len(result["started"]), 3)
        self.assertEqual(result["signals"], [0, 1])
        self.assertEqual((result["pending"], result["transaction"]), (1, 1))

    def test_failed_cancel_stops_next_caller_and_retains_transaction_cleanup(self):
        result = controller("cancel")
        self.assertIn("caller cancellation did not preserve native 57014", result["failure"])
        self.assertEqual(len(result["started"]), 3)
        self.assertEqual(result["signals"], [0, 1, 2])
        self.assertEqual((result["pending"], result["transaction"]), (0, 1))
        self.assertIn("native-full-conflict_nothing-completion", dict(result["saved"]))

    def test_cancel_counter_error_uses_original_uint64_check_and_stops_next_caller(self):
        result = controller("counter")
        self.assertIn("native cancellation count mismatch", result["failure"])
        self.assertEqual(len(result["started"]), 3)
        self.assertEqual(result["signals"], [0, 1, 2])
        self.assertEqual((result["pending"], result["transaction"]), (0, 0))
        self.assertIn("native-full-conflict_nothing-cancelled-counters", dict(result["saved"]))


if __name__ == "__main__":
    unittest.main()
