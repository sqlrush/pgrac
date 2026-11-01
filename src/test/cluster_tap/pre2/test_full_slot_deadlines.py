"""Run the native deadline controller with explicit IO boundaries.

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


def controller(fault=""):
    source = Path(__file__).resolve().parents[1] / "t/482_pre2_undo_active_slots.pl"
    text = source.read_text()
    functions = []
    for name in ("evaluate_sample", "judge", "counters", "binding", "census",
                 "lock_rows", "end_owner", "stable_supply", "run_full_slot_deadlines"):
        matches = re.findall(r"(?ms)^sub " + name + r"\n\{.*?^\}\n", text)
        if len(matches) != 1:
            raise AssertionError("native function extraction incomplete: " + name)
        functions.append(matches[0])
    keys = re.findall(r"(?ms)^my @counter_keys = qw\(.*?\);", text)
    if len(keys) != 1:
        raise AssertionError("native counter extraction incomplete")
    sample = blocked_sample()
    frames = []
    for index in range(2):
        before = dict(sample["counters_before"],
                      tt_slot_active_full_wait_count=str((1 << 64) - 20 + index),
                      tt_capacity_cancel_count=str((1 << 64) - 30 + index))
        blocked = dict(before, tt_slot_active_full_wait_count=str((1 << 64) - 19 + index))
        after = dict(blocked, tt_capacity_cancel_count=str((1 << 64) - 30 + 2 * index))
        if fault == "counter" and index == 0:
            after["tt_capacity_cancel_count"] = str((1 << 64) - 29)
        frames.extend((before, blocked, after))
    payload = dict(sample=sample, frames=frames, fault=fault)
    prefix = r'''
use strict;
use warnings;
use JSON::PP;
use FindBin;
use IPC::Run qw(run timeout);
my $input = decode_json(do { local $/; <STDIN> });
my $writer;
my (@saved, @sent, @started, @settings, @judged);
my ($index, $completed) = (-1, 0);
my $entry;
sub save_json { push @saved, [@_]; }
sub pass { push @judged, $_[0]; return 1; }
sub is { die "offline equality failed: $_[2]\n" unless $_[0] eq $_[1]; return 1; }
sub is_deeply {
    my $json = JSON::PP->new->canonical;
    die "offline deep equality failed: $_[2]\n"
      unless $json->encode($_[0]) eq $json->encode($_[1]);
    return 1;
}
sub wait_witness {
    die "missing pending native caller\n" unless $entry->{pending} && $entry->{transaction};
    die "OFFLINE_WITNESS_FAILURE\n" if $input->{fault} eq 'witness';
    my %activity = %{$input->{sample}{activity}};
    $activity{xid} = '' . (500 + $index);
    return \%activity;
}
{
    package DeadlineWriter;
    sub safe_psql {
        my ($self, $database, $sql) = @_;
        return JSON::PP::encode_json(shift @{$input->{frames}})
          if $sql =~ /^SELECT json_object_agg/;
        if ($sql eq 'SELECT test_pgrac_undo_slot_snapshot()') {
            my $snapshot = JSON::PP::decode_json(JSON::PP::encode_json($input->{sample}{before}{snapshot}));
            $snapshot->{binding_generation} = '999' if $completed && $input->{fault} eq 'owner';
            return JSON::PP::encode_json($snapshot);
        }
        return JSON::PP::encode_json($input->{sample}{locks}) if $sql =~ /FROM pg_locks/;
        die "unexpected observer SQL: $sql\n";
    }
}
{
    package DeadlineClient;
    sub query_safe {
        my ($self, $sql) = @_;
        if ($sql eq 'SELECT test_pgrac_undo_slot_binding()') {
            return JSON::PP::encode_json($self->{binding});
        }
        if ($sql =~ /^BEGIN; SET LOCAL lock_timeout=/) {
            die "previous caller still pending\n" if $entry->{pending};
            ++$index;
            $completed = 0;
            push @started, $sql;
            return '' . (500 + $index);
        }
        if ($sql =~ /^SELECT current_setting/) {
            push @settings, $sql;
            return $completed && $input->{fault} eq 'settings' ? '30s|90s' : '0|90s';
        }
        return '' if $sql eq 'ROLLBACK' || $sql =~ /^\\set VERBOSITY/;
        return '' . $self->{pid} if $sql eq 'SELECT pg_backend_pid()';
        die "unexpected caller SQL: $sql\n";
    }
    sub query_until {
        my ($self, $pattern, $sql) = @_;
        push @sent, {sql => $sql, pending => $entry->{pending}, transaction => $entry->{transaction}};
        return "G_SLOT_SENT\n";
    }
    sub query {
        my ($self, $sql) = @_;
        die "unexpected completion query\n" unless $sql eq '';
        $completed = 1;
        my $state = $index == 0 ? '55P03' : '57014';
        $state = '57014' if $index == 0 && $input->{fault} eq 'sqlstate';
        my $reason = $index == 0 ? 'lock timeout' : 'statement timeout';
        $reason = 'user request' if $input->{fault} eq 'reason';
        $self->{stderr} = "ERROR: $state: canceling statement due to $reason\n";
        $self->{binding}{wait_scope} = JSON::PP::true if $input->{fault} eq 'scope';
        $self->{binding}{available} = JSON::PP::true if $input->{fault} eq 'binding';
        $self->{binding}{pid} = 9000 if $input->{fault} eq 'binding';
        return ("G_SLOT_SQLSTATE $state\n", 1);
    }
}
'''
    suffix = r'''
$writer = bless {}, 'DeadlineWriter';
my @owners = map { {pid => $_->{pid}, client => bless({pid => $_->{pid}, binding => $_},
                                                   'DeadlineClient')} }
             @{$input->{sample}{before}{bindings}};
my $client = bless {pid => 9000, stderr => '',
                   binding => {available => JSON::PP::false, wait_scope => JSON::PP::false}},
                   'DeadlineClient';
$entry = {client => $client, pid => 9000, transaction => 0, pending => 0};
my $ok = eval { run_full_slot_deadlines('native', \@owners, $owners[0],
                                      $entry, $input->{sample}{before}); 1; };
print encode_json({failure => $ok ? '' : $@, started => \@started, sent => \@sent,
                   settings => \@settings, judged => \@judged, saved => \@saved,
                   frames_left => scalar @{$input->{frames}},
                   pending => $entry->{pending}, transaction => $entry->{transaction}});
'''
    result = subprocess.run(["perl", "-e", prefix + keys[0] + "\n" +
                             "\n".join(functions) + suffix], input=json.dumps(payload),
                            text=True, capture_output=True, cwd=source.parent,
                            env=dict(os.environ, PYTHON=sys.executable), timeout=20, check=True)
    if result.stderr:
        raise AssertionError(result.stderr)
    return json.loads(result.stdout)


class FullSlotDeadlineTests(unittest.TestCase):
    def test_both_original_deadlines_keep_distinct_errors_counts_and_local_settings(self):
        result = controller()
        self.assertEqual(result["failure"], "")
        self.assertEqual(len(result["started"]), 2)
        self.assertIn("SET LOCAL lock_timeout=30000", result["started"][0])
        self.assertIn("SET LOCAL statement_timeout=30000", result["started"][1])
        self.assertEqual(result["frames_left"], 0)
        self.assertEqual((result["pending"], result["transaction"]), (0, 0))
        self.assertEqual(len(result["settings"]), 3)
        self.assertEqual([row["sql"] for row in result["sent"]], [
            "\\echo G_SLOT_SENT\nUPDATE g_empty_undo SET value=999 WHERE false;\n"
            "\\echo G_SLOT_SQLSTATE :SQLSTATE\n"] * 2)
        self.assertTrue(all(row["pending"] == row["transaction"] == 1 for row in result["sent"]))
        saved = dict(result["saved"])
        for kind, state in (("lock", "55P03"), ("statement", "57014")):
            self.assertTrue(saved[f"native-deadline-{kind}-blocked-result"]["passed"])
            self.assertIn(state, saved[f"native-deadline-{kind}-completion"]["output"])

    def test_missing_witness_stops_and_preserves_pending_cleanup(self):
        result = controller("witness")
        self.assertIn("OFFLINE_WITNESS_FAILURE", result["failure"])
        self.assertEqual(len(result["started"]), 1)
        self.assertEqual((result["pending"], result["transaction"]), (1, 1))

    def test_wrong_deadline_error_or_user_cancellation_stops_next_case(self):
        for fault in ("sqlstate", "reason"):
            result = controller(fault)
            self.assertIn("native deadline did not preserve", result["failure"])
            self.assertEqual(len(result["started"]), 1)
            self.assertEqual((result["pending"], result["transaction"]), (0, 1))

    def test_binding_scope_and_original_owner_changes_are_not_accepted(self):
        for fault in ("binding", "scope", "owner"):
            result = controller(fault)
            self.assertIn("offline deep equality failed", result["failure"])
            self.assertEqual(len(result["started"]), 1)
            self.assertEqual((result["pending"], result["transaction"]), (0, 0))

    def test_lock_timeout_cannot_increment_caller_cancel_counter(self):
        result = controller("counter")
        self.assertIn("native deadline counter mismatch", result["failure"])
        self.assertEqual(len(result["started"]), 1)
        self.assertEqual((result["pending"], result["transaction"]), (0, 0))

    def test_transaction_local_deadlines_must_restore_original_settings(self):
        result = controller("settings")
        self.assertIn("offline equality failed", result["failure"])
        self.assertEqual(len(result["started"]), 1)
        self.assertEqual((result["pending"], result["transaction"]), (0, 0))


if __name__ == "__main__":
    unittest.main()
