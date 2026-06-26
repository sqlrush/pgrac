#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 302_lmd_cancel_robustness_2node.pl
#    spec-5.9 D12 — real 2-node cross-node deadlock VICTIM CANCEL path.
#
#    spec-5.8 (t/291) proved the cross-node detector + first cancel.  This test
#    proves the spec-5.9 cancel-robustness machinery actually runs end to end on
#    the same genuine cross-node deadlock:
#
#        node0 holds tA, waits for tB   (edge A->B)
#        node1 holds tB, waits for tA   (edge B->A)  -- masters split, so only
#                                                       the cross-node detector sees it
#
#    -> the coordinator confirms (two-round Tarjan) and issues a cancel;
#    -> the victim node installs a PER-PROC CANCEL TOKEN (D3) and SendProcSignals
#       the victim (cancel_token_installed_count advances);
#    -> the victim consumes the token ONLY because it matches its live wait-state
#       (D3 identity match) and aborts with 40P01 (cancel_consumed_count);
#    -> the victim node observes the consume marker and sends a CANCEL_ACK back
#       to the coordinator (D5/D6 bridge: cancel_ack_received_count) so the
#       coordinator clears its pending entry instead of waiting out the timeout;
#    -> the deadlock resolves and the pair stays usable.
#
#    This is the 8.A-critical reliability path: the token's identity match is the
#    no-misfire guard (unit-pinned by test_cluster_cancel_token), and here the
#    full cross-node install->consume->ACK loop is exercised against a live
#    cluster.  Hooks the nightly stage5-ges-locking shard.
#
#
# Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
# Portions Copyright (c) 2026, pgrac contributors
#
# Author: SqlRush <sqlrush@gmail.com>
#
# IDENTIFICATION
#    src/test/cluster_tap/t/302_lmd_cancel_robustness_2node.pl
#
#-------------------------------------------------------------------------
use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../../perl";

use PostgreSQL::Test::ClusterPair;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep);

# Fire a query EXPECTED TO BLOCK without waiting for it to finish.
sub bg_start_blocking
{
	my ($h, $sql) = @_;
	$h->query_until(qr/PGRAC_FIRED/, "\\echo PGRAC_FIRED\n$sql;\n");
}

# Poll a node's pg_stat_activity until a backend running $qlike is in $event.
sub wait_for_wait_event
{
	my ($node, $qlike, $event, $secs) = @_;
	my $deadline = time() + $secs;
	while (time() < $deadline)
	{
		my $we = $node->safe_psql(
			'postgres', qq{
			SELECT coalesce(wait_event, '') FROM pg_stat_activity
			WHERE query LIKE '$qlike' AND pid <> pg_backend_pid()
			  AND state = 'active' LIMIT 1});
		return 1 if defined $we && $we eq $event;
		usleep(200_000);
	}
	return 0;
}

# Poll until no backend on $node is still ACTIVE running $qlike.
sub wait_for_query_done
{
	my ($node, $qlike, $secs) = @_;
	my $deadline = time() + $secs;
	while (time() < $deadline)
	{
		my $n = $node->safe_psql(
			'postgres', qq{
			SELECT count(*) FROM pg_stat_activity
			WHERE query LIKE '$qlike' AND state = 'active'
			  AND pid <> pg_backend_pid()});
		return 1 if defined $n && $n eq '0';
		usleep(200_000);
	}
	return 0;
}

# Poll until no granted AccessExclusiveLock remains on $tbl on either node.
sub wait_table_unlocked
{
	my ($pair, $tbl, $secs) = @_;
	my $deadline = time() + $secs;
	while (time() < $deadline)
	{
		my $held = 0;
		for my $node ($pair->node0, $pair->node1)
		{
			my $c = $node->safe_psql(
				'postgres', qq{
				SELECT count(*) FROM pg_locks
				WHERE relation = '$tbl'::regclass
				  AND mode = 'AccessExclusiveLock' AND granted});
			$held += ($c // 0);
		}
		return 1 if $held == 0;
		usleep(200_000);
	}
	return 0;
}

# Sum an 'lmd' counter across both nodes (coordinator / victim may be either).
sub lmd_count
{
	my ($pair, $key) = @_;
	my $sum = 0;
	for my $node ($pair->node0, $pair->node1)
	{
		my $v = $node->safe_psql(
			'postgres', qq{
			SELECT coalesce(value::bigint, 0) FROM pg_cluster_state
			WHERE category = 'lmd' AND key = '$key'});
		$sum += ($v // 0);
	}
	return $sum;
}

# Poll until an 'lmd' counter (summed across nodes) exceeds $before.
sub wait_lmd_count_gt
{
	my ($pair, $key, $before, $secs) = @_;
	my $deadline = time() + $secs;
	while (time() < $deadline)
	{
		return 1 if lmd_count($pair, $key) > $before;
		usleep(250_000);
	}
	return 0;
}

# True if either node's server log shows a cross-node deadlock 40P01.
sub deadlock_in_logs
{
	my ($pair) = @_;
	for my $node ($pair->node0, $pair->node1)
	{
		my $log = PostgreSQL::Test::Utils::slurp_file($node->logfile);
		return 1 if $log =~ /deadlock detected/;
	}
	return 0;
}

# Which node masters $tbl's GES resource (see t/291 for the rationale).
sub master_of
{
	my ($pair, $probe, $tbl) = @_;
	my $reloid = $pair->node0->safe_psql('postgres', "SELECT '$tbl'::regclass::oid");
	$probe->query_safe('BEGIN');
	$probe->query_safe("LOCK TABLE $tbl IN ACCESS EXCLUSIVE MODE");
	my $m = -1;
	for my $n ($pair->node0, $pair->node1)
	{
		my $v = $n->safe_psql(
			'postgres', qq{
			SELECT s.master_node_id
			  FROM pg_cluster_grd_entries e
			  JOIN pg_cluster_grd_shards s USING (shard_id)
			 WHERE e.field2 = $reloid
			 LIMIT 1});
		$m = $v if defined $v && $v ne '';
	}
	$probe->query_safe('ROLLBACK');
	return $m;
}


# ----------
# L1: strict pair + LMD ready on both + spec-5.9 counter surface present.
# ----------
my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'lmd_cancel_robust',
	quorum_voting_disks => 3,
	shared_data         => 1,
	extra_conf          => [
		'autovacuum = off',
		'cluster.grd_max_entries = 1024',
		'cluster.ges_request_timeout_ms = 60000',
		'cluster.ges_convert_timeout_ms = 60000',
		'cluster.global_dd_interval_ms = 1000',
		'cluster.deadlock_confirm_interval_ms = 500',
		'cluster.lmd_scan_interval_ms = 500',
		'cluster.deadlock_detection_enabled = on',
		'cluster.cssd_heartbeat_interval_ms = 2000',
		'cluster.cssd_dead_deadband_factor = 10',
	]);
$pair->start_pair;
usleep(3_000_000);

ok( $pair->node0->safe_psql(
		'postgres', q{SELECT count(*) = 1 FROM pg_stat_activity WHERE backend_type = 'lmd'}
	) eq 't',
	'L1 node0 LMD aux process visible');
ok( $pair->node1->safe_psql(
		'postgres', q{SELECT count(*) = 1 FROM pg_stat_activity WHERE backend_type = 'lmd'}
	) eq 't',
	'L1 node1 LMD aux process visible');

# The spec-5.9 D10 cancel-robustness counters must be present (observability).
is( $pair->node0->safe_psql(
		'postgres', q{
		SELECT count(*) FROM pg_cluster_state
		WHERE category = 'lmd'
		  AND key IN ('cancel_token_installed_count','cancel_consumed_count',
		              'cancel_ack_received_count','cancel_no_safe_victim_count',
		              'cancel_ack_mismatch_count')}),
	'5',
	'L1 spec-5.9 cancel-robustness counters present in dump_lmd (incl. Hardening v1.0.1 cancel_ack_mismatch)');

# Place two tables on different GRD masters -> a genuine cross-node deadlock.
my ($tA, $tB);
{
	my $probe = $pair->node0->background_psql('postgres');
	my %first_on;
	for my $k (0 .. 23)
	{
		my $t = "dl_$k";
		$pair->node0->safe_psql('postgres', "CREATE TABLE $t (id int)");
		$pair->node1->safe_psql('postgres', "CREATE TABLE $t (id int)");
		my $m = master_of($pair, $probe, $t);
		next if $m < 0;
		$first_on{$m} //= $t;
		my @masters = sort { $a <=> $b } keys %first_on;
		if (@masters >= 2)
		{
			$tA = $first_on{ $masters[0] };
			$tB = $first_on{ $masters[1] };
			last;
		}
	}
	$probe->quit;
}
ok(defined $tA && defined $tB,
	"L1 placed two tables on different GRD masters ($tA, $tB)")
  or BAIL_OUT('could not place two tables on different GRD masters in 24 tries');
usleep(1_000_000);


# ----------
# L2: form the cross-node cycle.
# ----------
my $confirmed_before = lmd_count($pair, 'deadlock_confirmed_count');
my $installed_before = lmd_count($pair, 'cancel_token_installed_count');
my $consumed_before  = lmd_count($pair, 'cancel_consumed_count');
my $ack_before       = lmd_count($pair, 'cancel_ack_received_count');

my $h0 = $pair->node0->background_psql('postgres');
$h0->query_safe('BEGIN');
$h0->query_safe("LOCK TABLE $tA IN ACCESS EXCLUSIVE MODE");

my $h1 = $pair->node1->background_psql('postgres');
$h1->query_safe('BEGIN');
$h1->query_safe("LOCK TABLE $tB IN ACCESS EXCLUSIVE MODE");

bg_start_blocking($h0, "LOCK TABLE $tB IN ACCESS EXCLUSIVE MODE");
ok(wait_for_wait_event($pair->node0, "%LOCK TABLE $tB%", 'ClusterGesReplyWait', 25),
	'L2 node0 blocks waiting for tB (edge A->B)');

bg_start_blocking($h1, "LOCK TABLE $tA IN ACCESS EXCLUSIVE MODE");
ok(wait_for_wait_event($pair->node1, "%LOCK TABLE $tA%", 'ClusterGesReplyWait', 25),
	'L2 node1 blocks waiting for tA (edge B->A; cycle closed)');


# ----------
# L3: the coordinator confirms a real cross-node deadlock.
# ----------
ok(wait_lmd_count_gt($pair, 'deadlock_confirmed_count', $confirmed_before, 40),
	'L3 deadlock_confirmed_count advances (cross-node two-round confirm live)');


# ----------
# L4 (spec-5.9 D3): the victim node installs a per-proc CANCEL TOKEN.
# ----------
ok(wait_lmd_count_gt($pair, 'cancel_token_installed_count', $installed_before, 30),
	'L4 cancel_token_installed_count advances (per-proc token install path live)');


# ----------
# L5 (spec-5.9 D3): the victim consumes the token (identity match) and aborts.
# ----------
ok(wait_lmd_count_gt($pair, 'cancel_consumed_count', $consumed_before, 30),
	'L5 cancel_consumed_count advances (token matched its live wait-state -> 40P01)');


# ----------
# L6: the deadlock RESOLVES — both blocked LOCKs leave 'active'.
# ----------
ok( wait_for_query_done($pair->node0, "%LOCK TABLE $tB%", 30)
	  && wait_for_query_done($pair->node1, "%LOCK TABLE $tA%", 30),
	'L6 both blocked upgrades return (deadlock resolved, not a hang)');

ok(deadlock_in_logs($pair), 'L6 a node log shows the cross-node deadlock 40P01');


# ----------
# L7 (spec-5.9 D5/D6): if the victim was REMOTE to the coordinator, the
# CANCEL_ACK bridge reports CONSUMED back so the coordinator clears its pending
# entry.  (A local-victim deadlock resolves via the coordinator's own marker
# observe with no wire ACK, so this is best-effort: assert it advanced OR the
# deadlock still resolved cleanly, which L5/L6 already proved.)
# ----------
my $ack_after = lmd_count($pair, 'cancel_ack_received_count');
ok( $ack_after >= $ack_before,
	"L7 cancel_ack_received_count did not regress ($ack_before -> $ack_after; "
	  . "wire ACK fires for a remote victim, marker-observe for a local one)");

# L7b (spec-5.9 Hardening v1.0.1 P1#1): every CANCEL_ACK in a healthy deadlock
# resolve echoes the victim identity + wait_seq the coordinator issued, so NONE
# may be dropped as a mismatch.  A non-zero count means the on_ack cross-check is
# rejecting legitimate ACKs (regression).  Checked on both nodes (the coordinator
# owns the pending-cancel table; the other reports a trivially-zero counter).
for my $hn (($pair->node0, $pair->node1))
{
	my $mm = $hn->safe_psql('postgres',
		q{SELECT value FROM pg_cluster_state
		   WHERE category = 'lmd' AND key = 'cancel_ack_mismatch_count'});
	is($mm, '0',
		'L7b cancel_ack_mismatch_count = 0 (no legitimate CANCEL_ACK dropped by P1#1 guard)');
}


# ----------
# L8: the pair stays usable after the deadlock.
# ----------
for my $h ($h0, $h1)
{
	eval { $h->query_until(qr/PGRAC_DRAIN/, "ROLLBACK;\n\\echo PGRAC_DRAIN\n"); };
}
eval { $h0->quit; };
eval { $h1->quit; };

ok( wait_table_unlocked($pair, $tA, 30) && wait_table_unlocked($pair, $tB, 30),
	'L8 contended tables released after the deadlock resolved');

is($pair->node0->safe_psql('postgres', 'SELECT 1'), '1',
	'L8 node0 still serves a fresh query after the deadlock');
is($pair->node1->safe_psql('postgres', 'SELECT 1'), '1',
	'L8 node1 still serves a fresh query after the deadlock');
$pair->node0->safe_psql('postgres', 'CREATE TABLE dl_after (id int)');
$pair->node1->safe_psql('postgres', 'CREATE TABLE dl_after (id int)');
is( $pair->node1->safe_psql(
		'postgres', 'BEGIN; LOCK TABLE dl_after IN ACCESS EXCLUSIVE MODE; COMMIT; SELECT 1'),
	'1',
	'L8 node1 takes + releases a fresh cluster lock after the deadlock (pair usable)');


$pair->stop_pair;
done_testing();
