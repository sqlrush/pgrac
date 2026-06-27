#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 308_lmd_partial_failclosed_2node.pl
#    spec-5.8 Hardening v1.0.1 — FC1 member-set fail-closed acting gate,
#    real 2-node end-to-end.
#
#    The frozen FC1 contract (spec-5.8 §2.4 D4a / §3.5) requires the
#    coordinator to confirm/cancel ONLY when the received responder set
#    EXACTLY equals the expected (CSSD-alive) set; a received ⊊ expected
#    round is partial and MUST be discarded (never confirm/cancel) so a
#    cross-node cycle through an unheard-from node can never drive a false
#    victim cancel.  The shipped detector only enforced the admission half
#    (received ⊆ expected) and then ran Tarjan on the partial union; the
#    Hardening v1.0.1 fix wires the acting gate before Tarjan.
#
#    This is the contrast test to t/291 (the positive: same harness, no
#    injection -> confirm + cancel a real cross-node deadlock).  Here the
#    SAME real cross-node deadlock is built, but the coordinator-side
#    injection 'cluster-lmd-force-partial-round' (armed via
#    cluster.injection_points at LMD startup) drops one responder bit AFTER
#    the genuine REPORT collector ran — the collected union STILL contains
#    the real cycle, so the un-gated code would two-round-confirm and cancel
#    a victim.  The FC1 gate must instead discard every partial round, so:
#      - member_incomplete_count climbs (the gate fires on the real rounds),
#      - deadlock_confirmed_count / victim cancels stay 0 (no false victim),
#      - the deadlock degrades to the finite GES request timeout (no hang,
#        no 40P01).
#
#    Legs (real 2-node, no SKIP):
#      L1  pair alive + LMD ready on both + the FC1 counter present.
#      L2  two tables placed on DIFFERENT GRD masters (genuine cross-node).
#      L3  the cross-node cycle is formed (both upgrades block).
#      L4  the FC1 acting gate fires on the rounds that saw the real cycle:
#          member_incomplete_count advances.
#      L5  NO partial round is confirmed and NO victim is cancelled, even
#          though the collected union held a real cross-node cycle
#          (deadlock_confirmed_count / victim_cancel_sent_count /
#          cross_node_victim_cancel_sent_count all unchanged) — the un-gated
#          ship would have cancelled here (Rule 8.A; reviewer's worst case).
#      L6  fail-closed degrades to the finite GES request timeout: both
#          blocked upgrades return (no hang), and NO 40P01 "deadlock
#          detected" appears in any log (the detector never cancelled).
#      L7  the pair stays usable after the timeout-resolved deadlock.
#
#    Harness: ClusterPair shared_data + 3 voting disks; LMD + cross-node
#    detection enabled; short global_dd_interval so rounds are prompt; a
#    finite ges_request_timeout so the fail-closed deadlock resolves via the
#    timeout backstop; FC1 force-partial injection armed from boot.
#
# Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
# Portions Copyright (c) 2026, pgrac contributors
#
# Author: SqlRush <sqlrush@gmail.com>
#
# IDENTIFICATION
#    src/test/cluster_tap/t/308_lmd_partial_failclosed_2node.pl
#
# NOTES
#    pgrac-original file.
#    Spec: spec-5.8-full-cross-node-deadlock-detector.md (Hardening v1.0.1)
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../../perl";

use PostgreSQL::Test::ClusterPair;
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

# Poll until no backend on $node is still ACTIVE running $qlike (it returned —
# whether granted, cancelled, or timed out).
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

# Sum an 'lmd' counter across both nodes (the coordinator may be either node).
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

# True if either node's server log contains a "deadlock detected" 40P01.
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

# Which node masters $tbl's cluster-lock (GES) resource?  (Same technique as
# t/291: lock it in a held txn, read the entry's shard->master.)
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
# L1: strict pair + shared data + LMD ready on both + FC1 counter present.
# ----------
my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'lmd_fc1_partial',
	quorum_voting_disks => 3,
	shared_data         => 1,
	extra_conf          => [
		'autovacuum = off',
		'cluster.grd_max_entries = 1024',
		# Finite GES timeout: with the detector forced fail-closed, the
		# deadlock must degrade to the timeout backstop (we prove no hang).
		'cluster.ges_request_timeout_ms = 15000',
		'cluster.ges_convert_timeout_ms = 15000',
		# Prompt coordinator scans so the forced-partial gate fires repeatedly
		# while the deadlock is active.
		'cluster.global_dd_interval_ms = 1000',
		'cluster.deadlock_confirm_interval_ms = 500',
		'cluster.lmd_scan_interval_ms = 500',
		'cluster.deadlock_detection_enabled = on',
		# Keep a healthy peer from being falsely declared DEAD (mirrors t/291)
		# — FC1 must fire because of the injected drop, not a real CSSD death.
		'cluster.cssd_heartbeat_interval_ms = 2000',
		'cluster.cssd_dead_deadband_factor = 10',
		# spec-5.8 Hardening v1.0.1 — force every coordinator probe round to
		# look partial (drop one responder bit AFTER the real drain), arming
		# the SKIP behaviour in the LMD aux process at startup.
		"cluster.injection_points = 'cluster-lmd-force-partial-round:skip'",
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
is( $pair->node0->safe_psql(
		'postgres', q{
		SELECT count(*) FROM pg_cluster_state
		WHERE category = 'lmd' AND key = 'member_incomplete_count'}),
	'1',
	'L1 FC1 member_incomplete_count observability row present');


# ----------
# L2: place two tables on DIFFERENT GRD masters (genuine cross-node).
# ----------
my ($tA, $tB);
{
	my $probe = $pair->node0->background_psql('postgres');
	my %first_on;
	for my $k (0 .. 23)
	{
		my $t = "fc1_$k";
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
	"L2 placed two tables on different GRD masters ($tA, $tB) for a genuine cross-node deadlock")
  or BAIL_OUT('could not place two tables on different GRD masters in 24 tries');
usleep(1_000_000);


# ----------
# L3: form the cross-node cycle.
# ----------
my $confirmed0    = lmd_count($pair, 'deadlock_confirmed_count');
my $cancel0       = lmd_count($pair, 'victim_cancel_sent_count');
my $xnode_cancel0 = lmd_count($pair, 'cross_node_victim_cancel_sent_count');
my $incomplete0   = lmd_count($pair, 'member_incomplete_count');

my $h0 = $pair->node0->background_psql('postgres');
$h0->query_safe('BEGIN');
$h0->query_safe("LOCK TABLE $tA IN ACCESS EXCLUSIVE MODE");    # node0 holds tA

my $h1 = $pair->node1->background_psql('postgres');
$h1->query_safe('BEGIN');
$h1->query_safe("LOCK TABLE $tB IN ACCESS EXCLUSIVE MODE");    # node1 holds tB

bg_start_blocking($h0, "LOCK TABLE $tB IN ACCESS EXCLUSIVE MODE");    # edge A->B
ok(wait_for_wait_event($pair->node0, "%LOCK TABLE $tB%", 'ClusterGesReplyWait', 25),
	'L3 node0 blocks waiting for tB (cross-node edge A->B registered)');

bg_start_blocking($h1, "LOCK TABLE $tA IN ACCESS EXCLUSIVE MODE");    # edge B->A (cycle closed)
ok(wait_for_wait_event($pair->node1, "%LOCK TABLE $tA%", 'ClusterGesReplyWait', 25),
	'L3 node1 blocks waiting for tA (cross-node edge B->A registered; cycle closed)');


# ----------
# L4: the FC1 acting gate fires on the rounds that saw the real cross-node
#     cycle in the collected union — member_incomplete_count advances.
# ----------
ok( wait_lmd_count_gt($pair, 'member_incomplete_count', $incomplete0, 20),
	'L4 FC1 acting gate fires on the real-cycle rounds: member_incomplete_count advances');

# Give the two-round confirm window several more intervals: an un-gated ship
# would have confirmed + cancelled by now.
usleep(5_000_000);


# ----------
# L5: NO confirm + NO cancel, even though the union held a real cross-node
#     cycle (the discriminating assertion — the bug would cancel here).
# ----------
is( lmd_count($pair, 'deadlock_confirmed_count'),
	$confirmed0,
	'L5a no partial round confirmed despite a real cycle in the union (deadlock_confirmed_count unchanged)');
is( lmd_count($pair, 'victim_cancel_sent_count'),
	$cancel0,
	'L5b no local victim cancelled from a partial round');
is( lmd_count($pair, 'cross_node_victim_cancel_sent_count'),
	$xnode_cancel0,
	'L5c no cross-node victim cancelled from a partial round');


# ----------
# L6: fail-closed degrades to the finite GES request timeout (no hang), and
#     NO 40P01 "deadlock detected" ever surfaced (the detector never cancelled).
# ----------
ok( wait_for_query_done($pair->node0, "%LOCK TABLE $tB%", 40)
	  && wait_for_query_done($pair->node1, "%LOCK TABLE $tA%", 40),
	'L6a both blocked upgrades return via the GES timeout backstop (no hang)');
ok(!deadlock_in_logs($pair),
	'L6b no 40P01 "deadlock detected" in any log — FC1 never cancelled a victim from a partial round');


# ----------
# L7: the pair stays usable after the timeout-resolved deadlock.
# ----------
for my $h ($h0, $h1)
{
	eval { $h->query_until(qr/PGRAC_DRAIN/, "ROLLBACK;\n\\echo PGRAC_DRAIN\n"); };
}
eval { $h0->quit; };
eval { $h1->quit; };

is($pair->node0->safe_psql('postgres', 'SELECT 1'), '1',
	'L7a node0 still serves a fresh query after the fail-closed deadlock');
is($pair->node1->safe_psql('postgres', 'SELECT 1'), '1',
	'L7b node1 still serves a fresh query after the fail-closed deadlock');
$pair->node0->safe_psql('postgres',
	'CREATE TABLE fc1_after (x int); INSERT INTO fc1_after VALUES (1); DROP TABLE fc1_after;');
ok(1, 'L7c pair remains usable (DDL + DML commit) after FC1 fail-closed rounds');

$pair->stop_pair;
done_testing();
