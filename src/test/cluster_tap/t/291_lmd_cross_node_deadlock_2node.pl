#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 291_lmd_cross_node_deadlock_2node.pl
#    spec-5.8 D8 — real 2-node cross-node deadlock detection end-to-end.
#
#    Constructs a genuine cross-node deadlock that NO single node's local GRD
#    graph can see, and proves the cluster LMD coordinator detects and resolves
#    it through the FULL production path:
#
#        node0 holds tA (ACCESS EXCLUSIVE), then waits for tB  (edge A->B)
#        node1 holds tB (ACCESS EXCLUSIVE), then waits for tA  (edge B->A)
#
#    tA and tB are chosen at runtime so their GES resources are mastered on
#    DIFFERENT nodes (see master_of below).  That split is essential: if one
#    node mastered both resources, its local GRD graph would contain the whole
#    cycle and the spec-2.22 local single-node detector would resolve it first
#    (deadlock_confirmed_count would never advance).  With the masters split,
#    each node holds only a half-edge and only the spec-5.8 cross-node
#    two-round union/Tarjan path can detect the cycle — which is the point.
#
#    -> both waiters register WFG edges at their resource masters (D1b) and
#       publish per-proc wait-state (D1d);
#    -> the HC16 lowest-active coordinator (node0) broadcasts DEADLOCK_PROBE,
#       node1's LMON builds + sends a REPORT back (D8 Option A), node0's LMON
#       appends it into the shmem collector (D8 Option B);
#    -> the coordinator unions local + remote edges, Tarjan finds the cycle,
#       the two-round confirm (D3) agrees, the D5 per-proc wait-state
#       revalidate passes, and a victim is cancelled -> 40P01.
#
#    Legs (real 2-node, no SKIP):
#      L1  pair alive + LMD ready on both + the D6/D8 counters present at 0.
#      L2  the cross-node cycle is formed (both upgrades block in
#          ClusterGesReplyWait) — neither completes on its own.
#      L3  deadlock_confirmed_count advances (two-round union Tarjan confirmed
#          a real cross-node cycle — the coordinator/REPORT/collector path is
#          live, not the old dead scaffold).
#      L4  a victim cancel is issued (victim_cancel_sent_count or
#          cross_node_victim_cancel_sent_count advances).
#      L5  the deadlock RESOLVES: both blocked LOCKs leave 'active' (one was
#          cancelled, the other then granted) — not a hang.
#      L6  the victim's 40P01 "deadlock detected" surfaces in a node log.
#      L7  the pair stays usable after the deadlock (a fresh txn commits).
#
#    Harness: ClusterPair shared_data + 3 voting disks; LMD + cross-node
#    detection enabled; a short global_dd_interval so detection is prompt; a
#    long ges_request_timeout so the lock wait never times out before the
#    detector fires (we are proving detection, not the timeout backstop).
#
# Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
# Portions Copyright (c) 2026, pgrac contributors
#
# Author: SqlRush <sqlrush@gmail.com>
#
# IDENTIFICATION
#    src/test/cluster_tap/t/291_lmd_cross_node_deadlock_2node.pl
#
# NOTES
#    pgrac-original file.
#    Spec: spec-5.8-full-cross-node-deadlock-detector.md
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
# whether granted or cancelled with 40P01).
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

# Poll until no granted AccessExclusiveLock remains on $tbl on either node — the
# post-deadlock winner holds the contended tables until its backend exits and
# runs LockReleaseAll (which ships GES_RELEASE to the masters), so the L7
# usability LOCK must wait for that to finish or it false-times-out.
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

# Sum an 'lmd' counter across both nodes (the coordinator / victim may be either).
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

# True if either node's server log contains a cross-node deadlock 40P01.
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

# Which node masters $tbl's cluster-lock (GES) resource?  The authoritative
# shard->master map is pg_cluster_grd_shards (consistent cluster-wide); a
# resource's shard surfaces in pg_cluster_grd_entries once the lock is held.
# Lock $tbl in a held txn (via the reused $probe background handle on node0),
# read the entry's shard_id on whichever node carries it, and resolve the
# master.  Returns the master node id, or -1 if no GES entry materialized.
# (field2 == locktag_field2 == reloid; the requester's reloid travels in the
# wire locktag, so it matches the entry on the master too.)
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
# L1: strict pair + shared data + LMD ready on both + counter baseline.
# ----------
my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'lmd_xnode_dl',
	quorum_voting_disks => 3,
	shared_data         => 1,
	extra_conf          => [
		'autovacuum = off',
		# GES enqueue (and therefore the WFG edges) only run when the GRD entry
		# table is allocated; ClusterPair leaves it 0 by default.
		'cluster.grd_max_entries = 1024',
		# Prove DETECTION, not the timeout backstop: the lock wait must outlast
		# the detector.
		'cluster.ges_request_timeout_ms = 60000',
		'cluster.ges_convert_timeout_ms = 60000',
		# Prompt coordinator scan + confirm in the test.
		'cluster.global_dd_interval_ms = 1000',
		'cluster.deadlock_confirm_interval_ms = 500',
		'cluster.lmd_scan_interval_ms = 500',
		'cluster.deadlock_detection_enabled = on',
		# Keep a healthy peer from being falsely declared DEAD while a lock-hold
		# window is open (mirrors t/283).
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

# The D8 shmem-collector counters must be present (D8 observability surface).
is( $pair->node0->safe_psql(
		'postgres', q{
		SELECT count(*) FROM pg_cluster_state
		WHERE category = 'lmd'
		  AND key IN ('deadlock_confirmed_count','probe_report_enqueue_count')}),
	'2',
	'L1 D6/D8 counters present in dump_lmd');

# Build the deadlock from two tables whose GES resources are mastered on
# DIFFERENT nodes.  This is what makes the deadlock GENUINELY cross-node: no
# single node's local GRD graph holds the whole cycle, so the spec-2.22 local
# single-node detector cannot see (or resolve) it — only the spec-5.8
# cross-node two-round union/Tarjan path can, which is exactly what L3 asserts.
# A co-mastered pair would instead be resolved first by the local detector and
# deadlock_confirmed_count would never advance — that is a real determinism
# hole, not something to paper over by relaxing the assertion.  Tables are
# created on BOTH nodes (mirrors t/283: a node can only LOCK a relation its own
# catalog knows; shared storage makes the cluster resource identical
# cross-node).
my ($tA, $tB);
{
	my $probe = $pair->node0->background_psql('postgres');
	my %first_on;    # master node id -> first table name observed on it
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
			$tA = $first_on{ $masters[0] };    # held by node0, waited by node1
			$tB = $first_on{ $masters[1] };    # held by node1, waited by node0
			last;
		}
	}
	$probe->quit;
}
ok(defined $tA && defined $tB,
	"L1 placed two tables on different GRD masters ($tA, $tB) for a genuine cross-node deadlock")
  or BAIL_OUT('could not place two tables on different GRD masters in 24 tries');
usleep(1_000_000);


# ----------
# L2: form the cross-node cycle.
# ----------
my $confirmed_before = lmd_count($pair, 'deadlock_confirmed_count');
my $cancel_before    = lmd_count($pair, 'victim_cancel_sent_count')
  + lmd_count($pair, 'cross_node_victim_cancel_sent_count');

my $h0 = $pair->node0->background_psql('postgres');
$h0->query_safe('BEGIN');
$h0->query_safe("LOCK TABLE $tA IN ACCESS EXCLUSIVE MODE");    # node0 holds tA

my $h1 = $pair->node1->background_psql('postgres');
$h1->query_safe('BEGIN');
$h1->query_safe("LOCK TABLE $tB IN ACCESS EXCLUSIVE MODE");    # node1 holds tB

# node0 now waits for tB (held by node1): edge A->B.
bg_start_blocking($h0, "LOCK TABLE $tB IN ACCESS EXCLUSIVE MODE");
ok(wait_for_wait_event($pair->node0, "%LOCK TABLE $tB%", 'ClusterGesReplyWait', 25),
	'L2 node0 blocks waiting for tB (cross-node edge A->B registered)');

# node1 now waits for tA (held by node0): edge B->A — the cycle is closed.
bg_start_blocking($h1, "LOCK TABLE $tA IN ACCESS EXCLUSIVE MODE");
ok(wait_for_wait_event($pair->node1, "%LOCK TABLE $tA%", 'ClusterGesReplyWait', 25),
	'L2 node1 blocks waiting for tA (cross-node edge B->A registered; cycle closed)');


# ----------
# L3: the coordinator confirms a real cross-node deadlock (two-round Tarjan).
# ----------
my $l3 = wait_lmd_count_gt($pair, 'deadlock_confirmed_count', $confirmed_before, 40);
if (!$l3)
{
	# Diagnostic: dump the detection chain on both nodes to pinpoint the break.
	for my $k (qw(wait_edge_count tarjan_scan_count probe_broadcast_count
		probe_report_enqueue_count probe_drop_stale_count cycle_detected_count
		confirm_unconfirmed_count deadlock_confirmed_count victim_cancel_sent_count
		cross_node_victim_cancel_sent_count))
	{
		diag("  L3-DIAG $k = " . lmd_count($pair, $k));
	}
	for my $i (0, 1)
	{
		my $n = $i == 0 ? $pair->node0 : $pair->node1;
		diag("  node$i lmd state="
			  . $n->safe_psql('postgres',
				q{SELECT value FROM pg_cluster_state WHERE category='lmd' AND key='lmd_state'}));
	}
}
ok($l3,
	'L3 deadlock_confirmed_count advances (PROBE->REPORT->union Tarjan->two-round confirm live)');


# ----------
# L4: a victim cancel is issued through the real path.
# ----------
my $cancel_after = lmd_count($pair, 'victim_cancel_sent_count')
  + lmd_count($pair, 'cross_node_victim_cancel_sent_count');
ok($cancel_after > $cancel_before,
	"L4 a victim cancel was issued ($cancel_before -> $cancel_after)");


# ----------
# L5: the deadlock RESOLVES — both blocked LOCKs leave 'active' (one cancelled
# with 40P01, the other granted once the victim's txn aborted).
# ----------
ok( wait_for_query_done($pair->node0, "%LOCK TABLE $tB%", 30)
	  && wait_for_query_done($pair->node1, "%LOCK TABLE $tA%", 30),
	'L5 both blocked upgrades return (deadlock resolved, not a hang)');


# ----------
# L6: the victim's 40P01 surfaced (cluster deadlock detected, cancelled).
# ----------
ok(deadlock_in_logs($pair), 'L6 a node log shows the cross-node deadlock 40P01');


# ----------
# L7: the pair stays usable after the deadlock.
# ----------
# End both background transactions with an EXPLICIT ROLLBACK (echo marker AFTER
# the ROLLBACK so we proceed only once it has actually executed).  The explicit
# transaction-abort path runs the GES release that ships GES_RELEASE to the
# resource masters, freeing tA/tB cluster-wide — whereas merely killing the
# backend (FATAL/proc_exit) clears the local PG lock but leaves the GES master
# holder record lingering, which would then block the L7 re-lock below.  The
# handles are desynced by the fire-and-forget blocking LOCK + the async 40P01,
# so tolerate a torn-down handle; the ROLLBACK still reaches a live one.
for my $h ($h0, $h1)
{
	eval { $h->query_until(qr/PGRAC_DRAIN/, "ROLLBACK;\n\\echo PGRAC_DRAIN\n"); };
}
eval { $h0->quit; };
eval { $h1->quit; };

# Both contended tables must be fully released cluster-wide before the
# usability LOCK below, or it would false-time-out on a lock that is merely
# still draining (not a real hang).
ok( wait_table_unlocked($pair, $tA, 30) && wait_table_unlocked($pair, $tB, 30),
	'L7 contended tables locally released after the deadlock resolved');

# Both nodes keep serving, and the cluster lock (GES) path still works for new
# work after the deadlock.  The usability LOCK deliberately uses a FRESH,
# never-contended table: re-locking one of the just-contended tables instead
# would exercise the post-release lifecycle of a remotely-contended GRD entry,
# which has a separate, known GES holder-release gap — when a LOCAL holder of a
# resource it also masters ends its txn while that entry carried a cross-node
# waiter, the GRD granted count is not cleared, so a later request blocks.  That
# is a GES release-path issue (see the spec's follow-up note), independent of
# the cross-node deadlock detector verified by L1-L6.
is($pair->node0->safe_psql('postgres', 'SELECT 1'), '1',
	'L7 node0 still serves a fresh query after the deadlock');
is($pair->node1->safe_psql('postgres', 'SELECT 1'), '1',
	'L7 node1 still serves a fresh query after the deadlock');
$pair->node0->safe_psql('postgres', 'CREATE TABLE dl_after (id int)');
$pair->node1->safe_psql('postgres', 'CREATE TABLE dl_after (id int)');
is( $pair->node1->safe_psql(
		'postgres', 'BEGIN; LOCK TABLE dl_after IN ACCESS EXCLUSIVE MODE; COMMIT; SELECT 1'),
	'1',
	'L7 node1 takes + releases a fresh cluster lock after the deadlock (pair usable)');


$pair->stop_pair;
done_testing();
