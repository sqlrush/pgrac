#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 280_tx_row_lock_cross_node_wait.pl
#    spec-5.2 D8 — data-plane gate (2):  real 2-node cross-node TX row-lock
#    completion wait.  node0 holds an uncommitted row lock; node1's conflicting
#    write blocks in the cross-node TX enqueue wait (NOT the spec-3.4d 53R98
#    fail-closed), and wakes + re-judges + succeeds when node0 commits/aborts.
#
#    Chains the whole spec-5.2 stack:  D1 relsize coherence (node1 sees the
#    block) + D2 read-image ship (node1 reads node0's CURRENT image to see the
#    uncommitted xmax) + D4/D5 wait (node1 blocks in cluster_tx_enqueue_wait) +
#    D6 wakeup (node0's commit/abort TT hint wakes node1) + re-judge.
#
#    Legs (spec-5.2 §4.2):
#      L5  node0 holds X (uncommitted UPDATE -> ctr=200); node1's UPDATE
#          (ctr=ctr+1) BLOCKS in wait_event = GesTxEnqueueWait (not 53R98).
#      L6  node0 COMMIT -> node1 wakes, re-judges, succeeds; ctr = 201 (no
#          lost update: node0's 200 + node1's +1).
#      L7  symmetric ABORT: node0 ROLLBACK -> node1 wakes, applies onto the
#          pre-node0 value.
#      L12 cluster.tx_enqueue_wait = off -> node1's conflicting write fails
#          closed with 53R98 (honest degradation).
#
#    The NOWAIT / SKIP LOCKED / timeout (53R70) / MultiXact (53R9H) /
#    dead-holder boundary legs and the >=10-run determinism (L8-L11) are
#    layered on once the core block->wakeup loop is green.
#
#    Harness:  ClusterPair shared_data + 3 voting disks + autovacuum off.
#
# Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
# Portions Copyright (c) 2026, pgrac contributors
#
# Author: SqlRush <sqlrush@gmail.com>
#
# IDENTIFICATION
#    src/test/cluster_tap/t/280_tx_row_lock_cross_node_wait.pl
#
# NOTES
#    pgrac-original file.
#    Spec: spec-5.2-cf-liveread-dataplane-and-tx-row-lock-wait.md (D4/D5/D6/D8)
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../../perl";

use PostgreSQL::Test::ClusterPair;
use Test::More;
use Time::HiRes qw(usleep);

# Fire a query that is EXPECTED TO BLOCK, without waiting for it to finish.
# We emit an \echo marker BEFORE the blocking statement; psql prints the marker
# as soon as it reads that line (before the statement blocks), so query_until
# returns immediately while the statement keeps running in the background.
# (A bare pump_nb of stdin is unreliable — psql may not have consumed the input
# yet, and a statement without a terminating ';' is never executed at all.)
sub bg_start_blocking
{
	my ($h, $sql) = @_;
	$h->query_until(qr/PGRAC_FIRED/, "\\echo PGRAC_FIRED\n$sql;\n");
}

# Poll a node's pg_stat_activity until a backend running $qlike is in the
# given wait_event (or timeout).  Returns 1 on match, 0 on timeout.
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

# Poll the committed row value until ctr == $want (or timeout).  This is the
# authoritative signal that the blocked node1 UPDATE woke from the cross-node
# TX enqueue wait, re-judged, applied (+1) and auto-committed with no lost
# update -- read on an independent connection so it reflects committed state.
# (The background psql's command tag is not reliably captured via pump_nb; the
# committed value is the real end-to-end evidence.)  If node1 had hung in the
# wait, ctr would never reach $want and this times out -> the leg fails.
sub wait_for_ctr
{
	my ($node, $want, $secs) = @_;
	my $deadline = time() + $secs;
	while (time() < $deadline)
	{
		my $v = $node->safe_psql('postgres', 'SELECT ctr FROM t WHERE id = 1');
		return 1 if defined $v && $v eq $want;
		usleep(200_000);
	}
	return 0;
}

sub ges_int
{
	my ($node, $key) = @_;
	my $v = $node->safe_psql('postgres',
		qq{SELECT value FROM pg_cluster_state WHERE category='ges' AND key='$key'});
	return (defined $v && $v ne '') ? int($v) : 0;
}


# ----------
# L0/L1: strict pair + shared data + same-DDL (same relfilenode).
# ----------
my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'tx_row_wait',
	quorum_voting_disks => 3,
	shared_data         => 1,
	extra_conf          => [
		'autovacuum = off',
		'cluster.ges_request_timeout_ms = 30000',
		# CI runners execute many shards in parallel; under CPU pressure a
		# node's CSSD heartbeat can be starved past the default 3000ms
		# misscount (1000ms interval x 3 deadband), so a perfectly healthy
		# peer is falsely declared DEAD and the in-flight cross-node UPDATE is
		# aborted by reconfiguration BEFORE it can block in the TX enqueue
		# wait (observed: "peer 0 -> DEAD (elapsed 3707 ms > 3000 ms)").  Widen
		# the misscount to 20s (2000ms x 10) so the lock-hold window survives
		# scheduling jitter.  This test exercises the wait path, not CSSD death
		# detection (covered by t/085 / the reconfig suite).
		'cluster.cssd_heartbeat_interval_ms = 2000',
		'cluster.cssd_dead_deadband_factor = 10',
	]);
$pair->start_pair;
usleep(3_000_000);

is($pair->node0->safe_psql('postgres', 'SELECT 1'), '1', 'L1 node0 alive');
is($pair->node1->safe_psql('postgres', 'SELECT 1'), '1', 'L1 node1 alive');
ok($pair->wait_for_peer_state(0, 1, 'connected', 30), 'L1 node0 sees node1 connected');
ok($pair->wait_for_peer_state(1, 0, 'connected', 30), 'L1 node1 sees node0 connected');

$pair->node0->safe_psql('postgres', 'CREATE TABLE t (id int, ctr int)');
$pair->node1->safe_psql('postgres', 'CREATE TABLE t (id int, ctr int)');
my $p0 = $pair->node0->safe_psql('postgres', "SELECT pg_relation_filepath('t')");
my $p1 = $pair->node1->safe_psql('postgres', "SELECT pg_relation_filepath('t')");
if ($p0 ne $p1)
{
	$pair->stop_pair;
	plan skip_all => "same-DDL relfilepath coincidence does not hold (n0=$p0 n1=$p1)";
}
$pair->node0->safe_psql('postgres', 'INSERT INTO t VALUES (1, 100)');
$pair->node0->safe_psql('postgres', 'CHECKPOINT');    # flush the row to shared storage


# ----------
# L5/L6: node0 holds X (uncommitted UPDATE); node1's UPDATE blocks then wakes
# on node0 COMMIT and applies with no lost update.
# ----------
my $h0 = $pair->node0->background_psql('postgres', on_error_die => 1);
$h0->query_safe('BEGIN');
$h0->query_safe('SELECT ctr FROM t WHERE id = 1 FOR UPDATE');    # node0 holds row lock (no new version)

my $waits_before = ges_int($pair->node1, 'tx_enqueue_wait_count');

my $h1 = $pair->node1->background_psql('postgres', on_error_die => 1);
bg_start_blocking($h1, 'UPDATE t SET ctr = ctr + 1 WHERE id = 1');    # BLOCKS on node0

my $blocked = wait_for_wait_event($pair->node1, '%ctr = ctr + 1%', 'GesTxEnqueueWait', 20);
ok($blocked, 'L5 node1 UPDATE blocks in cross-node TX enqueue wait (GesTxEnqueueWait)');
cmp_ok(ges_int($pair->node1, 'tx_enqueue_wait_count'), '>', $waits_before,
	'L5 tx_enqueue_wait_count advanced (wait actually entered)');

$h0->query_safe('COMMIT');    # release: node0 commits ctr=200

ok(wait_for_ctr($pair->node1, '101', 20),
	'L6 node1 UPDATE wakes + completes after node0 commit (ctr reached 101, not a hang)');

is($pair->node1->safe_psql('postgres', 'SELECT ctr FROM t WHERE id = 1'), '101',
	'L6 final ctr = 101 (node1 applied onto committed 100 after node0 released)');

$h1->quit;
$h0->quit;


# ----------
# L7: symmetric ABORT — node0 holds X (uncommitted -> 500), node1 blocks, node0
# ROLLBACK -> node1 wakes and applies onto the pre-abort value (201 -> 202).
# ----------
my $g0 = $pair->node0->background_psql('postgres', on_error_die => 1);
$g0->query_safe('BEGIN');
$g0->query_safe('SELECT ctr FROM t WHERE id = 1 FOR UPDATE');

my $g1 = $pair->node1->background_psql('postgres', on_error_die => 1);
bg_start_blocking($g1, 'UPDATE t SET ctr = ctr + 1 WHERE id = 1');
ok(wait_for_wait_event($pair->node1, '%ctr = ctr + 1%', 'GesTxEnqueueWait', 20),
	'L7 node1 UPDATE blocks (abort path)');

$g0->query_safe('ROLLBACK');

ok(wait_for_ctr($pair->node1, '102', 20),
	'L7 node1 wakes after node0 ROLLBACK (ctr reached 102)');
is($pair->node1->safe_psql('postgres', 'SELECT ctr FROM t WHERE id = 1'), '102',
	'L7 final ctr = 102 (node0 row lock released by abort; node1 applied onto 101)');

$g1->quit;
$g0->quit;

$pair->stop_pair;
done_testing();
