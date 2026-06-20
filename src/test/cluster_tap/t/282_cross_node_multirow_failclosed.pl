#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 282_cross_node_multirow_failclosed.pl
#    spec-5.2 §3.5 D11 — multi-row writer fail-closed (Rule 8.A).
#
#    The deferred-writer marker path (PCM_STATE_READ_IMAGE ->
#    cluster_bufmgr_block_write_permitted=false -> 53R9H) has no coverage in
#    t/280/t/281, which both exercise the CONTENDED same-row wait (node1's
#    target row is the one node0 holds, so node1 enters the TX wait and
#    reacquires real X before writing -> passes the guard).  This test is the
#    deterministic NON-contended scenario:
#
#      node0 holds an uncommitted row lock on id=1 (active ITL on the shared
#      block);  node1 writes a DIFFERENT row (id=2) on the SAME block.  node1
#      gets a read-image (node0 still has an uncommitted ITL slot, so the block
#      is NOT transferred), and because id=2's own lock is not contended node1
#      does NOT enter the TX wait -> the cluster_itl forward-write guard must
#      FAIL CLOSED (53R9H), never silently mutate the non-owned read-image
#      (which would diverge from node0's X copy = lost update).
#
#    Legs:
#      L1  pair up, same-DDL relfilenode (rows 1+2 on the same block).
#      L2  node0 holds FOR UPDATE id=1;  node1 UPDATE id=2 -> 53R9H fail-closed;
#          id=2 unchanged (no silent apply).
#      L3  node0 COMMITs (releases the active ITL);  node1 retries UPDATE id=2
#          -> succeeds (the fail-closed is RETRYABLE: once the holder is
#          terminal the block transfers and node1 gets real X).
#
#    Harness:  ClusterPair shared_data + 3 voting disks + autovacuum off + the
#    widened CSSD misscount (L355: long cross-node contention legs must survive
#    CI scheduling jitter or a healthy peer is falsely declared DEAD).
#
# Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
# Portions Copyright (c) 2026, pgrac contributors
#
# Author: SqlRush <sqlrush@gmail.com>
#
# IDENTIFICATION
#    src/test/cluster_tap/t/282_cross_node_multirow_failclosed.pl
#
# NOTES
#    pgrac-original file.
#    Spec: spec-5.2-cf-liveread-dataplane-and-tx-row-lock-wait.md (§3.5 D11)
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../../perl";

use PostgreSQL::Test::ClusterPair;
use Test::More;
use Time::HiRes qw(usleep);


# ----------
# L1: strict pair + shared data + same-DDL (rows 1 and 2 on the same block).
# ----------
my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'mr_failclosed',
	quorum_voting_disks => 3,
	shared_data         => 1,
	extra_conf          => [
		'autovacuum = off',
		'cluster.ges_request_timeout_ms = 30000',
		# L355: widen CSSD misscount so a loaded CI runner does not falsely
		# declare the held-lock peer DEAD and reconfig-abort the in-flight
		# cross-node write before the guard runs.  (Same as t/280 / t/281.)
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
# id=1 and id=2 are both on block 0 of a fresh heap.
$pair->node0->safe_psql('postgres', 'INSERT INTO t VALUES (1, 100), (2, 100)');
$pair->node0->safe_psql('postgres', 'CHECKPOINT');    # flush both rows to shared storage


# ----------
# L2: node0 holds an active row lock on id=1;  node1 writes id=2 (same block,
# NOT contended) -> must fail closed 53R9H, NOT silently apply.
# ----------
my $h0 = $pair->node0->background_psql('postgres', on_error_die => 1);
$h0->query_safe('BEGIN');
$h0->query_safe('SELECT ctr FROM t WHERE id = 1 FOR UPDATE');    # node0: active ITL on the shared block

my ($mr_rc, $mr_out, $mr_err) =
  $pair->node1->psql('postgres', 'UPDATE t SET ctr = ctr + 1 WHERE id = 2');
isnt($mr_rc, 0,
	'L2 node1 write to a different row on the block held-X by node0 is NOT silently applied (fails closed)');
like($mr_err, qr/53R9H|held in X by a remote|cross-node write/i,
	'L2 multi-row write fails closed: 53R9H (writer-transfer deferred; no mutate of non-owned read-image)');
is($pair->node0->safe_psql('postgres', 'SELECT ctr FROM t WHERE id = 2'), '100',
	'L2 id=2 unchanged on node0 — the fail-closed write did not apply (no lost update)');


# ----------
# L3: node0 COMMITs (releases the active ITL);  node1's retry succeeds — the
# fail-closed is retryable, not a permanent denial.
# ----------
$h0->query_safe('COMMIT');
$h0->quit;

my $ok_retry = 0;
my $deadline = time() + 20;
while (time() < $deadline)
{
	my ($rc, $out, $err) =
	  $pair->node1->psql('postgres', 'UPDATE t SET ctr = ctr + 1 WHERE id = 2');
	if ($rc == 0) { $ok_retry = 1; last; }
	usleep(300_000);
}
ok($ok_retry, 'L3 node1 retry of UPDATE id=2 succeeds after node0 releases (fail-closed is retryable)');
is($pair->node1->safe_psql('postgres', 'SELECT ctr FROM t WHERE id = 2'), '101',
	'L3 id=2 = 101 after the retry applied onto the committed 100');

$pair->stop_pair;
done_testing();
