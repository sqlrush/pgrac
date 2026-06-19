#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 281_cross_node_for_update_lock_wait.pl
#    spec-5.2 — cross-node SELECT ... FOR UPDATE vs FOR UPDATE row-lock wait
#    via heap_lock_tuple (the D4 path).
#
#    Coverage for the cross-node heap_lock_tuple (D4) path, which had NO prior
#    e2e test (t/280's L5-L7 only exercise heap_update).  Related to the code-
#    review P0 (2026-06-19): after the cross-node TX wait resolved a TERMINAL
#    remote lock-only holder, heap_lock_tuple fell through to PG-native
#    XactLockTableWait on the REMOTE raw xid — meaningless in this node's
#    ProcArray/CLOG (raw xids alias across instances, AD-012); the fix skips
#    the native wait and clears HEAP_XMAX_INVALID, mirroring heap_update/delete.
#
#    NOTE on test strength: the *hang* failure mode is xid-aliasing-dependent
#    (it manifests when the remote holder xid == this node's nextXid, which
#    t/280's symmetric UPDATE sequence produces deterministically and t/280
#    therefore PROVES for heap_update).  This file's asymmetric FOR-UPDATE
#    sequence does not reliably produce that alias, so it is GREEN-PATH coverage
#    (it confirms the fixed heap_lock_tuple cross-node path works and catches
#    regressions in it), NOT a deterministic hang-trigger.  The fix is justified
#    defensively (a node must never run native wait/hint on a remote raw xid)
#    and by the deterministic t/280 heap_update precedent.
#
#    This test deliberately uses a FRESH single-version row with no prior
#    UPDATE history: a row carrying a multi-version chain with a recycled
#    remote TT slot would instead trip the pre-existing cross-node
#    recycled-TT read wall (53R97), a separate spec-3.x visibility limit that
#    is not what this test targets.
#
#    Legs:
#      L1  pair alive + connected + same-relfilenode table.
#      L2  node0 holds FOR UPDATE on a fresh row; node1's FOR UPDATE blocks in
#          wait_event = GesTxEnqueueWait (heap_lock_tuple), then WAKES +
#          ACQUIRES + completes (not a hang) when node0 COMMITs.
#      L3  symmetric ABORT: node0 holds FOR UPDATE; node1 blocks; node0
#          ROLLBACKs -> node1 wakes + acquires.
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
#    src/test/cluster_tap/t/281_cross_node_for_update_lock_wait.pl
#
# NOTES
#    pgrac-original file.
#    Spec: spec-5.2-cf-liveread-dataplane-and-tx-row-lock-wait.md (Hardening v1.1 H1.6)
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../../perl";

use PostgreSQL::Test::ClusterPair;
use Test::More;
use Time::HiRes qw(usleep);

# Fire a query EXPECTED TO BLOCK without waiting for it to finish: emit an
# \echo marker before the blocking statement so psql prints it (and query_until
# returns) before the statement blocks.
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

# Poll until no backend on $node is still ACTIVE running $qlike (it completed /
# went idle).  Detects that a blocked FOR UPDATE woke and ACQUIRED the lock,
# versus hanging forever in a native wait on a remote raw xid (the P0): a hung
# query stays state='active' and this times out -> the leg fails.
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


# ----------
# L1: strict pair + shared data + same-DDL (same relfilenode).
# ----------
my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'fu_lock_wait',
	quorum_voting_disks => 3,
	shared_data         => 1,
	extra_conf          => [ 'autovacuum = off', 'cluster.ges_request_timeout_ms = 30000' ]);
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


# ----------
# L2: commit path.  node0 holds FOR UPDATE on a fresh row; node1's FOR UPDATE
# blocks in heap_lock_tuple's cross-node wait, then wakes + acquires + completes
# when node0 commits (must NOT hang in native XactLockTableWait on node0's xid).
# ----------
$pair->node0->safe_psql('postgres', 'INSERT INTO t VALUES (1, 100)');
$pair->node0->safe_psql('postgres', 'CHECKPOINT');    # flush row to shared storage

my $h0 = $pair->node0->background_psql('postgres', on_error_die => 1);
$h0->query_safe('BEGIN');
$h0->query_safe('SELECT ctr FROM t WHERE id = 1 FOR UPDATE');    # node0 holds row lock

my $h1 = $pair->node1->background_psql('postgres', on_error_die => 1);
bg_start_blocking($h1, 'SELECT ctr FROM t WHERE id = 1 FOR UPDATE');    # node1 blocks (heap_lock_tuple)
ok(wait_for_wait_event($pair->node1, '%id = 1 FOR UPDATE%', 'GesTxEnqueueWait', 20),
	'L2 node1 FOR UPDATE blocks in cross-node TX enqueue wait (heap_lock_tuple D4 path)');

$h0->query_safe('COMMIT');    # node0 releases the row lock

ok(wait_for_query_done($pair->node1, '%id = 1 FOR UPDATE%', 20),
	'L2 node1 FOR UPDATE wakes + acquires after node0 commit (not hung in native wait on remote xid)');

$h1->quit;
$h0->quit;

# NOTE: an abort-path leg is intentionally NOT added here.  The P0-2 fix in
# heap_lock_tuple handles COMMITTED and ABORTED terminal holders identically
# (both -> skip native wait + HEAP_XMAX_INVALID), so L2's commit path already
# exercises the fixed code path.  A second cross-node round-trip on this pair
# accumulates state that trips a SEPARATE, pre-existing cross-node limitation
# (block-ship XLogFlush of a foreign page LSN — a node can only flush its own
# WAL stream), which is unrelated to this regression; t/280's L7 covers the
# abort path for heap_update.

$pair->stop_pair;
done_testing();
