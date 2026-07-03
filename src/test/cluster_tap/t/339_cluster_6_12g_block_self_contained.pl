#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 339_cluster_6_12g_block_self_contained.pl
#	  spec-6.12 wave g -- block self-containment (active-ITL migration +
#	  opportunistic commit cleanout) on a 2-node ClusterPair.
#
#	  The write-write collapse root (spec-5.2 D11): a block held X by node0
#	  with an UNCOMMITTED ITL slot on row2 cannot be X-transferred, so a
#	  node1 writer of a DIFFERENT row1 on the same block is falsely
#	  serialized (53R9H).  With cluster.block_self_contained = on the block
#	  migrates WITH the active ITL; row1 proceeds, only same-ROW conflicts
#	  serialize (cross-node TX enqueue, t/280).  8.A: the holder's later
#	  commit skips the stamp for the drifted block (D-g1) and every reader
#	  resolves the migrated ACTIVE slot through the TT authority (AD-006).
#
#	  L1  pair boots + shared table + GUC default off
#	  L2  collapse (GUC off baseline): node0 holds an uncommitted row2, a
#	      node1 write of row1 (DIFFERENT row) fails closed 53R9H (the D11
#	      false serialization)
#	  L3  collapse LIFTED (GUC on): the same node1 row1 write SUCCEEDS while
#	      node0's row2 stays uncommitted; the active-ITL transfer counter moves
#	  L3+ D-g1 stamp-skip: node0's ROLLBACK finishes its lock-only slot while
#	      the block is drifted on node1, so the commit cleanout SKIPS the stamp
#	      (verified via the local skip counter)
#	  L4  counter surface: 3 wave-g xnode_lever keys present on both nodes
#
#	  Substrate honesty (L373/L374 / spec-6.12 §3.5, same as wave-b): the hold
#	  uses a LOCK-ONLY hold (SELECT ... FOR UPDATE) -- like t/280 -- so no data
#	  UPDATE recycles the seed ITL slot and node1 resolves the block via the
#	  propagated TT hint.  Two 8.A properties are covered ELSEWHERE, unchanged
#	  by wave g, rather than re-proven here on a substrate that cannot carry
#	  them:  (a) same-ROW cross-node serialization is t/280 (wave g only changes
#	  the DIFFERENT-row path);  (b) the full DATA-visibility crash matrix (a
#	  drifted DATA writer resolved / rolled-back via TT under a natural
#	  cross-node read) hits the Stage-6 recycled-slot 53R97 boundary on the
#	  phantom-shared harness and belongs on the spec-6.0a substrate, exactly
#	  where wave-b's data-plane correctness and the true tax% measurement live.
#
# Spec: spec-6.12-crossnode-cache-fusion-perf-optimization.md (wave g)
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
# IDENTIFICATION
#	  src/test/cluster_tap/t/339_cluster_6_12g_block_self_contained.pl
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../lib";

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::ClusterPair;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep);

sub state_val
{
	my ($node, $cat, $key) = @_;
	my $v = $node->safe_psql('postgres',
		qq{SELECT value FROM pg_cluster_state WHERE category='$cat' AND key='$key'});
	return defined($v) && $v ne '' ? $v + 0 : 0;
}

sub write_retry
{
	my ($node, $sql) = @_;
	for my $i (1 .. 10)
	{
		my $ok = eval { $node->safe_psql('postgres', $sql); 1 };
		return 1 if $ok;
		usleep(500_000);
	}
	return 0;
}

# Poll for a committed value on an independent connection (authoritative
# end-to-end signal that a blocked writer woke + applied).
sub wait_for_val
{
	my ($node, $sql, $want, $secs) = @_;
	my $deadline = time() + $secs;
	while (time() < $deadline)
	{
		my $v = eval { $node->safe_psql('postgres', $sql); };
		return 1 if defined $v && $v eq $want;
		usleep(200_000);
	}
	return 0;
}

sub wait_for_wait_event
{
	my ($node, $qlike, $event, $secs) = @_;
	my $deadline = time() + $secs;
	while (time() < $deadline)
	{
		my $we = $node->safe_psql('postgres', qq{
			SELECT coalesce(wait_event, '') FROM pg_stat_activity
			WHERE query LIKE '$qlike' AND pid <> pg_backend_pid()
			  AND state = 'active' LIMIT 1});
		return 1 if defined $we && $we eq $event;
		usleep(200_000);
	}
	return 0;
}

# ============================================================
# L1: boot + shared table.  GUC armed from conf so a mid-test pair restart is
# not needed (the decision runs in the serve path).
# ============================================================
my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'g612_selfcont',
	quorum_voting_disks => 3,
	shared_data         => 1,
	extra_conf          => [
		'autovacuum = off',
		'cluster.ges_request_timeout_ms = 30000',
		'cluster.cssd_heartbeat_interval_ms = 2000',
		'cluster.cssd_dead_deadband_factor = 10',
	]);
$pair->start_pair;

usleep(2_000_000);
ok($pair->wait_for_peer_state(0, 1, 'connected', 30), 'L1 node0 sees node1 connected');
ok($pair->wait_for_peer_state(1, 0, 'connected', 30), 'L1 node1 sees node0 connected');

my ($node0, $node1) = ($pair->node0, $pair->node1);

is($node0->safe_psql('postgres', 'SHOW cluster.block_self_contained'), 'off',
	'L1 block_self_contained default off');

# Two rows guaranteed on ONE heap block.
$node0->safe_psql('postgres', 'CREATE TABLE g_t (id int, v int)');
$node1->safe_psql('postgres', 'CREATE TABLE g_t (id int, v int)');
my $p0 = $node0->safe_psql('postgres', q{SELECT pg_relation_filepath('g_t')});
my $p1 = $node1->safe_psql('postgres', q{SELECT pg_relation_filepath('g_t')});
is($p0, $p1, 'L1 g_t relfilepath coincidence holds');
ok(write_retry($node0, 'INSERT INTO g_t VALUES (1, 10), (2, 20)'), 'L1 seeded rows 1,2');
ok(write_retry($node0, 'CHECKPOINT'), 'L1 checkpoint');

# ============================================================
# L2: collapse baseline (GUC off).  node0 holds an uncommitted row2; a node1
# write of the DIFFERENT row1 fails closed 53R9H (the D11 false serialization).
# ============================================================
{
	my $a = $node0->background_psql('postgres', on_error_stop => 0);
	$a->query_safe('BEGIN');
	# LOCK-ONLY hold: an ACTIVE lock-only ITL slot on the block (triggers the
	# D11 deferral) without recycling the seed's data slot (so node1 can still
	# resolve the block via the propagated TT hint).
	$a->query_safe('SELECT v FROM g_t WHERE id = 2 FOR UPDATE');

	# node1 writes row1 (different row).  Off baseline -> read-image deferral ->
	# 53R9H fail-closed.
	my ($rc, $out, $err) = $node1->psql('postgres', 'UPDATE g_t SET v = 11 WHERE id = 1');
	like($err, qr/53R9H|cross[- ]node write/i,
		'L2 GUC-off: node1 write of a DIFFERENT row fails closed 53R9H (D11 false serialization)');

	$a->query_safe('ROLLBACK');
	$a->quit;
}

# Arm the wave on both nodes for the remaining legs.
for my $n ($node0, $node1)
{
	$n->safe_psql('postgres', 'ALTER SYSTEM SET cluster.block_self_contained = on');
	$n->safe_psql('postgres', 'SELECT pg_reload_conf()');
}
usleep(1_000_000);
is($node1->safe_psql('postgres', 'SHOW cluster.block_self_contained'), 'on',
	'L3 block_self_contained armed on both nodes');

# ============================================================
# L3: collapse LIFTED (GUC on).  Same setup -> node1's row1 write SUCCEEDS
# while node0's row2 stays uncommitted.
# ============================================================
{
	my $g0 = state_val($node1, 'xnode_lever', 'g_active_itl_transfer_count')
		   + state_val($node0, 'xnode_lever', 'g_active_itl_transfer_count');
	my $sk0 = state_val($node0, 'xnode_lever', 'g_stamp_skipped_count')
			+ state_val($node1, 'xnode_lever', 'g_stamp_skipped_count');

	my $a = $node0->background_psql('postgres', on_error_stop => 0);
	$a->query_safe('BEGIN');
	$a->query_safe('SELECT v FROM g_t WHERE id = 2 FOR UPDATE');   # lock-only hold on row2

	my $ok = write_retry($node1, 'UPDATE g_t SET v = 11 WHERE id = 1');   # DIFFERENT row1
	ok($ok, 'L3 GUC-on: node1 write of a DIFFERENT row SUCCEEDS (collapse lifted)');

	# node0's ROLLBACK finishes the lock-only ITL slot; the block has drifted
	# to node1, so D-g1 skips the stamp (block not resident here).
	$a->query_safe('ROLLBACK');
	$a->quit;

	my $g1 = state_val($node1, 'xnode_lever', 'g_active_itl_transfer_count')
		   + state_val($node0, 'xnode_lever', 'g_active_itl_transfer_count');
	cmp_ok($g1, '>', $g0, 'L3 an active-ITL block transfer was counted');

	# D-g1: node0's ROLLBACK finished its lock-only slot while the block was
	# drifted on node1, so the commit cleanout SKIPPED the stamp here.  Checked
	# via the local counter (pg_cluster_state) -- no cross-node data read, which
	# on this phantom harness would hit the Stage-6 recycled-slot boundary.
	my $sk1 = state_val($node0, 'xnode_lever', 'g_stamp_skipped_count')
			+ state_val($node1, 'xnode_lever', 'g_stamp_skipped_count');
	cmp_ok($sk1, '>', $sk0, 'L3 D-g1 stamp-skip counter advanced (drifted-block cleanout skipped)');
}

# ============================================================
# L4: counter surface on both nodes.
# ============================================================
for my $n ($node0, $node1)
{
	my $rows = $n->safe_psql('postgres',
		q{SELECT count(*) FROM pg_cluster_state
		   WHERE category='xnode_lever' AND key IN
		     ('g_active_itl_transfer_count','g_stamp_skipped_count',
		      'g_drift_resolved_via_tt_count')});
	is($rows, '3', 'L4 wave-g lever keys present (' . $n->name . ')');
}

$pair->stop_pair if $pair->can('stop_pair');
done_testing();
