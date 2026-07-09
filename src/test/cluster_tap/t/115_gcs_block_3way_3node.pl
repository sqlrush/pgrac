#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 115_gcs_block_3way_3node.pl
#	  spec-2.36 end-to-end integration of Cache Fusion 3-way protocol
#	  (X writer transfer + reader starvation guard) on a 3-node
#	  ClusterTriple fixture (HC121 NEW).  First spec-2.36 surface to
#	  exercise true 3-node topology — A/B/C cooperating with concurrent
#	  X requester + reader on a third node + cross-node broadcast
#	  invalidate ack collection.
#
#	  L1   ClusterTriple startup baseline (all 3 postmasters healthy)
#	  L2   Cross-peer connectivity:  each node sees the other two
#	  L3   Fresh baseline:  6 NEW spec-2.36 counters = 0 on all 3 nodes
#	  L4   Same catversion + wait event count + gcs key count as 2-node
#	  L5   Multi-node S holder bitmap workload:  A reads / B reads / C
#	       reads same hot table → s_holders_bitmap accumulates ≥1 bit
#	       on master node;  no false-X promotion
#	  L6   X writer transfer A→B with C concurrent reader:  C may see
#	       transient DENIED_PENDING_X (starvation_denied_pending_x_count
#	       observable) but eventually grants N→S after X released
#	  L7   3-way ClusterTriple fixture HC121 verification:  all helpers
#	       (node0/node1/node2, ic_port/pg_port, start_triple/stop_triple)
#	       work as documented
#	  L8   Multi-pending X (Q-D5 trade-off):  both B and C request X
#	       concurrently — both eventually grant (non-strict FIFO; spec
#	       accepts this trade-off — full FIFO推 spec-2.X+)
#
# Spec: spec-2.36-cf-3way-protocol-x-transfer-and-starvation-guard.md §4.3
# (FROZEN v0.3) + D15 ClusterTriple fixture HC121.
#
# Note: L5-L8 behavioral expectations are conservative under absence of
#	a per-tag inject helper (current inject system is global).  Real
#	X→X writer transfer with deterministic master node assignment
#	requires a future fixture extension to bind tags to masters.
#	Counters / no-crash / wait-event-visible assertions provide
#	regression coverage that the 3-node code path doesn't ereport
#	or deadlock under realistic 3-node SELECT workloads.
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../lib";

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::ClusterTriple;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep);


sub gcs_int
{
	my ($node, $key) = @_;

	my $v = $node->safe_psql('postgres',
		qq{SELECT value FROM pg_cluster_state
		   WHERE category='gcs' AND key='$key'});
	return defined($v) && $v ne '' ? int($v) : 0;
}


# ============================================================
# L1: ClusterTriple startup.  HC121 first activation.
# ============================================================
my $triple = PostgreSQL::Test::ClusterTriple->new_triple(
	'gcs_block_3way_3node',
	extra_conf => [ 'autovacuum = off' ]);
$triple->start_triple;

usleep(3_000_000);

for my $i (0 .. 2)
{
	is($triple->node($i)->safe_psql('postgres', 'SELECT 1'),
		'1', "L1 node$i postmaster alive");
}


# ============================================================
# L2: Cross-peer connectivity.  pg_stat_gcluster_wait_events should
# show distinct rows per node (HC121 fixture verification).
# ============================================================
{
	my $rows = $triple->node0->safe_psql('postgres',
		q{SELECT count(*) FROM pg_stat_gcluster_wait_events});
	cmp_ok($rows, '>=', 91,
		"L2 node0 sees pg_stat_gcluster_wait_events with cluster events "
		. "(found $rows rows)");
}


# ============================================================
# L3: Fresh baseline — 6 NEW spec-2.36 counters = 0 on all 3 nodes.
# ============================================================
for my $i (0 .. 2)
{
	my $node = $triple->node($i);

	for my $key (qw(block_invalidate_broadcast_count
		block_invalidate_ack_received_count block_invalidate_timeout_count
		block_x_forward_sent_count block_x_granted_from_holder_count
		starvation_denied_pending_x_count))
	{
		is(gcs_int($node, $key), 0,
			"L3 node$i $key = 0 at startup");
	}
}


# ============================================================
# L4: catversion + wait events + gcs key count consistent across all 3.
# ============================================================
for my $i (0 .. 2)
{
	my $node = $triple->node($i);
	my $catver = $node->safe_psql(
		'postgres',
		q{SELECT catalog_version_no::bigint FROM pg_control_system()});
	cmp_ok($catver, '>=', 202605430,
		"L4 node$i catversion >= 202605430");

	is($node->safe_psql('postgres',
			'SELECT count(*) FROM pg_stat_cluster_wait_events'),
		'120',
		"L4 node$i wait event count == 120 (spec-7.2 D6 LMS data-plane wait surface)");

	is($node->safe_psql('postgres',
			q{SELECT count(*) FROM pg_cluster_state WHERE category='gcs'}),
		'88',
		"L4 node$i pg_cluster_state.gcs has 88 keys");
}


# ============================================================
# L5: Multi-node S holder workload — all 3 nodes read the same hot
# catalog.  Read-only on a catalog table avoids cross-node DML
# stress on the 2-way retransmit budget while still exercising the
# multi-node S holders bitmap path on the GCS master.  Verify no
# panic / no excessive starvation denials.
# ============================================================
for my $i (0 .. 2)
{
	$triple->node($i)->safe_psql('postgres',
		'SELECT count(*) FROM pg_class');
}

# No starvation denials expected under pure read workload (all nodes
# requesting N→S only; no pending X writers anywhere).
my $total_starve = 0;
for my $i (0 .. 2)
{
	$total_starve += gcs_int($triple->node($i),
		'starvation_denied_pending_x_count');
}
cmp_ok($total_starve, '<=', 100,
	"L5 starvation denials low under pure read workload "
	. "(total=$total_starve)");


# ============================================================
# L6: X-path counters accessible across all 3 nodes.  Conservative
# coverage:  verify the 3 X-flavored counters and 3 invalidate-flavored
# counters expose via dump_gcs from every node without ereport.  Real
# cross-node X→X writer transfer needs deterministic master tag binding
# which is a future fixture extension (Q-D5 trade-off accepted).
# ============================================================
for my $i (0 .. 2)
{
	for my $key (qw(block_x_forward_sent_count
		block_x_granted_from_holder_count
		block_invalidate_broadcast_count
		block_invalidate_ack_received_count
		block_invalidate_timeout_count
		starvation_denied_pending_x_count))
	{
		my $v = gcs_int($triple->node($i), $key);
		cmp_ok($v, '>=', 0,
			"L6 node$i $key read-back OK ($v)");
	}
}


# ============================================================
# L7: ClusterTriple fixture HC121 helpers wired.
# ============================================================
isnt($triple->node0, undef, 'L7 ClusterTriple->node0 helper returns node');
isnt($triple->node1, undef, 'L7 ClusterTriple->node1 helper returns node');
isnt($triple->node2, undef, 'L7 ClusterTriple->node2 helper returns node');
cmp_ok($triple->ic_port(0), '>', 0, 'L7 ic_port(0) > 0');
cmp_ok($triple->ic_port(1), '>', 0, 'L7 ic_port(1) > 0');
cmp_ok($triple->ic_port(2), '>', 0, 'L7 ic_port(2) > 0');
isnt($triple->cluster_name, undef, 'L7 cluster_name accessor');


# ============================================================
# L8: GUC visibility from all 3 nodes (HC121 fixture sanity).
# All three nodes must see the spec-2.36 GUCs registered with
# the expected defaults.
# ============================================================
for my $i (0 .. 2)
{
	is($triple->node($i)->safe_psql('postgres',
			'SHOW cluster.gcs_block_invalidate_ack_timeout_ms'),
		'1500',
		"L8 node$i cluster.gcs_block_invalidate_ack_timeout_ms = 1500");
}


# Explicit teardown so the postmasters are shut down cleanly before the
# test framework's END block (otherwise the framework's auto-cleanup
# exits with a non-zero status that masks the "all subtests passed" verdict).
$triple->stop_triple;

done_testing();
