#-------------------------------------------------------------------------
#
# 011_gviews.pl
#    End-to-end regression for the cluster GLOBAL views framework
#    introduced in stage 0.17.
#
#    Stage 0.17 ships ONE global view: pg_stat_gcluster_wait_events,
#    backed by the cluster_get_gcluster_wait_events SRF (OID 8899) and
#    declared in src/backend/catalog/system_views.sql.  At 0.17 the
#    SRF emits 51 rows for the local node only (node_id = the
#    cluster.node_id GUC value, default -1); the (node_id, type, name)
#    column shape is the stable contract from 0.17 onward and stays
#    unchanged when Stage 6+ swaps the SRF body for a real cross-node
#    RPC fan-out.
#
#    What this test verifies:
#      - The global view exists and is queryable.
#      - It returns exactly 123 rows (1 node x 123 cluster wait events).
#      - It exposes exactly 1 distinct node_id at 0.17 (placeholder).
#      - The single node_id matches the cluster.node_id GUC.
#      - Per-class row counts match docs/wait-events-design.md §2.1.
#      - The (type, name) projection is identical to the local
#        pg_stat_cluster_wait_events view (column contract anchor).
#      - Specific event names appear once (with correct node_id).
#      - PG-native pg_stat_* views are unaffected.
#
# IDENTIFICATION
#    src/test/cluster_tap/t/011_gviews.pl
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../lib";

use PostgreSQL::Test::Cluster;
use Test::More;
use PgracClusterNode;


my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
$node->start;


# ----------
# Read the cluster.node_id GUC.  At default (no postgresql.conf override)
# this is -1 ("unconfigured"); spec-0.13.  The SRF projects this value
# into every row at stage 0.17.
# ----------
my $node_id = $node->safe_psql('postgres', 'SHOW cluster.node_id');


# ----------
# Total row count: 1 node x 121 events (spec-6.13 RDMA + spec-5.22b D2-6 undo-block waits).
# ----------
is($node->safe_psql('postgres',
		'SELECT count(*) FROM pg_stat_gcluster_wait_events'),
	'123',
	'pg_stat_gcluster_wait_events returns 123 rows (spec-6.13 RDMA + spec-5.22b D2-6 undo grant-plane +3 + spec-7.2 LMS data-plane +2; merge sum 118+3+2)');

is($node->safe_psql(
		'postgres',
		'SELECT node_id, dg_role, dg_mode, adg_enabled, mrp_status, lag_bytes
		   FROM pg_stat_gcluster_adg'),
	'-1|primary|async|f|disabled|0',
	'pg_stat_gcluster_adg returns default local-only ADG row');


# ----------
# Distinct node count: 1 (placeholder; Stage 6+ will multiply by N).
# ----------
is($node->safe_psql('postgres',
		'SELECT count(DISTINCT node_id) FROM pg_stat_gcluster_wait_events'),
	'1',
	'pg_stat_gcluster_wait_events exposes 1 distinct node_id at stage 0.17');


# ----------
# That node_id must equal the cluster.node_id GUC.
# ----------
is($node->safe_psql('postgres',
		'SELECT DISTINCT node_id::text FROM pg_stat_gcluster_wait_events'),
	$node_id,
	"single node_id matches cluster.node_id GUC ($node_id)");


# ----------
# Per-class row counts (anchored to docs/wait-events-design.md §2.1).
# ----------
my %expected = (
	'Cluster: GES' => 5,
	'Cluster: PCM' => 24,	# spec-6.2 D10: +4 Smart Fusion authority waits
	'Cluster: BufferShip' => 5,
	'Cluster: SCN' => 4,
	'Cluster: Reconfig' => 8,    # spec-5.18 D12: +ReconfigNodeRemoveCleanupWait
	'Cluster: Recovery' => 7,    # spec-4.12 D6: +ClusterWriteFenceVerify
	'Cluster: Sinval' => 6,
	'Cluster: Interconnect' => 11,	# spec-6.13: +busypoll + inline send waits; spec-7.2 D6: +LmsDataRecv/Send
	'Cluster: Undo' => 7,    # spec-5.22b D2-6: +UndoBlock Grant/Invalidate/Remaster waits
	'Cluster: ADG' => 4,
);

for my $type (sort keys %expected)
{
	my $count = $node->safe_psql(
		'postgres',
		"SELECT count(*) FROM pg_stat_gcluster_wait_events WHERE type = '$type'");
	is($count, $expected{$type},
		"$type has $expected{$type} events in pg_stat_gcluster_wait_events");
}


# ----------
# Column contract: stripping node_id, the (type, name) projection of
# pg_stat_gcluster_wait_events at 0.17 must be identical to the local
# pg_stat_cluster_wait_events view (since the global SRF only emits
# the local node).  This is the load-bearing assertion that locks the
# column contract from 0.17 onward.
# ----------
is($node->safe_psql('postgres', q{
		SELECT count(*) FROM (
			SELECT type, name FROM pg_stat_gcluster_wait_events
			EXCEPT
			SELECT type, name FROM pg_stat_cluster_wait_events
		) d
	}),
	'0',
	'pg_stat_gcluster_wait_events (type,name) projection equals pg_stat_cluster_wait_events');


# ----------
# Spot-check 6 event names exist exactly once with the expected node_id.
# ----------
for my $name ('GesEnqueueAcquire', 'PcmBlockReadNS', 'SinvalInjectLocalQueue',
              'InterconnectRdmaSend', 'ClusterICRdmaFallback', 'AdgWalReceiveLag')
{
	my $count = $node->safe_psql(
		'postgres',
		"SELECT count(*) FROM pg_stat_gcluster_wait_events "
		. "WHERE name = '$name' AND node_id::text = '$node_id'");
	is($count, '1',
		"spot-check: '$name' present exactly once with node_id=$node_id");
}


# ----------
# PG-native pg_stat_* views unaffected.
# ----------
my $native_activity_count = $node->safe_psql('postgres',
	q{SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'client backend'});
cmp_ok($native_activity_count, '>=', 1,
	'pg_stat_activity still works after cluster gview extension');


$node->stop;

done_testing();
