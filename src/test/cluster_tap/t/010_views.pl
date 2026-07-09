#-------------------------------------------------------------------------
#
# 010_views.pl
#    End-to-end regression for the cluster views framework introduced
#    in stage 0.16.
#
#    Stage 0.16 ships ONE view: pg_stat_cluster_wait_events, backed by
#    the cluster_get_wait_events SRF (OID 8898) and declared in
#    src/backend/catalog/system_views.sql.  This test verifies the SRF
#    + view + SQL pipeline is intact end to end on a real PG instance:
#
#      - The view exists and is queryable.
#      - It returns exactly 85 rows (one per cluster wait event through
#        spec-2.33;  spec-2.33 83 + spec-2.34 +2 reliability hardening).
#      - It exposes 10 distinct cluster wait classes (matching
#        docs/cluster-wait-events-design.md §2.1).
#      - Per-class row counts match the design doc (GES 5, PCM 16,
#        BufferShip 5, SCN 4, Reconfig 5, Recovery 5, Sinval 3,
#        Interconnect 5, Undo 4, ADG 4).
#      - Specific event names are present.
#      - PG-native pg_stat_* views are unaffected.
#
# IDENTIFICATION
#    src/test/cluster_tap/t/010_views.pl
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
# Total row count: 121 (spec-6.13 RDMA + spec-5.22b D2-6 undo-block grant-plane waits).
# ----------
is($node->safe_psql('postgres',
		'SELECT count(*) FROM pg_stat_cluster_wait_events'),
	'121',
	'pg_stat_cluster_wait_events returns 121 rows (spec-6.13 RDMA wait surface)');

is($node->safe_psql(
		'postgres',
		'SELECT node_id, dg_role, dg_mode, adg_enabled, mrp_status, lag_bytes
		   FROM pg_stat_cluster_adg'),
	'-1|primary|async|f|disabled|0',
	'pg_stat_cluster_adg returns default primary/ADG-off state');

is($node->safe_psql(
		'postgres',
		'SELECT receive_lsn IS NULL, apply_lsn IS NULL
		   FROM pg_stat_cluster_adg'),
	't|t',
	'pg_stat_cluster_adg leaves ADG LSNs NULL while disabled');


# ----------
# Distinct type count: current cluster wait-event type roster.
# ----------
is($node->safe_psql('postgres',
			'SELECT count(DISTINCT type) FROM pg_stat_cluster_wait_events'),
		'13',
		'13 distinct Cluster: * types');


# ----------
# Per-class counts (anchored to docs/wait-events-design.md §2.1).
# ----------
my %expected = (
	'Cluster: GES' => 5,
	'Cluster: PCM' => 24,	# spec-6.2 D10: +4 Smart Fusion authority waits
	'Cluster: BufferShip' => 5,
	'Cluster: SCN' => 4,
	'Cluster: Reconfig' => 8,    # spec-5.18 D12: +ReconfigNodeRemoveCleanupWait
	'Cluster: Recovery' => 7,    # spec-4.12 D6: +ClusterWriteFenceVerify
	'Cluster: Sinval' => 6,
	'Cluster: Interconnect' => 9,	# spec-6.13: +busypoll + inline send waits
	'Cluster: Undo' => 7,    # spec-5.22b D2-6: +UndoBlock Grant/Invalidate/Remaster waits
	'Cluster: ADG' => 4,
);

for my $type (sort keys %expected)
{
	my $count = $node->safe_psql(
		'postgres',
		"SELECT count(*) FROM pg_stat_cluster_wait_events WHERE type = '$type'");
	is($count, $expected{$type},
		"$type has $expected{$type} events");
}


# ----------
# Spot-check 6 event names exist.
# ----------
for my $name ('GesEnqueueAcquire', 'PcmBlockReadNS', 'SinvalInjectLocalQueue',
              'InterconnectRdmaSend', 'ClusterICRdmaFallback', 'AdgWalReceiveLag')
{
	my $count = $node->safe_psql(
		'postgres',
		"SELECT count(*) FROM pg_stat_cluster_wait_events WHERE name = '$name'");
	is($count, '1', "spot-check: '$name' present exactly once");
}


# ----------
# PG-native pg_stat_* views unaffected.
# ----------
my $native_activity_count = $node->safe_psql('postgres',
	q{SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'client backend'});
cmp_ok($native_activity_count, '>=', 1,
	'pg_stat_activity still works after cluster view extension');

# ----------
# spec-6.1: pg_stat_cluster_ic mux/RDMA observability view.
# ----------
is($node->safe_psql('postgres',
		q{SELECT count(*) FROM pg_stat_cluster_ic}),
	'1',
	'pg_stat_cluster_ic returns one single-node fallback row');

is($node->safe_psql('postgres',
		q{SELECT transport || '|' || rdma_state || '|' || provider
		    FROM pg_stat_cluster_ic}),
	'tcp|disabled|auto',
	'pg_stat_cluster_ic exposes TCP fallback transport defaults');

is($node->safe_psql('postgres',
		q{SELECT string_agg(column_name, ',' ORDER BY ordinal_position)
		    FROM information_schema.columns
		   WHERE table_name = 'pg_stat_cluster_ic'}),
	'node_id,transport,rdma_state,provider,rdma_addr,rdma_gid,rdma_port,mr_registered,cq_depth,fallback_count,send_count,recv_count,bytes_send,bytes_recv,block_sge_send_count,block_sge_fallback_count,tier3_send_count,inline_send_count,unsignaled_batch_count,busypoll_us_burned,busypoll_fallback_count,block_reply_lane_state,block_reply_lane_fallback_count,block_reply_lane_error_count,latency_us_sum,latency_sample_count,last_error_code,last_error,last_block_reply_error',
	'pg_stat_cluster_ic column contract matches spec-6.13 D8');


$node->stop;

done_testing();
