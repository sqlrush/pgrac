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
#      - It returns exactly 46 rows (one per cluster wait event from
#        spec-0.11).
#      - It exposes 10 distinct cluster wait classes (matching
#        docs/cluster-wait-events-design.md §2.1).
#      - Per-class row counts match the design doc (GES 5, PCM 6,
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
# Total row count: 46.
# ----------
is($node->safe_psql('postgres',
		'SELECT count(*) FROM pg_stat_cluster_wait_events'),
	'46',
	'pg_stat_cluster_wait_events returns 46 rows (matches spec-0.11 registration)');


# ----------
# Distinct type count: 10.
# ----------
is($node->safe_psql('postgres',
		'SELECT count(DISTINCT type) FROM pg_stat_cluster_wait_events'),
	'10',
	'10 distinct Cluster: * types');


# ----------
# Per-class counts (anchored to docs/wait-events-design.md §2.1).
# ----------
my %expected = (
	'Cluster: GES' => 5,
	'Cluster: PCM' => 6,
	'Cluster: BufferShip' => 5,
	'Cluster: SCN' => 4,
	'Cluster: Reconfig' => 5,
	'Cluster: Recovery' => 5,
	'Cluster: Sinval' => 3,
	'Cluster: Interconnect' => 5,
	'Cluster: Undo' => 4,
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
# Spot-check 5 event names exist.
# ----------
for my $name ('GesEnqueueAcquire', 'PcmBlockReadNS', 'SinvalInjectLocalQueue',
              'InterconnectRdmaSend', 'AdgScnSyncWait')
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


$node->stop;

done_testing();
