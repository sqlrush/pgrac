#-------------------------------------------------------------------------
#
# 016_perfmon.pl
#    End-to-end regression for the cluster performance-monitoring
#    framework (cluster_pgstat) and pg_stat_cluster_nodes view
#    introduced at stage 0.28.
#
#    Verifies the SQL surface backed by cluster_pgstat.c and the
#    cluster_get_stat_nodes SRF in cluster_views.c:
#
#      - pg_stat_cluster_nodes returns exactly 1 row (single-node
#        pseudo-cluster at stage 0).
#      - Column types are (int4, text, text, timestamptz, text, text).
#      - state is 'online' (hardcoded at stage 0).
#      - startup_time is a sane timestamptz (greater than the static
#        epoch baseline).
#      - pgrac_version contains the stage tag.
#      - pg_version is exactly '16.13'.
#      - role reflects pgrac.conf when configured; defaults to
#        'unknown' when no pgrac.conf is present and node_id stays at
#        the fallback.
#      - pg_stat_cluster_counters contains the demo counter
#        cluster.inject.armed_count.
#      - The counter mirrors cluster_injection_armed_count: arming
#        an injection point bumps the value by 1.
#      - 0.16 / 0.17 baselines (46 wait events) unchanged.
#
# IDENTIFICATION
#    src/test/cluster_tap/t/016_perfmon.pl
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


my $node = PgracClusterNode->new('main');
$node->init;
$node->start;


# ----------
# Test 1: pg_stat_cluster_nodes returns exactly one row.
# ----------
is( $node->safe_psql('postgres',
		'SELECT count(*) FROM pg_stat_cluster_nodes'),
	'1',
	'pg_stat_cluster_nodes returns 1 row (single-node pseudo-cluster)');


# ----------
# Test 2: Column types are (int4, text, text, timestamptz, text, text).
# ----------
is( $node->safe_psql(
		'postgres', q{
	SELECT string_agg(format_type(atttypid, atttypmod), ',' ORDER BY attnum)
	  FROM pg_attribute
	 WHERE attrelid = 'pg_stat_cluster_nodes'::regclass
	   AND attnum > 0 AND NOT attisdropped
}),
	'integer,text,text,timestamp with time zone,text,text',
	'pg_stat_cluster_nodes columns are (int4, text, text, timestamptz, text, text)');


# ----------
# Test 3: state is 'online' (hardcoded at stage 0).
# ----------
is($node->get_cluster_node_state, 'online',
	'state is "online" at stage 0');


# ----------
# Test 4: startup_time is a sane timestamptz.
# ----------
is( $node->safe_psql(
		'postgres',
		q{SELECT startup_time > '2026-01-01'::timestamptz FROM pg_stat_cluster_nodes}
	),
	't',
	'startup_time is later than 2026-01-01 (sanity)');


# ----------
# Test 5: pgrac_version contains the stage tag.
# ----------
my $version = $node->safe_psql('postgres',
	'SELECT pgrac_version FROM pg_stat_cluster_nodes');
like($version, qr/pgrac v\d+\.\d+\.\d+-stage\d+\.\d+/,
	"pgrac_version matches semver-with-stage pattern (got: $version)");


# ----------
# Test 6: pg_version is '16.13'.
# ----------
is( $node->safe_psql('postgres',
		'SELECT pg_version FROM pg_stat_cluster_nodes'),
	'16.13',
	'pg_version is exactly "16.13"');


# ----------
# Test 7: role reflects pgrac.conf single-node fallback.  When no
# pgrac.conf is present (default in tmp_check), spec-0.19's
# load_single_node_fallback synthesises a primary node for the local
# id; pg_stat_cluster_nodes joins through cluster_conf_lookup_node
# and therefore reports 'primary'.
# ----------
is( $node->safe_psql('postgres',
		'SELECT role FROM pg_stat_cluster_nodes'),
	'primary',
	'role reflects single-node fallback (primary) when pgrac.conf is absent');


# ----------
# Test 8: pg_stat_cluster_counters contains the demo counter.
# ----------
is( $node->safe_psql(
		'postgres',
		q{SELECT name FROM pg_stat_cluster_counters WHERE name='cluster.inject.armed_count'}
	),
	'cluster.inject.armed_count',
	'pg_stat_cluster_counters has cluster.inject.armed_count entry');


# ----------
# Test 9: counter mirrors cluster_injection_armed_count, verified in a
# single backend session.  cluster_pgstat counters are per-process, so
# arm + read must happen in the same psql session for the mirror sync
# to observe the bump.  spec-0.27 §3.6 documents this isolation.
# ----------
is( $node->safe_psql(
		'postgres', q{
	SELECT cluster_inject_fault('cluster-init-pre-shmem', 'warning', 0);
	SELECT value FROM pg_stat_cluster_counters
	 WHERE name='cluster.inject.armed_count';
}),
	"t\n1",
	'cluster.inject.armed_count is 1 after arming 1 point in same session');


# ----------
# Test 10: baseline regression -- 0.16 / 0.17 view row counts unchanged.
# ----------
is( $node->safe_psql('postgres',
		'SELECT count(*) FROM pg_stat_cluster_wait_events'),
	'98',
	'pg_stat_cluster_wait_events returns 98 rows (spec-4.6 +1 GRD shard remaster)');

$node->stop;

done_testing();
