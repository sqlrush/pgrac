#-------------------------------------------------------------------------
#
# 017_debug.pl
#    End-to-end regression for the cluster diagnostic snapshot view
#    pg_cluster_state introduced at stage 0.29.
#
#    Verifies the SQL surface backed by cluster_debug.c:
#
#      - pg_cluster_state view returns >= 20 rows (stage 0 ~25).
#      - All 7 categories are present (shmem / guc / ic / inject /
#        pgstat / conf / phase).
#      - Column types are (text, text, text); no NULL values.
#      - shmem.magic equals the expected CLUSTER_SHMEM_MAGIC value
#        (0x50475243 / "PGRC" little-endian).
#      - guc.cluster.node_id reflects the live GUC value.
#      - ic.active_tier_name matches cluster.interconnect_tier.
#      - All 14 injection points appear with .fault_type / .hits
#        (6 baseline + 8 stage-0.30 sweep additions).
#      - At least one pgstat counter is present.
#      - conf.node_count matches pg_cluster_nodes (spec-0.19 view).
#      - phase.cluster_phase is a recognised lifecycle string.
#      - Baseline regression: 0.16 / 0.17 / 0.19 / 0.27 / 0.28 view
#        row counts unchanged.
#
# IDENTIFICATION
#    src/test/cluster_tap/t/017_debug.pl
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
# Test 1: pg_cluster_state view exists and returns enough rows.
# ----------
my $row_count = $node->safe_psql('postgres',
	'SELECT count(*) FROM pg_cluster_state');
ok($row_count >= 20,
	"pg_cluster_state returns $row_count rows (>= 20 expected at stage 0)");


# ----------
# Test 2: All categories present.
# ----------
is( $node->safe_psql(
		'postgres',
		q{SELECT string_agg(DISTINCT category COLLATE "C", ','
		      ORDER BY category COLLATE "C")
		    FROM pg_cluster_state}),
	'advisory,block_format,buffer_format,catalog,cf,cluster_cssd,cluster_stats,conf,cr,cr_coord,cr_pool,ctrc,diag,dl,gcs,gcs_recovery,ges,grd,grd_recovery,guc,hang,hw,ic,inject,ir,ko,lck,lmd,lmon,lms,multixact_current,normal_start,pcm,pgstat,phase,r4,reconfig,reconfig_join,reconfig_touched,recovery,resolver_cache,scn,sequence,shared_fs,shmem,sinval,smart_fusion,ts,tt_2pc,tt_recovery,tt_status,tt_status_hint,undo,undo.cleaner.worker.0,undo.cleaner.worker.1,undo.cleaner.worker.2,undo.cleaner.worker.3,undo.cleaner.worker.4,undo.cleaner.worker.5,undo.cleaner.worker.6,undo.cleaner.worker.7,undo_cleaner,update_trace,update_trace_event,visibility,wal_thread,write_fence,xid_stripe,xnode_lever,xnode_profile',
	'all 70 categories appear (including normal-start, CTRC, cleaner workers and update trace)');

is( $node->safe_psql('postgres',
			q{SELECT count(*) FROM pg_cluster_state WHERE category='gcs'}),
		'117',
		'gcs category has 117 keys in the current registry');


# ----------
# Test 3: Column types are (text, text, text).
# ----------
is( $node->safe_psql(
		'postgres', q{
	SELECT string_agg(format_type(atttypid, atttypmod), ',' ORDER BY attnum)
	  FROM pg_attribute
	 WHERE attrelid = 'pg_cluster_state'::regclass
	   AND attnum > 0 AND NOT attisdropped
}),
	'text,text,text',
	'pg_cluster_state columns are (text, text, text)');


# ----------
# Test 4: No NULL values (spec-0.29 §3.2 contract).
# ----------
is( $node->safe_psql(
		'postgres',
		q{SELECT count(*) FROM pg_cluster_state
		   WHERE category IS NULL OR key IS NULL OR value IS NULL}),
	'0',
	'pg_cluster_state has no NULL values (NOT NULL contract)');

is(
	$node->get_cluster_state_value('pcm', 'resource_x_proof_readiness'),
	'AVAILABLE_PROOF_KIND',
	'Resource-X exact proof-kind observation is available');


# ----------
# Test 5: shmem.magic matches CLUSTER_SHMEM_MAGIC ("PGRC" LE = 0x50475243).
# ----------
is($node->get_cluster_state_value('shmem', 'magic'),
	'0x50475243',
	'shmem.magic equals 0x50475243 ("PGRC" little-endian)');


# ----------
# Test 6: guc.cluster.node_id reflects the live GUC.
# ----------
is($node->get_cluster_state_value('guc', 'cluster.node_id'),
	$node->safe_psql('postgres', 'SHOW cluster.node_id'),
	'guc.cluster.node_id matches SHOW cluster.node_id');


# ----------
# Test 7: ic.active_tier_name matches cluster.interconnect_tier GUC.
# ----------
is($node->get_cluster_state_value('ic', 'active_tier_name'),
	$node->safe_psql('postgres', 'SHOW cluster.interconnect_tier'),
	'ic.active_tier_name matches SHOW cluster.interconnect_tier');


# ----------
# Test 8: All injection points appear with .fault_type / .hits keys.
# ----------
is( $node->safe_psql(
		'postgres',
		q{SELECT count(*) FROM pg_cluster_state
		   WHERE category='inject' AND key LIKE '%.fault_type'}),
	'187',
	'all 187 injection points have a .fault_type entry, including the CTRC stage barrier');

is( $node->safe_psql(
		'postgres',
		q{SELECT count(*) FROM pg_cluster_state
		   WHERE category='inject' AND key LIKE '%.hits'}),
	'187',
	'all 187 injection points have a .hits entry, including the CTRC stage barrier');


# ----------
# Test 9: pgstat category has at least 1 counter; conf.node_count
# matches pg_cluster_nodes; phase is a recognised string.
# ----------
my $pgstat_count = $node->safe_psql('postgres',
	q{SELECT count(*) FROM pg_cluster_state WHERE category='pgstat'});
ok($pgstat_count >= 1,
	"pgstat category has $pgstat_count counter(s) (>= 1 expected)");

is($node->get_cluster_state_value('conf', 'node_count'),
	$node->safe_psql('postgres', 'SELECT count(*)::text FROM pg_cluster_nodes'),
	'conf.node_count matches pg_cluster_nodes row count');

my $phase = $node->get_cluster_state_value('phase', 'cluster_phase');
like($phase, qr/^(init|running|shutdown|\(unset\))$/,
	"phase.cluster_phase is a recognised string (got: $phase)");


# ----------
# Test 10: Baseline regression -- 0.16 view row count unchanged.
# ----------
is( $node->safe_psql('postgres',
		'SELECT count(*) FROM pg_stat_cluster_wait_events'),
	'123',
	'pg_stat_cluster_wait_events returns 123 rows (spec-6.13 RDMA + spec-5.22b D2-6 undo grant-plane +3 + spec-7.2 LMS data-plane +2; merge sum 118+3+2)');

$node->stop;

done_testing();
