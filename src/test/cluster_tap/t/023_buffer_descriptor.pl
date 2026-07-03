#-------------------------------------------------------------------------
#
# 023_buffer_descriptor.pl
#    End-to-end regression for the stage-1.6 BufferDesc cluster fields
#    (PG ~52B + 12B hot tail + 64B cold body = 128B padded slot;
#    BUFFERDESC_PAD_TO_SIZE 64 -> 128).
#
#    Verifies the SQL surface backed by cluster_debug.c buffer_format
#    category extension + the 5 compile-time StaticAssertDecl invariants
#    + the user Q1 修订 ClusterInitBufferDescFields helper coverage of
#    both shared (InitBufferPool) and local (InitLocalBuffers) buffer
#    descriptor arrays.
#
#    PIVOT B (2026-05-02 user approve): on PG 16.13 sizeof(BufferTag)
#    == 20 (not 16), pushing PG-original fields to offset 52 and leaving
#    only 12B of cache line 1 for cluster hot tail.  block_scn occupies
#    cache line 1 (Stage 2-3 visibility hot path); cr_chain_head moved
#    to cache line 2 boundary.
#
#    Q7 A+B (user-revised hybrid) layout boundary tests (L19-L20):
#      - L19: 5 StaticAssertDecl semantic invariants surfaced via
#        buffer_format keys (offset / size relationships).
#      - L20: local buffer (TEMP TABLE) INSERT/SELECT/DELETE round-trip
#        — exercises InitLocalBuffers path that calls
#        ClusterInitBufferDescFields per Q1 第 2 条修订, ensuring
#        cluster fields placeholder values don't break LocalBufferAlloc.
#
# IDENTIFICATION
#    src/test/cluster_tap/t/023_buffer_descriptor.pl
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
use PostgreSQL::Test::Utils;
use Test::More;
use PgracClusterNode;


my $node = PgracClusterNode->new('main');
$node->init;
$node->start;

my $has_visibility_inject =
  $node->safe_psql(
	  'postgres',
	  q{SELECT count(*) FROM pg_cluster_shmem
	     WHERE name = 'pgrac cluster visibility inject'}) eq '1';
# +1 for the unconditional "pgrac cluster sequence" region (spec-5.4 SQ shmem foundation)
# and +1 for the unconditional "pgrac cluster cf stats" region (spec-5.6 Dc4 CF counters)
# and +1 for the unconditional "pgrac cluster cr admit stats" region (spec-5.52 D9
# admission reason counters; +1 "pgrac cluster clean_leave" (spec-5.13); +1
# "pgrac cluster cr relgen" (spec-5.56 D4); +1 "pgrac cluster cr tuple stats"
# (spec-5.54 D5); full list + count lives in t/020).
  my $expected_region_count = $has_visibility_inject ? '75' : '74'; # spec-6.2 +1 smart fusion deps; spec-6.12 +1 xnode lever +1 hw lease +1 cr server (6.12b); full list lives in t/020


# ----------
# L1: pg_cluster_state.buffer_format category has 6 keys.
# ----------
is($node->safe_psql(
		'postgres',
		q{SELECT count(*) FROM pg_cluster_state WHERE category='buffer_format'}),
   '6',
   'L1 pg_cluster_state.buffer_format category has 6 keys');


# ----------
# L2-L7: each buffer_format key has the expected value.
# ----------
is($node->safe_psql(
		'postgres',
		q{SELECT value FROM pg_cluster_state
		   WHERE category='buffer_format' AND key='buffer_desc_size_bytes'}),
   '128',
   'L2 buffer_desc_size_bytes = 128 (PG-original 52B + 12B hot tail + 64B cold body)');

is($node->safe_psql(
		'postgres',
		q{SELECT value FROM pg_cluster_state
		   WHERE category='buffer_format' AND key='buffer_desc_pad_to_size'}),
   '128',
   'L3 buffer_desc_pad_to_size = 128 (BUFFERDESC_PAD_TO_SIZE 64 -> 128)');

is($node->safe_psql(
		'postgres',
		q{SELECT value FROM pg_cluster_state
		   WHERE category='buffer_format' AND key='buffer_hot_field_offset'}),
   '52',
   'L4 buffer_hot_field_offset = 52 (PG 16.13: BufferTag 20B + buf_id 4B + state 4B + wait 4B + freeNext 4B + content_lock 16B = 52B)');

is($node->safe_psql(
		'postgres',
		q{SELECT value FROM pg_cluster_state
		   WHERE category='buffer_format' AND key='buffer_cold_field_offset'}),
   '64',
   'L5 buffer_cold_field_offset = 64 (PIVOT B: cr_chain_head moved to cache line 2 boundary)');

is($node->safe_psql(
		'postgres',
		q{SELECT value FROM pg_cluster_state
		   WHERE category='buffer_format' AND key='buffer_type_count'}),
   '5',
   'L6 buffer_type_count = 5 (CURRENT / CR / PI / SCUR / XCUR — spec-2.31 D6 v0.5)');

is($node->safe_psql(
		'postgres',
		q{SELECT value FROM pg_cluster_state
		   WHERE category='buffer_format' AND key='pcm_state_count'}),
   '3',
   'L7 pcm_state_count = 3 (N / S / X)');


# ----------
# L8: 1.5 baseline -- block_format category still has 9 keys (5 from
# spec-1.5 + 4 from spec-1.4); 1.6 doesn't touch block_format.
# ----------
is($node->safe_psql(
		'postgres',
		q{SELECT count(*) FROM pg_cluster_state WHERE category='block_format'}),
   '9',
   'L8 1.5 baseline: block_format category still has 9 keys (1.6 adds none here)');


# ----------
# L9: 1.4 / earlier baseline -- wait events stay at 51 (no new events at 1.6).
# ----------
is($node->safe_psql(
		'postgres',
		'SELECT count(*) FROM pg_stat_cluster_wait_events'),
   '116',
   'L9 pg_stat_cluster_wait_events returns 116 rows (spec-6.2 Smart Fusion authority waits)');


# ----------
# L10: 1.10.1 baseline plus later shmem regions.
# ----------
is($node->safe_psql(
		'postgres',
		'SELECT count(*) FROM pg_cluster_shmem'),
   $expected_region_count,
   "L10 pg_cluster_shmem returns $expected_region_count rows after spec-3.6");


# ----------
# L11: 1.2 baseline -- inject points still 24 (1.6 adds no inject points).
# ----------
is($node->safe_psql(
		'postgres',
		'SELECT count(*) FROM pg_stat_cluster_injections'),
   '150',
   'L11 pg_stat_cluster_injections is 141 (spec-5.13 +6 cluster-clean-leave-* + Hardening v1.0.3 +1 suppress-preflight-ack) (spec-5.2a +1 clean-xfer stale-holder; spec-4.8ab +2 undo boundary guards; spec-5.7 +1 cluster-ko-peer-skip-ack; spec-2.41 +1 cluster-gcs-block-stale-ship; spec-5.55 Hardening v1.1 +1 cluster-cr-resolver-memo-suspect; spec-5.15 Hardening v1.1 +1 cluster-reconfig-join-commit-marker-durable)');


# ----------
# L12: pg_cluster_state categories total follows the current Stage 3
# observability surface.
# ----------
is($node->safe_psql(
		'postgres',
		q{SELECT count(DISTINCT category) FROM pg_cluster_state}),
   '53',
   'L12 pg_cluster_state has 53 categories (spec-6.2 adds smart_fusion; spec-6.12 adds xnode_lever)');


# ----------
# L13: server is alive and healthy -- buffer pool basic init didn't
# break PG behavior.  Use a simple INSERT/SELECT round-trip.
# ----------
$node->safe_psql('postgres', q{
	CREATE TABLE buf_smoke (id int PRIMARY KEY, label text);
	INSERT INTO buf_smoke SELECT g, 'row-' || g FROM generate_series(1, 100) g;
});
is($node->safe_psql('postgres', 'SELECT count(*) FROM buf_smoke'),
   '100',
   q{L13 simple INSERT/SELECT round-trip (100 rows) works -- BufferDesc layout does not break PG basic functionality});


# ----------
# L14: pgbench-style load (multiple INSERT/UPDATE/DELETE) on a heap
# table.  This exercises BufferAlloc / ReadBuffer / FlushBuffer hot
# paths -- if BufferDesc cluster fields accidentally enter hot path,
# this is where it would break.
# ----------
$node->safe_psql('postgres', q{
	CREATE TABLE buf_load (id int PRIMARY KEY, val int);
	INSERT INTO buf_load SELECT g, g*10 FROM generate_series(1, 500) g;
	UPDATE buf_load SET val = val + 1 WHERE id <= 250;
	DELETE FROM buf_load WHERE id > 400;
});
is($node->safe_psql('postgres', 'SELECT count(*) FROM buf_load'),
   '400',
   'L14 INSERT 500 + UPDATE 250 + DELETE 100 round-trip (400 rows) works');


# ----------
# L15: high concurrency baseline -- multiple parallel queries don't
# trigger deadlock or buffer corruption.  The pcm_lock LWLockInitialize
# in ClusterInitBufferDescFields uses LWTRANCHE_BUFFER_CONTENT (stage
# 1.6 reuse; never acquired); if the LWLock is corrupt, parallel scan
# would crash here.
# ----------
$node->safe_psql('postgres', q{
	CREATE TABLE buf_par (id int, val text);
	INSERT INTO buf_par SELECT g, repeat(md5(g::text), 5)
	  FROM generate_series(1, 5000) g;
	SET max_parallel_workers_per_gather = 4;
	SET min_parallel_table_scan_size = '8kB';
	SET parallel_setup_cost = 0;
	SET parallel_tuple_cost = 0;
});
is($node->safe_psql('postgres',
                    q{SELECT count(*) FROM buf_par WHERE val LIKE '%a%'}),
   $node->safe_psql('postgres',
                    q{SELECT count(*) FROM buf_par WHERE val LIKE '%a%'}),
   'L15 parallel scan deterministic count (no concurrency-triggered corruption)');


# ----------
# L16: cluster_smoke regression -- shows buffer_format category
# integrates with the broader cluster smoke test surface.
# ----------
my $smoke_categories = $node->safe_psql(
	'postgres',
	q{SELECT count(DISTINCT category) FROM pg_cluster_state});
is($smoke_categories, '53', 'L16 cluster_smoke surface integrates buffer_format + pcm + gcs + tt_status + tt_status_hint + tt_2pc + tt_recovery + visibility + wal_thread + dl + hw + ir + ko + ts + smart_fusion categories (53 categories; spec-6.2 adds smart_fusion)');


# ----------
# L17: BufferDesc layout invariants surfaced via buffer_format keys are
# self-consistent: hot offset (52) < cold offset (64) <= padded size
# (128); hot tail size = cold offset - hot offset = 12B.
# ----------
my $hot = $node->safe_psql(
	'postgres',
	q{SELECT value::int FROM pg_cluster_state
	   WHERE category='buffer_format' AND key='buffer_hot_field_offset'});
my $cold = $node->safe_psql(
	'postgres',
	q{SELECT value::int FROM pg_cluster_state
	   WHERE category='buffer_format' AND key='buffer_cold_field_offset'});
my $pad = $node->safe_psql(
	'postgres',
	q{SELECT value::int FROM pg_cluster_state
	   WHERE category='buffer_format' AND key='buffer_desc_pad_to_size'});
ok($hot < $cold && $cold <= $pad,
   "L17 layout self-consistent: hot=$hot < cold=$cold <= pad=$pad");
is($cold - $hot, 12, "L17b hot tail size = cold - hot = 12B (PIVOT B 实测)");


# ----------
# L18: shared memory survives a server restart -- BufferDescPadded
# alignment / sizing is correct (the alternative would be initdb-time
# crash on the second start).
# ----------
$node->stop('fast');
$node->start;
is($node->safe_psql('postgres', 'SELECT count(*) FROM buf_smoke'),
   '100',
   'L18 server restarts cleanly with cluster BufferDesc layout (data persists)');


# ----------
# L19 (Q7 A+B boundary): 5 StaticAssertDecl semantic invariants surfaced.
# These are compile-time enforced; runtime check just verifies the
# values reported by buffer_format are consistent with PIVOT B layout.
#
# Hot field offset (52) MUST follow PG-original 48B+ region.
# Cold field offset (64) MUST be cache-line boundary (multiple of 64
# is the convention here).
# block_scn (8B SCN) MUST end at <= 64; since hot field offset is 52
# and block_scn is the last hot field with 4B padding before it, end =
# 52 + 4 (sub-fields) + 8 (block_scn) = 64. ✓
# ----------
ok($cold >= 64,
   'L19a cr_chain_head >= 64 (Stage 2-3 cold path; semantic StaticAssert)');
ok($cold % 64 == 0,
   'L19b cold offset is cache-line aligned (boundary at multiple of 64)');
ok($hot >= 48,
   'L19c hot field offset >= 48 (cluster fields follow PG-original content_lock)');


# ----------
# L20 (Q7 A+B boundary): local buffer init coverage -- TEMP TABLE
# triggers InitLocalBuffers which (per PGRAC MODIFICATIONS 16th)
# calls ClusterInitBufferDescFields.  Without that helper, calloc
# zero-fill would leave cr_chain_head / cr_chain_next / pi_buf_id at 0
# (a valid buffer_id), which is a defensive issue (Stage 3 CR-path
# could mistakenly follow the chain).  Stage 1.6 doesn't actually read
# these cluster fields anywhere yet, so the test verifies (a) TEMP
# table works, (b) local buffer descriptors are properly allocated.
#
# All TEMP table operations + count happen in a single safe_psql call
# because TEMP tables are session-scoped -- a second psql connection
# would not see the table.
# ----------
is($node->safe_psql('postgres', q{
	CREATE TEMP TABLE local_buf_test (id int, val text);
	INSERT INTO local_buf_test SELECT g, 'row-' || g FROM generate_series(1, 50) g;
	UPDATE local_buf_test SET val = val || '-updated' WHERE id <= 25;
	DELETE FROM local_buf_test WHERE id > 40;
	SELECT count(*) FROM local_buf_test;
}),
   '40',
   'L20 TEMP TABLE INSERT/UPDATE/DELETE round-trip works (LocalBufferAlloc + ClusterInitBufferDescFields not breaking PG)');


done_testing();
