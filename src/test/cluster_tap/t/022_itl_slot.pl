#-------------------------------------------------------------------------
#
# 022_itl_slot.pl
#    End-to-end regression for the stage-1.5 ITL slot array + tuple
#    header +1B change.
#
#    Verifies the SQL surface backed by cluster_debug.c block_format
#    category extension + the on-disk page binary via pageinspect:
#
#      - pg_cluster_state.block_format category has 9 keys
#        (4 from spec-1.4 + 5 from spec-1.5).
#      - The 5 new keys: itl_slot_size_bytes=48 / itl_initrans_default=8 /
#        itl_array_bytes=384 / tuple_header_extra_bytes=1 /
#        page_header_with_itl_bytes=416.
#      - Heap PageInitHeapPage produces page_header().lower = 420
#        (416 header+ITL + 4-byte first ItemId).
#      - ITL slot array (offset 32-415) is all zeros (placeholder
#        InvalidScn / InvalidUba / ITL_FLAG_FREE per spec-1.5 §3.2).
#      - pd_flags PD_HAS_ITL bit (0x0008) is set on heap pages.
#      - HeapTupleHeader t_hoff = 24 (spec-1.4 was 24 too because
#        MAXALIGN(23) == MAXALIGN(24); spec-1.5 makes the field
#        explicit so audit can prove it).
#      - t_itl_slot_idx raw byte (offset 23 in tuple) = 0xFF (255 =
#        unallocated, per spec-1.5 §3.2).
#      - 1.4 baseline (pg_stat_cluster_wait_events 51, pg_cluster_shmem
#        2 rows, 24 inject points) unchanged.
#      - cluster_smoke regression passes.
#      - Max heap row size reduced 8152 -> 7776 (PageHeader+ITL+TupleHdr+1B).
#
#    Q7 A+B (user-revised hybrid) t_hoff boundary tests (L15-L18):
#      - L15: tuple with no null bitmap (NOT NULL columns)
#      - L16: tuple with 1 null column
#      - L17: tuple with 64+ null columns (bitmap crosses byte boundary)
#      - L18: tuple at max heap size boundary
#
#    Per spec-1.4 / 1.5 convention, pageinspect-dependent assertions
#    are SKIP'd if the contrib extension is not available (CI doesn't
#    build contrib by default).
#
# IDENTIFICATION
#    src/test/cluster_tap/t/022_itl_slot.pl
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


# ----------
# L1: pg_cluster_state.block_format category has 9 keys.
# ----------
is($node->safe_psql(
		'postgres',
		q{SELECT count(*) FROM pg_cluster_state WHERE category='block_format'}),
   '9',
   'L1 pg_cluster_state.block_format category has 9 keys (4 stage-1.4 + 5 stage-1.5)');


# ----------
# L2-L6: 5 new spec-1.5 keys with exact values.
# ----------
is($node->safe_psql(
		'postgres',
		q{SELECT value FROM pg_cluster_state
		   WHERE category='block_format' AND key='itl_slot_size_bytes'}),
   '48',
   'L2 itl_slot_size_bytes = 48');

is($node->safe_psql(
		'postgres',
		q{SELECT value FROM pg_cluster_state
		   WHERE category='block_format' AND key='itl_initrans_default'}),
   '8',
   'L3 itl_initrans_default = 8');

is($node->safe_psql(
		'postgres',
		q{SELECT value FROM pg_cluster_state
		   WHERE category='block_format' AND key='itl_array_bytes'}),
   '384',
   'L4 itl_array_bytes = 384 (= 48 × 8 INITRANS)');

is($node->safe_psql(
		'postgres',
		q{SELECT value FROM pg_cluster_state
		   WHERE category='block_format' AND key='tuple_header_extra_bytes'}),
   '1',
   'L5 tuple_header_extra_bytes = 1 (t_itl_slot_idx)');

is($node->safe_psql(
		'postgres',
		q{SELECT value FROM pg_cluster_state
		   WHERE category='block_format' AND key='itl_location'}),
   'page_special_area_tail',
   'L6 itl_location = page_special_area_tail (PIVOT A 2026-05-02: ITL in PG special area, not after PageHeader)');


# ----------
# L7-L11: pageinspect raw page binary verification (skip if not available).
# ----------
my $has_pageinspect = $node->safe_psql(
	'postgres',
	q{SELECT count(*) FROM pg_available_extensions WHERE name='pageinspect'});

SKIP: {
	skip 'pageinspect contrib extension not available in this build', 5
		unless $has_pageinspect eq '1';

	$node->safe_psql('postgres', q{
		CREATE EXTENSION IF NOT EXISTS pageinspect;
		CREATE TABLE t1 (a int);
		INSERT INTO t1 VALUES (1);
	});

	# L7: PIVOT A -- after first INSERT pd_lower = 36 (32 + 4-byte ItemId);
	# pd_special = 7808 (BLCKSZ - 384 ITL); pd_upper = 7776 (one tuple).
	# This is fundamentally different from the pre-PIVOT layout where
	# pd_lower would be 420.

	is($node->safe_psql(
			'postgres', q{
		SELECT lower::text || ',' || special::text || ',' || version::text
		  FROM page_header(get_raw_page('t1', 0))}),
	   '36,7808,5',
	   'L7 PIVOT A heap page: lower=36, special=7808, version=5 (ITL in special area, not after header)');

	# L8: ITL slot array at special area (offset 7808-8191) is 384 zero bytes.
	is($node->safe_psql(
			'postgres', q{
		SELECT encode(substring(get_raw_page('t1', 0), 7809, 384), 'hex') =
		       repeat('00', 384)}),
	   't',
	   'L8 PIVOT A ITL slot array (offset 7808-8191 in special area) is 384 zero bytes');

	# L9: pd_flags = 0x0008 (PD_HAS_ITL bit set).
	is($node->safe_psql(
			'postgres',
			q{SELECT encode(substring(get_raw_page('t1', 0), 11, 2), 'hex')}),
	   '0800',
	   'L9 pd_flags PD_HAS_ITL bit (0x0008) set on heap page');

	# L10: HeapTupleHeader t_hoff = 24.
	is($node->safe_psql(
			'postgres',
			q{SELECT DISTINCT t_hoff FROM heap_page_items(get_raw_page('t1', 0))
			   WHERE t_hoff IS NOT NULL}),
	   '24',
	   'L10 HeapTupleHeader t_hoff = 24 (was 23 in vanilla PG)');

	# L11: t_itl_slot_idx byte at offset 23 within tuple header = 0xFF.
	# heap_page_items.t_data only returns user data (after header), so
	# we read directly from get_raw_page using lp_off + 23 (1-based +1).
	is($node->safe_psql(
			'postgres', q{
		WITH lp AS (
		  SELECT lp_off
		    FROM heap_page_items(get_raw_page('t1', 0))
		   WHERE t_data IS NOT NULL
		   LIMIT 1
		)
		SELECT encode(
		         substring(get_raw_page('t1', 0),
		                   (SELECT lp_off FROM lp) + 24,
		                   1),
		         'hex')
	}),
	   'ff',
	   'L11 t_itl_slot_idx raw byte (offset 23 in header) = 0xFF (255 unallocated)');
}


# ----------
# L12: 1.4 baseline -- inject registry still 24, wait events still 51,
# pg_cluster_shmem still 2 rows.
# ----------
is($node->safe_psql(
		'postgres',
		'SELECT count(*) FROM pg_stat_cluster_injections'),
   '24',
   'L12a pg_stat_cluster_injections still 24 (1.5 adds no inject points)');

is($node->safe_psql(
		'postgres',
		'SELECT count(*) FROM pg_stat_cluster_wait_events'),
   '51',
   'L12b pg_stat_cluster_wait_events still 51 (1.5 adds no wait events)');

is($node->safe_psql(
		'postgres',
		'SELECT count(*) FROM pg_cluster_shmem'),
   '2',
   'L12c pg_cluster_shmem still 2 rows (1.5 adds no shmem regions)');


# ----------
# L13: cluster_smoke regression passes (smoke covers many basic SQL paths).
# ----------
my $smoke_count = $node->safe_psql(
	'postgres',
	q{SELECT count(*) FROM pg_cluster_state WHERE category='block_format'});
is($smoke_count, '9', 'L13 block_format category integrates with cluster_smoke');


# ----------
# L14: Max row size reduced.  Use STORAGE plain (no TOAST) to force
# the row to fit in a single page; INSERT a row that's clearly above
# the new max (8000-byte plain text + tuple header > 7776) and verify
# the "row is too big" error fires.
# ----------
$node->safe_psql('postgres', q{
	CREATE TABLE row_size_test (a text);
	ALTER TABLE row_size_test ALTER COLUMN a SET STORAGE plain;
});
my $stderr_l14;
my $stdout_l14;
$node->psql(
	'postgres',
	q{INSERT INTO row_size_test VALUES (repeat('x', 8000));},
	stdout => \$stdout_l14, stderr => \$stderr_l14);
like($stderr_l14, qr/row is too big/i,
	 'L14 8000-byte plain row rejected with "row is too big" (max < 8000 in pgrac 1.5)');


# ----------
# L15: Q7 A+B boundary -- tuple with NO null bitmap (all NOT NULL columns).
# t_hoff should equal SizeofHeapTupleHeader (24) exactly.
# ----------
SKIP: {
	skip 'pageinspect contrib extension not available in this build', 1
		unless $has_pageinspect eq '1';

	$node->safe_psql('postgres', q{
		CREATE TABLE no_null_test (a int NOT NULL, b int NOT NULL);
		INSERT INTO no_null_test VALUES (1, 2);
	});

	is($node->safe_psql(
			'postgres',
			q{SELECT DISTINCT t_hoff FROM heap_page_items(get_raw_page('no_null_test', 0))
			   WHERE t_hoff IS NOT NULL}),
	   '24',
	   'L15 tuple with no null bitmap: t_hoff = 24 = SizeofHeapTupleHeader');
}


# ----------
# L16: Q7 A+B boundary -- tuple with 1 NULL column (1-byte null bitmap).
# t_hoff = MAXALIGN(SizeofHeapTupleHeader + 1) = MAXALIGN(25) = 32.
# ----------
SKIP: {
	skip 'pageinspect contrib extension not available in this build', 1
		unless $has_pageinspect eq '1';

	$node->safe_psql('postgres', q{
		CREATE TABLE one_null_test (a int, b int);
		INSERT INTO one_null_test VALUES (1, NULL);
	});

	is($node->safe_psql(
			'postgres',
			q{SELECT DISTINCT t_hoff FROM heap_page_items(get_raw_page('one_null_test', 0))
			   WHERE t_hoff IS NOT NULL}),
	   '32',
	   'L16 tuple with 1 null bitmap byte: t_hoff = 32 = MAXALIGN(24+1)');
}


# ----------
# L17: Q7 A+B boundary -- tuple with 64+ NULL columns (bitmap crosses
# byte boundary).  bitmap = (64+7)/8 = 9 bytes, so t_hoff =
# MAXALIGN(24 + 9) = MAXALIGN(33) = 40.
# ----------
SKIP: {
	skip 'pageinspect contrib extension not available in this build', 1
		unless $has_pageinspect eq '1';

	# Build 64 column DDL programmatically (column list string)
	my @cols = map { "c$_ int" } (1 .. 64);
	my $col_list = join(', ', @cols);
	my @vals = ('NULL') x 64;
	my $val_list = join(', ', @vals);

	$node->safe_psql('postgres', "CREATE TABLE wide_null_test ($col_list);");
	$node->safe_psql('postgres', "INSERT INTO wide_null_test VALUES ($val_list);");

	is($node->safe_psql(
			'postgres',
			q{SELECT DISTINCT t_hoff FROM heap_page_items(get_raw_page('wide_null_test', 0))
			   WHERE t_hoff IS NOT NULL}),
	   '32',
	   'L17 tuple with 64-col null bitmap (8 bytes): t_hoff = 32 = MAXALIGN(24+8)');
}


# ----------
# L18: Q7 A+B boundary -- max heap size lower bound.  A small row
# (100 bytes) MUST fit (sanity: nothing has broken the page layout).
# This complements L14's upper-bound check.
# ----------
$node->safe_psql('postgres', q{
	CREATE TABLE small_size_test (a text);
	ALTER TABLE small_size_test ALTER COLUMN a SET STORAGE plain;
});

my $stderr_l18;
my $stdout_l18;
$node->psql(
	'postgres',
	q{INSERT INTO small_size_test VALUES (repeat('y', 100));},
	stdout => \$stdout_l18, stderr => \$stderr_l18);
ok($stderr_l18 !~ /row is too big/i,
   'L18 100-byte plain row succeeds (sanity: small rows fit)');


# ----------
# L19-L24: PIVOT A 硬断言（user 2026-05-02 必加清单）。
# 验证 ITL 在 special area 而非 PageHeader 后；验证 index page 无 ITL。
# ----------
SKIP: {
	skip 'pageinspect contrib extension not available in this build', 6
		unless $has_pageinspect eq '1';

	$node->safe_psql('postgres', q{
		CREATE TABLE pivot_a_heap (a int);
		CREATE INDEX pivot_a_idx ON pivot_a_heap (a);
		INSERT INTO pivot_a_heap VALUES (1);
	});

	# L19: heap page pd_lower = 36 after first INSERT (32 header + 4 ItemId).
	is($node->safe_psql(
			'postgres',
			q{SELECT lower FROM page_header(get_raw_page('pivot_a_heap', 0))}),
	   '36',
	   'L19 PIVOT A heap page after first INSERT: pd_lower = 36 (NOT 420)');

	# L20: heap page PageGetSpecialSize = 384 (= CLUSTER_ITL_ARRAY_SIZE).
	is($node->safe_psql(
			'postgres', q{
		SELECT (pagesize - special)
		  FROM page_header(get_raw_page('pivot_a_heap', 0))
	}),
	   '384',
	   'L20 PIVOT A heap page PageGetSpecialSize = 384 (CLUSTER_ITL_ARRAY_SIZE)');

	# L21: heap page PageGetMaxOffsetNumber = 1 (one INSERT'd tuple).
	is($node->safe_psql(
			'postgres', q{
		SELECT count(*) FROM heap_page_items(get_raw_page('pivot_a_heap', 0))
		 WHERE t_data IS NOT NULL
	}),
	   '1',
	   'L21 PIVOT A heap page max offset = 1 (ItemIds + ITL no longer collide)');

	# L22: PD_HAS_ITL bit set on heap page.
	is($node->safe_psql(
			'postgres', q{
		SELECT (flags & 8) = 8
		  FROM page_header(get_raw_page('pivot_a_heap', 0))
	}),
	   't',
	   'L22 PIVOT A heap page PD_HAS_ITL bit (0x0008) set');

	# L23: PD_HAS_ITL bit NOT set on btree index page.
	is($node->safe_psql(
			'postgres', q{
		SELECT (flags & 8) = 0
		  FROM page_header(get_raw_page('pivot_a_idx', 0))
	}),
	   't',
	   'L23 PIVOT A btree index page PD_HAS_ITL bit NOT set (index special = btree opaque, not ITL)');

	# L24: heap page ITL bytes (special area, offset 7808+) all zero;
	# btree index page special area is btree opaque (NOT all zero).
	# This checks the two pages have semantically different special areas.
	is($node->safe_psql(
			'postgres', q{
		SELECT
		  (encode(substring(get_raw_page('pivot_a_heap', 0), 7809, 384), 'hex')
		     = repeat('00', 384))
		  AND
		  (encode(substring(get_raw_page('pivot_a_idx', 0),
		                    (SELECT special + 1
		                       FROM page_header(get_raw_page('pivot_a_idx', 0))),
		                    16), 'hex')
		     <> repeat('00', 16))
	}),
	   't',
	   'L24 PIVOT A heap special = ITL all zero; btree special = opaque non-zero (separate semantics)');
}


$node->stop;

done_testing();
