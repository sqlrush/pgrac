#-------------------------------------------------------------------------
#
# 021_block_format.pl
#    End-to-end regression for the stage-1.4 block format change:
#    PageHeader +8B pd_block_scn + PG_PAGE_LAYOUT_VERSION 4 -> 5 +
#    SCN typedef stub + InvalidScn = 0.
#
#    Verifies the SQL surface backed by cluster_debug.c block_format
#    category + the on-disk page binary via pageinspect:
#
#      - pg_cluster_state.block_format category exists with 4 keys.
#      - page_layout_version = 5; page_header_size = 32;
#        scn_size_bytes = 8; invalid_scn_value = "0".
#      - Heap PageInit produces page_header() lower = 36 (32 header
#        + 4 byte first ItemId) on first INSERT; pagesize = 8192;
#        version = 5.
#      - Btree index PageInit produces page version = 5.
#      - pd_block_scn raw bytes (offset 24-31) are 8 zero bytes
#        (InvalidScn placeholder; spec-1.16 takes over real values).
#      - pd_pagesize_version raw bytes (offset 18-19) encode
#        BLCKSZ | 5 = 0x2005 little-endian.
#      - Stage 1.3 baseline (pg_cluster_shmem 2 rows; 24 inject
#        points; pg_stat_cluster_wait_events 51) unchanged.
#
# IDENTIFICATION
#    src/test/cluster_tap/t/021_block_format.pl
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
# +1 for the unconditional "pgrac cluster sequence" region (spec-5.4 SQ shmem foundation).
  my $expected_region_count = $has_visibility_inject ? '52' : '51';


# ----------
# L1: pg_cluster_state.block_format category exists.
# Stage 1.4 introduced 4 keys; stage 1.5 extends to 9 keys (+5 ITL).
# ----------
is($node->safe_psql(
		'postgres',
		q{SELECT count(*) FROM pg_cluster_state WHERE category='block_format'}),
   '9',
   'L1 pg_cluster_state.block_format category has 9 keys (4 stage-1.4 + 5 stage-1.5 ITL)');


# ----------
# L2: page_layout_version key = 5.
# ----------
is($node->safe_psql(
		'postgres',
		q{SELECT value FROM pg_cluster_state
		   WHERE category='block_format' AND key='page_layout_version'}),
   '5',
   'L2 page_layout_version = 5 (PG vanilla 4 + pgrac 1.4 bump)');


# ----------
# L3: page_header_size key = 32.
# ----------
is($node->safe_psql(
		'postgres',
		q{SELECT value FROM pg_cluster_state
		   WHERE category='block_format' AND key='page_header_size'}),
   '32',
   'L3 page_header_size = 32 (PG vanilla 24 + 8B pd_block_scn)');


# ----------
# L4: scn_size_bytes key = 8.
# ----------
is($node->safe_psql(
		'postgres',
		q{SELECT value FROM pg_cluster_state
		   WHERE category='block_format' AND key='scn_size_bytes'}),
   '8',
   'L4 scn_size_bytes = 8 (uint64 typedef alias)');


# ----------
# L5: invalid_scn_value key = "0".
# ----------
is($node->safe_psql(
		'postgres',
		q{SELECT value FROM pg_cluster_state
		   WHERE category='block_format' AND key='invalid_scn_value'}),
   '0',
   'L5 invalid_scn_value = "0" (locked by spec-1.4 §8 Q2 = A)');


# ----------
# L6-L9: heap PageInit + raw page binary inspection.  These rely on the
# pageinspect contrib extension; skip them if it is not available
# (e.g. CI environment that does not build contrib/).
# ----------
my $has_pageinspect = $node->safe_psql(
	'postgres',
	q{SELECT count(*) FROM pg_available_extensions WHERE name='pageinspect'});

SKIP: {
	skip 'pageinspect contrib extension not available in this build', 4
		unless $has_pageinspect eq '1';

	$node->safe_psql('postgres', q{
		CREATE EXTENSION IF NOT EXISTS pageinspect;
		CREATE TABLE t1 (a int);
		INSERT INTO t1 VALUES (1);
	});

	# L6: heap PageInit (1.4) -> heap PageInitHeapPage (1.5 PIVOT A).
	# Stage 1.5 reserves 384B ITL array in PG special area at page tail,
	# so pd_lower = 32 + 4 (first ItemId) = 36 (not 420 like pre-PIVOT).
	# pagesize = 8192; layout version = 5; pd_special = 7808.
	is($node->safe_psql(
			'postgres', q{
		SELECT lower::text || ',' || pagesize::text || ',' || version::text || ',' || special::text
		  FROM page_header(get_raw_page('t1', 0))}),
	   '36,8192,5,7808',
	   'L6 heap page first INSERT (PIVOT A): lower=36, pagesize=8192, version=5, special=7808 (ITL in special area)');

	# L7: btree index PageInit -- root page version = 5.
	$node->safe_psql('postgres', q{
		CREATE INDEX t1_idx ON t1 (a);
	});

	is($node->safe_psql(
			'postgres',
			q{SELECT version FROM page_header(get_raw_page('t1_idx', 0))}),
	   '5',
	   'L7 btree index page version = 5 (PageInit goes through new path)');

	# L8: pd_block_scn raw bytes at offset 24-31 must be 8 zero bytes.
	is($node->safe_psql(
			'postgres',
			q{SELECT encode(substring(get_raw_page('t1', 0), 25, 8), 'hex')}),
	   '0000000000000000',
	   'L8 pd_block_scn raw bytes are 8 zeros (InvalidScn placeholder)');

	# L9: pd_pagesize_version raw bytes at offset 18-19 = 0x2005 LE.
	is($node->safe_psql(
			'postgres',
			q{SELECT encode(substring(get_raw_page('t1', 0), 19, 2), 'hex')}),
	   '0520',
	   'L9 pd_pagesize_version raw bytes = 0520 LE = 0x2005 (BLCKSZ 8192 | layout 5)');
}


# ----------
# L10: Stage 1.10.1 baseline plus later shmem regions.
# ----------
is($node->safe_psql(
		'postgres',
		q{SELECT count(*) FROM pg_cluster_shmem}),
   $expected_region_count,
   "L10 pg_cluster_shmem returns $expected_region_count rows after spec-3.6");


# ----------
# L11: Inject registry baseline -- still 24 entries (1.4 adds no new
# inject points; structure-only stage with no hot path).
# ----------
is($node->safe_psql(
		'postgres',
		'SELECT count(*) FROM pg_stat_cluster_injections'),
   '128',
   'L11 pg_stat_cluster_injections is 128 (spec-5.2a +1 clean-xfer stale-holder; spec-4.8ab +2 undo boundary guards)');


# ----------
# L12: pg_stat_cluster_wait_events baseline (block format
# is structure-only; no new wait events at stage 1.4).
# ----------
is($node->safe_psql(
		'postgres',
		'SELECT count(*) FROM pg_stat_cluster_wait_events'),
   '102',
   'L12 pg_stat_cluster_wait_events returns 100 rows (spec-4.6 +1 GRD shard remaster)');


$node->stop;

done_testing();
