#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 104_grd_entry_shmem_alloc_smoke.pl
#	  spec-2.15 D13: single-node TAP smoke for cluster_grd entry table
#	  infrastructure layer.  4 mandatory tests (v0.2 P1.2 — internal C
#	  API behavior 留给 cluster_unit T-grd-2,TAP 只测 user-visible
#	  surface):
#	    L1  single-node primary alive
#	    L2  cluster.grd_max_entries=0 (default) → pg_cluster_grd_entries
#	        empty + grd_allocated_bytes=0 (NOT_READY sentinel 对外显化)
#	    L3  cluster.grd_max_entries=16 restart → grd_allocated_bytes>0
#	        (v0.4 P1.1 NOTE: HASH_PARTITION=4096 forces nbuckets>=4096
#	        so hash_estimate_size(4096,...) ≈ 3-5MB,not naive 12KB) +
#	        pg_cluster_grd_entries still 0 row (0 caller)
#	    L4  SET cluster.grd_max_entries session-level fail (PGC_POSTMASTER)
#	        + dump_grd 37 row baseline + GRD atomic counters all 0
#
#	  Spec authority: pgrac:specs/spec-2.15-grd-entry-table-holders-
#	  waiters.md (frozen v0.4 Q1-Q15 + 4 轮 codereview corrections).
#
#
# Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
# Portions Copyright (c) 2026, pgrac contributors
#
# Author: SqlRush <sqlrush@gmail.com>
#
# IDENTIFICATION
#	  src/test/cluster_tap/t/104_grd_entry_shmem_alloc_smoke.pl
#
# NOTES
#	  pgrac-original file.
#	  Spec: spec-2.15-grd-entry-table-holders-waiters.md (frozen v0.4)
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


# ----------
# L1: single-node primary init + start (no ClusterPair / no IC).
# ----------
my $node = PgracClusterNode->new('main');
$node->init;
$node->append_conf('postgresql.conf', "cluster.node_id = 0\n");
# This test explicitly covers the zero-capacity / NOT_READY GRD skeleton path.
# cluster.grd_max_entries now defaults to 1024 (spec-5.7 §3.1d engage-gate
# work), so the L2 zero-capacity coverage must request 0 explicitly rather than
# rely on the old implicit default.  (L3 below appends =16 and restarts; the
# last value in postgresql.conf wins, so the explicit 0 here does not affect it.)
$node->append_conf('postgresql.conf', "cluster.grd_max_entries = 0\n");
$node->start;

is($node->safe_psql('postgres', 'SELECT 1'), '1',
   'L1 single-node primary alive');


# ----------
# L2: cluster.grd_max_entries=0 (explicit) — sentinel NOT_READY 对外显化
#     pg_cluster_grd_entries empty + grd_allocated_bytes=0.
# ----------
is($node->safe_psql('postgres', 'SHOW cluster.grd_max_entries'),
   '0',
   'L2a cluster.grd_max_entries explicit 0 (zero-capacity coverage)');

is($node->safe_psql('postgres',
		'SELECT count(*)::int FROM pg_cluster_grd_entries'),
   '0',
   'L2b pg_cluster_grd_entries empty when GUC=0 (NOT_READY sentinel)');

is($node->safe_psql('postgres',
		q{SELECT value FROM pg_cluster_state
		   WHERE category='grd' AND key='grd_allocated_bytes'}),
   '0',
   'L2c grd_allocated_bytes=0 when GUC=0 (entry HTAB not allocated)');


# ----------
# L3: cluster.grd_max_entries=16 restart — HTAB allocated.
#     v0.4 P1.1: HASH_PARTITION=4096 forces nbuckets>=4096,so
#     hash_estimate_size(4096, sizeof(ClusterGrdEntry)) ≈ 3-5MB,
#     not the naive 16 × sizeof(entry) = 12KB.
# ----------
$node->stop;
$node->append_conf('postgresql.conf', "cluster.grd_max_entries = 16\n");
$node->start;

is($node->safe_psql('postgres', 'SHOW cluster.grd_max_entries'),
   '16',
   'L3a postmaster started cleanly with cluster.grd_max_entries=16');

ok($node->safe_psql('postgres',
		q{SELECT value::bigint > 0 FROM pg_cluster_state
		   WHERE category='grd' AND key='grd_allocated_bytes'}) eq 't',
   'L3b grd_allocated_bytes > 0 (v0.4 P1.1: HASH_PARTITION nbuckets >= 4096)');

is($node->safe_psql('postgres',
		'SELECT count(*)::int FROM pg_cluster_grd_entries'),
   '0',
   'L3c pg_cluster_grd_entries still 0 row (本 spec 0 caller / 0 mutation)');


# ----------
# L4: SET fail (PGC_POSTMASTER) + dump_grd 37 row baseline + GRD atomic
#     counters all 0 (本 spec 0 caller / 0 mutation).
# ----------
{
	my $stderr_l4;
	$node->psql('postgres',
		'SET cluster.grd_max_entries = 32;',
		stderr => \$stderr_l4);
	like($stderr_l4, qr/cannot be (changed|set)/i,
		 'L4a cluster.grd_max_entries is PGC_POSTMASTER (SET fails at session level)');
}

# dump_grd category='grd' total row count: spec-2.14 ships 8 emit_row,
# spec-2.15 v0.3 adds 6, spec-2.16 exposes 14 queue/pending counters,
# spec-2.17 adds 9 BAST/deadlock checkpoint counters, spec-2.24 adds
# 1 cleanup_skip_stale_cancel_count counter, spec-2.25 adds
# 1 relation_object_cluster_path_count counter, spec-2.26 adds
# 1 transaction_cluster_path_count counter, spec-5.1b D9 adds 3
# convert verdict counters (granted_inplace / enqueued / illegal), and
# spec-5.10 D7 adds 4 starvation-fairness counters (boost / barrier_enqueued
# / barrier_publish_fail / max_skip_observed).
is($node->safe_psql('postgres',
		q{SELECT count(*)::int FROM pg_cluster_state WHERE category='grd'}),
   '47',
   'L4b dump_grd category="grd" emits 47 rows (spec-2.14 8 + spec-2.15 6 + spec-2.16 14 + spec-2.17 9 + spec-2.24 1 + spec-2.25 1 + spec-2.26 1 + spec-5.1b 3 + spec-5.10 4)');

# All 3 NEW atomic counter baseline 0 (本 spec 0 caller invokes
# cluster_grd_entry_lookup_or_create).
is($node->safe_psql('postgres',
		q{SELECT (SELECT value FROM pg_cluster_state
		           WHERE category='grd' AND key='grd_entry_create_count')
		    || '|' ||
		         (SELECT value FROM pg_cluster_state
		           WHERE category='grd' AND key='grd_entry_lookup_hit_count')
		    || '|' ||
		         (SELECT value FROM pg_cluster_state
		           WHERE category='grd' AND key='grd_entry_full_count')}),
   '0|0|0',
   'L4c grd_entry_{create,lookup_hit,full}_count all 0 baseline (0 caller)');

is($node->safe_psql('postgres',
		q{SELECT count(*)::int FROM pg_cluster_state
		   WHERE category='grd'
		     AND key IN ('grd_bast_sent_count',
		                 'grd_bast_received_count',
		                 'grd_bast_ack_count',
		                 'grd_bast_retry_count',
		                 'grd_bast_reject_count',
		                 'grd_bast_stale_drop_count',
		                 'grd_deadlock_probe_drop_count',
		                 'grd_deadlock_probe_collision_drop_count',
		                 'grd_deadlock_chunk_oo_buffer_overflow_count')
		     AND value = '0'}),
   '9',
   'L4d spec-2.17 BAST/deadlock checkpoint counters all 0 baseline');


# ----------
# Cleanup.
# ----------
$node->stop;

done_testing();
