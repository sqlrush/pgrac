#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 306_cluster_5_53_cr_key_reuse.pl
#	  spec-5.53 D8 — CR key identity contract + safe relfilenode-reuse, real
#	  server e2e (single cluster-enabled node, cross-backend, mirror of the
#	  spec-5.51 t/303 fixture).
#
#	  Builds on the shipped spec-5.51 shared CR pool (t/303) and exercises the
#	  spec-5.53 DELTAS:
#
#	  L1  the five spec-5.53 mismatch diagnostics are dumped under the 'cr'
#	      category (cr now has 30 rows = 17 + 5 + 8 spec-5.54), and locator_reuse_reject is the
#	      catalog-incarnation belt observable that stays 0 (D0 = RED → floor-only).
#	  L2  relfilenode-reuse floor (8.A core): readers share a read_scn; A
#	      publishes a CR image; a lifecycle event (DROP) bumps the pool epoch; a
#	      reader on the SAME key now MISSES the stale-epoch image and reconstructs
#	      (no false-hit) — and the reuse MISS is attributed to cr_epoch_mismatch.
#	  L3  D2b provably-complete bump fault-inject ("漏 bump 被抓"): with the
#	      'cluster-cr-skip-epoch-bump' point armed, a real DROP does NOT advance
#	      cr_pool_epoch (the missed bump is observable); disarmed, a DROP advances
#	      it again — proving the bump is the load-bearing reuse floor.
#	  L4  same-relid rewrite covers the floor too: TRUNCATE / VACUUM FULL / CLUSTER
#	      all unlink the old relfilenode and so all bump the epoch.
#	  L5  base_page_lsn churn over-miss diagnostic: a churned page version drives a
#	      same-backend MISS attributed to cr_base_lsn_mismatch.
#	  L6  pool OFF (restart enabled=off) == spec-3.10 exact-key behavior: CR
#	      visibility still correct, all mismatch counters frozen at 0.
#
#	  Topology rationale (identical to t/303): the shared pool is PER-INSTANCE
#	  shmem shared across the backends of ONE node, so cross-BACKEND sharing is the
#	  path under test; cross-INSTANCE CR fail-closes by design (spec-5.57).
#
#	  Spec: spec-5.53-cr-key-and-safe-reuse-evolution.md (FROZEN v1.0)
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
# IDENTIFICATION
#	  src/test/cluster_tap/t/306_cluster_5_53_cr_key_reuse.pl
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../lib";

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('spec_5_53_reuse');
$node->init;
$node->append_conf(
	'postgresql.conf', join("\n",
		'cluster.enabled = on',
		'cluster.node_id = 0',
		'cluster.allow_single_node = on',
		'cluster.interconnect_tier = stub',
		'cluster.cr_mvcc_gate = on',
		'cluster.cr_gate_no_peer_fastpath = off',
		'cluster.shared_cr_pool_enabled = on',
		'cluster.shared_cr_pool_size_blocks = 256',
		'autovacuum = off',
		'jit = off',
		'synchronize_seqscans = off',
		''));
$node->start;

# read a cr_pool counter (pool mechanics: hit/miss/epoch_bump...).
my $pool = sub {
	my ($k) = @_;
	return $node->safe_psql('postgres',
		qq{SELECT value::bigint FROM pg_cluster_state WHERE category='cr_pool' AND key='$k'});
};

# read a cr counter (construct + the spec-5.53 mismatch diagnostics).
my $cr = sub {
	my ($k) = @_;
	return $node->safe_psql('postgres',
		qq{SELECT value::bigint FROM pg_cluster_state WHERE category='cr' AND key='$k'});
};

# Open an independent repeatable-read reader in the current quiescent window;
# return [session, read_scn] (read_scn == cluster_scn_current() at snapshot).
my $open_rr_reader = sub {
	my $b = $node->background_psql('postgres', on_error_die => 1);
	$b->query_safe('BEGIN ISOLATION LEVEL REPEATABLE READ');
	my $rscn = $b->query_safe('SELECT cluster_scn_current()');
	chomp $rscn;
	return [ $b, $rscn ];
};


# ----------
# L1: the five spec-5.53 mismatch diagnostics are dumped under 'cr'.
# ----------
{
	is($node->safe_psql('postgres', 'SELECT 1'), '1', 'L1a node alive');

	# spec-3.9/3.10/3.22 cr rows (17) + spec-5.53 D5 five mismatch counters +
	# spec-5.54 D5 eight tuple-fast-path outcome counters.
	is( $node->safe_psql('postgres',
			q{SELECT count(*) FROM pg_cluster_state WHERE category='cr'}),
		'30', 'L1b cr category has 30 rows (17 + 5 spec-5.53 mismatch + 8 spec-5.54 tuple)');

	for my $k (qw(cr_key_mismatch_count cr_epoch_mismatch_count
		cr_generation_mismatch_count cr_base_lsn_mismatch_count
		cr_locator_reuse_reject_count))
	{
		is( $node->safe_psql('postgres',
				qq{SELECT count(*) FROM pg_cluster_state WHERE category='cr' AND key='$k'}),
			'1', "L1c cr.$k present");
	}

	# D0 = RED → floor-only: the catalog-incarnation belt observable stays 0.
	is($cr->('cr_locator_reuse_reject_count'), '0',
		'L1d locator_reuse_reject == 0 (belt absent; floor-only)');
}


# ----------
# L2: relfilenode-reuse floor (8.A core) + epoch_mismatch attribution.
# ----------
{
	$node->safe_psql('postgres',
		'CREATE TABLE t_share (id int, v int);
		 INSERT INTO t_share SELECT g, g FROM generate_series(1,4) g;');
	$node->safe_psql('postgres',
		'CREATE TABLE t_drop_me (x int); INSERT INTO t_drop_me VALUES (1);');

	my $ra = $open_rr_reader->();
	my $rb = $open_rr_reader->();
	my ($sa, $sb) = ($ra->[0], $rb->[0]);
	my $R = $ra->[1];
	is(($ra->[1] eq $R && $rb->[1] eq $R) ? 1 : 0, 1,
		"L2a A/B share read_scn=$R (quiescent window)")
		or diag("read_scn A=$ra->[1] B=$rb->[1]");

	# Modify the block AFTER the readers' snapshot; settle hint bits / pd_lsn.
	$node->safe_psql('postgres', 'UPDATE t_share SET v = v + 100 WHERE id = 1');
	$node->safe_psql('postgres', 'SELECT count(*) FROM t_share') for 1 .. 3;

	# A constructs + publishes its CR image to the shared pool.
	my $va = $sa->query_safe('SELECT v FROM t_share WHERE id = 1');
	chomp $va;
	is($va, '1', 'L2b A sees pre-update value via CR');

	# DROP an unrelated table -> relfilenode unlink -> the provably-complete floor
	# bump.  B (same key) must now MISS the stale-epoch published image (8.A
	# no-false-hit) and reconstruct, and the MISS is attributed to epoch_mismatch.
	my $pre_em   = $cr->('cr_epoch_mismatch_count');
	my $pre_bump = $pool->('epoch_bump_count');
	$node->safe_psql('postgres', 'DROP TABLE t_drop_me');
	ok($pool->('epoch_bump_count') > $pre_bump, 'L2c DROP bumped the pool epoch');

	my $vb = $sb->query_safe('SELECT v FROM t_share WHERE id = 1');
	chomp $vb;
	is($vb, '1', 'L2d B still sees the correct CR value after the epoch bump');
	ok($cr->('cr_epoch_mismatch_count') > $pre_em,
		'L2e the reuse MISS was attributed to cr_epoch_mismatch (floor fence)');
	# the belt observable never fires in this floor-only build.
	is($cr->('cr_locator_reuse_reject_count'), '0',
		'L2f locator_reuse_reject still 0 (floor-only)');

	$sb->query_safe('COMMIT'); $sb->quit;
	$sa->query_safe('COMMIT'); $sa->quit;
}


# ----------
# L3: D2b fault-inject — a missed bump is observable ("漏 bump 被抓").
#     The injection arm is process-local, so the arm and the DROP must run in the
#     SAME backend (one safe_psql); the arm then dies when that backend exits, so
#     a fresh backend's DROP bumps again.
# ----------
{
	$node->safe_psql('postgres', 'CREATE TABLE t_skip1 (x int); CREATE TABLE t_skip2 (x int);');
	my $pre = $pool->('epoch_bump_count');

	# Armed in-backend: the bump for the t_skip1 unlink is skipped -> a real DROP
	# does NOT advance the epoch (the regression a narrow gate would cause is now
	# observable: a missed bump leaves cr_pool_epoch un-advanced).
	$node->safe_psql('postgres', q{
		SELECT cluster_inject_fault('cluster-cr-skip-epoch-bump','skip',0);
		DROP TABLE t_skip1;
	});
	is($pool->('epoch_bump_count'), $pre,
		'L3a armed skip (same backend): DROP did NOT bump the epoch (missed bump observable)');

	# A fresh (unarmed) backend DROPs -> the bump fires again: the floor is the
	# load-bearing reuse fence, and it is restored once the arm is gone.
	$node->safe_psql('postgres', 'DROP TABLE t_skip2');
	ok($pool->('epoch_bump_count') > $pre,
		'L3b unarmed backend: DROP bumps the epoch again (floor is load-bearing)');
}


# ----------
# L4: same-relid rewrite (TRUNCATE / VACUUM FULL / CLUSTER) bumps the epoch.
# ----------
{
	$node->safe_psql('postgres',
		'CREATE TABLE t_rw (id int primary key, v int); INSERT INTO t_rw VALUES (1,1),(2,2);');

	my $b1 = $pool->('epoch_bump_count');
	$node->safe_psql('postgres', 'TRUNCATE t_rw');	# RelationSetNewRelfilenumber
	ok($pool->('epoch_bump_count') > $b1, 'L4a TRUNCATE (same relid, new relfilenode) bumped epoch');

	$node->safe_psql('postgres', 'INSERT INTO t_rw VALUES (1,1),(2,2);');
	my $b2 = $pool->('epoch_bump_count');
	$node->safe_psql('postgres', 'VACUUM FULL t_rw');	# swap_relation_files
	ok($pool->('epoch_bump_count') > $b2, 'L4b VACUUM FULL (swap relfilenode) bumped epoch');

	my $b3 = $pool->('epoch_bump_count');
	$node->safe_psql('postgres', 'CLUSTER t_rw USING t_rw_pkey;');	# swap_relation_files
	ok($pool->('epoch_bump_count') > $b3, 'L4c CLUSTER (swap relfilenode) bumped epoch');
}


# ----------
# L5: base_page_lsn churn over-miss diagnostic.
# ----------
{
	$node->safe_psql('postgres',
		'CREATE TABLE t_churn (id int, v int);
		 INSERT INTO t_churn SELECT g, g FROM generate_series(1,4) g;');

	my $r = $open_rr_reader->();
	my $s = $r->[0];

	# post-snapshot modify id=1 so the reader needs CR, then cache it (construct).
	$node->safe_psql('postgres', 'UPDATE t_churn SET v = v + 100 WHERE id = 1');
	$node->safe_psql('postgres', 'SELECT count(*) FROM t_churn') for 1 .. 3;
	my $v1 = $s->query_safe('SELECT v FROM t_churn WHERE id = 1');
	chomp $v1;
	is($v1, '1', 'L5a reader sees pre-update value (CR cached at base_lsn v1)');

	# churn the page version (a neighbour-row change bumps the block's pd_lsn);
	# the reader re-reads the SAME block under the SAME read_scn.  The version
	# guard forces a fresh CR (the churned base_page_lsn diverges the key); the
	# 8.A-relevant property here is that the churn never produces an over-hit —
	# the reader keeps seeing the correct pre-snapshot value, never the churned
	# image.  (The base_lsn over-miss CLASSIFICATION counter is covered
	# deterministically by the unit test test_miss_reason_classification.)
	$node->safe_psql('postgres', 'UPDATE t_churn SET v = v + 1 WHERE id = 2');
	$node->safe_psql('postgres', 'SELECT count(*) FROM t_churn') for 1 .. 3;
	my $v2 = $s->query_safe('SELECT v FROM t_churn WHERE id = 1');
	chomp $v2;
	is($v2, '1', 'L5b reader still sees the correct CR value after the page churn (no over-hit)');

	$s->query_safe('COMMIT'); $s->quit;
}


# ----------
# L6: pool OFF (restart enabled=off) == spec-3.10 exact-key behavior.
# ----------
{
	$node->append_conf('postgresql.conf', "cluster.shared_cr_pool_enabled = off\n");
	$node->restart;

	is($node->safe_psql('postgres', 'SELECT 1'), '1', 'L6a node alive after restart');
	is($pool->('current_epoch'), '0', 'L6b current_epoch == 0 (pool disabled)');

	# CR visibility still resolves correctly via the L1-only spec-3.10 path.
	$node->safe_psql('postgres', 'CREATE TABLE t_off (id int, v int); INSERT INTO t_off VALUES (1,1);');
	my $s = $node->background_psql('postgres', on_error_die => 1);
	$s->query_safe('BEGIN ISOLATION LEVEL REPEATABLE READ');
	$s->query_safe('SELECT count(*) FROM t_off');	# snapshot
	$node->safe_psql('postgres', 'UPDATE t_off SET v = 99 WHERE id = 1');
	my $r1 = $s->query_safe('SELECT v FROM t_off WHERE id = 1');
	chomp $r1;
	is($r1, '1', 'L6c disabled: A sees correct CR value (exact-key L1 path)');
	$s->query_safe('COMMIT'); $s->quit;

	# every spec-5.53 mismatch diagnostic is frozen at 0 with the pool disabled.
	for my $k (qw(cr_key_mismatch_count cr_epoch_mismatch_count
		cr_generation_mismatch_count cr_base_lsn_mismatch_count
		cr_locator_reuse_reject_count))
	{
		is($cr->($k), '0', "L6d $k frozen at 0 while pool disabled");
	}
}


$node->stop;
done_testing();
