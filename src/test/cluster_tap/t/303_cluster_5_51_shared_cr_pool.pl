#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 303_cluster_5_51_shared_cr_pool.pl
#	  spec-5.51 D12 — dedicated shared (L2) CR buffer pool, real-server e2e.
#
#	  Topology: a SINGLE cluster-enabled node (allow_single_node) with
#	  cr_mvcc_gate=on + cr_gate_no_peer_fastpath=off so the CR gate really
#	  constructs on one node (mirror of the shipped spec-5.50 t/300 fixture).
#	  The shared pool is PER-INSTANCE shmem shared across the backends of one
#	  node, so cross-BACKEND sharing is the path under test here; cross-INSTANCE
#	  CR fail-closes by design (spec-5.57), hence a single node is the correct
#	  and sufficient topology (a 2-node ClusterPair would exercise the wrong,
#	  fail-closed path).
#
#	  Same-read_scn mechanism (spec-5.50 §2.3):  pg_export_snapshot +
#	  SET TRANSACTION SNAPSHOT FAIL-CLOSE in cluster mode by design (spec-3.3
#	  R5).  So instead this harness opens the readers in a QUIESCENT window:
#	  read_scn == cluster_scn_current() at snapshot time (procarray.c:2152), so
#	  with no commit between the readers they share the SAME read_scn (=> same
#	  CR key) — and the test PROVES it by reading each reader's
#	  cluster_scn_current() and asserting equality.  base_page_lsn is settled
#	  with post-write autocommit scans before measuring, so all readers compute
#	  the SAME CR key (otherwise the first reader to touch a page mutates its
#	  pd_lsn and the keys diverge per backend — spec-5.50 t/300 L2 finding).
#
#	  L1   pool ON (enabled + size>0): GUCs visible, cr_pool category has 18
#	       rows (10 pool + 8 spec-5.52 admission), current_epoch >= 1.
#	  L2   cross-backend L2 HIT: readers A and B share read_scn (PROVEN).  A
#	       re-reads a post-snapshot modified block -> CR construct -> publish to
#	       the shared pool; B reads the same block under the SAME read_scn -> L2
#	       HIT (pool hit_count++), and B does NOT reconstruct (cr_construct_count
#	       frozen across B's read).  This is the cross-backend sharing proof and
#	       the spec-5.51 core deliverable.
#	  L3   epoch invalidation / no false-hit: a reader C (same read_scn) snapshots
#	       in the same window; after A publishes, a lifecycle event (DROP of an
#	       unrelated table) bumps the pool epoch; C now MISSES the pool (epoch
#	       stale on the published entry) and reconstructs — the stale-epoch image
#	       is never served (8.A no-false-hit), and C still sees the correct value.
#	  L4   epoch bump counter: TRUNCATE also advances epoch_bump_count.
#	  L6   disabled (restart with enabled=off): current_epoch == 0, CR
#	       visibility still correct, pool counters frozen == spec-3.10 behavior.
#
#	  Spec: spec-5.51-dedicated-shared-cr-buffer-pool-v1.md (FROZEN v1.0)
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
# IDENTIFICATION
#	  src/test/cluster_tap/t/303_cluster_5_51_shared_cr_pool.pl
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../lib";

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# ----------
# Single cluster-enabled node.  cr_gate_no_peer_fastpath=off forces the gate to
# really construct on a single node (the 3.24 fast path would otherwise short the
# no-peer case).  The shared pool is enabled with a real size so A's published CR
# image is visible to B in the per-instance shmem pool.  autovacuum off keeps the
# instance-wide counters quiescent.
# ----------
my $node = PostgreSQL::Test::Cluster->new('spec_5_51_pool');
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

# read a cr_pool counter from pg_cluster_state by key.
my $pool = sub {
	my ($k) = @_;
	return $node->safe_psql('postgres',
		qq{SELECT value::bigint FROM pg_cluster_state WHERE category='cr_pool' AND key='$k'});
};

# read a cr counter (instance-wide construct/walk counters).
my $cr = sub {
	my ($k) = @_;
	return $node->safe_psql('postgres',
		qq{SELECT value::bigint FROM pg_cluster_state WHERE category='cr' AND key='$k'});
};

# Open an independent repeatable-read reader; return [session, read_scn].
# read_scn == cluster_scn_current() at snapshot time, so the value returned by
# the first in-txn query IS this backend's read_scn (mirror spec-5.50 t/300).
my $open_rr_reader = sub {
	my $b = $node->background_psql('postgres', on_error_die => 1);
	$b->query_safe('BEGIN ISOLATION LEVEL REPEATABLE READ');
	my $rscn = $b->query_safe('SELECT cluster_scn_current()');
	chomp $rscn;
	return [ $b, $rscn ];
};


# ----------
# L1: pool enabled, GUCs + cr_pool category.
# ----------
{
	is($node->safe_psql('postgres', 'SELECT 1'), '1', 'L1a node alive');

	is( $node->safe_psql('postgres',
			q{SELECT setting FROM pg_settings WHERE name='cluster.shared_cr_pool_enabled'}),
		'on', 'L1b shared_cr_pool_enabled = on');

	is( $node->safe_psql('postgres',
			q{SELECT setting FROM pg_settings WHERE name='cluster.shared_cr_pool_size_blocks'}),
		'256', 'L1c shared_cr_pool_size_blocks = 256');

	is( $node->safe_psql('postgres',
			q{SELECT context FROM pg_settings WHERE name='cluster.shared_cr_pool_size_blocks'}),
		'postmaster', 'L1d size_blocks is PGC_POSTMASTER');

	is( $node->safe_psql('postgres',
			q{SELECT count(*) FROM pg_cluster_state WHERE category='cr_pool'}),
		'18', 'L1e cr_pool category has 18 rows (10 pool + 8 spec-5.52 admission)');

	ok($pool->('current_epoch') >= 1, 'L1f current_epoch >= 1 (pool enabled)');
}


# ----------
# L2/L3: cross-backend L2 HIT + epoch invalidation via quiescent same-read_scn.
# ----------
{
	$node->safe_psql('postgres',
		'CREATE TABLE t_share (id int, v int);
		 INSERT INTO t_share SELECT g, g FROM generate_series(1,4) g;');
	$node->safe_psql('postgres', 'CREATE TABLE t_drop_me (x int); INSERT INTO t_drop_me VALUES (1);');

	# A, B, C all snapshot in the same quiescent window -> shared read_scn.
	my $ra = $open_rr_reader->();
	my $rb = $open_rr_reader->();
	my $rc = $open_rr_reader->();
	my ($sa, $sb, $sc) = ($ra->[0], $rb->[0], $rc->[0]);
	my $R = $ra->[1];
	my $shared = ($ra->[1] eq $R && $rb->[1] eq $R && $rc->[1] eq $R) ? 1 : 0;
	is($shared, 1, "L2a A/B/C share read_scn=$R (quiescent window)")
		or diag("read_scn A=$ra->[1] B=$rb->[1] C=$rc->[1]");

	# Modify the block AFTER the readers' snapshot -> they need CR for id=1.
	$node->safe_psql('postgres', 'UPDATE t_share SET v = v + 100 WHERE id = 1');

	# Settle base_page_lsn / hint bits so every reader computes the SAME CR key.
	$node->safe_psql('postgres', 'SELECT count(*) FROM t_share') for 1 .. 3;

	# Backend A: import is implicit (snapshot fixed) -> construct + publish.
	my $pre_reserve = $pool->('reserve_count');
	my $pre_publish = $pool->('publish_count');
	my $va = $sa->query_safe('SELECT v FROM t_share WHERE id = 1');
	chomp $va;
	is($va, '1', 'L2b A sees pre-update value via CR');
	ok($pool->('reserve_count') > $pre_reserve, 'L2c A reserved a pool slot');
	ok($pool->('publish_count') > $pre_publish, 'L2d A published its CR image to the shared pool');

	# Backend B: SAME read_scn -> SAME key -> must HIT the pool and NOT reconstruct.
	my $pre_hit       = $pool->('hit_count');
	my $pre_construct = $cr->('cr_construct_count');
	my $vb = $sb->query_safe('SELECT v FROM t_share WHERE id = 1');
	chomp $vb;
	is($vb, '1', 'L2e B sees the same pre-update value');
	ok($pool->('hit_count') > $pre_hit, 'L2f B HIT the shared pool (cross-backend share)');
	is($cr->('cr_construct_count'), $pre_construct,
		'L2g B did NOT reconstruct (served from the shared L2 pool)');

	# L3: epoch invalidation -- DROP bumps the pool epoch; C (same key) must MISS.
	my $pre_epoch = $pool->('current_epoch');
	my $pre_bump  = $pool->('epoch_bump_count');
	$node->safe_psql('postgres', 'DROP TABLE t_drop_me');	# relfilenode unlink -> bump
	ok($pool->('current_epoch') > $pre_epoch, 'L3a DROP advanced the pool epoch');
	ok($pool->('epoch_bump_count') > $pre_bump, 'L3b epoch_bump_count incremented on DROP');

	my $pre_miss       = $pool->('miss_count');
	my $pre_construct2 = $cr->('cr_construct_count');
	my $vc = $sc->query_safe('SELECT v FROM t_share WHERE id = 1');
	chomp $vc;
	is($vc, '1', 'L3c C still sees the correct CR value after epoch bump');
	ok($pool->('miss_count') > $pre_miss,
		'L3d C MISSED the pool (stale-epoch entry not served)');
	ok($cr->('cr_construct_count') > $pre_construct2,
		'L3e C reconstructed (8.A: no false-hit on a stale-epoch entry)');

	$sc->query_safe('COMMIT'); $sc->quit;
	$sb->query_safe('COMMIT'); $sb->quit;
	$sa->query_safe('COMMIT'); $sa->quit;
}


# ----------
# L4: TRUNCATE also bumps the epoch.
# ----------
{
	$node->safe_psql('postgres',
		'CREATE TABLE t_trunc (id int, v int); INSERT INTO t_trunc VALUES (1,1);');
	my $pre_bump = $pool->('epoch_bump_count');
	$node->safe_psql('postgres', 'TRUNCATE t_trunc');	# new relfilenode + old unlink
	ok($pool->('epoch_bump_count') > $pre_bump,
		'L4 TRUNCATE incremented epoch_bump_count');
}


# ----------
# L6: disabled (restart with enabled=off) == spec-3.10 behavior.
# ----------
{
	$node->append_conf('postgresql.conf', "cluster.shared_cr_pool_enabled = off\n");
	$node->restart;

	is($node->safe_psql('postgres', 'SELECT 1'), '1', 'L6a node alive after restart');
	is($pool->('current_epoch'), '0', 'L6b current_epoch == 0 (pool disabled)');

	# a CR read still resolves correctly through the L1-only spec-3.10 path.
	$node->safe_psql('postgres',
		'CREATE TABLE t_off (id int, v int); INSERT INTO t_off VALUES (1,1);');
	my $s = $node->background_psql('postgres', on_error_die => 1);
	$s->query_safe('BEGIN ISOLATION LEVEL REPEATABLE READ');
	$s->query_safe('SELECT count(*) FROM t_off');	# snapshot
	$node->safe_psql('postgres', 'UPDATE t_off SET v = 99 WHERE id = 1');
	my $r1 = $s->query_safe('SELECT v FROM t_off WHERE id = 1');
	my $r2 = $s->query_safe('SELECT v FROM t_off WHERE id = 1');
	chomp $r1; chomp $r2;
	is($r1, '1', 'L6c disabled: A sees correct CR value');
	is($r2, '1', 'L6d disabled: correct on re-read (L1 path)');
	$s->query_safe('COMMIT'); $s->quit;

	is($pool->('hit_count'), '0', 'L6e pool hit_count frozen at 0 while disabled');
	is($pool->('publish_count'), '0', 'L6f pool publish_count frozen at 0 while disabled');
}


$node->stop;
done_testing();
