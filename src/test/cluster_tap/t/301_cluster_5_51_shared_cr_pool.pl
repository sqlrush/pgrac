#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 301_cluster_5_51_shared_cr_pool.pl
#	  spec-5.51 D12 — dedicated shared (L2) CR buffer pool, real-server e2e.
#
#	  L1   pool ON (enabled + size>0): GUCs visible, cr_pool category has 10
#	       rows, current_epoch >= 1.
#	  L2   cross-backend L2 HIT: an exported snapshot makes backends A and B
#	       share the SAME read_scn (=> same CR key).  A re-reads a post-snapshot
#	       modified block -> CR construct -> publish to the shared pool; B reads
#	       the same block under the same exported snapshot -> L2 HIT (pool
#	       hit_count++), and B does NOT reconstruct (cr_construct_count frozen
#	       across B's read) -- this is the cross-backend sharing proof.
#	  L3   epoch invalidation / no false-hit: after the entry is published, a
#	       lifecycle event (DROP of an unrelated table -> relfilenode unlink)
#	       bumps the pool epoch; a THIRD reader C under the same exported
#	       snapshot now MISSES the pool (epoch stale) and reconstructs -- the
#	       stale-epoch image is never served (8.A no-false-hit).
#	  L4   epoch bump counter: DROP and TRUNCATE both advance epoch_bump_count.
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
#	  src/test/cluster_tap/t/301_cluster_5_51_shared_cr_pool.pl
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../lib";

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::ClusterPair;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep);


my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'spec_5_51_pool',
	extra_conf => [
		'autovacuum = off',
		'cluster.pcm_grd_max_entries = 0',
		'max_prepared_transactions = 4',
		'cluster.shared_cr_pool_enabled = on',
		'cluster.shared_cr_pool_size_blocks = 256',
	]);
$pair->start_pair;
usleep(2_000_000);

my $node0 = $pair->node0;

# read a counter from pg_cluster_state by (category, key)
my $cnt = sub {
	my ($cat, $k) = @_;
	return $node0->safe_psql('postgres',
		qq{SELECT value::bigint FROM pg_cluster_state WHERE category='$cat' AND key='$k'});
};


# ----------
# L1: pool enabled, GUCs + cr_pool category.
# ----------
{
	is($node0->safe_psql('postgres', 'SELECT 1'), '1', 'L1a node0 alive');

	is( $node0->safe_psql('postgres',
			q{SELECT setting FROM pg_settings WHERE name='cluster.shared_cr_pool_enabled'}),
		'on', 'L1b shared_cr_pool_enabled = on');

	is( $node0->safe_psql('postgres',
			q{SELECT setting FROM pg_settings WHERE name='cluster.shared_cr_pool_size_blocks'}),
		'256', 'L1c shared_cr_pool_size_blocks = 256');

	is( $node0->safe_psql('postgres',
			q{SELECT context FROM pg_settings WHERE name='cluster.shared_cr_pool_size_blocks'}),
		'postmaster', 'L1d size_blocks is PGC_POSTMASTER');

	is( $node0->safe_psql('postgres',
			q{SELECT count(*) FROM pg_cluster_state WHERE category='cr_pool'}),
		'10', 'L1e cr_pool category has 10 rows');

	ok($cnt->('cr_pool', 'current_epoch') >= 1,
		'L1f current_epoch >= 1 (pool enabled)');
}


# ----------
# L2: cross-backend L2 HIT via exported snapshot.
# ----------
{
	$node0->safe_psql('postgres',
		'CREATE TABLE t_share (id int, v int);
		 INSERT INTO t_share SELECT g, g FROM generate_series(1,4) g;');

	# long-lived exporter holds the snapshot open for A/B/C to import.
	my $exp = $node0->background_psql('postgres', on_error_stop => 0);
	$exp->query_safe('BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ');
	$exp->query_safe('SELECT count(*) FROM t_share');	# fix the snapshot
	my $snap = $exp->query_safe('SELECT pg_export_snapshot()');
	chomp $snap;

	# modify the block AFTER the exported snapshot -> importers need CR.
	$node0->safe_psql('postgres', 'UPDATE t_share SET v = v + 100 WHERE id = 1');

	my $pre_publish  = $cnt->('cr_pool', 'publish_count');
	my $pre_reserve  = $cnt->('cr_pool', 'reserve_count');

	# backend A: import snapshot, read -> construct + publish to pool.
	my $sa = $node0->background_psql('postgres', on_error_stop => 0);
	$sa->query_safe('BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ');
	$sa->query_safe("SET TRANSACTION SNAPSHOT '$snap'");
	my $ra = $sa->query_safe('SELECT v FROM t_share WHERE id = 1');
	chomp $ra;
	is($ra, '1', 'L2a A sees pre-update value via CR');

	ok($cnt->('cr_pool', 'reserve_count') > $pre_reserve,
		'L2b A reserved a pool slot');
	ok($cnt->('cr_pool', 'publish_count') > $pre_publish,
		'L2c A published its CR image to the shared pool');

	# backend B: SAME exported snapshot -> SAME key -> must HIT the pool, and
	# must NOT reconstruct (cr_construct_count frozen across B's read).
	my $pre_hit       = $cnt->('cr_pool', 'hit_count');
	my $pre_construct = $cnt->('cr', 'cr_construct_count');

	my $sb = $node0->background_psql('postgres', on_error_stop => 0);
	$sb->query_safe('BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ');
	$sb->query_safe("SET TRANSACTION SNAPSHOT '$snap'");
	my $rb = $sb->query_safe('SELECT v FROM t_share WHERE id = 1');
	chomp $rb;
	is($rb, '1', 'L2d B sees the same pre-update value');

	ok($cnt->('cr_pool', 'hit_count') > $pre_hit,
		'L2e B HIT the shared pool (cross-backend share)');
	is($cnt->('cr', 'cr_construct_count'), $pre_construct,
		'L2f B did NOT reconstruct (served from L2)');

	# ----------
	# L3: epoch invalidation -- after a lifecycle bump, C must MISS + rebuild.
	# ----------
	$node0->safe_psql('postgres', 'CREATE TABLE t_drop_me (x int); INSERT INTO t_drop_me VALUES (1);');
	my $pre_epoch       = $cnt->('cr_pool', 'current_epoch');
	my $pre_bump        = $cnt->('cr_pool', 'epoch_bump_count');
	$node0->safe_psql('postgres', 'DROP TABLE t_drop_me');	# relfilenode unlink -> bump

	ok($cnt->('cr_pool', 'current_epoch') > $pre_epoch,
		'L3a DROP advanced the pool epoch');
	ok($cnt->('cr_pool', 'epoch_bump_count') > $pre_bump,
		'L3b epoch_bump_count incremented on DROP');

	my $pre_miss       = $cnt->('cr_pool', 'miss_count');
	my $pre_construct2 = $cnt->('cr', 'cr_construct_count');

	my $sc = $node0->background_psql('postgres', on_error_stop => 0);
	$sc->query_safe('BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ');
	$sc->query_safe("SET TRANSACTION SNAPSHOT '$snap'");
	my $rc = $sc->query_safe('SELECT v FROM t_share WHERE id = 1');
	chomp $rc;
	is($rc, '1', 'L3c C still sees the correct CR value after epoch bump');

	ok($cnt->('cr_pool', 'miss_count') > $pre_miss,
		'L3d C MISSED the pool (stale epoch not served)');
	ok($cnt->('cr', 'cr_construct_count') > $pre_construct2,
		'L3e C reconstructed (no false-hit on a stale-epoch entry)');

	$sc->query_safe('COMMIT'); $sc->quit;
	$sb->query_safe('COMMIT'); $sb->quit;
	$sa->query_safe('COMMIT'); $sa->quit;
	$exp->query_safe('COMMIT'); $exp->quit;
}


# ----------
# L4: TRUNCATE also bumps the epoch.
# ----------
{
	$node0->safe_psql('postgres',
		'CREATE TABLE t_trunc (id int, v int); INSERT INTO t_trunc VALUES (1,1);');
	my $pre_bump = $cnt->('cr_pool', 'epoch_bump_count');
	$node0->safe_psql('postgres', 'TRUNCATE t_trunc');	# new relfilenode + old unlink
	ok($cnt->('cr_pool', 'epoch_bump_count') > $pre_bump,
		'L4 TRUNCATE incremented epoch_bump_count');
}


# ----------
# L6: disabled (restart with enabled=off) == spec-3.10 behavior.
# ----------
{
	$node0->append_conf('postgresql.conf', "cluster.shared_cr_pool_enabled = off\n");
	$node0->restart;
	usleep(1_000_000);

	is($node0->safe_psql('postgres', 'SELECT 1'), '1', 'L6a node0 alive after restart');
	is($cnt->('cr_pool', 'current_epoch'), '0',
		'L6b current_epoch == 0 (pool disabled)');

	# a CR read still resolves correctly through the L1-only spec-3.10 path.
	$node0->safe_psql('postgres',
		'CREATE TABLE t_off (id int, v int); INSERT INTO t_off VALUES (1,1);');
	my $sa = $node0->background_psql('postgres', on_error_stop => 0);
	$sa->query_safe('BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ');
	$sa->query_safe('SELECT count(*) FROM t_off');
	$node0->safe_psql('postgres', 'UPDATE t_off SET v = 99 WHERE id = 1');
	my $r1 = $sa->query_safe('SELECT v FROM t_off WHERE id = 1');
	my $r2 = $sa->query_safe('SELECT v FROM t_off WHERE id = 1');
	chomp $r1; chomp $r2;
	is($r1, '1', 'L6c disabled: A sees correct CR value');
	is($r2, '1', 'L6d disabled: correct on re-read (L1 path)');
	$sa->query_safe('COMMIT'); $sa->quit;

	is($cnt->('cr_pool', 'hit_count'), '0',
		'L6e pool hit_count frozen at 0 while disabled');
	is($cnt->('cr_pool', 'publish_count'), '0',
		'L6f pool publish_count frozen at 0 while disabled');
}


$pair->stop_pair;
done_testing();
