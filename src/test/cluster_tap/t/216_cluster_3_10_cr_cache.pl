#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 216_cluster_3_10_cr_cache.pl
#	  spec-3.10 D9 — CR block cache + full-block CR behavioral TAP.
#
#	  L1   ClusterPair up + GUC cluster.cr_cache_max_blocks default 64 +
#	       cr category has 30 rows (9 spec-3.9 + 4 spec-3.10 cache + 4 spec-3.22 xmax
#	       + 5 spec-5.53 mismatch + 8 spec-5.54 tuple)
#	  L2   cache HIT e2e: A's RR snapshot re-reads a post-snapshot-modified
#	       block twice → cr_cache_hit_count++ (2nd read served from cache),
#	       cr_cache_miss_count++ + cr_cache_install_count++ (1st read built it)
#	  L3   page_lsn guard: A caches a block; B modifies it (bumps page LSN);
#	       A re-reads under the SAME read_scn → MISS (guard) + A still sees its
#	       own snapshot version (no stale-layout reuse)
#	  L4   full-block CR: id=1 modified by txB, id=2 by txC (same block); A's
#	       snapshot sees BOTH old values (3.9 per-chain reverted only one →
#	       this is the full-block upgrade proof)
#	  L4b  same-row multi-update: id=3 UPDATEd by txB then txC; A's snapshot
#	       sees exactly ONE version = the read_scn value (proves write_scn-DESC
#	       apply order — slot_idx order would corrupt, spec-3.10 Q10)
#	  L5   eviction: cluster.cr_cache_max_blocks=2 + 3 distinct cached blocks
#	       → cr_cache_evict_count++
#	  L6   disable: cluster.cr_cache_max_blocks=0 → cr_cache_hit_count frozen
#	       (only miss/install), visibility still correct
#	  L7   4 cache counters exposed in pg_cluster_state cr category
#
#	  Spec: spec-3.10-cr-block-cache.md (FROZEN v0.3)
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
	'spec_3_10_cache',
	quorum_voting_disks => 3,
	extra_conf => [
		'autovacuum = off',
		'cluster.pcm_grd_max_entries = 0',
		'max_prepared_transactions = 4',
	]);
$pair->start_pair;
usleep(2_000_000);

my $node0 = $pair->node0;

my $val = sub {
	my ($k) = @_;
	return $node0->safe_psql('postgres',
		qq{SELECT value::bigint FROM pg_cluster_state WHERE category='cr' AND key='$k'});
};


# ----------
# L1: startup + GUC + cr category 13 rows
# ----------
{
	is($node0->safe_psql('postgres', 'SELECT 1'), '1', 'L1a node0 alive');

	is( $node0->safe_psql('postgres',
			q{SELECT setting FROM pg_settings WHERE name='cluster.cr_cache_max_blocks'}),
		'64', 'L1b cr_cache_max_blocks default 64');

	is( $node0->safe_psql('postgres',
			q{SELECT context FROM pg_settings WHERE name='cluster.cr_cache_max_blocks'}),
		'user', 'L1c cr_cache_max_blocks PGC_USERSET');

	is( $node0->safe_psql('postgres',
			q{SELECT count(*) FROM pg_cluster_state WHERE category='cr'}),
		'35',
		'L1d cr category has 35 rows (9 + 4 cache + 4 xmax + 5 spec-5.53 mismatch + 8 spec-5.54 tuple + 5 spec-5.56 lifecycle)');
}


# ----------
# L2: cache HIT e2e — A re-reads a post-snapshot-modified block twice.
# ----------
{
	$node0->safe_psql('postgres',
		'CREATE TABLE t_hit (id int, v int);
		 INSERT INTO t_hit SELECT g, g FROM generate_series(1,4) g;');

	my $pre_hit     = $val->('cr_cache_hit_count');
	my $pre_miss    = $val->('cr_cache_miss_count');
	my $pre_install = $val->('cr_cache_install_count');

	my $sa = $node0->background_psql('postgres', on_error_stop => 0);
	$sa->query_safe('BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ');
	$sa->query_safe('SELECT count(*) FROM t_hit');	# take snapshot

	# B modifies the block after A's snapshot -> A's re-reads need CR.
	$node0->safe_psql('postgres', 'UPDATE t_hit SET v = v + 100 WHERE id = 1');

	# A's first re-read: gate fires -> miss -> construct -> install.
	my $r1 = $sa->query_safe('SELECT v FROM t_hit WHERE id = 1');
	chomp $r1;
	is($r1, '1', 'L2a A sees pre-update value via CR (first read)');

	# A's second re-read (same snapshot, same block, same page_lsn): cache HIT.
	my $r2 = $sa->query_safe('SELECT v FROM t_hit WHERE id = 1');
	chomp $r2;
	is($r2, '1', 'L2b A sees pre-update value again (cache hit)');

	$sa->query_safe('COMMIT');
	$sa->quit;

	ok($val->('cr_cache_miss_count') > $pre_miss,
		'L2c cr_cache_miss_count incremented (first read built the image)');
	ok($val->('cr_cache_install_count') > $pre_install,
		'L2d cr_cache_install_count incremented');
	ok($val->('cr_cache_hit_count') > $pre_hit,
		'L2e cr_cache_hit_count incremented (second read served from cache)');
}


# ----------
# L3: page_lsn version guard — modify block between A's reads.
# ----------
{
	$node0->safe_psql('postgres',
		'CREATE TABLE t_guard (id int, v int);
		 INSERT INTO t_guard SELECT g, g FROM generate_series(1,4) g;');

	my $pre_miss = $val->('cr_cache_miss_count');

	my $sa = $node0->background_psql('postgres', on_error_stop => 0);
	$sa->query_safe('BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ');
	$sa->query_safe('SELECT count(*) FROM t_guard');

	$node0->safe_psql('postgres', 'UPDATE t_guard SET v = v + 100 WHERE id = 2');
	my $r1 = $sa->query_safe('SELECT v FROM t_guard WHERE id = 2');	# miss #1, cache
	chomp $r1;
	is($r1, '2', 'L3a A sees pre-update value');

	# Second modification bumps the page LSN -> A's cached image is stale-layout
	# eligible; the guard must force a fresh miss + still yield A's version.
	$node0->safe_psql('postgres', 'UPDATE t_guard SET v = v + 1000 WHERE id = 4');
	my $r2 = $sa->query_safe('SELECT v FROM t_guard WHERE id = 2');	# page_lsn changed -> miss #2
	chomp $r2;
	is($r2, '2', 'L3b A still sees its snapshot version after page_lsn change');

	$sa->query_safe('COMMIT');
	$sa->quit;

	# Two distinct page_lsn keys for the same block -> at least 2 misses.
	ok($val->('cr_cache_miss_count') >= $pre_miss + 2,
		'L3c page_lsn change forced a fresh miss (no stale-layout reuse)');
}


# ----------
# L4: full-block CR — two rows on one block changed by two transactions.
# ----------
{
	# all rows on page 0 (tiny table)
	$node0->safe_psql('postgres',
		"CREATE TABLE t_fb (id int, v text);
		 INSERT INTO t_fb VALUES (1,'a'),(2,'b'),(3,'c');");

	my $sa = $node0->background_psql('postgres', on_error_stop => 0);
	$sa->query_safe('BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ');
	$sa->query_safe('SELECT count(*) FROM t_fb');

	# two SEPARATE transactions each modify a different row on the same block
	$node0->safe_psql('postgres', q{UPDATE t_fb SET v = 'A' WHERE id = 1});
	$node0->safe_psql('postgres', q{UPDATE t_fb SET v = 'B' WHERE id = 2});

	# A's snapshot must see BOTH old values (full-block reverts both chains)
	my $av = $sa->query_safe(q{SELECT string_agg(v, ',' ORDER BY id) FROM t_fb});
	chomp $av;
	is($av, 'a,b,c', 'L4 A sees all pre-update values (full-block reverts both chains)');

	$sa->query_safe('COMMIT');
	$sa->quit;
}


# ----------
# L4b: same-row multi-update — write_scn-DESC peel order (Q10).
# ----------
{
	$node0->safe_psql('postgres',
		"CREATE TABLE t_multi (id int, v int);
		 INSERT INTO t_multi VALUES (3, 30);");

	my $sa = $node0->background_psql('postgres', on_error_stop => 0);
	$sa->query_safe('BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ');
	$sa->query_safe('SELECT count(*) FROM t_multi');

	# id=3 updated by TWO separate post-snapshot transactions (txB then txC)
	$node0->safe_psql('postgres', 'UPDATE t_multi SET v = 31 WHERE id = 3');	# txB
	$node0->safe_psql('postgres', 'UPDATE t_multi SET v = 32 WHERE id = 3');	# txC

	# A must see exactly ONE version = the read_scn value 30.  slot_idx order
	# would peel B before C and corrupt; write_scn DESC peels C then B -> 30.
	my $cnt = $sa->query_safe('SELECT count(*) FROM t_multi WHERE id = 3');
	chomp $cnt;
	is($cnt, '1', 'L4b A sees exactly one version of id=3');

	my $v = $sa->query_safe('SELECT v FROM t_multi WHERE id = 3');
	chomp $v;
	is($v, '30', 'L4b A sees the read_scn value 30 (write_scn-DESC peel; Q10)');

	$sa->query_safe('COMMIT');
	$sa->quit;
}


# ----------
# L5: eviction under a small cache.
# ----------
{
	my $pre_evict = $val->('cr_cache_evict_count');

	# 3 distinct one-row tables, each its own block; cache capped at 2.
	for my $t (1 .. 3) {
		$node0->safe_psql('postgres',
			"CREATE TABLE t_ev$t (id int, v int); INSERT INTO t_ev$t VALUES ($t,$t);");
	}

	my $sa = $node0->background_psql('postgres', on_error_stop => 0);
	$sa->query_safe('SET cluster.cr_cache_max_blocks = 2');
	$sa->query_safe('BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ');
	$sa->query_safe('SELECT 1');

	for my $t (1 .. 3) {
		$node0->safe_psql('postgres', "UPDATE t_ev$t SET v = v + 100 WHERE id = $t");
	}
	# A caches 3 distinct blocks into a 2-slot cache -> >=1 eviction.
	for my $t (1 .. 3) {
		$sa->query_safe("SELECT v FROM t_ev$t WHERE id = $t");
	}

	$sa->query_safe('COMMIT');
	$sa->quit;

	ok($val->('cr_cache_evict_count') > $pre_evict,
		'L5 cr_cache_evict_count incremented under a 2-block cache');
}


# ----------
# L6: disabled cache (max_blocks = 0) — no hits, visibility still correct.
# ----------
{
	$node0->safe_psql('postgres',
		'CREATE TABLE t_off (id int, v int); INSERT INTO t_off VALUES (1, 1);');

	my $pre_hit = $val->('cr_cache_hit_count');

	my $sa = $node0->background_psql('postgres', on_error_stop => 0);
	$sa->query_safe('SET cluster.cr_cache_max_blocks = 0');	# disable
	$sa->query_safe('BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ');
	$sa->query_safe('SELECT count(*) FROM t_off');

	$node0->safe_psql('postgres', 'UPDATE t_off SET v = 99 WHERE id = 1');

	my $r1 = $sa->query_safe('SELECT v FROM t_off WHERE id = 1');
	my $r2 = $sa->query_safe('SELECT v FROM t_off WHERE id = 1');
	chomp $r1; chomp $r2;
	is($r1, '1', 'L6a disabled: A sees correct CR value');
	is($r2, '1', 'L6b disabled: still correct on re-read');

	$sa->query_safe('COMMIT');
	$sa->quit;

	is($val->('cr_cache_hit_count'), $pre_hit,
		'L6c cr_cache_hit_count frozen while disabled (no caching)');
}


# ----------
# L7: 4 cache counters exposed.
# ----------
is( $node0->safe_psql('postgres',
		q{SELECT count(*) FROM pg_cluster_state WHERE category='cr'
		   AND key IN ('cr_cache_hit_count','cr_cache_miss_count',
		               'cr_cache_evict_count','cr_cache_install_count')}),
	'4', 'L7 4 CR cache counters exposed');


$pair->stop_pair;
done_testing();
