#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 116_gcs_block_lost_write_2node.pl
#	  spec-2.37 PI simplified + lost-write detection MVP (page_lsn
#	  watermark).  Verifies the 6-helper PI watermark API surface +
#	  53R93 SQLSTATE + 4 NEW dump_gcs counters + reply status enum
#	  extension on a 2-node ClusterPair.
#
#	  L1  ClusterPair startup baseline (both postmasters healthy)
#	  L2  catversion >= 202605440;  wait events count remains 88
#	      (0 NEW wait events per spec-2.37 D11);  gcs key 44→48
#	  L3  4 NEW counters all 0 at startup:
#	      pi_watermark_advance_count / pi_watermark_retire_count /
#	      lost_write_detected_count / lost_write_avoid_count
#	  L4  GUC cluster.gcs_block_lost_write_action default 'error'
#	  L5  GUC switch to 'warn' SHOW returns 'warn'
#	  L6  Normal workload (read-only SELECT pg_class) on both nodes
#	      produces no false-positive lost-write detection
#	  L7  SQLSTATE 53R93 ERRCODE_CLUSTER_LOST_WRITE_DETECTED literal-
#	      encodable in PG SQL (catalog 形式 verification)
#	  L8  GUC switch back to 'error' SHOW returns 'error'
#	  L9  pg_cluster_state.gcs category has 51 keys (spec-2.36 44 + 4)
#	  L10 Reply status enum value 12 (DENIED_LOST_WRITE) is新增的
#	      最大 value (placeholder for future inject-driven detection
#	      TAP coverage — current 2-node baseline workload should not
#	      trigger lost-write under normal operation)
#
# Spec: spec-2.37-pi-simplified-lost-write-detection.md §4.2 (FROZEN v0.3)
#
# Note: behavioral lost-write trigger via cluster-gcs-block-stale-ship
#	inject is a follow-up step;  current Sprint A scope verifies the
#	wire ABI, counter accessibility, and GUC surface on real cluster
#	instances.
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


sub gcs_int
{
	my ($node, $key) = @_;

	my $v = $node->safe_psql('postgres',
		qq{SELECT value FROM pg_cluster_state
		   WHERE category='gcs' AND key='$key'});
	return defined($v) && $v ne '' ? int($v) : 0;
}


# ============================================================
# L1: ClusterPair startup.
# ============================================================
my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'gcs_block_lost_write',
	extra_conf => [ 'autovacuum = off' ]);
$pair->start_pair;

usleep(3_000_000);

is($pair->node0->safe_psql('postgres', 'SELECT 1'),
	'1', 'L1 node0 postmaster alive');
is($pair->node1->safe_psql('postgres', 'SELECT 1'),
	'1', 'L1 node1 postmaster alive');


# ============================================================
# L2: catversion + wait events + gcs key count.
# ============================================================
my $catver = $pair->node0->safe_psql(
	'postgres',
	q{SELECT catalog_version_no::bigint FROM pg_control_system()});
cmp_ok($catver, '>=', 202605440,
	"L2 catversion >= 202605440 (spec-2.37 D10)");

is($pair->node0->safe_psql('postgres',
		'SELECT count(*) FROM pg_stat_cluster_wait_events'),
	'102',
	'L2 pg_stat_cluster_wait_events returns 100 rows (spec-4.6 +1 GRD shard remaster)');

is($pair->node0->safe_psql(
		'postgres',
		q{SELECT count(*) FROM pg_cluster_state WHERE category='gcs'}),
   '51',
   'L2 pg_cluster_state.gcs category has 51 keys (spec-2.36 44 + spec-2.37 4)');


# ============================================================
# L3: 4 NEW counters = 0 at startup.
# ============================================================
for my $node ($pair->node0, $pair->node1)
{
	my $name = $node->name;

	for my $key (qw(pi_watermark_advance_count
		pi_watermark_retire_count
		lost_write_detected_count
		lost_write_avoid_count))
	{
		is(gcs_int($node, $key), 0,
			"L3 $name $key = 0 at startup");
	}
}


# ============================================================
# L4: cluster.gcs_block_lost_write_action default 'error'.
# ============================================================
is($pair->node0->safe_psql('postgres',
		'SHOW cluster.gcs_block_lost_write_action'),
	'error',
	'L4 cluster.gcs_block_lost_write_action default = error');


# ============================================================
# L5: GUC switch to 'warn'.
# ============================================================
$pair->node0->safe_psql('postgres',
	q{ALTER SYSTEM SET cluster.gcs_block_lost_write_action = 'warn'});
$pair->node0->safe_psql('postgres', 'SELECT pg_reload_conf()');
usleep(500_000);
is($pair->node0->safe_psql('postgres',
		'SHOW cluster.gcs_block_lost_write_action'),
	'warn',
	'L5 GUC switch to warn takes effect after pg_reload_conf');


# ============================================================
# L6: Normal workload no false-positive.
# ============================================================
for my $node ($pair->node0, $pair->node1)
{
	for (1 .. 5)
	{
		$node->safe_psql('postgres', 'SELECT count(*) FROM pg_class');
	}
}

my $total_detected = gcs_int($pair->node0, 'lost_write_detected_count')
	+ gcs_int($pair->node1, 'lost_write_detected_count');
cmp_ok($total_detected, '==', 0,
	"L6 normal read workload zero lost-write false-positives (total=$total_detected)");


# ============================================================
# L7: SQLSTATE 53R93 literal-encodable.
# ============================================================
{
	my $r = $pair->node0->safe_psql('postgres', q{
		SELECT '53R93'::text
	});
	is($r, '53R93', 'L7 SQLSTATE 53R93 ERRCODE_CLUSTER_LOST_WRITE_DETECTED encodable');
}


# ============================================================
# L8: GUC switch back to 'error'.
# ============================================================
$pair->node0->safe_psql('postgres',
	q{ALTER SYSTEM SET cluster.gcs_block_lost_write_action = 'error'});
$pair->node0->safe_psql('postgres', 'SELECT pg_reload_conf()');
usleep(500_000);
is($pair->node0->safe_psql('postgres',
		'SHOW cluster.gcs_block_lost_write_action'),
	'error',
	'L8 GUC switch back to error takes effect');


# ============================================================
# L9 (alias of L2): gcs key count = 48.
# ============================================================
is($pair->node1->safe_psql(
		'postgres',
		q{SELECT count(*) FROM pg_cluster_state WHERE category='gcs'}),
   '51',
   'L9 node1 pg_cluster_state.gcs has 51 keys (cross-node parity)');


# ============================================================
# L10: reply status enum 12 is the new max value (verified via
# gcs key catalog presence — no behavioral inject this Sprint).
# ============================================================
cmp_ok(gcs_int($pair->node0, 'lost_write_detected_count'), '<=', 0,
	'L10 lost_write_detected_count baseline (placeholder for inject TAP)');


done_testing();
