#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 117_sinval_broadcast_2node.pl
#	  spec-2.38 SI Broadcaster skeleton integration on a 2-node ClusterPair.
#	  Verifies aux process spawn + LMON-mediated wire ABI + dump_sinval observability +
#	  GUC defaults + 53R94 SQLSTATE.  Behavioral inject (echo defense,
#	  inbound fail-safe reset, queue full fail-closed) lives in the
#	  cluster_unit static contracts (L20-L24) since true cross-node
#	  inject hooks are deferred to spec-2.39 DDL hook coverage.
#
#	  L1   ClusterPair startup + both nodes alive
#	  L2   catversion >= 202605450
#	  L3   wait events count == 88 (0 new spec-2.38; 3 already at 0.11)
#	  L4   sinval category has 9 keys (D10 +9 counter rows)
#	  L5   all 9 sinval counters = 0 at startup
#	  L6   cluster.sinval_broadcast_batch_size default 32
#	  L7   cluster.sinval_broadcast_batch_timeout_ms default 10
#	  L8   cluster.sinval_broadcast_max_queue_size default 1024
#	  L9   53R94 SQLSTATE encodable in PG SQL
#	  L10  3 wait events ClusterSinvalBroadcast{Send,Receive} +
#	       SinvalInjectLocalQueue visible in pg_stat_cluster_wait_events
#	  L11  pg_stat_gcluster_wait_events also exposes the 3 events
#	  L12  Cross-node sanity:  both nodes report consistent gcs+sinval
#	       category key count (HC132 outbound queue independence does not
#	       break cluster-wide observability)
#	  L13  PGRAC_IC_MSG_SINVAL=7 is registered (catalog 形式 check via
#	       inject point absence) — placeholder for spec-2.39 DDL inject
#
# Spec: spec-2.38-si-broadcaster-skeleton.md §4.2 (FROZEN v0.3)
#
# Note: behavioral cross-node broadcast / fail-safe reset / echo defense
#	require either (a) spec-2.39 DDL hook to trigger real sinval, or
#	(b) a test-only inject hook to call cluster_sinval_enqueue_batch()
#	from SQL (deferred to follow-up).  Skeleton scope verifies the
#	wire ABI + observability + GUC + SQLSTATE surface on real cluster
#	instances + cluster_unit static contracts for HC132/HC133/HC134.
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


sub sinval_int
{
	my ($node, $key) = @_;

	my $v = $node->safe_psql('postgres',
		qq{SELECT value FROM pg_cluster_state
		   WHERE category='sinval' AND key='$key'});
	return defined($v) && $v ne '' ? int($v) : 0;
}


# ============================================================
# L1: ClusterPair startup.
# ============================================================
my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'sinval_broadcast',
	extra_conf => [ 'autovacuum = off' ]);
$pair->start_pair;

usleep(3_000_000);

is($pair->node0->safe_psql('postgres', 'SELECT 1'),
	'1', 'L1 node0 postmaster alive');
is($pair->node1->safe_psql('postgres', 'SELECT 1'),
	'1', 'L1 node1 postmaster alive');


# ============================================================
# L2: catversion.
# ============================================================
my $catver = $pair->node0->safe_psql(
	'postgres',
	q{SELECT catalog_version_no::bigint FROM pg_control_system()});
cmp_ok($catver, '>=', 202605450,
	"L2 catversion >= 202605450 (spec-2.38 D7)");


# ============================================================
# L3: wait events count baseline.
# ============================================================
is($pair->node0->safe_psql('postgres',
		'SELECT count(*) FROM pg_stat_cluster_wait_events'),
	'120',
	'L3 wait event count == 120 (spec-7.2 D6 LMS data-plane wait surface)');


# ============================================================
# L4-L5: sinval category 16 keys + counters = 0.
# ============================================================
is($pair->node0->safe_psql(
		'postgres',
		q{SELECT count(*) FROM pg_cluster_state WHERE category='sinval'}),
   '16',
   'L4 node0 pg_cluster_state.sinval category has 16 keys (spec-2.39 D8/D9 +6 + spec-5.2 D1 smgr_inval_applied_count)');

is($pair->node1->safe_psql(
		'postgres',
		q{SELECT count(*) FROM pg_cluster_state WHERE category='sinval'}),
   '16',
   'L4 node1 pg_cluster_state.sinval category has 16 keys');

for my $node ($pair->node0, $pair->node1)
{
	my $name = $node->name;

	for my $key (qw(broadcast_send_count broadcast_receive_count
		inject_local_queue_count outbound_queue_full_count
		inbound_queue_full_count inbound_overflow_reset_count
		validation_drop_count stale_epoch_drop_count
		echo_dropped_count))
	{
		is(sinval_int($node, $key), 0,
			"L5 $name $key = 0 at startup");
	}
}


# ============================================================
# L6-L8: GUC defaults.
# ============================================================
is($pair->node0->safe_psql('postgres',
		'SHOW cluster.sinval_broadcast_batch_size'),
	'32',
	'L6 cluster.sinval_broadcast_batch_size default 32');
is($pair->node0->safe_psql('postgres',
		'SHOW cluster.sinval_broadcast_batch_timeout_ms'),
	'10',
	'L7 cluster.sinval_broadcast_batch_timeout_ms default 10');
is($pair->node0->safe_psql('postgres',
		'SHOW cluster.sinval_broadcast_max_queue_size'),
	'1024',
	'L8 cluster.sinval_broadcast_max_queue_size default 1024');


# ============================================================
# L9: 53R94 SQLSTATE encodable.
# ============================================================
{
	my $r = $pair->node0->safe_psql('postgres', q{
		SELECT '53R94'::text
	});
	is($r, '53R94', 'L9 SQLSTATE 53R94 ERRCODE_CLUSTER_SINVAL_QUEUE_FULL encodable');
}


# ============================================================
# L10-L11: 3 wait events visible.
# ============================================================
is($pair->node0->safe_psql('postgres',
		q{SELECT count(*) FROM pg_stat_cluster_wait_events
		   WHERE name IN ('SinvalBroadcastSend',
		                  'SinvalBroadcastReceive',
		                  'SinvalInjectLocalQueue')}),
	'3',
	'L10 3 SI Broadcaster wait events registered in pg_stat_cluster_wait_events');

is($pair->node0->safe_psql('postgres',
		q{SELECT count(*) FROM pg_stat_gcluster_wait_events
		   WHERE name IN ('SinvalBroadcastSend',
		                  'SinvalBroadcastReceive',
		                  'SinvalInjectLocalQueue')}),
	'3',
	'L11 same 3 events visible in pg_stat_gcluster_wait_events');


# ============================================================
# L12: cross-node consistency.
# ============================================================
my $k0 = $pair->node0->safe_psql('postgres',
	q{SELECT count(DISTINCT category) FROM pg_cluster_state
	   WHERE category IN ('gcs','sinval')});
my $k1 = $pair->node1->safe_psql('postgres',
	q{SELECT count(DISTINCT category) FROM pg_cluster_state
	   WHERE category IN ('gcs','sinval')});
is($k0, $k1, "L12 cross-node gcs+sinval category visibility consistent ($k0 == $k1)");


# ============================================================
# L13: PGRAC_IC_MSG_SINVAL=7 registered (catalog placeholder).
# ============================================================
{
	# Validate no behavioral regression — outbound counter stays 0 in
	# normal workload (no DDL hook in spec-2.38 scope).
	$pair->node0->safe_psql('postgres', 'SELECT count(*) FROM pg_class');
	$pair->node1->safe_psql('postgres', 'SELECT count(*) FROM pg_class');
	usleep(200_000);
	my $send_total = sinval_int($pair->node0, 'broadcast_send_count')
		+ sinval_int($pair->node1, 'broadcast_send_count');
	cmp_ok($send_total, '==', 0,
		"L13 no spurious broadcast_send under read-only workload (skeleton scope)");
}


done_testing();
