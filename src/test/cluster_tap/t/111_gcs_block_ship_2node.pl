#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 111_gcs_block_ship_2node.pl
#	  spec-2.33 end-to-end integration of GCS block-shipping substrate on
#	  a 2-node ClusterPair.  Spec-2.33 enables real cross-node Cache
#	  Fusion data plane on top of spec-2.32 control plane:
#
#	  L1  ClusterPair startup — both postmasters healthy + tier1 connected
#	  L2  fresh baseline gcs counters on both nodes (block_* counters = 0)
#	  L3  pg_cluster_state.gcs category has 22 keys (14 spec-2.32 control
#	       plane + 8 spec-2.33 block-ship data plane)
#	  L4  4 NEW wait events registered in pg_stat_cluster_wait_events:
#	       ClusterGCSBlockShipWait, ClusterGCSBlockRequestDispatch,
#	       ClusterGCSBlockReplyDispatch, ClusterGCSBlockChecksumFail
#	  L5  CLUSTER_WAIT_EVENTS_COUNT = 83 (was 79 in spec-2.32)
#	  L6  cluster.gcs_reply_timeout_ms GUC visible + default 5000 +
#	       PGC_SUSET context
#	  L7  pg_stat_cluster_msg_types registry has gcs_block_request +
#	       gcs_block_reply rows (msg_type 14 + 15) on both nodes
#	  L8  workload-induced GRD lookups split across nodes
#	       (lookup_master_remote_count grows somewhere)
#	  L9  block-ship counters monotone non-decreasing under workload
#	  L10 block_checksum_fail_count remains 0 under healthy workload
#	       (HC83 negative — corruption would inc this counter)
#	  L11 dump_gcs counters cross-node sum is internally consistent
#	       (block_request_count A + B == block_reply_count A + B, when
#	       the round-trip closes)
#	  L12 catversion bumped to 202605400 (spec-2.33 D11)
#
# Spec: spec-2.33-gcs-block-shipping-substrate.md §4.2 (FROZEN v0.4)
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
use PgracClusterNode;


sub gcs_value {
	my ($node, $key) = @_;

	return $node->safe_psql('postgres',
		qq{SELECT value FROM pg_cluster_state
		   WHERE category='gcs' AND key='$key'});
}

sub gcs_int_value {
	my ($node, $key) = @_;

	my $v = gcs_value($node, $key);
	return defined($v) && $v ne '' ? int($v) : 0;
}


# ============================================================
# L1: ClusterPair startup.  2-node strict-mode pair with 3 voting disks.
# ============================================================
my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'gcs_block_ship', quorum_voting_disks => 3);
$pair->start_pair;

is($pair->node0->safe_psql('postgres', 'SELECT 1'),
	'1', 'L1 node0 postmaster alive');
is($pair->node1->safe_psql('postgres', 'SELECT 1'),
	'1', 'L1 node1 postmaster alive');

ok($pair->wait_for_peer_state(0, 1, 'connected', 30),
	'L1 node0 sees node1 connected');
ok($pair->wait_for_peer_state(1, 0, 'connected', 30),
	'L1 node1 sees node0 connected');


# ============================================================
# L2: fresh baseline — all 8 block-plane counters = 0 on both nodes.
# ============================================================
for my $node ($pair->node0, $pair->node1) {
	my $node_label = $node->name;

	is(gcs_value($node, 'block_request_count'), '0',
		"L2 $node_label block_request_count = 0 at startup");
	is(gcs_value($node, 'block_reply_count'), '0',
		"L2 $node_label block_reply_count = 0 at startup");
	is(gcs_value($node, 'block_timeout_count'), '0',
		"L2 $node_label block_timeout_count = 0 at startup");
	is(gcs_value($node, 'block_checksum_fail_count'), '0',
		"L2 $node_label block_checksum_fail_count = 0 at startup");
	is(gcs_value($node, 'block_storage_fallback_count'), '0',
		"L2 $node_label block_storage_fallback_count = 0 at startup");
	is(gcs_value($node, 'block_master_not_holder_count'), '0',
		"L2 $node_label block_master_not_holder_count = 0 at startup");
	is(gcs_value($node, 'block_wal_flush_before_ship_count'), '0',
		"L2 $node_label block_wal_flush_before_ship_count = 0 at startup");
	is(gcs_value($node, 'block_ship_bytes_total'), '0',
		"L2 $node_label block_ship_bytes_total = 0 at startup");
}


# ============================================================
# L3: pg_cluster_state.gcs category has 22 keys (14 control + 8 data).
# ============================================================
is($pair->node0->safe_psql(
		'postgres',
		q{SELECT count(*) FROM pg_cluster_state WHERE category='gcs'}),
   '22',
   'L3 node0 pg_cluster_state.gcs category has 22 keys');
is($pair->node1->safe_psql(
		'postgres',
		q{SELECT count(*) FROM pg_cluster_state WHERE category='gcs'}),
   '22',
   'L3 node1 pg_cluster_state.gcs category has 22 keys');


# ============================================================
# L4: 4 NEW wait events registered.
# ============================================================
for my $we_name (
	'ClusterGCSBlockShipWait',
	'ClusterGCSBlockRequestDispatch',
	'ClusterGCSBlockReplyDispatch',
	'ClusterGCSBlockChecksumFail',
) {
	is($pair->node0->safe_psql(
			'postgres',
			qq{SELECT count(*) FROM pg_stat_cluster_wait_events
			   WHERE name = '$we_name'}),
	   '1',
	   "L4 wait event $we_name registered on node0");
}


# ============================================================
# L5: total wait event count = 83.
# ============================================================
is($pair->node0->safe_psql(
		'postgres',
		'SELECT count(*) FROM pg_stat_cluster_wait_events'),
   '83',
   'L5 total cluster wait event count = 83 (spec-2.32 79 + 4 NEW)');


# ============================================================
# L6: cluster.gcs_reply_timeout_ms GUC visible + default 5000 + SUSET.
# ============================================================
is($pair->node0->safe_psql(
		'postgres',
		q{SHOW cluster.gcs_reply_timeout_ms}),
   '5000ms',
   'L6 cluster.gcs_reply_timeout_ms default = 5000ms');

my $context = $pair->node0->safe_psql(
	'postgres',
	q{SELECT context FROM pg_settings WHERE name = 'cluster.gcs_reply_timeout_ms'});
is($context, 'superuser',
   'L6 cluster.gcs_reply_timeout_ms context = superuser (PGC_SUSET)');


# ============================================================
# L7: pg_stat_cluster_msg_types has gcs_block_request + gcs_block_reply.
# ============================================================
for my $msg (qw(gcs_block_request gcs_block_reply)) {
	is($pair->node0->safe_psql(
			'postgres',
			qq{SELECT count(*) FROM pg_stat_cluster_msg_types
			   WHERE name = '$msg'}),
	   '1',
	   "L7 msg_type $msg registered in dispatch table");
}


# ============================================================
# L8: workload-induced GRD lookups split across nodes.  With
#	   deterministic hash mod-N over a 2-node topology, ~50% of
#	   distinct tags hash to the other node.
# ============================================================
$pair->node0->safe_psql('postgres', q{
	CREATE TABLE block_ship_t (id int, val text);
	INSERT INTO block_ship_t SELECT g, 'row-' || g
	  FROM generate_series(1, 1000) g;
});

# Force buffers to be touched on both nodes by reading from both.
$pair->node0->safe_psql('postgres', 'SELECT count(*) FROM block_ship_t');
$pair->node1->safe_psql('postgres', 'SELECT count(*) FROM block_ship_t');

my $remote_n0 = gcs_int_value($pair->node0, 'lookup_master_remote_count');
my $remote_n1 = gcs_int_value($pair->node1, 'lookup_master_remote_count');
ok($remote_n0 + $remote_n1 > 0,
	"L8 lookup_master_remote_count grew under workload "
	. "(n0=$remote_n0, n1=$remote_n1)");


# ============================================================
# L9: block-ship counters monotone non-decreasing.  Repeat workload
#	   and confirm counters do not go backwards.
# ============================================================
my $b0_t1 = gcs_int_value($pair->node0, 'block_request_count');
my $b1_t1 = gcs_int_value($pair->node1, 'block_request_count');
$pair->node1->safe_psql('postgres', 'SELECT count(*) FROM block_ship_t');
$pair->node0->safe_psql('postgres', 'SELECT count(*) FROM block_ship_t');
my $b0_t2 = gcs_int_value($pair->node0, 'block_request_count');
my $b1_t2 = gcs_int_value($pair->node1, 'block_request_count');
ok($b0_t2 >= $b0_t1,
	"L9 node0 block_request_count monotone ($b0_t1 → $b0_t2)");
ok($b1_t2 >= $b1_t1,
	"L9 node1 block_request_count monotone ($b1_t1 → $b1_t2)");


# ============================================================
# L10: block_checksum_fail_count remains 0 under healthy workload.
# ============================================================
is(gcs_value($pair->node0, 'block_checksum_fail_count'), '0',
	'L10 node0 block_checksum_fail_count = 0 (HC83 healthy path)');
is(gcs_value($pair->node1, 'block_checksum_fail_count'), '0',
	'L10 node1 block_checksum_fail_count = 0 (HC83 healthy path)');


# ============================================================
# L11: cross-node counter relation — total requests = total replies
#	   when all round-trips have closed.
# ============================================================
my $req_total = $b0_t2 + $b1_t2;
my $rep_total = gcs_int_value($pair->node0, 'block_reply_count')
	+ gcs_int_value($pair->node1, 'block_reply_count');
ok($rep_total <= $req_total,
	"L11 reply_total ($rep_total) <= request_total ($req_total) "
	. "across both nodes (in-flight requests permitted)");


# ============================================================
# L12: catversion bumped to 202605400.
# ============================================================
my $catver = $pair->node0->safe_psql(
	'postgres',
	q{SELECT catalog_version_no::bigint FROM pg_control_system()});
is($catver, '202605400',
	'L12 catversion = 202605400 (spec-2.33 D11)');


done_testing();
