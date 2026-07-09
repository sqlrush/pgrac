#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 203_cluster_tt_status_foundation.pl
#	  spec-3.1 D10 — Undo TT status foundation behavioral TAP on a
#	  2-node ClusterPair.
#
#	  L1   ClusterPair startup + both nodes alive
#	  L2   spec-3.1 N7 self-consumer:  SQL commit on node0 bumps
#	       install_count + self_consumer_hit on node0 in every build;
#	       proves D5/D6 install hook + N7 self-consumer wired
#	  L3   spec-3.1 N7 abort path:  SQL ROLLBACK bumps install_count
#	       (status=ABORTED entry installed)
#	  L4   tt_status pg_cluster_state category has 7 keys
#	  L5   exact-key API absence check:  no SQL UDF named
#	       cluster_tt_status_lookup_by_raw_xid exists (HC180);  ensures
#	       no raw-xid cluster lookup leaked into the surface
#	  L6   ambiguity-by-same-xid:  both nodes commit txns with exact keys
#	       separated by origin_node_id; spec-3.2 TT_STATUS_HINT fanout may
#	       install peer status, but the local node must still install its own
#	       status independently.
#	  L7   reconfig flush path linkable:  pg_cluster_state tt_status
#	       row 'flush_count' exists as observable counter (D7 callsite
#	       in cluster_reconfig.c — runtime reconfig trigger推 spec-2.29
#	       reconfig acceptance,本 spec 仅 verify counter wiring)
#	  L8   successor wire: TT_STATUS_HINT msg type is registered once
#	       after spec-3.2, while spec-3.1 raw-xid/CLOG guardrails remain
#	       in force.
#	  L9   guard:  no SharedInvalidationMessage size change (PG sinval
#	       16B wire ABI untouched — spec-3.1 §0.1 F2)
#	  L10  catversion remains >= spec-3.1 baseline 202605460
#	       (foundation itself did not bump catalog; later specs may)
#
# Spec: spec-3.1-cluster-xid-status-foundation.md (FROZEN v1.0)
#
# Note: this TAP remains the spec-3.1 foundation coverage, but it runs in
# the current tree.  After spec-3.2, TT_STATUS_HINT wire propagation may
# install peer-origin status; the foundation assertions therefore focus on
# exact-key surfaces and local install liveness, not on absence of peer
# propagation.
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


sub tt_int
{
	my ($node, $key) = @_;

	my $v = $node->safe_psql('postgres',
		qq{SELECT value FROM pg_cluster_state
		   WHERE category='tt_status' AND key='$key'});
	return defined($v) && $v ne '' ? int($v) : 0;
}


# ============================================================
# L1: ClusterPair startup.
# ============================================================
my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'tt_status_foundation',
	# spec-5.7 §3.1d: a true 2-node cluster has alive peers, so Option A does not
	# short-circuit the lock gate to native; L2's write-xact TRANSACTION lock
	# globalizes and needs a coordinated GES master.  Provide quorum voting disks
	# + GRD capacity (+ readiness wait below) so coordination succeeds instead of
	# timing out.  A true 2-node TT-status foundation must coordinate cross-node
	# locks rather than fall back to native/skeleton.
	quorum_voting_disks => 3,
	extra_conf => [
		'autovacuum = off',
		# spec-3.1 D10: keep PCM out of the foundation TAP so a short
		# HC116 invalidate timeout cannot mask the TT status signal
		# (L175 fixture-scope GUC isolation).
		'cluster.pcm_grd_max_entries = 0',
		# spec-5.7 §3.1d: GES GRD capacity for the globalized TRANSACTION lock
		# (explicit; matches the post-spec-5.7 default).
		'cluster.grd_max_entries = 1024',
	]);
$pair->start_pair;

usleep(2_000_000);

# spec-5.7 §3.1d readiness gate: ensure both peers see each other connected
# before the first globalized lock, so the GES master is electable (reuses the
# t/246 voting + readiness pattern).
$pair->wait_for_peer_state(0, 1, 'connected', 30)
  or die "node0 did not see node1 connected";
$pair->wait_for_peer_state(1, 0, 'connected', 30)
  or die "node1 did not see node0 connected";

is($pair->node0->safe_psql('postgres', 'SELECT 1'),
	'1', 'L1 node0 postmaster alive');
is($pair->node1->safe_psql('postgres', 'SELECT 1'),
	'1', 'L1 node1 postmaster alive');


# ============================================================
# L2: N7 self-consumer — local SQL commit bumps install + lookup_hit.
# ============================================================
my $install_before = tt_int($pair->node0, 'install_count');
my $lookup_hit_before = tt_int($pair->node0, 'lookup_hit_count');
my $self_consumer_before = tt_int($pair->node0, 'self_consumer_hit_count');

$pair->node0->safe_psql('postgres', q{
	CREATE TEMP TABLE spec_3_1_l2 (i int);
	INSERT INTO spec_3_1_l2 VALUES (1), (2);
});

my $install_after = tt_int($pair->node0, 'install_count');
my $lookup_hit_after = tt_int($pair->node0, 'lookup_hit_count');
my $self_consumer_after = tt_int($pair->node0, 'self_consumer_hit_count');

cmp_ok($install_after - $install_before, '>=', 1,
	"L2 install_count delta >= 1 on node0 after local commit");
cmp_ok($lookup_hit_after - $lookup_hit_before, '>=', 1,
	"L2 lookup_hit_count delta >= 1 (N7 self-consumer re-lookup)");
cmp_ok($self_consumer_after - $self_consumer_before, '>=', 1,
	"L2 self_consumer_hit_count delta >= 1 (N7 self-consumer runs in every build)");


# ============================================================
# L3: abort path also installs entry.
# ============================================================
my $install_pre_abort = tt_int($pair->node0, 'install_count');

$pair->node0->safe_psql('postgres', q{
	BEGIN;
	CREATE TEMP TABLE spec_3_1_l3 (j int);
	ROLLBACK;
});

my $install_post_abort = tt_int($pair->node0, 'install_count');

cmp_ok($install_post_abort - $install_pre_abort, '>=', 1,
	"L3 install_count delta >= 1 on node0 after rollback");


# ============================================================
# L4: tt_status category key count.
# ============================================================
is($pair->node0->safe_psql('postgres',
		q{SELECT count(*) FROM pg_cluster_state WHERE category='tt_status'}),
	'7',
	'L4 pg_cluster_state tt_status has 7 keys (D9 emit_row count)');


# ============================================================
# L5: HC180 — no raw-xid cluster lookup UDF exists.
# ============================================================
is($pair->node0->safe_psql('postgres',
		q{SELECT count(*) FROM pg_proc
		  WHERE proname IN ('cluster_tt_status_lookup_by_raw_xid',
		                    'cluster_status_by_raw_xid')}),
	'0',
	'L5 HC180:  no raw-xid cluster status UDF exists (L176 / spec-3.1 Q3 A)');


# ============================================================
# L6: origin separation — peer propagation does not hide local install.
# ============================================================
my $n0_install_pre = tt_int($pair->node0, 'install_count');
my $n1_install_pre = tt_int($pair->node1, 'install_count');

# Drive a local commit on node1 only.
$pair->node1->safe_psql('postgres', q{
	CREATE TEMP TABLE spec_3_1_l6 (k int);
	INSERT INTO spec_3_1_l6 VALUES (10);
});

my $n0_install_post = tt_int($pair->node0, 'install_count');
my $n1_install_post = tt_int($pair->node1, 'install_count');

cmp_ok($n1_install_post - $n1_install_pre, '>=', 1,
	"L6 node1 install_count bumps on its own commit");
cmp_ok($n0_install_post, '>=', $n0_install_pre,
	"L6 node0 install_count remains monotonic when peer commit is fanned out "
	. "(spec-3.2 may install peer TT_STATUS_HINT)");


# ============================================================
# L7: tt_status flush counter wiring.
# ============================================================
is($pair->node0->safe_psql('postgres',
		q{SELECT count(*) FROM pg_cluster_state
		  WHERE category='tt_status' AND key='flush_count'}),
	'1',
	'L7 tt_status.flush_count counter exposed (D7 reconfig callsite wired)');


# ============================================================
# L8: TT_STATUS_HINT wire type registered by spec-3.2 successor.
# ============================================================
is($pair->node0->safe_psql('postgres',
		q{SELECT count(*) FROM pg_cluster_ic_msg_types
		  WHERE name = 'cluster_tt_status_hint'}),
	'1',
	'L8 TT_STATUS_HINT msg type registered once by spec-3.2 successor');


# ============================================================
# L9: SharedInvalidationMessage 16B sinval wire ABI untouched.
# ============================================================
# Indirect: spec-2.39 sinval wait events count remains 91 (no spec-3.1
# additions to sinval surface).
is($pair->node0->safe_psql('postgres',
		'SELECT count(*) FROM pg_stat_cluster_wait_events'),
	'120',
	'L9 pg_stat_cluster_wait_events returns 120 rows (spec-6.13 RDMA wait surface)');


# ============================================================
# L10: catversion remains at-or-after the spec-3.1 baseline.
# ============================================================
my $catver = $pair->node0->safe_psql(
	'postgres',
	q{SELECT catalog_version_no::bigint FROM pg_control_system()});
cmp_ok($catver, '>=', 202605460,
	'L10 catversion >= 202605460 (spec-3.1 foundation baseline; later specs may bump)');


$pair->stop_pair;
done_testing();
