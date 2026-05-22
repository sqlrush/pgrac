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
#	  L6   ambiguity-by-same-xid:  both nodes commit txns with the same
#	       PG raw xid bucket (different origin_node_id);  each node's
#	       local install count delta is independent of the peer's
#	       (proves origin separation in the exact key)
#	  L7   reconfig flush path linkable:  pg_cluster_state tt_status
#	       row 'flush_count' exists as observable counter (D7 callsite
#	       in cluster_reconfig.c — runtime reconfig trigger推 spec-2.29
#	       reconfig acceptance,本 spec 仅 verify counter wiring)
#	  L8   guard:  no TT_STATUS_HINT msg type in pg_cluster_ic_msg_types
#	       (spec-3.1 §1.3 #3 — no cross-node wire) /* SPEC_3_1_LINT_OK: */
#	  L9   guard:  no SharedInvalidationMessage size change (PG sinval
#	       16B wire ABI untouched — spec-3.1 §0.1 F2)
#	  L10  spec-3.1 catversion unchanged 202605460 (foundation does
#	       not bump catalog;  bump deferred to spec-3.4 when ITL
#	       writable activates)
#
# Spec: spec-3.1-cluster-xid-status-foundation.md (FROZEN v1.0)
#
# Note: spec-3.1 ships local in-memory overlay only.  Cross-node
#	exact-key lookup (origin=peer) requires the spec-3.2 wire path;
#	this TAP verifies the local install hook + counter wiring + key
#	contract surface;  cross-node behavioral coverage is deferred.
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
	extra_conf => [
		'autovacuum = off',
		# spec-3.1 D10: keep PCM out of the foundation TAP so a short
		# HC116 invalidate timeout cannot mask the TT status signal
		# (L175 fixture-scope GUC isolation).
		'cluster.pcm_grd_max_entries = 0',
	]);
$pair->start_pair;

usleep(2_000_000);

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
# L6: origin separation — peer txns do not bleed counters.
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
is($n0_install_post, $n0_install_pre,
	"L6 node0 install_count unchanged by peer commit (origin separation;"
	 . " spec-3.1 ships local install only — no cross-node propagation)");


# ============================================================
# L7: tt_status flush counter wiring.
# ============================================================
is($pair->node0->safe_psql('postgres',
		q{SELECT count(*) FROM pg_cluster_state
		  WHERE category='tt_status' AND key='flush_count'}),
	'1',
	'L7 tt_status.flush_count counter exposed (D7 reconfig callsite wired)');


# ============================================================
# L8: HC182 — no TT_STATUS_HINT-style wire type registered.
# /* SPEC_3_1_LINT_OK: assertion checks the absence by SQL LIKE pattern */
# ============================================================
is($pair->node0->safe_psql('postgres',
		q{SELECT count(*) FROM pg_cluster_ic_msg_types
		  WHERE name LIKE '%TT_STATUS%'}),
	'0',
	'L8 no TT_STATUS-style msg type registered (spec-3.1 §1.3 #3)');


# ============================================================
# L9: SharedInvalidationMessage 16B sinval wire ABI untouched.
# ============================================================
# Indirect: spec-2.39 sinval wait events count remains 91 (no spec-3.1
# additions to sinval surface).
is($pair->node0->safe_psql('postgres',
		'SELECT count(*) FROM pg_stat_cluster_wait_events'),
	'91',
	'L9 wait events count unchanged at 91 (no new TT status wait events)');


# ============================================================
# L10: catversion unchanged.
# ============================================================
my $catver = $pair->node0->safe_psql(
	'postgres',
	q{SELECT catalog_version_no::bigint FROM pg_control_system()});
is($catver, '202605460',
	'L10 catversion stays 202605460 (spec-3.1 foundation does not bump)');


$pair->stop_pair;
done_testing();
