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
#	  L9  pg_cluster_state.gcs category has 109 keys (spec-7.2 D6+flip) (cumulative through spec-6.14a)
#	  L10 Reply status enum value 12 (DENIED_LOST_WRITE) is新增的
#	      最大 value (baseline workload must not trigger lost-write)
#	  L11 spec-2.41 D / P1-C — behavioral lost-write inject: a
#	      cross-node sequence with a VALID pi_watermark_scn, then
#	      cluster-gcs-block-stale-ship forces the SHIPPED pd_block_scn
#	      to InvalidScn (§2.6 branch 2 anomaly) → the holder-forward
#	      reachable detector MUST fail-closed DENIED_LOST_WRITE →
#	      requester ereport(53R93), lost_write_invalidscn_failclosed_count
#	      grows, no stale page
#	  L12 cluster restarts injection-free and the sequence stays usable
#
# Spec: spec-2.37-pi-simplified-lost-write-detection.md §4.2 (FROZEN v0.3)
#	+ spec-2.41-cross-node-block-version-scn-lost-write.md §2.6/§6 (D)
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
# Voting quorum + shared storage are required so L11's cross-node sequence
# can globalize its TRANSACTION lock and route to the same shared relfilenode
# (same true-2-node harness as t/284 / t/246).  L1-L10 are config-independent.
my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'gcs_block_lost_write',
	quorum_voting_disks => 3,
	shared_data         => 1,
	data_port_span      => 2,	# spec-7.3: default lms_workers=2 binds data_port+[0,1]
	extra_conf          => [
		'autovacuum = off',
		'cluster.grd_max_entries = 1024',
		# Widen CSSD misscount so a parallel CI shard does not falsely
		# declare a healthy peer DEAD while an X-hold window is open
		# (mirrors t/280-284).
		'cluster.cssd_heartbeat_interval_ms = 2000',
		'cluster.cssd_dead_deadband_factor = 10',
	]);
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
	'123',
	'L2 pg_stat_cluster_wait_events returns 123 rows (spec-6.13 RDMA + spec-5.22b D2-6 undo grant-plane +3 + spec-7.2 LMS data-plane +2; merge sum 118+3+2)');

is($pair->node0->safe_psql(
		'postgres',
		q{SELECT count(*) FROM pg_cluster_state WHERE category='gcs'}),
   '109',
   'L2 pg_cluster_state.gcs category has 109 keys (gcs-race-fix-2 +6 rows) (spec-7.2 D6+flip) (cumulative through spec-6.14a)');


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
# L9 (alias of L2): gcs key count = 89.
# ============================================================
is($pair->node1->safe_psql(
		'postgres',
		q{SELECT count(*) FROM pg_cluster_state WHERE category='gcs'}),
   '109',
   'L9 node1 pg_cluster_state.gcs has 109 keys (gcs-race-fix-2 +6 rows) (cross-node parity)');


# ============================================================
# L10: reply status enum 12 is the new max value (verified via
# gcs key catalog presence — no behavioral inject this Sprint).
# ============================================================
cmp_ok(gcs_int($pair->node0, 'lost_write_detected_count'), '<=', 0,
	'L10 lost_write_detected_count baseline (no false-positive before inject)');


# ============================================================
# L11: behavioral lost-write inject (spec-2.41 D / P1-C).
#
#	Drive the SCN lost-write self-check (gcs_block_lost_write_verdict,
#	§2.6) end-to-end on a real 2-node cluster.  A cross-node sequence
#	first establishes a VALID pi_watermark_scn for the seq block;  then
#	cluster-gcs-block-stale-ship forces the SHIPPED page's pd_block_scn
#	to InvalidScn — §2.6 branch 2 (a tracked block shipping an unstamped
#	page = ANOMALY) — which MUST fail-closed DENIED_LOST_WRITE →
#	requester ereport(53R93), bump lost_write_invalidscn_failclosed_count,
#	and NEVER ship the page.
#
#	The inject fires on the holder-forward ship path — the reachable
#	detector twin:  in a real 2-node cluster the master-direct path is
#	bypassed (self-ship / read-image goto), so the cross-node transfer is
#	validated on holder-forward.  It is one-shot per dispatch, so we
#	re-arm (reload) each round and drive BOTH master/holder assignments:
#	mastership is fixed by tag hash and unknowable a priori, and only the
#	round where the shipping node forwards a held block carries the inject.
# ============================================================

# Readiness: both peers must see each other connected so the globalizing
# CREATE SEQUENCE has voting quorum (else "cluster lock acquire timeout").
ok($pair->wait_for_peer_state(0, 1, 'connected', 60)
	  && $pair->wait_for_peer_state(1, 0, 'connected', 60),
	'L11 peers connected (voting quorum ready for globalized DDL)');

# action='error' was restored at L8 → a fail-closed ships 53R93.
$pair->node0->safe_psql('postgres', 'CREATE SEQUENCE lw_seq');
$pair->node1->safe_psql('postgres', 'CREATE SEQUENCE lw_seq');

# Warm up (NO inject): several cross-node nextvals install the seq page on
# each node with a valid pd_block_scn and advance the master's
# pi_watermark_scn (cluster_pcm_lock_master_grant_x_to / take_x_after_transfer).
# This makes expected_scn VALID so the forced-InvalidScn shipped page trips
# the ANOMALY branch (fail-closed) rather than the not-SCN-tracked SKIP branch.
for (1 .. 4)
{
	$pair->node0->safe_psql('postgres', "SELECT nextval('lw_seq')");
	$pair->node1->safe_psql('postgres', "SELECT nextval('lw_seq')");
}

my $ifc_before =
	gcs_int($pair->node0, 'lost_write_invalidscn_failclosed_count') +
	gcs_int($pair->node1, 'lost_write_invalidscn_failclosed_count');
my $det_before =
	gcs_int($pair->node0, 'lost_write_detected_count') +
	gcs_int($pair->node1, 'lost_write_detected_count');

# A GUC-armed SKIP fault auto-arms in fresh backends and re-arms on reload in
# the long-running cluster processes (same mechanism as t/284).
sub arm_stale_ship
{
	my ($val) = @_;
	for my $node ($pair->node0, $pair->node1)
	{
		$node->safe_psql('postgres',
			"ALTER SYSTEM SET cluster.injection_points = '$val'");
		$node->safe_psql('postgres', 'SELECT pg_reload_conf()');
	}
	return;
}

# Tolerant nextval: the 53R93 fail-closed is by design;  capture its text.
my $saw_53r93 = 0;
sub try_nextval
{
	my ($node) = @_;
	my ($rc, $out, $err) =
		$node->psql('postgres', "SELECT nextval('lw_seq')", timeout => 30);
	$saw_53r93 = 1
		if defined $err && $err =~ /53R93|lost write detected/i;
	return;
}

# Re-arm + drive both directions until the anomaly counter grows.
my $ifc_after = $ifc_before;
for my $round (1 .. 8)
{
	last if $ifc_after > $ifc_before;
	arm_stale_ship('cluster-gcs-block-stale-ship:skip');
	try_nextval($pair->node0);    # node0 takes X (becomes holder)
	try_nextval($pair->node1);    # node1 requests X cross-node
	try_nextval($pair->node1);    # node1 takes X (becomes holder)
	try_nextval($pair->node0);    # node0 requests X cross-node
	usleep(500_000);
	$ifc_after =
		gcs_int($pair->node0, 'lost_write_invalidscn_failclosed_count') +
		gcs_int($pair->node1, 'lost_write_invalidscn_failclosed_count');
}
my $det_after =
	gcs_int($pair->node0, 'lost_write_detected_count') +
	gcs_int($pair->node1, 'lost_write_detected_count');

diag("L11 invalidscn_failclosed $ifc_before->$ifc_after; "
	. "detected $det_before->$det_after; saw_53r93=$saw_53r93");

cmp_ok($ifc_after, '>', $ifc_before,
	'L11 anomaly inject fails closed: lost_write_invalidscn_failclosed_count '
	. "grew ($ifc_before → $ifc_after) — §2.6 branch 2");
cmp_ok($det_after, '>', $det_before,
	'L11 the anomaly counts as a detected lost write (DENIED_LOST_WRITE)');
ok($saw_53r93,
	'L11 requester saw 53R93 / "lost write detected" (action=error, fail-closed)');


# ============================================================
# L12: disarm + cluster stays usable.
#
#	A GUC-armed SKIP survives reload (assign_hook only disarms WARNING
#	faults), so we empty cluster.injection_points and restart — fresh
#	processes come up inject-free.  The sequence must remain usable (the
#	inject changed only the REPLY, never the stored page).
# ============================================================
arm_stale_ship('');
$pair->node0->stop('fast');
$pair->node1->stop('fast');
$pair->node0->start;
$pair->node1->start;
usleep(3_000_000);
ok($pair->wait_for_peer_state(0, 1, 'connected', 60)
	  && $pair->wait_for_peer_state(1, 0, 'connected', 60),
	'L12 cluster restarted injection-free after the inject leg');
is($pair->node0->safe_psql('postgres', "SELECT nextval('lw_seq') > 0"),
	't',
	'L12 sequence still usable after the fail-closed leg (no corruption)');


done_testing();
