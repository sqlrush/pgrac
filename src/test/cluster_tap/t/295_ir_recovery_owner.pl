#-------------------------------------------------------------------------
#
# 295_ir_recovery_owner.pl
#    spec-5.7 D8 (§3.4) -- MECHANISM-LEVEL proof of the IR (instance-recovery
#    owner) GES-enforced uniqueness gate.
#
#    What IR protects: when a node dies, the survivors online-recover its WAL
#    data + visibility (the spec-4.11 thread-recovery worker).  The destructive
#    part of that recovery (physical rollback / cleanup) is NOT idempotent, so it
#    must run on EXACTLY ONE survivor.  Today uniqueness rests on the local-view
#    min(survivor) coordinator + the per-episode epoch.  Two survivors whose
#    alive-set views DIVERGE could each believe they own the recovery and both
#    mutate the dead resource -> double destructive apply (Rule 8.A).  IR(X) adds
#    a GES-enforced layer: the recovery worker takes IR(X) on
#    (dead_node, episode_epoch) before any destructive apply, so the GES master
#    grants exactly one owner; a non-owner fails closed with 53RA9 and never
#    mutates.
#
#    What this test drives (REAL, not mocked): the REAL cluster_ir_recovery_acquire
#    GES path, via the cluster_ir_acquire_probe TEST SRF -- the same call the
#    recovery worker mutation gate makes.  A persistent session on node0 holds
#    IR(X) on (dead_node, epoch); a competing claim then proves:
#      L1 exactly one claimant obtains IR(X) ('owner');
#      L2 the loser fails closed at the mutation gate (53RA9), exactly as the
#         recovery worker does;
#      L3 a stale launch epoch is refused ('not_ready') -- the IR-M5 bootstrap gate
#         (the L235 superseded-epoch abort is independently pinned by t/265);
#      L4 once the owner releases, the lock is available again (it is a real lock,
#         not a one-shot).
#
#    The accepted cluster epoch starts at 0 (CLUSTER_EPOCH_INITIAL); IR-M5
#    correctly refuses to engage until a real reconfig bumps it.  This test bumps
#    it with the proven synthetic inject (cluster_reconfig_inject_dead_node_test,
#    as in t/267/t/271/t/272/t/274) on a node id that is NOT a real cluster member,
#    so the epoch advances without killing a real node -- letting two live nodes
#    compete for the recovery-owner lock.
#
#    RECORDED e2e GAP (mechanism-level, honest): this drives the real GES IR(X)
#    acquire with real competing claims, but it does NOT reproduce a TRUE reconfig
#    with divergent survivor alive-set views.  Online thread recovery is 2-node-
#    scoped (a single survivor self-masters the IR resid -> no divergence is
#    constructible in-scope); and the synthetic-epoch cluster is quiescent, so the
#    fresh-epoch freeze-gate bypass (cluster_lock_acquire.c, exercised only while
#    the IR resid's shard is REBUILDING under a live remaster) is not driven here.
#    A true >2-node divergent-owner reconfig e2e is forward work.
#
#    Harness: ClusterPair shared_data + 3 voting disks + autovacuum off.
#
# Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
# Portions Copyright (c) 2026, pgrac contributors
#
# Author: SqlRush <sqlrush@gmail.com>
#
# IDENTIFICATION
#    src/test/cluster_tap/t/295_ir_recovery_owner.pl
#
# NOTES
#    pgrac-original file.
#    Spec: spec-5.7-misc-enqueue-classes.md (D8, §3.4)
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../../perl";

use PostgreSQL::Test::ClusterPair;
use Test::More;
use Time::HiRes qw(usleep);

my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'ir_owner',
	quorum_voting_disks => 3,
	shared_data         => 1,
	extra_conf          => [
		'autovacuum = off',
		'cluster.grd_max_entries = 1024',
		'cluster.cssd_heartbeat_interval_ms = 2000',
		'cluster.cssd_dead_deadband_factor = 10',
	]);
$pair->start_pair;
usleep(3_000_000);

my $n0 = $pair->node0;
my $n1 = $pair->node1;

# A synthetic dead-node id that is NOT one of the two real members (0, 1): the
# epoch advances but neither live node is actually killed, so both stay up to
# compete for the recovery-owner lock.
my $SYNTH_DEAD = 5;

# The (synthetic) dead node whose recovery ownership is contended in this test.
my $DEAD = 99;

# ----------
# Step 1: bump the accepted cluster epoch to non-zero on node0 (IR-M5 refuses to
# engage at epoch 0).  apply_epoch_bump_as_coordinator publishes the reconfig
# event, which node1 observes -- so node1 converges to the same epoch.
# ----------
is($n0->safe_psql('postgres', "SELECT cluster_reconfig_inject_dead_node_test($SYNTH_DEAD)"),
	't', 'epoch bumped on node0 via synthetic inject (non-member node id)');

# Poll node1 until its accepted epoch has converged: once non-zero AND matching
# node0's, a probe on a throwaway dead-node id returns a definitive verdict
# ('owner'/'native') rather than 'not_ready' (epoch 0 / mismatch).  Bounded wait.
my $converged = 0;
for (my $i = 0; $i < 30; $i++)
{
	my $v = $n1->safe_psql('postgres',
		"SELECT cluster_ir_acquire_probe($SYNTH_DEAD, 0, false)");
	if ($v ne 'not_ready')
	{
		$converged = 1;
		last;
	}
	usleep(1_000_000);
}
ok($converged, 'node1 converged to the bumped accepted epoch (cross-node)');

# ----------
# L1: node0 takes IR(X) on (DEAD, epoch) in a PERSISTENT session and HOLDS it
# (the GES holder survives transaction end -- IR has no PG-native lock for the
# LockReleaseAll hook to cover, mirroring DL).  Exactly one owner.
# ----------
my $hold0 = $n0->background_psql('postgres');
my $owner = $hold0->query("SELECT cluster_ir_acquire_probe($DEAD, 0, true)");
like($owner, qr/owner/,
	'L1 node0 obtains the IR(X) recovery-owner lock (exactly one owner)');

# ----------
# L2: node1 competes for the SAME (DEAD, epoch).  It is NOT the owner -> the
# mutation gate fails closed with 53RA9, exactly as the recovery worker does.
# ----------
my ($rc, $out, $err) = $n1->psql('postgres',
	"SELECT cluster_ir_acquire_probe($DEAD, 0, false)", timeout => 60);
isnt($rc, 0, 'L2 node1 competing claim is rejected (non-owner fails closed)');
like($err, qr/53RA9|recovery_owner_conflict|not the instance-recovery owner/i,
	'L2 node1 non-owner failure is the 53RA9 recovery-owner conflict (8.A)');

# ----------
# L3: a stale launch epoch (epoch_offset != 0 -> episode_epoch != the accepted
# epoch) is refused by the IR-M5 bootstrap gate -> 'not_ready', never an owner.
# (The runtime L235 superseded-epoch abort is independently pinned by t/265.)
# ----------
is($n1->safe_psql('postgres', "SELECT cluster_ir_acquire_probe($DEAD, 1000000, false)"),
	'not_ready',
	'L3 stale launch epoch is refused (IR-M5 bootstrap gate)');

# ----------
# L4: the owner releases -> the lock is available again (a real lock, not a
# one-shot).  node1 then wins it.
# ----------
is($hold0->query("SELECT cluster_ir_release_probe()"), 't',
	'L4 node0 releases the held IR(X) recovery-owner lock');
$hold0->quit;

# Give the release a moment to propagate to the master before node1 re-claims.
my $reclaimed = 0;
for (my $i = 0; $i < 15; $i++)
{
	my ($r2, $o2, $e2) =
	  $n1->psql('postgres', "SELECT cluster_ir_acquire_probe($DEAD, 0, false)", timeout => 60);
	if ($r2 == 0 && $o2 =~ /owner/)
	{
		$reclaimed = 1;
		last;
	}
	usleep(1_000_000);
}
ok($reclaimed, 'L4 after release node1 can claim IR(X) -- the lock is real and reusable');

# ----------
# L5: observability -- node0 recorded an owner grant; node1 recorded a 53RA9
# conflict.  Counters are per-node (shmem-local).
# ----------
my $owner_n0 = $n0->safe_psql('postgres',
	q{SELECT value::int FROM pg_cluster_state WHERE category='ir' AND key='owner_count'});
ok($owner_n0 >= 1, "L5 node0 ir.owner_count = $owner_n0 (>= 1 IR(X) grant recorded)");

my $conflict_n1 = $n1->safe_psql('postgres',
	q{SELECT value::int FROM pg_cluster_state WHERE category='ir' AND key='conflict_count'});
ok($conflict_n1 >= 1,
	"L5 node1 ir.conflict_count = $conflict_n1 (>= 1 53RA9 non-owner fail-closed recorded)");

$pair->stop_pair;
done_testing();
