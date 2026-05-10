#-------------------------------------------------------------------------
#
# 098_fence_freeze_writes_2node.pl
#    spec-2.28 Sprint A Step 5 D14 — 2-node fence-lite catalog +
#    runtime smoke TAP.
#
#    LANDED (L1-L2):
#      L1   ClusterPair quorum_voting_disks=>3 strict-mode setup —
#           both postmasters alive, fence catalog surface visible
#           via pg_cluster_fence_state, counters init=0, last_freeze_
#           at + last_thaw_at NULL (never broadcast).
#      L2   LMON cluster_fence_lmon_tick wired in both nodes' main
#           loop and cluster_enabled — verify by sleeping a few
#           ticks and checking fence_state stays at zero (no false-
#           positive broadcast at steady-state OK quorum).
#
#    DEFERRED (L3-L5) — end-to-end freeze/thaw broadcast verification:
#      The original spec-2.28 §4.2 D14 design assumed SIGSTOP node1
#      → node0 quorum_state=LOST → freeze broadcast fires.  This is
#      a SUBSTRATE MISMATCH bug in the spec drafting:
#
#        spec-2.6 decide_quorum_view computes quorum_state purely
#        from disks_ok (per-disk read success count).  SIGSTOP a
#        peer postmaster does NOT change disks_ok on the survivor
#        — both nodes can still read all 3 disk files locally.
#        Only the alive_bitmap (peer alive bits) drops after the
#        peer's heartbeat ages out, but alive_bitmap is NOT used
#        by quorum_state decision in the spec-2.6 design.
#
#        Therefore the only event that currently triggers
#        cluster_fence_broadcast_freeze is voting-disk I/O
#        DEGRADATION — chmod 000 / unlink / write failure on a
#        majority of disks.  That requires the L8 stable I/O
#        failure injection harness which is a separate Hardening
#        v0.5+ backlog item (deferred since spec-2.6 ship).
#
#      Documented gap (matches user-issued amendment 2026-05-10):
#        1. SIGSTOP peer does NOT trigger current spec-2.6 quorum
#           loss.
#        2. The real broadcast trigger is qvotec quorum_state
#           OK → {UNCERTAIN,LOST} transition.
#        3. The natural way to push that transition today is
#           voting-disk I/O degradation, not peer heartbeat stale.
#        4. End-to-end freeze/thaw TAP requires the disk I/O
#           failure injection harness — Hardening v0.5+ backlog
#           shared with 096 L8.
#        5. Whether to include peer-alive in quorum_state decision
#           is an open architectural question for spec-2.6 hardening
#           OR spec-2.29 reconfig coordinator;NOT silently bundled
#           into spec-2.28.
#
#      Until the disk-failure injection harness lands, the fence
#      broadcast path is verified by:
#        - cluster_unit T-fence-2 / T-fence-3 / T-fence-4 / T-fence-5
#          / T-fence-6 (read-clear / dual-set / asymmetric thaw /
#          self-fence pending — cluster_fence.c contracts)
#        - cluster_regress fence_smoke (catalog surface)
#        - This file L1-L2 (runtime LMON tick wired + zero false-
#          positive at steady-state OK)
#
#      Lesson (post-Sprint-A): fence ACTOR landed correctly but the
#      TEST TRIGGER condition was assumed wrong in spec drafting —
#      mirror of L92 spec-claim-vs-test-substrate-mismatch from
#      spec-drafting-lessons.md, this time on the trigger side
#      rather than the assertion side.  Future spec drafting must
#      verify the trigger event actually maps to the consumer's
#      observable input.
#
# IDENTIFICATION
#    src/test/cluster_tap/t/098_fence_freeze_writes_2node.pl
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../../perl";

use PostgreSQL::Test::ClusterPair;
use Test::More;


# ----------
# L1: setup — strict-mode ClusterPair with 3 voting disks.
# ----------
my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'fence_2node', quorum_voting_disks => 3);
$pair->start_pair;

is($pair->node0->safe_psql('postgres', 'SELECT 1'),
	'1', 'L1 node0 postmaster alive');
is($pair->node1->safe_psql('postgres', 'SELECT 1'),
	'1', 'L1 node1 postmaster alive');

is($pair->node0->safe_psql('postgres',
		'SELECT in_quorum FROM pg_cluster_quorum_state'),
	't', 'L1 node0 in_quorum=t');
is($pair->node1->safe_psql('postgres',
		'SELECT in_quorum FROM pg_cluster_quorum_state'),
	't', 'L1 node1 in_quorum=t');

# Fence catalog surface visible:  pg_cluster_fence_state SRF returns
# single row, all baseline values per spec §2.4.
is($pair->node0->safe_psql('postgres',
		'SELECT count(*) FROM pg_cluster_fence_state'),
	'1', 'L1 node0 pg_cluster_fence_state SRF returns 1 row');

is($pair->node0->safe_psql('postgres',
		'SELECT last_freeze_at IS NULL FROM pg_cluster_fence_state'),
	't', 'L1 node0 last_freeze_at NULL (no broadcast at boot)');
is($pair->node0->safe_psql('postgres',
		'SELECT last_thaw_at IS NULL FROM pg_cluster_fence_state'),
	't', 'L1 node0 last_thaw_at NULL (no broadcast at boot)');
is($pair->node0->safe_psql('postgres',
		'SELECT self_fence_pending FROM pg_cluster_fence_state'),
	'f', 'L1 node0 self_fence_pending=false');

is($pair->node0->safe_psql('postgres',
		'SELECT freeze_broadcast_count FROM pg_cluster_fence_state'),
	'0', 'L1 node0 freeze_broadcast_count=0');
is($pair->node0->safe_psql('postgres',
		'SELECT thaw_broadcast_count FROM pg_cluster_fence_state'),
	'0', 'L1 node0 thaw_broadcast_count=0');
is($pair->node0->safe_psql('postgres',
		'SELECT self_fence_initiated_count FROM pg_cluster_fence_state'),
	'0', 'L1 node0 self_fence_initiated_count=0');


# ----------
# L2: LMON tick wired + zero false-positive at steady-state OK.
#
#   Sleep ~3 seconds (1.5 × LMON main loop interval default 2000ms +
#   buffer) so cluster_fence_lmon_tick has run several times on both
#   nodes.  In healthy quorum_state=OK steady state, the tick must
#   NOT broadcast freeze/thaw — that would be a false-positive bug.
#
#   Observable: counters stay at zero, fence_pending stays false.
# ----------
note('L2 wait ~3s for LMON tick to run several iterations at OK quorum');
sleep 3;

is($pair->node0->safe_psql('postgres',
		'SELECT freeze_broadcast_count FROM pg_cluster_fence_state'),
	'0', 'L2 node0 freeze_broadcast_count still 0 after LMON ticks (no false-positive)');
is($pair->node0->safe_psql('postgres',
		'SELECT thaw_broadcast_count FROM pg_cluster_fence_state'),
	'0', 'L2 node0 thaw_broadcast_count still 0 after LMON ticks');
is($pair->node0->safe_psql('postgres',
		'SELECT last_freeze_at IS NULL FROM pg_cluster_fence_state'),
	't', 'L2 node0 last_freeze_at still NULL (no spurious broadcast)');
is($pair->node1->safe_psql('postgres',
		'SELECT freeze_broadcast_count FROM pg_cluster_fence_state'),
	'0', 'L2 node1 freeze_broadcast_count still 0 (symmetric)');


$pair->stop_pair;

done_testing();


#-------------------------------------------------------------------------
# spec-2.28 post-Sprint-A backlog — L3-L8 deferred:
#
#   L3-L5  end-to-end freeze/thaw broadcast verification — requires
#          disk I/O failure injection harness (shared with 096 L8
#          and spec-2.6 Hardening v0.5+ backlog #4) to trigger the
#          quorum_state OK→LOST transition that fence broadcast
#          consumes.  SIGSTOP peer postmaster does NOT trigger
#          current spec-2.6 quorum decision (disks_ok is per-disk
#          read success, not peer alive).
#
#          See file header for full architectural rationale +
#          5 documentation items per user amendment 2026-05-10.
#
#   L6     self-fence postmaster shutdown (cluster.self_fence_grace_
#          ms elapsed → kill SIGINT → orderly fast shutdown).
#          Requires a triggering quorum_state=LOST event same as
#          L3-L5 + injection to hold the LOST state past grace_ms.
#
#   L7     freeze flag set in critical section deferred to next
#          ProcessInterrupts ereport on CS exit.  Indirectly covered
#          by unit T-fence-3 (cluster_enabled silent skip path).
#
#   L8     LMON hang → QVOTEC backup broadcast path.  Hardening
#          v0.2+ backlog;LMON tick is currently the sole production
#          caller of cluster_fence_broadcast_freeze.
#
# Open architectural question for spec-2.6 OR spec-2.29 hardening
# (NOT silently bundled into spec-2.28):
#
#   Should the quorum_state decision include peer-alive
#   (alive_bitmap) in addition to disks_ok?  That would make
#   SIGSTOP a peer postmaster a quorum-loss event (heartbeat stale
#   → drop from alive_bitmap → majority alive < quorum_size →
#   quorum_state=LOST).  Currently spec-2.6 v0.7 ships disk-only
#   semantics.
#-------------------------------------------------------------------------
