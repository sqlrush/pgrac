#-------------------------------------------------------------------------
#
# 096_quorum_2node_round_trip.pl
#    spec-2.6 D20 — 2-node ClusterPair quorum runtime round-trip TAP.
#
#    Hardening v0.5 partial: L1-L3 happy path landed.  L4-L8 deferred
#    to Hardening v0.5+ backlog (see comment block at end of file).
#
#    L1   ClusterPair quorum_voting_disks=>3 strict-mode start_pair —
#         both postmasters alive, share the same 3 voting-disk files.
#    L2   Both nodes parsed cluster.voting_disks GUC symmetrically —
#         pg_cluster_voting_disks reports 3 rows on each side.  This
#         only proves the GUC string was parsed identically by both
#         postmasters; it does NOT prove either node successfully wrote
#         its self-slot to a given disk (per-disk health surfacing
#         requires D15 SRF state="unknown" placeholder to be replaced —
#         spec-2.6 Hardening v0.5+ backlog #3).  The implicit proof of
#         symmetric write success comes from L3 (in_quorum=t requires
#         disks_ok >= ceil(N/2)+1 from each side's read view).
#    L3   Quorum view converges on both nodes — in_quorum=t,
#         disks_ok=3, disks_total=3 from each postmaster's view.
#
#    Why this is the minimum useful 2-node runtime coverage:
#      * Single-node strict-mode is already covered by 097 L2
#        (PgracClusterNode multi-node pgrac.conf + 1 real
#        postmaster, in_quorum=t + disks_ok=3 within 3s).
#      * What 097 cannot exercise is two postmasters writing to
#        the same disk slots concurrently on each poll cycle.
#        With Q4 v0.2 lease semantics + Q5 v0.2 fail-closed
#        commit-boundary check, the read-decide-write ordering
#        from cluster_qvotec.c is exercised under real concurrent
#        I/O for the first time here.
#      * No SIGSTOP / SIGCONT / collision injection / disk-fail
#        injection — these depend on capabilities not yet shipped
#        (boot-time epoch recovery, ProcSignal freeze/thaw
#        broadcast, stable I/O failure injection harness).
#
# IDENTIFICATION
#    src/test/cluster_tap/t/096_quorum_2node_round_trip.pl
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


# spec-2.6 strict-mode harness — pre-allocates 3 shared voting disks,
# sets allow_single_node=off + cluster.voting_disks CSV on both nodes.
my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'qvotec_2node', quorum_voting_disks => 3);
$pair->start_pair;

is($pair->node0->safe_psql('postgres', 'SELECT 1'),
	'1', 'L1 node0 postmaster accepting queries after strict-mode start_pair');
is($pair->node1->safe_psql('postgres', 'SELECT 1'),
	'1', 'L1 node1 postmaster accepting queries after strict-mode start_pair');

my @disks = $pair->voting_disk_paths;
is(scalar @disks, 3, 'L1 ClusterPair pre-allocated 3 voting-disk files');
ok(-f $disks[0] && -f $disks[1] && -f $disks[2],
	'L1 all 3 voting-disk files exist on shared filesystem');


# Give qvotec on each node at least 2 poll cycles
# (cluster.quorum_poll_interval_ms default = 2000ms).
sleep 5;


# ============================================================
# L2: both nodes parsed cluster.voting_disks GUC symmetrically
# ============================================================
# Each side queries pg_cluster_voting_disks (its own GUC parse) and
# reports 3 disk rows.  This only proves ClusterPair wrote the same
# CSV into both postgresql.conf and both postmasters parsed it; it
# does NOT prove either node successfully wrote its self-slot to a
# given disk.  Per-disk health (D15) is a Hardening v0.5+ backlog
# item — until then, slot-write proof is implicit via L3 quorum
# convergence.
my $disks0 = $pair->node0->safe_psql(
	'postgres', 'SELECT count(*) FROM pg_cluster_voting_disks');
is($disks0, '3', 'L2 node0 parsed 3 voting-disk paths from GUC');

my $disks1 = $pair->node1->safe_psql(
	'postgres', 'SELECT count(*) FROM pg_cluster_voting_disks');
is($disks1, '3', 'L2 node1 parsed 3 voting-disk paths from GUC');


# ============================================================
# L3: in_quorum + disks_ok=3 from both postmasters
# ============================================================
my $in_quorum_0 = $pair->node0->safe_psql(
	'postgres', 'SELECT in_quorum FROM pg_cluster_quorum_state');
is($in_quorum_0, 't', 'L3 node0 in_quorum=true after poll cycles');

my $in_quorum_1 = $pair->node1->safe_psql(
	'postgres', 'SELECT in_quorum FROM pg_cluster_quorum_state');
is($in_quorum_1, 't', 'L3 node1 in_quorum=true after poll cycles');

my $disks_ok_0 = $pair->node0->safe_psql(
	'postgres', 'SELECT disks_ok FROM pg_cluster_quorum_state');
is($disks_ok_0, '3', 'L3 node0 disks_ok=3 (all three reachable)');

my $disks_ok_1 = $pair->node1->safe_psql(
	'postgres', 'SELECT disks_ok FROM pg_cluster_quorum_state');
is($disks_ok_1, '3', 'L3 node1 disks_ok=3 (all three reachable)');

my $disks_total_0 = $pair->node0->safe_psql(
	'postgres', 'SELECT disks_total FROM pg_cluster_quorum_state');
is($disks_total_0, '3', 'L3 node0 disks_total=3');

my $disks_total_1 = $pair->node1->safe_psql(
	'postgres', 'SELECT disks_total FROM pg_cluster_quorum_state');
is($disks_total_1, '3', 'L3 node1 disks_total=3');


$pair->stop_pair;

done_testing();


#-------------------------------------------------------------------------
# spec-2.6 Hardening v0.5+ backlog — L4-L8 still SKIP (per spec-2.28
# Sprint A Step 5 design-gap finding 2026-05-10):
#
#   L4-L5 SIGSTOP / SIGCONT peer postmaster → expected quorum_state
#         OK→LOST→OK transition.
#         **GAP DISCOVERED in spec-2.28 Sprint A Step 5 (098 TAP
#         design)**: spec-2.6 decide_quorum_view computes quorum_
#         state purely from disks_ok (per-disk read success);
#         SIGSTOP peer postmaster does NOT change disks_ok on the
#         survivor — both nodes can still read all 3 disk files.
#         The alive_bitmap drops the SIGSTOP'd peer after heartbeat
#         times out, but alive_bitmap is NOT used by quorum_state.
#         Therefore SIGSTOP cannot trigger fence broadcast in
#         current spec-2.6 design.
#
#         Whether to include peer-alive in quorum_state decision is
#         an open architectural question for spec-2.6 hardening OR
#         spec-2.29 reconfig coordinator;NOT silently bundled into
#         spec-2.28.  See 098_fence_freeze_writes_2node.pl header
#         for full rationale + 5 documentation items.
#
#         Real L4-L5 verification needs disk I/O failure injection
#         (same harness as L8 below) to drive disks_ok < quorum_
#         size → quorum_state=LOST → cluster_fence_broadcast_freeze.
#
#   L6   Two postmasters configured with the same node_id (forced
#        via cluster_name_override style trick) → one observes
#        Q6 v0.2 newer-self collision on disk and FATAL exits.
#        Blocked on: ClusterPair currently hardcodes node_id 0/1;
#        needs an opt-in collision-injection path.
#
#   L7   kill -9 both postmasters → restart → boot-time epoch
#        recovery.  Blocked on: current_epoch_at_boot=0 placeholder.
#
#   L8   chmod 000 / truncate / unlink all 3 voting-disk files →
#        both postmasters drop in_quorum within one poll cycle.
#        Blocked on: stable I/O failure injection harness.
#        (This is the harness needed for L4-L5 too — see above.)
#
# Tracked in spec-2.6 Hardening v0.5+ backlog appendix +
# spec-2.28 Hardening v0.5+ backlog (shared blocker).
#-------------------------------------------------------------------------
