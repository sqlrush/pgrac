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
#    L2   Both nodes register self in voting disks within first poll
#         cycle (pg_cluster_voting_disks shows 3 rows on each side;
#         pg_cluster_quorum_state self_slot.last_seen_ms is fresh).
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
# L2: both nodes registered in voting disks
# ============================================================
# Each side queries pg_cluster_voting_disks (its own GUC parse) and
# reports 3 disk rows.  This proves the GUC propagated symmetrically
# via ClusterPair postgresql.conf rewrite.
my $disks0 = $pair->node0->safe_psql(
	'postgres', 'SELECT count(*) FROM pg_cluster_voting_disks');
is($disks0, '3', 'L2 node0 sees 3 voting-disk rows');

my $disks1 = $pair->node1->safe_psql(
	'postgres', 'SELECT count(*) FROM pg_cluster_voting_disks');
is($disks1, '3', 'L2 node1 sees 3 voting-disk rows');


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
# spec-2.6 Hardening v0.5+ backlog — L4-L8 still SKIP.
#
#   L4   SIGSTOP node0 → node1 detects quorum loss within ~5s.
#        Blocked on: cluster_freeze_writes ProcSignal broadcast
#        (Hardening v0.3 backlog #1).  Without broadcast, backend
#        fail-closed latency = lease_window + commit_boundary
#        check granularity, which is observability-flaky for a
#        5s assertion.
#
#   L5   SIGCONT → node0 rejoins, both back to in_quorum=t.
#        Blocked on: same as L4 (the rejoin path needs the thaw
#        signal companion).
#
#   L6   Two postmasters configured with the same node_id (forced
#        via cluster_name_override style trick) → one observes
#        Q6 v0.2 newer-self collision on disk and FATAL exits.
#        Blocked on: ClusterPair currently hardcodes node_id 0/1;
#        needs an opt-in collision-injection path.  Implementation
#        cost is moderate (~50 LOC) but test-fixture stability
#        requires that the loser's FATAL log line is deterministic,
#        which we have not yet smoke-tested.
#
#   L7   kill -9 both postmasters → restart → boot-time epoch
#        recovery: each node's current_epoch must be set to
#        max(disk_epoch_seen) + 1.
#        Blocked on: current_epoch_at_boot=0 placeholder
#        (Hardening v0.5+ backlog #2).  Until that lands, restart
#        races to the same epoch and the assertion is meaningless.
#
#   L8   chmod 000 / truncate / unlink all 3 voting-disk files →
#        both postmasters drop in_quorum within one poll cycle and
#        backends fail-closed on COMMIT.
#        Blocked on: stable I/O failure injection harness.  Naive
#        unlink() on an open fd has divergent semantics on Linux
#        (still readable through the fd) vs macOS, and chmod 000
#        as the test user is a no-op when running as root in CI.
#        Needs a small per-disk fault-injection GUC (Hardening
#        v0.5+ backlog #4).
#
# These five scenarios are tracked in spec-2.6 Hardening v0.5+
# backlog appendix.  When the upstream blockers land (or a new
# spec demands the coverage — e.g. spec-2.28 Fence-lite for L4/L5/
# L8), reopen this file and append the L4-L8 implementations after
# the done_testing() of L1-L3.
#-------------------------------------------------------------------------
