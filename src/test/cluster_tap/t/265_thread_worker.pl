#-------------------------------------------------------------------------
#
# 265_thread_worker.pl
#    spec-4.11 D1 increment 3b-4b Part 2 — the online thread-recovery EXECUTOR
#    worker's testable core.
#
#    cluster_thread_recovery_worker_run(dead_tid) is what the per-episode
#    background worker runs: read the per-thread replay slot (the lmon launch
#    marked it REPLAYING and stamped the launch episode), enforce the L235
#    episode-staleness guard against the live GRD recovery episode, drive
#    cluster_thread_recovery_replay_one, and write the terminal slot state.  The
#    slot is OBSERVABILITY + episode coordination ONLY; the serve/unfreeze gate
#    reads the node-local merged.authority, not this slot (§2.4 Q4).
#
#    The TEST-ONLY SRF cluster_thread_recovery_worker_run_test(dead_tid,
#    epoch_offset, do_mark) simulates a launch (stamp REPLAYING at the live
#    episode + offset) then runs the core, returning  <res>:<final_slot_state>
#    (or <res>:noslot), then restores IDLE.  res is a ClusterThreadRecResult
#    (done 0 / blocked 1 / not_applicable 2); final_slot_state is a
#    ClusterThreadRecReplayState (idle 0 / replaying 1 / done 2 / blocked 3).
#
#    Single-node stand-in: cluster.online_thread_recovery is off / there are no
#    peers, so replay_one's scope gate returns not_applicable (2) without doing
#    actual recovery -- the in-scope DONE path is the 2-node e2e (Part 4).  What
#    this pins is the worker's CONTROL logic: dispatch + L235 gate + slot write.
#
#      L1 fresh launch: epoch matches -> dispatch replay_one (not_applicable on a
#         single node) and write the terminal slot state (blocked) -> "2:3".
#      L2/L3 stale launch: a non-zero offset makes the launch epoch stale -> the
#         L235 guard ABORTS before replay_one, leaving the slot REPLAYING -> "2:1".
#      L4 not REPLAYING: an unmarked (IDLE) slot is a no-op -> "2:0".
#      L5-L7 fail-closed: a bad dead_tid names no slot -> "2:noslot".
#
# IDENTIFICATION
#    src/test/cluster_tap/t/265_thread_worker.pl
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../lib";

use PgracClusterNode;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PgracClusterNode->new('threadworker');
$node->init;
$node->append_conf('postgresql.conf',
		"cluster.enabled = on\n"
	  . "cluster.node_id = 0\n"
	  . "cluster.allow_single_node = on\n"
	  . "autovacuum = off\n");
$node->start;

sub worker_run
{
	my ($tid, $offset, $mark) = @_;
	return $node->safe_psql('postgres',
		"SELECT cluster_thread_recovery_worker_run_test($tid, $offset, $mark)");
}

# ============================================================
# L1 fresh launch: matching epoch -> replay_one dispatched (not_applicable on a
# single node) and the terminal slot state written (blocked).
# ============================================================
is(worker_run(1, 0, 'true'), '2:3',
	'L1 fresh launch: dispatch replay_one + write terminal slot state (blocked)');

# ============================================================
# L2/L3 stale launch: a non-zero epoch offset makes the slot stale -> the L235
# guard aborts BEFORE replay_one and leaves the slot REPLAYING (no terminal write).
# ============================================================
is(worker_run(1, 7, 'true'), '2:1',
	'L2 stale launch: L235 aborts before replay_one, slot left REPLAYING');
is(worker_run(1, 1000000, 'true'), '2:1',
	'L3 stale launch: any non-zero epoch gap aborts (slot left REPLAYING)');

# ============================================================
# L4 not REPLAYING: an unmarked (IDLE) slot is a no-op -> not_applicable, IDLE.
# ============================================================
is(worker_run(1, 0, 'false'), '2:0',
	'L4 not REPLAYING: an IDLE slot is a no-op (worker touches nothing)');

# ============================================================
# L5-L7 fail-closed: a bad dead_tid names no slot.
# ============================================================
is(worker_run(0, 0, 'true'), '2:noslot',
	'L5 fail-closed: dead_tid 0 (legacy) -> no slot');
is(worker_run(200, 0, 'true'), '2:noslot',
	'L6 fail-closed: dead_tid above max thread id -> no slot');
is(worker_run(70000, 0, 'true'), '2:noslot',
	'L7 fail-closed: dead_tid beyond uint16 -> no slot');

$node->stop;
done_testing();
