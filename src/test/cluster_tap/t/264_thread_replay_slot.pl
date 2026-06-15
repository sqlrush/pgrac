#-------------------------------------------------------------------------
#
# 264_thread_replay_slot.pl
#    spec-4.11 D1 increment 3b-4b Part 1 — the per-thread online replay-state
#    shmem slot.
#
#    The online thread-recovery executor (3b-4b) needs per-dead-thread
#    bookkeeping that survives across the lmon launch and the worker run: the
#    launch path stamps the GRD recovery_episode_epoch and marks REPLAYING, the
#    per-episode worker writes the terminal DONE/BLOCKED and re-reads the epoch
#    for the L235 staleness guard.  Part 1 lands that slot (a small mirror in the
#    "pgrac recovery plan" shmem region), its bounds-checked accessor, the
#    mark/read/set helpers, and the pure epoch guard.  It is OBSERVABILITY +
#    episode coordination ONLY: the authoritative reader gate reads the
#    node-local merged.authority, NOT this slot (§2.4 Q4 3-way authority).  The
#    executor / worker / lmon wiring that drives the slot lands in 3b-4b Parts
#    2-4; here the round-trip is exercised directly via a TEST-ONLY SRF.
#
#    cluster_thread_replay_slot_test(dead_tid) round-trips the slot and returns
#    'noslot' for a bad id, else  <st0>:<st1>:<ep1>:<st2>:<abort_same>:<abort_diff>
#    where stN is a ClusterThreadRecReplayState int (idle 0 / replaying 1 / done 2
#    / blocked 3), ep1 is the stamped episode (0xABCD = 43981), and the abort
#    flags are the pure L235 guard for same / different live episodes.  The SRF
#    restores IDLE, so reruns are deterministic.
#
#      L1 round-trip: tid 1 -> mark REPLAYING (st1=1) stamps ep1=43981, set DONE
#         (st2=2), epoch guard same=0 / different=1; starts IDLE (st0=0).
#      L2 fields: the same round-trip, asserted field-by-field for readability.
#      L3 max boundary: the highest real thread id (128) also has a slot.
#      L4 idempotent: a rerun of tid 1 yields the identical summary (IDLE restored).
#      L5 fail-closed: dead_tid 0 (legacy) names no slot -> 'noslot'.
#      L6 fail-closed: dead_tid above the max thread id -> 'noslot'.
#      L7 fail-closed: dead_tid beyond uint16 -> 'noslot' (accessor range gate).
#
# IDENTIFICATION
#    src/test/cluster_tap/t/264_thread_replay_slot.pl
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

# The replay-plan shmem region is registered unconditionally on a cluster node,
# so the slot accessor works on a plain single node (no wal_threads_dir needed).
my $node = PgracClusterNode->new('replayslot');
$node->init;
$node->append_conf('postgresql.conf',
		"cluster.enabled = on\n"
	  . "cluster.node_id = 0\n"
	  . "cluster.allow_single_node = on\n"
	  . "autovacuum = off\n");
$node->start;

sub slot_test
{
	my ($tid) = @_;
	return $node->safe_psql('postgres', "SELECT cluster_thread_replay_slot_test($tid)");
}

# The deterministic summary for any valid slot: starts IDLE (0), mark REPLAYING
# (1) stamps episode 0xABCD = 43981, set DONE (2), epoch guard same=0 / diff=1.
my $expected = '0:1:43981:2:0:1';

# ============================================================
# L1 round-trip: a real thread id exercises every helper.
# ============================================================
is(slot_test(1), $expected,
	'L1 round-trip: tid 1 IDLE->REPLAYING(stamped)->DONE + L235 guard');

# ============================================================
# L2 fields: assert each property individually (discriminating, self-documenting).
# ============================================================
my ($st0, $st1, $ep1, $st2, $asame, $adiff) = split /:/, slot_test(1);
is($st0, 0, 'L2 slot starts IDLE (0)');
is($st1, 1, 'L2 mark_replaying sets REPLAYING (1)');
is($ep1, 43981, 'L2 mark_replaying stamps the launch episode (0xABCD)');
is($st2, 2, 'L2 set_state writes the terminal DONE (2)');
is($asame, 0, 'L2 L235 guard: same live episode -> no abort');
is($adiff, 1, 'L2 L235 guard: different live episode -> abort');

# ============================================================
# L3 max boundary: the highest real thread id (CLUSTER_WAL_THREAD_MAX = 128).
# ============================================================
is(slot_test(128), $expected, 'L3 max boundary: thread id 128 has a slot');

# ============================================================
# L4 idempotent: the SRF restores IDLE, so a rerun is identical.
# ============================================================
is(slot_test(1), $expected, 'L4 idempotent: rerun of tid 1 yields the same summary');

# ============================================================
# L5-L7 fail-closed: a bad id names no slot (the accessor's range gate, NEVER
# aliasing the unused slot 0).
# ============================================================
is(slot_test(0), 'noslot', 'L5 fail-closed: dead_tid 0 (legacy) -> noslot');
is(slot_test(200), 'noslot', 'L6 fail-closed: dead_tid above max thread id -> noslot');
is(slot_test(70000), 'noslot', 'L7 fail-closed: dead_tid beyond uint16 -> noslot');

$node->stop;
done_testing();
