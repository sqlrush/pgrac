#-------------------------------------------------------------------------
#
# 267_thread_recovery_e2e.pl
#    spec-4.11 D1 — the inject -> real lmon GRD recovery FSM -> WAIT_CLUSTER ->
#    launch_workers -> executor worker -> RECOVER -> unfreeze end-to-end, on a real
#    2-node cluster_fs + shared per-thread WAL pair.
#
#    This is the ONE piece of online thread recovery the unit + stand-in tests
#    (t/258-266) cannot reach: the in-scope wiring of a SYNTHETIC dead-node
#    reconfig event through node0's real lmon FSM tick, where decide_scope is
#    APPLICABLE (2 declared nodes + cluster_fs + GUC on + exactly one survivor),
#    so launch_workers actually stamps the replay slot REPLAYING and registers a
#    per-episode executor worker (cluster_thread_recovery_worker_main) that drives
#    the WHOLE recovery: validated window -> replay data + visibility to shared
#    storage -> publish 3-way authority -> the D3 gate opens -> P7 unfreeze.
#
#    HISTORY (3b-4c correction).  This e2e was originally written fail-closed: a
#    foreign replay_one(AUTO) was OBSERVED to BLOCK, and the cause was mis-attributed
#    to the D4 validated-end corruption guard.  An external review found the real
#    cause -- a 3b-4c P1 bug in the replay engine: after applying the record ending
#    at the validated boundary, it re-read the dead thread's LEGITIMATE crash-point
#    torn tail and failed closed (validated_end deliberately TOLERATES that tail).
#    With the fix (the engine stops at the validated boundary) the AUTO path
#    RECOVERS to DONE -- so this is now a genuine happy-path e2e.  The deterministic
#    fail-closed posture is still pinned elsewhere: t/261 (R13 inject / incomplete
#    window -> BLOCKED + 0 authority), t/263 (validated_end corruption guard), t/268
#    (panic policy).
#
#    HARNESS FAITHFULNESS.  The dead node is NOT actually killed: the synthetic
#    inject drives node0's FSM deterministically (no real death + CSSD deadband ->
#    a single stable episode).  node1 stays ALIVE but quiescent (no DML after the
#    CHECKPOINT).  So this proves the survivor RECOVERS a quiescent foreign thread
#    end to end -- a MECHANISM-COMPLETION demonstration, NOT a fully faithful
#    fenced-dead-node scenario (a real death would fence node1 so it cannot race
#    the survivor's shared-storage writes).  The faithful fenced-death happy-path is
#    closed by L8 below (spec-4.12b D8): under enforcement default-ON the inject
#    fences node1, and L8 proves node1's own cluster_smgr write then fails closed.
#
#    Anti-false-green pins (every leg fails if the path is skipped out of scope):
#      L3  slot reaches DONE  -- only reachable if decide_scope was APPLICABLE
#          (out of scope the launch is a no-op and the slot stays IDLE), the worker
#          was registered AND ran (only the worker writes a terminal slot state),
#          and replay_one returned DONE (a real recovery), not NOT_APPLICABLE
#          (which never reaches REPLAYING / a terminal slot state).
#      L4  remaster_done++ -- the FSM reached P7 unfreeze, gated behind the D3 gate
#          that opens only once origin 1 is materialized by the recovery.
#
#    Leg map:
#      L0   2-node cluster_fs + 3 voting disks + shared per-thread WAL; both
#           nodes alive; node0 sees node1 connected.
#      L1   node1 (node_id 1 -> thread_2) publishes a real ACTIVE wal-state slot
#           (work + CHECKPOINT), then quiescent; pre-inject baseline (no recovery,
#           slot IDLE, node1 not materialized).
#      L2   inject node1-dead -> node0's REAL lmon FSM enters recovery
#           (remaster_started++).
#      L3   in-scope launch fired through the FSM tick -> worker ran
#           replay_one(AUTO) -> slot DONE (thread recovered).
#      L4   unfreezes: FSM reached P7 unfreeze once the dead origin materialized
#           (remaster_done++).
#      L5   node-local authority published for origin 1 (materialized list contains
#           '1'; remote outcomes diverted, never below baseline).
#      L6   local_complete(thread_2) = true: the D3 unfreeze gate is now open.
#      L7   idempotent: slot stays DONE across ticks (no re-launch / worker storm;
#           should_launch skips a DONE slot at the current epoch).
#      L8   (spec-4.12b D8) default-ON: the declared-dead but still-alive node1's
#           own cluster_smgr write fails closed (53R51) -- closes the caveat.
#
# Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
# Portions Copyright (c) 2026, pgrac contributors
#
# Author: SqlRush <sqlrush@gmail.com>
#
# IDENTIFICATION
#    src/test/cluster_tap/t/267_thread_recovery_e2e.pl
#
# NOTES
#    pgrac-original file.
#    Spec: spec-4.11-thread-recovery.md (FROZEN v0.3)
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../../perl";

use PostgreSQL::Test::ClusterPair;
use Test::More;
use Time::HiRes qw(usleep);

# 2 declared nodes + cluster_fs shared data + shared per-thread WAL root +
# quorum, with online thread recovery enabled: the only in-scope configuration
# for decide_scope (node_count == 2 -> exactly one survivor).
my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'threce2e',
	quorum_voting_disks => 3,
	shared_data         => 1,
	wal_threads_root    => 1,
	extra_conf          => [ 'autovacuum = off', 'cluster.online_thread_recovery = on' ]);
$pair->start_pair;

# Let the IC tier1 mesh settle before the first cross-node round trip (t/111/251).
usleep(3_000_000);

my $n0 = $pair->node0;
my $n1 = $pair->node1;

sub dump0
{
	my ($cat, $key) = @_;
	return $n0->safe_psql('postgres',
		"SELECT value FROM cluster_dump_state() WHERE category = '$cat' AND key = '$key'");
}

# node1 -> thread_2 (node_id N routes WAL into thread_(N+1)).
sub slot2 { return $n0->safe_psql('postgres', 'SELECT cluster_thread_replay_slot_state_test(2)'); }

# ----------
# L0: both nodes up; node0 sees node1 connected.
# ----------
is($n0->safe_psql('postgres', 'SELECT 1'), '1', 'L0 node0 alive');
is($n1->safe_psql('postgres', 'SELECT 1'), '1', 'L0 node1 alive');
ok($pair->wait_for_peer_state(0, 1, 'connected', 30), 'L0 node0 sees node1 connected');

# ----------
# L1: node1 publishes a real ACTIVE thread_2 wal-state slot (work + CHECKPOINT),
#     so the survivor recovers a REAL foreign thread (not an empty slot) with a
#     genuine validated window.  node1 is then left quiescent (no more DML).
#     Capture the pre-inject baseline.
# ----------
$n1->safe_psql('postgres', q{
	CREATE TABLE n1_t (id int primary key, v int);
	INSERT INTO n1_t SELECT g, g FROM generate_series(1, 500) g;
	CHECKPOINT;
});

my $started0   = dump0('grd_recovery', 'remaster_started');
my $done0      = dump0('grd_recovery', 'remaster_done');
my $committed0 = dump0('recovery', 'remote_outcome_committed');

is(slot2(), '0', 'L1 baseline: thread_2 replay slot IDLE (no recovery yet)');
is($n0->safe_psql('postgres', "SELECT cluster_thread_local_complete_test(2, '0/0')"),
	'f', 'L1 baseline: node1 (origin 1) not materialized');

# ============================================================
# INJECT: a synthetic node1-dead reconfig episode on node0 (coordinator).  This
# publishes a ReconfigEvent the local lmon GRD recovery FSM consumes on its next
# tick -- driving the REAL inject -> FSM -> WAIT_CLUSTER -> launch wiring.
# ============================================================
my $logoff = -s $n0->logfile || 0;
is($n0->safe_psql('postgres', 'SELECT cluster_reconfig_inject_dead_node_test(1)'),
	't', 'inject: synthetic node1-dead reconfig accepted on node0');

# ----------
# L2: the REAL lmon FSM consumed the event and entered recovery.
# ----------
ok($n0->poll_query_until('postgres',
		"SELECT (SELECT value::bigint FROM cluster_dump_state() "
	  . "WHERE category = 'grd_recovery' AND key = 'remaster_started') > $started0",
		't'),
	'L2 inject drove the REAL lmon FSM into recovery (remaster_started++)');

# ----------
# L3: in-scope launch fired THROUGH the FSM tick -> the worker ran
#     replay_one(AUTO) and RECOVERED the dead thread to a terminal DONE.  A DONE
#     terminal slot (state 2) proves: decide_scope APPLICABLE (out of scope leaves
#     it IDLE), the worker registered + ran (only the worker writes a terminal
#     state), and replay_one reached DONE -- it derived a validated window, the
#     replay engine applied the dead thread's data + visibility to shared storage
#     up to the validated torn-tail boundary (3b-4c P1: it STOPS at the boundary
#     instead of failing closed on the legitimate crash-point torn tail), and it
#     published the 3-way authority.
#
#     NOTE (harness faithfulness): the inject marks node1 dead while node1 is still
#     ALIVE but quiescent (no DML after the CHECKPOINT above).  So this is a
#     MECHANISM-COMPLETION demonstration -- the survivor recovers a quiescent
#     foreign thread end to end -- NOT a fully faithful fenced-dead-node scenario
#     (a real death would fence node1 so it cannot race the survivor's shared-
#     storage writes).  The faithful fenced-death happy-path is forward-linked.
# ----------
ok($n0->poll_query_until('postgres', 'SELECT cluster_thread_replay_slot_state_test(2) = 2', 't'),
	'L3 FSM-launched worker ran replay_one(AUTO) -> slot DONE (thread recovered)');

# ----------
# L3b: pin the worker's verdict EXPLICITLY in the log -- the executor reached its
#     terminal log line with result "done", not "blocked" and not "not applicable".
# ----------
ok($n0->wait_for_log(qr/online thread recovery: dead thread 2 -> done/, $logoff),
	'L3b worker reached terminal verdict DONE (not "blocked" / not "not applicable")');

# ----------
# L4: the FSM reaches P7 unfreeze.  remaster_done increments at P7, which is gated
#     behind the D3 unfreeze gate; that gate OPENS once origin 1 is materialized by
#     the successful recovery -- so the FSM lifts the freeze.
# ----------
ok($n0->poll_query_until('postgres',
		"SELECT (SELECT value::bigint FROM cluster_dump_state() "
	  . "WHERE category = 'grd_recovery' AND key = 'remaster_done') > $done0",
		't'),
	'L4 unfreezes: FSM reached P7 unfreeze once the dead origin was materialized');

# ----------
# L5: node-local authority published for the recovered dead origin.
# ----------
like(dump0('recovery', 'materialized_remote_instances'), qr/(^|,)1(,|$)/,
	'L5 node-local authority published (origin 1 materialized)');
cmp_ok(dump0('recovery', 'remote_outcome_committed'), '>=', $committed0,
	'L5 remote outcomes materialized (visibility pass published, never below baseline)');

# ----------
# L6: the D3 unfreeze gate predicate is now OPEN for the recovered thread.
# ----------
is($n0->safe_psql('postgres', "SELECT cluster_thread_local_complete_test(2, '0/0')"),
	't', 'L6 local_complete(thread_2) = true (D3 unfreeze gate open after recovery)');

# ----------
# L7: idempotent -- the slot stays DONE across ticks (should_launch skips a DONE
#     slot at the current episode epoch, so no re-launch / worker storm).
# ----------
my $stable = 1;
for (1 .. 5) {
	usleep(400_000);
	$stable = 0 if slot2() ne '2';
}
ok($stable, 'L7 idempotent: slot stays DONE across ticks (no re-launch / worker storm)');

# ----------
# L8 (spec-4.12b D8 L9): close the 4.11 faithful-fenced-death caveat.  With
# enforcement default ON (4.12b D4), the inject above wrote a durable fence marker
# declaring node1 dead (node1 is in the dead_bitmap at the bumped epoch).  node1 is
# still ALIVE, so its OWN qvotec poll reads that marker, sets self_fenced, and every
# cluster_smgr shared-storage write fails closed (53R51) -- node1 can no longer race
# the survivor's writes.  We retry to absorb the poll/refresh latency; the loop
# succeeds the moment node1's token observes the fence.
# ----------
my $fenced = 0;
for (1 .. 40) {
	my ($rc, $out, $err) = $n1->psql('postgres',
		q{INSERT INTO n1_t SELECT g, g FROM generate_series(501, 700) g});
	if ($rc != 0 && $err =~ /write[ -]?fenced|53R51|cluster shared-storage .* rejected/) {
		$fenced = 1;
		last;
	}
	usleep(250_000);
}
ok($fenced,
	'L8 (4.12b D8) default-ON: declared-dead but alive node1 cluster_smgr write -> '
	  . 'write-fenced 53R51 (closes the 4.11 faithful-fenced-death caveat)');

$pair->stop_pair;
done_testing();
