#-------------------------------------------------------------------------
#
# 267_thread_recovery_e2e.pl
#    spec-4.11 D1 increment 3b-4b Part 4 — the inject -> real lmon GRD recovery
#    FSM -> WAIT_CLUSTER -> launch_workers -> executor worker -> fail-closed-frozen
#    end-to-end, on a real 2-node cluster_fs + shared per-thread WAL pair.
#
#    This is the ONE piece of online thread recovery the unit + stand-in tests
#    (t/258-266) cannot reach: the in-scope wiring of a SYNTHETIC dead-node
#    reconfig event through node0's real lmon FSM tick, where decide_scope is
#    APPLICABLE (2 declared nodes + cluster_fs + GUC on + exactly one survivor),
#    so launch_workers actually stamps the replay slot REPLAYING and registers a
#    per-episode executor worker (cluster_thread_recovery_worker_main).
#
#    WHY fail-closed (not happy-path-to-unfreeze).  Driving a foreign thread's
#    replay_one(AUTO) all the way to DONE needs the dead thread's wal-state slot
#    to yield a valid, cross-node-readable recoverable window; measure-first
#    showed that fail-closes in this single-machine harness (the observational
#    highest_lsn races the durably-flushed thread WAL, so the D4 validated-end
#    corruption guard correctly refuses to silently truncate committed WAL).
#    That is the SAME class of non-determinism t/251 hit and deferred to unit.
#    The happy path itself (replay -> DONE -> publish 3-way authority -> gate
#    flips -> unfreeze) is proven deterministically at t/261 (EXPLICIT window),
#    t/262 (gate flips once materialized), t/265 (worker dispatch) and t/266
#    (launch no-op out of scope).  So this test pins the genuinely-new wiring AND
#    the 8.A fail-closed posture: an unrecoverable dead origin keeps the cluster
#    frozen, never silently unfreezes, never publishes authority.
#
#    The dead node is NOT actually killed (the synthetic inject drives the FSM
#    deterministically without a real death + CSSD deadband, per the spec-4.11
#    3b-4b plan).  Keeping node1 alive also keeps the episode stable: no real
#    death => no competing CSSD reconfig => a single, deterministic episode.
#
#    Anti-false-green pins (every leg fails if the path is skipped out of scope):
#      L3  slot reaches BLOCKED  -- only reachable if decide_scope was APPLICABLE
#          (out of scope the launch is a no-op and the slot stays IDLE), the
#          worker was registered AND ran (only the worker writes a terminal slot
#          state), and replay_one returned BLOCKED (window-derivation /
#          unrecoverable), not NOT_APPLICABLE (which never reaches REPLAYING).
#      L4  remaster_done unchanged -- the FSM never reached P7 unfreeze; an
#          out-of-scope gate would be a no-op and the FSM WOULD unfreeze
#          (remaster_done++), so this independently fails on a scope skip.
#
#    Leg map:
#      L0   2-node cluster_fs + 3 voting disks + shared per-thread WAL; both
#           nodes alive; node0 sees node1 connected.
#      L1   node1 (node_id 1 -> thread_2) publishes a real ACTIVE wal-state slot
#           (work + CHECKPOINT); pre-inject baseline (no recovery, slot IDLE,
#           node1 not materialized).
#      L2   inject node1-dead -> node0's REAL lmon FSM enters recovery
#           (remaster_started++).
#      L3   in-scope launch fired through the FSM tick -> worker ran
#           replay_one(AUTO) -> slot BLOCKED (fail-closed window-derivation).
#      L4   stays frozen: FSM never reached P7 unfreeze (remaster_done unchanged).
#      L5   node-local authority NOT published for origin 1 (materialized list is
#           '-'; no remote commits diverted).
#      L6   local_complete(thread_2) = false: the D3 unfreeze gate stays closed.
#      L7   idempotent: slot stays BLOCKED across ticks (no re-launch / worker
#           storm; should_launch skips a BLOCKED slot at the current epoch).
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
#     so the survivor's recovery attempt blocks specifically at window
#     derivation of a real foreign thread (not on an empty slot).  Capture the
#     pre-inject baseline.
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
#     replay_one(AUTO) and fell back to BLOCKED at window derivation.  A BLOCKED
#     terminal slot proves: decide_scope APPLICABLE (out of scope leaves it
#     IDLE), worker registered+ran (only the worker writes a terminal state),
#     result = BLOCKED (REPLAYING is never reached for NOT_APPLICABLE).
# ----------
ok($n0->poll_query_until('postgres', 'SELECT cluster_thread_replay_slot_state_test(2) = 3', 't'),
	'L3 FSM-launched worker ran replay_one(AUTO) -> slot BLOCKED (fail-closed window-derivation)');

# ----------
# L3b: pin the worker's verdict EXPLICITLY in the log -- the executor reached its
#     terminal log line with result BLOCKED, not "done" and not "not applicable".
#     This is the airtight discriminator the user required: the failure is a real
#     replay_one BLOCKED (window-derivation / unrecoverable), not a scope skip.
# ----------
ok(
	$n0->wait_for_log(qr/online thread recovery: dead thread 2 -> blocked \(kept frozen\)/, $logoff),
	'L3b worker reached terminal verdict BLOCKED (not "done" / not "not applicable")');

# Let several more FSM ticks pass to settle the steady state.
usleep(2_000_000);

# ----------
# L4: the FSM stays frozen -- it never reached P7 unfreeze.  remaster_done only
#     increments at P7, which is gated behind the D3 unfreeze gate; that gate
#     stays closed because origin 1 is never materialized.  (An out-of-scope
#     gate would be a no-op -> the FSM would unfreeze -> remaster_done++.)
# ----------
is(dump0('grd_recovery', 'remaster_done'), $done0,
	'L4 stays frozen: FSM never reached P7 unfreeze (dead origin unmaterialized)');

# ----------
# L5: no node-local authority published for the unrecoverable dead origin.
# ----------
is(dump0('recovery', 'materialized_remote_instances'), '-',
	'L5 no node-local authority published (origin 1 not materialized)');
is(dump0('recovery', 'remote_outcome_committed'), $committed0,
	'L5 no remote commits materialized (visibility pass never published)');

# ----------
# L6: the D3 unfreeze gate predicate stays closed for the dead thread.
# ----------
is($n0->safe_psql('postgres', "SELECT cluster_thread_local_complete_test(2, '0/0')"),
	'f', 'L6 local_complete(thread_2) = false (D3 unfreeze gate stays closed)');

# ----------
# L7: idempotent -- the slot stays BLOCKED across ticks (should_launch skips a
#     BLOCKED slot at the current episode epoch, so no re-launch / worker storm).
# ----------
my $stable = 1;
for (1 .. 5) {
	usleep(400_000);
	$stable = 0 if slot2() ne '3';
}
ok($stable, 'L7 idempotent: slot stays BLOCKED across ticks (no re-launch / worker storm)');

$pair->stop_pair;
done_testing();
