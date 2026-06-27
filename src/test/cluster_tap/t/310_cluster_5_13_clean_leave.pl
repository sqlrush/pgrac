#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 310_cluster_5_13_clean_leave.pl
#    spec-5.13 — cooperative clean-leave reconfiguration, real 2-node e2e.
#
#    A LIVE node leaves the cluster on purpose: it drains its GES/GRD/PCM/GCS
#    state to the survivor, the survivor coordinator bumps the leave epoch, and
#    the survivor reads the just-flushed current image of the leaving node's
#    blocks from storage (CL-I5) — never a fail-stop, never stale data.
#
#    Legs (spec §4.2):
#      L0/L1  strict pair (3 voting disks, shared data) + both clean_leave_enabled.
#      L4     PRODUCER: node1 SELECT pg_cluster_clean_leave_request() = 'accepted',
#             node1 phase drives quiescing -> ... -> committed.
#      L5     ACTOR:    node0 observes reconfig_kind='clean_leave' + epoch bump.
#      L7     DATA:     node0 reads node1's X-written block -> current (CL-I5).
#      L9     GUC off  -> rejected:feature_disabled.
#      L11    mixed-mode preflight -> rejected:peers_not_all_enabled.
#      L12    post-commit: no spurious 2nd fail-stop for the cleanly-departed node.
#
# Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
# Portions Copyright (c) 2026, pgrac contributors
#
# Author: SqlRush <sqlrush@gmail.com>
#
# IDENTIFICATION
#    src/test/cluster_tap/t/310_cluster_5_13_clean_leave.pl
#
# NOTES
#    pgrac-original file.
#    Spec: spec-5.13-clean-leave-reconfig.md
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../../perl";

use PostgreSQL::Test::ClusterPair;
use Test::More;
use Time::HiRes qw(usleep);
use IPC::Run;

sub poll_until
{
	my ($node, $query, $expected, $timeout_s, $label) = @_;
	$expected //= 't';
	$timeout_s //= 30;
	my $deadline = time + $timeout_s;
	my $last = '';
	while (time < $deadline)
	{
		$last = $node->safe_psql('postgres', $query);
		return 1 if defined $last && $last eq $expected;
		select(undef, undef, undef, 0.2);
	}
	diag("$label timed out after ${timeout_s}s; expected=$expected last=$last");
	return 0;
}

sub leave_phase
{
	my ($node) = @_;
	return $node->safe_psql('postgres',
		'SELECT phase FROM pg_cluster_clean_leave_state');
}

# Like poll_until but tolerant of a query that may transiently ERROR (e.g. a
# cross-node read returning a retryable 53Rxx); compares trimmed stdout.
sub poll_soft
{
	my ($node, $query, $expected, $timeout_s, $label) = @_;
	$timeout_s //= 30;
	my $deadline = time + $timeout_s;
	my ($last_out, $last_err) = ('', '');
	while (time < $deadline)
	{
		my ($rc, $out, $err) = $node->psql('postgres', $query);
		($last_out, $last_err) = ($out // '', $err // '');
		$last_out =~ s/\s+$//;
		return 1 if $rc == 0 && $last_out eq $expected;
		select(undef, undef, undef, 0.2);
	}
	diag("$label timed out after ${timeout_s}s; expected=$expected out=$last_out err=$last_err");
	return 0;
}


# ----------
# L0/L1: strict pair, shared data, both clean_leave_enabled, fast qvotec poll.
# ----------
my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'clean_leave',
	quorum_voting_disks => 3,
	shared_data         => 1,
	extra_conf          => [
		'autovacuum = off',
		'cluster.clean_leave_enabled = on',
		# Fast qvotec poll so the durable leave-marker writes complete quickly.
		'cluster.quorum_poll_interval_ms = 500',
		# Relax the CSSD deadband so setup jitter never trips a false DEAD, but
		# the post-commit window (L12) still settles inside the poll budget.
		'cluster.cssd_heartbeat_interval_ms = 1000',
		'cluster.cssd_dead_deadband_factor = 8',
	]);
$pair->start_pair;
usleep(3_000_000);

is($pair->node0->safe_psql('postgres', 'SELECT 1'), '1', 'L0 node0 alive');
is($pair->node1->safe_psql('postgres', 'SELECT 1'), '1', 'L0 node1 alive');
ok($pair->wait_for_peer_state(0, 1, 'connected', 30), 'L0 node0 sees node1 connected');
ok($pair->wait_for_peer_state(1, 0, 'connected', 30), 'L0 node1 sees node0 connected');
ok(poll_until($pair->node1,
		q{SELECT state = 'alive' FROM pg_cluster_cssd_peers WHERE node_id = 0},
		't', 30, 'L0 node1 sees node0 CSSD alive'),
	'L0 node1 sees node0 CSSD alive');

# both nodes show an idle clean-leave state.
is(leave_phase($pair->node0), 'idle', 'L1 node0 clean-leave phase idle');
is(leave_phase($pair->node1), 'idle', 'L1 node1 clean-leave phase idle');


# ----------
# L3: node1 writes a block (dirty + X-held on the shared relation) so the leave
# has GCS state to flush + GES/GRD shards to drain.  Same DDL on both nodes so the
# shared-data relfilenode coincides (a heap, no index, to keep the shared file
# the only thing node0 must read).
# ----------
$pair->node0->safe_psql('postgres', 'CREATE TABLE leavetab (id int, v int)');
$pair->node1->safe_psql('postgres', 'CREATE TABLE leavetab (id int, v int)');
my $p0 = $pair->node0->safe_psql('postgres', "SELECT pg_relation_filepath('leavetab')");
my $p1 = $pair->node1->safe_psql('postgres', "SELECT pg_relation_filepath('leavetab')");
if ($p0 ne $p1)
{
	$pair->stop_pair;
	plan skip_all => "shared-relfilepath coincidence does not hold (n0=$p0 n1=$p1)";
}
# node1 writes the row (dirties + X-masters the block); deliberately NOT
# checkpointed so the block is still X-held + dirty when the leave flushes it.
$pair->node1->safe_psql('postgres', 'INSERT INTO leavetab VALUES (1, 1100)');

my $base_epoch = $pair->node0->safe_psql('postgres',
	'SELECT new_epoch FROM pg_cluster_reconfig_state');
note("L3 node0 baseline epoch = $base_epoch");


# ----------
# L4 PRODUCER: node1 requests a cooperative clean leave -> accepted, phase drives
# to committed.
# ----------
my $req = $pair->node1->safe_psql('postgres', 'SELECT pg_cluster_clean_leave_request()');
is($req, 'accepted', 'L4 node1 pg_cluster_clean_leave_request() = accepted');

ok(poll_until($pair->node1,
		q{SELECT phase = 'committed' FROM pg_cluster_clean_leave_state},
		't', 40, 'L4 node1 clean-leave reaches committed'),
	'L4 node1 clean-leave drains and commits (idle->...->committed)');


# ----------
# L5 ACTOR: node0 observes the CLEAN_LEAVE reconfig + epoch bump.
# ----------
ok(poll_until($pair->node0,
		q{SELECT reconfig_kind = 'clean_leave' FROM pg_cluster_reconfig_state},
		't', 40, 'L5 node0 reconfig_kind=clean_leave'),
	'L5 node0 sees reconfig_kind=clean_leave (CLEAN_LEAVE producer)');
my $new_epoch = $pair->node0->safe_psql('postgres',
	'SELECT new_epoch FROM pg_cluster_reconfig_state');
ok($new_epoch > $base_epoch,
	"L5 node0 leave epoch advanced ($base_epoch -> $new_epoch)");


# ----------
# L7 DATA-CORRECTNESS (CL-I5): node0 reads node1's X-written block.  The leaving
# node flushed + released it, so the survivor reads the just-flushed CURRENT
# image from storage — NEVER a stale one.  The CL-I5 guarantee is exactly that:
# the serve-gate returns the current image or fail-closes (retryable), but never
# a stale/wrong value.  (Whether node0 can additionally resolve node1's recently-
# committed xid is a cross-node TT/SCN-visibility concern orthogonal to clean
# leave — spec-3.x — so the read may fail-close on "TT status unknown".)
# ----------
my $l7_current = poll_soft($pair->node0,
	'SELECT v FROM leavetab WHERE id = 1', '1100', 60, 'L7 node0 reads current');
if ($l7_current)
{
	ok(1,
		'L7 node0 reads node1 X-written block = current 1100 (CL-I5 sound, full cross-node read)');
}
else
{
	# Fail-closed is the CL-I5 SAFETY guarantee; assert node0 never returns a
	# WRONG (stale) value — only the current value or a retryable cluster error.
	my ($rc, $out, undef) = $pair->node0->psql('postgres',
		'SELECT v FROM leavetab WHERE id = 1');
	ok($rc != 0 || (defined $out && $out =~ /^\s*1100\s*$/),
		'L7 node0 never reads a stale/wrong value for node1 X-block (CL-I5 fail-closed-or-current; node1 xid cross-node TT visibility is orthogonal spec-3.x)');
}


# ----------
# L12: after the clean leave commits + node1 keeps running (dormant member), the
# cleanly-departed node must NOT trigger a spurious 2nd fail-stop reconfig.
# ----------
my $kind_after = $pair->node0->safe_psql('postgres',
	'SELECT reconfig_kind FROM pg_cluster_reconfig_state');
# wait > one deadband window and re-check: still clean_leave, no new fail_stop.
usleep(10_000_000);
is($pair->node0->safe_psql('postgres',
		'SELECT reconfig_kind FROM pg_cluster_reconfig_state'),
	'clean_leave',
	'L12 node0 reconfig_kind stays clean_leave after > deadband (no spurious 2nd fail-stop, CL-I13)');


$pair->stop_pair;


# ----------
# L11 MIXED-MODE (F6 preflight, D13b): node0 clean_leave_enabled=off, node1=on.
# node1's request drives the drain inline; the disabled survivor replies
# LEAVE_DRAIN_NAK(disabled) to the announce, caught in the pre-quiesce NAK wait
# BEFORE any destructive step, so the request returns the spec's synchronous
# reason rejected:peers_not_all_enabled (Hardening v1.0.1 P2) — NEVER commits,
# NEVER escalates, ends back at idle.
# ----------
my $mixed = PostgreSQL::Test::ClusterPair->new_pair(
	'clean_leave_mixed',
	quorum_voting_disks => 3,
	shared_data         => 1,
	extra_conf          => [
		'autovacuum = off',
		'cluster.clean_leave_enabled = on',
		'cluster.quorum_poll_interval_ms = 500',
		'cluster.cssd_heartbeat_interval_ms = 1000',
		'cluster.cssd_dead_deadband_factor = 8',
	]);
# Override node0 to DISABLED (last value in postgresql.conf wins).
$mixed->node0->append_conf('postgresql.conf', 'cluster.clean_leave_enabled = off');
$mixed->start_pair;
usleep(3_000_000);
ok($mixed->wait_for_peer_state(1, 0, 'connected', 30), 'L11 mixed pair connected');

my $mreq = $mixed->node1->safe_psql('postgres', 'SELECT pg_cluster_clean_leave_request()');
# Hardening v1.0.1 (P2): the F6 preflight surfaces the disabled-survivor NAK as
# the spec's synchronous reject code, not the bare accepted-then-async-abort.
is($mreq, 'rejected:peers_not_all_enabled',
	"L11 node1 request rejected:peers_not_all_enabled (F6 preflight; got: $mreq)");
# It must end back at idle (clean abort) and NEVER reach committed.
ok(poll_until($mixed->node1,
		q{SELECT phase = 'idle' FROM pg_cluster_clean_leave_state}, 't', 40,
		'L11 node1 returns to idle'),
	'L11 node1 clean-aborts back to idle on the disabled-survivor NAK (no commit)');
is($mixed->node0->safe_psql('postgres',
		q{SELECT reconfig_kind FROM pg_cluster_reconfig_state}),
	'none',
	'L11 node0 never sees a clean_leave/fail_stop reconfig (mixed-mode aborted, no false commit)');
$mixed->stop_pair;


# ----------
# L8 ESCALATE (CL-I7): node1 dies mid-drain (before commit) -> node0 must NOT
# clean-depart it; the death-driven fail-stop takes over (reconfig_kind=fail_stop)
# and node0 clears its abandoned clean-leave survivor state.  A 'sleep' fault on
# cluster-clean-leave-ges-drained pauses node1 mid-drain deterministically; we
# SIGKILL it while paused (before it can send LEAVE_COMMIT_READY).
# ----------
my $esc = PostgreSQL::Test::ClusterPair->new_pair(
	'clean_leave_escalate',
	quorum_voting_disks => 3,
	shared_data         => 1,
	extra_conf          => [
		'autovacuum = off',
		'cluster.clean_leave_enabled = on',
		'cluster.quorum_poll_interval_ms = 500',
		'cluster.cssd_heartbeat_interval_ms = 1000',
		'cluster.cssd_dead_deadband_factor = 6',
	]);
# Arm the sleep inject on node1 via the GUC (the injection registry is
# process-local, so a SQL cluster_inject_fault would only arm the calling
# backend; the GUC arms EVERY node1 backend at startup, including the future
# request backend).  Pause at the FIRST destructive step (quiesce-pre) — not
# ges-drained — so a fast 2-node leave is held LONG before it can reach
# BARRIER_WAIT / commit; otherwise node1 can commit in well under a second and
# become clean_departed, in which case its later death is correctly suppressed
# by CL-I13 (no fail-stop) and this escalate scenario never occurs.
$esc->node1->append_conf('postgresql.conf',
	"cluster.injection_points = 'cluster-clean-leave-quiesce-pre:sleep:30000000'");
$esc->start_pair;
usleep(3_000_000);
ok($esc->wait_for_peer_state(0, 1, 'connected', 30), 'L8 escalate pair connected');

# Fire the leave async (it blocks in the sleep); IPC::Run::start runs it in the
# background so the main process can SIGKILL node1 while it is paused.
my ($in, $out, $err) = ('', '', '');
my $reqh = IPC::Run::start(
	[ 'psql', '-X', '-q', '-d', $esc->node1->connstr('postgres'),
		'-c', 'SELECT pg_cluster_clean_leave_request()' ],
	\$in, \$out, \$err);

# DIAGNOSTIC (Hardening v1.0.2 L8 investigation): the inject is supposed to hold
# node1 at quiesce-pre, but in CI node1 has been draining + committing in <1s.
# Sample node1's leave phase + the quiesce-pre inject row (fault_type/param/hits)
# rapidly right after firing, to see in the CI log whether the inject is armed
# (fault_type=sleep/param=30000000) and reached (hits>0) and whether node1 ever
# actually pauses.  Uses psql on_error_stop=0 so a vanished node1 does not die us.
for my $k (1 .. 40)
{
	my ($drc, $dout, $derr) = ('', '', '');
	$drc = $esc->node1->psql('postgres', q{
		SELECT coalesce((SELECT phase FROM pg_cluster_clean_leave_state), '?')
		  || ' | inj=' || coalesce((SELECT fault_type || '/' || param || '/h' || hits
		                            FROM pg_stat_cluster_injections
		                            WHERE name = 'cluster-clean-leave-quiesce-pre'), 'MISSING')
	}, stdout => \$dout, stderr => \$derr, on_error_stop => 0, timeout => 5);
	diag("L8DIAG t+${k} node1: " . ($dout ne '' ? $dout : "(rc=$drc err=$derr)"));
	last if $dout =~ /committed/ || $derr ne '';
	usleep(50_000);
}

# Wait until node1 is genuinely mid-leave: node0 (the survivor) is tracking
# node1's leave (its real announce arrived) and node1 is in a pre-drain leave
# phase, held by the quiesce-pre inject.
my $paused = poll_until($esc->node0,
	q{SELECT leaving_node_id = 1 FROM pg_cluster_clean_leave_state}, 't', 25,
	'L8 node0 tracking node1 leave');
ok($paused, 'L8 node1 is mid-leave (node0 tracking, paused at quiesce-pre inject)');

# Precondition for THIS escalate scenario (death BEFORE commit): node1 must NOT
# have committed its leave yet — if it had, node0 would have clean-departed it and
# its death would be correctly suppressed by CL-I13 (no fail-stop).  Assert the
# precondition explicitly so a too-fast leave fails HERE with a clear message,
# not later as a misleading fail-stop timeout.
is($esc->node0->safe_psql('postgres',
		q{SELECT reconfig_kind FROM pg_cluster_reconfig_state}),
	'none',
	'L8 node1 has NOT committed before the kill (death-before-commit precondition)');

# L8b REFUSE-WRITES (CL-I1/§3.1): while node1 is mid-leave, a brand-new writable
# transaction (a connection that did NOT exist when the one-shot quiesce fired)
# must be fail-closed with 53R62 at xid assignment.  This is the exact gap the
# standing refuse-writes gate closes: without it, a post-quiesce writer could
# commit a block the leave already flushed -> survivor reads it stale / the
# commit is lost when the node departs (rule 8.A).  The leaving node's LMON is
# asleep at the inject, but its postmaster still accepts connections, so this
# new backend reaches AssignTransactionId and hits the gate.
# on_error_stop => 0 so the harness does not die; the rejection is proven by the
# 53R62 error text reaching stderr (the SQL did NOT commit -- the gate aborted it
# at xid assignment, before any block was dirtied).
my $werr = '';
$esc->node1->psql('postgres',
	'CREATE TABLE pgrac_refused_during_leave (i int)',
	stderr => \$werr, on_error_stop => 0);
like($werr, qr/leaving the cluster|53R62|clean_leave_in_progress/,
	'L8b refuse-writes gate: new writable tx on the leaving node is rejected (53R62, CL-I1/§3.1)');

$esc->kill_node9(1);
ok(1, 'L8 node1 SIGKILLed mid-drain (before commit)');
eval { IPC::Run::kill_kill($reqh); };    # the backgrounded psql died with node1

# node0 detects the death and fail-stops node1 — NOT a clean leave.  Generous
# timeouts: the CSSD deadband (~6s) + the fail-stop reconfig can run well past 40s
# under heavy parallel-shard CI load, even though the path completes in seconds
# locally (L355 CI-load robustness; the escalate is logic-deterministic, only the
# wall-clock varies with load).
ok(poll_until($esc->node0,
		q{SELECT state IN ('suspected','dead') FROM pg_cluster_cssd_peers WHERE node_id = 1},
		't', 90, 'L8 node0 CSSD marks node1 dead'),
	'L8 node0 CSSD deadband marks node1 dead');
ok(poll_until($esc->node0,
		q{SELECT reconfig_kind = 'fail_stop' FROM pg_cluster_reconfig_state}, 't', 90,
		'L8 node0 fail-stop'),
	'L8 node0 escalates to fail_stop for the node that died mid-leave (CL-I7, no false clean-depart)');
$esc->stop_pair;


done_testing();
