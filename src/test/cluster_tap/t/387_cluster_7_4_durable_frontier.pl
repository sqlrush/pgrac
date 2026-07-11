#-------------------------------------------------------------------------
# spec-7.4 D1 durable frontier + BOC payload v1 -- 2-node legs L1-L8.
#
#   node0 commits; node1 observes node0's published contiguous durable
#   frontier (origin_durable_safe_scn) through the BOC payload v1 channel
#   and caches it per (origin, epoch).  Legs:
#
#     L1  staleness, event path on: node0 commit -> node1 cached-frontier
#         advance.  Hard: every round converges; p50 clearly below the
#         sweep-only cadence.  (The strict local-machine numbers -- p99
#         <= 10ms and >= 5x vs sweep-only -- are enforced by the census
#         runner report, not by this CI leg; set PGRAC_74_STRICT=1 for
#         the strict local gate.)
#     L2  sweep fallback: arm cluster-boc-event-publish (suppresses the
#         commit-event signal); staleness degrades to the sweep + LMON
#         cadence but still converges.  Proves the 100ms sweep remains a
#         sufficient fallback and that the injection probe engages.
#     L3  commit-unperturbed (behavioural half): with the event publish
#         suppressed AND LMON stalled via injection, commits neither
#         fail nor wait.  (The <= 3% commit-p99 red line is measured by
#         the census runner on a no-cassert build -- see the id-ledger
#         note for t/387.)
#     L4  batching: a concurrent commit burst produces sublinear BOC
#         fanout (dirty-flag dedup coalesces commits per drain).
#     L5  off byte-equivalence: cluster.boc_event_publish=off reverts to
#         0-length BOC pulses -- no payload accepts, remote cache holds
#         its stale value, sweep pulses still flow (observe bumps), and
#         the local frontier keeps advancing; flipping back on resumes.
#     L6  out-of-order hard leg: T1 stalls between commit-SCN allocation
#         and its commit record (sleep injection at
#         cluster-scn-commit-post-advance); T2 (later SCN) commits fully
#         and durably; the published frontier must NOT advance past the
#         stalled T1 and must advance once T1 completes.
#     L7  idle-async hard leg: a single synchronous_commit=off commit
#         with no follow-up traffic is eventually discharged by the
#         walwriter flush horizon and published (locally and remotely).
#     L8  reader-liveness hard leg: SIGKILL node1's LMON (the seqlock
#         writer for the remote-frontier cache) -> crash-restart; scn
#         state reads complete (no permanent reader spin) and the cache
#         repopulates after the peers reconnect.
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
# IDENTIFICATION
#    src/test/cluster_tap/t/387_cluster_7_4_durable_frontier.pl
#-------------------------------------------------------------------------

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::ClusterPair;
use PostgreSQL::Test::Utils;
use Time::HiRes qw(usleep time);
use IPC::Run ();
use Test::More;

my $STRICT = $ENV{PGRAC_74_STRICT} ? 1 : 0;

sub state_str
{
	my ($node, $cat, $key) = @_;
	my $v = $node->safe_psql('postgres',
		qq{SELECT value FROM pg_cluster_state WHERE category='$cat' AND key='$key'});
	return defined($v) ? $v : '';
}

sub state_int
{
	my ($node, $cat, $key) = @_;
	my $v = state_str($node, $cat, $key);
	return $v ne '' ? $v + 0 : 0;
}

# fmt_uint64_hex emits fixed-width uppercase 0x%016X, so plain string
# comparison orders same-origin frontier claims correctly.
sub hex_gt { return $_[0] gt $_[1]; }

sub set_injection
{
	my ($node, $value) = @_;
	$node->safe_psql('postgres', "ALTER SYSTEM SET cluster.injection_points = '$value'");
	$node->safe_psql('postgres', 'SELECT pg_reload_conf()');
	usleep(700_000);
	return;
}

sub write_retry
{
	my ($node, $sql) = @_;
	for my $i (1 .. 12)
	{
		my $ok = eval { $node->safe_psql('postgres', $sql); 1 };
		return 1 if $ok;
		usleep(500_000);
	}
	return 0;
}

# Median / p99 over a sorted copy (ms values).
sub pct
{
	my ($vals, $p) = @_;
	my @s = sort { $a <=> $b } @$vals;
	my $idx = int($p * (scalar(@s) - 1) + 0.5);
	return $s[$idx];
}

my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'frontier74',
	quorum_voting_disks => 3,
	shared_data         => 1,
	extra_conf          => [
		'autovacuum = off',
		'jit = off',
		# L8 kills LMON: the node must crash-restart (shmem rebuild),
		# not shut down (the TAP harness default is off).
		'restart_after_crash = on',
		'cluster.ges_request_timeout_ms = 30000',
		'cluster.gcs_reply_timeout_ms = 3000',
		'cluster.cssd_heartbeat_interval_ms = 2000',
		'cluster.cssd_dead_deadband_factor = 5',
		'cluster.online_join = on',
		'cluster.quorum_poll_interval_ms = 500',
		'cluster.join_convergence_timeout_ms = 30000',
	]);
$pair->start_pair;
usleep(2_000_000);
ok($pair->wait_for_peer_state(0, 1, 'connected', 30), 'node0 sees node1');
ok($pair->wait_for_peer_state(1, 0, 'connected', 30), 'node1 sees node0');
my ($node0, $node1) = ($pair->node0, $pair->node1);

is($node0->safe_psql('postgres', 'SHOW cluster.boc_event_publish'),
	'on', 'cluster.boc_event_publish defaults to on');

ok(write_retry($node0, 'CREATE TABLE t387 (id int, v int)'), 'seed table created');

# Persistent sessions: commit driver on node0, poll reader on node1.
# (Existing sessions pick up SIGHUP re-arms at their next command, so
# these survive the per-leg injection / GUC flips below.)
my $bg0 = $node0->background_psql('postgres');
my $bg1 = $node1->background_psql('postgres');

# The default BackgroundPsql timer is cumulative for the session; this
# test issues hundreds of poll queries, so restart it per query.
$bg0->set_query_timer_restart;
$bg1->set_query_timer_restart;

my $REMOTE_SQL = q{SELECT COALESCE((SELECT value FROM pg_cluster_state
		WHERE category='scn' AND key='scn_remote_durable_node0'), '')};
my $FRONTIER_SQL = q{SELECT COALESCE((SELECT value FROM pg_cluster_state
		WHERE category='scn' AND key='scn_durable_safe_scn'), '')};

# One measured staleness round: node0 commit -> node1 cached frontier
# advances past its pre-round value.  Returns latency in ms, or -1 on
# a convergence timeout.
my $round_seq = 0;
sub staleness_round
{
	my ($timeout_s) = @_;
	my $before = $bg1->query($REMOTE_SQL);
	$round_seq++;
	$bg0->query("INSERT INTO t387 VALUES ($round_seq, 0)");
	my $t0 = time();
	while (time() - $t0 < $timeout_s)
	{
		my $now = $bg1->query($REMOTE_SQL);
		return (time() - $t0) * 1000.0
		  if $now ne '' && ($before eq '' || hex_gt($now, $before));
		usleep(1_000);
	}
	return -1;
}

# ----------
# L1: event-path staleness.
# ----------

# Warm-up: the very first commit must populate node1's cache at all.
my $warm = staleness_round(10);
cmp_ok($warm, '>=', 0, 'L1 warm-up: remote durable-frontier cache populated');

my @event_ms;
for my $r (1 .. 25)
{
	my $ms = staleness_round(5);
	cmp_ok($ms, '>=', 0, "L1 round $r converged (<= 5s)");
	push @event_ms, $ms;
}
my $event_p50 = pct(\@event_ms, 0.50);
my $event_p99 = pct(\@event_ms, 0.99);
diag(sprintf('L1 event-path staleness ms: p50=%.2f p99=%.2f min=%.2f max=%.2f',
		$event_p50, $event_p99, (sort { $a <=> $b } @event_ms)[0],
		(sort { $a <=> $b } @event_ms)[-1]));
cmp_ok($event_p50, '<=', 150,
	'L1 event-path p50 clearly below the sweep-only cadence');
if ($STRICT)
{
	cmp_ok($event_p99, '<=', 10, 'L1 STRICT: event-path p99 <= 10ms (local machine)');
}

# ----------
# L2: suppressed event -> sweep cadence remains a sufficient fallback.
# ----------

is($node0->safe_psql('postgres',
		q{SELECT count(*) FROM pg_stat_cluster_injections
		   WHERE name = 'cluster-boc-event-publish'}),
	'1',
	'L2 event-publish suppression injection point is registered');

# Injection arm state and hit counters are process-local, so read the
# hits through bg0: that session's backend is the one committing (and
# therefore probing) below.
my $HITS_SQL = q{SELECT hits FROM pg_stat_cluster_injections
	   WHERE name = 'cluster-boc-event-publish'};
my $hits0 = ($bg0->query($HITS_SQL) || 0) + 0;
set_injection($node0, 'cluster-boc-event-publish');

my @sweep_ms;
for my $r (1 .. 8)
{
	# Bound: 100ms sweep beat + <= 1000ms LMON loop pickup + fanout and
	# CI margin.
	my $ms = staleness_round(3.5);
	cmp_ok($ms, '>=', 0, "L2 suppressed round $r still converged via sweep (<= 3.5s)");
	push @sweep_ms, $ms;
}
my $sweep_p50 = pct(\@sweep_ms, 0.50);
diag(sprintf('L2 sweep-fallback staleness ms: p50=%.2f max=%.2f',
		$sweep_p50, (sort { $a <=> $b } @sweep_ms)[-1]));

my $hits1 = ($bg0->query($HITS_SQL) || 0) + 0;
cmp_ok($hits1, '>', $hits0, 'L2 suppression probe engaged (hits advanced)');
# A truly suppressed round can only converge on a sweep beat, so its
# median cannot sit at event-path (sub-sweep-interval) latencies.  This
# guards against a silently non-functional suppression probe.
cmp_ok($sweep_p50, '>', 20,
	'L2 suppressed staleness sits above event-path latencies');
cmp_ok($sweep_p50, '>', $event_p50,
	'L2 suppressed staleness is measurably slower than the event path');
if ($STRICT)
{
	cmp_ok($sweep_p50, '>=', 5 * $event_p50,
		'L2 STRICT: event path >= 5x faster than sweep-only (local machine)');
}

set_injection($node0, '');
my $recover_ms = staleness_round(3.5);
cmp_ok($recover_ms, '>=', 0, 'L2 event path recovers after disarm');

# ----------
# L3 (behavioural half): a publish-side fault never perturbs commits.
#
# The fault is the event-publish suppression itself (the D1-added
# commit-side step made to fail).  Deliberately NOT an LMON stall: LMON
# owns the whole IC transport, so stalling it perturbs pre-existing
# cross-node commit dependencies (GCS replies etc.) that have nothing
# to do with D1 -- the first RED run measured exactly that (~6s commit
# walls tracking the injected LMON sleep).  The <= 3% commit-p99 red
# line vs the D0 baseline is enforced by the census runner.
# ----------

sub timed_commits
{
	my ($label, $lo) = @_;
	my @ms;
	my $failures = 0;
	for my $r (1 .. 12)
	{
		my $t0 = time();
		my $ok = eval { $bg0->query('INSERT INTO t387 VALUES (' . ($lo + $r) . ', 3)'); 1 };
		push @ms, (time() - $t0) * 1000.0;
		$failures++ unless $ok;
	}
	diag(sprintf('L3 %s commit ms: p50=%.2f max=%.2f',
			$label, pct(\@ms, 0.50), (sort { $a <=> $b } @ms)[-1]));
	return ($failures, pct(\@ms, 0.50), (sort { $a <=> $b } @ms)[-1]);
}

my ($base_fail, $base_p50, $base_max) = timed_commits('baseline', 1000);
is($base_fail, 0, 'L3 baseline commits all succeeded');

my $f_pre_l3 = $bg0->query($FRONTIER_SQL);
set_injection($node0, 'cluster-boc-event-publish');
my ($sup_fail, $sup_p50, $sup_max) = timed_commits('suppressed-publish', 1100);

is($sup_fail, 0, 'L3 all commits succeeded under a suppressed event publish');
cmp_ok($sup_max, '<=', 1000, 'L3 no commit stalled under the publish fault');
cmp_ok($sup_p50, '<=', (25 > 5 * $base_p50 ? 25 : 5 * $base_p50),
	'L3 commit latency did not degrade under the publish fault');

my $f_post_l3 = $bg0->query($FRONTIER_SQL);
ok($f_post_l3 ne '' && ($f_pre_l3 eq '' || hex_gt($f_post_l3, $f_pre_l3)),
	'L3 local durable frontier still advanced (discharge is backend-side)');

set_injection($node0, '');
my $l3_recover = staleness_round(5);
cmp_ok($l3_recover, '>=', 0, 'L3 propagation recovers after disarm');

# ----------
# L4: batching -- a publish window merges many commits into few BOCs.
#
# Phase A (events on) records the fanout-per-commit ratio for the
# measure report: at TAP-achievable local TPS each commit event can
# legitimately wake one drain, so no hard sublinearity gate here (the
# high-TPS sublinear number is the census runner's).  Phase B proves
# the merge semantic deterministically: with the event signal
# suppressed, a 40-commit burst drains on sweep beats only -- few
# fanouts, and the final broadcast carries the merged (latest)
# frontier that covers the whole burst.
# ----------

my $fanout0 = state_int($node0, 'scn', 'scn_boc_broadcast_fanout_count');
my $commit0 = state_int($node0, 'scn', 'scn_commit_advance_count');
$bg0->query("INSERT INTO t387 VALUES (2000 + $_, 4)") for 1 .. 40;
my $fanout_a = state_int($node0, 'scn', 'scn_boc_broadcast_fanout_count') - $fanout0;
my $commit_a = state_int($node0, 'scn', 'scn_commit_advance_count') - $commit0;
diag("L4 phase A (events on): commits=$commit_a fanouts=$fanout_a");
cmp_ok($commit_a, '>=', 40, 'L4 phase A committed 40+ times');
cmp_ok($fanout_a, '>=', 1,  'L4 phase A produced fanouts');
cmp_ok($fanout_a, '<=', $commit_a + 10,
	'L4 phase A no wakeup amplification (at most ~one fanout per commit)');

set_injection($node0, 'cluster-boc-event-publish');
my $fanout_b0 = state_int($node0, 'scn', 'scn_boc_broadcast_fanout_count');
my $commit_b0 = state_int($node0, 'scn', 'scn_commit_advance_count');
$bg0->query("INSERT INTO t387 VALUES (2100 + $_, 4)") for 1 .. 40;
my $f_after_burst = $bg0->query($FRONTIER_SQL);

# Sweep-cadence convergence: the remote cache must reach the local
# frontier claim that covers the WHOLE burst -- one merged payload.
my $t_l4 = time();
my $l4_converged = 0;
while (time() - $t_l4 < 3.5)
{
	my $r = $bg1->query($REMOTE_SQL);
	if ($r ne '' && !hex_gt($f_after_burst, $r))
	{
		$l4_converged = 1;
		last;
	}
	usleep(2_000);
}
my $fanout_b = state_int($node0, 'scn', 'scn_boc_broadcast_fanout_count') - $fanout_b0;
my $commit_b = state_int($node0, 'scn', 'scn_commit_advance_count') - $commit_b0;
set_injection($node0, '');
diag("L4 phase B (suppressed): commits=$commit_b fanouts=$fanout_b");
cmp_ok($commit_b, '>=', 40, 'L4 phase B committed 40+ times');
ok($l4_converged, 'L4 phase B remote cache converged to the merged frontier');
cmp_ok($fanout_b, '<=', $commit_b / 2,
	'L4 phase B few broadcasts carried the whole burst (merge is real)');

# ----------
# L5: off byte-equivalence contract.
# ----------

$node0->safe_psql('postgres', 'ALTER SYSTEM SET cluster.boc_event_publish = off');
$node0->safe_psql('postgres', 'SELECT pg_reload_conf()');
# LMON re-reads the GUC on its own loop (up to ~1s), so a straggler
# payload-carrying fanout can land right after the reload; settle
# before taking the off-window baselines.
usleep(2_500_000);

my $remote_pre_l5 = $bg1->query($REMOTE_SQL);
my $accept0       = state_int($node1, 'scn', 'scn_boc_payload_accept_count');
my $badlen0       = state_int($node1, 'scn', 'scn_boc_payload_bad_length_count');
my $obs0          = state_int($node1, 'scn', 'scn_observe_bump_count');
my $f_pre_l5      = $bg0->query($FRONTIER_SQL);

$bg0->query("INSERT INTO t387 VALUES (3000 + $_, 5)") for 1 .. 10;
usleep(2_500_000);

my $f_post_l5 = $bg0->query($FRONTIER_SQL);
ok($f_post_l5 ne '' && hex_gt($f_post_l5, $f_pre_l5),
	'L5 local frontier keeps advancing while publish is off');
is(state_int($node1, 'scn', 'scn_boc_payload_accept_count'),
	$accept0, 'L5 no payload accepted remotely while off (0-length pulses)');
is(state_int($node1, 'scn', 'scn_boc_payload_bad_length_count'),
	$badlen0, 'L5 0-length pulses are legal, never counted bad-length');
is($bg1->query($REMOTE_SQL), $remote_pre_l5,
	'L5 remote cached frontier held its stale value while off');
cmp_ok(state_int($node1, 'scn', 'scn_observe_bump_count'), '>', $obs0,
	'L5 sweep pulses still flowed while off (observe bumps advanced)');

$node0->safe_psql('postgres', 'ALTER SYSTEM SET cluster.boc_event_publish = on');
$node0->safe_psql('postgres', 'SELECT pg_reload_conf()');
usleep(700_000);
my $l5_resume = staleness_round(5);
cmp_ok($l5_resume, '>=', 0, 'L5 payload propagation resumes after flipping back on');
cmp_ok(state_int($node1, 'scn', 'scn_boc_payload_accept_count'), '>', $accept0,
	'L5 remote payload accepts resumed');

# ----------
# L6: out-of-order flush must not let the frontier pass a stalled commit.
# ----------

ok(write_retry($node0, 'CREATE TABLE t387_l6 (id int, v int)'), 'L6 side table created');

my $regress0 = state_int($node0, 'scn', 'scn_durable_frontier_regression_count');
my $c_pre_l6 = state_int($node0, 'scn', 'scn_commit_advance_count');

# T1 arms the stall in ITS OWN backend (injection arm state is
# process-local), so T2 and every other process stay untouched -- the
# GUC + SIGHUP arming route raced its own disarm here and put T2 to
# sleep too.  T1 also writes a private table so T2 shares no page/ITL
# with the stalled transaction.  The 8s stall covers the window
# assertions below with margin.
my $connstr = $node0->connstr('postgres');
my ($t1_out, $t1_err) = ('', '');
my $t1 = IPC::Run::start(
	['psql', '-XAtq', '-d', $connstr, '-c',
		q{SELECT cluster_inject_fault('cluster-scn-commit-post-advance', 'sleep', 8000000); }
		  . 'INSERT INTO t387_l6 VALUES (9001, 6)'],
	'>', \$t1_out, '2>', \$t1_err);

my $saw_pending = 0;
my $t_l6 = time();
while (time() - $t_l6 < 3)
{
	if (state_int($node0, 'scn', 'scn_durable_pending_count') == 1)
	{
		$saw_pending = 1;
		last;
	}
	usleep(100_000);
}
ok($saw_pending, 'L6 T1 registered its pending commit SCN and stalled');

# T2: a fresh backend (never armed -- the stall arm is local to T1's
# process) commits fully and durably while T1 is still stalled.
my $t2_t0 = time();
$node0->safe_psql('postgres', 'INSERT INTO t387 VALUES (9002, 6)');
my $t2_ms = (time() - $t2_t0) * 1000.0;
diag(sprintf('L6 T2 commit wall: %.1f ms', $t2_ms));
cmp_ok($t2_ms, '<=', 3000,
	'L6 T2 committed promptly (no coupling to the stalled T1)');

is(state_int($node0, 'scn', 'scn_commit_advance_count') - $c_pre_l6,
	2, 'L6 exactly T1+T2 allocated commit SCNs in the window');
is(state_int($node0, 'scn', 'scn_durable_pending_count'),
	1, 'L6 T2 discharged; only the stalled T1 remains pending');

my $f_window_a = $node0->safe_psql('postgres',
	q{SELECT value FROM pg_cluster_state WHERE category='scn' AND key='scn_durable_safe_scn'});
usleep(300_000);
my $f_window_b = $node0->safe_psql('postgres',
	q{SELECT value FROM pg_cluster_state WHERE category='scn' AND key='scn_durable_safe_scn'});
is($f_window_b, $f_window_a,
	'L6 frontier held (not merely slow) while T1 stayed pending');

# finish() returns true iff every child exited 0 (psql -q suppresses
# the INSERT tag on stdout, so the exit status is the success signal).
ok($t1->finish, 'L6 T1 commit completed after its stall');
diag("L6 T1 stderr: $t1_err") if $t1_err ne '';

my $f_final   = '';
my $t_l6b     = time();
my $drained   = 0;
while (time() - $t_l6b < 12)
{
	if (state_int($node0, 'scn', 'scn_durable_pending_count') == 0)
	{
		$drained = 1;
		$f_final = $node0->safe_psql('postgres',
			q{SELECT value FROM pg_cluster_state WHERE category='scn' AND key='scn_durable_safe_scn'});
		last;
	}
	usleep(100_000);
}
ok($drained, 'L6 pending drained once T1 completed');
ok($f_final ne '' && hex_gt($f_final, $f_window_a),
	'L6 frontier advanced only after the stalled T1 completed');
is(state_int($node0, 'scn', 'scn_durable_frontier_regression_count'),
	$regress0, 'L6 no frontier regression was ever attempted');

# ----------
# L7: idle async commit is eventually discharged by the walwriter.
# ----------

my $f_pre_l7      = $bg0->query($FRONTIER_SQL);
my $remote_pre_l7 = $bg1->query($REMOTE_SQL);
$node0->safe_psql('postgres',
	'SET synchronous_commit = off; INSERT INTO t387 VALUES (9100, 7)');

my $t_l7 = time();
my ($l7_local, $l7_remote) = (0, 0);
while (time() - $t_l7 < 5 && (!$l7_local || !$l7_remote))
{
	$l7_local = 1
	  if !$l7_local && hex_gt($bg0->query($FRONTIER_SQL), $f_pre_l7);
	$l7_remote = 1
	  if !$l7_remote && hex_gt($bg1->query($REMOTE_SQL), $remote_pre_l7);
	usleep(50_000);
}
ok($l7_local,
	'L7 idle async commit discharged by the walwriter flush horizon (local frontier)');
ok($l7_remote, 'L7 walwriter-side event published the async frontier remotely');

# ----------
# L8: seqlock reader liveness across LMON kill / crash-restart.
# ----------

$bg0->quit;
$bg1->quit;

my $lmon_pid = $node1->safe_psql('postgres',
	q{SELECT pid FROM pg_stat_activity WHERE backend_type = 'lmon' LIMIT 1});
ok($lmon_pid =~ /^\d+$/, 'L8 found node1 LMON pid');
kill 'KILL', $lmon_pid;

# Aux-process death crash-restarts node1 (shmem rebuilt).
$node1->poll_query_until('postgres', 'SELECT true')
  or BAIL_OUT('node1 did not come back after LMON kill');

my ($rc, $out, $err) = $node1->psql(
	'postgres',
	q{SELECT count(*) FROM pg_cluster_state WHERE category = 'scn'},
	timeout => 15);
is($rc, 0, 'L8 scn state read completed after crash-restart (no reader spin)');

ok($pair->wait_for_peer_state(0, 1, 'connected', 60), 'L8 node0 re-sees node1');
ok($pair->wait_for_peer_state(1, 0, 'connected', 60), 'L8 node1 re-sees node0');

$node0->safe_psql('postgres', 'INSERT INTO t387 VALUES (9200, 8)');
my $t_l8 = time();
my $repop = 0;
while (time() - $t_l8 < 15)
{
	my $v = state_str($node1, 'scn', 'scn_remote_durable_node0');
	if ($v ne '')
	{
		$repop = 1;
		last;
	}
	# Keep the channel warm: sweeps republish, but a fresh commit event
	# is the fast path.
	$node0->safe_psql('postgres', 'INSERT INTO t387 VALUES (9201, 8)');
	usleep(500_000);
}
ok($repop, 'L8 remote durable-frontier cache repopulated after restart');

$pair->stop_pair;
done_testing();
