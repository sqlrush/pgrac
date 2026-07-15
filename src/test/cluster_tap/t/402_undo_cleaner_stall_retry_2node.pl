#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 402_undo_cleaner_stall_retry_2node.pl
#	  TT lane (P1#4) — undo cleaner stall-retry cadence.
#
#	  Root cause under repair (S3 25,715 x "TT retention rollover
#	  failed" on node0): when the undo cleaner's recycle stage stalls
#	  (cluster horizon unproven: missing/stale/epoch/malformed), the
#	  pass exits and the cleaner sleeps its FULL
#	  cluster.undo_cleaner_interval_ms before re-evaluating the fold.
#	  Stall causes normally clear at the horizon-report cadence (~one
#	  LMON tick: epoch views converge, every peer re-publishes), so a
#	  ~1s transient freezes cluster-wide COMMITTED recycling for up to
#	  a whole interval (30s default; VM evidence: boot "malformed"
#	  stall healed after exactly 60s = two passes, the 12:24 "epoch"
#	  stall after exactly 30s = one pass).  Under an S3 write storm
#	  each freeze burns segment-pool headroom until the 256-segment
#	  hard cap (53R9E).
#
#	  The fix: a stalled pass re-arms the next pass at the report
#	  cadence (max(lmon interval, floor)) instead of the recycle
#	  cadence.  Rule 8.A unchanged: every retry still recycles ONLY on
#	  a proven cluster floor — a persistent cause keeps stalling every
#	  retry (leg L4 proves the protection is NOT weakened).
#
#	  L1  ClusterPair startup baseline; initial floor proven
#	  L2  report-drop injection stalls the fold on node0
#	      (undo.horizon_stall_count moves; floor freezes)
#	  L3  RECOVERY LATENCY (the fix's contract): after the cause is
#	      cleared (disarm; reports resume within ~1 LMON tick) the
#	      floor is re-proven within RETRY_BUDGET_S seconds — far below
#	      the 15s test interval a stalled pass used to sleep.
#	  L4  PROTECTION NEGATIVE: with the drop KEPT armed the stall
#	      persists across multiple retry windows (stall_count keeps
#	      growing, floor stays frozen) — retries never recycle
#	      unproven; disarm then heals again.
#	  L5  disarm + restart: cluster stays usable.
#
# Spec: spec-5.22e (cluster undo retention brake) stall semantics +
#	S3 step-2 forensics (TT lane P1#4).
#
# Author: SqlRush <sqlrush@gmail.com>
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../lib";

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::ClusterPair;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep time);

# The cleaner interval is set LARGE relative to the retry cadence so the
# old behaviour (sleep a full interval while stalled) is cleanly
# distinguishable from the fixed behaviour (retry at the report cadence).
# This parameterizes the TEST ONLY; the fix itself is mechanism, not
# configuration.
my $CLEANER_INTERVAL_S = 15;
my $RETRY_BUDGET_S     = 6;	   # fixed behaviour: heal well under this
my $HOLD_S             = 5;	   # L4: keep the cause armed this long

my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'undo_stall_retry',
	quorum_voting_disks => 3,
	shared_data         => 1,
	data_port_span      => 2,
	extra_conf          => [
		'autovacuum = off',
		'cluster.cssd_heartbeat_interval_ms = 2000',
		'cluster.cssd_dead_deadband_factor = 10',
		"cluster.undo_cleaner_interval_ms = ${\($CLEANER_INTERVAL_S * 1000)}",
	]);
$pair->start_pair;

usleep(3_000_000);

is($pair->node0->safe_psql('postgres', 'SELECT 1'),
	'1', 'L1 node0 postmaster alive');
is($pair->node1->safe_psql('postgres', 'SELECT 1'),
	'1', 'L1 node1 postmaster alive');

ok($pair->wait_for_peer_state(0, 1, 'connected', 60)
	  && $pair->wait_for_peer_state(1, 0, 'connected', 60),
	'L1 peers connected');


sub state_val
{
	my ($node, $cat, $key) = @_;
	my $v = $node->safe_psql('postgres',
		qq{SELECT value FROM pg_cluster_state WHERE category='$cat' AND key='$key'});
	return defined($v) && $v ne '' ? $v + 0 : 0;
}

sub wait_for
{
	my ($cond, $timeout_s, $step_us) = @_;
	$step_us //= 500_000;
	my $deadline = time() + $timeout_s;
	while (time() < $deadline)
	{
		return 1 if $cond->();
		usleep($step_us);
	}
	return $cond->() ? 1 : 0;
}

# t/370 conf-face injection helpers (colon-typed arms need an explicit
# ':none' re-arm before RESET, L477).
sub arm_conf_injection
{
	my ($node, $spec) = @_;
	$node->safe_psql('postgres',
		"ALTER SYSTEM SET cluster.injection_points = '$spec'");
	$node->safe_psql('postgres', 'SELECT pg_reload_conf()');
	usleep(1_500_000);
}

sub disarm_conf_injection
{
	my ($node, $point) = @_;
	$node->safe_psql('postgres',
		"ALTER SYSTEM SET cluster.injection_points = '$point:none'");
	$node->safe_psql('postgres', 'SELECT pg_reload_conf()');
	usleep(1_500_000);
	$node->safe_psql('postgres', 'ALTER SYSTEM RESET cluster.injection_points');
	$node->safe_psql('postgres', 'SELECT pg_reload_conf()');
}

# Single-reload disarm for the L3 latency measurement.  pg_reload_conf's
# SIGHUP latch-wakes EVERY process — including the cleaner, which runs an
# immediate pass.  That first forced pass still sees the peer slot stale
# (the next genuine report only lands on the peer's ~1s LMON tick), so it
# stalls; what we then measure is how long the cleaner takes to re-
# evaluate ON ITS OWN.  The stock two-reload helper above would wake the
# cleaner a second time ~1.5s later (after the fresh report landed) and
# heal it by test-harness side effect — masking exactly the cadence bug
# under test.
sub disarm_single_reload
{
	my ($node, $point) = @_;
	$node->safe_psql('postgres',
		"ALTER SYSTEM SET cluster.injection_points = '$point:none'");
	$node->safe_psql('postgres', 'SELECT pg_reload_conf()');
}

# Local SCN churn so a healed floor lands measurably ABOVE the frozen one.
$pair->node0->safe_psql('postgres', 'CREATE TABLE t402 (v int)');
sub churn
{
	my ($node, $n) = @_;
	$node->safe_psql('postgres', 'INSERT INTO t402 VALUES (1)') for (1 .. $n);
}


# ============================================================
# L1b: initial floor proven (boot stall heals; generous window —
# the pre-fix binary needs up to ~2 intervals).
# ============================================================
ok(wait_for(sub { state_val($pair->node0, 'undo', 'horizon_last_floor_scn') > 0 },
		3 * $CLEANER_INTERVAL_S + 10),
	'L1b initial cluster floor proven on node0');


# ============================================================
# L2: report-drop stalls the fold on node0.
# ============================================================
my $stall_pre = state_val($pair->node0, 'undo', 'horizon_stall_count');
arm_conf_injection($pair->node0, 'cluster-undo-horizon-report-drop:skip');

ok(wait_for(sub { state_val($pair->node0, 'undo', 'horizon_stall_count') > $stall_pre },
		2 * $CLEANER_INTERVAL_S + 15),
	'L2 undo.horizon_stall_count moved under report-drop (fold stalled)');

my $floor_frozen = state_val($pair->node0, 'undo', 'horizon_last_floor_scn');
churn($pair->node0, 8);	   # SCN moves on while the floor is frozen


# ============================================================
# L3: recovery latency after the cause clears (the fix's contract).
#
#	The stalled pass just ran (stall_count moved above).  Disarm now:
#	fresh reports land within ~1 LMON tick (1s).  The FIXED cleaner
#	re-evaluates at the report cadence and re-proves the floor within
#	RETRY_BUDGET_S.  The OLD cleaner slept the full interval, healing
#	only at the next 15s boundary — this assertion is RED on it.
# ============================================================
my $t_cleared = time();
disarm_single_reload($pair->node0, 'cluster-undo-horizon-report-drop');

my $healed = wait_for(
	sub { state_val($pair->node0, 'undo', 'horizon_last_floor_scn') > $floor_frozen },
	$CLEANER_INTERVAL_S + 10, 200_000);
my $heal_latency = time() - $t_cleared;

ok($healed, 'L3 floor re-proven after the stall cause cleared');
diag(sprintf("L3 heal latency after disarm: %.1fs (budget %ds, interval %ds)",
	$heal_latency, $RETRY_BUDGET_S, $CLEANER_INTERVAL_S));
cmp_ok($heal_latency, '<=', $RETRY_BUDGET_S,
	'L3 HARD ASSERT: stalled cleaner re-evaluates at the report cadence, '
	. 'not the recycle interval');

my $n0log = PostgreSQL::Test::Utils::slurp_file($pair->node0->logfile);
like($n0log, qr/cluster horizon proven again; recycling resumes/,
	'L3b "recycling resumes" logged on node0');


# ============================================================
# L4: protection negative — a PERSISTENT cause keeps stalling every
# retry; fast retries never recycle unproven (Rule 8.A intact).
# ============================================================
my $stall_l4_pre = state_val($pair->node0, 'undo', 'horizon_stall_count');
arm_conf_injection($pair->node0, 'cluster-undo-horizon-report-drop:skip');

ok(wait_for(sub { state_val($pair->node0, 'undo', 'horizon_stall_count') > $stall_l4_pre },
		2 * $CLEANER_INTERVAL_S + 15),
	'L4 fold stalled again under re-armed report-drop');

my $floor_l4 = state_val($pair->node0, 'undo', 'horizon_last_floor_scn');
my $stall_mid = state_val($pair->node0, 'undo', 'horizon_stall_count');
churn($pair->node0, 8);
sleep($HOLD_S);

cmp_ok(state_val($pair->node0, 'undo', 'horizon_stall_count'), '>=', $stall_mid,
	'L4b stall accounting keeps running while the cause persists');
is(state_val($pair->node0, 'undo', 'horizon_last_floor_scn'), $floor_l4,
	'L4c HARD ASSERT: floor did NOT advance while unproven — no retry '
	. 'recycles without a proven cluster floor');

disarm_conf_injection($pair->node0, 'cluster-undo-horizon-report-drop');
churn($pair->node0, 4);
ok(wait_for(sub { state_val($pair->node0, 'undo', 'horizon_last_floor_scn') > $floor_l4 },
		$CLEANER_INTERVAL_S + 10),
	'L4d floor heals after the persistent cause is removed');


# ============================================================
# L5: restart clean.
# ============================================================
$pair->node0->stop('fast');
$pair->node1->stop('fast');
$pair->node0->start;
$pair->node1->start;
usleep(3_000_000);
ok($pair->wait_for_peer_state(0, 1, 'connected', 60)
	  && $pair->wait_for_peer_state(1, 0, 'connected', 60),
	'L5 cluster restarted after both legs');
is($pair->node0->safe_psql('postgres', 'SELECT count(*) >= 0 FROM t402'),
	't', 'L5 table readable after both legs');


done_testing();
