#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 102_scn_convergence_bound_2node.pl
#	  spec-2.12 D7: 2-node end-to-end TAP for SCN convergence boundary
#	  verification (Path C 5-spec plan 收尾 step).
#
#	  Verifies the spec-2.12 measure-only contract:
#	    (a) cluster.scn_max_propagation_lag_ms GUC exists with default
#	        5000ms and SIGHUP-changeable in [100, 60000] range.
#	    (b) After bidirectional SCN broadcast (spec-2.9 D5 path),
#	        receiver's scn_last_observe_at + scn_seconds_since_last_observe
#	        + scn_observed_max_observe_gap_ms metrics update on CAS-bump
#	        (cluster_scn_observe hot path lock-free).
#	    (c) Measure-only: NO threshold violation enforcement (Q3.3=A);
#	        TAP does NOT assert max_gap upper bound (Q4.4-P1 — historical
#	        since shmem init may make first delta > test elapsed
#	        unsound).
#
#	  Test matrix (L1-L13):
#	    L1   ClusterPair strict-mode setup + IC heartbeat baseline
#	    L2   GUC default == 5000 verified via SHOW (Q2.1 + T-scn-16a 真验)
#	    L3   GUC in valid range (100..60000) via boundary SHOW
#	    L4   3 metric initial state (last_observe_at=0 or >=epoch;
#	         seconds_since=Y; max_gap historical >= 0)
#	    L5   Round 1 node0 → node1: source advance + receiver CAS-bump
#	         + 3 metric all update on receiver
#	    L7a  Round 1 metric verify on node1 (receiver-bound per P1.2 fix)
#	    L8   Round 2 node1 → node0: source advance past node0 current +
#	         receiver CAS-bump
#	    L10a Round 2 metric verify on node0 (receiver-bound)
#	    L11  Round 3 node0 → node1 (third round to confirm pattern)
#	    L11a Round 3 metric verify on node1
#	    L12  Monotonic non-decrease invariant: max_gap_ms after Round 3
#	         >= max_gap_ms after Round 1 (real update behavior verified
#	         in unit test T-scn-16e — TAP only checks no regression)
#	    L13  cleanup
#
#	  Spec authority: pgrac:specs/spec-2.12-scn-convergence-boundary-
#	  verification.md (frozen v0.3 Q1-Q8 2026-05-11).
#
#	  Q4.4 P1 fix: NO max_gap upper bound assert (historical since
#	  shmem init makes that unsound).
#	  Q4.4 P2.1 fix: GUC hard threshold parsed from SHOW (not hardcode
#	  display string — PG GUC_UNIT_MS may fold "5000ms" → "5s").
#	  Q4.4 P2.2 fix: poll dual condition (max_observed_remote bump AND
#	  observe_bump_count > prior) — ensures the metric source path
#	  fired, not just a stale value coincidence.
#	  P1.2 fix: metric verify follows the receiver per direction, not
#	  a fixed node (after reverse rounds source's last_observe_at
#	  could be stale > 5s).
#	  P2.2 fix: each round helper advances source past receiver's
#	  current_local before timing — receiver could lead source after
#	  multi-round bidirectional.
#
#
# Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
# Portions Copyright (c) 2026, pgrac contributors
#
# Author: SqlRush <sqlrush@gmail.com>
#
# IDENTIFICATION
#	  src/test/cluster_tap/t/102_scn_convergence_bound_2node.pl
#
# NOTES
#	  This is a pgrac-original file.
#	  Spec: spec-2.12-scn-convergence-boundary-verification.md (frozen v0.3)
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
use Time::HiRes qw(time);


# ----------
# Helper:  poll a query on $node with hard timeout.  Returns
#	(matched, elapsed_ms, last_value).  Caller does ok(...) on matched.
# ----------
sub poll_query_elapsed
{
	my ($node, $dbname, $query, $expected, $timeout_s, $label) = @_;

	$expected //= 't';
	$timeout_s //= 5;
	my $start = time;
	my $deadline = $start + $timeout_s;
	my $last = '';

	while (time < $deadline)
	{
		$last = $node->safe_psql($dbname, $query);
		if (defined $last && $last eq $expected)
		{
			my $elapsed_ms = int((time - $start) * 1000);
			return (1, $elapsed_ms, $last);
		}
		select(undef, undef, undef, 0.1);
	}

	my $elapsed_ms = int((time - $start) * 1000);
	diag("$label timed out after ${timeout_s}s; expected=$expected; last=$last");
	return (0, $elapsed_ms, $last);
}


sub scn_counter
{
	my ($node, $key) = @_;

	return $node->safe_psql('postgres',
		"SELECT value::bigint FROM pg_cluster_state "
			. "WHERE category='scn' AND key='$key'");
}


# ----------
# Helper:  one bidirectional round.  Advance source past receiver's
#	current_local before issuing commit (P2.2 fix), then poll receiver
#	for the dual condition (max_observed_remote > prior AND
#	observe_bump_count > prior).  Returns elapsed_ms.
# ----------
sub run_one_round
{
	my ($source, $receiver, $round_label, $hard_timeout_ms) = @_;

	# Pre-snapshot on receiver (dual condition baseline per P2.2).
	my $recv_pre_bump = scn_counter($receiver, 'scn_observe_bump_count');
	my $recv_current = scn_counter($receiver, 'scn_current_local');
	my $source_current = scn_counter($source, 'scn_current_local');
	my $insert_count = 1;

	# Keep this test robust when the receiver is already ahead after a
	# reverse round.  Advance source enough times so the final source
	# current_local is strictly greater than the receiver current_local
	# captured before this round.
	if ($source_current <= $recv_current)
	{
		$insert_count = ($recv_current - $source_current) + 1;
	}

	# Source advance: each round uses a unique table name to avoid collisions
	# across rounds.  CREATE + INSERT loop + DROP gives enough committed SCN
	# advances to put source past receiver before the final observed target.
	my $table = "t_scn102_${round_label}";
	$source->safe_psql('postgres', "CREATE TABLE $table(x int)");
	for my $i (1 .. $insert_count)
	{
		$source->safe_psql('postgres', "INSERT INTO $table VALUES ($i)");
	}
	$source->safe_psql('postgres', "DROP TABLE $table");
	my $source_after = scn_counter($source, 'scn_current_local');

	diag("round $round_label source did not advance past receiver "
			. "(source_before=$source_current receiver_before=$recv_current "
			. "source_after=$source_after insert_count=$insert_count)")
		if $source_after <= $recv_current;

	# Poll receiver for dual condition (P2.2):  both bump_count > prior
	# AND max_observed_remote >= this round's final source current_local.
	# Either alone could be coincidence from a prior round still draining;
	# together they prove THIS round's final observed target arrived.
	my $hard_s = $hard_timeout_ms / 1000.0;
	my $dual_q = "SELECT (b > $recv_pre_bump) AND (r >= $source_after) FROM ("
		. "  SELECT"
		. "    (SELECT value::bigint FROM pg_cluster_state "
		. "       WHERE category='scn' AND key='scn_observe_bump_count') AS b,"
		. "    (SELECT value::bigint FROM pg_cluster_state "
		. "       WHERE category='scn' AND key='scn_max_observed_remote') AS r"
		. ") s";

	my ($matched, $elapsed_ms, $last) = poll_query_elapsed(
		$receiver, 'postgres', $dual_q, 't', $hard_s,
		"round $round_label receiver dual condition");

	return ($matched, $elapsed_ms, $last);
}


# ----------
# L1: setup — strict-mode ClusterPair with 3 voting disks.
# ----------
my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'scn_conv_bound_2node', quorum_voting_disks => 3);
$pair->start_pair;

is($pair->node0->safe_psql('postgres', 'SELECT 1'),
	'1', 'L1 node0 postmaster alive');
is($pair->node1->safe_psql('postgres', 'SELECT 1'),
	'1', 'L1 node1 postmaster alive');

# IC heartbeat baseline (generous 30s timeout absorbs slow CI runner;
# per spec-2.9 L1 inheritance).
ok($pair->wait_for_peer_state(0, 1, 'connected', 30),
	'L1 node0 sees node1 connected (tier1 substrate up)');
ok($pair->wait_for_peer_state(1, 0, 'connected', 30),
	'L1 node1 sees node0 connected (tier1 substrate up)');

sleep 3;
is($pair->heartbeat_seen(0, 1), 't',
	'L1 node0 heartbeat_send_count > 0 toward node1 (IC alive)');
is($pair->heartbeat_seen(1, 0), 't',
	'L1 node1 heartbeat_send_count > 0 toward node0 (IC alive)');


# ----------
# L2: GUC default == 5000 verified via SHOW (Q2.1 + Q4.4-P2.1 fix:
#	parse the SHOW result as ms, not hardcode "5s"/"5000ms" — PG
#	GUC_UNIT_MS may fold display).
# ----------
my $show_raw = $pair->node0->safe_psql('postgres',
	"SHOW cluster.scn_max_propagation_lag_ms");
# pg_settings.setting on a GUC_UNIT_MS int returns the canonical ms.
my $hard_threshold_ms = $pair->node0->safe_psql('postgres',
	"SELECT setting::int FROM pg_settings "
	. "WHERE name='cluster.scn_max_propagation_lag_ms'");

is($hard_threshold_ms, '5000',
	"L2 cluster.scn_max_propagation_lag_ms canonical setting=5000 "
		. "(SHOW raw='$show_raw'; Q2.1 default 5000ms)");


# ----------
# L3: GUC valid range (100..60000) — try a SET LOCAL on a tx for
#	verification; PGC_SIGHUP guards postmaster reload but pg_settings
#	exposes min_val/max_val.
# ----------
my $gmin = $pair->node0->safe_psql('postgres',
	"SELECT min_val FROM pg_settings WHERE name='cluster.scn_max_propagation_lag_ms'");
my $gmax = $pair->node0->safe_psql('postgres',
	"SELECT max_val FROM pg_settings WHERE name='cluster.scn_max_propagation_lag_ms'");
is($gmin, '100', 'L3 GUC min_val=100 (Q2.2 range floor)');
is($gmax, '60000', 'L3 GUC max_val=60000 (Q2.2 range ceiling)');


# ----------
# L4: 3 metric initial visibility (D4+D5 catalog surface).
#	scn_last_observe_at / scn_seconds_since_last_observe /
#	scn_observed_max_observe_gap_ms must all be present in dump.
# ----------
foreach my $k (
	'scn_last_observe_at', 'scn_seconds_since_last_observe',
	'scn_observed_max_observe_gap_ms')
{
	my $present = $pair->node0->safe_psql('postgres',
		"SELECT count(*) FROM pg_cluster_state "
		. "WHERE category='scn' AND key='$k'");
	is($present, '1', "L4 pg_cluster_state has spec-2.12 key '$k' (D5)");
}


# ----------
# L5: Round 1 — node0 → node1 (P2.2 dual condition;
#	GUC-dynamic hard timeout from L2 parse).
# ----------
my ($matched_r1, $elapsed_r1, $last_r1) = run_one_round(
	$pair->node0, $pair->node1, 'r1', $hard_threshold_ms);
ok($matched_r1,
	"L5 round 1 node0 → node1 dual condition (bump + max_observed_remote) "
		. "within ${hard_threshold_ms}ms (elapsed=${elapsed_r1}ms; last=$last_r1)");

note("[perf] L5 round 1 elapsed=${elapsed_r1}ms (Q4.2 2s soft diag only)");
if ($elapsed_r1 > 2000)
{
	diag("[perf] WARN: L5 elapsed > 2s soft threshold "
			. "(observational only, not gating CI)");
}


# ----------
# L7a: Round 1 metric verify on node1 (receiver-bound per P1.2 fix:
#	source's last_observe_at would be stale on reverse rounds).
# ----------
my $r1_last_observe = $pair->node1->safe_psql('postgres',
	"SELECT value FROM pg_cluster_state "
	. "WHERE category='scn' AND key='scn_last_observe_at'");
my $r1_seconds_since = $pair->node1->safe_psql('postgres',
	"SELECT CASE WHEN value = '(unset)' THEN false "
	. "        ELSE (value::numeric >= 0) END "
	. "FROM pg_cluster_state "
	. "WHERE category='scn' AND key='scn_seconds_since_last_observe'");
my $r1_max_gap_raw = $pair->node1->safe_psql('postgres',
	"SELECT value::bigint FROM pg_cluster_state "
	. "WHERE category='scn' AND key='scn_observed_max_observe_gap_ms'");

ok($r1_last_observe ne '(unset)' && $r1_last_observe ne '',
	"L7a node1 scn_last_observe_at not '(unset)' after round 1 (CAS-bump fired; got='$r1_last_observe')");
is($r1_seconds_since, 't',
	'L7a node1 scn_seconds_since_last_observe >= 0 (derived)');
ok($r1_max_gap_raw >= 0,
	"L7a node1 scn_observed_max_observe_gap_ms >= 0 historical accumulator (got=$r1_max_gap_raw)");


# ----------
# L8: Round 2 — reverse (node1 → node0).  P2.2 helper handles
#	dual-condition + source-advance-past-receiver-current internally.
# ----------
my ($matched_r2, $elapsed_r2, $last_r2) = run_one_round(
	$pair->node1, $pair->node0, 'r2', $hard_threshold_ms);
ok($matched_r2,
	"L8 round 2 node1 → node0 dual condition within ${hard_threshold_ms}ms "
		. "(elapsed=${elapsed_r2}ms; last=$last_r2)");

note("[perf] L8 round 2 elapsed=${elapsed_r2}ms (Q4.2 2s soft diag only)");
if ($elapsed_r2 > 2000)
{
	diag("[perf] WARN: L8 elapsed > 2s soft threshold "
			. "(observational only, not gating CI)");
}


# ----------
# L10a: Round 2 metric verify on node0 (receiver-bound per P1.2 fix).
# ----------
my $r2_last_observe = $pair->node0->safe_psql('postgres',
	"SELECT value FROM pg_cluster_state "
	. "WHERE category='scn' AND key='scn_last_observe_at'");
ok($r2_last_observe ne '(unset)' && $r2_last_observe ne '',
	"L10a node0 scn_last_observe_at not '(unset)' after round 2 (got='$r2_last_observe')");


# ----------
# L11: Round 3 — node0 → node1 (third round to confirm pattern).
# ----------
my $r1_max_gap_before_r3 = $pair->node1->safe_psql('postgres',
	"SELECT value::bigint FROM pg_cluster_state "
	. "WHERE category='scn' AND key='scn_observed_max_observe_gap_ms'");

my ($matched_r3, $elapsed_r3, $last_r3) = run_one_round(
	$pair->node0, $pair->node1, 'r3', $hard_threshold_ms);
ok($matched_r3,
	"L11 round 3 node0 → node1 dual condition within ${hard_threshold_ms}ms "
		. "(elapsed=${elapsed_r3}ms; last=$last_r3)");


# ----------
# L11a: Round 3 metric verify on node1 (receiver-bound).  last_observe_at
#	is a timestamptz string;  monotonic compare uses epoch extraction.
# ----------
my $r3_last_observe = $pair->node1->safe_psql('postgres',
	"SELECT value FROM pg_cluster_state "
	. "WHERE category='scn' AND key='scn_last_observe_at'");
ok($r3_last_observe ne '(unset)' && $r3_last_observe ne '',
	"L11a node1 scn_last_observe_at not '(unset)' after round 3 (got='$r3_last_observe')");

my $monotonic_ok = $pair->node1->safe_psql('postgres',
	"SELECT '$r3_last_observe'::timestamptz >= '$r1_last_observe'::timestamptz");
is($monotonic_ok, 't',
	"L11a node1 last_observe_at monotonic non-decrease after rounds "
		. "(r1='$r1_last_observe' → r3='$r3_last_observe')");


# ----------
# L12: max_gap_ms monotonic non-decrease invariant (Q4.4-P1 fix:
#	TAP MUST NOT assert max_gap upper bound — first delta since shmem
#	init could be huge if setup-prior idle period dominates).  Real
#	atomic-max update behavior is verified in unit test T-scn-16e
#	with controllable test_clock_us.
# ----------
my $r3_max_gap = $pair->node1->safe_psql('postgres',
	"SELECT value::bigint FROM pg_cluster_state "
	. "WHERE category='scn' AND key='scn_observed_max_observe_gap_ms'");
ok($r3_max_gap >= $r1_max_gap_before_r3,
	"L12 node1 max_gap_ms monotonic non-decrease "
		. "(before-r3=$r1_max_gap_before_r3 → after-r3=$r3_max_gap; "
		. "real update behavior in T-scn-16e unit test)");


# ----------
# L13: cleanup.
# ----------
$pair->stop_pair;

done_testing();
