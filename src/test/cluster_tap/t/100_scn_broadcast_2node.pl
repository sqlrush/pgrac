#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 100_scn_broadcast_2node.pl
#	  spec-2.9 D5: 2-node end-to-end TAP for BOC broadcast skeleton.
#
#	  Walwriter on each node advances the BOC sweep cadence.  LMON drains
#	  those sweeps into PGRAC_IC_MSG_BOC_BROADCAST=3 frames because tier1
#	  IC fds are LMON process-local.  envelope.scn (frozen offset 20)
#	  piggybacks cluster_scn_current() on the wire;  receiver's
#	  cluster_ic_envelope_verify path invokes cluster_ic_envelope_observe_scn
#	  (spec-2.4 D5) which bumps scn_max_observed_remote via CAS-Lamport `>=`.
#
#	  L1-L5 cover both directions of the SCN propagation:
#	    L1 ClusterPair strict-mode setup + IC heartbeat baseline
#	    L2 node0 trigger SCN advance via a real commit
#	    L3 node1 scn_max_observed_remote > 0 within 5s hard timeout;
#	       2s diag()/note() soft perf threshold (observational only,
#	       NOT a hard ok() to avoid slow-CI-runner flake per Q8)
#	    L4 reverse direction (node1 → node0); same 5s/2s pattern
#	    L5 cleanup
#
#	  Spec authority: pgrac:specs/spec-2.9-scn-broadcast-service-
#	  activation.md (frozen v0.3 Q1-Q10 2026-05-11) — §4.2.
#
#	  Q8 nuance (user-mandated): "2s soft 不做 hard ok(elapsed < 2000)
#	  断言;改用 diag()/note() observational early-warn,不 gate CI"
#	  — slow CI runners must NOT cause flake.
#
#	  L99 nuance: PRODUCER side (walwriter alive + BOC sweep advancing)
#	  is verified before ACTOR side (scn_max_observed_remote bump).
#
#
# Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
# Portions Copyright (c) 2026, pgrac contributors
#
# Author: SqlRush <sqlrush@gmail.com>
#
# IDENTIFICATION
#	  src/test/cluster_tap/t/100_scn_broadcast_2node.pl
#
# NOTES
#	  This is a pgrac-original file.
#	  Spec: spec-2.9-scn-broadcast-service-activation.md (frozen v0.3)
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
# Helper:  poll a query on $node with 5s hard timeout (Q8).
#	Returns wall-clock elapsed_ms (float).  Caller decides pass/fail.
#	NOTE: caller MUST call ok(...) on the returned 'matched' flag;
#	the elapsed_ms is for diag()/note() reporting only (not a hard
#	assertion per Q8 修正).
# ----------
sub poll_query_elapsed
{
	my ($node, $dbname, $query, $expected, $timeout_s, $label) = @_;

	$expected //= 't';
	$timeout_s //= 5;	# spec-2.9 Q8: 5s hard timeout
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


# ----------
# L1: setup — strict-mode ClusterPair with 3 voting disks.
# ----------
my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'scn_broadcast_2node', quorum_voting_disks => 3);
$pair->start_pair;

is($pair->node0->safe_psql('postgres', 'SELECT 1'),
	'1', 'L1 node0 postmaster alive');
is($pair->node1->safe_psql('postgres', 'SELECT 1'),
	'1', 'L1 node1 postmaster alive');

# IC heartbeat baseline (per L99 substrate verify): both peers must
# see each other connected and exchanging IC heartbeats.  30s timeout
# is generous to absorb Ubuntu CI runner slow-tier1-TCP-connect (per
# L66 family: Ubuntu nightly intermittently hits 8-10s ceiling on
# postmaster startup + tier1 listener bind + active connect + HELLO
# exchange when system load accumulates over 100+ tests in the suite;
# macOS CI and local Apple silicon both <1s).  Generous timeout is
# preferable to retag spiral.
ok($pair->wait_for_peer_state(0, 1, 'connected', 30),
	'L1 node0 sees node1 connected (tier1 substrate up)');
ok($pair->wait_for_peer_state(1, 0, 'connected', 30),
	'L1 node1 sees node0 connected (tier1 substrate up)');

# Give 3s for at least one IC heartbeat round to confirm substrate
# (per spec-2.2 1Hz heartbeat default).
sleep 3;
is($pair->heartbeat_seen(0, 1), 't',
	'L1 node0 heartbeat_send_count > 0 toward node1 (IC alive)');
is($pair->heartbeat_seen(1, 0), 't',
	'L1 node1 heartbeat_send_count > 0 toward node0 (IC alive)');


# ----------
# L2: node0 trigger SCN advance via a real durable commit.
# ----------
my $node0_pre_scn = $pair->node0->safe_psql('postgres',
	'SELECT cluster_scn_current()::text');
my $node0_pre_boc_sweep = $pair->node0->safe_psql('postgres',
	"SELECT value::bigint FROM pg_cluster_state WHERE category='scn' AND key='scn_boc_sweep_count'");
my $node1_pre_remote = $pair->node1->safe_psql('postgres',
	"SELECT value::bigint FROM pg_cluster_state WHERE category='scn' AND key='scn_max_observed_remote'");

$pair->node0->safe_psql('postgres',
	'CREATE TABLE t_scn100_node0(x int)');
$pair->node0->safe_psql('postgres',
	'DROP TABLE t_scn100_node0');

my $node0_post_scn = $pair->node0->safe_psql('postgres',
	'SELECT cluster_scn_current()::text');

# PRODUCER assert (per L99):  walwriter alive on node0 + SCN actually
# advanced.  Without this, an actor failure at L3 might be wrongly
# attributed to recv-side bug.
ok($node0_post_scn ne $node0_pre_scn,
	'L2 node0 SCN advanced after commit (PRODUCER verify pre L99)');

# PRODUCER assert:  walwriter actually exists in node0 process list.
is( $pair->node0->safe_psql(
		'postgres',
		"SELECT count(*) FROM pg_stat_activity WHERE backend_type='walwriter'"),
	'1',
	'L2 node0 walwriter alive (PRODUCER verify pre L99)');

my ($matched_l2_boc, $elapsed_l2_boc, $last_l2_boc) = poll_query_elapsed(
	$pair->node0, 'postgres',
	"SELECT value::bigint > ${node0_pre_boc_sweep} FROM pg_cluster_state "
		. "WHERE category='scn' AND key='scn_boc_sweep_count'",
	't', 5, 'L2 node0 scn_boc_sweep_count advance');

ok($matched_l2_boc,
	"L2 node0 BOC sweep advanced > prior=$node0_pre_boc_sweep "
		. "within 5s (elapsed=${elapsed_l2_boc}ms)");


# ----------
# L3: poll node1 for scn_max_observed_remote > prior within 5s hard
#	timeout (Q8).  2s soft perf threshold via diag()/note() — NOT
#	a hard ok() per Q8 修正 (慢 CI runner 不抖动 flake).
# ----------
my ($matched_l3, $elapsed_l3, $last_l3) = poll_query_elapsed(
	$pair->node1, 'postgres',
	"SELECT value::bigint > ${node1_pre_remote} FROM pg_cluster_state "
		. "WHERE category='scn' AND key='scn_max_observed_remote'",
	't', 5, 'L3 node1 scn_max_observed_remote bump');

ok($matched_l3,
	"L3 node1 saw scn_max_observed_remote bump > prior=$node1_pre_remote "
		. "within 5s (elapsed=${elapsed_l3}ms)");

# Q8 2s 软 perf 提示 — 仅 diag/note,不 ok() — 不抖动慢 CI runner。
note("[perf] L3 node1 first-remote-SCN observed elapsed=${elapsed_l3}ms");
if ($elapsed_l3 > 2000)
{
	diag("[perf] WARN: L3 elapsed > 2s soft threshold "
			. "(observational only, not gating CI)");
}


# ----------
# L4: reverse direction (node1 → node0).  Same 5s hard + 2s soft
#	perf diag pattern.
# ----------
my $node0_pre_remote_l4 = $pair->node0->safe_psql('postgres',
	"SELECT value::bigint FROM pg_cluster_state WHERE category='scn' AND key='scn_max_observed_remote'");
my $node1_pre_boc_sweep_l4 = $pair->node1->safe_psql('postgres',
	"SELECT value::bigint FROM pg_cluster_state WHERE category='scn' AND key='scn_boc_sweep_count'");

$pair->node1->safe_psql('postgres',
	'CREATE TABLE t_scn100_node1(y int)');
$pair->node1->safe_psql('postgres',
	'DROP TABLE t_scn100_node1');

# PRODUCER assert on node1 walwriter alive.
is( $pair->node1->safe_psql(
		'postgres',
		"SELECT count(*) FROM pg_stat_activity WHERE backend_type='walwriter'"),
	'1',
	'L4 node1 walwriter alive (PRODUCER verify pre L99 reverse)');

my ($matched_l4_boc, $elapsed_l4_boc, $last_l4_boc) = poll_query_elapsed(
	$pair->node1, 'postgres',
	"SELECT value::bigint > ${node1_pre_boc_sweep_l4} FROM pg_cluster_state "
		. "WHERE category='scn' AND key='scn_boc_sweep_count'",
	't', 5, 'L4 node1 scn_boc_sweep_count advance (reverse)');

ok($matched_l4_boc,
	"L4 node1 BOC sweep advanced > prior=$node1_pre_boc_sweep_l4 "
		. "within 5s (elapsed=${elapsed_l4_boc}ms)");

my ($matched_l4, $elapsed_l4, $last_l4) = poll_query_elapsed(
	$pair->node0, 'postgres',
	"SELECT value::bigint > ${node0_pre_remote_l4} FROM pg_cluster_state "
		. "WHERE category='scn' AND key='scn_max_observed_remote'",
	't', 5, 'L4 node0 scn_max_observed_remote bump (reverse)');

ok($matched_l4,
	"L4 node0 saw scn_max_observed_remote bump > prior=$node0_pre_remote_l4 "
		. "within 5s (elapsed=${elapsed_l4}ms)");

note("[perf] L4 node0 first-remote-SCN observed elapsed=${elapsed_l4}ms");
if ($elapsed_l4 > 2000)
{
	diag("[perf] WARN: L4 elapsed > 2s soft threshold "
			. "(observational only, not gating CI)");
}


# ----------
# L5: cleanup.
# ----------
$pair->stop_pair;

# Final sanity: stop_pair returned cleanly (no orphaned postmaster).
# Implicit -- TAP harness will FAIL the test if a postgres process
# leaked.

done_testing();
