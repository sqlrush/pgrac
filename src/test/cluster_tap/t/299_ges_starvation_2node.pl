#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 299_ges_starvation_2node.pl
#    spec-5.10 D10 -- real 2-node GES enqueue lock-starvation fairness e2e.
#
#    A cross-node Exclusive advisory waiter is starved by a flood of compatible
#    Share advisory grants; the GES master charges each jumped grant a skip,
#    boosts the waiter to head-of-line at the bounded threshold, and holds later
#    conflicting jumpers behind it (the FAIRNESS_BARRIER).  The boost / barrier
#    are observed through the spec-5.10 grd_starvation_* dump counters.
#
#    Legs:
#      L1  X waiter starved by an S flood -> boosted (grd_starvation_boost_count
#          grows) -> later conflicting jumpers barriered
#          (grd_starvation_barrier_enqueued_count grows).
#      L3  boost never false-grants: the X waiter stays blocked while the S
#          holder is present (Rule 8.A).
#      L11 low-contention sanity: an uncontended advisory lock still grants.
#
#    The runtime-off (L8) / max_skips=0 (L9) legs exercise the SAME master-side
#    flag/threshold logic that is deterministically covered at the unit level
#    (test_cluster_grd_starvation U3 / U20); the barrier-deadlock detection (L5),
#    in-cycle-boosted-cancellable (L6) and node-dead sweep (L7) legs are unit-
#    covered (U14-U19).  They are not re-driven here: across-node they need
#    multi-section background-psql orchestration whose async output handling is
#    fragile (L370); this test keeps the deterministic cross-node core and lets
#    stop_pair tear the background sessions down.
#
#    Harness:  ClusterPair shared_data + 3 voting disks.
#
# Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
# Portions Copyright (c) 2026, pgrac contributors
#
# Author: SqlRush <sqlrush@gmail.com>
#
# IDENTIFICATION
#    src/test/cluster_tap/t/299_ges_starvation_2node.pl
#
# NOTES
#    pgrac-original file.
#    Spec: spec-5.10-lock-starvation-fairness-protection.md (D10)
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../../perl";

use PostgreSQL::Test::ClusterPair;
use Test::More;
use Time::HiRes qw(usleep);

# Sum a 'grd'-category counter across both nodes (the GES master for the
# advisory resid owns the boost/barrier; read both and sum).
sub grd_sum
{
	my ($pair, $key) = @_;
	my $total = 0;
	for my $n ($pair->node0, $pair->node1)
	{
		my $v = $n->safe_psql('postgres', qq{
			SELECT coalesce(sum(value::bigint), 0) FROM pg_cluster_state
			WHERE category = 'grd' AND key = '$key'});
		$total += int($v) if defined $v && $v ne '';
	}
	return $total;
}

# Count backends on $node actively running a blocking exclusive advisory acquire.
sub n_blocked_x_advisory
{
	my ($node) = @_;
	my $v = $node->safe_psql('postgres', q{
		SELECT count(*) FROM pg_stat_activity
		WHERE query LIKE '%pg_advisory_lock(%' AND state = 'active'
		  AND pid <> pg_backend_pid()});
	return (defined $v && $v ne '') ? int($v) : 0;
}

# ----------
# L0: strict pair + shared data.
# ----------
my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'ges_starvation',
	quorum_voting_disks => 3,
	shared_data         => 1,
	extra_conf          => [
		'autovacuum = off',
		'cluster.ges_request_timeout_ms = 30000',
		'cluster.ges_starvation_max_skips = 2',
		# Survive CI scheduling jitter (mirror t/280).
		'cluster.cssd_heartbeat_interval_ms = 2000',
		'cluster.cssd_dead_deadband_factor = 10',
	]);
$pair->start_pair;
usleep(3_000_000);

is($pair->node0->safe_psql('postgres', 'SELECT 1'), '1', 'L0 node0 alive');
is($pair->node1->safe_psql('postgres', 'SELECT 1'), '1', 'L0 node1 alive');
ok($pair->wait_for_peer_state(0, 1, 'connected', 30), 'L0 node0 sees node1 connected');
ok($pair->wait_for_peer_state(1, 0, 'connected', 30), 'L0 node1 sees node0 connected');

my $KEY = 5100;

# ----------
# L1: X waiter starved by an S flood -> boosted -> later jumper barriered.
# ----------
my $boost0 = grd_sum($pair, 'grd_starvation_boost_count');
my $barr0  = grd_sum($pair, 'grd_starvation_barrier_enqueued_count');

# node0 holds the key in SHARE mode (synchronous grant; output consumed).
my $sholder = $pair->node0->background_psql('postgres', on_error_die => 1);
$sholder->query_safe("SELECT pg_advisory_lock_shared($KEY)");

# node1 wants it EXCLUSIVE -> conflicts with the S holder -> blocks (the waiter
# that the S flood will starve).  Fired async (it blocks); its output is left
# unconsumed deliberately -- stop_pair tears it down at the end.
my $xwaiter = $pair->node1->background_psql('postgres', on_error_die => 1);
$xwaiter->query_safe("SET cluster.ges_request_timeout_ms = '25s'");
$xwaiter->query_until(qr/PGRAC_FIRED/,
	"\\echo PGRAC_FIRED\nSELECT pg_advisory_lock($KEY);\n");

my $blocked = 0;
for (1 .. 50) { if (n_blocked_x_advisory($pair->node1) >= 1) { $blocked = 1; last; } usleep(100_000); }
ok($blocked, 'L1 cross-node Exclusive advisory waiter blocks behind the Share holder');

# Flood Share-mode grants from node0: each is compatible with the S holder and
# is granted, but jumps the blocked X waiter -> skip++ -> boost at max_skips=2;
# once boosted, later S jumpers are barriered.  Each flood session is fired
# async (post-boost ones block as barriered) and is NOT cleaned up here.
my @flood;
for my $i (1 .. 6)
{
	my $h = $pair->node0->background_psql('postgres', on_error_die => 1);
	$h->query_until(qr/PGRAC_FIRED/,
		"\\echo PGRAC_FIRED\nSELECT pg_advisory_lock_shared($KEY);\n");
	push @flood, $h;
	usleep(200_000);
}

my $boosted = 0;
for (1 .. 50)
{
	if (grd_sum($pair, 'grd_starvation_boost_count') > $boost0) { $boosted = 1; last; }
	usleep(100_000);
}
ok($boosted, 'L1 the starved Exclusive waiter is boosted to head-of-line');
cmp_ok(grd_sum($pair, 'grd_starvation_barrier_enqueued_count'), '>', $barr0,
	'L1 a later conflicting jumper is held behind the boosted waiter (FAIRNESS_BARRIER)');

# ----------
# L3: boost never false-grants -- the X waiter is still blocked (the S holder
# is present, so granting X would violate the conflict matrix).
# ----------
ok(n_blocked_x_advisory($pair->node1) >= 1,
	'L3 boost never false-grants: the Exclusive waiter stays blocked under the Share holder');

# ----------
# L11: low-contention sanity -- an uncontended advisory lock still grants (fresh
# connections; independent key; robust to the held flood above).
# ----------
is($pair->node1->safe_psql('postgres',
		'SELECT pg_try_advisory_lock(999), pg_advisory_unlock(999)'),
	't|t', 'L11 uncontended advisory lock still grants (no fairness regression)');

$pair->stop_pair;
done_testing();
