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
#    Legs (spec-5.10 §4.2, core subset driven deterministically here):
#      L1  X waiter starved by S flood -> boosted -> later jumpers barriered
#          (grd_starvation_boost_count + grd_starvation_barrier_enqueued_count
#          both grow).
#      L3  boost never false-grants: the X waiter stays blocked while the S
#          holder is present (Rule 8.A).
#      L9  cluster.ges_starvation_max_skips tuning: 0 disables boosting (no
#          boost despite the same flood).
#      L8  runtime-off clean escape: cluster.ges_starvation_protection = off +
#          reload stops further boosting.
#      L11 low-contention sanity: an uncontended advisory lock still grants.
#
#    The barrier-deadlock detection leg (L5), the in-cycle-boosted-cancellable
#    leg (L6) and the node-dead sweep leg (L7) build a cross-node cycle through
#    the FAIRNESS_BARRIER edge / kill a node; they are layered on top of this
#    core loop and are covered at the unit level (test_cluster_grd_starvation
#    U14-U19) -- the master-side edge + sweep paths are deterministic there.
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

# Count backends on $node actively running a blocking advisory acquire.
sub n_blocked_advisory
{
	my ($node) = @_;
	my $v = $node->safe_psql('postgres', q{
		SELECT count(*) FROM pg_stat_activity
		WHERE query LIKE '%pg_advisory_lock(%' AND state = 'active'
		  AND pid <> pg_backend_pid()});
	return (defined $v && $v ne '') ? int($v) : 0;
}

# Fire N background Share-mode advisory holders that block-or-grant; returns the
# list of background handles (kept alive so the locks are held).
sub flood_shared
{
	my ($node, $key, $n) = @_;
	my @hs;
	for my $i (1 .. $n)
	{
		my $h = $node->background_psql('postgres', on_error_die => 1);
		$h->query_until(qr/PGRAC_FIRED/,
			"\\echo PGRAC_FIRED\nSELECT pg_advisory_lock_shared($key);\n");
		push @hs, $h;
		usleep(150_000);
	}
	return @hs;
}

# ----------
# L1: strict pair + shared data.
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

# node0 holds the key in SHARE mode.
my $sholder = $pair->node0->background_psql('postgres', on_error_die => 1);
$sholder->query_safe("SELECT pg_advisory_lock_shared($KEY)");

# node1 wants it EXCLUSIVE -> conflicts with the S holder -> blocks (the waiter
# that the S flood will starve).
my $xwaiter = $pair->node1->background_psql('postgres', on_error_die => 1);
$xwaiter->query_safe("SET cluster.ges_request_timeout_ms = '25s'");
$xwaiter->query_until(qr/PGRAC_FIRED/,
	"\\echo PGRAC_FIRED\nSELECT pg_advisory_lock($KEY);\n\\echo PGRAC_X_GRANTED\n");

my $blocked = 0;
for (1 .. 50) { if (n_blocked_advisory($pair->node1) >= 1) { $blocked = 1; last; } usleep(100_000); }
ok($blocked, 'L1 cross-node Exclusive advisory waiter blocks behind the Share holder');

# Flood Share-mode grants from node0: each is compatible with the S holder and
# is granted, but jumps the blocked X waiter -> skip++ -> boost at max_skips=2.
my @flood = flood_shared($pair->node0, $KEY, 6);

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
ok(n_blocked_advisory($pair->node1) >= 1,
	'L3 boost never false-grants: the Exclusive waiter stays blocked under the Share holder');

# Release everything for this leg.
$_->query_safe("SELECT pg_advisory_unlock_all()") for @flood;
$_->quit for @flood;
$sholder->query_safe("SELECT pg_advisory_unlock_all()");
# The X waiter wakes once all Share holders are gone.
ok($xwaiter->query_until(qr/PGRAC_X_GRANTED/, ""),
	'L1 the boosted Exclusive waiter is granted once the Share holders release');
$sholder->quit;
$xwaiter->query_safe("SELECT pg_advisory_unlock_all()");
$xwaiter->quit;

# ----------
# L9: max_skips = 0 disables boosting (no boost despite the same flood).
# ----------
$pair->node0->safe_psql('postgres', "ALTER SYSTEM SET cluster.ges_starvation_max_skips = 0");
$pair->node1->safe_psql('postgres', "ALTER SYSTEM SET cluster.ges_starvation_max_skips = 0");
$pair->node0->safe_psql('postgres', 'SELECT pg_reload_conf()');
$pair->node1->safe_psql('postgres', 'SELECT pg_reload_conf()');
usleep(500_000);

my $KEY9 = 5109;
my $boost_b = grd_sum($pair, 'grd_starvation_boost_count');
my $sh9 = $pair->node0->background_psql('postgres', on_error_die => 1);
$sh9->query_safe("SELECT pg_advisory_lock_shared($KEY9)");
my $xw9 = $pair->node1->background_psql('postgres', on_error_die => 1);
$xw9->query_safe("SET cluster.ges_request_timeout_ms = '25s'");
$xw9->query_until(qr/PGRAC_FIRED/,
	"\\echo PGRAC_FIRED\nSELECT pg_advisory_lock($KEY9);\n\\echo G9\n");
for (1 .. 30) { last if n_blocked_advisory($pair->node1) >= 1; usleep(100_000); }
my @flood9 = flood_shared($pair->node0, $KEY9, 6);
usleep(1_000_000);
is(grd_sum($pair, 'grd_starvation_boost_count'), $boost_b,
	'L9 max_skips = 0 disables boosting (no new boost despite the flood)');
$_->query_safe("SELECT pg_advisory_unlock_all()"), $_->quit for @flood9;
$sh9->query_safe("SELECT pg_advisory_unlock_all()");
$xw9->query_until(qr/G9/, "");
$sh9->quit;
$xw9->query_safe("SELECT pg_advisory_unlock_all()");
$xw9->quit;

# Restore protection settings.
$pair->node0->safe_psql('postgres', "ALTER SYSTEM SET cluster.ges_starvation_max_skips = 2");
$pair->node1->safe_psql('postgres', "ALTER SYSTEM SET cluster.ges_starvation_max_skips = 2");
$pair->node0->safe_psql('postgres', 'SELECT pg_reload_conf()');
$pair->node1->safe_psql('postgres', 'SELECT pg_reload_conf()');

# ----------
# L8: runtime-off clean escape -- protection off stops further boosting.
# ----------
$pair->node0->safe_psql('postgres', "ALTER SYSTEM SET cluster.ges_starvation_protection = off");
$pair->node1->safe_psql('postgres', "ALTER SYSTEM SET cluster.ges_starvation_protection = off");
$pair->node0->safe_psql('postgres', 'SELECT pg_reload_conf()');
$pair->node1->safe_psql('postgres', 'SELECT pg_reload_conf()');
usleep(500_000);

my $KEY8 = 5108;
my $boost_c = grd_sum($pair, 'grd_starvation_boost_count');
my $sh8 = $pair->node0->background_psql('postgres', on_error_die => 1);
$sh8->query_safe("SELECT pg_advisory_lock_shared($KEY8)");
my $xw8 = $pair->node1->background_psql('postgres', on_error_die => 1);
$xw8->query_safe("SET cluster.ges_request_timeout_ms = '25s'");
$xw8->query_until(qr/PGRAC_FIRED/,
	"\\echo PGRAC_FIRED\nSELECT pg_advisory_lock($KEY8);\n\\echo G8\n");
for (1 .. 30) { last if n_blocked_advisory($pair->node1) >= 1; usleep(100_000); }
my @flood8 = flood_shared($pair->node0, $KEY8, 6);
usleep(1_000_000);
is(grd_sum($pair, 'grd_starvation_boost_count'), $boost_c,
	'L8 protection off (clean escape): no new boost despite the flood');
$_->query_safe("SELECT pg_advisory_unlock_all()"), $_->quit for @flood8;
$sh8->query_safe("SELECT pg_advisory_unlock_all()");
$xw8->query_until(qr/G8/, "");
$sh8->quit;
$xw8->query_safe("SELECT pg_advisory_unlock_all()");
$xw8->quit;

# ----------
# L11: low-contention sanity -- an uncontended advisory lock still grants.
# ----------
is($pair->node0->safe_psql('postgres', 'SELECT pg_try_advisory_lock(999), pg_advisory_unlock(999)'),
	't|t', 'L11 uncontended advisory lock still grants (no fairness regression)');

$pair->stop_pair;
done_testing();
