#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 296_grd_entry_lifecycle_reclaim_2node.pl
#    spec-6.3a -- GRD/GES entry lifecycle reclaim under cross-node churn.
#
#    The workload uses a small GRD entry cap and more distinct cross-node
#    advisory resources than the cap.  Each resource is held on node0, observed
#    as unavailable from node1, released, and then reacquired from node1.  True
#    cold reclaim keeps pg_cluster_grd_entries bounded and prevents FULL/53R71
#    style exhaustion under holderless churn.
#
# IDENTIFICATION
#    src/test/cluster_tap/t/296_grd_entry_lifecycle_reclaim_2node.pl
#
# Portions Copyright (c) 2026, pgrac contributors
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../../perl";

use PostgreSQL::Test::ClusterPair;
use Test::More;
use Time::HiRes qw(usleep);

my ($pair, $n0, $n1);

sub grd_sum
{
	my ($key) = @_;
	my $sum = 0;
	for my $node ($n0, $n1)
	{
		my $v = $node->safe_psql(
			'postgres', qq{
			SELECT coalesce((
				SELECT value::bigint FROM pg_cluster_state
				WHERE category = 'grd' AND key = '$key'), 0)});
		$sum += int($v // 0);
	}
	return $sum;
}

sub grd_entry_rows
{
	my $sum = 0;
	for my $node ($n0, $n1)
	{
		my $v = $node->safe_psql('postgres',
			q{SELECT count(*)::int FROM pg_cluster_grd_entries});
		$sum += int($v // 0);
	}
	return $sum;
}

sub try_lock
{
	my ($node, $key) = @_;
	return $node->safe_psql('postgres', "SELECT pg_try_advisory_lock($key)");
}

sub wait_until_acquirable
{
	my ($node, $key, $secs) = @_;
	my $deadline = time() + $secs;

	while (time() < $deadline)
	{
		my $v = try_lock($node, $key);
		return 1 if defined $v && $v =~ /^t/;
		usleep(100_000);
	}
	return 0;
}

$pair = PostgreSQL::Test::ClusterPair->new_pair(
	'grd_lifecycle',
	quorum_voting_disks => 3,
	shared_data         => 1,
	extra_conf          => [
		'autovacuum = off',
		'cluster.grd_max_entries = 32',
		'cluster.grd_entry_reclaim = on',
		'cluster.grd_entry_reclaim_max_per_sweep = 512',
		'cluster.cssd_heartbeat_interval_ms = 2000',
		'cluster.cssd_dead_deadband_factor = 10',
	]);
$pair->start_pair;
usleep(3_000_000);

$n0 = $pair->node0;
$n1 = $pair->node1;

my $alive0 = $n0->safe_psql('postgres', 'SELECT 1');
my $alive1 = $n1->safe_psql('postgres', 'SELECT 1');
if (($alive0 // '') ne '1' || ($alive1 // '') ne '1')
{
	$pair->stop_pair;
	plan skip_all => "cluster pair prerequisites not met (alive0=$alive0 alive1=$alive1)";
}

ok($pair->wait_for_peer_state(0, 1, 'connected', 30), 'node0 sees node1 connected');
ok($pair->wait_for_peer_state(1, 0, 'connected', 30), 'node1 sees node0 connected');

is(grd_entry_rows(), 0, 'initial GRD entry views are empty');

my $reclaimed_before = grd_sum('grd_entries_reclaimed_count');
my $full_before = grd_sum('grd_entry_full_count');
my $iters = 48;
my $blocked_ok = 1;
my $release_ok = 1;
my $holder = $n0->background_psql('postgres', on_error_die => 1);

for my $i (1 .. $iters)
{
	my $key = 2960000 + $i;

	$holder->query_safe("SELECT pg_advisory_lock($key)");
	$blocked_ok &&= ((try_lock($n1, $key) // '') =~ /^f/);
	$holder->query_safe("SELECT pg_advisory_unlock($key)");
	$release_ok &&= wait_until_acquirable($n1, $key, 10);
}
$holder->quit;

ok($blocked_ok, "node1 try-locks were blocked for all $iters held resources");
ok($release_ok, "node1 reacquired all $iters resources after release");
is(grd_entry_rows(), 0, 'cross-node cold churn leaves no live GRD entries');
ok(grd_sum('grd_entries_reclaimed_count') - $reclaimed_before >= $iters,
	'cross-node cold churn increments reclaimed counter');
is(grd_sum('grd_entry_full_count') - $full_before, 0,
	'cross-node cold churn does not hit GRD entry FULL at cap 32');
ok(grd_sum('grd_pin_high_water') >= 1, 'cross-node cold churn observes lookup pins');

$pair->stop_pair;
done_testing();
