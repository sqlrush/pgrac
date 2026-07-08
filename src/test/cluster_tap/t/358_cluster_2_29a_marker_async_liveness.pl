#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 358_cluster_2_29a_marker_async_liveness.pl
#	  BUG-C1 / spec-2.29a — qvotec marker backlog must not park LMON.
#
#	  The hold injection freezes qvotec marker completion until released.  Before
#	  BUG-C1, LMON synchronously waited inside marker submit, stopped sending HELLOs,
#	  and peers could false-DEAD it during a long marker write.  This TAP holds the
#	  marker service for >=90s while an online join wants a PREPARE marker and checks:
#	    L1  hold injection is registered.
#	    L2  a true leave/restart creates a join marker backlog.
#	    L3  during a 95s hold, LMON keeps iterating and both live nodes keep each
#	        other CSSD-alive (no false-DEAD).
#	    L4  marker timeout/backlog is observable and does not publish admission.
#	    L5  releasing the hold lets the join converge.
#	    L6  an actual stopped node still becomes DEAD (no true-DEAD regression).
#
# Portions Copyright (c) 2026, pgrac contributors
#
# IDENTIFICATION
#	  src/test/cluster_tap/t/358_cluster_2_29a_marker_async_liveness.pl
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../../perl";

use PostgreSQL::Test::ClusterPair;
use Test::More;
use Time::HiRes qw(usleep);

sub poll_until
{
	my ($node, $query, $expected, $timeout_s, $label) = @_;
	$expected //= 't';
	$timeout_s //= 30;
	my $deadline = time + $timeout_s;
	my $last = '';
	while (time < $deadline)
	{
		$last = eval { $node->safe_psql('postgres', $query) } // '<err>';
		return 1 if defined $last && $last eq $expected;
		usleep(200_000);
	}
	diag("poll_until timeout ($label): last='$last' expected='$expected'");
	return 0;
}

sub state_int
{
	my ($node, $category, $key) = @_;
	my $v = $node->safe_psql('postgres',
		"SELECT COALESCE((SELECT value::bigint FROM pg_cluster_state "
	  . "WHERE category = '$category' AND key = '$key'), 0)");
	return $v + 0;
}

sub set_injection
{
	my ($node, $value) = @_;
	$node->safe_psql('postgres', "ALTER SYSTEM SET cluster.injection_points = '$value'");
	$node->safe_psql('postgres', 'SELECT pg_reload_conf()');
	usleep(700_000);
	return;
}

my @conf = (
	'autovacuum = off',
	'jit = off',
	'cluster.online_join = on',
	'cluster.join_convergence_timeout_ms = 180000',
	'cluster.quorum_poll_interval_ms = 500',
	'cluster.cssd_heartbeat_interval_ms = 1000',
	'cluster.cssd_dead_deadband_factor = 8',
	'cluster.ges_request_timeout_ms = 30000',
	'cluster.lmon_slow_iteration_warn_ms = 0',
);

my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'marker_async_liveness',
	quorum_voting_disks => 3,
	shared_data         => 1,
	extra_conf          => [@conf]);
$pair->start_pair;
usleep(3_000_000);

my $node0 = $pair->node0;
my $node1 = $pair->node1;

is($node0->safe_psql('postgres',
		q{SELECT count(*) FROM pg_stat_cluster_injections
		   WHERE name = 'cluster-qvotec-marker-service-hold'}),
	'1',
	'L1 hold injection point is registered');

ok($pair->wait_for_peer_state(0, 1, 'connected', 30), 'L1 node0 sees node1 connected');
ok($pair->wait_for_peer_state(1, 0, 'connected', 30), 'L1 node1 sees node0 connected');
ok(poll_until($node0,
		q{SELECT state = 'member' FROM pg_cluster_membership WHERE node_id = 1},
		't', 30, 'node1 initially member'),
	'L1 node1 initially MEMBER');

my $epoch0 = $node0->safe_psql('postgres', 'SELECT new_epoch FROM pg_cluster_reconfig_state') + 0;

$node1->stop;
ok(poll_until($node0,
		q{SELECT state IN ('suspected','dead') FROM pg_cluster_cssd_peers WHERE node_id = 1},
		't', 60, 'node1 true dead'),
	'L2 node0 detects the stopped node1 as dead');
ok(poll_until($node0,
		q{SELECT state <> 'member' FROM pg_cluster_membership WHERE node_id = 1},
		't', 60, 'node1 demoted from member'),
	'L2 node1 is demoted from MEMBER before rejoin');

set_injection($node0, 'cluster-qvotec-marker-service-hold');
my $lmon_iters_before = state_int($node0, 'lmon', 'lmon_slow_iter_count');
my $timeouts_before = state_int($node0, 'reconfig', 'marker_timeout_count');

$node1->start;
ok(poll_until($node0,
		q{SELECT state = 'alive' FROM pg_cluster_cssd_peers WHERE node_id = 1},
		't', 60, 'node1 CSSD alive after restart'),
	'L2 node1 is live again while marker service is held');

sleep 95;

my $lmon_iters_after = state_int($node0, 'lmon', 'lmon_slow_iter_count');
my $timeouts_after = state_int($node0, 'reconfig', 'marker_timeout_count');

cmp_ok($lmon_iters_after, '>', $lmon_iters_before + 20,
	'L3 node0 LMON keeps iterating during >=90s qvotec marker hold');
ok(poll_until($node1,
		q{SELECT state = 'alive' FROM pg_cluster_cssd_peers WHERE node_id = 0},
		't', 5, 'node1 still sees node0 alive after hold'),
	'L3 node1 still sees node0 CSSD-alive after the hold (no false-DEAD)');
ok(poll_until($node0,
		q{SELECT state = 'alive' FROM pg_cluster_cssd_peers WHERE node_id = 1},
		't', 5, 'node0 still sees node1 alive after hold'),
	'L3 node0 still sees node1 CSSD-alive after the hold');

cmp_ok($timeouts_after, '>', $timeouts_before,
	'L4 marker timeout/backlog counter advances while qvotec marker service is held');
isnt($node0->safe_psql('postgres',
		q{SELECT state FROM pg_cluster_membership WHERE node_id = 1}),
	'member',
	'L4 held/timeout marker does not publish node1 admission');

set_injection($node0, '');
ok(poll_until($node0,
		q{SELECT state = 'member' FROM pg_cluster_membership WHERE node_id = 1},
		't', 90, 'node1 joins after hold release'),
	'L5 node0 admits node1 after hold release');
ok(poll_until($node1,
		q{SELECT state = 'member' FROM pg_cluster_membership WHERE node_id = 1},
		't', 90, 'node1 self member after hold release'),
	'L5 node1 self-state reaches MEMBER after hold release');
cmp_ok($node0->safe_psql('postgres', 'SELECT new_epoch FROM pg_cluster_reconfig_state') + 0,
	'>', $epoch0,
	'L5 membership epoch advances after rejoin');

$node1->stop;
ok(poll_until($node0,
		q{SELECT state = 'dead' FROM pg_cluster_cssd_peers WHERE node_id = 1},
		't', 60, 'node1 true dead after final stop'),
	'L6 true DEAD detection still works after async marker liveness fix');
ok(poll_until($node0,
		q{SELECT state = 'dead' FROM pg_cluster_membership WHERE node_id = 1},
		't', 60, 'membership dead after final stop'),
	'L6 membership fail-stop path still demotes a truly stopped peer');

$pair->stop_pair;
done_testing();
