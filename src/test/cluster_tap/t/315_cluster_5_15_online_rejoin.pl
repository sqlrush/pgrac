#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 315_cluster_5_15_online_rejoin.pl
#	  spec-5.15 D9 — online declared-node join/rejoin, 2-node e2e.
#
#	  Proves the join path end to end without a full cluster restart:
#	    L1  2-node up; node1 is MEMBER in pg_cluster_membership (the decision
#	        SSOT, INV-J8).
#	    L2  node1 stops (clean) -> node0 reconfigures it out (membership 'dead',
#	        epoch bumps) -> node1 restarts with a fresh incarnation -> the
#	        coordinator (node0) drives the two-phase online join -> node1 returns
#	        to MEMBER, the epoch bumped again, and node1 is writable once admitted
#	        (its joiner write gate reopened).  No full cluster restart.
#
#	  The membership-SSOT off-isolation (§3.4), the transition 53R60 / 53R61
#	  gate, the >16-epoch catch-up and the half-publish crash window (spec L3-L8)
#	  are covered at the unit layer (test_cluster_reconfig / test_cluster_membership
#	  / test_cluster_epoch) and tracked for a Hardening follow-up TAP.
#
#	  Local 2-node harness: PERL5LIB=$HOME/perl5/lib/perl5 + IPC::Run; the strict
#	  pair + shared data + 3 voting disks recipe (mirror t/312).
#
#	  Spec: spec-5.15-online-declared-node-join-membership.md (FROZEN v1.0)
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
# IDENTIFICATION
#	  src/test/cluster_tap/t/315_cluster_5_15_online_rejoin.pl
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../../perl";

use PostgreSQL::Test::ClusterPair;
use Test::More;
use Time::HiRes qw(usleep);

# Poll $query on $node until it returns $expected (default 't') or times out.
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

sub membership_state
{
	my ($node, $nid) = @_;
	return $node->safe_psql('postgres',
		"SELECT state FROM pg_cluster_membership WHERE node_id = $nid");
}

sub current_epoch
{
	my ($node) = @_;
	my $v = $node->safe_psql('postgres', 'SELECT new_epoch FROM pg_cluster_reconfig_state');
	$v = 0 if !defined $v || $v eq '';
	return $v + 0;
}

# Strict pair + shared data + 3 voting disks (the proven 2-node recipe), with
# online_join ON and brisk CSSD timing so the rejoin converges within the test.
my @conf = (
	'cluster.online_join = on',
	'cluster.join_convergence_timeout_ms = 30000',
	'cluster.cssd_heartbeat_interval_ms = 500',
	'cluster.cssd_dead_deadband_factor = 6',
	'cluster.ges_request_timeout_ms = 30000',
	'autovacuum = off',
	'jit = off',
);

my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'spec_5_15_join',
	quorum_voting_disks => 3,
	shared_data         => 1,
	extra_conf          => [@conf]);
$pair->start_pair;
usleep(3_000_000);
my $node0 = $pair->node0;
my $node1 = $pair->node1;

# ----------
# L1 — baseline: both up, node1 is a MEMBER in node0's membership SSOT.
# ----------
{
	is($node0->safe_psql('postgres', 'SELECT 1'), '1', 'L1-i node0 alive');
	ok($pair->wait_for_peer_state(0, 1, 'connected', 30),
		'L1-ii node0 sees peer node1 connected');
	ok(poll_until($node0,
			q{SELECT state = 'alive' FROM pg_cluster_cssd_peers WHERE node_id = 1},
			't', 30, 'node1 CSSD alive'),
		'L1-iii node0 sees node1 CSSD alive');
	ok(poll_until($node0, q{SELECT state = 'member' FROM pg_cluster_membership WHERE node_id = 1},
			't', 30, 'node1 member'),
		'L1-iv node0 reports node1 as MEMBER (decision SSOT)');
}

# ----------
# L2 — leave then ONLINE rejoin (the core claim).
# ----------
{
	my $epoch_before = current_epoch($node0);

	# Clean stop of node1 -> node0's CSSD declares it dead -> a reconfig removes
	# it from the MEMBER set (membership_state -> 'dead', epoch bumps).
	$node1->stop;
	ok(poll_until($node0,
			q{SELECT state IN ('suspected','dead') FROM pg_cluster_cssd_peers WHERE node_id = 1},
			't', 40, 'node1 CSSD dead'),
		'L2-i node0 CSSD declared node1 dead');
	ok(poll_until($node0, q{SELECT state <> 'member' FROM pg_cluster_membership WHERE node_id = 1},
			't', 40, 'node1 not member'),
		'L2-ii node0 dropped node1 from the MEMBER set');
	my $epoch_dead = current_epoch($node0);
	cmp_ok($epoch_dead, '>', $epoch_before, 'L2-iii epoch bumped on the leave');

	# Bring node1 back: a fresh process => a fresh (µs-clock) slot incarnation.
	# The coordinator (node0) detects the join edge, vets the fresh incarnation,
	# and drives the two-phase publish -> node1 returns to MEMBER.
	$node1->start;
	ok(poll_until($node0, q{SELECT state = 'member' FROM pg_cluster_membership WHERE node_id = 1},
			't', 60, 'node1 rejoined member'),
		'L2-iv node0 republished node1 as MEMBER (online rejoin, no full restart)');
	cmp_ok(current_epoch($node0), '>', $epoch_dead,
		'L2-v epoch bumped again on the JOIN reconfig');

	# Once admitted, node1's own write gate reopens -> it is writable again.
	ok(poll_until($node1,
			q{SELECT 1 FROM (SELECT pg_catalog.txid_current()) s} ,
			'1', 60, 'node1 writable after admission'),
		'L2-vi node1 write gate reopened (it can assign an xid)');

	# node1 itself observes membership MEMBER (its self-state followed the gate).
	ok(poll_until($node1, q{SELECT state = 'member' FROM pg_cluster_membership WHERE node_id = 1},
			't', 30, 'node1 self member'),
		'L2-vii node1 observes its own state as MEMBER');
}

done_testing();
