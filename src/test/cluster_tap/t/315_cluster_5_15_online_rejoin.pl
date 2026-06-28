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
#	  L8 (Hardening v1.1, HF-1) proves the half-publish crash window end to end:
#	  a coordinator paused after the COMMITTED marker is durable but before the
#	  publish must NOT leave the joiner writable (it stays 53R60 then times out to
#	  53R61 — never writable while the cluster still sees it JOINING).
#
#	  The membership-SSOT off-isolation (§3.4), the transition 53R60 / 53R61
#	  gate and the >16-epoch catch-up are covered at the unit layer
#	  (test_cluster_reconfig / test_cluster_membership / test_cluster_epoch); the
#	  HF-1 publish-proof, HF-2 bootstrap epoch-proof and HF-3 marker identity
#	  grouping have direct unit coverage (U16-U19) in addition to L8.
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

# Poll until $node's reconfig epoch advances strictly past $floor, or time out.
# The membership-state demote and the epoch-bump publish are written sequentially
# inside one LMON tick but observed through two separate SRF round-trips, so on a
# loaded/slow runner the epoch can lag the membership demote by a tick.  Returns
# the observed epoch (> $floor on success, <= $floor on timeout).
sub poll_epoch_gt
{
	my ($node, $floor, $timeout_s, $label) = @_;
	$timeout_s //= 30;
	my $deadline = time + $timeout_s;
	my $e = current_epoch($node);
	while ($e <= $floor && time < $deadline)
	{
		usleep(200_000);
		$e = current_epoch($node);
	}
	diag("poll_epoch_gt timeout ($label): epoch=$e floor=$floor") if $e <= $floor;
	return $e;
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
			't', 60, 'node1 CSSD dead'),
		'L2-i node0 CSSD declared node1 dead');
	ok(poll_until($node0, q{SELECT state <> 'member' FROM pg_cluster_membership WHERE node_id = 1},
			't', 60, 'node1 not member'),
		'L2-ii node0 dropped node1 from the MEMBER set');
	# The membership demote (observed by L2-ii) and the epoch-bump publish are
	# written in the same LMON tick but read through two separate SRF round-trips,
	# so on a loaded runner the epoch can lag the demote by a tick -- poll for it
	# rather than reading once (the bump is what L2-v later builds on).
	my $epoch_dead = poll_epoch_gt($node0, $epoch_before, 60, 'epoch bump on leave');
	cmp_ok($epoch_dead, '>', $epoch_before, 'L2-iii epoch bumped on the leave');

	# Bring node1 back: a fresh process => a fresh (µs-clock) slot incarnation.
	# The coordinator (node0) detects the join edge, vets the fresh incarnation,
	# and drives the two-phase publish -> node1 returns to MEMBER.
	$node1->start;
	# Precondition for the join drive: the restarted peer must first be seen
	# CSSD-alive again (the transport peer-restart->reconnect chain -- the
	# substrate this spec had to fix).  Await it explicitly so a future failure
	# distinguishes "transport never reconnected" from "join drive stuck".
	# Generous convergence budgets: the online rejoin (CSSD reconnect + two-phase
	# join drive + voting-disk I/O) is correct-but-not-instant, and this test runs
	# last in a heavy reconfig shard on CI, so a loaded runner needs headroom.  The
	# polls still require the rejoin to actually happen -- they only tolerate slow.
	ok(poll_until($node0,
			q{SELECT state = 'alive' FROM pg_cluster_cssd_peers WHERE node_id = 1},
			't', 90, 'node1 CSSD alive again'),
		'L2-iiib node0 sees node1 CSSD alive again after restart (transport reconnect)');
	ok(poll_until($node0, q{SELECT state = 'member' FROM pg_cluster_membership WHERE node_id = 1},
			't', 90, 'node1 rejoined member'),
		'L2-iv node0 republished node1 as MEMBER (online rejoin, no full restart)');
	cmp_ok(current_epoch($node0), '>', $epoch_dead,
		'L2-v epoch bumped again on the JOIN reconfig');

	# Once admitted, node1's own write gate reopens -> it is writable again.
	ok(poll_until($node1,
			q{SELECT 1 FROM (SELECT pg_catalog.txid_current()) s} ,
			'1', 90, 'node1 writable after admission'),
		'L2-vi node1 write gate reopened (it can assign an xid)');

	# node1 itself observes membership MEMBER (its self-state followed the gate).
	ok(poll_until($node1, q{SELECT state = 'member' FROM pg_cluster_membership WHERE node_id = 1},
			't', 90, 'node1 self member'),
		'L2-vii node1 observes its own state as MEMBER');
}

# ----------
# L8 (Hardening v1.1, HF-1 / P1-1) — the half-publish crash window.
#
# A coordinator that has made the COMMITTED join marker majority-durable but
# crashes/stalls BEFORE the publish (epoch advance + survivor state=MEMBER) must
# NOT leave the joiner writable.  The v1.0 joiner opened its gate on the durable
# marker alone (note_self_admitted), reopening the window the r5 design claimed
# to close.  HF-1 gates gate-open on the publish-proof (a member quorum reached
# admitted_epoch), so a marker-durable-but-unpublished state keeps the joiner
# CLOSED; decision-2 then times it out to 53R61 (restart with a fresh
# incarnation) — never a "writable while the cluster still sees it JOINING".
#
# We arm a long sleep at the post-marker-durable injection point on the
# coordinator (node0): its LMON pauses inside Phase-2 commit_member with the
# COMMITTED marker durable but the publish not done, while we drive a rejoin and
# observe that node1 never becomes writable and node0 never publishes it MEMBER.
# A separate pair (the injection GUC arms every node0 backend at startup).
# ----------
{
	my @l8conf = (
		'cluster.online_join = on',
		'cluster.join_convergence_timeout_ms = 6000',
		'cluster.cssd_heartbeat_interval_ms = 500',
		'cluster.cssd_dead_deadband_factor = 6',
		'cluster.ges_request_timeout_ms = 30000',
		'autovacuum = off',
		'jit = off',
	);
	my $hp = PostgreSQL::Test::ClusterPair->new_pair(
		'spec_5_15_halfpub',
		quorum_voting_disks => 3,
		shared_data         => 1,
		extra_conf          => [@l8conf]);
	# Arm node0 to STALL right after the COMMITTED marker is durable but before
	# the publish.  40s >> the 6s join timeout, so node1 times out while node0 is
	# still paused (the registry is process-local, so the GUC — not a SQL arm — is
	# what reaches node0's LMON, mirroring t/310).
	$hp->node0->append_conf('postgresql.conf',
		"cluster.injection_points = 'cluster-reconfig-join-commit-marker-durable:sleep:40000000'");
	$hp->start_pair;
	usleep(3_000_000);
	my $n0 = $hp->node0;
	my $n1 = $hp->node1;

	ok($hp->wait_for_peer_state(0, 1, 'connected', 30), 'L8-i half-publish pair connected');
	ok(poll_until($n0, q{SELECT state = 'member' FROM pg_cluster_membership WHERE node_id = 1},
			't', 30, 'node1 member'),
		'L8-ii node1 is a MEMBER before the leave');

	# Leave then restart: node0 handles the leave normally (the inject only fires
	# in the JOIN commit), then drives the rejoin and pauses in Phase-2 with the
	# COMMITTED marker durable but unpublished.
	$n1->stop;
	ok(poll_until($n0, q{SELECT state <> 'member' FROM pg_cluster_membership WHERE node_id = 1},
			't', 60, 'node1 dropped'),
		'L8-iii node0 dropped node1 from the MEMBER set');
	$n1->start;

	# HF-1: while node0's COMMITTED marker is durable but it is paused before the
	# publish, node1's write gate must stay CLOSED (the durable marker alone does
	# NOT open it).  Probe node1 with a write (txid_current assigns an xid) and
	# require it to be refused — never silently writable.
	my $closed = 0;
	my $err = '';
	for (my $i = 0; $i < 35; $i++)    # ~7s, spanning the 6s join timeout
	{
		$err = '';
		my $rc = $n1->psql('postgres', 'SELECT txid_current()',
			stderr => \$err, on_error_stop => 0);
		if ($rc != 0 && $err =~ /joining the cluster|did not converge|rejected/)
		{
			$closed = 1;
			last;
		}
		usleep(200_000);
	}
	ok($closed,
		'L8-iv node1 write gate stayed CLOSED with the marker durable but unpublished (HF-1)');

	# No half-publish: node0 (paused before the publish) must NOT report node1 as
	# a MEMBER — node1 is not writable AND the cluster does not see it as a member.
	isnt($n0->safe_psql('postgres', q{SELECT state FROM pg_cluster_membership WHERE node_id = 1}),
		'member',
		'L8-v node0 does NOT see node1 as MEMBER (the publish never happened)');

	# decision-2: after the 6s join timeout (and still well before node0 wakes at
	# 40s), node1 latches 53R61 — a write fails FATAL, so it must restart with a
	# fresh incarnation.  This is the recovery the v1.1 design chose over re-drive.
	my $failed = 0;
	for (my $i = 0; $i < 60; $i++)    # up to ~12s, still < the 40s sleep
	{
		$err = '';
		my $rc = $n1->psql('postgres', 'SELECT txid_current()',
			stderr => \$err, on_error_stop => 0);
		if ($rc != 0 && $err =~ /did not converge|rejected/)
		{
			$failed = 1;
			last;
		}
		usleep(200_000);
	}
	ok($failed, 'L8-vi node1 timed out to 53R61 (decision-2; never half-published)');
}

done_testing();
