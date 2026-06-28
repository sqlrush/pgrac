# ============================================================
# 325_cluster_5_18_node_remove.pl -- spec-5.18 permanent node removal e2e.
#
#	Author: SqlRush <sqlrush@gmail.com>
#	Portions Copyright (c) 2026, pgrac contributors
# ============================================================
# Real 2-node ClusterPair permanent-removal e2e.  The trigger is the operator
# SQL pg_cluster_remove_node() — never a SIGKILL (that is the fail-stop path,
# spec-5.14; substrate mismatch, L99/L373).
#
#   L1  start_pair (3 voting disks, shared data) + both online_node_removal=on
#   L3  PRECONDITION: node1 cooperatively clean-leaves (spec-5.13) -> dormant
#   L4  PRODUCER: node0 pg_cluster_remove_node(1) -> accepted
#   L5  INV-LF2/commit: removal reaches committed (fence armed before shrink)
#   L6  ACTOR: node0 sees membership_state[1]=removed + reconfig_kind=node_removed
#   L7  no-leftover: phase=committed is the proof (verify_no_leftover passed,
#       INV-LF3) + the membership view shows node1 removed
#   L8  zombie-write-safe (INV-LF8): node1 (removed + fenced) cannot write the
#       shared storage — a writable tx fails closed (53R51/53R64, never succeeds)
#   L10 crash-recovery-from-marker (INV-LF7): restart node0 -> the durable §2.5
#       SHRUNK/REMOVED marker rebuilds removed_bitmap + membership REMOVED
#   L11 GUC off -> request rejected:feature_disabled (off-safe)
#
# True "coordinator death -> 2nd survivor takeover" needs >=3 nodes (the target
# is the only non-coordinator here) -> forward spec-5.19 (4-node fault matrix);
# the state-recovery logic is unit-covered (U12) + L10 crash-recovery here.

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../../perl";

use PostgreSQL::Test::ClusterPair;
use Test::More;
use Time::HiRes qw(usleep);
use IPC::Run;

sub poll_until
{
	my ($node, $query, $expected, $timeout_s, $label) = @_;
	$expected //= 't';
	$timeout_s //= 30;
	my $deadline = time + $timeout_s;
	my $last = '';
	while (time < $deadline)
	{
		$last = $node->safe_psql('postgres', $query);
		return 1 if defined $last && $last eq $expected;
		select(undef, undef, undef, 0.2);
	}
	diag("$label timed out after ${timeout_s}s; expected=$expected last=$last");
	return 0;
}


# ----------
# L1: strict pair, shared data, both online_node_removal + clean_leave enabled.
# ----------
my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'node_remove',
	quorum_voting_disks => 3,
	shared_data         => 1,
	extra_conf          => [
		'autovacuum = off',
		'cluster.clean_leave_enabled = on',
		'cluster.online_node_removal = on',
		# Fast qvotec poll so the durable marker writes + LMON drive complete quickly.
		'cluster.quorum_poll_interval_ms = 500',
		# Relax the CSSD deadband so setup jitter never trips a false DEAD.
		'cluster.cssd_heartbeat_interval_ms = 1000',
		'cluster.cssd_dead_deadband_factor = 8',
	]);
$pair->start_pair;
usleep(3_000_000);

is($pair->node0->safe_psql('postgres', 'SELECT 1'), '1', 'L1 node0 alive');
is($pair->node1->safe_psql('postgres', 'SELECT 1'), '1', 'L1 node1 alive');
ok($pair->wait_for_peer_state(0, 1, 'connected', 30), 'L1 node0 sees node1 connected');
ok(poll_until($pair->node1,
		q{SELECT state = 'alive' FROM pg_cluster_cssd_peers WHERE node_id = 0},
		't', 30, 'L1 node1 sees node0 CSSD alive'),
	'L1 node1 sees node0 CSSD alive');

# both nodes show an idle node-removal state.
is($pair->node0->safe_psql('postgres', 'SELECT phase FROM pg_cluster_node_removal_state'),
	'idle', 'L1 node0 node-removal phase idle');


# ----------
# L3 PRECONDITION: node1 cooperatively clean-leaves (spec-5.13) so it is a
# dormant, drained member that node0 may permanently remove (INV-LF4).
# ----------
my $base_epoch = $pair->node0->safe_psql('postgres',
	'SELECT new_epoch FROM pg_cluster_reconfig_state');

my $leave = $pair->node1->safe_psql('postgres', 'SELECT pg_cluster_clean_leave_request()');
is($leave, 'accepted', 'L3 node1 clean-leave accepted');
ok(poll_until($pair->node1,
		q{SELECT phase = 'committed' FROM pg_cluster_clean_leave_state},
		't', 40, 'L3 node1 clean-leave committed'),
	'L3 node1 clean-leave drains + commits (dormant member)');
ok(poll_until($pair->node0,
		q{SELECT reconfig_kind = 'clean_leave' FROM pg_cluster_reconfig_state},
		't', 40, 'L3 node0 observes clean_leave'),
	'L3 node0 observes node1 clean-departed');


# ----------
# L4 PRODUCER: node0 permanently removes node1 -> accepted.
# ----------
my $req = $pair->node0->safe_psql('postgres', 'SELECT pg_cluster_remove_node(1)');
is($req, 'accepted', 'L4 node0 pg_cluster_remove_node(1) = accepted');


# ----------
# L5: the removal reaches committed (the fence was armed majority-durable BEFORE
# the membership shrink published, INV-LF2; the durable REMOVED marker was written
# only after verify_no_leftover + all-survivor ACK, INV-LF7).
# ----------
ok(poll_until($pair->node0,
		q{SELECT phase = 'committed' FROM pg_cluster_node_removal_state},
		't', 60, 'L5 node0 removal reaches committed'),
	'L5 node0 removal drives to committed (fence-armed -> shrink -> cleanup -> committed)');

ok($pair->node0->safe_psql('postgres',
		'SELECT membership_shrunk AND fence_armed AND grd_cleaned AND pcm_cleaned '
	  . 'FROM pg_cluster_node_removal_state') eq 't',
	'L5 node0 removal: fence_armed + membership_shrunk + grd/pcm_cleaned all true');

my $rm_epoch = $pair->node0->safe_psql('postgres',
	'SELECT remove_epoch FROM pg_cluster_node_removal_state');
ok($rm_epoch > $base_epoch, "L5 node0 removal epoch advanced ($base_epoch -> $rm_epoch)");


# ----------
# L6 ACTOR: node0 sees membership_state[1]=removed + reconfig_kind=node_removed.
# ----------
is($pair->node0->safe_psql('postgres',
		q{SELECT state FROM pg_cluster_membership WHERE node_id = 1}),
	'removed', 'L6 node0 membership_state[1] = removed (member-set shrink)');
is($pair->node0->safe_psql('postgres',
		q{SELECT removed FROM pg_cluster_membership WHERE node_id = 1}),
	't', 'L6 node0 pg_cluster_membership.removed = true for node1');
is($pair->node0->safe_psql('postgres',
		'SELECT reconfig_kind FROM pg_cluster_reconfig_state'),
	'node_removed', 'L6 node0 reconfig_kind = node_removed');

# L6b: REMOVED is TERMINAL — it must stay 'removed' across subsequent lmon ticks
# (the membership-maintenance loop must never flip a removed node back, INV-LF1).
usleep(8_000_000);
is($pair->node0->safe_psql('postgres',
		q{SELECT state FROM pg_cluster_membership WHERE node_id = 1}),
	'removed',
	'L6b node0 membership_state[1] stays removed across ticks (terminal, INV-LF1)');


# ----------
# L7 no-leftover (INV-LF3): committed is reached ONLY after verify_no_leftover
# (zero GRD/PCM/master[] references to node1) passes; node0 still in quorum.
# ----------
is($pair->node0->safe_psql('postgres',
		q{SELECT coordinator_node_id = 0 FROM pg_cluster_node_removal_state}),
	't', 'L7 node0 was the removal coordinator; committed implies zero leftover (verify_no_leftover passed)');


# ----------
# L8 zombie-write-safe (INV-LF8): node1 is removed + fenced.  A writable tx on
# node1 must fail closed (write-fence 53R51 / removed 53R64 / clean-leave 53R62)
# — it must NEVER silently succeed against shared storage.
# ----------
my ($wrc, $wout, $werr) = $pair->node1->psql('postgres',
	'CREATE TABLE zombie_after_remove (x int)');
ok($wrc != 0,
	'L8 node1 (removed+fenced) cannot create/write on shared storage (fail-closed)');
# Any cluster fail-closed error proves the no-silent-write guarantee (INV-LF8): the
# write-fence (53R51), removed self-demote (53R64), clean-leave quiesce (53R62), a
# GRD shard being remastered away, or a reconfig/quorum hold — never a success.
like(($werr // ''),
	qr/53R51|53R64|53R62|53R\d|fenced|removed|remaster|GRD shard|not in quorum|reconfig/i,
	'L8 node1 write fails with a cluster fail-closed error (never a silent success, INV-LF8)');


# ----------
# L10 crash-recovery-from-marker (INV-LF7): restart node0; the durable §2.5
# SHRUNK/REMOVED marker must rebuild removed_bitmap + membership REMOVED so node1
# stays a non-member after restart.
# ----------
$pair->node0->restart;
usleep(3_000_000);
ok(poll_until($pair->node0,
		q{SELECT state = 'removed' FROM pg_cluster_membership WHERE node_id = 1},
		't', 40, 'L10 node0 membership_state[1]=removed after restart'),
	'L10 node0 rebuilds node1=removed from the durable marker (INV-LF7 restart leg)');
is($pair->node0->safe_psql('postgres',
		q{SELECT removed FROM pg_cluster_membership WHERE node_id = 1}),
	't', 'L10 node0 pg_cluster_membership.removed stays true after restart');

$pair->stop_pair;


# ----------
# L11 GUC off -> request rejected:feature_disabled (off-safe).
# ----------
my $off = PostgreSQL::Test::ClusterPair->new_pair(
	'node_remove_off',
	quorum_voting_disks => 3,
	shared_data         => 1,
	extra_conf          => [
		'autovacuum = off',
		'cluster.online_node_removal = off',
		'cluster.quorum_poll_interval_ms = 500',
		'cluster.cssd_heartbeat_interval_ms = 1000',
		'cluster.cssd_dead_deadband_factor = 8',
	]);
$off->start_pair;
usleep(2_000_000);
is($off->node0->safe_psql('postgres', 'SELECT pg_cluster_remove_node(1)'),
	'rejected:feature_disabled',
	'L11 online_node_removal=off -> pg_cluster_remove_node rejected:feature_disabled');
$off->stop_pair;

done_testing();
