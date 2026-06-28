#-------------------------------------------------------------------------
#
# 326_cluster_5_18_survivor_apply.pl
#    spec-5.18 Hardening v1.1 (HF-1 / HF-3, INV-LF11) -- a NON-coordinator
#    survivor APPLIES a permanent removal locally, it does not merely drop its
#    refs.  On a 3-node cluster node0 (the coordinator) removes node1; node2 is
#    the non-coordinator survivor.  node2 must, at runtime (no restart):
#      - seed its OWN removed_bitmap + membership_state[node1]=REMOVED (HF-1),
#        so its in-memory permanent-removal fact is present BEFORE any restart
#        and survives the coordinator leaving (INV-LF10/LF11);
#      - permanently remaster node1-mastered shards + clear node1 GES/PCM and
#        PROVE zero leftover before it ACKs (HF-3) -- the coordinator only
#        reaches `committed` once node2's verify-backed ACK lands.
#
#    Before the fix node2's announce handler only dropped its node1 refs + ACKed
#    (never seeding removed_bitmap / membership REMOVED), so node1 was absent
#    from node2's in-memory removed set until node2 next restarted -- a
#    coordinator-switch re-admit hazard.  L-H1 keys directly on node2's view.
#
#    True consecutive-removal survivor identity (HF-4) needs a 4th survivor that
#    witnesses two removals -> forward spec-5.19 (ClusterQuad fault matrix); the
#    adopt logic is unit-covered + the single-removal survivor apply is here.
#
# IDENTIFICATION
#    src/test/cluster_tap/t/326_cluster_5_18_survivor_apply.pl
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
# NOTES
#    pgrac-original file.  Compiled paths exercised only in --enable-cluster.
#    Spec: spec-5.18-online-node-leave-fence-cleanup.md (## Hardening v1.1)
#
#-------------------------------------------------------------------------
use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../../perl";

use PostgreSQL::Test::ClusterTriple;
use Test::More;
use Time::HiRes qw(usleep);
use IPC::Run ();

sub poll_until
{
	my ($node, $query, $want, $secs, $desc) = @_;
	my $deadline = time() + $secs;
	my $last = '';
	while (time() < $deadline)
	{
		$last = $node->safe_psql('postgres', $query);
		return 1 if defined $last && $last eq $want;
		usleep(200_000);
	}
	diag("poll_until timed out ($desc): last='$last' want='$want'");
	return 0;
}

# ----------
# L1: 3 declared nodes, shared data, fast qvotec poll; clean_leave +
# online_node_removal enabled on all.  Relax the CSSD deadband so setup jitter
# never trips a false DEAD (L355).
# ----------
my $triple = PostgreSQL::Test::ClusterTriple->new_triple(
	'nr3_survivor_apply',
	quorum_voting_disks => 3,
	shared_data         => 1,
	extra_conf          => [
		'autovacuum = off',
		'cluster.clean_leave_enabled = on',
		'cluster.online_node_removal = on',
		'cluster.quorum_poll_interval_ms = 500',
		'cluster.cssd_heartbeat_interval_ms = 1000',
		'cluster.cssd_dead_deadband_factor = 8',
	]);
$triple->start_triple;
usleep(3_000_000);    # let the IC mesh + qvotec lease settle

is($triple->node0->safe_psql('postgres', 'SELECT 1'), '1', 'L1 node0 alive');
is($triple->node1->safe_psql('postgres', 'SELECT 1'), '1', 'L1 node1 alive');
is($triple->node2->safe_psql('postgres', 'SELECT 1'), '1', 'L1 node2 alive');
ok($triple->wait_for_peer_state(0, 2, 'connected', 30), 'L1 node0 sees node2 connected');
ok($triple->wait_for_peer_state(2, 0, 'connected', 30), 'L1 node2 sees node0 connected');

# ----------
# L2 PRECONDITION: node1 cooperatively clean-leaves (spec-5.13) -> dormant,
# drained member that node0 may permanently remove (INV-LF4).  node2 observes it.
# ----------
my $leave = $triple->node1->safe_psql('postgres', 'SELECT pg_cluster_clean_leave_request()');
is($leave, 'accepted', 'L2 node1 clean-leave accepted');
ok(poll_until($triple->node1,
		q{SELECT phase = 'committed' FROM pg_cluster_clean_leave_state},
		't', 40, 'L2 node1 clean-leave committed'),
	'L2 node1 clean-leave drains + commits (dormant member)');
# the coordinator (node0) observes the clean-leave; reconfig_kind in the survivor
# view is 5.13 observability not changed by this spec, so key on node0 here and on
# node2's MEMBERSHIP (the fact this round fixes) at L-H1.
ok(poll_until($triple->node0,
		q{SELECT reconfig_kind = 'clean_leave' FROM pg_cluster_reconfig_state},
		't', 40, 'L2 node0 observes clean_leave'),
	'L2 node0 observes node1 clean-departed (removable precondition)');

# ----------
# L3 PRODUCER: node0 (the min-member coordinator) permanently removes node1.
# ----------
my $req = $triple->node0->safe_psql('postgres', 'SELECT pg_cluster_remove_node(1)');
is($req, 'accepted', 'L3 node0 pg_cluster_remove_node(1) = accepted');

# the removal reaches committed ONLY after the coordinator self-verifies AND every
# survivor (node2) sends a verify-backed cleanup ACK (INV-LF3 / INV-LF11, HF-3):
# node2 could not ACK without first remastering node1-mastered shards + proving
# zero leftover, so `committed` here implies node2 did the full local apply.
ok(poll_until($triple->node0,
		q{SELECT phase = 'committed' FROM pg_cluster_node_removal_state},
		't', 60, 'L3 node0 removal reaches committed'),
	'L3 node0 removal drives to committed (requires node2 verify-backed ACK, HF-3)');
is($triple->node0->safe_psql('postgres',
		q{SELECT state FROM pg_cluster_membership WHERE node_id = 1}),
	'removed', 'L3 node0 membership_state[1] = removed (coordinator side)');

# ----------
# L-H1 (HF-1, the headline): node2 is a NON-coordinator survivor; it must have
# applied the removal into its OWN membership at runtime (no restart) -- the
# durable permanent-removal fact present in node2's memory.  Before the fix node2
# never seeded removed_bitmap / membership REMOVED, so this read would be false.
# ----------
ok(poll_until($triple->node2,
		q{SELECT removed FROM pg_cluster_membership WHERE node_id = 1},
		't', 30, 'L-H1 node2 records node1 removed'),
	'L-H1 node2 (non-coordinator survivor) records node1 removed at runtime (HF-1/INV-LF11)');
is($triple->node2->safe_psql('postgres',
		q{SELECT state FROM pg_cluster_membership WHERE node_id = 1}),
	'removed', 'L-H1 node2 membership_state[1] = removed (survivor-local apply)');

# node2's removed_epoch matches the removal epoch (it seeded with the coordinator's
# floor, not a guess) -- corroborates the seed actually happened on the survivor.
is($triple->node2->safe_psql('postgres',
		q{SELECT removed_epoch = (SELECT remove_epoch FROM pg_cluster_node_removal_state)
		  FROM pg_cluster_membership WHERE node_id = 1}),
	't', 'L-H1 node2 removed_epoch matches the removal epoch (seeded, not guessed)');

# ----------
# L-H1b: REMOVED is terminal on the survivor too -- node2 must keep node1 removed
# across subsequent lmon ticks (INV-LF1, mirrors the coordinator-side guard).
# ----------
usleep(8_000_000);
is($triple->node2->safe_psql('postgres',
		q{SELECT removed FROM pg_cluster_membership WHERE node_id = 1}),
	't', 'L-H1b node2 keeps node1 removed across lmon ticks (terminal, INV-LF1)');

# ----------
# L-H2 (INV-LF11 payoff): stop the coordinator node0.  node2 retains node1 as
# removed in its OWN memory -- the permanent-removal fact is NOT lost when the
# coordinator that drove it leaves.  Before the fix node2 never held the fact, so
# a coordinator switch would have dropped node1 from the surviving baseline (a
# re-admit hazard); now node2 carries it.
# ----------
$triple->node0->stop;
usleep(3_000_000);
ok(poll_until($triple->node2,
		q{SELECT removed FROM pg_cluster_membership WHERE node_id = 1},
		't', 30, 'L-H2 node2 still records node1 removed after coordinator node0 stops'),
	'L-H2 node2 retains the permanent-removal fact after the coordinator leaves (INV-LF11)');

# node0 already stopped above; ->stop cleared its _pid so stop_triple skips it.
$triple->stop_triple;

done_testing();
