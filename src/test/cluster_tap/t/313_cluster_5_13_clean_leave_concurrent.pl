#-------------------------------------------------------------------------
#
# 313_cluster_5_13_clean_leave_concurrent.pl
#    spec-5.13 Hardening v1.0.1 (P1-3) -- single-leave-at-a-time is ENFORCED at
#    the wire/request layer, not merely documented.  On a 3-node cooperative
#    cluster one node (node1) starts a clean leave and is paused mid-drain (so it
#    is in progress, tracked by the survivors but not yet committed).  A SECOND
#    node that tries to leave concurrently is rejected with leave_in_progress --
#    its leave never overwrites the first leave's survivor-side serve-gate
#    protection (which would let the first leave's still-unflushed blocks be
#    served stale from storage = false-visible, 8.A).
#
#    Determinism: node1's leave is held at GES_DRAINING by a GUC-armed sleep on
#    the cluster-clean-leave-ges-drained injection (the registry is process-local,
#    so the GUC -- not a SQL arm -- reaches the request backend).  By the time it
#    pauses, node1's CLEAN_LEAVE_ANNOUNCE has already been broadcast, so every
#    survivor is tracking node1; a concurrent request is then deterministically
#    rejected (no death timing involved).
#
# IDENTIFICATION
#    src/test/cluster_tap/t/313_cluster_5_13_clean_leave_concurrent.pl
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
# NOTES
#    pgrac-original file.  Compiled paths exercised only in --enable-cluster.
#    Spec: spec-5.13-clean-leave-reconfig.md (## Hardening v1.0.1)
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

# 3 declared nodes, shared data, fast qvotec poll, clean_leave enabled on all.
my $triple = PostgreSQL::Test::ClusterTriple->new_triple(
	'cl3_concurrent',
	quorum_voting_disks => 3,
	shared_data         => 1,
	extra_conf          => [
		'autovacuum = off',
		'cluster.clean_leave_enabled = on',
		'cluster.quorum_poll_interval_ms = 500',
		'cluster.cssd_heartbeat_interval_ms = 1000',
		'cluster.cssd_dead_deadband_factor = 8',
	]);
# Hold node1's leave at GES_DRAINING via a GUC-armed sleep (30s) so it stays in
# progress (tracked by the survivors) while node2/node0 try to leave.
$triple->node1->append_conf('postgresql.conf',
	"cluster.injection_points = 'cluster-clean-leave-ges-drained:sleep:30000000'");
$triple->start_triple;
usleep(3_000_000); # let the IC mesh + qvotec lease settle

my $n0 = $triple->node0;
my $n1 = $triple->node1;
my $n2 = $triple->node2;

is($n0->safe_psql('postgres', 'SELECT 1'), '1', 'L0 node0 alive');
is($n1->safe_psql('postgres', 'SELECT 1'), '1', 'L0 node1 alive');
is($n2->safe_psql('postgres', 'SELECT 1'), '1', 'L0 node2 alive');
ok( poll_until(
		$n2, q{SELECT state = 'alive' FROM pg_cluster_cssd_peers WHERE node_id = 1},
		't', 30, 'node2 sees node1 CSSD alive'),
	'L0 node2 sees node1 CSSD alive');

# ----------
# L1: node1 starts a clean leave (async; it blocks in the sleep at ges-drained).
# ----------
my ($in, $out, $err) = ('', '', '');
my $reqh = IPC::Run::start(
	[ 'psql', '-X', '-q', '-d', $n1->connstr('postgres'),
		'-c', 'SELECT pg_cluster_clean_leave_request()' ],
	\$in, \$out, \$err);

# It must reach GES_DRAINING (paused at the inject) and the survivors must be
# tracking node1's leave before a concurrent leave is meaningful.
ok( poll_until(
		$n1, q{SELECT phase = 'ges_draining' FROM pg_cluster_clean_leave_state},
		't', 25, 'node1 paused mid-drain at ges_draining'),
	'L1 node1 paused mid-drain (leave in progress)');
ok( poll_until(
		$n0, q{SELECT leaving_node_id = 1 FROM pg_cluster_clean_leave_state},
		't', 25, 'node0 tracking node1 leave'),
	'L1 node0 (survivor) is tracking node1 leave');
ok( poll_until(
		$n2, q{SELECT leaving_node_id = 1 FROM pg_cluster_clean_leave_state},
		't', 25, 'node2 tracking node1 leave'),
	'L1 node2 (survivor) is tracking node1 leave');

# ----------
# L2 (P1-3): a SECOND node tries to leave while node1's leave is in progress ->
# rejected:leave_in_progress (request-side single-leave gate).  node2 must NOT
# overwrite its tracking of node1.
# ----------
is($n2->safe_psql('postgres', 'SELECT pg_cluster_clean_leave_request()'),
	'rejected:leave_in_progress',
	'L2 node2 concurrent leave rejected:leave_in_progress (single-leave-at-a-time)');
is($n0->safe_psql('postgres', 'SELECT pg_cluster_clean_leave_request()'),
	'rejected:leave_in_progress',
	'L2 node0 concurrent leave rejected:leave_in_progress');

# node2 still tracks node1's leave (its serve-gate protection intact, not dropped).
is($n2->safe_psql('postgres',
		q{SELECT leaving_node_id FROM pg_cluster_clean_leave_state}),
	'1',
	'L3 node2 still tracks node1 leave (serve-gate protection preserved)');
is($n2->safe_psql('postgres', q{SELECT phase FROM pg_cluster_clean_leave_state}),
	'idle',
	'L3 node2 own leave phase still idle (concurrent request did not start a leave)');

# Cleanup: kill the paused leaving node; fail-stop takes over (clean leave never
# weakens fail-stop safety).  Reap the backgrounded psql.
$n1->kill9;
eval { IPC::Run::kill_kill($reqh); };
ok( poll_until(
		$n0,
		q{SELECT state IN ('suspected','dead') FROM pg_cluster_cssd_peers WHERE node_id = 1},
		't', 40, 'node0 marks node1 dead'),
	'L4 node0 CSSD marks the killed leaving node dead (fail-stop takes over)');

$triple->stop_triple;

done_testing();
