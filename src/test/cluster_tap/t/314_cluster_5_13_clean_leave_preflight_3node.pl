#-------------------------------------------------------------------------
#
# 314_cluster_5_13_clean_leave_preflight_3node.pl
#    spec-5.13 Hardening v1.0.2 (P1) -- the F6 mixed-mode defense is a TRUE
#    side-effect-free preflight (§3.4 two-layer), verified at 3 nodes.  node0 and
#    node1 are clean_leave_enabled; node2 is disabled.  When node1 requests a
#    clean leave, the request must be rejected SYNCHRONOUSLY with
#    peers_not_all_enabled at the layer-1 preflight (preflight=true probe) BEFORE
#    it enters REQUESTED or broadcasts the real announce -- so the ENABLED
#    survivor node0 must NOT have entered leave-aware reconfig and must NOT have
#    run cluster_grd_cleanup_on_node_dead() against the still-alive node1.
#
#    Why 3 nodes: the 2-node mixed-mode test (t/310 L11) cannot catch the side
#    effect, because the only survivor there is the disabled peer (which NAKs
#    without ever recording state or cleaning up).  Exposing the premature
#    GRD/PCM ref-drop needs a THIRD, enabled survivor that would have processed
#    a real announce.
#
# IDENTIFICATION
#    src/test/cluster_tap/t/314_cluster_5_13_clean_leave_preflight_3node.pl
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
# NOTES
#    pgrac-original file.  Compiled paths exercised only in --enable-cluster.
#    Spec: spec-5.13-clean-leave-reconfig.md (## Hardening v1.0.2)
#
#-------------------------------------------------------------------------
use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../../perl";

use PostgreSQL::Test::ClusterTriple;
use Test::More;
use Time::HiRes qw(usleep);

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

# 3 declared nodes; node0/node1 enabled, node2 DISABLED (mixed mode).
my $triple = PostgreSQL::Test::ClusterTriple->new_triple(
	'cl3_preflight',
	quorum_voting_disks => 3,
	shared_data         => 1,
	extra_conf          => [
		'autovacuum = off',
		'cluster.clean_leave_enabled = on',
		'cluster.quorum_poll_interval_ms = 500',
		'cluster.cssd_heartbeat_interval_ms = 1000',
		'cluster.cssd_dead_deadband_factor = 8',
	]);
# node2 disabled (last value in postgresql.conf wins).
$triple->node2->append_conf('postgresql.conf', 'cluster.clean_leave_enabled = off');
$triple->start_triple;
usleep(3_000_000); # IC mesh + qvotec lease settle

my $n0 = $triple->node0;
my $n1 = $triple->node1;
my $n2 = $triple->node2;

is($n0->safe_psql('postgres', 'SELECT 1'), '1', 'L0 node0 alive');
is($n1->safe_psql('postgres', 'SELECT 1'), '1', 'L0 node1 alive');
is($n2->safe_psql('postgres', 'SELECT 1'), '1', 'L0 node2 alive');
is($n2->safe_psql('postgres', q{SHOW cluster.clean_leave_enabled}),
	'off', 'L0 node2 clean_leave disabled (mixed mode)');
is($n1->safe_psql('postgres', q{SHOW cluster.clean_leave_enabled}),
	'on', 'L0 node1 clean_leave enabled');
ok( poll_until(
		$n1, q{SELECT bool_and(state = 'alive') FROM pg_cluster_cssd_peers},
		't', 30, 'node1 sees all peers CSSD alive'),
	'L0 node1 sees both peers CSSD alive');

# Sanity: both survivors start with no clean-leave tracking.
is($n0->safe_psql('postgres', q{SELECT leaving_node_id FROM pg_cluster_clean_leave_state}),
	'-1', 'L1 node0 idle (leaving_node_id = -1)');

# ----------
# L2 (P1): node1 requests a clean leave.  A disabled survivor exists (node2), so
# the layer-1 preflight rejects it SYNCHRONOUSLY before any state / side effect.
# ----------
my $req = $n1->safe_psql('postgres', 'SELECT pg_cluster_clean_leave_request()');
is($req, 'rejected:peers_not_all_enabled',
	"L2 node1 request synchronously rejected:peers_not_all_enabled (F6 layer-1 preflight; got: $req)");

# node1 never entered REQUESTED.
is($n1->safe_psql('postgres', q{SELECT phase FROM pg_cluster_clean_leave_state}),
	'idle',
	'L3 node1 phase still idle (preflight rejected before REQUESTED)');

# ----------
# L4 (P1 crux): the ENABLED survivor node0 must NOT have entered leave-aware
# reconfig — it never recorded node1 as leaving, so cluster_grd_cleanup_on_node_dead
# (the real GRD/PCM ref-drop) never ran against the still-alive node1.  A stale
# leaving_node_id here (= the v1.0.1 bug) would mean the preflight had a side effect.
# Give it a generous settle window to prove it NEVER appears (not just "not yet").
# ----------
my $polluted = 0;
for (1 .. 15)
{
	my $lv = $n0->safe_psql('postgres',
		q{SELECT leaving_node_id FROM pg_cluster_clean_leave_state});
	$polluted = 1 if defined $lv && $lv ne '-1';
	usleep(200_000);
}
is($polluted, 0,
	'L4 node0 (enabled survivor) NEVER tracked node1 leave — preflight had no side effect (P1)');
is($n0->safe_psql('postgres', q{SELECT leaving_node_id FROM pg_cluster_clean_leave_state}),
	'-1',
	'L4 node0 leaving_node_id = -1 (no GRD/PCM cleanup side effect on the still-alive node1)');

# node0 saw no clean_leave/fail_stop reconfig either.
is($n0->safe_psql('postgres', q{SELECT reconfig_kind FROM pg_cluster_reconfig_state}),
	'none',
	'L5 node0 no reconfig (mixed-mode rejected at preflight, zero cluster effect)');

# After the rejection node1 can still operate normally (it never left).
is($n1->safe_psql('postgres', 'SELECT 1'), '1', 'L6 node1 still serving (leave was rejected)');

$triple->stop_triple;

done_testing();
