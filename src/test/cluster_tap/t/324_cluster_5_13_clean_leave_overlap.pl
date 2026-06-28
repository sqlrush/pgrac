# 324_cluster_5_13_clean_leave_overlap.pl
#
# spec-5.13 clean-leave reconfiguration — Hardening v1.0.4 (P1-1).
#
#   request-preflight vs another node's real announce OVERLAP.  Before v1.0.4,
#   request_in_progress only serialized two same-node requests; it did NOT stop
#   an incoming real announce from making this node a survivor of someone else's
#   leave WHILE this node is itself mid-request (leaving_node_id still -1 during
#   the multi-second preflight window).  The node would accept the other leave
#   (leaving_node_id := other), then its own self-bind would overwrite it with
#   self — silently dropping the other leave's serve-gate protection (8.A
#   false-visible).  t/313 only covers the "already tracking" race, not this
#   preflight overlap.
#
#   v1.0.4 fix: the announce handler NAKs LEAVE_IN_PROGRESS when this node's
#   request_in_progress != 0 (or a membership join is in flight), and the request
#   re-checks leaving_node_id under the lock before binding self.  So the second
#   node's leave is rejected and this node never tracks it.
#
#   Setup: 3 nodes, all clean_leave_enabled.  node1 holds its own request in the
#   cluster-clean-leave-request injection (:sleep, BEFORE preflight, so
#   request_in_progress=1 and leaving_node_id=-1).  While it is held, node0
#   requests a clean leave — its real announce reaches node1 mid-request.
#
# Author: SqlRush <sqlrush@gmail.com>
#
# IDENTIFICATION
#     src/test/cluster_tap/t/324_cluster_5_13_clean_leave_overlap.pl

use strict;
use warnings;
use FindBin;
use lib "$FindBin::RealBin/../../perl";

use PostgreSQL::Test::ClusterTriple;
use Test::More;
use Time::HiRes qw(usleep);
use IPC::Run;

sub leave_col
{
	my ($node, $col) = @_;
	return $node->safe_psql('postgres',
		"SELECT $col FROM pg_cluster_clean_leave_state");
}

my $triple = PostgreSQL::Test::ClusterTriple->new_triple(
	'cl_v104_overlap',
	quorum_voting_disks => 3,
	shared_data         => 1,
	extra_conf          => [
		'autovacuum = off',
		'cluster.clean_leave_enabled = on',
		'cluster.quorum_poll_interval_ms = 500',
		'cluster.cssd_heartbeat_interval_ms = 1000',
		'cluster.cssd_dead_deadband_factor = 8',
	]);

# node1 holds its own request inside the request injection (after it reserves
# request_in_progress, before preflight) so an incoming announce overlaps it.
$triple->node1->append_conf('postgresql.conf',
	"cluster.injection_points = 'cluster-clean-leave-request:sleep:9000000'");
$triple->start_triple;
usleep(3_000_000);

my $n0 = $triple->node0;
my $n1 = $triple->node1;
my $n2 = $triple->node2;

is($n0->safe_psql('postgres', 'SELECT 1'), '1', 'L0 node0 alive');
is($n1->safe_psql('postgres', 'SELECT 1'), '1', 'L0 node1 alive');
is($n2->safe_psql('postgres', 'SELECT 1'), '1', 'L0 node2 alive');
is(leave_col($n1, 'leaving_node_id'), '-1', 'L1 node1 idle (leaving_node_id = -1)');

# Fire node1's own request async — it reserves request_in_progress then sleeps
# ~9s inside the request injection (leaving_node_id still -1, mid-request).
my ($in1, $out1, $err1) = ('', '', '');
my $h1 = IPC::Run::start(
	[ 'psql', '-X', '-q', '-t', '-A', '-d', $n1->connstr('postgres'),
		'-c', 'SELECT pg_cluster_clean_leave_request()' ],
	\$in1, \$out1, \$err1);

# Give node1 time to enter the request + reserve (well inside the 9s window).
usleep(2_500_000);

# node0 now requests a clean leave; its real announce reaches node1 mid-request.
my $req0 = $n0->safe_psql('postgres', 'SELECT pg_cluster_clean_leave_request()');
is( $req0, 'rejected:leave_in_progress',
	'L2 (P1-1) node0 leave rejected:leave_in_progress — node1 NAKs while mid-request');

# The crux: node1 must NOT have become a survivor of node0's leave.  It is still
# mid-request (held in the sleep), so leaving_node_id is -1, never node0's id.
is(leave_col($n1, 'leaving_node_id'), '-1',
	'L3 (P1-1 crux) node1 never tracked node0 (no survivor-tracking overwrite, 8.A)');
is(leave_col($n1, 'phase'), 'idle',
	'L4 node1 still idle (its own request still held in preflight)');

# Reap node1's own request: now that node0 aborted, node1 has two healthy
# survivors and leaves cleanly (proving its request was unharmed by the overlap).
IPC::Run::finish($h1);
$out1 =~ s/\s+$//;
is($out1, 'accepted', 'L5 node1 own clean leave still accepted after the overlap');

done_testing();
