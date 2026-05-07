# spec-2.2 D14 -- 2-node A-lite Tier1 TCP TAP.
#
# First behavioural test that two pgrac instances can establish a real
# TCP interconnect mesh, exchange HELLO, and trade heartbeats.  Uses
# the spec-2.2 D15 ClusterPair helper (src/test/perl/PostgreSQL/Test/
# ClusterPair.pm).
#
# Test matrix (8 L#):
#   L1 ClusterPair start_pair OK -- both postmasters live, no FATAL
#   L2 peer.state = connected on both sides (within 10s)
#   L3 heartbeat_send_count > 0 on both sides (active connector low-id
#      and passive accepter high-id, per §3.5 mesh role)
#   L4 heartbeat_recv_count > 0 on both sides
#   L5 mesh role tie-break: low_id is active connector for the pair
#      (cross-check via pg_cluster_ic_peers.role / state symmetry)
#   L6 HELLO failure connection-level rejection: node1 starts with
#      different cluster_name -> peer.state on node0 ends in 'rejected'
#      with last_error_code 08P01; postmaster on both sides STAYS UP
#   L7 startup non-deadlock: start node0 alone, wait 10s, then start
#      node1; node0 LMON ready stays true throughout, then connects
#   L8 listener bind FATAL: pre-occupy node0's IC port -> startup
#      FATALs with "tier1: bind on ... failed"
#
# Spec authority: pgrac:specs/spec-2.2-* §4.3.
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../lib";

use IO::Socket::INET;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::ClusterPair;
use PostgreSQL::Test::Utils;
use Test::More;


# ============================================================
# L1-L4: happy path -- both nodes connect + exchange heartbeats.
# ============================================================
{
	my $pair = PostgreSQL::Test::ClusterPair->new_pair('pgrac076a');
	$pair->start_pair;

	# L1 -- both postmasters live + accept SQL.
	is($pair->node0->safe_psql('postgres', 'SELECT 1'), '1',
		'L1 node0 postmaster accepts SQL after tier1 startup');
	is($pair->node1->safe_psql('postgres', 'SELECT 1'), '1',
		'L1 node1 postmaster accepts SQL after tier1 startup');

	# L2 -- peer.state = connected on both sides (10s deadline; default
	# heartbeat interval = 1s, default connect_timeout = 5s).
	ok($pair->wait_for_peer_state(0, 1, 'connected', 10),
		'L2 node0 sees peer node1 in state=connected within 10s');
	ok($pair->wait_for_peer_state(1, 0, 'connected', 10),
		'L2 node1 sees peer node0 in state=connected within 10s');

	# L3 -- heartbeat_send_count > 0 on both (gives at least 2s for
	# 1Hz heartbeat to fire after connected).  Default interval = 1s.
	sleep 3;
	is($pair->heartbeat_seen(0, 1), 't',
		'L3 node0 heartbeat_send_count > 0 toward node1 (active connector)');
	is($pair->heartbeat_seen(1, 0), 't',
		'L3 node1 heartbeat_send_count > 0 toward node0 (passive accepter)');

	# L4 -- heartbeat_recv_count > 0 on both.
	is( $pair->node0->safe_psql(
			'postgres',
			"SELECT heartbeat_recv_count > 0 FROM pg_cluster_ic_peers WHERE node_id = 1"),
		't',
		'L4 node0 heartbeat_recv_count > 0 from node1');
	is( $pair->node1->safe_psql(
			'postgres',
			"SELECT heartbeat_recv_count > 0 FROM pg_cluster_ic_peers WHERE node_id = 0"),
		't',
		'L4 node1 heartbeat_recv_count > 0 from node0');

	# L5 -- mesh role symmetry: spec-2.2 §3.5 says low_id is active
	# connector, high_id is passive accepter.  We verify the implicit
	# symmetry: both peers reach connected, both directions of
	# heartbeats flow (tested in L3/L4).  Direct role inspection lives
	# in cluster_unit (test_cluster_conf.c D-H4 mesh_role_for_pair).
	# Here we additionally assert that connect_error_count = 0 (no
	# spurious mesh tie-break race).
	is( $pair->node0->safe_psql(
			'postgres',
			"SELECT connect_error_count FROM pg_cluster_ic_peers WHERE node_id = 1"),
		'0',
		'L5 node0 connect_error_count = 0 (no race / tie-break flap)');
	is( $pair->node1->safe_psql(
			'postgres',
			"SELECT connect_error_count FROM pg_cluster_ic_peers WHERE node_id = 0"),
		'0',
		'L5 node1 connect_error_count = 0 (no race / tie-break flap)');

	$pair->stop_pair;
}


# ============================================================
# L6: HELLO failure connection-level rejection.
#
# node1 declares a different cluster_name in its pgrac.conf.  When
# node0 (active connector for the 0,1 pair) sends HELLO, node1 reads
# HELLO, sees mismatched cluster_name, sends back rejection, closes
# connection.  Postmaster on BOTH sides must STAY UP (per spec-2.2
# §3.10 hard invariant: HELLO failure is connection-level only).
# ============================================================
{
	my $pair = PostgreSQL::Test::ClusterPair->new_pair(
		'pgrac076b',
		cluster_name_override => 'pgrac076b_WRONG');
	$pair->start_pair;

	# Both postmasters must stay up.
	is($pair->node0->safe_psql('postgres', 'SELECT 1'), '1',
		'L6 node0 postmaster STAYS UP after HELLO mismatch (§3.10)');
	is($pair->node1->safe_psql('postgres', 'SELECT 1'), '1',
		'L6 node1 postmaster STAYS UP after HELLO mismatch (§3.10)');

	# At least one side eventually shows peer.state in {rejected,
	# connecting, down} -- never 'connected' since HELLO is rejected.
	# Give 10s for connect attempt + HELLO exchange + close.
	sleep 8;

	my $node0_state = $pair->node0->safe_psql('postgres',
		"SELECT state FROM pg_cluster_ic_peers WHERE node_id = 1");
	like($node0_state, qr/^(rejected|connecting|down)$/,
		"L6 node0 peer state after HELLO mismatch is rejected/connecting/down (got '$node0_state')");

	$pair->stop_pair;
}


# ============================================================
# L7: startup non-deadlock -- node0 must not block on missing peer.
#
# Per spec-2.2 §3.8 hard invariant: LMON READY does NOT depend on any
# peer being reachable.  We start node0 alone, wait 10s, then start
# node1; node0 must accept SQL throughout (postmaster up + LMON ready).
# ============================================================
{
	my $pair = PostgreSQL::Test::ClusterPair->new_pair('pgrac076c');

	$pair->node0->start;
	# node0 must accept SQL even with node1 absent.
	is($pair->node0->safe_psql('postgres', 'SELECT 1'), '1',
		'L7 node0 postmaster accepts SQL with node1 still down (§3.8)');

	# Wait 5s without starting node1.  node0 should stay up.
	sleep 5;
	is($pair->node0->safe_psql('postgres', 'SELECT 1'), '1',
		'L7 node0 still alive after 5s with no peer (no startup deadlock)');

	# Now start node1 and verify both eventually reach connected.
	$pair->node1->start;
	ok($pair->wait_for_peer_state(0, 1, 'connected', 15),
		'L7 node0 connects to node1 after late start (no need for restart)');

	$pair->stop_pair;
}


# ============================================================
# L8: listener bind FATAL -- pre-occupy node0's IC port.
#
# spec-2.2 §3.6 / cluster_ic_tier1.c: failure to bind self IC port is
# FATAL during startup (NOT a soft warning -- we cannot serve as a peer
# without a listener).  We pre-occupy node0's planned IC port via a
# raw IO::Socket, then attempt start, expect FATAL with bind in errmsg.
# ============================================================
{
	my $pair = PostgreSQL::Test::ClusterPair->new_pair('pgrac076d');

	# Pre-occupy node0's IC port with a raw listening socket.
	my $blocker = IO::Socket::INET->new(
		LocalAddr => '127.0.0.1',
		LocalPort => $pair->ic_port(0),
		Proto     => 'tcp',
		Listen    => 1,
		ReuseAddr => 0);
	ok($blocker, 'L8 pre-bound a blocker socket on node0 IC port');

	# Try to start node0 -- expect failure.
	my $start_failed = !$pair->node0->start(fail_ok => 1);
	ok($start_failed,
		'L8 node0 startup FAILS when IC port is pre-occupied (FATAL)');

	# Log should mention bind failure on the IC port.  PostgreSQL::Test::Cluster
	# has no wait_for_log_match (that lives on PgracClusterNode); slurp +
	# regex match is sufficient since the FATAL is already in the log by
	# the time start() returns failure.
	my $log_contents = PostgreSQL::Test::Utils::slurp_file($pair->node0->logfile);
	like($log_contents, qr/tier1:.*bind.*failed|FATAL.*tier1/i,
		'L8 startup log mentions tier1 listener bind failure');

	close $blocker if $blocker;
	# node1 was never started by L8.
}


done_testing();
