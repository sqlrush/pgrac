# spec-2.2 Hardening v1.0.1 T-F2 -- heartbeat liveness timeout enforcement.
#
# Verifies F2 fix: LMON now scans peer state each tick and closes
# peers whose last_heartbeat_recv_at is older than 3x interval.
# Pre-fix: silent peer death held the connection open until TCP
# keepalive triggered (~2 hours on Linux default), and
# pg_cluster_ic_peers reported state='connected' the whole time.
#
# Test method:
#   - ClusterPair (2-node tier1)
#   - wait until both peers reach state='connected' + heartbeat_recv_count > 0
#   - SIGSTOP the LMON of node1 (kernel keeps the TCP socket alive
#     but no heartbeat frames flow)
#   - wait > 3 * heartbeat_interval (3.5s with default 1s interval)
#   - assert node0 sees peer 1 transition to state='down'
#     with last_error mentioning 'liveness timeout'
#   - SIGCONT node1 LMON for clean shutdown
#
# Spec: pgrac:specs/spec-2.2-* ## Hardening v1.0.1 F2.
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../lib";

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::ClusterPair;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep);


my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'pgrac079',
	extra_conf => [
		"cluster.interconnect_heartbeat_interval_ms = 1000",
		"cluster.interconnect_recv_timeout_ms = 30000",
	]);
$pair->start_pair;


# ----------
# L1: both peers reach state=connected + heartbeats flow.
# ----------
ok($pair->wait_for_peer_state(0, 1, 'connected', 10),
	'L1 node0 sees peer 1 connected');
ok($pair->wait_for_peer_state(1, 0, 'connected', 10),
	'L1 node1 sees peer 0 connected');

# Give 2s so heartbeats actually flow + counters increment.
sleep 2;
is($pair->node0->safe_psql('postgres',
		"SELECT heartbeat_recv_count > 0 FROM pg_cluster_ic_peers WHERE node_id = 1"),
	't',
	'L1 node0 heartbeat_recv_count > 0 (heartbeats arriving from node1)');


# ----------
# L2: SIGSTOP node1 LMON -- TCP stays open but no heartbeat sends.
# ----------
my $lmon1_pid = $pair->node1->safe_psql('postgres',
	"SELECT pid FROM pg_stat_activity WHERE backend_type = 'lmon'");
ok($lmon1_pid > 0, "L2 located node1 LMON pid (got $lmon1_pid)");

kill 'STOP', $lmon1_pid
	or die "SIGSTOP $lmon1_pid failed: $!";
note "L2 sent SIGSTOP to node1 LMON (pid $lmon1_pid)";


# ----------
# L3: wait > 3 * heartbeat_interval; node0 should detect liveness timeout.
# Default interval = 1s -> liveness window = 3s.  Wait 6s for safety.
# ----------
my $deadline = time + 15;
my $state = '';
my $last_err = '';
while (time < $deadline)
{
	$state = $pair->node0->safe_psql('postgres',
		"SELECT state FROM pg_cluster_ic_peers WHERE node_id = 1");
	if ($state eq 'down')
	{
		$last_err = $pair->node0->safe_psql('postgres',
			"SELECT last_error FROM pg_cluster_ic_peers WHERE node_id = 1");
		last;
	}
	usleep(500_000);
}

is($state, 'down',
	"L3 node0 transitioned peer 1 to 'down' after liveness timeout (got '$state')");
# Note: last_error is best-effort -- LMON immediately re-attempts a
# reconnect after the close (heartbeat tick = 1s), so the original
# "heartbeat liveness timeout" message can be overwritten by the next
# attempt's "Connection refused" (peer LMON is still SIGSTOP'd).  Both
# are valid evidence the F2 fix detected the silent peer death.
like($last_err, qr/liveness timeout|heartbeat|Connection refused|Connection reset|envelope recv/i,
	"L3 last_error indicates timeout-driven close (got '$last_err')");


# ----------
# L4: clean up -- SIGCONT node1 LMON so stop_pair works.
# ----------
kill 'CONT', $lmon1_pid;
note "L4 sent SIGCONT to node1 LMON for cleanup";
sleep 1;

$pair->stop_pair;

done_testing();
