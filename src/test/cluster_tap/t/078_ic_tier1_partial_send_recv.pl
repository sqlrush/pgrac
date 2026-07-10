# spec-2.2 Hardening v1.0.1 T-F1 -- HELLO partial send/recv buffer.
#
# Verifies F1 fix: pgrac LMON now accumulates partial HELLO bytes
# across multiple WL_SOCKET_READABLE wakeups via per-anon-slot buffer
# (cluster_ic_tier1_continue_hello_recv).  Pre-fix code used recv()
# with MSG_WAITALL on a nonblocking fd, which is a no-op on Linux/macOS
# -- a 1-byte read would be treated as protocol error and reject the
# peer.
#
# Test method:
#   - start a single-node pgrac with tier1 listener bound
#   - from perl, open a raw TCP connection to the listener_port
#   - send a valid 64-byte HELLO frame ONE BYTE AT A TIME with 30ms
#     between bytes (forces TCP to deliver in fragments)
#   - assert pgrac eventually reports peer_state = connected for the
#     spoofed peer node (within 10s window)
#   - control: send only 32 bytes then close -> assert pgrac drops
#     (no bogus connected state)
#
# Spec: pgrac:specs/spec-2.2-* ## Hardening v1.0.1 F1.
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../lib";

use IO::Socket::INET;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep);


# HELLO wire format (spec-2.2 §2.4 + cluster_ic.c::cluster_ic_build_hello):
#   offset 0  uint32 LE: magic = 0x4F4C4C48 ("HLLO")
#   offset 4  uint16 LE: hello_version    = 1
#   offset 6  uint16 LE: envelope_version = 1
#   offset 8  uint32 LE: source_node_id
#   offset 12 char[24] : cluster_name (NUL-terminated, padded with 0)
#   offset 36 char[28] : _pad (zeros)
# Total: 64 bytes
sub build_hello
{
	my ($source_node_id, $cluster_name) = @_;
	my $name_field = substr($cluster_name . ("\0" x 24), 0, 24);
	# pack: uint32 LE (V), uint16 LE (v), uint16 LE (v), uint32 LE (V),
	# 24-byte string (a24), then pad to 64 with zeros
	my $hello = pack('VvvVa24', 0x4F4C4C48, 1, 1, $source_node_id, $name_field);
	$hello .= "\0" x (64 - length $hello);
	die "HELLO not 64 bytes (got " . length($hello) . ")"
		unless length($hello) == 64;
	return $hello;
}


my $node = PostgreSQL::Test::Cluster->new('ic_partial_io');
$node->init;
$node->append_conf('postgresql.conf', "cluster.enabled = on\n");
$node->append_conf('postgresql.conf', "cluster.interconnect_tier = tier1\n");
$node->append_conf('postgresql.conf', "cluster.node_id = 0\n");
# spec-7.3 merge: this hand-rolled rig reserves ONE data port per node; the
# shipped default cluster.lms_workers=2 binds [data_port, data_port+1] and
# cross-wires consecutive free ports (HELLO DATA worker mismatch).  Pin the
# pool to one worker: N=1 is the spec-7.2 topology identity this rig was
# written against.
$node->append_conf('postgresql.conf', "cluster.lms_workers = 1\n");
$node->append_conf('postgresql.conf', "cluster.allow_single_node = on\n");

# Declare 2 nodes so we can spoof "peer 1" connecting to "peer 0" (us).
my $self_ic_port = PostgreSQL::Test::Cluster::get_free_port();
my $spoof_ic_port = PostgreSQL::Test::Cluster::get_free_port();
my $self_data_port = PostgreSQL::Test::Cluster::get_free_port();
my $spoof_data_port = PostgreSQL::Test::Cluster::get_free_port();
my $cluster_name = 'pgrac-078';
my $pgrac_conf = <<EOC;
[cluster]
name = $cluster_name

[node.0]
interconnect_addr = 127.0.0.1:$self_ic_port
data_addr = 127.0.0.1:$self_data_port

[node.1]
interconnect_addr = 127.0.0.1:$spoof_ic_port
data_addr = 127.0.0.1:$spoof_data_port
EOC
PostgreSQL::Test::Utils::append_to_file($node->data_dir . '/pgrac.conf',
	$pgrac_conf);

$node->start;


# ----------
# L1: pgrac listener is up + bound on self_ic_port.
# ----------
my $port = $node->safe_psql('postgres',
	"SELECT value FROM pg_cluster_state WHERE category='ic' AND key='tier1_listener_port'");
is($port, "$self_ic_port",
	"L1 tier1 listener bound on declared port (got $port, want $self_ic_port)");


# ----------
# L2: trickle HELLO 1 byte at a time -- accumulator must succeed.
# ----------
sub trickle_hello
{
	my ($source_node_id, $bytes_to_send) = @_;
	my $hello = build_hello($source_node_id, $cluster_name);
	$bytes_to_send //= length $hello;

	my $sock = IO::Socket::INET->new(
		PeerAddr => '127.0.0.1',
		PeerPort => $self_ic_port,
		Proto    => 'tcp',
		Timeout  => 5);
	die "connect failed: $!" unless defined $sock;
	$sock->autoflush(1);

	for (my $i = 0; $i < $bytes_to_send; $i++)
	{
		my $rv = syswrite($sock, substr($hello, $i, 1), 1);
		die "syswrite failed at byte $i: $!" unless $rv == 1;
		usleep(20_000);    # 20ms between bytes -> 1.28s for full HELLO
	}
	return $sock;
}

my $sock1 = trickle_hello(1, 64);     # full HELLO trickled
note "L2 trickled 64-byte HELLO (peer 1) over ~1.3s";

# Wait up to 10s for pgrac to report peer 1 as connected.
my $deadline = time + 10;
my $state = '';
while (time < $deadline)
{
	$state = $node->safe_psql('postgres',
		"SELECT state FROM pg_cluster_ic_peers WHERE node_id = 1");
	last if defined $state && $state eq 'connected';
	usleep(200_000);
}
is($state, 'connected',
	"L2 pgrac accepted trickled HELLO -> state = connected (got '$state')");


# Cleanup: keep socket open to keep peer in CONNECTED state until we read its heartbeat.
# We need to drain any heartbeats pgrac sends or it'll close on partial-write (out of scope here).
# Simplest: just shut down and let the next test run fresh.
shutdown($sock1, 2);
close $sock1;


# ----------
# L3: control -- send only 32 bytes (truncated) and close.
# pgrac must NOT report this peer as connected (state stays connecting/down).
# ----------
# To re-test with fresh peer state, restart the node so pending state resets.
$node->restart;
sleep 1;

my $sock2 = trickle_hello(1, 32);     # only 32 of 64 bytes
note "L3 trickled only 32 bytes HELLO (truncated)";
shutdown($sock2, 2);
close $sock2;

# Wait briefly + verify state did NOT reach 'connected'.
sleep 3;
my $state2 = $node->safe_psql('postgres',
	"SELECT state FROM pg_cluster_ic_peers WHERE node_id = 1");
isnt($state2, 'connected',
	"L3 truncated HELLO did NOT promote peer to connected (got '$state2')");


$node->stop;

done_testing();
