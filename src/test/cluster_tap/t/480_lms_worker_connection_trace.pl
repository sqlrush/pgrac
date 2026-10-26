#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 480_lms_worker_connection_trace.pl
#    Observe four native LMS DATA channels from a refused first connect
#    through HELLO, retained-byte snapshots, and postmaster shutdown.
#    A separate raw TCP leg checks partial anonymous HELLO evidence.
#
# Portions Copyright (c) 2026, pgrac contributors
# Author: SqlRush <sqlrush@gmail.com>
#
# IDENTIFICATION
#    src/test/cluster_tap/t/480_lms_worker_connection_trace.pl
#
# NOTES
#    This tests transport diagnostics on localhost, not cluster recovery
#    or reachability between separate hosts.
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use IO::Socket::INET;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::ClusterPair;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(time usleep);

sub wait_log
{
	my ($node, $predicate) = @_;
	my $deadline = time + 20;
	while (time < $deadline)
	{
		return 1 if $predicate->(slurp_file($node->logfile));
		usleep(100_000);
	}
	return 0;
}

my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'lms4trace',
	data_port_span => 4,
	extra_conf => [
		'cluster.lms_workers = 4',
		'log_min_messages = debug1',
		'autovacuum = off' ]);
my ($node0, $node1) = ($pair->node0, $pair->node1);

# Keep the real peer down until each worker has attempted its first dial.
$node0->start;
ok(wait_log($node0, sub {
	my ($log) = @_;
	my @owners = $log =~ /\[(\d+)\].*cluster_lms: DATA-plane listener bound/g;
	return 0 unless @owners == 4;
	for my $pid (@owners)
	{
		return 0 unless $log =~ /\[$pid\].*peer 1 closed: connect failed/;
	}
	return 1;
}), 'all four native DATA owners encounter the unavailable peer');

my $first_log = slurp_file($node0->logfile);
for my $worker (0 .. 3)
{
	like($first_log,
		qr/connection phase=connect-result .*plane=DATA worker=$worker workers=4 peer=1 .*target=127\.0\.0\.1:\d+ .*connect_rc=-?\d+ socket_error=\d+/,
		"worker $worker identifies the actual dial target and connect result");
	like($first_log,
		qr/connection phase=connect-complete .*plane=DATA worker=$worker workers=4 peer=1 .*socket_error=[1-9]\d*/,
		"worker $worker retains the native SO_ERROR failure");
}

$node1->start;
ok($pair->wait_for_peer_state(0, 1, 'connected', 20), 'CONTROL 0->1 connected');
ok($pair->wait_for_peer_state(1, 0, 'connected', 20), 'CONTROL 1->0 connected');
ok(wait_log($node1, sub {
	my ($log) = @_;
	for my $worker (0 .. 3)
	{
		return 0 unless $log =~ /HELLO verified.*\(DATA worker $worker\)/;
	}
	return 1;
}), 'native HELLO verified on all four DATA channels');

for my $worker (0 .. 3)
{
	like(slurp_file($node0->logfile),
		qr/connection phase=hello-sent .*plane=DATA worker=$worker workers=4 peer=1 .*local=127\.0\.0\.1:\d+ remote=127\.0\.0\.1:\d+ .*stream_serial=[1-9]\d* .*hello_remaining=0/,
		"worker $worker active HELLO includes endpoints and stream lifetime");
	like(slurp_file($node1->logfile),
		qr/connection phase=hello-verified .*plane=DATA worker=$worker workers=4 peer=0 .*hello_received=64/,
		"worker $worker passive HELLO retains its completed byte count");
}

$node0->stop;
$node1->stop;
for my $node ($node0, $node1)
{
	my $log = slurp_file($node->logfile);
	unlike($log, qr/(?:ERROR|FATAL|PANIC):/, $node->name . ' has no server failure');
	like($log, qr/database system is shut down/, $node->name . ' postmaster exit is logged');
	like($log,
		qr/connection phase=close .*plane=DATA worker=3 workers=4 .*send_tail=0 fifo_frames=0 fifo_bytes=0/,
		$node->name . ' records worker 3 ownership before close');
}

# A real socket ending after a partial HELLO must expose the accumulator
# before its anonymous slot is reset. It never becomes an admitted peer.
my $partial = PostgreSQL::Test::Cluster->new('lms4partial');
$partial->init;
my $control_port = PostgreSQL::Test::Cluster::get_free_port();
my $peer_control_port = PostgreSQL::Test::Cluster::get_free_port();
my $data_port = PostgreSQL::Test::Cluster::get_free_port_range(4);
my $peer_data_port = PostgreSQL::Test::Cluster::get_free_port_range(4);
$partial->append_conf('postgresql.conf', qq{
cluster.enabled = on
cluster.interconnect_tier = tier1
cluster.node_id = 1
cluster.lms_workers = 4
cluster.allow_single_node = on
log_min_messages = debug1
autovacuum = off
});
append_to_file($partial->data_dir . '/pgrac.conf', qq{
[cluster]
name = lms4partial
[node.0]
interconnect_addr = 127.0.0.1:$peer_control_port
data_addr = 127.0.0.1:$peer_data_port
[node.1]
interconnect_addr = 127.0.0.1:$control_port
data_addr = 127.0.0.1:$data_port
});
$partial->start;
ok(wait_log($partial, sub { $_[0] =~ /DATA-plane worker 3 started/ }),
	'partial-HELLO native worker 3 is running');
my $sock = IO::Socket::INET->new(
	PeerAddr => '127.0.0.1', PeerPort => $data_port + 3,
	Proto => 'tcp', Timeout => 5) or die "DATA connect: $!";
is(syswrite($sock, 'x' x 17), 17, 'send only a HELLO prefix over real TCP');
ok(wait_log($partial, sub {
	$_[0] =~ /connection phase=hello-recv .*plane=DATA worker=3 workers=4 peer=-1 .*hello_received=17/
}), 'partial receive records the original anonymous slot and exact bytes');
close($sock) or die "DATA close: $!";
ok(wait_log($partial, sub {
	$_[0] =~ /connection phase=hello-eof .*plane=DATA worker=3 workers=4 peer=-1 .*hello_received=17/
}), 'EOF preserves the partial HELLO evidence before reset');
$partial->stop;
unlike(slurp_file($partial->logfile), qr/(?:ERROR|FATAL|PANIC):/,
	'partial HELLO does not cause a server failure');

done_testing();
