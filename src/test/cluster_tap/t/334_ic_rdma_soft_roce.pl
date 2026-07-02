#-------------------------------------------------------------------------
#
# 334_ic_rdma_soft_roce.pl
#    spec-6.1 RDMA transport correctness harness for Linux soft-RoCE.
#
#    This test intentionally does not mock verbs.  It is opt-in because most
#    developer and CI machines do not have rdma_rxe, librdmacm, memlock
#    limits, and the RDMA userspace tools configured.  Set
#    PGRAC_RUN_RDMA_SOFT_ROCE=1 on a Linux runner with an active rxe link to
#    exercise the real RDMA CM/QP path.
#
# IDENTIFICATION
#    src/test/cluster_tap/t/334_ic_rdma_soft_roce.pl
#
# Author: SqlRush <sqlrush@gmail.com>
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use Config;
use FindBin;
use lib "$FindBin::RealBin/../lib";

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::ClusterPair;
use PostgreSQL::Test::Utils;
use Test::More;

plan skip_all => 'set PGRAC_RUN_RDMA_SOFT_ROCE=1 to run the soft-RoCE RDMA harness'
  unless $ENV{PGRAC_RUN_RDMA_SOFT_ROCE};
plan skip_all => 'soft-RoCE RDMA harness requires Linux'
  unless $^O eq 'linux';

sub command_exists
{
	my ($cmd) = @_;
	return system('sh', '-c', "command -v $cmd >/dev/null 2>&1") == 0;
}

plan skip_all => 'rdma command not available'
  unless command_exists('rdma');
plan skip_all => 'ibv_devices command not available'
  unless command_exists('ibv_devices');

my $rdma_links = `rdma link show 2>&1`;
plan skip_all => 'no active RDMA link found; configure rdma_rxe before running'
  unless $rdma_links =~ /\blink\b/i || $rdma_links =~ /\brxe/i;

my $probe = PostgreSQL::Test::Cluster->new('rdma_probe');
$probe->init;
my $pg_config_bin = $probe->config_data('--bindir') . '/pg_config';
my $configure_args = `"$pg_config_bin" --configure 2>&1`;
plan skip_all => 'postgres binary was not configured with --with-rdma'
  unless $configure_args =~ /--with-rdma\b/;

my $rdma_host = $ENV{PGRAC_RDMA_TEST_ADDR};
if (!defined $rdma_host || $rdma_host eq '')
{
	$rdma_host = `hostname -I 2>/dev/null`;
	chomp $rdma_host;
	($rdma_host) = split(/\s+/, $rdma_host);
}
plan skip_all => 'set PGRAC_RDMA_TEST_ADDR to the IP address bound to the rxe link'
  unless defined $rdma_host && $rdma_host ne '';

my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'ic_rdma_soft_roce',
	extra_conf => [
		"cluster.interconnect_rdma_fallback = auto",
		"cluster.interconnect_rdma_provider = verbs",
		"cluster.interconnect_rdma_completion = event",
		"cluster.interconnect_rdma_crc_offload = off",
		"cluster.pcm_grd_max_entries = 0",
	]);

my $rdma_port0 = PostgreSQL::Test::Cluster::get_free_port();
my $rdma_port1 = PostgreSQL::Test::Cluster::get_free_port();

for my $node ($pair->node0, $pair->node1)
{
	$node->adjust_conf('postgresql.conf', 'cluster.interconnect_tier', 'tier2');
}

sub write_rdma_pgrac_conf
{
	my ($node, $cluster_name) = @_;
	my $path = $node->data_dir . '/pgrac.conf';
	open(my $fh, '>', $path) or die "open $path: $!";
	print $fh <<"EOC";
[cluster]
name = $cluster_name

[node.0]
interconnect_addr = 127.0.0.1:@{[$pair->ic_port(0)]}
rdma_addr = $rdma_host:$rdma_port0
rdma_gid = rxe
rdma_port = 1
rdma_pkey = 0xffff
rdma_qkey = 0

[node.1]
interconnect_addr = 127.0.0.1:@{[$pair->ic_port(1)]}
rdma_addr = $rdma_host:$rdma_port1
rdma_gid = rxe
rdma_port = 1
rdma_pkey = 0xffff
rdma_qkey = 0
EOC
	close $fh;
}

write_rdma_pgrac_conf($pair->node0, 'ic_rdma_soft_roce');
write_rdma_pgrac_conf($pair->node1, 'ic_rdma_soft_roce');

$pair->start_pair;

sub wait_for_rdma_peer
{
	my ($node, $peer_id) = @_;
	my $row = '';

	for (1 .. 30)
	{
		$row = $node->safe_psql(
			'postgres',
			"SELECT transport || '|' || rdma_state || '|' || provider
			   FROM pg_stat_cluster_ic
			  WHERE node_id = $peer_id");
		last if $row =~ /^rdma\|connected\|/;
		sleep 1;
	}
	return $row;
}

is(wait_for_rdma_peer($pair->node0, 1), 'rdma|connected|verbs-generic',
	'node0 selects RDMA transport to node1 over soft-RoCE');
is(wait_for_rdma_peer($pair->node1, 0), 'rdma|connected|verbs-generic',
	'node1 selects RDMA transport to node0 over soft-RoCE');

is($pair->node0->safe_psql(
		'postgres',
		q{SELECT count(*) FROM pg_stat_cluster_wait_events
		   WHERE name IN ('InterconnectRdmaSend',
		                  'InterconnectRdmaRecv',
		                  'ClusterICRdmaPoll',
		                  'ClusterICRdmaConnect',
		                  'ClusterICRdmaFallback')}),
	'5',
	'RDMA wait events are registered');

my $send_count = $pair->node0->safe_psql(
	'postgres',
	q{SELECT send_count > 0 FROM pg_stat_cluster_ic WHERE node_id = 1});
is($send_count, 't', 'RDMA heartbeat/control sends are counted');

$pair->stop_pair;

done_testing();
