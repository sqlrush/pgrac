#-------------------------------------------------------------------------
#
# 334_ic_rdma_soft_roce.pl
#    spec-6.1/spec-6.13 RDMA transport correctness harness for Linux soft-RoCE.
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
use lib "$FindBin::RealBin/../../perl";

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::ClusterPair;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep);

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
	quorum_voting_disks => 3,
	shared_data         => 1,
	extra_conf => [
		"cluster.interconnect_rdma_fallback = auto",
		"cluster.interconnect_rdma_provider = verbs",
		"cluster.interconnect_rdma_completion = event",
		"cluster.interconnect_rdma_crc_offload = off",
		"cluster.grd_max_entries = 1024",
		"cluster.cssd_heartbeat_interval_ms = 2000",
		"cluster.cssd_dead_deadband_factor = 10",
	]);

my $rdma_port0 = PostgreSQL::Test::Cluster::get_free_port();
my $rdma_port1 = PostgreSQL::Test::Cluster::get_free_port();
my $data_port0 = PostgreSQL::Test::Cluster::get_free_port();
my $data_port1 = PostgreSQL::Test::Cluster::get_free_port();

for my $node ($pair->node0, $pair->node1)
{
	$node->adjust_conf('postgresql.conf', 'cluster.interconnect_tier', 'tier3');
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
data_addr = 127.0.0.1:$data_port0
rdma_addr = $rdma_host:$rdma_port0
rdma_gid = rxe
rdma_port = 1
rdma_pkey = 0xffff
rdma_qkey = 0

[node.1]
interconnect_addr = 127.0.0.1:@{[$pair->ic_port(1)]}
data_addr = 127.0.0.1:$data_port1
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
			"SELECT transport || '|' || rdma_state || '|' || provider || '|' ||
			        block_reply_lane_state
			   FROM pg_stat_cluster_ic
			  WHERE node_id = $peer_id");
		last if $row =~ /^rdma\|connected\|[^|]+\|connected$/;
		sleep 1;
	}
	return $row;
}

like(wait_for_rdma_peer($pair->node0, 1), qr/^rdma\|connected\|[^|]+\|connected$/,
	'node0 selects RDMA tier3 and connects block-reply lane to node1 over soft-RoCE');
like(wait_for_rdma_peer($pair->node1, 0), qr/^rdma\|connected\|[^|]+\|connected$/,
	'node1 selects RDMA tier3 and connects block-reply lane to node0 over soft-RoCE');

is($pair->node0->safe_psql(
		'postgres',
		q{SELECT count(*)
		    FROM pg_attribute
		   WHERE attrelid = 'pg_stat_cluster_ic'::regclass
		     AND attname IN ('block_reply_lane_state',
		                     'block_reply_lane_fallback_count',
		                     'block_reply_lane_error_count',
		                     'last_block_reply_error')}),
	'4',
	'D6 block-reply lane columns are exposed through pg_stat_cluster_ic');

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

sub gcs_counter
{
	my ($node, $key) = @_;
	my $value = $node->safe_psql(
		'postgres',
		qq{SELECT coalesce((SELECT value::bigint
		                      FROM pg_cluster_state
		                     WHERE category = 'gcs' AND key = '$key'), 0)});
	return int($value);
}

sub rdma_peer_counter
{
	my ($node, $peer_id, $column) = @_;
	my $value = $node->safe_psql(
		'postgres',
		qq{SELECT coalesce($column, 0)
		      FROM pg_stat_cluster_ic
		     WHERE node_id = $peer_id});
	return int($value);
}

for my $node ($pair->node0, $pair->node1)
{
	$node->safe_psql('postgres', q{
		DROP TABLE IF EXISTS rdma_direct_land;
		CREATE TABLE rdma_direct_land(id int PRIMARY KEY, payload text)
		  WITH (autovacuum_enabled = false);
	});
}

my $rel0 = $pair->node0->safe_psql(
	'postgres', q{SELECT pg_relation_filepath('rdma_direct_land')});
my $rel1 = $pair->node1->safe_psql(
	'postgres', q{SELECT pg_relation_filepath('rdma_direct_land')});
is($rel0, $rel1, "shared-data relation uses the same relfilenode ($rel0)");

$pair->node0->safe_psql('postgres', q{
	INSERT INTO rdma_direct_land
	SELECT g, repeat('x', 128) FROM generate_series(1, 512) AS g;
	CHECKPOINT;
});

my $direct_before = gcs_counter($pair->node1, 'direct_install_count');
my $fallback_before = rdma_peer_counter(
	$pair->node1, 0, 'block_reply_lane_fallback_count');
my ($read_rc, $read_stdout, $read_stderr) = (1, '', '');

for (1 .. 30)
{
	($read_rc, $read_stdout, $read_stderr) = $pair->node1->psql(
		'postgres',
		q{SELECT count(*) FROM rdma_direct_land WHERE id BETWEEN 1 AND 512},
		timeout => 30);
	last if $read_rc == 0
	  && gcs_counter($pair->node1, 'direct_install_count') > $direct_before;
	usleep(200_000);
}

is($read_rc, 0,
	"node1 cross-node read completes over RDMA block-reply lane ($read_stderr)");
cmp_ok(gcs_counter($pair->node1, 'direct_install_count'), '>', $direct_before,
	'D6 direct-land installs at least one verified page on the requester');
is(rdma_peer_counter($pair->node1, 0, 'block_reply_lane_fallback_count'),
	$fallback_before,
	'D6 direct-land success path does not increment block-reply lane fallback counter');

$pair->stop_pair;

done_testing();
