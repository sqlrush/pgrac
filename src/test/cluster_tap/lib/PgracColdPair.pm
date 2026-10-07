#-------------------------------------------------------------------------
#
# PgracColdPair.pm
#    Two-node shared fixture for the cold crash recovery TAPs (t/247,
#    t/248), provisioned only through supported entries.
#
#    initdb provisions the WAL state registry (--pgrac-wal-state-root) and
#    the seed's HW image (--pgrac-hw-snapshot-root/-owner); optional
#    catalog objects every node shares are created on the seed with the
#    cluster disabled; the seed's first clustered start, as the sole
#    declared member, publishes the shared control-file authority; node1
#    is a physical clone of that seed.  The server, initdb and
#    pg_basebackup write every registry, HW, control and identity byte.
#    This follows B's PostgreSQL::Test::ClusterSeed and its ClusterPair
#    seed path, which this line does not carry yet; the returned object is
#    an ordinary PostgreSQL::Test::ClusterPair.
#
#    Author: SqlRush <sqlrush@gmail.com>
#    Spec: spec-s9p2-05-instance-and-cluster-recovery.md
#
#-------------------------------------------------------------------------

package PgracColdPair;

use strict;
use warnings;

use Exporter 'import';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::ClusterPair;
use PostgreSQL::Test::ClusterVotingDisk qw(format_voting_file);
use PostgreSQL::Test::Utils;

our @EXPORT_OK = qw(new_cold_pair);

# Options: seed_sql (catalog objects for every node), extra_conf (array of
# postgresql.conf lines for both nodes).
sub new_cold_pair
{
	my ($name, %opts) = @_;
	my @pg = map { PostgreSQL::Test::Cluster::get_free_port() } (0, 1);
	my @ic = map { PostgreSQL::Test::Cluster::get_free_port() } (0, 1);
	my @data = map { PostgreSQL::Test::Cluster::get_free_port_range(2) } (0, 1);
	my @nodes = map {
		PostgreSQL::Test::Cluster->new("${name}_node$_", port => $pg[$_])
	} (0, 1);
	my ($node0, $node1) = @nodes;
	my $disk_dir = PostgreSQL::Test::Utils::tempdir();
	my @disks = map { "$disk_dir/disk$_" } (0 .. 2);
	format_voting_file($disks[$_], $_) for 0 .. 2;
	my $disks_csv = join(',', @disks);
	my $wal = PostgreSQL::Test::Utils::tempdir();
	my $shared = PostgreSQL::Test::Utils::tempdir();

	# 1. initdb provisions the registry and the HW image of the seed.
	$node0->init(allows_streaming => 1, extra => [
		'-X', "$wal/thread_1", "--pgrac-wal-state-root=$wal",
		"--pgrac-hw-snapshot-root=$shared", '--pgrac-hw-snapshot-owner=0' ]);

	# 2. Catalog objects every node shares, created before formation.
	if (defined $opts{seed_sql})
	{
		$node0->append_conf('postgresql.conf', <<EOC);
cluster.enabled = off
cluster.lms_enabled = off
cluster.node_id = 0
cluster.shared_data_dir = '$shared'
cluster.shared_storage_backend = cluster_fs
cluster.smgr_user_relations = on
cluster.relation_extend_lock_enabled = off
EOC
		$node0->start;
		$node0->safe_psql('postgres', $opts{seed_sql});
		$node0->stop;
	}

	# 3. The seed's first clustered start, as the sole declared member,
	# publishes the shared control-file authority.
	$node0->append_conf('postgresql.conf', <<EOC);
cluster.enabled = on
cluster.interconnect_tier = tier1
cluster.lms_enabled = on
cluster.allow_single_node = off
cluster.voting_disks = '$disks_csv'
cluster.node_id = 0
cluster.wal_threads_dir = '$wal'
cluster.shared_storage_backend = cluster_fs
cluster.shared_data_dir = '$shared'
cluster.controlfile_shared_authority = on
EOC
	PostgreSQL::Test::Utils::append_to_file($node0->data_dir . '/pgrac.conf',
		"[cluster]\nname = $name\n\n[node.0]\n"
		  . "interconnect_addr = 127.0.0.1:$ic[0]\ndata_addr = 127.0.0.1:$data[0]\n");
	$node0->start;
	my $authority = "$shared/global/pg_control";
	my $local = $node0->data_dir . '/global/pg_control';
	die 'native seed did not publish its shared control authority'
	  unless -l $local && readlink($local) eq $authority && -f $authority;
	$node0->backup('cold_pair_seed');
	$node0->stop;

	# 4. node1 is a physical clone with its own WAL thread.
	$node1->init_from_backup($node0, 'cold_pair_seed');
	PostgreSQL::Test::ClusterPair::_relocate_backup_pg_wal($node1, $wal, 2);
	my $sysid = PostgreSQL::Test::ClusterPair::_pg_controldata_system_identifier($node0);
	die 'cloned peer has another system identifier'
	  unless PostgreSQL::Test::ClusterPair::_pg_controldata_system_identifier($node1)
	  eq $sysid;

	for my $i (0, 1)
	{
		my $node = $nodes[$i];
		$node->append_conf('postgresql.conf', <<EOC);
cluster.enabled = on
cluster.interconnect_tier = tier1
cluster.lms_enabled = on
cluster.allow_single_node = off
cluster.voting_disks = '$disks_csv'
cluster.node_id = $i
cluster.wal_threads_dir = '$wal'
cluster.shared_storage_backend = cluster_fs
cluster.shared_data_dir = '$shared'
cluster.controlfile_shared_authority = on
cluster.smgr_user_relations = on
cluster.relation_extend_lock_enabled = on
shared_buffers = 16MB
EOC
		$node->append_conf('postgresql.conf', "$_\n") for @{ $opts{extra_conf} // [] };
		PostgreSQL::Test::Utils::append_to_file($node->data_dir . '/pgrac.conf',
			"\n[node.1]\ninterconnect_addr = 127.0.0.1:$ic[1]\n"
			  . "data_addr = 127.0.0.1:$data[1]\n");
	}

	return bless {
		node0 => $node0,
		node1 => $node1,
		cluster_name => $name,
		pg_ports => \@pg,
		ic_ports => \@ic,
		data_ports => \@data,
		voting_disk_paths => \@disks,
		wal_threads_root => $wal,
		shared_data_root => $shared,
		shared_control_root => $shared,
		shared_system_identifier => $sysid,
	}, 'PostgreSQL::Test::ClusterPair';
}

1;
