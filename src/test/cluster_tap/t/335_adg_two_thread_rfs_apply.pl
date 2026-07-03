#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 335_adg_two_thread_rfs_apply.pl
#    ADG physical standby two-thread RFS/apply smoke.
#
#    This is a narrow production-path harness for the ADG data plane:
#    a shared-sysid two-node primary cluster produces two WAL threads, while a
#    shared-sysid two-node standby cluster runs one RFS coordinator with two
#    upstream replication connections and one elected apply master.
#
# Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
# Portions Copyright (c) 2026, pgrac contributors
#
# Author: SqlRush <sqlrush@gmail.com>
#
# IDENTIFICATION
#    src/test/cluster_tap/t/335_adg_two_thread_rfs_apply.pl
#
# NOTES
#    pgrac-original file.
#    Spec: spec-6.4-adg-physical-standby-readonly.md
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use File::Path qw(make_path);
use FindBin;
use lib "$FindBin::RealBin/../../perl";
use lib "$FindBin::RealBin/../lib";

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use PgracClusterNode;
use Test::More;
use Time::HiRes qw(usleep);

sub trim
{
	my ($s) = @_;
	$s =~ s/\s+\z//;
	return $s;
}

sub quote_conf
{
	my ($s) = @_;
	$s =~ s/'/''/g;
	return "'$s'";
}

sub poll_psql_value
{
	my ($node, $sql, $expected, $label, $tries) = @_;

	$tries //= 160;
	for my $try (1 .. $tries)
	{
		my ($ret, $stdout, $stderr) = $node->psql('postgres', $sql);
		my $out = trim($stdout);

		if ($ret == 0 && (!defined $expected || $out eq $expected))
		{
			pass($label);
			return $out;
		}

		usleep(250_000);
	}

	fail($label);
	return undef;
}

sub make_voting_disks
{
	my ($prefix) = @_;
	my $disk_dir = PostgreSQL::Test::Utils::tempdir();
	my @disks;

	for my $i (0 .. 2)
	{
		my $path = "$disk_dir/${prefix}_disk$i";
		open(my $fh, '>', $path) or die "open $path: $!";
		binmode $fh;
		print $fh ("\0" x (4 * 128 * 512));
		close $fh;
		push @disks, $path;
	}
	return join(',', @disks);
}

sub write_pair_conf
{
	my ($node0, $node1, $cluster_name) = @_;
	my $ic0 = PostgreSQL::Test::Cluster::get_free_port();
	my $ic1 = PostgreSQL::Test::Cluster::get_free_port();
	my $conf = <<EOC;
[cluster]
name = $cluster_name

[node.0]
interconnect_addr = 127.0.0.1:$ic0

[node.1]
interconnect_addr = 127.0.0.1:$ic1
EOC

	PostgreSQL::Test::Utils::append_to_file($node0->data_dir . '/pgrac.conf', $conf);
	PostgreSQL::Test::Utils::append_to_file($node1->data_dir . '/pgrac.conf', $conf);
}

sub relocate_pg_wal
{
	my ($node, $wal_root, $thread_id) = @_;
	my $pg_wal = $node->data_dir . '/pg_wal';
	my $thread_dir = "$wal_root/thread_$thread_id";

	make_path($wal_root);
	die "$thread_dir already exists" if -e $thread_dir;
	rename $pg_wal, $thread_dir
	  or die "rename $pg_wal to $thread_dir: $!";
	symlink $thread_dir, $pg_wal
	  or die "symlink $pg_wal -> $thread_dir: $!";
}

sub copy_shared_root
{
	my ($from, $to) = @_;

	make_path($to);
	PostgreSQL::Test::Utils::system_log('cp', '-R', "$from/.", $to);
}

sub count_wal_files
{
	my ($dir) = @_;
	my $count = 0;

	opendir(my $dh, $dir) or return 0;
	while (defined(my $entry = readdir($dh)))
	{
		next if $entry =~ /^\./;
		$count++ if -f "$dir/$entry";
	}
	closedir($dh);
	return $count;
}

my $primary_wal_root = PostgreSQL::Test::Utils::tempdir();
my $primary_shared_root = PostgreSQL::Test::Utils::tempdir();
my $standby_wal_root = PostgreSQL::Test::Utils::tempdir();
my $standby_shared_root = PostgreSQL::Test::Utils::tempdir();
my $primary_disks = make_voting_disks('adg_p');
my $standby_disks = make_voting_disks('adg_s');

my $primary0 = PgracClusterNode->new('adg2_primary0');
$primary0->init(allows_streaming => 1, extra => [ '-X', "$primary_wal_root/thread_1" ]);
$primary0->append_conf('postgresql.conf', qq{
shared_buffers = 16MB
cluster.enabled = on
cluster.node_id = 0
cluster.allow_single_node = on
cluster.dg_role = primary
cluster.dg_mode = max_availability
cluster.enable_adg = on
cluster.wal_threads_dir = '$primary_wal_root'
cluster.shared_storage_backend = cluster_fs
cluster.shared_data_dir = '$primary_shared_root'
cluster.smgr_user_relations = on
cluster.adg_barrier_interval_ms = 100
wal_level = replica
max_wal_senders = 10
max_replication_slots = 10
wal_keep_size = '64MB'
max_prepared_transactions = 10
});
$primary0->start;
$primary0->safe_psql('postgres', q{
CREATE TABLE adg_pair_p0(id int primary key, origin text);
CREATE TABLE adg_pair_p1(id int primary key, origin text);
});
$primary0->backup('adg2_b');
$primary0->stop;

my $primary1 = PgracClusterNode->new('adg2_primary1');
$primary1->init_from_backup($primary0, 'adg2_b');
relocate_pg_wal($primary1, $primary_wal_root, 2);

my $primary_common_conf = qq{
shared_buffers = 16MB
cluster.enabled = on
cluster.interconnect_tier = tier1
cluster.allow_single_node = off
cluster.voting_disks = '$primary_disks'
cluster.dg_role = primary
cluster.enable_adg = on
cluster.wal_threads_dir = '$primary_wal_root'
cluster.shared_storage_backend = cluster_fs
cluster.shared_data_dir = '$primary_shared_root'
cluster.smgr_user_relations = on
cluster.adg_barrier_interval_ms = 100
cluster.cssd_heartbeat_interval_ms = 2000
cluster.cssd_dead_deadband_factor = 10
wal_level = replica
max_wal_senders = 10
max_replication_slots = 10
wal_keep_size = '64MB'
max_prepared_transactions = 10
};
$primary0->append_conf('postgresql.conf', $primary_common_conf);
$primary0->append_conf('postgresql.conf', "cluster.node_id = 0\n");
$primary1->append_conf('postgresql.conf', $primary_common_conf);
$primary1->append_conf('postgresql.conf', "cluster.node_id = 1\n");
write_pair_conf($primary0, $primary1, 'adg2_primary');
$primary0->start;
$primary1->start;

poll_psql_value($primary0,
	q{SELECT count(*) = 2 FROM pg_cluster_membership WHERE state = 'member'},
	't',
	'primary cluster has two live members');

copy_shared_root($primary_shared_root, $standby_shared_root);

my $standby0 = PgracClusterNode->new('adg2_standby0');
$standby0->init_from_backup($primary0, 'adg2_b');
$standby0->set_standby_mode;
relocate_pg_wal($standby0, $standby_wal_root, 1);

my $standby1 = PgracClusterNode->new('adg2_standby1');
$standby1->init_from_backup($primary0, 'adg2_b');
$standby1->set_standby_mode;
relocate_pg_wal($standby1, $standby_wal_root, 2);

my $rfs_conninfos = 'thread_id=1 ' . $primary0->connstr('postgres')
  . '; thread_id=2 ' . $primary1->connstr('postgres');
my $standby_common_conf = qq{
shared_buffers = 16MB
cluster.enabled = on
cluster.interconnect_tier = tier1
cluster.allow_single_node = off
cluster.voting_disks = '$standby_disks'
cluster.dg_role = standby
cluster.dg_mode = max_availability
cluster.enable_adg = on
cluster.apply_master_election = on
cluster.wal_threads_dir = '$standby_wal_root'
cluster.shared_storage_backend = cluster_fs
cluster.shared_data_dir = '$standby_shared_root'
cluster.smgr_user_relations = on
cluster.cssd_heartbeat_interval_ms = 2000
cluster.cssd_dead_deadband_factor = 10
max_prepared_transactions = 10
};
$standby0->append_conf('postgresql.conf', $standby_common_conf);
$standby0->append_conf('postgresql.conf', "cluster.node_id = 0\n");
$standby0->append_conf('postgresql.conf',
	'cluster.adg_rfs_conninfos = ' . quote_conf($rfs_conninfos) . "\n");
$standby1->append_conf('postgresql.conf', $standby_common_conf);
$standby1->append_conf('postgresql.conf', "cluster.node_id = 1\n");
write_pair_conf($standby0, $standby1, 'adg2_standby');

$standby0->start;
$standby1->start;

poll_psql_value($standby0,
	q{SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'remote file server'},
	'1',
	'standby node0 runs one RFS coordinator');
poll_psql_value($standby0,
	q{SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'managed recovery process'},
	'1',
	'standby node0 runs MRP coordinator');
poll_psql_value($standby1,
	q{SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'managed recovery process'},
	'1',
	'standby node1 runs MRP coordinator for attach/read service');

ok($standby0->wait_for_log_match(qr/ADG RFS upstream 1 for thread 1 connected and streaming/, 60),
	'RFS upstream 1 is bound to primary thread 1');
ok($standby0->wait_for_log_match(qr/ADG RFS upstream 2 for thread 2 connected and streaming/, 60),
	'RFS upstream 2 is bound to primary thread 2');

$primary0->safe_psql('postgres', q{INSERT INTO adg_pair_p0 VALUES (1, 'p0')});
$primary1->safe_psql('postgres', q{INSERT INTO adg_pair_p1 VALUES (2, 'p1')});

poll_psql_value($standby0,
	q{
SELECT count(*) || '|' || coalesce(sum(id), 0)
  FROM (SELECT id FROM adg_pair_p0
        UNION ALL
        SELECT id FROM adg_pair_p1) s
	},
	'2|3',
	'standby apply master reads commits from both primary WAL threads',
	240);

$standby0->safe_psql('postgres', 'CHECKPOINT');

poll_psql_value($standby1,
	q{
SELECT count(*) || '|' || coalesce(sum(id), 0)
  FROM (SELECT id FROM adg_pair_p0
        UNION ALL
        SELECT id FROM adg_pair_p1) s
	},
	'2|3',
	'standby attach node reads the apply-master shared result',
	240);

ok(count_wal_files("$standby_wal_root/thread_1") > 0,
	'RFS landed WAL files for primary thread 1');
ok(count_wal_files("$standby_wal_root/thread_2") > 0,
	'RFS landed WAL files for primary thread 2');

poll_psql_value($standby0,
	q{
SELECT dg_role, dg_mode, adg_enabled, mrp_status, apply_master_node_id,
       apply_master_term > 0, standby_consistent_scn > 0
  FROM pg_stat_cluster_adg
	},
	'standby|max_availability|t|ready|0|t|t',
	'ADG view reports elected apply master and read floor');

$standby1->stop;
$standby0->stop;
$primary1->stop;
$primary0->stop;

done_testing();
