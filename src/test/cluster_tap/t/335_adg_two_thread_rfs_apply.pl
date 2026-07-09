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
	my $data_port0 = PostgreSQL::Test::Cluster::get_free_port();
	my $data_port1 = PostgreSQL::Test::Cluster::get_free_port();
	my $conf = <<EOC;
[cluster]
name = $cluster_name

[node.0]
interconnect_addr = 127.0.0.1:$ic0
data_addr = 127.0.0.1:$data_port0

[node.1]
interconnect_addr = 127.0.0.1:$ic1
data_addr = 127.0.0.1:$data_port1
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

# Like wait_for_log_match, but only matches log content written after
# $offset bytes -- needed when the same line already appeared earlier
# (e.g. "ready to accept" before and after a fail-stop restart).
sub wait_for_log_match_from
{
	my ($node, $offset, $regex, $timeout) = @_;
	$timeout //= 60;

	my $deadline = time() + $timeout;
	my $logfile = $node->logfile;

	while (time() < $deadline)
	{
		if (-f $logfile)
		{
			my $content = PostgreSQL::Test::Utils::slurp_file($logfile);
			my $tail = length($content) > $offset ? substr($content, $offset) : '';
			return $& if $tail =~ /$regex/;
		}
		usleep(250_000);
	}
	return undef;
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
cluster.adg_lease_takeover_grace_ms = 1000
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

# standby1 is a non-master node: INV-ADG5 blocks its replay until it holds
# the apply-master lease, so it never reaches hot standby and pg_ctl -w
# cannot succeed.  Start it without waiting for read service.
{
	local $ENV{PGCTLTIMEOUT} = 5;
	my $started = $standby1->start(fail_ok => 1);
	is($started, 0,
		'standby node1 does not open read service without the apply-master lease');
}

poll_psql_value($standby0,
	q{SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'remote file server'},
	'1',
	'standby node0 runs one RFS coordinator');
poll_psql_value($standby0,
	q{SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'managed recovery process'},
	'1',
	'standby node0 runs MRP coordinator');

# Fail-closed posture (spec-6.4 P0-2): the non-master node parks before its
# first replayed record and refuses client reads instead of serving
# incoherent shared-storage pages.
ok($standby1->wait_for_log_match(
		qr/ADG standby is waiting for the apply-master lease before replaying WAL/, 60),
	'standby node1 parks at the INV-ADG5 gate as a warm takeover candidate');
{
	my ($ret1, $out1, $err1) = $standby1->psql('postgres', 'SELECT 1');
	isnt($ret1, 0, 'standby node1 refuses reads while not apply master');
	like($err1,
		qr/starting up|not yet accepting connections|read point is not available/,
		'standby node1 read refusal is the fail-closed startup/read-point error');
}

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

# spec-6.4 P0-2: the non-master node must keep refusing reads even while the
# apply master serves them -- cross-node standby reads stay fail-closed until
# cluster coherence covers them.
{
	my ($ret1, $out1, $err1) = $standby1->psql('postgres',
		q{SELECT count(*) FROM adg_pair_p0});
	isnt($ret1, 0,
		'standby attach node keeps refusing reads while node0 is apply master');
}

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

# ------------------------------------------------------------------
# L9: lease loss -- no double apply (spec-6.4 INV-ADG5 e2e).
#
# Freeze node0's MRP so its apply-master lease expires without renewal.
# The lease-expired master must fail stop BEFORE applying its next
# record (never apply under an invalid lease), and may only resume
# after re-taking the lease with a strictly higher term.  node1 stays
# excluded the whole time: the voting-disk candidate gate only admits
# the lowest CSSD-alive node, and node0 stays alive throughout.  (The
# full cssd-dead partition takeover is chaos-harness scope; the
# enforcement point of the invariant -- fail-stop before apply -- is
# exercised for real here.)
# ------------------------------------------------------------------
my $term_before = trim($standby0->safe_psql('postgres',
	q{SELECT apply_master_term FROM pg_stat_cluster_adg}));
ok($term_before =~ /^\d+$/ && $term_before > 0, 'L9 captured pre-freeze apply-master term');

my $mrp0_pid = trim($standby0->safe_psql('postgres',
	q{SELECT pid FROM pg_stat_activity WHERE backend_type = 'managed recovery process'}));
ok($mrp0_pid =~ /^\d+$/, 'L9 found node0 MRP pid');

my $standby0_log_offset = -s $standby0->logfile;

kill 'STOP', $mrp0_pid or die "SIGSTOP $mrp0_pid: $!";

# Push one more record at the lease-expired master: the INV-ADG5 gate must
# fail stop instead of applying it.
$primary0->safe_psql('postgres', q{INSERT INTO adg_pair_p0 VALUES (3, 'post-expiry')});
my $post_expiry_scn = trim($primary0->safe_psql('postgres', q{SELECT cluster_scn_current()}));
ok(wait_for_log_match_from($standby0, $standby0_log_offset,
		qr/was not renewed within the takeover-grace window|does not hold the apply-master lease|taken over by another node/,
		120),
	'L9 lease-expired node0 fails stop before applying the next record');

# Release the frozen MRP so the shutdown can finish.  A startup-process
# fail-stop takes the whole node down (the strictest fail-stop; an external
# supervisor restarts the instance).
kill 'CONT', $mrp0_pid;
ok(wait_for_log_match_from($standby0, $standby0_log_offset,
		qr/database system is shut down/, 120),
	'L9 failed-stop node0 shuts down completely');

# node1 was never admitted: even with the master down and the lease expired
# it stays excluded (node0 is still the durable candidate) and keeps
# refusing reads.
{
	my ($ret1, $out1, $err1) = $standby1->psql('postgres', 'SELECT 1');
	isnt($ret1, 0, 'L9 node1 never became apply master and still refuses reads');
}

# Retire node1 before restarting node0: its parked postmaster still
# heartbeats the voting disks, and a bootstrapping node0 would otherwise
# seed it into the membership and stall GCS against a node that runs no
# interconnect service (it never left recovery).
$standby1->stop;

# Restart node0 as the external supervisor would.  It re-takes the lease
# with a strictly higher term and applies the deferred record exactly once.
$standby0->stop('fast', fail_ok => 1);    # reconcile tracked pid with the dead postmaster
$standby0->start;

poll_psql_value($standby0,
	qq{SELECT apply_master_node_id = 0 AND apply_master_term > $term_before
	     FROM pg_stat_cluster_adg},
	't',
	'L9 node0 re-mastered under a strictly higher term');

# The deferred record is applied only now, under the new term: the read
# floor crossing the post-expiry commit SCN proves the apply happened after
# re-mastering (replay was fenced before it).  Asserted through the ADG
# view: a user-table read would consult GCS toward the retired-but-durably-
# registered node1 slot, which a standby cluster cannot judge dead (no CSSD
# below PM_RUN) -- that parked-peer membership gap is registered as
# Stage-6 coherence forward work in the spec amend.
poll_psql_value($standby0,
	qq{SELECT standby_consistent_scn >= $post_expiry_scn FROM pg_stat_cluster_adg},
	't',
	'L9 the deferred record is applied under the new term (read floor crosses it)',
	240);

$standby0->stop;
$primary1->stop;
$primary0->stop;

done_testing();
