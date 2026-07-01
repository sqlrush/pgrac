#-------------------------------------------------------------------------
#
# 333_block_device_multinode.pl
#	  spec-6.0a block_device backend 2-node coverage.
#
#	  Uses a CI-portable regular-file raw image shared by a ClusterPair.
#	  O_DIRECT and real SCSI-3 PR hardware legs remain external/manual; this
#	  TAP covers the portable 2-node correctness legs: owner-agnostic relpath
#	  mapping and a concurrent raw-layout create/extend storm over one shared
#	  device.  Crash-restart coverage is kept in the single-node raw-device TAP;
#	  ClusterPair SIGKILL leaves cluster child processes around long enough to
#	  make immediate same-data-dir restart a harness race rather than a storage
#	  assertion.
#
# IDENTIFICATION
#	  src/test/cluster_tap/t/333_block_device_multinode.pl
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use Cwd qw(abs_path);
use FindBin;
use lib "$FindBin::RealBin/../lib";

use IPC::Run qw(start finish);
use PostgreSQL::Test::ClusterPair;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep);

sub make_raw_image
{
	my ($path, $size_mb) = @_;

	open(my $fh, '>', $path) or die "open $path: $!";
	truncate($fh, $size_mb * 1024 * 1024)
	  or die "truncate $path: $!";
	close($fh) or die "close $path: $!";
}

sub quote_conf
{
	my ($path) = @_;
	$path =~ s/'/''/g;
	return $path;
}

sub start_psql_script
{
	my ($node, $sql) = @_;
	my %state = (out => '', err => '', in => $sql);
	my @argv = (
		'psql', '-X', '-q', '-v', 'ON_ERROR_STOP=1',
		'-d', $node->connstr('postgres'));

	$state{h} = start(\@argv, '<', \$state{in}, '>', \$state{out}, '2>', \$state{err});
	return \%state;
}

sub finish_psql_script
{
	my ($state) = @_;
	my $ok = eval { finish($state->{h}); };
	return ($ok ? 1 : 0, $state->{out}, $state->{err});
}

sub sum_tables_sql
{
	my ($prefix, $count) = @_;
	my @parts;

	for my $i (1 .. $count)
	{
		push @parts, "SELECT count(*)::bigint AS c FROM ${prefix}_$i";
	}
	return 'SELECT sum(c) FROM (' . join(' UNION ALL ', @parts) . ') s';
}

my $raw_dir = PostgreSQL::Test::Utils::tempdir();
my $raw_image = "$raw_dir/spec6_0a_pair_raw_device.img";
make_raw_image($raw_image, 256);
my $raw_conf = quote_conf(abs_path($raw_image));

my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'spec6raw',
	quorum_voting_disks => 3,
	extra_conf          => [
		'autovacuum = off',
		'cluster.ges_request_timeout_ms = 30000',
		'cluster.cssd_heartbeat_interval_ms = 2000',
		'cluster.cssd_dead_deadband_factor = 10',
		"cluster.shared_storage_backend = block_device",
		"cluster.block_device_path = '$raw_conf'",
		"cluster.block_device_use_odirect = off",
		"cluster.storage_fence_driver = disabled",
		"cluster.smgr_user_relations = on",
	]);

$pair->start_pair;
usleep(3_000_000);
ok($pair->wait_for_peer_state(0, 1, 'connected', 30),
	'L1 node0 sees node1 connected');
ok($pair->wait_for_peer_state(1, 0, 'connected', 30),
	'L1 node1 sees node0 connected');

my $n0 = $pair->node0;
my $n1 = $pair->node1;

is($n0->safe_psql(
		'postgres',
		q{SELECT value FROM pg_cluster_state
		   WHERE category = 'shared_fs' AND key = 'active_backend'}),
	'block_device',
	'L1 node0 active shared-storage backend is block_device');
is($n1->safe_psql(
		'postgres',
		q{SELECT value FROM pg_cluster_state
		   WHERE category = 'shared_fs' AND key = 'active_backend'}),
	'block_device',
	'L1 node1 active shared-storage backend is block_device');

$n0->safe_psql('postgres', q{
	CREATE TABLE bd_pair_owner (id int);
});
$n1->safe_psql('postgres', q{
	CREATE TABLE bd_pair_owner (id int);
});
my $path0 = $n0->safe_psql('postgres', q{SELECT pg_relation_filepath('bd_pair_owner')});
my $path1 = $n1->safe_psql('postgres', q{SELECT pg_relation_filepath('bd_pair_owner')});
is($path1, $path0,
	'L2 same-DDL owner-agnostic relation maps to the same relpath on both nodes');

$n1->safe_psql('postgres', q{
	CREATE TEMP TABLE bd_shift_001 (id int);
	CREATE TEMP TABLE bd_shift_002 (id int);
	CREATE TEMP TABLE bd_shift_003 (id int);
	CREATE TEMP TABLE bd_shift_004 (id int);
	CREATE TEMP TABLE bd_shift_005 (id int);
	CREATE TEMP TABLE bd_shift_006 (id int);
	CREATE TEMP TABLE bd_shift_007 (id int);
	CREATE TEMP TABLE bd_shift_008 (id int);
	CREATE TEMP TABLE bd_shift_009 (id int);
	CREATE TEMP TABLE bd_shift_010 (id int);
	CREATE TEMP TABLE bd_shift_011 (id int);
	CREATE TEMP TABLE bd_shift_012 (id int);
	CREATE TEMP TABLE bd_shift_013 (id int);
	CREATE TEMP TABLE bd_shift_014 (id int);
	CREATE TEMP TABLE bd_shift_015 (id int);
	CREATE TEMP TABLE bd_shift_016 (id int);
});

my $storm0 = <<'SQL';
DO $$
DECLARE
	i int;
BEGIN
	FOR i IN 1..8 LOOP
		EXECUTE format('CREATE TABLE bd_storm0_%s (id int)', i);
		EXECUTE format(
			'INSERT INTO bd_storm0_%s SELECT g FROM generate_series(1, 300) g',
			i);
	END LOOP;
END$$;
CHECKPOINT;
SQL

my $storm1 = <<'SQL';
DO $$
DECLARE
	i int;
BEGIN
	FOR i IN 1..8 LOOP
		EXECUTE format('CREATE TABLE bd_storm1_%s (id int)', i);
		EXECUTE format(
			'INSERT INTO bd_storm1_%s SELECT g FROM generate_series(1, 300) g',
			i);
	END LOOP;
END$$;
CHECKPOINT;
SQL

my $h0 = start_psql_script($n0, $storm0);
my $h1 = start_psql_script($n1, $storm1);
my ($ok0, $out0, $err0) = finish_psql_script($h0);
my ($ok1, $out1, $err1) = finish_psql_script($h1);
diag("node0 storm stdout=$out0 stderr=$err0") unless $ok0;
diag("node1 storm stdout=$out1 stderr=$err1") unless $ok1;
ok($ok0 && $ok1,
	'L17 concurrent 2-node raw-layout create/extend storm completes without overlap failure');
is($n0->safe_psql('postgres', sum_tables_sql('bd_storm0', 8)),
	'2400',
	'L17 node0 storm tables retain all rows');
is($n1->safe_psql('postgres', sum_tables_sql('bd_storm1', 8)),
	'2400',
	'L17 node1 storm tables retain all rows');

$pair->stop_pair;

done_testing();
