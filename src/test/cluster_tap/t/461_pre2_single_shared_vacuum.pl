#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 461_pre2_single_shared_vacuum.pl
#    Native single-member shared storage retains local MultiXact maintenance.
#
# Copyright (c) 2026, pgrac contributors
# Author: SqlRush <sqlrush@gmail.com>
#
# IDENTIFICATION
#    src/test/cluster_tap/t/461_pre2_single_shared_vacuum.pl
#
# NOTES
#    The native creator owns all persistent authority. No peer, stripe,
#    imported control file, or fabricated bootstrap marker is used.
#    Run with pre2/run_single_shared_vacuum.py in an isolated Linux VM.
#-------------------------------------------------------------------------
use strict;
use warnings;
use FindBin;
use lib "$FindBin::RealBin/../../perl";
use Cwd qw(realpath);
use IPC::Run;
use JSON::PP;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(time);

sub quoted
{
	my ($value) = @_;
	die 'invalid configuration scalar' if $value =~ /[\r\n\0]/;
	$value =~ s/\\/\\\\/g;
	$value =~ s/'/''/g;
	return "'$value'";
}

sub equal
{
	my ($got, $expected, $label) = @_;
	die "$label: expected [$expected], got [$got]\n" unless $got eq $expected;
}

my $root = realpath(PostgreSQL::Test::Utils::tempdir());
my $node = PostgreSQL::Test::Cluster->new('single_shared_vacuum');
my $data = $node->data_dir;
my $result = {phases => [], shared_config => 'on', xid_striping => 'off'};
my @holders;
local $ENV{PGCTLTIMEOUT} = 30;

my $passed = eval {
	my $fixture = decode_json(slurp_file($ENV{PGRAC_SINGLE_SHARED_FIXTURE}));
	die 'single-member quorum fixture required'
		unless $fixture->{count} == 1 && @{$fixture->{votes}} == 3;
	my %common = (
		'cluster.controlfile_shared_authority' => 'on',
		'cluster.enabled' => 'on', 'cluster.allow_single_node' => 'on',
		'cluster.shared_catalog' => 'on', 'cluster.shared_config' => 'on',
		'cluster.merged_recovery' => 'on', 'cluster.xid_striping' => 'off',
		'cluster.online_join' => 'on', 'cluster.lms_enabled' => 'on',
		'cluster.lms_workers' => 1, 'cluster.interconnect_tier' => 'tier1',
		'cluster.crossnode_runtime_visibility' => 'on',
		'cluster.undo_gcs_coherence' => 'on',
		'cluster.cf_enqueue_timeout_ms' => 1000,
		'cluster.storage_quorum_cluster' => $fixture->{name},
		'cluster.storage_quorum_nodes' => '0:1',
		'cluster.shared_data_dir' => "$root/shared",
		'cluster.shared_storage_backend' => 'cluster_fs',
		'cluster.shared_storage_uuid' => '01234567-89ab-cdef-0123-456789abcdef',
		'cluster.smgr_user_relations' => 'on',
		'cluster.undo_tablespace_path' => "$root/undo",
		'cluster.wal_threads_dir' => "$root/wal",
		'autovacuum' => 'off', 'wal_level' => 'replica');
	my %instance = ('cluster.node_id' => 0, port => $node->port,
		unix_socket_directories => $node->host, listen_addresses => '',
		shared_buffers => '16MB');
	my $request = "\@authority_uuid=123456789abc4ef0923456789abcdef0\n"
		. "\@configured_0=0000000000000001\n\@configured_1=0000000000000000\n"
		. "\@database_incarnation=1\n\@format=1\n\@generation=1\n"
		. "\@storage_uuid=0123456789abcdef0123456789abcdef\n"
		. "\@system_identifier=7584383251700000001\n";
	$request .= 'common.' . $_ . '=' . quoted($common{$_}) . "\n" for sort keys %common;
	$request .= 'node000.' . $_ . '=' . quoted($instance{$_}) . "\n" for sort keys %instance;
	append_to_file("$root/request.conf", $request);
	chmod 0600, "$root/request.conf" or die "chmod request: $!";
	my ($out, $err);
	my $created = IPC::Run::run(['initdb', '-D', "$root/cohort", '-k', '-A', 'trust',
		'--no-locale', '--pgrac-initdb-cohort',
		"--pgrac-initdb-shared-config=$root/request.conf"], '>', \$out, '2>', \$err);
	append_to_file("$ENV{TESTLOGDIR}/single-initdb.stdout", $out);
	append_to_file("$ENV{TESTLOGDIR}/single-initdb.stderr", $err);
	die "native one-member creation failed: $err" unless $created;
	rename "$root/cohort/node_0", $data or die "move native DATA: $!";
	my %discovery = map { $_ => $common{$_} } qw(cluster.enabled cluster.shared_config
		cluster.controlfile_shared_authority cluster.shared_data_dir
		cluster.wal_threads_dir cluster.undo_tablespace_path);
	$discovery{'cluster.node_id'} = 0;
	$discovery{'cluster.voting_disks'} = join(',', @{$fixture->{votes}});
	$node->append_conf('postgresql.conf', join("\n",
		map { $_ . '=' . quoted($discovery{$_}) } sort keys %discovery));
	my $ic_port = PostgreSQL::Test::Cluster::get_free_port();
	my $data_port = PostgreSQL::Test::Cluster::get_free_port();
	$node->append_conf('pgrac.conf', "[cluster]\nname=single_shared_vacuum\n"
		. "[node.0]\ninterconnect_addr=127.0.0.1:$ic_port\n"
		. "data_addr=127.0.0.1:$data_port\n");
	die 'single shared startup failed' unless $node->start(fail_ok => 1);
	push @{$result->{phases}}, 'started';
	equal($node->safe_psql('postgres', 'SHOW cluster.shared_config'), 'on', 'shared profile');
	equal($node->safe_psql('postgres', 'SHOW cluster.xid_striping'), 'off', 'dense IDs');
	equal($node->safe_psql('postgres', 'SELECT count(*) FROM pg_cluster_nodes'), '1', 'one member');
	$node->safe_psql('postgres', 'CREATE TABLE local_mx (id int PRIMARY KEY, v int); INSERT INTO local_mx VALUES (1, 10)');
	for (1 .. 2)
	{
		my $holder = $node->background_psql('postgres', timeout => 30);
		$holder->query_safe('BEGIN; SELECT v FROM local_mx WHERE id=1 FOR KEY SHARE');
		push @holders, $holder;
	}
	my $mx = $node->safe_psql('postgres', 'SELECT xmax::text FROM local_mx');
	die 'no local MultiXact created' unless $mx =~ /^[1-9][0-9]*$/;
	$result->{multi} = $mx;
	for my $phase ('active', 'committed')
	{
		if ($phase eq 'committed')
		{
			for my $holder (@holders) { $holder->query_safe('COMMIT'); $holder->quit; }
			@holders = ();
		}
		my $started = time;
		my ($rc, $stdout, $stderr) = $node->psql('postgres',
			'VACUUM (FREEZE, VERBOSE) local_mx', timeout => 60, on_error_stop => 1);
		append_to_file("$ENV{TESTLOGDIR}/single-$phase.stdout", $stdout);
		append_to_file("$ENV{TESTLOGDIR}/single-$phase.stderr", $stderr);
		push @{$result->{phases}}, {phase => $phase, rc => $rc, seconds => time - $started};
		equal($rc, '0', "$phase local VACUUM");
		die 'single-member page incorrectly retained' if $stderr =~ /shared MultiXact references retained:/;
		if ($phase eq 'active')
		{
			equal($node->safe_psql('postgres', "SELECT count(*) FROM pg_get_multixact_members('$mx')"),
				'2', 'both active local members remain');
		}
	}
	my $minimum = $node->safe_psql('postgres', "SELECT relminmxid::text FROM pg_class WHERE oid='local_mx'::regclass");
	$result->{relminmxid} = $minimum;
	die 'native local maintenance did not advance past committed members' unless $minimum > $mx;
	die 'first normal stop failed' unless $node->stop('fast', fail_ok => 1);
	die 'same-DATA restart failed' unless $node->start(fail_ok => 1);
	equal($node->safe_psql('postgres', 'SELECT id || \':\' || v FROM local_mx'), '1:10', 'row after restart');
	die 'second normal stop failed' unless $node->stop('fast', fail_ok => 1);
	die 'unexpected database error' if slurp_file($node->logfile) =~ /(?:ERROR|FATAL|PANIC):/;
	$result->{normal_stops} = 2;
	1;
};
my $error = $@;
if (!$passed)
{
	for my $holder (@holders) { eval { $holder->quit }; }
	$node->stop('fast', fail_ok => 1);
}
$result->{passed} = $passed ? JSON::PP::true : JSON::PP::false;
$result->{error} = $error if $error;
open(my $json, '>', "$ENV{TESTLOGDIR}/single-shared-vacuum.json") or die $!;
print {$json} JSON::PP->new->canonical->pretty->encode($result);
close($json) or die $!;
ok($passed, 'single-member shared storage keeps native local MX maintenance');
diag($error) unless $passed;
done_testing();
