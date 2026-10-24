#!/usr/bin/env perl
# Copyright (c) 2026, pgrac contributors
# Author: SqlRush <sqlrush@gmail.com>
# Observe a native cross-node HW dependency without terminating its owners.
use strict;
use warnings;
use FindBin;
use lib "$FindBin::RealBin/../../perl", "$FindBin::RealBin/../pre2";
use Scenario;
use PostgreSQL::Test::Utils qw(slurp_file);
use Test::More;
use JSON::PP;
use File::Path qw(make_path);
use Time::HiRes qw(clock_gettime CLOCK_MONOTONIC sleep);

my ($cluster, $error);
my $passed = eval {
	$cluster = PostgreSQL::Test::ClusterPRE2->new_cluster('pre2_wait_graph_snapshot',
		nodes => 4, blackbox => 1, required_operations => [],
		extra_conf => ['autovacuum = off']);
	# Runtime observation uses native node configuration, outside the frozen
	# PRE2 creator's extra_conf admission list.
	$_->append_conf('postgresql.conf', "cluster.trace_tx_identity = on\n") for $cluster->nodes;
	$cluster->start_cluster;
	my @nodes = $cluster->nodes;
	sql($nodes[0], 'CREATE EXTENSION test_pgrac_shared_config');
	sql($nodes[0], 'CREATE TABLE wait_graph_extension(id integer)');
	my @key = split /\|/, sql($nodes[0], q{
		SELECT d.oid,pg_relation_filenode(c.oid),
		  CASE WHEN c.reltablespace=0 THEN d.dattablespace ELSE c.reltablespace END
		FROM pg_class c CROSS JOIN pg_database d
		WHERE c.oid='wait_graph_extension'::regclass AND d.datname=current_database()});
	require_true(@key == 3 && !(grep { $_ !~ /^[1-9][0-9]*$/ } @key),
		'original HW locator must be independently known');
	my ($db,$rel,$spc) = @key;
	my $root = "$ENV{TESTLOGDIR}/wait-graph-probe";
	make_path($root);
	my @config = map { +{node => $_, connstr => $nodes[$_]->connstr('postgres')} } 0..3;
	open(my $config, '>', "$root/connections.json") or die $!;
	print {$config} encode_json(\@config);
	close($config) or die $!;
	my $holder = $nodes[0]->background_psql('postgres', timeout => 120);
	my $holder_pid = $holder->query_safe('SELECT pg_backend_pid()');
	my $waiter = $nodes[1]->background_psql('postgres', timeout => 120);
	my $waiter_pid = $waiter->query_safe('SELECT pg_backend_pid()');
	require_true($holder_pid =~ /^[1-9][0-9]*$/ && $waiter_pid =~ /^[1-9][0-9]*$/,
		'original backend PIDs must be known');
	$holder->query_safe(q{SELECT cluster_inject_fault('cluster-ko-extension-reserved','sleep',60000000)});
	$holder->query_until(qr/HOLDER_SENT/, "\\echo HOLDER_SENT\nINSERT INTO wait_graph_extension VALUES (1);\n");
	my $deadline = clock_gettime(CLOCK_MONOTONIC) + 15;
	my $reserved = 0;
	while (clock_gettime(CLOCK_MONOTONIC) < $deadline)
	{
		$reserved = slurp_file($nodes[0]->logfile) =~
		  /KO extension reservation injection: pid=$holder_pid locator=$spc\/$db\/$rel /;
		last if $reserved;
		sleep(0.02);
	}
	require_true($reserved, 'holder must pause at its original durable reservation');
	$waiter->query_until(qr/WAITER_SENT/, "\\echo WAITER_SENT\nINSERT INTO wait_graph_extension VALUES (2);\n");
	$deadline = clock_gettime(CLOCK_MONOTONIC) + 10;
	my $waiting = 0;
	while (clock_gettime(CLOCK_MONOTONIC) < $deadline)
	{
		$waiting = sql($nodes[1], "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid=$waiter_pid " .
		  "AND wait_event IN ('ClusterRelExtendWait','ClusterGesS4Wait','ClusterGesReplyWait'))") eq 't';
		last if $waiting;
		sleep(0.02);
	}
	require_true($waiting, 'second original backend must reach a native enqueue wait');
	my $python = $ENV{PYTHON} // 'python3';
	system($python, "$FindBin::RealBin/../pre2/wait_graph_installed_probe.py",
		"$root/connections.json", "$root/capture", $holder_pid, $waiter_pid,
		encode_json([map { 0+$_ } @key, 0,242,1]));
	require_equal($?, 0, 'complete native graph must prove the exact cross-node HW dependency');
	# The existing finite sleep expires; success never needs a forced cancel.
	is($holder->query_safe('SELECT 1'), '1', 'original holder completes after its finite pause');
	is($waiter->query_safe('SELECT 1'), '1', 'original waiter completes after native HW release');
	$holder->quit;
	$waiter->quit;
	require_equal(sql($_, 'SELECT count(*),sum(id) FROM wait_graph_extension'),
		'2|3', 'both inserts remain visible') for @nodes;
	$cluster->stop_cluster;
	unlike(slurp_file($_->logfile), qr/\b(?:ERROR|FATAL|PANIC):/,
		'normal native probe has no server error') for @nodes;
	1;
};
$error = $@ unless $passed;
if (!$passed && $cluster && $cluster->{running})
{
	my $clean = eval { $cluster->stop_cluster; 1 };
	$error .= "normal cleanup also failed: $@" unless $clean;
}
ok($passed, 'complete installed graph joins the original cross-node HW owner');
diag($error) unless $passed;
done_testing();
