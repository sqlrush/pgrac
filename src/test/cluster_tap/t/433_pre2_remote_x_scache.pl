#!/usr/bin/env perl
# Requires A S11/S12/S16 and exact target-block holder / ship observations.
# Global counters, a one-shot image, or the legacy phantom table are insufficient.
use strict;
use warnings;
use FindBin;
use lib "$FindBin::RealBin/../../perl", "$FindBin::RealBin/../pre2";
use Scenario;

run_scenario('pre2_remote_x_scache', [qw(cache_target cache_observation)], sub {
	my ($cluster) = @_;
	my @nodes = $cluster->nodes;
	sql($nodes[0], 'CREATE TABLE pre2_scache (id int PRIMARY KEY, v int NOT NULL)');
	sql($nodes[0], 'BEGIN; INSERT INTO pre2_scache VALUES (1, 7); COMMIT');
	# Same catalog relation and physical path on every node; no per-node DDL.
	my $path = sql($nodes[0], q{SELECT pg_relation_filepath('pre2_scache')});
	for my $i (1 .. 3)
	{
		require_equal(sql($nodes[$i], q{SELECT pg_relation_filepath('pre2_scache')}),
			$path, 'one shared relation path');
	}
	my $target = $cluster->observe('cache_target', {relation => 'pre2_scache', fork => 'main', block => 0});
	delete @$target{qw(status version)};
	require_equal($target->{path}, $path, 'observed target is the SQL relation');
	my $scope = {target => $target, holder => 0, reader => 1};
	my $before = $cluster->observe('cache_observation', $scope);
	require_equal(sql($nodes[1], 'SELECT v FROM pre2_scache WHERE id = 1'), '7',
		'first remote read under holder X');
	my $first = $cluster->observe('cache_observation', $scope);
	for (1 .. 3)
	{
		require_equal(sql($nodes[1], 'SELECT v FROM pre2_scache WHERE id = 1'),
			'7', 'repeat read from S cache');
	}
	my $repeat = $cluster->observe('cache_observation', $scope);
	$cluster->check_evidence('scache', $before, $first, $repeat, $target, 0, 1);
	sql($nodes[0], 'BEGIN; UPDATE pre2_scache SET v = 8 WHERE id = 1; COMMIT');
	require_equal(sql($nodes[1], 'SELECT v FROM pre2_scache WHERE id = 1'),
		'8', 'subsequent X write revoked the old S image');
	$cluster->stop_cluster;
}, ['cluster.read_scache = on', 'cluster.xnode_profile = on']);
