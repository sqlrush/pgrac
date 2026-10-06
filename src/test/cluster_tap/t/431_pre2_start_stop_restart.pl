#!/usr/bin/env perl
# Author: SqlRush <sqlrush@gmail.com>
# Requires A S11/S12 fresh initialization/atomic activation, S16 OPEN, S17
# checkpoint + exact CLOSED writer observation, S18 same-DATA restart.
use strict;
use warnings;
use FindBin;
use lib "$FindBin::RealBin/../../perl", "$FindBin::RealBin/../pre2";
use Scenario;

run_scenario('pre2_start_stop_restart', [], sub {
	my ($cluster) = @_;
	my @nodes = $cluster->nodes;
	sql($nodes[0], 'CREATE TABLE pre2_chain (id int PRIMARY KEY, v int NOT NULL)');
	for my $i (0 .. 3)
	{
		sql($nodes[$i], "BEGIN; INSERT INTO pre2_chain VALUES ($i, $i); COMMIT");
	}
	# Every member commits against a row written by a different member.
	for my $i (0 .. 3)
	{
		my $key = ($i + 1) % 4;
		sql($nodes[$i], "BEGIN; UPDATE pre2_chain SET v = v + 10 WHERE id = $key; COMMIT");
	}
	for my $node (@nodes)
	{
		require_equal(sql($node, q{SELECT string_agg(id || ':' || v, ',' ORDER BY id) FROM pre2_chain}),
			'0:10,1:11,2:12,3:13', 'all committed rows before shutdown');
	}
	$cluster->stop_cluster;
	$cluster->restart_cluster; # Does not call fresh_init or recreate any DATA.
	for my $node (@nodes)
	{
		require_equal(sql($node, q{SELECT string_agg(id || ':' || v, ',' ORDER BY id) FROM pre2_chain}),
			'0:10,1:11,2:12,3:13', 'same-DATA committed rows after restart');
	}
	sql($nodes[2], 'BEGIN; UPDATE pre2_chain SET v = v + 100 WHERE id = 0; COMMIT');
	require_equal(sql($nodes[3], 'SELECT v FROM pre2_chain WHERE id = 0'),
		'110', 'cross-node commit after same-DATA restart');
	$cluster->stop_cluster;
});
