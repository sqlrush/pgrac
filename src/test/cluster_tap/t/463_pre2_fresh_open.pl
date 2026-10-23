#!/usr/bin/env perl
# Author: SqlRush <sqlrush@gmail.com>
# Fresh formation must complete OPEN before any user relation is created.
use strict;
use warnings;
use FindBin;
use lib "$FindBin::RealBin/../../perl", "$FindBin::RealBin/../pre2";
use Scenario;

run_scenario('pre2_fresh_open', [], sub {
	my ($cluster) = @_;
	my @nodes = $cluster->nodes;

	# start_cluster waits for the real OPEN observation on every member,
	# including the complete member set and each exact current writer.
	require_equal(scalar(@nodes), 4, 'fresh formation has four members');
	for my $i (0 .. $#nodes)
	{
		require_equal(sql($nodes[$i], 'SELECT 1'), '1',
			"fresh node $i is SQL-ready after OPEN");
	}
	$cluster->stop_cluster;
});
