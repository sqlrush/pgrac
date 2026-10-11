#!/usr/bin/env perl
# Author: SqlRush <sqlrush@gmail.com>
# PGRAC: retained subtransaction parentage across checkpoint and clean restart.
use strict;
use warnings;
use FindBin;
use lib "$FindBin::RealBin/../../perl", "$FindBin::RealBin/../pre2";
use Scenario;

run_scenario('pre2_subtrans_stop', [], sub {
	my ($cluster) = @_;
	my @nodes = $cluster->nodes;
	my @parents;
	sql($nodes[0], 'CREATE TABLE subtrans_retained (id int PRIMARY KEY, v int NOT NULL)');
	sql($nodes[0], 'INSERT INTO subtrans_retained SELECT i, 0 FROM generate_series(0,3) i');
	for my $i (0 .. 3)
	{
		my $parent = $nodes[$i]->background_psql('postgres', timeout => 30);
		$parent->query_safe("BEGIN; SAVEPOINT keep; UPDATE subtrans_retained SET v=1 WHERE id=$i;
            SAVEPOINT discard; UPDATE subtrans_retained SET v=999 WHERE id=$i;
            ROLLBACK TO discard; RELEASE discard; RELEASE keep");
		push @parents, $parent;
	}
	for my $i (0 .. 3)
	{
		sql($nodes[$i], 'CHECKPOINT');
		require_equal(sql($nodes[$i], 'SELECT sum(v) FROM subtrans_retained'),
			'0', 'active parents remain invisible through checkpoint');
	}
	for my $parent (@parents)
	{
		$parent->query_safe('COMMIT');
		$parent->quit;
	}
	for my $node (@nodes)
	{
		require_equal(sql($node, q{SELECT string_agg(id||':'||v, ',' ORDER BY id) FROM subtrans_retained}),
			'0:1,1:1,2:1,3:1', 'committed children survive inner rollback');
	}
	$cluster->stop_cluster;
	$cluster->restart_cluster;
	for my $node (@nodes)
	{
		require_equal(sql($node, q{SELECT string_agg(id||':'||v, ',' ORDER BY id) FROM subtrans_retained}),
			'0:1,1:1,2:1,3:1', 'same DATA preserves committed parentage');
	}
	$cluster->stop_cluster;
});
