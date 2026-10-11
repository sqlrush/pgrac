#!/usr/bin/env perl
# Author: SqlRush <sqlrush@gmail.com>
# PGRAC: real VACUUM and retained local MultiXact members; lifecycle tests are separate.
use strict;
use warnings;
use FindBin;
use lib "$FindBin::RealBin/../../perl", "$FindBin::RealBin/../pre2";
use Scenario;
use Test::More;

run_scenario('pre2_multixact_maintenance', [], sub {
	my ($cluster) = @_;
	my @nodes = $cluster->nodes;
	my $probe = $ENV{PGRAC_PRE2_MX_PROBE};
	die 'missing compiled native range probe' unless defined($probe)
	  && $probe =~ m{^/[a-zA-Z0-9_./-]+$} && -f $probe;
	sql($nodes[0], "CREATE FUNCTION pre2_mx_range() RETURNS text AS '$probe' LANGUAGE C");
	sql($nodes[0], 'CREATE TABLE mx_retained (id int PRIMARY KEY, v int NOT NULL)');
	sql($nodes[0], 'INSERT INTO mx_retained SELECT i, i * 10 FROM generate_series(0,3) i');
	my (@holders, @multis);
	for my $i (0 .. 3)
	{
		my $a = $nodes[$i]->background_psql('postgres', timeout => 30);
		my $b = $nodes[$i]->background_psql('postgres', timeout => 30);
		$a->query_safe("BEGIN; SELECT v FROM mx_retained WHERE id=$i FOR KEY SHARE");
		$b->query_safe("BEGIN; SELECT v FROM mx_retained WHERE id=$i FOR KEY SHARE");
		my $mx = sql($nodes[$i], "SELECT xmax::text FROM mx_retained WHERE id=$i");
		require_true($mx =~ /^\d+$/ && $mx > 0 && $mx % 16 == $i,
			"node$i produced its own native MultiXact");
		push @holders, [$a, $b];
		push @multis, $mx;
		diag("node$i before " . sql($nodes[$i], 'SELECT pre2_mx_range()') . " mx=$mx");
	}
	# Use the existing VACUUM catalog path; no direct cursor or SLRU edits.
	sql($nodes[0], 'ALTER DATABASE template0 ALLOW_CONNECTIONS true');
	for my $database ('template0', 'template1', 'postgres')
	{
		for my $node (@nodes)
		{
			$node->safe_psql($database, "\\set VERBOSITY verbose\nVACUUM (FREEZE, VERBOSE)", timeout => 60);
		}
	}
	sql($nodes[0], 'ALTER DATABASE template0 ALLOW_CONNECTIONS false');
	my $foreign_lower = 0;
	for my $i (0 .. 3)
	{
		my $range = sql($nodes[$i], 'SELECT pre2_mx_range()');
		my ($lower, $next) = split /\|/, $range;
		my $floor = sql($nodes[$i], q{SELECT value FROM pg_cluster_state
            WHERE key='mxid_stripe_activated_floor'});
		$foreign_lower++ if $lower >= $floor && $lower % 16 != $i;
		diag("node$i after VACUUM $range floor=$floor");
		my $members = sql($nodes[$i], "SELECT count(*) FROM pg_get_multixact_members('$multis[$i]')");
		require_equal($members, '2', "node$i retained both live members");
		for my $holder (@{$holders[$i]})
		{
			$holder->query_safe('COMMIT');
			$holder->quit;
		}
	}
	require_true($foreign_lower > 0, 'real VACUUM reached a foreign striped lower bound');
	$cluster->stop_cluster;
});
