#!/usr/bin/env perl
# Copyright (c) 2026, pgrac contributors
# Author: SqlRush <sqlrush@gmail.com>
# PGRAC: native MultiXact initialization and same-DATA clean restart.
use strict;
use warnings;
use FindBin;
use lib "$FindBin::RealBin/../../perl", "$FindBin::RealBin/../pre2";
use Scenario;
use Test::More;
use PostgreSQL::Test::Utils qw(slurp_file);
use JSON::PP;
use IPC::Run qw(run timeout);

sub proof_snapshot
{
	my ($nodes, $starts, $cold) = @_;
	my @rows;
	for my $i (0 .. $#$nodes)
	{
		my $log = slurp_file($nodes->[$i]->logfile, $starts->[$i] // 0);
		my $proven = () = $log =~ /PGRAC_REASON=NATIVE_EMPTY_BOOT_PROVEN/g;
		require_true($log !~ /FATAL:|PANIC:|(?:ERROR|FATAL):[^\n]*(?:55000|XX001)/,
			"node$i has no startup refusal");
		my $floor = sql($nodes->[$i], q{SELECT value FROM pg_cluster_state
			WHERE key='mxid_stripe_activated_floor'});
		require_true($cold ? $proven == 0 : ($proven == 1 || ($proven == 0 && $floor =~ /^[1-9][0-9]*$/)),
			"node$i has the original startup proof or a real binding");
		push @rows, {node => $i, proven => $proven, bound_floor => $floor,
			path => $proven ? 'NATIVE_EMPTY_BOOT' : 'STRIPED', log => $nodes->[$i]->logfile};
	}
	require_true((grep { $_->{proven} == 1 } @rows) > 0,
		'fresh formation exercised the native empty branch') unless $cold;
	return \@rows;
}
sub save_summary
{
	my ($cluster, $summary) = @_;
	open(my $out, '>', "$cluster->{layout}{root}/mx-startup-proof.json") or die $!;
	print {$out} JSON::PP->new->canonical->pretty->encode($summary);
	close($out) or die $!;
}
run_scenario('pre2_multixact_restart', [], sub {
	my ($cluster) = @_;
	my @nodes = $cluster->nodes;
	my $summary = {first_start => proof_snapshot(\@nodes, [], 0)};
	save_summary($cluster, $summary);
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
	for my $i (0 .. 3)
	{
		require_equal(sql($nodes[$i], "SELECT count(*) FROM pg_get_multixact_members('$multis[$i]')"),
			'2', "node$i retains both live members before clean stop");
		for my $holder (@{$holders[$i]})
		{
			$holder->query_safe('COMMIT');
			$holder->quit;
		}
	}
	$cluster->stop_cluster;
	my $copy_dir = "$ENV{TESTDATADIR}/../first-stop-copy";
	my ($copy_out, $copy_err) = ('', '');
	run([$ENV{PYTHON} // 'python3', "$FindBin::RealBin/../pre2/mx_cold_copy.py",
		'capture', "$cluster->{layout}{root}/blackbox-layout.json",
		$ENV{TESTDATADIR}, $copy_dir], '>', \$copy_out, '2>', \$copy_err, timeout(60))
		or die "cold copy capture failed: $copy_err";
	$summary->{first_stop_copy} = decode_json($copy_out);
	save_summary($cluster, $summary);
	my @restart_starts = map { -s $_->logfile // 0 } @nodes;
	$cluster->restart_cluster;
	$summary->{same_data_restart} = proof_snapshot(\@nodes, \@restart_starts, 1);
	save_summary($cluster, $summary);
	for my $node (@nodes)
	{
		require_equal(sql($node, q{SELECT string_agg(id||':'||v, ',' ORDER BY id) FROM mx_retained}),
			'0:0,1:10,2:20,3:30', 'retained rows after normal restart');
	}
	$cluster->stop_cluster;
	for my $node (@nodes)
	{
		require_true(slurp_file($node->logfile) !~ /ERROR:|FATAL:|PANIC:/,
			'all positive lifecycle logs have no ERROR, FATAL or PANIC');
	}
});
