#!/usr/bin/env perl
# Copyright (c) 2026, pgrac contributors
# Author: SqlRush <sqlrush@gmail.com>
# PGRAC: real VACUUM and retained local MultiXact members across clean restart.
use strict;
use warnings;
use FindBin;
use lib "$FindBin::RealBin/../../perl", "$FindBin::RealBin/../pre2";
use Scenario;
use Test::More;
use Time::HiRes qw(time);
use PostgreSQL::Test::Utils qw(slurp_file);

sub maintenance_counters
{
	my ($node) = @_;
	my $value = sql($node, q{SELECT category || '.' || key || '=' || value
        FROM pg_cluster_state WHERE
        (category='visibility' AND key='vis_update_fork_count') OR
        (category='undo' AND key='horizon_admission_refuse_count')
        ORDER BY category, key});
	my %counts;
	for my $line (split /\n/, $value)
	{
		die "invalid maintenance counter: $line" unless $line =~ /^([a-z_.]+)=(\d+)$/;
		$counts{$1} = $2;
	}
	require_equal(scalar(keys %counts), '2', 'both maintenance counters available');
	return \%counts;
}

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
	my $identity = sql($nodes[1], q{SELECT oid, xmin, ctid,
        xmin::text::bigint % 16 AS origin FROM pg_database WHERE datname='template0'});
	diag("node1 template0 tuple: $identity");
	require_true($identity =~ /\|0$/, 'template0 maintenance tuple belongs to node0');
	for my $database ('template0', 'template1', 'postgres')
	{
		for my $i (0 .. 3)
		{
			my $node = $nodes[$i];
			my $before = maintenance_counters($node);
			my $cutoffs_before = sql($node, q{SELECT relfrozenxid::text || '|' || relminmxid::text
                FROM pg_class WHERE oid='mx_retained'::regclass})
				if $database eq 'postgres';
			my $started = time;
			my ($rc, $stdout, $stderr) = $node->psql($database,
				"\\set VERBOSITY verbose\nSET backtrace_functions='cluster_undo_horizon_read_admission_enforce';\nVACUUM (FREEZE, VERBOSE)",
				timeout => 60, on_error_stop => 1);
			my $elapsed = time - $started;
			my $prefix = "$ENV{TESTLOGDIR}/vacuum-node$i-$database";
			for my $stream (['stdout', $stdout], ['stderr', $stderr])
			{
				open(my $raw, '>', "$prefix.$stream->[0]") or die $!;
				print {$raw} $stream->[1];
				close($raw) or die $!;
			}
			diag("VACUUM_RAW node=$i database=$database rc=$rc seconds=$elapsed prefix=$prefix");
			require_equal($rc, '0', "node$i/$database maintenance completed");
			my $after = maintenance_counters($node);
			my $forks = $after->{'visibility.vis_update_fork_count'} - $before->{'visibility.vis_update_fork_count'};
			my $refusals = $after->{'undo.horizon_admission_refuse_count'} - $before->{'undo.horizon_admission_refuse_count'};
			diag("VACUUM_STEP node=$i database=$database rc=0 seconds=$elapsed forks=$forks refusals=$refusals before="
				. JSON::PP->new->canonical->encode($before) . ' after='
				. JSON::PP->new->canonical->encode($after));
			require_equal($refusals, '0', "node$i/$database no maintenance admission refusal");
			require_true($forks > 0, 'foreign maintenance used the update fork')
				if $database eq 'template0' && $i == 1;
			if ($database eq 'postgres')
			{
				my ($pages, $tuples) = (0, 0);
				while ($stderr =~ /shared MultiXact references retained: (\d+) pages, (\d+) tuples;/g)
				{
					$pages += $1;
					$tuples += $2;
				}
				require_true($pages > 0 && $tuples > 0,
					"node$i explicitly reported retained shared MX references");
				my $cutoffs_after = sql($node, q{SELECT relfrozenxid::text || '|' || relminmxid::text
                    FROM pg_class WHERE oid='mx_retained'::regclass});
				require_equal($cutoffs_after, $cutoffs_before,
					"node$i kept both catalog cutoffs for unexamined members");
				diag("MX_RETAINED node=$i pages=$pages tuples=$tuples cutoffs=$cutoffs_after");
			}
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
	my @restart_starts = map { -s $_->logfile // 0 } @nodes;
	$cluster->restart_cluster;
	my @restart;
	for my $i (0 .. $#nodes)
	{
		my $node = $nodes[$i];
		my $log = slurp_file($node->logfile, $restart_starts[$i]);
		my $proven = () = $log =~ /PGRAC_REASON=NATIVE_EMPTY_BOOT_PROVEN/g;
		my $floor = sql($node, q{SELECT value FROM pg_cluster_state
            WHERE key='mxid_stripe_activated_floor'});
		require_equal($proven, '0', "node$i did not recreate first-start proof after VACUUM");
		require_true($floor =~ /^[1-9][0-9]*$/ && $log !~ /(?:ERROR|FATAL|PANIC):/,
			"node$i cold restart has a binding and no refusal");
		require_equal(sql($node, q{SELECT string_agg(id||':'||v, ',' ORDER BY id) FROM mx_retained}),
			'0:0,1:10,2:20,3:30', 'retained rows after VACUUM and normal restart');
		push @restart, {node => $i, proven => $proven, bound_floor => $floor,
			log => $node->logfile};
	}
	open(my $out, '>', "$ENV{TESTLOGDIR}/mx-vacuum-restart.json") or die $!;
	print {$out} JSON::PP->new->canonical->pretty->encode({same_data_restart => \@restart});
	close($out) or die $!;
	$cluster->stop_cluster;
});
