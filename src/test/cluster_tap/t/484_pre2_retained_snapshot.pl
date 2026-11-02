#!/usr/bin/env perl
#-------------------------------------------------------------------------
# 484_pre2_retained_snapshot.pl
#    Native long-reader/exported/parallel snapshot preservation with VACUUM.
#    This subcheck does not certify recycle or frozen-horizon advancement.
#
# Copyright (c) 2026, pgrac contributors
# Author: SqlRush <sqlrush@gmail.com>
#-------------------------------------------------------------------------
use strict;
use warnings;
use FindBin;
use lib "$FindBin::RealBin/../../perl";
use PostgreSQL::Test::ClusterPRE2;
use PostgreSQL::Test::Utils qw(slurp_file);
use Test::More;
use JSON::PP;
use Time::HiRes qw(clock_gettime CLOCK_MONOTONIC);

my ($cluster, @nodes, @actors);
my $evidence = $ENV{TESTLOGDIR} // die 'native evidence directory missing';
my $rows_sql = 'SELECT id::text || chr(58) || v::text FROM g_m1_retained ORDER BY id';
my $base_rows = "0:10\n1:20\n2:30\n3:40";
my $new_rows = "0:110\n1:220\n2:330\n3:440";
my $stop_attempted = 0;

sub save_json
{
	my ($name, $value) = @_;
	open my $file, '>', "$evidence/retained-$name.json" or die $!;
	print {$file} JSON::PP->new->canonical->pretty->encode($value);
	close $file or die $!;
}

sub query_actor
{
	my ($actor, $sql) = @_;
	my $value = eval { $actor->{client}->query_safe($sql) };
	if ($@)
	{
		my $error = $@;
		save_json("$actor->{name}-query-error", {sql => $sql, error => $error,
			stderr => $actor->{client}{stderr}});
		die $error;
	}
	return $value;
}

sub actor
{
	my ($name, $node) = @_;
	my $entry = {name => $name, node => $node, transaction => 0,
		client => $nodes[$node]->background_psql('postgres', timeout => 120)};
	push @actors, $entry;
	$entry->{pid} = query_actor($entry, 'SELECT pg_backend_pid()');
	die 'invalid native actor PID' unless $entry->{pid} =~ /^[1-9][0-9]*$/;
	$entry->{backend_start} = $nodes[$node]->safe_psql('postgres',
		"SELECT backend_start::text FROM pg_stat_activity WHERE pid=$entry->{pid}", timeout => 10);
	die 'missing native actor birth' unless length($entry->{backend_start});
	save_json("$name-identity", {node => $node, pid => $entry->{pid},
		backend_start => $entry->{backend_start}});
	return $entry;
}

sub begin_reader
{
	my ($entry, $snapshot) = @_;
	$entry->{transaction} = 1;
	query_actor($entry, 'BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY');
	# Import before the first snapshot-taking SQL in this transaction.
	query_actor($entry, "SET TRANSACTION SNAPSHOT '$snapshot'") if defined $snapshot;
}

sub close_actor
{
	my ($entry, $command) = @_;
	return if $entry->{closed};
	if ($entry->{transaction})
	{
		query_actor($entry, $command);
		$entry->{transaction} = 0;
	}
	die "native actor $entry->{name} did not exit cleanly" unless $entry->{client}->quit;
	$entry->{closed} = 1;
}

sub check_reader
{
	my ($entry, $phase) = @_;
	my $rows = query_actor($entry, $rows_sql);
	save_json("$phase-$entry->{name}", {node => $entry->{node}, pid => $entry->{pid}, rows => $rows});
	is($rows, $base_rows, "$phase: $entry->{name} retains original row versions")
	  or die 'retained snapshot changed';
	if ($entry->{parallel})
	{
		my $text = query_actor($entry,
			'EXPLAIN (ANALYZE, FORMAT JSON) SELECT sum(v) FROM g_m1_retained');
		my $plan = decode_json($text);
		my $workers = 0;
		my $walk;
		$walk = sub {
			my ($item) = @_;
			if (ref($item) eq 'HASH')
			{
				$workers += $item->{'Workers Launched'} // 0;
				$walk->($_) for values %$item;
			}
			elsif (ref($item) eq 'ARRAY') { $walk->($_) for @$item; }
		};
		$walk->($plan);
		save_json("$phase-parallel-plan", {explain => $plan, workers_launched => $workers});
		cmp_ok($workers, '>', 0, "$phase: actual parallel snapshot worker launched")
		  or die 'parallel snapshot path not reached';
		is(query_actor($entry, 'SELECT sum(v)::text FROM g_m1_retained'), '100',
			"$phase: forced parallel read retains original sum") or die 'parallel result changed';
	}
}

sub fresh_rows
{
	my ($phase) = @_;
	my @cuts = map { $_->safe_psql('postgres', $rows_sql, timeout => 30) } @nodes;
	save_json("$phase-fresh-rows", \@cuts);
	for my $i (0 .. 3)
	{
		is($cuts[$i], $new_rows, "$phase: node$i sees only committed updates")
		  or die 'fresh row ledger mismatch';
	}
}

sub vacuum
{
	my ($node, $phase) = @_;
	my $start = clock_gettime(CLOCK_MONOTONIC);
	my ($rc, $stdout, $stderr) = $nodes[$node]->psql('postgres',
		'VACUUM (FREEZE, VERBOSE) g_m1_retained', timeout => 60, on_error_stop => 1);
	save_json("$phase-vacuum-node$node", {node => $node, rc => $rc,
		stdout => $stdout, stderr => $stderr,
		elapsed_monotonic_seconds => clock_gettime(CLOCK_MONOTONIC) - $start});
	is($rc, 0, "$phase: original VACUUM on node$node completes") or die 'native VACUUM failed';
}

my $execution = eval {
	$cluster = PostgreSQL::Test::ClusterPRE2->new_cluster('m1_retained_snapshot', nodes => 4,
		blackbox => 1, required_operations => [], extra_conf => ['autovacuum=off',
			'max_connections=100', 'max_worker_processes=32', 'max_parallel_workers=4',
			'cluster.undo_cleaner_interval_ms=1000', 'log_error_verbosity=verbose',
			q{log_line_prefix='%m [%p] '}]);
	$cluster->start_cluster;
	@nodes = $cluster->nodes;
	is(scalar(@nodes), 4, 'original PRE2 formation admits four normal members') or die 'formation differs';
	save_json('formation', [map { $cluster->observe('open_observation', {node => $_}) } 0 .. 3]);
	$nodes[0]->safe_psql('postgres', 'CREATE TABLE g_m1_retained (id int PRIMARY KEY, v int NOT NULL); '
		. 'INSERT INTO g_m1_retained VALUES (0,10),(1,20),(2,30),(3,40)', timeout => 60);
	my @readers;
	for my $i (0 .. 3)
	{
		my $reader = actor("reader$i", $i);
		begin_reader($reader);
		check_reader($reader, 'initial');
		push @readers, $reader;
	}
	my $export = decode_json(query_actor($readers[0], q{
WITH exported AS MATERIALIZED (SELECT pg_export_snapshot() AS id)
SELECT json_build_object('id', id, 'contents', pg_read_file('pg_snapshots/' || id)) FROM exported}));
	save_json('export', $export);
	die 'invalid original exported snapshot' unless ref($export) eq 'HASH'
	  && ($export->{id} // '') =~ /^[0-9A-Fa-f]+-[0-9A-Fa-f]+-[0-9]+$/
	  && length($export->{contents} // '');
	my $importer = actor('importer', 0);
	begin_reader($importer, $export->{id});
	my $parallel = actor('parallel_importer', 0);
	query_actor($parallel, 'SET debug_parallel_query=on; SET max_parallel_workers_per_gather=1');
	$parallel->{parallel} = 1;
	begin_reader($parallel, $export->{id});
	check_reader($_, 'initial') for ($importer, $parallel);
	my @writes;
	for my $i (0 .. 3)
	{
		my $writer = actor("writer$i", $i);
		$writer->{transaction} = 1;
		query_actor($writer, 'BEGIN');
		query_actor($writer, 'UPDATE g_m1_retained SET v=v+'.(100*($i+1))." WHERE id=$i");
		my $xid = query_actor($writer, q{SELECT pg_current_xact_id_if_assigned()::text || '|' ||
    (pg_current_xact_id_if_assigned()::text::numeric % 16)::text});
		die 'writer lacks exact native striped FullXid' unless $xid =~ /^[1-9][0-9]*\|\Q$i\E$/;
		close_actor($writer, 'COMMIT');
		push @writes, {node => $i, pid => $writer->{pid}, xid_origin => $xid};
		save_json('committed-writers', \@writes);
	}
	my $aborted = actor('aborted_writer', 2);
	$aborted->{transaction} = 1;
	query_actor($aborted, 'BEGIN; UPDATE g_m1_retained SET v=v+999 WHERE id=2');
	my $aborted_xid = query_actor($aborted, 'SELECT pg_current_xact_id_if_assigned()::text');
	die 'aborted writer lacks native FullXid' unless $aborted_xid =~ /^[1-9][0-9]*$/;
	close_actor($aborted, 'ROLLBACK');
	save_json('aborted-writer', {node => 2, pid => $aborted->{pid}, xid => $aborted_xid});
	fresh_rows('committed');
	for my $i (0 .. 3) { vacuum($i, 'all-readers'); }
	check_reader($_, 'all-readers') for (@readers, $importer, $parallel);
	close_actor($_, 'COMMIT') for @readers;
	vacuum(1, 'exporter-retired');
	check_reader($_, 'exporter-retired') for ($importer, $parallel);
	close_actor($importer, 'COMMIT');
	vacuum(2, 'ordinary-importer-retired');
	check_reader($parallel, 'ordinary-importer-retired');
	close_actor($parallel, 'COMMIT');
	for my $i (0 .. 3) { vacuum($i, 'all-readers-retired'); }
	fresh_rows('all-readers-retired');
	1;
};
my $first_error = $execution ? '' : $@;
my $cleanup_error = '';
for my $entry (@actors)
{
	next if $entry->{closed};
	# The original error is saved before clearing psql's accumulated stderr.
	save_json("$entry->{name}-cleanup-before", {stderr => $entry->{client}{stderr}});
	$entry->{client}{stderr} = '';
	eval { close_actor($entry, 'ROLLBACK'); 1 } or $cleanup_error .= $@;
}
my $normal_stop = 0;
if (defined($cluster) && $cluster->{running} && !$stop_attempted)
{
	$stop_attempted = 1;
	eval {
		my $receipt = $cluster->stop_cluster;
		save_json('normal-stop', $receipt);
		die 'full original normal-stop proof missing' unless $cluster->{normal_stop_proven};
		$normal_stop = 1;
		pass('all native members complete original normal stop');
		1;
	} or $cleanup_error .= $@;
}
my $logs_clean = 1;
for my $node (@nodes)
{
	$logs_clean = 0 unless unlike(slurp_file($node->logfile),
		qr/PANIC|checksum verification failed|invalid page in block/,
		'closed native member has no corruption or PANIC');
}
save_json('execution', {retained_reads_and_stop_passed =>
	($execution && $normal_stop && $logs_clean && $cleanup_error eq '') ? JSON::PP::true : JSON::PP::false,
	error => $first_error, cleanup_error => $cleanup_error,
	p02 => 'NOT_READY', stage_acceptance => 'NOT_READY',
	missing_gates => ['recycle and frozen-horizon progression after final reader release']});
if (!$execution || !$normal_stop || !$logs_clean || $cleanup_error ne '')
{
	fail('native retained-reader execution and cleanup complete');
	diag($first_error . $cleanup_error);
}
done_testing();
