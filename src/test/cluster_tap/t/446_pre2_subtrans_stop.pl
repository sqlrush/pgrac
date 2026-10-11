#!/usr/bin/env perl
# Copyright (c) 2026, pgrac contributors
# Author: SqlRush <sqlrush@gmail.com>
# PGRAC: retained subtransaction parentage across checkpoint and clean restart.
use strict;
use warnings;
use FindBin;
use lib "$FindBin::RealBin/../../perl", "$FindBin::RealBin/../pre2";
use Scenario;
use JSON::PP;
use PostgreSQL::Test::Utils qw(slurp_file);

sub subtrans_segments
{
	my ($node) = @_;
	my $names = sql($node, q{SELECT coalesce(string_agg(name, ',' ORDER BY name), '')
        FROM pg_ls_dir('pg_subtrans') AS name WHERE name ~ '^[0-9A-F]{4,6}$'});
	return [length($names) ? split(/,/, $names) : ()];
}

sub save_segments
{
	my ($summary) = @_;
	open(my $out, '>', "$ENV{TESTLOGDIR}/subtrans-segments.json") or die $!;
	print {$out} JSON::PP->new->canonical->pretty->encode($summary);
	close($out) or die $!;
}

sub clean_subtrans_logs
{
	my ($nodes) = @_;
	for my $node (@$nodes)
	{
		my $log = slurp_file($node->logfile);
		require_true($log !~ /could not fsync file "pg_subtrans\/|(?:ERROR|FATAL|PANIC):/,
			'deletion and later sync leave complete node logs clean');
	}
}

run_scenario('pre2_subtrans_stop', [], sub {
	my ($cluster) = @_;
	my @nodes = $cluster->nodes;
	my @parents;
	my $block_size = sql($nodes[0], 'SHOW block_size');
	require_true($block_size =~ /^[1-9][0-9]*$/ && $block_size % 4 == 0,
		'valid native SUBTRANS page size');
	# Native SUBTRANS stores 4-byte XIDs, with 32 pages per SLRU segment.
	my $xids_per_segment = ($block_size / 4) * 32;
	my $summary = {xids_per_segment => $xids_per_segment, nodes => []};
	for my $i (0 .. 3)
	{
		push @{$summary->{nodes}}, {node => $i, initial => subtrans_segments($nodes[$i])};
	}
	save_segments($summary);
	sql($nodes[0], 'CREATE TABLE subtrans_retained (id int PRIMARY KEY, v int NOT NULL)');
	sql($nodes[0], 'INSERT INTO subtrans_retained SELECT i, 0 FROM generate_series(0,3) i');
	for my $i (0 .. 3)
	{
		my $parent = $nodes[$i]->background_psql('postgres', timeout => 30);
		$parent->query_safe("BEGIN; SAVEPOINT keep; UPDATE subtrans_retained SET v=1 WHERE id=$i;
            SAVEPOINT discard; UPDATE subtrans_retained SET v=999 WHERE id=$i;
            ROLLBACK TO discard; RELEASE discard; RELEASE keep");
		my $identity = $parent->query_safe("SELECT pg_current_xact_id_if_assigned()::text,
            xmin::text FROM subtrans_retained WHERE id=$i");
		my ($top, $child) = split /\|/, $identity;
		require_true(defined($child) && $top =~ /^[1-9][0-9]*$/
			&& $child =~ /^[1-9][0-9]*$/ && $top <= 4294967295
			&& $child <= 4294967295 && $top != $child,
			"node$i has a real retained child and parent in the fresh XID epoch");
		$summary->{nodes}[$i]{parent_xid} = $top;
		$summary->{nodes}[$i]{child_xid} = $child;
		$summary->{nodes}[$i]{retained_segments}
			= [map {sprintf('%04X', int($_ / $xids_per_segment))} ($top, $child)];
		save_segments($summary);
		push @parents, $parent;
	}
	my @retained = sort {$a <=> $b}
		map {map {hex($_)} @{$_->{retained_segments}}} @{$summary->{nodes}};
	for my $i (0 .. 3)
	{
		my $row = $summary->{nodes}[$i];
		$row->{before_checkpoint} = subtrans_segments($nodes[$i]);
		$row->{old_segments} = [grep {hex($_) < $retained[0]} @{$row->{before_checkpoint}}];
		save_segments($summary);
		require_true(@{$row->{old_segments}} > 0,
			"node$i has an old physical SUBTRANS segment to truncate");
	}
	for my $i (0 .. 3)
	{
		sql($nodes[$i], 'CHECKPOINT');
		my $row = $summary->{nodes}[$i];
		$row->{after_checkpoint} = subtrans_segments($nodes[$i]);
		save_segments($summary);
		my %after = map {$_ => 1} @{$row->{after_checkpoint}};
		for my $old (@{$row->{old_segments}})
		{
			require_true(!$after{$old}, "node$i removed old SUBTRANS segment $old");
		}
		for my $live (@{$row->{retained_segments}})
		{
			require_true($after{$live}, "node$i retained active parent/child segment $live");
		}
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
	# The shutdown checkpoint consumes any old queued SYNC after deletion.
	$cluster->stop_cluster;
	clean_subtrans_logs(\@nodes);
	$cluster->restart_cluster;
	for my $node (@nodes)
	{
		require_equal(sql($node, q{SELECT string_agg(id||':'||v, ',' ORDER BY id) FROM subtrans_retained}),
			'0:1,1:1,2:1,3:1', 'same DATA preserves committed parentage');
	}
	$cluster->stop_cluster;
	clean_subtrans_logs(\@nodes);
});
