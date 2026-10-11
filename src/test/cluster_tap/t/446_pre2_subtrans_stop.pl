#!/usr/bin/env perl
# Copyright (c) 2026, pgrac contributors
# Author: SqlRush <sqlrush@gmail.com>
# PGRAC: native temporary child parentage and shared top-level commit durability.
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
	my ($nodes, $summary, $phase) = @_;
	my @checks;
	for my $i (0 .. $#$nodes)
	{
		my $node = $nodes->[$i];
		my $log = slurp_file($node->logfile);
		my $fsync = () = $log =~ /could not fsync file "pg_subtrans\//g;
		my $errors = () = $log =~ /ERROR:/g;
		my $fatals = () = $log =~ /FATAL:/g;
		my $panics = () = $log =~ /PANIC:/g;
		push @checks, {node => $i, logfile => $node->logfile,
			fsync_errors => $fsync, errors => $errors, fatals => $fatals,
			panics => $panics, clean => ($fsync + $errors + $fatals + $panics == 0
				? JSON::PP::true : JSON::PP::false)};
	}
	# Persist every node before any failed assertion ends the scenario.
	$summary->{sync_checks}{$phase} = \@checks;
	save_segments($summary);
	for my $check (@checks)
	{
		require_true($check->{clean},
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
	my $summary = {xids_per_segment => $xids_per_segment,
		child_source => 'native temporary row xmin', nodes => []};
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
		# Create the temporary relation outside the retained transaction. Its
		# implicit child writes only local buffers, while the shared row is
		# updated by the parent before entering the exception block.
		$parent->query_safe('CREATE TEMP TABLE subtrans_local (v int) ON COMMIT PRESERVE ROWS');
		$parent->query_safe("BEGIN; UPDATE subtrans_retained SET v=1 WHERE id=$i");
		$parent->query_safe(q{DO $$ BEGIN
            INSERT INTO subtrans_local VALUES (1);
        EXCEPTION WHEN division_by_zero THEN
            RAISE;
        END $$});
		my $identity = $parent->query_safe("SELECT pg_current_xact_id_if_assigned()::text,
            xmin::text FROM subtrans_local WHERE v=1");
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
			'0:1,1:1,2:1,3:1', 'top-level shared writes commit with native children');
	}
	# The shutdown checkpoint consumes any old queued SYNC after deletion.
	$cluster->stop_cluster;
	clean_subtrans_logs(\@nodes, $summary, 'first_stop');
	$cluster->restart_cluster;
	for my $node (@nodes)
	{
		require_equal(sql($node, q{SELECT string_agg(id||':'||v, ',' ORDER BY id) FROM subtrans_retained}),
			'0:1,1:1,2:1,3:1', 'same DATA preserves top-level shared writes');
	}
	$cluster->stop_cluster;
	clean_subtrans_logs(\@nodes, $summary, 'second_stop');
});
