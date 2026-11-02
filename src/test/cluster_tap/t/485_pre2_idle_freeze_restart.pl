#!/usr/bin/env perl
#-------------------------------------------------------------------------
# 485_pre2_idle_freeze_restart.pl
#    Observe actual tuple freezing and preservation across clean restart.
#    This subcheck does not certify the full F/H/catalog cutoff contract.
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
use Math::BigInt;
use Time::HiRes qw(clock_gettime CLOCK_MONOTONIC sleep);

my ($cluster, @nodes);
my $evidence = $ENV{TESTLOGDIR} // die 'native evidence directory missing';
my $stop_attempted = 0;
my $normal_stops = 0;
my %original;
my %writer_xmins;
my $expected_rows = join("\n", map { $_ . ':' . ($_ * 10) } 0 .. 15);
my $rows_sql = 'SELECT id::text || chr(58) || v::text FROM g_m1_idle_freeze ORDER BY id';
my $catalog_sql = q{SELECT json_build_object('oid', oid, 'relfrozenxid', relfrozenxid::text,
    'relminmxid', relminmxid::text) FROM pg_class WHERE oid='g_m1_idle_freeze'::regclass};
my $pages_sql = q{
WITH blocks AS MATERIALIZED (
 SELECT block, get_raw_page('g_m1_idle_freeze', block) AS image
 FROM generate_series(0, pg_relation_size('g_m1_idle_freeze') /
                         current_setting('block_size')::bigint - 1) AS b(block))
SELECT coalesce(json_agg(json_build_object('block', block,
 'image_hex', encode(image, 'hex'), 'items', (
 SELECT json_agg(json_build_object('lp', h.lp, 'lp_flags', h.lp_flags,
   'xmin', h.t_xmin::text, 'xmax', h.t_xmax::text, 'ctid', h.t_ctid::text,
   'infomask', h.t_infomask, 'infomask2', h.t_infomask2,
   'combined_flags', f.combined_flags, 'data_hex', encode(h.t_data, 'hex')) ORDER BY h.lp)
 FROM heap_page_items(image) AS h
 LEFT JOIN LATERAL heap_tuple_infomask_flags(h.t_infomask, h.t_infomask2) AS f ON true))
 ORDER BY block), '[]'::json) FROM blocks};

sub save_json
{
	my ($name, $value) = @_;
	open my $file, '>', "$evidence/idle-freeze-$name.json" or die $!;
	print {$file} JSON::PP->new->canonical->pretty->encode($value);
	close $file or die $!;
}

sub read_rows
{
	my ($phase) = @_;
	my @cuts;
	for my $i (0 .. 3)
	{
		my $rows = $nodes[$i]->safe_psql('postgres', $rows_sql, timeout => 30);
		push @cuts, {node => $i, rows => $rows};
		save_json("$phase-rows", \@cuts);
		is($rows, $expected_rows, "$phase: node$i preserves all committed literal rows")
		  or die 'native row ledger mismatch';
	}
}

sub page_cut
{
	my ($node, $phase, $frozen) = @_;
	my $pages = decode_json($nodes[$node]->safe_psql('postgres', $pages_sql, timeout => 30));
	save_json("$phase-node$node-pages", $pages);
	die 'native page array missing' unless ref($pages) eq 'ARRAY' && @$pages;
	my (%seen, %xmin_counts);
	for my $block (0 .. $#$pages)
	{
		my $page = $pages->[$block];
		die 'native page sequence differs' unless $page->{block} == $block;
		die 'native page image missing' unless ($page->{image_hex} // '') =~ /\A[0-9a-f]+\z/;
		die 'native page items missing' unless ref($page->{items}) eq 'ARRAY';
		for my $item (@{$page->{items}})
		{
			my $key = '(' . $block . ',' . $item->{lp} . ')';
			die 'unexpected line pointer shape' unless $item->{lp_flags} == 1 && !$seen{$key}++;
			die 'native tuple fields missing' unless defined($item->{xmin})
			  && defined($item->{xmax}) && defined($item->{data_hex})
			  && ref($item->{combined_flags}) eq 'ARRAY';
			my $is_frozen = grep { $_ eq 'HEAP_XMIN_FROZEN' } @{$item->{combined_flags}};
			is($is_frozen ? 1 : 0, $frozen, "$phase: node$node $key actual xmin freeze flag")
			  or die 'target tuple freeze path not proved';
			if (!$frozen)
			{
				die 'tuple xmin lacks original native writer' unless exists $writer_xmins{$item->{xmin}};
				die 'original INSERT tuple ctid differs' unless $item->{ctid} eq $key;
				$xmin_counts{$item->{xmin}}++;
				$original{$key} = {map { $_ => $item->{$_} } qw(xmin xmax ctid data_hex)};
			}
			else
			{
				die 'frozen tuple location differs' unless exists $original{$key};
				is_deeply({map { $_ => $item->{$_} } qw(xmin xmax ctid data_hex)}, $original{$key},
					"$phase: node$node $key retains original tuple identity and payload")
				  or die 'freeze/restart replaced target tuple';
			}
		}
	}
	is(scalar(keys %seen), 16, "$phase: node$node inspected every target tuple")
	  or die 'target tuple count differs';
	if (!$frozen)
	{
		is_deeply(\%xmin_counts, {map { $_ => 4 } keys %writer_xmins},
			'all four origins supplied their four original unfrozen tuples')
		  or die 'origin tuple ledger differs';
	}
}

sub normal_stop
{
	my ($phase) = @_;
	$stop_attempted = 1;
	my $receipt = $cluster->stop_cluster;
	save_json("$phase-normal-stop", $receipt);
	die 'full original normal-stop proof missing' unless $cluster->{normal_stop_proven};
	$normal_stops++;
	pass("$phase: four members complete the original normal stop");
}

my $execution = eval {
	$cluster = PostgreSQL::Test::ClusterPRE2->new_cluster('m1_idle_freeze', nodes => 4,
		blackbox => 1, required_operations => [], extra_conf => ['autovacuum=off',
			'cluster.undo_cleaner_interval_ms=1000', 'log_error_verbosity=verbose',
			q{log_line_prefix='%m [%p] '}]);
	$cluster->start_cluster;
	@nodes = $cluster->nodes;
	is(scalar(@nodes), 4, 'original PRE2 formation admits four normal members') or die 'formation differs';
	save_json('formation', [map { $cluster->observe('open_observation', {node => $_}) } 0 .. 3]);
	is($nodes[0]->safe_psql('postgres',
		q{SELECT count(*) FROM pg_available_extensions WHERE name='pageinspect'}, timeout => 30),
		'1', 'original installed pageinspect is required') or die 'pageinspect unavailable; no skip';
	$nodes[0]->safe_psql('postgres', 'CREATE EXTENSION pageinspect; '
		. 'CREATE TABLE g_m1_idle_freeze (id int PRIMARY KEY, v int NOT NULL)', timeout => 60);
	my @writers;
	for my $i (0 .. 3)
	{
		my $first = $i * 4;
		my $last = $first + 3;
		my $full = $nodes[$i]->safe_psql('postgres', "BEGIN; INSERT INTO g_m1_idle_freeze "
			. "SELECT id, id * 10 FROM generate_series($first,$last) AS id; "
			. 'SELECT pg_current_xact_id()::text; COMMIT', timeout => 60);
		die 'native writer FullXid missing' unless $full =~ /\A[1-9][0-9]*\z/;
		my $number = Math::BigInt->new($full);
		die 'native writer belongs to another origin' unless $number->copy->bmod(16)->numify == $i;
		my $raw = $number->copy->bmod('4294967296')->bstr;
		die 'native writer raw xmin duplicated' if exists $writer_xmins{$raw};
		$writer_xmins{$raw} = $i;
		push @writers, {node => $i, full_xid => $full, raw_xmin => $raw, first => $first, last => $last};
		save_json('writers', \@writers);
	}
	read_rows('before');
	page_cut(0, 'before', 0);
	my $catalog_before = decode_json($nodes[0]->safe_psql('postgres', $catalog_sql, timeout => 30));
	save_json('catalog-before', $catalog_before);
	# No SQL or business writer during this observation interval. Time is not
	# freeze authority: the later actual tuple checks must independently pass.
	my $start = clock_gettime(CLOCK_MONOTONIC);
	sleep(10);
	my $end = clock_gettime(CLOCK_MONOTONIC);
	save_json('business-idle', {start_monotonic => $start, end_monotonic => $end,
		elapsed_seconds => $end - $start, requested_seconds => 10});
	cmp_ok($end - $start, '>=', 10, 'original background owners had a business-idle interval')
	  or die 'business-idle interval interrupted';
	my ($rc, $stdout, $stderr) = $nodes[0]->psql('postgres',
		'VACUUM (FREEZE, VERBOSE) g_m1_idle_freeze', timeout => 60, on_error_stop => 1);
	save_json('vacuum', {rc => $rc, stdout => $stdout, stderr => $stderr});
	is($rc, 0, 'original VACUUM FREEZE returns successfully') or die 'native VACUUM failed';
	my @catalog_after;
	for my $i (0 .. 3)
	{
		page_cut($i, 'after-freeze', 1);
		push @catalog_after, decode_json($nodes[$i]->safe_psql('postgres', $catalog_sql, timeout => 30));
		save_json('catalog-after-freeze', \@catalog_after);
	}
	read_rows('after-freeze');
	normal_stop('first');
	$stop_attempted = 0;
	$cluster->restart_cluster;
	@nodes = $cluster->nodes;
	save_json('restart-formation', [map { $cluster->observe('open_observation', {node => $_}) } 0 .. 3]);
	for my $i (0 .. 3)
	{
		page_cut($i, 'after-restart', 1);
		my $catalog = decode_json($nodes[$i]->safe_psql('postgres', $catalog_sql, timeout => 30));
		save_json("after-restart-node$i-catalog", $catalog);
		is_deeply($catalog, $catalog_after[$i], "node$i preserves observed target catalog cutoffs")
		  or die 'target catalog changed across clean restart';
	}
	read_rows('after-restart');
	normal_stop('final');
	1;
};
my $first_error = $execution ? '' : $@;
my $cleanup_error = '';
if (defined($cluster) && $cluster->{running} && !$stop_attempted)
{
	eval { normal_stop('error-cleanup'); 1 } or $cleanup_error .= $@;
}
my $logs_clean = 1;
for my $node (@nodes)
{
	$logs_clean = 0 unless unlike(slurp_file($node->logfile),
		qr/(?:ERROR|FATAL|PANIC):|checksum verification failed|invalid page in block/,
		'closed native member has no error, refusal or corruption');
}
save_json('execution', {tuple_freeze_and_clean_restart_passed =>
	($execution && $normal_stops == 2 && $logs_clean && $cleanup_error eq '')
	? JSON::PP::true : JSON::PP::false,
	error => $first_error, cleanup_error => $cleanup_error, normal_stops => $normal_stops,
	p01 => 'NOT_READY', stage_acceptance => 'NOT_READY',
	missing_gates => ['qualified full-width F/H and catalog cutoff proof', 'complete M1 acceptance']});
if (!$execution || $normal_stops != 2 || !$logs_clean || $cleanup_error ne '')
{
	fail('native idle-freeze execution and cleanup complete');
	diag($first_error . $cleanup_error);
}
done_testing();
