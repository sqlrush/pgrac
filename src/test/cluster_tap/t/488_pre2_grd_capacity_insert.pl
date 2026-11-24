#!/usr/bin/env perl
# Ordinary small-buffer INSERT through the original four-member lifecycle.
# No state injection, forced reclaim, rollover UPDATE or canonical sampling.
# Author: SqlRush <sqlrush@gmail.com>
# Copyright (c) 2026, pgrac contributors
use strict;
use warnings;
use FindBin;
use lib "$FindBin::RealBin/../../perl";
use PostgreSQL::Test::ClusterPRE2;
use PostgreSQL::Test::Utils qw(slurp_file);
use Test::More;
use JSON::PP;
use IPC::Run qw(run timeout);

my $probe = $ENV{PGRAC_PRE2_PCM_STATS_PROBE} // die 'use the p145-grd-capacity native controller';
die 'native PCM statistics module missing' unless $probe =~ m{^/} && -f $probe && !-l $probe;
my $evidence = $ENV{TESTLOGDIR} // die 'native evidence directory missing';
my ($cluster, @nodes);
my ($stop_attempted, $query_number) = (0, 0);

sub save_json
{
	my ($name, $value) = @_;
	open my $file, '>', "$evidence/pcm-capacity-$name.json" or die $!;
	print {$file} JSON::PP->new->canonical->pretty->encode($value);
	close $file or die $!;
}

sub sql
{
	my ($index, $statement, $limit) = @_;
	my ($out, $err) = ('', '');
	my $rc = eval { $nodes[$index]->psql('postgres', $statement,
		timeout => $limit // 30, on_error_stop => 1, stdout => \$out, stderr => \$err) };
	my $error = $@;
	save_json('sql-' . ++$query_number, {node => $index, sql => $statement,
		rc => $rc, stdout => $out, stderr => $err, error => $error});
	die $error if $error;
	die "node$index native SQL failed: $err" unless defined($rc) && $rc == 0;
	return $out;
}

sub counter_cut
{
	my ($name) = @_;
	my @cut;
	for my $index (0 .. 3)
	{
		# No cluster_dump_state: its undo observation has an ensure side effect.
		push @cut, decode_json(sql($index, q{SELECT json_build_object(
 'node',current_setting('cluster.node_id')::int,
 'postmaster_start',pg_postmaster_start_time()::text,
 'stats',g_p145_pcm_stats()::json)}));
		save_json($name, \@cut);
	}
	return \@cut;
}

sub open_cut
{
	my ($name) = @_;
	my $cut = [map { $cluster->observe('open_observation', {node => $_}) } 0 .. 3];
	save_json($name, $cut);
	my @identity;
	for my $index (0 .. 3)
	{
		my $row = $cut->[$index];
		die 'NOT_REACHED: original OPEN identity incomplete' unless
		  ($row->{phase} // '') eq 'OPEN' && ($row->{system_identifier} // '') =~ /^[1-9][0-9]*$/
		  && ($row->{root_digest} // '') =~ /^[0-9a-f]{64}$/ && ($row->{generation} // 0) > 0
		  && join(',', @{$row->{members} // []}) eq '0,1,2,3'
		  && defined($row->{writer}{node}) && $row->{writer}{node} == $index
		  && ($row->{writer}{boot} // '') ne '';
		for my $field (qw(thread incarnation wal_generation))
		{
			die 'NOT_REACHED: original writer identity incomplete' unless
			  ($row->{writer}{$field} // '') =~ /^[1-9][0-9]*$/;
		}
		push @identity, { (map { $_ => $row->{$_} } qw(phase system_identifier root_digest generation members)),
			writer => {map { $_ => $row->{writer}{$_} } qw(node boot thread incarnation wal_generation)} };
	}
	return \@identity;
}

my $execution = eval {
	$cluster = PostgreSQL::Test::ClusterPRE2->new_cluster('p145_grd_capacity',
		nodes => 4, blackbox => 1, required_operations => [], extra_conf => [
			'shared_buffers=16MB', 'autovacuum=off', 'log_error_verbosity=verbose']);
	@nodes = $cluster->nodes;
	$cluster->start_cluster;
	is(scalar(@nodes), 4, 'original PRE2 formation admits four normal members') or die 'formation differs';
	my $original_open = open_cut('formation');
	for my $index (0 .. 3)
	{
		is(sql($index, q{SELECT pg_size_bytes(current_setting('shared_buffers'))::text
 || '|' || current_setting('block_size')}), '16777216|8192',
			"node$index keeps the original 16MB / 2048-buffer configuration") or die 'buffer sizing changed';
	}
	(my $quoted_probe = $probe) =~ s/'/''/g;
	sql(0, "CREATE FUNCTION g_p145_pcm_stats() RETURNS text AS '$quoted_probe',"
		. " 'test_pgrac_pcm_capacity_stats' LANGUAGE C STRICT VOLATILE PARALLEL UNSAFE");
	sql(0, q{CREATE TABLE m2_roll(id int,v int,pad text);
 ALTER TABLE m2_roll ALTER pad SET STORAGE PLAIN;});
	my $before = counter_cut('before');
	for my $row (@$before)
	{
		die 'NOT_REACHED: original GRD capacity is not 2048' unless $row->{stats}{capacity} == 2048;
	}
	# Preserve the failing INSERT verbatim, and let the original SQL timer apply.
	# The first SELECT binds the actual producer, not the observing connection.
	my $insert = sql(0, q{\set VERBOSITY verbose
 SET application_name='g_p145_capacity_insert';
 SET statement_timeout=90000;
 SELECT json_build_object('pid',pid,'backend_start',backend_start)::text
 FROM pg_stat_activity WHERE pid=pg_backend_pid();
 INSERT INTO m2_roll SELECT i,0,repeat(md5(i::text),215) FROM generate_series(1,10000)i;
 \echo P145_INSERT_SQLSTATE :SQLSTATE
}, 180);
	my @lines = split /\n/, $insert;
	die 'producer identity or SQLSTATE missing' unless @lines == 2;
	my $producer = decode_json($lines[0]);
	save_json('producer', $producer);
	die 'native producer identity invalid' unless ref($producer) eq 'HASH'
	  && ($producer->{pid} // '') =~ /^[1-9][0-9]*$/ && ($producer->{backend_start} // '') ne '';
	is($lines[1], 'P145_INSERT_SQLSTATE 00000', 'ordinary INSERT finishes without a transaction error')
	  or die 'INSERT did not complete normally';
	my $after = counter_cut('after');
	my $input = encode_json({before => $before, after => $after});
	my ($out, $err) = ('', '');
	run([$ENV{PYTHON} // 'python3', "$FindBin::RealBin/../pre2/pcm_capacity_evidence.py"],
		'<', \$input, '>', \$out, '2>', \$err, timeout(10)) or die "capacity judge failed: $err";
	my $result = decode_json($out);
	save_json('counter-result', $result);
	ok($result->{passed}, 'full 2048 capacity and original wait/reclaim counter increments witnessed')
	  or die 'NOT_REACHED: native capacity evidence incomplete';
	is_deeply(open_cut('after-open'), $original_open,
		'all original OPEN roots and writer identities remain current across the INSERT') or die 'formation identity changed';
	cmp_ok(sql(0, q{SELECT pg_relation_size('m2_roll')}), '>', 2048 * 8192,
		'original heap exceeds the buffer and GRD capacity') or die 'heap did not span the original capacity';
	for my $index (0 .. 3)
	{
		is(sql($index, q{SELECT count(*)::text || '|' || count(DISTINCT id)::text
 || '|' || min(id)::text || '|' || max(id)::text
 || '|' || count(*) FILTER (WHERE v IS DISTINCT FROM 0)::text
 || '|' || count(*) FILTER (WHERE pad IS DISTINCT FROM repeat(md5(id::text),215))::text
 FROM m2_roll}, 90), '10000|10000|1|10000|0|0',
			"node$index reads every original id/value/pad after capacity recovery") or die 'row content differs';
	}
	$stop_attempted = 1;
	save_json('normal-stop', $cluster->stop_cluster);
	pass('original four-member normal stop proves the original writer closure');
	1;
};
my $first_error = $execution ? '' : $@;
my $cleanup_error = '';
if (defined($cluster) && $cluster->{running} && !$stop_attempted)
{
	$stop_attempted = 1;
	eval { save_json('error-stop', $cluster->stop_cluster); 1 } or $cleanup_error = $@;
}
save_json('execution', {first_error => $first_error, cleanup_error => $cleanup_error});
my $logs_clean = 1;
for my $node (@nodes)
{
	if (!-f $node->logfile)
	{
		fail('member log missing');
		$logs_clean = 0;
		next;
	}
	$logs_clean = 0 unless unlike(slurp_file($node->logfile),
		qr/\b(?:ERROR|FATAL|PANIC):/, 'member log has no ERROR/FATAL/PANIC');
}
save_json('result', {passed => $execution && !$cleanup_error && $logs_clean ? JSON::PP::true : JSON::PP::false,
	first_error => $first_error, cleanup_error => $cleanup_error});
BAIL_OUT("native capacity INSERT failed: $first_error $cleanup_error")
  unless $execution && !$cleanup_error && $logs_clean;
done_testing();
