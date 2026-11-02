#!/usr/bin/env perl
#-------------------------------------------------------------------------
# 486_pre2_multixact_continuation.pl
#    Allocate local MultiXacts after restart and retain live members at VACUUM.
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

my ($cluster, @nodes, @holders);
my $evidence = $ENV{TESTLOGDIR} // die 'native evidence directory missing';
my ($stop_attempted, $normal_stops, $query_number) = (0, 0, 0);
my %issued;
my $expected_rows = join("\n", map { $_ . ':' . ($_ * 10) } 0 .. 7);
my $identity_sql = q{SELECT pg_backend_pid()::text || '|' || backend_start::text
 FROM pg_stat_activity WHERE pid=pg_backend_pid()};

sub save_json
{
	my ($name, $value) = @_;
	open my $file, '>', "$evidence/mx-continuation-$name.json" or die $!;
	print {$file} JSON::PP->new->canonical->pretty->encode($value);
	close $file or die $!;
}

sub sql
{
	my ($node, $statement, $timeout) = @_;
	my ($stdout, $stderr) = ('', '');
	my $rc = eval { $nodes[$node]->psql('postgres', $statement,
		timeout => $timeout // 30, on_error_stop => 1,
		stdout => \$stdout, stderr => \$stderr) };
	my $error = $@;
	save_json('sql-' . ++$query_number, {node => $node, sql => $statement,
		rc => $rc, stdout => $stdout, stderr => $stderr, error => $error});
	die $error if $error;
	die "node$node native SQL failed: $stderr" unless $rc == 0;
	return $stdout;
}

sub query_holder
{
	my ($holder, $statement) = @_;
	my $value = eval { $holder->{client}->query_safe($statement) };
	my $error = $@;
	save_json('holder-query-' . ++$query_number, {name => $holder->{name},
		node => $holder->{node}, sql => $statement, stdout => $value,
		error => $error, stderr => $holder->{client}{stderr}});
	die $error if $error;
	return $value;
}

sub close_holder
{
	my ($holder, $command) = @_;
	return if $holder->{closed};
	query_holder($holder, $command) if $holder->{transaction};
	$holder->{transaction} = 0;
	die "native holder $holder->{name} did not exit cleanly" unless $holder->{client}->quit;
	$holder->{closed} = 1;
}

sub row_cut
{
	my ($phase) = @_;
	my @cuts;
	for my $node (0 .. 3)
	{
		my $rows = sql($node,
			'SELECT id::text || chr(58) || v::text FROM g_m1_mx_continuation ORDER BY id');
		push @cuts, {node => $node, rows => $rows};
		save_json("$phase-rows", \@cuts);
		is($rows, $expected_rows, "$phase: node$node preserves every literal row")
		  or die 'native row ledger differs';
	}
}

sub member_cut
{
	my ($phase, $multis) = @_;
	for my $entry (@$multis)
	{
		my ($node, $row, $mx) = @{$entry}{qw(node row multi)};
		is(sql($node, "SELECT xmax::text FROM g_m1_mx_continuation WHERE id=$row"),
			$mx, "$phase: node$node retains the original live MultiXact")
		  or die 'live MultiXact replaced';
		my $members = sql($node, "SELECT xid::text || ':' || mode "
			. "FROM pg_get_multixact_members('$mx') ORDER BY xid::text COLLATE \"C\", mode");
		save_json("$phase-node$node-members", {node => $node, row => $row,
			multi => $mx, members => $members, expected_members => $entry->{expected_members}});
		is($members, $entry->{expected_members},
			"$phase: node$node retains both exact live key-share members")
		  or die 'native MultiXact members differ';
	}
}

sub live_round
{
	my ($phase, $first_row) = @_;
	my (@multis, @current);
	for my $node (0 .. 3)
	{
		my $row = $first_row + $node;
		my @raw;
		for my $member (0 .. 1)
		{
			my $holder = {name => "$phase-node$node-member$member", node => $node,
				client => $nodes[$node]->background_psql('postgres', timeout => 30)};
			push @holders, $holder;
			push @current, $holder;
			$holder->{client}->set_query_timer_restart();
			$holder->{identity} = query_holder($holder, $identity_sql);
			die 'native holder PID/birth missing' unless $holder->{identity} =~ /\A[1-9][0-9]*\|.+\z/;
			$holder->{transaction} = 1;
			query_holder($holder, 'BEGIN');
			my $full = query_holder($holder, 'SELECT pg_current_xact_id()::text');
			die 'native holder FullXid missing' unless $full =~ /\A[1-9][0-9]*\z/;
			my $number = Math::BigInt->new($full);
			die 'native holder origin differs' unless $number->copy->bmod(16)->numify == $node;
			$holder->{full_xid} = $full;
			$holder->{raw_xid} = $number->copy->bmod('4294967296')->bstr;
			push @raw, $holder->{raw_xid};
			is(query_holder($holder,
				"SELECT v FROM g_m1_mx_continuation WHERE id=$row FOR KEY SHARE"),
				'' . ($row * 10), "$phase: node$node member$member locks its literal row")
			  or die 'native key-share lock failed';
			save_json("$holder->{name}-identity", {map { $_ => $holder->{$_} }
				qw(name node identity full_xid raw_xid)});
		}
		die 'native member XIDs duplicated' if $raw[0] eq $raw[1];
		my $mx = sql($node, "SELECT xmax::text FROM g_m1_mx_continuation WHERE id=$row");
		die 'native local MultiXact missing' unless $mx =~ /\A[1-9][0-9]*\z/
		  && Math::BigInt->new($mx)->bmod(16)->numify == $node;
		die 'MultiXact reissued in this bounded lifecycle' if $issued{$mx}++;
		push @multis, {node => $node, row => $row, multi => $mx,
			expected_members => join("\n", map { "$_:keysh" } sort @raw)};
		save_json("$phase-multis", \@multis);
	}
	member_cut("$phase-before-vacuum", \@multis);
	for my $node (0 .. 3)
	{
		sql($node, 'VACUUM (FREEZE, VERBOSE) g_m1_mx_continuation', 60);
		pass("$phase: original node$node VACUUM succeeds with all holders live");
		member_cut("$phase-after-vacuum$node", \@multis);
	}
	row_cut($phase);
	for my $holder (@current)
	{
		is(query_holder($holder, $identity_sql), $holder->{identity},
			"$holder->{name} retains its original backend identity") or die 'native holder changed';
		is(query_holder($holder, 'SELECT pg_current_xact_id_if_assigned()::text'),
			$holder->{full_xid}, "$holder->{name} remains in its original live transaction")
		  or die 'native holder transaction changed';
		close_holder($holder, 'COMMIT');
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
	$cluster = PostgreSQL::Test::ClusterPRE2->new_cluster('m1_mx_continuation', nodes => 4,
		blackbox => 1, required_operations => [], extra_conf => ['autovacuum=off',
			'cluster.undo_cleaner_interval_ms=1000', 'log_error_verbosity=verbose',
			q{log_line_prefix='%m [%p] '}]);
	$cluster->start_cluster;
	@nodes = $cluster->nodes;
	is(scalar(@nodes), 4, 'original PRE2 formation admits four normal members') or die 'formation differs';
	save_json('formation', [map { $cluster->observe('open_observation', {node => $_}) } 0 .. 3]);
	sql(0, 'CREATE TABLE g_m1_mx_continuation (id int PRIMARY KEY, v int NOT NULL); '
		. 'INSERT INTO g_m1_mx_continuation SELECT id, id * 10 FROM generate_series(0,7) AS id', 60);
	row_cut('initial');
	live_round('before-restart', 0);
	normal_stop('first');
	$stop_attempted = 0;
	$cluster->restart_cluster;
	@nodes = $cluster->nodes;
	save_json('restart-formation', [map { $cluster->observe('open_observation', {node => $_}) } 0 .. 3]);
	row_cut('after-restart');
	live_round('after-restart', 4);
	normal_stop('final');
	1;
};
my $first_error = $execution ? '' : $@;
my $cleanup_error = '';
for my $holder (@holders)
{
	next if $holder->{closed};
	eval { close_holder($holder, 'ROLLBACK'); 1 } or do {
		$cleanup_error .= $@;
		eval { $holder->{client}->quit; 1 } or $cleanup_error .= $@;
	};
}
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
save_json('execution', {continuation_and_live_members_passed =>
	($execution && $normal_stops == 2 && $logs_clean && $cleanup_error eq '')
	? JSON::PP::true : JSON::PP::false,
	error => $first_error, cleanup_error => $cleanup_error, normal_stops => $normal_stops,
	x04 => 'NOT_READY', stage_acceptance => 'NOT_READY',
	missing_gates => ['G>N and offset-page gap coverage', 'occupied offset-page preservation proof',
		'exact concurrent allocator/VACUUM overlap', 'complete M1 acceptance']});
if (!$execution || $normal_stops != 2 || !$logs_clean || $cleanup_error ne '')
{
	fail('native MultiXact continuation execution and cleanup complete');
	diag($first_error . $cleanup_error);
}
done_testing();
