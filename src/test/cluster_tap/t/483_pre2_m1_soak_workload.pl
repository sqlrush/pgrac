#!/usr/bin/env perl
#-------------------------------------------------------------------------
# 483_pre2_m1_soak_workload.pl
#    Native one-hour workload/ledger and original four-member normal stop.
#    Full P07 additionally needs generation and four-resource evidence.
#
# Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
# Portions Copyright (c) 2026, pgrac contributors
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
use IPC::Run qw(run timeout);
use Time::HiRes qw(clock_gettime CLOCK_MONOTONIC);
use Digest::SHA;
use File::Copy qw(copy);

my $clients = $ENV{PGRAC_PRE2_SOAK_CLIENTS} // '';
die 'use the native m1-soak-workload controller (16/48/64)' unless $clients =~ /^(16|48|64)$/;
my $bindir = $ENV{PGRAC_PRE2_BINDIR} // die 'native binary directory missing';
my $python = $ENV{PYTHON} // 'python3';
my $helper = "$FindBin::RealBin/../pre2";
my $evidence = ($ENV{TESTLOGDIR} // die 'native evidence directory missing') . "/soak-$clients";
mkdir $evidence, 0700 or die $!;
my ($cluster, @nodes);
my $stop_attempted = 0;

sub save_text
{
	my ($name, $text) = @_;
	open my $file, '>', "$evidence/$name" or die $!;
	print {$file} $text;
	close $file or die $!;
}

sub save_json
{
	my ($name, $value) = @_;
	save_text($name, JSON::PP->new->canonical->pretty->encode($value));
}

sub descriptor
{
	my ($name) = @_;
	my $path = "$evidence/$name";
	die 'capture is not a regular file' unless -f $path && !-l $path;
	open my $file, '<', $path or die $!;
	binmode $file;
	my $size = -s $file;
	my $sha = Digest::SHA->new(256)->addfile($file)->hexdigest;
	close $file or die $!;
	return {path => $name, size_bytes => $size, sha256 => $sha};
}

sub row_cuts
{
	my ($when, $sql) = @_;
	my @files;
	for my $node (0 .. 3)
	{
		my $name = "$when-node$node.txt";
		save_text($name, $nodes[$node]->safe_psql('postgres', $sql, timeout => 60) . "\n");
		push @files, descriptor($name);
	}
	return \@files;
}

my $execution = eval {
	my ($text, $err) = ('', '');
	run([$python, "$helper/m1_soak_workload.py"], '>', \$text, '2>', \$err, timeout(10))
	  or die "workload planner failed: $err";
	my $plan = decode_json($text);
	my ($case) = grep { $_->{clients} == $clients } @{$plan->{cases}};
	die 'fixed workload profile missing' unless defined $case;
	save_json('plan.json', $plan);
	$cluster = PostgreSQL::Test::ClusterPRE2->new_cluster("m1_soak_$clients",
		nodes => 4, blackbox => 1, required_operations => [], extra_conf => $plan->{extra_conf});
	$cluster->start_cluster;
	@nodes = $cluster->nodes;
	is(scalar(@nodes), 4, 'original PRE2 formation admits four normal members') or die 'formation differs';
	save_json('formation.json', [map { $cluster->observe('open_observation', {node => $_}) } 0 .. 3]);
	$nodes[0]->safe_psql('postgres', $plan->{setup_sql}, timeout => 120);
	my $before = row_cuts('before', $plan->{row_cut_sql});
	copy("$helper/m1_soak.sql", "$evidence/workload.sql") or die $!;
	my @command = ("$bindir/pgbench", @{$case->{pgbench_arguments}},
		'-f', "$evidence/workload.sql", "--log-prefix=$evidence/pgbench_log",
		'-h', $nodes[0]->host, '-p', $nodes[0]->port, 'postgres');
	save_json('command.json', \@command);
	my $start = clock_gettime(CLOCK_MONOTONIC);
	my $ran = eval {
		run(\@command, '>', "$evidence/summary.txt", '2>', "$evidence/stderr.txt", timeout(3700));
		1;
	};
	my $status = $?;
	my $error = $@;
	my $end = clock_gettime(CLOCK_MONOTONIC);
	save_json('invocation.json', {start_monotonic_seconds => $start,
		end_monotonic_seconds => $end, elapsed_seconds => $end - $start,
		wait_status => $status, completed => $ran ? JSON::PP::true : JSON::PP::false,
		error => $error});
	die "pgbench did not complete normally: $error/status=$status" unless $ran && $status == 0;
	cmp_ok($end - $start, '>=', 3600, 'workload actually runs for the full hour') or die 'short workload';
	my $after = row_cuts('after', $plan->{row_cut_sql});
	opendir my $dir, $evidence or die $!;
	my @logs = sort grep { /^pgbench_log\./ } readdir $dir;
	closedir $dir or die $!;
	die 'expected original two-thread pgbench log pair' unless @logs == 2
	  && $logs[0] =~ /^pgbench_log\.[1-9][0-9]*$/ && $logs[1] eq "$logs[0].1";
	my $manifest = {version => 2, clients => 0 + $clients, duration_seconds => 3600,
		pgbench_rc => 0, workload => descriptor('workload.sql'),
		invocation => descriptor('invocation.json'),
		summary => descriptor('summary.txt'), stderr => descriptor('stderr.txt'),
		logs => [map { descriptor($_) } @logs], before => $before, after => $after};
	save_json('manifest.json', $manifest);
	my $checked = run([$python, "$helper/m1_soak_ledger.py", "$evidence/manifest.json"],
		'>', "$evidence/ledger.json", '2>', "$evidence/ledger-stderr.txt", timeout(300));
	die 'independent raw commit ledger rejected the native workload' unless $checked;
	my $ledger = decode_json(slurp_file("$evidence/ledger.json"));
	die 'ledger scope differs' unless $ledger->{workload_ledger_passed}
	  && $ledger->{elapsed_duration_passed}
	  && $ledger->{p07} eq 'NOT_READY' && $ledger->{stage_acceptance} eq 'NOT_READY';
	pass('all four row cuts agree with the independent per-client raw commit ledger');
	1;
};
my $first_error = $execution ? '' : $@;
my $cleanup_error = '';
my $normal_stop = 0;
if (defined($cluster) && $cluster->{running} && !$stop_attempted)
{
	$stop_attempted = 1;
	eval {
		my $receipt = $cluster->stop_cluster;
		save_json('normal-stop.json', $receipt);
		die 'full original normal-stop proof missing' unless $cluster->{normal_stop_proven};
		$normal_stop = 1;
		pass('all four native members complete the original normal-stop proof');
		1;
	} or $cleanup_error .= $@;
}
my $member_logs_passed = 0;
my $member_logs_error = '';
if ($normal_stop)
{
	eval {
		my @logs = map { $_->logfile } @nodes;
		die 'four closed native member logs missing' unless @logs == 4;
		my $checked = run([$python, "$helper/m1_soak_member_logs.py", @logs],
			'>', "$evidence/member-logs.json", '2>', "$evidence/member-logs-stderr.txt", timeout(60));
		my $result = decode_json(slurp_file("$evidence/member-logs.json"));
		die 'closed native member error check rejected the logs; see member-logs.json'
		  unless $checked && $result->{member_logs_clean}
		  && $result->{p07} eq 'NOT_READY' && $result->{stage_acceptance} eq 'NOT_READY';
		$member_logs_passed = 1;
		pass('four closed native member logs contain no ERROR, FATAL or PANIC headers');
		1;
	} or $member_logs_error = $@;
}
else
{
	$member_logs_error = 'original normal stop did not close all four member logs';
}
save_json('execution.json', {workload_and_stop_passed =>
	($execution && $normal_stop && $cleanup_error eq '' && $member_logs_passed) ? JSON::PP::true : JSON::PP::false,
	error => $first_error, cleanup_error => $cleanup_error,
	member_logs_passed => $member_logs_passed ? JSON::PP::true : JSON::PP::false,
	member_logs_error => $member_logs_error,
	p07 => 'NOT_READY', stage_acceptance => 'NOT_READY',
	missing_gates => ['real recycle generation progress', 'complete four-resource census']});
if (!$execution || !$normal_stop || $cleanup_error ne '' || !$member_logs_passed)
{
	fail('native workload execution and original cleanup complete');
	diag($first_error . $cleanup_error . $member_logs_error);
}
for my $node (@nodes)
{
	unlike(slurp_file($node->logfile), qr/PANIC|checksum verification failed|invalid page in block/,
		'closed native member has no corruption or PANIC');
}
done_testing();
