#!/usr/bin/env perl
# Copyright (c) 2026, pgrac contributors
# Author: SqlRush <sqlrush@gmail.com>
# Terminate the exact reserved-but-unmaterialized extension owner, then stop.
use strict;
use warnings;
use FindBin;
use lib "$FindBin::RealBin/../../perl";
use PostgreSQL::Test::ClusterPRE2;
use PostgreSQL::Test::Utils qw(slurp_file);
use Test::More;
use Time::HiRes qw(clock_gettime CLOCK_MONOTONIC sleep);

sub wait_log
{
	my ($node, $pattern, $seconds) = @_;
	my $deadline = clock_gettime(CLOCK_MONOTONIC) + $seconds;
	while (clock_gettime(CLOCK_MONOTONIC) < $deadline)
	{
		my $log = slurp_file($node->logfile);
		return $log if $log =~ $pattern;
		sleep(0.02);
	}
	return '';
}

my $cluster = PostgreSQL::Test::ClusterPRE2->new_cluster('pre2_extension_stop',
	nodes => 4, blackbox => 1, required_operations => [],
	extra_conf => ['autovacuum = off', 'log_error_verbosity = verbose']);
for my $member ($cluster->nodes)
{
	$member->append_conf('postgresql.conf',
		"cluster.injection_points = 'cluster-ko-extension-recovery-start:sleep:2000000'\n");
}
$cluster->start_cluster;
my @nodes = $cluster->nodes;
my $node = $nodes[0];
$node->safe_psql('postgres', 'CREATE TABLE abandoned_extension (id integer, payload text)');
my $block_size = $node->safe_psql('postgres', 'SHOW block_size');
my $worker = $node->background_psql('postgres', timeout => 120);
my $pid = $worker->query_safe('SELECT pg_backend_pid()');
BAIL_OUT('invalid extension backend identity') unless $pid =~ /^[1-9][0-9]*$/;
$worker->query_safe(q{SET statement_timeout = 0;
  SELECT cluster_inject_fault('cluster-ko-extension-reserved', 'sleep', 60000000)});
$worker->query_until(qr/INSERT_SENT/, "\\echo INSERT_SENT\nINSERT INTO abandoned_extension VALUES (1, repeat('a', 1000));\n");
my $armed = wait_log($node,
	qr/KO extension reservation injection: pid=$pid locator=/, 30);
ok($armed ne '', 'original backend reached durable reservation before DATA materialization');
BAIL_OUT('extension injection was not reached') unless $armed ne '';
my ($spc, $db, $rel, $first, $count) = $armed =~
	/KO extension reservation injection: pid=$pid locator=(\d+)\/(\d+)\/(\d+) first=(\d+) count=(\d+)/;
BAIL_OUT('invalid original reservation identity') unless defined($count)
	&& $spc == 1663 && $count > 0 && $count <= 1024 && $block_size =~ /^[1-9][0-9]*$/;
is($node->safe_psql('postgres', "SELECT pg_terminate_backend($pid, 10000)"),
	't', 'terminate the armed extension backend');
# The connection was intentionally killed. Finish without sending another
# command into its closed stdin, retaining its original stderr below.
my $worker_rc = $worker->{run}->finish;
ok(!$worker_rc && $worker->{stderr} =~ /terminating connection due to administrator command/,
	'original worker reports the expected backend termination');
my $handoff = wait_log($node,
	qr/KO extension recovery handoff: serial=\d+ pid=$pid .*reserved=yes first=$first count=$count/, 30);
ok($handoff ne '', 'native ResourceOwner handed off the exact failed reservation');
BAIL_OUT('no exact failed-extension handoff') unless $handoff ne '';
my ($serial) = $handoff =~ /KO extension recovery handoff: serial=(\d+) pid=$pid /;
my $retired = wait_log($node,
	qr/KO extension recovery retired: serial=$serial pid=$pid .*first=$first count=$count/, 30);
ok($retired ne '', 'existing checkpointer recovered and retired that original work');
BAIL_OUT('failed reservation was not recovered') unless $retired ne '';
my $path = $cluster->shared_root . "/base/$db/$rel";
open my $file, '<', $path or die "$path: $!";
binmode($file);
defined(sysseek($file, $first * $block_size, 0)) or die "seek $path: $!";
my $bytes = '';
my $length = sysread($file, $bytes, $count * $block_size);
close $file or die "close $path: $!";
is($length, $count * $block_size, 'entire original reservation is physically materialized');
is($bytes, "\0" x ($count * $block_size), 'aborted extension publishes only new empty pages');
is($node->safe_psql('postgres', 'SELECT count(*) FROM abandoned_extension'),
	'0', 'aborted insert remains absent');
$node->safe_psql('postgres', "INSERT INTO abandoned_extension VALUES (2, 'after recovery')");
is($node->safe_psql('postgres', 'SELECT count(*)::text || \'|\' || sum(id)::text FROM abandoned_extension'),
	'1|2', 'ordinary insertion works after recovery without resurrecting aborted data');

# A second exact reservation stays failed/pending while normal stop begins.
$node->safe_psql('postgres', 'CREATE TABLE abandoned_during_stop (id integer)');
my $closing = $node->background_psql('postgres', timeout => 120);
my $closing_pid = $closing->query_safe('SELECT pg_backend_pid()');
BAIL_OUT('invalid closing backend identity') unless $closing_pid =~ /^[1-9][0-9]*$/;
$closing->query_safe(q{SELECT cluster_inject_fault('cluster-ko-extension-reserved', 'sleep', 60000000)});
$closing->query_until(qr/CLOSING_SENT/, "\\echo CLOSING_SENT\nINSERT INTO abandoned_during_stop VALUES (3);\n");
my $closing_armed = wait_log($node,
	qr/KO extension reservation injection: pid=$closing_pid locator=/, 30);
ok($closing_armed ne '', 'closing backend is paused after its own native reservation');
BAIL_OUT('closing extension injection was not reached') unless $closing_armed ne '';
my ($closing_spc, $closing_db, $closing_rel, $closing_first, $closing_count) = $closing_armed =~
	/KO extension reservation injection: pid=$closing_pid locator=(\d+)\/(\d+)\/(\d+) first=(\d+) count=(\d+)/;
BAIL_OUT('invalid closing reservation') unless defined($closing_count)
	&& $closing_spc == 1663 && $closing_count > 0 && $closing_count <= 1024;
is($node->safe_psql('postgres', "SELECT pg_terminate_backend($closing_pid, 10000)"),
	't', 'terminate the backend whose recovery will overlap normal stop');
my $closing_rc = $closing->{run}->finish;
ok(!$closing_rc && $closing->{stderr} =~ /terminating connection due to administrator command/,
	'closing backend reports its expected termination');
my $closing_handoff = wait_log($node,
	qr/KO extension recovery handoff: serial=\d+ pid=$closing_pid .*reserved=yes/, 30);
ok($closing_handoff ne '', 'closing reservation is handed off before stop');
BAIL_OUT('closing reservation was not handed off') unless $closing_handoff ne '';
my ($closing_serial) = $closing_handoff =~ /KO extension recovery handoff: serial=(\d+) pid=$closing_pid /;
my $pending = wait_log($node,
	qr/KO extension recovery injection: serial=$closing_serial pid=$closing_pid /, 30);
ok($pending ne '', 'checkpointer owns the still-pending recovery before normal stop');
BAIL_OUT('recovery pause was not reached') unless $pending ne '';
my $stop_offset = -s $node->logfile;
my $stopped = eval { $cluster->stop_cluster; 1 };
ok($stopped, 'original four-node normal stop and writer CLOSED evidence pass');
diag($@) unless $stopped;
my $stop_log = substr(slurp_file($node->logfile), $stop_offset);
like($stop_log, qr/received fast shutdown request.*?KO extension recovery retired: serial=$closing_serial pid=$closing_pid /s,
	'normal stop began before that same failed responsibility retired');
my $closing_path = $cluster->shared_root . "/base/$closing_db/$closing_rel";
open my $closing_file, '<', $closing_path or die "$closing_path: $!";
binmode($closing_file);
defined(sysseek($closing_file, $closing_first * $block_size, 0)) or die "seek $closing_path: $!";
my $closing_bytes = '';
my $closing_length = sysread($closing_file, $closing_bytes, $closing_count * $block_size);
close $closing_file or die "close $closing_path: $!";
is($closing_length, $closing_count * $block_size, 'stop completed the entire pending physical range');
is($closing_bytes, "\0" x ($closing_count * $block_size), 'pending stop recovery also leaves new empty pages');
for my $member (@nodes)
{
	unlike(slurp_file($member->logfile), qr/PANIC:/, 'member has no PANIC');
}
done_testing();
