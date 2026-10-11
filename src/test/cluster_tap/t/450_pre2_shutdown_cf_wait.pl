#!/usr/bin/env perl
# Author: SqlRush <sqlrush@gmail.com>
# PGRAC: a normal shutdown checkpoint spans a real CF owner wait slice.
use strict;
use warnings;
use FindBin;
use lib "$FindBin::RealBin/../../perl", "$FindBin::RealBin/../pre2";
use Scenario;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(time usleep);

run_scenario('pre2_shutdown_cf_wait', [], sub {
	my ($cluster) = @_;
	my @nodes = $cluster->nodes;
	my $hook = "$FindBin::RealBin/../pre2/cf_shutdown_hook.py";
	die 'bounded CF debugger hook is not an absolute existing file'
	  unless defined($hook) && $hook =~ m{^/} && -f $hook;
	my $pid = sql($nodes[0], q{SELECT pid FROM pg_stat_activity WHERE backend_type = 'checkpointer'});
	require_true($pid =~ /^[1-9][0-9]*$/, 'one exact checkpointer was found');
	my $output = "$ENV{TESTLOGDIR}/cf-shutdown-holder";
	require_true(system('python3', $hook, 'start', $pid, $output) == 0,
		'bounded real CF holder was armed');
	my $error;
	eval {
		my $deadline = time() + 10;
		while (!-f "$output/armed" && time() < $deadline)
		{
			last if -f "$output/result.json";
			usleep(10000);
		}
		require_true(-f "$output/armed", 'debugger is ready before shutdown begins');
		$cluster->stop_cluster;
		1;
	} or $error = $@;
	my $finished = system('python3', $hook, 'finish', $pid, $output);
	die $error if defined($error);
	require_true($finished == 0, 'the original CF holder was released after its bounded pause');
	my $waits = 0;
	for my $node (@nodes)
	{
		my $log = slurp_file($node->logfile);
		$waits += () = $log =~ /PGRAC_FAMILY=CF_OWNER_WAIT consumer=checkpoint operation=(?:prepare|publish) mode=\d+ request=[1-9][0-9]* epoch=[1-9][0-9]* caller_cookie=[1-9][0-9]* owner_state=\d+ result=\d+ timeout_source=\d+ remaining_known=1 remaining_us=[1-9][0-9]*/g;
	}
	require_true($waits > 0, 'shutdown continued an original CF owner within its existing budget');
	diag("shutdown CF wait records=$waits holder_pid=$pid");
}, ['cluster.cf_enqueue_timeout_ms = 1000']);
