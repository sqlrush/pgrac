#!/usr/bin/env perl
# Author: SqlRush <sqlrush@gmail.com>
# PGRAC: a real online checkpoint spans the original CF wait slice.
use strict;
use warnings;
use FindBin;
use lib "$FindBin::RealBin/../../perl", "$FindBin::RealBin/../pre2";
use Scenario;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(time usleep);

run_scenario('pre2_checkpoint_cf_wait', [], sub {
	my ($cluster) = @_;
	my @nodes = $cluster->nodes;
	my $module = $ENV{PGRAC_PRE2_CF_WAIT_MODULE};
	die 'CF test module is not an absolute existing file'
	  unless defined($module) && $module =~ m{^/} && -f $module;
	$module =~ s/'/''/g;
	sql($nodes[0], "CREATE FUNCTION pre2_cf_hold(int) RETURNS void AS '$module', 'pre2_cf_hold' LANGUAGE C STRICT");
	my $holder = $nodes[1]->background_psql('postgres', timeout => 30);
	my $pid = $holder->query_safe('SELECT pg_backend_pid()');
	$holder->query_until(qr/holder-started/, "SELECT 'holder-started'; SELECT pre2_cf_hold(2800);\n");
	my $deadline = time() + 5;
	my $observed = 0;
	while (time() < $deadline)
	{
		my $log = slurp_file($nodes[1]->logfile);
		if ($log =~ /\[$pid\] LOG:  CF TAP hold granted/)
		{
			$observed = 1;
			last;
		}
		usleep(10000);
	}
	require_true($observed, 'original CF holder grant was observed');
	my $started = time();
	sql($nodes[0], 'CHECKPOINT');
	my $elapsed = time() - $started;
	$holder->query_safe('SELECT 1');
	$holder->quit;
	my $log = slurp_file($nodes[0]->logfile);
	my @waits = $log =~ /PGRAC_FAMILY=CF_OWNER_WAIT consumer=checkpoint operation=prepare mode=\d+ request=([1-9][0-9]*) epoch=([1-9][0-9]*) caller_cookie=([1-9][0-9]*) owner_state=\d+ result=\d+ timeout_source=\d+ remaining_known=0 remaining_us=0/g;
	require_true(@waits == 3,
		'checkpoint records one wait with its original nonzero owner identity');
	require_true($elapsed >= 1, 'checkpoint actually spans the original one-second CF slice');
	diag("checkpoint elapsed_seconds=$elapsed holder_pid=$pid");
	$cluster->stop_cluster;
}, ['cluster.cf_enqueue_timeout_ms = 1000']);
