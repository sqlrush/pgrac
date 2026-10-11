#!/usr/bin/env perl
# Author: SqlRush <sqlrush@gmail.com>
# PGRAC: cancel real database maintenance and verify transaction snapshot cleanup.
use strict;
use warnings;
use FindBin;
use lib "$FindBin::RealBin/../../perl", "$FindBin::RealBin/../pre2";
use Scenario;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(time usleep);

run_scenario('pre2_vacuum_snapshot_cancel', [], sub {
	my ($cluster) = @_;
	my @nodes = $cluster->nodes;
	my $node = $nodes[0];
	my $original = sql($node, q{SELECT datconnlimit FROM pg_database WHERE datname='template1'});
	require_true($original =~ /^-?\d+$/, 'original connection limit is numeric');
	my $holder = $node->background_psql('postgres', timeout => 60);
	my $worker = $node->background_psql('template1', timeout => 60);
	my $pid = $worker->query_safe('SELECT pg_backend_pid()');
	require_true($pid =~ /^\d+$/, 'maintenance backend identity');
	$worker->query_safe("\\set VERBOSITY verbose\n\\set ON_ERROR_STOP off\nSET backtrace_functions='ProcessInterrupts'");
	my $before_tuple = sql($node, q{SELECT ctid::text FROM pg_database WHERE datname='template1'});
	$holder->query_safe('BEGIN; ALTER DATABASE template1 CONNECTION LIMIT 5');
	my $holder_identity = $holder->query_safe(q{SELECT pg_backend_pid(),
        pg_current_xact_id_if_assigned()::text, oid, ctid, xmin, xmax
        FROM pg_database WHERE datname='template1'});
	my ($holder_pid, $holder_xid, $database_oid, $after_tuple) = split /\|/, $holder_identity;
	require_true($holder_pid =~ /^[1-9][0-9]*$/ && $holder_xid =~ /^[1-9][0-9]*$/,
		'real catalogue updater has an assigned transaction');
	diag("catalogue updater=$holder_identity old_tuple=$before_tuple worker=$pid");
	$worker->query_until(qr/VACUUM_SENT/, "\\echo VACUUM_SENT\nVACUUM (FREEZE);\n");
	my ($waiting, $last) = (0, '');
	my $deadline = time + 30;
	while (time < $deadline)
	{
		$last = sql($node, "SELECT state || '|' || coalesce(wait_event_type,'') || '|' || coalesce(wait_event,'') FROM pg_stat_activity WHERE pid=$pid");
		# The event is also used by earlier relation-lock release waits. The
		# original transaction-wait context must identify this catalogue row
		# before cancellation; the backtrace below independently checks it.
		if ($last =~ /^active\|(?:Lock\|transactionid|Cluster: BgProc\|ClusterGesReplyWait)$/)
		{
			my $observed_log = slurp_file($node->logfile);
			if ($observed_log =~ /\[$pid\] CONTEXT:  while updating tuple (?:\Q$before_tuple\E|\Q$after_tuple\E) in relation "pg_database"/)
			{
				$waiting = 1;
				last;
			}
		}
		usleep(100_000);
	}
	diag("maintenance wait pid=$pid state=$last");
	require_true($waiting, 'maintenance waits for the real catalogue updater');
	require_equal(sql($node, "SELECT (backend_xmin IS NOT NULL)::text FROM pg_stat_activity WHERE pid=$pid"),
		'true', 'waiting maintenance retains a real transaction snapshot');
	require_equal(sql($node, "SELECT pg_cancel_backend($pid)"), 't', 'cancel maintenance');
	my ($output, $error) = $worker->query('SELECT 1');
	diag("cancelled maintenance stderr: $worker->{stderr}");
	require_equal($error, '1', 'original cancellation was reported');
	require_true($worker->{stderr} =~ /57014/, 'original query-cancel SQLSTATE');
	my $log = slurp_file($node->logfile);
	my ($cancel) = $log =~ /(\[$pid\] ERROR:.*?\[$pid\] STATEMENT:[^\n]*)/s;
	require_true(defined($cancel) && $cancel =~ /canceling statement due to user request/,
		'cancelled statement is identified in the original server log');
	require_true($cancel =~ /XactLockTableWait/ && $cancel =~ /heap_inplace_lock/
		&& $cancel =~ /vac_update_datfrozenxid/,
		'real cancellation occurred inside database maintenance transaction waiting');
	require_true($cancel =~ /while updating tuple (?:\Q$before_tuple\E|\Q$after_tuple\E) in relation "pg_database"/,
		'cancelled inplace wait belongs to the same catalogue row versions');
	require_equal($holder->query_safe('SELECT pg_current_xact_id_if_assigned()::text'),
		$holder_xid, 'original updater transaction remains the blocking owner');
	my $idle = sql($node, "SELECT state || '|' || (backend_xmin IS NULL)::text FROM pg_stat_activity WHERE pid=$pid");
	require_equal($idle, 'idle|true', 'cancelled transaction released its xmin');
	$worker->{stderr} = ''; # The original error is retained above and in the log.
	$holder->query_safe('COMMIT');
	$holder->quit;
	$worker->query_safe('VACUUM (FREEZE)');
	$worker->quit;
	sql($node, "ALTER DATABASE template1 CONNECTION LIMIT $original");
	$cluster->stop_cluster;
});
