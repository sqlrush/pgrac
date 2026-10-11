# Copyright (c) 2026, pgrac contributors
# Author: SqlRush <sqlrush@gmail.com>
# PGRAC: exercise shared-heap refusal before any undo or CTRC receipt.
package SubtransactionGuard;
use strict;
use warnings;
use Exporter 'import';
use Scenario;
use JSON::PP;
use PostgreSQL::Test::Utils qw(slurp_file);
use Time::HiRes qw(time sleep);
our @EXPORT = qw(run_guard_case run_catalog_guard_case);

sub save_result
{
	my ($result) = @_;
	open(my $out, '>', "$ENV{TESTLOGDIR}/subtransaction-guard.json") or die $!;
	print {$out} JSON::PP->new->canonical->pretty->encode($result);
	close($out) or die $!;
}

sub counters
{
	my ($node, $timeout) = @_;
	my $raw = $node->safe_psql('postgres', q{SELECT category || '.' || key || '=' || value
        FROM pg_cluster_state WHERE (category='undo' AND key='prepare_start')
        OR (category='ctrc' AND key IN ('receipt_prepared', 'receipt_applied'))
        ORDER BY category, key}, timeout => $timeout // 30);
	my %counts;
	for my $line (split /\n/, $raw)
	{
		die "invalid receipt counter: $line" unless $line =~ /^([a-z_.]+)=(\d+)$/;
		$counts{$1} = 0 + $2;
	}
	require_equal(scalar(keys %counts), '3', 'all receipt counters available');
	return \%counts;
}

sub settle_setup_receipts
{
	my ($node, $result) = @_;
	my $started = time();
	# These are live gauges. Retire setup work before measuring a refusal.
	while (1)
	{
		my $remaining = 30 - (time() - $started);
		die 'setup receipts did not retire within 30 seconds' if $remaining <= 0;
		my $current = counters($node, $remaining);
		my $elapsed = time() - $started;
		$result->{setup_receipts} = {counts => $current, elapsed => $elapsed};
		save_result($result);
		die 'setup receipts did not retire within 30 seconds' if $elapsed >= 30;
		return if $current->{'ctrc.receipt_prepared'} == 0
			&& $current->{'ctrc.receipt_applied'} == 0;
		sleep(0.1);
	}
}

sub refusal
{
	my ($node, $result, $label, $script) = @_;
	my $row = {label => $label, before => counters($node)};
	push @{$result->{refusals}}, $row;
	save_result($result);
	my ($rc, $out, $err) = $node->psql('postgres',
		"\\set VERBOSITY verbose\n" . $script, on_error_stop => 0, timeout => 30);
	$row->{rc} = $rc;
	$row->{stdout} = $out;
	$row->{stderr} = $err;
	save_result($result);
	require_equal($rc, '0', "$label keeps the connection usable");
	require_true($out =~ /GUARD_STATE=0A000/, "$label reports exact SQLSTATE 0A000");
	$row->{after} = counters($node);
	save_result($result);
	for my $key (sort keys %{$row->{before}})
	{
		require_equal($row->{after}{$key}, $row->{before}{$key}, "$label preserves $key");
	}
}

sub run_guard_case
{
	my ($exception) = @_;
	my $name = $exception ? 'pre2_exception_refusal' : 'pre2_subtransaction_refusal';
	run_scenario($name, [], sub {
		my ($cluster) = @_;
		my @nodes = $cluster->nodes;
		my $result = {refusals => [], same_data_restart => JSON::PP::false};
		sql($nodes[0], 'CREATE TABLE guard_rows (id int PRIMARY KEY, v int NOT NULL)');
		sql($nodes[0], 'INSERT INTO guard_rows VALUES (1,0),(2,0),(3,0),(4,0)');
		if ($exception)
		{
			# The function's handler catches the original DML error; COMMIT
			# must not later unlock an undo buffer released by subabort.
			sql($nodes[0], q{CREATE FUNCTION guard_exception() RETURNS text LANGUAGE plpgsql AS $$
                BEGIN
                  BEGIN
                    UPDATE guard_rows SET v=999 WHERE id=2;
                    RETURN 'unexpected success';
                  EXCEPTION WHEN others THEN
                    RETURN SQLSTATE;
                  END;
                END $$});
			settle_setup_receipts($nodes[0], $result);
			refusal($nodes[0], $result, 'PL/pgSQL EXCEPTION', q{
                BEGIN;
                SELECT 'GUARD_STATE=' || guard_exception();
                COMMIT;
            });
		}
		else
		{
			settle_setup_receipts($nodes[0], $result);
			for my $operation (
				'UPDATE guard_rows SET v=999 WHERE id=2',
				'INSERT INTO guard_rows VALUES (5,999)',
				'DELETE FROM guard_rows WHERE id=2',
				'SELECT * FROM guard_rows WHERE id=2 FOR UPDATE')
			{
				refusal($nodes[0], $result, $operation,
					"BEGIN; SAVEPOINT child;\n$operation;\n"
					. "\\echo GUARD_STATE=:SQLSTATE\nROLLBACK TO child; COMMIT;\n");
			}
			refusal($nodes[0], $result, 'nested savepoints', q{
                BEGIN; SAVEPOINT a; SAVEPOINT b; SAVEPOINT c;
                UPDATE guard_rows SET v=999 WHERE id=2;
                \echo GUARD_STATE=:SQLSTATE
                ROLLBACK TO a; COMMIT;
            });
			refusal($nodes[0], $result, 'psql ON_ERROR_ROLLBACK', q{
                \set ON_ERROR_ROLLBACK on
                BEGIN;
                UPDATE guard_rows SET v=999 WHERE id=2;
                \echo GUARD_STATE=:SQLSTATE
                COMMIT;
            });
		}
		# T1: the first top-level write after a rejected child must commit.
		my ($rc, $out, $err) = $nodes[0]->psql('postgres', q{
            BEGIN; SAVEPOINT child;
            UPDATE guard_rows SET v=999 WHERE id=2;
            \echo GUARD_STATE=:SQLSTATE
            ROLLBACK TO child; RELEASE child;
            UPDATE guard_rows SET v=8 WHERE id=1;
            COMMIT;
        }, on_error_stop => 0, timeout => 30);
		$result->{parent_continues} = {rc => $rc, stdout => $out, stderr => $err};
		save_result($result);
		require_equal($rc, '0', 'parent writes after refused child');
		require_true($out =~ /GUARD_STATE=0A000/, 'continuation child refused');
		# S1: a refused child must not mark the parent's already-written ITL
		# as aborted. The parent commits its original write on the same page.
		require_equal(sql($nodes[0], q{SELECT count(DISTINCT
            split_part(trim(both '()' from ctid::text), ',', 1))
            FROM guard_rows WHERE id IN (3,4)}), '1', 'S1 rows share one page');
		($rc, $out, $err) = $nodes[0]->psql('postgres', q{
            BEGIN; UPDATE guard_rows SET v=7 WHERE id=3; SAVEPOINT child;
            UPDATE guard_rows SET v=999 WHERE id=4;
            \echo GUARD_STATE=:SQLSTATE
            ROLLBACK TO child; RELEASE child; COMMIT;
        }, on_error_stop => 0, timeout => 30);
		$result->{parent_survives} = {rc => $rc, stdout => $out, stderr => $err};
		save_result($result);
		require_equal($rc, '0', 'parent commits its write after refused child');
		require_true($out =~ /GUARD_STATE=0A000/, 'S1 child refused');
		# Native temporary relations and read-only savepoints retain PG semantics.
		require_equal(sql($nodes[0], q{CREATE TEMP TABLE guard_temp(v int);
            BEGIN; SAVEPOINT child; INSERT INTO guard_temp VALUES (1);
            SELECT count(*) FROM guard_temp; RELEASE child; COMMIT}),
			'1', 'temporary subtransaction remains native');
		for my $node (@nodes)
		{
			require_equal(sql($node, q{BEGIN; SAVEPOINT read_only;
                SELECT string_agg(id||':'||v, ',' ORDER BY id) FROM guard_rows;
                RELEASE read_only; COMMIT}), '1:8,2:0,3:7,4:0', 'remote parent result is correct');
		}
		$cluster->stop_cluster;
		$cluster->restart_cluster;
		for my $node (@nodes)
		{
			require_equal(sql($node, q{SELECT string_agg(id||':'||v, ',' ORDER BY id)
                FROM guard_rows}), '1:8,2:0,3:7,4:0', 'same DATA preserves committed parent');
		}
		$result->{same_data_restart} = JSON::PP::true;
		$cluster->stop_cluster;
		$result->{logs} = [map {my $log = slurp_file($_->logfile);
			{path => $_->logfile, panics => scalar(() = $log =~ /PANIC:/g),
			 strict_unlock_errors => scalar(() = $log =~ /does not hold buffer slot/g)}} @nodes];
		save_result($result);
		for my $row (@{$result->{logs}})
		{
			require_equal($row->{panics} + $row->{strict_unlock_errors}, '0', 'complete log has no PANIC');
		}
	});
}

# Use a separate cohort so catalog cleanup cannot hide DML acceptance results.
sub run_catalog_guard_case
{
	run_scenario('pre2_catalog_subtransaction_refusal', [], sub {
		my ($cluster) = @_;
		my @nodes = $cluster->nodes;
		my $result = {refusals => []};
		refusal($nodes[0], $result, 'shared catalog DDL', q{
            BEGIN; SAVEPOINT child;
            CREATE TABLE guard_forbidden (v int);
            \echo GUARD_STATE=:SQLSTATE
            ROLLBACK TO child; COMMIT;
        });
		$cluster->stop_cluster;
		$result->{logs} = [map {my $log = slurp_file($_->logfile);
			{path => $_->logfile, panics => scalar(() = $log =~ /PANIC:/g),
			 strict_unlock_errors => scalar(() = $log =~ /does not hold buffer slot/g)}} @nodes];
		save_result($result);
		for my $row (@{$result->{logs}})
		{
			require_equal($row->{panics} + $row->{strict_unlock_errors}, '0', 'complete log has no PANIC');
		}
	});
}
1;
