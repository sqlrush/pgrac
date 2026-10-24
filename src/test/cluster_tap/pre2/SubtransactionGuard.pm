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
use File::Find ();
use Cwd qw(abs_path);
use IPC::Run ();
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
	require_true(($out . $err) =~ /GUARD_STATE=0A000/, "$label reports exact SQLSTATE 0A000");
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

# Snapshot names, not changing contents of pre-existing shared relations.
sub storage_names
{
	my ($cluster) = @_;
	my @names;
	my $root = $cluster->shared_root;
	File::Find::find({no_chdir => 1, wanted => sub {
		return unless -f $_;
		my $name = $File::Find::name;
		$name =~ s/^\Q$root\E\///;
		push @names, $name if $name =~ m{(?:base/\d+|global)/\d+(?:[._].*)?$};
	}}, $root);
	return [sort @names];
}

sub catalog_refusal
{
	my ($cluster, $result, $label, $script, $caught_in_plpgsql, $operation) = @_;
	$operation //= 'creation';
	my $node = ($cluster->nodes)[0];
	my $before = storage_names($cluster);
	refusal($node, $result, $label,
		"SELECT pg_current_wal_insert_lsn() AS guard_start \\gset\n"
		. "\\echo GUARD_START=:guard_start\n" . $script
		. "\nSELECT pg_current_wal_insert_lsn() AS guard_end \\gset\n"
		. "\\echo GUARD_END=:guard_end\n");
	my $row = $result->{refusals}[-1];
	$row->{files_before} = $before;
	$row->{files_after} = storage_names($cluster);
	($row->{wal_start}) = $row->{stdout} =~ /GUARD_START=([0-9A-F]+\/[0-9A-F]+)/;
	($row->{wal_end}) = $row->{stdout} =~ /GUARD_END=([0-9A-F]+\/[0-9A-F]+)/;
	$row->{wal_dir} = abs_path($node->data_dir . '/pg_wal');
	save_result($result);
	require_true(defined($row->{wal_start}) && defined($row->{wal_end})
		&& defined($row->{wal_dir}), "$label captured exact WAL interval");
	require_identity($row->{files_after}, $before, "$label creates no relation files");
	require_true($row->{stderr} =~ /top-level transaction/
		&& $row->{stderr} =~ /PL\/pgSQL EXCEPTION/, "$label retains actionable hint");
	require_true($row->{stderr} =~ /shared relation storage \Q$operation\E does not support subtransactions/,
		"$label refuses at storage producer");
	require_true($caught_in_plpgsql || $row->{stderr} =~ /RelationCheckStorageTransaction/,
		"$label reports actual pre-storage producer location");
}

# Use a separate cohort so catalog cleanup cannot hide DML acceptance results.
sub run_catalog_guard_case
{
	run_scenario('pre2_catalog_subtransaction_refusal', [], sub {
		my ($cluster) = @_;
		my @nodes = $cluster->nodes;
		my $result = {refusals => [], same_data_restart => JSON::PP::false};
		catalog_refusal($cluster, $result, 'shared catalog DDL', q{
            BEGIN; SAVEPOINT child;
            CREATE TABLE guard_forbidden (v int);
            \echo GUARD_STATE=:SQLSTATE
            ROLLBACK TO child; COMMIT;
        });
		catalog_refusal($cluster, $result, 'internal serial sequence DDL', q{
            BEGIN; SAVEPOINT child;
            CREATE TABLE guard_serial_forbidden (id serial);
            \echo GUARD_STATE=:SQLSTATE
            ROLLBACK TO child; RELEASE child; COMMIT;
        });
		catalog_refusal($cluster, $result, 'CREATE TABLE AS', q{
            BEGIN; SAVEPOINT child;
            CREATE TABLE guard_ctas_forbidden AS SELECT 1 AS v;
            \echo GUARD_STATE=:SQLSTATE
            ROLLBACK TO child; COMMIT;
        });
		catalog_refusal($cluster, $result, 'PL/pgSQL EXCEPTION DDL', q{
            DO $$
            DECLARE message text; hint text; state text;
            BEGIN
              BEGIN
                CREATE TABLE guard_exception_forbidden(v int);
                RAISE EXCEPTION 'DDL unexpectedly succeeded';
              EXCEPTION WHEN feature_not_supported THEN
                GET STACKED DIAGNOSTICS message = MESSAGE_TEXT,
                  hint = PG_EXCEPTION_HINT, state = RETURNED_SQLSTATE;
                RAISE NOTICE 'GUARD_STATE=%; %; HINT: %', state, message, hint;
                IF state <> '0A000' THEN RAISE EXCEPTION 'wrong refusal'; END IF;
              END;
            END $$;
            \echo HANDLER_STATE=:SQLSTATE
        }, 1);
		require_true($result->{refusals}[-1]{stdout} =~ /HANDLER_STATE=00000/,
			'EXCEPTION handler returns normally');
		# Legal top-level DDL and temp subtransaction writes remain usable.
		sql($nodes[0], q{CREATE TABLE guard_parent(v int);
            INSERT INTO guard_parent VALUES (7),(11)});
		sql($nodes[0], 'CREATE SEQUENCE guard_sequence START 41');
		settle_setup_receipts($nodes[0], $result);
		catalog_refusal($cluster, $result, 'sequence rewrite', q{
            BEGIN; SAVEPOINT child;
            ALTER SEQUENCE guard_sequence RESTART WITH 99;
            \echo GUARD_STATE=:SQLSTATE
            ROLLBACK TO child; COMMIT;
        }, 0, 'sequence rewrite');
		require_equal(sql($nodes[0], "SELECT nextval('guard_sequence')"), '41',
			'top-level sequence access preserves the refused rewrite');
		require_equal(sql($nodes[0], q{CREATE TEMP TABLE guard_temp(v int);
            BEGIN; SAVEPOINT child; INSERT INTO guard_temp VALUES (1);
            ROLLBACK TO child; RELEASE child; INSERT INTO guard_temp VALUES (2);
            COMMIT; SELECT sum(v) FROM guard_temp}), '2', 'native temp rollback is unchanged');
		for my $node (@nodes)
		{
			require_equal(sql($node, q{SELECT count(*) || ':' || sum(v) FROM guard_parent}),
				'2:18', 'four-member committed data agrees');
			require_equal(sql($node, q{SELECT count(*) FROM pg_class
                WHERE relname IN ('guard_forbidden','guard_serial_forbidden',
                'guard_serial_forbidden_id_seq','guard_ctas_forbidden',
                'guard_exception_forbidden')}), '0', 'refused objects never enter shared catalog');
		}
		$result->{four_member_reconciliation} = JSON::PP::true;
		save_result($result);
		$cluster->stop_cluster;
		$result->{first_normal_stop} = JSON::PP::true;
		# The original stop has flushed the bounded intervals. XID assignment
		# and normal abort records are allowed; storage/undo mutation is not.
		for my $index (0 .. $#{$result->{refusals}})
		{
			my $row = $result->{refusals}[$index];
			my ($dump, $err) = ('', '');
			if ($row->{wal_start} ne $row->{wal_end})
			{
				my @command = ('pg_waldump', '-p', $row->{wal_dir},
					'-s', $row->{wal_start}, '-e', $row->{wal_end});
				$row->{wal_command} = \@command;
				my $ok = IPC::Run::run(\@command, '>', \$dump, '2>', \$err, IPC::Run::timeout(10));
				$row->{wal_dump_ok} = $ok ? JSON::PP::true : JSON::PP::false;
				$row->{wal_stderr} = $err;
				save_result($result);
				require_true($ok, 'bounded refusal WAL decodes completely');
			}
			my $path = "$ENV{TESTLOGDIR}/catalog-refusal-$index.wal.txt";
			open(my $wal, '>', $path) or die $!;
			print {$wal} $dump;
			close($wal) or die $!;
			$row->{wal_dump} = $path;
			require_true($dump !~ /rmgr:\s*(?:Storage|Heap|Heap2|Btree)\b/
				&& $dump !~ /desc:\s*(?:UNDO_BLOCK_WRITE|UNDO_TT_SLOT_CTRC_PREPARE)/,
				'no structure, tuple or undo-prepare WAL before refusal');
		}
		save_result($result);
		$cluster->restart_cluster;
		for my $node (@nodes)
		{
			require_equal(sql($node, q{SELECT count(*) || ':' || sum(v) FROM guard_parent}),
				'2:18', 'same DATA restart preserves committed data');
		}
		$result->{same_data_restart} = JSON::PP::true;
		$cluster->stop_cluster;
		$result->{second_normal_stop} = JSON::PP::true;
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
