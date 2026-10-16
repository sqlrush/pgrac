# Copyright (c) 2026, pgrac contributors
# Author: SqlRush <sqlrush@gmail.com>
# PGRAC AM05: deterministic transaction-boundary guard for shared DDL.
package SharedDDLAutocommit;
use strict;
use warnings;
use Exporter 'import';
use Scenario;
our @EXPORT = qw(run_shared_ddl_autocommit_case);

sub refuse_in_transaction
{
	my ($node, $label, $sql, $table, $expected_rows) = @_;
	my ($rc, $out, $err) = $node->psql('postgres',
		"\\set VERBOSITY verbose\n$sql", on_error_stop => 0, timeout => 30);
	my $all = ($out // '') . ($err // '');
	require_equal($rc, '0', "$label leaves the session usable");
	require_true($all =~ /AM05_STATE=25001/, "$label reports SQLSTATE 25001");
	require_true($err =~ /standalone top-level statement/,
		"$label reports the standalone-statement hint");
	require_equal(sql($node, "SELECT count(*) FROM $table"), $expected_rows,
		"$label leaves the relation present and unchanged");
}

sub run_shared_ddl_autocommit_case
{
	run_scenario('pre2_shared_ddl_autocommit', [], sub {
		my ($cluster) = @_;
		my ($node) = $cluster->nodes;
		require_equal(sql($node, "SHOW cluster.shared_storage_backend"), 'cluster_fs',
			'shared storage backend is enabled');
		require_equal(sql($node, "SHOW cluster.smgr_user_relations"), 'on',
			'shared user relation routing is enabled');
		require_equal(sql($node, "SHOW cluster.interconnect_tier"), 'tier1',
			'tier1 is the AM05 supported transport');

		sql($node, 'CREATE TABLE am05_drop_guard (id int PRIMARY KEY, v int)');
		sql($node, 'INSERT INTO am05_drop_guard VALUES (1, 10)');
		refuse_in_transaction($node, 'DROP in an explicit transaction', q{
            BEGIN;
            INSERT INTO am05_drop_guard VALUES (2, 20);
            DROP TABLE am05_drop_guard;
            \echo AM05_STATE=:SQLSTATE
            ROLLBACK;
        }, 'am05_drop_guard', '1');

		sql($node, 'CREATE TABLE am05_truncate_guard (id int PRIMARY KEY, v int)');
		sql($node, 'INSERT INTO am05_truncate_guard VALUES (1, 10)');
		refuse_in_transaction($node, 'TRUNCATE in an explicit transaction', q{
            BEGIN;
            INSERT INTO am05_truncate_guard VALUES (2, 20);
            TRUNCATE am05_truncate_guard;
            \echo AM05_STATE=:SQLSTATE
            ROLLBACK;
        }, 'am05_truncate_guard', '1');

		my ($rc, $out, $err) = $node->psql('postgres', q{
            DO $$
            DECLARE state text; hint text;
            BEGIN
              BEGIN
                DROP TABLE am05_drop_guard;
              EXCEPTION WHEN active_sql_transaction THEN
                GET STACKED DIAGNOSTICS state = RETURNED_SQLSTATE,
                  hint = PG_EXCEPTION_HINT;
                RAISE NOTICE 'AM05_DO_STATE=% HINT=%', state, hint;
              END;
            END $$;
        }, on_error_stop => 0, timeout => 30);
		require_equal($rc, '0', 'DO handler keeps the session usable');
		require_true(($out // '') =~ /AM05_DO_STATE=25001/,
			'DO reports SQLSTATE 25001 before DROP side effects');
		require_true(($out // '') =~ /standalone top-level statement/,
			'DO reports the standalone-statement hint');
		require_equal(sql($node, 'SELECT count(*) FROM am05_drop_guard'), '1',
			'DO refusal leaves the relation present');

		require_equal(sql($node, q{
            CREATE TEMP TABLE am05_temp_guard (v int);
            INSERT INTO am05_temp_guard VALUES (1),(2);
            BEGIN; TRUNCATE am05_temp_guard; COMMIT;
            SELECT count(*) FROM am05_temp_guard
        }), '0', 'temporary TRUNCATE keeps native semantics');

		sql($node, 'DROP TABLE am05_drop_guard');
		sql($node, 'DROP TABLE am05_truncate_guard');
		$cluster->stop_cluster;
	});
}

1;
