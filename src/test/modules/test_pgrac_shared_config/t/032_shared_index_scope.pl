# Copyright (c) 2026, PostgreSQL Global Development Group
# PGRAC: reject unsupported concurrent-index phases before native side effects.
# Author: SqlRush <sqlrush@gmail.com>
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('index_scope');
$node->init;
$node->start;
$node->safe_psql('postgres', q{
CREATE EXTENSION test_pgrac_shared_config;
CREATE TABLE scope_heap(id integer);
INSERT INTO scope_heap VALUES (1), (2);
CREATE INDEX scope_index ON scope_heap(id);
});
# Native non-shared concurrent operations remain supported.
$node->safe_psql('postgres', 'CREATE INDEX CONCURRENTLY scope_native ON scope_heap(id)');
$node->safe_psql('postgres', 'REINDEX INDEX CONCURRENTLY scope_native');
is($node->safe_psql('postgres', 'SELECT count(*) FROM scope_heap'), '2',
	'non-shared native concurrent operations preserve data');

my $inventory = q{
SELECT c.oid,c.relname,i.indisvalid,i.indisready
FROM pg_class c JOIN pg_index i ON i.indexrelid=c.oid
WHERE i.indrelid='scope_heap'::regclass ORDER BY c.oid};
my $before = $node->safe_psql('postgres', $inventory);
for my $sql (
	'CREATE INDEX CONCURRENTLY scope_rejected ON scope_heap(id)',
	'REINDEX INDEX CONCURRENTLY scope_index',
	'REINDEX TABLE CONCURRENTLY scope_heap',
	'REINDEX SCHEMA CONCURRENTLY public',
	'REINDEX DATABASE CONCURRENTLY postgres',
	'REINDEX SYSTEM CONCURRENTLY postgres')
{
	my ($out, $err);
	my $rc = $node->psql('postgres',
		"\\set VERBOSITY verbose\nSELECT test_pgrac_config_sql_mode(true);\n$sql;",
		stdout => \$out, stderr => \$err);
	isnt($rc, 0, "$sql refuses shared mode");
	like($err, qr/0A000:.*(?:concurrent index creation or reindexing|REINDEX) is not supported/s,
		"$sql returns the precise unsupported-feature reason");
	is($node->safe_psql('postgres', $inventory), $before,
		"$sql leaves index identity and validity unchanged");
}
my ($out, $err);
my $rc = $node->psql('postgres', q{
CREATE TEMP TABLE scope_temp(id integer);
SELECT test_pgrac_config_sql_mode(true);
\set ON_ERROR_STOP off
CREATE INDEX CONCURRENTLY scope_temp_index ON scope_temp(id);
\set ON_ERROR_STOP on
SELECT test_pgrac_config_sql_mode(false);
}, stdout => \$out, stderr => \$err);
is($rc, 0, 'fixture restores real mode before temporary-table exit cleanup');
like($err, qr/concurrent index creation or reindexing is not supported/,
	'temporary relation reaches the same shared scope boundary');

# The shared rejection must precede even a DDL-start trigger. An exception
# here cannot be hidden by a transaction rollback or a final inventory check.
$node->safe_psql('postgres', q{
CREATE FUNCTION scope_ddl_poison() RETURNS event_trigger LANGUAGE plpgsql AS
$$BEGIN RAISE EXCEPTION 'scope guard reached DDL trigger'; END$$;
CREATE EVENT TRIGGER scope_poison ON ddl_command_start EXECUTE FUNCTION scope_ddl_poison();
});
for my $sql (
	'REINDEX INDEX scope_index',
	'VACUUM FULL scope_heap',
	'CLUSTER scope_heap USING scope_index',
	'CREATE UNLOGGED TABLE scope_unlogged(id int)',
	'CREATE UNLOGGED TABLE scope_unlogged_as AS SELECT 1 AS id',
	'ALTER TABLE scope_heap SET UNLOGGED',
	'ALTER TABLE scope_heap SET TABLESPACE pg_default',
	'CREATE MATERIALIZED VIEW scope_matview AS SELECT * FROM scope_heap',
	'REFRESH MATERIALIZED VIEW scope_missing',
	'CREATE DATABASE scope_database',
	'DROP DATABASE scope_missing',
	q{CREATE TABLESPACE scope_tablespace LOCATION '/nonexistent-scope'},
	'DROP TABLESPACE scope_missing',
	'CREATE EXTENSION scope_missing',
	q{CREATE SUBSCRIPTION scope_subscription CONNECTION '' PUBLICATION scope_pub WITH (connect = false)},
	'CREATE SCHEMA scope_schema CREATE UNLOGGED TABLE scope_new(id int)')
{
	my ($out, $err);
	my $rc = $node->psql('postgres',
		"\\set VERBOSITY verbose\nSELECT test_pgrac_config_sql_mode(true);\n$sql;",
		stdout => \$out, stderr => \$err);
	isnt($rc, 0, "$sql refuses shared scope");
	like($err, qr/0A000:.*not supported in shared mode/s,
		"$sql refuses before event trigger or native execution");
	unlike($err, qr/scope guard reached DDL trigger/,
		"$sql does not fire a DDL start trigger");
}
$node->safe_psql('postgres', 'DROP EVENT TRIGGER scope_poison');
$node->safe_psql('postgres', q{SELECT pg_replication_origin_create('scope_native_origin')});
for my $sql (
	q{SELECT pg_logical_emit_message(true, 'scope', 'message')},
	q{SELECT pg_logical_emit_message(false, 'scope', 'message')},
	q{SELECT pg_replication_origin_create('scope_origin')},
	q{SELECT pg_replication_origin_drop('scope_native_origin')},
	q{SELECT pg_replication_origin_advance('scope_native_origin', '0/1234')},
	q{SELECT pg_replication_origin_session_setup('scope_native_origin')})
{
	my ($out, $err);
	my $rc = $node->psql('postgres',
		"\\set VERBOSITY verbose\nBEGIN;\nSELECT test_pgrac_config_sql_mode(true);\n"
		. "\\set ON_ERROR_STOP off\n$sql;\nSELECT test_pgrac_config_sql_mode(false);\n"
		. "ROLLBACK;\n\\set ON_ERROR_STOP on\nSELECT test_pgrac_config_sql_mode(false);",
		stdout => \$out, stderr => \$err);
	is($rc, 0, "$sql fixture restores native mode after the attempted call");
	like($err, qr/0A000:.*not supported in shared mode/s,
		"$sql has explicit shared refusal rather than a later WAL failure");
}
is($node->safe_psql('postgres', q{SELECT count(*) FROM pg_replication_origin WHERE roname='scope_origin'}),
	'0', 'origin creation refused before catalog insertion');
is($node->safe_psql('postgres', $inventory), $before,
	'unsupported user operations preserve pre-existing btree identities');
$node->stop('fast');
done_testing();
