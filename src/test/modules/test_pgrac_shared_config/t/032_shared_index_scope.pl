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
	like($err, qr/0A000:.*concurrent index creation or reindexing is not supported/s,
		"$sql returns the precise unsupported-feature reason");
	is($node->safe_psql('postgres', $inventory), $before,
		"$sql leaves index identity and validity unchanged");
}
my ($out, $err);
my $rc = $node->psql('postgres', q{
CREATE TEMP TABLE scope_temp(id integer);
SELECT test_pgrac_config_sql_mode(true);
CREATE INDEX CONCURRENTLY scope_temp_index ON scope_temp(id);
}, stdout => \$out, stderr => \$err);
isnt($rc, 0, 'temporary relation cannot silently downgrade CONCURRENTLY');
like($err, qr/concurrent index creation or reindexing is not supported/,
	'temporary relation reaches the same shared scope boundary');
$node->stop('fast');
done_testing();
