# Copyright (c) 2026, PostgreSQL Global Development Group
# PGRAC: native SQL semantics for the supported shared-catalog command surface.
# This does not certify cross-node storage, recovery, or distributed admission.
# Author: SqlRush <sqlrush@gmail.com>
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('catalog_transactions');
$node->init;
$node->append_conf('postgresql.conf', 'cluster.enabled=off');
$node->start;

sub sql_is
{
    my ($sql, $expected, $name) = @_;
    is($node->safe_psql('postgres', $sql), $expected, $name);
}

sql_is(q{
BEGIN;
CREATE SCHEMA p3b;
CREATE TABLE p3b.parent(id integer PRIMARY KEY, v text);
CREATE TABLE p3b.child(id integer REFERENCES p3b.parent(id), n integer CHECK(n>0));
INSERT INTO p3b.parent VALUES(1,'one');
INSERT INTO p3b.child VALUES(1,1);
SAVEPOINT before_column;
ALTER TABLE p3b.parent ADD COLUMN reverted integer;
CREATE INDEX reverted_index ON p3b.parent(v);
ROLLBACK TO before_column;
ALTER TABLE p3b.parent ADD COLUMN retained integer DEFAULT 4;
CREATE INDEX retained_index ON p3b.parent USING btree(v);
COMMIT;
SELECT string_agg(attname,',' ORDER BY attnum) FROM pg_attribute
 WHERE attrelid='p3b.parent'::regclass AND attnum>0 AND NOT attisdropped;
SELECT v,retained FROM p3b.parent;
SELECT to_regclass('p3b.reverted_index') IS NULL,
       to_regclass('p3b.retained_index') IS NOT NULL;
}, "id,v,retained\none|4\nt|t", 'table, column, constraints and btree honor SAVEPOINT and COMMIT');

sql_is(q{
BEGIN;
ALTER TABLE p3b.child DROP CONSTRAINT child_n_check;
ALTER TABLE p3b.parent RENAME COLUMN v TO renamed;
DROP INDEX p3b.retained_index;
ROLLBACK;
SELECT count(*) FROM pg_constraint WHERE conrelid='p3b.child'::regclass AND contype='c';
SELECT v FROM p3b.parent;
SELECT to_regclass('p3b.retained_index') IS NOT NULL;
}, "1\none\nt", 'ROLLBACK restores constraint, column identity and index');

sql_is(q{
BEGIN;
CREATE TABLE p3b.aborted(id integer);
CREATE SCHEMA p3b_aborted;
CREATE VIEW p3b.aborted_view AS SELECT 1 AS n;
CREATE FUNCTION p3b.aborted_function() RETURNS integer LANGUAGE SQL AS 'SELECT 1';
ROLLBACK;
SELECT to_regclass('p3b.aborted') IS NULL, to_regnamespace('p3b_aborted') IS NULL,
       to_regclass('p3b.aborted_view') IS NULL, to_regprocedure('p3b.aborted_function()') IS NULL;
}, 't|t|t|t', 'aborted creates leave no table, schema, view or function');

sql_is(q{
BEGIN;
TRUNCATE p3b.parent, p3b.child;
SELECT count(*) FROM p3b.parent;
ROLLBACK;
SELECT count(*) FROM p3b.parent;
BEGIN;
SAVEPOINT s;
TRUNCATE p3b.parent, p3b.child;
ROLLBACK TO s;
COMMIT;
SELECT count(*) FROM p3b.child;
BEGIN;
TRUNCATE p3b.parent, p3b.child;
COMMIT;
SELECT count(*) FROM p3b.parent;
INSERT INTO p3b.parent VALUES(1,'one',4);
}, "0\n1\n1\n0", 'TRUNCATE retains rollback/savepoint semantics and commits empty data');

sql_is(q{
CREATE VIEW p3b.v AS SELECT id,v FROM p3b.parent;
CREATE FUNCTION p3b.f() RETURNS integer LANGUAGE SQL AS 'SELECT 1';
BEGIN;
SAVEPOINT s;
CREATE OR REPLACE VIEW p3b.v AS SELECT id,v || 'changed' AS v FROM p3b.parent;
CREATE OR REPLACE FUNCTION p3b.f() RETURNS integer LANGUAGE SQL AS 'SELECT 9';
ROLLBACK TO s;
COMMIT;
SELECT v,p3b.f() FROM p3b.v;
}, 'one|1', 'view/function replacement rolls back at SAVEPOINT');

my $cached = $node->background_psql('postgres');
$cached->query_safe('PREPARE p3b_plan AS SELECT id,v,p3b.f() FROM p3b.v');
is($cached->query_safe('EXECUTE p3b_plan'), '1|one|1', 'existing backend caches relation and function');
$node->safe_psql('postgres', q{
BEGIN;
ALTER TABLE p3b.parent ADD COLUMN extra integer;
CREATE OR REPLACE FUNCTION p3b.f() RETURNS integer LANGUAGE SQL AS 'SELECT 2';
CREATE OR REPLACE VIEW p3b.v AS SELECT id,v || '!' AS v FROM p3b.parent;
COMMIT;
});
is($cached->query_safe('EXECUTE p3b_plan'), '1|one!|2', 'native SI refreshes cached plan, view and function');

sql_is(q{
BEGIN;
CREATE ROLE p3b_reader;
GRANT USAGE ON SCHEMA p3b TO p3b_reader;
GRANT SELECT ON p3b.parent TO p3b_reader;
COMMIT;
SELECT has_table_privilege('p3b_reader','p3b.parent','SELECT');
BEGIN;
SAVEPOINT s;
REVOKE SELECT ON p3b.parent FROM p3b_reader;
ROLLBACK TO s;
COMMIT;
SELECT has_table_privilege('p3b_reader','p3b.parent','SELECT');
BEGIN;
REVOKE SELECT ON p3b.parent FROM p3b_reader;
COMMIT;
SELECT has_table_privilege('p3b_reader','p3b.parent','SELECT');
BEGIN;
ALTER ROLE p3b_reader RENAME TO p3b_renamed;
ROLLBACK;
SELECT to_regrole('p3b_reader') IS NOT NULL, to_regrole('p3b_renamed') IS NULL;
BEGIN;
CREATE ROLE p3b_rolled_back;
ROLLBACK;
SELECT to_regrole('p3b_rolled_back') IS NULL;
}, "t\nt\nf\nt|t\nt", 'role and GRANT/REVOKE changes are transactional');

sql_is(q{
CREATE SEQUENCE p3b.s START 10 CACHE 7;
BEGIN;
SELECT nextval('p3b.s');
SAVEPOINT s;
SELECT nextval('p3b.s');
ROLLBACK TO s;
SELECT nextval('p3b.s');
COMMIT;
BEGIN;
ALTER SEQUENCE p3b.s RESTART 100 CACHE 3;
SELECT nextval('p3b.s');
ROLLBACK;
SELECT seqcache FROM pg_sequence WHERE seqrelid='p3b.s'::regclass;
BEGIN;
ALTER SEQUENCE p3b.s RESTART 200 CACHE 4;
COMMIT;
SELECT nextval('p3b.s');
BEGIN;
DROP SEQUENCE p3b.s;
ROLLBACK;
SELECT to_regclass('p3b.s') IS NOT NULL;
}, "10\n11\n12\n100\n7\n200\nt", 'sequence values are nontransactional; ALTER/RESTART/DROP remain transactional');

sql_is(q{
CREATE TABLE p3b.identity_table(id integer GENERATED ALWAYS AS IDENTITY, v text);
INSERT INTO p3b.identity_table(v) VALUES('a'),('b');
BEGIN;
TRUNCATE p3b.identity_table RESTART IDENTITY;
INSERT INTO p3b.identity_table(v) VALUES('temporary');
ROLLBACK;
SELECT string_agg(id::text,',' ORDER BY id) FROM p3b.identity_table;
TRUNCATE p3b.identity_table RESTART IDENTITY;
INSERT INTO p3b.identity_table(v) VALUES('new') RETURNING id;
}, "1,2\n1", 'identity and TRUNCATE internal rebuild preserve transaction semantics');

my $other = $node->background_psql('postgres');
$cached->query_safe('CREATE TEMP TABLE p3b_temp(n integer); INSERT INTO p3b_temp VALUES(1)');
$other->query_safe('CREATE TEMP TABLE p3b_temp(n integer); INSERT INTO p3b_temp VALUES(2)');
is($cached->query_safe('SELECT n FROM p3b_temp'), '1', 'first session owns its TEMP data');
is($other->query_safe('SELECT n FROM p3b_temp'), '2', 'same TEMP name is isolated in another session');
is($cached->query_safe(q{
BEGIN; CREATE TEMP TABLE p3b_temp_drop(n integer) ON COMMIT DROP; COMMIT;
SELECT to_regclass('pg_temp.p3b_temp_drop') IS NULL;
}), 't', 'TEMP ON COMMIT DROP honors the transaction');
$cached->quit;
$other->quit;

sql_is(q{
BEGIN;
DROP VIEW p3b.v;
DROP FUNCTION p3b.f();
DROP TABLE p3b.parent CASCADE;
ROLLBACK;
SELECT to_regclass('p3b.v') IS NOT NULL, to_regprocedure('p3b.f()') IS NOT NULL,
       to_regclass('p3b.parent') IS NOT NULL;
DROP SCHEMA p3b CASCADE;
DROP ROLE p3b_reader;
SELECT to_regnamespace('p3b') IS NULL;
}, "t|t|t\nt", 'DROP rollback restores dependencies and final DROP commits');

$node->stop('fast');
done_testing();
