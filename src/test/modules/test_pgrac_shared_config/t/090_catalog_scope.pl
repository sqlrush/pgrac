# Copyright (c) 2026, PostgreSQL Global Development Group
# PGRAC: shared SQL scope must reject before executing DDL callbacks.
# Author: SqlRush <sqlrush@gmail.com>
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# This selects the shared SQL guard in an isolated native server. It does
# not manufacture shared storage, membership, or distributed lock grants.
my $node = PostgreSQL::Test::Cluster->new('catalog_scope');
$node->init;
$node->append_conf('postgresql.conf', 'max_prepared_transactions = 4');
$node->start;
$node->safe_psql('postgres', q{
CREATE EXTENSION test_pgrac_shared_config;
CREATE TABLE scope_heap(id integer);
INSERT INTO scope_heap VALUES (1), (2);
CREATE INDEX scope_index ON scope_heap(id);
CREATE TABLE scope_parent(id integer) PARTITION BY RANGE(id);
CREATE TABLE scope_child(id integer);
CREATE TABLE scope_inherit(id integer);
CREATE TYPE scope_type AS (n integer);
CREATE TYPE scope_enum AS ENUM ('one');
CREATE DOMAIN scope_domain AS integer;
CREATE MATERIALIZED VIEW scope_mv AS SELECT * FROM scope_heap;
CREATE FUNCTION scope_ddl_poison() RETURNS event_trigger LANGUAGE plpgsql AS
$$BEGIN RAISE EXCEPTION 'scope guard reached DDL trigger'; END$$;
CREATE EVENT TRIGGER scope_poison ON ddl_command_start EXECUTE FUNCTION scope_ddl_poison();
});

for my $sql (
    q{PREPARE TRANSACTION 'scope_prepared'},
    q{COMMIT PREPARED 'scope_prepared'},
    q{ROLLBACK PREPARED 'scope_prepared'},
    'VACUUM FULL pg_class',
    'CREATE TYPE scope_new AS (n integer)',
    q{CREATE TYPE scope_new AS ENUM ('one')},
    'CREATE TYPE scope_new AS RANGE (subtype = integer)',
    'CREATE TYPE scope_new',
    'CREATE DOMAIN scope_new AS integer',
    'ALTER TYPE scope_type ADD ATTRIBUTE m integer',
    q{ALTER TYPE scope_enum ADD VALUE 'two'},
    'ALTER TYPE scope_type RENAME TO scope_new',
    'ALTER TYPE scope_type RENAME ATTRIBUTE n TO m',
    'ALTER TYPE scope_type SET SCHEMA pg_catalog',
    'ALTER TYPE scope_type OWNER TO CURRENT_USER',
    'ALTER DOMAIN scope_domain SET NOT NULL',
    'ALTER DOMAIN scope_domain RENAME CONSTRAINT old_constraint TO new_constraint',
    'DROP TYPE scope_type',
    'DROP DOMAIN scope_domain',
    'CREATE OPERATOR === (LEFTARG = integer, RIGHTARG = integer, FUNCTION = int4eq)',
    'CREATE OPERATOR CLASS scope_ops FOR TYPE integer USING btree AS OPERATOR 1 <',
    'CREATE OPERATOR FAMILY scope_ops USING btree',
    'ALTER OPERATOR FAMILY scope_ops USING btree ADD OPERATOR 1 < (integer, integer)',
    'ALTER OPERATOR === (integer, integer) SET (RESTRICT = eqsel)',
    'DROP OPERATOR === (integer, integer)',
    'DROP OPERATOR CLASS scope_ops USING btree',
    'DROP OPERATOR FAMILY scope_ops USING btree',
    'DROP EXTENSION plpgsql CASCADE',
    'CREATE TABLE scope_new (id integer) INHERITS (scope_inherit)',
    'CREATE TABLE scope_new PARTITION OF scope_parent FOR VALUES FROM (0) TO (10)',
    'ALTER TABLE scope_parent ATTACH PARTITION scope_child FOR VALUES FROM (0) TO (10)',
    'ALTER TABLE scope_parent DETACH PARTITION scope_child',
    'ALTER TABLE scope_parent DETACH PARTITION scope_child CONCURRENTLY',
    'ALTER TABLE scope_parent DETACH PARTITION scope_child FINALIZE',
    'ALTER TABLE scope_child INHERIT scope_inherit',
    'ALTER TABLE scope_child NO INHERIT scope_inherit',
    'ALTER TABLE scope_child ADD COLUMN added integer, INHERIT scope_inherit',
    'ALTER MATERIALIZED VIEW scope_mv RENAME COLUMN id TO renamed',
    'DROP MATERIALIZED VIEW scope_mv',
    'CREATE SCHEMA scope_nested CREATE TABLE nested (id integer) INHERITS (scope_inherit)',
    'CREATE SCHEMA scope_nested CREATE INDEX nested ON scope_heap USING hash(id)',
    'DROP INDEX CONCURRENTLY scope_index',
    'REINDEX INDEX scope_index',
    'CREATE INDEX CONCURRENTLY scope_new ON scope_heap(id)')
{
    my ($out, $err);
    my $rc = $node->psql('postgres',
        "\\set VERBOSITY verbose\nSELECT test_pgrac_config_sql_mode(true);\n$sql;",
        stdout => \$out, stderr => \$err);
    isnt($rc, 0, "$sql refuses shared mode");
    like($err, qr/0A000:.*(?:not supported in shared mode|supports only btree)/s,
        "$sql rejects before DDL callback or object lookup");
    unlike($err, qr/scope guard reached DDL trigger/,
        "$sql does not execute the DDL trigger");
    if ($sql =~ /CONCURRENTLY scope_(?:index|new)|^REINDEX/)
    {
        like($err, qr/HINT:.*DROP INDEX \+ CREATE INDEX/,
            'index refusal recommends the supported replacement');
    }
}
$node->safe_psql('postgres', 'DROP EVENT TRIGGER scope_poison');
# Native 2PC remains available; shared completion must refuse before changing
# a prepared transaction's state, including the rollback branch.
$node->safe_psql('postgres', q{
BEGIN; INSERT INTO scope_heap VALUES (3); PREPARE TRANSACTION 'scope_prepared';
});
for my $command ('COMMIT', 'ROLLBACK')
{
    my ($out, $err);
    my $rc = $node->psql('postgres',
        "\\set VERBOSITY verbose\nSELECT test_pgrac_config_sql_mode(true);\n"
        . "$command PREPARED 'scope_prepared';",
        stdout => \$out, stderr => \$err);
    isnt($rc, 0, "shared $command PREPARED refuses an existing transaction");
    like($err, qr/0A000:.*two-phase transactions.*not supported in shared mode/s,
        'shared 2PC refusal precedes prepared transaction completion');
    is($node->safe_psql('postgres', q{
SELECT count(*) FROM pg_prepared_xacts WHERE gid='scope_prepared'}), '1',
        'prepared transaction remains available to the native owner');
}
$node->safe_psql('postgres', q{ROLLBACK PREPARED 'scope_prepared'});
$node->safe_psql('postgres', q{
BEGIN; PREPARE TRANSACTION 'scope_native'; COMMIT PREPARED 'scope_native';
SELECT test_pgrac_config_sql_mode(true);
BEGIN; SAVEPOINT s; ROLLBACK TO s; RELEASE s; COMMIT;
BEGIN; ROLLBACK;
PREPARE scope_query AS SELECT 1; EXECUTE scope_query; DEALLOCATE scope_query;
});
is($node->safe_psql('postgres', q{
SELECT count(*) FROM pg_attribute
WHERE attrelid='scope_child'::regclass AND attname='added'}), '0',
    'mixed ALTER leaves earlier subcommands unexecuted');
is($node->safe_psql('postgres', q{
SELECT to_regnamespace('scope_nested') IS NULL}), 't',
    'nested unsupported DDL leaves the schema uncreated');
is($node->safe_psql('postgres', q{SELECT count(*) FROM scope_heap}), '2',
    'refusals preserve existing rows');

# Scope guards do not change native non-shared support.
$node->safe_psql('postgres', q{
CREATE TYPE scope_native AS ENUM ('ok');
ALTER TABLE scope_child INHERIT scope_inherit;
ALTER TABLE scope_child NO INHERIT scope_inherit;
});
$node->safe_psql('postgres', 'DROP INDEX CONCURRENTLY scope_index');
is($node->safe_psql('postgres', q{
SELECT to_regclass('scope_index') IS NULL}), 't',
    'native concurrent index drop is preserved');
$node->stop('fast');
done_testing();
