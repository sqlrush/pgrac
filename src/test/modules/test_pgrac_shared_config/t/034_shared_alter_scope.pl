# Copyright (c) 2026, PostgreSQL Global Development Group
# PGRAC: classify rewrites before event triggers, catalog or sequence writes.
# Author: SqlRush <sqlrush@gmail.com>
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('alter_scope');
$node->init;
$node->start;
$node->safe_psql('postgres', q{
CREATE EXTENSION test_pgrac_shared_config;
CREATE TABLE scope_plain(id integer, v text, w varchar(8));
INSERT INTO scope_plain VALUES (1, 'a', 'b');
CREATE TABLE scope_identity(id integer GENERATED ALWAYS AS IDENTITY);
INSERT INTO scope_identity DEFAULT VALUES;
CREATE TABLE scope_parent(id integer);
CREATE TABLE scope_child(extra integer) INHERITS(scope_parent);
CREATE TABLE scope_partitioned(id integer, w varchar(8)) PARTITION BY RANGE(id);
CREATE TABLE scope_partition PARTITION OF scope_partitioned FOR VALUES FROM (0) TO (10);
CREATE TYPE scope_type AS (id integer);
CREATE TABLE scope_typed OF scope_type;
CREATE DOMAIN scope_domain AS integer CHECK (VALUE > 0);
CREATE DOMAIN scope_plain_domain AS integer;
CREATE FUNCTION scope_alter_poison() RETURNS event_trigger LANGUAGE plpgsql AS
$$BEGIN RAISE EXCEPTION 'scope rewrite reached execution'; END$$;
CREATE EVENT TRIGGER scope_alter_poison ON ddl_command_start
EXECUTE FUNCTION scope_alter_poison();
});
my $inventory = q{SELECT c.oid, c.relfilenode, a.attnum, a.attname, a.atttypid,
a.atttypmod, a.attisdropped FROM pg_class c JOIN pg_attribute a ON a.attrelid=c.oid
WHERE c.relname LIKE 'scope_%' AND a.attnum > 0 ORDER BY c.oid, a.attnum};
my $before = $node->safe_psql('postgres', $inventory);

for my $sql (
 'ALTER TABLE scope_plain ALTER COLUMN id TYPE bigint',
 'ALTER TABLE scope_plain ALTER COLUMN v TYPE varchar(4)',
 'ALTER TABLE scope_plain ALTER COLUMN id TYPE integer USING id + 1',
 'ALTER TABLE scope_identity ALTER COLUMN id TYPE bigint',
 'ALTER TABLE scope_plain ADD COLUMN vol double precision DEFAULT random()',
 'ALTER TABLE scope_plain ADD COLUMN ser serial',
 'ALTER TABLE scope_plain ADD COLUMN ident integer GENERATED ALWAYS AS IDENTITY',
 'ALTER TABLE scope_plain ADD COLUMN gen integer GENERATED ALWAYS AS (id + 1) STORED',
 'ALTER TABLE scope_plain ADD COLUMN dom scope_domain',
 'ALTER TABLE scope_plain ALTER COLUMN id SET STATISTICS 20, ADD COLUMN vol double precision DEFAULT random()',
 'ALTER TABLE scope_parent ALTER COLUMN id TYPE bigint',
 'ALTER TYPE scope_type ALTER ATTRIBUTE id TYPE bigint CASCADE',
 'ALTER TABLE scope_partitioned ADD COLUMN vol double precision DEFAULT random()',
 'ALTER TABLE scope_plain DROP COLUMN id, ADD COLUMN IF NOT EXISTS id integer DEFAULT random()',
 'ALTER TABLE scope_parent ADD COLUMN vol double precision DEFAULT random()')
{
 my ($out, $err);
 my $rc = $node->psql('postgres',
  "\\set VERBOSITY verbose\nSELECT test_pgrac_config_sql_mode(true);\n$sql;",
  stdout => \$out, stderr => \$err);
 isnt($rc, 0, "$sql refuses a shared rewrite");
 like($err, qr/0A000:.*table rewrite is not supported in shared mode/s,
  "$sql is classified before user DDL execution");
 unlike($err, qr/scope rewrite reached execution/,
  "$sql does not invoke an earlier DDL trigger");
 is($node->safe_psql('postgres', $inventory), $before,
  "$sql leaves relation, column and sequence identity untouched");
}

# A no-rewrite operation must reach the marker, not the shared scope refusal.
# The marker stops the fixture before an actual shared mutation; it does not
# pretend to establish multi-node DDL authority.
for my $sql (
 'ALTER TABLE scope_plain ALTER COLUMN id TYPE integer',
 'ALTER TABLE scope_plain ALTER COLUMN v TYPE varchar',
 'ALTER TABLE scope_plain ALTER COLUMN w TYPE varchar(32)',
 'ALTER TABLE scope_plain ADD COLUMN n integer',
 'ALTER TABLE scope_plain ADD COLUMN n integer DEFAULT 7',
 'ALTER TABLE scope_plain ADD COLUMN n timestamptz DEFAULT now()',
 'ALTER TABLE scope_plain ADD COLUMN n scope_plain_domain',
 'ALTER TABLE scope_plain ADD COLUMN IF NOT EXISTS id integer DEFAULT random()',
 'ALTER TABLE scope_plain ALTER COLUMN id SET STATISTICS 20',
 'ALTER TABLE scope_plain ALTER COLUMN v SET STORAGE MAIN',
 'ALTER TABLE scope_plain SET ACCESS METHOD heap',
 'ALTER TABLE scope_plain SET LOGGED',
 'ALTER TABLE scope_partitioned ALTER COLUMN w TYPE varchar(32)',
 'ALTER TYPE scope_type ALTER ATTRIBUTE id TYPE integer CASCADE',
 'ALTER TABLE scope_parent ALTER COLUMN id TYPE integer')
{
 my ($out, $err);
 my $rc = $node->psql('postgres',
  "\\set VERBOSITY verbose\nSELECT test_pgrac_config_sql_mode(true);\n$sql;",
  stdout => \$out, stderr => \$err);
 isnt($rc, 0, "$sql reaches the deliberately failing marker");
 like($err, qr/scope rewrite reached execution/,
  "$sql is not falsely classified as a rewrite");
}
$node->safe_psql('postgres', 'DROP EVENT TRIGGER scope_alter_poison');
# A DDL-start trigger may change a context-sensitive coercion after the first
# preflight. The actual executor must recheck before catalog/sequence work.
$node->safe_psql('postgres', q{
CREATE TABLE scope_clock(ts timestamp);
INSERT INTO scope_clock VALUES ('2026-01-01 12:00:00');
CREATE FUNCTION scope_change_timezone() RETURNS event_trigger LANGUAGE plpgsql AS
$$BEGIN PERFORM set_config('TimeZone', 'America/New_York', true); END$$;
CREATE EVENT TRIGGER scope_change_timezone ON ddl_command_start
WHEN TAG IN ('ALTER TABLE') EXECUTE FUNCTION scope_change_timezone();
});
my $clock_before = $node->safe_psql('postgres', q{
SELECT c.relfilenode, a.atttypid FROM pg_class c JOIN pg_attribute a ON a.attrelid=c.oid
WHERE c.oid='scope_clock'::regclass AND a.attname='ts'});
my ($clock_out, $clock_err);
my $clock_rc = $node->psql('postgres', q{
\set VERBOSITY verbose
SET TimeZone='UTC';
SELECT test_pgrac_config_sql_mode(true);
ALTER TABLE scope_clock ALTER COLUMN ts TYPE timestamptz;
}, stdout => \$clock_out, stderr => \$clock_err);
isnt($clock_rc, 0, 'trigger cannot turn a classified no-rewrite into an executed rewrite');
like($clock_err, qr/0A000:.*table rewrite is not supported in shared mode/s,
 'changed coercion context is rejected before native catalog/storage mutation');
$node->safe_psql('postgres', 'DROP EVENT TRIGGER scope_change_timezone');
is($node->safe_psql('postgres', q{
SELECT c.relfilenode, a.atttypid FROM pg_class c JOIN pg_attribute a ON a.attrelid=c.oid
WHERE c.oid='scope_clock'::regclass AND a.attname='ts'}), $clock_before,
 'trigger-induced refusal preserves type and storage identity');
$node->safe_psql('postgres', q{
ALTER TABLE scope_plain ALTER COLUMN id TYPE bigint;
ALTER TABLE scope_plain ADD COLUMN vol double precision DEFAULT random();
});
is($node->safe_psql('postgres', 'SELECT id FROM scope_plain'), '1',
 'native non-shared rewrites remain executable');
$node->stop('fast');
done_testing();
