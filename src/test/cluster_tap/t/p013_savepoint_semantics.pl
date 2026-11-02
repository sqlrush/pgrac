#!/usr/bin/env perl
# Copyright (c) 2026, pgrac contributors
# Author: SqlRush <sqlrush@gmail.com>
# Shared SAVEPOINT and PL/pgSQL exception semantics on the real cohort.
use strict;
use warnings;
use FindBin;
use lib "$FindBin::RealBin/../../perl";
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::ClusterPRE2;
use PostgreSQL::Test::Utils;
use Test::More;

sub rows
{
	my ($node, $table) = @_;
	return $node->safe_psql('postgres',
		"SELECT coalesce(string_agg(id::text || ':' || v, ',' ORDER BY id), '') FROM $table",
		timeout => 30);
}

sub all_rows
{
	my ($cluster, $table, $expected, $label) = @_;
	for my $node ($cluster->nodes)
	{
		is(rows($node, $table), $expected, $node->name . " $label");
	}
}

sub successful_sql
{
	my ($node, $sql, $label) = @_;
	my ($out, $err) = ('', '');
	my $rc = $node->psql('postgres', $sql, stdout => \$out, stderr => \$err,
		timeout => 30);
	is($rc, 0, $label) or BAIL_OUT("$label failed: $err");
	is($err, '', "$label has no unexpected SQL error");
	return $out;
}

# No adapter or success-producing hooks: native admission and OPEN are required.
my $cluster = PostgreSQL::Test::ClusterPRE2->new_cluster('p013_semantics',
	nodes => 4, blackbox => 1,
	extra_conf => [ 'autovacuum = off', 'cluster.trace_normal_stop = on',
		'cluster.cf_enqueue_timeout_ms = 30000' ]);
$cluster->start_cluster;
my @nodes = $cluster->nodes;
my $creator = $nodes[0];
$creator->safe_psql('postgres', q{
	CREATE TABLE p013_named (id int PRIMARY KEY, v text NOT NULL);
	INSERT INTO p013_named SELECT i, 'base' FROM generate_series(1,4) i;
	CREATE TABLE p013_dml (id int PRIMARY KEY, v text NOT NULL);
	CREATE TABLE p013_pl (id int PRIMARY KEY, v text NOT NULL);
	CREATE TABLE p013_triggered (id int PRIMARY KEY, v text NOT NULL);
	CREATE TABLE p013_audit (id int PRIMARY KEY, v text NOT NULL);
	CREATE FUNCTION p013_audit_insert() RETURNS trigger LANGUAGE plpgsql AS $$
	BEGIN
		INSERT INTO p013_audit VALUES (NEW.id, NEW.v);
		RETURN NEW;
	END $$;
	CREATE TRIGGER p013_insert AFTER INSERT ON p013_triggered
		FOR EACH ROW EXECUTE FUNCTION p013_audit_insert();
	CREATE TABLE p013_parent (id int PRIMARY KEY, v text NOT NULL);
	CREATE TABLE p013_child (id int PRIMARY KEY, parent_id int
		REFERENCES p013_parent(id) ON DELETE CASCADE, v text NOT NULL);
	INSERT INTO p013_parent VALUES (1,'one'), (2,'two');
	INSERT INTO p013_child VALUES (11,1,'eleven'), (22,2,'twenty-two');
});

# Every origin writes a real child, rolls it back, and keeps its top alive.
my @parents;
for my $i (0 .. 3)
{
	my $id = $i + 1;
	my $parent = $nodes[$i]->background_psql('postgres', timeout => 30);
	$parent->query_safe("BEGIN; UPDATE p013_named SET v='keep' WHERE id=$id;");
	$parent->query_safe("SAVEPOINT keep_point; UPDATE p013_named SET v='discard' WHERE id=$id;");
	my $identity = $parent->query_safe("SELECT pg_current_xact_id_if_assigned()::text,
		xmin::text, xmin::text::numeric <>
		(pg_current_xact_id_if_assigned()::text::numeric % 4294967296)
		FROM p013_named WHERE id=$id");
	like($identity, qr/^\d+\|\d+\|t$/, "node$i assigned a distinct native child xid");
	diag("node$i top|child|distinct=$identity");
	$parent->query_safe('ROLLBACK TO SAVEPOINT keep_point');
	is($parent->query_safe("SELECT v FROM p013_named WHERE id=$id"),
		'keep', "node$i sees its surviving parent value");
	push @parents, $parent;
}
$creator->safe_psql('postgres', 'CHECKPOINT', timeout => 30);
all_rows($cluster, 'p013_named', '1:base,2:base,3:base,4:base',
	'keeps active parents invisible through checkpoint');
for my $parent (@parents)
{
	$parent->query_safe('COMMIT');
	$parent->quit;
}
all_rows($cluster, 'p013_named', '1:keep,2:keep,3:keep,4:keep',
	'sees only surviving committed values');

# INSERT/batch/COPY/UPDATE/DELETE are all in the same rollback scope.
my $out = successful_sql($nodes[1], <<'SQL', 'DML rollback and parent abort');
BEGIN;
INSERT INTO p013_dml VALUES (0,'parent');
SAVEPOINT mutate;
INSERT INTO p013_dml VALUES (1,'one'), (2,'two');
COPY p013_dml FROM STDIN WITH (FORMAT csv);
3,copy-three
4,copy-four
\.
UPDATE p013_dml SET v='child' WHERE id=0;
DELETE FROM p013_dml WHERE id=1;
SELECT 'MUTATE=' || string_agg(id || ':' || v, ',' ORDER BY id) FROM p013_dml;
ROLLBACK TO mutate;
SELECT 'RESTORE=' || string_agg(id || ':' || v, ',' ORDER BY id) FROM p013_dml;
INSERT INTO p013_dml VALUES (5,'released');
RELEASE mutate;
ROLLBACK;
SQL
like($out, qr/^MUTATE=0:child,2:two,3:copy-three,4:copy-four$/m,
	'all child DML ran before rollback');
like($out, qr/^RESTORE=0:parent$/m, 'rollback restores exact earlier keys and values');
all_rows($cluster, 'p013_dml', '', 'parent abort also removes RELEASEd effects');

successful_sql($nodes[2], q{
	BEGIN;
	INSERT INTO p013_dml VALUES (10,'parent');
	SAVEPOINT outer_point;
	INSERT INTO p013_dml VALUES (11,'outer');
	SAVEPOINT inner_point;
	INSERT INTO p013_dml VALUES (12,'inner');
	RELEASE inner_point;
	ROLLBACK TO outer_point;
	INSERT INTO p013_dml VALUES (13,'first-retry');
	ROLLBACK TO outer_point;
	INSERT INTO p013_dml VALUES (14,'last-retry');
	RELEASE outer_point;
	COMMIT;
}, 'nested RELEASE and repeated rollback-to');
all_rows($cluster, 'p013_dml', '10:parent,14:last-retry',
	'keeps only effects outside both rollback ranges');

# An old same-name point must never reappear after releasing its replacement.
my ($same_out, $same_err) = ('', '');
my $same_rc = $nodes[3]->psql('postgres', q{
	BEGIN; SAVEPOINT same_name; SAVEPOINT same_name;
	RELEASE same_name; ROLLBACK TO same_name; ROLLBACK;
}, on_error_stop => 0, stdout => \$same_out, stderr => \$same_err,
	extra_params => [ '-v', 'VERBOSITY=verbose' ], timeout => 30);
is($same_rc, 0, 'same-name error returns normally to psql');
my @same_errors = $same_err =~ /^.*ERROR:.*$/mg;
is(scalar(@same_errors), 1, 'exactly the missing replacement point fails');
like($same_err, qr/ERROR:\s+3B001:/, 'same-name replacement uses invalid-savepoint SQLSTATE');

successful_sql($nodes[3], q{
	BEGIN;
	INSERT INTO p013_pl VALUES (0,'parent');
	DO $$ BEGIN
		INSERT INTO p013_pl VALUES (1,'success');
	EXCEPTION WHEN division_by_zero THEN RAISE;
	END $$;
	DO $$ DECLARE seen int := 0; BEGIN
		BEGIN
			INSERT INTO p013_pl VALUES (2,'discard');
			seen := 17;
			PERFORM 1/0;
		EXCEPTION WHEN division_by_zero THEN
			INSERT INTO p013_pl VALUES (3,'handler-' || seen || '-' || SQLSTATE);
		END;
	END $$;
	DO $$ BEGIN
		BEGIN
			INSERT INTO p013_pl VALUES (4,'outer-discard');
			BEGIN
				INSERT INTO p013_pl VALUES (5,'inner-discard');
				RAISE unique_violation;
			EXCEPTION WHEN unique_violation THEN
				INSERT INTO p013_pl VALUES (6,'handler-discard');
				RAISE division_by_zero;
			END;
		EXCEPTION WHEN division_by_zero THEN
			INSERT INTO p013_pl VALUES (7,'outer-handler-' || SQLSTATE);
		END;
	END $$;
	DO $$ BEGIN
		INSERT INTO p013_pl VALUES (8,'unique-discard');
		INSERT INTO p013_pl VALUES (1,'duplicate');
	EXCEPTION WHEN unique_violation THEN
		INSERT INTO p013_pl VALUES (9,'unique-' || SQLSTATE);
	END $$;
	DO $$ BEGIN
		INSERT INTO p013_pl VALUES (10,'fk-discard');
		INSERT INTO p013_child VALUES (99,999,'invalid');
	EXCEPTION WHEN foreign_key_violation THEN
		INSERT INTO p013_pl VALUES (11,'fk-' || SQLSTATE);
	END $$;
	COMMIT;
}, 'PL success, error handlers, variables and nested handler errors');
all_rows($cluster, 'p013_pl',
	'0:parent,1:success,3:handler-17-22012,7:outer-handler-22012,9:unique-23505,11:fk-23503',
	'preserves exact PL values and original handled SQLSTATEs');
all_rows($cluster, 'p013_child', '11:eleven,22:twenty-two',
	'keeps earlier FK rows and discards the rejected insert');

successful_sql($nodes[0], q{
	BEGIN;
	INSERT INTO p013_triggered VALUES (0,'parent');
	SAVEPOINT trigger_point;
	INSERT INTO p013_triggered VALUES (1,'discard');
	ROLLBACK TO trigger_point;
	INSERT INTO p013_triggered VALUES (2,'keep');
	RELEASE trigger_point;
	COMMIT;
	BEGIN; SAVEPOINT abort_point;
	INSERT INTO p013_triggered VALUES (3,'parent-abort');
	RELEASE abort_point; ROLLBACK;
}, 'trigger SPI effects share their original scope');
all_rows($cluster, 'p013_triggered', '0:parent,2:keep', 'keeps exact trigger source rows');
all_rows($cluster, 'p013_audit', '0:parent,2:keep', 'keeps exact trigger side effects');

successful_sql($nodes[1], q{
	BEGIN; SAVEPOINT cascade_point;
	DELETE FROM p013_parent WHERE id=1;
	ROLLBACK TO cascade_point;
	DELETE FROM p013_parent WHERE id=2;
	RELEASE cascade_point; COMMIT;
}, 'RI cascade rollback and release');
all_rows($cluster, 'p013_parent', '1:one', 'restores the first cascade parent');
all_rows($cluster, 'p013_child', '11:eleven', 'restores the first cascade child');

my ($retry_out, $retry_err) = ('', '');
my $retry_rc = $nodes[2]->psql('postgres', q{
	BEGIN;
	INSERT INTO p013_dml VALUES (20,'before-error');
	INSERT INTO p013_dml VALUES (20,'duplicate');
	INSERT INTO p013_dml VALUES (21,'after-error');
	COMMIT;
}, on_error_stop => 0, stdout => \$retry_out, stderr => \$retry_err,
	extra_params => [ '-v', 'ON_ERROR_ROLLBACK=on', '-v', 'VERBOSITY=verbose' ], timeout => 30);
is($retry_rc, 0, 'psql automatic savepoint permits the transaction to continue');
my @retry_errors = $retry_err =~ /^.*ERROR:.*$/mg;
is(scalar(@retry_errors), 1, 'automatic rollback reports exactly one intended error');
like($retry_err, qr/ERROR:\s+23505:/, 'automatic rollback preserves unique-violation SQLSTATE');
all_rows($cluster, 'p013_dml', '10:parent,14:last-retry,20:before-error,21:after-error',
	'commits earlier and later statements around automatic rollback');

# Use the original normal-stop proof and restart these same DATA directories.
$cluster->stop_cluster;
$cluster->restart_cluster;
all_rows($cluster, 'p013_named', '1:keep,2:keep,3:keep,4:keep', 'retains savepoint results after restart');
all_rows($cluster, 'p013_dml', '10:parent,14:last-retry,20:before-error,21:after-error',
	'retains nested and automatic rollback results after restart');
all_rows($cluster, 'p013_pl',
	'0:parent,1:success,3:handler-17-22012,7:outer-handler-22012,9:unique-23505,11:fk-23503',
	'retains PL results after restart');
all_rows($cluster, 'p013_triggered', '0:parent,2:keep', 'retains trigger source rows after restart');
all_rows($cluster, 'p013_audit', '0:parent,2:keep', 'retains trigger results after restart');
all_rows($cluster, 'p013_parent', '1:one', 'retains cascade parent after restart');
all_rows($cluster, 'p013_child', '11:eleven', 'retains cascade child after restart');
$cluster->stop_cluster;
for my $node (@nodes)
{
	unlike(slurp_file($node->logfile), qr/TRAP:|PANIC:|terminated by signal/,
		$node->name . ' has no assertion, PANIC or signal termination');
}
done_testing();
