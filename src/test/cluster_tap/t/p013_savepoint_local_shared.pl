#!/usr/bin/env perl
# Copyright (c) 2026, pgrac contributors
# Author: SqlRush <sqlrush@gmail.com>
# Keep local temp and shared effects in their original transaction scopes.
use strict;
use warnings;
use FindBin;
use lib "$FindBin::RealBin/../../perl";
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::ClusterPRE2;
use PostgreSQL::Test::Utils;
use Test::More;

sub shared_rows
{
	my ($table) = @_;
	return "SELECT coalesce(string_agg(id::text || ':' || v || ':' || tag, chr(10) ORDER BY id),'') FROM $table";
}

sub temp_rows
{
	return q{SELECT string_agg(id::text || ':' || v, chr(10) ORDER BY id) FROM pg_temp.p013_scope_temp};
}

sub identity_sql
{
	my ($table) = @_;
	return "SELECT oid::text || ':' || relpersistence || ':' || pg_relation_filepath(oid) FROM pg_class WHERE oid='$table'::regclass";
}

sub shared_state
{
	my ($nodes, $table, $expected, $identity, $label) = @_;
	for my $node (@$nodes)
	{
		is($node->safe_psql('postgres', shared_rows($table), timeout => 30),
			$expected, $node->name . " $label exact shared rows");
		is($node->safe_psql('postgres', identity_sql($table), timeout => 30),
			$identity, $node->name . " $label original relation identity");
	}
}

my $cluster = PostgreSQL::Test::ClusterPRE2->new_cluster('p013_local_shared',
	nodes => 4, blackbox => 1,
	extra_conf => [ 'autovacuum = off', 'cluster.trace_normal_stop = on',
		'cluster.cf_enqueue_timeout_ms = 30000' ]);
$cluster->start_cluster;
my @nodes = $cluster->nodes;
my (@sessions, @identities);
for my $i (0 .. 3)
{
	my $s = $nodes[$i]->background_psql('postgres', timeout => 180);
	$s->set_query_timer_restart;
	$s->query_safe("CREATE TEMP TABLE p013_scope_temp(id int PRIMARY KEY,v text NOT NULL) ON COMMIT PRESERVE ROWS;
		INSERT INTO p013_scope_temp VALUES (0,'origin-$i');");
	is($s->query_safe(q{SELECT relpersistence || ':' || (relnamespace=pg_my_temp_schema())::text
		|| ':' || (pg_relation_filepath(oid) ~ '/t[0-9]+_[0-9]+$')::text
		FROM pg_class WHERE oid='pg_temp.p013_scope_temp'::regclass}),
		't:true:true', "node$i uses its own native temp relation");
	push @sessions, $s;
}

for my $i (0 .. 3)
{
	my $s = $sessions[$i];
	my $table = "p013_scope_shared_$i";
	$s->query_safe("BEGIN;
		CREATE TABLE $table(id int PRIMARY KEY,v text NOT NULL);
		ALTER TABLE $table ADD COLUMN tag text NOT NULL DEFAULT 'from-top';
		CREATE INDEX p013_scope_value_$i ON $table(v);
		INSERT INTO $table(id,v) VALUES (1,'parent');
		INSERT INTO p013_scope_temp VALUES (1,'parent');
		SAVEPOINT outer_point;
		UPDATE $table SET v='child' WHERE id=1;
		INSERT INTO $table(id,v) VALUES (2,'child');
		UPDATE p013_scope_temp SET v='child' WHERE id=1;
		INSERT INTO p013_scope_temp VALUES (2,'child');");
	my $witness = $s->query_safe("SELECT pg_current_xact_id_if_assigned()::text || '|' || xmin::text || '|' ||
		(xmin::text::numeric <> pg_current_xact_id_if_assigned()::text::numeric % 4294967296)::text
		FROM $table WHERE id=2");
	like($witness, qr/^\d+\|\d+\|true$/, "node$i shared child really ran under a distinct child xid");
	diag("node$i shared top|child|distinct=$witness");
	$s->query_safe("SAVEPOINT inner_point;
		DELETE FROM $table WHERE id=1;
		INSERT INTO $table(id,v) VALUES (3,'grandchild');
		DELETE FROM p013_scope_temp WHERE id=0;
		INSERT INTO p013_scope_temp VALUES (3,'grandchild');
		RELEASE inner_point;
		ROLLBACK TO outer_point;");
	is($s->query_safe(shared_rows($table)), '1:parent:from-top', "node$i restores data in its new top-level table");
	is($s->query_safe(temp_rows()), "0:origin-$i\n1:parent", "node$i restores only its local scope");
	is($s->query_safe("SELECT string_agg(attname::text || ':' || format_type(atttypid,atttypmod),',' ORDER BY attnum)
		FROM pg_attribute WHERE attrelid='$table'::regclass AND attnum>0 AND NOT attisdropped"),
		'id:integer,v:text,tag:text', "node$i retains top-level ALTER metadata");
	is($s->query_safe("SELECT count(*) FROM pg_index WHERE indrelid='$table'::regclass AND indisvalid"),
		'2', "node$i retains its top-level primary and secondary indexes");
	my $identity = $s->query_safe(identity_sql($table));
	like($identity, qr/^\d+:p:.+$/, "node$i top-level relation is permanent");
	push @identities, $identity;
	$s->query_safe("INSERT INTO $table(id,v) VALUES (4,'after');
		INSERT INTO p013_scope_temp VALUES (4,'after'); RELEASE outer_point; COMMIT;");
	shared_state(\@nodes, $table, "1:parent:from-top\n4:after:from-top", $identity, "origin$i commit");
	is($s->query_safe(temp_rows()), "0:origin-$i\n1:parent\n4:after", "node$i preserves local rows on commit");

	$s->query_safe("BEGIN;
		CREATE TABLE p013_scope_aborted_$i(v text);
		INSERT INTO p013_scope_aborted_$i VALUES ('top-created');
		SAVEPOINT merge_point;
		UPDATE $table SET v='abort-child' WHERE id=1;
		INSERT INTO $table(id,v) VALUES (8,'abort-child');
		DELETE FROM p013_scope_temp WHERE id=0;
		INSERT INTO p013_scope_temp VALUES (8,'abort-child');
		RELEASE merge_point; ROLLBACK;");
	shared_state(\@nodes, $table, "1:parent:from-top\n4:after:from-top", $identity, "origin$i parent abort");
	is($s->query_safe(temp_rows()), "0:origin-$i\n1:parent\n4:after", "node$i parent abort also removes released local effects");
	for my $node (@nodes)
	{
		is($node->safe_psql('postgres', "SELECT to_regclass('p013_scope_aborted_$i') IS NULL", timeout => 30),
			't', $node->name . " origin$i parent abort removes top-created relation");
	}

	$s->query_safe(qq{DO \$\$ BEGIN
		UPDATE $table SET v='exception-child' WHERE id=1;
		INSERT INTO $table(id,v) VALUES (5,'exception-child');
		UPDATE p013_scope_temp SET v='exception-child' WHERE id=1;
		INSERT INTO p013_scope_temp VALUES (5,'exception-child');
		RAISE EXCEPTION 'discard both domains';
	EXCEPTION WHEN raise_exception THEN
		INSERT INTO $table(id,v) VALUES (6,'handler');
		INSERT INTO p013_scope_temp VALUES (6,'handler');
	END; \$\$;});
	shared_state(\@nodes, $table, "1:parent:from-top\n4:after:from-top\n6:handler:from-top", $identity, "origin$i handler");
	is($s->query_safe(temp_rows()), "0:origin-$i\n1:parent\n4:after\n6:handler", "node$i exception discards both child domains and keeps the handler");
}

$nodes[0]->safe_psql('postgres', 'CHECKPOINT', timeout => 30);
for my $i (0 .. 3)
{
	is($sessions[$i]->query_safe(temp_rows()), "0:origin-$i\n1:parent\n4:after\n6:handler",
		"node$i same-name live temp table remains private through all origins and checkpoint");
	$sessions[$i]->quit;
}
$cluster->stop_cluster;
$cluster->restart_cluster;
for my $i (0 .. 3)
{
	shared_state(\@nodes, "p013_scope_shared_$i", "1:parent:from-top\n4:after:from-top\n6:handler:from-top",
		$identities[$i], "origin$i same-DATA restart");
	for my $node (@nodes)
	{
		is($node->safe_psql('postgres', "SELECT to_regclass('p013_scope_aborted_$i') IS NULL", timeout => 30),
			't', $node->name . " origin$i aborted relation stays absent after restart");
	}
}
for my $node (@nodes)
{
	is($node->safe_psql('postgres', q{SELECT to_regclass('pg_temp.p013_scope_temp') IS NULL}, timeout => 30),
		't', $node->name . " has no previous session's temp name");
}
$cluster->stop_cluster;
for my $node (@nodes)
{
	unlike(slurp_file($node->logfile), qr/TRAP:|PANIC:|terminated by signal|does not hold buffer slot/,
		$node->name . ' has no assertion, PANIC, signal termination or bad unlock');
}
done_testing();
