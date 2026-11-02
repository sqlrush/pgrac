#!/usr/bin/env perl
# Copyright (c) 2026, pgrac contributors
# Author: SqlRush <sqlrush@gmail.com>
# Keep structural refusals and parent resources valid after child DML.
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
		"SELECT coalesce(string_agg(id::text || ':' || v, chr(10) ORDER BY id),'') FROM $table",
		timeout => 30);
}

sub metadata
{
	my ($node) = @_;
	return $node->safe_psql('postgres', q{
		SELECT 'REL:' || relname || ':' || oid || ':' || relkind || ':' || pg_relation_filepath(oid)
		FROM pg_class WHERE oid IN ('p013_boundary_target'::regclass,
			'p013_boundary_target_pkey'::regclass,'p013_boundary_sequence'::regclass)
		UNION ALL
		SELECT 'ATTR:' || attnum || ':' || attname || ':' || format_type(atttypid,atttypmod) || ':' || attnotnull
		FROM pg_attribute WHERE attrelid='p013_boundary_target'::regclass AND attnum>0 AND NOT attisdropped
		UNION ALL SELECT 'INDEX:' || pg_get_indexdef('p013_boundary_target_pkey'::regclass)
		UNION ALL SELECT 'SEQ:' || seqstart || ':' || seqincrement || ':' || seqcache
		FROM pg_sequence WHERE seqrelid='p013_boundary_sequence'::regclass
		UNION ALL SELECT 'SEQVAL:' || last_value || ':' || CASE WHEN is_called THEN 'called' ELSE 'uncalled' END
		FROM p013_boundary_sequence
		UNION ALL SELECT 'FORBIDDEN:' || count(*) FROM pg_class WHERE relname IN
			('p013_boundary_new','p013_boundary_serial','p013_boundary_serial_id_seq',
			 'p013_boundary_ctas','p013_boundary_new_idx','p013_boundary_pl')
		ORDER BY 1
	}, timeout => 30);
}

sub all_state
{
	my ($cluster, $expected_rows, $expected_metadata, $label) = @_;
	for my $node ($cluster->nodes)
	{
		is(rows($node, 'p013_boundary_effect'), $expected_rows, $node->name . " $label exact surviving effects");
		is(rows($node, 'p013_boundary_target'), "1:target-one\n2:target-two", $node->name . " $label target remains usable");
		is(rows($node, 'p013_boundary_cursor_data'), "11:eleven\n22:twenty-two\n33:thirty-three",
			$node->name . " $label cursor source remains intact");
		is(metadata($node), $expected_metadata, $node->name . " $label original catalog/storage identities");
	}
}

my $cluster = PostgreSQL::Test::ClusterPRE2->new_cluster('p013_boundary_cleanup',
	nodes => 4, blackbox => 1,
	extra_conf => [ 'autovacuum = off', 'cluster.trace_normal_stop = on',
		'cluster.cf_enqueue_timeout_ms = 30000' ]);
$cluster->start_cluster;
my @nodes = $cluster->nodes;
$nodes[0]->safe_psql('postgres', q{
	CREATE TABLE p013_boundary_effect (id int PRIMARY KEY, v text NOT NULL);
	CREATE TABLE p013_boundary_target (id int PRIMARY KEY, v text NOT NULL);
	INSERT INTO p013_boundary_target VALUES (1,'target-one'),(2,'target-two');
	CREATE TABLE p013_boundary_cursor_data (id int PRIMARY KEY, v text NOT NULL);
	INSERT INTO p013_boundary_cursor_data VALUES (11,'eleven'),(22,'twenty-two'),(33,'thirty-three');
	CREATE SEQUENCE p013_boundary_sequence START 41 CACHE 1;
	CREATE TABLE p013_boundary_diagnostics (state text, message text, hint text);
}, timeout => 30);
my $original_metadata = metadata($nodes[0]);
like($original_metadata, qr/^FORBIDDEN:0$/m, 'setup has no forbidden objects');
like($original_metadata, qr/^SEQ:41:1:1$/m, 'setup sequence has its original parameters');
like($original_metadata, qr/^SEQVAL:41:uncalled$/m, 'setup sequence is still uncalled');

my @cases = (
	[ 'create', 'CREATE TABLE p013_boundary_new(v int)' ],
	[ 'serial', 'CREATE TABLE p013_boundary_serial(id serial)' ],
	[ 'ctas', 'CREATE TABLE p013_boundary_ctas AS SELECT * FROM p013_boundary_target' ],
	[ 'index', 'CREATE INDEX p013_boundary_new_idx ON p013_boundary_target(v)' ],
	[ 'restart', 'ALTER SEQUENCE p013_boundary_sequence RESTART WITH 99' ],
	[ 'sequence-alter', 'ALTER SEQUENCE p013_boundary_sequence INCREMENT BY 2' ],
	[ 'truncate', 'TRUNCATE p013_boundary_target' ],
	[ 'drop', 'DROP TABLE p013_boundary_target' ],
	[ 'reindex', 'REINDEX TABLE p013_boundary_target' ],
	[ 'rewrite', 'ALTER TABLE p013_boundary_target ALTER COLUMN v TYPE varchar(80)' ],
	[ 'cluster', 'CLUSTER p013_boundary_target USING p013_boundary_target_pkey' ]
);
my @kept;
for my $i (0 .. $#cases)
{
	my ($label, $ddl) = @{$cases[$i]};
	my ($parent_id, $after_id, $child_id) = (100 + 2*$i, 101 + 2*$i, 10000 + $i);
	my ($out, $err) = ('','');
	my $rc = $nodes[$i % 4]->psql('postgres', qq{
		BEGIN;
		INSERT INTO p013_boundary_effect VALUES ($parent_id,'parent-$label');
		DECLARE parent_cursor NO SCROLL CURSOR FOR SELECT id FROM p013_boundary_cursor_data ORDER BY id;
		\\echo PARENT_FIRST
		FETCH NEXT FROM parent_cursor;
		\\echo PARENT_FIRST_END
		SAVEPOINT ddl_point;
		INSERT INTO p013_boundary_effect VALUES ($child_id,'discard-$label');
		\\echo CHILD_DML_STATE=:SQLSTATE
		DECLARE child_cursor NO SCROLL CURSOR FOR SELECT id FROM p013_boundary_cursor_data ORDER BY id;
		\\echo CHILD_FIRST
		FETCH NEXT FROM child_cursor;
		\\echo CHILD_FIRST_END
		$ddl;
		\\echo DDL_STATE=:SQLSTATE
		ROLLBACK TO ddl_point;
		\\echo CLEAN_ONE=:SQLSTATE
		ROLLBACK TO ddl_point;
		\\echo CLEAN_TWO=:SQLSTATE
		SELECT 'PORTALS=' || string_agg(name,',' ORDER BY name) FROM pg_cursors
			WHERE name IN ('parent_cursor','child_cursor');
		\\echo PARENT_NEXT
		FETCH NEXT FROM parent_cursor;
		\\echo PARENT_NEXT_END
		UPDATE p013_boundary_target SET v=v WHERE id=1;
		INSERT INTO p013_boundary_effect VALUES ($after_id,'after-$label');
		RELEASE ddl_point;
		COMMIT;
		\\echo END_STATE=:SQLSTATE
		SELECT 'PORTALS_AFTER=' || count(*) FROM pg_cursors WHERE name IN ('parent_cursor','child_cursor');
	}, on_error_stop => 0, stdout => \$out, stderr => \$err,
		extra_params => [ '-v', 'VERBOSITY=verbose' ], timeout => 30);
	is($rc, 0, "$label returns normally to psql") or BAIL_OUT("$label psql failed: $err");
	diag("$label native stdout=$out stderr=$err");
	my @errors = $err =~ /^.*ERROR:.*$/mg;
	is(scalar(@errors), 1, "$label has exactly one intended error");
	like($err, qr/ERROR:\s+0A000:.*shared relation storage .* does not support subtransactions/,
		"$label retains the original storage refusal");
	like($err, qr/HINT:.*top-level transaction.*PL\/pgSQL EXCEPTION/, "$label retains the actionable hint");
	like($err, qr/RelationCheckStorageTransaction/, "$label reaches the storage producer guard");
	like($out, qr/^CHILD_DML_STATE=00000$/m, "$label really executed child DML first");
	like($out, qr/^DDL_STATE=0A000$/m, "$label reports the exact DDL SQLSTATE");
	like($out, qr/^CLEAN_ONE=00000$/m, "$label first rollback cleanup succeeds");
	like($out, qr/^CLEAN_TWO=00000$/m, "$label repeated cleanup succeeds");
	like($out, qr/^PORTALS=parent_cursor$/m, "$label drops only the child cursor");
	like($out, qr/^PARENT_FIRST\n11\nPARENT_FIRST_END$/m, "$label parent cursor started normally");
	like($out, qr/^CHILD_FIRST\n11\nCHILD_FIRST_END$/m, "$label child cursor was actually used");
	like($out, qr/^PARENT_NEXT\n22\nPARENT_NEXT_END$/m, "$label parent cursor remains usable");
	like($out, qr/^END_STATE=00000$/m, "$label parent can commit later DML");
	like($out, qr/^PORTALS_AFTER=0$/m, "$label commit closes the remaining non-holdable cursor");
	push @kept, "$parent_id:parent-$label", "$after_id:after-$label";
	all_state($cluster, join("\n",@kept), $original_metadata, $label);
}

# A preceding child-DML refusal must never masquerade as a caught DDL refusal.
$nodes[2]->safe_psql('postgres', q{
	BEGIN;
	INSERT INTO p013_boundary_effect VALUES (200,'parent-pl');
	DO $$ DECLARE progress int := 0; state text; message text; hint text; BEGIN
		INSERT INTO p013_boundary_effect VALUES (10099,'discard-pl');
		progress := 1;
		CREATE TABLE p013_boundary_pl(v int);
		RAISE EXCEPTION 'DDL unexpectedly succeeded';
	EXCEPTION WHEN feature_not_supported THEN
		GET STACKED DIAGNOSTICS state=RETURNED_SQLSTATE,message=MESSAGE_TEXT,hint=PG_EXCEPTION_HINT;
		IF progress <> 1 OR state <> '0A000' OR message NOT LIKE 'shared relation storage % does not support subtransactions'
		THEN RAISE EXCEPTION 'wrong refusal or earlier DML failure'; END IF;
		INSERT INTO p013_boundary_diagnostics VALUES (state,message,hint);
		INSERT INTO p013_boundary_effect VALUES (201,'handler-' || state);
	END; $$;
	COMMIT;
}, timeout => 30);
push @kept, '200:parent-pl', '201:handler-0A000';
my $expected = join("\n",@kept);
all_state($cluster, $expected, $original_metadata, 'PL handler continuation');
for my $node (@nodes)
{
	is($node->safe_psql('postgres', q{SELECT state FROM p013_boundary_diagnostics}, timeout => 30),
		'0A000', $node->name . ' PL records the original DDL state');
	like($node->safe_psql('postgres', q{SELECT hint FROM p013_boundary_diagnostics}, timeout => 30),
		qr/top-level transaction.*PL\/pgSQL EXCEPTION/, $node->name . ' PL retains the original hint');
}
$nodes[0]->safe_psql('postgres', 'CHECKPOINT', timeout => 30);
$cluster->stop_cluster;
$cluster->restart_cluster;
all_state($cluster, $expected, $original_metadata, 'same-DATA restart');
$cluster->stop_cluster;
for my $node (@nodes)
{
	unlike(slurp_file($node->logfile), qr/TRAP:|PANIC:|terminated by signal|does not hold buffer slot/,
		$node->name . ' has no assertion, PANIC, signal termination or bad unlock');
}
done_testing();
