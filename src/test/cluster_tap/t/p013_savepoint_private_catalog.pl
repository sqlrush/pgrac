#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# p013_savepoint_private_catalog.pl
#    Preserve private catalog state across child DML and native cleanup.
#
# Portions Copyright (c) 2026, pgrac contributors
# Author: SqlRush <sqlrush@gmail.com>
#
# IDENTIFICATION
#    src/test/cluster_tap/t/p013_savepoint_private_catalog.pl
#
# NOTES
#    Storage mode and private catalog routing are separate classifications.
#
#-------------------------------------------------------------------------
use strict;
use warnings;
use FindBin;
use lib "$FindBin::RealBin/../../perl";
use IPC::Run qw(run);
use Text::ParseWords qw(shellwords);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

sub rows_sql
{
	my ($table) = @_;
	my $tag = $table eq 'p013_private_data' ? " || ':' || tag" : '';
	return "SELECT coalesce(string_agg(id::text || ':' || v$tag, chr(10) ORDER BY id),'') FROM $table";
}

sub metadata_sql
{
	return q{
		SELECT 'REL:' || relname || ':' || oid || ':' || relkind::text || ':' || relpersistence::text || ':' || pg_relation_filepath(oid)
		FROM pg_class WHERE oid IN ('p013_private_data'::regclass,
			'p013_private_data_pkey'::regclass,'p013_private_value_idx'::regclass)
		UNION ALL SELECT 'ATTR:' || attnum || ':' || attname || ':' || format_type(atttypid,atttypmod) || ':' || attnotnull
		FROM pg_attribute WHERE attrelid='p013_private_data'::regclass AND attnum>0 AND NOT attisdropped
		UNION ALL SELECT 'INDEX:' || pg_get_indexdef(indexrelid)
		FROM pg_index WHERE indrelid='p013_private_data'::regclass
		UNION ALL SELECT 'DEFAULT:' || pg_get_expr(adbin,adrelid)
		FROM pg_attrdef WHERE adrelid='p013_private_data'::regclass
		ORDER BY 1
	};
}

sub local_file
{
	my ($node, $session, $relation, $temp, $label) = @_;
	my $path = $session->query_safe("SELECT pg_relation_filepath('$relation'::regclass)");
	like($path, $temp ? qr{^base/\d+/t\d+_\d+$} : qr{^base/\d+/\d+$},
		"$label has the expected local file identity");
	ok(-f ($node->data_dir . "/$path"), "$label file exists under this node's PGDATA");
}

my ($configure, $error) = ('', '');
run(['pg_config', '--configure'], '>', \$configure, '2>', \$error)
	 or BAIL_OUT("cannot identify the tested build: $error");
my $cluster_build = 0;
for my $option (shellwords($configure))
{
	$cluster_build = 1 if $option eq '--enable-cluster' || $option eq '--enable-cluster=yes';
	$cluster_build = 0 if $option eq '--disable-cluster' || $option eq '--enable-cluster=no';
}
diag("tested configure options: $configure");
my @profiles = $cluster_build ? ('runtime_off', 'private_catalog') : ('compiled_out');

for my $profile (@profiles)
{
	my $node = PostgreSQL::Test::Cluster->new("p013_$profile");
	$node->init;
	$node->append_conf('postgresql.conf', "autovacuum = off\nlog_error_verbosity = verbose\n");
	if ($cluster_build)
	{
		my $enabled = $profile eq 'private_catalog' ? 'on' : 'off';
		$node->append_conf('postgresql.conf', "cluster.enabled = $enabled\ncluster.node_id = 0\n"
			. "cluster.allow_single_node = on\ncluster.shared_catalog = off\ncluster.shared_config = off\n"
			. "cluster.shared_storage_backend = stub\ncluster.smgr_user_relations = off\n");
	}
	$node->start;
	my $s = $node->background_psql('postgres', timeout => 180);
	$s->set_query_timer_restart;
	if ($cluster_build)
	{
		is($s->query_safe(q{SELECT current_setting('cluster.enabled') || '|' ||
			current_setting('cluster.shared_catalog') || '|' || current_setting('cluster.shared_config') || '|' ||
			current_setting('cluster.shared_storage_backend') || '|' || current_setting('cluster.smgr_user_relations') || '|' ||
			current_setting('cluster.node_id')}),
			($profile eq 'private_catalog' ? 'on' : 'off') . '|off|off|stub|off|0',
			"$profile verifies the actual private storage configuration");
	}
	else
	{
		is($s->query_safe(q{SELECT current_setting('cluster.enabled', true) IS NULL}),
			't', "$profile has no compiled cluster setting");
	}
	local_file($node, $s, 'pg_catalog.pg_class', 0, "$profile catalog");
	$s->query_safe(q{
		CREATE TEMP TABLE p013_private_temp(id int PRIMARY KEY,v text NOT NULL) ON COMMIT PRESERVE ROWS;
		INSERT INTO p013_private_temp VALUES (0,'origin');
		BEGIN;
		CREATE TABLE p013_private_data(id int PRIMARY KEY,v text NOT NULL);
		ALTER TABLE p013_private_data ADD COLUMN tag text NOT NULL DEFAULT 'from-top';
		CREATE INDEX p013_private_value_idx ON p013_private_data(v);
		INSERT INTO p013_private_data(id,v) VALUES (1,'parent'),(5,'five');
		INSERT INTO p013_private_temp VALUES (1,'parent'),(5,'five');
	});
	my $metadata = $s->query_safe(metadata_sql());
	like($metadata, qr/^ATTR:3:tag:text:true$/m, "$profile really applied the top-level ALTER");
	local_file($node, $s, 'p013_private_data', 0, "$profile permanent relation");
	local_file($node, $s, 'pg_temp.p013_private_temp', 1, "$profile temporary relation");
	$s->query_safe(q{
		SAVEPOINT outer_point;
		UPDATE p013_private_data SET v='child' WHERE id=1;
		DELETE FROM p013_private_data WHERE id=5;
		INSERT INTO p013_private_data(id,v) VALUES (2,'child');
		UPDATE p013_private_temp SET v='child' WHERE id=1;
		DELETE FROM p013_private_temp WHERE id=5;
		INSERT INTO p013_private_temp VALUES (2,'child');
	});
	my $witness = $s->query_safe(q{SELECT pg_current_xact_id_if_assigned()::text || '|' || xmin::text || '|' ||
		(xmin::text::numeric <> pg_current_xact_id_if_assigned()::text::numeric % 4294967296)::text
		FROM p013_private_data WHERE id=2});
	like($witness, qr/^\d+\|\d+\|true$/, "$profile really executed permanent DML under a child xid");
	diag("$profile top|child|distinct=$witness");
	is($s->query_safe(rows_sql('p013_private_data')), "1:child:from-top\n2:child:from-top", "$profile all three child DML forms took effect");
	$s->query_safe(q{
		SAVEPOINT inner_point;
		INSERT INTO p013_private_data(id,v) VALUES (3,'grandchild');
		DELETE FROM p013_private_temp WHERE id=0;
		RELEASE inner_point;
		ROLLBACK TO outer_point;
		ROLLBACK TO outer_point;
	});
	is($s->query_safe(rows_sql('p013_private_data')), "1:parent:from-top\n5:five:from-top", "$profile repeated rollback restores parent rows");
	is($s->query_safe(rows_sql('pg_temp.p013_private_temp')), "0:origin\n1:parent\n5:five", "$profile native temp cleanup retains parent rows");
	is($s->query_safe(metadata_sql()), $metadata, "$profile child rollback retains all top-level catalog identities");
	$s->query_safe(q{
		INSERT INTO p013_private_data(id,v) VALUES (4,'after');
		INSERT INTO p013_private_temp VALUES (4,'after');
		RELEASE outer_point;
		COMMIT;
	});
	my $kept = "1:parent:from-top\n4:after:from-top\n5:five:from-top";
	is($node->safe_psql('postgres', rows_sql('p013_private_data'), timeout => 30), $kept,
		"$profile a fresh backend sees only committed surviving data");
	is($node->safe_psql('postgres', metadata_sql(), timeout => 30), $metadata,
		"$profile committed top-level metadata is unchanged");
	$s->query_safe(q{
		BEGIN;
		CREATE TABLE p013_private_aborted(v text);
		INSERT INTO p013_private_aborted VALUES ('top-created');
		SAVEPOINT merge_point;
		UPDATE p013_private_data SET v='abort-child' WHERE id=1;
		INSERT INTO p013_private_data(id,v) VALUES (8,'abort-child');
		INSERT INTO p013_private_temp VALUES (8,'abort-child');
		RELEASE merge_point;
		ROLLBACK;
	});
	is($s->query_safe(rows_sql('p013_private_data')), $kept, "$profile parent abort removes RELEASEd effects");
	is($node->safe_psql('postgres', q{SELECT to_regclass('p013_private_aborted') IS NULL}, timeout => 30),
		't', "$profile parent abort removes its own catalog creation");
	$s->query_safe(q{
		DO $$ DECLARE progress int := 0; message text; BEGIN
			UPDATE p013_private_data SET v='exception-child' WHERE id=1;
			DELETE FROM p013_private_data WHERE id=5;
			INSERT INTO p013_private_data(id,v) VALUES (9,'exception-child');
			INSERT INTO p013_private_temp VALUES (9,'exception-child');
			progress := 1;
			RAISE EXCEPTION 'p013 intentional child error';
		EXCEPTION WHEN raise_exception THEN
			GET STACKED DIAGNOSTICS message=MESSAGE_TEXT;
			IF progress <> 1 OR message <> 'p013 intentional child error' THEN
				RAISE EXCEPTION 'p013 unexpected earlier error';
			END IF;
			INSERT INTO p013_private_data(id,v) VALUES (6,'handler');
			INSERT INTO p013_private_temp VALUES (6,'handler');
		END $$;
	});
	$kept .= "\n6:handler:from-top";
	is($s->query_safe(rows_sql('p013_private_data')), $kept, "$profile implicit savepoint keeps only the handler");
	is($s->query_safe(rows_sql('pg_temp.p013_private_temp')), "0:origin\n1:parent\n4:after\n5:five\n6:handler",
		"$profile parent abort and exception do not leak local child rows");
	is($s->query_safe(metadata_sql()), $metadata, "$profile handler leaves catalog metadata intact");
	$s->quit;
	is($node->safe_psql('postgres', q{SELECT count(*) FROM pg_class WHERE relname='p013_private_temp'}, timeout => 30),
		'0', "$profile quitting removes the temp catalog row");
	$node->safe_psql('postgres', 'CHECKPOINT', timeout => 30);
	$node->restart;
	is($node->safe_psql('postgres', rows_sql('p013_private_data'), timeout => 30), $kept, "$profile same-DATA restart retains exact rows");
	is($node->safe_psql('postgres', metadata_sql(), timeout => 30), $metadata, "$profile same-DATA restart retains catalog identities");
	is($node->safe_psql('postgres', q{SELECT to_regclass('p013_private_aborted') IS NULL}, timeout => 30),
		't', "$profile aborted catalog creation stays absent after restart");
	$node->stop;
	unlike(slurp_file($node->logfile), qr/TRAP:|PANIC:|terminated by signal|does not hold buffer slot/,
		"$profile has no assertion, PANIC, signal termination or bad unlock");
}
done_testing();
