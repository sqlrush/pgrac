#!/usr/bin/env perl
# Copyright (c) 2026, pgrac contributors
# Author: SqlRush <sqlrush@gmail.com>
# Native storage paths must preserve the same rollback range.
use strict;
use warnings;
use FindBin;
use lib "$FindBin::RealBin/../../perl";
use Digest::MD5 qw(md5_hex);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::ClusterPRE2;
use PostgreSQL::Test::Utils;
use Test::More;

sub expected_row
{
	my ($id, $key, $value) = @_;
	return join('|', $id, $key, length($value), md5_hex($value));
}

sub all_access_paths
{
	my ($cluster, $table, $expected, $label) = @_;
	for my $node ($cluster->nodes)
	{
		for my $access ('heap', 'index')
		{
			my $settings = $access eq 'heap'
			  ? 'SET LOCAL enable_seqscan=on; SET LOCAL enable_indexscan=off;'
			  : 'SET LOCAL enable_seqscan=off; SET LOCAL enable_indexscan=on;';
			$settings .= 'SET LOCAL enable_bitmapscan=off; SET LOCAL enable_indexonlyscan=off;';
			my $query = "SELECT id, k, octet_length(v), md5(v) FROM $table"
			  . ' WHERE k IS NOT NULL ORDER BY k';
			my $plan = $node->safe_psql('postgres',
				"BEGIN; $settings EXPLAIN (COSTS OFF) $query; ROLLBACK;", timeout => 30);
			my $relation = qr/\Q$table\E(?:_(?:low|high))?\b/;
			my $index = $table eq 'p013_storage_route'
			  ? qr/p013_storage_route_(?:low|high)_k_idx/
			  : qr/\Q${table}_k_key\E/;
			like($plan, $access eq 'heap'
				? qr/Seq Scan on $relation/ : qr/Index Scan using $index on $relation/,
				$node->name . " $label uses $access access");
			is($node->safe_psql('postgres', "BEGIN; $settings $query; ROLLBACK;",
				timeout => 30), $expected, $node->name . " $label exact $access rows");
		}
	}
}

sub update_probe
{
	my ($session, $table, $update, $kind) = @_;
	my $probe = "SELECT ctid, pg_stat_get_xact_tuples_hot_updated('$table'::regclass),"
	  . " pg_stat_get_xact_tuples_newpage_updated('$table'::regclass) FROM $table WHERE id=1";
	my $before = $session->query_safe($probe);
	my ($old_block, $old_offset, $old_hot, $old_newpage) =
	  $before =~ /^\((\d+),(\d+)\)\|(\d+)\|(\d+)$/;
	defined($old_block) or BAIL_OUT("invalid pre-update witness: $before");
	is($session->query_safe("$update RETURNING id"), '1', "$kind updates the intended row");
	my $after = $session->query_safe($probe);
	my ($new_block, $new_offset, $new_hot, $new_newpage) =
	  $after =~ /^\((\d+),(\d+)\)\|(\d+)\|(\d+)$/;
	defined($new_block) or BAIL_OUT("invalid post-update witness: $after");
	isnt("$new_block,$new_offset", "$old_block,$old_offset", "$kind installs a new tuple version");
	is($new_hot - $old_hot, $kind eq 'HOT' ? 1 : 0, "$kind has the exact HOT counter delta");
	is($new_newpage - $old_newpage, $kind eq 'cross-page' ? 1 : 0,
		"$kind has the exact new-page counter delta");
	if ($kind eq 'cross-page')
	{
		isnt($new_block, $old_block, 'cross-page update actually changes heap block');
	}
	else
	{
		is($new_block, $old_block, "$kind stays on the original heap block");
	}
	is($session->query_safe("SELECT xmin::text::numeric <>"
		. ' (pg_current_xact_id_if_assigned()::text::numeric % 4294967296)'
		. " FROM $table WHERE id=1"), 't', "$kind uses a distinct child xid");
	diag("$kind ctid/counters before=$before after=$after");
}

my $cluster = PostgreSQL::Test::ClusterPRE2->new_cluster('p013_storage',
	nodes => 4, blackbox => 1,
	extra_conf => [ 'autovacuum = off', 'track_counts = on',
		'cluster.trace_normal_stop = on', 'cluster.cf_enqueue_timeout_ms = 30000' ]);
$cluster->start_cluster;
my @nodes = $cluster->nodes;
my $creator = $nodes[0];
# Deterministic ASCII payloads have independent Perl length/hash expectations.
my $toast_base = join('', map { md5_hex("base:$_") } 1 .. 384);
my $toast_keep = join('', map { md5_hex("keep:$_") } 1 .. 384);
my $toast_discard = join('', map { md5_hex("discard:$_") } 1 .. 512);
$creator->safe_psql('postgres', qq{
	CREATE TABLE p013_storage_hot (id int PRIMARY KEY, k int UNIQUE NOT NULL,
		v text NOT NULL) WITH (fillfactor=20);
	ALTER TABLE p013_storage_hot ALTER COLUMN v SET STORAGE PLAIN;
	INSERT INTO p013_storage_hot VALUES (1,1,repeat('b',128));
	CREATE TABLE p013_storage_cross (id int PRIMARY KEY, k int UNIQUE NOT NULL,
		v text NOT NULL) WITH (fillfactor=100);
	ALTER TABLE p013_storage_cross ALTER COLUMN v SET STORAGE PLAIN;
	INSERT INTO p013_storage_cross SELECT i,i,repeat('b',190) FROM generate_series(1,256) i;
	CREATE TABLE p013_storage_toast (id int PRIMARY KEY, k int UNIQUE NOT NULL, v text NOT NULL);
	ALTER TABLE p013_storage_toast ALTER COLUMN v SET STORAGE EXTERNAL;
	INSERT INTO p013_storage_toast VALUES (1,1,'$toast_base');
	CREATE TABLE p013_storage_route (id int PRIMARY KEY, k int NOT NULL, v text NOT NULL)
		PARTITION BY RANGE(id);
	CREATE TABLE p013_storage_route_low PARTITION OF p013_storage_route FOR VALUES FROM (1) TO (100);
	CREATE TABLE p013_storage_route_high PARTITION OF p013_storage_route FOR VALUES FROM (100) TO (200);
	CREATE INDEX p013_storage_route_k ON p013_storage_route(k);
	INSERT INTO p013_storage_route VALUES (1,1,'parent');
}, timeout => 30);

my $hot = $nodes[0]->background_psql('postgres', timeout => 30);
$hot->query_safe("BEGIN; UPDATE p013_storage_hot SET v=repeat('p',128) WHERE id=1;");
$hot->query_safe('SAVEPOINT hot_point');
update_probe($hot, 'p013_storage_hot',
	"UPDATE p013_storage_hot SET v=repeat('x',128) WHERE id=1", 'HOT');
$hot->query_safe('ROLLBACK TO hot_point');
is($hot->query_safe('SELECT md5(v) FROM p013_storage_hot WHERE id=1'), md5_hex('p' x 128),
	'HOT rollback restores the earlier parent value');
update_probe($hot, 'p013_storage_hot',
	'UPDATE p013_storage_hot SET k=101 WHERE id=1', 'non-HOT');
$hot->query_safe('ROLLBACK TO hot_point');
all_access_paths($cluster, 'p013_storage_hot', expected_row(1,1,'b' x 128),
	'active parent and discarded HOT/non-HOT versions');
$hot->query_safe("INSERT INTO p013_storage_hot VALUES (2,101,'key-reused'); RELEASE hot_point; COMMIT;");
$hot->quit;
my $hot_expected = join("\n", expected_row(1,1,'p' x 128), expected_row(2,101,'key-reused'));
all_access_paths($cluster, 'p013_storage_hot', $hot_expected, 'HOT/non-HOT surviving versions');

my $cross = $nodes[1]->background_psql('postgres', timeout => 30);
$cross->query_safe('BEGIN; SAVEPOINT cross_point');
update_probe($cross, 'p013_storage_cross',
	"UPDATE p013_storage_cross SET k=1001,v=repeat('x',3000) WHERE id=1", 'cross-page');
my $cross_base = join("\n", map { expected_row($_,$_,'b' x 190) } 1 .. 256);
all_access_paths($cluster, 'p013_storage_cross', $cross_base, 'uncommitted cross-page version');
$cross->query_safe('ROLLBACK TO cross_point');
is($cross->query_safe('SELECT k,octet_length(v),md5(v) FROM p013_storage_cross WHERE id=1'),
	join('|',1,190,md5_hex('b' x 190)), 'cross-page rollback restores the complete old row');
$cross->query_safe("INSERT INTO p013_storage_cross VALUES (257,1001,'key-reused'); RELEASE cross_point; COMMIT;");
$cross->query_safe("BEGIN; SAVEPOINT parent_abort;
	INSERT INTO p013_storage_cross VALUES (258,1002,repeat('z',3000)); RELEASE parent_abort; ROLLBACK;");
$cross->quit;
my $cross_expected = $cross_base . "\n" . expected_row(257,1001,'key-reused');
all_access_paths($cluster, 'p013_storage_cross', $cross_expected, 'cross-page rollback and released parent abort');

my $toast_relation = $creator->safe_psql('postgres', q{
	SELECT reltoastrelid::regclass::text FROM pg_class WHERE oid='p013_storage_toast'::regclass
}, timeout => 30);
$toast_relation =~ /^pg_toast\.pg_toast_\d+$/
	 or BAIL_OUT("unexpected TOAST relation: $toast_relation");
cmp_ok($creator->safe_psql('postgres', "SELECT count(*) FROM $toast_relation", timeout => 30),
	'>', 1, 'the original large value occupies real external TOAST chunks');
my $toast = $nodes[2]->background_psql('postgres', timeout => 30);
$toast->query_safe("BEGIN; UPDATE p013_storage_toast SET v='$toast_keep' WHERE id=1;
	SAVEPOINT toast_point; UPDATE p013_storage_toast SET k=101,v='$toast_discard' WHERE id=1;");
is($toast->query_safe('SELECT octet_length(v),md5(v) FROM p013_storage_toast WHERE id=1'),
	join('|',length($toast_discard),md5_hex($toast_discard)), 'child writes the intended full large value');
cmp_ok($toast->query_safe("SELECT count(*) FROM $toast_relation"), '>', 1,
	'the child large value also uses external chunks');
all_access_paths($cluster, 'p013_storage_toast', expected_row(1,1,$toast_base),
	'active parent and child TOAST versions');
$toast->query_safe('ROLLBACK TO toast_point');
is($toast->query_safe('SELECT octet_length(v),md5(v) FROM p013_storage_toast WHERE id=1'),
	join('|',length($toast_keep),md5_hex($toast_keep)), 'TOAST rollback restores the exact parent payload');
$toast->query_safe("INSERT INTO p013_storage_toast VALUES (2,101,'key-reused'); RELEASE toast_point; COMMIT;
	BEGIN; SAVEPOINT delete_point; DELETE FROM p013_storage_toast WHERE id=1;
	RELEASE delete_point; ROLLBACK;");
$toast->quit;
my $toast_expected = join("\n", expected_row(1,1,$toast_keep), expected_row(2,101,'key-reused'));
all_access_paths($cluster, 'p013_storage_toast', $toast_expected, 'TOAST rollback and released delete abort');

my $route = $nodes[3]->background_psql('postgres', timeout => 30);
$route->query_safe("BEGIN; SAVEPOINT route_point;
	UPDATE p013_storage_route SET id=101,k=101,v='discard' WHERE id=1;");
is($route->query_safe('SELECT tableoid::regclass::text,id,k,v FROM p013_storage_route'),
	'p013_storage_route_high|101|101|discard', 'child UPDATE actually routes to the other partition');
$route->query_safe('ROLLBACK TO route_point');
is($route->query_safe('SELECT tableoid::regclass::text,id,k,v FROM p013_storage_route'),
	'p013_storage_route_low|1|1|parent', 'rollback restores the original physical partition');
$route->query_safe("UPDATE p013_storage_route SET id=102,k=102,v='keep' WHERE id=1;
	RELEASE route_point; COMMIT;
	BEGIN; SAVEPOINT return_point; UPDATE p013_storage_route SET id=2,k=2,v='parent-abort' WHERE id=102;
	RELEASE return_point; ROLLBACK;");
$route->quit;
my $route_expected = expected_row(102,102,'keep');
all_access_paths($cluster, 'p013_storage_route', $route_expected, 'released partition move and parent abort');
for my $node (@nodes)
{
	is($node->safe_psql('postgres', 'SELECT tableoid::regclass::text FROM p013_storage_route',
		timeout => 30), 'p013_storage_route_high', $node->name . ' retains the committed destination');
}

$creator->safe_psql('postgres', 'CHECKPOINT', timeout => 30);
$cluster->stop_cluster;
$cluster->restart_cluster;
all_access_paths($cluster, 'p013_storage_hot', $hot_expected, 'HOT/non-HOT after same-DATA restart');
all_access_paths($cluster, 'p013_storage_cross', $cross_expected, 'cross-page after same-DATA restart');
all_access_paths($cluster, 'p013_storage_toast', $toast_expected, 'TOAST after same-DATA restart');
all_access_paths($cluster, 'p013_storage_route', $route_expected, 'partition move after same-DATA restart');
$cluster->stop_cluster;
for my $node (@nodes)
{
	unlike(slurp_file($node->logfile), qr/TRAP:|PANIC:|terminated by signal/,
		$node->name . ' has no assertion, PANIC or signal termination');
}
done_testing();
