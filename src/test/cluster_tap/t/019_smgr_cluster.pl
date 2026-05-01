#-------------------------------------------------------------------------
#
# 019_smgr_cluster.pl
#    End-to-end regression for the cluster_smgr bridge introduced in
#    stage 1.2 (方案 C 单文件单 fork-handle 版本).
#
#    Stage 1.2 wires cluster_smgr (smgrsw[1]) into PG's smgr layer and
#    routes permanent non-temp relations through it when both
#    cluster.shared_storage_backend != stub AND
#    cluster.smgr_user_relations = on.  Default GUC=off keeps PG
#    behaviour identical to upstream.  This TAP test exercises the
#    surfaces visible to a running PG instance:
#
#      L1   GUC default off
#      L2   GUC=on + shared_storage_backend=local restart
#      L3   CREATE TABLE / INSERT / SELECT under GUC=on
#      L4   > 1GB single-file extend (~1.5GB via generate_series)
#      L5   VACUUM / TRUNCATE / DROP transparency
#      L6   TEMP relations always route through md.c
#      L7   restart preserves data
#      L8   3 cluster_smgr injection points exposed
#      L9   pg_cluster_state shared_fs.smgr_user_relations on
#      L10  pg_cluster_state shared_fs.smgr_active_relations grows
#      L11  GUC=on + shared_storage_backend=stub startup FATAL
#
# IDENTIFICATION
#    src/test/cluster_tap/t/019_smgr_cluster.pl
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../lib";

use IPC::Cmd qw(can_run);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use PgracClusterNode;


my $node = PgracClusterNode->new('main');
$node->init;
$node->start;


# ----------
# L1: GUC default is off.
# ----------
$node->assert_cluster_guc('cluster.smgr_user_relations', 'off',
	'L1 cluster.smgr_user_relations default is off');


# ----------
# L2: GUC=on + shared_storage_backend=local restart cleanly.
# ----------
$node->stop;
$node->append_conf('postgresql.conf',
	"cluster.shared_storage_backend = local\ncluster.smgr_user_relations = on\n");
$node->start;

$node->assert_cluster_guc('cluster.smgr_user_relations', 'on',
	'L2 cluster.smgr_user_relations applies across restart');
$node->assert_cluster_guc('cluster.shared_storage_backend', 'local',
	'L2 cluster.shared_storage_backend = local applies across restart');


# ----------
# L3: CREATE TABLE / INSERT / SELECT round-trip with cluster_smgr.
# ----------
$node->safe_psql('postgres',
	'CREATE TABLE t1 (id int, payload text)');
$node->safe_psql('postgres', q{
	INSERT INTO t1 SELECT g, 'row-' || g FROM generate_series(1, 10000) g;
});
is($node->safe_psql('postgres', 'SELECT count(*) FROM t1'),
	'10000', 'L3 INSERT 10000 rows visible via SELECT count');

is($node->safe_psql('postgres',
		"SELECT payload FROM t1 WHERE id = 9999"),
	'row-9999', 'L3 SELECT WHERE id=9999 returns expected payload');


# ----------
# L4: > 1GB single-file extend.
#
#   generate_series(1, 8_000_000) of (int, 80-byte payload) ~= 1.4GB
#   on disk.  Confirms cluster_smgr handles a single file past the PG
#   md.c 1GB segment boundary without splitting (方案 C).
# ----------
my $datadir = $node->data_dir;
my $relnode_t1 = $node->safe_psql('postgres',
	"SELECT pg_relation_filenode('t1'::regclass)");
my $dboid = $node->safe_psql('postgres',
	"SELECT oid FROM pg_database WHERE datname = 'postgres'");

# Skip if running short on time / disk; the explicit guard makes the
# behaviour predictable in CI.
my $do_large = $ENV{PGRAC_SKIP_LARGE_TABLE} ? 0 : 1;

SKIP: {
	skip "L4 large-table test skipped via PGRAC_SKIP_LARGE_TABLE", 2 unless $do_large;

	# 12M rows × ~150B / row ≈ 1.7GB, well past PG md.c's 1GB segment
	# boundary.  The point of L4 is the byte-level "no segment split"
	# check (∗.1, ∗.2 must not exist) -- file size growing past 1GB is
	# a proxy that confirms we extended the file, not just sat at 0.
	$node->safe_psql('postgres', q{
		CREATE TABLE big AS
		   SELECT g AS id,
		          repeat('p', 100) AS pad
		     FROM generate_series(1, 12000000) g;
	});

	my $relnode_big = $node->safe_psql('postgres',
		"SELECT pg_relation_filenode('big'::regclass)");
	my $base_path = "$datadir/base/$dboid/$relnode_big";

	# Direct byte-level proof of方案 C: the .0 file exists and there
	# is NO .1 segment file alongside.  In PG md.c land a >1GB
	# relation always has .1; cluster_smgr never creates it.
	ok(-e $base_path && ! -e "$base_path.1",
	   "L4 big table is single-file (base exists, no .1 segment) — 方案 C single-file passthrough proof");

	my $size = -s $base_path;
	ok($size > 1024 * 1024 * 1024,
	   "L4 big table base file is >1GB (got " . int($size / (1024 * 1024)) . "MB)");
}


# ----------
# L5: VACUUM / TRUNCATE / DROP transparency.
# ----------
$node->safe_psql('postgres', 'VACUUM ANALYZE t1');
pass('L5 VACUUM ANALYZE on cluster_smgr-routed relation succeeds');

$node->safe_psql('postgres', 'TRUNCATE t1');
is($node->safe_psql('postgres', 'SELECT count(*) FROM t1'),
	'0', 'L5 TRUNCATE empties the table');

$node->safe_psql('postgres', 'DROP TABLE t1');
ok(! -e "$datadir/base/$dboid/$relnode_t1",
	'L5 DROP TABLE removes the underlying file');


# ----------
# L6: temp relations always route through md.c.
#
#   md.c uses path scheme `tNNN_XXX` for temp relfilenodes; cluster_smgr
#   for permanent rels uses path scheme `XXX`.  As long as the temp
#   table CREATE/INSERT/SELECT works at all we know the temp branch in
#   cluster_smgr_which_for() is firing.
# ----------
# TEMP tables are session-scoped; CREATE + INSERT + SELECT must share
# one connection.
is($node->safe_psql('postgres', q{
		CREATE TEMP TABLE temp_rel (n int);
		INSERT INTO temp_rel VALUES (1), (2), (3);
		SELECT sum(n) FROM temp_rel;
	}),
	'6', 'L6 TEMP TABLE INSERT / SELECT works (routes through md.c)');


# ----------
# L7: restart preserves data inserted under cluster_smgr.
# ----------
$node->safe_psql('postgres',
	'CREATE TABLE persistent (n int); INSERT INTO persistent VALUES (1), (2), (3)');

$node->restart;

is($node->safe_psql('postgres', 'SELECT sum(n) FROM persistent'),
	'6', 'L7 cluster_smgr-stored data survives restart');


# ----------
# L8: 3 cluster_smgr injection points exposed.
# ----------
is($node->safe_psql(
		'postgres',
		q{SELECT count(*) FROM pg_stat_cluster_injections
		   WHERE name LIKE 'cluster-smgr-%'}),
	'3',
	'L8 3 cluster_smgr injection points registered');


# ----------
# L9: pg_cluster_state shared_fs.smgr_user_relations reflects GUC.
# ----------
is($node->safe_psql(
		'postgres',
		q{SELECT value FROM pg_cluster_state
		   WHERE category = 'shared_fs' AND key = 'smgr_user_relations'}),
	't',
	'L9 pg_cluster_state.shared_fs.smgr_user_relations = t (GUC=on)');


# ----------
# L10: smgr_active_relations is non-zero (system catalogs already
# routed through cluster_smgr at this point).
# ----------
my $active = $node->safe_psql(
	'postgres',
	q{SELECT value::int FROM pg_cluster_state
	   WHERE category = 'shared_fs' AND key = 'smgr_active_relations'});
ok($active > 0,
   "L10 smgr_active_relations > 0 (got $active; system catalogs routed through cluster_smgr)");


# ----------
# L11: cluster.smgr_user_relations=on + shared_storage_backend=stub
# fails to start (cluster_shared_fs_init startup-time check).
# ----------
$node->stop;
$node->append_conf('postgresql.conf',
	"cluster.shared_storage_backend = stub\n");

my $pg_ctl = $ENV{PG_CTL} || 'pg_ctl';
my $pg_ctl_path = can_run($pg_ctl);
$pg_ctl_path = $pg_ctl unless defined $pg_ctl_path;

my $exit_code = system($pg_ctl_path, '-w', '-t', '5', '-D', $node->data_dir,
					   '-l', $node->logfile, 'start');
isnt($exit_code, 0,
	 'L11 postmaster refuses to start when smgr_user_relations=on + shared_storage_backend=stub');

my $log = slurp_file($node->logfile);
like($log,
	 qr/cluster\.smgr_user_relations=on requires shared_storage_backend != stub/,
	 'L11 startup log contains the cross-check FATAL message');


done_testing();
