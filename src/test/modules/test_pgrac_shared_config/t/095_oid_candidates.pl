# Copyright (c) 2026, PostgreSQL Global Development Group
# Native OID collision boundaries; shared lease math has separate unit tests.
# Cross-node catalog visibility and SPACE lifetime need the shared cluster.
# Author: SqlRush <sqlrush@gmail.com>
use strict;
use warnings;
use File::Path qw(make_path);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('oid_candidates');
$node->init;
my $shared_root = $node->basedir . '/file_collision_root';
make_path($shared_root);
$node->append_conf('postgresql.conf', qq{
cluster.enabled=off
autovacuum=off
});
$node->start;
$node->safe_psql('postgres', q{
CREATE FUNCTION force_oid_candidate(oid) RETURNS void
 AS '$libdir/test_pgrac_shared_config', 'test_pgrac_set_next_oid' LANGUAGE C STRICT;
CREATE FUNCTION relfile_candidate() RETURNS oid
 AS '$libdir/test_pgrac_shared_config', 'test_pgrac_relfile_candidate' LANGUAGE C;
CREATE FUNCTION shared_relfile_candidate(text,oid DEFAULT 0) RETURNS oid
 AS '$libdir/test_pgrac_shared_config', 'test_pgrac_shared_relfile_candidate' LANGUAGE C;
CREATE FUNCTION cached_main_result(test_root text, filenumber oid) RETURNS text
LANGUAGE plpgsql AS $$
BEGIN
 PERFORM shared_relfile_candidate(test_root,filenumber);
 RETURN 'accepted';
EXCEPTION WHEN duplicate_file THEN RETURN SQLSTATE;
END
$$;
CREATE FUNCTION try_namespace_oid(candidate oid) RETURNS text LANGUAGE plpgsql AS $$
BEGIN
 INSERT INTO pg_namespace(oid,nspname,nspowner,nspacl)
 VALUES(candidate,'duplicate_candidate',(SELECT oid FROM pg_roles WHERE rolname=current_user),NULL);
 RETURN 'inserted';
EXCEPTION WHEN unique_violation THEN RETURN SQLSTATE;
END
$$;
CREATE TABLE candidate_result(result text);
CREATE TABLE original_file(v text);
INSERT INTO original_file VALUES('preserved');
CREATE SCHEMA committed_oid;
});

my $next_namespace = q{SELECT pg_nextoid('pg_namespace','oid','pg_namespace_oid_index')};
my $writer = $node->background_psql('postgres');
my $reader = $node->background_psql('postgres');
$writer->query_safe(q{SET statement_timeout='15s'});
$reader->query_safe(q{SET statement_timeout='15s'; SET application_name='oid_unique_wait'});

$reader->query_safe('SELECT force_oid_candidate(4294967295)');
is($reader->query_safe($next_namespace), '4294967295', 'native allocator emits the final candidate');
is($reader->query_safe($next_namespace), '16384', 'native wrap skips the reserved OID range');

my $committed = $reader->query_safe(q{SELECT oid FROM pg_namespace WHERE nspname='committed_oid'});
$reader->query_safe("SELECT force_oid_candidate($committed)");
cmp_ok($reader->query_safe($next_namespace), '>', $committed,
       'GetNewOidWithIndex skips an existing committed catalog OID');

$writer->query_safe('BEGIN; CREATE SCHEMA uncommitted_oid');
my $uncommitted = $writer->query_safe(q{SELECT oid FROM pg_namespace WHERE nspname='uncommitted_oid'});
is($reader->query_safe(q{SELECT count(*) FROM pg_namespace WHERE nspname='uncommitted_oid'}), '0',
   'ordinary snapshot cannot see the other transaction catalog row');
$reader->query_safe("SELECT force_oid_candidate($uncommitted)");
cmp_ok($reader->query_safe($next_namespace), '>', $uncommitted,
       'SnapshotAny collision check skips the uncommitted catalog OID');
$writer->query_safe('ROLLBACK');

# Two leases spanning a wrap can hand out the same not-yet-inserted candidate.
# The native unique index must wait for the first inserter and reject aliasing.
$reader->query_safe('SELECT force_oid_candidate(2000000000)');
my $first = $reader->query_safe($next_namespace);
$reader->query_safe('SELECT force_oid_candidate(2000000000)');
my $second = $reader->query_safe($next_namespace);
is($first, $second, 'two uninstalled candidates can coincide');
$writer->query_safe(qq{
BEGIN;
INSERT INTO pg_namespace(oid,nspname,nspowner,nspacl)
 VALUES($first,'first_candidate',(SELECT oid FROM pg_roles WHERE rolname=current_user),NULL);
});
$reader->query_until(qr/unique_submitted/, qq{\\echo unique_submitted
INSERT INTO candidate_result SELECT try_namespace_oid($second);
});
ok($node->poll_query_until('postgres', q{
SELECT EXISTS(SELECT 1 FROM pg_stat_activity
 WHERE application_name='oid_unique_wait' AND wait_event='transactionid')
}), 'duplicate candidate insertion waits for the conflicting transaction');
$writer->query_safe('COMMIT');
is($reader->query_safe('SELECT result FROM candidate_result'), '23505',
   'unique index rejects the duplicate after the first catalog row commits');
is($reader->query_safe("SELECT nspname FROM pg_namespace WHERE oid=$first"), 'first_candidate',
   'duplicate insertion does not replace the original catalog object');

my $filenum = $reader->query_safe(q{SELECT pg_relation_filenode('original_file')});
my $path = $node->data_dir . '/' . $reader->query_safe(q{SELECT pg_relation_filepath('original_file')});
$reader->query_safe('CHECKPOINT');
my $before = slurp_file($path);
$reader->query_safe("SELECT force_oid_candidate($filenum)");
cmp_ok($reader->query_safe('SELECT relfile_candidate()'), '>', $filenum,
       'GetNewRelFileNumber without pg_class skips an existing file');
is(slurp_file($path), $before, 'file collision probing preserves the original bytes');
is($reader->query_safe('SELECT v FROM original_file'), 'preserved',
   'original relation still resolves to its data');

# A file present only in the shared root must also exclude that candidate.
# This calls the real storage router in an otherwise native test instance.
my $dbid = $reader->query_safe(q{SELECT oid FROM pg_database WHERE datname=current_database()});
make_path("$shared_root/base/$dbid");
my $shared_path = "$shared_root/base/$dbid/3000000000";
append_to_file($shared_path, 'old shared relation');
my $space_path = "$shared_root/base/$dbid/3000000001_space";
append_to_file($space_path, 'retained SPACE identity');
my @orphan_paths = (
    "$shared_root/base/$dbid/3000000002_fsm",
    "$shared_root/base/$dbid/3000000003_vm",
    "$shared_root/base/$dbid/3000000004_init");
append_to_file($_, 'orphan fork must exclude the locator') for @orphan_paths;
$reader->query_safe('SELECT force_oid_candidate(3000000000)');
# Backend re-selection emits known duplicate-registration warnings.  Keep
# them in the server log; SQL ERRORs still fail query_safe normally.
$reader->query_safe("SET client_min_messages=error");
is($reader->query_safe("SELECT shared_relfile_candidate('$shared_root')"), '3000000005',
   'selected storage backend excludes every fork, including orphan FSM, VM and INIT');
is(slurp_file($shared_path), 'old shared relation', 'shared collision probe leaves old file intact');
is(slurp_file($space_path), 'retained SPACE identity', 'old SPACE file is untouched');
is(slurp_file($_), 'orphan fork must exclude the locator', 'orphan fork is untouched')
    for @orphan_paths;
is($reader->query_safe('SHOW cluster.smgr_user_relations'), 'off',
   'file probe restores native routing after the test');
my $cached_path = "$shared_root/base/$dbid/3000000005";
my $cached_bytes = 'c' x 8192;
append_to_file($cached_path, $cached_bytes);
is($reader->query_safe("SELECT cached_main_result('$shared_root',3000000005)"), '58P02',
   'shared CREATE rejects an already-open main file before registering new ownership');
is(slurp_file($cached_path), $cached_bytes, 'cached collision leaves original bytes intact');
is($reader->query_safe('SHOW cluster.shared_catalog'), 'off',
   'cached-create error restores native catalog mode');

$reader->quit;
$writer->quit;
$node->stop;
done_testing();
