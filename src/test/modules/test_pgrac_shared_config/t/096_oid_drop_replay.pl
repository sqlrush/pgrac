# Copyright (c) 2026, PostgreSQL Global Development Group
# Exercise DROP's real commit WAL, checkpointer and crash replay. Two native
# instances supply independent WAL histories; only the fixture file namespace
# is shared. This is not shared-cluster admission or merged-recovery coverage.
# Author: SqlRush <sqlrush@gmail.com>
use strict;
use warnings;
use File::Copy qw(copy);
use File::Path qw(make_path);
use IO::Handle;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $dropper = PostgreSQL::Test::Cluster->new('oid_dropper');
$dropper->init;
$dropper->append_conf('postgresql.conf', q{
cluster.enabled=off
cluster.shared_storage_backend=local
cluster.smgr_user_relations=on
autovacuum=off
checkpoint_timeout='1h'
max_wal_size='1GB'
});
$dropper->start;
$dropper->safe_psql('postgres', q{
CREATE TABLE dropped_file(v text);
INSERT INTO dropped_file VALUES ('old object');
CHECKPOINT;
});
my $old_number = $dropper->safe_psql('postgres',
    q{SELECT pg_relation_filenode('dropped_file')});
my $old_relative = $dropper->safe_psql('postgres',
    q{SELECT pg_relation_filepath('dropped_file')});
my $old_path = $dropper->data_dir . "/$old_relative";

my $allocator = PostgreSQL::Test::Cluster->new('oid_allocator');
$allocator->init;
$allocator->append_conf('postgresql.conf', "cluster.enabled=off\nautovacuum=off\n");
$allocator->start;
$allocator->safe_psql('postgres', q{
CREATE FUNCTION force_oid_candidate(oid) RETURNS void
 AS '$libdir/test_pgrac_shared_config', 'test_pgrac_set_next_oid' LANGUAGE C STRICT;
CREATE FUNCTION shared_relfile_candidate(text,oid DEFAULT 0) RETURNS oid
 AS '$libdir/test_pgrac_shared_config', 'test_pgrac_shared_relfile_candidate' LANGUAGE C;
CREATE TABLE surviving_file(v text);
INSERT INTO surviving_file VALUES ('other node committed this content');
CHECKPOINT;
});
my $source_path = $allocator->data_dir . '/' . $allocator->safe_psql('postgres',
    q{SELECT pg_relation_filepath('surviving_file')});
my $source_bytes = slurp_file($source_path);

$dropper->safe_psql('postgres', 'DROP TABLE dropped_file');
ok(-f $old_path && -s $old_path == 0,
    'committed DROP leaves a zero-length MAIN tombstone until its next checkpoint');
$allocator->safe_psql('postgres', 'CHECKPOINT');
ok(-f $old_path && -s $old_path == 0,
    'another instance checkpoint cannot retire the dropping instance tombstone');

my $shared_root = $dropper->data_dir;
my $new_number = $allocator->safe_psql('postgres', qq{
SET client_min_messages=error;
SELECT force_oid_candidate($old_number);
SELECT shared_relfile_candidate('$shared_root');
});
$new_number =~ s/^\s+|\s+$//g;
cmp_ok($new_number, '>', $old_number,
    'another allocator wrapping to the dropped locator must skip it');
my $new_relative = $old_relative;
$new_relative =~ s{\d+$}{$new_number};
my $new_path = $dropper->data_dir . "/$new_relative";
ok(!-e $new_path, 'selected candidate has no preexisting main file');
copy($source_path, $new_path) or die "copy survivor fixture: $!";
open(my $survivor, '+<', $new_path) or die "open survivor fixture: $!";
$survivor->sync or die "sync survivor fixture: $!";
close($survivor) or die "close survivor fixture: $!";

# B's bytes never appear in A's WAL. A must replay its committed DROP without
# deleting them. Before the fix the allocator chose old_number and replay
# removed this very file, even with wal_level=replica.
$dropper->stop('immediate');
$dropper->start;
ok(-f $new_path && slurp_file($new_path) eq $source_bytes,
    'dropper crash replay preserves the independently committed file bytes');
ok(!-e $old_path, 'redo immediately retires the old tombstone');
is($dropper->safe_psql('postgres', q{SELECT to_regclass('dropped_file') IS NULL}), 't',
    'DROP was replayed from the dropper WAL');
like(slurp_file($dropper->logfile), qr/redo starts at/,
    'the dropper performed actual crash recovery');

$dropper->safe_psql('postgres', 'CREATE TABLE checkpoint_drop(v integer)');
my $checkpoint_path = $dropper->data_dir . '/' . $dropper->safe_psql('postgres',
    q{SELECT pg_relation_filepath('checkpoint_drop')});
$dropper->safe_psql('postgres', 'DROP TABLE checkpoint_drop');
ok(-f $checkpoint_path && -s $checkpoint_path == 0,
    'a second DROP also reserves its locator');
$dropper->safe_psql('postgres', 'CHECKPOINT');
ok(!-e $checkpoint_path, 'the existing checkpointer unlink queue retires the tombstone');
ok(-f $new_path && slurp_file($new_path) eq $source_bytes,
    'delayed unlink does not touch the surviving locator');

$dropper->safe_psql('postgres', 'CREATE TABLE failed_unlink(v integer)');
my $failed_path = $dropper->data_dir . '/' . $dropper->safe_psql('postgres',
    q{SELECT pg_relation_filepath('failed_unlink')});
$dropper->safe_psql('postgres', 'DROP TABLE failed_unlink');
my $parent = $failed_path;
$parent =~ s{/[^/]+$}{};
chmod(0555, $parent) == 1 or die "protect fixture directory: $!";
my ($out, $err);
my $checkpoint_rc = $dropper->psql('postgres', 'CHECKPOINT', stdout => \$out, stderr => \$err);
chmod(0700, $parent) == 1 or die "restore fixture directory: $!";
is($checkpoint_rc, 0, 'deferred unlink failure does not abort a committed checkpoint');
ok(-f $failed_path && -s $failed_path == 0,
    'failed unlink retains its tombstone and cannot expose the old file number');
like(slurp_file($dropper->logfile), qr/WARNING:.*could not remove file .*cluster_shared:/,
    'checkpointer reports deferred unlink failure as WARNING');

# Inject the storage truncate ERROR into actual commit cleanup. The same
# backend must then cancel normally, including in non-cassert builds.
$dropper->safe_psql('postgres', 'CREATE TABLE failed_truncate(v integer); CHECKPOINT');
my $truncate_path = $dropper->data_dir . '/' . $dropper->safe_psql('postgres',
    q{SELECT pg_relation_filepath('failed_truncate')});
my $truncate_rc = $dropper->psql('postgres', q{
SET cluster.injection_points='cluster-shared-fs-local-truncate:error';
BEGIN;
DROP TABLE failed_truncate;
COMMIT;
SET statement_timeout='250ms';
SELECT pg_sleep(10);
}, stdout => \$out, stderr => \$err, timeout => 15);
is($truncate_rc, 3, 'the backend commits DROP then remains cancellable after truncate failure');
like($err, qr/WARNING:.*cluster-shared-fs-local-truncate.*ERROR/,
    'the storage truncate fault is downgraded to WARNING');
like($err, qr/canceling statement due to statement timeout/,
    'the same backend still processes statement cancellation');
ok(-f $truncate_path, 'failed truncate retains its MAIN name');
is($dropper->safe_psql('postgres', q{SELECT to_regclass('failed_truncate') IS NULL}), 't',
    'the DROP transaction really committed despite its cleanup failure');

# Force a real auxiliary unlink failure after commit without interfering with
# catalog/WAL writes: unlink(2) cannot remove this directory at the VM name.
$dropper->safe_psql('postgres', 'CREATE TABLE failed_aux_unlink(v integer); CHECKPOINT');
my $aux_path = $dropper->data_dir . '/' . $dropper->safe_psql('postgres',
    q{SELECT pg_relation_filepath('failed_aux_unlink')}) . '_vm';
make_path($aux_path);
my $aux_rc = $dropper->psql('postgres', q{
BEGIN;
DROP TABLE failed_aux_unlink;
COMMIT;
SET statement_timeout='250ms';
SELECT pg_sleep(10);
}, stdout => \$out, stderr => \$err, timeout => 15);
is($aux_rc, 3, 'auxiliary unlink failure leaves the commit backend cancellable');
like($err, qr/WARNING:.*could not unlink .*_vm/,
    'post-commit auxiliary unlink failure is a WARNING');
like($err, qr/canceling statement due to statement timeout/,
    'same backend processes cancellation after auxiliary cleanup failure');
ok(-d $aux_path, 'failed auxiliary unlink leaves the conflicting name occupied');
# A pre-fix PANIC also prevents recovery from deleting the same VM path.
# Keep those failed assertions, then retire only the injected empty directory
# so the rest of this regression can run instead of waiting on a dead server.
if ($aux_rc == 2)
{
    $dropper->stop('immediate', fail_ok => 1);
    rmdir($aux_path) or die "remove injected auxiliary failure: $!";
    $dropper->start;
}
ok($dropper->poll_query_until('postgres', 'SELECT true'), 'dropper remains available');
is($dropper->safe_psql('postgres', q{SELECT to_regclass('failed_aux_unlink') IS NULL}), 't',
    'auxiliary cleanup failure does not undo the committed DROP');
$allocator->stop;
$dropper->stop;

# Catalog-only startup must not silently select minimal-WAL creation while
# its OID allocator permits wrap. This fails before catalog authority reads.
for my $catalog_first (0, 1)
{
    my $minimal = PostgreSQL::Test::Cluster->new("oid_minimal_wal_$catalog_first");
    $minimal->init;
    my @settings = ('cluster.shared_catalog=on', 'wal_level=minimal');
    @settings = reverse @settings unless $catalog_first;
    $minimal->append_conf('postgresql.conf',
        join("\n", 'cluster.enabled=off', 'max_wal_senders=0', @settings) . "\n");
    ok(!$minimal->start(fail_ok => 1),
        "shared catalog refuses minimal WAL in configuration order $catalog_first");
    like(slurp_file($minimal->logfile), qr/FATAL:.*shared catalogs require wal_level=replica/,
        'startup refuses instead of warning and silently disabling shared catalogs');
}
# Without durable root checkpoint publication this profile cannot retire
# shared locator reservations safely, even with full WAL. Reject startup.
for my $catalog_first (0, 1)
{
    my $catalog_only = PostgreSQL::Test::Cluster->new("oid_catalog_only_$catalog_first");
    $catalog_only->init;
    my @settings = ('cluster.shared_catalog=on', 'cluster.shared_config=off');
    @settings = reverse @settings unless $catalog_first;
    $catalog_only->append_conf('postgresql.conf',
        join("\n", 'cluster.enabled=off', 'wal_level=replica', @settings) . "\n");
    ok(!$catalog_only->start(fail_ok => 1),
        "catalog-only startup is rejected in configuration order $catalog_first");
    like(slurp_file($catalog_only->logfile),
        qr/FATAL:.*shared catalogs require cluster.shared_config=on/,
        'rejection is explicit before catalog or OID authority access');
}
done_testing();
