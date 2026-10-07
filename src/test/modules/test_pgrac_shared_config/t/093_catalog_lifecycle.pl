# Copyright (c) 2026, PostgreSQL Global Development Group
# PGRAC: native relfile lifetime and cached-plan invalidation for supported DDL.
# Shared SPACE authority and cross-node recovery require separate coverage.
# Author: SqlRush <sqlrush@gmail.com>
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('catalog_lifecycle');
$node->init;
$node->append_conf('postgresql.conf', 'cluster.enabled=off');
$node->start;
$node->safe_psql('postgres', q{
CREATE TABLE lifecycle(id integer PRIMARY KEY, v text);
ALTER TABLE lifecycle ALTER COLUMN v SET STORAGE EXTERNAL;
INSERT INTO lifecycle VALUES (1, repeat('a', 20000));
});

my $writer = $node->background_psql('postgres');
my $reader = $node->background_psql('postgres');
$reader->query_safe(q{
SET enable_seqscan=off;
PREPARE lifetime_read AS SELECT id, length(v), left(v,1) FROM lifecycle WHERE id=1;
});
is($reader->query_safe('EXECUTE lifetime_read'), '1|20000|a',
   'reader warms heap, TOAST and btree through a prepared statement');
like($reader->query_safe('EXPLAIN (COSTS OFF) EXECUTE lifetime_read'),
     qr/Index Scan using lifecycle_pkey/, 'prepared statement actually uses btree');

# Include every storage-bearing relation rebuilt by ordinary TRUNCATE.
my $map_sql = q{
WITH heap AS (SELECT oid, reltoastrelid FROM pg_class WHERE oid='lifecycle'::regclass),
     objects AS (
       SELECT oid FROM heap
       UNION ALL SELECT reltoastrelid FROM heap
       UNION ALL SELECT indexrelid FROM pg_index
         WHERE indrelid IN (SELECT oid FROM heap UNION ALL SELECT reltoastrelid FROM heap)
     )
SELECT c.oid, pg_relation_filenode(c.oid), pg_relation_filepath(c.oid)
 FROM pg_class c JOIN objects o USING (oid) ORDER BY c.oid;
};

sub relation_map
{
    my ($session) = @_;
    my %map;
    for my $line (split /\n/, $session->query_safe($map_sql))
    {
        my ($oid, $filenode, $path) = split /\|/, $line;
        $map{$oid} = { filenode => $filenode, path => $path };
    }
    return \%map;
}

sub paths_exist
{
    my ($map) = @_;
    return !grep { !-f $node->data_dir . '/' . $_->{path} } values %$map;
}

sub paths_removed
{
    my ($map) = @_;
    return !grep { -e $node->data_dir . '/' . $_->{path} } values %$map;
}

sub all_files_replaced
{
    my ($before, $after) = @_;
    return 0 unless keys(%$before) == keys(%$after);
    for my $oid (keys %$before)
    {
        return 0 unless exists $after->{$oid}
            && $before->{$oid}->{filenode} ne $after->{$oid}->{filenode};
    }
    return 1;
}

my $original = relation_map($reader);
is(scalar keys %$original, 4, 'capture heap, TOAST and both btree identities');
ok(paths_exist($original), 'original relation files exist');
my $catalog_map = $reader->query_safe(q{SELECT pg_relation_filenode('pg_class')});

$writer->query_safe('BEGIN; SAVEPOINT before_truncate; TRUNCATE lifecycle');
my $aborted = relation_map($writer);
ok(all_files_replaced($original, $aborted), 'TRUNCATE replaces all four files inside transaction');
ok(paths_exist($original), 'old files remain until transaction outcome');
ok(paths_exist($aborted), 'new files exist before rollback');
$writer->query_safe('ROLLBACK TO before_truncate; COMMIT');
is_deeply(relation_map($reader), $original, 'SAVEPOINT rollback restores all original identities');
is($reader->query_safe('EXECUTE lifetime_read'), '1|20000|a',
   'warm reader retains original data and index after rollback');
$node->safe_psql('postgres', 'CHECKPOINT');
ok(paths_removed($aborted), 'aborted replacement files are retired');
ok(paths_exist($original), 'rollback cleanup preserves old live files');

$writer->query_safe('BEGIN; TRUNCATE lifecycle');
my $committed = relation_map($writer);
ok(all_files_replaced($original, $committed), 'committing TRUNCATE prepares replacement files');
ok(paths_exist($original), 'commit has not reclaimed old files prematurely');
$writer->query_safe(q{INSERT INTO lifecycle VALUES (1, repeat('b', 18000)); COMMIT});
is_deeply(relation_map($reader), $committed, 'reader consumes new identities after commit');
is($reader->query_safe('EXECUTE lifetime_read'), '1|18000|b',
   'cached plan reads replacement heap, TOAST and btree after commit');
$node->safe_psql('postgres', 'CHECKPOINT');
ok(paths_removed($original), 'committed old files retire after checkpoint');
ok(paths_exist($committed), 'replacement files survive old-file reclamation');

$writer->query_safe('BEGIN; DROP TABLE lifecycle; ROLLBACK');
is_deeply(relation_map($reader), $committed, 'DROP rollback preserves all storage identities');
is($reader->query_safe('EXECUTE lifetime_read'), '1|18000|b',
   'warm plan remains usable after DROP rollback');

$writer->query_safe(q{
BEGIN;
DROP TABLE lifecycle;
CREATE TABLE lifecycle(id integer PRIMARY KEY, v text);
INSERT INTO lifecycle VALUES(1,'replacement');
COMMIT;
});
is($reader->query_safe('EXECUTE lifetime_read'), '1|11|r',
   'DROP and same-name CREATE invalidate the prepared relation identity');
my $replacement_oid = $reader->query_safe(q{SELECT 'lifecycle'::regclass::oid});
ok(!exists $committed->{$replacement_oid}, 'same name resolves to a new relation OID');
$node->safe_psql('postgres', 'CHECKPOINT');
ok(paths_removed($committed), 'DROP retires the previous incarnation files');
is($reader->query_safe(q{SELECT pg_relation_filenode('pg_class')}), $catalog_map,
   'supported DDL leaves the mapped catalog file unchanged');

# A savepoint can cancel a drop of a relation created by the outer transaction.
$writer->query_safe(q{
BEGIN;
CREATE TABLE created_then_kept(n integer);
INSERT INTO created_then_kept VALUES(7);
SAVEPOINT before_drop;
DROP TABLE created_then_kept;
ROLLBACK TO before_drop;
COMMIT;
});
is($reader->query_safe('SELECT n FROM created_then_kept'), '7',
   'subtransaction DROP rollback preserves outer CREATE storage');

$reader->quit;
$writer->quit;
$node->stop;
done_testing();
