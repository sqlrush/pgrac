# Copyright (c) 2026, PostgreSQL Global Development Group
# PGRAC: shared catalogs require acknowledged invalidation in either GUC order.
# Author: SqlRush <sqlrush@gmail.com>
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $case = 0;
for my $settings (
    ['cluster.shared_catalog=on', 'cluster.sinval_ack_mode=none'],
    ['cluster.sinval_ack_mode=none', 'cluster.shared_catalog=on'])
{
    my $node = PostgreSQL::Test::Cluster->new('catalog_invalidation_' . ++$case);
    $node->init;
    # Satisfy the independent WAL/root policy before checking either order of
    # the catalog/ACK settings; neither order may silently accept ACK=none.
    $node->append_conf('postgresql.conf',
        "wal_level=replica\ncluster.shared_config=on");
    $node->append_conf('postgresql.conf', join("\n", @$settings));
    ok(!$node->start(fail_ok => 1), 'unsafe acknowledgement configuration prevents startup');
    like(slurp_file($node->logfile), qr/shared catalogs require acknowledged invalidation/,
        'shared catalog safety check runs in either assignment order');
}
my $native = PostgreSQL::Test::Cluster->new('catalog_invalidation_native');
$native->init;
$native->append_conf('postgresql.conf', qq{cluster.enabled=off\ncluster.sinval_ack_mode=none});
$native->start;
is($native->safe_psql('postgres', 'SHOW cluster.sinval_ack_mode'), 'none',
    'non-shared acknowledgement setting is preserved');
# Use actual PG-generated init files from two databases, not synthetic files.
$native->safe_psql('postgres', 'CREATE DATABASE catalog_cache_peer');
$native->safe_psql('catalog_cache_peer', 'SELECT count(*) FROM pg_class');
my @dbids = split /\n/, $native->safe_psql('postgres',
    "SELECT oid FROM pg_database WHERE datname IN ('postgres','catalog_cache_peer') ORDER BY oid");
my @initfiles = (map { $native->data_dir . "/base/$_/pg_internal.init" } @dbids);
push @initfiles, $native->data_dir . '/global/pg_internal.init';
ok(-f $_, "native cache exists: $_") for @initfiles;
$native->safe_psql('postgres', q{
    CREATE FUNCTION p3b_catalog_reset() RETURNS void
    AS '$libdir/test_pgrac_shared_config', 'test_pgrac_catalog_reset' LANGUAGE C;
    SELECT p3b_catalog_reset();
});
ok(!-e $_, "publication reset removes native cache: $_") for @initfiles;
is($native->safe_psql('catalog_cache_peer', q{SELECT 1 FROM pg_class WHERE relname='pg_class'}),
    '1', 'new backend rebuilds usable catalog cache after full invalidation');
$native->stop('fast');
done_testing();
