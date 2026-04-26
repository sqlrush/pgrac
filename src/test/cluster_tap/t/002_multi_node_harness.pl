#-------------------------------------------------------------------------
#
# 002_multi_node_harness.pl
#    Multi-process harness test: two independent PG instances run
#    side by side without colliding on ports or data directories.
#
#    This validates the test harness can scale to multi-node tests
#    later (Stage 2 Cache Fusion MVP and beyond).  Stage 0.5 does
#    NOT exercise any cross-node communication.
#
# IDENTIFICATION
#    src/test/cluster_tap/t/002_multi_node_harness.pl
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

use PostgreSQL::Test::Cluster;
use Test::More;
use PgracClusterNode;


# Spin up two independent PG instances.
my $node1 = PostgreSQL::Test::Cluster->new('cluster_node1');
$node1->init;
$node1->start;

my $node2 = PostgreSQL::Test::Cluster->new('cluster_node2');
$node2->init;
$node2->start;

# Sanity: ports and data dirs differ.
isnt($node1->port,    $node2->port,    'two nodes have different TCP ports');
isnt($node1->data_dir, $node2->data_dir, 'two nodes have different data dirs');

# Both instances respond to SQL.
is($node1->safe_psql('postgres', 'SELECT 1'), '1', 'node1 responsive');
is($node2->safe_psql('postgres', 'SELECT 1'), '1', 'node2 responsive');

# Each instance reports PG 16.13 (binary identity sanity).
like($node1->safe_psql('postgres', 'SELECT version()'),
     qr/PostgreSQL 16\.13/,
     'node1 reports PG 16.13');
like($node2->safe_psql('postgres', 'SELECT version()'),
     qr/PostgreSQL 16\.13/,
     'node2 reports PG 16.13');

$node1->stop;
$node2->stop;

done_testing();
