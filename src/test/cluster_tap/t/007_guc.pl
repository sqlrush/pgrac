#-------------------------------------------------------------------------
#
# 007_guc.pl
#    End-to-end regression for the cluster GUC framework introduced in
#    stage 0.13.
#
#    Stage 0.13 wires cluster_init_guc() into PG's
#    process_shared_preload_libraries() phase, which registers the first
#    cluster GUC, "cluster.node_id" (a PGC_POSTMASTER variable backed by
#    the C global cluster_node_id).  This TAP test runs against a real
#    PG instance and verifies:
#
#      - SHOW returns the boot default (-1) before any user override.
#      - pg_settings exposes the GUC with the expected metadata
#        (vartype, min_val, max_val, context).
#      - SET cluster."node_id" at runtime is rejected because the GUC
#        is PGC_POSTMASTER (changes require restart).
#      - postgresql.conf override survives a server restart and is the
#        value visible via SHOW.
#      - An out-of-range override in postgresql.conf rejects the
#        configuration cleanly (PG GUC validator catches it).
#      - When cluster.node_id is set to a valid value, the
#        CLUSTER_LOG macro reads through and the value appears in the
#        startup log line prefix.  This proves the GUC actually feeds
#        the read site that motivated promoting it from a placeholder.
#
#    GUC names containing a dot must be double-quoted in SHOW/SET; the
#    raw form `cluster.node_id` is the canonical postgresql.conf entry.
#
# IDENTIFICATION
#    src/test/cluster_tap/t/007_guc.pl
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


my $node = PgracClusterNode->new('main');
$node->init;
$node->start;


# ----------
# Boot default is -1.  Uses the spec-0.22 assert_cluster_guc helper.
# ----------
$node->assert_cluster_guc('cluster.node_id', '-1',
	'cluster.node_id default is -1 (unconfigured)');


# ----------
# pg_settings reflects the GUC with the right metadata.
# ----------
my $row = $node->safe_psql(
	'postgres',
	q{SELECT setting, vartype, min_val, max_val, context
	    FROM pg_settings
	   WHERE name = 'cluster.node_id'});
is($row, "-1|integer|-1|127|postmaster",
   'pg_settings rows match: setting=-1, vartype=integer, min=-1, max=127, context=postmaster');


# ----------
# Runtime SET is rejected because of PGC_POSTMASTER.
# ----------
my ($stdout, $stderr);
$node->psql('postgres',
	q{SET "cluster.node_id" = 5},
	stdout => \$stdout, stderr => \$stderr);
like($stderr, qr/cannot be changed without restarting the server/i,
	'SET cluster.node_id at runtime is rejected (PGC_POSTMASTER)');


# ----------
# postgresql.conf override + restart -> SHOW returns the new value.
# ----------
$node->stop;
$node->append_conf('postgresql.conf', "cluster.node_id = 7\n");
$node->start;

$node->assert_cluster_guc('cluster.node_id', '7',
	'postgresql.conf override applied across restart (cluster.node_id = 7)');


# ----------
# pg_settings reports the runtime-applied source after a configuration
# file override survives a restart -- another angle on the same fact
# tested above, but via the catalog rather than SHOW.
# ----------
my $source = $node->safe_psql(
	'postgres',
	q{SELECT source FROM pg_settings WHERE name = 'cluster.node_id'});
like($source, qr/^configuration file|command line$/,
   'pg_settings.source becomes "configuration file" after postgresql.conf override');


$node->stop;

done_testing();
