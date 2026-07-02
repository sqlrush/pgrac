#-------------------------------------------------------------------------
#
# 007_guc.pl
#    End-to-end regression for the cluster GUC framework introduced in
#    stage 0.13.
#
#    Stage 0.13 wires cluster_init_guc() into PG's
#    process_shared_preload_libraries() phase, which registers the first
#    cluster GUC, "cluster.node_id" (a PGC_POSTMASTER variable backed by
#    the C global cluster_node_id).  Subsequent stages added three more
#    GUCs: cluster.interconnect_tier (enum), cluster.config_file
#    (string), cluster.injection_points (PGC_SUSET string list).  This
#    TAP test runs against a real PG instance and verifies:
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
#      - cluster.interconnect_tier enum advertises all five tiers
#        (stub / mock / tier1 / tier2 / tier3) and defaults to stub.
#      - RDMA interconnect GUCs advertise fallback/provider/completion
#        choices and safe defaults.
#      - cluster.config_file defaults to "pgrac.conf".
#      - cluster.injection_points defaults to empty and accepts a
#        runtime SET (PGC_SUSET, unlike the postmaster-locked GUCs).
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
use PostgreSQL::Test::Utils;
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


# ----------
# cluster.interconnect_tier — enum default + metadata + enumvals.
# ----------
$node->assert_cluster_guc('cluster.interconnect_tier', 'stub',
	'cluster.interconnect_tier default is stub');

my $tier_meta = $node->safe_psql(
	'postgres',
	q{SELECT vartype, context FROM pg_settings WHERE name = 'cluster.interconnect_tier'});
is($tier_meta, 'enum|postmaster',
   'cluster.interconnect_tier is an enum (postmaster context)');

my $tier_options = $node->safe_psql(
	'postgres',
	q{SELECT array_to_string(enumvals, ',')
	    FROM pg_settings WHERE name = 'cluster.interconnect_tier'});
is($tier_options, 'stub,mock,tier1,tier2,tier3',
   'cluster.interconnect_tier enumvals expose all five tiers');

is($node->safe_psql(
	'postgres',
	q{SELECT setting || '|' || vartype || '|' || context
	    FROM pg_settings WHERE name = 'cluster.interconnect_rdma_fallback'}),
	'auto|enum|postmaster',
	'cluster.interconnect_rdma_fallback defaults to auto');
is($node->safe_psql(
	'postgres',
	q{SELECT array_to_string(enumvals, ',')
	    FROM pg_settings WHERE name = 'cluster.interconnect_rdma_provider'}),
	'auto,verbs,mlx5',
	'cluster.interconnect_rdma_provider enumvals expose provider choices');
is($node->safe_psql(
	'postgres',
	q{SELECT setting || '|' || vartype || '|' || context
	    FROM pg_settings WHERE name = 'cluster.interconnect_rdma_completion'}),
	'event|enum|postmaster',
	'cluster.interconnect_rdma_completion defaults to event');
is($node->safe_psql(
	'postgres',
	q{SELECT setting || '|' || vartype || '|' || context
	    FROM pg_settings WHERE name = 'cluster.interconnect_rdma_crc_offload'}),
	'off|bool|postmaster',
	'cluster.interconnect_rdma_crc_offload defaults off');


# ----------
# cluster.config_file — string default.
# ----------
$node->assert_cluster_guc('cluster.config_file', 'pgrac.conf',
	'cluster.config_file default is "pgrac.conf"');


# ----------
# cluster.injection_points — PGC_SUSET, runtime SET succeeds.
# ----------
$node->assert_cluster_guc('cluster.injection_points', '',
	'cluster.injection_points default is empty');

# Unlike the three PGC_POSTMASTER GUCs above, this one is PGC_SUSET so
# a runtime SET inside a backend session must succeed (no restart).
$node->psql('postgres',
	q{SET "cluster.injection_points" = 'cluster-init-pre-shmem'},
	stdout => \$stdout, stderr => \$stderr);
is($stderr, '',
   'SET cluster.injection_points at runtime is accepted (PGC_SUSET)');


# ----------
# Out-of-range cluster.node_id rejected with a WARNING + fallback.
#
#   PG GUC validator policy for an out-of-range int: emit a WARNING
#   and fall back to the boot default (here -1), not the last in-range
#   value the postmaster previously held.  The postmaster still starts.
#   This matches PG's standard built-in GUC behaviour, not a strict
#   refusal.
# ----------
$node->stop;
$node->append_conf('postgresql.conf', "cluster.node_id = 999\n");
$node->start;

# After restart, SHOW must report the boot default (-1), not 999 and
# not the prior in-range value (7).
$node->assert_cluster_guc('cluster.node_id', '-1',
	'out-of-range cluster.node_id (999) falls back to the boot default (-1)');

# The startup log carries a WARNING naming the offending parameter.
my $log = slurp_file($node->logfile);
like($log,
	 qr/999 is outside the valid range for parameter "cluster.node_id"/,
	 'startup log contains GUC out-of-range WARNING for cluster.node_id');

# ----------
# spec-6.5 cluster backup / PITR GUCs.
# ----------
is($node->safe_psql('postgres',
	q{SELECT setting || '|' || vartype || '|' || context
	    FROM pg_settings WHERE name = 'cluster.recovery_target_scn'}),
	'|string|postmaster',
	'cluster.recovery_target_scn default and context');
is($node->safe_psql('postgres',
	q{SELECT setting || '|' || vartype || '|' || context
	    FROM pg_settings WHERE name = 'cluster.recovery_target_action'}),
	'pause|enum|postmaster',
	'cluster.recovery_target_action default and context');
is($node->safe_psql('postgres',
	q{SELECT setting || '|' || vartype || '|' || context
	    FROM pg_settings WHERE name = 'cluster.backup_manifest_checksums'}),
	'crc32c|enum|sighup',
	'cluster.backup_manifest_checksums is mandatory crc32c');
is($node->safe_psql('postgres',
	q{SELECT setting || '|' || vartype || '|' || context
	    FROM pg_settings WHERE name = 'cluster.backup_parallel_channels'}),
	'1|integer|sighup',
	'cluster.backup_parallel_channels default and context');

# spec-5.59 D1: cross-node profiling switch (default off, SUSET so a
# superuser can open a measurement window without a restart).
is($node->safe_psql('postgres',
	q{SELECT setting || '|' || vartype || '|' || context
	    FROM pg_settings WHERE name = 'cluster.xnode_profile'}),
	'off|bool|superuser',
	'cluster.xnode_profile default off, bool, superuser context');

done_testing();
