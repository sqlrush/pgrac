#-------------------------------------------------------------------------
#
# 018_shared_fs.pl
#    End-to-end regression for the cluster_shared_fs abstraction layer
#    introduced in stage 1.1.
#
#    Stage 1.1 shipped two built-in backends (stub + local) and reserved
#    four enumvals for later cluster backends.  Spec-6.0a promotes
#    block_device to a production provider; cluster_fs remains the
#    shared-filesystem provider name.  This TAP test exercises the
#    surfaces visible to a running PG instance:
#
#      - cluster.shared_storage_backend default is 'stub'.
#      - pg_settings exposes the GUC with vartype=enum,
#        context=postmaster, and all six enumvals.
#      - Runtime SET is rejected (PGC_POSTMASTER).
#      - postgresql.conf override = local restarts cleanly and
#        cluster_dump_state reports active_backend=local.
#      - postgresql.conf override = block_device prevents the server
#        from starting until cluster.block_device_path is configured
#        (fail-closed production storage startup).
#      - 12 cluster_shared_fs wait events are present in
#        pg_stat_cluster_wait_events under type='Cluster: SharedFs'.
#      - 3 cluster_shared_fs injection points appear in
#        pg_stat_cluster_injections (registry total: 17 = 14 + 3).
#      - cluster_inject_fault('cluster-shared-fs-init-top','warning',0)
#        followed by a restart bumps that point's hits counter.
#      - pg_cluster_state has a 'shared_fs' category with two keys
#        (active_backend, registered_backends).
#
# IDENTIFICATION
#    src/test/cluster_tap/t/018_shared_fs.pl
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
# L1: default GUC is 'stub'.
# ----------
$node->assert_cluster_guc('cluster.shared_storage_backend', 'stub',
	'L1 cluster.shared_storage_backend default is stub');


# ----------
# L2: pg_settings metadata + 6 enumvals.
# ----------
my $row = $node->safe_psql(
	'postgres',
	q{SELECT vartype, context FROM pg_settings
	   WHERE name = 'cluster.shared_storage_backend'});
is($row, 'enum|postmaster',
   'L2 cluster.shared_storage_backend is enum (postmaster context)');

my $enumvals = $node->safe_psql(
	'postgres',
	q{SELECT array_to_string(enumvals, ',')
	    FROM pg_settings WHERE name = 'cluster.shared_storage_backend'});
is($enumvals, 'stub,local,block_device,cluster_fs,rbd,multi_attach',
   'L3 cluster.shared_storage_backend enumvals expose all six backends');


# ----------
# L4: runtime SET is rejected (PGC_POSTMASTER).
# ----------
my ($stdout, $stderr);
$node->psql('postgres',
	q{SET "cluster.shared_storage_backend" = 'local'},
	stdout => \$stdout, stderr => \$stderr);
like($stderr, qr/cannot be changed without restarting the server/i,
	'L4 SET cluster.shared_storage_backend at runtime is rejected (PGC_POSTMASTER)');


# ----------
# L5: postgresql.conf override = local restarts cleanly.
# ----------
$node->stop;
$node->append_conf('postgresql.conf', "cluster.shared_storage_backend = local\n");
$node->start;

$node->assert_cluster_guc('cluster.shared_storage_backend', 'local',
	'L5 cluster.shared_storage_backend = local applied across restart');

is($node->safe_psql('postgres',
		q{SELECT value FROM pg_cluster_state
		   WHERE category = 'shared_fs' AND key = 'active_backend'}),
	'local',
	'L6 pg_cluster_state.shared_fs.active_backend reflects override = local');


# ----------
# L7: 12 wait events under "Cluster: SharedFs".
# ----------
is($node->safe_psql(
		'postgres',
		q{SELECT count(*) FROM pg_stat_cluster_wait_events
		   WHERE type = 'Cluster: SharedFs'}),
	'12',
	'L7 12 cluster_shared_fs wait events registered under type "Cluster: SharedFs"');


# ----------
# L8: 3 cluster-shared-fs-* injection points + total registry 17.
# ----------
is($node->safe_psql(
		'postgres',
		q{SELECT count(*) FROM pg_stat_cluster_injections
		   WHERE name LIKE 'cluster-shared-fs-%'}),
	'3',
	'L8 3 cluster_shared_fs injection points registered');

is($node->safe_psql(
		'postgres',
		'SELECT count(*) FROM pg_stat_cluster_injections'),
	'159',
	'L9 total injection registry size is 159 (spec-6.14 D5+D8 +3; spec-5.6a +1; spec-6.12i +1 cluster-lms-undo-fetch; spec-6.12b +1 cluster-lms-cr-construct; spec-6.12a ㉕ +1 cluster-gcs-block-remote-downgrade; spec-5.2a +1 clean-xfer stale-holder; spec-4.8ab +2 undo boundary guards; spec-5.7 +1 cluster-ko-peer-skip-ack; spec-2.41 +1 cluster-gcs-block-stale-ship; spec-5.55 Hardening v1.1 +1 cluster-cr-resolver-memo-suspect; spec-5.15 Hardening v1.1 +1 cluster-reconfig-join-commit-marker-durable; spec-2.29a +1 cluster-qvotec-marker-service-hold)');


# ----------
# L10: pg_cluster_state shared_fs category contents.  Stage 1.2 added
# smgr_active_relations + smgr_user_relations rows.
# ----------
is($node->safe_psql(
		'postgres',
		q{SELECT string_agg(key, ',' ORDER BY key) FROM pg_cluster_state
		   WHERE category = 'shared_fs'}),
	'active_backend,registered_backends,smgr_active_relations,smgr_inval_bcast_sent_count,smgr_user_relations',
	'L10 pg_cluster_state.shared_fs has all 5 expected keys (1.1 + 1.2 + spec-5.2 D1 smgr_inval_bcast_sent_count)');

is($node->safe_psql(
		'postgres',
	q{SELECT value FROM pg_cluster_state
		   WHERE category = 'shared_fs' AND key = 'registered_backends'}),
	'stub,local,block_device,shared_fs',
	'L11 registered_backends lists all built-in backends (spec-6.0a adds block_device)');


# ----------
# L12: block_device without a device path makes startup FATAL.
#
#   Switch from "local" to "block_device" (PG GUC takes the last
#   assignment for a given key).  The production raw provider must not
#   silently fall back to a stub path, so startup fails unless
#   cluster.block_device_path names an absolute device/file path.  We
#   cannot use $node->start because PostgreSQL::Test::Cluster calls
#   BAIL_OUT on a failed pg_ctl start (uncatchable by eval), so we
#   invoke pg_ctl directly via system() and inspect the exit code + log.
# ----------
$node->stop;
$node->append_conf('postgresql.conf', "cluster.shared_storage_backend = block_device\n");

my $pg_ctl = $ENV{PG_CTL} || 'pg_ctl';
my $exit_code = system($pg_ctl, '-w', '-t', '6', '-D', $node->data_dir,
					   '-l', $node->logfile, 'start');
isnt($exit_code, 0,
	 'L12 postmaster refuses to start when block_device has no configured path');

# The startup attempt left a postmaster log behind; confirm the
# specific fail-closed detail reached it.
my $log = slurp_file($node->logfile);
like($log,
	 qr/cluster\.block_device_path must be set when shared_storage_backend=block_device/,
	 'L13 startup log names missing cluster.block_device_path');


done_testing();
