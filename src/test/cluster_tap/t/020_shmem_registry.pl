#-------------------------------------------------------------------------
#
# 020_shmem_registry.pl
#    End-to-end regression for the cluster shmem region registry
#    introduced at stage 1.3.
#
#    Verifies the SQL surface backed by cluster_shmem.c registry +
#    cluster_views.c::cluster_shmem_dump_regions:
#
#      - pg_cluster_shmem view exists and returns the stage 1.3
#        baseline (2 rows: cluster_ctl + cluster_conf).
#      - Column types are (text, bigint, int4, text).
#      - All rows have non-NULL values.
#      - cluster_ctl region is exactly 24 bytes (sizeof(ClusterShmemCtl)
#        on 64-bit ABI; MAXALIGN does not pad past 24).
#      - pg_cluster_state.shmem.region_count == 2.
#      - pg_cluster_state.shmem.total_bytes equals sum from view.
#      - Per-region rollup keys (region.<name>.bytes / .owner) appear
#        for both registered regions.
#      - cluster.shmem_max_regions GUC is int / postmaster context /
#        default 64 / range [17, 256].
#      - Lowering cluster.shmem_max_regions to the current baseline
#        region count still allows every region to register (no FATAL).
#      - 4 cluster-shmem-* injection points exist in
#        pg_stat_cluster_injections (registry total: 24 = 20 + 4).
#      - cluster_inject_fault('cluster-shmem-views-srf-entry','warning',0)
#        + SELECT pg_cluster_shmem -> log WARNING.
#      - pg_cluster_state.guc.cluster.shmem_max_regions reflects the
#        current GUC value.
#      - pg_stat_cluster_wait_events baseline stays in sync with cluster
#        wait-event additions.
#      - 0.30 stage-0 acceptance baseline (pg_cluster_state row count
#        non-decreasing) preserved by the additive shmem.region.* keys.
#
# IDENTIFICATION
#    src/test/cluster_tap/t/020_shmem_registry.pl
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
# L1: pg_cluster_shmem view exists with the 4-column signature.
# ----------
is($node->safe_psql(
		'postgres', q{
	SELECT string_agg(format_type(atttypid, atttypmod), ',' ORDER BY attnum)
	  FROM pg_attribute
	 WHERE attrelid = 'pg_cluster_shmem'::regclass
	   AND attnum > 0 AND NOT attisdropped
}),
   'text,bigint,integer,text',
   'L1 pg_cluster_shmem columns are (text, bigint, integer, text)');


# ----------
# L2: stage 1.10.1 baseline -- 5 rows (cluster_ctl + cluster_conf +
# cluster_pcm_grd + cluster startup phase added by spec-1.10.1 F1).
# ----------
is($node->safe_psql(
		'postgres',
		q{SELECT count(*) FROM pg_cluster_shmem}),
	   '33',
	   'L2 pg_cluster_shmem returns 31 rows (spec-2.39 + spec-2.38 SI Broadcaster regions (ack wait + ack outbound + inbound + outbound))');

is($node->safe_psql(
		'postgres',
		q{SELECT string_agg(name, ',' ORDER BY name) FROM pg_cluster_shmem}),
   'pgrac cluster conf,pgrac cluster control,pgrac cluster cssd,pgrac cluster diag,pgrac cluster epoch,pgrac cluster fence,pgrac cluster gcs,pgrac cluster gcs block,pgrac cluster gcs block dedup,pgrac cluster ges,pgrac cluster ges dedup,pgrac cluster ges reply wait,pgrac cluster grd,pgrac cluster grd outbound,pgrac cluster grd pending,pgrac cluster grd work queue,pgrac cluster lck,pgrac cluster lmd,pgrac cluster lmd graph,pgrac cluster lmon,pgrac cluster lms,pgrac cluster pcm grd,pgrac cluster qvotec,pgrac cluster reconfig,pgrac cluster scn,pgrac cluster sinval ack outbound,pgrac cluster sinval ack wait,pgrac cluster sinval inbound,pgrac cluster sinval outbound,pgrac cluster smgr,pgrac cluster startup phase,pgrac cluster stats,pgrac cluster_ic_tier1',
   'L3 pg_cluster_shmem rows are exactly the 33 foundational regions (spec-2.39 adds sinval ack wait + ack outbound on top of spec-2.38 inbound + outbound)');


# ----------
# L4: no NULL values (informational view contract).
# ----------
is($node->safe_psql(
		'postgres',
		q{SELECT count(*) FROM pg_cluster_shmem
		   WHERE name IS NULL OR size_bytes IS NULL
		      OR lwlock_count IS NULL OR owner_subsys IS NULL}),
   '0',
   'L4 pg_cluster_shmem has no NULL values');


# ----------
# L5: cluster_ctl region size is 24 bytes (sizeof(ClusterShmemCtl) +
# MAXALIGN; on 64-bit ABI the struct is exactly 24 bytes).
# ----------
is($node->safe_psql(
		'postgres',
		q{SELECT size_bytes FROM pg_cluster_shmem
		   WHERE name = 'pgrac cluster control'}),
   '24',
   'L5 cluster_ctl region size matches sizeof(ClusterShmemCtl) = 24');

is($node->safe_psql(
		'postgres',
		q{SELECT owner_subsys FROM pg_cluster_shmem
		   WHERE name = 'pgrac cluster control'}),
   'cluster_ctl',
   'L6 cluster_ctl region owner_subsys = "cluster_ctl"');

is($node->safe_psql(
		'postgres',
		q{SELECT owner_subsys FROM pg_cluster_shmem
		   WHERE name = 'pgrac cluster conf'}),
   'cluster_conf',
   'L7 cluster_conf region owner_subsys = "cluster_conf"');


# ----------
# L8: pg_cluster_state.shmem.region_count + total_bytes summary keys.
# ----------
is($node->safe_psql(
		'postgres',
		q{SELECT value FROM pg_cluster_state
		   WHERE category = 'shmem' AND key = 'region_count'}),
	   '33',
	   'L8 pg_cluster_state.shmem.region_count = 31 (spec-2.38 SI Broadcaster regions included)');

is($node->safe_psql(
		'postgres', q{
	SELECT (SELECT value::int8 FROM pg_cluster_state
	         WHERE category='shmem' AND key='total_bytes')
	     = (SELECT sum(size_bytes) FROM pg_cluster_shmem)
}),
   't',
   'L9 pg_cluster_state.shmem.total_bytes equals sum(size_bytes) from view');


# ----------
# L10: per-region rollup keys (region.<name>.bytes / .owner).
# ----------
is($node->safe_psql(
		'postgres',
		q{SELECT count(*) FROM pg_cluster_state
		   WHERE category='shmem' AND key LIKE 'region.%.bytes'}),
	   '33',
	   'L10 pg_cluster_state.shmem has 31 region.<name>.bytes keys (one per region)');

is($node->safe_psql(
		'postgres',
		q{SELECT count(*) FROM pg_cluster_state
		   WHERE category='shmem' AND key LIKE 'region.%.owner'}),
	   '33',
	   'L11 pg_cluster_state.shmem has 31 region.<name>.owner keys (one per region)');


# ----------
# L12: cluster.shmem_max_regions GUC metadata + default + range.
# ----------
is($node->safe_psql(
		'postgres', q{
	SELECT vartype || '|' || context || '|' || boot_val ||
	       '|' || min_val || '|' || max_val
	  FROM pg_settings
	 WHERE name = 'cluster.shmem_max_regions'
}),
   'integer|postmaster|64|33|256',
   'L12 cluster.shmem_max_regions: int / postmaster / default 64 / [31,256] (spec-2.38 SI Broadcaster bumps min_val 29→31)');

is($node->safe_psql(
		'postgres',
		q{SELECT value FROM pg_cluster_state
		   WHERE category = 'guc' AND key = 'cluster.shmem_max_regions'}),
   '64',
   'L13 pg_cluster_state.guc.cluster.shmem_max_regions reflects live GUC');


# ----------
# L14: 4 cluster-shmem-* injection points + total registry 24.
# ----------
is($node->safe_psql(
		'postgres',
		q{SELECT count(*) FROM pg_stat_cluster_injections
		   WHERE name LIKE 'cluster-shmem-%'}),
   '5',
   'L14 5 cluster-shmem-* injection points (4 stage-1.3 + 1 stage-0.27 cluster-shmem-request)');

is($node->safe_psql(
		'postgres',
		'SELECT count(*) FROM pg_stat_cluster_injections'),
   '114',
   'L15 total injection registry size is 112 after spec-2.38');


# ----------
# L16: cluster_inject_fault on the SRF entry point fires WARNING.
# Single psql session so the per-backend arm state is observable.
# ----------
my $stdout_inject = $node->safe_psql('postgres', q{
	SELECT cluster_inject_fault('cluster-shmem-views-srf-entry','warning',0);
});
my ($stdout, $stderr);
$node->psql(
	'postgres',
	q{SELECT cluster_inject_fault('cluster-shmem-views-srf-entry','warning',0);
	  SELECT count(*) FROM pg_cluster_shmem;},
	stdout => \$stdout,
	stderr => \$stderr);
like($stderr,
	 qr/cluster injection point/i,
	 'L16 cluster-shmem-views-srf-entry fires WARNING when armed');


# ----------
# L17: pg_stat_cluster_wait_events baseline includes spec-2.20 GES S4 wait.
# ----------
is($node->safe_psql(
		'postgres',
		'SELECT count(*) FROM pg_stat_cluster_wait_events'),
	   '91',
	   'L17 pg_stat_cluster_wait_events returns 88 rows after spec-2.36 D7');


# ----------
# L18: GUC max_regions=29 (boundary minimum, spec-2.34 D2 bump) admits
# all baseline regions.
# ----------
$node->stop;
$node->append_conf('postgresql.conf', "cluster.shmem_max_regions = 33\n");
$node->start;

is($node->safe_psql(
		'postgres',
		q{SELECT count(*) FROM pg_cluster_shmem}),
	   '33',
	   'L18 cluster.shmem_max_regions = 31 exactly admits the 33 baseline regions (lower bound match)');

is($node->safe_psql(
		'postgres',
		q{SELECT value FROM pg_cluster_state
   WHERE category = 'guc' AND key = 'cluster.shmem_max_regions'}),
   '33',
   'L19 pg_cluster_state.guc.cluster.shmem_max_regions reflects override = 31');

$node->stop;

done_testing();
