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
#        default 80 / range [40, 256].  The legal lower bound is the
#        historical minimum; L18 separately verifies that setting the GUC
#        to the current region count admits all registered regions.
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

my $has_visibility_inject =
  $node->safe_psql(
	  'postgres',
	  q{SELECT count(*) FROM pg_cluster_shmem
	     WHERE name = 'pgrac cluster visibility inject'}) eq '1';
# +1 for the unconditional "pgrac cluster sequence" region (spec-5.4 SQ
# instance-cache shmem foundation; sorts between scn and sinval ack outbound)
# and +1 for the unconditional "pgrac cluster cf stats" region (spec-5.6 Dc4).
# +5 for the unconditional spec-5.7 enqueue-class counter regions: "pgrac
# cluster dl" (D4, sorts between diag and durable), "pgrac cluster hw" (D1),
# "pgrac cluster ir" (D8) and "pgrac cluster ko" (D6/D7, both sort between grd
# work queue and lck), and "pgrac cluster ts" (D5, sorts between subtrans state
# and tt local seq).
# +1 for the unconditional "pgrac cluster lmd probe" region (spec-5.8 D8 — the
# LMON->LMD cross-node REPORT collector; sorts between "lmd graph" and "lmon").
# +1 for the unconditional "pgrac cluster cr pool" region (spec-5.51 D1; always
# registered, size_fn returns 0 bytes when the pool is disabled; sorts between
# "cr counters" and "cssd").
# +1 for the unconditional "pgrac cluster cr admit stats" region (spec-5.52 D9;
# independent admission reason counters, always registered; sorts between
# "control" and "cr counters").
# spec-5.13 D2: +1 for the "pgrac cluster clean_leave" region (sorts between
# "cf stats" and "conf").  spec-5.54 D5: +1 "pgrac cluster cr tuple stats".
# spec-5.56 D4: +1 "pgrac cluster cr relgen" (per-relation lifecycle generation
# table, always registered, 0 bytes when disabled; sorts between "cr pool" and
# "cr tuple stats").
my $expected_region_count = $has_visibility_inject ? '65' : '64';
my $expected_regions =
  'pgrac block recovery,pgrac cluster advisory,pgrac cluster cf stats,pgrac cluster clean_leave,pgrac cluster conf,pgrac cluster control,pgrac cluster cr admit stats,pgrac cluster cr counters,pgrac cluster cr pool,pgrac cluster cr relgen,pgrac cluster cr tuple stats,pgrac cluster cssd,pgrac cluster diag,pgrac cluster dl,pgrac cluster durable tt counters,pgrac cluster epoch,pgrac cluster fence,pgrac cluster gcs,pgrac cluster gcs block,pgrac cluster gcs block dedup,pgrac cluster ges,pgrac cluster ges dedup,pgrac cluster ges reply wait,pgrac cluster grd,pgrac cluster grd outbound,pgrac cluster grd pending,pgrac cluster grd work queue,pgrac cluster hw,pgrac cluster ir,pgrac cluster ko,pgrac cluster lck,pgrac cluster lmd,pgrac cluster lmd graph,pgrac cluster lmd probe,pgrac cluster lmon,pgrac cluster lms,pgrac cluster lock-path counters,pgrac cluster multixact overlay,pgrac cluster pcm grd,pgrac cluster qvotec,pgrac cluster reconfig,pgrac cluster scn,pgrac cluster sequence,pgrac cluster sinval ack outbound,pgrac cluster sinval ack wait,pgrac cluster sinval inbound,pgrac cluster sinval outbound,pgrac cluster smgr,pgrac cluster startup phase,pgrac cluster stats,pgrac cluster subtrans state,pgrac cluster ts,pgrac cluster tt local seq,pgrac cluster tt slot allocator,pgrac cluster tt status hint outbound,pgrac cluster tt status overlay,pgrac cluster tx enqueue,pgrac cluster undo cleaner,pgrac cluster undo record cursor';
$expected_regions .= ',pgrac cluster visibility inject'
  if $has_visibility_inject;
# spec-4.12 D7: cooperative write-fence region;  always registered.  Sorts after
# 'pgrac cluster visibility inject' ('w' > 'v') and before 'pgrac cluster_ic_tier1'
# (space 0x20 < underscore 0x5F at the 'cluster ' boundary).
$expected_regions .= ',pgrac cluster write fence';
$expected_regions .= ',pgrac cluster_ic_tier1';
# spec-4.3 D4: recovery plan mirror;  sorts after the 'pgrac cluster*'
# block and the underscore form ('r' > '_').
$expected_regions .= ',pgrac recovery plan';
# spec-3.18 D1: undo buffer pool region;  sorts after the 'pgrac cluster*'
# block (ORDER BY name: 'u' > 'c').
$expected_regions .= ',pgrac undo buffer pool';
# spec-4.1 D7: per-thread WAL routing region;  sorts last ('w' > 'u').
$expected_regions .= ',pgrac wal thread';


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
# L2: cumulative shmem baseline.  spec-3.9 adds the CR counter region,
# bringing the production registry to 42 rows; builds with the visibility
# inject region present use 43.  The cluster.shmem_max_regions legal lower
# bound stays in the historical 40/41 band (ENABLE_INJECTION builds keep 41).
# ----------
is($node->safe_psql(
		'postgres',
		q{SELECT count(*) FROM pg_cluster_shmem}),
	   $expected_region_count,
	   'L2 pg_cluster_shmem returns the spec-3.9 baseline region count');

is($node->safe_psql(
		'postgres',
		q{SELECT string_agg(name, ',' ORDER BY name) FROM pg_cluster_shmem}),
	   $expected_regions,
	   'L3 pg_cluster_shmem rows are exactly the spec-3.9 baseline regions');


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
	   $expected_region_count,
	   'L8 pg_cluster_state.shmem.region_count matches the spec-3.6 baseline');

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
	   $expected_region_count,
	   'L10 pg_cluster_state.shmem has one region.<name>.bytes key per baseline region');

is($node->safe_psql(
		'postgres',
		q{SELECT count(*) FROM pg_cluster_state
		   WHERE category='shmem' AND key LIKE 'region.%.owner'}),
	   $expected_region_count,
	   'L11 pg_cluster_state.shmem has one region.<name>.owner key per baseline region');


# ----------
# L12: cluster.shmem_max_regions GUC metadata + default + range.
# ----------
like($node->safe_psql(
		'postgres', q{
	SELECT vartype || '|' || context || '|' || boot_val ||
	       '|' || min_val || '|' || max_val
	  FROM pg_settings
	 WHERE name = 'cluster.shmem_max_regions'
}),
   qr/^integer\|postmaster\|80\|(40|41)\|256$/,
   'L12 cluster.shmem_max_regions default 80 (spec-5.56: 64 -> 80 for cr relgen region)');

is($node->safe_psql(
		'postgres',
		q{SELECT value FROM pg_cluster_state
		   WHERE category = 'guc' AND key = 'cluster.shmem_max_regions'}),
   '80',
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
   '139',
   'L15 total injection registry size is 139 (spec-5.13 +6 cluster-clean-leave-* + Hardening v1.0.3 +1 suppress-preflight-ack; spec-5.2a +1 clean-xfer stale-holder; spec-4.8ab +2 undo boundary guards; spec-5.7 +1 cluster-ko-peer-skip-ack; spec-2.41 +1 cluster-gcs-block-stale-ship)');


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
	   '102',
	   'L17 pg_stat_cluster_wait_events returns 100 rows (spec-4.6 +1 GRD shard remaster)');


# ----------
# L18: GUC max_regions at the current boundary minimum admits all baseline
# regions.
# ----------
$node->stop;
$node->append_conf('postgresql.conf',
	"cluster.shmem_max_regions = $expected_region_count\n");
$node->start;

is($node->safe_psql(
		'postgres',
		q{SELECT count(*) FROM pg_cluster_shmem}),
	   $expected_region_count,
	   'L18 cluster.shmem_max_regions exactly admits the current baseline regions');

is($node->safe_psql(
		'postgres',
		q{SELECT value FROM pg_cluster_state
   WHERE category = 'guc' AND key = 'cluster.shmem_max_regions'}),
   $expected_region_count,
   'L19 pg_cluster_state.guc.cluster.shmem_max_regions reflects lower-bound override');

$node->stop;

done_testing();
