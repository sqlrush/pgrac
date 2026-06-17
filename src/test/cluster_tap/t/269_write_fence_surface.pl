#-------------------------------------------------------------------------
#
# 269_write_fence_surface.pl
#    spec-4.12 cooperative write-fence (split-brain recovery guard):
#    single-node SURFACE verification of the wiring + observability +
#    fail-closed-default state that D1-D7 land.
#
#    The cooperative write fence makes every shared-storage write consult a
#    local atomic write-fence token (refreshed by qvotec from a durable
#    voting-disk fence marker that the reconfig coordinator writes BEFORE
#    recovery).  A stale / lease-expired / self-fenced node fails closed
#    (53R51 / PANIC); the recovery / rejoin paths re-verify the durable
#    marker (Oracle-aligned authority check, full redo still applied).
#
#    spec-4.12b D4: enforcement now ships default ON.  A single node with
#    no voting disks cannot fence (no shared storage, no quorum authority)
#    and auto-degrades to a no-op at runtime (cluster_write_fence_enforcing()
#    == false), so it stays writable out of the box; qvotec logs the degrade
#    once at startup.  The baseline-marker subsystem (D2) is what makes the
#    multi-node default-ON safe.
#
#    Test matrix (single-node; catalog / GUC / SRF / observability + the
#    single-node auto-degrade no-op functional path):
#
#      L1   cluster.write_fence_enforcement GUC registered (enum,
#           default ON -- spec-4.12b D4, postmaster context)
#      L2   cluster.write_fence_lease_ms GUC default 6000ms (sighup)
#      L3   the "pgrac cluster write fence" shmem region is registered
#      L4   both write-fence wait events are registered
#           (ClusterWriteFenceMarkerWrite + ClusterWriteFenceVerify)
#      L5   the write_fence dump category exposes 8 counters (4 spec-4.12 +
#           4 spec-4.12b D6 baseline), all 0 at a fresh start (no fence fired)
#      L6   default ON + no voting disks -> auto-degrade to a no-op: a
#           normal heap write/read round-trips (the fence never blocks an
#           unfenced single node)
#      L7   after the normal write, the 4 counters are still 0 (the gate
#           did not fire in the auto-degraded single-node mode)
#      L8   qvotec logged the single-node auto-degrade notice once at
#           startup (spec-4.12b D4 LOG-once observability)
#
#    NOT reachable single-node (honest forward, mirrors 4.9/4.11):
#      The fence FIRING scenarios from spec-4.12 §4.2 -- core 8.A marker-
#      before-recovery order, a fenced node's hot write -> 53R51, lease
#      expiry, stale-but-in-quorum, recovery direct-check BLOCKED, multi-
#      dead bitmap, the t/267 fenced-write leg, and the marker-write-
#      failed -> no-recovery path -- all require enforcement=on + a multi-
#      node reconfig that writes a durable quorum-majority voting-disk
#      marker.  Those land in a multi-node fence harness; this surface
#      test deliberately does not fake them (rule 8 / rule 18).
#
# IDENTIFICATION
#    src/test/cluster_tap/t/269_write_fence_surface.pl
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
# L1: cluster.write_fence_enforcement GUC registered (enum / postmaster /
# default ON -- spec-4.12b D4 flipped the default; single node auto-degrades).
# ----------
my $enf = $node->safe_psql('postgres',
	q{SELECT setting, vartype, context FROM pg_settings
	    WHERE name = 'cluster.write_fence_enforcement'});
is($enf, 'on|enum|postmaster',
   'L1 cluster.write_fence_enforcement registered (enum / postmaster / default on)');


# ----------
# L2: cluster.write_fence_lease_ms GUC default 6000ms (sighup-tunable).
# ----------
my $lease = $node->safe_psql('postgres',
	q{SELECT setting, vartype, context FROM pg_settings
	    WHERE name = 'cluster.write_fence_lease_ms'});
is($lease, '6000|integer|sighup',
   'L2 cluster.write_fence_lease_ms default 6000ms (integer / sighup)');


# ----------
# L3: the write-fence shmem region is registered.
# ----------
my $region = $node->safe_psql('postgres',
	q{SELECT name FROM pg_cluster_shmem WHERE name = 'pgrac cluster write fence'});
is($region, 'pgrac cluster write fence',
   'L3 "pgrac cluster write fence" shmem region registered');


# ----------
# L4: both write-fence wait events are registered.
# ----------
my $waits = $node->safe_psql('postgres',
	q{SELECT string_agg(name, ',' ORDER BY name)
	    FROM pg_stat_cluster_wait_events
	   WHERE name LIKE 'ClusterWriteFence%'});
is($waits, 'ClusterWriteFenceMarkerWrite,ClusterWriteFenceVerify',
   'L4 both write-fence wait events registered (MarkerWrite + Verify)');


# ----------
# L5: the write_fence dump category exposes 8 counters (the 4 spec-4.12 plus
# the 4 spec-4.12b D6 baseline-subsystem fields), all 0 at a fresh start.
# (baseline_published / baseline_authority_age_us can only be exercised by a
# multi-node cluster authoring a baseline -> D8 e2e on CI; single-node stays 0.)
# ----------
my $keys = $node->safe_psql('postgres',
	q{SELECT string_agg(key, ',' ORDER BY key)
	    FROM pg_cluster_state WHERE category = 'write_fence'});
is($keys,
   'baseline_author_is_self,baseline_authority_age_us,baseline_published,'
	   . 'baseline_stale_rejected,durable_check_blocked,hot_gate_blocked,'
	   . 'marker_write_failed,minority_marker_ignored',
   'L5 write_fence dump category exposes the 8 counters (4 spec-4.12 + 4 D6)');

my $all_zero = $node->safe_psql('postgres',
	q{SELECT bool_and(value = '0') FROM pg_cluster_state WHERE category = 'write_fence'});
is($all_zero, 't', 'L5 all 8 write_fence counters are 0 at a fresh start');


# ----------
# L6: default ON + no voting disks -> auto-degrade to a no-op.  A normal heap
# write + read round-trips: the fence never blocks an unfenced single node.
# ----------
$node->safe_psql('postgres',
	q{CREATE TABLE wf_smoke(id int primary key, v text)});
$node->safe_psql('postgres',
	q{INSERT INTO wf_smoke SELECT g, 'row' || g FROM generate_series(1, 500) g});
my $cnt = $node->safe_psql('postgres', q{SELECT count(*) FROM wf_smoke});
is($cnt, '500',
   'L6 single-node auto-degrade: cluster_smgr write/extend gate is a no-op (500 rows)');

# A truncate + re-extend exercises the truncate / extend gate entries too.
$node->safe_psql('postgres', q{TRUNCATE wf_smoke});
$node->safe_psql('postgres',
	q{INSERT INTO wf_smoke SELECT g, 'r' || g FROM generate_series(1, 50) g});
my $cnt2 = $node->safe_psql('postgres', q{SELECT count(*) FROM wf_smoke});
is($cnt2, '50', 'L6 truncate + re-extend also pass the gate in auto-degraded mode');


# ----------
# L7: after the normal writes, the 4 counters are STILL 0 -- the gate did
# not fire (no false-positive fencing of an unfenced single node).
# ----------
my $still_zero = $node->safe_psql('postgres',
	q{SELECT bool_and(value = '0') FROM pg_cluster_state WHERE category = 'write_fence'});
is($still_zero, 't',
   'L7 write_fence counters stay 0 after normal writes (gate did not fire)');


# ----------
# L8: spec-4.12b D4 -- qvotec logs the single-node auto-degrade notice once at
# startup (enforcement=on but no voting disks -> write fence inactive).  This is
# the observable proof that default-ON does not silently block a single node.
# ----------
ok($node->log_contains(
	   qr/enforcement is on but no voting disks are configured; the write fence is inactive/),
   'L8 qvotec logged the single-node auto-degrade notice (D4 LOG-once)');


$node->stop;

done_testing();
