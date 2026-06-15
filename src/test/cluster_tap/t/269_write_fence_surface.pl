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
#    Ships default OFF (opt-in): a safe default-ON for a healthy steady-
#    state cluster needs the baseline-marker subsystem, deferred to a 4.12b
#    follow-up; without it an unfenced cluster has no durable marker, the
#    token never refreshes, and the gate would fail every write closed.
#
#    Test matrix (single-node; catalog / GUC / SRF / observability + the
#    enforcement-off no-op functional path):
#
#      L1   cluster.write_fence_enforcement GUC registered (enum,
#           default off, postmaster context)
#      L2   cluster.write_fence_lease_ms GUC default 6000ms (sighup)
#      L3   the "pgrac cluster write fence" shmem region is registered
#      L4   both write-fence wait events are registered
#           (ClusterWriteFenceMarkerWrite + ClusterWriteFenceVerify)
#      L5   the write_fence dump category exposes 4 counters, all 0 at
#           a fresh start (no fence has fired)
#      L6   enforcement=off (default) -> the hot gate is a no-op: a
#           normal heap write/read round-trips (the fence never blocks
#           an unfenced single node)
#      L7   after the normal write, the 4 counters are still 0 (the gate
#           did not fire in off mode)
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
# default off -- opt-in; safe default-ON needs the 4.12b baseline marker).
# ----------
my $enf = $node->safe_psql('postgres',
	q{SELECT setting, vartype, context FROM pg_settings
	    WHERE name = 'cluster.write_fence_enforcement'});
is($enf, 'off|enum|postmaster',
   'L1 cluster.write_fence_enforcement registered (enum / postmaster / default off)');


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
# L5: the write_fence dump category exposes 4 counters, all 0 at fresh
# start (no fence has fired).
# ----------
my $keys = $node->safe_psql('postgres',
	q{SELECT string_agg(key, ',' ORDER BY key)
	    FROM pg_cluster_state WHERE category = 'write_fence'});
is($keys,
   'durable_check_blocked,hot_gate_blocked,marker_write_failed,minority_marker_ignored',
   'L5 write_fence dump category exposes the 4 counters');

my $all_zero = $node->safe_psql('postgres',
	q{SELECT bool_and(value = '0') FROM pg_cluster_state WHERE category = 'write_fence'});
is($all_zero, 't', 'L5 all 4 write_fence counters are 0 at a fresh start');


# ----------
# L6: enforcement=off (default) -> the hot gate is a no-op.  A normal heap
# write + read round-trips: the fence never blocks an unfenced single node.
# ----------
$node->safe_psql('postgres',
	q{CREATE TABLE wf_smoke(id int primary key, v text)});
$node->safe_psql('postgres',
	q{INSERT INTO wf_smoke SELECT g, 'row' || g FROM generate_series(1, 500) g});
my $cnt = $node->safe_psql('postgres', q{SELECT count(*) FROM wf_smoke});
is($cnt, '500',
   'L6 enforcement=off: cluster_smgr write/extend gate is a no-op (500 rows written + read)');

# A truncate + re-extend exercises the truncate / extend gate entries too.
$node->safe_psql('postgres', q{TRUNCATE wf_smoke});
$node->safe_psql('postgres',
	q{INSERT INTO wf_smoke SELECT g, 'r' || g FROM generate_series(1, 50) g});
my $cnt2 = $node->safe_psql('postgres', q{SELECT count(*) FROM wf_smoke});
is($cnt2, '50', 'L6 truncate + re-extend also pass the gate in off mode');


# ----------
# L7: after the normal writes, the 4 counters are STILL 0 -- the gate did
# not fire in off mode (no false-positive fencing of an unfenced node).
# ----------
my $still_zero = $node->safe_psql('postgres',
	q{SELECT bool_and(value = '0') FROM pg_cluster_state WHERE category = 'write_fence'});
is($still_zero, 't',
   'L7 write_fence counters stay 0 after normal writes (gate did not fire in off mode)');


$node->stop;

done_testing();
