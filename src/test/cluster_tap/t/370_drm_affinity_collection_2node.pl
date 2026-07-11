#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 370_drm_affinity_collection_2node.pl
#	  spec-7.6 6.3b — DRM per-shard affinity collection, 2-node leg.
#
#	  Validates the D2 admission hooks fire on a live 2-node pair and that the
#	  current master accumulates BOTH its own (local) and its peer's (remote)
#	  access samples — the "current-master authoritative" property (Amend
#	  v1.1.2 R2).  A requester-side hook would only ever see access_count[self];
#	  only the master, which every node's requests for its shards converge on,
#	  can build the full per-node matrix a dominant-node ratio needs.
#
#	  Both nodes take a broad range of distinct advisory locks.  With the
#	  shipped round-robin master map (shard % declared_count) each node masters
#	  roughly half the shards, so each node records:
#	    - samples_local  from its own requests for shards IT masters
#	    - samples_remote from the PEER's requests for shards IT masters
#	  (remote requests are sampled in cluster_ges_request_handler, which runs in
#	  the LMON process, and drained by the LMON tick — spec-7.6 6.3b R4.3).
#
#	  Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../lib";

use Time::HiRes qw(usleep);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::ClusterPair;
use PostgreSQL::Test::Utils;
use Test::More;


# drm_affinity dump counter accessor.
sub drm_ctr
{
	my ($node, $key) = @_;
	my $v = $node->safe_psql('postgres',
		"SELECT value FROM cluster_dump_state() "
		. "WHERE category = 'drm_affinity' AND key = '$key'");
	return defined($v) && $v ne '' ? $v + 0 : -1;
}

# Take a broad range of distinct advisory locks in one transaction so every key
# issues a first-logical GES REQUEST (peer-mastered keys route cross-node).
sub drive_locks
{
	my ($node, $base, $count) = @_;
	$node->safe_psql('postgres', qq{
		DO \$\$
		DECLARE i int;
		BEGIN
			FOR i IN 0..$count LOOP
				PERFORM pg_advisory_xact_lock($base + i);
			END LOOP;
		END \$\$;
	});
}


my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'drm370',
	# DRM affinity collection needs a coordinated live pair (GES routing over a
	# real quorum + GRD) exactly like the other live 2-node GES tests.
	quorum_voting_disks => 3,
	extra_conf => [
		'cluster.shared_storage_backend = local',
		'cluster.grd_max_entries = 4096',
		'cluster.cssd_heartbeat_interval_ms = 2000',
		'cluster.cssd_dead_deadband_factor = 10',
		# DRM collection ON, sample every hit, count every access (deterministic
		# for the test; production defaults stay off / 1-in-16 / 1000).
		'cluster.drm_enabled = on',
		'cluster.drm_affinity_sample_rate = 1',
		'cluster.drm_min_access_count = 1',
	]);
$pair->start_pair;

$pair->wait_for_peer_state(0, 1, 'connected', 30)
  or BAIL_OUT('node0 did not observe peer 1 connected');
$pair->wait_for_peer_state(1, 0, 'connected', 30)
  or BAIL_OUT('node1 did not observe peer 0 connected');
usleep(3_000_000); # CSSD READY + quorum + LMS warmup settle

is($pair->node0->safe_psql('postgres', 'SELECT 1'), '1', 'node0 accepts SQL');
is($pair->node1->safe_psql('postgres', 'SELECT 1'), '1', 'node1 accepts SQL');

# The dump category is wired (D9 observability surface for 6.3b).
is($pair->node0->safe_psql('postgres',
		"SELECT count(*) > 0 FROM cluster_dump_state() WHERE category = 'drm_affinity'"),
	't', 'drm_affinity dump category present on node0');

# Baseline: nothing sampled yet across the pair (both nodes just started).
# (Background lock traffic may exist, so we only require the DELTA below.)
my $n0_local0 = drm_ctr($pair->node0, 'samples_local');
my $n0_remote0 = drm_ctr($pair->node0, 'samples_remote');
my $n1_local0 = drm_ctr($pair->node1, 'samples_local');
my $n1_remote0 = drm_ctr($pair->node1, 'samples_remote');

# Drive cross-node access demand from BOTH nodes over disjoint key ranges.  A
# wide range guarantees each node both masters some of its own keys (local) and
# receives the peer's requests for shards it masters (remote), and enough remote
# requests to fill + drain the LMON sample ring.
drive_locks($pair->node0, 3_700_000, 400);
drive_locks($pair->node1, 3_800_000, 400);

# Let the LMON tick drain the remote-sample ring on both nodes.
usleep(4_000_000);

# Nudge the tick along with a little more traffic + settle.
drive_locks($pair->node0, 3_710_000, 100);
drive_locks($pair->node1, 3_810_000, 100);
usleep(4_000_000);

my $n0_local = drm_ctr($pair->node0, 'samples_local');
my $n0_remote = drm_ctr($pair->node0, 'samples_remote');
my $n1_local = drm_ctr($pair->node1, 'samples_local');
my $n1_remote = drm_ctr($pair->node1, 'samples_remote');

note("node0 local $n0_local0 -> $n0_local, remote $n0_remote0 -> $n0_remote");
note("node1 local $n1_local0 -> $n1_local, remote $n1_remote0 -> $n1_remote");

# L1/L3 — local admission hook fires: each node records its own requests for
# shards it masters.
cmp_ok($n0_local, '>', $n0_local0, 'L1 node0 samples_local increased (local admission hook)');
cmp_ok($n1_local, '>', $n1_local0, 'L3 node1 samples_local increased (local admission hook)');

# L2/L4 — remote admission hook fires: each node records the PEER's requests for
# shards it masters (current-master authoritative sees self + peer, Amend R2).
cmp_ok($n0_remote, '>', $n0_remote0, 'L2 node0 samples_remote increased (peer requests, R2 self+peer)');
cmp_ok($n1_remote, '>', $n1_remote0, 'L4 node1 samples_remote increased (peer requests, R2 self+peer)');

# L5 — internal consistency: recorded == local + remote on each node (no path
# double-counts or drops; INV-DRM9 pure accounting).
my $n0_rec = drm_ctr($pair->node0, 'samples_recorded');
is($n0_rec, $n0_local + $n0_remote, 'L5 node0 samples_recorded == local + remote');

$pair->stop_pair if $pair->can('stop_pair');

done_testing();
