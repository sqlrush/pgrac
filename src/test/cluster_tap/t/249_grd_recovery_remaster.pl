#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 249_grd_recovery_remaster.pl
#    spec-4.6 D0 — GRD/GES failure-driven remaster gap-pin TAP
#    (measure-first: pin the CURRENT recovery hole, flip legs to the
#    fixed behaviour as D2-D4 land).
#
#    Closure target under test (spec-4.6 hard DoD):  when the master
#    node of a GRD shard dies, the surviving node must (a) remaster the
#    shard to a survivor, (b) keep / rebind surviving holders, (c) be
#    able to issue NEW requests against the remastered resource.  None
#    of this exists today: the master map is statically frozen at LMON
#    init (cluster_grd_master_map_refresh is a Stage-6 DRM no-op
#    placeholder), so a dead node owns its shards forever.
#
#    Leg map (originally measure-first XFAIL pins of the pre-4.6 gap;
#    legs flip to the fixed expectation as each deliverable lands —
#    git history of this file documents the pinned broken behaviour):
#      L0  strict-mode ClusterPair (3 voting disks) + master map sanity:
#          both nodes agree on a 2-way round-robin master map.
#      L1  PREMISE: node1 acquires xact-advisory locks; discovery finds
#          keys whose GRD entry lives on node0 (= node0-mastered,
#          remote-granted).  One key stays HELD by a live node1 backend.
#      L2  kill -9 node0 postmaster → CSSD deadband DEAD edge →
#          reconfig coordinator fires on node1 (epoch advances).
#          Substrate legs — these must PASS both before and after 4.6.
#      L3  (D2+D1 flipped) failure-driven remaster moves every shard of
#          the dead node to the survivor; dead node owns 0 shards.
#          Pinned gap was: map statically frozen, dead node owns 2048.
#      L6  (D3) the surviving holder is rebuilt on the NEW master via
#          the cooperative redeclare (idle-in-transaction backend
#          reached through DoingCommandRead CHECK_FOR_INTERRUPTS).
#      L4  (D2-D4 flipped) a NEW request for a previously-dead-mastered
#          resource succeeds.  Pinned gap was: bounded GES wait then
#          FAIL_INTERNAL(16) via the stale S4 reject mapping.
#      L13 (D3/P0#3, 2-node form) a holder on an UNAFFECTED shard
#          (master survived) is rebound IN PLACE — the epoch bump
#          staled its identity too; holders_rebound +
#          unaffected_holder_survived counters fire.
#      L5  (D3 flipped) the rebound holders release against their
#          current-epoch masters and both resources are immediately
#          re-acquirable.  Pinned gap was: release silently swallowed.
#      L7  (D5 flipped) grd_recovery dump category: 31 keys (spec-4.6
#          §2.4 base 13 + spec-5.16 D5 join-direction 5 + spec-4.6a
#          liveness/episode surface 13), full key roster, episode trail
#          asserted (remaster_started/done, shards_remastered=2048,
#          holders_redeclared>=1, rebuild_timeout=0).
#
#    Discipline notes:
#      - poll_query_until_timeout pattern + producer-before-actor
#        ordering copied from 099_reconfig_actor.pl (L99 lesson).
#      - kill -9 targets the POSTMASTER here (unlike 099's SIGSTOP of
#        CSSD): 4.6's scenario is real node death; CSSD dies with the
#        postmaster (WL_EXIT_ON_PM_DEATH) so the heartbeat producer
#        stops and the survivor's deadband fires the DEAD edge.
#      - cluster.ges_request_timeout_ms is shortened so dead-master
#        request/release waits stay TAP-friendly (default 60 s).
#      - xact-level advisory locks (pg_advisory_xact_lock) enter the
#        cluster GES gate; session-level stays native (HC11) — so all
#        legs use the xact form inside open transactions.
#
# Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
# Portions Copyright (c) 2026, pgrac contributors
#
# Author: SqlRush <sqlrush@gmail.com>
#
# IDENTIFICATION
#    src/test/cluster_tap/t/249_grd_recovery_remaster.pl
#
# NOTES
#    pgrac-original file.
#    Spec: spec-4.6-recovery-aware-grd-ges-remaster.md (D0 gap-pin)
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../../perl";

use PostgreSQL::Test::ClusterPair;
use Test::More;

sub poll_query_until_timeout
{
	my ($node, $dbname, $query, $expected, $timeout_s, $label) = @_;

	$expected //= 't';
	$timeout_s //= 15;

	my $deadline = time + $timeout_s;
	my $last = '';

	while (time < $deadline)
	{
		$last = $node->safe_psql($dbname, $query);
		return 1 if defined $last && $last eq $expected;
		select(undef, undef, undef, 0.1);
	}

	diag("$label timed out after ${timeout_s}s; expected=$expected; last=$last");
	return 0;
}

# Advisory key window for this test.  Keys < 2^32 land entirely in
# resid field3 (field2 = 0, field4 = 1, lockmethodid = USER = 2,
# type = LOCKTAG_ADVISORY = 10).
my $KEY_BASE  = 4960000;
my $KEY_COUNT = 24;


# ----------
# L0: strict-mode pair + GES timeout bounding + master map sanity.
# ----------
my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'grd_remaster',
	quorum_voting_disks => 3,
	extra_conf => [
		# Opt into the cluster GES lock path: the lock.c gate requires
		# cluster.grd_max_entries > 0 (default 0 = skeleton mode, gate
		# off entirely; see lock.c cluster gate predicate).
		"cluster.grd_max_entries = 4096",
		# Bound dead-master request/release waits (default 60 s), and
		# disable retransmit so (a) healthy acquires wait the full
		# deadline instead of aborting after one 100 ms backoff tick,
		# (b) the retransmit starvation WARNINGs (guarded by
		# max_attempts > 0) stay out of psql stderr.
		"cluster.ges_request_timeout_ms = 3000",
		"cluster.ges_retransmit_max_attempts = 0",
		# L3 ordering assertions scrape the DEBUG1 phase trail.
		"log_min_messages = debug1",
	]);
$pair->start_pair;

is($pair->node0->safe_psql('postgres', 'SELECT 1'),
	'1', 'L0 node0 postmaster alive');
is($pair->node1->safe_psql('postgres', 'SELECT 1'),
	'1', 'L0 node1 postmaster alive');

ok(poll_query_until_timeout($pair->node0, 'postgres',
		'SELECT in_quorum FROM pg_cluster_quorum_state', 't', 15,
		'L0 node0 in_quorum=t'),
	'L0 node0 in_quorum=t');
ok(poll_query_until_timeout($pair->node1, 'postgres',
		'SELECT in_quorum FROM pg_cluster_quorum_state', 't', 15,
		'L0 node1 in_quorum=t'),
	'L0 node1 in_quorum=t');

# 2-node master map: both masters present, deterministic round-robin,
# identical view from both nodes.
is($pair->node1->safe_psql('postgres',
		'SELECT count(DISTINCT master_node_id) FROM pg_cluster_grd_shards'),
	'2', 'L0 node1 sees 2 distinct shard masters (round-robin map)');
is($pair->node0->safe_psql('postgres',
		q{SELECT count(*) FROM pg_cluster_grd_shards WHERE master_node_id NOT IN (0, 1)}),
	'0', 'L0 node0 master map only contains declared nodes');

my $map_hash_0 = $pair->node0->safe_psql('postgres',
	q{SELECT md5(string_agg(master_node_id::text, ',' ORDER BY shard_id))
	    FROM pg_cluster_grd_shards});
my $map_hash_1 = $pair->node1->safe_psql('postgres',
	q{SELECT md5(string_agg(master_node_id::text, ',' ORDER BY shard_id))
	    FROM pg_cluster_grd_shards});
is($map_hash_0, $map_hash_1, 'L0 master map identical on both nodes');

# CSSD heartbeat substrate established (099 discipline) — otherwise the
# kill leg could "pass" because heartbeats never started.
ok(poll_query_until_timeout($pair->node1, 'postgres',
		q{SELECT state = 'alive' AND heartbeat_recv_count > 0
		    FROM pg_cluster_cssd_peers WHERE node_id = 0},
		't', 15, 'L0 node1 sees node0 CSSD alive + heartbeats flowing'),
	'L0 node1 sees node0 CSSD alive + heartbeats flowing');


# ----------
# L1: PREMISE — node1 holds a grant on a node0-mastered resource.
#
#    Discovery: acquire $KEY_COUNT xact-advisory locks from node1 in
#    one transaction; GRD entries materialise on whichever node masters
#    each key's shard.  Keys whose entry shows up on node0 are
#    node0-mastered (remote-granted to node1).  Release the discovery
#    batch, then re-acquire one discovered key in a session that stays
#    open across the kill (the surviving holder), and keep a second
#    discovered key FREE as the post-kill new-request probe.
# ----------
my $acquire_all = join("\n",
	map { "SELECT pg_advisory_xact_lock(" . ($KEY_BASE + $_) . ");" }
		(1 .. $KEY_COUNT));

my $discovery = $pair->node1->background_psql('postgres', on_error_stop => 0);
$discovery->query_safe('BEGIN');
# query (not query_safe): remote-master acquires may emit benign
# WARNINGs under load; query_safe dies on any stderr.
$discovery->query($acquire_all);

my $node0_keys = $pair->node0->safe_psql('postgres', qq{
	SELECT field3 FROM pg_cluster_grd_entries
	 WHERE type = 10 AND lockmethodid = 2 AND field4 = 1
	   AND field3 BETWEEN @{[$KEY_BASE + 1]} AND @{[$KEY_BASE + $KEY_COUNT]}
	   AND ngranted > 0
	 ORDER BY field3
});
my @n0 = split(/\n/, $node0_keys // '');
note("L1 discovery: node0-mastered held keys = " . scalar(@n0) . " of $KEY_COUNT");
ok(scalar(@n0) >= 2,
	'L1 discovery found >=2 node0-mastered advisory keys (remote-master grant path works)');

BAIL_OUT('L1 discovery found fewer than 2 node0-mastered keys; '
	. 'widen KEY_COUNT — cannot pin the gap without a node0-mastered resource')
	if scalar(@n0) < 2;

my $K_hold  = $n0[0];
my $K_probe = $n0[1];

# An UNAFFECTED-shard holder (node1-mastered key): exercises the
# in-place identity rebind (P0#3) — the epoch bump stales this holder's
# identity even though its master survives, so without the cluster-wide
# rebind its release would be rejected forever.
my $node1_keys = $pair->node1->safe_psql('postgres', qq{
	SELECT field3 FROM pg_cluster_grd_entries
	 WHERE type = 10 AND lockmethodid = 2 AND field4 = 1
	   AND field3 BETWEEN @{[$KEY_BASE + 1]} AND @{[$KEY_BASE + $KEY_COUNT]}
	   AND ngranted > 0
	 ORDER BY field3
});
my @n1 = split(/\n/, $node1_keys // '');
ok(scalar(@n1) >= 1, 'L1 discovery found >=1 node1-mastered (unaffected-shard) key');
BAIL_OUT('no node1-mastered key found') if scalar(@n1) < 1;
my $K_unaff = $n1[0];

# Shard + map cross-check for the held key's resource.
my $S_hold = $pair->node0->safe_psql('postgres', qq{
	SELECT shard_id FROM pg_cluster_grd_entries
	 WHERE type = 10 AND lockmethodid = 2 AND field4 = 1 AND field3 = $K_hold
});
ok($S_hold =~ /^\d+$/, "L1 captured shard for held key (K_hold=$K_hold shard=$S_hold)");
is($pair->node1->safe_psql('postgres',
		"SELECT master_node_id FROM pg_cluster_grd_shards WHERE shard_id = $S_hold"),
	'0', 'L1 node1 master map confirms shard S is node0-mastered');

# Release the discovery batch; re-acquire K_hold (dead-master shard)
# and K_unaff (unaffected shard) in the session that survives the kill.
$discovery->query_safe('COMMIT');
$discovery->query_safe('BEGIN');
$discovery->query("SELECT pg_advisory_xact_lock($K_hold)");
$discovery->query("SELECT pg_advisory_xact_lock($K_unaff)");

ok(poll_query_until_timeout($pair->node0, 'postgres',
		qq{SELECT ngranted = 1 FROM pg_cluster_grd_entries
		    WHERE type = 10 AND lockmethodid = 2 AND field4 = 1 AND field3 = $K_hold},
		't', 10, 'L1 node0 GRD entry shows the surviving holder'),
	'L1 node0 GRD entry shows ngranted=1 for the held key (premise pinned)');


# ----------
# L2: kill -9 node0 → DEAD edge → reconfig fires on node1 (substrate).
# ----------
$pair->kill_node9(0);
ok(1, 'L2 node0 postmaster killed with SIGKILL');

ok(poll_query_until_timeout($pair->node1, 'postgres',
		q{SELECT state IN ('suspected', 'dead')
		    FROM pg_cluster_cssd_peers WHERE node_id = 0},
		't', 20, 'L2 PRODUCER node1 sees node0 suspected/dead'),
	'L2 PRODUCER node1 CSSD deadband marks node0 suspected/dead');

ok(poll_query_until_timeout($pair->node1, 'postgres',
		q{SELECT event_id != 0 FROM pg_cluster_reconfig_state},
		't', 20, 'L2 ACTOR node1 reconfig event fired'),
	'L2 ACTOR node1 reconfig event fired (event_id != 0)');

is($pair->node1->safe_psql('postgres',
		q{SELECT new_epoch > old_epoch FROM pg_cluster_reconfig_state}),
	't', 'L2 epoch advanced on node1');
is($pair->node1->safe_psql('postgres',
		q{SELECT observer_role FROM pg_cluster_reconfig_state}),
	'coordinator', 'L2 node1 is reconfig coordinator (min survivor)');
ok(poll_query_until_timeout($pair->node1, 'postgres',
		'SELECT in_quorum FROM pg_cluster_quorum_state', 't', 15,
		'L2 node1 keeps quorum (1 node + voting disk majority)'),
	'L2 node1 keeps quorum after node0 death');


# ----------
# L3 (FLIPPED by D2+D1): failure-driven remaster moves every dead-owned
#     shard to the survivor.  Poll: the LMON recovery sequence (P0-P7)
#     runs within a tick or two of the reconfig event.
# ----------
ok(poll_query_until_timeout($pair->node1, 'postgres',
		"SELECT master_node_id = 1 FROM pg_cluster_grd_shards WHERE shard_id = $S_hold",
		't', 20, 'L3 shard S remastered to survivor node1'),
	'L3 shard S remastered from dead node0 to survivor node1 (D2 flip)');

is($pair->node1->safe_psql('postgres',
		q{SELECT count(*) FROM pg_cluster_grd_shards WHERE master_node_id = 0}),
	'0', 'L3 dead node0 owns zero shards after remaster (D2 flip)');


# ----------
# L6-core (D3): the surviving holder was rebound — its grant lives in
#     node1's own GRD entry (the new master) with ngranted=1.  The bg
#     session is idle in an open transaction; the cooperative redeclare
#     reaches it via the DoingCommandRead CHECK_FOR_INTERRUPTS path.
# ----------
ok(poll_query_until_timeout($pair->node1, 'postgres',
		qq{SELECT ngranted = 1 FROM pg_cluster_grd_entries
		    WHERE type = 10 AND lockmethodid = 2 AND field4 = 1 AND field3 = $K_hold},
		't', 20, 'L6 surviving holder rebuilt on new master'),
	'L6 surviving holder rebuilt on the new master via cooperative rebind (D3)');


# ----------
# Episode completion gate: P7 unfreeze reached (remaster_done >= 1).
#     The freeze gate would absorb a sub-200ms window, but the LMON
#     tick cadence is not contractual — wait for the explicit signal.
# ----------
ok(poll_query_until_timeout($pair->node1, 'postgres',
		q{SELECT value::bigint >= 1 FROM cluster_dump_state()
		   WHERE category = 'grd_recovery' AND key = 'remaster_done'},
		't', 20, 'episode completion (remaster_done >= 1)'),
	'recovery episode completed through P7 unfreeze (remaster_done >= 1)');


# ----------
# L3-order (D1): the DEBUG1 trail shows the P-sequence in order on the
#     survivor: P1 freeze -> P4 remaster -> P5 redeclare broadcast ->
#     local barrier done -> P6 sweep + P7 unfreeze.
# ----------
{
	my $log = PostgreSQL::Test::Utils::slurp_file($pair->node1->logfile);
	my @needles = (
		'cluster_grd_recovery: P1 freeze',
		'cluster_grd_recovery: P4 remaster moved',
		'cluster_grd_recovery: P5 redeclare gen',
		'cluster_grd_recovery: local barrier done',
		'cluster_grd_recovery: cluster gate passed; P6 swept',
	);
	my $pos = 0;
	my $in_order = 1;
	for my $needle (@needles)
	{
		my $found = index($log, $needle, $pos);
		if ($found < 0) { $in_order = 0; diag("L3 missing/out-of-order: $needle"); last; }
		$pos = $found;
	}
	ok($in_order, 'L3 P-sequence appears in order in the survivor log (D1 ordering)');
}


# ----------
# L4 (FLIPPED by D2-D4): a NEW request for the previously-dead-mastered
#     probe key now succeeds — the shard is locally mastered by node1.
# ----------
my ($rc4, $out4, $err4) = $pair->node1->psql('postgres',
	"BEGIN;\nSELECT pg_advisory_xact_lock($K_probe);\nCOMMIT;",
	timeout => 60);
is($rc4, 0,
	"L4 new acquire of remastered resource succeeds (was: dead-master timeout; err=$err4)");


# ----------
# L13 (2-node form, P0#3): the UNAFFECTED-shard holder was rebound IN
#     PLACE — its master (node1) survived, but the epoch bump staled
#     the stored identity; without the cluster-wide rebind its release
#     would be rejected by inbound validation forever.
# ----------
ok(poll_query_until_timeout($pair->node1, 'postgres',
		q{SELECT value::bigint >= 1 FROM cluster_dump_state()
		   WHERE category = 'grd_recovery' AND key = 'holders_rebound'},
		't', 20, 'L13 in-place rebind happened'),
	'L13 unaffected-shard holder rebound in place (holders_rebound >= 1)');
is($pair->node1->safe_psql('postgres',
		q{SELECT value::bigint >= 1 FROM cluster_dump_state()
		   WHERE category = 'grd_recovery' AND key = 'unaffected_holder_survived'}),
	't', 'L13 unaffected_holder_survived counter fired');


# ----------
# L5 (FLIPPED by D3): the rebound holder releases against the NEW
#     master (rebind made cluster_holder_raw current-epoch, so the
#     release is not rejected), and BOTH resources are immediately
#     re-acquirable by a fresh session (affected + unaffected shard).
# ----------
$discovery->query('COMMIT');
ok(1, 'L5 holder COMMIT completes (releases land on current-epoch masters)');
$discovery->quit;

my ($rc5, $out5, $err5) = $pair->node1->psql('postgres',
	"BEGIN;\nSELECT pg_advisory_xact_lock($K_hold);\nSELECT pg_advisory_xact_lock($K_unaff);\nCOMMIT;",
	timeout => 60);
is($rc5, 0,
	"L5 re-acquire of both released resources succeeds post-rebind (err=$err5)");


# ----------
# L7 (FLIPPED by D5; spec-5.16 D5 +5 join-direction counters; spec-4.6a
#     +13 episode/liveness keys): grd_recovery dump category exposes 31
#     keys and the recovery episode left the expected trail.
# ----------
is($pair->node1->safe_psql('postgres',
		q{SELECT count(*) FROM cluster_dump_state() WHERE category = 'grd_recovery'}),
	'32',
	'L7 grd_recovery dump category exposes 32 keys (spec-4.6 D5 + spec-5.16 join + spec-4.6a + crash-rejoin barrier)');

is($pair->node1->safe_psql('postgres', q{
	SELECT string_agg(key, ',' ORDER BY key) FROM cluster_dump_state()
	 WHERE category = 'grd_recovery'}),
	'block_path_failclosed,block_redeclare_cursor,block_redeclare_done,'
	. 'block_redeclare_epoch,cluster_gate_timeout,converts_requeued,'
	. 'done_self_bitmap_hash,done_self_epoch,episode_epoch,event_coordinator,'
	. 'event_old_epoch,holders_rebound,holders_redeclared,'
	. 'join_block_recovering_failclosed,join_block_views_rebuilt,'
	. 'join_remaster_done,join_remaster_started,join_shards_remastered,'
	. 'last_event_id,offpath_crash_rejoin_fenced,rebuild_timeout,remaster_done,remaster_failed,'
	. 'remaster_started,shards_remastered,stale_holder_swept,'
	. 'stale_request_drop,state,state_enum_value,unaffected_holder_survived,'
	. 'wait_epoch_escape,waiters_requeued',
	'L7 grd_recovery key roster (spec-4.6 §2.4 + spec-5.16 D5 + spec-4.6a)');

is($pair->node1->safe_psql('postgres',
		q{SELECT value::bigint >= 1 FROM cluster_dump_state()
		   WHERE category = 'grd_recovery' AND key = 'remaster_started'}),
	't', 'L7 remaster_started >= 1');
is($pair->node1->safe_psql('postgres',
		q{SELECT value::bigint >= 1 FROM cluster_dump_state()
		   WHERE category = 'grd_recovery' AND key = 'remaster_done'}),
	't', 'L7 remaster_done >= 1 (P7 reached)');
is($pair->node1->safe_psql('postgres',
		q{SELECT value FROM cluster_dump_state()
		   WHERE category = 'grd_recovery' AND key = 'shards_remastered'}),
	'2048', 'L7 shards_remastered = 2048 (every dead-owned shard moved)');
is($pair->node1->safe_psql('postgres',
		q{SELECT value::bigint >= 1 FROM cluster_dump_state()
		   WHERE category = 'grd_recovery' AND key = 'holders_redeclared'}),
	't', 'L7 holders_redeclared >= 1 (rebuild insert on the new master)');
is($pair->node1->safe_psql('postgres',
		q{SELECT value FROM cluster_dump_state()
		   WHERE category = 'grd_recovery' AND key = 'rebuild_timeout'}),
	'0', 'L7 rebuild_timeout = 0 (barrier completed in time)');


$pair->stop_pair;

done_testing();
