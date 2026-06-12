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
#    Leg map (XFAIL-gap legs assert TODAY's broken behaviour and are
#    annotated "XFAIL-gap"; D2-D4 flip them to the fixed expectation —
#    same leg numbers, spec-4.6 §4.2):
#      L0  strict-mode ClusterPair (3 voting disks) + master map sanity:
#          both nodes agree on a 2-way round-robin master map.
#      L1  PREMISE: node1 acquires xact-advisory locks; discovery finds
#          keys whose GRD entry lives on node0 (= node0-mastered,
#          remote-granted).  One key stays HELD by a live node1 backend.
#      L2  kill -9 node0 postmaster → CSSD deadband DEAD edge →
#          reconfig coordinator fires on node1 (epoch advances).
#          Substrate legs — these must PASS both before and after 4.6.
#      L3  XFAIL-gap: master map on node1 STILL routes shard S to the
#          DEAD node0 (no failure-driven remaster).  D2 flips: master
#          becomes a survivor (node1).
#      L4  XFAIL-gap: a NEW request for a node0-mastered resource from
#          node1 fails (bounded GES timeout, SQLSTATE 55P03 today).
#          D2-D4 flip: acquire succeeds against the new master.
#      L5  XFAIL-gap: the surviving holder's release is silently
#          swallowed toward the dead master (commit completes, grant
#          state unrecoverable), and re-acquiring the SAME resource
#          fails like L4.  D3 flips: holder rebinds, release reaches
#          the new master, re-acquire succeeds.
#      L6  XFAIL-gap: no grd_recovery observability category exists.
#          D5 flips: 13 counters under category='grd_recovery'.
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

# Shard + map cross-check for the held key's resource.
my $S_hold = $pair->node0->safe_psql('postgres', qq{
	SELECT shard_id FROM pg_cluster_grd_entries
	 WHERE type = 10 AND lockmethodid = 2 AND field4 = 1 AND field3 = $K_hold
});
ok($S_hold =~ /^\d+$/, "L1 captured shard for held key (K_hold=$K_hold shard=$S_hold)");
is($pair->node1->safe_psql('postgres',
		"SELECT master_node_id FROM pg_cluster_grd_shards WHERE shard_id = $S_hold"),
	'0', 'L1 node1 master map confirms shard S is node0-mastered');

# Release the discovery batch; re-acquire only K_hold in the session
# that will survive the kill.
$discovery->query_safe('COMMIT');
$discovery->query_safe('BEGIN');
$discovery->query("SELECT pg_advisory_xact_lock($K_hold)");

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
# L3: XFAIL-gap — master map frozen: shard S still routed to DEAD node0.
#     D2 flips this leg to: master_node_id = 1 (deterministic survivor)
#     + master_generation bump.
# ----------
my $master_after = $pair->node1->safe_psql('postgres',
	"SELECT master_node_id FROM pg_cluster_grd_shards WHERE shard_id = $S_hold");
is($master_after, '0',
	'L3 XFAIL-gap pinned: master map still routes shard S to DEAD node0 '
	. '(cluster_grd_master_map_refresh is a no-op placeholder; D2 flips to survivor)');

my $dead_owned = $pair->node1->safe_psql('postgres',
	q{SELECT count(*) FROM pg_cluster_grd_shards WHERE master_node_id = 0});
ok($dead_owned > 0,
	"L3 XFAIL-gap pinned: dead node0 still owns $dead_owned shards on node1's map "
	. '(D2 flips to 0)');

note('spec-4.6 gap pinned: GRD master map is statically frozen at LMON init; '
	. 'a dead node owns its shards forever — the 4.6 closure target.');


# ----------
# L4: XFAIL-gap — NEW request for the node0-mastered probe key fails.
#     The GES request is routed to the dead master and dies after the
#     bounded GES wait.  Pinned surface today: "cluster_lock_acquire
#     failed (result=16)" = FAIL_INTERNAL, because the S4 reject-reason
#     mapping uses placeholder values (1/2/3) that do not match the
#     real GES_REJECT_REASON_* enum (TIMEOUT=4) — a pre-existing wart
#     owned by spec-4.6 D4 (the request-path SQLSTATE surface).
#     D2-D4 flip this leg to: acquire SUCCEEDS against the new master.
# ----------
my ($rc4, $out4, $err4) = $pair->node1->psql('postgres',
	"BEGIN;\nSELECT pg_advisory_xact_lock($K_probe);\nCOMMIT;",
	timeout => 60);
isnt($rc4, 0,
	'L4 XFAIL-gap pinned: new acquire of dead-master resource errors out '
	. '(D2-D4 flip: succeeds against remastered survivor)');
like($err4, qr/cluster lock acquire timeout|cluster_lock_acquire failed/,
	"L4 XFAIL-gap pinned: bounded GES wait then error (err=$err4)");


# ----------
# L5: XFAIL-gap — surviving holder cannot meaningfully release: the
#     release toward the dead master is swallowed after the bounded
#     wait (commit still completes — pinned), and the resource stays
#     unreachable (re-acquire fails like L4).  D3 flips: holder is
#     rebound to the new master during rebuild, release reaches the
#     new master, re-acquire succeeds.
# ----------
$discovery->query('COMMIT');
ok(1, 'L5 XFAIL-gap pinned: holder COMMIT completes (release toward dead '
	. 'master silently swallowed after bounded wait — leak by design today)');
$discovery->quit;

my ($rc5, $out5, $err5) = $pair->node1->psql('postgres',
	"BEGIN;\nSELECT pg_advisory_xact_lock($K_hold);\nCOMMIT;",
	timeout => 60);
isnt($rc5, 0,
	'L5 XFAIL-gap pinned: re-acquire of the previously-held resource fails '
	. '(master map still points at dead node; D3 flips: rebind + remaster '
	. 'make the resource reachable again)');


# ----------
# L6: XFAIL-gap — no grd_recovery observability yet (D5 flips: 13
#     counters under category='grd_recovery').
# ----------
is($pair->node1->safe_psql('postgres',
		q{SELECT count(*) FROM cluster_dump_state() WHERE category = 'grd_recovery'}),
	'0', 'L6 XFAIL-gap pinned: grd_recovery dump category absent (D5 adds 13 counters)');


$pair->stop_pair;

done_testing();
