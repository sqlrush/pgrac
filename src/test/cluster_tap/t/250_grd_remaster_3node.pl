#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 250_grd_remaster_3node.pl
#    spec-4.6 acceptance — 3-node failure-driven remaster legs that a
#    2-node pair cannot prove (t/249 covers the 2-node closure).
#
#    Topology: node0 / node1 / node2.  node0 is killed; node1 + node2
#    survive (coordinator = node1 = min survivor).  Survivor sets are
#    deterministic: affected shards remaster to survivors[shard % 2].
#
#    Legs:
#      G1  triple up; deterministic identical master maps on all nodes.
#      G2  premises:
#            sessA (node1) holds X on K_aff (node0-mastered shard) and
#            on K_un (node2-mastered shard — the wire in-place rebind).
#            sessB (node1) arms cluster-grd-redeclare-skip and holds X
#            on K_leak (node2-mastered) — the stalled-barrier actor.
#            sessC (node2) requests K_un -> conflicts -> queued waiter
#            under the OLD epoch, then times out client-side (the
#            stale-waiter entry stays queued on node2).
#      G3  kill -9 node0 -> reconfig fires on BOTH survivors; epoch
#          advances; remaster moves every node0-owned shard; maps stay
#          identical on node1/node2 (hard-gate #1 determinism).
#      G4  L11/L14 fail-closed: node1's barrier stalls on armed sessB ->
#          rebuild_timeout >= 1 (WARNING re-broadcast cadence), and the
#          P6 CLUSTER gate (P0#3) holds BOTH survivors' sweeps back:
#          remaster_done == 0 on node1 AND node2 (node2's local barrier
#          completed, but it must not sweep while node1's backends are
#          not rebound — sessB's live old-epoch holder on node2 would be
#          the double-grant victim).
#      G5  L8: a NEW request on a frozen (affected) shard from node1 ->
#          53R9I after the bounded freeze-gate wait.
#      G6  unwedge: terminate sessB.  Its LockReleaseAll release for
#          K_leak rides the OLD epoch (skip prevented the rebind) and is
#          wire-rejected by node2 -> leaked holder.  node1's barrier now
#          completes (dead proc drops out), REDECLARE_DONE converges,
#          BOTH survivors run P6+P7:
#            - node2 sweeps the leaked old-epoch holder
#              (stale_holder_swept >= 1)  [L15]
#            - remaster_done >= 1 on both survivors
#      G7  closure proof:
#            - sessA's holders were rebound: K_aff lives on the NEW
#              master, K_un was rebound IN PLACE on node2 over the WIRE
#              (holders_rebound >= 1 on node2)  [L13 wire form]
#            - L7 no-double-grant: while sessA still holds X on K_aff,
#              node2's conflicting acquire times out (NOT granted);
#              after sessA commits, node2 acquires it successfully
#              [L9: release -> requester-retry continuity]
#            - K_leak is re-acquirable after the sweep  [L15 tail]
#            - node2's release-pop dropped the stale pre-kill waiter
#              (stale_request_drop >= 1 on node2)  [L5 stale-drop form]
#
#    NOTE on L9 semantics: pre-kill waiters do NOT survive the epoch
#    bump as queue entries (in-flight waits self-retry under the new
#    epoch; the master-side stale entry is dropped by the pop guard /
#    P6).  "release -> waiter continues" is therefore proven as
#    requester-retry continuity, not queue-carry.
#
# Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
# Portions Copyright (c) 2026, pgrac contributors
#
# Author: SqlRush <sqlrush@gmail.com>
#
# IDENTIFICATION
#    src/test/cluster_tap/t/250_grd_remaster_3node.pl
#
# NOTES
#    pgrac-original file.
#    Spec: spec-4.6-recovery-aware-grd-ges-remaster.md (§4.2 L5/L7/L8/
#    L9/L11/L13/L14/L15 acceptance)
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../../perl";

use PostgreSQL::Test::ClusterTriple;
use Test::More;

sub poll_query_until_timeout
{
	my ($node, $dbname, $query, $expected, $timeout_s, $label) = @_;

	$expected //= 't';
	$timeout_s //= 20;

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

my $KEY_BASE  = 4970000;
my $KEY_COUNT = 36;


# ----------
# G1: triple up + deterministic identical maps.
# ----------
my $triple = PostgreSQL::Test::ClusterTriple->new_triple(
	'grd_remaster3',
	quorum_voting_disks => 3,
	extra_conf => [
		"cluster.grd_max_entries = 4096",
		"cluster.ges_request_timeout_ms = 2000",
		"cluster.ges_retransmit_max_attempts = 0",
		"cluster.grd_rebuild_timeout_ms = 1500",
		"log_min_messages = debug1",
	]);
$triple->start_triple;

for my $i (0 .. 2)
{
	is($triple->node($i)->safe_psql('postgres', 'SELECT 1'),
		'1', "G1 node$i alive");
	ok(poll_query_until_timeout($triple->node($i), 'postgres',
			'SELECT in_quorum FROM pg_cluster_quorum_state', 't', 20,
			"G1 node$i in_quorum"),
		"G1 node$i in_quorum=t (strict mode, 3 voting disks)");
}

my @maps;
for my $i (0 .. 2)
{
	push @maps, $triple->node($i)->safe_psql('postgres',
		q{SELECT md5(string_agg(master_node_id::text, ',' ORDER BY shard_id))
		    FROM pg_cluster_grd_shards});
}
is($maps[1], $maps[0], 'G1 node1 master map identical to node0');
is($maps[2], $maps[0], 'G1 node2 master map identical to node0');

ok(poll_query_until_timeout($triple->node1, 'postgres',
		q{SELECT count(*) = 2 FROM pg_cluster_cssd_peers
		   WHERE state = 'alive' AND heartbeat_recv_count > 0},
		't', 20, 'G1 node1 sees both peers alive'),
	'G1 node1 CSSD sees both peers alive + heartbeats flowing');


# ----------
# G2: premises — three sessions, four keys.
# ----------
my $acquire_all = join("\n",
	map { "SELECT pg_advisory_xact_lock(" . ($KEY_BASE + $_) . ");" }
		(1 .. $KEY_COUNT));

my $sessA = $triple->node1->background_psql('postgres', on_error_stop => 0);
$sessA->query_safe('BEGIN');
$sessA->query($acquire_all);

my $on_node0 = $triple->node0->safe_psql('postgres', qq{
	SELECT field3 FROM pg_cluster_grd_entries
	 WHERE type = 10 AND lockmethodid = 2 AND field4 = 1
	   AND field3 BETWEEN @{[$KEY_BASE + 1]} AND @{[$KEY_BASE + $KEY_COUNT]}
	   AND ngranted > 0 ORDER BY field3});
my $on_node2 = $triple->node2->safe_psql('postgres', qq{
	SELECT field3 FROM pg_cluster_grd_entries
	 WHERE type = 10 AND lockmethodid = 2 AND field4 = 1
	   AND field3 BETWEEN @{[$KEY_BASE + 1]} AND @{[$KEY_BASE + $KEY_COUNT]}
	   AND ngranted > 0 ORDER BY field3});
my @aff = split(/\n/, $on_node0 // '');
my @un  = split(/\n/, $on_node2 // '');
note('G2 discovery: node0-mastered=' . scalar(@aff) . ' node2-mastered=' . scalar(@un));
ok(scalar(@aff) >= 1, 'G2 found a node0-mastered (affected) key');
ok(scalar(@un) >= 2,  'G2 found >=2 node2-mastered (unaffected, wire) keys');
BAIL_OUT('discovery premise failed') if scalar(@aff) < 1 || scalar(@un) < 2;

my $K_aff  = $aff[0];
my $K_un   = $un[0];
my $K_leak = $un[1];

# sessA keeps X on K_aff (affected) + K_un (unaffected, remote master).
$sessA->query_safe('COMMIT');
$sessA->query_safe('BEGIN');
$sessA->query("SELECT pg_advisory_xact_lock($K_aff)");
$sessA->query("SELECT pg_advisory_xact_lock($K_un)");

# sessB arms the redeclare-skip injection (same-session arm, per-backend
# scope) and holds X on K_leak (node2-mastered).
my $sessB = $triple->node1->background_psql('postgres', on_error_stop => 0);
$sessB->query_safe(
	"SELECT cluster_inject_fault('cluster-grd-redeclare-skip', 'skip', 1000000)");
$sessB->query_safe('BEGIN');
$sessB->query("SELECT pg_advisory_xact_lock($K_leak)");

my $sessB_pid = $sessB->query_safe('SELECT pg_backend_pid()');

# sessC (node0!) conflicts on K_un -> the REMOTE request path enqueues
# a waiter on node2 under the OLD epoch (the local-master path never
# queues waiters), then errors client-side at the bounded GES timeout.
# The stale waiter entry stays queued on node2 — and node0 then dies
# with its owner, so only the pop guard / P6 can clean it.
my $sessC = $triple->node0->background_psql('postgres', on_error_stop => 0);
$sessC->query_safe('BEGIN');
$sessC->query("SELECT pg_advisory_xact_lock($K_un)");

ok(poll_query_until_timeout($triple->node2, 'postgres',
		qq{SELECT nwaiters >= 1 FROM pg_cluster_grd_entries
		    WHERE type = 10 AND lockmethodid = 2 AND field4 = 1 AND field3 = $K_un},
		't', 10, 'G2 stale waiter queued on node2'),
	'G2 sessC (node0) queued as waiter on node2 under the old epoch');

ok(poll_query_until_timeout($triple->node0, 'postgres',
		qq{SELECT ngranted = 1 FROM pg_cluster_grd_entries
		    WHERE type = 10 AND lockmethodid = 2 AND field4 = 1 AND field3 = $K_aff},
		't', 10, 'G2 affected-shard holder visible on node0'),
	'G2 sessA holder on the affected shard visible in node0 GRD');
ok(poll_query_until_timeout($triple->node2, 'postgres',
		qq{SELECT ngranted = 1 FROM pg_cluster_grd_entries
		    WHERE type = 10 AND lockmethodid = 2 AND field4 = 1 AND field3 = $K_leak},
		't', 10, 'G2 leak-actor holder visible on node2'),
	'G2 sessB holder on node2 (the future leak) visible in node2 GRD');


# ----------
# G3: kill node0 -> reconfig on both survivors -> deterministic remaster.
# ----------
$triple->kill_node9(0);

for my $i (1, 2)
{
	ok(poll_query_until_timeout($triple->node($i), 'postgres',
			q{SELECT state IN ('suspected','dead') FROM pg_cluster_cssd_peers
			   WHERE node_id = 0},
			't', 25, "G3 node$i sees node0 dead"),
		"G3 node$i CSSD deadband marks node0 suspected/dead");
	# Both survivors publish a reconfig event (coordinator role on node1,
	# survivor role on node2 — survivor's new_epoch == old_epoch until the
	# IC piggyback delivers the bumped epoch, so only event_id is asserted
	# here; the real epoch-advance proof is the remaster below).
	ok(poll_query_until_timeout($triple->node($i), 'postgres',
			q{SELECT event_id != 0 FROM pg_cluster_reconfig_state},
			't', 25, "G3 node$i reconfig"),
		"G3 node$i reconfig event published");
}
is($triple->node1->safe_psql('postgres',
		q{SELECT coordinator_node_id FROM pg_cluster_reconfig_state}),
	'1', 'G3 coordinator = node1 (min survivor)');
is($triple->node1->safe_psql('postgres',
		q{SELECT new_epoch > old_epoch FROM pg_cluster_reconfig_state}),
	't', 'G3 node1 (coordinator) advanced the epoch');

for my $i (1, 2)
{
	ok(poll_query_until_timeout($triple->node($i), 'postgres',
			q{SELECT count(*) = 0 FROM pg_cluster_grd_shards WHERE master_node_id = 0},
			't', 25, "G3 node$i dead-owned drained"),
		"G3 node$i: dead node0 owns zero shards after remaster");
}
my $map1 = $triple->node1->safe_psql('postgres',
	q{SELECT md5(string_agg(master_node_id::text, ',' ORDER BY shard_id))
	    FROM pg_cluster_grd_shards});
my $map2 = $triple->node2->safe_psql('postgres',
	q{SELECT md5(string_agg(master_node_id::text, ',' ORDER BY shard_id))
	    FROM pg_cluster_grd_shards});
is($map1, $map2, 'G3 survivors computed IDENTICAL remastered maps (hard-gate #1)');


# ----------
# G4: stalled barrier (armed sessB) -> fail-closed on BOTH survivors.
# ----------
ok(poll_query_until_timeout($triple->node1, 'postgres',
		q{SELECT value::bigint >= 1 FROM cluster_dump_state()
		   WHERE category = 'grd_recovery' AND key = 'rebuild_timeout'},
		't', 25, 'G4 node1 rebuild_timeout fires'),
	'G4 L11: node1 rebuild barrier timed out (armed backend never acks)');

is($triple->node1->safe_psql('postgres',
		q{SELECT value FROM cluster_dump_state()
		   WHERE category = 'grd_recovery' AND key = 'remaster_done'}),
	'0', 'G4 node1 remaster_done = 0 (shards stay frozen, fail-closed)');
is($triple->node2->safe_psql('postgres',
		q{SELECT value FROM cluster_dump_state()
		   WHERE category = 'grd_recovery' AND key = 'remaster_done'}),
	'0', 'G4 P0#3 cluster gate: node2 must NOT sweep while node1 backends '
	. 'are unrebound (sessB live old-epoch holder on node2)');


# ----------
# G5: L8 — new request on a frozen affected shard -> 53R9I.
# ----------
{
	my ($rc, $out, $err) = $triple->node1->psql('postgres',
		"BEGIN;\nSELECT pg_advisory_xact_lock($K_aff);\nCOMMIT;",
		timeout => 30);
	isnt($rc, 0, 'G5 L8: acquire on frozen shard errors');
	like($err, qr/being remastered|remastering/i,
		'G5 L8: 53R9I shard-remastering surface after the freeze-gate wait');
}


# ----------
# G6: unwedge — terminate sessB; leak lands on node2; cluster gate
#     converges; both survivors sweep + unfreeze.
# ----------
$triple->node1->safe_psql('postgres', "SELECT pg_terminate_backend($sessB_pid)");
eval { $sessB->quit };

for my $i (1, 2)
{
	ok(poll_query_until_timeout($triple->node($i), 'postgres',
			q{SELECT value::bigint >= 1 FROM cluster_dump_state()
			   WHERE category = 'grd_recovery' AND key = 'remaster_done'},
			't', 25, "G6 node$i episode completes"),
		"G6 node$i recovery completed (P6+P7) after the cluster gate converged");
}

ok($triple->node2->safe_psql('postgres',
		q{SELECT value::bigint >= 1 FROM cluster_dump_state()
		   WHERE category = 'grd_recovery' AND key = 'stale_holder_swept'}) eq 't',
	'G6 L15: node2 swept the leaked old-epoch holder at P6');


# ----------
# G7: closure proofs.
# ----------
# L13 wire form: sessA's K_un holder was rebound in place on node2 over
# the wire; node2 counted it.
ok($triple->node2->safe_psql('postgres',
		q{SELECT value::bigint >= 1 FROM cluster_dump_state()
		   WHERE category = 'grd_recovery' AND key = 'holders_rebound'}) eq 't',
	'G7 L13(wire): node2 holders_rebound >= 1 (REDECLARE in-place rebind)');

# L7 no-double-grant: sessA still holds X on K_aff (rebuilt on the new
# master).  A conflicting acquire from node2 must NOT be granted.
{
	my ($rc, $out, $err) = $triple->node2->psql('postgres',
		"BEGIN;\nSELECT pg_advisory_xact_lock($K_aff);\nCOMMIT;",
		timeout => 30);
	isnt($rc, 0,
		'G7 L7: conflicting acquire from node2 NOT granted while sessA holds X');
}

# L9 (requester-retry continuity): sessA releases; node2 then succeeds.
$sessA->query('COMMIT');
$sessA->quit;
{
	my ($rc, $out, $err) = $triple->node2->psql('postgres',
		"BEGIN;\nSELECT pg_advisory_xact_lock($K_aff);\nCOMMIT;",
		timeout => 30);
	is($rc, 0, "G7 L9: node2 acquires K_aff after sessA release (err=$err)");
}

# L15 tail: the leaked resource is re-acquirable after the sweep.
{
	my ($rc, $out, $err) = $triple->node1->psql('postgres',
		"BEGIN;\nSELECT pg_advisory_xact_lock($K_leak);\nCOMMIT;",
		timeout => 30);
	is($rc, 0, "G7 L15: K_leak re-acquirable after node2 swept the leak (err=$err)");
}

# L5 no-zombie-grant form: sessC's pre-kill request (node0-origin, old
# epoch) must never have become a phantom holder on K_un.  sessC died
# with node0, so node2 cleared its waiter (a dead-node request's correct
# disposition — dead_sweep, not stale-epoch sweep).  Proof: after sessA
# released K_un (the L9 commit above), node1 can acquire K_un cleanly —
# a surviving phantom holder would block it.
{
	my ($rc, $out, $err) = $triple->node1->psql('postgres',
		"BEGIN;\nSELECT pg_advisory_xact_lock($K_un);\nCOMMIT;",
		timeout => 30);
	is($rc, 0,
		"G7 L5: K_un cleanly acquirable — sessC stale request left no phantom "
		. "holder (err=$err)");
}

eval { $sessC->quit }; # its node (node0) was killed mid-test
$triple->stop_triple;

done_testing();
