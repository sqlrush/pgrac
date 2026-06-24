#-------------------------------------------------------------------------
#
# 297_ko_object_flush.pl
#    spec-5.7 D6 (§3.5/§3.6) -- the KO (object-reuse flush) cross-node barrier.
#
#    Before a relation's storage is physically removed/truncated, every alive
#    peer must flush + drop the relfilenode's buffers and acknowledge, or the
#    dropping node fails closed (53RAA) -- otherwise a peer's stale dirty buffer
#    could be written back after the file is unlinked, recreating the file or
#    corrupting a reused relfilenode (8.A).  The correctness gate is APPLY-AFTER-
#    DROP: a peer ACKs ONLY after it has really dropped the buffers (KO-M6).
#
#    What this drives (REAL cross-node protocol via cluster_ko_flush_probe):
#      L1 2-node start; dump the KO counter baseline on both nodes.
#      L2 HAPPY PATH (REAL fanout + apply-after-drop ACK): node0 runs the barrier
#         on a synthetic relfilenode -> a REAL KO_FLUSH is fanned out to node1 ->
#         node1's SI Broadcaster aux REALLY flushes+drops + ACKs -> the probe
#         returns 'ok'.  Asserted via the real counters: node0 ko.flush_count and
#         ko.ack_received_count advance; node1 ko.peer_apply_count advances (the
#         peer really applied).
#      L3 FAIL-CLOSED (apply-after-drop, 8.A): arm cluster-ko-peer-skip-ack:skip
#         on node1 so its aux applies the drop but does NOT ACK; node0's barrier
#         then times out -> 53RAA.  Asserted: the probe raises 53RAA, node0
#         ko.failclosed_count advances, and node1 ko.peer_apply_count STILL
#         advances (the peer applied -- it just withheld the ACK; this is the
#         apply-after-drop discipline: an applied-but-unacked peer must NOT let
#         the dropping node proceed).
#
#    RECORDED e2e GAP (honest): the relfilenode is synthetic (no real buffers on
#    the peer, so the peer's flush+drop is a no-op), because a real SHARED
#    relation visible on BOTH nodes needs cross-node catalog OID coherence
#    (Stage-3 -- the same wall the DL/IR/TT 2-node tests record).  The barrier
#    MACHINERY is fully real here: real IC fanout to the real peer, the real
#    peer-side SI-Broadcaster-aux drop, and the real ACK round trip / timeout.
#
#    Harness: ClusterPair shared_data + 3 voting disks + autovacuum off.
#
# Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
# Portions Copyright (c) 2026, pgrac contributors
#
# Author: SqlRush <sqlrush@gmail.com>
#
# IDENTIFICATION
#    src/test/cluster_tap/t/297_ko_object_flush.pl
#
# NOTES
#    pgrac-original file.
#    Spec: spec-5.7-misc-enqueue-classes.md (D6, §3.5/§3.6)
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../../perl";

use PostgreSQL::Test::ClusterPair;
use Test::More;
use Time::HiRes qw(usleep);

my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'ko_flush',
	quorum_voting_disks => 3,
	shared_data         => 1,
	extra_conf          => [
		'autovacuum = off',
		'cluster.grd_max_entries = 1024',
		'cluster.cssd_heartbeat_interval_ms = 2000',
		'cluster.cssd_dead_deadband_factor = 10',
	]);
$pair->start_pair;
usleep(3_000_000);

my $n0 = $pair->node0;
my $n1 = $pair->node1;

# KO counter helper.
sub ko
{
	my ($node, $key) = @_;
	my $v = $node->safe_psql('postgres',
		"SELECT value::bigint FROM pg_cluster_state WHERE category='ko' AND key='$key'");
	return defined $v && $v ne '' ? $v : 0;
}

# A synthetic relfilenode triple for the barrier (no real buffers on the peer;
# the protocol still runs end to end).
my ($spc, $db, $rel) = (1663, 5, 91001);

# ============================================================
# L1: 2-node start; KO counter baseline present on both nodes.
# ============================================================
ok(defined ko($n0, 'flush_count'), 'L1 node0 KO counters present (ko category)');
ok(defined ko($n1, 'peer_apply_count'), 'L1 node1 KO counters present (ko category)');

# ============================================================
# L2: HAPPY PATH -- REAL cross-node fanout + apply-after-drop ACK.
# Retry until the barrier genuinely runs cross-node (the peer must be CSSD-alive
# for node0's alive-mask to include it; before that the probe is a native no-op).
# ============================================================
my $flush_before = ko($n0, 'flush_count');
my $ack_before   = ko($n0, 'ack_received_count');
my $apply_before = ko($n1, 'peer_apply_count');

my $ran = 0;
for my $try (1 .. 30)
{
	my ($rc, $out, $err) = $n0->psql('postgres',
		"SELECT cluster_ko_flush_probe($spc, $db, $rel + $try)", timeout => 60);
	is($rc, 0, "L2 probe attempt $try returns ok") if $try == 1;
	# the barrier ran cross-node once the peer actually applied a drop.
	if (ko($n1, 'peer_apply_count') > $apply_before)
	{
		$ran = 1;
		last;
	}
	usleep(1_000_000);
}

ok($ran, 'L2 the barrier ran cross-node (node1 really applied a flush+drop)');
ok(ko($n0, 'flush_count') > $flush_before,
	'L2 node0 ko.flush_count advanced (a barrier was initiated)');
ok(ko($n0, 'ack_received_count') > $ack_before,
	'L2 node0 ko.ack_received_count advanced (peer apply-after-drop ACK recorded)');
ok(ko($n1, 'peer_apply_count') > $apply_before,
	'L2 node1 ko.peer_apply_count advanced (peer flushed+dropped THEN acked)');

# ============================================================
# L3: FAIL-CLOSED -- peer applies the drop but withholds the ACK (8.A).
# Arm cluster-ko-peer-skip-ack:skip in node1's aux (GUC + reload, t/284 pattern),
# then probe -> node0's barrier waits cluster.sinval_ack_timeout_ms (default 5s)
# for an ACK that never comes -> fails closed 53RAA.
# ============================================================
$n1->safe_psql('postgres',
	"ALTER SYSTEM SET cluster.injection_points = 'cluster-ko-peer-skip-ack:skip'");
$n1->safe_psql('postgres', 'SELECT pg_reload_conf()');
usleep(500_000);    # let node1's aux pick up the reloaded GUC

my $fc_before    = ko($n0, 'failclosed_count');
my $apply_before2 = ko($n1, 'peer_apply_count');

my ($rc3, $o3, $e3) = $n0->psql('postgres',
	"SELECT cluster_ko_flush_probe($spc, $db, 92002)",
	timeout => 60);
isnt($rc3, 0, 'L3 probe with a peer that withholds the ACK is rejected');
like($e3, qr/53RAA|object_flush_unavailable|could not confirm every peer/i,
	'L3 KO barrier fails closed with 53RAA when a peer applies but does not ACK (KO-M6, 8.A)');

ok(ko($n0, 'failclosed_count') > $fc_before,
	'L3 node0 ko.failclosed_count advanced (the 53RAA fail-closed was recorded)');
ok(ko($n1, 'peer_apply_count') > $apply_before2,
	'L3 node1 ko.peer_apply_count advanced (the peer DID apply the drop -- it only withheld the ACK)');

# disarm so nothing lingers.
$n1->safe_psql('postgres', "ALTER SYSTEM SET cluster.injection_points = ''");
$n1->safe_psql('postgres', 'SELECT pg_reload_conf()');

$pair->stop_pair;
done_testing();
