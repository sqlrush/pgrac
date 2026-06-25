#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 251_gcs_pcm_warm_recovery.pl
#    spec-4.7 D0 — GCS/PCM warm recovery gap-pin TAP (measure-first).
#
#    The block layer (PCM lock state machine spec-2.30+, GCS block
#    protocol spec-2.33-2.37) keeps ALL its coordination state in shmem
#    (GrdEntry: master_state / x_holder_node / s_holders_bitmap /
#    pi_watermark_lsn, cluster_pcm_lock.c).  A node death or a clean
#    restart loses that state with NO rebuild protocol, so warm recovery
#    of the block path is blocked today.
#
#    MEASURE-FIRST FINDING (recorded honestly — L77/L239).  Three gap
#    signatures were proposed (spec-4.7 §4.2):
#      (a) restart-rejected  "master rejected transition_id"
#      (b) dead-master       53R9K block-path-not-rebuilt
#      (c) retransmit budget 53R90
#    Running this test against the shipped spec-4.7a binary shows only (a)
#    reproduces deterministically e2e.  (b)/(c) are NOT reproducible with
#    the current harness primitives, for two independent reasons:
#      - hold-until-revoked (spec-4.7a, cluster.gcs_block_local_cache on):
#        a block held S/X stays cached locally after a peer master dies,
#        and cluster_pcm_mode_covers short-circuits the acquire BEFORE any
#        master lookup -> the dead-master guard is never reached.
#      - restart-resets-deadband: to clear a node's held state you restart
#        it, but a fresh restart resets CSSD death detection — the just-
#        restarted survivor optimistically sees the (actually dead) peer
#        as alive/connected (observed: cssd=alive ic=connected, HELLO sent
#        state CONNECTED), so cluster_cssd_get_peer_state(master) != DEAD
#        and the 53R9K guard does not fire.
#    The 53R9K guard (cluster_gcs_block.c:555) only fires in the narrow
#    window "not-held block AND survivor already converged on peer-DEAD
#    (not reset by a restart)", which needs controlled eviction or a
#    death-injection point that the current harness lacks.
#
#    Per the L239 re-scope (user 2026-06-13): this measure-first TAP pins
#    the deterministic gaps (a) + observability as XFAIL/RED; signatures
#    (b)/(c) are deferred to the D1-D5 acceptance legs, where the new
#    RECOVERING / redo-before-unfreeze state machine provides controllable
#    states (a block resource can be put into RECOVERING / before-redo-
#    boundary deterministically, which the raw 4.6 dead-master guard
#    cannot).  Their decision logic is proven at cluster_unit
#    (test_cluster_gcs_recovery, D1-D3).  spec-4.7 §4.2 / §7 DoD reconciled.
#
#    Topology (ClusterTriple, shared_data + 3 voting disks):
#      node0 owns the user relation and is the only holder of its blocks.
#      node1 / node2 are master-ONLY peers (never holders) for the ~2/3 of
#      warm_t's blocks the BufferTag hash masters away from node0.  node0's
#      single-node DML legitimately acquires S/X on peer-mastered blocks
#      (the peer grants as master; NO second holder, so NO spec-2.36
#      writer-transfer — healthy, confirmed by the L0 probe).  The
#      restart-rejected signature surfaces on the SAME blocks across a
#      holder restart with no live concurrent same-block contention
#      anywhere (L210: deterministic, no timing-window race asserted).
#      3 nodes (vs the pair) so the D1-D5 acceptance legs can later add the
#      cross-survivor / not-double-X (L7) scenarios on the same harness.
#
#    Leg map (D0 pins TODAY's broken behaviour;  D1-D6 flip / add legs,
#    spec-4.7 §4.2):
#      L0   strict triple (shared_data + 3 voting disks); node0 owns +
#           holds a GCS-tracked user table; healthy read+write probe ok.
#      L1   (sig a / D3 flip) node0 clean restart -> first re-access to a
#           previously-held peer-mastered block trips "master rejected
#           transition_id".  D3 flips: master_state rebuilt, access works.
#      L1b  (P1#3 / D1+D3 flip) two concurrent first re-accesses are both
#           unserved today (no idempotent lazy rebuild).  D1/D3 flip: one
#           idempotent rebuild serves both (block_state_rebuilt += 1).
#      L2/L3 (sig b/c) DEFERRED to D1-D5 acceptance + cluster_unit (L239,
#           see finding above).  SKIPed here with reason.
#      Lobs (D6/observability, FLIPPED) gcs_recovery dump category exposes the
#           8 warm-recovery counters under category='gcs_recovery'.
#
# Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
# Portions Copyright (c) 2026, pgrac contributors
#
# Author: SqlRush <sqlrush@gmail.com>
#
# IDENTIFICATION
#    src/test/cluster_tap/t/251_gcs_pcm_warm_recovery.pl
#
# NOTES
#    pgrac-original file.
#    Spec: spec-4.7-gcs-pcm-warm-recovery.md (D0 gap-pin)
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../../perl";

use PostgreSQL::Test::ClusterTriple;
use Test::More;
use Time::HiRes qw(usleep);


# ----------
# L0: strict triple + shared data backend (user-relation blocks route
#     through PCM/GCS) + 3 voting disks (cross-node CSSD quorum; the
#     D1-D5 acceptance legs reuse the kill/cross-survivor path).  node0
#     owns + holds the table; node1/node2 are master-only peers for ~2/3
#     of its blocks.
# ----------
my $triple = PostgreSQL::Test::ClusterTriple->new_triple(
	'gcs_warm',
	quorum_voting_disks => 3,
	shared_data         => 1,
	extra_conf          => [
		# NOTE: do NOT set cluster.grd_max_entries (opens the GES
		# logical-lock path -> CREATE TABLE DDL "cluster lock acquire
		# timeout").  The block (PCM) path is gated independently by
		# cluster_pcm_is_active() + cluster_bufmgr_should_pcm_track().
		'autovacuum = off',
	]);
$triple->start_triple;

# tier1 mesh may retry once before peers' listeners are bound; let the IC
# peers settle so L0's first GCS round-trip is not raced (t/111 pattern).
usleep(3_000_000);

is($triple->node0->safe_psql('postgres', 'SELECT 1'), '1', 'L0 node0 alive');
is($triple->node1->safe_psql('postgres', 'SELECT 1'), '1', 'L0 node1 alive');
is($triple->node2->safe_psql('postgres', 'SELECT 1'), '1', 'L0 node2 alive');
ok($triple->wait_for_peer_state(0, 1, 'connected', 30),
	'L0 node0 sees node1 connected');
ok($triple->wait_for_peer_state(0, 2, 'connected', 30),
	'L0 node0 sees node2 connected');

# node0 owns a GCS-tracked user table (permanent, relNumber >=
# FirstNormalObjectId; shared_data routes its blocks through PCM/GCS).
# 50k rows span enough heap+index pages that the BufferTag hash masters
# some on node1/node2 -> node0 becomes a registered holder at those peers.
$triple->node0->safe_psql('postgres', q{
	CREATE TABLE warm_t (id int primary key, v int);
	INSERT INTO warm_t SELECT g, g FROM generate_series(1, 50000) g;
});
is($triple->node0->safe_psql('postgres', 'SELECT count(*) FROM warm_t'),
	'50000',
	'L0 node0 owns + holds the GCS-tracked table (healthy read probe)');
is($triple->node0->safe_psql('postgres',
		'UPDATE warm_t SET v = v + 1 WHERE id = 1 RETURNING v'),
	'2',
	'L0 node0 healthy single-node write probe (peer-as-master, no contention)');


# ----------
# L1 (sig a): node0 clean restart, then re-access previously-held blocks.
#     master_state on the peer master retains node0 as a holder; node0's
#     fresh N->S re-request is rejected as an illegal transition.
# ----------
$triple->node0->safe_psql('postgres', 'CHECKPOINT');
$triple->node0->restart;
ok($triple->wait_for_peer_state(0, 1, 'connected', 30),
	'L1 node0 reconnects to node1 after restart');

my ($rc1, $out1, $err1) = $triple->node0->psql('postgres',
	'SELECT count(*) FROM warm_t', timeout => 30);
isnt($rc1, 0,
	'L1 measure-first gap pinned: post-restart re-access to a previously-held '
	. 'block errors (PCM holder state lost; no rebuild protocol; D3 flips)');
like($err1,
	qr/master rejected transition_id|GCS|PCM|block|recovery|remaster|not.*holder/i,
	"L1 measure-first gap pinned: block-layer failure signature (err=$err1)");


# ----------
# L1b (P1#3): two concurrent first re-accesses post-restart.  Today
#     neither is served (no idempotent lazy rebuild).  D1/D3 flip: a
#     single rebuild serves both (block_state_rebuilt increments once).
# ----------
my $sA = $triple->node0->background_psql('postgres', on_error_stop => 0);
my $sB = $triple->node0->background_psql('postgres', on_error_stop => 0);
my $rA = $sA->query('SELECT count(*) FROM warm_t');
my $rB = $sB->query('SELECT count(*) FROM warm_t');
my $both_ok = ($rA =~ /\b50000\b/ && $rB =~ /\b50000\b/);
ok(!$both_ok,
	'L1b measure-first gap pinned: concurrent post-restart first-access not '
	. 'served (no idempotent lazy rebuild; D1/D3 flip: both succeed, once)');
eval { $sA->quit };
eval { $sB->quit };


# ----------
# L2/L3 (sig b: dead-master 53R9K / sig c: retransmit budget 53R90).
#     DEFERRED to the D1-D5 acceptance legs + cluster_unit (L239 re-scope,
#     user 2026-06-13).  See the measure-first finding in the file header:
#     post-4.7a these are not reproducible as a deterministic e2e here
#     (hold-until-revoked masks held blocks; a survivor restart resets the
#     deadband so the corpse is seen alive and the 53R9K guard never
#     fires).  The decision logic is proven deterministically at
#     cluster_unit (test_cluster_gcs_recovery); the real e2e flip lands
#     once D1-D5 introduce the RECOVERING / redo-before-unfreeze states,
#     which can be driven into the not-rebuilt / before-redo-boundary
#     condition deterministically.
# ----------
SKIP: {
	skip "sig b/c (dead-master 53R9K, retransmit-budget 53R90) not "
		. "deterministically reproducible e2e against the 4.7a baseline "
		. "(hold-until-revoked masks held blocks; survivor restart resets "
		. "death detection -> corpse seen alive -> guard not reached). "
		. "Decision logic proven at cluster_unit; e2e flip lands with the "
		. "D1-D5 RECOVERING/redo-gate machinery. spec-4.7 §4.2 L2/L3 + L239.",
		2;

	fail('L2 dead-master read fail-closed (unreached: deferred)');
	fail('L3 dead-master write fail-closed (unreached: deferred)');
}


# ----------
# Lobs (D6/observability): no 'gcs_recovery' dump category yet (D6 flips:
#     8 warm-recovery counters: block_resources_recovering / buffers_redeclared /
#     block_state_rebuilt / redo_boundary_waits / redo_boundary_reached /
#     stale_block_drop / ambiguous_owner_failclosed / before_boundary_failclosed).
#     spec-2.41 D7 adds 2 redo-coverage serve-gate counters to the same category
#     (redo_coverage_required_lsn_zero_count / redo_coverage_gate_block_count) → 10.
# ----------
is($triple->node0->safe_psql('postgres',
		q{SELECT count(*) FROM cluster_dump_state()
		    WHERE category = 'gcs_recovery'}),
	'10',
	'Lobs (D6 flipped + spec-2.41 D7): gcs_recovery dump category exposes 10 '
	. 'counters — 8 warm-recovery (block_resources_recovering / buffers_redeclared / '
	. 'block_state_rebuilt / redo_boundary_waits / redo_boundary_reached / '
	. 'stale_block_drop / ambiguous_owner_failclosed / before_boundary_failclosed) '
	. '+ 2 redo-coverage serve-gate (redo_coverage_required_lsn_zero_count / '
	. 'redo_coverage_gate_block_count)');


$triple->stop_triple;
done_testing();
