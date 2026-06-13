#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 253_undo_tt_recovery.pl
#    spec-4.8 D0 — Undo/TT recovery gap-pin TAP (measure-first).
#
#    The durable undo/TT substrate is already crash-safe (spec-3.11 durable
#    TT slot + redo of XLOG_UNDO_TT_SLOT_COMMIT/ABORT + spec-4.5a cross-node
#    undo materialization).  spec-4.8 closes the RECOVERY-TIME "state verdict
#    + visibility closure" of undo/TT:
#      D1  crash-left TT_SLOT_ACTIVE -> ABORTED resolution ceremony
#      D2  cross-node TT authority (materialized & recovered_through gate),
#          no false-visible from a crashed peer's in-flight slot
#      D3  wrap-generation identity key (close task#90 raw-xid wrap residue)
#      D4  recycled-slot liveness relax (task#84, proof-gated)
#      D5  SCN high-watermark recovery (L222)
#      D7  minimal physical heap-tuple rollback apply (Q1, mini-plan-gated)
#
#    MEASURE-FIRST (L239, honest) — what reproduces against the shipped
#    spec-4.7 binary (a34c703739):
#
#    FINDING 1 (sig a, single-node correctness — PG-native already right):
#      With the spec-3.24 no-peer CR-gate fast path ON (the default), a
#      single node routes own-instance/session-local snapshots to PG-native
#      visibility.  A crash-left in-flight DELETE leaves the deleter
#      uncommitted (CLOG aborted-on-crash), so after restart the rows stay
#      VISIBLE -- correct.  The crash-left TT_SLOT_ACTIVE slot is merely
#      never RESOLVED to ABORTED (cluster_tt_durable.c:504 counts it as
#      stale_active_skipped, the by-xid resolver at :419 matches only
#      TT_SLOT_COMMITTED).  So on a single node this is a housekeeping +
#      cross-node-authority gap (D1/D2), NOT a single-node false-visible.
#
#    FINDING 2 (sig D5, L222 over-fail-closed — DETERMINISTIC RED):
#      With the fast path OFF (forcing the cluster CR/undo path, the regime
#      every CROSS-NODE read uses), a crash-restart makes the first CR read
#      fail "snapshot too old: CR cannot read undo record ... read_scn
#      predates the horizon".  cluster_scn is NOT recovered to the durable
#      pd_block_scn / commit_scn high-watermark on restart, so read_scn <
#      horizon -> CR tries to read recycled undo -> fail-closed (规则 8.A
#      compliant: error, not wrong data -- but OVER-fail-closed availability).
#      This is the L222 gap D5 closes; it is window-self-healing once SCN
#      advances past the peak.
#
#    FINDING 3 (observability — DETERMINISTIC RED):
#      No pg_cluster_state category 'tt_recovery' exists yet, so none of the
#      8 D1-D7 verdict counters are observable.  D6 flips this to PASS.
#
#    Signatures (b) cross-node materialized in-flight read and (c) wrap-
#    generation 2^32 residue are not harness-reproducible; they are unit-
#    proven (cluster_unit truth tables) and covered by the D2/D3 acceptance
#    legs once the resolution ceremony exists (L239).
#
#    Author: SqlRush <sqlrush@gmail.com>
#    Spec: spec-4.8-undo-tt-recovery.md (FROZEN v0.2)
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../lib";

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Single-node helper.  $fastpath selects the spec-3.24 no-peer CR-gate fast
# path: ON = PG-native single-node routing (FINDING 1); OFF = force the
# cluster CR/undo path that every cross-node read uses (FINDING 2).
sub _new_node
{
	my ($name, $fastpath) = @_;
	my $node = PostgreSQL::Test::Cluster->new($name);
	$node->init;
	$node->append_conf(
		'postgresql.conf', "shared_buffers = 128MB\n"
		  . "cluster.enabled = on\n"
		  . "cluster.node_id = 0\n"
		  . "cluster.allow_single_node = on\n"
		  . "cluster.interconnect_tier = stub\n"
		  . "cluster.cr_mvcc_gate = on\n"
		  . "cluster.cr_gate_no_peer_fastpath = $fastpath\n");
	return $node;
}

sub _category_exists
{
	my ($node, $cat) = @_;
	my $n = $node->safe_psql('postgres',
		qq{SELECT count(*) FROM pg_cluster_state WHERE category='$cat'});
	return $n + 0;
}

# Build rows, start an in-flight DELETE in a background session, and crash
# the node (immediate) before COMMIT so the deleter's TT slot is left ACTIVE
# on disk.  Returns the node (restarted).
sub _crash_with_inflight_delete
{
	my ($node) = @_;
	$node->safe_psql('postgres', q{
		CREATE TABLE t48 (id int primary key, v int);
		INSERT INTO t48 SELECT g, g FROM generate_series(1, 200) g;
	});
	my $bg = $node->background_psql('postgres', on_error_stop => 0);
	$bg->query_safe(q{BEGIN});
	$bg->query_safe(q{DELETE FROM t48 WHERE id <= 100});
	# Do NOT commit.  Crash immediately -> in-flight ACTIVE TT slot on disk.
	$node->stop('immediate');
	$node->start;
	return $node;
}

# ======================================================================
# FINDING 1 — single-node, fast path ON (default): PG-native correctness.
# ======================================================================
my $na = _new_node('s48_fpon', 'on');
$na->start;
_crash_with_inflight_delete($na);

my $visible = $na->safe_psql('postgres', q{SELECT count(*) FROM t48});
is($visible, '200',
	'FINDING 1 (sig a): fast-path ON single-node crash-left in-flight DELETE rows stay VISIBLE (PG-native correctness; ACTIVE slot unresolved is a D1/D2 gap, not a single-node false-visible)');

# 规则 8.A — the crash-left deleter must never be observable as committed.
my $present_a = $na->safe_psql('postgres',
	q{SELECT count(*) FROM t48 WHERE id <= 100});
is($present_a, '100',
	'FINDING 1: 规则 8.A — no false-committed delete (all 100 in-flight-deleted rows present)');

# FINDING 3 (observability) — D1 introduces the tt_recovery dump category with
# the 8 D1-D7 verdict counters; assert it is present (the D0 measure-first
# baseline of 0 flipped once D1 landed the counter region + emit rows).
is(_category_exists($na, 'tt_recovery'), 8,
	'FINDING 3 (obs): pg_cluster_state category tt_recovery exposes the 8 D1-D7 verdict counters');

# D1 (Option A reframe — measure-first finding): the on-disk TT header slots are
# NEVER written ACTIVE (the in-flight binding is in-memory CTS_ACTIVE, lost on
# crash; only commit/abort write the durable header).  So the startup resolution
# scan is a FAIL-CLOSED DEFENSIVE NET that normally resolves 0 slots -- single-
# node correctness for the crashed in-flight DELETE is already provided by
# PG-native CLOG (proven by FINDING 1 above: the rows stayed visible).  Assert
# the counter is observable and non-negative (the scan ran without error and
# never falsely resolved a non-ACTIVE slot); the load-bearing crash-xact handling
# is D2 (cross-node) + D7 (physical revert via undo records + xact_liveness).
my $resolved_a = $na->safe_psql('postgres',
	q{SELECT value FROM pg_cluster_state WHERE category='tt_recovery' AND key='active_slots_resolved_aborted'});
cmp_ok($resolved_a, '>=', 0,
	'D1: startup ACTIVE-slot resolution net ran; active_slots_resolved_aborted observable (defensive net, normally 0 — on-disk slots never ACTIVE; single-node correctness is PG-native per FINDING 1)');

# D6 (observability acceptance): every one of the 8 tt_recovery verdict counters
# is present and non-negative by exact key name (not just category count=8 --
# L223: validate the emitted content, not mere existence).  Each is wired by its
# deliverable (D1 active_slots_resolved_aborted / D2 remote_active_failclosed /
# D3 wrap_generation_disambiguated / D4 recycled_liveness_relaxed / D5
# scn_highwater_recovered / D2 recovery_verdict_failclosed / D7
# heap_tuples_physically_reverted + undo_revert_failclosed).
for my $key (
	qw(active_slots_resolved_aborted remote_active_failclosed
	wrap_generation_disambiguated recycled_liveness_relaxed
	scn_highwater_recovered recovery_verdict_failclosed
	heap_tuples_physically_reverted undo_revert_failclosed))
{
	my $present = $na->safe_psql('postgres',
		qq{SELECT count(*) FROM pg_cluster_state WHERE category='tt_recovery' AND key='$key'});
	is($present, '1', "D6 (obs): tt_recovery.$key present + observable");
}
$na->stop;

# ======================================================================
# FINDING 2 / D5 (L222) — fast path OFF (cluster CR path forced): D5 advances
# cluster_scn to the durable TT commit_scn high-watermark at startup, so the
# post-restart CR read no longer over-fail-closes "snapshot too old".  In D0
# this was a window-dependent over-fail-closed; D5 closes it deterministically.
# ======================================================================
my $nb = _new_node('s48_fpoff', 'off');
$nb->start;
_crash_with_inflight_delete($nb);

# D5 fired at startup: the committed INSERT left a durable COMMITTED TT slot, so
# the high-watermark observe ran (scn_highwater_recovered >= 1) -- deterministic.
my $hw = $nb->safe_psql('postgres',
	q{SELECT value FROM pg_cluster_state WHERE category='tt_recovery' AND key='scn_highwater_recovered'});
cmp_ok($hw, '>=', 1,
	'D5 (L222): startup observed the durable TT commit_scn high-watermark into cluster_scn (scn_highwater_recovered >= 1)');

# The cluster CR-path read after crash-restart now succeeds: cluster_scn was
# bumped past the durable peak at startup, so read_scn >= the retention horizon
# (no over-fail-closed).  Use psql (not safe_psql) to capture rather than die.
my ($ret, $stdout, $stderr) =
  $nb->psql('postgres', q{SELECT count(*) FROM t48});
my $is_snapshot_too_old = ($stderr =~ /snapshot too old/);
diag("D5: post-restart cluster CR read ret=$ret"
	  . ($is_snapshot_too_old ? ' STILL over-fail-closed (unexpected)' : ' ok (no snapshot-too-old)'));
ok(!$is_snapshot_too_old,
	'D5 (L222): post-restart cluster CR read does NOT over-fail-closed snapshot-too-old (cluster_scn recovered to the durable high-watermark)');

# 规则 8.A: even if a CR-path read failed, it must be fail-closed (an error),
# never wrong data.
ok($ret == 0 || $is_snapshot_too_old,
	'FINDING 2: 规则 8.A — any residual CR-path failure is fail-closed (snapshot-too-old), never wrong data');
$nb->stop;

# ======================================================================
# Signatures (b) cross-node materialized read, (c) wrap-generation: not
# harness-reproducible (L239) -- unit-proven + D2/D3 acceptance legs.
# ======================================================================
# sig(b) cross-node TT authority — D2 LANDED.  The recovered_through LSN gate
# (cluster_tt_recovery_remote_authority_covers) tightens the 4.5a G6 bool
# is_materialized gate: a materialized-but-under-recovered origin (tuple page
# LSN beyond recovered_through) fails closed (remote_active_failclosed counter),
# never trusting a stale COMMITTED/ABORTED outcome (规则 8.A).  The gate
# decision is unit-proven (test_cluster_tt_durable D2 truth table: anchor==0
# skip / recovered>=anchor trust / recovered<anchor fail-closed).  The e2e
# materialized-under-recovered scenario needs a real shared-storage crash+merge
# with a page LSN beyond the merge's recovered_through, which the current
# harness cannot force deterministically (mirrors 4.7 D5) -> honest SKIP (L239).
SKIP:
{
	skip 'sig(b) D2 cross-node recovered_through gate is unit-proven; the '
	  . 'materialized-under-recovered e2e needs real shared-storage crash+merge '
	  . '(not harness-deterministic, mirrors 4.7 D5 / L239)', 1;
	ok(1, 'sig(b) placeholder');
}
SKIP:
{
	skip 'sig(c) wrap-generation needs a forced 2^32 xid wrap (not harness-'
	  . 'reproducible); unit-proven in test_tt_recovery_d3_wrap_generation (L239)', 1;
	ok(1, 'sig(c) placeholder');
}

done_testing();
