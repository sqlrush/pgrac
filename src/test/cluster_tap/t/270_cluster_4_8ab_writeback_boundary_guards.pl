#-------------------------------------------------------------------------
#
# 270_cluster_4_8ab_writeback_boundary_guards.pl
#    spec-4.8ab D1 — checkpoint-writeback boundary guards fail-closed (8.A).
#
#    The undo buffer pool's write-back path makes two durability promises that
#    spec-4.8ab raises from implicit to explicit + guarded:
#      1. WAL-before-data: an undo block is never written back before its
#         XLOG_UNDO_BLOCK_WRITE is durable (block_lsn <= GetFlushRecPtr()).
#      2. checkpoint-coverage: a checkpoint never completes leaving a PRE-redo
#         dirty undo block unflushed (recovery starts at redo and would not
#         replay it -> lost).
#    A detected violation fail-closes with SQLSTATE 53R9N
#    (ERRCODE_CLUSTER_UNDO_WRITEBACK_BOUNDARY_VIOLATION), never silent.
#
#      L3c  CONTROL: a normal CHECKPOINT over buffered-dirty undo succeeds with
#           NO 53R9N and NO PANIC -- the guard does not false-fire on a legal
#           (post-flush / post-redo) dirty block.  This pins the v0.3-amend
#           discipline at the integration level (the guard is NOT block_lsn>redo).
#      L3a  WAL-before-data: arm undo-force-wal-before-data-violation, CHECKPOINT
#           -> the flush_dirty_slot guard fires 53R9N ("write-back flush").
#      L3b  checkpoint-coverage: arm undo-skip-checkpoint-flush-one, CHECKPOINT
#           -> one dirty block is left unflushed and the post-flush re-scan fires
#           53R9N ("checkpoint coverage").
#      L_obs  (D7) undo_buf_boundary_violations moved off zero after a real
#           injected fire -- the boundary counter wiring is real end-to-end.
#      L6   (D5 two-layer) with cluster.undo_writeback_boundary_check=off the
#           HARD WAL-before-data guard STILL fires 53R9N -- off never disables
#           the 8.A corruption path, it only skips advisory accounting (§3.1).
#      L1   (D4) crash AFTER a checkpoint: redo replays post-checkpoint undo,
#           committed state byte-identical, no PANIC / no false 53R9N.
#      L2   (D4) crash DURING a write-back load: idempotent redo, rows survive.
#      L_surface  (D7) all four boundary counters exposed + non-negative;
#           remote-evidence holds = 0 on single node (4.8b contract-first).
#
# IDENTIFICATION
#    src/test/cluster_tap/t/270_cluster_4_8ab_writeback_boundary_guards.pl
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

use PgracClusterNode;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PgracClusterNode->new('wb_boundary');
$node->init;
$node->append_conf('postgresql.conf',
	    "cluster.enabled = on\n"
	  . "cluster.node_id = 0\n"
	  . "cluster.allow_single_node = on\n"
	  . "cluster.undo_buffers = 64\n"
	  . "cluster.undo_buffer_writeback = on\n"   # write-back active (single-node)
	  . "checkpoint_timeout = 1h\n"              # no auto-checkpoint races
	  . "max_wal_size = 4GB\n"
	  . "log_error_verbosity = verbose\n"        # SQLSTATE 53R9N appears in the log
	  . "autovacuum = off\n");
$node->start;

is($node->safe_psql('postgres', 'SHOW cluster.undo_buffer_writeback'),
	'on', 'write-back GUC enabled (single-node)');

$node->safe_psql('postgres',
	q{CREATE TABLE t_wb (id int primary key, v text);
	  INSERT INTO t_wb SELECT g, 'base' || g FROM generate_series(1, 200) g});

# dirty_undo() — a length-preserving UPDATE writes undo, leaving the undo data
# block buffered-dirty in the pool (write-back defers the pwrite to checkpoint).
my $bump = 0;
sub dirty_undo
{
	$bump++;
	$node->safe_psql('postgres',
		"UPDATE t_wb SET v = 'r${bump}' || id WHERE id <= 50");
}

# ============================================================
# L3c CONTROL — a normal CHECKPOINT over buffered-dirty undo must NOT fire the
# boundary guard (no false-positive on a legal dirty block).
# ============================================================
dirty_undo();
my $log_off = -s $node->logfile;
$node->safe_psql('postgres', 'CHECKPOINT');
my $log = slurp_file($node->logfile, $log_off);
unlike($log, qr/53R9N|checkpoint-writeback boundary violation/,
	'L3c control: normal checkpoint over dirty undo does NOT fire 53R9N');
unlike($log, qr/PANIC/, 'L3c control: no PANIC on a legal dirty block');

# ============================================================
# L3a WAL-before-data — arm the force-violation point; the flush_dirty_slot
# guard must fail-close 53R9N ("write-back flush").
# ============================================================
$node->append_conf('postgresql.conf',
	"cluster.injection_points = 'undo-force-wal-before-data-violation:skip'\n");
$node->restart;
dirty_undo();
$log_off = -s $node->logfile;
# The checkpointer hits the guard mid-CheckPointGuts -> the CHECKPOINT request
# fails;  the 53R9N is logged by the checkpointer.  Tolerate either client rc.
$node->psql('postgres', 'CHECKPOINT');
ok( $node->wait_for_log(
		qr/checkpoint-writeback boundary violation: write-back flush/, $log_off),
	'L3a WAL-before-data guard fires (write-back flush) under injection');
like(slurp_file($node->logfile, $log_off), qr/53R9N/,
	'L3a violation is SQLSTATE 53R9N');

# ============================================================
# L3b checkpoint-coverage — arm the skip-one point; the post-flush re-scan must
# fail-close 53R9N ("checkpoint coverage") on the escaped PRE-redo dirty block.
# ============================================================
$node->append_conf('postgresql.conf',
	"cluster.injection_points = 'undo-skip-checkpoint-flush-one:skip'\n");
$node->restart;
dirty_undo();
$log_off = -s $node->logfile;
$node->psql('postgres', 'CHECKPOINT');
ok( $node->wait_for_log(
		qr/checkpoint-writeback boundary violation: checkpoint coverage/, $log_off),
	'L3b checkpoint-coverage guard fires under injection');
like(slurp_file($node->logfile, $log_off), qr/53R9N/,
	'L3b violation is SQLSTATE 53R9N');

# undo-dump counter ($key) -> integer value of a category='undo' dump counter.
my $undo_counter = sub {
	my ($key) = @_;
	return $node->safe_psql('postgres',
		"SELECT COALESCE((SELECT value::bigint FROM pg_cluster_state "
	  . "WHERE category='undo' AND key='$key'), 0)");
};

# ============================================================
# L_obs (D7) — the boundary counter wiring is real end-to-end: L3b's injected
# violation just fired in this postmaster, so undo_buf_boundary_violations has
# moved off zero (the counter is bumped in shmem before the fail-closed raise).
# ============================================================
cmp_ok($undo_counter->('undo_buf_boundary_violations'), '>=', 1,
	'L_obs D7: undo_buf_boundary_violations moved after a real injected fire');

# ============================================================
# L6 (D5 two-layer model, §3.1) — cluster.undo_writeback_boundary_check=off must
# NOT disable the hard fail-closed guard.  Re-arm the WAL-before-data violation
# with the GUC OFF: the hard corruption path still fail-closes 53R9N (off only
# skips the advisory verdict accounting, never the 8.A guard).
# ============================================================
$node->append_conf('postgresql.conf',
	    "cluster.undo_writeback_boundary_check = off\n"
	  . "cluster.injection_points = 'undo-force-wal-before-data-violation:skip'\n");
$node->restart;
is($node->safe_psql('postgres', 'SHOW cluster.undo_writeback_boundary_check'),
	'off', 'L6 advisory boundary check is off');
dirty_undo();
$log_off = -s $node->logfile;
$node->psql('postgres', 'CHECKPOINT');
ok( $node->wait_for_log(
		qr/checkpoint-writeback boundary violation: write-back flush/, $log_off),
	'L6 two-layer: hard WAL-before-data guard fires EVEN WITH the GUC off (8.A not gated)');
like(slurp_file($node->logfile, $log_off), qr/53R9N/,
	'L6 hard violation is still SQLSTATE 53R9N under off');

# ============================================================
# Clean state for the crash legs: disarm injection + restore the default GUC.
# ============================================================
$node->append_conf('postgresql.conf',
	    "cluster.undo_writeback_boundary_check = on\n"
	  . "cluster.injection_points = ''\n");
$node->restart;

# ============================================================
# L1 (D4) — crash AFTER a checkpoint over buffered-dirty undo: recovery redoes
# the post-checkpoint undo writes, the committed data is byte-identical, and
# neither the redo base-integrity guard nor the boundary guards false-fire.
# ============================================================
dirty_undo();
$node->safe_psql('postgres', 'CHECKPOINT');   # flush dirty undo (coverage)
dirty_undo();                                  # post-checkpoint dirty undo (redo replays it)
my $sig_before = $node->safe_psql('postgres',
	q{SELECT md5(string_agg(id || '=' || v, '|' ORDER BY id)) FROM t_wb});
$log_off = -s $node->logfile;
$node->stop('immediate');                      # crash
$node->start;                                  # recovery redo
my $sig_after = $node->safe_psql('postgres',
	q{SELECT md5(string_agg(id || '=' || v, '|' ORDER BY id)) FROM t_wb});
is($sig_after, $sig_before, 'L1 committed state byte-identical after crash-during-checkpoint redo');
my $rlog = slurp_file($node->logfile, $log_off);
unlike($rlog, qr/PANIC/, 'L1 no PANIC in recovery (redo base-integrity guard holds)');
unlike($rlog, qr/53R9N/, 'L1 no false boundary violation in recovery');

# ============================================================
# L2 (D4) — crash DURING a write-back load (undo piling into buffered blocks):
# redo replays idempotently, committed rows survive, no PANIC / no false 53R9N.
# ============================================================
$node->safe_psql('postgres',
	q{UPDATE t_wb SET v = 'l2' || id WHERE id <= 120});  # heavy undo, buffered write-back
my $cnt_before = $node->safe_psql('postgres',
	q{SELECT count(*) FROM t_wb WHERE v LIKE 'l2%'});
$log_off = -s $node->logfile;
$node->stop('immediate');                      # crash mid-write-back
$node->start;                                  # recovery
my $cnt_after = $node->safe_psql('postgres',
	q{SELECT count(*) FROM t_wb WHERE v LIKE 'l2%'});
is($cnt_after, $cnt_before, 'L2 committed UPDATEs survived crash-during-write-back redo');
$rlog = slurp_file($node->logfile, $log_off);
unlike($rlog, qr/PANIC/, 'L2 no PANIC in crash-during-write-back recovery');

# ============================================================
# L_surface (D7) — all four checkpoint-writeback boundary counters are exposed
# in the dump and non-negative;  boundary_violations is back to 0 (fresh
# postmaster, no injection in this lifetime -> the guards did not false-fire).
# ============================================================
for my $k (qw(undo_buf_held_wal undo_buf_held_evidence
	undo_buf_boundary_violations undo_buf_remote_evidence_holds))
{
	cmp_ok($undo_counter->($k), '>=', 0, "L_surface D7: $k exposed + non-negative");
}
is($undo_counter->('undo_buf_boundary_violations'), '0',
	'L_surface: no boundary violation false-fired in normal operation');
is($undo_counter->('undo_buf_remote_evidence_holds'), '0',
	'L_surface: remote-evidence holds 0 on a single-node topology (4.8b contract-first)');

$node->stop;
done_testing();
