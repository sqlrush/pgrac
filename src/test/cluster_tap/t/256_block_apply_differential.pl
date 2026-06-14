#-------------------------------------------------------------------------
#
# 256_block_apply_differential.pl
#    spec-4.10 D3b — byte-for-byte differential for the backend single-block
#    redo-apply framework (online block recovery, corruption-critical, R11).
#
#    The 8.A / R11 correctness guarantee: for every record type on the apply
#    matrix, reconstructing a block by single-block redo-apply
#    (cluster_block_apply_redo_test, driving cluster_block_apply_one over a
#    WAL range) must be BYTE-FOR-BYTE identical to the block PG's real redo
#    leaves on disk.  A divergence is a silent wrong-block install.
#
#    The reference page is taken from PG's REAL crash-recovery redo, not the
#    live page: a live page may carry hint bits / residual hole bytes that WAL
#    replay never reproduces.  Each leg therefore:
#        CHECKPOINT -> capture start_lsn -> op1 (FPI-bearing) [-> op2 (delta
#        under test)] -> capture end_lsn -> crash (immediate) -> restart
#        (PG real redo) -> CHECKPOINT (flush replayed page) -> compare the
#        SRF reconstruction over [start_lsn, end_lsn] against the on-disk page.
#
#    Determinism (L247): autovacuum off (no stray VM/prune records on the
#    block), full_page_writes on (op1 carries the FPI), large wal_keep_size
#    (the post-restart CHECKPOINT must not recycle the WAL the SRF re-reads),
#    and the table is NEVER read before the compare (no hint bits).
#
#    Apply core (D3b), via the raw apply SRF:
#      L1   single INSERT: heap FPI + trailing cluster ITL-touch (Generic)
#      L2   heap INSERT delta on top of an FPI base
#      L3   heap DELETE delta on top of an FPI base
#      L4   heap HOT UPDATE delta (same page; prefix/suffix-from-old)
#      L5   heap non-HOT UPDATE delta (same page)
#      L6   fail-closed: a delta with no FPI base -> SRF returns NULL (8.A)
#
#    Online reconstructor (D2), via cluster_block_recovery_reconstruct over an
#    explicit window -- same crash-recovery differential:
#      R1   reconstruct (FPI base + delta) byte-for-byte == real redo
#      R2   skip-pre-base + latest-FPI-wins (window opens after an earlier FPI;
#           a second checkpoint's FPI is the base; an earlier delta is skipped)
#      R3   fail-closed: window holds no FPI base -> UNRECOVERABLE (NULL, 8.A)
#
#    pg_ctl-level stop('immediate')/start cycles (not the TAP bootstrap
#    path), so the crash-restart legs run on this host directly (cf. t/225).
#
# IDENTIFICATION
#    src/test/cluster_tap/t/256_block_apply_differential.pl
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

my $node = PgracClusterNode->new('blkapply');
$node->init;
$node->append_conf('postgresql.conf',
	    "cluster.enabled = on\n"
	  . "cluster.node_id = 0\n"
	  . "cluster.allow_single_node = on\n"
	  . "autovacuum = off\n"
	  . "full_page_writes = on\n"
	  . "wal_keep_size = 256MB\n");
$node->start;

# ----------------------------------------------------------------------
# crash_and_diff: crash the node, restart (PG real redo), flush the
# replayed page, and classify the single-block reconstruction over
# [$start,$end] against the on-disk reference page.  Returns one of
# 'match' / 'mismatch' / 'recon-null'.
# ----------------------------------------------------------------------
sub crash_and_diff
{
	my ($rel, $start, $end) = @_;

	$node->stop('immediate');
	$node->start;

	# Flush the redo-replayed page to disk so pg_read_binary_file sees it.
	# Nothing has SELECTed the table, so the page carries no hint bits.
	$node->safe_psql('postgres', 'CHECKPOINT');

	return $node->safe_psql('postgres', qq{
		WITH r AS (
			SELECT cluster_block_apply_redo_test(
					   '$rel'::regclass, 0, 0,
					   '$start'::pg_lsn, '$end'::pg_lsn) AS recon
		)
		SELECT CASE
				 WHEN recon IS NULL THEN 'recon-null'
				 WHEN recon = pg_read_binary_file(
						 pg_relation_filepath('$rel'), 0, 8192) THEN 'match'
				 ELSE 'mismatch'
			   END
		FROM r;
	});
}

# Same crash-recovery differential, but through the D2 online-recovery
# reconstructor (cluster_block_recovery_reconstruct over an explicit window),
# not the raw apply SRF.
sub crash_and_diff_recon
{
	my ($rel, $scan_lower, $scan_upper) = @_;

	$node->stop('immediate');
	$node->start;
	$node->safe_psql('postgres', 'CHECKPOINT');

	return $node->safe_psql('postgres', qq{
		WITH r AS (
			SELECT cluster_block_recovery_reconstruct_test(
					   '$rel'::regclass, 0, 0,
					   '$scan_lower'::pg_lsn, '$scan_upper'::pg_lsn) AS recon
		)
		SELECT CASE
				 WHEN recon IS NULL THEN 'recon-null'
				 WHEN recon = pg_read_binary_file(
						 pg_relation_filepath('$rel'), 0, 8192) THEN 'match'
				 ELSE 'mismatch'
			   END
		FROM r;
	});
}

# Seed a table with a handful of short rows (all on block 0) and checkpoint,
# leaving block 0 established and clean so the next touch bears the FPI.
sub seed_and_checkpoint
{
	my ($rel) = @_;
	$node->safe_psql('postgres',
		"CREATE TABLE $rel (id int, v text) WITH (autovacuum_enabled = off)");
	$node->safe_psql('postgres',
		"INSERT INTO $rel SELECT g, 'seed' || g FROM generate_series(1, 8) g");
	$node->safe_psql('postgres', 'CHECKPOINT');
}

# ============================================================
# L1: single-INSERT reconstruction (heap FPI + ITL-touch Generic).
#     op1 is the first block-0 touch after the checkpoint -> the heap INSERT
#     bears the full-page image, and the cluster ITL-touch finish logs a
#     trailing Generic record that stamps the block's ITL slots.  The chain
#     is therefore FPI (heap) -> delta (Generic); both must replay.
# ============================================================
{
	seed_and_checkpoint('t256_fpi');
	my $start = $node->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');
	$node->safe_psql('postgres', "INSERT INTO t256_fpi VALUES (100, 'op1')");
	my $end = $node->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');

	is(crash_and_diff('t256_fpi', $start, $end), 'match',
		'L1 single INSERT (heap FPI + ITL-touch Generic) == real redo');
}

# ============================================================
# L2: heap INSERT delta on an FPI base (D3b.3).
#     op1 bears the FPI; op2 is a second insert on block 0 (no FPI) -> a
#     pure heap_insert delta, the record type under test.
# ============================================================
{
	seed_and_checkpoint('t256_ins');
	my $start = $node->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');
	$node->safe_psql('postgres', "INSERT INTO t256_ins VALUES (100, 'op1_fpi')");
	$node->safe_psql('postgres', "INSERT INTO t256_ins VALUES (101, 'op2_delta')");
	my $end = $node->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');

	is(crash_and_diff('t256_ins', $start, $end), 'match',
		'L2 heap INSERT delta byte-for-byte == real redo');
}

# ============================================================
# L3: heap DELETE delta on an FPI base (D3b.3).
#     op1 inserts a row (FPI); op2 deletes it (no FPI) -> a heap_delete delta.
# ============================================================
{
	seed_and_checkpoint('t256_del');
	my $start = $node->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');
	$node->safe_psql('postgres', "INSERT INTO t256_del VALUES (100, 'op1_fpi')");
	$node->safe_psql('postgres', "DELETE FROM t256_del WHERE id = 100");
	my $end = $node->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');

	is(crash_and_diff('t256_del', $start, $end), 'match',
		'L3 heap DELETE delta byte-for-byte == real redo');
}

# ============================================================
# L4: heap HOT UPDATE delta, same page (D3b.3).
#     No index on the table -> the update is HOT and (with room) stays on
#     block 0: old tuple gets xmax/ctid, new tuple is added on the same page.
#     Exercises the prefix/suffix-from-old reconstruction.
# ============================================================
{
	seed_and_checkpoint('t256_upd');
	my $start = $node->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');
	$node->safe_psql('postgres', "INSERT INTO t256_upd VALUES (100, 'op1_fpi_value')");
	$node->safe_psql('postgres', "UPDATE t256_upd SET v = 'op2_updated_value' WHERE id = 100");
	my $end = $node->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');

	is(crash_and_diff('t256_upd', $start, $end), 'match',
		'L4 heap HOT UPDATE delta (same page) byte-for-byte == real redo');
}

# ============================================================
# L5: heap non-HOT UPDATE delta, same page (D3b.3).
#     An index on v makes the update non-HOT (XLOG_HEAP_UPDATE); with room
#     the new tuple still lands on block 0.
# ============================================================
{
	$node->safe_psql('postgres',
		"CREATE TABLE t256_updn (id int, v text) WITH (autovacuum_enabled = off)");
	$node->safe_psql('postgres', "CREATE INDEX t256_updn_v ON t256_updn (v)");
	$node->safe_psql('postgres',
		"INSERT INTO t256_updn SELECT g, 'seed' || g FROM generate_series(1, 8) g");
	$node->safe_psql('postgres', 'CHECKPOINT');
	my $start = $node->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');
	$node->safe_psql('postgres', "INSERT INTO t256_updn VALUES (100, 'op1_fpi_value')");
	$node->safe_psql('postgres', "UPDATE t256_updn SET v = 'op2_nonhot_value' WHERE id = 100");
	my $end = $node->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');

	is(crash_and_diff('t256_updn', $start, $end), 'match',
		'L5 heap non-HOT UPDATE delta (same page) byte-for-byte == real redo');
}

# ============================================================
# L6: fail-closed (8.A) — a WAL range whose first block-0 record is a delta
#     with no FPI base must reconstruct to NULL, never a partial/wrong page.
#     start_lsn is captured AFTER op1's FPI, so the range holds only the op2
#     delta; the SRF has no base to apply onto and fails closed.  (No crash
#     needed: this exercises the SRF's base-required guard, not redo.)
# ============================================================
{
	seed_and_checkpoint('t256_fc');
	$node->safe_psql('postgres', "INSERT INTO t256_fc VALUES (100, 'op1_fpi')");
	my $start = $node->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');
	$node->safe_psql('postgres', "INSERT INTO t256_fc VALUES (101, 'op2_delta')");
	my $end = $node->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');

	my $is_null = $node->safe_psql('postgres', qq{
		SELECT cluster_block_apply_redo_test(
				   't256_fc'::regclass, 0, 0,
				   '$start'::pg_lsn, '$end'::pg_lsn) IS NULL;
	});
	is($is_null, 't',
		'L6 fail-closed: delta with no FPI base in range -> NULL (8.A)');
}

# ============================================================
# R1 (D2): online reconstruct over an explicit window == real redo.
#     scan_lower before op1's FPI, scan_upper after op2's delta -> the
#     reconstructor finds the FPI base and applies the delta to the
#     WAL-derived target (op2).
# ============================================================
{
	seed_and_checkpoint('t256_r1');
	my $lo = $node->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');
	$node->safe_psql('postgres', "INSERT INTO t256_r1 VALUES (100, 'op1_fpi')");
	$node->safe_psql('postgres', "INSERT INTO t256_r1 VALUES (101, 'op2_delta')");
	my $hi = $node->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');

	is(crash_and_diff_recon('t256_r1', $lo, $hi), 'match',
		'R1 D2 reconstruct (FPI base + delta) byte-for-byte == real redo');
}

# ============================================================
# R2 (D2): skip-pre-base + latest-FPI-wins.
#     The window opens AFTER op1's FPI, so the first in-window block touch is
#     op2 (a delta with no base) -- the reconstructor must SKIP it.  A second
#     checkpoint makes op3 carry a fresh FPI (the latest FPI <= target); op4 is
#     a delta on top.  The reconstruction must use FPI(op3) as the base and end
#     at op4, ignoring the earlier FPI(op1) and the skipped delta(op2).
# ============================================================
{
	seed_and_checkpoint('t256_r2');
	$node->safe_psql('postgres', "INSERT INTO t256_r2 VALUES (100, 'op1_fpi_a')");
	my $lo = $node->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');   # after op1
	$node->safe_psql('postgres', "INSERT INTO t256_r2 VALUES (101, 'op2_delta_b')");
	$node->safe_psql('postgres', 'CHECKPOINT');                              # ckpt2
	$node->safe_psql('postgres', "INSERT INTO t256_r2 VALUES (102, 'op3_fpi_c')");
	$node->safe_psql('postgres', "INSERT INTO t256_r2 VALUES (103, 'op4_delta_d')");
	my $hi = $node->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');   # after op4

	is(crash_and_diff_recon('t256_r2', $lo, $hi), 'match',
		'R2 D2 reconstruct skip-pre-base + latest-FPI-wins == real redo');
}

# ============================================================
# R3 (D2, 8.A): fail-closed when the window holds no FPI base.
#     scan_lower after op1's FPI -> only op2's delta is in window -> the
#     reconstructor has no base and returns UNRECOVERABLE (NULL).  (No crash:
#     this exercises the reconstructor's no-base guard.)
# ============================================================
{
	seed_and_checkpoint('t256_r3');
	$node->safe_psql('postgres', "INSERT INTO t256_r3 VALUES (100, 'op1_fpi')");
	my $lo = $node->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');   # after op1 FPI
	$node->safe_psql('postgres', "INSERT INTO t256_r3 VALUES (101, 'op2_delta')");
	my $hi = $node->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');

	my $is_null = $node->safe_psql('postgres', qq{
		SELECT cluster_block_recovery_reconstruct_test(
				   't256_r3'::regclass, 0, 0,
				   '$lo'::pg_lsn, '$hi'::pg_lsn) IS NULL;
	});
	is($is_null, 't',
		'R3 D2 reconstruct fail-closed: no FPI base in window -> NULL (8.A)');
}

$node->stop;
done_testing();
