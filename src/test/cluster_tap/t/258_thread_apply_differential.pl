#-------------------------------------------------------------------------
#
# 258_thread_apply_differential.pl
#    spec-4.11 D1 — byte-for-byte differential for the online thread-recovery
#    page apply matrix (Q10-B, corruption-critical 8.A).
#
#    spec-4.11 increment 2 wraps the t/256-proven single-block apply core
#    (cluster_block_apply_one) with PG redo's LSN-gate, so a survivor can
#    online-replay a dead thread's WAL stream onto a LIVE shared-storage page
#    idempotently.  Two guarantees are proven here against PG's REAL
#    crash-recovery redo (the page pg_read_binary_file returns after a real
#    stop('immediate')/start replays the same WAL):
#
#      PARITY (base_page = NULL): a single apply-through built from the FPI base
#      + deltas is byte-for-byte identical to real redo -- the wrapper does not
#      change the apply result.
#        L1   single INSERT: heap FPI + trailing cluster ITL-touch (Generic)
#        L2   heap INSERT delta on an FPI base
#        L3   heap DELETE delta on an FPI base
#        L4   heap HOT UPDATE delta (same page; prefix/suffix-from-old)
#        L5   heap non-HOT UPDATE delta (same page)
#
#      IDEMPOTENCE (base_page = the real-redo reference page; DELTA-ONLY window):
#      re-applying a record the page already reflects is a no-op (the redo
#      LSN-gate skips it), so the page is unchanged == reference.  This is the
#      spec-4.11 v0.3 partial-apply contract: a retry / cold redo from a
#      validated lower bound must not double-apply.  WITHOUT the gate a
#      re-applied delta corrupts the page (PageAddItem at an occupied offset ->
#      FAILED -> NULL), so each leg returns 'recon-null' if the gate is missing.
#      An FPI-bearing window cannot prove this -- the FPI would reset the page
#      each replay -- hence the window is opened AFTER op1's FPI.
#        L6   re-apply heap INSERT delta over the reference page -> unchanged
#        L7   re-apply heap HOT UPDATE delta over the reference page -> unchanged
#
#      FAIL-CLOSED (8.A):
#        L8   a delta with no FPI base and no seeded base_page -> NULL
#
#    Determinism (L247): autovacuum off (no stray VM/prune records on the
#    block), full_page_writes on (op1 carries the FPI), large wal_keep_size
#    (the post-restart CHECKPOINT must not recycle the WAL the SRF re-reads),
#    and the table is NEVER read before the compare (no hint bits).
#
#    pg_ctl-level stop('immediate')/start cycles (not the TAP bootstrap path),
#    so the crash-restart legs run on this host directly (cf. t/256).
#
# IDENTIFICATION
#    src/test/cluster_tap/t/258_thread_apply_differential.pl
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

my $node = PgracClusterNode->new('threadapply');
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
# crash_and_diff: crash the node, restart (PG real redo), flush the replayed
# page, and classify the LSN-gated apply-through over [$start,$end] (built from
# a zero page -- base_page NULL, PARITY mode) against the on-disk reference
# page.  Returns 'match' / 'mismatch' / 'recon-null'.
# ----------------------------------------------------------------------
sub crash_and_diff
{
	my ($rel, $start, $end) = @_;

	$node->stop('immediate');
	$node->start;
	$node->safe_psql('postgres', 'CHECKPOINT');

	return $node->safe_psql('postgres', qq{
		WITH r AS (
			SELECT cluster_thread_apply_redo_test(
					   '$rel'::regclass, 0, 0,
					   '$start'::pg_lsn, '$end'::pg_lsn, NULL) AS recon
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

# ----------------------------------------------------------------------
# crash_and_diff_idempotent: crash/restart for the real-redo reference page,
# then re-apply a DELTA-ONLY window [$delta_start,$end] over that reference
# page (base_page = reference).  The redo LSN-gate must skip every record (the
# page already reflects it), leaving the page unchanged == reference.  WITHOUT
# the gate the re-applied delta corrupts the page -> SRF returns NULL.
# ----------------------------------------------------------------------
sub crash_and_diff_idempotent
{
	my ($rel, $delta_start, $end) = @_;

	$node->stop('immediate');
	$node->start;
	$node->safe_psql('postgres', 'CHECKPOINT');

	return $node->safe_psql('postgres', qq{
		WITH ref AS (
			SELECT pg_read_binary_file(
					   pg_relation_filepath('$rel'), 0, 8192) AS p
		),
		r AS (
			SELECT cluster_thread_apply_redo_test(
					   '$rel'::regclass, 0, 0,
					   '$delta_start'::pg_lsn, '$end'::pg_lsn,
					   (SELECT p FROM ref)) AS recon
		)
		SELECT CASE
				 WHEN recon IS NULL THEN 'recon-null'
				 WHEN recon = (SELECT p FROM ref) THEN 'match'
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
# PARITY (base_page = NULL): apply-through == real redo.
# ============================================================

# L1: single INSERT (heap FPI + ITL-touch Generic).  op1 is the first block-0
#     touch after the checkpoint -> heap INSERT bears the FPI; the cluster
#     ITL-touch finish logs a trailing Generic record.  Chain: FPI -> delta.
{
	seed_and_checkpoint('t258_fpi');
	my $start = $node->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');
	$node->safe_psql('postgres', "INSERT INTO t258_fpi VALUES (100, 'op1')");
	my $end = $node->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');

	is(crash_and_diff('t258_fpi', $start, $end), 'match',
		'L1 single INSERT (heap FPI + ITL-touch Generic) == real redo');
}

# L2: heap INSERT delta on an FPI base.  op1 bears the FPI; op2 is a pure
#     heap_insert delta (no FPI), the record type under test.
{
	seed_and_checkpoint('t258_ins');
	my $start = $node->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');
	$node->safe_psql('postgres', "INSERT INTO t258_ins VALUES (100, 'op1_fpi')");
	$node->safe_psql('postgres', "INSERT INTO t258_ins VALUES (101, 'op2_delta')");
	my $end = $node->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');

	is(crash_and_diff('t258_ins', $start, $end), 'match',
		'L2 heap INSERT delta byte-for-byte == real redo');
}

# L3: heap DELETE delta on an FPI base.  op1 inserts a row (FPI); op2 deletes
#     it (no FPI) -> a heap_delete delta.
{
	seed_and_checkpoint('t258_del');
	my $start = $node->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');
	$node->safe_psql('postgres', "INSERT INTO t258_del VALUES (100, 'op1_fpi')");
	$node->safe_psql('postgres', "DELETE FROM t258_del WHERE id = 100");
	my $end = $node->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');

	is(crash_and_diff('t258_del', $start, $end), 'match',
		'L3 heap DELETE delta byte-for-byte == real redo');
}

# L4: heap HOT UPDATE delta, same page.  No index -> HOT update stays on block
#     0: old tuple gets xmax/ctid, new tuple added on the same page.  Exercises
#     prefix/suffix-from-old reconstruction.
{
	seed_and_checkpoint('t258_upd');
	my $start = $node->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');
	$node->safe_psql('postgres', "INSERT INTO t258_upd VALUES (100, 'op1_fpi_value')");
	$node->safe_psql('postgres', "UPDATE t258_upd SET v = 'op2_updated_value' WHERE id = 100");
	my $end = $node->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');

	is(crash_and_diff('t258_upd', $start, $end), 'match',
		'L4 heap HOT UPDATE delta (same page) byte-for-byte == real redo');
}

# L5: heap non-HOT UPDATE delta, same page.  An index on v makes the update
#     non-HOT (XLOG_HEAP_UPDATE); with room the new tuple still lands on block 0.
{
	$node->safe_psql('postgres',
		"CREATE TABLE t258_updn (id int, v text) WITH (autovacuum_enabled = off)");
	$node->safe_psql('postgres', "CREATE INDEX t258_updn_v ON t258_updn (v)");
	$node->safe_psql('postgres',
		"INSERT INTO t258_updn SELECT g, 'seed' || g FROM generate_series(1, 8) g");
	$node->safe_psql('postgres', 'CHECKPOINT');
	my $start = $node->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');
	$node->safe_psql('postgres', "INSERT INTO t258_updn VALUES (100, 'op1_fpi_value')");
	$node->safe_psql('postgres', "UPDATE t258_updn SET v = 'op2_nonhot_value' WHERE id = 100");
	my $end = $node->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');

	is(crash_and_diff('t258_updn', $start, $end), 'match',
		'L5 heap non-HOT UPDATE delta (same page) byte-for-byte == real redo');
}

# ============================================================
# IDEMPOTENCE (base_page = reference; DELTA-ONLY window): the LSN-gate skips a
# record the page already reflects -> page unchanged.  delta_start is captured
# AFTER op1's FPI, so the window holds only the op2 delta (no FPI to reset the
# page).  WITHOUT the gate the re-applied delta corrupts -> 'recon-null'.
# ============================================================

# L6: re-apply a heap INSERT delta the page already has.  Without the gate
#     PageAddItem at op2's occupied offset fails -> NULL.
{
	seed_and_checkpoint('t258_idem_ins');
	$node->safe_psql('postgres', "INSERT INTO t258_idem_ins VALUES (100, 'op1_fpi')");
	my $delta_start = $node->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');
	$node->safe_psql('postgres', "INSERT INTO t258_idem_ins VALUES (101, 'op2_delta')");
	my $end = $node->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');

	is(crash_and_diff_idempotent('t258_idem_ins', $delta_start, $end), 'match',
		'L6 re-apply INSERT delta over reference -> LSN-gated no-op (idempotent)');
}

# L7: re-apply a heap HOT UPDATE delta the page already has.  Without the gate
#     PageAddItem of the new tuple at its occupied offset fails -> NULL.
{
	seed_and_checkpoint('t258_idem_upd');
	$node->safe_psql('postgres', "INSERT INTO t258_idem_upd VALUES (100, 'op1_fpi_value')");
	my $delta_start = $node->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');
	$node->safe_psql('postgres', "UPDATE t258_idem_upd SET v = 'op2_updated' WHERE id = 100");
	my $end = $node->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');

	is(crash_and_diff_idempotent('t258_idem_upd', $delta_start, $end), 'match',
		'L7 re-apply HOT UPDATE delta over reference -> LSN-gated no-op (idempotent)');
}

# ============================================================
# FAIL-CLOSED (8.A)
# ============================================================

# L8: a WAL range whose first block-0 record is a delta with no FPI base and no
#     seeded base_page must return NULL, never a partial/wrong page.  start_lsn
#     is captured AFTER op1's FPI, base_page is NULL -> the SRF has no base.
{
	seed_and_checkpoint('t258_fc');
	$node->safe_psql('postgres', "INSERT INTO t258_fc VALUES (100, 'op1_fpi')");
	my $start = $node->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');
	$node->safe_psql('postgres', "INSERT INTO t258_fc VALUES (101, 'op2_delta')");
	my $end = $node->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');

	my $is_null = $node->safe_psql('postgres', qq{
		SELECT cluster_thread_apply_redo_test(
				   't258_fc'::regclass, 0, 0,
				   '$start'::pg_lsn, '$end'::pg_lsn, NULL) IS NULL;
	});
	is($is_null, 't',
		'L8 fail-closed: delta with no FPI base, no base_page -> NULL (8.A)');
}

$node->stop;
done_testing();
