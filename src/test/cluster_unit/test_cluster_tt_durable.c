/*-------------------------------------------------------------------------
 *
 * test_cluster_tt_durable.c
 *	  cluster_unit tests for spec-3.11 durable TT slot (D2).
 *
 *	  Covers the I/O-free decision logic + the lookup / by-xid scan logic with
 *	  a mocked cluster_undo_smgr (canned slot / block).  The real file-I/O
 *	  behavior (write -> read -> redo -> restart survival) is end-to-end in
 *	  cluster_tap t/219 (spec-3.11 §4; same pure-unit + e2e-IO split as
 *	  spec-3.9 / spec-3.10 CR -- undo segment I/O needs a real $PGDATA).
 *
 *	  U1  byte-layout (sizeof TTSlot==32, xl_undo_tt_slot_commit==24)
 *	  U2  cluster_tt_durable_redo_decide -- APPLY (newer / same-wrap reuse /
 *	      unused-slot first write) / SKIP (stale) / BADSTATUS (§2.3 last-writer)
 *	  U3  cluster_tt_durable_slot_match -- exact / wrong-xid / UNUSED /
 *	      invalid-scn (xid-match; recycle stamps a new owner xid)
 *	  U4  cluster_tt_slot_durable_lookup -- match / wrong-xid / unused /
 *	      read-fail; stable CLOG-window lookup success / torn-read fail-closed /
 *	      uncommitted fail-closed (mocked smgr)
 *	  U5  cluster_tt_slot_durable_lookup_by_xid -- 0 / 1 / >1 matches (mocked
 *	      block); ambiguity fail-closed (规则 8.A)
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-3.11-durable-tt-slot.md (§4.1)
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_tt_durable.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <string.h>

#include "access/transam.h"
#include "storage/bufpage.h"

#include "cluster/cluster_scn.h"
#include "cluster/cluster_tt_durable.h"
#include "cluster/cluster_undo_cleaner.h" /* scan-pass stats (spec-3.13 D2-B) */

/* spec-3.13 D2-B stub: scan pass compares commit_scn vs horizon. */
int
scn_time_cmp(SCN a, SCN b)
{
	uint64 la = a & ((((uint64)1) << 56) - 1);
	uint64 lb = b & ((((uint64)1) << 56) - 1);

	return (la < lb) ? -1 : (la > lb) ? 1 : 0;
}
#include "cluster/cluster_tt_slot.h"
#include "cluster/cluster_undo_segment.h"
#include "cluster/cluster_undo_smgr.h"
#include "cluster/storage/cluster_undo_alloc.h" /* spec-3.22: file_exists prototype */
#include "cluster/storage/cluster_undo_xlog.h"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();


/* ============================================================
 *	ereport / Assert stubs (cluster_tt_durable.c ereports on I/O fail)
 * ============================================================ */
static int last_ereport_errcode = 0;

bool
errstart(int elevel pg_attribute_unused(), const char *domain pg_attribute_unused())
{
	return true;
}
bool
errstart_cold(int elevel, const char *domain)
{
	return errstart(elevel, domain);
}
void
errfinish(const char *filename pg_attribute_unused(), int lineno pg_attribute_unused(),
		  const char *funcname pg_attribute_unused())
{
	abort(); /* ERROR path not exercised by these tests */
}
int
errcode(int sqlerrcode)
{
	last_ereport_errcode = sqlerrcode;
	return 0;
}
int
errmsg(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}
void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}


/* ============================================================
 *	Mocked cluster_undo_smgr + emit + cluster_node_id
 * ============================================================ */
int cluster_node_id = 0;

static TTSlot g_canned_slot;	  /* returned by read_header_bytes */
static bool g_read_hdr_ok = true; /* read_header_bytes success flag */
static TTSlot g_canned_slot_second;
static bool g_read_hdr_second_enabled = false;
static int g_read_hdr_calls = 0;

static char g_canned_block[BLCKSZ];		  /* returned by read_block for segment 1 */
static uint32 g_canned_block_segment = 1; /* which segment_id the canned block answers */

/* spec-3.15 D5 stub: 0x31 emit (WAL machinery not linked in unit). */
XLogRecPtr
cluster_undo_emit_tt_slot_abort(uint8 instance pg_attribute_unused(),
								uint32 segment_id pg_attribute_unused(),
								uint16 slot_offset pg_attribute_unused(),
								uint16 wrap pg_attribute_unused(),
								TransactionId xid pg_attribute_unused())
{
	return InvalidXLogRecPtr;
}

/* spec-4.8 D7-A stub: 0x90 set-head emit (WAL machinery not linked in unit). */
XLogRecPtr
cluster_undo_emit_tt_slot_set_head(uint8 instance pg_attribute_unused(),
								   uint32 segment_id pg_attribute_unused(),
								   uint16 slot_offset pg_attribute_unused(),
								   uint16 wrap pg_attribute_unused(),
								   TransactionId xid pg_attribute_unused(),
								   UBA first_undo_block pg_attribute_unused())
{
	return InvalidXLogRecPtr;
}

XLogRecPtr
cluster_undo_emit_tt_slot_commit(uint8 instance pg_attribute_unused(),
								 uint32 segment_id pg_attribute_unused(),
								 uint16 slot_offset pg_attribute_unused(),
								 uint16 wrap pg_attribute_unused(),
								 TransactionId xid pg_attribute_unused(),
								 SCN commit_scn pg_attribute_unused())
{
	return InvalidXLogRecPtr;
}

/*
 * Observability hooks (cluster_tt_durable_stat.c) stubbed as no-ops: the pure
 * logic under test bumps counters / brackets wait events through these, but the
 * shmem region + wait-event backend symbols are not linked into cluster_unit.
 */
void
cluster_tt_durable_count_commit(void)
{}
void
cluster_tt_durable_count_lookup(bool hit pg_attribute_unused())
{}
void
cluster_tt_durable_count_by_xid_scan(void)
{}
void
cluster_tt_durable_count_redo_apply(void)
{}
void
cluster_tt_durable_io_wait_start(void)
{}
void
cluster_tt_durable_io_wait_end(void)
{}

/* spec-3.13 D6 stubs: scan-pass wait wrappers (no pgstat in unit). */
void
cluster_undo_cleaner_scan_wait_start(void)
{}
void
cluster_undo_cleaner_scan_wait_end(void)
{}

bool
cluster_undo_smgr_read_header_bytes(ClusterUndoPathIntent intent pg_attribute_unused(),
									uint32 segment_id pg_attribute_unused(),
									uint8 owner_instance pg_attribute_unused(), uint32 offset,
									char *buf, uint32 len)
{
	g_read_hdr_calls++;
	if (!g_read_hdr_ok || buf == NULL || len != sizeof(TTSlot))
		return false;
	(void)offset;
	memcpy(buf,
		   (g_read_hdr_second_enabled && g_read_hdr_calls == 2) ? &g_canned_slot_second
																: &g_canned_slot,
		   sizeof(TTSlot));
	return true;
}

static int g_write_hdr_calls = 0;  /* spec-3.13 D2-B scan-only invariant probe */
static TTSlot g_last_written_slot; /* spec-3.15: capture write payload */

bool
cluster_undo_smgr_write_header_bytes(ClusterUndoPathIntent intent pg_attribute_unused(),
									 uint32 segment_id pg_attribute_unused(),
									 uint8 owner_instance pg_attribute_unused(),
									 uint32 offset pg_attribute_unused(), const char *buf,
									 uint32 len)
{
	g_write_hdr_calls++;
	if (buf != NULL && len == sizeof(TTSlot))
		memcpy(&g_last_written_slot, buf, sizeof(TTSlot));
	return buf != NULL && len == sizeof(TTSlot);
}

bool
cluster_undo_smgr_read_block(ClusterUndoPathIntent intent pg_attribute_unused(), uint32 segment_id,
							 uint8 owner_instance pg_attribute_unused(), uint32 block_no, char *buf)
{
	if (buf == NULL || block_no != 0 || segment_id != g_canned_block_segment)
		return false; /* other segments "don't exist" -> by-xid skips them */
	memcpy(buf, g_canned_block, BLCKSZ);
	return true;
}

/*
 * spec-3.22: a segment that file_exists() reports present but read_block() fails
 * = an unreadable existing segment (I/O error) -> the by-xid scan is incomplete
 * -> SCAN_UNAVAILABLE (never a 0-match).  Default 0 = "no existing segment is
 * unreadable", so every read_block miss is a genuinely-absent segment (sound
 * skip) and the scan stays complete -- matching the legacy by-xid behavior.
 */
static uint32 g_unreadable_existing_segment = 0;

bool
cluster_undo_segment_file_exists(uint8 owner_instance pg_attribute_unused(), uint32 segment_id)
{
	return segment_id == g_unreadable_existing_segment;
}


/* helper: seed g_canned_block's TT slot i with (status, xid, commit_scn). */
static void
seed_block_slot(int i, uint8 status, TransactionId xid, SCN commit_scn)
{
	UndoSegmentHeaderData *hdr = (UndoSegmentHeaderData *)g_canned_block;

	hdr->tt_slots[i].status = status;
	hdr->tt_slots[i].xid = xid;
	hdr->tt_slots[i].commit_scn = commit_scn;
}

static bool g_commit_check_result = true;
static int g_commit_check_calls = 0;
static int g_commit_check_read_calls_seen = 0;
static TransactionId g_commit_check_xid = InvalidTransactionId;

static bool
mock_xid_committed(TransactionId xid)
{
	g_commit_check_calls++;
	g_commit_check_xid = xid;
	g_commit_check_read_calls_seen = g_read_hdr_calls;
	return g_commit_check_result;
}

static void
reset_header_read_mock(void)
{
	g_read_hdr_ok = true;
	g_read_hdr_second_enabled = false;
	g_read_hdr_calls = 0;
	memset(&g_canned_slot, 0, sizeof(g_canned_slot));
	memset(&g_canned_slot_second, 0, sizeof(g_canned_slot_second));
	g_commit_check_result = true;
	g_commit_check_calls = 0;
	g_commit_check_read_calls_seen = 0;
	g_commit_check_xid = InvalidTransactionId;
}


/* ============================================================
 *	U1: byte layout
 * ============================================================ */
UT_TEST(test_layout_sizes)
{
	UT_ASSERT_EQ((int)sizeof(TTSlot), 32);
	UT_ASSERT_EQ((int)sizeof(xl_undo_tt_slot_commit), 24);
}


/* ============================================================
 *	U2: redo wrap-comparison table (§2.3)
 * ============================================================ */
UT_TEST(test_redo_decide_newer_wrap_applies)
{
	/* rec.wrap > slot.wrap -> APPLY (recycle-then-commit normal path). */
	UT_ASSERT_EQ((int)cluster_tt_durable_redo_decide(TT_SLOT_COMMITTED, 100, 5, 200, 6),
				 (int)CLUSTER_TT_REDO_APPLY);
}
UT_TEST(test_redo_decide_same_wrap_same_xid_idempotent)
{
	UT_ASSERT_EQ((int)cluster_tt_durable_redo_decide(TT_SLOT_COMMITTED, 100, 5, 100, 5),
				 (int)CLUSTER_TT_REDO_APPLY);
}
UT_TEST(test_redo_decide_same_wrap_diff_xid_applies)
{
	/* spec-3.11 §2.3 (P0 fix): FREE-path slot reuse keeps wrap unchanged while
	 * the xid differs; BIND is not WAL'd so the on-disk slot lags.  "same wrap,
	 * different xid" during redo is normal reuse -> APPLY (last-writer-wins),
	 * NOT corruption (规则 8.A: crash recovery of a reused slot must not PANIC). */
	UT_ASSERT_EQ((int)cluster_tt_durable_redo_decide(TT_SLOT_COMMITTED, 100, 5, 999, 5),
				 (int)CLUSTER_TT_REDO_APPLY);
}
UT_TEST(test_redo_decide_unused_slot_applies)
{
	/* zero-init (UNUSED, xid 0, wrap 0) slot + first commit record (wrap 0) ->
	 * APPLY (the crash-recovery PANIC this fix repaired: t/219 L2/L7). */
	UT_ASSERT_EQ((int)cluster_tt_durable_redo_decide(TT_SLOT_UNUSED, 0, 0, 768, 0),
				 (int)CLUSTER_TT_REDO_APPLY);
}
UT_TEST(test_redo_decide_older_wrap_skips)
{
	UT_ASSERT_EQ((int)cluster_tt_durable_redo_decide(TT_SLOT_COMMITTED, 100, 6, 100, 5),
				 (int)CLUSTER_TT_REDO_SKIP);
}
UT_TEST(test_redo_decide_bad_status)
{
	/* status > TT_SLOT_RECYCLABLE (4) -> BADSTATUS even if wrap would apply. */
	UT_ASSERT_EQ((int)cluster_tt_durable_redo_decide(99, 100, 5, 200, 6),
				 (int)CLUSTER_TT_REDO_BADSTATUS);
}


/* ============================================================
 *	U3: slot_match predicate (C5)
 * ============================================================ */
UT_TEST(test_slot_match_exact)
{
	UT_ASSERT_EQ((int)cluster_tt_durable_slot_match(TT_SLOT_COMMITTED, 100, 5, scn_encode(1, 42),
													100, CLUSTER_TT_WRAP_ANY),
				 1);
}
UT_TEST(test_slot_match_wrong_xid)
{
	/* recycle stamps a new owner xid -> xid mismatch is the recycle detector. */
	UT_ASSERT_EQ((int)cluster_tt_durable_slot_match(TT_SLOT_COMMITTED, 999, 5, scn_encode(1, 42),
													100, CLUSTER_TT_WRAP_ANY),
				 0);
}
UT_TEST(test_slot_match_unused)
{
	UT_ASSERT_EQ((int)cluster_tt_durable_slot_match(TT_SLOT_UNUSED, 100, 5, scn_encode(1, 42), 100,
													CLUSTER_TT_WRAP_ANY),
				 0);
}
UT_TEST(test_slot_match_invalid_scn)
{
	UT_ASSERT_EQ((int)cluster_tt_durable_slot_match(TT_SLOT_COMMITTED, 100, 5, InvalidScn, 100,
													CLUSTER_TT_WRAP_ANY),
				 0);

	/* spec-4.5a G4 (F3, L9b): wrap-qualified matching.  A known expected
	 * wrap MATCHES its own generation and EXCLUDES a recycled slot whose
	 * 32-bit xid wrapped to the same value (same xid, different wrap) --
	 * the case xid-only matching could not detect. */
	UT_ASSERT_EQ(
		(int)cluster_tt_durable_slot_match(TT_SLOT_COMMITTED, 100, 5, scn_encode(1, 42), 100, 5),
		1);
	UT_ASSERT_EQ(
		(int)cluster_tt_durable_slot_match(TT_SLOT_COMMITTED, 100, 6, scn_encode(1, 42), 100, 5),
		0);
}


/* ============================================================
 *	U4: cluster_tt_slot_durable_lookup (mocked smgr)
 * ============================================================ */
UT_TEST(test_lookup_match)
{
	SCN got = InvalidScn;

	g_read_hdr_ok = true;
	memset(&g_canned_slot, 0, sizeof(g_canned_slot));
	g_canned_slot.status = TT_SLOT_COMMITTED;
	g_canned_slot.xid = 100;
	g_canned_slot.wrap = 5;
	g_canned_slot.commit_scn = scn_encode(1, 42);

	UT_ASSERT_EQ((int)cluster_tt_slot_durable_lookup(1, 0, 100, CLUSTER_TT_WRAP_ANY, &got), 1);
	UT_ASSERT_EQ((int)(scn_local(got)), 42);
}
UT_TEST(test_lookup_wrong_xid_miss)
{
	SCN got = InvalidScn;

	g_read_hdr_ok = true;
	memset(&g_canned_slot, 0, sizeof(g_canned_slot));
	g_canned_slot.status = TT_SLOT_COMMITTED;
	g_canned_slot.xid = 999; /* slot recycled to a new owner xid */
	g_canned_slot.commit_scn = scn_encode(1, 42);

	UT_ASSERT_EQ((int)cluster_tt_slot_durable_lookup(1, 0, 100, CLUSTER_TT_WRAP_ANY, &got), 0);
}
UT_TEST(test_lookup_unused_miss)
{
	SCN got = InvalidScn;

	g_read_hdr_ok = true;
	memset(&g_canned_slot, 0, sizeof(g_canned_slot)); /* status=UNUSED(0) */

	UT_ASSERT_EQ((int)cluster_tt_slot_durable_lookup(1, 0, 100, CLUSTER_TT_WRAP_ANY, &got), 0);
}
UT_TEST(test_lookup_read_fail_miss)
{
	SCN got = InvalidScn;

	g_read_hdr_ok = false; /* segment absent / I/O error */
	UT_ASSERT_EQ((int)cluster_tt_slot_durable_lookup(1, 0, 100, CLUSTER_TT_WRAP_ANY, &got), 0);
}
UT_TEST(test_lookup_committed_stable_success)
{
	SCN got = InvalidScn;

	reset_header_read_mock();
	g_canned_slot.status = TT_SLOT_COMMITTED;
	g_canned_slot.xid = 100;
	g_canned_slot.wrap = 5;
	g_canned_slot.commit_scn = scn_encode(1, 42);

	UT_ASSERT_EQ((int)cluster_tt_slot_durable_lookup_committed_stable(
					 1, 0, 100, CLUSTER_TT_WRAP_ANY, mock_xid_committed, &got),
				 1);
	UT_ASSERT_EQ((int)(scn_local(got)), 42);
	UT_ASSERT_EQ(g_read_hdr_calls, 2);
	UT_ASSERT_EQ(g_commit_check_calls, 1);
	UT_ASSERT_EQ((int)g_commit_check_xid, 100);
	UT_ASSERT_EQ(g_commit_check_read_calls_seen, 1);
}
UT_TEST(test_lookup_committed_stable_torn_read_failclosed)
{
	SCN got = InvalidScn;

	reset_header_read_mock();
	g_canned_slot.status = TT_SLOT_COMMITTED;
	g_canned_slot.xid = 100;
	g_canned_slot.wrap = 5;
	g_canned_slot.commit_scn = scn_encode(1, 42);
	g_canned_slot_second = g_canned_slot;
	g_canned_slot_second.commit_scn = scn_encode(1, 17);
	g_read_hdr_second_enabled = true;

	UT_ASSERT_EQ((int)cluster_tt_slot_durable_lookup_committed_stable(
					 1, 0, 100, CLUSTER_TT_WRAP_ANY, mock_xid_committed, &got),
				 0);
	UT_ASSERT_EQ(g_read_hdr_calls, 2);
	UT_ASSERT_EQ(g_commit_check_calls, 1);
}
UT_TEST(test_lookup_committed_stable_uncommitted_failclosed)
{
	SCN got = InvalidScn;

	reset_header_read_mock();
	g_canned_slot.status = TT_SLOT_COMMITTED;
	g_canned_slot.xid = 100;
	g_canned_slot.wrap = 5;
	g_canned_slot.commit_scn = scn_encode(1, 42);
	g_commit_check_result = false;

	UT_ASSERT_EQ((int)cluster_tt_slot_durable_lookup_committed_stable(
					 1, 0, 100, CLUSTER_TT_WRAP_ANY, mock_xid_committed, &got),
				 0);
	UT_ASSERT_EQ(g_read_hdr_calls, 1);
	UT_ASSERT_EQ(g_commit_check_calls, 1);
}


/* ============================================================
 *	U5: cluster_tt_slot_durable_lookup_by_xid (mocked block)
 * ============================================================ */
UT_TEST(test_by_xid_zero_match_miss)
{
	SCN got = InvalidScn;

	cluster_node_id = 0;
	memset(g_canned_block, 0, sizeof(g_canned_block)); /* all UNUSED */
	UT_ASSERT_EQ((int)cluster_tt_slot_durable_lookup_by_xid(12345, &got), 0);
}
UT_TEST(test_by_xid_one_match)
{
	SCN got = InvalidScn;

	cluster_node_id = 0;
	memset(g_canned_block, 0, sizeof(g_canned_block));
	seed_block_slot(3, TT_SLOT_COMMITTED, 12345, scn_encode(1, 77));
	UT_ASSERT_EQ((int)cluster_tt_slot_durable_lookup_by_xid(12345, &got), 1);
	UT_ASSERT_EQ((int)(scn_local(got)), 77);
}
UT_TEST(test_by_xid_two_match_ambiguous_failclosed)
{
	SCN got = InvalidScn;

	cluster_node_id = 0;
	memset(g_canned_block, 0, sizeof(g_canned_block));
	seed_block_slot(3, TT_SLOT_COMMITTED, 12345, scn_encode(1, 77));
	seed_block_slot(9, TT_SLOT_COMMITTED, 12345, scn_encode(1, 88)); /* duplicate xid */
	/* 规则 8.A: >1 match -> fail-closed (never first-match). */
	UT_ASSERT_EQ((int)cluster_tt_slot_durable_lookup_by_xid(12345, &got), 0);
}

/* ============================================================
 *	U7: spec-3.22 durable by-xid RESOLVE enum + pure classifier.
 *
 *	cluster_tt_durable_classify maps the scan tallies to the resolve
 *	verdict; the §2.4 fix is that a slot OWNED BY xid with an unstamped
 *	(invalid) commit_scn is a 1-match (XID_MATCH_INVALID_SCN, retained,
 *	NOT below horizon) -- never conflated with a 0-match (recycled,
 *	provably below horizon).  scan_complete gates the 0/1 tally so an
 *	unreadable existing segment is SCAN_UNAVAILABLE, never a 0-match.
 * ============================================================ */

/* --- pure classifier truth table (no I/O) --- */
UT_TEST(test_classify_zero_match_complete_is_recycled)
{
	UT_ASSERT_EQ((int)cluster_tt_durable_classify(0, false, true),
				 (int)CLUSTER_TT_DURABLE_RECYCLED_ZERO_MATCH);
}
UT_TEST(test_classify_one_valid_complete_is_resolved)
{
	UT_ASSERT_EQ((int)cluster_tt_durable_classify(1, true, true),
				 (int)CLUSTER_TT_DURABLE_RESOLVED_SCN);
}
UT_TEST(test_classify_one_invalid_complete_is_invalid_scn)
{
	/* §2.4: owned-by-xid but commit_scn unstamped -> retained, NOT recycled. */
	UT_ASSERT_EQ((int)cluster_tt_durable_classify(1, false, true),
				 (int)CLUSTER_TT_DURABLE_XID_MATCH_INVALID_SCN);
}
UT_TEST(test_classify_two_match_is_ambiguous_regardless)
{
	UT_ASSERT_EQ((int)cluster_tt_durable_classify(2, true, true),
				 (int)CLUSTER_TT_DURABLE_AMBIGUOUS_WRAP);
	/* >1 is definitive ambiguity -- wins even over an incomplete scan. */
	UT_ASSERT_EQ((int)cluster_tt_durable_classify(2, false, false),
				 (int)CLUSTER_TT_DURABLE_AMBIGUOUS_WRAP);
}
UT_TEST(test_classify_incomplete_scan_is_unavailable)
{
	/* incomplete scan beats a 0- or 1-tally: cannot prove recycled / resolved. */
	UT_ASSERT_EQ((int)cluster_tt_durable_classify(0, false, false),
				 (int)CLUSTER_TT_DURABLE_SCAN_UNAVAILABLE);
	UT_ASSERT_EQ((int)cluster_tt_durable_classify(1, true, false),
				 (int)CLUSTER_TT_DURABLE_SCAN_UNAVAILABLE);
}

/* --- resolve-by-xid scan (mocked smgr) --- */
UT_TEST(test_resolve_zero_match_recycled)
{
	SCN got = scn_encode(1, 9);

	cluster_node_id = 0;
	g_unreadable_existing_segment = 0;
	memset(g_canned_block, 0, sizeof(g_canned_block));
	/* slot recycled to a DIFFERENT owner xid -> 0 matches for the target. */
	seed_block_slot(5, TT_SLOT_COMMITTED, 999, scn_encode(1, 50));
	UT_ASSERT_EQ((int)cluster_tt_slot_durable_resolve_by_xid(12345, CLUSTER_TT_WRAP_ANY, &got, NULL,
															 NULL, NULL),
				 (int)CLUSTER_TT_DURABLE_RECYCLED_ZERO_MATCH);
	UT_ASSERT_EQ((int)SCN_VALID(got), 0); /* commit_scn cleared on non-RESOLVED */
}
UT_TEST(test_resolve_one_valid_resolved)
{
	SCN got = InvalidScn;

	cluster_node_id = 0;
	g_unreadable_existing_segment = 0;
	memset(g_canned_block, 0, sizeof(g_canned_block));
	seed_block_slot(3, TT_SLOT_COMMITTED, 12345, scn_encode(1, 77));
	UT_ASSERT_EQ((int)cluster_tt_slot_durable_resolve_by_xid(12345, CLUSTER_TT_WRAP_ANY, &got, NULL,
															 NULL, NULL),
				 (int)CLUSTER_TT_DURABLE_RESOLVED_SCN);
	UT_ASSERT_EQ((int)(scn_local(got)), 77);
}
/* spec-5.55 D1/U1: RESOLVED_SCN also reports the matched durable slot identity
 * (out_seg/out_slot/out_wrap) for the shared resolver cache position hint; NULL
 * out params (legacy callers) resolve unchanged. */
UT_TEST(test_resolve_reports_matched_identity)
{
	SCN got = InvalidScn;
	uint16 seg = 0xffff;
	uint16 slot = 0xffff;
	uint16 wrap = 0xffff;

	cluster_node_id = 0;
	g_unreadable_existing_segment = 0;
	memset(g_canned_block, 0, sizeof(g_canned_block));
	seed_block_slot(5, TT_SLOT_COMMITTED, 12345, scn_encode(1, 77));
	UT_ASSERT_EQ((int)cluster_tt_slot_durable_resolve_by_xid(12345, CLUSTER_TT_WRAP_ANY, &got, &seg,
															 &slot, &wrap),
				 (int)CLUSTER_TT_DURABLE_RESOLVED_SCN);
	UT_ASSERT_EQ((int)seg, 1);	/* g_canned_block answers for segment_id 1 */
	UT_ASSERT_EQ((int)slot, 5); /* the seeded slot offset */
	UT_ASSERT_EQ((int)wrap, 0); /* seeded wrap default */

	/* a 0-match (RECYCLED) leaves the out params untouched (only set on RESOLVED). */
	seg = slot = wrap = 0xffff;
	memset(g_canned_block, 0, sizeof(g_canned_block));
	seed_block_slot(5, TT_SLOT_COMMITTED, 999, scn_encode(1, 50)); /* different xid */
	got = InvalidScn;
	UT_ASSERT_EQ((int)cluster_tt_slot_durable_resolve_by_xid(12345, CLUSTER_TT_WRAP_ANY, &got, &seg,
															 &slot, &wrap),
				 (int)CLUSTER_TT_DURABLE_RECYCLED_ZERO_MATCH);
	UT_ASSERT_EQ((int)seg, 0xffff); /* untouched on non-RESOLVED */
}
UT_TEST(test_resolve_xid_match_invalid_scn_not_recycled)
{
	SCN got = scn_encode(1, 9);

	/* THE conflation fix: COMMITTED slot owned by xid, commit_scn unstamped.
	 * Legacy lookup_by_xid counted this as 0 (SCN_VALID gate) -> looked
	 * recycled.  The resolve enum must report XID_MATCH_INVALID_SCN. */
	cluster_node_id = 0;
	g_unreadable_existing_segment = 0;
	memset(g_canned_block, 0, sizeof(g_canned_block));
	seed_block_slot(7, TT_SLOT_COMMITTED, 12345, InvalidScn);
	UT_ASSERT_EQ((int)cluster_tt_slot_durable_resolve_by_xid(12345, CLUSTER_TT_WRAP_ANY, &got, NULL,
															 NULL, NULL),
				 (int)CLUSTER_TT_DURABLE_XID_MATCH_INVALID_SCN);
	UT_ASSERT_EQ((int)SCN_VALID(got), 0);
}
UT_TEST(test_resolve_two_match_ambiguous)
{
	SCN got = InvalidScn;

	cluster_node_id = 0;
	g_unreadable_existing_segment = 0;
	memset(g_canned_block, 0, sizeof(g_canned_block));
	seed_block_slot(3, TT_SLOT_COMMITTED, 12345, scn_encode(1, 77));
	seed_block_slot(9, TT_SLOT_COMMITTED, 12345, scn_encode(1, 88));
	UT_ASSERT_EQ((int)cluster_tt_slot_durable_resolve_by_xid(12345, CLUSTER_TT_WRAP_ANY, &got, NULL,
															 NULL, NULL),
				 (int)CLUSTER_TT_DURABLE_AMBIGUOUS_WRAP);
}
UT_TEST(test_resolve_node_degraded_unavailable)
{
	SCN got = InvalidScn;

	/* single-node degraded: NO durable scan possible -> SCAN_UNAVAILABLE,
	 * never a 0-match (which would false-prove "recycled below horizon"). */
	cluster_node_id = -1;
	UT_ASSERT_EQ((int)cluster_tt_slot_durable_resolve_by_xid(12345, CLUSTER_TT_WRAP_ANY, &got, NULL,
															 NULL, NULL),
				 (int)CLUSTER_TT_DURABLE_SCAN_UNAVAILABLE);
	cluster_node_id = 0; /* restore for later tests */
}
UT_TEST(test_resolve_unreadable_existing_segment_unavailable)
{
	SCN got = InvalidScn;

	/* an existing segment that fails to read -> incomplete scan -> UNAVAILABLE,
	 * never a 0-match.  Segment 2 is not the canned block (read_block fails) but
	 * file_exists() reports it present (I/O error on a real segment). */
	cluster_node_id = 0;
	memset(g_canned_block, 0, sizeof(g_canned_block));
	g_unreadable_existing_segment = 2;
	UT_ASSERT_EQ((int)cluster_tt_slot_durable_resolve_by_xid(12345, CLUSTER_TT_WRAP_ANY, &got, NULL,
															 NULL, NULL),
				 (int)CLUSTER_TT_DURABLE_SCAN_UNAVAILABLE);
	g_unreadable_existing_segment = 0; /* restore */
}
UT_TEST(test_resolve_lookup_wrapper_maps_resolved_only)
{
	/* the thin wrapper (xmin-side caller) is true IFF RESOLVED_SCN. */
	SCN got = InvalidScn;

	cluster_node_id = 0;
	g_unreadable_existing_segment = 0;
	memset(g_canned_block, 0, sizeof(g_canned_block));
	seed_block_slot(3, TT_SLOT_COMMITTED, 12345, scn_encode(1, 77));
	UT_ASSERT_EQ((int)cluster_tt_slot_durable_lookup_by_xid(12345, &got), 1);
	UT_ASSERT_EQ((int)(scn_local(got)), 77);

	/* unstamped owned-by-xid -> wrapper false (was already false legacy-side). */
	memset(g_canned_block, 0, sizeof(g_canned_block));
	seed_block_slot(3, TT_SLOT_COMMITTED, 12345, InvalidScn);
	UT_ASSERT_EQ((int)cluster_tt_slot_durable_lookup_by_xid(12345, &got), 0);
}


/* ============================================================
 *	U6: cluster_undo_segment_tt_header_scan_pass (spec-3.13 D2-B,
 *	scan-only) — classification + zero-write invariant.
 * ============================================================ */

UT_TEST(test_scan_pass_classifies_inventory)
{
	ClusterUndoCleanerPassStats stats = { 0 };
	bool ok;

	memset(g_canned_block, 0, sizeof(g_canned_block));
	seed_block_slot(0, TT_SLOT_COMMITTED, (TransactionId)100, (SCN)5);	/* below */
	seed_block_slot(1, TT_SLOT_COMMITTED, (TransactionId)101, (SCN)20); /* == horizon: retained */
	seed_block_slot(2, TT_SLOT_COMMITTED, (TransactionId)102, (SCN)0);	/* unresolved (8.A) */
	seed_block_slot(3, TT_SLOT_ACTIVE, (TransactionId)103, (SCN)0);		/* stale residue */
	seed_block_slot(4, TT_SLOT_ABORTED, (TransactionId)104, (SCN)0);	/* no inventory impact */

	ok = cluster_undo_segment_tt_header_scan_pass(1, 1, (SCN)20, &stats);
	UT_ASSERT_EQ((int)ok, 1);
	UT_ASSERT_EQ((int)stats.header_tt_slots_below_horizon, 1);
	UT_ASSERT_EQ((int)stats.header_unresolved_committed, 1);
	UT_ASSERT_EQ((int)stats.stale_active_skipped, 1);
	UT_ASSERT_EQ((int)stats.segments_scanned, 1);
}

UT_TEST(test_scan_pass_writes_nothing)
{
	/* v0.3 (3) scan-only invariant: the pass must never call the smgr write
	 * surface, and the canned block bytes must be bit-identical after. */
	ClusterUndoCleanerPassStats stats = { 0 };
	char before[BLCKSZ];

	memset(g_canned_block, 0, sizeof(g_canned_block));
	seed_block_slot(0, TT_SLOT_COMMITTED, (TransactionId)100, (SCN)5);
	memcpy(before, g_canned_block, BLCKSZ);
	g_write_hdr_calls = 0;

	(void)cluster_undo_segment_tt_header_scan_pass(1, 1, (SCN)20, &stats);

	UT_ASSERT_EQ(g_write_hdr_calls, 0);
	UT_ASSERT_EQ(memcmp(before, g_canned_block, BLCKSZ), 0);
}

UT_TEST(test_scan_pass_read_fail_returns_false)
{
	ClusterUndoCleanerPassStats stats = { 0 };
	bool ok;

	/* segment 7 has no canned block -> read_block returns false. */
	ok = cluster_undo_segment_tt_header_scan_pass(7, 1, (SCN)20, &stats);
	UT_ASSERT_EQ((int)ok, 0);
	UT_ASSERT_EQ((int)stats.segments_scanned, 0);
}


/* ============================================================
 *	spec-3.15 D5 — durable_abort stamps ABORTED preserving identity
 * ============================================================ */

UT_TEST(test_durable_abort_preserves_identity)
{
	/* V-2: identity (xid/wrap) must survive so by-exact-key lookups
	 * resolve ABORTED instead of missing into 53R97. */
	g_read_hdr_ok = true;
	memset(&g_canned_slot, 0, sizeof(g_canned_slot));
	g_canned_slot.status = TT_SLOT_ACTIVE;
	g_canned_slot.xid = (TransactionId)777;
	g_canned_slot.wrap = 4;
	g_canned_slot.commit_scn = (SCN)123; /* stale garbage to be cleared */

	g_write_hdr_calls = 0;
	cluster_tt_slot_durable_abort(1, 7, (TransactionId)777, 4);

	UT_ASSERT_EQ(g_write_hdr_calls, 1);
	UT_ASSERT_EQ((int)g_last_written_slot.status, (int)TT_SLOT_ABORTED);
	UT_ASSERT_EQ((int)g_last_written_slot.xid, 777);
	UT_ASSERT_EQ((int)g_last_written_slot.wrap, 4);
	UT_ASSERT_EQ((int)SCN_VALID(g_last_written_slot.commit_scn), 0);
}


/* ============================================================
 *	spec-3.16 D2 — 0x30/0x60 redo decide table idempotent + shared
 *
 *	cluster_tt_durable_redo_decide is the SINGLE last-writer-wins table
 *	for BOTH XLOG_UNDO_TT_SLOT_COMMIT (0x30) and XLOG_UNDO_TT_SLOT_ABORT
 *	(0x60); the handlers differ only in the status they stamp on APPLY
 *	(COMMITTED vs ABORTED).  These tests lock that the table is a pure
 *	function (replay-idempotent) and that the abort path reaches the
 *	same decisions (anti-divergence, L216).
 * ============================================================ */

UT_TEST(test_redo_decide_idempotent_replay)
{
	/* Pure function: same inputs -> same decision, any number of replays
	 * (the byte-level idempotency of the on-disk slot is the e2e job of
	 * t/225 L1/L2; here we lock the decision determinism). */
	int i;

	for (i = 0; i < 5; i++) {
		UT_ASSERT_EQ((int)cluster_tt_durable_redo_decide(TT_SLOT_COMMITTED, 100, 5, 100, 5),
					 (int)CLUSTER_TT_REDO_APPLY); /* same owner: idempotent re-apply */
		UT_ASSERT_EQ((int)cluster_tt_durable_redo_decide(TT_SLOT_COMMITTED, 100, 6, 100, 5),
					 (int)CLUSTER_TT_REDO_SKIP); /* disk newer wrap: stale record */
		UT_ASSERT_EQ((int)cluster_tt_durable_redo_decide(TT_SLOT_ACTIVE, 100, 5, 200, 6),
					 (int)CLUSTER_TT_REDO_APPLY); /* recycle-then-write */
	}
}

UT_TEST(test_redo_decide_abort_shares_commit_table)
{
	/* The 0x60 abort redo handler feeds the SAME decide table as 0x30.
	 * Enumerate the abort-replay scenarios and confirm they reach the
	 * same verdicts -- a future change that forks abort decisioning must
	 * break this test. */

	/* abort record replayed onto its own already-ABORTED slot (crash
	 * after the abort write): same wrap/xid -> idempotent APPLY. */
	UT_ASSERT_EQ((int)cluster_tt_durable_redo_decide(TT_SLOT_ABORTED, 777, 4, 777, 4),
				 (int)CLUSTER_TT_REDO_APPLY);
	/* abort record onto a slot a newer owner already took (higher wrap)
	 * -> SKIP (do not clobber the newer transaction). */
	UT_ASSERT_EQ((int)cluster_tt_durable_redo_decide(TT_SLOT_ACTIVE, 800, 5, 777, 4),
				 (int)CLUSTER_TT_REDO_SKIP);
	/* abort record onto the still-ACTIVE slot it owns -> APPLY. */
	UT_ASSERT_EQ((int)cluster_tt_durable_redo_decide(TT_SLOT_ACTIVE, 777, 4, 777, 4),
				 (int)CLUSTER_TT_REDO_APPLY);
	/* invalid disk status -> corruption regardless of commit/abort. */
	UT_ASSERT_EQ((int)cluster_tt_durable_redo_decide(99, 777, 4, 777, 4),
				 (int)CLUSTER_TT_REDO_BADSTATUS);
}


/* ============================================================
 *	spec-4.8 D1: crash-left ACTIVE liveness classifier truth table
 * ============================================================ */
UT_TEST(test_recovery_liveness_indeterminable_is_ambiguous)
{
	/* !determinable -> AMBIGUOUS (fail-closed -> caller aborts the slot). */
	UT_ASSERT_EQ((int)cluster_tt_recovery_classify_liveness(false, false, false),
				 (int)CLUSTER_TT_RECOVERY_AMBIGUOUS);
	UT_ASSERT_EQ((int)cluster_tt_recovery_classify_liveness(false, true, true),
				 (int)CLUSTER_TT_RECOVERY_AMBIGUOUS);
}
UT_TEST(test_recovery_liveness_committed_is_live)
{
	/* did_commit -> LIVE (never abort a committed xact, even if slot ACTIVE). */
	UT_ASSERT_EQ((int)cluster_tt_recovery_classify_liveness(true, true, false),
				 (int)CLUSTER_TT_RECOVERY_LIVE);
}
UT_TEST(test_recovery_liveness_committed_precedence_over_inprogress)
{
	/* did_commit wins over is_in_progress (fail-safe precedence). */
	UT_ASSERT_EQ((int)cluster_tt_recovery_classify_liveness(true, true, true),
				 (int)CLUSTER_TT_RECOVERY_LIVE);
}
UT_TEST(test_recovery_liveness_inprogress_is_live)
{
	/* !did_commit && is_in_progress -> LIVE (resurrected prepared 2PC). */
	UT_ASSERT_EQ((int)cluster_tt_recovery_classify_liveness(true, false, true),
				 (int)CLUSTER_TT_RECOVERY_LIVE);
}
UT_TEST(test_recovery_liveness_neither_is_dead)
{
	/* determinable, !committed, !in_progress -> DEAD (crash-left -> ABORTED). */
	UT_ASSERT_EQ((int)cluster_tt_recovery_classify_liveness(true, false, false),
				 (int)CLUSTER_TT_RECOVERY_DEAD);
}
UT_TEST(test_recovery_liveness_dead_and_ambiguous_both_abort)
{
	/* The resolve loop aborts both DEAD and AMBIGUOUS; LIVE alone is kept. */
	UT_ASSERT(cluster_tt_recovery_classify_liveness(true, false, false)
			  != CLUSTER_TT_RECOVERY_LIVE);
	UT_ASSERT(cluster_tt_recovery_classify_liveness(false, false, false)
			  != CLUSTER_TT_RECOVERY_LIVE);
}

/* ============================================================
 *	spec-4.8 D2: cross-node TT authority recovered_through LSN gate
 * ============================================================ */
UT_TEST(test_remote_authority_anchor_zero_skips_gate)
{
	/* anchor_lsn 0 (unwritten page) -> is_materialized-only (pre-D2). */
	UT_ASSERT(cluster_tt_recovery_remote_authority_covers(0, 0));
	UT_ASSERT(cluster_tt_recovery_remote_authority_covers(100, 0));
}
UT_TEST(test_remote_authority_recovered_covers_anchor)
{
	/* recovered_through >= anchor -> trust the durable outcome. */
	UT_ASSERT(cluster_tt_recovery_remote_authority_covers(200, 100));
}
UT_TEST(test_remote_authority_recovered_equal_anchor)
{
	/* boundary: recovered_through == anchor -> covered (>=). */
	UT_ASSERT(cluster_tt_recovery_remote_authority_covers(100, 100));
}
UT_TEST(test_remote_authority_under_recovered_failclosed)
{
	/* recovered_through < anchor -> NOT covered -> caller fail-closes. */
	UT_ASSERT(!cluster_tt_recovery_remote_authority_covers(99, 100));
	UT_ASSERT(!cluster_tt_recovery_remote_authority_covers(0, 1));
}

/* ============================================================
 *	spec-4.8 D3 (task#90): WRAP_ANY by-xid wrap-suspect gate
 * ============================================================ */
UT_TEST(test_wrap_suspect_wrap_checked_never_suspect)
{
	/* expected_wrap != WRAP_ANY (already disambiguated) -> never suspect. */
	UT_ASSERT(!cluster_tt_recovery_wrap_suspect(5, scn_encode(1, 10), scn_encode(1, 20), false));
}
UT_TEST(test_wrap_suspect_retention_reliable_never_suspect)
{
	/* retention reliable: below-horizon 1-match is a legit recycle-lag commit
	 * (a wrapped collision's slot would already be recycled) -> not suspect. */
	UT_ASSERT(!cluster_tt_recovery_wrap_suspect(CLUSTER_TT_WRAP_ANY, scn_encode(1, 10),
												scn_encode(1, 20), true));
}
UT_TEST(test_wrap_suspect_below_horizon_unreliable_is_suspect)
{
	/* WRAP_ANY + unreliable retention + below horizon -> wrap-suspect. */
	UT_ASSERT(cluster_tt_recovery_wrap_suspect(CLUSTER_TT_WRAP_ANY, scn_encode(1, 10),
											   scn_encode(1, 20), false));
}
UT_TEST(test_wrap_suspect_at_or_above_horizon_not_suspect)
{
	/* WRAP_ANY + unreliable + match at/above horizon -> not below -> trusted. */
	UT_ASSERT(!cluster_tt_recovery_wrap_suspect(CLUSTER_TT_WRAP_ANY, scn_encode(1, 20),
												scn_encode(1, 20), false));
	UT_ASSERT(!cluster_tt_recovery_wrap_suspect(CLUSTER_TT_WRAP_ANY, scn_encode(1, 30),
												scn_encode(1, 20), false));
}
UT_TEST(test_wrap_suspect_unjudgeable_unreliable_failclosed)
{
	/* WRAP_ANY + unreliable + invalid horizon/scn -> cannot judge -> suspect
	 * (fail-closed). */
	UT_ASSERT(cluster_tt_recovery_wrap_suspect(CLUSTER_TT_WRAP_ANY, scn_encode(1, 10), InvalidScn,
											   false));
	UT_ASSERT(cluster_tt_recovery_wrap_suspect(CLUSTER_TT_WRAP_ANY, InvalidScn, scn_encode(1, 20),
											   false));
}

/* ============================================================
 *	spec-4.8 D7 (mini-plan v2): index-aware physical rollback safety matrix
 * ============================================================ */
UT_TEST(test_revert_non_delete_failclosed)
{
	/* INSERT/UPDATE (not a DELETE record) -> fail-closed (index-unsafe),
	 * regardless of the other inputs. */
	UT_ASSERT_EQ((int)cluster_tt_recovery_classify_revert(false, true, true, false),
				 (int)CLUSTER_TT_REVERT_FAILCLOSED);
	UT_ASSERT_EQ((int)cluster_tt_recovery_classify_revert(false, false, false, true),
				 (int)CLUSTER_TT_REVERT_FAILCLOSED);
}
UT_TEST(test_revert_delete_not_aborted_failclosed)
{
	/* DELETE record but the deleter is not aborted -> never revert. */
	UT_ASSERT_EQ((int)cluster_tt_recovery_classify_revert(true, false, true, false),
				 (int)CLUSTER_TT_REVERT_FAILCLOSED);
}
UT_TEST(test_revert_delete_already_clear_skip_done)
{
	/* DELETE + aborted + xmax already clear -> idempotent SKIP (done). */
	UT_ASSERT_EQ((int)cluster_tt_recovery_classify_revert(true, true, false, true),
				 (int)CLUSTER_TT_REVERT_SKIP_DONE);
	/* SKIP_DONE wins even if identity would otherwise match. */
	UT_ASSERT_EQ((int)cluster_tt_recovery_classify_revert(true, true, true, true),
				 (int)CLUSTER_TT_REVERT_SKIP_DONE);
}
UT_TEST(test_revert_delete_identity_match_apply)
{
	/* DELETE + aborted + not-clear + tuple still carries this deleter's xmax
	 * -> APPLY (clear xmax). */
	UT_ASSERT_EQ((int)cluster_tt_recovery_classify_revert(true, true, true, false),
				 (int)CLUSTER_TT_REVERT_APPLY);
}
UT_TEST(test_revert_delete_identity_mismatch_failclosed)
{
	/* DELETE + aborted + not-clear but the tuple no longer carries this
	 * deleter's xmax (slot/tuple reused) -> fail-closed (identity gate). */
	UT_ASSERT_EQ((int)cluster_tt_recovery_classify_revert(true, true, false, false),
				 (int)CLUSTER_TT_REVERT_FAILCLOSED);
}


int
main(int argc, char **argv)
{
	UT_PLAN(60);

	UT_RUN(test_layout_sizes);

	UT_RUN(test_redo_decide_newer_wrap_applies);
	UT_RUN(test_redo_decide_same_wrap_same_xid_idempotent);
	UT_RUN(test_redo_decide_same_wrap_diff_xid_applies);
	UT_RUN(test_redo_decide_unused_slot_applies);
	UT_RUN(test_redo_decide_older_wrap_skips);
	UT_RUN(test_redo_decide_bad_status);

	UT_RUN(test_slot_match_exact);
	UT_RUN(test_slot_match_wrong_xid);
	UT_RUN(test_slot_match_unused);
	UT_RUN(test_slot_match_invalid_scn);

	UT_RUN(test_lookup_match);
	UT_RUN(test_lookup_wrong_xid_miss);
	UT_RUN(test_lookup_unused_miss);
	UT_RUN(test_lookup_read_fail_miss);
	UT_RUN(test_lookup_committed_stable_success);
	UT_RUN(test_lookup_committed_stable_torn_read_failclosed);
	UT_RUN(test_lookup_committed_stable_uncommitted_failclosed);

	UT_RUN(test_by_xid_zero_match_miss);
	UT_RUN(test_by_xid_one_match);
	UT_RUN(test_by_xid_two_match_ambiguous_failclosed);

	UT_RUN(test_classify_zero_match_complete_is_recycled);
	UT_RUN(test_classify_one_valid_complete_is_resolved);
	UT_RUN(test_classify_one_invalid_complete_is_invalid_scn);
	UT_RUN(test_classify_two_match_is_ambiguous_regardless);
	UT_RUN(test_classify_incomplete_scan_is_unavailable);
	UT_RUN(test_resolve_zero_match_recycled);
	UT_RUN(test_resolve_one_valid_resolved);
	UT_RUN(test_resolve_reports_matched_identity);
	UT_RUN(test_resolve_xid_match_invalid_scn_not_recycled);
	UT_RUN(test_resolve_two_match_ambiguous);
	UT_RUN(test_resolve_node_degraded_unavailable);
	UT_RUN(test_resolve_unreadable_existing_segment_unavailable);
	UT_RUN(test_resolve_lookup_wrapper_maps_resolved_only);

	UT_RUN(test_scan_pass_classifies_inventory);
	UT_RUN(test_scan_pass_writes_nothing);
	UT_RUN(test_scan_pass_read_fail_returns_false);

	UT_RUN(test_durable_abort_preserves_identity);

	UT_RUN(test_redo_decide_idempotent_replay);
	UT_RUN(test_redo_decide_abort_shares_commit_table);

	UT_RUN(test_recovery_liveness_indeterminable_is_ambiguous);
	UT_RUN(test_recovery_liveness_committed_is_live);
	UT_RUN(test_recovery_liveness_committed_precedence_over_inprogress);
	UT_RUN(test_recovery_liveness_inprogress_is_live);
	UT_RUN(test_recovery_liveness_neither_is_dead);
	UT_RUN(test_recovery_liveness_dead_and_ambiguous_both_abort);

	UT_RUN(test_remote_authority_anchor_zero_skips_gate);
	UT_RUN(test_remote_authority_recovered_covers_anchor);
	UT_RUN(test_remote_authority_recovered_equal_anchor);
	UT_RUN(test_remote_authority_under_recovered_failclosed);

	UT_RUN(test_wrap_suspect_wrap_checked_never_suspect);
	UT_RUN(test_wrap_suspect_retention_reliable_never_suspect);
	UT_RUN(test_wrap_suspect_below_horizon_unreliable_is_suspect);
	UT_RUN(test_wrap_suspect_at_or_above_horizon_not_suspect);
	UT_RUN(test_wrap_suspect_unjudgeable_unreliable_failclosed);

	UT_RUN(test_revert_non_delete_failclosed);
	UT_RUN(test_revert_delete_not_aborted_failclosed);
	UT_RUN(test_revert_delete_already_clear_skip_done);
	UT_RUN(test_revert_delete_identity_match_apply);
	UT_RUN(test_revert_delete_identity_mismatch_failclosed);

	UT_DONE();
	return ut_failed_count != 0 ? 1 : 0;
}
