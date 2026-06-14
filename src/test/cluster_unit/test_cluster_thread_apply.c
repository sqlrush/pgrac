/*-------------------------------------------------------------------------
 *
 * test_cluster_thread_apply.c
 *	  pgrac spec-4.11 D1 — cluster_unit tests for the online thread-recovery
 *	  page apply matrix (Q10-B, corruption-critical 8.A).
 *
 *	  spec-4.11 increment 2 adds ONE new corruption-critical primitive on top
 *	  of the t/256-proven single-block apply core (cluster_block_apply_one):
 *	  the redo LSN-gate.  A survivor online-replays a dead thread's WAL stream
 *	  through to shared storage, so each record is applied to a LIVE page that
 *	  may already reflect it (unlike spec-4.10 which rebuilds from a clean FPI
 *	  base).  cluster_thread_apply_record_to_page() therefore mirrors PG redo's
 *	  XLogReadBufferForRedoExtended BLK_DONE skip: a populated page already at
 *	  or past the record's end LSN is left untouched (DONE), which is what makes
 *	  stream apply-through idempotent on retry (spec-4.11 v0.3 partial-apply).
 *
 *	  Two layers are covered here:
 *
 *	    A. cluster_thread_apply_decide() — the PURE LSN-gate routing decision
 *	       (NOOP / DONE / APPLY).  This is the whole new-correctness surface;
 *	       the FPI-vs-delta choice stays inside cluster_block_apply_one.
 *
 *	    B. cluster_thread_apply_record_to_page() — the wrapper.  Driven with a
 *	       fabricated XLogReaderState and a stubbed cluster_block_apply_one so
 *	       the gate / NOOP / APPLY-result-mapping paths are exercised
 *	       standalone.  The byte-for-byte differential against PG's real redo
 *	       (real WAL bytes) is the cluster_tap t/258 differential's job — a unit
 *	       binary cannot decode a real FPI image.
 *
 *	  8.A discipline pinned here: cluster_block_apply_one returning anything but
 *	  OK (UNSUPPORTED off-matrix rmgr / FAILED unusable image) maps to BLOCKED
 *	  fail-closed, never a silent success; applied_lsn is left Invalid so the
 *	  driver discards the poisoned page rather than installing it.
 *
 *	  Standalone executable per spec-0.4 §9.2.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_thread_apply.c
 *
 * Spec: spec-4.11-thread-recovery.md (FROZEN v0.3)
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include <stddef.h>
#include <stdlib.h>
#include <string.h>

#include "access/rmgr.h"
#include "access/xlogreader.h"
#include "access/xlogrecord.h"
#include "storage/bufpage.h"
#include "cluster/cluster_block_apply.h"
#include "cluster/cluster_thread_recovery_apply.h"

#include "unit_test.h"

UT_DEFINE_GLOBALS();


/* ----------
 * Stubs for the PG/cluster symbols cluster_thread_recovery_apply.o references.
 *
 * The wrapper reads PageIsNew / PageGetLSN (inline), the XLogRec* block-ref
 * accessors (inline), record->EndRecPtr (field), and delegates the actual page
 * mutation to cluster_block_apply_one.  We stub cluster_block_apply_one here:
 * its byte-for-byte correctness is t/256's (and t/258's) job; this unit only
 * proves the LSN-gate wrapper routes and maps results correctly.
 *
 * PageGetLSN/PageSetLSN are inline and read the merged-recovery window externs
 * via the pgrac PageSetLSN hook; a backend doing online thread recovery is NOT
 * inside the startup merge window, so these stay false/0.
 * ----------
 */
bool cluster_recmerge_window_active = false;
uint64 cluster_recmerge_window_scn = 0;
uint64 cluster_recmerge_window_own_lsn = 0;
bool cluster_recmerge_apply_foreign = false;

void
ExceptionalCondition(const char *conditionName, const char *fileName, int lineNumber)
{
	/* Declared noreturn in PG; must not fire in these tests. */
	printf("# unexpected Assert: %s at %s:%d\n", conditionName, fileName, lineNumber);
	abort();
}

/* Controllable cluster_block_apply_one stub. */
static int stub_apply_one_calls = 0;
static ClusterBlkApplyResult stub_apply_one_ret = CLUSTER_BLKAPPLY_OK;

ClusterBlkApplyResult
cluster_block_apply_one(XLogReaderState *record, uint8 block_id, char *page)
{
	stub_apply_one_calls++;
	return stub_apply_one_ret;
}


/* ----------
 * Record fabrication: a minimal XLogReaderState wrapping a DecodedXLogRecord
 * with a single block reference.  Only the fields the wrapper reads (block-ref
 * in_use + EndRecPtr) are populated.
 * ----------
 */
typedef struct FakeRecord {
	XLogReaderState st;
	union {
		DecodedXLogRecord dec;
		/* cppcheck-suppress unusedStructMember */
		char pad[sizeof(DecodedXLogRecord) + 2 * sizeof(DecodedBkpBlock)];
	} u;
} FakeRecord;

static XLogReaderState *
make_record(FakeRecord *fr, RmgrId rmid, int max_block_id, bool in_use, XLogRecPtr endlsn)
{
	DecodedXLogRecord *dec = &fr->u.dec;

	memset(fr, 0, sizeof(*fr));
	dec->header.xl_rmid = rmid;
	dec->max_block_id = max_block_id;
	if (max_block_id >= 0)
		dec->blocks[0].in_use = in_use;
	fr->st.record = dec;
	fr->st.EndRecPtr = endlsn;
	return &fr->st;
}

/* Build a populated (non-new) page carrying a given pd_lsn. */
static void
make_page_with_lsn(char *page, XLogRecPtr lsn)
{
	PageHeader ph = (PageHeader)page;

	memset(page, 0, BLCKSZ);
	ph->pd_lower = SizeOfPageHeaderData;
	ph->pd_upper = BLCKSZ; /* pd_upper != 0 -> not PageIsNew */
	ph->pd_special = BLCKSZ;
	PageSetLSN(page, lsn);
}


/* ==========================================================================
 * A. cluster_thread_apply_decide() — pure LSN-gate routing truth table
 * ========================================================================== */

UT_TEST(test_decide_no_block_ref_is_noop)
{
	/* no block reference -> NOOP regardless of LSNs */
	UT_ASSERT_EQ((int)cluster_thread_apply_decide(false, false, 0x9000, 0x1000),
				 (int)CLUSTER_THREADAPPLY_ACT_NOOP);
}

UT_TEST(test_decide_new_page_is_never_gated)
{
	/* an all-zero (new) page is never gated, even if record_end <= page_lsn(0) */
	UT_ASSERT_EQ((int)cluster_thread_apply_decide(true, true, 0x1000, 0x0),
				 (int)CLUSTER_THREADAPPLY_ACT_APPLY);
}

UT_TEST(test_decide_record_after_page_is_apply)
{
	/* populated page, record ends past the page version -> apply the record */
	UT_ASSERT_EQ((int)cluster_thread_apply_decide(true, false, 0x2000, 0x1000),
				 (int)CLUSTER_THREADAPPLY_ACT_APPLY);
}

UT_TEST(test_decide_record_equal_page_is_done)
{
	/*
	 * Boundary: record_end == page_lsn means the page already reflects this
	 * record (mirror XLogReadBufferForRedoExtended: lsn <= PageGetLSN -> BLK_DONE).
	 */
	UT_ASSERT_EQ((int)cluster_thread_apply_decide(true, false, 0x1000, 0x1000),
				 (int)CLUSTER_THREADAPPLY_ACT_DONE);
}

UT_TEST(test_decide_record_before_page_is_done)
{
	/* populated page already past the record -> idempotent skip */
	UT_ASSERT_EQ((int)cluster_thread_apply_decide(true, false, 0x1000, 0x2000),
				 (int)CLUSTER_THREADAPPLY_ACT_DONE);
}

UT_TEST(test_decide_actions_distinct)
{
	UT_ASSERT_NE((int)CLUSTER_THREADAPPLY_ACT_NOOP, (int)CLUSTER_THREADAPPLY_ACT_DONE);
	UT_ASSERT_NE((int)CLUSTER_THREADAPPLY_ACT_DONE, (int)CLUSTER_THREADAPPLY_ACT_APPLY);
	UT_ASSERT_NE((int)CLUSTER_THREADAPPLY_ACT_NOOP, (int)CLUSTER_THREADAPPLY_ACT_APPLY);
}


/* ==========================================================================
 * B. cluster_thread_apply_record_to_page() — wrapper (fabricated records)
 * ========================================================================== */

UT_TEST(test_apply_no_block_ref_noop)
{
	FakeRecord fr;
	char page[BLCKSZ];
	XLogReaderState *rec;
	XLogRecPtr applied = 0xDEAD;

	make_page_with_lsn(page, 0x1000);
	rec = make_record(&fr, RM_HEAP_ID, -1 /* no blocks */, false, 0x2000);
	stub_apply_one_calls = 0;
	UT_ASSERT_EQ((int)cluster_thread_apply_record_to_page(rec, 0, page, &applied),
				 (int)CLUSTER_THREADAPPLY_NOOP);
	/* no apply attempted; applied_lsn reflects the page's unchanged version */
	UT_ASSERT_EQ(stub_apply_one_calls, 0);
	UT_ASSERT_EQ((long long)applied, (long long)0x1000);
}

UT_TEST(test_apply_lsn_gate_skips_already_applied)
{
	FakeRecord fr;
	char page[BLCKSZ];
	XLogReaderState *rec;
	XLogRecPtr applied = 0;

	/* page already at 0x2000; a record ending at 0x1000 must be skipped (DONE) */
	make_page_with_lsn(page, 0x2000);
	rec = make_record(&fr, RM_HEAP_ID, 0, true /* in_use */, 0x1000);
	stub_apply_one_calls = 0;
	UT_ASSERT_EQ((int)cluster_thread_apply_record_to_page(rec, 0, page, &applied),
				 (int)CLUSTER_THREADAPPLY_DONE);
	/* idempotence: the proven apply core is NOT re-invoked on a gated record */
	UT_ASSERT_EQ(stub_apply_one_calls, 0);
	UT_ASSERT_EQ((long long)applied, (long long)0x2000);
}

UT_TEST(test_apply_applies_and_advances_lsn)
{
	FakeRecord fr;
	char page[BLCKSZ];
	XLogReaderState *rec;
	XLogRecPtr applied = 0;

	/* page at 0x1000; a record ending at 0x2000 is applied, page advances */
	make_page_with_lsn(page, 0x1000);
	rec = make_record(&fr, RM_HEAP_ID, 0, true, 0x2000);
	stub_apply_one_calls = 0;
	stub_apply_one_ret = CLUSTER_BLKAPPLY_OK;
	UT_ASSERT_EQ((int)cluster_thread_apply_record_to_page(rec, 0, page, &applied),
				 (int)CLUSTER_THREADAPPLY_APPLIED);
	UT_ASSERT_EQ(stub_apply_one_calls, 1);
	UT_ASSERT_EQ((long long)applied, (long long)0x2000);
}

UT_TEST(test_apply_new_page_is_not_gated)
{
	FakeRecord fr;
	char page[BLCKSZ];
	XLogReaderState *rec;
	XLogRecPtr applied = 0;

	/* a fresh (all-zero) page is applied even though record_end > page_lsn(0) */
	memset(page, 0, sizeof(page)); /* PageIsNew */
	rec = make_record(&fr, RM_HEAP_ID, 0, true, 0x2000);
	stub_apply_one_calls = 0;
	stub_apply_one_ret = CLUSTER_BLKAPPLY_OK;
	UT_ASSERT_EQ((int)cluster_thread_apply_record_to_page(rec, 0, page, &applied),
				 (int)CLUSTER_THREADAPPLY_APPLIED);
	UT_ASSERT_EQ(stub_apply_one_calls, 1);
	UT_ASSERT_EQ((long long)applied, (long long)0x2000);
}

UT_TEST(test_apply_unsupported_is_blocked)
{
	FakeRecord fr;
	char page[BLCKSZ];
	XLogReaderState *rec;
	XLogRecPtr applied = 0x1234;

	/* off-matrix rmgr (UNSUPPORTED) -> fail-closed BLOCKED, applied_lsn invalid */
	make_page_with_lsn(page, 0x1000);
	rec = make_record(&fr, RM_BTREE_ID, 0, true, 0x2000);
	stub_apply_one_ret = CLUSTER_BLKAPPLY_UNSUPPORTED;
	UT_ASSERT_EQ((int)cluster_thread_apply_record_to_page(rec, 0, page, &applied),
				 (int)CLUSTER_THREADAPPLY_BLOCKED);
	UT_ASSERT_EQ((long long)applied, (long long)InvalidXLogRecPtr);
}

UT_TEST(test_apply_failed_is_blocked)
{
	FakeRecord fr;
	char page[BLCKSZ];
	XLogReaderState *rec;
	XLogRecPtr applied = 0x1234;

	/* on-matrix but image/delta unusable (FAILED) -> fail-closed BLOCKED */
	make_page_with_lsn(page, 0x1000);
	rec = make_record(&fr, RM_HEAP_ID, 0, true, 0x2000);
	stub_apply_one_ret = CLUSTER_BLKAPPLY_FAILED;
	UT_ASSERT_EQ((int)cluster_thread_apply_record_to_page(rec, 0, page, &applied),
				 (int)CLUSTER_THREADAPPLY_BLOCKED);
	UT_ASSERT_EQ((long long)applied, (long long)InvalidXLogRecPtr);
}

UT_TEST(test_apply_results_distinct)
{
	UT_ASSERT_NE((int)CLUSTER_THREADAPPLY_APPLIED, (int)CLUSTER_THREADAPPLY_DONE);
	UT_ASSERT_NE((int)CLUSTER_THREADAPPLY_APPLIED, (int)CLUSTER_THREADAPPLY_NOOP);
	UT_ASSERT_NE((int)CLUSTER_THREADAPPLY_APPLIED, (int)CLUSTER_THREADAPPLY_BLOCKED);
	UT_ASSERT_NE((int)CLUSTER_THREADAPPLY_DONE, (int)CLUSTER_THREADAPPLY_NOOP);
	UT_ASSERT_NE((int)CLUSTER_THREADAPPLY_DONE, (int)CLUSTER_THREADAPPLY_BLOCKED);
	UT_ASSERT_NE((int)CLUSTER_THREADAPPLY_NOOP, (int)CLUSTER_THREADAPPLY_BLOCKED);
}


int
main(void)
{
	UT_PLAN(13);

	/* A. pure LSN-gate decide truth table */
	UT_RUN(test_decide_no_block_ref_is_noop);
	UT_RUN(test_decide_new_page_is_never_gated);
	UT_RUN(test_decide_record_after_page_is_apply);
	UT_RUN(test_decide_record_equal_page_is_done);
	UT_RUN(test_decide_record_before_page_is_done);
	UT_RUN(test_decide_actions_distinct);

	/* B. wrapper dispatch + result mapping */
	UT_RUN(test_apply_no_block_ref_noop);
	UT_RUN(test_apply_lsn_gate_skips_already_applied);
	UT_RUN(test_apply_applies_and_advances_lsn);
	UT_RUN(test_apply_new_page_is_not_gated);
	UT_RUN(test_apply_unsupported_is_blocked);
	UT_RUN(test_apply_failed_is_blocked);

	UT_RUN(test_apply_results_distinct);

	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
