/*-------------------------------------------------------------------------
 *
 * test_cluster_block_apply.c
 *	  pgrac spec-4.10 D3a — cluster_unit tests for the backend single-block
 *	  redo-apply framework (online block recovery, corruption-critical).
 *
 *	  Two layers are covered here:
 *
 *	    A. cluster_block_apply_decide() — the PURE, rmgr-agnostic routing
 *	       decision (NOOP / FPI / DELTA).  FPI is chosen iff the record
 *	       carries an apply-able full-page image for the block; otherwise a
 *	       per-rmgr delta handler is required.  No block reference -> NOOP.
 *
 *	    B. cluster_block_apply_one() — the dispatcher.  Driven here with a
 *	       fabricated XLogReaderState/DecodedXLogRecord and a stubbed
 *	       RestoreBlockImage so the NOOP / UNSUPPORTED / FPI-restore paths
 *	       are exercised standalone.  The byte-for-byte differential against
 *	       PG's real redo (real WAL bytes) lands as a cluster_tap differential
 *	       in t/256 — a unit binary cannot decode a real FPI image.
 *
 *	    C. cluster_block_apply_one() RM_GENERIC delta — applyPageRedo mirror
 *	       (fragment apply + hole zero), driven with a stubbed
 *	       XLogRecGetBlockData so the apply + 8.A fail-closed bounds are
 *	       exercised standalone.
 *
 *	  8.A discipline pinned here (T_heap_delta_unsupported): until a heap
 *	  record-type passes the D3b byte-for-byte differential it stays OFF the
 *	  apply matrix and a delta-route record fails closed (UNSUPPORTED), never
 *	  silently "succeeding".
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
 *	  src/test/cluster_unit/test_cluster_block_apply.c
 *
 * Spec: spec-4.10-online-block-recovery.md (FROZEN v0.4)
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include <stddef.h>
#include <stdlib.h>
#include <string.h>

#include "access/htup.h" /* HeapTupleHeader (spec-3.26 stub) */
#include "access/rmgr.h"
#include "access/xlogreader.h"
#include "access/xlogrecord.h"
#include "storage/bufpage.h"
#include "cluster/cluster_block_apply.h"

#include "unit_test.h"

UT_DEFINE_GLOBALS();


/* ----------
 * Stubs for the PG backend symbols cluster_block_apply.o references.
 *
 * cluster_block_apply.c calls RestoreBlockImage (the only non-inline
 * external on the FPI path) and, via the inline PageSetLSN hook, reads the
 * merged-recovery window externs.  PageIsNew / PageSetLSN / PageGetLSN and
 * the XLogRec* accessors are inline and need no stub.  Assert() resolves to
 * ExceptionalCondition in a non-cassert build path; provide a stub so the
 * link succeeds regardless of build flags.
 * ----------
 */

/* Merged-recovery window externs (bufpage.h PageSetLSN hook).  A backend
 * doing online block recovery is NOT inside the startup merge window, so
 * these stay false/0 — exactly the context cluster_block_apply_one targets. */
bool cluster_recmerge_window_active = false;
uint64 cluster_recmerge_window_scn = 0;
uint64 cluster_recmerge_window_own_lsn = 0;
bool cluster_recmerge_apply_foreign = false;

/* Controllable RestoreBlockImage stub. */
static bool stub_restore_ret = true;	   /* what RestoreBlockImage returns */
static bool stub_restore_make_new = false; /* leave page all-zero (PageIsNew) */
static int stub_restore_calls = 0;

bool
RestoreBlockImage(XLogReaderState *record, uint8 block_id, char *page)
{
	stub_restore_calls++;
	if (!stub_restore_ret)
		return false;

	memset(page, 0, BLCKSZ);
	if (!stub_restore_make_new) {
		/* A restored real page is not "new": pd_upper != 0. */
		PageHeader ph = (PageHeader)page;

		ph->pd_lower = SizeOfPageHeaderData;
		ph->pd_upper = BLCKSZ;
		ph->pd_special = BLCKSZ;
	}
	return true;
}

void
ExceptionalCondition(const char *conditionName, const char *fileName, int lineNumber)
{
	/* Declared noreturn in PG; must not fire in these tests. */
	printf("# unexpected Assert: %s at %s:%d\n", conditionName, fileName, lineNumber);
	abort();
}

/* Controllable XLogRecGetBlockData stub (the RM_GENERIC delta path reads the
 * record's block data; supply a crafted fragment buffer here). */
static char *stub_blockdata = NULL;
static Size stub_blockdata_len = 0;

char *
XLogRecGetBlockData(XLogReaderState *record, uint8 block_id, Size *len)
{
	if (len)
		*len = stub_blockdata_len;
	return stub_blockdata;
}

/* Controllable stub for the heap matrix applicator (its byte-for-byte
 * correctness is the t/256 differential's job; here we only verify that the
 * delta dispatcher routes RM_HEAP records to it and propagates the result). */
static int stub_heap_calls = 0;
static ClusterBlkApplyResult stub_heap_ret = CLUSTER_BLKAPPLY_UNSUPPORTED;

ClusterBlkApplyResult
cluster_block_apply_heap(XLogReaderState *record, uint8 block_id, char *page)
{
	stub_heap_calls++;
	return stub_heap_ret;
}

/* spec-3.26: cluster_block_apply.o now references the shared ITL slot-stamp
 * helper (via cluster_block_apply_clusteritl).  Its byte-equivalence is
 * test_cluster_itl_finish_wal's job; stub it here so the dispatch test links. */
Size
cluster_itl_redo_apply_block_local_delta(Page page pg_attribute_unused(),
										 HeapTupleHeader htup pg_attribute_unused(),
										 const char *itl_block_start pg_attribute_unused())
{
	return 0;
}


/* ----------
 * Record fabrication: a minimal XLogReaderState wrapping a DecodedXLogRecord
 * with a single block reference.  Only the fields read by the XLogRec*
 * accessors + the FPI path are populated.
 * ----------
 */
typedef struct FakeRecord {
	XLogReaderState st;
	/* DecodedXLogRecord has a FLEXIBLE_ARRAY_MEMBER blocks[]; reserve two. */
	union {
		DecodedXLogRecord dec;
		/* cppcheck-suppress unusedStructMember */
		char pad[sizeof(DecodedXLogRecord) + 2 * sizeof(DecodedBkpBlock)];
	} u;
} FakeRecord;

static XLogReaderState *
make_record(FakeRecord *fr, RmgrId rmid, int max_block_id, bool in_use, bool has_image,
			bool apply_image, XLogRecPtr endlsn)
{
	DecodedXLogRecord *dec = &fr->u.dec;

	memset(fr, 0, sizeof(*fr));
	dec->header.xl_rmid = rmid;
	dec->max_block_id = max_block_id;
	if (max_block_id >= 0) {
		dec->blocks[0].in_use = in_use;
		dec->blocks[0].has_image = has_image;
		dec->blocks[0].apply_image = apply_image;
	}
	fr->st.record = dec;
	fr->st.EndRecPtr = endlsn;
	return &fr->st;
}


/* ==========================================================================
 * A. cluster_block_apply_decide() — pure routing truth table
 * ========================================================================== */

UT_TEST(test_decide_no_block_ref_is_noop)
{
	UT_ASSERT_EQ((int)cluster_block_apply_decide(false, false, false),
				 (int)CLUSTER_BLKAPPLY_ACT_NOOP);
}

UT_TEST(test_decide_no_block_ref_dominates_image)
{
	/* image flags are meaningless without a block reference */
	UT_ASSERT_EQ((int)cluster_block_apply_decide(false, true, true),
				 (int)CLUSTER_BLKAPPLY_ACT_NOOP);
}

UT_TEST(test_decide_apply_image_is_fpi)
{
	UT_ASSERT_EQ((int)cluster_block_apply_decide(true, true, true), (int)CLUSTER_BLKAPPLY_ACT_FPI);
}

UT_TEST(test_decide_image_not_for_apply_is_delta)
{
	/* image present for consistency-checking only -> redo applies the delta */
	UT_ASSERT_EQ((int)cluster_block_apply_decide(true, true, false),
				 (int)CLUSTER_BLKAPPLY_ACT_DELTA);
}

UT_TEST(test_decide_no_image_is_delta)
{
	UT_ASSERT_EQ((int)cluster_block_apply_decide(true, false, false),
				 (int)CLUSTER_BLKAPPLY_ACT_DELTA);
}

UT_TEST(test_decide_actions_distinct)
{
	UT_ASSERT_NE((int)CLUSTER_BLKAPPLY_ACT_NOOP, (int)CLUSTER_BLKAPPLY_ACT_FPI);
	UT_ASSERT_NE((int)CLUSTER_BLKAPPLY_ACT_FPI, (int)CLUSTER_BLKAPPLY_ACT_DELTA);
	UT_ASSERT_NE((int)CLUSTER_BLKAPPLY_ACT_NOOP, (int)CLUSTER_BLKAPPLY_ACT_DELTA);
}


/* ==========================================================================
 * B. cluster_block_apply_one() — dispatcher (fabricated records)
 * ========================================================================== */

UT_TEST(test_apply_one_no_block_ref_noop)
{
	FakeRecord fr;
	char page[BLCKSZ];
	XLogReaderState *rec;

	memset(page, 0xAB, sizeof(page));
	rec = make_record(&fr, RM_HEAP_ID, -1 /* no blocks */, false, false, false, 0x1000);
	stub_restore_calls = 0;
	UT_ASSERT_EQ((int)cluster_block_apply_one(rec, 0, page), (int)CLUSTER_BLKAPPLY_NOOP);
	/* page must be untouched on NOOP, and no FPI restore attempted */
	UT_ASSERT_EQ(stub_restore_calls, 0);
	UT_ASSERT_EQ((unsigned char)page[0], 0xAB);
}

UT_TEST(test_apply_one_unsupported_rmgr_failclosed)
{
	FakeRecord fr;
	char page[BLCKSZ];
	XLogReaderState *rec;

	/* RM_XLOG_ID delta with no image: not on the D3a apply matrix. */
	rec = make_record(&fr, RM_XLOG_ID, 0, true /* in_use */, false, false, 0x2000);
	stub_restore_calls = 0;
	UT_ASSERT_EQ((int)cluster_block_apply_one(rec, 0, page), (int)CLUSTER_BLKAPPLY_UNSUPPORTED);
	UT_ASSERT_EQ(stub_restore_calls, 0);
}

UT_TEST(test_apply_one_heap_delta_routes_to_handler)
{
	FakeRecord fr;
	char page[BLCKSZ];
	XLogReaderState *rec;

	/*
	 * A heap delta (no FPI) dispatches to the per-rmgr heap applicator, never
	 * the FPI path; the handler's byte-for-byte correctness is the t/256
	 * differential's job.  Here we verify the routing and that the handler's
	 * result (incl. an 8.A fail-closed UNSUPPORTED) propagates unchanged.
	 */
	rec = make_record(&fr, RM_HEAP_ID, 0, true, false, false, 0x3000);

	stub_restore_calls = 0;
	stub_heap_calls = 0;
	stub_heap_ret = CLUSTER_BLKAPPLY_OK;
	UT_ASSERT_EQ((int)cluster_block_apply_one(rec, 0, page), (int)CLUSTER_BLKAPPLY_OK);
	UT_ASSERT_EQ(stub_heap_calls, 1);
	UT_ASSERT_EQ(stub_restore_calls, 0); /* delta path, no FPI restore */

	stub_heap_calls = 0;
	stub_heap_ret = CLUSTER_BLKAPPLY_UNSUPPORTED;
	UT_ASSERT_EQ((int)cluster_block_apply_one(rec, 0, page), (int)CLUSTER_BLKAPPLY_UNSUPPORTED);
	UT_ASSERT_EQ(stub_heap_calls, 1);
}

UT_TEST(test_apply_one_fpi_restores_and_sets_lsn)
{
	FakeRecord fr;
	char page[BLCKSZ];
	XLogReaderState *rec;
	XLogRecPtr endlsn = 0x0000000012345678ULL;

	/* FPI present + apply-able, on RM_HEAP_ID (FPI is rmgr-agnostic). */
	rec = make_record(&fr, RM_HEAP_ID, 0, true, true /* has_image */, true /* apply_image */,
					  endlsn);
	stub_restore_ret = true;
	stub_restore_make_new = false;
	stub_restore_calls = 0;

	UT_ASSERT_EQ((int)cluster_block_apply_one(rec, 0, page), (int)CLUSTER_BLKAPPLY_OK);
	UT_ASSERT_EQ(stub_restore_calls, 1);
	/* PageSetLSN must stamp the record's end LSN (version endpoint). */
	UT_ASSERT_EQ((long long)PageGetLSN(page), (long long)endlsn);
}

UT_TEST(test_apply_one_fpi_new_page_skips_lsn)
{
	FakeRecord fr;
	char page[BLCKSZ];
	XLogReaderState *rec;

	/*
	 * Mirror XLogReadBufferForRedoExtended: an uninitialized (all-zero) page
	 * must NOT get its LSN set (would corrupt the page).  PageGetLSN stays 0.
	 */
	rec = make_record(&fr, RM_HEAP_ID, 0, true, true, true, 0xDEADBEEF);
	stub_restore_ret = true;
	stub_restore_make_new = true; /* RestoreBlockImage leaves page all-zero */
	stub_restore_calls = 0;

	UT_ASSERT_EQ((int)cluster_block_apply_one(rec, 0, page), (int)CLUSTER_BLKAPPLY_OK);
	UT_ASSERT_EQ(stub_restore_calls, 1);
	UT_ASSERT_EQ((long long)PageGetLSN(page), 0LL);
}

UT_TEST(test_apply_one_fpi_restore_failure_failclosed)
{
	FakeRecord fr;
	char page[BLCKSZ];
	XLogReaderState *rec;

	/* RestoreBlockImage failing (corrupt FPI) -> fail-closed, never OK. */
	rec = make_record(&fr, RM_HEAP_ID, 0, true, true, true, 0x4000);
	stub_restore_ret = false;
	stub_restore_make_new = false;
	stub_restore_calls = 0;

	UT_ASSERT_EQ((int)cluster_block_apply_one(rec, 0, page), (int)CLUSTER_BLKAPPLY_FAILED);
	UT_ASSERT_EQ(stub_restore_calls, 1);
}

/* ==========================================================================
 * C. RM_GENERIC delta — applyPageRedo mirror (fragment apply + hole zero)
 * ========================================================================== */

/* Build a page header with a hole [pd_lower, pd_upper); dirty the hole 0xFF so
 * the hole-zeroing is observable.  Special area is [pd_special, BLCKSZ). */
static void
make_page_with_hole(char *page, uint16 pd_lower, uint16 pd_upper, uint16 pd_special)
{
	PageHeader ph = (PageHeader)page;

	memset(page, 0, BLCKSZ);
	ph->pd_lower = pd_lower;
	ph->pd_upper = pd_upper;
	ph->pd_special = pd_special;
	if (pd_upper > pd_lower)
		memset(page + pd_lower, 0xFF, pd_upper - pd_lower);
}

UT_TEST(test_apply_one_generic_applies_fragment_and_zeros_hole)
{
	FakeRecord fr;
	char page[BLCKSZ];
	XLogReaderState *rec;
	char delta[2 + 2 + 4];
	OffsetNumber off = (OffsetNumber)(BLCKSZ - 4); /* special area, above hole */
	OffsetNumber len = 4;
	XLogRecPtr endlsn = 0x0000000055667788ULL;

	/* hole = [24, BLCKSZ-4); special = [BLCKSZ-4, BLCKSZ) */
	make_page_with_hole(page, SizeOfPageHeaderData, BLCKSZ - 4, BLCKSZ - 4);

	/* one fragment: write 0xAA x4 into the special area (survives hole zero) */
	memcpy(delta, &off, sizeof(off));
	memcpy(delta + 2, &len, sizeof(len));
	memset(delta + 4, 0xAA, 4);
	stub_blockdata = delta;
	stub_blockdata_len = sizeof(delta);

	rec = make_record(&fr, RM_GENERIC_ID, 0, true, false, false, endlsn);
	UT_ASSERT_EQ((int)cluster_block_apply_one(rec, 0, page), (int)CLUSTER_BLKAPPLY_OK);
	/* fragment applied to the special area */
	UT_ASSERT_EQ((unsigned char)page[BLCKSZ - 4], 0xAA);
	UT_ASSERT_EQ((unsigned char)page[BLCKSZ - 1], 0xAA);
	/* hole zeroed (was 0xFF) */
	UT_ASSERT_EQ((unsigned char)page[100], 0x00);
	/* LSN stamped */
	UT_ASSERT_EQ((long long)PageGetLSN(page), (long long)endlsn);

	stub_blockdata = NULL;
	stub_blockdata_len = 0;
}

UT_TEST(test_apply_one_generic_truncated_header_failclosed)
{
	FakeRecord fr;
	char page[BLCKSZ];
	XLogReaderState *rec;
	char delta[3]; /* < 4-byte fragment header -> malformed */

	make_page_with_hole(page, SizeOfPageHeaderData, BLCKSZ, BLCKSZ);
	stub_blockdata = delta;
	stub_blockdata_len = 3;

	/* 8.A: a malformed fragment header fails closed, never a partial write. */
	rec = make_record(&fr, RM_GENERIC_ID, 0, true, false, false, 0x10);
	UT_ASSERT_EQ((int)cluster_block_apply_one(rec, 0, page), (int)CLUSTER_BLKAPPLY_FAILED);

	stub_blockdata = NULL;
	stub_blockdata_len = 0;
}

UT_TEST(test_apply_one_generic_oob_fragment_failclosed)
{
	FakeRecord fr;
	char page[BLCKSZ];
	XLogReaderState *rec;
	char delta[2 + 2 + 4];
	OffsetNumber off = (OffsetNumber)(BLCKSZ - 2); /* off+len = BLCKSZ+2 > BLCKSZ */
	OffsetNumber len = 4;

	make_page_with_hole(page, SizeOfPageHeaderData, BLCKSZ, BLCKSZ);
	memcpy(delta, &off, sizeof(off));
	memcpy(delta + 2, &len, sizeof(len));
	memset(delta + 4, 0xAA, 4);
	stub_blockdata = delta;
	stub_blockdata_len = sizeof(delta);

	/* 8.A: an out-of-page fragment fails closed, never an OOB write. */
	rec = make_record(&fr, RM_GENERIC_ID, 0, true, false, false, 0x20);
	UT_ASSERT_EQ((int)cluster_block_apply_one(rec, 0, page), (int)CLUSTER_BLKAPPLY_FAILED);

	stub_blockdata = NULL;
	stub_blockdata_len = 0;
}

UT_TEST(test_apply_results_distinct)
{
	/* result contract: the four outcomes are distinct values */
	UT_ASSERT_NE((int)CLUSTER_BLKAPPLY_OK, (int)CLUSTER_BLKAPPLY_NOOP);
	UT_ASSERT_NE((int)CLUSTER_BLKAPPLY_OK, (int)CLUSTER_BLKAPPLY_UNSUPPORTED);
	UT_ASSERT_NE((int)CLUSTER_BLKAPPLY_OK, (int)CLUSTER_BLKAPPLY_FAILED);
	UT_ASSERT_NE((int)CLUSTER_BLKAPPLY_NOOP, (int)CLUSTER_BLKAPPLY_UNSUPPORTED);
	UT_ASSERT_NE((int)CLUSTER_BLKAPPLY_NOOP, (int)CLUSTER_BLKAPPLY_FAILED);
	UT_ASSERT_NE((int)CLUSTER_BLKAPPLY_UNSUPPORTED, (int)CLUSTER_BLKAPPLY_FAILED);
}


int
main(void)
{
	UT_PLAN(16);

	/* A. pure decide truth table */
	UT_RUN(test_decide_no_block_ref_is_noop);
	UT_RUN(test_decide_no_block_ref_dominates_image);
	UT_RUN(test_decide_apply_image_is_fpi);
	UT_RUN(test_decide_image_not_for_apply_is_delta);
	UT_RUN(test_decide_no_image_is_delta);
	UT_RUN(test_decide_actions_distinct);

	/* B. dispatcher */
	UT_RUN(test_apply_one_no_block_ref_noop);
	UT_RUN(test_apply_one_unsupported_rmgr_failclosed);
	UT_RUN(test_apply_one_heap_delta_routes_to_handler);
	UT_RUN(test_apply_one_fpi_restores_and_sets_lsn);
	UT_RUN(test_apply_one_fpi_new_page_skips_lsn);
	UT_RUN(test_apply_one_fpi_restore_failure_failclosed);

	/* C. RM_GENERIC delta */
	UT_RUN(test_apply_one_generic_applies_fragment_and_zeros_hole);
	UT_RUN(test_apply_one_generic_truncated_header_failclosed);
	UT_RUN(test_apply_one_generic_oob_fragment_failclosed);

	UT_RUN(test_apply_results_distinct);

	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
