/*-------------------------------------------------------------------------
 *
 * test_cluster_itl_reader_real_triple.c
 *	  pgrac spec-3.4b D12 — cluster_itl_get_tt_ref 3-branch reader tests.
 *
 *	  Tests build a synthetic 8 KB heap page with an ITL special area
 *	  (CLUSTER_ITL_ARRAY_SIZE = 384 bytes) and call the real reader on
 *	  hand-crafted slot states (FREE / InvalidUba legacy / real UBA /
 *	  malformed UBA).  No postmaster / shmem required.
 *
 *	  18 tests covering:
 *	    T1   sizeof(ClusterUndoTTSlotRef) == 32
 *	    T2   reader returns false on NULL page
 *	    T3   reader returns false on NULL ref
 *	    T4   reader returns false on slot_idx >= INITRANS
 *	    T5   reader returns false on slot.flags == ITL_FLAG_FREE
 *	    T6   reader returns false on PageHasItl=false (no ITL area)
 *	    T7   3-branch B2 (InvalidUba legacy): returns zero triple
 *	    T8   3-branch B2: cached_commit_scn carries through
 *	    T9   3-branch B2: cluster_epoch is set (non-zero on epoch advance)
 *	    T10  3-branch B3 (real UBA): origin_node_id derived from segment_id
 *	    T11  3-branch B3: undo_segment_id matches UBA segment_id
 *	    T12  3-branch B3: tt_slot_id = offset_to_id(slot_offset) = offset+1
 *	    T13  3-branch B3: slot offset 0 → tt_slot_id == 1 (F1 sentinel separation)
 *	    T14  3-branch B3: slot offset 47 → tt_slot_id == 48
 *	    T15  3-branch B3 with malformed UBA → ereport ERROR (ERRCODE_DATA_CORRUPTED)
 *	    T16  3-branch B3 with segment_id producing out-of-range node → ereport ERROR
 *	    T17  reader fills has_cached_status=true for COMMITTED+SCN_VALID
 *	    T18  reader fills has_cached_status=false for ACTIVE
 *
 *	  Spec: spec-3.4b-real-tt-allocator-uba-encoding-production-cross-node.md
 *	        (v0.3 FROZEN 2026-05-24)
 *
 * Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_itl_reader_real_triple.c
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include <setjmp.h>
#include <stdlib.h>
#include <string.h>

#include "access/transam.h"
#include "cluster/cluster_itl.h"
#include "cluster/cluster_itl_slot.h"
#include "cluster/cluster_scn.h"
#include "cluster/cluster_tt_slot.h"
#include "cluster/cluster_uba.h"
#include "storage/bufpage.h"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"


UT_DEFINE_GLOBALS();


/* ============================================================
 *	ereport / Assert stubs
 *	cluster_itl.c calls ereport(ERROR, ...) on malformed UBA;
 *	we siglongjmp out so the test can detect the raise.
 * ============================================================ */
static sigjmp_buf ereport_recover_jmp;
static int ereport_raised_count = 0;
static int last_ereport_errcode = 0;

bool
errstart(int elevel, const char *domain pg_attribute_unused())
{
	if (elevel >= ERROR) {
		ereport_raised_count++;
		return true;
	}
	return false;
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
	siglongjmp(ereport_recover_jmp, 1);
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
int
errhint(const char *fmt pg_attribute_unused(), ...)
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


/* cluster_itl.c references these PG-side helpers we stub. */
void
MarkBufferDirty(Buffer buf pg_attribute_unused())
{}

Page
BufferGetPage(Buffer buf)
{
	return (Page)(uintptr_t)buf;
}

/* PG core helpers we need to stub to avoid linking the full storage/page
 * subsystem.  PageInit / PageInitHeapPage are stubbed below; only the
 * bytes we depend on (pd_flags PD_HAS_ITL bit, pd_special offset) are
 * set by hand. */
void
PageInit(Page page pg_attribute_unused(), Size pageSize pg_attribute_unused(),
		 Size specialSize pg_attribute_unused())
{}
void
PageInitHeapPage(Page page pg_attribute_unused(), Size pageSize pg_attribute_unused(),
				 Size specialSize pg_attribute_unused())
{}

/* Buffer manager globals referenced by cluster_itl.o's MarkBufferDirty
 * inline expansion / other transitive references. */
char *BufferBlocks = NULL;
void *LocalBufferBlockPointers[1] = { NULL };
int NBuffers = 0;
int NLocBuffer = 0;

int
errmsg_internal(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}


/* cluster_epoch_get_current is called by reader; stub a deterministic value. */
uint64
cluster_epoch_get_current(void)
{
	return 42;
}


/* cluster_enabled / cluster_node_id are referenced (defensively) by the
 * cluster_itl writer path; provide defaults. */
bool cluster_enabled = false;
int cluster_node_id = 0;


/* GetCurrentTransactionNestLevel is called by cluster_itl_check_subxact_or_error
 * but the reader path doesn't reach it; stub to 1 (top-level). */
int
GetCurrentTransactionNestLevel(void)
{
	return 1;
}


/* ============================================================
 *	Synthetic page builder
 *	Build an 8 KB page with PD_HAS_ITL + ITL slot array at the
 *	special area tail.
 * ============================================================ */

static char synthetic_page[BLCKSZ];

static Page
build_itl_page(void)
{
	PageHeader hdr;

	memset(synthetic_page, 0, BLCKSZ);
	hdr = (PageHeader)synthetic_page;
	hdr->pd_flags = PD_HAS_ITL;
	hdr->pd_special = (LocationIndex)(BLCKSZ - CLUSTER_ITL_ARRAY_SIZE);
	hdr->pd_upper = hdr->pd_special;
	hdr->pd_lower = SizeOfPageHeaderData;
	hdr->pd_pagesize_version = BLCKSZ | PG_PAGE_LAYOUT_VERSION;
	return (Page)synthetic_page;
}


static ClusterItlSlotData *
slot_at(Page page, uint8 idx)
{
	return &ClusterPageGetItlSlots(page)[idx];
}


/* ============================================================
 *	Tests
 * ============================================================ */

UT_TEST(test_t1_ref_sizeof_32)
{
	UT_ASSERT_EQ((int)sizeof(ClusterUndoTTSlotRef), 32);
}

UT_TEST(test_t2_null_page_returns_false)
{
	ClusterUndoTTSlotRef ref;

	UT_ASSERT_EQ((int)cluster_itl_get_tt_ref(NULL, 0, &ref), 0);
}

UT_TEST(test_t3_null_ref_returns_false)
{
	Page page = build_itl_page();

	UT_ASSERT_EQ((int)cluster_itl_get_tt_ref(page, 0, NULL), 0);
}

UT_TEST(test_t4_slot_idx_out_of_range_returns_false)
{
	Page page = build_itl_page();
	ClusterUndoTTSlotRef ref;

	UT_ASSERT_EQ((int)cluster_itl_get_tt_ref(page, CLUSTER_ITL_INITRANS_DEFAULT, &ref), 0);
	UT_ASSERT_EQ((int)cluster_itl_get_tt_ref(page, 255, &ref), 0);
}

UT_TEST(test_t5_free_slot_returns_false)
{
	Page page = build_itl_page();
	ClusterUndoTTSlotRef ref;
	ClusterItlSlotData *s = slot_at(page, 0);

	s->flags = ITL_FLAG_FREE;
	UT_ASSERT_EQ((int)cluster_itl_get_tt_ref(page, 0, &ref), 0);
}

UT_TEST(test_t6_no_itl_area_returns_false)
{
	char no_itl[BLCKSZ];
	PageHeader hdr;
	ClusterUndoTTSlotRef ref;

	memset(no_itl, 0, BLCKSZ);
	hdr = (PageHeader)no_itl;
	hdr->pd_flags = 0; /* PD_HAS_ITL NOT set */
	hdr->pd_special = BLCKSZ;
	hdr->pd_upper = BLCKSZ;
	hdr->pd_lower = SizeOfPageHeaderData;
	hdr->pd_pagesize_version = BLCKSZ | PG_PAGE_LAYOUT_VERSION;
	UT_ASSERT_EQ((int)cluster_itl_get_tt_ref((Page)no_itl, 0, &ref), 0);
}


/* ---------- Branch 2: UBA_is_invalid legacy fallback ---------- */

UT_TEST(test_t7_legacy_invaliduba_returns_zero_triple)
{
	Page page = build_itl_page();
	ClusterUndoTTSlotRef ref;
	ClusterItlSlotData *s = slot_at(page, 0);

	s->flags = ITL_FLAG_ACTIVE;
	s->xid = (TransactionId)12345;
	s->undo_segment_head = (UBA)InvalidUba_init;
	s->commit_scn = InvalidScn;

	UT_ASSERT_EQ((int)cluster_itl_get_tt_ref(page, 0, &ref), 1);
	UT_ASSERT_EQ((int)ref.origin_node_id, 0);
	UT_ASSERT_EQ((int)ref.undo_segment_id, 0);
	UT_ASSERT_EQ((int)ref.tt_slot_id, 0);
}

UT_TEST(test_t8_legacy_cached_commit_scn_passthrough)
{
	Page page = build_itl_page();
	ClusterUndoTTSlotRef ref;
	ClusterItlSlotData *s = slot_at(page, 0);
	SCN expected = (SCN)7777;

	s->flags = ITL_FLAG_COMMITTED;
	s->xid = (TransactionId)12345;
	s->undo_segment_head = (UBA)InvalidUba_init;
	s->commit_scn = expected;

	UT_ASSERT_EQ((int)cluster_itl_get_tt_ref(page, 0, &ref), 1);
	UT_ASSERT_EQ((int)(ref.cached_commit_scn == expected), 1);
}

UT_TEST(test_t9_legacy_cluster_epoch_set)
{
	Page page = build_itl_page();
	ClusterUndoTTSlotRef ref;
	ClusterItlSlotData *s = slot_at(page, 0);

	s->flags = ITL_FLAG_ACTIVE;
	s->xid = (TransactionId)12345;
	s->undo_segment_head = (UBA)InvalidUba_init;
	s->commit_scn = InvalidScn;

	UT_ASSERT_EQ((int)cluster_itl_get_tt_ref(page, 0, &ref), 1);
	UT_ASSERT_EQ((int)ref.cluster_epoch, 42); /* stub returns 42 */
}


/* ---------- Branch 3: real UBA decode + owner lookup ---------- */

UT_TEST(test_t10_real_uba_origin_node_derived)
{
	Page page = build_itl_page();
	ClusterUndoTTSlotRef ref;
	ClusterItlSlotData *s = slot_at(page, 0);

	s->flags = ITL_FLAG_ACTIVE;
	s->xid = (TransactionId)12345;
	s->undo_segment_head = uba_encode(257 /* node 1's first segment */, 0, 5, 0);
	s->commit_scn = InvalidScn;

	UT_ASSERT_EQ((int)cluster_itl_get_tt_ref(page, 0, &ref), 1);
	UT_ASSERT_EQ((int)ref.origin_node_id, 1);
}

UT_TEST(test_t11_real_uba_undo_segment_id)
{
	Page page = build_itl_page();
	ClusterUndoTTSlotRef ref;
	ClusterItlSlotData *s = slot_at(page, 0);

	s->flags = ITL_FLAG_ACTIVE;
	s->xid = (TransactionId)12345;
	s->undo_segment_head = uba_encode(257, 0, 5, 0);

	UT_ASSERT_EQ((int)cluster_itl_get_tt_ref(page, 0, &ref), 1);
	UT_ASSERT_EQ((int)ref.undo_segment_id, 257);
}

UT_TEST(test_t12_real_uba_tt_slot_id_offset_plus_1)
{
	Page page = build_itl_page();
	ClusterUndoTTSlotRef ref;
	ClusterItlSlotData *s = slot_at(page, 0);

	s->flags = ITL_FLAG_ACTIVE;
	s->xid = (TransactionId)12345;
	s->undo_segment_head = uba_encode(257, 0, 5, 0);

	UT_ASSERT_EQ((int)cluster_itl_get_tt_ref(page, 0, &ref), 1);
	UT_ASSERT_EQ((int)ref.tt_slot_id, 6); /* offset 5 → id 6 (F1) */
}

UT_TEST(test_t13_real_uba_slot0_id_is_1)
{
	Page page = build_itl_page();
	ClusterUndoTTSlotRef ref;
	ClusterItlSlotData *s = slot_at(page, 0);

	s->flags = ITL_FLAG_ACTIVE;
	s->xid = (TransactionId)12345;
	s->undo_segment_head = uba_encode(1, 0, 0, 0);

	UT_ASSERT_EQ((int)cluster_itl_get_tt_ref(page, 0, &ref), 1);
	UT_ASSERT_EQ((int)ref.tt_slot_id, 1); /* F1: offset 0 → id 1 (NOT 0) */
}

UT_TEST(test_t14_real_uba_slot47_id_is_48)
{
	Page page = build_itl_page();
	ClusterUndoTTSlotRef ref;
	ClusterItlSlotData *s = slot_at(page, 0);

	s->flags = ITL_FLAG_ACTIVE;
	s->xid = (TransactionId)12345;
	s->undo_segment_head = uba_encode(1, 0, 47, 0);

	UT_ASSERT_EQ((int)cluster_itl_get_tt_ref(page, 0, &ref), 1);
	UT_ASSERT_EQ((int)ref.tt_slot_id, 48);
}

UT_TEST(test_t15_malformed_uba_raises)
{
	Page page = build_itl_page();
	ClusterUndoTTSlotRef ref;
	ClusterItlSlotData *s = slot_at(page, 0);
	int before = ereport_raised_count;

	s->flags = ITL_FLAG_ACTIVE;
	s->xid = (TransactionId)12345;
	/* Reserved high bits non-zero -> uba_decode returns false. */
	s->undo_segment_head.raw[0] = 1;
	s->undo_segment_head.raw[1] = ((uint64)1ULL << 32);

	if (sigsetjmp(ereport_recover_jmp, 1) == 0) {
		(void)cluster_itl_get_tt_ref(page, 0, &ref);
		UT_ASSERT_EQ(0, 1);
	}
	UT_ASSERT_NE(ereport_raised_count, before);
}

UT_TEST(test_t16_out_of_range_node_raises)
{
	Page page = build_itl_page();
	ClusterUndoTTSlotRef ref;
	ClusterItlSlotData *s = slot_at(page, 0);
	int before = ereport_raised_count;

	s->flags = ITL_FLAG_ACTIVE;
	s->xid = (TransactionId)12345;
	s->undo_segment_head = uba_encode(32769 /* derived node = 128 > 127 */, 0, 0, 0);

	if (sigsetjmp(ereport_recover_jmp, 1) == 0) {
		(void)cluster_itl_get_tt_ref(page, 0, &ref);
		UT_ASSERT_EQ(0, 1);
	}
	UT_ASSERT_NE(ereport_raised_count, before);
}


/* ---------- has_cached_status semantics ---------- */

UT_TEST(test_t17_has_cached_status_true_for_committed_valid_scn)
{
	Page page = build_itl_page();
	ClusterUndoTTSlotRef ref;
	ClusterItlSlotData *s = slot_at(page, 0);

	s->flags = ITL_FLAG_COMMITTED;
	s->xid = (TransactionId)12345;
	s->undo_segment_head = uba_encode(1, 0, 0, 0);
	s->commit_scn = (SCN)9999; /* valid */

	UT_ASSERT_EQ((int)cluster_itl_get_tt_ref(page, 0, &ref), 1);
	UT_ASSERT_EQ((int)ref.has_cached_status, 1);
}

UT_TEST(test_t18_has_cached_status_false_for_active)
{
	Page page = build_itl_page();
	ClusterUndoTTSlotRef ref;
	ClusterItlSlotData *s = slot_at(page, 0);

	s->flags = ITL_FLAG_ACTIVE;
	s->xid = (TransactionId)12345;
	s->undo_segment_head = uba_encode(1, 0, 0, 0);
	s->commit_scn = InvalidScn;

	UT_ASSERT_EQ((int)cluster_itl_get_tt_ref(page, 0, &ref), 1);
	UT_ASSERT_EQ((int)ref.has_cached_status, 0);
}


int
main(void)
{
	UT_RUN(test_t1_ref_sizeof_32);
	UT_RUN(test_t2_null_page_returns_false);
	UT_RUN(test_t3_null_ref_returns_false);
	UT_RUN(test_t4_slot_idx_out_of_range_returns_false);
	UT_RUN(test_t5_free_slot_returns_false);
	UT_RUN(test_t6_no_itl_area_returns_false);
	UT_RUN(test_t7_legacy_invaliduba_returns_zero_triple);
	UT_RUN(test_t8_legacy_cached_commit_scn_passthrough);
	UT_RUN(test_t9_legacy_cluster_epoch_set);
	UT_RUN(test_t10_real_uba_origin_node_derived);
	UT_RUN(test_t11_real_uba_undo_segment_id);
	UT_RUN(test_t12_real_uba_tt_slot_id_offset_plus_1);
	UT_RUN(test_t13_real_uba_slot0_id_is_1);
	UT_RUN(test_t14_real_uba_slot47_id_is_48);
	UT_RUN(test_t15_malformed_uba_raises);
	UT_RUN(test_t16_out_of_range_node_raises);
	UT_RUN(test_t17_has_cached_status_true_for_committed_valid_scn);
	UT_RUN(test_t18_has_cached_status_false_for_active);

	return ut_failed_count == 0 ? 0 : 1;
}
