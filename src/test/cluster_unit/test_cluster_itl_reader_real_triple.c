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
 *	    T17a reader exposes a NEEDS_CLEANOUT retained SCN as a C1b
 *	         candidate; it is not terminal authority without origin proof
 *	    T18  reader fills has_cached_status=false for ACTIVE
 *	    T19  lock-only raw_xmax scan ignores data ACTIVE slot for same xid
 *	    T20  lock-only raw_xmax scan rejects ambiguous duplicate same wrap
 *	    T21  lock-only raw_xmax scan chooses highest-wrap unique match
 *	    T21a-c data raw_xid scan ignores lock/marker states, rejects an
 *	         ambiguous highest wrap, and selects a unique highest wrap
 *	    T22  spec-3.9 L213 redo parity: fresh page gets pd_block_scn = write_scn
 *	    T23  spec-3.9 L213 redo parity: older write_scn is a monotonic no-op
 *	    T24  spec-3.9 L213 redo parity: InvalidScn write_scn (lock-only) no-op
 *	    T25  spec-3.9 L213 redo parity: newer write_scn advances the watermark
 *	    T33  spec-3.6b current-MX: a lossy marker never evicts a completed
 *	         DATA slot still addressable by tuple.t_itl_slot_idx
 *	    T34  spec-7.1 watch-2: marker reuse of a completed lock-only slot
 *	         contributes nothing
 *	    T35  spec-7.1 watch-2: marker into a FREE slot contributes nothing
 *	    T36  spec-3.6b current-MX: numeric xid/multixact-id collision still
 *	         preserves the completed DATA anchor
 *	    T37-T46  spec-8.4 R4 D6: pure lock-only slot-index selection,
 *	         canonical failure sentinel, highest-wrap winner, equal-winning-
 *	         wrap ambiguity, and old/new reader agreement
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

#include "access/heapam_xlog.h" /* xl_heap_itl_delta_v2 / _block (spec-3.9 L213 redo parity) */
#include "access/htup_details.h"
#include "access/transam.h"
#include "cluster/cluster_itl.h"
#include "cluster/cluster_cr_apply.h"
#include "cluster/cluster_itl_slot.h"
#include "cluster/cluster_scn.h"
#include "cluster/cluster_shmem.h" /* ClusterShmemRegion (spec-3.4e D6 stub) */
#include "cluster/cluster_tt_slot.h"
#include "cluster/cluster_uba.h"
#include "cluster/cluster_xnode_profile.h" /* spec-5.59 D7 stub — profiling gate */
#include "miscadmin.h"					   /* ProcessingMode / Mode (spec-3.4e D6 stub) */
#include "storage/bufpage.h"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"


UT_DEFINE_GLOBALS();

/* The inverse witness overwrites a present tuple; re-adding a missing item
 * is outside this fixture and must not silently succeed. */
OffsetNumber
PageAddItemExtended(Page page, Item item, Size size, OffsetNumber offset, int flags)
{
	UT_ASSERT(false);
	return InvalidOffsetNumber;
}


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

/* spec-5.2 D11: cluster_itl.c's forward-write guard calls the bufmgr
 * write-permission check (bufmgr.o, not linked here).  Stub to "permitted" so
 * the pure ITL-reader/allocator fixture behaves exactly as pre-D11. */
bool
cluster_bufmgr_block_write_permitted(Buffer buf pg_attribute_unused())
{
	return true;
}

/* spec-4.5a G6: cluster_itl.c's slot-pin check calls the merged-recovery
 * materialized gate (cluster_recovery_merge.o, not linked here).  Stub to
 * "nothing materialized" so the allocator behaves exactly as pre-G6 in this
 * pure ITL-reader fixture. */
bool
cluster_merged_any_remote_materialized(void)
{
	return false;
}
bool
cluster_merged_instance_is_materialized(int origin_node pg_attribute_unused())
{
	return false;
}

/* spec-5.59 D7 stubs: cluster_itl.o now carries GUC-gated profiling probes
 * (cluster_xnode_profile.h); the unit harness links neither cluster_guc.o
 * nor cluster_xnode_profile.o, so define the two gate symbols inertly
 * (probes early-return on enabled=false / Ctl=NULL). */
bool cluster_xnode_profile_enabled = false;
ClusterXnodeProfileShared *ClusterXnodeProfileCtl = NULL;

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


/*
 * scn_time_cmp -- spec-3.9 Hardening L213: the redo pd_block_scn parity
 * branch in cluster_itl_redo_apply_block_local_delta calls scn_time_cmp.
 * Linking the full cluster_scn.o would drag in shmem/atomic state, so we
 * provide a byte-faithful local stub using the header-only scn_local()
 * inline (identical to the real impl in cluster_scn.c).
 */
int
scn_time_cmp(SCN a, SCN b)
{
	uint64 la = scn_local(a);
	uint64 lb = scn_local(b);

	if (la < lb)
		return -1;
	if (la > lb)
		return 1;
	return 0;
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
 *	spec-3.4e D6 stubs:  cluster_itl.c now references shmem APIs
 *	(IsBootstrapProcessingMode is a macro using `Mode` global;
 *	ShmemInitStruct;  cluster_shmem_register_region) for fail_closed
 *	counter aggregation.  Reader path doesn't reach those;  provide
 *	stub `Mode` global (NormalProcessing = 2) so the macro evaluates
 *	false, plus link-only stubs for the shmem calls (size_fn /
 *	init_fn never invoked under the IsBootstrapProcessingMode short-
 *	circuit, but ld still needs symbols).
 * ============================================================ */
ProcessingMode Mode = NormalProcessing;

void *
ShmemInitStruct(const char *name pg_attribute_unused(), Size size pg_attribute_unused(),
				bool *foundPtr pg_attribute_unused())
{
	if (foundPtr)
		*foundPtr = true;
	return NULL;
}

void
cluster_shmem_register_region(const ClusterShmemRegion *region pg_attribute_unused())
{}


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
	/* spec-3.10 §v0.5: special area is now 392B (384B slot array + 8B ITL
	 * header).  Reserve the full special size so ClusterPageGetItlHeader (at
	 * special offset 384) stays in-bounds. */
	hdr->pd_special = (LocationIndex)(BLCKSZ - CLUSTER_ITL_SPECIAL_SIZE);
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

UT_TEST(test_t17a_has_cached_status_true_for_needs_cleanout_valid_scn)
{
	Page page = build_itl_page();
	ClusterUndoTTSlotRef ref;
	ClusterItlSlotData *s = slot_at(page, 0);

	s->flags = ITL_FLAG_NEEDS_CLEANOUT;
	s->xid = (TransactionId)4196256;
	s->undo_segment_head = uba_encode(1, 25, 15, 63);
	s->commit_scn = (SCN)111914;

	UT_ASSERT_EQ((int)cluster_itl_get_tt_ref(page, 0, &ref), 1);
	UT_ASSERT_EQ((int)ref.local_xid, 4196256);
	UT_ASSERT_EQ((int)ref.origin_node_id, 0);
	UT_ASSERT_EQ((int)ref.undo_segment_id, 1);
	UT_ASSERT_EQ((int)ref.tt_slot_id, 16);
	UT_ASSERT_EQ((int)ref.cached_commit_scn, 111914);
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


/* ---------- spec-3.4d F9: lock-only raw_xmax scan ---------- */

UT_TEST(test_t19_lock_scan_ignores_data_active_same_xid)
{
	Page page = build_itl_page();
	ClusterUndoTTSlotRef ref;
	ClusterItlSlotData *data = slot_at(page, 0);
	ClusterItlSlotData *lock = slot_at(page, 1);

	data->flags = ITL_FLAG_ACTIVE;
	data->xid = (TransactionId)777;
	data->undo_segment_head = uba_encode(1, 0, 1, 0);

	lock->flags = ITL_FLAG_LOCK_ONLY_ACTIVE;
	lock->xid = (TransactionId)777;
	lock->wrap = 1;
	lock->undo_segment_head = uba_encode(257, 0, 7, 0);

	UT_ASSERT_EQ((int)cluster_itl_find_lock_tt_ref_by_xmax(page, (TransactionId)777, &ref), 1);
	UT_ASSERT_EQ((int)ref.undo_segment_id, 257);
	UT_ASSERT_EQ((int)ref.tt_slot_id, 8);
}

UT_TEST(test_t20_lock_scan_rejects_ambiguous_same_wrap)
{
	Page page = build_itl_page();
	ClusterUndoTTSlotRef ref;
	ClusterItlSlotData *a = slot_at(page, 0);
	ClusterItlSlotData *b = slot_at(page, 1);

	a->flags = ITL_FLAG_LOCK_ONLY_ACTIVE;
	a->xid = (TransactionId)888;
	a->wrap = 3;
	a->undo_segment_head = uba_encode(1, 0, 2, 0);

	b->flags = ITL_FLAG_LOCK_ONLY_ACTIVE;
	b->xid = (TransactionId)888;
	b->wrap = 3;
	b->undo_segment_head = uba_encode(257, 0, 4, 0);

	UT_ASSERT_EQ((int)cluster_itl_find_lock_tt_ref_by_xmax(page, (TransactionId)888, &ref), 0);
}

UT_TEST(test_t21_lock_scan_chooses_highest_wrap)
{
	Page page = build_itl_page();
	ClusterUndoTTSlotRef ref;
	ClusterItlSlotData *old = slot_at(page, 0);
	ClusterItlSlotData *newer = slot_at(page, 1);

	old->flags = ITL_FLAG_LOCK_ONLY_ACTIVE;
	old->xid = (TransactionId)999;
	old->wrap = 1;
	old->undo_segment_head = uba_encode(1, 0, 2, 0);

	newer->flags = ITL_FLAG_LOCK_ONLY_ACTIVE;
	newer->xid = (TransactionId)999;
	newer->wrap = 4;
	newer->undo_segment_head = uba_encode(257, 0, 9, 0);

	UT_ASSERT_EQ((int)cluster_itl_find_lock_tt_ref_by_xmax(page, (TransactionId)999, &ref), 1);
	UT_ASSERT_EQ((int)ref.undo_segment_id, 257);
	UT_ASSERT_EQ((int)ref.tt_slot_id, 10);
}

UT_TEST(test_t21a_data_scan_ignores_lock_and_marker_same_xid)
{
	Page page = build_itl_page();
	ClusterUndoTTSlotRef ref;
	ClusterItlSlotData *data = slot_at(page, 0);
	ClusterItlSlotData *lock = slot_at(page, 1);
	ClusterItlSlotData *marker = slot_at(page, 2);

	data->flags = ITL_FLAG_COMMITTED;
	data->xid = (TransactionId)1001;
	data->wrap = 1;
	data->undo_segment_head = uba_encode(257, 0, 11, 0);
	data->commit_scn = (SCN)7000;

	lock->flags = ITL_FLAG_LOCK_ONLY_COMMITTED;
	lock->xid = (TransactionId)1001;
	lock->wrap = 8;
	lock->undo_segment_head = uba_encode(1, 0, 2, 0);

	marker->flags = ITL_FLAG_LOCK_ONLY_XMAX_IS_MULTI;
	marker->xid = (TransactionId)1001;
	marker->wrap = 9;
	marker->undo_segment_head = uba_encode(1, 0, 3, 0);

	UT_ASSERT_EQ((int)cluster_itl_find_data_tt_ref_by_xid(page, (TransactionId)1001, &ref), 1);
	UT_ASSERT_EQ((int)ref.undo_segment_id, 257);
	UT_ASSERT_EQ((int)ref.tt_slot_id, 12);
	UT_ASSERT_EQ((int)ref.has_cached_status, 1);
}

UT_TEST(test_t21b_data_scan_rejects_ambiguous_highest_wrap)
{
	Page page = build_itl_page();
	ClusterUndoTTSlotRef ref;
	ClusterItlSlotData *a = slot_at(page, 0);
	ClusterItlSlotData *b = slot_at(page, 1);

	a->flags = ITL_FLAG_COMMITTED;
	a->xid = (TransactionId)1002;
	a->wrap = 4;
	a->undo_segment_head = uba_encode(1, 0, 1, 0);
	b->flags = ITL_FLAG_ABORTED;
	b->xid = (TransactionId)1002;
	b->wrap = 4;
	b->undo_segment_head = uba_encode(257, 0, 2, 0);

	UT_ASSERT_EQ((int)cluster_itl_find_data_tt_ref_by_xid(page, (TransactionId)1002, &ref), 0);
}

UT_TEST(test_t21c_data_scan_chooses_unique_highest_wrap)
{
	Page page = build_itl_page();
	ClusterUndoTTSlotRef ref;
	ClusterItlSlotData *old = slot_at(page, 0);
	ClusterItlSlotData *newer = slot_at(page, 1);

	old->flags = ITL_FLAG_COMMITTED;
	old->xid = (TransactionId)1003;
	old->wrap = 2;
	old->undo_segment_head = uba_encode(1, 0, 1, 0);
	newer->flags = ITL_FLAG_NEEDS_CLEANOUT;
	newer->xid = (TransactionId)1003;
	newer->wrap = 5;
	newer->undo_segment_head = uba_encode(257, 0, 13, 0);

	UT_ASSERT_EQ((int)cluster_itl_find_data_tt_ref_by_xid(page, (TransactionId)1003, &ref), 1);
	UT_ASSERT_EQ((int)ref.undo_segment_id, 257);
	UT_ASSERT_EQ((int)ref.tt_slot_id, 14);
}

/* A HOT successor can already have been updated again: its tuple-header ITL
 * then names the later xmax writer, while proof of its xmin creator must keep
 * the independently selected historical DATA slot index. */
UT_TEST(test_t21d_data_slot_index_returns_historical_creator)
{
	Page page = build_itl_page();
	ClusterItlSlotData *creator = slot_at(page, 2);
	ClusterItlSlotData *later_updater = slot_at(page, 6);
	uint8 index = CLUSTER_ITL_SLOT_UNALLOCATED;

	creator->flags = ITL_FLAG_COMMITTED;
	creator->xid = (TransactionId)1004;
	creator->wrap = 3;
	creator->undo_segment_head = uba_encode(257, 0, 17, 0);

	later_updater->flags = ITL_FLAG_COMMITTED;
	later_updater->xid = (TransactionId)2004;
	later_updater->wrap = 8;
	later_updater->undo_segment_head = uba_encode(1, 0, 18, 0);

	UT_ASSERT_EQ((int)cluster_itl_find_data_slot_index_by_xid(page, (TransactionId)1004, &index),
				 1);
	UT_ASSERT_EQ((int)index, 2);
}

UT_TEST(test_t37_lock_slot_index_null_page_sets_sentinel)
{
	uint8 index = 3;

	UT_ASSERT_EQ((int)cluster_itl_find_lock_slot_index_by_xmax(NULL, (TransactionId)1001, &index),
				 0);
	UT_ASSERT_EQ((int)index, (int)CLUSTER_ITL_SLOT_UNALLOCATED);
}

UT_TEST(test_t38_lock_slot_index_miss_sets_sentinel)
{
	Page page = build_itl_page();
	uint8 index = 3;

	UT_ASSERT_EQ((int)cluster_itl_find_lock_slot_index_by_xmax(page, (TransactionId)1002, &index),
				 0);
	UT_ASSERT_EQ((int)index, (int)CLUSTER_ITL_SLOT_UNALLOCATED);
}

UT_TEST(test_t39_lock_slot_index_unique_winner)
{
	Page page = build_itl_page();
	ClusterItlSlotData *slot = slot_at(page, 4);
	uint8 index = CLUSTER_ITL_SLOT_UNALLOCATED;

	slot->flags = ITL_FLAG_LOCK_ONLY_ACTIVE;
	slot->xid = (TransactionId)1003;
	slot->wrap = 2;
	slot->undo_segment_head = uba_encode(257, 3, 7, 5);
	UT_ASSERT_EQ((int)cluster_itl_find_lock_slot_index_by_xmax(page, (TransactionId)1003, &index),
				 1);
	UT_ASSERT_EQ((int)index, 4);
}

UT_TEST(test_t40_lock_slot_index_chooses_highest_wrap)
{
	Page page = build_itl_page();
	ClusterItlSlotData *old = slot_at(page, 2);
	ClusterItlSlotData *newer = slot_at(page, 6);
	uint8 index = CLUSTER_ITL_SLOT_UNALLOCATED;

	old->flags = ITL_FLAG_LOCK_ONLY_ACTIVE;
	old->xid = (TransactionId)1004;
	old->wrap = 8;
	old->undo_segment_head = uba_encode(1, 4, 8, 6);
	newer->flags = ITL_FLAG_LOCK_ONLY_ACTIVE;
	newer->xid = (TransactionId)1004;
	newer->wrap = 9;
	newer->undo_segment_head = uba_encode(257, 5, 9, 7);
	UT_ASSERT_EQ((int)cluster_itl_find_lock_slot_index_by_xmax(page, (TransactionId)1004, &index),
				 1);
	UT_ASSERT_EQ((int)index, 6);
}

UT_TEST(test_t41_lock_slot_index_equal_winning_wrap_is_ambiguous)
{
	Page page = build_itl_page();
	ClusterItlSlotData *a = slot_at(page, 1);
	ClusterItlSlotData *b = slot_at(page, 7);
	uint8 index = 2;

	a->flags = ITL_FLAG_LOCK_ONLY_ACTIVE;
	a->xid = (TransactionId)1005;
	a->wrap = 10;
	a->undo_segment_head = uba_encode(1, 6, 10, 8);
	b->flags = ITL_FLAG_LOCK_ONLY_ACTIVE;
	b->xid = (TransactionId)1005;
	b->wrap = 10;
	b->undo_segment_head = uba_encode(257, 7, 11, 9);
	UT_ASSERT_EQ((int)cluster_itl_find_lock_slot_index_by_xmax(page, (TransactionId)1005, &index),
				 0);
	UT_ASSERT_EQ((int)index, (int)CLUSTER_ITL_SLOT_UNALLOCATED);
}

UT_TEST(test_t42_lock_slot_index_lower_wrap_duplicate_does_not_hide_winner)
{
	Page page = build_itl_page();
	uint8 index = CLUSTER_ITL_SLOT_UNALLOCATED;
	uint8 i;

	for (i = 0; i < 2; i++) {
		ClusterItlSlotData *slot = slot_at(page, i);

		slot->flags = ITL_FLAG_LOCK_ONLY_ACTIVE;
		slot->xid = (TransactionId)1006;
		slot->wrap = 11;
		slot->undo_segment_head = uba_encode(i == 0 ? 1 : 257, 8 + i, 12 + i, 10 + i);
	}
	slot_at(page, 5)->flags = ITL_FLAG_LOCK_ONLY_ACTIVE;
	slot_at(page, 5)->xid = (TransactionId)1006;
	slot_at(page, 5)->wrap = 12;
	slot_at(page, 5)->undo_segment_head = uba_encode(513, 10, 14, 12);
	UT_ASSERT_EQ((int)cluster_itl_find_lock_slot_index_by_xmax(page, (TransactionId)1006, &index),
				 1);
	UT_ASSERT_EQ((int)index, 5);
}

UT_TEST(test_t43_lock_slot_index_ignores_data_and_invalid_uba)
{
	Page page = build_itl_page();
	uint8 index = 6;

	slot_at(page, 0)->flags = ITL_FLAG_ACTIVE;
	slot_at(page, 0)->xid = (TransactionId)1007;
	slot_at(page, 0)->undo_segment_head = uba_encode(1, 11, 15, 13);
	slot_at(page, 1)->flags = ITL_FLAG_LOCK_ONLY_ACTIVE;
	slot_at(page, 1)->xid = (TransactionId)1007;
	memset(&slot_at(page, 1)->undo_segment_head, 0, sizeof(slot_at(page, 1)->undo_segment_head));
	UT_ASSERT_EQ((int)cluster_itl_find_lock_slot_index_by_xmax(page, (TransactionId)1007, &index),
				 0);
	UT_ASSERT_EQ((int)index, (int)CLUSTER_ITL_SLOT_UNALLOCATED);
}

UT_TEST(test_t44_lock_slot_index_and_old_reader_select_same_slot)
{
	Page page = build_itl_page();
	ClusterItlSlotData *winner = slot_at(page, 3);
	ClusterUndoTTSlotRef ref;
	uint8 index = CLUSTER_ITL_SLOT_UNALLOCATED;

	winner->flags = ITL_FLAG_LOCK_ONLY_ACTIVE;
	winner->xid = (TransactionId)1008;
	winner->wrap = 13;
	winner->undo_segment_head = uba_encode(257, 12, 23, 14);
	UT_ASSERT_EQ((int)cluster_itl_find_lock_slot_index_by_xmax(page, (TransactionId)1008, &index),
				 1);
	UT_ASSERT_EQ((int)index, 3);
	UT_ASSERT_EQ((int)cluster_itl_find_lock_tt_ref_by_xmax(page, (TransactionId)1008, &ref), 1);
	UT_ASSERT_EQ((int)ref.undo_segment_id, 257);
	UT_ASSERT_EQ((int)ref.tt_slot_id, 24);
	UT_ASSERT_EQ((int)winner->wrap, 13);
	UT_ASSERT_EQ((int)winner->flags, (int)ITL_FLAG_LOCK_ONLY_ACTIVE);
	UT_ASSERT_EQ((int)winner->xid, 1008);
}

UT_TEST(test_t45_lock_slot_index_invalid_xid_sets_sentinel)
{
	Page page = build_itl_page();
	uint8 index = 0;

	UT_ASSERT_EQ((int)cluster_itl_find_lock_slot_index_by_xmax(page, InvalidTransactionId, &index),
				 0);
	UT_ASSERT_EQ((int)index, (int)CLUSTER_ITL_SLOT_UNALLOCATED);
}

UT_TEST(test_t46_lock_slot_index_page_without_itl_sets_sentinel)
{
	PageHeaderData page;
	uint8 index = 0;

	memset(&page, 0, sizeof(page));
	UT_ASSERT_EQ(
		(int)cluster_itl_find_lock_slot_index_by_xmax((Page)&page, (TransactionId)1009, &index), 0);
	UT_ASSERT_EQ((int)index, (int)CLUSTER_ITL_SLOT_UNALLOCATED);
	UT_ASSERT_EQ(
		(int)cluster_itl_find_lock_slot_index_by_xmax((Page)&page, (TransactionId)1009, NULL), 0);
}


/* ---------- spec-3.9 Hardening L213: pd_block_scn redo parity ----------
 *
 *	cluster_itl_redo_apply_block_local_delta must re-apply the page-level
 *	pd_block_scn watermark (own-instance CR page gate) from a v2 ITL delta,
 *	exactly mirroring cluster_itl_stamp_active on the primary.  Without it a
 *	crash-recovered / standby page keeps pd_block_scn = InvalidScn and the
 *	CR page gate never fires there.  These tests drive the REAL apply fn on a
 *	synthetic ITL page and assert the watermark.
 */

static char redo_delta_buf[256];

static const char *
make_v2_active_delta(uint8 slot_idx, ClusterItlFlags flags_after, TransactionId xid, SCN write_scn)
{
	xl_heap_itl_delta_block *hdr = (xl_heap_itl_delta_block *)redo_delta_buf;
	xl_heap_itl_delta_v2 *d;

	memset(redo_delta_buf, 0, sizeof(redo_delta_buf));
	hdr->ndeltas = 1;
	hdr->reserved = 0;
	hdr->format_version = CLUSTER_ITL_DELTA_FORMAT_V2;

	d = (xl_heap_itl_delta_v2 *)(redo_delta_buf + offsetof(xl_heap_itl_delta_block, deltas));
	d->slot_idx = slot_idx;
	d->flags_after = (uint16)flags_after;
	d->xid = xid;
	d->write_scn = write_scn;
	d->commit_scn = InvalidScn;
	d->undo_segment_head = uba_encode(1, 0, slot_idx, 0);
	return redo_delta_buf;
}

UT_TEST(test_t22_redo_parity_sets_pd_block_scn_on_fresh_page)
{
	/* A crash-recovered / standby page arrives with pd_block_scn = 0. */
	Page page = build_itl_page();

	((PageHeader)page)->pd_block_scn = InvalidScn;
	(void)cluster_itl_redo_apply_block_local_delta(
		page, NULL, make_v2_active_delta(0, ITL_FLAG_ACTIVE, (TransactionId)12345, (SCN)1000));
	/* Parity: redo reproduced the primary's pd_block_scn = write_scn. */
	UT_ASSERT_EQ((int)(((PageHeader)page)->pd_block_scn == (SCN)1000), 1);
}

UT_TEST(test_t23_redo_parity_monotonic_older_write_scn_noop)
{
	Page page = build_itl_page();

	((PageHeader)page)->pd_block_scn = (SCN)2000;
	(void)cluster_itl_redo_apply_block_local_delta(
		page, NULL, make_v2_active_delta(0, ITL_FLAG_ACTIVE, (TransactionId)12345, (SCN)1000));
	/* Older write_scn must NOT lower the "last modified at" watermark. */
	UT_ASSERT_EQ((int)(((PageHeader)page)->pd_block_scn == (SCN)2000), 1);
}

UT_TEST(test_t24_redo_parity_invalid_write_scn_noop)
{
	Page page = build_itl_page();

	((PageHeader)page)->pd_block_scn = (SCN)1500;
	/* Lock-only delta carries InvalidScn write_scn -> SCN_VALID guard no-op,
	 * exactly mirroring stamp_active which only writes on a valid write_scn. */
	(void)cluster_itl_redo_apply_block_local_delta(
		page, NULL,
		make_v2_active_delta(0, ITL_FLAG_LOCK_ONLY_ACTIVE, (TransactionId)12345, InvalidScn));
	UT_ASSERT_EQ((int)(((PageHeader)page)->pd_block_scn == (SCN)1500), 1);
}

UT_TEST(test_t25_redo_parity_newer_write_scn_advances)
{
	Page page = build_itl_page();

	((PageHeader)page)->pd_block_scn = (SCN)1000;
	(void)cluster_itl_redo_apply_block_local_delta(
		page, NULL, make_v2_active_delta(0, ITL_FLAG_ACTIVE, (TransactionId)12345, (SCN)3000));
	/* Newer write_scn advances the watermark. */
	UT_ASSERT_EQ((int)(((PageHeader)page)->pd_block_scn == (SCN)3000), 1);
}


/* ============================================================
 *	spec-3.10 §v0.5 slot-reuse fail-closed:  recycle watermark
 *	contribution predicate (E4-E6 + ACTIVE-diff-xid + positive),
 *	monotone advance, and redo-derived parity (E7 unit level).
 * ============================================================ */

UT_TEST(test_t26_watermark_free_slot_no_contribution)
{
	/* E4: a FREE slot evicts nothing. */
	UT_ASSERT_EQ((int)SCN_VALID(cluster_itl_recycle_watermark_contribution(
					 ITL_FLAG_FREE, InvalidTransactionId, InvalidScn, (TransactionId)200)),
				 0);
}

UT_TEST(test_t27_watermark_same_xid_no_contribution)
{
	/* E5: the same xid reusing its own (COMMITTED) slot contributes nothing. */
	UT_ASSERT_EQ((int)SCN_VALID(cluster_itl_recycle_watermark_contribution(
					 ITL_FLAG_COMMITTED, (TransactionId)100, (SCN)5000, (TransactionId)100)),
				 0);
}

UT_TEST(test_t28_watermark_lock_only_no_contribution)
{
	/* A completed locker can carry a predecessor DATA head in retained undo.
	 * Discard without a history proof is now a loss, just like DATA. */
	UT_ASSERT_EQ(
		(int)SCN_VALID(cluster_itl_recycle_watermark_contribution(
			ITL_FLAG_LOCK_ONLY_COMMITTED, (TransactionId)100, (SCN)5000, (TransactionId)200)),
		1);
	UT_ASSERT_EQ(
		(int)SCN_VALID(cluster_itl_recycle_watermark_contribution(
			ITL_FLAG_LOCK_ONLY_ABORTED, (TransactionId)100, (SCN)5000, (TransactionId)200)),
		1);
}

UT_TEST(test_t29_watermark_active_diff_xid_no_contribution)
{
	/* User tightening 2026-06-02: an ACTIVE slot owned by a DIFFERENT xid must
	 * never be recycled, so the predicate must NOT silently mask it. */
	UT_ASSERT_EQ((int)SCN_VALID(cluster_itl_recycle_watermark_contribution(
					 ITL_FLAG_ACTIVE, (TransactionId)100, (SCN)5000, (TransactionId)200)),
				 0);
}

UT_TEST(test_t30_watermark_completed_data_contributes)
{
	/* Positive: COMMITTED / ABORTED / NEEDS_CLEANOUT (different xid, valid
	 * write_scn) all contribute their write_scn. */
	UT_ASSERT_EQ((int)(cluster_itl_recycle_watermark_contribution(
						   ITL_FLAG_COMMITTED, (TransactionId)100, (SCN)5000, (TransactionId)200)
					   == (SCN)5000),
				 1);
	UT_ASSERT_EQ((int)(cluster_itl_recycle_watermark_contribution(
						   ITL_FLAG_ABORTED, (TransactionId)100, (SCN)5000, (TransactionId)200)
					   == (SCN)5000),
				 1);
	UT_ASSERT_EQ((int)(cluster_itl_recycle_watermark_contribution(ITL_FLAG_NEEDS_CLEANOUT,
																  (TransactionId)100, (SCN)5000,
																  (TransactionId)200)
					   == (SCN)5000),
				 1);
	/* Completed data but InvalidScn write_scn -> nothing. */
	UT_ASSERT_EQ((int)SCN_VALID(cluster_itl_recycle_watermark_contribution(
					 ITL_FLAG_COMMITTED, (TransactionId)100, InvalidScn, (TransactionId)200)),
				 0);
}

UT_TEST(test_t31_watermark_advance_monotone)
{
	Page page = build_itl_page();

	ClusterPageGetItlHeader(page)->itl_recycle_watermark_scn = InvalidScn;
	/* InvalidScn contrib -> no-op. */
	cluster_itl_block_watermark_advance(page, InvalidScn);
	UT_ASSERT_EQ((int)SCN_VALID(ClusterPageGetItlHeader(page)->itl_recycle_watermark_scn), 0);
	/* First valid contrib -> set. */
	cluster_itl_block_watermark_advance(page, (SCN)5000);
	UT_ASSERT_EQ((int)(ClusterPageGetItlHeader(page)->itl_recycle_watermark_scn == (SCN)5000), 1);
	/* Older contrib -> no-op (monotone). */
	cluster_itl_block_watermark_advance(page, (SCN)3000);
	UT_ASSERT_EQ((int)(ClusterPageGetItlHeader(page)->itl_recycle_watermark_scn == (SCN)5000), 1);
	/* Newer contrib -> advances. */
	cluster_itl_block_watermark_advance(page, (SCN)7000);
	UT_ASSERT_EQ((int)(ClusterPageGetItlHeader(page)->itl_recycle_watermark_scn == (SCN)7000), 1);
}

UT_TEST(test_t32_redo_recomputes_recycle_watermark)
{
	/* E7 (unit level): redo of an ACTIVE delta that recycles a COMMITTED slot
	 * folds the evicted COMMITTED writer's write_scn into the page watermark --
	 * the non-FPI incremental delta path restores the fail-closed guard. */
	Page page = build_itl_page();
	ClusterItlSlotData *slot = slot_at(page, 0);

	ClusterPageGetItlHeader(page)->itl_recycle_watermark_scn = InvalidScn;
	/* Pre-state: slot 0 holds a COMMITTED data writer xid=100, write_scn=5000. */
	slot->flags = ITL_FLAG_COMMITTED;
	slot->xid = (TransactionId)100;
	slot->write_scn = (SCN)5000;
	/* Redo a NEW ACTIVE writer (xid=200, write_scn=6000) recycling slot 0. */
	(void)cluster_itl_redo_apply_block_local_delta(
		page, NULL, make_v2_active_delta(0, ITL_FLAG_ACTIVE, (TransactionId)200, (SCN)6000));
	/* Watermark folded the EVICTED COMMITTED write_scn (5000), not the new 6000. */
	UT_ASSERT_EQ((int)(ClusterPageGetItlHeader(page)->itl_recycle_watermark_scn == (SCN)5000), 1);
	/* And the slot is now the new ACTIVE writer. */
	UT_ASSERT_EQ((int)(slot->flags == ITL_FLAG_ACTIVE), 1);
	UT_ASSERT_EQ((int)(slot->write_scn == (SCN)6000), 1);
}


/* spec-5.2 §3.5 D11: cluster_itl_page_has_active_slot — the holder-side defer
 * trigger.  In-progress (ACTIVE / LOCK_ONLY_ACTIVE) => true; FREE and all
 * terminal states => false. */
/* ============================================================
 *	spec-3.6b current-MX authority closure: a MultiXact marker is a lossy
 *	page-format hint and must never evict a completed DATA slot.  A live
 *	tuple may still point at that slot through t_itl_slot_idx; replacing it
 *	with a marker makes the next visibility check fail closed as "TT slot
 *	recycled" before the authoritative current-MX path can run.  T33-T36
 *	drive the REAL cluster_itl_stamp_multixact_marker through the
 *	bufmgr globals (BufferBlocks -> synthetic page, Buffer 1): the
 *	static-inline BufferGetPage compiled into cluster_itl.o resolves
 *	Buffer 1 to BufferBlocks + 0.
 * ============================================================ */

static Buffer
marker_buffer_for(Page page)
{
	BufferBlocks = (char *)page;
	NBuffers = 1;
	return (Buffer)1;
}

UT_TEST(test_t33_marker_full_page_preserves_completed_data_anchor)
{
	Page page = build_itl_page();
	Buffer buf = marker_buffer_for(page);
	uint8 i;
	uint8 idx;

	/* Every slot ACTIVE (not FREE, not reusable) except slot 5: a COMMITTED
	 * data writer -- the only reuse candidate on a full page. */
	for (i = 0; i < CLUSTER_ITL_INITRANS_DEFAULT; i++) {
		slot_at(page, i)->flags = ITL_FLAG_ACTIVE;
		slot_at(page, i)->xid = (TransactionId)(1000 + i);
	}
	slot_at(page, 5)->flags = ITL_FLAG_COMMITTED;
	slot_at(page, 5)->xid = (TransactionId)100;
	slot_at(page, 5)->write_scn = (SCN)5000;
	slot_at(page, 5)->wrap = 7;
	ClusterPageGetItlHeader(page)->itl_recycle_watermark_scn = InvalidScn;

	idx = cluster_itl_stamp_multixact_marker(buf, (MultiXactId)4242);
	UT_ASSERT_EQ((int)idx, (int)CLUSTER_ITL_SLOT_UNALLOCATED);
	UT_ASSERT_EQ((int)(slot_at(page, 5)->flags == ITL_FLAG_COMMITTED), 1);
	UT_ASSERT_EQ((int)slot_at(page, 5)->xid, 100);
	UT_ASSERT_EQ((int)(slot_at(page, 5)->write_scn == (SCN)5000), 1);
	UT_ASSERT_EQ((int)slot_at(page, 5)->wrap, 7);
	UT_ASSERT_EQ((int)SCN_VALID(ClusterPageGetItlHeader(page)->itl_recycle_watermark_scn), 0);
}

UT_TEST(test_t34_marker_reuse_lock_only_preserves_loss_bound)
{
	Page page = build_itl_page();
	Buffer buf = marker_buffer_for(page);
	uint8 i;
	uint8 idx;

	for (i = 0; i < CLUSTER_ITL_INITRANS_DEFAULT; i++) {
		slot_at(page, i)->flags = ITL_FLAG_ACTIVE;
		slot_at(page, i)->xid = (TransactionId)(1000 + i);
	}
	/* Marker publication has no inverse history trailer. A completed lock
	 * can carry an older DATA anchor, so losing it must retain a loss bound. */
	slot_at(page, 3)->flags = ITL_FLAG_LOCK_ONLY_COMMITTED;
	slot_at(page, 3)->xid = (TransactionId)100;
	slot_at(page, 3)->write_scn = (SCN)5000;
	ClusterPageGetItlHeader(page)->itl_recycle_watermark_scn = InvalidScn;

	idx = cluster_itl_stamp_multixact_marker(buf, (MultiXactId)4242);
	UT_ASSERT_EQ((int)idx, 3);
	UT_ASSERT_EQ(ClusterPageGetItlHeader(page)->itl_recycle_watermark_scn, (SCN)5000);
}

UT_TEST(test_t35_marker_free_slot_no_fold)
{
	Page page = build_itl_page();
	Buffer buf = marker_buffer_for(page);
	uint8 idx;

	/* All slots FREE (build_itl_page zero-fill): marker takes slot 0 with no
	 * eviction, so the watermark must stay Invalid and wrap must stay 0. */
	ClusterPageGetItlHeader(page)->itl_recycle_watermark_scn = InvalidScn;

	idx = cluster_itl_stamp_multixact_marker(buf, (MultiXactId)4242);
	UT_ASSERT_EQ((int)idx, 0);
	UT_ASSERT_EQ((int)SCN_VALID(ClusterPageGetItlHeader(page)->itl_recycle_watermark_scn), 0);
	UT_ASSERT_EQ((int)slot_at(page, 0)->wrap, 0);
}

UT_TEST(test_t36_marker_xid_mxid_collision_preserves_data_anchor)
{
	Page page = build_itl_page();
	Buffer buf = marker_buffer_for(page);
	uint8 i;
	uint8 idx;

	/* 8.A pin: the completed DATA xid numerically equals the MultiXactId
	 * being stamped.  The separate id domains do not authorize replacing
	 * the tuple's data anchor with a marker. */
	for (i = 0; i < CLUSTER_ITL_INITRANS_DEFAULT; i++) {
		slot_at(page, i)->flags = ITL_FLAG_ACTIVE;
		slot_at(page, i)->xid = (TransactionId)(1000 + i);
	}
	slot_at(page, 2)->flags = ITL_FLAG_COMMITTED;
	slot_at(page, 2)->xid = (TransactionId)4242;
	slot_at(page, 2)->write_scn = (SCN)6000;
	ClusterPageGetItlHeader(page)->itl_recycle_watermark_scn = InvalidScn;

	idx = cluster_itl_stamp_multixact_marker(buf, (MultiXactId)4242);
	UT_ASSERT_EQ((int)idx, (int)CLUSTER_ITL_SLOT_UNALLOCATED);
	UT_ASSERT_EQ((int)(slot_at(page, 2)->flags == ITL_FLAG_COMMITTED), 1);
	UT_ASSERT_EQ((int)slot_at(page, 2)->xid, 4242);
	UT_ASSERT_EQ((int)(slot_at(page, 2)->write_scn == (SCN)6000), 1);
	UT_ASSERT_EQ((int)SCN_VALID(ClusterPageGetItlHeader(page)->itl_recycle_watermark_scn), 0);
}

UT_TEST(test_marker_recasts_stale_marker_before_free_slot)
{
	Page page = build_itl_page();
	Buffer buf = marker_buffer_for(page);
	uint8 idx;

	/* A marker never joins ITL touch/terminal cleanup.  Recast the existing
	 * lossy marker before consuming another slot. */
	slot_at(page, 4)->flags = ITL_FLAG_LOCK_ONLY_XMAX_IS_MULTI;
	slot_at(page, 4)->xid = (TransactionId)4000;
	slot_at(page, 4)->wrap = 9;

	idx = cluster_itl_stamp_multixact_marker(buf, (MultiXactId)4242);
	UT_ASSERT_EQ((int)idx, 4);
	UT_ASSERT_EQ((int)slot_at(page, 4)->xid, 4242);
	UT_ASSERT_EQ((int)(slot_at(page, 4)->flags == ITL_FLAG_LOCK_ONLY_XMAX_IS_MULTI), 1);
	UT_ASSERT_EQ((int)slot_at(page, 4)->wrap, 10);
	UT_ASSERT_EQ((int)(slot_at(page, 0)->flags == ITL_FLAG_FREE), 1);
}

UT_TEST(test_marker_same_mxid_is_idempotent)
{
	Page page = build_itl_page();
	Buffer buf = marker_buffer_for(page);
	uint8 idx;

	slot_at(page, 4)->flags = ITL_FLAG_LOCK_ONLY_XMAX_IS_MULTI;
	slot_at(page, 4)->xid = (TransactionId)4242;
	slot_at(page, 4)->wrap = 9;

	idx = cluster_itl_stamp_multixact_marker(buf, (MultiXactId)4242);
	UT_ASSERT_EQ((int)idx, 4);
	UT_ASSERT_EQ((int)slot_at(page, 4)->xid, 4242);
	UT_ASSERT_EQ((int)slot_at(page, 4)->wrap, 9);
	UT_ASSERT_EQ((int)(slot_at(page, 0)->flags == ITL_FLAG_FREE), 1);
}

UT_TEST(test_marker_collapses_legacy_stale_accumulation)
{
	Page page = build_itl_page();
	Buffer buf = marker_buffer_for(page);
	uint8 idx;
	uint8 i;
	int marker_count = 0;

	for (i = 1; i <= 3; i++) {
		slot_at(page, i)->flags = ITL_FLAG_LOCK_ONLY_XMAX_IS_MULTI;
		slot_at(page, i)->xid = (TransactionId)(4000 + i);
		slot_at(page, i)->wrap = (uint16)(10 + i);
	}

	idx = cluster_itl_stamp_multixact_marker(buf, (MultiXactId)4242);
	UT_ASSERT_EQ((int)idx, 1);
	for (i = 0; i < CLUSTER_ITL_INITRANS_DEFAULT; i++)
		if (slot_at(page, i)->flags == ITL_FLAG_LOCK_ONLY_XMAX_IS_MULTI)
			marker_count++;
	UT_ASSERT_EQ(marker_count, 1);
	UT_ASSERT_EQ((int)slot_at(page, 1)->xid, 4242);
	UT_ASSERT_EQ((int)slot_at(page, 1)->wrap, 12);
	UT_ASSERT_EQ((int)(slot_at(page, 2)->flags == ITL_FLAG_FREE), 1);
	UT_ASSERT_EQ((int)slot_at(page, 2)->xid, (int)InvalidTransactionId);
	UT_ASSERT_EQ((int)slot_at(page, 2)->wrap, 12);
	UT_ASSERT_EQ((int)(slot_at(page, 3)->flags == ITL_FLAG_FREE), 1);
	UT_ASSERT_EQ((int)slot_at(page, 3)->xid, (int)InvalidTransactionId);
	UT_ASSERT_EQ((int)slot_at(page, 3)->wrap, 13);
}

UT_TEST(test_d11_page_has_active_slot_detects_active)
{
	Page page = build_itl_page();
	int i;

	for (i = 0; i < CLUSTER_ITL_INITRANS_DEFAULT; i++)
		slot_at(page, (uint8)i)->flags = ITL_FLAG_FREE;
	UT_ASSERT_EQ((int)cluster_itl_page_has_active_slot(page), 0);

	/* terminal / cleanout states do not count as active */
	slot_at(page, 1)->flags = ITL_FLAG_COMMITTED;
	slot_at(page, 2)->flags = ITL_FLAG_ABORTED;
	slot_at(page, 3)->flags = ITL_FLAG_NEEDS_CLEANOUT;
	slot_at(page, 4)->flags = ITL_FLAG_LOCK_ONLY_COMMITTED;
	slot_at(page, 5)->flags = ITL_FLAG_LOCK_ONLY_ABORTED;
	UT_ASSERT_EQ((int)cluster_itl_page_has_active_slot(page), 0);

	/* a data writer in progress => active */
	slot_at(page, 6)->flags = ITL_FLAG_ACTIVE;
	UT_ASSERT_EQ((int)cluster_itl_page_has_active_slot(page), 1);

	/* reset; a row-lock holder in progress => active */
	slot_at(page, 6)->flags = ITL_FLAG_FREE;
	slot_at(page, 7)->flags = ITL_FLAG_LOCK_ONLY_ACTIVE;
	UT_ASSERT_EQ((int)cluster_itl_page_has_active_slot(page), 1);
}

UT_TEST(test_d11_page_has_active_slot_no_itl_is_false)
{
	static char no_itl[BLCKSZ];
	PageHeader hdr = (PageHeader)no_itl;

	memset(no_itl, 0, BLCKSZ);
	hdr->pd_flags = 0; /* no PD_HAS_ITL */
	hdr->pd_special = BLCKSZ;
	UT_ASSERT_EQ((int)cluster_itl_page_has_active_slot((Page)no_itl), 0);
}

UT_TEST(test_precommit_cleanout_evidence_is_not_directly_reusable)
{
	Page page = build_itl_page();
	Buffer buf = marker_buffer_for(page);
	uint8 slot_index = CLUSTER_ITL_SLOT_UNALLOCATED;
	uint8 i;

	for (i = 0; i < CLUSTER_ITL_INITRANS_DEFAULT; i++) {
		slot_at(page, i)->flags = ITL_FLAG_ACTIVE;
		slot_at(page, i)->xid = (TransactionId)(5000 + i);
	}
	slot_at(page, 3)->flags = ITL_FLAG_NEEDS_CLEANOUT;
	slot_at(page, 3)->commit_scn = (SCN)7001;

	UT_ASSERT_EQ((int)cluster_itl_alloc_or_reuse_slot(buf, (TransactionId)6000, &slot_index), 0);
	UT_ASSERT_EQ((int)slot_index, (int)CLUSTER_ITL_SLOT_UNALLOCATED);
}

static HeapTupleHeader
append_plain_lock_tuple(Page page, TransactionId xid)
{
	PageHeader header = (PageHeader)page;
	OffsetNumber offset = PageGetMaxOffsetNumber(page) + 1;
	HeapTupleHeader tuple;

	header->pd_lower += sizeof(ItemIdData);
	header->pd_upper -= MAXALIGN(SizeofHeapTupleHeader);
	ItemIdSetNormal(PageGetItemId(page, offset), header->pd_upper, SizeofHeapTupleHeader);
	tuple = (HeapTupleHeader)PageGetItem(page, PageGetItemId(page, offset));
	HeapTupleHeaderSetXmin(tuple, FrozenTransactionId);
	HeapTupleHeaderSetXmax(tuple, xid);
	tuple->t_infomask = HEAP_XMIN_FROZEN | HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_EXCL_LOCK;
	tuple->t_hoff = SizeofHeapTupleHeader;
	tuple->t_itl_slot_idx = CLUSTER_ITL_SLOT_UNALLOCATED;
	ItemPointerSet(&tuple->t_ctid, 17, offset);
	return tuple;
}

/* Counterfactual RED uses the unchanged production DATA selector, not a
 * simulated allocator. Normal builds exercise the exact UPDATE selector. */
#ifdef TEST_ITL_BASELINE_SELECTOR
#define cluster_itl_alloc_update_slot(buf, xid, off, out)                                          \
	cluster_itl_alloc_or_reuse_slot(buf, xid, out)
#endif

static Page
build_eight_locked_rows(void)
{
	Page page = build_itl_page();

	for (int i = 0; i < CLUSTER_ITL_INITRANS_DEFAULT; i++) {
		ClusterItlSlotData *slot = slot_at(page, i);

		(void)append_plain_lock_tuple(page, 700 + i);
		slot->xid = 700 + i;
		slot->flags = ITL_FLAG_LOCK_ONLY_ACTIVE;
		slot->wrap = 12;
		slot->undo_segment_head = uba_encode(1, 2, i, 1);
	}
	return page;
}

UT_TEST(update_own_predecessor_lock_does_not_need_a_ninth_slot)
{
	for (int i = 0; i < CLUSTER_ITL_INITRANS_DEFAULT; i++) {
		Page page = build_eight_locked_rows();
		Buffer buf = marker_buffer_for(page);
		PGAlignedBlock before;
		uint8 selected = CLUSTER_ITL_SLOT_UNALLOCATED;
		bool found;

		memcpy(before.data, page, BLCKSZ);
		found = cluster_itl_alloc_update_slot(buf, 700 + i, i + 1, &selected);
		UT_ASSERT(found);
		UT_ASSERT_EQ(memcmp(before.data, page, BLCKSZ), 0);
		if (!found)
			continue;
		UT_ASSERT_EQ(selected, i);
		/* Selection alone never releases a row lock. Publication retains the
		 * predecessor via the caller's existing history/receipt boundary. */
		cluster_itl_stamp_active_with_history(buf, selected, 700 + i, 900, uba_encode(1, 3, i, 1));
		UT_ASSERT_EQ(slot_at(page, selected)->flags, ITL_FLAG_ACTIVE);
		UT_ASSERT_EQ(slot_at(page, selected)->wrap, 13);
		UT_ASSERT_EQ(memcmp(PageGetItem(page, PageGetItemId(page, i + 1)),
							PageGetItem((Page)before.data, PageGetItemId((Page)before.data, i + 1)),
							SizeofHeapTupleHeader),
					 0);
		for (int j = 0; j < CLUSTER_ITL_INITRANS_DEFAULT; j++)
			if (j != i)
				UT_ASSERT_EQ(memcmp(slot_at(page, j), slot_at((Page)before.data, j),
									sizeof(ClusterItlSlotData)),
							 0);
	}
}

UT_TEST(update_handoff_refuses_unproved_or_still_referenced_lock_slot)
{
	for (int fault = 0; fault < 10; fault++) {
		Page page = build_eight_locked_rows();
		Buffer buf = marker_buffer_for(page);
		HeapTupleHeader tuple = (HeapTupleHeader)PageGetItem(page, PageGetItemId(page, 1));
		PGAlignedBlock before;
		uint8 selected = CLUSTER_ITL_SLOT_UNALLOCATED;
		OffsetNumber off = 1;

		switch (fault) {
		case 0:
			(void)append_plain_lock_tuple(page, 700);
			break;
		case 1:
			tuple->t_infomask |= HEAP_XMAX_IS_MULTI;
			break;
		case 2:
			*slot_at(page, 1) = *slot_at(page, 0);
			break;
		case 3:
			slot_at(page, 0)->undo_segment_head = (UBA)InvalidUba_init;
			break;
		case 4:
			slot_at(page, 0)->wrap = UINT16_MAX;
			break;
		case 5:
			off = 9;
			break;
		case 6:
			HeapTupleHeaderSetXmax(tuple, 701);
			break;
		case 7:
			ItemIdSetNormal(PageGetItemId(page, 1), BLCKSZ - 1, 24);
			break;
		case 8:
			tuple->t_infomask |= HEAP_XMAX_INVALID;
			break;
		case 9:
			append_plain_lock_tuple(page, 901)->t_infomask |= HEAP_XMAX_IS_MULTI;
			break;
		}
		memcpy(before.data, page, BLCKSZ);
		UT_ASSERT(!cluster_itl_alloc_update_slot(buf, 700, off, &selected));
		UT_ASSERT_EQ(selected, CLUSTER_ITL_SLOT_UNALLOCATED);
		UT_ASSERT_EQ(memcmp(before.data, page, BLCKSZ), 0);
	}
}

UT_TEST(update_handoff_keeps_ordinary_data_allocation_priority)
{
	for (int leg = 0; leg < 2; leg++) {
		Page page = build_eight_locked_rows();
		Buffer buf = marker_buffer_for(page);
		uint8 selected = CLUSTER_ITL_SLOT_UNALLOCATED;

		slot_at(page, 5)->flags = leg == 0 ? ITL_FLAG_FREE : ITL_FLAG_ACTIVE;
		slot_at(page, 5)->xid = 700;
		UT_ASSERT(cluster_itl_alloc_update_slot(buf, 700, 1, &selected));
		UT_ASSERT_EQ(selected, 5);
		UT_ASSERT_EQ(slot_at(page, 0)->flags, ITL_FLAG_LOCK_ONLY_ACTIVE);
	}
}

UT_TEST(completed_lock_reuse_normalizes_only_matching_plain_locks)
{
	int leg;

	for (leg = 0; leg < 2; leg++) {
		Page page = build_itl_page();
		Buffer buffer = marker_buffer_for(page);
		HeapTupleHeader matching = append_plain_lock_tuple(page, 700);
		HeapTupleHeader other = append_plain_lock_tuple(page, 701);
		HeapTupleHeader multi = append_plain_lock_tuple(page, 700);
		HeapTupleHeader data = append_plain_lock_tuple(page, 700);
		HeapTupleHeaderData before = *matching;
		ClusterItlSlotData *slot = slot_at(page, 0);

		multi->t_infomask |= HEAP_XMAX_IS_MULTI;
		data->t_infomask &= ~(HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_EXCL_LOCK);
		slot->flags = leg == 0 ? ITL_FLAG_LOCK_ONLY_COMMITTED : ITL_FLAG_LOCK_ONLY_ABORTED;
		slot->xid = 700;
		slot->wrap = 7;
		cluster_itl_stamp_active(buffer, 0, 702, 900, uba_encode(1, 2, 8, 1));
		before.t_infomask |= HEAP_XMAX_INVALID;
		UT_ASSERT_EQ(memcmp(matching, &before, SizeofHeapTupleHeader), 0);
		UT_ASSERT((other->t_infomask & HEAP_XMAX_INVALID) == 0);
		UT_ASSERT((multi->t_infomask & HEAP_XMAX_INVALID) == 0);
		UT_ASSERT((data->t_infomask & HEAP_XMAX_INVALID) == 0);
	}
}

UT_TEST(active_own_lock_to_data_keeps_unreleased_locks)
{
	Page page = build_itl_page();
	Buffer buffer = marker_buffer_for(page);
	HeapTupleHeader tuple = append_plain_lock_tuple(page, 700);
	ClusterItlSlotData *slot = slot_at(page, 0);

	slot->flags = ITL_FLAG_LOCK_ONLY_ACTIVE;
	slot->xid = 700;
	cluster_itl_stamp_active(buffer, 0, 700, 900, uba_encode(1, 2, 8, 1));
	UT_ASSERT((tuple->t_infomask & HEAP_XMAX_INVALID) == 0);
}

UT_TEST(replacement_redo_normalizes_old_active_lock_and_is_idempotent)
{
	Page page = build_itl_page();
	HeapTupleHeader tuple = append_plain_lock_tuple(page, 700);
	ClusterItlSlotData *slot = slot_at(page, 0);
	char after[BLCKSZ];
	const char *delta;

	/* Terminal cleanout may have been an unlogged hint. Replacement WAL,
	 * not a requester-local status guess, proves the old owner was retired. */
	slot->flags = ITL_FLAG_LOCK_ONLY_ACTIVE;
	slot->xid = 700;
	delta = make_v2_active_delta(0, ITL_FLAG_ACTIVE, 702, 900);
	(void)cluster_itl_redo_apply_block_local_delta(page, NULL, delta);
	UT_ASSERT((tuple->t_infomask & HEAP_XMAX_INVALID) != 0);
	memcpy(after, page, BLCKSZ);
	(void)cluster_itl_redo_apply_block_local_delta(page, NULL, delta);
	UT_ASSERT_EQ(memcmp(after, page, BLCKSZ), 0);
}

UT_TEST(marker_reuse_normalizes_retired_plain_lock)
{
	Page page = build_itl_page();
	Buffer buffer = marker_buffer_for(page);
	HeapTupleHeader tuple = append_plain_lock_tuple(page, 700);
	int i;

	for (i = 0; i < CLUSTER_ITL_INITRANS_DEFAULT; i++) {
		slot_at(page, i)->flags = ITL_FLAG_ACTIVE;
		slot_at(page, i)->xid = 800 + i;
	}
	slot_at(page, 0)->flags = ITL_FLAG_LOCK_ONLY_COMMITTED;
	slot_at(page, 0)->xid = 700;
	UT_ASSERT_EQ(cluster_itl_stamp_multixact_marker(buffer, 900), 0);
	UT_ASSERT((tuple->t_infomask & HEAP_XMAX_INVALID) != 0);
}

#include "test_cluster_itl_undo_capture.inc"

UT_TEST(undo_capture_and_real_inverse_do_not_resurrect_retired_plain_lock)
{
	Page page = build_itl_page();
	HeapTupleHeader tuple = append_plain_lock_tuple(page, 700);
	ClusterItlSlotData *slot = slot_at(page, 0);
	UndoRecordHeader header = { 0 };
	UndoItlPayload payload = { 0 };
	ClusterUndoTTSlotRef ref;

	slot->flags = ITL_FLAG_LOCK_ONLY_COMMITTED;
	slot->xid = 700;
	slot->wrap = 7;
	slot->undo_segment_head = uba_encode(257, 1, 7, 1);
	payload.prev_xmax = 700;
	payload.prev_infomask = tuple->t_infomask;
	payload.prev_infomask2 = tuple->t_infomask2;
	UT_ASSERT(cluster_heap_capture_undo_prior_lock(page, UNDO_RECORD_ITL, 0, 0, slot, &payload,
												   sizeof(payload)));
	HeapTupleHeaderSetXmax(tuple, 701);
	slot->xid = 701;
	slot->flags = ITL_FLAG_LOCK_ONLY_ACTIVE;
	header.target_offset = FirstOffsetNumber;
	UT_ASSERT(cluster_cr_apply_itl_inverse((char *)page, &header, &payload));
	UT_ASSERT((tuple->t_infomask & HEAP_XMAX_INVALID) != 0
			  || cluster_itl_find_lock_tt_ref_by_xmax(page, 700, &ref));
	UT_ASSERT_EQ(slot->flags, ITL_FLAG_FREE);
}

UT_TEST(retired_older_or_ambiguous_slot_cannot_release_newer_locker)
{
	int leg;

	for (leg = 0; leg < 2; leg++) {
		Page page = build_itl_page();
		Buffer buffer = marker_buffer_for(page);
		HeapTupleHeader tuple = append_plain_lock_tuple(page, 700);
		ClusterItlSlotData *old = slot_at(page, 0);
		ClusterItlSlotData *newer = slot_at(page, 1);

		old->flags = ITL_FLAG_LOCK_ONLY_COMMITTED;
		old->xid = 700;
		old->wrap = 7;
		newer->flags = ITL_FLAG_LOCK_ONLY_ACTIVE;
		newer->xid = 700;
		newer->wrap = leg == 0 ? 8 : 7;
		cluster_itl_stamp_active(buffer, 0, 701, 900, uba_encode(1, 1, 2, 0));
		UT_ASSERT((tuple->t_infomask & HEAP_XMAX_INVALID) == 0);
	}
}

UT_TEST(undo_active_and_unproved_lock_headers_are_not_normalized)
{
	int leg;

	for (leg = 0; leg < 5; leg++) {
		Page page = build_itl_page();
		HeapTupleHeader tuple = append_plain_lock_tuple(page, 700);
		ClusterItlSlotData *slot = slot_at(page, 0);
		UndoRecordHeader header = { 0 };
		UndoItlPayload payload = { 0 };
		ClusterUndoTTSlotRef ref;
		uint16 before;

		slot->flags = leg == 0 ? ITL_FLAG_LOCK_ONLY_ACTIVE : ITL_FLAG_LOCK_ONLY_COMMITTED;
		slot->xid = 700;
		slot->wrap = 7;
		slot->undo_segment_head = uba_encode(257, 1, 7, 1);
		if (leg == 1)
			tuple->t_infomask |= HEAP_XMAX_IS_MULTI;
		if (leg == 2)
			tuple->t_infomask &= ~(HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_EXCL_LOCK);
		if (leg == 3)
			HeapTupleHeaderSetXmax(tuple, 701);
		if (leg == 4)
			*slot_at(page, 1) = *slot;
		before = tuple->t_infomask;
		payload.prev_xmax = HeapTupleHeaderGetRawXmax(tuple);
		payload.prev_infomask = before;
		UT_ASSERT(cluster_heap_capture_undo_prior_lock(page, UNDO_RECORD_ITL, 0, 0, slot, &payload,
													   sizeof(payload)));
		UT_ASSERT_EQ(payload.prev_infomask, before);
		if (leg == 0) {
			header.target_offset = FirstOffsetNumber;
			UT_ASSERT(cluster_cr_apply_itl_inverse((char *)page, &header, &payload));
			UT_ASSERT_EQ(slot->flags, ITL_FLAG_LOCK_ONLY_ACTIVE);
			UT_ASSERT(cluster_itl_find_lock_tt_ref_by_xmax(page, 700, &ref));
			UT_ASSERT_EQ(ref.local_xid, 700);
		}
	}
}

UT_TEST(full_tuple_undo_copy_does_not_resurrect_terminal_lock)
{
	Page page = build_itl_page();
	HeapTupleHeader tuple = append_plain_lock_tuple(page, 700);
	ClusterItlSlotData *slot = slot_at(page, 0);
	UndoRecordHeader header = { 0 };
	char payload[128] pg_attribute_aligned(MAXIMUM_ALIGNOF);
	int leg;

	slot->flags = ITL_FLAG_LOCK_ONLY_COMMITTED;
	slot->xid = 700;
	slot->wrap = 7;
	slot->undo_segment_head = uba_encode(257, 1, 7, 1);
	for (leg = 0; leg < 2; leg++) {
		uint8 type = leg == 0 ? UNDO_RECORD_UPDATE : UNDO_RECORD_DELETE;
		Size offset = leg == 0 ? sizeof(UndoUpdatePayload) : sizeof(UndoDeletePayload);
		HeapTupleHeader copy = (HeapTupleHeader)(payload + offset);

		memset(payload, 0, sizeof(payload));
		memcpy(copy, tuple, SizeofHeapTupleHeader);
		UT_ASSERT(cluster_heap_capture_undo_prior_lock(page, type, 0, 0, slot, payload,
													   offset + SizeofHeapTupleHeader));
		UT_ASSERT((copy->t_infomask & HEAP_XMAX_INVALID) != 0);
		UT_ASSERT((tuple->t_infomask & HEAP_XMAX_INVALID) == 0);
		if (leg == 1) {
			header.target_offset = FirstOffsetNumber;
			UT_ASSERT(cluster_cr_apply_delete_inverse((char *)page, &header,
													  (UndoDeletePayload *)payload, (char *)copy,
													  SizeofHeapTupleHeader));
			UT_ASSERT((tuple->t_infomask & HEAP_XMAX_INVALID) != 0);
		}
	}
}

UT_TEST(test_retained_history_stamp_does_not_erase_or_advance_loss_watermark)
{
	Page page = build_itl_page();
	ClusterItlSlotData *slot = slot_at(page, 0);

	slot->xid = 100;
	slot->flags = ITL_FLAG_COMMITTED;
	slot->wrap = 7;
	slot->write_scn = 500;
	slot->commit_scn = 600;
	slot->undo_segment_head = uba_encode(1, 7, 0, 0);
	ClusterPageGetItlHeader(page)->itl_recycle_watermark_scn = 300;
	cluster_itl_stamp_active_with_history(marker_buffer_for(page), 0, 101, 700,
										  uba_encode(1, 7, 1, 0));
	UT_ASSERT_EQ(ClusterPageGetItlHeader(page)->itl_recycle_watermark_scn, 300);
	UT_ASSERT_EQ(slot->wrap, 8);
	UT_ASSERT_EQ(slot->xid, 101);
}

UT_TEST(test_retained_history_v4_redo_matches_primary_without_fpi)
{
	for (int variant = 0; variant < 2; variant++) {
		Page page = build_itl_page();
		ClusterItlSlotData *slot = slot_at(page, 0);
		PGAlignedBlock before;
		PGAlignedBlock primary;
		xl_heap_itl_delta_block *header = (xl_heap_itl_delta_block *)redo_delta_buf;
		xl_heap_itl_delta_v3 *delta = (xl_heap_itl_delta_v3 *)(redo_delta_buf + 8);

		slot->xid = variant == 0 ? 100 : 101;
		slot->flags = variant == 0 ? ITL_FLAG_COMMITTED : ITL_FLAG_LOCK_ONLY_ACTIVE;
		slot->wrap = 7;
		slot->write_scn = 500;
		slot->commit_scn = variant == 0 ? 600 : InvalidScn;
		slot->undo_segment_head = uba_encode(1, 7, 0, 0);
		ClusterPageGetItlHeader(page)->itl_recycle_watermark_scn = 300;
		memcpy(before.data, page, BLCKSZ);
		cluster_itl_stamp_active_with_history(marker_buffer_for(page), 0, 101, 700,
											  uba_encode(1, 7, 1, 0));
		memcpy(primary.data, page, BLCKSZ);
		memcpy(page, before.data, BLCKSZ);
		memset(redo_delta_buf, 0, sizeof(redo_delta_buf));
		header->ndeltas = 1;
		header->format_version = CLUSTER_ITL_DELTA_FORMAT_V4;
		delta->slot_idx = 0;
		delta->flags_after = ITL_FLAG_ACTIVE;
		delta->xid = 101;
		delta->write_scn = 700;
		delta->undo_segment_head = uba_encode(1, 7, 1, 0);
		if (sigsetjmp(ereport_recover_jmp, 1) == 0) {
			UT_ASSERT_EQ(cluster_itl_redo_apply_block_local_delta(page, NULL, redo_delta_buf), 40);
			UT_ASSERT_EQ(cluster_itl_wal_block_consumed_bytes(redo_delta_buf), 40);
			UT_ASSERT_EQ(memcmp(page, primary.data, BLCKSZ), 0);
			UT_ASSERT_EQ(cluster_itl_redo_apply_block_local_delta(page, NULL, redo_delta_buf), 40);
			UT_ASSERT_EQ(memcmp(page, primary.data, BLCKSZ), 0);
		} else
			UT_ASSERT(false);
	}
}

UT_TEST(test_retained_history_v4_rejects_complete_array_before_mutation)
{
	int variant;

	for (variant = 0; variant < 9; variant++) {
		Page page = build_itl_page();
		PGAlignedBlock before;
		xl_heap_itl_delta_block *header = (xl_heap_itl_delta_block *)redo_delta_buf;
		xl_heap_itl_delta_v3 *delta = (xl_heap_itl_delta_v3 *)(redo_delta_buf + 8);
		volatile bool rejected = false;

		memset(redo_delta_buf, 0, sizeof(redo_delta_buf));
		header->format_version = CLUSTER_ITL_DELTA_FORMAT_V4;
		header->ndeltas = 2;
		delta[0].slot_idx = 0;
		delta[0].flags_after = ITL_FLAG_ACTIVE;
		delta[0].xid = 101;
		delta[0].write_scn = 700;
		delta[0].undo_segment_head = uba_encode(1, 7, 1, 0);
		delta[1] = delta[0];
		delta[1].slot_idx = 1;
		switch (variant) {
		case 0:
			delta[1].slot_idx = 0;
			break;
		case 1:
			delta[1].slot_idx = 8;
			break;
		case 2:
			delta[1].xid = InvalidTransactionId;
			break;
		case 3:
			delta[1].write_scn = InvalidScn;
			break;
		case 4:
			delta[1].flags_after = ITL_FLAG_COMMITTED;
			break;
		case 5:
			delta[1].undo_segment_head = uba_encode(1, 0, 1, 0);
			break;
		case 6:
			header->reserved = 1;
			break;
		case 7:
			header->ndeltas = 9;
			break;
		case 8:
			((PageHeader)page)->pd_flags &= ~PD_HAS_ITL;
			break;
		}
		memcpy(before.data, page, BLCKSZ);
		if (sigsetjmp(ereport_recover_jmp, 1) == 0)
			(void)cluster_itl_redo_apply_block_local_delta(page, NULL, redo_delta_buf);
		else
			rejected = true;
		UT_ASSERT(rejected);
		UT_ASSERT_EQ(memcmp(before.data, page, BLCKSZ), 0);
	}
}

UT_TEST(test_retained_lock_history_v4_redo_matches_primary)
{
	Page page = build_itl_page();
	ClusterItlSlotData *slot = slot_at(page, 0);
	PGAlignedBlock before, primary;
	xl_heap_itl_delta_block *header = (xl_heap_itl_delta_block *)redo_delta_buf;
	xl_heap_itl_delta_v3 *delta = (xl_heap_itl_delta_v3 *)(redo_delta_buf + 8);

	slot->xid = 100;
	slot->flags = ITL_FLAG_COMMITTED;
	slot->wrap = 7;
	slot->write_scn = 500;
	slot->commit_scn = 600;
	slot->lock_count = 3;
	slot->first_change_lsn = 123;
	slot->undo_segment_head = uba_encode(1, 7, 0, 0);
	ClusterPageGetItlHeader(page)->itl_recycle_watermark_scn = 300;
	memcpy(before.data, page, BLCKSZ);
	cluster_itl_stamp_lock_active_with_history(marker_buffer_for(page), 0, 101, 700,
											   uba_encode(1, 7, 1, 0));
	memcpy(primary.data, page, BLCKSZ);
	memcpy(page, before.data, BLCKSZ);
	memset(redo_delta_buf, 0, sizeof(redo_delta_buf));
	header->ndeltas = 1;
	header->format_version = CLUSTER_ITL_DELTA_FORMAT_V4;
	delta->slot_idx = 0;
	delta->flags_after = ITL_FLAG_LOCK_ONLY_ACTIVE;
	delta->xid = 101;
	delta->write_scn = 700;
	delta->undo_segment_head = uba_encode(1, 7, 1, 0);
	if (sigsetjmp(ereport_recover_jmp, 1) == 0) {
		UT_ASSERT_EQ(cluster_itl_redo_apply_block_local_delta(page, NULL, redo_delta_buf), 40);
		UT_ASSERT_EQ(memcmp(primary.data, page, BLCKSZ), 0);
		UT_ASSERT_EQ(cluster_itl_redo_apply_block_local_delta(page, NULL, redo_delta_buf), 40);
		UT_ASSERT_EQ(memcmp(primary.data, page, BLCKSZ), 0);
	} else
		UT_ASSERT(false);
}

int
main(void)
{
	UT_PLAN(72);
	UT_RUN(update_own_predecessor_lock_does_not_need_a_ninth_slot);
	UT_RUN(update_handoff_refuses_unproved_or_still_referenced_lock_slot);
	UT_RUN(update_handoff_keeps_ordinary_data_allocation_priority);
	UT_RUN(test_retained_lock_history_v4_redo_matches_primary);
	UT_RUN(test_retained_history_v4_rejects_complete_array_before_mutation);
	UT_RUN(test_retained_history_stamp_does_not_erase_or_advance_loss_watermark);
	UT_RUN(test_retained_history_v4_redo_matches_primary_without_fpi);
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
	UT_RUN(test_t17a_has_cached_status_true_for_needs_cleanout_valid_scn);
	UT_RUN(test_t18_has_cached_status_false_for_active);
	UT_RUN(test_t19_lock_scan_ignores_data_active_same_xid);
	UT_RUN(test_t20_lock_scan_rejects_ambiguous_same_wrap);
	UT_RUN(test_t21_lock_scan_chooses_highest_wrap);
	UT_RUN(test_t21a_data_scan_ignores_lock_and_marker_same_xid);
	UT_RUN(test_t21b_data_scan_rejects_ambiguous_highest_wrap);
	UT_RUN(test_t21c_data_scan_chooses_unique_highest_wrap);
	UT_RUN(test_t21d_data_slot_index_returns_historical_creator);
	UT_RUN(test_t37_lock_slot_index_null_page_sets_sentinel);
	UT_RUN(test_t38_lock_slot_index_miss_sets_sentinel);
	UT_RUN(test_t39_lock_slot_index_unique_winner);
	UT_RUN(test_t40_lock_slot_index_chooses_highest_wrap);
	UT_RUN(test_t41_lock_slot_index_equal_winning_wrap_is_ambiguous);
	UT_RUN(test_t42_lock_slot_index_lower_wrap_duplicate_does_not_hide_winner);
	UT_RUN(test_t43_lock_slot_index_ignores_data_and_invalid_uba);
	UT_RUN(test_t44_lock_slot_index_and_old_reader_select_same_slot);
	UT_RUN(test_t45_lock_slot_index_invalid_xid_sets_sentinel);
	UT_RUN(test_t46_lock_slot_index_page_without_itl_sets_sentinel);
	UT_RUN(test_t22_redo_parity_sets_pd_block_scn_on_fresh_page);
	UT_RUN(test_t23_redo_parity_monotonic_older_write_scn_noop);
	UT_RUN(test_t24_redo_parity_invalid_write_scn_noop);
	UT_RUN(test_t25_redo_parity_newer_write_scn_advances);
	/* spec-3.10 §v0.5 slot-reuse fail-closed watermark. */
	UT_RUN(test_t26_watermark_free_slot_no_contribution);
	UT_RUN(test_t27_watermark_same_xid_no_contribution);
	UT_RUN(test_t28_watermark_lock_only_no_contribution);
	UT_RUN(test_t29_watermark_active_diff_xid_no_contribution);
	UT_RUN(test_t30_watermark_completed_data_contributes);
	UT_RUN(test_t31_watermark_advance_monotone);
	UT_RUN(test_t32_redo_recomputes_recycle_watermark);
	/* spec-3.6b: lossy markers preserve completed DATA anchors. */
	UT_RUN(test_t33_marker_full_page_preserves_completed_data_anchor);
	UT_RUN(test_t34_marker_reuse_lock_only_preserves_loss_bound);
	UT_RUN(test_t35_marker_free_slot_no_fold);
	UT_RUN(test_t36_marker_xid_mxid_collision_preserves_data_anchor);
	UT_RUN(test_marker_recasts_stale_marker_before_free_slot);
	UT_RUN(test_marker_same_mxid_is_idempotent);
	UT_RUN(test_marker_collapses_legacy_stale_accumulation);
	UT_RUN(test_d11_page_has_active_slot_detects_active);
	UT_RUN(test_d11_page_has_active_slot_no_itl_is_false);
	UT_RUN(test_precommit_cleanout_evidence_is_not_directly_reusable);
	UT_RUN(completed_lock_reuse_normalizes_only_matching_plain_locks);
	UT_RUN(active_own_lock_to_data_keeps_unreleased_locks);
	UT_RUN(replacement_redo_normalizes_old_active_lock_and_is_idempotent);
	UT_RUN(marker_reuse_normalizes_retired_plain_lock);
	UT_RUN(undo_capture_and_real_inverse_do_not_resurrect_retired_plain_lock);
	UT_RUN(retired_older_or_ambiguous_slot_cannot_release_newer_locker);
	UT_RUN(undo_active_and_unproved_lock_headers_are_not_normalized);
	UT_RUN(full_tuple_undo_copy_does_not_resurrect_terminal_lock);

	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
