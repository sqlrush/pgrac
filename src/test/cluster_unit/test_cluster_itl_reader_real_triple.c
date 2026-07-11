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
 *	    T19  lock-only raw_xmax scan ignores data ACTIVE slot for same xid
 *	    T20  lock-only raw_xmax scan rejects ambiguous duplicate same wrap
 *	    T21  lock-only raw_xmax scan chooses highest-wrap unique match
 *	    T22  spec-3.9 L213 redo parity: fresh page gets pd_block_scn = write_scn
 *	    T23  spec-3.9 L213 redo parity: older write_scn is a monotonic no-op
 *	    T24  spec-3.9 L213 redo parity: InvalidScn write_scn (lock-only) no-op
 *	    T25  spec-3.9 L213 redo parity: newer write_scn advances the watermark
 *	    T33  spec-7.1 watch-2: marker reuse of a completed DATA slot folds
 *	         the evicted write_scn into the recycle watermark
 *	    T34  spec-7.1 watch-2: marker reuse of a completed lock-only slot
 *	         contributes nothing
 *	    T35  spec-7.1 watch-2: marker into a FREE slot contributes nothing
 *	    T36  spec-7.1 watch-2: numeric xid/multixact-id collision must not
 *	         suppress the fold (new_xid = InvalidTransactionId)
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
#include "access/transam.h"
#include "cluster/cluster_itl.h"
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
	/* E6: completed lock-only slots never anchor a visibility-affecting undo. */
	UT_ASSERT_EQ(
		(int)SCN_VALID(cluster_itl_recycle_watermark_contribution(
			ITL_FLAG_LOCK_ONLY_COMMITTED, (TransactionId)100, (SCN)5000, (TransactionId)200)),
		0);
	UT_ASSERT_EQ(
		(int)SCN_VALID(cluster_itl_recycle_watermark_contribution(
			ITL_FLAG_LOCK_ONLY_ABORTED, (TransactionId)100, (SCN)5000, (TransactionId)200)),
		0);
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
 *	spec-7.1 watch-2 (spec-3.10 §v0.5 parity):  a MultiXact marker
 *	stamp that evicts a completed DATA slot drops that slot's undo
 *	anchor from the per-page CR candidate set exactly like a
 *	data-writer recycle, so it must fold the evicted write_scn into
 *	the recycle watermark (otherwise own-instance CR can silently
 *	reconstruct a post-snapshot version -- false-visible).  T33-T36
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

UT_TEST(test_t33_marker_reuse_completed_data_folds_watermark)
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
	UT_ASSERT_EQ((int)idx, 5);
	/* watch-2: the evicted COMMITTED writer's write_scn reached the
	 * watermark BEFORE the marker overwrote the slot. */
	UT_ASSERT_EQ((int)(ClusterPageGetItlHeader(page)->itl_recycle_watermark_scn == (SCN)5000), 1);
	UT_ASSERT_EQ((int)(slot_at(page, 5)->flags == ITL_FLAG_LOCK_ONLY_XMAX_IS_MULTI), 1);
	UT_ASSERT_EQ((int)slot_at(page, 5)->wrap, 8);
}

UT_TEST(test_t34_marker_reuse_lock_only_no_fold)
{
	Page page = build_itl_page();
	Buffer buf = marker_buffer_for(page);
	uint8 i;
	uint8 idx;

	for (i = 0; i < CLUSTER_ITL_INITRANS_DEFAULT; i++) {
		slot_at(page, i)->flags = ITL_FLAG_ACTIVE;
		slot_at(page, i)->xid = (TransactionId)(1000 + i);
	}
	/* Completed lock-only slot: reusable, but it anchors no tuple versions
	 * (§v0.5 B2/Q1) -- must NOT contribute.  Poison write_scn proves the
	 * flag predicate, not the SCN_VALID guard, rejects it. */
	slot_at(page, 3)->flags = ITL_FLAG_LOCK_ONLY_COMMITTED;
	slot_at(page, 3)->xid = (TransactionId)100;
	slot_at(page, 3)->write_scn = (SCN)5000;
	ClusterPageGetItlHeader(page)->itl_recycle_watermark_scn = InvalidScn;

	idx = cluster_itl_stamp_multixact_marker(buf, (MultiXactId)4242);
	UT_ASSERT_EQ((int)idx, 3);
	UT_ASSERT_EQ((int)SCN_VALID(ClusterPageGetItlHeader(page)->itl_recycle_watermark_scn), 0);
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

UT_TEST(test_t36_marker_reuse_xid_mxid_collision_still_folds)
{
	Page page = build_itl_page();
	Buffer buf = marker_buffer_for(page);
	uint8 i;
	uint8 idx;

	/* 8.A pin: the evicted COMMITTED xid numerically equals the MultiXactId
	 * being stamped (xid and multixact-id are separate counters and can
	 * collide in value).  The contribution helper's same-xid reuse exemption
	 * must NOT fire -- the marker path passes InvalidTransactionId as
	 * new_xid, never the multixact id. */
	for (i = 0; i < CLUSTER_ITL_INITRANS_DEFAULT; i++) {
		slot_at(page, i)->flags = ITL_FLAG_ACTIVE;
		slot_at(page, i)->xid = (TransactionId)(1000 + i);
	}
	slot_at(page, 2)->flags = ITL_FLAG_COMMITTED;
	slot_at(page, 2)->xid = (TransactionId)4242;
	slot_at(page, 2)->write_scn = (SCN)6000;
	ClusterPageGetItlHeader(page)->itl_recycle_watermark_scn = InvalidScn;

	idx = cluster_itl_stamp_multixact_marker(buf, (MultiXactId)4242);
	UT_ASSERT_EQ((int)idx, 2);
	UT_ASSERT_EQ((int)(ClusterPageGetItlHeader(page)->itl_recycle_watermark_scn == (SCN)6000), 1);
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
	UT_RUN(test_t19_lock_scan_ignores_data_active_same_xid);
	UT_RUN(test_t20_lock_scan_rejects_ambiguous_same_wrap);
	UT_RUN(test_t21_lock_scan_chooses_highest_wrap);
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
	/* spec-7.1 watch-2: marker reuse folds the recycle watermark. */
	UT_RUN(test_t33_marker_reuse_completed_data_folds_watermark);
	UT_RUN(test_t34_marker_reuse_lock_only_no_fold);
	UT_RUN(test_t35_marker_free_slot_no_fold);
	UT_RUN(test_t36_marker_reuse_xid_mxid_collision_still_folds);
	UT_RUN(test_d11_page_has_active_slot_detects_active);
	UT_RUN(test_d11_page_has_active_slot_no_itl_is_false);

	return ut_failed_count == 0 ? 0 : 1;
}
