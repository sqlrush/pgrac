/*-------------------------------------------------------------------------
 *
 * test_cluster_itl_finish_wal.c
 *	  pgrac spec-3.26 D7 — byte-equivalence unit tests for the bespoke
 *	  RM_CLUSTER_ITL ITL xact-finish WAL record.
 *
 *	  The bespoke emit (cluster_itl_touch.c) stamps the touched ITL slots via
 *	  itl_finish_stamp_page() and WAL-logs a v1 xl_heap_itl_delta built from the
 *	  post-stamp slot state.  Redo (cluster_itl_redo / online block recovery)
 *	  replays it through the shared helper cluster_itl_redo_apply_block_local_delta
 *	  (htup = NULL).  These tests prove the rule-8.A invariant: a replayed page is
 *	  BYTE-IDENTICAL to the primary's itl_finish_stamp_page() result -- the same
 *	  page the legacy GenericXLog redo produced.  A divergence here would be a
 *	  false ITL state on a crash-recovered / standby page (false-visible).
 *
 *	  Tests:
 *	    T1  COMMITTED single slot: redo == stamp (whole page byte-equal)
 *	    T2  ABORTED single slot:   redo == stamp
 *	    T3  LOCK_ONLY commit:      redo == stamp (lock-only flag transition)
 *	    T4  UBA preservation:      v1 delta leaves the on-page UBA intact
 *	    T5  multi-slot one block:  redo == stamp for every slot
 *	    T6  v1 delta block layout:  format_version + ndeltas + consumed bytes
 *
 *	  Standalone; links cluster_itl.o for the shared apply helper, stubbing the
 *	  PG core / shmem symbols it transitively references (mirrors the proven
 *	  test_cluster_itl_reader_real_triple.c fixture).
 *
 * Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_itl_finish_wal.c
 *
 * Spec: spec-3.26-single-node-write-tax-cpu-closure.md
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include <setjmp.h>
#include <stdlib.h>
#include <string.h>

#include "access/heapam_xlog.h"
#include "access/transam.h"
#include "cluster/cluster_itl.h"
#include "cluster/cluster_itl_slot.h"
#include "cluster/cluster_scn.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_uba.h"
#include "miscadmin.h"
#include "storage/bufpage.h"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"


UT_DEFINE_GLOBALS();


/* ============================================================
 *	Link stubs for cluster_itl.o (mirrors the reader fixture).
 * ============================================================ */
static sigjmp_buf ereport_recover_jmp;
static int ereport_raised_count = 0;

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
errcode(int sqlerrcode pg_attribute_unused())
{
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
int
errmsg_internal(const char *fmt pg_attribute_unused(), ...)
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

void
MarkBufferDirty(Buffer buf pg_attribute_unused())
{}
bool
cluster_bufmgr_block_write_permitted(Buffer buf pg_attribute_unused())
{
	return true;
}
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

Page
BufferGetPage(Buffer buf)
{
	return (Page)(uintptr_t)buf;
}
void
PageInit(Page page pg_attribute_unused(), Size pageSize pg_attribute_unused(),
		 Size specialSize pg_attribute_unused())
{}
void
PageInitHeapPage(Page page pg_attribute_unused(), Size pageSize pg_attribute_unused(),
				 Size specialSize pg_attribute_unused())
{}

char *BufferBlocks = NULL;
void *LocalBufferBlockPointers[1] = { NULL };
int NBuffers = 0;
int NLocBuffer = 0;

uint64
cluster_epoch_get_current(void)
{
	return 42;
}

/* byte-faithful local scn_time_cmp (header-only scn_local, identical to impl). */
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

bool cluster_enabled = false;
int cluster_node_id = 0;

int
GetCurrentTransactionNestLevel(void)
{
	return 1;
}

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
 *	ITL page + delta fixtures
 * ============================================================ */

static void
init_itl_page(char *page, SCN block_scn)
{
	PageHeader hdr;

	memset(page, 0, BLCKSZ);
	hdr = (PageHeader)page;
	hdr->pd_flags = PD_HAS_ITL;
	hdr->pd_special = (LocationIndex)(BLCKSZ - CLUSTER_ITL_SPECIAL_SIZE);
	hdr->pd_upper = hdr->pd_special;
	hdr->pd_lower = SizeOfPageHeaderData;
	hdr->pd_pagesize_version = BLCKSZ | PG_PAGE_LAYOUT_VERSION;
	hdr->pd_block_scn = block_scn; /* stamp_active sets this = write_scn */
}

static ClusterItlSlotData *
slot_at(char *page, uint8 idx)
{
	return &ClusterPageGetItlSlots((Page)page)[idx];
}

static void
set_active_slot(char *page, uint8 idx, uint8 flags, TransactionId xid, SCN wscn, UBA uba)
{
	ClusterItlSlotData *slot = slot_at(page, idx);

	memset(slot, 0, sizeof(*slot));
	slot->flags = flags; /* ITL_FLAG_ACTIVE or ITL_FLAG_LOCK_ONLY_ACTIVE */
	slot->xid = xid;
	slot->write_scn = wscn;
	slot->commit_scn = InvalidScn;
	slot->undo_segment_head = uba;
}

/* Reference stamp -- byte-faithful copy of itl_finish_stamp_page (static in
 * cluster_itl_touch.c).  This is the page the legacy GenericXLog redo produces. */
static void
ref_stamp(char *page, uint8 idx, bool is_commit, SCN commit_scn)
{
	ClusterItlSlotData *slot = slot_at(page, idx);
	bool is_lock_only = ITL_FLAG_IS_LOCK_ONLY(slot->flags);

	if (is_commit) {
		slot->flags = is_lock_only ? ITL_FLAG_LOCK_ONLY_COMMITTED : ITL_FLAG_COMMITTED;
		slot->commit_scn = commit_scn;
	} else {
		slot->flags = is_lock_only ? ITL_FLAG_LOCK_ONLY_ABORTED : ITL_FLAG_ABORTED;
		slot->commit_scn = InvalidScn;
	}
}

/* Build the v1 delta block from the POST-stamp slot state (mirrors the emit). */
static Size
build_v1_delta(char *buf, char *stamped_page, const uint8 *idxs, uint8 n)
{
	xl_heap_itl_delta_block *hdr = (xl_heap_itl_delta_block *)buf;
	char *deltas = buf + offsetof(xl_heap_itl_delta_block, deltas);
	uint8 i;

	hdr->ndeltas = n;
	hdr->reserved = 0;
	hdr->format_version = CLUSTER_ITL_DELTA_FORMAT_V1;
	for (i = 0; i < n; i++) {
		ClusterItlSlotData *slot = slot_at(stamped_page, idxs[i]);
		xl_heap_itl_delta d;

		d.slot_idx = idxs[i];
		d.flags_after = slot->flags;
		d.xid = slot->xid;
		d.write_scn = slot->write_scn;
		d.commit_scn = slot->commit_scn;
		memcpy(deltas + (Size)i * sizeof(d), &d, sizeof(d));
	}
	return offsetof(xl_heap_itl_delta_block, deltas) + (Size)n * sizeof(xl_heap_itl_delta);
}

/*
 * Core byte-equivalence check: given a fresh ACTIVE-slot page, finishing slot
 * `idx` (commit or abort) via the bespoke redo helper must produce a page
 * byte-identical to the reference itl_finish_stamp_page result.
 */
static void
assert_finish_byte_equal(uint8 active_flags, uint8 idx, bool is_commit, SCN wscn, SCN commit_scn,
						 UBA uba)
{
	static char pa[BLCKSZ];
	static char pb[BLCKSZ];
	char deltabuf[offsetof(xl_heap_itl_delta_block, deltas) + sizeof(xl_heap_itl_delta)];

	/* Identical ACTIVE starting state on both pages. */
	init_itl_page(pa, wscn);
	init_itl_page(pb, wscn);
	set_active_slot(pa, idx, active_flags, 1234u, wscn, uba);
	set_active_slot(pb, idx, active_flags, 1234u, wscn, uba);

	/* Pa = primary stamp (legacy GenericXLog redo equivalent). */
	ref_stamp(pa, idx, is_commit, commit_scn);

	/* Pb = bespoke redo: apply the v1 delta built from Pa's post-stamp slot. */
	build_v1_delta(deltabuf, pa, &idx, 1);
	(void)cluster_itl_redo_apply_block_local_delta((Page)pb, NULL, deltabuf);

	UT_ASSERT_EQ(memcmp(pa, pb, BLCKSZ), 0);
}


/* ============================================================
 *	Tests
 * ============================================================ */

UT_TEST(test_t1_committed_single_byte_equal)
{
	assert_finish_byte_equal(ITL_FLAG_ACTIVE, 3, /* commit */ true, scn_encode(0, 100),
							 scn_encode(0, 200), uba_encode(7, 9, 11, 13));
}

UT_TEST(test_t2_aborted_single_byte_equal)
{
	assert_finish_byte_equal(ITL_FLAG_ACTIVE, 5, /* abort */ false, scn_encode(0, 100), InvalidScn,
							 uba_encode(7, 9, 11, 13));
}

UT_TEST(test_t3_lock_only_commit_byte_equal)
{
	assert_finish_byte_equal(ITL_FLAG_LOCK_ONLY_ACTIVE, 2, /* commit */ true, scn_encode(0, 100),
							 scn_encode(0, 200), uba_encode(7, 9, 11, 13));
}

UT_TEST(test_t4_uba_preserved)
{
	static char pa[BLCKSZ];
	static char pb[BLCKSZ];
	char deltabuf[offsetof(xl_heap_itl_delta_block, deltas) + sizeof(xl_heap_itl_delta)];
	UBA uba = uba_encode(21, 22, 23, 24);
	uint8 idx = 4;

	init_itl_page(pa, scn_encode(0, 100));
	init_itl_page(pb, scn_encode(0, 100));
	set_active_slot(pa, idx, ITL_FLAG_ACTIVE, 99u, scn_encode(0, 100), uba);
	set_active_slot(pb, idx, ITL_FLAG_ACTIVE, 99u, scn_encode(0, 100), uba);

	ref_stamp(pa, idx, true, scn_encode(0, 200));
	build_v1_delta(deltabuf, pa, &idx, 1);
	(void)cluster_itl_redo_apply_block_local_delta((Page)pb, NULL, deltabuf);

	/* v1 carries InvalidUba -> on-page UBA must survive unchanged on both. */
	UT_ASSERT_EQ((int)UBA_is_invalid(slot_at(pb, idx)->undo_segment_head), 0);
	UT_ASSERT_EQ(memcmp(&slot_at(pa, idx)->undo_segment_head, &slot_at(pb, idx)->undo_segment_head,
						sizeof(UBA)),
				 0);
	UT_ASSERT_EQ(memcmp(pa, pb, BLCKSZ), 0);
}

UT_TEST(test_t5_multi_slot_byte_equal)
{
	static char pa[BLCKSZ];
	static char pb[BLCKSZ];
	char deltabuf[offsetof(xl_heap_itl_delta_block, deltas) + 3 * sizeof(xl_heap_itl_delta)];
	uint8 idxs[3] = { 1, 4, 6 };
	SCN wscn = scn_encode(0, 100);
	SCN cscn = scn_encode(0, 300);
	uint8 i;

	init_itl_page(pa, wscn);
	init_itl_page(pb, wscn);
	for (i = 0; i < 3; i++) {
		set_active_slot(pa, idxs[i], ITL_FLAG_ACTIVE, 1000u + i, wscn, uba_encode(1, 1, 1, i + 1));
		set_active_slot(pb, idxs[i], ITL_FLAG_ACTIVE, 1000u + i, wscn, uba_encode(1, 1, 1, i + 1));
		ref_stamp(pa, idxs[i], true, cscn);
	}

	build_v1_delta(deltabuf, pa, idxs, 3);
	(void)cluster_itl_redo_apply_block_local_delta((Page)pb, NULL, deltabuf);

	UT_ASSERT_EQ(memcmp(pa, pb, BLCKSZ), 0);
}

UT_TEST(test_t6_v1_delta_block_layout)
{
	static char pa[BLCKSZ];
	char deltabuf[offsetof(xl_heap_itl_delta_block, deltas) + 2 * sizeof(xl_heap_itl_delta)];
	uint8 idxs[2] = { 0, 7 };
	const xl_heap_itl_delta_block *hdr;
	Size consumed;
	SCN wscn = scn_encode(0, 100);

	init_itl_page(pa, wscn);
	set_active_slot(pa, 0, ITL_FLAG_ACTIVE, 1u, wscn, uba_encode(1, 1, 1, 1));
	set_active_slot(pa, 7, ITL_FLAG_ACTIVE, 2u, wscn, uba_encode(1, 1, 1, 2));
	ref_stamp(pa, 0, true, scn_encode(0, 200));
	ref_stamp(pa, 7, true, scn_encode(0, 200));

	consumed = build_v1_delta(deltabuf, pa, idxs, 2);
	hdr = (const xl_heap_itl_delta_block *)deltabuf;

	UT_ASSERT_EQ((int)hdr->format_version, (int)CLUSTER_ITL_DELTA_FORMAT_V1);
	UT_ASSERT_EQ((int)hdr->ndeltas, 2);
	UT_ASSERT_EQ((int)consumed,
				 (int)(offsetof(xl_heap_itl_delta_block, deltas) + 2 * sizeof(xl_heap_itl_delta)));
	/* the apply helper agrees on consumed footprint */
	UT_ASSERT_EQ((int)cluster_itl_wal_block_consumed_bytes(deltabuf), (int)consumed);
}


int
main(void)
{
	UT_RUN(test_t1_committed_single_byte_equal);
	UT_RUN(test_t2_aborted_single_byte_equal);
	UT_RUN(test_t3_lock_only_commit_byte_equal);
	UT_RUN(test_t4_uba_preserved);
	UT_RUN(test_t5_multi_slot_byte_equal);
	UT_RUN(test_t6_v1_delta_block_layout);
	return ut_failed_count == 0 ? 0 : 1;
}
