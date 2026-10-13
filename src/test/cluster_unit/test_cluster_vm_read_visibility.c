/*-------------------------------------------------------------------------
 *
 * test_cluster_vm_read_visibility.c
 *    Keep a disk VM positive bit separate from index tuple visibility.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_vm_read_visibility.c
 *
 * NOTES
 *    The raw reader and index-only consumer are unchanged product functions.
 *    I/O exposes an older disk image while another owner has cleared current.
 *    This is deterministic branch coverage, not a cross-node SQL result.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/relscan.h"
#include "access/visibilitymap.h"
#include "cluster/cluster_space_storage.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/smgr.h"
#include "utils/rel.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

static PGAlignedBlock disk_vm, current_vm, reader_vm;
static SMgrRelationData fixture_smgr;
static bool fixture_shared, fixture_visible;
static unsigned fixture_reads, fixture_grants, fixture_fetches, fixture_index_calls;
static ItemPointerData fixture_tid;

#define MAPSIZE (BLCKSZ - MAXALIGN(SizeOfPageHeaderData))
#define HEAPBLOCKS_PER_PAGE (MAPSIZE * 4)
#define HEAPBLK_TO_MAPBLOCK(x) ((x) / HEAPBLOCKS_PER_PAGE)
#define HEAPBLK_TO_MAPBYTE(x) (((x) % HEAPBLOCKS_PER_PAGE) / 4)
#define HEAPBLK_TO_OFFSET(x) (((x) % 4) * 2)
#define cluster_storage_mode_enabled() fixture_shared
#define cluster_shared_config fixture_shared
#define cluster_smgr_which_for(...) 1
#define cluster_space_relation_get_identity(rel, identity) ((void)(rel), (void)(identity), false)
#define cluster_space_init_vm_buffer_wal(identity, buf) ((void)(identity), (void)(buf), false)
#define RelationGetSmgr(rel) ((void)(rel), &fixture_smgr)
#undef BufferIsValid
#define BufferIsValid(buf) ((buf) != InvalidBuffer)
#undef BufferGetPage
#define BufferGetPage(buf) ((void)(buf), (Page)reader_vm.data)
#define BufferGetBlockNumber(buf) ((void)(buf), 0)
#define vm_extend(...) (abort(), InvalidBuffer)
#define smgrexists(...) true
#define smgrnblocks(...) (abort(), 0)
#define LockBufferForVisibilityMapPageInit(buf) (abort(), (buf))
#define PageInit(...) abort()
#undef elog
#define elog(...) abort()
#undef ereport
#define ereport(...) abort()
#undef CHECK_FOR_INTERRUPTS
#define CHECK_FOR_INTERRUPTS() ((void)0)
#define index_beginscan(...) (abort(), (IndexScanDesc)NULL)
#define index_rescan(...) abort()
#define ExecQualAndReset(...) (abort(), false)
#define PredicateLockPage(...) ((void)0)
#define StoreIndexTuple(node, slot, tuple, desc) ((slot)->tts_flags &= ~TTS_FLAG_EMPTY)
#define ExecForceStoreHeapTuple(tuple, slot, free_tuple) ((slot)->tts_flags &= ~TTS_FLAG_EMPTY)
#define ExecClearTuple(slot) ((slot)->tts_flags |= TTS_FLAG_EMPTY, (slot))

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	(void)condition;
	(void)file;
	(void)line;
	abort();
}

Buffer
ReadBufferExtended(Relation rel, ForkNumber fork, BlockNumber block,
				   ReadBufferMode mode, BufferAccessStrategy strategy)
{
	(void)rel;
	(void)strategy;
	UT_ASSERT_EQ(fork, VISIBILITYMAP_FORKNUM);
	UT_ASSERT_EQ(block, 0);
	UT_ASSERT_EQ(mode, fixture_shared ? RBM_NORMAL : RBM_ZERO_ON_ERROR);
	fixture_reads++;
	reader_vm = disk_vm;
	return 1;
}

void
ReleaseBuffer(Buffer buffer)
{
	(void)buffer;
}

void
LockBuffer(Buffer buffer, int mode)
{
	(void)buffer;
	(void)mode;
	fixture_grants++;
}

ItemPointer
index_getnext_tid(IndexScanDesc scan, ScanDirection direction)
{
	(void)scan;
	(void)direction;
	return fixture_index_calls++ == 0 ? &fixture_tid : NULL;
}

bool
index_fetch_heap(IndexScanDesc scan, TupleTableSlot *slot)
{
	(void)scan;
	(void)slot;
	fixture_fetches++;
	return fixture_visible;
}

#include "test_cluster_vm_read_visibility.inc"

static void
check_read(bool shared, bool visible)
{
	RelationData relation = {0};
	FormData_pg_class form = {0};
	IndexOnlyScanState node = {0};
	IndexOnlyScan plan = {0};
	EState estate = {0};
	IndexScanDescData scan = {0};
	TupleTableSlot slot = {0};
	TupleTableSlot heap_slot = {0};
	IndexTupleData tuple = {0};
	Buffer observer = InvalidBuffer;

	fixture_shared = shared;
	fixture_visible = visible;
	fixture_reads = fixture_grants = fixture_fetches = fixture_index_calls = 0;
	memset(&disk_vm, 0, sizeof(disk_vm));
	memset(&current_vm, 0, sizeof(current_vm));
	memset(&reader_vm, 0, sizeof(reader_vm));
	((PageHeader)disk_vm.data)->pd_lower = SizeOfPageHeaderData;
	((PageHeader)disk_vm.data)->pd_upper = BLCKSZ;
	((PageHeader)current_vm.data)->pd_lower = SizeOfPageHeaderData;
	((PageHeader)current_vm.data)->pd_upper = BLCKSZ;
	PageGetContents(disk_vm.data)[0] = VISIBILITYMAP_ALL_VISIBLE;
	fixture_smgr.smgr_cached_nblocks[VISIBILITYMAP_FORKNUM] = 1;
	relation.rd_rel = &form;
	form.relpersistence = RELPERSISTENCE_PERMANENT;
	ItemPointerSet(&fixture_tid, 0, 1);
	estate.es_direction = ForwardScanDirection;
	plan.indexorderdir = ForwardScanDirection;
	node.ss.ps.state = &estate;
	node.ss.ps.plan = (Plan *)&plan;
	node.ss.ss_currentRelation = &relation;
	node.ss.ss_ScanTupleSlot = &slot;
	node.ioss_TableSlot = &heap_slot;
	node.ioss_ScanDesc = &scan;
	node.ioss_VMBuffer = InvalidBuffer;
	scan.heapRelation = &relation;
	scan.xs_itup = &tuple;
	slot.tts_flags = TTS_FLAG_EMPTY;

	/* Raw diagnostic stays truthful even after semantic consumers distrust it. */
	UT_ASSERT_EQ(visibilitymap_get_status(&relation, 0, &observer), VISIBILITYMAP_ALL_VISIBLE);
	UT_ASSERT_EQ(fixture_reads, 1);
	UT_ASSERT_EQ(fixture_grants, 0);
	UT_ASSERT_EQ(PageGetContents(current_vm.data)[0], 0);
	ReleaseBuffer(observer);
	UT_ASSERT(IndexOnlyNext(&node) == &slot);
	UT_ASSERT_EQ(PageGetContents(disk_vm.data)[0], VISIBILITYMAP_ALL_VISIBLE);
	UT_ASSERT_EQ(fixture_grants, 0);
	if (shared)
	{
		UT_ASSERT_EQ(fixture_fetches, 1);
		UT_ASSERT_EQ(TTS_EMPTY(&slot), !visible);
	}
	else
	{
		UT_ASSERT_EQ(fixture_fetches, 0);
		UT_ASSERT(!TTS_EMPTY(&slot));
	}
}

UT_TEST(shared_stale_vm_does_not_expose_uncommitted_tid)
{
	check_read(true, false);
}

UT_TEST(shared_stale_vm_keeps_precisely_visible_tuple)
{
	check_read(true, true);
}

UT_TEST(native_vm_shortcut_is_unchanged)
{
	check_read(false, true);
}

UT_TEST(all_other_consumers_ignore_shared_positive_bits)
{
	RelationData rel = {0};
	FormData_pg_class form = {0};
	HeapScanDescData heap = {0};
	SnapshotData snap = {0};
	BitmapHeapScanState bitmap = {0};
	TBMIterateResult item = {0};
	PGAlignedBlock page = {0};

	rel.rd_rel = &form;
	heap.rs_base.rs_rd = &rel;
	heap.rs_base.rs_snapshot = &snap;
	bitmap.ss.ss_currentRelation = &rel;
	bitmap.can_skip_fetch = true;
	bitmap.tbmres = &item;
	bitmap.vmbuffer = 1;
	PageSetAllVisible(page.data);
	/* Real raw reader above already proved the disk-positive/current-clear gap. */
	PageGetContents(reader_vm.data)[0] = VISIBILITYMAP_ALL_VISIBLE;
	for (int mode = 0; mode < 3; mode++)
	{
		bool restricted = mode == 1;
		fixture_shared = mode != 0;
		form.relpersistence = mode == 2 ? RELPERSISTENCE_TEMP : RELPERSISTENCE_PERMANENT;
		UT_ASSERT_EQ(fixture_pagemode(&heap, page.data, &snap), !restricted);
		UT_ASSERT_EQ(fixture_sample(&heap.rs_base, page.data), !restricted);
		UT_ASSERT_EQ(fixture_planner_fetch(&rel, 0, 1), restricted);
		UT_ASSERT_EQ(fixture_statapprox_skip(&rel, 0, 1), !restricted);
		for (int recheck = 0; recheck <= 1; recheck++)
		{
			item.recheck = recheck != 0;
			UT_ASSERT_EQ(fixture_BitmapHeapNext0(&bitmap, &item, &item), !restricted && !recheck);
			UT_ASSERT_EQ(fixture_BitmapPrefetch0(&bitmap, &item, &item), !restricted && !recheck);
			UT_ASSERT_EQ(fixture_BitmapPrefetch1(&bitmap, &item, &item), !restricted && !recheck);
		}
		item.recheck = false;
	}
	UT_ASSERT(PageIsAllVisible(page.data));
	UT_ASSERT_EQ(PageGetContents(reader_vm.data)[0], VISIBILITYMAP_ALL_VISIBLE);
}

int
main(void)
{
	UT_PLAN(4);
	UT_RUN(shared_stale_vm_does_not_expose_uncommitted_tid);
	UT_RUN(shared_stale_vm_keeps_precisely_visible_tuple);
	UT_RUN(native_vm_shortcut_is_unchanged);
	UT_RUN(all_other_consumers_ignore_shared_positive_bits);
	UT_DONE();
	return ut_failed_count != 0;
}
