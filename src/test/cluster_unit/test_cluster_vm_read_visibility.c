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

sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;

void
pg_re_throw(void)
{
	abort();
}

/* This legacy-reader fixture deliberately has no qualified catalog. The
 * real qualified reader and publisher-to-IOS chain have a separate fixture. */
bool
visibilitymap_read_qualified_begin(Relation rel, BlockNumber block,
								  struct SnapshotData *snapshot, Buffer *buffer,
								  volatile VisibilityMapReadScope *scope, uint8 *status)
{
	(void)rel;
	(void)block;
	(void)snapshot;
	(void)buffer;
	UT_ASSERT_EQ(scope->locked_buffer, InvalidBuffer);
	*status = 0;
	return false;
}

static PGAlignedBlock disk_vm, current_vm, reader_vm;
static SMgrRelationData fixture_smgr;
static bool fixture_shared, fixture_visible;
static unsigned fixture_reads, fixture_grants, fixture_fetches, fixture_index_calls;
static unsigned fixture_unlocks, fixture_releases;
static bool fixture_pcm = true, fixture_grant_allowed = true, fixture_locked;
static RelFileLocator fixture_locator;
static ForkNumber fixture_fork = VISIBILITYMAP_FORKNUM;
static BlockNumber fixture_block;
static ItemPointerData fixture_tid;

#define MAPSIZE (BLCKSZ - MAXALIGN(SizeOfPageHeaderData))
#define HEAPBLOCKS_PER_PAGE (MAPSIZE * 4)
#define HEAPBLK_TO_MAPBLOCK(x) ((x) / HEAPBLOCKS_PER_PAGE)
#define HEAPBLK_TO_MAPBYTE(x) (((x) % HEAPBLOCKS_PER_PAGE) / 4)
#define HEAPBLK_TO_OFFSET(x) (((x) % 4) * 2)
#define cluster_storage_mode_enabled() fixture_shared
#define cluster_pcm_is_active() fixture_pcm
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
	UT_ASSERT_EQ(mode, fixture_shared && RelationIsPermanent(rel) ? RBM_NORMAL : RBM_ZERO_ON_ERROR);
	fixture_reads++;
	fixture_locator = rel->rd_locator;
	fixture_fork = fork;
	fixture_block = block;
	reader_vm = disk_vm;
	return 1;
}

void
ReleaseBuffer(Buffer buffer)
{
	(void)buffer;
	fixture_releases++;
}

void
LockBuffer(Buffer buffer, int mode)
{
	(void)buffer;
	if (mode == BUFFER_LOCK_UNLOCK) {
		UT_ASSERT(fixture_locked);
		fixture_locked = false;
		fixture_unlocks++;
	} else {
		UT_ASSERT_EQ(mode, BUFFER_LOCK_SHARE);
		UT_ASSERT(!fixture_locked);
		fixture_grants++;
		fixture_locked = true;
		reader_vm = current_vm;
	}
}

bool
ClusterLockBufferShareBarrierAware(Buffer buffer)
{
	if (!fixture_grant_allowed)
		return false;
	LockBuffer(buffer, BUFFER_LOCK_SHARE);
	return true;
}

void
BufferGetTag(Buffer buffer, RelFileLocator *locator, ForkNumber *fork, BlockNumber *block)
{
	(void)buffer;
	*locator = fixture_locator;
	*fork = fixture_fork;
	*block = fixture_block;
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

/* The legacy fixture also has no owned maintenance snapshot. */
#define ActiveSnapshotSet() false
#define GetActiveSnapshot() NULL
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

/* The parent has only the raw reader: keep that real behavior as RED input. */
#ifndef FIXTURE_HAS_COHERENT_VM_READER
static bool
fixture_raw_vm_status(Relation rel, BlockNumber block, Buffer *buffer, uint8 *status)
{
	*status = visibilitymap_get_status(rel, block, buffer);
	return true;
}
#define visibilitymap_get_status_coherent fixture_raw_vm_status
#endif

static void
setup_coherent_vm(RelationData *relation, FormData_pg_class *form)
{
	memset(relation, 0, sizeof(*relation));
	memset(form, 0, sizeof(*form));
	relation->rd_rel = form;
	relation->rd_locator = (RelFileLocator){ 1663, 123, 456 };
	form->relpersistence = RELPERSISTENCE_PERMANENT;
	fixture_shared = fixture_pcm = fixture_grant_allowed = true;
	fixture_locked = false;
	fixture_reads = fixture_grants = fixture_unlocks = fixture_releases = 0;
	fixture_locator = relation->rd_locator;
	fixture_fork = VISIBILITYMAP_FORKNUM;
	fixture_block = 0;
	fixture_smgr.smgr_cached_nblocks[VISIBILITYMAP_FORKNUM] = 1;
	memset(&disk_vm, 0, sizeof(disk_vm));
	memset(&current_vm, 0, sizeof(current_vm));
	((PageHeader)current_vm.data)->pd_lower = MAXALIGN(SizeOfPageHeaderData);
	((PageHeader)current_vm.data)->pd_upper = BLCKSZ;
	((PageHeader)current_vm.data)->pd_special = BLCKSZ;
	((PageHeader)current_vm.data)->pd_pagesize_version = BLCKSZ | PG_PAGE_LAYOUT_VERSION;
	((PageHeader)current_vm.data)->pd_block_scn = 17;
	disk_vm = reader_vm = current_vm;
	PageGetContents(disk_vm.data)[0] = VISIBILITYMAP_ALL_VISIBLE;
}

UT_TEST(coherent_vm_uses_current_clear_instead_of_disk_positive)
{
	RelationData relation;
	FormData_pg_class form;
	Buffer buffer = InvalidBuffer;
	uint8 status = 0xff;

	setup_coherent_vm(&relation, &form);
	UT_ASSERT(visibilitymap_get_status_coherent(&relation, 0, &buffer, &status));
	UT_ASSERT_EQ(status, 0);
	UT_ASSERT_EQ(fixture_reads, 1);
	UT_ASSERT_EQ(fixture_grants, 1);
	UT_ASSERT_EQ(fixture_unlocks, 1);
	UT_ASSERT(!fixture_locked);
	UT_ASSERT_EQ(PageGetContents(disk_vm.data)[0], VISIBILITYMAP_ALL_VISIBLE);
}

UT_TEST(coherent_vm_revalidates_every_reused_buffer_and_returns_positive)
{
	RelationData relation;
	FormData_pg_class form;
	Buffer buffer = InvalidBuffer;
	uint8 status;

	setup_coherent_vm(&relation, &form);
	PageGetContents(disk_vm.data)[0] = 0;
	PageGetContents(current_vm.data)[0] = VISIBILITYMAP_VALID_BITS;
	UT_ASSERT(visibilitymap_get_status_coherent(&relation, 0, &buffer, &status));
	UT_ASSERT_EQ(status, VISIBILITYMAP_VALID_BITS);
	PageGetContents(current_vm.data)[0] = 0;
	UT_ASSERT(visibilitymap_get_status_coherent(&relation, 0, &buffer, &status));
	UT_ASSERT_EQ(status, 0);
	PageGetContents(current_vm.data)[0] = VISIBILITYMAP_ALL_VISIBLE << 2;
	UT_ASSERT(visibilitymap_get_status_coherent(&relation, 1, &buffer, &status));
	UT_ASSERT_EQ(status, VISIBILITYMAP_ALL_VISIBLE);
	UT_ASSERT_EQ(fixture_reads, 1);
	UT_ASSERT_EQ(fixture_grants, 3);
	UT_ASSERT_EQ(fixture_unlocks, 3);
	UT_ASSERT(!fixture_locked);
}

UT_TEST(coherent_vm_rebinds_same_block_from_wrong_relation_or_fork)
{
	for (unsigned mismatch = 0; mismatch < 3; mismatch++) {
		RelationData relation;
		FormData_pg_class form;
		Buffer buffer = 1;
		uint8 status;

		setup_coherent_vm(&relation, &form);
		if (mismatch == 0)
			fixture_locator.relNumber++;
		else if (mismatch == 1)
			fixture_fork = MAIN_FORKNUM;
		else
			fixture_block = 1;
		UT_ASSERT(visibilitymap_get_status_coherent(&relation, 0, &buffer, &status));
		UT_ASSERT_EQ(status, 0);
		UT_ASSERT_EQ(fixture_releases, 1);
		UT_ASSERT_EQ(fixture_reads, 1);
		UT_ASSERT_EQ(fixture_grants, 1);
		UT_ASSERT_EQ(fixture_unlocks, 1);
	}
}

UT_TEST(coherent_vm_refusal_never_reuses_positive_output)
{
	for (unsigned refusal = 0; refusal < 5; refusal++) {
		RelationData relation;
		FormData_pg_class form;
		Buffer buffer = InvalidBuffer;
		uint8 status = VISIBILITYMAP_VALID_BITS;

		setup_coherent_vm(&relation, &form);
		PageGetContents(current_vm.data)[0] = VISIBILITYMAP_VALID_BITS;
		if (refusal == 0)
			fixture_grant_allowed = false;
		else if (refusal == 1)
			fixture_pcm = false;
		else if (refusal == 2)
			((PageHeader)current_vm.data)->pd_block_scn = 0;
		else if (refusal == 3)
			((PageHeader)current_vm.data)->pd_special--;
		else
			PageGetContents(current_vm.data)[0] = VISIBILITYMAP_ALL_FROZEN;
		UT_ASSERT(!visibilitymap_get_status_coherent(&relation, 0, &buffer, &status));
		UT_ASSERT_EQ(status, 0);
		UT_ASSERT(!fixture_locked);
		UT_ASSERT_EQ(fixture_grants, fixture_unlocks);
	}
}

UT_TEST(coherent_vm_zero_and_missing_pages_remain_read_only)
{
	RelationData relation;
	FormData_pg_class form;
	Buffer buffer = InvalidBuffer;
	uint8 status;

	setup_coherent_vm(&relation, &form);
	memset(&current_vm, 0, sizeof(current_vm));
	UT_ASSERT(visibilitymap_get_status_coherent(&relation, 0, &buffer, &status));
	UT_ASSERT_EQ(status, 0);
	PageGetContents(current_vm.data)[0] = 1;
	UT_ASSERT(!visibilitymap_get_status_coherent(&relation, 0, &buffer, &status));
	UT_ASSERT_EQ(status, 0);
	setup_coherent_vm(&relation, &form);
	buffer = InvalidBuffer;
	fixture_smgr.smgr_cached_nblocks[VISIBILITYMAP_FORKNUM] = 0;
	UT_ASSERT(visibilitymap_get_status_coherent(&relation, 0, &buffer, &status));
	UT_ASSERT_EQ(status, 0);
	UT_ASSERT_EQ(buffer, InvalidBuffer);
	UT_ASSERT_EQ(fixture_reads + fixture_grants, 0);
	setup_coherent_vm(&relation, &form);
	fixture_shared = false;
	UT_ASSERT(visibilitymap_get_status_coherent(&relation, 0, &buffer, &status));
	UT_ASSERT_EQ(status, VISIBILITYMAP_ALL_VISIBLE);
	UT_ASSERT_EQ(fixture_grants, 0);
}

UT_TEST(vm_read_scope_retains_current_image_until_end)
{
	RelationData relation;
	FormData_pg_class form;
	Buffer buffer = InvalidBuffer;
	VisibilityMapReadScope scope = {0};
	uint8 status = 0xff;

	setup_coherent_vm(&relation, &form);
	PageGetContents(disk_vm.data)[0] = 0;
	PageGetContents(current_vm.data)[0] = VISIBILITYMAP_VALID_BITS;
	UT_ASSERT(visibilitymap_read_begin(&relation, 0, &buffer, &scope, &status));
	UT_ASSERT_EQ(status, VISIBILITYMAP_VALID_BITS);
	UT_ASSERT_EQ(scope.locked_buffer, buffer);
	UT_ASSERT(fixture_locked);
	UT_ASSERT_EQ(fixture_grants, 1);
	UT_ASSERT_EQ(fixture_unlocks, 0);
	UT_ASSERT(visibilitymap_get_status_pinned(&relation, 0, buffer, &status));
	UT_ASSERT_EQ(status, VISIBILITYMAP_VALID_BITS);
	visibilitymap_read_end(&scope);
	UT_ASSERT_EQ(scope.locked_buffer, InvalidBuffer);
	UT_ASSERT(!fixture_locked);
	UT_ASSERT_EQ(buffer, 1);
	UT_ASSERT_EQ(fixture_unlocks, 1);
	UT_ASSERT_EQ(fixture_releases, 0);

	/* The next operation must revalidate the grant and observe the clear. */
	PageGetContents(current_vm.data)[0] = 0;
	UT_ASSERT(visibilitymap_read_begin(&relation, 0, &buffer, &scope, &status));
	UT_ASSERT_EQ(status, 0);
	UT_ASSERT(fixture_locked);
	UT_ASSERT_EQ(fixture_reads, 1);
	UT_ASSERT_EQ(fixture_grants, 2);
	visibilitymap_read_end(&scope);
	UT_ASSERT_EQ(fixture_unlocks, 2);
	UT_ASSERT_EQ(fixture_releases, 0);
}

UT_TEST(vm_read_scope_reuse_preserves_active_owner_and_end_is_idempotent)
{
	RelationData relation;
	FormData_pg_class form;
	Buffer buffer = InvalidBuffer;
	Buffer other = InvalidBuffer;
	VisibilityMapReadScope scope = {0};
	uint8 status;

	setup_coherent_vm(&relation, &form);
	UT_ASSERT(visibilitymap_read_begin(&relation, 0, &buffer, &scope, &status));
	status = VISIBILITYMAP_VALID_BITS;
	UT_ASSERT(!visibilitymap_read_begin(&relation, 1, &other, &scope, &status));
	UT_ASSERT_EQ(status, 0);
	UT_ASSERT_EQ(other, InvalidBuffer);
	UT_ASSERT_EQ(scope.locked_buffer, buffer);
	UT_ASSERT(fixture_locked);
	UT_ASSERT_EQ(fixture_reads, 1);
	UT_ASSERT_EQ(fixture_grants, 1);
	UT_ASSERT_EQ(fixture_unlocks, 0);
	visibilitymap_read_end(&scope);
	visibilitymap_read_end(&scope);
	visibilitymap_read_end(NULL);
	UT_ASSERT_EQ(fixture_unlocks, 1);
	UT_ASSERT_EQ(fixture_releases, 0);
	UT_ASSERT_EQ(buffer, 1);
	UT_ASSERT_EQ(scope.locked_buffer, InvalidBuffer);
	UT_ASSERT(!fixture_locked);
}

UT_TEST(vm_read_scope_rebinds_wrong_cached_tags_before_locking)
{
	for (unsigned mismatch = 0; mismatch < 3; mismatch++) {
		RelationData relation;
		FormData_pg_class form;
		Buffer buffer = 1;
		VisibilityMapReadScope scope = {0};
		uint8 status;

		setup_coherent_vm(&relation, &form);
		if (mismatch == 0)
			fixture_locator.relNumber++;
		else if (mismatch == 1)
			fixture_fork = MAIN_FORKNUM;
		else
			fixture_block++;
		UT_ASSERT(visibilitymap_read_begin(&relation, 0, &buffer, &scope, &status));
		UT_ASSERT_EQ(status, 0);
		UT_ASSERT_EQ(scope.locked_buffer, buffer);
		UT_ASSERT(fixture_locked);
		UT_ASSERT_EQ(fixture_releases, 1);
		UT_ASSERT_EQ(fixture_reads, 1);
		UT_ASSERT_EQ(fixture_grants, 1);
		UT_ASSERT_EQ(fixture_unlocks, 0);
		visibilitymap_read_end(&scope);
		UT_ASSERT(!fixture_locked);
		UT_ASSERT_EQ(fixture_unlocks, 1);
		UT_ASSERT_EQ(fixture_releases, 1);
	}
}

UT_TEST(vm_read_scope_refuses_without_positive_output_or_retained_lock)
{
	for (unsigned refusal = 0; refusal < 5; refusal++) {
		RelationData relation;
		FormData_pg_class form;
		Buffer buffer = InvalidBuffer;
		VisibilityMapReadScope scope = {0};
		uint8 status = VISIBILITYMAP_VALID_BITS;

		setup_coherent_vm(&relation, &form);
		PageGetContents(current_vm.data)[0] = VISIBILITYMAP_VALID_BITS;
		if (refusal == 0)
			fixture_grant_allowed = false;
		else if (refusal == 1)
			fixture_pcm = false;
		else if (refusal == 2)
			((PageHeader)current_vm.data)->pd_block_scn = 0;
		else if (refusal == 3)
			((PageHeader)current_vm.data)->pd_special--;
		else
			PageGetContents(current_vm.data)[0] = VISIBILITYMAP_ALL_FROZEN;
		UT_ASSERT(!visibilitymap_read_begin(&relation, 0, &buffer, &scope, &status));
		UT_ASSERT_EQ(status, 0);
		UT_ASSERT_EQ(scope.locked_buffer, InvalidBuffer);
		UT_ASSERT(!fixture_locked);
		UT_ASSERT_EQ(fixture_grants, fixture_unlocks);
		UT_ASSERT_EQ(fixture_releases, 0);
		visibilitymap_read_end(&scope);
		UT_ASSERT_EQ(fixture_grants, fixture_unlocks);
	}
}

UT_TEST(vm_read_scope_bad_arguments_do_not_acquire_or_release)
{
	for (unsigned bad = 0; bad < 5; bad++) {
		RelationData relation;
		FormData_pg_class form;
		Buffer buffer = InvalidBuffer;
		VisibilityMapReadScope scope = {0};
		uint8 status = VISIBILITYMAP_VALID_BITS;

		setup_coherent_vm(&relation, &form);
		UT_ASSERT(!visibilitymap_read_begin(bad == 0 ? NULL : &relation,
										bad == 1 ? InvalidBlockNumber : 0,
										bad == 2 ? NULL : &buffer,
										bad == 3 ? NULL : &scope,
										bad == 4 ? NULL : &status));
		UT_ASSERT_EQ(status, bad == 4 ? VISIBILITYMAP_VALID_BITS : 0);
		UT_ASSERT_EQ(scope.locked_buffer, InvalidBuffer);
		UT_ASSERT_EQ(buffer, InvalidBuffer);
		UT_ASSERT_EQ(fixture_reads + fixture_grants + fixture_releases, 0);
		visibilitymap_read_end(&scope);
		UT_ASSERT_EQ(fixture_unlocks, 0);
	}
}

UT_TEST(vm_read_scope_zero_and_missing_maps_remain_read_only)
{
	RelationData relation;
	FormData_pg_class form;
	Buffer buffer = InvalidBuffer;
	VisibilityMapReadScope scope = {0};
	uint8 status;

	setup_coherent_vm(&relation, &form);
	memset(&current_vm, 0, sizeof(current_vm));
	UT_ASSERT(visibilitymap_read_begin(&relation, 0, &buffer, &scope, &status));
	UT_ASSERT_EQ(status, 0);
	UT_ASSERT(fixture_locked);
	UT_ASSERT(PageIsNew(reader_vm.data));
	visibilitymap_read_end(&scope);
	UT_ASSERT(PageIsNew(current_vm.data));
	PageGetContents(current_vm.data)[0] = VISIBILITYMAP_ALL_VISIBLE;
	UT_ASSERT(!visibilitymap_read_begin(&relation, 0, &buffer, &scope, &status));
	UT_ASSERT_EQ(status, 0);
	UT_ASSERT_EQ(scope.locked_buffer, InvalidBuffer);
	UT_ASSERT(!fixture_locked);
	UT_ASSERT_EQ(fixture_grants, fixture_unlocks);
	UT_ASSERT_EQ(fixture_releases, 0);

	setup_coherent_vm(&relation, &form);
	buffer = InvalidBuffer;
	fixture_smgr.smgr_cached_nblocks[VISIBILITYMAP_FORKNUM] = 0;
	status = VISIBILITYMAP_VALID_BITS;
	UT_ASSERT(visibilitymap_read_begin(&relation, 0, &buffer, &scope, &status));
	UT_ASSERT_EQ(status, 0);
	UT_ASSERT_EQ(buffer, InvalidBuffer);
	UT_ASSERT_EQ(scope.locked_buffer, InvalidBuffer);
	visibilitymap_read_end(&scope);
	UT_ASSERT_EQ(fixture_reads + fixture_grants + fixture_unlocks + fixture_releases, 0);
}

UT_TEST(vm_read_scope_preserves_native_and_local_raw_behavior)
{
	for (unsigned mode = 0; mode < 2; mode++) {
		RelationData relation;
		FormData_pg_class form;
		Buffer buffer = InvalidBuffer;
		VisibilityMapReadScope scope = {0};
		uint8 status;

		setup_coherent_vm(&relation, &form);
		fixture_shared = mode != 0;
		fixture_pcm = fixture_grant_allowed = false;
		if (mode == 1)
			form.relpersistence = RELPERSISTENCE_TEMP;
		UT_ASSERT(visibilitymap_read_begin(&relation, 0, &buffer, &scope, &status));
		UT_ASSERT_EQ(status, VISIBILITYMAP_ALL_VISIBLE);
		UT_ASSERT_EQ(buffer, 1);
		UT_ASSERT_EQ(scope.locked_buffer, InvalidBuffer);
		UT_ASSERT(!fixture_locked);
		visibilitymap_read_end(&scope);
		UT_ASSERT_EQ(fixture_reads, 1);
		UT_ASSERT_EQ(fixture_grants + fixture_unlocks + fixture_releases, 0);
	}
}

int
main(void)
{
	UT_PLAN(16);
	UT_RUN(shared_stale_vm_does_not_expose_uncommitted_tid);
	UT_RUN(shared_stale_vm_keeps_precisely_visible_tuple);
	UT_RUN(native_vm_shortcut_is_unchanged);
	UT_RUN(all_other_consumers_ignore_shared_positive_bits);
	UT_RUN(coherent_vm_uses_current_clear_instead_of_disk_positive);
	UT_RUN(coherent_vm_revalidates_every_reused_buffer_and_returns_positive);
	UT_RUN(coherent_vm_rebinds_same_block_from_wrong_relation_or_fork);
	UT_RUN(coherent_vm_refusal_never_reuses_positive_output);
	UT_RUN(coherent_vm_zero_and_missing_pages_remain_read_only);
	UT_RUN(vm_read_scope_retains_current_image_until_end);
	UT_RUN(vm_read_scope_reuse_preserves_active_owner_and_end_is_idempotent);
	UT_RUN(vm_read_scope_rebinds_wrong_cached_tags_before_locking);
	UT_RUN(vm_read_scope_refuses_without_positive_output_or_retained_lock);
	UT_RUN(vm_read_scope_bad_arguments_do_not_acquire_or_release);
	UT_RUN(vm_read_scope_zero_and_missing_maps_remain_read_only);
	UT_RUN(vm_read_scope_preserves_native_and_local_raw_behavior);
	UT_DONE();
	return ut_failed_count != 0;
}
