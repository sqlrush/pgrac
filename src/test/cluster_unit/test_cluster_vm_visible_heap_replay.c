/*-------------------------------------------------------------------------
 *
 * test_cluster_vm_visible_heap_replay.c
 *    Replay both pages of a captured native visible publication.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_vm_visible_heap_replay.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "cluster/cluster_page_wal.h"
#include "storage/bufmgr.h"
#include "storage/freespace.h"
#include "storage/standby.h"

int vm_replay_fixture_main(void);
void heap_replay_publisher_dirty(Buffer buffer);
#define VM_HEAP_REPLAY_FIXTURE
#define VM_REPLAY_MAIN vm_replay_fixture_main
#define MarkBufferDirty heap_replay_publisher_dirty
#include "test_cluster_vm_visible_replay.c"
#undef MarkBufferDirty

static unsigned heap_replay_reads[2], heap_replay_fsm;
static unsigned heap_replay_skip;
static bool heap_replay_allowed = true, heap_replay_error_at_fsm;
static unsigned heap_replay_failures;
HotStandbyState standbyState = STANDBY_DISABLED;

bool heap_replay_read_identity(RelFileLocator locator, ClusterSpaceIdentity *out);

int
errdetail(const char *format, ...)
{
	(void)format;
	UT_ASSERT(expecting_error && CritSectionCount == 0);
	return 0;
}

bool
heap_replay_read_identity(RelFileLocator locator, ClusterSpaceIdentity *out)
{
	return replay_read_identity(locator, out);
}

/* Selected graph decisions are inputs, not a recovery graph implementation.
 * Bind them to the original producer's complete before/result identities. */
bool
cluster_cold_redo_block_decision_v1(XLogReaderState *record, uint8 id, ClusterColdRedoBlockV1 *out)
{
	const RfPageVersionEdgeEntryV1 *entry;

	UT_ASSERT(record == replay_reader && id < 2);
	if (record == NULL || record != replay_reader || record->record == NULL || id >= 2)
		return false;
	entry = &record->record->page_version_edge.entries[id];
	UT_ASSERT_EQ(entry->block_id, id);
	memset(out, 0, sizeof(*out));
	out->action
		= (heap_replay_skip & (1U << id)) ? CLUSTER_COLD_REDO_SKIP : CLUSTER_COLD_REDO_APPLY;
	out->expected_kind = CLUSTER_COLD_DATA_PRESENT;
	out->expected_before = entry->before;
	memcpy(out->result.segment_incarnation, entry->result_incarnation, 16);
	out->result.mutation_token = record->record->page_version_edge.result_token;
	return true;
}

bool
cluster_page_wal_cold_redo_write_allowed_v1(void)
{
	UT_ASSERT(InRecovery && MyBackendType == B_STARTUP);
	UT_ASSERT(!pinned[0] && !pinned[1] && !held[0] && !held[1]);
	return heap_replay_allowed;
}

void
cluster_page_wal_cold_redo_fail_v1(void)
{
	heap_replay_failures++;
}

Buffer
XLogReadBufferExtended(RelFileLocator locator, ForkNumber forknum, BlockNumber block,
					   ReadBufferMode mode, Buffer recent)
{
	unsigned slot = forknum == MAIN_FORKNUM ? 0 : 1;

	UT_ASSERT(RelFileLocatorEquals(locator, relation.rd_locator));
	UT_ASSERT((forknum == MAIN_FORKNUM || forknum == VISIBILITYMAP_FORKNUM) && block == 0);
	UT_ASSERT(mode == RBM_NORMAL_NO_LOG && recent == InvalidBuffer);
	UT_ASSERT(InRecovery && cluster_page_cold_redo_active_v1);
	UT_ASSERT(!pinned[0] && !pinned[1] && !held[0] && !held[1]);
	/* A selected VM image is reached only after the native heap leg. */
	if (slot == 1)
		UT_ASSERT(PageIsAllVisible(pages[0].data));
	pinned[slot] = true;
	heap_replay_reads[slot]++;
	return slot + 1;
}

void
MarkBufferDirty(Buffer buffer)
{
	UT_ASSERT(buffer == 1 || buffer == 2);
	if (buffer != 1 && buffer != 2)
		abort();
	UT_ASSERT(InRecovery && cluster_page_cold_redo_active_v1 && CritSectionCount == 0);
	UT_ASSERT(pinned[buffer - 1] && held[buffer - 1]);
	dirty[buffer - 1]++;
	cluster_page_cold_redo_dirty_v1(buffer);
}

void
XLogRecordPageWithFreeSpace(RelFileLocator locator, BlockNumber heap_block, Size space)
{
	UT_ASSERT(RelFileLocatorEquals(locator, relation.rd_locator) && heap_block == 0);
	UT_ASSERT(!pinned[0] && !pinned[1] && !held[0] && !held[1]);
	UT_ASSERT_EQ(space, PageGetFreeSpace(pages[0].data));
	heap_replay_fsm++;
	if (heap_replay_error_at_fsm)
		elog(ERROR, "controlled interruption between native heap and VM redo");
}

void
ResolveRecoveryConflictWithSnapshot(TransactionId horizon, bool catalog, RelFileLocator locator)
{
	(void)horizon;
	(void)catalog;
	(void)locator;
	abort(); /* This driver has no standby reader. */
}

void
FlushOneBuffer(Buffer buffer)
{
	(void)buffer;
	abort(); /* No init-fork flush or physical persistence is supplied. */
}

Relation
CreateFakeRelcacheEntry(RelFileLocator locator)
{
	(void)locator;
	abort(); /* The versioned entry must not fall back to runtime VM init. */
}

void
FreeFakeRelcacheEntry(Relation rel)
{
	(void)rel;
	abort();
}

#include "test_cluster_vm_visible_heap_read.inc"
#include "test_cluster_vm_visible_heap_redo.inc"

static void
heap_replay_prepare(OffsetNumber unused, bool share, unsigned persisted)
{
	replay_prepare(unused, share);
	pages[0] = persisted & 2 ? replay_heap : before[0];
	pages[1] = persisted & 1 ? replay_result : before[1];
	heap_replay_skip = persisted;
	heap_replay_reads[0] = heap_replay_reads[1] = heap_replay_fsm = 0;
	heap_replay_allowed = true;
	heap_replay_error_at_fsm = false;
	heap_replay_failures = 0;
}

static void
heap_replay_apply(void)
{
	cluster_page_cold_redo_begin_v1(replay_reader);
	heap_xlog_visible(replay_reader);
	cluster_page_cold_redo_end_v1(replay_reader);
}

static void
heap_replay_assert_result(void)
{
	UT_ASSERT(memcmp(pages[0].data, replay_heap.data, BLCKSZ) == 0);
	UT_ASSERT(memcmp(pages[1].data, replay_result.data, BLCKSZ) == 0);
	UT_ASSERT(PageIsAllVisible(pages[0].data));
	UT_ASSERT_EQ(((PageHeader)pages[0].data)->pd_block_scn, edge_token);
	UT_ASSERT_EQ(((PageHeader)pages[1].data)->pd_block_scn, edge_token);
	UT_ASSERT_EQ(inserts, 1);
	UT_ASSERT_EQ(next_token, edge_token);
	UT_ASSERT(!pinned[0] && !pinned[1] && !held[0] && !held[1]);
	UT_ASSERT(!cluster_page_cold_redo_active_v1);
	UT_ASSERT_EQ(heap_replay_failures, 0);
}

UT_TEST(test_native_visible_redo_restores_heap_and_vm)
{
	static const OffsetNumber counts[] = { 0, 1, 8, MaxHeapTuplesPerPage };

	for (unsigned n = 0; n < lengthof(counts); n++) {
		for (unsigned share = 0; share < 2; share++) {
			heap_replay_prepare(counts[n], share != 0, 0);
			heap_replay_apply();
			heap_replay_assert_result();
			UT_ASSERT_EQ(heap_replay_reads[0], 1);
			UT_ASSERT_EQ(heap_replay_reads[1], 1);
			UT_ASSERT_EQ(heap_replay_fsm, 1);
		}
	}
}

UT_TEST(test_native_visible_redo_completes_each_two_page_persistence_cut)
{
	for (unsigned cut = 0; cut < 4; cut++) {
		heap_replay_prepare(8, true, cut);
		heap_replay_apply();
		heap_replay_assert_result();
		UT_ASSERT_EQ(heap_replay_reads[0], (cut & 2) ? 0 : 1);
		UT_ASSERT_EQ(heap_replay_reads[1], (cut & 1) ? 0 : 1);
		UT_ASSERT_EQ(heap_replay_fsm, (cut & 2) ? 0 : 1);
		UT_ASSERT_EQ(dirty[0], (cut & 2) ? 1 : 2);
		UT_ASSERT_EQ(dirty[1], (cut & 1) ? 1 : 2);
	}
}

UT_TEST(test_native_visible_redo_repeated_selection_skips_both_results)
{
	heap_replay_prepare(8, false, 0);
	heap_replay_apply();
	heap_replay_assert_result();
	heap_replay_skip = 3;
	heap_replay_apply();
	heap_replay_assert_result();
	UT_ASSERT_EQ(heap_replay_reads[0], 1);
	UT_ASSERT_EQ(heap_replay_reads[1], 1);
	UT_ASSERT_EQ(heap_replay_fsm, 1);
	UT_ASSERT_EQ(dirty[0], 2);
	UT_ASSERT_EQ(dirty[1], 2);
}

UT_TEST(test_cold_begin_refuses_unavailable_write_or_identity_before_pages)
{
	for (unsigned fault = 0; fault < 2; fault++) {
		volatile bool caught = false;

		heap_replay_prepare(8, false, 0);
		if (fault == 0)
			heap_replay_allowed = false;
		else
			replay_identity_ok = false;
		expecting_error = true;
		if (setjmp(error_jump) == 0)
			heap_replay_apply();
		else
			caught = true;
		expecting_error = false;
		/* The fixture invokes the driver's native abort after a refused begin;
		 * this does not model ResourceOwner/proc_exit or OS cleanup. */
		cluster_page_cold_redo_abort_v1();
		UT_ASSERT(caught);
		UT_ASSERT_EQ(heap_replay_reads[0] + heap_replay_reads[1] + heap_replay_fsm, 0);
		UT_ASSERT(memcmp(pages[0].data, before[0].data, BLCKSZ) == 0);
		UT_ASSERT(memcmp(pages[1].data, before[1].data, BLCKSZ) == 0);
		UT_ASSERT(!pinned[0] && !pinned[1] && !held[0] && !held[1]);
		UT_ASSERT_EQ(inserts, 1);
	}
}

UT_TEST(test_heap_result_survives_error_before_vm_and_finishes_on_reselection)
{
	volatile bool caught = false;

	heap_replay_prepare(8, true, 0);
	heap_replay_error_at_fsm = true;
	expecting_error = true;
	if (setjmp(error_jump) == 0)
		heap_replay_apply();
	else
		caught = true;
	expecting_error = false;
	UT_ASSERT(caught);
	UT_ASSERT(memcmp(pages[0].data, replay_heap.data, BLCKSZ) == 0);
	UT_ASSERT(memcmp(pages[1].data, before[1].data, BLCKSZ) == 0);
	UT_ASSERT(!pinned[0] && !pinned[1] && !held[0] && !held[1]);
	UT_ASSERT_EQ(inserts, 1);
	UT_ASSERT(cluster_page_cold_redo_active_v1);
	cluster_page_cold_redo_abort_v1();
	UT_ASSERT(!cluster_page_cold_redo_active_v1);
	heap_replay_error_at_fsm = false;
	heap_replay_skip = 2;
	heap_replay_apply();
	heap_replay_assert_result();
	UT_ASSERT_EQ(heap_replay_reads[0], 1);
	UT_ASSERT_EQ(heap_replay_reads[1], 1);
	UT_ASSERT_EQ(heap_replay_fsm, 1);
}

int
main(void)
{
	UT_PLAN(5);
	UT_RUN(test_native_visible_redo_restores_heap_and_vm);
	UT_RUN(test_native_visible_redo_completes_each_two_page_persistence_cut);
	UT_RUN(test_native_visible_redo_repeated_selection_skips_both_results);
	UT_RUN(test_cold_begin_refuses_unavailable_write_or_identity_before_pages);
	UT_RUN(test_heap_result_survives_error_before_vm_and_finishes_on_reselection);
	XLogReaderFree(replay_reader);
	free(replay_raw);
	UT_DONE();
	return ut_failed_count != 0;
}
