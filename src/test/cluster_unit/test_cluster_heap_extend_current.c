/*-------------------------------------------------------------------------
 *
 * test_cluster_heap_extend_current.c
 *    Actual heap-extension consumer across remote first initialization.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_heap_extend_current.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/heaptoast.h"
#include "access/hio.h"
#include "catalog/pg_class.h"
#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_itl_slot.h"
#include "storage/bufmgr.h"
#include "storage/freespace.h"
#include "storage/lmgr.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

static PGAlignedBlock pages[4], incoming, preserved;
static RelationData relation_data;
static FormData_pg_class relation_class;
static int pins[4];
static bool locked[4], valid[4];
static bool storage_mode, remote_image, cancel_acquire;
static int ordinary_acquires, direct_refusals, dirties, fsm_records;
static uint32 extension_flags;
static sigjmp_buf error_jump;

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "assertion: %s at %s:%d\n", condition, file, line);
	abort();
}

#include "test_cluster_heap_extend_page.inc"

static void
fixture_error(void)
{
	siglongjmp(error_jump, 1);
}

static BlockNumber
fixture_extend(BufferManagerRelation bmr, ForkNumber fork, BufferAccessStrategy strategy,
			   uint32 flags, uint32 count, Buffer *buffers, uint32 *extended)
{
	uint32 i;
	BufferTag tag = { 1663, 5, 16386, MAIN_FORKNUM, 17003 };

	UT_ASSERT(bmr.rel == &relation_data);
	UT_ASSERT_EQ(fork, MAIN_FORKNUM);
	UT_ASSERT(count > 0 && count <= 4);
	extension_flags = flags;
	for (i = 0; i < count; i++) {
		buffers[i] = (Buffer)i + 1;
		pins[i]++;
	}
	if (flags & EB_LOCK_FIRST) {
		/* The old real consumer asks the foreground known-new path to
		 * acquire current before I/O completion. Its actual production
		 * proof predicate rejects this already-joined remote MAIN image.
		 * This fixture is a controlled boundary, not a live GCS replay. */
		if (storage_mode && remote_image
			&& !GcsBlockResourceXDirectInitProofAllowedExact(&tag, false, true)) {
			direct_refusals++;
			fixture_error();
		}
		if (remote_image)
			memcpy(pages[0].data, incoming.data, BLCKSZ);
		locked[0] = true;
	}
	for (i = 0; i < count; i++)
		valid[i] = true; /* The physical extension API completed input I/O. */
	*extended = count;
	return 17003;
}

static void
fixture_lock(Buffer buffer, int mode)
{
	int i = buffer - 1;

	UT_ASSERT(i >= 0 && i < 4 && pins[i] > 0 && valid[i]);
	if (mode == BUFFER_LOCK_UNLOCK) {
		UT_ASSERT(locked[i]);
		locked[i] = false;
		return;
	}
	UT_ASSERT_EQ(mode, BUFFER_LOCK_EXCLUSIVE);
	UT_ASSERT(!locked[i]);
	ordinary_acquires++;
	if (cancel_acquire)
		fixture_error();
	/* Actual Resource-X is responsible for exact authority, image and
	 * retained delivery. Here only its successful buffer result is supplied. */
	if (remote_image && i == 0)
		memcpy(pages[i].data, incoming.data, BLCKSZ);
	locked[i] = true;
}

static void
fixture_dirty(Buffer buffer)
{
	UT_ASSERT(locked[buffer - 1]);
	dirties++;
}

static void
fixture_release(Buffer buffer)
{
	UT_ASSERT(pins[buffer - 1] > 0 && !locked[buffer - 1]);
	pins[buffer - 1]--;
}

#define cluster_storage_mode_enabled() storage_mode
#define ExtendBufferedRelBy fixture_extend
#define RelationExtensionLockWaiterCount(relation) 0
#define LockBuffer fixture_lock
#define MarkBufferDirty fixture_dirty
#define ReleaseBuffer fixture_release
#define IncrBufferRefCount(buffer) (pins[(buffer) - 1]++)
#define BufferGetPage(buffer) (pages[(buffer) - 1].data)
#define BufferGetPageSize(buffer) BLCKSZ
#define BufferGetBlockNumber(buffer) ((BlockNumber)(17002 + (buffer)))
#define RecordPageWithFreeSpace(relation, block, space) (fsm_records++)
#define FreeSpaceMapVacuumRange(relation, first, last) ((void)0)
#undef elog
#define elog(level, ...) fixture_error()
#include "test_cluster_heap_extend_current.inc"

static void
reset_fixture(bool cluster, bool remote, bool nonempty)
{
	memset(pages, 0, sizeof(pages));
	memset(pins, 0, sizeof(pins));
	memset(locked, 0, sizeof(locked));
	memset(valid, 0, sizeof(valid));
	memset(&relation_data, 0, sizeof(relation_data));
	memset(&relation_class, 0, sizeof(relation_class));
	relation_data.rd_rel = &relation_class;
	relation_class.relpersistence = RELPERSISTENCE_PERMANENT;
	relation_class.relkind = RELKIND_RELATION;
	storage_mode = cluster;
	remote_image = remote;
	cancel_acquire = false;
	ordinary_acquires = direct_refusals = dirties = fsm_records = 0;
	extension_flags = UINT32_MAX;
	PageInitHeapPage(incoming.data, BLCKSZ, 0);
	if (nonempty) {
		PageHeader page = (PageHeader)incoming.data;

		page->pd_lower += sizeof(ItemIdData);
		page->pd_upper -= 128;
		ItemIdSetNormal(PageGetItemId(incoming.data, 1), page->pd_upper, 128);
		memset(incoming.data + page->pd_upper, 0x4d, 128);
	}
	preserved = incoming;
}

UT_TEST(cluster_fresh_extension_uses_ordinary_current_after_io)
{
	bool unlocked = false;
	Buffer result = InvalidBuffer;

	reset_fixture(true, false, false);
	if (sigsetjmp(error_jump, 0) == 0)
		result = RelationAddBlocks(&relation_data, NULL, 1, false, &unlocked);
	UT_ASSERT_EQ(result, 1);
	UT_ASSERT_EQ(extension_flags, 0);
	UT_ASSERT_EQ(ordinary_acquires, 1);
	UT_ASSERT(unlocked && !locked[0] && pins[0] == 1 && valid[0]);
	UT_ASSERT(PageHasItl(pages[0].data));
	UT_ASSERT_EQ(dirties, 1);
}

UT_TEST(remote_initialized_heap_is_not_reinitialized)
{
	int occupied;

	for (occupied = 0; occupied <= 1; occupied++) {
		bool unlocked = false;
		Buffer result = InvalidBuffer;

		reset_fixture(true, true, occupied != 0);
		if (sigsetjmp(error_jump, 0) == 0)
			result = RelationAddBlocks(&relation_data, NULL, 1, false, &unlocked);
		UT_ASSERT_EQ(result, 1);
		UT_ASSERT_EQ(direct_refusals, 0);
		UT_ASSERT_EQ(ordinary_acquires, 1);
		UT_ASSERT_EQ(dirties, 0);
		UT_ASSERT(unlocked && !locked[0] && pins[0] == 1);
		UT_ASSERT(memcmp(pages[0].data, preserved.data, BLCKSZ) == 0);
	}
}

UT_TEST(noncluster_and_local_keep_locked_new_contract)
{
	int local;

	for (local = 0; local <= 1; local++) {
		bool unlocked = true;

		reset_fixture(local != 0, false, false);
		if (local)
			relation_class.relpersistence = RELPERSISTENCE_TEMP;
		UT_ASSERT_EQ(RelationAddBlocks(&relation_data, NULL, 1, false, &unlocked), 1);
		UT_ASSERT_EQ(extension_flags, EB_LOCK_FIRST);
		UT_ASSERT_EQ(ordinary_acquires, 0);
		UT_ASSERT(!unlocked && locked[0] && dirties == 1);
	}
}

UT_TEST(malformed_current_heap_refuses_without_mutation)
{
	int defect;

	for (defect = 0; defect < 6; defect++) {
		PageHeader page;
		bool unlocked = false;
		volatile bool returned = false;

		reset_fixture(true, true, false);
		page = (PageHeader)incoming.data;
		switch (defect) {
		case 0:
			page->pd_flags &= ~PD_HAS_ITL;
			break;
		case 1:
			page->pd_lower = 1;
			break;
		case 2:
			page->pd_special = BLCKSZ;
			break;
		case 3:
			page->pd_pagesize_version--;
			break;
		case 4:
			page->pd_upper = page->pd_special + 1;
			break;
		case 5:
			page->pd_lower++;
			break;
		}
		preserved = incoming;
		if (sigsetjmp(error_jump, 0) == 0) {
			(void)RelationAddBlocks(&relation_data, NULL, 1, false, &unlocked);
			returned = true;
		}
		UT_ASSERT(!returned && dirties == 0);
		if (ordinary_acquires != 0)
			UT_ASSERT(memcmp(pages[0].data, preserved.data, BLCKSZ) == 0);
	}
}

UT_TEST(cancel_before_current_does_not_initialize)
{
	bool unlocked = false;
	volatile bool returned = false;

	reset_fixture(true, false, false);
	cancel_acquire = true;
	if (sigsetjmp(error_jump, 0) == 0) {
		(void)RelationAddBlocks(&relation_data, NULL, 1, false, &unlocked);
		returned = true;
	}
	UT_ASSERT(!returned && ordinary_acquires == 1 && dirties == 0);
	UT_ASSERT(PageIsNew(pages[0].data) && !locked[0]);
}

UT_TEST(bulk_and_extra_pages_preserve_pin_and_fsm_accounting)
{
	BulkInsertStateData bulk = { 0 };
	bool unlocked = false;

	reset_fixture(true, true, true);
	if (sigsetjmp(error_jump, 0) != 0) {
		UT_ASSERT(false);
		return;
	}
	UT_ASSERT_EQ(RelationAddBlocks(&relation_data, &bulk, 3, true, &unlocked), 1);
	UT_ASSERT(unlocked && !locked[0]);
	UT_ASSERT(pins[0] == 2 && pins[1] == 0 && pins[2] == 0);
	UT_ASSERT_EQ(bulk.current_buf, 1);
	UT_ASSERT_EQ(bulk.next_free, 17004);
	UT_ASSERT_EQ(bulk.last_free, 17005);
	UT_ASSERT_EQ(bulk.already_extended_by, 3);
	UT_ASSERT_EQ(dirties, 0);
	UT_ASSERT(memcmp(pages[0].data, preserved.data, BLCKSZ) == 0);
	reset_fixture(true, false, false);
	UT_ASSERT_EQ(RelationAddBlocks(&relation_data, NULL, 3, true, &unlocked), 1);
	UT_ASSERT_EQ(fsm_records, 2);
	UT_ASSERT(unlocked && pins[0] == 1 && pins[1] == 0 && pins[2] == 0);
}

int
main(void)
{
	UT_PLAN(6);
	UT_RUN(cluster_fresh_extension_uses_ordinary_current_after_io);
	UT_RUN(remote_initialized_heap_is_not_reinitialized);
	UT_RUN(noncluster_and_local_keep_locked_new_contract);
	UT_RUN(malformed_current_heap_refuses_without_mutation);
	UT_RUN(cancel_before_current_does_not_initialize);
	UT_RUN(bulk_and_extra_pages_preserve_pin_and_fsm_accounting);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
