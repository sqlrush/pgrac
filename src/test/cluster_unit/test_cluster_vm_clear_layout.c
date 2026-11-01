/*-------------------------------------------------------------------------
 * test_cluster_vm_clear_layout.c
 *    Shared VM withdrawal validates the versioned bitmap before access.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#ifdef FIXTURE_NO_ASSERTS
#undef USE_ASSERT_CHECKING
#undef Assert
#define Assert(condition) ((void)true)
#undef AssertMacro
#define AssertMacro(condition) ((void)true)
#undef PG_USED_FOR_ASSERTS_ONLY
#define PG_USED_FOR_ASSERTS_ONLY pg_attribute_unused()
#endif

#include <setjmp.h>

#include "access/visibilitymap.h"
#include "access/xlog.h"
#include "catalog/pg_class.h"
#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_guc.h"
#include "cluster/storage/cluster_smgr.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "utils/rel.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

#define MAPSIZE (BLCKSZ - MAXALIGN(SizeOfPageHeaderData))
#define HEAPBLOCKS_PER_PAGE (MAPSIZE * 4)

bool cluster_shared_config = true, cluster_enabled = true, InRecovery;
int NBuffers = 1, NLocBuffer, wal_level = WAL_LEVEL_REPLICA;
char *BufferBlocks;
Block *LocalBufferBlockPointers;

static PGAlignedBlock image, before;
static RelationData relation;
static FormData_pg_class relform;
static RelFileLocator actual_locator;
static ForkNumber actual_fork;
static BlockNumber actual_block;
static bool pinned, held;
static bool aux_refused, aux_replaced, expecting_error;
static int storage_kind;
static unsigned locks, unlocks, aux_calls, dirty, errors, assertions;
static jmp_buf error_jump;

void
ExceptionalCondition(const char *condition pg_attribute_unused(),
					 const char *file pg_attribute_unused(), int line pg_attribute_unused())
{
	assertions++;
	abort();
}

bool
errstart(int level, const char *domain pg_attribute_unused())
{
	return level >= ERROR;
}

bool
errstart_cold(int level, const char *domain)
{
	return errstart(level, domain);
}

int
errmsg_internal(const char *format pg_attribute_unused(), ...)
{
	return 0;
}

void
errfinish(const char *file pg_attribute_unused(), int line pg_attribute_unused(),
		  const char *function pg_attribute_unused())
{
	if (!expecting_error)
		abort();
	errors++;
	longjmp(error_jump, 1);
}

void
BufferGetTag(Buffer buffer, RelFileLocator *locator, ForkNumber *forknum, BlockNumber *block)
{
	if (buffer != 1)
		abort();
	*locator = actual_locator;
	*forknum = actual_fork;
	*block = actual_block;
}

BlockNumber
BufferGetBlockNumber(Buffer buffer)
{
	if (buffer != 1)
		abort();
	return actual_block;
}

int
cluster_smgr_which_for(RelFileLocator locator, BackendId backend)
{
	if (!RelFileLocatorEquals(locator, relation.rd_locator) || backend != InvalidBackendId)
		abort();
	return storage_kind;
}

void
LockBuffer(Buffer buffer, int mode)
{
	if (buffer != 1 || !pinned)
		abort();
	if (mode == BUFFER_LOCK_EXCLUSIVE) {
		if (held)
			abort();
		held = true;
		locks++;
	} else if (mode == BUFFER_LOCK_UNLOCK) {
		if (!held)
			abort();
		held = false;
		unlocks++;
	} else
		abort();
}

bool
ClusterLockBufferExclusiveAuxiliaryAware(Buffer *buffer,
										struct ResourceXAuxiliaryAcquireContext *context)
{
	if (buffer == NULL || *buffer != 1 || context == NULL || held)
		abort();
	aux_calls++;
	if (aux_refused) {
		if (aux_replaced) {
			pinned = false;
			*buffer = InvalidBuffer;
		}
		return false;
	}
	LockBuffer(*buffer, BUFFER_LOCK_EXCLUSIVE);
	return true;
}

void
MarkBufferDirty(Buffer buffer)
{
	if (buffer != 1)
		abort();
	dirty++;
}

static void
reset(void)
{
	memset(&relation, 0, sizeof(relation));
	memset(&relform, 0, sizeof(relform));
	relation.rd_rel = &relform;
	relation.rd_locator = (RelFileLocator){1663, 5, 900};
	relform.relpersistence = RELPERSISTENCE_PERMANENT;
	actual_locator = relation.rd_locator;
	actual_fork = VISIBILITYMAP_FORKNUM;
	actual_block = 0;
	BufferBlocks = image.data;
	memset(image.data, 0, BLCKSZ);
	PageInit(image.data, BLCKSZ, 0);
	((PageHeader)image.data)->pd_block_scn = 30;
	memset(PageGetContents(image.data), 255, MAPSIZE);
	pinned = true;
	held = aux_refused = aux_replaced = expecting_error = InRecovery = false;
	cluster_shared_config = cluster_enabled = true;
	storage_kind = 1;
	locks = unlocks = aux_calls = dirty = errors = assertions = 0;
}

static bool
clear_entry(int entry, BlockNumber heap_block, uint8 flags)
{
	Buffer buffer = 1;
	ResourceXAuxiliaryAcquireContext context = {0};
	bool cleared = true;

	if (entry == 0)
		return visibilitymap_clear_locked(&relation, heap_block, buffer, flags);
	if (entry == 1)
		return visibilitymap_clear(&relation, heap_block, buffer, flags);
	if (!visibilitymap_clear_retry_aware(&relation, heap_block, &buffer, flags,
									   &context, &cleared))
		return false;
	return cleared;
}

static void
expect_refusal(int entry, BlockNumber heap_block, uint8 flags)
{
	memcpy(before.data, image.data, BLCKSZ);
	expecting_error = true;
	if (setjmp(error_jump) == 0) {
		(void)clear_entry(entry, heap_block, flags);
		expecting_error = false;
		UT_ASSERT(false);
	} else
		expecting_error = false;
	UT_ASSERT_EQ(errors, 1);
	UT_ASSERT_EQ(dirty + assertions, 0);
	UT_ASSERT(memcmp(before.data, image.data, BLCKSZ) == 0);
	/* Ordinary ERROR leaves caller-owned locks for native error cleanup.
	 * This fixture does not claim ResourceOwner/proc_exit cleanup coverage. */
}

UT_TEST(test_clear_refuses_missing_page_version_in_every_entry)
{
	for (int entry = 0; entry < 3; entry++) {
		reset();
		held = entry == 0;
		((PageHeader)image.data)->pd_block_scn = 0;
		expect_refusal(entry, 0, VISIBILITYMAP_VALID_BITS);
	}
}

UT_TEST(test_dirty_unformatted_page_is_not_a_bitmap)
{
	for (int entry = 0; entry < 3; entry++) {
		reset();
		held = entry == 0;
		((PageHeader)image.data)->pd_upper = 0;
		UT_ASSERT(PageIsNew(image.data));
		expect_refusal(entry, 0, VISIBILITYMAP_ALL_FROZEN);
	}
}

UT_TEST(test_clear_refuses_invalid_vm_layout_before_rmw)
{
	for (int entry = 0; entry < 3; entry++) {
		for (int cause = 0; cause < 10; cause++) {
			PageHeader header;

			reset();
			held = entry == 0;
			header = (PageHeader)image.data;
			switch (cause) {
			case 0: header->pd_block_scn = 0; break;
			case 1: header->pd_lower++; break;
			case 2: header->pd_upper--; break;
			case 3: header->pd_special--; break;
			case 4: header->pd_pagesize_version ^= 1; break;
			case 5: header->pd_pagesize_version ^= 256; break;
			case 6: header->pd_prune_xid = 17; break;
			case 7: header->pd_flags |= PD_ALL_VISIBLE; break;
			case 8: header->pd_flags = PD_LSN_ORIGIN_MASK; break;
			case 9: header->pd_upper = 0; break;
			}
			expect_refusal(entry, 0, VISIBILITYMAP_VALID_BITS);
		}
	}
}

UT_TEST(test_noop_cannot_launder_invalid_layout_or_version)
{
	for (int entry = 0; entry < 3; entry++) {
		for (int cause = 0; cause < 2; cause++) {
			reset();
			held = entry == 0;
			memset(PageGetContents(image.data), 0, MAPSIZE);
			if (cause == 0)
				((PageHeader)image.data)->pd_upper = 0;
			else
				((PageHeader)image.data)->pd_block_scn = 0;
			expect_refusal(entry, 0, VISIBILITYMAP_VALID_BITS);
		}
	}
}

UT_TEST(test_qualified_rmw_preserves_neighbors_and_version_owner)
{
	static const BlockNumber blocks[] = {0, 1, 3, 4, HEAPBLOCKS_PER_PAGE - 1,
										 HEAPBLOCKS_PER_PAGE, HEAPBLOCKS_PER_PAGE + 3};
	static const uint8 flags[] = {VISIBILITYMAP_ALL_FROZEN, VISIBILITYMAP_VALID_BITS};

	for (int entry = 0; entry < 3; entry++) {
		for (unsigned b = 0; b < lengthof(blocks); b++) {
			for (unsigned f = 0; f < lengthof(flags); f++) {
				Size byte = (blocks[b] % HEAPBLOCKS_PER_PAGE) / 4;
				uint8 mask = flags[f] << ((blocks[b] % 4) * 2);

				reset();
				held = entry == 0;
				actual_block = blocks[b] / HEAPBLOCKS_PER_PAGE;
				memcpy(before.data, image.data, BLCKSZ);
				((uint8 *)PageGetContents(before.data))[byte] &= ~mask;
				UT_ASSERT(clear_entry(entry, blocks[b], flags[f]));
				UT_ASSERT_EQ(dirty, 1);
				UT_ASSERT(memcmp(before.data, image.data, BLCKSZ) == 0);
				UT_ASSERT(!clear_entry(entry, blocks[b], flags[f]));
				UT_ASSERT_EQ(dirty, 1);
				UT_ASSERT_EQ(locks, unlocks);
			}
		}
	}
}

UT_TEST(test_auxiliary_refusal_does_not_inspect_or_modify_bytes)
{
	for (int replaced = 0; replaced < 2; replaced++) {
		Buffer buffer = 1;
		ResourceXAuxiliaryAcquireContext context = {0};
		bool cleared = true;

		reset();
		aux_refused = true;
		aux_replaced = replaced;
		memcpy(before.data, image.data, BLCKSZ);
		UT_ASSERT(!visibilitymap_clear_retry_aware(&relation, 0, &buffer,
			VISIBILITYMAP_VALID_BITS, &context, &cleared));
		UT_ASSERT(!cleared);
		UT_ASSERT_EQ(buffer, replaced ? InvalidBuffer : 1);
		UT_ASSERT_EQ(locks + unlocks + dirty + errors, 0);
		UT_ASSERT(memcmp(before.data, image.data, BLCKSZ) == 0);
	}
}

UT_TEST(test_native_and_unlogged_keep_original_clear_semantics)
{
	for (int entry = 0; entry < 3; entry++) {
		for (int kind = 0; kind < 3; kind++) {
			reset();
			held = entry == 0;
			((PageHeader)image.data)->pd_lower++;
			if (kind == 0)
				cluster_shared_config = false;
			else if (kind == 1)
				storage_kind = 0;
			else
				relform.relpersistence = RELPERSISTENCE_UNLOGGED;
			UT_ASSERT(clear_entry(entry, 0, VISIBILITYMAP_VALID_BITS));
			UT_ASSERT_EQ(dirty, 1);
		}
	}
}

UT_TEST(test_recovery_flag_does_not_authorize_invalid_runtime_bytes)
{
	for (int entry = 0; entry < 3; entry++) {
		reset();
		held = entry == 0;
		InRecovery = true;
		((PageHeader)image.data)->pd_upper = 0;
		expect_refusal(entry, 0, VISIBILITYMAP_VALID_BITS);
	}
}

UT_TEST(test_bad_target_or_mask_refuses_before_lock)
{
	for (int entry = 0; entry < 3; entry++) {
		for (int cause = 0; cause < 7; cause++) {
			BlockNumber block = 0;
			uint8 flags = VISIBILITYMAP_VALID_BITS;

			reset();
			held = entry == 0;
			switch (cause) {
			case 0: actual_locator.relNumber++; break;
			case 1: actual_fork = MAIN_FORKNUM; break;
			case 2: actual_block++; break;
			case 3: flags = 0; break;
			case 4: flags = VISIBILITYMAP_ALL_VISIBLE; break;
			case 5: flags = 255; break;
			case 6: block = InvalidBlockNumber; break;
			}
			expect_refusal(entry, block, flags);
			UT_ASSERT_EQ(locks + unlocks + aux_calls, 0);
		}
	}
}

int
main(void)
{
	UT_PLAN(9);
	UT_RUN(test_clear_refuses_missing_page_version_in_every_entry);
	UT_RUN(test_dirty_unformatted_page_is_not_a_bitmap);
	UT_RUN(test_clear_refuses_invalid_vm_layout_before_rmw);
	UT_RUN(test_noop_cannot_launder_invalid_layout_or_version);
	UT_RUN(test_qualified_rmw_preserves_neighbors_and_version_owner);
	UT_RUN(test_auxiliary_refusal_does_not_inspect_or_modify_bytes);
	UT_RUN(test_native_and_unlogged_keep_original_clear_semantics);
	UT_RUN(test_recovery_flag_does_not_authorize_invalid_runtime_bytes);
	UT_RUN(test_bad_target_or_mask_refuses_before_lock);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
