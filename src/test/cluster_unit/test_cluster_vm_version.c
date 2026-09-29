/*-------------------------------------------------------------------------
 *
 * test_cluster_vm_version.c
 *    Native VM read versus versioned initialization and pin-reuse boundaries.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_vm_version.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <setjmp.h>

#include "access/visibilitymap.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "catalog/pg_class.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_space_storage.h"
#include "cluster/storage/cluster_smgr.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/smgr.h"
#include "utils/inval.h"
#include "utils/rel.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

#define MAPSIZE (BLCKSZ - MAXALIGN(SizeOfPageHeaderData))
#define HEAPBLOCKS_PER_PAGE (MAPSIZE * 4)
#define HEAPBLK_TO_MAPBLOCK(x) ((x) / HEAPBLOCKS_PER_PAGE)
#define HEAPBLK_TO_MAPBYTE(x) (((x) % HEAPBLOCKS_PER_PAGE) / 4)
#define HEAPBLK_TO_OFFSET(x) (((x) % 4) * 2)

bool cluster_shared_config = true, cluster_enabled = true, InRecovery;
bool wal_log_hints;
int NBuffers = 1, NLocBuffer, wal_level = WAL_LEVEL_REPLICA;
char *BufferBlocks;
Block *LocalBufferBlockPointers;
volatile uint32 CritSectionCount;
bool cluster_recmerge_window_active, cluster_recmerge_apply_foreign;
uint64 cluster_recmerge_window_scn, cluster_recmerge_window_own_lsn;

static PGAlignedBlock vm;
static RelationData relation;
static FormData_pg_class relform;
static SMgrRelationData storage;
static unsigned reads, identities, init_calls, locks, unlocks, pins, releases, dirty;
static bool identity_ok, expecting_error, release_on_lock, installed_remote;
static bool corrupt_extension_fallback;
static unsigned extensions, invalidations;
static ReadBufferMode read_mode;
static jmp_buf error_jump;

/* The original native VM routine calls these through its normal interfaces. */
static Buffer vm_readbuf(Relation rel, BlockNumber block, bool extend);
static Buffer vm_extend(Relation rel, BlockNumber blocks);

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	printf("# Unexpected assertion: %s at %s:%d\n", condition, file, line);
	abort();
}

bool
RecoveryInProgress(void)
{
	return InRecovery;
}

int
cluster_smgr_which_for(RelFileLocator locator, BackendId backend)
{
	if (!RelFileLocatorEquals(locator, relation.rd_locator) || backend != InvalidBackendId)
		abort();
	return 1;
}

bool
cluster_space_relation_get_identity(Relation rel, ClusterSpaceIdentity *out)
{
	if (rel != &relation || pins != 0 || locks != unlocks)
		abort();
	identities++;
	memset(out, 0, sizeof(*out));
	out->key.locator = rel->rd_locator;
	return identity_ok;
}

/* The real WAL initializer is independently covered by space-copy-version.
 * Here this fixture asserts the native call occurs with exactly the locked
 * zero destination; it is not a simulated storage durability proof. */
bool
cluster_space_init_vm_buffer_wal(const ClusterSpaceIdentity *identity, Buffer buffer)
{
	PGAlignedBlock zero;

	memset(&zero, 0, sizeof(zero));
	if (buffer != 1 || pins != 1 || locks != unlocks + 1 || identities != 1
		|| !RelFileLocatorEquals(identity->key.locator, relation.rd_locator)
		|| memcmp(zero.data, vm.data, BLCKSZ) != 0)
		abort();
	PageInit(vm.data, BLCKSZ, 0);
	((PageHeader)vm.data)->pd_block_scn = 101;
	init_calls++;
	return true;
}

SMgrRelation
smgropen(RelFileLocator locator, BackendId backend)
{
	if (!RelFileLocatorEquals(locator, relation.rd_locator) || backend != InvalidBackendId)
		abort();
	return &storage;
}

void
smgrsetowner(SMgrRelation *owner, SMgrRelation rel)
{
	*owner = rel;
}

bool
smgrexists(SMgrRelation rel, ForkNumber forknum)
{
	if (rel != &storage || forknum != VISIBILITYMAP_FORKNUM)
		abort();
	return true;
}

BlockNumber
smgrnblocks(SMgrRelation rel, ForkNumber forknum)
{
	if (rel != &storage || forknum != VISIBILITYMAP_FORKNUM)
		abort();
	return storage.smgr_cached_nblocks[forknum];
}

Buffer
ReadBufferExtended(Relation rel, ForkNumber forknum, BlockNumber block, ReadBufferMode mode,
				   BufferAccessStrategy strategy)
{
	if (rel != &relation || forknum != VISIBILITYMAP_FORKNUM || block != 0 || strategy != NULL
		|| pins != 0)
		abort();
	reads++;
	pins++;
	read_mode = mode;
	return 1;
}

Buffer
ExtendBufferedRelTo(BufferManagerRelation bmr, ForkNumber forknum, BufferAccessStrategy strategy,
					uint32 flags, BlockNumber blocks, ReadBufferMode mode)
{
	if (bmr.rel != &relation || bmr.smgr != NULL || forknum != VISIBILITYMAP_FORKNUM
		|| strategy != NULL || flags != (EB_CREATE_FORK_IF_NEEDED | EB_CLEAR_SIZE_CACHE)
		|| blocks != 1)
		abort();
	extensions++;
	read_mode = mode;
	/* A concurrent extension may leave an existing page for bufmgr to read.
	 * Preserve its native mode-dependent corruption behavior at this I/O seam. */
	if (corrupt_extension_fallback) {
		if (mode == RBM_NORMAL)
			elog(ERROR, "invalid page in concurrent VM extension fallback");
		if (mode != RBM_ZERO_ON_ERROR)
			abort();
		memset(vm.data, 0, BLCKSZ);
	}
	storage.smgr_cached_nblocks[VISIBILITYMAP_FORKNUM] = blocks;
	return ReadBufferExtended(bmr.rel, forknum, blocks - 1, mode, strategy);
}

void
CacheInvalidateSmgr(RelFileLocatorBackend locator)
{
	if (!RelFileLocatorEquals(locator.locator, relation.rd_locator)
		|| locator.backend != InvalidBackendId)
		abort();
	invalidations++;
}

bool
ReadRecentBuffer(RelFileLocator locator, ForkNumber forknum, BlockNumber block, Buffer recent)
{
	if (!RelFileLocatorEquals(locator, relation.rd_locator) || forknum != VISIBILITYMAP_FORKNUM
		|| block != 0 || recent != 1 || pins != 0)
		abort();
	pins++;
	return true;
}

BlockNumber
BufferGetBlockNumber(Buffer buffer)
{
	if (buffer != 1 || pins == 0)
		abort();
	return 0;
}

void
ReleaseBuffer(Buffer buffer)
{
	if (buffer != 1 || pins == 0 || locks != unlocks)
		abort();
	pins--;
	releases++;
}

Buffer
LockBufferForVisibilityMapPageInit(Buffer buffer)
{
	if (buffer != 1 || pins != 1)
		abort();
	if (release_on_lock) {
		release_on_lock = false;
		ReleaseBuffer(buffer);
		return InvalidBuffer;
	}
	locks++;
	if (installed_remote) {
		PageInit(vm.data, BLCKSZ, 0);
		((PageHeader)vm.data)->pd_block_scn = 55;
	}
	return buffer;
}

void
LockBuffer(Buffer buffer, int mode)
{
	if (buffer != 1 || pins != 1)
		abort();
	if (mode == BUFFER_LOCK_EXCLUSIVE)
		locks++;
	else if (mode == BUFFER_LOCK_UNLOCK)
		unlocks++;
	else
		abort();
}

void
UnlockReleaseBuffer(Buffer buffer)
{
	LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
	ReleaseBuffer(buffer);
}

void
MarkBufferDirty(Buffer buffer)
{
	if (buffer != 1 || pins != 1 || locks != unlocks + 1 || CritSectionCount == 0)
		abort();
	dirty++;
}

bool
DataChecksumsEnabled(void)
{
	return false;
}

XLogRecPtr
log_newpage_buffer(Buffer buffer, bool standard)
{
	(void)buffer;
	(void)standard;
	abort();
}

bool
errstart(int level, const char *domain)
{
	(void)domain;
	if (level == DEBUG1)
		return false;
	if (!expecting_error || level != ERROR)
		abort();
	return true;
}

int
errmsg_internal(const char *fmt, ...)
{
	(void)fmt;
	return 0;
}

void
errfinish(const char *file, int line, const char *func)
{
	(void)file;
	(void)line;
	(void)func;
	if (expecting_error)
		longjmp(error_jump, 1);
	abort();
}

#include "test_cluster_vm_version.inc"

static void
reset(bool shared)
{
	memset(&vm, 0, sizeof(vm));
	memset(&storage, 0, sizeof(storage));
	memset(&relation, 0, sizeof(relation));
	memset(&relform, 0, sizeof(relform));
	relform.relpersistence = RELPERSISTENCE_PERMANENT;
	relation.rd_rel = &relform;
	relation.rd_locator = (RelFileLocator){ 1663, 5, 16384 };
	relation.rd_backend = InvalidBackendId;
	relation.rd_smgr = &storage;
	storage.smgr_rlocator.locator = relation.rd_locator;
	storage.smgr_rlocator.backend = InvalidBackendId;
	storage.smgr_cached_nblocks[VISIBILITYMAP_FORKNUM] = 2;
	BufferBlocks = vm.data;
	cluster_shared_config = shared;
	reads = identities = init_calls = locks = unlocks = pins = releases = dirty = 0;
	extensions = invalidations = 0;
	identity_ok = true;
	expecting_error = release_on_lock = installed_remote = InRecovery = false;
	corrupt_extension_fallback = false;
}

UT_TEST(test_shared_read_observes_zero_without_initialization)
{
	PGAlignedBlock before;
	Buffer buffer;

	reset(true);
	before = vm;
	buffer = vm_readbuf(&relation, 0, false);
	UT_ASSERT_EQ(buffer, 1);
	UT_ASSERT(memcmp(before.data, vm.data, BLCKSZ) == 0);
	UT_ASSERT_EQ(locks, 0);
	UT_ASSERT_EQ(identities, 0);
	UT_ASSERT_EQ(init_calls, 0);
	UT_ASSERT_EQ(read_mode, RBM_NORMAL);
	UT_ASSERT(!visibilitymap_pin_ok(10, buffer));
	ReleaseBuffer(buffer);
}

UT_TEST(test_write_pin_reinitializes_read_only_zero_with_version)
{
	Buffer buffer;

	reset(true);
	buffer = vm_readbuf(&relation, 0, false);
	visibilitymap_pin(&relation, 10, &buffer);
	UT_ASSERT_EQ(buffer, 1);
	UT_ASSERT_EQ(identities, 1);
	UT_ASSERT_EQ(init_calls, 1);
	UT_ASSERT_EQ(((PageHeader)vm.data)->pd_block_scn, 101);
	UT_ASSERT(visibilitymap_pin_ok(10, buffer));
	UT_ASSERT_EQ(locks, unlocks);
	ReleaseBuffer(buffer);
}

UT_TEST(test_recent_pin_refuses_unformatted_without_leaking_pin)
{
	Buffer buffer = InvalidBuffer;

	reset(true);
	UT_ASSERT(!visibilitymap_pin_recent(&relation, 10, 1, &buffer));
	UT_ASSERT_EQ(buffer, InvalidBuffer);
	UT_ASSERT_EQ(pins, 0);
	/* Keep later checks runnable on the unfixed production branch. */
	if (BufferIsValid(buffer))
		ReleaseBuffer(buffer);
	buffer = InvalidBuffer;
	PageInit(vm.data, BLCKSZ, 0);
	((PageHeader)vm.data)->pd_block_scn = 55;
	UT_ASSERT(visibilitymap_pin_recent(&relation, 10, 1, &buffer));
	ReleaseBuffer(buffer);
}

UT_TEST(test_missing_identity_fails_before_vm_access)
{
	reset(true);
	identity_ok = false;
	expecting_error = true;
	if (setjmp(error_jump) == 0) {
		(void)vm_readbuf(&relation, 0, true);
		UT_ASSERT(false);
	}
	UT_ASSERT_EQ(identities, 1);
	UT_ASSERT_EQ(reads, 0);
	UT_ASSERT_EQ(pins, 0);
	UT_ASSERT_EQ(locks, 0);
	UT_ASSERT(PageIsNew(vm.data));
}

UT_TEST(test_retry_and_remote_initialization_keep_original_ownership)
{
	Buffer buffer;

	reset(true);
	release_on_lock = true;
	buffer = vm_readbuf(&relation, 0, true);
	UT_ASSERT_EQ(reads, 2);
	UT_ASSERT_EQ(init_calls, 1);
	UT_ASSERT_EQ(identities, 1);
	ReleaseBuffer(buffer);
	reset(true);
	installed_remote = true;
	buffer = vm_readbuf(&relation, 0, true);
	UT_ASSERT_EQ(init_calls, 0);
	UT_ASSERT_EQ(((PageHeader)vm.data)->pd_block_scn, 55);
	ReleaseBuffer(buffer);
}

UT_TEST(test_truncate_zero_tail_does_not_dirty_an_unformatted_page)
{
	reset(true);
	UT_ASSERT_EQ(visibilitymap_prepare_truncate(&relation, 10), 1);
	UT_ASSERT(PageIsNew(vm.data));
	UT_ASSERT_EQ(dirty, 0);
	UT_ASSERT_EQ(init_calls, 0);
	/* Even a zero tail is rechecked under the original exclusive content lock. */
	UT_ASSERT_EQ(locks, 1);
	UT_ASSERT_EQ(unlocks, locks);
	UT_ASSERT_EQ(pins, 0);
}

UT_TEST(test_legacy_read_keeps_native_initialization)
{
	Buffer buffer;

	reset(false);
	buffer = vm_readbuf(&relation, 0, false);
	UT_ASSERT(!PageIsNew(vm.data));
	UT_ASSERT_EQ(locks, 1);
	UT_ASSERT_EQ(identities, 0);
	UT_ASSERT_EQ(init_calls, 0);
	UT_ASSERT_EQ(read_mode, RBM_ZERO_ON_ERROR);
	ReleaseBuffer(buffer);
}

UT_TEST(test_shared_extension_preserves_existing_and_zero_predecessors)
{
	Buffer buffer;

	reset(true);
	storage.smgr_cached_nblocks[VISIBILITYMAP_FORKNUM] = 0;
	PageInit(vm.data, BLCKSZ, 0);
	((PageHeader)vm.data)->pd_block_scn = 77;
	buffer = vm_readbuf(&relation, 0, true);
	UT_ASSERT_EQ(extensions, 1);
	UT_ASSERT_EQ(invalidations, 1);
	UT_ASSERT_EQ(read_mode, RBM_NORMAL);
	UT_ASSERT_EQ(init_calls, 0);
	UT_ASSERT_EQ(((PageHeader)vm.data)->pd_block_scn, 77);
	ReleaseBuffer(buffer);

	reset(true);
	storage.smgr_cached_nblocks[VISIBILITYMAP_FORKNUM] = 0;
	buffer = vm_readbuf(&relation, 0, true);
	UT_ASSERT_EQ(read_mode, RBM_NORMAL);
	UT_ASSERT_EQ(init_calls, 1);
	UT_ASSERT_EQ(invalidations, 1);
	ReleaseBuffer(buffer);
}

UT_TEST(test_shared_extension_corruption_cannot_become_zero_base)
{
	PGAlignedBlock before;

	reset(true);
	storage.smgr_cached_nblocks[VISIBILITYMAP_FORKNUM] = 0;
	memset(vm.data, 0x5a, BLCKSZ);
	before = vm;
	corrupt_extension_fallback = expecting_error = true;
	if (setjmp(error_jump) == 0) {
		Buffer buffer = vm_readbuf(&relation, 0, true);

		ReleaseBuffer(buffer);
		UT_ASSERT(false);
	}
	UT_ASSERT_EQ(extensions, 1);
	UT_ASSERT_EQ(read_mode, RBM_NORMAL);
	UT_ASSERT_EQ(init_calls, 0);
	UT_ASSERT_EQ(invalidations, 0);
	UT_ASSERT_EQ(pins, 0);
	UT_ASSERT(memcmp(before.data, vm.data, BLCKSZ) == 0);
}

UT_TEST(test_legacy_extension_keeps_native_zero_on_error)
{
	Buffer buffer;

	reset(false);
	storage.smgr_cached_nblocks[VISIBILITYMAP_FORKNUM] = 0;
	memset(vm.data, 0x5a, BLCKSZ);
	corrupt_extension_fallback = true;
	buffer = vm_readbuf(&relation, 0, true);
	UT_ASSERT_EQ(extensions, 1);
	UT_ASSERT_EQ(read_mode, RBM_ZERO_ON_ERROR);
	UT_ASSERT_EQ(init_calls, 0);
	UT_ASSERT(!PageIsNew(vm.data));
	UT_ASSERT_EQ(((PageHeader)vm.data)->pd_block_scn, 0);
	ReleaseBuffer(buffer);
}

int
main(void)
{
	UT_PLAN(10);
	UT_RUN(test_shared_read_observes_zero_without_initialization);
	UT_RUN(test_write_pin_reinitializes_read_only_zero_with_version);
	UT_RUN(test_recent_pin_refuses_unformatted_without_leaking_pin);
	UT_RUN(test_missing_identity_fails_before_vm_access);
	UT_RUN(test_retry_and_remote_initialization_keep_original_ownership);
	UT_RUN(test_truncate_zero_tail_does_not_dirty_an_unformatted_page);
	UT_RUN(test_legacy_read_keeps_native_initialization);
	UT_RUN(test_shared_extension_preserves_existing_and_zero_predecessors);
	UT_RUN(test_shared_extension_corruption_cannot_become_zero_base);
	UT_RUN(test_legacy_extension_keeps_native_zero_on_error);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
