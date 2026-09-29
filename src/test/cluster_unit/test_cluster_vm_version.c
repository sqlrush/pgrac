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
#include "catalog/pg_control.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_scn.h"
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
int NBuffers = 1, NLocBuffer, wal_level = WAL_LEVEL_REPLICA, cluster_node_id;
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
static unsigned prepared, version_edges, wal_records, legacy_wal;
static uint64 token;
static bool begun;
static RfPageVersionEdgeEntryV1 wal_edge;
static PGAlignedBlock wal_image;
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

/* The storage seam is a fixture; version capture/stamp/register are real. */
bool
cluster_space_prepare_buffer_versions(const ClusterSpaceIdentity *identity, const Buffer *buffers,
									  const uint8 *ids, uint8 count, RfPageProducerBatchV1 *batch)
{
	RfPageProducerComponentV1 component = { 0 };
	if (count != 1 || buffers[0] != 1 || ids[0] != 0 || pins != 1 || locks != unlocks + 1
		|| CritSectionCount != 0 || identities != 1
		|| !RelFileLocatorEquals(identity->key.locator, relation.rd_locator))
		abort();
	component.page = vm.data;
	component.page_class = RF_PAGE_CLASS_ORDINARY;
	component.before_kind = RF_PAGE_STATE_PRESENT;
	memset(component.segment_incarnation, 7, 16);
	prepared++;
	return rf_page_producer_prepare_v1(&component, 1, batch);
}

SCN
cluster_scn_advance(void)
{
	return ++token;
}

void
XLogBeginInsert(void)
{
	if (begun || locks != unlocks + 1 || CritSectionCount == 0 || dirty != 1)
		abort();
	begun = true;
}

void
XLogRegisterBuffer(uint8 id, Buffer buffer, uint8 flags)
{
	if (!begun || id != 0 || buffer != 1 || flags != REGBUF_FORCE_IMAGE)
		abort();
	wal_image = vm;
}

void
XLogRegisterPageVersionEdge(uint64 result, const RfPageVersionEdgeEntryV1 *entries, uint8 count)
{
	if (!begun || count != 1 || result != token)
		abort();
	wal_edge = entries[0];
	version_edges++;
}

XLogRecPtr
XLogInsert(RmgrId rmid, uint8 info)
{
	if (!begun || rmid != RM_XLOG_ID || info != XLOG_FPI || version_edges != 1
		|| ((PageHeader)wal_image.data)->pd_block_scn != token)
		abort();
	begun = false;
	wal_records++;
	return 200;
}

XLogRecPtr
log_newpage_buffer(Buffer buffer, bool standard)
{
	if (buffer != 1 || standard || locks != unlocks + 1 || CritSectionCount == 0)
		abort();
	legacy_wal++;
	PageSetLSN(vm.data, 100);
	return 100;
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
	prepared = version_edges = wal_records = legacy_wal = 0;
	token = 200;
	begun = wal_log_hints = false;
	memset(&wal_edge, 0, sizeof(wal_edge));
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

static void
truncate_page(bool shared)
{
	reset(shared);
	PageInit(vm.data, BLCKSZ, 0);
	((PageHeader)vm.data)->pd_block_scn = 55;
	memset(PageGetContents(vm.data), 0xff, MAPSIZE);
}

UT_TEST(test_truncate_changed_tail_has_one_exact_fpi)
{
	const BlockNumber boundaries[] = { 1, 4, 10, HEAPBLOCKS_PER_PAGE - 1 };
	for (int hints = 0; hints < 2; hints++)
		for (int i = 0; i < lengthof(boundaries); i++) {
			PGAlignedBlock expected;
			BlockNumber n = boundaries[i];
			char *map;
			truncate_page(true);
			wal_log_hints = hints;
			expected = vm;
			map = PageGetContents(expected.data);
			map[HEAPBLK_TO_MAPBYTE(n)] &= (1 << HEAPBLK_TO_OFFSET(n)) - 1;
			memset(map + HEAPBLK_TO_MAPBYTE(n) + 1, 0, MAPSIZE - HEAPBLK_TO_MAPBYTE(n) - 1);
			UT_ASSERT_EQ(visibilitymap_prepare_truncate(&relation, n), 1);
			UT_ASSERT_EQ(identities, 1);
			UT_ASSERT_EQ(prepared, 1);
			UT_ASSERT_EQ(version_edges, 1);
			UT_ASSERT_EQ(wal_records, 1);
			UT_ASSERT_EQ(legacy_wal, 0);
			UT_ASSERT_EQ(wal_edge.before.mutation_token, 55);
			UT_ASSERT_EQ(wal_edge.before_kind, RF_PAGE_STATE_PRESENT);
			UT_ASSERT_EQ(((PageHeader)vm.data)->pd_block_scn, 201);
			UT_ASSERT_EQ(PageGetLSN(vm.data), 200);
			UT_ASSERT(memcmp(PageGetContents(vm.data), PageGetContents(expected.data), MAPSIZE)
					  == 0);
			UT_ASSERT(
				memcmp(PageGetContents(wal_image.data), PageGetContents(expected.data), MAPSIZE)
				== 0);
			UT_ASSERT_EQ(pins, 0);
			UT_ASSERT_EQ(locks, unlocks);
		}
}

UT_TEST(test_truncate_unchanged_tail_and_page_boundary_are_noops)
{
	PGAlignedBlock before;
	truncate_page(true);
	memset(PageGetContents(vm.data) + 2, 0, MAPSIZE - 2);
	PageGetContents(vm.data)[2] = 0x0f;
	before = vm;
	UT_ASSERT_EQ(visibilitymap_prepare_truncate(&relation, 10), 1);
	UT_ASSERT(memcmp(before.data, vm.data, BLCKSZ) == 0);
	UT_ASSERT_EQ(prepared + dirty + wal_records + legacy_wal, 0);
	truncate_page(true);
	before = vm;
	UT_ASSERT_EQ(visibilitymap_prepare_truncate(&relation, HEAPBLOCKS_PER_PAGE), 1);
	UT_ASSERT_EQ(identities + reads + prepared + dirty + wal_records, 0);
	UT_ASSERT(memcmp(before.data, vm.data, BLCKSZ) == 0);
}

UT_TEST(test_truncate_bad_identity_or_token_fails_before_mutation)
{
	for (int bad_token = 0; bad_token < 2; bad_token++) {
		PGAlignedBlock before;
		truncate_page(true);
		identity_ok = bad_token;
		if (bad_token)
			((PageHeader)vm.data)->pd_block_scn = 0;
		before = vm;
		expecting_error = true;
		if (setjmp(error_jump) == 0) {
			(void)visibilitymap_prepare_truncate(&relation, 10);
			UT_ASSERT(false);
		}
		UT_ASSERT(memcmp(before.data, vm.data, BLCKSZ) == 0);
		UT_ASSERT_EQ(dirty + wal_records + legacy_wal, 0);
		UT_ASSERT_EQ(reads, bad_token);
		UT_ASSERT_EQ(CritSectionCount, 0);
	}
}

UT_TEST(test_truncate_native_and_recovery_do_not_read_runtime_identity)
{
	for (int shared = 0; shared < 2; shared++)
		for (int hints = 0; hints < 2; hints++) {
			truncate_page(shared);
			InRecovery = shared;
			wal_log_hints = hints;
			UT_ASSERT_EQ(visibilitymap_prepare_truncate(&relation, 10), 1);
			UT_ASSERT_EQ(identities + prepared + wal_records + version_edges, 0);
			UT_ASSERT_EQ(legacy_wal, !shared && hints);
			UT_ASSERT_EQ(((PageHeader)vm.data)->pd_block_scn, 55);
			UT_ASSERT_EQ((uint8)PageGetContents(vm.data)[2], 0x0f);
			UT_ASSERT_EQ(dirty, 1);
		}
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
	UT_PLAN(14);
	UT_RUN(test_shared_read_observes_zero_without_initialization);
	UT_RUN(test_write_pin_reinitializes_read_only_zero_with_version);
	UT_RUN(test_recent_pin_refuses_unformatted_without_leaking_pin);
	UT_RUN(test_missing_identity_fails_before_vm_access);
	UT_RUN(test_retry_and_remote_initialization_keep_original_ownership);
	UT_RUN(test_truncate_zero_tail_does_not_dirty_an_unformatted_page);
	UT_RUN(test_truncate_changed_tail_has_one_exact_fpi);
	UT_RUN(test_truncate_unchanged_tail_and_page_boundary_are_noops);
	UT_RUN(test_truncate_bad_identity_or_token_fails_before_mutation);
	UT_RUN(test_truncate_native_and_recovery_do_not_read_runtime_identity);
	UT_RUN(test_legacy_read_keeps_native_initialization);
	UT_RUN(test_shared_extension_preserves_existing_and_zero_predecessors);
	UT_RUN(test_shared_extension_corruption_cannot_become_zero_base);
	UT_RUN(test_legacy_extension_keeps_native_zero_on_error);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
