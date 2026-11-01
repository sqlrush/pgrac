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
#include "cluster/cluster_pcm_lock.h"
#include "cluster/cluster_scn.h"
#include "cluster/cluster_space_storage.h"
#include "cluster/cluster_semantic_activation.h"
#include "cluster/storage/cluster_smgr.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/smgr.h"
#include "utils/inval.h"
#include "utils/rel.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

#ifdef FIXTURE_NO_ASSERTS
#undef Assert
#define Assert(condition) ((void)0)
#endif

/* Online page/version fixtures use TARGET_READY; recovered admission is
 * controlled separately and is never granted by these storage leaves. */
BackendType MyBackendType = B_BACKEND;
ClusterNormalStartState
cluster_semantic_normal_start_state(void)
{
	return CLUSTER_NORMAL_START_TARGET_READY;
}
bool
cluster_semantic_activation_phase1_pristine(void)
{
	return false;
}
bool
cluster_semantic_activation_recovered_open_current(void)
{
	return false;
}


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
static RelFileLocator fixture_locator;
static ForkNumber fixture_fork;
static BlockNumber fixture_block;
static unsigned extensions, invalidations;
static unsigned prepared, version_edges, wal_records, legacy_wal;
static uint64 token;
static bool begun;
static RfPageVersionEdgeEntryV1 wal_edge;
static PGAlignedBlock wal_image;
static ReadBufferMode read_mode;
static int last_lock_mode;
static jmp_buf error_jump;
static unsigned retry_replacements, retry_cancel_after, retry_handoffs, retry_cancellations;
static unsigned truncate_epoch, truncate_drift_at, truncate_wrong_tag;
static unsigned truncate_captures, truncate_rechecks;
static unsigned truncate_aux_calls, truncate_aux_retries, truncate_aux_cancel_at;
static bool truncate_aux_consumes, truncate_aux_missing;
static ResourceXAuxiliaryAcquireContext *truncate_aux_context;
volatile sig_atomic_t InterruptPending;

/* A real init handoff has already released the obsolete pin. Cancellation
 * must be observed before the relation-aware loop fetches another mapping. */
void
ProcessInterrupts(void)
{
	if (!expecting_error || pins != 0 || locks != unlocks || CritSectionCount != 0)
		abort();
	InterruptPending = false;
	retry_cancellations++;
	elog(ERROR, "controlled cancellation at unpinned VM initialization retry");
}

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

/* Controlled SPACE publication seam: the original observation cannot be
 * refreshed after the VM read or content lock. The cache implementation is
 * covered separately; these leaves assert the caller's lock/IO schedule. */
bool
cluster_space_relation_identity_capture_v1(Relation rel, ClusterSpaceIdentityObservationV1 *out)
{
	ClusterSpaceIdentity identity;

	truncate_captures++;
	memset(out, 0, sizeof(*out));
	if (!cluster_space_relation_get_identity(rel, &identity))
		return false;
	out->identity = identity;
	out->invalidations = truncate_epoch;
	return true;
}

bool
cluster_space_relation_identity_current_v1(Relation rel,
										   const ClusterSpaceIdentityObservationV1 *expected)
{
	if (rel != &relation || pins != 1 || locks != unlocks + 1
		|| last_lock_mode != BUFFER_LOCK_EXCLUSIVE || CritSectionCount != 0
		|| truncate_captures != 1 || identities != 1)
		abort();
	truncate_rechecks++;
	return identity_ok && expected->invalidations == truncate_epoch
		   && RelFileLocatorEquals(expected->identity.key.locator, relation.rd_locator);
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
	fixture_locator = rel->rd_locator;
	fixture_fork = forknum;
	fixture_block = block;
	read_mode = mode;
	if (truncate_drift_at == 1)
		truncate_epoch++;
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
	return fixture_block;
}

void
BufferGetTag(Buffer buffer, RelFileLocator *locator, ForkNumber *forknum, BlockNumber *block)
{
	if (buffer != 1 || pins == 0)
		abort();
	*locator = fixture_locator;
	*forknum = fixture_fork;
	*block = fixture_block;
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
	if (retry_replacements > 0) {
		retry_replacements--;
		retry_handoffs++;
		ReleaseBuffer(buffer);
		if (retry_cancel_after != 0 && retry_handoffs == retry_cancel_after)
			InterruptPending = true;
		return InvalidBuffer;
	}
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
	if (mode == BUFFER_LOCK_EXCLUSIVE || mode == BUFFER_LOCK_SHARE) {
		last_lock_mode = mode;
		locks++;
		if (truncate_drift_at == 2)
			truncate_epoch++;
		switch (truncate_wrong_tag) {
		case 1: fixture_locator.spcOid++; break;
		case 2: fixture_locator.dbOid++; break;
		case 3: fixture_locator.relNumber++; break;
		case 4: fixture_fork = MAIN_FORKNUM; break;
		case 5: fixture_block++; break;
		}
	} else if (mode == BUFFER_LOCK_UNLOCK)
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

/* Original auxiliary API outcomes, not native transport or refcount proof.
 * A clean refusal owns no content lock and may have consumed the old pin. */
bool
ClusterLockBufferExclusiveAuxiliaryAware(Buffer *buffer, ResourceXAuxiliaryAcquireContext *context)
{
	UT_ASSERT(buffer != NULL && *buffer == 1 && context != NULL);
	UT_ASSERT_EQ(pins, 1);
	UT_ASSERT_EQ(locks, unlocks);
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT(!InRecovery && cluster_shared_config && RelationIsPermanent(&relation));
	truncate_aux_calls++;
	if (truncate_aux_context == NULL) {
		truncate_aux_context = context;
		UT_ASSERT(!context->active);
		UT_ASSERT_EQ(context->absolute_deadline_us, 0);
		context->active = true;
		context->absolute_deadline_us = 12345;
		context->diagnostic_request_sequence = 71;
	} else {
		UT_ASSERT(context == truncate_aux_context);
		UT_ASSERT(context->active);
		UT_ASSERT_EQ(context->absolute_deadline_us, 12345);
		UT_ASSERT_EQ(context->diagnostic_request_sequence, 71);
		UT_ASSERT_EQ(context->reobserve_count, truncate_aux_calls - 1);
	}
	if (truncate_aux_retries > 0) {
		truncate_aux_retries--;
		context->reobserve = true;
		context->reobserve_count++;
		if (truncate_aux_consumes) {
			ReleaseBuffer(*buffer);
			*buffer = InvalidBuffer;
		}
		if (truncate_drift_at == 3)
			truncate_epoch++;
		if (truncate_aux_missing)
			storage.smgr_cached_nblocks[VISIBILITYMAP_FORKNUM] = 0;
		if (truncate_aux_cancel_at == truncate_aux_calls)
			InterruptPending = true;
		return false;
	}
	LockBuffer(*buffer, BUFFER_LOCK_EXCLUSIVE);
	memset(context, 0, sizeof(*context));
	return true;
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

#include "test_cluster_vm_buffer_matches.inc"
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
	last_lock_mode = BUFFER_LOCK_UNLOCK;
	fixture_locator = relation.rd_locator;
	fixture_fork = VISIBILITYMAP_FORKNUM;
	fixture_block = 0;
	retry_replacements = retry_cancel_after = retry_handoffs = retry_cancellations = 0;
	truncate_epoch = 1;
	truncate_drift_at = truncate_wrong_tag = truncate_captures = truncate_rechecks = 0;
	truncate_aux_calls = truncate_aux_retries = truncate_aux_cancel_at = 0;
	truncate_aux_consumes = truncate_aux_missing = false;
	truncate_aux_context = NULL;
	InterruptPending = false;
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

static bool
truncate_raises_error(void)
{
	expecting_error = true;
	if (setjmp(error_jump) == 0) {
		(void)visibilitymap_prepare_truncate(&relation, 10);
		expecting_error = false;
		return false;
	}
	expecting_error = false;
	return true;
}

/* Each corruption independently violates the existing VM page format. */
static void
damage_truncate_layout(unsigned variant)
{
	PageHeader header = (PageHeader)vm.data;

	switch (variant) {
	case 0:
		PageSetPageSizeAndVersion(vm.data, BLCKSZ / 2, PG_PAGE_LAYOUT_VERSION);
		break;
	case 1:
		PageSetPageSizeAndVersion(vm.data, BLCKSZ, 0);
		break;
	case 2:
		header->pd_lower++;
		break;
	case 3:
		header->pd_upper--;
		break;
	case 4:
		header->pd_special--;
		break;
	case 5:
		header->pd_prune_xid = FirstNormalTransactionId;
		break;
	case 6:
		header->pd_flags = PD_HAS_FREE_LINES;
		break;
	case 7:
		header->pd_flags = PD_LSN_ORIGIN_MASK;
		break;
	case 8:
		header->pd_block_scn = 0;
		break;
	case 9:
		header->pd_upper = 0;
		break; /* PageIsNew with a nonzero header. */
	default:
		abort();
	}
}

UT_TEST(test_truncate_rejects_nonpristine_new_tail_before_noop)
{
	for (int recovery = 0; recovery < 2; recovery++)
		for (int position = 0; position < 2; position++) {
			PGAlignedBlock before;

			reset(true);
			InRecovery = recovery;
			vm.data[position ? BLCKSZ - 1 : MAXALIGN(SizeOfPageHeaderData)] = 1;
			before = vm;
			UT_ASSERT(truncate_raises_error());
			UT_ASSERT(memcmp(before.data, vm.data, BLCKSZ) == 0);
			UT_ASSERT_EQ(dirty + init_calls + prepared + wal_records + legacy_wal, 0);
			UT_ASSERT_EQ(CritSectionCount, 0);
			if (pins != 0)
				UnlockReleaseBuffer(1); /* Test leaf, not ResourceOwner unwinding. */
		}
}

UT_TEST(test_truncate_rejects_malformed_layout_before_mutation_or_noop)
{
	for (int recovery = 0; recovery < 2; recovery++)
		for (int changed = 0; changed < 2; changed++)
			for (unsigned variant = 0; variant < 10; variant++) {
				PGAlignedBlock before;

				truncate_page(true);
				InRecovery = recovery;
				if (!changed)
					memset(PageGetContents(vm.data), 0, MAPSIZE);
				damage_truncate_layout(variant);
				before = vm;
				UT_ASSERT(truncate_raises_error());
				UT_ASSERT(memcmp(before.data, vm.data, BLCKSZ) == 0);
				UT_ASSERT_EQ(dirty + init_calls + prepared + wal_records + legacy_wal, 0);
				UT_ASSERT_EQ(CritSectionCount, 0);
				if (pins != 0)
					UnlockReleaseBuffer(1);
			}
}

UT_TEST(test_truncate_rejects_wrong_target_before_mutation_or_noop)
{
	for (int profile = 0; profile < 3; profile++)
		for (unsigned field = 1; field <= 5; field++) {
			PGAlignedBlock before;

			truncate_page(true);
			if (profile == 0)
				memset(vm.data, 0, BLCKSZ);
			if (profile == 1)
				memset(PageGetContents(vm.data), 0, MAPSIZE);
			truncate_wrong_tag = field;
			before = vm;
			UT_ASSERT(truncate_raises_error());
			UT_ASSERT(memcmp(before.data, vm.data, BLCKSZ) == 0);
			UT_ASSERT_EQ(dirty + prepared + wal_records + legacy_wal, 0);
			UT_ASSERT_EQ(CritSectionCount, 0);
			if (pins != 0)
				UnlockReleaseBuffer(1);
		}
}

UT_TEST(test_truncate_cannot_refresh_original_space_after_vm_acquisition)
{
	for (int profile = 0; profile < 3; profile++)
		for (unsigned phase = 1; phase <= 2; phase++) {
			PGAlignedBlock before;

			truncate_page(true);
			if (profile == 0)
				memset(vm.data, 0, BLCKSZ);
			if (profile == 1)
				memset(PageGetContents(vm.data), 0, MAPSIZE);
			truncate_drift_at = phase;
			before = vm;
			UT_ASSERT(truncate_raises_error());
			UT_ASSERT(memcmp(before.data, vm.data, BLCKSZ) == 0);
			UT_ASSERT_EQ(truncate_captures, 1);
			UT_ASSERT_EQ(truncate_rechecks, 1);
			UT_ASSERT_EQ(identities, 1);
			UT_ASSERT_EQ(dirty + prepared + wal_records + legacy_wal, 0);
			UT_ASSERT_EQ(CritSectionCount, 0);
			if (pins != 0)
				UnlockReleaseBuffer(1);
		}
}

UT_TEST(test_truncate_unlogged_keeps_native_profile)
{
	for (int shared = 0; shared < 2; shared++)
		for (int zero = 0; zero < 2; zero++) {
			truncate_page(shared);
			relform.relpersistence = RELPERSISTENCE_UNLOGGED;
			if (zero)
				memset(vm.data, 0, BLCKSZ);
			else
				((PageHeader)vm.data)->pd_block_scn = 0;
			UT_ASSERT_EQ(visibilitymap_prepare_truncate(&relation, 10), 1);
			UT_ASSERT(!PageIsNew(vm.data));
			UT_ASSERT_EQ(((PageHeader)vm.data)->pd_block_scn, 0);
			UT_ASSERT_EQ((uint8)PageGetContents(vm.data)[2], zero ? 0 : 0x0f);
			UT_ASSERT_EQ(read_mode, RBM_ZERO_ON_ERROR);
			UT_ASSERT_EQ(dirty, 1);
			UT_ASSERT_EQ(identities + truncate_captures + truncate_rechecks + prepared + wal_records
							 + legacy_wal + version_edges,
						 0);
			UT_ASSERT_EQ(pins, 0);
			UT_ASSERT_EQ(locks, unlocks);
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

UT_TEST(test_cold_truncate_reads_clear_tail_without_mutation)
{
	const BlockNumber limits[] = { 1, 4, 10, HEAPBLOCKS_PER_PAGE - 1 };
	for (int zero = 0; zero < 2; zero++)
		for (int i = 0; i < lengthof(limits); i++) {
			BlockNumber size = 99, n = limits[i];
			PGAlignedBlock before;

			reset(true);
			InRecovery = true;
			if (!zero) {
				PageInit(vm.data, BLCKSZ, 0);
				memset(PageGetContents(vm.data), 0xff, HEAPBLK_TO_MAPBYTE(n));
				PageGetContents(vm.data)[HEAPBLK_TO_MAPBYTE(n)] = (1 << HEAPBLK_TO_OFFSET(n)) - 1;
				((PageHeader)vm.data)->pd_block_scn = 55;
			}
			before = vm;
			UT_ASSERT(visibilitymap_prepare_cold_truncate(&relation, n, &size));
			UT_ASSERT_EQ(size, 1);
			UT_ASSERT_EQ(reads, 1);
			UT_ASSERT_EQ(read_mode, RBM_NORMAL);
			UT_ASSERT_EQ(last_lock_mode, BUFFER_LOCK_SHARE);
			UT_ASSERT_EQ(pins, 0);
			UT_ASSERT_EQ(locks, unlocks);
			UT_ASSERT_EQ(init_calls + dirty + prepared + wal_records + identities, 0);
			UT_ASSERT(memcmp(before.data, vm.data, BLCKSZ) == 0);
		}
}

UT_TEST(test_cold_truncate_rejects_nonclear_tail_and_wrong_mode)
{
	for (int variant = 0; variant < 6; variant++) {
		BlockNumber size = 99;
		PGAlignedBlock before;

		reset(true);
		InRecovery = true;
		PageInit(vm.data, BLCKSZ, 0);
		if (variant == 0)
			PageGetContents(vm.data)[2] = 0x10;
		if (variant == 1)
			PageGetContents(vm.data)[MAPSIZE - 1] = 1;
		if (variant == 2)
			cluster_shared_config = false;
		if (variant == 3)
			InRecovery = false;
		if (variant == 4)
			relform.relpersistence = RELPERSISTENCE_TEMP;
		if (variant == 5) {
			memset(vm.data, 0, BLCKSZ);
			vm.data[BLCKSZ - 1] = 1; /* A new header is not proof of all-zero bytes. */
		}
		before = vm;
		UT_ASSERT(!visibilitymap_prepare_cold_truncate(&relation, 10, &size));
		UT_ASSERT_EQ(size, 99);
		UT_ASSERT_EQ(init_calls + dirty + prepared + wal_records + identities, 0);
		UT_ASSERT_EQ(pins, 0);
		UT_ASSERT_EQ(locks, unlocks);
		UT_ASSERT(memcmp(before.data, vm.data, BLCKSZ) == 0);
	}
}

UT_TEST(test_cold_truncate_page_boundary_and_short_fork_need_no_read)
{
	const BlockNumber limits[] = { 0, HEAPBLOCKS_PER_PAGE, 2 * HEAPBLOCKS_PER_PAGE + 10 };
	for (int i = 0; i < lengthof(limits); i++) {
		BlockNumber size = 99;

		reset(true);
		InRecovery = true;
		UT_ASSERT(visibilitymap_prepare_cold_truncate(&relation, limits[i], &size));
		UT_ASSERT_EQ(size, i == 2 ? InvalidBlockNumber : (BlockNumber)i);
		UT_ASSERT_EQ(reads + pins + locks + init_calls + dirty, 0);
	}
}

UT_TEST(test_cold_truncate_rejects_clear_tail_with_malformed_layout)
{
	for (unsigned variant = 0; variant < 10; variant++) {
		BlockNumber size = 99;
		PGAlignedBlock before;

		truncate_page(true);
		InRecovery = true;
		memset(PageGetContents(vm.data), 0, MAPSIZE);
		damage_truncate_layout(variant);
		before = vm;
		UT_ASSERT(!visibilitymap_prepare_cold_truncate(&relation, 10, &size));
		UT_ASSERT_EQ(size, 99);
		UT_ASSERT(memcmp(before.data, vm.data, BLCKSZ) == 0);
		UT_ASSERT_EQ(reads, 1);
		UT_ASSERT_EQ(last_lock_mode, BUFFER_LOCK_SHARE);
		UT_ASSERT_EQ(pins, 0);
		UT_ASSERT_EQ(locks, unlocks);
		UT_ASSERT_EQ(
			identities + truncate_captures + truncate_rechecks + dirty + prepared + wal_records, 0);
	}
}

UT_TEST(test_cold_truncate_rejects_clear_tail_with_wrong_target)
{
	for (int zero = 0; zero < 2; zero++)
		for (unsigned field = 1; field <= 5; field++) {
			BlockNumber size = 99;
			PGAlignedBlock before;

			reset(true);
			InRecovery = true;
			if (!zero) {
				PageInit(vm.data, BLCKSZ, 0);
				((PageHeader)vm.data)->pd_block_scn = 55;
			}
			truncate_wrong_tag = field;
			before = vm;
			UT_ASSERT(!visibilitymap_prepare_cold_truncate(&relation, 10, &size));
			UT_ASSERT_EQ(size, 99);
			UT_ASSERT(memcmp(before.data, vm.data, BLCKSZ) == 0);
			UT_ASSERT_EQ(pins, 0);
			UT_ASSERT_EQ(locks, unlocks);
			UT_ASSERT_EQ(identities + truncate_captures + truncate_rechecks + dirty + prepared
							 + wal_records,
						 0);
		}
}

UT_TEST(test_write_pin_replaces_wrong_relation_fork_or_map_block)
{
	for (int field = 0; field < 5; field++) {
		Buffer buffer;

		reset(true);
		PageInit(vm.data, BLCKSZ, 0);
		((PageHeader)vm.data)->pd_block_scn = 55;
		buffer = vm_readbuf(&relation, 0, false);
		switch (field) {
		case 0:
			fixture_locator.spcOid++;
			break;
		case 1:
			fixture_locator.dbOid++;
			break;
		case 2:
			fixture_locator.relNumber++;
			break;
		case 3:
			fixture_fork = MAIN_FORKNUM;
			break;
		case 4:
			fixture_block++;
			break;
		}
		visibilitymap_pin(&relation, 10, &buffer);
		UT_ASSERT_EQ(releases, 1);
		UT_ASSERT_EQ(reads, 2);
		UT_ASSERT_EQ(pins, 1);
		UT_ASSERT(RelFileLocatorEquals(fixture_locator, relation.rd_locator));
		UT_ASSERT_EQ(fixture_fork, VISIBILITYMAP_FORKNUM);
		UT_ASSERT_EQ(fixture_block, 0);
		UT_ASSERT_EQ(dirty + init_calls, 0);
		ReleaseBuffer(buffer);
	}
}

UT_TEST(test_write_pin_reuses_exact_initialized_target_without_io)
{
	Buffer buffer;

	reset(true);
	PageInit(vm.data, BLCKSZ, 0);
	((PageHeader)vm.data)->pd_block_scn = 55;
	buffer = vm_readbuf(&relation, 0, false);
	visibilitymap_pin(&relation, 10, &buffer);
	UT_ASSERT_EQ(reads, 1);
	UT_ASSERT_EQ(identities + releases + locks + dirty + init_calls, 0);
	UT_ASSERT_EQ(pins, 1);
	ReleaseBuffer(buffer);
}

/* Execute the full original VM lookup; only its I/O and init-handoff edges
 * above are controlled. A failed call returns no caller-owned buffer. */
static Buffer
try_vm_init(bool extend)
{
	expecting_error = true;
	if (setjmp(error_jump) == 0) {
		Buffer result = vm_readbuf(&relation, 0, extend);

		expecting_error = false;
		return result;
	}
	expecting_error = false;
	return InvalidBuffer;
}

UT_TEST(test_shared_init_cancel_after_replacement_cannot_fetch_or_publish_again)
{
	for (int missing = 0; missing < 2; missing++) {
		for (int replacements = 1; replacements <= 4; replacements += 3) {
			for (int cancel_at = 1; cancel_at <= replacements; cancel_at += 3) {
				Buffer result;

				reset(true);
				if (missing)
					storage.smgr_cached_nblocks[VISIBILITYMAP_FORKNUM] = 0;
				retry_replacements = replacements;
				retry_cancel_after = cancel_at;
				result = try_vm_init(true);
				UT_ASSERT_EQ(result, InvalidBuffer);
				UT_ASSERT_EQ(retry_cancellations, 1);
				UT_ASSERT_EQ(retry_handoffs, cancel_at);
				UT_ASSERT_EQ(reads, cancel_at);
				UT_ASSERT_EQ(extensions, missing);
				UT_ASSERT_EQ(invalidations, missing);
				UT_ASSERT_EQ(identities, 1);
				UT_ASSERT_EQ(pins + locks + unlocks + init_calls + dirty + wal_records, 0);
				UT_ASSERT_EQ(releases, cancel_at);
				UT_ASSERT(PageIsNew(vm.data));
				UT_ASSERT_EQ(((PageHeader)vm.data)->pd_block_scn, 0);
				if (BufferIsValid(result))
					ReleaseBuffer(result);
			}
		}
	}
}

UT_TEST(test_pending_shared_vm_cancel_precedes_first_storage_access)
{
	for (int extend = 0; extend < 2; extend++) {
		Buffer result;

		reset(true);
		InterruptPending = true;
		result = try_vm_init(extend != 0);
		UT_ASSERT_EQ(result, InvalidBuffer);
		UT_ASSERT_EQ(retry_cancellations, 1);
		UT_ASSERT_EQ(identities, extend);
		UT_ASSERT_EQ(reads + extensions + invalidations + pins + locks + init_calls + dirty, 0);
		UT_ASSERT(PageIsNew(vm.data));
		if (BufferIsValid(result))
			ReleaseBuffer(result);
	}
}

UT_TEST(test_shared_init_replacements_without_cancel_preserve_one_initialization)
{
	for (int missing = 0; missing < 2; missing++) {
		for (int replacements = 1; replacements <= 4; replacements += 3) {
			Buffer result;

			reset(true);
			if (missing)
				storage.smgr_cached_nblocks[VISIBILITYMAP_FORKNUM] = 0;
			retry_replacements = replacements;
			result = try_vm_init(true);
			UT_ASSERT_EQ(result, 1);
			UT_ASSERT_EQ(retry_cancellations, 0);
			UT_ASSERT_EQ(retry_handoffs, replacements);
			UT_ASSERT_EQ(reads, replacements + 1);
			UT_ASSERT_EQ(releases, replacements);
			UT_ASSERT_EQ(extensions, missing);
			UT_ASSERT_EQ(invalidations, missing);
			UT_ASSERT_EQ(identities, 1);
			UT_ASSERT_EQ(pins, 1);
			UT_ASSERT_EQ(locks, 1);
			UT_ASSERT_EQ(unlocks, 1);
			UT_ASSERT_EQ(init_calls, 1);
			UT_ASSERT_EQ(((PageHeader)vm.data)->pd_block_scn, 101);
			ReleaseBuffer(result);
			UT_ASSERT_EQ(pins, 0);
		}
	}
}

UT_TEST(test_nonversioned_vm_lookup_keeps_original_interrupt_and_init_profile)
{
	for (int profile = 0; profile < 2; profile++) {
		Buffer result;

		reset(profile != 0);
		if (profile)
			relform.relpersistence = RELPERSISTENCE_UNLOGGED;
		InterruptPending = true;
		result = try_vm_init(false);
		UT_ASSERT_EQ(result, 1);
		UT_ASSERT_EQ(retry_cancellations + identities + init_calls, 0);
		UT_ASSERT_EQ(reads, 1);
		UT_ASSERT_EQ(read_mode, RBM_ZERO_ON_ERROR);
		UT_ASSERT(!PageIsNew(vm.data));
		UT_ASSERT_EQ(pins, 1);
		UT_ASSERT_EQ(locks, 1);
		UT_ASSERT_EQ(unlocks, 1);
		ReleaseBuffer(result);
		UT_ASSERT_EQ(pins, 0);
	}
}

UT_TEST(test_truncate_reobserves_one_original_auxiliary_operation)
{
	for (int consumed = 0; consumed < 2; consumed++)
		for (unsigned retries = 1; retries <= 4; retries += 3)
			for (int profile = 0; profile < 3; profile++) {
				truncate_page(true);
				if (profile == 1)
					memset(PageGetContents(vm.data), 0, MAPSIZE);
				if (profile == 2)
					memset(vm.data, 0, BLCKSZ);
				truncate_aux_retries = retries;
				truncate_aux_consumes = consumed != 0;
				UT_ASSERT_EQ(visibilitymap_prepare_truncate(&relation, 10), 1);
				UT_ASSERT_EQ(truncate_aux_calls, retries + 1);
				UT_ASSERT_EQ(reads, retries + 1);
				UT_ASSERT_EQ(releases, retries + 1);
				UT_ASSERT_EQ(pins, 0);
				UT_ASSERT_EQ(identities, 1);
				UT_ASSERT_EQ(truncate_captures, 1);
				UT_ASSERT_EQ(truncate_rechecks, 1);
				UT_ASSERT_EQ(locks, 1);
				UT_ASSERT_EQ(unlocks, 1);
				UT_ASSERT_EQ(prepared, profile == 0);
				UT_ASSERT_EQ(wal_records, profile == 0);
				UT_ASSERT_EQ(dirty, profile == 0);
				UT_ASSERT_EQ(init_calls + legacy_wal, 0);
				if (profile == 0) {
					UT_ASSERT_EQ(wal_edge.before.mutation_token, 55);
					UT_ASSERT_EQ((uint8)PageGetContents(vm.data)[2], 0x0f);
					UT_ASSERT_EQ(PageGetContents(vm.data)[MAPSIZE - 1], 0);
				}
			}
}

UT_TEST(test_truncate_cancel_after_refusal_precedes_repin_and_wal)
{
	for (int consumed = 0; consumed < 2; consumed++)
		for (unsigned cancel = 1; cancel <= 4; cancel += 3) {
			PGAlignedBlock before;

			truncate_page(true);
			before = vm;
			truncate_aux_retries = truncate_aux_cancel_at = cancel;
			truncate_aux_consumes = consumed != 0;
			UT_ASSERT(truncate_raises_error());
			UT_ASSERT_EQ(truncate_aux_calls, cancel);
			UT_ASSERT_EQ(reads, cancel);
			UT_ASSERT_EQ(releases, cancel);
			UT_ASSERT_EQ(pins + locks + unlocks, 0);
			UT_ASSERT_EQ(retry_cancellations, 1);
			UT_ASSERT_EQ(truncate_captures, 1);
			UT_ASSERT_EQ(truncate_rechecks, 0);
			UT_ASSERT_EQ(init_calls + dirty + prepared + wal_records + legacy_wal, 0);
			UT_ASSERT(memcmp(before.data, vm.data, BLCKSZ) == 0);
		}
}

UT_TEST(test_truncate_reobservation_cannot_refresh_original_space)
{
	for (int consumed = 0; consumed < 2; consumed++)
		for (int profile = 0; profile < 3; profile++) {
			PGAlignedBlock before;

			truncate_page(true);
			if (profile == 1)
				memset(PageGetContents(vm.data), 0, MAPSIZE);
			if (profile == 2)
				memset(vm.data, 0, BLCKSZ);
			before = vm;
			truncate_aux_retries = 1;
			truncate_aux_consumes = consumed != 0;
			truncate_drift_at = 3;
			UT_ASSERT(truncate_raises_error());
			UT_ASSERT_EQ(truncate_aux_calls, 2);
			UT_ASSERT_EQ(reads, 2);
			UT_ASSERT_EQ(identities, 1);
			UT_ASSERT_EQ(truncate_captures, 1);
			UT_ASSERT_EQ(truncate_rechecks, 1);
			UT_ASSERT_EQ(init_calls + dirty + prepared + wal_records + legacy_wal, 0);
			UT_ASSERT(memcmp(before.data, vm.data, BLCKSZ) == 0);
			if (pins != 0)
				UnlockReleaseBuffer(1); /* Test leaf, not ResourceOwner. */
		}
}

UT_TEST(test_truncate_missing_tail_and_nonversioned_profiles_keep_owners)
{
	for (int consumed = 0; consumed < 2; consumed++) {
		PGAlignedBlock before;

		truncate_page(true);
		before = vm;
		truncate_aux_retries = 1;
		truncate_aux_consumes = consumed != 0;
		truncate_aux_missing = true;
		UT_ASSERT_EQ(visibilitymap_prepare_truncate(&relation, 10), InvalidBlockNumber);
		UT_ASSERT_EQ(truncate_aux_calls, 1);
		UT_ASSERT_EQ(reads, 1);
		UT_ASSERT_EQ(releases, 1);
		UT_ASSERT_EQ(pins + locks + unlocks, 0);
		UT_ASSERT_EQ(init_calls + dirty + prepared + wal_records + legacy_wal, 0);
		UT_ASSERT(memcmp(before.data, vm.data, BLCKSZ) == 0);
	}
	for (int profile = 0; profile < 3; profile++) {
		truncate_page(profile != 0);
		if (profile == 1)
			InRecovery = true;
		if (profile == 2)
			relform.relpersistence = RELPERSISTENCE_UNLOGGED;
		UT_ASSERT_EQ(visibilitymap_prepare_truncate(&relation, 10), 1);
		UT_ASSERT_EQ(truncate_aux_calls, 0);
		UT_ASSERT_EQ(pins, 0);
		UT_ASSERT_EQ(locks, unlocks);
	}
}

int
main(void)
{
	UT_PLAN(34);
	UT_RUN(test_truncate_rejects_nonpristine_new_tail_before_noop);
	UT_RUN(test_truncate_rejects_malformed_layout_before_mutation_or_noop);
	UT_RUN(test_truncate_rejects_wrong_target_before_mutation_or_noop);
	UT_RUN(test_truncate_cannot_refresh_original_space_after_vm_acquisition);
	UT_RUN(test_cold_truncate_rejects_clear_tail_with_malformed_layout);
	UT_RUN(test_cold_truncate_rejects_clear_tail_with_wrong_target);
	UT_RUN(test_truncate_unlogged_keeps_native_profile);
	UT_RUN(test_shared_init_cancel_after_replacement_cannot_fetch_or_publish_again);
	UT_RUN(test_pending_shared_vm_cancel_precedes_first_storage_access);
	UT_RUN(test_shared_init_replacements_without_cancel_preserve_one_initialization);
	UT_RUN(test_nonversioned_vm_lookup_keeps_original_interrupt_and_init_profile);
	UT_RUN(test_cold_truncate_reads_clear_tail_without_mutation);
	UT_RUN(test_cold_truncate_rejects_nonclear_tail_and_wrong_mode);
	UT_RUN(test_cold_truncate_page_boundary_and_short_fork_need_no_read);
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
	UT_RUN(test_write_pin_replaces_wrong_relation_fork_or_map_block);
	UT_RUN(test_write_pin_reuses_exact_initialized_target_without_io);
	UT_RUN(test_truncate_reobserves_one_original_auxiliary_operation);
	UT_RUN(test_truncate_cancel_after_refusal_precedes_repin_and_wal);
	UT_RUN(test_truncate_reobservation_cannot_refresh_original_space);
	UT_RUN(test_truncate_missing_tail_and_nonversioned_profiles_keep_owners);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
