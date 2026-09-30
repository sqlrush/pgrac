/*-------------------------------------------------------------------------
 *
 * test_cluster_space_cache.c
 *    Execute native DML identity consumers with real SPACE decoding.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_space_cache.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include <setjmp.h>

#include "access/xlog.h"
#include "access/xact.h"
#include "access/htup_details.h"
#include "access/xloginsert.h"
#include "catalog/pg_control.h"
#include "catalog/pg_class.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_itl_cleanout.h"
#include "cluster/cluster_itl.h"
#include "cluster/cluster_pcm_own.h"
#include "cluster/cluster_pcm_x_bufmgr.h"
#include "cluster/cluster_semantic_activation.h"
#include "cluster/cluster_tx_resolve.h"
#include "cluster/cluster_xid_stripe.h"
#include "access/heapam.h"
#include "cluster/cluster_space_storage.h"
#include "cluster/cluster_wal_thread.h"
#include "cluster/storage/cluster_smgr.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/buf_internals.h"
#include "storage/proc.h"
#include "storage/fsm_internals.h"
#include "storage/smgr.h"
#include "storage/shmem.h"
#include "storage/s_lock.h"
#include "utils/catcache.h"
#include "utils/inval.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

bool cluster_enabled = true, cluster_shared_config = true;
int cluster_node_id = 0, wal_level = WAL_LEVEL_REPLICA;
int NBuffers = 1, NLocBuffer;
char *BufferBlocks;
Block *LocalBufferBlockPointers;
volatile sig_atomic_t InterruptPending;

/* Keep PostgreSQL's real dynahash; only its process memory/error boundaries
 * and the invalidation transport are fixtures. */
static MemoryContextData memory_context = { .type = T_AllocSetContext };
MemoryContext TopMemoryContext = &memory_context;
MemoryContext CacheMemoryContext;
static RelcacheCallbackFunction invalidate;
static Datum invalidate_arg;

static PGAlignedBlock page;
static SMgrRelationData smgr;
static RelationData relation_data;
static FormData_pg_class catalog;
static ClusterWalSourceRef ref;
static ClusterSpaceIdentity disk_identity;
static unsigned exists_calls, size_calls, read_calls;
static bool pinned, locked, recovering, have_ref, have_space, invalidate_on_release;
volatile uint32 CritSectionCount;
bool cluster_recmerge_window_active, cluster_recmerge_apply_foreign;
uint64 cluster_recmerge_window_scn, cluster_recmerge_window_own_lsn;
static bool content_x, writer_allowed = true, commit_pending, foreign_xid;
static uint32 hint_own_flags;
static uint64 hint_writer_token, hint_activation_generation;
static bool xmax_committed;
static unsigned hint_calls, dirty_calls, wal_calls, version_edges;
static uint64 next_token;
static RfPageVersionEdgeEntryV1 recorded_edge;
static PGAlignedBlock hint_before, hint_image;
static bool expected_error;
static jmp_buf error_jump;
static bool lazy_lock_available;
static unsigned lazy_unlocks;
static bool native_hint_fpi;
static ForkNumber hint_fork = MAIN_FORKNUM;
static PGPROC process;
PGPROC *MyProc = &process;
static bool truncate_hints_needed;
bool InRecovery;
static uint8 expected_xlog_info = XLOG_FPI_FOR_HINT;
static BufferDescPadded hint_descriptor[1];
BufferDescPadded *BufferDescriptors = hint_descriptor;

bool
LWLockHeldByMeInMode(LWLock *lock, LWLockMode mode)
{
	UT_ASSERT(lock == BufferDescriptorGetContentLock(GetBufferDescriptor(0)));
	UT_ASSERT_EQ(mode, LW_EXCLUSIVE);
	return content_x;
}

bool
cluster_xid_foreign_class_cheap(TransactionId xid)
{
	return foreign_xid;
}
uint32
LockBufHdr(BufferDesc *desc)
{
	uint32 state = pg_atomic_read_u32(&desc->state);
	UT_ASSERT(desc == GetBufferDescriptor(0));
	UT_ASSERT(!(state & BM_LOCKED));
	pg_atomic_write_u32(&desc->state, state | BM_LOCKED);
	return state;
}
/* Runtime authority is the fixture boundary. Both production samplers and
 * their shared mutation predicate execute unchanged. */
bool cluster_read_scache;
#define cluster_pcm_is_active() true
#define cluster_bufmgr_should_pcm_track(buf) true
#define cluster_bufmgr_pcm_x_retained_image_locked(buf, state) (!writer_allowed)
#define cluster_pcm_own_flags_get(id) hint_own_flags
#define cluster_pcm_own_writer_activation_token_get(id) hint_writer_token
#define cluster_pcm_own_resource_x_activation_generation_get(id) hint_activation_generation
#include "test_cluster_space_hint_write_gate.inc"
#undef cluster_pcm_is_active
#undef cluster_bufmgr_should_pcm_track
#undef cluster_bufmgr_pcm_x_retained_image_locked
#undef cluster_pcm_own_flags_get
#undef cluster_pcm_own_writer_activation_token_get
#undef cluster_pcm_own_resource_x_activation_generation_get
bool
BufferIsPermanent(Buffer buffer)
{
	UT_ASSERT_EQ(buffer, 1);
	return true;
}
void
BufferGetTag(Buffer buffer, RelFileLocator *locator, ForkNumber *forknum, BlockNumber *block)
{
	UT_ASSERT_EQ(buffer, 1);
	*locator = relation_data.rd_locator;
	*forknum = hint_fork;
	*block = 7;
}
XLogRecPtr
TransactionIdGetCommitLSN(TransactionId xid)
{
	return 100;
}
bool
TransactionIdDidCommit(TransactionId xid)
{
	return xmax_committed;
}
bool
XLogNeedsFlush(XLogRecPtr lsn)
{
	return commit_pending;
}
XLogRecPtr
BufferGetLSNAtomic(Buffer buffer)
{
	return PageGetLSN(page.data);
}
SCN
cluster_scn_advance(void)
{
	UT_ASSERT((content_x || hint_fork == FSM_FORKNUM) && CritSectionCount == 0);
	UT_ASSERT(memcmp(hint_before.data, page.data, BLCKSZ) == 0);
	return ++next_token;
}
void
MarkBufferDirtyHint(Buffer buffer, bool standard)
{
	UT_ASSERT(buffer == 1 && standard);
	hint_calls++;
}
void
MarkBufferDirty(Buffer buffer)
{
	UT_ASSERT(buffer == 1 && content_x && CritSectionCount == 1);
	dirty_calls++;
}
void
XLogBeginInsert(void)
{
	if (native_hint_fpi)
		UT_ASSERT_EQ(CritSectionCount, 0);
	else
		UT_ASSERT(CritSectionCount == 1 && dirty_calls == wal_calls + 1);
}
void
XLogRegisterBuffer(uint8 id, Buffer buffer, uint8 flags)
{
	UT_ASSERT(id == 0 && buffer == 1);
	UT_ASSERT_EQ(flags, REGBUF_FORCE_IMAGE | (hint_fork == FSM_FORKNUM ? 0 : REGBUF_STANDARD));
	hint_image = page;
}
void
XLogRegisterPageVersionEdge(uint64 token, const RfPageVersionEdgeEntryV1 *entries, uint8 count)
{
	UT_ASSERT_EQ(count, 1);
	UT_ASSERT_EQ(token, 101);
	recorded_edge = entries[0];
	version_edges++;
}
void
XLogRegisterBlock(uint8 id, RelFileLocator *locator, ForkNumber forknum, BlockNumber block,
				  char *data, uint8 flags)
{
	UT_ASSERT(native_hint_fpi && id == 0 && flags == 0);
	UT_ASSERT_EQ(forknum, hint_fork);
	UT_ASSERT(RelFileLocatorEquals(*locator, relation_data.rd_locator));
	memcpy(hint_image.data, data, BLCKSZ);
}
XLogRecPtr
GetRedoRecPtr(void)
{
	return 1000;
}
XLogRecPtr
XLogInsert(RmgrId rmid, uint8 info)
{
	UT_ASSERT(rmid == RM_XLOG_ID && info == expected_xlog_info);
	wal_calls++;
	return 200;
}
XLogRecPtr
log_newpage_buffer(Buffer buffer, bool standard)
{
	UT_ASSERT(buffer == 1 && !standard);
	hint_image = page;
	wal_calls++;
	PageSetLSN(page.data, 200);
	return 200;
}
int
errdetail(const char *fmt, ...)
{
	return 0;
}
bool errstart(int level, const char *domain) { return level >= ERROR; }
bool errstart_cold(int level, const char *domain) { return errstart(level, domain); }
int errmsg_internal(const char *fmt, ...) { return 0; }
void errfinish(const char *file, int line, const char *func)
{
	if (!expected_error)
		abort();
	longjmp(error_jump, 1);
}
#include "test_cluster_space_hint_owners.inc"
#include "test_cluster_space_xmax_hint.inc"
#include "test_cluster_space_fsm_hint.inc"

static void
fsm_truncate_publication(void)
{
	Relation rel = &relation_data;
	Buffer buf = 1;
	uint16 first_removed_slot = 3;
	RfPageProducerBatchV1 hint_batch;
	bool versioned;

#undef XLogHintBitIsNeeded
#define XLogHintBitIsNeeded() truncate_hints_needed
#include "test_cluster_space_fsm_truncate.inc"
#undef XLogHintBitIsNeeded
}
#include "test_cluster_space_census_types.inc"

bool
cluster_semantic_activation_recheck_r4_terminal_census(const ClusterSemanticAdmissionToken *token)
{
	return true;
}

/* Actual publication after census qualification, covered by the native
 * lock-order fixture. Slot and tuple publication here is production code. */
static ClusterHeapItlTerminalBatchApplyResult
terminal_census_publish(Buffer buffer, const ClusterHeapItlTerminalCensus *census,
						 const uint8 *terminal_flags)
{
	ClusterHeapItlTerminalBatchApplyResult result = {CLUSTER_HEAP_ITL_BATCH_REFUSED, 0, 0};
	RfPageProducerBatchV1 hint_batch pg_attribute_unused();
	ClusterSpaceHintResult hint_result pg_attribute_unused() = CLUSTER_SPACE_HINT_NATIVE;
	bool tuple_refs_changed = false;
	uint8 i;
#include "test_cluster_space_census_mutation.inc"
}

void
ProcessInterrupts(void)
{
	abort();
}

void
CreateCacheMemoryContext(void)
{
	CacheMemoryContext = &memory_context;
}

MemoryContext
AllocSetContextCreateInternal(MemoryContext parent, const char *name, Size min, Size init, Size max)
{
	if (parent != &memory_context)
		abort();
	return &memory_context;
}

void *
MemoryContextAlloc(MemoryContext context, Size size)
{
	if (context != &memory_context)
		abort();
	return malloc(size);
}

void *
MemoryContextAllocExtended(MemoryContext context, Size size, int flags)
{
	return MemoryContextAlloc(context, size);
}

void
MemoryContextSetIdentifier(MemoryContext context, const char *name)
{
	if (context != &memory_context)
		abort();
}

void
pfree(void *ptr)
{
	free(ptr);
}

Size
add_size(Size a, Size b)
{
	if (a > SIZE_MAX - b)
		abort();
	return a + b;
}

Size
mul_size(Size a, Size b)
{
	if (b != 0 && a > SIZE_MAX / b)
		abort();
	return a * b;
}

int
GetCurrentTransactionNestLevel(void)
{
	return 1;
}

int
s_lock(volatile slock_t *lock, const char *file, int line, const char *func)
{
	/* A local cache must never enter the shared-hash contention boundary. */
	abort();
}

int
errcode(int sqlerrcode)
{
	return 0;
}

int
errmsg(const char *fmt, ...)
{
	return 0;
}

void
CacheRegisterRelcacheCallback(RelcacheCallbackFunction callback, Datum arg)
{
	if (invalidate != NULL)
		abort();
	invalidate = callback;
	invalidate_arg = arg;
}

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	printf("# Unexpected assertion: %s at %s:%d\n", condition, file, line);
	abort();
}

bool
RecoveryInProgress(void)
{
	return recovering;
}

bool
cluster_wal_thread_current_v2_ref(ClusterWalSourceRef *out)
{
	*out = ref;
	return have_ref;
}

bool
cluster_wal_thread_restart_v2_ref(ClusterWalSourceRef *out)
{
	abort();
}

int
cluster_smgr_which_for(RelFileLocator locator, BackendId backend)
{
	if (!RelFileLocatorEquals(locator, relation_data.rd_locator) || backend != InvalidBackendId)
		abort();
	return 1;
}

SMgrRelation
smgropen(RelFileLocator locator, BackendId backend)
{
	if (!RelFileLocatorEquals(locator, relation_data.rd_locator) || backend != InvalidBackendId)
		abort();
	return &smgr;
}

bool
smgrexists(SMgrRelation rel, ForkNumber forknum)
{
	if (rel != &smgr || forknum != SPACE_FORKNUM)
		abort();
	exists_calls++;
	return have_space;
}

BlockNumber
smgrnblocks(SMgrRelation rel, ForkNumber forknum)
{
	if (rel != &smgr || forknum != SPACE_FORKNUM)
		abort();
	size_calls++;
	return 1;
}

Buffer
ReadBufferWithoutRelcache(RelFileLocator locator, ForkNumber forknum, BlockNumber block,
						  ReadBufferMode mode, BufferAccessStrategy strategy, bool permanent)
{
	if (!RelFileLocatorEquals(locator, relation_data.rd_locator) || forknum != SPACE_FORKNUM
		|| block != 0 || mode != RBM_NORMAL || strategy != NULL || !permanent || pinned)
		abort();
	read_calls++;
	pinned = true;
	return 1;
}

void
LockBuffer(Buffer buffer, int mode)
{
	if (mode == BUFFER_LOCK_UNLOCK && content_x && !pinned) {
		content_x = false;
		lazy_unlocks++;
		return;
	}
	if (buffer != 1 || !pinned || locked || mode != BUFFER_LOCK_SHARE)
		abort();
	locked = true;
}

bool
ConditionalLockBuffer(Buffer buffer)
{
	UT_ASSERT(buffer == 1 && !content_x);
	content_x = lazy_lock_available;
	return content_x;
}

BlockNumber
BufferGetBlockNumber(Buffer buffer)
{
	if (buffer != 1 || !pinned || !locked)
		abort();
	return 0;
}

void
UnlockReleaseBuffer(Buffer buffer)
{
	if (buffer != 1 || !pinned || !locked)
		abort();
	pinned = locked = false;
	if (invalidate_on_release) {
		invalidate_on_release = false;
		disk_identity.incarnation[0] = 0x67;
		disk_identity.sequence = 2;
		disk_identity.operation = 20;
		if (!cluster_space_identity_page_encode(&disk_identity, 20, page.data, BLCKSZ))
			abort();
		invalidate(invalidate_arg, relation_data.rd_id);
	}
}

/* Original DML and allocation prologues; no copied routing policy. */
static ClusterSpaceIdentity
insert_identity(Relation relation)
{
	ClusterSpaceIdentity cluster_page_identity;
	bool cluster_page_versioned = false;

	memset(&cluster_page_identity, 0, sizeof(cluster_page_identity));
#include "test_cluster_space_cache_insert.inc"
	if (!cluster_page_versioned)
		abort();
	return cluster_page_identity;
}

static ClusterSpaceIdentity
delete_identity(Relation relation)
{
	ClusterSpaceIdentity cluster_page_identity;
	bool cluster_page_versioned = false;

	memset(&cluster_page_identity, 0, sizeof(cluster_page_identity));
#include "test_cluster_space_cache_delete.inc"
	if (!cluster_page_versioned)
		abort();
	return cluster_page_identity;
}

static ClusterSpaceIdentity
update_identity(Relation relation)
{
	ClusterSpaceIdentity cluster_page_identity;
	bool cluster_page_versioned = false;

	memset(&cluster_page_identity, 0, sizeof(cluster_page_identity));
#include "test_cluster_space_cache_update.inc"
	if (!cluster_page_versioned)
		abort();
	return cluster_page_identity;
}

static ClusterSpaceIdentity
allocation_identity(Relation relation)
{
	ClusterSpaceIdentity page_identity_storage;
	const struct ClusterSpaceIdentity *page_identity = NULL;

#include "test_cluster_space_cache_allocation.inc"
	if (page_identity == NULL)
		abort();
	return *page_identity;
}

static ClusterSpaceIdentity
btree_build_identity(Relation relation)
{
	struct {
		Relation index;
		bool btws_use_wal, versioned;
		ClusterSpaceIdentity identity;
	} wstate;

	memset(&wstate, 0, sizeof(wstate));
	wstate.index = relation;
	wstate.btws_use_wal = RelationNeedsWAL(relation);
#include "test_cluster_btree_build_identity.inc"
	UT_ASSERT(wstate.versioned);
	return wstate.identity;
}

static void
setup(void)
{
	cluster_shared_config = true;
	if (invalidate != NULL)
		invalidate(invalidate_arg, InvalidOid);
	memset(&relation_data, 0, sizeof(relation_data));
	memset(&catalog, 0, sizeof(catalog));
	memset(&ref, 0, sizeof(ref));
	memset(&disk_identity, 0, sizeof(disk_identity));
	relation_data.rd_locator = (RelFileLocator){ 1663, 5, 16384 };
	relation_data.rd_id = 16384;
	relation_data.rd_isvalid = true;
	relation_data.rd_rel = &catalog;
	catalog.relpersistence = RELPERSISTENCE_PERMANENT;
	ref.claim.identity.system_identifier = 42;
	ref.claim.database_incarnation = 3;
	memset(ref.claim.identity.storage_uuid, 0x23, 16);
	disk_identity.key.system_identifier = 42;
	disk_identity.key.database_incarnation = 3;
	memset(disk_identity.key.storage_uuid, 0x23, 16);
	disk_identity.key.locator = relation_data.rd_locator;
	memset(disk_identity.incarnation, 0x45, 16);
	disk_identity.sequence = 1;
	disk_identity.operation = 19;
	disk_identity.state = CLUSTER_SPACE_IDENTITY_LIVE;
	if (!cluster_space_identity_page_encode(&disk_identity, 19, page.data, BLCKSZ))
		abort();
	BufferBlocks = page.data;
	exists_calls = size_calls = read_calls = 0;
	pinned = locked = false;
	recovering = invalidate_on_release = false;
	have_ref = have_space = true;
}

UT_TEST(test_insert_allocation_and_delete_share_one_identity_read)
{
	ClusterSpaceIdentity insert, allocation, deletion;
	int i;

	setup();
	insert = insert_identity(&relation_data);
	allocation = allocation_identity(&relation_data);
	deletion = delete_identity(&relation_data);
	UT_ASSERT_EQ(insert.incarnation[0], 0x45);
	UT_ASSERT_EQ(allocation.sequence, 1);
	UT_ASSERT_EQ(deletion.operation, 19);
	for (i = 0; i < 100; i++) {
		ClusterSpaceIdentity repeated
			= i % 2 ? update_identity(&relation_data) : insert_identity(&relation_data);

		UT_ASSERT_EQ(repeated.incarnation[0], 0x45);
	}
	UT_ASSERT_EQ(exists_calls, 1);
	UT_ASSERT_EQ(size_calls, 1);
	UT_ASSERT_EQ(read_calls, 1);
	UT_ASSERT(!pinned && !locked);
}

UT_TEST(test_targeted_invalidation_reloads_new_incarnation)
{
	ClusterSpaceIdentity out;

	setup();
	(void)insert_identity(&relation_data);
	disk_identity.incarnation[0] = 0x66;
	disk_identity.sequence = 2;
	disk_identity.operation = 20;
	UT_ASSERT(cluster_space_identity_page_encode(&disk_identity, 20, page.data, BLCKSZ));
	invalidate(invalidate_arg, relation_data.rd_id);
	out = delete_identity(&relation_data);
	UT_ASSERT_EQ(out.incarnation[0], 0x66);
	UT_ASSERT_EQ(out.sequence, 2);
	UT_ASSERT_EQ(read_calls, 2);
	(void)allocation_identity(&relation_data);
	UT_ASSERT_EQ(read_calls, 2);
}

UT_TEST(test_unrelated_invalidation_preserves_hit_global_reset_drops_it)
{
	setup();
	(void)insert_identity(&relation_data);
	invalidate(invalidate_arg, 20000);
	(void)insert_identity(&relation_data);
	UT_ASSERT_EQ(read_calls, 1);
	invalidate(invalidate_arg, InvalidOid);
	(void)insert_identity(&relation_data);
	UT_ASSERT_EQ(read_calls, 2);
}

UT_TEST(test_locator_and_namespace_changes_cannot_hit_old_cache)
{
	ClusterSpaceIdentity out;
	int change;

	setup();
	(void)insert_identity(&relation_data);
	for (change = 0; change < 4; change++) {
		switch (change) {
		case 0:
			disk_identity.key.locator.relNumber++;
			relation_data.rd_locator = disk_identity.key.locator;
			break;
		case 1:
			ref.claim.identity.system_identifier = ++disk_identity.key.system_identifier;
			break;
		case 2:
			ref.claim.database_incarnation = ++disk_identity.key.database_incarnation;
			break;
		case 3:
			ref.claim.identity.storage_uuid[0] = ++disk_identity.key.storage_uuid[0];
			break;
		}
		disk_identity.incarnation[0]++;
		UT_ASSERT(cluster_space_identity_page_encode(&disk_identity, 19, page.data, BLCKSZ));
		UT_ASSERT(cluster_space_relation_get_identity(&relation_data, &out));
		UT_ASSERT_EQ(out.incarnation[0], 0x46 + change);
		UT_ASSERT_EQ(read_calls, change + 2);
	}
}

UT_TEST(test_invalidation_during_read_reobserves_instead_of_publishing_old_value)
{
	ClusterSpaceIdentity out;

	setup();
	invalidate_on_release = true;
	UT_ASSERT(cluster_space_relation_get_identity(&relation_data, &out));
	UT_ASSERT_EQ(out.incarnation[0], 0x67);
	UT_ASSERT_EQ(out.sequence, 2);
	UT_ASSERT_EQ(read_calls, 2);
	(void)insert_identity(&relation_data);
	UT_ASSERT_EQ(read_calls, 2);
	UT_ASSERT(!pinned && !locked);
}

UT_TEST(test_invalid_relcache_cannot_resurrect_a_stale_entry)
{
	ClusterSpaceIdentity out, saved;

	setup();
	(void)insert_identity(&relation_data);
	memset(&out, 0x55, sizeof(out));
	saved = out;
	relation_data.rd_isvalid = false;
	UT_ASSERT(!cluster_space_relation_get_identity(&relation_data, &out));
	UT_ASSERT(memcmp(&out, &saved, sizeof(out)) == 0);
	relation_data.rd_isvalid = true;
	UT_ASSERT(cluster_space_relation_get_identity(&relation_data, &out));
	UT_ASSERT_EQ(read_calls, 2);
}

UT_TEST(test_unavailable_authority_and_recovery_do_not_reuse_cache)
{
	ClusterSpaceIdentity out, saved;

	setup();
	(void)insert_identity(&relation_data);
	memset(&out, 0x55, sizeof(out));
	saved = out;
	have_ref = false;
	UT_ASSERT(!cluster_space_relation_get_identity(&relation_data, &out));
	have_ref = true;
	recovering = true;
	UT_ASSERT(!cluster_space_relation_get_identity(&relation_data, &out));
	recovering = false;
	catalog.relpersistence = RELPERSISTENCE_UNLOGGED;
	UT_ASSERT(!cluster_space_relation_get_identity(&relation_data, &out));
	UT_ASSERT(memcmp(&out, &saved, sizeof(out)) == 0);
	UT_ASSERT_EQ(read_calls, 1);
}

UT_TEST(test_missing_corrupt_and_tombstoned_identity_never_poison_cache)
{
	ClusterSpaceIdentity out, saved;

	setup();
	memset(&out, 0x55, sizeof(out));
	saved = out;
	have_space = false;
	UT_ASSERT(!cluster_space_relation_get_identity(&relation_data, &out));
	have_space = true;
	page.data[60] ^= 1;
	UT_ASSERT(!cluster_space_relation_get_identity(&relation_data, &out));
	UT_ASSERT_EQ(read_calls, 1);
	disk_identity.state = CLUSTER_SPACE_IDENTITY_TOMBSTONED;
	disk_identity.sequence = 2;
	disk_identity.operation = 20;
	UT_ASSERT(cluster_space_identity_page_encode(&disk_identity, 20, page.data, BLCKSZ));
	UT_ASSERT(!cluster_space_relation_get_identity(&relation_data, &out));
	UT_ASSERT_EQ(read_calls, 2);
	UT_ASSERT(memcmp(&out, &saved, sizeof(out)) == 0);
	disk_identity.state = CLUSTER_SPACE_IDENTITY_LIVE;
	UT_ASSERT(cluster_space_identity_page_encode(&disk_identity, 20, page.data, BLCKSZ));
	UT_ASSERT(cluster_space_relation_get_identity(&relation_data, &out));
	UT_ASSERT_EQ(read_calls, 3);
	UT_ASSERT(!pinned && !locked);
}

UT_TEST(test_btree_build_captures_one_cached_identity_value)
{
	ClusterSpaceIdentity first, second;

	setup();
	first = btree_build_identity(&relation_data);
	second = btree_build_identity(&relation_data);
	UT_ASSERT_EQ(read_calls, 1);
	UT_ASSERT(memcmp(&first, &second, sizeof(first)) == 0);
	invalidate((Datum)0, RelationGetRelid(&relation_data));
	UT_ASSERT(memcmp(&first, &disk_identity, sizeof(first)) == 0);
	UT_ASSERT(!pinned && !locked);
}

static HeapTupleHeader
hint_setup(bool cache)
{
	HeapTupleHeader tuple;
	setup();
	if (cache)
		(void) insert_identity(&relation_data);
	PageInitHeapPage(page.data, BLCKSZ, 0);
	((PageHeader) page.data)->pd_block_scn = 20;
	tuple = (HeapTupleHeader) (page.data + MAXALIGN(SizeOfPageHeaderData));
	HeapTupleHeaderSetXmin(tuple, 77);
	HeapTupleHeaderSetXmax(tuple, 78);
	((PageHeader) page.data)->pd_lower += MAXALIGN(SizeofHeapTupleHeader);
	hint_before = page;
	hint_calls = dirty_calls = wal_calls = version_edges = 0;
	next_token = 100;
	content_x = writer_allowed = true;
	hint_descriptor[0].bufferdesc.pcm_state = PCM_STATE_X;
	hint_own_flags = 0;
	hint_writer_token = hint_activation_generation = 0;
	commit_pending = foreign_xid = false;
	xmax_committed = false;
	CritSectionCount = 0;
	expected_error = false;
	lazy_lock_available = true;
	lazy_unlocks = 0;
	native_hint_fpi = false;
	hint_fork = MAIN_FORKNUM;
	InRecovery = false;
	truncate_hints_needed = true;
	expected_xlog_info = XLOG_FPI_FOR_HINT;
	return tuple;
}

static void
check_hint_version(void)
{
	UT_ASSERT_EQ(version_edges, 1);
	UT_ASSERT_EQ(wal_calls, 1);
	UT_ASSERT_EQ(dirty_calls, 1);
	UT_ASSERT_EQ(hint_calls, 0);
	UT_ASSERT_EQ(next_token, 101);
	UT_ASSERT_EQ(recorded_edge.before.mutation_token, 20);
	UT_ASSERT(memcmp(recorded_edge.result_incarnation, disk_identity.incarnation, 16) == 0);
	UT_ASSERT_EQ(((PageHeader)page.data)->pd_block_scn, 101);
	UT_ASSERT_EQ(((PageHeader)hint_image.data)->pd_block_scn, 101);
	UT_ASSERT_EQ(PageGetLSN(page.data), 200);
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT_EQ(read_calls, 1);
}

UT_TEST(test_hint_share_lock_never_changes_shared_page)
{
	HeapTupleHeader tuple = hint_setup(true);
	content_x = false;
	SetHintBits(tuple, 1, HEAP_XMIN_COMMITTED, InvalidTransactionId);
	UT_ASSERT(memcmp(hint_before.data, page.data, BLCKSZ) == 0);
	UT_ASSERT_EQ(hint_calls + dirty_calls + wal_calls + version_edges, 0);
	UT_ASSERT_EQ(read_calls, 1);
}
UT_TEST(test_hint_exclusive_cached_identity_versions_before_store)
{
	HeapTupleHeader tuple = hint_setup(true);
	SetHintBits(tuple, 1, HEAP_XMIN_COMMITTED, InvalidTransactionId);
	check_hint_version();
	UT_ASSERT(HeapTupleHeaderXminCommitted(tuple));
	SetHintBits(tuple, 1, HEAP_XMIN_COMMITTED, InvalidTransactionId);
	check_hint_version();
}
UT_TEST(test_hint_refusal_has_no_io_or_mutation)
{
	for (int bad = 0; bad < 6; bad++) {
		HeapTupleHeader tuple = hint_setup(bad != 0);
		switch (bad) {
		case 0: break;
		case 1: invalidate(invalidate_arg, relation_data.rd_id); break;
		case 2: ref.claim.database_incarnation++; break;
		case 3: writer_allowed = false; break;
		case 4: recovering = true; break;
		case 5: CritSectionCount = 1; break;
		}
		SetHintBits(tuple, 1, HEAP_XMIN_COMMITTED, InvalidTransactionId);
		UT_ASSERT(memcmp(hint_before.data, page.data, BLCKSZ) == 0);
		UT_ASSERT_EQ(hint_calls + dirty_calls + wal_calls + version_edges, 0);
		UT_ASSERT_EQ(read_calls, bad != 0 ? 1 : 0);
		UT_ASSERT_EQ(next_token, 100);
		CritSectionCount = 0;
	}
}
UT_TEST(test_hint_rejects_dirty_gate_difference_before_critical_section)
{
	for (int bad = 0; bad < 5; bad++) {
		HeapTupleHeader tuple = hint_setup(true);
		BufferDesc *desc = GetBufferDescriptor(0);

		switch (bad) {
		case 0: desc->pcm_state = PCM_STATE_N; break;
		case 1: desc->pcm_state = PCM_STATE_S; break;
		case 2: hint_writer_token = 1; break;
		case 3: hint_activation_generation = 1; break;
		case 4: hint_own_flags = PCM_OWN_FLAG_GRANT_PENDING; break;
		}
		UT_ASSERT(cluster_bufmgr_block_write_permitted(1));
		UT_ASSERT(!cluster_pcm_x_content_holder_mutation_allowed(
			true, true, false, desc->pcm_state, hint_own_flags,
			hint_writer_token, hint_activation_generation));
		SetHintBits(tuple, 1, HEAP_XMIN_COMMITTED, InvalidTransactionId);
		UT_ASSERT(memcmp(hint_before.data, page.data, BLCKSZ) == 0);
		UT_ASSERT_EQ(hint_calls + dirty_calls + wal_calls + version_edges, 0);
		UT_ASSERT_EQ(next_token, 100);
		UT_ASSERT_EQ(CritSectionCount, 0);
	}
}
UT_TEST(test_hint_existing_content_holder_can_finish_revoking_x)
{
	HeapTupleHeader tuple = hint_setup(true);
	hint_own_flags = PCM_OWN_FLAG_REVOKING;
	SetHintBits(tuple, 1, HEAP_XMIN_COMMITTED, InvalidTransactionId);
	check_hint_version();
	UT_ASSERT(HeapTupleHeaderXminCommitted(tuple));
}
UT_TEST(test_released_xmax_is_not_skipped_with_shared_identity)
{
	HeapTupleHeader tuple = hint_setup(true);
	cluster_heap_stamp_released_xmax_invalid(tuple, 1);
	check_hint_version();
	UT_ASSERT(tuple->t_infomask & HEAP_XMAX_INVALID);
}
UT_TEST(test_hint_native_and_commit_interlock_remain)
{
	HeapTupleHeader tuple = hint_setup(false);
	cluster_shared_config = false;
	content_x = false;
	SetHintBits(tuple, 1, HEAP_XMIN_COMMITTED, 77);
	UT_ASSERT(HeapTupleHeaderXminCommitted(tuple));
	UT_ASSERT_EQ(hint_calls, 1);
	UT_ASSERT_EQ(dirty_calls + wal_calls + version_edges + read_calls, 0);
	tuple = hint_setup(true);
	commit_pending = true;
	SetHintBits(tuple, 1, HEAP_XMIN_COMMITTED, 77);
	UT_ASSERT(memcmp(hint_before.data, page.data, BLCKSZ) == 0);
	commit_pending = false;
	foreign_xid = true;
	SetHintBits(tuple, 1, HEAP_XMIN_COMMITTED, InvalidTransactionId);
	UT_ASSERT(memcmp(hint_before.data, page.data, BLCKSZ) == 0);
	UT_ASSERT_EQ(hint_calls + dirty_calls + wal_calls + version_edges, 0);
}
UT_TEST(test_required_xmax_missing_identity_refuses_before_store)
{
	HeapTupleHeader tuple = hint_setup(false);
	expected_error = true;
	if (setjmp(error_jump) == 0) {
		cluster_heap_stamp_released_xmax_invalid(tuple, 1);
		UT_ASSERT(false);
	}
	expected_error = false;
	UT_ASSERT(memcmp(hint_before.data, page.data, BLCKSZ) == 0);
	UT_ASSERT_EQ(hint_calls + dirty_calls + wal_calls + version_edges + read_calls, 0);
}
UT_TEST(test_native_waited_xmax_cannot_silently_skip_required_hint)
{
	HeapTupleHeader tuple = hint_setup(false);
	expected_error = true;
	if (setjmp(error_jump) == 0) {
		UpdateXmaxHintBits(tuple, 1, 78);
		UT_ASSERT(false);
	}
	expected_error = false;
	UT_ASSERT(memcmp(hint_before.data, page.data, BLCKSZ) == 0);
	UT_ASSERT_EQ(hint_calls + dirty_calls + wal_calls + version_edges + read_calls, 0);

	tuple = hint_setup(true);
	UpdateXmaxHintBits(tuple, 1, 78);
	UT_ASSERT(tuple->t_infomask & HEAP_XMAX_INVALID);
	check_hint_version();

	tuple = hint_setup(false);
	xmax_committed = commit_pending = true;
	UpdateXmaxHintBits(tuple, 1, 78);
	UT_ASSERT(memcmp(hint_before.data, page.data, BLCKSZ) == 0);
	UT_ASSERT_EQ(hint_calls + dirty_calls + wal_calls + version_edges + read_calls, 0);
}
UT_TEST(test_lazy_cleanout_versions_only_qualified_exclusive_hint)
{
	for (int bad = 0; bad < 5; bad++) {
		ClusterItlSlotData *slot;
		bool stamped;
		(void) hint_setup(bad != 1);
		content_x = false;
		slot = &ClusterPageGetItlSlots(page.data)[0];
		slot->flags = ITL_FLAG_ACTIVE;
		slot->xid = 77;
		slot->commit_scn = InvalidScn;
		if (bad == 2) lazy_lock_available = false;
		if (bad == 3) slot->xid++;
		if (bad == 4) writer_allowed = false;
		hint_before = page;
		stamped = cluster_itl_cleanout_lazy(1, 0, 77, 80);
		UT_ASSERT_EQ(stamped, bad == 0);
		UT_ASSERT(!content_x);
		if (bad == 0) {
			check_hint_version();
			UT_ASSERT_EQ(slot->flags, ITL_FLAG_COMMITTED);
			UT_ASSERT_EQ(slot->commit_scn, 80);
		} else {
			UT_ASSERT(memcmp(hint_before.data, page.data, BLCKSZ) == 0);
			UT_ASSERT_EQ(hint_calls + dirty_calls + wal_calls + version_edges, 0);
			UT_ASSERT_EQ(read_calls, bad == 1 ? 0 : 1);
		}
		UT_ASSERT_EQ(lazy_unlocks, bad == 2 ? 0 : 1);
	}
}
UT_TEST(test_terminal_census_preserves_required_cleanup_and_versions_it)
{
	for (int missing = 0; missing < 2; missing++) {
		ClusterHeapItlTerminalCensus census = {0};
		ClusterHeapItlTerminalBatchApplyResult result;
		uint8 terminal_flags[CLUSTER_ITL_INITRANS_DEFAULT] = {ITL_FLAG_LOCK_ONLY_COMMITTED};
		ClusterItlSlotData *slot;
		HeapTupleHeader tuple;
		PGAlignedBlock tuple_bytes;

		(void) hint_setup(!missing);
		PageInitHeapPage(page.data, BLCKSZ, 0);
		((PageHeader) page.data)->pd_block_scn = 20;
		memset(&tuple_bytes, 0, sizeof(tuple_bytes));
		tuple = (HeapTupleHeader) tuple_bytes.data;
		tuple->t_hoff = MAXALIGN(SizeofHeapTupleHeader);
		tuple->t_infomask = HEAP_XMIN_COMMITTED | HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_KEYSHR_LOCK;
		HeapTupleHeaderSetXmax(tuple, 77);
		UT_ASSERT_EQ(PageAddItem(page.data, (Item) tuple, 64, 1, false, true), 1);
		slot = &ClusterPageGetItlSlots(page.data)[0];
		slot->flags = ITL_FLAG_LOCK_ONLY_ACTIVE;
		slot->xid = 77;
		slot->wrap = 2;
		census.terminal_mask = 1;
		census.terminal_count = 1;
		census.outcomes[0] = CLUSTER_TX_COMMITTED;
		census.resolutions[0].commit_scn = 80;
		hint_before = page;
		result = terminal_census_publish(1, &census, terminal_flags);
		if (missing) {
			UT_ASSERT_EQ(result.kind, CLUSTER_HEAP_ITL_BATCH_REFUSED);
			UT_ASSERT(memcmp(hint_before.data, page.data, BLCKSZ) == 0);
			UT_ASSERT_EQ(hint_calls + dirty_calls + wal_calls + version_edges + read_calls, 0);
		} else {
			check_hint_version();
			UT_ASSERT_EQ(result.kind, CLUSTER_HEAP_ITL_BATCH_STALE_CURRENT_X);
			UT_ASSERT_EQ(result.stamped_count, 1);
			UT_ASSERT_EQ(slot->flags, ITL_FLAG_LOCK_ONLY_COMMITTED);
			tuple = (HeapTupleHeader) PageGetItem(page.data, PageGetItemId(page.data, 1));
			UT_ASSERT(tuple->t_infomask & HEAP_XMAX_INVALID);
		}
	}
}
UT_TEST(test_fsm_hint_uses_explicit_rebuildable_component)
{
	(void) hint_setup(false);
	hint_fork = FSM_FORKNUM;
	native_hint_fpi = true;
	content_x = false;
	MyProc->delayChkptFlags = DELAY_CHKPT_START;
	UT_ASSERT_EQ(XLogSaveBufferForHint(1, false), 200);
	UT_ASSERT_EQ(wal_calls, 1);
	UT_ASSERT_EQ(version_edges, 1);
	UT_ASSERT_EQ(recorded_edge.page_class, RF_PAGE_CLASS_REBUILDABLE_FSM);
	UT_ASSERT_EQ(recorded_edge.before_kind, RF_PAGE_STATE_REBUILDABLE);
	UT_ASSERT_EQ(recorded_edge.result_kind, RF_PAGE_STATE_REBUILDABLE);
	UT_ASSERT_EQ(read_calls + dirty_calls + hint_calls, 0);
	UT_ASSERT(memcmp(hint_before.data, page.data, BLCKSZ) == 0);
	UT_ASSERT(memcmp(hint_before.data, hint_image.data, BLCKSZ) == 0);
}
UT_TEST(test_unprepared_shared_hint_cannot_emit_native_fpi)
{
	(void) hint_setup(true);
	native_hint_fpi = true;
	MyProc->delayChkptFlags = DELAY_CHKPT_START;
	expected_error = true;
	if (setjmp(error_jump) == 0) {
		(void) XLogSaveBufferForHint(1, false);
		UT_ASSERT(false);
	}
	expected_error = false;
	UT_ASSERT_EQ(wal_calls + version_edges + dirty_calls + hint_calls, 0);
	UT_ASSERT(memcmp(hint_before.data, page.data, BLCKSZ) == 0);
}
UT_TEST(test_fsm_truncate_fpi_keeps_rebuildable_class)
{
	for (int shared = 0; shared < 2; shared++) {
		(void) hint_setup(false);
		cluster_shared_config = shared;
		hint_fork = FSM_FORKNUM;
		expected_xlog_info = XLOG_FPI;
		PageInit(page.data, BLCKSZ, 0);
		UT_ASSERT(fsm_set_avail(page.data, 2, 99));
		UT_ASSERT(fsm_set_avail(page.data, 5, 100));
		hint_before = page;
		fsm_truncate_publication();
		UT_ASSERT_EQ(fsm_get_avail(page.data, 2), 99);
		UT_ASSERT_EQ(fsm_get_avail(page.data, 5), 0);
		UT_ASSERT_EQ(wal_calls, 1);
		UT_ASSERT_EQ(dirty_calls, 1);
		UT_ASSERT_EQ(version_edges, shared ? 1 : 0);
		UT_ASSERT_EQ(next_token, shared ? 101 : 100);
		UT_ASSERT_EQ(((PageHeader)page.data)->pd_block_scn, 0);
		UT_ASSERT_EQ(PageGetLSN(page.data), 200);
		UT_ASSERT_EQ(CritSectionCount + read_calls, 0);
		if (shared) {
			UT_ASSERT_EQ(recorded_edge.page_class, RF_PAGE_CLASS_REBUILDABLE_FSM);
			UT_ASSERT_EQ(recorded_edge.result_kind, RF_PAGE_STATE_REBUILDABLE);
		}
	}
}
int
main(void)
{
	UT_PLAN(23);
	UT_RUN(test_insert_allocation_and_delete_share_one_identity_read);
	UT_RUN(test_targeted_invalidation_reloads_new_incarnation);
	UT_RUN(test_unrelated_invalidation_preserves_hit_global_reset_drops_it);
	UT_RUN(test_locator_and_namespace_changes_cannot_hit_old_cache);
	UT_RUN(test_invalidation_during_read_reobserves_instead_of_publishing_old_value);
	UT_RUN(test_invalid_relcache_cannot_resurrect_a_stale_entry);
	UT_RUN(test_unavailable_authority_and_recovery_do_not_reuse_cache);
	UT_RUN(test_missing_corrupt_and_tombstoned_identity_never_poison_cache);
	UT_RUN(test_btree_build_captures_one_cached_identity_value);
	UT_RUN(test_hint_share_lock_never_changes_shared_page);
	UT_RUN(test_hint_exclusive_cached_identity_versions_before_store);
	UT_RUN(test_hint_refusal_has_no_io_or_mutation);
	UT_RUN(test_hint_rejects_dirty_gate_difference_before_critical_section);
	UT_RUN(test_hint_existing_content_holder_can_finish_revoking_x);
	UT_RUN(test_released_xmax_is_not_skipped_with_shared_identity);
	UT_RUN(test_hint_native_and_commit_interlock_remain);
	UT_RUN(test_required_xmax_missing_identity_refuses_before_store);
	UT_RUN(test_native_waited_xmax_cannot_silently_skip_required_hint);
	UT_RUN(test_lazy_cleanout_versions_only_qualified_exclusive_hint);
	UT_RUN(test_terminal_census_preserves_required_cleanup_and_versions_it);
	UT_RUN(test_fsm_hint_uses_explicit_rebuildable_component);
	UT_RUN(test_unprepared_shared_hint_cannot_emit_native_fpi);
	UT_RUN(test_fsm_truncate_fpi_keeps_rebuildable_class);
	UT_DONE();
	return ut_failed_count != 0;
}
