/*-------------------------------------------------------------------------
 *
 * test_cluster_space_storage.c
 *    Actual SPACE create/replay adapters with native I/O boundary fixtures.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_space_storage.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlog.h"
#include "access/xloginsert.h"
#include "access/xact.h"
#include "access/xlogutils.h"
#include "access/visibilitymap.h"
#include "catalog/pg_class.h"
#include "catalog/pg_tablespace_d.h"
#include "catalog/storage.h"
#include "catalog/storage_xlog.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_ko.h"
#include "cluster/cluster_hw.h"
#include "cluster/cluster_pcm_x_bufmgr.h"
#include "cluster/cluster_page_producer.h"
#include "cluster/cluster_space_storage.h"
#include "cluster/cluster_space_reservation.h"
#include "cluster/cluster_wal_thread.h"
#include "cluster/cluster_xnode_profile.h"
#include "cluster/storage/cluster_smgr.h"
#include "miscadmin.h"
#include "replication/origin.h"
#include "storage/bufmgr.h"
#include "storage/buf_internals.h"
#include "storage/freespace.h"
#include "storage/smgr.h"
#include "storage/proc.h"
#include "utils/hsearch.h"
#include "utils/inval.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

bool cluster_shared_config = true, cluster_enabled = true;
int cluster_node_id = 0;
int NBuffers = 2, NLocBuffer;
char *BufferBlocks;
Block *LocalBufferBlockPointers;
volatile uint32 CritSectionCount;
bool cluster_recmerge_window_active, cluster_recmerge_apply_foreign;
uint64 cluster_recmerge_window_scn, cluster_recmerge_window_own_lsn;
int wal_level = WAL_LEVEL_REPLICA;
BackendId MyBackendId = 1, ParallelLeaderBackendId = InvalidBackendId;
MemoryContext TopMemoryContext = (MemoryContext)1;
MemoryContext TopTransactionContext = (MemoryContext)2;

static PGAlignedBlock pages[2];
#define page pages[0]
static SMgrRelationData storage;
static ClusterWalSourceRef ref;
static bool have_ref, shared, exists, recovering;
static uint8 pinned, locked;
static unsigned io_calls, create_calls, wal_calls, dirty_calls, release_calls;
static BlockNumber blocks;
static uint8 wal_bytes[CLUSTER_SPACE_STRUCTURE_WAL_BYTES];
static uint8 commit_bytes[4096];
static uint32 commit_len;
static uint8 wal_info;
static uint32 registered_len;
static unsigned main_create_calls, unlink_calls;
static bool native_owner, delete_registered;
static RelFileLocator locator = { DEFAULTTABLESPACE_OID, 5, 16384 };
static BlockNumber main_blocks;
static unsigned truncate_calls, flush_calls, fsm_vacuums;
static bool auxiliary_forks;
static Relation fake_relation;
static unsigned fake_allocations, fake_frees;
static int nest_level;
static unsigned ko_calls;
static unsigned current_ref_reads, restart_ref_reads;
static bool reject_current_ref;
static uint64 next_token;
static bool reserve_owner, writer_allowed, revoke_on_flush;
static bool hw_held, fail_hw_lock, throw_reserve_flush;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
static BufferDescPadded descriptors[2];
BufferDescPadded *BufferDescriptors = descriptors;
static bool truncate_owner, drop_owner, deleted_all, expecting_error;
static unsigned relation_flushes, fork_syncs, invalidations, random_calls;
static PGPROC proc;
PGPROC *MyProc = &proc;
XLogRecPtr XactLastRecEnd;
int MyXactFlags;
bool cluster_smart_fusion = false;
static unsigned native_commits, commit_decisions;
static bool plain_commit_emitter;
static bool forceSyncCommit;
int synchronous_commit = SYNCHRONOUS_COMMIT_OFF;
Oid MyDatabaseId = 5, MyDatabaseTableSpace = DEFAULTTABLESPACE_OID;
RepOriginId replorigin_session_origin = InvalidRepOriginId;
XLogRecPtr replorigin_session_origin_lsn;
TimestampTz replorigin_session_origin_timestamp;

/* The two real native commit critical-region fragments below retain their
 * WAL/decision/checkpoint ordering. Profiling and unrelated observers are
 * boundaries; the SPACE owner is the real linked product implementation. */
#define cluster_xp_begin(scope, bucket) ((void)0)
#define cluster_xp_end(scope) ((void)0)
void cluster_backup_pending_commit_exit(void) {}
bool cluster_scn_durable_pending_fill_lsn(SCN scn, XLogRecPtr lsn) { abort(); }
void cluster_sf_publish_origin_durable_lsn(void) { abort(); }
bool cluster_scn_pending_commit_clear(SCN scn) { return true; }
XLogRecPtr cluster_adg_emit_thread_barrier(void) { abort(); }
bool cluster_scn_durable_pending_discharge_scn(SCN scn) { abort(); }
TimestampTz GetCurrentTransactionStopTimestamp(void) { return 1; }
void XLogSetAsyncXactLSN(XLogRecPtr lsn) { abort(); }
void TransactionIdAsyncCommitTree(TransactionId xid, int count, TransactionId *children,
								 XLogRecPtr lsn) { abort(); }
void TransactionIdCommitTree(TransactionId xid, int count, TransactionId *children)
{
	UT_ASSERT_EQ(flush_calls, 1);
	UT_ASSERT_EQ(native_commits, 1);
	UT_ASSERT_EQ(CritSectionCount, 1);
	UT_ASSERT_EQ(((PageHeader)page.data)->pd_block_scn, 17);
	commit_decisions++;
}

#define CLUSTER_INJECTION_POINT(name) ((void)0)
#include "test_cluster_space_commit_emit.inc"

void XLogSetRecordFlags(uint8 flags) { UT_ASSERT_EQ(flags, XLOG_INCLUDE_ORIGIN); }

void
pg_re_throw(void)
{
	if (PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}

bool
cluster_hw_lock(const ClusterResId *resid, HwLock *lock)
{
	ClusterResId expected;

	UT_ASSERT(!hw_held);
	cluster_hw_resid_encode(locator, MAIN_FORKNUM, &expected);
	UT_ASSERT(memcmp(resid, &expected, sizeof(expected)) == 0);
	memset(lock, 0, sizeof(*lock));
	if (fail_hw_lock)
		return false;
	lock->req.resid = *resid;
	lock->req.lockmode = ExclusiveLock;
	lock->held = lock->coordinated = hw_held = true;
	return true;
}

void
cluster_hw_unlock(HwLock *lock)
{
	UT_ASSERT(hw_held && lock->held);
	lock->held = lock->coordinated = hw_held = false;
}

bool
cluster_bufmgr_pcm_x_content_holder_write_permitted(BufferDesc *buffer)
{
	UT_ASSERT(reserve_owner);
	if (!truncate_owner) {
		UT_ASSERT(buffer == GetBufferDescriptor(1));
		UT_ASSERT(locked == 2 || (recovering && locked == 3));
	} else
		UT_ASSERT(locked & (1 << buffer->buf_id));
	return writer_allowed;
}

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	printf("# Unexpected assertion: %s at %s:%d\n", condition, file, line);
	abort();
}

bool
pg_strong_random(void *bytes, size_t len)
{
	memset(bytes, 0x45 + random_calls++, len);
	return true;
}

uint64
rf_page_mutation_token_next(void)
{
	return next_token++;
}

bool
cluster_wal_thread_current_v2_ref(ClusterWalSourceRef *out)
{
	current_ref_reads++;
	*out = ref;
	return have_ref && !reject_current_ref;
}

bool
cluster_wal_thread_restart_v2_ref(ClusterWalSourceRef *out)
{
	restart_ref_reads++;
	*out = ref;
	return have_ref;
}

bool
RecoveryInProgress(void)
{
	return recovering;
}

int
cluster_smgr_which_for(RelFileLocator tag, BackendId backend)
{
	if (!RelFileLocatorEquals(tag, locator) || backend != InvalidBackendId)
		abort();
	return shared ? 1 : 0;
}

SMgrRelation
smgropen(RelFileLocator tag, BackendId backend)
{
	if (!RelFileLocatorEquals(tag, locator) || backend != InvalidBackendId)
		abort();
	io_calls++;
	return &storage;
}

bool
smgrexists(SMgrRelation rel, ForkNumber forknum)
{
	if (rel != &storage)
		abort();
	if (deleted_all)
		return false;
	if ((recovering || truncate_owner) && (forknum == FSM_FORKNUM || forknum == VISIBILITYMAP_FORKNUM))
		return auxiliary_forks;
	if (truncate_owner && forknum == MAIN_FORKNUM)
		return true;
	if (forknum != SPACE_FORKNUM)
		abort();
	return exists;
}

void
smgrcreate(SMgrRelation rel, ForkNumber forknum, bool is_redo)
{
	if (rel != &storage || is_redo != recovering)
		abort();
	if (forknum == MAIN_FORKNUM && (native_owner || recovering)) {
		main_create_calls++;
		return;
	}
	if (forknum != SPACE_FORKNUM || (native_owner && !delete_registered))
		abort();
	create_calls++;
	exists = true;
}

BlockNumber
smgrnblocks(SMgrRelation rel, ForkNumber forknum)
{
	if (rel != &storage)
		abort();
	if ((recovering || truncate_owner) && forknum == MAIN_FORKNUM)
		return main_blocks;
	if ((recovering || truncate_owner) && auxiliary_forks && forknum == FSM_FORKNUM)
		return 8;
	if ((recovering || truncate_owner) && auxiliary_forks && forknum == VISIBILITYMAP_FORKNUM)
		return 3;
	if (forknum != SPACE_FORKNUM)
		abort();
	return blocks;
}

void
XLogFlush(XLogRecPtr lsn)
{
	if (drop_owner) {
		UT_ASSERT_EQ(lsn, UINT64_C(0x10000300));
		UT_ASSERT_EQ(CritSectionCount, recovering ? 0 : 1);
		UT_ASSERT_EQ(locked, deleted_all ? 0 : 3);
		flush_calls++;
		return;
	}
	if (truncate_owner) {
		UT_ASSERT_EQ(lsn, UINT64_C(0x10000200));
		UT_ASSERT_EQ(CritSectionCount, 1);
		flush_calls++;
		return;
	}
	if (reserve_owner) {
		if (recovering || lsn != UINT64_C(0x10000200) || locked != 2 || CritSectionCount != 0)
			abort();
		flush_calls++;
		if (throw_reserve_flush)
			pg_re_throw();
		if (revoke_on_flush)
			writer_allowed = false;
		return;
	}
	if (!recovering || lsn != UINT64_C(0x20000200) || locked != 3)
		abort();
	flush_calls++;
}

bool XLogNeedsFlush(XLogRecPtr lsn)
{
	UT_ASSERT(drop_owner && lsn == UINT64_C(0x10000300));
	return flush_calls == 0;
}

void
XLogTruncateRelation(RelFileLocator tag, ForkNumber forknum, BlockNumber count)
{
	if (!RelFileLocatorEquals(tag, locator) || forknum != MAIN_FORKNUM || count != 4)
		abort();
}

void
smgrclearowner(SMgrRelation *owner, SMgrRelation rel)
{
	if (rel != &storage || owner != &fake_relation->rd_smgr)
		abort();
	*owner = NULL;
	rel->smgr_owner = NULL;
}
void
smgrsetowner(SMgrRelation *owner, SMgrRelation rel)
{
	UT_ASSERT(rel == &storage);
	*owner = rel;
	rel->smgr_owner = owner;
}

BlockNumber
FreeSpaceMapPrepareTruncateRel(Relation rel, BlockNumber count)
{
	if (rel != fake_relation || count != 4 || !auxiliary_forks
		|| !RelFileLocatorEquals(rel->rd_locator, locator)
		|| rel->rd_rel->relpersistence != RELPERSISTENCE_PERMANENT)
		abort();
	return 1;
}
BlockNumber
visibilitymap_prepare_truncate(Relation rel, BlockNumber count)
{
	if (rel != fake_relation || count != 4 || !auxiliary_forks)
		abort();
	return 1;
}
void
FreeSpaceMapVacuumRange(Relation rel, BlockNumber start, BlockNumber end)
{
	if (rel != fake_relation || start != 4 || end != InvalidBlockNumber || !auxiliary_forks
		|| truncate_calls == 0)
		abort();
	fsm_vacuums++;
}

void
smgrtruncate2(SMgrRelation rel, ForkNumber *forks, int nforks, BlockNumber *oldblocks,
			  BlockNumber *newblocks)
{
	if (rel != &storage || nforks != (auxiliary_forks ? 3 : 1) || forks[0] != MAIN_FORKNUM
		|| (!truncate_owner && (locked != 3 || pinned != 3))
		|| CritSectionCount == 0 || flush_calls != truncate_calls + 1
		|| oldblocks[0] != main_blocks || newblocks[0] != 4)
		abort();
	if (auxiliary_forks
		&& (forks[1] != FSM_FORKNUM || forks[2] != VISIBILITYMAP_FORKNUM || oldblocks[1] != 8
			|| oldblocks[2] != 3 || newblocks[1] != 1 || newblocks[2] != 1))
		abort();
	main_blocks = newblocks[0];
	truncate_calls++;
}

Buffer
ReadBufferWithoutRelcache(RelFileLocator tag, ForkNumber forknum, BlockNumber block,
						  ReadBufferMode mode, BufferAccessStrategy strategy, bool permanent)
{
	if (!RelFileLocatorEquals(tag, locator) || forknum != SPACE_FORKNUM || strategy != NULL
		|| !permanent)
		abort();
	if (block == P_NEW) {
		if (blocks > 1 || mode != RBM_ZERO_AND_LOCK)
			abort();
		block = blocks++;
		locked |= 1 << block;
	} else if (block > 1 || block >= blocks || mode != RBM_NORMAL)
		abort();
	if (pinned & (1 << block))
		abort();
	pinned |= 1 << block;
	return block + 1;
}

BlockNumber
BufferGetBlockNumber(Buffer buffer)
{
	if (buffer < 1 || buffer > 2 || !(pinned & (1 << (buffer - 1))))
		abort();
	return buffer - 1;
}

void
LockBuffer(Buffer buffer, int mode)
{
	if (buffer < 1 || buffer > 2 || !(pinned & (1 << (buffer - 1))) || (locked & (1 << (buffer - 1)))
		|| (mode != BUFFER_LOCK_EXCLUSIVE && mode != BUFFER_LOCK_SHARE))
		abort();
	locked |= 1 << (buffer - 1);
}

void
UnlockReleaseBuffer(Buffer buffer)
{
	if (buffer < 1 || buffer > 2 || !(pinned & (1 << (buffer - 1)))
		|| !(locked & (1 << (buffer - 1))) || CritSectionCount != 0)
		abort();
	pinned &= ~(1 << (buffer - 1));
	locked &= ~(1 << (buffer - 1));
	release_calls++;
}

void
MarkBufferDirty(Buffer buffer)
{
	if (buffer < 1 || buffer > 2 || !(pinned & (1 << (buffer - 1)))
		|| !(locked & (1 << (buffer - 1))) || CritSectionCount != 1
		|| (buffer == 1 ? !cluster_space_identity_page_valid(page.data, BLCKSZ)
			: !cluster_space_reservation_page_valid(pages[1].data, BLCKSZ)))
		abort();
	dirty_calls++;
}

void
XLogBeginInsert(void)
{
	if (drop_owner)
		commit_len = 0;
	if (truncate_owner) {
		UT_ASSERT_EQ(CritSectionCount, 1);
		return;
	}
	if ((!locked && !native_owner) || CritSectionCount != 0)
		abort();
}

void
XLogRegisterData(char *bytes, uint32 len)
{
	if (drop_owner) {
		UT_ASSERT_EQ(CritSectionCount, 1);
		if (len > sizeof(commit_bytes) - commit_len)
			abort();
		memcpy(commit_bytes + commit_len, bytes, len);
		commit_len += len;
		return;
	}
	registered_len = len;
	if (native_owner && len == sizeof(xl_smgr_create) && CritSectionCount == 0)
		return;
	if (truncate_owner && len == sizeof(xl_smgr_truncate)) {
		memcpy(wal_bytes, bytes, len);
		return;
	}
	if ((len != sizeof(wal_bytes) && len != CLUSTER_SPACE_WAL_BYTES
		&& len != CLUSTER_SPACE_RESERVATION_WAL_BYTES) || CritSectionCount != 1)
		abort();
	memcpy(wal_bytes, bytes, len);
}

XLogRecPtr
XLogInsert(RmgrId rmid, uint8 info)
{
	ClusterSpaceWalChange change;
	ClusterSpaceStructureChange pair;
	ClusterSpaceIdentity id;
	ClusterSpaceReservation reservation;
	uint64 token;

	if (drop_owner) {
		xl_xact_parsed_commit parsed;

		UT_ASSERT_EQ(CritSectionCount, 1);
		UT_ASSERT(MyProc->delayChkptFlags & DELAY_CHKPT_START);
		UT_ASSERT_EQ(rmid, RM_XACT_ID);
		if (plain_commit_emitter) {
			wal_info = info;
			wal_calls++;
			return XactLastRecEnd = UINT64_C(0x10000300);
		}
		UT_ASSERT_EQ(info, XLOG_XACT_COMMIT | XLOG_XACT_HAS_INFO | XLR_SPECIAL_REL_UPDATE);
		UT_ASSERT_EQ(locked, 3);
		UT_ASSERT_EQ(dirty_calls, 2);
		UT_ASSERT(ParseCommitRecord(info, (xl_xact_commit *)commit_bytes, commit_len, &parsed));
		UT_ASSERT_EQ(parsed.nspace_drops, 1);
		UT_ASSERT(RelFileLocatorEquals(parsed.xlocators[0], locator));
		UT_ASSERT(cluster_space_structure_wal_decode(parsed.space_drops,
			CLUSTER_SPACE_STRUCTURE_WAL_BYTES, &pair));
		memcpy(wal_bytes, parsed.space_drops, sizeof(wal_bytes));
		registered_len = sizeof(wal_bytes);
		UT_ASSERT_EQ(pair.identity.action, CLUSTER_SPACE_WAL_TOMBSTONE);
		UT_ASSERT_EQ(((PageHeader)page.data)->pd_block_scn, pair.identity.before_token);
		UT_ASSERT_EQ(((PageHeader)pages[1].data)->pd_block_scn, pair.reservation.before_token);
		wal_info = info;
		wal_calls++;
		native_commits++;
		return XactLastRecEnd = UINT64_C(0x10000300);
	}
	if (truncate_owner) {
		UT_ASSERT_EQ(CritSectionCount, 1);
		UT_ASSERT_EQ(rmid, RM_SMGR_ID);
		if (info == (XLOG_SMGR_SPACE_IDENTITY | XLR_SPECIAL_REL_UPDATE)) {
			UT_ASSERT_EQ(locked, 3);
			UT_ASSERT_EQ(relation_flushes, 1);
			UT_ASSERT(fork_syncs >= 2);
			UT_ASSERT(cluster_space_structure_wal_decode(wal_bytes, registered_len, &pair));
			UT_ASSERT_EQ(pair.identity.action, CLUSTER_SPACE_WAL_TRUNCATE);
			/* Both buffers are selected for checkpoint, but retain before
			 * until the original physical shrink has completed. */
			UT_ASSERT_EQ(dirty_calls, 2);
			UT_ASSERT_EQ(((PageHeader)page.data)->pd_block_scn, pair.identity.before_token);
			UT_ASSERT_EQ(((PageHeader)pages[1].data)->pd_block_scn, pair.reservation.before_token);
		}
		wal_calls++;
		wal_info = info;
		return UINT64_C(0x10000200);
	}
	if (native_owner && info == (XLOG_SMGR_CREATE | XLR_SPECIAL_REL_UPDATE)
		&& registered_len == sizeof(xl_smgr_create) && rmid == RM_SMGR_ID)
		return UINT64_C(0x10000100);
	if (reserve_owner && info == (XLOG_SMGR_SPACE_RESERVATION | XLR_SPECIAL_REL_UPDATE)) {
		ClusterSpaceReservationChange advance;

		if (rmid != RM_SMGR_ID || CritSectionCount != 1 || registered_len != CLUSTER_SPACE_RESERVATION_WAL_BYTES
			|| !cluster_space_reservation_wal_decode(wal_bytes, registered_len, &advance)
			|| !cluster_space_reservation_page_decode(pages[1].data, BLCKSZ, SPACE_FORKNUM, 1,
				&advance.result.identity.key, &reservation, &token)
			|| advance.action != CLUSTER_SPACE_RESERVATION_ADVANCE || token != advance.result_token
			|| reservation.next_block != advance.result.next_block)
			abort();
		wal_calls++;
		wal_info = info;
		return UINT64_C(0x10000200);
	}
	if (rmid != RM_SMGR_ID || CritSectionCount != 1
		|| !cluster_space_wal_decode(wal_bytes, CLUSTER_SPACE_WAL_BYTES, &change)
		|| !cluster_space_identity_page_decode(page.data, BLCKSZ, SPACE_FORKNUM, 0,
											   &change.result.key, &id, &token)
		|| token != change.result_token)
		abort();
	if (registered_len == CLUSTER_SPACE_STRUCTURE_WAL_BYTES
		&& (!cluster_space_structure_wal_decode(wal_bytes, sizeof(wal_bytes), &pair)
			|| !cluster_space_reservation_page_decode(pages[1].data, BLCKSZ, SPACE_FORKNUM, 1,
				&change.result.key, &reservation, &token) || token != change.result_token
			|| reservation.next_block != 0))
		abort();
	wal_calls++;
	wal_info = info;
	return UINT64_C(0x10000200);
}

/* Native relation-create/pending-delete ownership is real; only memory,
 * external storage, and unused wal_level=minimal hash boundaries are faked. */
bool
IsInParallelMode(void)
{
	return false;
}
int
GetCurrentTransactionNestLevel(void)
{
	return nest_level;
}
void *
MemoryContextAlloc(MemoryContext context, Size size)
{
	if (drop_owner && recovering && context == TopMemoryContext)
		return malloc(size);
	if (context != TopMemoryContext || !native_owner)
		abort();
	delete_registered = true;
	return malloc(size);
}
void *
palloc(Size size)
{
	return malloc(size);
}
void *
palloc0(Size size)
{
	/* Real CreateFakeRelcacheEntry calls this boundary. Model a normal
	 * recovery memory context, which disallows allocation in a critical
	 * section, rather than masking the native allocation with a static rel. */
	UT_ASSERT_EQ(CritSectionCount, 0);
	if (truncate_owner && fake_relation != NULL)
		return calloc(1, size);
	if (fake_relation != NULL || size < sizeof(RelationData))
		abort();
	fake_relation = calloc(1, size);
	fake_allocations++;
	return fake_relation;
}
void *
repalloc(void *ptr, Size size)
{
	return realloc(ptr, size);
}
void
pfree(void *ptr)
{
	if (ptr == fake_relation) {
		fake_frees++;
		fake_relation = NULL;
	}
	free(ptr);
}
HTAB *
hash_create(const char *name, long count, const HASHCTL *info, int flags)
{
	(void)name;
	(void)count;
	(void)info;
	(void)flags;
	abort();
}
void *
hash_search(HTAB *hash, const void *key, HASHACTION action, bool *found)
{
	(void)hash;
	(void)key;
	(void)action;
	(void)found;
	abort();
}
void hash_seq_init(HASH_SEQ_STATUS *status, HTAB *hash) { abort(); }
void *hash_seq_search(HASH_SEQ_STATUS *status) { abort(); }
void
smgrdounlinkall(SMgrRelation *rels, int count, bool redo)
{
	if (count != 1 || rels[0] != &storage || redo || !delete_registered)
		abort();
	if (drop_owner) {
		UT_ASSERT_EQ(commit_decisions, 1);
		UT_ASSERT_EQ(locked | pinned | CritSectionCount | MyProc->delayChkptFlags, 0);
		UT_ASSERT_EQ(((PageHeader)page.data)->pd_block_scn, 18);
		UT_ASSERT_EQ(PageGetLSN(page.data), UINT64_C(0x10000300));
	}
	unlink_calls++;
}
void
smgrclose(SMgrRelation rel)
{
	if (rel != &storage)
		abort();
	if (rel->smgr_owner != NULL)
		*rel->smgr_owner = NULL;
}
void
cluster_ko_flush_and_wait_ack(RelFileLocator tag, char persistence)
{
	if (!RelFileLocatorEquals(tag, locator) || persistence != RELPERSISTENCE_PERMANENT)
		abort();
	ko_calls++;
}
int
errmsg(const char *fmt, ...)
{
	(void)fmt;
	abort();
}
bool errstart(int level, const char *domain) { return level >= ERROR; }
bool errstart_cold(int level, const char *domain) { return errstart(level, domain); }
int errmsg_internal(const char *fmt, ...) { return 0; }
void
errfinish(const char *file, int line, const char *function)
{
	if (expecting_error) pg_re_throw();
	fprintf(stderr, "unexpected error %s:%d %s\n", file, line, function);
	abort();
}
void
FlushRelationBuffers(Relation rel)
{
	UT_ASSERT(truncate_owner && rel == fake_relation && locked == 0);
	UT_ASSERT_EQ(ko_calls, 1);
	relation_flushes++;
}
void
smgrimmedsync(SMgrRelation rel, ForkNumber fork)
{
	UT_ASSERT(truncate_owner && rel == &storage && locked == 0);
	UT_ASSERT_EQ(relation_flushes, 1);
	fork_syncs++;
}
void
CacheInvalidateRelcache(Relation rel)
{
	UT_ASSERT(rel == fake_relation && truncate_owner);
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT_EQ(locked, 0);
	invalidations++;
}
int
errcode(int code)
{
	(void)code;
	abort();
}

static void
reset(void)
{
	if (fake_relation != NULL)
		abort();
	fake_allocations = fake_frees = 0;
	truncate_owner = drop_owner = deleted_all = expecting_error = false;
	relation_flushes = fork_syncs = invalidations = random_calls = 0;
	native_commits = commit_decisions = 0;
	plain_commit_emitter = false;
	commit_len = 0;
	forceSyncCommit = false;
	replorigin_session_origin = InvalidRepOriginId;
	wal_level = WAL_LEVEL_REPLICA;
	memset(&proc, 0, sizeof(proc));
	descriptors[0].bufferdesc.buf_id = 0;
	descriptors[1].bufferdesc.buf_id = 1;
	memset(pages, 0, sizeof(pages));
	memset(wal_bytes, 0, sizeof(wal_bytes));
	memset(&storage, 0, sizeof(storage));
	memset(&ref, 0, sizeof(ref));
	ref.claim.identity.system_identifier = 1;
	ref.claim.identity.storage_uuid[15] = 3;
	ref.claim.database_incarnation = 2;
	ref.claim.identity.origin_node_id = 0;
	ref.claim.identity.origin_thread_id = 1;
	cluster_shared_config = cluster_enabled = have_ref = shared = true;
	exists = recovering = pinned = locked = false;
	cluster_recmerge_window_active = cluster_recmerge_apply_foreign = false;
	native_owner = delete_registered = false;
	main_create_calls = unlink_calls = registered_len = 0;
	main_blocks = 10;
	truncate_calls = flush_calls = fsm_vacuums = 0;
	auxiliary_forks = false;
	nest_level = 1;
	ko_calls = 0;
	current_ref_reads = restart_ref_reads = 0;
	reject_current_ref = false;
	next_token = 17;
	reserve_owner = revoke_on_flush = false;
	hw_held = fail_hw_lock = throw_reserve_flush = false;
	writer_allowed = true;
	CritSectionCount = blocks = io_calls = create_calls = wal_calls = dirty_calls = release_calls
		= 0;
	BufferBlocks = page.data;
	storage.smgr_rlocator.locator = locator;
}

UT_TEST(test_real_create_binds_selected_namespace_and_wal)
{
	ClusterSpaceStructureChange change = {0};

	reset();
	UT_ASSERT(cluster_space_relation_create(locator));
	UT_ASSERT_EQ(create_calls, 1);
	UT_ASSERT_EQ(wal_calls, 1);
	UT_ASSERT_EQ(dirty_calls, 2);
	UT_ASSERT_EQ(release_calls, 2);
	UT_ASSERT_EQ(blocks, 2);
	UT_ASSERT_EQ(registered_len, CLUSTER_SPACE_STRUCTURE_WAL_BYTES);
	UT_ASSERT(!pinned && !locked);
	UT_ASSERT_EQ(wal_info, 0x30 | XLR_SPECIAL_REL_UPDATE);
	UT_ASSERT_EQ(PageGetLSN(page.data), UINT64_C(0x10000200));
	UT_ASSERT_EQ(((PageHeader)page.data)->pd_block_scn, 17);
	UT_ASSERT(cluster_space_structure_wal_decode(wal_bytes, sizeof(wal_bytes), &change));
	UT_ASSERT_EQ(change.identity.result.key.database_incarnation, 2);
	UT_ASSERT_EQ(change.identity.result.key.storage_uuid[15], 3);
	UT_ASSERT_EQ(change.identity.result.incarnation[15], 0x45);
	UT_ASSERT(cluster_space_reservation_page_valid(pages[1].data, BLCKSZ));
	UT_ASSERT_EQ(PageGetLSN(pages[1].data), UINT64_C(0x10000200));
}

UT_TEST(test_no_create_on_legacy_or_unproved_namespace)
{
	reset();
	cluster_shared_config = false;
	UT_ASSERT(cluster_space_relation_create(locator));
	UT_ASSERT_EQ(io_calls, 0);
	cluster_shared_config = true;
	shared = false;
	UT_ASSERT(cluster_space_relation_create(locator));
	UT_ASSERT_EQ(io_calls, 0);
	shared = true;
	have_ref = false;
	UT_ASSERT(!cluster_space_relation_create(locator));
	UT_ASSERT_EQ(io_calls, 0);
	have_ref = true;
	ref.claim.database_incarnation = 0;
	UT_ASSERT(!cluster_space_relation_create(locator));
	UT_ASSERT_EQ(io_calls, 0);
}

UT_TEST(test_existing_identity_is_never_recreated)
{
	PGAlignedBlock saved;

	reset();
	exists = true;
	memset(&page, 0x5a, sizeof(page));
	saved = page;
	UT_ASSERT(!cluster_space_relation_create(locator));
	UT_ASSERT_EQ(wal_calls, 0);
	UT_ASSERT_EQ(create_calls, 0);
	UT_ASSERT(memcmp(&page, &saved, BLCKSZ) == 0);
}

UT_TEST(test_identity_read_is_exact_and_never_creates)
{
	ClusterSpaceIdentity out;
	ClusterSpaceIdentity saved;
	unsigned prior_creates;

	reset();
	memset(&out, 0xa5, sizeof(out));
	saved = out;
	UT_ASSERT(!cluster_space_relation_read_identity(locator, &out));
	UT_ASSERT_EQ(create_calls, 0);
	UT_ASSERT(memcmp(&out, &saved, sizeof(out)) == 0);
	UT_ASSERT(cluster_space_relation_create(locator));
	prior_creates = create_calls;
	UT_ASSERT(cluster_space_relation_read_identity(locator, &out));
	UT_ASSERT_EQ(out.state, CLUSTER_SPACE_IDENTITY_LIVE);
	UT_ASSERT_EQ(out.incarnation[15], 0x45);
	UT_ASSERT(!pinned && !locked);
	saved = out;
	ref.claim.database_incarnation++;
	UT_ASSERT(!cluster_space_relation_read_identity(locator, &out));
	UT_ASSERT(memcmp(&out, &saved, sizeof(out)) == 0);
	ref.claim.database_incarnation--;
	page.data[160] = 1;
	UT_ASSERT(!cluster_space_relation_read_identity(locator, &out));
	UT_ASSERT(memcmp(&out, &saved, sizeof(out)) == 0);
	UT_ASSERT_EQ(create_calls, prior_creates);
	UT_ASSERT(!pinned && !locked);
	page.data[160] = 0;
	out.state = CLUSTER_SPACE_IDENTITY_TOMBSTONED;
	UT_ASSERT(cluster_space_identity_page_encode(&out, 18, page.data, BLCKSZ));
	out = saved;
	UT_ASSERT(!cluster_space_relation_read_identity(locator, &out));
	UT_ASSERT(memcmp(&out, &saved, sizeof(out)) == 0);
	UT_ASSERT(!pinned && !locked);
}

UT_TEST(test_recovery_identity_uses_only_restart_namespace)
{
	ClusterSpaceIdentity out, saved;
	unsigned writes;

	reset();
	UT_ASSERT(cluster_space_relation_create(locator));
	writes = wal_calls;
	memset(&out, 0xa5, sizeof(out));
	saved = out;
	current_ref_reads = restart_ref_reads = 0;
	UT_ASSERT(!cluster_space_relation_read_redo_identity(locator, &out));
	UT_ASSERT(memcmp(&out, &saved, sizeof(out)) == 0);
	UT_ASSERT_EQ(restart_ref_reads, 0);
	recovering = reject_current_ref = true;
	UT_ASSERT(!cluster_space_relation_read_identity(locator, &out));
	UT_ASSERT(cluster_space_relation_read_redo_identity(locator, &out));
	UT_ASSERT_EQ(current_ref_reads, 0);
	UT_ASSERT_EQ(restart_ref_reads, 1);
	UT_ASSERT_EQ(out.state, CLUSTER_SPACE_IDENTITY_LIVE);
	UT_ASSERT_EQ(out.incarnation[15], 0x45);
	saved = out;
	ref.claim.database_incarnation++;
	UT_ASSERT(!cluster_space_relation_read_redo_identity(locator, &out));
	UT_ASSERT(memcmp(&out, &saved, sizeof(out)) == 0);
	ref.claim.database_incarnation--;
	have_ref = false;
	UT_ASSERT(!cluster_space_relation_read_redo_identity(locator, &out));
	UT_ASSERT(memcmp(&out, &saved, sizeof(out)) == 0);
	UT_ASSERT_EQ(wal_calls, writes);
	UT_ASSERT(!pinned && !locked);
}

UT_TEST(test_real_replay_exact_duplicate_and_preserved_token)
{
	DecodedXLogRecord decoded;
	XLogReaderState reader;
	PGAlignedBlock saved;

	reset();
	UT_ASSERT(cluster_space_relation_create(locator));
	memset(&decoded, 0, sizeof(decoded));
	memset(&reader, 0, sizeof(reader));
	decoded.header.xl_rmid = RM_SMGR_ID;
	decoded.header.xl_info = 0x30 | XLR_SPECIAL_REL_UPDATE;
	decoded.main_data = (char *)wal_bytes;
	decoded.main_data_len = sizeof(wal_bytes);
	decoded.max_block_id = -1;
	reader.record = &decoded;
	reader.EndRecPtr = UINT64_C(0x20000200);
	reader.cluster_expected_thread_id = 1;
	recovering = true;
	memset(pages, 0, sizeof(pages));
	blocks = 0;
	dirty_calls = 0;
	/* Legacy merge's SCN stamp must not replace the typed result token. */
	cluster_recmerge_window_active = true;
	cluster_recmerge_window_scn = 999;
	UT_ASSERT(cluster_space_relation_redo(&reader));
	UT_ASSERT_EQ(((PageHeader)page.data)->pd_block_scn, 17);
	UT_ASSERT_EQ(dirty_calls, 2);
	saved = page;
	UT_ASSERT(cluster_space_relation_redo(&reader));
	UT_ASSERT_EQ(dirty_calls, 2);
	UT_ASSERT(memcmp(&page, &saved, BLCKSZ) == 0);
	ref.claim.database_incarnation++;
	UT_ASSERT(!cluster_space_relation_redo(&reader));
	UT_ASSERT(memcmp(&page, &saved, BLCKSZ) == 0);
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT(!pinned && !locked);
}

UT_TEST(test_native_create_registers_abort_cleanup_before_space)
{
	reset();
	native_owner = true;
	UT_ASSERT(RelationCreateStorage(locator, RELPERSISTENCE_PERMANENT, true) == &storage);
	UT_ASSERT_EQ(main_create_calls, 1);
	UT_ASSERT_EQ(create_calls, 1);
	UT_ASSERT_EQ(wal_calls, 1);
	UT_ASSERT(delete_registered);
	smgrDoPendingDeletes(false);
	UT_ASSERT_EQ(unlink_calls, 1);
}

UT_TEST(test_native_descriptor_recognizes_typed_record)
{
	const char *name = smgr_identify(0x30 | XLR_SPECIAL_REL_UPDATE);

	UT_ASSERT(name != NULL);
	if (name != NULL)
		UT_ASSERT(strcmp(name, "SPACE_IDENTITY") == 0);
	UT_ASSERT(smgr_identify(0xf0) == NULL);
}

static void
truncate_record(XLogReaderState *reader, DecodedXLogRecord *decoded)
{
	ClusterSpaceStructureChange pair;
	ClusterSpaceWalChange *change = &pair.identity;
	ClusterSpaceReservationChange *reservation = &pair.reservation;

	if (!cluster_space_relation_create(locator)
		|| !cluster_space_structure_wal_decode(wal_bytes, sizeof(wal_bytes), &pair))
		abort();
	change->action = CLUSTER_SPACE_WAL_TRUNCATE;
	change->nblocks = 4;
	change->expected = change->result;
	change->before_token = change->result_token;
	change->result.incarnation[0] ^= 1;
	change->result.sequence++;
	change->result.operation = ++change->result_token;
	reservation->action = CLUSTER_SPACE_RESERVATION_RESET;
	reservation->before = reservation->result;
	reservation->before.next_block = 10;
	reservation->before_token = 99;
	reservation->result.identity = change->result;
	reservation->result_token = change->result_token;
	reservation->first_block = reservation->result.next_block = 4;
	if (!cluster_space_reservation_page_encode(&reservation->before, 99, pages[1].data, BLCKSZ)
		|| !cluster_space_structure_wal_encode(&pair, wal_bytes, sizeof(wal_bytes)))
		abort();
	memset(decoded, 0, sizeof(*decoded));
	memset(reader, 0, sizeof(*reader));
	decoded->header.xl_rmid = RM_SMGR_ID;
	decoded->header.xl_info = XLOG_SMGR_SPACE_IDENTITY | XLR_SPECIAL_REL_UPDATE;
	decoded->main_data = (char *)wal_bytes;
	decoded->main_data_len = sizeof(wal_bytes);
	decoded->max_block_id = -1;
	reader->record = decoded;
	reader->EndRecPtr = UINT64_C(0x20000200);
	reader->cluster_expected_thread_id = 1;
	recovering = true;
}

UT_TEST(test_truncate_replay_checks_identity_before_native_shrink)
{
	XLogReaderState reader;
	DecodedXLogRecord decoded;
	PGAlignedBlock saved;

	reset();
	truncate_record(&reader, &decoded);
	smgr_redo(&reader);
	UT_ASSERT_EQ(main_blocks, 4);
	UT_ASSERT_EQ(truncate_calls, 1);
	UT_ASSERT_EQ(((PageHeader)page.data)->pd_block_scn, 18);
	UT_ASSERT(!pinned && !locked);
	saved = page;
	UT_ASSERT(cluster_space_relation_redo(&reader));
	UT_ASSERT_EQ(truncate_calls, 2);
	UT_ASSERT(memcmp(&page, &saved, BLCKSZ) == 0);
	UT_ASSERT_EQ(dirty_calls, 4); /* Two pages, CREATE and first TRUNCATE only. */
	UT_ASSERT_EQ(fake_allocations, 2);
	UT_ASSERT_EQ(fake_frees, fake_allocations);
}

UT_TEST(test_truncate_mismatch_or_foreign_input_does_not_touch_files)
{
	XLogReaderState reader;
	DecodedXLogRecord decoded;
	PGAlignedBlock saved;

	reset();
	truncate_record(&reader, &decoded);
	/* No claim that the legacy local minRecoveryPoint is a foreign-WAL
	 * durability coordinate. The per-origin input binding is still required. */
	cluster_recmerge_apply_foreign = true;
	saved = page;
	UT_ASSERT(!cluster_space_relation_redo(&reader));
	UT_ASSERT_EQ(truncate_calls, 0);
	UT_ASSERT_EQ(flush_calls, 0);
	UT_ASSERT(memcmp(&page, &saved, BLCKSZ) == 0);
	cluster_recmerge_apply_foreign = false;
	reader.cluster_expected_thread_id = 2;
	UT_ASSERT(!cluster_space_relation_redo(&reader));
	UT_ASSERT_EQ(truncate_calls, 0);
	UT_ASSERT_EQ(flush_calls, 0);
	reader.cluster_expected_thread_id = 1;
	((PageHeader)page.data)->pd_block_scn = 99;
	saved = page;
	UT_ASSERT(!cluster_space_relation_redo(&reader));
	UT_ASSERT_EQ(truncate_calls, 0);
	UT_ASSERT_EQ(main_blocks, 10);
	UT_ASSERT_EQ(flush_calls, 0);
	UT_ASSERT(memcmp(&page, &saved, BLCKSZ) == 0);
	UT_ASSERT(!pinned && !locked);
}

UT_TEST(test_truncate_replay_preserves_native_auxiliary_forks)
{
	XLogReaderState reader;
	DecodedXLogRecord decoded;

	reset();
	truncate_record(&reader, &decoded);
	auxiliary_forks = true;
	smgr_redo(&reader);
	UT_ASSERT_EQ(main_blocks, 4);
	UT_ASSERT_EQ(truncate_calls, 1);
	UT_ASSERT_EQ(fsm_vacuums, 1);
	UT_ASSERT_EQ(blocks, 2); /* SPACE was not physically truncated. */
	UT_ASSERT_EQ(fake_allocations, 1);
	UT_ASSERT_EQ(fake_frees, fake_allocations);
	UT_ASSERT_EQ(((PageHeader)page.data)->pd_block_scn, 18);
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT(!pinned && !locked);
}

UT_TEST(test_aborted_drop_keeps_existing_live_identity)
{
	RelationData rel;
	FormData_pg_class form;
	PGAlignedBlock saved;

	reset();
	UT_ASSERT(cluster_space_relation_create(locator));
	saved = page;
	native_owner = true;
	memset(&rel, 0, sizeof(rel));
	memset(&form, 0, sizeof(form));
	rel.rd_locator = locator;
	rel.rd_backend = InvalidBackendId;
	rel.rd_smgr = &storage;
	rel.rd_rel = &form;
	form.relpersistence = RELPERSISTENCE_PERMANENT;
	storage.smgr_owner = &rel.rd_smgr;
	RelationDropStorage(&rel);
	UT_ASSERT_EQ(ko_calls, 1);
	UT_ASSERT_EQ(unlink_calls, 0);
	UT_ASSERT(memcmp(&page, &saved, BLCKSZ) == 0);
	smgrDoPendingDeletes(false);
	UT_ASSERT_EQ(unlink_calls, 0);
	UT_ASSERT(memcmp(&page, &saved, BLCKSZ) == 0);
	UT_ASSERT_EQ(wal_calls, 1); /* No early irreversible tombstone. */
}

UT_TEST(test_subabort_forgets_drop_without_tombstoning)
{
	RelationData rel;
	FormData_pg_class form;
	PGAlignedBlock saved;

	reset();
	UT_ASSERT(cluster_space_relation_create(locator));
	saved = page;
	native_owner = true;
	nest_level = 2;
	memset(&rel, 0, sizeof(rel));
	memset(&form, 0, sizeof(form));
	rel.rd_locator = locator;
	rel.rd_backend = InvalidBackendId;
	rel.rd_smgr = &storage;
	rel.rd_rel = &form;
	form.relpersistence = RELPERSISTENCE_PERMANENT;
	storage.smgr_owner = &rel.rd_smgr;
	RelationDropStorage(&rel);
	AtSubAbort_smgr();
	nest_level = 1;
	smgrDoPendingDeletes(true);
	UT_ASSERT_EQ(unlink_calls, 0);
	UT_ASSERT(memcmp(&page, &saved, BLCKSZ) == 0);
	UT_ASSERT_EQ(wal_calls, 1);
}

UT_TEST(test_create_partial_restart_validates_before_extending)
{
	XLogReaderState reader = {0};
	DecodedXLogRecord decoded = {0};
	PGAlignedBlock saved;
	unsigned creates;

	reset();
	UT_ASSERT(cluster_space_relation_create(locator));
	decoded.header.xl_rmid = RM_SMGR_ID;
	decoded.header.xl_info = XLOG_SMGR_SPACE_IDENTITY | XLR_SPECIAL_REL_UPDATE;
	decoded.main_data = (char *)wal_bytes;
	decoded.main_data_len = sizeof(wal_bytes);
	decoded.max_block_id = -1;
	reader.record = &decoded;
	reader.EndRecPtr = UINT64_C(0x20000200);
	reader.cluster_expected_thread_id = 1;
	recovering = true;
	blocks = 1;
	memset(&pages[1], 0, BLCKSZ);
	((PageHeader)page.data)->pd_block_scn = 999;
	saved = page;
	creates = create_calls;
	dirty_calls = 0;
	UT_ASSERT(!cluster_space_relation_redo(&reader));
	UT_ASSERT_EQ(blocks, 1);
	UT_ASSERT_EQ(create_calls, creates);
	UT_ASSERT_EQ(dirty_calls, 0);
	UT_ASSERT(memcmp(&saved, &page, BLCKSZ) == 0);
	((PageHeader)page.data)->pd_block_scn = 17;
	saved = page;
	UT_ASSERT(cluster_space_relation_redo(&reader));
	UT_ASSERT_EQ(blocks, 2);
	UT_ASSERT_EQ(dirty_calls, 1);
	UT_ASSERT(memcmp(&saved, &page, BLCKSZ) == 0);
	UT_ASSERT(cluster_space_reservation_page_valid(pages[1].data, BLCKSZ));
	decoded.main_data_len = CLUSTER_SPACE_WAL_BYTES;
	UT_ASSERT(!cluster_space_relation_redo(&reader));
	UT_ASSERT_EQ(dirty_calls, 1);
	UT_ASSERT(!pinned && !locked);
}

UT_TEST(test_truncate_bad_reservation_cannot_change_identity_or_files)
{
	XLogReaderState reader;
	DecodedXLogRecord decoded;
	ClusterSpaceStructureChange change;
	PGAlignedBlock saved[2];
	unsigned dirties;

	reset();
	truncate_record(&reader, &decoded);
	UT_ASSERT(cluster_space_structure_wal_decode(wal_bytes, sizeof(wal_bytes), &change));
	change.reservation.before.next_block++;
	UT_ASSERT(cluster_space_reservation_page_encode(&change.reservation.before,
		change.reservation.before_token, pages[1].data, BLCKSZ));
	memcpy(saved, pages, sizeof(pages));
	dirties = dirty_calls;
	UT_ASSERT(!cluster_space_relation_redo(&reader));
	UT_ASSERT_EQ(truncate_calls, 0);
	UT_ASSERT_EQ(flush_calls, 0);
	UT_ASSERT_EQ(dirty_calls, dirties);
	UT_ASSERT_EQ(main_blocks, 10);
	UT_ASSERT(memcmp(saved, pages, sizeof(pages)) == 0);
	UT_ASSERT(!pinned && !locked);
}

UT_TEST(test_truncate_partial_restart_finishes_original_physical_owner)
{
	for (unsigned installed = 0; installed < 4; installed++) {
		XLogReaderState reader;
		DecodedXLogRecord decoded;
		ClusterSpaceStructureChange change;

		reset();
		truncate_record(&reader, &decoded);
		UT_ASSERT(cluster_space_structure_wal_decode(wal_bytes, sizeof(wal_bytes), &change));
		if (installed & 1)
			UT_ASSERT(cluster_space_identity_page_encode(&change.identity.result,
				change.identity.result_token, page.data, BLCKSZ));
		if (installed & 2)
			UT_ASSERT(cluster_space_reservation_page_encode(&change.reservation.result,
				change.reservation.result_token, pages[1].data, BLCKSZ));
		dirty_calls = 0;
		UT_ASSERT(cluster_space_relation_redo(&reader));
		UT_ASSERT_EQ(truncate_calls, 1);
		UT_ASSERT_EQ(flush_calls, 1);
		UT_ASSERT_EQ(main_blocks, 4);
		UT_ASSERT_EQ(dirty_calls, 2 - ((installed & 1) != 0) - ((installed & 2) != 0));
		UT_ASSERT_EQ(((PageHeader)page.data)->pd_block_scn, change.identity.result_token);
		UT_ASSERT_EQ(((PageHeader)pages[1].data)->pd_block_scn, change.reservation.result_token);
		UT_ASSERT(!pinned && !locked);
	}
}

static void
reservation_owner(ClusterSpaceIdentity *identity, HwLock *lock)
{
	ClusterSpaceStructureChange creation;

	reset();
	if (!cluster_space_relation_create(locator)
		|| !cluster_space_structure_wal_decode(wal_bytes, sizeof(wal_bytes), &creation))
		abort();
	*identity = creation.identity.result;
	memset(lock, 0, sizeof(*lock));
	lock->held = lock->coordinated = true;
	lock->req.lockmode = ExclusiveLock;
	cluster_hw_resid_encode(locator, MAIN_FORKNUM, &lock->req.resid);
	reserve_owner = true;
}

UT_TEST(test_reservation_owner_uses_exact_page_and_flushes_before_grant)
{
	ClusterSpaceIdentity identity;
	ClusterSpaceReservationChange advance = {0};
	HwLock lock;
	uint32 granted = 99;

	reservation_owner(&identity, &lock);
	UT_ASSERT_EQ(cluster_space_reserve(&identity, &lock, 7, &granted), 0);
	UT_ASSERT_EQ(granted, 7);
	UT_ASSERT_EQ(flush_calls, 1);
	UT_ASSERT_EQ(wal_calls, 2);
	UT_ASSERT_EQ(wal_info, XLOG_SMGR_SPACE_RESERVATION | XLR_SPECIAL_REL_UPDATE);
	UT_ASSERT(cluster_space_reservation_wal_decode(wal_bytes, registered_len, &advance));
	UT_ASSERT_EQ(advance.before_token, 17);
	UT_ASSERT_EQ(advance.result_token, 18);
	UT_ASSERT_EQ(advance.before.next_block, 0);
	UT_ASSERT_EQ(advance.result.next_block, 7);
	UT_ASSERT_EQ(cluster_space_reserve(&identity, &lock, 3, &granted), 7);
	UT_ASSERT_EQ(granted, 3);
	UT_ASSERT_EQ(flush_calls, 2);
	UT_ASSERT_EQ(blocks, 2);
	UT_ASSERT_EQ(main_blocks, 10); /* Caller still owns DATA extension. */
	UT_ASSERT(!pinned && !locked);
}

UT_TEST(test_reservation_owner_requires_identity_hw_and_exact_pcm_permission)
{
	for (int variant = 0; variant < 6; variant++) {
		ClusterSpaceIdentity identity;
		HwLock lock;
		PGAlignedBlock saved[2];
		uint32 granted = 99;

		reservation_owner(&identity, &lock);
		memcpy(saved, pages, sizeof(pages));
		if (variant == 0) lock.held = false;
		if (variant == 1) lock.coordinated = false;
		if (variant == 2) lock.req.resid.field2++;
		if (variant == 3) identity.incarnation[15]++;
		if (variant == 4) writer_allowed = false;
		if (variant == 5) ref.claim.database_incarnation++;
		UT_ASSERT_EQ(cluster_space_reserve(&identity, &lock, 7, &granted), InvalidBlockNumber);
		UT_ASSERT_EQ(granted, 0);
		UT_ASSERT_EQ(wal_calls, 1);
		UT_ASSERT_EQ(flush_calls, 0);
		UT_ASSERT(memcmp(saved, pages, sizeof(pages)) == 0);
		UT_ASSERT(!pinned && !locked);
	}
}

UT_TEST(test_revoked_after_flush_never_grants_or_reuses_the_reservation)
{
	ClusterSpaceIdentity identity;
	HwLock lock;
	uint32 granted = 99;

	reservation_owner(&identity, &lock);
	revoke_on_flush = true;
	UT_ASSERT_EQ(cluster_space_reserve(&identity, &lock, 4, &granted), InvalidBlockNumber);
	UT_ASSERT_EQ(granted, 0);
	UT_ASSERT_EQ(flush_calls, 1);
	UT_ASSERT_EQ(wal_calls, 2);
	/* The durable but unreturned range is not rolled back. A subsequent
	 * legitimate current-X owner continues after it. */
	revoke_on_flush = false;
	writer_allowed = true;
	UT_ASSERT_EQ(cluster_space_reserve(&identity, &lock, 2, &granted), 4);
	UT_ASSERT_EQ(granted, 2);
	UT_ASSERT_EQ(flush_calls, 2);
	UT_ASSERT(!pinned && !locked);
}

UT_TEST(test_reservation_owner_last_block_then_exhausted)
{
	ClusterSpaceIdentity identity;
	ClusterSpaceReservation state = {0};
	HwLock lock;
	uint32 granted = 99;

	reservation_owner(&identity, &lock);
	state.identity = identity;
	state.next_block = MaxBlockNumber;
	UT_ASSERT(cluster_space_reservation_page_encode(&state, 17, pages[1].data, BLCKSZ));
	UT_ASSERT_EQ(cluster_space_reserve(&identity, &lock, 2, &granted), MaxBlockNumber);
	UT_ASSERT_EQ(granted, 1);
	UT_ASSERT_EQ(cluster_space_reserve(&identity, &lock, 2, &granted), InvalidBlockNumber);
	UT_ASSERT_EQ(granted, 0);
	UT_ASSERT_EQ(flush_calls, 1);
	UT_ASSERT_EQ(wal_calls, 2);
}

static void
reservation_record(XLogReaderState *reader, DecodedXLogRecord *decoded)
{
	ClusterSpaceIdentity identity;
	HwLock lock;
	PGAlignedBlock before;
	uint32 granted;

	reservation_owner(&identity, &lock);
	before = pages[1];
	if (cluster_space_reserve(&identity, &lock, 7, &granted) != 0 || granted != 7)
		abort();
	pages[1] = before;
	memset(reader, 0, sizeof(*reader));
	memset(decoded, 0, sizeof(*decoded));
	decoded->header.xl_rmid = RM_SMGR_ID;
	decoded->header.xl_info = XLOG_SMGR_SPACE_RESERVATION | XLR_SPECIAL_REL_UPDATE;
	decoded->main_data = (char *)wal_bytes;
	decoded->main_data_len = CLUSTER_SPACE_RESERVATION_WAL_BYTES;
	decoded->max_block_id = -1;
	reader->record = decoded;
	reader->EndRecPtr = UINT64_C(0x20000400);
	reader->cluster_expected_thread_id = 1;
	recovering = reject_current_ref = true;
	dirty_calls = flush_calls = wal_calls = 0;
}

UT_TEST(test_reservation_native_replay_exact_before_and_duplicate)
{
	XLogReaderState reader;
	DecodedXLogRecord decoded;
	PGAlignedBlock identity, result;
	ClusterSpaceReservation reservation;
	ClusterSpaceReservationChange change;
	uint64 token;

	reservation_record(&reader, &decoded);
	identity = page;
	UT_ASSERT(cluster_space_reservation_wal_decode(wal_bytes,
		CLUSTER_SPACE_RESERVATION_WAL_BYTES, &change));
	/* Numeric LSN order must not hide an exact predecessor. */
	PageSetLSN(pages[1].data, reader.EndRecPtr + 1000);
	UT_ASSERT(cluster_space_relation_redo(&reader));
	UT_ASSERT(cluster_space_reservation_page_decode(pages[1].data, BLCKSZ, SPACE_FORKNUM,
		1, &change.result.identity.key, &reservation, &token));
	UT_ASSERT_EQ(reservation.next_block, 7);
	UT_ASSERT_EQ(token, 18);
	UT_ASSERT_EQ(PageGetLSN(pages[1].data), reader.EndRecPtr);
	UT_ASSERT_EQ(dirty_calls, 1);
	UT_ASSERT_EQ(flush_calls + wal_calls + truncate_calls, 0);
	UT_ASSERT(memcmp(identity.data, page.data, BLCKSZ) == 0);
	result = pages[1];
	/* Also exercise the real rmgr dispatcher for the already-result case. */
	if (token == 18)
		smgr_redo(&reader);
	UT_ASSERT_EQ(dirty_calls, 1);
	UT_ASSERT(memcmp(result.data, pages[1].data, BLCKSZ) == 0);
	UT_ASSERT(!pinned && !locked);
}

UT_TEST(test_reservation_replay_refusal_never_changes_target)
{
	for (int variant = 0; variant < 12; variant++) {
		XLogReaderState reader;
		DecodedXLogRecord decoded;
		PGAlignedBlock saved[2];

		reservation_record(&reader, &decoded);
		if (variant == 0) ref.claim.database_incarnation++;
		if (variant == 1) reader.cluster_expected_thread_id = 2;
		if (variant == 2) cluster_recmerge_apply_foreign = true;
		if (variant == 3) decoded.max_block_id = 0;
		if (variant == 4) decoded.main_data_len = 24;
		if (variant == 5) ((PageHeader)pages[1].data)->pd_block_scn++;
		if (variant == 6) writer_allowed = false;
		if (variant == 7) blocks = 1;
		if (variant >= 8) {
			ClusterSpaceIdentity wrong;
			uint64 token;
			ClusterSpaceReservationChange c;

			UT_ASSERT(cluster_space_reservation_wal_decode(wal_bytes,
				CLUSTER_SPACE_RESERVATION_WAL_BYTES, &c));
			UT_ASSERT(cluster_space_identity_page_decode(page.data, BLCKSZ,
				SPACE_FORKNUM, 0, &c.result.identity.key, &wrong, &token));
			if (variant == 8) wrong.incarnation[15]++;
			if (variant == 9) wrong.sequence++;
			UT_ASSERT(cluster_space_identity_page_encode(&wrong, token, page.data, BLCKSZ));
			if (variant == 10) memset(page.data, 0, BLCKSZ);
			if (variant == 11) page.data[BLCKSZ - 1] = 1;
		}
		memcpy(saved, pages, sizeof(pages));
		UT_ASSERT(!cluster_space_relation_redo(&reader));
		UT_ASSERT(memcmp(saved, pages, sizeof(pages)) == 0);
		UT_ASSERT_EQ(dirty_calls + wal_calls + flush_calls + truncate_calls, 0);
		UT_ASSERT(!pinned && !locked);
	}
}

UT_TEST(test_reservation_opcode_cannot_perform_structural_init)
{
	XLogReaderState reader;
	DecodedXLogRecord decoded;
	ClusterSpaceStructureChange pair;
	PGAlignedBlock saved[2];
	const char *name = smgr_identify(XLOG_SMGR_SPACE_RESERVATION);

	UT_ASSERT(name != NULL);
	if (name != NULL) UT_ASSERT(strcmp(name, "SPACE_RESERVATION") == 0);
	reservation_record(&reader, &decoded);
	recovering = reserve_owner = reject_current_ref = false;
	exists = false;
	blocks = 0;
	memset(pages, 0, sizeof(pages));
	UT_ASSERT(cluster_space_relation_create(locator));
	UT_ASSERT(cluster_space_structure_wal_decode(wal_bytes, sizeof(wal_bytes), &pair));
	UT_ASSERT(cluster_space_reservation_wal_encode(&pair.reservation, wal_bytes,
		CLUSTER_SPACE_RESERVATION_WAL_BYTES));
	recovering = reserve_owner = true;
	dirty_calls = wal_calls = 0;
	memcpy(saved, pages, sizeof(pages));
	UT_ASSERT(!cluster_space_relation_redo(&reader));
	UT_ASSERT(memcmp(saved, pages, sizeof(pages)) == 0);
	UT_ASSERT_EQ(dirty_calls + wal_calls + flush_calls, 0);
}

UT_TEST(test_private_owner_requires_exact_range_and_releases_hw)
{
	ClusterSpaceIdentity identity;
	HwLock lock;

	reservation_owner(&identity, &lock);
	fail_hw_lock = true;
	UT_ASSERT(!cluster_space_reserve_exact(&identity, 0, 4));
	UT_ASSERT_EQ(wal_calls, 1);
	fail_hw_lock = false;
	UT_ASSERT(cluster_space_reserve_exact(&identity, 0, 4));
	UT_ASSERT(!hw_held);
	/* Another range can never be mistaken for an empty destination. */
	UT_ASSERT(!cluster_space_reserve_exact(&identity, 0, 4));
	UT_ASSERT(!hw_held);
	UT_ASSERT(cluster_space_reserve_exact(&identity, 8, 1));
	UT_ASSERT(!hw_held);
	UT_ASSERT_EQ(flush_calls, 3);
}

UT_TEST(test_private_owner_releases_hw_on_wal_flush_error)
{
	ClusterSpaceIdentity identity;
	HwLock lock;
	volatile bool caught = false;

	reservation_owner(&identity, &lock);
	throw_reserve_flush = true;
	PG_TRY();
	{
		(void)cluster_space_reserve_exact(&identity, 0, 4);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT(!hw_held);
	UT_ASSERT_EQ(CritSectionCount, 0);
}

static Relation
native_truncate_relation(void)
{
	ClusterSpaceIdentity identity;
	ClusterSpaceReservation reservation = {0};
	HwLock lock;
	Relation rel;

	reservation_owner(&identity, &lock);
	reservation.identity = identity;
	reservation.next_block = 10;
	if (!cluster_space_reservation_page_encode(&reservation, 23, pages[1].data, BLCKSZ))
		abort();
	rel = CreateFakeRelcacheEntry(locator);
	rel->rd_id = locator.relNumber;
	rel->rd_smgr = &storage;
	truncate_owner = true;
	wal_calls = dirty_calls = flush_calls = 0;
	return rel;
}

UT_TEST(test_native_truncate_logs_pair_after_durable_base_before_publish)
{
	ClusterSpaceStructureChange change;
	Relation rel = native_truncate_relation();
	ClusterSpaceIdentity identity;
	ClusterSpaceReservation reservation;
	uint64 token;

	RelationTruncate(rel, 4);
	UT_ASSERT_EQ(wal_info, XLOG_SMGR_SPACE_IDENTITY | XLR_SPECIAL_REL_UPDATE);
	UT_ASSERT_EQ(main_blocks, 4);
	UT_ASSERT_EQ(truncate_calls, 1);
	UT_ASSERT_EQ(relation_flushes, 1);
	UT_ASSERT_EQ(fork_syncs, 2);
	UT_ASSERT_EQ(invalidations, 1);
	UT_ASSERT_EQ(MyProc->delayChkptFlags, 0);
	UT_ASSERT(cluster_space_structure_wal_decode(wal_bytes, registered_len, &change));
	if (registered_len == CLUSTER_SPACE_STRUCTURE_WAL_BYTES) {
		UT_ASSERT(cluster_space_identity_page_decode(page.data, BLCKSZ, SPACE_FORKNUM, 0,
			&change.identity.result.key, &identity, &token));
		UT_ASSERT_EQ(token, change.identity.result_token);
		UT_ASSERT_EQ(identity.sequence, 2);
		UT_ASSERT(memcmp(identity.incarnation, change.identity.expected.incarnation, 16) != 0);
		UT_ASSERT(cluster_space_reservation_page_decode(pages[1].data, BLCKSZ, SPACE_FORKNUM, 1,
			&identity.key, &reservation, &token));
		UT_ASSERT_EQ(reservation.next_block, 4);
		UT_ASSERT_EQ(token, change.identity.result_token);
	}
	UT_ASSERT(!pinned && !locked);
	FreeFakeRelcacheEntry(rel);
}

UT_TEST(test_native_truncate_bad_pair_refuses_before_physical_change)
{
	Relation rel = native_truncate_relation();
	PGAlignedBlock saved[2];
	volatile bool caught = false;

	pages[1].data[BLCKSZ - 1] = 1;
	memcpy(saved, pages, sizeof(pages));
	expecting_error = true;
	PG_TRY(); { RelationTruncate(rel, 4); }
	PG_CATCH(); { caught = true; } PG_END_TRY();
	expecting_error = false;
	UT_ASSERT(caught);
	UT_ASSERT_EQ(main_blocks, 10);
	UT_ASSERT_EQ(truncate_calls + wal_calls + dirty_calls + invalidations, 0);
	UT_ASSERT(memcmp(saved, pages, sizeof(pages)) == 0);
	UT_ASSERT(!pinned && !locked);
	FreeFakeRelcacheEntry(rel);
}

static void
emit_drop_commit(ClusterSpaceDropState *state, const RelFileLocator *deletes, int count)
{
	uint32 len;
	const char *data = cluster_space_drop_wal(state, &len);

	cluster_space_drop_mark_dirty(state);
	XactLogCommitRecord(1, 0, NULL, count, unconstify(RelFileLocator *, deletes),
		0, NULL, 0, NULL, false, 0, InvalidTransactionId, NULL, InvalidScn, NULL, data, len);
}

UT_TEST(test_drop_pair_stays_live_until_native_commit_is_durable)
{
	Relation rel = native_truncate_relation();
	RelFileLocator deletes[2] = {locator, locator};
	ClusterSpaceDropState *state;
	ClusterSpaceStructureChange change;
	ClusterSpaceIdentity identity;
	ClusterSpaceReservation reservation;
	PGAlignedBlock saved[2];
	uint64 token;

	drop_owner = true;
	memcpy(saved, pages, sizeof(saved));
	state = cluster_space_drop_prepare(deletes, 2);
	UT_ASSERT(state != NULL);
	if (state == NULL) { FreeFakeRelcacheEntry(rel); return; }
	UT_ASSERT(memcmp(saved, pages, sizeof(saved)) == 0);
	UT_ASSERT_EQ(wal_calls + dirty_calls, 0);
	START_CRIT_SECTION();
	MyProc->delayChkptFlags |= DELAY_CHKPT_START;
	emit_drop_commit(state, deletes, 2);
	UT_ASSERT_EQ(wal_calls, 1); /* Native pending delete can contain duplicates. */
	UT_ASSERT(memcmp(saved, pages, sizeof(saved)) == 0);
	XLogFlush(UINT64_C(0x10000300)); /* Original forced COMMIT flush. */
	cluster_space_drop_publish(state, UINT64_C(0x10000300));
	UT_ASSERT(cluster_space_structure_wal_decode(wal_bytes, registered_len, &change));
	UT_ASSERT(cluster_space_identity_page_decode(page.data, BLCKSZ, SPACE_FORKNUM, 0,
		&change.identity.result.key, &identity, &token));
	UT_ASSERT_EQ(identity.state, CLUSTER_SPACE_IDENTITY_TOMBSTONED);
	UT_ASSERT_EQ(identity.sequence, 2);
	UT_ASSERT_EQ(token, change.identity.result_token);
	UT_ASSERT(memcmp(identity.incarnation, change.identity.expected.incarnation, 16) == 0);
	UT_ASSERT(cluster_space_reservation_page_decode(pages[1].data, BLCKSZ, SPACE_FORKNUM, 1,
		&identity.key, &reservation, &token));
	UT_ASSERT_EQ(reservation.next_block, 10);
	UT_ASSERT_EQ(PageGetLSN(page.data), UINT64_C(0x10000300));
	UT_ASSERT_EQ(PageGetLSN(pages[1].data), UINT64_C(0x10000300));
	MyProc->delayChkptFlags &= ~DELAY_CHKPT_START;
	END_CRIT_SECTION();
	cluster_space_drop_finish(state);
	UT_ASSERT_EQ(unlink_calls + truncate_calls + fork_syncs + relation_flushes, 0);
	UT_ASSERT(!locked && !pinned);
	FreeFakeRelcacheEntry(rel);
}

UT_TEST(test_drop_precommit_failure_or_abandonment_preserves_live_pages)
{
	for (int corrupt = 0; corrupt <= 1; corrupt++) {
		Relation rel = native_truncate_relation();
		ClusterSpaceDropState *state;
		PGAlignedBlock saved[2];

		drop_owner = true;
		if (corrupt)
			pages[1].data[BLCKSZ - 1] = 1;
		memcpy(saved, pages, sizeof(saved));
		state = cluster_space_drop_prepare(&locator, 1);
		UT_ASSERT((state != NULL) == !corrupt);
		if (state != NULL)
			cluster_space_drop_finish(state);
		UT_ASSERT(memcmp(saved, pages, sizeof(saved)) == 0);
		UT_ASSERT_EQ(unlink_calls + truncate_calls + wal_calls + dirty_calls, 0);
		UT_ASSERT(!locked && !pinned);
		FreeFakeRelcacheEntry(rel);
	}
}

UT_TEST(test_native_commit_logs_decides_and_publishes_before_checkpoint_release)
{
	Relation rel = native_truncate_relation();
	ClusterSpaceDropState *space_drop;
	const char *space_drop_data;
	uint32 space_drop_len;
	bool wrote_xlog = true, markXidCommitted = true, forceSyncCommit = false;
	bool RelcacheInitFileInval = false, has_tt_fold = false, commit_record_flushed = false;
	int nchildren = 0, nrels = 1, ndroppedstats = 0, nmsgs = 0;
	int synchronous_commit = SYNCHRONOUS_COMMIT_OFF;
	TransactionId xid = 501, *children = NULL;
	RelFileLocator *rels = &locator;
	xl_xact_stats_item *droppedstats = NULL;
	SharedInvalidationMessage *invalMessages = NULL;
	SCN commit_scn = InvalidScn, tt_commit_scn = InvalidScn;
	xl_xact_tt_commit tt_fold = {0};
	ClusterSpaceIdentity identity;
	ClusterSpaceIdentityKey expected;
	uint64 token;

	drop_owner = true;
	UT_ASSERT(cluster_space_relation_read_identity(locator, &identity));
	expected = identity.key;
	native_owner = true;
	storage.smgr_owner = &rel->rd_smgr;
	RelationDropStorage(rel);
	nrels = smgrGetPendingDeletes(true, &rels);
	UT_ASSERT_EQ(nrels, 1);
	space_drop = cluster_space_drop_prepare(rels, nrels);
	UT_ASSERT(space_drop != NULL);
	if (space_drop == NULL) { FreeFakeRelcacheEntry(rel); return; }
	space_drop_data = cluster_space_drop_wal(space_drop, &space_drop_len);
#include "test_cluster_space_commit.inc"
	UT_ASSERT_EQ(native_commits, 1);
	UT_ASSERT_EQ(commit_decisions, 1);
	UT_ASSERT(commit_record_flushed);
	UT_ASSERT_EQ(MyProc->delayChkptFlags, 0);
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT(space_drop == NULL);
	UT_ASSERT(!pinned && !locked);
	UT_ASSERT(cluster_space_identity_page_decode(page.data, BLCKSZ, SPACE_FORKNUM, 0,
		&expected, &identity, &token));
	UT_ASSERT_EQ(identity.state, CLUSTER_SPACE_IDENTITY_TOMBSTONED);
	if (space_drop != NULL)
		cluster_space_drop_finish(space_drop); /* Release the pre-fix RED fixture. */
	smgrDoPendingDeletes(true);
	UT_ASSERT_EQ(unlink_calls, 1);
	pfree(rels);
	FreeFakeRelcacheEntry(rel);
}

static Relation
drop_replay_record(XLogReaderState *reader, DecodedXLogRecord *decoded)
{
	Relation rel = native_truncate_relation();
	ClusterSpaceDropState *state;

	drop_owner = true;
	state = cluster_space_drop_prepare(&locator, 1);
	if (state == NULL)
		abort();
	START_CRIT_SECTION();
	MyProc->delayChkptFlags |= DELAY_CHKPT_START;
	emit_drop_commit(state, &locator, 1);
	/* Crash cut after COMMIT WAL, before publishing SPACE or unlinking.
	 * Restart begins at this record with no preceding intent state. */
	MyProc->delayChkptFlags = 0;
	END_CRIT_SECTION();
	cluster_space_drop_finish(state);
	memset(reader, 0, sizeof(*reader));
	memset(decoded, 0, sizeof(*decoded));
	decoded->header.xl_rmid = RM_XACT_ID;
	decoded->header.xl_info = wal_info;
	decoded->header.xl_xid = 501;
	decoded->main_data = (char *)commit_bytes;
	decoded->main_data_len = commit_len;
	decoded->max_block_id = -1;
	reader->record = decoded;
	reader->system_identifier = 1;
	reader->ReadRecPtr = UINT64_C(0x10000200);
	reader->EndRecPtr = UINT64_C(0x10000300);
	reader->cluster_expected_thread_id = 1;
	recovering = reject_current_ref = true;
	dirty_calls = wal_calls = flush_calls = 0;
	return rel;
}

static void
native_commit_redo_prefix(XLogReaderState *record, xl_xact_parsed_commit *parsed,
						  TransactionId xid)
{
#include "test_cluster_space_commit_redo.inc"
}

UT_TEST(test_atomic_drop_commit_replays_all_partial_tombstones)
{
	for (unsigned mask = 0; mask < 4; mask++) {
		XLogReaderState reader;
		DecodedXLogRecord decoded;
		Relation rel = drop_replay_record(&reader, &decoded);
		ClusterSpaceStructureChange change;
		ClusterSpaceIdentity identity;
		xl_xact_parsed_commit parsed;
		uint64 token;

		UT_ASSERT(cluster_space_structure_wal_decode(wal_bytes, registered_len, &change));
		if (mask & 1)
			UT_ASSERT(cluster_space_identity_page_encode(&change.identity.result,
				change.identity.result_token, page.data, BLCKSZ));
		if (mask & 2)
			UT_ASSERT(cluster_space_reservation_page_encode(&change.reservation.result,
				change.reservation.result_token, pages[1].data, BLCKSZ));
		UT_ASSERT(ParseCommitRecord(wal_info, (xl_xact_commit *)commit_bytes, commit_len, &parsed));
		memset(wal_bytes, 0xEE, sizeof(wal_bytes)); /* No prior record retained. */
		native_commit_redo_prefix(&reader, &parsed, 501);
		UT_ASSERT(cluster_space_identity_page_decode(page.data, BLCKSZ, SPACE_FORKNUM, 0,
			&change.identity.result.key, &identity, &token));
		UT_ASSERT_EQ(identity.state, CLUSTER_SPACE_IDENTITY_TOMBSTONED);
		UT_ASSERT_EQ(token, change.identity.result_token);
		UT_ASSERT_EQ(PageGetLSN(page.data), reader.EndRecPtr);
		UT_ASSERT_EQ(flush_calls, 1);
		UT_ASSERT_EQ(dirty_calls, 2);
		UT_ASSERT(!pinned && !locked);
		FreeFakeRelcacheEntry(rel);
	}
}

UT_TEST(test_drop_commit_refuses_missing_or_unbound_payload_before_mutation)
{
	for (int bad = 0; bad < 11; bad++) {
		XLogReaderState reader;
		DecodedXLogRecord decoded;
		Relation rel = drop_replay_record(&reader, &decoded);
		PGAlignedBlock saved[2];
		xl_xact_parsed_commit parsed;
		uint32 xinfo, count;
		char *tail;

		UT_ASSERT(ParseCommitRecord(wal_info, (xl_xact_commit *)commit_bytes, commit_len, &parsed));
		tail = unconstify(char *, parsed.space_drops) - sizeof(uint32);
		switch (bad) {
		case 0:
			memcpy(&xinfo, commit_bytes + MinSizeOfXactCommit, sizeof(xinfo));
			xinfo &= ~XACT_XINFO_HAS_SPACE_DROP;
			memcpy(commit_bytes + MinSizeOfXactCommit, &xinfo, sizeof(xinfo));
			decoded.main_data_len = tail - (char *)commit_bytes;
			break;
		case 1: decoded.header.xl_xid++; break;
		case 2: reader.cluster_expected_thread_id++; break;
		case 3: cluster_recmerge_apply_foreign = true; break;
		case 4: pages[1].data[BLCKSZ - 1] = 1; break;
		case 5: exists = false; break; /* Missing SPACE is not proof MAIN is absent. */
		case 6: /* Valid but duplicate typed payload cannot name one deletion twice. */
			memcpy(commit_bytes + commit_len, parsed.space_drops, CLUSTER_SPACE_STRUCTURE_WAL_BYTES);
			decoded.main_data_len += CLUSTER_SPACE_STRUCTURE_WAL_BYTES;
			count = 2; memcpy(tail, &count, sizeof(count)); break;
		case 7: count = UINT32_MAX; memcpy(tail, &count, sizeof(count)); break;
		case 8: tail[sizeof(uint32)] ^= 1; break; /* Invalid typed magic. */
		case 9: {
			ClusterSpaceStructureChange foreign;

			UT_ASSERT(cluster_space_structure_wal_decode(parsed.space_drops,
				CLUSTER_SPACE_STRUCTURE_WAL_BYTES, &foreign));
			foreign.identity.expected.key.locator.relNumber++;
			foreign.identity.result.key.locator.relNumber++;
			foreign.reservation.before.identity.key.locator.relNumber++;
			foreign.reservation.result.identity.key.locator.relNumber++;
			UT_ASSERT(cluster_space_structure_wal_encode(&foreign,
				unconstify(char *, parsed.space_drops), CLUSTER_SPACE_STRUCTURE_WAL_BYTES));
			break;
		}
		case 10: reader.system_identifier++; break;
		}
		memcpy(saved, pages, sizeof(saved));
		UT_ASSERT(!cluster_space_drop_replay_commit(&reader, 501));
		UT_ASSERT(memcmp(saved, pages, sizeof(saved)) == 0);
		UT_ASSERT_EQ(dirty_calls + flush_calls + truncate_calls + unlink_calls, 0);
		UT_ASSERT(!pinned && !locked);
		FreeFakeRelcacheEntry(rel);
	}
}

UT_TEST(test_standalone_tombstone_has_no_commit_authority)
{
	XLogReaderState reader;
	DecodedXLogRecord decoded;
	Relation rel = drop_replay_record(&reader, &decoded);
	PGAlignedBlock saved[2];

	memcpy(saved, pages, sizeof(saved));
	decoded.header.xl_rmid = RM_SMGR_ID;
	decoded.header.xl_info = XLOG_SMGR_SPACE_IDENTITY | XLR_SPECIAL_REL_UPDATE;
	decoded.main_data = (char *)wal_bytes;
	decoded.main_data_len = sizeof(wal_bytes);
	UT_ASSERT(!cluster_space_relation_redo(&reader));
	UT_ASSERT(memcmp(saved, pages, sizeof(saved)) == 0);
	UT_ASSERT(!cluster_space_drop_replay_commit(&reader, 501));
	UT_ASSERT_EQ(dirty_calls + flush_calls + unlink_calls, 0);
	UT_ASSERT(!pinned && !locked);
	FreeFakeRelcacheEntry(rel);
}

UT_TEST(test_drop_replay_accepts_only_complete_physical_absence)
{
	XLogReaderState reader;
	DecodedXLogRecord decoded;
	Relation rel = drop_replay_record(&reader, &decoded);

	deleted_all = true;
	UT_ASSERT(cluster_space_drop_replay_commit(&reader, 501));
	UT_ASSERT_EQ(flush_calls, 1);
	UT_ASSERT_EQ(dirty_calls + unlink_calls, 0);
	UT_ASSERT(!pinned && !locked);
	FreeFakeRelcacheEntry(rel);
}

UT_TEST(test_checkpoint_redo_may_begin_at_drop_commit)
{
	XLogReaderState reader;
	DecodedXLogRecord decoded;
	Relation rel = drop_replay_record(&reader, &decoded);

	/* CreateCheckPoint selects redo before waiting DELAY_CHKPT_START.
	 * The preceding intent may therefore be outside restart input. */
	UT_ASSERT(cluster_space_drop_replay_commit(&reader, 501));
	UT_ASSERT_EQ(((PageHeader)page.data)->pd_block_scn, 18);
	UT_ASSERT(!pinned && !locked);
	FreeFakeRelcacheEntry(rel);
}

UT_TEST(test_native_commit_optional_tail_and_bounded_parser)
{
	Relation rel = native_truncate_relation();
	ClusterSpaceDropState *state;
	xl_xact_parsed_commit parsed, sentinel;
	TransactionId children[2] = {502, 503};
	xl_xact_stats_item stats = {0};
	SharedInvalidationMessage msg = {0};
	xl_xact_tt_commit tt = {0};
	uint32 len;
	const char *data;

	drop_owner = true;
	state = cluster_space_drop_prepare(&locator, 1);
	UT_ASSERT(state != NULL);
	if (state == NULL) { FreeFakeRelcacheEntry(rel); return; }
	data = cluster_space_drop_wal(state, &len);
	forceSyncCommit = true;
	replorigin_session_origin = 7;
	replorigin_session_origin_lsn = 456;
	replorigin_session_origin_timestamp = 789;
	START_CRIT_SECTION();
	MyProc->delayChkptFlags |= DELAY_CHKPT_START;
	cluster_space_drop_mark_dirty(state);
	XactLogCommitRecord(123, 2, children, 1, &locator, 1, &stats, 1, &msg,
		true, XACT_FLAGS_ACQUIREDACCESSEXCLUSIVELOCK, InvalidTransactionId, NULL,
		42, &tt, data, len);
	MyProc->delayChkptFlags = 0;
	END_CRIT_SECTION();
	cluster_space_drop_finish(state);
	UT_ASSERT(ParseCommitRecord(wal_info, (xl_xact_commit *)commit_bytes, commit_len, &parsed));
	UT_ASSERT_EQ(parsed.nsubxacts, 2);
	UT_ASSERT_EQ(parsed.nstats, 1);
	UT_ASSERT_EQ(parsed.nmsgs, 1);
	UT_ASSERT_EQ(parsed.scn, 42);
	UT_ASSERT_EQ(parsed.origin_timestamp, 789);
	UT_ASSERT(parsed.has_tt_commit && parsed.nspace_drops == 1);
	memset(&sentinel, 0xAA, sizeof(sentinel));
	for (Size truncated = 0; truncated < commit_len; truncated++) {
		parsed = sentinel;
		UT_ASSERT(!ParseCommitRecord(wal_info, (xl_xact_commit *)commit_bytes, truncated, &parsed));
		UT_ASSERT(memcmp(&parsed, &sentinel, sizeof(parsed)) == 0);
	}
	UT_ASSERT(!ParseCommitRecord(wal_info, (xl_xact_commit *)commit_bytes, commit_len + 1, &parsed));
	UT_ASSERT(!ParseCommitRecord(wal_info | XLOG_XACT_COMMIT_PREPARED,
		(xl_xact_commit *)commit_bytes, commit_len, &parsed));
	UT_ASSERT_EQ(((PageHeader)page.data)->pd_block_scn, 17);
	UT_ASSERT_EQ(flush_calls + unlink_calls, 0);
	FreeFakeRelcacheEntry(rel);
}

UT_TEST(test_native_prepared_commit_preserves_empty_gid)
{
	Relation rel = native_truncate_relation();
	xl_xact_parsed_commit parsed;

	drop_owner = plain_commit_emitter = true;
	wal_level = WAL_LEVEL_LOGICAL;
	START_CRIT_SECTION();
	MyProc->delayChkptFlags = DELAY_CHKPT_START;
	XactLogCommitRecord(123, 0, NULL, 0, NULL, 0, NULL, 0, NULL, false,
		0, 501, "", InvalidScn, NULL, NULL, 0);
	MyProc->delayChkptFlags = 0;
	END_CRIT_SECTION();
	memset(&parsed, 0, sizeof(parsed));
	UT_ASSERT(ParseCommitRecord(wal_info, (xl_xact_commit *)commit_bytes, commit_len, &parsed));
	UT_ASSERT_EQ(parsed.twophase_xid, 501);
	UT_ASSERT(parsed.twophase_gid[0] == '\0' && parsed.nspace_drops == 0);
	/* The native redo prefix has no SPACE work for this prepared commit. */
	native_commit_redo_prefix(NULL, &parsed, 501);
	UT_ASSERT_EQ(wal_calls, 1);
	UT_ASSERT_EQ(dirty_calls + flush_calls, 0);
	commit_bytes[commit_len - 1] = 'x';
	UT_ASSERT(!ParseCommitRecord(wal_info, (xl_xact_commit *)commit_bytes, commit_len, &parsed));
	FreeFakeRelcacheEntry(rel);
}

int
main(void)
{
	setvbuf(stdout, NULL, _IONBF, 0);
	UT_PLAN(37);
	UT_RUN(test_real_create_binds_selected_namespace_and_wal);
	UT_RUN(test_no_create_on_legacy_or_unproved_namespace);
	UT_RUN(test_existing_identity_is_never_recreated);
	UT_RUN(test_identity_read_is_exact_and_never_creates);
	UT_RUN(test_recovery_identity_uses_only_restart_namespace);
	UT_RUN(test_real_replay_exact_duplicate_and_preserved_token);
	UT_RUN(test_native_create_registers_abort_cleanup_before_space);
	UT_RUN(test_native_descriptor_recognizes_typed_record);
	UT_RUN(test_truncate_replay_checks_identity_before_native_shrink);
	UT_RUN(test_truncate_mismatch_or_foreign_input_does_not_touch_files);
	UT_RUN(test_truncate_replay_preserves_native_auxiliary_forks);
	UT_RUN(test_aborted_drop_keeps_existing_live_identity);
	UT_RUN(test_subabort_forgets_drop_without_tombstoning);
	UT_RUN(test_create_partial_restart_validates_before_extending);
	UT_RUN(test_truncate_bad_reservation_cannot_change_identity_or_files);
	UT_RUN(test_truncate_partial_restart_finishes_original_physical_owner);
	UT_RUN(test_reservation_owner_uses_exact_page_and_flushes_before_grant);
	UT_RUN(test_reservation_owner_requires_identity_hw_and_exact_pcm_permission);
	UT_RUN(test_revoked_after_flush_never_grants_or_reuses_the_reservation);
	UT_RUN(test_reservation_owner_last_block_then_exhausted);
	UT_RUN(test_reservation_native_replay_exact_before_and_duplicate);
	UT_RUN(test_reservation_replay_refusal_never_changes_target);
	UT_RUN(test_reservation_opcode_cannot_perform_structural_init);
	UT_RUN(test_private_owner_requires_exact_range_and_releases_hw);
	UT_RUN(test_private_owner_releases_hw_on_wal_flush_error);
	UT_RUN(test_native_truncate_logs_pair_after_durable_base_before_publish);
	UT_RUN(test_native_truncate_bad_pair_refuses_before_physical_change);
	UT_RUN(test_drop_pair_stays_live_until_native_commit_is_durable);
	UT_RUN(test_drop_precommit_failure_or_abandonment_preserves_live_pages);
	UT_RUN(test_native_commit_logs_decides_and_publishes_before_checkpoint_release);
	UT_RUN(test_atomic_drop_commit_replays_all_partial_tombstones);
	UT_RUN(test_drop_commit_refuses_missing_or_unbound_payload_before_mutation);
	UT_RUN(test_standalone_tombstone_has_no_commit_authority);
	UT_RUN(test_drop_replay_accepts_only_complete_physical_absence);
	UT_RUN(test_checkpoint_redo_may_begin_at_drop_commit);
	UT_RUN(test_native_commit_optional_tail_and_bounded_parser);
	UT_RUN(test_native_prepared_commit_preserves_empty_gid);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
