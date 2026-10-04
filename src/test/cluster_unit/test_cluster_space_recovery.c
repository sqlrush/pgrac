/*-------------------------------------------------------------------------
 *
 * test_cluster_space_recovery.c
 *    Native SPACE recovery owner with explicit transport/buffer fixtures.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_space_recovery.c
 *
 * NOTES
 *    Exact codec and recovery owner are real; GCS/root grants are fixtures.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "cluster/cluster_page_wal.h"
#include <unistd.h>

#include "access/xlog.h"
#include "access/twophase.h"
#include "catalog/pg_tablespace_d.h"
#include "catalog/storage_xlog.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_cold_recovery.h"
#include "cluster/cluster_hw.h"
#include "cluster/cluster_pcm_x_bufmgr.h"
#include "cluster/cluster_mode.h"
#include "cluster/cluster_recovery_merge.h"
#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_sf_dep.h"
#include "cluster/cluster_space_recovery.h"
#include "cluster/cluster_wal_tail.h"
#include "cluster/cluster_tt_durable.h"
#include "cluster/cluster_xid_stripe.h"
#include "cluster/storage/cluster_smgr.h"
#include "miscadmin.h"
#include "executor/instrument.h"
#include "pg_trace.h"
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "storage/buf_internals.h"
#include "storage/smgr.h"
#include "storage/checksum.h"
#include "storage/checksum_impl.h"
#include "utils/resowner.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
volatile uint32 CritSectionCount;
volatile uint32 InterruptHoldoffCount;
int NBuffers = 2, NLocBuffer;
char *BufferBlocks;
Block *LocalBufferBlockPointers;
bool cluster_enabled = true, cluster_shared_config = true;
static BufferDescPadded descriptors[2];
BufferDescPadded *BufferDescriptors = descriptors;
static PGAlignedBlock pages[2];
static FILE *file;
static SMgrRelationData relation;
static ClusterSpaceReservationChange changes[2];
static uint8 payload[2][CLUSTER_SPACE_RESERVATION_WAL_BYTES];
static RfSideOnlineOperationV1 operations[2];
static ClusterRecoveryDutyKey duties[2];
static ClusterControlRootSnapshot roots[2];
static ClusterControlRootReadToken root_tokens[2];
static ClusterThreadRecoveryAuthorityV1 sources[2];
static ClusterRecoverySerialGuard serials[2];
AuxProcType MyAuxProcType = NotAnAuxProcess;
static bool recovery = true, cold_current = true;
static bool cold_window = true, merge_claim = true;
static uint16 cold_origins = 2;
static ClusterRecoveryFencePlan *cold_fence = (void *)7;
static ClusterSpaceIdentityKey key;
static RfContributorStreamCutV1 cuts[2];
static XLogRecPtr native_redo[2];
static XLogRecPtr source_redo[2];
static uint64 source_database[2];
static bool source_unavailable;
static unsigned pins, locks, writes, syncs, reads, lock_calls;
static bool hw_held, permitted, stale, exists, corrupt_disk, structural, wrong_cut;
static bool creating;
static BlockNumber existing_blocks;
static unsigned creates, extensions, x_locks;
static int fail_write_block;
static int corrupt_after_sync_block;
static bool corrupt_checksum, checksums, ignore_checksum_failure;
static int throw_at;
int cluster_node_id;
bool cluster_smart_fusion, cluster_past_image;
ClusterPcmOwnEntry *ClusterPcmOwnArray;
BufferUsage pgBufferUsage;
static bool sf_blocked, stale_during_io, stale_after_write;
static unsigned cold_truncates;
static int cold_last_truncate_flags;
static bool fail_cold_truncate, stale_after_truncate;
static BlockNumber cold_main_blocks;
static bool checkpoint_during_flush;
static unsigned wal_flushes, io_aborts;
static bool cluster_pcm_x_finish_retain_flush_active;
static bool cluster_pcm_x_finish_retain_flush_io_active;
static bool cluster_pcm_x_finish_retain_flush_error_context_pushed;
static ErrorContextCallback *cluster_pcm_x_finish_retain_flush_error_context_previous;
#ifdef ENABLE_INJECTION
static bool cluster_pcm_x_finish_retain_flush_fault_active;
#define CLUSTER_INJECTION_POINT(name) ((void)0)
#define cluster_injection_should_skip(name) false
#endif
static const ClusterThreadRecoveryFabricPlanV1 *fabric = (void *)1;
static RfSideOnlinePlanV1 *side;
static ResourceOwner source_owner = (void *)6;

static bool
cold_protected(void)
{
	return MyAuxProcType == StartupProcess && recovery && cold_current && cold_origins == 2
		   && serials[0].held && serials[1].held
		   && serials[0].mode == CLUSTER_RECOVERY_SERIAL_COLD_FORMED
		   && serials[1].mode == CLUSTER_RECOVERY_SERIAL_COLD_FORMED;
}

uint16
cluster_recovery_merge_fence_plan_origin_count(const ClusterRecoveryFencePlan *plan)
{
	return plan == cold_fence ? cold_origins : 0;
}

bool
cluster_recovery_merge_fence_plan_revalidate_nowait(ClusterRecoveryFencePlan *plan)
{
	return plan == cold_fence && cold_current;
}

bool
cluster_recovery_merge_fence_plan_origin(const ClusterRecoveryFencePlan *plan, uint16 index,
										 uint16 *thread, ClusterControlRootSnapshot *root,
										 ClusterControlRootReadToken *token)
{
	if (plan != cold_fence || index >= cold_origins || index >= 2)
		return false;
	*thread = duties[index].origin_thread_id;
	*root = roots[index];
	memset(token, 0, sizeof(*token));
	return true;
}

bool
cluster_recovery_merge_fence_plan_authority(ClusterRecoveryFencePlan *plan, uint16 thread,
											ClusterThreadRecoveryAuthorityV1 *out)
{
	if (plan != cold_fence || !cold_current)
		return false;
	for (uint16 i = 0; i < cold_origins && i < 2; i++)
		if (thread == duties[i].origin_thread_id) {
			*out = sources[i];
			return true;
		}
	return false;
}

ClusterControlRootResult
cluster_control_root_recovery_source_v1(const ClusterControlRootSnapshot *root,
										const ClusterControlRootReadToken *token,
										ClusterWalSourceRef *out, XLogRecPtr *redo)
{
	UT_ASSERT(CurrentResourceOwner == source_owner);
	UT_ASSERT(!pins && !locks && !hw_held);
	for (int i = 0; i < 2; i++)
		if (!source_unavailable && memcmp(root, &roots[i], sizeof(*root)) == 0
			&& memcmp(token, &root_tokens[i], sizeof(*token)) == 0) {
			memset(out, 0, sizeof(*out));
			out->claim.identity = duties[i];
			out->claim.database_incarnation = source_database[i];
			*redo = source_redo[i];
			return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
		}
	return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
}

bool
cluster_cold_replay_window_active_v1(void)
{
	return cold_window;
}

bool
cluster_recovery_merge_claim_is_held(void)
{
	return merge_claim;
}

ClusterWalPinResult
cluster_wal_retention_pin_borrow_cold_v1(ClusterWalRetentionPin **pin,
										 ClusterRecoverySerialGuard **guards, uint16 capacity,
										 uint16 *count)
{
	if (CurrentResourceOwner != source_owner || !cold_current || stale || capacity < cold_origins)
		return CLUSTER_WAL_PIN_STALE;
	for (uint16 i = 0; i < cold_origins; i++)
		if (!serials[i].held || serials[i].mode != CLUSTER_RECOVERY_SERIAL_COLD_FORMED)
			return CLUSTER_WAL_PIN_STALE;
	for (uint16 i = 0; i < cold_origins; i++)
		guards[i] = &serials[i];
	*pin = (void *)3;
	*count = cold_origins;
	return CLUSTER_WAL_PIN_OK;
}

ClusterControlRootResult
cluster_control_root_read_canonical(uint16 tid, const ClusterRecoveryDutyKey *duty,
									ClusterControlRootReadMode mode,
									ClusterControlRootSnapshot *root,
									ClusterControlRootReadToken *token)
{
	UT_ASSERT(CurrentResourceOwner == source_owner && !pins && !locks && !hw_held);
	UT_ASSERT_EQ(mode, CLUSTER_CONTROL_ROOT_READ_STRONG);
	for (uint16 i = 0; i < 2; i++)
		if (tid == duties[i].origin_thread_id && memcmp(duty, &duties[i], sizeof(*duty)) == 0) {
			*root = roots[i];
			*token = root_tokens[i];
			return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
		}
	return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
}
/* A SPACE-only source must never query transaction/2PC state. */
uint64
GetSystemIdentifier(void)
{
	abort();
}
int
cluster_xid_origin_slot(TransactionId xid)
{
	abort();
}
bool
cluster_tt_slot_durable_read_exact_stable(uint32 segment, uint16 slot, TransactionId xid,
										  uint16 wrap, TTSlot *out)
{
	abort();
}
TwoPhaseRecoveryPendingResult
TwoPhaseRecoveryPendingReadExact(TransactionId xid, Oid database, const char *gid, void **out,
								 uint32 *length)
{
	abort();
}
ResourceOwner CurrentResourceOwner;
static ResourceOwner child_owner = (void *)5;
ResourceOwner
ResourceOwnerCreate(ResourceOwner parent, const char *name)
{
	UT_ASSERT(parent == source_owner && CurrentResourceOwner == source_owner);
	return child_owner;
}
void
ResourceOwnerDelete(ResourceOwner owner)
{
	UT_ASSERT_EQ(pins, 0);
}
void
ResourceOwnerRelease(ResourceOwner owner, ResourceReleasePhase phase, bool commit, bool top)
{
	UT_ASSERT(owner == child_owner && CurrentResourceOwner == child_owner);
	if (phase == RESOURCE_RELEASE_BEFORE_LOCKS) {
		UT_ASSERT_EQ(locks, 0);
		pins = 0;
	}
}

void
pg_re_throw(void)
{
	if (PG_exception_stack)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}
void
ExceptionalCondition(const char *a, const char *b, int c)
{
	abort();
}
static void
fault(int at)
{
	if (throw_at == at) {
		InterruptHoldoffCount = 0;
		pg_re_throw();
	}
}

const RfSideOnlinePlanV1 *
cluster_thread_recovery_fabric_side_plan_v1(const ClusterThreadRecoveryFabricPlanV1 *p)
{
	return p == fabric ? side : NULL;
}
uint32
cluster_thread_recovery_fabric_participant_count_v1(const ClusterThreadRecoveryFabricPlanV1 *p)
{
	return p == fabric ? 2 : 0;
}
bool
cluster_thread_recovery_fabric_identity_matches_v1(const ClusterThreadRecoveryFabricPlanV1 *p,
												   uint64 sysid, const uint8 uuid[16])
{
	return p == fabric && sysid == key.system_identifier && !memcmp(uuid, key.storage_uuid, 16);
}
bool
cluster_thread_recovery_fabric_cut_v1(const ClusterThreadRecoveryFabricPlanV1 *p, uint32 i,
									  RfContributorStreamCutV1 *out)
{
	if (p != fabric || i >= 2)
		return false;
	*out = cuts[i];
	if (wrong_cut)
		out->scan_end_exclusive++;
	return true;
}
ClusterThreadRecoveryAuthorityResultV1
cluster_thread_recovery_authority_revalidate_nowait_v1(const ClusterThreadRecoveryAuthorityV1 *a)
{
	if (locks == 3)
		fault(6);
	for (uint16 i = 0; i < 2; i++)
		if (CurrentResourceOwner == source_owner && !stale
			&& memcmp(&a->root_snapshot->identity, a->duty, sizeof(*a->duty)) == 0
			&& a->serial_guard == sources[i].serial_guard
			&& a->retention_pin == sources[i].retention_pin && a->formation == sources[i].formation
			&& a->fence_need_set == sources[i].fence_need_set
			&& a->fence_admission_set == sources[i].fence_admission_set
			&& memcmp(a->duty, &duties[i], sizeof(*a->duty)) == 0
			&& memcmp(a->root_snapshot, &roots[i], sizeof(*a->root_snapshot)) == 0
			&& memcmp(a->root_token, &root_tokens[i], sizeof(*a->root_token)) == 0)
			return CLUSTER_THREAD_AUTHORITY_OK;
	return CLUSTER_THREAD_AUTHORITY_ROOT_STALE;
}
bool
cluster_thread_recovery_authority_covers_window_v1(const ClusterThreadRecoveryAuthorityV1 *a,
												   uint16 tid, XLogRecPtr lo, XLogRecPtr hi)
{
	return a->duty->origin_thread_id == tid && lo == 100 && hi == 200;
}
bool
cluster_hw_lock(const ClusterResId *id, HwLock *lock)
{
	UT_ASSERT(CurrentResourceOwner == source_owner);
	UT_ASSERT(!hw_held && !pins && !locks);
	memset(lock, 0, sizeof(*lock));
	lock->held = lock->coordinated = hw_held = true;
	lock->req.resid = *id;
	lock->req.lockmode = ExclusiveLock;
	lock_calls++;
	return true;
}
void
cluster_hw_unlock(HwLock *lock)
{
	UT_ASSERT(CurrentResourceOwner == source_owner);
	UT_ASSERT(!pins && !locks && hw_held);
	lock->held = hw_held = false;
}
int
cluster_smgr_which_for(RelFileLocator r, BackendId b)
{
	return 1;
}
SMgrRelation
smgropen(RelFileLocator r, BackendId b)
{
	UT_ASSERT(RelFileLocatorEquals(r, key.locator));
	return &relation;
}
bool
smgrexists(SMgrRelation r, ForkNumber f)
{
	return exists;
}
BlockNumber
smgrnblocks(SMgrRelation r, ForkNumber f)
{
	return existing_blocks;
}
void
smgrcreate(SMgrRelation r, ForkNumber fork, bool is_redo)
{
	UT_ASSERT((hw_held || cold_protected()) && CurrentResourceOwner == child_owner);
	UT_ASSERT_EQ(fork, SPACE_FORKNUM);
	UT_ASSERT(is_redo && !exists && existing_blocks == 0);
	exists = true;
	creates++;
}
Buffer
ReadBufferWithoutRelcache(RelFileLocator r, ForkNumber f, BlockNumber b, ReadBufferMode m,
						  BufferAccessStrategy s, bool perm)
{
	UT_ASSERT(CurrentResourceOwner == child_owner);
	UT_ASSERT((hw_held || cold_protected()) && f == SPACE_FORKNUM);
	if (b == P_NEW) {
		UT_ASSERT(creating && exists && existing_blocks < 2 && m == RBM_ZERO_AND_LOCK);
		b = existing_blocks++;
		extensions++;
		memset(pages[b].data, 0, BLCKSZ);
		UT_ASSERT_EQ(pwrite(fileno(file), pages[b].data, BLCKSZ, (off_t)b * BLCKSZ), BLCKSZ);
		pins |= 1 << b;
		LockBuffer(b + 1, BUFFER_LOCK_EXCLUSIVE);
		return b + 1;
	}
	UT_ASSERT(b < existing_blocks && b < 2 && m == RBM_NORMAL);
	pins |= 1 << b;
	fault(1);
	return b + 1;
}
void
LockBuffer(Buffer b, int mode)
{
	if (mode == BUFFER_LOCK_UNLOCK) {
		locks &= ~(1 << (b - 1));
		x_locks &= ~(1 << (b - 1));
		InterruptHoldoffCount--;
	} else {
		UT_ASSERT(pins & (1 << (b - 1)));
		locks |= 1 << (b - 1);
		if (mode == BUFFER_LOCK_EXCLUSIVE)
			x_locks |= 1 << (b - 1);
		InterruptHoldoffCount++;
		fault(2);
	}
}
void
ReleaseBuffer(Buffer b)
{
	pins &= ~(1 << (b - 1));
}
bool
LWLockHeldByMe(LWLock *lock)
{
	for (int i = 0; i < 2; i++)
		if (lock == BufferDescriptorGetContentLock(&descriptors[i].bufferdesc))
			return (locks & (1 << i)) != 0;
	return false;
}
bool
LWLockHeldByMeInMode(LWLock *lock, LWLockMode mode)
{
	return LWLockHeldByMe(lock)
		   && (mode != LW_EXCLUSIVE
			   || (lock == BufferDescriptorGetContentLock(&descriptors[0].bufferdesc)
					   ? (x_locks & 1) != 0
					   : (x_locks & 2) != 0));
}
bool
cluster_bufmgr_pcm_x_content_holder_write_permitted(BufferDesc *b)
{
	UT_ASSERT((locks & (1 << b->buf_id)) && (pins & (1 << b->buf_id))
			  && (x_locks & (1 << b->buf_id)) && (hw_held || cold_protected()));
	return permitted;
}
BlockNumber
BufferGetBlockNumber(Buffer b)
{
	return b - 1;
}
void
BufferGetTag(Buffer b, RelFileLocator *r, ForkNumber *f, BlockNumber *n)
{
	*r = key.locator;
	*f = SPACE_FORKNUM;
	*n = b - 1;
}
void
MarkBufferDirty(Buffer b)
{
	UT_ASSERT(b == 2 || ((creating || structural) && b == 1));
	UT_ASSERT(x_locks & (1 << (b - 1)));
	UT_ASSERT(CritSectionCount > 0);
	pg_atomic_fetch_or_u32(&descriptors[b - 1].bufferdesc.state, BM_DIRTY | BM_JUST_DIRTIED);
}
bool
PageIsVerifiedForFork(Page p, ForkNumber f, BlockNumber b, int flags)
{
	static const PGAlignedBlock zero;
	if (memcmp(p, zero.data, BLCKSZ) == 0)
		return true;
	return f == SPACE_FORKNUM
		   && (!checksums || ignore_checksum_failure
			   || ((PageHeader)p)->pd_checksum == pg_checksum_page(p, b))
		   && (b == 0 ? cluster_space_identity_page_valid(p, BLCKSZ)
					  : cluster_space_reservation_page_valid(p, BLCKSZ));
}
void
smgrread(SMgrRelation r, ForkNumber f, BlockNumber b, void *out)
{
	UT_ASSERT_EQ(f, SPACE_FORKNUM);
	reads++;
	fault(3);
	UT_ASSERT_EQ(pread(fileno(file), out, BLCKSZ, (off_t)b * BLCKSZ), BLCKSZ);
	if (corrupt_disk && writes)
		((char *)out)[200] ^= 1;
	if (corrupt_checksum && writes && b == 1)
		((PageHeader)out)->pd_checksum ^= 1;
	if (syncs != 0 && (int)b == corrupt_after_sync_block)
		((PageHeader)out)->pd_checksum ^= 1;
}
void
smgrimmedsync(SMgrRelation r, ForkNumber f)
{
	UT_ASSERT(CurrentResourceOwner == child_owner);
	UT_ASSERT(locks == 3 && (hw_held || cold_protected()));
	fault(5);
	UT_ASSERT_EQ(fsync(fileno(file)), 0);
	syncs++;
}
void
smgrwrite(SMgrRelation r, ForkNumber f, BlockNumber b, const void *data, bool skip)
{
	UT_ASSERT(b == 1 || ((creating || structural) && b == 0));
	UT_ASSERT_EQ(f, SPACE_FORKNUM);
	fault(4);
	if ((int)b == fail_write_block) {
		InterruptHoldoffCount = 0;
		pg_re_throw();
	}
	writes++;
	UT_ASSERT_EQ(pwrite(fileno(file), data, BLCKSZ, (off_t)b * BLCKSZ), BLCKSZ);
	if (stale_after_write)
		stale = true;
}
bool
smgr_cold_truncate_preflight(const xl_smgr_truncate *truncate,
							 const ClusterSpaceRecoveryBatchV1 *batch)
{
	UT_ASSERT(CurrentResourceOwner == child_owner);
	UT_ASSERT(cluster_space_recovery_truncate_preflight_permitted_v1(batch, truncate));
	if (pins != 3)
		UT_ASSERT(!cluster_space_recovery_truncate_permitted_v1(batch, truncate));
	return !fail_cold_truncate;
}

bool
smgr_redo_cold_truncate(const xl_smgr_truncate *truncate, const ClusterSpaceRecoveryBatchV1 *batch)
{
	xl_smgr_truncate wrong = *truncate;

	UT_ASSERT(cluster_space_recovery_truncate_permitted_v1(batch, truncate));
	wrong.blkno++;
	UT_ASSERT(!cluster_space_recovery_truncate_permitted_v1(batch, &wrong));
	wrong = *truncate;
	wrong.flags ^= SMGR_TRUNCATE_HEAP;
	UT_ASSERT(!cluster_space_recovery_truncate_permitted_v1(batch, &wrong));
	wrong = *truncate;
	wrong.rlocator.relNumber++;
	UT_ASSERT(!cluster_space_recovery_truncate_permitted_v1(batch, &wrong));
	UT_ASSERT(CurrentResourceOwner == child_owner && locks == 3 && x_locks == 3 && pins == 3);
	UT_ASSERT_EQ(wal_flushes, 0);
	if (fail_cold_truncate)
		return false;
	cold_last_truncate_flags = truncate->flags;
	if ((truncate->flags & SMGR_TRUNCATE_HEAP) != 0 && cold_main_blocks > truncate->blkno) {
		cold_main_blocks = truncate->blkno;
		cold_truncates++;
	}
	if (stale_after_truncate)
		stale = true;
	return true;
}
uint32
LockBufHdr(BufferDesc *buf)
{
	return pg_atomic_fetch_or_u32(&buf->state, BM_LOCKED) | BM_LOCKED;
}
static bool
StartBufferIO(BufferDesc *buf, bool input)
{
	uint32 state = pg_atomic_read_u32(&buf->state);
	if (!(state & BM_DIRTY))
		return false;
	UT_ASSERT(!(state & BM_IO_IN_PROGRESS));
	pg_atomic_fetch_or_u32(&buf->state, BM_IO_IN_PROGRESS);
	if (stale_during_io)
		stale = true;
	return true;
}
static void
TerminateBufferIO(BufferDesc *buf, bool clear, uint32 flags)
{
	uint32 state = pg_atomic_read_u32(&buf->state) & ~BM_IO_IN_PROGRESS;
	if (clear)
		state &= ~(BM_DIRTY | BM_JUST_DIRTIED | BM_IO_ERROR);
	pg_atomic_write_u32(&buf->state, state | flags);
}
void
AbortBufferIO(Buffer b)
{
	io_aborts++;
	TerminateBufferIO(GetBufferDescriptor(b - 1), false, BM_IO_ERROR);
}
bool
RecoveryInProgress(void)
{
	return recovery;
}
static bool cold_write_allowed = true;
bool
cluster_page_wal_cold_redo_write_allowed_v1(void)
{
	return cold_write_allowed;
}
XLogRecPtr
GetXLogInsertRecPtr(void)
{
	return 100;
}
void
XLogFlush(XLogRecPtr lsn)
{
	wal_flushes++;
}
bool
cluster_sf_dep_buffer_flush_blocked(BufferDesc *buf)
{
	if (checkpoint_during_flush)
		pg_atomic_fetch_or_u32(&buf->state, BM_CHECKPOINT_NEEDED);
	return sf_blocked;
}
void
cluster_gcs_block_pi_write_note(BufferTag tag, uint64 scn)
{
	abort();
}
char *
PageSetChecksumCopy(Page page, BlockNumber block)
{
	static PGAlignedBlock copy;
	if (!checksums)
		return page;
	memcpy(copy.data, page, BLCKSZ);
	((PageHeader)copy.data)->pd_checksum = pg_checksum_page(copy.data, block);
	return copy.data;
}
bool
DataChecksumsEnabled(void)
{
	return checksums;
}
static bool
cluster_bufmgr_pcm_x_retained_image_locked(BufferDesc *buf, uint32 state)
{
	return false;
}
static void
shared_buffer_write_error_callback(void *arg)
{
	abort();
}
#define BufHdrGetBlock(buf) ((Block)(BufferBlocks + (Size)(buf)->buf_id * BLCKSZ))
#define BufferGetLSN(buf) PageGetLSN(BufHdrGetBlock(buf))
#define BufferIsPinned(buf) ((pins & (1 << ((buf) - 1))) != 0)
#define pgstat_prepare_io_time() ((instr_time){ 0 })
#define pgstat_count_io_op_time(a, b, c, d, e) ((void)(d))
#undef ereport
#define ereport(level, rest)                                                                       \
	do {                                                                                           \
		InterruptHoldoffCount = 0;                                                                 \
		pg_re_throw();                                                                             \
	} while (0)
/* This suite supplies only the original SPACE owner, never PAGE evidence. */
#define cluster_page_wal_snapshot_v1(buffer, out) ((void)(buffer), (void)(out), false)
#define cluster_page_wal_same_mutation_v1(a, b) ((void)(a), (void)(b), false)
#define cluster_page_wal_flush_source_v1(a, b) ((void)(a), (void)(b), false)
/* SPACE recovery writes skip the first-record bookkeeping (R-A22). */
#define cluster_page_wal_first_observe_locked_v1(buf, out)                                         \
	((void)(buf), (void)(out), CLUSTER_PAGE_WAL_FIRST_ABSENT)
#define cluster_page_wal_first_clear_written_locked_v1(buf, observed, token)                       \
	((void)(buf), (void)(observed), (void)(token), false)
#include "test_cluster_space_recovery_flush.inc"

static void
make_plan(void)
{
	RfSideOnlinePlanRequestV1 request = { .system_identifier = 17,
										  .physical_cuts = cuts,
										  .participant_count = 2,
										  .redo_starts = native_redo };

	rf_side_online_plan_destroy_v1(&side);
	memcpy(request.storage_uuid, key.storage_uuid, 16);
	UT_ASSERT_EQ(rf_side_online_plan_create_v1(&request, &side), RF_PAGE_PROOF_DETAIL_OK);
	for (int i = 0; i < 2; i++) {
		XLogReaderState reader = { 0 };
		DecodedXLogRecord decoded = { 0 };
		RfDetachedRecordPlanV1 record = { 0 };
		RfPageOnlineRecordIdentityV1 identity = operations[i].identity;
		uint8 structural_bytes[CLUSTER_SPACE_STRUCTURE_WAL_BYTES];

		decoded.main_data = (char *)payload[i];
		decoded.main_data_len = sizeof(payload[i]);
		if (creating && i == 0) {
			ClusterSpaceStructureChange change = { 0 };
			change.identity.action = CLUSTER_SPACE_WAL_CREATE;
			change.identity.nblocks = InvalidBlockNumber;
			change.identity.result = changes[0].before.identity;
			change.identity.result_token = changes[0].before_token;
			change.reservation.action = CLUSTER_SPACE_RESERVATION_INIT;
			change.reservation.result = changes[0].before;
			change.reservation.result_token = changes[0].before_token;
			UT_ASSERT(cluster_space_structure_wal_encode(&change, structural_bytes,
														 sizeof(structural_bytes)));
			decoded.main_data = (char *)structural_bytes;
			decoded.main_data_len = sizeof(structural_bytes);
			identity.record.info = XLOG_SMGR_SPACE_IDENTITY | XLR_SPECIAL_REL_UPDATE;
		}
		if (structural && i == 1) {
			ClusterSpaceStructureChange change = { 0 };
			change.identity.action = CLUSTER_SPACE_WAL_TRUNCATE;
			change.identity.expected = changes[0].result.identity;
			change.identity.result = change.identity.expected;
			memset(change.identity.result.incarnation, 0x22, 16);
			change.identity.result.sequence++;
			change.identity.result.operation++;
			change.identity.nblocks = 5;
			change.identity.before_token = 901;
			change.identity.result_token = 19;
			change.reservation.action = CLUSTER_SPACE_RESERVATION_RESET;
			change.reservation.before = changes[0].result;
			change.reservation.result.identity = change.identity.result;
			change.reservation.result.next_block = change.reservation.first_block = 5;
			change.reservation.before_token = 80;
			change.reservation.result_token = 19;
			UT_ASSERT(cluster_space_structure_wal_encode(&change, structural_bytes,
														 sizeof(structural_bytes)));
			decoded.main_data = (char *)structural_bytes;
			decoded.main_data_len = sizeof(structural_bytes);
			identity.record.info = XLOG_SMGR_SPACE_IDENTITY | XLR_SPECIAL_REL_UPDATE;
		}
		decoded.max_block_id = -1;
		decoded.header.xl_rmid = identity.record.rmid;
		decoded.header.xl_info = identity.record.info;
		decoded.header.xl_crc = identity.record.record_crc;
		decoded.lsn = identity.record.read_rec_ptr;
		decoded.next_lsn = identity.record.end_rec_ptr;
		reader.record = &decoded;
		reader.system_identifier = identity.record.system_identifier;
		reader.ReadRecPtr = decoded.lsn;
		reader.EndRecPtr = decoded.next_lsn;
		record.source_record = &reader;
		record.preflight_complete = true;
		record.route.rmid = identity.record.rmid;
		record.route.normalized_info = identity.record.info & ~XLR_INFO_MASK;
		record.route.record_owner = RF_ROUTE_OWNER_SIDE_TYPED;
		record.route.block_policy = RF_ROUTE_BLOCKS_FORBIDDEN;
		record.route.codec_id = RF_ROUTE_CODEC_SIDE_STANDARD;
		UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(side, &record, &identity),
					 RF_PAGE_PROOF_DETAIL_OK);
	}
	UT_ASSERT(rf_side_online_plan_bind_database_v1(side, 42));
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(side), RF_PAGE_PROOF_DETAIL_OK);
}

static void
reset(void)
{
	MyAuxProcType = NotAnAuxProcess;
	recovery = cold_current = true;
	cold_window = merge_claim = true;
	cold_truncates = 0;
	cold_last_truncate_flags = 0;
	cold_main_blocks = 7;
	fail_cold_truncate = stale_after_truncate = false;
	cold_origins = 2;
	source_unavailable = false;
	memset(serials, 0, sizeof(serials));
	cold_write_allowed = true;
	CurrentResourceOwner = source_owner;
	checksums = true;
	ignore_checksum_failure = corrupt_checksum = false;
	if (file)
		fclose(file);
	file = tmpfile();
	if (!file)
		abort();
	pins = locks = writes = syncs = reads = lock_calls = 0;
	wal_flushes = io_aborts = 0;
	sf_blocked = stale_during_io = stale_after_write = false;
	InterruptHoldoffCount = 0;
	cluster_smart_fusion = false;
	hw_held = stale = corrupt_disk = structural = wrong_cut = false;
	creating = false;
	existing_blocks = 2;
	creates = extensions = x_locks = 0;
	fail_write_block = -1;
	corrupt_after_sync_block = -1;
	exists = permitted = true;
	throw_at = 0;
	memset(changes, 0, sizeof(changes));
	memset(operations, 0, sizeof(operations));
	memset(duties, 0, sizeof(duties));
	memset(roots, 0, sizeof(roots));
	memset(root_tokens, 0, sizeof(root_tokens));
	memset(sources, 0, sizeof(sources));
	memset(cuts, 0, sizeof(cuts));
	changes[0].action = CLUSTER_SPACE_RESERVATION_ADVANCE;
	changes[0].before.identity.key.system_identifier = 17;
	changes[0].before.identity.key.database_incarnation = 42;
	memset(changes[0].before.identity.key.storage_uuid, 0x51, 16);
	changes[0].before.identity.key.locator = (RelFileLocator){ DEFAULTTABLESPACE_OID, 5, 16384 };
	memset(changes[0].before.identity.incarnation, 0x21, 16);
	changes[0].before.identity.sequence = changes[0].before.identity.operation = 1;
	changes[0].before.identity.state = CLUSTER_SPACE_IDENTITY_LIVE;
	changes[0].before.next_block = changes[0].first_block = 3;
	changes[0].granted = 4;
	changes[0].result = changes[0].before;
	changes[0].result.next_block = 7;
	changes[0].before_token = 100;
	changes[0].result_token = 80;
	changes[1] = changes[0];
	changes[1].before = changes[0].result;
	changes[1].first_block = 7;
	changes[1].result.next_block = 11;
	changes[1].before_token = 80;
	changes[1].result_token = 19;
	key = changes[0].before.identity.key;
	for (int i = 0; i < 2; i++) {
		native_redo[i] = 100;
		source_redo[i] = 100;
		source_database[i] = 42;
		UT_ASSERT(
			cluster_space_reservation_wal_encode(&changes[i], payload[i], sizeof(payload[i])));
		operations[i].kind = RF_SIDE_ONLINE_OPERATION_SPACE;
		operations[i].space_key = key;
		operations[i].owned_payload = payload[i];
		operations[i].owned_payload_length = sizeof(payload[i]);
		operations[i].identity.participant_index = i;
		operations[i].identity.record
			= (RfPageReplayRecordIdentityV1){ .system_identifier = 17,
											  .origin_thread = i + 2,
											  .timeline_id = 7,
											  .read_rec_ptr = 100,
											  .end_rec_ptr = 200,
											  .record_crc = 55,
											  .rmid = RM_SMGR_ID,
											  .info = XLOG_SMGR_SPACE_RESERVATION
													  | XLR_SPECIAL_REL_UPDATE };
		memcpy(operations[i].identity.record.storage_uuid, key.storage_uuid, 16);
		duties[i].system_identifier = 17;
		duties[i].origin_thread_id = i + 2;
		duties[i].origin_owner_incarnation = 19;
		memcpy(duties[i].storage_uuid, key.storage_uuid, 16);
		roots[i].identity = duties[i];
		roots[i].checkpoint_tli = roots[i].tail_tli = 7;
		roots[i].checkpoint_lower_lsn = 100;
		roots[i].validated_tail_lsn_exclusive = 200;
		sources[i].duty = &duties[i];
		sources[i].root_snapshot = &roots[i];
		sources[i].root_token = &root_tokens[i];
		sources[i].retention_pin = (void *)3;
		serials[i].held = true;
		serials[i].mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED;
		serials[i].duty = duties[i];
		serials[i].root_read_token = root_tokens[i];
		sources[i].serial_guard = &serials[i];
		cuts[i] = (RfContributorStreamCutV1){ .failed_thread = i + 2,
											  .origin_owner_incarnation = 19,
											  .timeline_id = 7,
											  .flags = RF_CONTRIBUTOR_CUT_COMPLETE,
											  .scan_begin_inclusive = 100,
											  .scan_end_exclusive = 200 };
		descriptors[i].bufferdesc.buf_id = i;
		pg_atomic_init_u32(&descriptors[i].bufferdesc.state,
						   BM_VALID | BM_TAG_VALID | BM_PERMANENT);
		InitBufferTag(&descriptors[i].bufferdesc.tag, &key.locator, SPACE_FORKNUM, i);
	}
	BufferBlocks = pages[0].data;
	UT_ASSERT(cluster_space_identity_page_encode(&changes[0].before.identity, 901, pages[0].data,
												 BLCKSZ));
	UT_ASSERT(
		cluster_space_reservation_page_encode(&changes[0].before, 100, pages[1].data, BLCKSZ));
	for (BlockNumber i = 0; i < 2; i++)
		UT_ASSERT_EQ(
			pwrite(fileno(file), PageSetChecksumCopy(pages[i].data, i), BLCKSZ, (off_t)i * BLCKSZ),
			BLCKSZ);
	UT_ASSERT_EQ(fsync(fileno(file)), 0);
	make_plan();
}

static bool
space_begin(void *arg)
{
	return arg != NULL;
}
static void
space_end(void *arg, bool complete)
{
	UT_ASSERT(arg != NULL);
}

UT_TEST(test_actual_reservation_install_and_repeat)
{
	ClusterSpaceRecoveryBatchV1 *batch = NULL;
	ClusterSpaceReservation got;
	uint64 token;
	int origin;
	reset();
	UT_ASSERT(cluster_space_recovery_preflight_v1(fabric, sources, 2, &batch));
	if (!batch)
		return;
	UT_ASSERT_EQ(writes, 0);
	UT_ASSERT(!pins && !locks && !hw_held);
	{
		RfSideOnlineApplyOpsV1 ops
			= { .arg = batch,
				.begin_protected_set = space_begin,
				.end_protected_set = space_end,
				.preflight_space = cluster_space_recovery_preflight_operation_v1,
				.apply_space = cluster_space_recovery_applied_operation_v1 };
		UT_ASSERT_EQ(rf_side_online_plan_preflight_v1(side, &ops), RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(rf_side_online_plan_apply_v1(side, &ops),
					 RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE);
	}
	UT_ASSERT(cluster_space_recovery_apply_v1(batch));
	UT_ASSERT_EQ(writes, 1);
	UT_ASSERT_EQ(syncs, 1);
	UT_ASSERT_EQ(wal_flushes, 0);
	{
		RfSideOnlineApplyOpsV1 ops
			= { .arg = batch,
				.begin_protected_set = space_begin,
				.end_protected_set = space_end,
				.preflight_space = cluster_space_recovery_preflight_operation_v1,
				.apply_space = cluster_space_recovery_applied_operation_v1 };
		RfSideOnlineOperationV1 op;
		UT_ASSERT_EQ(rf_side_online_plan_apply_v1(side, &ops), RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT(rf_side_online_plan_operation_v1(side, 0, &op));
		UT_ASSERT(cluster_space_recovery_applied_operation_v1(batch, &op));
		op.identity.record.read_rec_ptr++;
		UT_ASSERT(!cluster_space_recovery_applied_operation_v1(batch, &op));
	}
	UT_ASSERT(cluster_space_reservation_page_decode(pages[1].data, BLCKSZ, SPACE_FORKNUM, 1, &key,
													&got, &token));
	UT_ASSERT_EQ(got.next_block, 11);
	UT_ASSERT_EQ(token, 19);
	UT_ASSERT_EQ(PageGetLSN(pages[1].data), 200);
	UT_ASSERT(PageGetLSNOrigin(pages[1].data, &origin));
	UT_ASSERT_EQ(origin, 2);
	cluster_space_recovery_destroy_v1(&batch);
	UT_ASSERT(cluster_space_recovery_preflight_v1(fabric, sources, 2, &batch));
	UT_ASSERT(cluster_space_recovery_apply_v1(batch));
	UT_ASSERT_EQ(writes, 2);
	UT_ASSERT_EQ(syncs, 2);
	cluster_space_recovery_destroy_v1(&batch);
	UT_ASSERT(!pins && !locks && !hw_held && batch == NULL);
}

UT_TEST(test_sources_and_structure_refuse_before_mutation)
{
	for (int variant = 0; variant < 10; variant++) {
		ClusterSpaceRecoveryBatchV1 *batch = NULL;
		reset();
		if (variant == 0)
			stale = true;
		if (variant == 1) {
			structural = true;
			make_plan();
		}
		if (variant == 2)
			sources[1].retention_pin = (void *)4;
		if (variant == 3)
			wrong_cut = true;
		if (variant == 4)
			roots[1].checkpoint_tli++;
		if (variant == 5)
			roots[1].identity.system_identifier++;
		if (variant == 7) {
			uint16 invalid = PGRAC_PAGE_LSN_ORIGIN_MAX + 2;
			operations[1].identity.record.origin_thread = invalid;
			duties[1].origin_thread_id = roots[1].identity.origin_thread_id = invalid;
			cuts[1].failed_thread = invalid;
			make_plan();
		}
		if (variant == 8)
			cuts[1].origin_owner_incarnation = duties[1].origin_owner_incarnation + 1;
		if (variant == 9)
			cuts[1].origin_owner_incarnation = 0;
		UT_ASSERT(
			!cluster_space_recovery_preflight_v1(fabric, sources, variant == 6 ? 1 : 2, &batch));
		UT_ASSERT(batch == NULL && writes == 0 && lock_calls == 0);
		cluster_space_recovery_destroy_v1(&batch);
	}
}

UT_TEST(test_target_and_late_authority_refuse)
{
	for (int variant = 0; variant < 5; variant++) {
		ClusterSpaceRecoveryBatchV1 *batch = NULL;
		reset();
		if (variant == 0)
			exists = false;
		if (variant == 1)
			permitted = false;
		if (variant == 2)
			pages[0].data[200] = 1;
		if (variant < 3)
			UT_ASSERT(!cluster_space_recovery_preflight_v1(fabric, sources, 2, &batch));
		else {
			UT_ASSERT(cluster_space_recovery_preflight_v1(fabric, sources, 2, &batch));
			if (variant == 3)
				stale = true;
			else
				((PageHeader)pages[1].data)->pd_block_scn = 777;
			UT_ASSERT(!cluster_space_recovery_apply_v1(batch));
		}
		UT_ASSERT_EQ(writes, 0);
		cluster_space_recovery_destroy_v1(&batch);
		UT_ASSERT(!pins && !locks && !hw_held);
	}
}

UT_TEST(test_disk_failure_never_completes)
{
	ClusterSpaceRecoveryBatchV1 *batch = NULL;
	reset();
	UT_ASSERT(cluster_space_recovery_preflight_v1(fabric, sources, 2, &batch));
	corrupt_disk = true;
	UT_ASSERT(!cluster_space_recovery_apply_v1(batch));
	cluster_space_recovery_destroy_v1(&batch);
	UT_ASSERT(!pins && !locks && !hw_held);
}

UT_TEST(test_error_releases_original_resources)
{
	for (int variant = 0; variant < 12; variant++) {
		int at = variant % 6 + 1;
		ClusterSpaceRecoveryBatchV1 *volatile batch = NULL;
		volatile bool caught = false;
		reset();
		if (variant >= 6) {
			MyAuxProcType = StartupProcess;
			UT_ASSERT(cluster_space_recovery_cold_preflight_v1(
				side, cold_fence, (ClusterSpaceRecoveryBatchV1 **)&batch));
		} else
			UT_ASSERT(cluster_space_recovery_preflight_v1(fabric, sources, 2,
														  (ClusterSpaceRecoveryBatchV1 **)&batch));
		if (!batch)
			continue;
		throw_at = at;
		PG_TRY();
		{
			(void)cluster_space_recovery_apply_v1(batch);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(caught);
		UT_ASSERT(!pins && !locks && !hw_held);
		UT_ASSERT(CurrentResourceOwner == source_owner);
		UT_ASSERT(!(pg_atomic_read_u32(&descriptors[1].bufferdesc.state) & BM_IO_IN_PROGRESS));
		if (at == 4)
			UT_ASSERT_EQ(io_aborts, 1);
		cluster_space_recovery_destroy_v1((ClusterSpaceRecoveryBatchV1 **)&batch);
	}
}

UT_TEST(test_native_flush_wait_and_stale_source_unwind)
{
	for (int variant = 0; variant < 6; variant++) {
		int failure = variant % 3;
		ClusterSpaceRecoveryBatchV1 *batch = NULL;
		PGAlignedBlock before;
		uint32 before_state;
		volatile bool caught = false;
		reset();
		stale_after_write = false;
		if (variant >= 3) {
			MyAuxProcType = StartupProcess;
			UT_ASSERT(cluster_space_recovery_cold_preflight_v1(side, cold_fence, &batch));
		} else
			UT_ASSERT(cluster_space_recovery_preflight_v1(fabric, sources, 2, &batch));
		if (!batch)
			continue;
		before = pages[1];
		before_state = pg_atomic_read_u32(&descriptors[1].bufferdesc.state);
		if (failure == 0)
			cluster_smart_fusion = sf_blocked = true;
		else if (failure == 1)
			stale_during_io = true;
		else
			stale_after_write = true;
		PG_TRY();
		{
			UT_ASSERT(!cluster_space_recovery_apply_v1(batch));
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT_EQ(caught, failure == 1);
		UT_ASSERT_EQ(writes, failure == 2);
		UT_ASSERT_EQ(syncs, 0);
		UT_ASSERT_EQ(io_aborts, failure == 1);
		UT_ASSERT_EQ(wal_flushes, 0);
		if (failure < 2) {
			UT_ASSERT(memcmp(pages[1].data, before.data, BLCKSZ) == 0);
			UT_ASSERT_EQ(pg_atomic_read_u32(&descriptors[1].bufferdesc.state), before_state);
		} else {
			/* A completed/uncertain write must never roll the HWM back. */
			ClusterSpaceReservation got;
			uint64 token;
			UT_ASSERT(cluster_space_reservation_page_decode(pages[1].data, BLCKSZ, SPACE_FORKNUM, 1,
															&key, &got, &token));
			UT_ASSERT_EQ(got.next_block, 11);
			UT_ASSERT_EQ(token, 19);
		}
		UT_ASSERT(!pins && !locks && !hw_held && error_context_stack == NULL);
		UT_ASSERT(!(pg_atomic_read_u32(&descriptors[1].bufferdesc.state) & BM_IO_IN_PROGRESS));
		cluster_space_recovery_destroy_v1(&batch);
		stale_after_write = false;
	}
}

UT_TEST(test_ordinary_native_flush_keeps_local_wal)
{
	reset();
	pg_atomic_fetch_or_u32(&descriptors[1].bufferdesc.state, BM_DIRTY);
	FlushBufferWithRecovery(&descriptors[1].bufferdesc, &relation, IOOBJECT_RELATION,
							IOCONTEXT_NORMAL, NULL, NULL, NULL, NULL);
	UT_ASSERT_EQ(wal_flushes, 1);
	UT_ASSERT_EQ(writes, 1);
	UT_ASSERT_EQ(io_aborts, 0);
}

UT_TEST(test_cold_violation_blocks_native_write_even_for_already_dirty_page)
{
	for (int prior_dirty = 0; prior_dirty < 2; prior_dirty++) {
		uint32 state;
		volatile bool caught = false;
		reset();
		if (prior_dirty)
			pg_atomic_fetch_or_u32(&descriptors[1].bufferdesc.state,
								   BM_DIRTY | BM_CHECKPOINT_NEEDED);
		/* The cold dirty hook trips the shared latch while content-X is held.
		 * MarkBufferDirty may still publish dirty before the rmgr releases X. */
		cold_write_allowed = false;
		pg_atomic_fetch_or_u32(&descriptors[1].bufferdesc.state, BM_DIRTY | BM_JUST_DIRTIED);
		state = pg_atomic_read_u32(&descriptors[1].bufferdesc.state);
		PG_TRY();
		{
			FlushBufferWithRecovery(&descriptors[1].bufferdesc, &relation, IOOBJECT_RELATION,
									IOCONTEXT_NORMAL, NULL, NULL, NULL, NULL);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(caught);
		UT_ASSERT_EQ(writes, 0);
		UT_ASSERT_EQ(wal_flushes, 0);
		UT_ASSERT_EQ(pg_atomic_read_u32(&descriptors[1].bufferdesc.state), state);
		UT_ASSERT_EQ(io_aborts, 0);
	}
	cold_write_allowed = true;
}

UT_TEST(test_failed_install_preserves_checkpoint_of_original_dirty_page)
{
	for (int was_dirty = 0; was_dirty < 2; was_dirty++) {
		ClusterSpaceRecoveryBatchV1 *batch = NULL;
		PGAlignedBlock before;
		uint32 expected;
		reset();
		if (was_dirty)
			pg_atomic_fetch_or_u32(&descriptors[1].bufferdesc.state, BM_DIRTY);
		before = pages[1];
		expected = pg_atomic_read_u32(&descriptors[1].bufferdesc.state);
		UT_ASSERT(cluster_space_recovery_preflight_v1(fabric, sources, 2, &batch));
		if (!batch)
			continue;
		cluster_smart_fusion = sf_blocked = checkpoint_during_flush = true;
		UT_ASSERT(!cluster_space_recovery_apply_v1(batch));
		checkpoint_during_flush = false;
		if (was_dirty)
			expected |= BM_CHECKPOINT_NEEDED;
		UT_ASSERT_EQ(pg_atomic_read_u32(&descriptors[1].bufferdesc.state), expected);
		UT_ASSERT(memcmp(pages[1].data, before.data, BLCKSZ) == 0);
		UT_ASSERT_EQ(writes, 0);
		UT_ASSERT_EQ(syncs, 0);
		UT_ASSERT(!pins && !locks && !hw_held);
		cluster_space_recovery_destroy_v1(&batch);
	}
}

UT_TEST(test_reject_unrelated_resource_owner)
{
	ClusterSpaceRecoveryBatchV1 *batch = NULL;
	reset();
	UT_ASSERT(cluster_space_recovery_preflight_v1(fabric, sources, 2, &batch));
	if (!batch)
		return;
	CurrentResourceOwner = (void *)7;
	UT_ASSERT(!cluster_space_recovery_apply_v1(batch));
	UT_ASSERT_EQ(writes, 0);
	UT_ASSERT_EQ(lock_calls, 1);
	UT_ASSERT(CurrentResourceOwner == (void *)7);
	CurrentResourceOwner = source_owner;
	UT_ASSERT(cluster_space_recovery_apply_v1(batch));
	cluster_space_recovery_destroy_v1(&batch);
}

UT_TEST(test_physical_checksum_is_strict)
{
	for (int variant = 0; variant < 3; variant++) {
		ClusterSpaceRecoveryBatchV1 *batch = NULL;
		reset();
		ignore_checksum_failure = true;
		if (variant != 1) {
			PGAlignedBlock disk;
			UT_ASSERT_EQ(pread(fileno(file), disk.data, BLCKSZ, 0), BLCKSZ);
			((PageHeader)disk.data)->pd_checksum ^= 1;
			if (variant == 2)
				memset(disk.data, 0, BLCKSZ);
			UT_ASSERT_EQ(pwrite(fileno(file), disk.data, BLCKSZ, 0), BLCKSZ);
			UT_ASSERT(PageIsVerifiedForFork(disk.data, SPACE_FORKNUM, 0, 0));
			UT_ASSERT(!cluster_space_recovery_preflight_v1(fabric, sources, 2, &batch));
			UT_ASSERT_EQ(writes, 0);
		} else {
			UT_ASSERT(cluster_space_recovery_preflight_v1(fabric, sources, 2, &batch));
			corrupt_checksum = true;
			UT_ASSERT(!cluster_space_recovery_apply_v1(batch));
		}
		cluster_space_recovery_destroy_v1(&batch);
		UT_ASSERT(!pins && !locks && !hw_held);
	}
}

static void
survivor_advance(BlockNumber next, uint64 token, bool durable)
{
	ClusterSpaceReservation value = changes[0].before;
	value.next_block = next;
	UT_ASSERT(cluster_space_reservation_page_encode(&value, token, pages[1].data, BLCKSZ));
	PageSetLSNPreserveOrigin(pages[1].data, 987);
	UT_ASSERT(PageSetLSNOrigin(pages[1].data, 0));
	if (durable) {
		UT_ASSERT_EQ(pwrite(fileno(file), PageSetChecksumCopy(pages[1].data, 1), BLCKSZ, BLCKSZ),
					 BLCKSZ);
		UT_ASSERT_EQ(fsync(fileno(file)), 0);
	} else
		pg_atomic_fetch_or_u32(&descriptors[1].bufferdesc.state, BM_DIRTY);
}

UT_TEST(test_interleaved_writers_use_durable_successor_before_suffix)
{
	ClusterSpaceRecoveryBatchV1 *batch = NULL;
	ClusterSpaceReservation got;
	uint64 token;
	reset();
	changes[1].before.next_block = changes[1].first_block = 9;
	changes[1].result.next_block = 13;
	changes[1].before_token = 7;
	UT_ASSERT(cluster_space_reservation_wal_encode(&changes[1], payload[1], sizeof(payload[1])));
	make_plan();
	survivor_advance(9, 7, true);
	UT_ASSERT(cluster_space_recovery_preflight_v1(fabric, sources, 2, &batch));
	if (!batch)
		return;
	UT_ASSERT(cluster_space_recovery_apply_v1(batch));
	UT_ASSERT_EQ(writes, 1);
	UT_ASSERT_EQ(syncs, 2); /* original survivor base, then recovered suffix */
	UT_ASSERT(cluster_space_reservation_page_decode(pages[1].data, BLCKSZ, SPACE_FORKNUM, 1, &key,
													&got, &token));
	UT_ASSERT_EQ(got.next_block, 13);
	UT_ASSERT_EQ(token, 19);
	cluster_space_recovery_destroy_v1(&batch);
}

UT_TEST(test_survivor_extension_after_preflight_is_not_overwritten)
{
	ClusterSpaceRecoveryBatchV1 *batch = NULL;
	PGAlignedBlock before;
	reset();
	UT_ASSERT(cluster_space_recovery_preflight_v1(fabric, sources, 2, &batch));
	if (!batch)
		return;
	survivor_advance(15, 7, true);
	before = pages[1];
	UT_ASSERT(cluster_space_recovery_apply_v1(batch));
	UT_ASSERT_EQ(writes, 0);
	UT_ASSERT_EQ(syncs, 1);
	UT_ASSERT(memcmp(pages[1].data, before.data, BLCKSZ) == 0);
	cluster_space_recovery_destroy_v1(&batch);
}

UT_TEST(test_install_then_survivor_extend_then_retry)
{
	ClusterSpaceRecoveryBatchV1 *batch = NULL;
	PGAlignedBlock before;
	reset();
	UT_ASSERT(cluster_space_recovery_preflight_v1(fabric, sources, 2, &batch));
	if (!batch)
		return;
	UT_ASSERT(cluster_space_recovery_apply_v1(batch));
	cluster_space_recovery_destroy_v1(&batch);
	survivor_advance(15, 7, true);
	before = pages[1];
	UT_ASSERT(cluster_space_recovery_preflight_v1(fabric, sources, 2, &batch));
	if (!batch)
		return;
	UT_ASSERT(cluster_space_recovery_apply_v1(batch));
	UT_ASSERT_EQ(writes, 1); /* only the original install */
	UT_ASSERT_EQ(syncs, 2);
	UT_ASSERT(memcmp(pages[1].data, before.data, BLCKSZ) == 0);
	cluster_space_recovery_destroy_v1(&batch);
}

UT_TEST(test_successor_cache_without_physical_proof_refuses)
{
	for (int variant = 0; variant < 2; variant++) {
		ClusterSpaceRecoveryBatchV1 *batch = NULL;
		PGAlignedBlock before;
		reset();
		if (variant == 1)
			UT_ASSERT(cluster_space_recovery_preflight_v1(fabric, sources, 2, &batch));
		survivor_advance(15, 7, false);
		before = pages[1];
		if (variant == 0)
			UT_ASSERT(!cluster_space_recovery_preflight_v1(fabric, sources, 2, &batch));
		else
			UT_ASSERT(!cluster_space_recovery_apply_v1(batch));
		UT_ASSERT_EQ(writes, 0);
		UT_ASSERT_EQ(syncs, 0);
		UT_ASSERT(memcmp(pages[1].data, before.data, BLCKSZ) == 0);
		cluster_space_recovery_destroy_v1(&batch);
	}
}

UT_TEST(test_cold_complete_fence_installs_interleaved_reservations_and_retries)
{
	ClusterSpaceRecoveryBatchV1 *batch = NULL;
	ClusterSpaceReservation got;
	uint64 token;
	reset();
	MyAuxProcType = StartupProcess;
	UT_ASSERT(cluster_space_recovery_cold_preflight_v1(side, cold_fence, &batch));
	UT_ASSERT_NOT_NULL(batch);
	UT_ASSERT_EQ(writes, 0);
	UT_ASSERT(cluster_space_recovery_apply_v1(batch));
	UT_ASSERT_EQ(writes, 1);
	UT_ASSERT_EQ(lock_calls, 0);  /* cold isolation never opens serving HW */
	UT_ASSERT_EQ(wal_flushes, 0); /* no foreign numeric LSN flush */
	UT_ASSERT_EQ(syncs, 1);
	UT_ASSERT(cluster_space_reservation_page_decode(pages[1].data, BLCKSZ, SPACE_FORKNUM, 1, &key,
													&got, &token));
	UT_ASSERT_EQ(got.next_block, 11);
	UT_ASSERT_EQ(token, 19);
	cluster_space_recovery_destroy_v1(&batch);
	UT_ASSERT(cluster_space_recovery_cold_preflight_v1(side, cold_fence, &batch));
	UT_ASSERT(cluster_space_recovery_apply_v1(batch));
	UT_ASSERT_EQ(((PageHeader)pages[1].data)->pd_block_scn, 19);
	cluster_space_recovery_destroy_v1(&batch);
	UT_ASSERT(!pins && !locks && !hw_held && CurrentResourceOwner == source_owner);
}

UT_TEST(test_cold_owner_and_complete_source_set_required_before_io)
{
	for (int variant = 0; variant < 12; variant++) {
		ClusterSpaceRecoveryBatchV1 *batch = NULL;
		reset();
		MyAuxProcType = StartupProcess;
		switch (variant) {
		case 0:
			MyAuxProcType = BgWriterProcess;
			break;
		case 1:
			recovery = false;
			break;
		case 2:
			cold_current = false;
			break;
		case 3:
			cold_origins = 1;
			break;
		case 4:
			serials[1].mode = CLUSTER_RECOVERY_SERIAL_ONLINE;
			break;
		case 5:
			serials[1].held = false;
			break;
		case 6:
			roots[1].identity.origin_owner_incarnation++;
			break;
		case 7:
			roots[1].validated_tail_lsn_exclusive++;
			break;
		case 8:
			sources[1].retention_pin = (void *)4;
			break;
		case 9:
			source_redo[1] = 150;
			break;
		case 10:
			source_database[1]++;
			break;
		case 11:
			source_unavailable = true;
			break;
		}
		UT_ASSERT(!cluster_space_recovery_cold_preflight_v1(side, cold_fence, &batch));
		UT_ASSERT(batch == NULL && !pins && !locks && !hw_held);
		UT_ASSERT_EQ(writes, 0);
		UT_ASSERT_EQ(reads, 0);
		UT_ASSERT_EQ(lock_calls, 0);
	}
}

UT_TEST(test_cold_late_fence_change_never_writes)
{
	ClusterSpaceRecoveryBatchV1 *batch = NULL;
	PGAlignedBlock before;
	reset();
	MyAuxProcType = StartupProcess;
	before = pages[1];
	UT_ASSERT(cluster_space_recovery_cold_preflight_v1(side, cold_fence, &batch));
	UT_ASSERT_NOT_NULL(batch);
	cold_current = false;
	UT_ASSERT(!cluster_space_recovery_apply_v1(batch));
	UT_ASSERT_EQ(writes, 0);
	UT_ASSERT(memcmp(&before, &pages[1], sizeof(before)) == 0);
	cluster_space_recovery_destroy_v1(&batch);
	UT_ASSERT(!pins && !locks && !hw_held && CurrentResourceOwner == source_owner);
}

UT_TEST(test_cold_history_does_not_replay_and_structural_owner_is_required)
{
	for (int history = 0; history < 2; history++) {
		ClusterSpaceRecoveryBatchV1 *batch = NULL;
		PGAlignedBlock before[2];
		reset();
		MyAuxProcType = StartupProcess;
		structural = true;
		if (history) {
			native_redo[0] = native_redo[1] = 200;
			source_redo[0] = source_redo[1] = 200;
		}
		make_plan();
		memcpy(before, pages, sizeof(before));
		UT_ASSERT_EQ(cluster_space_recovery_cold_preflight_v1(side, cold_fence, &batch), history);
		if (history) {
			UT_ASSERT_NOT_NULL(batch);
			UT_ASSERT(cluster_space_recovery_apply_v1(batch));
		} else
			UT_ASSERT_NULL(batch);
		UT_ASSERT_EQ(writes, 0);
		UT_ASSERT_EQ(reads, 0);
		UT_ASSERT_EQ(syncs, 0);
		UT_ASSERT_EQ(lock_calls, 0);
		UT_ASSERT(memcmp(before, pages, sizeof(before)) == 0);
		cluster_space_recovery_destroy_v1(&batch);
		UT_ASSERT(!pins && !locks && !hw_held && CurrentResourceOwner == source_owner);
	}
}

static void
create_input(int shape)
{
	reset();
	creating = true;
	changes[0].before.next_block = changes[0].first_block = 0;
	changes[0].result.next_block = 4;
	changes[1] = changes[0];
	UT_ASSERT(cluster_space_reservation_wal_encode(&changes[1], payload[1], sizeof(payload[1])));
	make_plan();
	exists = shape != 0;
	existing_blocks = shape < 2 ? 0 : (shape == 2 ? 1 : 2);
	memset(pages, 0, sizeof(pages));
	if (shape == 2 || shape == 4 || shape == 6)
		UT_ASSERT(cluster_space_identity_page_encode(&changes[0].before.identity, 100,
													 pages[0].data, BLCKSZ));
	if (shape == 5 || shape == 6)
		UT_ASSERT(cluster_space_reservation_page_encode(
			shape == 6 ? &changes[0].result : &changes[0].before, shape == 6 ? 80 : 100,
			pages[1].data, BLCKSZ));
	UT_ASSERT_EQ(ftruncate(fileno(file), 0), 0);
	for (BlockNumber i = 0; i < existing_blocks; i++)
		UT_ASSERT_EQ(
			pwrite(fileno(file),
				   PageIsNew(pages[i].data) ? pages[i].data : PageSetChecksumCopy(pages[i].data, i),
				   BLCKSZ, (off_t)i * BLCKSZ),
			BLCKSZ);
}

static bool
create_preflight(bool cold, ClusterSpaceRecoveryBatchV1 **batch)
{
	if (cold) {
		MyAuxProcType = StartupProcess;
		return cluster_space_recovery_cold_preflight_v1(side, cold_fence, batch);
	}
	return cluster_space_recovery_preflight_v1(fabric, sources, 2, batch);
}

UT_TEST(test_create_missing_partial_and_complete_space_then_cross_source_advance)
{
	for (int cold = 0; cold < 2; cold++)
		for (int shape = 0; shape < 7; shape++) {
			ClusterSpaceRecoveryBatchV1 *batch = NULL;
			ClusterSpaceIdentity identity;
			ClusterSpaceReservation reservation;
			PGAlignedBlock before[2];
			BlockNumber before_blocks;
			uint64 token;
			int origin;
			create_input(shape);
			memcpy(before, pages, sizeof(before));
			before_blocks = existing_blocks;
			UT_ASSERT(create_preflight(cold, &batch));
			UT_ASSERT_NOT_NULL(batch);
			UT_ASSERT_EQ(creates, 0);
			UT_ASSERT_EQ(extensions, 0);
			UT_ASSERT_EQ(writes, 0);
			UT_ASSERT(memcmp(before, pages, sizeof(before)) == 0);
			if (!batch)
				continue;
			UT_ASSERT(cluster_space_recovery_apply_v1(batch));
			UT_ASSERT_EQ(creates, shape == 0);
			UT_ASSERT_EQ(extensions, 2 - before_blocks);
			UT_ASSERT_EQ(writes, 2);
			UT_ASSERT_EQ(syncs, 1);
			UT_ASSERT_EQ(wal_flushes, 0);
			UT_ASSERT(cluster_space_identity_page_decode(pages[0].data, BLCKSZ, SPACE_FORKNUM, 0,
														 &key, &identity, &token));
			UT_ASSERT_EQ(token, 100);
			UT_ASSERT(PageGetLSNOrigin(pages[0].data, &origin));
			UT_ASSERT_EQ(origin, duties[0].origin_thread_id - 1);
			UT_ASSERT_EQ(PageGetLSN(pages[0].data), 200);
			UT_ASSERT(cluster_space_reservation_page_decode(pages[1].data, BLCKSZ, SPACE_FORKNUM, 1,
															&key, &reservation, &token));
			UT_ASSERT_EQ(token, 80);
			UT_ASSERT_EQ(reservation.next_block, 4);
			UT_ASSERT(PageGetLSNOrigin(pages[1].data, &origin));
			UT_ASSERT_EQ(origin, duties[1].origin_thread_id - 1);
			cluster_space_recovery_destroy_v1(&batch);
			UT_ASSERT(create_preflight(cold, &batch));
			UT_ASSERT(cluster_space_recovery_apply_v1(batch));
			UT_ASSERT_EQ(creates, shape == 0);
			UT_ASSERT_EQ(extensions, 2 - before_blocks);
			cluster_space_recovery_destroy_v1(&batch);
			UT_ASSERT(!pins && !locks && !x_locks && !hw_held);
			UT_ASSERT(CurrentResourceOwner == source_owner);
		}
}

UT_TEST(test_create_flush_failure_keeps_restartable_components)
{
	for (int fail = -1; fail < 2; fail++) {
		ClusterSpaceRecoveryBatchV1 *batch = NULL;
		PGAlignedBlock before[2];
		volatile bool caught = false;
		create_input(0);
		memcpy(before, pages, sizeof(before));
		UT_ASSERT(create_preflight(true, &batch));
		if (!batch)
			continue;
		fail_write_block = fail;
		if (fail < 0)
			cluster_smart_fusion = sf_blocked = true;
		PG_TRY();
		{
			UT_ASSERT(!cluster_space_recovery_apply_v1(batch));
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT_EQ(caught, fail >= 0);
		UT_ASSERT_EQ(writes, fail == 1);
		UT_ASSERT_EQ(syncs, 0);
		UT_ASSERT_EQ(wal_flushes, 0);
		if (fail < 0)
			UT_ASSERT(memcmp(before, pages, sizeof(before)) == 0);
		else {
			UT_ASSERT_EQ(((PageHeader)pages[0].data)->pd_block_scn, 100);
			UT_ASSERT_EQ(((PageHeader)pages[1].data)->pd_block_scn, 80);
		}
		UT_ASSERT(!pins && !locks && !x_locks && !hw_held && error_context_stack == NULL);
		UT_ASSERT(CurrentResourceOwner == source_owner);
		cluster_space_recovery_destroy_v1(&batch);
		fail_write_block = -1;
		cluster_smart_fusion = sf_blocked = false;
		UT_ASSERT(create_preflight(true, &batch));
		UT_ASSERT(cluster_space_recovery_apply_v1(batch));
		cluster_space_recovery_destroy_v1(&batch);
	}
}

UT_TEST(test_create_never_replaces_another_incarnation_or_uses_stale_fence)
{
	for (int variant = 0; variant < 2; variant++) {
		ClusterSpaceRecoveryBatchV1 *batch = NULL;
		PGAlignedBlock before[2];
		create_input(variant == 0 ? 4 : 0);
		if (variant == 0) {
			ClusterSpaceIdentity wrong = changes[0].before.identity;
			memset(wrong.incarnation, 0x45, sizeof(wrong.incarnation));
			UT_ASSERT(cluster_space_identity_page_encode(&wrong, 100, pages[0].data, BLCKSZ));
		}
		memcpy(before, pages, sizeof(before));
		if (variant == 0)
			UT_ASSERT(!create_preflight(true, &batch));
		else {
			UT_ASSERT(create_preflight(true, &batch));
			cold_current = false;
			UT_ASSERT(!cluster_space_recovery_apply_v1(batch));
		}
		UT_ASSERT_EQ(creates, 0);
		UT_ASSERT_EQ(extensions, 0);
		UT_ASSERT_EQ(writes, 0);
		UT_ASSERT(memcmp(before, pages, sizeof(before)) == 0);
		cluster_space_recovery_destroy_v1(&batch);
	}
}

UT_TEST(test_native_create_step_does_not_advance_or_complete_the_batch)
{
	for (int cold = 0; cold < 2; cold++) {
		ClusterSpaceRecoveryBatchV1 *batch = NULL;
		ClusterSpaceReservation reservation;
		uint64 token;
		int origin;

		create_input(0);
		UT_ASSERT(create_preflight(cold, &batch));
		UT_ASSERT(cluster_space_recovery_apply_through_v1(batch, 0, 0));
		UT_ASSERT(cluster_space_reservation_page_decode(pages[1].data, BLCKSZ, SPACE_FORKNUM, 1,
														&key, &reservation, &token));
		UT_ASSERT_EQ(reservation.next_block, 0);
		UT_ASSERT_EQ(token, 100);
		UT_ASSERT_EQ(writes, 2);
		UT_ASSERT_EQ(syncs, 1);
		UT_ASSERT(PageGetLSNOrigin(pages[1].data, &origin));
		UT_ASSERT_EQ(origin, duties[0].origin_thread_id - 1);
		UT_ASSERT_EQ(PageGetLSN(pages[1].data), operations[0].identity.record.end_rec_ptr);
		UT_ASSERT(!cluster_space_recovery_applied_operation_v1(batch, &operations[1]));
		UT_ASSERT(cluster_space_recovery_apply_through_v1(batch, 0, 1));
		UT_ASSERT(cluster_space_reservation_page_decode(pages[1].data, BLCKSZ, SPACE_FORKNUM, 1,
														&key, &reservation, &token));
		UT_ASSERT_EQ(reservation.next_block, 4);
		UT_ASSERT_EQ(token, 80);
		UT_ASSERT(PageGetLSNOrigin(pages[1].data, &origin));
		UT_ASSERT_EQ(origin, duties[1].origin_thread_id - 1);
		UT_ASSERT(!cluster_space_recovery_applied_operation_v1(batch, &operations[1]));
		UT_ASSERT(cluster_space_recovery_apply_v1(batch));
		UT_ASSERT(cluster_space_recovery_applied_operation_v1(batch, &operations[1]));
		cluster_space_recovery_destroy_v1(&batch);
		UT_ASSERT(!pins && !locks && !hw_held && CurrentResourceOwner == source_owner);
	}
}

UT_TEST(test_native_prefix_preserves_later_reservation_but_installs_missing_identity)
{
	ClusterSpaceRecoveryBatchV1 *batch = NULL;
	PGAlignedBlock successor;
	int origin;

	create_input(6);
	memset(pages[0].data, 0, BLCKSZ);
	PageSetLSNPreserveOrigin(pages[1].data, operations[1].identity.record.end_rec_ptr);
	UT_ASSERT(PageSetLSNOrigin(pages[1].data, duties[1].origin_thread_id - 1));
	successor = pages[1];
	UT_ASSERT_EQ(pwrite(fileno(file), pages[0].data, BLCKSZ, 0), BLCKSZ);
	UT_ASSERT_EQ(pwrite(fileno(file), PageSetChecksumCopy(pages[1].data, 1), BLCKSZ, BLCKSZ),
				 BLCKSZ);
	UT_ASSERT(create_preflight(true, &batch));
	UT_ASSERT(cluster_space_recovery_apply_through_v1(batch, 0, 0));
	UT_ASSERT_EQ(writes, 1);
	UT_ASSERT_EQ(syncs, 2);
	UT_ASSERT(memcmp(successor.data, pages[1].data, BLCKSZ) == 0);
	UT_ASSERT_EQ(((PageHeader)pages[0].data)->pd_block_scn, 100);
	UT_ASSERT(PageGetLSNOrigin(pages[0].data, &origin));
	UT_ASSERT_EQ(origin, duties[0].origin_thread_id - 1);
	UT_ASSERT(!cluster_space_recovery_applied_operation_v1(batch, &operations[1]));
	cluster_space_recovery_destroy_v1(&batch);
	UT_ASSERT(!pins && !locks && !hw_held && CurrentResourceOwner == source_owner);
}

UT_TEST(test_native_prefix_invalid_target_or_position_has_no_io)
{
	ClusterSpaceRecoveryBatchV1 *batch = NULL;
	unsigned previous_reads;
	create_input(0);
	UT_ASSERT(create_preflight(true, &batch));
	previous_reads = reads;
	UT_ASSERT(!cluster_space_recovery_apply_through_v1(batch, 1, 0));
	UT_ASSERT(!cluster_space_recovery_apply_through_v1(batch, 0, UINT32_MAX));
	UT_ASSERT(!cluster_space_recovery_apply_through_v1(batch, 0, 2));
	UT_ASSERT_EQ(reads, previous_reads);
	UT_ASSERT_EQ(writes, 0);
	UT_ASSERT_EQ(syncs, 0);
	UT_ASSERT_EQ(creates, 0);
	UT_ASSERT_EQ(extensions, 0);
	cluster_space_recovery_destroy_v1(&batch);
}

UT_TEST(test_native_prefix_partial_write_retries_without_installing_the_suffix)
{
	ClusterSpaceRecoveryBatchV1 *batch = NULL;
	volatile bool caught = false;
	create_input(0);
	UT_ASSERT(create_preflight(true, &batch));
	fail_write_block = 1;
	PG_TRY();
	{
		(void)cluster_space_recovery_apply_through_v1(batch, 0, 0);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(writes, 1);
	UT_ASSERT_EQ(((PageHeader)pages[0].data)->pd_block_scn, 100);
	UT_ASSERT_EQ(((PageHeader)pages[1].data)->pd_block_scn, 100);
	UT_ASSERT(!pins && !locks && !hw_held && CurrentResourceOwner == source_owner);
	fail_write_block = -1;
	UT_ASSERT(cluster_space_recovery_apply_through_v1(batch, 0, 0));
	UT_ASSERT_EQ(((PageHeader)pages[1].data)->pd_block_scn, 100);
	UT_ASSERT(!cluster_space_recovery_applied_operation_v1(batch, &operations[1]));
	UT_ASSERT(cluster_space_recovery_apply_v1(batch));
	UT_ASSERT_EQ(((PageHeader)pages[1].data)->pd_block_scn, 80);
	cluster_space_recovery_destroy_v1(&batch);
}

UT_TEST(test_successor_sync_still_rechecks_unchanged_identity)
{
	ClusterSpaceRecoveryBatchV1 *batch = NULL;
	reset();
	survivor_advance(15, 9, true);
	UT_ASSERT(create_preflight(true, &batch));
	corrupt_after_sync_block = 0;
	UT_ASSERT(!cluster_space_recovery_apply_through_v1(batch, 0, 0));
	UT_ASSERT_EQ(writes, 0);
	UT_ASSERT_EQ(syncs, 1);
	UT_ASSERT(!pins && !locks && !hw_held && CurrentResourceOwner == source_owner);
	cluster_space_recovery_destroy_v1(&batch);
}

static void
compact_inputs(ClusterSpaceRecoveryInput inputs[2], ClusterSpaceColdSourceV1 origin[2])
{
	for (uint32 i = 0; i < 2; i++) {
		RfSideOnlineOperationV1 operation;
		UT_ASSERT(rf_side_online_plan_operation_v1(side, i, &operation));
		inputs[i] = (ClusterSpaceRecoveryInput){ operation.owned_payload,
												 operation.owned_payload_length };
		origin[i] = (ClusterSpaceColdSourceV1){ duties[i].origin_thread_id,
												operation.identity.record.end_rec_ptr };
	}
}

UT_TEST(test_compact_cold_create_and_advance_use_original_sources)
{
	for (int founder = 0; founder < 2; founder++) {
		ClusterSpaceRecoveryInput inputs[2];
		ClusterSpaceColdSourceV1 origin[2];
		ClusterSpaceReservation observed = { 0 };
		uint64 token;
		int page_origin = -1;
		bool installed;

		create_input(0);
		MyAuxProcType = StartupProcess;
		/* This owner fixture explicitly includes the founder. A bare node
		 * number or restart ref may not fabricate it in production. */
		if (founder) {
			duties[0].origin_thread_id = 1;
			roots[0].identity = serials[0].duty = duties[0];
		}
		compact_inputs(inputs, origin);
		installed = cluster_space_cold_install_v1(&key, inputs, origin, 2, 0, 0);
		UT_ASSERT(installed);
		if (!installed)
			continue;
		UT_ASSERT_EQ(creates, 1);
		UT_ASSERT_EQ(extensions, 2);
		UT_ASSERT(PageGetLSNOrigin(pages[0].data, &page_origin));
		UT_ASSERT_EQ(page_origin, origin[0].origin_thread - 1);
		UT_ASSERT(PageGetLSNOrigin(pages[1].data, &page_origin));
		UT_ASSERT_EQ(page_origin, origin[0].origin_thread - 1);
		UT_ASSERT(cluster_space_reservation_page_decode(pages[1].data, BLCKSZ, SPACE_FORKNUM, 1,
														&key, &observed, &token));
		UT_ASSERT_EQ(observed.next_block, 0);
		UT_ASSERT(cluster_space_cold_install_v1(&key, inputs, origin, 2, 1, 0));
		UT_ASSERT(cluster_space_reservation_page_decode(pages[1].data, BLCKSZ, SPACE_FORKNUM, 1,
														&key, &observed, &token));
		UT_ASSERT_EQ(observed.next_block, 4);
		UT_ASSERT(PageGetLSNOrigin(pages[1].data, &page_origin));
		UT_ASSERT_EQ(page_origin, origin[1].origin_thread - 1);
		UT_ASSERT_EQ(lock_calls, 0);
		UT_ASSERT_EQ(wal_flushes, 0);
		UT_ASSERT(!pins && !locks && CurrentResourceOwner == source_owner);
	}
}

UT_TEST(test_compact_cold_rejects_missing_owner_and_source_drift_before_io)
{
	for (int variant = 0; variant < 8; variant++) {
		ClusterSpaceRecoveryInput inputs[2];
		ClusterSpaceColdSourceV1 origin[2];
		PGAlignedBlock saved[2];

		create_input(0);
		MyAuxProcType = StartupProcess;
		compact_inputs(inputs, origin);
		memcpy(saved, pages, sizeof(saved));
		if (variant == 0)
			origin[0].origin_thread = 1; /* Missing founder. */
		if (variant == 1)
			origin[1].end_rec_ptr = native_redo[1]; /* history only */
		if (variant == 2)
			origin[1].end_rec_ptr++;
		if (variant == 3)
			source_database[1]++;
		if (variant == 4)
			source_unavailable = true;
		if (variant == 5)
			cold_window = false;
		if (variant == 6)
			merge_claim = false;
		if (variant == 7)
			serials[1].root_read_token.record_crc32c++;
		UT_ASSERT(!cluster_space_cold_install_v1(&key, inputs, origin, 2, 0, 0));
		UT_ASSERT_EQ(writes + creates + extensions + syncs + reads, 0);
		UT_ASSERT(memcmp(saved, pages, sizeof(saved)) == 0);
		UT_ASSERT(!pins && !locks && CurrentResourceOwner == source_owner);
	}
}

UT_TEST(test_compact_cold_preflights_suffix_and_preserves_canonical_order)
{
	ClusterSpaceRecoveryInput inputs[2];
	ClusterSpaceColdSourceV1 origin[2];
	ClusterSpaceRecoveryInput swap;

	create_input(0);
	MyAuxProcType = StartupProcess;
	compact_inputs(inputs, origin);
	swap = inputs[0];
	inputs[0] = inputs[1];
	inputs[1] = swap;
	UT_ASSERT(!cluster_space_cold_install_v1(&key, inputs, origin, 2, 0, 0));
	UT_ASSERT_EQ(writes + creates + extensions + syncs, 0);
	compact_inputs(inputs, origin);
	inputs[1].length--;
	UT_ASSERT(!cluster_space_cold_install_v1(&key, inputs, origin, 2, 0, 0));
	UT_ASSERT_EQ(writes + creates + extensions + syncs, 0);
}

UT_TEST(test_compact_cold_history_successor_requires_physical_proof)
{
	for (int durable = 0; durable < 2; durable++) {
		ClusterSpaceRecoveryInput inputs[2];
		ClusterSpaceColdSourceV1 origin[2];
		PGAlignedBlock before;

		create_input(4);
		MyAuxProcType = StartupProcess;
		changes[1].before.next_block = changes[1].first_block = 9;
		changes[1].result.next_block = 13;
		changes[1].before_token = 7;
		UT_ASSERT(
			cluster_space_reservation_wal_encode(&changes[1], payload[1], sizeof(payload[1])));
		make_plan();
		survivor_advance(9, 7, durable);
		before = pages[1];
		compact_inputs(inputs, origin);
		UT_ASSERT_EQ(cluster_space_cold_install_v1(&key, inputs, origin, 2, 0, 0), durable);
		UT_ASSERT(memcmp(before.data, pages[1].data, BLCKSZ) == 0);
		if (!durable)
			UT_ASSERT_EQ(writes + syncs + creates + extensions, 0);
		else
			UT_ASSERT(syncs >= 2);
		UT_ASSERT(!pins && !locks && CurrentResourceOwner == source_owner);
	}
}

UT_TEST(test_compact_cold_failed_io_keeps_original_owner_and_retryable_prefix)
{
	for (int variant = 0; variant < 3; variant++) {
		ClusterSpaceRecoveryInput inputs[2];
		ClusterSpaceColdSourceV1 origin[2];
		volatile bool caught = false;

		create_input(0);
		MyAuxProcType = StartupProcess;
		compact_inputs(inputs, origin);
		if (variant == 0)
			fail_write_block = 1;
		if (variant == 1)
			stale_after_write = true;
		if (variant == 2)
			throw_at = 3;
		PG_TRY();
		{
			UT_ASSERT(!cluster_space_cold_install_v1(&key, inputs, origin, 2, 0, 0));
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		/* Native read/write errors throw; a stale source refuses without ERROR. */
		UT_ASSERT_EQ(caught, variant != 1);
		UT_ASSERT(!pins && !locks && CurrentResourceOwner == source_owner);
		UT_ASSERT(serials[0].held && serials[1].held);
		fail_write_block = -1;
		stale_after_write = stale = false;
		throw_at = 0;
		UT_ASSERT(cluster_space_cold_install_v1(&key, inputs, origin, 2, 0, 0));
		UT_ASSERT_EQ(((PageHeader)pages[1].data)->pd_block_scn, 100);
		UT_ASSERT_EQ(wal_flushes, 0);
	}
}

UT_TEST(test_compact_cold_structure_requires_exact_two_component_chain)
{
	ClusterSpaceRecoveryInput inputs[2];
	ClusterSpaceColdSourceV1 origin[2];
	PGAlignedBlock before[2];

	reset();
	MyAuxProcType = StartupProcess;
	structural = true;
	make_plan();
	compact_inputs(inputs, origin);
	inputs[1].length--;
	memcpy(before, pages, sizeof(before));
	UT_ASSERT(!cluster_space_cold_install_v1(&key, inputs, origin, 2, 0, 0));
	UT_ASSERT_EQ(writes + creates + extensions + syncs + reads, 0);
	UT_ASSERT(memcmp(before, pages, sizeof(before)) == 0);
}

UT_TEST(test_compact_cold_truncate_runs_original_owner_at_its_step_only)
{
	ClusterSpaceRecoveryInput inputs[2];
	ClusterSpaceColdSourceV1 origin[2];
	ClusterSpaceReservation got;
	uint64 token;

	reset();
	structural = true;
	make_plan();
	MyAuxProcType = StartupProcess;
	compact_inputs(inputs, origin);
	UT_ASSERT(cluster_space_cold_install_v1(&key, inputs, origin, 2, 0, 0));
	UT_ASSERT_EQ(cold_truncates, 0);
	UT_ASSERT_EQ(cold_main_blocks, 7);
	UT_ASSERT(cluster_space_cold_install_v1(&key, inputs, origin, 2, 1, SMGR_TRUNCATE_ALL));
	UT_ASSERT_EQ(cold_truncates, 1);
	UT_ASSERT_EQ(cold_main_blocks, 5);
	UT_ASSERT(cluster_space_reservation_page_decode(pages[1].data, BLCKSZ, SPACE_FORKNUM, 1, &key,
													&got, &token));
	UT_ASSERT_EQ(got.next_block, 5);
	UT_ASSERT_EQ(token, 19);
	UT_ASSERT(cluster_space_cold_install_v1(&key, inputs, origin, 2, 1, SMGR_TRUNCATE_ALL));
	UT_ASSERT_EQ(cold_truncates, 1);
	UT_ASSERT_EQ(wal_flushes, 0);
	UT_ASSERT(!pins && !locks && CurrentResourceOwner == source_owner);
}

UT_TEST(test_compact_cold_truncate_failure_cannot_publish_new_identity)
{
	for (int late = 0; late < 2; late++) {
		ClusterSpaceRecoveryInput inputs[2];
		ClusterSpaceColdSourceV1 origin[2];
		PGAlignedBlock before[2];

		reset();
		structural = true;
		make_plan();
		MyAuxProcType = StartupProcess;
		compact_inputs(inputs, origin);
		memcpy(before, pages, sizeof(before));
		fail_cold_truncate = !late;
		stale_after_truncate = late;
		UT_ASSERT(!cluster_space_cold_install_v1(&key, inputs, origin, 2, 1, SMGR_TRUNCATE_ALL));
		UT_ASSERT_EQ(cold_truncates, late);
		UT_ASSERT_EQ(writes, 0);
		UT_ASSERT(memcmp(before, pages, sizeof(before)) == 0);
		UT_ASSERT(!pins && !locks && CurrentResourceOwner == source_owner);
		stale = stale_after_truncate = fail_cold_truncate = false;
		UT_ASSERT(cluster_space_cold_install_v1(&key, inputs, origin, 2, 1, SMGR_TRUNCATE_ALL));
		UT_ASSERT_EQ(cold_main_blocks, 5);
	}
}

static void
compact_lifecycle(ClusterSpaceRecoveryInput inputs[4], ClusterSpaceColdSourceV1 origin[4],
				  uint8 bytes[4][CLUSTER_SPACE_STRUCTURE_WAL_BYTES])
{
	ClusterSpaceStructureChange truncate, drop;
	ClusterSpaceReservationChange advance;

	reset();
	structural = true;
	make_plan();
	MyAuxProcType = StartupProcess;
	compact_inputs(inputs, origin);
	UT_ASSERT(cluster_space_structure_wal_decode(inputs[1].data, inputs[1].length, &truncate));
	memcpy(bytes[0], inputs[0].data, inputs[0].length);
	memcpy(bytes[1], inputs[1].data, inputs[1].length);
	advance = (ClusterSpaceReservationChange){ 0 };
	advance.action = CLUSTER_SPACE_RESERVATION_ADVANCE;
	advance.before = truncate.reservation.result;
	advance.result = advance.before;
	advance.first_block = 5;
	advance.granted = 4;
	advance.result.next_block = 9;
	advance.before_token = 19;
	advance.result_token = 77;
	UT_ASSERT(cluster_space_reservation_wal_encode(&advance, bytes[2],
												   CLUSTER_SPACE_RESERVATION_WAL_BYTES));
	drop = (ClusterSpaceStructureChange){ 0 };
	drop.identity.action = CLUSTER_SPACE_WAL_TOMBSTONE;
	drop.identity.nblocks = InvalidBlockNumber;
	drop.identity.expected = truncate.identity.result;
	drop.identity.result = drop.identity.expected;
	drop.identity.result.state = CLUSTER_SPACE_IDENTITY_TOMBSTONED;
	drop.identity.result.sequence++;
	drop.identity.result.operation = 88;
	drop.identity.before_token = 19;
	drop.identity.result_token = 88;
	drop.reservation.action = CLUSTER_SPACE_RESERVATION_TOMBSTONE;
	drop.reservation.before = advance.result;
	drop.reservation.result = drop.reservation.before;
	drop.reservation.result.identity = drop.identity.result;
	drop.reservation.before_token = 77;
	drop.reservation.result_token = 88;
	UT_ASSERT(cluster_space_structure_wal_encode(&drop, bytes[3], sizeof(bytes[3])));
	for (int i = 0; i < 4; i++) {
		inputs[i]
			= (ClusterSpaceRecoveryInput){ bytes[i], i % 2 ? CLUSTER_SPACE_STRUCTURE_WAL_BYTES
														   : CLUSTER_SPACE_RESERVATION_WAL_BYTES };
		origin[i] = (ClusterSpaceColdSourceV1){ 2 + i % 2, 120 + 20 * i };
	}
}

UT_TEST(test_compact_cold_regrowth_and_commit_tombstone_preserve_later_data)
{
	ClusterSpaceRecoveryInput inputs[4];
	ClusterSpaceColdSourceV1 origin[4];
	uint8 bytes[4][CLUSTER_SPACE_STRUCTURE_WAL_BYTES];
	PGAlignedBlock before[2];
	ClusterSpaceIdentity identity;
	uint64 token;

	compact_lifecycle(inputs, origin, bytes);
	for (uint32 through = 0; through < 4; through++) {
		UT_ASSERT(cluster_space_cold_install_v1(&key, inputs, origin, 4, through,
												through == 1 ? SMGR_TRUNCATE_ALL : 0));
		if (through == 2)
			cold_main_blocks = 9; /* Later native PAGE installs after their reservation. */
	}
	UT_ASSERT_EQ(cold_truncates, 1);
	UT_ASSERT_EQ(cold_main_blocks, 9); /* Physical deletion belongs to COMMIT. */
	UT_ASSERT(cluster_space_identity_page_decode(pages[0].data, BLCKSZ, SPACE_FORKNUM, 0, &key,
												 &identity, &token));
	UT_ASSERT_EQ(identity.state, CLUSTER_SPACE_IDENTITY_TOMBSTONED);
	UT_ASSERT_EQ(token, 88);
	memcpy(before, pages, sizeof(before));
	UT_ASSERT(cluster_space_cold_install_v1(&key, inputs, origin, 4, 1, 0));
	UT_ASSERT_EQ(cold_truncates, 1);
	UT_ASSERT_EQ(cold_main_blocks, 9);
	UT_ASSERT(memcmp(before, pages, sizeof(before)) == 0);
	UT_ASSERT_EQ(wal_flushes, 0);
}

UT_TEST(test_compact_cold_final_install_never_repeats_intermediate_shrink)
{
	ClusterSpaceRecoveryInput inputs[4];
	ClusterSpaceColdSourceV1 origin[4];
	uint8 bytes[4][CLUSTER_SPACE_STRUCTURE_WAL_BYTES];

	compact_lifecycle(inputs, origin, bytes);
	UT_ASSERT(cluster_space_cold_install_v1(&key, inputs, origin, 4, 3, 0));
	UT_ASSERT_EQ(cold_truncates, 0);
	UT_ASSERT_EQ(cold_main_blocks, 7);
	UT_ASSERT_EQ(((PageHeader)pages[0].data)->pd_block_scn, 88);
	UT_ASSERT_EQ(((PageHeader)pages[1].data)->pd_block_scn, 88);
}

UT_TEST(test_compact_cold_named_shrink_runs_even_when_space_is_already_durable)
{
	ClusterSpaceRecoveryInput inputs[2];
	ClusterSpaceColdSourceV1 origins[2];

	reset();
	structural = true;
	make_plan();
	MyAuxProcType = StartupProcess;
	compact_inputs(inputs, origins);
	UT_ASSERT(cluster_space_cold_install_v1(&key, inputs, origins, 2, 1, SMGR_TRUNCATE_ALL));
	cold_main_blocks = 9; /* The SPACE result alone is not a fork durability proof. */
	UT_ASSERT(cluster_space_cold_install_v1(&key, inputs, origins, 2, 1, SMGR_TRUNCATE_HEAP));
	UT_ASSERT_EQ(cold_main_blocks, 5);
	UT_ASSERT_EQ(cold_truncates, 2);
}

UT_TEST(test_compact_cold_unrequested_shrink_does_not_inspect_or_modify_data)
{
	ClusterSpaceRecoveryInput inputs[2];
	ClusterSpaceColdSourceV1 origins[2];

	reset();
	structural = true;
	make_plan();
	MyAuxProcType = StartupProcess;
	compact_inputs(inputs, origins);
	fail_cold_truncate = true;
	UT_ASSERT(cluster_space_cold_install_v1(&key, inputs, origins, 2, 1, 0));
	UT_ASSERT_EQ(cold_main_blocks, 7);
	UT_ASSERT_EQ(cold_truncates, 0);
	UT_ASSERT_EQ(cold_last_truncate_flags, 0);
	UT_ASSERT_EQ(((PageHeader)pages[0].data)->pd_block_scn, 19);
}

UT_TEST(test_compact_cold_invalid_shrink_request_refuses_before_first_mutation)
{
	for (int variant = 0; variant < 2; variant++) {
		ClusterSpaceRecoveryInput inputs[2];
		ClusterSpaceColdSourceV1 origins[2];

		reset();
		structural = true;
		make_plan();
		MyAuxProcType = StartupProcess;
		compact_inputs(inputs, origins);
		UT_ASSERT(
			!cluster_space_cold_install_v1(&key, inputs, origins, 2, variant,
										   variant ? (1 << SPACE_FORKNUM) : SMGR_TRUNCATE_HEAP));
		UT_ASSERT_EQ(writes + creates + extensions + syncs + cold_truncates, 0);
		UT_ASSERT_EQ(cold_main_blocks, 7);
	}
}

UT_TEST(test_compact_cold_vm_shrink_does_not_shrink_main)
{
	ClusterSpaceRecoveryInput inputs[2];
	ClusterSpaceColdSourceV1 origins[2];

	reset();
	structural = true;
	make_plan();
	MyAuxProcType = StartupProcess;
	compact_inputs(inputs, origins);
	UT_ASSERT(
		cluster_space_cold_install_v1(&key, inputs, origins, 2, 1, 1 << VISIBILITYMAP_FORKNUM));
	UT_ASSERT_EQ(cold_main_blocks, 7);
	UT_ASSERT_EQ(cold_truncates, 0);
	UT_ASSERT_EQ(cold_last_truncate_flags, SMGR_TRUNCATE_VM);
	UT_ASSERT_EQ(((PageHeader)pages[0].data)->pd_block_scn, 19);
}

UT_TEST(test_compact_cold_truncate_qualifies_history_advance_gap)
{
	for (int durable = 0; durable < 2; durable++) {
		ClusterSpaceRecoveryInput inputs[4];
		ClusterSpaceColdSourceV1 origin[4];
		uint8 bytes[4][CLUSTER_SPACE_STRUCTURE_WAL_BYTES];
		ClusterSpaceStructureChange truncate;

		compact_lifecycle(inputs, origin, bytes);
		UT_ASSERT(cluster_space_structure_wal_decode(bytes[1], sizeof(bytes[1]), &truncate));
		truncate.reservation.before.next_block = 9;
		truncate.reservation.before_token = 7;
		UT_ASSERT(cluster_space_structure_wal_encode(&truncate, bytes[1], sizeof(bytes[1])));
		survivor_advance(9, 7, durable);
		cold_main_blocks = 9;
		UT_ASSERT_EQ(cluster_space_cold_install_v1(&key, inputs, origin, 4, 1, SMGR_TRUNCATE_ALL),
					 durable);
		UT_ASSERT_EQ(cold_truncates, durable);
		UT_ASSERT_EQ(cold_main_blocks, durable ? 5 : 9);
		if (!durable)
			UT_ASSERT_EQ(writes + creates + extensions + syncs, 0);
		UT_ASSERT(!pins && !locks && CurrentResourceOwner == source_owner);
	}
}

UT_TEST(test_compact_cold_partial_space_write_retries_without_losing_regrowth)
{
	for (int block = 0; block < 2; block++) {
		ClusterSpaceRecoveryInput inputs[4];
		ClusterSpaceColdSourceV1 origin[4];
		uint8 bytes[4][CLUSTER_SPACE_STRUCTURE_WAL_BYTES];
		volatile bool caught = false;

		compact_lifecycle(inputs, origin, bytes);
		fail_write_block = block;
		PG_TRY();
		{
			(void)cluster_space_cold_install_v1(&key, inputs, origin, 4, 1, SMGR_TRUNCATE_ALL);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(caught);
		UT_ASSERT_EQ(cold_truncates, 1);
		UT_ASSERT(!pins && !locks && CurrentResourceOwner == source_owner);
		fail_write_block = -1;
		UT_ASSERT(cluster_space_cold_install_v1(&key, inputs, origin, 4, 1, SMGR_TRUNCATE_ALL));
		UT_ASSERT(cluster_space_cold_install_v1(&key, inputs, origin, 4, 2, 0));
		cold_main_blocks = 9;
		UT_ASSERT(cluster_space_cold_install_v1(&key, inputs, origin, 4, 1, 0));
		UT_ASSERT_EQ(cold_main_blocks, 9);
	}
}

UT_TEST(test_cold_commit_already_qualifies_both_durable_components_without_mutation)
{
	ClusterSpaceRecoveryInput inputs[4];
	ClusterSpaceColdSourceV1 origins[4];
	uint8 bytes[4][CLUSTER_SPACE_STRUCTURE_WAL_BYTES];
	PGAlignedBlock before[2];
	unsigned prior_writes, prior_syncs;

	compact_lifecycle(inputs, origins, bytes);
	UT_ASSERT(cluster_space_cold_install_v1(&key, inputs, origins, 4, 3, 0));
	memcpy(before, pages, sizeof(before));
	prior_writes = writes;
	prior_syncs = syncs;
	UT_ASSERT(cluster_space_recovery_cold_drop_already_v1(&key, &inputs[3], &origins[3]));
	UT_ASSERT_EQ(writes, prior_writes);
	UT_ASSERT_EQ(syncs, prior_syncs + 1);
	UT_ASSERT_EQ(wal_flushes, 0);
	UT_ASSERT(memcmp(before, pages, sizeof(before)) == 0);
	UT_ASSERT(!pins && !locks && CurrentResourceOwner == source_owner);
}

UT_TEST(test_cold_commit_already_refuses_unwritten_result_wrong_origin_and_stale_owner)
{
	for (int bad = 0; bad < 10; bad++) {
		ClusterSpaceRecoveryInput inputs[4];
		ClusterSpaceColdSourceV1 origins[4];
		uint8 bytes[4][CLUSTER_SPACE_STRUCTURE_WAL_BYTES];
		PGAlignedBlock before[2], old[2];
		unsigned prior_writes;

		compact_lifecycle(inputs, origins, bytes);
		memcpy(old, pages, sizeof(old));
		UT_ASSERT(cluster_space_cold_install_v1(&key, inputs, origins, 4, 3, 0));
		if (bad == 0)
			pages[0] = old[0];
		if (bad == 1)
			pages[1] = old[1];
		if (bad == 2)
			UT_ASSERT_EQ(pwrite(fileno(file), old, sizeof(old), 0), sizeof(old));
		if (bad == 3)
			origins[3].origin_thread = 2;
		if (bad == 4)
			origins[3].end_rec_ptr++;
		if (bad == 5)
			cold_window = false;
		if (bad == 6)
			stale = true;
		if (bad == 7)
			corrupt_after_sync_block = 1;
		if (bad == 8)
			existing_blocks = 1;
		if (bad == 9)
			permitted = false;
		memcpy(before, pages, sizeof(before));
		prior_writes = writes;
		UT_ASSERT(!cluster_space_recovery_cold_drop_already_v1(&key, &inputs[3], &origins[3]));
		UT_ASSERT_EQ(writes, prior_writes);
		UT_ASSERT_EQ(wal_flushes, 0);
		UT_ASSERT(memcmp(before, pages, sizeof(before)) == 0);
		UT_ASSERT(!pins && !locks && CurrentResourceOwner == source_owner);
	}
}

UT_TEST(test_cold_commit_io_error_keeps_original_authority_for_retry)
{
	const int faults[] = { 1, 2, 3, 5, 6 };
	for (unsigned i = 0; i < lengthof(faults); i++) {
		ClusterSpaceRecoveryInput inputs[4];
		ClusterSpaceColdSourceV1 origins[4];
		uint8 bytes[4][CLUSTER_SPACE_STRUCTURE_WAL_BYTES];
		PGAlignedBlock before[2];
		unsigned prior_writes;
		volatile bool caught = false;

		compact_lifecycle(inputs, origins, bytes);
		UT_ASSERT(cluster_space_cold_install_v1(&key, inputs, origins, 4, 3, 0));
		memcpy(before, pages, sizeof(before));
		prior_writes = writes;
		throw_at = faults[i];
		PG_TRY();
		{
			(void)cluster_space_recovery_cold_drop_already_v1(&key, &inputs[3], &origins[3]);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(caught);
		UT_ASSERT(!pins && !locks && CurrentResourceOwner == source_owner);
		UT_ASSERT(memcmp(before, pages, sizeof(before)) == 0);
		UT_ASSERT_EQ(writes, prior_writes);
		throw_at = 0;
		UT_ASSERT(cluster_space_recovery_cold_drop_already_v1(&key, &inputs[3], &origins[3]));
	}
}

UT_TEST(test_compact_cold_physical_refusal_precedes_all_prefix_mutations)
{
	ClusterSpaceRecoveryInput inputs[3];
	ClusterSpaceColdSourceV1 origins[3];
	uint8 bytes[CLUSTER_SPACE_STRUCTURE_WAL_BYTES];
	ClusterSpaceStructureChange truncate = { 0 };
	PGAlignedBlock before[2];

	create_input(0);
	MyAuxProcType = StartupProcess;
	structural = true;
	compact_inputs(inputs, origins);
	origins[0].end_rec_ptr = 120;
	origins[1].end_rec_ptr = 140;
	truncate.identity.action = CLUSTER_SPACE_WAL_TRUNCATE;
	truncate.identity.expected = changes[1].result.identity;
	truncate.identity.result = truncate.identity.expected;
	truncate.identity.result.incarnation[0] ^= 1;
	truncate.identity.result.sequence++;
	truncate.identity.result.operation = 19;
	truncate.identity.nblocks = 2;
	truncate.identity.before_token = 100;
	truncate.identity.result_token = 19;
	truncate.reservation.action = CLUSTER_SPACE_RESERVATION_RESET;
	truncate.reservation.before = changes[1].result;
	truncate.reservation.result.identity = truncate.identity.result;
	truncate.reservation.result.next_block = truncate.reservation.first_block = 2;
	truncate.reservation.before_token = 80;
	truncate.reservation.result_token = 19;
	UT_ASSERT(cluster_space_structure_wal_encode(&truncate, bytes, sizeof(bytes)));
	inputs[2] = (ClusterSpaceRecoveryInput){ bytes, sizeof(bytes) };
	origins[2] = (ClusterSpaceColdSourceV1){ 2, 160 };
	memcpy(before, pages, sizeof(before));
	fail_cold_truncate = true;
	UT_ASSERT(!cluster_space_cold_install_v1(&key, inputs, origins, 3, 2, SMGR_TRUNCATE_ALL));
	UT_ASSERT_EQ(writes + creates + extensions + cold_truncates, 0);
	UT_ASSERT(memcmp(before, pages, sizeof(before)) == 0);
	UT_ASSERT(!pins && !locks && CurrentResourceOwner == source_owner);
	fail_cold_truncate = false;
	UT_ASSERT(cluster_space_cold_install_v1(&key, inputs, origins, 3, 2, SMGR_TRUNCATE_ALL));
	fail_cold_truncate = true; /* No requested shrink: do not inspect the regrown VM. */
	UT_ASSERT(cluster_space_cold_install_v1(&key, inputs, origins, 3, 2, 0));
}

int
main(void)
{
	UT_PLAN(47);
	UT_RUN(test_compact_cold_vm_shrink_does_not_shrink_main);
	UT_RUN(test_compact_cold_named_shrink_runs_even_when_space_is_already_durable);
	UT_RUN(test_compact_cold_unrequested_shrink_does_not_inspect_or_modify_data);
	UT_RUN(test_compact_cold_invalid_shrink_request_refuses_before_first_mutation);
	UT_RUN(test_compact_cold_physical_refusal_precedes_all_prefix_mutations);
	UT_RUN(test_cold_commit_io_error_keeps_original_authority_for_retry);
	UT_RUN(test_cold_commit_already_qualifies_both_durable_components_without_mutation);
	UT_RUN(test_cold_commit_already_refuses_unwritten_result_wrong_origin_and_stale_owner);
	UT_RUN(test_compact_cold_truncate_qualifies_history_advance_gap);
	UT_RUN(test_compact_cold_regrowth_and_commit_tombstone_preserve_later_data);
	UT_RUN(test_compact_cold_final_install_never_repeats_intermediate_shrink);
	UT_RUN(test_compact_cold_partial_space_write_retries_without_losing_regrowth);
	UT_RUN(test_compact_cold_truncate_runs_original_owner_at_its_step_only);
	UT_RUN(test_compact_cold_truncate_failure_cannot_publish_new_identity);
	UT_RUN(test_compact_cold_history_successor_requires_physical_proof);
	UT_RUN(test_compact_cold_failed_io_keeps_original_owner_and_retryable_prefix);
	UT_RUN(test_compact_cold_structure_requires_exact_two_component_chain);
	UT_RUN(test_compact_cold_create_and_advance_use_original_sources);
	UT_RUN(test_compact_cold_rejects_missing_owner_and_source_drift_before_io);
	UT_RUN(test_compact_cold_preflights_suffix_and_preserves_canonical_order);
	UT_RUN(test_successor_sync_still_rechecks_unchanged_identity);
	UT_RUN(test_native_create_step_does_not_advance_or_complete_the_batch);
	UT_RUN(test_native_prefix_preserves_later_reservation_but_installs_missing_identity);
	UT_RUN(test_native_prefix_invalid_target_or_position_has_no_io);
	UT_RUN(test_native_prefix_partial_write_retries_without_installing_the_suffix);
	UT_RUN(test_create_missing_partial_and_complete_space_then_cross_source_advance);
	UT_RUN(test_create_flush_failure_keeps_restartable_components);
	UT_RUN(test_create_never_replaces_another_incarnation_or_uses_stale_fence);
	UT_RUN(test_cold_complete_fence_installs_interleaved_reservations_and_retries);
	UT_RUN(test_cold_owner_and_complete_source_set_required_before_io);
	UT_RUN(test_cold_late_fence_change_never_writes);
	UT_RUN(test_cold_history_does_not_replay_and_structural_owner_is_required);
	UT_RUN(test_cold_violation_blocks_native_write_even_for_already_dirty_page);
	UT_RUN(test_failed_install_preserves_checkpoint_of_original_dirty_page);
	UT_RUN(test_interleaved_writers_use_durable_successor_before_suffix);
	UT_RUN(test_survivor_extension_after_preflight_is_not_overwritten);
	UT_RUN(test_install_then_survivor_extend_then_retry);
	UT_RUN(test_successor_cache_without_physical_proof_refuses);
	UT_RUN(test_actual_reservation_install_and_repeat);
	UT_RUN(test_sources_and_structure_refuse_before_mutation);
	UT_RUN(test_target_and_late_authority_refuse);
	UT_RUN(test_disk_failure_never_completes);
	UT_RUN(test_error_releases_original_resources);
	UT_RUN(test_native_flush_wait_and_stale_source_unwind);
	UT_RUN(test_ordinary_native_flush_keeps_local_wal);
	UT_RUN(test_reject_unrelated_resource_owner);
	UT_RUN(test_physical_checksum_is_strict);
	if (file)
		fclose(file);
	rf_side_online_plan_destroy_v1(&side);
	UT_DONE();
	return ut_failed_count != 0;
}
