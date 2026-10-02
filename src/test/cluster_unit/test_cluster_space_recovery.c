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
#include "cluster/cluster_hw.h"
#include "cluster/cluster_pcm_x_bufmgr.h"
#include "cluster/cluster_mode.h"
#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_sf_dep.h"
#include "cluster/cluster_space_recovery.h"
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
static ClusterThreadRecoveryAuthorityV1 sources[2];
static ClusterSpaceIdentityKey key;
static RfContributorStreamCutV1 cuts[2];
static unsigned pins, locks, writes, syncs, reads, lock_calls;
static bool hw_held, permitted, stale, exists, corrupt_disk, structural, wrong_cut;
static bool corrupt_checksum, checksums, ignore_checksum_failure;
static int throw_at;
int cluster_node_id;
bool cluster_smart_fusion, cluster_past_image;
ClusterPcmOwnEntry *ClusterPcmOwnArray;
BufferUsage pgBufferUsage;
static bool sf_blocked, stale_during_io, stale_after_write;
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
static ResourceOwner source_owner = (void *)6;
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
	return CurrentResourceOwner == source_owner && !stale && (a == &sources[0] || a == &sources[1])
			   ? CLUSTER_THREAD_AUTHORITY_OK
			   : CLUSTER_THREAD_AUTHORITY_ROOT_STALE;
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
	return 2;
}
Buffer
ReadBufferWithoutRelcache(RelFileLocator r, ForkNumber f, BlockNumber b, ReadBufferMode m,
						  BufferAccessStrategy s, bool perm)
{
	UT_ASSERT(CurrentResourceOwner == child_owner);
	UT_ASSERT(hw_held && f == SPACE_FORKNUM && b < 2);
	pins |= 1 << b;
	fault(1);
	return b + 1;
}
void
LockBuffer(Buffer b, int mode)
{
	if (mode == BUFFER_LOCK_UNLOCK) {
		locks &= ~(1 << (b - 1));
		InterruptHoldoffCount--;
	} else {
		UT_ASSERT(pins & (1 << (b - 1)));
		locks |= 1 << (b - 1);
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
			   || lock == BufferDescriptorGetContentLock(&descriptors[1].bufferdesc));
}
bool
cluster_bufmgr_pcm_x_content_holder_write_permitted(BufferDesc *b)
{
	UT_ASSERT(locks == 3 && pins == 3 && hw_held);
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
	UT_ASSERT_EQ(b, 2);
	UT_ASSERT(CritSectionCount > 0);
	pg_atomic_fetch_or_u32(&descriptors[1].bufferdesc.state, BM_DIRTY | BM_JUST_DIRTIED);
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
}
void
smgrimmedsync(SMgrRelation r, ForkNumber f)
{
	UT_ASSERT(CurrentResourceOwner == child_owner);
	UT_ASSERT(locks == 3 && hw_held);
	fault(5);
	UT_ASSERT_EQ(fsync(fileno(file)), 0);
	syncs++;
}
void
smgrwrite(SMgrRelation r, ForkNumber f, BlockNumber b, const void *data, bool skip)
{
	UT_ASSERT_EQ(b, 1);
	UT_ASSERT_EQ(f, SPACE_FORKNUM);
	fault(4);
	writes++;
	UT_ASSERT_EQ(pwrite(fileno(file), data, BLCKSZ, BLCKSZ), BLCKSZ);
	if (stale_after_write)
		stale = true;
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
	return true;
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
#include "test_cluster_space_recovery_flush.inc"

static void
make_plan(void)
{
	RfSideOnlinePlanRequestV1 request
		= { .system_identifier = 17, .physical_cuts = cuts, .participant_count = 2 };

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
	sf_blocked = stale_during_io = false;
	InterruptHoldoffCount = 0;
	cluster_smart_fusion = false;
	hw_held = stale = corrupt_disk = structural = wrong_cut = false;
	exists = permitted = true;
	throw_at = 0;
	memset(changes, 0, sizeof(changes));
	memset(operations, 0, sizeof(operations));
	memset(duties, 0, sizeof(duties));
	memset(roots, 0, sizeof(roots));
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
		sources[i].retention_pin = (void *)3;
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
	for (int at = 1; at <= 6; at++) {
		ClusterSpaceRecoveryBatchV1 *volatile batch = NULL;
		volatile bool caught = false;
		reset();
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
	for (int variant = 0; variant < 3; variant++) {
		ClusterSpaceRecoveryBatchV1 *batch = NULL;
		PGAlignedBlock before;
		uint32 before_state;
		volatile bool caught = false;
		reset();
		stale_after_write = false;
		UT_ASSERT(cluster_space_recovery_preflight_v1(fabric, sources, 2, &batch));
		if (!batch)
			continue;
		before = pages[1];
		before_state = pg_atomic_read_u32(&descriptors[1].bufferdesc.state);
		if (variant == 0)
			cluster_smart_fusion = sf_blocked = true;
		else if (variant == 1)
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
		UT_ASSERT_EQ(caught, variant == 1);
		UT_ASSERT_EQ(writes, variant == 2);
		UT_ASSERT_EQ(syncs, 0);
		UT_ASSERT_EQ(io_aborts, variant == 1);
		UT_ASSERT_EQ(wal_flushes, 0);
		if (variant < 2) {
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

int
main(void)
{
	UT_PLAN(15);
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
