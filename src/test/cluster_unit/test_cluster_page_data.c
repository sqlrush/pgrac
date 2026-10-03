/* Native resident DATA owner + full FlushBuffer body; actual file I/O and
 * checksum. Mapping/PCM grants, WAL flush and BufferIO are explicit fixtures.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"
#include <unistd.h>

#include "access/xlog.h"
#include "access/xact.h"
#include "access/xlog_internal.h"
#include "catalog/storage_xlog.h"
#include "catalog/pg_control.h"
#include "catalog/pg_tablespace_d.h"
#include "cluster/cluster_block_apply.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_mode.h"
#include "cluster/cluster_page_data.h"
#include "cluster/cluster_pi_write.h"
#include "cluster/cluster_pi_data.h"
#include "cluster/cluster_pi_writeback.h"
#include "cluster/cluster_page_wal.h"
#include "cluster/cluster_pcm_x_bufmgr.h"
#include "cluster/cluster_space_storage.h"
#include "cluster/cluster_space_recovery.h"
#include "cluster/cluster_wal_thread.h"
#include "cluster/cluster_wal_writer.h"
#include "cluster/cluster_qvotec.h"
#include "cluster/cluster_wal_inputs.h"
#include "cluster/cluster_membership.h"
#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_sf_dep.h"
#include "cluster/storage/cluster_smgr.h"
#include "executor/instrument.h"
#include "miscadmin.h"
#include "pg_trace.h"
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "storage/checksum.h"
#include "storage/checksum_impl.h"
#include "storage/smgr.h"
#include "storage/shmem.h"
#include "utils/resowner.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();
static uint32 wait_event;
uint32 *my_wait_event_info = &wait_event;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
volatile uint32 CritSectionCount, InterruptHoldoffCount;
int cluster_node_id = 0, NBuffers = 2, NLocBuffer;
bool cluster_enabled = true, cluster_shared_config = true, cluster_shared_catalog = true;
bool cluster_smart_fusion, cluster_past_image;
ResourceOwner CurrentResourceOwner = (void *)1;
BackendType MyBackendType = B_BG_WRITER;
MemoryContext TopMemoryContext = (void *)1;
BufferUsage pgBufferUsage;
static BufferDescPadded descriptors[2];
BufferDescPadded *BufferDescriptors = descriptors;
static PGAlignedBlock pages[2];
char *BufferBlocks = (char *)pages;
Block *LocalBufferBlockPointers;
static ClusterPcmOwnEntry own[2];
ClusterPcmOwnEntry *ClusterPcmOwnArray = own;
static LWLock mapping;
static unsigned pins[2];
static int data_slot = 1;
static bool locks[2], resident[2], busy[2], selected, sf_blocked, bad_checksum, bad_bytes;
static bool redeclare_scan_test;
static int redeclare_scope = 1;
static unsigned redeclare_content_attempts;
static ClusterPcmLocalPiSnapshotV1 logical_pi;
static bool logical_pi_raced;
static unsigned logical_pi_retire_calls;

bool
cluster_pcm_local_pi_snapshot_v1(BufferTag tag, ClusterPcmLocalPiSnapshotV1 *out)
{
	*out = logical_pi;
	if (logical_pi.binding_generation == 0)
		out->resource = tag;
	return true;
}

bool
cluster_pcm_local_pi_retire_v1(const ClusterPcmLocalPiSnapshotV1 *local,
							   const ClusterPageDataReceiptV1 *receipt,
							   const RfPageOnlinePlanV1 *plan, const ClusterWalSourceRef *sources,
							   uint32 source_count, const ClusterPiPhysicalAckV1 *ack)
{
	int32 node;
	logical_pi_retire_calls++;
	return !logical_pi_raced && cluster_page_data_pi_ack_read_v1(ack, receipt, &node)
		   && node == cluster_node_id
		   && cluster_page_data_covers_local_pi_v1(receipt, plan, sources, source_count, local);
}

#ifndef PGRAC_TEST_REAL_PI_WRITEBACK
uint64
cluster_epoch_get_current(void)
{
	return 1;
}
#endif

int
cluster_grd_block_redeclare_state_v1(BufferTag tag, uint64 epoch, uint64 *hash)
{
	UT_ASSERT_EQ(epoch, cluster_epoch_get_current());
	*hash = 17;
	return redeclare_scope;
}
static bool stale_sync, redirty_sync, checksums = true;
static bool zero_disk, late_fence;
static bool storage_read, storage_cut_current = true, storage_cut_changed, storage_space_changed;
static bool remote_data_ready;
static ClusterPageWalBindingV1 remote_data_binding;
static ClusterPcmPiWriteCutV1 remote_data_cut;
static ClusterPiDataFactV1 notice_fact;
static bool notice_ready, remote_ack_ready, remote_ack_current;
static ClusterWalWriterToken remote_ack_writer;

#ifndef PGRAC_TEST_REAL_PI_WRITEBACK
bool
cluster_pi_writeback_notice_read_v1(const ClusterPiWritebackNoticeV1 *notice, uint32 index,
									ClusterPiDataFactV1 *out)
{
	if (!notice_ready || notice != (const void *)1 || index != 0)
		return false;
	*out = notice_fact;
	return true;
}

bool
cluster_pi_writeback_ack_read_v1(const ClusterPiWritebackJobV1 *job, uint32 index,
								 const ClusterPageDataReceiptV1 *receipt,
								 ClusterWalWriterToken *out)
{
	if (!remote_ack_ready || job != (const void *)1 || index != 0)
		return false;
	*out = remote_ack_writer;
	return true;
}

bool
cluster_pi_writeback_ack_current_v1(const ClusterPiDataFactV1 *fact,
									const ClusterWalWriterToken *peer)
{
	return remote_ack_current && memcmp(peer, &remote_ack_writer, sizeof(*peer)) == 0;
}
#endif

bool
cluster_pi_data_read_v1(const ClusterPiDataV1 *job, ClusterPageWalBindingV1 *binding,
						ClusterPcmPiWriteCutV1 *cut)
{
	if (!remote_data_ready || job != (const ClusterPiDataV1 *)1)
		return false;
	*binding = remote_data_binding;
	*cut = remote_data_cut;
	return true;
}
static unsigned pi_discards;
static int pi_discard_race;
static int throw_at;
static int concurrent_sync;
static unsigned writes, syncs, reads, wal_flushes, aborts;
static XLogRecPtr local_insert_end;
static FILE *file;
static SMgrRelationData relation;
static ClusterPageDataTargetV1 target;
static ClusterWalSourceRef writer;
static uint64 ack_writer_epoch = 1, ack_boot = 9;
static bool ack_writer_ready = true, ack_epoch_race;
static bool ack_retired_allowed;
static unsigned ack_retired_calls;
static ClusterWalSourceRef ack_retired_source, ack_retired_writer;
#ifndef PGRAC_TEST_REAL_PI_WRITEBACK
/* The complete ROOT reader has separate native-file tests. These predicates
 * are its explicit boundary; DATA/ancestry and acknowledgements below are real. */
static ClusterWalInputV1 recovered_inputs[3];
static bool recovered_ready, recovered_pinned;
static ClusterMembershipState recovered_state;
static uint64 recovered_boot, recovered_membership;
uint64
cluster_membership_cut_generation(void)
{
	return recovered_membership;
}
bool
cluster_membership_cut_generation_current(uint64 expected)
{
	return expected != 0 && (expected & 1) == 0 && expected == recovered_membership;
}
ClusterMembershipState
cluster_membership_get_state(int32 node)
{
	return node == 1 ? recovered_state : CLUSTER_MEMBER_MEMBER;
}
uint64
cluster_membership_get_last_admitted_incarnation(int32 node)
{
	return node == 1 ? recovered_boot : 9;
}
uint32
cluster_wal_inputs_count_v1(ClusterWalInputsV1 *inputs)
{
	return inputs == (void *)1 && recovered_pinned ? 3 : 0;
}
const ClusterWalInputV1 *
cluster_wal_inputs_at_v1(ClusterWalInputsV1 *inputs, uint32 i)
{
	return cluster_wal_inputs_count_v1(inputs) == 3 && i < 3 ? &recovered_inputs[i] : NULL;
}
ClusterControlRootResult
cluster_wal_inputs_revalidate_v1(ClusterWalInputsV1 *inputs)
{
	return inputs == (void *)1 && recovered_pinned && recovered_ready
		? CLUSTER_CONTROL_ROOT_OK_PRIMARY : CLUSTER_CONTROL_ROOT_STALE_TOKEN;
}
bool
cluster_wal_inputs_recovered_owner_v1(ClusterWalInputsV1 *inputs, int32 node,
										ClusterWalSourceRef *out)
{
	if (node != 1 || cluster_wal_inputs_revalidate_v1(inputs) != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return false;
	*out = recovered_inputs[1].source;
	return true;
}
#endif

bool
cluster_wal_inputs_local_predecessor_retired_v1(ClusterWalInputsV1 *inputs,
												const ClusterWalSourceRef *predecessor,
												const ClusterWalSourceRef *current)
{
	ack_retired_calls++;
	return inputs == (void *)1 && ack_retired_allowed
		   && memcmp(predecessor, &ack_retired_source, sizeof(*predecessor)) == 0
		   && memcmp(current, &ack_retired_writer, sizeof(*current)) == 0;
}

static void *page_sources_memory;
static bool source_capture;
static ClusterSpaceIdentity identity;
static bool cluster_pcm_x_finish_retain_flush_active;
static bool cluster_pcm_x_finish_retain_flush_io_active;
static bool cluster_pcm_x_finish_retain_flush_error_context_pushed;
static ErrorContextCallback *cluster_pcm_x_finish_retain_flush_error_context_previous;
#ifdef ENABLE_INJECTION
static bool cluster_pcm_x_finish_retain_flush_fault_active;
#define CLUSTER_INJECTION_POINT(name) ((void)0)
#define cluster_injection_should_skip(name) false
#endif

/* Original native token/boot are the external boundary for this buffer
 * fixture. test_cluster_wal_writer exercises their actual native owner. */
uint64
cluster_qvotec_get_self_incarnation(void)
{
	return ack_boot;
}

ClusterControlRootResult
cluster_wal_writer_begin(TimeLineID timeline, ClusterWalWriterToken *out)
{
	memset(out, 0, sizeof(*out));
	if (!ack_writer_ready || timeline != writer.timeline)
		return CLUSTER_CONTROL_ROOT_RECONFIG_WAIT;
	out->ref = writer;
	out->epoch = ack_writer_epoch;
	out->startup_first_lsn = 0;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

void
ExceptionalCondition(const char *a, const char *b, int c)
{
	fprintf(stderr, "%s %s:%d\n", a, b, c);
	abort();
}
void
pg_re_throw(void)
{
	if (PG_exception_stack)
		siglongjmp(*PG_exception_stack, 1);
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
void *
MemoryContextAllocAligned(MemoryContext context, Size size, Size alignto, int flags)
{
	void *memory = NULL;
	UT_ASSERT(context == TopMemoryContext && flags == 0);
	if (posix_memalign(&memory, alignto, size) != 0)
		abort();
	return memory;
}
bool
RecoveryInProgress(void)
{
	return false;
}
bool cluster_recmerge_window_active;
bool cluster_recmerge_apply_foreign;
uint64 cluster_recmerge_window_scn, cluster_recmerge_window_own_lsn;
ClusterBlkApplyResult
cluster_block_apply_heap(XLogReaderState *record, uint8 block_id, char *page)
{
	/* The contribution integration below exercises actual FPI codecs only. */
	abort();
}
int cluster_pcm_grd_max_entries = 100;
static ClusterConf conf = { .node_count = 2 };
ClusterConf *ClusterConfShmem = &conf;
int
cluster_smgr_which_for(RelFileLocator r, BackendId b)
{
	return 1;
}
bool
cluster_wal_thread_current_v2_ref(ClusterWalSourceRef *out)
{
	if (!selected)
		return false;
	*out = writer;
	return true;
}
bool
cluster_control_root_identity_equal(const ClusterControlRootIdentity *a,
									const ClusterControlRootIdentity *b)
{
	return memcmp(a, b, sizeof(*a)) == 0;
}
uint32
BufTableHashCode(BufferTag *tag)
{
	return tag->forkNum;
}
int
BufTableLookup(BufferTag *tag, uint32 hash)
{
	for (int i = 0; i < 2; i++)
		if (resident[i] && BufferTagsEqual(&descriptors[i].bufferdesc.tag, tag))
			return i;
	return -1;
}
#undef BufMappingPartitionLock
#define BufMappingPartitionLock(hash) (&mapping)
bool
LWLockAcquire(LWLock *lock, LWLockMode mode)
{
	UT_ASSERT(lock == &mapping);
	if (mode == LW_EXCLUSIVE && pi_discard_race != 0) {
		if (pi_discard_race == 1)
			pg_atomic_fetch_add_u64(&own[1].generation, 1);
		else if (pi_discard_race == 2)
			pg_atomic_fetch_add_u32(&descriptors[1].bufferdesc.state, 1);
		else
			((PageHeader)pages[1].data)->pd_block_scn++;
		pi_discard_race = 0;
	}
	HOLD_INTERRUPTS();
	return true;
}
bool
LWLockConditionalAcquire(LWLock *lock, LWLockMode mode)
{
	for (int i = 0; i < 2; i++)
		if (lock == BufferDescriptorGetContentLock(&descriptors[i].bufferdesc)) {
			if (redeclare_scan_test)
				redeclare_content_attempts++;
			if (busy[i])
				return false;
			UT_ASSERT(pins[i] && !locks[i]);
			if (i == 1 && !redeclare_scan_test)
				UT_ASSERT(locks[0]);
			locks[i] = true;
			HOLD_INTERRUPTS();
			return true;
		}
	abort();
}
void
LWLockRelease(LWLock *lock)
{
	/* Match the native release contract even in a release build. */
	UT_ASSERT(InterruptHoldoffCount > 0);
	RESUME_INTERRUPTS();
	if (lock == &mapping)
		return;
	for (int i = 0; i < 2; i++)
		if (lock == BufferDescriptorGetContentLock(&descriptors[i].bufferdesc)) {
			UT_ASSERT(locks[i]);
			locks[i] = false;
			return;
		}
	abort();
}
bool
LWLockHeldByMe(LWLock *lock)
{
	for (int i = 0; i < 2; i++)
		if (lock == BufferDescriptorGetContentLock(&descriptors[i].bufferdesc))
			return locks[i];
	return false;
}
bool
LWLockHeldByMeInMode(LWLock *lock, LWLockMode mode)
{
	return LWLockHeldByMe(lock) && (mode != LW_EXCLUSIVE || source_capture);
}
uint32
LockBufHdr(BufferDesc *buf)
{
	return pg_atomic_fetch_or_u32(&buf->state, BM_LOCKED) | BM_LOCKED;
}
static void
cluster_bufmgr_pin_for_gcs_locked(BufferDesc *buf, uint32 state)
{
	pins[buf->buf_id]++;
	UnlockBufHdr(buf, state);
}
static void
cluster_bufmgr_unpin_for_gcs(BufferDesc *buf)
{
	UT_ASSERT(!locks[buf->buf_id] && pins[buf->buf_id]);
	pins[buf->buf_id]--;
}
static bool
cluster_bufmgr_should_pcm_track(BufferDesc *buf)
{
	return true;
}
static bool
cluster_bufmgr_pcm_current_image_locked(BufferDesc *buf, uint32 state)
{
	return cluster_pcm_x_current_image_shape(buf->pcm_state, buf->buffer_type,
											 (state & BM_VALID) != 0);
}
static bool
cluster_bufmgr_pcm_x_retained_image_locked(BufferDesc *buf, uint32 state)
{
	return buf->buffer_type == BUF_TYPE_PI && (state & BM_VALID);
}
ClusterPcmOwnResult
cluster_bufmgr_pcm_own_snapshot(BufferDesc *buf, ClusterPcmOwnSnapshot *out)
{
	memset(out, 0, sizeof(*out));
	out->tag = buf->tag;
	out->generation = cluster_pcm_own_gen_get(buf->buf_id);
	out->reservation_token = cluster_pcm_own_reservation_token_get(buf->buf_id);
	out->flags = cluster_pcm_own_flags_get(buf->buf_id);
	out->writer_activation_token = cluster_pcm_own_writer_activation_token_get(buf->buf_id);
	out->resource_x_activation_generation
		= cluster_pcm_own_resource_x_activation_generation_get(buf->buf_id);
	out->semantic_buf_state
		= pg_atomic_read_u32(&buf->state) & CLUSTER_PCM_OWN_SEMANTIC_BUF_STATE_MASK;
	out->pcm_state = buf->pcm_state;
	out->buffer_type = buf->buffer_type;
	return CLUSTER_PCM_OWN_OK;
}
bool
cluster_bufmgr_pcm_x_content_holder_write_permitted(BufferDesc *buf)
{
	return buf->buf_id == 1 && locks[1] && source_capture;
}

Size
mul_size(Size a, Size b)
{
	return a * b;
}
Size
add_size(Size a, Size b)
{
	return a + b;
}
void *
ShmemInitStruct(const char *name, Size size, bool *found)
{
	free(page_sources_memory);
	page_sources_memory = calloc(1, size);
	UT_ASSERT(page_sources_memory != NULL);
	*found = false;
	return page_sources_memory;
}
bool
cluster_space_recovery_flush_permitted_v1(const ClusterSpaceRecoveryBatchV1 *b, Buffer buf)
{
	return false;
}
static bool
StartBufferIO(BufferDesc *buf, bool input)
{
	if (!(pg_atomic_read_u32(&buf->state) & BM_DIRTY))
		return false;
	UT_ASSERT(!(pg_atomic_read_u32(&buf->state) & BM_IO_IN_PROGRESS));
	pg_atomic_fetch_or_u32(&buf->state, BM_IO_IN_PROGRESS);
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
AbortBufferIO(Buffer buf)
{
	aborts++;
	TerminateBufferIO(GetBufferDescriptor(buf - 1), false, BM_IO_ERROR);
}
XLogRecPtr
GetXLogInsertRecPtr(void)
{
	return local_insert_end;
}
void
XLogFlush(XLogRecPtr lsn)
{
	UT_ASSERT_EQ(lsn, PageGetLSN(pages[data_slot].data));
	wal_flushes++;
	fault(1);
}
bool
cluster_sf_dep_buffer_flush_blocked(BufferDesc *buf)
{
	return sf_blocked;
}
void
cluster_gcs_block_pi_write_note(BufferTag tag, uint64 scn)
{
	abort();
}
bool
DataChecksumsEnabled(void)
{
	return checksums;
}
SMgrRelation
smgropen(RelFileLocator r, BackendId b)
{
	UT_ASSERT(RelFileLocatorEquals(r, target.identity.locator));
	return &relation;
}
void
smgrwrite(SMgrRelation r, ForkNumber f, BlockNumber b, const void *data, bool skip)
{
	UT_ASSERT(locks[0] && locks[data_slot] && wal_flushes);
	UT_ASSERT_EQ(f, target.identity.forknum);
	UT_ASSERT_EQ(b, target.identity.blockno);
	fault(2);
	writes++;
	UT_ASSERT_EQ(pwrite(fileno(file), data, BLCKSZ, 0), BLCKSZ);
}
void
smgrimmedsync(SMgrRelation r, ForkNumber f)
{
	UT_ASSERT(locks[0] && !locks[1]);
	fault(3);
	UT_ASSERT_EQ(fsync(fileno(file)), 0);
	syncs++;
	if (concurrent_sync == 1) {
		RfPageVersionEdgeEntryV1 edge = { 0 };
		edge.page_class = RF_PAGE_CLASS_ORDINARY;
		edge.result_kind = RF_PAGE_STATE_PRESENT;
		memcpy(edge.result_incarnation, identity.incarnation, 16);
		((PageHeader)pages[1].data)->pd_block_scn++;
		source_capture = locks[1] = true;
		HOLD_INTERRUPTS();
		UT_ASSERT_EQ(cluster_page_wal_capture_native_v1(2, &edge,
														((PageHeader)pages[1].data)->pd_block_scn,
														0x300, 0x400, 0x9193, RM_HEAP_ID, 0),
					 CLUSTER_PAGE_WAL_CAPTURED);
		RESUME_INTERRUPTS();
		source_capture = locks[1] = false;
		/* A concurrent write plus its own flush can leave the buffer clean.
		 * Checking BM_DIRTY alone must not qualify our older DATA receipt. */
	}
	if (concurrent_sync == 2)
		busy[1] = true;
	if (stale_sync)
		writer.claim.identity.origin_owner_incarnation++;
	if (redirty_sync)
		pg_atomic_fetch_or_u32(&descriptors[data_slot].bufferdesc.state, BM_DIRTY);
	if (late_fence)
		pg_atomic_write_u32(&own[data_slot].flags, PCM_OWN_FLAG_REVOKING);
	if (storage_cut_changed)
		storage_cut_current = false;
	if (storage_space_changed)
		pg_atomic_fetch_add_u64(&own[0].generation, 1);
}
void
smgrread(SMgrRelation r, ForkNumber f, BlockNumber b, void *out)
{
	UT_ASSERT(locks[0] && !locks[1] && (storage_read || syncs));
	fault(storage_read && reads > 0 ? 5 : 4);
	reads++;
	UT_ASSERT_EQ(pread(fileno(file), out, BLCKSZ, 0), BLCKSZ);
	if (zero_disk) {
		memset(out, 0, BLCKSZ);
		return;
	}
	if (bad_checksum)
		((PageHeader)out)->pd_checksum ^= 1;
	if (bad_bytes && (!storage_read || reads > 1)) {
		((char *)out)[500] ^= 1;
		PageSetChecksumInplace(out, b);
	}
}
bool
cluster_pcm_lock_pi_storage_matches_v1(const ClusterPcmPiStorageCutV1 *cut)
{
	return storage_cut_current && cluster_pcm_pi_storage_cut_valid_v1(cut);
}
/* Exact native eviction boundary. The new consumer must already hold
 * mapping-X and the original header, with no pin or current residency. */
static bool
InvalidateBufferCommitLocked(BufferDesc *buf, BufferTag *tag, uint32 hash, LWLock *partition,
							 uint32 state)
{
	if (ack_epoch_race) {
		ack_writer_epoch++;
		ack_epoch_race = false;
	}
	UT_ASSERT(state & BM_LOCKED);
	UT_ASSERT(BufferTagsEqual(&buf->tag, tag));
	UT_ASSERT_EQ(BUF_STATE_GET_REFCOUNT(state), 0);
	UT_ASSERT_EQ(buf->pcm_state, PCM_STATE_N);
	UT_ASSERT_EQ(buf->buffer_type, BUF_TYPE_PI);
	cluster_page_wal_reset_reuse_locked(buf);
	ClearBufferTag(&buf->tag);
	resident[buf->buf_id] = false;
	buf->buffer_type = BUF_TYPE_CURRENT;
	pg_atomic_fetch_add_u64(&own[buf->buf_id].generation, 1);
	UnlockBufHdr(buf, state & ~(BUF_FLAG_MASK | BUF_USAGECOUNT_MASK));
	LWLockRelease(partition);
	pi_discards++;
	return true;
}
static void
shared_buffer_write_error_callback(void *arg)
{
	abort();
}
#define BufHdrGetBlock(buf) ((Block)(BufferBlocks + (Size)(buf)->buf_id * BLCKSZ))
#define BufferGetLSN(buf) PageGetLSN(BufHdrGetBlock(buf))
#define BufferIsPinned(buf) (pins[(buf) - 1] != 0)
#define pgstat_prepare_io_time() ((instr_time){ 0 })
#define pgstat_count_io_op_time(a, b, c, d, e) ((void)(d))
#undef ereport
#define ereport(level, rest)                                                                       \
	do {                                                                                           \
		InterruptHoldoffCount = 0;                                                                 \
		pg_re_throw();                                                                             \
	} while (0)
#include "test_cluster_space_recovery_flush.inc"
#include "test_cluster_page_flush.inc"
#include "test_cluster_page_data.inc"
#include "test_cluster_block_redeclare_scan.inc"

static void
bind_native_record(uint8 rmid, uint8 info)
{
	RfPageVersionEdgeEntryV1 e = { 0 };
	e.page_class = RF_PAGE_CLASS_ORDINARY;
	e.result_kind = RF_PAGE_STATE_PRESENT;
	memcpy(e.result_incarnation, identity.incarnation, 16);
	source_capture = locks[1] = true;
	HOLD_INTERRUPTS();
	UT_ASSERT(cluster_page_wal_capture_native_v1(2, &e, target.version.mutation_token, 0x100, 0x200,
												 0x9192, rmid, info)
			  == CLUSTER_PAGE_WAL_CAPTURED);
	RESUME_INTERRUPTS();
	source_capture = locks[1] = false;
}

static void
bind_native_source(void)
{
	bind_native_record(RM_HEAP_ID, 0);
}

static ClusterPageWalBindingV1
read_page_source(void)
{
	ClusterPageWalBindingV1 result = { 0 };
	locks[data_slot] = true;
	UT_ASSERT(cluster_page_wal_snapshot_v1(data_slot + 1, &result));
	locks[data_slot] = false;
	return result;
}

static void
reset(void)
{
	PageHeader p = (PageHeader)pages[1].data;

	data_slot = 1;
	redeclare_scope = 1;
	redeclare_content_attempts = 0;
	local_insert_end = 0x500;
	ack_writer_epoch = 1;
	ack_boot = 9;
	ack_writer_ready = true;
	ack_epoch_race = false;
	CurrentResourceOwner = (void *)1;
	MyBackendType = B_BG_WRITER;
	cluster_node_id = 0;
	if (file)
		fclose(file);
	file = tmpfile();
	UT_ASSERT(file != NULL);
	memset(pages, 0, sizeof(pages));
	memset(descriptors, 0, sizeof(descriptors));
	memset(own, 0, sizeof(own));
	memset(&writer, 0, sizeof(writer));
	memset(&target, 0, sizeof(target));
	memset(&identity, 0, sizeof(identity));
	target.database_incarnation = 3;
	target.identity.system_identifier = 17;
	target.identity.storage_uuid[0] = 1;
	target.identity.locator = (RelFileLocator){ DEFAULTTABLESPACE_OID, 9, 18000 };
	target.identity.forknum = MAIN_FORKNUM;
	target.identity.blockno = 7;
	target.version.segment_incarnation[0] = 12;
	target.version.mutation_token = 80;
	identity.key.system_identifier = 17;
	identity.key.database_incarnation = 3;
	identity.key.storage_uuid[0] = 1;
	identity.key.locator = target.identity.locator;
	identity.incarnation[0] = 12;
	identity.sequence = 1;
	identity.operation = 1;
	identity.state = CLUSTER_SPACE_IDENTITY_LIVE;
	UT_ASSERT(cluster_space_identity_page_encode(&identity, 10, pages[0].data, BLCKSZ));
	p->pd_lower = SizeOfPageHeaderData;
	p->pd_upper = p->pd_special = BLCKSZ;
	p->pd_pagesize_version = BLCKSZ | PG_PAGE_LAYOUT_VERSION;
	p->pd_block_scn = 80;
	PageSetLSNPreserveOrigin(pages[1].data, 0x200);
	UT_ASSERT(PageSetLSNOrigin(pages[1].data, 0));
	writer.claim.identity.system_identifier = 17;
	writer.claim.identity.storage_uuid[0] = 1;
	writer.claim.identity.authority_uuid[0] = 8;
	writer.claim.identity.origin_thread_id = 1;
	writer.claim.identity.thread_claim_created_at = 100;
	writer.claim.identity.origin_owner_incarnation = 9;
	writer.claim.identity.root_lineage_seq = 1;
	writer.claim.database_incarnation = 3;
	writer.claim.max_config_generation = 1;
	writer.claim.claim_sha256[0] = 6;
	writer.timeline = 1;
	for (int i = 0; i < 2; i++) {
		BufferDesc *buf = &descriptors[i].bufferdesc;
		buf->buf_id = i;
		buf->pcm_state = i ? PCM_STATE_X : PCM_STATE_S;
		buf->buffer_type = i ? BUF_TYPE_XCUR : BUF_TYPE_SCUR;
		InitBufferTag(&buf->tag, &target.identity.locator, i ? MAIN_FORKNUM : SPACE_FORKNUM,
					  i ? 7 : 0);
		pg_atomic_init_u32(&buf->state,
						   BM_VALID | BM_TAG_VALID | BM_PERMANENT | (i ? BM_DIRTY : 0));
		pg_atomic_init_u64(&own[i].generation, 2);
		pins[i] = 0;
		locks[i] = busy[i] = false;
		resident[i] = true;
	}
	relation.smgr_rlocator.locator = target.identity.locator;
	selected = true;
	sf_blocked = bad_checksum = bad_bytes = stale_sync = redirty_sync = false;
	zero_disk = late_fence = false;
	storage_read = storage_cut_changed = storage_space_changed = false;
	storage_cut_current = true;
	pi_discards = 0;
	pi_discard_race = 0;
	checksums = true;
	throw_at = 0;
	concurrent_sync = 0;
	writes = syncs = reads = wal_flushes = aborts = 0;
	cluster_smart_fusion = cluster_past_image = false;
	error_context_stack = NULL;
	cluster_page_wal_shmem_init();
	bind_native_source();
}
static void
clean(void)
{
	for (int i = 0; i < 2; i++) {
		UT_ASSERT_EQ(pins[i], 0);
		UT_ASSERT(!locks[i]);
		UT_ASSERT(!(pg_atomic_read_u32(&descriptors[i].bufferdesc.state)
					& (BM_IO_IN_PROGRESS | BM_LOCKED)));
	}
	UT_ASSERT(error_context_stack == NULL);
	UT_ASSERT_EQ(InterruptHoldoffCount, 0);
}
static void
success_and_old_completion(void)
{
	ClusterPageDataReceiptV1 *receipt = NULL;
	ClusterPageDataTargetV1 out;
	PGAlignedBlock disk;
	reset();
	UT_ASSERT(cluster_bufmgr_write_page_data_v1(&target, &receipt));
	if (receipt == NULL)
		return;
	UT_ASSERT(receipt != NULL);
	UT_ASSERT_EQ(writes, 1);
	UT_ASSERT_EQ(syncs, 1);
	UT_ASSERT_EQ(reads, 1);
	UT_ASSERT_EQ(pread(fileno(file), disk.data, BLCKSZ, 0), BLCKSZ);
	UT_ASSERT_EQ(((PageHeader)disk.data)->pd_checksum, pg_checksum_page(disk.data, 7));
	UT_ASSERT_EQ(((PageHeader)disk.data)->pd_block_scn, 80);
	clean();
	((PageHeader)pages[1].data)->pd_block_scn = 19;
	pg_atomic_fetch_or_u32(&descriptors[1].bufferdesc.state, BM_DIRTY);
	UT_ASSERT(cluster_page_data_receipt_read_v1(receipt, &out));
	UT_ASSERT_EQ(out.version.mutation_token, 80);
	UT_ASSERT_EQ(out.database_incarnation, 3);
	UT_ASSERT(memcmp(out.version.segment_incarnation, target.version.segment_incarnation, 16) == 0);
	UT_ASSERT(pg_atomic_read_u32(&descriptors[1].bufferdesc.state) & BM_DIRTY);
	cluster_page_data_receipt_free_v1(&receipt);
	UT_ASSERT(receipt == NULL);
}
static void
identity_refusals(void)
{
	for (int c = 0; c < 8; c++) {
		ClusterPageDataReceiptV1 *r = NULL;
		reset();
		switch (c) {
		case 0:
			target.database_incarnation++;
			break;
		case 1:
			target.version.segment_incarnation[15]++;
			break;
		case 2:
			target.version.mutation_token++;
			break;
		case 3:
			target.identity.storage_uuid[15]++;
			break;
		case 4:
			target.identity.forknum = FSM_FORKNUM;
			break;
		case 5:
			selected = false;
			break;
		case 6:
			identity.state = CLUSTER_SPACE_IDENTITY_TOMBSTONED;
			UT_ASSERT(cluster_space_identity_page_encode(&identity, 10, pages[0].data, BLCKSZ));
			break;
		case 7:
			((PageHeader)pages[1].data)->pd_special = 0;
			break;
		}
		UT_ASSERT(!cluster_bufmgr_write_page_data_v1(&target, &r));
		UT_ASSERT(r == NULL);
		UT_ASSERT_EQ(writes + syncs + reads + wal_flushes, 0);
		clean();
	}
}
static void
authority_refusals(void)
{
	for (int c = 0; c < 9; c++) {
		ClusterPageDataReceiptV1 *r = NULL;
		reset();
		switch (c) {
		case 0:
			resident[0] = false;
			break;
		case 1:
			resident[1] = false;
			break;
		case 2:
			busy[0] = true;
			break;
		case 3:
			busy[1] = true;
			break;
		case 4:
			descriptors[1].bufferdesc.buffer_type = BUF_TYPE_PI;
			break;
		case 5:
			pg_atomic_write_u32(&own[1].flags, PCM_OWN_FLAG_REVOKING);
			break;
		case 6:
			pg_atomic_write_u64(&own[1].writer_activation_token, 1);
			break;
		case 7:
			pg_atomic_fetch_or_u32(&descriptors[1].bufferdesc.state, BM_IO_ERROR);
			break;
		case 8:
			descriptors[1].bufferdesc.pcm_state = PCM_STATE_S;
			break;
		}
		UT_ASSERT(!cluster_bufmgr_write_page_data_v1(&target, &r));
		UT_ASSERT(r == NULL);
		UT_ASSERT_EQ(writes + syncs + reads + wal_flushes, 0);
		clean();
	}
}
static void
explicit_wal_origin(void)
{
	for (int c = 0; c < 4; c++) {
		ClusterPageDataReceiptV1 *r = NULL;
		reset();
		if (c < 2) {
			UT_ASSERT(PageSetLSNOrigin(pages[1].data, 1));
			PageSetLSNPreserveOrigin(pages[1].data, c ? 0x800 : 0x100);
		} else if (c == 2)
			PageSetLSNPreserveOrigin(pages[1].data, 0x800);
		else
			PageClearLSNOrigin(pages[1].data);
		UT_ASSERT(!cluster_bufmgr_write_page_data_v1(&target, &r));
		UT_ASSERT_EQ(writes + syncs + wal_flushes, 0);
		clean();
	}
}
static void
io_failure_cleanup(void)
{
	for (int at = 1; at <= 4; at++) {
		ClusterPageDataReceiptV1 *r = NULL;
		volatile bool caught = false;
		reset();
		throw_at = at;
		PG_TRY();
		{
			(void)cluster_bufmgr_write_page_data_v1(&target, &r);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(caught);
		UT_ASSERT(r == NULL);
		UT_ASSERT_EQ(aborts, at <= 2 ? 1 : 0);
		clean();
	}
}
static void
completion_refusals(void)
{
	for (int c = 0; c < 7; c++) {
		ClusterPageDataReceiptV1 *r = NULL;
		reset();
		if (c == 0)
			bad_checksum = true;
		if (c == 1)
			bad_bytes = true;
		if (c == 2)
			stale_sync = true;
		if (c == 3)
			redirty_sync = true;
		if (c == 4)
			cluster_smart_fusion = sf_blocked = true;
		if (c == 5)
			zero_disk = true;
		if (c == 6)
			late_fence = true;
		UT_ASSERT(!cluster_bufmgr_write_page_data_v1(&target, &r));
		UT_ASSERT(r == NULL);
		clean();
		if (c == 3 || c == 4)
			UT_ASSERT(pg_atomic_read_u32(&descriptors[1].bufferdesc.state) & BM_DIRTY);
	}
}
static void
vm_and_clean_completion(void)
{
	for (int enabled = 0; enabled <= 1; enabled++) {
		ClusterPageDataReceiptV1 *r = NULL;
		ClusterPageDataTargetV1 out;
		reset();
		checksums = enabled;
		target.identity.forknum = VISIBILITYMAP_FORKNUM;
		descriptors[1].bufferdesc.tag.forkNum = VISIBILITYMAP_FORKNUM;
		bind_native_source();
		UT_ASSERT(cluster_bufmgr_write_page_data_v1(&target, &r));
		if (!r)
			continue;
		UT_ASSERT(cluster_page_data_receipt_read_v1(r, &out));
		UT_ASSERT_EQ(out.identity.forknum, VISIBILITYMAP_FORKNUM);
		cluster_page_data_receipt_free_v1(&r);
		UT_ASSERT(cluster_bufmgr_write_page_data_v1(&target, &r));
		UT_ASSERT_EQ(writes, 1);
		UT_ASSERT_EQ(wal_flushes, 1);
		UT_ASSERT_EQ(syncs, 2);
		UT_ASSERT_EQ(reads, 2);
		cluster_page_data_receipt_free_v1(&r);
		clean();
	}
}
static void
new_claim_never_flushes_old_coordinate(void)
{
	for (int c = 0; c < 3; c++) {
		ClusterPageDataReceiptV1 *r = NULL;
		reset();
		if (c == 0)
			writer.claim.identity.origin_owner_incarnation++;
		if (c == 1)
			writer.claim.claim_sha256[15]++;
		if (c == 2) {
			source_capture = locks[1] = true;
			UT_ASSERT(cluster_page_wal_forget_v1(2));
			source_capture = locks[1] = false;
		}
		UT_ASSERT(!cluster_bufmgr_write_page_data_v1(&target, &r));
		UT_ASSERT(r == NULL);
		UT_ASSERT_EQ(writes + syncs + wal_flushes, 0);
		cluster_page_data_receipt_free_v1(&r);
		clean();
	}
}

static void
foreign_certified_image_uses_original_wal(void)
{
	for (int c = 0; c < 2; c++) {
		ClusterPageDataReceiptV1 *r = NULL;
		ClusterPageWalBindingV1 certified, original;
		ClusterPageWalInstallV1 prepared = { 0 };
		reset();
		original = read_page_source();
		UT_ASSERT(cluster_page_wal_flush_source_v1(&original, &certified));
		UT_ASSERT_EQ(wal_flushes, 1);
		writer.claim.identity.origin_thread_id = 2;
		writer.claim.identity.origin_node_id = cluster_node_id = 1;
		writer.claim.identity.origin_owner_incarnation++;
		writer.claim.claim_sha256[0]++;
		local_insert_end = c ? 0x100 : 0x900;
		/* Same page, uncertified old source cannot use the receiver's WAL. */
		UT_ASSERT(!cluster_bufmgr_write_page_data_v1(&target, &r));
		UT_ASSERT_EQ(writes + syncs, 0);
		source_capture = locks[1] = true;
		HOLD_INTERRUPTS();
		UT_ASSERT(cluster_page_wal_prepare_install_v1(2, &certified, pages[1].data, &prepared));
		UT_ASSERT(cluster_page_wal_publish_install_v1(2, &prepared));
		cluster_page_wal_release_install_v1(&prepared);
		RESUME_INTERRUPTS();
		source_capture = locks[1] = false;
		UT_ASSERT(cluster_bufmgr_write_page_data_v1(&target, &r));
		UT_ASSERT(r != NULL);
		UT_ASSERT_EQ(wal_flushes, 1);
		UT_ASSERT_EQ(writes, 1);
		UT_ASSERT_EQ(syncs, 1);
		cluster_page_data_receipt_free_v1(&r);
		clean();
	}
}
static void
ordinary_flush_uses_original_source(void)
{
	for (int scenario = 0; scenario < 4; scenario++) {
		ClusterPageWalBindingV1 certified, original;
		ClusterPageWalInstallV1 prepared = { 0 };
		volatile bool threw = false;
		reset();
		if (scenario > 0) {
			original = read_page_source();
			UT_ASSERT(cluster_page_wal_flush_source_v1(&original, &certified));
			writer.claim.identity.origin_thread_id = 2;
			writer.claim.identity.origin_node_id = cluster_node_id = 1;
			writer.claim.claim_sha256[0]++;
			local_insert_end = scenario == 2 ? 0x100 : 0x900;
			if (scenario != 3) {
				source_capture = locks[1] = true;
				HOLD_INTERRUPTS();
				UT_ASSERT(
					cluster_page_wal_prepare_install_v1(2, &certified, pages[1].data, &prepared));
				UT_ASSERT(cluster_page_wal_publish_install_v1(2, &prepared));
				cluster_page_wal_release_install_v1(&prepared);
				RESUME_INTERRUPTS();
				source_capture = locks[1] = false;
			}
		}
		/* Native checkpointer/replacement caller pins and content-locks the
		 * page. Its ordinary ResourceOwner error cleanup is a fixture here. */
		pins[1] = 1;
		locks[0] = locks[1] = true;
		PG_TRY();
		{
			FlushBuffer(&descriptors[1].bufferdesc, &relation, IOOBJECT_RELATION, IOCONTEXT_NORMAL);
		}
		PG_CATCH();
		{
			threw = true;
			AbortBufferIO(2);
			error_context_stack = NULL;
		}
		PG_END_TRY();
		locks[0] = locks[1] = false;
		pins[1] = 0;
		UT_ASSERT_EQ(threw, scenario == 3);
		UT_ASSERT_EQ(wal_flushes, 1); /* Foreign cases only flushed at A. */
		UT_ASSERT_EQ(writes, scenario == 3 ? 0 : 1);
		clean();
	}
}

/* Decoded WAL is an explicit input boundary; actual preflight, detached FPI
 * apply, dependency sealing, DATA I/O and receipt qualification run together. */
static RfPageOnlinePlanV1 *
data_contribution_plan_count(const ClusterWalSourceRef *sources, unsigned count)
{
	RfContributorStreamCutV1 cuts[4] = { { 0 } };
	RfPageOnlinePlanRequestV1 request = { 0 };
	RfPageOnlinePlanV1 *plan = NULL;
	uint64 tokens[] = { 10, 80, 19, 7, 6 };
	for (unsigned i = 0; i < count; i++) {
		cuts[i].failed_thread = sources[i].claim.identity.origin_thread_id;
		cuts[i].origin_owner_incarnation = sources[i].claim.identity.origin_owner_incarnation;
		cuts[i].timeline_id = 1;
		cuts[i].flags = RF_CONTRIBUTOR_CUT_COMPLETE;
		cuts[i].scan_begin_inclusive = 0x100;
		cuts[i].scan_end_exclusive = 0x200;
	}
	request.system_identifier = target.identity.system_identifier;
	memcpy(request.storage_uuid, target.identity.storage_uuid, 16);
	request.physical_cuts = cuts;
	request.participant_count = count;
	request.retention_binding_cookie = 41;
	UT_ASSERT_EQ(rf_page_online_plan_create_v1(&request, &plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(rf_page_online_plan_bind_sources_v1(plan, sources, count));
	for (unsigned i = 0; i < count; i++) {
		union {
			DecodedXLogRecord record;
			char bytes[sizeof(DecodedXLogRecord) + 2 * sizeof(DecodedBkpBlock)];
		} decoded = { 0 };
		XLogReaderState reader = { 0 };
		RfDetachedRecordPlanV1 detached;
		RfPageOnlineRecordIdentityV1 input = { 0 };
		PGAlignedBlock image;
		char error[1024];
		DecodedXLogRecord *record = &decoded.record;
		reader.record = record;
		reader.ReadRecPtr = record->lsn = 0x100;
		reader.EndRecPtr = record->next_lsn = 0x200;
		reader.system_identifier = request.system_identifier;
		reader.errormsg_buf = error;
		record->header.xl_rmid = RM_XLOG_ID;
		record->header.xl_info = XLOG_FPI;
		record->header.xl_crc = 0x9192;
		record->max_block_id = i == 2 ? 1 : 0;
		record->has_page_version_edge = true;
		record->page_version_edge.entry_count = record->max_block_id + 1;
		record->page_version_edge.result_token = tokens[i + 1];
		memcpy(image.data, pages[1].data, BLCKSZ);
		((PageHeader)image.data)->pd_block_scn = tokens[i + 1];
		for (int j = 0; j <= record->max_block_id; j++) {
			DecodedBkpBlock *block = &record->blocks[j];
			RfPageVersionEdgeEntryV1 *edge = &record->page_version_edge.entries[j];
			block->in_use = block->has_image = block->apply_image = true;
			block->rlocator = target.identity.locator;
			block->forknum = j == 0 ? MAIN_FORKNUM : VISIBILITYMAP_FORKNUM;
			block->blkno = i == 3 ? 8 : 7;
			block->bkp_image = image.data;
			block->bimg_len = BLCKSZ;
			block->bimg_info = BKPIMAGE_APPLY;
			edge->block_id = j;
			edge->page_class = RF_PAGE_CLASS_ORDINARY;
			edge->before_kind = edge->result_kind = RF_PAGE_STATE_PRESENT;
			memcpy(edge->before.segment_incarnation, target.version.segment_incarnation, 16);
			memcpy(edge->result_incarnation, target.version.segment_incarnation, 16);
			edge->before.mutation_token = i == 3 ? 10 : (j == 0 ? tokens[i] : 30);
			edge->edge_flags = RF_PAGE_EDGE_FULL_IMAGE_APPLY | RF_PAGE_EDGE_FULL_COVERAGE;
		}
		UT_ASSERT_EQ(rf_page_detached_preflight_v1(&reader, true, NULL, &detached),
					 RF_PAGE_PROOF_DETAIL_OK);
		input.record.system_identifier = request.system_identifier;
		memcpy(input.record.storage_uuid, request.storage_uuid, 16);
		input.record.origin_thread = i == 3 ? 1 : i + 1;
		input.record.timeline_id = 1;
		input.record.read_rec_ptr = 0x100;
		input.record.end_rec_ptr = 0x200;
		input.record.record_crc = 0x9192;
		input.record.rmid = RM_XLOG_ID;
		input.record.info = XLOG_FPI;
		input.participant_index = count == 4 ? (i == 0 ? 0 : (i == 3 ? 1 : i + 1)) : i;
		UT_ASSERT_EQ(rf_page_online_plan_queue_record_v1(plan, &detached, &input),
					 RF_PAGE_PROOF_DETAIL_OK);
	}
	UT_ASSERT_EQ(rf_page_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	return plan;
}

static RfPageOnlinePlanV1 *
data_contribution_plan(const ClusterWalSourceRef sources[3])
{
	return data_contribution_plan_count(sources, 3);
}

static RfPageOnlinePlanV1 *
prepare_storage_observation(ClusterWalSourceRef sources[3], ClusterPcmPiStorageCutV1 *cut)
{
	RfPageOnlinePlanV1 *plan;
	reset();
	memset(cut, 0, sizeof(*cut));
	for (int i = 0; i < 3; i++) {
		sources[i] = writer;
		sources[i].claim.identity.origin_thread_id = i + 1;
		sources[i].claim.identity.origin_node_id = i;
		sources[i].claim.claim_sha256[1] = i;
	}
	plan = data_contribution_plan(sources);
	cut->authority.state = PCM_STATE_N;
	cut->authority.master_holder.node_id = UINT32_MAX;
	cut->authority.x_holder_node = cut->authority.pending_x_requester_node = -1;
	cut->authority.transition_count = 6;
	cut->resource = descriptors[1].bufferdesc.tag;
	cut->pi_holders_bitmap = 7;
	cut->binding_generation = 1;
	cut->resource_formation = 17;
	cut->authority_generation = 3;
	cut->master_generation = 4;
	cut->master_session_incarnation = 31;
	/* Original C bytes are already on disk. No current DATA buffer is
	 * involved in the actual owner below; only SPACE0 stays resident. */
	resident[1] = false;
	storage_read = true;
	target.version.mutation_token = ((PageHeader)pages[1].data)->pd_block_scn = 7;
	UT_ASSERT(PageSetLSNOrigin(pages[1].data, 2));
	PageSetChecksumInplace(pages[1].data, 7);
	UT_ASSERT_EQ(pwrite(fileno(file), pages[1].data, BLCKSZ, 0), BLCKSZ);
	return plan;
}

static void
n_s_storage_observation_qualifies_only_terminal_data(void)
{
	for (int mode = 0; mode < 2; mode++) {
		ClusterWalSourceRef sources[3];
		ClusterPcmPiStorageCutV1 cut, proven;
		ClusterPageDataReceiptV1 *receipt = NULL;
		RfPageContributionPrefixV1 prefixes[3];
		RfPageOnlinePlanV1 *plan = prepare_storage_observation(sources, &cut);
		if (mode) {
			cut.authority.state = PCM_STATE_S;
			cut.authority.master_holder.node_id = 2;
			cut.authority.s_holders_bitmap = 4;
		}
		UT_ASSERT(cluster_bufmgr_observe_pi_storage_v1(&target, &cut, plan, sources, 3, &receipt));
		UT_ASSERT(receipt != NULL);
		UT_ASSERT_EQ(writes + wal_flushes, 0);
		UT_ASSERT_EQ(syncs, 1);
		UT_ASSERT_EQ(reads, 2);
		UT_ASSERT(cluster_page_data_pi_storage_proof_v1(receipt, plan, sources, 3, &proven));
		UT_ASSERT_EQ(memcmp(&proven, &cut, sizeof(cut)), 0);
		UT_ASSERT(cluster_page_data_prefix_v1(
			plan, sources, 3, (const ClusterPageDataReceiptV1 *const *)&receipt, 1, prefixes));
		UT_ASSERT_EQ(prefixes[0].first_uncovered_lsn, 0x200);
		UT_ASSERT_EQ(prefixes[1].first_uncovered_lsn, 0x200);
		UT_ASSERT_EQ(prefixes[2].first_uncovered_lsn, 0x100); /* Unwritten VM remains. */
		cluster_page_data_receipt_free_v1(&receipt);
		rf_page_online_plan_destroy_v1(&plan);
		clean();
	}
}

static void
storage_observation_rejects_incomplete_or_changed_proof(void)
{
	for (int c = 0; c < 11; c++) {
		ClusterWalSourceRef sources[3];
		ClusterPcmPiStorageCutV1 cut;
		ClusterPageDataReceiptV1 *receipt = NULL;
		RfPageOnlinePlanV1 *plan = prepare_storage_observation(sources, &cut);
		if (c == 0)
			target.version.mutation_token
				= 80; /* A's earlier completion is insufficient for N/S. */
		if (c == 1)
			sources[1].claim.identity.origin_owner_incarnation++;
		if (c == 2)
			bad_checksum = true;
		if (c == 3)
			bad_bytes = true;
		if (c == 4)
			storage_cut_changed = true;
		if (c == 5)
			storage_space_changed = true;
		if (c == 6)
			stale_sync = true;
		if (c == 7)
			zero_disk = true;
		if (c == 8)
			resident[0] = false;
		if (c == 9)
			storage_cut_current = false;
		if (c == 10) {
			UT_ASSERT(PageSetLSNOrigin(pages[1].data, 1));
			PageSetChecksumInplace(pages[1].data, 7);
			UT_ASSERT_EQ(pwrite(fileno(file), pages[1].data, BLCKSZ, 0), BLCKSZ);
		}
		UT_ASSERT(!cluster_bufmgr_observe_pi_storage_v1(&target, &cut, plan, sources, 3, &receipt));
		UT_ASSERT(receipt == NULL);
		UT_ASSERT_EQ(writes + wal_flushes, 0);
		if (c < 2 || c == 8 || c == 9)
			UT_ASSERT_EQ(reads + syncs, 0);
		cluster_page_data_receipt_free_v1(&receipt);
		rf_page_online_plan_destroy_v1(&plan);
		clean();
	}
}

static void
storage_observation_releases_space_on_io_error(void)
{
	for (int at = 3; at <= 5; at++) {
		ClusterWalSourceRef sources[3];
		ClusterPcmPiStorageCutV1 cut;
		ClusterPageDataReceiptV1 *receipt = NULL;
		RfPageOnlinePlanV1 *plan = prepare_storage_observation(sources, &cut);
		volatile bool caught = false;
		throw_at = at;
		PG_TRY();
		{
			(void)cluster_bufmgr_observe_pi_storage_v1(&target, &cut, plan, sources, 3, &receipt);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(caught);
		UT_ASSERT(receipt == NULL);
		UT_ASSERT_EQ(writes + wal_flushes + aborts, 0);
		rf_page_online_plan_destroy_v1(&plan);
		clean();
	}
}

static void
plan_sources_are_immutable(void)
{
	RfContributorStreamCutV1 cut = { 0 };
	RfPageOnlinePlanRequestV1 request = { 0 };
	RfPageOnlinePlanV1 *plan = NULL;
	ClusterWalSourceRef source, observed, sentinel;
	RfPageContributionPrefixV1 prefix, untouched;

	reset();
	cut.failed_thread = cut.timeline_id = 1;
	cut.origin_owner_incarnation = writer.claim.identity.origin_owner_incarnation;
	cut.flags = RF_CONTRIBUTOR_CUT_COMPLETE | RF_CONTRIBUTOR_CUT_EXPLICIT_EMPTY;
	cut.scan_begin_inclusive = cut.scan_end_exclusive = 0x100;
	request.system_identifier = writer.claim.identity.system_identifier;
	memcpy(request.storage_uuid, writer.claim.identity.storage_uuid, 16);
	request.physical_cuts = &cut;
	request.participant_count = request.retention_binding_cookie = 1;
	UT_ASSERT_EQ(rf_page_online_plan_create_v1(&request, &plan), RF_PAGE_PROOF_DETAIL_OK);
	for (int i = 0; i < 14; i++) {
		source = writer;
		switch (i) {
		case 0:
			source.claim.identity.system_identifier++;
			break;
		case 1:
			source.claim.identity.storage_uuid[0]++;
			break;
		case 2:
			source.claim.identity.origin_thread_id++;
			break;
		case 3:
			source.claim.identity.origin_node_id++;
			break;
		case 4:
			source.claim.identity.reserved42++;
			break;
		case 5:
			source.claim.identity.reserved60++;
			break;
		case 6:
			source.claim.identity.thread_claim_created_at = 0;
			break;
		case 7:
			source.claim.identity.origin_owner_incarnation = 0;
			break;
		case 8:
			source.claim.identity.root_lineage_seq = 0;
			break;
		case 9:
			memset(source.claim.identity.authority_uuid, 0, 16);
			break;
		case 10:
			source.claim.database_incarnation = 0;
			break;
		case 11:
			source.claim.max_config_generation = 0;
			break;
		case 12:
			memset(source.claim.claim_sha256, 0, 32);
			break;
		case 13:
			source.timeline++;
			break;
		}
		UT_ASSERT(!rf_page_online_plan_bind_sources_v1(plan, &source, 1));
	}
	source = writer;
	UT_ASSERT(!rf_page_online_plan_bind_sources_v1(plan, &source, 0));
	UT_ASSERT(!rf_page_online_plan_bind_sources_v1(plan, NULL, 1));
	UT_ASSERT(rf_page_online_plan_bind_sources_v1(plan, &source, 1));
	UT_ASSERT(!rf_page_online_plan_bind_sources_v1(plan, &source, 1));
	memset(&sentinel, 0xa5, sizeof(sentinel));
	observed = sentinel;
	UT_ASSERT(!rf_page_online_plan_source_v1(plan, 0, &observed));
	UT_ASSERT_EQ(memcmp(&observed, &sentinel, sizeof(observed)), 0);
	source.claim.identity.origin_owner_incarnation++;
	UT_ASSERT_EQ(rf_page_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(rf_page_online_plan_source_v1(plan, 0, &observed));
	UT_ASSERT(cluster_page_data_source_same(&observed, &writer));
	observed = sentinel;
	UT_ASSERT(!rf_page_online_plan_source_v1(plan, 1, &observed));
	UT_ASSERT_EQ(memcmp(&observed, &sentinel, sizeof(observed)), 0);
	UT_ASSERT(cluster_page_data_prefix_v1(plan, &writer, 1, NULL, 0, &prefix));
	UT_ASSERT_EQ(prefix.first_uncovered_lsn, 0x100);
	rf_page_online_plan_destroy_v1(&plan);
	UT_ASSERT_EQ(rf_page_online_plan_create_v1(&request, &plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_page_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(!rf_page_online_plan_bind_sources_v1(plan, &writer, 1));
	memset(&untouched, 0xa5, sizeof(untouched));
	prefix = untouched;
	UT_ASSERT(!cluster_page_data_prefix_v1(plan, &writer, 1, NULL, 0, &prefix));
	UT_ASSERT_EQ(memcmp(&prefix, &untouched, sizeof(prefix)), 0);
	rf_page_online_plan_destroy_v1(&plan);
	clean();
}

static ClusterPcmPiWriteCutV1
data_pi_cut(void)
{
	ClusterPcmPiWriteCutV1 cut = { 0 };
	InitBufferTag(&cut.holder.assertion.resource, &target.identity.locator, target.identity.forknum,
				  target.identity.blockno);
	cut.holder.assertion.requester_node = cluster_node_id;
	cut.holder.base_authority_generation = 1;
	cut.holder.final_authority_generation = 2;
	cut.holder.resource_formation = 17;
	cut.holder.master_session_incarnation = 31;
	cut.holder.assertion_sequence = 41;
	cut.holder.requester_target_generation = cluster_pcm_own_gen_get(data_slot);
	cut.holder.phase = RESOURCE_X_MASTER_SETTLED;
	cut.binding_generation = 51;
	cut.transition_count = 4;
	cut.master_generation = 71;
	cut.master_node = 1;
	cut.pi_holders_bitmap = 3;
	return cut;
}

static void
pi_cut_must_match_current_holder_before_data(void)
{
	for (int wrong = 0; wrong < 9; wrong++) {
		ClusterPageDataReceiptV1 *receipt = NULL;
		ClusterPcmPiWriteCutV1 cut;
		reset();
		cut = data_pi_cut();
		switch (wrong) {
		case 0:
			cut.holder.assertion.requester_node++;
			break;
		case 1:
			cut.holder.requester_target_generation++;
			break;
		case 2:
			cut.holder.assertion.resource.blockNum++;
			break;
		case 3:
			cut.holder.phase = RESOURCE_X_MASTER_GRANT_COMMITTED;
			break;
		case 4:
			cut.holder.resource_formation = 0;
			break;
		case 5:
			cut.master_generation = 0;
			break;
		case 6:
			cut.transition_count = UINT64_MAX;
			break;
		case 7:
			cut.pi_holders_bitmap = 0;
			break;
		case 8:
			break; /* NULL is not the ordinary endpoint. */
		}
		UT_ASSERT(
			!cluster_bufmgr_write_page_data_at_cut_v1(&target, wrong == 8 ? NULL : &cut, &receipt));
		UT_ASSERT(receipt == NULL);
		UT_ASSERT_EQ(writes + syncs + reads, 0);
		clean();
	}
}

static void
tag_write_samples_real_identity_and_version(void)
{
	ClusterPcmPiWriteCutV1 cut, exported;
	ClusterPageDataReceiptV1 *receipt = NULL;
	ClusterPageDataTargetV1 observed;
	ClusterPageWalBindingV1 binding;
	reset();
	cut = data_pi_cut();
	UT_ASSERT(cluster_bufmgr_write_tag_data_at_cut_v1(&identity.key, &cut, &receipt));
	UT_ASSERT(cluster_page_data_receipt_read_v1(receipt, &observed));
	UT_ASSERT(rf_page_identity_equal_v1(&observed.identity, &target.identity));
	UT_ASSERT(rf_page_version_equal_v1(&observed.version, &target.version));
	UT_ASSERT(cluster_page_data_pi_export_v1(receipt, &binding, &exported));
	UT_ASSERT_EQ(memcmp(&cut, &exported, sizeof(cut)), 0);
	UT_ASSERT_EQ(binding.flags, CLUSTER_PAGE_WAL_NATIVE_FLUSHED);
	UT_ASSERT_EQ(binding.record_end, 0x200);
	UT_ASSERT_EQ(writes, 1);
	UT_ASSERT_EQ(syncs, 1);
	UT_ASSERT_EQ(reads, 1);
	UT_ASSERT_EQ(wal_flushes, 1);
	cluster_page_data_receipt_free_v1(&receipt);
	clean();
}

static void
tag_write_rejects_wrong_namespace_or_holder(void)
{
	for (int wrong = 0; wrong < 11; wrong++) {
		ClusterPcmPiWriteCutV1 cut;
		ClusterSpaceIdentityKey key;
		ClusterPageDataReceiptV1 *receipt = NULL;
		reset();
		cut = data_pi_cut();
		key = identity.key;
		switch (wrong) {
		case 0:
			key.system_identifier++;
			break;
		case 1:
			key.database_incarnation++;
			break;
		case 2:
			key.storage_uuid[15]++;
			break;
		case 3:
			key.locator.relNumber++;
			break;
		case 4:
			cut.holder.requester_target_generation++;
			break;
		case 5:
			cut.holder.assertion.requester_node++;
			break;
		case 6:
			resident[0] = false;
			break;
		case 7:
			resident[1] = false;
			break;
		case 8:
			MyBackendType = B_LMON;
			break;
		case 9:
			CurrentResourceOwner = NULL;
			break;
		case 10:
			identity.state = CLUSTER_SPACE_IDENTITY_TOMBSTONED;
			UT_ASSERT(cluster_space_identity_page_encode(&identity, 10, pages[0].data, BLCKSZ));
			break;
		}
		UT_ASSERT(!cluster_bufmgr_write_tag_data_at_cut_v1(&key, &cut, &receipt));
		UT_ASSERT(receipt == NULL);
		UT_ASSERT_EQ(writes + syncs + reads + wal_flushes, 0);
		clean();
	}
}

static void
remote_import_requires_original_job(void)
{
	ClusterPcmPiWriteCutV1 cut, exported;
	ClusterPageDataReceiptV1 *written = NULL, *imported = NULL;
	ClusterPageDataTargetV1 observed;
	ClusterPageWalBindingV1 binding;
	reset();
	cut = data_pi_cut();
	UT_ASSERT(cluster_bufmgr_write_tag_data_at_cut_v1(&identity.key, &cut, &written));
	UT_ASSERT(cluster_page_data_pi_export_v1(written, &remote_data_binding, &remote_data_cut));
	remote_data_ready = false;
	UT_ASSERT(!cluster_page_data_from_remote_v1((const ClusterPiDataV1 *)1, &imported));
	UT_ASSERT(imported == NULL);
	remote_data_ready = true;
	UT_ASSERT(!cluster_page_data_from_remote_v1(NULL, &imported));
	UT_ASSERT(cluster_page_data_from_remote_v1((const ClusterPiDataV1 *)1, &imported));
	UT_ASSERT(cluster_page_data_receipt_read_v1(imported, &observed));
	UT_ASSERT(rf_page_version_equal_v1(&observed.version, &target.version));
	UT_ASSERT(cluster_page_data_pi_export_v1(imported, &binding, &exported));
	UT_ASSERT_EQ(memcmp(&binding, &remote_data_binding, sizeof(binding)), 0);
	UT_ASSERT_EQ(memcmp(&exported, &cut, sizeof(cut)), 0);
	cluster_page_data_receipt_free_v1(&imported);
	remote_data_binding.flags = 0;
	UT_ASSERT(!cluster_page_data_from_remote_v1((const ClusterPiDataV1 *)1, &imported));
	UT_ASSERT(imported == NULL);
	cluster_page_data_receipt_free_v1(&written);
	remote_data_ready = false;
	clean();
}

static void
tag_write_io_failure_never_exports_completion(void)
{
	for (int at = 1; at <= 4; at++) {
		ClusterPcmPiWriteCutV1 cut;
		ClusterPageDataReceiptV1 *volatile receipt = NULL;
		volatile bool caught = false;
		reset();
		cut = data_pi_cut();
		throw_at = at;
		PG_TRY();
		{
			(void)cluster_bufmgr_write_tag_data_at_cut_v1(&identity.key, &cut,
														  (ClusterPageDataReceiptV1 **)&receipt);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(caught);
		UT_ASSERT(receipt == NULL);
		clean();
	}
}

static void
physical_receipts_close_only_exact_contributions(void)
{
	ClusterPageDataReceiptV1 *old = NULL, *old_at_cut = NULL, *main = NULL, *vm = NULL;
	ClusterPcmPiWriteCutV1 cut, proven, pi_sentinel;
	const ClusterPageDataReceiptV1 *receipts[2];
	ClusterWalSourceRef sources[3];
	RfPageContributionPrefixV1 prefixes[3] = { { 0 } }, retained[3] = { { 0 } }, sentinel[3];
	RfPageOnlinePlanV1 *plan;
	FILE *main_file;
	reset();
	for (int i = 0; i < 3; i++) {
		sources[i] = writer;
		sources[i].claim.identity.origin_thread_id = i + 1;
		sources[i].claim.identity.origin_node_id = i;
		sources[i].claim.claim_sha256[1] = i;
	}
	plan = data_contribution_plan(sources);
	bind_native_record(RM_XLOG_ID, XLOG_FPI);
	UT_ASSERT(cluster_bufmgr_write_page_data_v1(&target, &old));
	memset(&pi_sentinel, 0xa5, sizeof(pi_sentinel));
	proven = pi_sentinel;
	UT_ASSERT(!cluster_page_data_pi_proof_v1(old, plan, sources, 3, &proven));
	UT_ASSERT_EQ(memcmp(&proven, &pi_sentinel, sizeof(proven)), 0);
	cut = data_pi_cut();
	UT_ASSERT(cluster_bufmgr_write_page_data_at_cut_v1(&target, &cut, &old_at_cut));
	/* Its own older cut may qualify this earlier DATA. The master must
	 * reject it after A -> B -> C; the PAGE prefix below leaves B/C owed. */
	UT_ASSERT(cluster_page_data_pi_proof_v1(old_at_cut, plan, sources, 3, &proven));
	UT_ASSERT_EQ(memcmp(&proven, &cut, sizeof(proven)), 0);
	receipts[0] = old;
	UT_ASSERT(cluster_page_data_prefix_v1(plan, sources, 3, receipts, 1, prefixes));
	UT_ASSERT_EQ(prefixes[0].first_uncovered_lsn, 0x200);
	UT_ASSERT_EQ(prefixes[1].first_uncovered_lsn, 0x100);
	UT_ASSERT_EQ(prefixes[2].first_uncovered_lsn, 0x100);
	UT_ASSERT(rf_page_online_plan_dependency_prefix_v1(plan, prefixes, 3, retained));
	for (int i = 0; i < 3; i++)
		UT_ASSERT_EQ(retained[i].first_uncovered_lsn, 0x100);
	writer = sources[2];
	cluster_node_id = 2;
	target.version.mutation_token = ((PageHeader)pages[1].data)->pd_block_scn = 7;
	UT_ASSERT(PageSetLSNOrigin(pages[1].data, 2));
	pg_atomic_fetch_or_u32(&descriptors[1].bufferdesc.state, BM_DIRTY);
	bind_native_record(RM_XLOG_ID, XLOG_FPI);
	cut = data_pi_cut();
	/* The request still knows A's token. C already changed the page before
	 * the background request arrived; write the actual current version. */
	target.version.mutation_token = 80;
	UT_ASSERT(!cluster_bufmgr_write_page_data_at_cut_v1(&target, &cut, &main));
	UT_ASSERT(main == NULL);
	UT_ASSERT(cluster_bufmgr_write_current_data_at_cut_v1(&target, &cut, &main));
	target.version.mutation_token = 7;
	UT_ASSERT(cluster_page_data_pi_proof_v1(main, plan, sources, 3, &proven));
	UT_ASSERT_EQ(memcmp(&proven, &cut, sizeof(proven)), 0);
	cut.binding_generation++;
	UT_ASSERT(cluster_page_data_pi_proof_v1(main, plan, sources, 3, &proven));
	UT_ASSERT(proven.binding_generation != cut.binding_generation);
	receipts[0] = main;
	UT_ASSERT(cluster_page_data_prefix_v1(plan, sources, 3, receipts, 1, prefixes));
	UT_ASSERT_EQ(prefixes[0].first_uncovered_lsn, 0x200);
	UT_ASSERT_EQ(prefixes[1].first_uncovered_lsn, 0x200);
	UT_ASSERT_EQ(prefixes[2].first_uncovered_lsn, 0x100); /* C's VM is not written. */
	UT_ASSERT(rf_page_online_plan_dependency_prefix_v1(plan, prefixes, 3, retained));
	for (int i = 0; i < 3; i++)
		UT_ASSERT_EQ(retained[i].first_uncovered_lsn, 0x100);
	main_file = file;
	file = tmpfile(); /* Physical fork routing remains the explicit smgr fixture. */
	UT_ASSERT(file != NULL);
	target.identity.forknum = descriptors[1].bufferdesc.tag.forkNum = VISIBILITYMAP_FORKNUM;
	pg_atomic_fetch_or_u32(&descriptors[1].bufferdesc.state, BM_DIRTY);
	bind_native_record(RM_XLOG_ID, XLOG_FPI);
	UT_ASSERT(cluster_bufmgr_write_page_data_v1(&target, &vm));
	receipts[1] = vm;
	UT_ASSERT(cluster_page_data_prefix_v1(plan, sources, 3, receipts, 2, prefixes));
	for (int i = 0; i < 3; i++)
		UT_ASSERT_EQ(prefixes[i].first_uncovered_lsn, 0x200);
	UT_ASSERT(rf_page_online_plan_dependency_prefix_v1(plan, prefixes, 3, retained));
	for (int i = 0; i < 3; i++)
		UT_ASSERT_EQ(retained[i].first_uncovered_lsn, 0x200);
	/* Even these actual completions do not publish A's root. If its durable
	 * checkpoint has not advanced, B/C still retain the ancestry A needs. */
	prefixes[0].first_uncovered_lsn = 0x100;
	UT_ASSERT(rf_page_online_plan_dependency_prefix_v1(plan, prefixes, 3, retained));
	for (int i = 0; i < 3; i++)
		UT_ASSERT_EQ(retained[i].first_uncovered_lsn, 0x100);
	/* An old completion plus C's VM still cannot cover B/C's main page. */
	receipts[0] = old;
	UT_ASSERT(cluster_page_data_prefix_v1(plan, sources, 3, receipts, 2, prefixes));
	UT_ASSERT_EQ(prefixes[1].first_uncovered_lsn, 0x100);
	UT_ASSERT_EQ(prefixes[2].first_uncovered_lsn, 0x100);
	memset(sentinel, 0xa5, sizeof(sentinel));
	for (int i = 0; i < 7; i++) {
		ClusterWalSourceRef changed[3];
		memcpy(changed, sources, sizeof(changed));
		memcpy(prefixes, sentinel, sizeof(prefixes));
		receipts[0] = main;
		receipts[1] = vm;
		if (i == 0)
			changed[2].claim.identity.origin_owner_incarnation++;
		if (i == 1)
			changed[2].claim.claim_sha256[0]++;
		if (i == 2)
			changed[2].claim.database_incarnation++;
		if (i == 3)
			receipts[0] = NULL;
		if (i == 4)
			receipts[1] = main;
		if (i == 5) {
			receipts[0] = vm;
			receipts[1] = main;
		}
		if (i == 6)
			changed[1].claim.identity.origin_owner_incarnation++;
		UT_ASSERT(!cluster_page_data_prefix_v1(plan, changed, 3, receipts, 2, prefixes));
		UT_ASSERT_EQ(memcmp(prefixes, sentinel, sizeof(prefixes)), 0);
	}
	/* Physical completion of another record/version is not evidence for
	 * this sealed input, even at the same page address and writer. */
	for (int i = 0; i < 4; i++) {
		ClusterPageDataReceiptV1 *other = NULL;
		RfPageVersionEdgeEntryV1 edge = { 0 };
		uint64 token = i == 3 ? 999 : 7;
		XLogRecPtr end = i == 3 ? 0x400 : 0x200;
		target.version.mutation_token = ((PageHeader)pages[1].data)->pd_block_scn = token;
		PageSetLSNPreserveOrigin(pages[1].data, end);
		edge.page_class = RF_PAGE_CLASS_ORDINARY;
		edge.result_kind = RF_PAGE_STATE_PRESENT;
		memcpy(edge.result_incarnation, target.version.segment_incarnation, 16);
		source_capture = locks[1] = true;
		HOLD_INTERRUPTS();
		UT_ASSERT(cluster_page_wal_capture_native_v1(2, &edge, token,
													 i == 3	  ? 0x300
													 : i == 2 ? 0x108
															  : 0x100,
													 end, i == 0 ? 0x9193 : 0x9192, RM_XLOG_ID,
													 i == 1 ? XLOG_FPI_FOR_HINT : XLOG_FPI)
				  == CLUSTER_PAGE_WAL_CAPTURED);
		RESUME_INTERRUPTS();
		source_capture = locks[1] = false;
		pg_atomic_fetch_or_u32(&descriptors[1].bufferdesc.state, BM_DIRTY);
		UT_ASSERT(cluster_bufmgr_write_page_data_v1(&target, &other));
		receipts[0] = main;
		receipts[1] = other;
		memcpy(prefixes, sentinel, sizeof(prefixes));
		UT_ASSERT(!cluster_page_data_prefix_v1(plan, sources, 3, receipts, 2, prefixes));
		UT_ASSERT_EQ(memcmp(prefixes, sentinel, sizeof(prefixes)), 0);
		cluster_page_data_receipt_free_v1(&other);
	}
	cluster_page_data_receipt_free_v1(&old);
	cluster_page_data_receipt_free_v1(&old_at_cut);
	cluster_page_data_receipt_free_v1(&main);
	cluster_page_data_receipt_free_v1(&vm);
	rf_page_online_plan_destroy_v1(&plan);
	fclose(file);
	file = main_file;
	clean();
}

static void
physical_pi_from_source(const ClusterWalSourceRef *source, uint64 token)
{
	uint64 requested = target.version.mutation_token;
	writer = *source;
	cluster_node_id = writer.claim.identity.origin_node_id;
	resident[1] = true;
	((PageHeader)pages[1].data)->pd_block_scn = token;
	UT_ASSERT(PageSetLSNOrigin(pages[1].data, writer.claim.identity.origin_thread_id - 1));
	pg_atomic_write_u32(&descriptors[1].bufferdesc.state, BM_VALID | BM_TAG_VALID | BM_PERMANENT);
	target.version.mutation_token = token;
	bind_native_record(RM_XLOG_ID, XLOG_FPI);
	target.version.mutation_token = requested;
	descriptors[1].bufferdesc.pcm_state = PCM_STATE_N;
	descriptors[1].bufferdesc.buffer_type = BUF_TYPE_PI;
	pg_atomic_fetch_and_u32(&descriptors[1].bufferdesc.state, ~BM_VALID);
}

static void
physical_pi_discard_is_ancestry_and_generation_exact(void)
{
	for (int c = 0; c < 15; c++) {
		ClusterWalSourceRef sources[3], pi_source;
		ClusterPcmPiStorageCutV1 cut;
		ClusterPageDataReceiptV1 *receipt = NULL;
		RfPageOnlinePlanV1 *plan = prepare_storage_observation(sources, &cut);
		ClusterPiPhysicalResultV1 expected
			= c == 0 ? CLUSTER_PI_PHYSICAL_DISCARDED : CLUSTER_PI_PHYSICAL_RETRY;
		UT_ASSERT(cluster_bufmgr_observe_pi_storage_v1(&target, &cut, plan, sources, 3, &receipt));
		pi_source = sources[0];
		if (c == 5)
			pi_source.claim.identity.origin_owner_incarnation++;
		physical_pi_from_source(&pi_source, 80);
		if (c == 1)
			pg_atomic_fetch_add_u32(&descriptors[1].bufferdesc.state, 1);
		if (c == 2)
			pg_atomic_fetch_or_u32(&descriptors[1].bufferdesc.state, BM_DIRTY);
		if (c == 3)
			pg_atomic_write_u32(&own[1].flags, PCM_OWN_FLAG_REVOKING);
		if (c == 4)
			((PageHeader)pages[1].data)->pd_block_scn++;
		if (c == 6)
			pi_discard_race = 1;
		if (c == 7)
			pi_discard_race = 2;
		if (c == 8 || c == 9) {
			descriptors[1].bufferdesc.pcm_state = c == 8 ? PCM_STATE_X : PCM_STATE_S;
			descriptors[1].bufferdesc.buffer_type = c == 8 ? BUF_TYPE_XCUR : BUF_TYPE_SCUR;
			pg_atomic_fetch_or_u32(&descriptors[1].bufferdesc.state, BM_VALID);
			expected = CLUSTER_PI_PHYSICAL_REPLACED;
		}
		if (c == 10) {
			resident[1] = false;
			expected = CLUSTER_PI_PHYSICAL_ABSENT;
		}
		if (c == 11) {
			locks[1] = source_capture = true;
			UT_ASSERT(cluster_page_wal_forget_v1(2));
			locks[1] = source_capture = false;
		}
		if (c == 12)
			receipt->target.version.segment_incarnation[0]++;
		if (c == 13)
			pi_discard_race = 3;
		if (c == 14) {
			identity.incarnation[0]++;
			physical_pi_from_source(&pi_source, 80);
		}
		UT_ASSERT_EQ(cluster_bufmgr_discard_pi_at_data_v1(receipt, plan, sources, 3), expected);
		UT_ASSERT_EQ(pi_discards, c == 0 ? 1 : 0);
		if (c == 0) {
			UT_ASSERT_EQ(cluster_bufmgr_discard_pi_at_data_v1(receipt, plan, sources, 3),
						 CLUSTER_PI_PHYSICAL_ABSENT);
			UT_ASSERT_EQ(pi_discards, 1);
		}
		UT_ASSERT_EQ(writes + wal_flushes, 0);
		cluster_page_data_receipt_free_v1(&receipt);
		rf_page_online_plan_destroy_v1(&plan);
		clean();
	}
}

static void
physical_ack_consumes_original_prior_exit_and_full_retained_scope(void)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		ClusterWalSourceRef sources[4], current;
		ClusterPcmPiStorageCutV1 cut;
		ClusterPageDataReceiptV1 *receipt = NULL;
		ClusterPiPhysicalAckV1 *ack = NULL;
		RfPageOnlinePlanV1 *plan = prepare_storage_observation(sources, &cut);
		current = writer;
		rf_page_online_plan_destroy_v1(&plan);
		sources[3] = sources[2];
		sources[2] = sources[1];
		sources[1] = sources[0]; /* Live A writes another block in the same cut. */
		sources[0].claim.identity.origin_owner_incarnation--;
		plan = data_contribution_plan_count(sources, 4);
		UT_ASSERT(cluster_bufmgr_observe_pi_storage_v1(&target, &cut, plan, sources, 4, &receipt));
		writer = current;
		ack_retired_source = sources[0];
		ack_retired_writer = current;
		ack_retired_allowed = fault != 1;
		ack_retired_calls = 0;
		UT_ASSERT_EQ(cluster_bufmgr_ack_pi_at_data_v1(receipt, plan, sources, 4,
													  fault == 2 ? NULL : (void *)1, &ack),
					 fault == 0);
		UT_ASSERT_EQ(ack_retired_calls, 1);
		UT_ASSERT_EQ(pi_discards, 0); /* Exited boot cannot have a local PI. */
		cluster_page_data_pi_ack_free_v1(&ack);
		cluster_page_data_receipt_free_v1(&receipt);
		rf_page_online_plan_destroy_v1(&plan);
		ack_retired_allowed = false;
		clean();
	}
}

static void
physical_ack_requires_actual_consumption_and_original_boot(void)
{
	for (int c = 0; c < 11; c++) {
		ClusterWalSourceRef sources[3], native;
		ClusterPcmPiStorageCutV1 cut;
		ClusterPageDataReceiptV1 *receipt = NULL;
		ClusterPiPhysicalAckV1 *ack = NULL;
		RfPageOnlinePlanV1 *plan = prepare_storage_observation(sources, &cut);
		int32 node = 99;
		native = writer;
		if (c == 4) {
			rf_page_online_plan_destroy_v1(&plan);
			sources[0].claim.identity.origin_owner_incarnation--;
			plan = data_contribution_plan(sources);
		}
		if (c == 7)
			cut.pi_holders_bitmap = 6;
		UT_ASSERT(cluster_bufmgr_observe_pi_storage_v1(&target, &cut, plan, sources, 3, &receipt));
		physical_pi_from_source(&sources[0], 80);
		writer = native;
		if (c == 1)
			resident[1] = false;
		if (c == 2) {
			descriptors[1].bufferdesc.pcm_state = PCM_STATE_S;
			descriptors[1].bufferdesc.buffer_type = BUF_TYPE_SCUR;
			pg_atomic_fetch_or_u32(&descriptors[1].bufferdesc.state, BM_VALID);
		}
		if (c == 3)
			pg_atomic_fetch_add_u32(&descriptors[1].bufferdesc.state, 1);
		if (c == 5)
			ack_writer_ready = false;
		if (c == 6)
			ack_boot++;
		if (c == 8)
			CurrentResourceOwner = NULL;
		if (c == 9)
			MyBackendType = B_LMON;
		if (c == 10)
			ack_epoch_race = true;
		UT_ASSERT_EQ(cluster_bufmgr_ack_pi_at_data_v1(receipt, plan, sources, 3, NULL, &ack),
					 c < 3);
		UT_ASSERT_EQ(pi_discards, c == 0 || c == 10 ? 1 : 0);
		if (c < 3) {
			UT_ASSERT(cluster_page_data_pi_ack_read_v1(ack, receipt, &node));
			UT_ASSERT_EQ(node, 0);
		} else
			UT_ASSERT(ack == NULL);
		if (c == 10) {
			/* Failure after discard keeps master responsibility; a retry
			 * observes actual absence under the new token. */
			UT_ASSERT(cluster_bufmgr_ack_pi_at_data_v1(receipt, plan, sources, 3, NULL, &ack));
			UT_ASSERT(cluster_page_data_pi_ack_read_v1(ack, receipt, &node));
			UT_ASSERT_EQ(pi_discards, 1);
		}
		cluster_page_data_pi_ack_free_v1(&ack);
		UT_ASSERT(ack == NULL);
		CurrentResourceOwner = (void *)1;
		MyBackendType = B_BG_WRITER;
		cluster_page_data_receipt_free_v1(&receipt);
		rf_page_online_plan_destroy_v1(&plan);
		clean();
	}
}

static void
physical_ack_cannot_change_data_cut_or_owner(void)
{
	ClusterWalSourceRef sources[3];
	ClusterPcmPiStorageCutV1 cut;
	ClusterPageDataReceiptV1 *receipt = NULL;
	ClusterPiPhysicalAckV1 *ack = NULL;
	RfPageOnlinePlanV1 *plan = prepare_storage_observation(sources, &cut);
	ClusterPageDataReceiptV1 original;
	ClusterWalSourceRef native = writer;
	int32 node;
	UT_ASSERT(cluster_bufmgr_observe_pi_storage_v1(&target, &cut, plan, sources, 3, &receipt));
	physical_pi_from_source(&sources[0], 80);
	UT_ASSERT(cluster_bufmgr_ack_pi_at_data_v1(receipt, plan, sources, 3, NULL, &ack));
	original = *receipt;
	for (int c = 0; c < 8; c++) {
		if (c == 0)
			receipt->target.version.mutation_token++;
		if (c == 1)
			receipt->wal.record_crc++;
		if (c == 2)
			receipt->storage_cut.authority.transition_count++;
		if (c == 3)
			CurrentResourceOwner = (void *)2;
		if (c == 4)
			ack_writer_epoch++;
		if (c == 5)
			writer.claim.identity.authority_uuid[0]++;
		if (c == 6)
			ack_boot++;
		if (c == 7)
			cluster_node_id = 1;
		UT_ASSERT(!cluster_page_data_pi_ack_read_v1(ack, receipt, &node));
		UT_ASSERT_EQ(node, -1);
		*receipt = original;
		CurrentResourceOwner = (void *)1;
		ack_writer_epoch = 1;
		writer = native;
		ack_boot = 9;
		cluster_node_id = 0;
		UT_ASSERT(cluster_page_data_pi_ack_read_v1(ack, receipt, &node));
	}
	cluster_page_data_pi_ack_free_v1(&ack);
	cluster_page_data_receipt_free_v1(&receipt);
	rf_page_online_plan_destroy_v1(&plan);
	clean();
}

static void
same_claim_data_under_later_root_ceiling(void)
{
	ClusterWalSourceRef sources[3];
	ClusterPageDataReceiptV1 *receipt = NULL;
	ClusterPcmPiWriteCutV1 cut, proven;
	RfPageContributionPrefixV1 prefixes[3] = { { 0 } };
	RfPageOnlinePlanV1 *plan;
	reset();
	writer.claim.max_config_generation = 3;
	for (unsigned i = 0; i < 3; i++) {
		sources[i] = writer;
		sources[i].claim.identity.origin_thread_id = i + 1;
		sources[i].claim.identity.origin_node_id = i;
		sources[i].claim.claim_sha256[1] = i;
	}
	bind_native_record(RM_XLOG_ID, XLOG_FPI);
	cut = data_pi_cut();
	UT_ASSERT(cluster_bufmgr_write_page_data_at_cut_v1(&target, &cut, &receipt));
	for (unsigned i = 0; i < 3; i++)
		sources[i].claim.max_config_generation++;
	plan = data_contribution_plan(sources);
	UT_ASSERT(cluster_page_data_prefix_v1(
		plan, sources, 3, (const ClusterPageDataReceiptV1 *const *)&receipt, 1, prefixes));
	UT_ASSERT_EQ(prefixes[0].first_uncovered_lsn, 0x200);
	UT_ASSERT_EQ(prefixes[1].first_uncovered_lsn, 0x100);
	UT_ASSERT(cluster_page_data_pi_proof_v1(receipt, plan, sources, 3, &proven));
	/* Caller arrays must still match the sealed selection exactly. This
	 * change never grants authority to an edited source reference. */
	sources[0].claim.max_config_generation++;
	UT_ASSERT(!cluster_page_data_prefix_v1(
		plan, sources, 3, (const ClusterPageDataReceiptV1 *const *)&receipt, 1, prefixes));
	sources[0].claim.max_config_generation--;
	rf_page_online_plan_destroy_v1(&plan);
	for (unsigned i = 0; i < 3; i++)
		sources[i].claim.max_config_generation -= 2;
	plan = data_contribution_plan(sources);
	UT_ASSERT(!cluster_page_data_prefix_v1(
		plan, sources, 3, (const ClusterPageDataReceiptV1 *const *)&receipt, 1, prefixes));
	cluster_page_data_receipt_free_v1(&receipt);
	rf_page_online_plan_destroy_v1(&plan);
	clean();
}

static void
same_claim_pi_under_later_root_ceiling(void)
{
	for (int future = 0; future < 2; future++) {
		ClusterWalSourceRef sources[3], original;
		ClusterPcmPiStorageCutV1 cut;
		ClusterPageDataReceiptV1 *receipt = NULL;
		RfPageOnlinePlanV1 *plan = prepare_storage_observation(sources, &cut);
		rf_page_online_plan_destroy_v1(&plan);
		for (unsigned i = 0; i < 3; i++)
			sources[i].claim.max_config_generation++;
		plan = data_contribution_plan(sources);
		UT_ASSERT(cluster_bufmgr_observe_pi_storage_v1(&target, &cut, plan, sources, 3, &receipt));
		original = sources[0];
		if (future)
			original.claim.max_config_generation++;
		else
			original.claim.max_config_generation--;
		physical_pi_from_source(&original, 80);
		UT_ASSERT_EQ(cluster_bufmgr_discard_pi_at_data_v1(receipt, plan, sources, 3),
					 future ? CLUSTER_PI_PHYSICAL_RETRY : CLUSTER_PI_PHYSICAL_DISCARDED);
		UT_ASSERT_EQ(pi_discards, future ? 0 : 1);
		UT_ASSERT_EQ(writes + wal_flushes, 0);
		cluster_page_data_receipt_free_v1(&receipt);
		rf_page_online_plan_destroy_v1(&plan);
		clean();
	}
}

static void
missing_pi_terminal_receipt_accepts_original_claim_ceiling(void)
{
	for (int replacement = 0; replacement < 3; replacement++) {
		ClusterWalSourceRef sources[3];
		ClusterPcmPiStorageCutV1 storage_cut;
		ClusterPcmPiWriteCutV1 cut, verified;
		ClusterPageDataReceiptV1 *receipt = NULL;
		RfPageOnlinePlanV1 *plan = prepare_storage_observation(sources, &storage_cut);
		rf_page_online_plan_destroy_v1(&plan);
		for (unsigned i = 0; i < 3; i++)
			sources[i].claim.max_config_generation = 3;
		writer = sources[2];
		cluster_node_id = 2;
		resident[1] = true;
		storage_read = false;
		bind_native_record(RM_XLOG_ID, XLOG_FPI);
		cut = data_pi_cut();
		UT_ASSERT(cluster_bufmgr_write_page_data_at_cut_v1(&target, &cut, &receipt));
		for (unsigned i = 0; i < 3; i++)
			sources[i].claim.max_config_generation = 4;
		plan = data_contribution_plan(sources);
		UT_ASSERT(cluster_page_data_pi_proof_v1(receipt, plan, sources, 3, &verified));
		if (replacement == 0) {
			physical_pi_from_source(&sources[0], 80);
			UT_ASSERT_EQ(cluster_bufmgr_discard_pi_at_data_v1(receipt, plan, sources, 3),
						 CLUSTER_PI_PHYSICAL_DISCARDED);
		} else if (replacement == 2) {
			descriptors[1].bufferdesc.pcm_state = PCM_STATE_S;
			descriptors[1].bufferdesc.buffer_type = BUF_TYPE_SCUR;
		}
		/* Lost-ACK retry after actual deletion, or a stable current replacement. */
		UT_ASSERT_EQ(cluster_bufmgr_discard_pi_at_data_v1(receipt, plan, sources, 3),
					 replacement == 0 ? CLUSTER_PI_PHYSICAL_ABSENT : CLUSTER_PI_PHYSICAL_REPLACED);
		cluster_page_data_receipt_free_v1(&receipt);
		rf_page_online_plan_destroy_v1(&plan);
		clean();
	}
}

static void
physical_pi_newer_than_actual_data_is_not_discarded(void)
{
	ClusterWalSourceRef sources[3];
	ClusterPcmPiStorageCutV1 storage_cut;
	ClusterPcmPiWriteCutV1 cut;
	ClusterPageDataReceiptV1 *receipt = NULL;
	RfPageOnlinePlanV1 *plan = prepare_storage_observation(sources, &storage_cut);
	physical_pi_from_source(&sources[0], 80);
	storage_read = false;
	descriptors[1].bufferdesc.pcm_state = PCM_STATE_X;
	descriptors[1].bufferdesc.buffer_type = BUF_TYPE_XCUR;
	pg_atomic_fetch_or_u32(&descriptors[1].bufferdesc.state, BM_VALID | BM_DIRTY);
	target.version.mutation_token = 80;
	cut = data_pi_cut();
	UT_ASSERT(cluster_bufmgr_write_page_data_at_cut_v1(&target, &cut, &receipt));
	physical_pi_from_source(&sources[1], 19);
	UT_ASSERT_EQ(cluster_bufmgr_discard_pi_at_data_v1(receipt, plan, sources, 3),
				 CLUSTER_PI_PHYSICAL_RETRY);
	UT_ASSERT_EQ(pi_discards, 0);
	/* Same old DATA can retire its own exact PI, independent of numeric
	 * token ordering (80 is older than 19 in this sealed chain). */
	physical_pi_from_source(&sources[0], 80);
	UT_ASSERT_EQ(cluster_bufmgr_discard_pi_at_data_v1(receipt, plan, sources, 3),
				 CLUSTER_PI_PHYSICAL_DISCARDED);
	UT_ASSERT_EQ(pi_discards, 1);
	cluster_page_data_receipt_free_v1(&receipt);
	rf_page_online_plan_destroy_v1(&plan);
	clean();
}

static void
missing_pi_cannot_confirm_nonterminal_data(void)
{
	for (int current = 0; current < 3; current++) {
		ClusterWalSourceRef sources[3];
		ClusterPcmPiStorageCutV1 storage_cut;
		ClusterPcmPiWriteCutV1 cut, verified;
		ClusterPageDataReceiptV1 *receipt = NULL;
		RfPageOnlinePlanV1 *plan = prepare_storage_observation(sources, &storage_cut);
		physical_pi_from_source(&sources[0], 80);
		storage_read = false;
		descriptors[1].bufferdesc.pcm_state = PCM_STATE_X;
		descriptors[1].bufferdesc.buffer_type = BUF_TYPE_XCUR;
		pg_atomic_fetch_or_u32(&descriptors[1].bufferdesc.state, BM_VALID | BM_DIRTY);
		target.version.mutation_token = 80;
		cut = data_pi_cut();
		UT_ASSERT(cluster_bufmgr_write_page_data_at_cut_v1(&target, &cut, &receipt));
		UT_ASSERT(cluster_page_data_pi_proof_v1(receipt, plan, sources, 3, &verified));
		/* The sealed chain has B(19), C(7) after this actual A(80) DATA.
		 * Absence or a replacement current buffer cannot erase those PIs. */
		if (current == 0)
			resident[1] = false;
		else if (current == 2) {
			descriptors[1].bufferdesc.pcm_state = PCM_STATE_S;
			descriptors[1].bufferdesc.buffer_type = BUF_TYPE_SCUR;
		}
		UT_ASSERT_EQ(cluster_bufmgr_discard_pi_at_data_v1(receipt, plan, sources, 3),
					 CLUSTER_PI_PHYSICAL_RETRY);
		UT_ASSERT_EQ(pi_discards, 0);
		cluster_page_data_receipt_free_v1(&receipt);
		rf_page_online_plan_destroy_v1(&plan);
		clean();
	}
}

static void
data_sync_releases_content_and_rechecks_clean_replacement(void)
{
	for (int changed = 1; changed <= 2; changed++) {
		ClusterPageDataReceiptV1 *receipt = NULL;
		reset();
		concurrent_sync = changed;
		UT_ASSERT(!cluster_bufmgr_write_page_data_v1(&target, &receipt));
		UT_ASSERT(receipt == NULL);
		UT_ASSERT_EQ(syncs, 1);
		UT_ASSERT_EQ(reads, 1);
		UT_ASSERT_EQ(writes, 1);
		clean();
	}
}

static void
notice_import_requires_actual_fact_and_qualified_master_job(void)
{
	ClusterPageDataReceiptV1 *written = NULL, *received = NULL;
	ClusterPcmPiWriteCutV1 cut;
	ClusterPiDataFactV1 observed;
	reset();
	cut = data_pi_cut();
	UT_ASSERT(cluster_bufmgr_write_tag_data_at_cut_v1(&identity.key, &cut, &written));
	UT_ASSERT(cluster_page_data_pi_fact_v1(written, &notice_fact));
	notice_ready = false;
	UT_ASSERT(!cluster_page_data_from_notice_v1((const void *)1, 0, &received));
	notice_ready = true;
	UT_ASSERT(!cluster_page_data_from_notice_v1((const void *)1, 1, &received));
	UT_ASSERT(cluster_page_data_from_notice_v1((const void *)1, 0, &received));
	UT_ASSERT(cluster_page_data_pi_fact_v1(received, &observed));
	UT_ASSERT_EQ(memcmp(&observed, &notice_fact, sizeof(observed)), 0);
	cluster_page_data_receipt_free_v1(&received);
	notice_fact.binding.flags = 0;
	UT_ASSERT(!cluster_page_data_from_notice_v1((const void *)1, 0, &received));
	UT_ASSERT(received == NULL);
	cluster_page_data_receipt_free_v1(&written);
	clean();
}

static void
remote_ack_import_preserves_peer_and_resource_owner(void)
{
	ClusterPageDataReceiptV1 *written = NULL;
	ClusterPiPhysicalAckV1 *ack = NULL;
	ClusterPcmPiWriteCutV1 cut;
	int32 node;
	reset();
	cut = data_pi_cut();
	UT_ASSERT(cluster_bufmgr_write_tag_data_at_cut_v1(&identity.key, &cut, &written));
	UT_ASSERT_EQ(cluster_wal_writer_begin(writer.timeline, &remote_ack_writer), 0);
	/* The actual transport peer/notice boundary is explicit in this buffer
	 * fixture. Its real protocol owner is exercised by writeback tests. */
	cluster_node_id = 1;
	writer.claim.identity.origin_node_id = 1;
	writer.claim.identity.origin_thread_id = 2;
	writer.claim.identity.origin_owner_incarnation = ack_boot = 31;
	remote_ack_ready = false;
	remote_ack_current = true;
	UT_ASSERT(!cluster_page_data_pi_ack_import_v1((const void *)1, 0, written, &ack));
	remote_ack_ready = true;
	UT_ASSERT(cluster_page_data_pi_ack_import_v1((const void *)1, 0, written, &ack));
	UT_ASSERT(cluster_page_data_pi_ack_read_v1(ack, written, &node));
	UT_ASSERT_EQ(node, 0);
	CurrentResourceOwner = (void *)2;
	UT_ASSERT(!cluster_page_data_pi_ack_read_v1(ack, written, &node));
	CurrentResourceOwner = (void *)1;
	remote_ack_current = false;
	UT_ASSERT(!cluster_page_data_pi_ack_read_v1(ack, written, &node));
	remote_ack_current = true;
	ack_writer_epoch++;
	UT_ASSERT(!cluster_page_data_pi_ack_read_v1(ack, written, &node));
	cluster_page_data_pi_ack_free_v1(&ack);
	cluster_page_data_receipt_free_v1(&written);
	clean();
}

static unsigned redeclare_count;
static bool redeclare_ack;
static uint8 redeclare_mode;
static bool
redeclare_observe(BufferTag tag, uint8 mode, XLogRecPtr lsn, SCN scn, void *arg)
{
	UT_ASSERT(BufferTagsEqual(&tag, &descriptors[1].bufferdesc.tag));
	redeclare_count++;
	redeclare_mode = mode;
	return redeclare_ack;
}

static void
redeclare_scan_includes_physical_pi_and_preserves_unacknowledged_cursor(void)
{
	BufferDesc *buf;
	reset();
	redeclare_scan_test = true;
	buf = &descriptors[1].bufferdesc;
	buf->buffer_type = BUF_TYPE_PI;
	buf->pcm_state = PCM_STATE_N;
	pg_atomic_write_u32(&buf->state, BM_TAG_VALID | BM_PERMANENT);
	redeclare_count = 0;
	redeclare_ack = false;
	UT_ASSERT_EQ(cluster_bufmgr_redeclare_scan_chunk(1, 1, redeclare_observe, NULL), -2);
	UT_ASSERT_EQ(redeclare_count, 1);
	UT_ASSERT_EQ(redeclare_mode, PCM_STATE_N);
	redeclare_ack = true;
	UT_ASSERT_EQ(cluster_bufmgr_redeclare_scan_chunk(1, 1, redeclare_observe, NULL), 2);
	UT_ASSERT_EQ(redeclare_count, 2);
	UT_ASSERT_EQ(buf->buffer_type, BUF_TYPE_PI);
	UT_ASSERT_EQ(writes + wal_flushes, 0);
	UT_ASSERT_EQ(pins[1], 0);
	redeclare_scan_test = false;
	clean();
}

static void
redeclare_scan_retries_content_busy_and_zero_lsn_current(void)
{
	reset();
	redeclare_scan_test = true;
	redeclare_ack = true;
	redeclare_count = 0;
	busy[1] = true;
	UT_ASSERT_EQ(cluster_bufmgr_redeclare_scan_chunk(1, 1, redeclare_observe, NULL), -2);
	UT_ASSERT_EQ(redeclare_count, 0);
	busy[1] = false;
	PageSetLSNPreserveOrigin(pages[1].data, 0);
	UT_ASSERT_EQ(cluster_bufmgr_redeclare_scan_chunk(1, 1, redeclare_observe, NULL), 2);
	UT_ASSERT_EQ(redeclare_count, 1);
	UT_ASSERT_EQ(redeclare_mode, PCM_STATE_X);
	UT_ASSERT_EQ(pins[1], 0);
	redeclare_scan_test = false;
	clean();
}

static void
redeclare_scan_filters_scope_before_content_or_io_wait(void)
{
	for (int scope = -1; scope <= 1; scope++) {
		reset();
		redeclare_scan_test = true;
		redeclare_scope = scope;
		redeclare_count = 0;
		busy[1] = true;
		UT_ASSERT_EQ(cluster_bufmgr_redeclare_scan_chunk(1, 1, redeclare_observe, NULL),
					 scope == 0 ? 2 : -2);
		UT_ASSERT_EQ(redeclare_content_attempts, scope == 1 ? 1 : 0);
		UT_ASSERT_EQ(redeclare_count, 0);
		UT_ASSERT_EQ(pins[1], 0);
		pg_atomic_fetch_or_u32(&descriptors[1].bufferdesc.state, BM_IO_IN_PROGRESS);
		UT_ASSERT_EQ(cluster_bufmgr_redeclare_scan_chunk(1, 1, redeclare_observe, NULL),
					 scope == 0 ? 2 : -2);
		UT_ASSERT_EQ(redeclare_count, 0);
		UT_ASSERT_EQ(pins[1], 0);
		UT_ASSERT(pg_atomic_read_u32(&descriptors[1].bufferdesc.state) & BM_IO_IN_PROGRESS);
		pg_atomic_fetch_and_u32(&descriptors[1].bufferdesc.state, ~BM_IO_IN_PROGRESS);
		redeclare_scan_test = false;
		clean();
	}
}

static ClusterPcmLocalPiSnapshotV1
logical_anchors(const ClusterWalSourceRef sources[3], BufferTag tag)
{
	ClusterPcmLocalPiSnapshotV1 local = { 0 };
	local.resource = tag;
	local.binding_generation = 11;
	local.revision = 12;
	local.first.source = sources[0];
	local.first.identity = target.identity;
	local.first.version = target.version;
	local.first.version.mutation_token = 80;
	local.first.record_start = 0x100;
	local.first.record_end = 0x200;
	local.first.record_crc = 0x9192;
	local.first.rmid = RM_XLOG_ID;
	local.first.info = XLOG_FPI;
	local.first.flags = CLUSTER_PAGE_WAL_NATIVE_FLUSHED;
	local.last = local.first;
	local.last.source = sources[1];
	local.last.version.mutation_token = 19;
	return local;
}

static void
data_requires_both_detached_logical_anchors_in_its_ancestry(void)
{
	for (unsigned fault = 0; fault < 7; fault++) {
		ClusterWalSourceRef sources[3];
		ClusterPcmPiStorageCutV1 cut;
		ClusterPageDataReceiptV1 *receipt = NULL;
		RfPageOnlinePlanV1 *plan = prepare_storage_observation(sources, &cut);
		ClusterPcmLocalPiSnapshotV1 local = logical_anchors(sources, cut.resource);
		UT_ASSERT(cluster_bufmgr_observe_pi_storage_v1(&target, &cut, plan, sources, 3, &receipt));
		if (fault == 1)
			local.first.record_crc++;
		if (fault == 2)
			local.last.source.claim.identity.origin_owner_incarnation++;
		if (fault == 3)
			local.last.version.mutation_token = 6;
		if (fault == 4)
			local.first.version.segment_incarnation[1]++;
		if (fault == 5)
			local.resource.blockNum++;
		if (fault == 6)
			memset(&local.first, 0, sizeof(local.first));
		UT_ASSERT_EQ(cluster_page_data_covers_local_pi_v1(receipt, plan, sources, 3, &local),
					 fault == 0);
		UT_ASSERT_EQ(pi_discards + writes, 0);
		cluster_page_data_receipt_free_v1(&receipt);
		rf_page_online_plan_destroy_v1(&plan);
		clean();
	}
}

static void
physical_ack_rechecks_detached_responsibility_after_consumption(void)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		ClusterWalSourceRef sources[3];
		ClusterPcmPiStorageCutV1 cut;
		ClusterPageDataReceiptV1 *receipt = NULL;
		ClusterPiPhysicalAckV1 *ack = NULL;
		RfPageOnlinePlanV1 *plan = prepare_storage_observation(sources, &cut);
		UT_ASSERT(cluster_bufmgr_observe_pi_storage_v1(&target, &cut, plan, sources, 3, &receipt));
		logical_pi = logical_anchors(sources, cut.resource);
		logical_pi_raced = fault == 1;
		logical_pi_retire_calls = 0;
		if (fault == 2)
			logical_pi.first.record_crc++;
		UT_ASSERT_EQ(cluster_bufmgr_ack_pi_at_data_v1(receipt, plan, sources, 3, NULL, &ack),
					 fault == 0);
		UT_ASSERT_EQ(logical_pi_retire_calls, fault == 2 ? 0 : 1);
		UT_ASSERT_EQ(pi_discards + writes, 0); /* actual ABSENT physical result */
		cluster_page_data_pi_ack_free_v1(&ack);
		cluster_page_data_receipt_free_v1(&receipt);
		rf_page_online_plan_destroy_v1(&plan);
		memset(&logical_pi, 0, sizeof(logical_pi));
		logical_pi_raced = false;
		clean();
	}
}

#ifndef PGRAC_TEST_REAL_PI_WRITEBACK
static void
recovered_fixture(const ClusterWalSourceRef sources[3])
{
	memset(recovered_inputs, 0, sizeof(recovered_inputs));
	for (uint32 i = 0; i < 3; i++)
		recovered_inputs[i].source = sources[i];
	recovered_ready = recovered_pinned = true;
	recovered_state = CLUSTER_MEMBER_DEAD;
	recovered_boot = sources[1].claim.identity.origin_owner_incarnation;
	recovered_membership = 2;
}

static void
recovery_ack_needs_full_input_terminal_data_and_exact_dead_boot(void)
{
	for (int fault = 0; fault < 11; fault++) {
		ClusterWalSourceRef sources[3];
		ClusterPcmPiStorageCutV1 cut;
		ClusterPageDataReceiptV1 *receipt = NULL;
		ClusterPiPhysicalAckV1 *ack = NULL;
		ClusterWalWriterToken exported;
		int32 node = -1;
		RfPageOnlinePlanV1 *plan = prepare_storage_observation(sources, &cut);
		UT_ASSERT(cluster_bufmgr_observe_pi_storage_v1(&target, &cut, plan, sources, 3, &receipt));
		recovered_fixture(sources);
		if (fault == 1)
			recovered_ready = false;
		if (fault == 2)
			recovered_pinned = false;
		if (fault == 3)
			recovered_inputs[2].source = recovered_inputs[0].source;
		if (fault == 4)
			recovered_inputs[0].source.claim.claim_sha256[0] ^= 1;
		if (fault == 5)
			recovered_state = CLUSTER_MEMBER_MEMBER;
		if (fault == 6)
			recovered_boot++;
		if (fault == 7)
			recovered_membership = 0;
		if (fault == 8)
			recovered_membership = 3;
		if (fault == 9)
			receipt->storage_cut.master_node = 2;
		if (fault == 10)
			receipt->storage_cut.pi_holders_bitmap &= ~2u;
		UT_ASSERT_EQ(cluster_bufmgr_ack_recovered_pi_at_data_v1(
			receipt, plan, sources, 3, (void *)1, 1, &ack), fault == 0);
		UT_ASSERT_EQ(pi_discards + writes, 0);
		if (fault == 0) {
			UT_ASSERT(cluster_page_data_pi_ack_read_v1(ack, receipt, &node));
			UT_ASSERT_EQ(node, 1);
			UT_ASSERT(!cluster_page_data_pi_ack_export_v1(ack, receipt, &exported));
		}
		cluster_page_data_pi_ack_free_v1(&ack);
		cluster_page_data_receipt_free_v1(&receipt);
		rf_page_online_plan_destroy_v1(&plan);
		clean();
	}
}

static void
recovery_ack_cannot_outlive_original_input_membership_or_owner(void)
{
	for (int fault = 0; fault < 9; fault++) {
		ClusterWalSourceRef sources[3];
		ClusterPcmPiStorageCutV1 cut;
		ClusterPageDataReceiptV1 *receipt = NULL;
		ClusterPiPhysicalAckV1 *ack = NULL;
		ResourceOwner owner;
		int32 node = -1;
		RfPageOnlinePlanV1 *plan = prepare_storage_observation(sources, &cut);
		UT_ASSERT(cluster_bufmgr_observe_pi_storage_v1(&target, &cut, plan, sources, 3, &receipt));
		recovered_fixture(sources);
		UT_ASSERT(cluster_bufmgr_ack_recovered_pi_at_data_v1(
			receipt, plan, sources, 3, (void *)1, 1, &ack));
		owner = CurrentResourceOwner;
		if (fault == 1)
			recovered_pinned = false;
		if (fault == 2)
			recovered_ready = false;
		if (fault == 3)
			recovered_membership += 2;
		if (fault == 4)
			recovered_state = CLUSTER_MEMBER_JOINING;
		if (fault == 5)
			recovered_boot++;
		if (fault == 6)
			CurrentResourceOwner = (void *)9;
		if (fault == 7)
			receipt->storage_cut.authority.transition_count++;
		if (fault == 8)
			recovered_inputs[1].source.claim.identity.root_lineage_seq++;
		UT_ASSERT_EQ(cluster_page_data_pi_ack_read_v1(ack, receipt, &node), fault == 0);
		UT_ASSERT_EQ(node, fault == 0 ? 1 : -1);
		CurrentResourceOwner = owner;
		cluster_page_data_pi_ack_free_v1(&ack);
		cluster_page_data_receipt_free_v1(&receipt);
		rf_page_online_plan_destroy_v1(&plan);
		clean();
	}
}

static void
recovery_ack_refuses_successor_pi_not_covered_by_actual_data(void)
{
	ClusterWalSourceRef sources[3];
	ClusterPcmPiStorageCutV1 storage_cut;
	ClusterPcmPiWriteCutV1 cut, verified;
	ClusterPageDataReceiptV1 *receipt = NULL;
	ClusterPiPhysicalAckV1 *ack = NULL;
	RfPageOnlinePlanV1 *plan = prepare_storage_observation(sources, &storage_cut);
	physical_pi_from_source(&sources[0], 80);
	storage_read = false;
	descriptors[1].bufferdesc.pcm_state = PCM_STATE_X;
	descriptors[1].bufferdesc.buffer_type = BUF_TYPE_XCUR;
	pg_atomic_fetch_or_u32(&descriptors[1].bufferdesc.state, BM_VALID | BM_DIRTY);
	target.version.mutation_token = 80;
	cut = data_pi_cut();
	UT_ASSERT(cluster_bufmgr_write_page_data_at_cut_v1(&target, &cut, &receipt));
	UT_ASSERT(cluster_page_data_pi_proof_v1(receipt, plan, sources, 3, &verified));
	recovered_fixture(sources);
	UT_ASSERT(
		!cluster_bufmgr_ack_recovered_pi_at_data_v1(receipt, plan, sources, 3, (void *)1, 1, &ack));
	UT_ASSERT(ack == NULL);
	UT_ASSERT_EQ(pi_discards, 0);
	cluster_page_data_receipt_free_v1(&receipt);
	rf_page_online_plan_destroy_v1(&plan);
	clean();
}
#endif

/* The original native SPACE insertion is covered by test_cluster_page_wal.
 * Here the existing image-install boundary supplies its complete source;
 * typed decoding, live holder checks and physical I/O stay real. */
static void
install_space_source(const ClusterPageWalBindingV1 *binding)
{
	ClusterPageWalInstallV1 prepared = { 0 };
	source_capture = locks[data_slot] = true;
	HOLD_INTERRUPTS();
	UT_ASSERT(cluster_page_wal_prepare_install_v1(data_slot + 1, binding, pages[data_slot].data,
												  &prepared));
	UT_ASSERT(cluster_page_wal_publish_install_v1(data_slot + 1, &prepared));
	cluster_page_wal_release_install_v1(&prepared);
	RESUME_INTERRUPTS();
	source_capture = locks[data_slot] = false;
}

static void
encode_space_target(const ClusterSpaceIdentity *value)
{
	if (data_slot == 0)
		UT_ASSERT(cluster_space_identity_page_encode(value, 80, pages[0].data, BLCKSZ));
	else {
		ClusterSpaceReservation reservation = { 0 };
		reservation.identity = *value;
		reservation.next_block = 17;
		UT_ASSERT(cluster_space_reservation_page_encode(&reservation, 80, pages[1].data, BLCKSZ));
	}
	PageSetLSNPreserveOrigin(pages[data_slot].data, 0x200);
	UT_ASSERT(PageSetLSNOrigin(pages[data_slot].data, 0));
}

static void
space_fixture(int block, bool tombstone)
{
	ClusterPageWalBindingV1 binding;
	reset();
	binding = read_page_source();
	data_slot = block;
	target.identity.forknum = SPACE_FORKNUM;
	target.identity.blockno = block;
	identity.state = tombstone ? CLUSTER_SPACE_IDENTITY_TOMBSTONED : CLUSTER_SPACE_IDENTITY_LIVE;
	identity.sequence = 2;
	identity.operation = 3;
	UT_ASSERT(cluster_space_identity_page_encode(&identity, 10, pages[0].data, BLCKSZ));
	encode_space_target(&identity);
	InitBufferTag(&descriptors[block].bufferdesc.tag, &identity.key.locator, SPACE_FORKNUM, block);
	descriptors[block].bufferdesc.pcm_state = PCM_STATE_X;
	descriptors[block].bufferdesc.buffer_type = BUF_TYPE_XCUR;
	pg_atomic_fetch_or_u32(&descriptors[block].bufferdesc.state, BM_DIRTY);
	binding.identity = target.identity;
	binding.rmid = tombstone ? RM_XACT_ID : RM_SMGR_ID;
	binding.info = tombstone ? XLOG_XACT_COMMIT : XLOG_SMGR_SPACE_IDENTITY;
	install_space_source(&binding);
}

static void
space_data_writes_exact_typed_live_and_tombstoned_pages(void)
{
	for (int block = 0; block < 2; block++) {
		for (int tombstone = 0; tombstone < 2; tombstone++) {
			ClusterPageDataReceiptV1 *receipt = NULL;
			ClusterPageDataTargetV1 observed;
			PGAlignedBlock disk;
			space_fixture(block, tombstone);
			UT_ASSERT(cluster_bufmgr_write_page_data_v1(&target, &receipt));
			UT_ASSERT_EQ(writes, 1);
			UT_ASSERT_EQ(wal_flushes, 1);
			UT_ASSERT_EQ(syncs, 1);
			UT_ASSERT_EQ(reads, 1);
			clean();
			if (receipt == NULL)
				continue;
			UT_ASSERT(cluster_page_data_receipt_read_v1(receipt, &observed));
			UT_ASSERT_EQ(observed.identity.forknum, SPACE_FORKNUM);
			UT_ASSERT_EQ(observed.identity.blockno, block);
			UT_ASSERT_EQ(observed.version.mutation_token, 80);
			UT_ASSERT_EQ(pread(fileno(file), disk.data, BLCKSZ, 0), BLCKSZ);
			UT_ASSERT_EQ(((PageHeader)disk.data)->pd_checksum, pg_checksum_page(disk.data, block));
			((PageHeader)disk.data)->pd_checksum = 0;
			UT_ASSERT_EQ(memcmp(disk.data, pages[block].data, BLCKSZ), 0);
			cluster_page_data_receipt_free_v1(&receipt);
		}
	}
}

static void
space_binding_rejects_wrong_typed_identity_before_io(void)
{
	for (int block = 0; block < 2; block++) {
		for (int mismatch = 0; mismatch < 8; mismatch++) {
			ClusterPageDataReceiptV1 *receipt = NULL;
			ClusterPageWalBindingV1 binding = { 0 };
			ClusterSpaceIdentity wrong;
			space_fixture(block, false);
			wrong = identity;
			switch (mismatch) {
			case 0:
				wrong.key.database_incarnation++;
				break;
			case 1:
				wrong.incarnation[1]++;
				break;
			case 2:
				wrong.sequence++;
				break;
			case 3:
				wrong.operation++;
				break;
			case 4:
				wrong.state = CLUSTER_SPACE_IDENTITY_TOMBSTONED;
				break;
			case 5:
				wrong.key.locator.relNumber++;
				break;
			case 6:
				wrong.key.storage_uuid[1]++;
				break;
			}
			encode_space_target(&wrong);
			if (mismatch == 7)
				((PageHeader)pages[block].data)->pd_flags &= ~PD_SPACE_METADATA;
			locks[block] = true;
			UT_ASSERT(!cluster_page_wal_read_v1(block + 1, &identity, &binding));
			locks[block] = false;
			/* Block zero is its own live identity authority. For sequence,
			 * operation and state changes the old external identity rejects
			 * above; DATA may independently sample the new valid identity. */
			if (block == 1 || mismatch < 2 || mismatch > 4) {
				UT_ASSERT(!cluster_bufmgr_write_page_data_v1(&target, &receipt));
				UT_ASSERT(receipt == NULL);
				UT_ASSERT_EQ(writes + syncs + reads + wal_flushes, 0);
			}
			clean();
		}
	}
}

static void
space_data_requires_current_x_and_unchanged_completion(void)
{
	for (int block = 0; block < 2; block++) {
		for (int failure = 0; failure < 8; failure++) {
			ClusterPageDataReceiptV1 *receipt = NULL;
			space_fixture(block, false);
			if (failure == 0)
				descriptors[block].bufferdesc.pcm_state = PCM_STATE_S;
			if (failure == 1)
				bad_checksum = true;
			if (failure == 2)
				bad_bytes = true;
			if (failure == 3)
				stale_sync = true;
			if (failure == 4)
				late_fence = true;
			if (failure == 5)
				redirty_sync = true;
			if (failure == 6)
				storage_space_changed = true;
			if (failure == 7)
				zero_disk = true;
			UT_ASSERT(!cluster_bufmgr_write_page_data_v1(&target, &receipt));
			UT_ASSERT(receipt == NULL);
			UT_ASSERT_EQ(writes, failure == 0 ? 0 : 1);
			clean();
		}
	}
}

static void
space_data_io_errors_release_single_identity_owner(void)
{
	for (int block = 0; block < 2; block++) {
		for (int at = 1; at <= 4; at++) {
			ClusterPageDataReceiptV1 *receipt = NULL;
			volatile bool caught = false;
			space_fixture(block, false);
			throw_at = at;
			PG_TRY();
			{
				(void)cluster_bufmgr_write_page_data_v1(&target, &receipt);
			}
			PG_CATCH();
			{
				caught = true;
			}
			PG_END_TRY();
			UT_ASSERT(caught);
			UT_ASSERT(receipt == NULL);
			UT_ASSERT_EQ(aborts, at <= 2 ? 1 : 0);
			clean();
		}
	}
}

static void
space_foreign_data_uses_only_original_native_flush(void)
{
	for (int block = 0; block < 2; block++) {
		ClusterPageDataReceiptV1 *receipt = NULL;
		ClusterPageWalBindingV1 original, certified;
		space_fixture(block, false);
		original = read_page_source();
		UT_ASSERT(cluster_page_wal_flush_source_v1(&original, &certified));
		writer.claim.identity.origin_thread_id = 2;
		writer.claim.identity.origin_node_id = cluster_node_id = 1;
		writer.claim.identity.origin_owner_incarnation++;
		writer.claim.claim_sha256[0]++;
		local_insert_end = 0x100;
		UT_ASSERT(!cluster_bufmgr_write_page_data_v1(&target, &receipt));
		UT_ASSERT_EQ(writes + syncs + reads, 0);
		install_space_source(&certified);
		UT_ASSERT(cluster_bufmgr_write_page_data_v1(&target, &receipt));
		UT_ASSERT_EQ(wal_flushes, 1);
		UT_ASSERT_EQ(writes, 1);
		UT_ASSERT_EQ(syncs, 1);
		UT_ASSERT_EQ(reads, 1);
		cluster_page_data_receipt_free_v1(&receipt);
		clean();
	}
}

static void
space_tag_receipt_exports_only_after_real_data_completion(void)
{
	for (int block = 0; block < 2; block++) {
		ClusterPageDataReceiptV1 *receipt = NULL;
		ClusterPcmPiWriteCutV1 cut, exported;
		ClusterPageWalBindingV1 binding;
		ClusterWalSourceRef source;
		RfPageOnlinePlanV1 *ordinary_plan;
		reset();
		source = writer;
		ordinary_plan = data_contribution_plan_count(&source, 1);
		space_fixture(block, true);
		cut = data_pi_cut();
		UT_ASSERT(cluster_bufmgr_write_tag_data_at_cut_v1(&identity.key, &cut, &receipt));
		UT_ASSERT_EQ(writes, 1);
		UT_ASSERT_EQ(syncs, 1);
		UT_ASSERT_EQ(reads, 1);
		UT_ASSERT(cluster_page_data_pi_export_v1(receipt, &binding, &exported));
		if (receipt != NULL) {
			UT_ASSERT_EQ(binding.flags, CLUSTER_PAGE_WAL_NATIVE_FLUSHED);
			UT_ASSERT_EQ(binding.identity.forknum, SPACE_FORKNUM);
			UT_ASSERT_EQ(binding.identity.blockno, block);
			UT_ASSERT_EQ(memcmp(&exported, &cut, sizeof(cut)), 0);
		}
		UT_ASSERT(!cluster_page_data_pi_proof_v1(receipt, ordinary_plan, &source, 1, &exported));
		cluster_page_data_receipt_free_v1(&receipt);
		rf_page_online_plan_destroy_v1(&ordinary_plan);
		cut.holder.assertion.resource.blockNum = 2;
		UT_ASSERT(!cluster_bufmgr_write_tag_data_at_cut_v1(&identity.key, &cut, &receipt));
		UT_ASSERT(receipt == NULL);
		UT_ASSERT_EQ(writes, 1);
		clean();
	}
}

int
main(void)
{
#ifndef PGRAC_TEST_REAL_PI_WRITEBACK
	UT_PLAN(46);
	UT_RUN(recovery_ack_needs_full_input_terminal_data_and_exact_dead_boot);
	UT_RUN(recovery_ack_cannot_outlive_original_input_membership_or_owner);
	UT_RUN(recovery_ack_refuses_successor_pi_not_covered_by_actual_data);
#else
	UT_PLAN(43);
#endif
	UT_RUN(space_data_writes_exact_typed_live_and_tombstoned_pages);
	UT_RUN(space_binding_rejects_wrong_typed_identity_before_io);
	UT_RUN(space_data_requires_current_x_and_unchanged_completion);
	UT_RUN(space_data_io_errors_release_single_identity_owner);
	UT_RUN(space_foreign_data_uses_only_original_native_flush);
	UT_RUN(space_tag_receipt_exports_only_after_real_data_completion);
	UT_RUN(missing_pi_terminal_receipt_accepts_original_claim_ceiling);
	UT_RUN(data_sync_releases_content_and_rechecks_clean_replacement);
	UT_RUN(redeclare_scan_includes_physical_pi_and_preserves_unacknowledged_cursor);
	UT_RUN(redeclare_scan_retries_content_busy_and_zero_lsn_current);
	UT_RUN(redeclare_scan_filters_scope_before_content_or_io_wait);
	UT_RUN(notice_import_requires_actual_fact_and_qualified_master_job);
	UT_RUN(remote_ack_import_preserves_peer_and_resource_owner);
	UT_RUN(tag_write_io_failure_never_exports_completion);
	UT_RUN(remote_import_requires_original_job);
	UT_RUN(tag_write_samples_real_identity_and_version);
	UT_RUN(tag_write_rejects_wrong_namespace_or_holder);
	UT_RUN(physical_ack_consumes_original_prior_exit_and_full_retained_scope);
	UT_RUN(physical_ack_requires_actual_consumption_and_original_boot);
	UT_RUN(physical_ack_cannot_change_data_cut_or_owner);
	UT_RUN(same_claim_data_under_later_root_ceiling);
	UT_RUN(same_claim_pi_under_later_root_ceiling);
	UT_RUN(success_and_old_completion);
	UT_RUN(identity_refusals);
	UT_RUN(authority_refusals);
	UT_RUN(explicit_wal_origin);
	UT_RUN(io_failure_cleanup);
	UT_RUN(completion_refusals);
	UT_RUN(vm_and_clean_completion);
	UT_RUN(new_claim_never_flushes_old_coordinate);
	UT_RUN(foreign_certified_image_uses_original_wal);
	UT_RUN(ordinary_flush_uses_original_source);
	UT_RUN(plan_sources_are_immutable);
	UT_RUN(pi_cut_must_match_current_holder_before_data);
	UT_RUN(physical_receipts_close_only_exact_contributions);
	UT_RUN(n_s_storage_observation_qualifies_only_terminal_data);
	UT_RUN(storage_observation_rejects_incomplete_or_changed_proof);
	UT_RUN(storage_observation_releases_space_on_io_error);
	UT_RUN(physical_pi_discard_is_ancestry_and_generation_exact);
	UT_RUN(physical_pi_newer_than_actual_data_is_not_discarded);
	UT_RUN(missing_pi_cannot_confirm_nonterminal_data);
	UT_RUN(data_requires_both_detached_logical_anchors_in_its_ancestry);
	UT_RUN(physical_ack_rechecks_detached_responsibility_after_consumption);
	if (file)
		fclose(file);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
