/*-------------------------------------------------------------------------
 * test_cluster_ko_source_io.c
 *    Actual N/S source preparation versus the original DROP marker.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * KO lifecycle, source decisions, copy and reservation transitions are real.
 * GES/READY prerequisites and descriptor/storage primitives are fixtures;
 * this is not a cross-node READY or durability proof.
 *-------------------------------------------------------------------------
 */
int ko_stop_fixture_main(void);
#define main ko_stop_fixture_main
#include "test_cluster_ko_stop.c"
#undef main
#include "cluster/cluster_pcm_own.h"
#include "cluster/cluster_pcm_x_bufmgr.h"
#include "cluster/cluster_gcs_block.h"

int NBuffers = 1;
static ClusterPcmOwnEntry source_entry;
static BufferDesc source_buf;
static PGIOAlignedBlock io_resident_page, storage_page;
static LWLock source_lock;
static ClusterKoSharedMessageV3 source_request;
static unsigned source_opens, source_reads, source_flushes, source_wal_flushes;
static unsigned marker_attempts, marker_accepted;
static bool source_locked, source_lock_busy;
static int source_pins, source_fault;

static bool
source_commit(void)
{
	return cluster_ko_peer_commit_v3(source_request.base.origin_node,
									 source_request.base.epoch, source_request.base.origin_boot,
									 source_request.base.peer_boot, source_request.base.batch_id);
}

static void
source_marker_probe(void)
{
	UT_ASSERT(SpinLockFree(&storage.shared_lock));
	marker_attempts++;
	if (source_commit())
		marker_accepted++;
}

static uint32
source_header(BufferDesc *buf)
{
	uint32 state = pg_atomic_read_u32(&buf->state);
	UT_ASSERT_EQ(buf, &source_buf);
	UT_ASSERT((state & BM_LOCKED) == 0);
	pg_atomic_write_u32(&buf->state, state | BM_LOCKED);
	return state | BM_LOCKED;
}

static bool
source_acquire(LWLock *lock, LWLockMode mode)
{
	UT_ASSERT(lock == &source_lock && !source_locked);
	UT_ASSERT(mode == LW_SHARED || mode == LW_EXCLUSIVE);
	if (source_lock_busy)
		return false;
	source_locked = true;
	source_marker_probe();
	return true;
}

static void
source_release(LWLock *lock)
{
	UT_ASSERT(lock == &source_lock && source_locked);
	source_marker_probe();
	source_locked = false;
}

static void
source_pin(BufferDesc *buf)
{
	uint32 state = pg_atomic_read_u32(&buf->state);
	UT_ASSERT((state & BM_LOCKED) != 0);
	source_pins++;
	UnlockBufHdr(buf, state + BUF_REFCOUNT_ONE);
}

static void
source_unpin(BufferDesc *buf)
{
	UT_ASSERT(!source_locked && source_pins == 1);
	source_marker_probe();
	source_pins--;
	pg_atomic_fetch_sub_u32(&buf->state, BUF_REFCOUNT_ONE);
}

static SMgrRelation
source_open(RelFileLocator locator, BackendId backend)
{
	UT_ASSERT(RelFileLocatorEquals(locator, source_request.base.key.locator));
	UT_ASSERT_EQ(backend, InvalidBackendId);
	source_opens++;
	source_marker_probe();
	if (source_fault == 1)
		pg_re_throw();
	return (SMgrRelation)1;
}

static void
source_read(SMgrRelation rel, ForkNumber fork, BlockNumber block, void *dst)
{
	UT_ASSERT(rel == (SMgrRelation)1 && fork == MAIN_FORKNUM && block == 4);
	UT_ASSERT(!source_locked);
	source_reads++;
	source_marker_probe();
	if (source_fault == 2)
		pg_re_throw();
	memcpy(dst, storage_page.data, BLCKSZ);
}

static void
source_flush(BufferDesc *buf)
{
	UT_ASSERT(source_locked && source_pins == 1 && buf == &source_buf);
	source_flushes++;
	source_marker_probe();
	if (source_fault == 4)
		pg_re_throw();
	pg_atomic_fetch_and_u32(&buf->state, ~(BM_DIRTY | BM_JUST_DIRTIED | BM_CHECKPOINT_NEEDED));
}

bool
cluster_page_wal_snapshot_v1(Buffer buffer, ClusterPageWalBindingV1 *out)
{
	UT_ASSERT(source_locked && buffer == BufferDescriptorGetBuffer(&source_buf));
	memset(out, 0, sizeof(*out));
	out->record_start = 0x120;
	return true;
}

bool
cluster_page_wal_flush_source_v1(const ClusterPageWalBindingV1 *in,
								 ClusterPageWalBindingV1 *out)
{
	UT_ASSERT(!source_locked && in->record_start == 0x120);
	source_wal_flushes++;
	source_marker_probe();
	if (source_fault == 5)
		pg_re_throw();
	*out = *in;
	return source_fault != 6;
}

#define LockBufHdr source_header
#define BufferDescriptorGetContentLock(buf) (&source_lock)
#define LWLockConditionalAcquire source_acquire
#define LWLockRelease source_release
#define LWLockHeldByMe(lock) source_locked
#define LWLockHeldByMeInMode(lock, mode) source_locked
#undef BufHdrGetBlock
#define BufHdrGetBlock(buf) ((Block)io_resident_page.data)
#define ReservePrivateRefCountEntry() ((void)0)
#define ResourceOwnerEnlargeBuffers(owner) ((void)0)
#define PinBuffer_Locked source_pin
#define UnpinBuffer source_unpin
#define FlushBuffer(buf, rel, obj, context) source_flush(buf)
#define smgropen source_open
#define smgrread source_read
#define PageIsVerifiedForFork(page, fork, block, flags) (source_fault != 3)
#define cluster_bufmgr_pcm_own_observe_s_source_hard_failure(reason, cause, buf, live, required, local, disk, state, type, result, aborted) \
	((void)(reason), (void)(cause), (void)(buf), (void)(live), (void)(required), \
	 (void)(local), (void)(disk), (void)(state), (void)(type), (void)(result), (void)(aborted))
#include "test_cluster_ko_source_io.inc"
#undef cluster_bufmgr_pcm_own_observe_s_source_hard_failure

static ClusterPcmOwnSnapshot
source_fixture(bool s, unsigned n)
{
	ClusterKoPeerFenceHandleV3 handle;
	ClusterKoPeerFenceStateV3 state;
	ClusterPcmOwnSnapshot before;
	source_request = peer_fence_fixture(30000 + n);
	local_scan_rel_number = source_request.base.key.locator.relNumber;
	UT_ASSERT(cluster_ko_peer_fence_begin_v3(&source_request, &handle, &state));
	UT_ASSERT(fixture_completed_fence_ready(&source_request, &handle));
	fixture_peer_reply_accepted(&storage.peer_fences[handle.slot]);
	memset(&source_buf, 0, sizeof(source_buf));
	memset(&source_entry, 0, sizeof(source_entry));
	ClusterPcmOwnArray = &source_entry;
	InitBufferTag(&source_buf.tag, &source_request.base.key.locator, MAIN_FORKNUM, 4);
	source_buf.pcm_state = s ? PCM_STATE_S : PCM_STATE_N;
	source_buf.buffer_type = s ? BUF_TYPE_SCUR : BUF_TYPE_CURRENT;
	pg_atomic_init_u32(&source_buf.state, BM_TAG_VALID | BM_VALID);
	pg_atomic_init_u64(&source_entry.generation, 2);
	pg_atomic_init_u64(&source_entry.reservation_token, 3);
	pg_atomic_init_u64(&source_entry.writer_activation_token, 0);
	pg_atomic_init_u64(&source_entry.resource_x_activation_generation, 0);
	pg_atomic_init_u64(&source_entry.delivery_attempt, 0);
	pg_atomic_init_u32(&source_entry.flags, 0);
	memset(io_resident_page.data, 0, BLCKSZ);
	memset(storage_page.data, 0, BLCKSZ);
	io_resident_page.data[BLCKSZ - 1] = 0x11;
	storage_page.data[BLCKSZ - 1] = 0x22;
	((PageHeader)io_resident_page.data)->pd_block_scn = s ? 93 : 91;
	((PageHeader)storage_page.data)->pd_block_scn = 93;
	PageXLogRecPtrSet(((PageHeader)storage_page.data)->pd_lsn, UINT64_C(0x200));
	PageXLogRecPtrSet(((PageHeader)io_resident_page.data)->pd_lsn, UINT64_C(0x200));
	source_opens = source_reads = source_flushes = source_wal_flushes = 0;
	marker_attempts = marker_accepted = 0;
	source_locked = source_lock_busy = false;
	source_pins = source_fault = 0;
	cluster_pcm_own_snapshot_locked(&source_buf, &before);
	return before;
}

static ClusterPcmOwnResult
source_prepare(bool s, const ClusterPcmOwnSnapshot *before, ClusterPcmOwnSnapshot *out,
			   char *bytes, XLogRecPtr *lsn, uint64 *scn)
{
	ClusterPcmOwnSourcePrepareRefusal refusal;
	ClusterPageWalBindingV1 wal;
	return s ? cluster_bufmgr_pcm_own_prepare_s_source_image(
				   &source_buf, before, 0, out, bytes, lsn, scn, &refusal, &wal)
			 : cluster_bufmgr_pcm_own_prepare_n_source_image(
				   &source_buf, before, out, bytes, lsn, scn);
}

static void
source_check_no_call(void)
{
	for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++)
		UT_ASSERT_EQ(storage.directory_io[i].serial, 0);
	UT_ASSERT(!source_locked && source_pins == 0);
	UT_ASSERT((pg_atomic_read_u32(&source_buf.state) & BM_LOCKED) == 0);
}

UT_TEST(test_n_s_source_cannot_publish_marker_inside_read_copy_or_wal)
{
	for (int s = 0; s < 2; s++) {
		ClusterPcmOwnSnapshot before = source_fixture(s, s), out;
		char bytes[BLCKSZ];
		XLogRecPtr lsn;
		uint64 scn;
		UT_ASSERT_EQ(source_prepare(s, &before, &out, bytes, &lsn, &scn), CLUSTER_PCM_OWN_OK);
		UT_ASSERT_EQ(bytes[BLCKSZ - 1], s ? 0x11 : 0x22);
		UT_ASSERT_EQ(io_resident_page.data[BLCKSZ - 1], s ? 0x11 : 0x22);
		UT_ASSERT_EQ(lsn, 0x200);
		UT_ASSERT_EQ(scn, 93);
		UT_ASSERT(marker_attempts >= 4);
		UT_ASSERT_EQ(marker_accepted, 0);
		UT_ASSERT_EQ(source_wal_flushes, s);
		UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), PCM_OWN_FLAG_REVOKING);
		source_check_no_call();
		/* Ending raw IO does not finish the retained source reservation. */
		UT_ASSERT_EQ(s ? cluster_bufmgr_pcm_own_abort_s_revoke(&source_buf, &out)
					   : cluster_bufmgr_pcm_own_abort_n_revoke(&source_buf, &out), CLUSTER_PCM_OWN_OK);
		UT_ASSERT(source_commit());
	}
}

UT_TEST(test_committed_drop_denies_clean_and_dirty_source_before_side_effects)
{
	for (int s = 0; s < 2; s++)
		for (int dirty = 0; dirty < 2; dirty++) {
			ClusterPcmOwnSnapshot before = source_fixture(s, 10 + s * 2 + dirty), out;
			char bytes[BLCKSZ];
			XLogRecPtr lsn = 77;
			uint64 scn = 77;
			uint32 state;
			if (dirty)
				pg_atomic_fetch_or_u32(&source_buf.state, BM_DIRTY | BM_JUST_DIRTIED);
			state = pg_atomic_read_u32(&source_buf.state);
			UT_ASSERT(source_commit());
			memset(&out, 0xa5, sizeof(out));
			memset(bytes, 0xa5, sizeof(bytes));
			UT_ASSERT_EQ(source_prepare(s, &before, &out, bytes, &lsn, &scn), CLUSTER_PCM_OWN_BUSY);
			UT_ASSERT_EQ(source_opens + source_reads + source_flushes + source_wal_flushes, 0);
			UT_ASSERT_EQ(pg_atomic_read_u32(&source_buf.state), state);
			UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
			UT_ASSERT_EQ(cluster_pcm_own_reservation_token_get(0), 3);
			UT_ASSERT_EQ(out.reservation_token, 0);
			UT_ASSERT_EQ(bytes[BLCKSZ - 1], 0);
			UT_ASSERT_EQ(lsn + scn, 0);
			source_check_no_call();
		}
}

UT_TEST(test_dirty_source_keeps_call_through_flush_and_original_unpin)
{
	for (int s = 0; s < 2; s++)
		for (int error = 0; error < 2; error++) {
			ClusterPcmOwnSnapshot before = source_fixture(s, 20 + s * 2 + error), out;
			char bytes[BLCKSZ];
			XLogRecPtr lsn;
			uint64 scn;
			volatile bool caught = false;
			pg_atomic_fetch_or_u32(&source_buf.state, BM_DIRTY | BM_JUST_DIRTIED);
			source_fault = error ? 4 : 0;
			PG_TRY();
			{
				UT_ASSERT_EQ(source_prepare(s, &before, &out, bytes, &lsn, &scn), CLUSTER_PCM_OWN_BUSY);
			}
			PG_CATCH(); { caught = true; }
			PG_END_TRY();
			UT_ASSERT_EQ(caught, error != 0);
			UT_ASSERT_EQ(source_flushes, 1);
			UT_ASSERT_EQ(source_opens + source_reads, 0);
			UT_ASSERT_EQ(marker_accepted, 0);
			UT_ASSERT_EQ((pg_atomic_read_u32(&source_buf.state) & BM_DIRTY) != 0, error != 0);
			UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
			source_check_no_call();
			UT_ASSERT(source_commit());
		}
}

UT_TEST(test_source_error_and_refusal_keep_exact_abort_before_call_end)
{
	for (int s = 0; s < 2; s++)
		for (int fault = 1; fault <= (s ? 6 : 3); fault++) {
			ClusterPcmOwnSnapshot before = source_fixture(s, 30 + s * 10 + fault), out;
			char bytes[BLCKSZ];
			XLogRecPtr lsn;
			uint64 scn;
			volatile ClusterPcmOwnResult result = CLUSTER_PCM_OWN_INVALID;
			volatile bool caught = false;
			if (fault == 4)
				continue;
			source_fault = fault;
			PG_TRY(); { result = source_prepare(s, &before, &out, bytes, &lsn, &scn); }
			PG_CATCH(); { caught = true; }
			PG_END_TRY();
			UT_ASSERT_EQ(caught, fault == 1 || fault == 2 || fault == 5);
			if (!caught)
				UT_ASSERT_EQ(result, fault == 3 ? CLUSTER_PCM_OWN_CORRUPT : CLUSTER_PCM_OWN_STALE);
			UT_ASSERT_EQ(marker_accepted, 0);
			UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
			source_check_no_call();
			UT_ASSERT(source_commit());
		}
}

UT_TEST(test_original_stale_content_busy_and_io_busy_leave_no_call)
{
	for (int s = 0; s < 2; s++)
		for (int refusal = 0; refusal < 4; refusal++) {
			ClusterPcmOwnSnapshot before = source_fixture(s, 60 + s * 4 + refusal), out;
			char bytes[BLCKSZ];
			XLogRecPtr lsn;
			uint64 scn;
			if (refusal == 0)
				before.generation++;
			else if (refusal == 1)
				pg_atomic_fetch_or_u32(&source_buf.state, BM_IO_IN_PROGRESS);
			else {
				source_lock_busy = true;
				if (refusal == 3)
					pg_atomic_fetch_or_u32(&source_buf.state, BM_DIRTY);
			}
			UT_ASSERT_EQ(source_prepare(s, &before, &out, bytes, &lsn, &scn),
						 refusal == 0 ? CLUSTER_PCM_OWN_STALE : CLUSTER_PCM_OWN_BUSY);
			UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
			UT_ASSERT_EQ(marker_accepted, 0);
			source_check_no_call();
			UT_ASSERT(source_commit());
		}
}

int
main(void)
{
	UT_PLAN(5);
	UT_RUN(test_n_s_source_cannot_publish_marker_inside_read_copy_or_wal);
	UT_RUN(test_committed_drop_denies_clean_and_dirty_source_before_side_effects);
	UT_RUN(test_dirty_source_keeps_call_through_flush_and_original_unpin);
	UT_RUN(test_source_error_and_refusal_keep_exact_abort_before_call_end);
	UT_RUN(test_original_stale_content_busy_and_io_busy_leave_no_call);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
