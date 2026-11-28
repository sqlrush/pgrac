/* Original shared writer/flush walk; physical storage and pin mechanics use
 * the existing DATA fixture. No writer admission/result is substituted.
 * Author: SqlRush <sqlrush@gmail.com>
 */
int abort_writer_fixture_main(void);
#define main abort_writer_fixture_main
#define cluster_bufmgr_should_pcm_track abort_fixture_should_pcm_track
#define LWLockAcquire abort_fixture_LWLockAcquire
#include "test_cluster_page_data.c"
#undef main
#undef cluster_bufmgr_should_pcm_track
#undef LWLockAcquire

static unsigned abort_reservations, abort_owner_enlarges;
static ClusterConf abort_configuration;
void ResourceOwnerEnlargeBuffers(ResourceOwner owner);
bool LWLockAcquire(LWLock *lock, LWLockMode mode);

bool
LWLockAcquire(LWLock *lock, LWLockMode mode)
{
	if (lock == &mapping)
		return abort_fixture_LWLockAcquire(lock, mode);
	for (int i = 0; i < NBuffers; i++)
		if (lock == BufferDescriptorGetContentLock(&descriptors[i].bufferdesc)) {
			UT_ASSERT(pins[i] && !locks[i] && !busy[i]);
			locks[i] = true;
			HOLD_INTERRUPTS();
			return true;
		}
	abort();
}

static void
abort_storage_write(SMgrRelation rel, ForkNumber fork, BlockNumber block,
					const void *data, bool skip)
{
	UT_ASSERT(rel == &relation && locks[data_slot] && pins[data_slot] != 0);
	UT_ASSERT_EQ(fork, target.identity.forknum);
	UT_ASSERT_EQ(block, target.identity.blockno);
	UT_ASSERT_EQ(wal_flushes, 1); /* Observed before the DATA provider call. */
	UT_ASSERT(pg_atomic_read_u32(&descriptors[data_slot].bufferdesc.state) & BM_IO_IN_PROGRESS);
	UT_ASSERT(!skip && data != NULL);
	writes++;
}

void
ResourceOwnerEnlargeBuffers(ResourceOwner owner)
{
	UT_ASSERT(owner == CurrentResourceOwner);
	abort_owner_enlarges++;
}
static void
ReservePrivateRefCountEntry(void)
{
	abort_reservations++;
}
static void
PinBuffer_Locked(BufferDesc *buf)
{
	uint32 state = pg_atomic_read_u32(&buf->state);
	UT_ASSERT(state & BM_LOCKED);
	pins[buf->buf_id]++;
	UnlockBufHdr(buf, state + BUF_REFCOUNT_ONE);
}
static void
UnpinBuffer(BufferDesc *buf)
{
	uint32 state = LockBufHdr(buf);
	UT_ASSERT(pins[buf->buf_id] != 0);
	pins[buf->buf_id]--;
	UnlockBufHdr(buf, state - BUF_REFCOUNT_ONE);
}

#include "test_cluster_abort_writer_flush.inc"

static void
abort_writer_reset(ForkNumber forknum)
{
	reset_for_fork(forknum);
	memset(&abort_configuration, 0, sizeof(abort_configuration));
	abort_configuration.node_count = 4;
	ClusterConfShmem = &abort_configuration;
	cluster_recmerge_window_active = false;
	MyBackendType = B_BACKEND;
	UT_ASSERT(cluster_pcm_is_active()); /* Actual production topology gate. */
	storage_write_hook = abort_storage_write;
	relation.smgr_rlocator.backend = InvalidBackendId;
	relation.smgr_which = CLUSTER_SMGR_SMGRSW_INDEX;
	/* An unrelated dirty resident must not be written or counted clean. */
	descriptors[0].bufferdesc.tag.relNumber++;
	pg_atomic_fetch_or_u32(&descriptors[0].bufferdesc.state, BM_TAG_VALID | BM_DIRTY);
	pg_atomic_fetch_or_u32(&descriptors[1].bufferdesc.state, BM_TAG_VALID | BM_DIRTY);
	abort_reservations = abort_owner_enlarges = 0;
}

static void
check_unpinned(void)
{
	UT_ASSERT_EQ(pins[0] + pins[1], 0);
	UT_ASSERT(!locks[0] && !locks[1]);
}

UT_TEST(original_writer_flushes_wal_then_data_for_exact_storage)
{
	abort_writer_reset(MAIN_FORKNUM);
	UT_ASSERT(cluster_bufmgr_flush_created_abort_v1(&relation));
	UT_ASSERT_EQ(writes, 1);
	UT_ASSERT_EQ(wal_flushes, 1);
	UT_ASSERT_EQ(pg_atomic_read_u32(&descriptors[1].bufferdesc.state) &
				 (BM_DIRTY | BM_IO_IN_PROGRESS | BM_IO_ERROR), 0);
	UT_ASSERT(pg_atomic_read_u32(&descriptors[0].bufferdesc.state) & BM_DIRTY);
	UT_ASSERT_EQ(abort_owner_enlarges, 1);
	check_unpinned();
	clean();
}

UT_TEST(original_writer_skip_preserves_dirty_and_refuses_completion)
{
	abort_writer_reset(MAIN_FORKNUM);
	cluster_smart_fusion = sf_blocked = true;
	UT_ASSERT(!cluster_bufmgr_flush_created_abort_v1(&relation));
	UT_ASSERT_EQ(writes + wal_flushes, 0);
	UT_ASSERT(pg_atomic_read_u32(&descriptors[1].bufferdesc.state) & BM_DIRTY);
	check_unpinned();
	clean();
}

UT_TEST(revoking_vm_current_flushes_before_handoff)
{
	abort_writer_reset(VISIBILITYMAP_FORKNUM);
	pg_atomic_write_u64(&own[1].reservation_token, 9);
	pg_atomic_write_u32(&own[1].flags, PCM_OWN_FLAG_REVOKING);
	UT_ASSERT(cluster_bufmgr_flush_created_abort_v1(&relation));
	UT_ASSERT_EQ(writes, 1);
	UT_ASSERT_EQ(wal_flushes, 1);
	UT_ASSERT(!(pg_atomic_read_u32(&descriptors[1].bufferdesc.state) & BM_DIRTY));
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(1), PCM_OWN_FLAG_REVOKING);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_token_get(1), 9);
	check_unpinned();
	clean();
}

UT_TEST(dirty_retained_vm_refuses_abort_writeback)
{
	BufferDesc *buf = &descriptors[1].bufferdesc;

	abort_writer_reset(VISIBILITYMAP_FORKNUM);
	pg_atomic_write_u64(&own[1].reservation_token, 9);
	pg_atomic_write_u32(&own[1].flags, PCM_OWN_FLAG_REVOKING);
	buf->pcm_state = PCM_STATE_N;
	buf->buffer_type = BUF_TYPE_PI;
	UT_ASSERT(!cluster_bufmgr_flush_created_abort_v1(&relation));
	UT_ASSERT_EQ(writes + wal_flushes, 0);
	UT_ASSERT(pg_atomic_read_u32(&buf->state) & BM_DIRTY);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_token_get(1), 9);
	check_unpinned();
	clean();
}

/* The ERROR leaf in the DATA fixture resets interrupt holdoff. Model the
 * abort cleanup of the caller's content lock and pin before checking leaks. */
static void
release_abort_writer_after_error(void)
{
	UT_ASSERT_EQ(InterruptHoldoffCount, 0);
	locks[1] = false;
	if (pins[1])
		UnpinBuffer(&descriptors[1].bufferdesc);
}

static void
check_corrupt_vm_reservation(uint32 flags, uint64 token)
{
	volatile bool caught = false;

	abort_writer_reset(VISIBILITYMAP_FORKNUM);
	pg_atomic_write_u64(&own[1].reservation_token, token);
	pg_atomic_write_u32(&own[1].flags, flags);
	PG_TRY();
	{
		(void) cluster_bufmgr_flush_created_abort_v1(&relation);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(writes + wal_flushes, 0);
	UT_ASSERT(pg_atomic_read_u32(&descriptors[1].bufferdesc.state) & BM_DIRTY);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(1), flags);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_token_get(1), token);
	if (caught)
		release_abort_writer_after_error();
	check_unpinned();
	clean();
}

UT_TEST(corrupt_vm_reservation_never_reaches_storage)
{
	check_corrupt_vm_reservation(PCM_OWN_FLAG_REVOKING, 0);
	check_corrupt_vm_reservation(PCM_OWN_FLAG_REVOKING | PCM_OWN_FLAG_GRANT_PENDING, 9);
}

UT_TEST(original_dirty_activation_fence_raises_without_output)
{
	volatile bool caught = false;
	abort_writer_reset(MAIN_FORKNUM);
	pg_atomic_write_u64(&own[1].writer_activation_token, 7);
	PG_TRY();
	{
		(void) cluster_bufmgr_flush_created_abort_v1(&relation);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(writes + wal_flushes, 0);
	UT_ASSERT(pg_atomic_read_u32(&descriptors[1].bufferdesc.state) & BM_DIRTY);
	UT_ASSERT_EQ(pg_atomic_read_u64(&own[1].writer_activation_token), 7);
	release_abort_writer_after_error();
	pg_atomic_write_u64(&own[1].writer_activation_token, 0);
	check_unpinned();
	clean();
}

UT_TEST(clean_io_or_error_is_not_completion)
{
	for (int fault = 0; fault < 2; fault++) {
		abort_writer_reset(MAIN_FORKNUM);
		pg_atomic_fetch_and_u32(&descriptors[1].bufferdesc.state, ~BM_DIRTY);
		pg_atomic_fetch_or_u32(&descriptors[1].bufferdesc.state,
							  fault == 0 ? BM_IO_IN_PROGRESS : BM_IO_ERROR);
		UT_ASSERT(!cluster_bufmgr_flush_created_abort_v1(&relation));
		UT_ASSERT_EQ(writes + wal_flushes, 0);
		pg_atomic_fetch_and_u32(&descriptors[1].bufferdesc.state,
							   ~(BM_IO_IN_PROGRESS | BM_IO_ERROR));
		check_unpinned();
		clean();
	}
}

UT_TEST(local_temp_or_missing_storage_is_not_admitted)
{
	abort_writer_reset(MAIN_FORKNUM);
	UT_ASSERT(!cluster_bufmgr_flush_created_abort_v1(NULL));
	relation.smgr_which = 0;
	UT_ASSERT(!cluster_bufmgr_flush_created_abort_v1(&relation));
	relation.smgr_which = CLUSTER_SMGR_SMGRSW_INDEX;
	relation.smgr_rlocator.backend = 1;
	UT_ASSERT(!cluster_bufmgr_flush_created_abort_v1(&relation));
	UT_ASSERT_EQ(writes + wal_flushes + abort_owner_enlarges, 0);
	check_unpinned();
	clean();
}

int
main(void)
{
	UT_PLAN(8);
	UT_RUN(original_writer_flushes_wal_then_data_for_exact_storage);
	UT_RUN(original_writer_skip_preserves_dirty_and_refuses_completion);
	UT_RUN(revoking_vm_current_flushes_before_handoff);
	UT_RUN(dirty_retained_vm_refuses_abort_writeback);
	UT_RUN(corrupt_vm_reservation_never_reaches_storage);
	UT_RUN(original_dirty_activation_fence_raises_without_output);
	UT_RUN(clean_io_or_error_is_not_completion);
	UT_RUN(local_temp_or_missing_storage_is_not_admitted);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
