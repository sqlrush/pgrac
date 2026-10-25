/*-------------------------------------------------------------------------
 * test_cluster_pcm_read_source.c
 *    First-fill source admission and exact cancellation owners.
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
int pcm_read_source_fixture_main(void);
#define main pcm_read_source_fixture_main
#include "test_cluster_pcm_own.c"
#undef main

#include "cluster/cluster_pcm_read_source.h"

UT_TEST(test_typed_source_allows_disk_only_for_exact_disk_grant)
{
	ClusterPcmReadSource source = { 0 };
	BufferTag tag = { 0 };

	tag.relNumber = 16392;
	tag.blockNum = 94;
	source.tag = tag;
	source.base_generation = 48;
	source.reservation_token = 49;
	source.quiescent = true;
	source.durable = true;
	source.kind = CLUSTER_PCM_SOURCE_DISK_GRANT;
	UT_ASSERT(cluster_pcm_read_source_allows_disk(&source, &tag, 48, 49));
	source.kind = CLUSTER_PCM_SOURCE_IMAGE_INSTALLED;
	UT_ASSERT(!cluster_pcm_read_source_allows_disk(&source, &tag, 48, 49));
	source.kind = CLUSTER_PCM_SOURCE_RETRY;
	UT_ASSERT(!cluster_pcm_read_source_allows_disk(&source, &tag, 48, 49));
	source.kind = CLUSTER_PCM_SOURCE_FAIL;
	UT_ASSERT(!cluster_pcm_read_source_allows_disk(&source, &tag, 48, 49));
}

UT_TEST(test_source_cannot_authorize_successor_or_active_delivery)
{
	ClusterPcmReadSource source = { 0 };
	BufferTag tag = { 0 };

	tag.relNumber = 16392;
	tag.blockNum = 94;
	source.tag = tag;
	source.base_generation = 48;
	source.reservation_token = 49;
	source.quiescent = true;
	source.durable = true;
	source.kind = CLUSTER_PCM_SOURCE_DISK_GRANT;
	UT_ASSERT(!cluster_pcm_read_source_allows_disk(&source, &tag, 49, 49));
	UT_ASSERT(!cluster_pcm_read_source_allows_disk(&source, &tag, 48, 50));
	tag.blockNum++;
	UT_ASSERT(!cluster_pcm_read_source_allows_disk(&source, &tag, 48, 49));
	tag.blockNum--;
	source.quiescent = false;
	UT_ASSERT(!cluster_pcm_read_source_allows_disk(&source, &tag, 48, 49));
}

bool
cluster_pcm_lock_acquire_buffer(BufferDesc *buf pg_attribute_unused(),
								PcmLockMode mode pg_attribute_unused(),
								bool *retry_denied pg_attribute_unused())
{
	/* Cancellation at the real transport boundary, before any image copy. */
	pg_re_throw();
	return false;
}


static ClusterPcmReadSourceKind fixture_kind = CLUSTER_PCM_SOURCE_FAIL;
static bool fixture_cancel = true, fixture_quiet = true, fixture_release_error;
static bool fixture_cancel_retry_wait, fixture_shutdown, fixture_shutdown_checked;
static unsigned fixture_calls, fixture_retries, fixture_io_aborts, fixture_releases;

ClusterPcmReadSourceKind
cluster_pcm_lock_acquire_buffer_source(BufferDesc *buf pg_attribute_unused(),
									   ClusterPcmReadSource *source)
{
	fixture_calls++;
	if (fixture_shutdown && fixture_calls > 3)
		pg_re_throw(); /* A broken loop must fail without hanging this test. */
	source->quiescent = fixture_quiet;
	if (fixture_cancel)
		pg_re_throw();
	source->kind = fixture_kind;
	if (fixture_retries > 0) {
		fixture_retries--;
		source->kind = CLUSTER_PCM_SOURCE_RETRY;
	}
	source->durable = source->kind == CLUSTER_PCM_SOURCE_DISK_GRANT
		|| source->kind == CLUSTER_PCM_SOURCE_IMAGE_INSTALLED;
	return source->kind;
}

void
cluster_pcm_lock_check_shutdown_wait(void)
{
	if (fixture_shutdown || fixture_cancel_retry_wait) {
		UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
		fixture_shutdown_checked = true;
		pg_re_throw();
	}
}

static ClusterPcmOwnResult
fixture_reserve(BufferDesc *buf, PcmLockMode mode, ClusterPcmOwnSnapshot *base, uint64 *token,
				bool *covered, ClusterPcmOwnSnapshot *unused pg_attribute_unused())
{
	UT_ASSERT_EQ(mode, PCM_LOCK_MODE_S);
	*covered = false;
	if (cluster_bufmgr_pcm_own_snapshot(buf, base) != CLUSTER_PCM_OWN_OK)
		return CLUSTER_PCM_OWN_STALE;
	return cluster_pcm_own_reservation_begin_exact(buf->buf_id, base->generation,
												   PCM_OWN_FLAG_GRANT_PENDING, token);
}

static bool
fixture_content_acquire(LWLock *lock, LWLockMode mode)
{
	if (lock == &transition_mapping_lock) {
		UT_ASSERT_EQ(mode, LW_SHARED);
		UT_ASSERT(!transition_mapping_held);
		transition_mapping_held = true;
		return true;
	}
	UT_ASSERT_EQ(mode, LW_SHARED);
	UT_ASSERT(lock == BufferDescriptorGetContentLock(transition_buf));
	UT_ASSERT(!transition_content_held);
	transition_content_held = true;
	return true;
}
static void
fixture_release(BufferDesc *buf, PcmLockMode mode)
{
	UT_ASSERT_EQ(buf->buf_id, 0);
	UT_ASSERT_EQ(mode, PCM_LOCK_MODE_S);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), PCM_OWN_FLAG_GRANT_PENDING);
	UT_ASSERT(pg_atomic_read_u32(&buf->state) & BM_IO_IN_PROGRESS);
	fixture_releases++;
	if (fixture_release_error)
		pg_re_throw();
}
static void
fixture_abort_io(Buffer b)
{
	UT_ASSERT_EQ(b, 1);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
	fixture_io_aborts++;
	pg_atomic_fetch_and_u32(&transition_buf->state, ~BM_IO_IN_PROGRESS);
	pg_atomic_fetch_or_u32(&transition_buf->state, BM_IO_ERROR);
}
static unsigned fixture_nacks;
static void
fixture_source_complete(BufferDesc *buf, ClusterPcmReadSource *source, bool installed)
{
	UT_ASSERT(!installed);
	if (source->pending_s && source->quiescent) {
		UT_ASSERT_EQ(cluster_pcm_own_flags_get(buf->buf_id), 0);
		UT_ASSERT_EQ(buf->pcm_state, PCM_STATE_N);
		UT_ASSERT(pg_atomic_read_u32(&buf->state) & BM_IO_IN_PROGRESS);
		fixture_nacks++;
		source->pending_s = false;
	}
}
#define cluster_gcs_block_read_source_complete fixture_source_complete
#define cluster_bufmgr_trace_cleanup(buf, phase, token) ((void)0)
#define cluster_block_io_trace_source(source, phase) ((void)0)
#define cluster_gcs_block_read_source_quiesce(buf, source) fixture_quiet
#define cluster_pcm_lock_release_buffer_for_eviction fixture_release
#define cluster_bufmgr_pcm_begin_grant_reservation_wait fixture_reserve
#define AbortBufferIO fixture_abort_io
#define CHECK_FOR_INTERRUPTS()                                                                     \
	do {                                                                                           \
		if (fixture_cancel_retry_wait)                                                             \
			pg_re_throw();                                                                         \
	} while (0)
#define pgstat_report_wait_start(event) ((void)0)
#define pgstat_report_wait_end() ((void)0)
#define pg_usleep(us) ((void)0)
#define LWLockAcquire fixture_content_acquire
#define LWLockRelease transition_lock_release
#define ereport(level, ...) pg_re_throw()
#define elog(level, ...) ((void)0)
#define LockBufHdr transition_lock_header
#define BufTableHashCode(tag) 0
#define BufMappingPartitionLock(hash) (&transition_mapping_lock)
#define BufTableLookup(tag, hash) 0
#define GetBufferDescriptor(id) transition_buf
#include "test_cluster_pcm_read_source_owner.inc"
#undef GetBufferDescriptor
#undef BufTableLookup
#undef BufMappingPartitionLock
#undef BufTableHashCode
#undef LockBufHdr
#undef elog
#undef ereport
#undef LWLockRelease
#undef LWLockAcquire
#undef pg_usleep
#undef pgstat_report_wait_end
#undef pgstat_report_wait_start
#undef CHECK_FOR_INTERRUPTS
#undef AbortBufferIO
#undef cluster_bufmgr_pcm_begin_grant_reservation_wait
#undef cluster_pcm_lock_release_buffer_for_eviction
#undef cluster_gcs_block_read_source_quiesce
#undef cluster_gcs_block_read_source_complete
#undef cluster_block_io_trace_source
#undef cluster_bufmgr_trace_cleanup

/* Break caught: generic buffer cleanup must not clear IO first and strand
 * the source reservation. Execute the real pre-cleanup PG_CATCH owner. */
UT_TEST(test_cancel_first_fill_retires_exact_reservation_before_io_cleanup)
{
	BufferDesc buf;
	ClusterPcmOwnEntry entry;
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	ClusterPcmOwnSnapshot base;
	uint64 token = 0;
	ClusterPcmReadSource source = { 0 };
	bool retry_denied = false;
	volatile bool caught = false;

	n_predecessor_fixture(&buf, &entry, BUF_TYPE_CURRENT);
	pg_atomic_write_u32(&buf.state, BM_TAG_VALID | BM_IO_IN_PROGRESS | BUF_REFCOUNT_ONE);
	UT_ASSERT_EQ(cluster_bufmgr_pcm_own_snapshot(&buf, &base), CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_begin_exact(0, base.generation,
														 PCM_OWN_FLAG_GRANT_PENDING, &token),
				 CLUSTER_PCM_OWN_OK);
	source.tag = base.tag;
	source.base_generation = base.generation;
	source.reservation_token = token;
	PG_TRY();
	{
		(void)cluster_bufmgr_pcm_acquire_shared_owned(&buf, &base, token, &retry_denied, &source);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_token_get(0), token);
	UT_ASSERT((pg_atomic_read_u32(&buf.state) & BM_IO_IN_PROGRESS) != 0);
	ClusterPcmOwnArray = saved;
}

UT_TEST(test_unknown_delivery_cannot_use_generic_io_cleanup_as_proof)
{
	BufferDesc buf;
	ClusterPcmOwnEntry entry;
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	ClusterPcmOwnSnapshot base;
	ClusterPcmReadSource source = { 0 };
	uint64 token;
	n_predecessor_fixture(&buf, &entry, BUF_TYPE_CURRENT);
	pg_atomic_write_u32(&buf.state, BM_TAG_VALID | BUF_REFCOUNT_ONE);
	UT_ASSERT_EQ(cluster_bufmgr_pcm_own_snapshot(&buf, &base), CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_begin_exact(0, base.generation,
														 PCM_OWN_FLAG_GRANT_PENDING, &token),
				 CLUSTER_PCM_OWN_OK);
	source.tag = base.tag;
	source.base_generation = base.generation;
	source.reservation_token = token;
	source.quiescent = false;
	UT_ASSERT_NE(cluster_bufmgr_pcm_abort_shared_owned(&buf, &base, token, &source),
				 CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), PCM_OWN_FLAG_GRANT_PENDING);
	ClusterPcmOwnArray = saved;
}

UT_TEST(test_native_first_fill_owners_four_results_and_local_retry)
{
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	for (int k = CLUSTER_PCM_SOURCE_FAIL; k <= CLUSTER_PCM_SOURCE_RETRY; k++) {
		BufferDesc buf;
		ClusterPcmOwnEntry entry;
		ClusterBufmgrReadSource read = { 0 };
		volatile bool caught = false;
		n_predecessor_fixture(&buf, &entry, BUF_TYPE_CURRENT);
		transition_buf = &buf;
		transition_content_held = false;
		pg_atomic_write_u32(&buf.state, BM_TAG_VALID | BM_IO_IN_PROGRESS | BUF_REFCOUNT_ONE);
		fixture_kind = (ClusterPcmReadSourceKind)k;
		fixture_cancel = false;
		fixture_quiet = true;
		fixture_calls = 0;
		fixture_retries = k == CLUSTER_PCM_SOURCE_RETRY ? 1 : 0;
		if (k == CLUSTER_PCM_SOURCE_RETRY)
			fixture_kind = CLUSTER_PCM_SOURCE_DISK_GRANT;
		read.buf = &buf;
		PG_TRY();
		{
			cluster_bufmgr_pcm_read_source_begin(&buf, &read);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT_EQ(caught, k == CLUSTER_PCM_SOURCE_FAIL);
		UT_ASSERT_EQ(fixture_calls, k == CLUSTER_PCM_SOURCE_RETRY ? 2 : 1);
		if (!caught) {
			UT_ASSERT_EQ(cluster_bufmgr_pcm_read_source_disk_allowed(&buf, &read),
						 k != CLUSTER_PCM_SOURCE_IMAGE_INSTALLED);
			cluster_bufmgr_pcm_read_source_finish(&buf, &read);
			UT_ASSERT(!read.pending);
			UT_ASSERT(!transition_content_held);
			UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
			UT_ASSERT_EQ(buf.pcm_state,
						 PCM_STATE_S);
			UT_ASSERT(pg_atomic_read_u32(&buf.state) & BM_IO_IN_PROGRESS);
			UT_ASSERT(!(pg_atomic_read_u32(&buf.state) & BM_VALID));
		}
	}
	fixture_cancel = true;
	fixture_quiet = true;
	ClusterPcmOwnArray = saved;
}

UT_TEST(test_first_fill_error_owner_orders_rollback_retirement_and_io_abort)
{
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	for (int scenario = 0; scenario < 6; scenario++) {
		BufferDesc buf;
		ClusterPcmOwnEntry entry;
		ClusterBufmgrReadSource read = { 0 };
		volatile bool caught = false;
		n_predecessor_fixture(&buf, &entry, BUF_TYPE_CURRENT);
		transition_buf = &buf;
		transition_content_held = false;
		pg_atomic_write_u32(&buf.state, BM_TAG_VALID | BM_IO_IN_PROGRESS | BUF_REFCOUNT_ONE);
		fixture_cancel = false;
		fixture_kind = CLUSTER_PCM_SOURCE_DISK_GRANT;
		fixture_retries = 0;
		fixture_quiet = true;
		read.buf = &buf;
		cluster_bufmgr_pcm_read_source_begin(&buf, &read);
		fixture_io_aborts = fixture_releases = fixture_nacks = 0;
		if (scenario == 5)
			read.source.pending_s = true;
		fixture_release_error = scenario == 1;
		fixture_quiet = scenario != 2;
		if (scenario == 3)
			pg_atomic_fetch_add_u64(&entry.reservation_token, 1);
		if (scenario == 4)
			pg_atomic_write_u64(&entry.delivery_attempt, 100);
		PG_TRY();
		{
			cluster_bufmgr_pcm_read_source_error(0, PointerGetDatum(&read));
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT_EQ(caught, scenario == 1);
		UT_ASSERT_EQ(fixture_io_aborts, scenario == 0 || scenario == 5 ? 1 : 0);
		if (scenario == 5) {
			UT_ASSERT_EQ(fixture_releases, 0);
			UT_ASSERT_EQ(fixture_nacks, 1);
		}
		UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), scenario == 0 || scenario == 5 ? 0 : PCM_OWN_FLAG_GRANT_PENDING);
		UT_ASSERT_EQ((pg_atomic_read_u32(&buf.state) & BM_IO_IN_PROGRESS) != 0, scenario != 0 && scenario != 5);
	}
	fixture_cancel = true;
	fixture_quiet = true;
	fixture_release_error = false;
	ClusterPcmOwnArray = saved;
}

UT_TEST(test_invalidate_sees_entire_first_fill_until_native_io_publication)
{
	BufferDesc buf;
	ClusterPcmOwnEntry entry;
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	ClusterBufmgrReadSource read = { 0 };
	BufferTag other;
	n_predecessor_fixture(&buf, &entry, BUF_TYPE_CURRENT);
	transition_buf = &buf;
	transition_mapping_held = false;
	transition_content_held = false;
	pg_atomic_write_u32(&buf.state, BM_TAG_VALID | BM_IO_IN_PROGRESS | BUF_REFCOUNT_ONE);
	fixture_cancel = false;
	fixture_quiet = true;
	fixture_retries = 0;
	fixture_kind = CLUSTER_PCM_SOURCE_DISK_GRANT;
	cluster_bufmgr_pcm_read_source_begin(&buf, &read);
	UT_ASSERT(cluster_bufmgr_block_grant_pending(buf.tag));
	UT_ASSERT(!cluster_bufmgr_pending_s_aborted(buf.tag));
	cluster_bufmgr_pcm_read_source_finish(&buf, &read);
	UT_ASSERT_EQ(buf.pcm_state, PCM_STATE_S);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
	UT_ASSERT(cluster_bufmgr_block_grant_pending(buf.tag));
	other = buf.tag;
	other.blockNum++;
	UT_ASSERT(!cluster_bufmgr_block_grant_pending(other));
	pg_atomic_fetch_or_u32(&buf.state, BM_VALID);
	pg_atomic_fetch_and_u32(&buf.state, ~BM_IO_IN_PROGRESS);
	UT_ASSERT(!cluster_bufmgr_block_grant_pending(buf.tag));
	/* Generic IO cleanup cannot make an unretired/unknown reservation look
	 * already invalidated to a different node. */
	n_predecessor_fixture(&buf, &entry, BUF_TYPE_CURRENT);
	transition_buf = &buf;
	pg_atomic_write_u32(&buf.state, BM_TAG_VALID | BM_IO_IN_PROGRESS | BUF_REFCOUNT_ONE);
	memset(&read, 0, sizeof(read));
	read.buf = &buf;
	cluster_bufmgr_pcm_read_source_begin(&buf, &read);
	fixture_quiet = false;
	cluster_bufmgr_pcm_read_source_error(0, PointerGetDatum(&read));
	pg_atomic_fetch_and_u32(&buf.state, ~BM_IO_IN_PROGRESS);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), PCM_OWN_FLAG_GRANT_PENDING);
	UT_ASSERT(cluster_bufmgr_block_grant_pending(buf.tag));
	fixture_quiet = true;
	fixture_cancel = true;
	ClusterPcmOwnArray = saved;
}

UT_TEST(test_cancel_after_retry_retirement_still_finishes_native_io)
{
	BufferDesc buf;
	ClusterPcmOwnEntry entry;
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	ClusterBufmgrReadSource read = { 0 };
	volatile bool caught = false;
	n_predecessor_fixture(&buf, &entry, BUF_TYPE_CURRENT);
	transition_buf = &buf;
	transition_content_held = false;
	pg_atomic_write_u32(&buf.state, BM_TAG_VALID | BM_IO_IN_PROGRESS | BUF_REFCOUNT_ONE);
	fixture_cancel = false;
	fixture_quiet = true;
	fixture_cancel_retry_wait = true;
	fixture_retries = 1;
	fixture_kind = CLUSTER_PCM_SOURCE_DISK_GRANT;
	read.buf = &buf;
	fixture_io_aborts = 0;
	PG_TRY();
	{
		cluster_bufmgr_pcm_read_source_begin(&buf, &read);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT(!read.pending);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
	cluster_bufmgr_pcm_read_source_error(0, PointerGetDatum(&read));
	UT_ASSERT_EQ(fixture_io_aborts, 1);
	UT_ASSERT(!(pg_atomic_read_u32(&buf.state) & BM_IO_IN_PROGRESS));
	fixture_cancel_retry_wait = false;
	fixture_cancel = true;
	ClusterPcmOwnArray = saved;
}

UT_TEST(test_first_fill_consumes_only_proven_terminal_cover_and_keeps_pi)
{
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	for (int remote = 0; remote < 2; remote++) {
		for (int stale = 0; stale < 2; stale++) {
			BufferDesc buf;
			ClusterPcmOwnEntry entry;
			PGAlignedBlock image;
			XLogRecPtr lsn;
			ClusterBufmgrGcsCopyRefusal refusal;
			ClusterPcmReadGrantKey key = { 0 };
			ClusterBufmgrGcsDowngradeOutcome result;
			drop_fixture(&buf, &entry, true);
			buf.pcm_state = PCM_STATE_X;
			buf.buffer_type = BUF_TYPE_XCUR;
			transition_downgrade_active = true;
			downgrade_applies = downgrade_notifies = downgrade_fuses = 0;
			downgrade_preflight = stale ? RESOURCE_X_APPLY_STALE : RESOURCE_X_APPLY_APPLIED;
			key.tag = buf.tag;
			key.request_id = 12;
			key.epoch = 1;
			key.backend_id = 1;
			key.requester_node = 3;
			result = cluster_bufmgr_first_fill_s_prepare_image(&key,
				remote ? 2 : cluster_node_id, &lsn, image.data, &refusal);
			UT_ASSERT_EQ(result, stale ? CLUSTER_BUFMGR_GCS_DOWNGRADE_REFUSED_PRE_NOTIFY
				: CLUSTER_BUFMGR_GCS_DOWNGRADE_COMMITTED);
			UT_ASSERT_EQ(buf.pcm_state, stale ? PCM_STATE_X : PCM_STATE_S);
			UT_ASSERT_EQ(downgrade_applies + downgrade_notifies, !stale);
			UT_ASSERT_EQ(transition_flush_count, !stale);
			if (!stale)
				UT_ASSERT_EQ(memcmp(image.data, transition_page.data, BLCKSZ), 0);
			transition_downgrade_active = false;
			drop_fixture_done(saved);
		}
	}
}

UT_TEST(test_retired_retry_reservation_can_ack_exact_revoke_without_clearing_io)
{
	BufferDesc buf;
	ClusterPcmOwnEntry entry;
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	ClusterPcmOwnSnapshot base;
	uint64 token;
	n_predecessor_fixture(&buf, &entry, BUF_TYPE_CURRENT);
	transition_buf = &buf;
	transition_mapping_held = false;
	pg_atomic_write_u32(&buf.state, BM_TAG_VALID | BM_IO_IN_PROGRESS | BUF_REFCOUNT_ONE);
	pg_atomic_write_u64(&entry.reservation_token, 0);
	UT_ASSERT(!cluster_bufmgr_pending_s_aborted(buf.tag)); /* token has not been reserved */
	pg_atomic_write_u64(&entry.reservation_token, 48);
	UT_ASSERT_EQ(cluster_bufmgr_pcm_own_snapshot(&buf, &base), CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_begin_exact(0, base.generation,
		PCM_OWN_FLAG_GRANT_PENDING, &token), CLUSTER_PCM_OWN_OK);
	UT_ASSERT(!cluster_bufmgr_pending_s_aborted(buf.tag));
	UT_ASSERT_EQ(cluster_pcm_own_reservation_abort_exact(0, base.generation, token,
		PCM_OWN_FLAG_GRANT_PENDING), CLUSTER_PCM_OWN_OK);
	UT_ASSERT(cluster_bufmgr_pending_s_aborted(buf.tag));
	UT_ASSERT(pg_atomic_read_u32(&buf.state) & BM_IO_IN_PROGRESS);
	UT_ASSERT(cluster_bufmgr_block_grant_pending(buf.tag)); /* generic invalidates still wait */
	pg_atomic_write_u64(&entry.delivery_attempt, 9);
	UT_ASSERT(!cluster_bufmgr_pending_s_aborted(buf.tag));
	pg_atomic_write_u64(&entry.delivery_attempt, 0);
	buf.pcm_state = PCM_STATE_S;
	UT_ASSERT(!cluster_bufmgr_pending_s_aborted(buf.tag));
	ClusterPcmOwnArray = saved;
}

UT_TEST(test_shutdown_retry_unwinds_only_after_exact_retirement)
{
	BufferDesc buf;
	ClusterPcmOwnEntry entry;
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	ClusterBufmgrReadSource read = { 0 };
	volatile bool caught = false;

	n_predecessor_fixture(&buf, &entry, BUF_TYPE_CURRENT);
	transition_buf = &buf;
	pg_atomic_write_u32(&buf.state, BM_TAG_VALID | BM_IO_IN_PROGRESS | BUF_REFCOUNT_ONE);
	fixture_cancel = false;
	fixture_quiet = true;
	fixture_shutdown = true;
	fixture_shutdown_checked = false;
	fixture_retries = 100;
	fixture_calls = 0;
	read.buf = &buf;
	fixture_io_aborts = 0;
	PG_TRY();
	{
		cluster_bufmgr_pcm_read_source_begin(&buf, &read);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT(fixture_shutdown_checked);
	UT_ASSERT_EQ(fixture_calls, 1);
	UT_ASSERT(!read.pending);
	cluster_bufmgr_pcm_read_source_error(0, PointerGetDatum(&read));
	UT_ASSERT_EQ(fixture_io_aborts, 1);
	fixture_shutdown = false;
	fixture_retries = 0;
	fixture_cancel = true;
	ClusterPcmOwnArray = saved;
}

#include <dirent.h>
#include <sys/stat.h>
static char *trace_data_dir;
static char trace_line[2048];
static unsigned trace_lines;
bool cluster_trace_block_io = false;
static void fixture_trace_stderr(const char *fmt, ...) pg_attribute_printf(1, 2);
static void
fixture_trace_stderr(const char *fmt, ...)
{
	va_list args;
	va_start(args, fmt);
	vsnprintf(trace_line, sizeof(trace_line), fmt, args);
	va_end(args);
	trace_lines++;
}
#define DataDir trace_data_dir
#define MyProcPid getpid()
#define GetCurrentTimestamp() INT64CONST(42)
#define write_stderr fixture_trace_stderr
#define LockBufHdr transition_lock_header
#include "../../backend/cluster/cluster_block_io_trace.c"
#undef LockBufHdr
#undef write_stderr
#undef GetCurrentTimestamp
#undef MyProcPid
#undef DataDir

UT_TEST(test_trace_is_default_off_and_captures_exact_failed_bytes_without_overwrite)
{
	char root[] = "/tmp/pgrac-first-source-XXXXXX";
	char statdir[MAXPGPATH], path[MAXPGPATH];
	PGAlignedBlock failed, restored;
	BufferTag tag = { 0 };
	DIR *dir;
	struct dirent *entry;
	unsigned captures = 0;
	memset(failed.data, 0xa7, BLCKSZ);
	tag.relNumber = 16392;
	tag.blockNum = 94;
	trace_data_dir = mkdtemp(root);
	UT_ASSERT(trace_data_dir != NULL);
	snprintf(statdir, sizeof(statdir), "%s/pg_stat_tmp", root);
	UT_ASSERT_EQ(mkdir(statdir, 0700), 0);
	trace_lines = 0;
	errno = EINTR;
	cluster_block_io_trace_failed(tag, failed.data, "DISK", 6686, 11612);
	UT_ASSERT_EQ(trace_lines, 0);
	UT_ASSERT_EQ(errno, EINTR);
	cluster_trace_block_io = true;
	for (int i = 0; i < 2; i++) {
		cluster_block_io_trace_failed(tag, failed.data, "DISK", 6686, 11612);
		UT_ASSERT_EQ(errno, EINTR);
		UT_ASSERT(strstr(trace_line, "bytes=8192 capture_errno=0") != NULL);
	}
	dir = opendir(statdir);
	UT_ASSERT(dir != NULL);
	while ((entry = readdir(dir)) != NULL) {
		struct stat st;
		int fd;
		if (entry->d_name[0] == '.')
			continue;
		snprintf(path, sizeof(path), "%s/%s", statdir, entry->d_name);
		UT_ASSERT_EQ(stat(path, &st), 0);
		UT_ASSERT_EQ(st.st_size, BLCKSZ);
		UT_ASSERT_EQ(st.st_mode & 0777, 0600);
		fd = open(path, O_RDONLY);
		UT_ASSERT(fd >= 0);
		UT_ASSERT_EQ(read(fd, restored.data, BLCKSZ), BLCKSZ);
		UT_ASSERT_EQ(close(fd), 0);
		UT_ASSERT_EQ(memcmp(restored.data, failed.data, BLCKSZ), 0);
		UT_ASSERT_EQ(unlink(path), 0);
		captures++;
	}
	UT_ASSERT_EQ(closedir(dir), 0);
	UT_ASSERT_EQ(captures, 2);
	UT_ASSERT_EQ(rmdir(statdir), 0);
	errno = EDOM;
	cluster_block_io_trace_failed(tag, failed.data, "WIRE", 1, 2);
	UT_ASSERT_EQ(errno, EDOM);
	UT_ASSERT(strstr(trace_line, "bytes=0") != NULL);
	UT_ASSERT_EQ(rmdir(root), 0);
	trace_data_dir = NULL;
	cluster_trace_block_io = false;
}

int
main(void)
{
	UT_PLAN(12);
	UT_RUN(test_typed_source_allows_disk_only_for_exact_disk_grant);
	UT_RUN(test_source_cannot_authorize_successor_or_active_delivery);
	UT_RUN(test_cancel_first_fill_retires_exact_reservation_before_io_cleanup);
	UT_RUN(test_unknown_delivery_cannot_use_generic_io_cleanup_as_proof);
	UT_RUN(test_native_first_fill_owners_four_results_and_local_retry);
	UT_RUN(test_first_fill_error_owner_orders_rollback_retirement_and_io_abort);
	UT_RUN(test_trace_is_default_off_and_captures_exact_failed_bytes_without_overwrite);
	UT_RUN(test_invalidate_sees_entire_first_fill_until_native_io_publication);
	UT_RUN(test_cancel_after_retry_retirement_still_finishes_native_io);
	UT_RUN(test_shutdown_retry_unwinds_only_after_exact_retirement);
	UT_RUN(test_retired_retry_reservation_can_ack_exact_revoke_without_clearing_io);
	UT_RUN(test_first_fill_consumes_only_proven_terminal_cover_and_keeps_pi);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
