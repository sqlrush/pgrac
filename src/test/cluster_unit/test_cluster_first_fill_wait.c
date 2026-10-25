/*-------------------------------------------------------------------------
 *
 * test_cluster_first_fill_wait.c
 *    Native IO episodes and first-fill continuation owners.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_first_fill_wait.c
 *
 * NOTES
 *    This is a pgrac-original file.
 *
 *-------------------------------------------------------------------------
 */
int first_fill_own_fixture_main(void);
#define main first_fill_own_fixture_main
#include "test_cluster_pcm_own.c"
#undef main

#include "cluster/cluster_lms.h"
#include "cluster/cluster_block_io_trace.h"
#include "cluster/cluster_gcs_block_dedup.h"
#include "storage/latch.h"

int cluster_gcs_block_retransmit_initial_backoff_ms = 100;
int cluster_gcs_block_retransmit_max_retries = 4;
int cluster_gcs_reply_timeout_ms = 5000;

static unsigned woke_workers, shipments, cancellations;
static uint64 wait_epoch = 1;
static TimestampTz wait_now = 10;
static bool wait_stop, serve_busy;
static unsigned latch_waits, cancel_after;
static void
wait_wakeup(int worker)
{
	woke_workers |= 1U << worker;
}
#define LockBufHdr transition_lock_header
#define LWLockAcquire transition_lock_acquire
#define LWLockRelease transition_lock_release
#define BufTableHashCode(tag) 0
#define BufMappingPartitionLock(hash) (&transition_mapping_lock)
#define BufTableLookup(tag, hash) 0
#define GetBufferDescriptor(id) transition_buf
#define cluster_lms_wakeup wait_wakeup
#include "test_cluster_first_fill_bufmgr.inc"
#undef GetBufferDescriptor
#undef BufTableLookup
#undef BufMappingPartitionLock
#undef BufTableHashCode
#undef LWLockRelease
#undef LWLockAcquire
#undef LockBufHdr

static void wait_serve(const ClusterICEnvelope *, const void *);
static void gcs_first_fill_master_tick(int worker pg_attribute_unused()) { }
static int wait_channel(void) { return 0; }
static void wait_check(void)
{
	if (cancel_after != 0 && latch_waits >= cancel_after)
		pg_re_throw();
}
static int
wait_latch(Latch *latch pg_attribute_unused(), int events,
	long ms, uint32 event pg_attribute_unused())
{
	UT_ASSERT(ms == 10 || ms == 1000);
	UT_ASSERT_EQ((events & WL_LATCH_SET) != 0, ms == 1000);
	latch_waits++;
	return WL_TIMEOUT;
}
static bool
gcs_block_pending_s_notify_action(const ClusterPcmReadGrantKey *key, int master, uint8 action)
{
	UT_ASSERT_EQ(action, GCS_BLOCK_DONE_PENDING_S_NACK);
	UT_ASSERT_EQ(key->requester_node, 1);
	UT_ASSERT_EQ(master, 2);
	cancellations++;
	return true;
}
static void gcs_block_forward_reply_immediate_deny(const GcsBlockForwardPayload *fwd)
{
	UT_ASSERT_EQ(fwd->master_node, 2);
}
#define GCS_FIRST_FILL_PARK_MAX 64
#define ClusterGcsBlock ((void *)1)
#define cluster_ic_tier1_my_data_channel wait_channel
#define GetCurrentTimestamp() wait_now
#define cluster_epoch_get_current() wait_epoch
#define cluster_normal_stop_requested() wait_stop
#define cluster_gcs_handle_block_forward_envelope wait_serve
#define cluster_block_io_trace_note(tag, event, request, io, id) ((void)0)
#define cluster_block_io_trace_source(source, phase) ((void)0)
#define cluster_pcm_lock_check_shutdown_wait wait_check
#define WaitLatch wait_latch
#define ResetLatch(latch) ((void)0)
#define MyLatch ((Latch *)1)
#define ereport(level, ...) pg_re_throw()
#include "test_cluster_first_fill_holder.inc"
#undef ereport
#undef MyLatch
#undef ResetLatch
#undef WaitLatch
#undef cluster_pcm_lock_check_shutdown_wait
#undef cluster_block_io_trace_source
#undef cluster_block_io_trace_note
#undef cluster_gcs_handle_block_forward_envelope
#undef cluster_normal_stop_requested
#undef cluster_epoch_get_current
#undef GetCurrentTimestamp
#undef cluster_ic_tier1_my_data_channel
#undef ClusterGcsBlock
#undef cluster_lms_wakeup

/* Transport/production boundary: the real continuation must call it once,
 * after IO. A conditional content refusal retains the same owner locally. */
static void
wait_serve(const ClusterICEnvelope *env, const void *payload)
{
	const GcsBlockForwardPayload *fwd = payload;
	UT_ASSERT_EQ(env->source_node_id, 2);
	UT_ASSERT(!gcs_first_fill_holder_park(fwd, false));
	if (serve_busy) {
		UT_ASSERT(gcs_first_fill_holder_park(fwd, true));
		return;
	}
	UT_ASSERT(!(pg_atomic_read_u32(&transition_buf->state) & BM_IO_IN_PROGRESS));
	shipments++;
}

static GcsBlockForwardPayload
wait_setup(BufferDesc *buf, ClusterPcmOwnEntry *entry)
{
	GcsBlockForwardPayload forward = {0};
	n_predecessor_fixture(buf, entry, BUF_TYPE_XCUR);
	buf->pcm_state = PCM_STATE_X;
	pg_atomic_write_u32(&buf->state, BM_TAG_VALID | BM_VALID | BM_DIRTY | BUF_REFCOUNT_ONE);
	transition_buf = buf;
	transition_mapping_held = transition_content_held = false;
	memset(gcs_first_fill_holder_parks, 0, sizeof(gcs_first_fill_holder_parks));
	shipments = cancellations = woke_workers = 0;
	wait_stop = serve_busy = false;
	wait_epoch = 1;
	wait_now = 10;
	forward.tag = buf->tag;
	forward.epoch = 1;
	forward.request_id = 71;
	forward.original_requester_node = 1;
	forward.requester_backend_id = 3;
	forward.master_node = 2;
	return forward;
}

UT_TEST(holder_parks_one_request_through_native_io_and_ships_on_completion)
{
	BufferDesc buf;
	ClusterPcmOwnEntry entry;
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	GcsBlockForwardPayload fwd = wait_setup(&buf, &entry);
	uint32 workers;
	UT_ASSERT_EQ(cluster_pcm_own_write_io_begin(0), CLUSTER_PCM_OWN_OK);
	pg_atomic_fetch_or_u32(&buf.state, BM_IO_IN_PROGRESS);
	UT_ASSERT(gcs_first_fill_holder_park(&fwd, false));
	for (int i = 0; i < 100; i++) {
		UT_ASSERT(gcs_first_fill_holder_park(&fwd, false));
		cluster_gcs_block_first_fill_tick();
	}
	/* A response/replay timer is not native write completion. */
	wait_now += INT64CONST(60000000);
	cluster_gcs_block_first_fill_tick();
	UT_ASSERT_EQ(shipments, 0);
	UT_ASSERT_EQ(cancellations, 0);
	UT_ASSERT_EQ(cluster_pcm_own_write_io_complete(0, &workers), CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(workers, 1);
	pg_atomic_fetch_and_u32(&buf.state, ~BM_IO_IN_PROGRESS);
	cluster_gcs_block_first_fill_tick();
	UT_ASSERT_EQ(shipments, 1);
	UT_ASSERT(!gcs_first_fill_holder_parks[0].in_use);
	cluster_gcs_block_first_fill_tick();
	UT_ASSERT_EQ(shipments, 1);
	ClusterPcmOwnArray = saved;
}

UT_TEST(completed_io_with_busy_native_content_retains_the_same_request)
{
	BufferDesc buf;
	ClusterPcmOwnEntry entry;
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	GcsBlockForwardPayload fwd = wait_setup(&buf, &entry);
	UT_ASSERT(gcs_first_fill_holder_park(&fwd, true));
	serve_busy = true;
	cluster_gcs_block_first_fill_tick();
	UT_ASSERT(gcs_first_fill_holder_parks[0].in_use);
	UT_ASSERT_EQ(shipments + cancellations, 0);
	serve_busy = false;
	wait_now += 10000;
	cluster_gcs_block_first_fill_tick();
	UT_ASSERT_EQ(shipments, 1);
	UT_ASSERT_EQ(cancellations, 0);
	ClusterPcmOwnArray = saved;
}

UT_TEST(stale_resident_membership_and_stop_retire_without_shipping)
{
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	for (int why = 0; why < 4; why++) {
		BufferDesc buf;
		ClusterPcmOwnEntry entry;
		GcsBlockForwardPayload fwd = wait_setup(&buf, &entry);
		UT_ASSERT(gcs_first_fill_holder_park(&fwd, true));
		if (why == 0) buf.tag.blockNum++;
		if (why == 1) pg_atomic_fetch_add_u64(&entry.generation, 1);
		if (why == 2) wait_epoch++;
		if (why == 3) wait_stop = true;
		cluster_gcs_block_first_fill_tick();
		UT_ASSERT_EQ(shipments, 0);
		UT_ASSERT_EQ(cancellations, 1);
		UT_ASSERT(!gcs_first_fill_holder_parks[0].in_use);
	}
	ClusterPcmOwnArray = saved;
}

UT_TEST(holder_capacity_refuses_new_work_without_overwriting_the_live_request)
{
	BufferDesc buf;
	ClusterPcmOwnEntry entry;
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	GcsBlockForwardPayload fwd = wait_setup(&buf, &entry);
	for (unsigned i = 0; i < 64; i++) {
		fwd.request_id = 71 + i;
		UT_ASSERT(gcs_first_fill_holder_park(&fwd, true));
	}
	fwd.request_id = 900;
	UT_ASSERT(gcs_first_fill_holder_park(&fwd, true));
	UT_ASSERT_EQ(cancellations, 1);
	UT_ASSERT_EQ(gcs_first_fill_holder_parks[0].forward.request_id, 71);
	UT_ASSERT_EQ(gcs_first_fill_holder_parks[63].forward.request_id, 134);
	UT_ASSERT_EQ(shipments, 0);
	ClusterPcmOwnArray = saved;
}

UT_TEST(requester_retry_requires_a_specific_event_and_cannot_create_third_attempt)
{
	ClusterPcmReadSource source = {0};
	volatile bool caught = false;
	latch_waits = cancel_after = 0;
	PG_TRY(); { cluster_gcs_block_read_source_retry_wait(&source, 0); }
	PG_CATCH(); { caught = true; } PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(latch_waits, 0);
	source.retry_event = source.quiescent = true;
	source.request_epoch = wait_epoch = 1;
	cluster_gcs_block_read_source_retry_wait(&source, 0);
	UT_ASSERT_EQ(latch_waits, 1);
	caught = false;
	cancel_after = 4;
	PG_TRY(); { cluster_gcs_block_read_source_retry_wait(&source, 1); }
	PG_CATCH(); { caught = true; } PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(latch_waits, 4);
	cancel_after = 0;
}

UT_TEST(native_output_completion_wakes_only_its_registered_workers)
{
	ClusterPcmOwnEntry entry = {0};
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	uint32 workers = 99;
	ClusterPcmOwnArray = &entry;
	UT_ASSERT_EQ(cluster_pcm_own_write_io_begin(0), CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(pg_atomic_read_u64(&entry.write_io_generation), 1);
	UT_ASSERT_EQ(cluster_pcm_own_write_io_begin(0), CLUSTER_PCM_OWN_BUSY);
	entry.service_waiters = 5;
	UT_ASSERT_EQ(cluster_pcm_own_write_io_complete(0, &workers), CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(workers, 5);
	UT_ASSERT_EQ(entry.service_waiters, 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(&entry.write_io_generation), 2);
	UT_ASSERT_EQ(cluster_pcm_own_write_io_complete(0, &workers), CLUSTER_PCM_OWN_STALE);
	UT_ASSERT_EQ(workers, 0);
	UT_ASSERT_EQ(cluster_pcm_own_write_io_begin(0), CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(pg_atomic_read_u64(&entry.write_io_generation), 3);
	UT_ASSERT_EQ(cluster_pcm_own_write_io_complete(0, &workers), CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(workers, 0);
	ClusterPcmOwnArray = saved;
}

UT_TEST(native_output_episode_never_wraps_into_an_old_write)
{
	ClusterPcmOwnEntry entry = {0};
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	ClusterPcmOwnArray = &entry;
	pg_atomic_write_u64(&entry.write_io_generation, UINT64_MAX - 1);
	UT_ASSERT_EQ(cluster_pcm_own_write_io_begin(0), CLUSTER_PCM_OWN_EXHAUSTED);
	UT_ASSERT_EQ(pg_atomic_read_u64(&entry.write_io_generation), UINT64_MAX - 1);
	ClusterPcmOwnArray = saved;
}

UT_TEST(native_io_completion_and_abort_both_wake_the_exact_continuation)
{
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	for (int abort_write = 0; abort_write <= 1; abort_write++) {
		BufferDesc buf;
		ClusterPcmOwnEntry entry;
		GcsBlockForwardPayload fwd = wait_setup(&buf, &entry);
		transition_owned_io = transition_io_wakes = native_service_wake_workers = 0;
		UT_ASSERT(StartBufferIO(&buf, false));
		UT_ASSERT_EQ(transition_owned_io, 1);
		UT_ASSERT(gcs_first_fill_holder_park(&fwd, false));
		if (abort_write)
			transition_production_abort(BufferDescriptorGetBuffer(&buf));
		else
			TerminateBufferIO(&buf, true, 0);
		UT_ASSERT_EQ(transition_owned_io, 0);
		UT_ASSERT_EQ(transition_io_wakes, 1);
		UT_ASSERT_EQ(native_service_wake_workers, 1);
		UT_ASSERT_EQ(entry.service_waiters, 0);
		UT_ASSERT_EQ(pg_atomic_read_u64(&entry.write_io_generation), 2);
		UT_ASSERT(!(pg_atomic_read_u32(&buf.state) & BM_IO_IN_PROGRESS));
		cluster_gcs_block_first_fill_tick();
		UT_ASSERT_EQ(shipments, abort_write ? 0 : 1);
		UT_ASSERT_EQ(cancellations, abort_write ? 1 : 0);
	}
	{
		BufferDesc buf;
		ClusterPcmOwnEntry entry;
		(void)wait_setup(&buf, &entry);
		pg_atomic_write_u32(&buf.state, BM_TAG_VALID);
		UT_ASSERT(StartBufferIO(&buf, true));
		UT_ASSERT_EQ(pg_atomic_read_u64(&entry.write_io_generation), 0);
		TerminateBufferIO(&buf, false, BM_VALID);
		UT_ASSERT_EQ(pg_atomic_read_u64(&entry.write_io_generation), 0);
		UT_ASSERT_EQ(transition_owned_io, 0);
	}
	ClusterPcmOwnArray = saved;
}

int
main(void)
{
	UT_PLAN(8);
	UT_RUN(holder_parks_one_request_through_native_io_and_ships_on_completion);
	UT_RUN(completed_io_with_busy_native_content_retains_the_same_request);
	UT_RUN(stale_resident_membership_and_stop_retire_without_shipping);
	UT_RUN(holder_capacity_refuses_new_work_without_overwriting_the_live_request);
	UT_RUN(requester_retry_requires_a_specific_event_and_cannot_create_third_attempt);
	UT_RUN(native_output_completion_wakes_only_its_registered_workers);
	UT_RUN(native_output_episode_never_wraps_into_an_old_write);
	UT_RUN(native_io_completion_and_abort_both_wake_the_exact_continuation);
	UT_DONE();
	return ut_failed_count != 0;
}
