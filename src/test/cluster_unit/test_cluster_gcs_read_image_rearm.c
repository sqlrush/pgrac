/*-------------------------------------------------------------------------
 *
 * test_cluster_gcs_read_image_rearm.c
 *    Exercise read-image misses through the original reservation owners.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_gcs_read_image_rearm.c
 *
 * NOTES
 *    Transport, clock and admission are controlled boundaries. The waiter,
 *    shared-acquire catch, rearm and publication bodies are production code;
 *    reservation atomics use the real pcm_own object. This is not a native
 *    LockBuffer, SQL, membership or network test.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "miscadmin.h"
#include "cluster/cluster_content_trace.h"
#include "cluster/cluster_pcm_x_bufmgr.h"
#include "cluster/cluster_shmem.h"

int read_image_trace_fixture_main(void);
#define main read_image_trace_fixture_main
#include "test_cluster_gcs_read_image_trace.c"
#undef main

int NBuffers = 1;
bool cluster_gcs_block_local_cache = true;
bool cluster_trace_tx_identity = false;
volatile sig_atomic_t InterruptPending = false;
volatile uint32 InterruptHoldoffCount = 0;
static ClusterPcmOwnEntry ownership;
static ClusterPcmOwnSnapshot base;
static uint64 token, covered_generation;
static bool gate_open, barrier_refused, cancel_on_yield;
static unsigned yields, wait_clears;

bool
cluster_pcm_lock_resource_x_gate_snapshot(ResourceXGateSnapshot *out)
{
	memset(out, 0, sizeof(*out));
	out->phase = gate_open ? RESOURCE_X_GATE_OPEN : RESOURCE_X_GATE_FROZEN;
	return true;
}

bool LWLockHeldByMe(LWLock *lock pg_attribute_unused()) { return lock_depth != 0; }
bool
LWLockAcquireOrWait(LWLock *lock pg_attribute_unused(), LWLockMode mode pg_attribute_unused())
{ abort(); }

void
ProcessInterrupts(void)
{
	InterruptPending = false;
	ereport(ERROR, (errcode(ERRCODE_QUERY_CANCELED), errmsg("controlled caller cancel")));
}

static inline int32 GetPrivateRefCount(Buffer buffer pg_attribute_unused()) { return 1; }
void
cluster_content_trace_wait_set(ClusterContentTraceWait kind pg_attribute_unused(),
							 const ClusterWaitTrace *wait pg_attribute_unused())
{ abort(); }
void
cluster_content_trace_wait_clear(ClusterContentTraceWait kind)
{ UT_ASSERT_EQ(kind, CLCONTENT_RETRY); wait_clears++; }

static void
cluster_pcm_own_report_bump_failure(BufferDesc *buf pg_attribute_unused(),
	ClusterPcmOwnResult result pg_attribute_unused(), uint64 generation pg_attribute_unused(),
	uint32 flags pg_attribute_unused(), const char *context pg_attribute_unused())
{ abort(); }
static void
cluster_bufmgr_resource_x_writer_report_failure(ResourceXApplyResult result pg_attribute_unused(),
	BufferDesc *buf pg_attribute_unused(), const char *context pg_attribute_unused())
{ abort(); }
static void
cluster_bufmgr_trace_cleanup(BufferDesc *buf pg_attribute_unused(),
	const char *stage pg_attribute_unused(), uint64 attempt pg_attribute_unused())
{}

bool
cluster_pcm_lock_acquire_buffer(BufferDesc *buf, PcmLockMode mode, bool *retry)
{
	UT_ASSERT_EQ(mode, PCM_LOCK_MODE_S);
	return cluster_gcs_local_master_read_image_and_wait(buf, &fixture_authority, true, retry);
}
ClusterPcmReadSourceKind
cluster_pcm_lock_acquire_buffer_source(BufferDesc *buf pg_attribute_unused(),
	ClusterPcmReadSource *source pg_attribute_unused())
{ abort(); }
ClusterPcmOwnResult
cluster_pcm_own_reclaim_read_image_exact(BufferDesc *buf pg_attribute_unused(),
	const ClusterPcmOwnSnapshot *snapshot pg_attribute_unused(),
	uint64 *generation pg_attribute_unused())
{ abort(); }

void *ShmemInitStruct(const char *name pg_attribute_unused(), Size size pg_attribute_unused(),
					 bool *found pg_attribute_unused()) { abort(); }
Size mul_size(Size a pg_attribute_unused(), Size b pg_attribute_unused()) { abort(); }
void cluster_shmem_register_region(const ClusterShmemRegion *region pg_attribute_unused())
{ abort(); }
void perform_spin_delay(SpinDelayStatus *status pg_attribute_unused()) { abort(); }
void finish_spin_delay(SpinDelayStatus *status pg_attribute_unused()) {}

#include "test_cluster_pcm_snapshot_owner.inc"
#define GetBufferDescriptor(id) ((void)(id), &fixture_buffer)
#include "test_cluster_gcs_read_rearm_owner.inc"
#include "test_cluster_read_image_publish_owner.inc"
#undef GetBufferDescriptor

static void
yield_observer(long timeout pg_attribute_unused(), uint32 event)
{
	UT_ASSERT_EQ(event, WAIT_EVENT_PCM_BLOCK_CONVERT_WAIT);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&fixture_buffer.state) & BM_LOCKED, 0);
	UT_ASSERT_EQ(lock_depth, 0);
	UT_ASSERT_EQ(installs, 0);
	yields++;
	if (cancel_on_yield)
		InterruptPending = true;
}

static void
setup_owner(void)
{
	ClusterPcmGrantBeginWaitReason reason;
	bool covered = false;

	reset_scenario(0, false);
	memset(&ownership, 0, sizeof(ownership));
	pg_atomic_init_u64(&ownership.generation, 5);
	pg_atomic_init_u64(&ownership.reservation_token, 7);
	ClusterPcmOwnArray = &ownership;
	fixture_buffer.pcm_state = PCM_STATE_N;
	fixture_buffer.buffer_type = BUF_TYPE_CURRENT;
	pg_atomic_init_u32(&fixture_buffer.state, BM_VALID | BM_TAG_VALID);
	gate_open = true;
	barrier_refused = cancel_on_yield = false;
	InterruptPending = false;
	yields = wait_clears = 0;
	covered_generation = 0;
	fixture_wait_hook = yield_observer;
	UT_ASSERT_EQ(cluster_pcm_own_begin_grant_reservation(&fixture_buffer, PCM_LOCK_MODE_S,
		&base, &token, &covered, &reason), CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(token, 8);
	UT_ASSERT(!covered);
}

/* Run the real catch even when the waiter terminates before returning. */
static bool
acquire(int selected)
{
	sigjmp_buf outer;
	bool granted = false;

	scenario = selected;
	caught_error = retry_denied = false;
	PG_exception_stack = &outer;
	if (sigsetjmp(outer, 0) == 0)
		granted = cluster_bufmgr_pcm_acquire_shared_owned(&fixture_buffer, &base, token,
			&retry_denied, NULL);
	else
		caught_error = true;
	PG_exception_stack = NULL;
	return granted;
}

static ClusterBufmgrPcmRetryRearmResult
rearm(void)
{
	sigjmp_buf outer;
	volatile ClusterBufmgrPcmRetryRearmResult result = CLUSTER_BUFMGR_PCM_RETRY_BARRIER_REFUSED;

	caught_error = false;
	PG_exception_stack = &outer;
	if (sigsetjmp(outer, 0) == 0)
		result = cluster_bufmgr_pcm_retry_denied_rearm(&fixture_buffer, PCM_LOCK_MODE_S,
			&base, &token, 0, &barrier_refused, &covered_generation);
	else
		caught_error = true;
	PG_exception_stack = NULL;
	return result;
}

UT_TEST(repeated_missing_reply_then_image_publishes_only_final_reservation)
{
	uint64 published = 0;

	setup_owner();
	for (int miss = 0; miss < 9; miss++) {
		UT_ASSERT(!acquire(0));
		UT_ASSERT(!caught_error && retry_denied);
		if (caught_error) return;
		UT_ASSERT_EQ(installs, 0);
		UT_ASSERT_EQ(releases, miss + 1);
		UT_ASSERT_EQ(token, 8 + miss);
		UT_ASSERT_EQ(rearm(), CLUSTER_BUFMGR_PCM_RETRY_REARMED);
		UT_ASSERT(!caught_error && !barrier_refused);
		UT_ASSERT_EQ(token, 9 + miss);
		UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), PCM_OWN_FLAG_GRANT_PENDING);
		UT_ASSERT_EQ(base.generation, 5);
	}
	UT_ASSERT(!acquire(1)); /* Non-durable image, not a durable S grant. */
	UT_ASSERT(!caught_error && !retry_denied);
	UT_ASSERT_EQ(installs, 1);
	UT_ASSERT_EQ(releases, 10);
	UT_ASSERT_EQ(last_forward.request_id, 60);
	UT_ASSERT_EQ(yields, 9);
	UT_ASSERT_EQ(wait_clears, 9);
	LWLockAcquire(BufferDescriptorGetContentLock(&fixture_buffer), LW_SHARED);
	UT_ASSERT_EQ(cluster_pcm_own_publish_read_image_exact(&fixture_buffer, &base, token,
		&published), CLUSTER_PCM_OWN_OK);
	LWLockRelease(BufferDescriptorGetContentLock(&fixture_buffer));
	UT_ASSERT_EQ(published, 6);
	UT_ASSERT_EQ(fixture_buffer.pcm_state, PCM_STATE_READ_IMAGE);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
}

UT_TEST(cancel_after_miss_retires_exact_reservation_before_interrupt)
{
	setup_owner();
	UT_ASSERT(!acquire(0));
	UT_ASSERT(!caught_error && retry_denied);
	if (caught_error) return;
	cancel_on_yield = true;
	(void)rearm();
	UT_ASSERT(caught_error);
	UT_ASSERT_EQ(error_code, ERRCODE_QUERY_CANCELED);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_token_get(0), 8);
	UT_ASSERT_EQ(releases, 1);
	UT_ASSERT_EQ(installs, 0);
	UT_ASSERT_EQ(yields, 1);
	UT_ASSERT_EQ(wait_clears, 1);
}

UT_TEST(closed_gate_after_miss_does_not_rearm_or_install)
{
	setup_owner();
	UT_ASSERT(!acquire(0));
	UT_ASSERT(!caught_error && retry_denied);
	if (caught_error) return;
	gate_open = false;
	UT_ASSERT_EQ(rearm(), CLUSTER_BUFMGR_PCM_RETRY_BARRIER_REFUSED);
	UT_ASSERT(!caught_error && barrier_refused);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_token_get(0), 8);
	UT_ASSERT_EQ(yields, 0);
	UT_ASSERT_EQ(installs, 0);
}

UT_TEST(stale_abort_preserves_successor_cover_after_miss)
{
	ClusterPcmOwnEntry before;

	setup_owner();
	UT_ASSERT(!acquire(0));
	UT_ASSERT(!caught_error && retry_denied);
	if (caught_error) return;
	pg_atomic_write_u64(&ownership.generation, 6);
	pg_atomic_write_u64(&ownership.reservation_token, 10);
	pg_atomic_write_u32(&ownership.flags, 0);
	fixture_buffer.pcm_state = PCM_STATE_S;
	memcpy(&before, &ownership, sizeof(before));
	UT_ASSERT_EQ(rearm(), CLUSTER_BUFMGR_PCM_RETRY_COVERED);
	UT_ASSERT(!caught_error);
	UT_ASSERT_EQ(covered_generation, 6);
	UT_ASSERT_EQ(memcmp(&before, &ownership, sizeof(before)), 0);
	UT_ASSERT_EQ(yields, 0);
	UT_ASSERT_EQ(installs, 0);
}

UT_TEST(dead_holder_error_cleans_original_shared_reservation)
{
	setup_owner();
	fixture_holder_state = CLUSTER_CSSD_PEER_DEAD;
	UT_ASSERT(!acquire(0));
	UT_ASSERT(caught_error && !retry_denied);
	UT_ASSERT_EQ(error_code, ERRCODE_FEATURE_NOT_SUPPORTED);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
	UT_ASSERT_EQ(installs, 0);
	UT_ASSERT_EQ(releases, 1);
}

UT_TEST(checksum_failure_after_retry_cleans_only_rearmed_reservation)
{
	setup_owner();
	UT_ASSERT(!acquire(0));
	UT_ASSERT(!caught_error && retry_denied);
	if (caught_error) return;
	UT_ASSERT_EQ(rearm(), CLUSTER_BUFMGR_PCM_RETRY_REARMED);
	UT_ASSERT(!acquire(2));
	UT_ASSERT(caught_error && !retry_denied);
	UT_ASSERT_EQ(error_code, ERRCODE_FEATURE_NOT_SUPPORTED);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_token_get(0), 9);
	UT_ASSERT_EQ(installs, 0);
	UT_ASSERT_EQ(releases, 2);
}

int
main(void)
{
	UT_PLAN(6);
	UT_RUN(repeated_missing_reply_then_image_publishes_only_final_reservation);
	UT_RUN(cancel_after_miss_retires_exact_reservation_before_interrupt);
	UT_RUN(closed_gate_after_miss_does_not_rearm_or_install);
	UT_RUN(stale_abort_preserves_successor_cover_after_miss);
	UT_RUN(dead_holder_error_cleans_original_shared_reservation);
	UT_RUN(checksum_failure_after_retry_cleans_only_rearmed_reservation);
	UT_DONE();
	return ut_failed_count > 0 ? 1 : 0;
}
