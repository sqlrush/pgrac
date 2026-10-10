/*-------------------------------------------------------------------------
 *
 * test_cluster_maintenance_read.c
 *    Execute the native conditional current-image maintenance reader.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_maintenance_read.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_pcm_x_bufmgr.h"
#include "storage/bufmgr.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "assertion failed: %s at %s:%d\n", condition, file, line);
	abort();
}

static BufferDescPadded descriptors[1];
BufferDescPadded *BufferDescriptors = descriptors;
int NBuffers = 1, NLocBuffer;
bool cluster_shared_config = true;
bool cluster_gcs_block_local_cache = true;

static bool pinned, active, tracked, available, held, gate_available;
static unsigned acquires, releases, snapshots;
static ClusterPcmOwnSnapshot current;
static ResourceXGateSnapshot gate;
static ClusterPcmOwnResult snapshot_result;
static void (*on_acquire)(void);
static uint64 metric_counts[PCM_RX_METRIC_COUNT];
static const char *const metric_keys[] = {
#define MAINTENANCE_METRIC_KEY(id, key) key,
	PCM_RX_METRICS(MAINTENANCE_METRIC_KEY)
#undef MAINTENANCE_METRIC_KEY
};

/* Observe the reader's counter API calls; the shared atomic store is
 * owned and tested by the existing PCM metrics implementation. */
void
cluster_pcm_rx_metric_note(PcmRxMetric metric)
{
	UT_ASSERT((unsigned)metric < PCM_RX_METRIC_COUNT);
	metric_counts[metric]++;
}

static uint64
metric_count(const char *key)
{
	for (unsigned i = 0; i < lengthof(metric_keys); i++)
		if (strcmp(metric_keys[i], key) == 0)
			return metric_counts[i];
	return 0;
}

static bool
fixture_acquire(LWLock *lock, LWLockMode mode)
{
	UT_ASSERT(lock == BufferDescriptorGetContentLock(&descriptors[0].bufferdesc));
	UT_ASSERT_EQ(mode, LW_SHARED);
	UT_ASSERT(!held);
	acquires++;
	if (!available)
		return false;
	held = true;
	if (on_acquire != NULL)
		on_acquire();
	return true;
}

static void
fixture_release(LWLock *lock)
{
	UT_ASSERT(lock == BufferDescriptorGetContentLock(&descriptors[0].bufferdesc));
	UT_ASSERT(held);
	held = false;
	releases++;
}

static ClusterPcmOwnResult
fixture_snapshot(BufferDesc *buf, ClusterPcmOwnSnapshot *out)
{
	UT_ASSERT(buf == &descriptors[0].bufferdesc);
	UT_ASSERT(held);
	snapshots++;
	*out = current;
	return snapshot_result;
}

static bool
fixture_gate(ResourceXGateSnapshot *out)
{
	UT_ASSERT(held);
	*out = gate;
	return gate_available;
}

/* Only lock/descriptor/admission boundaries are fixtures. The reader body
 * and the cached-cover predicate it calls are the actual product code. */
#undef BufferIsPinned
#define BufferIsPinned(buffer) ((void)(buffer), pinned)
#define cluster_pcm_is_active() active
#define cluster_bufmgr_should_pcm_track(buf) ((void)(buf), tracked)
#define cluster_bufmgr_pcm_own_snapshot fixture_snapshot
#define cluster_pcm_lock_resource_x_gate_snapshot fixture_gate
#define LWLockConditionalAcquire fixture_acquire
#define LWLockRelease fixture_release
#include "test_cluster_maintenance_read.inc"

static void
reset(void)
{
	memset(&current, 0, sizeof(current));
	memset(&gate, 0, sizeof(gate));
	memset(metric_counts, 0, sizeof(metric_counts));
	current.semantic_buf_state = BM_TAG_VALID | BM_VALID;
	current.generation = 42;
	current.reservation_token = 9;
	current.pcm_state = PCM_STATE_S;
	current.buffer_type = BUF_TYPE_CURRENT;
	gate.phase = RESOURCE_X_GATE_OPEN;
	pinned = active = tracked = available = gate_available = true;
	cluster_shared_config = cluster_gcs_block_local_cache = true;
	held = false;
	acquires = releases = snapshots = 0;
	snapshot_result = CLUSTER_PCM_OWN_OK;
	on_acquire = NULL;
}

UT_TEST(test_current_s_and_x_are_read_without_writer_or_reservation)
{
	for (unsigned i = 0; i < 2; i++) {
		ClusterPcmOwnSnapshot before;

		reset();
		current.pcm_state = i == 0 ? PCM_STATE_S : PCM_STATE_X;
		current.semantic_buf_state |= BM_DIRTY;
		before = current;
		UT_ASSERT(ClusterLockBufferShareIfCovered(1));
		UT_ASSERT(held && pinned);
		UT_ASSERT_EQ(acquires, 1);
		UT_ASSERT_EQ(releases, 0);
		UT_ASSERT_EQ(snapshots, 1);
		UT_ASSERT(memcmp(&before, &current, sizeof(current)) == 0);
		fixture_release(BufferDescriptorGetContentLock(&descriptors[0].bufferdesc));
	}
}

UT_TEST(test_invalid_local_unpinned_and_inactive_do_not_acquire)
{
	for (unsigned i = 0; i < 7; i++) {
		Buffer buffer = 1;

		reset();
		switch (i) {
		case 0:
			buffer = InvalidBuffer;
			break;
		case 1:
			buffer = -1;
			NLocBuffer = 1;
			break;
		case 2:
			pinned = false;
			break;
		case 3:
			active = false;
			break;
		case 4:
			tracked = false;
			break;
		case 5:
			cluster_shared_config = false;
			break;
		case 6:
			cluster_gcs_block_local_cache = false;
			break;
		}
		UT_ASSERT(!ClusterLockBufferShareIfCovered(buffer));
		UT_ASSERT(!held);
		UT_ASSERT_EQ(acquires, 0);
		UT_ASSERT_EQ(snapshots, 0);
	}
}

UT_TEST(test_busy_content_has_no_wait_or_owner)
{
	reset();
	available = false;
	UT_ASSERT(!ClusterLockBufferShareIfCovered(1));
	UT_ASSERT(pinned && !held);
	UT_ASSERT_EQ(acquires, 1);
	UT_ASSERT_EQ(releases, 0);
	UT_ASSERT_EQ(snapshots, 0);
}

UT_TEST(test_unproven_current_images_release_only_content_lock)
{
	for (unsigned i = 0; i < 13; i++) {
		reset();
		switch (i) {
		case 0:
			snapshot_result = CLUSTER_PCM_OWN_NOT_READY;
			break;
		case 1:
			current.buffer_type = BUF_TYPE_CR;
			break;
		case 2:
			current.buffer_type = BUF_TYPE_PI;
			break;
		case 3:
			current.semantic_buf_state &= ~BM_VALID;
			break;
		case 4:
			current.semantic_buf_state &= ~BM_TAG_VALID;
			break;
		case 5:
			current.semantic_buf_state |= BM_IO_IN_PROGRESS;
			break;
		case 6:
			current.semantic_buf_state |= BM_IO_ERROR;
			break;
		case 7:
			current.pcm_state = PCM_STATE_N;
			break;
		case 8:
			current.pcm_state = PCM_STATE_READ_IMAGE;
			break;
		case 9:
			current.flags = PCM_OWN_FLAG_REVOKING;
			break;
		case 10:
			current.flags = PCM_OWN_FLAG_GRANT_PENDING;
			break;
		case 11:
			current.writer_activation_token = 5;
			break;
		case 12:
			current.resource_x_activation_generation = 6;
			break;
		}
		UT_ASSERT(!ClusterLockBufferShareIfCovered(1));
		UT_ASSERT(pinned && !held);
		UT_ASSERT_EQ(acquires, 1);
		UT_ASSERT_EQ(releases, 1);
	}
}

UT_TEST(test_unknown_or_closed_gate_retains_original_pin)
{
	for (unsigned i = 0; i < 2; i++) {
		reset();
		if (i == 0)
			gate_available = false;
		else
			gate.phase = RESOURCE_X_GATE_FROZEN;
		UT_ASSERT(!ClusterLockBufferShareIfCovered(1));
		UT_ASSERT(pinned && !held);
		UT_ASSERT_EQ(releases, 1);
	}
}

static void
revoke_before_content(void)
{
	current.generation++;
	current.flags = PCM_OWN_FLAG_REVOKING;
}

UT_TEST(test_transition_before_content_is_sampled_and_refused)
{
	reset();
	on_acquire = revoke_before_content;
	UT_ASSERT(!ClusterLockBufferShareIfCovered(1));
	UT_ASSERT_EQ(current.generation, 43);
	UT_ASSERT_EQ(snapshots, 1);
	UT_ASSERT(pinned && !held);
}

UT_TEST(test_covered_reads_count_each_s_and_x_hit_once)
{
	reset();
	UT_ASSERT(ClusterLockBufferShareIfCovered(1));
	fixture_release(BufferDescriptorGetContentLock(&descriptors[0].bufferdesc));
	current.pcm_state = PCM_STATE_X;
	UT_ASSERT(ClusterLockBufferShareIfCovered(1));
	fixture_release(BufferDescriptorGetContentLock(&descriptors[0].bufferdesc));
	UT_ASSERT_EQ(metric_count("maintenance_read_share_hit_count"), 2);
	UT_ASSERT_EQ(metric_count("maintenance_read_x_fallback_count"), 0);
}

UT_TEST(test_every_refusal_counts_one_fallback_without_claiming_x_success)
{
	for (unsigned i = 0; i < 5; i++) {
		Buffer buffer = 1;

		reset();
		switch (i) {
		case 0:
			buffer = InvalidBuffer;
			break;
		case 1:
			available = false;
			break;
		case 2:
			current.pcm_state = PCM_STATE_N;
			break;
		case 3:
			gate.phase = RESOURCE_X_GATE_FROZEN;
			break;
		case 4:
			gate_available = false;
			break;
		}
		UT_ASSERT(!ClusterLockBufferShareIfCovered(buffer));
		UT_ASSERT(!held && pinned);
		UT_ASSERT_EQ(metric_count("maintenance_read_share_hit_count"), 0);
		UT_ASSERT_EQ(metric_count("maintenance_read_x_fallback_count"), 1);
	}
}

int
main(void)
{
	UT_PLAN(8);
	UT_RUN(test_current_s_and_x_are_read_without_writer_or_reservation);
	UT_RUN(test_invalid_local_unpinned_and_inactive_do_not_acquire);
	UT_RUN(test_busy_content_has_no_wait_or_owner);
	UT_RUN(test_unproven_current_images_release_only_content_lock);
	UT_RUN(test_unknown_or_closed_gate_retains_original_pin);
	UT_RUN(test_transition_before_content_is_sampled_and_refused);
	UT_RUN(test_covered_reads_count_each_s_and_x_hit_once);
	UT_RUN(test_every_refusal_counts_one_fallback_without_claiming_x_success);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
