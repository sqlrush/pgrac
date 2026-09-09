/*-------------------------------------------------------------------------
 * STOP-02 \u00a717.7 cache publication/invalidation interleaving tests.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <pthread.h>
#include <setjmp.h>
#include <stdarg.h>
#include <stdlib.h>
#include <unistd.h>

#include "cluster/cluster_epoch.h"
#include "cluster/cluster_lmon.h"
#include "cluster/cluster_membership.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_undo_horizon.h"
#include "cluster/cluster_write_fence.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/shmem.h"
#include "utils/elog.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"

#undef printf
#undef fprintf
#undef snprintf
#undef sprintf
#undef vsnprintf
#undef vfprintf
#undef vprintf
#undef vsprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

int CritSectionCount = 0;
int cluster_node_id = 0;
int cluster_write_fence_enforcement = CLUSTER_WRITE_FENCE_ENFORCE_ON;
int cluster_write_fence_lease_ms = 1000;
char *cluster_voting_disks = "mock";
static uint32 test_wait_event_info = 0;
uint32 *my_wait_event_info = &test_wait_event_info;

static const ClusterShmemRegion *fence_region;
static const ClusterShmemRegion *epoch_region;
static union {
	max_align_t align;
	uint8 bytes[4096];
} fence_shmem;
static union {
	max_align_t align;
	uint8 bytes[4096];
} epoch_shmem;
static bool fence_shmem_found;
static bool epoch_shmem_found;

#ifdef USE_ASSERT_CHECKING
static pthread_mutex_t race_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t race_cv = PTHREAD_COND_INITIALIZER;
static bool publisher_acquired;
static bool release_publisher;
static bool invalidator_started;
static bool invalidator_done;
#endif
static jmp_buf error_jump;
static bool capture_error;
static bool renew_at_error;
static int captured_level;
static int captured_code;
static char captured_detail[2048];
static ClusterFenceMarker renewal_marker;

void
ExceptionalCondition(const char *conditionName, const char *fileName, int lineNumber)
{
	printf("# Assert failed: %s at %s:%d\n", conditionName, fileName, lineNumber);
	abort();
}

void *
ShmemInitStruct(const char *name, Size size, bool *foundPtr)
{
	if (size > sizeof(fence_shmem.bytes))
		abort();
	if (strcmp(name, "pgrac cluster write fence") == 0) {
		*foundPtr = fence_shmem_found;
		fence_shmem_found = true;
		return fence_shmem.bytes;
	}
	if (strcmp(name, "pgrac cluster epoch") == 0) {
		*foundPtr = epoch_shmem_found;
		epoch_shmem_found = true;
		return epoch_shmem.bytes;
	}
	abort();
}

void
cluster_shmem_register_region(const ClusterShmemRegion *region)
{
	if (strcmp(region->name, "pgrac cluster write fence") == 0)
		fence_region = region;
	else if (strcmp(region->name, "pgrac cluster epoch") == 0)
		epoch_region = region;
}

void
on_shmem_exit(pg_on_exit_callback function, Datum arg)
{
	(void)function;
	(void)arg;
}

void
SetLatch(Latch *latch)
{
	(void)latch;
}

void
cluster_lmon_marker_complete_wakeup(void)
{}

bool
cluster_qvotec_in_quorum(void)
{
	return true;
}

void
cluster_undo_horizon_note_self_member(void)
{}

TimestampTz
GetCurrentTimestamp(void)
{
	return 100;
}

void
pg_usleep(long microsec)
{
	(void)usleep((useconds_t)microsec);
}

ClusterFenceAuthorityReadResult
cluster_write_fence_read_durable_authority(ClusterFenceAuthorityProof *out)
{
	(void)out;
	return CLUSTER_FENCE_AUTHORITY_NO_MAJORITY;
}

bool
errstart(int elevel, const char *domain)
{
	(void)domain;
	if (!capture_error)
		return false;
	captured_level = elevel;
	if (renew_at_error) {
		renew_at_error = false;
		cluster_write_fence_refresh_from_marker(&renewal_marker, 9000);
	}
	return true;
}

bool
errstart_cold(int elevel, const char *domain)
{
	return errstart(elevel, domain);
}

void
errfinish(const char *filename, int lineno, const char *funcname)
{
	(void)filename;
	(void)lineno;
	(void)funcname;
	if (capture_error)
		longjmp(error_jump, 1);
}

int
errcode(int sqlerrcode)
{
	captured_code = sqlerrcode;
	return 0;
}
int errmsg(const char *fmt, ...) { (void)fmt; return 0; }
int
errmsg_internal(const char *fmt, ...)
{
	(void)fmt;
	return 0;
}
int
errdetail(const char *fmt, ...)
{
	va_list args;
	va_start(args, fmt);
	vsnprintf(captured_detail, sizeof(captured_detail), fmt, args);
	va_end(args);
	return 0;
}
int errhint(const char *fmt, ...) { (void)fmt; return 0; }

static ClusterFenceMarker
cache_marker(uint64 event_id)
{
	ClusterFenceMarker marker;

	memset(&marker, 0, sizeof(marker));
	marker.magic = CLUSTER_FENCE_MARKER_MAGIC;
	marker.version = CLUSTER_FENCE_MARKER_VERSION;
	marker.fence_epoch = 8;
	marker.fence_event_id = event_id;
	marker.fence_generation = 2;
	marker.issuer_node_id = 1;
	marker.fenced_dead_bitmap[0] = 0x04;
	return marker;
}

static void
attach_cache(void)
{
	memset(&fence_shmem, 0, sizeof(fence_shmem));
	fence_shmem_found = false;
	fence_region = NULL;
	cluster_write_fence_shmem_register();
	if (fence_region == NULL)
		abort();
	fence_region->init_fn();
}

static void
attach_epoch(void)
{
	memset(&epoch_shmem, 0, sizeof(epoch_shmem));
	epoch_shmem_found = false;
	epoch_region = NULL;
	cluster_epoch_shmem_register();
	if (epoch_region == NULL)
		abort();
	epoch_region->init_fn();
}

#ifdef USE_ASSERT_CHECKING
static void
publisher_pause_hook(void)
{
	(void)pthread_mutex_lock(&race_lock);
	publisher_acquired = true;
	(void)pthread_cond_broadcast(&race_cv);
	while (!release_publisher)
		(void)pthread_cond_wait(&race_cv, &race_lock);
	(void)pthread_mutex_unlock(&race_lock);
}

static void
invalidator_start_hook(void)
{
	(void)pthread_mutex_lock(&race_lock);
	invalidator_started = true;
	(void)pthread_cond_broadcast(&race_cv);
	(void)pthread_mutex_unlock(&race_lock);
}

static void *
publisher_main(void *arg)
{
	ClusterFenceMarker *marker = (ClusterFenceMarker *)arg;
	uint64 expected_sequence = cluster_write_fence_authority_cache_sequence();

	(void)cluster_write_fence_authority_cache_publish_if_unchanged(
		marker, UINT64_C(1000000), expected_sequence);
	return NULL;
}

static void *
invalidator_main(void *arg)
{
	(void)arg;
	cluster_write_fence_authority_cache_invalidate();
	(void)pthread_mutex_lock(&race_lock);
	invalidator_done = true;
	(void)pthread_cond_broadcast(&race_cv);
	(void)pthread_mutex_unlock(&race_lock);
	return NULL;
}

#endif

UT_TEST(test_cache_publish_revalidate_and_invalidate)
{
	ClusterFenceMarker marker = cache_marker(UINT64_C(0xAA));
	uint64 expected_sequence;

	attach_cache();
	expected_sequence = cluster_write_fence_authority_cache_sequence();
	UT_ASSERT(cluster_write_fence_authority_cache_publish_if_unchanged(
		&marker, UINT64_C(1000000), expected_sequence));
	UT_ASSERT_EQ(cluster_write_fence_revalidate_cached_nowait(&marker, UINT64_C(1000000)),
				 CLUSTER_FENCE_CACHE_MATCH);
	cluster_write_fence_authority_cache_invalidate();
	UT_ASSERT_EQ(cluster_write_fence_revalidate_cached_nowait(&marker, UINT64_C(1000000)),
				 CLUSTER_FENCE_CACHE_INVALID);
}

UT_TEST(test_invalidation_rejects_late_prechange_proof)
{
	ClusterFenceMarker marker = cache_marker(UINT64_C(0xAA));
	uint64 prechange_sequence;

	attach_cache();
	prechange_sequence = cluster_write_fence_authority_cache_sequence();
	cluster_write_fence_authority_cache_invalidate();
	UT_ASSERT(!cluster_write_fence_authority_cache_publish_if_unchanged(
		&marker, UINT64_C(1000000), prechange_sequence));
	UT_ASSERT_EQ(cluster_write_fence_revalidate_cached_nowait(&marker, UINT64_C(1000000)),
				 CLUSTER_FENCE_CACHE_INVALID);
}

UT_TEST(test_mutation_guard_keeps_cache_unavailable_until_change_finishes)
{
	ClusterFenceMarker marker = cache_marker(UINT64_C(0xAA));
	uint64 expected_sequence;
	uint64 odd_sequence;

	attach_cache();
	expected_sequence = cluster_write_fence_authority_cache_sequence();
	UT_ASSERT(cluster_write_fence_authority_cache_publish_if_unchanged(
		&marker, UINT64_C(1000000), expected_sequence));
	odd_sequence = cluster_write_fence_authority_cache_mutation_begin();
	UT_ASSERT(odd_sequence != 0);
	UT_ASSERT_EQ(cluster_write_fence_revalidate_cached_nowait(&marker, UINT64_C(1000000)),
				 CLUSTER_FENCE_CACHE_UNAVAILABLE);
	cluster_write_fence_authority_cache_mutation_end(odd_sequence);
	UT_ASSERT_EQ(cluster_write_fence_revalidate_cached_nowait(&marker, UINT64_C(1000000)),
				 CLUSTER_FENCE_CACHE_INVALID);
}

/* These production interleaving hooks exist only in cassert builds. */
#ifdef USE_ASSERT_CHECKING
UT_TEST(test_invalidate_waits_out_preexisting_publisher)
{
	ClusterFenceMarker old_marker = cache_marker(UINT64_C(0xAA));
	pthread_t publisher;
	pthread_t invalidator;

	attach_cache();
	publisher_acquired = false;
	release_publisher = false;
	invalidator_started = false;
	invalidator_done = false;
	cluster_write_fence_cache_test_after_publish_acquire_hook = publisher_pause_hook;
	cluster_write_fence_cache_test_before_invalidate_acquire_hook = invalidator_start_hook;
	(void)pthread_create(&publisher, NULL, publisher_main, &old_marker);
	(void)pthread_mutex_lock(&race_lock);
	while (!publisher_acquired)
		(void)pthread_cond_wait(&race_cv, &race_lock);
	(void)pthread_mutex_unlock(&race_lock);
	(void)pthread_create(&invalidator, NULL, invalidator_main, NULL);
	(void)pthread_mutex_lock(&race_lock);
	while (!invalidator_started)
		(void)pthread_cond_wait(&race_cv, &race_lock);
	UT_ASSERT(!invalidator_done);
	release_publisher = true;
	(void)pthread_cond_broadcast(&race_cv);
	(void)pthread_mutex_unlock(&race_lock);
	(void)pthread_join(publisher, NULL);
	(void)pthread_join(invalidator, NULL);
	cluster_write_fence_cache_test_after_publish_acquire_hook = NULL;
	cluster_write_fence_cache_test_before_invalidate_acquire_hook = NULL;
	UT_ASSERT(invalidator_done);
	UT_ASSERT_NE(cluster_write_fence_revalidate_cached_nowait(&old_marker, UINT64_C(1000000)),
				 CLUSTER_FENCE_CACHE_MATCH);
}
#endif

UT_TEST(test_membership_mutation_invalidates_cache_before_change)
{
	ClusterFenceMarker marker = cache_marker(UINT64_C(0xAA));
	uint64 expected_sequence;

	attach_cache();
	cluster_membership_attach(NULL);
	expected_sequence = cluster_write_fence_authority_cache_sequence();
	UT_ASSERT(cluster_write_fence_authority_cache_publish_if_unchanged(
		&marker, UINT64_C(1000000), expected_sequence));
	UT_ASSERT_EQ(cluster_write_fence_revalidate_cached_nowait(&marker, UINT64_C(1000000)),
				 CLUSTER_FENCE_CACHE_MATCH);
	cluster_membership_set_state(3, CLUSTER_MEMBER_DEAD);
	UT_ASSERT_EQ(cluster_write_fence_revalidate_cached_nowait(&marker, UINT64_C(1000000)),
				 CLUSTER_FENCE_CACHE_INVALID);
}

UT_TEST(test_epoch_mutation_invalidates_cache_before_change)
{
	ClusterFenceMarker marker = cache_marker(UINT64_C(0xAA));
	uint64 old_epoch;
	uint64 new_epoch;
	uint64 expected_sequence;

	attach_cache();
	attach_epoch();
	expected_sequence = cluster_write_fence_authority_cache_sequence();
	UT_ASSERT(cluster_write_fence_authority_cache_publish_if_unchanged(
		&marker, UINT64_C(1000000), expected_sequence));
	cluster_epoch_advance_for_reconfig(&old_epoch, &new_epoch);
	UT_ASSERT_EQ(old_epoch, CLUSTER_EPOCH_INITIAL);
	UT_ASSERT_EQ(new_epoch, CLUSTER_EPOCH_INITIAL + 1);
	UT_ASSERT_EQ(cluster_write_fence_revalidate_cached_nowait(&marker, UINT64_C(1000000)),
				 CLUSTER_FENCE_CACHE_INVALID);
}

UT_TEST(test_external_fence_counters_are_exact_and_restart_empty)
{
	struct timespec now;
	uint64 age_ms = UINT64_MAX;
	uint64 sample;

	attach_cache();
	UT_ASSERT_EQ(cluster_write_fence_get_external_admit_requested(), 0);
	UT_ASSERT_EQ(cluster_write_fence_get_external_write_excluded(), 0);
	UT_ASSERT_EQ(cluster_write_fence_get_external_last_journal_seq(), 0);
	UT_ASSERT(!cluster_write_fence_get_external_last_proof_age_ms(&age_ms));
	UT_ASSERT_EQ(age_ms, 0);

	cluster_write_fence_note_external_admit_requested();
	cluster_write_fence_note_external_rejected();
	cluster_write_fence_note_external_unknown();
	cluster_write_fence_note_external_unavailable();
	cluster_write_fence_note_external_identity_mismatch();
	cluster_write_fence_note_external_expired();
	cluster_write_fence_note_external_daemon_disconnect();
	cluster_write_fence_note_external_mutation_gate_blocked();
	cluster_write_fence_note_external_publish_gate_blocked();
	UT_ASSERT_EQ(clock_gettime(CLOCK_MONOTONIC, &now), 0);
	sample = (uint64) now.tv_sec * UINT64_C(1000000000) +
		(uint64) now.tv_nsec;
	cluster_write_fence_note_external_write_excluded(41, sample);
	cluster_write_fence_note_external_write_excluded(40, sample - 1);

	UT_ASSERT_EQ(cluster_write_fence_get_external_admit_requested(), 1);
	UT_ASSERT_EQ(cluster_write_fence_get_external_write_excluded(), 2);
	UT_ASSERT_EQ(cluster_write_fence_get_external_rejected(), 1);
	UT_ASSERT_EQ(cluster_write_fence_get_external_unknown(), 1);
	UT_ASSERT_EQ(cluster_write_fence_get_external_unavailable(), 1);
	UT_ASSERT_EQ(cluster_write_fence_get_external_identity_mismatch(), 1);
	UT_ASSERT_EQ(cluster_write_fence_get_external_expired(), 1);
	UT_ASSERT_EQ(cluster_write_fence_get_external_daemon_disconnect(), 1);
	UT_ASSERT_EQ(cluster_write_fence_get_external_mutation_gate_blocked(), 1);
	UT_ASSERT_EQ(cluster_write_fence_get_external_publish_gate_blocked(), 1);
	UT_ASSERT_EQ(cluster_write_fence_get_external_last_journal_seq(), 41);
	UT_ASSERT(cluster_write_fence_get_external_last_proof_age_ms(&age_ms));
}

UT_TEST(test_passive_fence_snapshot_retains_expiry_without_mutation)
{
	ClusterWriteFenceObservation observed;
	ClusterFenceMarker marker;
	uint8 before[sizeof(fence_shmem.bytes)];
	void (*observe)(ClusterWriteFenceObservation *) = cluster_write_fence_observe;

	UT_ASSERT_NOT_NULL((void *)observe);
	if (observe == NULL)
		return;
	attach_cache();
	attach_epoch();
	marker = cache_marker(12);
	marker.fence_epoch = cluster_epoch_get_current();
	cluster_write_fence_refresh_from_marker(&marker, 99);
	memcpy(before, fence_shmem.bytes, sizeof(before));
	observe(&observed);
	UT_ASSERT(observed.attached && observed.enforcing && observed.engaged);
	UT_ASSERT(!observed.allowed && !observed.self_fenced);
	UT_ASSERT_EQ(observed.now_us, 100);
	UT_ASSERT_EQ(observed.expiry_us, 99);
	UT_ASSERT_EQ(observed.event_id, 12);
	UT_ASSERT_EQ(observed.last_refresh_us, 100);
	UT_ASSERT_EQ(memcmp(before, fence_shmem.bytes, sizeof(before)), 0);
	UT_ASSERT(!cluster_write_fence_allowed());
}

/* Real reject wrapper must retain the failed read, even if publication completes
 * at the subsequent error boundary. No copied authority or failure model. */
UT_TEST(test_denial_detail_keeps_original_expiry_after_renewal)
{
	attach_cache();
	attach_epoch();
	renewal_marker = cache_marker(10);
	renewal_marker.fence_epoch = cluster_epoch_get_current();
	cluster_write_fence_refresh_from_marker(&renewal_marker, 100);
	UT_ASSERT(!cluster_write_fence_allowed());
	captured_detail[0] = '\0';
	capture_error = renew_at_error = true;
	if (setjmp(error_jump) == 0)
		cluster_write_fence_reject_if_fenced("write");
	capture_error = false;
	UT_ASSERT_EQ(captured_level, ERROR);
	UT_ASSERT_EQ(captured_code, ERRCODE_CLUSTER_WRITE_FENCED);
	UT_ASSERT(cluster_write_fence_allowed());
	UT_ASSERT(strstr(captured_detail, "PGRAC_FAMILY=WRITE_FENCE") != NULL);
	UT_ASSERT(strstr(captured_detail, "PGRAC_REASON=LEASE_EXPIRED") != NULL);
	UT_ASSERT(strstr(captured_detail, "sampled_now_us=100 sampled_expiry_us=100") != NULL);
	UT_ASSERT(strstr(captured_detail, "sampled_expiry_us=9000") == NULL);
}

UT_TEST(test_critical_zero_lease_remains_panic_with_exact_reason)
{
	attach_cache();
	attach_epoch();
	renewal_marker = cache_marker(11);
	renewal_marker.fence_epoch = cluster_epoch_get_current();
	cluster_write_fence_refresh_from_marker(&renewal_marker, 0);
	captured_detail[0] = '\0';
	capture_error = true;
	renew_at_error = false;
	CritSectionCount = 1;
	if (setjmp(error_jump) == 0)
		cluster_write_fence_reject_if_fenced("write");
	capture_error = false;
	CritSectionCount = 0;
	UT_ASSERT_EQ(captured_level, PANIC);
	UT_ASSERT_EQ(captured_code, ERRCODE_CLUSTER_WRITE_FENCED);
	UT_ASSERT(strstr(captured_detail, "PGRAC_REASON=LEASE_ZERO") != NULL);
	UT_ASSERT(strstr(captured_detail, "sampled_expiry_us=0") != NULL);
	UT_ASSERT(!cluster_write_fence_allowed());
}

int
main(void)
{
#ifdef USE_ASSERT_CHECKING
	UT_PLAN(10);
#else
	UT_PLAN(9);
#endif
	UT_RUN(test_cache_publish_revalidate_and_invalidate);
#ifdef USE_ASSERT_CHECKING
	UT_RUN(test_invalidate_waits_out_preexisting_publisher);
#endif
	UT_RUN(test_invalidation_rejects_late_prechange_proof);
	UT_RUN(test_mutation_guard_keeps_cache_unavailable_until_change_finishes);
	UT_RUN(test_membership_mutation_invalidates_cache_before_change);
	UT_RUN(test_epoch_mutation_invalidates_cache_before_change);
	UT_RUN(test_external_fence_counters_are_exact_and_restart_empty);
	UT_RUN(test_passive_fence_snapshot_retains_expiry_without_mutation);
	UT_RUN(test_denial_detail_keeps_original_expiry_after_renewal);
	UT_RUN(test_critical_zero_lease_remains_panic_with_exact_reason);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
