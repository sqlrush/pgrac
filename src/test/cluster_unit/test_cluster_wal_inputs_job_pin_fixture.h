/* Actual pin bodies; native, allocator and callback registration are fixture
 * leaves. The new production callback branch is exercised directly, without
 * claiming native ResourceOwner dispatch or a real service loop. */
#include "cluster/cluster_guc.h"
#include "utils/wait_event.h"
#include "test_cluster_wal_inputs_job_pin_types.inc"

static unsigned pin_native_refs, pin_native_calls, pin_fail_at, pin_throw_at;
static unsigned pin_registrations;
static LockAcquireResult pin_native_result = LOCKACQUIRE_OK;
static bool pin_bound_before_native;

void *palloc0(Size bytes);
void pfree(void *pointer);
void *
palloc0(Size bytes)
{
	return MemoryContextAllocZero(CurrentMemoryContext, bytes);
}
void
pfree(void *pointer)
{
	JobFixtureAllocation **next = &job_fixture_allocations;

	while (*next != NULL) {
		JobFixtureAllocation *entry = *next;

		if (entry->allocation == pointer) {
			*next = entry->next;
			free(entry);
			break;
		}
		next = &entry->next;
	}
	free(pointer);
}

LockAcquireResult
LockAcquire(const LOCKTAG *tag, LOCKMODE mode, bool session, bool dontwait)
{
	(void)session;
	UT_ASSERT_EQ(tag->locktag_type, LOCKTAG_USERLOCK);
	UT_ASSERT_EQ(mode, ShareLock);
	UT_ASSERT(dontwait);
	++pin_native_calls;
	if (pin_native_result != LOCKACQUIRE_NOT_AVAIL)
		++pin_native_refs;
	if (pin_native_calls == 1)
		pin_bound_before_native = active_read_pin != NULL && active_read_pin->job != NULL
			&& active_read_pin->job->pin == active_read_pin
			&& active_read_pin->job->pin_generation == active_read_pin->generation;
	if (pin_native_calls == pin_fail_at)
		g_s5_result = CLUSTER_LOCK_ACQUIRE_NOT_AVAIL;
	if (pin_native_calls == pin_throw_at)
		throw_during_install = true;
	return pin_native_result;
}
bool
LockRelease(const LOCKTAG *tag, LOCKMODE mode, bool session)
{
	(void)session;
	UT_ASSERT_EQ(tag->locktag_type, LOCKTAG_USERLOCK);
	UT_ASSERT_EQ(mode, ShareLock);
	UT_ASSERT(pin_native_refs > 0);
	if (pin_native_refs == 0)
		return false;
	--pin_native_refs;
	return true;
}
ClusterLockAcquireResult
cluster_lock_acquire_s7_cleanup(const ClusterLockAcquireRequest *request)
{
	(void)request;
	return CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
}

/* Counts registration calls only; this leaf cannot confirm any cleanup. */
static void walr_resource_ensure_callback(void) { ++pin_registrations; }
#include "test_cluster_control_walr.inc"
#include "test_cluster_wal_inputs_job_pin_bodies.inc"
