/*-------------------------------------------------------------------------
 * test_cluster_relmap_lock.c
 *    Exercise the original relmap singleton across its GES boundaries.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <setjmp.h>

#include "../../backend/cluster/cluster_relmap_lock.c"

#undef printf
#include "unit_test.h"

UT_DEFINE_GLOBALS();

int cluster_ges_request_timeout_ms = 1234;
static ClusterLockAcquireResult acquire_result, promote_result, release_result;
static ClusterLockAcquireRequest granted_request;
static unsigned int acquire_calls, promote_calls, release_calls;
static bool release_error, release_nested, expect_error;
static int error_level;
static sigjmp_buf error_target;

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	printf("# unexpected Assert: %s at %s:%d\n", condition, file, line);
	if (expect_error)
	{
		error_level = FATAL;
		siglongjmp(error_target, 1);
	}
	abort();
}

bool
errstart(int level, const char *domain pg_attribute_unused())
{
	error_level = level;
	if (level >= ERROR && !expect_error)
		abort();
	return level >= ERROR;
}

bool
errstart_cold(int level, const char *domain)
{
	return errstart(level, domain);
}

void
errfinish(const char *file pg_attribute_unused(), int line pg_attribute_unused(),
		  const char *function pg_attribute_unused())
{
	if (error_level >= ERROR)
		siglongjmp(error_target, 1);
}

int
errmsg(const char *format pg_attribute_unused(), ...)
{
	return 0;
}

TimestampTz
GetCurrentTimestamp(void)
{
	return INT64CONST(987654000);
}

ClusterLockAcquireResult
cluster_lock_acquire_seven_step(const ClusterLockAcquireRequest *request)
{
	ClusterLockAcquireRequest *mutable_request = (ClusterLockAcquireRequest *) request;

	acquire_calls++;
	UT_ASSERT_EQ(request->resid.type, CLUSTER_RELMAP_RESID_TYPE);
	UT_ASSERT_EQ(request->resid.field1, 0);
	UT_ASSERT_EQ(request->resid.field2, 0);
	UT_ASSERT_EQ(request->resid.field3, 0);
	UT_ASSERT_EQ(request->resid.field4, 0);
	UT_ASSERT_EQ(request->resid.lockmethodid, DEFAULT_LOCKMETHOD);
	UT_ASSERT_EQ(request->lockmode, ExclusiveLock);
	UT_ASSERT_EQ(request->op, CLUSTER_LOCK_OP_REQUEST);
	UT_ASSERT_EQ(request->current_mode, NoLock);
	UT_ASSERT_EQ(request->lockmethod_id, DEFAULT_LOCKMETHOD);
	UT_ASSERT(!request->dontwait && !request->sessionLock);
	UT_ASSERT_EQ(request->caller_local_start_ts_ms, 987654);
	UT_ASSERT_EQ(request->timeout_ms, cluster_ges_request_timeout_ms);
	UT_ASSERT_EQ(request->wait_event, WAIT_EVENT_CLUSTER_RELMAP_WRITE);
	/* Model the original acquire provider filling its exact holder identity. */
	mutable_request->holder.node_id = 3;
	mutable_request->holder.procno = 79;
	mutable_request->holder.cluster_epoch = 411;
	mutable_request->holder.request_id = 900 + acquire_calls;
	mutable_request->request_id = mutable_request->holder.request_id;
	mutable_request->master_gen_snapshot = 13;
	granted_request = *request;
	return acquire_result;
}

ClusterLockAcquireResult
cluster_lock_acquire_s5_promote(const ClusterLockAcquireRequest *request)
{
	promote_calls++;
	UT_ASSERT(memcmp(request, &granted_request, sizeof(*request)) == 0);
	return promote_result;
}

ClusterLockAcquireResult
cluster_lock_acquire_s6_release(const ClusterLockAcquireRequest *request)
{
	release_calls++;
	UT_ASSERT(cluster_relmap_authority_x_held());
	UT_ASSERT(memcmp(request, &granted_request, sizeof(*request)) == 0);
	if (release_error)
		ereport(ERROR, (errmsg("controlled release ERROR after entry")));
	if (release_nested)
	{
		if (release_calls > 1)
			ereport(ERROR, (errmsg("controlled nested release bound")));
		cluster_relmap_lock_abort_release();
	}
	return release_result;
}

static void
reset_process(void)
{
	/* An isolated test process, never a recovery/reset operation. */
	memset(&relmap_x_hold, 0, sizeof(relmap_x_hold));
	memset(&granted_request, 0, sizeof(granted_request));
	acquire_result = CLUSTER_LOCK_ACQUIRE_NEED_PG_NATIVE_LOCK;
	promote_result = release_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	acquire_calls = promote_calls = release_calls = 0;
	release_error = release_nested = expect_error = false;
	error_level = 0;
}

static int
release_attempt(bool aborting)
{
	error_level = 0;
	expect_error = true;
	if (sigsetjmp(error_target, 1) == 0)
	{
		if (aborting)
			cluster_relmap_lock_abort_release();
		else
			cluster_relmap_authority_x_unlock();
	}
	expect_error = false;
	return error_level;
}

UT_TEST(empty_release_is_noop)
{
	reset_process();
	UT_ASSERT_EQ(release_attempt(false), 0);
	UT_ASSERT_EQ(release_attempt(true), 0);
	UT_ASSERT_EQ(release_calls, 0);
	UT_ASSERT(!cluster_relmap_authority_x_held());
}

UT_TEST(native_release_keeps_original_path)
{
	for (int aborting = 0; aborting < 2; aborting++)
	{
		reset_process();
		acquire_result = CLUSTER_LOCK_ACQUIRE_OK_NATIVE;
		UT_ASSERT(cluster_relmap_authority_x_lock());
		UT_ASSERT(cluster_relmap_authority_x_held());
		UT_ASSERT_EQ(promote_calls, 0);
		UT_ASSERT_EQ(release_attempt(aborting), 0);
		UT_ASSERT(!cluster_relmap_authority_x_held());
		UT_ASSERT_EQ(release_attempt(!aborting), 0);
		UT_ASSERT_EQ(release_calls, 0);
	}
}

UT_TEST(confirmed_release_retires_only_original_hold)
{
	static const ClusterLockAcquireResult granted[] = {
		CLUSTER_LOCK_ACQUIRE_NEED_PG_NATIVE_LOCK,
		CLUSTER_LOCK_ACQUIRE_OK_GRANTED, CLUSTER_LOCK_ACQUIRE_OK_CONVERTED
	};

	for (size_t i = 0; i < lengthof(granted); i++)
		for (int aborting = 0; aborting < 2; aborting++)
		{
			reset_process();
			acquire_result = granted[i];
			UT_ASSERT(cluster_relmap_authority_x_lock());
			UT_ASSERT_EQ(promote_calls, 1);
			UT_ASSERT_EQ(release_attempt(aborting), 0);
			UT_ASSERT_EQ(release_calls, 1);
			UT_ASSERT(!cluster_relmap_authority_x_held());
			UT_ASSERT_EQ(release_attempt(!aborting), 0);
			UT_ASSERT_EQ(release_calls, 1);
		}
}

UT_TEST(unconfirmed_release_keeps_hold_and_cannot_retry)
{
	static const ClusterLockAcquireResult refused[] = {
		CLUSTER_LOCK_ACQUIRE_OK_CONVERTED, CLUSTER_LOCK_ACQUIRE_PENDING,
		CLUSTER_LOCK_ACQUIRE_OK_NATIVE, CLUSTER_LOCK_ACQUIRE_NEED_PG_NATIVE_LOCK,
		CLUSTER_LOCK_ACQUIRE_FAIL_LMS_UNAVAILABLE,
		CLUSTER_LOCK_ACQUIRE_FAIL_GRD_NOT_READY,
		CLUSTER_LOCK_ACQUIRE_FAIL_RESERVATION_FULL,
		CLUSTER_LOCK_ACQUIRE_FAIL_TIMEOUT, CLUSTER_LOCK_ACQUIRE_FAIL_DEADLOCK,
		CLUSTER_LOCK_ACQUIRE_FAIL_CANCEL, CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL,
		CLUSTER_LOCK_ACQUIRE_FAIL_LMD_WAIT_EDGE_FULL,
		CLUSTER_LOCK_ACQUIRE_FAIL_SHARD_REMASTERING,
		CLUSTER_LOCK_ACQUIRE_FAIL_STALE_GENERATION,
		CLUSTER_LOCK_ACQUIRE_FAIL_FEATURE_NOT_SUPPORTED,
		CLUSTER_LOCK_ACQUIRE_FAIL_ILLEGAL_CONVERT, CLUSTER_LOCK_ACQUIRE_NOT_AVAIL,
		(ClusterLockAcquireResult) 999
	};

	for (size_t i = 0; i < lengthof(refused); i++)
		for (int aborting = 0; aborting < 2; aborting++)
		{
			reset_process();
			UT_ASSERT(cluster_relmap_authority_x_lock());
			release_result = refused[i];
			UT_ASSERT_EQ(release_attempt(aborting), PANIC);
			UT_ASSERT(cluster_relmap_authority_x_held());
			UT_ASSERT_EQ(release_calls, 1);
			UT_ASSERT(memcmp(&relmap_x_hold.req, &granted_request,
						 sizeof(granted_request)) == 0);
			/* Even a later successful provider must never be called again. */
			release_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
			UT_ASSERT_EQ(release_attempt(!aborting), PANIC);
			UT_ASSERT_EQ(release_attempt(aborting), PANIC);
			UT_ASSERT(cluster_relmap_authority_x_held());
			UT_ASSERT_EQ(release_calls, 1);
		}
}

UT_TEST(release_error_preserves_abort_responsibility)
{
	for (int aborting = 0; aborting < 2; aborting++)
	{
		reset_process();
		UT_ASSERT(cluster_relmap_authority_x_lock());
		release_error = true;
		UT_ASSERT_EQ(release_attempt(aborting), ERROR);
		UT_ASSERT(cluster_relmap_authority_x_held());
		release_error = false;
		UT_ASSERT_EQ(release_attempt(true), PANIC);
		UT_ASSERT_EQ(release_attempt(false), PANIC);
		UT_ASSERT_EQ(release_calls, 1);
		UT_ASSERT(memcmp(&relmap_x_hold.req, &granted_request,
					 sizeof(granted_request)) == 0);
	}
}

UT_TEST(nested_release_never_enters_second_s6)
{
	reset_process();
	UT_ASSERT(cluster_relmap_authority_x_lock());
	release_nested = true;
	/* Bound the old implementation's recursion with the fixture's ERROR. */
	release_error = false;
	expect_error = true;
	if (sigsetjmp(error_target, 1) == 0)
		cluster_relmap_authority_x_unlock();
	expect_error = false;
	UT_ASSERT_EQ(error_level, PANIC);
	UT_ASSERT_EQ(release_calls, 1);
	UT_ASSERT(cluster_relmap_authority_x_held());
}

UT_TEST(uncertain_release_cannot_replace_original_request)
{
	reset_process();
	UT_ASSERT(cluster_relmap_authority_x_lock());
	release_error = true;
	UT_ASSERT_EQ(release_attempt(false), ERROR);
	/* The parent has only a debug Assert for this acquisition attempt. */
	expect_error = true;
	if (sigsetjmp(error_target, 1) == 0)
	{
		UT_ASSERT(!cluster_relmap_authority_x_lock());
	}
	else
		UT_ASSERT(false);
	expect_error = false;
	UT_ASSERT_EQ(acquire_calls, 1);
	UT_ASSERT_EQ(promote_calls, 1);
	UT_ASSERT(memcmp(&relmap_x_hold.req, &granted_request,
				 sizeof(granted_request)) == 0);
}

UT_TEST(rejected_acquisition_has_no_release)
{
	reset_process();
	acquire_result = CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;
	UT_ASSERT(!cluster_relmap_authority_x_lock());
	UT_ASSERT(!cluster_relmap_authority_x_held());
	UT_ASSERT_EQ(promote_calls, 0);
	UT_ASSERT_EQ(release_attempt(true), 0);
	UT_ASSERT_EQ(release_calls, 0);
	reset_process();
	promote_result = CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;
	UT_ASSERT(!cluster_relmap_authority_x_lock());
	UT_ASSERT(!cluster_relmap_authority_x_held());
	UT_ASSERT_EQ(promote_calls, 1);
	UT_ASSERT_EQ(release_attempt(false), 0);
	UT_ASSERT_EQ(release_calls, 0);
}

UT_TEST(confirmed_release_allows_next_lifecycle)
{
	reset_process();
	for (int i = 0; i < 4; i++)
	{
		UT_ASSERT(cluster_relmap_authority_x_lock());
		UT_ASSERT_EQ(granted_request.request_id, 901 + i);
		UT_ASSERT_EQ(release_attempt(i % 2), 0);
		UT_ASSERT(!cluster_relmap_authority_x_held());
	}
	UT_ASSERT_EQ(acquire_calls, 4);
	UT_ASSERT_EQ(promote_calls, 4);
	UT_ASSERT_EQ(release_calls, 4);
}

int
main(void)
{
	UT_PLAN(9);
	UT_RUN(empty_release_is_noop);
	UT_RUN(native_release_keeps_original_path);
	UT_RUN(confirmed_release_retires_only_original_hold);
	UT_RUN(unconfirmed_release_keeps_hold_and_cannot_retry);
	UT_RUN(release_error_preserves_abort_responsibility);
	UT_RUN(nested_release_never_enters_second_s6);
	UT_RUN(uncertain_release_cannot_replace_original_request);
	UT_RUN(rejected_acquisition_has_no_release);
	UT_RUN(confirmed_release_allows_next_lifecycle);
	UT_DONE();
	return ut_failed_count != 0;
}
