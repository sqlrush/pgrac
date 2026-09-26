/*-------------------------------------------------------------------------
 * test_cluster_control_walr.c -- actual WALR wrappers and stable owners.
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * WALR bodies are extracted verbatim at build time. Stable owner/registry
 * objects are production C. Native lock, S1-S5 and transport remain explicit
 * fixture boundaries; this is not a network integration test.
 *-------------------------------------------------------------------------
 */
#define PGRAC_CONTROL_CF_EMBEDDED
#include "test_cluster_cf_enqueue.c"

static unsigned native_refs[MAX_LOCKMODES];
static LockAcquireResult native_result = LOCKACQUIRE_OK;

bool
errstart_cold(int elevel pg_attribute_unused(), const char *domain pg_attribute_unused())
{
	abort(); /* No FATAL/ERROR path is silently accepted. */
	return false;
}
int
errmsg_internal(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}
LockAcquireResult
LockAcquire(const LOCKTAG *tag, LOCKMODE mode, bool session pg_attribute_unused(),
			bool dontwait pg_attribute_unused())
{
	UT_ASSERT_EQ(tag->locktag_type, LOCKTAG_USERLOCK);
	if (native_result != LOCKACQUIRE_NOT_AVAIL)
		native_refs[mode]++;
	return native_result;
}
bool
LockRelease(const LOCKTAG *tag, LOCKMODE mode, bool session pg_attribute_unused())
{
	UT_ASSERT_EQ(tag->locktag_type, LOCKTAG_USERLOCK);
	UT_ASSERT(native_refs[mode] > 0);
	if (native_refs[mode] == 0)
		return false;
	native_refs[mode]--;
	return true;
}
ClusterLockAcquireResult
cluster_lock_acquire_s7_cleanup(const ClusterLockAcquireRequest *request pg_attribute_unused())
{
	return CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
}

#include "test_cluster_control_walr.inc"

static ClusterLockAcquireRequest
walr_fixture_request(void)
{
	ClusterLockAcquireRequest request = { 0 };

	request.resid.type = CLUSTER_WAL_RETENTION_RESID_TYPE;
	request.resid.lockmethodid = DEFAULT_LOCKMETHOD;
	request.resid.field1 = 2;
	request.lockmode = ShareLock;
	request.op = CLUSTER_LOCK_OP_REQUEST;
	request.dontwait = true;
	request.timeout_ms = 1;
	g_next_request_id++;
	g_seven_result = CLUSTER_LOCK_ACQUIRE_NEED_PG_NATIVE_LOCK;
	g_s5_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	return request;
}

UT_TEST(native_s_is_kept_until_exact_retirement)
{
	ClusterLockAcquireRequest request = walr_fixture_request();

	UT_ASSERT_EQ(walr_request_acquire_actual(&request), CLUSTER_LOCK_ACQUIRE_OK_GRANTED);
	UT_ASSERT_EQ(native_refs[ShareLock], 1);
	UT_ASSERT(cluster_lock_owner_request_usable(&request));
	UT_ASSERT_EQ(walr_request_release_actual(&request), CLUSTER_LOCK_ACQUIRE_PENDING);
	UT_ASSERT_EQ(native_refs[ShareLock], 1);
	UT_ASSERT(!cluster_lock_owner_request_usable(&request));
	UT_ASSERT(acknowledge_retirements());
	UT_ASSERT_EQ(walr_request_release_actual(&request), CLUSTER_LOCK_ACQUIRE_OK_GRANTED);
	UT_ASSERT_EQ(native_refs[ShareLock], 0);
}

UT_TEST(failed_native_publication_keeps_only_owned_cleanup)
{
	ClusterLockAcquireRequest request = walr_fixture_request();
	ClusterControlRequestView view;
	uint32 cursor = 0;
	uint64 enumerated;
	unsigned abandoned = 0;

	g_s5_result = CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;
	UT_ASSERT_EQ(walr_request_acquire_actual(&request), CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL);
	UT_ASSERT_EQ(native_refs[ShareLock], 0);
	memset(&request, 0, sizeof(request));
	while (cluster_control_request_next(&cursor, &view)) {
		UT_ASSERT_EQ(view.state, CLUSTER_CONTROL_REQUEST_ABANDONED);
		abandoned++;
	}
	UT_ASSERT_EQ(abandoned, 1);
	UT_ASSERT(acknowledge_retirements());
	UT_ASSERT(cluster_lock_owners_redeclare(&enumerated));
	UT_ASSERT_EQ(enumerated, 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&MyProc->cluster_grd_registered_count), 0);
}

UT_TEST(upgrade_uses_new_attempt_and_downgrade_keeps_confirmed_identity)
{
	ClusterLockAcquireRequest share = walr_fixture_request(), exclusive, down;
	bool cleanup = false;

	UT_ASSERT_EQ(walr_request_acquire_actual(&share), CLUSTER_LOCK_ACQUIRE_OK_GRANTED);
	exclusive = share;
	exclusive.op = CLUSTER_LOCK_OP_CONVERT;
	exclusive.lockmode = ExclusiveLock;
	exclusive.current_mode = ShareLock;
	exclusive.convert_old_request_id = share.request_id;
	g_s5_result = CLUSTER_LOCK_ACQUIRE_OK_CONVERTED;
	UT_ASSERT_EQ(walr_request_convert_actual(&exclusive, 0, &cleanup),
				 CLUSTER_LOCK_ACQUIRE_OK_CONVERTED);
	UT_ASSERT(!cleanup);
	UT_ASSERT(exclusive.request_id != share.request_id);
	UT_ASSERT_EQ(exclusive.control_owner_id, share.control_owner_id);
	UT_ASSERT(cluster_lock_owner_request_usable(&exclusive));
	UT_ASSERT(!cluster_lock_owner_request_usable(&share));
	UT_ASSERT(acknowledge_retirements());
	down = exclusive;
	down.lockmode = ShareLock;
	down.current_mode = ExclusiveLock;
	down.convert_old_request_id = 0;
	UT_ASSERT_EQ(walr_request_convert_actual(&down, exclusive.request_id, &cleanup),
				 CLUSTER_LOCK_ACQUIRE_OK_CONVERTED);
	UT_ASSERT_EQ(down.request_id, exclusive.request_id);
	UT_ASSERT(!cluster_lock_owner_request_usable(&exclusive));
	UT_ASSERT(cluster_lock_owner_request_usable(&down));
	/* Mirror the actual pin consumer: remove temporary native X and S. */
	walr_native_lock_release_or_fatal(&exclusive);
	walr_native_lock_release_or_fatal(&down);
	UT_ASSERT_EQ(native_refs[ShareLock], 1);
	UT_ASSERT_EQ(native_refs[ExclusiveLock], 0);
	UT_ASSERT_EQ(walr_request_release_actual(&down), CLUSTER_LOCK_ACQUIRE_PENDING);
	UT_ASSERT(acknowledge_retirements());
	UT_ASSERT_EQ(walr_request_release_actual(&down), CLUSTER_LOCK_ACQUIRE_OK_GRANTED);
	UT_ASSERT_EQ(native_refs[ShareLock], 0);
}

UT_TEST(failed_upgrade_waits_for_restore_proof_before_old_pin_is_usable)
{
	ClusterLockAcquireRequest share = walr_fixture_request(), exclusive;
	uint64 enumerated;
	bool cleanup;

	UT_ASSERT_EQ(walr_request_acquire_actual(&share), CLUSTER_LOCK_ACQUIRE_OK_GRANTED);
	exclusive = share;
	exclusive.op = CLUSTER_LOCK_OP_CONVERT;
	exclusive.current_mode = ShareLock;
	exclusive.lockmode = ExclusiveLock;
	exclusive.convert_old_request_id = share.request_id;
	g_s5_result = CLUSTER_LOCK_ACQUIRE_NOT_AVAIL;
	UT_ASSERT_EQ(walr_request_convert_actual(&exclusive, 0, &cleanup),
				 CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL);
	UT_ASSERT(!cleanup);
	UT_ASSERT_EQ(native_refs[ShareLock], 1);
	UT_ASSERT_EQ(native_refs[ExclusiveLock], 0);
	UT_ASSERT(!cluster_lock_owner_request_usable(&share));
	UT_ASSERT(!cluster_lock_owners_redeclare(&enumerated));
	UT_ASSERT(acknowledge_retirements());
	UT_ASSERT(cluster_lock_owners_redeclare(&enumerated));
	UT_ASSERT(cluster_lock_owner_request_usable(&share));
	UT_ASSERT_EQ(walr_request_release_actual(&share), CLUSTER_LOCK_ACQUIRE_PENDING);
	UT_ASSERT(acknowledge_retirements());
	UT_ASSERT_EQ(walr_request_release_actual(&share), CLUSTER_LOCK_ACQUIRE_OK_GRANTED);
}

UT_TEST(failed_downgrade_never_reconstructs_x_and_keeps_native_refs_exact)
{
	ClusterLockAcquireRequest share = walr_fixture_request(), exclusive, down;
	bool cleanup;

	UT_ASSERT_EQ(walr_request_acquire_actual(&share), CLUSTER_LOCK_ACQUIRE_OK_GRANTED);
	exclusive = share;
	exclusive.op = CLUSTER_LOCK_OP_CONVERT;
	exclusive.current_mode = ShareLock;
	exclusive.lockmode = ExclusiveLock;
	exclusive.convert_old_request_id = share.request_id;
	g_s5_result = CLUSTER_LOCK_ACQUIRE_OK_CONVERTED;
	UT_ASSERT_EQ(walr_request_convert_actual(&exclusive, 0, &cleanup),
				 CLUSTER_LOCK_ACQUIRE_OK_CONVERTED);
	down = exclusive;
	down.lockmode = ShareLock;
	down.current_mode = ExclusiveLock;
	down.convert_old_request_id = 0;
	g_s5_result = CLUSTER_LOCK_ACQUIRE_FAIL_TIMEOUT;
	UT_ASSERT_EQ(walr_request_convert_actual(&down, exclusive.request_id, &cleanup),
				 CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL);
	UT_ASSERT(!cluster_lock_owner_request_usable(&exclusive));
	UT_ASSERT(!cluster_lock_owner_request_usable(&down));
	UT_ASSERT_EQ(native_refs[ShareLock], 1);
	UT_ASSERT_EQ(native_refs[ExclusiveLock], 1);
	UT_ASSERT_EQ(walr_request_release_actual(&exclusive), CLUSTER_LOCK_ACQUIRE_PENDING);
	UT_ASSERT(acknowledge_retirements());
	UT_ASSERT_EQ(walr_request_release_actual(&exclusive), CLUSTER_LOCK_ACQUIRE_OK_GRANTED);
	/* Error cleanup drops the original pin's native S separately. */
	walr_native_lock_release_or_fatal(&share);
	UT_ASSERT_EQ(native_refs[ShareLock], 0);
	UT_ASSERT_EQ(native_refs[ExclusiveLock], 0);
}

UT_TEST(failed_upgrade_cross_cut_retires_x_before_redeclaring_known_s)
{
	ClusterLockAcquireRequest share = walr_fixture_request(), exclusive;
	ClusterControlRequestView view;
	uint64 enumerated;
	uint32 cursor;
	bool cleanup;
	unsigned polls = owner_poll_count;

	UT_ASSERT_EQ(walr_request_acquire_actual(&share), CLUSTER_LOCK_ACQUIRE_OK_GRANTED);
	exclusive = share;
	exclusive.op = CLUSTER_LOCK_OP_CONVERT;
	exclusive.current_mode = ShareLock;
	exclusive.lockmode = ExclusiveLock;
	exclusive.convert_old_request_id = share.request_id;
	g_s5_result = CLUSTER_LOCK_ACQUIRE_NOT_AVAIL;
	UT_ASSERT_EQ(walr_request_convert_actual(&exclusive, 0, &cleanup),
				 CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL);
	owner_epoch++;
	owner_generation++;
	owner_rebuild_frozen = false;
	UT_ASSERT(!cluster_lock_owners_redeclare(&enumerated));
	cursor = 0;
	while (cluster_control_request_next(&cursor, &view))
		if (view.mode == ExclusiveLock)
			UT_ASSERT_EQ(view.message.previous_request, share.request_id);
	/* LMON can run first: the new empty master cannot restore old S. */
	cursor = 0;
	while (cluster_control_request_next(&cursor, &view)) {
		ClusterControlRequestCut cut;
		ClusterControlRetireMessage reply;

		if (view.mode != ExclusiveLock)
			continue;
		control_cut(&cut);
		UT_ASSERT(cluster_control_request_claim(&view.handle, retire_driver, &cut, &reply));
		reply.verb = CLUSTER_CONTROL_RETIRE_INVALID;
		UT_ASSERT(cluster_control_request_ack(&reply, cut.master, retire_driver, &cut));
	}
	UT_ASSERT(!cluster_lock_owners_redeclare(&enumerated)); /* No freeze, no reset. */
	owner_rebuild_frozen = true;
	UT_ASSERT(!cluster_lock_owners_redeclare(&enumerated));
	cursor = 0;
	while (cluster_control_request_next(&cursor, &view))
		if (view.mode == ExclusiveLock)
			UT_ASSERT_EQ(view.message.previous_request, 0);
	UT_ASSERT_EQ(owner_poll_count, polls); /* No pretend S before X retirement. */
	UT_ASSERT(!cluster_lock_owner_request_usable(&share));
	UT_ASSERT(acknowledge_retirements());
	(void)cluster_lock_owners_redeclare(&enumerated);
	UT_ASSERT(!cluster_lock_owner_request_usable(&share));
	owner_reply = CLUSTER_GES_REDECLARE_CONFIRMED;
	UT_ASSERT(!cluster_lock_owners_redeclare(&enumerated)); /* New S + old S debt. */
	UT_ASSERT(acknowledge_retirements());
	UT_ASSERT(cluster_lock_owners_redeclare(&enumerated));
	UT_ASSERT(cluster_lock_owner_request_usable(&share));
	UT_ASSERT(cluster_lock_owner_request_refresh(&share));
	UT_ASSERT_EQ(share.holder.cluster_epoch, owner_epoch);
	UT_ASSERT_EQ(native_refs[ShareLock], 1);
	UT_ASSERT_EQ(walr_request_release_actual(&share), CLUSTER_LOCK_ACQUIRE_PENDING);
	UT_ASSERT(acknowledge_retirements());
	UT_ASSERT_EQ(walr_request_release_actual(&share), CLUSTER_LOCK_ACQUIRE_OK_GRANTED);
	owner_rebuild_frozen = false;
}

UT_TEST(rebound_x_downgrade_refreshes_the_stable_owners_identity)
{
	ClusterLockAcquireRequest share = walr_fixture_request(), exclusive, down;
	ClusterLockAcquireResult result;
	uint64 enumerated, old_request;
	bool cleanup;

	UT_ASSERT_EQ(walr_request_acquire_actual(&share), CLUSTER_LOCK_ACQUIRE_OK_GRANTED);
	exclusive = share;
	exclusive.op = CLUSTER_LOCK_OP_CONVERT;
	exclusive.current_mode = ShareLock;
	exclusive.lockmode = ExclusiveLock;
	g_s5_result = CLUSTER_LOCK_ACQUIRE_OK_CONVERTED;
	UT_ASSERT_EQ(walr_request_convert_actual(&exclusive, 0, &cleanup),
				 CLUSTER_LOCK_ACQUIRE_OK_CONVERTED);
	UT_ASSERT(acknowledge_retirements());
	UT_ASSERT(cluster_lock_owners_redeclare(&enumerated)); /* Consume old S at its cut. */
	owner_epoch++;
	owner_generation++;
	owner_reply = CLUSTER_GES_REDECLARE_CONFIRMED;
	UT_ASSERT(!cluster_lock_owners_redeclare(&enumerated));
	UT_ASSERT(acknowledge_retirements());
	UT_ASSERT(cluster_lock_owners_redeclare(&enumerated));
	UT_ASSERT(cluster_lock_owner_request_usable(&exclusive));
	old_request = exclusive.request_id;
	down = exclusive; /* Consumer still carries its pre-rebuild guard. */
	down.current_mode = ExclusiveLock;
	down.lockmode = ShareLock;
	down.convert_old_request_id = 0;
	UT_ASSERT_EQ(walr_request_convert_actual(&down, old_request + 1, &cleanup),
				 CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL);
	UT_ASSERT(cluster_lock_owner_request_usable(&exclusive));
	result = walr_request_convert_actual(&down, old_request, &cleanup);
	UT_ASSERT_EQ(result, CLUSTER_LOCK_ACQUIRE_OK_CONVERTED);
	if (result == CLUSTER_LOCK_ACQUIRE_OK_CONVERTED) {
		UT_ASSERT(down.request_id != old_request);
		UT_ASSERT_EQ(down.holder.cluster_epoch, owner_epoch);
		walr_native_lock_release_or_fatal(&exclusive);
		walr_native_lock_release_or_fatal(&down);
		UT_ASSERT_EQ(walr_request_release_actual(&down), CLUSTER_LOCK_ACQUIRE_PENDING);
		UT_ASSERT(acknowledge_retirements());
		UT_ASSERT_EQ(walr_request_release_actual(&down), CLUSTER_LOCK_ACQUIRE_OK_GRANTED);
	} else {
		(void)walr_request_release_actual(&exclusive);
		UT_ASSERT(acknowledge_retirements());
		(void)walr_request_release_actual(&exclusive);
		walr_native_lock_release_or_fatal(&share);
	}
	UT_ASSERT_EQ(native_refs[ShareLock], 0);
	UT_ASSERT_EQ(native_refs[ExclusiveLock], 0);
}

UT_TEST(error_in_upgrade_surrenders_both_remote_holds_without_native_leak)
{
	ClusterLockAcquireRequest share = walr_fixture_request(), exclusive;
	ClusterControlRequestView view;
	uint32 cursor = 0;
	bool cleanup;
	volatile bool caught = false;

	UT_ASSERT_EQ(walr_request_acquire_actual(&share), CLUSTER_LOCK_ACQUIRE_OK_GRANTED);
	exclusive = share;
	exclusive.op = CLUSTER_LOCK_OP_CONVERT;
	exclusive.current_mode = ShareLock;
	exclusive.lockmode = ExclusiveLock;
	exclusive.convert_old_request_id = share.request_id;
	throw_during_install = true;
	PG_TRY();
	{
		(void)walr_request_convert_actual(&exclusive, 0, &cleanup);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	throw_during_install = false;
	UT_ASSERT(caught);
	UT_ASSERT_EQ(native_refs[ShareLock], 1);
	UT_ASSERT_EQ(native_refs[ExclusiveLock], 0);
	while (cluster_control_request_next(&cursor, &view)) {
		UT_ASSERT_EQ(view.state, CLUSTER_CONTROL_REQUEST_ABANDONED);
		UT_ASSERT_EQ(view.message.previous_request, 0);
	}
	UT_ASSERT_EQ(walr_request_release_actual(&share), CLUSTER_LOCK_ACQUIRE_PENDING);
	UT_ASSERT(acknowledge_retirements());
	UT_ASSERT_EQ(walr_request_release_actual(&share), CLUSTER_LOCK_ACQUIRE_OK_GRANTED);
	UT_ASSERT_EQ(native_refs[ShareLock], 0);
}

UT_TEST(error_in_acquire_cannot_leave_an_unpublished_native_ref)
{
	ClusterLockAcquireRequest request = walr_fixture_request();
	volatile bool caught = false;
	uint64 enumerated;

	throw_during_install = true;
	PG_TRY();
	{
		(void)walr_request_acquire_actual(&request);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	throw_during_install = false;
	UT_ASSERT(caught);
	UT_ASSERT_EQ(native_refs[ShareLock], 0);
	UT_ASSERT(acknowledge_retirements());
	UT_ASSERT(cluster_lock_owners_redeclare(&enumerated));
	UT_ASSERT_EQ(enumerated, 0);
}

UT_TEST(foreground_release_waits_but_lmon_never_waits_on_itself)
{
	ClusterLockAcquireRequest request = walr_fixture_request();
	unsigned before = owner_wait_calls;

	UT_ASSERT_EQ(walr_request_acquire_actual(&request), CLUSTER_LOCK_ACQUIRE_OK_GRANTED);
	owner_clock_step = 1;
	owner_ack_on_wait = true;
	MyBackendType = B_BACKEND;
	UT_ASSERT_EQ(walr_request_release_actual(&request), CLUSTER_LOCK_ACQUIRE_OK_GRANTED);
	UT_ASSERT_EQ(owner_wait_calls, before + 1);
	UT_ASSERT_EQ(native_refs[ShareLock], 0);
	owner_ack_on_wait = false;
	owner_clock_step = 60000000;
	/* A missing ACK on LMON must return to its ordinary CONTROL loop. */
	MyBackendType = B_LMON;
	request = walr_fixture_request();
	UT_ASSERT_EQ(walr_request_acquire_actual(&request), CLUSTER_LOCK_ACQUIRE_OK_GRANTED);
	before = owner_wait_calls;
	UT_ASSERT_EQ(walr_request_release_actual(&request), CLUSTER_LOCK_ACQUIRE_PENDING);
	UT_ASSERT_EQ(owner_wait_calls, before);
	UT_ASSERT(acknowledge_retirements());
	UT_ASSERT_EQ(walr_request_release_actual(&request), CLUSTER_LOCK_ACQUIRE_OK_GRANTED);
	MyBackendType = B_INVALID;
}

int
main(void)
{
	MyProcPid = 4002;
	owner_proc.pgprocno = 42;
	pg_atomic_init_u32(&owner_proc.cluster_grd_registered_count, 0);
	pg_atomic_init_u64(&owner_proc.cluster_grd_redeclare_acked, 0);
	pg_atomic_init_u64(&owner_proc.cluster_grd_redeclare_acked_epoch, 0);
	cluster_control_request_shmem_init();
	retire_driver = cluster_control_request_driver_start();
	cluster_shared_config = true;
	UT_PLAN(10);
	UT_RUN(native_s_is_kept_until_exact_retirement);
	UT_RUN(failed_native_publication_keeps_only_owned_cleanup);
	UT_RUN(upgrade_uses_new_attempt_and_downgrade_keeps_confirmed_identity);
	UT_RUN(failed_upgrade_waits_for_restore_proof_before_old_pin_is_usable);
	UT_RUN(failed_downgrade_never_reconstructs_x_and_keeps_native_refs_exact);
	UT_RUN(failed_upgrade_cross_cut_retires_x_before_redeclaring_known_s);
	UT_RUN(rebound_x_downgrade_refreshes_the_stable_owners_identity);
	UT_RUN(error_in_upgrade_surrenders_both_remote_holds_without_native_leak);
	UT_RUN(error_in_acquire_cannot_leave_an_unpublished_native_ref);
	UT_RUN(foreground_release_waits_but_lmon_never_waits_on_itself);
	UT_DONE();
	return ut_failed_count != 0;
}
