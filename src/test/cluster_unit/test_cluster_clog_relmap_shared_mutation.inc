/*-------------------------------------------------------------------------
 * test_cluster_clog_relmap_shared_mutation.inc
 *     Held CLOG observations cannot cross another backend's native mutation.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_clog_relmap_shared_mutation.inc
 * NOTES
 *     fork, shared counter memory and native CLOG/SLRU/file I/O execute.
 *     Authority and native locks remain fixtures, not cluster qualification.
 *-------------------------------------------------------------------------
 */
#include <poll.h>

static void
relmap_shared_child_write(unsigned route, XidStatus status)
{
	ClusterSideProjectionOperationV1 op = {0};
	xl_clog_zeropage_full birth = {0, 0, (UINT64_C(7) << 32) | 3};
	XLogReaderState reader = {0};
	DecodedXLogRecord decoded = {0};
	int page = 0;
	InRecovery = authority = true;
	op.kind = CLUSTER_SIDE_PROJECTION_CLOG;
	op.action = CLUSTER_SIDE_PROJECTION_ACTION_ZERO_PAGE;
	op.normalized_info = CLOG_ZEROPAGE;
	if (route == 0 || route == 1) {
		if (route == 1)
			UT_ASSERT(cluster_clog_native_recovery_record_apply_v1(1, &op));
		UT_ASSERT(cluster_clog_native_recovery_apply_v1(1, 100, 0, NULL, status));
	} else if (route == 2)
		UT_ASSERT(cluster_clog_native_recovery_record_apply_v1(1, &op));
	else if (route == 3) {
		op.normalized_info = CLOG_ZEROPAGE_FULL;
		UT_ASSERT(cluster_clog_native_recovery_full_record_apply_v1(1, &op,
			(const uint8 *)&birth, sizeof(birth)));
	} else if (route == 4) {
		op.action = CLUSTER_SIDE_PROJECTION_ACTION_TRUNCATE;
		op.normalized_info = CLOG_TRUNCATE;
		op.oldest_xid = 3; op.oldest_database = 11;
		UT_ASSERT(cluster_clog_native_recovery_record_apply_v1(1, &op));
	} else if (route == 5)
		ClusterClogAdoptNativeStatus(100, status);
	else if (route == 6) {
		LWLockAcquire(XactSLRULock, LW_EXCLUSIVE);
		SimpleLruWritePage(XactCtl, ZeroCLOGPage(0, false));
		LWLockRelease(XactSLRULock);
	} else if (route == 7 || route == 8) {
		reader.record = &decoded;
		decoded.main_data = route == 7 ? (char *)&page : (char *)&birth;
		decoded.main_data_len = route == 7 ? sizeof(page) : sizeof(birth);
		decoded.max_block_id = -1;
		decoded.header.xl_info = route == 7 ? CLOG_ZEROPAGE : CLOG_ZEROPAGE_FULL;
		clog_redo(&reader);
	} else if (route == 9) {
		UT_ASSERT(cluster_clog_native_recovery_verify_v1(1, 100, 0, NULL, status));
		UT_ASSERT(!cluster_clog_native_recovery_apply_v1(1, 100, -1, NULL, status));
		UT_ASSERT(!cluster_clog_native_recovery_record_apply_v1(1, NULL));
	} else {
		volatile bool caught = false, ok = true;
		relmap_base_interrupts = false;
		if (route == 10) directory_sync_error = EIO;
		else fail_dirsync = expect_error = true;
		PG_TRY(); {
			ok = cluster_clog_native_recovery_apply_v1(1, 100, 0, NULL, status);
		} PG_CATCH(); { caught = true; } PG_END_TRY();
		UT_ASSERT(route == 10 ? !ok : caught);
	}
}

static void
relmap_shared_wait(pid_t child)
{
	int status = 0;
	UT_ASSERT(child > 0);
	if (child > 0) {
		UT_ASSERT_EQ(waitpid(child, &status, 0), child);
		UT_ASSERT(WIFEXITED(status));
		UT_ASSERT_EQ(WIFEXITED(status) ? WEXITSTATUS(status) : -1, 0);
	}
}

static void
relmap_shared_write(unsigned route, XidStatus status)
{
	pid_t child;
	fflush(NULL);
	child = fork();
	if (child == 0) {
		unsigned previous = ut_failed_count;
		relmap_shared_child_write(route, status);
		_exit(ut_failed_count == previous ? 0 : 71);
	}
	relmap_shared_wait(child);
}

UT_TEST(relmap_other_process_terminal_aba_revokes_original_custody)
{
	for (int status = TRANSACTION_STATUS_COMMITTED; status <= TRANSACTION_STATUS_ABORTED;
		 ++status) {
		ClusterRelmapClogBaseV1 base;
		ClusterRelmapClogBaseHandleV1 *held = NULL;
		UT_ASSERT(relmap_mutation_open(1, status, &base, &held));
		if (held == NULL) return;
		relmap_shared_write(1, status);
		UT_ASSERT_EQ(fixture_status(100), status);
		relmap_mutation_refuses(&base, &held);
	}
}

UT_TEST(relmap_other_process_zero_full_and_truncate_revoke)
{
	for (unsigned route = 2; route <= 4; ++route) {
		ClusterRelmapClogBaseV1 base;
		ClusterRelmapClogBaseHandleV1 *held = NULL;
		UT_ASSERT(relmap_mutation_open(1, TRANSACTION_STATUS_IN_PROGRESS, &base, &held));
		if (held == NULL) return;
		relmap_shared_write(route, TRANSACTION_STATUS_IN_PROGRESS);
		UT_ASSERT_EQ(fixture_status(100), TRANSACTION_STATUS_IN_PROGRESS);
		relmap_mutation_refuses(&base, &held);
	}
	for (unsigned route = 10; route <= 11; ++route) {
		ClusterRelmapClogBaseV1 base;
		ClusterRelmapClogBaseHandleV1 *held = NULL;
		UT_ASSERT(relmap_mutation_open(1, TRANSACTION_STATUS_COMMITTED, &base, &held));
		if (held == NULL) return;
		relmap_shared_write(route, TRANSACTION_STATUS_COMMITTED);
		relmap_mutation_refuses(&base, &held);
	}
}

UT_TEST(relmap_other_process_local_status_zero_and_redo_revoke)
{
	for (unsigned route = 5; route <= 8; ++route) {
		ClusterRelmapClogBaseV1 base;
		ClusterRelmapClogBaseHandleV1 *held = NULL;
		UT_ASSERT(relmap_mutation_open(0, route == 5 ? TRANSACTION_STATUS_ABORTED
			: TRANSACTION_STATUS_IN_PROGRESS, &base, &held));
		if (held == NULL) return;
		relmap_shared_write(route, TRANSACTION_STATUS_ABORTED);
		relmap_mutation_refuses(&base, &held);
	}
}

UT_TEST(relmap_other_process_verify_refusal_and_origin_isolation)
{
	ClusterRelmapClogBaseV1 base;
	ClusterRelmapClogBaseHandleV1 *held = NULL;
	UT_ASSERT(relmap_mutation_open(1, TRANSACTION_STATUS_COMMITTED, &base, &held));
	if (held == NULL) return;
	relmap_shared_write(9, TRANSACTION_STATUS_COMMITTED);
	UT_ASSERT(cluster_clog_relmap_base_read_v1(&relmap_base_source,
		relmap_base_query(), &base, &held));
	cluster_clog_relmap_base_release_v1(&held);
	UT_ASSERT(relmap_mutation_open(0, TRANSACTION_STATUS_ABORTED, &base, &held));
	if (held == NULL) return;
	fixture_origin = 1; fixture_zero(0); fixture_bit(100, TRANSACTION_STATUS_COMMITTED);
	relmap_shared_write(1, TRANSACTION_STATUS_COMMITTED);
	UT_ASSERT(cluster_clog_relmap_base_read_v1(&relmap_base_source,
		relmap_base_query(), &base, &held));
	cluster_clog_relmap_base_release_v1(&held);
	/* A new observation may sample the completed native attempt; an old one
	 * cannot replace its original mutation generation with that sample. */
	UT_ASSERT(relmap_mutation_open(1, TRANSACTION_STATUS_COMMITTED, &base, &held));
	cluster_clog_relmap_base_release_v1(&held);
	fixture_idle();
}

static int relmap_shared_ready, relmap_shared_resume;
static bool
relmap_shared_pipe_read(int fd)
{
	struct pollfd event = {fd, POLLIN, 0};
	char byte;
	return poll(&event, 1, 5000) == 1 && read(fd, &byte, 1) == 1;
}

static void
relmap_shared_pause_sync(void)
{
	fixture_native_sync_hook = NULL;
	if (write(relmap_shared_ready, "r", 1) != 1
		|| !relmap_shared_pipe_read(relmap_shared_resume)) _exit(72);
}

UT_TEST(relmap_other_process_inflight_write_refuses_new_and_old_reads)
{
	for (unsigned existing = 0; existing < 2; ++existing) {
		ClusterRelmapClogBaseV1 base;
		ClusterRelmapClogBaseHandleV1 *held = NULL;
		int ready[2], resume[2];
		pid_t child;
		bool entered;
		relmap_base_reset(1); fixture_zero(0); fixture_bit(100, TRANSACTION_STATUS_COMMITTED);
		if (existing)
			UT_ASSERT(cluster_clog_relmap_base_read_v1(&relmap_base_source,
				relmap_base_query(), &base, &held));
		UT_ASSERT_EQ(pipe(ready), 0); UT_ASSERT_EQ(pipe(resume), 0);
		fflush(NULL); child = fork();
		if (child == 0) {
			unsigned previous = ut_failed_count;
			close(ready[0]); close(resume[1]);
			relmap_shared_ready = ready[1]; relmap_shared_resume = resume[0];
			fixture_native_sync_hook = relmap_shared_pause_sync;
			relmap_shared_child_write(0, TRANSACTION_STATUS_COMMITTED);
			_exit(ut_failed_count == previous ? 0 : 73);
		}
		close(ready[1]); close(resume[0]);
		entered = relmap_shared_pipe_read(ready[0]);
		UT_ASSERT(entered);
		if (entered) {
			UT_ASSERT(!cluster_clog_relmap_base_read_v1(&relmap_base_source,
				relmap_base_query(), &base, &held));
			UT_ASSERT_EQ(base.queried_full_xid, 0);
			UT_ASSERT(!authority);
			UT_ASSERT_EQ(transient_fds, existing);
			UT_ASSERT_EQ(write(resume[1], "r", 1), 1);
		}
		close(ready[0]); close(resume[1]);
		relmap_shared_wait(child);
		cluster_clog_relmap_base_release_v1(&held);
		fixture_idle();
	}
}

static unsigned relmap_shared_root_target;
static void
relmap_shared_during_root(void)
{
	if (relmap_base_rechecks != relmap_shared_root_target) return;
	relmap_base_recheck_hook = NULL;
	relmap_shared_write(6, TRANSACTION_STATUS_IN_PROGRESS);
}

UT_TEST(relmap_other_process_mutation_during_root_cannot_publish)
{
	for (unsigned existing = 0; existing < 2; ++existing) {
		ClusterRelmapClogBaseV1 base;
		ClusterRelmapClogBaseHandleV1 *held = NULL;
		relmap_base_reset(0); fixture_zero(0);
		clog_recovery_cache_discard(XactCtl);
		if (existing)
			UT_ASSERT(cluster_clog_relmap_base_read_v1(&relmap_base_source,
				relmap_base_query(), &base, &held));
		relmap_shared_root_target = relmap_base_rechecks + 2;
		relmap_base_recheck_hook = relmap_shared_during_root;
		UT_ASSERT(!cluster_clog_relmap_base_read_v1(&relmap_base_source,
			relmap_base_query(), &base, &held));
		UT_ASSERT(relmap_base_recheck_hook == NULL);
		UT_ASSERT(!authority);
		UT_ASSERT_EQ(base.queried_full_xid, 0);
		UT_ASSERT_EQ(transient_fds, existing);
		cluster_clog_relmap_base_release_v1(&held);
		fixture_idle();
	}
}

UT_TEST(relmap_shared_mutation_saturation_and_missing_state_never_grant)
{
	ClogRelmapMutation *saved = ClogRelmapMutations;
	uint64 generation = pg_atomic_read_u64(&saved[1].generation);
	ClusterRelmapClogBaseV1 base;
	ClusterRelmapClogBaseHandleV1 *held = NULL;
	for (unsigned fault = 0; fault < 4; ++fault) {
		unsigned io;
		relmap_base_reset(1); fixture_zero(0); fixture_bit(100, TRANSACTION_STATUS_COMMITTED);
		if (fault == 0) ClogRelmapMutations = NULL;
		else if (fault == 1) pg_atomic_write_u64(&saved[1].generation, 0);
		else if (fault == 2) {
			pg_atomic_write_u64(&saved[1].generation, UINT64_MAX - 1);
			UT_ASSERT(cluster_clog_relmap_base_read_v1(&relmap_base_source,
				relmap_base_query(), &base, &held));
			relmap_shared_write(0, TRANSACTION_STATUS_COMMITTED);
			UT_ASSERT_EQ(pg_atomic_read_u64(&saved[1].generation), UINT64_MAX);
		} else {
			pg_atomic_write_u32(&saved[1].writers, UINT32_MAX - 1);
			relmap_shared_write(0, TRANSACTION_STATUS_COMMITTED);
			UT_ASSERT_EQ(pg_atomic_read_u32(&saved[1].writers), UINT32_MAX);
		}
		io = reads + syncs + dirsyncs;
		UT_ASSERT(!cluster_clog_relmap_base_read_v1(&relmap_base_source,
			relmap_base_query(), &base, &held));
		UT_ASSERT_EQ(base.queried_full_xid, 0);
		UT_ASSERT(!authority);
		UT_ASSERT_EQ(reads + syncs + dirsyncs, io);
		cluster_clog_relmap_base_release_v1(&held);
		ClogRelmapMutations = saved;
		if (fault >= 2) {
			/* Native mutation continues, but cannot wrap saturated custody
			 * state into eligibility for another observation. */
			relmap_shared_write(0, TRANSACTION_STATUS_COMMITTED);
			UT_ASSERT(fault == 2
				? pg_atomic_read_u64(&saved[1].generation) == UINT64_MAX
				: pg_atomic_read_u32(&saved[1].writers) == UINT32_MAX);
		}
		/* Restore only this unit fixture, with no outstanding handle. There
		 * is no production reset/rebase operation for saturated state. */
		pg_atomic_write_u64(&saved[1].generation, generation);
		pg_atomic_write_u32(&saved[1].writers, 0);
		fixture_idle();
	}
	clog_recovery_shmem_init();
	UT_ASSERT(ClogRelmapMutations == saved);
	UT_ASSERT_EQ(pg_atomic_read_u64(&saved[1].generation), generation);
	UT_ASSERT_EQ(pg_atomic_read_u32(&saved[1].writers), 0);
}
