/*-------------------------------------------------------------------------
 * test_cluster_subtrans_order.c
 *    Native SUBTRANS deletion and queued file synchronization.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_subtrans_order.c
 * NOTES
 *    PGRAC test. Actual SLRU, SUBTRANS, sync, checkpointer queue and hash
 *    bodies operate on temporary files. Process allocation, LWLocks and
 *    error delivery are fixtures; forked writers share real mapped buffers.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include <dirent.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <poll.h>
#include "../../backend/access/transam/subtrans.c"
#ifdef SLRU_TEST_SOURCE
#include SLRU_TEST_SOURCE
#else
#include "../../backend/access/transam/slru.c"
#endif
#include "../../backend/storage/sync/sync.c"
#include "../../backend/postmaster/checkpointer.c"
#include "../../backend/utils/hash/dynahash.c"
#include "../../backend/nodes/list.c"
#include "../../common/hashfn.c"

#undef printf
#undef fprintf
#undef snprintf
#include "unit_test.h"
UT_DEFINE_GLOBALS();

#include "test_cluster_subtrans_order_fixture.h"

static void
queued_delete_case(bool truncate)
{
	fixture_wait_child(fixture_backend(0, B_BACKEND, false));
	UT_ASSERT_EQ(CheckpointerShmem->num_requests, 1);
	UT_ASSERT(fixture_exists(0));
	fixture_delete(truncate);
	UT_ASSERT(!fixture_exists(0));
	UT_ASSERT(fixture_process_sync());
	UT_ASSERT_EQ(error_level, 0);
	UT_ASSERT_EQ(error_errno, 0);
	UT_ASSERT_EQ(sync_calls, 0);
}
UT_TEST(queued_sync_then_checkpointer_forget)
{
	queued_delete_case(false);
}
UT_TEST(queued_sync_then_subtrans_truncate)
{
	queued_delete_case(true);
}

static void
inflight_case(bool truncate, int stage)
{
	pid_t writer = fixture_backend(0, B_BACKEND, stage), deleter;
	pipe_receive(pause_writer[0]);
	UT_ASSERT_EQ(SubTransCtl->shared->page_status[0], SLRU_PAGE_WRITE_IN_PROGRESS);
	UT_ASSERT_EQ(CheckpointerShmem->num_requests, stage == 2 ? 1 : 0);
	fflush(NULL);
	deleter = fork();
	if (deleter < 0)
		fixture_panic();
	if (deleter == 0) {
		reader_notify = true;
		fixture_delete(truncate);
		UT_ASSERT(!fixture_exists(0));
		UT_ASSERT(fixture_process_sync());
		UT_ASSERT_EQ(error_level, 0);
		UT_ASSERT_EQ(error_errno, 0);
		fflush(NULL);
		_exit(ut_current_failed ? 1 : 0);
	}
	pipe_receive(wait_reader[0]);
	UT_ASSERT(fixture_exists(0));
	pipe_send(resume_writer[1]);
	fixture_wait_child(writer);
	fixture_wait_child(deleter);
}
UT_TEST(delete_waits_for_original_writer_to_queue_sync)
{
	inflight_case(false, 1);
}
UT_TEST(truncate_waits_for_original_writer_to_queue_sync)
{
	inflight_case(true, 1);
}
UT_TEST(same_name_new_segment_is_synced)
{
	FileTag tag;
	PendingFsyncEntry *entry;

	fixture_wait_child(fixture_backend(0, B_BACKEND, false));
	fixture_delete(false);
	UT_ASSERT(!fixture_exists(0));
	INIT_SLRUFILETAG(tag, SYNC_HANDLER_CLUSTER_SUBTRANS, 0);
	entry = hash_search(pendingOps, &tag, HASH_FIND, NULL);
	UT_ASSERT(entry != NULL);
	if (entry == NULL)
		return;
	UT_ASSERT(entry->canceled);

	/* PGRAC: a new SYNC must reactivate the still-present canceled entry. */
	fixture_wait_child(fixture_backend(0, B_BACKEND, false));
	UT_ASSERT(fixture_exists(0));
	AbsorbSyncRequests();
	UT_ASSERT(hash_search(pendingOps, &tag, HASH_FIND, NULL) == entry);
	UT_ASSERT(!entry->canceled);
	UT_ASSERT(fixture_process_sync());
	UT_ASSERT_EQ(sync_calls, 1);
}
UT_TEST(startup_forget_uses_original_queue_order)
{
	pid_t pid;
	fixture_wait_child(fixture_backend(0, B_BACKEND, false));
	fflush(NULL);
	pid = fork();
	if (pid < 0)
		fixture_panic();
	if (pid == 0) {
		MyBackendType = B_STARTUP;
		MyAuxProcType = StartupProcess;
		pendingOps = NULL;
		fixture_delete(false);
		UT_ASSERT_EQ(CheckpointerShmem->num_requests, 2);
		fflush(NULL);
		_exit(ut_current_failed ? 1 : 0);
	}
	fixture_wait_child(pid);
	UT_ASSERT(fixture_process_sync());
	UT_ASSERT_EQ(sync_calls, 0);
}
UT_TEST(retained_parent_is_not_deleted)
{
	int page = 2 * SLRU_PAGES_PER_SEGMENT;
	TransactionId child = page * SUBTRANS_XACTS_PER_PAGE + 11;
	fixture_wait_child(fixture_backend(page, B_BACKEND, false));
	LWLockAcquire(SubtransSLRULock, LW_EXCLUSIVE);
	SubTransCtl->shared->page_status[0] = SLRU_PAGE_EMPTY;
	LWLockRelease(SubtransSLRULock);
	TruncateSUBTRANS(child - 1);
	UT_ASSERT(fixture_exists(2));
	UT_ASSERT_EQ(SubTransGetParent(child - 1), 123);
	UT_ASSERT(fixture_process_sync());
	UT_ASSERT_EQ(sync_calls, 1);
}
UT_TEST(missing_retained_file_keeps_fsync_failure)
{
	fixture_wait_child(fixture_backend(0, B_BACKEND, false));
	unlink("pg_subtrans/0000");
	UT_ASSERT(!fixture_process_sync());
	UT_ASSERT_EQ(error_level, PANIC);
	UT_ASSERT_EQ(error_errno, ENOENT);
}
UT_TEST(non_subtrans_handler_does_not_absorb)
{
	SubTransCtl->sync_handler = SYNC_HANDLER_CLOG;
	fixture_wait_child(fixture_backend(0, B_BACKEND, false));
	fixture_delete(false);
	UT_ASSERT_EQ(CheckpointerShmem->num_requests, 1);
	UT_ASSERT(!fixture_process_sync());
	UT_ASSERT_EQ(error_errno, ENOENT);
}

UT_TEST(delete_waits_for_queued_writer_before_valid)
{
	inflight_case(false, 2);
}
UT_TEST(truncate_waits_for_queued_writer_before_valid)
{
	inflight_case(true, 2);
}
static void
dirty_case(bool truncate)
{
	fixture_wait_child(fixture_backend(0, B_BACKEND, false));
	LWLockAcquire(SubtransSLRULock, LW_EXCLUSIVE);
	SubTransCtl->shared->page_dirty[0] = true;
	LWLockRelease(SubtransSLRULock);
	fixture_delete(truncate);
	UT_ASSERT(!fixture_exists(0));
	UT_ASSERT(fixture_process_sync());
	UT_ASSERT_EQ(error_level, 0);
	UT_ASSERT_EQ(error_errno, 0);
}
UT_TEST(delete_drains_dirty_buffer_before_forget)
{
	dirty_case(false);
}
UT_TEST(truncate_drains_dirty_buffer_before_forget)
{
	dirty_case(true);
}
static void
full_queue_case(bool fault)
{
	pid_t pid;
	fixture_wait_child(fixture_backend(SLRU_PAGES_PER_SEGMENT, B_BACKEND, false));
	CheckpointerShmem->max_requests = 1;
	fflush(NULL);
	pid = fork();
	if (pid < 0)
		fixture_panic();
	if (pid == 0) {
		sigjmp_buf local;
		ut_current_failed = 0;
		MyBackendType = B_BACKEND;
		MyAuxProcType = NotAnAuxProcess;
		pendingOps = NULL;
		sync_fails = fault;
		PG_exception_stack = &local;
		catch_error = true;
		if (sigsetjmp(local, 1) == 0)
			fixture_write(0);
		catch_error = false;
		PG_exception_stack = NULL;
		UT_ASSERT_EQ(sync_calls, 1);
		UT_ASSERT_EQ(error_level, fault ? PANIC : 0);
		if (fault) {
			UT_ASSERT_EQ(error_errno, EIO);
			UT_ASSERT_EQ(slru_errcause, SLRU_FSYNC_FAILED);
		}
		fflush(NULL);
		_exit(ut_current_failed ? 1 : 0);
	}
	fixture_wait_child(pid);
	UT_ASSERT_EQ(CheckpointerShmem->num_requests, 1);
	UT_ASSERT_EQ(CheckpointerShmem->num_backend_fsync, 1);
	UT_ASSERT(fixture_exists(0));
	if (!fault) {
		fixture_delete(false);
		UT_ASSERT(fixture_process_sync());
		UT_ASSERT_EQ(sync_calls, 1);
	}
}
UT_TEST(full_queue_uses_original_fsync_fallback)
{
	full_queue_case(false);
}
UT_TEST(full_queue_fsync_eio_remains_fatal)
{
	full_queue_case(true);
}
UT_TEST(wrapped_old_segment_keeps_current_parent)
{
	int last_page = (int)(UINT32_MAX / SUBTRANS_XACTS_PER_PAGE);
	int current_page = 2 * SLRU_PAGES_PER_SEGMENT;
	fixture_wait_child(fixture_backend(last_page, B_BACKEND, false));
	fixture_wait_child(fixture_backend(current_page, B_BACKEND, false));
	TruncateSUBTRANS(current_page * SUBTRANS_XACTS_PER_PAGE + 11);
	UT_ASSERT(!fixture_exists(last_page / SLRU_PAGES_PER_SEGMENT));
	UT_ASSERT(fixture_exists(2));
	UT_ASSERT_EQ(SubTransGetParent(current_page * SUBTRANS_XACTS_PER_PAGE + 10), 123);
	UT_ASSERT(fixture_process_sync());
	UT_ASSERT_EQ(sync_calls, 1);
}
static void
allocation_fault_case(bool critical)
{
	sigjmp_buf local;
	fixture_wait_child(fixture_backend(0, B_BACKEND, false));
	if (critical) {
		pid_t pid;
		fflush(NULL);
		pid = fork();
		if (pid < 0)
			fixture_panic();
		if (pid == 0) {
			FileTag tag;
			pendingOps = NULL;
			MyAuxProcType = NotAnAuxProcess;
			MyBackendType = B_BACKEND;
			for (int i = 1; i < 512; i++) {
				INIT_SLRUFILETAG(tag, SYNC_HANDLER_CLUSTER_SUBTRANS, i);
				UT_ASSERT(RegisterSyncRequest(&tag, SYNC_REQUEST, false));
			}
			fflush(NULL);
			_exit(ut_current_failed ? 1 : 0);
		}
		fixture_wait_child(pid);
	}
	fail_copy_alloc = !critical;
	fail_critical_alloc = critical;
	PG_exception_stack = &local;
	catch_error = true;
	if (sigsetjmp(local, 1) == 0)
		fixture_delete(false);
	catch_error = false;
	PG_exception_stack = NULL;
	UT_ASSERT_EQ(error_level, critical ? PANIC : ERROR);
	UT_ASSERT_EQ(CheckpointerShmem->num_requests, critical ? 0 : 1);
	UT_ASSERT(fixture_exists(0));
	UT_ASSERT_EQ(CritSectionCount, critical ? 1 : 0);
}
UT_TEST(absorb_copy_failure_does_not_delete_or_clear_queue)
{
	allocation_fault_case(false);
}
UT_TEST(absorb_hash_failure_after_transfer_remains_panic)
{
	allocation_fault_case(true);
}
UT_TEST(nonshared_subtrans_has_no_sync_work)
{
	SubTransCtl->sync_handler = SYNC_HANDLER_NONE;
	fixture_wait_child(fixture_backend(0, B_BACKEND, false));
	fixture_delete(false);
	UT_ASSERT(!fixture_exists(0));
	UT_ASSERT_EQ(CheckpointerShmem->num_requests, 0);
	UT_ASSERT(fixture_process_sync());
}

static void
run_case(void (*fn)(void))
{
	pid_t pid;
	int status;
	fflush(NULL);
	pid = fork();
	if (pid < 0)
		fixture_panic();
	if (pid == 0) {
		alarm(15);
		fixture_setup();
		fn();
		fixture_cleanup();
		fflush(NULL);
		_exit(ut_current_failed ? 1 : 0);
	}
	UT_ASSERT_EQ(waitpid(pid, &status, 0), pid);
	UT_ASSERT(WIFEXITED(status) && WEXITSTATUS(status) == 0);
}
#define WRAPPED_CASE(name)                                                                         \
	static void run_##name(void)                                                                   \
	{                                                                                              \
		run_case(name);                                                                            \
	}
WRAPPED_CASE(queued_sync_then_checkpointer_forget)
WRAPPED_CASE(queued_sync_then_subtrans_truncate)
WRAPPED_CASE(delete_waits_for_original_writer_to_queue_sync)
WRAPPED_CASE(truncate_waits_for_original_writer_to_queue_sync)
WRAPPED_CASE(same_name_new_segment_is_synced)
WRAPPED_CASE(startup_forget_uses_original_queue_order)
WRAPPED_CASE(retained_parent_is_not_deleted)
WRAPPED_CASE(missing_retained_file_keeps_fsync_failure)
WRAPPED_CASE(non_subtrans_handler_does_not_absorb)
WRAPPED_CASE(delete_waits_for_queued_writer_before_valid)
WRAPPED_CASE(truncate_waits_for_queued_writer_before_valid)
WRAPPED_CASE(delete_drains_dirty_buffer_before_forget)
WRAPPED_CASE(truncate_drains_dirty_buffer_before_forget)
WRAPPED_CASE(full_queue_uses_original_fsync_fallback)
WRAPPED_CASE(full_queue_fsync_eio_remains_fatal)
WRAPPED_CASE(wrapped_old_segment_keeps_current_parent)
WRAPPED_CASE(absorb_copy_failure_does_not_delete_or_clear_queue)
WRAPPED_CASE(absorb_hash_failure_after_transfer_remains_panic)
WRAPPED_CASE(nonshared_subtrans_has_no_sync_work)

int
main(void)
{
	UT_PLAN(19);
	UT_RUN(run_queued_sync_then_checkpointer_forget);
	UT_RUN(run_queued_sync_then_subtrans_truncate);
	UT_RUN(run_delete_waits_for_original_writer_to_queue_sync);
	UT_RUN(run_truncate_waits_for_original_writer_to_queue_sync);
	UT_RUN(run_same_name_new_segment_is_synced);
	UT_RUN(run_startup_forget_uses_original_queue_order);
	UT_RUN(run_retained_parent_is_not_deleted);
	UT_RUN(run_missing_retained_file_keeps_fsync_failure);
	UT_RUN(run_non_subtrans_handler_does_not_absorb);
	UT_RUN(run_delete_waits_for_queued_writer_before_valid);
	UT_RUN(run_truncate_waits_for_queued_writer_before_valid);
	UT_RUN(run_delete_drains_dirty_buffer_before_forget);
	UT_RUN(run_truncate_drains_dirty_buffer_before_forget);
	UT_RUN(run_full_queue_uses_original_fsync_fallback);
	UT_RUN(run_full_queue_fsync_eio_remains_fatal);
	UT_RUN(run_wrapped_old_segment_keeps_current_parent);
	UT_RUN(run_absorb_copy_failure_does_not_delete_or_clear_queue);
	UT_RUN(run_absorb_hash_failure_after_transfer_remains_panic);
	UT_RUN(run_nonshared_subtrans_has_no_sync_work);
	UT_DONE();
	return ut_failed_count != 0;
}
