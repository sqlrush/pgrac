/*-------------------------------------------------------------------------
 * test_cluster_clog_relmap_physical_mutation.inc
 *     Delayed native page writes revoke held CLOG observations.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_clog_relmap_physical_mutation.inc
 * NOTES
 *     Original foreign CLOG initialization, SLRU writes, files and shared
 *     counters execute. Locks, authority and faults are fixtures. Same-
 *     postmaster attempt observation is not continuous lifetime authority.
 *-------------------------------------------------------------------------
 */
static int
relmap_physical_prepare(void)
{
	SlruCtl ctl = &ClogRecoveryCtl[1];
	int slot;

	relmap_base_reset(1);
	clog_recovery_cache_discard(ctl);
	fixture_zero(0);
	/* Model an already dirty native cache before taking the held disk read.
	 * No logical CLOG mutation or reservation runs after that read. */
	LWLockAcquire(ctl->shared->ControlLock, LW_EXCLUSIVE);
	slot = SimpleLruZeroPage(ctl, 0);
	LWLockRelease(ctl->shared->ControlLock);
	return slot;
}

static void
relmap_physical_zero_file(void)
{
	PGAlignedBlock actual, zero = {0};
	char path[MAXPGPATH];
	int fd;

	fixture_path(0, path);
	fd = open(path, O_RDONLY);
	UT_ASSERT(fd >= 0);
	if (fd < 0) return;
	UT_ASSERT_EQ(pread(fd, actual.data, BLCKSZ, 0), BLCKSZ);
	UT_ASSERT_EQ(memcmp(actual.data, zero.data, BLCKSZ), 0);
	UT_ASSERT_EQ(close(fd), 0);
}

UT_TEST(relmap_late_physical_write_revokes_equal_byte_observation)
{
	SlruCtl ctl = &ClogRecoveryCtl[1];
	ClusterRelmapClogBaseV1 base;
	ClusterRelmapClogBaseHandleV1 *held = NULL;
	uint64 before, after;

	(void)relmap_physical_prepare();
	UT_ASSERT(cluster_clog_relmap_base_read_v1(&relmap_base_source,
		relmap_base_query(), &base, &held));
	UT_ASSERT(clog_relmap_mutation_snapshot(1, &before));
	/* Original checkpoint/eviction writeback, with no surrounding CLOG call. */
	SimpleLruWriteAll(ctl, true);
	UT_ASSERT(clog_relmap_mutation_snapshot(1, &after));
	UT_ASSERT(after > before);
	relmap_physical_zero_file();
	clog_recovery_cache_discard(ctl);
	relmap_mutation_refuses(&base, &held);
}

UT_TEST(relmap_clean_write_and_unrelated_controller_preserve_observation)
{
	static SlruCtlData auxiliary;
	static LWLock auxiliary_lock;
	SlruCtl ctl = &ClogRecoveryCtl[1];
	ClusterRelmapClogBaseV1 base;
	ClusterRelmapClogBaseHandleV1 *held = NULL;
	uint64 before, after;
	char path[MAXPGPATH], file[MAXPGPATH];

	(void)relmap_physical_prepare();
	SimpleLruWriteAll(ctl, true);
	UT_ASSERT(cluster_clog_relmap_base_read_v1(&relmap_base_source,
		relmap_base_query(), &base, &held));
	UT_ASSERT(clog_relmap_mutation_snapshot(1, &before));
	SimpleLruWriteAll(ctl, true);
	/* Init must clear a copied observer as it initializes this distinct SLRU. */
	auxiliary = *ctl;
	snprintf(path, sizeof(path), "%s/physical_aux", fixture_root);
	UT_ASSERT_EQ(mkdir(path, 0700), 0);
	LWLockInitialize(&auxiliary_lock, LWTRANCHE_XACT_BUFFER);
	SimpleLruInit(&auxiliary, "Physical unrelated fixture", 2, 0,
		&auxiliary_lock, path, LWTRANCHE_XACT_BUFFER, SYNC_HANDLER_NONE);
	LWLockAcquire(&auxiliary_lock, LW_EXCLUSIVE);
	SimpleLruWritePage(&auxiliary, SimpleLruZeroPage(&auxiliary, 0));
	LWLockRelease(&auxiliary_lock);
	UT_ASSERT(clog_relmap_mutation_snapshot(1, &after));
	UT_ASSERT_EQ(after, before);
	UT_ASSERT(cluster_clog_relmap_base_read_v1(&relmap_base_source,
		relmap_base_query(), &base, &held));
	cluster_clog_relmap_base_release_v1(&held);
	clog_recovery_cache_discard(ctl);
	snprintf(file, sizeof(file), "%s/0000", path);
	UT_ASSERT_EQ(unlink(file), 0);
	UT_ASSERT_EQ(rmdir(path), 0);
	fixture_idle();
}

static void
relmap_physical_pause_write(void)
{
	fixture_native_write_hook = NULL;
	if (write(relmap_shared_ready, "r", 1) != 1
		|| !relmap_shared_pipe_read(relmap_shared_resume)) _exit(72);
}

UT_TEST(relmap_physical_write_in_progress_refuses_old_and_new_reads)
{
	for (unsigned existing = 0; existing < 2; ++existing) {
		SlruCtl ctl = &ClogRecoveryCtl[1];
		ClusterRelmapClogBaseV1 base;
		ClusterRelmapClogBaseHandleV1 *held = NULL;
		int ready[2], resume[2];
		pid_t child;
		bool entered;
		uint64 generation;

		(void)relmap_physical_prepare();
		if (existing)
			UT_ASSERT(cluster_clog_relmap_base_read_v1(&relmap_base_source,
				relmap_base_query(), &base, &held));
		UT_ASSERT_EQ(pipe(ready), 0); UT_ASSERT_EQ(pipe(resume), 0);
		fflush(NULL); child = fork();
		if (child == 0) {
			ut_current_failed = 0;
			close(ready[0]); close(resume[1]);
			relmap_shared_ready = ready[1]; relmap_shared_resume = resume[0];
			fixture_native_write_hook = relmap_physical_pause_write;
			SimpleLruWriteAll(ctl, true);
			_exit(ut_current_failed ? 71 : 0);
		}
		close(ready[1]); close(resume[0]);
		entered = relmap_shared_pipe_read(ready[0]);
		UT_ASSERT(entered);
		if (entered) {
			UT_ASSERT(!clog_relmap_mutation_snapshot(1, &generation));
			UT_ASSERT(!cluster_clog_relmap_base_read_v1(&relmap_base_source,
				relmap_base_query(), &base, &held));
			UT_ASSERT_EQ(base.queried_full_xid, 0);
			UT_ASSERT(!authority);
			UT_ASSERT_EQ(transient_fds, existing);
			UT_ASSERT_EQ(write(resume[1], "r", 1), 1);
		}
		close(ready[0]); close(resume[1]);
		relmap_shared_wait(child);
		UT_ASSERT(clog_relmap_mutation_snapshot(1, &generation));
		cluster_clog_relmap_base_release_v1(&held);
		clog_recovery_cache_discard(ctl);
		fixture_idle();
	}
}

UT_TEST(relmap_returned_write_error_restores_native_state_but_revokes_observation)
{
	SlruCtl ctl = &ClogRecoveryCtl[1];
	ClusterRelmapClogBaseV1 base;
	ClusterRelmapClogBaseHandleV1 *held = NULL;
	volatile bool caught = false;
	uint64 before, after;
	int slot = relmap_physical_prepare();

	UT_ASSERT(cluster_clog_relmap_base_read_v1(&relmap_base_source,
		relmap_base_query(), &base, &held));
	UT_ASSERT(clog_relmap_mutation_snapshot(1, &before));
	LWLockAcquire(ctl->shared->ControlLock, LW_EXCLUSIVE);
	fail_open = expect_error = true;
	PG_TRY(); {
		SimpleLruWritePage(ctl, slot);
	} PG_CATCH(); { caught = true; } PG_END_TRY();
	fail_open = expect_error = false;
	UT_ASSERT(caught);
	UT_ASSERT(LWLockHeldByMe(ctl->shared->ControlLock));
	UT_ASSERT(!LWLockHeldByMe(&ctl->shared->buffer_locks[slot].lock));
	UT_ASSERT_EQ(ctl->shared->page_status[slot], SLRU_PAGE_VALID);
	UT_ASSERT(ctl->shared->page_dirty[slot]);
	UT_ASSERT(clog_relmap_mutation_snapshot(1, &after));
	UT_ASSERT(after > before);
	/* Original error delivery cleared holdoff; the original caller owns cleanup. */
	HOLD_INTERRUPTS();
	LWLockRelease(ctl->shared->ControlLock);
	UT_ASSERT_EQ(InterruptHoldoffCount, 0);
	clog_recovery_cache_discard(ctl);
	relmap_physical_zero_file();
	relmap_mutation_refuses(&base, &held);
}

static void
relmap_physical_error_write(void)
{
	fixture_native_write_hook = NULL;
	(void)errstart(ERROR, NULL);
}

UT_TEST(relmap_abrupt_write_error_keeps_original_writer_debt)
{
	SlruCtl ctl = &ClogRecoveryCtl[1];
	ClusterRelmapClogBaseV1 base;
	ClusterRelmapClogBaseHandleV1 *held = NULL;
	pid_t child;
	uint64 generation;
	int slot = relmap_physical_prepare();

	UT_ASSERT(cluster_clog_relmap_base_read_v1(&relmap_base_source,
		relmap_base_query(), &base, &held));
	fflush(NULL); child = fork();
	if (child == 0) {
		volatile bool caught = false;
		ut_current_failed = 0;
		fixture_native_write_hook = relmap_physical_error_write;
		expect_error = true;
		LWLockAcquire(ctl->shared->ControlLock, LW_EXCLUSIVE);
		PG_TRY(); {
			SimpleLruWritePage(ctl, slot);
		} PG_CATCH(); { caught = true; } PG_END_TRY();
		UT_ASSERT(caught);
		UT_ASSERT_EQ(ctl->shared->page_status[slot], SLRU_PAGE_WRITE_IN_PROGRESS);
		UT_ASSERT(LWLockHeldByMe(&ctl->shared->buffer_locks[slot].lock));
		UT_ASSERT(!LWLockHeldByMe(ctl->shared->ControlLock));
		/* No fabricated native restoration, end callback or counter reset. */
		_exit(ut_current_failed ? 71 : 0);
	}
	relmap_shared_wait(child);
	UT_ASSERT(!clog_relmap_mutation_snapshot(1, &generation));
	clog_recovery_cache_discard(ctl);
	relmap_mutation_refuses(&base, &held);
	UT_ASSERT(!clog_relmap_mutation_snapshot(1, &generation));
	/* Last group: the original shared debt survives until fixture teardown. */
}
