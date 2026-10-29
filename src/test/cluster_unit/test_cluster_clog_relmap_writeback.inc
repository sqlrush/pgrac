/*-------------------------------------------------------------------------
 * test_cluster_clog_relmap_writeback.inc
 *     Native SLRU writeback invalidates held CLOG observations.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_clog_relmap_writeback.inc
 * NOTES
 *     Actual checkpoint, eviction and direct writeback use native files.
 *     Original authority and LWLocks are fixtures, not lifetime authority.
 *-------------------------------------------------------------------------
 */
static SlruCtl
relmap_writeback_ctl(int origin)
{
	return origin == 0 ? XactCtl : &ClogRecoveryCtl[origin];
}

/* Model an already-dirty, byte-identical page without calling a logical
 * mutator: those already invalidated observations before this regression. */
static int
relmap_writeback_dirty(SlruCtl ctl)
{
	int slot;
	clog_recovery_cache_discard(ctl);
	LWLockAcquire(ctl->shared->ControlLock, LW_EXCLUSIVE);
	slot = SimpleLruReadPage(ctl, 0, true, 100);
	ctl->shared->page_dirty[slot] = true;
	LWLockRelease(ctl->shared->ControlLock);
	return slot;
}

static void
relmap_writeback_direct(SlruCtl ctl, int slot)
{
	LWLockAcquire(ctl->shared->ControlLock, LW_EXCLUSIVE);
	SimpleLruWritePage(ctl, slot);
	LWLockRelease(ctl->shared->ControlLock);
}

static void
relmap_writeback_same_image(const ClusterRelmapClogBaseV1 *base)
{
	PGAlignedBlock image;
	char path[MAXPGPATH];
	int fd;
	fixture_path(0, path);
	fd = open(path, O_RDONLY);
	UT_ASSERT(fd >= 0);
	if (fd >= 0) {
		UT_ASSERT_EQ(read(fd, image.data, BLCKSZ), BLCKSZ);
		UT_ASSERT(memcmp(image.data, base->image, BLCKSZ) == 0);
		UT_ASSERT_EQ(close(fd), 0);
	}
}

UT_TEST(relmap_checkpoint_equal_bytes_revokes_held_observation)
{
	ClusterRelmapClogBaseV1 base;
	ClusterRelmapClogBaseHandleV1 *held = NULL;
	unsigned before;
	UT_ASSERT(relmap_mutation_open(0, TRANSACTION_STATUS_COMMITTED, &base, &held));
	if (held == NULL) return;
	(void)relmap_writeback_dirty(XactCtl);
	before = writes;
	CheckPointCLOG();
	UT_ASSERT(writes > before);
	relmap_writeback_same_image(&base);
	clog_recovery_cache_discard(XactCtl);
	relmap_mutation_refuses(&base, &held);
}

UT_TEST(relmap_direct_writeback_tracks_original_local_and_foreign_caches)
{
	for (int origin = 0; origin < 2; ++origin) {
		ClusterRelmapClogBaseV1 base;
		ClusterRelmapClogBaseHandleV1 *held = NULL;
		SlruCtl ctl = relmap_writeback_ctl(origin);
		int slot;
		UT_ASSERT(relmap_mutation_open(origin, TRANSACTION_STATUS_ABORTED, &base, &held));
		if (held == NULL) return;
		slot = relmap_writeback_dirty(ctl);
		relmap_writeback_direct(ctl, slot);
		relmap_writeback_same_image(&base);
		clog_recovery_cache_discard(ctl);
		relmap_mutation_refuses(&base, &held);
	}
}

UT_TEST(relmap_eviction_writeback_revokes_equal_byte_custody)
{
	for (int origin = 0; origin < 2; ++origin) {
		ClusterRelmapClogBaseV1 base;
		ClusterRelmapClogBaseHandleV1 *held = NULL;
		SlruCtl ctl = relmap_writeback_ctl(origin);
		int slot, count = ctl->shared->num_slots;
		unsigned before;
		relmap_base_reset(origin);
		for (int page = 1; page <= count; ++page) fixture_zero(page);
		UT_ASSERT(relmap_mutation_open(origin, TRANSACTION_STATUS_COMMITTED, &base, &held));
		if (held == NULL) return;
		slot = relmap_writeback_dirty(ctl);
		LWLockAcquire(ctl->shared->ControlLock, LW_EXCLUSIVE);
		for (int page = 1; page < count; ++page)
			(void)SimpleLruReadPage(ctl, page, true, InvalidTransactionId);
		ctl->shared->latest_page_number = count - 1;
		before = writes;
		(void)SimpleLruReadPage(ctl, count, true, InvalidTransactionId);
		UT_ASSERT(writes > before);
		UT_ASSERT_EQ(ctl->shared->page_number[slot], count);
		LWLockRelease(ctl->shared->ControlLock);
		relmap_writeback_same_image(&base);
		clog_recovery_cache_discard(ctl);
		relmap_mutation_refuses(&base, &held);
	}
}

UT_TEST(relmap_writeback_clean_and_other_origin_preserve_observation)
{
	ClusterRelmapClogBaseV1 base;
	ClusterRelmapClogBaseHandleV1 *held = NULL;
	uint64 generation;
	SlruCtlData unrelated;
	int slot;
	UT_ASSERT(relmap_mutation_open(0, TRANSACTION_STATUS_ABORTED, &base, &held));
	if (held == NULL) return;
	clog_recovery_cache_discard(XactCtl);
	generation = pg_atomic_read_u64(&ClogRelmapMutations[0].generation);
	CheckPointCLOG();
	UT_ASSERT_EQ(pg_atomic_read_u64(&ClogRelmapMutations[0].generation), generation);
	fixture_origin = 1; fixture_zero(0);
	(void)relmap_writeback_dirty(&ClogRecoveryCtl[1]);
	SimpleLruWriteAll(&ClogRecoveryCtl[1], false);
	UT_ASSERT_EQ(pg_atomic_read_u64(&ClogRelmapMutations[0].generation), generation);
	/* Matching directory/shared memory do not identify a native controller. */
	unrelated = ClogRecoveryCtl[1];
	slot = relmap_writeback_dirty(&unrelated);
	generation = pg_atomic_read_u64(&ClogRelmapMutations[1].generation);
	relmap_writeback_direct(&unrelated, slot);
	UT_ASSERT_EQ(pg_atomic_read_u64(&ClogRelmapMutations[1].generation), generation);
	clog_recovery_cache_discard(&ClogRecoveryCtl[1]);
	UT_ASSERT(cluster_clog_relmap_base_read_v1(&relmap_base_source,
		relmap_base_query(), &base, &held));
	cluster_clog_relmap_base_release_v1(&held);
	fixture_idle();
}

UT_TEST(relmap_failed_writeback_keeps_sticky_debt_and_releases_writer)
{
	for (int origin = 0; origin < 2; ++origin) {
		ClusterRelmapClogBaseV1 base;
		ClusterRelmapClogBaseHandleV1 *held = NULL;
		SlruCtl ctl = relmap_writeback_ctl(origin);
		volatile bool caught = false;
		int slot;
		UT_ASSERT(relmap_mutation_open(origin, TRANSACTION_STATUS_COMMITTED, &base, &held));
		if (held == NULL) return;
		slot = relmap_writeback_dirty(ctl);
		fail_open = expect_error = true;
		PG_TRY(); { relmap_writeback_direct(ctl, slot); }
		PG_CATCH(); {
			caught = true;
			if (LWLockHeldByMe(ctl->shared->ControlLock)) {
				HOLD_INTERRUPTS();
				LWLockRelease(ctl->shared->ControlLock);
			}
		} PG_END_TRY();
		fail_open = expect_error = false;
		UT_ASSERT(caught);
		UT_ASSERT_EQ(pg_atomic_read_u32(&ClogRelmapMutations[origin].writers), 0);
		UT_ASSERT(ctl->shared->page_dirty[slot]);
		relmap_writeback_same_image(&base);
		clog_recovery_cache_discard(ctl);
		relmap_mutation_refuses(&base, &held);
	}
}

static void
relmap_writeback_pause(void)
{
	fixture_native_write_hook = NULL;
	if (write(relmap_shared_ready, "r", 1) != 1
		|| !relmap_shared_pipe_read(relmap_shared_resume)) _exit(74);
}

UT_TEST(relmap_inflight_physical_write_refuses_new_and_original_custody)
{
	for (int origin = 0; origin < 2; ++origin) {
		for (unsigned existing = 0; existing < 2; ++existing) {
			ClusterRelmapClogBaseV1 base;
			ClusterRelmapClogBaseHandleV1 *held = NULL;
			SlruCtl ctl = relmap_writeback_ctl(origin);
			int ready[2], resume[2], slot;
			pid_t child;
			bool entered;
			relmap_base_reset(origin); fixture_zero(0);
			if (existing)
				UT_ASSERT(cluster_clog_relmap_base_read_v1(&relmap_base_source,
					relmap_base_query(), &base, &held));
			slot = relmap_writeback_dirty(ctl);
			UT_ASSERT_EQ(pipe(ready), 0); UT_ASSERT_EQ(pipe(resume), 0);
			fflush(NULL); child = fork();
			if (child == 0) {
				unsigned previous = ut_failed_count;
				close(ready[0]); close(resume[1]);
				relmap_shared_ready = ready[1]; relmap_shared_resume = resume[0];
				fixture_native_write_hook = relmap_writeback_pause;
				relmap_writeback_direct(ctl, slot);
				_exit(ut_failed_count == previous ? 0 : 75);
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
			UT_ASSERT_EQ(pg_atomic_read_u32(&ClogRelmapMutations[origin].writers), 0);
			cluster_clog_relmap_base_release_v1(&held);
			clog_recovery_cache_discard(ctl);
			fixture_idle();
		}
	}
}
