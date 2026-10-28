/*-------------------------------------------------------------------------
 * test_cluster_clog_relmap_native_init.inc
 *     Native own-controller initialization and delayed writeback observation.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_clog_relmap_native_init.inc
 * NOTES
 *     Original CLOG initialization, checkpoint, eviction and files execute.
 *     Locks, authority and sync-queue refusal are fixtures; original inline
 *     fsync executes. This is not native startup or lifetime certification.
 *-------------------------------------------------------------------------
 */
static void
relmap_native_own_initialize(void)
{
	static bool initialized;
	uint64 foreign_generation;
	uint32 foreign_writers;

	relmap_base_reset(0);
	fixture_idle();
	foreign_generation = pg_atomic_read_u64(&ClogRelmapMutations[1].generation);
	foreign_writers = pg_atomic_read_u32(&ClogRelmapMutations[1].writers);
	if (!initialized) {
		/* Reattach the original shared counters; never reset unfinished debt. */
		CLOGShmemInit();
		initialized = true;
	}
	UT_ASSERT_EQ(pg_atomic_read_u64(&ClogRelmapMutations[1].generation),
		foreign_generation);
	UT_ASSERT_EQ(pg_atomic_read_u32(&ClogRelmapMutations[1].writers), foreign_writers);
	UT_ASSERT_EQ(XactCtl->sync_handler, SYNC_HANDLER_CLOG);
	UT_ASSERT_EQ(CLOGShmemBuffers(), 4);
	UT_ASSERT_EQ(XactCtl->shared->num_slots, 4);
	/* Reject enqueue so original SLRU performs the real inline file fsync. */
	fixture_native_sync_fallback = true;
}

static void
relmap_native_own_file_identity(const struct stat *before)
{
	struct stat after;
	char path[MAXPGPATH];

	fixture_path(0, path);
	UT_ASSERT_EQ(stat(path, &after), 0);
	UT_ASSERT_EQ(after.st_dev, before->st_dev);
	UT_ASSERT_EQ(after.st_ino, before->st_ino);
	UT_ASSERT_EQ(after.st_size, before->st_size);
	relmap_physical_zero_file();
}

UT_TEST(relmap_native_own_checkpoint_revokes_equal_byte_observation)
{
	ClusterRelmapClogBaseV1 base;
	ClusterRelmapClogBaseHandleV1 *held = NULL;
	struct stat before;
	char path[MAXPGPATH];
	uint64 generation, after;
	unsigned written, synced, queued;
	int slot;

	relmap_native_own_initialize();
	fixture_zero(0);
	LWLockAcquire(XactSLRULock, LW_EXCLUSIVE);
	slot = ZeroCLOGPage(0, false);
	LWLockRelease(XactSLRULock);
	fixture_path(0, path);
	UT_ASSERT_EQ(stat(path, &before), 0);
	UT_ASSERT(cluster_clog_relmap_base_read_v1(&relmap_base_source,
		relmap_base_query(), &base, &held));
	UT_ASSERT(clog_relmap_mutation_snapshot(0, &generation));
	written = writes; synced = syncs; queued = fixture_native_sync_requests;
	/* No logical CLOG mutation or reservation surrounds this native flush. */
	CheckPointCLOG();
	UT_ASSERT_EQ(writes, written + 1);
	UT_ASSERT_EQ(fixture_native_sync_requests, queued + 1);
	UT_ASSERT_EQ(syncs, synced + 1);
	UT_ASSERT_EQ(XactCtl->shared->page_status[slot], SLRU_PAGE_VALID);
	UT_ASSERT(!XactCtl->shared->page_dirty[slot]);
	UT_ASSERT(clog_relmap_mutation_snapshot(0, &after));
	UT_ASSERT(after > generation);
	relmap_native_own_file_identity(&before);
	relmap_mutation_refuses(&base, &held);
}

UT_TEST(relmap_native_own_eviction_revokes_without_logical_mutation)
{
	ClusterRelmapClogBaseV1 base;
	ClusterRelmapClogBaseHandleV1 *held = NULL;
	struct stat before;
	char path[MAXPGPATH];
	uint64 generation, after;
	unsigned written, synced, queued;
	int slots[4], replacement;

	relmap_native_own_initialize();
	for (int page = 0; page <= 4; ++page)
		fixture_zero(page);
	LWLockAcquire(XactSLRULock, LW_EXCLUSIVE);
	for (int page = 0; page < 4; ++page)
		slots[page] = ZeroCLOGPage(page, false);
	LWLockRelease(XactSLRULock);
	fixture_path(0, path);
	UT_ASSERT_EQ(stat(path, &before), 0);
	UT_ASSERT(cluster_clog_relmap_base_read_v1(&relmap_base_source,
		relmap_base_query(), &base, &held));
	UT_ASSERT(clog_relmap_mutation_snapshot(0, &generation));
	written = writes; synced = syncs; queued = fixture_native_sync_requests;
	/* A native cache miss writes the dirty victim; no later CLOG call runs. */
	LWLockAcquire(XactSLRULock, LW_EXCLUSIVE);
	replacement = SimpleLruReadPage(XactCtl, 4, true, InvalidTransactionId);
	UT_ASSERT(LWLockHeldByMe(XactSLRULock));
	LWLockRelease(XactSLRULock);
	UT_ASSERT_EQ(replacement, slots[0]);
	UT_ASSERT_EQ(XactCtl->shared->page_number[replacement], 4);
	UT_ASSERT_EQ(XactCtl->shared->page_status[replacement], SLRU_PAGE_VALID);
	UT_ASSERT(!XactCtl->shared->page_dirty[replacement]);
	UT_ASSERT_EQ(writes, written + 1);
	UT_ASSERT_EQ(fixture_native_sync_requests, queued + 1);
	UT_ASSERT_EQ(syncs, synced + 1);
	for (int page = 1; page < 4; ++page) {
		UT_ASSERT_EQ(XactCtl->shared->page_number[slots[page]], page);
		UT_ASSERT(XactCtl->shared->page_dirty[slots[page]]);
		for (int group = 0; group < CLOG_LSNS_PER_PAGE; ++group)
			UT_ASSERT_EQ(XactCtl->shared->group_lsn[slots[page] * CLOG_LSNS_PER_PAGE + group],
				InvalidXLogRecPtr);
	}
	UT_ASSERT(clog_relmap_mutation_snapshot(0, &after));
	UT_ASSERT(after > generation);
	relmap_native_own_file_identity(&before);
	/* Local fd cleanup does not discard the other dirty native pages. */
	relmap_mutation_refuses(&base, &held);
	for (int page = 1; page < 4; ++page)
		UT_ASSERT(XactCtl->shared->page_dirty[slots[page]]);
}
