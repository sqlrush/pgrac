/*-------------------------------------------------------------------------
 * test_cluster_clog_resident_birth.inc
 *     Native FULL initialization input follows the original resident slot.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_clog_resident_birth.inc
 * NOTES
 *     Original CLOG/SLRU/files execute; locks, WAL and episode are fixtures.
 *     Resident initialization input is not era or continuous lifetime proof.
 *-------------------------------------------------------------------------
 */
static uint64
resident_birth_full(void)
{
	return (UINT64_C(3) << 32) | FirstNormalTransactionId;
}

static void
resident_birth_prepare(bool redo)
{
	birth_reset();
	if (redo) {
		xl_clog_zeropage_full birth = {0, 0, resident_birth_full()};
		DecodedXLogRecord decoded = {0};
		XLogReaderState reader = {0};
		decoded.header.xl_info = CLOG_ZEROPAGE_FULL;
		decoded.main_data = (char *)&birth;
		decoded.main_data_len = sizeof(birth);
		decoded.max_block_id = -1;
		reader.record = &decoded;
		clog_redo(&reader);
	} else {
		LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
		UT_ASSERT(!birth_call_caught(FullTransactionIdFromU64(resident_birth_full())));
		LWLockRelease(XidGenLock);
	}
	relmap_base_reset(0);
}

static bool
resident_birth_read(ClusterRelmapClogBaseV1 *base, ClusterRelmapClogBaseHandleV1 **held)
{
	return cluster_clog_relmap_base_read_v1(&relmap_base_source,
		relmap_base_query(), base, held);
}

UT_TEST(resident_birth_comes_from_native_full_producer_not_query)
{
	ClusterRelmapClogBaseV1 base;
	ClusterRelmapClogBaseHandleV1 *held = NULL;
	resident_birth_prepare(false);
	/* A dirty native initialization is not a matching physical observation. */
	UT_ASSERT(resident_birth_read(&base, &held));
	UT_ASSERT(!base.resident_birth_observed);
	UT_ASSERT_EQ(base.resident_first_full_xid, 0);
	cluster_clog_relmap_base_release_v1(&held);
	CheckPointCLOG();
	UT_ASSERT(resident_birth_read(&base, &held));
	UT_ASSERT(base.resident_birth_observed);
	UT_ASSERT_EQ(base.resident_first_full_xid, resident_birth_full());
	UT_ASSERT_NE(base.resident_first_full_xid >> 32, relmap_base_query() >> 32);
	cluster_clog_relmap_base_release_v1(&held);
	/* A foreign controller cannot borrow the local controller's FULL input. */
	relmap_base_reset(1); fixture_zero(0);
	UT_ASSERT(resident_birth_read(&base, &held));
	UT_ASSERT(!base.resident_birth_observed);
	UT_ASSERT_EQ(base.resident_first_full_xid, 0);
	cluster_clog_relmap_base_release_v1(&held); fixture_idle();
}

UT_TEST(resident_birth_redo_legacy_and_wal_error_boundaries)
{
	ClusterRelmapClogBaseV1 base;
	ClusterRelmapClogBaseHandleV1 *held = NULL;
	resident_birth_prepare(true);
	UT_ASSERT(resident_birth_read(&base, &held));
	UT_ASSERT(base.resident_birth_observed);
	UT_ASSERT_EQ(base.resident_first_full_xid, resident_birth_full());
	cluster_clog_relmap_base_release_v1(&held);
	/* Legacy zero cannot preserve the prior slot's FULL initialization. */
	LWLockAcquire(XactSLRULock, LW_EXCLUSIVE);
	ZeroCLOGPage(0, false);
	LWLockRelease(XactSLRULock);
	CheckPointCLOG();
	relmap_base_reset(0);
	UT_ASSERT(resident_birth_read(&base, &held));
	UT_ASSERT(!base.resident_birth_observed);
	UT_ASSERT_EQ(base.resident_first_full_xid, 0);
	cluster_clog_relmap_base_release_v1(&held);
	birth_wal.fail = true; relmap_base_interrupts = false;
	LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
	UT_ASSERT(birth_call_caught(FullTransactionIdFromU64(resident_birth_full())));
	UT_ASSERT(LWLockHeldByMe(XactSLRULock));
	LWLockRelease(XactSLRULock); LWLockRelease(XidGenLock);
	birth_wal.fail = false;
	CheckPointCLOG(); relmap_base_reset(0);
	UT_ASSERT(resident_birth_read(&base, &held));
	UT_ASSERT(!base.resident_birth_observed);
	UT_ASSERT_EQ(base.resident_first_full_xid, 0);
	cluster_clog_relmap_base_release_v1(&held); fixture_idle();
}

UT_TEST(resident_birth_cache_loss_refuses_held_rebase_and_disk_inference)
{
	ClusterRelmapClogBaseV1 base;
	ClusterRelmapClogBaseHandleV1 *held = NULL;
	uint64 before, after;
	resident_birth_prepare(true);
	UT_ASSERT(resident_birth_read(&base, &held));
	UT_ASSERT(base.resident_birth_observed);
	UT_ASSERT(clog_relmap_mutation_snapshot(0, &before));
	/* Original cache discard does not change the already-read native file. */
	clog_recovery_cache_discard(XactCtl);
	LWLockAcquire(XactSLRULock, LW_EXCLUSIVE);
	(void)SimpleLruReadPage(XactCtl, 0, true, 100);
	LWLockRelease(XactSLRULock);
	UT_ASSERT(clog_relmap_mutation_snapshot(0, &after));
	UT_ASSERT_EQ(before, after);
	UT_ASSERT(!resident_birth_read(&base, &held));
	UT_ASSERT_EQ(base.queried_full_xid, 0);
	UT_ASSERT_EQ(transient_fds, 1);
	UT_ASSERT(held != NULL && held->failed);
	if (held != NULL) UT_ASSERT(held->base.resident_birth_observed);
	cluster_clog_relmap_base_release_v1(&held);
	relmap_base_reset(0);
	UT_ASSERT(resident_birth_read(&base, &held));
	UT_ASSERT(!base.resident_birth_observed);
	UT_ASSERT_EQ(base.resident_first_full_xid, 0);
	cluster_clog_relmap_base_release_v1(&held); fixture_idle();
}

static unsigned resident_birth_root_calls;
static void
resident_birth_drop_during_final_root(void)
{
	if (++resident_birth_root_calls == 2)
		clog_recovery_cache_discard(XactCtl);
}

UT_TEST(resident_birth_is_rechecked_after_original_final_root)
{
	ClusterRelmapClogBaseV1 base;
	ClusterRelmapClogBaseHandleV1 *held = NULL;
	resident_birth_prepare(true);
	resident_birth_root_calls = 0;
	relmap_base_recheck_hook = resident_birth_drop_during_final_root;
	UT_ASSERT(!resident_birth_read(&base, &held));
	UT_ASSERT_EQ(base.queried_full_xid, 0);
	UT_ASSERT(held == NULL);
	UT_ASSERT(!authority);
	UT_ASSERT_EQ(transient_fds, 0);
	relmap_base_recheck_hook = NULL;
	cluster_clog_relmap_base_release_v1(&held);
	fixture_idle();
}

UT_TEST(resident_birth_original_slru_remap_truncate_and_delete_clear_slots)
{
	int slot;
	resident_birth_prepare(true);
	for (int page = 1; page <= 8; page++) fixture_zero(page);
	LWLockAcquire(XactSLRULock, LW_EXCLUSIVE);
	/* A different native endpoint lets the original LRU evict page zero. */
	slot = ZeroCLOGPage(32, false);
	SimpleLruWritePage(XactCtl, slot);
	for (int page = 1; page <= 8; page++) {
		slot = SimpleLruReadPage(XactCtl, page, true, InvalidTransactionId);
		UT_ASSERT_EQ(XactCtl->shared->page_initial_full_xid[slot], 0);
	}
	slot = SimpleLruReadPage(XactCtl, 0, true, 100);
	UT_ASSERT_EQ(XactCtl->shared->page_initial_full_xid[slot], 0);
	LWLockRelease(XactSLRULock);
	for (int deletion = 0; deletion < 2; deletion++) {
		resident_birth_prepare(true);
		LWLockAcquire(XactSLRULock, LW_EXCLUSIVE);
		slot = ZeroCLOGPage(32, false);
		SimpleLruWritePage(XactCtl, slot);
		LWLockRelease(XactSLRULock);
		if (deletion) SlruDeleteSegment(XactCtl, 0);
		else SimpleLruTruncate(XactCtl, 32);
		LWLockAcquire(XactSLRULock, LW_EXCLUSIVE);
		for (int i = 0; i < XactCtl->shared->num_slots; i++) {
			if (XactCtl->shared->page_status[i] == SLRU_PAGE_EMPTY)
				UT_ASSERT_EQ(XactCtl->shared->page_initial_full_xid[i], 0);
		}
		LWLockRelease(XactSLRULock);
	}
	fixture_idle();
}
