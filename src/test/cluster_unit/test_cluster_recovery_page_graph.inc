/*-------------------------------------------------------------------------
 * test_cluster_recovery_page_graph.inc
 *   Original physical WAL and queued PAGE codecs, without graph stubs.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */

#include "storage/lwlock.h"
static unsigned inventory_graph_fault;
static bool inventory_graph_caller_lock;

/* Native lock-list enumeration is a scheduler leaf, not an owner matcher. */
void
ForEachLWLockHeldByMe(void (*callback)(LWLock *, LWLockMode, void *), void *context)
{
	static LWLock caller_lock;
	if (inventory_graph_caller_lock) callback(&caller_lock, LW_EXCLUSIVE, context);
}

static WalTestRecord
inventory_graph_page(const ClusterWalSourceRef *source, XLogRecPtr at,
	XLogRecPtr previous, unsigned node)
{
	uint8 bytes[2048] = {0};
	uint8 *wire = bytes + SizeOfXLogRecord;
	XLogRecord record = {0};
	XLogRecordBlockHeader block = {.id = 0, .fork_flags = MAIN_FORKNUM | BKPBLOCK_HAS_IMAGE};
	XLogRecordBlockImageHeader image = {.length = SizeOfPageHeaderData,
		.hole_offset = SizeOfPageHeaderData, .bimg_info = BKPIMAGE_HAS_HOLE | BKPIMAGE_APPLY};
	RelFileLocator locator = {1663, 5, 16386};
	BlockNumber number = 7;
	PGAlignedBlock page = {0};
	uint64 before = node == 0 ? 70 : 77, after = node == 0 ? 77 : 78;
	size_t used = SizeOfXLogRecord + XLR_PAGE_VERSION_EDGE_HEADER_SIZE
		+ XLR_PAGE_VERSION_EDGE_ENTRY_SIZE;
	char path[MAXPGPATH];
	int fd;

	if (inventory_graph_fault == 1) before = after;
	wire[0] = XLR_BLOCK_ID_PAGE_VERSION_EDGE; wire[1] = XLR_PAGE_VERSION_EDGE_FORMAT_V1;
	wire[2] = 1; wire[3] = XLR_PAGE_VERSION_EDGE_ENTRY_SIZE;
	memcpy(wire + 8, &after, sizeof(after)); wire += XLR_PAGE_VERSION_EDGE_HEADER_SIZE;
	wire[1] = RF_PAGE_CLASS_ORDINARY; wire[2] = wire[3] = RF_PAGE_STATE_PRESENT;
	wire[4] = RF_PAGE_EDGE_FULL_IMAGE_APPLY | RF_PAGE_EDGE_FULL_COVERAGE;
	memset(wire + 8, 0x21, 16); memcpy(wire + 24, &before, sizeof(before));
	memset(wire + 32, 0x21, 16);
	memcpy(bytes + used, &block, SizeOfXLogRecordBlockHeader); used += SizeOfXLogRecordBlockHeader;
	memcpy(bytes + used, &image, SizeOfXLogRecordBlockImageHeader); used += SizeOfXLogRecordBlockImageHeader;
	memcpy(bytes + used, &locator, sizeof(locator)); used += sizeof(locator);
	memcpy(bytes + used, &number, sizeof(number)); used += sizeof(number);
	((PageHeader)page.data)->pd_lower = SizeOfPageHeaderData;
	((PageHeader)page.data)->pd_upper = ((PageHeader)page.data)->pd_special = BLCKSZ;
	((PageHeader)page.data)->pd_block_scn = after;
	PageSetPageSizeAndVersion(page.data, BLCKSZ, PG_PAGE_LAYOUT_VERSION);
	memcpy(bytes + used, page.data, SizeOfPageHeaderData); used += SizeOfPageHeaderData;
	record.xl_tot_len = used; record.xl_prev = previous;
	record.xl_rmid = RM_GENERIC_ID;
	record.xl_info = inventory_graph_fault == 2 ? 0x10 : 0;
	record.xl_scn = 5906;
	INIT_CRC32C(record.xl_crc);
	COMP_CRC32C(record.xl_crc, bytes + SizeOfXLogRecord, used - SizeOfXLogRecord);
	COMP_CRC32C(record.xl_crc, &record, offsetof(XLogRecord, xl_crc)); FIN_CRC32C(record.xl_crc);
	memcpy(bytes, &record, SizeOfXLogRecord);
	v2_checkpoint_wal_path(&source->claim.identity, at, source->timeline, path);
	UT_ASSERT(at % XLOG_BLCKSZ + used < XLOG_BLCKSZ);
	fd = open(path, O_WRONLY); UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(pwrite(fd, bytes, used, at % wal_segment_size), used);
	UT_ASSERT_EQ(close(fd), 0);
	return (WalTestRecord){1, MAXALIGN(at + used), at, record.xl_crc};
}

/* Existing contribution fixture supplies real threads1/2. ROOT-only fixtures
 * deliberately exercise thread128, outside the unchanged PAGE origin ABI. */
static ClusterWalInputsV1 *
inventory_graph_fixture(bool page)
{
	ClusterWalInputsV1 *inputs = inputs_contribution_fixture(-1);
	ControlRootImage root;
	ControlFileData common;
	ClusterControlRootFileToken token;

	cluster_wal_inputs_release_v1(&inputs);
	test_actual_cf = ShareLock;
	UT_ASSERT_EQ(cluster_control_root_v3_read_control_locked(v2_storage, TEST_SYSID,
		&root, &common, &token), 0);
	for (unsigned node = 0; node < 2; node++) {
		ClusterControlRootSnapshot *snapshot = &root.records[node];
		ClusterWalSourceRef source = {0};
		WalTestRecord commit, extra, tail;
		xl_smgr_create payload = {.rlocator = {1663, 5, 16385}, .forkNum = MAIN_FORKNUM};

		source.claim.identity = snapshot->identity; source.timeline = snapshot->tail_tli;
		commit = inventory_write_commit(&source, snapshot->validated_tail_lsn_exclusive,
			snapshot->tail_last_record_lsn, 8765, false);
		extra = page ? inventory_graph_page(&source, commit.exclusive_end, commit.record_start, node)
			: inventory_write_record(&source, commit.exclusive_end, commit.record_start,
				RM_XLOG_ID, XLOG_NOOP, "abc", 3);
		tail = inventory_write_record(&source, extra.exclusive_end, extra.record_start,
			RM_SMGR_ID, XLOG_SMGR_CREATE, &payload, sizeof(payload));
		if (node == 0) {
			inventory_fault_source = source; inventory_commit_start = commit.record_start;
			inventory_commit_previous = snapshot->tail_last_record_lsn;
		}
		snapshot->tail_last_record_lsn = tail.record_start;
		snapshot->tail_last_record_crc32c = tail.record_crc;
		snapshot->validated_tail_lsn_exclusive = tail.exclusive_end;
	}
	UT_ASSERT_EQ(cluster_control_root_v3_encode(&root), 0); v2_write_roots(root.bytes);
	test_actual_cf = NoLock; test_inputs_startup_cf = true;
	UT_ASSERT_EQ(inputs_begin_for_role(true, &inputs), 0);
	inventory_census_calls = 0; inventory_between_passes = NULL;
	return inputs;
}

UT_TEST(test_page_graph_original_full_cuts_order_cross_thread_page_edges)
{
	ClusterRecoveryNativeInventory *inventory = NULL;
	ClusterRecoveryNativeInventoryDiag diag;
	ClusterRecoveryPagePlanBuildV1 built;
	ClusterRecoveryPageWalV1 wal;
	RfPageOnlineTargetViewV1 target;
	ClusterWalInputsV1 *inputs;
	uint32 sources = 0;
	inputs = inventory_graph_fixture(true);
	UT_ASSERT_EQ(cluster_recovery_native_inventory_build(inputs, 1024 * 1024, &inventory, &diag), 0);
	built = cluster_recovery_native_inventory_page_plan_v1(inputs, inventory, 1024 * 1024);
	UT_ASSERT_EQ(built.detail, RF_PAGE_PROOF_DETAIL_OK); UT_ASSERT(built.plan != NULL);
	UT_ASSERT_EQ(inventory_census_calls, 3);
	UT_ASSERT(cluster_recovery_native_inventory_source_count_v1(inventory, &sources));
	UT_ASSERT_EQ(sources, 2);
	UT_ASSERT_EQ(rf_page_online_plan_target_count_v1(built.plan), 1);
	UT_ASSERT(rf_page_online_plan_target_v1(built.plan, 0, &target));
	if (built.plan != NULL && rf_page_online_plan_target_v1(built.plan, 0, &target)) {
		int origin = -1;

		UT_ASSERT_EQ(target.expected_before.mutation_token, 70);
		UT_ASSERT_EQ(target.expected_result.mutation_token, 78);
		UT_ASSERT_EQ(target.contributors->participant_count, sources);
		UT_ASSERT_EQ(target.contributors->edge_count, 2);
		UT_ASSERT_EQ(target.contributors->edges[0].record_identity.origin_thread, 1);
		UT_ASSERT_EQ(target.contributors->edges[1].record_identity.origin_thread, 2);
		UT_ASSERT_EQ(((PageHeader)target.canonical_page)->pd_block_scn, 78);
		UT_ASSERT(PageGetLSNOrigin((Page)target.canonical_page, &origin));
		UT_ASSERT_EQ(origin, 1);
		for (uint32 i = 0; i < sources; i++) {
			ClusterWalSourceRef bound;
			ClusterRecoveryWalSourceObservationV1 original;
			UT_ASSERT(rf_page_online_plan_source_v1(built.plan, i, &bound));
			UT_ASSERT(cluster_recovery_native_inventory_source_at_v1(inventory, i, &original));
			UT_ASSERT(memcmp(&bound, &original.source, sizeof(bound)) == 0);
			UT_ASSERT(cluster_recovery_native_inventory_page_at_v1(inventory, i, &wal));
			/* Original full cuts extend through the SMGR tail, after PAGE. */
			UT_ASSERT(wal.reader->EndRecPtr < original.cut.scan_end_exclusive);
			UT_ASSERT_EQ(target.contributors->cuts[i].scan_end_exclusive, wal.reader->EndRecPtr);
			UT_ASSERT_EQ(target.contributors->cuts[i].origin_owner_incarnation,
				original.cut.origin_owner_incarnation);
		}
		UT_ASSERT_EQ(PageGetLSN((Page)target.canonical_page), wal.reader->EndRecPtr);
	}
	rf_page_online_plan_destroy_v1(&built.plan);
	UT_ASSERT(inventory->complete);
	cluster_recovery_native_inventory_destroy(inventory); cold_census_done(&inputs);
	inventory_extra = 0;
}

UT_TEST(test_page_graph_no_page_sources_and_original_native_cursor_remain)
{
	ClusterRecoveryNativeInventory *inventory = NULL;
	ClusterRecoveryNativeInventoryDiag diag;
	ClusterRecoveryNativeEffect effect;
	ClusterRecoveryPagePlanBuildV1 built;
	ClusterWalInputsV1 *inputs;
	inputs = inventory_graph_fixture(false);
	UT_ASSERT_EQ(cluster_recovery_native_inventory_build(inputs, 1024 * 1024, &inventory, &diag), 0);
	UT_ASSERT_EQ(cluster_recovery_native_inventory_next(inventory, &effect), CLUSTER_RECOVERY_NATIVE_INVENTORY_EFFECT);
	UT_ASSERT_EQ(effect.kind, CLUSTER_RECOVERY_NATIVE_TERMINAL);
	for (unsigned attempt = 0; attempt < 2; attempt++) {
		built = cluster_recovery_native_inventory_page_plan_v1(inputs, inventory, 1024 * 1024);
		UT_ASSERT_EQ(built.detail, RF_PAGE_PROOF_DETAIL_OK); UT_ASSERT(built.plan != NULL);
		UT_ASSERT_EQ(rf_page_online_plan_target_count_v1(built.plan), 0);
		for (uint32 i = 0; i < inventory->source_count; i++) {
			ClusterWalSourceRef bound;
			UT_ASSERT(rf_page_online_plan_source_v1(built.plan, i, &bound));
			UT_ASSERT(inventory_source_equal(&bound, &inventory->sources[i].source));
		}
		rf_page_online_plan_destroy_v1(&built.plan);
	}
	UT_ASSERT_EQ(cluster_recovery_native_inventory_next(inventory, &effect), CLUSTER_RECOVERY_NATIVE_INVENTORY_EFFECT);
	UT_ASSERT_EQ(effect.kind, CLUSTER_RECOVERY_NATIVE_INVALIDATION);
	UT_ASSERT_EQ(inventory_census_calls, 4);
	cluster_recovery_native_inventory_destroy(inventory); cold_census_done(&inputs);
	inventory_extra = 0;
}

UT_TEST(test_page_graph_keeps_explicit_empty_and_historical_generations)
{
	int saved_node = cluster_node_id;
	uint64 saved_incarnation = test_self_incarnation;
	ClusterRecoverySerialGuard serial;
	ClusterThreadRecLaunchEligibility eligibility;
	ClusterWalInputsV1 *inputs = NULL;
	ClusterRecoveryNativeInventory *inventory = NULL;
	ClusterRecoveryNativeInventoryDiag diag;
	ClusterRecoveryPagePlanBuildV1 built = {0};
	ControlRootImage root;
	ControlFileData control;
	ClusterControlRootFileToken token;
	unsigned empty = 0;
	(void)pending_worker_fixture(&serial, &eligibility);
	if (ut_current_failed) goto cleanup;
	test_worker_pin_held = false; test_history_sync_count = 0; test_history_fail_sync = UINT_MAX;
	UT_ASSERT_EQ(thread_recovery_worker_run(&eligibility), CLUSTER_THREADREC_DONE);
	test_history_fail_sync = 0; test_actual_cf = test_cf_mode = ShareLock;
	UT_ASSERT_EQ(cluster_control_root_v3_read_control_locked(v2_storage, TEST_SYSID, &root, &control, &token), 0);
	memset(&root.startup[3], 0, sizeof(root.startup[3]));
	UT_ASSERT_EQ(cluster_control_root_v3_encode(&root), 0); v2_write_roots(root.bytes);
	test_actual_cf = test_cf_mode = NoLock; test_inputs_startup_cf = true;
	UT_ASSERT_EQ(inputs_begin_for_role(true, &inputs), 0);
	if (inputs == NULL) goto cleanup;
	inventory_census_calls = 0; inventory_between_passes = NULL;
	UT_ASSERT_EQ(cluster_recovery_native_inventory_build(inputs, 1024 * 1024, &inventory, &diag), 0);
	built = cluster_recovery_native_inventory_page_plan_v1(inputs, inventory, 1024 * 1024);
	UT_ASSERT_EQ(built.detail, RF_PAGE_PROOF_DETAIL_OK); UT_ASSERT(built.plan != NULL);
	for (uint32 i = 0; inventory != NULL && i < inventory->source_count; i++) {
		ClusterWalSourceRef source;
		UT_ASSERT(rf_page_online_plan_source_v1(built.plan, i, &source));
		UT_ASSERT(inventory_source_equal(&source, &inventory->sources[i].source));
		if (inventory->sources[i].cut.flags & RF_CONTRIBUTOR_CUT_EXPLICIT_EMPTY) empty++;
	}
	UT_ASSERT_EQ(empty, 1);
	UT_ASSERT_EQ(inventory->source_count, cluster_wal_inputs_count_v1(inputs));
	UT_ASSERT_EQ(inventory_census_calls, 3);
cleanup:
	rf_page_online_plan_destroy_v1(&built.plan);
	cluster_recovery_native_inventory_destroy(inventory); cold_census_done(&inputs);
	test_reserve_mode = false; cluster_node_id = saved_node; test_self_incarnation = saved_incarnation;
}

UT_TEST(test_page_graph_budget_refuses_without_exporting_a_prefix)
{
	ClusterRecoveryNativeInventory *inventory = NULL;
	ClusterRecoveryNativeInventoryDiag diag;
	ClusterRecoveryPagePlanBuildV1 built;
	ClusterWalInputsV1 *inputs;
	inputs = inventory_graph_fixture(true);
	UT_ASSERT_EQ(cluster_recovery_native_inventory_build(inputs, 1024 * 1024, &inventory, &diag), 0);
	for (unsigned attempt = 0; attempt < 2; attempt++) {
		built = cluster_recovery_native_inventory_page_plan_v1(inputs, inventory,
			attempt ? RF_PAGE_ONLINE_PLAN_MAX_BYTES + 1 : 1);
		UT_ASSERT(built.plan == NULL); UT_ASSERT(built.detail != RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT(inventory->complete); UT_ASSERT_EQ(inventory_census_calls, 2);
	}
	built = cluster_recovery_native_inventory_page_plan_v1(inputs, inventory, 20000);
	UT_ASSERT(built.plan == NULL); UT_ASSERT_EQ(built.detail, RF_PAGE_PROOF_DETAIL_CAPACITY);
	UT_ASSERT(!inventory->complete);
	cluster_recovery_native_inventory_destroy(inventory); cold_census_done(&inputs);
	inventory_extra = 0;
}

UT_TEST(test_page_graph_crc_valid_original_rewrite_is_sticky)
{
	ClusterRecoveryNativeInventory *inventory = NULL;
	ClusterRecoveryNativeInventoryDiag diag;
	ClusterRecoveryPagePlanBuildV1 built;
	ClusterWalInputsV1 *inputs;
	inputs = inventory_graph_fixture(true);
	UT_ASSERT_EQ(cluster_recovery_native_inventory_build(inputs, 1024 * 1024, &inventory, &diag), 0);
	inventory_fault = 0; inventory_drift();
	built = cluster_recovery_native_inventory_page_plan_v1(inputs, inventory, 1024 * 1024);
	UT_ASSERT(built.plan == NULL); UT_ASSERT(built.detail != RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(!inventory->complete);
	built = cluster_recovery_native_inventory_page_plan_v1(inputs, inventory, 1024 * 1024);
	UT_ASSERT(built.plan == NULL); UT_ASSERT(built.detail != RF_PAGE_PROOF_DETAIL_OK);
	cluster_recovery_native_inventory_destroy(inventory); cold_census_done(&inputs);
	inventory_extra = 0;
}

UT_TEST(test_page_graph_bad_codec_and_cycle_keep_original_debt)
{
	for (unsigned fault = 1; fault <= 2; fault++) {
		ClusterRecoveryNativeInventory *inventory = NULL;
		ClusterRecoveryNativeInventoryDiag diag;
		ClusterRecoveryPagePlanBuildV1 built;
		ClusterWalInputsV1 *inputs;
		inventory_graph_fault = fault;
		inputs = inventory_graph_fixture(true);
		UT_ASSERT_EQ(cluster_recovery_native_inventory_build(inputs, 1024 * 1024, &inventory, &diag), 0);
		built = cluster_recovery_native_inventory_page_plan_v1(inputs, inventory, 1024 * 1024);
		UT_ASSERT(built.plan == NULL); UT_ASSERT(built.detail != RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT(!inventory->complete);
		UT_ASSERT(inputs_pin_held); UT_ASSERT_EQ(inputs_pin_count, 2);
		cluster_recovery_native_inventory_destroy(inventory); cold_census_done(&inputs);
	}
	inventory_graph_fault = inventory_extra = 0;
}

UT_TEST(test_page_graph_changed_native_checkpoint_never_rebases)
{
	ClusterRecoveryNativeInventory *inventory = NULL;
	ClusterRecoveryNativeInventoryDiag diag;
	ClusterRecoveryPagePlanBuildV1 built;
	ClusterWalInputsV1 *inputs;
	inputs = inventory_graph_fixture(true);
	UT_ASSERT_EQ(cluster_recovery_native_inventory_build(inputs, 1024 * 1024, &inventory, &diag), 0);
	inventory->sources[0].history.input.native_redo++;
	built = cluster_recovery_native_inventory_page_plan_v1(inputs, inventory, 1024 * 1024);
	UT_ASSERT(built.plan == NULL); UT_ASSERT(built.detail != RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(!inventory->complete);
	cluster_recovery_native_inventory_destroy(inventory); cold_census_done(&inputs);
	inventory_extra = 0;
}

UT_TEST(test_page_graph_native_error_cleans_private_state_only)
{
	ClusterRecoveryNativeInventory *inventory = NULL;
	ClusterRecoveryNativeInventoryDiag diag;
	ClusterWalInputsV1 *inputs;
	volatile bool caught = false;
	inputs = inventory_graph_fixture(true);
	UT_ASSERT_EQ(cluster_recovery_native_inventory_build(inputs, 1024 * 1024, &inventory, &diag), 0);
	test_throw_root_read = true;
	PG_TRY();
	{
		ClusterRecoveryPagePlanBuildV1 built
			= cluster_recovery_native_inventory_page_plan_v1(inputs, inventory, 1024 * 1024);
		rf_page_online_plan_destroy_v1(&built.plan);
	}
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	test_throw_root_read = false;
	UT_ASSERT(caught); UT_ASSERT(!inventory->complete);
	UT_ASSERT(inventory->status != CLUSTER_RECOVERY_NATIVE_INVENTORY_OBSERVED);
	cluster_recovery_native_inventory_destroy(inventory); cold_census_done(&inputs);
	inventory_extra = 0;
}

UT_TEST(test_page_graph_refuses_caller_locks_before_any_original_io)
{
	ClusterRecoveryNativeInventory *inventory = NULL;
	ClusterRecoveryNativeInventoryDiag diag;
	ClusterRecoveryPagePlanBuildV1 built;
	ClusterWalInputsV1 *inputs = inventory_graph_fixture(true);
	UT_ASSERT_EQ(cluster_recovery_native_inventory_build(inputs, 1024 * 1024, &inventory, &diag), 0);
	inventory_graph_caller_lock = true;
	built = cluster_recovery_native_inventory_page_plan_v1(inputs, inventory, 1024 * 1024);
	inventory_graph_caller_lock = false;
	UT_ASSERT(built.plan == NULL); UT_ASSERT_EQ(built.detail, RF_PAGE_PROOF_DETAIL_WOULD_BLOCK);
	UT_ASSERT_EQ(inventory_census_calls, 2); UT_ASSERT(inventory->complete);
	rf_page_online_plan_destroy_v1(&built.plan);
	cluster_recovery_native_inventory_destroy(inventory); cold_census_done(&inputs);
}
