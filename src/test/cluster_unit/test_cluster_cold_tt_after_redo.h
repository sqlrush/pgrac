/* Author: SqlRush <sqlrush@gmail.com>
 * Execute the real original TT file capture/readback over separate pre- and
 * post-redo images. File writes model completed redo; no top/loser proof.
 * Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md
 */
UT_TEST(test_after_redo_reads_changed_slots_without_overwriting_original_base)
{
	ClusterColdTypedV1 typed;
	UndoSegmentHeaderData header = header_for(1, 256);
	ClusterColdTtSlotInputV1 before, after;

	fixture(&typed);
	typed.system_identifier = typed.observer.database_incarnation = 1;
	header.tt_slots[0].status = TT_SLOT_ACTIVE;
	header.tt_slots[0].xid = 100;
	header.tt_slots[0].wrap = 3;
	write_header(256, &header, true);
	UT_ASSERT_EQ(cluster_cold_tt_base_capture_v1(&typed, INPUT_BUDGET), CLUSTER_COLD_OK);
	UT_ASSERT(cluster_cold_tt_base_slot_v1(&typed, 0, 0, &before));
	for (uint32 i = 0; i < TT_SLOTS_PER_SEGMENT; i++)
		memset(&header.tt_slots[i], i + 1, sizeof(TTSlot));
	write_header(256, &header, false);
	UT_ASSERT_EQ(cluster_cold_tt_after_redo_capture_v1(&typed), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(cluster_cold_tt_after_redo_count_v1(&typed), 1);
	for (uint32 i = 0; i < TT_SLOTS_PER_SEGMENT; i++) {
		UT_ASSERT(cluster_cold_tt_after_redo_slot_v1(&typed, 0, i, &after));
		UT_ASSERT_EQ(after.key.owner_instance, 1);
		UT_ASSERT_EQ(after.key.segment_id, 256);
		UT_ASSERT_EQ(after.key.segment_generation, 0);
		UT_ASSERT_EQ(after.key.slot_offset, i);
		UT_ASSERT_EQ(memcmp(&after.slot, &header.tt_slots[i], sizeof(TTSlot)), 0);
	}
	UT_ASSERT(cluster_cold_tt_base_slot_v1(&typed, 0, 0, &after));
	UT_ASSERT_EQ(memcmp(&after, &before, sizeof(before)), 0);
	UT_ASSERT_EQ(cluster_cold_tt_after_redo_recheck_v1(&typed), CLUSTER_COLD_OK);
	cluster_cold_tt_base_destroy_v1(&typed);
	UT_ASSERT_EQ(allocations, 0);
}

UT_TEST(test_after_redo_reenumerates_created_removed_and_history_origin_segments)
{
	ClusterColdTypedV1 typed;
	UndoSegmentHeaderData old = header_for(1, 1), added = header_for(128, 32768);
	ClusterColdTtSlotInputV1 slot;
	char path[MAXPGPATH];

	fixture(&typed);
	typed.system_identifier = typed.observer.database_incarnation = 1;
	typed.participant_count = 3;
	typed.participants[1].thread_id = typed.participants[2].thread_id = 128;
	typed.replay_count = 1;
	write_header(1, &old, true);
	UT_ASSERT_EQ(cluster_cold_tt_base_capture_v1(&typed, INPUT_BUDGET), CLUSTER_COLD_OK);
	file_path(1, path);
	UT_ASSERT_EQ(unlink(path), 0);
	added.wrap_count = 7;
	write_header(32768, &added, true);
	memset(visits, 0, sizeof(visits));
	UT_ASSERT_EQ(cluster_cold_tt_after_redo_capture_v1(&typed), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(cluster_cold_tt_after_redo_count_v1(&typed), 1);
	UT_ASSERT(cluster_cold_tt_after_redo_slot_v1(&typed, 0, TT_SLOTS_PER_SEGMENT - 1, &slot));
	UT_ASSERT_EQ(slot.key.owner_instance, 128);
	UT_ASSERT_EQ(slot.key.segment_id, 32768);
	UT_ASSERT_EQ(slot.key.segment_generation, 7);
	for (uint32 n = 0; n < 256; n++) {
		UT_ASSERT_EQ(visits[1][n], 1);
		UT_ASSERT_EQ(visits[128][n], 1);
	}
	UT_ASSERT_EQ(cluster_cold_tt_base_count_v1(&typed), 1);
	UT_ASSERT_EQ(cluster_cold_tt_after_redo_recheck_v1(&typed), CLUSTER_COLD_OK);
	cluster_cold_tt_base_destroy_v1(&typed);
}

UT_TEST(test_after_redo_readback_drift_is_sticky_and_keeps_original_input)
{
	for (int fault = 0; fault < 4; fault++) {
		ClusterColdTypedV1 typed;
		UndoSegmentHeaderData header = header_for(1, 1);
		ClusterColdTtSlotInputV1 out;
		char path[MAXPGPATH];

		fixture(&typed);
		typed.system_identifier = typed.observer.database_incarnation = 1;
		write_header(1, &header, true);
		UT_ASSERT_EQ(cluster_cold_tt_base_capture_v1(&typed, INPUT_BUDGET), CLUSTER_COLD_OK);
		UT_ASSERT_EQ(cluster_cold_tt_after_redo_capture_v1(&typed), CLUSTER_COLD_OK);
		if (fault == 0) {
			header._reserved[0] = 1;
			write_header(1, &header, false);
		} else if (fault == 1) {
			char retained[MAXPGPATH];

			file_path(1, path);
			snprintf(retained, sizeof(retained), "%s/retained", test_dir);
			UT_ASSERT_EQ(rename(path, retained), 0);
			write_header(1, &header, true);
		} else if (fault == 2)
			read_fault = 2;
		else
			write_header(2, &header, true); /* invalid present file, never absent */
		UT_ASSERT_EQ(cluster_cold_tt_after_redo_recheck_v1(&typed), CLUSTER_COLD_SOURCE_GAP);
		read_fault = 0;
		UT_ASSERT_EQ(cluster_cold_tt_after_redo_recheck_v1(&typed), CLUSTER_COLD_SOURCE_GAP);
		UT_ASSERT_EQ(cluster_cold_tt_after_redo_count_v1(&typed), 0);
		UT_ASSERT(!cluster_cold_tt_after_redo_slot_v1(&typed, 0, 0, &out));
		UT_ASSERT_EQ(cluster_cold_tt_base_count_v1(&typed), 1);
		cluster_cold_tt_base_destroy_v1(&typed);
	}
}

UT_TEST(test_after_redo_shares_original_tt_budget_and_discards_partial_capture)
{
	for (int fault = 0; fault < 2; fault++) {
		ClusterColdTypedV1 typed;
		UndoSegmentHeaderData first = header_for(1, 1), second = header_for(1, 2);

		fixture(&typed);
		typed.system_identifier = typed.observer.database_incarnation = 1;
		write_header(1, &first, true);
		write_header(2, &second, true);
		UT_ASSERT_EQ(cluster_cold_tt_base_capture_v1(&typed, INPUT_BUDGET), CLUSTER_COLD_OK);
		if (fault == 0)
			typed.tt_base->budget
				= typed.tt_base->used + sizeof(ClusterColdTtBaseV1) + sizeof(ColdTtInputV1);
		else
			alloc_fail = alloc_calls + 3; /* after one header has been retained */
		UT_ASSERT_EQ(cluster_cold_tt_after_redo_capture_v1(&typed),
					 fault == 0 ? CLUSTER_COLD_CAPACITY : CLUSTER_COLD_OOM);
		UT_ASSERT_EQ(cluster_cold_tt_after_redo_count_v1(&typed), 0);
		UT_ASSERT_EQ(cluster_cold_tt_base_count_v1(&typed), 2);
		cluster_cold_tt_base_destroy_v1(&typed);
		UT_ASSERT_EQ(allocations, 0);
	}
	{
		ClusterColdTypedV1 typed;
		UndoSegmentHeaderData header = header_for(1, 1);
		PGAlignedBlock block;
		uint32 generation;
		Size used;

		fixture(&typed);
		typed.system_identifier = typed.observer.database_incarnation = 1;
		write_header(1, &header, true);
		UT_ASSERT_EQ(cluster_cold_tt_base_capture_v1(&typed, INPUT_BUDGET), CLUSTER_COLD_OK);
		typed.tt_base->budget = typed.tt_base->used + sizeof(ClusterColdTtBaseV1)
								+ sizeof(ColdTtInputV1) + sizeof(ColdTtBlockV1) - 1;
		UT_ASSERT_EQ(cluster_cold_tt_after_redo_capture_v1(&typed), CLUSTER_COLD_OK);
		used = typed.tt_base->used;
		UT_ASSERT(!cluster_cold_tt_base_block_v1(&typed, 0, 1, &block, &generation));
		UT_ASSERT_EQ(typed.tt_base->detail, CLUSTER_COLD_CAPACITY);
		UT_ASSERT_EQ(typed.tt_base->used, used);
		UT_ASSERT(typed.tt_base->used + typed.tt_after_redo->used <= typed.tt_base->budget);
		UT_ASSERT_EQ(cluster_cold_tt_after_redo_count_v1(&typed), 0);
		cluster_cold_tt_base_destroy_v1(&typed);
		UT_ASSERT_EQ(allocations, 0);
	}
}

UT_TEST(test_after_redo_requires_original_namespace_and_base)
{
	for (int fault = 0; fault < 5; fault++) {
		ClusterColdTypedV1 typed;

		fixture(&typed);
		typed.system_identifier = typed.observer.database_incarnation = 1;
		if (fault != 0)
			UT_ASSERT_EQ(cluster_cold_tt_base_capture_v1(&typed, INPUT_BUDGET), CLUSTER_COLD_OK);
		if (fault == 1)
			typed.system_identifier++;
		if (fault == 2)
			typed.observer.database_incarnation++;
		if (fault == 3)
			typed.participants[0].thread_id = 2;
		if (fault == 4)
			typed.tt_base->detail = CLUSTER_COLD_SOURCE_GAP;
		UT_ASSERT(cluster_cold_tt_after_redo_capture_v1(&typed) != CLUSTER_COLD_OK);
		UT_ASSERT_EQ(cluster_cold_tt_after_redo_count_v1(&typed), 0);
		cluster_cold_tt_base_destroy_v1(&typed);
	}
}

UT_TEST(test_after_redo_failed_io_never_exports_partial_slots)
{
	for (int fault = 0; fault < 3; fault++) {
		ClusterColdTypedV1 typed;
		UndoSegmentHeaderData header = header_for(1, 1);

		fixture(&typed);
		typed.system_identifier = typed.observer.database_incarnation = 1;
		write_header(1, &header, true);
		UT_ASSERT_EQ(cluster_cold_tt_base_capture_v1(&typed, INPUT_BUDGET), CLUSTER_COLD_OK);
		if (fault == 0)
			read_fault = 2;
		else if (fault == 1)
			close_fault = 1;
		else
			open_error = EACCES;
		UT_ASSERT_EQ(cluster_cold_tt_after_redo_capture_v1(&typed), CLUSTER_COLD_SOURCE_GAP);
		UT_ASSERT_EQ(cluster_cold_tt_after_redo_count_v1(&typed), 0);
		cluster_cold_tt_base_destroy_v1(&typed);
	}
}

UT_TEST(test_after_redo_reset_and_bounds_clear_only_post_redo_outputs)
{
	ClusterColdTypedV1 typed;
	UndoSegmentHeaderData header = header_for(1, 1);
	ClusterColdTtSlotInputV1 slot, zero = { 0 };

	fixture(&typed);
	typed.system_identifier = typed.observer.database_incarnation = 1;
	write_header(1, &header, true);
	UT_ASSERT_EQ(cluster_cold_tt_base_capture_v1(&typed, INPUT_BUDGET), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(cluster_cold_tt_after_redo_capture_v1(&typed), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(cluster_cold_tt_after_redo_capture_v1(&typed), CLUSTER_COLD_STATE);
	for (int fault = 0; fault < 3; fault++) {
		memset(&slot, 0xA5, sizeof(slot));
		UT_ASSERT(
			!cluster_cold_tt_after_redo_slot_v1(fault == 0 ? NULL : &typed, fault == 1 ? 1 : 0,
												fault == 2 ? TT_SLOTS_PER_SEGMENT : 0, &slot));
		UT_ASSERT_EQ(memcmp(&slot, &zero, sizeof(slot)), 0);
	}
	UT_ASSERT(!cluster_cold_tt_after_redo_slot_v1(&typed, 0, 0, NULL));
	cluster_cold_tt_after_redo_reset_v1(&typed);
	UT_ASSERT_EQ(cluster_cold_tt_after_redo_count_v1(&typed), 0);
	UT_ASSERT_EQ(cluster_cold_tt_base_count_v1(&typed), 1);
	UT_ASSERT_EQ(cluster_cold_tt_after_redo_capture_v1(&typed), CLUSTER_COLD_OK);
	cluster_cold_tt_after_redo_reset_v1(NULL);
	cluster_cold_tt_base_destroy_v1(&typed);
	UT_ASSERT_EQ(allocations, 0);
}

UT_TEST(test_after_redo_namespace_drift_hides_captured_outputs)
{
	ClusterColdTypedV1 typed;
	UndoSegmentHeaderData header = header_for(1, 1);
	ClusterColdTtSlotInputV1 slot;

	fixture(&typed);
	typed.system_identifier = typed.observer.database_incarnation = 1;
	write_header(1, &header, true);
	UT_ASSERT_EQ(cluster_cold_tt_base_capture_v1(&typed, INPUT_BUDGET), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(cluster_cold_tt_after_redo_capture_v1(&typed), CLUSTER_COLD_OK);
	typed.system_identifier++;
	UT_ASSERT_EQ(cluster_cold_tt_after_redo_count_v1(&typed), 0);
	UT_ASSERT(!cluster_cold_tt_after_redo_slot_v1(&typed, 0, 0, &slot));
	UT_ASSERT_EQ(cluster_cold_tt_after_redo_recheck_v1(&typed), CLUSTER_COLD_SOURCE_GAP);
	cluster_cold_tt_base_destroy_v1(&typed);
}
