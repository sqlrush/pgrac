/* Original C0/slot owner is real; native writer, physical TT and I/O remain
 * the existing fixture leaves. A returned value is never graph authority. */
static void
ctrc_terminal_subject_fixture(ClusterRecoveryUndoEntry *physical, bool accepted)
{
	c0_subject();
	ctrc_census.subject_count = ctrc_census.unresolved_count = 1;
	UT_ASSERT(cluster_cold_recovery_consume_loser_inputs_v1(info.cluster_tx_start));
	/* The retained-native loader is a fixture leaf; preserve its original
	 * pre-C0 input, which the real consumer deliberately did not overwrite. */
	native_ctrc_work.entries[0] = census.entries[0].input;
	memset(physical, 0, sizeof(*physical));
	physical->owner_instance = native_ctrc_work.entries[0].key.owner_instance;
	physical->segment_id = native_ctrc_work.entries[0].key.segment_id;
	physical->segment_generation = native_ctrc_work.entries[0].key.segment_generation;
	physical->slot_offset = native_ctrc_work.entries[0].key.slot_offset;
	physical->slot = native_ctrc_work.entries[0].slot;
	physical->slot.status = TT_SLOT_ABORTED; /* literal expected original C0 successor */
	if (accepted) {
		serving_actor(); UT_ASSERT(cluster_recovery_tx_result_accept_v1()); startup_actor();
	}
}

UT_TEST(test_ctrc_terminal_subject_uses_actual_c0_and_exact_accepted_result)
{
	ClusterRecoveryUndoEntry physical;
	ClusterColdTtSlotInputV1 out, unchanged, original, expected;
	ClusterRecoveryTxResultValueV1 tx;

	ctrc_terminal_subject_fixture(&physical, false);
	original = native_ctrc_work.entries[0];
	memset(&out, 0xa5, sizeof(out)); unchanged = out;
	UT_ASSERT(!cluster_recovery_ctrc_terminal_subject_v1(info.cluster_tx_start,
		&native_ctrc_work, 0, &physical, &out));
	UT_ASSERT(memcmp(&out, &unchanged, sizeof(out)) == 0);
	serving_actor(); UT_ASSERT(cluster_recovery_tx_result_accept_v1()); startup_actor();
	tx = window_region.checkpoint_owner.tx_result;
	expected = original; expected.slot.status = TT_SLOT_ABORTED;
	UT_ASSERT(cluster_recovery_ctrc_terminal_subject_v1(info.cluster_tx_start,
		&native_ctrc_work, 0, &physical, &out));
	UT_ASSERT(memcmp(&out, &expected, sizeof(out)) == 0);
	UT_ASSERT_EQ(out.slot.status, TT_SLOT_ABORTED);
	UT_ASSERT_EQ(original.slot.status, TT_SLOT_ACTIVE);
	UT_ASSERT(memcmp(&native_ctrc_work.entries[0], &original, sizeof(original)) == 0);
	UT_ASSERT(memcmp(&tx, &window_region.checkpoint_owner.tx_result, sizeof(tx)) == 0);
	UT_ASSERT_EQ(native_ctrc_work.counts.unresolved_count, 1);
	UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_result.state, CLUSTER_RECOVERY_CTRC_RESULT_PENDING);
	UT_ASSERT_EQ(wal_inserts, 1); UT_ASSERT_EQ(stamp_writes, 1); UT_ASSERT_EQ(stamp_closes, 1);
}

UT_TEST(test_ctrc_terminal_subject_rejects_changed_physical_and_original_identity)
{
	for (unsigned fault = 0; fault < 16; fault++) {
		ClusterRecoveryUndoEntry physical;
		ClusterColdTtSlotInputV1 out, unchanged;
		ctrc_terminal_subject_fixture(&physical, true);
		switch (fault) {
		case 0: physical.owner_instance++; break;
		case 1: physical.segment_id++; break;
		case 2: physical.segment_generation++; break;
		case 3: physical.slot_offset++; break;
		case 4: physical.slot.xid++; break;
		case 5: physical.slot.wrap++; break;
		case 6: physical.slot.status = TT_SLOT_ACTIVE; break;
		case 7: physical.slot.status = TT_SLOT_COMMITTED; physical.slot.commit_scn = 51; break;
		case 8: physical.slot.flags |= TT_SLOT_FLAG_CTRC_RELEASE_PROVEN; break;
		case 9: physical.slot.commit_scn = 71; break;
		case 10: physical.slot.first_undo_block.raw[0]++; break;
		case 11: native_ctrc_work.entries[0].key.xid++; break;
		case 12: native_ctrc_work.entries[0].key.segment_generation++; break;
		case 13: native_ctrc_work.entries[0].slot.first_undo_block.raw[0]++; break;
		case 14:
			census.entries[1] = census.entries[0]; census.count = census.proven_count = 2;
			window_region.checkpoint_owner.tx_result.subject_count = 2;
			window_region.checkpoint_owner.tx_result.processed_count = 2;
			window_region.checkpoint_owner.tx_result.completed_count = 2;
			break;
		case 15: native_ctrc_work.counts.unqualified_count = 1; break;
		}
		memset(&out, 0xa5, sizeof(out)); unchanged = out;
		UT_ASSERT(!cluster_recovery_ctrc_terminal_subject_v1(info.cluster_tx_start,
			&native_ctrc_work, 0, &physical, &out));
		UT_ASSERT(memcmp(&out, &unchanged, sizeof(out)) == 0);
	}
}

UT_TEST(test_ctrc_terminal_subject_requires_complete_current_original_components)
{
	for (unsigned fault = 0; fault < 13; fault++) {
		ClusterRecoveryUndoEntry physical;
		ClusterColdTtSlotInputV1 out, unchanged;
		ctrc_terminal_subject_fixture(&physical, true);
		switch (fault) {
		case 0: window_region.checkpoint_owner.tx_result.state = CLUSTER_RECOVERY_TX_RESULT_PENDING; break;
		case 1: window_region.checkpoint_owner.tx_accepted_generation++; break;
		case 2: window_region.checkpoint_owner.tx_result.input_root_sha256[7]++; break;
		case 3: window_region.checkpoint_owner.tx_result.subject_count++; break;
		case 4: window_region.checkpoint_owner.tx_result.processed_count = 0; break;
		case 5: typed.ctrc_work_generation++; break;
		case 6: CurrentResourceOwner = NULL; break;
		case 7: owner_boot++; break;
		case 8: native_recovery = true; break;
		case 9: typed.census_root.image_sha256[4]++; break;
		case 10: CritSectionCount = 1; break;
		case 11: census.entries[0].proof.qualified_base = false; break;
		case 12: window_region.checkpoint_owner.tx_result.unresolved_count = 1; break;
		}
		memset(&out, 0xa5, sizeof(out)); unchanged = out;
		UT_ASSERT(!cluster_recovery_ctrc_terminal_subject_v1(info.cluster_tx_start,
			&native_ctrc_work, 0, &physical, &out));
		UT_ASSERT(memcmp(&out, &unchanged, sizeof(out)) == 0);
	}
}

UT_TEST(test_ctrc_terminal_subject_preserves_preexisting_terminal_bytes)
{
	for (unsigned committed = 0; committed < 2; committed++) {
		ClusterColdTtSlotInputV1 original = {0}, out;
		ClusterRecoveryUndoEntry physical = {0};
		reset(); retain(); UT_ASSERT(ClusterBeginColdRecoveryTx(&info));
		ctrc_census.subject_count = ctrc_census.unresolved_count = 1;
		UT_ASSERT(cluster_cold_recovery_consume_loser_inputs_v1(info.cluster_tx_start));
		original.key.owner_instance = 2; original.key.segment_id = 257;
		original.key.segment_generation = 3; original.key.slot_offset = 1;
		original.key.xid = 110; original.key.slot_wrap = 4;
		original.slot.xid = 110; original.slot.wrap = 4;
		original.slot.status = committed ? TT_SLOT_COMMITTED : TT_SLOT_ABORTED;
		original.slot.commit_scn = committed ? 61 : InvalidScn;
		original.slot.first_undo_block = uba_encode(257, 3, 2, 16);
		native_ctrc_work.entries[0] = original;
		physical.owner_instance = 2; physical.segment_id = 257;
		physical.segment_generation = 3; physical.slot_offset = 1; physical.slot = original.slot;
		serving_actor(); UT_ASSERT(cluster_recovery_tx_result_accept_v1()); startup_actor();
		UT_ASSERT(cluster_recovery_ctrc_terminal_subject_v1(info.cluster_tx_start,
			&native_ctrc_work, 0, &physical, &out));
		UT_ASSERT(memcmp(&out, &original, sizeof(out)) == 0);
		physical.slot.flags ^= TT_SLOT_FLAG_CTRC_RELEASE_PROVEN;
		UT_ASSERT(!cluster_recovery_ctrc_terminal_subject_v1(info.cluster_tx_start,
			&native_ctrc_work, 0, &physical, &out));
		UT_ASSERT(memcmp(&out, &original, sizeof(out)) == 0);
		UT_ASSERT_EQ(wal_inserts + stamp_writes, 0);
	}
}

UT_TEST(test_ctrc_terminal_subject_refuses_aliases_and_late_owner_change)
{
	ClusterRecoveryUndoEntry physical, original_physical;
	ClusterColdTtSlotInputV1 original, out, unchanged;
	ClusterColdTopTxCensusEntryV1 original_top;
	ClusterColdTypedV1 original_typed;
	struct ClusterRecoveryStartV1 original_start;

	ctrc_terminal_subject_fixture(&physical, true);
	original = native_ctrc_work.entries[0]; original_physical = physical;
	original_top = census.entries[0]; original_typed = typed; original_start = *info.cluster_tx_start;
	{
		ClusterColdTtSlotInputV1 *aliases[] = {&native_ctrc_work.entries[0],
			&census.entries[0].input, (ClusterColdTtSlotInputV1 *)&physical,
			(ClusterColdTtSlotInputV1 *)&typed, (ClusterColdTtSlotInputV1 *)info.cluster_tx_start,
			(ClusterColdTtSlotInputV1 *)((char *)info.cluster_tx_start + sizeof(void *)),
			(ClusterColdTtSlotInputV1 *)&window_region.checkpoint_owner.tx_result};
		for (unsigned i = 0; i < lengthof(aliases); i++)
			UT_ASSERT(!cluster_recovery_ctrc_terminal_subject_v1(info.cluster_tx_start,
				&native_ctrc_work, 0, &physical, aliases[i]));
	}
	for (Size offset = sizeof(void *); offset < sizeof(original_start); offset += sizeof(void *))
		UT_ASSERT(!cluster_recovery_ctrc_terminal_subject_v1(info.cluster_tx_start,
			&native_ctrc_work, 0, &physical,
			(ClusterColdTtSlotInputV1 *)((char *)info.cluster_tx_start + offset)));
	UT_ASSERT(memcmp(&original, &native_ctrc_work.entries[0], sizeof(original)) == 0);
	UT_ASSERT(memcmp(&original_physical, &physical, sizeof(physical)) == 0);
	UT_ASSERT(memcmp(&original_top, &census.entries[0], sizeof(original_top)) == 0);
	UT_ASSERT(memcmp(&original_typed, &typed, sizeof(typed)) == 0);
	UT_ASSERT(memcmp(&original_start, info.cluster_tx_start, sizeof(original_start)) == 0);
	memset(&out, 0xa5, sizeof(out)); unchanged = out;
	UT_ASSERT(!cluster_recovery_ctrc_terminal_subject_v1(info.cluster_tx_start,
		&native_ctrc_work, 1, &physical, &out));
	invalidate_writer_on_xid_ready = true;
	UT_ASSERT(!cluster_recovery_ctrc_terminal_subject_v1(info.cluster_tx_start,
		&native_ctrc_work, 0, &physical, &out));
	UT_ASSERT(memcmp(&out, &unchanged, sizeof(out)) == 0);
}
