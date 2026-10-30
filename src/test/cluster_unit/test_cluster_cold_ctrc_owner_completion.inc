/* Real Start/PAGE owner -> original canonical completion bytes.
 * PAGE transport is controlled; no graph/qualification/release is inferred.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
/* Existing pure codec now runs in this target; these allocation leaves use
 * the unchanged fixture allocator and retain its failure accounting. */
void *palloc(Size bytes)
{
    return MemoryContextAllocExtended(TopMemoryContext, bytes,
        MCXT_ALLOC_ZERO | MCXT_ALLOC_NO_OOM);
}
void *palloc_extended(Size bytes, int flags)
{
    (void)flags;
    return palloc(bytes);
}

static uint8 owner_completion_evidence[BLCKSZ + MAXPGPATH + 96];
static Size owner_completion_length, owner_completion_path_length;
static uint8 owner_completion_bytes[MAXPGPATH + 96];

static void
owner_completion_ready(bool unchanged)
{
	RfPageCtrcOwnerProofV1 proof;
	const ClusterRecoveryCtrcReplaySetSourceV1 *source;
	uint8 *atom = owner_completion_evidence + 16, *id = atom + 10;
	char *path;
	Size id_length;

	startup_image_ready(unchanged);
	proof = ClusterColdRecoveryCtrcPageOwnerProof(info.cluster_tx_start, 0);
	UT_ASSERT_EQ(proof.detail, RF_PAGE_PROOF_DETAIL_OK);
	source = &info.cluster_tx_start->ctrc_page_sources[prefix_source_ordinal];
	path = relpathperm(proof.space.key.locator, MAIN_FORKNUM);
	owner_completion_path_length = strlen(path);
	id_length = 46 + owner_completion_path_length;
	memset(owner_completion_evidence, 0, sizeof(owner_completion_evidence));
	cluster_undo_tt_ctrc_put_u64_le(owner_completion_evidence, 1);
	cluster_undo_tt_ctrc_put_u64_le(owner_completion_evidence + 8, 34 + id_length + BLCKSZ);
	cluster_undo_tt_ctrc_put_u16_le(atom, 2);
	cluster_undo_tt_ctrc_put_u64_le(atom + 2, id_length + 16);
	cluster_undo_tt_ctrc_put_u64_le(id, proof.space.key.system_identifier);
	cluster_undo_tt_ctrc_put_u64_le(id + 8, proof.space.key.database_incarnation);
	cluster_undo_tt_ctrc_put_u16_le(id + 16, source->generation.source.claim.identity.origin_node_id);
	cluster_undo_tt_ctrc_put_u32_le(id + 18, CLUSTER_RECOVERY_FILE_RELATION);
	cluster_undo_tt_ctrc_put_u64_le(id + 22, owner_completion_path_length);
	memcpy(id + 30, path, owner_completion_path_length);
	pfree(path);
	/* Native relation has no U64 file generation. Zero is not an era grant. */
	cluster_undo_tt_ctrc_put_u64_le(id + 38 + owner_completion_path_length, proof.blockno);
	cluster_undo_tt_ctrc_put_u64_le(id + id_length, (uint64)proof.blockno * BLCKSZ);
	cluster_undo_tt_ctrc_put_u64_le(id + id_length + 8, BLCKSZ);
	cluster_undo_tt_ctrc_put_u64_le(id + id_length + 16, BLCKSZ);
	memcpy(id + id_length + 24, proof.image.data, BLCKSZ);
	owner_completion_length = 50 + id_length + BLCKSZ;
	memset(owner_completion_bytes, 99, sizeof(owner_completion_bytes));
	startup_image_calls = 0;
}

static RfPageCtrcOwnerProofV1
owner_completion_run(Size budget, Size *length)
{
	RfPageCtrcOwnerCompletionV1 value = ClusterColdRecoveryCtrcPageOwnerCompletion(
        info.cluster_tx_start, 0, owner_completion_evidence, owner_completion_length, budget);
    for (Size i = value.length; i < sizeof(value.completion); i++)
        UT_ASSERT_EQ(value.completion[i], 0);
    *length = value.length;
    memset(owner_completion_bytes, 0, sizeof(owner_completion_bytes));
    if (value.length != 0) memcpy(owner_completion_bytes, value.completion, value.length);
    return value.owner;
}

UT_TEST(test_startup_owner_completion_uses_actual_rule_and_complete_native_witness)
{
	for (unsigned unchanged = 0; unchanged < 2; unchanged++) {
		RfPageCtrcOwnerProofV1 proof;
		uint8 ids[MAXPGPATH + 96]; Size length = 0, ids_length = 0;
		owner_completion_ready(unchanged != 0);
		proof = owner_completion_run(128 * 1024, &length);
		UT_ASSERT_EQ(proof.detail, RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(proof.owner_family, 10); UT_ASSERT_EQ(proof.rule, unchanged ? 6 : 1);
		UT_ASSERT(cluster_recovery_ctrc_witness_ids_from_evidence_v2(owner_completion_evidence,
			owner_completion_length, ids, sizeof(ids), &ids_length));
		UT_ASSERT_EQ(length, 4 + ids_length);
		UT_ASSERT_EQ(cluster_undo_tt_ctrc_get_u16_le(owner_completion_bytes), 1);
		UT_ASSERT_EQ(cluster_undo_tt_ctrc_get_u16_le(owner_completion_bytes + 2), proof.rule);
		UT_ASSERT(memcmp(owner_completion_bytes + 4, ids, ids_length) == 0);
		UT_ASSERT_EQ(startup_image_calls, 1); UT_ASSERT_EQ(prefix_steps, 2);
		UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
		retirement_local_cleanup();
	}
}

UT_TEST(test_startup_owner_completion_rejects_incomplete_or_wrong_object_witness)
{
	for (unsigned fault = 0; fault < 12; fault++) {
		RfPageCtrcOwnerProofV1 proof; Size length = 99;
		uint8 *id;
		owner_completion_ready(false); id = owner_completion_evidence + 26;
		switch (fault) {
			case 0: owner_completion_evidence[0] = 0; break;
			case 1: owner_completion_evidence[16] = 1; break;
			case 2: id[0] ^= 1; break;
			case 3: id[8] ^= 1; break;
			case 4: id[16] ^= 1; break;
			case 5: id[18] ^= 1; break;
			case 6: id[30] ^= 1; break;
			case 7: id[30 + owner_completion_path_length] = 1; break;
			case 8: id[38 + owner_completion_path_length] ^= 1; break;
			case 9: id[46 + owner_completion_path_length] ^= 1; break;
			case 10: owner_completion_evidence[owner_completion_length - 1] ^= 1; break;
			case 11: owner_completion_length--; break;
		}
		proof = owner_completion_run(128 * 1024, &length);
		UT_ASSERT(proof.detail != RF_PAGE_PROOF_DETAIL_OK && startup_owner_empty(proof));
		UT_ASSERT_EQ(length, 0); UT_ASSERT_EQ(owner_completion_bytes[0], 0);
		UT_ASSERT(pending_storage_mask == 3 && observation_scope_live);
		retirement_local_cleanup();
	}
}

UT_TEST(test_startup_owner_completion_wait_and_late_drift_never_encode_success)
{
	for (unsigned drift = 0; drift < 2; drift++) {
		RfPageCtrcOwnerProofV1 proof; Size length = 99;
		owner_completion_ready(false);
		if (drift) prefix_destroy_callback = startup_image_late_drift;
		else { completed_readback_mode = 4; completed_readback_calls = 0; }
		proof = owner_completion_run(128 * 1024, &length);
		prefix_destroy_callback = NULL;
		UT_ASSERT(proof.detail != RF_PAGE_PROOF_DETAIL_OK && startup_owner_empty(proof));
		UT_ASSERT_EQ(length, 0); UT_ASSERT_EQ(owner_completion_bytes[0], 0);
		UT_ASSERT(pending_storage_mask == 3 && observation_scope_live);
		retirement_local_cleanup();
	}
}

UT_TEST(test_startup_owner_completion_error_keeps_native_duty_and_private_cleanup)
{
	volatile bool caught = false; Size length = 99;
	unsigned allocated, freed;
	owner_completion_ready(false); completed_readback_mode = 3;
	allocated = allocations; freed = frees;
	PG_TRY(); { (void)owner_completion_run(128 * 1024, &length); }
	PG_CATCH(); { caught = true; } PG_END_TRY();
	UT_ASSERT(caught && cluster_recovery_cold_start->revoked);
	/* No returned value was published through the longjmp. */
	UT_ASSERT_EQ(length, 99); UT_ASSERT_EQ(allocations - allocated, frees - freed);
	UT_ASSERT(pending_storage_mask == 3 && observation_scope_live);
	UT_ASSERT_EQ(pending_storage_closes, 0); retirement_local_cleanup();
}

UT_TEST(test_startup_owner_completion_budget_and_bad_handle_refuse_before_native_read)
{
	RfPageCtrcOwnerProofV1 proof; Size length = 99;
	owner_completion_ready(false);
	proof = owner_completion_run(1, &length);
	UT_ASSERT(proof.detail != RF_PAGE_PROOF_DETAIL_OK && startup_owner_empty(proof));
	UT_ASSERT_EQ(length, 0); UT_ASSERT_EQ(startup_image_calls, 0);
	{
        RfPageCtrcOwnerCompletionV1 value = ClusterColdRecoveryCtrcPageOwnerCompletion(
            NULL, 0, owner_completion_evidence, owner_completion_length, 128 * 1024);
        UT_ASSERT(value.owner.detail != RF_PAGE_PROOF_DETAIL_OK && startup_owner_empty(value.owner));
        UT_ASSERT_EQ(value.length, 0);
    }
    UT_ASSERT_EQ(startup_image_calls, 0); UT_ASSERT(!info.cluster_tx_start->revoked);
	allocation_ok = false;
	proof = owner_completion_run(128 * 1024, &length);
	allocation_ok = true;
	UT_ASSERT_EQ(proof.detail, RF_PAGE_PROOF_DETAIL_OOM);
	UT_ASSERT_EQ(length, 0); UT_ASSERT_EQ(startup_image_calls, 0);
	UT_ASSERT(!info.cluster_tx_start->revoked && pending_storage_mask == 3);
	retirement_local_cleanup();
}

static RfPageProofDetailV1
owner_completion_change_witness(uint32 index, RfPageProofDetailV1 detail)
{
	(void)index;
	owner_completion_evidence[owner_completion_length - 1] ^= 1;
	return detail;
}

UT_TEST(test_startup_owner_completion_changed_input_during_read_cannot_replace_snapshot)
{
	RfPageCtrcOwnerProofV1 proof; Size length = 99;
	owner_completion_ready(false); startup_image_observer = owner_completion_change_witness;
	proof = owner_completion_run(128 * 1024, &length);
	startup_image_observer = NULL;
	UT_ASSERT(proof.detail != RF_PAGE_PROOF_DETAIL_OK && startup_owner_empty(proof));
	UT_ASSERT_EQ(length, 0); UT_ASSERT_EQ(owner_completion_bytes[0], 0);
	UT_ASSERT(info.cluster_tx_start->revoked && pending_storage_mask == 3);
	retirement_local_cleanup();
}
