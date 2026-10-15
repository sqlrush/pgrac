/*-------------------------------------------------------------------------
 * test_cluster_pcm_object_retire.c
 *    Original directory owners consume terminal DROP identity.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Real PCM producers/scans/retire tail; KO terminal proof, HTAB and locks
 * are explicit fixture boundaries. No DATA/ACK or SQL completion claim.
 *-------------------------------------------------------------------------
 */
int pcm_fixture_main(void);
#define main pcm_fixture_main
#include "test_cluster_pcm_lock.c"
#undef main

static struct StopPcmEntryLayout *
directory_entry(BufferTag tag)
{
	return hash_search((HTAB *)&fake_pcm_htab_token, &tag, HASH_FIND, NULL);
}

static BufferTag
directory_fixture(void)
{
	BufferTag tag = make_tag(7300);
	reset_fake_pcm_runtime(8);
	cluster_shared_config = true;
	cluster_node_id = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(cluster_gcs_block_master_rebuild_from_redeclare(tag, PCM_STATE_N, 80, 90, 0, 7));
	object_drop_locator = BufTagGetRelFileLocator(&tag);
	object_drop_retired = true;
	return tag;
}

static unsigned
scan_directory(void)
{
	BufferTag tags[8];
	uint32 cursor = 0;
	return cluster_pcm_lock_pi_candidates_v1(&cursor, 128, tags, lengthof(tags));
}

UT_TEST(test_terminal_drop_ends_directory_pi_without_data_or_ack)
{
	BufferTag tag = directory_fixture(), live = tag, tags[8];
	ClusterPcmPiStorageCutV1 old;
	uint32 cursor = 0;
	struct StopPcmEntryLayout *entry = directory_entry(tag);
	PcmRetireRefusal why;
	uint64 generation = entry->binding_generation;

	UT_ASSERT(cluster_pcm_lock_pi_storage_snapshot_v1(tag, &old));
	UT_ASSERT(!pcm_entry_try_retire_exact(&tag, generation, PCM_RETIRE_REASON_PI_DISCARDED));
	UT_ASSERT(!pcm_entry_retire_classify_exact(&tag, generation, &why));
	UT_ASSERT_EQ(why, PCM_RETIRE_REFUSAL_PI_PRESENT);
	live.relNumber++;
	UT_ASSERT(cluster_gcs_block_master_rebuild_from_redeclare(live, PCM_STATE_N, 80, 90, 0, 7));
	UT_ASSERT_EQ(cluster_pcm_lock_pi_candidates_v1(&cursor, 128, tags, lengthof(tags)), 1);
	UT_ASSERT(BufferTagsEqual(&tags[0], &live));
	UT_ASSERT(directory_entry(tag) == NULL);
	UT_ASSERT(directory_entry(live) != NULL);
	UT_ASSERT(!cluster_pcm_lock_pi_storage_matches_v1(&old));
	UT_ASSERT_EQ(fake_lwlock_depth, 0);
}

UT_TEST(test_original_bounded_reclaim_retires_terminal_watermark_and_pi)
{
	BufferTag tag = directory_fixture(), watermark = tag;
	PcmReclaimBatch result;
	watermark.blockNum++;
	UT_ASSERT(cluster_gcs_block_master_rebuild_from_redeclare(watermark, PCM_STATE_N, 80, 90, 0, 7));
	/* A directory watermark may outlive its bitmap; it is not local PI. */
	pg_atomic_write_u32(&directory_entry(watermark)->pi_holders_bitmap, 0);
	UT_ASSERT(cluster_pcm_lock_reclaim_bounded(8, &result));
	UT_ASSERT_EQ(result.retired_count, 2);
	UT_ASSERT(directory_entry(tag) == NULL && directory_entry(watermark) == NULL);
	UT_ASSERT_EQ(fake_lwlock_depth, 0);
}

UT_TEST(test_unproven_or_wrong_object_keeps_original_debt)
{
	BufferTag tag = directory_fixture();
	struct StopPcmEntryLayout before = *directory_entry(tag);
	PcmReclaimBatch result;
	object_drop_retired = false;
	UT_ASSERT_EQ(scan_directory(), 1);
	UT_ASSERT(cluster_pcm_lock_reclaim_bounded(8, &result));
	UT_ASSERT_EQ(result.retired_count, 0);
	UT_ASSERT_EQ(memcmp(directory_entry(tag), &before, sizeof(before)), 0);
	object_drop_retired = true;
	object_drop_locator.relNumber++;
	UT_ASSERT_EQ(scan_directory(), 1);
	UT_ASSERT_EQ(memcmp(directory_entry(tag), &before, sizeof(before)), 0);
	object_drop_locator.relNumber--;
	UT_ASSERT_EQ(scan_directory(), 0);
	UT_ASSERT(directory_entry(tag) == NULL);
}

UT_TEST(test_terminal_identity_never_clears_local_or_inflight_owners)
{
	for (unsigned fault = 0; fault < 13; fault++) {
		BufferTag tag = directory_fixture();
		struct StopPcmEntryLayout *entry = directory_entry(tag), before;
		PcmReclaimBatch result;
		switch (fault) {
		case 0: pg_atomic_write_u32(&entry->pin_count, 1); break;
		case 1: pg_atomic_write_u32(&entry->wait_refcount, 1); break;
		case 2: pg_atomic_write_u32(&entry->transport_refcount, 1); break;
		case 3: entry->local_pi_first.source_flags = 1; break;
		case 4: entry->local_pi_last.source_flags = 1; break;
		case 5: entry->s_holder_refcount_local = 1; break;
		case 6: pg_atomic_write_u32(&entry->master_state, PCM_STATE_S); break;
		case 7: pg_atomic_write_u32(&entry->master_state, PCM_STATE_X); break;
		case 8: entry->pending_x_requester_node = 2; break;
		case 9: entry->pending_x_since_lsn = 1; break;
		case 10: entry->resource_x_formation = 18; break;
		case 11: pg_atomic_write_u32(&entry->s_holders_bitmap, 2); break;
		case 12: entry->x_holder_node = 2; break;
		}
		before = *entry;
		(void)scan_directory();
		UT_ASSERT(cluster_pcm_lock_reclaim_bounded(8, &result));
		UT_ASSERT_EQ(result.retired_count, 0);
		UT_ASSERT(directory_entry(tag) != NULL);
		UT_ASSERT_EQ(memcmp(directory_entry(tag), &before, sizeof(before)), 0);
		UT_ASSERT_EQ(fake_lwlock_depth, 0);
	}
}

UT_TEST(test_canonical_wait_proof_is_not_a_pure_retired_entry)
{
	BufferTag tag = directory_fixture();
	ResourceXDecodedFrame assertion = make_resource_x_master_frame(RESOURCE_X_WIRE_ASSERT_X, tag, 2, 2);
	ResourceXMasterSnapshot snapshot;
	PcmReclaimBatch result;
	struct StopPcmEntryLayout before;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_exact(&assertion, 2, &snapshot),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(snapshot.phase, RESOURCE_X_MASTER_WAIT_PROOF);
	before = *directory_entry(tag);
	UT_ASSERT_EQ(scan_directory(), 1);
	UT_ASSERT(cluster_pcm_lock_reclaim_bounded(8, &result));
	UT_ASSERT_EQ(result.retired_count, 0);
	UT_ASSERT_EQ(memcmp(directory_entry(tag), &before, sizeof(before)), 0);
}

static BufferTag changed_tag;
static void
change_entry_generation(void)
{
	directory_entry(changed_tag)->binding_generation++;
}

UT_TEST(test_rebind_gate_and_lock_contention_do_not_erase_entry)
{
	BufferTag tag = directory_fixture();
	struct StopPcmEntryLayout *entry = directory_entry(tag);
	uint64 generation = entry->binding_generation;
	changed_tag = tag;
	object_entry_lock_hook = change_entry_generation;
	(void)scan_directory();
	UT_ASSERT(directory_entry(tag) != NULL);
	entry->binding_generation = generation;
	fake_lwlock_conditional_fail_once = true;
	(void)scan_directory();
	UT_ASSERT(directory_entry(tag) != NULL);
	UT_ASSERT(!fake_lwlock_conditional_fail_once);
	UT_ASSERT_EQ(scan_directory(), 0);
	UT_ASSERT(directory_entry(tag) == NULL);
	UT_ASSERT_EQ(fake_lwlock_depth, 0);
}

UT_TEST(test_terminal_identity_closes_legal_node31_directory_debt)
{
	BufferTag tag = directory_fixture();
	/* All 32 bits are protocol node IDs; bit31 is not an invalid bitmap. */
	pg_atomic_write_u32(&directory_entry(tag)->pi_holders_bitmap, UINT32_C(1) << 31);
	UT_ASSERT_EQ(scan_directory(), 0);
	UT_ASSERT(directory_entry(tag) == NULL);
}

int
main(void)
{
	UT_PLAN(7);
	UT_RUN(test_terminal_drop_ends_directory_pi_without_data_or_ack);
	UT_RUN(test_original_bounded_reclaim_retires_terminal_watermark_and_pi);
	UT_RUN(test_unproven_or_wrong_object_keeps_original_debt);
	UT_RUN(test_terminal_identity_never_clears_local_or_inflight_owners);
	UT_RUN(test_canonical_wait_proof_is_not_a_pure_retired_entry);
	UT_RUN(test_rebind_gate_and_lock_contention_do_not_erase_entry);
	UT_RUN(test_terminal_identity_closes_legal_node31_directory_debt);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
