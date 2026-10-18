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

static ClusterPageWalBindingV1
drop_local_fixture(BufferTag tag)
{
	ClusterPageWalBindingV1 binding = local_pi_setup(tag);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(cluster_pcm_local_pi_record_v1(tag, &binding));
	object_drop_locator = BufTagGetRelFileLocator(&tag);
	object_drop_retired = true;
	return binding;
}

UT_TEST(test_committed_drop_moves_only_its_two_local_references)
{
	BufferTag tag = make_tag(7400);
	ClusterPageWalBindingV1 binding = drop_local_fixture(tag), observed, other = binding;
	ClusterPageWalRefV1 retained = { 0 }, first_new = { 0 }, second_new = { 0 };
	uint16 original = directory_entry(tag)->local_pi_first.source_flags & UINT16_C(0x7fff);
	uint64 generation = directory_entry(tag)->binding_generation;
	PcmRetireRefusal why;

	UT_ASSERT_EQ(original, 1); /* Fresh real source pool in this fixture. */
	UT_ASSERT(cluster_page_wal_ref_retain_v1(&binding, &retained));
	UT_ASSERT(!pcm_entry_retire_classify_exact(&tag, generation, &why));
	UT_ASSERT_EQ(why, PCM_RETIRE_REFUSAL_PI_PRESENT);
	UT_ASSERT(!pcm_entry_try_retire_exact(&tag, generation, PCM_RETIRE_REASON_PI_DISCARDED));
	UT_ASSERT(pcm_entry_try_retire_exact(&tag, generation, PCM_RETIRE_REASON_OBJECT_RETIRED));
	UT_ASSERT(directory_entry(tag) == NULL);
	UT_ASSERT_EQ(fake_lwlock_depth, 0);
	UT_ASSERT(cluster_page_wal_ref_read_v1(&retained, binding.identity.locator,
		binding.identity.forknum, binding.identity.blockno, &observed));
	UT_ASSERT_EQ(memcmp(&observed.source, &binding.source, sizeof(binding.source)), 0);
	/* The third owner still pins the original source. After it releases,
	 * actual source-slot reuse proves that neither local reference leaked. */
	other.source.claim.identity.origin_owner_incarnation++;
	UT_ASSERT(cluster_page_wal_ref_retain_v1(&other, &first_new));
	UT_ASSERT((first_new.source_flags & UINT16_C(0x7fff)) != original);
	UT_ASSERT(cluster_page_wal_ref_release_v1(&retained));
	other.source.claim.identity.origin_owner_incarnation++;
	UT_ASSERT(cluster_page_wal_ref_retain_v1(&other, &second_new));
	UT_ASSERT_EQ(second_new.source_flags & UINT16_C(0x7fff), original);
	UT_ASSERT(!pcm_entry_try_retire_exact(&tag, generation, PCM_RETIRE_REASON_OBJECT_RETIRED));
	UT_ASSERT(cluster_page_wal_ref_release_v1(&first_new));
	UT_ASSERT(cluster_page_wal_ref_release_v1(&second_new));
}

UT_TEST(test_original_cp_and_lmon_owners_end_dead_object_local_pi)
{
	for (unsigned owner = 0; owner < 2; owner++) {
		BufferTag tag = make_tag(7410 + owner);
		ClusterPageWalBindingV1 binding = drop_local_fixture(tag);
		ClusterPageWalRefV1 first_new = { 0 }, last_new = { 0 };
		PcmReclaimBatch batch;
		uint16 first = directory_entry(tag)->local_pi_first.source_flags & UINT16_C(0x7fff);
		uint16 last;

		binding.source.claim.identity.origin_node_id = 1;
		binding.source.claim.identity.origin_thread_id = 2;
		binding.source.claim.identity.origin_owner_incarnation++;
		binding.record_start = 300;
		binding.record_end = 400;
		binding.version.mutation_token++;
		UT_ASSERT(cluster_pcm_local_pi_record_v1(tag, &binding));
		last = directory_entry(tag)->local_pi_last.source_flags & UINT16_C(0x7fff);
		UT_ASSERT(first != last);
		if (owner == 0)
			UT_ASSERT_EQ(scan_directory(), 0);
		else {
			UT_ASSERT(cluster_pcm_lock_reclaim_bounded(4, &batch));
			UT_ASSERT_EQ(batch.retired_count, 1);
		}
		UT_ASSERT(directory_entry(tag) == NULL);
		UT_ASSERT_EQ(fake_lwlock_depth, 0);
		binding.source.claim.identity.origin_owner_incarnation++;
		UT_ASSERT(cluster_page_wal_ref_retain_v1(&binding, &first_new));
		UT_ASSERT_EQ(first_new.source_flags & UINT16_C(0x7fff), first);
		binding.source.claim.identity.origin_owner_incarnation++;
		UT_ASSERT(cluster_page_wal_ref_retain_v1(&binding, &last_new));
		UT_ASSERT_EQ(last_new.source_flags & UINT16_C(0x7fff), last);
		UT_ASSERT(cluster_page_wal_ref_release_v1(&first_new));
		UT_ASSERT(cluster_page_wal_ref_release_v1(&last_new));
	}
}

UT_TEST(test_dead_local_pi_refuses_unknown_identity_or_malformed_reference)
{
	for (unsigned fault = 0; fault < 12; fault++) {
		BufferTag tag = make_tag(7420 + fault);
		struct StopPcmEntryLayout *entry, before;
		(void)drop_local_fixture(tag);
		entry = directory_entry(tag);
		switch (fault) {
		case 0: object_drop_retired = false; break;
		case 1: object_drop_locator.relNumber++; break;
		case 2: entry->local_pi_first.source_flags = 0; break;
		case 3: entry->local_pi_last.source_flags = 0; break;
		case 4: entry->local_pi_first.source_flags = UINT16_MAX; break;
		case 5: entry->local_pi_last.end = entry->local_pi_last.start; break;
		case 6: entry->local_pi_first.token = 0; break;
		case 7: entry->local_pi_last.source_flags &= UINT16_C(0x7fff); break;
		case 8: entry->local_pi_revision = 0; break;
		case 9: entry->local_pi_revision = UINT64_MAX; break;
		case 10: entry->local_pi_revision = UINT64_MAX - 1; break;
		case 11: memset(entry->local_pi_first.incarnation, 0, 16); break;
		}
		before = *entry;
		UT_ASSERT(!pcm_entry_try_retire_exact(&tag, entry->binding_generation,
			PCM_RETIRE_REASON_OBJECT_RETIRED));
		UT_ASSERT(directory_entry(tag) != NULL);
		UT_ASSERT_EQ(memcmp(directory_entry(tag), &before, sizeof(before)), 0);
		UT_ASSERT_EQ(fake_lwlock_depth, 0);
	}
}

UT_TEST(test_dead_local_pi_keeps_inflight_current_and_reference_gates)
{
	for (unsigned fault = 0; fault < 10; fault++) {
		BufferTag tag = make_tag(7440 + fault);
		ClusterPageWalBindingV1 binding = drop_local_fixture(tag), observed;
		struct StopPcmEntryLayout *entry = directory_entry(tag), before;
		switch (fault) {
		case 0: pg_atomic_write_u32(&entry->pin_count, 1); break;
		case 1: pg_atomic_write_u32(&entry->wait_refcount, 1); break;
		case 2: pg_atomic_write_u32(&entry->transport_refcount, 1); break;
		case 3: entry->s_holder_refcount_local = 1; break;
		case 4: pg_atomic_write_u32(&entry->master_state, PCM_STATE_X); break;
		case 5: entry->pending_x_requester_node = 2; break;
		case 6: entry->pending_x_since_lsn = 1; break;
		case 7: entry->resource_x_formation = 18; break;
		case 8: pg_atomic_write_u32(&entry->s_holders_bitmap, 2); break;
		case 9: fake_lwlock_conditional_fail_once = true; break;
		}
		before = *entry;
		UT_ASSERT(!pcm_entry_try_retire_exact(&tag, entry->binding_generation,
			PCM_RETIRE_REASON_OBJECT_RETIRED));
		UT_ASSERT_EQ(memcmp(directory_entry(tag), &before, sizeof(before)), 0);
		UT_ASSERT(cluster_page_wal_ref_read_v1(&entry->local_pi_first,
			binding.identity.locator, tag.forkNum, tag.blockNum, &observed));
		UT_ASSERT_EQ(memcmp(&observed, &binding, sizeof(binding)), 0);
		UT_ASSERT_EQ(fake_lwlock_depth, 0);
	}
}

UT_TEST(test_origin_discard_scope_does_not_retire_master_directory)
{
	BufferTag tag = make_tag(7499);
	struct StopPcmEntryLayout *entry, before, expected;
	uint32 cursor = 0;

	cluster_node_id = 0;
	(void)drop_local_fixture(tag);
	object_drop_retired = false;
	object_origin_discard = true; /* Exact KO owner tested by its production consumer. */
	entry = directory_entry(tag);
	pg_atomic_write_u32(&entry->pi_holders_bitmap, 5);
	entry->pi_watermark_lsn = 200;
	entry->pi_watermark_scn = 90;
	before = *entry;
	UT_ASSERT(!pcm_entry_try_retire_exact(&tag, entry->binding_generation,
		PCM_RETIRE_REASON_OBJECT_RETIRED));
	UT_ASSERT_EQ(memcmp(entry, &before, sizeof(before)), 0);
	UT_ASSERT_EQ(cluster_pcm_lock_ko_local_relation_discard_v4(object_drop_locator, 17, 128,
		&cursor), CLUSTER_PCM_PI_RELATION_EMPTY);
	expected = before;
	memset(&expected.local_pi_first, 0, sizeof(expected.local_pi_first));
	memset(&expected.local_pi_last, 0, sizeof(expected.local_pi_last));
	expected.local_pi_revision++;
	UT_ASSERT_EQ(memcmp(entry, &expected, sizeof(expected)), 0);
	UT_ASSERT(!pcm_entry_try_retire_exact(&tag, entry->binding_generation,
		PCM_RETIRE_REASON_OBJECT_RETIRED));
	UT_ASSERT_EQ(memcmp(entry, &expected, sizeof(expected)), 0);
	UT_ASSERT_EQ(fake_lwlock_depth, 0);
}

/* Mutation under the entry lock must move only the two exact local refs;
 * the master entry and independent directory requests survive. */
UT_TEST(test_local_drop_discard_preserves_remote_master_obligation)
{
	BufferTag tag = make_tag(7500);
	ResourceXDecodedFrame assertion;
	ResourceXMasterSnapshot before_master, after_master;
	struct StopPcmEntryLayout before, expected;
	uint32 cursor = 0;

	cluster_node_id = 0;
	(void)drop_local_fixture(tag);
	pg_atomic_write_u32(&directory_entry(tag)->pi_holders_bitmap, 5);
	directory_entry(tag)->pi_watermark_lsn = 200;
	directory_entry(tag)->pi_watermark_scn = 90;
	assertion = make_resource_x_master_frame(RESOURCE_X_WIRE_ASSERT_X, tag, 2, 2);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_exact(&assertion, 2, &before_master),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(before_master.phase, RESOURCE_X_MASTER_WAIT_PROOF);
	before = *directory_entry(tag);
	UT_ASSERT_EQ(cluster_pcm_lock_ko_local_relation_scan_v3(object_drop_locator, 17, 128, &cursor),
		CLUSTER_PCM_PI_RELATION_PENDING);
	UT_ASSERT_EQ(memcmp(directory_entry(tag), &before, sizeof(before)), 0);
	cursor = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_ko_local_relation_discard_v4(object_drop_locator, 17, 128, &cursor),
		CLUSTER_PCM_PI_RELATION_EMPTY);
	expected = before;
	memset(&expected.local_pi_first, 0, sizeof(expected.local_pi_first));
	memset(&expected.local_pi_last, 0, sizeof(expected.local_pi_last));
	expected.local_pi_revision++;
	UT_ASSERT_NOT_NULL(directory_entry(tag));
	UT_ASSERT_EQ(memcmp(directory_entry(tag), &expected, sizeof(expected)), 0);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_master_snapshot_exact(
		&assertion.common.logical_assertion, &after_master), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(memcmp(&before_master, &after_master, sizeof(before_master)), 0);
	cursor = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_ko_directory_relation_scan_v3(object_drop_locator, 17, 128, &cursor),
		CLUSTER_PCM_PI_RELATION_PENDING);
	UT_ASSERT_EQ(fake_lwlock_depth, 0);
}

UT_TEST(test_local_drop_discard_releases_exact_sources_once)
{
	BufferTag tag = make_tag(7501);
	ClusterPageWalBindingV1 binding, other, observed;
	ClusterPageWalRefV1 retained = { 0 }, next = { 0 }, reused = { 0 };
	uint16 source_slot;
	uint64 revision;
	uint32 cursor = 0;

	cluster_node_id = 0;
	binding = drop_local_fixture(tag);
	UT_ASSERT(cluster_page_wal_ref_retain_v1(&binding, &retained));
	source_slot = retained.source_flags & UINT16_C(0x7fff);
	revision = directory_entry(tag)->local_pi_revision;
	UT_ASSERT_EQ(cluster_pcm_lock_ko_local_relation_discard_v4(object_drop_locator, 17, 128, &cursor),
		CLUSTER_PCM_PI_RELATION_EMPTY);
	UT_ASSERT_EQ(directory_entry(tag)->local_pi_revision, revision + 1);
	UT_ASSERT(cluster_page_wal_ref_read_v1(&retained, binding.identity.locator,
		tag.forkNum, tag.blockNum, &observed));
	UT_ASSERT_EQ(memcmp(&observed, &binding, sizeof(binding)), 0);
	other = binding;
	other.source.claim.identity.origin_owner_incarnation++;
	UT_ASSERT(cluster_page_wal_ref_retain_v1(&other, &next));
	UT_ASSERT((next.source_flags & UINT16_C(0x7fff)) != source_slot);
	UT_ASSERT(cluster_page_wal_ref_release_v1(&retained));
	other.source.claim.identity.origin_owner_incarnation++;
	UT_ASSERT(cluster_page_wal_ref_retain_v1(&other, &reused));
	UT_ASSERT_EQ(reused.source_flags & UINT16_C(0x7fff), source_slot);
	cursor = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_ko_local_relation_discard_v4(object_drop_locator, 17, 128, &cursor),
		CLUSTER_PCM_PI_RELATION_EMPTY);
	UT_ASSERT_EQ(directory_entry(tag)->local_pi_revision, revision + 1);
	UT_ASSERT(cluster_page_wal_ref_release_v1(&next));
	UT_ASSERT(cluster_page_wal_ref_release_v1(&reused));
}

UT_TEST(test_local_drop_discard_rejects_unknown_identity_refs_and_cut)
{
	for (unsigned fault = 0; fault < 16; fault++) {
		BufferTag tag = make_tag(7510 + fault);
		struct StopPcmEntryLayout *entry, before;
		RelFileLocator locator;
		uint64 formation = 17;
		uint32 cursor = 0, budget = 128;

		cluster_node_id = 0;
		(void)drop_local_fixture(tag);
		entry = directory_entry(tag);
		locator = object_drop_locator;
		switch (fault) {
		case 0: object_drop_retired = false; break;
		case 1: locator.relNumber++; break;
		case 2: entry->local_pi_first.source_flags = 0; break;
		case 3: entry->local_pi_last.source_flags = 0; break;
		case 4: entry->local_pi_first.source_flags = UINT16_MAX; break;
		case 5: entry->local_pi_last.end = entry->local_pi_last.start; break;
		case 6: entry->local_pi_first.token = 0; break;
		case 7: entry->local_pi_last.source_flags &= UINT16_C(0x7fff); break;
		case 8: entry->local_pi_revision = 0; break;
		case 9: entry->local_pi_revision = UINT64_MAX; break;
		case 10: entry->local_pi_revision = UINT64_MAX - 1; break;
		case 11: memset(entry->local_pi_first.incarnation, 0, 16); break;
		case 12: formation++; break;
		case 13: budget = 0; break;
		case 14: budget = 129; break;
		case 15: entry->local_pi_first.source_flags = entry->local_pi_last.source_flags = 0; break;
		}
		before = *entry;
		UT_ASSERT_EQ(cluster_pcm_lock_ko_local_relation_discard_v4(locator, formation, budget, &cursor),
			CLUSTER_PCM_PI_RELATION_INVALID);
		UT_ASSERT_EQ(memcmp(entry, &before, sizeof(before)), 0);
		UT_ASSERT_EQ(fake_lwlock_depth, 0);
	}
}

UT_TEST(test_local_drop_discard_keeps_current_and_inflight_local_owners)
{
	for (unsigned fault = 0; fault < 8; fault++) {
		BufferTag tag = make_tag(7540 + fault);
		struct StopPcmEntryLayout *entry, before;
		ResourceXDecodedFrame assertion;
		ResourceXMasterSnapshot snapshot;
		uint32 cursor = 0;

		cluster_node_id = 0;
		(void)drop_local_fixture(tag);
		entry = directory_entry(tag);
		switch (fault) {
		case 0: pg_atomic_write_u32(&entry->pin_count, 1); break;
		case 1: pg_atomic_write_u32(&entry->wait_refcount, 1); break;
		case 2: pg_atomic_write_u32(&entry->transport_refcount, 1); break;
		case 3: entry->s_holder_refcount_local = 1; break;
		case 4: pg_atomic_write_u32(&entry->master_state, PCM_STATE_X); break;
		case 5: entry->pending_x_requester_node = 0; break;
		case 6: entry->resource_x_formation = 18; break;
		case 7:
			assertion = make_resource_x_master_frame(RESOURCE_X_WIRE_ASSERT_X, tag, 0, 0);
			UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_exact(&assertion, 0, &snapshot),
				RESOURCE_X_APPLY_APPLIED);
			break;
		}
		before = *entry;
		UT_ASSERT(cluster_pcm_lock_ko_local_relation_discard_v4(object_drop_locator, 17, 128, &cursor)
			!= CLUSTER_PCM_PI_RELATION_EMPTY);
		UT_ASSERT_EQ(memcmp(entry, &before, sizeof(before)), 0);
		UT_ASSERT_EQ(fake_lwlock_depth, 0);
	}
}

UT_TEST(test_local_drop_discard_is_bounded_and_leaves_other_locator)
{
	BufferTag tag = make_tag(7560), sibling = tag, other = tag;
	ClusterPageWalBindingV1 binding, next;
	ClusterPcmLocalPiSnapshotV1 untouched, after;
	struct StopPcmEntryLayout *first, *second;
	uint32 first_slot;
	uint32 cursor = 0;

	cluster_node_id = 0;
	binding = drop_local_fixture(tag);
	sibling.forkNum = VISIBILITYMAP_FORKNUM;
	next = binding;
	next.identity.forknum = sibling.forkNum;
	UT_ASSERT(cluster_pcm_local_pi_record_v1(sibling, &next));
	other.relNumber++;
	next = binding;
	next.identity.locator = BufTagGetRelFileLocator(&other);
	UT_ASSERT(cluster_pcm_local_pi_record_v1(other, &next));
	UT_ASSERT(cluster_pcm_local_pi_snapshot_v1(other, &untouched));
	/* Registry placement is hashed, not insertion ordered. Select the first
	 * matching slot so one-slot work must leave the other reference intact. */
	first = directory_entry(tag);
	second = directory_entry(sibling);
	if (first->registry_slot > second->registry_slot) {
		first = directory_entry(sibling);
		second = directory_entry(tag);
	}
	first_slot = first->registry_slot;
	cursor = first_slot;
	UT_ASSERT_EQ(cluster_pcm_lock_ko_local_relation_discard_v4(object_drop_locator, 17, 1, &cursor),
		CLUSTER_PCM_PI_RELATION_MORE);
	UT_ASSERT_EQ(cursor, first_slot + 1);
	UT_ASSERT_EQ(first->local_pi_first.source_flags, 0);
	UT_ASSERT(second->local_pi_first.source_flags != 0);
	cursor = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_ko_local_relation_discard_v4(object_drop_locator, 17, 128, &cursor),
		CLUSTER_PCM_PI_RELATION_EMPTY);
	UT_ASSERT_EQ(directory_entry(tag)->local_pi_first.source_flags, 0);
	UT_ASSERT_EQ(directory_entry(sibling)->local_pi_first.source_flags, 0);
	UT_ASSERT(cluster_pcm_local_pi_snapshot_v1(other, &after));
	UT_ASSERT_EQ(memcmp(&untouched, &after, sizeof(after)), 0);
}

static void
close_discard_gate(void)
{
	ResourceXGateSnapshot gate;
	UT_ASSERT(cluster_pcm_lock_resource_x_gate_snapshot(&gate));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_fail_closed_exact(&gate),
		RESOURCE_X_APPLY_APPLIED);
}

UT_TEST(test_local_drop_discard_rechecks_binding_and_gate_after_entry_lock)
{
	BufferTag tag = make_tag(7561);
	struct StopPcmEntryLayout before;
	uint32 cursor = 0;

	cluster_node_id = 0;
	(void)drop_local_fixture(tag);
	before = *directory_entry(tag);
	changed_tag = tag;
	object_entry_lock_hook = change_entry_generation;
	UT_ASSERT_EQ(cluster_pcm_lock_ko_local_relation_discard_v4(object_drop_locator, 17, 128, &cursor),
		CLUSTER_PCM_PI_RELATION_INVALID);
	before.binding_generation++;
	UT_ASSERT_EQ(memcmp(directory_entry(tag), &before, sizeof(before)), 0);
	UT_ASSERT_EQ(fake_lwlock_depth, 0);
	(void)drop_local_fixture(tag);
	before = *directory_entry(tag);
	cursor = 0;
	object_entry_lock_hook = close_discard_gate;
	UT_ASSERT_EQ(cluster_pcm_lock_ko_local_relation_discard_v4(object_drop_locator, 17, 128, &cursor),
		CLUSTER_PCM_PI_RELATION_INVALID);
	UT_ASSERT_EQ(memcmp(directory_entry(tag), &before, sizeof(before)), 0);
	UT_ASSERT_EQ(fake_lwlock_depth, 0);
}

/* Build the original requester receipt/install path for this node, rather
 * than granting a terminal cover by assigning the private round phase.
 * KO exclusion and physical discard remain explicit fixture boundaries. */
static ResourceXAcquisitionRef
drop_begin_cached_round(BufferTag tag)
{
	ResourceXAssertion assertion;
	ResourceXDecodedFrame request, ack, dispatch, grant, image;
	ResourceXAcquisitionRef terminal, ref;
	ResourceXRequesterJoinSnapshot join;

	/* Heap/VM PI is typed WAL responsibility. Other forks exercise current
	 * and round retirement without inventing an unsupported heap PI. */
	if (tag.forkNum == MAIN_FORKNUM || tag.forkNum == VISIBILITYMAP_FORKNUM)
		(void)drop_local_fixture(tag);
	else {
		(void)local_pi_setup(tag);
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
			RESOURCE_X_APPLY_APPLIED);
		object_drop_locator = BufTagGetRelFileLocator(&tag);
	}
	cluster_node_id = 1; /* Distinct from this receipt fixture's master/source0. */
	fake_gcs_master_node = 0;
	object_drop_retired = false;
	object_origin_discard = true;
	/* The existing isolated requester fixture constructs protocol records
	 * without installed R4/fabric admission. Restore shared mode before every
	 * discard call; only KO/physical exclusion are mocked for that consumer. */
	cluster_shared_config = false;
	UT_ASSERT(resource_x_assertion_init(&tag, 1, &assertion));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_step_exact(&assertion,
		0, 17, 31, 77, 51, 61, 1000, 900, 100, 50, false, 0, &request, &terminal),
		RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST);
	ack = make_resource_x_bootstrap_ack_values(&request, 1, 71);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_accept_ack_exact(
		&ack, 0, 61, 77, 110, &dispatch), RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_ASSERT);
	make_resource_x_remote_join_pair(tag, 1, &grant, &image);
	retarget_resource_x_remote_join_pair(&grant, &image, 3, 1);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_join_exact(&image, 0, &join),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_join_exact(&grant, 0, &join),
		RESOURCE_X_APPLY_APPLIED);
	ref = make_resource_x_acquisition_ref(tag, 1, 17, 1);
	cluster_shared_config = true;
	return ref;
}

static void
drop_finish_cached_round(const ResourceXAcquisitionRef *ref)
{
	ResourceXBufferInstallProof install = { 0 };
	ResourceXBufferActivationProof activation = { 0 };
	cluster_shared_config = false; /* Complete the original isolated producer. */
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_t1_grant_exact(ref), RESOURCE_X_APPLY_APPLIED);
	install.ownership_generation = 91;
	install.writer_activation_token = 12;
	install.resource_x_activation_generation = 1;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_apply_exact(ref, &install),
		RESOURCE_X_APPLY_APPLIED);
	activation.ownership_generation = 91;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_activate_exact(ref, &activation),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(cluster_pcm_lock_resource_x_bootstrap_round_cover_matches_exact(ref, 31, 77, 91));
	fake_gcs_master_node = cluster_node_id; /* Construct the local mirror only. */
	cluster_pcm_lock_acquire(ref->assertion.resource, PCM_LOCK_MODE_X);
	fake_gcs_master_node = 0;
	cluster_shared_config = true;
}

static ResourceXIntentSlot
drop_notification(const ResourceXAcquisitionRef *ref)
{
	ResourceXIntentSlot intent = { 0 };
	ResourceXIntentProbeResult result = RESOURCE_X_INTENT_PROBE_MORE;
	uint32 cursor = 0;
	for (unsigned call = 0; call < 16 && result == RESOURCE_X_INTENT_PROBE_MORE; call++)
		result = cluster_pcm_lock_resource_x_ready_intent_probe_exact(
			&ref->assertion.resource, &cursor, &intent);
	UT_ASSERT_EQ(result, RESOURCE_X_INTENT_PROBE_FOUND);
	UT_ASSERT_EQ(intent.body.owner_kind, RESOURCE_X_INTENT_OWNER_REQUESTER_SETTLEMENT);
	return intent;
}

static void
drop_complete_notification(const ResourceXAcquisitionRef *ref)
{
	ResourceXIntentSlot intent = drop_notification(ref);
	/* Transport is a fixture boundary; consume the real control-intent
	 * owner, never a DATA/PI receipt or a fabricated remote retirement ACK. */
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_stage_exact(
		&intent, intent.first_armed_us + 1), RESOURCE_X_INTENT_STAGED);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_intent_complete_exact(&intent));
}

static ResourceXAcquisitionRef
drop_cached_round(BufferTag tag)
{
	ResourceXAcquisitionRef ref = drop_begin_cached_round(tag);
	drop_finish_cached_round(&ref);
	drop_complete_notification(&ref);
	return ref;
}

UT_TEST(test_drop_local_terminal_pair_and_pi_all_forks)
{
	for (int fork = MAIN_FORKNUM; fork <= MAX_FORKNUM; fork++) {
		BufferTag tag = make_tag(7600 + fork);
		struct StopPcmEntryLayout *entry;
		ResourceXAcquisitionRef ref;
		uint32 cursor = 0;
		tag.forkNum = fork;
		ref = drop_cached_round(tag);
		entry = directory_entry(tag);
		UT_ASSERT_EQ(cluster_pcm_lock_ko_local_relation_discard_v4(object_drop_locator,
			17, 128, &cursor), CLUSTER_PCM_PI_RELATION_EMPTY);
		UT_ASSERT_EQ(pg_atomic_read_u32(&entry->master_state), PCM_STATE_N);
		UT_ASSERT_EQ(entry->x_holder_node, -1);
		UT_ASSERT_EQ(entry->resource_x_bootstrap_round.phase, 0);
		UT_ASSERT_EQ(entry->local_pi_first.source_flags | entry->local_pi_last.source_flags, 0);
		UT_ASSERT(!cluster_pcm_lock_resource_x_bootstrap_round_cover_matches_exact(&ref, 31, 77, 91));
		cursor = 0;
		UT_ASSERT_EQ(cluster_pcm_lock_ko_local_relation_scan_v3(object_drop_locator,
			17, 128, &cursor), CLUSTER_PCM_PI_RELATION_EMPTY);
		UT_ASSERT_EQ(fake_lwlock_depth, 0);
	}
}

UT_TEST(test_drop_local_terminal_floors_and_idempotence)
{
	BufferTag tag = make_tag(7610);
	ResourceXAcquisitionRef ref = drop_cached_round(tag);
	struct StopPcmEntryLayout *entry = directory_entry(tag), before, once;
	uint32 cursor = 0;
	/* Completed delivery retains a sequence and exact install lineage. These
	 * are history, not an outstanding delivery executor or install request. */
	entry->resource_x_bootstrap_round.delivery_executor_sequence = 5;
	entry->resource_x_bootstrap_round.install_claim_source = 1;
	entry->resource_x_bootstrap_round.install_claim_pending_generation = 90;
	entry->resource_x_bootstrap_round.install_claim_reservation_token = 7;
	before = *entry;
	UT_ASSERT_EQ(cluster_pcm_lock_ko_local_relation_discard_v4(object_drop_locator,
		17, 128, &cursor), CLUSTER_PCM_PI_RELATION_EMPTY);
	UT_ASSERT_EQ(entry->resource_x_bootstrap_round.highest_attempt_floor,
		before.resource_x_bootstrap_round.highest_attempt_floor);
	UT_ASSERT_EQ(entry->resource_x_bootstrap_round.failed_attempt_floor,
		before.resource_x_bootstrap_round.failed_attempt_floor);
	UT_ASSERT_EQ(entry->resource_x_bootstrap_round.cancelled_attempt_floor,
		before.resource_x_bootstrap_round.highest_attempt_floor);
	UT_ASSERT_EQ(entry->resource_x_retired_acquisition_generation,
		before.resource_x_retired_acquisition_generation);
	UT_ASSERT_EQ(entry->resource_x_requester_base_generation,
		before.resource_x_requester_base_generation);
	UT_ASSERT_EQ(entry->local_pi_revision, before.local_pi_revision + 1);
	UT_ASSERT(!cluster_pcm_lock_resource_x_bootstrap_round_cover_matches_exact(&ref, 31, 77, 91));
	once = *entry;
	cursor = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_ko_local_relation_discard_v4(object_drop_locator,
		17, 128, &cursor), CLUSTER_PCM_PI_RELATION_EMPTY);
	UT_ASSERT_EQ(memcmp(entry, &once, sizeof(once)), 0);
}

UT_TEST(test_drop_local_cached_s_preserves_foreign_current_and_master_pi)
{
	BufferTag tag = make_tag(7611);
	struct StopPcmEntryLayout *entry;
	uint32 cursor = 0;
	cluster_node_id = 0;
	(void)drop_local_fixture(tag);
	object_drop_retired = false;
	object_origin_discard = true;
	cluster_shared_config = false; /* Same mirror-construction boundary as X. */
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	cluster_shared_config = true;
	entry = directory_entry(tag);
	/* No local user remains; a foreign node's cached declaration is separate. */
	entry->s_holder_refcount_local = 0;
	pg_atomic_fetch_or_u32(&entry->s_holders_bitmap, 4);
	pg_atomic_write_u32(&entry->pi_holders_bitmap, 5);
	entry->pi_watermark_lsn = 200;
	entry->pi_watermark_scn = 90;
	UT_ASSERT_EQ(cluster_pcm_lock_ko_local_relation_discard_v4(object_drop_locator,
		17, 128, &cursor), CLUSTER_PCM_PI_RELATION_EMPTY);
	UT_ASSERT_EQ(pg_atomic_read_u32(&entry->master_state), PCM_STATE_S);
	UT_ASSERT_EQ(pg_atomic_read_u32(&entry->s_holders_bitmap), 4);
	UT_ASSERT_EQ(entry->master_holder.node_id, 2);
	UT_ASSERT_EQ(pg_atomic_read_u32(&entry->pi_holders_bitmap), 5);
	UT_ASSERT_EQ(entry->pi_watermark_lsn, 200);
	UT_ASSERT_EQ(entry->pi_watermark_scn, 90);
	cursor = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_ko_directory_relation_scan_v3(object_drop_locator,
		17, 128, &cursor), CLUSTER_PCM_PI_RELATION_PENDING);
}

UT_TEST(test_drop_local_send_delivery_install_and_user_debt_are_atomic)
{
	for (unsigned fault = 0; fault < 14; fault++) {
		BufferTag tag = make_tag(7620 + fault);
		struct StopPcmEntryLayout *entry, before;
		uint32 cursor = 0;
		(void)drop_cached_round(tag);
		entry = directory_entry(tag);
		switch (fault) {
		case 0: entry->resource_x_bootstrap_round.send.pending = 1; break;
		case 1: entry->resource_x_bootstrap_round.delivery_target.buffer_id_plus_one = 1; break;
		case 2: entry->resource_x_bootstrap_round.delivery_executor_sequence = UINT64_MAX; break;
		case 3: entry->resource_x_bootstrap_round.delivery_executor_purpose = 1; break;
		case 4: entry->resource_x_bootstrap_round.install_claim_pending_generation = 1; break;
		case 5: entry->resource_x_bootstrap_round.install_claim_reservation_token = 1; break;
		case 6: entry->resource_x_bootstrap_round.install_claim_source = 1; break;
		case 7: pg_atomic_write_u32(&entry->pin_count, 1); break;
		case 8: pg_atomic_write_u32(&entry->wait_refcount, 1); break;
		case 9: pg_atomic_write_u32(&entry->transport_refcount, 1); break;
		case 10: entry->convert_queue = (PcmConvertQueue *)entry; break;
		case 11: entry->pending_x_requester_node = cluster_node_id; entry->pending_x_since_lsn = 1; break;
		case 12: entry->resource_x_local_owner.state = 1; break;
		case 13: entry->resource_x_bootstrap_round.phase = 3; break;
		}
		before = *entry;
		UT_ASSERT(cluster_pcm_lock_ko_local_relation_discard_v4(object_drop_locator,
			17, 128, &cursor) != CLUSTER_PCM_PI_RELATION_EMPTY);
		UT_ASSERT_EQ(memcmp(entry, &before, sizeof(before)), 0);
		UT_ASSERT_EQ(fake_lwlock_depth, 0);
	}
}

UT_TEST(test_drop_local_terminal_identity_and_pi_failure_are_atomic)
{
	for (unsigned fault = 0; fault < 16; fault++) {
		BufferTag tag = make_tag(7640 + fault);
		struct StopPcmEntryLayout *entry, before;
		ClusterPcmResourceXBootstrapRound *round;
		uint32 cursor = 0;
		(void)drop_cached_round(tag);
		entry = directory_entry(tag);
		round = &entry->resource_x_bootstrap_round;
		switch (fault) {
		case 0: round->terminal_ref.assertion.resource.blockNum++; break;
		case 1: round->request.logical_assertion.resource.blockNum++; break;
		case 2: round->resource_formation++; break;
		case 3: round->master_session_incarnation = 0; break;
		case 4: round->r4_record_generation = 0; break;
		case 5: round->cached_ownership_generation = 0; break;
		case 6: round->terminal_authority_generation = 0; break;
		case 7: round->highest_attempt_floor = UINT64_MAX; break;
		case 8: round->failed_attempt_floor = round->highest_attempt_floor + 1; break;
		case 9: round->cancelled_attempt_floor = round->highest_attempt_floor + 1; break;
		case 10: entry->resource_x_retired_acquisition_generation++; break;
		case 11: entry->local_pi_last.source_flags &= UINT16_C(0x7fff); break;
		case 12: entry->local_pi_last.token = 0; break;
		case 13: round->request.logical_assertion.requester_node = 2; break;
		case 14: round->failed_attempt_floor = round->highest_attempt_floor; break;
		case 15: round->cancelled_attempt_floor = round->highest_attempt_floor; break;
		}
		before = *entry;
		UT_ASSERT(cluster_pcm_lock_ko_local_relation_discard_v4(object_drop_locator,
			17, 128, &cursor) != CLUSTER_PCM_PI_RELATION_EMPTY);
		UT_ASSERT_EQ(memcmp(entry, &before, sizeof(before)), 0);
	}
}

UT_TEST(test_drop_local_receipt_stays_with_original_installer_until_terminal)
{
	BufferTag tag = make_tag(7660);
	ResourceXAcquisitionRef ref = drop_begin_cached_round(tag);
	struct StopPcmEntryLayout *entry = directory_entry(tag), before = *entry;
	uint32 cursor = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_ko_local_relation_discard_v4(object_drop_locator,
		17, 128, &cursor), CLUSTER_PCM_PI_RELATION_PENDING);
	UT_ASSERT_EQ(memcmp(entry, &before, sizeof(before)), 0);
	/* Original T1/T2/T3 consumes the receipt and establishes terminal cover. */
	drop_finish_cached_round(&ref);
	cursor = 0;
	before = *entry;
	UT_ASSERT_EQ(cluster_pcm_lock_ko_local_relation_discard_v4(object_drop_locator,
		17, 128, &cursor), CLUSTER_PCM_PI_RELATION_PENDING);
	UT_ASSERT_EQ(memcmp(entry, &before, sizeof(before)), 0);
	{
		ResourceXIntentSlot intent = drop_notification(&ref);
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_outbound_intent_stage_exact(
			&intent, intent.first_armed_us + 1), RESOURCE_X_INTENT_STAGED);
		cursor = 0;
		UT_ASSERT_EQ(cluster_pcm_lock_ko_local_relation_discard_v4(object_drop_locator,
			17, 128, &cursor), CLUSTER_PCM_PI_RELATION_PENDING);
		UT_ASSERT_EQ(memcmp(entry, &before, sizeof(before)), 0);
		UT_ASSERT(cluster_pcm_lock_resource_x_outbound_intent_complete_exact(&intent));
	}
	cursor = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_ko_local_relation_discard_v4(object_drop_locator,
		17, 128, &cursor), CLUSTER_PCM_PI_RELATION_EMPTY);
}

UT_TEST(test_drop_local_current_retirement_preserves_remote_master_request)
{
	BufferTag tag = make_tag(7661);
	ResourceXDecodedFrame assertion;
	ResourceXMasterSnapshot before, after;
	uint32 cursor = 0;
	(void)drop_cached_round(tag);
	assertion = make_resource_x_master_frame(RESOURCE_X_WIRE_ASSERT_X, tag, 2, 2);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_exact(&assertion, 2, &before),
		RESOURCE_X_APPLY_APPLIED);
	/* A foreign head sees this node's cached X as an existing blocker. Its
	 * canonical request remains byte-for-byte unchanged by local discard. */
	UT_ASSERT_EQ(before.phase, RESOURCE_X_MASTER_WAIT_BLOCKERS);
	UT_ASSERT_EQ(cluster_pcm_lock_ko_local_relation_discard_v4(object_drop_locator,
		17, 128, &cursor), CLUSTER_PCM_PI_RELATION_EMPTY);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_master_snapshot_exact(
		&assertion.common.logical_assertion, &after), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(memcmp(&before, &after, sizeof(before)), 0);
}

UT_TEST(test_drop_local_empty_round_shape_and_live_scope_are_not_shortcuts)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		BufferTag tag = make_tag(7670 + fault);
		struct StopPcmEntryLayout *entry, before;
		uint32 cursor = 0;
		cluster_node_id = 0;
		(void)drop_local_fixture(tag);
		entry = directory_entry(tag);
		if (fault == 0) entry->resource_x_bootstrap_round.send.episode = 1;
		if (fault == 1) entry->resource_x_bootstrap_round.failed_attempt_floor = 1;
		if (fault == 2) object_drop_retired = object_origin_discard = false;
		if (fault == 3) entry->resource_x_bootstrap_round.delivery_target.buffer_id_plus_one = 1;
		before = *entry;
		UT_ASSERT(cluster_pcm_lock_ko_local_relation_discard_v4(object_drop_locator,
			17, 128, &cursor) != CLUSTER_PCM_PI_RELATION_EMPTY);
		UT_ASSERT_EQ(memcmp(entry, &before, sizeof(before)), 0);
	}
}

UT_TEST(test_drop_local_canonical_empty_round_current_is_exact_and_node_local)
{
	for (unsigned holder = 0; holder <= 2; holder += 2) {
		BufferTag tag = make_tag(7680 + holder);
		struct StopPcmEntryLayout *entry, before;
		uint32 cursor = 0;
		(void)drop_local_fixture(tag);
		cluster_node_id = holder;
		cluster_shared_config = false; /* Construct this node's cached mirror. */
		cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_X);
		cluster_shared_config = true;
		cluster_node_id = 0;
		object_drop_retired = object_origin_discard = false;
		entry = directory_entry(tag);
		before = *entry;
		UT_ASSERT_EQ(cluster_pcm_lock_ko_local_relation_discard_v4(object_drop_locator,
			17, 128, &cursor), CLUSTER_PCM_PI_RELATION_INVALID);
		UT_ASSERT_EQ(memcmp(entry, &before, sizeof(before)), 0);
		object_origin_discard = true;
		UT_ASSERT_EQ(cluster_pcm_lock_ko_local_relation_discard_v4(object_drop_locator,
			17, 128, &cursor), CLUSTER_PCM_PI_RELATION_EMPTY);
		UT_ASSERT_EQ(entry->x_holder_node, holder == 0 ? -1 : 2);
		UT_ASSERT_EQ(pg_atomic_read_u32(&entry->master_state), holder == 0 ? PCM_STATE_N : PCM_STATE_X);
		UT_ASSERT_EQ(entry->local_pi_first.source_flags | entry->local_pi_last.source_flags, 0);
	}
}

UT_TEST(test_drop_local_delivery_finishes_only_through_original_owner)
{
	DeliveryObserverFixture fixture;
	struct StopPcmEntryLayout *entry, before;
	uint32 cursor = 0;
	setup_terminal_delivery_observer(&fixture);
	cluster_enabled = cluster_shared_config = true;
	object_drop_locator = BufTagGetRelFileLocator(&fixture.ref.assertion.resource);
	object_drop_retired = false;
	object_origin_discard = true;
	entry = directory_entry(fixture.ref.assertion.resource);
	before = *entry;
	UT_ASSERT_EQ(cluster_pcm_lock_ko_local_relation_discard_v4(object_drop_locator,
		17, 128, &cursor), CLUSTER_PCM_PI_RELATION_PENDING);
	UT_ASSERT_EQ(memcmp(entry, &before, sizeof(before)), 0);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_delivery_complete_exact(
		&fixture.delivery, &fixture.installed), RESOURCE_X_APPLY_APPLIED);
	drop_complete_notification(&fixture.ref);
	cursor = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_ko_local_relation_discard_v4(object_drop_locator,
		17, 128, &cursor), CLUSTER_PCM_PI_RELATION_EMPTY);
	UT_ASSERT_EQ(entry->resource_x_bootstrap_round.phase, 0);
}

UT_TEST(test_drop_local_requester_send_completes_before_terminal_consumption)
{
	BufferTag tag = make_tag(7685);
	ResourceXAcquisitionRef ref = drop_begin_cached_round(tag);
	struct StopPcmEntryLayout *entry = directory_entry(tag), before;
	ResourceXDecodedFrame dispatch = { 0 };
	ResourceXIntentSlot send;
	uint32 cursor = 0;
	dispatch.kind = RESOURCE_X_WIRE_ASSERT_X;
	dispatch.payload_bytes = RESOURCE_X_CONTROL_V1_BYTES;
	dispatch.common = entry->resource_x_bootstrap_round.assertion;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_send_begin_exact(
		&dispatch, 0, 1, 120, &send), RESOURCE_X_INTENT_STAGED);
	before = *entry;
	UT_ASSERT_EQ(cluster_pcm_lock_ko_local_relation_discard_v4(object_drop_locator,
		17, 128, &cursor), CLUSTER_PCM_PI_RELATION_PENDING);
	UT_ASSERT_EQ(memcmp(entry, &before, sizeof(before)), 0);
	UT_ASSERT(cluster_pcm_lock_resource_x_requester_send_complete_exact(&send, true, 121));
	UT_ASSERT(!cluster_pcm_lock_resource_x_requester_send_complete_exact(&send, true, 122));
	drop_finish_cached_round(&ref);
	drop_complete_notification(&ref);
	cursor = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_ko_local_relation_discard_v4(object_drop_locator,
		17, 128, &cursor), CLUSTER_PCM_PI_RELATION_EMPTY);
}

/* Real master assertion/grant/install/SETTLED producer, followed by LOCAL. */
static BufferTag
drop_settled_master_fixture(bool tombstone)
{
	BufferTag tag = make_tag(7300);
	PcmAuthoritySnapshot authority;
	ResourceXDecodedFrame assertion;
	ResourceXDecodedFrame settlement;
	ResourceXDurableProof durable;
	ResourceXIntentSlot grant_intent;
	ResourceXMasterSnapshot snapshot;
	uint8 grant_bytes[RESOURCE_X_PROOF_V1_BYTES];

	reset_fake_pcm_runtime(4);
	cluster_node_id = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_apply_gcs_transition_result(tag, PCM_TRANS_N_TO_X, 0),
				 PCM_GCS_TRANSITION_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_apply_gcs_transition_result(tag, PCM_TRANS_X_TO_N_RELEASE, 0),
				 PCM_GCS_TRANSITION_APPLIED);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &authority));

	assertion = make_resource_x_master_frame(RESOURCE_X_WIRE_ASSERT_X, tag, 3, 3);
	assertion.common.base_authority_generation = authority.transition_count;
	assertion.common.authority_generation = authority.transition_count;
	UT_ASSERT_EQ(
		cluster_pcm_lock_resource_x_adapter_base_bind_exact(&assertion.common.logical_assertion, 17,
															authority.transition_count, &authority),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_assert_exact(&assertion, 3, &snapshot),
				 RESOURCE_X_APPLY_APPLIED);

	memset(&durable, 0, sizeof(durable));
	durable.assertion = assertion.common.logical_assertion;
	durable.base_authority_generation = authority.transition_count;
	durable.resource_formation = 17;
	durable.master_session_incarnation = 31;
	durable.assertion_sequence = assertion.common.assertion_sequence;
	durable.requester_target_generation = 41;
	durable.page_scn_lsn = 82;
	durable.page_checksum = UINT32_C(0x12345678);
	durable.source_proof_crc32c = UINT32_C(0x87654321);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_durable_proof_exact(&durable, &snapshot),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(
		cluster_pcm_lock_resource_x_grant_intent_snapshot_exact(
			&assertion.common.logical_assertion, &grant_intent, grant_bytes, sizeof(grant_bytes)),
		RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_grant_intent_stage_exact(&grant_intent, 101),
				 RESOURCE_X_INTENT_STAGED);
	UT_ASSERT(cluster_pcm_lock_resource_x_grant_intent_complete_exact(&grant_intent));

	settlement = make_resource_x_master_frame(RESOURCE_X_WIRE_INSTALL_SETTLEMENT, tag, 3, 3);
	settlement.common.outcome = RESOURCE_X_OUTCOME_OK;
	settlement.common.base_authority_generation = durable.base_authority_generation;
	settlement.common.authority_generation = snapshot.final_authority_generation;
	settlement.body.install_settlement.conversion_base_generation
		= durable.base_authority_generation;
	settlement.body.install_settlement.final_authority_generation
		= snapshot.final_authority_generation;
	settlement.body.install_settlement.requester_connection_generation = 91;
	settlement.body.install_settlement.requester_target_generation = 41;
	settlement.body.install_settlement.page_scn_lsn = durable.page_scn_lsn;
	settlement.body.install_settlement.page_checksum = durable.page_checksum;
	settlement.body.install_settlement.source_proof_crc32c = durable.source_proof_crc32c;
	settlement.body.install_settlement.installed_mode = PCM_STATE_X;
	settlement.body.install_settlement.requester_role = RESOURCE_X_REQUESTER_ROLE_ACQUIRER;
	settlement.body.install_settlement.terminal_outcome = RESOURCE_X_OUTCOME_OK;
	settlement.body.install_settlement.terminal_state = RESOURCE_X_SETTLEMENT_TERMINAL_INSTALLED;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_install_settlement_exact(&settlement, 3, &snapshot),
				 RESOURCE_X_APPLY_APPLIED);
	if (tombstone)
		UT_ASSERT_EQ(
		cluster_pcm_lock_resource_x_settled_retire_exact(
			&assertion.common.logical_assertion, assertion.common.assertion_sequence, &snapshot),
		RESOURCE_X_APPLY_APPLIED);

	cluster_node_id = 3; /* Original requester now runs its local consumer. */
	object_drop_locator = BufTagGetRelFileLocator(&tag);
	object_drop_retired = false;
	object_origin_discard = true;
	cluster_enabled = cluster_shared_config = true;
	return tag;
}

UT_TEST(test_real_settled_local_drop_stays_pending_until_original_am03_retirement)
{
	for (unsigned tombstone = 0; tombstone < 2; tombstone++) {
		BufferTag tag = drop_settled_master_fixture(tombstone != 0);
		struct StopPcmEntryLayout *entry = directory_entry(tag), before;
		uint32 cursor = 0;
		const char *reason = NULL;

		UT_ASSERT_EQ(stop_pcm_poll(false, NULL, NULL, NULL), CLUSTER_NORMAL_STOP_READY);
		UT_ASSERT_EQ(cluster_pcm_lock_ko_local_relation_discard_v4(object_drop_locator,
			17, 128, &cursor), CLUSTER_PCM_PI_RELATION_EMPTY);
		UT_ASSERT_EQ(pg_atomic_read_u32(&entry->master_state), PCM_STATE_N);
		UT_ASSERT_EQ(entry->x_holder_node, -1);
		UT_ASSERT_EQ(entry->local_pi_first.source_flags, 0);
		UT_ASSERT_EQ(stop_pcm_poll(false, NULL, NULL, &reason), CLUSTER_NORMAL_STOP_INVALID);
		UT_ASSERT_EQ(scan_directory(), 0);
		UT_ASSERT(directory_entry(tag) != NULL);
		object_origin_discard = false;
		object_drop_retired = true; /* Explicit immutable KO/no-I/O proof boundary. */
		before = *entry;
		UT_ASSERT_EQ(stop_pcm_poll(false, NULL, NULL, &reason), CLUSTER_NORMAL_STOP_PENDING);
		UT_ASSERT_STR_EQ(reason, "PCM_OBJECT_RETIRE_PENDING");
		UT_ASSERT(memcmp(entry, &before, sizeof(before)) == 0);
		UT_ASSERT_EQ(scan_directory(), 0);
		UT_ASSERT(directory_entry(tag) == NULL);
		UT_ASSERT_EQ(stop_pcm_poll(false, NULL, NULL, &reason), CLUSTER_NORMAL_STOP_READY);
		UT_ASSERT_EQ(fake_lwlock_depth, 0);
	}
}

UT_TEST(test_pcm_stop_first_refusal_keeps_original_locked_identity)
{
	BufferTag tag = drop_settled_master_fixture(true);
	PcmNormalStopRefusalV1 first, after;
	struct StopPcmEntryLayout *entry = directory_entry(tag);
	uint32 cursor = 0;

	memset(&first, 0x55, sizeof(first)); after = first;
	UT_ASSERT(!cluster_pcm_normal_stop_first_refusal_v1(&first));
	UT_ASSERT(memcmp(&first, &after, sizeof(first)) == 0);
	UT_ASSERT_EQ(cluster_pcm_lock_ko_local_relation_discard_v4(object_drop_locator,
		17, 128, &cursor), CLUSTER_PCM_PI_RELATION_EMPTY);
	UT_ASSERT_EQ(stop_pcm_poll(false, NULL, NULL, NULL), CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT(cluster_pcm_normal_stop_first_refusal_v1(&first));
	UT_ASSERT_EQ(first.code, 218); /* Exact original SETTLED/no-cover predicate. */
	UT_ASSERT(first.source_line != 0);
	UT_ASSERT(BufferTagsEqual(&first.tag, &tag));
	UT_ASSERT_EQ(first.binding, entry->binding_generation);
	UT_ASSERT_EQ(first.slot, entry->registry_slot);
	UT_ASSERT_EQ(first.mode, PCM_STATE_N);
	UT_ASSERT_EQ(first.x_holder, -1);
	UT_ASSERT_EQ(first.pi_holders, 0);
	UT_ASSERT_EQ(first.pins | first.waits | first.transport | first.send_pending, 0);
	UT_ASSERT_EQ(first.tombstone_valid, 1);
	UT_ASSERT_EQ(first.tombstone_node, 3);
	UT_ASSERT_EQ(first.tombstone_phase, RESOURCE_X_MASTER_SETTLED);
	UT_ASSERT_EQ(first.request_node, 3);
	UT_ASSERT_EQ(first.request_phase, RESOURCE_X_MASTER_SETTLED);
	pg_atomic_write_u32(&entry->lifecycle, UINT32_MAX);
	UT_ASSERT_EQ(stop_pcm_poll(false, NULL, NULL, NULL), CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT(cluster_pcm_normal_stop_first_refusal_v1(&after));
	UT_ASSERT(memcmp(&first, &after, sizeof(first)) == 0);
	UT_ASSERT(!cluster_pcm_normal_stop_first_refusal_v1(NULL));
}

UT_TEST(test_object_retire_pending_never_hides_another_invalid_entry_or_registry)
{
	for (unsigned orphan = 0; orphan < 2; orphan++) {
		BufferTag tag = drop_settled_master_fixture(true), other = tag;
		struct StopPcmEntryLayout *entry = directory_entry(tag), before;
		PcmNormalStopRefusalV1 first = {0};
		uint32 cursor = 0;
		const char *reason = NULL;
		bool found = false;

		UT_ASSERT_EQ(cluster_pcm_lock_ko_local_relation_discard_v4(object_drop_locator,
			17, 128, &cursor), CLUSTER_PCM_PI_RELATION_EMPTY);
		object_origin_discard = false;
		object_drop_retired = true;
		before = *entry;
		UT_ASSERT_EQ(stop_pcm_poll(false, NULL, NULL, &reason), CLUSTER_NORMAL_STOP_PENDING);
		UT_ASSERT_STR_EQ(reason, "PCM_OBJECT_RETIRE_PENDING");
		other.relNumber++;
		UT_ASSERT(cluster_gcs_block_master_rebuild_from_redeclare(
			other, PCM_STATE_N, 80, 90, 0, 7));
		/* The fake hash visits the proven dead entry first. The independent
		 * refusal must survive both the entry and reverse-registry scans. */
		if (orphan)
			(void)hash_search((HTAB *)&fake_pcm_htab_token, &other, HASH_REMOVE, &found);
		else
			pg_atomic_write_u32(&directory_entry(other)->lifecycle, UINT32_MAX);
		if (orphan)
			UT_ASSERT(found);
		UT_ASSERT_EQ(stop_pcm_poll(false, NULL, NULL, &reason), CLUSTER_NORMAL_STOP_INVALID);
		UT_ASSERT(memcmp(entry, &before, sizeof(before)) == 0);
		UT_ASSERT(directory_entry(tag) != NULL);
		UT_ASSERT(cluster_pcm_normal_stop_first_refusal_v1(&first));
		UT_ASSERT_EQ(first.code, 218);
		UT_ASSERT(BufferTagsEqual(&first.tag, &tag));
		UT_ASSERT_EQ(fake_lwlock_depth, 0);
	}
}

UT_TEST(test_settled_object_retirement_keeps_every_unqualified_responsibility)
{
	for (unsigned fault = 0; fault < 12; fault++) {
		BufferTag tag = drop_settled_master_fixture(true);
		struct StopPcmEntryLayout *entry = directory_entry(tag), before;
		uint32 cursor = 0;
		UT_ASSERT_EQ(cluster_pcm_lock_ko_local_relation_discard_v4(object_drop_locator,
			17, 128, &cursor), CLUSTER_PCM_PI_RELATION_EMPTY);
		object_origin_discard = false;
		object_drop_retired = true;
		switch (fault) {
		case 0: object_drop_retired = false; break; /* Includes entered-I/O not closed. */
		case 1: object_drop_locator.relNumber++; break;
		case 2: entry->binding_generation++; break;
		case 3: pg_atomic_write_u32(&entry->pin_count, 1); break;
		case 4: pg_atomic_write_u32(&entry->wait_refcount, 1); break;
		case 5: pg_atomic_write_u32(&entry->transport_refcount, 1); break;
		case 6: entry->resource_x_local_owner.state = UINT8_MAX; break;
		case 7: entry->resource_x_bootstrap_round.send.pending = 1; break;
		case 8: entry->s_holder_refcount_local = 1; break;
		case 9: entry->pending_x_requester_node = 1; break;
		case 10:
			pg_atomic_write_u32(&entry->master_state, PCM_STATE_X);
			entry->x_holder_node = 3;
			break;
		case 11: entry->resource_x_bootstrap_round.phase = UINT8_C(5) /* Original private FAILED_CLOSED phase, not the public action. */; break;
		}
		before = *entry;
		UT_ASSERT(stop_pcm_poll(false, NULL, NULL, NULL) != CLUSTER_NORMAL_STOP_READY);
		UT_ASSERT_EQ(scan_directory(), 0);
		UT_ASSERT(directory_entry(tag) != NULL);
		UT_ASSERT(memcmp(entry, &before, sizeof(before)) == 0);
		UT_ASSERT_EQ(fake_lwlock_depth, 0);
	}
}
UT_TEST(test_pcm_stop_refusal_distinguishes_original_protocol_and_outer_guards)
{
	const uint32 codes[] = {101, 102, 104, 105, 105, 302};
	for (unsigned fault = 0; fault < lengthof(codes); fault++) {
		BufferTag tag = drop_settled_master_fixture(true);
		struct StopPcmEntryLayout *entry = directory_entry(tag);
		PcmNormalStopRefusalV1 first;
		uint32 cursor = 0;
		UT_ASSERT_EQ(cluster_pcm_lock_ko_local_relation_discard_v4(object_drop_locator,
			17, 128, &cursor), CLUSTER_PCM_PI_RELATION_EMPTY);
		switch (fault) {
		case 0: pg_atomic_write_u32(&entry->lifecycle, UINT32_MAX); break;
		case 1: entry->resource_x_local_owner.state = UINT8_MAX; break;
		case 2: entry->resource_x_bootstrap_round.phase = UINT8_MAX; break;
		case 3: entry->binding_generation = 0; break;
		case 4: entry->registry_slot = UINT32_MAX; break;
		case 5: entry->resource_x_bootstrap_round.phase = UINT8_C(5) /* Original private FAILED_CLOSED phase, not the public action. */; break;
		}
		UT_ASSERT_EQ(stop_pcm_poll(false, NULL, NULL, NULL), CLUSTER_NORMAL_STOP_INVALID);
		UT_ASSERT(cluster_pcm_normal_stop_first_refusal_v1(&first));
		UT_ASSERT_EQ(first.code, codes[fault]);
		UT_ASSERT_EQ(first.binding, entry->binding_generation);
		UT_ASSERT_EQ(first.slot, entry->registry_slot);
		UT_ASSERT(BufferTagsEqual(&first.tag, &tag));
		UT_ASSERT_EQ(first.owner_state, entry->resource_x_local_owner.state);
		UT_ASSERT_EQ(first.round_phase, entry->resource_x_bootstrap_round.phase);
		UT_ASSERT_EQ(fake_lwlock_depth, 0);
	}
}

int
main(void)
{
	UT_PLAN(38);
	UT_RUN(test_object_retire_pending_never_hides_another_invalid_entry_or_registry);
	UT_RUN(test_settled_object_retirement_keeps_every_unqualified_responsibility);
	UT_RUN(test_pcm_stop_refusal_distinguishes_original_protocol_and_outer_guards);
	UT_RUN(test_pcm_stop_first_refusal_keeps_original_locked_identity);
	UT_RUN(test_real_settled_local_drop_stays_pending_until_original_am03_retirement);
	UT_RUN(test_origin_discard_scope_does_not_retire_master_directory);
	UT_RUN(test_terminal_drop_ends_directory_pi_without_data_or_ack);
	UT_RUN(test_original_bounded_reclaim_retires_terminal_watermark_and_pi);
	UT_RUN(test_unproven_or_wrong_object_keeps_original_debt);
	UT_RUN(test_terminal_identity_never_clears_local_or_inflight_owners);
	UT_RUN(test_canonical_wait_proof_is_not_a_pure_retired_entry);
	UT_RUN(test_rebind_gate_and_lock_contention_do_not_erase_entry);
	UT_RUN(test_terminal_identity_closes_legal_node31_directory_debt);
	UT_RUN(test_committed_drop_moves_only_its_two_local_references);
	UT_RUN(test_original_cp_and_lmon_owners_end_dead_object_local_pi);
	UT_RUN(test_dead_local_pi_refuses_unknown_identity_or_malformed_reference);
	UT_RUN(test_dead_local_pi_keeps_inflight_current_and_reference_gates);
	/* Original live-object consumers remain DATA/ACK-qualified. */
	UT_RUN(test_local_pi_alone_prevents_directory_reclamation);
	UT_RUN(test_local_pi_retirement_requires_exact_responsibility_and_physical_ack);
	UT_RUN(test_local_structural_retirement_preserves_concurrent_responsibility);
	UT_RUN(test_local_pi_keeps_first_and_latest_across_master_changes);
	UT_RUN(test_local_drop_discard_preserves_remote_master_obligation);
	UT_RUN(test_local_drop_discard_releases_exact_sources_once);
	UT_RUN(test_local_drop_discard_rejects_unknown_identity_refs_and_cut);
	UT_RUN(test_local_drop_discard_keeps_current_and_inflight_local_owners);
	UT_RUN(test_local_drop_discard_is_bounded_and_leaves_other_locator);
	UT_RUN(test_local_drop_discard_rechecks_binding_and_gate_after_entry_lock);
	UT_RUN(test_drop_local_terminal_pair_and_pi_all_forks);
	UT_RUN(test_drop_local_terminal_floors_and_idempotence);
	UT_RUN(test_drop_local_cached_s_preserves_foreign_current_and_master_pi);
	UT_RUN(test_drop_local_send_delivery_install_and_user_debt_are_atomic);
	UT_RUN(test_drop_local_terminal_identity_and_pi_failure_are_atomic);
	UT_RUN(test_drop_local_receipt_stays_with_original_installer_until_terminal);
	UT_RUN(test_drop_local_current_retirement_preserves_remote_master_request);
	UT_RUN(test_drop_local_empty_round_shape_and_live_scope_are_not_shortcuts);
	UT_RUN(test_drop_local_canonical_empty_round_current_is_exact_and_node_local);
	UT_RUN(test_drop_local_delivery_finishes_only_through_original_owner);
	UT_RUN(test_drop_local_requester_send_completes_before_terminal_consumption);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
