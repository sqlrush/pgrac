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

int
main(void)
{
	UT_PLAN(22);
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
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
