/* Whole original relation/history references survive their scanner buffers.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
static ClusterRecoveryCensus *
reference_census_fixture(ClusterWalInputsV1 **inputs, int *fd, char path[MAXPGPATH])
{
	ClusterRecoveryCensus *census;
	owned_page_roots_fixture = true;
	census = owned_census_fixture(inputs, fd, path, false);
	owned_page_roots_fixture = false;
	owned_page_root_files(*fd);
	return census;
}

UT_TEST(test_census_references_own_every_untouched_heap_and_inverse_history_emission)
{
	ClusterWalInputsV1 *inputs;
	ClusterRecoveryCensus *census;
	ClusterRecoveryCensusEntry emitted;
	ClusterRecoveryCensusReferenceV1 expected[64] = {0};
	char *tuples = calloc(64, BLCKSZ);
	const ClusterRecoveryCensusReferencesV1 *view;
	const ClusterRecoveryCensusPagesV1 *pages;
	ClusterRecoveryCensusResult result = CLUSTER_RECOVERY_CENSUS_MORE;
	uint32 count = 0, histories = 0, items = 0, free_slots = 0, page_wal = 99;
	char path[MAXPGPATH]; int fd;
	census = reference_census_fixture(&inputs, &fd, path);
	UT_ASSERT_NULL(cluster_recovery_census_references_v1(census));
	UT_ASSERT(tuples != NULL);
	for (unsigned steps = 0; steps < 200000; steps++) {
		result = cluster_recovery_census_next(census, &emitted);
		if (result >= CLUSTER_RECOVERY_CENSUS_DRAINED) break;
		if (result == CLUSTER_RECOVERY_CENSUS_ENTRY
			&& emitted.kind == CLUSTER_RECOVERY_CENSUS_RELATION
			&& emitted.value.relation.kind == CLUSTER_RECOVERY_RELATIONS_REFERENCE) {
			const ClusterRecoveryPageReference *ref = &emitted.value.relation.reference;
			UT_ASSERT(count < lengthof(expected));
			if (count == lengthof(expected)) break;
			expected[count].page_index = census->page_count - 1;
			expected[count].version = ref->version;
			expected[count].entry = ref->entry;
			expected[count].transaction = ref->transaction;
			if (ref->entry.tuple_length != 0) {
				UT_ASSERT(ref->entry.tuple_length <= BLCKSZ);
				memcpy(tuples + count * BLCKSZ, ref->entry.tuple_bytes, ref->entry.tuple_length);
			}
			histories += ref->version != 0;
			items += ref->entry.kind == CLUSTER_RECOVERY_HEAP_ITEM;
			free_slots += ref->entry.kind == CLUSTER_RECOVERY_HEAP_ITL
				&& ref->entry.itl.flags == ITL_FLAG_FREE;
			count++;
		}
	}
	UT_ASSERT_EQ(result, CLUSTER_RECOVERY_CENSUS_DRAINED);
	UT_ASSERT(count > 8 && histories != 0 && items != 0 && free_slots != 0);
	UT_ASSERT(cluster_recovery_census_page_wal_count_v1(census, &page_wal));
	UT_ASSERT_EQ(page_wal, 0); /* No PAGE WAL/touched-set supplies these references. */
	view = cluster_recovery_census_references_v1(census);
	pages = cluster_recovery_census_pages_v1(census);
	UT_ASSERT(view != NULL && pages != NULL);
	if (view != NULL && pages != NULL) {
		UT_ASSERT_EQ(view->count, count);
		for (uint32 i = 0; i < Min(count, view->count); i++) {
			ClusterRecoveryCensusReferenceV1 value = view->entries[i];
			UT_ASSERT(value.page_index < pages->count);
			UT_ASSERT_EQ(pages->entries[value.page_index].reservation.identity.key.locator.relNumber, 16385);
			if (value.entry.tuple_length != 0) {
				UT_ASSERT(value.entry.tuple_bytes != expected[i].entry.tuple_bytes);
				UT_ASSERT(memcmp(value.entry.tuple_bytes, tuples + i * BLCKSZ,
					value.entry.tuple_length) == 0);
			}
			value.entry.tuple_bytes = expected[i].entry.tuple_bytes;
			UT_ASSERT(memcmp(&value, &expected[i], sizeof(value)) == 0);
		}
		UT_ASSERT(cluster_recovery_census_references_v1(census) == view);
	}
	free(tuples); owned_census_done(census, &inputs, fd, path);
}

UT_TEST(test_census_references_complete_empty_is_not_prefix_or_lost_scope)
{
	ClusterWalInputsV1 *inputs;
	ClusterRecoveryCensus *census;
	const ClusterRecoveryCensusReferencesV1 *view;
	char path[MAXPGPATH]; int fd;
	census = owned_census_fixture(&inputs, &fd, path, false);
	UT_ASSERT_NULL(cluster_recovery_census_references_v1(census));
	UT_ASSERT_EQ(owned_census_until(census, 0), CLUSTER_RECOVERY_CENSUS_DRAINED);
	view = cluster_recovery_census_references_v1(census);
	UT_ASSERT_NOT_NULL(view);
	if (view != NULL) UT_ASSERT_EQ(view->count, 0);
	inputs_pin_current = false;
	UT_ASSERT_NULL(cluster_recovery_census_references_v1(census));
	inputs_pin_current = true;
	UT_ASSERT_NULL(cluster_recovery_census_references_v1(census));
	owned_census_done(census, &inputs, fd, path);
}

UT_TEST(test_census_references_reject_modified_metadata_tuple_and_rebound_pointer)
{
	for (unsigned fault = 0; fault < 5; fault++) {
		ClusterWalInputsV1 *inputs;
		ClusterRecoveryCensus *census;
		const ClusterRecoveryCensusReferencesV1 *view;
		ClusterRecoveryCensusManifest manifest;
		char path[MAXPGPATH]; int fd;
		census = reference_census_fixture(&inputs, &fd, path);
		UT_ASSERT_EQ(owned_census_until(census, 0), CLUSTER_RECOVERY_CENSUS_DRAINED);
		view = cluster_recovery_census_references_v1(census);
		UT_ASSERT_NOT_NULL(view);
		if (view != NULL) {
			ClusterRecoveryCensusReferenceV1 *value = NULL;
			for (uint32 i = 0; i < view->count; i++)
				if (view->entries[i].entry.tuple_length != 0) {
					value = (ClusterRecoveryCensusReferenceV1 *)&view->entries[i]; break;
				}
			UT_ASSERT_NOT_NULL(value);
			if (value != NULL) {
				if (fault == 0) value->version++;
				if (fault == 1) value->page_index++;
				if (fault == 2) ((char *)value->entry.tuple_bytes)[0] ^= 1;
				if (fault == 3) value->entry.tuple_bytes = (char *)(uintptr_t)1;
				if (fault == 4) value->transaction.transaction.slot_wrap++;
				UT_ASSERT_NULL(cluster_recovery_census_references_v1(census));
				UT_ASSERT(!cluster_recovery_census_manifest(census, &manifest));
				UT_ASSERT_NULL(cluster_recovery_census_references_v1(census));
			}
		}
		owned_census_done(census, &inputs, fd, path);
	}
}

UT_TEST(test_census_references_bound_metadata_and_tuple_storage_before_publication)
{
	for (unsigned fault = 0; fault < 2; fault++) {
		ClusterWalInputsV1 *inputs;
		ClusterRecoveryCensus *census;
		char path[MAXPGPATH]; int fd;
		census = reference_census_fixture(&inputs, &fd, path);
		/* Retain the allocated original cap for cleanup; force its boundary. */
		if (fault == 0) census->reference_capacity = 0;
		else census->reference_tuple_capacity = 0;
		UT_ASSERT_EQ(owned_census_until(census, 0), CLUSTER_RECOVERY_CENSUS_CAPACITY);
		UT_ASSERT_NULL(cluster_recovery_census_references_v1(census));
		owned_census_sticky(census, CLUSTER_RECOVERY_CENSUS_CAPACITY);
		owned_census_done(census, &inputs, fd, path);
	}
}
