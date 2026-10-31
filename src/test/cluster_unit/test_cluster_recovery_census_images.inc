/* Complete original scan versions, not reconstructed reference fragments. */
static ClusterRecoveryCensus *
census_images_fixture(ClusterWalInputsV1 **inputs, int *fd, char path[MAXPGPATH])
{
	ClusterRecoveryCensus *census = reference_census_fixture(inputs, fd, path);
	cluster_recovery_census_destroy(census);
	census = NULL;
	UT_ASSERT_EQ(cluster_recovery_census_begin_images_v1(*inputs, *fd, 8 * 1024 * 1024,
		&census), CLUSTER_RECOVERY_CENSUS_MORE);
	return census;
}

UT_TEST(test_census_images_retain_all_physical_and_inverse_full_bytes)
{
	ClusterWalInputsV1 *inputs; int fd; char path[MAXPGPATH];
	ClusterRecoveryCensus *census = census_images_fixture(&inputs, &fd, path);
	ClusterRecoveryCensusEntry entry;
	ClusterRecoveryCensusImageV1 *expected = calloc(64, sizeof(*expected));
	ClusterRecoveryCensusResult result = CLUSTER_RECOVERY_CENSUS_MORE;
	const ClusterRecoveryCensusImagesV1 *view;
	uint32 count = 0, physical = 0, histories = 0;
	UT_ASSERT(expected != NULL);
	UT_ASSERT_NULL(cluster_recovery_census_images_v1(census));
	for (unsigned step = 0; expected != NULL && step < 200000; step++) {
		result = cluster_recovery_census_next(census, &entry);
		if (result >= CLUSTER_RECOVERY_CENSUS_DRAINED) break;
		if (result != CLUSTER_RECOVERY_CENSUS_ENTRY
			|| entry.kind != CLUSTER_RECOVERY_CENSUS_RELATION) continue;
		if (entry.value.relation.kind == CLUSTER_RECOVERY_RELATIONS_PAGE) {
			UT_ASSERT(count < 64);
			if (count == 64) break;
			expected[count].page_index = physical++;
			memcpy(expected[count++].bytes.data, entry.value.relation.page.page, BLCKSZ);
		} else if (entry.value.relation.kind == CLUSTER_RECOVERY_RELATIONS_REFERENCE) {
			const ClusterRecoveryPageReference *ref = &entry.value.relation.reference;
			UT_ASSERT(ref->page != NULL && count != 0);
			if (ref->page == NULL || count == 0) break;
			if (expected[count-1].version != ref->version) {
				UT_ASSERT(count < 64);
				if (count == 64) break;
				expected[count].page_index = physical-1;
				expected[count].version = ref->version;
				memcpy(expected[count++].bytes.data, ref->page, BLCKSZ);
				histories++;
			}
			UT_ASSERT(memcmp(expected[count-1].bytes.data, ref->page, BLCKSZ) == 0);
		}
	}
	UT_ASSERT_EQ(result, CLUSTER_RECOVERY_CENSUS_DRAINED);
	UT_ASSERT(physical != 0 && histories != 0);
	UT_ASSERT(census->relations == NULL); /* borrowed history buffers are gone */
	view = cluster_recovery_census_images_v1(census);
	UT_ASSERT_NOT_NULL(view);
	if (view != NULL && expected != NULL) {
		UT_ASSERT_EQ(view->count, count);
		UT_ASSERT(memcmp(view->entries, expected, count * sizeof(*expected)) == 0);
		UT_ASSERT_EQ(physical, census->page_count);
		UT_ASSERT(cluster_recovery_census_images_v1(census) == view);
	}
	free(expected); owned_census_done(census, &inputs, fd, path);
}

UT_TEST(test_census_images_old_mode_prefix_and_caller_locks_never_enter)
{
	ClusterWalInputsV1 *inputs; int fd; char path[MAXPGPATH];
	ClusterRecoveryCensus *census = reference_census_fixture(&inputs, &fd, path);
	UT_ASSERT_EQ(owned_census_until(census, 0), CLUSTER_RECOVERY_CENSUS_DRAINED);
	UT_ASSERT_NULL(cluster_recovery_census_images_v1(census));
	UT_ASSERT_EQ(census->status, CLUSTER_RECOVERY_CENSUS_DRAINED);
	owned_census_done(census, &inputs, fd, path);
	census = census_images_fixture(&inputs, &fd, path);
	UT_ASSERT_NULL(cluster_recovery_census_images_v1(census));
	UT_ASSERT_EQ(census->status, CLUSTER_RECOVERY_CENSUS_MORE);
	UT_ASSERT_EQ(owned_census_until(census, 0), CLUSTER_RECOVERY_CENSUS_DRAINED);
	for (unsigned mode = 0; mode < 3; mode++) {
		if (mode == 0) inventory_graph_caller_lock = true;
		if (mode == 1) CritSectionCount = 1;
		if (mode == 2) ShutdownRequestPending = true;
		UT_ASSERT_NULL(cluster_recovery_census_images_v1(census));
		inventory_graph_caller_lock = false; CritSectionCount = 0; ShutdownRequestPending = false;
		UT_ASSERT_EQ(census->status, CLUSTER_RECOVERY_CENSUS_DRAINED);
	}
	UT_ASSERT_NOT_NULL(cluster_recovery_census_images_v1(census));
	owned_census_done(census, &inputs, fd, path);
}

UT_TEST(test_census_images_changed_full_bytes_identity_or_source_revoke_whole_view)
{
	for (unsigned fault = 0; fault < 5; fault++) {
		ClusterWalInputsV1 *inputs; int fd; char path[MAXPGPATH];
		ClusterRecoveryCensus *census = census_images_fixture(&inputs, &fd, path);
		const ClusterRecoveryCensusImagesV1 *view;
		UT_ASSERT_EQ(owned_census_until(census, 0), CLUSTER_RECOVERY_CENSUS_DRAINED);
		view = cluster_recovery_census_images_v1(census); UT_ASSERT_NOT_NULL(view);
		if (view != NULL) {
			ClusterRecoveryCensusImagesV1 *changed = (ClusterRecoveryCensusImagesV1 *)view;
			if (fault == 0) changed->entries[0].bytes.data[BLCKSZ-1] ^= 1;
			if (fault == 1) changed->entries[0].version++;
			if (fault == 2) changed->entries[0].page_index++;
			if (fault == 3) changed->count--;
			if (fault == 4) inputs_pin_current = false;
			UT_ASSERT_NULL(cluster_recovery_census_images_v1(census));
			UT_ASSERT_EQ(census->status, CLUSTER_RECOVERY_CENSUS_REFUSED);
			UT_ASSERT(inputs_pin_held && inputs != NULL);
			inputs_pin_current = true;
			UT_ASSERT_NULL(cluster_recovery_census_images_v1(census));
		}
		owned_census_done(census, &inputs, fd, path);
	}
}

UT_TEST(test_census_images_capacity_failure_never_exports_a_prefix)
{
	ClusterWalInputsV1 *inputs; int fd; char path[MAXPGPATH];
	ClusterRecoveryCensus *census = census_images_fixture(&inputs, &fd, path);
	/* Keep the actual allocation; force its first full-version boundary. */
	census->image_capacity = 1;
	UT_ASSERT_EQ(owned_census_until(census, 0), CLUSTER_RECOVERY_CENSUS_CAPACITY);
	UT_ASSERT_NULL(cluster_recovery_census_images_v1(census));
	UT_ASSERT(inputs_pin_held && inputs != NULL);
	owned_census_sticky(census, CLUSTER_RECOVERY_CENSUS_CAPACITY);
	owned_census_done(census, &inputs, fd, path);
}

UT_TEST(test_census_images_manifest_error_retains_original_input_custody)
{
	ClusterWalInputsV1 *inputs; int fd; char path[MAXPGPATH];
	ClusterRecoveryCensus *census = census_images_fixture(&inputs, &fd, path);
	const ClusterRecoveryCensusImagesV1 *volatile view = NULL;
	volatile bool caught = false;
	UT_ASSERT_EQ(owned_census_until(census, 0), CLUSTER_RECOVERY_CENSUS_DRAINED);
	test_throw_root_read = true;
	PG_TRY(); { view = cluster_recovery_census_images_v1(census); }
	PG_CATCH(); { caught = true; } PG_END_TRY();
	test_throw_root_read = false;
	UT_ASSERT(caught); UT_ASSERT_NULL(view);
	UT_ASSERT_EQ(census->status, CLUSTER_RECOVERY_CENSUS_REFUSED);
	UT_ASSERT(inputs_pin_held && inputs != NULL);
	UT_ASSERT_NULL(cluster_recovery_census_images_v1(census));
	owned_census_done(census, &inputs, fd, path);
}
