/*-------------------------------------------------------------------------
 *
 * test_cluster_control_debt_prepared.inc
 *     PREPARED candidate bytes preserve the original control selection.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_control_debt_prepared.inc
 *
 * NOTES
 *     Original test-only helpers are private to the selected-debt fixture.
 *     Spec: 2026-11-01-pre2-e1-format-closure-v1.2-path-c.md
 *-------------------------------------------------------------------------
 */
static uint8 prepared_before[133120], prepared_after[133120];

static void prepared_begin(uint64 count)
{
	store_begin(count);
	if (!cluster_control_root_reservations_encode_v4(&root, &debt_registry,
		debt_original_ref, prepared_before, 66560)) abort();
	memcpy(prepared_before + 66560, prepared_before, 66560);
	control_debt_wire_destroy_v1(&image);
	debt_batches[0].prepare = 15;
	if (!control_debt_wire_encode_v1(debt_original, debt_original_bytes,
		debt_original_ref, 15, 17, debt_batches, 1, DEBT_TEST_BUDGET, &image)) abort();
	memset(prepared_after, 0xa5, sizeof(prepared_after));
}
static bool prepared_candidate(Size budget, uint8 *out)
{
	return control_debt_root_candidate_v4(prepared_before, sizeof(prepared_before),
		image, debt_batches, 1, budget, out, sizeof(prepared_after));
}
UT_TEST(prepared_candidate_keeps_entire_original_pair_and_coverage)
{
	for (unsigned v4 = 0; v4 < 2; ++v4) {
		ControlRootImage *decoded = calloc(1, sizeof(*decoded));
		uint8 before[133120];
		debt_fixture_v4 = v4;
		prepared_begin(130);
		memcpy(before, prepared_before, sizeof(before));
		UT_ASSERT(prepared_candidate(DEBT_TEST_BUDGET, prepared_after));
		UT_ASSERT(!memcmp(before, prepared_before, sizeof(before)));
		UT_ASSERT(!memcmp(prepared_after + 66560, before, 66560));
		UT_ASSERT_EQ(prepared_after[16], 15);
		UT_ASSERT(!memcmp(prepared_after + 512, image->reference, 128));
		/* Independently enumerate the only allowed changed byte ranges. */
		for (Size i = 0; i < 66560; ++i)
			if (!(i >= 16 && i < 24) && !(i >= 512 && i < 640) && i < 1020)
				UT_ASSERT_EQ(prepared_after[i], before[i]);
		UT_ASSERT(!memcmp(prepared_after + 1024, before + 1024, 65536));
		UT_ASSERT(cluster_control_root_registry_decode_v4(prepared_after, 66560,
			reg.storage_uuid, reg.system_identifier, decoded));
		UT_ASSERT_EQ(decoded->header.file_txn_seq, 15);
		UT_ASSERT(!cluster_control_root_registry_matches_v4(prepared_after, 66560, &reg));
		UT_ASSERT(!memcmp(image->registry + 256, debt_original, debt_original_bytes));
		free(decoded); store_end();
	}
	debt_fixture_v4 = false;
}
UT_TEST(prepared_candidate_reloads_only_after_test_owned_install)
{
	ControlDebtStoreAttemptV1 attempt = {0};
	ControlDebtWireImageV1 *loaded = NULL;
	uint8 ref[128];
	prepared_begin(65);
	UT_ASSERT(prepared_candidate(DEBT_TEST_BUDGET, prepared_after));
	UT_ASSERT(store_create(&attempt, ref));
	/* This fixture alone writes ROOT. The pure candidate builder never does. */
	write_root(store_global, "pgrac_control_root", prepared_after);
	write_root(store_global, "pgrac_control_root.bak", prepared_after + 66560);
	UT_ASSERT(control_debt_store_load_selected_v4(store_global, prepared_after,
		sizeof(prepared_after), reg.storage_uuid, reg.system_identifier,
		DEBT_TEST_BUDGET, &loaded));
	UT_ASSERT(loaded && loaded->page_bytes == image->page_bytes);
	if (loaded) UT_ASSERT(!memcmp(loaded->pages, image->pages, image->page_bytes));
	UT_ASSERT(!memcmp(prepared_before, prepared_after + 66560, 66560));
	control_debt_wire_destroy_v1(&loaded); selected_end();
}
UT_TEST(prepared_candidate_refuses_wrong_basis_and_nonadjacent_generation)
{
	uint8 saved[133120];
	for (unsigned fault = 0; fault < 3; ++fault) {
		prepared_begin(1);
		memcpy(saved, prepared_after, sizeof(saved));
		if (fault == 0) {
			/* Valid same-generation ROOT selecting a different original registry. */
			debt_reserved[0].batch_uuid[0] ^= 1;
			Size n; uint8 ref[128];
			UT_ASSERT(cluster_recovery_registry_size_v3(&debt_registry, DEBT_TEST_BUDGET, &n));
			uint8 *bytes = malloc(n);
			UT_ASSERT(cluster_recovery_registry_encode_v3(&debt_registry, bytes, n, ref));
			UT_ASSERT(cluster_control_root_reservations_encode_v4(&root, &debt_registry,
				ref, prepared_before, 66560));
			memcpy(prepared_before + 66560, prepared_before, 66560);
			free(bytes);
		} else {
			control_debt_wire_destroy_v1(&image);
			UT_ASSERT(control_debt_wire_encode_v1(debt_original, debt_original_bytes,
				debt_original_ref, fault == 1 ? 16 : PG_UINT64_MAX - 1, 17,
				debt_batches, 1, DEBT_TEST_BUDGET, &image));
		}
		UT_ASSERT(!prepared_candidate(DEBT_TEST_BUDGET, prepared_after));
		UT_ASSERT(!memcmp(saved, prepared_after, sizeof(saved)));
		store_end();
	}
}
UT_TEST(prepared_candidate_refuses_bad_backup_and_debt_pages)
{
	uint8 saved[133120];
	for (unsigned fault = 0; fault < 3; ++fault) {
		prepared_begin(65);
		memcpy(saved, prepared_after, sizeof(saved));
		if (fault == 0) prepared_before[66560 + 1024 + 100] ^= 1;
		if (fault == 1) {
			/* Well-formed backup from another configured cohort is not the cut. */
			prepared_before[66560 + 100] ^= 1;
			fixture_put_le(prepared_before + 66560, 1020,
				fixture_crc(prepared_before + 66560, 1020), 4);
		}
		if (fault == 2) image->pages[33024 + 400] ^= 1;
		UT_ASSERT(!prepared_candidate(DEBT_TEST_BUDGET, prepared_after));
		UT_ASSERT(!memcmp(saved, prepared_after, sizeof(saved)));
		store_end();
	}
}
UT_TEST(prepared_candidate_aliases_and_budget_preserve_all_inputs)
{
	uint8 saved[133120], ref[128];
	prepared_begin(65);
	memcpy(saved, prepared_before, sizeof(saved));
	memcpy(ref, image->reference, sizeof(ref));
	UT_ASSERT(!prepared_candidate(DEBT_TEST_BUDGET, prepared_before));
	UT_ASSERT(!memcmp(saved, prepared_before, sizeof(saved)));
	UT_ASSERT(!prepared_candidate(DEBT_TEST_BUDGET, (uint8 *)image));
	UT_ASSERT(!prepared_candidate(DEBT_TEST_BUDGET, image->pages));
	UT_ASSERT(!prepared_candidate(DEBT_TEST_BUDGET, debt_original));
	UT_ASSERT(!memcmp(ref, image->reference, sizeof(ref)));
	memcpy(saved, prepared_after, sizeof(saved));
	UT_ASSERT(!prepared_candidate(1024, prepared_after));
	UT_ASSERT(!memcmp(saved, prepared_after, sizeof(saved)));
	store_end();
}
UT_TEST(prepared_candidate_does_not_treat_selected_debt_as_empty)
{
	uint8 saved[133120];
	prepared_begin(1);
	UT_ASSERT(prepared_candidate(DEBT_TEST_BUDGET, prepared_after));
	memcpy(prepared_before, prepared_after, sizeof(prepared_before));
	memcpy(saved, prepared_after, sizeof(saved));
	UT_ASSERT(!prepared_candidate(DEBT_TEST_BUDGET, prepared_after));
	UT_ASSERT(!memcmp(saved, prepared_after, sizeof(saved)));
	store_end();
}
