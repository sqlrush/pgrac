/*-------------------------------------------------------------------------
 * test_cluster_control_debt_selected.c
 *     Complete debt bytes selected by the original physical ROOT pair.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_control_debt_selected.c
 *-------------------------------------------------------------------------
 */
#define DEBT_WIRE_ROOT_FIXTURE 1
#define DEBT_STORE_TEST_MAIN debt_selected_old_store_main
static void selected_read_hook(void);
#define CONTROL_DEBT_STORE_TEST_READ_HOOK selected_read_hook
#include "test_cluster_control_debt_store.c"

static uint8 selected_pair[133120];
static uint64 selected_cf_sequence = 1;
static unsigned selected_hook_kind;
uint64 cluster_cf_attempt_sequence(void) { return selected_cf_sequence; }

static void selected_write_pair(void)
{
	write_root(store_global, "pgrac_control_root", selected_pair);
	write_root(store_global, "pgrac_control_root.bak", selected_pair + 66560);
}
static void selected_read_hook(void)
{
	unsigned kind = selected_hook_kind;
	selected_hook_kind = 0;
	if (kind == 1 || kind == 4) {
		uint8 *backup = malloc(66560);
		if (!backup) abort();
		memcpy(backup, selected_pair, 66560);
		write_root(store_global, "pgrac_control_root.bak", backup);
		if (kind == 4) memcpy(selected_pair + 66560, backup, 66560);
		free(backup);
	} else if (kind == 2) cf_held = false;
	else if (kind == 3) selected_cf_sequence++;
}
static void selected_begin(void)
{
	ControlDebtStoreAttemptV1 attempt = {0};
	uint8 ref[128], *bytes;
	Size n;
	cf_held = true; selected_cf_sequence = 1; selected_hook_kind = 0;
	store_begin(65);
	if (!store_create(&attempt, ref) || !cluster_recovery_registry_size_v2(&reg,
		DEBT_TEST_BUDGET, &n)) abort();
	bytes = malloc(n);
	if (!bytes || !cluster_recovery_registry_encode_v2(&reg, bytes, n, ref)
		|| !cluster_control_root_registry_encode_v4(&root, &reg, ref, selected_pair, 66560)) abort();
	free(bytes);
	/* Exact independent slot/CRC construction. The backup may retain the old
	 * normal selection; debt is loaded only from the primary's own reference. */
	memcpy(selected_pair + 66560, selected_pair, 66560);
	fixture_put_le(selected_pair, 16, 16, 8);
	memcpy(selected_pair + 512, image->reference, 128);
	fixture_put_le(selected_pair, 1020, fixture_crc(selected_pair, 1020), 4);
	selected_write_pair();
}
static void selected_end(void)
{
	cf_held = true;
	if (unlinkat(store_global, "pgrac_control_root", 0)
		|| unlinkat(store_global, "pgrac_control_root.bak", 0)) abort();
	store_end();
}
static bool selected_load(ControlDebtWireImageV1 **out)
{
	return control_debt_store_load_selected_v4(store_global, selected_pair, sizeof(selected_pair),
		reg.storage_uuid, reg.system_identifier, DEBT_TEST_BUDGET, out);
}
UT_TEST(selected_debt_root_decodes_without_available_admission)
{
	ControlRootImage *metadata = calloc(1, sizeof(*metadata));
	selected_begin();
	UT_ASSERT(cluster_control_root_registry_decode_v4(selected_pair, 66560,
		reg.storage_uuid, reg.system_identifier, metadata));
	UT_ASSERT_EQ(metadata->header.format_version, 4);
	UT_ASSERT_EQ(metadata->header.file_txn_seq, 16);
	UT_ASSERT(!cluster_recovery_registry_reference_valid_v2(selected_pair + 512));
	UT_ASSERT(!cluster_recovery_registry_reference_valid_v3(selected_pair + 512, CATALOG_VERSION_NO));
	UT_ASSERT(!cluster_control_root_registry_matches_v4(selected_pair, 66560, &reg));
	free(metadata); selected_end();
}
UT_TEST(selected_whole_debt_reload_without_input_vectors)
{
	for (unsigned v4 = 0; v4 < 2; v4++) {
		ControlDebtWireImageV1 *loaded = NULL;
		ClusterRecoveryRegistryDecodedV3 *old = NULL;
		debt_fixture_v4 = v4; selected_begin();
		memset(debt_entries, 0, sizeof(debt_entries));
		memset(debt_batches, 0, sizeof(debt_batches));
		UT_ASSERT(selected_load(&loaded));
		if (loaded) {
			UT_ASSERT(!memcmp(loaded->registry, image->registry, image->registry_bytes));
			UT_ASSERT(!memcmp(loaded->pages, image->pages, image->page_bytes));
			UT_ASSERT(debt_old_open(loaded->registry + 256, debt_original_bytes,
				debt_original_ref, DEBT_TEST_BUDGET, &old));
			if (old) {
				const ClusterRecoveryRegistryV3 *r = cluster_recovery_registry_view_v3(old);
				UT_ASSERT_EQ(r->available.coverage_format_version, v4 ? 4 : 2);
				UT_ASSERT_EQ(r->reservation_count, 2);
				UT_ASSERT(!memcmp(r->available.coverage.files[0].creation_batch_uuid,
					files[0].creation_batch_uuid, 16));
			}
		}
		cluster_recovery_registry_destroy_v3(&old);
		control_debt_wire_destroy_v1(&loaded); selected_end();
	}
	debt_fixture_v4 = false;
}
UT_TEST(selected_shape_and_root_generation_cannot_be_resealed_into_admission)
{
	static const Size offsets[] = {0, 4, 8, 12, 16, 24, 32, 40, 48, 56, 64, 96};
	for (unsigned i = 0; i < lengthof(offsets); i++) {
		ControlRootImage *metadata = malloc(sizeof(*metadata));
		ControlDebtWireImageV1 *loaded = NULL;
		uint8 *ref;
		selected_begin(); ref = selected_pair + 512;
		if (offsets[i] == 16) debt_put(ref, 16, 17, 8);
		else if (offsets[i] == 64) memset(ref + 64, 0, 32);
		else ref[offsets[i]] ^= 0x40;
		debt_put(ref, 124, debt_crc(ref, 124), 4);
		fixture_put_le(selected_pair, 1020, fixture_crc(selected_pair, 1020), 4);
		UT_ASSERT(!cluster_control_root_registry_decode_v4(selected_pair, 66560,
			reg.storage_uuid, reg.system_identifier, metadata));
		selected_write_pair(); UT_ASSERT(!selected_load(&loaded));
		UT_ASSERT(loaded == NULL); free(metadata); selected_end();
	}
}
UT_TEST(selected_root_or_cf_change_and_mutable_baseline_refuse)
{
	for (unsigned kind = 1; kind <= 4; kind++) {
		ControlDebtWireImageV1 *loaded = NULL;
		selected_begin(); selected_hook_kind = kind;
		UT_ASSERT(!selected_load(&loaded));
		UT_ASSERT(selected_hook_kind == 0 && loaded == NULL);
		control_debt_wire_destroy_v1(&loaded);
		selected_end();
	}
}
UT_TEST(selected_missing_whole_page_or_basis_has_no_backup_fallback)
{
	for (unsigned kind = 0; kind < 3; kind++) {
		ControlDebtWireImageV1 *loaded = NULL;
		char name[96];
		int dir;
		selected_begin();
		dir = openat(store_global, "pgrac_recovery_registry", O_RDONLY | O_DIRECTORY);
		if (!kind) debt_store_name(image->registry + debt_get(image->registry, 136, 8) + 128 + 64,
			false, name);
		else if (kind == 1) debt_store_name(image->reference + 64, true, name);
		else snprintf(name, sizeof(name), "%016llx-%016llx-%016llx.v3", 15LL, 16LL, 17LL);
		UT_ASSERT(unlinkat(dir, name, 0) == 0); close(dir);
		UT_ASSERT(!selected_load(&loaded)); UT_ASSERT(loaded == NULL);
		selected_end();
	}
}
UT_TEST(selected_alias_budget_and_original_lock_refuse_before_debt_reads)
{
	ControlDebtWireImageV1 *loaded = NULL;
	uint8 before[128];
	selected_begin(); reset_faults(); memcpy(before, selected_pair + 512, 128);
	UT_ASSERT(!control_debt_store_load_selected_v4(store_global, selected_pair, sizeof(selected_pair),
		reg.storage_uuid, 11, DEBT_TEST_BUDGET, (ControlDebtWireImageV1 **)(selected_pair + 512)));
	UT_ASSERT(!control_debt_store_load_selected_v4(store_global, selected_pair, sizeof(selected_pair),
		reg.storage_uuid, 11, 1024, &loaded));
	UT_ASSERT_EQ(reads, 0); UT_ASSERT(!memcmp(before, selected_pair + 512, 128));
	cf_held = false; UT_ASSERT(!selected_load(&loaded)); UT_ASSERT_EQ(reads, 0);
	cf_held = true; selected_cf_sequence = 0;
	UT_ASSERT(!selected_load(&loaded)); UT_ASSERT_EQ(reads, 0);
	UT_ASSERT(loaded == NULL); selected_end();
}
UT_TEST(selected_backup_pair_rules_and_missing_primary_remain_exact)
{
	for (unsigned kind = 0; kind < 4; kind++) {
		ControlDebtWireImageV1 *loaded = NULL;
		selected_begin();
		if (!kind) { /* Identical current pair is allowed, as is the old backup above. */
			memcpy(selected_pair + 66560, selected_pair, 66560); selected_write_pair();
			UT_ASSERT(selected_load(&loaded));
		} else {
			if (kind == 1) {
				memcpy(selected_pair + 66560, selected_pair, 66560);
				selected_pair[66560 + 512 + 64] ^= 1;
				debt_put(selected_pair + 66560 + 512, 124,
					debt_crc(selected_pair + 66560 + 512, 124), 4);
				fixture_put_le(selected_pair + 66560, 1020,
					fixture_crc(selected_pair + 66560, 1020), 4);
				selected_write_pair();
			} else if (kind == 2) {
				UT_ASSERT(unlinkat(store_global, "pgrac_control_root.bak", 0) == 0);
				UT_ASSERT(linkat(store_global, "pgrac_control_root", store_global,
					"pgrac_control_root.bak", 0) == 0);
			} else {
				UT_ASSERT(unlinkat(store_global, "pgrac_control_root", 0) == 0);
			}
			UT_ASSERT(!selected_load(&loaded)); UT_ASSERT(loaded == NULL);
			if (kind == 3) write_root(store_global, "pgrac_control_root", selected_pair);
		}
		control_debt_wire_destroy_v1(&loaded); selected_end();
	}
}
int main(void)
{
	UT_PLAN(10);
	UT_RUN(test_original_codec_v4_extent_and_exact_reference);
	UT_RUN(test_v4_refuses_forged_reference_and_record);
	UT_RUN(test_selected_root_registry_real_files_and_late_change);
	UT_RUN(selected_debt_root_decodes_without_available_admission);
	UT_RUN(selected_whole_debt_reload_without_input_vectors);
	UT_RUN(selected_shape_and_root_generation_cannot_be_resealed_into_admission);
	UT_RUN(selected_root_or_cf_change_and_mutable_baseline_refuse);
	UT_RUN(selected_missing_whole_page_or_basis_has_no_backup_fallback);
	UT_RUN(selected_alias_budget_and_original_lock_refuse_before_debt_reads);
	UT_RUN(selected_backup_pair_rules_and_missing_primary_remain_exact);
	UT_DONE(); return ut_failed_count != 0;
}
