/*-------------------------------------------------------------------------
 * test_cluster_recovery_managed_relmap_scan.c
 *   Registered managed pairs through the original control and ROOT readers.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *   src/test/cluster_unit/test_cluster_recovery_managed_relmap_scan.c
 * NOTES
 *   Control custody/configuration and crypto allocation leaves are controlled.
 *   Registry selection, codecs, namespace, ROOT reads and traversal are real.
 *-------------------------------------------------------------------------
 */
#define TEST_CONTROL_RELMAP_CONSUMER 1
#include "test_cluster_control_relmap_selection.c"
#include <dirent.h>
#include "cluster/cluster_recovery_relmap_scan.h"

void ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "%s:%d: %s\n", file, line, condition);
	abort();
}
static unsigned scan_reads, fault_read;
static int read_fault;
static ssize_t managed_scan_pread(int fd, void *bytes, size_t n, off_t at)
{
	ssize_t result = pread(fd, bytes, n, at);
	if (n == CLUSTER_RELMAP_FILE_SIZE_V2 && ++scan_reads == fault_read) {
		if (read_fault == 1 && result == n)
			((uint8 *)bytes)[n - 1] ^= 1;
		if (read_fault == 2) { errno = EIO; return -1; }
		if (read_fault == 3) cf_sequence++;
	}
	return result;
}
#define pread managed_scan_pread
#include "../../backend/cluster/cluster_recovery_root_set.c"
#undef pread
#include "test_cluster_recovery_relmap_table_unavailable.h"
#include "../../backend/cluster/cluster_recovery_relmap_scan.c"

static char physical_path[100];
static int physical_fd;
static ClusterRecoveryRootSet *physical_roots;
static ClusterControlRelmapSelectionV2 *physical_selection;
static ClusterRecoveryRelmapScan *physical_scan;
static void physical_write(const char *path, const void *bytes, size_t n)
{
	int fd = openat(physical_fd, path, O_CREAT | O_TRUNC | O_WRONLY, 0600);
	if (fd < 0 || write(fd, bytes, n) != n || close(fd)) abort();
}
static void physical_remove(int dir)
{
	DIR *stream = fdopendir(dup(dir));
	struct dirent *e;
	if (!stream) abort();
	while ((e = readdir(stream))) {
		struct stat st;
		int child;
		if (!strcmp(e->d_name, ".") || !strcmp(e->d_name, "..")) continue;
		if (fstatat(dir, e->d_name, &st, AT_SYMLINK_NOFOLLOW)) abort();
		if (S_ISDIR(st.st_mode)) {
			child = openat(dir, e->d_name, O_RDONLY | O_DIRECTORY);
			if (child < 0) abort();
			physical_remove(child); close(child);
			if (unlinkat(dir, e->d_name, AT_REMOVEDIR)) abort();
		} else if (unlinkat(dir, e->d_name, 0)) abort();
	}
	closedir(stream);
}
static RelMapFile managed_map(unsigned count, Oid filenumber)
{
	RelMapFile map = {0};
	map.magic = RELMAPPER_FILEMAGIC;
	map.num_mappings = count;
	for (unsigned i = 0; i < count; i++)
		map.mappings[i] = (RelMapping){100 + i, filenumber + i};
	INIT_CRC32C(map.crc);
	COMP_CRC32C(map.crc, &map, offsetof(RelMapFile, crc));
	FIN_CRC32C(map.crc);
	return map;
}
static ClusterRelmapV2Image managed_image(unsigned index, bool pending)
{
	ClusterRelmapV2Image image = {0};
	image.identity = (ClusterRelmapV2Namespace){11, 12, completed[index].database_oid, index == 0};
	image.committed_generation = 7;
	image.committed = managed_map(1, 400 + index);
	if (pending) {
		image.pending_generation = 8;
		image.pending_owner = (ClusterRelmapV2Owner){1, 0x200000020, 3, 4, 500, 6};
		image.pending = managed_map(1, 600 + index);
	}
	return image;
}
static void managed_pair_write(unsigned index, unsigned copy,
	const ClusterRecoveryFileIdentityV2 *id, const ClusterRelmapV2Image *image)
{
	uint8 bytes[CLUSTER_RELMAP_FILE_SIZE_V2];
	char name[160];
	if (!cluster_relmap_file_encode_v2(id, image, bytes, sizeof(bytes))) abort();
	if (index == 0) snprintf(name, sizeof(name), "global/pgrac_relmap_authority%s", copy ? ".bak" : "");
	else snprintf(name, sizeof(name), "base/%u/pgrac_relmap_authority%s", completed[index].database_oid, copy ? ".bak" : "");
	physical_write(name, bytes, sizeof(bytes));
}
static void managed_fixture(unsigned count, bool pending)
{
	inventory_begin(count);
	cf_sequence = 1;
	physical_roots = NULL; physical_selection = NULL; physical_scan = NULL;
	strcpy(physical_path, "/tmp/pgrac-managed-scan-XXXXXX");
	if (!mkdtemp(physical_path)) abort();
	physical_fd = open(physical_path, O_RDONLY | O_DIRECTORY);
	if (physical_fd < 0 || mkdirat(physical_fd, "global", 0700) || mkdirat(physical_fd, "base", 0700)) abort();
	for (unsigned i = 0; i < count; i++) {
		ClusterRelmapV2Image image = managed_image(i, pending);
		char dir[80];
		if (i) { snprintf(dir, sizeof(dir), "base/%u", completed[i].database_oid); if (mkdirat(physical_fd, dir, 0700)) abort(); }
		for (unsigned c = 0; c < 2; c++) managed_pair_write(i, c, &completed[i], &image);
	}
	fault_read = read_fault = scan_reads = 0;
}
static void managed_observe(void)
{
	ClusterRecoveryRootsEvent event;
	ClusterRecoveryRootsResult result;
	if (!inventory_read(&physical_selection)) abort();
	if (cluster_recovery_root_set_begin(physical_fd, 11, 12, reg.storage_uuid, 8*1024*1024, &physical_roots) != CLUSTER_RECOVERY_ROOTS_MORE) abort();
	for (unsigned i = 0; i < 50000; i++) {
		result = cluster_recovery_root_set_step(physical_roots, &event);
		if (result >= CLUSTER_RECOVERY_ROOTS_OBSERVED) break;
	}
	if (result != CLUSTER_RECOVERY_ROOTS_OBSERVED) abort();
	scan_reads = 0;
}
static ClusterRecoveryRelmapResult managed_begin(size_t budget)
{
#ifdef TEST_MANAGED_RELMAP_SCAN_PARENT
	return cluster_recovery_relmap_scan_begin(physical_roots, 11, 12, reg.storage_uuid, budget, &physical_scan);
#else
	return cluster_recovery_relmap_scan_begin_managed_v2(physical_roots, physical_selection, budget, &physical_scan);
#endif
}
static ClusterRecoveryRelmapResult managed_drain(unsigned *images, unsigned *mappings, unsigned *targets)
{
	ClusterRecoveryRelmapEntry out;
	for (unsigned i = 0; i < 200000; i++) {
		ClusterRecoveryRelmapResult result = cluster_recovery_relmap_scan_next(physical_scan, &out);
		if (result >= CLUSTER_RECOVERY_RELMAP_DRAINED) return result;
		if (result == CLUSTER_RECOVERY_RELMAP_ENTRY) {
#ifndef TEST_MANAGED_RELMAP_SCAN_PARENT
			ClusterRecoveryFileIdentityV2 selected;
			UT_ASSERT(out.file_identity != NULL && out.managed_image != NULL);
			UT_ASSERT(cluster_control_root_relmap_selection_lookup_v4(physical_selection,
				out.key.locator.dbOid, out.key.locator.spcOid, 0, &selected));
			UT_ASSERT(identity_same(out.file_identity, &selected));
			UT_ASSERT(out.image->num_mappings == (out.pending
				? out.managed_image->pending.num_mappings : out.managed_image->committed.num_mappings));
#endif
			if (out.kind == CLUSTER_RECOVERY_RELMAP_IMAGE) (*images)++;
			if (out.kind == CLUSTER_RECOVERY_RELMAP_MAPPING) (*mappings)++;
			if (out.kind == CLUSTER_RECOVERY_RELMAP_TARGET) (*targets)++;
		}
	}
	abort();
}
static void managed_end(void)
{
	cluster_recovery_relmap_scan_destroy(physical_scan);
	cluster_recovery_root_set_destroy(physical_roots);
	cluster_control_root_relmap_selection_destroy_v4(&physical_selection);
	physical_remove(physical_fd); close(physical_fd);
	if (rmdir(physical_path)) abort();
	selection_end();
}
UT_TEST(test_registered_pairs_keep_both_maps_and_all_forks)
{
	unsigned images = 0, mappings = 0, targets = 0;
	managed_fixture(2, true); managed_observe();
	UT_ASSERT_EQ(managed_begin(32768), CLUSTER_RECOVERY_RELMAP_MORE);
	UT_ASSERT_EQ(managed_drain(&images, &mappings, &targets), CLUSTER_RECOVERY_RELMAP_DRAINED);
	UT_ASSERT_EQ(images, 8); UT_ASSERT_EQ(mappings, 8); UT_ASSERT_EQ(targets, 32);
	UT_ASSERT(cf_held && fcntl(selection_dir, F_GETFD) >= 0);
	managed_end();
}
static void managed_sticky(ClusterRecoveryRelmapResult expected)
{
	ClusterRecoveryRelmapEntry out, zero = {0};
	ClusterRecoveryRelmapDiag diag;
	uint8 digest[32];
	unsigned before = scan_reads;
	memset(&out, 0xa5, sizeof(out));
	UT_ASSERT_EQ(cluster_recovery_relmap_scan_next(physical_scan, &out), expected);
	UT_ASSERT_EQ(memcmp(&out, &zero, sizeof(out)), 0);
	UT_ASSERT(!cluster_recovery_root_set_digest(physical_roots, digest));
	UT_ASSERT(!cluster_recovery_relmap_scan_diag(physical_scan, &diag));
	UT_ASSERT_EQ(scan_reads, before);
	UT_ASSERT(cf_held && fcntl(selection_dir, F_GETFD) >= 0);
}
static ClusterRecoveryRelmapResult managed_finish(void)
{
	unsigned images = 0, mappings = 0, targets = 0;
	return managed_drain(&images, &mappings, &targets);
}
static void managed_first_image(void)
{
	ClusterRecoveryRelmapEntry out;
	for (unsigned i = 0; i < 1000; i++) {
		ClusterRecoveryRelmapResult result = cluster_recovery_relmap_scan_next(physical_scan, &out);
		if (result == CLUSTER_RECOVERY_RELMAP_ENTRY) {
			UT_ASSERT_EQ(out.kind, CLUSTER_RECOVERY_RELMAP_IMAGE);
			return;
		}
		if (result > CLUSTER_RECOVERY_RELMAP_DRAINED) abort();
	}
	abort();
}
UT_TEST(test_whole_registration_exceeds_sixty_four_without_growing_scratch)
{
	unsigned images = 0, mappings = 0, targets = 0;
	managed_fixture(65, false); managed_observe();
	UT_ASSERT_EQ(managed_begin(32768), CLUSTER_RECOVERY_RELMAP_MORE);
	UT_ASSERT_EQ(managed_drain(&images, &mappings, &targets), CLUSTER_RECOVERY_RELMAP_DRAINED);
	UT_ASSERT_EQ(images, 130); UT_ASSERT_EQ(mappings, 130); UT_ASSERT_EQ(targets, 520);
	managed_end();
	managed_fixture(2, false);
	UT_ASSERT_EQ(unlinkat(physical_fd, "base/1001/pgrac_relmap_authority", 0), 0);
	UT_ASSERT_EQ(unlinkat(physical_fd, "base/1001/pgrac_relmap_authority.bak", 0), 0);
	managed_observe(); UT_ASSERT_EQ(managed_begin(32768), CLUSTER_RECOVERY_RELMAP_MORE);
	UT_ASSERT_EQ(managed_finish(), CLUSTER_RECOVERY_RELMAP_BASE_REQUIRED);
	managed_sticky(CLUSTER_RECOVERY_RELMAP_BASE_REQUIRED); managed_end();
}
UT_TEST(test_partial_publication_preserves_each_copy_and_empty_full_images)
{
	ClusterRelmapV2Image image;
	unsigned images = 0, mappings = 0, targets = 0;
	managed_fixture(1, true);
	image = managed_image(0, true);
	image.committed = image.pending; image.committed_generation = image.pending_generation;
	image.pending_generation = 0; memset(&image.pending_owner, 0, sizeof(image.pending_owner));
	memset(&image.pending, 0, sizeof(image.pending));
	managed_pair_write(0, 0, &completed[0], &image);
	managed_observe(); UT_ASSERT_EQ(managed_begin(32768), CLUSTER_RECOVERY_RELMAP_MORE);
	UT_ASSERT_EQ(managed_drain(&images, &mappings, &targets), CLUSTER_RECOVERY_RELMAP_DRAINED);
	UT_ASSERT_EQ(images, 3); UT_ASSERT_EQ(mappings, 3); UT_ASSERT_EQ(targets, 12);
	managed_end();
	managed_fixture(1, true); image = managed_image(0, true);
	image.committed = managed_map(0, 400); image.pending = managed_map(MAX_MAPPINGS, 600);
	for (unsigned c = 0; c < 2; c++) managed_pair_write(0, c, &completed[0], &image);
	managed_observe(); UT_ASSERT_EQ(managed_begin(32768), CLUSTER_RECOVERY_RELMAP_MORE);
	images = mappings = targets = 0;
	UT_ASSERT_EQ(managed_drain(&images, &mappings, &targets), CLUSTER_RECOVERY_RELMAP_DRAINED);
	UT_ASSERT_EQ(images, 4); UT_ASSERT_EQ(mappings, 2*MAX_MAPPINGS); UT_ASSERT_EQ(targets, 8*MAX_MAPPINGS);
	managed_end();
}
UT_TEST(test_file_cannot_supply_its_own_registration)
{
	for (unsigned axis = 0; axis < 9; axis++) {
		ClusterRecoveryFileIdentityV2 id;
		ClusterRelmapV2Image image;
		managed_fixture(1, false); id = completed[0]; image = managed_image(0, false);
		switch (axis) {
			case 0: id.creation_generation++; break;
			case 1: id.registration_generation++; break;
			case 2: id.redo_branch_uuid[0] ^= 1; break;
			case 3: id.redo_branch_generation++; break;
			case 4: id.redo_timeline++; break;
			case 5: id.origin = 1; break;
			case 6: id.system_identifier++; image.identity.system_identifier++; break;
			case 7: id.database_incarnation++; image.identity.database_incarnation++; break;
			case 8: id.storage_uuid[0] ^= 1; break;
		}
		for (unsigned c = 0; c < 2; c++) managed_pair_write(0, c, &id, &image);
		managed_observe(); UT_ASSERT_EQ(managed_begin(32768), CLUSTER_RECOVERY_RELMAP_MORE);
		UT_ASSERT_EQ(managed_finish(), CLUSTER_RECOVERY_RELMAP_CORRUPT);
		managed_sticky(CLUSTER_RECOVERY_RELMAP_CORRUPT); managed_end();
	}
}
UT_TEST(test_missing_extra_raw_truncated_intent_and_conflicting_copies_refuse)
{
	for (unsigned variant = 0; variant < 7; variant++) {
		ClusterRelmapV2Image image;
		ClusterRecoveryRelmapResult result;
		uint8 bytes[CLUSTER_RELMAP_FILE_SIZE_V2];
		managed_fixture(1, true); image = managed_image(0, true);
		switch (variant) {
			case 0: UT_ASSERT_EQ(unlinkat(physical_fd, "global/pgrac_relmap_authority.bak", 0), 0); break;
			case 1:
				UT_ASSERT_EQ(mkdirat(physical_fd, "base/9000", 0700), 0);
				UT_ASSERT(cluster_relmap_file_encode_v2(&completed[0], &image, bytes, sizeof(bytes)));
				physical_write("base/9000/pgrac_relmap_authority", bytes, sizeof(bytes)); break;
			case 2: physical_write("global/pg_filenode.map", &image.committed, sizeof(RelMapFile)); break;
			case 3: physical_write("global/pgrac_relmap_authority", "short", 5); break;
			case 4:
				image.pending_owner.wal_end = 0;
				UT_ASSERT(cluster_relmap_wal_encode_intent_v2(&completed[0], &image, bytes, sizeof(bytes)));
				physical_write("global/pgrac_relmap_authority", bytes, sizeof(bytes)); break;
			case 5:
				image.pending = managed_map(1, 999);
				managed_pair_write(0, 1, &completed[0], &image); break;
			case 6: UT_ASSERT_EQ(unlinkat(physical_fd, "global/pgrac_relmap_authority", 0), 0); break;
		}
		managed_observe(); UT_ASSERT_EQ(managed_begin(32768), CLUSTER_RECOVERY_RELMAP_MORE);
		result = managed_finish(); UT_ASSERT(result > CLUSTER_RECOVERY_RELMAP_DRAINED);
		managed_sticky(result); managed_end();
	}
}
UT_TEST(test_original_control_custody_and_final_whole_registry_remain_required)
{
	for (unsigned variant = 0; variant < 4; variant++) {
		managed_fixture(1, false); managed_observe();
		UT_ASSERT_EQ(managed_begin(32768), CLUSTER_RECOVERY_RELMAP_MORE);
		managed_first_image();
		if (variant == 0) cf_sequence++;
		if (variant == 1) config_ok = false;
		if (variant == 2) { inventory[0].image_digest[0] ^= 1; inventory_write(); }
		if (variant == 3) {
			/* The last reread is not permission to omit final control validation. */
			while (physical_scan->diag.rereads < 2) {
				ClusterRecoveryRelmapEntry out;
				UT_ASSERT(cluster_recovery_relmap_scan_next(physical_scan, &out) <= CLUSTER_RECOVERY_RELMAP_ENTRY);
			}
			config_ok = false;
		}
		UT_ASSERT_EQ(managed_finish(), CLUSTER_RECOVERY_RELMAP_CHANGED);
		managed_sticky(CLUSTER_RECOVERY_RELMAP_CHANGED); managed_end();
	}
}
UT_TEST(test_whole_file_reread_and_io_bracket_fail_sticky)
{
	for (unsigned variant = 1; variant <= 3; variant++) {
		ClusterRecoveryRelmapResult expected = variant == 2 ? CLUSTER_RECOVERY_RELMAP_IO : CLUSTER_RECOVERY_RELMAP_CHANGED;
		managed_fixture(1, false); managed_observe();
		UT_ASSERT_EQ(managed_begin(32768), CLUSTER_RECOVERY_RELMAP_MORE);
		fault_read = variant == 1 ? 2 : 1; read_fault = variant;
		UT_ASSERT_EQ(managed_finish(), expected);
		managed_sticky(expected); managed_end();
	}
}
UT_TEST(test_first_control_error_revokes_only_local_observation)
{
	volatile bool caught = false;
	ClusterRecoveryRelmapEntry out, zero = {0};
	managed_fixture(1, false); managed_observe();
	UT_ASSERT_EQ(managed_begin(32768), CLUSTER_RECOVERY_RELMAP_MORE);
	config_error = true;
	PG_TRY(); { managed_finish(); }
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	UT_ASSERT(caught);
	memset(&out, 0xa5, sizeof(out));
	UT_ASSERT_EQ(cluster_recovery_relmap_scan_next(physical_scan, &out), CLUSTER_RECOVERY_RELMAP_CHANGED);
	UT_ASSERT_EQ(memcmp(&out, &zero, sizeof(out)), 0);
	managed_sticky(CLUSTER_RECOVERY_RELMAP_CHANGED); managed_end();
	managed_fixture(1, false); managed_observe(); config_error = true; caught = false;
	PG_TRY(); { managed_begin(32768); }
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	UT_ASSERT(caught && physical_scan == NULL);
	UT_ASSERT(cf_held && fcntl(selection_dir, F_GETFD) >= 0);
	managed_end();
}
#ifndef TEST_MANAGED_RELMAP_SCAN_PARENT
UT_TEST(test_budget_fresh_output_and_owned_output_aliases)
{
	managed_fixture(1, false); managed_observe();
	UT_ASSERT_EQ(managed_begin(1), CLUSTER_RECOVERY_RELMAP_CAPACITY);
	UT_ASSERT(physical_scan == NULL && scan_reads == 0); managed_end();
	for (unsigned variant = 0; variant < 3; variant++) {
		ClusterRecoveryRelmapEntry *alias;
		managed_fixture(1, false); managed_observe();
		UT_ASSERT_EQ(managed_begin(32768), CLUSTER_RECOVERY_RELMAP_MORE);
		managed_first_image();
		alias = variant == 0 ? (void *)physical_scan : variant == 1
			? (void *)&physical_scan->managed->image : (void *)&physical_scan->managed->identity;
		UT_ASSERT_EQ(cluster_recovery_relmap_scan_next(physical_scan, alias), CLUSTER_RECOVERY_RELMAP_INVALID);
		managed_sticky(CLUSTER_RECOVERY_RELMAP_INVALID); managed_end();
	}
	managed_fixture(1, false); managed_observe();
	UT_ASSERT_EQ(managed_begin(32768), CLUSTER_RECOVERY_RELMAP_MORE);
	{
		ClusterRecoveryRelmapScan *before = physical_scan;
		UT_ASSERT_EQ(managed_begin(32768), CLUSTER_RECOVERY_RELMAP_INVALID);
		UT_ASSERT(physical_scan == before);
	}
	managed_end();
}
#endif
UT_TEST(test_diagnostic_alias_and_lost_control_cannot_preserve_observation)
{
	ClusterRecoveryRelmapDiag diag;
	managed_fixture(1, false); managed_observe();
	UT_ASSERT_EQ(managed_begin(32768), CLUSTER_RECOVERY_RELMAP_MORE);
	cf_sequence++;
	UT_ASSERT(!cluster_recovery_relmap_scan_diag(physical_scan, &diag));
	managed_sticky(CLUSTER_RECOVERY_RELMAP_CHANGED); managed_end();
	for (unsigned variant = 0; variant < 2; variant++) {
		managed_fixture(1, false); managed_observe();
		UT_ASSERT_EQ(managed_begin(32768), CLUSTER_RECOVERY_RELMAP_MORE);
		UT_ASSERT(!cluster_recovery_relmap_scan_diag(physical_scan, variant
			? (void *)&physical_scan->managed->image : (void *)&physical_scan->image));
		managed_sticky(CLUSTER_RECOVERY_RELMAP_CHANGED); managed_end();
	}
}
UT_TEST(test_borrowed_root_and_control_outputs_refuse_before_write)
{
	for (unsigned variant = 0; variant < 4; variant++) {
		void *alias;
		uint8 saved[sizeof(ClusterRecoveryRelmapEntry)];
		ClusterRecoveryRelmapResult result;
		managed_fixture(1, false); managed_observe();
		UT_ASSERT_EQ(managed_begin(32768), CLUSTER_RECOVERY_RELMAP_MORE);
		alias = variant & 1 ? (void *)((uint8 *)physical_selection + 1)
			: (void *)cluster_recovery_root_set_entry(physical_roots, 0);
		memcpy(saved, alias, sizeof(saved));
		if (variant < 2) {
			result = cluster_recovery_relmap_scan_next(physical_scan, alias);
			UT_ASSERT_EQ(result, CLUSTER_RECOVERY_RELMAP_INVALID);
		} else
			UT_ASSERT(!cluster_recovery_relmap_scan_diag(physical_scan, alias));
		UT_ASSERT_EQ(memcmp(saved, alias, sizeof(saved)), 0);
		/* Restore only test-owned snapshots if the parent wrote borrowed data,
		 * so a failing negative test does not lose the original FD on cleanup. */
		memcpy(alias, saved, sizeof(saved));
		UT_ASSERT(cf_held && fcntl(selection_dir, F_GETFD) >= 0);
		managed_end();
	}
	managed_fixture(1, false); managed_observe();
	{
		ClusterRecoveryRelmapScan **alias = (void *)(physical_roots->bytes + BLCKSZ - 64);
		ClusterRecoveryRelmapResult result;
		memset(alias, 0, sizeof(*alias));
		result = cluster_recovery_relmap_scan_begin_managed_v2(physical_roots,
			physical_selection, 32768, alias);
		UT_ASSERT_EQ(result, CLUSTER_RECOVERY_RELMAP_INVALID);
		UT_ASSERT(*alias == NULL && scan_reads == 0);
		/* Parent may publish into ROOT scratch; consume only that local leak. */
		if (*alias != NULL) cluster_recovery_relmap_scan_destroy(*alias);
		memset(alias, 0, sizeof(*alias));
	}
	managed_end();
}
UT_TEST(test_raw_entry_cannot_silently_drain_managed_files)
{
	managed_fixture(1, false); managed_observe();
	UT_ASSERT_EQ(cluster_recovery_relmap_scan_begin(physical_roots, 11, 12, reg.storage_uuid,
		32768, &physical_scan), CLUSTER_RECOVERY_RELMAP_MORE);
	UT_ASSERT_EQ(managed_finish(), CLUSTER_RECOVERY_RELMAP_BASE_REQUIRED);
	managed_sticky(CLUSTER_RECOVERY_RELMAP_BASE_REQUIRED); managed_end();
}
int main(void)
{
	/* Keep the original fixture cases referenced, without executing them here. */
	(void)&test_original_codec_v4_extent_and_exact_reference;
	(void)&test_v4_refuses_forged_reference_and_record;
	(void)&test_selected_root_registry_real_files_and_late_change;
	(void)&test_control_selects_complete_original_file_identity;
	(void)&test_control_selection_refuses_missing_custody_and_dependency;
	(void)&test_control_selection_late_root_and_error_preserve_output;
	(void)&test_control_selection_valid_outer_hash_cannot_hide_noncanonical_source;
	(void)&test_control_selection_key_and_named_directory_remain_original;
	(void)&test_control_selection_ambiguous_births_refuse_without_latest_rule;
	(void)&test_complete_decoded_vector_exceeds_active_pin_capacity;
	(void)&test_complete_inventory_origin_lookup_and_sources;
	(void)&test_inventory_requires_original_custody_and_fresh_output;
	(void)&test_inventory_rejects_ambiguous_birth_origin_and_incomplete_copy;
	(void)&test_inventory_requires_complete_configured_source_set_and_shared_address;
	(void)&test_complete_receipt_membership_refuses_partial_extra_and_wrong_identity;
	(void)&test_post_io_root_registry_error_and_cf_reacquire_cannot_revive;
	(void)&test_post_io_named_directory_remains_original;
	(void)&test_actual_pair_sync_readback_then_complete_control_recheck;
	(void)&test_copied_checkpoint_reference_does_not_revive_cf_authority;
	UT_PLAN(12);
	UT_RUN(test_registered_pairs_keep_both_maps_and_all_forks);
	UT_RUN(test_whole_registration_exceeds_sixty_four_without_growing_scratch);
	UT_RUN(test_partial_publication_preserves_each_copy_and_empty_full_images);
	UT_RUN(test_file_cannot_supply_its_own_registration);
	UT_RUN(test_missing_extra_raw_truncated_intent_and_conflicting_copies_refuse);
	UT_RUN(test_original_control_custody_and_final_whole_registry_remain_required);
	UT_RUN(test_whole_file_reread_and_io_bracket_fail_sticky);
	UT_RUN(test_first_control_error_revokes_only_local_observation);
#ifndef TEST_MANAGED_RELMAP_SCAN_PARENT
	UT_RUN(test_budget_fresh_output_and_owned_output_aliases);
#endif
	UT_RUN(test_diagnostic_alias_and_lost_control_cannot_preserve_observation);
	UT_RUN(test_borrowed_root_and_control_outputs_refuse_before_write);
	UT_RUN(test_raw_entry_cannot_silently_drain_managed_files);
	UT_DONE();
	return ut_failed_count != 0;
}
