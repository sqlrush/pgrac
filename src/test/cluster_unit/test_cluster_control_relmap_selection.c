/* Actual complete ROOT/registry selection and pair file I/O. The original
 * common/config/storage/CF presence leaves remain controlled, not admission. */
#define TEST_CONTROL_RELMAP_SELECTION 1
#include "catalog/pg_tablespace_d.h"
#include "common/cluster_relmap_file.h"
#include "test_cluster_control_file_selection.c"
static uint64 cf_sequence = 1;
uint64 cluster_cf_attempt_sequence(void);
uint64 cluster_cf_attempt_sequence(void)
{
	return cf_sequence;
}
static ClusterRecoveryCheckpointFileV2 inventory[146];
static ClusterRecoveryFileIdentityV2 completed[73];
static const char *registry_name =
	"pgrac_recovery_registry/000000000000000f-0000000000000010-0000000000000011.v2";
static void inventory_write(void)
{
	Size n;
	uint8 ref[128], *bytes;
	int fd;
	if (!cluster_recovery_registry_size_v2(&reg, 65536, &n))
		abort();
	bytes = malloc(n);
	if (!bytes || !cluster_recovery_registry_encode_v2(&reg, bytes, n, ref) ||
		!cluster_control_root_registry_encode_v4(&root, &reg, ref, selection_bytes, 66560))
		abort();
	fd = openat(selection_dir, registry_name, O_WRONLY | O_TRUNC);
	if (fd < 0 || write(fd, bytes, n) != n || close(fd))
		abort();
	free(bytes);
	write_root(selection_dir, "pgrac_control_root", selection_bytes);
	write_root(selection_dir, "pgrac_control_root.bak", selection_bytes);
}
static void inventory_begin(unsigned count)
{
	selection_begin();
	for (unsigned i = 0; i < count; i++) {
		for (unsigned copy = 0; copy < 2; copy++) {
			ClusterRecoveryCheckpointFileV2 *f = &inventory[i * 2 + copy];
			*f = files[copy];
			f->identity.database_oid = i == 0 ? InvalidOid : 1000 + i;
			f->identity.tablespace_oid = i == 0 ? GLOBALTABLESPACE_OID : DEFAULTTABLESPACE_OID;
			f->identity.origin = i == 0 ? 0 : 1;
		}
		completed[i] = inventory[i * 2].identity;
	}
	reg.coverage.files = inventory;
	reg.coverage.file_count = count * 2;
	inventory_write();
}
static bool inventory_read(ClusterControlRelmapSelectionV2 **out)
{
	return cluster_control_root_relmap_selection_read_locked_v4(&expected_config, 65536, out) == 0;
}
static bool identity_same(const ClusterRecoveryFileIdentityV2 *a,
						  const ClusterRecoveryFileIdentityV2 *b)
{
	uint8 bytes[128];
	return cluster_recovery_file_identity_encode_v2(a, bytes, sizeof(bytes)) &&
		   cluster_recovery_file_identity_matches_v2(bytes, sizeof(bytes), b);
}
UT_TEST(test_complete_inventory_origin_lookup_and_sources)
{
	ClusterControlRelmapSelectionV2 *s = NULL;
	ClusterRecoveryFileIdentityV2 out;
	ClusterRecoveryCheckpointSourceV2 source;
	inventory_begin(73);
	UT_ASSERT(inventory_read(&s));
	UT_ASSERT_EQ(cluster_control_root_relmap_selection_count_v4(s), 73);
	for (unsigned i = 0; i < 73; i++) {
		UT_ASSERT(cluster_control_root_relmap_selection_at_v4(s, i, &out));
		UT_ASSERT(identity_same(&out, &completed[i]));
		UT_ASSERT(cluster_control_root_relmap_selection_lookup_v4(
			s, out.database_oid, out.tablespace_oid, out.object_id, &out));
		UT_ASSERT(identity_same(&out, &completed[i]));
	}
	UT_ASSERT_EQ(cluster_control_root_relmap_selection_source_count_v4(s), 2);
	UT_ASSERT(cluster_control_root_relmap_selection_source_at_v4(s, 1, &source));
	UT_ASSERT_EQ(source.source.claim.identity.origin_node_id, 1);
	UT_ASSERT_EQ(source.replay_end, sources[1].replay_end);
	UT_ASSERT(!cluster_control_root_relmap_selection_at_v4(s, 73, &out));
	UT_ASSERT(!cluster_control_root_relmap_selection_lookup_v4(s, 9000, 1663, 0, &out));
	UT_ASSERT(cluster_control_root_relmap_selection_complete_locked_v4(s, completed, 73) == 0);
	cluster_control_root_relmap_selection_destroy_v4(&s);
	UT_ASSERT(s == NULL && cf_held && fcntl(selection_dir, F_GETFD) >= 0);
	cluster_control_root_relmap_selection_destroy_v4(&s);
	selection_end();
}
UT_TEST(test_inventory_requires_original_custody_and_fresh_output)
{
	ClusterControlRelmapSelectionV2 *s = NULL;
	inventory_begin(2);
	cf_held = false;
	UT_ASSERT(!inventory_read(&s) && s == NULL && dependencies == 0);
	cf_held = true;
	UT_ASSERT(cluster_control_root_relmap_selection_read_locked_v4(&expected_config, 512, &s) != 0);
	UT_ASSERT(s == NULL);
	UT_ASSERT(cluster_control_root_relmap_selection_read_locked_v4(&expected_config, 65536,
																   (void *)&expected_config) != 0);
	UT_ASSERT(inventory_read(&s));
	UT_ASSERT(!inventory_read(&s));
	UT_ASSERT(!cluster_control_root_relmap_selection_at_v4(s, 0, (void *)s));
	UT_ASSERT(!cluster_control_root_relmap_selection_source_at_v4(s, 0, (void *)s));
	cluster_control_root_relmap_selection_destroy_v4(&s);
	selection_end();
}
UT_TEST(test_inventory_rejects_ambiguous_birth_origin_and_incomplete_copy)
{
	ClusterControlRelmapSelectionV2 *s = NULL;
	inventory_begin(2);
	for (unsigned i = 2; i < 4; i++) {
		inventory[i].identity = inventory[i - 2].identity;
		inventory[i].identity.creation_generation++;
	}
	inventory_write();
	UT_ASSERT(!inventory_read(&s) && s == NULL);
	for (unsigned i = 2; i < 4; i++) {
		inventory[i].identity = inventory[i - 2].identity;
		inventory[i].identity.origin = 1;
	}
	inventory_write();
	UT_ASSERT(!inventory_read(&s) && s == NULL);
	reg.coverage.file_count = 1;
	inventory[0].copy_count = 1;
	inventory_write();
	UT_ASSERT(!inventory_read(&s) && s == NULL);
	selection_end();
}
UT_TEST(test_inventory_requires_complete_configured_source_set_and_shared_address)
{
	ClusterControlRelmapSelectionV2 *s = NULL;
	inventory_begin(1);
	for (unsigned i = 0; i < 2; i++)
		inventory[i].source_count = 1;
	reg.coverage.source_count = 1;
	inventory_write();
	UT_ASSERT(!inventory_read(&s) && s == NULL);
	reg.coverage.source_count = 2;
	for (unsigned i = 0; i < 2; i++)
		inventory[i].source_count = 2;
	sources[1].source.claim.identity.root_lineage_seq++;
	inventory_write();
	UT_ASSERT(!inventory_read(&s) && s == NULL);
	sources[1].source.claim.identity.root_lineage_seq--;
	for (unsigned i = 0; i < 2; i++)
		inventory[i].identity.tablespace_oid = DEFAULTTABLESPACE_OID;
	inventory_write();
	UT_ASSERT(!inventory_read(&s) && s == NULL);
	selection_end();
}
UT_TEST(test_complete_receipt_membership_refuses_partial_extra_and_wrong_identity)
{
	ClusterControlRelmapSelectionV2 *s = NULL;
	inventory_begin(2);
	UT_ASSERT(inventory_read(&s));
	UT_ASSERT(cluster_control_root_relmap_selection_complete_locked_v4(s, completed, 1) != 0);
	UT_ASSERT(cluster_control_root_relmap_selection_complete_locked_v4(s, completed, 2) != 0);
	cluster_control_root_relmap_selection_destroy_v4(&s);
	UT_ASSERT(inventory_read(&s));
	UT_ASSERT(cluster_control_root_relmap_selection_complete_locked_v4(s, completed, 3) != 0);
	cluster_control_root_relmap_selection_destroy_v4(&s);
	UT_ASSERT(inventory_read(&s));
	completed[1].creation_generation++;
	UT_ASSERT(cluster_control_root_relmap_selection_complete_locked_v4(s, completed, 2) != 0);
	cluster_control_root_relmap_selection_destroy_v4(&s);
	selection_end();
}
UT_TEST(test_post_io_root_registry_error_and_cf_reacquire_cannot_revive)
{
	ClusterControlRelmapSelectionV2 *s = NULL;
	ClusterRecoveryFileIdentityV2 out, before;
	volatile bool caught = false;
	inventory_begin(2);
	memset(&out, 0xa5, sizeof(out));
	before = out;
	UT_ASSERT(inventory_read(&s));
	cf_sequence++;
	UT_ASSERT(!cluster_control_root_relmap_selection_at_v4(s, 0, &out));
	cf_sequence--;
	UT_ASSERT_EQ(cluster_control_root_relmap_selection_count_v4(s), 0);
	UT_ASSERT(memcmp(&out, &before, sizeof(out)) == 0);
	cluster_control_root_relmap_selection_destroy_v4(&s);
	UT_ASSERT(inventory_read(&s));
	config_hook = selection_change;
	UT_ASSERT(cluster_control_root_relmap_selection_recheck_locked_v4(s) != 0);
	write_root(selection_dir, "pgrac_control_root", selection_bytes);
	UT_ASSERT(cluster_control_root_relmap_selection_recheck_locked_v4(s) != 0);
	cluster_control_root_relmap_selection_destroy_v4(&s);
	UT_ASSERT(inventory_read(&s));
	config_error = true;
	PG_TRY();
	{
		(void)cluster_control_root_relmap_selection_recheck_locked_v4(s);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	config_error = false;
	UT_ASSERT(caught && cf_held && fcntl(selection_dir, F_GETFD) >= 0);
	UT_ASSERT(cluster_control_root_relmap_selection_recheck_locked_v4(s) != 0);
	cluster_control_root_relmap_selection_destroy_v4(&s);
	UT_ASSERT(inventory_read(&s));
	{
		int fd = openat(selection_dir, registry_name, O_WRONLY);
		UT_ASSERT(fd >= 0 && pwrite(fd, "x", 1, 256) == 1 && close(fd) == 0);
	}
	UT_ASSERT(cluster_control_root_relmap_selection_recheck_locked_v4(s) != 0);
	cluster_control_root_relmap_selection_destroy_v4(&s);
	selection_end();
}
UT_TEST(test_post_io_named_directory_remains_original)
{
	ClusterControlRelmapSelectionV2 *s = NULL;
	char old[160], moved[160];
	inventory_begin(1);
	UT_ASSERT(inventory_read(&s));
	selection_directory_drift();
	UT_ASSERT(cluster_control_root_relmap_selection_recheck_locked_v4(s) != 0);
	snprintf(old, sizeof(old), "%s/global", selection_path);
	snprintf(moved, sizeof(moved), "%s/displaced", selection_path);
	UT_ASSERT(rmdir(old) == 0 && rename(moved, old) == 0);
	cluster_control_root_relmap_selection_destroy_v4(&s);
	selection_end();
}
UT_TEST(test_actual_pair_sync_readback_then_complete_control_recheck)
{
	ClusterControlRelmapSelectionV2 *s = NULL;
	ClusterRelmapFilePairV2 pair;
	ClusterRelmapV2Image image;
	uint8 bytes[2304];
	inventory_begin(2);
	UT_ASSERT(inventory_read(&s));
	for (unsigned i = 0; i < 2; i++) {
		char path[160];
		int dir;
		snprintf(path, sizeof(path), "%s/pair%u", selection_path, i);
		UT_ASSERT(mkdir(path, 0700) == 0);
		dir = open(path, O_RDONLY | O_DIRECTORY);
		UT_ASSERT(dir >= 0);
		memset(&image, 0, sizeof(image));
		image.identity = (ClusterRelmapV2Namespace){11, 12, completed[i].database_oid, i == 0};
		image.committed_generation = 1;
		image.committed.magic = RELMAPPER_FILEMAGIC;
		INIT_CRC32C(image.committed.crc);
		COMP_CRC32C(image.committed.crc, &image.committed, offsetof(RelMapFile, crc));
		FIN_CRC32C(image.committed.crc);
		UT_ASSERT(cluster_relmap_file_encode_v2(&completed[i], &image, bytes, sizeof(bytes)));
		for (unsigned c = 0; c < 2; c++) {
			int fd = openat(dir, c ? "pgrac_relmap_authority.bak" : "pgrac_relmap_authority",
							O_WRONLY | O_CREAT | O_EXCL, 0600);
			UT_ASSERT(fd >= 0 && write(fd, bytes, sizeof(bytes)) == sizeof(bytes) &&
					  close(fd) == 0);
		}
		UT_ASSERT(cluster_relmap_file_pair_checkpoint_v2(dir, &completed[i], &pair));
		UT_ASSERT(memcmp(pair.bytes[0], bytes, sizeof(bytes)) == 0);
		UT_ASSERT(memcmp(pair.bytes[1], bytes, sizeof(bytes)) == 0);
		UT_ASSERT(unlinkat(dir, "pgrac_relmap_authority", 0) == 0);
		UT_ASSERT(unlinkat(dir, "pgrac_relmap_authority.bak", 0) == 0);
		UT_ASSERT(close(dir) == 0 && rmdir(path) == 0);
	}
	UT_ASSERT(cluster_control_root_relmap_selection_complete_locked_v4(s, completed, 2) == 0);
	cluster_control_root_relmap_selection_destroy_v4(&s);
	selection_end();
}
UT_TEST(test_copied_checkpoint_reference_does_not_revive_cf_authority)
{
	ClusterControlRelmapSelectionV2 *old = NULL, *fresh = NULL;
	ClusterControlRelmapReferenceV2 before, after;
	inventory_begin(2);
	UT_ASSERT(inventory_read(&old));
	UT_ASSERT(cluster_control_root_relmap_selection_reference_v4(old, &before));
	UT_ASSERT(before.logical_count == 2 && before.source_count == 2);
	UT_ASSERT(!cluster_control_root_relmap_selection_reference_v4(old, (void *)old));
	cf_sequence++;
	UT_ASSERT(!cluster_control_root_relmap_selection_reference_v4(old, &after));
	UT_ASSERT(inventory_read(&fresh));
	UT_ASSERT(cluster_control_root_relmap_selection_reference_v4(fresh, &after));
	UT_ASSERT(memcmp(&before, &after, sizeof(before)) == 0);
	UT_ASSERT(cluster_control_root_relmap_selection_count_v4(old) == 0);
	cluster_control_root_relmap_selection_destroy_v4(&fresh);
	inventory[0].image_digest[0] ^= 1;
	inventory_write();
	UT_ASSERT(inventory_read(&fresh));
	UT_ASSERT(cluster_control_root_relmap_selection_reference_v4(fresh, &after));
	UT_ASSERT(memcmp(before.registry_reference, after.registry_reference, 128) != 0);
	UT_ASSERT(memcmp(before.root_pair_sha256, after.root_pair_sha256, 32) != 0);
	cluster_control_root_relmap_selection_destroy_v4(&old);
	cluster_control_root_relmap_selection_destroy_v4(&fresh);
	selection_end();
}

#ifndef TEST_CONTROL_RELMAP_CONSUMER
int main(void)
{
	UT_PLAN(19);
	UT_RUN(test_original_codec_v4_extent_and_exact_reference);
	UT_RUN(test_v4_refuses_forged_reference_and_record);
	UT_RUN(test_selected_root_registry_real_files_and_late_change);
	UT_RUN(test_control_selects_complete_original_file_identity);
	UT_RUN(test_control_selection_refuses_missing_custody_and_dependency);
	UT_RUN(test_control_selection_late_root_and_error_preserve_output);
	UT_RUN(test_control_selection_valid_outer_hash_cannot_hide_noncanonical_source);
	UT_RUN(test_control_selection_key_and_named_directory_remain_original);
	UT_RUN(test_control_selection_ambiguous_births_refuse_without_latest_rule);
	UT_RUN(test_complete_decoded_vector_exceeds_active_pin_capacity);
	UT_RUN(test_complete_inventory_origin_lookup_and_sources);
	UT_RUN(test_inventory_requires_original_custody_and_fresh_output);
	UT_RUN(test_inventory_rejects_ambiguous_birth_origin_and_incomplete_copy);
	UT_RUN(test_inventory_requires_complete_configured_source_set_and_shared_address);
	UT_RUN(test_complete_receipt_membership_refuses_partial_extra_and_wrong_identity);
	UT_RUN(test_post_io_root_registry_error_and_cf_reacquire_cannot_revive);
	UT_RUN(test_post_io_named_directory_remains_original);
	UT_RUN(test_actual_pair_sync_readback_then_complete_control_recheck);
	UT_RUN(test_copied_checkpoint_reference_does_not_revive_cf_authority);
	UT_DONE();
	return ut_failed_count != 0;
}

#endif /* TEST_CONTROL_RELMAP_CONSUMER */
