#define TEST_CONTROL_REGISTRY 1
int registry_codec_fixture_main(void);
#define main registry_codec_fixture_main
#include "test_cluster_recovery_registry.c"
#undef main
#include "../../backend/cluster/cluster_control_root_registry_private.h"
#include "port/pg_crc32c.h"
#include "storage/lockdefs.h"
/* Only the lock-presence leaf is controlled; codecs, file reads and digests real. */
static bool cf_held = true;
bool cluster_cf_held_is_clusterwide(LOCKMODE mode);
bool cluster_cf_held_is_clusterwide(LOCKMODE mode)
{
	return cf_held;
}
static ControlRootImage root;
static void root_fixture(void)
{
	fixture();
	memset(&root, 0, sizeof(root));
	root.header.format_version = 3;
	root.header.file_txn_seq = 14;
	root.header.system_identifier = 11;
	memcpy(root.header.storage_uuid, reg.storage_uuid, 16);
	memset(root.header.authority_uuid, 0xab, 16);
	root.header.authority_uuid[6] = 0x4b;
	root.header.authority_uuid[8] = 0x8b;
	root.header.activation_state = 2;
	root.header.lineage_kind = PGRAC_CONTROL_LINEAGE_CREATION_V1;
	root.header.created_at_usec = 1;
	root.header.published_at_usec = 2;
	memset(root.header.migration_round_sha256, 1, 32);
	memset(root.header.source_wal_state_sha256, 2, 32);
	root.header.migration_prepare_generation = 1;
	root.header.target_feature_bitmap = PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_DUTY_IDENTITY_V1;
	root.header.v2.database_state = CLUSTER_CONTROL_ROOT_DATABASE_MOUNTED;
	root.header.v2.database_incarnation = 12;
	root.header.v2.formation_seq = 1;
	root.header.v2.configured[0] = 3;
	root.header.v2.config_generation = 1;
	memset(root.header.v2.config_sha256, 1, 32);
	root.header.v2.control_image_generation = 1;
	memset(root.header.v2.control_image_sha256, 2, 32);
	root.header.v2.catalog_manifest_generation = 1;
	memset(root.header.v2.catalog_manifest_sha256, 3, 32);
	root.header.v2.global_scn_high_water = 1;
	for (int i = 0; i < 2; i++)
	{
		ClusterControlRootSnapshot *s = &root.records[i];
		memcpy(sources[i].source.claim.identity.authority_uuid, root.header.authority_uuid, 16);
		s->identity = sources[i].source.claim.identity;
		s->lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN;
		s->root_flags =
			CLUSTER_CONTROL_ROOT_FLAG_CLAIM_VALID | CLUSTER_CONTROL_ROOT_FLAG_CHECKPOINT_VALID;
		s->root_publish_seq = 1;
		s->checkpoint_tli = 1;
		s->checkpoint_lower_lsn = 100;
		s->checkpoint_source_kind = CLUSTER_CONTROL_ROOT_CHECKPOINT_NATIVE_V1;
		s->lifecycle_reason = CLUSTER_CONTROL_ROOT_PUBLISH_THREAD_OPEN;
		root.present[i] = true;
		root.publisher_incarnation[i] = 10 + i;
		root.publisher_node[i] = i;
		root.refs[i].anchor_generation = 1;
		memset(root.refs[i].anchor_sha256, 1, 32);
		memcpy(root.refs[i].claim_sha256, sources[i].source.claim.claim_sha256, 32);
	}
}
UT_TEST(test_original_codec_v4_extent_and_exact_reference)
{
	uint8 registry[1344], ref[128], bytes[66560];
	ControlRootImage out;
	root_fixture();
	UT_ASSERT_EQ(cluster_control_root_v3_encode(&root), 0);
	UT_ASSERT(cluster_recovery_registry_encode_v2(&reg, registry, sizeof(registry), ref));
	UT_ASSERT(cluster_control_root_registry_encode_v4(&root, &reg, ref, bytes, sizeof(bytes)));
	UT_ASSERT_EQ(bytes[4], 4);
	UT_ASSERT_EQ(bytes[7], 4);
	UT_ASSERT(memcmp(bytes + 512, ref, 128) == 0);
	UT_ASSERT(memcmp(bytes + 1024, root.bytes + 512, 65536) == 0);
	UT_ASSERT(cluster_control_root_registry_matches_v4(bytes, sizeof(bytes), &reg));
	UT_ASSERT(cluster_control_root_v3_decode(bytes, sizeof(bytes), reg.storage_uuid, 11, &out) !=
			  0);
	for (Size i = 376; i < 512; i++)
		UT_ASSERT_EQ(bytes[i], 0);
}
static void fixture_put_le(uint8 *p, Size at, uint64 n, unsigned w)
{
	for (unsigned i = 0; i < w; i++)
		p[at + i] = (uint8)(n >> (8 * i));
}
static uint32 fixture_crc(const uint8 *p, Size n)
{
	pg_crc32c c;
	INIT_CRC32C(c);
	COMP_CRC32C(c, p, n);
	FIN_CRC32C(c);
	return c;
}
UT_TEST(test_v4_refuses_forged_reference_and_record)
{
	uint8 registry[1344], ref[128], bytes[66560], saved[66560];
	root_fixture();
	UT_ASSERT(cluster_recovery_registry_encode_v2(&reg, registry, sizeof(registry), ref));
	UT_ASSERT(cluster_control_root_registry_encode_v4(&root, &reg, ref, bytes, sizeof(bytes)));
	memcpy(saved, bytes, sizeof(bytes));
	ref[72] ^= 1;
	UT_ASSERT(!cluster_control_root_registry_encode_v4(&root, &reg, ref, bytes, sizeof(bytes)));
	UT_ASSERT(memcmp(saved, bytes, sizeof(bytes)) == 0);
	bytes[1024] = 1;
	UT_ASSERT(!cluster_control_root_registry_matches_v4(bytes, sizeof(bytes), &reg));
	memcpy(bytes, saved, sizeof(bytes));
	bytes[504] = 1;
	UT_ASSERT(!cluster_control_root_registry_matches_v4(bytes, sizeof(bytes), &reg));
	memcpy(bytes, saved, sizeof(bytes));
	/* Recompute all container CRCs: only the original record version gate can reject. */
	bytes[1024 + 4] = 2;
	fixture_put_le(bytes, 1024 + 504, fixture_crc(bytes + 1024, 504), 4);
	fixture_put_le(bytes, 96, fixture_crc(bytes + 1024, 65536), 4);
	fixture_put_le(bytes, 1020, fixture_crc(bytes, 1020), 4);
	UT_ASSERT(!cluster_control_root_registry_matches_v4(bytes, sizeof(bytes), &reg));
	memcpy(bytes, saved, sizeof(bytes));
	reg.coverage.control_generation++;
	UT_ASSERT(!cluster_control_root_registry_matches_v4(bytes, sizeof(bytes), &reg));
	root_fixture();
	memcpy(bytes, saved, sizeof(bytes));
	fixture_put_le(bytes, 180, CLUSTER_RECOVERY_CONTROL_ROOT_FEATURE, 8);
	fixture_put_le(bytes, 1020, fixture_crc(bytes, 1020), 4);
	UT_ASSERT(!cluster_control_root_registry_matches_v4(bytes, sizeof(bytes), &reg));
}
static void write_root(int dir, const char *name, const uint8 *bytes)
{
	int fd = openat(dir, name, O_WRONLY | O_CREAT | O_TRUNC, 0600);
	if (fd < 0 || write(fd, bytes, 66560) != 66560 || fsync(fd) != 0 || close(fd) != 0)
		abort();
}
static int root_hook_dir;
static void replace_primary(void)
{
	int fd = openat(root_hook_dir, "pgrac_control_root", O_WRONLY);
	if (fd < 0 || pwrite(fd, "x", 1, 1024) != 1 || close(fd) != 0)
		abort();
}
UT_TEST(test_selected_root_registry_real_files_and_late_change)
{
	char path[] = "/tmp/pgrac-root-registry-XXXXXX", name[160];
	uint8 ref[128], bytes[66560];
	int dir;
	root_fixture();
	UT_ASSERT(mkdtemp(path) != NULL);
	dir = open(path, O_RDONLY | O_DIRECTORY);
	UT_ASSERT(dir >= 0);
	UT_ASSERT(cluster_recovery_registry_create_v2(dir, &reg, 4096, ref));
	UT_ASSERT(cluster_control_root_registry_encode_v4(&root, &reg, ref, bytes, sizeof(bytes)));
	write_root(dir, "pgrac_control_root", bytes);
	write_root(dir, "pgrac_control_root.bak", bytes);
	UT_ASSERT(
		cluster_control_root_registry_read_selected_v4(dir, bytes, sizeof(bytes), &reg, 4096));

	/* A valid older backup must preserve the same immutable creation lineage. */
	{
		uint8 old_registry[1344], old_ref[128], old_root[66560];
		root.header.file_txn_seq = 13;
		reg.coverage.control_generation = 13;
		root.header.migration_round_sha256[0] ^= 1;
		UT_ASSERT(
			cluster_recovery_registry_encode_v2(&reg, old_registry, sizeof(old_registry), old_ref));
		UT_ASSERT(cluster_control_root_registry_encode_v4(&root, &reg, old_ref, old_root,
														  sizeof(old_root)));
		write_root(dir, "pgrac_control_root.bak", old_root);
		root_fixture();
		UT_ASSERT(
			!cluster_control_root_registry_read_selected_v4(dir, bytes, sizeof(bytes), &reg, 4096));
		write_root(dir, "pgrac_control_root.bak", bytes);
	}
	root_hook_dir = dir;
	test_registry_io_read_hook(replace_primary);
	UT_ASSERT(
		!cluster_control_root_registry_read_selected_v4(dir, bytes, sizeof(bytes), &reg, 4096));
	write_root(dir, "pgrac_control_root", bytes);
	cf_held = false;
	UT_ASSERT(
		!cluster_control_root_registry_read_selected_v4(dir, bytes, sizeof(bytes), &reg, 4096));
	cf_held = true;
	bytes[1024] = 1;
	write_root(dir, "pgrac_control_root", bytes);
	bytes[1024] = 0;
	UT_ASSERT(
		!cluster_control_root_registry_read_selected_v4(dir, bytes, sizeof(bytes), &reg, 4096));
	write_root(dir, "pgrac_control_root", bytes);
	UT_ASSERT_EQ(unlinkat(dir, "pgrac_control_root.bak", 0), 0);
	UT_ASSERT(
		!cluster_control_root_registry_read_selected_v4(dir, bytes, sizeof(bytes), &reg, 4096));
	UT_ASSERT_EQ(unlinkat(dir, "pgrac_control_root", 0), 0);
	snprintf(name, sizeof(name), "pgrac_recovery_registry/%016llx-%016llx-%016llx.v2", 15LL, 16LL,
			 17LL);
	UT_ASSERT_EQ(unlinkat(dir, name, 0), 0);
	UT_ASSERT_EQ(unlinkat(dir, "pgrac_recovery_registry", AT_REMOVEDIR), 0);
	UT_ASSERT_EQ(close(dir), 0);
	UT_ASSERT_EQ(rmdir(path), 0);
}
#ifndef TEST_CONTROL_FILE_SELECTION
int main(void)
{
	UT_PLAN(3);
	UT_RUN(test_original_codec_v4_extent_and_exact_reference);
	UT_RUN(test_v4_refuses_forged_reference_and_record);
	UT_RUN(test_selected_root_registry_real_files_and_late_change);
	UT_DONE();
	return ut_failed_count != 0;
}

#endif
