/* Real ROOT/registry I/O and selector. Native common/config/storage leaves only
 * are controlled; they cannot substitute for the caller's CF custody. */
#define TEST_CONTROL_FILE_SELECTION 1
#include "test_cluster_control_registry.c"
#include "cluster/cluster_control_file_identity.h"
#include "cluster/cluster_cf_authority.h"
#include "cluster/cluster_cf_storage.h"
#include "cluster/cluster_conf.h"
#include "cluster/storage/cluster_shared_fs.h"
#include "storage/fd.h"
#include "miscadmin.h"
#include "utils/elog.h"
#include "../../common/sha2_int.h"
char *cluster_shared_data_dir;
char *DataDir = "/original-local-data";
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
static bool storage_ok = true, common_ok = true, config_ok = true, config_error;
static unsigned dependencies;
static void (*config_hook)(void);
static ClusterSharedConfigRef expected_config;
void
pg_re_throw(void)
{
	if (PG_exception_stack)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}
int
OpenTransientFile(const char *p, int flags)
{
	return open(p, flags, 0600);
}
int
CloseTransientFile(int fd)
{
	return close(fd);
}
int
cluster_conf_node_count(void)
{
	return 2;
}
void
cluster_shared_fs_get_storage_uuid(char *out, size_t n)
{
	snprintf(out, n, "21212121212121212121212121212121");
}
ClusterCfContractState
cluster_cf_contract_load(const char *path)
{
	return storage_ok && !strcmp(path, DataDir) ? CLUSTER_CF_CONTRACT_CROSSNODE_VERIFIED
												: CLUSTER_CF_CONTRACT_UNVERIFIED;
}
bool
cluster_cf_storage_write_allowed(ClusterCfContractState s, bool multi)
{
	return multi && s == CLUSTER_CF_CONTRACT_CROSSNODE_VERIFIED;
}
bool
cluster_cf_storage_probe_local(void)
{
	abort();
}
ClusterControlRootResult
cluster_cf_control_image_read_locked(uint64 generation, const uint8 hash[32], uint64 sys,
									 ControlFileData *out)
{
	uint8 h[32];
	memset(h, 2, 32);
	dependencies++;
	if (!common_ok || generation != 1 || sys != 11 || memcmp(hash, h, 32))
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	memset(out, 0, sizeof(*out));
	out->system_identifier = sys;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}
ClusterControlRootResult
cluster_shared_config_read_locked(const char *path, const ClusterSharedConfigRef *ref,
								  ClusterSharedConfigImage *out)
{
	dependencies++;
	memset(out, 0, sizeof(*out));
	if (config_error)
		pg_re_throw();
	if (!config_ok || strcmp(path, cluster_shared_data_dir)
		|| memcmp(ref, &expected_config, sizeof(*ref)))
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	if (config_hook) {
		void (*hook)(void) = config_hook;
		config_hook = NULL;
		hook();
	}
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}
void
cluster_shared_config_free(ClusterSharedConfigImage *out)
{
	memset(out, 0, sizeof(*out));
}
static char selection_path[100];
static int selection_dir;
static uint8 selection_bytes[66560];
static ClusterControlFileKeyV2 file_key;
static void
selection_begin(void)
{
	uint8 ref[128];
	char global[140];
	root_fixture();
	memset(&expected_config, 0, sizeof(expected_config));
	expected_config.identity.system_identifier = 11;
	expected_config.identity.database_incarnation = 12;
	expected_config.identity.generation = 1;
	memcpy(expected_config.identity.storage_uuid, root.header.storage_uuid, 16);
	memcpy(expected_config.identity.authority_uuid, root.header.authority_uuid, 16);
	expected_config.identity.configured[0] = 3;
	memset(expected_config.sha256, 1, 32);
	memset(&file_key, 0, sizeof(file_key));
	file_key.kind = CLUSTER_RECOVERY_FILE_RELMAP_V2;
	strcpy(selection_path, "/tmp/pgrac-control-selection-XXXXXX");
	if (!mkdtemp(selection_path))
		abort();
	snprintf(global, sizeof(global), "%s/global", selection_path);
	if (mkdir(global, 0700))
		abort();
	selection_dir = open(global, O_RDONLY | O_DIRECTORY);
	cluster_shared_data_dir = selection_path;
	if (!cluster_recovery_registry_create_v2(selection_dir, &reg, 4096, ref)
		|| !cluster_control_root_registry_encode_v4(&root, &reg, ref, selection_bytes, 66560))
		abort();
	write_root(selection_dir, "pgrac_control_root", selection_bytes);
	write_root(selection_dir, "pgrac_control_root.bak", selection_bytes);
	cf_held = storage_ok = common_ok = config_ok = true;
	config_error = false;
	dependencies = 0;
	config_hook = NULL;
}
static void
selection_end(void)
{
	char global[140];
	unlinkat(selection_dir, "pgrac_control_root", 0);
	unlinkat(selection_dir, "pgrac_control_root.bak", 0);
	unlinkat(selection_dir,
			 "pgrac_recovery_registry/000000000000000f-0000000000000010-0000000000000011.v2", 0);
	unlinkat(selection_dir, "pgrac_recovery_registry", AT_REMOVEDIR);
	if (close(selection_dir))
		abort();
	snprintf(global, sizeof(global), "%s/global", selection_path);
	if (rmdir(global) || rmdir(selection_path))
		abort();
	cluster_shared_data_dir = NULL;
}
static bool
selected(ClusterRecoveryFileIdentityV2 *out)
{
	return cluster_control_root_file_identity_read_locked_v4(&expected_config, &file_key, 4096, out)
		   == 0;
}
UT_TEST(test_control_selects_complete_original_file_identity)
{
	ClusterRecoveryFileIdentityV2 out;
	uint8 identity[128];
	selection_begin();
	memset(&out, 0, sizeof(out));
	UT_ASSERT(selected(&out));
	UT_ASSERT(dependencies >= 2);
	UT_ASSERT(cluster_recovery_file_identity_encode_v2(&files[0].identity, identity, 128));
	UT_ASSERT(cluster_recovery_file_identity_matches_v2(identity, 128, &out));
	file_key.object_id++;
	UT_ASSERT(!selected(&out));
	UT_ASSERT(cluster_recovery_file_identity_matches_v2(identity, 128, &out));
	selection_end();
}
UT_TEST(test_control_selection_refuses_missing_custody_and_dependency)
{
	ClusterRecoveryFileIdentityV2 out, before;
	selection_begin();
	memset(&out, 0xa5, sizeof(out));
	before = out;
	cf_held = false;
	UT_ASSERT(!selected(&out));
	UT_ASSERT_EQ(dependencies, 0);
	cf_held = true;
	storage_ok = false;
	UT_ASSERT(!selected(&out));
	UT_ASSERT_EQ(dependencies, 0);
	storage_ok = true;
	common_ok = false;
	UT_ASSERT(!selected(&out));
	common_ok = true;
	config_ok = false;
	UT_ASSERT(!selected(&out));
	config_ok = true;
	expected_config.identity.database_incarnation++;
	UT_ASSERT(!selected(&out));
	expected_config.identity.database_incarnation--;
	UT_ASSERT(memcmp(&out, &before, sizeof(out)) == 0);
	UT_ASSERT(cluster_control_root_file_identity_read_locked_v4(&expected_config, &file_key, 4096,
																(void *)&expected_config)
			  != 0);
	UT_ASSERT(
		cluster_control_root_file_identity_read_locked_v4(&expected_config, &file_key, 512, &out)
		!= 0);
	UT_ASSERT(memcmp(&out, &before, sizeof(out)) == 0);
	selection_end();
}
static void
selection_change(void)
{
	root_hook_dir = selection_dir;
	replace_primary();
}
UT_TEST(test_control_selection_late_root_and_error_preserve_output)
{
	ClusterRecoveryFileIdentityV2 out, before;
	volatile bool caught = false;
	selection_begin();
	memset(&out, 0xa5, sizeof(out));
	before = out;
	config_hook = selection_change;
	UT_ASSERT(!selected(&out));
	write_root(selection_dir, "pgrac_control_root", selection_bytes);
	test_registry_io_read_hook(selection_change);
	UT_ASSERT(!selected(&out));
	write_root(selection_dir, "pgrac_control_root", selection_bytes);
	config_error = true;
	PG_TRY();
	{
		(void)selected(&out);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	config_error = false;
	UT_ASSERT(cf_held && fcntl(selection_dir, F_GETFD) >= 0);
	UT_ASSERT(memcmp(&out, &before, sizeof(out)) == 0);
	UT_ASSERT(selected(&out));
	selection_end();
}

static void
fixture_put_be(uint8 *p, Size at, uint64 v, unsigned width)
{
	for (unsigned i = 0; i < width; i++)
		p[at + i] = (uint8)(v >> (8 * (width - i - 1)));
}
static void
fixture_sha(const uint8 *p, Size n, uint8 out[32])
{
	pg_sha256_ctx context;
	pg_sha256_init(&context);
	pg_sha256_update(&context, p, n);
	pg_sha256_final(&context, out);
}
UT_TEST(test_control_selection_valid_outer_hash_cannot_hide_noncanonical_source)
{
	uint8 bytes[1344], ref[128];
	int fd;
	ClusterRecoveryFileIdentityV2 out, before;
	selection_begin();
	memset(&out, 0xa5, sizeof(out));
	before = out;
	UT_ASSERT(cluster_recovery_registry_encode_v2(&reg, bytes, sizeof(bytes), ref));
	/* Authenticated full object, with invalid boolean outside the query file.
	 * Recompute all surrounding CRC/digests to exercise full semantic decode. */
	bytes[256 + 288 + 152] = 2;
	fixture_sha(bytes + 256, 576, bytes + 168);
	fixture_put_be(bytes, 232, fixture_crc(bytes + 256, sizeof(bytes) - 256), 4);
	memset(bytes + 236, 0, 4);
	fixture_put_be(bytes, 236, fixture_crc(bytes, 256), 4);
	fixture_sha(bytes, sizeof(bytes), ref + 72);
	memset(ref + 104, 0, 4);
	fixture_put_be(ref, 104, fixture_crc(ref, 128), 4);
	memcpy(selection_bytes + 512, ref, 128);
	fixture_put_le(selection_bytes, 1020, fixture_crc(selection_bytes, 1020), 4);
	fd = openat(selection_dir,
				"pgrac_recovery_registry/000000000000000f-0000000000000010-0000000000000011.v2",
				O_WRONLY);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(pwrite(fd, bytes, sizeof(bytes), 0), sizeof(bytes));
	UT_ASSERT_EQ(close(fd), 0);
	write_root(selection_dir, "pgrac_control_root", selection_bytes);
	write_root(selection_dir, "pgrac_control_root.bak", selection_bytes);
	UT_ASSERT(!selected(&out));
	UT_ASSERT(memcmp(&out, &before, sizeof(out)) == 0);
	selection_end();
}
static void
selection_key_drift(void)
{
	file_key.object_id++;
}
static void
selection_directory_drift(void)
{
	char old[160], moved[160];
	snprintf(old, sizeof(old), "%s/global", selection_path);
	snprintf(moved, sizeof(moved), "%s/displaced", selection_path);
	if (rename(old, moved) || mkdir(old, 0700))
		abort();
}
UT_TEST(test_control_selection_key_and_named_directory_remain_original)
{
	ClusterRecoveryFileIdentityV2 out, before;
	char old[160], moved[160];
	selection_begin();
	memset(&out, 0xa5, sizeof(out));
	before = out;
	config_hook = selection_key_drift;
	UT_ASSERT(!selected(&out));
	file_key.object_id--;
	config_hook = selection_directory_drift;
	UT_ASSERT(!selected(&out));
	snprintf(old, sizeof(old), "%s/global", selection_path);
	snprintf(moved, sizeof(moved), "%s/displaced", selection_path);
	UT_ASSERT_EQ(rmdir(old), 0);
	UT_ASSERT_EQ(rename(moved, old), 0);
	UT_ASSERT(memcmp(&out, &before, sizeof(out)) == 0);
	UT_ASSERT(selected(&out));
	selection_end();
}
UT_TEST(test_control_selection_ambiguous_births_refuse_without_latest_rule)
{
	uint8 registry[1344], ref[128];
	int fd;
	ClusterRecoveryFileIdentityV2 out, before;
	selection_begin();
	memset(&out, 0xa5, sizeof(out));
	before = out;
	files[0].copy_count = files[1].copy_count = 1;
	files[1].copy_index = 0;
	files[1].identity.creation_generation++;
	UT_ASSERT(cluster_recovery_registry_encode_v2(&reg, registry, sizeof(registry), ref));
	UT_ASSERT(cluster_control_root_registry_encode_v4(&root, &reg, ref, selection_bytes, 66560));
	fd = openat(selection_dir,
				"pgrac_recovery_registry/000000000000000f-0000000000000010-0000000000000011.v2",
				O_WRONLY);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(pwrite(fd, registry, sizeof(registry), 0), sizeof(registry));
	UT_ASSERT_EQ(close(fd), 0);
	write_root(selection_dir, "pgrac_control_root", selection_bytes);
	write_root(selection_dir, "pgrac_control_root.bak", selection_bytes);
	UT_ASSERT(!selected(&out));
	UT_ASSERT(memcmp(&out, &before, sizeof(out)) == 0);
	selection_end();
}
UT_TEST(test_complete_decoded_vector_exceeds_active_pin_capacity)
{
	ClusterRecoveryCheckpointFileV2 many[73];
	uint8 *bytes, ref[128];
	Size n;
	ClusterRecoveryRegistryDecodedV2 *decoded = NULL;
	fixture();
	for (int i = 0; i < 73; i++) {
		many[i] = files[0];
		many[i].identity.object_id = i;
		many[i].copy_count = 1;
		many[i].copy_index = 0;
	}
	reg.coverage.files = many;
	reg.coverage.file_count = 73;
	UT_ASSERT(cluster_recovery_registry_size_v2(&reg, 32768, &n));
	bytes = malloc(n);
	UT_ASSERT(bytes != NULL);
	UT_ASSERT(cluster_recovery_registry_encode_v2(&reg, bytes, n, ref));
	UT_ASSERT(cluster_recovery_registry_decode_v2(bytes, n, ref, n, &decoded));
	UT_ASSERT_EQ(cluster_recovery_registry_view_v2(decoded)->coverage.file_count, 73);
	UT_ASSERT(cluster_recovery_registry_matches_v2(
		bytes, n, cluster_recovery_registry_view_v2(decoded), ref));
	UT_ASSERT(!cluster_recovery_registry_decode_v2(bytes, n, ref, n, &decoded));
	cluster_recovery_registry_destroy_v2(&decoded);
	UT_ASSERT(decoded == NULL);
	cluster_recovery_registry_destroy_v2(&decoded);
	UT_ASSERT(!cluster_recovery_registry_decode_v2(bytes, n, ref, n - 1, &decoded));
	free(bytes);
}
int
main(void)
{
	UT_PLAN(10);
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
	UT_DONE();
	return ut_failed_count != 0;
}
