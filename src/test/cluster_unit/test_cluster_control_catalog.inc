/* Read actual ROOT-selected catalog objects, never a success I/O stub.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "../../backend/cluster/cluster_control_catalog_private.h"
#include "../../backend/cluster/cluster_initdb_catalog_private.h"
#include "../../backend/cluster/cluster_initdb_common_private.h"
#include <dirent.h>

typedef struct CatalogReadFixture {
	char path[MAXPGPATH];
	ClusterControlBootstrapSnapshot expected;
} CatalogReadFixture;

static bool catalog_fixture_remove(const char *path)
{
	DIR *dir = opendir(path);
	struct dirent *entry;
	if (dir == NULL) return false;
	while ((entry = readdir(dir)) != NULL) {
		char child[MAXPGPATH]; struct stat st;
		if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) continue;
		snprintf(child, sizeof(child), "%s/%s", path, entry->d_name);
		if (lstat(child, &st) != 0 || (S_ISDIR(st.st_mode) ? !catalog_fixture_remove(child) : unlink(child) != 0)) {
			closedir(dir); return false;
		}
	}
	return closedir(dir) == 0 && rmdir(path) == 0;
}

static void catalog_read_fixture(CatalogReadFixture *f)
{
	ControlRootImage *root = calloc(1, sizeof(*root));
	uint8 initialized[1536], binding[256], clog[BLCKSZ] = {0};
	ControlFileData cf = {0};
	const ControlFileData *sources[128] = {&cf};
	ClusterInitdbCommon common;
	ClusterSharedConfigRef config = {0};
	ClusterCatalogInitialInput input = {0};
	char path[MAXPGPATH], hex[65];
	int fd;
	const char *dirs[] = {"global", "global/control_images", "global/catalog_checkpoints",
		"pgrac_initdb_native_side", "pgrac_initdb_native_side/pg_xact"};
	memset(f, 0, sizeof(*f));
	strlcpy(f->path, "/tmp/pgrac-catalog-read.XXXXXX", sizeof(f->path));
	UT_ASSERT(root != NULL && mkdtemp(f->path) != NULL);
	initialized_fixture(initialized, root, true, 1);
	config.identity.system_identifier = cf.system_identifier = root->header.system_identifier;
	config.identity.database_incarnation = root->header.v2.database_incarnation;
	config.identity.configured[0] = config.identity.generation = 1;
	memcpy(config.identity.authority_uuid, root->header.authority_uuid, 16);
	memcpy(config.identity.storage_uuid, root->header.storage_uuid, 16);
	cf.pg_control_version = PG_CONTROL_VERSION; cf.catalog_version_no = CATALOG_VERSION_NO;
	cf.state = DB_SHUTDOWNED; cf.blcksz = BLCKSZ;
	cf.checkPoint = cf.checkPointCopy.redo = 0x2000028;
	cf.checkPointCopy.ThisTimeLineID = cf.checkPointCopy.PrevTimeLineID = 1;
	cf.checkPointCopy.nextXid = FullTransactionIdFromU64(750);
	cf.checkPointCopy.oldestXid = 3;
	cf.checkPointCopy.nextOid = 13253;
	cf.checkPointCopy.nextMulti = cf.checkPointCopy.oldestMulti = 1;
	INIT_CRC32C(cf.crc); COMP_CRC32C(cf.crc, &cf, offsetof(ControlFileData, crc)); FIN_CRC32C(cf.crc);
	UT_ASSERT(cluster_initdb_common_build(&config, sources, &common));
	memcpy(root->header.v2.control_image_sha256, common.control_sha256, 32);
	memcpy(root->header.v2.catalog_manifest_sha256, common.catalog_sha256, 32);
	UT_ASSERT_EQ(cluster_control_root_v3_encode(root), 0);
	f->expected.binding.system_identifier = root->header.system_identifier;
	f->expected.binding.database_incarnation = root->header.v2.database_incarnation;
	f->expected.binding.lineage_kind = PGRAC_CONTROL_LINEAGE_CREATION_V1;
	f->expected.binding.migration_prepare_generation = 1;
	memcpy(f->expected.binding.storage_uuid, root->header.storage_uuid, 16);
	memcpy(f->expected.binding.authority_uuid, root->header.authority_uuid, 16);
	memcpy(f->expected.binding.migration_round_sha256, root->header.migration_round_sha256, 32);
	memcpy(f->expected.binding.source_wal_state_sha256, root->header.source_wal_state_sha256, 32);
	UT_ASSERT(pgrac_control_binding_encode(&f->expected.binding, binding, sizeof(binding)));
	sha256_bytes(root->bytes, sizeof(root->bytes), f->expected.root_sha256);
	f->expected.root_sequence = root->header.file_txn_seq;
	for (unsigned i = 0; i < lengthof(dirs); i++) {
		snprintf(path, sizeof(path), "%s/%s", f->path, dirs[i]); UT_ASSERT_EQ(mkdir(path, 0700), 0);
	}
	snprintf(path, sizeof(path), "%s/global/pgrac_control_root", f->path);
	write_all_or_abort(path, root->bytes, sizeof(root->bytes));
	snprintf(path, sizeof(path), "%s/global/pgrac_control_binding", f->path);
	write_all_or_abort(path, binding, sizeof(binding));
	bootstrap_hex(common.control_sha256, hex);
	snprintf(path, sizeof(path), "%s/global/control_images/1-%s.bin", f->path, hex);
	write_all_or_abort(path, common.control, sizeof(common.control));
	bootstrap_hex(common.catalog_sha256, hex);
	snprintf(path, sizeof(path), "%s/global/catalog_checkpoints/1-%s.json", f->path, hex);
	write_all_or_abort(path, common.catalog, common.catalog_length);
	snprintf(path, sizeof(path), "%s/pgrac_initdb_native_side/pg_xact/0000", f->path);
	write_all_or_abort(path, clog, sizeof(clog));
	input.identity.system_identifier = cf.system_identifier;
	input.identity.database_incarnation = config.identity.database_incarnation;
	memcpy(input.identity.authority_uuid, config.identity.authority_uuid, 16);
	memcpy(input.identity.storage_uuid, config.identity.storage_uuid, 16);
	input.native_control = &cf; input.native_control_length = sizeof(cf);
	input.native_clog = clog; input.native_clog_length = sizeof(clog);
	snprintf(path, sizeof(path), "%s/global", f->path);
	fd = open(path, O_RDONLY | O_DIRECTORY);
	UT_ASSERT(fd >= 0 && cluster_initdb_catalog_create(fd, &input) && close(fd) == 0);
	free(root);
}

UT_TEST(test_catalog_read_requires_independent_original_and_exact_root)
{
	CatalogReadFixture f;
	ClusterControlCatalogRead *read = NULL;
	ClusterCatalogStartupInput input;
	char path[MAXPGPATH];
	catalog_read_fixture(&f);
	UT_ASSERT(cluster_control_catalog_read(f.path, f.path, &f.expected, &read, &input));
	UT_ASSERT(cluster_catalog_startup_validate(&input));
	UT_ASSERT(cluster_control_catalog_current(read));
	cluster_control_catalog_release(&read);
	UT_ASSERT(read == NULL);
	f.expected.root_sha256[0] ^= 1;
	UT_ASSERT(!cluster_control_catalog_read(f.path, f.path, &f.expected, &read, &input));
	UT_ASSERT(v2_zero(&input, sizeof(input)));
	cluster_control_catalog_release(&read);
	f.expected.root_sha256[0] ^= 1;
	snprintf(path, sizeof(path), "%s/pgrac_initdb_native_side/pg_xact/0000", f.path);
	UT_ASSERT_EQ(unlink(path), 0);
	UT_ASSERT(!cluster_control_catalog_read(f.path, f.path, &f.expected, &read, &input));
	cluster_control_catalog_release(&read);
	UT_ASSERT(catalog_fixture_remove(f.path));
}
UT_TEST(test_catalog_recheck_rejects_replaced_root_and_changed_allocator)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		CatalogReadFixture f;
		ClusterControlCatalogRead *read = NULL;
		ClusterCatalogStartupInput input;
		char path[MAXPGPATH], saved[MAXPGPATH];
		catalog_read_fixture(&f);
		UT_ASSERT(cluster_control_catalog_read(f.path, f.path, &f.expected, &read, &input));
		UT_ASSERT(cluster_catalog_startup_validate(&input));
		snprintf(path, sizeof(path), "%s/%s", f.path, fault == 0 ? "global/pgrac_control_root"
			: fault == 1 ? "global/pgrac_oid_authority" : "pgrac_initdb_native_side/pg_xact/0000");
		if (fault == 0) {
			uint8 bytes[66048];
			read_all_or_abort(path, bytes, sizeof(bytes));
			snprintf(saved, sizeof(saved), "%s.saved", path);
			UT_ASSERT_EQ(rename(path, saved), 0);
			write_all_or_abort(path, bytes, sizeof(bytes));
		} else {
			int fd = open(path, O_WRONLY);
			UT_ASSERT(fd >= 0 && pwrite(fd, "X", 1, 0) == 1 && close(fd) == 0);
		}
		UT_ASSERT(!cluster_control_catalog_current(read));
		cluster_control_catalog_release(&read);
		UT_ASSERT(catalog_fixture_remove(f.path));
	}
}
UT_TEST(test_catalog_read_refuses_missing_corrupt_and_unsafe_objects)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		CatalogReadFixture f;
		ClusterControlCatalogRead *read = NULL;
		ClusterCatalogStartupInput input;
		char path[MAXPGPATH];
		bool result;
		catalog_read_fixture(&f);
		snprintf(path, sizeof(path), "%s/global/pgrac_catalog_authority", f.path);
		if (fault == 0) UT_ASSERT_EQ(unlink(path), 0);
		if (fault == 1) { int fd = open(path, O_WRONLY); UT_ASSERT(fd >= 0 && pwrite(fd, "X", 1, 0) == 1 && close(fd) == 0); }
		if (fault == 2) { UT_ASSERT_EQ(unlink(path), 0); UT_ASSERT_EQ(mkfifo(path, 0600), 0); }
		if (fault == 3) UT_ASSERT_EQ(chmod(path, 0666), 0);
		result = cluster_control_catalog_read(f.path, f.path, &f.expected, &read, &input);
		UT_ASSERT(!result || !cluster_catalog_startup_validate(&input));
		cluster_control_catalog_release(&read);
		UT_ASSERT(catalog_fixture_remove(f.path));
	}
}

UT_TEST(test_creation_bootstrap_requires_both_space_format_bits)
{
	CatalogReadFixture f;
	ControlRootImage root;
	uint8 bytes[66048];
	char path[MAXPGPATH];
	catalog_read_fixture(&f);
	snprintf(path, sizeof(path), "%s/global/pgrac_control_root", f.path);
	read_all_or_abort(path, bytes, sizeof(bytes));
	UT_ASSERT_EQ(cluster_control_bootstrap_root_decode(bytes, sizeof(bytes),
		f.expected.binding.storage_uuid, f.expected.binding.system_identifier, &root), 0);
	for (unsigned bit = 25; bit <= 26; bit++) {
		root.header.target_feature_bitmap |= UINT64CONST(3) << 25;
		root.header.target_feature_bitmap &= ~(UINT64CONST(1) << bit);
		UT_ASSERT_EQ(cluster_control_bootstrap_root_bound(&f.expected.binding, &root),
			CLUSTER_CONTROL_ROOT_PROFILE_UNSUPPORTED);
	}
	UT_ASSERT(catalog_fixture_remove(f.path));
}
