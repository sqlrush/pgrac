/* Actual sharedfs DROP I/O with faults only at the POSIX boundary.
 * Author: SqlRush <sqlrush@gmail.com> */
#define main sharedfs_existing_tests_main
#include "test_cluster_shared_fs_sharedfs.c"
#undef main
#include "cluster/cluster_space_identity.h"

bool enableFsync = true;

enum DropFault {
	DROP_OK,
	DROP_TRUNCATE,
	DROP_MAIN_SYNC,
	DROP_AUX_UNLINK,
	DROP_PARTIAL,
	DROP_DIRECTORY_SYNC,
	DROP_UNLINK_ENOENT,
	DROP_REPLACE_MAIN,
	DROP_REPLACE_SPACE,
	DROP_CHANGE_IDENTITY,
	DROP_NEW_OPTIONAL,
	DROP_REPLACE_DIRECTORY
};
static enum DropFault drop_fault;
static ClusterSpaceIdentity drop_identity;
static char drop_paths[MAX_FORKNUM + 1][MAXPGPATH];
static char drop_directory[MAXPGPATH];
static int drop_main_syncs;
static int drop_directory_syncs;
static int drop_unlinks;
static int drop_truncates;

static void
write_drop_file(const char *path, const void *bytes, size_t length)
{
	int fd = open(path, O_RDWR | O_CREAT | O_EXCL, 0600);

	if (fd < 0 || write(fd, bytes, length) != length || close(fd) != 0)
		abort();
}

static int
drop_ftruncate(int fd, off_t size)
{
	drop_truncates++;
	if (drop_fault == DROP_TRUNCATE) {
		errno = EIO;
		return -1;
	}
	return ftruncate(fd, size);
}

static int
drop_fsync(int fd)
{
	struct stat st;
	PGAlignedBlock page;
	char moved[MAXPGPATH];
	int changed;

	if (fstat(fd, &st) != 0)
		abort();
	if (S_ISDIR(st.st_mode)) {
		drop_directory_syncs++;
		if (drop_unlinks < 3)
			abort(); /* success may never precede the actual auxiliary unlinks */
		if (drop_fault == DROP_DIRECTORY_SYNC) {
			errno = EIO;
			return -1;
		}
		if (drop_fault == DROP_REPLACE_DIRECTORY) {
			snprintf(moved, sizeof(moved), "%s.old", drop_directory);
			if (rename(drop_directory, moved) != 0 || mkdir(drop_directory, 0700) != 0)
				abort();
		}
		return fsync(fd);
	}
	drop_main_syncs++;
	if (st.st_size != 0 || drop_unlinks != 0)
		abort(); /* MAIN reservation must be durable before removing any fork */
	if (drop_fault == DROP_MAIN_SYNC) {
		errno = EIO;
		return -1;
	}
	if (!cluster_space_identity_page_encode(&drop_identity, 17, page.data, BLCKSZ))
		abort();
	if (drop_fault == DROP_REPLACE_MAIN || drop_fault == DROP_REPLACE_SPACE) {
		int fork = drop_fault == DROP_REPLACE_MAIN ? MAIN_FORKNUM : SPACE_FORKNUM;

		snprintf(moved, sizeof(moved), "%s.old", drop_paths[fork]);
		if (rename(drop_paths[fork], moved) != 0)
			abort();
		write_drop_file(drop_paths[fork], page.data, BLCKSZ);
	}
	if (drop_fault == DROP_CHANGE_IDENTITY) {
		ClusterSpaceIdentity newer = drop_identity;

		newer.incarnation[0]++;
		if (!cluster_space_identity_page_encode(&newer, 17, page.data, BLCKSZ))
			abort();
		changed = open(drop_paths[SPACE_FORKNUM], O_RDWR);
		if (changed < 0 || pwrite(changed, page.data, BLCKSZ, 0) != BLCKSZ)
			abort();
		close(changed);
	}
	if (drop_fault == DROP_NEW_OPTIONAL)
		write_drop_file(drop_paths[INIT_FORKNUM], page.data, BLCKSZ);
	return fsync(fd);
}

static int
drop_unlinkat(int directory, const char *name, int flags)
{
	drop_unlinks++;
	if (drop_main_syncs != 1)
		abort();
	if (drop_fault == DROP_AUX_UNLINK
		|| (drop_fault == DROP_PARTIAL && strstr(name, "_vm") != NULL)) {
		errno = EIO;
		return -1;
	}
	if (drop_fault == DROP_UNLINK_ENOENT) {
		if (unlinkat(directory, name, flags) != 0)
			abort();
		errno = ENOENT;
		return -1;
	}
	return unlinkat(directory, name, flags);
}

#define ftruncate drop_ftruncate
#define pg_fsync drop_fsync
#define unlinkat drop_unlinkat
#include "../../backend/cluster/storage/cluster_shared_fs_sharedfs.c"
#undef ftruncate
#undef pg_fsync
#undef unlinkat

static void
drop_setup(const char *tag, bool optional_init)
{
	PGAlignedBlock page;
	ForkNumber fork;

	fresh_root(tag);
	memset(&drop_identity, 0, sizeof(drop_identity));
	drop_identity.key.system_identifier = 71;
	drop_identity.key.database_incarnation = 13;
	memset(drop_identity.key.storage_uuid, 0x21, 16);
	drop_identity.key.locator = (RelFileLocator){1663, 5, 16384};
	memset(drop_identity.incarnation, 0x31, 16);
	drop_identity.sequence = 2;
	drop_identity.operation = 8;
	drop_identity.state = CLUSTER_SPACE_IDENTITY_TOMBSTONED;
	if (!cluster_space_identity_page_encode(&drop_identity, 17, page.data, BLCKSZ))
		abort();
	snprintf(drop_directory, sizeof(drop_directory), "%s/base/5", test_root);
	if (pg_mkdir_p(drop_directory, 0700) != 0)
		abort();
	for (fork = 0; fork <= MAX_FORKNUM; fork++) {
		char *path = cluster_shared_fs_sharedfs_relpath(drop_identity.key.locator, fork);

		snprintf(drop_paths[fork], sizeof(drop_paths[fork]), "%s", path);
		pfree(path);
		if (optional_init || fork != INIT_FORKNUM)
			write_drop_file(drop_paths[fork], page.data, BLCKSZ);
	}
	drop_fault = DROP_OK;
	drop_main_syncs = drop_directory_syncs = drop_unlinks = drop_truncates = 0;
	enableFsync = true;
}

UT_TEST(test_drop_durable_all_forks)
{
	struct stat before, after;
	ForkNumber fork;

	drop_setup("durable_all", true);
	UT_ASSERT_EQ(stat(drop_paths[MAIN_FORKNUM], &before), 0);
	UT_ASSERT(cluster_shared_fs_sharedfs_drop_durable(&drop_identity, 17));
	UT_ASSERT_EQ(stat(drop_paths[MAIN_FORKNUM], &after), 0);
	UT_ASSERT_EQ(before.st_ino, after.st_ino);
	UT_ASSERT_EQ(after.st_size, 0);
	UT_ASSERT_EQ(drop_main_syncs, 1);
	UT_ASSERT_EQ(drop_directory_syncs, 1);
	UT_ASSERT_EQ(drop_unlinks, MAX_FORKNUM);
	for (fork = 1; fork <= MAX_FORKNUM; fork++)
		UT_ASSERT(access(drop_paths[fork], F_OK) != 0 && errno == ENOENT);
}

UT_TEST(test_drop_absent_optional_is_not_a_failed_unlink)
{
	drop_setup("durable_optional", false);
	UT_ASSERT(cluster_shared_fs_sharedfs_drop_durable(&drop_identity, 17));
	UT_ASSERT_EQ(drop_unlinks, MAX_FORKNUM - 1);
	UT_ASSERT_EQ(drop_directory_syncs, 1);
}

#define FAULT_TEST(name, fault) \
	UT_TEST(name) { \
		drop_setup(#name, fault != DROP_NEW_OPTIONAL); \
		drop_fault = fault; \
		UT_ASSERT(!cluster_shared_fs_sharedfs_drop_durable(&drop_identity, 17)); \
		UT_ASSERT(drop_truncates == 1); \
	}
FAULT_TEST(test_drop_truncate_failure, DROP_TRUNCATE)
FAULT_TEST(test_drop_main_sync_failure, DROP_MAIN_SYNC)
FAULT_TEST(test_drop_aux_unlink_failure, DROP_AUX_UNLINK)
FAULT_TEST(test_drop_partial_failure, DROP_PARTIAL)
FAULT_TEST(test_drop_directory_sync_failure, DROP_DIRECTORY_SYNC)
FAULT_TEST(test_drop_unlink_enoent_is_not_success, DROP_UNLINK_ENOENT)
FAULT_TEST(test_drop_replaced_main, DROP_REPLACE_MAIN)
FAULT_TEST(test_drop_replaced_space, DROP_REPLACE_SPACE)
FAULT_TEST(test_drop_changed_space_identity, DROP_CHANGE_IDENTITY)
FAULT_TEST(test_drop_new_optional_fork, DROP_NEW_OPTIONAL)
FAULT_TEST(test_drop_replaced_directory, DROP_REPLACE_DIRECTORY)

UT_TEST(test_drop_missing_required_forks)
{
	drop_setup("missing_main", true);
	UT_ASSERT_EQ(unlink(drop_paths[MAIN_FORKNUM]), 0);
	UT_ASSERT(!cluster_shared_fs_sharedfs_drop_durable(&drop_identity, 17));
	UT_ASSERT_EQ(drop_truncates, 0);
	drop_setup("missing_space", true);
	UT_ASSERT_EQ(unlink(drop_paths[SPACE_FORKNUM]), 0);
	UT_ASSERT(!cluster_shared_fs_sharedfs_drop_durable(&drop_identity, 17));
	UT_ASSERT_EQ(drop_truncates, 0);
}

UT_TEST(test_drop_identity_or_token_mismatch)
{
	ClusterSpaceIdentity wrong;

	drop_setup("wrong_identity", true);
	wrong = drop_identity;
	wrong.incarnation[0]++;
	UT_ASSERT(!cluster_shared_fs_sharedfs_drop_durable(&wrong, 17));
	UT_ASSERT(!cluster_shared_fs_sharedfs_drop_durable(&drop_identity, 18));
	wrong = drop_identity;
	wrong.state = CLUSTER_SPACE_IDENTITY_LIVE;
	UT_ASSERT(!cluster_shared_fs_sharedfs_drop_durable(&wrong, 17));
	UT_ASSERT(!cluster_shared_fs_sharedfs_drop_durable(NULL, 17));
	UT_ASSERT_EQ(drop_truncates, 0);
}

UT_TEST(test_drop_disabled_fsync_cannot_prove_durability)
{
	drop_setup("disabled_fsync", true);
	enableFsync = false;
	UT_ASSERT(!cluster_shared_fs_sharedfs_drop_durable(&drop_identity, 17));
	UT_ASSERT_EQ(drop_truncates, 0);
	enableFsync = true;
}

int
main(void)
{
	UT_PLAN(16);
	UT_RUN(test_drop_durable_all_forks);
	UT_RUN(test_drop_absent_optional_is_not_a_failed_unlink);
	UT_RUN(test_drop_truncate_failure);
	UT_RUN(test_drop_main_sync_failure);
	UT_RUN(test_drop_aux_unlink_failure);
	UT_RUN(test_drop_partial_failure);
	UT_RUN(test_drop_directory_sync_failure);
	UT_RUN(test_drop_unlink_enoent_is_not_success);
	UT_RUN(test_drop_replaced_main);
	UT_RUN(test_drop_replaced_space);
	UT_RUN(test_drop_changed_space_identity);
	UT_RUN(test_drop_new_optional_fork);
	UT_RUN(test_drop_replaced_directory);
	UT_RUN(test_drop_missing_required_forks);
	UT_RUN(test_drop_identity_or_token_mismatch);
	UT_RUN(test_drop_disabled_fsync_cannot_prove_durability);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
