/* Cold recovery namespace discovery through real directory/file descriptors.
 * Author: SqlRush <sqlrush@gmail.com>
 * Copyright (c) 2026, pgrac contributors. */
#include "postgres.h"
#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include "catalog/pg_tablespace_d.h"
#include "cluster/cluster_recovery_namespace.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "%s at %s:%d\n", condition, file, line);
	abort();
}

static bool inject_readdir_error;
static bool inject_fstat_error;
static unsigned inject_pread_fault;
static bool inject_read_corruption;
static int modify_during_read = -1;

static ssize_t
namespace_test_pread(int fd, void *buffer, size_t length, off_t offset)
{
	ssize_t result;

	if (inject_pread_fault == 1) {
		errno = EIO;
		return -1;
	}
	result = pread(fd, buffer, inject_pread_fault == 2 ? length - 1 : length, offset);
	if (inject_read_corruption && result > 0)
		((uint8 *)buffer)[result - 1] ^= 1;
	if (modify_during_read >= 0 && ftruncate(modify_during_read, 1) != 0)
		abort();
	return result;
}

static int
namespace_test_fstat(int fd, struct stat *st)
{
	if (inject_fstat_error) {
		errno = EIO;
		return -1;
	}
	return fstat(fd, st);
}

static struct dirent *
namespace_test_readdir(DIR *dir)
{
	if (inject_readdir_error) {
		errno = EIO;
		return NULL;
	}
	return readdir(dir);
}

#define readdir namespace_test_readdir
#define fstat namespace_test_fstat
#define pread namespace_test_pread
#include "../../backend/cluster/cluster_recovery_namespace.c"
#undef readdir
#undef fstat
#undef pread

static char test_path[MAXPGPATH];

static int
new_root(void)
{
	char *dir;

	snprintf(test_path, sizeof(test_path), "/tmp/pgrac-recovery-ns-XXXXXX");
	dir = mkdtemp(test_path);
	return dir == NULL ? -1 : open(dir, O_RDONLY | O_DIRECTORY);
}

static void
write_file(int root, const char *name)
{
	int fd = openat(root, name, O_CREAT | O_EXCL | O_WRONLY, 0600);

	if (fd < 0 || write(fd, "cold", 4) != 4 || close(fd) != 0)
		abort();
}

static void
remove_root(int root)
{
	DIR *dir = fdopendir(openat(root, ".", O_RDONLY | O_DIRECTORY));
	struct dirent *entry;

	while ((entry = readdir(dir)) != NULL) {
		struct stat st;

		if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
			continue;
		if (fstatat(root, entry->d_name, &st, AT_SYMLINK_NOFOLLOW) != 0)
			abort();
		if (S_ISDIR(st.st_mode)) {
			int child = openat(root, entry->d_name, O_RDONLY | O_DIRECTORY | O_NOFOLLOW);

			remove_root(child);
			if (unlinkat(root, entry->d_name, AT_REMOVEDIR) != 0)
				abort();
		} else if (unlinkat(root, entry->d_name, 0) != 0)
			abort();
	}
	closedir(dir);
	close(root);
}

UT_TEST(test_shared_relation_names)
{
	const char *paths[] = { "global/1260", "base/5/16385", "base/5/16385_fsm", "base/5/16385_vm",
							"base/5/16385_space" };
	ForkNumber forks[]
		= { MAIN_FORKNUM, MAIN_FORKNUM, FSM_FORKNUM, VISIBILITYMAP_FORKNUM, SPACE_FORKNUM };

	for (unsigned i = 0; i < lengthof(paths); i++) {
		RelFileLocator locator;
		ForkNumber forknum;

		UT_ASSERT_EQ(cluster_recovery_namespace_classify(paths[i], &locator, &forknum),
					 CLUSTER_RECOVERY_FILE_RELATION);
		UT_ASSERT_EQ(locator.spcOid, i == 0 ? GLOBALTABLESPACE_OID : DEFAULTTABLESPACE_OID);
		UT_ASSERT_EQ(locator.dbOid, i == 0 ? InvalidOid : 5);
		UT_ASSERT_EQ(locator.relNumber, i == 0 ? 1260 : 16385);
		UT_ASSERT_EQ(forknum, forks[i]);
	}
}

/* Each truncated prefix is its own exact-sized allocation, rather than a
 * pointer into a longer literal whose backing object hides short inputs. */
UT_TEST(test_native_slru_short_paths_never_form_a_segment_key)
{
	const char *prefixes[] = { "pg_xact/", "pg_subtrans/", "pg_commit_ts/", "pg_multixact/offsets/",
							   "pg_multixact/members/" };

	for (unsigned i = 0; i < lengthof(prefixes); i++) {
		for (size_t n = 0; n <= strlen(prefixes[i]); n++) {
			char *path = malloc(n + 1);
			RelFileLocator locator;
			ForkNumber forknum;
			uint8 family = 0xa5;
			uint32 segment = UINT32_MAX;

			if (path == NULL)
				abort();
			memcpy(path, prefixes[i], n);
			path[n] = '\0';
			UT_ASSERT(!namespace_native_key(path, &family, &segment));
			UT_ASSERT_EQ(family, 0xa5);
			UT_ASSERT_EQ(segment, UINT32_MAX);
			UT_ASSERT_EQ(
				cluster_recovery_namespace_classify_ex(path, &locator, &forknum, &family, &segment),
				CLUSTER_RECOVERY_FILE_UNCLASSIFIED);
			UT_ASSERT_EQ(family, 0);
			UT_ASSERT_EQ(segment, UINT32_MAX);
			free(path);
		}
	}
}

UT_TEST(test_native_slru_segment_names_are_explicit_obligations)
{
	struct NativeName {
		const char *path;
		uint8 family;
		uint32 segment;
	};
	static const struct NativeName valid[] = {
		{ "pg_xact/0000", CLUSTER_RECOVERY_NATIVE_FAMILY_XACT, 0 },
		{ "pg_subtrans/00AF", CLUSTER_RECOVERY_NATIVE_FAMILY_SUBTRANS, 0xAF },
		{ "pg_commit_ts/FFFF", CLUSTER_RECOVERY_NATIVE_FAMILY_COMMIT_TS, 0xFFFF },
		{ "pg_multixact/offsets/0001", CLUSTER_RECOVERY_NATIVE_FAMILY_MX_OFFSETS, 1 },
		{ "pg_multixact/members/00F0", CLUSTER_RECOVERY_NATIVE_FAMILY_MX_MEMBERS, 0xF0 },
	};
	static const char *const invalid[] = {
		"pg_xact/000",
		"pg_xact/000a",
		"pg_xact/0000.bak",
		"pg_subtrans/00000",
		"pg_commit_ts/0fff",
		"pg_multixact/0000",
		"pg_multixact/offsets/0000/extra",
		"native_side/origin_128/pg_xact/0000",
	};

	for (unsigned i = 0; i < lengthof(valid); i++) {
		RelFileLocator locator;
		ForkNumber forknum;
		uint8 family = 0;
		uint32 segment = 0;

		UT_ASSERT_EQ(cluster_recovery_namespace_classify_ex(valid[i].path, &locator, &forknum,
															&family, &segment),
					 CLUSTER_RECOVERY_FILE_NATIVE_SLRU);
		UT_ASSERT_EQ(family, valid[i].family);
		UT_ASSERT_EQ(segment, valid[i].segment);
		UT_ASSERT_EQ(locator.spcOid, InvalidOid);
		UT_ASSERT_EQ(forknum, InvalidForkNumber);
	}
	for (unsigned i = 0; i < lengthof(invalid); i++) {
		RelFileLocator locator;
		ForkNumber forknum;
		uint8 family = 0xa5;
		uint32 segment = UINT32_MAX;

		UT_ASSERT_EQ(cluster_recovery_namespace_classify_ex(invalid[i], &locator, &forknum, &family,
															&segment),
					 CLUSTER_RECOVERY_FILE_UNCLASSIFIED);
		UT_ASSERT_EQ(family, 0);
		UT_ASSERT_EQ(segment, UINT32_MAX);
	}
}

UT_TEST(test_twophase_state_names_are_explicit_obligations)
{
	static const char *const valid[] = {
		"pg_twophase/00000001",
		"pg_twophase/7FFFFFFF",
		"pg_twophase/FFFFFFFF",
	};
	static const char *const invalid[] = {
		"pg_twophase/0000001",		  "pg_twophase/0000000a",  "pg_twophase/00000001.crc",
		"pg_twophase/00000001/extra", "pg_twophase/000000000", "pg_twophase/FFFFFFFF.tmp",
	};

	for (unsigned i = 0; i < lengthof(valid); i++) {
		RelFileLocator locator;
		ForkNumber forknum;

		UT_ASSERT_EQ(cluster_recovery_namespace_classify(valid[i], &locator, &forknum),
					 CLUSTER_RECOVERY_FILE_TWOPHASE);
		UT_ASSERT_EQ(locator.spcOid, InvalidOid);
		UT_ASSERT_EQ(forknum, InvalidForkNumber);
	}
	for (unsigned i = 0; i < lengthof(invalid); i++) {
		RelFileLocator locator;
		ForkNumber forknum;

		UT_ASSERT_EQ(cluster_recovery_namespace_classify(invalid[i], &locator, &forknum),
					 CLUSTER_RECOVERY_FILE_UNCLASSIFIED);
	}
}

UT_TEST(test_real_twophase_entries_carry_xid)
{
	int root = new_root();
	ClusterRecoveryNamespace *scan = NULL;
	ClusterRecoveryNamespaceEntry entry;
	ClusterRecoveryNamespaceResult result;
	int borrowed = -1;
	bool found = false;

	UT_ASSERT(root >= 0);
	UT_ASSERT_EQ(mkdirat(root, "pg_twophase", 0700), 0);
	write_file(root, "pg_twophase/00ABCDEF");
	UT_ASSERT_EQ(cluster_recovery_namespace_begin(root, 16, &scan),
				 CLUSTER_RECOVERY_NAMESPACE_MORE);
	while ((result = cluster_recovery_namespace_next(scan, &entry, &borrowed))
		   <= CLUSTER_RECOVERY_NAMESPACE_MORE) {
		if (result == CLUSTER_RECOVERY_NAMESPACE_MORE
			|| entry.kind == CLUSTER_RECOVERY_FILE_DIRECTORY)
			continue;
		if (strcmp(entry.path, "pg_twophase/00ABCDEF") == 0) {
			UT_ASSERT_EQ(entry.kind, CLUSTER_RECOVERY_FILE_TWOPHASE);
			UT_ASSERT_EQ(entry.twophase_xid, UINT32_C(0x00ABCDEF));
			found = true;
		}
	}
	UT_ASSERT_EQ(result, CLUSTER_RECOVERY_NAMESPACE_END);
	UT_ASSERT(found);
	cluster_recovery_namespace_destroy(scan);
	remove_root(root);
	rmdir(test_path);
}

UT_TEST(test_real_native_slru_entries_carry_family_and_segment)
{
	int root = new_root();
	ClusterRecoveryNamespace *scan = NULL;
	ClusterRecoveryNamespaceEntry entry;
	ClusterRecoveryNamespaceResult result;
	int borrowed = -1;
	bool found = false;

	UT_ASSERT(root >= 0);
	UT_ASSERT_EQ(mkdirat(root, "pg_multixact", 0700), 0);
	UT_ASSERT_EQ(mkdirat(root, "pg_multixact/offsets", 0700), 0);
	write_file(root, "pg_multixact/offsets/00F0");
	UT_ASSERT_EQ(cluster_recovery_namespace_begin(root, 16, &scan),
				 CLUSTER_RECOVERY_NAMESPACE_MORE);
	while ((result = cluster_recovery_namespace_next(scan, &entry, &borrowed))
		   <= CLUSTER_RECOVERY_NAMESPACE_MORE) {
		if (result == CLUSTER_RECOVERY_NAMESPACE_MORE
			|| entry.kind == CLUSTER_RECOVERY_FILE_DIRECTORY)
			continue;
		if (strcmp(entry.path, "pg_multixact/offsets/00F0") == 0) {
			UT_ASSERT_EQ(entry.kind, CLUSTER_RECOVERY_FILE_NATIVE_SLRU);
			UT_ASSERT_EQ(entry.native_family, CLUSTER_RECOVERY_NATIVE_FAMILY_MX_OFFSETS);
			UT_ASSERT_EQ(entry.native_segment, 0xF0);
			found = true;
		}
	}
	UT_ASSERT_EQ(result, CLUSTER_RECOVERY_NAMESPACE_END);
	UT_ASSERT(found);
	cluster_recovery_namespace_destroy(scan);
	remove_root(root);
	rmdir(test_path);
}

UT_TEST(test_relmap_and_both_authority_copies)
{
	const char *names[] = { "pg_filenode.map", "pg_filenode.map.bak", "pgrac_relmap_authority",
							"pgrac_relmap_authority.bak" };
	ClusterRecoveryFileKind kinds[]
		= { CLUSTER_RECOVERY_FILE_RELMAP, CLUSTER_RECOVERY_FILE_RELMAP_BACKUP,
			CLUSTER_RECOVERY_FILE_RELMAP_AUTHORITY, CLUSTER_RECOVERY_FILE_RELMAP_AUTHORITY_BACKUP };

	for (unsigned i = 0; i < lengthof(names); i++) {
		char path[128];
		RelFileLocator locator;
		ForkNumber forknum;

		snprintf(path, sizeof(path), "base/5/%s", names[i]);
		UT_ASSERT_EQ(cluster_recovery_namespace_classify(path, &locator, &forknum), kinds[i]);
		UT_ASSERT_EQ(locator.dbOid, 5);
		UT_ASSERT_EQ(locator.relNumber, InvalidOid);
		UT_ASSERT_EQ(forknum, InvalidForkNumber);
	}
}

UT_TEST(test_unknown_and_aliases_cannot_be_empty_coverage)
{
	const char *paths[] = { "base/05/100",
							"base/0/100",
							"base/5/0100",
							"base/5/0",
							"base/4294967296/100",
							"base/5/4294967296",
							"base/5/100.1",
							"base/5/100_init",
							"base/5/100_space.1",
							"base/5/100_vm.bak",
							"base/5/t1_100",
							"global/1/100",
							"pg_tblspc/1/PG_16/5/100",
							"/base/5/100",
							"base/5/../100",
							"base/5//100",
							"base/5/pgrac_relmap_authority.tmp",
							"base/5/pg_filenode.map.bak.tmp",
							"unknown" };

	for (unsigned i = 0; i < lengthof(paths); i++) {
		RelFileLocator locator;
		ForkNumber forknum;

		memset(&locator, 0x5a, sizeof(locator));
		UT_ASSERT_EQ(cluster_recovery_namespace_classify(paths[i], &locator, &forknum),
					 CLUSTER_RECOVERY_FILE_UNCLASSIFIED);
		UT_ASSERT_EQ(locator.spcOid, InvalidOid);
		UT_ASSERT_EQ(locator.dbOid, InvalidOid);
		UT_ASSERT_EQ(locator.relNumber, InvalidOid);
		UT_ASSERT_EQ(forknum, InvalidForkNumber);
	}
}

UT_TEST(test_undo_names_keep_origin_and_reject_aliases)
{
	const char *valid[]
		= { "pg_undo/instance_0/seg_1.dat", "pg_undo/instance_0/seg_256.dat",
			"pg_undo/instance_1/seg_257.dat", "pg_undo/instance_127/seg_32768.dat" };
	const char *unknown[]
		= { "pg_undo/instance_00/seg_1.dat",		  "pg_undo/instance_0/seg_01.dat",
			"pg_undo/instance_0/seg_0.dat",			  "pg_undo/instance_0/seg_257.dat",
			"pg_undo/instance_1/seg_256.dat",		  "pg_undo/instance_128/seg_32769.dat",
			"pg_undo/instance_42949672960/seg_1.dat", "pg_undo/instance_0/seg_42949672960.dat",
			"pg_undo/instance_0/seg_1.dat.bak",		  "pg_undo/instance_0/seg_1.dat/extra",
			"pg_undo/instance_-1/seg_1.dat",		  "other/instance_0/seg_1.dat" };
	RelFileLocator locator;
	ForkNumber forknum;

	for (unsigned i = 0; i < lengthof(valid); i++) {
		UT_ASSERT_EQ(cluster_recovery_namespace_classify(valid[i], &locator, &forknum),
					 CLUSTER_RECOVERY_FILE_UNDO_SEGMENT);
		UT_ASSERT_EQ(locator.spcOid, 0);
		UT_ASSERT_EQ(forknum, InvalidForkNumber);
	}
	for (unsigned i = 0; i < lengthof(unknown); i++)
		UT_ASSERT_EQ(cluster_recovery_namespace_classify(unknown[i], &locator, &forknum),
					 CLUSTER_RECOVERY_FILE_UNCLASSIFIED);
}

UT_TEST(test_real_undo_files_export_physical_keys_without_a_live_bitmap)
{
	int root = new_root();
	ClusterRecoveryNamespace *scan = NULL;
	ClusterRecoveryNamespaceEntry entry;
	ClusterRecoveryNamespaceResult result;
	unsigned files = 0;
	int borrowed = -1;

	UT_ASSERT(root >= 0);
	UT_ASSERT_EQ(mkdirat(root, "pg_undo", 0700), 0);
	UT_ASSERT_EQ(mkdirat(root, "pg_undo/instance_1", 0700), 0);
	write_file(root, "pg_undo/instance_1/seg_257.dat");
	write_file(root, "pg_undo/instance_1/seg_512.dat");
	UT_ASSERT_EQ(cluster_recovery_namespace_begin(root, 16, &scan),
				 CLUSTER_RECOVERY_NAMESPACE_MORE);
	while ((result = cluster_recovery_namespace_next(scan, &entry, &borrowed))
		   <= CLUSTER_RECOVERY_NAMESPACE_MORE) {
		if (result == CLUSTER_RECOVERY_NAMESPACE_MORE
			|| entry.kind == CLUSTER_RECOVERY_FILE_DIRECTORY)
			continue;
		UT_ASSERT_EQ(entry.kind, CLUSTER_RECOVERY_FILE_UNDO_SEGMENT);
		UT_ASSERT_EQ(entry.undo_owner_instance, 2);
		UT_ASSERT_EQ(entry.undo_segment,
					 strcmp(entry.path, "pg_undo/instance_1/seg_257.dat") == 0 ? 257 : 512);
		UT_ASSERT(borrowed >= 0);
		files++;
	}
	UT_ASSERT_EQ(result, CLUSTER_RECOVERY_NAMESPACE_END);
	UT_ASSERT_EQ(files, 2);
	cluster_recovery_namespace_destroy(scan);
	remove_root(root);
	rmdir(test_path);
}

UT_TEST(test_real_tree_includes_untouched_and_unknown_files)
{
	int root = new_root();
	ClusterRecoveryNamespace *scan = NULL;
	ClusterRecoveryNamespaceEntry entry;
	ClusterRecoveryNamespaceResult result;
	unsigned files = 0, directories = 0, unknown = 0;
	int borrowed = -1;
	char bytes[4];

	UT_ASSERT(root >= 0);
	UT_ASSERT_EQ(mkdirat(root, "global", 0700), 0);
	write_file(root, "global/1260");
	write_file(root, "global/1260_vm");
	write_file(root, "global/pgrac_relmap_authority.bak");
	write_file(root, "unclassified-owner");
	UT_ASSERT_EQ(cluster_recovery_namespace_begin(root, 16, &scan),
				 CLUSTER_RECOVERY_NAMESPACE_MORE);
	while ((result = cluster_recovery_namespace_next(scan, &entry, &borrowed))
		   <= CLUSTER_RECOVERY_NAMESPACE_MORE) {
		if (result == CLUSTER_RECOVERY_NAMESPACE_MORE)
			continue;
		if (entry.kind == CLUSTER_RECOVERY_FILE_DIRECTORY) {
			directories++;
			UT_ASSERT_EQ(borrowed, -1);
		} else {
			files++;
			unknown += entry.kind == CLUSTER_RECOVERY_FILE_UNCLASSIFIED;
			UT_ASSERT_EQ(pread(borrowed, bytes, sizeof(bytes), 0), 4);
			UT_ASSERT_EQ(memcmp(bytes, "cold", 4), 0);
		}
	}
	UT_ASSERT_EQ(result, CLUSTER_RECOVERY_NAMESPACE_END);
	UT_ASSERT_EQ(files, 4);
	UT_ASSERT_EQ(directories, 1);
	UT_ASSERT_EQ(unknown, 1);
	cluster_recovery_namespace_destroy(scan);
	UT_ASSERT(fcntl(root, F_GETFD) >= 0); /* Borrowed root stays with its owner. */
	remove_root(root);
	rmdir(test_path);
}

UT_TEST(test_symlink_and_hardlink_are_not_candidates)
{
	for (unsigned hard = 0; hard < 2; hard++) {
		int root = new_root();
		ClusterRecoveryNamespace *scan = NULL;
		ClusterRecoveryNamespaceEntry entry;
		ClusterRecoveryNamespaceResult result;
		int fd;

		UT_ASSERT(root >= 0);
		if (hard) {
			write_file(root, "original");
			UT_ASSERT_EQ(linkat(root, "original", root, "alias", 0), 0);
		} else
			UT_ASSERT_EQ(symlinkat("/tmp", root, "escape"), 0);
		UT_ASSERT_EQ(cluster_recovery_namespace_begin(root, 8, &scan),
					 CLUSTER_RECOVERY_NAMESPACE_MORE);
		do {
			result = cluster_recovery_namespace_next(scan, &entry, &fd);
		} while (result <= CLUSTER_RECOVERY_NAMESPACE_MORE);
		UT_ASSERT_EQ(result, CLUSTER_RECOVERY_NAMESPACE_UNSAFE);
		UT_ASSERT_EQ(cluster_recovery_namespace_next(scan, &entry, &fd), result);
		cluster_recovery_namespace_destroy(scan);
		remove_root(root);
		rmdir(test_path);
	}
}

UT_TEST(test_read_error_is_not_eof)
{
	int root = new_root(), fd;
	ClusterRecoveryNamespace *scan = NULL;
	ClusterRecoveryNamespaceEntry entry;

	UT_ASSERT_EQ(cluster_recovery_namespace_begin(root, 8, &scan), CLUSTER_RECOVERY_NAMESPACE_MORE);
	inject_readdir_error = true;
	UT_ASSERT_EQ(cluster_recovery_namespace_next(scan, &entry, &fd), CLUSTER_RECOVERY_NAMESPACE_IO);
	inject_readdir_error = false;
	UT_ASSERT_EQ(cluster_recovery_namespace_next(scan, &entry, &fd), CLUSTER_RECOVERY_NAMESPACE_IO);
	cluster_recovery_namespace_destroy(scan);
	remove_root(root);
	rmdir(test_path);
}

UT_TEST(test_disappeared_or_modified_file_invalidates_observation)
{
	for (unsigned modified = 0; modified < 2; modified++) {
		int root = new_root(), fd;
		ClusterRecoveryNamespace *scan = NULL;
		ClusterRecoveryNamespaceEntry entry;

		write_file(root, "candidate");
		UT_ASSERT_EQ(cluster_recovery_namespace_begin(root, 8, &scan),
					 CLUSTER_RECOVERY_NAMESPACE_MORE);
		UT_ASSERT_EQ(cluster_recovery_namespace_next(scan, &entry, &fd),
					 CLUSTER_RECOVERY_NAMESPACE_ENTRY);
		if (modified) {
			int writer = openat(root, "candidate", O_WRONLY);
			UT_ASSERT_EQ(ftruncate(writer, 3), 0);
			close(writer);
		} else
			UT_ASSERT_EQ(unlinkat(root, "candidate", 0), 0);
		UT_ASSERT_EQ(cluster_recovery_namespace_next(scan, &entry, &fd),
					 CLUSTER_RECOVERY_NAMESPACE_CHANGED);
		UT_ASSERT_EQ(fd, -1);
		UT_ASSERT_EQ(entry.path[0], 0);
		cluster_recovery_namespace_destroy(scan);
		remove_root(root);
		rmdir(test_path);
	}
}

UT_TEST(test_capacity_never_seals_a_prefix)
{
	int root = new_root(), fd;
	ClusterRecoveryNamespace *scan = NULL;
	ClusterRecoveryNamespaceEntry entry;

	write_file(root, "one");
	write_file(root, "two");
	UT_ASSERT_EQ(cluster_recovery_namespace_begin(root, 1, &scan), CLUSTER_RECOVERY_NAMESPACE_MORE);
	UT_ASSERT_EQ(cluster_recovery_namespace_next(scan, &entry, &fd),
				 CLUSTER_RECOVERY_NAMESPACE_ENTRY);
	UT_ASSERT_EQ(cluster_recovery_namespace_next(scan, &entry, &fd),
				 CLUSTER_RECOVERY_NAMESPACE_CAPACITY);
	cluster_recovery_namespace_destroy(scan);
	remove_root(root);
	rmdir(test_path);
}

UT_TEST(test_new_directory_entry_invalidates_finished_set)
{
	int root = new_root(), fd;
	ClusterRecoveryNamespace *scan = NULL;
	ClusterRecoveryNamespaceEntry entry;
	ClusterRecoveryNamespaceResult result;

	write_file(root, "one");
	UT_ASSERT_EQ(cluster_recovery_namespace_begin(root, 8, &scan), CLUSTER_RECOVERY_NAMESPACE_MORE);
	UT_ASSERT_EQ(cluster_recovery_namespace_next(scan, &entry, &fd),
				 CLUSTER_RECOVERY_NAMESPACE_ENTRY);
	write_file(root, "two");
	do {
		result = cluster_recovery_namespace_next(scan, &entry, &fd);
	} while (result <= CLUSTER_RECOVERY_NAMESPACE_MORE);
	UT_ASSERT_EQ(result, CLUSTER_RECOVERY_NAMESPACE_CHANGED);
	cluster_recovery_namespace_destroy(scan);
	remove_root(root);
	rmdir(test_path);
}

UT_TEST(test_stat_io_failure_keeps_its_cause)
{
	for (unsigned directory = 0; directory < 2; directory++) {
		int root = new_root(), fd;
		ClusterRecoveryNamespace *scan = NULL;
		ClusterRecoveryNamespaceEntry entry;

		if (directory)
			UT_ASSERT_EQ(mkdirat(root, "candidate", 0700), 0);
		else
			write_file(root, "candidate");
		UT_ASSERT_EQ(cluster_recovery_namespace_begin(root, 8, &scan),
					 CLUSTER_RECOVERY_NAMESPACE_MORE);
		inject_fstat_error = true;
		UT_ASSERT_EQ(cluster_recovery_namespace_next(scan, &entry, &fd),
					 CLUSTER_RECOVERY_NAMESPACE_IO);
		inject_fstat_error = false;
		UT_ASSERT_EQ(cluster_recovery_namespace_next(scan, &entry, &fd),
					 CLUSTER_RECOVERY_NAMESPACE_IO);
		cluster_recovery_namespace_destroy(scan);
		remove_root(root);
		rmdir(test_path);
	}
}

UT_TEST(test_borrowed_directory_cursor_does_not_skip_files)
{
	int root = new_root(), fd;
	DIR *prior;
	ClusterRecoveryNamespace *scan = NULL;
	ClusterRecoveryNamespaceEntry entry;

	write_file(root, "candidate");
	prior = fdopendir(dup(root));
	UT_ASSERT(prior != NULL);
	while (readdir(prior) != NULL)
		;
	closedir(prior);
	UT_ASSERT_EQ(cluster_recovery_namespace_begin(root, 8, &scan), CLUSTER_RECOVERY_NAMESPACE_MORE);
	UT_ASSERT_EQ(cluster_recovery_namespace_next(scan, &entry, &fd),
				 CLUSTER_RECOVERY_NAMESPACE_ENTRY);
	UT_ASSERT_EQ(strcmp(entry.path, "candidate"), 0);
	UT_ASSERT_EQ(cluster_recovery_namespace_next(scan, &entry, &fd),
				 CLUSTER_RECOVERY_NAMESPACE_END);
	UT_ASSERT_EQ(cluster_recovery_namespace_next(scan, &entry, &fd),
				 CLUSTER_RECOVERY_NAMESPACE_END);
	cluster_recovery_namespace_destroy(scan);
	remove_root(root);
	rmdir(test_path);
}

UT_TEST(test_current_file_reads_all_pages_without_moving_fd_cursor)
{
	int root = new_root(), fd, writer;
	ClusterRecoveryNamespace *scan = NULL;
	ClusterRecoveryNamespaceEntry entry;
	char expected[BLCKSZ], observed[BLCKSZ];

	writer = openat(root, "untouched", O_CREAT | O_EXCL | O_WRONLY, 0600);
	UT_ASSERT(writer >= 0);
	for (unsigned page = 0; page < 2; page++) {
		memset(expected, page + 17, sizeof(expected));
		UT_ASSERT_EQ(write(writer, expected, sizeof(expected)), sizeof(expected));
	}
	close(writer);
	UT_ASSERT_EQ(cluster_recovery_namespace_begin(root, 8, &scan), CLUSTER_RECOVERY_NAMESPACE_MORE);
	UT_ASSERT_EQ(cluster_recovery_namespace_next(scan, &entry, &fd),
				 CLUSTER_RECOVERY_NAMESPACE_ENTRY);
	UT_ASSERT_EQ(entry.size, 2 * BLCKSZ);
	UT_ASSERT_EQ(lseek(fd, 3, SEEK_SET), 3);
	for (unsigned page = 0; page < 2; page++) {
		memset(expected, page + 17, sizeof(expected));
		UT_ASSERT_EQ(cluster_recovery_namespace_read_current(scan, (uint64)page * BLCKSZ, observed,
															 sizeof(observed)),
					 CLUSTER_RECOVERY_NAMESPACE_ENTRY);
		UT_ASSERT_EQ(memcmp(expected, observed, sizeof(expected)), 0);
		UT_ASSERT_EQ(lseek(fd, 0, SEEK_CUR), 3);
	}
	UT_ASSERT_EQ(cluster_recovery_namespace_next(scan, &entry, &fd),
				 CLUSTER_RECOVERY_NAMESPACE_END);
	cluster_recovery_namespace_destroy(scan);
	remove_root(root);
	rmdir(test_path);
}

UT_TEST(test_current_read_short_io_and_changed_are_not_empty_pages)
{
	for (unsigned fault = 1; fault <= 4; fault++) {
		int root = new_root(), fd, writer = -1;
		ClusterRecoveryNamespace *scan = NULL;
		ClusterRecoveryNamespaceEntry entry;
		char observed[4] = { 0x5a, 0x5a, 0x5a, 0x5a };
		char zero[4] = { 0 };
		ClusterRecoveryNamespaceResult expected
			= fault <= 2 ? CLUSTER_RECOVERY_NAMESPACE_IO : CLUSTER_RECOVERY_NAMESPACE_CHANGED;

		write_file(root, "candidate");
		UT_ASSERT_EQ(cluster_recovery_namespace_begin(root, 8, &scan),
					 CLUSTER_RECOVERY_NAMESPACE_MORE);
		UT_ASSERT_EQ(cluster_recovery_namespace_next(scan, &entry, &fd),
					 CLUSTER_RECOVERY_NAMESPACE_ENTRY);
		if (fault <= 2)
			inject_pread_fault = fault;
		else if (fault == 3) {
			writer = openat(root, "candidate", O_WRONLY);
			UT_ASSERT(writer >= 0);
			modify_during_read = writer;
		} else {
			UT_ASSERT_EQ(renameat(root, "candidate", root, "replaced"), 0);
			write_file(root, "candidate");
		}
		UT_ASSERT_EQ(cluster_recovery_namespace_read_current(scan, 0, observed, sizeof(observed)),
					 expected);
		UT_ASSERT_EQ(memcmp(observed, zero, sizeof(zero)), 0);
		inject_pread_fault = 0;
		modify_during_read = -1;
		if (writer >= 0)
			close(writer);
		UT_ASSERT_EQ(cluster_recovery_namespace_next(scan, &entry, &fd), expected);
		cluster_recovery_namespace_destroy(scan);
		remove_root(root);
		rmdir(test_path);
	}
}

UT_TEST(test_current_read_rejects_out_of_range_without_sealing)
{
	const uint64 offsets[] = { 1, UINT64_MAX };

	for (unsigned i = 0; i < lengthof(offsets); i++) {
		int root = new_root(), fd;
		ClusterRecoveryNamespace *scan = NULL;
		ClusterRecoveryNamespaceEntry entry;
		char observed[4] = { 1, 1, 1, 1 }, zero[4] = { 0 };

		write_file(root, "candidate");
		UT_ASSERT_EQ(cluster_recovery_namespace_begin(root, 8, &scan),
					 CLUSTER_RECOVERY_NAMESPACE_MORE);
		UT_ASSERT_EQ(cluster_recovery_namespace_next(scan, &entry, &fd),
					 CLUSTER_RECOVERY_NAMESPACE_ENTRY);
		UT_ASSERT_EQ(
			cluster_recovery_namespace_read_current(scan, offsets[i], observed, sizeof(observed)),
			CLUSTER_RECOVERY_NAMESPACE_INVALID);
		UT_ASSERT_EQ(memcmp(observed, zero, sizeof(zero)), 0);
		UT_ASSERT_EQ(cluster_recovery_namespace_next(scan, &entry, &fd),
					 CLUSTER_RECOVERY_NAMESPACE_INVALID);
		cluster_recovery_namespace_destroy(scan);
		remove_root(root);
		rmdir(test_path);
	}
}

int
main(void)
{
	UT_PLAN(21);
	UT_RUN(test_shared_relation_names);
	UT_RUN(test_native_slru_segment_names_are_explicit_obligations);
	UT_RUN(test_native_slru_short_paths_never_form_a_segment_key);
	UT_RUN(test_real_native_slru_entries_carry_family_and_segment);
	UT_RUN(test_twophase_state_names_are_explicit_obligations);
	UT_RUN(test_real_twophase_entries_carry_xid);
	UT_RUN(test_relmap_and_both_authority_copies);
	UT_RUN(test_undo_names_keep_origin_and_reject_aliases);
	UT_RUN(test_real_undo_files_export_physical_keys_without_a_live_bitmap);
	UT_RUN(test_unknown_and_aliases_cannot_be_empty_coverage);
	UT_RUN(test_real_tree_includes_untouched_and_unknown_files);
	UT_RUN(test_symlink_and_hardlink_are_not_candidates);
	UT_RUN(test_read_error_is_not_eof);
	UT_RUN(test_disappeared_or_modified_file_invalidates_observation);
	UT_RUN(test_capacity_never_seals_a_prefix);
	UT_RUN(test_new_directory_entry_invalidates_finished_set);
	UT_RUN(test_stat_io_failure_keeps_its_cause);
	UT_RUN(test_borrowed_directory_cursor_does_not_skip_files);
	UT_RUN(test_current_file_reads_all_pages_without_moving_fd_cursor);
	UT_RUN(test_current_read_short_io_and_changed_are_not_empty_pages);
	UT_RUN(test_current_read_rejects_out_of_range_without_sealing);
	UT_DONE();
	return ut_failed_count != 0;
}
