/* Real creation I/O and syscall-only faults for the original relmap owner.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres_fe.h"
#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include "cluster/cluster_relmap_authority.h"
#include "common/logging.h"
#include "common/relmap.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();
enum Fault {
	NONE,
	WRITE_FAIL,
	SHORT_WRITE,
	FILE_SYNC_FAIL,
	DIR_SYNC_FAIL,
	READBACK_SHORT,
	READBACK_CORRUPT,
	REPLACE_TARGET,
	CHANGE_SOURCE,
	TAMPER_EARLIER_COPY,
	REPLACE_EARLIER_COPY,
	CLOSE_FAIL
};
static enum Fault fault;
static int written_fd = -1;
static unsigned writes, file_syncs, directory_syncs;
static unsigned fault_at = 1;
static char root[MAXPGPATH];
static int root_fd, source_fd, shared_fd;

static ssize_t
test_pwrite(int fd, const void *bytes, size_t size, off_t offset)
{
	++writes;
	written_fd = fd;
	if (fault == WRITE_FAIL && writes == fault_at) {
		errno = ENOSPC;
		return -1;
	}
	if (fault == SHORT_WRITE && writes == fault_at)
		return pwrite(fd, bytes, size - 1, offset);
	if (writes == 8 && fault == TAMPER_EARLIER_COPY) {
		int earlier = openat(shared_fd, "global/pgrac_relmap_authority", O_WRONLY);
		char byte = 'X';
		UT_ASSERT(earlier >= 0 && pwrite(earlier, &byte, 1, 0) == 1 && close(earlier) == 0);
	}
	if (writes == 8 && fault == REPLACE_EARLIER_COPY) {
		UT_ASSERT(unlinkat(shared_fd, "global/pgrac_relmap_authority", 0) == 0);
		UT_ASSERT(symlinkat("pgrac_relmap_authority.bak", shared_fd, "global/pgrac_relmap_authority") == 0);
	}
	if (writes == 1 && fault == REPLACE_TARGET) {
		UT_ASSERT(renameat(shared_fd, "base/5", shared_fd, "base/saved") == 0);
		UT_ASSERT(mkdirat(shared_fd, "base/5", 0700) == 0);
	}
	if (writes == 1 && fault == CHANGE_SOURCE) {
		int input = openat(source_fd, "base/5/pg_filenode.map", O_WRONLY);
		char byte = 'X';
		UT_ASSERT(input >= 0 && pwrite(input, &byte, 1, 0) == 1 && close(input) == 0);
	}
	return pwrite(fd, bytes, size, offset);
}
static ssize_t
test_pread(int fd, void *bytes, size_t size, off_t offset)
{
	ssize_t result;
	if (fd == written_fd && fault == READBACK_SHORT && writes == fault_at)
		return 0;
	result = pread(fd, bytes, size, offset);
	if (fd == written_fd && result > 0 && fault == READBACK_CORRUPT && writes == fault_at)
		((char *)bytes)[0] ^= 1;
	return result;
}
static int
test_fsync(int fd)
{
	struct stat st;
	UT_ASSERT(fstat(fd, &st) == 0);
	if (S_ISREG(st.st_mode)) {
		++file_syncs;
		if (fault == FILE_SYNC_FAIL && file_syncs == fault_at) {
			errno = EIO;
			return -1;
		}
	} else {
		++directory_syncs;
		if (fault == DIR_SYNC_FAIL && directory_syncs == fault_at) {
			errno = EIO;
			return -1;
		}
	}
	return fsync(fd);
}
static int
test_close(int fd)
{
	int rc = close(fd);
	if (rc == 0 && fd == written_fd && fault == CLOSE_FAIL && writes == fault_at) {
		errno = EIO;
		return -1;
	}
	return rc;
}
#define pwrite test_pwrite
#define pread test_pread
#define fsync test_fsync
#define close test_close
#include "../../bin/initdb/pgrac_relmap.c"
#undef pwrite
#undef pread
#undef fsync
#undef close

static const char *const paths[] = { "global", "base/1", "base/4", "base/5" };
static const Oid shared_oids[]
	= { 1262, 2964, 1213, 1260, 1261, 1214, 2396, 6000, 3592, 6243, 6100, 4177, 4178,
		2966, 2967, 4185, 4186, 4175, 4176, 2846, 2847, 4181, 4182, 4060, 4061, 6244,
		6245, 4183, 4184, 2671, 2672, 2965, 2697, 2698, 2676, 2677, 6303, 2694, 2695,
		6302, 1232, 1233, 2397, 6001, 6002, 3593, 6246, 6247, 6114, 6115 };
static const Oid local_oids[] = { 1259, 1249, 1255, 1247, 2836, 2837, 4171, 4172, 2690,
								  2691, 2703, 2704, 2658, 2659, 2662, 2663, 3455 };

static void
prepare(void)
{
	fault = NONE;
	fault_at = 1;
	written_fd = -1;
	writes = file_syncs = directory_syncs = 0;
	strlcpy(root, "/tmp/pgrac-initdb-relmap-XXXXXX", sizeof(root));
	UT_ASSERT(mkdtemp(root) != NULL);
	root_fd = open(root, O_RDONLY | O_DIRECTORY);
	UT_ASSERT(root_fd >= 0 && mkdirat(root_fd, "source", 0700) == 0
			  && mkdirat(root_fd, "shared", 0700) == 0);
	source_fd = openat(root_fd, "source", O_RDONLY | O_DIRECTORY);
	shared_fd = openat(root_fd, "shared", O_RDONLY | O_DIRECTORY);
	UT_ASSERT(source_fd >= 0 && shared_fd >= 0);
	UT_ASSERT(mkdirat(source_fd, "base", 0700) == 0 && mkdirat(shared_fd, "base", 0700) == 0);
	for (unsigned i = 0; i < lengthof(paths); ++i) {
		RelMapFile map = { 0 };
		const Oid *oids = i == 0 ? shared_oids : local_oids;
		char name[MAXPGPATH];
		int fd;
		UT_ASSERT(mkdirat(source_fd, paths[i], 0700) == 0
				  && mkdirat(shared_fd, paths[i], 0700) == 0);
		map.magic = RELMAPPER_FILEMAGIC;
		map.num_mappings = i == 0 ? lengthof(shared_oids) : lengthof(local_oids);
		for (int j = 0; j < map.num_mappings; ++j)
			map.mappings[j] = (RelMapping){ oids[j], oids[j] };
		INIT_CRC32C(map.crc);
		COMP_CRC32C(map.crc, &map, offsetof(RelMapFile, crc));
		FIN_CRC32C(map.crc);
		snprintf(name, sizeof(name), "%s/pg_filenode.map", paths[i]);
		fd = openat(source_fd, name, O_WRONLY | O_CREAT | O_EXCL, 0600);
		UT_ASSERT(fd >= 0 && write(fd, &map, sizeof(map)) == sizeof(map) && close(fd) == 0);
	}
}
static void
remove_contents(int fd)
{
	DIR *dir = fdopendir(openat(fd, ".", O_RDONLY | O_DIRECTORY));
	struct dirent *entry;
	UT_ASSERT(dir != NULL);
	while ((entry = readdir(dir)) != NULL) {
		struct stat st;
		if (!strcmp(entry->d_name, ".") || !strcmp(entry->d_name, ".."))
			continue;
		UT_ASSERT(fstatat(fd, entry->d_name, &st, AT_SYMLINK_NOFOLLOW) == 0);
		if (S_ISDIR(st.st_mode)) {
			int child = openat(fd, entry->d_name, O_RDONLY | O_DIRECTORY | O_NOFOLLOW);
			UT_ASSERT(child >= 0);
			remove_contents(child);
			UT_ASSERT(close(child) == 0 && unlinkat(fd, entry->d_name, AT_REMOVEDIR) == 0);
		} else
			UT_ASSERT(unlinkat(fd, entry->d_name, 0) == 0);
	}
	UT_ASSERT(closedir(dir) == 0);
}
static void
cleanup(void)
{
	UT_ASSERT(fcntl(source_fd, F_GETFD) >= 0 && fcntl(shared_fd, F_GETFD) >= 0);
	close(source_fd);
	close(shared_fd);
	remove_contents(root_fd);
	close(root_fd);
	UT_ASSERT(rmdir(root) == 0);
}

UT_TEST(real_files_equal_pure_images_and_sources_unchanged)
{
	prepare();
	UT_ASSERT(pgrac_initdb_relmap_create(source_fd, shared_fd));
	UT_ASSERT(writes == 4 && file_syncs == 4 && directory_syncs >= 6);
	for (unsigned i = 0; i < lengthof(paths); ++i) {
		RelMapFile map;
		char name[MAXPGPATH], expected[CLUSTER_RELMAP_AUTHORITY_FILE_SIZE] = { 0 };
		char actual[sizeof(expected)];
		Oid dbid = i == 0 ? 0 : (i == 1 ? 1 : i + 2);
		int fd;
		snprintf(name, sizeof(name), "%s/pg_filenode.map", paths[i]);
		fd = openat(source_fd, name, O_RDONLY);
		UT_ASSERT(fd >= 0 && read(fd, &map, sizeof(map)) == sizeof(map) && close(fd) == 0);
		UT_ASSERT(cluster_relmap_authority_init_image(i == 0, dbid, &map, sizeof(map), expected,
													  sizeof(expected))
				  == CLUSTER_RELMAP_INIT_OK);
		snprintf(name, sizeof(name), "%s/pgrac_relmap_authority", paths[i]);
		fd = openat(shared_fd, name, O_RDONLY);
		UT_ASSERT(fd >= 0 && read(fd, actual, sizeof(actual)) == sizeof(actual) && close(fd) == 0);
		UT_ASSERT(memcmp(actual, expected, sizeof(actual)) == 0);
	}
	UT_ASSERT(!pgrac_initdb_relmap_create(source_fd, shared_fd) && writes == 4);
	cleanup();
}
UT_TEST(last_input_corrupt_refuses_before_any_write)
{
	int fd;
	prepare();
	fd = openat(source_fd, "base/5/pg_filenode.map", O_WRONLY);
	UT_ASSERT(fd >= 0 && ftruncate(fd, 0) == 0 && close(fd) == 0);
	UT_ASSERT(!pgrac_initdb_relmap_create(source_fd, shared_fd) && writes == 0);
	cleanup();
}
UT_TEST(source_file_alias_refuses)
{
	prepare();
	UT_ASSERT(renameat(source_fd, "base/5/pg_filenode.map", source_fd, "saved") == 0);
	UT_ASSERT(symlinkat("../../saved", source_fd, "base/5/pg_filenode.map") == 0);
	UT_ASSERT(!pgrac_initdb_relmap_create(source_fd, shared_fd) && writes == 0);
	cleanup();
}
UT_TEST(target_ancestor_alias_refuses)
{
	prepare();
	UT_ASSERT(renameat(shared_fd, "base", shared_fd, "saved") == 0);
	UT_ASSERT(symlinkat("saved", shared_fd, "base") == 0);
	UT_ASSERT(!pgrac_initdb_relmap_create(source_fd, shared_fd) && writes == 0);
	cleanup();
}
UT_TEST(existing_authority_variants_are_never_adopted)
{
	const char *const suffix[] = { "", ".bak", ".tmp", ".bak.tmp" };
	for (unsigned i = 0; i < lengthof(suffix); ++i) {
		char name[MAXPGPATH], byte = 0;
		int fd;
		prepare();
		snprintf(name, sizeof(name), "base/5/pgrac_relmap_authority%s", suffix[i]);
		fd = openat(shared_fd, name, O_RDWR | O_CREAT | O_EXCL, 0600);
		UT_ASSERT(fd >= 0 && write(fd, "X", 1) == 1);
		UT_ASSERT(!pgrac_initdb_relmap_create(source_fd, shared_fd) && writes == 0);
		UT_ASSERT(pread(fd, &byte, 1, 0) == 1 && byte == 'X' && close(fd) == 0);
		cleanup();
	}
}
UT_TEST(unexpected_database_refuses)
{
	prepare();
	UT_ASSERT(mkdirat(source_fd, "base/42", 0700) == 0);
	UT_ASSERT(!pgrac_initdb_relmap_create(source_fd, shared_fd) && writes == 0);
	cleanup();
}
UT_TEST(same_root_cannot_be_source_and_target)
{
	prepare();
	UT_ASSERT(!pgrac_initdb_relmap_create(source_fd, source_fd) && writes == 0);
	cleanup();
}
static void
io_failure(enum Fault value)
{
	prepare();
	fault = value;
	UT_ASSERT(!pgrac_initdb_relmap_create(source_fd, shared_fd));
	UT_ASSERT(writes > 0);
	if (value != WRITE_FAIL && value != SHORT_WRITE)
		UT_ASSERT(file_syncs > 0);
	cleanup();
}
UT_TEST(short_write_refuses)
{
	io_failure(SHORT_WRITE);
}
UT_TEST(write_error_refuses)
{
	io_failure(WRITE_FAIL);
}
UT_TEST(file_sync_error_refuses)
{
	io_failure(FILE_SYNC_FAIL);
}
UT_TEST(directory_sync_error_refuses)
{
	io_failure(DIR_SYNC_FAIL);
}
UT_TEST(readback_short_refuses)
{
	io_failure(READBACK_SHORT);
}
UT_TEST(readback_corruption_refuses)
{
	io_failure(READBACK_CORRUPT);
}
UT_TEST(target_replacement_refuses)
{
	io_failure(REPLACE_TARGET);
}
UT_TEST(source_change_refuses)
{
	io_failure(CHANGE_SOURCE);
}

static bool
create_v2(void)
{
	return pgrac_initdb_relmap_v2_create(source_fd, shared_fd, 91, 27);
}

UT_TEST(v2_creates_independent_clean_pairs_in_all_namespaces)
{
	prepare();
	UT_ASSERT(create_v2());
	UT_ASSERT(writes == 8 && file_syncs == 8 && directory_syncs >= 10);
	for (unsigned i = 0; i < lengthof(paths); ++i) {
		ClusterRelmapV2Namespace identity = { 91, 27, i == 0 ? 0 : (i == 1 ? 1 : i + 2), i == 0 };
		ClusterRelmapV2View view = { 0 };
		RelMapFile native = { 0 };
		char name[MAXPGPATH], bytes[2][CLUSTER_RELMAP_V2_FILE_SIZE] = { { 0 } };
		struct stat copies[2] = { { 0 } };
		int fd;
		for (unsigned c = 0; c < 2; ++c) {
			snprintf(name, sizeof(name), "%s/pgrac_relmap_authority%s", paths[i], c ? ".bak" : "");
			fd = openat(shared_fd, name, O_RDONLY | O_NOFOLLOW);
			UT_ASSERT(fd >= 0 && fstat(fd, &copies[c]) == 0
				&& read(fd, bytes[c], sizeof(bytes[c])) == sizeof(bytes[c]) && close(fd) == 0);
			UT_ASSERT(copies[c].st_size == sizeof(bytes[c]) && copies[c].st_nlink == 1);
		}
		UT_ASSERT(copies[0].st_ino != copies[1].st_ino && memcmp(bytes[0], bytes[1], sizeof(bytes[0])) == 0);
		UT_ASSERT_EQ(cluster_relmap_v2_read_pair(bytes[0], sizeof(bytes[0]), bytes[1], sizeof(bytes[1]),
												   &identity, &view), CLUSTER_RELMAP_V2_PAIR_VALID);
		UT_ASSERT(view.pending_complete && !view.has_pending && view.committed_generation == 1);
		snprintf(name, sizeof(name), "%s/pg_filenode.map", paths[i]);
		fd = openat(source_fd, name, O_RDONLY);
		UT_ASSERT(fd >= 0 && read(fd, &native, sizeof(native)) == sizeof(native) && close(fd) == 0);
		UT_ASSERT(memcmp(&native, &view.committed, sizeof(native)) == 0);
	}
	UT_ASSERT(!create_v2() && writes == 8);
	cleanup();
}

UT_TEST(v2_invalid_creation_identity_refuses_before_write)
{
	prepare();
	UT_ASSERT(!pgrac_initdb_relmap_v2_create(source_fd, shared_fd, 0, 27) && writes == 0);
	UT_ASSERT(!pgrac_initdb_relmap_v2_create(source_fd, shared_fd, 91, 0) && writes == 0);
	cleanup();
}

UT_TEST(v2_last_native_input_invalid_refuses_before_write)
{
	prepare();
	int fd = openat(source_fd, "base/5/pg_filenode.map", O_WRONLY);
	UT_ASSERT(fd >= 0 && ftruncate(fd, 0) == 0 && close(fd) == 0);
	UT_ASSERT(!create_v2() && writes == 0);
	cleanup();
}

UT_TEST(v2_existing_files_never_adopted_or_overwritten)
{
	const char *const suffix[] = { "", ".bak", ".tmp", ".bak.tmp" };
	for (unsigned i = 0; i < lengthof(suffix); ++i) {
		prepare();
		char name[MAXPGPATH], byte = 0;
		snprintf(name, sizeof(name), "base/5/pgrac_relmap_authority%s", suffix[i]);
		int fd = openat(shared_fd, name, O_RDWR | O_CREAT | O_EXCL, 0600);
		UT_ASSERT(fd >= 0 && write(fd, "X", 1) == 1);
		UT_ASSERT(!create_v2() && writes == 0);
		UT_ASSERT(pread(fd, &byte, 1, 0) == 1 && byte == 'X' && close(fd) == 0);
		cleanup();
	}
}

UT_TEST(v2_directory_and_source_aliases_refuse)
{
	prepare();
	UT_ASSERT(!pgrac_initdb_relmap_v2_create(source_fd, source_fd, 91, 27) && writes == 0);
	UT_ASSERT(mkdirat(shared_fd, "base/42", 0700) == 0);
	UT_ASSERT(!create_v2() && writes == 0);
	cleanup();
	prepare();
	UT_ASSERT(renameat(shared_fd, "base", shared_fd, "saved") == 0
		&& symlinkat("saved", shared_fd, "base") == 0);
	UT_ASSERT(!create_v2() && writes == 0);
	cleanup();
	prepare();
	UT_ASSERT(renameat(source_fd, "base/5/pg_filenode.map", source_fd, "saved") == 0
		&& symlinkat("../../saved", source_fd, "base/5/pg_filenode.map") == 0);
	UT_ASSERT(!create_v2() && writes == 0);
	cleanup();
}

UT_TEST(v2_both_copies_io_errors_refuse_and_partial_retry_refuses)
{
	const enum Fault failures[] = { WRITE_FAIL, SHORT_WRITE, FILE_SYNC_FAIL,
		DIR_SYNC_FAIL, READBACK_SHORT, READBACK_CORRUPT, CLOSE_FAIL };
	const unsigned cuts[] = { 1, 2, 8 };
	for (unsigned i = 0; i < lengthof(failures); ++i)
		for (unsigned j = 0; j < lengthof(cuts); ++j) {
			prepare();
			fault = failures[i];
			fault_at = cuts[j];
			UT_ASSERT(!create_v2() && writes == fault_at);
			unsigned before = writes;
			fault = NONE;
			UT_ASSERT(!create_v2() && writes == before);
			cleanup();
		}
}

UT_TEST(v2_final_read_rejects_earlier_copy_and_source_changes)
{
	const enum Fault failures[] = { TAMPER_EARLIER_COPY, REPLACE_EARLIER_COPY,
		REPLACE_TARGET, CHANGE_SOURCE };
	for (unsigned i = 0; i < lengthof(failures); ++i) {
		prepare();
		fault = failures[i];
		UT_ASSERT(!create_v2() && writes > 0);
		cleanup();
	}
}

UT_TEST(v2_final_directory_sync_failure_refuses)
{
	for (unsigned i = 9; i <= 10; ++i) {
		prepare();
		fault = DIR_SYNC_FAIL;
		fault_at = i;
		UT_ASSERT(!create_v2() && writes == 8 && directory_syncs == i);
		cleanup();
	}
}

int
main(int argc, char **argv)
{
	pg_logging_init(argv[0]);
	UT_PLAN(23);
	UT_RUN(real_files_equal_pure_images_and_sources_unchanged);
	UT_RUN(last_input_corrupt_refuses_before_any_write);
	UT_RUN(source_file_alias_refuses);
	UT_RUN(target_ancestor_alias_refuses);
	UT_RUN(existing_authority_variants_are_never_adopted);
	UT_RUN(unexpected_database_refuses);
	UT_RUN(same_root_cannot_be_source_and_target);
	UT_RUN(short_write_refuses);
	UT_RUN(write_error_refuses);
	UT_RUN(file_sync_error_refuses);
	UT_RUN(directory_sync_error_refuses);
	UT_RUN(readback_short_refuses);
	UT_RUN(readback_corruption_refuses);
	UT_RUN(target_replacement_refuses);
	UT_RUN(source_change_refuses);
	UT_RUN(v2_creates_independent_clean_pairs_in_all_namespaces);
	UT_RUN(v2_invalid_creation_identity_refuses_before_write);
	UT_RUN(v2_last_native_input_invalid_refuses_before_write);
	UT_RUN(v2_existing_files_never_adopted_or_overwritten);
	UT_RUN(v2_directory_and_source_aliases_refuse);
	UT_RUN(v2_both_copies_io_errors_refuse_and_partial_retry_refuses);
	UT_RUN(v2_final_read_rejects_earlier_copy_and_source_changes);
	UT_RUN(v2_final_directory_sync_failure_refuses);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
