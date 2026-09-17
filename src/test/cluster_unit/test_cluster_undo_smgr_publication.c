/*-------------------------------------------------------------------------
 *
 * test_cluster_undo_smgr_publication.c
 *    Unit tests for the Spec 8.4A first-publication storage seam.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "cluster/cluster_undo_smgr.h"
#include "cluster/cluster_undo_segment.h"
#include "cluster/cluster_undo_record_api.h"
#include "common/file_perm.h"
#include "miscadmin.h"
#include "storage/fd.h"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

int MyProcPid = 4321;
pg_time_t MyStartTime = 0;
TimestampTz MyStartTimestamp = INT64CONST(0x102030405060);
int cluster_node_id = 0;
int pg_file_create_mode = 0600;
int pg_dir_create_mode = 0700;

static char publication_dir[MAXPGPATH];
static bool basic_open_forced_error = false;
static int fsync_calls = 0;
static int fsync_fail_on_call = 0;
static bool pwrite_forced_error = false;
static int product_close_calls = 0;
static int product_close_fail_on_call = 0;
static bool free_dir_forced_error = false;
static bool basic_open_swap_to_symlink = false;
static char basic_open_swap_target[MAXPGPATH];

/* Backend path helpers are linked from libpgport_srv. An unexpected ERROR
 * must fail this standalone fixture, never become an accepted I/O result. */
bool
errstart_cold(int level, const char *domain)
{
	return true;
}
int
errcode(int code)
{
	return 0;
}
int
errmsg(const char *fmt, ...)
{
	return 0;
}
int
errmsg_internal(const char *fmt, ...)
{
	return 0;
}
void
errfinish(const char *file, int line, const char *function)
{
	abort();
}

void
cluster_undo_record_note_smgr_open(void)
{}
void
cluster_undo_record_note_smgr_close(void)
{}
void
cluster_undo_record_note_smgr_pread(void)
{}
void
cluster_undo_record_note_smgr_pwrite(void)
{}

void
ExceptionalCondition(const char *conditionName, const char *fileName, int lineNumber)
{
	fprintf(stderr, "assertion failed: %s at %s:%d\n", conditionName, fileName, lineNumber);
	abort();
}

int
BasicOpenFile(const char *fileName, int fileFlags)
{
	if (basic_open_forced_error) {
		errno = EACCES;
		return -1;
	}
	if (basic_open_swap_to_symlink) {
		basic_open_swap_to_symlink = false;
		UT_ASSERT_EQ(unlink(fileName), 0);
		UT_ASSERT_EQ(symlink(basic_open_swap_target, fileName), 0);
	}
	return open(fileName, fileFlags, pg_file_create_mode);
}

DIR *
AllocateDir(const char *dirname)
{
	return opendir(dirname);
}

int
FreeDir(DIR *dir)
{
	int result = closedir(dir);

	if (result == 0 && free_dir_forced_error) {
		errno = EIO;
		return -1;
	}
	return result;
}

int
pg_fsync(int fd)
{
	fsync_calls++;
	if (fsync_fail_on_call > 0 && fsync_calls == fsync_fail_on_call) {
		errno = EIO;
		return -1;
	}
	return fsync(fd);
}

static ssize_t
test_product_pwrite(int fd, const void *buf, size_t nbytes, off_t offset)
{
	if (pwrite_forced_error) {
		errno = EIO;
		return -1;
	}
	return pwrite(fd, buf, nbytes, offset);
}

static int
test_product_close(int fd)
{
	int result;

	product_close_calls++;
	result = close(fd);
	if (result == 0 && product_close_fail_on_call > 0
		&& product_close_calls == product_close_fail_on_call) {
		errno = EIO;
		return -1;
	}
	return result;
}

int
cluster_undo_path_resolve(ClusterUndoPathIntent intent pg_attribute_unused(), uint8 owner_instance,
						  uint32 segment_id, char *buf, size_t buf_size)
{
	int ret = snprintf(buf, buf_size, "%s/seg_%u.dat", publication_dir, (unsigned)segment_id);

	return ret < 0 || (size_t)ret >= buf_size ? -1 : 0;
}

bool
cluster_undo_segment_header_identity_ok(const char *blockbuf,
										uint32 segment_id pg_attribute_unused(),
										uint8 owner_instance pg_attribute_unused())
{
	return blockbuf != NULL && (unsigned char)blockbuf[0] == 0xa5;
}

#undef pg_pwrite
#define pg_pwrite test_product_pwrite
#define close test_product_close
#include "../../backend/cluster/storage/cluster_undo_smgr.c"
#undef close
#undef pg_pwrite

static void
resolve_final(char path[MAXPGPATH])
{
	UT_ASSERT_EQ(cluster_undo_path_resolve(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, path, MAXPGPATH),
				 0);
}

static void
remove_if_present(const char *path)
{
	if (path != NULL && path[0] != '\0')
		(void)unlink(path);
}

static void
write_segment_file(const char *path, const char *block0, off_t size)
{
	int fd = open(path, O_RDWR | O_CREAT | O_EXCL | PG_BINARY, pg_file_create_mode);

	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(ftruncate(fd, size), 0);
	if (size >= BLCKSZ)
		UT_ASSERT_EQ(pwrite(fd, block0, BLCKSZ, 0), BLCKSZ);
	UT_ASSERT_EQ(close(fd), 0);
}

static void
write_empty_file(const char *path)
{
	int fd = open(path, O_WRONLY | O_CREAT | O_EXCL | PG_BINARY, pg_file_create_mode);

	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(close(fd), 0);
}

static void
make_page(char page[BLCKSZ], unsigned char marker)
{
	memset(page, 0, BLCKSZ);
	page[0] = (char)marker;
	page[BLCKSZ - 1] = (char)(marker ^ 0xff);
}

static void
resolve_root_descriptor(char path[MAXPGPATH])
{
	UT_ASSERT(snprintf(path, MAXPGPATH, "%s/pgrac_undo_root.control", publication_dir) < MAXPGPATH);
}

static int
count_root_descriptor_temps(void)
{
	DIR *dir = opendir(publication_dir);
	struct dirent *entry;
	int count = 0;

	UT_ASSERT_NOT_NULL(dir);
	if (dir == NULL)
		return -1;
	while ((entry = readdir(dir)) != NULL) {
		if (strstr(entry->d_name, "pgrac_undo_root.control.pgrac-rdtmp.") == entry->d_name)
			count++;
	}
	UT_ASSERT_EQ(closedir(dir), 0);
	return count;
}

UT_TEST(test_probe_distinguishes_absent_and_preserves_output)
{
	char final[MAXPGPATH];
	char page[BLCKSZ];

	resolve_final(final);
	remove_if_present(final);
	memset(page, 0x6d, sizeof(page));
	UT_ASSERT_EQ(cluster_undo_smgr_probe_segment(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, page),
				 CLUSTER_UNDO_SMGR_FINAL_ABSENT);
	UT_ASSERT_EQ((unsigned char)page[0], 0x6d);
	UT_ASSERT_EQ((unsigned char)page[BLCKSZ - 1], 0x6d);
}

UT_TEST(test_probe_accepts_only_exact_full_identity)
{
	char final[MAXPGPATH];
	char expected[BLCKSZ];
	char observed[BLCKSZ];

	resolve_final(final);
	remove_if_present(final);
	make_page(expected, 0xa5);
	write_segment_file(final, expected, UNDO_SEGMENT_SIZE_BYTES);
	memset(observed, 0, sizeof(observed));
	fsync_calls = 0;
	UT_ASSERT_EQ(cluster_undo_smgr_probe_segment(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, observed),
				 CLUSTER_UNDO_SMGR_FINAL_EXACT);
	UT_ASSERT_EQ(fsync_calls, 1);
	UT_ASSERT_EQ(memcmp(observed, expected, BLCKSZ), 0);
	remove_if_present(final);
}

UT_TEST(test_probe_classifies_short_or_wrong_identity_as_invalid)
{
	char final[MAXPGPATH];
	char page[BLCKSZ];
	char observed[BLCKSZ];

	resolve_final(final);
	remove_if_present(final);
	make_page(page, 0xa5);
	write_segment_file(final, page, BLCKSZ);
	UT_ASSERT_EQ(cluster_undo_smgr_probe_segment(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, observed),
				 CLUSTER_UNDO_SMGR_FINAL_INVALID);
	remove_if_present(final);

	make_page(page, 0x5a);
	write_segment_file(final, page, UNDO_SEGMENT_SIZE_BYTES);
	UT_ASSERT_EQ(cluster_undo_smgr_probe_segment(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, observed),
				 CLUSTER_UNDO_SMGR_FINAL_INVALID);
	remove_if_present(final);
}

UT_TEST(test_probe_reports_non_absent_open_failure_as_io_error)
{
	char final[MAXPGPATH];
	char page[BLCKSZ];

	resolve_final(final);
	remove_if_present(final);
	make_page(page, 0xa5);
	write_segment_file(final, page, UNDO_SEGMENT_SIZE_BYTES);
	basic_open_forced_error = true;
	UT_ASSERT_EQ(cluster_undo_smgr_probe_segment(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, page),
				 CLUSTER_UNDO_SMGR_FINAL_IO_ERROR);
	basic_open_forced_error = false;
	remove_if_present(final);
}

UT_TEST(test_probe_close_failure_overrides_invalid_size)
{
	char final[MAXPGPATH];
	char page[BLCKSZ];

	resolve_final(final);
	remove_if_present(final);
	make_page(page, 0xa5);
	write_segment_file(final, page, BLCKSZ);
	product_close_calls = 0;
	product_close_fail_on_call = 1;
	UT_ASSERT_EQ(cluster_undo_smgr_probe_segment(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, page),
				 CLUSTER_UNDO_SMGR_FINAL_IO_ERROR);
	product_close_fail_on_call = 0;
	remove_if_present(final);
}

UT_TEST(test_temp_is_same_directory_boot_unique_and_exactly_cleaned)
{
	char final[MAXPGPATH];
	char temp[MAXPGPATH];
	char sibling[MAXPGPATH];
	int fd;

	resolve_final(final);
	remove_if_present(final);
	temp[0] = '\0';
	UT_ASSERT(
		cluster_undo_smgr_provision_temp_create(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, temp));
	UT_ASSERT(strncmp(temp, publication_dir, strlen(publication_dir)) == 0);
	UT_ASSERT(strstr(temp, ".pgrac-b0tmp.") != NULL);
	UT_ASSERT(access(temp, F_OK) == 0);

	UT_ASSERT(snprintf(sibling, sizeof(sibling), "%s.sibling", temp) < (int)sizeof(sibling));
	fd = open(sibling, O_WRONLY | O_CREAT | O_EXCL | PG_BINARY, pg_file_create_mode);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(close(fd), 0);
	UT_ASSERT(
		cluster_undo_smgr_provision_temp_cleanup(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, temp));
	UT_ASSERT(access(temp, F_OK) != 0 && errno == ENOENT);
	UT_ASSERT(access(sibling, F_OK) == 0);
	remove_if_present(sibling);
}

UT_TEST(test_cleanup_rejects_final_path)
{
	char final[MAXPGPATH];
	char page[BLCKSZ];

	resolve_final(final);
	remove_if_present(final);
	make_page(page, 0xa5);
	write_segment_file(final, page, UNDO_SEGMENT_SIZE_BYTES);
	UT_ASSERT(
		!cluster_undo_smgr_provision_temp_cleanup(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, final));
	UT_ASSERT(access(final, F_OK) == 0);
	remove_if_present(final);
}

UT_TEST(test_startup_cleanup_removes_boot_foreign_temp_and_fsyncs_directory)
{
	char final[MAXPGPATH];
	char foreign[MAXPGPATH];

	resolve_final(final);
	UT_ASSERT(snprintf(foreign, sizeof(foreign), "%s.pgrac-b0tmp.777.888.1", final)
			  < (int)sizeof(foreign));
	remove_if_present(foreign);
	write_empty_file(foreign);

	fsync_calls = 0;
	UT_ASSERT(cluster_undo_smgr_cleanup_boot_foreign_temps(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1));
	UT_ASSERT(access(foreign, F_OK) != 0 && errno == ENOENT);
	UT_ASSERT_EQ(fsync_calls, 1);
}

UT_TEST(test_startup_cleanup_refuses_current_boot_temp)
{
	char current[MAXPGPATH];

	UT_ASSERT(
		cluster_undo_smgr_provision_temp_create(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, current));
	UT_ASSERT(!cluster_undo_smgr_cleanup_boot_foreign_temps(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1));
	UT_ASSERT(access(current, F_OK) == 0);
	UT_ASSERT(
		cluster_undo_smgr_provision_temp_cleanup(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, current));
}

UT_TEST(test_startup_cleanup_refuses_symlink_with_valid_foreign_name)
{
	char final[MAXPGPATH];
	char foreign[MAXPGPATH];

	resolve_final(final);
	UT_ASSERT(snprintf(foreign, sizeof(foreign), "%s.pgrac-b0tmp.777.888.2", final)
			  < (int)sizeof(foreign));
	remove_if_present(foreign);
	UT_ASSERT_EQ(symlink("does-not-exist", foreign), 0);

	UT_ASSERT(!cluster_undo_smgr_cleanup_boot_foreign_temps(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1));
	UT_ASSERT_EQ(access(foreign, F_OK), -1);
	UT_ASSERT_EQ(errno, ENOENT);
	{
		struct stat st;

		UT_ASSERT_EQ(lstat(foreign, &st), 0);
		UT_ASSERT(S_ISLNK(st.st_mode));
	}
	remove_if_present(foreign);
}

UT_TEST(test_startup_cleanup_refuses_malformed_temp_and_preserves_final)
{
	char final[MAXPGPATH];
	char malformed[MAXPGPATH];
	char page[BLCKSZ];

	resolve_final(final);
	remove_if_present(final);
	make_page(page, 0xa5);
	write_segment_file(final, page, UNDO_SEGMENT_SIZE_BYTES);
	UT_ASSERT(snprintf(malformed, sizeof(malformed), "%s.pgrac-b0tmp.bad.888.3", final)
			  < (int)sizeof(malformed));
	remove_if_present(malformed);
	write_empty_file(malformed);

	UT_ASSERT(!cluster_undo_smgr_cleanup_boot_foreign_temps(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1));
	UT_ASSERT(access(malformed, F_OK) == 0);
	UT_ASSERT(access(final, F_OK) == 0);
	remove_if_present(malformed);
	remove_if_present(final);
}

UT_TEST(test_startup_cleanup_refuses_truncated_temp_marker)
{
	char final[MAXPGPATH];
	char malformed[MAXPGPATH];

	resolve_final(final);
	UT_ASSERT(snprintf(malformed, sizeof(malformed), "%s.pgrac-b0tmp", final)
			  < (int)sizeof(malformed));
	remove_if_present(malformed);
	write_empty_file(malformed);

	UT_ASSERT(!cluster_undo_smgr_cleanup_boot_foreign_temps(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1));
	UT_ASSERT(access(malformed, F_OK) == 0);
	remove_if_present(malformed);
}

UT_TEST(test_startup_cleanup_empty_namespace_preserves_ordinary_final)
{
	char final[MAXPGPATH];
	char page[BLCKSZ];

	resolve_final(final);
	remove_if_present(final);
	make_page(page, 0xa5);
	write_segment_file(final, page, UNDO_SEGMENT_SIZE_BYTES);
	fsync_calls = 0;
	UT_ASSERT(cluster_undo_smgr_cleanup_boot_foreign_temps(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1));
	UT_ASSERT(access(final, F_OK) == 0);
	UT_ASSERT_EQ(fsync_calls, 0);
	remove_if_present(final);
}

UT_TEST(test_startup_cleanup_propagates_directory_close_failure)
{
	free_dir_forced_error = true;
	UT_ASSERT(!cluster_undo_smgr_cleanup_boot_foreign_temps(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1));
	free_dir_forced_error = false;
}

UT_TEST(test_startup_cleanup_propagates_directory_fsync_failure)
{
	char final[MAXPGPATH];
	char foreign[MAXPGPATH];

	resolve_final(final);
	UT_ASSERT(snprintf(foreign, sizeof(foreign), "%s.pgrac-b0tmp.777.888.4", final)
			  < (int)sizeof(foreign));
	remove_if_present(foreign);
	write_empty_file(foreign);

	fsync_calls = 0;
	fsync_fail_on_call = 1;
	UT_ASSERT(!cluster_undo_smgr_cleanup_boot_foreign_temps(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1));
	fsync_fail_on_call = 0;
	UT_ASSERT(access(foreign, F_OK) != 0 && errno == ENOENT);
}

UT_TEST(test_publish_is_no_replace_and_fsyncs_file_and_parent)
{
	char final[MAXPGPATH];
	char temp[MAXPGPATH];
	char page[BLCKSZ];
	char observed[BLCKSZ];

	resolve_final(final);
	remove_if_present(final);
	make_page(page, 0xa5);
	UT_ASSERT(
		cluster_undo_smgr_provision_temp_create(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, temp));
	fsync_calls = 0;
	UT_ASSERT_EQ(cluster_undo_smgr_provision_temp_publish(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1,
														  temp, page),
				 CLUSTER_UNDO_SMGR_PUBLISH_PUBLISHED);
	UT_ASSERT_EQ(fsync_calls, 2);
	UT_ASSERT(access(temp, F_OK) != 0 && errno == ENOENT);
	UT_ASSERT_EQ(cluster_undo_smgr_probe_segment(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, observed),
				 CLUSTER_UNDO_SMGR_FINAL_EXACT);
	UT_ASSERT_EQ(memcmp(observed, page, BLCKSZ), 0);
	remove_if_present(final);
}

UT_TEST(test_eexist_loser_never_overwrites_final_and_drops_own_temp)
{
	char final[MAXPGPATH];
	char temp[MAXPGPATH];
	char winner[BLCKSZ];
	char loser[BLCKSZ];
	char observed[BLCKSZ];

	resolve_final(final);
	remove_if_present(final);
	make_page(winner, 0xa5);
	winner[1] = 0x11;
	write_segment_file(final, winner, UNDO_SEGMENT_SIZE_BYTES);
	make_page(loser, 0xa5);
	loser[1] = 0x22;
	UT_ASSERT(
		cluster_undo_smgr_provision_temp_create(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, temp));
	UT_ASSERT_EQ(cluster_undo_smgr_provision_temp_publish(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1,
														  temp, loser),
				 CLUSTER_UNDO_SMGR_PUBLISH_EXISTS);
	UT_ASSERT(access(temp, F_OK) != 0 && errno == ENOENT);
	UT_ASSERT_EQ(cluster_undo_smgr_probe_segment(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, observed),
				 CLUSTER_UNDO_SMGR_FINAL_EXACT);
	UT_ASSERT_EQ(memcmp(observed, winner, BLCKSZ), 0);
	remove_if_present(final);
}

UT_TEST(test_eexist_invalid_final_is_never_repaired_or_overwritten)
{
	char final[MAXPGPATH];
	char temp[MAXPGPATH];
	char invalid[BLCKSZ];
	char candidate[BLCKSZ];
	char observed[BLCKSZ];
	int fd;

	resolve_final(final);
	remove_if_present(final);
	make_page(invalid, 0x5a);
	invalid[1] = 0x33;
	write_segment_file(final, invalid, UNDO_SEGMENT_SIZE_BYTES);
	make_page(candidate, 0xa5);
	candidate[1] = 0x44;
	UT_ASSERT(
		cluster_undo_smgr_provision_temp_create(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, temp));
	UT_ASSERT_EQ(cluster_undo_smgr_provision_temp_publish(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1,
														  temp, candidate),
				 CLUSTER_UNDO_SMGR_PUBLISH_INVALID);
	UT_ASSERT(access(temp, F_OK) != 0 && errno == ENOENT);
	fd = open(final, O_RDONLY | PG_BINARY);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(pread(fd, observed, BLCKSZ, 0), BLCKSZ);
	UT_ASSERT_EQ(close(fd), 0);
	UT_ASSERT_EQ(memcmp(observed, invalid, BLCKSZ), 0);
	remove_if_present(final);
}

UT_TEST(test_eexist_retry_fsyncs_parent_after_prior_parent_fsync_failure)
{
	char final[MAXPGPATH];
	char failed_temp[MAXPGPATH];
	char retry_temp[MAXPGPATH];
	char page[BLCKSZ];

	resolve_final(final);
	remove_if_present(final);
	make_page(page, 0xa5);
	UT_ASSERT(cluster_undo_smgr_provision_temp_create(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1,
													  failed_temp));
	fsync_calls = 0;
	fsync_fail_on_call = 2;
	UT_ASSERT_EQ(cluster_undo_smgr_provision_temp_publish(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1,
														  failed_temp, page),
				 CLUSTER_UNDO_SMGR_PUBLISH_IO_ERROR);
	UT_ASSERT(access(final, F_OK) == 0);
	UT_ASSERT(access(failed_temp, F_OK) != 0 && errno == ENOENT);

	UT_ASSERT(cluster_undo_smgr_provision_temp_create(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1,
													  retry_temp));
	fsync_calls = 0;
	fsync_fail_on_call = 0;
	UT_ASSERT_EQ(cluster_undo_smgr_provision_temp_publish(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1,
														  retry_temp, page),
				 CLUSTER_UNDO_SMGR_PUBLISH_EXISTS);
	UT_ASSERT_EQ(fsync_calls, 2);
	UT_ASSERT(access(retry_temp, F_OK) != 0 && errno == ENOENT);
	remove_if_present(final);
}

UT_TEST(test_temp_write_failure_closes_fd_and_removes_only_owned_temp)
{
	char final[MAXPGPATH];
	char temp[MAXPGPATH];
	char sibling[MAXPGPATH];
	char page[BLCKSZ];
	int fd;

	resolve_final(final);
	remove_if_present(final);
	make_page(page, 0xa5);
	UT_ASSERT(
		cluster_undo_smgr_provision_temp_create(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, temp));
	UT_ASSERT(snprintf(sibling, sizeof(sibling), "%s.sibling", temp) < (int)sizeof(sibling));
	fd = open(sibling, O_WRONLY | O_CREAT | O_EXCL | PG_BINARY, pg_file_create_mode);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(close(fd), 0);

	product_close_calls = 0;
	pwrite_forced_error = true;
	UT_ASSERT_EQ(cluster_undo_smgr_provision_temp_publish(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1,
														  temp, page),
				 CLUSTER_UNDO_SMGR_PUBLISH_IO_ERROR);
	pwrite_forced_error = false;
	UT_ASSERT_EQ(product_close_calls, 1);
	UT_ASSERT(access(temp, F_OK) != 0 && errno == ENOENT);
	UT_ASSERT(access(sibling, F_OK) == 0);
	UT_ASSERT(access(final, F_OK) != 0 && errno == ENOENT);
	remove_if_present(sibling);
}

UT_TEST(test_root_descriptor_mirror_publish_is_exact_and_durable)
{
	char final[MAXPGPATH];
	uint8 image[512];
	uint8 observed[512];
	struct stat st;
	int i;

	resolve_root_descriptor(final);
	remove_if_present(final);
	for (i = 0; i < 512; i++)
		image[i] = (uint8)(i ^ 0x39);
	memset(observed, 0xa5, sizeof(observed));
	fsync_calls = 0;
	UT_ASSERT_EQ(cluster_undo_smgr_root_descriptor_publish(publication_dir, image),
				 CLUSTER_UNDO_SMGR_ROOT_MIRROR_PUBLISHED);
	UT_ASSERT_EQ(fsync_calls, 2);
	UT_ASSERT_EQ(count_root_descriptor_temps(), 0);
	UT_ASSERT_EQ(lstat(final, &st), 0);
	UT_ASSERT(S_ISREG(st.st_mode));
	UT_ASSERT_EQ(st.st_size, 512);
	UT_ASSERT_EQ(cluster_undo_smgr_root_descriptor_probe(publication_dir, image, observed),
				 CLUSTER_UNDO_SMGR_ROOT_MIRROR_EXACT);
	UT_ASSERT_EQ(memcmp(observed, image, sizeof(image)), 0);
	remove_if_present(final);
}

UT_TEST(test_root_descriptor_mirror_eexist_is_validate_only)
{
	char final[MAXPGPATH];
	uint8 winner[512];
	uint8 loser[512];
	uint8 observed[512];
	int i;

	resolve_root_descriptor(final);
	remove_if_present(final);
	for (i = 0; i < 512; i++) {
		winner[i] = (uint8)(i ^ 0x6d);
		loser[i] = (uint8)(i ^ 0xc3);
	}
	UT_ASSERT_EQ(cluster_undo_smgr_root_descriptor_publish(publication_dir, winner),
				 CLUSTER_UNDO_SMGR_ROOT_MIRROR_PUBLISHED);
	UT_ASSERT_EQ(cluster_undo_smgr_root_descriptor_publish(publication_dir, winner),
				 CLUSTER_UNDO_SMGR_ROOT_MIRROR_EXACT);
	UT_ASSERT_EQ(count_root_descriptor_temps(), 0);
	UT_ASSERT_EQ(cluster_undo_smgr_root_descriptor_publish(publication_dir, loser),
				 CLUSTER_UNDO_SMGR_ROOT_MIRROR_HOLD);
	UT_ASSERT_EQ(count_root_descriptor_temps(), 0);
	memset(observed, 0, sizeof(observed));
	UT_ASSERT_EQ(cluster_undo_smgr_root_descriptor_probe(publication_dir, winner, observed),
				 CLUSTER_UNDO_SMGR_ROOT_MIRROR_EXACT);
	UT_ASSERT_EQ(memcmp(observed, winner, sizeof(winner)), 0);
	remove_if_present(final);
}

UT_TEST(test_root_descriptor_mirror_reads_exact_retry_candidate)
{
	char final[MAXPGPATH];
	uint8 image[512];
	uint8 observed[512];
	int i;

	resolve_root_descriptor(final);
	remove_if_present(final);
	for (i = 0; i < 512; i++)
		image[i] = (uint8)(i ^ 0x91);
	UT_ASSERT_EQ(cluster_undo_smgr_root_descriptor_publish(publication_dir, image),
				 CLUSTER_UNDO_SMGR_ROOT_MIRROR_PUBLISHED);
	memset(observed, 0, sizeof(observed));
	UT_ASSERT_EQ(cluster_undo_smgr_root_descriptor_read_candidate(publication_dir, observed),
				 CLUSTER_UNDO_SMGR_ROOT_MIRROR_EXACT);
	UT_ASSERT_EQ(memcmp(observed, image, sizeof(image)), 0);
	remove_if_present(final);
}

UT_TEST(test_root_descriptor_mirror_probe_rejects_short_and_symlink)
{
	char final[MAXPGPATH];
	char target[MAXPGPATH];
	uint8 expected[512];
	uint8 sentinel[512];
	uint8 observed[512];
	int fd;

	resolve_root_descriptor(final);
	remove_if_present(final);
	memset(expected, 0x5a, sizeof(expected));
	memset(sentinel, 0x7c, sizeof(sentinel));
	observed[0] = 0;
	memcpy(observed, sentinel, sizeof(observed));
	UT_ASSERT_EQ(cluster_undo_smgr_root_descriptor_probe(publication_dir, expected, observed),
				 CLUSTER_UNDO_SMGR_ROOT_MIRROR_ABSENT);
	UT_ASSERT_EQ(memcmp(observed, sentinel, sizeof(observed)), 0);

	fd = open(final, O_WRONLY | O_CREAT | O_EXCL | PG_BINARY, pg_file_create_mode);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(write(fd, expected, 511), 511);
	UT_ASSERT_EQ(close(fd), 0);
	memcpy(observed, sentinel, sizeof(observed));
	UT_ASSERT_EQ(cluster_undo_smgr_root_descriptor_probe(publication_dir, expected, observed),
				 CLUSTER_UNDO_SMGR_ROOT_MIRROR_HOLD);
	UT_ASSERT_EQ(memcmp(observed, sentinel, sizeof(observed)), 0);
	remove_if_present(final);

	UT_ASSERT(snprintf(target, sizeof(target), "%s/root-target", publication_dir)
			  < (int)sizeof(target));
	remove_if_present(target);
	fd = open(target, O_WRONLY | O_CREAT | O_EXCL | PG_BINARY, pg_file_create_mode);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(write(fd, expected, sizeof(expected)), sizeof(expected));
	UT_ASSERT_EQ(close(fd), 0);
	UT_ASSERT_EQ(symlink(target, final), 0);
	memcpy(observed, sentinel, sizeof(observed));
	UT_ASSERT_EQ(cluster_undo_smgr_root_descriptor_probe(publication_dir, expected, observed),
				 CLUSTER_UNDO_SMGR_ROOT_MIRROR_HOLD);
	UT_ASSERT_EQ(memcmp(observed, sentinel, sizeof(observed)), 0);
	remove_if_present(final);
	remove_if_present(target);
}

UT_TEST(test_root_descriptor_mirror_probe_rejects_lstat_open_symlink_swap)
{
	char final[MAXPGPATH];
	char target[MAXPGPATH];
	uint8 expected[512];
	uint8 sentinel[512];
	uint8 observed[512];
	int fd;

	resolve_root_descriptor(final);
	remove_if_present(final);
	UT_ASSERT(snprintf(target, sizeof(target), "%s/root-race-target", publication_dir)
			  < (int)sizeof(target));
	remove_if_present(target);
	memset(expected, 0x2d, sizeof(expected));
	memset(sentinel, 0xb6, sizeof(sentinel));

	fd = open(final, O_WRONLY | O_CREAT | O_EXCL | PG_BINARY, pg_file_create_mode);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(write(fd, sentinel, sizeof(sentinel)), sizeof(sentinel));
	UT_ASSERT_EQ(close(fd), 0);
	fd = open(target, O_WRONLY | O_CREAT | O_EXCL | PG_BINARY, pg_file_create_mode);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(write(fd, expected, sizeof(expected)), sizeof(expected));
	UT_ASSERT_EQ(close(fd), 0);

	strlcpy(basic_open_swap_target, target, sizeof(basic_open_swap_target));
	basic_open_swap_to_symlink = true;
	memcpy(observed, sentinel, sizeof(observed));
	UT_ASSERT_EQ(cluster_undo_smgr_root_descriptor_probe(publication_dir, expected, observed),
				 CLUSTER_UNDO_SMGR_ROOT_MIRROR_HOLD);
	UT_ASSERT(!basic_open_swap_to_symlink);
	UT_ASSERT_EQ(memcmp(observed, sentinel, sizeof(observed)), 0);

	remove_if_present(final);
	remove_if_present(target);
}

UT_TEST(test_root_descriptor_mirror_write_failure_cleans_owned_temp)
{
	char final[MAXPGPATH];
	uint8 image[512];

	resolve_root_descriptor(final);
	remove_if_present(final);
	memset(image, 0x4b, sizeof(image));
	pwrite_forced_error = true;
	UT_ASSERT_EQ(cluster_undo_smgr_root_descriptor_publish(publication_dir, image),
				 CLUSTER_UNDO_SMGR_ROOT_MIRROR_IO_ERROR);
	pwrite_forced_error = false;
	UT_ASSERT(access(final, F_OK) != 0 && errno == ENOENT);
	UT_ASSERT_EQ(count_root_descriptor_temps(), 0);
}

int
main(void)
{
	char template[] = "/tmp/pgrac-b0pub-XXXXXX";

	UT_ASSERT_NOT_NULL(mkdtemp(template));
	strlcpy(publication_dir, template, sizeof(publication_dir));
	UT_PLAN(26);
	UT_RUN(test_probe_distinguishes_absent_and_preserves_output);
	UT_RUN(test_probe_accepts_only_exact_full_identity);
	UT_RUN(test_probe_classifies_short_or_wrong_identity_as_invalid);
	UT_RUN(test_probe_reports_non_absent_open_failure_as_io_error);
	UT_RUN(test_probe_close_failure_overrides_invalid_size);
	UT_RUN(test_temp_is_same_directory_boot_unique_and_exactly_cleaned);
	UT_RUN(test_cleanup_rejects_final_path);
	UT_RUN(test_startup_cleanup_removes_boot_foreign_temp_and_fsyncs_directory);
	UT_RUN(test_startup_cleanup_refuses_current_boot_temp);
	UT_RUN(test_startup_cleanup_refuses_symlink_with_valid_foreign_name);
	UT_RUN(test_startup_cleanup_refuses_malformed_temp_and_preserves_final);
	UT_RUN(test_startup_cleanup_refuses_truncated_temp_marker);
	UT_RUN(test_startup_cleanup_empty_namespace_preserves_ordinary_final);
	UT_RUN(test_startup_cleanup_propagates_directory_close_failure);
	UT_RUN(test_startup_cleanup_propagates_directory_fsync_failure);
	UT_RUN(test_publish_is_no_replace_and_fsyncs_file_and_parent);
	UT_RUN(test_eexist_loser_never_overwrites_final_and_drops_own_temp);
	UT_RUN(test_eexist_invalid_final_is_never_repaired_or_overwritten);
	UT_RUN(test_eexist_retry_fsyncs_parent_after_prior_parent_fsync_failure);
	UT_RUN(test_temp_write_failure_closes_fd_and_removes_only_owned_temp);
	UT_RUN(test_root_descriptor_mirror_publish_is_exact_and_durable);
	UT_RUN(test_root_descriptor_mirror_eexist_is_validate_only);
	UT_RUN(test_root_descriptor_mirror_reads_exact_retry_candidate);
	UT_RUN(test_root_descriptor_mirror_probe_rejects_short_and_symlink);
	UT_RUN(test_root_descriptor_mirror_probe_rejects_lstat_open_symlink_swap);
	UT_RUN(test_root_descriptor_mirror_write_failure_cleans_owned_temp);
	UT_ASSERT_EQ(rmdir(publication_dir), 0);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
