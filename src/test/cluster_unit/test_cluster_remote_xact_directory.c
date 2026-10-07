/*-------------------------------------------------------------------------
 * Actual canonical projection directory qualification against temporary
 * files. Only authority observation and an injected fsync failure are fixtures.
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include "access/slru.h"
#include "common/file_perm.h"
#include "storage/fd.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

char *cluster_shared_data_dir;
int pg_dir_create_mode = S_IRWXU;
static int allowed_origin;
static unsigned syncs;
static bool sync_fail;
static char root[MAXPGPATH], native[MAXPGPATH], origin_dir[MAXPGPATH], leaf[MAXPGPATH];
static SlruCtlData control;

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "%s:%d: %s\n", file, line, condition);
	abort();
}

static bool
remote_xact_origin_writer_permitted(int node)
{
	return node == allowed_origin;
}

int
pg_fsync(int fd)
{
	syncs++;
	if (sync_fail) {
		errno = EIO;
		return -1;
	}
	return fsync(fd);
}

#include "test_cluster_remote_xact_directory.inc"

static void
setup(void)
{
	char temporary[] = "/tmp/pre2-remote-projection-XXXXXX";
	char *created = mkdtemp(temporary);

	if (created == NULL)
		abort();
	strlcpy(root, created, sizeof(root));
	/* A canonical route longer than the old 64-byte SLRU field. */
	snprintf(native, sizeof(native), "%s/native_side", root);
	snprintf(origin_dir, sizeof(origin_dir), "%s/origin_127", native);
	snprintf(leaf, sizeof(leaf), "%s/pg_xact_remote_v2", origin_dir);
	if (mkdir(native, 0700) != 0 || mkdir(origin_dir, 0700) != 0)
		abort();
	memset(&control, 0, sizeof(control));
	strlcpy(control.Dir, leaf, sizeof(control.Dir));
	cluster_shared_data_dir = root;
	allowed_origin = -1;
	syncs = 0;
	sync_fail = false;
}

static void
cleanup(void)
{
	struct stat st;

	if (lstat(leaf, &st) == 0) {
		if (S_ISLNK(st.st_mode))
			UT_ASSERT_EQ(unlink(leaf), 0);
		else
			UT_ASSERT_EQ(rmdir(leaf), 0);
	}
	UT_ASSERT_EQ(rmdir(origin_dir), 0);
	UT_ASSERT_EQ(rmdir(native), 0);
	UT_ASSERT_EQ(rmdir(root), 0);
}

UT_TEST(read_does_not_create_and_unqualified_write_has_no_effect)
{
	struct stat st;
	setup();
	UT_ASSERT(strlen(leaf) > 64);
	UT_ASSERT(remote_xact_directory_ready(&control, 127, false));
	UT_ASSERT_EQ(lstat(leaf, &st), -1);
	UT_ASSERT_EQ(errno, ENOENT);
	UT_ASSERT(!remote_xact_directory_ready(&control, 127, true));
	UT_ASSERT_EQ(lstat(leaf, &st), -1);
	UT_ASSERT_EQ(syncs, 0);
	cleanup();
}

UT_TEST(owner_creation_and_retry_require_parent_durability)
{
	struct stat st;
	setup();
	allowed_origin = 127;
	sync_fail = true;
	UT_ASSERT(!remote_xact_directory_ready(&control, 127, true));
	UT_ASSERT_EQ(lstat(leaf, &st), 0);
	UT_ASSERT(S_ISDIR(st.st_mode));
	UT_ASSERT_EQ(syncs, 1);
	sync_fail = false;
	UT_ASSERT(remote_xact_directory_ready(&control, 127, true));
	UT_ASSERT_EQ(syncs, 2);
	allowed_origin = -1;
	UT_ASSERT(remote_xact_directory_ready(&control, 127, false));
	UT_ASSERT_EQ(syncs, 2);
	UT_ASSERT(!remote_xact_directory_ready(&control, 127, true));
	UT_ASSERT_EQ(syncs, 2);
	cleanup();
}

UT_TEST(path_origin_and_symlink_mismatch_never_redirect)
{
	setup();
	allowed_origin = 127;
	UT_ASSERT(!remote_xact_directory_ready(&control, 1, true));
	UT_ASSERT_EQ(symlink(origin_dir, leaf), 0);
	UT_ASSERT(!remote_xact_directory_ready(&control, 127, true));
	UT_ASSERT(!remote_xact_directory_ready(&control, 127, false));
	UT_ASSERT_EQ(syncs, 0);
	cleanup();
}

UT_TEST(missing_or_writable_native_origin_is_not_adopted)
{
	setup();
	allowed_origin = 127;
	UT_ASSERT_EQ(chmod(origin_dir, 0770), 0);
	UT_ASSERT(!remote_xact_directory_ready(&control, 127, true));
	UT_ASSERT_EQ(syncs, 0);
	UT_ASSERT_EQ(chmod(origin_dir, 0700), 0);
	UT_ASSERT_EQ(rmdir(origin_dir), 0);
	UT_ASSERT(!remote_xact_directory_ready(&control, 127, true));
	UT_ASSERT_EQ(syncs, 0);
	UT_ASSERT_EQ(mkdir(origin_dir, 0700), 0);
	cleanup();
}

int
main(void)
{
	UT_PLAN(4);
	UT_RUN(read_does_not_create_and_unqualified_write_has_no_effect);
	UT_RUN(owner_creation_and_retry_require_parent_durability);
	UT_RUN(path_origin_and_symlink_mismatch_never_redirect);
	UT_RUN(missing_or_writable_native_origin_is_not_adopted);
	UT_DONE();
	return ut_failed_count != 0;
}
