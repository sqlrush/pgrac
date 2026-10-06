/* Real original-tree bytes and syscall fault boundaries.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"
#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include "unit_test.h"
#include "../../backend/cluster/cluster_initdb_tree_private.h"
UT_DEFINE_GLOBALS();
void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	abort();
}
static char fixture_directory[MAXPGPATH];
static int fixture_root;

static unsigned read_fault;
static bool same_tick, rewrite_observed;
static struct stat rewrite_stat;

/* Some filesystems report unchanged timestamps for an equal-length rewrite
 * in one tick. Keep the real write, and make that syscall result deterministic. */
static void
same_tick_stat(struct stat *st)
{
	if (!rewrite_observed || st->st_dev != rewrite_stat.st_dev || st->st_ino != rewrite_stat.st_ino)
		return;
#ifdef __APPLE__
	st->st_mtimespec = rewrite_stat.st_mtimespec;
	st->st_ctimespec = rewrite_stat.st_ctimespec;
#else
	st->st_mtim = rewrite_stat.st_mtim;
	st->st_ctim = rewrite_stat.st_ctim;
#endif
}
static int
fault_fstat(int fd, struct stat *st)
{
	int result = fstat(fd, st);
	if (result == 0)
		same_tick_stat(st);
	return result;
}
static int
fault_fstatat(int fd, const char *name, struct stat *st, int flags)
{
	int result = fstatat(fd, name, st, flags);
	if (result == 0)
		same_tick_stat(st);
	return result;
}
static ssize_t
fault_pread(int fd, void *bytes, size_t length, off_t offset)
{
	if (read_fault == 1) {
		errno = EIO;
		return -1;
	}
	if (read_fault == 2)
		return 0;
	if (read_fault == 3) {
		int writer = openat(fixture_root, "mutating", O_WRONLY);
		ssize_t n = pread(fd, bytes, length, offset);
		if (same_tick && fstat(fd, &rewrite_stat) != 0)
			abort();
		if (writer < 0 || pwrite(writer, "X", 1, 0) != 1 || close(writer) != 0)
			abort();
		rewrite_observed = same_tick;
		read_fault = 0;
		return n;
	}
	return pread(fd, bytes, length, offset);
}
#define pread fault_pread
#define fstat fault_fstat
#define fstatat fault_fstatat
#include "../../backend/cluster/cluster_initdb_tree.c"
#undef pread
#undef fstat
#undef fstatat

static void
put(const char *name, const char *value)
{
	int fd = openat(fixture_root, name, O_WRONLY | O_CREAT | O_TRUNC, 0600);
	UT_ASSERT(fd >= 0 && write(fd, value, strlen(value)) == strlen(value) && close(fd) == 0);
}

static bool
zero(const void *value, size_t length)
{
	const uint8 *p = value;
	for (size_t i = 0; i < length; i++)
		if (p[i])
			return false;
	return true;
}

UT_TEST(empty_and_sorted_tree)
{
	ClusterInitdbTree first, again;
	/* Independently calculated for TREE domain plus a, a./z, a/z and .hidden. */
	static const uint8 expected[32]
		= { 0xb2, 0x16, 0x7a, 0x04, 0xfd, 0x97, 0x3c, 0x59, 0x0d, 0x6f, 0xc7,
			0x1c, 0x85, 0xf2, 0x3c, 0x5f, 0x9d, 0x48, 0x6d, 0x65, 0x0c, 0x39,
			0x46, 0xe1, 0xb6, 0x5d, 0x32, 0xc7, 0xd8, 0xc9, 0xcd, 0xe9 };
	UT_ASSERT(cluster_initdb_tree_read(fixture_root, false, &first));
	UT_ASSERT(!zero(first.content, 32));
	UT_ASSERT(mkdirat(fixture_root, "a", 0700) == 0 && mkdirat(fixture_root, "a.", 0700) == 0);
	put("a/z", "one");
	put("a./z", "two");
	put(".hidden", "secret");
	UT_ASSERT(cluster_initdb_tree_read(fixture_root, false, &first));
	UT_ASSERT(memcmp(first.content, expected, 32) == 0);
	UT_ASSERT(cluster_initdb_tree_read(fixture_root, false, &again));
	UT_ASSERT(memcmp(&first, &again, sizeof(first)) == 0);
	put("a/z", "ONE");
	UT_ASSERT(cluster_initdb_tree_read(fixture_root, false, &again));
	UT_ASSERT(memcmp(first.content, again.content, 32) != 0);
	UT_ASSERT(unlinkat(fixture_root, "a/z", 0) == 0 && unlinkat(fixture_root, "a./z", 0) == 0);
	UT_ASSERT(unlinkat(fixture_root, ".hidden", 0) == 0);
	UT_ASSERT(unlinkat(fixture_root, "a", AT_REMOVEDIR) == 0
			  && unlinkat(fixture_root, "a.", AT_REMOVEDIR) == 0);
}

UT_TEST(alias_and_unsafe_files_are_not_observations)
{
	ClusterInitdbTree out;
	for (unsigned i = 0; i < 5; i++) {
		put("source", "keep");
		if (i == 0)
			UT_ASSERT(symlinkat("source", fixture_root, "alias") == 0);
		if (i == 1)
			UT_ASSERT(linkat(fixture_root, "source", fixture_root, "alias", 0) == 0);
		if (i == 2)
			UT_ASSERT(mkfifoat(fixture_root, "alias", 0600) == 0);
		if (i == 3)
			UT_ASSERT(fchmodat(fixture_root, "source", 0666, 0) == 0);
		if (i == 4)
			UT_ASSERT(fchmod(fixture_root, 0777) == 0);
		memset(&out, 0xa5, sizeof(out));
		UT_ASSERT(!cluster_initdb_tree_read(fixture_root, false, &out));
		UT_ASSERT(zero(&out, sizeof(out)));
		UT_ASSERT(fchmod(fixture_root, 0700) == 0);
		(void)unlinkat(fixture_root, "alias", 0);
		UT_ASSERT(unlinkat(fixture_root, "source", 0) == 0);
	}
}

UT_TEST(recheck_omits_only_creator_derived_objects)
{
	ClusterInitdbTree before, after;
	UT_ASSERT(mkdirat(fixture_root, "global", 0700) == 0);
	put("global/source", "original");
	UT_ASSERT(cluster_initdb_tree_read(fixture_root, false, &before));
	UT_ASSERT(mkdirat(fixture_root, "global/wal_startup", 0700) == 0);
	put("global/wal_startup/input", "derived");
	put("global/pgrac_control_root.bak", "derived root");
	UT_ASSERT(cluster_initdb_tree_read(fixture_root, true, &after));
	UT_ASSERT(memcmp(before.content, after.content, 32) == 0);
	UT_ASSERT(memcmp(before.identity, after.identity, 32) == 0);
	UT_ASSERT(cluster_initdb_tree_read(fixture_root, false, &after));
	UT_ASSERT(memcmp(before.content, after.content, 32) != 0);
	put("global/unexpected", "extra");
	UT_ASSERT(cluster_initdb_tree_read(fixture_root, true, &after));
	UT_ASSERT(memcmp(before.content, after.content, 32) != 0);
	UT_ASSERT(unlinkat(fixture_root, "global/unexpected", 0) == 0);
	put("global/source", "changed");
	UT_ASSERT(cluster_initdb_tree_read(fixture_root, true, &after));
	UT_ASSERT(memcmp(before.content, after.content, 32) != 0);
	UT_ASSERT(unlinkat(fixture_root, "global/source", 0) == 0);
	UT_ASSERT(unlinkat(fixture_root, "global/pgrac_control_root.bak", 0) == 0);
	UT_ASSERT(unlinkat(fixture_root, "global/wal_startup/input", 0) == 0);
	UT_ASSERT(unlinkat(fixture_root, "global/wal_startup", AT_REMOVEDIR) == 0);
	UT_ASSERT(unlinkat(fixture_root, "global", AT_REMOVEDIR) == 0);
}

UT_TEST(changes_and_io_failure_clear_observation)
{
	ClusterInitdbTree out;
	for (unsigned fault = 1; fault <= 3; fault++) {
		put("mutating", "original");
		read_fault = fault;
		memset(&out, 0xa5, sizeof(out));
		UT_ASSERT(!cluster_initdb_tree_read(fixture_root, false, &out));
		UT_ASSERT(zero(&out, sizeof(out)));
		read_fault = 0;
		UT_ASSERT(unlinkat(fixture_root, "mutating", 0) == 0);
	}
}

UT_TEST(recursion_can_fill_and_grow_the_inventory)
{
	ClusterInitdbTree a, b;
	char name[80];
	UT_ASSERT(mkdirat(fixture_root, "many", 0700) == 0);
	for (unsigned i = 0; i < 130; i++) {
		snprintf(name, sizeof(name), "many/file_%03u", i);
		put(name, "page");
	}
	UT_ASSERT(cluster_initdb_tree_read(fixture_root, false, &a));
	UT_ASSERT(cluster_initdb_tree_read(fixture_root, false, &b));
	UT_ASSERT(memcmp(&a, &b, sizeof(a)) == 0);
	for (unsigned i = 0; i < 130; i++) {
		snprintf(name, sizeof(name), "many/file_%03u", i);
		UT_ASSERT(unlinkat(fixture_root, name, 0) == 0);
	}
	UT_ASSERT(unlinkat(fixture_root, "many", AT_REMOVEDIR) == 0);
}

UT_TEST(same_tick_rewrite_clears_observation)
{
	ClusterInitdbTree out;
	char first;
	int fd;
	put("mutating", "original");
	same_tick = true;
	read_fault = 3;
	memset(&out, 0xa5, sizeof(out));
	UT_ASSERT(!cluster_initdb_tree_read(fixture_root, false, &out));
	UT_ASSERT(zero(&out, sizeof(out)));
	UT_ASSERT(rewrite_observed && read_fault == 0);
	fd = openat(fixture_root, "mutating", O_RDONLY);
	UT_ASSERT(fd >= 0 && pread(fd, &first, 1, 0) == 1 && first == 'X' && close(fd) == 0);
	same_tick = rewrite_observed = false;
	UT_ASSERT(unlinkat(fixture_root, "mutating", 0) == 0);
}

int
main(void)
{
	strlcpy(fixture_directory, "/tmp/pgrac-tree-XXXXXX", sizeof(fixture_directory));
	if (!mkdtemp(fixture_directory))
		return 2;
	fixture_root = open(fixture_directory, O_RDONLY | O_DIRECTORY);
	if (fixture_root < 0)
		return 2;
	UT_PLAN(6);
	UT_RUN(empty_and_sorted_tree);
	UT_RUN(alias_and_unsafe_files_are_not_observations);
	UT_RUN(recheck_omits_only_creator_derived_objects);
	UT_RUN(changes_and_io_failure_clear_observation);
	UT_RUN(recursion_can_fill_and_grow_the_inventory);
	UT_RUN(same_tick_rewrite_clears_observation);
	UT_ASSERT(close(fixture_root) == 0 && rmdir(fixture_directory) == 0);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
