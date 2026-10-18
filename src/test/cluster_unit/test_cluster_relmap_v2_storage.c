/*-------------------------------------------------------------------------
 *
 * test_cluster_relmap_v2_storage.c
 *    Real filesystem tests of the production relmap v2 storage entry points.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_relmap_v2_storage.c
 *
 * NOTES
 *    Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "cluster/cluster_relmap_authority.h"
#include "miscadmin.h"
#include "storage/fd.h"

#undef printf
#undef fprintf
#undef snprintf
#undef vsnprintf
#include "unit_test.h"

UT_DEFINE_GLOBALS();

char *cluster_shared_data_dir;
bool enableFsync = true;

static char root[MAXPGPATH];
static char directory[MAXPGPATH];
static unsigned int writes, renames, file_syncs, dir_syncs, reads;
static unsigned int fail_write, fail_rename, fail_file_sync, fail_dir_sync;
static bool corrupt_after_rename, replace_during_read, replace_after_rename;
static const char *const names[] = {
	"pgrac_relmap_authority", "pgrac_relmap_authority.bak",
	"pgrac_relmap_authority.tmp", "pgrac_relmap_authority.bak.tmp", "replacement"
};
static const ClusterRelmapV2Namespace identity = { 12345, 67890, InvalidOid, true };
static const ClusterRelmapV2Owner owner = { 3, UINT64CONST(0x200000123), 0, 101, 909, 202 };

/* Only backend facilities are supplied here.  Every successful I/O is real. */
void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "unexpected Assert: %s at %s:%d\n", condition, file, line);
	abort();
}

bool
errstart(int elevel, const char *domain pg_attribute_unused())
{
	if (elevel >= ERROR)
		abort();
	return false;
}

bool
errstart_cold(int elevel, const char *domain)
{
	return errstart(elevel, domain);
}

void
errfinish(const char *file pg_attribute_unused(), int line pg_attribute_unused(),
		  const char *func pg_attribute_unused())
{}

int
errcode_for_file_access(void)
{
	return 0;
}

int
errmsg(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

int
OpenTransientFile(const char *name, int flags)
{
	return open(name, flags, 0600);
}

int
CloseTransientFile(int fd)
{
	return close(fd);
}

int
durable_rename(const char *old, const char *new, int elevel pg_attribute_unused())
{
	/* The v1 implementation is linked but is never called by these tests. */
	abort();
}

int
pg_fsync(int fd)
{
	struct stat st;
	unsigned int call;
	unsigned int fail;

	if (fstat(fd, &st) != 0)
		abort();
	if (S_ISDIR(st.st_mode)) {
		call = ++dir_syncs;
		fail = fail_dir_sync;
	} else {
		call = ++file_syncs;
		fail = fail_file_sync;
	}
	if (fail != 0 && call == fail) {
		errno = EIO;
		return -1;
	}
	return fsync(fd);
}

/* Same product source compiled with syscall names redirected, no body copy. */
ssize_t relmap_test_write(int fd, const void *bytes, size_t len);
ssize_t relmap_test_read(int fd, void *bytes, size_t len);
int relmap_test_renameat(int olddir, const char *old, int newdir, const char *new);

ssize_t
relmap_test_write(int fd, const void *bytes, size_t len)
{
	if (++writes == fail_write) {
		errno = ENOSPC;
		return -1;
	}
	return write(fd, bytes, len);
}

static void
replace_primary(void)
{
	char path[MAXPGPATH];
	char replacement[MAXPGPATH];
	unsigned char bytes[2176];
	int fd;

	snprintf(path, sizeof(path), "%s/%s", directory, names[0]);
	snprintf(replacement, sizeof(replacement), "%s/replacement", directory);
	fd = open(path, O_RDONLY);
	if (fd < 0 || read(fd, bytes, sizeof(bytes)) != sizeof(bytes) || close(fd) != 0)
		abort();
	fd = open(replacement, O_WRONLY | O_CREAT | O_EXCL, 0600);
	if (fd < 0 || write(fd, bytes, sizeof(bytes)) != sizeof(bytes) || close(fd) != 0
		|| rename(replacement, path) != 0)
		abort();
}

ssize_t
relmap_test_read(int fd, void *bytes, size_t len)
{
	ssize_t n = read(fd, bytes, len);

	reads++;
	if (replace_during_read && reads == 1)
		replace_primary();
	return n;
}

int
relmap_test_renameat(int olddir, const char *old, int newdir, const char *new)
{
	int rc;

	if (++renames == fail_rename) {
		errno = EIO;
		return -1;
	}
	rc = renameat(olddir, old, newdir, new);
	if (rc == 0 && renames == 1 && replace_after_rename)
		replace_primary();
	if (rc == 0 && renames == 1 && corrupt_after_rename) {
		int fd = openat(newdir, new, O_WRONLY);
		unsigned char bad = 0xff;

		if (fd < 0 || pwrite(fd, &bad, 1, 128) != 1 || close(fd) != 0)
			abort();
	}
	return rc;
}

static void
cleanup(void)
{
	char path[MAXPGPATH];
	const char *dirs[] = { "global", "base/42" };

	if (root[0] == '\0')
		return;
	for (size_t d = 0; d < lengthof(dirs); d++) {
		for (size_t n = 0; n < lengthof(names); n++) {
			snprintf(path, sizeof(path), "%s/%s/%s", root, dirs[d], names[n]);
			unlink(path);
		}
		snprintf(path, sizeof(path), "%s/%s", root, dirs[d]);
		rmdir(path);
	}
	snprintf(path, sizeof(path), "%s/base", root);
	rmdir(path);
	rmdir(root);
	root[0] = '\0';
}

static RelMapFile
map_image(RelFileNumber number)
{
	RelMapFile map;

	memset(&map, 0, sizeof(map));
	map.magic = RELMAPPER_FILEMAGIC;
	map.num_mappings = 1;
	map.mappings[0].mapoid = 1259;
	map.mappings[0].mapfilenumber = number;
	INIT_CRC32C(map.crc);
	COMP_CRC32C(map.crc, &map, offsetof(RelMapFile, crc));
	FIN_CRC32C(map.crc);
	return map;
}

static void
write_copy(int copy, const unsigned char *bytes, size_t len)
{
	char path[MAXPGPATH];
	int fd;

	snprintf(path, sizeof(path), "%s/%s", directory, names[copy]);
	fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0600);
	if (fd < 0 || write(fd, bytes, len) != len || fsync(fd) != 0 || close(fd) != 0)
		abort();
}

static void
read_copy(int copy, unsigned char bytes[2176])
{
	char path[MAXPGPATH];
	struct stat st;
	int fd;

	snprintf(path, sizeof(path), "%s/%s", directory, names[copy]);
	fd = open(path, O_RDONLY);
	if (fd < 0 || fstat(fd, &st) != 0 || st.st_size != 2176
		|| read(fd, bytes, 2176) != 2176 || close(fd) != 0)
		abort();
}

static void
setup(const ClusterRelmapV2Namespace *ns)
{
	ClusterRelmapV2Image image;
	unsigned char bytes[2176];
	char path[MAXPGPATH];

	cleanup();
	strcpy(root, "/tmp/pgrac_relmap_v2_XXXXXX");
	if (mkdtemp(root) == NULL)
		abort();
	snprintf(path, sizeof(path), "%s/global", root);
	if (mkdir(path, 0700) != 0)
		abort();
	snprintf(path, sizeof(path), "%s/base", root);
	if (mkdir(path, 0700) != 0)
		abort();
	snprintf(path, sizeof(path), "%s/base/42", root);
	if (mkdir(path, 0700) != 0)
		abort();
	snprintf(directory, sizeof(directory), "%s/%s", root, ns->shared ? "global" : "base/42");
	cluster_shared_data_dir = root;
	enableFsync = true;
	writes = renames = file_syncs = dir_syncs = reads = 0;
	fail_write = fail_rename = fail_file_sync = fail_dir_sync = 0;
	corrupt_after_rename = replace_during_read = replace_after_rename = false;
	memset(&image, 0, sizeof(image));
	image.identity = *ns;
	image.committed_generation = 7;
	image.committed = map_image(40000);
	if (cluster_relmap_v2_encode(&image, bytes, sizeof(bytes)) != CLUSTER_RELMAP_V2_VALID)
		abort();
	write_copy(0, bytes, sizeof(bytes));
	write_copy(1, bytes, sizeof(bytes));
}

static bool
stage(const ClusterRelmapV2Namespace *ns)
{
	RelMapFile map = map_image(40001);

	return cluster_relmap_authority_v2_write_pending(ns, 8, &owner, &map);
}

UT_TEST(test_stage_both_copies_before_success)
{
	ClusterRelmapV2View view;
	ClusterRelmapV2Image disk;
	unsigned char primary[2176];
	unsigned char backup[2176];

	setup(&identity);
	UT_ASSERT(stage(&identity));
	read_copy(0, primary);
	read_copy(1, backup);
	UT_ASSERT(memcmp(primary, backup, sizeof(primary)) == 0);
	UT_ASSERT_EQ(cluster_relmap_v2_decode(backup, sizeof(backup), &identity, &disk),
				 CLUSTER_RELMAP_V2_VALID);
	UT_ASSERT_EQ(disk.pending_generation, 8);
	UT_ASSERT_EQ(disk.pending_owner.top_full_xid, UINT64CONST(0x200000123));
	UT_ASSERT_EQ(disk.pending_owner.boot_incarnation, 202);
	UT_ASSERT_EQ(disk.pending_owner.admitted_incarnation, 101);
	UT_ASSERT_EQ(disk.pending_owner.membership_epoch, 0);
	UT_ASSERT_EQ(disk.pending_owner.wal_end, 909);
	UT_ASSERT_EQ(cluster_relmap_authority_v2_read(&identity, &view), CLUSTER_RELMAP_V2_PAIR_VALID);
	UT_ASSERT_EQ(view.committed_generation, 7);
	UT_ASSERT_EQ(view.committed.mappings[0].mapfilenumber, 40000);
	UT_ASSERT_EQ(view.pending.mappings[0].mapfilenumber, 40001);
	UT_ASSERT(view.pending_complete && view.has_pending);
	UT_ASSERT_EQ(file_syncs, 2);
	UT_ASSERT_EQ(dir_syncs, 2);
}

UT_TEST(test_per_database_namespace)
{
	ClusterRelmapV2Namespace local = identity;
	ClusterRelmapV2View view;

	local.shared = false;
	local.dbid = 42;
	setup(&local);
	UT_ASSERT(stage(&local));
	UT_ASSERT_EQ(cluster_relmap_authority_v2_read(&local, &view), CLUSTER_RELMAP_V2_PAIR_VALID);
	UT_ASSERT(view.has_pending && view.pending_complete);
	UT_ASSERT_EQ(view.pending_owner.node, 3);
}

UT_TEST(test_missing_authority_is_never_seeded)
{
	char path[MAXPGPATH];

	setup(&identity);
	for (int i = 0; i < 2; i++) {
		snprintf(path, sizeof(path), "%s/%s", directory, names[i]);
		UT_ASSERT_EQ(unlink(path), 0);
	}
	UT_ASSERT(!stage(&identity));
	UT_ASSERT_EQ(writes, 0);
	for (int i = 0; i < 2; i++) {
		snprintf(path, sizeof(path), "%s/%s", directory, names[i]);
		UT_ASSERT_EQ(access(path, F_OK), -1);
	}
}

UT_TEST(test_fallback_is_incomplete_and_cannot_stage)
{
	ClusterRelmapV2View view;
	unsigned char bytes[2176];

	setup(&identity);
	read_copy(0, bytes);
	bytes[128] ^= 1;
	write_copy(0, bytes, sizeof(bytes));
	UT_ASSERT_EQ(cluster_relmap_authority_v2_read(&identity, &view),
				 CLUSTER_RELMAP_V2_PAIR_DEGRADED);
	UT_ASSERT(view.used_backup && !view.pending_complete && !view.has_pending);
	UT_ASSERT_EQ(view.committed.mappings[0].mapfilenumber, 40000);
	UT_ASSERT(!stage(&identity));
	UT_ASSERT_EQ(writes, 0);
	write_copy(0, bytes, 100);
	UT_ASSERT_EQ(cluster_relmap_authority_v2_read(&identity, &view),
				 CLUSTER_RELMAP_V2_PAIR_DEGRADED);
	UT_ASSERT(!stage(&identity));
}

UT_TEST(test_wrong_identity_format_or_owner_does_not_write)
{
	ClusterRelmapV2Namespace wrong = identity;
	ClusterRelmapV2Owner bad_owner = owner;
	RelMapFile map = map_image(40001);
	unsigned char bytes[2176];
	ClusterRelmapV2View view;

	setup(&identity);
	wrong.database_incarnation++;
	UT_ASSERT(!stage(&wrong));
	UT_ASSERT(!cluster_relmap_authority_v2_write_pending(&identity, 9, &owner, &map));
	bad_owner.boot_incarnation = 0;
	UT_ASSERT(!cluster_relmap_authority_v2_write_pending(&identity, 8, &bad_owner, &map));
	enableFsync = false;
	UT_ASSERT(!stage(&identity));
	enableFsync = true;
	read_copy(0, bytes);
	bytes[0] = 0xa9;
	bytes[1] = 0xde;
	bytes[2] = 0x40;
	bytes[3] = 0x01; /* v1 must not fall back to the valid v2 backup. */
	write_copy(0, bytes, sizeof(bytes));
	UT_ASSERT_EQ(cluster_relmap_authority_v2_read(&identity, &view), CLUSTER_RELMAP_V2_PAIR_INVALID);
	UT_ASSERT(!stage(&identity));
	UT_ASSERT_EQ(writes, 0);
}

UT_TEST(test_existing_pending_is_never_overwritten)
{
	unsigned char before[2176];
	unsigned char after[2176];

	setup(&identity);
	UT_ASSERT(stage(&identity));
	read_copy(0, before);
	UT_ASSERT(!stage(&identity));
	read_copy(0, after);
	UT_ASSERT(memcmp(before, after, sizeof(before)) == 0);
	UT_ASSERT_EQ(renames, 2);
}

UT_TEST(test_symlink_copies_and_temp_are_rejected)
{
	char path[MAXPGPATH];
	char target[MAXPGPATH];
	ClusterRelmapV2View view;

	for (int i = 0; i < 3; i++) {
		setup(&identity);
		snprintf(path, sizeof(path), "%s/%s", directory, names[i]);
		snprintf(target, sizeof(target), "%s/%s", directory, names[1 - (i == 1)]);
		if (i < 2)
			UT_ASSERT_EQ(unlink(path), 0);
		UT_ASSERT_EQ(symlink(target, path), 0);
		if (i < 2)
			UT_ASSERT_EQ(cluster_relmap_authority_v2_read(&identity, &view),
						 CLUSTER_RELMAP_V2_PAIR_INVALID);
		UT_ASSERT(!stage(&identity));
		UT_ASSERT_EQ(writes, 0);
	}
}

UT_TEST(test_inode_replaced_during_read_is_not_accepted)
{
	ClusterRelmapV2View view;
	ClusterRelmapV2View zero;

	setup(&identity);
	replace_during_read = true;
	memset(&view, 0xa5, sizeof(view));
	memset(&zero, 0, sizeof(zero));
	UT_ASSERT_EQ(cluster_relmap_authority_v2_read(&identity, &view), CLUSTER_RELMAP_V2_PAIR_INVALID);
	UT_ASSERT(memcmp(&view, &zero, sizeof(view)) == 0);
}

UT_TEST(test_each_io_failure_retains_the_obligation)
{
	ClusterRelmapV2View view;

	/* Fail each copy's write/file sync/rename/directory sync in the real path. */
	for (int kind = 0; kind < 4; kind++) {
		for (int copy = 1; copy <= 2; copy++) {
			setup(&identity);
			if (kind == 0)
				fail_write = copy;
			else if (kind == 1)
				fail_file_sync = copy;
			else if (kind == 2)
				fail_rename = copy;
			else
				fail_dir_sync = copy;
			UT_ASSERT(!stage(&identity));
			UT_ASSERT_EQ(cluster_relmap_authority_v2_read(&identity, &view),
						 CLUSTER_RELMAP_V2_PAIR_VALID);
			UT_ASSERT_EQ(view.committed.mappings[0].mapfilenumber, 40000);
			/* Primary replacement is already possible at the first dir sync. */
			UT_ASSERT_EQ(view.has_pending, copy == 2 || kind == 3);
		}
	}
}

UT_TEST(test_corrupt_or_replaced_readback_never_acknowledges_stage)
{
	setup(&identity);
	corrupt_after_rename = true;
	UT_ASSERT(!stage(&identity));
	UT_ASSERT_EQ(renames, 1);
	setup(&identity);
	replace_after_rename = true;
	UT_ASSERT(!stage(&identity));
	UT_ASSERT_EQ(renames, 1);
}

int
main(void)
{
	UT_PLAN(10);
	UT_RUN(test_stage_both_copies_before_success);
	UT_RUN(test_per_database_namespace);
	UT_RUN(test_missing_authority_is_never_seeded);
	UT_RUN(test_fallback_is_incomplete_and_cannot_stage);
	UT_RUN(test_wrong_identity_format_or_owner_does_not_write);
	UT_RUN(test_existing_pending_is_never_overwritten);
	UT_RUN(test_symlink_copies_and_temp_are_rejected);
	UT_RUN(test_inode_replaced_during_read_is_not_accepted);
	UT_RUN(test_each_io_failure_retains_the_obligation);
	UT_RUN(test_corrupt_or_replaced_readback_never_acknowledges_stage);
	cleanup();
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
