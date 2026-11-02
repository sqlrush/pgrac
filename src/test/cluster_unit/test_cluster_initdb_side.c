/* Native SIDE creation: real files, hash readback, syscall-only faults.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres_fe.h"
#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include "common/logging.h"
#include "unit_test.h"
#include "common/cluster_recovery_bootstrap.h"
#include "access/clog_file.h"

UT_DEFINE_GLOBALS();
enum Fault { NONE, WRITE_FAIL, SYNC_FAIL, CORRUPT, SHORT_READ };
static enum Fault fault;
static int written_fd = -1;
static unsigned writes, file_syncs;
static char root[MAXPGPATH];
static int root_fd, source_fd, shared_fd;

static ssize_t
test_pwrite(int fd, const void *data, size_t size, off_t off)
{
	++writes;
	written_fd = fd;
	if (fault == WRITE_FAIL) {
		errno = ENOSPC;
		return -1;
	}
	return pwrite(fd, data, size, off);
}
static ssize_t
test_pread(int fd, void *data, size_t size, off_t off)
{
	if (fault == SHORT_READ && fd == written_fd)
		return 0;
	return pread(fd, data, size, off);
}
static int
test_fsync(int fd)
{
	struct stat st;
	UT_ASSERT(fstat(fd, &st) == 0);
	if (S_ISREG(st.st_mode)) {
		++file_syncs;
		if (fault == SYNC_FAIL) {
			errno = EIO;
			return -1;
		}
		if (fault == CORRUPT) {
			char byte = 'X';
			UT_ASSERT(pwrite(fd, &byte, 1, 77) == 1);
		}
	}
	return fsync(fd);
}

#define pwrite test_pwrite
#define pread test_pread
#define fsync test_fsync
#include "../../bin/initdb/pgrac_side.c"
#undef pwrite
#undef pread
#undef fsync

static void
prepare(void)
{
	int dirs[7];
	PGAlignedBlock page;
	fault = NONE;
	writes = file_syncs = 0;
	written_fd = -1;
	memset(page.data, 'a', BLCKSZ);
	strlcpy(root, "/tmp/pgrac-initdb-side-XXXXXX", sizeof(root));
	UT_ASSERT(mkdtemp(root) != NULL);
	root_fd = open(root, O_RDONLY | O_DIRECTORY);
	UT_ASSERT(root_fd >= 0);
	UT_ASSERT(mkdirat(root_fd, "source", 0700) == 0 && mkdirat(root_fd, "shared", 0700) == 0);
	dirs[0] = source_fd = open_directory(root_fd, "source");
	shared_fd = open_directory(root_fd, "shared");
	UT_ASSERT(source_fd >= 0 && shared_fd >= 0);
	for (unsigned i = 0; i < lengthof(directories); ++i) {
		UT_ASSERT(mkdirat(dirs[parents[i]], directories[i], 0700) == 0);
		dirs[i + 1] = open_directory(dirs[parents[i]], directories[i]);
		UT_ASSERT(dirs[i + 1] >= 0);
		if (i != 2 && i != 3) {
			int fd = openat(dirs[i + 1], "0000", O_CREAT | O_EXCL | O_WRONLY, 0600);
			UT_ASSERT(fd >= 0 && write(fd, page.data, BLCKSZ) == BLCKSZ && close(fd) == 0);
		}
	}
	for (unsigned i = 1; i < lengthof(dirs); ++i)
		close(dirs[i]);
}

/* Only the tree made by this fixture; lstat never follows a test alias. */
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
static bool
target_absent(void)
{
	struct stat st;
	return fstatat(shared_fd, "native_side", &st, AT_SYMLINK_NOFOLLOW) != 0 && errno == ENOENT;
}

UT_TEST(actual_copy_sync_and_readback)
{
	prepare();
	UT_ASSERT(pgrac_initdb_side_create(source_fd, shared_fd));
	UT_ASSERT(writes == 4 && file_syncs == 4);
	UT_ASSERT(!target_absent());
	cleanup();
}
UT_TEST(existing_namespace_is_unchanged)
{
	struct stat st;
	prepare();
	UT_ASSERT(mkdirat(shared_fd, "native_side", 0700) == 0);
	UT_ASSERT(!pgrac_initdb_side_create(source_fd, shared_fd) && writes == 0);
	UT_ASSERT(fstatat(shared_fd, "native_side/origin_0", &st, 0) != 0 && errno == ENOENT);
	cleanup();
}
UT_TEST(source_alias_refuses_before_mutation)
{
	prepare();
	UT_ASSERT(renameat(source_fd, "pg_xact/0000", source_fd, "original") == 0);
	UT_ASSERT(symlinkat("../original", source_fd, "pg_xact/0000") == 0);
	UT_ASSERT(!pgrac_initdb_side_create(source_fd, shared_fd) && target_absent() && writes == 0);
	cleanup();
}
UT_TEST(source_partial_page_refuses_before_mutation)
{
	int fd;
	prepare();
	fd = openat(source_fd, "pg_subtrans/0000", O_WRONLY);
	UT_ASSERT(fd >= 0 && ftruncate(fd, BLCKSZ - 1) == 0 && close(fd) == 0);
	UT_ASSERT(!pgrac_initdb_side_create(source_fd, shared_fd) && target_absent() && writes == 0);
	cleanup();
}
UT_TEST(unsupported_commit_ts_input_refuses)
{
	PGAlignedBlock page = { 0 };
	int fd;
	prepare();
	fd = openat(source_fd, "pg_commit_ts/0000", O_WRONLY | O_CREAT | O_EXCL, 0600);
	UT_ASSERT(fd >= 0 && write(fd, page.data, BLCKSZ) == BLCKSZ && close(fd) == 0);
	UT_ASSERT(!pgrac_initdb_side_create(source_fd, shared_fd) && target_absent() && writes == 0);
	cleanup();
}
static void
io_failure(enum Fault value)
{
	prepare();
	fault = value;
	UT_ASSERT(!pgrac_initdb_side_create(source_fd, shared_fd));
	UT_ASSERT(writes > 0);
	if (value != WRITE_FAIL)
		UT_ASSERT(file_syncs > 0);
	cleanup();
}
UT_TEST(write_failure_refuses)
{
	io_failure(WRITE_FAIL);
}
UT_TEST(fsync_failure_refuses)
{
	io_failure(SYNC_FAIL);
}
UT_TEST(changed_readback_refuses)
{
	io_failure(CORRUPT);
}
UT_TEST(short_readback_refuses)
{
	io_failure(SHORT_READ);
}

UT_TEST(peer_copies_its_own_original_source)
{
	int native, file;
	char byte;
	prepare();
	UT_ASSERT(pgrac_initdb_side_create(source_fd, shared_fd));
	native = open_directory(shared_fd, "native_side");
	UT_ASSERT(native >= 0);
	file = openat(source_fd, "pg_xact/0000", O_WRONLY);
	UT_ASSERT(file >= 0 && pwrite(file, "b", 1, 0) == 1 && fsync(file) == 0 && close(file) == 0);
	UT_ASSERT(pgrac_initdb_side_origin_create(source_fd, native, 127));
	file = openat(native, "origin_127/pg_xact/0000", O_RDONLY);
	UT_ASSERT(file >= 0 && read(file, &byte, 1) == 1 && byte == 'b' && close(file) == 0);
	file = openat(native, "origin_0/pg_xact/0000", O_RDONLY);
	UT_ASSERT(file >= 0 && read(file, &byte, 1) == 1 && byte == 'a' && close(file) == 0);
	writes = 0;
	UT_ASSERT(!pgrac_initdb_side_origin_create(source_fd, native, 127) && writes == 0);
	UT_ASSERT(!pgrac_initdb_side_origin_create(source_fd, native, 0) && writes == 0);
	UT_ASSERT(!pgrac_initdb_side_origin_create(source_fd, native, 128) && writes == 0);
	UT_ASSERT(close(native) == 0);
	cleanup();
}

UT_TEST(peer_alias_is_not_adopted)
{
	int native;
	prepare();
	UT_ASSERT(pgrac_initdb_side_create(source_fd, shared_fd));
	native = open_directory(shared_fd, "native_side");
	UT_ASSERT(native >= 0 && symlinkat("origin_0", native, "origin_1") == 0);
	writes = 0;
	UT_ASSERT(!pgrac_initdb_side_origin_create(source_fd, native, 1) && writes == 0);
	UT_ASSERT(close(native) == 0);
	cleanup();
}

UT_TEST(peer_failed_write_cannot_be_adopted)
{
	int native;
	prepare();
	UT_ASSERT(pgrac_initdb_side_create(source_fd, shared_fd));
	native = open_directory(shared_fd, "native_side");
	UT_ASSERT(native >= 0);
	writes = 0;
	fault = SYNC_FAIL;
	UT_ASSERT(!pgrac_initdb_side_origin_create(source_fd, native, 1) && writes > 0);
	writes = 0;
	fault = NONE;
	UT_ASSERT(!pgrac_initdb_side_origin_create(source_fd, native, 1) && writes == 0);
	UT_ASSERT(close(native) == 0);
	cleanup();
}

static ClusterRecoveryFileIdentityV2
managed_source(uint16 origin)
{
	ClusterRecoveryFileIdentityV2 id = {0};
	ClusterRecoveryBootFileV2 observed;
	int fd;

	id.kind = CLUSTER_RECOVERY_FILE_CLOG_V2;
	id.format_id = CLUSTER_RECOVERY_FORMAT_ID_V2;
	id.format_version = CLUSTER_RECOVERY_FORMAT_VERSION_V2;
	id.system_identifier = 101;
	id.database_incarnation = 103;
	id.storage_uuid[0] = 5; id.redo_branch_uuid[0] = 6;
	id.redo_branch_generation = 107; id.redo_timeline = 2;
	id.creation_generation = 109; id.registration_generation = 113;
	id.origin = origin;
	fd = openat(source_fd, "pg_xact/0000", O_RDWR | O_TRUNC | O_NOFOLLOW);
	UT_ASSERT(fd >= 0 && cluster_clog_bootstrap_v2(fd, &id,
		FullTransactionIdFromU64(3), &observed));
	UT_ASSERT_EQ(observed.synced_bytes, 16448);
	UT_ASSERT(close(fd) == 0);
	return id;
}

UT_TEST(managed_clog_copies_complete_framed_file)
{
	ClusterRecoveryFileIdentityV2 id;
	ClusterRecoveryBootFileV2 source = {0}, target = {0};
	int fd;

	prepare();
	id = managed_source(0);
	UT_ASSERT(pgrac_initdb_side_create_v2(source_fd, shared_fd, &id));
	fd = openat(source_fd, "pg_xact/0000", O_RDONLY | O_NOFOLLOW);
	UT_ASSERT(fd >= 0 && cluster_clog_bootstrap_read_v2(fd, &id, &source));
	UT_ASSERT(close(fd) == 0);
	fd = openat(shared_fd, "native_side/origin_0/pg_xact/0000", O_RDONLY | O_NOFOLLOW);
	UT_ASSERT(fd >= 0 && cluster_clog_bootstrap_read_v2(fd, &id, &target));
	UT_ASSERT(close(fd) == 0);
	UT_ASSERT_EQ(target.synced_bytes, 16448);
	UT_ASSERT(memcmp(source.image_digest, target.image_digest, 32) == 0);
	UT_ASSERT_EQ(writes, 6);
	UT_ASSERT_EQ(file_syncs, 4);
	cleanup();
}

UT_TEST(managed_peer_keeps_its_selected_origin_identity)
{
	ClusterRecoveryFileIdentityV2 founder, peer;
	ClusterRecoveryBootFileV2 observed;
	int native, fd;

	prepare();
	founder = managed_source(0);
	UT_ASSERT(pgrac_initdb_side_create_v2(source_fd, shared_fd, &founder));
	native = open_directory(shared_fd, "native_side");
	UT_ASSERT(native >= 0);
	peer = managed_source(127);
	UT_ASSERT(!pgrac_initdb_side_origin_create_v2(source_fd, native, 127, &founder));
	UT_ASSERT(pgrac_initdb_side_origin_create_v2(source_fd, native, 127, &peer));
	fd = openat(native, "origin_127/pg_xact/0000", O_RDONLY | O_NOFOLLOW);
	UT_ASSERT(fd >= 0 && cluster_clog_bootstrap_read_v2(fd, &peer, &observed));
	UT_ASSERT(!cluster_clog_bootstrap_read_v2(fd, &founder, &observed));
	UT_ASSERT(close(fd) == 0);
	fd = openat(native, "origin_0/pg_xact/0000", O_RDONLY | O_NOFOLLOW);
	UT_ASSERT(fd >= 0 && cluster_clog_bootstrap_read_v2(fd, &founder, &observed));
	UT_ASSERT(close(fd) == 0);
	writes = 0;
	UT_ASSERT(!pgrac_initdb_side_origin_create_v2(source_fd, native, 127, &peer));
	UT_ASSERT(!pgrac_initdb_side_origin_create_v2(source_fd, native, 0, &founder));
	UT_ASSERT(!pgrac_initdb_side_origin_create_v2(source_fd, native, 128, &peer));
	UT_ASSERT_EQ(writes, 0);
	UT_ASSERT(close(native) == 0);
	cleanup();
}

UT_TEST(managed_expected_and_framing_refuse_before_creation)
{
	for (unsigned variant = 0; variant < 24; variant++)
	{
		ClusterRecoveryFileIdentityV2 id;
		int fd;

		prepare();
		id = managed_source(0);
		fd = openat(source_fd, "pg_xact/0000", O_RDWR | O_NOFOLLOW);
		UT_ASSERT(fd >= 0);
		switch (variant)
		{
			case 0: id.kind = CLUSTER_RECOVERY_FILE_UNDO_V2; break;
			case 1: id.format_id++; break;
			case 2: id.format_version++; break;
			case 3: id.system_identifier++; break;
			case 4: id.database_incarnation++; break;
			case 5: id.storage_uuid[15]++; break;
			case 6: id.redo_branch_uuid[15]++; break;
			case 7: id.redo_branch_generation++; break;
			case 8: id.redo_timeline++; break;
			case 9: id.database_oid++; break;
			case 10: id.tablespace_oid++; break;
			case 11: id.origin++; break;
			case 12: id.object_id++; break;
			case 13: id.creation_generation++; break;
			case 14: id.registration_generation++; break;
			case 15: UT_ASSERT(ftruncate(fd, 16447) == 0); break;
			case 16: UT_ASSERT(ftruncate(fd, 16449) == 0); break;
			case 17: UT_ASSERT(pwrite(fd, "X", 1, 0) == 1); break;
			case 18: UT_ASSERT(pwrite(fd, "X", 1, 8192 + 63) == 1); break;
			case 19: UT_ASSERT(pwrite(fd, "X", 1, 16447) == 1); break;
			case 20: UT_ASSERT(unlinkat(source_fd, "pg_xact/0000", 0) == 0); break;
			case 21: UT_ASSERT(linkat(source_fd, "pg_xact/0000", source_fd, "alias", 0) == 0); break;
			case 22:
				UT_ASSERT(renameat(source_fd, "pg_xact/0000", source_fd, "original") == 0);
				UT_ASSERT(symlinkat("../original", source_fd, "pg_xact/0000") == 0);
				break;
			case 23:
			{
				int extra = openat(source_fd, "pg_xact/0001", O_WRONLY | O_CREAT | O_EXCL, 0600);
				UT_ASSERT(extra >= 0 && write(extra, "x", 1) == 1 && close(extra) == 0);
				break;
			}
		}
		UT_ASSERT(close(fd) == 0);
		UT_ASSERT(!pgrac_initdb_side_create_v2(source_fd, shared_fd, &id));
		UT_ASSERT(!pgrac_initdb_side_create_v2(source_fd, shared_fd, NULL));
		UT_ASSERT(target_absent() && writes == 0);
		cleanup();
	}
}

UT_TEST(managed_io_failure_preserves_source_and_unadoptable_target)
{
	static const enum Fault faults[] = {WRITE_FAIL, SYNC_FAIL, CORRUPT, SHORT_READ};

	for (unsigned i = 0; i < lengthof(faults); i++)
	{
		ClusterRecoveryFileIdentityV2 id;
		ClusterRecoveryBootFileV2 before, after;
		int fd;

		prepare();
		id = managed_source(0);
		fd = openat(source_fd, "pg_xact/0000", O_RDONLY | O_NOFOLLOW);
		UT_ASSERT(fd >= 0 && cluster_clog_bootstrap_read_v2(fd, &id, &before));
		fault = faults[i];
		UT_ASSERT(!pgrac_initdb_side_create_v2(source_fd, shared_fd, &id));
		UT_ASSERT(writes > 0);
		UT_ASSERT(cluster_clog_bootstrap_read_v2(fd, &id, &after));
		UT_ASSERT(memcmp(before.image_digest, after.image_digest, 32) == 0);
		UT_ASSERT(close(fd) == 0);
		fault = NONE;
		writes = 0;
		UT_ASSERT(!pgrac_initdb_side_create_v2(source_fd, shared_fd, &id));
		UT_ASSERT_EQ(writes, 0);
		cleanup();
	}
}

UT_TEST(managed_postbootstrap_frame_and_local_seed_boundary)
{
	ClusterRecoveryFileIdentityV2 id;
	ClusterClogFileBindingV2 binding = {0};
	ClusterClogPageIdentityV2 page;
	uint8 frame[CLUSTER_CLOG_FRAME_BYTES], payload[CLUSTER_CLOG_PAGE_BYTES];
	struct stat st;
	int fd, seed;

	prepare();
	id = managed_source(0);
	binding.identity = id;
	fd = openat(source_fd, "pg_xact/0000", O_RDWR | O_NOFOLLOW);
	UT_ASSERT(fd >= 0 && pread(fd, frame, sizeof(frame), 8192) == sizeof(frame));
	UT_ASSERT(cluster_clog_file_page_decode_v2(&binding, 0, frame, sizeof(frame),
		&page, payload, sizeof(payload)));
	payload[0] = 0x44;
	page.identity.page_lsn = page.image_lsn = 100;
	UT_ASSERT(cluster_clog_file_page_encode_v2(&binding, &page, payload, sizeof(payload),
		frame, sizeof(frame)));
	UT_ASSERT(pwrite(fd, frame, sizeof(frame), 8192) == sizeof(frame) && fsync(fd) == 0);
	UT_ASSERT(close(fd) == 0);
	UT_ASSERT(mkdirat(source_fd, "pg_undo", 0700) == 0);
	seed = openat(source_fd, "pg_undo/seed", O_CREAT | O_EXCL | O_WRONLY, 0600);
	UT_ASSERT(seed >= 0 && write(seed, "local TT seed", 13) == 13 && close(seed) == 0);
	writes = file_syncs = 0;
	UT_ASSERT(!pgrac_initdb_side_create(source_fd, shared_fd));
	UT_ASSERT(target_absent() && writes == 0);
	UT_ASSERT(pgrac_initdb_side_create_v2(source_fd, shared_fd, &id));
	fd = openat(shared_fd, "native_side/origin_0/pg_xact/0000", O_RDONLY | O_NOFOLLOW);
	UT_ASSERT(fd >= 0 && pread(fd, frame, sizeof(frame), 8192) == sizeof(frame));
	UT_ASSERT(cluster_clog_file_page_decode_v2(&binding, 0, frame, sizeof(frame),
		&page, payload, sizeof(payload)));
	UT_ASSERT_EQ(payload[0], 0x44);
	UT_ASSERT_EQ(page.identity.page_lsn, 100);
	UT_ASSERT_EQ(page.identity.page_generation, 3);
	UT_ASSERT_EQ(U64FromFullTransactionId(page.identity.birth_full_xid), 3);
	UT_ASSERT(close(fd) == 0);
	UT_ASSERT(fstatat(shared_fd, "native_side/origin_0/pg_undo", &st, 0) != 0 && errno == ENOENT);
	cleanup();
	prepare();
	UT_ASSERT(!pgrac_initdb_side_create_v2(source_fd, shared_fd, &id));
	UT_ASSERT(target_absent() && writes == 0);
	cleanup();
}

int
main(int argc, char **argv)
{
	pg_logging_init(argv[0]);
	UT_PLAN(17);
	UT_RUN(actual_copy_sync_and_readback);
	UT_RUN(existing_namespace_is_unchanged);
	UT_RUN(source_alias_refuses_before_mutation);
	UT_RUN(source_partial_page_refuses_before_mutation);
	UT_RUN(unsupported_commit_ts_input_refuses);
	UT_RUN(write_failure_refuses);
	UT_RUN(fsync_failure_refuses);
	UT_RUN(changed_readback_refuses);
	UT_RUN(short_readback_refuses);
	UT_RUN(peer_copies_its_own_original_source);
	UT_RUN(peer_alias_is_not_adopted);
	UT_RUN(peer_failed_write_cannot_be_adopted);
	UT_RUN(managed_clog_copies_complete_framed_file);
	UT_RUN(managed_peer_keeps_its_selected_origin_identity);
	UT_RUN(managed_expected_and_framing_refuse_before_creation);
	UT_RUN(managed_io_failure_preserves_source_and_unadoptable_target);
	UT_RUN(managed_postbootstrap_frame_and_local_seed_boundary);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
