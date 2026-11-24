/* Whole-file CLOG sync/readback. Actual framing, file I/O and SHA256; only
 * failure/race timing and fsync configuration are finite test boundaries. */
#define main clog_storage_fixture_main
int clog_storage_fixture_main(void);
#include "test_cluster_clog_storage.c"
#undef main

#include "common/cryptohash.h"
#include "storage/fd.h"

static int sync_file_fsync(int fd);
#undef pg_fsync
#define pg_fsync sync_file_fsync
#ifdef CLOG_SYNC_TEST_SOURCE
#include CLOG_SYNC_TEST_SOURCE
#else
#include "../../backend/access/transam/clog_sync.c"
#endif
#undef pg_fsync

bool enableFsync;
static int directory_fd;
static unsigned sync_count, sync_fail_at, sync_change;
static ClusterClogFileSyncV2 sync_result, saved_sync_result;

static int
sync_file_fsync(int fd)
{
	int result;

	sync_count++;
	if (fd != (sync_count == 1 ? file : directory_fd)) abort();
	if (sync_count == sync_fail_at) { errno = EIO; return -1; }
	result = fsync(fd);
	if (sync_count == 1 && sync_change) {
		if (sync_change == 1) {
			/* A different, valid full frame with the same extent/header. */
			uint8 frame[8256];
			memset(payload, 0x79, sizeof(payload));
			page.identity.logical_page = 64;
			page.identity.birth_full_xid = FullTransactionIdFromU64(
				(UINT64CONST(7) << 32) + UINT64CONST(64) * 32768);
			if (!cluster_clog_file_page_encode_v2(&binding, &page, payload,
				8192, frame, sizeof(frame)) || pwrite(file, frame, sizeof(frame), 8192) != sizeof(frame)) abort();
		} else if (sync_change == 2) {
			if (ftruncate(file, 8192 + 8256) != 0) abort();
		} else if (sync_change == 3) {
			enableFsync = false;
		} else if (sync_change == 4) {
			uint8 bad = 0xff;
			if (pwrite(file, &bad, 1, 0) != 1) abort();
		}
	}
	return result;
}

static void
sync_reset(unsigned pages)
{
	reset(pages);
	directory_fd = open("/tmp", O_RDONLY | O_DIRECTORY);
	if (directory_fd < 0) abort();
	enableFsync = true;
	sync_count = sync_fail_at = sync_change = 0;
	memset(&sync_result, 0xa5, sizeof(sync_result));
	saved_sync_result = sync_result;
}

static void
sync_close(void)
{
	UT_ASSERT(fcntl(file, F_GETFD) >= 0);
	UT_ASSERT(fcntl(directory_fd, F_GETFD) >= 0);
	close(file);
	close(directory_fd);
}

static bool
sync_file(void)
{
	return cluster_clog_storage_sync_file_v2(file, directory_fd, &binding, &sync_result);
}

UT_TEST(sync_complete_extent_returns_actual_identity_digest_and_watermarks)
{
	uint8 digest[32];
	pg_cryptohash_ctx *hash;

	sync_reset(32);
	/* A later physical page supplies both maxima, rather than the first. */
	page.identity.page_lsn = 700;
	page.image_lsn = 900;
	UT_ASSERT(cluster_clog_file_page_encode_v2(&binding, &page, payload, 8192,
		image + 8192 + 31 * 8256, 8256));
	UT_ASSERT_EQ(pwrite(file, image, sizeof(image), 0), sizeof(image));
	/* Independent one-shot hash of the fixture's complete original image. */
	hash = pg_cryptohash_create(PG_SHA256);
	UT_ASSERT(hash && pg_cryptohash_init(hash) == 0);
	UT_ASSERT_EQ(pg_cryptohash_update(hash, image, sizeof(image)), 0);
	UT_ASSERT_EQ(pg_cryptohash_final(hash, digest, sizeof(digest)), 0);
	pg_cryptohash_free(hash);
	UT_ASSERT(sync_file());
	UT_ASSERT_EQ(sync_count, 2);
	UT_ASSERT(memcmp(&sync_result.binding, &binding, sizeof(binding)) == 0);
	UT_ASSERT_EQ(sync_result.synced_bytes, 272384);
	UT_ASSERT_EQ(sync_result.extent_pages, 32);
	UT_ASSERT_EQ(sync_result.materialized_pages, UINT32_MAX);
	UT_ASSERT_EQ(sync_result.max_page_lsn, 700);
	UT_ASSERT_EQ(sync_result.max_image_lsn, 900);
	UT_ASSERT(memcmp(sync_result.image_digest, digest, 32) == 0);
	UT_ASSERT_EQ(lseek(file, 0, SEEK_CUR), 0);
	sync_close();
}

UT_TEST(sync_sparse_slots_do_not_supply_page_birth)
{
	sync_reset(4);
	memset(image + 8192, 0, 3 * 8256);
	UT_ASSERT_EQ(pwrite(file, image, 8192 + 4 * 8256, 0), 8192 + 4 * 8256);
	UT_ASSERT(sync_file());
	UT_ASSERT_EQ(sync_result.synced_bytes, 41216);
	UT_ASSERT_EQ(sync_result.extent_pages, 4);
	UT_ASSERT_EQ(sync_result.materialized_pages, 8);
	UT_ASSERT_EQ(sync_result.max_image_lsn, 100);
	sync_close();
}

UT_TEST(sync_empty_partial_corrupt_and_foreign_identity_refuse)
{
	for (unsigned fault = 0; fault < 7; fault++) {
		sync_reset(2);
		if (fault == 0) UT_ASSERT_EQ(ftruncate(file, 8192), 0);
		if (fault == 1) UT_ASSERT_EQ(ftruncate(file, 8192 + 8256 - 1), 0);
		if (fault == 2) UT_ASSERT_EQ(ftruncate(file, 272385), 0);
		if (fault == 3) { uint8 bad = 0xff; UT_ASSERT_EQ(pwrite(file, &bad, 1, 8192 + 64), 1); }
		if (fault == 4) binding.identity.origin++;
		if (fault == 5) { memset(image + 8192, 0, 2 * 8256); UT_ASSERT_EQ(pwrite(file, image, 24704, 0), 24704); }
		if (fault == 6) binding.identity.creation_generation++;
		UT_ASSERT(!sync_file());
		UT_ASSERT(memcmp(&sync_result, &saved_sync_result, sizeof(sync_result)) == 0);
		UT_ASSERT_EQ(sync_count, 0);
		sync_close();
	}
}

UT_TEST(sync_file_and_directory_failures_keep_outputs_and_borrowed_fds)
{
	for (unsigned failure = 1; failure <= 2; failure++) {
		sync_reset(2);
		sync_fail_at = failure;
		UT_ASSERT(!sync_file());
		UT_ASSERT_EQ(sync_count, failure);
		UT_ASSERT(memcmp(&sync_result, &saved_sync_result, sizeof(sync_result)) == 0);
		sync_close();
	}
}

UT_TEST(sync_valid_replacement_extent_header_and_fsync_drift_refuse)
{
	for (unsigned fault = 1; fault <= 4; fault++) {
		sync_reset(2);
		sync_change = fault;
		UT_ASSERT(!sync_file());
		UT_ASSERT(memcmp(&sync_result, &saved_sync_result, sizeof(sync_result)) == 0);
		sync_close();
	}
}

UT_TEST(sync_short_or_failed_whole_read_never_publishes_a_prefix)
{
	for (unsigned call = 1; call <= 6; call++) {
		for (unsigned failure = 0; failure < 2; failure++) {
			sync_reset(2);
			fault_call = call;
			short_read = failure == 0;
			failed_read = failure == 1;
			UT_ASSERT(!sync_file());
			UT_ASSERT(memcmp(&sync_result, &saved_sync_result, sizeof(sync_result)) == 0);
			sync_close();
		}
	}
}

UT_TEST(sync_disabled_and_invalid_or_aliased_inputs_do_no_io)
{
	sync_reset(2);
	enableFsync = false;
	UT_ASSERT(!sync_file());
	enableFsync = true;
	UT_ASSERT(!cluster_clog_storage_sync_file_v2(-1, directory_fd, &binding, &sync_result));
	UT_ASSERT(!cluster_clog_storage_sync_file_v2(file, -1, &binding, &sync_result));
	UT_ASSERT(!cluster_clog_storage_sync_file_v2(file, directory_fd, NULL, &sync_result));
	UT_ASSERT(!cluster_clog_storage_sync_file_v2(file, directory_fd, &binding, NULL));
	UT_ASSERT(!cluster_clog_storage_sync_file_v2(file, directory_fd, &binding,
		(ClusterClogFileSyncV2 *) &binding));
	UT_ASSERT_EQ(reads + sync_count, 0);
	UT_ASSERT(memcmp(&sync_result, &saved_sync_result, sizeof(sync_result)) == 0);
	UT_ASSERT(!cluster_clog_storage_sync_file_v2(file, file, &binding, &sync_result));
	sync_close();
}

int
main(void)
{
	UT_PLAN(7);
	UT_RUN(sync_complete_extent_returns_actual_identity_digest_and_watermarks);
	UT_RUN(sync_sparse_slots_do_not_supply_page_birth);
	UT_RUN(sync_empty_partial_corrupt_and_foreign_identity_refuse);
	UT_RUN(sync_file_and_directory_failures_keep_outputs_and_borrowed_fds);
	UT_RUN(sync_valid_replacement_extent_header_and_fsync_drift_refuse);
	UT_RUN(sync_short_or_failed_whole_read_never_publishes_a_prefix);
	UT_RUN(sync_disabled_and_invalid_or_aliased_inputs_do_no_io);
	UT_DONE();
	return ut_failed_count != 0;
}
