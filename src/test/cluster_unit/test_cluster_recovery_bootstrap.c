/* Actual temporary-file BOOT I/O. Scheduling/fault boundaries are controlled. */
#ifdef FRONTEND
#include "postgres_fe.h"
#else
#include "postgres.h"
#endif
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include "access/clog_file.h"
#include "common/cluster_recovery_bootstrap.h"
#include "../../common/sha2_int.h"
#include "cluster/cluster_undo_segment_init.h"

ssize_t boot_test_pwrite(int, const void *, size_t, off_t);
ssize_t boot_test_pread(int, void *, size_t, off_t);
int boot_test_fsync(int);
int boot_test_truncate(int, off_t);
static unsigned writes, reads, syncs, truncates;
static unsigned fail_write, short_write;
static bool fail_read, short_read, fail_sync, fail_truncate, corrupt_tail, extra_byte;
#undef pg_pwrite
#undef pg_pread
#define pg_pwrite boot_test_pwrite
#define pg_pread boot_test_pread
#define fsync boot_test_fsync
#define ftruncate boot_test_truncate
#ifdef BOOTSTRAP_TEST_SOURCE
#include BOOTSTRAP_TEST_SOURCE
#else
#include "../../common/cluster_recovery_bootstrap.c"
#endif
#undef pg_pwrite
#undef pg_pread
#undef fsync
#undef ftruncate
#undef printf
#undef fprintf
#undef snprintf
#include "unit_test.h"
UT_DEFINE_GLOBALS();

#ifndef FRONTEND
void
ExceptionalCondition(const char *condition, const char *source, int line)
{
	fprintf(stderr, "%s at %s:%d\n", condition, source, line);
	abort();
}
#endif
ssize_t
boot_test_pwrite(int fd, const void *p, size_t n, off_t off)
{
	writes++;
	if (writes == fail_write) { errno = EIO; return -1; }
	if (writes == short_write) return pwrite(fd, p, n / 2, off);
	return pwrite(fd, p, n, off);
}
ssize_t
boot_test_pread(int fd, void *p, size_t n, off_t off)
{
	reads++;
	if (fail_read) { errno = EIO; return -1; }
	if (short_read) return pread(fd, p, n / 2, off);
	return pread(fd, p, n, off);
}
int
boot_test_fsync(int fd)
{
	struct stat st;

	syncs++;
	if (fail_sync) { errno = EIO; return -1; }
	if (fsync(fd) != 0 || fstat(fd, &st) != 0) abort();
	if (corrupt_tail && pwrite(fd, "X", 1, st.st_size - 1) != 1) abort();
	if (extra_byte && pwrite(fd, "X", 1, st.st_size) != 1) abort();
	return 0;
}
int
boot_test_truncate(int fd, off_t size)
{
	truncates++;
	if (fail_truncate) { errno = ENOSPC; return -1; }
	return ftruncate(fd, size);
}
static ClusterRecoveryFileIdentityV2 identity;
static ClusterRecoveryBootFileV2 out, sentinel;
static int fd;
static void
reset(ClusterRecoveryFileKindV2 kind)
{
	char path[] = "/tmp/pgrac-boot-XXXXXX";

	if (fd >= 0) close(fd);
	fd = mkstemp(path);
	if (fd < 0 || unlink(path) != 0 || lseek(fd, 7, SEEK_SET) != 7) abort();
	memset(&identity, 0, sizeof(identity));
	identity.kind = kind;
	identity.format_id = CLUSTER_RECOVERY_FORMAT_ID_V2;
	identity.format_version = CLUSTER_RECOVERY_FORMAT_VERSION_V2;
	identity.system_identifier = 101;
	identity.database_incarnation = 103;
	identity.storage_uuid[0] = 5;
	identity.redo_branch_uuid[0] = 6;
	identity.redo_branch_generation = 107;
	identity.redo_timeline = 2;
	identity.creation_generation = 109;
	identity.registration_generation = 113;
	memset(&sentinel, 0xA5, sizeof(sentinel));
	out = sentinel;
	writes = reads = syncs = truncates = fail_write = short_write = 0;
	fail_read = short_read = fail_sync = fail_truncate = corrupt_tail = extra_byte = false;
}
static bool
clog_create(void)
{
	return cluster_clog_bootstrap_v2(fd, &identity,
		FullTransactionIdFromU64(FirstNormalTransactionId), &out);
}
static off_t
size_now(void)
{
	struct stat st;

	if (fstat(fd, &st) != 0) abort();
	return st.st_size;
}
static void
independent_digest(uint8 digest[32])
{
	pg_sha256_ctx ctx;
	uint8 bytes[4096];
	off_t total = size_now();

	pg_sha256_init(&ctx);
	for (off_t pos = 0; pos < total;) {
		size_t n = Min((off_t)sizeof(bytes), total - pos);

		if (pread(fd, bytes, n, pos) != (ssize_t)n) abort();
		pg_sha256_update(&ctx, bytes, n);
		pos += n;
	}
	pg_sha256_final(&ctx, digest);
}
UT_TEST(clog_boot_sync_full_readback) {
	uint8 digest[32], header[8192], frame[8256], payload[8192];
	ClusterClogFileBindingV2 binding;
	ClusterClogPageIdentityV2 page;

	reset(CLUSTER_RECOVERY_FILE_CLOG_V2);
	UT_ASSERT(clog_create());
	UT_ASSERT_EQ(size_now(), 16448); UT_ASSERT_EQ(writes, 2); UT_ASSERT_EQ(syncs, 1);
	UT_ASSERT_EQ(reads, 2); UT_ASSERT_EQ(lseek(fd, 0, SEEK_CUR), 7);
	UT_ASSERT_EQ(out.synced_bytes, 16448);
	UT_ASSERT_EQ(out.identity.creation_generation, 109);
	UT_ASSERT_EQ(out.page.logical_page, 0); UT_ASSERT(out.page.birth_observed);
	UT_ASSERT_EQ(U64FromFullTransactionId(out.page.birth_full_xid), 3);
	UT_ASSERT_EQ(out.page.page_generation, 3); UT_ASSERT_EQ(out.page.page_lsn, 0);
	independent_digest(digest); UT_ASSERT(memcmp(digest, out.image_digest, 32) == 0);
	memset(&binding, 0, sizeof(binding)); binding.identity = identity;
	UT_ASSERT_EQ(pread(fd, header, sizeof(header), 0), sizeof(header));
	UT_ASSERT(cluster_clog_file_header_matches_v2(header, sizeof(header), &binding));
	UT_ASSERT_EQ(pread(fd, frame, sizeof(frame), 8192), sizeof(frame));
	UT_ASSERT(cluster_clog_file_page_decode_v2(&binding, 0, frame, sizeof(frame), &page, payload, sizeof(payload)));
	for (unsigned i = 0; i < sizeof(payload); i++) UT_ASSERT_EQ(payload[i], 0);
	UT_ASSERT_EQ(out.page.metadata_crc, page.identity.metadata_crc);
	UT_ASSERT_EQ(out.page.payload_crc, page.identity.payload_crc);
}
UT_TEST(clog_invalid_identity_and_birth_do_no_io) {
	for (unsigned variant = 0; variant < 9; variant++) {
		FullTransactionId next = FullTransactionIdFromU64(3);

		reset(CLUSTER_RECOVERY_FILE_CLOG_V2);
		switch (variant) {
			case 0: identity.creation_generation = 0; break;
			case 1: identity.kind = CLUSTER_RECOVERY_FILE_UNDO_V2; break;
			case 2: identity.object_id = 1; break;
			case 3: identity.origin = 128; break;
			case 4: identity.format_version = 1; break;
			case 5: next = FullTransactionIdFromU64(0); break;
			case 6: next = FullTransactionIdFromU64(4); break;
			case 7: next = FullTransactionIdFromU64(UINT64CONST(0x100000003)); break;
			case 8: identity.database_incarnation = 0; break;
		}
		UT_ASSERT(!cluster_clog_bootstrap_v2(fd, &identity, next, &out));
		UT_ASSERT_EQ(size_now(), 0); UT_ASSERT_EQ(writes + syncs + reads, 0);
		UT_ASSERT(memcmp(&out, &sentinel, sizeof(out)) == 0);
	}
}
UT_TEST(existing_partial_append_and_alias_refuse) {
	reset(CLUSTER_RECOVERY_FILE_CLOG_V2);
	UT_ASSERT_EQ(pwrite(fd, "old", 3, 0), 3);
	UT_ASSERT(!clog_create()); UT_ASSERT_EQ(writes, 0); UT_ASSERT_EQ(size_now(), 3);
	reset(CLUSTER_RECOVERY_FILE_CLOG_V2);
	UT_ASSERT_EQ(fcntl(fd, F_SETFL, O_APPEND), 0);
	UT_ASSERT(!clog_create()); UT_ASSERT_EQ(writes, 0);
	UT_ASSERT_EQ(fcntl(fd, F_SETFL, 0), 0);
	UT_ASSERT(!cluster_clog_bootstrap_v2(fd, &identity, FullTransactionIdFromU64(3),
		(ClusterRecoveryBootFileV2 *)&identity));
	UT_ASSERT_EQ(writes, 0);
	UT_ASSERT(!cluster_clog_bootstrap_v2(-1, &identity, FullTransactionIdFromU64(3), &out));
	UT_ASSERT(memcmp(&out, &sentinel, sizeof(out)) == 0);
}
UT_TEST(clog_faults_preserve_partial_fd_and_output) {
	for (unsigned variant = 0; variant < 8; variant++) {
		reset(CLUSTER_RECOVERY_FILE_CLOG_V2);
		switch (variant) {
			case 0: fail_write = 1; break;
			case 1: short_write = 1; break;
			case 2: fail_write = 2; break;
			case 3: short_write = 2; break;
			case 4: fail_sync = true; break;
			case 5: fail_read = true; break;
			case 6: short_read = true; break;
			case 7: corrupt_tail = true; break;
		}
		UT_ASSERT(!clog_create());
		UT_ASSERT(memcmp(&out, &sentinel, sizeof(out)) == 0);
		UT_ASSERT(fcntl(fd, F_GETFD) >= 0);
		if (variant > 1) UT_ASSERT(size_now() >= 8192);
	}
}
UT_TEST(clog_observe_valid_later_frame_is_readonly) {
	ClusterRecoveryBootFileV2 before;
	ClusterClogFileBindingV2 binding;
	ClusterClogPageIdentityV2 page;
	uint8 frame[8256], payload[8192] = {0}, digest[32];

	reset(CLUSTER_RECOVERY_FILE_CLOG_V2);
	UT_ASSERT(clog_create()); before = out;
	binding.identity = identity; binding.segment = 0;
	memset(&page, 0, sizeof(page)); page.identity = out.page;
	page.identity.page_lsn = 500; page.image_lsn = 490; payload[0] = 64;
	UT_ASSERT(cluster_clog_file_page_encode_v2(&binding, &page, payload, sizeof(payload), frame, sizeof(frame)));
	UT_ASSERT_EQ(pwrite(fd, frame, sizeof(frame), 8192), sizeof(frame));
	writes = syncs = 0;
	UT_ASSERT(cluster_clog_bootstrap_read_v2(fd, &identity, &out));
	UT_ASSERT_EQ(writes + syncs, 0); UT_ASSERT_EQ(out.page.page_lsn, 500);
	UT_ASSERT_EQ(U64FromFullTransactionId(out.page.birth_full_xid), 3);
	independent_digest(digest); UT_ASSERT(memcmp(digest, out.image_digest, 32) == 0);
	UT_ASSERT(memcmp(before.image_digest, out.image_digest, 32) != 0);
	UT_ASSERT_EQ(lseek(fd, 0, SEEK_CUR), 7);
}
UT_TEST(clog_observer_wrong_identity_corruption_and_extent_refuse) {
	for (unsigned variant = 0; variant < 5; variant++) {
		reset(CLUSTER_RECOVERY_FILE_CLOG_V2); UT_ASSERT(clog_create());
		if (variant == 0) identity.redo_branch_generation++;
		if (variant == 1) UT_ASSERT_EQ(pwrite(fd, "X", 1, 16447), 1);
		if (variant == 2) UT_ASSERT_EQ(ftruncate(fd, 16447), 0);
		if (variant == 3) UT_ASSERT_EQ(pwrite(fd, "X", 1, 16448), 1);
		if (variant == 4) fail_read = true;
		out = sentinel; writes = syncs = 0;
		UT_ASSERT(!cluster_clog_bootstrap_read_v2(fd, &identity, &out));
		UT_ASSERT(memcmp(&out, &sentinel, sizeof(out)) == 0); UT_ASSERT_EQ(writes + syncs, 0);
	}
}
UT_TEST(extra_bytes_after_sync_refuse) {
	reset(CLUSTER_RECOVERY_FILE_CLOG_V2); extra_byte = true;
	UT_ASSERT(!clog_create()); UT_ASSERT_EQ(size_now(), 16449);
	UT_ASSERT(memcmp(&out, &sentinel, sizeof(out)) == 0);
}
#ifdef USE_PGRAC_CLUSTER
UT_TEST(undo_original_seed_tt40_checksum_identity_full_digest) {
	PGAlignedBlock block;
	UndoSegmentHeaderDataV2 *header = (UndoSegmentHeaderDataV2 *)block.data;
	uint8 digest[32];
	ClusterRecoveryBootFileV2 first;

	reset(CLUSTER_RECOVERY_FILE_UNDO_V2);
	UT_ASSERT(cluster_undo_bootstrap_v2(fd, &identity, &out));
	UT_ASSERT_EQ(size_now(), UNDO_SEGMENT_SIZE_BYTES); UT_ASSERT_EQ(writes, 1);
	UT_ASSERT_EQ(syncs, 1); UT_ASSERT_EQ(truncates, 1); UT_ASSERT_EQ(reads, 8192);
	UT_ASSERT_EQ(out.synced_bytes, UNDO_SEGMENT_SIZE_BYTES);
	UT_ASSERT_EQ(pread(fd, block.data, BLCKSZ, 0), BLCKSZ);
	UT_ASSERT_EQ(header->segment_id, 0); UT_ASSERT_EQ(header->owner_instance, 1);
	UT_ASSERT_EQ(header->tt_layout_version, 2); UT_ASSERT_EQ(header->tt_slot_bytes, 40);
	UT_ASSERT_EQ(header->tt_active_count, 0); UT_ASSERT_EQ(header->tt_next_free_slot, 0);
	UT_ASSERT_EQ(cluster_undo_segment_header_integrity(block.data), CLUSTER_UNDO_HEADER_VALID);
	UT_ASSERT(cluster_recovery_file_identity_matches_v2((uint8 *)block.data + 3072, 128, &identity));
	UT_ASSERT(!out.page.birth_observed); UT_ASSERT_EQ(U64FromFullTransactionId(out.page.birth_full_xid), 0);
	UT_ASSERT_EQ(out.page.page_generation, 0); UT_ASSERT_EQ(out.page.page_lsn, 0);
	independent_digest(digest); UT_ASSERT(memcmp(digest, out.image_digest, 32) == 0);
	first = out; writes = syncs = truncates = 0;
	UT_ASSERT(cluster_undo_bootstrap_read_v2(fd, &identity, &out));
	UT_ASSERT(memcmp(&first, &out, sizeof(out)) == 0);
	UT_ASSERT_EQ(writes + syncs + truncates, 0); UT_ASSERT_EQ(lseek(fd, 0, SEEK_CUR), 7);
	/* The new BOOT helper must not relax existing runtime segment0 refusal. */
	UT_ASSERT(!cluster_undo_segment_make_header_v2_bytes(0, 1, block.data));
}
UT_TEST(undo_wrong_seed_and_tt32_refuse_without_writes) {
	PGAlignedBlock block;

	for (unsigned variant = 0; variant < 4; variant++) {
		reset(CLUSTER_RECOVERY_FILE_UNDO_V2);
		if (variant == 0) identity.origin = 1;
		if (variant == 1) identity.object_id = 1;
		if (variant == 2) identity.kind = CLUSTER_RECOVERY_FILE_CLOG_V2;
		if (variant == 3) {
			cluster_undo_segment_make_header_bytes(0, 1, block.data);
			UT_ASSERT_EQ(pwrite(fd, block.data, BLCKSZ, 0), BLCKSZ);
		}
		UT_ASSERT(!cluster_undo_bootstrap_v2(fd, &identity, &out));
		UT_ASSERT_EQ(writes + truncates + syncs, 0);
		UT_ASSERT(memcmp(&out, &sentinel, sizeof(out)) == 0);
	}
}
UT_TEST(undo_faults_and_last_byte_are_not_ignored) {
	for (unsigned variant = 0; variant < 6; variant++) {
		reset(CLUSTER_RECOVERY_FILE_UNDO_V2);
		if (variant == 0) fail_truncate = true;
		if (variant == 1) fail_sync = true;
		if (variant == 2) corrupt_tail = true;
		if (variant == 3) fail_read = true;
		if (variant == 4) short_read = true;
		if (variant == 5) extra_byte = true;
		UT_ASSERT(!cluster_undo_bootstrap_v2(fd, &identity, &out));
		UT_ASSERT(memcmp(&out, &sentinel, sizeof(out)) == 0); UT_ASSERT(fcntl(fd, F_GETFD) >= 0);
		if (variant != 0) UT_ASSERT(size_now() >= UNDO_SEGMENT_SIZE_BYTES);
	}
}
UT_TEST(undo_readonly_full_tail_and_checksum_recheck) {
	reset(CLUSTER_RECOVERY_FILE_UNDO_V2); UT_ASSERT(cluster_undo_bootstrap_v2(fd, &identity, &out));
	UT_ASSERT_EQ(pwrite(fd, "X", 1, UNDO_SEGMENT_SIZE_BYTES / 2), 1);
	out = sentinel; writes = syncs = truncates = 0;
	UT_ASSERT(!cluster_undo_bootstrap_read_v2(fd, &identity, &out));
	UT_ASSERT(memcmp(&out, &sentinel, sizeof(out)) == 0);
	UT_ASSERT_EQ(writes + syncs + truncates, 0);
	UT_ASSERT_EQ(pwrite(fd, "\0", 1, UNDO_SEGMENT_SIZE_BYTES / 2), 1);
	UT_ASSERT_EQ(pwrite(fd, "X", 1, 112), 1);
	UT_ASSERT(!cluster_undo_bootstrap_read_v2(fd, &identity, &out));
	UT_ASSERT_EQ(writes + syncs + truncates, 0);
}
#else
UT_TEST(undo_disabled_refuses_without_mutation) {
	reset(CLUSTER_RECOVERY_FILE_UNDO_V2);
	UT_ASSERT(!cluster_undo_bootstrap_v2(fd, &identity, &out));
	UT_ASSERT(!cluster_undo_bootstrap_read_v2(fd, &identity, &out));
	UT_ASSERT(memcmp(&out, &sentinel, sizeof(out)) == 0);
	UT_ASSERT_EQ(writes + syncs + reads + truncates, 0);
}
#endif
int
main(void)
{
	fd = -1;
#ifdef USE_PGRAC_CLUSTER
	UT_PLAN(11);
#else
	UT_PLAN(8);
#endif
	UT_RUN(clog_boot_sync_full_readback);
	UT_RUN(clog_invalid_identity_and_birth_do_no_io);
	UT_RUN(existing_partial_append_and_alias_refuse);
	UT_RUN(clog_faults_preserve_partial_fd_and_output);
	UT_RUN(clog_observe_valid_later_frame_is_readonly);
	UT_RUN(clog_observer_wrong_identity_corruption_and_extent_refuse);
	UT_RUN(extra_bytes_after_sync_refuse);
#ifdef USE_PGRAC_CLUSTER
	UT_RUN(undo_original_seed_tt40_checksum_identity_full_digest);
	UT_RUN(undo_wrong_seed_and_tt32_refuse_without_writes);
	UT_RUN(undo_faults_and_last_byte_are_not_ignored);
	UT_RUN(undo_readonly_full_tail_and_checksum_recheck);
#else
	UT_RUN(undo_disabled_refuses_without_mutation);
#endif
	close(fd); UT_DONE(); return ut_failed_count != 0;
}
