/* Actual temporary-file reads; only fault/race timing is controlled. */
#include "postgres.h"
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

static ssize_t fixture_pread(int fd, void *bytes, size_t length, off_t offset);
#undef pg_pread
#define pg_pread fixture_pread
#ifdef CLOG_STORAGE_TEST_SOURCE
#include CLOG_STORAGE_TEST_SOURCE
#else
#include "../../backend/access/transam/clog_storage.c"
#endif

#undef printf
#undef fprintf
#undef snprintf
#include "unit_test.h"
UT_DEFINE_GLOBALS();

static uint8 image[272384], payload[8192], output[8192], saved_output[8192];
static ClusterClogFileBindingV2 binding;
static ClusterClogPageIdentityV2 page, observed, saved_observed;
static unsigned reads, fault_call;
static bool short_read, failed_read, change_header, extend_tail;
static int file;

void
ExceptionalCondition(const char *condition, const char *source, int line)
{
	fprintf(stderr, "%s at %s:%d\n", condition, source, line);
	abort();
}

static ssize_t
fixture_pread(int fd, void *bytes, size_t length, off_t offset)
{
	ssize_t result;
	reads++;
	if (reads == fault_call && failed_read) {
		errno = EIO;
		return -1;
	}
	if (reads == fault_call && short_read)
		return pread(fd, bytes, length - 1, offset);
	result = pread(fd, bytes, length, offset);
	if (reads == 2 && change_header) {
		ClusterClogFileBindingV2 changed = binding;
		uint8 header[8192];
		changed.identity.creation_generation++;
		if (!cluster_clog_file_header_encode_v2(&changed, header, sizeof(header)) ||
			pwrite(fd, header, sizeof(header), 0) != sizeof(header))
			abort();
	}
	if (reads == 2 && extend_tail) {
		if (pwrite(fd, image + 8192 + 8256, 1, 8192 + 8256) != 1)
			abort();
	}
	return result;
}

static void
reset(unsigned pages)
{
	char path[] = "/tmp/pgrac-clog-read-XXXXXX";
	memset(&binding, 0, sizeof(binding));
	binding.identity.kind = CLUSTER_RECOVERY_FILE_CLOG_V2;
	binding.identity.format_id = CLUSTER_RECOVERY_FORMAT_ID_V2;
	binding.identity.format_version = CLUSTER_RECOVERY_FORMAT_VERSION_V2;
	binding.identity.system_identifier = 123;
	binding.identity.database_incarnation = 456;
	binding.identity.storage_uuid[0] = 1;
	binding.identity.redo_branch_uuid[0] = 2;
	binding.identity.redo_branch_generation = 2;
	binding.identity.redo_timeline = 1;
	binding.identity.creation_generation = 3;
	binding.identity.registration_generation = 4;
	binding.identity.object_id = binding.segment = 2;
	memset(image, 0, sizeof(image));
	if (!cluster_clog_file_header_encode_v2(&binding, image, 8192))
		abort();
	for (unsigned i = 0; i < pages; i++) {
		uint64 offset;
		memset(&page, 0, sizeof(page));
		page.identity.logical_page = 64 + i;
		page.identity.page_generation = 9 + i;
		page.identity.birth_observed = true;
		page.identity.birth_full_xid = FullTransactionIdFromU64(
			(UINT64CONST(7) << 32) + page.identity.logical_page * 32768);
		page.identity.page_lsn = 80;
		page.image_lsn = 100;
		memset(payload, 0x30 + i, sizeof(payload));
		if (!cluster_clog_file_offset_v2(i, &offset) ||
			!cluster_clog_file_page_encode_v2(&binding, &page, payload, 8192,
											 image + offset, 8256))
			abort();
	}
	file = mkstemp(path);
	if (file < 0 || unlink(path) != 0 ||
		pwrite(file, image, 8192 + pages * 8256, 0) != (ssize_t)(8192 + pages * 8256))
		abort();
	memset(output, 0xa5, sizeof(output));
	memcpy(saved_output, output, sizeof(output));
	memset(&observed, 0x5a, sizeof(observed));
	saved_observed = observed;
	reads = fault_call = 0;
	short_read = failed_read = change_header = extend_tail = false;
}

static bool
read_page(uint64 number)
{
	return cluster_clog_storage_read_page_v2(file, &binding, number,
											   &observed, output, sizeof(output));
}

static void
refused_outputs(void)
{
	UT_ASSERT(memcmp(output, saved_output, sizeof(output)) == 0);
	UT_ASSERT(memcmp(&observed, &saved_observed, sizeof(observed)) == 0);
	UT_ASSERT(fcntl(file, F_GETFD) >= 0);
}

UT_TEST(first_and_last_frame_use_framed_offsets)
{
	reset(32);
	UT_ASSERT(read_page(64));
	UT_ASSERT_EQ(observed.identity.logical_page, 64);
	UT_ASSERT_EQ(output[0], 0x30);
	UT_ASSERT_EQ(output[8191], 0x30);
	UT_ASSERT(read_page(95));
	UT_ASSERT_EQ(observed.identity.logical_page, 95);
	UT_ASSERT_EQ(observed.identity.page_generation, 40);
	UT_ASSERT_EQ(U64FromFullTransactionId(observed.identity.birth_full_xid),
		(UINT64CONST(7) << 32) + UINT64CONST(95) * 32768);
	UT_ASSERT_EQ(observed.identity.page_lsn, 80);
	UT_ASSERT_EQ(observed.image_lsn, 100);
	UT_ASSERT_EQ(output[0], 0x4f);
	UT_ASSERT_EQ(output[8191], 0x4f);
	UT_ASSERT_EQ(lseek(file, 0, SEEK_CUR), 0);
	UT_ASSERT(fcntl(file, F_GETFD) >= 0);
	close(file);
}

UT_TEST(partly_extended_segment_keeps_existing_complete_pages)
{
	reset(3);
	UT_ASSERT(read_page(66));
	UT_ASSERT_EQ(output[0], 0x32);
	UT_ASSERT_EQ(observed.identity.logical_page, 66);
	close(file);
}

UT_TEST(expected_identity_is_not_supplied_by_disk)
{
	reset(1);
	binding.identity.origin++;
	UT_ASSERT(!read_page(64));
	refused_outputs();
	binding.identity.origin--;
	binding.identity.creation_generation++;
	UT_ASSERT(!read_page(64));
	refused_outputs();
	binding.identity.creation_generation--;
	binding.identity.redo_branch_uuid[0]++;
	UT_ASSERT(!read_page(64));
	refused_outputs();
	close(file);
}

UT_TEST(legacy_partial_and_extra_extents_are_refused)
{
	const off_t lengths[] = {8192, 8192 + 8256 - 1, 272385};
	for (unsigned i = 0; i < lengthof(lengths); i++) {
		reset(1);
		UT_ASSERT_EQ(ftruncate(file, lengths[i]), 0);
		UT_ASSERT(!read_page(64));
		refused_outputs();
		close(file);
	}
}

UT_TEST(header_frame_and_payload_corruption_preserve_outputs)
{
	const off_t offsets[] = {0, 20, 8192 + 16, 8192 + 48, 8192 + 64};
	for (unsigned i = 0; i < lengthof(offsets); i++) {
		uint8 bad;
		reset(1);
		bad = image[offsets[i]] ^ 1;
		UT_ASSERT_EQ(pwrite(file, &bad, 1, offsets[i]), 1);
		UT_ASSERT(!read_page(64));
		refused_outputs();
		close(file);
	}
}

UT_TEST(short_and_failed_reads_leave_borrowed_fd_and_outputs)
{
	for (unsigned i = 1; i <= 3; i++) {
		for (unsigned failure = 0; failure < 2; failure++) {
			reset(1);
			fault_call = i;
			short_read = failure == 0;
			failed_read = failure == 1;
			UT_ASSERT(!read_page(64));
			refused_outputs();
			close(file);
		}
	}
}

UT_TEST(late_header_change_does_not_publish_early_payload)
{
	reset(1);
	change_header = true;
	UT_ASSERT(!read_page(64));
	refused_outputs();
	close(file);
}

UT_TEST(bad_arguments_and_aliases_refuse_before_io)
{
	reset(1);
	UT_ASSERT(!read_page(63));
	UT_ASSERT(!read_page(96));
	UT_ASSERT(!cluster_clog_storage_read_page_v2(-1, &binding, 64, &observed, output, 8192));
	UT_ASSERT(!cluster_clog_storage_read_page_v2(file, NULL, 64, &observed, output, 8192));
	UT_ASSERT(!cluster_clog_storage_read_page_v2(file, &binding, 64, &observed, output, 8191));
	UT_ASSERT(!cluster_clog_storage_read_page_v2(file, &binding, 64,
		(ClusterClogPageIdentityV2 *)output, output, 8192));
	UT_ASSERT(!cluster_clog_storage_read_page_v2(file, &binding, 64,
		(ClusterClogPageIdentityV2 *)&binding, output, 8192));
	UT_ASSERT_EQ(reads, 0);
	refused_outputs();
	close(file);
}

UT_TEST(unrelated_extension_keeps_complete_target_readable)
{
	for (unsigned late = 0; late < 2; late++) {
		reset(2);
		UT_ASSERT_EQ(ftruncate(file, 8192 + 8256 + (late ? 0 : 1)), 0);
		extend_tail = late;
		UT_ASSERT(read_page(64));
		UT_ASSERT_EQ(observed.identity.logical_page, 64);
		UT_ASSERT_EQ(output[0], 0x30);
		UT_ASSERT_EQ(output[8191], 0x30);
		UT_ASSERT(fcntl(file, F_GETFD) >= 0);
		close(file);
	}
}

int
main(void)
{
	UT_PLAN(9);
	UT_RUN(first_and_last_frame_use_framed_offsets);
	UT_RUN(partly_extended_segment_keeps_existing_complete_pages);
	UT_RUN(expected_identity_is_not_supplied_by_disk);
	UT_RUN(legacy_partial_and_extra_extents_are_refused);
	UT_RUN(header_frame_and_payload_corruption_preserve_outputs);
	UT_RUN(short_and_failed_reads_leave_borrowed_fd_and_outputs);
	UT_RUN(late_header_change_does_not_publish_early_payload);
	UT_RUN(bad_arguments_and_aliases_refuse_before_io);
	UT_RUN(unrelated_extension_keeps_complete_target_readable);
	UT_DONE();
	return ut_failed_count != 0;
}
