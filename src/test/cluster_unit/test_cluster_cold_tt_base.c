/*-------------------------------------------------------------------------
 * test_cluster_cold_tt_base.c
 *    Real original-owner TT input capture against sparse temporary files.
 *
 * The product implementation is included unchanged below, with only read
 * and close boundaries redirected for I/O faults. Path resolution and
 * allocation are fixtures; this does not run recovery or prove outcomes.
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1
#include "postgres.h"

#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "cluster/cluster_cold_recovery.h"
#include "cluster/cluster_undo_segment.h"
#include "cluster/storage/cluster_shared_fs.h"
#include "cluster/storage/cluster_undo_alloc.h"
#include "storage/fd.h"
#include "utils/memutils.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

static char test_dir[MAXPGPATH], opened_path[MAXPGPATH];
static uint16 visits[129][256];
static int open_error, read_fault, close_fault, alloc_fail, alloc_calls, allocations;
static bool path_error;

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "%s at %s:%d\n", condition, file, line);
	abort();
}

void *
palloc_extended(Size size, int flags)
{
	void *result;

	if (++alloc_calls == alloc_fail)
		return NULL;
	result = (flags & MCXT_ALLOC_ZERO) ? calloc(1, size) : malloc(size);
	if (result != NULL)
		allocations++;
	return result;
}

void
pfree(void *pointer)
{
	if (pointer != NULL) {
		allocations--;
		free(pointer);
	}
}

int
cluster_shared_fs_undo_path_resolve(uint8 owner, uint32 segment, char *buf, size_t size)
{
	uint32 first = (owner - 1) * CLUSTER_UNDO_SEGS_PER_INSTANCE + 1;

	Assert(owner >= 1 && owner <= 128 && segment >= first && segment < first + 256);
	visits[owner][segment - first]++;
	if (path_error)
		return -1;
	return snprintf(buf, size, "%s/%u", test_dir, segment) >= size ? -1 : 0;
}

int
BasicOpenFile(const char *path, int flags)
{
	strlcpy(opened_path, path, sizeof(opened_path));
	if (open_error) {
		errno = open_error;
		return -1;
	}
	return open(path, flags);
}

static ssize_t
fixture_pread(int fd, void *buf, size_t count, off_t offset)
{
	ssize_t result;

	if (read_fault == 1) {
		errno = EIO;
		return -1;
	}
	result = pread(fd, buf, read_fault == 2 ? count - 1 : count, offset);
	if (read_fault == 3)
		UT_ASSERT_EQ(unlink(opened_path), 0);
	return result;
}

static int
fixture_close(int fd)
{
	int result = close(fd);

	if (close_fault) {
		errno = EIO;
		return -1;
	}
	return result;
}

#undef pg_pread
#define pg_pread fixture_pread
#define close fixture_close
#include "../../backend/cluster/cluster_cold_recovery_tt.c"
#undef close
#undef pg_pread

static void
clean_files(void)
{
	DIR *dir;
	struct dirent *entry;
	char path[MAXPGPATH];

	if (test_dir[0] == '\0')
		return;
	dir = opendir(test_dir);
	Assert(dir != NULL);
	while ((entry = readdir(dir)) != NULL) {
		if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
			continue;
		snprintf(path, sizeof(path), "%s/%s", test_dir, entry->d_name);
		if (unlink(path) != 0)
			UT_ASSERT_EQ(rmdir(path), 0);
	}
	UT_ASSERT_EQ(closedir(dir), 0);
	UT_ASSERT_EQ(rmdir(test_dir), 0);
	test_dir[0] = '\0';
}

static void
fixture(ClusterColdTypedV1 *typed)
{
	clean_files();
	UT_ASSERT_EQ(allocations, 0);
	memset(typed, 0, sizeof(*typed));
	memset(visits, 0, sizeof(visits));
	open_error = read_fault = close_fault = alloc_fail = alloc_calls = 0;
	path_error = false;
	strlcpy(test_dir, "/tmp/pgrac-cold-tt-XXXXXX", sizeof(test_dir));
	UT_ASSERT_NOT_NULL(mkdtemp(test_dir));
	typed->participant_count = 1;
	typed->participants[0].thread_id = 1;
}

static void
file_path(uint32 segment, char *path)
{
	snprintf(path, MAXPGPATH, "%s/%u", test_dir, segment);
}

static void
write_header(uint32 segment, const UndoSegmentHeaderData *header, bool create)
{
	char path[MAXPGPATH];
	int fd;

	file_path(segment, path);
	fd = open(path, O_RDWR | (create ? O_CREAT | O_EXCL : 0), 0600);
	UT_ASSERT(fd >= 0);
	if (fd < 0)
		return;
	if (create)
		UT_ASSERT_EQ(ftruncate(fd, UNDO_SEGMENT_SIZE_BYTES), 0);
	UT_ASSERT_EQ(pwrite(fd, header, sizeof(*header), 0), sizeof(*header));
	UT_ASSERT_EQ(close(fd), 0);
}

static UndoSegmentHeaderData
header_for(uint8 owner, uint32 segment)
{
	UndoSegmentHeaderData header = { 0 };

	header.pd_flags = PD_UNDO_SEG_HEADER;
	header.pd_lower = SizeOfPageHeaderData;
	header.pd_upper = header.pd_special = BLCKSZ;
	header.pd_pagesize_version = BLCKSZ | PG_PAGE_LAYOUT_VERSION;
	header.segment_id = segment;
	header.segment_size_bytes = UNDO_SEGMENT_SIZE_BYTES;
	header.segment_state = SEGMENT_RECYCLABLE; /* not the current segment */
	header.owner_instance = owner;
	header.tt_slots_count = TT_SLOTS_PER_SEGMENT;
	return header;
}

#define INPUT_BUDGET ((Size)1024 * 1024)

UT_TEST(test_absence_scans_all_addresses_and_deduplicates_history_origins)
{
	ClusterColdTypedV1 typed;

	fixture(&typed);
	typed.participant_count = 3;
	typed.participants[1].thread_id = typed.participants[2].thread_id = 128;
	typed.replay_count = 1; /* both later participants are history */
	UT_ASSERT_EQ(cluster_cold_tt_base_capture_v1(&typed, INPUT_BUDGET), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(cluster_cold_tt_base_count_v1(&typed), 0);
	for (uint32 n = 0; n < 256; n++) {
		UT_ASSERT_EQ(visits[1][n], 1);
		UT_ASSERT_EQ(visits[128][n], 1);
		UT_ASSERT_EQ(visits[2][n], 0);
	}
	UT_ASSERT_EQ(cluster_cold_tt_base_recheck_v1(&typed), CLUSTER_COLD_OK);
	for (uint32 n = 0; n < 256; n++)
		UT_ASSERT_EQ(visits[128][n], 2);
	cluster_cold_tt_base_destroy_v1(&typed);
}

UT_TEST(test_all_slots_noncurrent_segments_and_generation_zero_are_retained)
{
	ClusterColdTypedV1 typed;
	UndoSegmentHeaderData header = header_for(1, 256);
	const UndoSegmentHeaderData *saved;
	uint8 owner = 0;
	uint32 segment = 0;

	fixture(&typed);
	for (uint32 i = 0; i < TT_SLOTS_PER_SEGMENT; i++)
		memset(&header.tt_slots[i], i + 1, sizeof(TTSlot)); /* raw bytes, no verdict */
	header.wrap_count = 0;
	header._reserved[0] = 17;
	write_header(256, &header, true);
	UT_ASSERT_EQ(cluster_cold_tt_base_capture_v1(&typed, INPUT_BUDGET), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(cluster_cold_tt_base_count_v1(&typed), 1);
	saved = cluster_cold_tt_base_header_v1(&typed, 0, &owner, &segment);
	UT_ASSERT_NOT_NULL(saved);
	UT_ASSERT_EQ(owner, 1);
	UT_ASSERT_EQ(segment, 256);
	UT_ASSERT_EQ(memcmp(saved, &header, sizeof(header)), 0);
	UT_ASSERT_EQ(cluster_cold_tt_base_recheck_v1(&typed), CLUSTER_COLD_OK);
	cluster_cold_tt_base_destroy_v1(&typed);
}

UT_TEST(test_history_origin_last_segment_is_captured)
{
	ClusterColdTypedV1 typed;
	UndoSegmentHeaderData first = header_for(1, 1), last = header_for(128, 32768);
	uint8 owner;
	uint32 segment;

	fixture(&typed);
	typed.participant_count = 2;
	typed.replay_count = 1;
	typed.participants[1].thread_id = 128;
	write_header(1, &first, true);
	write_header(32768, &last, true);
	UT_ASSERT_EQ(cluster_cold_tt_base_capture_v1(&typed, INPUT_BUDGET), CLUSTER_COLD_OK);
	UT_ASSERT_EQ(cluster_cold_tt_base_count_v1(&typed), 2);
	UT_ASSERT_NOT_NULL(cluster_cold_tt_base_header_v1(&typed, 1, &owner, &segment));
	UT_ASSERT_EQ(owner, 128);
	UT_ASSERT_EQ(segment, 32768);
	cluster_cold_tt_base_destroy_v1(&typed);
}

UT_TEST(test_invalid_present_input_never_exports_partial_capture)
{
	for (int fault = 0; fault < 10; fault++) {
		ClusterColdTypedV1 typed;
		UndoSegmentHeaderData first = header_for(1, 1), bad = header_for(1, 2);
		char path[MAXPGPATH];

		fixture(&typed);
		write_header(1, &first, true);
		if (fault == 0)
			bad.segment_id++;
		if (fault == 1)
			bad.owner_instance++;
		if (fault == 2)
			bad.tt_slots_count--;
		if (fault == 3)
			bad.pd_pagesize_version = 0;
		if (fault == 4)
			bad.segment_state = SEGMENT_INVALID;
		if (fault == 5)
			bad.segment_size_bytes--;
		file_path(2, path);
		if (fault == 8)
			UT_ASSERT_EQ(mkdir(path, 0700), 0);
		else if (fault == 9)
			UT_ASSERT_EQ(symlink("1", path), 0);
		else {
			write_header(2, &bad, true);
			if (fault == 6 || fault == 7)
				UT_ASSERT_EQ(truncate(path, fault == 6 ? BLCKSZ : 0), 0);
		}
		UT_ASSERT_EQ(cluster_cold_tt_base_capture_v1(&typed, INPUT_BUDGET),
					 CLUSTER_COLD_SOURCE_GAP);
		UT_ASSERT_EQ(cluster_cold_tt_base_count_v1(&typed), 0);
		UT_ASSERT_EQ(allocations, 1); /* failed container only; partial bytes freed */
		UT_ASSERT_EQ(cluster_cold_tt_base_recheck_v1(&typed), CLUSTER_COLD_SOURCE_GAP);
		cluster_cold_tt_base_destroy_v1(&typed);
	}
}

UT_TEST(test_only_enoent_is_absent)
{
	int errors[] = { EACCES, EIO, ENOTDIR };

	for (uint32 i = 0; i < lengthof(errors) + 1; i++) {
		ClusterColdTypedV1 typed;

		fixture(&typed);
		if (i < lengthof(errors))
			open_error = errors[i];
		else
			path_error = true;
		UT_ASSERT_EQ(cluster_cold_tt_base_capture_v1(&typed, INPUT_BUDGET),
					 CLUSTER_COLD_SOURCE_GAP);
		cluster_cold_tt_base_destroy_v1(&typed);
	}
}

UT_TEST(test_read_short_error_close_error_and_unlinked_path_refuse)
{
	for (int fault = 1; fault <= 4; fault++) {
		ClusterColdTypedV1 typed;
		UndoSegmentHeaderData header = header_for(1, 1);

		fixture(&typed);
		write_header(1, &header, true);
		read_fault = fault < 4 ? fault : 0;
		close_fault = fault == 4;
		UT_ASSERT_EQ(cluster_cold_tt_base_capture_v1(&typed, INPUT_BUDGET),
					 CLUSTER_COLD_SOURCE_GAP);
		UT_ASSERT_EQ(cluster_cold_tt_base_count_v1(&typed), 0);
		cluster_cold_tt_base_destroy_v1(&typed);
	}
}

UT_TEST(test_capacity_failure_discards_partial_input_and_cannot_recheck)
{
	ClusterColdTypedV1 typed;
	UndoSegmentHeaderData first = header_for(1, 1), second = header_for(1, 2);

	fixture(&typed);
	write_header(1, &first, true);
	write_header(2, &second, true);
	UT_ASSERT_EQ(cluster_cold_tt_base_capture_v1(&typed, 9000), CLUSTER_COLD_CAPACITY);
	UT_ASSERT_EQ(cluster_cold_tt_base_count_v1(&typed), 0);
	UT_ASSERT_EQ(cluster_cold_tt_base_recheck_v1(&typed), CLUSTER_COLD_CAPACITY);
	UT_ASSERT_EQ(cluster_cold_tt_base_capture_v1(&typed, INPUT_BUDGET), CLUSTER_COLD_STATE);
	cluster_cold_tt_base_destroy_v1(&typed);
}

UT_TEST(test_allocation_failure_discards_partial_input)
{
	for (int failure = 1; failure <= 3; failure++) {
		ClusterColdTypedV1 typed;
		UndoSegmentHeaderData first = header_for(1, 1), second = header_for(1, 2);

		fixture(&typed);
		write_header(1, &first, true);
		write_header(2, &second, true);
		alloc_fail = failure;
		UT_ASSERT_EQ(cluster_cold_tt_base_capture_v1(&typed, INPUT_BUDGET), CLUSTER_COLD_OOM);
		UT_ASSERT_EQ(cluster_cold_tt_base_count_v1(&typed), 0);
		cluster_cold_tt_base_destroy_v1(&typed);
		UT_ASSERT_EQ(allocations, 0);
	}
}

UT_TEST(test_any_block_zero_byte_drift_is_sticky)
{
	for (int fault = 0; fault < 4; fault++) {
		ClusterColdTypedV1 typed;
		UndoSegmentHeaderData original = header_for(1, 256), changed = original;

		fixture(&typed);
		write_header(256, &original, true);
		UT_ASSERT_EQ(cluster_cold_tt_base_capture_v1(&typed, INPUT_BUDGET), CLUSTER_COLD_OK);
		if (fault == 0)
			changed.wrap_count++;
		if (fault == 1)
			((char *)&changed)[BLCKSZ - 1] ^= 1;
		if (fault == 2)
			((char *)&changed.tt_slots[47])[0] ^= 1;
		if (fault == 3)
			changed.pd_checksum++;
		write_header(256, &changed, false);
		UT_ASSERT_EQ(cluster_cold_tt_base_recheck_v1(&typed), CLUSTER_COLD_SOURCE_GAP);
		write_header(256, &original, false);
		UT_ASSERT_EQ(cluster_cold_tt_base_recheck_v1(&typed), CLUSTER_COLD_SOURCE_GAP);
		UT_ASSERT_EQ(cluster_cold_tt_base_count_v1(&typed), 0);
		cluster_cold_tt_base_destroy_v1(&typed);
	}
}

UT_TEST(test_presence_replacement_size_and_io_drift_refuse)
{
	for (int fault = 0; fault < 6; fault++) {
		ClusterColdTypedV1 typed;
		UndoSegmentHeaderData header = header_for(1, 1), added = header_for(1, 256);
		char path[MAXPGPATH], held[MAXPGPATH];

		fixture(&typed);
		write_header(1, &header, true);
		UT_ASSERT_EQ(cluster_cold_tt_base_capture_v1(&typed, INPUT_BUDGET), CLUSTER_COLD_OK);
		file_path(1, path);
		if (fault == 0)
			UT_ASSERT_EQ(unlink(path), 0);
		if (fault == 1)
			write_header(256, &added, true);
		if (fault == 2 || fault == 4) {
			snprintf(held, sizeof(held), "%s/held", test_dir);
			UT_ASSERT_EQ(rename(path, held), 0);
			if (fault == 2)
				write_header(1, &header, true); /* identical bytes, different inode */
			else
				UT_ASSERT_EQ(symlink("held", path), 0);
		}
		if (fault == 3)
			UT_ASSERT_EQ(truncate(path, BLCKSZ), 0);
		if (fault == 5)
			read_fault = 1;
		UT_ASSERT_EQ(cluster_cold_tt_base_recheck_v1(&typed), CLUSTER_COLD_SOURCE_GAP);
		UT_ASSERT_EQ(cluster_cold_tt_base_count_v1(&typed), 0);
		cluster_cold_tt_base_destroy_v1(&typed);
	}
}

UT_TEST(test_participant_origin_drift_refuses)
{
	ClusterColdTypedV1 typed;

	fixture(&typed);
	UT_ASSERT_EQ(cluster_cold_tt_base_capture_v1(&typed, INPUT_BUDGET), CLUSTER_COLD_OK);
	typed.participants[0].thread_id = 2;
	UT_ASSERT_EQ(cluster_cold_tt_base_recheck_v1(&typed), CLUSTER_COLD_SOURCE_GAP);
	typed.participants[0].thread_id = 1;
	UT_ASSERT_EQ(cluster_cold_tt_base_recheck_v1(&typed), CLUSTER_COLD_SOURCE_GAP);
	cluster_cold_tt_base_destroy_v1(&typed);
}

UT_TEST(test_invalid_input_budget_lookup_and_destroy)
{
	ClusterColdTypedV1 typed;
	uint8 owner = 99;
	uint32 segment = 99;

	fixture(&typed);
	UT_ASSERT_EQ(cluster_cold_tt_base_capture_v1(NULL, INPUT_BUDGET),
				 CLUSTER_COLD_PARTICIPANT_INVALID);
	for (int bad = 0; bad < 3; bad++) {
		typed.participant_count = bad == 2 ? 0 : 1;
		typed.participants[0].thread_id = bad == 0 ? 0 : 129;
		UT_ASSERT_EQ(cluster_cold_tt_base_capture_v1(&typed, INPUT_BUDGET),
					 CLUSTER_COLD_PARTICIPANT_INVALID);
	}
	typed.participant_count = 1;
	typed.participants[0].thread_id = 1;
	UT_ASSERT_EQ(cluster_cold_tt_base_capture_v1(&typed, 0), CLUSTER_COLD_CAPACITY);
	UT_ASSERT_EQ(alloc_calls, 0);
	UT_ASSERT_EQ(cluster_cold_tt_base_recheck_v1(&typed), CLUSTER_COLD_STATE);
	UT_ASSERT_NULL(cluster_cold_tt_base_header_v1(&typed, 0, &owner, &segment));
	UT_ASSERT_EQ(owner, 99);
	UT_ASSERT_EQ(segment, 99);
	cluster_cold_tt_base_destroy_v1(&typed);
	cluster_cold_tt_base_destroy_v1(NULL);
	clean_files();
	UT_ASSERT_EQ(allocations, 0);
}

static ClusterColdTtSlotKeyV1
slot_key(uint8 owner, uint32 segment, uint32 generation, uint32 offset, TransactionId xid,
		 uint16 wrap)
{
	ClusterColdTtSlotKeyV1 key = { 0 };

	key.owner_instance = owner;
	key.segment_id = segment;
	key.segment_generation = generation;
	key.slot_offset = offset;
	key.xid = xid;
	key.slot_wrap = wrap;
	return key;
}

static void
assert_cleared(const ClusterColdTtSlotInputV1 *out)
{
	ClusterColdTtSlotInputV1 zero = { 0 };

	UT_ASSERT_EQ(memcmp(out, &zero, sizeof(zero)), 0);
}

UT_TEST(test_slot_enumeration_includes_every_raw_history_slot)
{
	ClusterColdTypedV1 typed;
	UndoSegmentHeaderData header = header_for(128, 32768);
	ClusterColdTtSlotInputV1 out;

	fixture(&typed);
	typed.participant_count = 2;
	typed.participants[1].thread_id = 128;
	typed.replay_count = 1;
	for (uint32 i = 0; i < TT_SLOTS_PER_SEGMENT; i++)
		memset(&header.tt_slots[i], i, sizeof(TTSlot));
	write_header(32768, &header, true);
	UT_ASSERT_EQ(cluster_cold_tt_base_capture_v1(&typed, INPUT_BUDGET), CLUSTER_COLD_OK);
	for (uint32 i = 0; i < TT_SLOTS_PER_SEGMENT; i++) {
		UT_ASSERT(cluster_cold_tt_base_slot_v1(&typed, 0, i, &out));
		UT_ASSERT_EQ(out.key.owner_instance, 128);
		UT_ASSERT_EQ(out.key.segment_id, 32768);
		UT_ASSERT_EQ(out.key.segment_generation, 0);
		UT_ASSERT_EQ(out.key.slot_offset, i);
		UT_ASSERT_EQ(out.key.xid, header.tt_slots[i].xid);
		UT_ASSERT_EQ(out.key.slot_wrap, header.tt_slots[i].wrap);
		UT_ASSERT_EQ(memcmp(&out.slot, &header.tt_slots[i], sizeof(TTSlot)), 0);
	}
	cluster_cold_tt_base_destroy_v1(&typed);
}

UT_TEST(test_slot_enumeration_bounds_and_missing_input_clear_outputs)
{
	ClusterColdTypedV1 typed;
	UndoSegmentHeaderData header = header_for(1, 1);
	ClusterColdTtSlotInputV1 out;

	fixture(&typed);
	write_header(1, &header, true);
	UT_ASSERT_EQ(cluster_cold_tt_base_capture_v1(&typed, INPUT_BUDGET), CLUSTER_COLD_OK);
	for (int fault = 0; fault < 5; fault++) {
		memset(&out, 0xA5, sizeof(out));
		UT_ASSERT(!cluster_cold_tt_base_slot_v1(fault == 0 ? NULL : &typed,
												fault == 1	 ? 1
												: fault == 2 ? UINT32_MAX
															 : 0,
												fault == 3	 ? TT_SLOTS_PER_SEGMENT
												: fault == 4 ? UINT32_MAX
															 : 0,
												&out));
		assert_cleared(&out);
	}
	UT_ASSERT(!cluster_cold_tt_base_slot_v1(&typed, 0, 0, NULL));
	cluster_cold_tt_base_destroy_v1(&typed);
	memset(&out, 0xA5, sizeof(out));
	UT_ASSERT(!cluster_cold_tt_base_slot_v1(&typed, 0, 0, &out));
	assert_cleared(&out);
}

UT_TEST(test_exact_match_preserves_active_head_generation_zero_and_last_slot)
{
	ClusterColdTypedV1 typed;
	UndoSegmentHeaderData header = header_for(128, 32768);
	ClusterColdTtSlotKeyV1 key = slot_key(128, 32768, 0, 47, 1234, 0);
	ClusterColdTtSlotInputV1 out;

	fixture(&typed);
	typed.participant_count = 2;
	typed.participants[1].thread_id = 128;
	typed.replay_count = 1;
	header.tt_slots[47].xid = key.xid;
	header.tt_slots[47].status = TT_SLOT_ACTIVE;
	header.tt_slots[47].first_undo_block.raw[0] = UINT64_C(0x12345678);
	header.tt_slots[47].first_undo_block.raw[1] = UINT64_C(0xABCDEF);
	write_header(32768, &header, true);
	UT_ASSERT_EQ(cluster_cold_tt_base_capture_v1(&typed, INPUT_BUDGET), CLUSTER_COLD_OK);
	UT_ASSERT(cluster_cold_tt_base_match_v1(&typed, &key, &out));
	UT_ASSERT_EQ(out.key.segment_generation, 0);
	UT_ASSERT_EQ(out.key.slot_wrap, 0);
	UT_ASSERT_EQ(out.slot.status, TT_SLOT_ACTIVE);
	UT_ASSERT_EQ(memcmp(&out.slot, &header.tt_slots[47], sizeof(TTSlot)), 0);
	cluster_cold_tt_base_destroy_v1(&typed);
}

UT_TEST(test_exact_match_rejects_each_identity_mismatch_without_partial_output)
{
	ClusterColdTypedV1 typed;
	UndoSegmentHeaderData header = header_for(1, 256);
	ClusterColdTtSlotKeyV1 key = slot_key(1, 256, 7, 47, 1234, 9);
	ClusterColdTtSlotInputV1 out;

	fixture(&typed);
	header.wrap_count = key.segment_generation;
	header.tt_slots[47].xid = key.xid;
	header.tt_slots[47].wrap = key.slot_wrap;
	header.tt_slots[47].status = TT_SLOT_ACTIVE;
	write_header(256, &header, true);
	UT_ASSERT_EQ(cluster_cold_tt_base_capture_v1(&typed, INPUT_BUDGET), CLUSTER_COLD_OK);
	UT_ASSERT(cluster_cold_tt_base_match_v1(&typed, &key, &out));
	for (int fault = 0; fault < 7; fault++) {
		ClusterColdTtSlotKeyV1 bad = key;

		switch (fault) {
		case 0:
			bad.owner_instance++;
			break;
		case 1:
			bad.segment_id--;
			break;
		case 2:
			bad.segment_generation--;
			break;
		case 3:
			bad.segment_generation++;
			break;
		case 4:
			bad.slot_offset--;
			break;
		case 5:
			bad.xid++;
			break;
		case 6:
			bad.slot_wrap++;
			break;
		}
		memset(&out, 0xA5, sizeof(out));
		UT_ASSERT(!cluster_cold_tt_base_match_v1(&typed, &bad, &out));
		assert_cleared(&out);
	}
	cluster_cold_tt_base_destroy_v1(&typed);
}

UT_TEST(test_exact_match_invalid_claims_and_null_arguments_clear_outputs)
{
	ClusterColdTypedV1 typed;
	UndoSegmentHeaderData header = header_for(1, 1);
	ClusterColdTtSlotKeyV1 key = slot_key(1, 1, 0, 0, 3, 0);
	ClusterColdTtSlotInputV1 out;

	fixture(&typed);
	write_header(1, &header, true);
	UT_ASSERT_EQ(cluster_cold_tt_base_capture_v1(&typed, INPUT_BUDGET), CLUSTER_COLD_OK);
	for (int fault = 0; fault < 10; fault++) {
		ClusterColdTtSlotKeyV1 bad = key;

		switch (fault) {
		case 0:
			bad.owner_instance = 0;
			break;
		case 1:
			bad.owner_instance = 129;
			break;
		case 2:
			bad.segment_id = 0;
			break;
		case 3:
			bad.slot_offset = TT_SLOTS_PER_SEGMENT;
			break;
		case 4:
			bad.slot_offset = UINT32_MAX;
			break;
		case 5:
			bad.xid = InvalidTransactionId;
			break;
		case 6:
			bad.xid = FrozenTransactionId;
			break;
		case 7:
			bad.slot_wrap = TT_WRAP_INVALID;
			break;
		}
		memset(&out, 0xA5, sizeof(out));
		UT_ASSERT(!cluster_cold_tt_base_match_v1(fault == 8 ? NULL : &typed,
												 fault == 9 ? NULL : &bad, &out));
		assert_cleared(&out);
	}
	UT_ASSERT(!cluster_cold_tt_base_match_v1(&typed, &key, NULL));
	cluster_cold_tt_base_destroy_v1(&typed);
}

UT_TEST(test_slot_copies_and_aliased_match_do_not_modify_captured_input)
{
	ClusterColdTypedV1 typed;
	UndoSegmentHeaderData header = header_for(1, 1);
	ClusterColdTtSlotInputV1 first, second;

	fixture(&typed);
	header.tt_slots[0].xid = 3;
	header.tt_slots[0].status = TT_SLOT_ACTIVE;
	write_header(1, &header, true);
	UT_ASSERT_EQ(cluster_cold_tt_base_capture_v1(&typed, INPUT_BUDGET), CLUSTER_COLD_OK);
	UT_ASSERT(cluster_cold_tt_base_slot_v1(&typed, 0, 0, &first));
	UT_ASSERT(cluster_cold_tt_base_match_v1(&typed, &first.key, &first));
	first.slot.xid = 999;
	UT_ASSERT(cluster_cold_tt_base_slot_v1(&typed, 0, 0, &second));
	UT_ASSERT_EQ(second.slot.xid, 3);
	UT_ASSERT_EQ(second.key.xid, 3);
	UT_ASSERT_EQ(cluster_cold_tt_base_recheck_v1(&typed), CLUSTER_COLD_OK);
	cluster_cold_tt_base_destroy_v1(&typed);
	UT_ASSERT_EQ(second.slot.xid, 3); /* independent copy survives release */
}

UT_TEST(test_failed_capture_and_sticky_recheck_export_no_slot_or_match)
{
	for (int fault = 0; fault < 2; fault++) {
		ClusterColdTypedV1 typed;
		UndoSegmentHeaderData header = header_for(1, 1);
		ClusterColdTtSlotKeyV1 key = slot_key(1, 1, 0, 0, 3, 0);
		ClusterColdTtSlotInputV1 out;

		fixture(&typed);
		header.tt_slots[0].xid = 3;
		header.tt_slots[0].status = TT_SLOT_ACTIVE;
		write_header(1, &header, true);
		if (fault == 0) {
			read_fault = 1;
			UT_ASSERT_EQ(cluster_cold_tt_base_capture_v1(&typed, INPUT_BUDGET),
						 CLUSTER_COLD_SOURCE_GAP);
		} else {
			UT_ASSERT_EQ(cluster_cold_tt_base_capture_v1(&typed, INPUT_BUDGET), CLUSTER_COLD_OK);
			header.tt_slots[0].wrap++;
			write_header(1, &header, false);
			UT_ASSERT_EQ(cluster_cold_tt_base_recheck_v1(&typed), CLUSTER_COLD_SOURCE_GAP);
		}
		memset(&out, 0xA5, sizeof(out));
		UT_ASSERT(!cluster_cold_tt_base_slot_v1(&typed, 0, 0, &out));
		assert_cleared(&out);
		memset(&out, 0xA5, sizeof(out));
		UT_ASSERT(!cluster_cold_tt_base_match_v1(&typed, &key, &out));
		assert_cleared(&out);
		cluster_cold_tt_base_destroy_v1(&typed);
	}
}

int
main(void)
{
	UT_PLAN(19);
	UT_RUN(test_absence_scans_all_addresses_and_deduplicates_history_origins);
	UT_RUN(test_all_slots_noncurrent_segments_and_generation_zero_are_retained);
	UT_RUN(test_history_origin_last_segment_is_captured);
	UT_RUN(test_invalid_present_input_never_exports_partial_capture);
	UT_RUN(test_only_enoent_is_absent);
	UT_RUN(test_read_short_error_close_error_and_unlinked_path_refuse);
	UT_RUN(test_capacity_failure_discards_partial_input_and_cannot_recheck);
	UT_RUN(test_allocation_failure_discards_partial_input);
	UT_RUN(test_any_block_zero_byte_drift_is_sticky);
	UT_RUN(test_presence_replacement_size_and_io_drift_refuse);
	UT_RUN(test_participant_origin_drift_refuses);
	UT_RUN(test_invalid_input_budget_lookup_and_destroy);
	UT_RUN(test_slot_enumeration_includes_every_raw_history_slot);
	UT_RUN(test_slot_enumeration_bounds_and_missing_input_clear_outputs);
	UT_RUN(test_exact_match_preserves_active_head_generation_zero_and_last_slot);
	UT_RUN(test_exact_match_rejects_each_identity_mismatch_without_partial_output);
	UT_RUN(test_exact_match_invalid_claims_and_null_arguments_clear_outputs);
	UT_RUN(test_slot_copies_and_aliased_match_do_not_modify_captured_input);
	UT_RUN(test_failed_capture_and_sticky_recheck_export_no_slot_or_match);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
