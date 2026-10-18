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
#include "cluster/cluster_recovery_reference_graph.h"
#include "cluster/cluster_recovery_undo_scan.h"
#include "cluster/cluster_undo_segment.h"
#include "cluster/storage/cluster_shared_fs.h"
#include "cluster/storage/cluster_undo_alloc.h"
#include "storage/fd.h"
#include "utils/memutils.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

#include "test_cluster_heap_small_scn.inc"

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

	if (read_fault == 1 || (read_fault == 4 && offset != 0)) {
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

/* This raw-capture fixture does not link the semantic redo consumer. */
bool
cluster_cold_typed_undo_base_redo_block_v1(const ClusterColdTypedV1 *typed, uint32 participant,
										   uint32 segment_index, uint32 block_no,
										   const PGAlignedBlock *base, PGAlignedBlock *out)
{
	(void)typed;
	(void)participant;
	(void)segment_index;
	(void)block_no;
	(void)base;
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	return false;
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

static UBA
record_address(uint32 segment, uint32 block, uint16 row)
{
	UBA address = { { ((uint64)block << 32) | segment, ((uint64)row << 16) | 6 } };

	return address;
}

static void
write_record_block(uint8 owner, uint32 segment, uint32 blockno, bool corrupt_second)
{
	PGAlignedBlock page = { 0 };
	UndoBlockHeader block = { 0 };
	char path[MAXPGPATH];
	int fd;

	block.magic = PGRAC_UNDO_BLOCK_MAGIC;
	block.block_version = UNDO_BLOCK_VERSION_1;
	block.free_offset = sizeof(block);
	for (uint16 i = 0; i < 2; i++) {
		UndoRecordHeader header = { 0 };
		UndoSlotDirEntry directory = { 0 };
		UndoInsertPayload payload = { 32, 0 };

		header.record_type = UNDO_RECORD_INSERT;
		header.flags = UNDO_REC_FLAG_FIRST_IN_TX;
		header.payload_length = sizeof(payload);
		header.xid = 42 + i;
		header.origin_node_id = owner - 1;
		header.tt_slot_segment_id = (owner - 1) * CLUSTER_UNDO_SEGS_PER_INSTANCE + 1;
		header.tt_slot_id = 7;
		header.tt_wrap_plus1 = i; /* Unknown wrap stays raw input. */
		header.target_locator = (RelFileLocator){ 1663, 5, 16385 };
		header.target_offset = 1;
		directory.record_offset = block.free_offset;
		directory.record_length = sizeof(header) + sizeof(payload);
		directory.record_type = header.record_type;
		directory.flags = header.flags;
		if (i == 1 && corrupt_second)
			directory.record_offset = sizeof(block) - 1;
		memcpy(page.data + block.free_offset, &header, sizeof(header));
		memcpy(page.data + block.free_offset + sizeof(header), &payload, sizeof(payload));
		memcpy(page.data + UNDO_SLOT_DIR_OFFSET(i), &directory, sizeof(directory));
		block.slot_count++;
		block.free_offset += directory.record_length;
	}
	memcpy(page.data, &block, sizeof(block));
	file_path(segment, path);
	fd = open(path, O_RDWR);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(pwrite(fd, page.data, BLCKSZ, (off_t)blockno * BLCKSZ), BLCKSZ);
	UT_ASSERT_EQ(close(fd), 0);
}

static void
assert_record_cleared(const ClusterRecoveryUndoRecord *record, uint32 generation)
{
	ClusterRecoveryUndoRecord zero = { 0 };

	UT_ASSERT_EQ(memcmp(record, &zero, sizeof(zero)), 0);
	UT_ASSERT_EQ(generation, 0);
}

UT_TEST(test_original_undo_records_keep_raw_identity_and_history_origin)
{
	for (uint16 owner = 1; owner <= 128; owner += 127) {
		ClusterColdTypedV1 typed;
		uint32 segment = (owner - 1) * CLUSTER_UNDO_SEGS_PER_INSTANCE + 1;
		UndoSegmentHeaderData header = header_for(owner, segment);
		ClusterRecoveryUndoRecord record;
		uint32 generation = UINT32_MAX;

		fixture(&typed);
		typed.participant_count = 2;
		typed.replay_count = 1;
		typed.participants[1].thread_id = owner;
		write_header(segment, &header, true);
		write_record_block(owner, segment, 3, false);
		UT_ASSERT_EQ(cluster_cold_tt_base_capture_v1(&typed, INPUT_BUDGET), CLUSTER_COLD_OK);
		for (uint16 row = 0; row < 2; row++) {
			int previous_calls = alloc_calls;

			UT_ASSERT(cluster_cold_tt_undo_record_v1(&typed, record_address(segment, 3, row),
													 &record, &generation));
			UT_ASSERT_EQ(record.header.xid, 42 + row);
			UT_ASSERT_EQ(record.header.tt_wrap_plus1, row);
			UT_ASSERT_EQ(record.header.origin_node_id, owner - 1);
			UT_ASSERT_EQ(record.block_header.crc64, 0); /* Not checksum authority. */
			UT_ASSERT_EQ(generation, 0);
			if (row == 0) {
				typed.tt_base->budget = typed.tt_base->used;
				alloc_fail = alloc_calls + 1;
			} else
				UT_ASSERT_EQ(alloc_calls, previous_calls);
		}
		UT_ASSERT_EQ(cluster_cold_tt_base_recheck_v1(&typed), CLUSTER_COLD_OK);
		cluster_cold_tt_base_destroy_v1(&typed);
		UT_ASSERT_EQ(record.header.xid, 43); /* Independent output copy. */
	}
}

UT_TEST(test_record_physical_segment_is_distinct_from_canonical_tt_segment)
{
	ClusterColdTypedV1 typed;
	UndoSegmentHeaderData canonical = header_for(1, 1), physical = header_for(1, 2);
	ClusterRecoveryUndoRecord record;
	uint32 generation;

	fixture(&typed);
	physical.wrap_count = 91;
	write_header(1, &canonical, true);
	write_header(2, &physical, true);
	write_record_block(1, 2, 3, false);
	UT_ASSERT_EQ(cluster_cold_tt_base_capture_v1(&typed, INPUT_BUDGET), CLUSTER_COLD_OK);
	UT_ASSERT(
		cluster_cold_tt_undo_record_v1(&typed, record_address(2, 3, 0), &record, &generation));
	UT_ASSERT_EQ(record.header.tt_slot_segment_id, 1);
	UT_ASSERT_EQ(generation, 91);
	UT_ASSERT_EQ(cluster_cold_tt_base_recheck_v1(&typed), CLUSTER_COLD_OK);
	cluster_cold_tt_base_destroy_v1(&typed);
}

UT_TEST(test_record_file_and_io_drift_invalidate_all_captured_inputs)
{
	for (int fault = 0; fault < 10; fault++) {
		ClusterColdTypedV1 typed;
		UndoSegmentHeaderData header = header_for(1, 1);
		ClusterRecoveryUndoRecord record;
		uint32 generation = UINT32_MAX;
		char path[MAXPGPATH];
		int fd;

		fixture(&typed);
		write_header(1, &header, true);
		write_record_block(1, 1, 3, false);
		UT_ASSERT_EQ(cluster_cold_tt_base_capture_v1(&typed, INPUT_BUDGET), CLUSTER_COLD_OK);
		file_path(1, path);
		switch (fault) {
		case 0:
			UT_ASSERT_EQ(unlink(path), 0);
			break;
		case 1:
			fd = open(path, O_RDONLY); /* Hold old inode to forbid reuse. */
			UT_ASSERT_EQ(unlink(path), 0);
			write_header(1, &header, true);
			write_record_block(1, 1, 3, false);
			UT_ASSERT_EQ(close(fd), 0);
			break;
		case 2:
			UT_ASSERT_EQ(truncate(path, BLCKSZ), 0);
			break;
		case 3:
			header._reserved[0]++;
			write_header(1, &header, false);
			break;
		case 4:
		case 5:
		case 6:
			read_fault = fault - 3;
			break;
		case 7:
			close_fault = 1;
			break;
		case 8:
			path_error = true;
			break;
		case 9:
			typed.participants[0].thread_id = 2;
			break;
		}
		memset(&record, 0xA5, sizeof(record));
		UT_ASSERT(
			!cluster_cold_tt_undo_record_v1(&typed, record_address(1, 3, 0), &record, &generation));
		assert_record_cleared(&record, generation);
		UT_ASSERT_EQ(cluster_cold_tt_base_count_v1(&typed), 0);
		cluster_cold_tt_base_destroy_v1(&typed);
	}
}

UT_TEST(test_record_recheck_covers_unused_bytes_and_data_read_failure)
{
	for (int fault = 0; fault < 2; fault++) {
		ClusterColdTypedV1 typed;
		UndoSegmentHeaderData header = header_for(1, 1);
		ClusterRecoveryUndoRecord record;
		uint32 generation;
		char path[MAXPGPATH];
		char changed = 42;
		int fd;

		fixture(&typed);
		write_header(1, &header, true);
		write_record_block(1, 1, 3, false);
		UT_ASSERT_EQ(cluster_cold_tt_base_capture_v1(&typed, INPUT_BUDGET), CLUSTER_COLD_OK);
		UT_ASSERT(
			cluster_cold_tt_undo_record_v1(&typed, record_address(1, 3, 0), &record, &generation));
		if (fault == 0) {
			file_path(1, path);
			fd = open(path, O_RDWR);
			UT_ASSERT_EQ(pwrite(fd, &changed, 1, 3 * BLCKSZ + BLCKSZ / 2), 1);
			UT_ASSERT_EQ(close(fd), 0);
		} else
			read_fault = 4; /* Header rereads succeed; retained DATA reread fails. */
		UT_ASSERT_EQ(cluster_cold_tt_base_recheck_v1(&typed), CLUSTER_COLD_SOURCE_GAP);
		UT_ASSERT_EQ(cluster_cold_tt_base_count_v1(&typed), 0);
		memset(&record, 0xA5, sizeof(record));
		generation = UINT32_MAX;
		UT_ASSERT(
			!cluster_cold_tt_undo_record_v1(&typed, record_address(1, 3, 0), &record, &generation));
		assert_record_cleared(&record, generation);
		cluster_cold_tt_base_destroy_v1(&typed);
	}
}

UT_TEST(test_complete_block_refuses_bad_unselected_row)
{
	ClusterColdTypedV1 typed;
	UndoSegmentHeaderData header = header_for(1, 1);
	ClusterRecoveryUndoRecord record;
	uint32 generation = UINT32_MAX;

	fixture(&typed);
	write_header(1, &header, true);
	write_record_block(1, 1, 3, true);
	UT_ASSERT_EQ(cluster_cold_tt_base_capture_v1(&typed, INPUT_BUDGET), CLUSTER_COLD_OK);
	memset(&record, 0xA5, sizeof(record));
	UT_ASSERT(
		!cluster_cold_tt_undo_record_v1(&typed, record_address(1, 3, 0), &record, &generation));
	assert_record_cleared(&record, generation);
	UT_ASSERT_EQ(cluster_cold_tt_base_count_v1(&typed), 0);
	cluster_cold_tt_base_destroy_v1(&typed);
}

UT_TEST(test_record_capacity_and_allocation_failure_discard_the_capture)
{
	for (int fault = 0; fault < 2; fault++) {
		ClusterColdTypedV1 typed;
		UndoSegmentHeaderData header = header_for(1, 1);
		ClusterRecoveryUndoRecord record;
		uint32 generation = UINT32_MAX;

		fixture(&typed);
		write_header(1, &header, true);
		write_record_block(1, 1, 3, false);
		UT_ASSERT_EQ(cluster_cold_tt_base_capture_v1(&typed, INPUT_BUDGET), CLUSTER_COLD_OK);
		if (fault == 0)
			typed.tt_base->budget = typed.tt_base->used;
		else
			alloc_fail = alloc_calls + 1;
		memset(&record, 0xA5, sizeof(record));
		UT_ASSERT(
			!cluster_cold_tt_undo_record_v1(&typed, record_address(1, 3, 0), &record, &generation));
		assert_record_cleared(&record, generation);
		UT_ASSERT_EQ(cluster_cold_tt_base_recheck_v1(&typed),
					 fault == 0 ? CLUSTER_COLD_CAPACITY : CLUSTER_COLD_OOM);
		UT_ASSERT_EQ(cluster_cold_tt_base_count_v1(&typed), 0);
		cluster_cold_tt_base_destroy_v1(&typed);
		UT_ASSERT_EQ(allocations, 0);
	}
}

UT_TEST(test_record_invalid_address_and_missing_source_clear_outputs)
{
	for (int fault = 0; fault < 7; fault++) {
		ClusterColdTypedV1 typed;
		UndoSegmentHeaderData header = header_for(1, 1);
		ClusterRecoveryUndoRecord record;
		uint32 generation = UINT32_MAX;
		UBA address = record_address(1, 3, 0);

		fixture(&typed);
		write_header(1, &header, true);
		write_record_block(1, 1, 3, false);
		UT_ASSERT_EQ(cluster_cold_tt_base_capture_v1(&typed, INPUT_BUDGET), CLUSTER_COLD_OK);
		switch (fault) {
		case 0:
			address = record_address(0, 3, 0);
			break;
		case 1:
			address = record_address(1, 0, 0);
			break;
		case 2:
			address = record_address(1, UNDO_SEGMENT_SIZE_BYTES / BLCKSZ, 0);
			break;
		case 3:
			address = record_address(2, 3, 0); /* Absent segment in selected origin. */
			break;
		case 4:
			address = record_address(CLUSTER_UNDO_SEGS_PER_INSTANCE + 1, 3, 0);
			break;
		case 5:
			address = record_address(1, 3, 2); /* No such row. */
			break;
		case 6:
			typed.refusal = CLUSTER_COLD_SOURCE_GAP;
			break;
		}
		memset(&record, 0xA5, sizeof(record));
		UT_ASSERT(!cluster_cold_tt_undo_record_v1(&typed, address, &record, &generation));
		assert_record_cleared(&record, generation);
		UT_ASSERT_EQ(cluster_cold_tt_base_count_v1(&typed), 0);
		cluster_cold_tt_base_destroy_v1(&typed);
	}
}

static ClusterRecoveryTxnKey
graph_fixture(ClusterColdTypedV1 *typed, uint8 owner)
{
	uint32 segment = (owner - 1) * CLUSTER_UNDO_SEGS_PER_INSTANCE + 1;
	UndoSegmentHeaderData canonical = header_for(owner, segment);
	UndoSegmentHeaderData physical = header_for(owner, segment + 1);
	ClusterRecoveryTxnKey key = { 0 };

	fixture(typed);
	typed->system_identifier = 1234;
	typed->observer.database_incarnation = 5678;
	typed->participant_count = 2;
	typed->replay_count = 1;
	typed->participants[1].thread_id = owner; /* A retained history origin. */
	canonical.tt_slots[6].xid = 43;
	canonical.tt_slots[6].wrap = 0;
	canonical.tt_slots[6].status = TT_SLOT_ACTIVE;
	physical.wrap_count = 91;
	write_header(segment, &canonical, true);
	write_header(segment + 1, &physical, true);
	write_record_block(owner, segment, 3, false);
	write_record_block(owner, segment + 1, 3, false);
	UT_ASSERT_EQ(cluster_cold_tt_base_capture_v1(typed, INPUT_BUDGET), CLUSTER_COLD_OK);
	key.system_identifier = typed->system_identifier;
	key.database_incarnation = typed->observer.database_incarnation;
	key.segment_id = segment;
	key.xid = 43;
	key.origin_node_id = owner - 1;
	key.slot_offset = 6;
	return key;
}

/* Fixture edits use original on-disk directory offsets, without the adapter. */
static UndoRecordHeader
graph_record_header(uint32 segment, bool replace, const UndoRecordHeader *replacement)
{
	UndoRecordHeader header;
	UndoSlotDirEntry directory;
	char path[MAXPGPATH];
	int fd;

	file_path(segment, path);
	fd = open(path, O_RDWR);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(pread(fd, &directory, sizeof(directory), 3 * BLCKSZ + UNDO_SLOT_DIR_OFFSET(1)),
				 sizeof(directory));
	UT_ASSERT_EQ(pread(fd, &header, sizeof(header), 3 * BLCKSZ + directory.record_offset),
				 sizeof(header));
	if (replace) {
		directory.flags = replacement->flags;
		UT_ASSERT_EQ(
			pwrite(fd, replacement, sizeof(*replacement), 3 * BLCKSZ + directory.record_offset),
			sizeof(*replacement));
		UT_ASSERT_EQ(
			pwrite(fd, &directory, sizeof(directory), 3 * BLCKSZ + UNDO_SLOT_DIR_OFFSET(1)),
			sizeof(directory));
	}
	UT_ASSERT_EQ(close(fd), 0);
	return header;
}

static void
graph_change_padding(uint32 segment)
{
	char path[MAXPGPATH];
	char changed = 1;
	int fd;

	file_path(segment, path);
	fd = open(path, O_RDWR);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(pwrite(fd, &changed, 1, 3 * BLCKSZ + BLCKSZ / 2), 1);
	UT_ASSERT_EQ(close(fd), 0);
}

UT_TEST(test_graph_read_binds_history_key_and_retains_physical_generation)
{
	for (uint16 owner = 1; owner <= 128; owner += 127) {
		ClusterColdTypedV1 typed;
		ClusterRecoveryTxnKey key = graph_fixture(&typed, owner);
		ClusterRecoveryUndoRecord record;
		uint32 generation;
		int calls;

		UT_ASSERT_EQ(cluster_cold_tt_graph_read_v1(&typed, &key,
												   record_address(key.segment_id + 1, 3, 1),
												   &record, &generation),
					 CLUSTER_RECOVERY_GRAPH_RECORD);
		UT_ASSERT_EQ(generation, 91);
		UT_ASSERT_EQ(record.header.tt_slot_segment_id, key.segment_id);
		UT_ASSERT_EQ(record.block_header.crc64, 0); /* No checksum authority. */
		calls = alloc_calls;
		typed.tt_base->budget = typed.tt_base->used;
		alloc_fail = alloc_calls + 1;
		UT_ASSERT_EQ(cluster_cold_tt_graph_read_v1(&typed, &key,
												   record_address(key.segment_id + 1, 3, 1),
												   &record, &generation),
					 CLUSTER_RECOVERY_GRAPH_RECORD);
		UT_ASSERT_EQ(alloc_calls, calls);
		UT_ASSERT_EQ(cluster_cold_tt_base_recheck_v1(&typed), CLUSTER_COLD_OK);
		cluster_cold_tt_base_destroy_v1(&typed);
	}
}

UT_TEST(test_graph_key_and_captured_namespace_mismatches_are_sticky)
{
	for (int fault = 0; fault < 15; fault++) {
		ClusterColdTypedV1 typed;
		ClusterRecoveryTxnKey key = graph_fixture(&typed, 1), changed = key;
		ClusterRecoveryUndoRecord record;
		uint32 generation = UINT32_MAX;

		switch (fault) {
		case 0:
			changed.system_identifier++;
			break;
		case 1:
			changed.database_incarnation++;
			break;
		case 2:
			changed.segment_id++;
			break;
		case 3:
			changed.segment_generation++;
			break;
		case 4:
			changed.xid++;
			break;
		case 5:
			changed.origin_node_id++;
			break;
		case 6:
			changed.slot_offset++;
			break;
		case 7:
			changed.slot_wrap++;
			break;
		case 8:
			changed.system_identifier = ++typed.system_identifier;
			break;
		case 9:
			changed.database_incarnation = ++typed.observer.database_incarnation;
			break;
		case 10:
			changed.system_identifier = 0;
			break;
		case 11:
			changed.database_incarnation = 0;
			break;
		case 12:
			changed.xid = InvalidTransactionId;
			break;
		case 13:
			changed.origin_node_id = UINT16_MAX;
			break;
		case 14:
			changed.slot_offset = UINT16_MAX;
			break;
		}
		memset(&record, 0xA5, sizeof(record));
		UT_ASSERT_EQ(cluster_cold_tt_graph_read_v1(&typed, &changed, record_address(2, 3, 1),
												   &record, &generation),
					 CLUSTER_RECOVERY_GRAPH_IDENTITY);
		assert_record_cleared(&record, generation);
		UT_ASSERT_EQ(cluster_cold_tt_base_count_v1(&typed), 0);
		UT_ASSERT(cluster_cold_tt_graph_read_v1(&typed, &key, record_address(2, 3, 1), &record,
												&generation)
				  != CLUSTER_RECOVERY_GRAPH_RECORD);
		assert_record_cleared(&record, generation);
		cluster_cold_tt_base_destroy_v1(&typed);
	}
}

UT_TEST(test_graph_record_identity_and_unknown_wrap_refuse)
{
	for (int fault = 0; fault < 6; fault++) {
		ClusterColdTypedV1 typed;
		ClusterRecoveryTxnKey key = graph_fixture(&typed, 1);
		UndoRecordHeader header = graph_record_header(2, false, NULL);
		ClusterRecoveryUndoRecord record;
		uint32 generation = UINT32_MAX;

		switch (fault) {
		case 0:
			header.tt_slot_segment_id++;
			break;
		case 1:
			header.origin_node_id++;
			break;
		case 2:
			header.xid++;
			break;
		case 3:
			header.tt_slot_id++;
			break;
		case 4:
			header.tt_wrap_plus1 = 0;
			break;
		case 5:
			header.tt_wrap_plus1++;
			break;
		}
		graph_record_header(2, true, &header);
		memset(&record, 0xA5, sizeof(record));
		UT_ASSERT(cluster_cold_tt_graph_read_v1(&typed, &key, record_address(2, 3, 1), &record,
												&generation)
				  != CLUSTER_RECOVERY_GRAPH_RECORD);
		assert_record_cleared(&record, generation);
		UT_ASSERT_EQ(cluster_cold_tt_base_count_v1(&typed), 0);
		cluster_cold_tt_base_destroy_v1(&typed);
	}
}

UT_TEST(test_graph_cached_reads_recheck_all_source_bytes)
{
	for (int fault = 0; fault < 4; fault++) {
		ClusterColdTypedV1 typed;
		ClusterRecoveryTxnKey key = graph_fixture(&typed, 1);
		ClusterRecoveryUndoRecord record;
		uint32 generation;

		UT_ASSERT_EQ(cluster_cold_tt_graph_read_v1(&typed, &key, record_address(2, 3, 1), &record,
												   &generation),
					 CLUSTER_RECOVERY_GRAPH_RECORD);
		if (fault == 0)
			graph_change_padding(2);
		else if (fault == 1) {
			UndoSegmentHeaderData changed = header_for(1, 2);
			changed.wrap_count = 92;
			write_header(2, &changed, false);
		} else if (fault == 2)
			read_fault = 4;
		else {
			char path[MAXPGPATH];
			file_path(2, path);
			UT_ASSERT_EQ(unlink(path), 0);
		}
		memset(&record, 0xA5, sizeof(record));
		generation = UINT32_MAX;
		UT_ASSERT_EQ(cluster_cold_tt_graph_read_v1(&typed, &key, record_address(2, 3, 1), &record,
												   &generation),
					 CLUSTER_RECOVERY_GRAPH_INVALID);
		assert_record_cleared(&record, generation);
		UT_ASSERT_EQ(cluster_cold_tt_base_count_v1(&typed), 0);
		cluster_cold_tt_base_destroy_v1(&typed);
	}
}

UT_TEST(test_graph_record_allocation_refusals_keep_capacity_detail)
{
	for (int fault = 0; fault < 2; fault++) {
		ClusterColdTypedV1 typed;
		ClusterRecoveryTxnKey key = graph_fixture(&typed, 1);
		ClusterRecoveryUndoRecord record;
		uint32 generation = UINT32_MAX;

		if (fault == 0)
			typed.tt_base->budget = typed.tt_base->used;
		else
			alloc_fail = alloc_calls + 1;
		memset(&record, 0xA5, sizeof(record));
		UT_ASSERT_EQ(cluster_cold_tt_graph_read_v1(&typed, &key, record_address(2, 3, 1), &record,
												   &generation),
					 CLUSTER_RECOVERY_GRAPH_CAPACITY);
		assert_record_cleared(&record, generation);
		UT_ASSERT_EQ(cluster_cold_tt_base_recheck_v1(&typed),
					 fault == 0 ? CLUSTER_COLD_CAPACITY : CLUSTER_COLD_OOM);
		UT_ASSERT_EQ(cluster_cold_tt_base_count_v1(&typed), 0);
		cluster_cold_tt_base_destroy_v1(&typed);
	}
}

static void
graph_traverse(bool change_before_verify)
{
	ClusterColdTypedV1 typed;
	ClusterRecoveryTxnKey key = graph_fixture(&typed, 128);
	ClusterRecoveryReferenceGraph *graph = NULL;
	ClusterRecoveryGraphRecord out;
	ClusterRecoveryGraphResult result = CLUSTER_RECOVERY_GRAPH_MORE;
	UndoRecordHeader head = graph_record_header(key.segment_id + 1, false, NULL);
	unsigned records = 0;

	head.prev_uba = record_address(key.segment_id, 3, 1);
	head.flags = 0;
	head.target_locator.relNumber++; /* prev is a transaction edge across relations. */
	graph_record_header(key.segment_id + 1, true, &head);
	UT_ASSERT_EQ(cluster_recovery_reference_graph_begin(INPUT_BUDGET, cluster_cold_tt_graph_read_v1,
														&typed, &graph),
				 CLUSTER_RECOVERY_GRAPH_MORE);
	UT_ASSERT_EQ(
		cluster_recovery_reference_graph_add(graph, &key, record_address(key.segment_id + 1, 3, 1)),
		CLUSTER_RECOVERY_GRAPH_MORE);
	for (unsigned i = 0; i < 32; i++) {
		result = cluster_recovery_reference_graph_step(graph, &out);
		if (result == CLUSTER_RECOVERY_GRAPH_RECORD) {
			UT_ASSERT_NOT_NULL(out.record);
			UT_ASSERT_EQ(out.record_segment_generation, records == 0 ? 91 : 0);
			records++;
			if (change_before_verify && records == 2)
				graph_change_padding(key.segment_id + 1);
		} else if (result != CLUSTER_RECOVERY_GRAPH_MORE)
			break;
	}
	UT_ASSERT_EQ(records, 2);
	UT_ASSERT_EQ(result, change_before_verify ? CLUSTER_RECOVERY_GRAPH_INVALID
											  : CLUSTER_RECOVERY_GRAPH_DRAINED);
	UT_ASSERT_NULL(out.record);
	UT_ASSERT_EQ(cluster_recovery_reference_graph_step(graph, &out), result);
	if (change_before_verify)
		UT_ASSERT_EQ(cluster_cold_tt_base_count_v1(&typed), 0);
	else {
		UT_ASSERT_EQ(cluster_recovery_reference_graph_count(graph), 2);
		UT_ASSERT_EQ(cluster_cold_tt_base_recheck_v1(&typed), CLUSTER_COLD_OK);
	}
	cluster_recovery_reference_graph_destroy(graph);
	cluster_cold_tt_base_destroy_v1(&typed);
}

UT_TEST(test_original_graph_traverses_cross_segment_cross_relation_prev)
{
	graph_traverse(false);
}

UT_TEST(test_original_graph_second_pass_refuses_cached_byte_changes)
{
	graph_traverse(true);
}

UT_TEST(test_graph_invalid_arguments_clear_available_outputs)
{
	for (int fault = 0; fault < 4; fault++) {
		ClusterColdTypedV1 typed;
		ClusterRecoveryTxnKey key = graph_fixture(&typed, 1);
		ClusterRecoveryUndoRecord record, zero = { 0 };
		uint32 generation = UINT32_MAX;

		memset(&record, 0xA5, sizeof(record));
		UT_ASSERT_EQ(
			cluster_cold_tt_graph_read_v1(fault == 0 ? NULL : &typed, fault == 1 ? NULL : &key,
										  record_address(2, 3, 1), fault == 2 ? NULL : &record,
										  fault == 3 ? NULL : &generation),
			CLUSTER_RECOVERY_GRAPH_INVALID);
		if (fault != 2)
			UT_ASSERT_EQ(memcmp(&record, &zero, sizeof(zero)), 0);
		if (fault != 3)
			UT_ASSERT_EQ(generation, 0);
		if (fault != 0)
			UT_ASSERT_EQ(cluster_cold_tt_base_count_v1(&typed), 0);
		cluster_cold_tt_base_destroy_v1(&typed);
	}
}

#include "test_cluster_cold_tt_after_redo.h"

int
main(void)
{
	UT_PLAN(45);
	UT_RUN(test_after_redo_reads_changed_slots_without_overwriting_original_base);
	UT_RUN(test_after_redo_retains_changed_undo_blocks_separately);
	UT_RUN(test_after_redo_undo_only_drift_invalidates_post_input);
	UT_RUN(test_after_redo_undo_capture_failure_discards_partial_post_input);
	UT_RUN(test_after_redo_reenumerates_created_removed_and_history_origin_segments);
	UT_RUN(test_after_redo_readback_drift_is_sticky_and_keeps_original_input);
	UT_RUN(test_after_redo_shares_original_tt_budget_and_discards_partial_capture);
	UT_RUN(test_after_redo_requires_original_namespace_and_base);
	UT_RUN(test_after_redo_failed_io_never_exports_partial_slots);
	UT_RUN(test_after_redo_reset_and_bounds_clear_only_post_redo_outputs);
	UT_RUN(test_after_redo_namespace_drift_hides_captured_outputs);
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
	UT_RUN(test_original_undo_records_keep_raw_identity_and_history_origin);
	UT_RUN(test_record_physical_segment_is_distinct_from_canonical_tt_segment);
	UT_RUN(test_record_file_and_io_drift_invalidate_all_captured_inputs);
	UT_RUN(test_record_recheck_covers_unused_bytes_and_data_read_failure);
	UT_RUN(test_complete_block_refuses_bad_unselected_row);
	UT_RUN(test_record_capacity_and_allocation_failure_discard_the_capture);
	UT_RUN(test_record_invalid_address_and_missing_source_clear_outputs);
	UT_RUN(test_graph_read_binds_history_key_and_retains_physical_generation);
	UT_RUN(test_graph_key_and_captured_namespace_mismatches_are_sticky);
	UT_RUN(test_graph_record_identity_and_unknown_wrap_refuse);
	UT_RUN(test_graph_cached_reads_recheck_all_source_bytes);
	UT_RUN(test_graph_record_allocation_refusals_keep_capacity_detail);
	UT_RUN(test_original_graph_traverses_cross_segment_cross_relation_prev);
	UT_RUN(test_original_graph_second_pass_refuses_cached_byte_changes);
	UT_RUN(test_graph_invalid_arguments_clear_available_outputs);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
