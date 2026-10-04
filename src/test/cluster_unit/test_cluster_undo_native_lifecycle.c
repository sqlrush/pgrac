/*-------------------------------------------------------------------------
 * test_cluster_undo_native_lifecycle.c
 *    Native typed UNDO lifecycle: real headers and physical I/O boundaries.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <setjmp.h>

#include "cluster/cluster_side_undo.h"
#include "cluster/cluster_tt_durable.h"
#include "cluster/cluster_uba.h"
#include "cluster/cluster_undo_segment.h"
#include "cluster/cluster_undo_segment_init.h"
#include "cluster/storage/cluster_undo_alloc.h"
#include "cluster/storage/cluster_undo_xlog.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

static PGAlignedBlock disk;
static uint32 opens, writes, syncs, closes, extends, mkdirs, dirsyncs, applies, skips;
static bool expect_panic;
static bool fail_sync;
static jmp_buf panic_jump;

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "%s:%d: %s\n", file, line, condition);
	abort();
}

#include "test_cluster_undo_header_identity.inc"

static int
fixture_path(int intent, uint8 instance, uint32 segment, char *path, size_t length)
{
	UT_ASSERT_EQ(intent, CLUSTER_UNDO_PATH_RECOVERY_SHARED);
	UT_ASSERT_EQ(instance, 3);
	UT_ASSERT_EQ(segment, 513);
	strlcpy(path, "undo/instance_2/seg_513.dat", length);
	return 0;
}

static int
fixture_open(const char *path, int flags)
{
	opens++;
	return 42;
}

static int
fixture_extend(int fd, off_t size)
{
	UT_ASSERT_EQ(fd, 42);
	UT_ASSERT_EQ(size, UNDO_SEGMENT_SIZE_BYTES);
	extends++;
	return 0;
}

static ssize_t
fixture_read(int fd, void *buffer, size_t length, off_t offset)
{
	UT_ASSERT_EQ(fd, 42);
	UT_ASSERT_EQ(length, BLCKSZ);
	UT_ASSERT_EQ(offset, 0);
	memcpy(buffer, disk.data, length);
	return length;
}

static ssize_t
fixture_write(int fd, const void *buffer, size_t length, off_t offset)
{
	UT_ASSERT_EQ(fd, 42);
	UT_ASSERT((length == BLCKSZ && offset == 0)
		|| (length == sizeof(TTSlot)
			&& offset == offsetof(UndoSegmentHeaderData, tt_slots) + 4 * sizeof(TTSlot)));
	UT_ASSERT_EQ(syncs, 0);
	writes++;
	memcpy(disk.data + offset, buffer, length);
	return length;
}

static int
fixture_sync(int fd)
{
	UT_ASSERT_EQ(fd, 42);
	UT_ASSERT_EQ(writes, 1);
	UT_ASSERT_EQ(closes, 0);
	syncs++;
	return fail_sync ? -1 : 0;
}

static int
fixture_close(int fd)
{
	UT_ASSERT_EQ(fd, 42);
	closes++;
	return 0;
}

static void
fixture_dir_sync(const char *path, bool isdir)
{
	UT_ASSERT(isdir);
	UT_ASSERT(strcmp(path, "undo/instance_2") == 0);
	UT_ASSERT_EQ(syncs, 1);
	UT_ASSERT_EQ(closes, 1);
	dirsyncs++;
}

#undef ereport
#define ereport(level_, rest_) \
	do { if (expect_panic) longjmp(panic_jump, 1); abort(); } while (0)
#define build_undo_segment_path fixture_path
#define cluster_undo_recovery_intent_for_owner(instance_) CLUSTER_UNDO_PATH_RECOVERY_SHARED
#define ensure_undo_instance_subdir(instance_) (mkdirs++)
#define BasicOpenFile fixture_open
#define ftruncate fixture_extend
#undef pg_pread
#undef pg_pwrite
#define pg_pread fixture_read
#define pg_pwrite fixture_write
#define pg_fsync fixture_sync
#define close fixture_close
#define fsync_fname fixture_dir_sync
#define cluster_vis_bump_recovery_undo_redo_applies() (applies++)
#define cluster_vis_bump_recovery_undo_redo_skips() (skips++)
#include "test_cluster_undo_lifecycle_native.inc"
#define cluster_undo_redo_open_segment(instance_, segment_, path_, create_) fixture_open(path_, 0)
#define cluster_tt_durable_count_redo_apply() ((void)0)
#include "test_cluster_undo_bind_native.inc"

static void
reset_io(void)
{
	opens = writes = syncs = closes = extends = mkdirs = dirsyncs = applies = skips = 0;
	fail_sync = false;
}

static void
apply_record(uint8 opcode, void *data, uint32 length)
{
	XLogReaderState reader = {0};
	DecodedXLogRecord record = {0};
	ClusterUndoDecoded decoded;

	record.header.xl_rmid = RM_CLUSTER_UNDO_ID;
	record.header.xl_info = opcode;
	record.main_data = data;
	record.main_data_len = length;
	reader.record = &record;
	if (!cluster_undo_decode(&reader, &decoded) || !cluster_undo_preflight(&decoded)) {
		if (expect_panic)
			longjmp(panic_jump, 1);
		abort();
	}
	switch (opcode) {
	case XLOG_UNDO_SEGMENT_INIT:
		cluster_undo_redo_segment_init(&decoded, (const uint8 *)data + decoded.payload_offset);
		break;
	case XLOG_UNDO_SEGMENT_REUSE:
		cluster_undo_redo_segment_reuse(&decoded, (const uint8 *)data + decoded.payload_offset);
		break;
	case XLOG_UNDO_SEGMENT_RECYCLE:
		cluster_undo_redo_segment_recycle(&decoded);
		break;
	default:
		abort();
	}
}

UT_TEST(test_private_tt_header_sequence_and_unrelated_bytes)
{
	ClusterUndoDecoded decoded = {0};
	PGAlignedBlock base, result, original;
	UndoSegmentHeaderData *header = (UndoSegmentHeaderData *)base.data;
	TTSlot *slot = &header->tt_slots[4];

	reset_io();
	cluster_undo_segment_make_header_bytes(513, 3, base.data);
	header->wrap_count = 9;
	/* Other slots, allocation metadata and every untouched byte survive. */
	header->tt_slots[5].xid = 712;
	original = base;
	decoded.kind = CLUSTER_UNDO_KIND_TT_BIND;
	decoded.opcode = XLOG_UNDO_TT_SLOT_BIND;
	decoded.instance = 3;
	decoded.segment_id = 513;
	decoded.expected_generation = 9;
	decoded.slot_offset = 4;
	decoded.wrap = 2;
	decoded.xid = 701;
	decoded.format_version = CLUSTER_UNDO_TT_BIND_VERSION;
	UT_ASSERT_EQ(cluster_undo_prepare_header_v1(&decoded, NULL, 0, base.data, result.data),
				 CLUSTER_UNDO_HEADER_APPLY);
	if (ut_current_failed)
		return;
	UT_ASSERT(memcmp(base.data, original.data, BLCKSZ) == 0);
	base = result;
	UT_ASSERT_EQ(slot->status, TT_SLOT_ACTIVE);
	UT_ASSERT_EQ(slot->xid, 701);
	UT_ASSERT_EQ(slot->wrap, 2);
	UT_ASSERT(UBA_is_invalid(slot->first_undo_block));
	decoded.kind = CLUSTER_UNDO_KIND_TT_COMMIT;
	decoded.opcode = XLOG_UNDO_TT_SLOT_COMMIT;
	decoded.format_version = 0;
	decoded.commit_scn = 804;
	UT_ASSERT_EQ(cluster_undo_prepare_header_v1(&decoded, NULL, 0, base.data, base.data),
				 CLUSTER_UNDO_HEADER_APPLY);
	UT_ASSERT_EQ(slot->status, TT_SLOT_COMMITTED);
	UT_ASSERT_EQ(slot->commit_scn, 804);
	memset(slot, 0, sizeof(*slot));
	UT_ASSERT(memcmp(base.data, original.data, BLCKSZ) == 0);
	UT_ASSERT_EQ(opens + writes + syncs, 0);
}

UT_TEST(test_private_tt_header_refuses_identity_and_conflicting_predecessor)
{
	for (int fault = 0; fault < 7; fault++) {
		ClusterUndoDecoded decoded = {0};
		PGAlignedBlock base, before, result, untouched;
		UndoSegmentHeaderData *header = (UndoSegmentHeaderData *)base.data;

		cluster_undo_segment_make_header_bytes(513, 3, base.data);
		header->wrap_count = 9;
		decoded.kind = CLUSTER_UNDO_KIND_TT_BIND;
		decoded.opcode = XLOG_UNDO_TT_SLOT_BIND;
		decoded.instance = 3;
		decoded.segment_id = 513;
		decoded.expected_generation = 9;
		decoded.slot_offset = 4;
		decoded.wrap = 2;
		decoded.xid = 701;
		decoded.format_version = CLUSTER_UNDO_TT_BIND_VERSION;
		if (fault == 0) header->owner_instance = 2;
		if (fault == 1) header->segment_id++;
		if (fault == 2) header->wrap_count--;
		if (fault == 3) header->tt_slots[4].commit_scn = 52;
		if (fault == 4) decoded.opcode = XLOG_UNDO_TT_SLOT_COMMIT;
		if (fault == 5) decoded.has_payload = true;
		if (fault == 6) ((PageHeader)base.data)->pd_pagesize_version = 0;
		before = base;
		memset(untouched.data, 0x62, BLCKSZ);
		result = untouched;
		UT_ASSERT_EQ(cluster_undo_prepare_header_v1(&decoded, NULL, 0, base.data, result.data),
					 CLUSTER_UNDO_HEADER_BLOCKED);
		UT_ASSERT(memcmp(base.data, before.data, BLCKSZ) == 0);
		UT_ASSERT(memcmp(result.data, untouched.data, BLCKSZ) == 0);
	}
}

UT_TEST(test_native_bind_uses_complete_header_identity_and_durable_slot)
{
	for (int fault = 0; fault < 4; fault++) {
		PGAlignedBlock before;
		UndoSegmentHeaderData *header = (UndoSegmentHeaderData *)disk.data;

		cluster_undo_segment_make_header_bytes(513, 3, disk.data);
		header->wrap_count = 9;
		if (fault == 1) header->pd_pagesize_version = 0;
		if (fault == 2) header->segment_size_bytes--;
		if (fault == 3) header->pd_flags &= ~PD_UNDO_SEG_HEADER;
		before = disk;
		reset_io();
		expect_panic = fault != 0;
		if (setjmp(panic_jump) == 0) {
			cluster_tt_durable_redo_bind_slot(3, 513, 9, 4, 2, 701);
			UT_ASSERT(!expect_panic);
		}
		expect_panic = false;
		if (fault == 0) {
			UT_ASSERT_EQ(header->tt_slots[4].status, TT_SLOT_ACTIVE);
			UT_ASSERT_EQ(writes, 1);
			UT_ASSERT_EQ(syncs, 1);
			UT_ASSERT_EQ(applies, 1);
			reset_io();
			cluster_tt_durable_redo_bind_slot(3, 513, 9, 4, 2, 701);
			UT_ASSERT_EQ(writes + syncs + applies + skips, 0);
		} else {
			UT_ASSERT_EQ(writes + syncs + applies, 0);
			UT_ASSERT(memcmp(disk.data, before.data, BLCKSZ) == 0);
		}
		UT_ASSERT_EQ(closes, 1);
	}
	cluster_undo_segment_make_header_bytes(513, 3, disk.data);
	((UndoSegmentHeaderData *)disk.data)->wrap_count = 10;
	reset_io();
	cluster_tt_durable_redo_bind_slot(3, 513, 9, 4, 2, 701);
	UT_ASSERT_EQ(writes + syncs + applies, 0);
	UT_ASSERT_EQ(skips, 1);
	((UndoSegmentHeaderData *)disk.data)->wrap_count = 9;
	reset_io();
	fail_sync = expect_panic = true;
	if (setjmp(panic_jump) == 0) {
		cluster_tt_durable_redo_bind_slot(3, 513, 9, 4, 2, 701);
		UT_ASSERT(false);
	}
	fail_sync = expect_panic = false;
	UT_ASSERT_EQ(writes, 1);
	UT_ASSERT_EQ(syncs, 1);
	UT_ASSERT_EQ(closes, 1);
	UT_ASSERT_EQ(applies + skips, 0);
}

/* PU-D-9 (F-D-30): BIND redo over the same entity's durable terminal stamp
 * skips without writing; another xid at that wrap still PANICs. */
UT_TEST(test_native_bind_replay_over_own_terminal_skips)
{
	static const uint8 terminal[] = { TT_SLOT_COMMITTED, TT_SLOT_ABORTED };

	for (int i = 0; i < (int)lengthof(terminal); i++) {
		UndoSegmentHeaderData *header = (UndoSegmentHeaderData *)disk.data;
		TTSlot *slot = &header->tt_slots[4];
		PGAlignedBlock before;

		cluster_undo_segment_make_header_bytes(513, 3, disk.data);
		header->wrap_count = 9;
		slot->xid = 701;
		slot->wrap = 2;
		slot->flags = TT_FLAGS_RESERVED;
		slot->status = terminal[i];
		slot->commit_scn = terminal[i] == TT_SLOT_COMMITTED ? 804 : InvalidScn;
		before = disk;
		reset_io();
		expect_panic = false;
		if (setjmp(panic_jump) == 0)
			cluster_tt_durable_redo_bind_slot(3, 513, 9, 4, 2, 701);
		else
			UT_ASSERT(false);
		UT_ASSERT_EQ(writes + syncs + applies, 0);
		UT_ASSERT_EQ(skips, 1);
		UT_ASSERT_EQ(closes, 1);
		UT_ASSERT(memcmp(disk.data, before.data, BLCKSZ) == 0);

		reset_io();
		expect_panic = true;
		if (setjmp(panic_jump) == 0) {
			cluster_tt_durable_redo_bind_slot(3, 513, 9, 4, 2, 702);
			UT_ASSERT(false);
		}
		expect_panic = false;
		UT_ASSERT_EQ(writes + syncs + applies + skips, 0);
		UT_ASSERT(memcmp(disk.data, before.data, BLCKSZ) == 0);
		if (ut_current_failed)
			printf("# terminal status %u\n", terminal[i]);
	}
}

UT_TEST(test_private_abort_head_release_and_exact_stale_results)
{
	for (int exact = 0; exact < 2; exact++) {
		ClusterUndoDecoded decoded = {0};
		PGAlignedBlock base, result, untouched;
		UndoSegmentHeaderData *header = (UndoSegmentHeaderData *)base.data;
		TTSlot *slot = &header->tt_slots[4];

		cluster_undo_segment_make_header_bytes(513, 3, base.data);
		header->wrap_count = 9;
		slot->status = TT_SLOT_ACTIVE;
		slot->xid = 701;
		slot->wrap = 2;
		decoded.kind = CLUSTER_UNDO_KIND_TT_ABORT;
		decoded.opcode = XLOG_UNDO_TT_SLOT_ABORT;
		decoded.instance = 3;
		decoded.segment_id = 513;
		decoded.expected_generation = 9;
		decoded.slot_offset = 4;
		decoded.wrap = 2;
		decoded.xid = 701;
		decoded.format_version = exact ? CLUSTER_UNDO_TT_ABORT_EXACT_VERSION : 0;
		UT_ASSERT_EQ(cluster_undo_prepare_header_v1(&decoded, NULL, 0, base.data, base.data),
					 CLUSTER_UNDO_HEADER_APPLY);
		UT_ASSERT_EQ(slot->status, TT_SLOT_ABORTED);
		UT_ASSERT(UBA_is_invalid(slot->first_undo_block));
		memset(untouched.data, 0x6a, BLCKSZ);
		result = untouched;
		UT_ASSERT_EQ(cluster_undo_prepare_header_v1(&decoded, NULL, 0, base.data, result.data),
					 CLUSTER_UNDO_HEADER_ALREADY);
		UT_ASSERT(memcmp(result.data, untouched.data, BLCKSZ) == 0);
		decoded.kind = CLUSTER_UNDO_KIND_TT_SET_HEAD;
		decoded.opcode = XLOG_UNDO_TT_SLOT_SET_HEAD;
		decoded.format_version = 0;
		decoded.first_undo_block = uba_encode(513, 2, 4, 48);
		UT_ASSERT_EQ(cluster_undo_prepare_header_v1(&decoded, NULL, 0, base.data, base.data),
					 CLUSTER_UNDO_HEADER_APPLY);
		UT_ASSERT(memcmp(&slot->first_undo_block, &decoded.first_undo_block, sizeof(UBA)) == 0);
		UT_ASSERT_EQ(cluster_undo_prepare_header_v1(&decoded, NULL, 0, base.data, result.data),
					 CLUSTER_UNDO_HEADER_ALREADY);

		decoded.kind = CLUSTER_UNDO_KIND_TT_CTRC_RELEASE;
		decoded.opcode = XLOG_UNDO_TT_SLOT_CTRC_RELEASE;
		decoded.format_version = CLUSTER_UNDO_TT_CTRC_RELEASE_VERSION;
		decoded.flags = CLUSTER_UNDO_TT_CTRC_RELEASE_ALL_TOUCHED_ACKED;
		decoded.terminal_status = TT_SLOT_ABORTED;
		decoded.cluster_epoch = 11;
		decoded.root_id = 12;
		decoded.root_generation = 13;
		decoded.formation_epoch = 14;
		decoded.admission_record_generation = 15;
		decoded.seal_generation = 16;
		decoded.touched_nodes_low = 4;
		memset(decoded.ack_set_digest, 0x5a, sizeof(decoded.ack_set_digest));
		UT_ASSERT_EQ(cluster_undo_prepare_header_v1(&decoded, NULL, 0, base.data, base.data),
					 CLUSTER_UNDO_HEADER_APPLY);
		UT_ASSERT_EQ(slot->flags, TT_SLOT_FLAG_CTRC_RELEASE_PROVEN);
		UT_ASSERT_EQ(cluster_undo_prepare_header_v1(&decoded, NULL, 0, base.data, result.data),
					 CLUSTER_UNDO_HEADER_ALREADY);
		UT_ASSERT(memcmp(result.data, untouched.data, BLCKSZ) == 0);
		header->wrap_count++;
		UT_ASSERT_EQ(cluster_undo_prepare_header_v1(&decoded, NULL, 0, base.data, result.data),
					 CLUSTER_UNDO_HEADER_SKIP_STALE);
		UT_ASSERT(memcmp(result.data, untouched.data, BLCKSZ) == 0);
	}
}

UT_TEST(test_native_init_validates_real_image_before_file_mutation)
{
	struct {
		xl_cluster_undo_segment_init header;
		char image[BLCKSZ];
	} wal = {0};
	PGAlignedBlock image, before;

	wal.header.instance = 3;
	wal.header.segment_id = 513;
	for (int invalid = 0; invalid < 5; invalid++) {
		cluster_undo_segment_make_header_bytes(513, 3, image.data);
		if (invalid == 1)
			((UndoSegmentHeaderData *)image.data)->owner_instance = 2;
		if (invalid == 2)
			((UndoSegmentHeaderData *)image.data)->segment_id = 514;
		if (invalid == 3)
			((PageHeader)image.data)->pd_pagesize_version = 0;
		if (invalid == 4)
			((UndoSegmentHeaderData *)image.data)->wrap_count = 1;
		memcpy(wal.image, image.data, BLCKSZ);
		memset(disk.data, 0x5b, BLCKSZ);
		before = disk;
		reset_io();
		expect_panic = invalid != 0;
		if (setjmp(panic_jump) == 0) {
			apply_record(XLOG_UNDO_SEGMENT_INIT, &wal, sizeof(wal));
			UT_ASSERT(!expect_panic);
		}
		expect_panic = false;
		if (invalid == 0) {
			UT_ASSERT(memcmp(disk.data, image.data, BLCKSZ) == 0);
			UT_ASSERT_EQ(writes, 1);
			UT_ASSERT_EQ(syncs, 1);
			UT_ASSERT_EQ(dirsyncs, 1);
			UT_ASSERT_EQ(applies, 1);
		} else {
			UT_ASSERT_EQ(opens + extends + mkdirs + writes + syncs + applies, 0);
			UT_ASSERT(memcmp(disk.data, before.data, BLCKSZ) == 0);
		}
	}
}

UT_TEST(test_native_reuse_image_generation_and_recycle_identity)
{
	struct {
		xl_undo_segment_reuse header;
		char image[BLCKSZ];
	} wal = {0};
	PGAlignedBlock image, before;
	xl_undo_segment_recycle recycle = {0};

	wal.header.instance = recycle.instance = 3;
	wal.header.segment_id = recycle.segment_id = 513;
	wal.header.old_generation = 8;
	wal.header.new_generation = 9;
	cluster_undo_segment_make_header_bytes(513, 3, image.data);
	((UndoSegmentHeaderData *)image.data)->wrap_count = 10;
	memcpy(wal.image, image.data, BLCKSZ);
	cluster_undo_segment_make_header_bytes(513, 3, disk.data);
	((UndoSegmentHeaderData *)disk.data)->wrap_count = 8;
	before = disk;
	reset_io();
	expect_panic = true;
	if (setjmp(panic_jump) == 0) {
		apply_record(XLOG_UNDO_SEGMENT_REUSE, &wal, sizeof(wal));
		UT_ASSERT(false);
	}
	expect_panic = false;
	UT_ASSERT_EQ(opens + extends + mkdirs + writes + syncs + applies, 0);
	UT_ASSERT(memcmp(disk.data, before.data, BLCKSZ) == 0);

	disk = before;
	((UndoSegmentHeaderData *)disk.data)->owner_instance = 2;
	before = disk;
	recycle.expected_generation = 8;
	recycle.old_state = SEGMENT_COMMITTED;
	recycle.new_state = SEGMENT_RECYCLABLE;
	reset_io();
	expect_panic = true;
	if (setjmp(panic_jump) == 0) {
		apply_record(XLOG_UNDO_SEGMENT_RECYCLE, &recycle, sizeof(recycle));
		UT_ASSERT(false);
	}
	expect_panic = false;
	UT_ASSERT_EQ(writes + syncs + applies, 0);
	UT_ASSERT(memcmp(disk.data, before.data, BLCKSZ) == 0);
}

UT_TEST(test_native_reuse_durability_and_stale_generation)
{
	struct {
		xl_undo_segment_reuse header;
		char image[BLCKSZ];
	} wal = {0};
	PGAlignedBlock image, before;

	wal.header.instance = 3;
	wal.header.segment_id = 513;
	wal.header.old_generation = 8;
	wal.header.new_generation = 9;
	cluster_undo_segment_make_header_bytes(513, 3, image.data);
	((UndoSegmentHeaderData *)image.data)->wrap_count = 9;
	memcpy(wal.image, image.data, BLCKSZ);
	for (int generation = 8; generation <= 10; generation++) {
		cluster_undo_segment_make_header_bytes(513, 3, disk.data);
		((UndoSegmentHeaderData *)disk.data)->wrap_count = generation;
		before = disk;
		reset_io();
		apply_record(XLOG_UNDO_SEGMENT_REUSE, &wal, sizeof(wal));
		if (generation <= 9) {
			UT_ASSERT_EQ(writes, 1);
			UT_ASSERT_EQ(syncs, 1);
			UT_ASSERT_EQ(dirsyncs, 1);
			UT_ASSERT_EQ(applies, 1);
			UT_ASSERT(memcmp(disk.data, image.data, BLCKSZ) == 0);
		} else {
			UT_ASSERT_EQ(writes + syncs + applies, 0);
			UT_ASSERT_EQ(skips, 1);
			UT_ASSERT(memcmp(disk.data, before.data, BLCKSZ) == 0);
		}
		UT_ASSERT_EQ(closes, 1);
	}
}

UT_TEST(test_private_header_decision_preserves_native_generation_and_output)
{
	ClusterUndoDecoded decoded = {0};
	PGAlignedBlock image, base, output, unchanged;

	decoded.kind = CLUSTER_UNDO_KIND_SEGMENT_REUSE;
	decoded.opcode = XLOG_UNDO_SEGMENT_REUSE;
	decoded.instance = 3;
	decoded.segment_id = 513;
	decoded.expected_generation = 8;
	decoded.new_generation = 9;
	decoded.has_payload = decoded.has_fpi = true;
	decoded.payload_length = BLCKSZ;
	cluster_undo_segment_make_header_bytes(513, 3, image.data);
	((UndoSegmentHeaderData *)image.data)->wrap_count = 9;
	memset(unchanged.data, 0x7d, BLCKSZ);
	for (int generation = 7; generation <= 10; generation++) {
		ClusterUndoHeaderPrepareResultV1 result;

		cluster_undo_segment_make_header_bytes(513, 3, base.data);
		((UndoSegmentHeaderData *)base.data)->wrap_count = generation;
		output = unchanged;
		result = cluster_undo_prepare_header_v1(&decoded, (const uint8 *)image.data,
			BLCKSZ, base.data, output.data);
		UT_ASSERT_EQ(result, generation == 7 ? CLUSTER_UNDO_HEADER_BLOCKED :
			generation == 10 ? CLUSTER_UNDO_HEADER_SKIP_STALE : CLUSTER_UNDO_HEADER_APPLY);
		UT_ASSERT(memcmp(output.data, generation == 8 || generation == 9 ?
			image.data : unchanged.data, BLCKSZ) == 0);
	}
	decoded.expected_generation = UINT32_MAX;
	decoded.new_generation = 0;
	UT_ASSERT(!cluster_undo_preflight(&decoded));
	decoded.kind = CLUSTER_UNDO_KIND_SEGMENT_RECYCLE;
	decoded.opcode = XLOG_UNDO_SEGMENT_RECYCLE;
	decoded.expected_generation = 8;
	decoded.has_payload = decoded.has_fpi = false;
	decoded.payload_length = 0;
	decoded.old_state = SEGMENT_COMMITTED;
	decoded.new_state = SEGMENT_RECYCLABLE;
	for (int state = SEGMENT_ALLOCATED; state <= SEGMENT_RECYCLABLE; state++) {
		cluster_undo_segment_make_header_bytes(513, 3, base.data);
		((UndoSegmentHeaderData *)base.data)->wrap_count = 8;
		((UndoSegmentHeaderData *)base.data)->segment_state = state;
		((UndoSegmentHeaderData *)base.data)->tt_slots[1].xid = 31;
		output = base;
		((UndoSegmentHeaderData *)output.data)->segment_state = SEGMENT_RECYCLABLE;
		UT_ASSERT_EQ(cluster_undo_prepare_header_v1(&decoded, NULL, 0, base.data, base.data),
			CLUSTER_UNDO_HEADER_APPLY);
		UT_ASSERT(memcmp(base.data, output.data, BLCKSZ) == 0);
	}
}

UT_TEST(test_native_recycle_fsync_failure_does_not_report_completion)
{
	xl_undo_segment_recycle record = {0};

	record.instance = 3;
	record.segment_id = 513;
	record.expected_generation = 8;
	record.old_state = SEGMENT_COMMITTED;
	record.new_state = SEGMENT_RECYCLABLE;
	cluster_undo_segment_make_header_bytes(513, 3, disk.data);
	((UndoSegmentHeaderData *)disk.data)->wrap_count = 8;
	reset_io();
	fail_sync = expect_panic = true;
	if (setjmp(panic_jump) == 0) {
		apply_record(XLOG_UNDO_SEGMENT_RECYCLE, &record, sizeof(record));
		UT_ASSERT(false);
	}
	fail_sync = expect_panic = false;
	UT_ASSERT_EQ(writes, 1);
	UT_ASSERT_EQ(syncs, 1);
	UT_ASSERT_EQ(closes, 1);
	UT_ASSERT_EQ(applies + skips + dirsyncs, 0);
}

UT_TEST(test_native_exact_commit_rejects_bad_header_before_write)
{
	for (int fault = 0; fault < 4; fault++) {
		PGAlignedBlock before;
		UndoSegmentHeaderData *header = (UndoSegmentHeaderData *)disk.data;

		cluster_undo_segment_make_header_bytes(513, 3, disk.data);
		header->wrap_count = 8;
		header->tt_slots[4].xid = 802;
		header->tt_slots[4].wrap = 7;
		header->tt_slots[4].status = TT_SLOT_ACTIVE;
		if (fault == 1) header->pd_pagesize_version = 0;
		if (fault == 2) header->segment_size_bytes = 0;
		if (fault == 3) header->pd_flags &= ~PD_UNDO_SEG_HEADER;
		before = disk;
		reset_io();
		expect_panic = fault != 0;
		if (setjmp(panic_jump) == 0) {
			cluster_tt_durable_redo_stamp_slot_exact(3, 513, 8, 4, 7, 802, 999);
			UT_ASSERT_EQ(fault, 0);
		}
		expect_panic = false;
		if (fault != 0) {
			UT_ASSERT_EQ(writes + syncs + applies, 0);
			UT_ASSERT(memcmp(disk.data, before.data, BLCKSZ) == 0);
		} else {
			UT_ASSERT_EQ(header->tt_slots[4].status, TT_SLOT_COMMITTED);
			UT_ASSERT_EQ(header->tt_slots[4].commit_scn, 999);
			UT_ASSERT_EQ(writes, 1);
			UT_ASSERT_EQ(syncs, 1);
		}
		UT_ASSERT_EQ(closes, 1);
	}
}

int
main(void)
{
	UT_PLAN(11);
	UT_RUN(test_native_exact_commit_rejects_bad_header_before_write);
	UT_RUN(test_private_abort_head_release_and_exact_stale_results);
	UT_RUN(test_native_bind_uses_complete_header_identity_and_durable_slot);
	UT_RUN(test_native_bind_replay_over_own_terminal_skips);
	UT_RUN(test_private_tt_header_sequence_and_unrelated_bytes);
	UT_RUN(test_private_tt_header_refuses_identity_and_conflicting_predecessor);
	UT_RUN(test_native_init_validates_real_image_before_file_mutation);
	UT_RUN(test_native_reuse_image_generation_and_recycle_identity);
	UT_RUN(test_native_reuse_durability_and_stale_generation);
	UT_RUN(test_private_header_decision_preserves_native_generation_and_output);
	UT_RUN(test_native_recycle_fsync_failure_does_not_report_completion);
	UT_DONE();
	return ut_failed_count != 0;
}
