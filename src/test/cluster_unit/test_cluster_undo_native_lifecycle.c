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

#include "access/xact.h"
#include "cluster/cluster_side_undo.h"
#include "cluster/cluster_tt_durable.h"
#include "cluster/cluster_uba.h"
#include "cluster/cluster_undo_segment.h"
#include "cluster/cluster_undo_segment_init.h"
#include "cluster/cluster_undo_smgr.h"
#include "cluster/storage/cluster_undo_alloc.h"
#include "cluster/storage/cluster_undo_xlog.h"
#include "storage/checksum.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

static PGAlignedBlock disk;
/* What survives a power loss: the file as of its last fsync, or as of the
 * last checkpoint that found the segment recorded for it. */
static PGAlignedBlock durable;
static uint32 opens, writes, syncs, closes, extends, mkdirs, dirsyncs, applies, skips;
static uint32 notes;
static bool note_accept;
static bool note_pending;
static bool expect_panic;
static bool fail_sync;
static uint32 reads;
static bool fail_open, fail_write, fail_close, corrupt_post_read;
static uint32 short_read_number;
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
	if (fail_open) {
		errno = ENOENT;
		return -1;
	}
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
	reads++;
	if (reads == short_read_number)
		return BLCKSZ - 1;
	memcpy(buffer, disk.data, length);
	if (corrupt_post_read && reads == 2)
		((char *)buffer)[BLCKSZ - 1] ^= 1;
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
	if (fail_write)
		return -1;
	writes++;
	memcpy(disk.data + offset, buffer, length);
	return length;
}

static int
fixture_sync(int fd)
{
	UT_ASSERT_EQ(fd, 42);
	UT_ASSERT(writes <= 1);
	UT_ASSERT_EQ(closes, 0);
	syncs++;
	if (fail_sync)
		return -1;
	memcpy(durable.data, disk.data, BLCKSZ);
	return 0;
}

static int fixture_sync(int fd);

/* Record the segment for the next checkpoint (own runtime undo) or fsync it
 * now (a path the checkpointer cannot resolve). */
bool
cluster_undo_smgr_header_unchanged_durable(ClusterUndoPathIntent intent pg_attribute_unused(),
										   uint32 segment_id, uint8 owner_instance)
{
	UT_ASSERT_EQ(segment_id, 513);
	UT_ASSERT_EQ(owner_instance, 3);
	notes++;
	if (note_accept) {
		note_pending = true;
		return true;
	}
	return fixture_sync(42) == 0;
}

static void
fixture_checkpoint(void)
{
	if (note_pending)
		memcpy(durable.data, disk.data, BLCKSZ);
	note_pending = false;
}

static void
fixture_power_loss(void)
{
	memcpy(disk.data, durable.data, BLCKSZ);
}

static int
fixture_close(int fd)
{
	UT_ASSERT_EQ(fd, 42);
	closes++;
	return fail_close ? -1 : 0;
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
#define ereport(level_, rest_)                                                                     \
	do {                                                                                           \
		if (expect_panic)                                                                          \
			longjmp(panic_jump, 1);                                                                \
		abort();                                                                                   \
	} while (0)
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
static const UBA InvalidUbaVal = InvalidUba_init; /* the legacy stamp's head reset */
#include "test_cluster_undo_bind_native.inc"
#include "test_cluster_undo_tt_v2_native.inc"

/* Other legacy terminal/data-block owners are outside this fixture. An
 * accidental dispatch to one is a failure, never a successful substitute. */
#define RecoveryInProgress() true
#define InRecovery true
#define cluster_undo_redo_tt_slot_commit(decoded_) abort()
#define cluster_undo_redo_tt_slot_abort(decoded_) abort()
#define cluster_undo_redo_tt_slot_set_head(decoded_) abort()
#define cluster_undo_redo_tt_ctrc_release(decoded_) abort()
#define cluster_undo_redo_block_write(decoded_, payload_, lsn_) abort()
#define cluster_hw_redo_reserve(record_) abort()
#include "test_cluster_undo_dispatch_native.inc"

static void
reset_io(void)
{
	opens = writes = syncs = closes = extends = mkdirs = dirsyncs = applies = skips = 0;
	notes = 0;
	note_pending = false;
	fail_sync = false;
	reads = 0;
	fail_open = fail_write = fail_close = corrupt_post_read = false;
	short_read_number = 0;
}

static void
apply_record(uint8 opcode, void *data, uint32 length)
{
	XLogReaderState reader = { 0 };
	DecodedXLogRecord record = { 0 };
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
	ClusterUndoDecoded decoded = { 0 };
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
		ClusterUndoDecoded decoded = { 0 };
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
		if (fault == 0)
			header->owner_instance = 2;
		if (fault == 1)
			header->segment_id++;
		if (fault == 2)
			header->wrap_count--;
		if (fault == 3)
			header->tt_slots[4].commit_scn = 52;
		if (fault == 4)
			decoded.opcode = XLOG_UNDO_TT_SLOT_COMMIT;
		if (fault == 5)
			decoded.has_payload = true;
		if (fault == 6)
			((PageHeader)base.data)->pd_pagesize_version = 0;
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
		if (fault == 1)
			header->pd_pagesize_version = 0;
		if (fault == 2)
			header->segment_size_bytes--;
		if (fault == 3)
			header->pd_flags &= ~PD_UNDO_SEG_HEADER;
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
			/* Present state is made durable, not rewritten (F-D-29 residue). */
			UT_ASSERT_EQ(writes + applies + skips, 0);
			UT_ASSERT_EQ(notes, 1);
			UT_ASSERT_EQ(syncs, 1);
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
	UT_ASSERT_EQ(writes + applies, 0);
	UT_ASSERT_EQ(notes, 1);
	UT_ASSERT_EQ(syncs, 1);
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
 * skips without rewriting (it only makes the present bytes durable); another
 * xid at that wrap still PANICs. */
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
		UT_ASSERT_EQ(writes + applies, 0);
		UT_ASSERT_EQ(notes, 1);
		UT_ASSERT_EQ(syncs, 1);
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

/*
 * F-D-29 review P1: after a process crash the page cache may still hold a TT
 * stamp that was never fsynced, while shared memory -- and its checkpoint
 * record of unsynced header writes -- was rebuilt.  A replayed record whose
 * effect block zero already shows must therefore be made durable, or
 * recorded for the next checkpoint, like a fresh write; otherwise that
 * checkpoint moves past the record and a power loss drops the stamp.
 */
UT_TEST(test_native_replay_of_present_tt_state_survives_power_loss)
{
	/* Replay kinds: exact commit (ALREADY), bind (ALREADY), legacy commit over
	 * a newer wrap (SKIP), exact commit into a newer generation (SKIP_STALE). */
	enum { EXACT_ALREADY, BIND_ALREADY, LEGACY_SKIP, EXACT_STALE, NKINDS };

	for (int variant = 0; variant < NKINDS * 2; variant++) {
		int kind = variant / 2;
		UndoSegmentHeaderData *header = (UndoSegmentHeaderData *)disk.data;
		TTSlot *slot = &header->tt_slots[4];

		cluster_undo_segment_make_header_bytes(513, 3, disk.data);
		header->wrap_count = 9;
		slot->xid = 701;
		slot->wrap = 2;
		slot->flags = TT_FLAGS_RESERVED;
		slot->first_undo_block = (UBA)InvalidUba_init;
		slot->status = kind == BIND_ALREADY ? TT_SLOT_UNUSED : TT_SLOT_ACTIVE;
		if (kind == BIND_ALREADY)
			memset(slot, 0, sizeof(*slot));
		durable = disk; /* last checkpoint: before the record */
		/* The crashed process left the record's effect -- or a newer state --
		 * in the page cache only. */
		slot->xid = 701;
		slot->wrap = kind == LEGACY_SKIP ? 3 : 2;
		slot->flags = TT_FLAGS_RESERVED;
		slot->first_undo_block = (UBA)InvalidUba_init;
		slot->status = kind == BIND_ALREADY ? TT_SLOT_ACTIVE : TT_SLOT_COMMITTED;
		slot->commit_scn = kind == BIND_ALREADY ? InvalidScn : 804;
		if (kind == EXACT_STALE)
			header->wrap_count = 10;
		reset_io();
		note_accept = variant % 2 == 0; /* own runtime undo vs recovery scope */
		expect_panic = false;
		if (setjmp(panic_jump) == 0) {
			if (kind == BIND_ALREADY)
				cluster_tt_durable_redo_bind_slot(3, 513, 9, 4, 2, 701);
			else if (kind == LEGACY_SKIP)
				cluster_tt_durable_redo_stamp_slot(3, 513, 4, 2, 701, 804);
			else
				cluster_tt_durable_redo_stamp_slot_exact(3, 513, 9, 4, 2, 701, 804);
		} else
			UT_ASSERT(false);
		UT_ASSERT_EQ(writes, 0);
		UT_ASSERT_EQ(notes, 1);
		UT_ASSERT_EQ(syncs, note_accept ? 0 : 1);
		fixture_checkpoint();
		fixture_power_loss();
		UT_ASSERT_EQ(slot->status, kind == BIND_ALREADY ? TT_SLOT_ACTIVE : TT_SLOT_COMMITTED);
		UT_ASSERT_EQ(slot->xid, 701);
		UT_ASSERT_EQ(slot->wrap, kind == LEGACY_SKIP ? 3 : 2);
		UT_ASSERT_EQ(header->wrap_count, kind == EXACT_STALE ? 10 : 9);
		if (ut_current_failed)
			printf("# replay durability variant %d\n", variant);
	}
	note_accept = false;

	/* A failed sync of the present state PANICs; replay never skips past it. */
	reset_io();
	fail_sync = expect_panic = true;
	if (setjmp(panic_jump) == 0) {
		cluster_tt_durable_redo_stamp_slot_exact(3, 513, 9, 4, 2, 701, 804);
		UT_ASSERT(false);
	}
	fail_sync = expect_panic = false;
	UT_ASSERT_EQ(writes, 0);
	UT_ASSERT_EQ(notes, 1);
}

UT_TEST(test_private_abort_head_release_and_exact_stale_results)
{
	for (int exact = 0; exact < 2; exact++) {
		ClusterUndoDecoded decoded = { 0 };
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
	} wal = { 0 };
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
	} wal = { 0 };
	PGAlignedBlock image, before;
	xl_undo_segment_recycle recycle = { 0 };

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
	} wal = { 0 };
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
	ClusterUndoDecoded decoded = { 0 };
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
		result = cluster_undo_prepare_header_v1(&decoded, (const uint8 *)image.data, BLCKSZ,
												base.data, output.data);
		UT_ASSERT_EQ(result, generation == 7	? CLUSTER_UNDO_HEADER_BLOCKED
							 : generation == 10 ? CLUSTER_UNDO_HEADER_SKIP_STALE
												: CLUSTER_UNDO_HEADER_APPLY);
		UT_ASSERT(memcmp(output.data,
						 generation == 8 || generation == 9 ? image.data : unchanged.data, BLCKSZ)
				  == 0);
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
	xl_undo_segment_recycle record = { 0 };

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
		if (fault == 1)
			header->pd_pagesize_version = 0;
		if (fault == 2)
			header->segment_size_bytes = 0;
		if (fault == 3)
			header->pd_flags &= ~PD_UNDO_SEG_HEADER;
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

UT_TEST(test_native_v2_bind_keeps_original_full_xid)
{
	xl_undo_tt_identity_v2 record = { 0 };
	XLogReaderState reader = { 0 };
	DecodedXLogRecord wal = { 0 };
	bool refused = false;
	UndoSegmentHeaderDataV2 *header = (UndoSegmentHeaderDataV2 *)disk.data;

	UT_ASSERT(cluster_undo_segment_make_header_v2_bytes(513, 3, disk.data));
	header->wrap_count = 9;
	header->pd_checksum = pg_checksum_page(disk.data, 0);
	durable = disk;
	record.segment_id = 513;
	record.segment_generation = 9;
	record.xid = 701;
	record.slot_offset = 4;
	record.wrap = 2;
	record.instance = 3;
	record.format_version = CLUSTER_UNDO_TT_IDENTITY_V2_VERSION;
	record.original_full_xid = (UINT64_C(7) << 32) | 701;
	wal.header.xl_rmid = RM_CLUSTER_UNDO_ID;
	wal.header.xl_info = XLOG_UNDO_TT_SLOT_BIND;
	wal.main_data = (char *)&record;
	wal.main_data_len = sizeof(record);
	reader.record = &wal;
	reset_io();
	expect_panic = true;
	if (setjmp(panic_jump) == 0)
		cluster_undo_redo(&reader);
	else
		refused = true;
	expect_panic = false;
	UT_ASSERT(!refused);
	UT_ASSERT_EQ(header->tt_slots[4].status, TT_SLOT_ACTIVE);
	UT_ASSERT_EQ(header->tt_slots[4].original_full_xid, record.original_full_xid);
	UT_ASSERT_EQ(writes, 1);
	UT_ASSERT_EQ(syncs, 1);
	UT_ASSERT(memcmp(disk.data, durable.data, BLCKSZ) == 0);
}

/* The actual rmgr entry is exercised above controlled file syscalls. */
static bool
replay_v2_record(uint8 opcode, void *payload, Size length)
{
	XLogReaderState reader = { 0 };
	DecodedXLogRecord wal = { 0 };
	bool accepted = true;

	wal.header.xl_rmid = RM_CLUSTER_UNDO_ID;
	wal.header.xl_info = opcode;
	wal.main_data = payload;
	wal.main_data_len = length;
	reader.record = &wal;
	expect_panic = true;
	if (setjmp(panic_jump) == 0)
		cluster_undo_redo(&reader);
	else
		accepted = false;
	expect_panic = false;
	return accepted;
}

static xl_undo_tt_identity_v2
native_v2_identity(uint64 full)
{
	xl_undo_tt_identity_v2 rec = { 0 };

	rec.segment_id = 513;
	rec.segment_generation = 9;
	rec.xid = (TransactionId)full;
	rec.slot_offset = 4;
	rec.wrap = 2;
	rec.instance = 3;
	rec.format_version = CLUSTER_UNDO_TT_IDENTITY_V2_VERSION;
	rec.original_full_xid = full;
	return rec;
}

static void
native_v2_base(uint64 full, uint8 status)
{
	UndoSegmentHeaderDataV2 *header = (UndoSegmentHeaderDataV2 *)disk.data;
	TTSlotV2 *slot = &header->tt_slots[4];

	UT_ASSERT(cluster_undo_segment_make_header_v2_bytes(513, 3, disk.data));
	header->wrap_count = 9;
	header->_reserved[7] = 0x53;
	if (status != TT_SLOT_UNUSED) {
		slot->xid = (TransactionId)full;
		slot->wrap = 2;
		slot->status = status;
		slot->original_full_xid = full;
		slot->commit_scn = status == TT_SLOT_COMMITTED ? 804 : InvalidScn;
	}
	header->pd_checksum = pg_checksum_page(disk.data, 0);
	durable = disk;
	reset_io();
}

static void
check_native_v2_page(PGAlignedBlock *expected)
{
	((PageHeader)expected->data)->pd_checksum = pg_checksum_page(expected->data, 0);
	UT_ASSERT(memcmp(disk.data, expected->data, BLCKSZ) == 0);
	UT_ASSERT(memcmp(durable.data, expected->data, BLCKSZ) == 0);
	UT_ASSERT_EQ(reads, 2);
	UT_ASSERT_EQ(writes, 1);
	UT_ASSERT_EQ(syncs, 1);
	UT_ASSERT_EQ(closes, 1);
	UT_ASSERT_EQ(applies, 1);
	UT_ASSERT_EQ(skips + extends + mkdirs + notes, 0);
}

UT_TEST(test_native_v2_all_terminal_deltas_keep_full_page_identity)
{
	for (uint32 epoch = 0; epoch <= 7; epoch += 7) {
		const uint64 full = ((uint64)epoch << 32) | 701;
		xl_undo_tt_slot_commit_v2 commit = { 0 };
		xl_undo_tt_slot_set_head_v2 head = { 0 };
		xl_undo_tt_slot_ctrc_release_v2 release = { 0 };
		uint8 bytes[CLUSTER_UNDO_TT_CTRC_RELEASE_V2_BYTES];
		xl_undo_tt_identity_v2 identity = native_v2_identity(full);
		PGAlignedBlock expected;
		TTSlotV2 *slot = &((UndoSegmentHeaderDataV2 *)expected.data)->tt_slots[4];

		native_v2_base(full, TT_SLOT_ACTIVE);
		commit.identity = identity;
		commit.commit_scn = 804;
		expected = disk;
		slot->status = TT_SLOT_COMMITTED;
		slot->commit_scn = 804;
		UT_ASSERT(replay_v2_record(XLOG_UNDO_TT_SLOT_COMMIT, &commit, sizeof(commit)));
		check_native_v2_page(&expected);

		release.release.segment_id = 513;
		release.release.segment_generation = 9;
		release.release.xid = 701;
		release.release.cluster_epoch = 3;
		release.release.root_id = 11;
		release.release.root_generation = 12;
		release.release.formation_epoch = 13;
		release.release.admission_record_generation = 14;
		release.release.seal_generation = 15;
		release.release.touched_nodes_low = 4;
		release.release.ack_set_digest[0] = 0x67;
		release.release.slot_offset = 4;
		release.release.slot_wrap = 2;
		release.release.owner_instance = 3;
		release.release.terminal_status = TT_SLOT_COMMITTED;
		release.release.format_version = CLUSTER_UNDO_TT_CTRC_RELEASE_V2_VERSION;
		release.release.flags = CLUSTER_UNDO_TT_CTRC_RELEASE_ALL_TOUCHED_ACKED;
		release.original_full_xid = full;
		UT_ASSERT(cluster_undo_tt_ctrc_release_v2_encode(&release, bytes));
		reset_io();
		expected = disk;
		slot->flags = TT_SLOT_FLAG_CTRC_RELEASE_PROVEN;
		UT_ASSERT(replay_v2_record(XLOG_UNDO_TT_SLOT_CTRC_RELEASE, bytes, sizeof(bytes)));
		check_native_v2_page(&expected);

		native_v2_base(full, TT_SLOT_ACTIVE);
		expected = disk;
		slot->status = TT_SLOT_ABORTED;
		UT_ASSERT(replay_v2_record(XLOG_UNDO_TT_SLOT_ABORT, &identity, sizeof(identity)));
		check_native_v2_page(&expected);
		reset_io();
		head.identity = identity;
		head.first_undo_block = uba_encode(513, 1, 4, 64);
		expected = disk;
		slot->first_undo_block = head.first_undo_block;
		UT_ASSERT(replay_v2_record(XLOG_UNDO_TT_SLOT_SET_HEAD, &head, sizeof(head)));
		check_native_v2_page(&expected);
	}
}

UT_TEST(test_native_v2_conflicts_refuse_before_write)
{
	const uint64 full = (UINT64_C(7) << 32) | 701;

	for (int fault = 0; fault < 12; fault++) {
		xl_undo_tt_slot_commit_v2 record = { 0 };
		UndoSegmentHeaderDataV2 *header = (UndoSegmentHeaderDataV2 *)disk.data;
		PGAlignedBlock before;

		native_v2_base(full, TT_SLOT_ACTIVE);
		record.identity = native_v2_identity(full);
		record.commit_scn = 804;
		switch (fault) {
		case 0: header->tt_layout_version = 0; break;
		case 1: header->tt_layout_version = 3; break;
		case 2: header->tt_slot_bytes = 32; break;
		case 3: header->tt_layout_reserved = 1; break;
		case 4: header->wrap_count = 8; break;
		case 5: header->tt_slots[4].original_full_xid = 701; break;
		case 6: header->tt_slots[4].xid = 702; break;
		case 7: memset(&header->tt_slots[4], 0, sizeof(TTSlotV2)); break;
		case 8: header->owner_instance = 2; break;
		case 9: header->tt_active_count = TT_SLOTS_PER_SEGMENT + 1; break;
		case 10: header->segment_size_bytes = 0; break;
		case 11: break; /* corrupted checksum, while framing is otherwise valid */
		}
		header->pd_checksum = pg_checksum_page(disk.data, 0);
		if (fault == 11)
			header->_reserved[8] ^= 1;
		before = disk;
		UT_ASSERT(!replay_v2_record(XLOG_UNDO_TT_SLOT_COMMIT, &record, sizeof(record)));
		UT_ASSERT_EQ(writes + syncs + applies + skips, 0);
		UT_ASSERT_EQ(closes, 1);
		UT_ASSERT(memcmp(disk.data, before.data, BLCKSZ) == 0);
	}
}

UT_TEST(test_native_v2_bad_record_refuses_before_file_access)
{
	const uint64 full = (UINT64_C(7) << 32) | 701;

	for (int fault = 0; fault < 12; fault++) {
		xl_undo_tt_identity_v2 record = native_v2_identity(full);
		Size length = sizeof(record);
		PGAlignedBlock before;

		native_v2_base(full, TT_SLOT_UNUSED);
		before = disk;
		switch (fault) {
		case 0: record.format_version = 3; break;
		case 1: record.flags = 1; break;
		case 2: record.reserved = 1; break;
		case 3: record.original_full_xid = 0; break;
		case 4: record.original_full_xid = UINT64_MAX; break;
		case 5: record.original_full_xid++; break;
		case 6: record.instance = 2; break;
		case 7: record.slot_offset = TT_SLOTS_PER_SEGMENT; break;
		case 8: record.wrap = TT_WRAP_INVALID; break;
		case 9: record.segment_generation = UINT32_MAX; break;
		case 10: record.xid = FrozenTransactionId; break;
		case 11: length--; break;
		}
		UT_ASSERT(!replay_v2_record(XLOG_UNDO_TT_SLOT_BIND, &record, length));
		UT_ASSERT_EQ(opens + reads + writes + syncs + closes + applies + skips, 0);
		UT_ASSERT(memcmp(disk.data, before.data, BLCKSZ) == 0);
	}
}

UT_TEST(test_native_legacy_record_cannot_use_tt40_slot_geometry)
{
	const uint64 full = (UINT64_C(7) << 32) | 701;
	xl_undo_tt_slot_bind bind = { .segment_id = 513, .segment_generation = 9,
		.xid = 701, .slot_offset = 4, .wrap = 2, .instance = 3,
		.format_version = CLUSTER_UNDO_TT_BIND_VERSION };
	xl_xact_tt_commit commit = { .segment_id = 513, .segment_generation = 9,
		.xid = 701, .slot_offset = 4, .wrap = 2, .instance = 3,
		.format_version = CLUSTER_XACT_TT_COMMIT_VERSION, .commit_scn = 804 };

	for (int folded = 0; folded < 2; folded++) {
		PGAlignedBlock before;
		bool refused = false;

		native_v2_base(full, TT_SLOT_ACTIVE);
		before = disk;
		if (folded) {
			expect_panic = true;
			if (setjmp(panic_jump) == 0)
				cluster_tt_durable_redo_xact_commit(&commit, 0);
			else
				refused = true;
			expect_panic = false;
		} else
			refused = !replay_v2_record(XLOG_UNDO_TT_SLOT_BIND, &bind, sizeof(bind));
		UT_ASSERT(refused);
		UT_ASSERT_EQ(writes + syncs + applies + skips, 0);
		UT_ASSERT_EQ(closes, 1);
		UT_ASSERT(memcmp(disk.data, before.data, BLCKSZ) == 0);
	}
}

UT_TEST(test_native_v2_present_or_physical_stale_is_durable_without_write)
{
	const uint64 full = (UINT64_C(7) << 32) | 701;
	xl_undo_tt_slot_commit_v2 record = { 0 };

	record.identity = native_v2_identity(full);
	record.commit_scn = 804;
	for (int state = 0; state < 3; state++) {
		UndoSegmentHeaderDataV2 *header = (UndoSegmentHeaderDataV2 *)disk.data;
		PGAlignedBlock expected;

		native_v2_base(full, TT_SLOT_COMMITTED);
		if (state == 1)
			header->wrap_count++;
		if (state == 2) {
			header->tt_slots[4].wrap++;
			header->tt_slots[4].original_full_xid = 701;
		}
		header->pd_checksum = pg_checksum_page(disk.data, 0);
		expected = disk;
		memset(durable.data, 0, BLCKSZ);
		UT_ASSERT(replay_v2_record(XLOG_UNDO_TT_SLOT_COMMIT, &record, sizeof(record)));
		UT_ASSERT_EQ(writes + applies + notes, 0);
		UT_ASSERT_EQ(syncs, 1);
		UT_ASSERT_EQ(reads, 2);
		UT_ASSERT_EQ(closes, 1);
		UT_ASSERT_EQ(skips, state == 0 ? 0 : 1);
		fixture_power_loss();
		UT_ASSERT(memcmp(disk.data, expected.data, BLCKSZ) == 0);
	}
}

UT_TEST(test_native_v2_io_failure_never_reports_completion)
{
	xl_undo_tt_identity_v2 record = native_v2_identity((UINT64_C(7) << 32) | 701);

	for (int fault = 0; fault < 7; fault++) {
		native_v2_base(record.original_full_xid, TT_SLOT_UNUSED);
		switch (fault) {
		case 0: fail_open = true; break;
		case 1: short_read_number = 1; break;
		case 2: fail_write = true; break;
		case 3: fail_sync = true; break;
		case 4: short_read_number = 2; break;
		case 5: corrupt_post_read = true; break;
		case 6: fail_close = true; break;
		}
		UT_ASSERT(!replay_v2_record(XLOG_UNDO_TT_SLOT_BIND, &record, sizeof(record)));
		UT_ASSERT_EQ(applies + skips, 0);
		UT_ASSERT_EQ(extends + mkdirs + notes, 0);
		UT_ASSERT_EQ(closes, fault == 0 ? 0 : 1);
		UT_ASSERT_EQ(writes, fault < 3 ? 0 : 1);
		if (fault == 3) {
			fixture_power_loss();
			UT_ASSERT_EQ(((UndoSegmentHeaderDataV2 *)disk.data)->tt_slots[4].status,
						 TT_SLOT_UNUSED);
		}
	}
}

UT_TEST(test_folded_v2_native_commit_uses_the_same_fullxid_owner)
{
	for (int epoch = 0; epoch <= 7; epoch += 7) {
		uint64 full = ((uint64)epoch << 32) | 701;
		xl_xact_tt_commit tt = { .segment_id = 513, .segment_generation = 9,
			.xid = 701, .slot_offset = 4, .wrap = 2, .instance = 3,
			.format_version = CLUSTER_XACT_TT_COMMIT_V2_VERSION, .commit_scn = 804 };
		PGAlignedBlock expected;
		TTSlotV2 *slot = &((UndoSegmentHeaderDataV2 *)expected.data)->tt_slots[4];

		native_v2_base(full, TT_SLOT_ACTIVE);
		expected = disk;
		slot->status = TT_SLOT_COMMITTED;
		slot->commit_scn = 804;
		cluster_tt_durable_redo_xact_commit(&tt, full);
		check_native_v2_page(&expected);
	}
}

UT_TEST(test_folded_native_dispatch_preserves_v1_and_refuses_bad_identity)
{
	xl_xact_tt_commit tt = { .segment_id = 513, .segment_generation = 9,
		.xid = 701, .slot_offset = 4, .wrap = 2, .instance = 3,
		.format_version = CLUSTER_XACT_TT_COMMIT_VERSION, .commit_scn = 804 };
	UndoSegmentHeaderData *header = (UndoSegmentHeaderData *)disk.data;

	cluster_undo_segment_make_header_bytes(513, 3, disk.data);
	header->wrap_count = 9;
	header->tt_slots[4].xid = 701;
	header->tt_slots[4].wrap = 2;
	header->tt_slots[4].status = TT_SLOT_ACTIVE;
	reset_io();
	cluster_tt_durable_redo_xact_commit(&tt, 0);
	UT_ASSERT_EQ(header->tt_slots[4].status, TT_SLOT_COMMITTED);
	UT_ASSERT_EQ(header->tt_slots[4].commit_scn, 804);
	UT_ASSERT_EQ(applies, 1);
	for (int fault = 0; fault < 4; fault++) {
		uint64 full = (UINT64_C(7) << 32) | 701;
		bool refused = false;

		tt.format_version = CLUSTER_XACT_TT_COMMIT_V2_VERSION;
		if (fault == 0) tt.format_version = CLUSTER_XACT_TT_COMMIT_VERSION;
		if (fault == 1) tt.format_version = 3;
		if (fault == 2) full++;
		if (fault == 3) full = UINT64_MAX;
		reset_io();
		expect_panic = true;
		if (setjmp(panic_jump) == 0)
			cluster_tt_durable_redo_xact_commit(&tt, full);
		else
			refused = true;
		expect_panic = false;
		UT_ASSERT(refused);
		UT_ASSERT_EQ(opens + writes + syncs + applies, 0);
	}
}


UT_TEST(test_native_v2_lifecycle_init_validates_sealed_fresh_template)
{
	struct { xl_cluster_undo_segment_init header; uint8 image[BLCKSZ]; } wal = { 0 };
	PGAlignedBlock image, before;

	wal.header.instance = 3;
	wal.header.segment_id = 513;
	for (int fault = 0; fault < 6; fault++) {
		UT_ASSERT(cluster_undo_segment_make_header_v2_bytes(513, 3, image.data));
		if (fault == 2) ((UndoSegmentHeaderDataV2 *)image.data)->tt_slot_bytes = 32;
		if (fault == 3) ((UndoSegmentHeaderDataV2 *)image.data)->tt_slots[4].original_full_xid = 701;
		if (fault == 4) ((UndoSegmentHeaderDataV2 *)image.data)->tt_layout_version = 3;
		((PageHeader)image.data)->pd_checksum = pg_checksum_page(image.data, 0);
		if (fault == 1) image.data[BLCKSZ - 1] ^= 1;
		if (fault == 5) ((PageHeader)image.data)->pd_checksum = 0;
		memcpy(wal.image, image.data, BLCKSZ);
		memset(disk.data, 0, BLCKSZ);
		before = disk;
		reset_io();
		UT_ASSERT_EQ(replay_v2_record(XLOG_UNDO_SEGMENT_INIT, &wal, sizeof(wal)), fault == 0);
		if (fault == 0) {
			UT_ASSERT(memcmp(disk.data, image.data, BLCKSZ) == 0);
			UT_ASSERT(memcmp(durable.data, image.data, BLCKSZ) == 0);
			UT_ASSERT_EQ(writes, 1);
			UT_ASSERT_EQ(applies, 1);
			UT_ASSERT_EQ(dirsyncs, 1);
		} else {
			UT_ASSERT_EQ(opens + writes + extends + syncs + applies, 0);
			UT_ASSERT(memcmp(disk.data, before.data, BLCKSZ) == 0);
		}
	}
}

UT_TEST(test_native_init_cannot_upgrade_or_downgrade_existing_layout)
{
	struct { xl_cluster_undo_segment_init header; uint8 image[BLCKSZ]; } wal = { 0 };
	PGAlignedBlock image, before;

	wal.header.instance = 3;
	wal.header.segment_id = 513;
	for (int v2 = 0; v2 < 2; v2++) {
		if (v2) {
			UT_ASSERT(cluster_undo_segment_make_header_v2_bytes(513, 3, image.data));
			cluster_undo_segment_make_header_bytes(513, 3, disk.data);
		} else {
			cluster_undo_segment_make_header_bytes(513, 3, image.data);
			UT_ASSERT(cluster_undo_segment_make_header_v2_bytes(513, 3, disk.data));
		}
		((PageHeader)image.data)->pd_checksum = pg_checksum_page(image.data, 0);
		((PageHeader)disk.data)->pd_checksum = pg_checksum_page(disk.data, 0);
		memcpy(wal.image, image.data, BLCKSZ);
		before = disk;
		reset_io();
		UT_ASSERT(!replay_v2_record(XLOG_UNDO_SEGMENT_INIT, &wal, sizeof(wal)));
		UT_ASSERT_EQ(writes + extends + syncs + applies, 0);
		UT_ASSERT(memcmp(disk.data, before.data, BLCKSZ) == 0);
	}
}

UT_TEST(test_native_v2_reuse_generation_and_format_are_exact)
{
	struct { xl_undo_segment_reuse header; uint8 image[BLCKSZ]; } wal = { 0 };
	PGAlignedBlock image, before;

	wal.header.instance = 3;
	wal.header.segment_id = 513;
	wal.header.old_generation = 8;
	wal.header.new_generation = 9;
	UT_ASSERT(cluster_undo_segment_make_header_v2_bytes(513, 3, image.data));
	((UndoSegmentHeaderDataV2 *)image.data)->wrap_count = 9;
	((PageHeader)image.data)->pd_checksum = pg_checksum_page(image.data, 0);
	memcpy(wal.image, image.data, BLCKSZ);
	for (int state = 0; state < 7; state++) {
		native_v2_base((UINT64_C(7) << 32) | 701, TT_SLOT_ABORTED);
		((UndoSegmentHeaderDataV2 *)disk.data)->wrap_count = state < 4 ? 7 + state : 8;
		if (state == 4) cluster_undo_segment_make_header_bytes(513, 3, disk.data);
		if (state == 5) ((UndoSegmentHeaderDataV2 *)disk.data)->tt_layout_version = 3;
		((PageHeader)disk.data)->pd_checksum = pg_checksum_page(disk.data, 0);
		if (state == 6) disk.data[BLCKSZ - 1] ^= 1;
		before = disk;
		reset_io();
		UT_ASSERT_EQ(replay_v2_record(XLOG_UNDO_SEGMENT_REUSE, &wal, sizeof(wal)),
					 state >= 1 && state <= 3);
		if (state == 1 || state == 2) {
			UT_ASSERT(memcmp(disk.data, image.data, BLCKSZ) == 0);
			UT_ASSERT(memcmp(durable.data, image.data, BLCKSZ) == 0);
			UT_ASSERT_EQ(writes, 1);
			UT_ASSERT_EQ(applies, 1);
		} else {
			UT_ASSERT(memcmp(disk.data, before.data, BLCKSZ) == 0);
			UT_ASSERT_EQ(writes + applies, 0);
			UT_ASSERT_EQ(skips, state == 3 ? 1 : 0);
		}
	}
}

UT_TEST(test_native_v2_recycle_keeps_all_full_xids_and_checks_readback)
{
	xl_undo_segment_recycle rec = { 0 };
	PGAlignedBlock expected, before;

	rec.instance = 3;
	rec.segment_id = 513;
	rec.expected_generation = 9;
	rec.old_state = SEGMENT_COMMITTED;
	rec.new_state = SEGMENT_RECYCLABLE;
	for (int fault = 0; fault < 3; fault++) {
		native_v2_base((UINT64_C(7) << 32) | 701, TT_SLOT_ABORTED);
		before = disk;
		expected = disk;
		((UndoSegmentHeaderDataV2 *)expected.data)->segment_state = SEGMENT_RECYCLABLE;
		((PageHeader)expected.data)->pd_checksum = pg_checksum_page(expected.data, 0);
		fail_sync = fault == 1;
		corrupt_post_read = fault == 2;
		UT_ASSERT_EQ(replay_v2_record(XLOG_UNDO_SEGMENT_RECYCLE, &rec, sizeof(rec)), fault == 0);
		UT_ASSERT_EQ(applies, fault == 0 ? 1 : 0);
		if (fault == 0) {
			UT_ASSERT(memcmp(disk.data, expected.data, BLCKSZ) == 0);
			UT_ASSERT(memcmp(durable.data, expected.data, BLCKSZ) == 0);
			UT_ASSERT_EQ(reads, 2);
			UT_ASSERT(memcmp(&((UndoSegmentHeaderDataV2 *)disk.data)->tt_slots,
							&((UndoSegmentHeaderDataV2 *)before.data)->tt_slots,
							sizeof(((UndoSegmentHeaderDataV2 *)disk.data)->tt_slots)) == 0);
		}
	}
}

/* Runtime ROLLBACK PREPARED can leave the complete ABORT+HEAD image on
 * disk before recovery starts at the earlier ABORT record. Both records must
 * replay in order without erasing its exact original transaction/head. */
UT_TEST(test_prepared_v2_abort_head_prefix_replay_preserves_original_head)
{
	for (uint32 epoch = 0; epoch <= 7; epoch += 7) {
		const uint64 full = ((uint64)epoch << 32) | 701;
		xl_undo_tt_identity_v2 identity = native_v2_identity(full);
		xl_undo_tt_slot_set_head_v2 head = { .identity = identity,
			.first_undo_block = uba_encode(513, 1, 4, 64) };
		PGAlignedBlock expected;

		native_v2_base(full, TT_SLOT_ACTIVE);
		UT_ASSERT(replay_v2_record(XLOG_UNDO_TT_SLOT_ABORT, &identity, sizeof(identity)));
		reset_io();
		UT_ASSERT(replay_v2_record(XLOG_UNDO_TT_SLOT_SET_HEAD, &head, sizeof(head)));
		expected = disk;
		for (unsigned repeat = 0; repeat < 2; repeat++) {
			reset_io();
			if (repeat == 0)
				UT_ASSERT(replay_v2_record(XLOG_UNDO_TT_SLOT_ABORT, &identity, sizeof(identity)));
			else
				UT_ASSERT(replay_v2_record(XLOG_UNDO_TT_SLOT_SET_HEAD, &head, sizeof(head)));
			UT_ASSERT_EQ(memcmp(&disk, &expected, BLCKSZ), 0);
			UT_ASSERT_EQ(memcmp(&durable, &expected, BLCKSZ), 0);
			UT_ASSERT_EQ(writes + applies, 0);
			UT_ASSERT_EQ(reads, 2);
			UT_ASSERT_EQ(syncs, 1);
			UT_ASSERT_EQ(closes, 1);
		}
		for (unsigned fault = 0; fault < 7; fault++) {
			UndoSegmentHeaderDataV2 *header = (UndoSegmentHeaderDataV2 *)disk.data;
			TTSlotV2 *slot = &header->tt_slots[4];
			disk = expected;
			switch (fault) {
			case 0: slot->first_undo_block = uba_encode(513, 1, 5, 64); break;
			case 1: slot->first_undo_block = uba_encode(514, 1, 4, 64); break;
			case 2: slot->first_undo_block = uba_encode(513, 0, 4, 64); break;
			case 3: slot->original_full_xid += UINT64_C(1) << 32; break;
			case 4: slot->status = TT_SLOT_COMMITTED; slot->commit_scn = 804; break;
			case 5: slot->flags = 0x80; break;
			case 6: slot->flags = TT_SLOT_FLAG_CTRC_RELEASE_PROVEN; break;
			}
			header->pd_checksum = pg_checksum_page(disk.data, 0);
			durable = disk;
			reset_io();
			UT_ASSERT(!replay_v2_record(XLOG_UNDO_TT_SLOT_ABORT, &identity, sizeof(identity)));
			UT_ASSERT_EQ(writes + applies, 0);
		}
	}
}

int
main(void)
{
	UT_PLAN(26);
	UT_RUN(test_native_exact_commit_rejects_bad_header_before_write);
	UT_RUN(test_private_abort_head_release_and_exact_stale_results);
	UT_RUN(test_native_bind_uses_complete_header_identity_and_durable_slot);
	UT_RUN(test_native_bind_replay_over_own_terminal_skips);
	UT_RUN(test_native_replay_of_present_tt_state_survives_power_loss);
	UT_RUN(test_private_tt_header_sequence_and_unrelated_bytes);
	UT_RUN(test_private_tt_header_refuses_identity_and_conflicting_predecessor);
	UT_RUN(test_native_init_validates_real_image_before_file_mutation);
	UT_RUN(test_native_reuse_image_generation_and_recycle_identity);
	UT_RUN(test_native_reuse_durability_and_stale_generation);
	UT_RUN(test_private_header_decision_preserves_native_generation_and_output);
	UT_RUN(test_native_recycle_fsync_failure_does_not_report_completion);
	UT_RUN(test_native_v2_bind_keeps_original_full_xid);
	UT_RUN(test_native_v2_all_terminal_deltas_keep_full_page_identity);
	UT_RUN(test_native_v2_conflicts_refuse_before_write);
	UT_RUN(test_native_v2_present_or_physical_stale_is_durable_without_write);
	UT_RUN(test_native_v2_io_failure_never_reports_completion);
	UT_RUN(test_folded_v2_native_commit_uses_the_same_fullxid_owner);
	UT_RUN(test_folded_native_dispatch_preserves_v1_and_refuses_bad_identity);
	UT_RUN(test_native_v2_bad_record_refuses_before_file_access);
	UT_RUN(test_native_legacy_record_cannot_use_tt40_slot_geometry);
	UT_RUN(test_native_v2_lifecycle_init_validates_sealed_fresh_template);
	UT_RUN(test_native_init_cannot_upgrade_or_downgrade_existing_layout);
	UT_RUN(test_native_v2_reuse_generation_and_format_are_exact);
	UT_RUN(test_native_v2_recycle_keeps_all_full_xids_and_checks_readback);
	UT_RUN(test_prepared_v2_abort_head_prefix_replay_preserves_original_head);
	UT_DONE();
	return ut_failed_count != 0;
}
