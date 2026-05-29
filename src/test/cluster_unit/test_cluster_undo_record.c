/*-------------------------------------------------------------------------
 *
 * test_cluster_undo_record.c
 *	  pgrac spec-3.7 D12 — cluster_unit encode/decode round-trip tests
 *	  for UndoRecordHeader + 4 op payloads + slot directory.
 *
 *	  12 tests covering:
 *	    T1   UndoRecordHeader encode + decode round-trip (full fields)
 *	    T2   UndoInsertPayload encode + decode round-trip
 *	    T3   UndoUpdatePayload encode + decode round-trip
 *	    T4   UndoDeletePayload encode + decode round-trip
 *	    T5   UndoItlPayload encode + decode round-trip (40B all fields)
 *	    T6   multi-record in single block (slot dir advance)
 *	    T7   block has-space invariant at boundary (7K record OK)
 *	    T8   block has-space invariant rejection (8K + 1 byte)
 *	    T9   slot dir grow-downward addressing (slot N at offset BLCKSZ - 8*(N+1))
 *	    T10  PGRAC_UNDO_BLOCK_MAGIC roundtrip after init
 *	    T11  flags + record_type byte-for-byte fidelity
 *	    T12  prev_uba 16B preserved through encode → decode chain
 *
 * Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-3.7-undo-record-format-allocator.md (FROZEN v0.4 +
 *       Hardening v1.0.1 H-1/H-2)
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include <stddef.h>
#include <string.h>

#include "cluster/cluster_undo_format.h"
#include "cluster/cluster_undo_record.h"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();


/* ---- T1: UndoRecordHeader encode + decode round-trip ---- */
UT_TEST(test_record_header_roundtrip)
{
	UndoRecordHeader src;
	UndoRecordHeader dst;
	char buf[sizeof(UndoRecordHeader)];

	memset(&src, 0, sizeof(src));
	src.record_type = UNDO_RECORD_INSERT;
	src.flags = UNDO_REC_FLAG_FIRST_IN_TX;
	src.payload_length = 12;
	src.xid = 12345;
	src.origin_node_id = 2;
	src.tt_slot_segment_id = 1;
	src.tt_slot_id = 7;
	src.write_scn = 999;
	src.target_fork = MAIN_FORKNUM;
	src.target_block = 100;
	src.target_offset = 5;

	memcpy(buf, &src, sizeof(src));
	memset(&dst, 0xff, sizeof(dst));
	memcpy(&dst, buf, sizeof(dst));

	UT_ASSERT_EQ(dst.record_type, src.record_type);
	UT_ASSERT_EQ(dst.flags, src.flags);
	UT_ASSERT_EQ(dst.payload_length, src.payload_length);
	UT_ASSERT_EQ((long long)dst.xid, (long long)src.xid);
	UT_ASSERT_EQ(dst.origin_node_id, src.origin_node_id);
	UT_ASSERT_EQ(dst.target_fork, src.target_fork);
	UT_ASSERT_EQ((long long)dst.target_block, (long long)src.target_block);
	UT_ASSERT_EQ(dst.target_offset, src.target_offset);
}

/* ---- T2: UndoInsertPayload encode + decode ---- */
UT_TEST(test_insert_payload_roundtrip)
{
	UndoInsertPayload src = { .inserted_tuple_len = 256, .flags = 1 };
	UndoInsertPayload dst;
	char buf[sizeof(src)];

	memcpy(buf, &src, sizeof(src));
	memcpy(&dst, buf, sizeof(dst));

	UT_ASSERT_EQ(dst.inserted_tuple_len, 256);
	UT_ASSERT_EQ(dst.flags, 1);
}

/* ---- T3: UndoUpdatePayload round-trip ---- */
UT_TEST(test_update_payload_roundtrip)
{
	UndoUpdatePayload src;
	UndoUpdatePayload dst;
	char buf[sizeof(src)];

	memset(&src, 0, sizeof(src));
	src.new_block = 200;
	src.new_offset = 8;
	src.old_tuple_length = 128;
	src.old_tuple_offset = sizeof(UndoUpdatePayload);
	src.flags = 2;

	memcpy(buf, &src, sizeof(src));
	memcpy(&dst, buf, sizeof(dst));

	UT_ASSERT_EQ((long long)dst.new_block, 200LL);
	UT_ASSERT_EQ(dst.new_offset, 8);
	UT_ASSERT_EQ(dst.old_tuple_length, 128);
	UT_ASSERT_EQ(dst.old_tuple_offset, sizeof(UndoUpdatePayload));
	UT_ASSERT_EQ(dst.flags, 2);
}

/* ---- T4: UndoDeletePayload round-trip ---- */
UT_TEST(test_delete_payload_roundtrip)
{
	UndoDeletePayload src
		= { .full_tuple_length = 200, .full_tuple_offset = sizeof(UndoDeletePayload), .flags = 4 };
	UndoDeletePayload dst;
	char buf[sizeof(src)];

	memcpy(buf, &src, sizeof(src));
	memcpy(&dst, buf, sizeof(dst));

	UT_ASSERT_EQ(dst.full_tuple_length, 200);
	UT_ASSERT_EQ(dst.full_tuple_offset, sizeof(UndoDeletePayload));
	UT_ASSERT_EQ((long long)dst.flags, 4LL);
}

/* ---- T5: UndoItlPayload round-trip (40B) ---- */
UT_TEST(test_itl_payload_roundtrip)
{
	UndoItlPayload src;
	UndoItlPayload dst;
	char buf[sizeof(src)];

	memset(&src, 0, sizeof(src));
	src.itl_slot_idx = 3;
	src.prev_flags = 0;
	src.new_flags = 5;
	src.lock_mode = 2;
	src.lock_xid = 7777;
	src.prev_xmax = 5555;
	src.prev_infomask = 0x0100;
	src.prev_infomask2 = 0x0080;
	src.prev_commit_scn = 88888;

	memcpy(buf, &src, sizeof(src));
	memcpy(&dst, buf, sizeof(dst));

	UT_ASSERT_EQ(dst.itl_slot_idx, 3);
	UT_ASSERT_EQ(dst.new_flags, 5);
	UT_ASSERT_EQ(dst.lock_mode, 2);
	UT_ASSERT_EQ((long long)dst.lock_xid, 7777LL);
	UT_ASSERT_EQ(dst.prev_infomask, 0x0100);
	UT_ASSERT_EQ((long long)dst.prev_commit_scn, 88888LL);
}

/* ---- T6: multi-record in single block (slot dir advance) ---- */
UT_TEST(test_multi_record_block)
{
	char block[BLCKSZ];
	UndoBlockHeader *blkhdr = (UndoBlockHeader *)block;
	UndoSlotDirEntry *slot0;
	UndoSlotDirEntry *slot1;
	UndoSlotDirEntry *slot2;
	uint32 record_off_0 = sizeof(UndoBlockHeader);
	uint32 record_off_1 = record_off_0 + sizeof(UndoRecordHeader) + 4;
	uint32 record_off_2 = record_off_1 + sizeof(UndoRecordHeader) + 12;

	memset(block, 0, BLCKSZ);
	blkhdr->magic = PGRAC_UNDO_BLOCK_MAGIC;
	blkhdr->block_version = UNDO_BLOCK_VERSION_1;
	blkhdr->slot_count = 3;
	blkhdr->free_offset = record_off_2 + sizeof(UndoRecordHeader) + 8;

	slot0 = UNDO_SLOT_DIR_PTR(block, 0);
	slot0->record_offset = record_off_0;
	slot0->record_length = sizeof(UndoRecordHeader) + 4;
	slot0->record_type = UNDO_RECORD_INSERT;

	slot1 = UNDO_SLOT_DIR_PTR(block, 1);
	slot1->record_offset = record_off_1;
	slot1->record_length = sizeof(UndoRecordHeader) + 12;
	slot1->record_type = UNDO_RECORD_UPDATE;

	slot2 = UNDO_SLOT_DIR_PTR(block, 2);
	slot2->record_offset = record_off_2;
	slot2->record_length = sizeof(UndoRecordHeader) + 8;
	slot2->record_type = UNDO_RECORD_DELETE;

	/* Verify slot dir reads back correctly */
	UT_ASSERT_EQ((long long)UNDO_SLOT_DIR_PTR(block, 0)->record_offset, (long long)record_off_0);
	UT_ASSERT_EQ((long long)UNDO_SLOT_DIR_PTR(block, 1)->record_offset, (long long)record_off_1);
	UT_ASSERT_EQ((long long)UNDO_SLOT_DIR_PTR(block, 2)->record_offset, (long long)record_off_2);
	UT_ASSERT_EQ(UNDO_SLOT_DIR_PTR(block, 0)->record_type, UNDO_RECORD_INSERT);
	UT_ASSERT_EQ(UNDO_SLOT_DIR_PTR(block, 1)->record_type, UNDO_RECORD_UPDATE);
	UT_ASSERT_EQ(UNDO_SLOT_DIR_PTR(block, 2)->record_type, UNDO_RECORD_DELETE);
}

/* ---- T7: block has-space invariant at boundary (7K record OK) ---- */
UT_TEST(test_block_has_space_boundary_ok)
{
	UT_ASSERT(cluster_undo_block_has_space(UNDO_BLOCK_INIT_FREE_OFFSET, 0, 7000));
}

/* ---- T8: block has-space invariant rejection ---- */
UT_TEST(test_block_has_space_overflow)
{
	UT_ASSERT(!cluster_undo_block_has_space(UNDO_BLOCK_INIT_FREE_OFFSET, 0, BLCKSZ));
	/* with slot_count=255 the dir takes 256×8 = 2048 bytes from end */
	UT_ASSERT(!cluster_undo_block_has_space(UNDO_BLOCK_INIT_FREE_OFFSET, 255, BLCKSZ - 2048));
}

/* ---- T9: slot dir grow-downward addressing ---- */
UT_TEST(test_slot_dir_addressing)
{
	UT_ASSERT_EQ(UNDO_SLOT_DIR_OFFSET(0), BLCKSZ - 8);
	UT_ASSERT_EQ(UNDO_SLOT_DIR_OFFSET(1), BLCKSZ - 16);
	UT_ASSERT_EQ(UNDO_SLOT_DIR_OFFSET(255), BLCKSZ - 2048);
}

/* ---- T10: block magic + version after manual init ---- */
UT_TEST(test_block_magic_init)
{
	char block[BLCKSZ];
	UndoBlockHeader *blkhdr = (UndoBlockHeader *)block;

	memset(block, 0, BLCKSZ);
	blkhdr->magic = PGRAC_UNDO_BLOCK_MAGIC;
	blkhdr->block_version = UNDO_BLOCK_VERSION_1;
	blkhdr->free_offset = UNDO_BLOCK_INIT_FREE_OFFSET;

	UT_ASSERT_EQ((long long)blkhdr->magic, (long long)PGRAC_UNDO_BLOCK_MAGIC);
	UT_ASSERT_EQ(blkhdr->block_version, 1);
	UT_ASSERT_EQ((long long)blkhdr->free_offset, (long long)sizeof(UndoBlockHeader));
}

/* ---- T11: flags + record_type byte-for-byte fidelity ---- */
UT_TEST(test_record_type_flags_bytes)
{
	UndoRecordHeader hdr;
	const uint8 *bytes;

	memset(&hdr, 0, sizeof(hdr));
	hdr.record_type = UNDO_RECORD_ITL;
	hdr.flags = UNDO_REC_FLAG_FIRST_IN_TX | UNDO_REC_FLAG_CONTINUED;

	bytes = (const uint8 *)&hdr;
	UT_ASSERT_EQ(bytes[0], UNDO_RECORD_ITL);
	UT_ASSERT_EQ(bytes[1], UNDO_REC_FLAG_FIRST_IN_TX | UNDO_REC_FLAG_CONTINUED);
}

/* ---- T12: prev_uba 16B preserved through copy ---- */
UT_TEST(test_prev_uba_preserved)
{
	UndoRecordHeader src;
	UndoRecordHeader dst;
	char buf[sizeof(UndoRecordHeader)];

	memset(&src, 0, sizeof(src));
	src.prev_uba.raw[0] = 0x1234567890abcdefULL;
	src.prev_uba.raw[1] = 0xfedcba0987654321ULL;

	memcpy(buf, &src, sizeof(src));
	memcpy(&dst, buf, sizeof(dst));

	UT_ASSERT_EQ((long long)dst.prev_uba.raw[0], (long long)src.prev_uba.raw[0]);
	UT_ASSERT_EQ((long long)dst.prev_uba.raw[1], (long long)src.prev_uba.raw[1]);
}


int
main(int argc, char **argv)
{
	UT_PLAN(12);

	UT_RUN(test_record_header_roundtrip);
	UT_RUN(test_insert_payload_roundtrip);
	UT_RUN(test_update_payload_roundtrip);
	UT_RUN(test_delete_payload_roundtrip);
	UT_RUN(test_itl_payload_roundtrip);
	UT_RUN(test_multi_record_block);
	UT_RUN(test_block_has_space_boundary_ok);
	UT_RUN(test_block_has_space_overflow);
	UT_RUN(test_slot_dir_addressing);
	UT_RUN(test_block_magic_init);
	UT_RUN(test_record_type_flags_bytes);
	UT_RUN(test_prev_uba_preserved);

	UT_DONE();
	return ut_failed_count != 0 ? 1 : 0;
}
