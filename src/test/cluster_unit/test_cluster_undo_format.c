/*-------------------------------------------------------------------------
 *
 * test_cluster_undo_format.c
 *	  pgrac spec-3.7 D12 — cluster_unit ABI / sizeof / offsetof /
 *	  magic / enum / SQLSTATE tests for Undo Record Format.
 *
 *	  12 tests covering:
 *	    T1   UndoBlockHeader sizeof = 40 (HC211 Hardening v1.0.1) +
 *	         offsetof first_change_scn = 16, first_change_lsn = 24, crc64 = 32
 *	    T2   UndoSlotDirEntry sizeof = 8 (HC212)
 *	    T3   UndoRecordHeader sizeof = 64 (HC213) + offsetof prev_uba = 24,
 *	         target_locator = 40, target_block = 56, target_offset = 60
 *	    T4   RelFileLocator + ForkNumber + BlockNumber + OffsetNumber + pad sum = 24 (HC213a)
 *	    T5   UndoInsertPayload sizeof = 4 (HC214)
 *	    T6   UndoUpdatePayload sizeof = 12 (HC215 Hardening v1.0.1 H-2)
 *	    T7   UndoDeletePayload sizeof = 8 (HC216)
 *	    T8   UndoItlPayload sizeof = 40 (HC217)
 *	    T9   PGRAC_UNDO_BLOCK_MAGIC = 0x55444F31 ("UDO1")
 *	    T10  UndoRecordType enum INVALID=0 / INSERT=1 / UPDATE=2 / DELETE=3 / ITL=4
 *	    T11  53R9D SQLSTATE encode = MAKE_SQLSTATE('5','3','R','9','D')
 *	    T12  block has-space helper: 7K record OK, 8K record fail
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
#include "utils/errcodes.h"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();


/* ---- T1: UndoBlockHeader 40B + key offsetof ---- */
UT_TEST(test_undo_block_header)
{
	UT_ASSERT_EQ(sizeof(UndoBlockHeader), 40);
	UT_ASSERT_EQ(offsetof(UndoBlockHeader, magic), 0);
	UT_ASSERT_EQ(offsetof(UndoBlockHeader, block_version), 4);
	UT_ASSERT_EQ(offsetof(UndoBlockHeader, slot_count), 6);
	UT_ASSERT_EQ(offsetof(UndoBlockHeader, free_offset), 8);
	UT_ASSERT_EQ(offsetof(UndoBlockHeader, first_change_scn), 16);
	UT_ASSERT_EQ(offsetof(UndoBlockHeader, first_change_lsn), 24);
	UT_ASSERT_EQ(offsetof(UndoBlockHeader, crc64), 32);
}

/* ---- T2: UndoSlotDirEntry 8B ---- */
UT_TEST(test_undo_slot_dir_entry)
{
	UT_ASSERT_EQ(sizeof(UndoSlotDirEntry), 8);
	UT_ASSERT_EQ(offsetof(UndoSlotDirEntry, record_offset), 0);
	UT_ASSERT_EQ(offsetof(UndoSlotDirEntry, record_length), 4);
	UT_ASSERT_EQ(offsetof(UndoSlotDirEntry, record_type), 6);
	UT_ASSERT_EQ(offsetof(UndoSlotDirEntry, flags), 7);
}

/* ---- T3: UndoRecordHeader 64B + key offsetof ---- */
UT_TEST(test_undo_record_header)
{
	UT_ASSERT_EQ(sizeof(UndoRecordHeader), 64);
	UT_ASSERT_EQ(offsetof(UndoRecordHeader, record_type), 0);
	UT_ASSERT_EQ(offsetof(UndoRecordHeader, flags), 1);
	UT_ASSERT_EQ(offsetof(UndoRecordHeader, payload_length), 2);
	UT_ASSERT_EQ(offsetof(UndoRecordHeader, xid), 4);
	UT_ASSERT_EQ(offsetof(UndoRecordHeader, write_scn), 16);
	UT_ASSERT_EQ(offsetof(UndoRecordHeader, prev_uba), 24);
	UT_ASSERT_EQ(offsetof(UndoRecordHeader, target_locator), 40);
	UT_ASSERT_EQ(offsetof(UndoRecordHeader, target_fork), 52);
	UT_ASSERT_EQ(offsetof(UndoRecordHeader, target_block), 56);
	UT_ASSERT_EQ(offsetof(UndoRecordHeader, target_offset), 60);
}

/* ---- T4: target locator arithmetic 24B ---- */
UT_TEST(test_undo_record_target)
{
	/* RelFileLocator(12) + ForkNumber(4) + BlockNumber(4) + OffsetNumber(2) + pad(2) = 24 */
	UT_ASSERT_EQ((int) (sizeof(RelFileLocator) + sizeof(ForkNumber)
						+ sizeof(BlockNumber) + sizeof(OffsetNumber) + 2),
				 24);
}

/* ---- T5: UndoInsertPayload 4B ---- */
UT_TEST(test_undo_insert_payload)
{
	UT_ASSERT_EQ(sizeof(UndoInsertPayload), 4);
	UT_ASSERT_EQ(offsetof(UndoInsertPayload, inserted_tuple_len), 0);
	UT_ASSERT_EQ(offsetof(UndoInsertPayload, flags), 2);
}

/* ---- T6: UndoUpdatePayload 12B (Hardening v1.0.1 H-2 dropped _pad16) ---- */
UT_TEST(test_undo_update_payload)
{
	UT_ASSERT_EQ(sizeof(UndoUpdatePayload), 12);
	UT_ASSERT_EQ(offsetof(UndoUpdatePayload, new_block), 0);
	UT_ASSERT_EQ(offsetof(UndoUpdatePayload, new_offset), 4);
	UT_ASSERT_EQ(offsetof(UndoUpdatePayload, old_tuple_length), 6);
	UT_ASSERT_EQ(offsetof(UndoUpdatePayload, old_tuple_offset), 8);
	UT_ASSERT_EQ(offsetof(UndoUpdatePayload, flags), 10);
}

/* ---- T7: UndoDeletePayload 8B ---- */
UT_TEST(test_undo_delete_payload)
{
	UT_ASSERT_EQ(sizeof(UndoDeletePayload), 8);
	UT_ASSERT_EQ(offsetof(UndoDeletePayload, full_tuple_length), 0);
	UT_ASSERT_EQ(offsetof(UndoDeletePayload, full_tuple_offset), 2);
	UT_ASSERT_EQ(offsetof(UndoDeletePayload, flags), 4);
}

/* ---- T8: UndoItlPayload 40B ---- */
UT_TEST(test_undo_itl_payload)
{
	UT_ASSERT_EQ(sizeof(UndoItlPayload), 40);
	UT_ASSERT_EQ(offsetof(UndoItlPayload, itl_slot_idx), 0);
	UT_ASSERT_EQ(offsetof(UndoItlPayload, lock_xid), 4);
	UT_ASSERT_EQ(offsetof(UndoItlPayload, prev_xmax), 8);
	UT_ASSERT_EQ(offsetof(UndoItlPayload, prev_commit_scn), 16);
	UT_ASSERT_EQ(offsetof(UndoItlPayload, prev_undo_segment_head), 24);
}

/* ---- T9: magic constant ---- */
UT_TEST(test_undo_block_magic)
{
	UT_ASSERT_EQ((long long) PGRAC_UNDO_BLOCK_MAGIC, (long long) 0x55444F31U);
	UT_ASSERT_EQ(UNDO_BLOCK_VERSION_1, 1);
}

/* ---- T10: UndoRecordType enum ---- */
UT_TEST(test_undo_record_type)
{
	UT_ASSERT_EQ(UNDO_RECORD_INVALID, 0);
	UT_ASSERT_EQ(UNDO_RECORD_INSERT, 1);
	UT_ASSERT_EQ(UNDO_RECORD_UPDATE, 2);
	UT_ASSERT_EQ(UNDO_RECORD_DELETE, 3);
	UT_ASSERT_EQ(UNDO_RECORD_ITL, 4);
}

/* ---- T11: 53R9D SQLSTATE ---- */
UT_TEST(test_sqlstate_53R9D)
{
	UT_ASSERT_EQ(ERRCODE_CLUSTER_UNDO_RECORD_INVALID_UBA,
				 MAKE_SQLSTATE('5','3','R','9','D'));
}

/* ---- T12: cluster_undo_block_has_space helper ---- */
UT_TEST(test_block_has_space)
{
	/* Empty block: free_offset = 40 (UndoBlockHeader), slot_count = 0.
	 * Block size 8192;  slot dir grows from end (BLCKSZ - 8 per slot).
	 * 7K record OK:  40 + 7168 = 7208 ≤ 8192 - 8 = 8184. */
	UT_ASSERT(cluster_undo_block_has_space(40, 0, 7168));

	/* 8K record fail: 40 + 8192 > 8184. */
	UT_ASSERT(!cluster_undo_block_has_space(40, 0, 8192));

	/* Half-full + small record OK. */
	UT_ASSERT(cluster_undo_block_has_space(4096, 100, 100));
}


int
main(int argc, char **argv)
{
	UT_PLAN(12);

	UT_RUN(test_undo_block_header);
	UT_RUN(test_undo_slot_dir_entry);
	UT_RUN(test_undo_record_header);
	UT_RUN(test_undo_record_target);
	UT_RUN(test_undo_insert_payload);
	UT_RUN(test_undo_update_payload);
	UT_RUN(test_undo_delete_payload);
	UT_RUN(test_undo_itl_payload);
	UT_RUN(test_undo_block_magic);
	UT_RUN(test_undo_record_type);
	UT_RUN(test_sqlstate_53R9D);
	UT_RUN(test_block_has_space);

	UT_DONE();
	return ut_failed_count != 0 ? 1 : 0;
}
