/*-------------------------------------------------------------------------
 *
 * test_cluster_uba.c
 *	  pgrac spec-3.4b D10 — cluster_unit static + ABI tests for the
 *	  UBA encode/decode helpers (cluster_uba.h) and the matching
 *	  TT-slot offset/id sentinel separation (cluster_tt_slot.h).
 *
 *	  Pure compile-time + inline-helper exercise; no PG backend / shmem
 *	  required.  All UBA helpers (uba_encode / uba_decode / uba_get_*)
 *	  live inline in cluster_uba.h; the two-tier uba_origin_node_id is
 *	  in cluster_uba.c and is exercised via direct call.
 *
 *	  Tests:
 *	    T1   sizeof(UBA) == 16 (spec-1.5 invariant; reaffirmed by 3.4b D1)
 *	    T2   InvalidUba is all-zero
 *	    T3   uba_decode(InvalidUba) returns false
 *	    T4   uba_encode/decode round-trip (segment_id=1, offset=0)
 *	    T5   uba_encode/decode round-trip (segment_id=UINT16_MAX, offset=47)
 *	    T6   uba_encode/decode round-trip mid-range values
 *	    T7   uba_decode rejects segment_id == 0
 *	    T8   uba_decode rejects segment_id > UINT16_MAX
 *	    T9   uba_decode rejects tt_slot_offset >= TT_SLOTS_PER_SEGMENT
 *	    T10  uba_decode rejects non-zero reserved bits
 *	    T11  uba_get_segment_id extracts the low 32 bits of raw[0]
 *	    T12  uba_get_tt_slot_offset extracts the low 16 bits of raw[1]
 *	    T13  offset_to_id maps 0 → 1, 47 → 48
 *	    T14  id_to_offset maps 1 → 0, 48 → 47
 *	    T15  offset_to_id / id_to_offset are mutual inverses across the range
 *	    T16  uba_origin_node_id(InvalidUba) returns InvalidNodeId
 *	    T17  uba_origin_node_id derives node 0 from segment_id 1
 *	    T18  uba_origin_node_id derives node 1 from segment_id 257
 *	    T19  uba_origin_node_id rejects out-of-range derived node
 *	    T20  CLUSTER_UNDO_SEGS_PER_INSTANCE is 256 (F4 segment_id range invariant)
 *	    T21  CLUSTER_UNDO_OWNER_INVALID is 0 (rename of DEFAULT_OWNER)
 *	    T22  CLUSTER_ITL_DELTA_FORMAT_V1 is 0 (legacy backward-compat sentinel)
 *	    T23  CLUSTER_ITL_DELTA_FORMAT_V2 is 1 (new code path)
 *	    T24  INVALID_TT_SLOT_OFFSET is 0xFFFF (out of valid [0, 48) range)
 *
 *	  Spec: spec-3.4b-real-tt-allocator-uba-encoding-production-cross-node.md
 *	        (v0.3 FROZEN 2026-05-24)
 *
 * Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_uba.c
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include <stddef.h>
#include <string.h>

#include "access/heapam_xlog.h"			/* CLUSTER_ITL_DELTA_FORMAT_V[12] */
#include "cluster/cluster_scn.h"
#include "cluster/cluster_tt_slot.h"
#include "cluster/cluster_uba.h"
#include "cluster/storage/cluster_undo_alloc.h"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"


UT_DEFINE_GLOBALS();


void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}


/* ---------- T1-T2: ABI shape ---------- */

UT_TEST(test_t1_uba_sizeof_16)
{
	UT_ASSERT_EQ((int) sizeof(UBA), 16);
}

UT_TEST(test_t2_invalid_uba_all_zero)
{
	UBA u = InvalidUba_init;

	UT_ASSERT_EQ((int) (u.raw[0] == 0), 1);
	UT_ASSERT_EQ((int) (u.raw[1] == 0), 1);
	UT_ASSERT_EQ((int) UBA_is_invalid(u), 1);
}


/* ---------- T3-T10: uba_decode validation ---------- */

UT_TEST(test_t3_decode_invalid_uba_returns_false)
{
	UBA u = InvalidUba_init;
	uint32 seg, blk;
	uint16 off, row;

	UT_ASSERT_EQ((int) uba_decode(u, &seg, &blk, &off, &row), 0);
}

UT_TEST(test_t4_roundtrip_low_values)
{
	UBA u = uba_encode(1, 0, 0, 0);
	uint32 seg, blk;
	uint16 off, row;

	UT_ASSERT_EQ((int) uba_decode(u, &seg, &blk, &off, &row), 1);
	UT_ASSERT_EQ((int) seg, 1);
	UT_ASSERT_EQ((int) blk, 0);
	UT_ASSERT_EQ((int) off, 0);
	UT_ASSERT_EQ((int) row, 0);
}

UT_TEST(test_t5_roundtrip_high_values)
{
	UBA u = uba_encode(UINT16_MAX, 0xFFFFFFFFu, TT_SLOTS_PER_SEGMENT - 1, 0xFFFFu);
	uint32 seg, blk;
	uint16 off, row;

	UT_ASSERT_EQ((int) uba_decode(u, &seg, &blk, &off, &row), 1);
	UT_ASSERT_EQ((int) seg, (int) UINT16_MAX);
	UT_ASSERT_EQ((int) blk, (int) 0xFFFFFFFFu);
	UT_ASSERT_EQ((int) off, TT_SLOTS_PER_SEGMENT - 1);
	UT_ASSERT_EQ((int) row, (int) 0xFFFFu);
}

UT_TEST(test_t6_roundtrip_mid_range)
{
	UBA u = uba_encode(257, 12345, 3, 99);
	uint32 seg, blk;
	uint16 off, row;

	UT_ASSERT_EQ((int) uba_decode(u, &seg, &blk, &off, &row), 1);
	UT_ASSERT_EQ((int) seg, 257);
	UT_ASSERT_EQ((int) blk, 12345);
	UT_ASSERT_EQ((int) off, 3);
	UT_ASSERT_EQ((int) row, 99);
}

UT_TEST(test_t7_decode_rejects_segment_zero)
{
	/*
	 * Construct a UBA with segment_id == 0 manually (real encoder asserts
	 * against this).  Decoder must reject.
	 */
	UBA u;
	uint32 seg, blk;
	uint16 off, row;

	u.raw[0] = (uint64) 12345 << 32;	/* block_no=12345, segment_id=0 */
	u.raw[1] = 1;						/* slot_offset=1, row_offset=0 */
	UT_ASSERT_EQ((int) uba_decode(u, &seg, &blk, &off, &row), 0);
}

UT_TEST(test_t8_decode_rejects_segment_over_uint16max)
{
	UBA u;
	uint32 seg, blk;
	uint16 off, row;

	u.raw[0] = (uint64) 0x10000ULL;		/* segment_id = 65536 */
	u.raw[1] = 1;
	UT_ASSERT_EQ((int) uba_decode(u, &seg, &blk, &off, &row), 0);
}

UT_TEST(test_t9_decode_rejects_offset_overflow)
{
	UBA u;
	uint32 seg, blk;
	uint16 off, row;

	u.raw[0] = 1;									/* segment_id = 1 */
	u.raw[1] = (uint64) TT_SLOTS_PER_SEGMENT;		/* offset = 48 (out of range) */
	UT_ASSERT_EQ((int) uba_decode(u, &seg, &blk, &off, &row), 0);
}

UT_TEST(test_t10_decode_rejects_nonzero_reserved)
{
	UBA u;
	uint32 seg, blk;
	uint16 off, row;

	u.raw[0] = 1;
	u.raw[1] = ((uint64) 1ULL << 32);		/* reserved high 32 bits non-zero */
	UT_ASSERT_EQ((int) uba_decode(u, &seg, &blk, &off, &row), 0);
}


/* ---------- T11-T12: uba_get_* accessors ---------- */

UT_TEST(test_t11_get_segment_id)
{
	UBA u = uba_encode(257, 7, 5, 0);

	UT_ASSERT_EQ((int) uba_get_segment_id(u), 257);
}

UT_TEST(test_t12_get_tt_slot_offset)
{
	UBA u = uba_encode(257, 7, 5, 0);

	UT_ASSERT_EQ((int) uba_get_tt_slot_offset(u), 5);
}


/* ---------- T13-T15: offset/id sentinel separation (F1) ---------- */

UT_TEST(test_t13_offset_to_id_endpoints)
{
	UT_ASSERT_EQ((int) cluster_tt_slot_offset_to_id(0), 1);
	UT_ASSERT_EQ((int) cluster_tt_slot_offset_to_id(TT_SLOTS_PER_SEGMENT - 1),
				 TT_SLOTS_PER_SEGMENT);
}

UT_TEST(test_t14_id_to_offset_endpoints)
{
	UT_ASSERT_EQ((int) cluster_tt_slot_id_to_offset(1), 0);
	UT_ASSERT_EQ((int) cluster_tt_slot_id_to_offset(TT_SLOTS_PER_SEGMENT),
				 TT_SLOTS_PER_SEGMENT - 1);
}

UT_TEST(test_t15_offset_id_mutual_inverse)
{
	int i;

	for (i = 0; i < TT_SLOTS_PER_SEGMENT; i++)
	{
		uint32 id = cluster_tt_slot_offset_to_id((uint16) i);
		uint16 off = cluster_tt_slot_id_to_offset(id);

		UT_ASSERT_EQ((int) off, i);
	}
}


/* ---------- T16-T19: uba_origin_node_id derivation ---------- */

UT_TEST(test_t16_origin_node_invalid_uba)
{
	UBA u = InvalidUba_init;

	UT_ASSERT_EQ((int) uba_origin_node_id(u), (int) InvalidNodeId);
}

UT_TEST(test_t17_origin_node_segment_1)
{
	UBA u = uba_encode(1, 0, 0, 0);		/* node 0's first segment */

	UT_ASSERT_EQ((int) uba_origin_node_id(u), 0);
}

UT_TEST(test_t18_origin_node_segment_257)
{
	UBA u = uba_encode(257, 0, 0, 0);	/* node 1's first segment */

	UT_ASSERT_EQ((int) uba_origin_node_id(u), 1);
}

UT_TEST(test_t19_origin_node_out_of_range_rejected)
{
	/*
	 * Synthesize a UBA whose decoded segment_id is within UINT16_MAX but
	 * derives a node beyond SCN_MAX_VALID_NODE_ID.  derived = (seg - 1) /
	 * CLUSTER_UNDO_SEGS_PER_INSTANCE.  For derived = 128 (one past the
	 * valid 127 cap) we need seg in [128*256+1, 129*256] = [32769, 33024].
	 */
	UBA u = uba_encode(32769, 0, 0, 0);

	UT_ASSERT_EQ((int) uba_origin_node_id(u), (int) InvalidNodeId);
}


/* ---------- T20-T24: constants / sentinels ---------- */

UT_TEST(test_t20_segs_per_instance_is_256)
{
	UT_ASSERT_EQ((int) CLUSTER_UNDO_SEGS_PER_INSTANCE, 256);
}

UT_TEST(test_t21_owner_invalid_is_zero)
{
	UT_ASSERT_EQ((int) CLUSTER_UNDO_OWNER_INVALID, 0);
}

UT_TEST(test_t22_itl_delta_format_v1_is_zero)
{
	UT_ASSERT_EQ((int) CLUSTER_ITL_DELTA_FORMAT_V1, 0);
}

UT_TEST(test_t23_itl_delta_format_v2_is_one)
{
	UT_ASSERT_EQ((int) CLUSTER_ITL_DELTA_FORMAT_V2, 1);
}

UT_TEST(test_t24_invalid_tt_slot_offset_is_0xFFFF)
{
	UT_ASSERT_EQ((int) INVALID_TT_SLOT_OFFSET, (int) 0xFFFF);
}


int
main(void)
{
	UT_RUN(test_t1_uba_sizeof_16);
	UT_RUN(test_t2_invalid_uba_all_zero);
	UT_RUN(test_t3_decode_invalid_uba_returns_false);
	UT_RUN(test_t4_roundtrip_low_values);
	UT_RUN(test_t5_roundtrip_high_values);
	UT_RUN(test_t6_roundtrip_mid_range);
	UT_RUN(test_t7_decode_rejects_segment_zero);
	UT_RUN(test_t8_decode_rejects_segment_over_uint16max);
	UT_RUN(test_t9_decode_rejects_offset_overflow);
	UT_RUN(test_t10_decode_rejects_nonzero_reserved);
	UT_RUN(test_t11_get_segment_id);
	UT_RUN(test_t12_get_tt_slot_offset);
	UT_RUN(test_t13_offset_to_id_endpoints);
	UT_RUN(test_t14_id_to_offset_endpoints);
	UT_RUN(test_t15_offset_id_mutual_inverse);
	UT_RUN(test_t16_origin_node_invalid_uba);
	UT_RUN(test_t17_origin_node_segment_1);
	UT_RUN(test_t18_origin_node_segment_257);
	UT_RUN(test_t19_origin_node_out_of_range_rejected);
	UT_RUN(test_t20_segs_per_instance_is_256);
	UT_RUN(test_t21_owner_invalid_is_zero);
	UT_RUN(test_t22_itl_delta_format_v1_is_zero);
	UT_RUN(test_t23_itl_delta_format_v2_is_one);
	UT_RUN(test_t24_invalid_tt_slot_offset_is_0xFFFF);

	return ut_failed_count == 0 ? 0 : 1;
}
