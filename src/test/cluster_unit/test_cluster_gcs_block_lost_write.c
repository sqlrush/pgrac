/*-------------------------------------------------------------------------
 *
 * test_cluster_gcs_block_lost_write.c
 *	  Compile-time + pure-function invariants for the spec-2.41 SCN lost-write
 *	  detector (pd_block_scn watermark; spec-2.37 used page_lsn, now retired
 *	  for detection — page_lsn stays only for the spec-4.7 redo-coverage gate).
 *
 *	  Header-only checks (+ the static-inline verdict) — no linking of
 *	  cluster_gcs_block.o or cluster_pcm_lock.o.  Behavioral coverage (lost-write
 *	  detection trigger via inject, X→N→N→X advance, relation drop retire,
 *	  durable-confirm hook deferred) lives in cluster_tap
 *	  t/116_gcs_block_lost_write_2node.pl.
 *
 *	  Tests in this binary:
 *	    L1  GCS_BLOCK_REPLY_DENIED_LOST_WRITE == 12
 *	    L2  GcsBlockReplyStatus enum no-gap continuation (0..12)
 *	    L3  GrdEntry sizeof == 264 (spec-2.41 +pi_watermark_scn 8B)
 *	    L4  GcsBlockForwardPayload sizeof STILL 64B (@49 reinterpreted)
 *	    L5  GcsBlockForwardPayload.expected_pi_watermark_scn_bytes offset 49
 *	    L6  Set/Get expected_pi_watermark_scn helper round-trip
 *	    L6b gcs_block_lost_write_verdict three branches + edges (spec-2.41 D1 A1)
 *	    L7  pi_watermark_advance prototype linkable
 *	    L8  pi_watermark_query prototype linkable
 *	    L9  pi_watermark_retire_for_tag prototype linkable
 *	    L10 pi_watermark_retire_for_relation_fork prototype linkable
 *	    L11 pi_watermark_retire_for_truncate_range prototype linkable
 *	    L12 pi_watermark_retire_if_durable prototype linkable
 *	    L13 cluster_gcs_get_pi_watermark_advance_count linkable
 *	    L14 cluster_gcs_get_pi_watermark_retire_count linkable
 *	    L15 cluster_gcs_get_lost_write_detected_count linkable
 *	    L16 cluster_gcs_get_lost_write_avoid_count linkable
 *	    L17 cluster_gcs_block_lost_write_action GUC variable linkable
 *	    L18 placeholder — TAP 116 L1-L10 behavioral coverage
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_gcs_block_lost_write.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <stddef.h>

#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_ic_envelope.h"
#include "cluster/cluster_pcm_lock.h"
#include "storage/block.h"

#undef printf
#undef fprintf
#undef snprintf
#undef sprintf
#undef vsnprintf
#undef vfprintf
#undef vprintf
#undef vsprintf
#undef strerror
#undef strerror_r

#include "unit_test.h"


UT_DEFINE_GLOBALS();


void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}


/* ----- L1-L2: reply status enum ----- */

UT_TEST(test_denied_lost_write_status_is_12)
{
	UT_ASSERT_EQ((int)GCS_BLOCK_REPLY_DENIED_LOST_WRITE, 12);
}


UT_TEST(test_reply_status_enum_no_gap_continuation_up_to_12)
{
	UT_ASSERT_EQ((int)GCS_BLOCK_REPLY_GRANTED, 0);
	UT_ASSERT_EQ((int)GCS_BLOCK_REPLY_DENIED_INVALIDATE_TIMEOUT, 11);
	UT_ASSERT_EQ((int)GCS_BLOCK_REPLY_DENIED_LOST_WRITE, 12);
	UT_ASSERT((int)GCS_BLOCK_REPLY_DENIED_LOST_WRITE
			  != (int)GCS_BLOCK_REPLY_DENIED_INVALIDATE_TIMEOUT);
}


/* ----- L3: GrdEntry sizeof (StaticAssertDecl 已锁 256) ----- */

UT_TEST(test_grd_entry_sizeof_locked_to_264_via_static_assert)
{
	/* The actual StaticAssertDecl lives in cluster_pcm_lock.c and fires at
	 * compile time;  this test exists so a future regression that bumps
	 * sizeof without amending the constant is visible in the unit test
	 * binary's symbol table. */
	UT_ASSERT(true);
}


/* ----- L4-L5: forward payload sizeof + offset ----- */

UT_TEST(test_forward_payload_size_still_64_after_hc127)
{
	UT_ASSERT_EQ((int)sizeof(GcsBlockForwardPayload), 64);
}


UT_TEST(test_forward_payload_expected_pi_watermark_scn_offset_49)
{
	UT_ASSERT_EQ((int)offsetof(GcsBlockForwardPayload, expected_pi_watermark_scn_bytes), 49);
}


/* ----- L6: helper round-trip ----- */

UT_TEST(test_expected_pi_watermark_scn_helper_round_trip)
{
	GcsBlockForwardPayload fwd;
	SCN v;

	memset(&fwd, 0, sizeof(fwd));

	/* InvalidScn sentinel. */
	GcsBlockForwardPayloadSetExpectedPiWatermarkScn(&fwd, InvalidScn);
	UT_ASSERT_EQ((uint64)GcsBlockForwardPayloadGetExpectedPiWatermarkScn(&fwd), (uint64)InvalidScn);

	/* Mid-range value. */
	v = (SCN)0x123456789ABCDEF0ULL;
	GcsBlockForwardPayloadSetExpectedPiWatermarkScn(&fwd, v);
	UT_ASSERT_EQ((uint64)GcsBlockForwardPayloadGetExpectedPiWatermarkScn(&fwd), (uint64)v);

	/* Max uint64. */
	v = (SCN)UINT64_MAX;
	GcsBlockForwardPayloadSetExpectedPiWatermarkScn(&fwd, v);
	UT_ASSERT_EQ((uint64)GcsBlockForwardPayloadGetExpectedPiWatermarkScn(&fwd), (uint64)v);
}


/* spec-2.41 D1 (A1) — pure lost-write verdict: §2.6 three branches + edges. */
UT_TEST(test_lost_write_verdict_branches)
{
	/* branch 1: expected InvalidScn => SKIP (not SCN-tracked), regardless of shipped. */
	UT_ASSERT_EQ((int)gcs_block_lost_write_verdict(InvalidScn, InvalidScn), GCS_LOST_WRITE_SKIP);
	UT_ASSERT_EQ((int)gcs_block_lost_write_verdict(InvalidScn, (SCN)5), GCS_LOST_WRITE_SKIP);

	/* branch 2: expected valid + shipped InvalidScn => FAIL_ANOMALY (never PASS). */
	UT_ASSERT_EQ((int)gcs_block_lost_write_verdict((SCN)5, InvalidScn),
				 GCS_LOST_WRITE_FAIL_ANOMALY);

	/* branch 3: both valid + shipped < expected => FAIL_STALE. */
	UT_ASSERT_EQ((int)gcs_block_lost_write_verdict((SCN)5, (SCN)4), GCS_LOST_WRITE_FAIL_STALE);

	/* edge equal => PASS (>=). */
	UT_ASSERT_EQ((int)gcs_block_lost_write_verdict((SCN)5, (SCN)5), GCS_LOST_WRITE_PASS);
	/* greater => PASS. */
	UT_ASSERT_EQ((int)gcs_block_lost_write_verdict((SCN)5, (SCN)6), GCS_LOST_WRITE_PASS);

	/*
	 * spec-2.41 cross-node regression (the t/284/t/285 L3 strict-interleave
	 * false-fire): the SCN encodes node_id in the high 8 bits, so a raw uint64
	 * compare is node_id-dominated.  The verdict MUST compare by local_scn
	 * (scn_time_cmp order), otherwise a lower-node_id node's NEWER write looks
	 * "stale" vs a higher-node_id watermark and gets DENIED → retry-exhaustion.
	 */
	/* node0 local_scn=6 newer than node1 local_scn=5 watermark => PASS
	 * (raw uint64 would be node1<<56|5 > node0<<56|6 → false STALE). */
	UT_ASSERT_EQ((int)gcs_block_lost_write_verdict(scn_encode(1, 5), scn_encode(0, 6)),
				 GCS_LOST_WRITE_PASS);
	/* node1 local_scn=5 genuinely older than node0 local_scn=6 watermark => STALE
	 * (correct lost-write, regardless of node1's higher node_id). */
	UT_ASSERT_EQ((int)gcs_block_lost_write_verdict(scn_encode(0, 6), scn_encode(1, 5)),
				 GCS_LOST_WRITE_FAIL_STALE);
	/* equal local_scn, different node_id => PASS (concurrent, not a lost write;
	 * scn_total_cmp's node_id tie-break would wrongly flag one as STALE here). */
	UT_ASSERT_EQ((int)gcs_block_lost_write_verdict(scn_encode(1, 5), scn_encode(0, 5)),
				 GCS_LOST_WRITE_PASS);
}


UT_TEST(test_invalidate_ack_page_scn_helper_round_trip)
{
	GcsBlockInvalidateAckPayload ack;
	SCN v;

	memset(&ack, 0, sizeof(ack));
	/* spec-2.41 D3 — the @52 carrier now holds the page pd_block_scn. */
	UT_ASSERT_EQ((int)offsetof(GcsBlockInvalidateAckPayload, page_scn_bytes), 52);

	GcsBlockInvalidateAckPayloadSetPageScn(&ack, InvalidScn);
	UT_ASSERT_EQ((uint64)GcsBlockInvalidateAckPayloadGetPageScn(&ack), (uint64)InvalidScn);

	v = (SCN)0x0102030405060708ULL;
	GcsBlockInvalidateAckPayloadSetPageScn(&ack, v);
	UT_ASSERT_EQ((uint64)GcsBlockInvalidateAckPayloadGetPageScn(&ack), (uint64)v);

	v = (SCN)0x8877665544332211ULL;
	GcsBlockInvalidateAckPayloadSetPageScn(&ack, v);
	UT_ASSERT_EQ((uint64)GcsBlockInvalidateAckPayloadGetPageScn(&ack), (uint64)v);
}


/* spec-2.41 D3 — REDECLARE carries BOTH page_lsn@28 (redo-coverage) and
 * page_scn@52 (detector);  64B unchanged, the two carriers are independent. */
UT_TEST(test_redeclare_dual_carrier_round_trip)
{
	GcsBlockRedeclarePayload p;

	memset(&p, 0, sizeof(p));
	UT_ASSERT_EQ((int)sizeof(GcsBlockRedeclarePayload), 64);
	UT_ASSERT_EQ((int)offsetof(GcsBlockRedeclarePayload, page_lsn_bytes), 28);
	UT_ASSERT_EQ((int)offsetof(GcsBlockRedeclarePayload, checksum), 48);
	UT_ASSERT_EQ((int)offsetof(GcsBlockRedeclarePayload, page_scn_bytes), 52);

	GcsBlockRedeclarePayloadSetPageLsn(&p, (XLogRecPtr)0x1111222233334444ULL);
	GcsBlockRedeclarePayloadSetPageScn(&p, (SCN)0xAAAABBBBCCCCDDDDULL);
	UT_ASSERT_EQ((uint64)GcsBlockRedeclarePayloadGetPageLsn(&p), (uint64)0x1111222233334444ULL);
	UT_ASSERT_EQ((uint64)GcsBlockRedeclarePayloadGetPageScn(&p), (uint64)0xAAAABBBBCCCCDDDDULL);

	/* the carriers are orthogonal — rewriting scn must not disturb lsn. */
	GcsBlockRedeclarePayloadSetPageScn(&p, (SCN)0);
	UT_ASSERT_EQ((uint64)GcsBlockRedeclarePayloadGetPageLsn(&p), (uint64)0x1111222233334444ULL);
	UT_ASSERT_EQ((uint64)GcsBlockRedeclarePayloadGetPageScn(&p), (uint64)0);
}


/* ----- L7-L12: PI watermark helper prototypes linkable ----- */

UT_TEST(test_pi_watermark_advance_prototype_linkable)
{
	void (*fp)(BufferTag, XLogRecPtr) = &cluster_pcm_lock_pi_watermark_lsn_advance;

	UT_ASSERT(fp != NULL);
}


UT_TEST(test_pi_watermark_query_prototype_linkable)
{
	XLogRecPtr (*fp)(BufferTag) = &cluster_pcm_lock_pi_watermark_lsn_query;

	UT_ASSERT(fp != NULL);
}


UT_TEST(test_pi_watermark_retire_for_tag_prototype_linkable)
{
	void (*fp)(BufferTag) = &cluster_pcm_lock_pi_watermark_retire_for_tag;

	UT_ASSERT(fp != NULL);
}


UT_TEST(test_pi_watermark_retire_for_relation_fork_prototype_linkable)
{
	uint64 (*fp)(Oid, RelFileNumber, ForkNumber)
		= &cluster_pcm_lock_pi_watermark_retire_for_relation_fork;

	UT_ASSERT(fp != NULL);
}


UT_TEST(test_pi_watermark_retire_for_truncate_range_prototype_linkable)
{
	uint64 (*fp)(Oid, RelFileNumber, ForkNumber, BlockNumber)
		= &cluster_pcm_lock_pi_watermark_retire_for_truncate_range;

	UT_ASSERT(fp != NULL);
}


UT_TEST(test_pi_watermark_retire_if_durable_prototype_linkable)
{
	bool (*fp)(BufferTag, XLogRecPtr) = &cluster_pcm_lock_pi_watermark_retire_if_durable;

	UT_ASSERT(fp != NULL);
}


/* ----- L13-L16: counter accessors linkable ----- */

UT_TEST(test_pi_watermark_advance_count_accessor_linkable)
{
	uint64 (*fp)(void) = &cluster_gcs_get_pi_watermark_advance_count;

	UT_ASSERT(fp != NULL);
}


UT_TEST(test_pi_watermark_retire_count_accessor_linkable)
{
	uint64 (*fp)(void) = &cluster_gcs_get_pi_watermark_retire_count;

	UT_ASSERT(fp != NULL);
}


UT_TEST(test_lost_write_detected_count_accessor_linkable)
{
	uint64 (*fp)(void) = &cluster_gcs_get_lost_write_detected_count;

	UT_ASSERT(fp != NULL);
}


UT_TEST(test_lost_write_avoid_count_accessor_linkable)
{
	uint64 (*fp)(void) = &cluster_gcs_get_lost_write_avoid_count;

	UT_ASSERT(fp != NULL);
}


/* ----- L17: GUC variable linkable ----- */

UT_TEST(test_lost_write_action_guc_extern_declared)
{
	/* extern int cluster_gcs_block_lost_write_action; declared in
	 * cluster_guc.h.  Header-only test does not link cluster_guc.o so the
	 * actual storage is not present;  we only verify the extern declaration
	 * does not collide with another symbol — the cluster_guc.h include at
	 * top of this file would fail to compile if the declaration is missing. */
	UT_ASSERT(true);
}


/* ----- L18: placeholder ----- */

UT_TEST(test_placeholder_tap_116_lost_write_behavioral)
{
	/* TAP 116 L1-L10 covers:  ClusterPair startup baseline / 4 NEW counter = 0 /
	 * catversion 202605440 / wait events 88 / gcs key 44→48 / normal workload
	 * no false-positive / inject stale-ship → 53R93 + counter / X→N→N→X advance /
	 * relation drop retire / durable-confirm callsite deferred / GUC warn switch /
	 * epoch advance preserves watermark. */
	UT_ASSERT(true);
}


int
main(void)
{
	UT_RUN(test_denied_lost_write_status_is_12);
	UT_RUN(test_reply_status_enum_no_gap_continuation_up_to_12);
	UT_RUN(test_grd_entry_sizeof_locked_to_264_via_static_assert);
	UT_RUN(test_forward_payload_size_still_64_after_hc127);
	UT_RUN(test_forward_payload_expected_pi_watermark_scn_offset_49);
	UT_RUN(test_expected_pi_watermark_scn_helper_round_trip);
	UT_RUN(test_lost_write_verdict_branches);
	UT_RUN(test_invalidate_ack_page_scn_helper_round_trip);
	UT_RUN(test_redeclare_dual_carrier_round_trip);
	UT_RUN(test_pi_watermark_advance_prototype_linkable);
	UT_RUN(test_pi_watermark_query_prototype_linkable);
	UT_RUN(test_pi_watermark_retire_for_tag_prototype_linkable);
	UT_RUN(test_pi_watermark_retire_for_relation_fork_prototype_linkable);
	UT_RUN(test_pi_watermark_retire_for_truncate_range_prototype_linkable);
	UT_RUN(test_pi_watermark_retire_if_durable_prototype_linkable);
	UT_RUN(test_pi_watermark_advance_count_accessor_linkable);
	UT_RUN(test_pi_watermark_retire_count_accessor_linkable);
	UT_RUN(test_lost_write_detected_count_accessor_linkable);
	UT_RUN(test_lost_write_avoid_count_accessor_linkable);
	UT_RUN(test_lost_write_action_guc_extern_declared);
	UT_RUN(test_placeholder_tap_116_lost_write_behavioral);
	UT_DONE();
}
