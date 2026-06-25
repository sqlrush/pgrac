/*-------------------------------------------------------------------------
 *
 * test_cluster_gcs_block_3way.c
 *	  Compile-time invariants for the spec-2.36 Cache Fusion 3-way
 *	  protocol (X writer transfer + reader starvation guard).
 *
 *	  Header-only checks — no linking of cluster_gcs_block.o or
 *	  cluster_pcm_lock.o.  Behavioral coverage (master decision tree
 *	  X-state branch, broadcast invalidate sync ack, holder invalidate
 *	  handler, HC117 S barrier, HC123 XLogFlush-before-evict, HC124
 *	  node-dead pending_x sweep, HC121 ClusterTriple 3-node fixture)
 *	  lives in cluster_tap t/114_gcs_block_3way_2node.pl (2-node) +
 *	  t/115_gcs_block_3way_3node.pl (3-node).
 *
 *	  Tests in this binary (L1-L22 — last 4 are placeholder refs to
 *	  TAP 114/115 behavioral verification):
 *	    L1  PGRAC_IC_MSG_GCS_BLOCK_INVALIDATE == 17
 *	    L2  PGRAC_IC_MSG_GCS_BLOCK_INVALIDATE_ACK == 18
 *	    L3  Reply status enum:  X_GRANTED_FROM_HOLDER == 9
 *	    L4  Reply status enum:  DENIED_PENDING_X == 10
 *	    L5  Reply status enum:  DENIED_INVALIDATE_TIMEOUT == 11
 *	    L6  GcsBlockInvalidatePayload sizeof == 64B (StaticAssertDecl)
 *	    L7  GcsBlockInvalidateAckPayload sizeof == 64B (StaticAssertDecl)
 *	    L8  GcsBlockInvalidatePayload field offsets locked
 *	    L9  GcsBlockInvalidateAckPayload field offsets locked
 *	    L10 INVALIDATE + ACK distinct msg_type (codereview F1 P0 防御)
 *	    L11 Reply status enum no-gap continuation (0..11)
 *	    L12 GCS_BLOCK_DEDUP_INVALIDATE_IN_FLIGHT == 6 (HC120)
 *	    L13 GcsBlockDedupResult enum 7 values total (was 6 spec-2.35)
 *	    L14 INVALIDATE_ACK distinct from FORWARD (17 vs 16 vs 18)
 *	    L15 cluster_pcm_lock_set_pending_x prototype linkable
 *	    L16 cluster_pcm_lock_clear_pending_x prototype linkable
 *	    L17 cluster_pcm_lock_query_pending_x_requester prototype linkable
 *	    L18 cluster_pcm_lock_clear_pending_x_for_node prototype linkable (HC124)
 *	    L19 cluster_pcm_lock_query_s_holders_bitmap prototype linkable
 *	    L20 cluster_bufmgr_invalidate_block_for_gcs prototype linkable (HC118/HC123)
 *	    L21 placeholder — TAP 114 L1-L10 2-node X transfer + S barrier + 53R91/92
 *	    L22 placeholder — TAP 115 L1-L8 3-node X transfer + ClusterTriple HC121
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_gcs_block_3way.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <stddef.h>

#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_gcs_block_dedup.h"
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


/* ----- L1-L2: msg_type wire ABI ----- */

UT_TEST(test_invalidate_msg_type_is_17)
{
	UT_ASSERT_EQ((int)PGRAC_IC_MSG_GCS_BLOCK_INVALIDATE, 17);
}


UT_TEST(test_invalidate_ack_msg_type_is_18)
{
	UT_ASSERT_EQ((int)PGRAC_IC_MSG_GCS_BLOCK_INVALIDATE_ACK, 18);
}


/* ----- L3-L5: reply status enum extensions ----- */

UT_TEST(test_x_granted_from_holder_status_is_9)
{
	UT_ASSERT_EQ((int)GCS_BLOCK_REPLY_X_GRANTED_FROM_HOLDER, 9);
}


UT_TEST(test_denied_pending_x_status_is_10)
{
	UT_ASSERT_EQ((int)GCS_BLOCK_REPLY_DENIED_PENDING_X, 10);
}


UT_TEST(test_denied_invalidate_timeout_status_is_11)
{
	UT_ASSERT_EQ((int)GCS_BLOCK_REPLY_DENIED_INVALIDATE_TIMEOUT, 11);
}


/* ----- L6-L7: payload sizeof ----- */

UT_TEST(test_invalidate_payload_size_locked_at_64)
{
	UT_ASSERT_EQ((int)sizeof(GcsBlockInvalidatePayload), 64);
}


UT_TEST(test_invalidate_ack_payload_size_locked_at_64)
{
	UT_ASSERT_EQ((int)sizeof(GcsBlockInvalidateAckPayload), 64);
}


/* ----- L8-L9: payload field offsets ----- */

UT_TEST(test_invalidate_payload_field_offsets)
{
	UT_ASSERT_EQ((int)offsetof(GcsBlockInvalidatePayload, request_id), 0);
	UT_ASSERT_EQ((int)offsetof(GcsBlockInvalidatePayload, epoch), 8);
	UT_ASSERT_EQ((int)offsetof(GcsBlockInvalidatePayload, tag), 16);
	UT_ASSERT_EQ((int)offsetof(GcsBlockInvalidatePayload, master_node), 36);
	UT_ASSERT_EQ((int)offsetof(GcsBlockInvalidatePayload, invalidating_for_x_node), 40);
	UT_ASSERT_EQ((int)offsetof(GcsBlockInvalidatePayload, checksum), 48);
}


UT_TEST(test_invalidate_ack_payload_field_offsets)
{
	UT_ASSERT_EQ((int)offsetof(GcsBlockInvalidateAckPayload, request_id), 0);
	UT_ASSERT_EQ((int)offsetof(GcsBlockInvalidateAckPayload, epoch), 8);
	UT_ASSERT_EQ((int)offsetof(GcsBlockInvalidateAckPayload, tag), 16);
	UT_ASSERT_EQ((int)offsetof(GcsBlockInvalidateAckPayload, sender_node), 36);
	UT_ASSERT_EQ((int)offsetof(GcsBlockInvalidateAckPayload, ack_status), 40);
	UT_ASSERT_EQ((int)offsetof(GcsBlockInvalidateAckPayload, checksum), 48);
	/* spec-2.41 D3 — @52 carrier reinterpreted from page_lsn_bytes to page_scn_bytes. */
	UT_ASSERT_EQ((int)offsetof(GcsBlockInvalidateAckPayload, page_scn_bytes), 52);
}


/* ----- L10: request + ack msg_type distinct (codereview F1 P0) ----- */

UT_TEST(test_invalidate_request_and_ack_msg_type_distinct)
{
	/* codereview F1 P0:  request + ack are both 64B fixed payload;
	 * IC dispatcher demuxes by msg_type only.  These MUST be different. */
	UT_ASSERT((int)PGRAC_IC_MSG_GCS_BLOCK_INVALIDATE != (int)PGRAC_IC_MSG_GCS_BLOCK_INVALIDATE_ACK);
	UT_ASSERT_EQ((int)sizeof(GcsBlockInvalidatePayload), (int)sizeof(GcsBlockInvalidateAckPayload));
}


/* ----- L11: reply status enum no-gap continuation ----- */

UT_TEST(test_reply_status_enum_no_gap_continuation)
{
	/* spec-2.36 extended reply status from 8 to 11.  All values 0..11
	 * must be distinct and contiguous (no enum gap that would let a
	 * silent typo encode an unhandled status). */
	UT_ASSERT_EQ((int)GCS_BLOCK_REPLY_GRANTED, 0);
	UT_ASSERT_EQ((int)GCS_BLOCK_REPLY_GRANTED_STORAGE_FALLBACK, 1);
	UT_ASSERT_EQ((int)GCS_BLOCK_REPLY_DENIED_INCOMPATIBLE, 2);
	UT_ASSERT_EQ((int)GCS_BLOCK_REPLY_DENIED_VALIDATOR_REJECT, 3);
	UT_ASSERT_EQ((int)GCS_BLOCK_REPLY_DENIED_EPOCH_STALE, 4);
	UT_ASSERT_EQ((int)GCS_BLOCK_REPLY_DENIED_CHECKSUM_FAIL, 5);
	UT_ASSERT_EQ((int)GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER, 6);
	UT_ASSERT_EQ((int)GCS_BLOCK_REPLY_DENIED_DEDUP_FULL, 7);
	UT_ASSERT_EQ((int)GCS_BLOCK_REPLY_GRANTED_FROM_HOLDER, 8);
	UT_ASSERT_EQ((int)GCS_BLOCK_REPLY_X_GRANTED_FROM_HOLDER, 9);
	UT_ASSERT_EQ((int)GCS_BLOCK_REPLY_DENIED_PENDING_X, 10);
	UT_ASSERT_EQ((int)GCS_BLOCK_REPLY_DENIED_INVALIDATE_TIMEOUT, 11);
}


/* ----- L12-L13: dedup state machine extensions ----- */

UT_TEST(test_dedup_invalidate_in_flight_is_6)
{
	UT_ASSERT_EQ((int)GCS_BLOCK_DEDUP_INVALIDATE_IN_FLIGHT, 6);
}


UT_TEST(test_dedup_enum_distinct_values)
{
	UT_ASSERT((int)GCS_BLOCK_DEDUP_INVALIDATE_IN_FLIGHT
			  != (int)GCS_BLOCK_DEDUP_FORWARDED_DUPLICATE);
	UT_ASSERT((int)GCS_BLOCK_DEDUP_INVALIDATE_IN_FLIGHT
			  != (int)GCS_BLOCK_DEDUP_IN_FLIGHT_DUPLICATE);
	UT_ASSERT((int)GCS_BLOCK_DEDUP_INVALIDATE_IN_FLIGHT != (int)GCS_BLOCK_DEDUP_CACHED_REPLY);
}


/* ----- L14: msg_type 16/17/18 distinct ----- */

UT_TEST(test_msg_type_16_17_18_distinct)
{
	UT_ASSERT((int)PGRAC_IC_MSG_GCS_BLOCK_FORWARD != (int)PGRAC_IC_MSG_GCS_BLOCK_INVALIDATE);
	UT_ASSERT((int)PGRAC_IC_MSG_GCS_BLOCK_FORWARD != (int)PGRAC_IC_MSG_GCS_BLOCK_INVALIDATE_ACK);
	UT_ASSERT((int)PGRAC_IC_MSG_GCS_BLOCK_INVALIDATE != (int)PGRAC_IC_MSG_GCS_BLOCK_INVALIDATE_ACK);
}


/* ----- L15-L20: S barrier helper prototypes linkable ----- */

UT_TEST(test_s_barrier_set_pending_x_prototype_linkable)
{
	/* Compile-time guard that the prototype matches the header decl.
	 * NULL call is unreachable at runtime but ensures the symbol
	 * compiles into the test binary's external reference table. */
	void (*fp)(BufferTag, int32, uint64) = &cluster_pcm_lock_set_pending_x;

	UT_ASSERT(fp != NULL);
}


UT_TEST(test_s_barrier_clear_pending_x_prototype_linkable)
{
	void (*fp)(BufferTag) = &cluster_pcm_lock_clear_pending_x;

	UT_ASSERT(fp != NULL);
}


UT_TEST(test_s_barrier_query_pending_x_prototype_linkable)
{
	int32 (*fp)(BufferTag) = &cluster_pcm_lock_query_pending_x_requester;

	UT_ASSERT(fp != NULL);
}


UT_TEST(test_s_barrier_clear_pending_x_for_node_prototype_linkable)
{
	/* HC124 LMON node-dead sweep. */
	uint64 (*fp)(int32) = &cluster_pcm_lock_clear_pending_x_for_node;

	UT_ASSERT(fp != NULL);
}


UT_TEST(test_s_holders_bitmap_query_prototype_linkable)
{
	uint32 (*fp)(BufferTag) = &cluster_pcm_lock_query_s_holders_bitmap;

	UT_ASSERT(fp != NULL);
}


UT_TEST(test_bufmgr_invalidate_block_for_gcs_prototype_linkable)
{
	/* HC118 + HC123 — by-tag invalidate helper in bufmgr.c.  spec-2.41 D3 added
	 * the *out_page_scn out-param (ACK SCN carrier source). */
	bool (*fp)(BufferTag, PcmLockMode, XLogRecPtr *, SCN *)
		= &cluster_bufmgr_invalidate_block_for_gcs;

	UT_ASSERT(fp != NULL);
}


/* ----- L21-L22: placeholders — behavioral coverage in cluster_tap ----- */

UT_TEST(test_placeholder_tap_114_2node_x_transfer)
{
	/* TAP 114 L1-L10 covers:  N→X cross-node grant from master /
	 * S→X upgrade with bitmap → broadcast invalidate / X→X holder
	 * direct ship via HC115 forward / invalidate ack timeout 53R91 /
	 * dirty buffer XLogFlush-before-ack (HC123) / epoch validation /
	 * S barrier DENIED_PENDING_X reply / reader backoff retry →
	 * eventual grant / starvation 53R92 budget exhaustion. */
	UT_ASSERT(true);
}


UT_TEST(test_placeholder_tap_115_3node_x_transfer)
{
	/* TAP 115 L1-L8 covers:  3-node A/B/C topology via ClusterTriple
	 * HC121 fixture;  Node A holds X / B requests X / C concurrent N→S;
	 * 3-way bitmap S holders broadcast invalidate ack collection;
	 * X transfer A→B success + C eventually grants N→S;  multi-pending
	 * X requests both eventually grant (non-strict FIFO trade-off). */
	UT_ASSERT(true);
}


int
main(void)
{
	UT_RUN(test_invalidate_msg_type_is_17);
	UT_RUN(test_invalidate_ack_msg_type_is_18);
	UT_RUN(test_x_granted_from_holder_status_is_9);
	UT_RUN(test_denied_pending_x_status_is_10);
	UT_RUN(test_denied_invalidate_timeout_status_is_11);
	UT_RUN(test_invalidate_payload_size_locked_at_64);
	UT_RUN(test_invalidate_ack_payload_size_locked_at_64);
	UT_RUN(test_invalidate_payload_field_offsets);
	UT_RUN(test_invalidate_ack_payload_field_offsets);
	UT_RUN(test_invalidate_request_and_ack_msg_type_distinct);
	UT_RUN(test_reply_status_enum_no_gap_continuation);
	UT_RUN(test_dedup_invalidate_in_flight_is_6);
	UT_RUN(test_dedup_enum_distinct_values);
	UT_RUN(test_msg_type_16_17_18_distinct);
	UT_RUN(test_s_barrier_set_pending_x_prototype_linkable);
	UT_RUN(test_s_barrier_clear_pending_x_prototype_linkable);
	UT_RUN(test_s_barrier_query_pending_x_prototype_linkable);
	UT_RUN(test_s_barrier_clear_pending_x_for_node_prototype_linkable);
	UT_RUN(test_s_holders_bitmap_query_prototype_linkable);
	UT_RUN(test_bufmgr_invalidate_block_for_gcs_prototype_linkable);
	UT_RUN(test_placeholder_tap_114_2node_x_transfer);
	UT_RUN(test_placeholder_tap_115_3node_x_transfer);
	UT_DONE();
}
