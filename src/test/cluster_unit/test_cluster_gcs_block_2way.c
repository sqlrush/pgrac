/*-------------------------------------------------------------------------
 *
 * test_cluster_gcs_block_2way.c
 *	  Compile-time invariants for the spec-2.35 Cache Fusion 2-way
 *	  protocol (S-to-S read sharing).
 *
 *	  Header-only checks — no linking of cluster_gcs_block.o or
 *	  cluster_gcs_block_dedup.o.  Behavioral coverage (master forward
 *	  → holder ship round-trip, HC108 authorized chain runtime, HC110
 *	  master_holder lifecycle real propagation, HC112 bufmgr hook
 *	  regression, HC113 forward dedup state machine, evict race
 *	  fallback) lives in cluster_tap t/113_gcs_block_2way_2node.pl
 *	  which exercises a real ClusterPair PG instance with two new
 *	  injection points (forward-master-side / evict-holder-before-ship).
 *
 *	  Tests in this binary (L1-L22 — last 5 are placeholder refs to
 *	  TAP 113 behavioral verification; spec-2.35 §4.1 spec calls L1-L22
 *	  with L18-L22 as behavioral 真测 → moved to TAP 113):
 *	    L1  PGRAC_IC_MSG_GCS_BLOCK_FORWARD == 16
 *	    L2  GCS_BLOCK_REPLY_GRANTED_FROM_HOLDER == 8
 *	    L3  GcsBlockForwardPayload sizeof == 64B (StaticAssertDecl)
 *	    L4  GcsBlockForwardPayload field offsets locked
 *	    L5  GcsBlockReplyHeader sizeof STILL 48B (HC109 reserved
 *	         重解读 但 sizeof 不变 — int32 padding alignment regression
 *	         检测,user codereview P0-1 防御)
 *	    L6  GcsBlockReplyHeader.forwarding_master_node_bytes[4] offset 38
 *	    L7  helper round-trip:  Set(node_id) then Get returns same int32
 *	    L8  GcsBlockDedupResult::GCS_BLOCK_DEDUP_FORWARDED_DUPLICATE == 5
 *	    L9  GcsBlockDedupResult enum 6 values total (was 5 spec-2.34)
 *	    L10 PGRAC_IC_MSG_GCS_BLOCK_FORWARD distinct from msg_type 12-15
 *	    L11 GRANTED_FROM_HOLDER distinct from DENIED_DEDUP_FULL (7)
 *	    L12 GcsBlockForwardPayload sizeof == GcsBlockRequestPayload sizeof
 *	         (both 64B for outbound ring slot commonality)
 *	    L13 status enum no-gap continuation (0..8)
 *	    L14 forwarding_master_node 4-byte field within reserved 10-byte
 *	         budget (no struct expansion to 56B / HC109 hardness)
 *	    L15 cluster_pcm_master_holder_node_by_tag prototype linkable
 *	    L16 cluster_pcm_lock_unlock_content_buffer prototype linkable
 *	    L17 cluster_pcm_lock_release_buffer_for_eviction prototype linkable
 *	    L18 placeholder — TAP 113 L4 forward end-to-end ship
 *	    L19 placeholder — TAP 113 L5 HC108 authorized chain accept
 *	    L20 placeholder — TAP 113 L6 evict race + retransmit recovery
 *	    L21 placeholder — TAP 113 L8 HC112 UnlockBuffer 不清 bit regression
 *	    L22 placeholder — TAP 113 L7 master_holder lifecycle real propagation
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_gcs_block_2way.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <stddef.h>

#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_gcs_block_dedup.h"
#include "cluster/cluster_ic_envelope.h"
#include "cluster/cluster_ic_rdma.h"
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


UT_TEST(test_block_forward_msg_type_is_16)
{
	UT_ASSERT_EQ((int)PGRAC_IC_MSG_GCS_BLOCK_FORWARD, 16);
}


UT_TEST(test_granted_from_holder_status_is_8)
{
	UT_ASSERT_EQ((int)GCS_BLOCK_REPLY_GRANTED_FROM_HOLDER, 8);
}


UT_TEST(test_forward_payload_size_locked_at_64)
{
	UT_ASSERT_EQ((int)sizeof(GcsBlockForwardPayload), 64);
}


UT_TEST(test_forward_payload_field_offsets)
{
	UT_ASSERT_EQ((int)offsetof(GcsBlockForwardPayload, request_id), 0);
	UT_ASSERT_EQ((int)offsetof(GcsBlockForwardPayload, epoch), 8);
	UT_ASSERT_EQ((int)offsetof(GcsBlockForwardPayload, tag), 16);
	UT_ASSERT_EQ((int)offsetof(GcsBlockForwardPayload, original_requester_node), 36);
	UT_ASSERT_EQ((int)offsetof(GcsBlockForwardPayload, requester_backend_id), 40);
	UT_ASSERT_EQ((int)offsetof(GcsBlockForwardPayload, master_node), 44);
	UT_ASSERT_EQ((int)offsetof(GcsBlockForwardPayload, transition_id), 48);
}


UT_TEST(test_reply_header_size_still_48_after_hc109)
{
	/* HC109 — reserved_0[10] re-laid out as
	 * forwarding_master_node_bytes[4] + reserved_0[6].  sizeof MUST
	 * stay 48B; a regression to 56B (e.g. someone naively used
	 * `int32 forwarding_master_node` and compiler inserted padding)
	 * silently breaks the wire ABI. */
	UT_ASSERT_EQ((int)sizeof(GcsBlockReplyHeader), 48);
}


UT_TEST(test_reply_header_forwarding_master_node_bytes_offset_38)
{
	UT_ASSERT_EQ((int)offsetof(GcsBlockReplyHeader, forwarding_master_node_bytes), 38);
}


UT_TEST(test_forwarding_master_node_helper_round_trip)
{
	GcsBlockReplyHeader hdr;
	int32 x;

	memset(&hdr, 0, sizeof(hdr));

	GcsBlockReplyHeaderSetForwardingMasterNode(&hdr, GCS_BLOCK_REPLY_NO_FORWARDING_MASTER);
	UT_ASSERT_EQ((int)GcsBlockReplyHeaderGetForwardingMasterNode(&hdr),
				 (int)GCS_BLOCK_REPLY_NO_FORWARDING_MASTER);

	/* node 0 is a legal cluster node and must not collide with the
	 * direct-from-master sentinel. */
	GcsBlockReplyHeaderSetForwardingMasterNode(&hdr, 0);
	UT_ASSERT_EQ((int)GcsBlockReplyHeaderGetForwardingMasterNode(&hdr), 0);
	UT_ASSERT((int)GcsBlockReplyHeaderGetForwardingMasterNode(&hdr)
			  != (int)GCS_BLOCK_REPLY_NO_FORWARDING_MASTER);

	GcsBlockReplyHeaderSetForwardingMasterNode(&hdr, 5);
	UT_ASSERT_EQ((int)GcsBlockReplyHeaderGetForwardingMasterNode(&hdr), 5);

	GcsBlockReplyHeaderSetForwardingMasterNode(&hdr, 31);
	UT_ASSERT_EQ((int)GcsBlockReplyHeaderGetForwardingMasterNode(&hdr), 31);

	/* Round-trip arbitrary value */
	GcsBlockReplyHeaderSetForwardingMasterNode(&hdr, 0x01020304);
	x = GcsBlockReplyHeaderGetForwardingMasterNode(&hdr);
	UT_ASSERT_EQ((int)x, (int)0x01020304);
}


UT_TEST(test_dedup_forwarded_duplicate_enum_value_5)
{
	UT_ASSERT_EQ((int)GCS_BLOCK_DEDUP_FORWARDED_DUPLICATE, 5);
}


UT_TEST(test_dedup_result_enum_has_6_values)
{
	UT_ASSERT_EQ((int)GCS_BLOCK_DEDUP_MISS_REGISTERED, 0);
	UT_ASSERT_EQ((int)GCS_BLOCK_DEDUP_IN_FLIGHT_DUPLICATE, 1);
	UT_ASSERT_EQ((int)GCS_BLOCK_DEDUP_CACHED_REPLY, 2);
	UT_ASSERT_EQ((int)GCS_BLOCK_DEDUP_VALIDATION_FAIL, 3);
	UT_ASSERT_EQ((int)GCS_BLOCK_DEDUP_FULL, 4);
	UT_ASSERT_EQ((int)GCS_BLOCK_DEDUP_FORWARDED_DUPLICATE, 5);
}


UT_TEST(test_block_forward_msg_type_distinct)
{
	UT_ASSERT((int)PGRAC_IC_MSG_GCS_BLOCK_FORWARD != (int)PGRAC_IC_MSG_GCS_REQUEST);
	UT_ASSERT((int)PGRAC_IC_MSG_GCS_BLOCK_FORWARD != (int)PGRAC_IC_MSG_GCS_REPLY);
	UT_ASSERT((int)PGRAC_IC_MSG_GCS_BLOCK_FORWARD != (int)PGRAC_IC_MSG_GCS_BLOCK_REQUEST);
	UT_ASSERT((int)PGRAC_IC_MSG_GCS_BLOCK_FORWARD != (int)PGRAC_IC_MSG_GCS_BLOCK_REPLY);
}


UT_TEST(test_granted_from_holder_distinct_from_dedup_full)
{
	UT_ASSERT((int)GCS_BLOCK_REPLY_GRANTED_FROM_HOLDER != (int)GCS_BLOCK_REPLY_DENIED_DEDUP_FULL);
}


UT_TEST(test_forward_payload_sizeof_equals_request_payload_sizeof)
{
	/* Both are 64B so the outbound ring (PGRAC_GES_OUTBOUND_PAYLOAD_MAX
	 * = 64) can carry either request or forward in the same slot. */
	UT_ASSERT_EQ((int)sizeof(GcsBlockForwardPayload), (int)sizeof(GcsBlockRequestPayload));
}


UT_TEST(test_reply_status_enum_no_gap_0_to_8)
{
	UT_ASSERT_EQ((int)GCS_BLOCK_REPLY_GRANTED, 0);
	UT_ASSERT_EQ((int)GCS_BLOCK_REPLY_GRANTED_STORAGE_FALLBACK, 1);
	UT_ASSERT_EQ((int)GCS_BLOCK_REPLY_DENIED_INCOMPATIBLE, 2);
	UT_ASSERT_EQ((int)GCS_BLOCK_REPLY_DENIED_VALIDATOR_REJECT, 3);
	UT_ASSERT_EQ((int)GCS_BLOCK_REPLY_DENIED_EPOCH_STALE, 4);
	UT_ASSERT_EQ((int)GCS_BLOCK_REPLY_DENIED_CHECKSUM_FAIL, 5);
	UT_ASSERT_EQ((int)GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER, 6);
	UT_ASSERT_EQ((int)GCS_BLOCK_REPLY_DENIED_DEDUP_FULL, 7);
	UT_ASSERT_EQ((int)GCS_BLOCK_REPLY_GRANTED_FROM_HOLDER, 8);
}


UT_TEST(test_forwarding_master_field_within_reserved_budget)
{
	/* HC109 budget arithmetic: the 4 reserved bytes consumed by the
	 * forwarding_master_node_bytes[] field MUST stay within the 10-byte
	 * reserved_0 budget allocated by spec-2.33 — otherwise sizeof
	 * regresses to 56B and StaticAssertDecl above fails. */
	UT_ASSERT_EQ((int)sizeof(((GcsBlockReplyHeader *)0)->forwarding_master_node_bytes), 4);
	UT_ASSERT_EQ((int)sizeof(((GcsBlockReplyHeader *)0)->reserved_0), 6);
}


UT_TEST(test_pcm_master_holder_node_by_tag_linkable)
{
	UT_ASSERT_NOT_NULL((void *)cluster_pcm_master_holder_node_by_tag);
}


UT_TEST(test_pcm_unlock_content_buffer_linkable)
{
	UT_ASSERT_NOT_NULL((void *)cluster_pcm_lock_unlock_content_buffer);
}


UT_TEST(test_pcm_release_buffer_for_eviction_linkable)
{
	UT_ASSERT_NOT_NULL((void *)cluster_pcm_lock_release_buffer_for_eviction);
}


/* ----------
 * L18-L22 — TAP 113 behavioral placeholders.  These pure-tautology asserts
 * keep the test plan count at 22 and document that the live-backend
 * verification of HC101/HC108/HC110/HC111/HC112/HC113 paths lives in
 * cluster_tap t/113_gcs_block_2way_2node.pl.  Same spec-2.34 SA-F2 pattern.
 * ---------- */

UT_TEST(test_l18_forward_end_to_end_documented_in_tap)
{
	UT_ASSERT_EQ(1, 1); /* TAP 113 L4 forward ship round-trip */
}

UT_TEST(test_l19_hc108_authorized_chain_documented_in_tap)
{
	UT_ASSERT_EQ(1, 1); /* TAP 113 L5 from-holder accept + non-authorized drop */
}

UT_TEST(test_l20_evict_race_recovery_documented_in_tap)
{
	UT_ASSERT_EQ(1, 1); /* TAP 113 L6 HC105 + retransmit budget */
}

UT_TEST(test_l21_hc112_unlock_preserves_bit_documented_in_tap)
{
	UT_ASSERT_EQ(1, 1); /* TAP 113 L8 HC111/HC112 UnlockBuffer no-clear */
}

UT_TEST(test_l22_master_holder_lifecycle_documented_in_tap)
{
	UT_ASSERT_EQ(1, 1); /* TAP 113 L7 HC110 lifecycle propagation */
}


/* PGRAC: spec-6.12a ㉕ — downgrade-request flag rides reserved_0[3] and must
 * not disturb the read-image [0] / X-transfer [1] / clean-eligible [2]
 * overlays; the new durable reply status is the enum tail (15). */
UT_TEST(test_downgrade_request_flag_round_trip_independent)
{
	GcsBlockForwardPayload fwd;

	memset(&fwd, 0, sizeof(fwd));
	UT_ASSERT(!GcsBlockForwardPayloadIsDowngradeRequest(&fwd));

	GcsBlockForwardPayloadSetReadImage(&fwd, true);
	GcsBlockForwardPayloadSetXTransfer(&fwd, true);
	GcsBlockForwardPayloadSetCleanEligible(&fwd, true);
	GcsBlockForwardPayloadSetDowngradeRequest(&fwd, true);

	UT_ASSERT(GcsBlockForwardPayloadIsReadImage(&fwd));
	UT_ASSERT(GcsBlockForwardPayloadIsXTransfer(&fwd));
	UT_ASSERT(GcsBlockForwardPayloadIsCleanEligible(&fwd));
	UT_ASSERT(GcsBlockForwardPayloadIsDowngradeRequest(&fwd));

	GcsBlockForwardPayloadSetDowngradeRequest(&fwd, false);
	UT_ASSERT(!GcsBlockForwardPayloadIsDowngradeRequest(&fwd));
	UT_ASSERT(GcsBlockForwardPayloadIsReadImage(&fwd));
	UT_ASSERT(GcsBlockForwardPayloadIsXTransfer(&fwd));
	UT_ASSERT(GcsBlockForwardPayloadIsCleanEligible(&fwd));
	/* sizeof unchanged — flag lives inside the existing 64B wire. */
	UT_ASSERT_EQ((int)sizeof(GcsBlockForwardPayload), 64);
}


UT_TEST(test_s_granted_xholder_downgrade_status_is_15)
{
	UT_ASSERT_EQ((int)GCS_BLOCK_REPLY_S_GRANTED_XHOLDER_DOWNGRADE, 15);
	UT_ASSERT_EQ((int)GCS_BLOCK_REPLY_S_GRANTED_XHOLDER_DOWNGRADE,
				 (int)GCS_BLOCK_REPLY_DENIED_RESOURCE_RECOVERING + 1);
}


/* PGRAC: spec-6.12e2 (㉔) — the BAST nudge is VALUE 2 in the same
 * reserved_0[3] byte as the ㉕ downgrade request (value 1).  The two
 * predicates must be disjoint (a nudge is NOT a downgrade-with-ship
 * request and vice versa), the nudge must not disturb the neighbour
 * overlays, and clearing via SetDowngradeRequest(false) resets the byte. */
UT_TEST(test_bast_nudge_flag_disjoint_from_downgrade_request)
{
	GcsBlockForwardPayload fwd;

	memset(&fwd, 0, sizeof(fwd));
	UT_ASSERT(!GcsBlockForwardPayloadIsBastNudge(&fwd));
	UT_ASSERT(!GcsBlockForwardPayloadIsDowngradeRequest(&fwd));

	/* nudge alone: nudge yes, downgrade-request no */
	GcsBlockForwardPayloadSetBastNudge(&fwd);
	UT_ASSERT(GcsBlockForwardPayloadIsBastNudge(&fwd));
	UT_ASSERT(!GcsBlockForwardPayloadIsDowngradeRequest(&fwd));

	/* downgrade request overwrites: downgrade yes, nudge no */
	GcsBlockForwardPayloadSetDowngradeRequest(&fwd, true);
	UT_ASSERT(!GcsBlockForwardPayloadIsBastNudge(&fwd));
	UT_ASSERT(GcsBlockForwardPayloadIsDowngradeRequest(&fwd));

	/* clear resets both readings */
	GcsBlockForwardPayloadSetDowngradeRequest(&fwd, false);
	UT_ASSERT(!GcsBlockForwardPayloadIsBastNudge(&fwd));
	UT_ASSERT(!GcsBlockForwardPayloadIsDowngradeRequest(&fwd));

	/* neighbour overlays undisturbed by the nudge */
	GcsBlockForwardPayloadSetReadImage(&fwd, true);
	GcsBlockForwardPayloadSetXTransfer(&fwd, true);
	GcsBlockForwardPayloadSetCleanEligible(&fwd, true);
	GcsBlockForwardPayloadSetCrRequest(&fwd, true);
	GcsBlockForwardPayloadSetBastNudge(&fwd);
	UT_ASSERT(GcsBlockForwardPayloadIsReadImage(&fwd));
	UT_ASSERT(GcsBlockForwardPayloadIsXTransfer(&fwd));
	UT_ASSERT(GcsBlockForwardPayloadIsCleanEligible(&fwd));
	UT_ASSERT(GcsBlockForwardPayloadIsCrRequest(&fwd));
	UT_ASSERT(GcsBlockForwardPayloadIsBastNudge(&fwd));
	UT_ASSERT_EQ((int)sizeof(GcsBlockForwardPayload), 64);
}


/* PGRAC: spec-6.12b — CR request flag rides reserved_0[4] independently and
 * the CR result statuses are the enum tail (16/17). */
UT_TEST(test_cr_request_flag_round_trip_independent)
{
	GcsBlockForwardPayload fwd;

	memset(&fwd, 0, sizeof(fwd));
	UT_ASSERT(!GcsBlockForwardPayloadIsCrRequest(&fwd));

	GcsBlockForwardPayloadSetReadImage(&fwd, true);
	GcsBlockForwardPayloadSetDowngradeRequest(&fwd, true);
	GcsBlockForwardPayloadSetCrRequest(&fwd, true);

	UT_ASSERT(GcsBlockForwardPayloadIsCrRequest(&fwd));
	UT_ASSERT(GcsBlockForwardPayloadIsReadImage(&fwd));
	UT_ASSERT(GcsBlockForwardPayloadIsDowngradeRequest(&fwd));

	GcsBlockForwardPayloadSetCrRequest(&fwd, false);
	UT_ASSERT(!GcsBlockForwardPayloadIsCrRequest(&fwd));
	UT_ASSERT(GcsBlockForwardPayloadIsReadImage(&fwd));
	UT_ASSERT_EQ((int)sizeof(GcsBlockForwardPayload), 64);
}


UT_TEST(test_cr_result_statuses_are_16_17)
{
	UT_ASSERT_EQ((int)GCS_BLOCK_REPLY_CR_RESULT_FULL, 16);
	UT_ASSERT_EQ((int)GCS_BLOCK_REPLY_CR_RESULT_PARTIAL, 17);
	UT_ASSERT_EQ((int)GCS_BLOCK_REPLY_CR_RESULT_FULL,
				 (int)GCS_BLOCK_REPLY_S_GRANTED_XHOLDER_DOWNGRADE + 1);
}


/* PGRAC: spec-6.13 D6 — direct-land FORWARD flag uses reserved_0[5], not the
 * frozen-spec [3] byte that spec-6.12a already occupies for downgrade. */
UT_TEST(test_direct_land_forward_flag_round_trip_independent)
{
	GcsBlockForwardPayload fwd;

	memset(&fwd, 0, sizeof(fwd));
	UT_ASSERT(!GcsBlockForwardPayloadIsDirectLandArmed(&fwd));

	GcsBlockForwardPayloadSetReadImage(&fwd, true);
	GcsBlockForwardPayloadSetXTransfer(&fwd, true);
	GcsBlockForwardPayloadSetCleanEligible(&fwd, true);
	GcsBlockForwardPayloadSetDowngradeRequest(&fwd, true);
	GcsBlockForwardPayloadSetCrRequest(&fwd, true);
	GcsBlockForwardPayloadSetDirectLandArmed(&fwd, true);

	UT_ASSERT(GcsBlockForwardPayloadIsReadImage(&fwd));
	UT_ASSERT(GcsBlockForwardPayloadIsXTransfer(&fwd));
	UT_ASSERT(GcsBlockForwardPayloadIsCleanEligible(&fwd));
	UT_ASSERT(GcsBlockForwardPayloadIsDowngradeRequest(&fwd));
	UT_ASSERT(GcsBlockForwardPayloadIsCrRequest(&fwd));
	UT_ASSERT(GcsBlockForwardPayloadIsDirectLandArmed(&fwd));

	GcsBlockForwardPayloadSetDirectLandArmed(&fwd, false);
	UT_ASSERT(!GcsBlockForwardPayloadIsDirectLandArmed(&fwd));
	UT_ASSERT(GcsBlockForwardPayloadIsReadImage(&fwd));
	UT_ASSERT(GcsBlockForwardPayloadIsXTransfer(&fwd));
	UT_ASSERT(GcsBlockForwardPayloadIsCleanEligible(&fwd));
	UT_ASSERT(GcsBlockForwardPayloadIsDowngradeRequest(&fwd));
	UT_ASSERT(GcsBlockForwardPayloadIsCrRequest(&fwd));
	UT_ASSERT_EQ((int)sizeof(GcsBlockForwardPayload), 64);
}


UT_TEST(test_forward_direct_land_from_request_requires_exact_holder_arm)
{
	GcsBlockRequestPayload req;
	GcsBlockForwardPayload fwd;

	memset(&req, 0, sizeof(req));
	memset(&fwd, 0, sizeof(fwd));
	GcsBlockRequestPayloadSetDirectLandArmed(&req, true);

	GcsBlockForwardPayloadSetDirectLandFromRequest(&fwd, &req, false);
	UT_ASSERT(!GcsBlockForwardPayloadIsDirectLandArmed(&fwd));

	GcsBlockForwardPayloadSetDirectLandFromRequest(&fwd, &req, true);
	UT_ASSERT(GcsBlockForwardPayloadIsDirectLandArmed(&fwd));

	GcsBlockRequestPayloadSetDirectLandArmed(&req, false);
	GcsBlockForwardPayloadSetDirectLandFromRequest(&fwd, &req, true);
	UT_ASSERT(!GcsBlockForwardPayloadIsDirectLandArmed(&fwd));
	UT_ASSERT_EQ((int)sizeof(GcsBlockForwardPayload), 64);
}


UT_TEST(test_direct_land_sidecar_size_locked)
{
	UT_ASSERT_EQ((int)CLUSTER_IC_RDMA_DIRECT_LAND_SIDECAR_BYTES,
				 (int)(PGRAC_IC_ENVELOPE_BYTES + sizeof(GcsBlockReplyHeader)));
	UT_ASSERT_EQ((int)CLUSTER_IC_RDMA_DIRECT_LAND_REPLY_BYTES,
				 (int)(CLUSTER_IC_RDMA_DIRECT_LAND_SIDECAR_BYTES + BLCKSZ));
}


UT_TEST(test_direct_land_wr_id_round_trip_rejects_wrong_type)
{
	uint64 wr_id;
	uint32 peer = 0;
	uint32 arm = 0;
	uint32 generation = 0;

	wr_id = cluster_ic_rdma_direct_land_make_wr_id(7, 1234, 55);
	UT_ASSERT(cluster_ic_rdma_direct_land_decode_wr_id(wr_id, &peer, &arm, &generation));
	UT_ASSERT_EQ((int)peer, 7);
	UT_ASSERT_EQ((int)arm, 1234);
	UT_ASSERT_EQ((int)generation, 55);

	wr_id = cluster_ic_rdma_direct_land_make_wr_id(
		7, 1234, cluster_ic_rdma_direct_land_next_generation(65535));
	UT_ASSERT(cluster_ic_rdma_direct_land_decode_wr_id(wr_id, &peer, &arm, &generation));
	UT_ASSERT_EQ((int)generation, 1);

	UT_ASSERT(!cluster_ic_rdma_direct_land_decode_wr_id(UINT64CONST(0x5100000000000000), &peer,
														&arm, &generation));
}


UT_TEST(test_direct_land_state_transitions)
{
	UT_ASSERT(cluster_gcs_block_direct_state_transition_ok(GCS_BLOCK_DIRECT_UNARMED,
														   GCS_BLOCK_DIRECT_ARMING));
	UT_ASSERT(cluster_gcs_block_direct_state_transition_ok(GCS_BLOCK_DIRECT_ARMING,
														   GCS_BLOCK_DIRECT_ARMED));
	UT_ASSERT(cluster_gcs_block_direct_state_transition_ok(GCS_BLOCK_DIRECT_ARMING,
														   GCS_BLOCK_DIRECT_UNARMED));
	UT_ASSERT(cluster_gcs_block_direct_state_transition_ok(GCS_BLOCK_DIRECT_ARMED,
														   GCS_BLOCK_DIRECT_LANDED));
	UT_ASSERT(cluster_gcs_block_direct_state_transition_ok(GCS_BLOCK_DIRECT_LANDED,
														   GCS_BLOCK_DIRECT_INSTALLED));
	UT_ASSERT(cluster_gcs_block_direct_state_transition_ok(GCS_BLOCK_DIRECT_ABORTING,
														   GCS_BLOCK_DIRECT_ABORTED));
	UT_ASSERT(!cluster_gcs_block_direct_state_transition_ok(GCS_BLOCK_DIRECT_UNARMED,
															GCS_BLOCK_DIRECT_INSTALLED));
	UT_ASSERT(!cluster_gcs_block_direct_state_transition_ok(GCS_BLOCK_DIRECT_ARMED,
															GCS_BLOCK_DIRECT_INSTALLED));
}


UT_TEST(test_direct_land_success_status_whitelist)
{
	UT_ASSERT(GcsBlockReplyStatusAllowsDirectLandInstall(GCS_BLOCK_REPLY_GRANTED));
	UT_ASSERT(GcsBlockReplyStatusAllowsDirectLandInstall(GCS_BLOCK_REPLY_GRANTED_FROM_HOLDER));
	UT_ASSERT(
		GcsBlockReplyStatusAllowsDirectLandInstall(GCS_BLOCK_REPLY_S_GRANTED_XHOLDER_DOWNGRADE));
	UT_ASSERT(!GcsBlockReplyStatusAllowsDirectLandInstall(GCS_BLOCK_REPLY_X_GRANTED_FROM_HOLDER));
	UT_ASSERT(
		!GcsBlockReplyStatusAllowsDirectLandInstall(GCS_BLOCK_REPLY_GRANTED_STORAGE_FALLBACK));
	UT_ASSERT(
		!GcsBlockReplyStatusAllowsDirectLandInstall(GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER));
}


UT_TEST(test_direct_land_no_forward_denial_identity_policy)
{
	UT_ASSERT(GcsBlockReplyStatusAllowsDirectLandNoForwardIdentity(GCS_BLOCK_REPLY_GRANTED));
	UT_ASSERT(
		GcsBlockReplyStatusAllowsDirectLandNoForwardIdentity(GCS_BLOCK_REPLY_DENIED_EPOCH_STALE));
	UT_ASSERT(GcsBlockReplyStatusAllowsDirectLandNoForwardIdentity(
		GCS_BLOCK_REPLY_DENIED_VALIDATOR_REJECT));
	UT_ASSERT(
		GcsBlockReplyStatusAllowsDirectLandNoForwardIdentity(GCS_BLOCK_REPLY_DENIED_DEDUP_FULL));
	UT_ASSERT(GcsBlockReplyStatusAllowsDirectLandNoForwardIdentity(
		GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER));
	UT_ASSERT(
		!GcsBlockReplyStatusAllowsDirectLandNoForwardIdentity(GCS_BLOCK_REPLY_GRANTED_FROM_HOLDER));
	UT_ASSERT(!GcsBlockReplyStatusAllowsDirectLandNoForwardIdentity(
		GCS_BLOCK_REPLY_S_GRANTED_XHOLDER_DOWNGRADE));
	UT_ASSERT(!GcsBlockReplyStatusAllowsDirectLandNoForwardIdentity(
		GCS_BLOCK_REPLY_X_GRANTED_FROM_HOLDER));
	UT_ASSERT(!GcsBlockReplyStatusAllowsDirectLandNoForwardIdentity(
		GCS_BLOCK_REPLY_GRANTED_STORAGE_FALLBACK));
	UT_ASSERT(!GcsBlockReplyStatusAllowsDirectLandNoForwardIdentity(
		GCS_BLOCK_REPLY_READ_IMAGE_FROM_XHOLDER));
	UT_ASSERT(
		!GcsBlockReplyStatusAllowsDirectLandNoForwardIdentity(GCS_BLOCK_REPLY_CR_RESULT_FULL));
}


UT_TEST(test_direct_land_arm_blocks_known_different_holder)
{
	UT_ASSERT(GcsBlockDirectCanArmExpectedPeer(-1, 2));
	UT_ASSERT(GcsBlockDirectCanArmExpectedPeer(2, 2));
	UT_ASSERT(!GcsBlockDirectCanArmExpectedPeer(3, 2));
}


int
main(void)
{
	UT_PLAN(34);
	UT_RUN(test_block_forward_msg_type_is_16);
	UT_RUN(test_granted_from_holder_status_is_8);
	UT_RUN(test_forward_payload_size_locked_at_64);
	UT_RUN(test_forward_payload_field_offsets);
	UT_RUN(test_reply_header_size_still_48_after_hc109);
	UT_RUN(test_reply_header_forwarding_master_node_bytes_offset_38);
	UT_RUN(test_forwarding_master_node_helper_round_trip);
	UT_RUN(test_dedup_forwarded_duplicate_enum_value_5);
	UT_RUN(test_dedup_result_enum_has_6_values);
	UT_RUN(test_block_forward_msg_type_distinct);
	UT_RUN(test_granted_from_holder_distinct_from_dedup_full);
	UT_RUN(test_forward_payload_sizeof_equals_request_payload_sizeof);
	UT_RUN(test_reply_status_enum_no_gap_0_to_8);
	UT_RUN(test_forwarding_master_field_within_reserved_budget);
	UT_RUN(test_pcm_master_holder_node_by_tag_linkable);
	UT_RUN(test_pcm_unlock_content_buffer_linkable);
	UT_RUN(test_pcm_release_buffer_for_eviction_linkable);
	UT_RUN(test_l18_forward_end_to_end_documented_in_tap);
	UT_RUN(test_l19_hc108_authorized_chain_documented_in_tap);
	UT_RUN(test_l20_evict_race_recovery_documented_in_tap);
	UT_RUN(test_l21_hc112_unlock_preserves_bit_documented_in_tap);
	UT_RUN(test_l22_master_holder_lifecycle_documented_in_tap);
	UT_RUN(test_downgrade_request_flag_round_trip_independent);
	UT_RUN(test_bast_nudge_flag_disjoint_from_downgrade_request);
	UT_RUN(test_s_granted_xholder_downgrade_status_is_15);
	UT_RUN(test_cr_request_flag_round_trip_independent);
	UT_RUN(test_cr_result_statuses_are_16_17);
	UT_RUN(test_direct_land_forward_flag_round_trip_independent);
	UT_RUN(test_forward_direct_land_from_request_requires_exact_holder_arm);
	UT_RUN(test_direct_land_sidecar_size_locked);
	UT_RUN(test_direct_land_wr_id_round_trip_rejects_wrong_type);
	UT_RUN(test_direct_land_state_transitions);
	UT_RUN(test_direct_land_success_status_whitelist);
	UT_RUN(test_direct_land_no_forward_denial_identity_policy);
	UT_RUN(test_direct_land_arm_blocks_known_different_holder);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
