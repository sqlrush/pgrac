/*-------------------------------------------------------------------------
 *
 * test_cluster_gcs_block.c
 *	  Compile-time invariants for the spec-2.33 GCS block-shipping
 *	  substrate (cluster_gcs_block.h wire ABI + math invariants).
 *
 *	  spec-2.33 ships cross-node 8KB block shipping on top of the spec-2.32
 *	  control plane.  This unit binary checks compile-time invariants
 *	  (struct sizes, field offsets, enum values, hash math) that are
 *	  verifiable from headers alone — no linking of cluster_gcs_block.o.
 *	  Symbol linkability + behavioral coverage (XLogFlush invocation order,
 *	  checksum fail-closed, PageSetLSN install, HC89 single-retry
 *	  revalidation, sparse-topology end-to-end ship) lives in cluster_tap
 *	  t/111_gcs_block_ship_2node.pl which exercises a real PG instance.
 *
 *	  Tests in this binary (L1-L15):
 *	    L1  msg_type enum values (BLOCK_REQUEST=14, BLOCK_REPLY=15;
 *	         spec-2.32 GCS_REQUEST=12 / REPLY=13 preserved;
 *	         CSSD_HEARTBEAT=11 preserved)
 *	    L2  payload struct sizes + GCS_BLOCK_DATA_SIZE locked
 *	    L3  GcsBlockRequestPayload field offsets (64B layout — Sprint A
 *	         SA-F1 PG-fact:  natural 8-aligned struct size, reserved_0[19]
 *	         absorbs the trailing pad to 64B)
 *	    L4  GcsBlockReplyHeader field offsets (48B layout; spec-2.35
 *	         reuses 4 bytes of the original reserved budget for
 *	         forwarding_master_node_bytes)
 *	    L5  GcsBlockReplyStatus enum values through spec-2.35
 *	    L6  sparse node_id topology — hash mod-N over declared array
 *	         {0,2,5} only returns those three node_ids (HC81 math)
 *	    L7  deterministic hash determinism — same tag returns same master
 *	         on 32 repeated invocations
 *	    L8  LWTRANCHE_CLUSTER_GCS_BLOCK enum value distinct from GCS / PCM
 *	    L9  4 NEW wait events distinct from each other + GCS_REPLY_WAIT
 *	    L10 GCS_BLOCK_REPLY_PAYLOAD_TOTAL_SIZE math = header (48) + 8192
 *	    L11 reply key compound (backend_id, request_id) — HC80 layout
 *	    L12 reserved_0 padding bytes zero after memset(0)
 *	    L13 BLCKSZ == GCS_BLOCK_DATA_SIZE invariant (defensive cross-check
 *	         of the StaticAssertDecl in cluster_gcs_block.h)
 *	    L14 ClusterICMsgType enum extends without gap (spec-2.32 12/13
 *	         consecutive with spec-2.33 14/15)
 *	    L15 GcsBlockRequestPayload.tag is the standard PG BufferTag 20B
 *	         (defensive cross-check that PG-fact BufferTag size is unchanged)
 *
 *	  Sprint A finding (SA-F2):  spec §4.1 calls for 26 unit tests
 *	  (L1-L26).  Many of L18-L26 are behavioral (HC82 XLogFlush invocation
 *	  order via test hook, HC83 checksum fail-closed buffer no-pollute,
 *	  HC84 PageSetLSN install on GRANTED, HC88 master-not-holder paths,
 *	  HC89 single-retry revalidation) which require a live backend +
 *	  cluster_gcs_block_test_xlog_flush_hook + lsn_drift_hook execution.
 *	  Those move to cluster_tap t/111;  this unit binary keeps 15 tests
 *	  covering compile-time invariants.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_gcs_block.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <stddef.h>

#include "cluster/cluster_cssd.h"
#include "cluster/cluster_gcs.h"
#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_ic_envelope.h"
#include "cluster/cluster_thread_recovery.h"
#include "common/hashfn.h"
#include "storage/buf_internals.h"
#include "storage/lwlock.h"
#include "utils/wait_event.h"

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


UT_TEST(test_gcs_block_msg_type_enum_values_no_collision)
{
	UT_ASSERT_EQ((int)PGRAC_IC_MSG_GCS_BLOCK_REQUEST, 14);
	UT_ASSERT_EQ((int)PGRAC_IC_MSG_GCS_BLOCK_REPLY, 15);
	UT_ASSERT_EQ((int)PGRAC_IC_MSG_GCS_REQUEST, 12);
	UT_ASSERT_EQ((int)PGRAC_IC_MSG_GCS_REPLY, 13);
	UT_ASSERT_EQ((int)PGRAC_IC_MSG_CSSD_HEARTBEAT, 11);
	UT_ASSERT_EQ((int)PGRAC_IC_MSG_CF_BLOCK_SHIP, 6);
}


UT_TEST(test_gcs_block_payload_sizes_locked)
{
	UT_ASSERT_EQ((int)sizeof(GcsBlockRequestPayload), 64);
	UT_ASSERT_EQ((int)sizeof(GcsBlockReplyHeader), 48);
	UT_ASSERT_EQ((int)GCS_BLOCK_DATA_SIZE, 8192);
}


UT_TEST(test_gcs_block_request_field_offsets)
{
	UT_ASSERT_EQ((int)offsetof(GcsBlockRequestPayload, request_id), 0);
	UT_ASSERT_EQ((int)offsetof(GcsBlockRequestPayload, epoch), 8);
	UT_ASSERT_EQ((int)offsetof(GcsBlockRequestPayload, tag), 16);
	UT_ASSERT_EQ((int)offsetof(GcsBlockRequestPayload, sender_node), 36);
	UT_ASSERT_EQ((int)offsetof(GcsBlockRequestPayload, requester_backend_id), 40);
	UT_ASSERT_EQ((int)offsetof(GcsBlockRequestPayload, transition_id), 44);
	UT_ASSERT_EQ((int)offsetof(GcsBlockRequestPayload, reserved_0), 45);
}


UT_TEST(test_gcs_block_reply_header_field_offsets)
{
	UT_ASSERT_EQ((int)offsetof(GcsBlockReplyHeader, request_id), 0);
	UT_ASSERT_EQ((int)offsetof(GcsBlockReplyHeader, page_lsn), 8);
	UT_ASSERT_EQ((int)offsetof(GcsBlockReplyHeader, epoch), 16);
	UT_ASSERT_EQ((int)offsetof(GcsBlockReplyHeader, checksum), 24);
	UT_ASSERT_EQ((int)offsetof(GcsBlockReplyHeader, sender_node), 28);
	UT_ASSERT_EQ((int)offsetof(GcsBlockReplyHeader, requester_backend_id), 32);
	UT_ASSERT_EQ((int)offsetof(GcsBlockReplyHeader, transition_id), 36);
	UT_ASSERT_EQ((int)offsetof(GcsBlockReplyHeader, status), 37);
	UT_ASSERT_EQ((int)offsetof(GcsBlockReplyHeader, forwarding_master_node_bytes), 38);
	UT_ASSERT_EQ((int)offsetof(GcsBlockReplyHeader, reserved_0), 42);
}


UT_TEST(test_gcs_block_reply_status_enum_values_through_spec_2_35)
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


UT_TEST(test_gcs_block_sparse_hash_mod_n_distribution)
{
	const int declared[3] = { 0, 2, 5 };
	const int declared_count = 3;
	int i;

	for (i = 0; i < 100; i++) {
		BufferTag tag = { 0 };
		uint32 h;
		int master;

		tag.spcOid = (Oid)(i * 13 + 1);
		tag.dbOid = (Oid)(i * 17 + 1);
		tag.blockNum = (BlockNumber)(i * 19);

		h = hash_bytes((const unsigned char *)&tag, sizeof(tag));
		master = declared[h % (uint32)declared_count];

		UT_ASSERT(master == 0 || master == 2 || master == 5);
	}
}


UT_TEST(test_gcs_block_hash_deterministic_same_tag_same_master)
{
	const int declared[4] = { 0, 1, 2, 3 };
	const int declared_count = 4;
	BufferTag tag = { 0 };
	uint32 h1;
	int m1;
	int i;

	tag.spcOid = (Oid)0x12345;
	tag.dbOid = (Oid)0x6789a;
	tag.blockNum = (BlockNumber)42;

	h1 = hash_bytes((const unsigned char *)&tag, sizeof(tag));
	m1 = declared[h1 % (uint32)declared_count];
	for (i = 0; i < 32; i++) {
		uint32 h2 = hash_bytes((const unsigned char *)&tag, sizeof(tag));
		int m2 = declared[h2 % (uint32)declared_count];

		UT_ASSERT_EQ(m1, m2);
	}
}


UT_TEST(test_gcs_block_lwlock_tranche_distinct)
{
	UT_ASSERT((int)LWTRANCHE_CLUSTER_GCS_BLOCK != (int)LWTRANCHE_CLUSTER_GCS);
	UT_ASSERT((int)LWTRANCHE_CLUSTER_GCS_BLOCK != (int)LWTRANCHE_CLUSTER_PCM);
}


UT_TEST(test_gcs_block_wait_events_distinct)
{
	UT_ASSERT((int)WAIT_EVENT_GCS_BLOCK_SHIP_WAIT != (int)WAIT_EVENT_GCS_REPLY_WAIT);
	UT_ASSERT((int)WAIT_EVENT_GCS_BLOCK_REQUEST_DISPATCH != (int)WAIT_EVENT_GCS_BLOCK_SHIP_WAIT);
	UT_ASSERT((int)WAIT_EVENT_GCS_BLOCK_REPLY_DISPATCH
			  != (int)WAIT_EVENT_GCS_BLOCK_REQUEST_DISPATCH);
	UT_ASSERT((int)WAIT_EVENT_GCS_BLOCK_CHECKSUM_FAIL != (int)WAIT_EVENT_GCS_BLOCK_REPLY_DISPATCH);
}


UT_TEST(test_gcs_block_reply_total_size_is_8240)
{
	UT_ASSERT_EQ((int)GCS_BLOCK_REPLY_PAYLOAD_TOTAL_SIZE, 8240);
	UT_ASSERT_EQ((int)GCS_BLOCK_REPLY_PAYLOAD_TOTAL_SIZE,
				 (int)(sizeof(GcsBlockReplyHeader) + GCS_BLOCK_DATA_SIZE));
}


UT_TEST(test_gcs_block_reply_key_is_compound)
{
	GcsBlockReplyHeader hdr;

	memset(&hdr, 0xab, sizeof(hdr));
	UT_ASSERT_EQ((int)offsetof(GcsBlockReplyHeader, request_id), 0);
	UT_ASSERT_EQ((int)offsetof(GcsBlockReplyHeader, requester_backend_id), 32);
	UT_ASSERT_EQ((int)sizeof(hdr.request_id), 8);
	UT_ASSERT_EQ((int)sizeof(hdr.requester_backend_id), 4);
}


UT_TEST(test_gcs_block_reserved_padding_present)
{
	GcsBlockRequestPayload req;
	GcsBlockReplyHeader rep;

	UT_ASSERT_EQ((int)sizeof(req.reserved_0), 19);
	UT_ASSERT_EQ((int)sizeof(rep.forwarding_master_node_bytes), 4);
	UT_ASSERT_EQ((int)sizeof(rep.reserved_0), 6);
	memset(&req, 0, sizeof(req));
	memset(&rep, 0, sizeof(rep));
	UT_ASSERT_EQ((int)req.reserved_0[0], 0);
	UT_ASSERT_EQ((int)req.reserved_0[18], 0);
	UT_ASSERT_EQ((int)rep.forwarding_master_node_bytes[0], 0);
	UT_ASSERT_EQ((int)rep.forwarding_master_node_bytes[3], 0);
	UT_ASSERT_EQ((int)rep.reserved_0[0], 0);
	UT_ASSERT_EQ((int)rep.reserved_0[5], 0);
}


UT_TEST(test_gcs_block_data_size_equals_blcksz)
{
	UT_ASSERT_EQ((int)GCS_BLOCK_DATA_SIZE, (int)BLCKSZ);
}


UT_TEST(test_gcs_block_msg_type_enum_extends_without_gap)
{
	UT_ASSERT_EQ((int)PGRAC_IC_MSG_GCS_REPLY + 1, (int)PGRAC_IC_MSG_GCS_BLOCK_REQUEST);
	UT_ASSERT_EQ((int)PGRAC_IC_MSG_GCS_BLOCK_REQUEST + 1, (int)PGRAC_IC_MSG_GCS_BLOCK_REPLY);
}


UT_TEST(test_gcs_block_tag_is_standard_buffer_tag_20b)
{
	GcsBlockRequestPayload req;

	/* BufferTag in PG 16 is 5×uint32 = 20B (spec-2.30 v0.2 F1 PG-fact). */
	UT_ASSERT_EQ((int)sizeof(req.tag), 20);
	UT_ASSERT_EQ((int)sizeof(BufferTag), 20);
}


/* spec-5.2 D2 (U3): pure master-side decision for an X-held N→S read.
 * node0 = holder/master in DIRECT, node1 = requester. */
UT_TEST(test_xheld_read_ship_decision_truth_table)
{
	/* N→S read, block held X, master(0) is the resident holder → direct ship. */
	UT_ASSERT_EQ(gcs_block_xheld_read_ship_decision((uint8)PCM_TRANS_N_TO_S, (int)PCM_LOCK_MODE_X,
													0 /*holder*/, 1 /*requester*/, 0 /*master*/,
													true /*resident*/),
				 GCS_XHELD_READ_DIRECT_FROM_MASTER);

	/* Holder(0) is a different node from the master(1) → forward to holder. */
	UT_ASSERT_EQ(gcs_block_xheld_read_ship_decision((uint8)PCM_TRANS_N_TO_S, (int)PCM_LOCK_MODE_X,
													0 /*holder*/, 1 /*requester*/,
													1 /*master==requester*/, false),
				 GCS_XHELD_READ_FORWARD_TO_HOLDER);

	/* Master is the recorded holder but the buffer is NOT resident (evict
	 * race) → fail-closed (never a silent stale read). */
	UT_ASSERT_EQ(gcs_block_xheld_read_ship_decision((uint8)PCM_TRANS_N_TO_S, (int)PCM_LOCK_MODE_X,
													0, 1, 0, false /*not resident*/),
				 GCS_XHELD_READ_DENY);

	/* Holder == requester (read-ship to self) → deny. */
	UT_ASSERT_EQ(gcs_block_xheld_read_ship_decision((uint8)PCM_TRANS_N_TO_S, (int)PCM_LOCK_MODE_X,
													1 /*holder==requester*/, 1, 0, true),
				 GCS_XHELD_READ_DENY);

	/* No valid holder → deny. */
	UT_ASSERT_EQ(gcs_block_xheld_read_ship_decision((uint8)PCM_TRANS_N_TO_S, (int)PCM_LOCK_MODE_X,
													-1 /*no holder*/, 1, 0, true),
				 GCS_XHELD_READ_DENY);

	/* Block held S (not X) → not applicable; the existing 2-way share path
	 * handles it. */
	UT_ASSERT_EQ(gcs_block_xheld_read_ship_decision((uint8)PCM_TRANS_N_TO_S, (int)PCM_LOCK_MODE_S,
													0, 1, 0, true),
				 GCS_XHELD_READ_NOT_APPLICABLE);

	/* A write request (N→X) on an X-held block is never a read-image case. */
	UT_ASSERT_EQ(gcs_block_xheld_read_ship_decision((uint8)PCM_TRANS_N_TO_X, (int)PCM_LOCK_MODE_X,
													0, 1, 0, true),
				 GCS_XHELD_READ_NOT_APPLICABLE);
}


/* spec-5.2 D2: read-image forward flag overlays reserved_0[0] without
 * growing the 64B forward wire. */
UT_TEST(test_forward_payload_read_image_flag_roundtrip)
{
	GcsBlockForwardPayload fwd;

	memset(&fwd, 0, sizeof(fwd));
	UT_ASSERT_EQ(GcsBlockForwardPayloadIsReadImage(&fwd) ? 1 : 0, 0);

	GcsBlockForwardPayloadSetReadImage(&fwd, true);
	UT_ASSERT_EQ(GcsBlockForwardPayloadIsReadImage(&fwd) ? 1 : 0, 1);

	GcsBlockForwardPayloadSetReadImage(&fwd, false);
	UT_ASSERT_EQ(GcsBlockForwardPayloadIsReadImage(&fwd) ? 1 : 0, 0);

	/* Setting the flag must not perturb the HC127 watermark bytes. */
	GcsBlockForwardPayloadSetExpectedPiWatermarkScn(&fwd, (SCN)0x1122334455667788ULL);
	GcsBlockForwardPayloadSetReadImage(&fwd, true);
	UT_ASSERT_EQ((long long)GcsBlockForwardPayloadGetExpectedPiWatermarkScn(&fwd),
				 (long long)0x1122334455667788ULL);

	UT_ASSERT_EQ((int)sizeof(GcsBlockForwardPayload), 64);
}


/* spec-5.2a D1 (U2): clean-page X-transfer eligibility flag.  The request
 * payload carries it in reserved_0[0] (free) and the forward payload in
 * reserved_0[2] (v0.3 P0 FIX — reserved_0[0]/[1] are already the spec-5.2
 * read-image / X-transfer flags, so the forward eligibility flag MUST NOT
 * reuse them).  This test pins the roundtrip AND the three-way orthogonality:
 * setting clean-eligible must never perturb read-image or X-transfer, and
 * vice versa.  ABI stays 64B for both payloads. */
UT_TEST(test_clean_page_xfer_eligible_flag_roundtrip_and_orthogonal)
{
	GcsBlockRequestPayload req;
	GcsBlockForwardPayload fwd;

	/* request-side roundtrip (reserved_0[0]). */
	memset(&req, 0, sizeof(req));
	UT_ASSERT_EQ(GcsBlockRequestPayloadIsCleanEligible(&req) ? 1 : 0, 0);
	GcsBlockRequestPayloadSetCleanEligible(&req, true);
	UT_ASSERT_EQ(GcsBlockRequestPayloadIsCleanEligible(&req) ? 1 : 0, 1);
	GcsBlockRequestPayloadSetCleanEligible(&req, false);
	UT_ASSERT_EQ(GcsBlockRequestPayloadIsCleanEligible(&req) ? 1 : 0, 0);

	/* forward-side roundtrip (reserved_0[2]). */
	memset(&fwd, 0, sizeof(fwd));
	UT_ASSERT_EQ(GcsBlockForwardPayloadIsCleanEligible(&fwd) ? 1 : 0, 0);
	GcsBlockForwardPayloadSetCleanEligible(&fwd, true);
	UT_ASSERT_EQ(GcsBlockForwardPayloadIsCleanEligible(&fwd) ? 1 : 0, 1);
	GcsBlockForwardPayloadSetCleanEligible(&fwd, false);
	UT_ASSERT_EQ(GcsBlockForwardPayloadIsCleanEligible(&fwd) ? 1 : 0, 0);

	/* Orthogonality: clean-eligible vs read-image[0] vs X-transfer[1]. */
	memset(&fwd, 0, sizeof(fwd));
	GcsBlockForwardPayloadSetReadImage(&fwd, true);
	GcsBlockForwardPayloadSetXTransfer(&fwd, true);
	GcsBlockForwardPayloadSetCleanEligible(&fwd, true);
	UT_ASSERT_EQ(GcsBlockForwardPayloadIsReadImage(&fwd) ? 1 : 0, 1);
	UT_ASSERT_EQ(GcsBlockForwardPayloadIsXTransfer(&fwd) ? 1 : 0, 1);
	UT_ASSERT_EQ(GcsBlockForwardPayloadIsCleanEligible(&fwd) ? 1 : 0, 1);

	/* Clearing clean-eligible leaves read-image / X-transfer untouched. */
	GcsBlockForwardPayloadSetCleanEligible(&fwd, false);
	UT_ASSERT_EQ(GcsBlockForwardPayloadIsReadImage(&fwd) ? 1 : 0, 1);
	UT_ASSERT_EQ(GcsBlockForwardPayloadIsXTransfer(&fwd) ? 1 : 0, 1);
	UT_ASSERT_EQ(GcsBlockForwardPayloadIsCleanEligible(&fwd) ? 1 : 0, 0);

	/* Clearing read-image / X-transfer leaves clean-eligible untouched. */
	GcsBlockForwardPayloadSetCleanEligible(&fwd, true);
	GcsBlockForwardPayloadSetReadImage(&fwd, false);
	GcsBlockForwardPayloadSetXTransfer(&fwd, false);
	UT_ASSERT_EQ(GcsBlockForwardPayloadIsReadImage(&fwd) ? 1 : 0, 0);
	UT_ASSERT_EQ(GcsBlockForwardPayloadIsXTransfer(&fwd) ? 1 : 0, 0);
	UT_ASSERT_EQ(GcsBlockForwardPayloadIsCleanEligible(&fwd) ? 1 : 0, 1);

	/* Setting the forward clean flag must not perturb the HC127 watermark. */
	GcsBlockForwardPayloadSetExpectedPiWatermarkScn(&fwd, (SCN)0x1122334455667788ULL);
	GcsBlockForwardPayloadSetCleanEligible(&fwd, true);
	UT_ASSERT_EQ((long long)GcsBlockForwardPayloadGetExpectedPiWatermarkScn(&fwd),
				 (long long)0x1122334455667788ULL);

	/* Both payloads stay 64B. */
	UT_ASSERT_EQ((int)sizeof(GcsBlockRequestPayload), 64);
	UT_ASSERT_EQ((int)sizeof(GcsBlockForwardPayload), 64);
}


/* spec-6.13 D6: request-side direct-land flag rides reserved_0[1] and must
 * stay independent from the existing clean-page eligibility flag at [0]. */
UT_TEST(test_request_payload_direct_land_flag_roundtrip_and_orthogonal)
{
	GcsBlockRequestPayload req;

	memset(&req, 0, sizeof(req));
	UT_ASSERT_EQ(GcsBlockRequestPayloadIsCleanEligible(&req) ? 1 : 0, 0);
	UT_ASSERT_EQ(GcsBlockRequestPayloadIsDirectLandArmed(&req) ? 1 : 0, 0);

	GcsBlockRequestPayloadSetCleanEligible(&req, true);
	GcsBlockRequestPayloadSetDirectLandArmed(&req, true);
	UT_ASSERT_EQ(GcsBlockRequestPayloadIsCleanEligible(&req) ? 1 : 0, 1);
	UT_ASSERT_EQ(GcsBlockRequestPayloadIsDirectLandArmed(&req) ? 1 : 0, 1);

	GcsBlockRequestPayloadSetDirectLandArmed(&req, false);
	UT_ASSERT_EQ(GcsBlockRequestPayloadIsDirectLandArmed(&req) ? 1 : 0, 0);
	UT_ASSERT_EQ(GcsBlockRequestPayloadIsCleanEligible(&req) ? 1 : 0, 1);

	GcsBlockRequestPayloadSetCleanEligible(&req, false);
	GcsBlockRequestPayloadSetDirectLandArmed(&req, true);
	UT_ASSERT_EQ(GcsBlockRequestPayloadIsCleanEligible(&req) ? 1 : 0, 0);
	UT_ASSERT_EQ(GcsBlockRequestPayloadIsDirectLandArmed(&req) ? 1 : 0, 1);
	UT_ASSERT_EQ((int)sizeof(GcsBlockRequestPayload), 64);
}


/* spec-5.2a D3 (U3): pure master-side clean-page X-transfer decision, all 5
 * branches.  Master == self runs the handler; args are (x_holder, requester,
 * master). */
UT_TEST(test_clean_xfer_master_decision_5_branches)
{
	/* ① x_holder == requester → idempotent (already holds X). */
	UT_ASSERT_EQ(gcs_block_clean_xfer_master_decision(1 /*holder*/, 1 /*req*/, 0 /*master*/),
				 GCS_CLEAN_XFER_IDEMPOTENT);
	/* ② no holder → storage-fallback. */
	UT_ASSERT_EQ(gcs_block_clean_xfer_master_decision(-1, 1, 0), GCS_CLEAN_XFER_STORAGE_FALLBACK);
	/* ③ x_holder == master(self) → path-B self-ship. */
	UT_ASSERT_EQ(gcs_block_clean_xfer_master_decision(0 /*holder==master*/, 1, 0),
				 GCS_CLEAN_XFER_SELF_SHIP);
	/* ④ other live holder, master == requester → forward to holder. */
	UT_ASSERT_EQ(gcs_block_clean_xfer_master_decision(2 /*other holder*/, 0 /*req==master*/, 0),
				 GCS_CLEAN_XFER_FORWARD_TO_HOLDER);
	/* ⑤ other live holder, master ∉ {req,holder} (3-node third party) → DENY. */
	UT_ASSERT_EQ(gcs_block_clean_xfer_master_decision(2 /*holder*/, 1 /*req*/, 0 /*master*/),
				 GCS_CLEAN_XFER_THIRD_PARTY_DENY);
	/* idempotent wins even when requester would otherwise be a third party
	 * (holder == requester is checked first). */
	UT_ASSERT_EQ(gcs_block_clean_xfer_master_decision(1, 1, 2), GCS_CLEAN_XFER_IDEMPOTENT);
}


/* spec-5.2a D3 (U4): pure stale-holder-break predicate.  Only an eligible
 * request that got a holder DENIED_MASTER_NOT_HOLDER reply breaks the loop via
 * storage-fallback; a non-eligible reply, a timeout (no reply), or any other
 * status does NOT (Rule 8.A: never storage-fallback unless the holder proved it
 * dropped). */
UT_TEST(test_clean_xfer_stale_break_predicate)
{
	/* eligible + got_reply + DENIED_MASTER_NOT_HOLDER → break. */
	UT_ASSERT_EQ(gcs_block_clean_xfer_should_stale_break(
					 true, true, (uint8)GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER)
					 ? 1
					 : 0,
				 1);
	/* NOT eligible → never break (heap transient retransmit path). */
	UT_ASSERT_EQ(gcs_block_clean_xfer_should_stale_break(
					 false, true, (uint8)GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER)
					 ? 1
					 : 0,
				 0);
	/* timeout (no reply) → never break (cannot prove holder dropped). */
	UT_ASSERT_EQ(gcs_block_clean_xfer_should_stale_break(
					 true, false, (uint8)GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER)
					 ? 1
					 : 0,
				 0);
	/* a different reply (e.g. X_GRANTED / READ_IMAGE) → never break. */
	UT_ASSERT_EQ(gcs_block_clean_xfer_should_stale_break(
					 true, true, (uint8)GCS_BLOCK_REPLY_X_GRANTED_FROM_HOLDER)
					 ? 1
					 : 0,
				 0);
	UT_ASSERT_EQ(gcs_block_clean_xfer_should_stale_break(
					 true, true, (uint8)GCS_BLOCK_REPLY_READ_IMAGE_FROM_XHOLDER)
					 ? 1
					 : 0,
				 0);
}


/* spec-5.22d D4-6: reserved_0[6] VALUE 4 = dead-owner AUTHORITY verdict
 * request.  Pins the value-multiplex against the three existing kinds
 * (1 = undo-TT fetch, 2 = derived verdict, 3 = authoritative verdict): the
 * kind-4 predicate must never match 1/2/3 and vice versa, and setting kind 4
 * must not perturb the widened-xid watermark carrier.  ABI stays 64B. */
UT_TEST(test_forward_payload_undo_authority_verdict_kind4)
{
	GcsBlockForwardPayload fwd;

	memset(&fwd, 0, sizeof(fwd));
	UT_ASSERT_EQ(GcsBlockForwardPayloadIsUndoAuthorityVerdictRequest(&fwd) ? 1 : 0, 0);

	GcsBlockForwardPayloadSetUndoAuthorityVerdictRequest(&fwd);
	UT_ASSERT_EQ(GcsBlockForwardPayloadIsUndoAuthorityVerdictRequest(&fwd) ? 1 : 0, 1);
	/* kind 4 is NOT one of the owner-served verdict kinds (2/3), NOT the
	 * undo-TT fetch (1) */
	UT_ASSERT_EQ(GcsBlockForwardPayloadIsUndoVerdictRequest(&fwd) ? 1 : 0, 0);
	UT_ASSERT_EQ(GcsBlockForwardPayloadIsUndoVerdictAuthoritative(&fwd) ? 1 : 0, 0);
	UT_ASSERT_EQ(GcsBlockForwardPayloadIsUndoTtFetchRequest(&fwd) ? 1 : 0, 0);

	/* and the owner-served kinds are NOT the authority kind */
	GcsBlockForwardPayloadSetUndoVerdictRequest(&fwd, true /* value 3 */);
	UT_ASSERT_EQ(GcsBlockForwardPayloadIsUndoAuthorityVerdictRequest(&fwd) ? 1 : 0, 0);
	GcsBlockForwardPayloadSetUndoVerdictRequest(&fwd, false /* value 2 */);
	UT_ASSERT_EQ(GcsBlockForwardPayloadIsUndoAuthorityVerdictRequest(&fwd) ? 1 : 0, 0);
	GcsBlockForwardPayloadSetUndoTtFetchRequest(&fwd, true /* value 1 */);
	UT_ASSERT_EQ(GcsBlockForwardPayloadIsUndoAuthorityVerdictRequest(&fwd) ? 1 : 0, 0);

	/* kind 4 must not perturb the widened-xid carrier bytes */
	memset(&fwd, 0, sizeof(fwd));
	GcsBlockForwardPayloadSetExpectedPiWatermarkScn(&fwd, (SCN)0x00000000AABBCCDDULL);
	GcsBlockForwardPayloadSetUndoAuthorityVerdictRequest(&fwd);
	UT_ASSERT_EQ((long long)GcsBlockForwardPayloadGetExpectedPiWatermarkScn(&fwd),
				 (long long)0x00000000AABBCCDDULL);

	UT_ASSERT_EQ((int)sizeof(GcsBlockForwardPayload), 64);
}


/* spec-5.22d D4-6: the authority fetch tag carries the dead OWNER in the
 * previously-empty tag.relNumber as owner+1 (0 stays "absent" so the three
 * owner-served kinds keep their strict empty-relNumber shape).  The serve
 * side NEVER blind-trusts this field — it re-derives the authority and
 * only answers when the triple check passes — but the encode/decode
 * roundtrip and the 0-absent boundary are pinned here. */
UT_TEST(test_undo_authority_fetch_tag_owner_roundtrip)
{
	BufferTag legacy = GcsBlockUndoFetchTagMake(7, 0);
	BufferTag tag;
	uint32 segment_id = 0;
	uint32 block_no = 99;
	int32 owner = -1;

	/* legacy owner-served tag: relNumber empty, owner decode refuses */
	UT_ASSERT_EQ((int)legacy.relNumber, 0);
	UT_ASSERT_EQ(GcsBlockUndoAuthorityFetchTagDecodeOwner(legacy, &owner) ? 1 : 0, 0);

	/* authority tag: owner 2 rides as relNumber 3; base fields unchanged */
	tag = GcsBlockUndoAuthorityFetchTagMake(7, 0, 2 /* owner */);
	UT_ASSERT_EQ((int)tag.relNumber, 3);
	UT_ASSERT_EQ(GcsBlockUndoFetchTagDecode(tag, &segment_id, &block_no) ? 1 : 0, 1);
	UT_ASSERT_EQ((int)segment_id, 7);
	UT_ASSERT_EQ((int)block_no, 0);
	UT_ASSERT_EQ(GcsBlockUndoAuthorityFetchTagDecodeOwner(tag, &owner) ? 1 : 0, 1);
	UT_ASSERT_EQ((int)owner, 2);

	/* owner 0 (node id 0) must survive the +1 bias roundtrip */
	tag = GcsBlockUndoAuthorityFetchTagMake(1, 0, 0);
	UT_ASSERT_EQ((int)tag.relNumber, 1);
	UT_ASSERT_EQ(GcsBlockUndoAuthorityFetchTagDecodeOwner(tag, &owner) ? 1 : 0, 1);
	UT_ASSERT_EQ((int)owner, 0);

	/* wrong magic: never decodes, wherever the relNumber points */
	tag.spcOid = (Oid)0xDEADBEEF;
	UT_ASSERT_EQ(GcsBlockUndoAuthorityFetchTagDecodeOwner(tag, &owner) ? 1 : 0, 0);
}


/* spec-5.22d D4-6: the authority-served verdict page version is a DISTINCT
 * provenance value — an old requester's strict ==1 gate refuses it (fail
 * closed) and the new authority leg accepts ONLY it (an owner-served v1
 * page can never masquerade as an authority serve). */
UT_TEST(test_undo_verdict_version_authority_distinct)
{
	UT_ASSERT_EQ((int)CLUSTER_GCS_UNDO_VERDICT_VERSION, 1);
	UT_ASSERT_EQ((int)CLUSTER_GCS_UNDO_VERDICT_VERSION_AUTHORITY, 2);
}


int
main(void)
{
	UT_PLAN(24);
	UT_RUN(test_gcs_block_msg_type_enum_values_no_collision);
	UT_RUN(test_gcs_block_payload_sizes_locked);
	UT_RUN(test_gcs_block_request_field_offsets);
	UT_RUN(test_gcs_block_reply_header_field_offsets);
	UT_RUN(test_gcs_block_reply_status_enum_values_through_spec_2_35);
	UT_RUN(test_gcs_block_sparse_hash_mod_n_distribution);
	UT_RUN(test_gcs_block_hash_deterministic_same_tag_same_master);
	UT_RUN(test_gcs_block_lwlock_tranche_distinct);
	UT_RUN(test_gcs_block_wait_events_distinct);
	UT_RUN(test_gcs_block_reply_total_size_is_8240);
	UT_RUN(test_gcs_block_reply_key_is_compound);
	UT_RUN(test_gcs_block_reserved_padding_present);
	UT_RUN(test_gcs_block_data_size_equals_blcksz);
	UT_RUN(test_gcs_block_msg_type_enum_extends_without_gap);
	UT_RUN(test_gcs_block_tag_is_standard_buffer_tag_20b);
	UT_RUN(test_xheld_read_ship_decision_truth_table);
	UT_RUN(test_forward_payload_read_image_flag_roundtrip);
	UT_RUN(test_clean_page_xfer_eligible_flag_roundtrip_and_orthogonal);
	UT_RUN(test_request_payload_direct_land_flag_roundtrip_and_orthogonal);
	UT_RUN(test_clean_xfer_master_decision_5_branches);
	UT_RUN(test_clean_xfer_stale_break_predicate);
	UT_RUN(test_forward_payload_undo_authority_verdict_kind4);
	UT_RUN(test_undo_authority_fetch_tag_owner_roundtrip);
	UT_RUN(test_undo_verdict_version_authority_distinct);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
