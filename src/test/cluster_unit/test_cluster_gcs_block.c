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
#include "cluster/cluster_lmd_wait_state.h"
#include "cluster/cluster_pcm_x_convert.h"
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


static char *
read_source_path(const char *path)
{
	FILE *file;
	long length;
	char *source;

	file = fopen(path, "rb");
	UT_ASSERT_NOT_NULL(file);
	if (file == NULL)
		return NULL;
	UT_ASSERT_EQ(fseek(file, 0, SEEK_END), 0);
	length = ftell(file);
	UT_ASSERT(length > 0);
	UT_ASSERT_EQ(fseek(file, 0, SEEK_SET), 0);
	source = malloc((size_t)length + 1);
	UT_ASSERT_NOT_NULL(source);
	if (source == NULL) {
		fclose(file);
		return NULL;
	}
	UT_ASSERT_EQ(fread(source, 1, (size_t)length, file), (size_t)length);
	source[length] = '\0';
	fclose(file);
	return source;
}


static char *
read_gcs_block_source(void)
{
	return read_source_path(GCS_BLOCK_SOURCE_PATH);
}


static int
count_occurrences(const char *source, const char *needle)
{
	int count = 0;
	size_t needle_length = strlen(needle);

	while ((source = strstr(source, needle)) != NULL) {
		count++;
		source += needle_length;
	}
	return count;
}


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


UT_TEST(test_pcm_x_session_auth_sample_classifies_epoch_zero_and_torn_reads)
{
	ClusterGcsPcmXAuthSample sample;

	memset(&sample, 0, sizeof(sample));
	sample.connection_before_valid = true;
	sample.connection_after_valid = true;
	sample.slot_before_valid = true;
	sample.slot_after_valid = true;
	sample.fresh_before = true;
	sample.fresh_after = true;
	sample.session_before = 41;
	sample.session_after = 41;
	sample.slot_generation_before = 7;
	sample.slot_generation_after = 7;
	/* Both INITIAL epoch 0 and the registered RDMA connection generation 0
	 * are live values, not empty sentinels. */
	UT_ASSERT_EQ(cluster_gcs_pcm_x_auth_sample_classify(&sample, 0), PCM_X_SESSION_AUTH_OK);

	sample.slot_generation_after++;
	UT_ASSERT_EQ(cluster_gcs_pcm_x_auth_sample_classify(&sample, 0), PCM_X_SESSION_AUTH_SLOT_TORN);
	sample.slot_generation_after = sample.slot_generation_before;
	sample.fresh_after = false;
	UT_ASSERT_EQ(cluster_gcs_pcm_x_auth_sample_classify(&sample, 0),
				 PCM_X_SESSION_AUTH_FRESH_NOT_READY);
	sample.fresh_after = true;
	sample.connection_generation_after = 1;
	UT_ASSERT_EQ(cluster_gcs_pcm_x_auth_sample_classify(&sample, 0),
				 PCM_X_SESSION_AUTH_CONNECTION_TORN);
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
 * request.  Pins the value-multiplex against the other kinds (1 = undo-TT
 * fetch, 2 = derived verdict, 3 = MULTI verdict, 5 = authoritative verdict):
 * the kind-4 predicate must never match 1/2/3/5 and vice versa, and setting
 * kind 4 must not perturb the widened-xid watermark carrier.  ABI stays 64B. */
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
	GcsBlockForwardPayloadSetUndoVerdictRequest(&fwd, true /* value 5 */);
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


/* spec-5.22f Hardening (RC#1 integration review): the AUTHORITATIVE single
 * verdict sub-kind (spec-5.22f D6-7) must NOT share reserved_0[6] value 3 with
 * the spec-7.1 D3-b MULTI verdict request.  It originally did, so
 * IsUndoVerdictRequest matched a multi request and the forward handler's
 * single-verdict branch stole it before the multi branch -> a cross-node
 * multixact member serve refused and the requester fail-closed 53R97
 * (t/359_mxid G5 red on the branch, green on main).  Lock the full byte legend
 * (1 fetch / 2 derived / 3 MULTI / 4 authority / 5 authoritative) so the five
 * request kinds stay mutually exclusive across every Is* predicate. */
UT_TEST(test_forward_payload_undo_verdict_kinds_no_collision)
{
	GcsBlockForwardPayload fwd;

	/* MULTI verdict (value 3): matched ONLY by the multi predicate. */
	memset(&fwd, 0, sizeof(fwd));
	GcsBlockForwardPayloadSetUndoMultiVerdictRequest(&fwd, true);
	UT_ASSERT_EQ(GcsBlockForwardPayloadIsUndoMultiVerdictRequest(&fwd) ? 1 : 0, 1);
	UT_ASSERT_EQ(GcsBlockForwardPayloadIsUndoVerdictRequest(&fwd) ? 1 : 0, 0); /* the collision */
	UT_ASSERT_EQ(GcsBlockForwardPayloadIsUndoVerdictAuthoritative(&fwd) ? 1 : 0, 0);
	UT_ASSERT_EQ(GcsBlockForwardPayloadIsUndoAuthorityVerdictRequest(&fwd) ? 1 : 0, 0);
	UT_ASSERT_EQ(GcsBlockForwardPayloadIsUndoTtFetchRequest(&fwd) ? 1 : 0, 0);

	/* AUTHORITATIVE single verdict (value 5): a verdict request, authoritative,
	 * but NOT a multi and NOT the dead-owner authority kind. */
	memset(&fwd, 0, sizeof(fwd));
	GcsBlockForwardPayloadSetUndoVerdictRequest(&fwd, true);
	UT_ASSERT_EQ(GcsBlockForwardPayloadIsUndoVerdictRequest(&fwd) ? 1 : 0, 1);
	UT_ASSERT_EQ(GcsBlockForwardPayloadIsUndoVerdictAuthoritative(&fwd) ? 1 : 0, 1);
	UT_ASSERT_EQ(GcsBlockForwardPayloadIsUndoMultiVerdictRequest(&fwd) ? 1 : 0, 0);
	UT_ASSERT_EQ(GcsBlockForwardPayloadIsUndoAuthorityVerdictRequest(&fwd) ? 1 : 0, 0);
	UT_ASSERT_EQ(GcsBlockForwardPayloadIsUndoTtFetchRequest(&fwd) ? 1 : 0, 0);

	/* DERIVED single verdict (value 2): a verdict request, NOT authoritative,
	 * NOT a multi. */
	memset(&fwd, 0, sizeof(fwd));
	GcsBlockForwardPayloadSetUndoVerdictRequest(&fwd, false);
	UT_ASSERT_EQ(GcsBlockForwardPayloadIsUndoVerdictRequest(&fwd) ? 1 : 0, 1);
	UT_ASSERT_EQ(GcsBlockForwardPayloadIsUndoVerdictAuthoritative(&fwd) ? 1 : 0, 0);
	UT_ASSERT_EQ(GcsBlockForwardPayloadIsUndoMultiVerdictRequest(&fwd) ? 1 : 0, 0);
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


UT_TEST(test_pcm_x_enqueue_ingress_binds_transport_epoch_and_master)
{
	PcmXEnqueuePayload request;

	memset(&request, 0, sizeof(request));
	request.identity.node_id = 1;
	request.identity.procno = 7;
	request.identity.cluster_epoch = 9;
	request.identity.request_id = 11;
	request.identity.wait_seq = 13;
	request.prehandle.sender_session_incarnation = 17;
	request.prehandle.prehandle_sequence = 19;

	UT_ASSERT(cluster_gcs_pcm_x_enqueue_ingress_valid(&request, sizeof(request), 1, 9, 2, 2));
	UT_ASSERT(!cluster_gcs_pcm_x_enqueue_ingress_valid(&request, sizeof(request) - 1, 1, 9, 2, 2));
	UT_ASSERT(!cluster_gcs_pcm_x_enqueue_ingress_valid(&request, sizeof(request), 0, 9, 2, 2));
	UT_ASSERT(!cluster_gcs_pcm_x_enqueue_ingress_valid(&request, sizeof(request), 1, 10, 2, 2));
	UT_ASSERT(!cluster_gcs_pcm_x_enqueue_ingress_valid(&request, sizeof(request), 1, 9, 3, 2));
	request.prehandle.prehandle_sequence = 0;
	UT_ASSERT(!cluster_gcs_pcm_x_enqueue_ingress_valid(&request, sizeof(request), 1, 9, 2, 2));
}

UT_TEST(test_pcm_x_admit_confirm_ingress_binds_requester_and_master)
{
	PcmXPhasePayload phase;

	memset(&phase, 0, sizeof(phase));
	phase.ref.identity.node_id = 1;
	phase.ref.identity.procno = 7;
	phase.ref.identity.cluster_epoch = 9;
	phase.ref.identity.request_id = 11;
	phase.ref.identity.wait_seq = 13;
	phase.ref.handle.ticket_id = 17;
	phase.ref.handle.queue_generation = 19;
	phase.phase = PCM_X_LOCAL_RELIABLE_PHASE_ADMIT_CONFIRM;

	UT_ASSERT(cluster_gcs_pcm_x_admit_confirm_ingress_valid(&phase, sizeof(phase), 1, 9, 2, 2));
	UT_ASSERT(
		!cluster_gcs_pcm_x_admit_confirm_ingress_valid(&phase, sizeof(phase) - 1, 1, 9, 2, 2));
	UT_ASSERT(!cluster_gcs_pcm_x_admit_confirm_ingress_valid(&phase, sizeof(phase), 0, 9, 2, 2));
	phase.reason = 1;
	UT_ASSERT(!cluster_gcs_pcm_x_admit_confirm_ingress_valid(&phase, sizeof(phase), 1, 9, 2, 2));
}

UT_TEST(test_pcm_x_admit_confirm_ack_binds_exact_master_source)
{
	PcmXPhasePayload phase;

	memset(&phase, 0, sizeof(phase));
	phase.ref.identity.node_id = 1;
	phase.ref.identity.procno = 7;
	phase.ref.identity.cluster_epoch = 9;
	phase.ref.identity.request_id = 11;
	phase.ref.identity.wait_seq = 13;
	phase.ref.handle.ticket_id = 17;
	phase.ref.handle.queue_generation = 19;
	phase.phase = PCM_X_LOCAL_RELIABLE_PHASE_ADMIT_CONFIRM;

	UT_ASSERT(cluster_gcs_pcm_x_admit_confirm_ack_ingress_valid(&phase, sizeof(phase), 2, 9, 2, 1));
	UT_ASSERT(
		!cluster_gcs_pcm_x_admit_confirm_ack_ingress_valid(&phase, sizeof(phase), 3, 9, 2, 1));
	phase.ref.grant_generation = 1;
	UT_ASSERT(
		!cluster_gcs_pcm_x_admit_confirm_ack_ingress_valid(&phase, sizeof(phase), 2, 9, 2, 1));
}

UT_TEST(test_pcm_x_cancel_requests_bind_exact_source_epoch_master_and_phase)
{
	PcmXPrehandleCancelPayload prehandle;
	PcmXPhasePayload cancel;

	memset(&prehandle, 0, sizeof(prehandle));
	prehandle.identity.node_id = 1;
	prehandle.identity.procno = 7;
	prehandle.identity.cluster_epoch = 9;
	prehandle.identity.request_id = 11;
	prehandle.identity.wait_seq = 13;
	prehandle.prehandle.sender_session_incarnation = 17;
	prehandle.prehandle.prehandle_sequence = 19;
	UT_ASSERT(cluster_gcs_pcm_x_prehandle_cancel_ingress_valid(&prehandle, sizeof(prehandle), 1, 9,
															   2, 2));
	UT_ASSERT(!cluster_gcs_pcm_x_prehandle_cancel_ingress_valid(&prehandle, sizeof(prehandle), 0, 9,
																2, 2));
	prehandle.prehandle.prehandle_sequence = 0;
	UT_ASSERT(!cluster_gcs_pcm_x_prehandle_cancel_ingress_valid(&prehandle, sizeof(prehandle), 1, 9,
																2, 2));

	memset(&cancel, 0, sizeof(cancel));
	cancel.ref.identity = prehandle.identity;
	cancel.ref.identity.node_id = 1;
	cancel.ref.handle.ticket_id = 23;
	cancel.ref.handle.queue_generation = 29;
	cancel.phase = PCM_X_LOCAL_RELIABLE_PHASE_CANCEL;
	UT_ASSERT(cluster_gcs_pcm_x_cancel_ingress_valid(&cancel, sizeof(cancel), 1, 9, 2, 2));
	cancel.phase = PCM_X_LOCAL_RELIABLE_PHASE_ADMIT_CONFIRM;
	UT_ASSERT(!cluster_gcs_pcm_x_cancel_ingress_valid(&cancel, sizeof(cancel), 1, 9, 2, 2));
}

UT_TEST(test_pcm_x_cancel_acks_bind_exact_master_and_canonical_payload)
{
	PcmXAdmitAckPayload prehandle_ack;
	PcmXPhasePayload cancel_ack;

	memset(&prehandle_ack, 0, sizeof(prehandle_ack));
	prehandle_ack.ref.identity.node_id = 1;
	prehandle_ack.ref.identity.procno = 7;
	prehandle_ack.ref.identity.cluster_epoch = 9;
	prehandle_ack.ref.identity.request_id = 11;
	prehandle_ack.ref.identity.wait_seq = 13;
	prehandle_ack.ref.handle.ticket_id = 17;
	prehandle_ack.ref.handle.queue_generation = 19;
	prehandle_ack.prehandle.sender_session_incarnation = 23;
	prehandle_ack.prehandle.prehandle_sequence = 29;
	prehandle_ack.result = PCM_X_QUEUE_OK;
	prehandle_ack.phase = PCM_X_LOCAL_RELIABLE_PHASE_PREHANDLE_CANCEL;
	UT_ASSERT(cluster_gcs_pcm_x_prehandle_cancel_ack_ingress_valid(
		&prehandle_ack, sizeof(prehandle_ack), 2, 9, 2, 1));
	prehandle_ack.result = PCM_X_QUEUE_DUPLICATE;
	UT_ASSERT(!cluster_gcs_pcm_x_prehandle_cancel_ack_ingress_valid(
		&prehandle_ack, sizeof(prehandle_ack), 2, 9, 2, 1));

	memset(&cancel_ack, 0, sizeof(cancel_ack));
	cancel_ack.ref = prehandle_ack.ref;
	cancel_ack.phase = PCM_X_LOCAL_RELIABLE_PHASE_CANCEL;
	UT_ASSERT(
		cluster_gcs_pcm_x_cancel_ack_ingress_valid(&cancel_ack, sizeof(cancel_ack), 2, 9, 2, 1));
	cancel_ack.ref.grant_generation = 1;
	UT_ASSERT(
		!cluster_gcs_pcm_x_cancel_ack_ingress_valid(&cancel_ack, sizeof(cancel_ack), 2, 9, 2, 1));
}

UT_TEST(test_pcm_x_wait_identity_maps_to_real_wfg_vertex)
{
	PcmXWaitIdentity identity;
	ClusterLmdVertex vertex;

	memset(&identity, 0, sizeof(identity));
	identity.node_id = 2;
	identity.procno = 7;
	identity.xid = 11;
	identity.cluster_epoch = 13;
	identity.request_id = 17;
	identity.wait_seq = 19;
	memset(&vertex, 0xA5, sizeof(vertex));

	cluster_gcs_pcm_x_vertex_from_identity(&identity, &vertex);
	UT_ASSERT_EQ(vertex.node_id, 2);
	UT_ASSERT_EQ(vertex.procno, (uint32)7);
	UT_ASSERT_EQ(vertex.xid, (TransactionId)11);
	UT_ASSERT_EQ(vertex.cluster_epoch, (uint64)13);
	UT_ASSERT_EQ(vertex.request_id, (uint64)17);
	UT_ASSERT_EQ(vertex.wait_seq, (uint64)19);
	UT_ASSERT_EQ(vertex.local_start_ts_ms, (int64)0);
}


UT_TEST(test_pcm_x_initial_epoch_zero_is_exact_across_wire_classes)
{
	ClusterLmdWaitStateSnapshot wait_snapshot;
	PcmXEnqueuePayload enqueue;
	PcmXRetirePayload retire;
	PcmXTicketRef ref;

	memset(&enqueue, 0, sizeof(enqueue));
	enqueue.identity.node_id = 1;
	enqueue.identity.procno = 7;
	enqueue.identity.cluster_epoch = 0;
	enqueue.identity.request_id = 11;
	enqueue.identity.wait_seq = 13;
	enqueue.prehandle.sender_session_incarnation = 17;
	enqueue.prehandle.prehandle_sequence = 19;
	UT_ASSERT(cluster_gcs_pcm_x_enqueue_ingress_valid(&enqueue, sizeof(enqueue), 1, 0, 2, 2));
	UT_ASSERT(!cluster_gcs_pcm_x_enqueue_ingress_valid(&enqueue, sizeof(enqueue), 1, 1, 2, 2));

	memset(&ref, 0, sizeof(ref));
	ref.identity = enqueue.identity;
	ref.handle.ticket_id = 23;
	ref.handle.queue_generation = 29;
	UT_ASSERT(cluster_gcs_pcm_x_ticket_ref_wire_valid(&ref, 0));
	UT_ASSERT(!cluster_gcs_pcm_x_ticket_ref_wire_valid(&ref, 1));
	ref.grant_generation = 31;
	UT_ASSERT(cluster_gcs_pcm_x_transfer_ref_wire_valid(&ref, 0));
	UT_ASSERT(cluster_gcs_pcm_x_terminal_ref_wire_valid(&ref, 0));
	UT_ASSERT(!cluster_gcs_pcm_x_transfer_ref_wire_valid(&ref, 1));

	memset(&retire, 0, sizeof(retire));
	retire.cluster_epoch = 0;
	retire.master_session_incarnation = 37;
	retire.retire_through_ticket_id = 41;
	retire.sender_node = 3;
	UT_ASSERT(cluster_gcs_pcm_x_retire_request_ingress_valid(&retire, sizeof(retire), 2, 37, 0, 3));
	UT_ASSERT(
		!cluster_gcs_pcm_x_retire_request_ingress_valid(&retire, sizeof(retire), 2, 37, 1, 3));
	UT_ASSERT(cluster_gcs_pcm_x_retire_ack_ingress_valid(&retire, sizeof(retire), 3, 0, 37, 2));
	UT_ASSERT(!cluster_gcs_pcm_x_retire_ack_ingress_valid(&retire, sizeof(retire), 3, 1, 37, 2));

	memset(&wait_snapshot, 0, sizeof(wait_snapshot));
	wait_snapshot.active = true;
	wait_snapshot.kind = CLUSTER_LMD_WAIT_PCM_CONVERT;
	wait_snapshot.request_id = enqueue.identity.request_id;
	wait_snapshot.cluster_epoch = 0;
	wait_snapshot.wait_seq = enqueue.identity.wait_seq;
	UT_ASSERT(cluster_gcs_pcm_x_wait_identity_matches(
		&enqueue.identity, 1, 8, CLUSTER_LMD_WAIT_STATE_READ_ACTIVE, &wait_snapshot));
	UT_ASSERT(cluster_gcs_block_epoch_advance_stales_slot(true, 0, 1));
	UT_ASSERT(!cluster_gcs_block_epoch_advance_stales_slot(false, 0, 1));
	UT_ASSERT(!cluster_gcs_block_epoch_advance_stales_slot(true, 1, 1));
}

UT_TEST(test_pcm_x_blocker_header_ingress_binds_master_not_requester_source)
{
	PcmXBlockerSetHeaderPayload header;

	memset(&header, 0, sizeof(header));
	header.ref.identity.node_id = 1;
	header.ref.identity.procno = 7;
	header.ref.identity.cluster_epoch = 9;
	header.ref.identity.request_id = 11;
	header.ref.identity.wait_seq = 13;
	header.ref.handle.ticket_id = 17;
	header.ref.handle.queue_generation = 19;
	header.set_generation = UINT64_C(0x100000001);
	header.nblockers = 2;
	header.set_crc32c = UINT32_C(0x12345678);

	/* Holder node 3 reports blockers for requester node 1 to tag master 2. */
	UT_ASSERT(cluster_gcs_pcm_x_blocker_header_ingress_valid(&header, sizeof(header), 3, 9, 2, 2));
	UT_ASSERT(
		!cluster_gcs_pcm_x_blocker_header_ingress_valid(&header, sizeof(header) - 1, 3, 9, 2, 2));
	UT_ASSERT(
		!cluster_gcs_pcm_x_blocker_header_ingress_valid(&header, sizeof(header), 3, 10, 2, 2));
	UT_ASSERT(!cluster_gcs_pcm_x_blocker_header_ingress_valid(&header, sizeof(header), 3, 9, 1, 2));
	header.set_generation = 0;
	UT_ASSERT(!cluster_gcs_pcm_x_blocker_header_ingress_valid(&header, sizeof(header), 3, 9, 2, 2));
}

UT_TEST(test_pcm_x_blocker_edge_ingress_binds_blocker_to_holder_source)
{
	PcmXBlockerChunkPayload edge;

	memset(&edge, 0, sizeof(edge));
	edge.requester_node = 1;
	edge.requester_procno = 7;
	edge.cluster_epoch = 9;
	edge.request_id = 11;
	edge.handle.ticket_id = 17;
	edge.handle.queue_generation = 19;
	edge.set_generation = UINT64_C(0x100000001);
	edge.blocker.node_id = 3;
	edge.blocker.procno = 23;
	edge.blocker.cluster_epoch = 9;
	edge.blocker.request_id = 29;
	edge.blocker.wait_seq = 31;

	UT_ASSERT(cluster_gcs_pcm_x_blocker_edge_ingress_valid(&edge, sizeof(edge), 3, 9, 2, 2));
	edge.blocker.node_id = 4;
	UT_ASSERT(!cluster_gcs_pcm_x_blocker_edge_ingress_valid(&edge, sizeof(edge), 3, 9, 2, 2));
	edge.blocker.node_id = 3;
	edge.blocker.cluster_epoch = 10;
	UT_ASSERT(!cluster_gcs_pcm_x_blocker_edge_ingress_valid(&edge, sizeof(edge), 3, 9, 2, 2));
	edge.blocker.cluster_epoch = 9;
	edge.grant_generation = 1;
	UT_ASSERT(!cluster_gcs_pcm_x_blocker_edge_ingress_valid(&edge, sizeof(edge), 3, 9, 2, 2));
	edge.grant_generation = 0;
	edge.blocker.request_id = 0;
	edge.blocker.xid = (TransactionId)37;
	UT_ASSERT(cluster_gcs_pcm_x_blocker_edge_ingress_valid(&edge, sizeof(edge), 3, 9, 2, 2));
	edge.blocker.xid = InvalidTransactionId;
	UT_ASSERT(!cluster_gcs_pcm_x_blocker_edge_ingress_valid(&edge, sizeof(edge), 3, 9, 2, 2));
	edge.blocker.request_id = 29;
	edge.blocker.wait_seq = 0;
	UT_ASSERT(!cluster_gcs_pcm_x_blocker_edge_ingress_valid(&edge, sizeof(edge), 3, 9, 2, 2));
}

UT_TEST(test_pcm_x_blocker_ack_carries_full_generation_and_binds_master_source)
{
	PcmXPhasePayload ack;
	PcmXPhasePayload probe;
	PcmXTicketRef ref;
	const uint64 generation = UINT64_C(0xFEDCBA9876543210);

	memset(&ack, 0, sizeof(ack));
	ack.ref.identity.node_id = 1;
	ack.ref.identity.procno = 7;
	ack.ref.identity.cluster_epoch = 9;
	ack.ref.identity.request_id = 11;
	ack.ref.identity.wait_seq = 13;
	ack.ref.handle.ticket_id = 17;
	ack.ref.handle.queue_generation = 19;
	cluster_gcs_pcm_x_blocker_ack_set_generation(&ack, generation);

	UT_ASSERT_EQ(cluster_gcs_pcm_x_blocker_ack_generation(&ack), generation);
	UT_ASSERT(cluster_gcs_pcm_x_blocker_ack_ingress_valid(&ack, sizeof(ack), 2, 9, 2, 3));
	UT_ASSERT(!cluster_gcs_pcm_x_blocker_ack_ingress_valid(&ack, sizeof(ack), 4, 9, 2, 3));
	UT_ASSERT(!cluster_gcs_pcm_x_blocker_ack_ingress_valid(&ack, sizeof(ack), 2, 10, 2, 3));
	cluster_gcs_pcm_x_blocker_ack_set_generation(&ack, 0);
	UT_ASSERT(!cluster_gcs_pcm_x_blocker_ack_ingress_valid(&ack, sizeof(ack), 2, 9, 2, 3));

	/* Type 48 uses the same authenticated master->holder direction for both
	 * arms.  An all-zero generation is the PROBE request; it is never a
	 * generation-exact ACK. */
	probe = ack;
	UT_ASSERT_EQ(cluster_gcs_pcm_x_blocker_ack_generation(&probe), 0);
	UT_ASSERT(cluster_gcs_pcm_x_blocker_probe_ingress_valid(&probe, sizeof(probe), 2, 9, 2, 3));
	UT_ASSERT(!cluster_gcs_pcm_x_blocker_probe_ingress_valid(&probe, sizeof(probe), 4, 9, 2, 3));
	UT_ASSERT(!cluster_gcs_pcm_x_blocker_probe_ingress_valid(&probe, sizeof(probe), 2, 10, 2, 3));
	cluster_gcs_pcm_x_blocker_ack_set_generation(&probe, 1);
	UT_ASSERT(!cluster_gcs_pcm_x_blocker_probe_ingress_valid(&probe, sizeof(probe), 2, 9, 2, 3));
	UT_ASSERT(cluster_gcs_pcm_x_blocker_ack_ingress_valid(&probe, sizeof(probe), 2, 9, 2, 3));

	/* The ACK producer has no representation for the reserved PROBE value. */
	ref = ack.ref;
	memset(&ack, 0xA5, sizeof(ack));
	UT_ASSERT(!cluster_gcs_pcm_x_blocker_ack_build(&ref, 0, &ack));
	UT_ASSERT_EQ(cluster_gcs_pcm_x_blocker_ack_generation(&ack), 0);
	UT_ASSERT(!cluster_gcs_pcm_x_blocker_ack_build(&ref, UINT64_MAX, &ack));
	UT_ASSERT_EQ(cluster_gcs_pcm_x_blocker_ack_generation(&ack), 0);
	UT_ASSERT(cluster_gcs_pcm_x_blocker_ack_build(&ref, generation, &ack));
	UT_ASSERT(memcmp(&ack.ref, &ref, sizeof(ref)) == 0);
	UT_ASSERT_EQ(cluster_gcs_pcm_x_blocker_ack_generation(&ack), generation);
}

UT_TEST(test_pcm_x_drain_poll_binds_exact_master_and_generation)
{
	PcmXDrainPollPayload poll;

	memset(&poll, 0, sizeof(poll));
	poll.ref.identity.node_id = 1;
	poll.ref.identity.procno = 7;
	poll.ref.identity.cluster_epoch = 9;
	poll.ref.identity.request_id = 11;
	poll.ref.identity.wait_seq = 13;
	poll.ref.handle.ticket_id = 17;
	poll.ref.handle.queue_generation = 19;
	poll.ref.grant_generation = 23;
	poll.drain_generation = 29;

	/* The tag master is node 2; node 3 is one terminal participant. */
	UT_ASSERT(cluster_gcs_pcm_x_drain_poll_ingress_valid(&poll, sizeof(poll), 2, 9, 2, 3));
	UT_ASSERT(!cluster_gcs_pcm_x_drain_poll_ingress_valid(&poll, sizeof(poll) - 1, 2, 9, 2, 3));
	UT_ASSERT(!cluster_gcs_pcm_x_drain_poll_ingress_valid(&poll, sizeof(poll), 4, 9, 2, 3));
	UT_ASSERT(!cluster_gcs_pcm_x_drain_poll_ingress_valid(&poll, sizeof(poll), 2, 10, 2, 3));
	poll.drain_generation = 0;
	UT_ASSERT(!cluster_gcs_pcm_x_drain_poll_ingress_valid(&poll, sizeof(poll), 2, 9, 2, 3));
	poll.drain_generation = 29;
	poll.ref.grant_generation = UINT64_MAX;
	UT_ASSERT(!cluster_gcs_pcm_x_drain_poll_ingress_valid(&poll, sizeof(poll), 2, 9, 2, 3));
}

UT_TEST(test_pcm_x_drain_ack_binds_participant_and_canonical_payload)
{
	PcmXPhasePayload ack;

	memset(&ack, 0, sizeof(ack));
	ack.ref.identity.node_id = 1;
	ack.ref.identity.procno = 7;
	ack.ref.identity.cluster_epoch = 9;
	ack.ref.identity.request_id = 11;
	ack.ref.identity.wait_seq = 13;
	ack.ref.handle.ticket_id = 17;
	ack.ref.handle.queue_generation = 19;
	ack.ref.grant_generation = 23;

	/* Participant node 3 ACKs to the local tag master node 2. */
	UT_ASSERT(cluster_gcs_pcm_x_drain_ack_ingress_valid(&ack, sizeof(ack), 3, 9, 2, 2));
	UT_ASSERT(!cluster_gcs_pcm_x_drain_ack_ingress_valid(&ack, sizeof(ack), 3, 9, 4, 2));
	ack.reason = 1;
	UT_ASSERT(!cluster_gcs_pcm_x_drain_ack_ingress_valid(&ack, sizeof(ack), 3, 9, 2, 2));
	ack.reason = 0;
	ack.flags = 1;
	UT_ASSERT(!cluster_gcs_pcm_x_drain_ack_ingress_valid(&ack, sizeof(ack), 3, 9, 2, 2));
}

UT_TEST(test_pcm_x_retire_request_binds_master_session_and_target)
{
	PcmXRetirePayload retire;

	memset(&retire, 0, sizeof(retire));
	retire.cluster_epoch = 9;
	retire.master_session_incarnation = 17;
	retire.retire_through_ticket_id = 23;
	retire.sender_node = 3;

	UT_ASSERT(cluster_gcs_pcm_x_retire_request_ingress_valid(&retire, sizeof(retire), 2, 17, 9, 3));
	UT_ASSERT(
		!cluster_gcs_pcm_x_retire_request_ingress_valid(&retire, sizeof(retire), 2, 19, 9, 3));
	UT_ASSERT(
		!cluster_gcs_pcm_x_retire_request_ingress_valid(&retire, sizeof(retire), 2, 17, 10, 3));
	retire.sender_node = 4;
	UT_ASSERT(
		!cluster_gcs_pcm_x_retire_request_ingress_valid(&retire, sizeof(retire), 2, 17, 9, 3));
}

UT_TEST(test_pcm_x_retire_ack_binds_responder_and_master_authority)
{
	PcmXRetirePayload ack;

	memset(&ack, 0, sizeof(ack));
	ack.cluster_epoch = 9;
	ack.master_session_incarnation = 17;
	ack.retire_through_ticket_id = 23;
	ack.sender_node = 3;

	UT_ASSERT(cluster_gcs_pcm_x_retire_ack_ingress_valid(&ack, sizeof(ack), 3, 9, 17, 2));
	UT_ASSERT(!cluster_gcs_pcm_x_retire_ack_ingress_valid(&ack, sizeof(ack), 4, 9, 17, 2));
	UT_ASSERT(!cluster_gcs_pcm_x_retire_ack_ingress_valid(&ack, sizeof(ack), 3, 9, 19, 2));
	ack.flags = 1;
	UT_ASSERT(!cluster_gcs_pcm_x_retire_ack_ingress_valid(&ack, sizeof(ack), 3, 9, 17, 2));
}


UT_TEST(test_pcm_x_formation_identical_complete_samples_may_revalidate)
{
	PcmXPeerBinding after[PCM_X_PROTOCOL_NODE_LIMIT];
	PcmXPeerBinding before[PCM_X_PROTOCOL_NODE_LIMIT];

	memset(before, 0, sizeof(before));
	before[0].cluster_epoch = 9;
	before[0].peer_session_incarnation = 101;
	before[1].cluster_epoch = 9;
	before[1].peer_session_incarnation = 102;
	memcpy(after, before, sizeof(after));
	UT_ASSERT(cluster_gcs_pcm_x_formation_samples_stable(true, before, true, after));
}


UT_TEST(test_pcm_x_formation_transient_or_inconsistent_sample_is_tick_noop)
{
	PcmXPeerBinding after[PCM_X_PROTOCOL_NODE_LIMIT];
	PcmXPeerBinding before[PCM_X_PROTOCOL_NODE_LIMIT];

	memset(before, 0, sizeof(before));
	before[0].cluster_epoch = 9;
	before[0].peer_session_incarnation = 101;
	memcpy(after, before, sizeof(after));
	UT_ASSERT(!cluster_gcs_pcm_x_formation_samples_stable(false, before, true, after));
	UT_ASSERT(!cluster_gcs_pcm_x_formation_samples_stable(true, before, false, after));
	after[0].peer_session_incarnation++;
	UT_ASSERT(!cluster_gcs_pcm_x_formation_samples_stable(true, before, true, after));
	memcpy(after, before, sizeof(after));
	after[1].cluster_epoch = 10;
	after[1].peer_session_incarnation = 102;
	UT_ASSERT(!cluster_gcs_pcm_x_formation_samples_stable(true, before, true, after));
}


UT_TEST(test_pcm_x_confirm_publish_then_stale_requires_exact_graph_close)
{
	UT_ASSERT(cluster_gcs_pcm_x_confirm_compensation_required(UINT64_C(7001), PCM_X_QUEUE_STALE));
	UT_ASSERT(
		cluster_gcs_pcm_x_confirm_compensation_required(UINT64_C(7001), PCM_X_QUEUE_NOT_READY));
	UT_ASSERT(!cluster_gcs_pcm_x_confirm_compensation_required(0, PCM_X_QUEUE_STALE));
	UT_ASSERT(!cluster_gcs_pcm_x_confirm_compensation_required(UINT64_C(7001), PCM_X_QUEUE_OK));
	UT_ASSERT(
		!cluster_gcs_pcm_x_confirm_compensation_required(UINT64_C(7001), PCM_X_QUEUE_DUPLICATE));
}


static void
pcm_x_test_init_transfer_ref(PcmXTicketRef *ref)
{
	memset(ref, 0, sizeof(*ref));
	ref->identity.node_id = 1;
	ref->identity.procno = 7;
	ref->identity.xid = 9;
	ref->identity.cluster_epoch = 11;
	ref->identity.request_id = 13;
	ref->identity.wait_seq = 17;
	ref->identity.base_own_generation = 19;
	ref->handle.ticket_id = 23;
	ref->handle.queue_generation = 29;
	ref->grant_generation = 31;
}


UT_TEST(test_pcm_x_revoke_ingress_binds_master_and_exact_transfer_key)
{
	PcmXRevokePayload revoke;
	uint64 image_id;

	memset(&revoke, 0, sizeof(revoke));
	pcm_x_test_init_transfer_ref(&revoke.ref);
	UT_ASSERT(cluster_pcm_x_image_id_encode(2, 37, &image_id));
	revoke.image_id = image_id;

	/* Master node 2 revokes current holder node 3 for requester node 1. */
	UT_ASSERT(cluster_gcs_pcm_x_revoke_ingress_valid(&revoke, sizeof(revoke), 2, 11, 2, 3));
	UT_ASSERT(!cluster_gcs_pcm_x_revoke_ingress_valid(&revoke, sizeof(revoke) - 1, 2, 11, 2, 3));
	UT_ASSERT(!cluster_gcs_pcm_x_revoke_ingress_valid(&revoke, sizeof(revoke), 3, 11, 2, 3));
	UT_ASSERT(!cluster_gcs_pcm_x_revoke_ingress_valid(&revoke, sizeof(revoke), 2, 12, 2, 3));
	revoke.ref.grant_generation = 0;
	UT_ASSERT(!cluster_gcs_pcm_x_revoke_ingress_valid(&revoke, sizeof(revoke), 2, 11, 2, 3));
	revoke.ref.grant_generation = 31;
	revoke.image_id = 0;
	UT_ASSERT(!cluster_gcs_pcm_x_revoke_ingress_valid(&revoke, sizeof(revoke), 2, 11, 2, 3));
	revoke.image_id = UINT64CONST(0xe000000000000025);
	UT_ASSERT(!cluster_gcs_pcm_x_revoke_ingress_valid(&revoke, sizeof(revoke), 2, 11, 2, 3));
	UT_ASSERT(cluster_pcm_x_image_id_encode(3, 37, &revoke.image_id));
	UT_ASSERT(!cluster_gcs_pcm_x_revoke_ingress_valid(&revoke, sizeof(revoke), 2, 11, 2, 3));
}


UT_TEST(test_pcm_x_image_ready_ingress_binds_holder_image_to_master)
{
	PcmXGrantPayload ready;
	uint64 image_id;

	memset(&ready, 0, sizeof(ready));
	pcm_x_test_init_transfer_ref(&ready.ref);
	UT_ASSERT(cluster_pcm_x_image_id_encode(2, 37, &image_id));
	ready.image.image_id = image_id;
	ready.image.source_node = 3;
	ready.image.page_scn = 41;
	ready.image.page_lsn = 43;
	ready.image.page_checksum = UINT32_C(0x12345678);

	/* source_own_generation=0 is the legal first ownership generation. */
	UT_ASSERT(cluster_gcs_pcm_x_image_ready_ingress_valid(&ready, sizeof(ready), 3, 11, 2, 2));
	UT_ASSERT(!cluster_gcs_pcm_x_image_ready_ingress_valid(&ready, sizeof(ready) - 1, 3, 11, 2, 2));
	ready.image.source_node = 4;
	UT_ASSERT(!cluster_gcs_pcm_x_image_ready_ingress_valid(&ready, sizeof(ready), 3, 11, 2, 2));
	ready.image.source_node = 3;
	UT_ASSERT(!cluster_gcs_pcm_x_image_ready_ingress_valid(&ready, sizeof(ready), 3, 11, 1, 2));
	ready.image.image_id = 0;
	UT_ASSERT(!cluster_gcs_pcm_x_image_ready_ingress_valid(&ready, sizeof(ready), 3, 11, 2, 2));
	ready.image.image_id = UINT64CONST(0xf000000000000025);
	UT_ASSERT(!cluster_gcs_pcm_x_image_ready_ingress_valid(&ready, sizeof(ready), 3, 11, 2, 2));
	UT_ASSERT(cluster_pcm_x_image_id_encode(3, 37, &ready.image.image_id));
	UT_ASSERT(!cluster_gcs_pcm_x_image_ready_ingress_valid(&ready, sizeof(ready), 3, 11, 2, 2));
}


UT_TEST(test_pcm_x_prepare_grant_ingress_binds_master_to_requester)
{
	PcmXGrantPayload grant;
	uint64 image_id;

	memset(&grant, 0, sizeof(grant));
	pcm_x_test_init_transfer_ref(&grant.ref);
	UT_ASSERT(cluster_pcm_x_image_id_encode(2, 37, &image_id));
	grant.image.image_id = image_id;
	grant.image.source_node = 3;

	UT_ASSERT(cluster_gcs_pcm_x_prepare_grant_ingress_valid(&grant, sizeof(grant), 2, 11, 2, 1));
	UT_ASSERT(!cluster_gcs_pcm_x_prepare_grant_ingress_valid(&grant, sizeof(grant), 2, 11, 2, 4));
	UT_ASSERT(!cluster_gcs_pcm_x_prepare_grant_ingress_valid(&grant, sizeof(grant), 3, 11, 2, 1));
	grant.image.source_node = PCM_X_PROTOCOL_NODE_LIMIT;
	UT_ASSERT(!cluster_gcs_pcm_x_prepare_grant_ingress_valid(&grant, sizeof(grant), 2, 11, 2, 1));
	grant.image.source_node = 3;
	grant.ref.identity.request_id = 0;
	UT_ASSERT(!cluster_gcs_pcm_x_prepare_grant_ingress_valid(&grant, sizeof(grant), 2, 11, 2, 1));
	grant.ref.identity.request_id = 13;
	grant.image.image_id = UINT64CONST(0xe000000000000025);
	UT_ASSERT(!cluster_gcs_pcm_x_prepare_grant_ingress_valid(&grant, sizeof(grant), 2, 11, 2, 1));
	UT_ASSERT(cluster_pcm_x_image_id_encode(3, 37, &grant.image.image_id));
	UT_ASSERT(!cluster_gcs_pcm_x_prepare_grant_ingress_valid(&grant, sizeof(grant), 2, 11, 2, 1));
}


UT_TEST(test_pcm_x_install_ready_ingress_is_canonical_requester_ack)
{
	PcmXInstallReadyPayload ready;
	uint64 image_id;

	memset(&ready, 0, sizeof(ready));
	pcm_x_test_init_transfer_ref(&ready.ref);
	UT_ASSERT(cluster_pcm_x_image_id_encode(2, 37, &image_id));
	ready.image_id = image_id;
	ready.result = PCM_X_QUEUE_OK;
	ready.phase = PGRAC_IC_MSG_PCM_X_INSTALL_READY;

	UT_ASSERT(cluster_gcs_pcm_x_install_ready_ingress_valid(&ready, sizeof(ready), 1, 11, 2, 2));
	UT_ASSERT(!cluster_gcs_pcm_x_install_ready_ingress_valid(&ready, sizeof(ready), 3, 11, 2, 2));
	ready.result = PCM_X_QUEUE_DUPLICATE;
	UT_ASSERT(!cluster_gcs_pcm_x_install_ready_ingress_valid(&ready, sizeof(ready), 1, 11, 2, 2));
	ready.result = PCM_X_QUEUE_OK;
	ready.phase = PGRAC_IC_MSG_PCM_X_PREPARE_GRANT;
	UT_ASSERT(!cluster_gcs_pcm_x_install_ready_ingress_valid(&ready, sizeof(ready), 1, 11, 2, 2));
	ready.phase = PGRAC_IC_MSG_PCM_X_INSTALL_READY;
	ready.flags = 1;
	UT_ASSERT(!cluster_gcs_pcm_x_install_ready_ingress_valid(&ready, sizeof(ready), 1, 11, 2, 2));
	ready.flags = 0;
	ready.image_id = UINT64CONST(0xf000000000000025);
	UT_ASSERT(!cluster_gcs_pcm_x_install_ready_ingress_valid(&ready, sizeof(ready), 1, 11, 2, 2));
	UT_ASSERT(cluster_pcm_x_image_id_encode(3, 37, &ready.image_id));
	UT_ASSERT(!cluster_gcs_pcm_x_install_ready_ingress_valid(&ready, sizeof(ready), 1, 11, 2, 2));
}


UT_TEST(test_pcm_x_commit_x_ingress_is_canonical_master_phase)
{
	PcmXPhasePayload commit;

	memset(&commit, 0, sizeof(commit));
	pcm_x_test_init_transfer_ref(&commit.ref);
	commit.phase = PGRAC_IC_MSG_PCM_X_COMMIT_X;

	UT_ASSERT(cluster_gcs_pcm_x_commit_x_ingress_valid(&commit, sizeof(commit), 2, 11, 2, 1));
	UT_ASSERT(!cluster_gcs_pcm_x_commit_x_ingress_valid(&commit, sizeof(commit), 3, 11, 2, 1));
	UT_ASSERT(!cluster_gcs_pcm_x_commit_x_ingress_valid(&commit, sizeof(commit), 2, 11, 2, 4));
	commit.reason = 1;
	UT_ASSERT(!cluster_gcs_pcm_x_commit_x_ingress_valid(&commit, sizeof(commit), 2, 11, 2, 1));
	commit.reason = 0;
	commit.flags = 1;
	UT_ASSERT(!cluster_gcs_pcm_x_commit_x_ingress_valid(&commit, sizeof(commit), 2, 11, 2, 1));
}


UT_TEST(test_pcm_x_final_ack_ingress_binds_exact_committed_generation)
{
	PcmXFinalAckPayload ack;
	uint64 image_id;

	memset(&ack, 0, sizeof(ack));
	pcm_x_test_init_transfer_ref(&ack.ref);
	ack.ref.identity.base_own_generation = 0;
	UT_ASSERT(cluster_pcm_x_image_id_encode(2, 37, &image_id));
	ack.image_id = image_id;
	ack.committed_own_generation = 1;

	UT_ASSERT(cluster_gcs_pcm_x_final_ack_ingress_valid(&ack, sizeof(ack), 1, 11, 2, 2));
	UT_ASSERT(!cluster_gcs_pcm_x_final_ack_ingress_valid(&ack, sizeof(ack), 3, 11, 2, 2));
	ack.committed_own_generation = 2;
	UT_ASSERT(!cluster_gcs_pcm_x_final_ack_ingress_valid(&ack, sizeof(ack), 1, 11, 2, 2));
	ack.ref.identity.base_own_generation = UINT64_MAX;
	ack.committed_own_generation = 0;
	UT_ASSERT(!cluster_gcs_pcm_x_final_ack_ingress_valid(&ack, sizeof(ack), 1, 11, 2, 2));
	ack.ref.identity.base_own_generation = 0;
	ack.committed_own_generation = 1;
	ack.image_id = 0;
	UT_ASSERT(!cluster_gcs_pcm_x_final_ack_ingress_valid(&ack, sizeof(ack), 1, 11, 2, 2));
	ack.image_id = UINT64CONST(0xe000000000000025);
	UT_ASSERT(!cluster_gcs_pcm_x_final_ack_ingress_valid(&ack, sizeof(ack), 1, 11, 2, 2));
	UT_ASSERT(cluster_pcm_x_image_id_encode(3, 37, &ack.image_id));
	UT_ASSERT(!cluster_gcs_pcm_x_final_ack_ingress_valid(&ack, sizeof(ack), 1, 11, 2, 2));
}


UT_TEST(test_pcm_x_final_commit_ack_ingress_is_canonical_master_phase)
{
	PcmXPhasePayload ack;

	memset(&ack, 0, sizeof(ack));
	pcm_x_test_init_transfer_ref(&ack.ref);
	ack.phase = PGRAC_IC_MSG_PCM_X_FINAL_COMMIT_ACK;

	UT_ASSERT(cluster_gcs_pcm_x_final_commit_ack_ingress_valid(&ack, sizeof(ack), 2, 11, 2, 1));
	UT_ASSERT(!cluster_gcs_pcm_x_final_commit_ack_ingress_valid(&ack, sizeof(ack), 3, 11, 2, 1));
	ack.phase = PGRAC_IC_MSG_PCM_X_FINAL_CONFIRM;
	UT_ASSERT(!cluster_gcs_pcm_x_final_commit_ack_ingress_valid(&ack, sizeof(ack), 2, 11, 2, 1));
	ack.phase = PGRAC_IC_MSG_PCM_X_FINAL_COMMIT_ACK;
	ack.flags = 1;
	UT_ASSERT(!cluster_gcs_pcm_x_final_commit_ack_ingress_valid(&ack, sizeof(ack), 2, 11, 2, 1));
}


UT_TEST(test_pcm_x_final_confirm_ingress_is_canonical_requester_phase)
{
	PcmXPhasePayload confirm;

	memset(&confirm, 0, sizeof(confirm));
	pcm_x_test_init_transfer_ref(&confirm.ref);
	confirm.phase = PGRAC_IC_MSG_PCM_X_FINAL_CONFIRM;

	UT_ASSERT(
		cluster_gcs_pcm_x_final_confirm_ingress_valid(&confirm, sizeof(confirm), 1, 11, 2, 2));
	UT_ASSERT(
		!cluster_gcs_pcm_x_final_confirm_ingress_valid(&confirm, sizeof(confirm), 3, 11, 2, 2));
	UT_ASSERT(
		!cluster_gcs_pcm_x_final_confirm_ingress_valid(&confirm, sizeof(confirm), 1, 11, 3, 2));
	confirm.reason = 1;
	UT_ASSERT(
		!cluster_gcs_pcm_x_final_confirm_ingress_valid(&confirm, sizeof(confirm), 1, 11, 2, 2));
	confirm.reason = 0;
	confirm.phase = PGRAC_IC_MSG_PCM_X_FINAL_COMMIT_ACK;
	UT_ASSERT(
		!cluster_gcs_pcm_x_final_confirm_ingress_valid(&confirm, sizeof(confirm), 1, 11, 2, 2));
}

UT_TEST(test_pcm_x_master_drive_selects_exact_authority_and_next_holder)
{
	PcmAuthoritySnapshot authority;
	const uint64 ticket_id = 73;
	uint32 holders;
	int32 holder;
	int32 source;

	memset(&authority, 0, sizeof(authority));
	authority.state = PCM_STATE_X;
	authority.x_holder_node = 2;
	authority.master_holder.node_id = 2;
	authority.pending_x_requester_node = 3;
	authority.pending_x_since_lsn = UINT64_C(0x8000000000000000) | ticket_id;
	UT_ASSERT_EQ(
		cluster_gcs_pcm_x_authority_holders_exact(&authority, 3, ticket_id, &holders, &source),
		PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(holders, UINT32_C(1) << 2);
	UT_ASSERT_EQ(source, 2);
	UT_ASSERT_EQ(
		cluster_gcs_pcm_x_authority_holders_exact(&authority, 3, ticket_id + 1, &holders, &source),
		PCM_X_QUEUE_STALE);
	authority.pending_x_requester_node = 2;
	UT_ASSERT_EQ(
		cluster_gcs_pcm_x_authority_holders_exact(&authority, 2, ticket_id, &holders, &source),
		PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(holders, UINT32_C(1) << 2);
	UT_ASSERT_EQ(source, 2);

	authority.state = PCM_STATE_S;
	authority.x_holder_node = -1;
	authority.s_holders_bitmap = (UINT32_C(1) << 1) | (UINT32_C(1) << 3);
	authority.master_holder.node_id = 1;
	authority.pending_x_requester_node = 3;
	UT_ASSERT_EQ(
		cluster_gcs_pcm_x_authority_holders_exact(&authority, 3, ticket_id, &holders, &source),
		PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(holders, authority.s_holders_bitmap);
	/* A requester S mirror must be the source even when another holder is
	 * available.  Invalidating requester S first bumps its local ownership
	 * generation; the subsequent X commit would then be a second bump while
	 * FINAL_ACK is generation-exact at identity.base+1. */
	UT_ASSERT_EQ(source, 3);
	UT_ASSERT_EQ(
		cluster_gcs_pcm_x_authority_holders_exact(&authority, 0, ticket_id, &holders, &source),
		PCM_X_QUEUE_STALE);
	authority.pending_x_requester_node = 0;
	UT_ASSERT_EQ(
		cluster_gcs_pcm_x_authority_holders_exact(&authority, 0, ticket_id, &holders, &source),
		PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(source, 1);
	/* A canonical S master-holder that is also the requester retains that
	 * source role; the exact self handoff keeps the lifecycle finite. */
	authority.s_holders_bitmap = (UINT32_C(1) << 0) | (UINT32_C(1) << 1);
	authority.master_holder.node_id = 0;
	UT_ASSERT_EQ(
		cluster_gcs_pcm_x_authority_holders_exact(&authority, 0, ticket_id, &holders, &source),
		PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(source, 0);
	/* A sole requester S copy reuses its exact REVOKING lifecycle as the
	 * requester grant reservation; selecting self is therefore finite and
	 * preserves the single ownership-generation bump. */
	authority.s_holders_bitmap = UINT32_C(1) << 0;
	UT_ASSERT_EQ(
		cluster_gcs_pcm_x_authority_holders_exact(&authority, 0, ticket_id, &holders, &source),
		PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(source, 0);
	authority.s_holders_bitmap = (UINT32_C(1) << 1) | (UINT32_C(1) << 3);
	authority.master_holder.node_id = 2;
	UT_ASSERT_EQ(
		cluster_gcs_pcm_x_authority_holders_exact(&authority, 0, ticket_id, &holders, &source),
		PCM_X_QUEUE_CORRUPT);

	memset(&authority, 0, sizeof(authority));
	authority.state = PCM_STATE_N;
	authority.x_holder_node = -1;
	authority.master_holder.node_id = UINT32_MAX;
	authority.pending_x_requester_node = 0;
	authority.pending_x_since_lsn = UINT64_C(0x8000000000000000) | ticket_id;
	UT_ASSERT_EQ(
		cluster_gcs_pcm_x_authority_holders_exact(&authority, 0, ticket_id, &holders, &source),
		PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(holders, UINT32_C(1) << 0);
	UT_ASSERT_EQ(source, 0);
	authority.master_holder.node_id = 1;
	UT_ASSERT_EQ(
		cluster_gcs_pcm_x_authority_holders_exact(&authority, 0, ticket_id, &holders, &source),
		PCM_X_QUEUE_CORRUPT);
	authority.master_holder.node_id = PCM_X_PROTOCOL_NODE_LIMIT;
	UT_ASSERT_EQ(
		cluster_gcs_pcm_x_authority_holders_exact(&authority, 0, ticket_id, &holders, &source),
		PCM_X_QUEUE_CORRUPT);

	UT_ASSERT_EQ(cluster_gcs_pcm_x_next_unacked_holder((UINT32_C(1) << 1) | (UINT32_C(1) << 3),
													   UINT32_C(1) << 1, &holder),
				 PCM_X_QUEUE_OK);
	UT_ASSERT_EQ(holder, 3);
	UT_ASSERT_EQ(cluster_gcs_pcm_x_next_unacked_holder(UINT32_C(1) << 1, UINT32_C(1) << 1, &holder),
				 PCM_X_QUEUE_NOT_FOUND);
	UT_ASSERT_EQ(holder, -1);
	UT_ASSERT_EQ(cluster_gcs_pcm_x_next_unacked_holder(UINT32_C(1) << 1, UINT32_C(1) << 2, &holder),
				 PCM_X_QUEUE_CORRUPT);
}

UT_TEST(test_pcm_x_master_drive_wiring_binds_grd_barrier_to_exact_ticket)
{
	char *source = read_gcs_block_source();
	char *claim;
	char *cancel;
	char *end;
	char *finalize;
	char *fail_closed;
	char *graph;
	char *handler;
	char *note_stale;
	char *prepare;
	char *stale;
	char *state;
	char *reserve;
	char *publish;
	char *post_verify_state;
	char *normalize;
	char *revalidate;
	char *clear;

	UT_ASSERT_NOT_NULL(source);
	if (source == NULL)
		return;
	claim = strstr(source, "\ngcs_block_pcm_x_ensure_pending_x_claim(");
	end = claim != NULL ? strstr(claim, "\n}\n") : NULL;
	UT_ASSERT_NOT_NULL(claim);
	UT_ASSERT_NOT_NULL(end);
	if (claim != NULL && end != NULL) {
		state = strstr(claim, "cluster_pcm_x_master_pending_x_claim_state_exact(");
		reserve = strstr(claim, "cluster_pcm_lock_try_reserve_pending_x(");
		publish = strstr(claim, "cluster_pcm_x_master_pending_x_claim_exact(");
		revalidate
			= publish != NULL ? strstr(publish, "cluster_pcm_lock_queue_pending_x_exact(") : NULL;
		post_verify_state
			= revalidate != NULL
				  ? strstr(revalidate, "cluster_pcm_x_master_pending_x_claim_state_exact(")
				  : NULL;
		normalize
			= post_verify_state != NULL ? strstr(post_verify_state, "PCM_X_QUEUE_NOT_READY") : NULL;
		UT_ASSERT_NOT_NULL(state);
		UT_ASSERT_NOT_NULL(reserve);
		UT_ASSERT_NOT_NULL(publish);
		UT_ASSERT_NOT_NULL(revalidate);
		UT_ASSERT_NOT_NULL(post_verify_state);
		UT_ASSERT_NOT_NULL(normalize);
		if (state != NULL && reserve != NULL && publish != NULL && revalidate != NULL
			&& post_verify_state != NULL && normalize != NULL)
			UT_ASSERT(state < reserve && reserve < publish && publish < revalidate
					  && revalidate < post_verify_state && post_verify_state < normalize
					  && normalize < end);
		clear = strstr(claim, "cluster_pcm_lock_clear_pending_x_if(");
		UT_ASSERT(clear == NULL || clear > end);
	}
	cancel = strstr(source, "\ngcs_block_pcm_x_cancel_claimed_probe_exact(");
	if (cancel != NULL)
		cancel = strstr(cancel + 1, "\ngcs_block_pcm_x_cancel_claimed_probe_exact(");
	end = cancel != NULL ? strstr(cancel, "\n}\n") : NULL;
	UT_ASSERT_NOT_NULL(cancel);
	UT_ASSERT_NOT_NULL(end);
	if (cancel != NULL && end != NULL) {
		graph = strstr(cancel, "cluster_lmd_graph_remove_edge_by_waiter_exact_result(");
		stale = graph != NULL ? strstr(graph, "CLUSTER_LMD_GRAPH_REMOVE_STALE") : NULL;
		note_stale = stale != NULL
						 ? strstr(stale, "cluster_lmd_pcm_convert_wfg_note_exact_remove_stale(")
						 : NULL;
		fail_closed
			= note_stale != NULL ? strstr(note_stale, "cluster_pcm_x_runtime_fail_closed(") : NULL;
		clear = strstr(cancel, "cluster_pcm_lock_clear_queue_pending_x_exact(");
		finalize = strstr(cancel, "cluster_pcm_x_master_pending_x_cancel_finalize_exact(");
		UT_ASSERT_NOT_NULL(graph);
		UT_ASSERT_NOT_NULL(stale);
		UT_ASSERT_NOT_NULL(note_stale);
		UT_ASSERT_NOT_NULL(fail_closed);
		UT_ASSERT_NOT_NULL(clear);
		UT_ASSERT_NOT_NULL(finalize);
		if (graph != NULL && stale != NULL && note_stale != NULL && fail_closed != NULL
			&& clear != NULL && finalize != NULL)
			UT_ASSERT(graph < stale && stale < note_stale && note_stale < fail_closed
					  && fail_closed < clear && clear < finalize && finalize < end);
		prepare = strstr(cancel, "cluster_pcm_x_master_pending_x_cancel_prepare_exact(");
		UT_ASSERT(prepare == NULL || prepare > end);
	}
	handler = strstr(source, "\ncluster_gcs_handle_pcm_x_cancel_envelope(");
	end = handler != NULL ? strstr(handler, "\n}\n") : NULL;
	UT_ASSERT_NOT_NULL(handler);
	UT_ASSERT_NOT_NULL(end);
	if (handler != NULL && end != NULL) {
		prepare = strstr(handler, "cluster_pcm_x_master_pending_x_cancel_prepare_exact(");
		cancel = prepare != NULL ? strstr(prepare, "gcs_block_pcm_x_cancel_claimed_probe_exact(")
								 : NULL;
		UT_ASSERT_NOT_NULL(prepare);
		UT_ASSERT_NOT_NULL(cancel);
		if (prepare != NULL && cancel != NULL)
			UT_ASSERT(prepare < cancel && cancel < end);
		UT_ASSERT(strstr(handler, "cluster_pcm_x_master_cancel_reversible_exact(") == NULL);
	}
	free(source);
}

UT_TEST(test_pcm_x_cancel_cleanup_classifies_exact_wfg_and_post_clear_failure)
{
	char *source = read_gcs_block_source();
	char *ordinary;
	char *claimed;
	char *end;
	char *exact;
	char *removed;
	char *stale;
	char *note_stale;
	char *fail_closed;
	char *finalize;

	UT_ASSERT_NOT_NULL(source);
	if (source == NULL)
		return;
	ordinary = strstr(source, "\ncluster_gcs_pcm_x_remove_cancelled_waiter(");
	end = ordinary != NULL ? strstr(ordinary, "\n}\n") : NULL;
	UT_ASSERT_NOT_NULL(ordinary);
	UT_ASSERT_NOT_NULL(end);
	if (ordinary != NULL && end != NULL) {
		exact = strstr(ordinary, "cluster_lmd_graph_remove_edge_by_waiter_exact_result(");
		removed = exact != NULL ? strstr(exact, "CLUSTER_LMD_GRAPH_REMOVE_REMOVED") : NULL;
		stale = removed != NULL ? strstr(removed, "CLUSTER_LMD_GRAPH_REMOVE_STALE") : NULL;
		note_stale = stale != NULL
						 ? strstr(stale, "cluster_lmd_pcm_convert_wfg_note_exact_remove_stale(")
						 : NULL;
		fail_closed
			= note_stale != NULL ? strstr(note_stale, "cluster_pcm_x_runtime_fail_closed(") : NULL;
		UT_ASSERT_NOT_NULL(exact);
		UT_ASSERT_NOT_NULL(removed);
		UT_ASSERT_NOT_NULL(stale);
		UT_ASSERT_NOT_NULL(note_stale);
		UT_ASSERT_NOT_NULL(fail_closed);
		if (exact != NULL && removed != NULL && stale != NULL && note_stale != NULL
			&& fail_closed != NULL)
			UT_ASSERT(exact < removed && removed < stale && stale < note_stale
					  && note_stale < fail_closed && fail_closed < end);
	}
	claimed = strstr(source, "\ngcs_block_pcm_x_cancel_claimed_probe_exact(");
	if (claimed != NULL)
		claimed = strstr(claimed + 1, "\ngcs_block_pcm_x_cancel_claimed_probe_exact(");
	end = claimed != NULL ? strstr(claimed, "\n}\n") : NULL;
	UT_ASSERT_NOT_NULL(claimed);
	UT_ASSERT_NOT_NULL(end);
	if (claimed != NULL && end != NULL) {
		finalize = strstr(claimed,
						  "result = cluster_pcm_x_master_pending_x_cancel_finalize_exact(token)");
		fail_closed
			= finalize != NULL ? strstr(finalize, "cluster_pcm_x_runtime_fail_closed(") : NULL;
		UT_ASSERT_NOT_NULL(finalize);
		UT_ASSERT_NOT_NULL(fail_closed);
		if (finalize != NULL && fail_closed != NULL)
			UT_ASSERT(finalize < fail_closed && fail_closed < end);
	}
	free(source);
}

UT_TEST(test_pcm_x_terminal_retry_reclaims_cancel_cleanup_after_owner_death)
{
	char *source = read_gcs_block_source();
	char *cleanup;
	char *kick;
	char *end;
	char *prepare;
	char *claimed;
	char *ordinary;
	char *cancel_gate;
	char *cancel_ack;
	char *prehandle_ack;
	char *snapshot;
	char *stage;
	char *stage_second;
	char *detach;

	UT_ASSERT_NOT_NULL(source);
	if (source == NULL)
		return;
	cleanup = strstr(source, "\ngcs_block_pcm_x_cancel_terminal_cleanup_exact(");
	end = cleanup != NULL ? strstr(cleanup, "\n}\n") : NULL;
	UT_ASSERT_NOT_NULL(cleanup);
	UT_ASSERT_NOT_NULL(end);
	if (cleanup != NULL && end != NULL) {
		prepare = strstr(cleanup, "cluster_pcm_x_master_pending_x_cancel_prepare_exact(");
		claimed = prepare != NULL ? strstr(prepare, "gcs_block_pcm_x_cancel_claimed_probe_exact(")
								  : NULL;
		ordinary = prepare != NULL ? strstr(prepare, "cluster_gcs_pcm_x_remove_cancelled_waiter(")
								   : NULL;
		UT_ASSERT_NOT_NULL(prepare);
		UT_ASSERT_NOT_NULL(claimed);
		UT_ASSERT_NOT_NULL(ordinary);
		if (prepare != NULL && claimed != NULL && ordinary != NULL)
			UT_ASSERT(prepare < claimed && prepare < ordinary && claimed < end && ordinary < end);
	}
	kick = strstr(source, "\ncluster_gcs_pcm_x_terminal_kick(");
	end = kick != NULL ? strstr(kick, "\n}\n\n\nstatic") : NULL;
	UT_ASSERT_NOT_NULL(kick);
	UT_ASSERT_NOT_NULL(end);
	if (kick != NULL && end != NULL) {
		cancel_gate = strstr(kick, "ref->grant_generation == 0");
		cleanup = cancel_gate != NULL
					  ? strstr(cancel_gate, "gcs_block_pcm_x_cancel_terminal_cleanup_exact(")
					  : NULL;
		snapshot = cleanup != NULL
					   ? strstr(cleanup, "cluster_pcm_x_master_cancel_ack_snapshot_exact(")
					   : NULL;
		prehandle_ack
			= snapshot != NULL ? strstr(snapshot, "PGRAC_IC_MSG_PCM_X_PREHANDLE_CANCEL_ACK") : NULL;
		cancel_ack
			= prehandle_ack != NULL ? strstr(prehandle_ack, "PGRAC_IC_MSG_PCM_X_CANCEL_ACK") : NULL;
		stage = snapshot != NULL ? strstr(snapshot, "cluster_gcs_pcm_x_stage_frame(") : NULL;
		stage_second = stage != NULL ? strstr(stage + 1, "cluster_gcs_pcm_x_stage_frame(") : NULL;
		detach = strstr(kick, "cluster_pcm_x_master_detach_terminal_exact(");
		UT_ASSERT_NOT_NULL(cancel_gate);
		UT_ASSERT_NOT_NULL(cleanup);
		UT_ASSERT_NOT_NULL(snapshot);
		UT_ASSERT_NOT_NULL(prehandle_ack);
		UT_ASSERT_NOT_NULL(cancel_ack);
		UT_ASSERT_NOT_NULL(stage);
		UT_ASSERT_NOT_NULL(stage_second);
		UT_ASSERT_NOT_NULL(detach);
		if (cancel_gate != NULL && cleanup != NULL && snapshot != NULL && prehandle_ack != NULL
			&& cancel_ack != NULL && stage != NULL && stage_second != NULL && detach != NULL)
			UT_ASSERT(cancel_gate < cleanup && cleanup < snapshot && snapshot < prehandle_ack
					  && prehandle_ack < cancel_ack && snapshot < stage && stage < stage_second
					  && stage_second < detach && detach < end);
	}
	free(source);
}


UT_TEST(test_pcm_x_invalidate_ack_matches_only_exact_unacked_holder)
{
	GcsBlockInvalidateAckPayload ack;
	PcmXMasterDriveSnapshot snapshot;

	memset(&ack, 0, sizeof(ack));
	memset(&snapshot, 0, sizeof(snapshot));
	snapshot.ref.identity.tag.spcOid = 11;
	snapshot.ref.identity.tag.dbOid = 12;
	snapshot.ref.identity.tag.relNumber = 13;
	snapshot.ref.identity.tag.blockNum = 14;
	snapshot.ref.identity.cluster_epoch = 0;
	snapshot.ref.identity.request_id = 19;
	snapshot.ticket_state = PCM_XT_ACTIVE_TRANSFER;
	snapshot.pending_s_holders_bitmap = (UINT32_C(1) << 1) | (UINT32_C(1) << 3);
	snapshot.acked_s_holders_bitmap = UINT32_C(1) << 1;
	ack.tag = snapshot.ref.identity.tag;
	ack.epoch = snapshot.ref.identity.cluster_epoch;
	ack.request_id = snapshot.ref.identity.request_id;
	ack.sender_node = 3;

	UT_ASSERT_EQ(cluster_gcs_pcm_x_invalidate_ack_match_exact(&snapshot, &ack, 0, 3),
				 PCM_X_QUEUE_OK);
	ack.ack_status = GCS_BLOCK_INVALIDATE_ACK_STATUS_RETRYABLE_BUSY;
	UT_ASSERT_EQ(cluster_gcs_pcm_x_invalidate_ack_match_exact(&snapshot, &ack, 0, 3),
				 PCM_X_QUEUE_BUSY);
	ack.ack_status = 0;
	ack.sender_node = 1;
	UT_ASSERT_EQ(cluster_gcs_pcm_x_invalidate_ack_match_exact(&snapshot, &ack, 0, 1),
				 PCM_X_QUEUE_DUPLICATE);
	ack.sender_node = 2;
	UT_ASSERT_EQ(cluster_gcs_pcm_x_invalidate_ack_match_exact(&snapshot, &ack, 0, 2),
				 PCM_X_QUEUE_STALE);
	ack.sender_node = 3;
	ack.ack_status = 1;
	UT_ASSERT_EQ(cluster_gcs_pcm_x_invalidate_ack_match_exact(&snapshot, &ack, 0, 3),
				 PCM_X_QUEUE_BAD_STATE);
	ack.ack_status = 0;
	ack.request_id++;
	UT_ASSERT_EQ(cluster_gcs_pcm_x_invalidate_ack_match_exact(&snapshot, &ack, 0, 3),
				 PCM_X_QUEUE_NOT_FOUND);
}


UT_TEST(test_pcm_x_final_ack_builds_exact_grd_handoff_token)
{
	PcmAuthoritySnapshot authority;
	PcmXGrdHandoffToken handoff;
	PcmXMasterFinalAckToken final;

	memset(&authority, 0, sizeof(authority));
	memset(&final, 0, sizeof(final));
	final.final_ack.ref.identity.tag.spcOid = 21;
	final.final_ack.ref.identity.tag.dbOid = 22;
	final.final_ack.ref.identity.tag.relNumber = 23;
	final.final_ack.ref.identity.tag.blockNum = 24;
	final.final_ack.ref.identity.node_id = 3;
	final.final_ack.ref.identity.procno = 25;
	final.final_ack.ref.identity.cluster_epoch = 0;
	final.final_ack.ref.identity.request_id = 27;
	final.final_ack.ref.handle.ticket_id = 35;
	final.final_ack.ref.grant_generation = 28;
	final.final_ack.image_id = 29;
	final.final_ack.committed_own_generation = 30;
	final.image.image_id = final.final_ack.image_id;
	/* Generation zero is the legal first ownership generation. */
	final.image.source_own_generation = 0;
	final.image.page_scn = 32;
	final.image.page_lsn = 33;
	final.image.source_node = 2;
	final.image.page_checksum = 34;

	UT_ASSERT(cluster_gcs_pcm_x_grd_handoff_token_build(&final, &authority, &handoff));
	UT_ASSERT(BufferTagsEqual(&handoff.tag, &final.final_ack.ref.identity.tag));
	UT_ASSERT_EQ(handoff.cluster_epoch, 0);
	UT_ASSERT_EQ(handoff.request_id, 27);
	UT_ASSERT_EQ(handoff.ticket_id, 35);
	UT_ASSERT_EQ(handoff.grant_generation, 28);
	UT_ASSERT_EQ(handoff.image_id, 29);
	UT_ASSERT_EQ(handoff.source_own_generation, 0);
	UT_ASSERT_EQ(handoff.requester_node, 3);
	UT_ASSERT_EQ(handoff.requester_procno, 25);
	UT_ASSERT_EQ(handoff.source_node, 2);
	UT_ASSERT_EQ(handoff.page_checksum, 34);
	final.image.image_id++;
	UT_ASSERT(!cluster_gcs_pcm_x_grd_handoff_token_build(&final, &authority, &handoff));
}


UT_TEST(test_pcm_x_holder_image_evidence_never_uses_generation_as_presence)
{
	PcmXLocalHolderProgress progress;
	PcmXTicketRef ref;

	memset(&progress, 0, sizeof(progress));
	memset(&ref, 0, sizeof(ref));
	ref.identity.node_id = 1;
	ref.identity.cluster_epoch = 2;
	ref.identity.request_id = 3;
	ref.handle.ticket_id = 4;
	ref.handle.queue_generation = 5;
	ref.grant_generation = 6;
	progress.ref = ref;
	progress.image.image_id = 7;
	progress.image.source_node = 2;
	progress.image.source_own_generation = 0;

	/* REVOKE has already published image_id, but that alone is not READY. */
	UT_ASSERT(!cluster_gcs_pcm_x_holder_image_ready_exact(&progress, &ref, 7, 2));
	progress.last_response_opcode = PGRAC_IC_MSG_PCM_X_REVOKE;
	progress.pending_opcode = PGRAC_IC_MSG_PCM_X_IMAGE_READY;
	progress.phase = PGRAC_IC_MSG_PCM_X_IMAGE_READY;
	UT_ASSERT(cluster_gcs_pcm_x_holder_image_ready_exact(&progress, &ref, 7, 2));
	UT_ASSERT(!cluster_gcs_pcm_x_holder_image_ready_exact(&progress, &ref, 7, 3));

	progress.pending_opcode = 0;
	progress.phase = 0;
	progress.last_response_opcode = PGRAC_IC_MSG_PCM_X_DRAIN_POLL;
	progress.flags = PCM_X_LOCAL_TAG_F_HOLDER_TERMINAL_MASK;
	UT_ASSERT(cluster_gcs_pcm_x_holder_image_drained_exact(&progress, &ref, 7, 2));
	UT_ASSERT(!cluster_gcs_pcm_x_holder_image_drained_exact(&progress, &ref, 8, 2));
}


UT_TEST(test_pcm_x_pending_x_marker_is_only_a_pre_handoff_gate)
{
	UT_ASSERT(cluster_gcs_pcm_x_transfer_pre_handoff_phase(0));
	UT_ASSERT(cluster_gcs_pcm_x_transfer_pre_handoff_phase(PGRAC_IC_MSG_PCM_X_REVOKE));
	UT_ASSERT(!cluster_gcs_pcm_x_transfer_pre_handoff_phase(PGRAC_IC_MSG_PCM_X_PREPARE_GRANT));
	UT_ASSERT(!cluster_gcs_pcm_x_transfer_pre_handoff_phase(PGRAC_IC_MSG_PCM_X_FINAL_COMMIT_ACK));
}


UT_TEST(test_pcm_x_ready_publication_follows_exact_retained_commit)
{
	char *source = read_gcs_block_source();
	const char *begin;
	const char *materialize;
	const char *finish;
	const char *publish;
	const char *send;
	const char *end;

	if (source == NULL)
		return;
	begin = strstr(source, "\ngcs_block_pcm_x_materialize_reserved_work(");
	UT_ASSERT_NOT_NULL(begin);
	if (begin == NULL) {
		free(source);
		return;
	}
	end = strstr(begin + 1, "\n}\n\n\n");
	materialize = strstr(begin, "cluster_gcs_block_dedup_pcm_x_materialize(");
	finish = strstr(begin, "cluster_bufmgr_pcm_own_finish_revoke_retain(");
	publish = strstr(begin, "cluster_gcs_block_dedup_pcm_x_publish_ready_exact(");
	send = strstr(begin, "gcs_block_pcm_x_stage_ready_work(");
	UT_ASSERT_NOT_NULL(end);
	UT_ASSERT_NOT_NULL(materialize);
	UT_ASSERT_NOT_NULL(finish);
	UT_ASSERT_NOT_NULL(publish);
	UT_ASSERT_NOT_NULL(send);
	if (end != NULL && materialize != NULL && finish != NULL && publish != NULL && send != NULL)
		UT_ASSERT(materialize < finish && finish < publish && publish < send && send < end);
	free(source);
}


UT_TEST(test_pcm_x_ready_materializes_exact_n_s_or_x_source_without_wire_change)
{
	char *source = read_gcs_block_source();
	const char *abort;
	const char *begin;
	const char *copy;
	const char *finish;
	const char *materialize;
	const char *publish;
	const char *revoke_handler;
	const char *drain;
	const char *generic_install;

	UT_ASSERT_NOT_NULL(source);
	if (source == NULL)
		return;
	begin = strstr(source, "\ngcs_block_pcm_x_materialize_reserved_work(");
	abort = strstr(source, "\ngcs_block_pcm_x_abort_image_before_finish(");
	revoke_handler = strstr(source, "\ncluster_gcs_handle_pcm_x_revoke_envelope(");
	drain = strstr(source, "\ngcs_block_pcm_x_local_drain_apply_exact(");
	generic_install = strstr(source, "\ngcs_block_install_block(");
	UT_ASSERT_NOT_NULL(begin);
	UT_ASSERT_NOT_NULL(abort);
	UT_ASSERT_NOT_NULL(revoke_handler);
	UT_ASSERT_NOT_NULL(drain);
	UT_ASSERT_NOT_NULL(generic_install);
	if (revoke_handler != NULL) {
		UT_ASSERT_NOT_NULL(strstr(revoke_handler, "own_snapshot.pcm_state == (uint8)PCM_STATE_N"));
		UT_ASSERT_NOT_NULL(strstr(revoke_handler, "own_snapshot.pcm_state == (uint8)PCM_STATE_S"));
		UT_ASSERT_NOT_NULL(strstr(revoke_handler, "own_snapshot.pcm_state == (uint8)PCM_STATE_X"));
	}
	if (begin != NULL) {
		UT_ASSERT_NOT_NULL(strstr(begin, "current.pcm_state == (uint8)PCM_STATE_N"));
		UT_ASSERT_NOT_NULL(strstr(begin, "current.pcm_state == (uint8)PCM_STATE_S"));
		UT_ASSERT_NOT_NULL(strstr(begin, "current.pcm_state == (uint8)PCM_STATE_X"));
		UT_ASSERT_NOT_NULL(strstr(begin, "cluster_bufmgr_pcm_own_prepare_n_source_image("));
		UT_ASSERT_NOT_NULL(strstr(begin, "cluster_bufmgr_pcm_own_begin_s_revoke("));
		UT_ASSERT_NOT_NULL(strstr(begin, "cluster_bufmgr_pcm_own_begin_x_revoke("));
		UT_ASSERT_NOT_NULL(strstr(begin, "cluster_pcm_x_revoke_finish_mode("));
		UT_ASSERT_NOT_NULL(strstr(begin, "CLUSTER_PCM_X_REVOKE_FINISH_DROP"));
		copy = strstr(begin, "cluster_bufmgr_copy_block_for_gcs(");
		materialize = strstr(begin, "cluster_gcs_block_dedup_pcm_x_materialize(");
		finish = strstr(begin, "cluster_bufmgr_pcm_own_finish_revoke_retain(");
		publish = strstr(begin, "cluster_gcs_block_dedup_pcm_x_publish_ready_exact(");
		UT_ASSERT_NOT_NULL(copy);
		UT_ASSERT_NOT_NULL(materialize);
		UT_ASSERT_NOT_NULL(finish);
		UT_ASSERT_NOT_NULL(publish);
		if (copy != NULL && materialize != NULL && finish != NULL && publish != NULL)
			UT_ASSERT(copy < materialize && materialize < finish && finish < publish);
	}
	if (abort != NULL) {
		UT_ASSERT_NOT_NULL(strstr(abort, "cluster_bufmgr_pcm_own_abort_n_revoke("));
		UT_ASSERT_NOT_NULL(strstr(abort, "cluster_bufmgr_pcm_own_abort_s_revoke("));
		UT_ASSERT_NOT_NULL(strstr(abort, "cluster_bufmgr_pcm_own_abort_x_revoke("));
	}
	if (drain != NULL) {
		const char *local_drain = strstr(drain, "cluster_pcm_x_local_drain_poll_exact(");
		const char *duplicate_guard
			= local_drain != NULL ? strstr(local_drain, "if (result != PCM_X_QUEUE_OK)") : NULL;
		const char *release_record = strstr(drain, "cluster_gcs_block_dedup_pcm_x_release_exact(");
		const char *finish_mode_gate = strstr(drain, "cluster_pcm_x_revoke_finish_mode(");
		const char *drop_arm = strstr(drain, "CLUSTER_PCM_X_REVOKE_FINISH_DROP");
		const char *release_retained
			= strstr(drain, "cluster_bufmgr_pcm_own_release_retained_image(");

		UT_ASSERT_NOT_NULL(local_drain);
		UT_ASSERT_NOT_NULL(duplicate_guard);
		UT_ASSERT_NOT_NULL(release_record);
		UT_ASSERT_NOT_NULL(finish_mode_gate);
		UT_ASSERT_NOT_NULL(drop_arm);
		UT_ASSERT_NOT_NULL(release_retained);
		if (local_drain != NULL && duplicate_guard != NULL && release_record != NULL
			&& finish_mode_gate != NULL && drop_arm != NULL && release_retained != NULL)
			UT_ASSERT(local_drain < duplicate_guard && duplicate_guard < release_record
					  && release_record < finish_mode_gate && finish_mode_gate < drop_arm
					  && drop_arm < release_retained);
	}
	if (generic_install != NULL) {
		const char *content = strstr(generic_install, "LWLockAcquire(content_lock, LW_EXCLUSIVE)");
		const char *gate
			= strstr(generic_install, "cluster_bufmgr_pcm_x_content_write_permitted(buf)");
		const char *copy = strstr(generic_install, "memcpy(page, block_data");

		UT_ASSERT_NOT_NULL(content);
		UT_ASSERT_NOT_NULL(gate);
		UT_ASSERT_NOT_NULL(copy);
		if (content != NULL && gate != NULL && copy != NULL)
			UT_ASSERT(content < gate && gate < copy);
	}
	free(source);
}


UT_TEST(test_pcm_x_self_and_remote_drain_share_full_image_release_wrapper)
{
	char *source = read_gcs_block_source();
	const char *apply;
	const char *handler;
	const char *handler_end;
	const char *raw_drain;
	const char *stage;
	const char *terminal;
	const char *terminal_end;

	UT_ASSERT_NOT_NULL(source);
	if (source == NULL)
		return;
	handler = strstr(source, "\ncluster_gcs_handle_pcm_x_drain_poll_envelope(");
	terminal = strstr(source, "\ncluster_gcs_pcm_x_terminal_kick(");
	UT_ASSERT_NOT_NULL(handler);
	UT_ASSERT_NOT_NULL(terminal);
	if (handler != NULL) {
		handler_end = strstr(handler + 1, "\n}\n\n\n");
		apply = strstr(handler, "gcs_block_pcm_x_local_drain_apply_exact(");
		raw_drain = strstr(handler, "cluster_pcm_x_local_drain_poll_exact(");
		UT_ASSERT_NOT_NULL(handler_end);
		UT_ASSERT_NOT_NULL(apply);
		if (handler_end != NULL && apply != NULL)
			UT_ASSERT(apply < handler_end);
		if (handler_end != NULL && raw_drain != NULL)
			UT_ASSERT(raw_drain > handler_end);
	}
	if (terminal != NULL) {
		terminal_end = strstr(terminal + 1, "\n}\n\n\n");
		stage = strstr(terminal, "cluster_gcs_pcm_x_stage_frame(PGRAC_IC_MSG_PCM_X_DRAIN_POLL");
		UT_ASSERT_NOT_NULL(terminal_end);
		UT_ASSERT_NOT_NULL(stage);
		if (terminal_end != NULL && stage != NULL)
			UT_ASSERT(stage < terminal_end);
		/* Self is not a special raw-drain arm: every participant, local or
		 * remote, enters the same authenticated envelope handler above. */
		if (terminal_end != NULL) {
			raw_drain = strstr(terminal, "cluster_pcm_x_local_drain_poll_exact(");
			apply = strstr(terminal, "gcs_block_pcm_x_local_drain_apply_exact(");
			if (raw_drain != NULL)
				UT_ASSERT(raw_drain > terminal_end);
			if (apply != NULL)
				UT_ASSERT(apply > terminal_end);
		}
	}
	free(source);
}


UT_TEST(test_pcm_x_ready_admission_marks_before_send_and_rolls_back_refusal)
{
	char *source = read_gcs_block_source();
	const char *begin;
	const char *end;
	const char *mark;
	const char *send;
	const char *rollback;

	if (source == NULL)
		return;
	begin = strstr(source, "\ngcs_block_pcm_x_stage_ready_work(");
	UT_ASSERT_NOT_NULL(begin);
	if (begin == NULL) {
		free(source);
		return;
	}
	end = strstr(begin + 1, "\n}\n\n\n");
	mark = strstr(begin, "cluster_gcs_block_dedup_pcm_x_mark_staged_exact(");
	send = strstr(begin, "cluster_gcs_pcm_x_stage_frame(");
	rollback = strstr(begin, "cluster_gcs_block_dedup_pcm_x_unmark_staged_exact(");
	UT_ASSERT_NOT_NULL(end);
	UT_ASSERT_NOT_NULL(mark);
	UT_ASSERT_NOT_NULL(send);
	UT_ASSERT_NOT_NULL(rollback);
	if (end != NULL && mark != NULL && send != NULL && rollback != NULL)
		UT_ASSERT(mark < send && send < rollback && rollback < end);
	free(source);
}


UT_TEST(test_pcm_x_lms_owner_death_and_restart_audit_fail_closed)
{
	char *gcs_source = read_gcs_block_source();
	char *lms_source = read_source_path(LMS_SOURCE_PATH);
	const char *owner_start;
	const char *owner_exit;
	const char *main_start;
	const char *worker_start;
	const char *main_call;
	const char *worker_call;

	if (gcs_source == NULL || lms_source == NULL) {
		free(gcs_source);
		free(lms_source);
		return;
	}
	owner_exit = strstr(gcs_source, "\ngcs_block_pcm_x_owner_exit(");
	owner_start = strstr(gcs_source, "\ncluster_gcs_block_pcm_x_owner_start(");
	UT_ASSERT_NOT_NULL(owner_exit);
	UT_ASSERT_NOT_NULL(owner_start);
	if (owner_exit != NULL) {
		UT_ASSERT_NOT_NULL(strstr(owner_exit, "code != 0"));
		UT_ASSERT_NOT_NULL(strstr(owner_exit, "cluster_pcm_x_runtime_fail_closed()"));
	}
	if (owner_start != NULL) {
		UT_ASSERT_NOT_NULL(strstr(owner_start, "before_shmem_exit("));
		UT_ASSERT_NOT_NULL(
			strstr(owner_start, "cluster_gcs_block_dedup_pcm_x_restart_audit(worker_id)"));
	}

	main_start = strstr(lms_source, "\nLmsMain(void)");
	worker_start = strstr(lms_source, "\nLmsWorkerMain(int worker_id)");
	UT_ASSERT_NOT_NULL(main_start);
	UT_ASSERT_NOT_NULL(worker_start);
	main_call
		= main_start != NULL ? strstr(main_start, "cluster_gcs_block_pcm_x_owner_start(0)") : NULL;
	worker_call = worker_start != NULL
					  ? strstr(worker_start, "cluster_gcs_block_pcm_x_owner_start(worker_id)")
					  : NULL;
	UT_ASSERT_NOT_NULL(main_call);
	UT_ASSERT_NOT_NULL(worker_call);
	if (main_start != NULL && main_call != NULL)
		UT_ASSERT(main_call < strstr(main_start, "for (;;)"));
	if (worker_start != NULL && worker_call != NULL)
		UT_ASSERT(worker_call < strstr(worker_start, "for (;;)"));
	UT_ASSERT_EQ(count_occurrences(lms_source, "cluster_gcs_block_pcm_x_owner_start("), 2);
	free(gcs_source);
	free(lms_source);
}


UT_TEST(test_pcm_x_image_fetch_intercepts_canonical_id_before_generic_dedup)
{
	char *source = read_gcs_block_source();
	char *handler;
	char *intercept;
	char *generic_lookup;
	char *serve;

	UT_ASSERT_NOT_NULL(source);
	if (source == NULL)
		return;
	handler = strstr(source, "\ncluster_gcs_handle_block_request_envelope(");
	serve = strstr(source, "\ngcs_block_pcm_x_serve_image_fetch(");
	UT_ASSERT_NOT_NULL(handler);
	UT_ASSERT_NOT_NULL(serve);
	if (handler != NULL) {
		intercept = strstr(handler, "gcs_block_pcm_x_serve_image_fetch(env, req, dedup_worker_id)");
		generic_lookup = strstr(handler, "cluster_gcs_block_dedup_lookup_or_register(");
		UT_ASSERT_NOT_NULL(intercept);
		UT_ASSERT_NOT_NULL(generic_lookup);
		if (intercept != NULL && generic_lookup != NULL)
			UT_ASSERT(intercept < generic_lookup);
	}
	if (serve != NULL) {
		UT_ASSERT_NOT_NULL(strstr(serve, "cluster_pcm_x_image_fetch_request_exact("));
		UT_ASSERT(count_occurrences(serve, "cluster_pcm_x_local_holder_progress_exact(") >= 2);
		UT_ASSERT_NOT_NULL(strstr(serve, "gcs_block_pcm_x_authenticated_session("));
		UT_ASSERT_NOT_NULL(strstr(serve, "gcs_block_pcm_x_revalidate_peer_binding("));
		UT_ASSERT_NOT_NULL(strstr(serve, "cluster_gcs_block_dedup_pcm_x_lookup("));
		UT_ASSERT_NOT_NULL(strstr(serve, "gcs_block_resend_cached_reply("));
	}
	free(source);
}


UT_TEST(test_pcm_x_requester_fetch_revalidates_queue_and_reservation_before_install)
{
	char *source = read_gcs_block_source();
	const char *fetch;
	const char *fetch_end;
	const char *backoff_branch;
	const char *backoff_wait;
	const char *backoff_before;
	const char *backoff_after;
	const char *cv_wait;
	const char *cv_timeout;
	const char *cv_before;
	const char *cv_after;
	const char *install_call;
	const char *install;
	const char *install_end;
	const char *install_runtime_before;
	const char *install_publish;
	const char *install_runtime_after;
	const char *recovering_retry;
	const char *recovering_retry_counter;
	const char *reply_validation;
	char *reply_handler;

	UT_ASSERT_NOT_NULL(source);
	if (source == NULL)
		return;
	fetch = strstr(source, "\ncluster_gcs_pcm_x_fetch_image_and_install(");
	fetch_end = fetch != NULL ? strstr(fetch, "\n}\n") : NULL;
	UT_ASSERT_NOT_NULL(fetch);
	UT_ASSERT_NOT_NULL(fetch_end);
	if (fetch != NULL && fetch_end != NULL) {
		UT_ASSERT_NOT_NULL(strstr(fetch, "gcs_block_try_reserve_exact_slot("));
		UT_ASSERT_NOT_NULL(strstr(fetch, "cluster_pcm_x_image_fetch_build_request("));
		UT_ASSERT_NOT_NULL(strstr(fetch, "const PcmXRuntimeSnapshot *request_runtime"));
		UT_ASSERT_NOT_NULL(strstr(fetch, "cluster_pcm_x_image_fetch_reply_exact("));
		UT_ASSERT_NOT_NULL(strstr(fetch, "cluster_pcm_x_image_fetch_reservation_exact("));
		UT_ASSERT_NOT_NULL(strstr(fetch, "gcs_block_pcm_x_install_reserved_image_exact("));
		UT_ASSERT_NOT_NULL(strstr(fetch, "gcs_block_release_slot(slot)"));
		recovering_retry = strstr(fetch, "GCS_BLOCK_REPLY_DENIED_RESOURCE_RECOVERING");
		recovering_retry_counter = strstr(fetch, "pcm_x_image_fetch_recovering_retry_count");
		reply_validation = strstr(fetch, "cluster_pcm_x_image_fetch_reply_exact(");
		UT_ASSERT_NOT_NULL(recovering_retry);
		UT_ASSERT_NOT_NULL(recovering_retry_counter);
		UT_ASSERT(recovering_retry == NULL || recovering_retry < fetch_end);
		UT_ASSERT(recovering_retry_counter == NULL || recovering_retry_counter < fetch_end);
		UT_ASSERT(recovering_retry == NULL || reply_validation == NULL
				  || recovering_retry < reply_validation);
		UT_ASSERT(recovering_retry_counter == NULL || reply_validation == NULL
				  || recovering_retry_counter < reply_validation);
		backoff_branch = strstr(fetch, "if (retry_attempt > 0)");
		backoff_wait
			= backoff_branch != NULL ? strstr(backoff_branch, "(void)WaitLatch(MyLatch") : NULL;
		backoff_before
			= backoff_wait != NULL
				  ? strstr(backoff_branch, "gcs_block_pcm_x_fetch_requester_authority_exact(")
				  : NULL;
		backoff_after
			= backoff_wait != NULL
				  ? strstr(backoff_wait, "gcs_block_pcm_x_fetch_requester_authority_exact(")
				  : NULL;
		cv_wait = strstr(fetch, "ConditionVariableTimedSleep(");
		cv_timeout = strstr(fetch, "timeout_ms = (long)");
		cv_before = cv_timeout != NULL && cv_wait != NULL
						? strstr(cv_timeout, "gcs_block_pcm_x_fetch_requester_authority_exact(")
						: NULL;
		cv_after = cv_wait != NULL
					   ? strstr(cv_wait, "gcs_block_pcm_x_fetch_requester_authority_exact(")
					   : NULL;
		install_call = cv_after != NULL
						   ? strstr(cv_after, "gcs_block_pcm_x_install_reserved_image_exact(")
						   : NULL;
		UT_ASSERT_NOT_NULL(backoff_branch);
		UT_ASSERT_NOT_NULL(backoff_wait);
		UT_ASSERT_NOT_NULL(backoff_before);
		UT_ASSERT_NOT_NULL(backoff_after);
		UT_ASSERT_NOT_NULL(cv_wait);
		UT_ASSERT_NOT_NULL(cv_timeout);
		UT_ASSERT_NOT_NULL(cv_before);
		UT_ASSERT_NOT_NULL(cv_after);
		UT_ASSERT_NOT_NULL(install_call);
		if (backoff_branch != NULL && backoff_before != NULL && backoff_wait != NULL
			&& backoff_after != NULL && cv_timeout != NULL && cv_before != NULL && cv_wait != NULL
			&& cv_after != NULL && install_call != NULL)
			UT_ASSERT(backoff_branch < backoff_before && backoff_before < backoff_wait
					  && backoff_wait < backoff_after && cv_timeout < cv_before
					  && cv_before < cv_wait && cv_wait < cv_after && cv_after < install_call
					  && install_call < fetch_end);
	}
	install = strstr(source, "\ngcs_block_pcm_x_install_reserved_image_exact(");
	install_end = install != NULL ? strstr(install, "\n}\n") : NULL;
	UT_ASSERT_NOT_NULL(install);
	UT_ASSERT_NOT_NULL(install_end);
	if (install != NULL && install_end != NULL) {
		UT_ASSERT_NOT_NULL(strstr(install, "const PcmXRuntimeSnapshot *request_runtime"));
		install_runtime_before = strstr(install, "cluster_gcs_pcm_x_requester_runtime_exact(");
		install_publish = strstr(install, "cluster_bufmgr_pcm_own_publish_installed_x_image(");
		install_runtime_after
			= install_publish != NULL
				  ? strstr(install_publish, "cluster_gcs_pcm_x_requester_runtime_exact(")
				  : NULL;
		UT_ASSERT_NOT_NULL(install_runtime_before);
		UT_ASSERT_NOT_NULL(install_publish);
		UT_ASSERT_NOT_NULL(install_runtime_after);
		if (install_runtime_before != NULL && install_publish != NULL
			&& install_runtime_after != NULL)
			UT_ASSERT(install_runtime_before < install_publish
					  && install_publish < install_runtime_after
					  && install_runtime_after < install_end);
	}
	reply_handler = strstr(source, "\ncluster_gcs_handle_block_reply_envelope(");
	UT_ASSERT_NOT_NULL(reply_handler);
	if (reply_handler != NULL) {
		UT_ASSERT_NOT_NULL(
			strstr(reply_handler, "env->source_node_id != (uint32)hdr->sender_node"));
		UT_ASSERT_NOT_NULL(strstr(reply_handler, "env->dest_node_id != (uint32)cluster_node_id"));
	}
	free(source);
}


UT_TEST(test_pcm_x_self_source_handoff_is_no_copy_and_drain_preserves_x)
{
	char *source = read_gcs_block_source();
	const char *adopt;
	const char *adopt_end;
	const char *adopt_page_check;
	const char *adopt_handoff;
	const char *finish;
	const char *finish_end;
	const char *finish_preflight;
	const char *finish_progress;
	const char *finish_try;
	const char *finish_fail_closed;
	const char *finish_commit;
	const char *finish_catch;
	const char *finish_catch_guard;
	const char *finish_catch_fail_closed;
	const char *finish_catch_release;
	const char *finish_catch_rethrow;
	const char *finish_end_try;
	const char *copy;
	const char *materialize;
	const char *materialize_end;
	const char *self_arm;
	const char *remote_finish;
	const char *drain;
	const char *drain_end;
	const char *verify_x;
	const char *release_record;
	const char *self_return;
	const char *release_retained;

	UT_ASSERT_NOT_NULL(source);
	if (source == NULL)
		return;
	adopt = strstr(source, "\ncluster_gcs_pcm_x_adopt_self_image(");
	finish = strstr(source, "\ncluster_gcs_pcm_x_finish_self_image_x(");
	materialize = strstr(source, "\ngcs_block_pcm_x_materialize_reserved_work(");
	drain = strstr(source, "\ngcs_block_pcm_x_local_drain_apply_exact(");
	UT_ASSERT_NOT_NULL(adopt);
	UT_ASSERT_NOT_NULL(finish);
	UT_ASSERT_NOT_NULL(materialize);
	UT_ASSERT_NOT_NULL(drain);
	if (adopt == NULL || finish == NULL || materialize == NULL || drain == NULL) {
		free(source);
		return;
	}
	adopt_end = finish;
	finish_end = strstr(finish + 1, "\n\n/*");
	materialize_end = strstr(materialize + 1, "\n\n\n");
	drain_end = strstr(drain + 1, "\n\n\n");
	UT_ASSERT_NOT_NULL(finish_end);
	UT_ASSERT_NOT_NULL(materialize_end);
	UT_ASSERT_NOT_NULL(drain_end);
	copy = strstr(adopt, "memcpy(page");
	UT_ASSERT(copy == NULL || copy >= adopt_end);
	adopt_page_check = strstr(adopt, "gcs_block_pcm_x_self_page_exact(");
	adopt_handoff = strstr(adopt, "cluster_bufmgr_pcm_own_handoff_revoke_to_x_reservation(");
	UT_ASSERT_NOT_NULL(adopt_page_check);
	UT_ASSERT_NOT_NULL(adopt_handoff);
	if (adopt_page_check != NULL && adopt_handoff != NULL)
		UT_ASSERT(adopt_page_check < adopt_end && adopt_handoff < adopt_end);
	if (finish_end != NULL) {
		finish_preflight = strstr(finish, "cluster_bufmgr_pcm_own_snapshot(");
		finish_progress = strstr(finish, "cluster_pcm_x_local_progress_exact(");
		finish_try = strstr(finish, "PG_TRY();");
		finish_fail_closed = strstr(finish, "cluster_pcm_x_runtime_fail_closed();");
		finish_commit = strstr(finish, "cluster_bufmgr_pcm_own_finish_x_commit(");
		UT_ASSERT_NOT_NULL(finish_preflight);
		UT_ASSERT_NOT_NULL(finish_progress);
		UT_ASSERT_NOT_NULL(finish_try);
		UT_ASSERT_NOT_NULL(finish_fail_closed);
		UT_ASSERT_NOT_NULL(finish_commit);
		if (finish_preflight != NULL && finish_progress != NULL && finish_try != NULL
			&& finish_fail_closed != NULL && finish_commit != NULL)
			UT_ASSERT(finish_preflight < finish_progress && finish_progress < finish_try
					  && finish_try < finish_commit && finish_commit < finish_end
					  && finish_fail_closed < finish_end);
		finish_catch = finish_try != NULL ? strstr(finish_try, "PG_CATCH();") : NULL;
		finish_catch_guard
			= finish_catch != NULL ? strstr(finish_catch, "if (handoff_live || committed)") : NULL;
		finish_catch_fail_closed
			= finish_catch_guard != NULL
				  ? strstr(finish_catch_guard, "cluster_pcm_x_runtime_fail_closed();")
				  : NULL;
		finish_catch_release = finish_catch_fail_closed != NULL
								   ? strstr(finish_catch_fail_closed,
											"if (content_locked && LWLockHeldByMe(content_lock))")
								   : NULL;
		finish_catch_rethrow
			= finish_catch_release != NULL ? strstr(finish_catch_release, "PG_RE_THROW();") : NULL;
		finish_end_try
			= finish_catch_rethrow != NULL ? strstr(finish_catch_rethrow, "PG_END_TRY();") : NULL;
		UT_ASSERT_NOT_NULL(finish_catch);
		UT_ASSERT_NOT_NULL(finish_catch_guard);
		UT_ASSERT_NOT_NULL(finish_catch_fail_closed);
		UT_ASSERT_NOT_NULL(finish_catch_release);
		UT_ASSERT_NOT_NULL(finish_catch_rethrow);
		UT_ASSERT_NOT_NULL(finish_end_try);
		if (finish_catch != NULL && finish_catch_guard != NULL && finish_catch_fail_closed != NULL
			&& finish_catch_release != NULL && finish_catch_rethrow != NULL
			&& finish_end_try != NULL)
			UT_ASSERT(finish_catch < finish_catch_guard
					  && finish_catch_guard < finish_catch_fail_closed
					  && finish_catch_fail_closed < finish_catch_release
					  && finish_catch_release < finish_catch_rethrow
					  && finish_catch_rethrow < finish_end_try && finish_end_try < finish_end);
	}
	self_arm = strstr(materialize, "if (self_source_handoff)");
	remote_finish = strstr(materialize, "cluster_bufmgr_pcm_own_finish_revoke_retain(");
	UT_ASSERT_NOT_NULL(self_arm);
	UT_ASSERT_NOT_NULL(remote_finish);
	if (self_arm != NULL && remote_finish != NULL && materialize_end != NULL)
		UT_ASSERT(self_arm < remote_finish && remote_finish < materialize_end);

	verify_x = strstr(drain, "cluster_bufmgr_pcm_own_self_handoff_x_exact(");
	release_record = strstr(drain, "cluster_gcs_block_dedup_pcm_x_release_exact(");
	self_return
		= release_record != NULL ? strstr(release_record, "if (self_source_handoff)") : NULL;
	release_retained = strstr(drain, "cluster_bufmgr_pcm_own_release_retained_image(");
	UT_ASSERT_NOT_NULL(verify_x);
	UT_ASSERT_NOT_NULL(release_record);
	UT_ASSERT_NOT_NULL(self_return);
	UT_ASSERT_NOT_NULL(release_retained);
	if (verify_x != NULL && release_record != NULL && self_return != NULL
		&& release_retained != NULL && drain_end != NULL)
		UT_ASSERT(verify_x < release_record && release_record < self_return
				  && self_return < release_retained && release_retained < drain_end);
	free(source);
}


UT_TEST(test_pcm_x_retire_wake_identity_is_wait_generation_exact)
{
	PcmXWaitIdentity identity;
	ClusterLmdWaitStateSnapshot snapshot;

	memset(&identity, 0, sizeof(identity));
	identity.node_id = 2;
	identity.procno = 7;
	identity.xid = (TransactionId)42;
	identity.cluster_epoch = UINT64_C(9);
	identity.request_id = UINT64_C(23);
	identity.wait_seq = UINT64_C(31);
	memset(&snapshot, 0, sizeof(snapshot));
	snapshot.active = true;
	snapshot.kind = CLUSTER_LMD_WAIT_PCM_CONVERT;
	snapshot.request_id = identity.request_id;
	snapshot.cluster_epoch = identity.cluster_epoch;
	snapshot.xid = identity.xid;
	snapshot.wait_seq = identity.wait_seq;

	UT_ASSERT(cluster_gcs_pcm_x_wait_identity_matches(
		&identity, 2, 8, CLUSTER_LMD_WAIT_STATE_READ_ACTIVE, &snapshot));
	UT_ASSERT(!cluster_gcs_pcm_x_wait_identity_matches(
		&identity, 1, 8, CLUSTER_LMD_WAIT_STATE_READ_ACTIVE, &snapshot));
	UT_ASSERT(!cluster_gcs_pcm_x_wait_identity_matches(
		&identity, 2, 7, CLUSTER_LMD_WAIT_STATE_READ_ACTIVE, &snapshot));
	UT_ASSERT(!cluster_gcs_pcm_x_wait_identity_matches(
		&identity, 2, 8, CLUSTER_LMD_WAIT_STATE_READ_BUSY, &snapshot));
	UT_ASSERT(!cluster_gcs_pcm_x_wait_identity_matches(
		&identity, 2, 8, CLUSTER_LMD_WAIT_STATE_READ_INACTIVE, &snapshot));
	snapshot.active = false;
	UT_ASSERT(!cluster_gcs_pcm_x_wait_identity_matches(
		&identity, 2, 8, CLUSTER_LMD_WAIT_STATE_READ_ACTIVE, &snapshot));
	snapshot.active = true;
	snapshot.kind = CLUSTER_LMD_WAIT_GES;
	UT_ASSERT(!cluster_gcs_pcm_x_wait_identity_matches(
		&identity, 2, 8, CLUSTER_LMD_WAIT_STATE_READ_ACTIVE, &snapshot));
	snapshot.kind = CLUSTER_LMD_WAIT_PCM_CONVERT;
	snapshot.request_id++;
	UT_ASSERT(!cluster_gcs_pcm_x_wait_identity_matches(
		&identity, 2, 8, CLUSTER_LMD_WAIT_STATE_READ_ACTIVE, &snapshot));
	snapshot.request_id = identity.request_id;
	snapshot.cluster_epoch++;
	UT_ASSERT(!cluster_gcs_pcm_x_wait_identity_matches(
		&identity, 2, 8, CLUSTER_LMD_WAIT_STATE_READ_ACTIVE, &snapshot));
	snapshot.cluster_epoch = identity.cluster_epoch;
	snapshot.wait_seq++;
	UT_ASSERT(!cluster_gcs_pcm_x_wait_identity_matches(
		&identity, 2, 8, CLUSTER_LMD_WAIT_STATE_READ_ACTIVE, &snapshot));
	snapshot.wait_seq = identity.wait_seq;
	snapshot.xid++;
	UT_ASSERT(!cluster_gcs_pcm_x_wait_identity_matches(
		&identity, 2, 8, CLUSTER_LMD_WAIT_STATE_READ_ACTIVE, &snapshot));
	snapshot.xid = identity.xid;
	identity.request_id = 0;
	UT_ASSERT(!cluster_gcs_pcm_x_wait_identity_matches(
		&identity, 2, 8, CLUSTER_LMD_WAIT_STATE_READ_ACTIVE, &snapshot));
}


UT_TEST(test_pcm_x_requester_driver_owns_fifo_and_transfer_lifecycles)
{
	char *source = read_gcs_block_source();
	const char *driver;
	const char *driver_end;
	const char *rekey_helper;
	const char *rekey_helper_end;
	const char *snapshot_before;
	const char *rekey_exact;
	const char *snapshot_after;
	const char *wrapper;
	const char *wrapper_end;
	const char *cleanup;
	const char *cleanup_end;
	const char *handoff_pointer;
	const char *handoff_active;
	const char *handoff_publish;
	const char *requester_clear;
	const char *cleanup_handoff;
	const char *cleanup_wfg;
	const char *cleanup_release;
	const char *cleanup_abort;
	const char *cleanup_clear_helper;
	const char *cleanup_clear_helper_end;
	const char *cleanup_stale;
	const char *cleanup_stale_clear;
	const char *cleanup_stale_return;
	const char *cleanup_owner_retry;
	const char *cleanup_owner_retry_clear;
	const char *cleanup_owner_retry_return;
	const char *cleanup_fail_closed;
	const char *cleanup_fail_closed_clear;
	const char *cleanup_fail_closed_return;
	const char *formation;
	const char *formation_wait;
	const char *authenticated_session;
	const char *authority_preflight;
	const char *request_id;
	const char *wait_publish;
	const char *nested_guard;
	const char *ownership_snapshot;
	const char *first_wait;
	const char *join;
	const char *leader_rekey;
	const char *claim;
	const char *enqueue;
	const char *follower_snapshot;
	const char *graph_replace;
	const char *graph_clear;
	const char *self_adopt;
	const char *remote_fetch;
	const char *install_ready;
	const char *self_finish;
	const char *remote_finish;
	const char *final_ack;
	const char *final_confirm;
	const char *clear_wait;
	const char *authority_helper;
	const char *authority_helper_end;
	const char *authority_runtime;
	const char *authority_runtime_exact;
	const char *authority_peer;
	const char *wait_exact;
	const char *wait_exact_end;
	const char *wait_exact_first_authority;
	const char *wait_exact_physical;
	const char *wait_exact_second_authority;
	const char *wait_scan;
	int raw_wait_count = 0;
	int exact_wait_count = 0;

	UT_ASSERT_NOT_NULL(source);
	if (source == NULL)
		return;
	driver = strstr(source, "\ngcs_block_pcm_x_acquire_writer_impl(");
	driver_end = driver != NULL ? strstr(driver, "\n}\n") : NULL;
	rekey_helper = strstr(source, "\ngcs_block_pcm_x_rekey_leader_base_exact(");
	rekey_helper_end = rekey_helper != NULL ? strstr(rekey_helper, "\n}\n") : NULL;
	wrapper = strstr(source, "\ncluster_gcs_pcm_x_acquire_writer(");
	wrapper_end = wrapper != NULL ? strstr(wrapper, "\n}\n") : NULL;
	cleanup = strstr(source, "\ngcs_block_pcm_x_requester_cleanup_impl(");
	cleanup_end = cleanup != NULL ? strstr(cleanup, "\n}\n") : NULL;
	cleanup_clear_helper = strstr(source, "\ngcs_block_pcm_x_requester_clear_wait(");
	cleanup_clear_helper_end
		= cleanup_clear_helper != NULL ? strstr(cleanup_clear_helper, "\n}\n") : NULL;
	authority_helper = strstr(source, "\ngcs_block_pcm_x_requester_authority_exact(");
	authority_helper_end = authority_helper != NULL ? strstr(authority_helper, "\n}\n") : NULL;
	wait_exact = strstr(source, "\ngcs_block_pcm_x_requester_wait_exact(");
	wait_exact_end = wait_exact != NULL ? strstr(wait_exact, "\n}\n") : NULL;
	UT_ASSERT_NOT_NULL(driver);
	UT_ASSERT_NOT_NULL(driver_end);
	UT_ASSERT_NOT_NULL(rekey_helper);
	UT_ASSERT_NOT_NULL(rekey_helper_end);
	UT_ASSERT_NOT_NULL(wrapper);
	UT_ASSERT_NOT_NULL(wrapper_end);
	UT_ASSERT_NOT_NULL(cleanup);
	UT_ASSERT_NOT_NULL(cleanup_end);
	UT_ASSERT_NOT_NULL(cleanup_clear_helper);
	UT_ASSERT_NOT_NULL(cleanup_clear_helper_end);
	UT_ASSERT_NOT_NULL(authority_helper);
	UT_ASSERT_NOT_NULL(authority_helper_end);
	UT_ASSERT_NOT_NULL(wait_exact);
	UT_ASSERT_NOT_NULL(wait_exact_end);
	if (driver == NULL || driver_end == NULL || wrapper == NULL || wrapper_end == NULL) {
		free(source);
		return;
	}
	UT_ASSERT_NOT_NULL(strstr(driver, "ClusterPcmXConvertShmem == NULL"));
	UT_ASSERT_NOT_NULL(strstr(wrapper, "PG_TRY();"));
	UT_ASSERT_NOT_NULL(strstr(wrapper, "gcs_block_pcm_x_requester_cleanup_noexcept(cleanup)"));
	UT_ASSERT_NOT_NULL(strstr(wrapper, "ReThrowError(original_error)"));
	handoff_pointer = strstr(wrapper, "cleanup->claim_handoff_out = claim_handed_off");
	handoff_active
		= handoff_pointer != NULL ? strstr(handoff_pointer, "cleanup->active = true") : NULL;
	handoff_publish = strstr(wrapper, "*claim_handed_off = true");
	requester_clear = handoff_publish != NULL ? strstr(handoff_publish, "memset(cleanup, 0") : NULL;
	UT_ASSERT_NOT_NULL(handoff_pointer);
	UT_ASSERT_NOT_NULL(handoff_active);
	UT_ASSERT_NOT_NULL(handoff_publish);
	UT_ASSERT_NOT_NULL(requester_clear);
	if (handoff_pointer != NULL && handoff_active != NULL && handoff_publish != NULL
		&& requester_clear != NULL)
		UT_ASSERT(handoff_pointer < handoff_active && handoff_publish < requester_clear
				  && requester_clear < wrapper_end);
	cleanup_handoff = cleanup != NULL ? strstr(cleanup, "cleanup->claim_handoff_out") : NULL;
	cleanup_wfg = cleanup != NULL ? strstr(cleanup, "if (cleanup->wfg_live)") : NULL;
	cleanup_release = cleanup != NULL
						  ? strstr(cleanup, "cluster_gcs_pcm_x_writer_claim_release_and_wake_exact")
						  : NULL;
	cleanup_abort
		= cleanup != NULL ? strstr(cleanup, "cluster_pcm_x_local_writer_claim_abort_exact") : NULL;
	UT_ASSERT_NOT_NULL(cleanup_handoff);
	UT_ASSERT_NOT_NULL(cleanup_wfg);
	UT_ASSERT_NOT_NULL(cleanup_release);
	UT_ASSERT(cleanup_abort == NULL || cleanup_abort >= cleanup_end);
	if (cleanup_handoff != NULL && cleanup_wfg != NULL && cleanup_end != NULL)
		UT_ASSERT(cleanup_handoff < cleanup_wfg && cleanup_wfg < cleanup_end);
	if (cleanup_clear_helper != NULL && cleanup_clear_helper_end != NULL) {
		UT_ASSERT_NOT_NULL(strstr(cleanup_clear_helper, "cleanup->wait_published"));
		UT_ASSERT_NOT_NULL(strstr(cleanup_clear_helper, "cluster_lmd_wait_state_clear("));
		UT_ASSERT_NOT_NULL(strstr(cleanup_clear_helper, "cleanup->wait_published = false"));
	}
	cleanup_stale = cleanup != NULL ? strstr(cleanup, "CLUSTER_LMD_GRAPH_REMOVE_STALE") : NULL;
	cleanup_stale_clear
		= cleanup_stale != NULL
			  ? strstr(cleanup_stale, "gcs_block_pcm_x_requester_clear_wait(cleanup)")
			  : NULL;
	cleanup_stale_return = cleanup_stale_clear != NULL
							   ? strstr(cleanup_stale_clear, "return PCM_X_QUEUE_CORRUPT")
							   : NULL;
	cleanup_owner_retry
		= cleanup != NULL ? strstr(cleanup, "== CLUSTER_PCM_X_OWNER_EXIT_RETRY") : NULL;
	cleanup_owner_retry_clear
		= cleanup_owner_retry != NULL
			  ? strstr(cleanup_owner_retry, "gcs_block_pcm_x_requester_clear_wait(cleanup)")
			  : NULL;
	cleanup_owner_retry_return = cleanup_owner_retry_clear != NULL
									 ? strstr(cleanup_owner_retry_clear, "return result;")
									 : NULL;
	cleanup_fail_closed
		= cleanup_owner_retry_return != NULL
			  ? strstr(cleanup_owner_retry_return, "cluster_pcm_x_runtime_fail_closed()")
			  : NULL;
	cleanup_fail_closed_clear
		= cleanup_fail_closed != NULL
			  ? strstr(cleanup_fail_closed, "gcs_block_pcm_x_requester_clear_wait(cleanup)")
			  : NULL;
	cleanup_fail_closed_return = cleanup_fail_closed_clear != NULL
									 ? strstr(cleanup_fail_closed_clear, "return result;")
									 : NULL;
	UT_ASSERT_NOT_NULL(cleanup_stale_clear);
	UT_ASSERT_NOT_NULL(cleanup_stale_return);
	UT_ASSERT_NOT_NULL(cleanup_owner_retry_clear);
	UT_ASSERT_NOT_NULL(cleanup_owner_retry_return);
	UT_ASSERT_NOT_NULL(cleanup_fail_closed_clear);
	UT_ASSERT_NOT_NULL(cleanup_fail_closed_return);
	if (cleanup_stale_clear != NULL && cleanup_stale_return != NULL)
		UT_ASSERT(cleanup_stale_clear < cleanup_stale_return && cleanup_stale_return < cleanup_end);
	if (cleanup_owner_retry_clear != NULL && cleanup_owner_retry_return != NULL)
		UT_ASSERT(cleanup_owner_retry_clear < cleanup_owner_retry_return
				  && cleanup_owner_retry_return < cleanup_end);
	if (cleanup_fail_closed_clear != NULL && cleanup_fail_closed_return != NULL)
		UT_ASSERT(cleanup_fail_closed_clear < cleanup_fail_closed_return
				  && cleanup_fail_closed_return < cleanup_end);
	authority_runtime = authority_helper != NULL
							? strstr(authority_helper, "cluster_pcm_x_runtime_snapshot()")
							: NULL;
	authority_runtime_exact
		= authority_runtime != NULL
			  ? strstr(authority_runtime, "cluster_gcs_pcm_x_requester_runtime_exact(")
			  : NULL;
	authority_peer
		= authority_runtime_exact != NULL
			  ? strstr(authority_runtime_exact, "gcs_block_pcm_x_revalidate_peer_binding(")
			  : NULL;
	wait_exact_first_authority
		= wait_exact != NULL ? strstr(wait_exact, "gcs_block_pcm_x_requester_authority_exact(")
							 : NULL;
	wait_exact_physical
		= wait_exact_first_authority != NULL
			  ? strstr(wait_exact_first_authority, "gcs_block_pcm_x_requester_wait(wait_index)")
			  : NULL;
	wait_exact_second_authority
		= wait_exact_physical != NULL
			  ? strstr(wait_exact_physical + 1, "gcs_block_pcm_x_requester_authority_exact(")
			  : NULL;
	UT_ASSERT_NOT_NULL(authority_runtime);
	UT_ASSERT_NOT_NULL(authority_runtime_exact);
	UT_ASSERT_NOT_NULL(authority_peer);
	UT_ASSERT_NOT_NULL(wait_exact_first_authority);
	UT_ASSERT_NOT_NULL(wait_exact_physical);
	UT_ASSERT_NOT_NULL(wait_exact_second_authority);
	if (authority_runtime != NULL && authority_runtime_exact != NULL && authority_peer != NULL
		&& authority_helper_end != NULL)
		UT_ASSERT(authority_runtime < authority_runtime_exact
				  && authority_runtime_exact < authority_peer
				  && authority_peer < authority_helper_end);
	if (wait_exact_first_authority != NULL && wait_exact_physical != NULL
		&& wait_exact_second_authority != NULL && wait_exact_end != NULL)
		UT_ASSERT(wait_exact_first_authority < wait_exact_physical
				  && wait_exact_physical < wait_exact_second_authority
				  && wait_exact_second_authority < wait_exact_end);
	formation = strstr(driver, "cluster_gcs_pcm_x_requester_formation_action(");
	formation_wait = formation != NULL
						 ? strstr(formation, "gcs_block_pcm_x_requester_wait(&wait_index)")
						 : NULL;
	authenticated_session = formation_wait != NULL
								? strstr(formation_wait, "gcs_block_pcm_x_authenticated_session(")
								: NULL;
	authority_preflight
		= authenticated_session != NULL
			  ? strstr(authenticated_session, "gcs_block_pcm_x_requester_authority_exact(")
			  : NULL;
	request_id = authority_preflight != NULL
					 ? strstr(authority_preflight, "gcs_block_pcm_x_next_request_id(")
					 : NULL;
	wait_publish
		= request_id != NULL ? strstr(request_id, "cluster_lmd_wait_state_publish(") : NULL;
	nested_guard = wait_publish != NULL
					   ? strstr(wait_publish, "cluster_pcm_x_nested_wait_guard_before_block()")
					   : NULL;
	ownership_snapshot
		= nested_guard != NULL ? strstr(nested_guard, "cluster_bufmgr_pcm_own_snapshot(buf") : NULL;
	first_wait = ownership_snapshot != NULL
					 ? strstr(ownership_snapshot, "gcs_block_pcm_x_requester_wait_exact(")
					 : NULL;
	UT_ASSERT_NOT_NULL(formation);
	UT_ASSERT_NOT_NULL(formation_wait);
	UT_ASSERT_NOT_NULL(authenticated_session);
	UT_ASSERT_NOT_NULL(authority_preflight);
	UT_ASSERT_NOT_NULL(request_id);
	UT_ASSERT_NOT_NULL(wait_publish);
	UT_ASSERT_NOT_NULL(nested_guard);
	UT_ASSERT_NOT_NULL(ownership_snapshot);
	UT_ASSERT_NOT_NULL(first_wait);
	if (formation != NULL && formation_wait != NULL && authenticated_session != NULL
		&& authority_preflight != NULL && request_id != NULL && wait_publish != NULL
		&& nested_guard != NULL && ownership_snapshot != NULL && first_wait != NULL
		&& driver_end != NULL)
		UT_ASSERT(formation < formation_wait && formation_wait < authenticated_session
				  && authenticated_session < authority_preflight && authority_preflight < request_id
				  && request_id < wait_publish && wait_publish < nested_guard
				  && nested_guard < ownership_snapshot && ownership_snapshot < first_wait
				  && first_wait < driver_end);
	wait_scan = driver;
	while (wait_scan != NULL
		   && (wait_scan = strstr(wait_scan, "gcs_block_pcm_x_requester_wait(&wait_index)")) != NULL
		   && wait_scan < driver_end) {
		raw_wait_count++;
		wait_scan++;
	}
	wait_scan = driver;
	while (wait_scan != NULL
		   && (wait_scan = strstr(wait_scan, "gcs_block_pcm_x_requester_wait_exact(")) != NULL
		   && wait_scan < driver_end) {
		exact_wait_count++;
		wait_scan++;
	}
	UT_ASSERT_EQ(raw_wait_count, 1);
	UT_ASSERT_EQ(exact_wait_count, 9);
	snapshot_before
		= rekey_helper != NULL ? strstr(rekey_helper, "cluster_bufmgr_pcm_own_snapshot(") : NULL;
	rekey_exact
		= snapshot_before != NULL
			  ? strstr(snapshot_before, "cluster_pcm_x_local_leader_rekey_generation_exact(")
			  : NULL;
	snapshot_after
		= rekey_exact != NULL ? strstr(rekey_exact, "cluster_bufmgr_pcm_own_snapshot(") : NULL;
	UT_ASSERT_NOT_NULL(snapshot_before);
	UT_ASSERT_NOT_NULL(rekey_exact);
	UT_ASSERT_NOT_NULL(snapshot_after);
	if (snapshot_before != NULL && rekey_exact != NULL && snapshot_after != NULL
		&& rekey_helper_end != NULL)
		UT_ASSERT(snapshot_before < rekey_exact && rekey_exact < snapshot_after
				  && snapshot_after < rekey_helper_end);
	join = strstr(driver, "cluster_pcm_x_local_join_begin(");
	leader_rekey = join != NULL ? strstr(join, "gcs_block_pcm_x_rekey_leader_base_exact(") : NULL;
	claim = leader_rekey != NULL ? strstr(leader_rekey, "cluster_pcm_x_local_writer_claim_exact(")
								 : NULL;
	enqueue = claim != NULL ? strstr(claim, "cluster_pcm_x_local_enqueue_arm_exact(") : NULL;
	graph_clear = strstr(driver, "cluster_pcm_x_local_follower_wfg_clear_exact(");
	follower_snapshot
		= graph_clear != NULL
			  ? strstr(graph_clear, "cluster_pcm_x_local_follower_wfg_snapshot_exact(")
			  : NULL;
	graph_replace = follower_snapshot != NULL
						? strstr(follower_snapshot, "cluster_lmd_graph_replace_waiter_edges_exact(")
						: NULL;
	self_adopt = strstr(driver, "cluster_gcs_pcm_x_adopt_self_image(");
	remote_fetch = strstr(driver, "cluster_gcs_pcm_x_fetch_image_and_install(");
	install_ready = strstr(driver, "cluster_pcm_x_local_install_ready_arm_exact(");
	self_finish = strstr(driver, "cluster_gcs_pcm_x_finish_self_image_x(");
	remote_finish = strstr(driver, "cluster_bufmgr_pcm_own_finish_x_commit(");
	final_ack = strstr(driver, "cluster_pcm_x_local_final_ack_arm_exact(");
	final_confirm = strstr(driver, "PGRAC_IC_MSG_PCM_X_FINAL_CONFIRM");
	clear_wait = strstr(driver, "cluster_lmd_wait_state_clear(");

	UT_ASSERT_NOT_NULL(join);
	UT_ASSERT_NOT_NULL(leader_rekey);
	UT_ASSERT_NOT_NULL(claim);
	UT_ASSERT_NOT_NULL(enqueue);
	UT_ASSERT_NOT_NULL(follower_snapshot);
	UT_ASSERT_NOT_NULL(graph_replace);
	UT_ASSERT_NOT_NULL(graph_clear);
	UT_ASSERT_NOT_NULL(self_adopt);
	UT_ASSERT_NOT_NULL(remote_fetch);
	UT_ASSERT_NOT_NULL(install_ready);
	UT_ASSERT_NOT_NULL(self_finish);
	UT_ASSERT_NOT_NULL(remote_finish);
	UT_ASSERT_NOT_NULL(final_ack);
	UT_ASSERT_NOT_NULL(final_confirm);
	UT_ASSERT_NOT_NULL(clear_wait);
	if (join != NULL && leader_rekey != NULL && claim != NULL && enqueue != NULL)
		UT_ASSERT(join < leader_rekey && leader_rekey < claim && claim < enqueue
				  && enqueue < driver_end);
	UT_ASSERT_NOT_NULL(strstr(driver, "cluster_pcm_x_local_lookup_exact(&handle.identity"));
	UT_ASSERT_NOT_NULL(
		strstr(driver, "cluster_gcs_pcm_x_role_refresh_exact(&handle, &fresh_handle)"));
	UT_ASSERT_NOT_NULL(strstr(driver, "initial_own.pcm_state != (uint8)PCM_STATE_X"));
	{
		const char *preflight;
		const char *preflight_retry;

		preflight = strstr(driver, "cluster_gcs_pcm_x_remote_reservation_preflight(");
		preflight_retry
			= preflight != NULL
				  ? strstr(preflight, "GCS_BLOCK_PCM_X_RETRY_SITE_RESERVATION_PREFLIGHT")
				  : NULL;
		UT_ASSERT_NOT_NULL(preflight);
		UT_ASSERT_NOT_NULL(preflight_retry);
		/* The transient-BUSY wait dispatch must sit between the preflight and
		 * the image fetch;  a non-wait verdict is the only fail-closed exit. */
		if (preflight != NULL && preflight_retry != NULL && remote_fetch != NULL)
			UT_ASSERT(preflight < preflight_retry && preflight_retry < remote_fetch);
	}
	if (follower_snapshot != NULL && graph_replace != NULL && graph_clear != NULL)
		UT_ASSERT(graph_clear < follower_snapshot && follower_snapshot < graph_replace
				  && graph_replace < driver_end);
	if (self_adopt != NULL && remote_fetch != NULL && install_ready != NULL)
		UT_ASSERT(self_adopt < install_ready && remote_fetch < install_ready);
	if (self_finish != NULL && remote_finish != NULL && final_ack != NULL)
		UT_ASSERT(self_finish < final_ack && remote_finish < final_ack);
	if (final_confirm != NULL && clear_wait != NULL)
		UT_ASSERT(final_confirm < clear_wait && clear_wait < driver_end);
	free(source);
}


UT_TEST(test_pcm_x_requester_retry_policy_is_operation_exact)
{
	PcmXRuntimeSnapshot current;
	PcmXRuntimeSnapshot start;
	const struct {
		GcsBlockPcmXRequesterSite site;
		PcmXQueueResult result;
		GcsBlockPcmXRetryAction action;
	} cases[] = {
		{ GCS_BLOCK_PCM_X_RETRY_SITE_JOIN, PCM_X_QUEUE_NO_CAPACITY, GCS_BLOCK_PCM_X_RETRY_WAIT },
		{ GCS_BLOCK_PCM_X_RETRY_SITE_JOIN, PCM_X_QUEUE_STALE, GCS_BLOCK_PCM_X_RETRY_FAIL_CLOSED },
		{ GCS_BLOCK_PCM_X_RETRY_SITE_LEADER_REKEY, PCM_X_QUEUE_BUSY, GCS_BLOCK_PCM_X_RETRY_WAIT },
		{ GCS_BLOCK_PCM_X_RETRY_SITE_LEADER_REKEY, PCM_X_QUEUE_STALE,
		  GCS_BLOCK_PCM_X_RETRY_FAIL_CLOSED },
		{ GCS_BLOCK_PCM_X_RETRY_SITE_ROLE_REFRESH, PCM_X_QUEUE_BUSY, GCS_BLOCK_PCM_X_RETRY_WAIT },
		{ GCS_BLOCK_PCM_X_RETRY_SITE_ROLE_REFRESH, PCM_X_QUEUE_GATE_RETRY,
		  GCS_BLOCK_PCM_X_RETRY_WAIT },
		{ GCS_BLOCK_PCM_X_RETRY_SITE_ROLE_REFRESH, PCM_X_QUEUE_STALE,
		  GCS_BLOCK_PCM_X_RETRY_FAIL_CLOSED },
		{ GCS_BLOCK_PCM_X_RETRY_SITE_ROLE_REFRESH, PCM_X_QUEUE_NOT_FOUND,
		  GCS_BLOCK_PCM_X_RETRY_FAIL_CLOSED },
		{ GCS_BLOCK_PCM_X_RETRY_SITE_CLAIM, PCM_X_QUEUE_BUSY, GCS_BLOCK_PCM_X_RETRY_WAIT },
		{ GCS_BLOCK_PCM_X_RETRY_SITE_CLAIM, PCM_X_QUEUE_NO_CAPACITY,
		  GCS_BLOCK_PCM_X_RETRY_FAIL_CLOSED },
		{ GCS_BLOCK_PCM_X_RETRY_SITE_CUTOFF, PCM_X_QUEUE_GATE_RETRY, GCS_BLOCK_PCM_X_RETRY_WAIT },
		{ GCS_BLOCK_PCM_X_RETRY_SITE_CUTOFF, PCM_X_QUEUE_BAD_STATE,
		  GCS_BLOCK_PCM_X_RETRY_FAIL_CLOSED },
		{ GCS_BLOCK_PCM_X_RETRY_SITE_FOLLOWER_SNAPSHOT, PCM_X_QUEUE_BARRIER_CLOSED,
		  GCS_BLOCK_PCM_X_RETRY_WAIT },
		/* STALE at the follower WFG snapshot is the same slot-churn signal
		 * (promotion / round advance) the claim site recovers from with a
		 * refresh lookup;  it must re-dispatch, not close the runtime. */
		{ GCS_BLOCK_PCM_X_RETRY_SITE_FOLLOWER_SNAPSHOT, PCM_X_QUEUE_STALE,
		  GCS_BLOCK_PCM_X_RETRY_REFRESH_ROLE },
		{ GCS_BLOCK_PCM_X_RETRY_SITE_FOLLOWER_SNAPSHOT, PCM_X_QUEUE_CORRUPT,
		  GCS_BLOCK_PCM_X_RETRY_FAIL_CLOSED },
		{ GCS_BLOCK_PCM_X_RETRY_SITE_WFG_COMMIT, PCM_X_QUEUE_STALE,
		  GCS_BLOCK_PCM_X_RETRY_RESNAPSHOT_WFG },
		{ GCS_BLOCK_PCM_X_RETRY_SITE_WFG_COMMIT, PCM_X_QUEUE_CORRUPT,
		  GCS_BLOCK_PCM_X_RETRY_FAIL_CLOSED },
		{ GCS_BLOCK_PCM_X_RETRY_SITE_WFG_CLEAR, PCM_X_QUEUE_NOT_READY,
		  GCS_BLOCK_PCM_X_RETRY_FAIL_CLOSED },
		{ GCS_BLOCK_PCM_X_RETRY_SITE_PROGRESS, PCM_X_QUEUE_NOT_READY, GCS_BLOCK_PCM_X_RETRY_WAIT },
		{ GCS_BLOCK_PCM_X_RETRY_SITE_PROGRESS, PCM_X_QUEUE_BUSY, GCS_BLOCK_PCM_X_RETRY_WAIT },
		{ GCS_BLOCK_PCM_X_RETRY_SITE_PROGRESS, PCM_X_QUEUE_STALE,
		  GCS_BLOCK_PCM_X_RETRY_FAIL_CLOSED },
		{ GCS_BLOCK_PCM_X_RETRY_SITE_PRECOMMIT_ARM, PCM_X_QUEUE_BAD_STATE,
		  GCS_BLOCK_PCM_X_RETRY_RELOAD_PROGRESS },
		{ GCS_BLOCK_PCM_X_RETRY_SITE_PRECOMMIT_ARM, PCM_X_QUEUE_STALE,
		  GCS_BLOCK_PCM_X_RETRY_FAIL_CLOSED },
		{ GCS_BLOCK_PCM_X_RETRY_SITE_IMAGE_FETCH, PCM_X_QUEUE_BUSY, GCS_BLOCK_PCM_X_RETRY_WAIT },
		{ GCS_BLOCK_PCM_X_RETRY_SITE_IMAGE_FETCH, PCM_X_QUEUE_BARRIER_CLOSED,
		  GCS_BLOCK_PCM_X_RETRY_FAIL_CLOSED },
		{ GCS_BLOCK_PCM_X_RETRY_SITE_POSTCOMMIT_ARM, PCM_X_QUEUE_NOT_READY,
		  GCS_BLOCK_PCM_X_RETRY_FAIL_CLOSED },
		{ GCS_BLOCK_PCM_X_RETRY_SITE_POSTCOMMIT_ARM, PCM_X_QUEUE_BAD_STATE,
		  GCS_BLOCK_PCM_X_RETRY_FAIL_CLOSED },
		{ GCS_BLOCK_PCM_X_RETRY_SITE_POSTCOMMIT_REPLAY_ARM, PCM_X_QUEUE_BAD_STATE,
		  GCS_BLOCK_PCM_X_RETRY_RELOAD_PROGRESS },
		{ GCS_BLOCK_PCM_X_RETRY_SITE_POSTCOMMIT_REPLAY_ARM, PCM_X_QUEUE_STALE,
		  GCS_BLOCK_PCM_X_RETRY_FAIL_CLOSED },
		/* Remote-reservation preflight: BUSY is a transient own-slot lifecycle
		 * (a revoke/grant flag mid-flight on this node) and must wait, never
		 * close the runtime.  STALE stays fail-closed for now: it means the
		 * enqueue-time base_own_generation was consumed by a revoke while the
		 * request was queued, which the grant/final-ack chain cannot absorb
		 * without a master-visible rebase (pending protocol amendment). */
		{ GCS_BLOCK_PCM_X_RETRY_SITE_RESERVATION_PREFLIGHT, PCM_X_QUEUE_BUSY,
		  GCS_BLOCK_PCM_X_RETRY_WAIT },
		{ GCS_BLOCK_PCM_X_RETRY_SITE_RESERVATION_PREFLIGHT, PCM_X_QUEUE_STALE,
		  GCS_BLOCK_PCM_X_RETRY_FAIL_CLOSED },
		{ GCS_BLOCK_PCM_X_RETRY_SITE_RESERVATION_PREFLIGHT, PCM_X_QUEUE_CORRUPT,
		  GCS_BLOCK_PCM_X_RETRY_FAIL_CLOSED },
		{ GCS_BLOCK_PCM_X_RETRY_SITE_RESERVATION_PREFLIGHT, PCM_X_QUEUE_COUNTER_EXHAUSTED,
		  GCS_BLOCK_PCM_X_RETRY_FAIL_CLOSED },
	};
	size_t i;

	for (i = 0; i < lengthof(cases); i++)
		UT_ASSERT_EQ(cluster_gcs_pcm_x_requester_retry_action(cases[i].site, cases[i].result),
					 cases[i].action);

	UT_ASSERT(cluster_gcs_pcm_x_nested_guard_retryable(PCM_X_QUEUE_BUSY));
	UT_ASSERT(cluster_gcs_pcm_x_nested_guard_retryable(PCM_X_QUEUE_GATE_RETRY));
	UT_ASSERT(cluster_gcs_pcm_x_nested_guard_retryable(PCM_X_QUEUE_STALE));
	UT_ASSERT(!cluster_gcs_pcm_x_nested_guard_retryable(PCM_X_QUEUE_BARRIER_CLOSED));
	UT_ASSERT(!cluster_gcs_pcm_x_nested_guard_retryable(PCM_X_QUEUE_NOT_READY));
	UT_ASSERT(!cluster_gcs_pcm_x_nested_guard_retryable(PCM_X_QUEUE_CORRUPT));

	memset(&start, 0, sizeof(start));
	UT_ASSERT_EQ(cluster_gcs_pcm_x_requester_formation_action(&start),
				 GCS_BLOCK_PCM_X_FORMATION_WAIT);
	start.state = PCM_X_RUNTIME_ACTIVE;
	start.gate_generation = 7;
	start.master_session_incarnation = 11;
	UT_ASSERT_EQ(cluster_gcs_pcm_x_requester_formation_action(&start),
				 GCS_BLOCK_PCM_X_FORMATION_PROCEED);
	current = start;
	UT_ASSERT(cluster_gcs_pcm_x_requester_runtime_exact(&start, &current));
	current = start;
	current.gate_generation = 0;
	UT_ASSERT_EQ(cluster_gcs_pcm_x_requester_formation_action(&current),
				 GCS_BLOCK_PCM_X_FORMATION_FAIL_CLOSED);
	current = start;
	current.master_session_incarnation = 0;
	UT_ASSERT_EQ(cluster_gcs_pcm_x_requester_formation_action(&current),
				 GCS_BLOCK_PCM_X_FORMATION_FAIL_CLOSED);
	current = start;
	current.gate_generation++;
	UT_ASSERT(!cluster_gcs_pcm_x_requester_runtime_exact(&start, &current));
	current = start;
	current.state = PCM_X_RUNTIME_RECOVERY_BLOCKED;
	UT_ASSERT_EQ(cluster_gcs_pcm_x_requester_formation_action(&current),
				 GCS_BLOCK_PCM_X_FORMATION_FAIL_CLOSED);
	UT_ASSERT(!cluster_gcs_pcm_x_requester_runtime_exact(&start, &current));
	memset(&current, 0, sizeof(current));
	current.gate_generation = 1;
	UT_ASSERT_EQ(cluster_gcs_pcm_x_requester_formation_action(&current),
				 GCS_BLOCK_PCM_X_FORMATION_FAIL_CLOSED);
	memset(&current, 0, sizeof(current));
	current.master_session_incarnation = 1;
	UT_ASSERT_EQ(cluster_gcs_pcm_x_requester_formation_action(&current),
				 GCS_BLOCK_PCM_X_FORMATION_FAIL_CLOSED);
	current = start;
	current.state = PCM_X_RUNTIME_SHUTTING_DOWN;
	UT_ASSERT_EQ(cluster_gcs_pcm_x_requester_formation_action(&current),
				 GCS_BLOCK_PCM_X_FORMATION_FAIL_CLOSED);
}


/* One CLAIMED-without-cookie observation is a legal cancel/serve two-phase
 * window;  only a per-ticket streak that survives consecutive periodic drive
 * ticks may close the runtime.  Any settled drive for the tag resets it. */
UT_TEST(test_pcm_x_drive_anomaly_streak_gates_fail_closed)
{
	GcsBlockPcmXDriveAnomaly table[4];
	BufferTag tag_a;
	BufferTag tag_b;
	uint32 i;

	memset(table, 0, sizeof(table));
	memset(&tag_a, 0, sizeof(tag_a));
	memset(&tag_b, 0, sizeof(tag_b));
	tag_a.blockNum = 1;
	tag_b.blockNum = 2;

	for (i = 1; i < GCS_BLOCK_PCM_X_DRIVE_ANOMALY_FUSE; i++)
		UT_ASSERT(!cluster_gcs_pcm_x_drive_anomaly_note(table, 4, &tag_a, 7));
	UT_ASSERT(cluster_gcs_pcm_x_drive_anomaly_note(table, 4, &tag_a, 7));

	/* A different ticket on the same tag counts independently. */
	UT_ASSERT(!cluster_gcs_pcm_x_drive_anomaly_note(table, 4, &tag_a, 8));
	UT_ASSERT(!cluster_gcs_pcm_x_drive_anomaly_note(table, 4, &tag_b, 7));

	/* Settling the tag clears every streak bound to it, and only it. */
	cluster_gcs_pcm_x_drive_anomaly_settle(table, 4, &tag_a);
	UT_ASSERT(!cluster_gcs_pcm_x_drive_anomaly_note(table, 4, &tag_a, 7));
	for (i = 2; i < GCS_BLOCK_PCM_X_DRIVE_ANOMALY_FUSE; i++)
		UT_ASSERT(!cluster_gcs_pcm_x_drive_anomaly_note(table, 4, &tag_b, 7));
	UT_ASSERT(cluster_gcs_pcm_x_drive_anomaly_note(table, 4, &tag_b, 7));

	/* A missing tracking substrate stays fail-closed. */
	UT_ASSERT(cluster_gcs_pcm_x_drive_anomaly_note(NULL, 4, &tag_a, 7));
	UT_ASSERT(cluster_gcs_pcm_x_drive_anomaly_note(table, 0, &tag_a, 7));

	/* Table pressure recycles the lowest streak:  stale one-shot entries are
	 * evicted while a persisting anomaly keeps its count uninterrupted. */
	cluster_gcs_pcm_x_drive_anomaly_settle(table, 4, &tag_a);
	cluster_gcs_pcm_x_drive_anomaly_settle(table, 4, &tag_b);
	tag_a.blockNum = 50;
	for (i = 1; i <= 3; i++)
		UT_ASSERT(!cluster_gcs_pcm_x_drive_anomaly_note(table, 4, &tag_a, 9));
	tag_a.blockNum = 100;
	UT_ASSERT(!cluster_gcs_pcm_x_drive_anomaly_note(table, 4, &tag_a, 1));
	tag_a.blockNum = 101;
	UT_ASSERT(!cluster_gcs_pcm_x_drive_anomaly_note(table, 4, &tag_a, 1));
	tag_a.blockNum = 102;
	UT_ASSERT(!cluster_gcs_pcm_x_drive_anomaly_note(table, 4, &tag_a, 1));
	tag_a.blockNum = 103;
	UT_ASSERT(!cluster_gcs_pcm_x_drive_anomaly_note(table, 4, &tag_a, 1));
	tag_a.blockNum = 50;
	for (i = 4; i < GCS_BLOCK_PCM_X_DRIVE_ANOMALY_FUSE; i++)
		UT_ASSERT(!cluster_gcs_pcm_x_drive_anomaly_note(table, 4, &tag_a, 9));
	UT_ASSERT(cluster_gcs_pcm_x_drive_anomaly_note(table, 4, &tag_a, 9));
}


/* Pin the four transient-churn recovery arms in the source:  benign slot
 * churn and two-phase cookie windows must route to per-request refresh or
 * per-ticket damping instead of an immediate runtime fuse. */
UT_TEST(test_pcm_x_transient_churn_recovers_without_runtime_fuse)
{
	char *source = read_gcs_block_source();
	const char *driver;
	const char *driver_end;
	const char *cleanup;
	const char *cleanup_end;
	const char *ensure;
	const char *ensure_end;
	const char *drive;
	const char *drive_end;
	const char *scan;

	UT_ASSERT_NOT_NULL(source);
	if (source == NULL)
		return;

	/* (a) follower WFG snapshot STALE takes the claim-site refresh lookup:
	 * the driver must contain a second handle-identity lookup site. */
	driver = strstr(source, "\ngcs_block_pcm_x_acquire_writer_impl(");
	driver_end = driver != NULL ? strstr(driver, "\n}\n") : NULL;
	UT_ASSERT_NOT_NULL(driver);
	UT_ASSERT_NOT_NULL(driver_end);
	scan = driver != NULL ? strstr(driver, "cluster_pcm_x_local_lookup_exact(&handle.identity")
						  : NULL;
	UT_ASSERT_NOT_NULL(scan);
	scan = scan != NULL ? strstr(scan + 1, "cluster_pcm_x_local_lookup_exact(&handle.identity")
						: NULL;
	UT_ASSERT_NOT_NULL(scan);
	if (scan != NULL && driver_end != NULL)
		UT_ASSERT(scan < driver_end);

	/* (b) cleanup CANCEL_LOCAL retries a STALE cancel through a refreshed
	 * handle for the exact same identity before any fail-closed verdict. */
	cleanup = strstr(source, "\ngcs_block_pcm_x_requester_cleanup_impl(");
	cleanup_end = cleanup != NULL ? strstr(cleanup, "\n}\n") : NULL;
	UT_ASSERT_NOT_NULL(cleanup);
	UT_ASSERT_NOT_NULL(cleanup_end);
	scan = cleanup != NULL
			   ? strstr(cleanup, "cluster_pcm_x_local_lookup_exact(&cleanup->handle.identity")
			   : NULL;
	UT_ASSERT_NOT_NULL(scan);
	if (scan != NULL && cleanup_end != NULL)
		UT_ASSERT(scan < cleanup_end);

	/* (c) the already-claimed ensure arm rechecks the ticket under its own
	 * domain lock before calling a missing cookie BAD_STATE. */
	ensure = strstr(source, "\ngcs_block_pcm_x_ensure_pending_x_claim(");
	ensure_end = ensure != NULL ? strstr(ensure, "\n}\n") : NULL;
	UT_ASSERT_NOT_NULL(ensure);
	UT_ASSERT_NOT_NULL(ensure_end);
	scan = ensure != NULL ? strstr(ensure, "cluster_pcm_x_master_pending_x_claim_state_exact(")
						  : NULL;
	UT_ASSERT_NOT_NULL(scan);
	scan = scan != NULL ? strstr(scan + 1, "cluster_pcm_x_master_pending_x_claim_state_exact(")
						: NULL;
	UT_ASSERT_NOT_NULL(scan);
	scan = scan != NULL ? strstr(scan + 1, "cluster_pcm_x_master_pending_x_claim_state_exact(")
						: NULL;
	UT_ASSERT_NOT_NULL(scan);
	if (scan != NULL && ensure_end != NULL)
		UT_ASSERT(scan < ensure_end);

	/* (d) the drive dispatch damps BAD_STATE through the per-ticket streak
	 * and settles it on any non-anomalous completion. */
	drive = strstr(source, "\ngcs_block_pcm_x_master_drive_tag(");
	drive_end = drive != NULL ? strstr(drive, "\n}\n") : NULL;
	UT_ASSERT_NOT_NULL(drive);
	UT_ASSERT_NOT_NULL(drive_end);
	scan = drive != NULL ? strstr(drive, "cluster_gcs_pcm_x_drive_anomaly_note(") : NULL;
	UT_ASSERT_NOT_NULL(scan);
	if (scan != NULL && drive_end != NULL)
		UT_ASSERT(scan < drive_end);
	scan = drive != NULL ? strstr(drive, "cluster_gcs_pcm_x_drive_anomaly_settle(") : NULL;
	UT_ASSERT_NOT_NULL(scan);
	if (scan != NULL && drive_end != NULL)
		UT_ASSERT(scan < drive_end);

	free(source);
}


UT_TEST(test_pcm_x_remote_reservation_preflight_binds_identity_base)
{
	ClusterPcmOwnSnapshot live;
	PcmXWaitIdentity identity;

	memset(&live, 0, sizeof(live));
	memset(&identity, 0, sizeof(identity));
	identity.tag.spcOid = 7;
	identity.tag.dbOid = 8;
	identity.tag.relNumber = 9;
	identity.tag.forkNum = MAIN_FORKNUM;
	identity.tag.blockNum = 10;
	live.tag = identity.tag;
	live.generation = 41;
	live.reservation_token = 12;
	live.pcm_state = (uint8)PCM_STATE_N;
	identity.base_own_generation = live.generation;

	UT_ASSERT_EQ(cluster_gcs_pcm_x_remote_reservation_preflight(&live, &identity), PCM_X_QUEUE_OK);
	live.generation++;
	UT_ASSERT_EQ(cluster_gcs_pcm_x_remote_reservation_preflight(&live, &identity),
				 PCM_X_QUEUE_STALE);
	live.generation = identity.base_own_generation;
	live.flags = PCM_OWN_FLAG_GRANT_PENDING;
	UT_ASSERT_EQ(cluster_gcs_pcm_x_remote_reservation_preflight(&live, &identity),
				 PCM_X_QUEUE_BUSY);
	live.flags = PCM_OWN_FLAG_REVOKING;
	UT_ASSERT_EQ(cluster_gcs_pcm_x_remote_reservation_preflight(&live, &identity),
				 PCM_X_QUEUE_BUSY);
	live.flags = PCM_OWN_FLAG_GRANT_PENDING | PCM_OWN_FLAG_REVOKING;
	UT_ASSERT_EQ(cluster_gcs_pcm_x_remote_reservation_preflight(&live, &identity),
				 PCM_X_QUEUE_CORRUPT);
	live.flags = UINT32_C(0x80);
	UT_ASSERT_EQ(cluster_gcs_pcm_x_remote_reservation_preflight(&live, &identity),
				 PCM_X_QUEUE_CORRUPT);
	live.flags = 0;
	live.pcm_state = (uint8)PCM_STATE_S;
	UT_ASSERT_EQ(cluster_gcs_pcm_x_remote_reservation_preflight(&live, &identity),
				 PCM_X_QUEUE_STALE);
	live.pcm_state = (uint8)PCM_STATE_N;
	live.tag.blockNum++;
	UT_ASSERT_EQ(cluster_gcs_pcm_x_remote_reservation_preflight(&live, &identity),
				 PCM_X_QUEUE_STALE);
	live.tag = identity.tag;
	identity.base_own_generation = UINT64_MAX;
	live.generation = UINT64_MAX;
	UT_ASSERT_EQ(cluster_gcs_pcm_x_remote_reservation_preflight(&live, &identity),
				 PCM_X_QUEUE_COUNTER_EXHAUSTED);
}


UT_TEST(test_pcm_x_requester_wait_backoff_saturates)
{
	uint32 wait_index = 0;
	uint32 i;

	for (i = 0; i < CLUSTER_PCM_X_HOLDER_RETRY_BATCH_WAITS + 4; i++)
		wait_index = cluster_gcs_pcm_x_requester_wait_index_advance(wait_index);
	UT_ASSERT_EQ(wait_index, CLUSTER_PCM_X_HOLDER_RETRY_BATCH_WAITS - 1);
	UT_ASSERT_EQ(cluster_pcm_x_holder_retry_delay_ms(wait_index),
				 cluster_pcm_x_holder_retry_delay_ms(CLUSTER_PCM_X_HOLDER_RETRY_BATCH_WAITS + 20));
}


UT_TEST(test_pcm_x_requester_cleanup_never_guesses_after_irreversible_boundary)
{
	UT_ASSERT_EQ(cluster_gcs_pcm_x_requester_cleanup_action(false, false, false, false),
				 GCS_BLOCK_PCM_X_CLEANUP_NONE);
	UT_ASSERT_EQ(cluster_gcs_pcm_x_requester_cleanup_action(true, false, false, false),
				 GCS_BLOCK_PCM_X_CLEANUP_CANCEL_LOCAL);
	UT_ASSERT_EQ(cluster_gcs_pcm_x_requester_cleanup_action(true, true, false, false),
				 GCS_BLOCK_PCM_X_CLEANUP_CANCEL_LOCAL);
	UT_ASSERT_EQ(cluster_gcs_pcm_x_requester_cleanup_action(false, true, false, false),
				 GCS_BLOCK_PCM_X_CLEANUP_PRESERVE_FAIL_CLOSED);
	UT_ASSERT_EQ(cluster_gcs_pcm_x_requester_cleanup_action(true, true, true, false),
				 GCS_BLOCK_PCM_X_CLEANUP_PRESERVE_FAIL_CLOSED);
	UT_ASSERT_EQ(cluster_gcs_pcm_x_requester_cleanup_action(true, true, false, true),
				 GCS_BLOCK_PCM_X_CLEANUP_PRESERVE_FAIL_CLOSED);
}


UT_TEST(test_pcm_x_retire_commit_wakes_exact_waiters_before_ack_or_resolve)
{
	char *source = read_gcs_block_source();
	const char *common;
	const char *common_end;
	const char *allocate;
	const char *allocate_after;
	const char *collect;
	const char *exact_wake;
	const char *wake;
	const char *wake_end;
	const char *read;
	const char *match;
	const char *set_latch;
	const char *writer_release;
	const char *writer_release_end;
	const char *writer_collect;
	const char *writer_wake;
	const char *writer_allocate;
	const char *writer_cleanup;
	const char *writer_cleanup_end;
	const char *writer_cleanup_try;
	const char *writer_cleanup_exact;
	const char *writer_cleanup_catch;
	const char *writer_cleanup_flush;
	const char *writer_cleanup_fail_closed;
	const char *remote;
	const char *remote_end;
	const char *remote_apply;
	const char *remote_ack;
	const char *terminal;
	const char *terminal_end;
	const char *self_apply;
	const char *self_resolve;

	UT_ASSERT_NOT_NULL(source);
	if (source == NULL)
		return;
	wake = strstr(source, "\ngcs_block_pcm_x_wake_requester_exact(");
	wake_end = wake != NULL ? strstr(wake, "\n}\n") : NULL;
	UT_ASSERT_NOT_NULL(wake);
	UT_ASSERT_NOT_NULL(wake_end);
	if (wake != NULL && wake_end != NULL) {
		read = strstr(wake, "cluster_lmd_wait_state_read_exact(");
		match = read != NULL ? strstr(read, "cluster_gcs_pcm_x_wait_identity_matches(") : NULL;
		set_latch = match != NULL ? strstr(match, "SetLatch(") : NULL;
		UT_ASSERT_NOT_NULL(read);
		UT_ASSERT_NOT_NULL(match);
		UT_ASSERT_NOT_NULL(set_latch);
		if (read != NULL && match != NULL && set_latch != NULL)
			UT_ASSERT(read < match && match < set_latch && set_latch < wake_end);
	}
	writer_release = strstr(source, "\ncluster_gcs_pcm_x_writer_claim_release_and_wake_exact(");
	writer_release_end = writer_release != NULL ? strstr(writer_release, "\n}\n") : NULL;
	UT_ASSERT_NOT_NULL(writer_release);
	UT_ASSERT_NOT_NULL(writer_release_end);
	if (writer_release != NULL && writer_release_end != NULL) {
		writer_collect
			= strstr(writer_release, "cluster_pcm_x_local_writer_claim_release_collect_exact(");
		writer_wake = writer_collect != NULL
						  ? strstr(writer_collect, "gcs_block_pcm_x_wake_requester_exact(")
						  : NULL;
		writer_allocate = strstr(writer_release, "palloc");
		UT_ASSERT_NOT_NULL(writer_collect);
		UT_ASSERT_NOT_NULL(writer_wake);
		if (writer_collect != NULL && writer_wake != NULL)
			UT_ASSERT(writer_collect < writer_wake && writer_wake < writer_release_end);
		UT_ASSERT(writer_allocate == NULL || writer_allocate > writer_release_end);
	}
	writer_cleanup = strstr(source, "\ncluster_gcs_pcm_x_writer_claim_cleanup_and_wake_noexcept(");
	writer_cleanup_end = writer_cleanup != NULL ? strstr(writer_cleanup, "\n}\n") : NULL;
	UT_ASSERT_NOT_NULL(writer_cleanup);
	UT_ASSERT_NOT_NULL(writer_cleanup_end);
	if (writer_cleanup != NULL && writer_cleanup_end != NULL) {
		writer_cleanup_try = strstr(writer_cleanup, "PG_TRY();");
		writer_cleanup_exact
			= writer_cleanup_try != NULL
				  ? strstr(writer_cleanup_try,
						   "cluster_gcs_pcm_x_writer_claim_release_and_wake_exact(claim)")
				  : NULL;
		writer_cleanup_catch
			= writer_cleanup_exact != NULL ? strstr(writer_cleanup_exact, "PG_CATCH();") : NULL;
		writer_cleanup_flush = writer_cleanup_catch != NULL
								   ? strstr(writer_cleanup_catch, "FlushErrorState();")
								   : NULL;
		writer_cleanup_fail_closed
			= writer_cleanup_flush != NULL
				  ? strstr(writer_cleanup_flush, "cluster_pcm_x_runtime_fail_closed();")
				  : NULL;
		UT_ASSERT_NOT_NULL(writer_cleanup_try);
		UT_ASSERT_NOT_NULL(writer_cleanup_exact);
		UT_ASSERT_NOT_NULL(writer_cleanup_catch);
		UT_ASSERT_NOT_NULL(writer_cleanup_flush);
		UT_ASSERT_NOT_NULL(writer_cleanup_fail_closed);
		if (writer_cleanup_try != NULL && writer_cleanup_exact != NULL
			&& writer_cleanup_catch != NULL && writer_cleanup_flush != NULL
			&& writer_cleanup_fail_closed != NULL)
			UT_ASSERT(writer_cleanup_try < writer_cleanup_exact
					  && writer_cleanup_exact < writer_cleanup_catch
					  && writer_cleanup_catch < writer_cleanup_flush
					  && writer_cleanup_flush < writer_cleanup_fail_closed
					  && writer_cleanup_fail_closed < writer_cleanup_end);
	}
	common = strstr(source, "\ngcs_block_pcm_x_local_retire_apply_and_wake(");
	common_end = common != NULL ? strstr(common, "\n}\n") : NULL;
	UT_ASSERT_NOT_NULL(common);
	UT_ASSERT_NOT_NULL(common_end);
	if (common != NULL && common_end != NULL) {
		allocate = strstr(common, "palloc0(");
		collect = allocate != NULL
					  ? strstr(allocate, "cluster_pcm_x_local_retire_up_to_collect_exact(")
					  : NULL;
		exact_wake
			= collect != NULL ? strstr(collect, "gcs_block_pcm_x_wake_requester_exact(") : NULL;
		allocate_after = allocate != NULL ? strstr(allocate + 1, "palloc0(") : NULL;
		UT_ASSERT_NOT_NULL(allocate);
		UT_ASSERT_NOT_NULL(collect);
		UT_ASSERT_NOT_NULL(exact_wake);
		if (allocate != NULL && collect != NULL && exact_wake != NULL)
			UT_ASSERT(allocate < collect && collect < exact_wake && exact_wake < common_end);
		UT_ASSERT(allocate_after == NULL || allocate_after > common_end);
	}
	remote = strstr(source, "\ncluster_gcs_handle_pcm_x_retire_up_to_envelope(");
	remote_end = remote != NULL ? strstr(remote, "\n}\n") : NULL;
	UT_ASSERT_NOT_NULL(remote);
	UT_ASSERT_NOT_NULL(remote_end);
	if (remote != NULL && remote_end != NULL) {
		remote_apply = strstr(remote, "gcs_block_pcm_x_local_retire_apply_and_wake(");
		remote_ack = remote_apply != NULL ? strstr(remote_apply, "gcs_block_pcm_x_send_retire_ack(")
										  : NULL;
		UT_ASSERT_NOT_NULL(remote_apply);
		UT_ASSERT_NOT_NULL(remote_ack);
		if (remote_apply != NULL && remote_ack != NULL)
			UT_ASSERT(remote_apply < remote_ack && remote_ack < remote_end);
	}
	terminal = strstr(source, "\ncluster_gcs_pcm_x_terminal_kick(");
	terminal_end = terminal != NULL ? strstr(terminal, "\n}\n\n\nstatic") : NULL;
	UT_ASSERT_NOT_NULL(terminal);
	UT_ASSERT_NOT_NULL(terminal_end);
	if (terminal != NULL && terminal_end != NULL) {
		self_apply = strstr(terminal, "gcs_block_pcm_x_local_retire_apply_and_wake(");
		self_resolve = self_apply != NULL
						   ? strstr(self_apply, "cluster_pcm_x_master_retire_ack_resolve_exact(")
						   : NULL;
		UT_ASSERT_NOT_NULL(self_apply);
		UT_ASSERT_NOT_NULL(self_resolve);
		if (self_apply != NULL && self_resolve != NULL)
			UT_ASSERT(self_apply < self_resolve && self_resolve < terminal_end);
	}
	UT_ASSERT_EQ(count_occurrences(source, "gcs_block_pcm_x_local_retire_apply_and_wake("), 3);
	UT_ASSERT_EQ(count_occurrences(source, "cluster_pcm_x_local_retire_up_to_collect_exact("), 1);
	free(source);
}


UT_TEST(test_pcm_x_tagless_retire_uses_explicit_data_plane_handoff)
{
	char *source = read_gcs_block_source();
	const char *handler;
	const char *handler_end;
	const char *kick;
	const char *kick_end;
	const char *ack_send;
	const char *request_stage;

	UT_ASSERT_NOT_NULL(source);
	if (source == NULL)
		return;
	handler = strstr(source, "\ncluster_gcs_handle_pcm_x_retire_up_to_envelope(");
	handler_end = handler != NULL ? strstr(handler + 1, "\n}\n\n\n") : NULL;
	kick = strstr(source, "\ncluster_gcs_pcm_x_terminal_kick(");
	kick_end = kick != NULL ? strstr(kick + 1, "\n}\n\n\nstatic") : NULL;
	UT_ASSERT_NOT_NULL(handler);
	UT_ASSERT_NOT_NULL(handler_end);
	UT_ASSERT_NOT_NULL(kick);
	UT_ASSERT_NOT_NULL(kick_end);
	ack_send = handler != NULL ? strstr(handler, "gcs_block_pcm_x_send_retire_ack(") : NULL;
	request_stage = kick != NULL ? strstr(kick, "gcs_block_pcm_x_stage_retire_up_to(") : NULL;
	UT_ASSERT_NOT_NULL(ack_send);
	UT_ASSERT_NOT_NULL(request_stage);
	if (handler_end != NULL && ack_send != NULL)
		UT_ASSERT(ack_send < handler_end);
	if (kick_end != NULL && request_stage != NULL)
		UT_ASSERT(request_stage < kick_end);
	free(source);
}


UT_TEST(test_pcm_x_role_refresh_accepts_only_same_member_promotion)
{
	PcmXLocalHandle follower;
	PcmXLocalHandle promoted;

	memset(&follower, 0, sizeof(follower));
	follower.identity.node_id = 1;
	follower.identity.procno = 7;
	follower.identity.xid = 11;
	follower.identity.cluster_epoch = 13;
	follower.identity.request_id = 17;
	follower.identity.wait_seq = 19;
	follower.identity.base_own_generation = 23;
	follower.tag_slot.slot_index = 29;
	follower.tag_slot.slot_generation = 31;
	follower.membership_slot.slot_index = 37;
	follower.membership_slot.slot_generation = 41;
	follower.local_sequence = 43;
	follower.local_round = 47;
	follower.role = PCM_X_LOCAL_ROLE_FOLLOWER;
	promoted = follower;
	promoted.role = PCM_X_LOCAL_ROLE_NODE_LEADER;

	UT_ASSERT(cluster_gcs_pcm_x_role_refresh_exact(&follower, &promoted));
	promoted.role = PCM_X_LOCAL_ROLE_FOLLOWER;
	UT_ASSERT(!cluster_gcs_pcm_x_role_refresh_exact(&follower, &promoted));
	promoted = follower;
	promoted.role = PCM_X_LOCAL_ROLE_NODE_LEADER;
	promoted.membership_slot.slot_generation++;
	UT_ASSERT(!cluster_gcs_pcm_x_role_refresh_exact(&follower, &promoted));
	promoted = follower;
	promoted.role = PCM_X_LOCAL_ROLE_NODE_LEADER;
	promoted.local_sequence++;
	UT_ASSERT(!cluster_gcs_pcm_x_role_refresh_exact(&follower, &promoted));
	promoted = follower;
	promoted.role = PCM_X_LOCAL_ROLE_NODE_LEADER;
	promoted.local_round++;
	UT_ASSERT(!cluster_gcs_pcm_x_role_refresh_exact(&follower, &promoted));
	promoted = follower;
	promoted.role = PCM_X_LOCAL_ROLE_NODE_LEADER;
	promoted.identity.base_own_generation++;
	UT_ASSERT(!cluster_gcs_pcm_x_role_refresh_exact(&follower, &promoted));
}


int
main(void)
{
	UT_PLAN(76);
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
	UT_RUN(test_pcm_x_session_auth_sample_classifies_epoch_zero_and_torn_reads);
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
	UT_RUN(test_forward_payload_undo_verdict_kinds_no_collision);
	UT_RUN(test_undo_authority_fetch_tag_owner_roundtrip);
	UT_RUN(test_undo_verdict_version_authority_distinct);
	UT_RUN(test_pcm_x_enqueue_ingress_binds_transport_epoch_and_master);
	UT_RUN(test_pcm_x_admit_confirm_ingress_binds_requester_and_master);
	UT_RUN(test_pcm_x_admit_confirm_ack_binds_exact_master_source);
	UT_RUN(test_pcm_x_cancel_requests_bind_exact_source_epoch_master_and_phase);
	UT_RUN(test_pcm_x_cancel_acks_bind_exact_master_and_canonical_payload);
	UT_RUN(test_pcm_x_wait_identity_maps_to_real_wfg_vertex);
	UT_RUN(test_pcm_x_initial_epoch_zero_is_exact_across_wire_classes);
	UT_RUN(test_pcm_x_blocker_header_ingress_binds_master_not_requester_source);
	UT_RUN(test_pcm_x_blocker_edge_ingress_binds_blocker_to_holder_source);
	UT_RUN(test_pcm_x_blocker_ack_carries_full_generation_and_binds_master_source);
	UT_RUN(test_pcm_x_formation_identical_complete_samples_may_revalidate);
	UT_RUN(test_pcm_x_formation_transient_or_inconsistent_sample_is_tick_noop);
	UT_RUN(test_pcm_x_confirm_publish_then_stale_requires_exact_graph_close);
	UT_RUN(test_pcm_x_drain_poll_binds_exact_master_and_generation);
	UT_RUN(test_pcm_x_drain_ack_binds_participant_and_canonical_payload);
	UT_RUN(test_pcm_x_retire_request_binds_master_session_and_target);
	UT_RUN(test_pcm_x_retire_ack_binds_responder_and_master_authority);
	UT_RUN(test_pcm_x_revoke_ingress_binds_master_and_exact_transfer_key);
	UT_RUN(test_pcm_x_image_ready_ingress_binds_holder_image_to_master);
	UT_RUN(test_pcm_x_prepare_grant_ingress_binds_master_to_requester);
	UT_RUN(test_pcm_x_install_ready_ingress_is_canonical_requester_ack);
	UT_RUN(test_pcm_x_commit_x_ingress_is_canonical_master_phase);
	UT_RUN(test_pcm_x_final_ack_ingress_binds_exact_committed_generation);
	UT_RUN(test_pcm_x_final_commit_ack_ingress_is_canonical_master_phase);
	UT_RUN(test_pcm_x_final_confirm_ingress_is_canonical_requester_phase);
	UT_RUN(test_pcm_x_master_drive_selects_exact_authority_and_next_holder);
	UT_RUN(test_pcm_x_master_drive_wiring_binds_grd_barrier_to_exact_ticket);
	UT_RUN(test_pcm_x_cancel_cleanup_classifies_exact_wfg_and_post_clear_failure);
	UT_RUN(test_pcm_x_terminal_retry_reclaims_cancel_cleanup_after_owner_death);
	UT_RUN(test_pcm_x_invalidate_ack_matches_only_exact_unacked_holder);
	UT_RUN(test_pcm_x_final_ack_builds_exact_grd_handoff_token);
	UT_RUN(test_pcm_x_holder_image_evidence_never_uses_generation_as_presence);
	UT_RUN(test_pcm_x_pending_x_marker_is_only_a_pre_handoff_gate);
	UT_RUN(test_pcm_x_ready_publication_follows_exact_retained_commit);
	UT_RUN(test_pcm_x_ready_materializes_exact_n_s_or_x_source_without_wire_change);
	UT_RUN(test_pcm_x_self_and_remote_drain_share_full_image_release_wrapper);
	UT_RUN(test_pcm_x_ready_admission_marks_before_send_and_rolls_back_refusal);
	UT_RUN(test_pcm_x_lms_owner_death_and_restart_audit_fail_closed);
	UT_RUN(test_pcm_x_image_fetch_intercepts_canonical_id_before_generic_dedup);
	UT_RUN(test_pcm_x_requester_fetch_revalidates_queue_and_reservation_before_install);
	UT_RUN(test_pcm_x_self_source_handoff_is_no_copy_and_drain_preserves_x);
	UT_RUN(test_pcm_x_retire_wake_identity_is_wait_generation_exact);
	UT_RUN(test_pcm_x_requester_driver_owns_fifo_and_transfer_lifecycles);
	UT_RUN(test_pcm_x_requester_retry_policy_is_operation_exact);
	UT_RUN(test_pcm_x_drive_anomaly_streak_gates_fail_closed);
	UT_RUN(test_pcm_x_transient_churn_recovers_without_runtime_fuse);
	UT_RUN(test_pcm_x_remote_reservation_preflight_binds_identity_base);
	UT_RUN(test_pcm_x_requester_wait_backoff_saturates);
	UT_RUN(test_pcm_x_requester_cleanup_never_guesses_after_irreversible_boundary);
	UT_RUN(test_pcm_x_retire_commit_wakes_exact_waiters_before_ack_or_resolve);
	UT_RUN(test_pcm_x_tagless_retire_uses_explicit_data_plane_handoff);
	UT_RUN(test_pcm_x_role_refresh_accepts_only_same_member_promotion);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
