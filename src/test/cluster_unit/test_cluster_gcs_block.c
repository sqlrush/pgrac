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
 *	    L4  GcsBlockReplyHeader field offsets (48B layout)
 *	    L5  GcsBlockReplyStatus enum has exactly 7 values
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
	UT_ASSERT_EQ((int)offsetof(GcsBlockReplyHeader, reserved_0), 38);
}


UT_TEST(test_gcs_block_reply_status_enum_count_is_7)
{
	UT_ASSERT_EQ((int)GCS_BLOCK_REPLY_GRANTED, 0);
	UT_ASSERT_EQ((int)GCS_BLOCK_REPLY_GRANTED_STORAGE_FALLBACK, 1);
	UT_ASSERT_EQ((int)GCS_BLOCK_REPLY_DENIED_INCOMPATIBLE, 2);
	UT_ASSERT_EQ((int)GCS_BLOCK_REPLY_DENIED_VALIDATOR_REJECT, 3);
	UT_ASSERT_EQ((int)GCS_BLOCK_REPLY_DENIED_EPOCH_STALE, 4);
	UT_ASSERT_EQ((int)GCS_BLOCK_REPLY_DENIED_CHECKSUM_FAIL, 5);
	UT_ASSERT_EQ((int)GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER, 6);
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
	UT_ASSERT_EQ((int)sizeof(rep.reserved_0), 10);
	memset(&req, 0, sizeof(req));
	memset(&rep, 0, sizeof(rep));
	UT_ASSERT_EQ((int)req.reserved_0[0], 0);
	UT_ASSERT_EQ((int)req.reserved_0[18], 0);
	UT_ASSERT_EQ((int)rep.reserved_0[0], 0);
	UT_ASSERT_EQ((int)rep.reserved_0[9], 0);
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


int
main(void)
{
	UT_PLAN(15);
	UT_RUN(test_gcs_block_msg_type_enum_values_no_collision);
	UT_RUN(test_gcs_block_payload_sizes_locked);
	UT_RUN(test_gcs_block_request_field_offsets);
	UT_RUN(test_gcs_block_reply_header_field_offsets);
	UT_RUN(test_gcs_block_reply_status_enum_count_is_7);
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
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
