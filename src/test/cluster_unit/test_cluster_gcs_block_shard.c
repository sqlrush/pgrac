/*-------------------------------------------------------------------------
 *
 * test_cluster_gcs_block_shard.c
 *	  Unit tests for the DATA-plane payload -> worker routing layer
 *	  (spec-7.3 D4/D9: cluster_gcs_block_payload_shard).
 *
 *	  cluster_gcs_block_payload_shard() is the single staging-path
 *	  decision that picks the outbound ring (= DATA worker) for a
 *	  block-family frame.  These tests pin the D9 "ring-group routing"
 *	  truth table on the REAL function (not a reimplementation):
 *
 *	    - the three tag-carrying staging types (REQUEST / FORWARD /
 *	      INVALIDATE) route to exactly cluster_lms_shard_for_tag(tag, N)
 *	      -- same-tag family affinity is what keeps the INVALIDATE-ACK
 *	      -> same-tag re-REQUEST wire FIFO intact after the N-way split
 *	      (D0-① WATCH: the ACK direct-sends from worker[shard(tag)]'s
 *	      dispatch process, the re-REQUEST stages back to shard(tag);
 *	      equal shards == one worker stream == order preserved),
 *	    - the DATA-plane registry partition is pinned: REPLY is the explicit
 *	      direct-send whitelist, while INVALIDATE-ACK may be staged for a
 *	      local master so the DATA worker performs the required self-dispatch;
 *	      any
 *	      OTHER msg_type is refused (-1) -- the 8.A fail-closed contract
 *	      that an undeclared DATA frame is never defaulted to a worker,
 *	    - the payload-length ABI pin: a size mismatch can never read a
 *	      tag from a stale offset (returns -1 instead), and
 *	    - N == 1 degenerates to worker 0 (spec-7.2 topology identity).
 *
 *	  The live 2-node multi-tag distribution (per-worker counters move on
 *	  distinct workers) is TAP t/367 L7; this file pins the pure routing
 *	  math those counters depend on.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_gcs_block_shard.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-7.3-lms-worker-pool.md (D4/D9)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <stddef.h>

#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_ic_envelope.h"
#include "cluster/cluster_lms_shard.h"
#include "cluster/cluster_multixact_current_wire.h"
#include "cluster/cluster_terminal_ref_census.h"
#include "cluster/cluster_resource_x_node_wire.h"
#include "storage/buf_internals.h"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *conditionName, const char *fileName, int lineNumber)
{
	printf("# Assert failed: %s at %s:%d\n", conditionName, fileName, lineNumber);
	abort();
}

/* Build a BufferTag from its five flat fields (spec-7.3 shard-key domain). */
static BufferTag
make_tag(Oid spc, Oid db, RelFileNumber rel, ForkNumber fork, BlockNumber blk)
{
	BufferTag tag;

	tag.spcOid = spc;
	tag.dbOid = db;
	tag.relNumber = rel;
	tag.forkNum = fork;
	tag.blockNum = blk;
	return tag;
}

/*
 * Helpers: minimal well-formed staging payloads.  Only the tag feeds the
 * shard; every other field is zero (the router must not read them).
 */
static GcsBlockRequestPayload
make_request(BufferTag tag)
{
	GcsBlockRequestPayload p;

	memset(&p, 0, sizeof(p));
	p.tag = tag;
	return p;
}

static GcsBlockForwardPayload
make_forward(BufferTag tag)
{
	GcsBlockForwardPayload p;

	memset(&p, 0, sizeof(p));
	p.tag = tag;
	return p;
}

static GcsBlockInvalidatePayload
make_invalidate(BufferTag tag)
{
	GcsBlockInvalidatePayload p;

	memset(&p, 0, sizeof(p));
	p.tag = tag;
	return p;
}

/* Review F4: a DONE with a NONZERO epoch -- the router must key on the
 * tag alone;  an epoch-0-only fixture would green-light a router that
 * accidentally reads the epoch field. */
static GcsBlockDonePayload
make_done(BufferTag tag)
{
	GcsBlockDonePayload p;

	memset(&p, 0, sizeof(p));
	p.epoch = 7;
	p.tag = tag;
	return p;
}

/* ======================================================================
 * U1 -- each staging type routes to exactly shard_for_tag(tag, N): the
 *		 payload router adds no input of its own (double-end agreement,
 *		 R1) and stays in range for every N.
 * ====================================================================== */
UT_TEST(test_route_matches_shard_for_tag)
{
	int n;
	int i;

	for (i = 0; i < 128; i++) {
		BufferTag tag = make_tag(1663, 5, 16384 + (i % 13), MAIN_FORKNUM, (BlockNumber)(i * 31));
		GcsBlockRequestPayload req = make_request(tag);
		GcsBlockForwardPayload fwd = make_forward(tag);
		GcsBlockInvalidatePayload inv = make_invalidate(tag);
		GcsBlockDonePayload done = make_done(tag);

		for (n = 1; n <= CLUSTER_LMS_MAX_WORKERS; n++) {
			int expect = cluster_lms_shard_for_tag(&tag, n);

			UT_ASSERT_EQ(cluster_gcs_block_payload_shard(PGRAC_IC_MSG_GCS_BLOCK_REQUEST, &req,
														 sizeof(req), n),
						 expect);
			UT_ASSERT_EQ(cluster_gcs_block_payload_shard(PGRAC_IC_MSG_GCS_BLOCK_FORWARD, &fwd,
														 sizeof(fwd), n),
						 expect);
			UT_ASSERT_EQ(cluster_gcs_block_payload_shard(PGRAC_IC_MSG_GCS_BLOCK_INVALIDATE, &inv,
														 sizeof(inv), n),
						 expect);
			/* review F4: DONE rides the same tag shard as the REQUEST it
			 * retires (it must land on the dedup entry's worker). */
			UT_ASSERT_EQ(cluster_gcs_block_payload_shard(PGRAC_IC_MSG_GCS_BLOCK_DONE, &done,
														 sizeof(done), n),
						 expect);
			UT_ASSERT(expect >= 0);
			UT_ASSERT(expect < n);
		}
	}
}

/* ======================================================================
 * U2 -- same-tag family affinity (ACK -> same-tag re-REQUEST FIFO): an
 *		 INVALIDATE for tag T and a later REQUEST for the same T must pick
 *		 the SAME worker, so the direct-sent ACK (riding worker[shard]'s
 *		 channel from its dispatch process) and the staged re-REQUEST
 *		 share one wire stream and cannot reorder (D0-① WATCH).
 * ====================================================================== */
UT_TEST(test_route_ack_request_interleave_affinity)
{
	int i;

	for (i = 0; i < 64; i++) {
		BufferTag tag = make_tag(1663 + (i % 3), 5, 20000 + i, (ForkNumber)(i % (MAX_FORKNUM + 1)),
								 (BlockNumber)i);
		GcsBlockInvalidatePayload inv = make_invalidate(tag);
		GcsBlockRequestPayload req = make_request(tag);
		int s_inv = cluster_gcs_block_payload_shard(PGRAC_IC_MSG_GCS_BLOCK_INVALIDATE, &inv,
													sizeof(inv), CLUSTER_LMS_MAX_WORKERS);
		int s_req = cluster_gcs_block_payload_shard(PGRAC_IC_MSG_GCS_BLOCK_REQUEST, &req,
													sizeof(req), CLUSTER_LMS_MAX_WORKERS);

		UT_ASSERT(s_inv >= 0);
		UT_ASSERT_EQ(s_req, s_inv);
	}
}

/* ======================================================================
 * U3 -- legacy DATA-plane registry partition pin ("every DATA msg_type has a
 *		 declared shard key or is explicitly direct-send"): REQUEST / FORWARD /
 *		 INVALIDATE / DONE are ring-routable; INVALIDATE-ACK must also be
 *		 routable because a local master cannot use the generic IC self-send
 *		 no-op.  REPLY remains the direct-send whitelist.  A NEW DATA type that
 *		 tries to stage without a key is refused at runtime by this same
 *		 -1 (fail-closed, never defaulted -- 8.A);  this test pins the
 *		 declared partition so a silent re-plumb fails loudly.
 * ====================================================================== */
UT_TEST(test_route_registry_partition)
{
	BufferTag tag = make_tag(1663, 5, 16384, MAIN_FORKNUM, 7);
	GcsBlockRequestPayload req = make_request(tag);
	GcsBlockForwardPayload fwd = make_forward(tag);
	GcsBlockInvalidatePayload inv = make_invalidate(tag);
	GcsBlockInvalidateAckPayload ack = { 0 };
	GcsBlockDonePayload done = make_done(tag);
	uint8 raw[64];

	memset(raw, 0, sizeof(raw));
	ack.tag = tag;

	/* Routable staging types. */
	UT_ASSERT(cluster_gcs_block_payload_shard(PGRAC_IC_MSG_GCS_BLOCK_REQUEST, &req, sizeof(req),
											  CLUSTER_LMS_MAX_WORKERS)
			  >= 0);
	UT_ASSERT(cluster_gcs_block_payload_shard(PGRAC_IC_MSG_GCS_BLOCK_FORWARD, &fwd, sizeof(fwd),
											  CLUSTER_LMS_MAX_WORKERS)
			  >= 0);
	UT_ASSERT(cluster_gcs_block_payload_shard(PGRAC_IC_MSG_GCS_BLOCK_INVALIDATE, &inv, sizeof(inv),
											  CLUSTER_LMS_MAX_WORKERS)
			  >= 0);
	UT_ASSERT(cluster_gcs_block_payload_shard(PGRAC_IC_MSG_GCS_BLOCK_INVALIDATE_ACK, &ack,
											  sizeof(ack), CLUSTER_LMS_MAX_WORKERS)
			  >= 0);
	UT_ASSERT(cluster_gcs_block_payload_shard(PGRAC_IC_MSG_GCS_BLOCK_DONE, &done, sizeof(done),
											  CLUSTER_LMS_MAX_WORKERS)
			  >= 0);

	/* Direct-send whitelist: no shard key by design (spec-7.3 §3.6). */
	UT_ASSERT_EQ(cluster_gcs_block_payload_shard(PGRAC_IC_MSG_GCS_BLOCK_REPLY, raw, sizeof(raw),
												 CLUSTER_LMS_MAX_WORKERS),
				 -1);
}

/* ======================================================================
 * U4 -- fail-closed refuse for anything outside the DATA block family:
 *		 a CONTROL-plane block message (REDECLARE) and an arbitrary
 *		 unknown msg_type must never be defaulted onto a ring, and a
 *		 NULL payload is refused regardless of type.
 * ====================================================================== */
UT_TEST(test_route_unroutable_fail_closed)
{
	BufferTag tag = make_tag(1663, 5, 16384, MAIN_FORKNUM, 7);
	GcsBlockRequestPayload req = make_request(tag);
	GcsBlockDonePayload done = make_done(tag);
	uint8 raw[64];

	memset(raw, 0, sizeof(raw));

	UT_ASSERT_EQ(cluster_gcs_block_payload_shard(PGRAC_IC_MSG_GCS_BLOCK_REDECLARE, raw, sizeof(raw),
												 CLUSTER_LMS_MAX_WORKERS),
				 -1);
	UT_ASSERT_EQ(cluster_gcs_block_payload_shard(0xFF, raw, sizeof(raw), CLUSTER_LMS_MAX_WORKERS),
				 -1);
	UT_ASSERT_EQ(cluster_gcs_block_payload_shard(0, raw, sizeof(raw), CLUSTER_LMS_MAX_WORKERS), -1);
	UT_ASSERT_EQ(cluster_gcs_block_payload_shard(PGRAC_IC_MSG_GCS_BLOCK_DONE, &done,
												 (uint16)(sizeof(done) - 1),
												 CLUSTER_LMS_MAX_WORKERS),
				 -1);
	UT_ASSERT_EQ(cluster_gcs_block_payload_shard(PGRAC_IC_MSG_GCS_BLOCK_REQUEST, NULL, sizeof(req),
												 CLUSTER_LMS_MAX_WORKERS),
				 -1);
}

/* ======================================================================
 * U5 -- payload-length ABI pin: a length that does not exactly match the
 *		 declared wire struct is refused (-1) for every routable type --
 *		 a mismatched frame can never have its "tag" read from a stale
 *		 offset and silently misroute (8.A).
 * ====================================================================== */
UT_TEST(test_route_length_mismatch_refused)
{
	BufferTag tag = make_tag(1663, 5, 16384, MAIN_FORKNUM, 7);
	GcsBlockRequestPayload req = make_request(tag);
	GcsBlockForwardPayload fwd = make_forward(tag);
	GcsBlockInvalidatePayload inv = make_invalidate(tag);
	GcsBlockDonePayload done = make_done(tag);
	struct {
		uint8 msg_type;
		const void *payload;
		uint16 good_len;
	} cases[4];
	int i;

	cases[0].msg_type = PGRAC_IC_MSG_GCS_BLOCK_REQUEST;
	cases[0].payload = &req;
	cases[0].good_len = sizeof(req);
	cases[1].msg_type = PGRAC_IC_MSG_GCS_BLOCK_FORWARD;
	cases[1].payload = &fwd;
	cases[1].good_len = sizeof(fwd);
	cases[2].msg_type = PGRAC_IC_MSG_GCS_BLOCK_INVALIDATE;
	cases[2].payload = &inv;
	cases[2].good_len = sizeof(inv);
	cases[3].msg_type = PGRAC_IC_MSG_GCS_BLOCK_DONE;
	cases[3].payload = &done;
	cases[3].good_len = sizeof(done);

	for (i = 0; i < 4; i++) {
		UT_ASSERT_EQ(cluster_gcs_block_payload_shard(cases[i].msg_type, cases[i].payload,
													 cases[i].good_len - 1,
													 CLUSTER_LMS_MAX_WORKERS),
					 -1);
		UT_ASSERT_EQ(cluster_gcs_block_payload_shard(cases[i].msg_type, cases[i].payload,
													 cases[i].good_len + 1,
													 CLUSTER_LMS_MAX_WORKERS),
					 -1);
		UT_ASSERT_EQ(cluster_gcs_block_payload_shard(cases[i].msg_type, cases[i].payload, 0,
													 CLUSTER_LMS_MAX_WORKERS),
					 -1);
		/* And the exact length still routes. */
		UT_ASSERT(cluster_gcs_block_payload_shard(cases[i].msg_type, cases[i].payload,
												  cases[i].good_len, CLUSTER_LMS_MAX_WORKERS)
				  >= 0);
	}
}

/* ======================================================================
 * U6 -- N == 1 degenerate: every routable frame lands on worker 0 (the
 *		 spec-7.2 single-LMS topology identity;  rings[0] byte path).
 * ====================================================================== */
UT_TEST(test_route_n1_degenerate_zero)
{
	int i;

	for (i = 0; i < 64; i++) {
		BufferTag tag = make_tag(1663, 5 + (i % 5), 16384 + i, MAIN_FORKNUM, (BlockNumber)(i * 7));
		GcsBlockRequestPayload req = make_request(tag);
		GcsBlockForwardPayload fwd = make_forward(tag);
		GcsBlockInvalidatePayload inv = make_invalidate(tag);

		UT_ASSERT_EQ(
			cluster_gcs_block_payload_shard(PGRAC_IC_MSG_GCS_BLOCK_REQUEST, &req, sizeof(req), 1),
			0);
		UT_ASSERT_EQ(
			cluster_gcs_block_payload_shard(PGRAC_IC_MSG_GCS_BLOCK_FORWARD, &fwd, sizeof(fwd), 1),
			0);
		UT_ASSERT_EQ(cluster_gcs_block_payload_shard(PGRAC_IC_MSG_GCS_BLOCK_INVALIDATE, &inv,
													 sizeof(inv), 1),
					 0);
	}
}

/* ======================================================================
 * U7 -- only the tag feeds the route: flipping every non-tag field of a
 *		 staging payload (request_id / epoch / nodes / backend / flags)
 *		 never moves the shard (the router reads &p->tag and nothing
 *		 else -- same-tag streams cannot fork on metadata).
 * ====================================================================== */
UT_TEST(test_route_ignores_non_tag_fields)
{
	BufferTag tag = make_tag(1663, 5, 16384, MAIN_FORKNUM, 99);
	GcsBlockRequestPayload req = make_request(tag);
	int ref = cluster_gcs_block_payload_shard(PGRAC_IC_MSG_GCS_BLOCK_REQUEST, &req, sizeof(req),
											  CLUSTER_LMS_MAX_WORKERS);
	int i;

	UT_ASSERT(ref >= 0);
	for (i = 1; i <= 32; i++) {
		GcsBlockRequestPayload v = make_request(tag);

		v.request_id = (uint64)i * 0x9E3779B97F4A7C15ull;
		v.epoch = (uint64)i;
		v.sender_node = i % 4;
		v.requester_backend_id = i;
		v.transition_id = (uint8)(i % 9);
		memset(v.reserved_0, (int)(i & 0xFF), sizeof(v.reserved_0));

		UT_ASSERT_EQ(cluster_gcs_block_payload_shard(PGRAC_IC_MSG_GCS_BLOCK_REQUEST, &v, sizeof(v),
													 CLUSTER_LMS_MAX_WORKERS),
					 ref);
	}
}

/* R4 extended route frames retain the legacy offset-16 BufferTag key. */
enum {
	GCS_BLOCK_ROUTE_TAG_OFFSET = 16,
	GCS_BLOCK_LEGACY_ROUTE_LEN = 64,
	GCS_BLOCK_R4_REQUEST_ROUTE_LEN = 80,
	GCS_BLOCK_R4_FORWARD_ROUTE_LEN = 96,
	GCS_BLOCK_R4_ROUTE_PROBE_LEN = 97
};

static void
make_r4_route_frame(uint8 *frame, Size frame_len, BufferTag tag, uint8 fill)
{
	memset(frame, fill, frame_len);
	memcpy(frame + GCS_BLOCK_ROUTE_TAG_OFFSET, &tag, sizeof(tag));
}

/* ======================================================================
 * U8 -- REQUEST80 and FORWARD96 use the same offset-16 tag shard as their
 *		 legacy 64-byte forms.  Changing every byte outside the tag must not
 *		 move either extended frame to another worker.
 * ====================================================================== */
UT_TEST(test_r4_extended_route_exact_lengths_and_tag_affinity)
{
	BufferTag tag = make_tag(1663, 5, 24002, MAIN_FORKNUM, 101);
	GcsBlockRequestPayload legacy_req = make_request(tag);
	GcsBlockForwardPayload legacy_fwd = make_forward(tag);
	union {
		uint64 align;
		uint8 bytes[GCS_BLOCK_R4_ROUTE_PROBE_LEN];
	} req_a, req_b, fwd_a, fwd_b;
	int expected = cluster_lms_shard_for_tag(&tag, CLUSTER_LMS_MAX_WORKERS);

	UT_ASSERT_EQ(sizeof(legacy_req), GCS_BLOCK_LEGACY_ROUTE_LEN);
	UT_ASSERT_EQ(sizeof(legacy_fwd), GCS_BLOCK_LEGACY_ROUTE_LEN);
	UT_ASSERT_EQ(cluster_gcs_block_payload_shard(PGRAC_IC_MSG_GCS_BLOCK_REQUEST, &legacy_req,
												 sizeof(legacy_req), CLUSTER_LMS_MAX_WORKERS),
				 expected);
	UT_ASSERT_EQ(cluster_gcs_block_payload_shard(PGRAC_IC_MSG_GCS_BLOCK_FORWARD, &legacy_fwd,
												 sizeof(legacy_fwd), CLUSTER_LMS_MAX_WORKERS),
				 expected);

	make_r4_route_frame(req_a.bytes, GCS_BLOCK_R4_REQUEST_ROUTE_LEN, tag, 0x00);
	make_r4_route_frame(req_b.bytes, GCS_BLOCK_R4_REQUEST_ROUTE_LEN, tag, 0xA5);
	UT_ASSERT_EQ(cluster_gcs_block_payload_shard(PGRAC_IC_MSG_GCS_BLOCK_REQUEST, req_a.bytes,
												 GCS_BLOCK_R4_REQUEST_ROUTE_LEN,
												 CLUSTER_LMS_MAX_WORKERS),
				 expected);
	UT_ASSERT_EQ(cluster_gcs_block_payload_shard(PGRAC_IC_MSG_GCS_BLOCK_REQUEST, req_b.bytes,
												 GCS_BLOCK_R4_REQUEST_ROUTE_LEN,
												 CLUSTER_LMS_MAX_WORKERS),
				 expected);

	make_r4_route_frame(fwd_a.bytes, GCS_BLOCK_R4_FORWARD_ROUTE_LEN, tag, 0x00);
	make_r4_route_frame(fwd_b.bytes, GCS_BLOCK_R4_FORWARD_ROUTE_LEN, tag, 0x5A);
	UT_ASSERT_EQ(cluster_gcs_block_payload_shard(PGRAC_IC_MSG_GCS_BLOCK_FORWARD, fwd_a.bytes,
												 GCS_BLOCK_R4_FORWARD_ROUTE_LEN,
												 CLUSTER_LMS_MAX_WORKERS),
				 expected);
	UT_ASSERT_EQ(cluster_gcs_block_payload_shard(PGRAC_IC_MSG_GCS_BLOCK_FORWARD, fwd_b.bytes,
												 GCS_BLOCK_R4_FORWARD_ROUTE_LEN,
												 CLUSTER_LMS_MAX_WORKERS),
				 expected);
}

/* ======================================================================
 * Spec 8.4 D4 -- endpoint -2 plus R4_UNDO_DATA_FETCH is the sole exception
 * to the offset-16 tag shard: it must use the existing DATA worker 0 in both
 * directions.  Either half alone remains ordinary tag-sharded, and adjacent
 * lengths remain unroutable.
 * ====================================================================== */
UT_TEST(test_r4_kind4_internal_endpoint_routes_only_to_data_worker0)
{
	ClusterR4CrForwardPayload forward;
	BufferTag tag;
	int ordinary_shard = 0;
	int i;

	for (i = 1; i < 10000 && ordinary_shard == 0; i++) {
		tag = make_tag(1663, 5, 26000, MAIN_FORKNUM, (BlockNumber)i);
		ordinary_shard = cluster_lms_shard_for_tag(&tag, CLUSTER_LMS_MAX_WORKERS);
	}
	UT_ASSERT(ordinary_shard > 0);

	memset(&forward, 0, sizeof(forward));
	forward.base.tag = tag;
	forward.base.requester_backend_id = CLUSTER_GCS_BLOCK_R4_INTERNAL_ENDPOINT;
	forward.extension.r4_version = CLUSTER_R4_WIRE_VERSION;
	forward.extension.r4_kind = CLUSTER_R4_WIRE_UNDO_DATA_FETCH;
	UT_ASSERT_EQ(cluster_gcs_block_payload_shard(PGRAC_IC_MSG_GCS_BLOCK_FORWARD, &forward,
												 sizeof(forward), CLUSTER_LMS_MAX_WORKERS),
				 0);

	forward.base.requester_backend_id = 1;
	UT_ASSERT_EQ(cluster_gcs_block_payload_shard(PGRAC_IC_MSG_GCS_BLOCK_FORWARD, &forward,
												 sizeof(forward), CLUSTER_LMS_MAX_WORKERS),
				 ordinary_shard);
	forward.base.requester_backend_id = CLUSTER_GCS_BLOCK_R4_INTERNAL_ENDPOINT;
	forward.extension.r4_kind = CLUSTER_R4_WIRE_CR_BUILD;
	UT_ASSERT_EQ(cluster_gcs_block_payload_shard(PGRAC_IC_MSG_GCS_BLOCK_FORWARD, &forward,
												 sizeof(forward), CLUSTER_LMS_MAX_WORKERS),
				 ordinary_shard);
	forward.extension.r4_kind = CLUSTER_R4_WIRE_UNDO_DATA_FETCH;
	UT_ASSERT_EQ(cluster_gcs_block_payload_shard(PGRAC_IC_MSG_GCS_BLOCK_FORWARD, &forward,
												 sizeof(forward) - 1, CLUSTER_LMS_MAX_WORKERS),
				 -1);
	UT_ASSERT_EQ(cluster_gcs_block_payload_shard(PGRAC_IC_MSG_GCS_BLOCK_FORWARD, &forward,
												 sizeof(forward) + 1, CLUSTER_LMS_MAX_WORKERS),
				 -1);
}

/* The M4-scoped existing kind-2 extension shares the already existing DATA0
 * cooperative driver.  Its backend endpoint is positive (the reply lands in
 * that backend's physical R4_CR slot), so extension kind -- not endpoint -2
 * -- is the frozen routing discriminator. */
UT_TEST(test_r4_kind2_terminal_census_routes_to_existing_data_worker0)
{
	ClusterR4CrForwardPayload forward;
	BufferTag tag;
	int ordinary_shard = 0;
	int i;

	for (i = 1; i < 10000 && ordinary_shard == 0; i++) {
		tag = make_tag(GCS_BLOCK_UNDO_FETCH_TAG_MAGIC, 5, 0, MAIN_FORKNUM, (BlockNumber)i);
		ordinary_shard = cluster_lms_shard_for_tag(&tag, CLUSTER_LMS_MAX_WORKERS);
	}
	UT_ASSERT(ordinary_shard > 0);
	memset(&forward, 0, sizeof(forward));
	forward.base.tag = tag;
	forward.base.requester_backend_id = 1;
	forward.extension.r4_version = CLUSTER_R4_WIRE_VERSION;
	forward.extension.r4_kind = CLUSTER_R4_WIRE_TX_RESOLVE;
	UT_ASSERT_EQ(cluster_gcs_block_payload_shard(PGRAC_IC_MSG_GCS_BLOCK_FORWARD, &forward,
												 sizeof(forward), CLUSTER_LMS_MAX_WORKERS),
				 0);

	forward.extension.r4_version = 0;
	UT_ASSERT_EQ(cluster_gcs_block_payload_shard(PGRAC_IC_MSG_GCS_BLOCK_FORWARD, &forward,
												 sizeof(forward), CLUSTER_LMS_MAX_WORKERS),
				 ordinary_shard);
	forward.extension.r4_version = CLUSTER_R4_WIRE_VERSION;
	forward.extension.r4_kind = CLUSTER_R4_WIRE_CR_BUILD;
	UT_ASSERT_EQ(cluster_gcs_block_payload_shard(PGRAC_IC_MSG_GCS_BLOCK_FORWARD, &forward,
												 sizeof(forward), CLUSTER_LMS_MAX_WORKERS),
				 ordinary_shard);
	UT_ASSERT_EQ(cluster_gcs_block_payload_shard(PGRAC_IC_MSG_GCS_BLOCK_FORWARD, &forward,
												 sizeof(forward) - 1, CLUSTER_LMS_MAX_WORKERS),
				 -1);
}

/* ======================================================================
 * U9 -- extended route admission is exact, not a minimum-size check:
 *		 REQUEST accepts only 64/80 and FORWARD accepts only 64/96.  Adjacent
 *		 lengths and the other frame kind's extended length fail closed.
 * ====================================================================== */
UT_TEST(test_r4_extended_route_length_mismatch_refused)
{
	BufferTag tag = make_tag(1663, 5, 24003, MAIN_FORKNUM, 103);
	union {
		uint64 align;
		uint8 bytes[GCS_BLOCK_R4_ROUTE_PROBE_LEN];
	} payload;
	const uint16 request_bad_lengths[] = { 0, 63, 65, 79, 81, 95, 96, 97 };
	const uint16 forward_bad_lengths[] = { 0, 63, 65, 79, 80, 81, 95, 97 };
	Size i;

	make_r4_route_frame(payload.bytes, sizeof(payload.bytes), tag, 0xC3);
	for (i = 0; i < lengthof(request_bad_lengths); i++)
		UT_ASSERT_EQ(cluster_gcs_block_payload_shard(PGRAC_IC_MSG_GCS_BLOCK_REQUEST, payload.bytes,
													 request_bad_lengths[i],
													 CLUSTER_LMS_MAX_WORKERS),
					 -1);
	for (i = 0; i < lengthof(forward_bad_lengths); i++)
		UT_ASSERT_EQ(cluster_gcs_block_payload_shard(PGRAC_IC_MSG_GCS_BLOCK_FORWARD, payload.bytes,
													 forward_bad_lengths[i],
													 CLUSTER_LMS_MAX_WORKERS),
					 -1);
}

/* ======================================================================
 * Current-MX describe/proof forwards preserve the request identity in the
 * legacy 64-byte prefix but overlay its BufferTag.  Their frozen 128-byte
 * frames therefore route by GcsBlockCurrentMxRouteTagMake(), and only the
 * two runtime kinds are admitted at that exact length.
 * ====================================================================== */
UT_TEST(test_current_mx_forward128_routes_by_request_identity)
{
	ClusterCurrentMxDescribeForwardV2 describe;
	ClusterCurrentMxProofForwardV2 proof;
	BufferTag route_tag;
	int expected;

	memset(&describe, 0, sizeof(describe));
	describe.prefix.request_id = UINT64CONST(0x1122334455667788);
	describe.prefix.epoch = UINT64CONST(0x0102030405060708);
	describe.prefix.original_requester_node = 2;
	describe.prefix.requester_backend_id = 19;
	describe.prefix.kind = GCS_BLOCK_FORWARD_KIND_CURRENT_MX_DESCRIBE;
	describe.trailer.magic = CLUSTER_CURRENT_MX_WIRE_MAGIC;
	describe.trailer.version = CLUSTER_CURRENT_MX_WIRE_VERSION;

	route_tag = GcsBlockCurrentMxRouteTagMake(describe.prefix.request_id, describe.prefix.epoch,
											  describe.prefix.original_requester_node,
											  describe.prefix.requester_backend_id);
	expected = cluster_lms_shard_for_tag(&route_tag, CLUSTER_LMS_MAX_WORKERS);
	UT_ASSERT_EQ(cluster_gcs_block_payload_shard(PGRAC_IC_MSG_GCS_BLOCK_FORWARD, &describe,
												 sizeof(describe), CLUSTER_LMS_MAX_WORKERS),
				 expected);

	memset(&proof, 0, sizeof(proof));
	proof.prefix.request_id = describe.prefix.request_id;
	proof.prefix.epoch = describe.prefix.epoch;
	proof.prefix.original_requester_node = describe.prefix.original_requester_node;
	proof.prefix.requester_backend_id = describe.prefix.requester_backend_id;
	proof.prefix.kind = GCS_BLOCK_FORWARD_KIND_CURRENT_MX_MEMBER_PROOF;
	proof.trailer.magic = CLUSTER_CURRENT_MX_WIRE_MAGIC;
	proof.trailer.version = CLUSTER_CURRENT_MX_WIRE_VERSION;
	UT_ASSERT_EQ(cluster_gcs_block_payload_shard(PGRAC_IC_MSG_GCS_BLOCK_FORWARD, &proof,
												 sizeof(proof), CLUSTER_LMS_MAX_WORKERS),
				 expected);

	describe.prefix.kind = GCS_BLOCK_FORWARD_KIND_CURRENT_MX_STATS;
	UT_ASSERT_EQ(cluster_gcs_block_payload_shard(PGRAC_IC_MSG_GCS_BLOCK_FORWARD, &describe,
												 sizeof(describe), CLUSTER_LMS_MAX_WORKERS),
				 -1);
	UT_ASSERT_EQ(cluster_gcs_block_payload_shard(PGRAC_IC_MSG_GCS_BLOCK_FORWARD, &proof,
												 sizeof(proof) - 1, CLUSTER_LMS_MAX_WORKERS),
				 -1);
	UT_ASSERT_EQ(cluster_gcs_block_payload_shard(PGRAC_IC_MSG_GCS_BLOCK_FORWARD, &proof,
												 sizeof(proof) + 1, CLUSTER_LMS_MAX_WORKERS),
				 -1);
}

/* MXA-T24: the 136-byte CTRC CLOSE/CERTIFICATE family must use the same
 * request-identity route at enqueue and receive.  A malformed domain marker
 * is unroutable, never a legacy BufferTag frame. */
UT_TEST(test_ctrc_forward136_routes_only_exact_wire_domain)
{
	ClusterCtrcSealRequestV1 request;
	BufferTag route_tag;
	int expected;

	memset(&request, 0, sizeof(request));
	request.request_id = UINT64CONST(0x8899aabbccddeeff);
	request.cluster_epoch = UINT64CONST(0x01020304);
	request.original_requester_node = 2;
	request.requester_backend_id = CLUSTER_CTRC_INTERNAL_ENDPOINT;
	request.grant_generation = 17;
	request.slot_wrap = 3;
	request.owner_instance = 3;
	request.suboperation = CTRC_SEAL_CLOSE_AND_CLEAN;
	request.segment_generation = 9;
	request.selector_version = CLUSTER_CTRC_SELECTOR_VERSION;
	request.forward_kind = CLUSTER_CTRC_FORWARD_KIND;
	request.magic = CLUSTER_CTRC_WIRE_MAGIC;
	request.wire_version = CLUSTER_CTRC_WIRE_VERSION;
	request.wire_length = CLUSTER_CTRC_SEAL_REQUEST_BYTES;
	request.participant_capability_record_generation = 23;

	route_tag = GcsBlockCurrentMxRouteTagMake(request.request_id, request.cluster_epoch,
											  request.original_requester_node,
											  request.requester_backend_id);
	expected = cluster_lms_shard_for_tag(&route_tag, CLUSTER_LMS_MAX_WORKERS);
	UT_ASSERT_EQ(cluster_gcs_block_payload_shard(PGRAC_IC_MSG_GCS_BLOCK_FORWARD, &request,
												 sizeof(request), CLUSTER_LMS_MAX_WORKERS),
				 expected);

	request.forward_kind = GCS_BLOCK_FORWARD_KIND_UNDO_FRESHREF_C1B_PAIR;
	UT_ASSERT_EQ(cluster_gcs_block_payload_shard(PGRAC_IC_MSG_GCS_BLOCK_FORWARD, &request,
												 sizeof(request), CLUSTER_LMS_MAX_WORKERS),
				 -1);
	request.forward_kind = CLUSTER_CTRC_FORWARD_KIND;
	request.wire_version++;
	UT_ASSERT_EQ(cluster_gcs_block_payload_shard(PGRAC_IC_MSG_GCS_BLOCK_FORWARD, &request,
												 sizeof(request), CLUSTER_LMS_MAX_WORKERS),
				 -1);
	request.wire_version = CLUSTER_CTRC_WIRE_VERSION;
	request.reserved_tail = 1;
	UT_ASSERT_EQ(cluster_gcs_block_payload_shard(PGRAC_IC_MSG_GCS_BLOCK_FORWARD, &request,
												 sizeof(request), CLUSTER_LMS_MAX_WORKERS),
				 -1);
	request.reserved_tail = 0;
	request.participant_capability_record_generation = 0;
	UT_ASSERT_EQ(cluster_gcs_block_payload_shard(PGRAC_IC_MSG_GCS_BLOCK_FORWARD, &request,
												 sizeof(request), CLUSTER_LMS_MAX_WORKERS),
				 -1);
	request.participant_capability_record_generation = 23;
	UT_ASSERT_EQ(cluster_gcs_block_payload_shard(PGRAC_IC_MSG_GCS_BLOCK_FORWARD, &request,
												 sizeof(request) - 1, CLUSTER_LMS_MAX_WORKERS),
				 -1);
}

/* Every staged PCM-X frame is tag-affine.  RETIRE/RETIRE_ACK are the only
 * direct-send members because their compact payload intentionally has no tag. */
UT_TEST(test_pi_durable_note_routes_to_exact_tag_worker)
{
	bool seen[CLUSTER_LMS_MAX_WORKERS] = { false };
	int seen_count = 0;
	int i;

	for (i = 0; i < 4096 && seen_count < CLUSTER_LMS_MAX_WORKERS; i++) {
		BufferTag tag = make_tag(1663, 5, 16384 + (i % 4), MAIN_FORKNUM, (BlockNumber)i);
		GcsBlockInvalidateAckPayload note = { 0 };
		int expected = cluster_lms_shard_for_tag(&tag, CLUSTER_LMS_MAX_WORKERS);
		int routed;

		note.tag = tag;
		note.ack_status = GCS_BLOCK_INVALIDATE_ACK_STATUS_PI_DURABLE_NOTE;
		routed = cluster_gcs_block_payload_shard(PGRAC_IC_MSG_GCS_BLOCK_INVALIDATE_ACK, &note,
												 sizeof(note), CLUSTER_LMS_MAX_WORKERS);
		UT_ASSERT_EQ(routed, expected);
		if (!seen[routed]) {
			seen[routed] = true;
			seen_count++;
		}
	}
	UT_ASSERT(seen[1]);
	UT_ASSERT_EQ(seen_count, CLUSTER_LMS_MAX_WORKERS);
}

/* Build each legal Resource-X wire shape through the production codec.  The
 * shard router must validate the reused message domain before it extracts the
 * decoded assertion tag; raw offset-16 routing would accept the corrupted
 * companion frame below. */
static uint16
make_resource_x_route_frame(ResourceXWireKind kind, uint8 msg_type, BufferTag tag, uint8 *bytes,
							uint16 capacity)
{
	ResourceXDecodedFrame frame;
	ResourceXWireReject reject = RESOURCE_X_WIRE_REJECT_BAD_ARGUMENT;
	uint16 payload_len = 0;

	memset(&frame, 0, sizeof(frame));
	UT_ASSERT(resource_x_assertion_init(&tag, 3, &frame.common.logical_assertion));
	frame.kind = kind;
	frame.common.base_authority_generation = 11;
	frame.common.resource_formation = 12;
	frame.common.master_session_incarnation = 13;
	frame.common.assertion_sequence = 14;
	frame.common.ordered_lane = 15;
	frame.common.action_node = 3;
	frame.common.observed_mode = PCM_STATE_N;
	frame.common.target_mode = PCM_STATE_X;
	frame.common.sender_connection_generation = 16;
	frame.common.authority_generation = 17;

	if (kind == RESOURCE_X_WIRE_PREASSERT_BOOTSTRAP) {
		frame.common.ordered_lane = 0;
		frame.common.authority_generation = 0;
		if (msg_type == RESOURCE_X_MSG_ASSERT_X)
			frame.common.base_authority_generation = 0;
		else
			frame.common.outcome = RESOURCE_X_OUTCOME_OK;
	} else if (kind == RESOURCE_X_WIRE_BLOCK_TO_N) {
		frame.common.action_node = 7;
		frame.common.observed_mode = PCM_STATE_X;
		frame.common.target_mode = PCM_STATE_N;
		frame.common.source_candidate = 1;
		frame.common.retain_pi_if_dirty = 1;
	} else if (kind == RESOURCE_X_WIRE_BLOCKED_TO_N) {
		frame.common.action_node = 7;
		frame.common.observed_mode = PCM_STATE_X;
		frame.common.target_mode = PCM_STATE_N;
		frame.common.outcome = RESOURCE_X_OUTCOME_OK;
		frame.blocked_has_remote_proof = capacity == RESOURCE_X_PROOF_V1_BYTES;
		if (frame.blocked_has_remote_proof) {
			frame.body.blocked_to_n.source_carrier_generation = 18;
			frame.body.blocked_to_n.requester_target_generation = frame.common.assertion_sequence;
			frame.body.blocked_to_n.source_disposition = RESOURCE_X_DISPOSITION_REMOTE_NONWRITABLE;
			frame.body.blocked_to_n.proof_kind = RESOURCE_X_PROOF_REMOTE_CARRIER;
			frame.body.blocked_to_n.holder_connection_generation = 20;
			frame.body.blocked_to_n.acting_formation = frame.common.resource_formation;
		}
	} else if (kind == RESOURCE_X_WIRE_RELEASE_X) {
		frame.common.observed_mode = PCM_STATE_X;
		frame.common.target_mode = PCM_STATE_N;
		frame.common.outcome = RESOURCE_X_OUTCOME_OK;
	} else if (kind == RESOURCE_X_WIRE_LOCAL_PROOF_DECLARATION) {
		frame.common.outcome = RESOURCE_X_OUTCOME_OK;
		frame.body.local_proof.local_holder_authority_generation = 21;
		frame.body.local_proof.requester_target_generation = frame.common.assertion_sequence;
		frame.body.local_proof.requester_connection_generation = 23;
		frame.body.local_proof.local_proof_generation = 24;
	} else if (kind == RESOURCE_X_WIRE_AUTHORITY_GRANT) {
		frame.common.outcome = RESOURCE_X_OUTCOME_OK;
		frame.body.authority_grant.final_authority_generation = frame.common.authority_generation;
		frame.body.authority_grant.requester_target_generation = frame.common.assertion_sequence;
		frame.body.authority_grant.proof_kind = RESOURCE_X_PROOF_DURABLE_STORAGE;
		frame.body.authority_grant.source_disposition = RESOURCE_X_DISPOSITION_DURABLE_STORAGE;
		frame.body.authority_grant.requester_connection_generation = 26;
	} else if (kind == RESOURCE_X_WIRE_IMAGE_ENVELOPE) {
		frame.common.action_node = 7;
		frame.common.outcome = RESOURCE_X_OUTCOME_OK;
		frame.body.image_envelope.conversion_base_generation
			= frame.common.base_authority_generation;
		frame.body.image_envelope.source_carrier_generation = 27;
		frame.body.image_envelope.requester_target_generation = frame.common.assertion_sequence;
		frame.body.image_envelope.image_length = RESOURCE_X_PAGE_BYTES;
		frame.body.image_envelope.source_disposition = RESOURCE_X_DISPOSITION_REMOTE_NONWRITABLE;
		frame.body.image_envelope.proof_kind = RESOURCE_X_PROOF_REMOTE_CARRIER;
	} else if (kind == RESOURCE_X_WIRE_INSTALL_SETTLEMENT) {
		frame.common.outcome = RESOURCE_X_OUTCOME_OK;
		frame.body.install_settlement.conversion_base_generation
			= frame.common.base_authority_generation;
		frame.body.install_settlement.final_authority_generation
			= frame.common.authority_generation;
		frame.body.install_settlement.requester_connection_generation = 29;
		frame.body.install_settlement.requester_target_generation = frame.common.assertion_sequence;
		frame.body.install_settlement.installed_mode = PCM_STATE_X;
		frame.body.install_settlement.requester_role = RESOURCE_X_REQUESTER_ROLE_ACQUIRER;
		frame.body.install_settlement.terminal_outcome = RESOURCE_X_OUTCOME_OK;
		frame.body.install_settlement.terminal_state = RESOURCE_X_SETTLEMENT_TERMINAL_INSTALLED;
	}

	UT_ASSERT(
		cluster_resource_x_wire_encode(msg_type, &frame, bytes, capacity, &payload_len, &reject));
	UT_ASSERT_EQ(reject, RESOURCE_X_WIRE_REJECT_NONE);
	return payload_len;
}

UT_TEST(test_resource_x_reused_types_route_only_after_strict_domain_decode)
{
	static const struct {
		uint8 msg_type;
		ResourceXWireKind kind;
		uint16 payload_len;
	} cases[] = {
		{ RESOURCE_X_MSG_ASSERT_X, RESOURCE_X_WIRE_PREASSERT_BOOTSTRAP,
		  RESOURCE_X_CONTROL_V1_BYTES },
		{ RESOURCE_X_MSG_ASSERT_X, RESOURCE_X_WIRE_ASSERT_X, RESOURCE_X_CONTROL_V1_BYTES },
		{ RESOURCE_X_MSG_ASSERT_X, RESOURCE_X_WIRE_LOCAL_PROOF_DECLARATION,
		  RESOURCE_X_SHORT_V1_BYTES },
		{ RESOURCE_X_MSG_BLOCK_TO_N, RESOURCE_X_WIRE_BLOCK_TO_N, RESOURCE_X_CONTROL_V1_BYTES },
		{ RESOURCE_X_MSG_BLOCKED_TO_N, RESOURCE_X_WIRE_BLOCKED_TO_N, RESOURCE_X_CONTROL_V1_BYTES },
		{ RESOURCE_X_MSG_BLOCKED_TO_N, RESOURCE_X_WIRE_BLOCKED_TO_N, RESOURCE_X_PROOF_V1_BYTES },
		{ RESOURCE_X_MSG_IMAGE_OR_GRANT, RESOURCE_X_WIRE_AUTHORITY_GRANT,
		  RESOURCE_X_PROOF_V1_BYTES },
		{ RESOURCE_X_MSG_IMAGE_OR_GRANT, RESOURCE_X_WIRE_IMAGE_ENVELOPE,
		  RESOURCE_X_IMAGE_V1_BYTES },
		{ RESOURCE_X_MSG_IMAGE_OR_GRANT, RESOURCE_X_WIRE_PREASSERT_BOOTSTRAP,
		  RESOURCE_X_CONTROL_V1_BYTES },
		{ RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, RESOURCE_X_WIRE_RELEASE_X,
		  RESOURCE_X_CONTROL_V1_BYTES },
		{ RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, RESOURCE_X_WIRE_INSTALL_SETTLEMENT,
		  RESOURCE_X_SHORT_V1_BYTES }
	};
	BufferTag tag = make_tag(1663, 5, 37001, FSM_FORKNUM, 771);
	union {
		uint64 align;
		uint8 bytes[RESOURCE_X_IMAGE_V1_BYTES];
	} payload;
	int expected = cluster_lms_shard_for_tag(&tag, CLUSTER_LMS_MAX_WORKERS);
	Size i;

	for (i = 0; i < lengthof(cases); i++) {
		uint16 encoded_len = make_resource_x_route_frame(cases[i].kind, cases[i].msg_type, tag,
														 payload.bytes, cases[i].payload_len);

		UT_ASSERT_EQ(encoded_len, cases[i].payload_len);
		UT_ASSERT_EQ(cluster_gcs_block_payload_shard(cases[i].msg_type, payload.bytes, encoded_len,
													 CLUSTER_LMS_MAX_WORKERS),
					 expected);

		payload.bytes[20] ^= UINT8_C(0x01);
		UT_ASSERT_EQ(cluster_gcs_block_payload_shard(cases[i].msg_type, payload.bytes, encoded_len,
													 CLUSTER_LMS_MAX_WORKERS),
					 -1);
		payload.bytes[20] ^= UINT8_C(0x01);
	}
}

UT_TEST(test_resource_x_length_collisions_preserve_legacy_domains)
{
	BufferTag tag = make_tag(1663, 5, 37002, MAIN_FORKNUM, 772);
	GcsBlockRequestPayload request = make_request(tag);
	GcsBlockInvalidatePayload invalidate = make_invalidate(tag);
	GcsBlockInvalidateAckPayload ack = { 0 };
	GcsBlockDonePayload done = make_done(tag);
	uint8 legacy_reply[GCS_BLOCK_REPLY_PAYLOAD_TOTAL_SIZE] = { 0 };
	int expected = cluster_lms_shard_for_tag(&tag, CLUSTER_LMS_MAX_WORKERS);

	ack.tag = tag;
	UT_ASSERT_EQ(cluster_gcs_block_payload_shard(PGRAC_IC_MSG_GCS_BLOCK_REQUEST, &request,
												 sizeof(request), CLUSTER_LMS_MAX_WORKERS),
				 expected);
	UT_ASSERT_EQ(cluster_gcs_block_payload_shard(PGRAC_IC_MSG_GCS_BLOCK_INVALIDATE, &invalidate,
												 sizeof(invalidate), CLUSTER_LMS_MAX_WORKERS),
				 expected);
	UT_ASSERT_EQ(cluster_gcs_block_payload_shard(PGRAC_IC_MSG_GCS_BLOCK_INVALIDATE_ACK, &ack,
												 sizeof(ack), CLUSTER_LMS_MAX_WORKERS),
				 expected);
	UT_ASSERT_EQ(cluster_gcs_block_payload_shard(PGRAC_IC_MSG_GCS_BLOCK_DONE, &done, sizeof(done),
												 CLUSTER_LMS_MAX_WORKERS),
				 expected);
	UT_ASSERT_EQ(cluster_gcs_block_payload_shard(PGRAC_IC_MSG_GCS_BLOCK_REPLY, legacy_reply,
												 sizeof(legacy_reply), CLUSTER_LMS_MAX_WORKERS),
				 -1);
}

int
main(void)
{
	UT_PLAN(16);
	UT_RUN(test_route_matches_shard_for_tag);
	UT_RUN(test_route_ack_request_interleave_affinity);
	UT_RUN(test_route_registry_partition);
	UT_RUN(test_route_unroutable_fail_closed);
	UT_RUN(test_route_length_mismatch_refused);
	UT_RUN(test_route_n1_degenerate_zero);
	UT_RUN(test_route_ignores_non_tag_fields);
	UT_RUN(test_r4_extended_route_exact_lengths_and_tag_affinity);
	UT_RUN(test_r4_kind4_internal_endpoint_routes_only_to_data_worker0);
	UT_RUN(test_r4_kind2_terminal_census_routes_to_existing_data_worker0);
	UT_RUN(test_r4_extended_route_length_mismatch_refused);
	UT_RUN(test_current_mx_forward128_routes_by_request_identity);
	UT_RUN(test_ctrc_forward136_routes_only_exact_wire_domain);
	UT_RUN(test_pi_durable_note_routes_to_exact_tag_worker);
	UT_RUN(test_resource_x_reused_types_route_only_after_strict_domain_decode);
	UT_RUN(test_resource_x_length_collisions_preserve_legacy_domains);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
