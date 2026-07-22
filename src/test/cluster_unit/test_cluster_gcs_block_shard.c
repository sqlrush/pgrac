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
#include "cluster/cluster_pcm_x_convert.h"
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

/* Every staged PCM-X frame is tag-affine.  RETIRE/RETIRE_ACK are the only
 * direct-send members because their compact payload intentionally has no tag. */
UT_TEST(test_pcm_x_route_truth_table)
{
	BufferTag tag = make_tag(1663, 5, 24001, MAIN_FORKNUM, 73);
	union {
		PcmXGrantPayload largest;
		uint8 bytes[sizeof(PcmXGrantPayload)];
	} payload;
	struct {
		uint8 msg_type;
		uint16 payload_len;
	} staged[] = {
		{ PGRAC_IC_MSG_PCM_X_ENQUEUE, sizeof(PcmXEnqueuePayload) },
		{ PGRAC_IC_MSG_PCM_X_ADMIT_ACK, sizeof(PcmXAdmitAckPayload) },
		{ PGRAC_IC_MSG_PCM_X_ADMIT_CONFIRM, sizeof(PcmXPhasePayload) },
		{ PGRAC_IC_MSG_PCM_X_ADMIT_CONFIRM_ACK, sizeof(PcmXPhasePayload) },
		{ PGRAC_IC_MSG_PCM_X_BLOCKER_SET_BEGIN, sizeof(PcmXBlockerSetHeaderPayload) },
		{ PGRAC_IC_MSG_PCM_X_BLOCKER_SET_EDGE, sizeof(PcmXBlockerChunkPayload) },
		{ PGRAC_IC_MSG_PCM_X_BLOCKER_SET_COMMIT, sizeof(PcmXBlockerSetHeaderPayload) },
		{ PGRAC_IC_MSG_PCM_X_BLOCKER_SET_ACK, sizeof(PcmXPhasePayload) },
		{ PGRAC_IC_MSG_PCM_X_REVOKE, sizeof(PcmXRevokePayload) },
		{ PGRAC_IC_MSG_PCM_X_REVOKE, sizeof(PcmXRevokePayloadV2) },
		{ PGRAC_IC_MSG_PCM_X_IMAGE_READY, sizeof(PcmXGrantPayload) },
		{ PGRAC_IC_MSG_PCM_X_PREPARE_GRANT, sizeof(PcmXGrantPayload) },
		{ PGRAC_IC_MSG_PCM_X_INSTALL_READY, sizeof(PcmXInstallReadyPayload) },
		{ PGRAC_IC_MSG_PCM_X_COMMIT_X, sizeof(PcmXPhasePayload) },
		{ PGRAC_IC_MSG_PCM_X_FINAL_ACK, sizeof(PcmXFinalAckPayload) },
		{ PGRAC_IC_MSG_PCM_X_FINAL_COMMIT_ACK, sizeof(PcmXPhasePayload) },
		{ PGRAC_IC_MSG_PCM_X_FINAL_CONFIRM, sizeof(PcmXPhasePayload) },
		{ PGRAC_IC_MSG_PCM_X_PREHANDLE_CANCEL, sizeof(PcmXPrehandleCancelPayload) },
		{ PGRAC_IC_MSG_PCM_X_PREHANDLE_CANCEL_ACK, sizeof(PcmXAdmitAckPayload) },
		{ PGRAC_IC_MSG_PCM_X_CANCEL, sizeof(PcmXPhasePayload) },
		{ PGRAC_IC_MSG_PCM_X_CANCEL_ACK, sizeof(PcmXPhasePayload) },
		{ PGRAC_IC_MSG_PCM_X_DRAIN_POLL, sizeof(PcmXDrainPollPayload) },
		{ PGRAC_IC_MSG_PCM_X_DRAIN_ACK, sizeof(PcmXPhasePayload) },
	};
	Size i;
	int expected = cluster_lms_shard_for_tag(&tag, CLUSTER_LMS_MAX_WORKERS);

	memset(&payload, 0, sizeof(payload));
	memcpy(payload.bytes, &tag, sizeof(tag));
	for (i = 0; i < lengthof(staged); i++) {
		UT_ASSERT_EQ(cluster_gcs_block_payload_shard(staged[i].msg_type, payload.bytes,
													 staged[i].payload_len,
													 CLUSTER_LMS_MAX_WORKERS),
					 expected);
		UT_ASSERT_EQ(cluster_gcs_block_payload_shard(staged[i].msg_type, payload.bytes,
													 staged[i].payload_len - 1,
													 CLUSTER_LMS_MAX_WORKERS),
					 -1);
	}
	UT_ASSERT_EQ(cluster_gcs_block_payload_shard(PGRAC_IC_MSG_PCM_X_RETIRE_UP_TO, payload.bytes,
												 sizeof(PcmXRetirePayload),
												 CLUSTER_LMS_MAX_WORKERS),
				 -1);
	UT_ASSERT_EQ(cluster_gcs_block_payload_shard(PGRAC_IC_MSG_PCM_X_RETIRE_ACK, payload.bytes,
												 sizeof(PcmXRetirePayload),
												 CLUSTER_LMS_MAX_WORKERS),
				 -1);
}

/* A status-3 PI durable note is an INVALIDATE_ACK frame and must use the
 * exact tag shard.  Exercise every live worker, including worker 1 (the
 * t/400 failure arm), against the real payload router. */
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

int
main(void)
{
	UT_PLAN(9);
	UT_RUN(test_route_matches_shard_for_tag);
	UT_RUN(test_route_ack_request_interleave_affinity);
	UT_RUN(test_route_registry_partition);
	UT_RUN(test_route_unroutable_fail_closed);
	UT_RUN(test_route_length_mismatch_refused);
	UT_RUN(test_route_n1_degenerate_zero);
	UT_RUN(test_route_ignores_non_tag_fields);
	UT_RUN(test_pcm_x_route_truth_table);
	UT_RUN(test_pi_durable_note_routes_to_exact_tag_worker);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
