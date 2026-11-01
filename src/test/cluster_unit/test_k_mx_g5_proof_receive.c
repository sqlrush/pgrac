/* Provisional proof windows through the original composition owner. */
int k_proof_original_main(void);
#define main k_proof_original_main
#include "test_cluster_mx_composition.c"
#undef main
#include "cluster/cluster_multixact_range_wire.h"

static ClusterMxRangeWireContext
context(void)
{
	ClusterMxRangeWireContext c = { 0 };
	c.request_id = 501;
	c.epoch = 9;
	c.key.origin_node_id = 2;
	c.key.cluster_epoch = 9;
	c.key.multixact_id = 1000;
	c.format_id = CLUSTER_FOREIGN_MX_FORMAT_ID_V1;
	c.version = CLUSTER_FOREIGN_MX_FORMAT_VERSION_V1;
	c.loaded_catalog = 202611010;
	c.peer_capabilities = CLUSTER_FOREIGN_MX_REQUIRED_CAPABILITIES;
	c.peer_generation = 42;
	c.requested_cap = 7;
	c.source = source;
	c.source_bytes = sizeof(source);
	return c;
}
static ClusterMxCompositionReceive *
composition(uint32 total, ClusterMxCompositionReceiveStats *stats)
{
	ClusterMxCompositionReceive *op = begin(total, stats);
	ClusterCurrentMxMemberDesc members[256];
	for (uint32 first = 0; first < total;) {
		uint32 n = Min(total - first, 256);
		ClusterMxCompositionReply r = reply(total, first, n, members);
		UT_ASSERT_EQ(cluster_mx_composition_accept(&op, MXCOMP_READ_OK, &r),
					 r.last ? MXCOMP_COMPLETE : MXCOMP_PARTIAL);
		first += n;
	}
	return op;
}
static ClusterMxProofReply
window(uint32 total, uint32 first, uint32 n, ClusterMxFullMemberProof *proofs,
	   ClusterCurrentMxMemberDesc *members)
{
	ClusterMxProofReply r = { 0 };
	r.source = source;
	r.source_bytes = sizeof(source);
	r.total = total;
	r.first = first;
	r.count = n;
	r.next = first + n;
	r.last = r.next == total;
	r.proofs = proofs;
	r.proof_bytes = (Size)n * sizeof(*proofs);
	r.frame_bytes = 144 + r.proof_bytes;
	for (uint32 i = 0; i < n; i++) {
		memset(&proofs[i], 0, sizeof(proofs[i]));
		memset(&members[i], 0, sizeof(members[i]));
		members[i].xid = 100 + first + i;
		proofs[i].member_ordinal = first + i;
		proofs[i].origin_node_id = 2;
		proofs[i].proof.member_xid = members[i].xid;
		proofs[i].proof.member_ordinal = i;
		proofs[i].proof.state = CCM_COMMITTED;
		proofs[i].proof.commit_scn = 1000 + first + i;
	}
	return r;
}
static uint32
encode(const ClusterMxRangeWireContext *c, const ClusterMxProofReply *r, PGAlignedBlock *frame)
{
	uint32 bytes = 0;
	UT_ASSERT(
		cluster_mx_range_wire_encode_proofs(c, MXCOMP_READ_OK, r, frame->data, BLCKSZ, &bytes));
	return bytes;
}
UT_TEST(full_proofs_use_32_bit_ordinals_and_wait_for_all)
{
	const uint32 sizes[] = { 2, 257, 65537 };
	for (unsigned k = 0; k < lengthof(sizes); k++) {
		ClusterMxCompositionReceiveStats stats = { 0 };
		ClusterMxCompositionReceive *op = composition(sizes[k], &stats);
		ClusterMxRangeWireContext c = context();
		ClusterMxFullMemberProof entries[7];
		ClusterCurrentMxMemberDesc members[7];
		uint32 nout = 99, seen = 0;
		const ClusterMxCompositionProofPage *page;
		UT_ASSERT_NULL(cluster_mx_composition_proofs(op, &nout));
		UT_ASSERT_EQ(nout, 0);
		for (uint32 first = 0; first < sizes[k];) {
			uint32 n = Min(sizes[k] - first, 7);
			PGAlignedBlock frame;
			ClusterMxProofReply r = window(sizes[k], first, n, entries, members), out;
			uint32 bytes = encode(&c, &r, &frame);
			UT_ASSERT_EQ(cluster_mx_range_wire_decode_proofs(frame.data, bytes, &c, 2, sizes[k],
															 first, members, n, &out),
						 MXCOMP_READ_OK);
			UT_ASSERT_EQ(cluster_mx_composition_accept_proofs(&op, MXCOMP_READ_OK, &out),
						 r.last ? MXCOMP_COMPLETE : MXCOMP_PARTIAL);
			if (!r.last) {
				UT_ASSERT_NULL(cluster_mx_composition_proofs(op, &nout));
				UT_ASSERT_EQ(nout, 0);
			}
			first += n;
		}
		page = cluster_mx_composition_proofs(op, &nout);
		UT_ASSERT_NOT_NULL(page);
		UT_ASSERT_EQ(nout, sizes[k]);
		for (; page; page = page->next)
			for (uint32 i = 0; i < page->count; i++, seen++) {
				UT_ASSERT_EQ(page->proofs[i].member_ordinal, seen);
				UT_ASSERT_EQ(page->proofs[i].proof.member_xid, 100 + seen);
				UT_ASSERT_EQ(page->proofs[i].proof.commit_scn, 1000 + seen);
				UT_ASSERT_EQ(page->proofs[i].proof.member_ordinal, i);
			}
		UT_ASSERT_EQ(seen, sizes[k]);
		cluster_mx_composition_finish(&op);
		check_discarded(op);
	}
}
UT_TEST(bad_last_reply_discards_composition_and_proofs)
{
	for (unsigned fault = 0; fault < 12; fault++) {
		ClusterMxCompositionReceiveStats stats = { 0 };
		ClusterMxCompositionReceive *op = composition(9, &stats);
		ClusterMxRangeWireContext c = context();
		ClusterMxFullMemberProof entries[7];
		ClusterCurrentMxMemberDesc members[7];
		PGAlignedBlock frame;
		ClusterMxProofReply r = window(9, 0, 7, entries, members), out, empty = { 0 };
		ClusterMxRangeWireHeader *h;
		ClusterMxFullMemberProof *wire;
		uint32 bytes = encode(&c, &r, &frame);
		UT_ASSERT_EQ(
			cluster_mx_range_wire_decode_proofs(frame.data, bytes, &c, 2, 9, 0, members, 7, &out),
			MXCOMP_READ_OK);
		UT_ASSERT_EQ(cluster_mx_composition_accept_proofs(&op, MXCOMP_READ_OK, &out),
					 MXCOMP_PARTIAL);
		r = window(9, 7, 2, entries, members);
		bytes = encode(&c, &r, &frame);
		h = (void *)frame.data;
		wire = (void *)(frame.data + 144);
		switch (fault) {
		case 0:
			h->version++;
			break;
		case 1:
			h->request_id++;
			break;
		case 2:
			h->epoch++;
			break;
		case 3:
			frame.data[80] ^= 1;
			break;
		case 4:
			h->first++;
			break;
		case 5:
			h->next--;
			break;
		case 6:
			wire[1].member_ordinal = 65536;
			break;
		case 7:
			wire[1].proof.member_xid++;
			break;
		case 8:
			wire[1].proof.state = CCM_UNKNOWN;
			break;
		case 9:
			wire[1].proof.commit_scn = InvalidScn;
			break;
		case 10:
			wire[1].proof.origin_fullxid_v2[0] = 2;
			break;
		case 11:
			wire[1].origin_node_id = 3;
			break;
		}
		memset(&out, 0xa5, sizeof(out));
		UT_ASSERT_EQ(
			cluster_mx_range_wire_decode_proofs(frame.data, bytes, &c, 2, 9, 7, members, 2, &out),
			MXCOMP_READ_UNKNOWN);
		UT_ASSERT_EQ(memcmp(&out, &empty, sizeof(out)), 0);
		UT_ASSERT_EQ(cluster_mx_composition_accept_proofs(&op, MXCOMP_READ_UNKNOWN, &out),
					 MXCOMP_UNKNOWN);
		check_discarded(op);
		UT_ASSERT_EQ(stats.unknown_results, 1);
	}
}
UT_TEST(proof_receiver_rejects_order_total_and_member_drift)
{
	for (unsigned fault = 0; fault < 7; fault++) {
		ClusterMxCompositionReceiveStats stats = { 0 };
		ClusterMxCompositionReceive *op = composition(9, &stats);
		ClusterMxFullMemberProof entries[7];
		ClusterCurrentMxMemberDesc members[7];
		ClusterMxProofReply r = window(9, 0, 7, entries, members);
		UT_ASSERT_EQ(cluster_mx_composition_accept_proofs(&op, MXCOMP_READ_OK, &r), MXCOMP_PARTIAL);
		r = window(9, 7, 2, entries, members);
		switch (fault) {
		case 0:
			r.first = 0;
			break;
		case 1:
			r.total++;
			break;
		case 2:
			r.last = false;
			break;
		case 3:
			entries[1].proof.member_status = MultiXactStatusUpdate;
			break;
		case 4:
			entries[1].proof.member_xid++;
			break;
		case 5:
			entries[1].proof.member_ordinal = 7;
			break;
		case 6:
			r.source_bytes--;
			break;
		}
		UT_ASSERT_EQ(cluster_mx_composition_accept_proofs(&op, MXCOMP_READ_OK, &r), MXCOMP_UNKNOWN);
		UT_ASSERT_NULL(op);
		cluster_mx_composition_finish(&op);
		check_discarded(op);
	}
}
UT_TEST(proof_unknown_retry_and_extra_tail_have_no_partial_output)
{
	for (unsigned mode = 0; mode < 3; mode++) {
		ClusterMxCompositionReceiveStats stats = { 0 };
		ClusterMxCompositionReceive *op = composition(2, &stats);
		ClusterMxFullMemberProof entries[7];
		ClusterCurrentMxMemberDesc members[7];
		ClusterMxProofReply r = window(2, 0, 2, entries, members);
		if (mode == 2)
			UT_ASSERT_EQ(cluster_mx_composition_accept_proofs(&op, MXCOMP_READ_OK, &r),
						 MXCOMP_COMPLETE);
		UT_ASSERT_EQ(cluster_mx_composition_accept_proofs(&op,
														  mode == 0	  ? MXCOMP_READ_UNKNOWN
														  : mode == 1 ? MXCOMP_READ_RETRY
																	  : MXCOMP_READ_OK,
														  &r),
					 MXCOMP_UNKNOWN);
		check_discarded(op);
		UT_ASSERT_EQ(stats.unknown_results, 1);
		UT_ASSERT_EQ(stats.restart_requests, mode == 1 ? 1 : 0);
	}
}
UT_TEST(proof_allocation_error_retires_original_operation)
{
	static ClusterMxCompositionReceive *op;
	static ClusterMxCompositionReceiveStats stats;
	ClusterMxFullMemberProof entries[7];
	ClusterCurrentMxMemberDesc members[7];
	ClusterMxProofReply r;
	bool caught = false;
	memset(&stats, 0, sizeof(stats));
	op = composition(2, &stats);
	r = window(2, 0, 2, entries, members);
	fail_at = allocations + 1;
	PG_TRY();
	{
		(void)cluster_mx_composition_accept_proofs(&op, MXCOMP_READ_OK, &r);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	fail_at = 0;
	UT_ASSERT(caught);
	check_discarded(op);
	UT_ASSERT_EQ(stats.unknown_results, 1);
}
UT_TEST(proof_codec_preserves_carrier_and_rejects_bad_shapes)
{
	ClusterMxRangeWireContext c = context();
	ClusterMxFullMemberProof entry[7];
	ClusterCurrentMxMemberDesc members[7];
	ClusterMxProofReply r = window(65537, 65536, 1, entry, members), out, empty = { 0 };
	PGAlignedBlock frame, before;
	uint32 bytes;
	entry[0].proof.state = CCM_ACTIVE;
	entry[0].proof.commit_scn = InvalidScn;
	entry[0].proof.key.origin_node_id = 2;
	entry[0].proof.key.undo_segment_id = 3;
	entry[0].proof.key.tt_slot_id = 4;
	entry[0].proof.key.cluster_epoch = 9;
	entry[0].proof.key.local_xid = members[0].xid;
	ClusterCurrentMemberProofSetCtrcBinding(&entry[0].proof, 8, 2);
	ClusterCurrentMemberProofSetCtrcGrant(&entry[0].proof, 17);
	/* Opaque structural fixture, not a valid native envelope or ACTIVE grant. */
	memset(entry[0].proof.origin_fullxid_v2, 0x5a, 144);
	entry[0].proof.origin_fullxid_v2[0] = 2;
	entry[0].proof.origin_fullxid_v2[1] = 0;
	entry[0].proof.origin_fullxid_v2[2] = 144;
	entry[0].proof.origin_fullxid_v2[3] = 0;
	bytes = encode(&c, &r, &frame);
	UT_ASSERT_EQ(bytes, 344);
	UT_ASSERT_EQ(cluster_mx_range_wire_decode_proofs(frame.data, bytes, &c, 2, 65537, 65536,
													 members, 1, &out),
				 MXCOMP_READ_OK);
	UT_ASSERT_NOT_NULL(out.proofs);
	if (out.proofs != NULL) {
		UT_ASSERT_EQ(memcmp(out.proofs, entry, 200), 0);
		UT_ASSERT_EQ(out.proofs[0].member_ordinal, 65536);
		UT_ASSERT_EQ(out.proofs[0].proof.member_ordinal, 0);
	}
	for (unsigned mode = 0; mode < 7; mode++) {
		memset(&out, 0xa5, sizeof(out));
		UT_ASSERT_EQ(cluster_mx_range_wire_decode_proofs(mode == 1 ? frame.data + 1 : frame.data,
														 mode == 2	 ? bytes - 1
														 : mode == 3 ? bytes + 1
														 : mode == 4 ? BLCKSZ
																	 : bytes,
														 mode == 0 ? NULL : &c, mode == 5 ? 3 : 2,
														 65537, 65536, members, mode == 6 ? 8 : 1,
														 &out),
					 MXCOMP_READ_UNKNOWN);
		UT_ASSERT_EQ(memcmp(&out, &empty, sizeof(out)), 0);
	}
	for (unsigned mode = 0; mode < 4; mode++) {
		uint32 written = 99;
		uint16 saved = entry[0].reserved16;
		memset(&before, 0xa5, sizeof(before));
		frame = before;
		if (mode == 0)
			entry[0].reserved16 = 1;
		UT_ASSERT(!cluster_mx_range_wire_encode_proofs(&c, MXCOMP_READ_OK, &r, frame.data,
													   mode == 0   ? BLCKSZ
													   : mode == 1 ? 343
													   : mode == 2 ? 0
																   : 1,
													   &written));
		UT_ASSERT_EQ(written, 0);
		UT_ASSERT_EQ(memcmp(&frame, &before, sizeof(frame)), 0);
		entry[0].reserved16 = saved;
	}
	for (unsigned mode = 0; mode < 2; mode++) {
		uint32 written = 0;
		ClusterMxCompositionReadStatus status = mode ? MXCOMP_READ_RETRY : MXCOMP_READ_UNKNOWN;
		UT_ASSERT(
			cluster_mx_range_wire_encode_proofs(&c, status, NULL, frame.data, BLCKSZ, &written));
		UT_ASSERT_EQ(written, 144);
		memset(&out, 0xa5, sizeof(out));
		UT_ASSERT_EQ(cluster_mx_range_wire_decode_proofs(frame.data, written, &c, 2, 65537, 65536,
														 members, 1, &out),
					 status);
		UT_ASSERT_EQ(memcmp(&out, &empty, sizeof(out)), 0);
	}
}

int
main(void)
{
	UT_PLAN(6);
	UT_RUN(full_proofs_use_32_bit_ordinals_and_wait_for_all);
	UT_RUN(bad_last_reply_discards_composition_and_proofs);
	UT_RUN(proof_receiver_rejects_order_total_and_member_drift);
	UT_RUN(proof_unknown_retry_and_extra_tail_have_no_partial_output);
	UT_RUN(proof_allocation_error_retires_original_operation);
	UT_RUN(proof_codec_preserves_carrier_and_rejects_bad_shapes);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
