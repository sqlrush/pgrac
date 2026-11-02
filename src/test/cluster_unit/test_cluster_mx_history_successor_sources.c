/* Actual collector/graph/decoder/member assembly and consume-once finish.
 * Source bytes and transport landing are controlled leaves; this is not a
 * native TT/CTRC, GCS landing, physical mutation or SQL witness. */
#define HISTORY_ROUNDTRIP_FIXTURE
#include "test_cluster_mx_history_capture.c"
#include "../../backend/storage/page/itemptr.c"

void
cluster_xid_stripe_lazy_latch(void)
{
	abort(); /* successor_setup installs the actual stripe arithmetic first. */
}

typedef enum SuccessorSourceShape {
	SOURCE_PAGE_PRIOR,
	SOURCE_GLOBAL_NONPAGE,
	SOURCE_SHARED,
	SOURCE_ABSENT
} SuccessorSourceShape;
static SuccessorSourceShape source_shape;
static unsigned source_fault, source_B_reads;

/* Change only the controlled source envelope, before the real graph decoder.
 * PAGE_PRIOR: current slot 2 belongs to top120/child128; its saved prior is
 * top80/child96's creation B. GLOBAL_NONPAGE: A -> other-page op -> B.
 * Neither graph contains a current raw-child ITL for B. */
static ClusterMxHistoryStep
source_landing(const ClusterMxHistoryRequestWire *wire, ClusterMxHistoryPeerResult *out)
{
	ClusterMxHistoryRequest r;
	ClusterMxHistoryPeer peer;
	UndoRecordHeader h;
	UndoItlHistoryTrailer history;
	UndoOperationTrailer operation;
	uint32 segment, block;
	uint16 tt, row;
	char *body;
	size_t body_length;
	ClusterMxHistoryStep step = finish_landing_input(wire, out);
	if (step != CMXH_COMPLETE)
		return step;
	if (!cluster_mx_history_request_decode(wire, sizeof(*wire), cluster_node_id, 0, 7, &r)
		|| !uba_decode_record(r.locator.uba, &segment, &block, &tt, &row)
		|| !cluster_mx_history_peer_decode(&out->peer, &peer))
		return CMXH_UNKNOWN;
	memcpy(&h, out->data + sizeof(UndoBlockHeader), sizeof(h));
	body = (char *)out->data + sizeof(UndoBlockHeader) + sizeof(h);
	body_length = h.payload_length - sizeof(history) - sizeof(operation);
	memcpy(&history, body + body_length, sizeof(history));
	memcpy(&operation, body + body_length + sizeof(history), sizeof(operation));
	if (block == 7) {
		memset(&h.prev_uba, 0, sizeof(h.prev_uba));
		h.flags |= UNDO_REC_FLAG_FIRST_IN_TX;
		if (source_shape == SOURCE_GLOBAL_NONPAGE || source_shape == SOURCE_SHARED) {
			h.flags &= ~UNDO_REC_FLAG_FIRST_IN_TX;
			h.prev_uba = uba_encode(1, 6, 3, 0);
		}
		operation.operation_sequence = 5;
	}
	if (block == 5 && source_shape != SOURCE_GLOBAL_NONPAGE) {
		h.target_offset = 3; /* The newer operation does not create the candidate. */
		history.entries[0].slot_index = 2;
		operation.effect_state = CLUSTER_SAVEPOINT_EFFECT_LIVE;
		operation.operation_sequence = 4;
		h.xid = 128;
		h.tt_wrap_plus1 = 2;
		operation.canonical_top_fxid = U64FromFullTransactionId(
			FullTransactionIdFromEpochAndXid(1, 120));
		operation.operation_fxid = U64FromFullTransactionId(
			FullTransactionIdFromEpochAndXid(1, 128));
		peer.canonical.xid = 120;
		peer.canonical.wrap = 1;
		if (source_fault == 6) history.entries[0].prior.write_scn++;
	}
	if (source_shape == SOURCE_GLOBAL_NONPAGE && (block == 5 || block == 6)) {
		memset(&history.entries[0].prior, 0, sizeof(ClusterItlSlotData));
		history.entries[0].prior.wrap = 1;
		history.entries[0].slot_index = 2;
		operation.operation_sequence = block == 6 ? 4 : 1;
		if (block == 6) {
			h.target_block++;
			h.target_offset = 3;
			history.entries[0].block = h.target_block;
			h.flags &= ~UNDO_REC_FLAG_FIRST_IN_TX;
			h.prev_uba = uba_encode(1, 5, 3, 0);
		}
	}
	if (block == (source_shape == SOURCE_GLOBAL_NONPAGE ? 5 : 6)) {
		source_B_reads++;
		if (source_fault == 1) return CMXH_UNKNOWN;
		if (source_fault == 2) pg_re_throw();
		if (source_fault == 3) operation.operation_fxid += UINT64CONST(1) << 32;
		if (source_fault == 4) operation.effect_state = CLUSTER_SAVEPOINT_EFFECT_DISCARDED;
		if (source_fault == 5) {
			peer.canonical.xid++;
			operation.canonical_top_fxid++;
		}
	}
	memcpy(out->data + sizeof(UndoBlockHeader), &h, sizeof(h));
	memcpy(body + body_length, &history, sizeof(history));
	memcpy(body + body_length + sizeof(history), &operation, sizeof(operation));
	/* FIRST_IN_TX is part of the physical directory envelope as well. */
	{
		UndoSlotDirEntry dir;
		memcpy(&dir, out->data + BLCKSZ - sizeof(dir), sizeof(dir));
		dir.flags = h.flags;
		memcpy(out->data + BLCKSZ - sizeof(dir), &dir, sizeof(dir));
	}
	peer.effect = operation.effect_state;
	peer.top_fxid = FullTransactionIdFromU64(operation.canonical_top_fxid);
	peer.operation_fxid = FullTransactionIdFromU64(operation.operation_fxid);
	peer.operation_sequence = operation.operation_sequence;
	peer.previous = h.prev_uba;
	peer.data_crc = finish_crc(out->data, BLCKSZ);
	if (operation.effect_state != CLUSTER_SAVEPOINT_EFFECT_LIVE) {
		peer.flags &= ~CLUSTER_MX_HISTORY_HAS_GRANT;
		peer.ctrc_grant = 0;
	}
	return cluster_mx_history_peer_encode(&peer, &out->peer) ? CMXH_COMPLETE : CMXH_UNKNOWN;
}

static Snapshot
source_setup(SuccessorSourceShape shape, ClusterTxOutcome outcome,
	ClusterMxHistorySuccessorChallenge *challenge)
{
	Snapshot s = successor_setup(challenge);
	PageHeader page = (PageHeader)BufferBlocks;
	HeapTupleHeader candidate;
	ClusterItlSlotData *roots = ClusterPageGetItlSlots(BufferBlocks);
	page->pd_lower += sizeof(ItemIdData);
	page->pd_upper -= MAXALIGN(SizeofHeapTupleHeader);
	ItemIdSetNormal(PageGetItemId(BufferBlocks, 2), page->pd_upper, SizeofHeapTupleHeader);
	candidate = (HeapTupleHeader)PageGetItem(BufferBlocks, PageGetItemId(BufferBlocks, 2));
	memset(candidate, 0, SizeofHeapTupleHeader);
	candidate->t_hoff = SizeofHeapTupleHeader;
	HeapTupleHeaderSetXmin(candidate, 96);
	ItemPointerSet(&candidate->t_ctid, 3, 2);
	memset(&roots[2], 0, sizeof(roots[2]));
	if (shape == SOURCE_PAGE_PRIOR || shape == SOURCE_SHARED)
		roots[2] = (ClusterItlSlotData){ .xid = 120, .flags = ITL_FLAG_ACTIVE,
			.wrap = 2, .undo_segment_head = uba_encode(1, 5, 3, 0), .write_scn = 82 };
	source_shape = shape; source_fault = source_B_reads = 0;
	finish_successor_order = 2; /* Source block5 contains saved prior B block6. */
	finish_outcome = outcome; finish_distinct_capabilities = true;
	poll_input = source_landing;
	return s;
}
static bool
source_begin(Snapshot s, ClusterMxHistorySuccessorChallenge *challenge,
	ClusterMxHistoryCollector **c)
{
	bool ok = cluster_mx_history_capture_begin_with_successor(&relation, 1, &tuple, s,
		&mxkey, members, 2, &admission, challenge, NULL, c);
	UT_ASSERT(ok); return ok;
}
static ClusterMxHistoryStep
source_ready(ClusterMxHistoryCollector *c)
{
	content_locked = false;
	return cluster_mx_history_wait(c);
}
static ClusterMxHistoryStep
source_finish(ClusterMxHistoryCollector *c, ClusterMxHistorySuccessorChallenge *challenge,
	ClusterMxHistorySuccessorProof *b, uint32 capabilities[2])
{
	content_locked = true;
	return cluster_mx_history_recheck_and_finish_with_successor(c, &relation, 1, &tuple,
		&mxkey, members, 2, finish_keys, finish_proofs, capabilities, 2, challenge, NULL, NULL, b);
}
static void
source_clean(ClusterMxHistoryCollector *c, Snapshot s)
{
	clean(c, s);
	members[0] = (ClusterCurrentMxMemberDesc){ 101, MultiXactStatusForShare, { 0 } };
	members[1].xid = 102;
}
static void
source_check_B(const ClusterMxHistorySuccessorProof *b, const uint32 capabilities[2],
	bool global)
{
	UT_ASSERT_EQ(b->key.locator.xid, 80);
	UT_ASSERT_EQ(b->member.base.member_xid, 96);
	UT_ASSERT_EQ(b->key.target.offnum, 2);
	UT_ASSERT_EQ(b->member.flags, CLUSTER_CURRENT_OPERATION_PROOF_EXACT);
	UT_ASSERT_EQ(b->member.effect_state, CLUSTER_SAVEPOINT_EFFECT_LIVE);
	UT_ASSERT_EQ(b->requester_capability_generation, 55);
	UT_ASSERT_EQ(capabilities[0], 21); UT_ASSERT_EQ(capabilities[1], 34);
	UT_ASSERT(FullTransactionIdEquals(b->member.top_fxid, FullTransactionIdFromEpochAndXid(1,80)));
	UT_ASSERT(FullTransactionIdEquals(b->member.operation_fxid, FullTransactionIdFromEpochAndXid(1,96)));
	UT_ASSERT(cluster_undo_record_uba_equal(b->key.locator.uba, uba_encode(1,global ? 5 : 6,3,0)));
	UT_ASSERT_EQ(b->key.locator.itl_slot_index, 2);
	UT_ASSERT_EQ(source_B_reads, 1); UT_ASSERT_EQ(finish_reads, 4);
	UT_ASSERT_EQ(finish_requests, 4); UT_ASSERT_EQ(updater_fetches, 0);
}

UT_TEST(creator_B_reached_through_overwritten_ITL_owner)
{
	const ClusterTxOutcome states[] = { CLUSTER_TX_IN_PROGRESS, CLUSTER_TX_COMMITTED, CLUSTER_TX_ABORTED };
	for (unsigned i = 0; i < lengthof(states); ++i) {
		ClusterMxHistorySuccessorChallenge challenge; ClusterMxHistoryCollector *c = NULL;
		ClusterMxHistorySuccessorProof b; uint32 capabilities[2];
		Snapshot s = source_setup(SOURCE_PAGE_PRIOR, states[i], &challenge);
		if (source_begin(s, &challenge, &c)) {
			UT_ASSERT_EQ(source_ready(c), CMXH_COMPLETE);
			UT_ASSERT_EQ(source_finish(c, &challenge, &b, capabilities), CMXH_COMPLETE);
			source_check_B(&b, capabilities, false);
			UT_ASSERT_EQ(ClusterPageGetItlSlots(BufferBlocks)[2].xid, 120);
			UT_ASSERT_EQ(remembered, 2);
		}
		source_clean(c, s);
	}
}
UT_TEST(creator_B_reached_through_nonpage_global_predecessor)
{
	ClusterMxHistorySuccessorChallenge challenge; ClusterMxHistoryCollector *c = NULL;
	ClusterMxHistorySuccessorProof b; uint32 capabilities[2];
	Snapshot s = source_setup(SOURCE_GLOBAL_NONPAGE, CLUSTER_TX_COMMITTED, &challenge);
	if (source_begin(s, &challenge, &c)) {
		UT_ASSERT_EQ(source_ready(c), CMXH_COMPLETE);
		UT_ASSERT_EQ(source_finish(c, &challenge, &b, capabilities), CMXH_COMPLETE);
		source_check_B(&b, capabilities, true);
	}
	source_clean(c, s);
}
UT_TEST(shared_B_is_fetched_once_and_every_edge_requalified)
{
	for (unsigned fault = 0; fault < 2; ++fault) {
		ClusterMxHistorySuccessorChallenge challenge; ClusterMxHistoryCollector *c = NULL;
		ClusterMxHistorySuccessorProof b; uint32 capabilities[2];
		Snapshot s = source_setup(SOURCE_SHARED, CLUSTER_TX_COMMITTED, &challenge);
		if (fault) source_fault = 6; /* B was fetched through A before this edge. */
		if (source_begin(s, &challenge, &c)) {
			UT_ASSERT_EQ(source_ready(c), fault ? CMXH_UNKNOWN : CMXH_COMPLETE);
			if (!fault) {
				UT_ASSERT_EQ(source_finish(c, &challenge, &b, capabilities), CMXH_COMPLETE);
				source_check_B(&b, capabilities, false);
			} else {
				UT_ASSERT_EQ(source_finish(c, &challenge, &b, capabilities), CMXH_UNKNOWN);
				successor_unknown(&b); finish_unknown(2);
				UT_ASSERT_EQ(remembered, 1);
			}
			UT_ASSERT_EQ(source_B_reads, 1);
		}
		source_clean(c, s);
	}
}
UT_TEST(complete_graph_without_creator_never_invents_raw_B)
{
	ClusterMxHistorySuccessorChallenge challenge; ClusterMxHistoryCollector *c = NULL;
	ClusterMxHistorySuccessorProof b; uint32 capabilities[2];
	Snapshot s = source_setup(SOURCE_ABSENT, CLUSTER_TX_COMMITTED, &challenge);
	if (source_begin(s, &challenge, &c)) {
		UT_ASSERT_EQ(source_ready(c), CMXH_UNKNOWN);
		UT_ASSERT_EQ(source_finish(c, &challenge, &b, capabilities), CMXH_UNKNOWN);
		successor_unknown(&b); finish_unknown(2);
		UT_ASSERT_EQ(capabilities[0] | capabilities[1], 0);
		UT_ASSERT_EQ(source_B_reads, 0); UT_ASSERT_EQ(finish_reads, 2);
		UT_ASSERT_EQ(updater_fetches, 0); UT_ASSERT_EQ(remembered, 1);
	}
	source_clean(c, s);
}
UT_TEST(unavailable_or_invalid_creator_cancels_complete_A)
{
	for (unsigned fault = 1; fault <= 5; ++fault) {
		ClusterMxHistorySuccessorChallenge challenge; ClusterMxHistoryCollector *c = NULL;
		ClusterMxHistorySuccessorProof b; uint32 capabilities[2];
		volatile bool caught = false;
		Snapshot s = source_setup(SOURCE_PAGE_PRIOR, CLUSTER_TX_COMMITTED, &challenge);
		source_fault = fault;
		if (source_begin(s, &challenge, &c)) {
			PG_TRY(); { UT_ASSERT_EQ(source_ready(c), CMXH_UNKNOWN); }
			PG_CATCH(); { caught = true; }
			PG_END_TRY();
			UT_ASSERT_EQ(caught, fault == 2);
			UT_ASSERT_EQ(source_finish(c, &challenge, &b, capabilities), CMXH_UNKNOWN);
			successor_unknown(&b); finish_unknown(2);
			UT_ASSERT_EQ(capabilities[0] | capabilities[1], 0);
			UT_ASSERT_EQ(source_B_reads, 1); UT_ASSERT_EQ(updater_fetches, 0);
			UT_ASSERT_EQ(remembered, 1);
		}
		source_clean(c, s);
	}
}
UT_TEST(overwritten_owner_relock_drift_discards_A_and_B)
{
	ClusterMxHistorySuccessorChallenge challenge; ClusterMxHistoryCollector *c = NULL;
	ClusterMxHistorySuccessorProof b; uint32 capabilities[2];
	Snapshot s = source_setup(SOURCE_PAGE_PRIOR, CLUSTER_TX_COMMITTED, &challenge);
	if (source_begin(s, &challenge, &c)) {
		UT_ASSERT_EQ(source_ready(c), CMXH_COMPLETE);
		ClusterPageGetItlSlots(BufferBlocks)[2].write_scn++;
		UT_ASSERT_EQ(source_finish(c, &challenge, &b, capabilities), CMXH_REQUALIFY);
		successor_unknown(&b); finish_unknown(2);
		UT_ASSERT_EQ(capabilities[0] | capabilities[1], 0); UT_ASSERT_EQ(remembered, 1);
	}
	source_clean(c, s);
}
UT_TEST(history_source_B_delivery_is_consume_once)
{
	ClusterMxHistorySuccessorChallenge challenge; ClusterMxHistoryCollector *c = NULL;
	ClusterMxHistorySuccessorProof b; uint32 capabilities[2];
	Snapshot s = source_setup(SOURCE_PAGE_PRIOR, CLUSTER_TX_COMMITTED, &challenge);
	if (source_begin(s, &challenge, &c)) {
		UT_ASSERT_EQ(source_ready(c), CMXH_COMPLETE);
		UT_ASSERT_EQ(source_finish(c, &challenge, &b, capabilities), CMXH_COMPLETE);
		UT_ASSERT_EQ(source_finish(c, &challenge, &b, capabilities), CMXH_UNKNOWN);
		successor_unknown(&b); finish_unknown(2);
		UT_ASSERT_EQ(capabilities[0] | capabilities[1], 0);
		UT_ASSERT_EQ(source_B_reads, 1); UT_ASSERT_EQ(remembered, 2);
	}
	source_clean(c, s);
}
int main(void)
{
	history_fixture_init(); UT_PLAN(7);
	UT_RUN(creator_B_reached_through_overwritten_ITL_owner);
	UT_RUN(creator_B_reached_through_nonpage_global_predecessor);
	UT_RUN(shared_B_is_fetched_once_and_every_edge_requalified);
	UT_RUN(complete_graph_without_creator_never_invents_raw_B);
	UT_RUN(unavailable_or_invalid_creator_cancels_complete_A);
	UT_RUN(overwritten_owner_relock_drift_discards_A_and_B);
	UT_RUN(history_source_B_delivery_is_consume_once);
	free(procArray); UT_DONE(); return ut_failed_count ? 1 : 0;
}
