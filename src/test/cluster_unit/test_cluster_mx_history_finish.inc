/* Actual collector, native snapshot/admission and graph/member verification.
 * External request/landing bytes are controlled inputs. Registered GCS landing
 * and native origin TT/CTRC are not witnesses of this fixture. */
#include "cluster/cluster_mx_history_member.h"
#include "port/pg_crc32c.h"
static ClusterSavepointOperationKey finish_keys[256];
static ClusterCurrentMemberOperationProof finish_proofs[256];
static unsigned finish_requests, finish_reads, finish_cancels;
static bool finish_pending, finish_distinct_capabilities;
static TransactionId finish_top;
static ClusterTxOutcome finish_outcome;
static bool finish_all_discarded, finish_successor_in_graph, finish_successor_fxid_drift;
static bool finish_same_page_successor, finish_native_top_records;

static uint32
finish_crc(const void *data, size_t n)
{
	pg_crc32c crc;
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, data, n);
	FIN_CRC32C(crc);
	return crc;
}
static ClusterMxHistoryStep
finish_prepare(const ClusterMxHistoryRequest *input, ClusterMxHistoryRequestWire *out)
{
	ClusterMxHistoryRequest r = *input;
	uint32 segment, block;
	uint16 tt, row;
	if (!uba_decode_record(r.locator.uba, &segment, &block, &tt, &row))
		return CMXH_UNKNOWN;
	r.forward = (GcsBlockForwardPayload){ .request_id = ++finish_requests,
										  .epoch = r.read_epoch,
										  .tag = GcsBlockUndoFetchTagMake(segment, block),
										  .original_requester_node = cluster_node_id,
										  .requester_backend_id = MyBackendId,
										  .master_node = uba_origin_node_id(r.locator.uba),
										  .transition_id = PCM_TRANS_N_TO_S };
	r.requester_capability_generation = finish_distinct_capabilities
		? (block == 7 ? 21 : block == 8 ? 34 : 55) : 12;
	r.origin_capability_generation = 43;
	return cluster_mx_history_request_encode(&r, out) ? CMXH_COMPLETE : CMXH_UNKNOWN;
}
static bool
finish_submit(const ClusterMxHistoryRequestWire *r)
{
	return true;
}
static ClusterMxHistoryStep
finish_landing_input(const ClusterMxHistoryRequestWire *wire, ClusterMxHistoryPeerResult *out)
{
	ClusterMxHistoryRequest r;
	ClusterMxHistoryPeer peer = { 0 };
	UndoBlockHeader bh = { 0 };
	UndoRecordHeader h = { 0 };
	UndoSlotDirEntry dir = { 0 };
	UndoInsertPayload insert = { 64, 0 };
	UndoItlHistoryTrailer history = { 0 };
	UndoOperationTrailer op = { 0 };
	uint32 segment, block;
	uint16 tt, row;
	unsigned index;
	char *body;
	memset(out, 0, sizeof(*out));
	if (finish_pending)
		return CMXH_PENDING;
	if (!cluster_mx_history_request_decode(wire, sizeof(*wire), cluster_node_id, 0, 7, &r)
		|| !uba_decode_record(r.locator.uba, &segment, &block, &tt, &row)
		|| ((block < 7 || block > 8) && !(finish_successor_in_graph && block == 6)))
		return CMXH_UNKNOWN;
	index = block == 6 ? 2 : block - 7;
	finish_reads++;
	bh.magic = PGRAC_UNDO_BLOCK_MAGIC;
	bh.block_version = UNDO_BLOCK_VERSION_1;
	bh.slot_count = 1;
	bh.block_lsn = 123 + index;
	h.record_type = UNDO_RECORD_INSERT;
	h.flags
		= UNDO_REC_FLAG_HAS_ITL_HISTORY | UNDO_REC_FLAG_HAS_OPERATION | UNDO_REC_FLAG_FIRST_IN_TX;
	h.xid = members[index == 2 ? 0 : index].xid;
	if (finish_successor_in_graph && index == 0) {
		h.flags &= ~UNDO_REC_FLAG_FIRST_IN_TX;
		h.prev_uba = uba_encode(1, 6, 3, 0);
	}
	h.origin_node_id = 0;
	h.tt_slot_segment_id = 1;
	h.tt_slot_id = 4;
	h.tt_wrap_plus1 = 1;
	h.write_scn = index == 2 ? 79 : 80 + index;
	h.target_locator = r.target_locator;
	h.target_fork = r.target_fork;
	h.target_block = r.target_block;
	h.target_offset = r.target_offset;
	if (index == 2 && !finish_same_page_successor)
		h.target_block++; /* B may create a successor on another heap page. */
	if (index == 2 && finish_same_page_successor)
		h.target_offset++; /* Same-page B creates the captured successor. */
	history.magic = UNDO_ITL_HISTORY_MAGIC;
	history.version = UNDO_ITL_HISTORY_VERSION;
	history.count = 1;
	history.entries[0] = (UndoItlHistoryEntry){ .block = h.target_block,
												.slot_index = index,
												.after_kind = ITL_FLAG_ACTIVE,
												.after_write_scn = h.write_scn };
	if (finish_native_top_records && index == 2)
		history.entries[0].prior.wrap = 1;
	op.version = UNDO_OPERATION_TRAILER_VERSION;
	op.length = sizeof(op);
	op.effect_state = index || finish_all_discarded ? CLUSTER_SAVEPOINT_EFFECT_DISCARDED
													: CLUSTER_SAVEPOINT_EFFECT_LIVE;
	op.canonical_top_fxid
		= U64FromFullTransactionId(FullTransactionIdFromEpochAndXid(1, finish_top));
	op.operation_fxid = U64FromFullTransactionId(FullTransactionIdFromEpochAndXid(1, h.xid));
	op.operation_sequence = finish_successor_in_graph ? (index == 2 ? 1 : index + 2) : index + 1;
	if (index == 2 && finish_successor_fxid_drift)
		op.operation_fxid += UINT64CONST(1) << 32;
	op.command_id = 4;
	op.subid = 3;
	if (finish_native_top_records && index != 1) {
		h.flags &= ~UNDO_REC_FLAG_HAS_OPERATION;
		op.effect_state = CLUSTER_SAVEPOINT_EFFECT_LIVE;
		op.operation_sequence = 0;
	}
	body = (char *)out->data + sizeof(bh) + sizeof(h);
	memcpy(body, &insert, sizeof(insert));
	memcpy(body + sizeof(insert), &history, sizeof(history));
	memcpy(body + sizeof(insert) + sizeof(history), &op, sizeof(op));
	h.payload_length = sizeof(insert) + sizeof(history) + sizeof(op);
	if (!(h.flags & UNDO_REC_FLAG_HAS_OPERATION))
		h.payload_length -= sizeof(op);
	dir.record_offset = sizeof(bh);
	dir.record_length = sizeof(h) + h.payload_length;
	dir.record_type = h.record_type;
	dir.flags = h.flags;
	bh.free_offset = sizeof(bh) + dir.record_length;
	memcpy(out->data, &bh, sizeof(bh));
	memcpy(out->data + sizeof(bh), &h, sizeof(h));
	memcpy(out->data + BLCKSZ - sizeof(dir), &dir, sizeof(dir));
	peer.flags = CLUSTER_MX_HISTORY_ORDERED;
	peer.record_class = CMXH_OPERATION;
	if (!(h.flags & UNDO_REC_FLAG_HAS_OPERATION))
		peer.record_class = CMXH_V1_TOP;
	peer.outcome = finish_outcome;
	peer.effect = op.effect_state;
	peer.proof_kind = CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG;
	peer.canonical = (ClusterTTSlotPhysicalLocator){ 1, finish_top, 3, 0, 0 };
	peer.top_fxid = FullTransactionIdFromU64(op.canonical_top_fxid);
	peer.operation_fxid = FullTransactionIdFromU64(op.operation_fxid);
	peer.operation_sequence = op.operation_sequence;
	peer.previous = h.prev_uba;
	peer.record = r.locator.uba;
	peer.data_crc = finish_crc(out->data, BLCKSZ);
	peer.data_lsn = bh.block_lsn;
	peer.commit_scn = finish_outcome == CLUSTER_TX_COMMITTED ? 90 : InvalidScn;
	if (finish_outcome == CLUSTER_TX_IN_PROGRESS
		&& op.effect_state == CLUSTER_SAVEPOINT_EFFECT_LIVE) {
		peer.flags |= CLUSTER_MX_HISTORY_HAS_GRANT;
		peer.ctrc_grant = 47;
	}
	peer.formation_epoch = 7;
	peer.admission_generation = r.admission_generation;
	if (!cluster_mx_history_peer_encode(&peer, &out->peer))
		return CMXH_UNKNOWN;
	out->request = *wire;
	ClusterR4WireWriteU64(out->auth + 8, 101);
	out->admitted = 1; /* Simulated external landing boundary, not a verifier. */
	return CMXH_COMPLETE;
}
static void
finish_cancel_input(const ClusterMxHistoryRequestWire *r)
{
	/* The original request is retired before dropping native retention. */
	UT_ASSERT_EQ(remembered, 2);
	finish_cancels++;
}
static void
finish_sources(void)
{
	ClusterItlSlotData *roots = ClusterPageGetItlSlots(BufferBlocks);
	memset(roots, 0, CLUSTER_ITL_SPECIAL_SIZE);
	for (unsigned i = 0; i < 2; ++i)
		roots[i] = (ClusterItlSlotData){ .xid = 100,
										 .flags = ITL_FLAG_ACTIVE,
										 .undo_segment_head = uba_encode(1, 7 + i, 3, 0),
										 .write_scn = 80 + i };
	prepare_input = finish_prepare;
	submit_input = finish_submit;
	poll_input = finish_landing_input;
	cancel_input = finish_cancel_input;
	finish_requests = finish_reads = finish_cancels = 0;
	finish_pending = finish_distinct_capabilities = false;
	finish_top = 100;
	finish_outcome = CLUSTER_TX_COMMITTED;
	finish_all_discarded = false;
	finish_successor_in_graph = finish_successor_fxid_drift = false;
	finish_same_page_successor = finish_native_top_records = false;
	memset(finish_keys, 0xa5, sizeof(finish_keys));
	memset(finish_proofs, 0xa5, sizeof(finish_proofs));
}
static ClusterMxHistoryStep
finish_ready(ClusterMxHistoryCollector *c, bool graph_only)
{
	content_locked = false;
	for (unsigned i = 0; i < 128; ++i) {
		ClusterMxHistoryStep step
			= graph_only ? cluster_mx_history_collect_poll(c) : cluster_mx_history_poll(c);
		if (step != CMXH_PENDING)
			return step;
	}
	UT_ASSERT(false);
	return CMXH_UNKNOWN;
}
static ClusterMxHistoryStep
finish_deliver(ClusterMxHistoryCollector *c, uint16 capacity)
{
	content_locked = true;
	return cluster_mx_history_recheck_and_finish(c, &relation, 1, &tuple, &mxkey, members, 2,
												 finish_keys, finish_proofs, capacity);
}
static void
finish_unknown(uint16 n)
{
	ClusterSavepointOperationKey zero = { 0 };
	for (uint16 i = 0; i < n; ++i) {
		UT_ASSERT_EQ(finish_proofs[i].base.state, CCM_UNKNOWN);
		UT_ASSERT_EQ(finish_proofs[i].flags, 0);
		UT_ASSERT_EQ(memcmp(&finish_keys[i], &zero, sizeof(zero)), 0);
	}
}
static void
finish_complete_set_once_and_retention(void)
{
	Snapshot s = setup();
	ClusterMxHistoryCollector *c = NULL;
	finish_sources();
	UT_ASSERT(begin(s, &c));
	UT_ASSERT_EQ(finish_ready(c, false), CMXH_COMPLETE);
	{
		unsigned allocations = allocation_calls;
		for (unsigned i = 0; i < 3; ++i)
			UT_ASSERT_EQ(cluster_mx_history_poll(c), CMXH_COMPLETE);
		UT_ASSERT_EQ(allocation_calls, allocations);
	}
	UT_ASSERT_EQ(finish_deliver(c, 256), CMXH_COMPLETE);
	UT_ASSERT_EQ(finish_proofs[0].base.member_xid, 101);
	UT_ASSERT_EQ(finish_proofs[1].base.member_xid, 102);
	UT_ASSERT_EQ(finish_proofs[1].base.member_ordinal, 1);
	UT_ASSERT_EQ(finish_proofs[0].effect_state, CLUSTER_SAVEPOINT_EFFECT_LIVE);
	UT_ASSERT_EQ(finish_proofs[1].effect_state, CLUSTER_SAVEPOINT_EFFECT_DISCARDED);
	UT_ASSERT_EQ(finish_proofs[1].base.state, CCM_COMMITTED);
	UT_ASSERT_EQ(finish_proofs[2].base.state, CCM_UNKNOWN);
	UT_ASSERT_EQ(finish_requests, 2);
	UT_ASSERT_EQ(finish_reads, 2);
	UT_ASSERT_EQ(remembered, 2);
	UT_ASSERT_EQ(finish_deliver(c, 2), CMXH_UNKNOWN);
	finish_unknown(2);
	UT_ASSERT_EQ(remembered, 2);
	content_locked = false;
	UT_ASSERT_EQ(cluster_mx_history_poll(c), CMXH_UNKNOWN);
	UT_ASSERT_EQ(remembered, 2);
	UT_ASSERT_EQ(finish_reads, 2);
	{
		const ClusterMxHistoryGraph *g = (void *)1;
		content_locked = true;
		UT_ASSERT(!cluster_mx_history_collect_recheck_graph(c, &relation, 1, &tuple, &mxkey,
															members, 2, &g));
		UT_ASSERT(g == NULL);
		UT_ASSERT_EQ(remembered, 2);
	}
	clean(c, s);
}
static void
finish_sole_snapshot_reference_lasts_through_consume(void)
{
	Snapshot s = setup();
	ClusterMxHistoryCollector *c = NULL;
	finish_sources();
	UT_ASSERT(begin(s, &c));
	UnregisterSnapshot(s);
	UT_ASSERT_EQ(remembered, 1);
	UT_ASSERT_EQ(finish_ready(c, false), CMXH_COMPLETE);
	UT_ASSERT_EQ(finish_deliver(c, 2), CMXH_COMPLETE);
	UT_ASSERT_EQ(remembered, 1);
	UT_ASSERT_EQ(pg_atomic_read_u64(&proc.cluster_read_scn_atomic), 100);
	clean(c, NULL);
}
static void
finish_full_heap_recheck_discards_results(void)
{
	for (unsigned fault = 0; fault < 8; ++fault) {
		Snapshot s = setup();
		ClusterMxHistoryCollector *c = NULL;
		ClusterCurrentMxMemberDesc changed[2];
		finish_sources();
		UT_ASSERT(begin(s, &c));
		UT_ASSERT_EQ(finish_ready(c, false), CMXH_COMPLETE);
		memcpy(changed, members, sizeof(changed));
		switch (fault) {
		case 0:
			tuple.t_data->t_infomask2++;
			break;
		case 1:
			PageXLogRecPtrSet(((PageHeader)BufferBlocks)->pd_lsn, UINT64CONST(78));
			break;
		case 2:
			ClusterPageGetItlSlots(BufferBlocks)[0].write_scn++;
			break;
		case 3:
			pcm_source.generation++;
			break;
		case 4:
			changed[0].member_status = MultiXactStatusForKeyShare;
			break;
		case 5:
			relation.rd_id++;
			break;
		case 6:
			pinned = false;
			break;
		case 7:
			CurrentResourceOwner = (ResourceOwner)2;
			break;
		}
		content_locked = true;
		UT_ASSERT_EQ(cluster_mx_history_recheck_and_finish(c, &relation, 1, &tuple, &mxkey, changed,
														   2, finish_keys, finish_proofs, 2),
					 CMXH_REQUALIFY);
		finish_unknown(2);
		UT_ASSERT_EQ(remembered, 1);
		UT_ASSERT_EQ(finish_reads, 2);
		clean(c, s);
	}
}
static void
finish_before_ready_is_pending(void)
{
	Snapshot s = setup();
	ClusterMxHistoryCollector *c = NULL;
	finish_sources();
	UT_ASSERT(begin(s, &c));
	UT_ASSERT_EQ(finish_deliver(c, 2), CMXH_PENDING);
	finish_unknown(2);
	UT_ASSERT_EQ(remembered, 2);
	UT_ASSERT_EQ(finish_reads, 0);
	UT_ASSERT_EQ(finish_ready(c, false), CMXH_COMPLETE);
	UT_ASSERT_EQ(finish_deliver(c, 2), CMXH_COMPLETE);
	clean(c, s);
}
static void
member_poll_never_waits_with_heap_lock_or_critical_section(void)
{
	for (unsigned fault = 0; fault < 3; ++fault) {
		Snapshot s = setup();
		ClusterMxHistoryCollector *c = NULL;
		finish_sources();
		UT_ASSERT(begin(s, &c));
		content_locked = fault == 0;
		if (fault == 1)
			pinned = false;
		if (fault == 2)
			CritSectionCount = 1;
		UT_ASSERT_EQ(cluster_mx_history_poll(c), CMXH_UNKNOWN);
		UT_ASSERT_EQ(finish_requests, 0);
		UT_ASSERT_EQ(remembered, 1);
		CritSectionCount = 0;
		clean(c, s);
	}
}
static void
member_ready_scope_drift_cancels(void)
{
	Snapshot s = setup();
	ClusterMxHistoryCollector *c = NULL;
	finish_sources();
	UT_ASSERT(begin(s, &c));
	UT_ASSERT_EQ(finish_ready(c, false), CMXH_COMPLETE);
	pg_atomic_write_u32(&history_gate.transition_closed, 1);
	UT_ASSERT_EQ(cluster_mx_history_poll(c), CMXH_UNKNOWN);
	UT_ASSERT_EQ(remembered, 1);
	UT_ASSERT_EQ(finish_deliver(c, 2), CMXH_UNKNOWN);
	finish_unknown(2);
	clean(c, s);
}
static void
member_result_allocation_failure_is_whole_unknown(void)
{
	for (unsigned fail = 1; fail <= 2; ++fail) {
		Snapshot s = setup();
		ClusterMxHistoryCollector *c = NULL;
		finish_sources();
		UT_ASSERT(begin(s, &c));
		UT_ASSERT_EQ(finish_ready(c, true), CMXH_COMPLETE);
		fail_allocation = allocation_calls + fail;
		UT_ASSERT_EQ(cluster_mx_history_poll(c), CMXH_UNKNOWN);
		UT_ASSERT_EQ(remembered, 1);
		UT_ASSERT_EQ(finish_deliver(c, 2), CMXH_UNKNOWN);
		finish_unknown(2);
		clean(c, s);
	}
}
static void
member_missing_support_is_not_complete(void)
{
	Snapshot s = setup();
	ClusterMxHistoryCollector *c = NULL;
	finish_sources();
	memset(&ClusterPageGetItlSlots(BufferBlocks)[1], 0, sizeof(ClusterItlSlotData));
	UT_ASSERT(begin(s, &c));
	UT_ASSERT_EQ(finish_ready(c, false), CMXH_UNKNOWN);
	UT_ASSERT_EQ(remembered, 1);
	UT_ASSERT_EQ(finish_deliver(c, 2), CMXH_UNKNOWN);
	finish_unknown(2);
	clean(c, s);
}
static void
member_ready_owner_cleanup(void)
{
	Snapshot s = setup();
	ClusterMxHistoryCollector *c = NULL;
	finish_sources();
	UT_ASSERT(begin(s, &c));
	UT_ASSERT_EQ(finish_ready(c, false), CMXH_COMPLETE);
	cleanup_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, false, cleanup_argument);
	UT_ASSERT_EQ(remembered, 1);
	UT_ASSERT_EQ(finish_deliver(c, 2), CMXH_UNKNOWN);
	finish_unknown(2);
	clean(c, s);
}
static void
member_transport_pending_and_cancel_order(void)
{
	Snapshot s = setup();
	ClusterMxHistoryCollector *c = NULL;
	finish_sources();
	finish_pending = true;
	UT_ASSERT(begin(s, &c));
	content_locked = false;
	for (unsigned i = 0; i < 100; ++i)
		UT_ASSERT_EQ(cluster_mx_history_poll(c), CMXH_PENDING);
	UT_ASSERT_EQ(finish_requests, 1);
	UT_ASSERT_EQ(finish_reads, 0);
	UT_ASSERT_EQ(remembered, 2);
	cleanup_callback(RESOURCE_RELEASE_BEFORE_LOCKS, false, false, cleanup_argument);
	UT_ASSERT_EQ(finish_cancels, 1);
	UT_ASSERT_EQ(remembered, 1);
	clean(c, s);
}
static void
finish_capacity_boundaries(void)
{
	for (unsigned fault = 0; fault < 4; ++fault) {
		Snapshot s = setup();
		ClusterMxHistoryCollector *c = NULL;
		ClusterCurrentMemberOperationProof canary;
		uint16 capacity = fault == 0 ? 0 : fault == 1 ? 1 : fault == 2 ? 257 : 2;
		finish_sources();
		UT_ASSERT(begin(s, &c));
		UT_ASSERT_EQ(finish_ready(c, false), CMXH_COMPLETE);
		canary = finish_proofs[0];
		content_locked = true;
		UT_ASSERT_EQ(cluster_mx_history_recheck_and_finish(
						 c, &relation, 1, &tuple, &mxkey, members, 2, finish_keys,
						 fault == 3 ? NULL : finish_proofs, capacity),
					 CMXH_UNKNOWN);
		if (capacity == 0 || capacity > 256)
			UT_ASSERT_EQ(memcmp(&canary, &finish_proofs[0], sizeof(canary)), 0);
		else if (fault != 3)
			finish_unknown(capacity);
		UT_ASSERT_EQ(remembered, 1);
		clean(c, s);
	}
}
static void
finish_read_image_requires_new_bracket(void)
{
	for (unsigned fresh = 0; fresh < 2; ++fresh) {
		Snapshot s = setup();
		ClusterMxHistoryCollector *c = NULL;
		finish_sources();
		exclusive = false;
		pcm_source.pcm_state = PCM_STATE_READ_IMAGE;
		pcm_source.buffer_type = BUF_TYPE_CURRENT;
		UT_ASSERT(begin(s, &c));
		UT_ASSERT_EQ(finish_ready(c, false), CMXH_COMPLETE);
		if (fresh) {
			pcm_source.generation++;
			pcm_source.reservation_token++;
		}
		UT_ASSERT_EQ(finish_deliver(c, 2), fresh ? CMXH_COMPLETE : CMXH_REQUALIFY);
		if (!fresh)
			finish_unknown(2);
		clean(c, s);
	}
}


static ClusterCurrentUpdaterChallenge
updater_challenge(unsigned block)
{
	ClusterCurrentUpdaterChallenge c = { 0 };
	c.updater_xid = members[0].xid;
	c.candidate_next_xmin_alias = (ClusterCurrentMxSuccessorAlias){ .origin_node_id = 0,
																	.undo_record_segment_id = 1,
																	.tt_slot_id = 4,
																	.cluster_epoch = 7,
																	.local_xid = c.updater_xid };
	c.candidate_next_xmin_locator = (ClusterTxLocator){ .uba = uba_encode(1, block, 3, 0),
														.xid = c.updater_xid,
														.tt_wrap = TT_WRAP_INVALID,
														.itl_kind = ITL_FLAG_ACTIVE,
														.itl_slot_index = 0 };
	return c;
}
static bool
updater_begin(Snapshot s, const ClusterCurrentUpdaterChallenge *challenge,
			  ClusterMxHistoryCollector **c)
{
	members[0].member_status = MultiXactStatusNoKeyUpdate;
	return cluster_mx_history_capture_begin_with_updater(&relation, 1, &tuple, s, &mxkey, members,
														 2, &admission, challenge, c);
}
static ClusterMxHistoryStep
updater_deliver(ClusterMxHistoryCollector *c, const ClusterCurrentUpdaterChallenge *challenge,
				ClusterCurrentUpdaterProof *proof)
{
	content_locked = true;
	return cluster_mx_history_recheck_and_finish_with_updater(c, &relation, 1, &tuple, &mxkey,
															  members, 2, finish_keys,
															  finish_proofs, 2, challenge, proof);
}
static void
updater_graph_proof_reused_and_delivered_once(void)
{
	Snapshot s = setup();
	ClusterMxHistoryCollector *c = NULL;
	ClusterCurrentUpdaterChallenge challenge = updater_challenge(7);
	ClusterCurrentUpdaterProof proof;
	finish_sources();
	UT_ASSERT(updater_begin(s, &challenge, &c));
	UT_ASSERT_EQ(finish_ready(c, false), CMXH_COMPLETE);
	UT_ASSERT_EQ(updater_deliver(c, &challenge, &proof), CMXH_COMPLETE);
	UT_ASSERT_EQ(proof.verdict, CUCP_MATCH);
	UT_ASSERT_EQ(proof.candidate_next_xmin_locator.tt_wrap, 0);
	UT_ASSERT_EQ(proof.updater_xid, 101);
	UT_ASSERT_EQ(finish_proofs[0].base.member_xid, 101);
	UT_ASSERT_EQ(finish_reads, 2);
	UT_ASSERT_EQ(remembered, 2);
	UT_ASSERT_EQ(updater_deliver(c, &challenge, &proof), CMXH_UNKNOWN);
	UT_ASSERT_EQ(proof.verdict, CUCP_UNKNOWN);
	finish_unknown(2);
	clean(c, s);
	members[0].member_status = MultiXactStatusForShare;
}
static void
updater_legacy_finish_cannot_drop_required_B(void)
{
	Snapshot s = setup();
	ClusterMxHistoryCollector *c = NULL;
	ClusterCurrentUpdaterChallenge challenge = updater_challenge(7);
	finish_sources();
	UT_ASSERT(updater_begin(s, &challenge, &c));
	UT_ASSERT_EQ(finish_ready(c, false), CMXH_COMPLETE);
	UT_ASSERT_EQ(finish_deliver(c, 2), CMXH_UNKNOWN);
	finish_unknown(2);
	UT_ASSERT_EQ(remembered, 1);
	clean(c, s);
	members[0].member_status = MultiXactStatusForShare;
}
static void
updater_finish_rechecks_challenge(void)
{
	Snapshot s = setup();
	ClusterMxHistoryCollector *c = NULL;
	ClusterCurrentUpdaterChallenge challenge = updater_challenge(7), fresh;
	ClusterCurrentUpdaterProof proof;
	finish_sources();
	UT_ASSERT(updater_begin(s, &challenge, &c));
	UT_ASSERT_EQ(finish_ready(c, false), CMXH_COMPLETE);
	fresh = challenge;
	fresh.candidate_next_xmin_locator.uba = uba_encode(1, 9, 3, 0);
	UT_ASSERT_EQ(updater_deliver(c, &fresh, &proof), CMXH_REQUALIFY);
	UT_ASSERT_EQ(proof.verdict, CUCP_UNKNOWN);
	finish_unknown(2);
	UT_ASSERT_EQ(remembered, 1);
	clean(c, s);
	members[0].member_status = MultiXactStatusForShare;
}
static void
updater_found_wrong_writer_is_unknown_without_retry(void)
{
	Snapshot s = setup();
	ClusterMxHistoryCollector *c = NULL;
	ClusterCurrentUpdaterChallenge challenge = updater_challenge(8);
	ClusterCurrentUpdaterProof proof;
	finish_sources();
	UT_ASSERT(updater_begin(s, &challenge, &c));
	UT_ASSERT_EQ(finish_ready(c, false), CMXH_UNKNOWN);
	UT_ASSERT_EQ(updater_deliver(c, &challenge, &proof), CMXH_UNKNOWN);
	UT_ASSERT_EQ(proof.verdict, CUCP_UNKNOWN);
	UT_ASSERT_EQ(remembered, 1);
	clean(c, s);
	members[0].member_status = MultiXactStatusForShare;
}


/* Controlled external B landing; real request and reply codecs remain in
 * the path. This is not an origin TT/CTRC or transport witness. */
static unsigned updater_fetches, updater_fault;
#ifndef HISTORY_ROUNDTRIP_FIXTURE
void
cluster_xid_stripe_lazy_latch(void)
{
	abort();
}
#endif
ClusterMxResolveResult
cluster_gcs_current_mx_operation_proof_fetch_and_wait(int32 origin,
													  ClusterCurrentMxOperationForwardV6 *request,
													  ClusterCurrentMemberOperationProof *proofs,
													  uint16 capacity, uint16 *count,
													  ClusterCurrentUpdaterProof *updater,
													  uint32 *capability, TimestampTz deadline)
{
	ClusterCurrentMxOperationForwardV6 decoded;
	ClusterCurrentMxOperationAsk ask;
	ClusterCurrentMxUpdaterChallengeWire challenge;
	ClusterCurrentMxOperationReplyPageV6 page;
	ClusterCurrentMemberOperationProof peer = { 0 };
	ClusterCurrentUpdaterProof b = { 0 };
	ClusterMxResolveResult result;
	updater_fetches++;
	UT_ASSERT(!content_locked && CritSectionCount == 0);
	UT_ASSERT_EQ(remembered, 2);
	UT_ASSERT_EQ(finish_reads, 2); /* A already complete; never repeat it. */
	if (!cluster_multixact_operation_wire_validate_forward(request, sizeof(*request),
														   cluster_node_id, origin, 7, &decoded)
		|| !cluster_multixact_operation_wire_get_ask(&decoded, 0, &ask))
		return CMX_RESOLVE_UNKNOWN;
	UT_ASSERT(!ask.exact);
	UT_ASSERT_EQ(decoded.prefix.body_kind, CLUSTER_CURRENT_MX_PROOF_BODY_UPDATER_CHALLENGE);
	memcpy(&challenge, decoded.body, sizeof(challenge));
	peer.format_version = CLUSTER_CURRENT_OPERATION_PROOF_VERSION;
	peer.base.member_xid = challenge.updater_xid;
	peer.base.member_status = challenge.member_status;
	peer.base.member_ordinal = challenge.member_ordinal;
	peer.base.state = CCM_COMMITTED;
	peer.base.commit_scn = 90;
	b.mxkey = decoded.prefix.mxkey;
	b.candidate_next_xmin_alias = challenge.candidate_next_xmin_alias;
	b.candidate_next_xmin_locator = challenge.candidate_next_xmin_locator;
	b.candidate_next_xmin_locator.tt_wrap = 0;
	b.updater_xid = challenge.updater_xid;
	b.member_ordinal = challenge.member_ordinal;
	b.verdict = updater_fault == 2 ? CUCP_MISMATCH : CUCP_MATCH;
	if (updater_fault == 1)
		return CMX_RESOLVE_UNKNOWN;
	if (updater_fault == 3)
		pg_atomic_write_u32(&history_gate.transition_closed, 1);
	if (updater_fault == 4)
		pg_re_throw();
	if (!cluster_multixact_operation_wire_encode_reply(&decoded, origin, 7, CMX_RESOLVE_OK, &peer,
													   1, &b, 12, &page)
		|| !cluster_multixact_operation_wire_validate_reply(&page, sizeof(page), origin, 7, request,
															&result, proofs, capacity, count,
															updater, capability))
		return CMX_RESOLVE_UNKNOWN;
	if (updater_fault == 5)
		updater->updater_xid++;
	if (updater_fault == 6)
		*capability = 0;
	return result;
}
static Snapshot
updater_remote_setup(ClusterCurrentUpdaterChallenge *challenge)
{
	Snapshot s = setup();
	finish_sources();
	vacuum_next_xid = U64FromFullTransactionId(FullTransactionIdFromEpochAndXid(1, 256));
	cluster_xid_stripe_latch_runtime(true, cluster_node_id, FullTransactionIdFromEpochAndXid(1, 3));
	members[0].xid = 96;
	members[1].xid = 112;
	finish_top = 80;
	for (unsigned i = 0; i < 2; ++i)
		ClusterPageGetItlSlots(BufferBlocks)[i].xid = finish_top;
	*challenge = updater_challenge(9); /* B is outside the retained A graph. */
	updater_fetches = updater_fault = 0;
	return s;
}
static void
updater_missing_B_uses_original_provenance_once(void)
{
	ClusterCurrentUpdaterChallenge challenge;
	Snapshot s = updater_remote_setup(&challenge);
	ClusterMxHistoryCollector *c = NULL;
	ClusterCurrentUpdaterProof proof;
	UT_ASSERT(updater_begin(s, &challenge, &c));
	UT_ASSERT_EQ(finish_ready(c, false), CMXH_COMPLETE);
	UT_ASSERT_EQ(cluster_mx_history_poll(c), CMXH_COMPLETE);
	UT_ASSERT_EQ(updater_fetches, 1);
	UT_ASSERT_EQ(updater_deliver(c, &challenge, &proof), CMXH_COMPLETE);
	UT_ASSERT_EQ(proof.verdict, CUCP_MATCH);
	UT_ASSERT_EQ(finish_reads, 2);
	clean(c, s);
	members[0] = (ClusterCurrentMxMemberDesc){ 101, MultiXactStatusForShare, { 0 } };
	members[1].xid = 102;
}
static void
updater_missing_B_failure_cancels_whole_attempt(void)
{
	for (unsigned fault = 1; fault <= 6; ++fault) {
		ClusterCurrentUpdaterChallenge challenge;
		Snapshot s = updater_remote_setup(&challenge);
		ClusterMxHistoryCollector *c = NULL;
		ClusterCurrentUpdaterProof proof;
		volatile bool caught = false;
		updater_fault = fault;
		UT_ASSERT(updater_begin(s, &challenge, &c));
		PG_TRY();
		{
			UT_ASSERT_EQ(finish_ready(c, false), CMXH_UNKNOWN);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT_EQ(caught, fault == 4);
		UT_ASSERT_EQ(updater_fetches, 1);
		UT_ASSERT_EQ(updater_deliver(c, &challenge, &proof), CMXH_UNKNOWN);
		UT_ASSERT_EQ(proof.verdict, CUCP_UNKNOWN);
		finish_unknown(2);
		UT_ASSERT_EQ(remembered, 1);
		clean(c, s);
	}
	members[0] = (ClusterCurrentMxMemberDesc){ 101, MultiXactStatusForShare, { 0 } };
	members[1].xid = 102;
}

static void
updater_reuses_distinct_B_page_and_checks_full_xid(void)
{
	for (unsigned drift = 0; drift < 2; ++drift) {
		Snapshot s = setup();
		ClusterMxHistoryCollector *c = NULL;
		ClusterCurrentUpdaterChallenge challenge = updater_challenge(6);
		ClusterCurrentUpdaterProof proof;
		finish_sources();
		updater_fetches = 0;
		finish_successor_in_graph = true;
		finish_successor_fxid_drift = drift;
		UT_ASSERT(updater_begin(s, &challenge, &c));
		UT_ASSERT_EQ(finish_ready(c, false), drift ? CMXH_UNKNOWN : CMXH_COMPLETE);
		UT_ASSERT_EQ(updater_deliver(c, &challenge, &proof), drift ? CMXH_UNKNOWN : CMXH_COMPLETE);
		UT_ASSERT_EQ(proof.verdict, drift ? CUCP_UNKNOWN : CUCP_MATCH);
		UT_ASSERT_EQ(finish_reads, 3);
		UT_ASSERT_EQ(updater_fetches, 0);
		UT_ASSERT_EQ(remembered, drift ? 1 : 2);
		clean(c, s);
	}
	members[0].member_status = MultiXactStatusForShare;
}
static void
updater_graph_reuse_preserves_A_outcome_and_effect(void)
{
	ClusterTxOutcome outcomes[]
		= { CLUSTER_TX_IN_PROGRESS, CLUSTER_TX_COMMITTED, CLUSTER_TX_ABORTED };
	for (unsigned state = 0; state < lengthof(outcomes); ++state)
		for (unsigned discarded = 0; discarded < 2; ++discarded) {
			Snapshot s = setup();
			ClusterMxHistoryCollector *c = NULL;
			ClusterCurrentUpdaterChallenge challenge = updater_challenge(7);
			ClusterCurrentUpdaterProof proof;
			finish_sources();
			finish_outcome = outcomes[state];
			finish_all_discarded = discarded;
			UT_ASSERT(updater_begin(s, &challenge, &c));
			UT_ASSERT_EQ(finish_ready(c, false), CMXH_COMPLETE);
			UT_ASSERT_EQ(updater_deliver(c, &challenge, &proof), CMXH_COMPLETE);
			UT_ASSERT_EQ(proof.verdict, CUCP_MATCH);
			UT_ASSERT_EQ(finish_proofs[0].effect_state, discarded
															? CLUSTER_SAVEPOINT_EFFECT_DISCARDED
															: CLUSTER_SAVEPOINT_EFFECT_LIVE);
			UT_ASSERT_EQ(finish_proofs[0].base.state, state == 0   ? CCM_ACTIVE
													  : state == 1 ? CCM_COMMITTED
																   : CCM_ABORTED);
			UT_ASSERT_EQ(ClusterCurrentMemberProofGetCtrcGrant(&finish_proofs[0].base),
						 state == 0 && !discarded ? 47 : 0);
			clean(c, s);
		}
	members[0].member_status = MultiXactStatusForShare;
}
static void
updater_bad_capture_and_unfinished_attempt_refused(void)
{
	for (unsigned fault = 0; fault < 7; ++fault) {
		Snapshot s = setup();
		ClusterMxHistoryCollector *c = NULL;
		ClusterCurrentUpdaterChallenge challenge = updater_challenge(7);
		ClusterCurrentUpdaterProof proof;
		finish_sources();
		members[0].member_status = MultiXactStatusNoKeyUpdate;
		switch (fault) {
		case 0:
			challenge.reserved16++;
			break;
		case 1:
			challenge.member_ordinal = 2;
			break;
		case 2:
			challenge.updater_xid++;
			break;
		case 3:
			challenge.candidate_next_xmin_alias.reserved32++;
			break;
		case 4:
			challenge.candidate_next_xmin_locator.tt_wrap = 0;
			break;
		case 5:
			members[0].member_status = MultiXactStatusForShare;
			break;
		}
		UT_ASSERT_EQ(cluster_mx_history_capture_begin_with_updater(
						 &relation, 1, &tuple, s, &mxkey, members, 2, &admission, &challenge, &c),
					 fault == 6);
		if (fault == 6) {
			UT_ASSERT_EQ(finish_ready(c, true), CMXH_COMPLETE);
			UT_ASSERT_EQ(updater_deliver(c, &challenge, &proof), CMXH_PENDING);
			UT_ASSERT_EQ(proof.verdict, CUCP_UNKNOWN);
			finish_unknown(2);
		} else {
			UT_ASSERT(c == NULL);
			UT_ASSERT_EQ(remembered, 1);
		}
		clean(c, s);
	}
	members[0].member_status = MultiXactStatusForShare;
}
