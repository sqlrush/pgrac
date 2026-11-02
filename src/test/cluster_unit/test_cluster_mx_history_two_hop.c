/* Continuous controlled composition, not native recursion or SQL. The real
 * collector/graph, writer, receipts, transition and ITL publisher run. Origin
 * bytes, PCM, descriptor storage, independent undo receipt and WAL return are
 * external controlled leaves. No scope/page/source reset between the hops. */
#define errmsg_internal two_unused_errmsg
#define errdetail_internal two_unused_detail
#define HISTORY_TWO_HOP_FIXTURE
#include "test_cluster_mx_history_writer_publication.c"
#undef main
#undef HISTORY_TWO_HOP_FIXTURE
#undef errmsg_internal
#undef errdetail_internal
int errmsg_internal(const char *fmt, ...);
int errdetail_internal(const char *fmt, ...);
int
errmsg_internal(const char *fmt, ...)
{
	va_list args;
	va_start(args, fmt); printf("# ERROR "); vprintf(fmt, args); puts(""); va_end(args);
	return 0;
}
int
errdetail_internal(const char *fmt, ...)
{
	va_list args;
	va_start(args, fmt); printf("# DETAIL "); vprintf(fmt, args); puts(""); va_end(args);
	return 0;
}

typedef struct TwoHopRecord {
	UBA uba;
	ClusterMxHistoryPeer peer;
	PGAlignedBlock bytes;
	unsigned reads;
} TwoHopRecord;
static TwoHopRecord two_records[7];
static unsigned two_missing, two_discard;
static bool two_extra_delta;
static ClusterMxHistoryStep
two_prepare(const ClusterMxHistoryRequest *input, ClusterMxHistoryRequestWire *wire)
{
	ClusterMxHistoryRequest r;
	ClusterMxHistoryStep step = finish_prepare(input, wire);
	if (step != CMXH_COMPLETE
		|| !cluster_mx_history_request_decode(wire, sizeof(*wire), cluster_node_id,
			uba_origin_node_id(input->locator.uba), 7, &r)) return CMXH_UNKNOWN;
	/* The selected second A has its own original capability, independent of B. */
	if (cluster_undo_record_uba_equal(input->locator.uba, uba_encode(1, 13, 3, 0)))
		r.requester_capability_generation = 21;
	return cluster_mx_history_request_encode(&r, wire) ? CMXH_COMPLETE : CMXH_UNKNOWN;
}

static void
two_record(unsigned at, uint32 segment, uint32 block, uint16 offnum,
	unsigned slot, TransactionId child, SCN scn, bool lock, UBA previous,
	const ClusterItlSlotData *prior)
{
	static const uint64 sequences[] = { 2, 3, 1, 5, 4, 6, 1 };
	TwoHopRecord *r = &two_records[at];
	UndoBlockHeader bh = { .magic = PGRAC_UNDO_BLOCK_MAGIC,
		.block_version = UNDO_BLOCK_VERSION_1, .slot_count = 1, .block_lsn = 123 + at };
	UndoRecordHeader h = { .record_type = lock ? UNDO_RECORD_ITL : UNDO_RECORD_INSERT,
		.flags = UNDO_REC_FLAG_HAS_ITL_HISTORY | UNDO_REC_FLAG_HAS_OPERATION,
		.xid = child, .origin_node_id = segment == 257 ? 1 : 0,
		.tt_slot_segment_id = segment, .tt_slot_id = 4, .tt_wrap_plus1 = 1,
		.write_scn = scn, .target_locator = { pcm_source.tag.spcOid,
			pcm_source.tag.dbOid, pcm_source.tag.relNumber }, .target_fork = MAIN_FORKNUM,
		.target_block = 3, .target_offset = offnum, .prev_uba = previous };
	UndoItlHistoryTrailer history = { .magic = UNDO_ITL_HISTORY_MAGIC,
		.version = UNDO_ITL_HISTORY_VERSION, .count = 1 };
	UndoOperationTrailer op = { .version = UNDO_OPERATION_TRAILER_VERSION,
		.length = sizeof(op), .effect_state = child == 112 ?
		CLUSTER_SAVEPOINT_EFFECT_DISCARDED : CLUSTER_SAVEPOINT_EFFECT_LIVE,
		.canonical_top_fxid = U64FromFullTransactionId(FullTransactionIdFromEpochAndXid(1,
			segment == 257 ? 129 : 80)),
		.operation_fxid = U64FromFullTransactionId(FullTransactionIdFromEpochAndXid(1, child)),
		.operation_sequence = sequences[at], .command_id = 4, .subid = 3 };
	UndoInsertPayload inserted = { 64, 0 };
	UndoUpdatePayload updated = { .new_block = 3, .new_offset = 3,
		.old_tuple_length = SizeofHeapTupleHeader, .old_tuple_offset = sizeof(updated) };
	UndoItlPayload locked = { .itl_slot_idx = slot,
		.new_flags = ITL_FLAG_LOCK_ONLY_ACTIVE, .lock_xid = child };
	UndoSlotDirEntry dir = { .record_offset = sizeof(bh), .record_type = h.record_type };
	size_t n = at == 4 ? sizeof(updated) + SizeofHeapTupleHeader
		: lock ? sizeof(locked) : sizeof(inserted);
	char *body = r->bytes.data + sizeof(bh) + sizeof(h);
	memset(r, 0, sizeof(*r)); r->uba = uba_encode(segment, block, 3, 0);
	if (UBA_is_invalid(previous)) h.flags |= UNDO_REC_FLAG_FIRST_IN_TX;
	history.entries[0] = (UndoItlHistoryEntry){ .block = 3, .slot_index = slot,
		.after_kind = lock ? ITL_FLAG_LOCK_ONLY_ACTIVE : ITL_FLAG_ACTIVE,
		.after_write_scn = scn };
	if (prior != NULL) history.entries[0].prior = *prior;
	else if (slot == 2 || slot == 4) history.entries[0].prior.wrap = 1;
	if (at == 4) {
		h.record_type = dir.record_type = UNDO_RECORD_UPDATE;
		memcpy(body, &updated, sizeof(updated));
	} else memcpy(body, lock ? (const void *)&locked : (const void *)&inserted, n);
	memcpy(body + n, &history, sizeof(history));
	memcpy(body + n + sizeof(history), &op, sizeof(op));
	h.payload_length = n + sizeof(history) + sizeof(op);
	dir.flags = h.flags; dir.record_length = sizeof(h) + h.payload_length;
	bh.free_offset = sizeof(bh) + dir.record_length;
	memcpy(r->bytes.data, &bh, sizeof(bh));
	memcpy(r->bytes.data + sizeof(bh), &h, sizeof(h));
	memcpy(r->bytes.data + BLCKSZ - sizeof(dir), &dir, sizeof(dir));
	r->peer = (ClusterMxHistoryPeer){ .flags = CLUSTER_MX_HISTORY_ORDERED,
		.record_class = CMXH_OPERATION, .outcome = CLUSTER_TX_IN_PROGRESS,
		.effect = op.effect_state, .proof_kind = CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG,
		.canonical = { segment, segment == 257 ? 129 : 80, 3, 0, 0 },
		.top_fxid = FullTransactionIdFromU64(op.canonical_top_fxid),
		.operation_fxid = FullTransactionIdFromU64(op.operation_fxid),
		.operation_sequence = op.operation_sequence, .previous = previous,
		.record = r->uba, .data_lsn = bh.block_lsn, .formation_epoch = 7,
		.admission_generation = admission.record_generation };
	if (child != 112) { r->peer.flags |= CLUSTER_MX_HISTORY_HAS_GRANT;
		r->peer.ctrc_grant = segment == 257 ? 73 : 47; }
	r->peer.data_crc = finish_crc(r->bytes.data, BLCKSZ);
}
static ClusterMxHistoryStep
two_landing(const ClusterMxHistoryRequestWire *wire, ClusterMxHistoryPeerResult *out)
{
	ClusterMxHistoryRequest r;
	if (!cluster_mx_history_request_decode(wire, sizeof(*wire), cluster_node_id, 0, 7, &r)
		&& !cluster_mx_history_request_decode(wire, sizeof(*wire), cluster_node_id, 1, 7, &r))
		return CMXH_UNKNOWN;
	for (unsigned i = 0; i < lengthof(two_records); ++i) {
		TwoHopRecord *record = &two_records[i];
		ClusterMxHistoryPeer peer = record->peer;
		if (!cluster_undo_record_uba_equal(record->uba, r.locator.uba)) continue;
		record->reads++; finish_reads++;
		printf("# record=%u target=%u edge=%u\n", i, r.target_offset, r.edge_kind);
		if (two_missing == i + 1) return CMXH_UNKNOWN;
		memset(out, 0, sizeof(*out)); memcpy(out->data, record->bytes.data, BLCKSZ);
		if (two_discard == i + 1) { /* Inconsistent peer bytes must also refuse. */
			peer.effect = CLUSTER_SAVEPOINT_EFFECT_DISCARDED;
			peer.flags &= ~CLUSTER_MX_HISTORY_HAS_GRANT; peer.ctrc_grant = 0;
		}
		if (!cluster_mx_history_peer_encode(&peer, &out->peer)) return CMXH_UNKNOWN;
		out->request = *wire; out->admitted = 1;
		ClusterR4WireWriteU64(out->auth + 8, 101);
		return CMXH_COMPLETE;
	}
	return CMXH_UNKNOWN;
}
static HeapTupleData
two_tuple(OffsetNumber offnum)
{
	HeapTupleData t = { 0 }; ItemId lp = PageGetItemId(BufferBlocks, offnum);
	ItemPointerSet(&t.t_self, 3, offnum); t.t_tableOid = relation.rd_id;
	t.t_data = (HeapTupleHeader)PageGetItem(BufferBlocks, lp); t.t_len = ItemIdGetLength(lp);
	return t;
}
static Snapshot
two_setup(void)
{
	Snapshot s = writer_setup(CLUSTER_TX_IN_PROGRESS, false);
	PageHeader ph = (PageHeader)BufferBlocks;
	ClusterItlSlotData *slots = ClusterPageGetItlSlots(BufferBlocks);
	HeapTupleData second = hot_candidate(), third;
	ClusterItlSlotData prior = slots[2], update_prior;
	update_prior = prior; update_prior.undo_segment_head = uba_encode(1, 11, 3, 0);
	update_prior.write_scn = 82;
	memcpy(second.t_data, tuple.t_data, SizeofHeapTupleHeader);
	HeapTupleHeaderSetXmin(second.t_data, 96);
	ItemPointerSet(&second.t_data->t_ctid, 3, 3); second.t_data->t_itl_slot_idx = 2;
	ph->pd_lower += sizeof(ItemIdData); ph->pd_upper -= MAXALIGN(SizeofHeapTupleHeader);
	ItemIdSetNormal(&ph->pd_linp[2], ph->pd_upper, SizeofHeapTupleHeader);
	third = two_tuple(3); memset(third.t_data, 0, SizeofHeapTupleHeader);
	third.t_data->t_hoff = SizeofHeapTupleHeader; HeapTupleHeaderSetXmin(third.t_data, 96);
	third.t_data->t_itl_slot_idx = 2; ItemPointerSet(&third.t_data->t_ctid, 3, 3);
	/* Immutable top80 append chain: 6(B2) <- 7(A1) <- 8(discarded)
	 * <- 11(B3 UPDATE) <- 13(A2 lock) <- 14(discarded). Page prior of
	 * 11 is 6; of 13 is 11. Own top129/child145 has a separate 12. */
	two_record(0, 1, 7, 1, 0, 96, 80, false, uba_encode(1, 6, 3, 0), NULL);
	two_record(1, 1, 8, 1, 1, 112, 81, false, uba_encode(1, 7, 3, 0), NULL);
	two_record(2, 1, 6, 2, 2, 96, 79, false, (UBA){ { 0, 0 } }, NULL);
	two_record(3, 1, 13, 2, 2, 96, 83, true, uba_encode(1, 11, 3, 0), &update_prior);
	two_record(4, 1, 11, 2, 2, 96, 82, false, uba_encode(1, 8, 3, 0), &prior);
	two_record(5, 1, 14, 2, 5, 112, 84, false, uba_encode(1, 13, 3, 0), NULL);
	two_record(6, 257, 12, 1, 7, 145, 91, true, (UBA){ { 0, 0 } }, &slots[7]);
	slots[2].flags = ITL_FLAG_LOCK_ONLY_ACTIVE; slots[2].write_scn = 83; slots[2].wrap = 2;
	slots[2].undo_segment_head = uba_encode(1, 13, 3, 0);
	slots[5] = (ClusterItlSlotData){ .xid = 80, .flags = ITL_FLAG_ACTIVE,
		.undo_segment_head = uba_encode(1, 14, 3, 0), .write_scn = 84 };
	canonical_source = (ClusterCanonicalTxnBinding){ .segment_id = 257,
		.xid = 129, .slot_offset = 3, .origin_instance = 2,
		.publish_state = CLUSTER_CANONICAL_TXN_PUBLISHED, .active_lsn = 123 };
	publication_source = CLUSTER_CANONICAL_TXN_PUBLISHED;
	prepare_input = two_prepare; poll_input = two_landing;
	two_missing = two_discard = 0; two_extra_delta = false;
	publication_descriptor = 33; publication_count = publication_prepares = publication_applies = publication_cancels = 0;
	publication_retry = false; publication_retry_at = 0;
	mx_history_recursive_open(s); return s;
}
static bool
two_publish(MxHeapReadout *out, HeapTuple root, unsigned hop, ClusterHeapSuccessorProof *next)
{
	MxPublicationReadout plan;
	ClusterCtrcTargetV1 target = { .kind = CTRC_TARGET_EXACT_TID, .block_number = 3,
		.offset_number = ItemPointerGetOffsetNumber(&root->t_self) };
	memset(next, 0, sizeof(*next));
	if (!publication_prepare(&plan)) return false;
	if (!mx_history_recursive_next(root, next)) { mx_publication_cancel(&plan); return false; }
	if (!mx_history_transition_plan_at(1, root, &out->successor, &plan, false,
			uba_encode(257, 12 + hop, 3, 0), 91 + hop)) {
		mx_publication_cancel(&plan); return false;
	}
	target.multixact_id = plan.descriptor; target.descriptor_hash = plan.descriptor_hash;
	if (mx_publication_apply(&target, false, &plan) != 0) return false;
	mx_history_transition_write_header(root, false);
	cluster_itl_stamp_lock_active_with_history(1, 7, 129, 91 + hop,
		uba_encode(257, 12 + hop, 3, 0));
	PageSetLSN(BufferBlocks, 129 + hop);
	if (two_extra_delta) two_tuple(3).t_data->t_infomask2++;
	return mx_history_transition_finish(1, true, 129 + hop);
}
UT_TEST(continuous_three_tuple_two_edge_publication)
{
	Snapshot s = two_setup(); MxHeapReadout a, b;
	HeapTupleData first = tuple, second = two_tuple(2), third = two_tuple(3);
	ClusterHeapSuccessorProof next_a, next_b;
	int result = writer_scoped_call(&a);
	printf("# hop1 result=%d error=%d requests=%u reads=%u\n", result, error_code, finish_requests, finish_reads);
	UT_ASSERT_EQ(result, 2);
	if (result != 2) { writer_clean(s, true); return; }
	UT_ASSERT_EQ(finish_requests, 6); UT_ASSERT_EQ(finish_reads, 6);
	UT_ASSERT_EQ(two_records[6].reads, 0);
	if (!two_publish(&a, &first, 0, &next_a)) {
		UT_ASSERT(false); writer_clean(s, true); return;
	}
	UT_ASSERT_EQ(writer_match(&second, &next_a, true), 1);
	/* Keep the scope, page and all source bytes, including newly published 12. */
	mx_history_recursive_begin_hop(); publication_descriptor = 37; tuple = second;
	result = writer_scoped_call(&b);
	printf("# hop2 result=%d error=%d requests=%u reads=%u own_reads=%u\n", result, error_code, finish_requests, finish_reads, two_records[6].reads);
	UT_ASSERT_EQ(result, 2);
	if (result != 2) { tuple = first; writer_clean(s, true); return; }
	UT_ASSERT_EQ(two_records[6].reads, 1);
	for (unsigned i = 0; i < 6; i++) UT_ASSERT_EQ(two_records[i].reads, 2);
	UT_ASSERT_EQ(finish_requests, 13); UT_ASSERT_EQ(finish_reads, 13);
	UT_ASSERT_EQ(a.successor.writer.scope_generation, b.successor.writer.scope_generation);
	UT_ASSERT_EQ(a.successor.writer.snapshot_identity, b.successor.writer.snapshot_identity);
	UT_ASSERT_EQ(a.successor.writer.read_scn, b.successor.writer.read_scn);
	UT_ASSERT_EQ(publication_A_capability, 21);
	UT_ASSERT_EQ(a.successor.writer.capability_generation, 55);
	UT_ASSERT_EQ(b.successor.writer.capability_generation, 55);
	UT_ASSERT(cluster_undo_record_uba_equal(a.successor.writer.member.operation_uba,
		uba_encode(1, 6, 3, 0)));
	UT_ASSERT(cluster_undo_record_uba_equal(b.successor.writer.member.operation_uba,
		uba_encode(1, 11, 3, 0)));
	UT_ASSERT_EQ(ItemPointerGetOffsetNumber(&b.successor.tid), 3);
	UT_ASSERT(memcmp(&a.successor.writer.key, &b.successor.writer.key, sizeof(a.successor.writer.key)) != 0);
	if (!two_publish(&b, &second, 1, &next_b)) {
		UT_ASSERT(false); tuple = first; writer_clean(s, true); return;
	}
	UT_ASSERT_EQ(publication_A_capability, 21);
	UT_ASSERT_EQ(writer_match(&third, &next_b, true), 1);
	UT_ASSERT_EQ(writer_match(&third, &next_b, true), 0);
	UT_ASSERT_EQ(writer_match(&second, &next_a, true), 0);
	UT_ASSERT_EQ(publication_applied_target.offset_number, 2);
	UT_ASSERT_EQ(publication_prepares, 4); UT_ASSERT_EQ(publication_applies, 4);
	UT_ASSERT_EQ(publication_cancels, 0);
	for (unsigned i = 0; i < lengthof(two_records); i++)
		UT_ASSERT_EQ(finish_crc(two_records[i].bytes.data, BLCKSZ), two_records[i].peer.data_crc);
	tuple = first; writer_clean(s, true);
}
/* Prefix uses the same actual consumer as the positive case. Only the
 * external second landing is faulted; the first publication is retained. */
static bool
two_reach_second(MxHeapReadout *out)
{
	MxHeapReadout a; ClusterHeapSuccessorProof next;
	HeapTupleData second = two_tuple(2);
	if (writer_scoped_call(&a) != 2 || !two_publish(&a, &tuple, 0, &next)
		|| writer_match(&second, &next, true) != 1) return false;
	mx_history_recursive_begin_hop(); publication_descriptor = 37; tuple = second;
	return writer_scoped_call(out) == 2;
}
UT_TEST(second_hop_requires_retained_own_history_and_complete_B)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		Snapshot s = two_setup(); HeapTupleData first = tuple, second = two_tuple(2);
		MxHeapReadout a, b; ClusterHeapSuccessorProof next;
		if (writer_scoped_call(&a) != 2 || !two_publish(&a, &first, 0, &next)
			|| writer_match(&second, &next, true) != 1) {
			UT_ASSERT(false); writer_clean(s, true); continue;
		}
		mx_history_recursive_begin_hop(); publication_descriptor = 37; tuple = second;
		if (fault == 0) two_missing = 7;
		if (fault == 1) two_missing = 5;
		if (fault == 2) two_discard = 5;
		UT_ASSERT_EQ(writer_scoped_call(&b), -1);
		UT_ASSERT_EQ(error_code, ERRCODE_CLUSTER_CROSS_NODE_WRITE_CONFLICT);
		bridge_no_authority(&b);
		UT_ASSERT_EQ(publication_prepares, 2); UT_ASSERT_EQ(publication_applies, 2);
		UT_ASSERT_EQ(publication_cancels, 0);
		UT_ASSERT_EQ(two_records[fault == 0 ? 6 : 4].reads, fault == 0 ? 1 : 2);
		tuple = first; writer_clean(s, true);
	}
}
UT_TEST(second_transition_rejects_extra_delta_after_APPLY)
{
	Snapshot s = two_setup(); HeapTupleData first = tuple, third = two_tuple(3);
	MxHeapReadout b; ClusterHeapSuccessorProof next;
	if (!two_reach_second(&b)) { UT_ASSERT(false); tuple = first; writer_clean(s, true); return; }
	two_extra_delta = true;
	UT_ASSERT(!two_publish(&b, &tuple, 1, &next));
	UT_ASSERT_EQ(publication_prepares, 4); UT_ASSERT_EQ(publication_applies, 4);
	if (!next.valid || publication_applies != 4) {
		UT_ASSERT(false); tuple = first; writer_clean(s, true); return;
	}
	UT_ASSERT_EQ(writer_match(&third, &next, true), 0);
	UT_ASSERT(!mx_history_transition_finish(1, true, 130));
	UT_ASSERT_EQ(publication_cancels, 0); tuple = first; writer_clean(s, true);
}
UT_TEST(second_hop_edge_remains_consume_once)
{
	Snapshot s = two_setup(); HeapTupleData first = tuple;
	MxHeapReadout b; ClusterHeapSuccessorProof next;
	volatile bool rejected = false;
	if (!two_reach_second(&b)) { UT_ASSERT(false); tuple = first; writer_clean(s, true); return; }
	UT_ASSERT(mx_history_recursive_next(&tuple, &next));
	UT_ASSERT_EQ(ItemPointerGetOffsetNumber(&next.tid), 3);
	PG_TRY(); { (void)mx_history_recursive_next(&tuple, &next); }
	PG_CATCH(); { rejected = true; }
	PG_END_TRY(); UT_ASSERT(rejected);
	UT_ASSERT_EQ(publication_prepares, 2); UT_ASSERT_EQ(publication_applies, 2);
	tuple = first; writer_clean(s, true);
}
int main(void)
{
	history_fixture_init(); UT_PLAN(4);
	UT_RUN(continuous_three_tuple_two_edge_publication);
	UT_RUN(second_hop_requires_retained_own_history_and_complete_B);
	UT_RUN(second_transition_rejects_extra_delta_after_APPLY);
	UT_RUN(second_hop_edge_remains_consume_once);
	free(procArray); UT_DONE(); return ut_failed_count ? 1 : 0;
}
