/* Real graph, queue/landing, wire and record decoders. Transport byte sources,
 * scheduling and original route fixture posture are explicit boundaries.
 * No local TT resolution, grant or retention result is fabricated here.
 * Portions Copyright (c) 2026, pgrac contributors */
#include "postgres.h"
#include "cluster/cluster_mx_history_graph.h"
NodeId graph_fixture_unused_origin(UBA);
uint64 graph_fixture_unused_hash(const ClusterCurrentMxKey *, const ClusterCurrentMxMemberDesc *,
								 uint16);
bool graph_fixture_unused_resolve(TransactionId, uint8, uint16, uint16, uint32, bool,
								  const ClusterTTStatusKey *, const ClusterTTStatusResult *,
								  ClusterCurrentMxExactLookupFn, void *,
								  ClusterCurrentMemberProof *);
bool graph_fixture_unused_successor(const ClusterCurrentMxSuccessorAlias *,
									const ClusterTxLocator *, TransactionId, uint16, uint32);
#define uba_origin_node_id graph_fixture_unused_origin
#define cluster_multixact_current_descriptor_hash graph_fixture_unused_hash
#define cluster_multixact_current_resolve_origin_member_proof graph_fixture_unused_resolve
#define cluster_multixact_current_successor_provenance_well_formed graph_fixture_unused_successor
void graph_fixture_unused_pfree(void *);
#define pfree graph_fixture_unused_pfree
int graph_backend_unused_main(void);
#define HISTORY_BACKEND_TEST_MAIN graph_backend_unused_main
#include "test_cluster_mx_history_backend.c"
#undef pfree
#undef uba_origin_node_id
#undef cluster_multixact_current_descriptor_hash
#undef cluster_multixact_current_resolve_origin_member_proof
#undef cluster_multixact_current_successor_provenance_well_formed
#include "cluster/cluster_mx_history_graph.h"
#include "utils/memutils.h"

static unsigned graph_allocations, graph_fail_allocation;
void *
MemoryContextAllocExtended(MemoryContext context, Size size, int flags)
{
	graph_allocations++;
	if (graph_fail_allocation && graph_allocations == graph_fail_allocation)
		return NULL;
	return calloc(1, size);
}

void
pfree(void *pointer)
{
	free(pointer);
}

typedef struct GraphSource {
	UBA address, previous;
	ClusterItlSlotData prior;
	uint32 top, child, block, generation;
	uint8 slot, kind, effect;
	uint64 sequence;
	uint16 wrap;
} GraphSource;
static GraphSource *sources;
static size_t source_count;
static unsigned graph_fetches;
static unsigned malformed_peer, missing_landing;
static ClusterMxHistoryCapture input;

static GraphSource
source_at(uint32 block)
{
	GraphSource s = { 0 };
	s.address = uba_encode(257, block, 3, 0);
	s.top = 100;
	s.child = 103;
	s.block = 17;
	s.generation = 12;
	s.kind = CMXH_OPERATION;
	s.effect = CLUSTER_SAVEPOINT_EFFECT_LIVE;
	s.sequence = block;
	s.wrap = 2;
	return s;
}
static ClusterItlSlotData
source_head(const GraphSource *s)
{
	ClusterItlSlotData h = s->prior;
	if (h.flags != ITL_FLAG_FREE && !(h.flags == ITL_FLAG_ACTIVE && h.xid == s->top))
		h.wrap++;
	h.xid = s->top;
	h.flags = ITL_FLAG_ACTIVE;
	h.lock_count = 0;
	h.commit_scn = 0;
	h.undo_segment_head = s->address;
	h.write_scn = 100 + s->sequence;
	h.first_change_lsn = 0;
	return h;
}
static void
graph_reset(size_t n)
{
	ClusterMxHistoryRequest r;
	history_reset();
	graph_allocations = graph_fail_allocation = graph_fetches = 0;
	malformed_peer = missing_landing = 0;
	sources = calloc(n, sizeof(*sources));
	source_count = n;
	memset(&input, 0, sizeof(input));
	r = history_input();
	input.pcm.tag = (BufferTag){ 1663, 42, 99, MAIN_FORKNUM, 17 };
	ItemPointerSet(&input.tid, 17, 2);
	input.mxkey = (ClusterCurrentMxKey){ 0, 0, 10, 9, 0 };
	input.nmembers = 2;
	input.members[0] = (ClusterCurrentMxMemberDesc){ 103, MultiXactStatusForShare, { 0 } };
	input.members[1] = (ClusterCurrentMxMemberDesc){ 104, MultiXactStatusForKeyShare, { 0 } };
	input.descriptor_hash
		= cluster_multixact_current_descriptor_hash(&input.mxkey, input.members, 2);
	input.admission = (ClusterSemanticAdmissionToken){ r.feature_bit, r.admission_generation, 9,
													   CLUSTER_SEMANTIC_TARGET_SIDE, true };
	input.read_scn = r.read_scn;
	input.retained_floor = r.retained_floor;
	input.read_epoch = r.read_epoch;
	input.snapshot_identity = r.snapshot_identity;
	input.collector_id = r.collector_id;
	input.capture_generation = r.capture_generation;
	input.backend_incarnation = r.backend_incarnation;
}
static ClusterMxHistoryGraph *
graph_begin(void)
{
	return cluster_mx_history_graph_create(&input, CurrentMemoryContext);
}
static void
graph_clean(ClusterMxHistoryGraph *g)
{
	cluster_mx_history_graph_destroy(g);
	free(sources);
	sources = NULL;
	source_count = 0;
}

/* Build actual native undo bytes, then pass through the real reply codec,
 * registered handler and consume-once backend poll before graph acceptance. */
static bool
graph_deliver(ClusterMxHistoryGraph *g, ClusterMxHistoryRequest r)
{
	ClusterMxHistoryRequestWire wire;
	ClusterMxHistoryPeerResult result;
	ClusterMxHistoryReplyWire reply;
	ClusterMxHistoryPeer p = { 0 };
	ClusterMxHistoryPeerWire pw;
	UndoRecordHeader h = { 0 };
	UndoBlockHeader bh = { 0 };
	UndoSlotDirEntry dir = { 0 };
	UndoOperationTrailer op = { 0 };
	UndoItlHistoryTrailer history = { 0 };
	UndoInsertPayload insert = { 64, 0 };
	UndoSavepointRollbackPayload control = { 0 };
	PGAlignedBlock data;
	uint8 auth[16] = { 0 };
	char *body;
	const GraphSource *s = NULL;
	uint32 segment, block;
	uint16 tt, row;
	size_t body_size;
	ClusterICEnvelope env;
	for (size_t i = 0; i < source_count; ++i)
		if (cluster_undo_record_uba_equal(r.locator.uba, sources[i].address)) {
			s = &sources[i];
			break;
		}
	if (!s) {
		(void)cluster_mx_history_graph_accept(g, NULL);
		return false;
	}
	UT_ASSERT(uba_decode_record(r.locator.uba, &segment, &block, &tt, &row));
	if (cluster_gcs_block_mx_history_prepare_routed(&r, &wire) != CMXH_COMPLETE)
		return false;
	if (!cluster_mx_history_request_decode(&wire, sizeof(wire), 2, 1, 9, &r))
		return false;
	if (!cluster_gcs_block_mx_history_submit(&wire))
		return false;
	graph_fetches++;
	memset(&data, 0, sizeof(data));
	bh.magic = PGRAC_UNDO_BLOCK_MAGIC;
	bh.block_version = UNDO_BLOCK_VERSION_1;
	bh.slot_count = 1;
	bh.block_lsn = 120 + s->sequence;
	h.record_type
		= s->kind == CMXH_ROLLBACK_CONTROL ? UNDO_RECORD_SAVEPOINT_ROLLBACK : UNDO_RECORD_INSERT;
	h.flags = s->kind == CMXH_ROLLBACK_CONTROL ? UNDO_REC_FLAG_HAS_OPERATION
											   : UNDO_REC_FLAG_HAS_ITL_HISTORY;
	if (s->kind == CMXH_OPERATION)
		h.flags |= UNDO_REC_FLAG_HAS_OPERATION;
	if (UBA_is_invalid(s->previous))
		h.flags |= UNDO_REC_FLAG_FIRST_IN_TX;
	h.xid = s->kind == CMXH_V1_TOP ? s->top : s->child;
	h.origin_node_id = 1;
	h.tt_slot_segment_id = 257;
	h.tt_slot_id = 4;
	h.tt_wrap_plus1 = s->wrap + 1;
	h.write_scn = 100 + s->sequence;
	h.prev_uba = s->previous;
	h.target_locator = (RelFileLocator){ 1663, 42, 99 };
	h.target_fork = MAIN_FORKNUM;
	h.target_block = s->block;
	h.target_offset = 2;
	op.version = UNDO_OPERATION_TRAILER_VERSION;
	op.length = sizeof(op);
	op.effect_state = s->effect;
	op.canonical_top_fxid = U64FromFullTransactionId(FullTransactionIdFromEpochAndXid(2, s->top));
	op.operation_fxid = U64FromFullTransactionId(FullTransactionIdFromEpochAndXid(2, s->child));
	op.operation_sequence = s->sequence;
	op.canonical_segment_generation = s->generation;
	op.command_id = 4;
	op.subid = 3;
	history.magic = UNDO_ITL_HISTORY_MAGIC;
	history.version = UNDO_ITL_HISTORY_VERSION;
	history.count = 1;
	history.entries[0] = (UndoItlHistoryEntry){ .block = s->block,
												.slot_index = s->slot,
												.after_kind = ITL_FLAG_ACTIVE,
												.after_write_scn = h.write_scn,
												.prior = s->prior };
	body = data.data + sizeof(bh) + sizeof(h);
	if (s->kind == CMXH_ROLLBACK_CONTROL) {
		memset(&h.target_locator, 0, sizeof(h.target_locator));
		h.target_block = h.target_offset = 0;
		control.version = UNDO_SAVEPOINT_ROLLBACK_VERSION;
		control.length = sizeof(control);
		control.phase = UNDO_SAVEPOINT_ROLLBACK_PENDING;
		control.rollback_sequence = 1;
		control.start_uba = control.next_uba = s->previous;
		control.scope_child_fxid = op.operation_fxid;
		memcpy(body, &control, sizeof(control));
		memcpy(body + sizeof(control), &op, sizeof(op));
		body_size = sizeof(control) + sizeof(op);
	} else {
		memcpy(body, &insert, sizeof(insert));
		memcpy(body + sizeof(insert), &history, sizeof(history));
		body_size = sizeof(insert) + sizeof(history);
		if (s->kind == CMXH_OPERATION) {
			memcpy(body + body_size, &op, sizeof(op));
			body_size += sizeof(op);
		}
	}
	h.payload_length = body_size;
	dir.record_offset = sizeof(bh);
	dir.record_length = sizeof(h) + body_size;
	dir.record_type = h.record_type;
	dir.flags = h.flags;
	bh.free_offset = dir.record_offset + dir.record_length;
	memcpy(data.data, &bh, sizeof(bh));
	memcpy(data.data + sizeof(bh), &h, sizeof(h));
	memcpy(data.data + BLCKSZ - sizeof(dir), &dir, sizeof(dir));
	p.flags = CLUSTER_MX_HISTORY_ORDERED;
	p.record_class = s->kind;
	p.outcome = CLUSTER_TX_COMMITTED;
	p.effect = s->kind == CMXH_ROLLBACK_CONTROL ? 0 : s->effect;
	p.proof_kind = CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG;
	p.canonical = (ClusterTTSlotPhysicalLocator){ 257, s->top, 3, s->wrap, 0 };
	p.canonical_generation = s->generation;
	p.top_fxid = FullTransactionIdFromEpochAndXid(2, s->top);
	p.operation_fxid
		= s->kind == CMXH_V1_TOP ? p.top_fxid : FullTransactionIdFromEpochAndXid(2, s->child);
	p.operation_sequence = s->kind == CMXH_V1_TOP ? 0 : s->sequence;
	p.record = s->address;
	p.previous = s->previous;
	p.data_generation = 0;
	p.data_crc = history_crc(data.data, BLCKSZ);
	p.data_lsn = bh.block_lsn;
	p.commit_scn = 1100;
	p.formation_epoch = r.read_epoch;
	p.admission_generation = r.admission_generation;
	if (malformed_peer == 1)
		p.operation_fxid = FullTransactionIdFromEpochAndXid(3, s->child);
	if (malformed_peer == 2)
		p.canonical_generation++;
	if (malformed_peer == 3)
		p.previous = uba_encode(258, 7, 3, 0);
	ClusterR4WireWriteU64(auth, p.canonical_generation);
	ClusterR4WireWriteU64(auth + 8, 1001);
	UT_ASSERT(cluster_mx_history_peer_encode(&p, &pw));
	UT_ASSERT(
		cluster_mx_history_reply_encode(&wire, 1, 130, (uint8 *)data.data, auth, &pw, &reply));
	env = route_test_envelope(PGRAC_IC_MSG_GCS_BLOCK_REPLY, 1, 2, 8816);
	cluster_gcs_handle_block_reply_envelope(&env, &reply);
	UT_ASSERT_EQ(cluster_gcs_block_mx_history_poll(&wire, &result), CMXH_COMPLETE);
	if (missing_landing == 1)
		result.admitted = 0;
	if (missing_landing == 2)
		result.consumed = 1;
	return cluster_mx_history_graph_accept(g, &result);
}
static ClusterMxHistoryStep
graph_run(ClusterMxHistoryGraph *g)
{
	for (size_t safety = 0; safety < 200000; ++safety) {
		ClusterMxHistoryRequest r;
		bool needs_read = false;
		ClusterMxHistoryStep step = cluster_mx_history_graph_next(g, &r, &needs_read);
		if (step != CMXH_PENDING)
			return step;
		if (needs_read && !graph_deliver(g, r))
			return CMXH_UNKNOWN;
	}
	UT_ASSERT(false);
	return CMXH_UNKNOWN;
}
static void
check_hidden(ClusterMxHistoryGraph *g)
{
	ClusterMxHistoryNodeView view, zero = { 0 };
	memset(&view, 0xa5, sizeof(view));
	UT_ASSERT(!cluster_mx_history_graph_view(g, 0, &view));
	UT_ASSERT_EQ(memcmp(&view, &zero, sizeof(view)), 0);
	UT_ASSERT_EQ(cluster_mx_history_graph_count(g), 0);
}
UT_TEST(graph_single_root_real_landing)
{
	ClusterMxHistoryGraph *g;
	ClusterMxHistoryNodeView v;
	graph_reset(1);
	sources[0] = source_at(7);
	input.roots[0] = source_head(&sources[0]);
	g = graph_begin();
	UT_ASSERT(g != NULL);
	if (g) {
		check_hidden(g);
		UT_ASSERT_EQ(graph_run(g), CMXH_COMPLETE);
		UT_ASSERT_EQ(graph_fetches, 1);
		UT_ASSERT_EQ(cluster_mx_history_graph_count(g), 1);
		UT_ASSERT(cluster_mx_history_graph_view(g, 0, &v));
		UT_ASSERT(v.record && v.record->matches_target);
		UT_ASSERT_EQ(v.global_prev, CMXH_NO_NODE);
		UT_ASSERT_EQ(v.page_prior, CMXH_NO_NODE);
	}
	graph_clean(g);
}
UT_TEST(graph_global_nonpage_and_shared_prior)
{
	ClusterMxHistoryGraph *g;
	ClusterMxHistoryNodeView v;
	graph_reset(3);
	sources[0] = source_at(7);
	sources[1] = source_at(6);
	sources[2] = source_at(5);
	sources[0].previous = sources[1].address;
	sources[1].previous = sources[2].address;
	sources[1].block = 18;
	sources[0].prior = source_head(&sources[2]);
	input.roots[0] = source_head(&sources[0]);
	g = graph_begin();
	UT_ASSERT(g != NULL);
	if (g) {
		UT_ASSERT_EQ(graph_run(g), CMXH_COMPLETE);
		UT_ASSERT_EQ(graph_fetches, 3);
		UT_ASSERT(cluster_mx_history_graph_view(g, 0, &v));
		UT_ASSERT_EQ(v.page_prior, 2);
		UT_ASSERT(cluster_mx_history_graph_view(g, 1, &v));
		UT_ASSERT(v.record && !v.record->matches_target);
	}
	graph_clean(g);
}
UT_TEST(graph_multiple_roots_share_done_node)
{
	ClusterMxHistoryGraph *g;
	graph_reset(3);
	sources[0] = source_at(7);
	sources[1] = source_at(6);
	sources[2] = source_at(5);
	sources[0].previous = sources[2].address;
	sources[1].previous = sources[2].address;
	sources[1].slot = 1;
	input.roots[0] = source_head(&sources[0]);
	input.roots[1] = source_head(&sources[1]);
	g = graph_begin();
	UT_ASSERT(g != NULL);
	if (g) {
		UT_ASSERT_EQ(graph_run(g), CMXH_COMPLETE);
		UT_ASSERT_EQ(graph_fetches, 3);
	}
	graph_clean(g);
}
UT_TEST(graph_control_and_v1_are_distinct)
{
	ClusterMxHistoryGraph *g;
	ClusterMxHistoryNodeView v;
	graph_reset(3);
	sources[0] = source_at(7);
	sources[1] = source_at(6);
	sources[2] = source_at(5);
	sources[0].previous = sources[1].address;
	sources[1].previous = sources[2].address;
	sources[1].kind = CMXH_ROLLBACK_CONTROL;
	sources[2].kind = CMXH_V1_TOP;
	input.roots[0] = source_head(&sources[0]);
	g = graph_begin();
	UT_ASSERT(g != NULL);
	if (g) {
		UT_ASSERT_EQ(graph_run(g), CMXH_COMPLETE);
		UT_ASSERT_EQ(graph_fetches, 3);
		UT_ASSERT(cluster_mx_history_graph_view(g, 1, &v));
		if (v.record)
			UT_ASSERT_EQ(v.record->record_class, CMXH_ROLLBACK_CONTROL);
		UT_ASSERT(v.record && !v.record->matches_target);
		if (v.peer)
			UT_ASSERT_EQ(v.peer->ctrc_grant, 0);
		UT_ASSERT(cluster_mx_history_graph_view(g, 2, &v));
		if (v.record)
			UT_ASSERT_EQ(v.record->record_class, CMXH_V1_TOP);
		if (v.record && v.peer)
			UT_ASSERT(FullTransactionIdEquals(v.record->identity.top_fxid, v.peer->top_fxid));
	}
	graph_clean(g);
}
UT_TEST(graph_global_cycle_rejected)
{
	ClusterMxHistoryGraph *g;
	graph_reset(2);
	sources[0] = source_at(7);
	sources[1] = source_at(6);
	sources[1].address = uba_encode(258, 6, 3, 0);
	sources[0].previous = sources[1].address;
	sources[1].previous = sources[0].address;
	input.roots[0] = source_head(&sources[0]);
	g = graph_begin();
	UT_ASSERT(g != NULL);
	if (g) {
		UT_ASSERT_EQ(graph_run(g), CMXH_UNKNOWN);
		UT_ASSERT_EQ(graph_fetches, 2);
		check_hidden(g);
	}
	graph_clean(g);
}
UT_TEST(graph_prior_cycle_rejected)
{
	ClusterMxHistoryGraph *g;
	graph_reset(2);
	sources[0] = source_at(7);
	sources[1] = source_at(6);
	sources[0].prior = source_head(&sources[1]);
	sources[1].prior = source_head(&sources[0]);
	input.roots[0] = source_head(&sources[0]);
	g = graph_begin();
	UT_ASSERT(g != NULL);
	if (g) {
		UT_ASSERT_EQ(graph_run(g), CMXH_UNKNOWN);
		UT_ASSERT_EQ(graph_fetches, 2);
		check_hidden(g);
	}
	graph_clean(g);
}
UT_TEST(graph_conflicting_owner_shared_address_rejected)
{
	ClusterMxHistoryGraph *g;
	graph_reset(3);
	sources[0] = source_at(7);
	sources[1] = source_at(6);
	sources[2] = source_at(5);
	sources[0].previous = sources[2].address;
	sources[1].previous = sources[2].address;
	sources[1].top = 200;
	sources[1].child = 203;
	sources[1].generation = 13;
	sources[0].prior = source_head(&sources[1]);
	input.roots[0] = source_head(&sources[0]);
	g = graph_begin();
	UT_ASSERT(g != NULL);
	if (g) {
		UT_ASSERT_EQ(graph_run(g), CMXH_UNKNOWN);
		UT_ASSERT_EQ(graph_fetches, 3);
		check_hidden(g);
	}
	graph_clean(g);
}
UT_TEST(graph_shared_address_new_head_constraint_rechecked)
{
	ClusterMxHistoryGraph *g;
	graph_reset(3);
	sources[0] = source_at(7);
	sources[1] = source_at(6);
	sources[2] = source_at(5);
	sources[0].previous = sources[2].address;
	sources[1].previous = sources[2].address;
	sources[0].prior = source_head(&sources[1]);
	sources[1].prior = source_head(&sources[2]);
	sources[1].prior.write_scn++;
	input.roots[0] = source_head(&sources[0]);
	g = graph_begin();
	UT_ASSERT(g != NULL);
	if (g) {
		UT_ASSERT_EQ(graph_run(g), CMXH_UNKNOWN);
		UT_ASSERT_EQ(graph_fetches, 3);
		check_hidden(g);
	}
	graph_clean(g);
}
UT_TEST(graph_missing_predecessor_is_not_a_tail)
{
	ClusterMxHistoryGraph *g;
	graph_reset(1);
	sources[0] = source_at(7);
	sources[0].previous = uba_encode(257, 6, 3, 0);
	input.roots[0] = source_head(&sources[0]);
	g = graph_begin();
	UT_ASSERT(g != NULL);
	if (g) {
		UT_ASSERT_EQ(graph_run(g), CMXH_UNKNOWN);
		check_hidden(g);
	}
	graph_clean(g);
}
UT_TEST(graph_no_fixed_depth_and_one_fetch_per_address)
{
	ClusterMxHistoryGraph *g;
	const size_t n = 1500;
	graph_reset(n);
	for (size_t i = 0; i < n; ++i) {
		sources[i] = source_at(n + 2 - i);
		if (i)
			sources[i - 1].previous = sources[i].address;
	}
	input.roots[0] = source_head(&sources[0]);
	g = graph_begin();
	UT_ASSERT(g != NULL);
	if (g) {
		UT_ASSERT_EQ(graph_run(g), CMXH_COMPLETE);
		UT_ASSERT_EQ(graph_fetches, n);
		UT_ASSERT_EQ(cluster_mx_history_graph_count(g), n);
	}
	graph_clean(g);
}
UT_TEST(graph_allocation_failure_cannot_complete_partial_graph)
{
	for (unsigned fail = 1; fail < 30; ++fail) {
		ClusterMxHistoryGraph *g;
		graph_reset(20);
		for (size_t i = 0; i < 20; ++i) {
			sources[i] = source_at(30 - i);
			if (i)
				sources[i - 1].previous = sources[i].address;
		}
		input.roots[0] = source_head(&sources[0]);
		graph_fail_allocation = fail;
		g = graph_begin();
		if (g) {
			UT_ASSERT_EQ(graph_run(g), CMXH_UNKNOWN);
			check_hidden(g);
		}
		graph_clean(g);
	}
}
UT_TEST(graph_peer_claims_must_match_exact_record)
{
	for (unsigned fault = 1; fault <= 3; ++fault) {
		ClusterMxHistoryGraph *g;
		graph_reset(1);
		sources[0] = source_at(7);
		input.roots[0] = source_head(&sources[0]);
		malformed_peer = fault;
		g = graph_begin();
		UT_ASSERT(g != NULL);
		if (g) {
			UT_ASSERT_EQ(graph_run(g), CMXH_UNKNOWN);
			check_hidden(g);
		}
		graph_clean(g);
	}
}
UT_TEST(graph_pure_decode_or_consumed_result_cannot_enter)
{
	for (unsigned fault = 1; fault <= 2; ++fault) {
		ClusterMxHistoryGraph *g;
		graph_reset(1);
		sources[0] = source_at(7);
		input.roots[0] = source_head(&sources[0]);
		missing_landing = fault;
		g = graph_begin();
		UT_ASSERT(g != NULL);
		if (g) {
			UT_ASSERT_EQ(graph_run(g), CMXH_UNKNOWN);
			check_hidden(g);
		}
		graph_clean(g);
	}
}
UT_TEST(graph_routes_actual_backend_and_capacity)
{
	ClusterMxHistoryGraph *g;
	ClusterMxHistoryRequest edge, routed;
	ClusterMxHistoryRequestWire wire, held[8], zero = { 0 };
	bool read = false;
	graph_reset(1);
	sources[0] = source_at(7);
	input.roots[0] = source_head(&sources[0]);
	g = graph_begin();
	UT_ASSERT(g != NULL);
	if (g) {
		UT_ASSERT_EQ(cluster_mx_history_graph_next(g, &edge, &read), CMXH_PENDING);
		UT_ASSERT(!read);
		UT_ASSERT_EQ(cluster_mx_history_graph_next(g, &edge, &read), CMXH_PENDING);
		UT_ASSERT(read);
		for (unsigned n = 0; n < 8; ++n) {
			UT_ASSERT_EQ(cluster_gcs_block_mx_history_prepare_routed(&edge, &held[n]),
						 CMXH_COMPLETE);
		}
		UT_ASSERT_EQ(cluster_gcs_block_mx_history_prepare_routed(&edge, &wire), CMXH_PENDING);
		UT_ASSERT_EQ(memcmp(&wire, &zero, sizeof(wire)), 0);
		for (unsigned n = 0; n < 8; ++n)
			cluster_gcs_block_mx_history_cancel(&held[n]);
		UT_ASSERT_EQ(cluster_gcs_block_mx_history_prepare_routed(&edge, &wire), CMXH_COMPLETE);
		if (cluster_mx_history_request_decode(&wire, sizeof(wire), 2, 1, 9, &routed)) {
			UT_ASSERT_EQ(routed.forward.requester_backend_id, MyBackendId);
			UT_ASSERT_EQ(routed.forward.original_requester_node, cluster_node_id);
			UT_ASSERT_EQ(routed.forward.master_node, 1);
			UT_ASSERT_EQ(routed.requester_capability_generation, 12);
			UT_ASSERT_EQ(routed.origin_capability_generation, 43);
			UT_ASSERT_EQ(routed.forward.tag.blockNum, GcsBlockUndoFetchTagMake(257, 7).blockNum);
		} else
			UT_ASSERT(false);
		cluster_gcs_block_mx_history_cancel(&wire);
		edge.backend_incarnation++;
		UT_ASSERT_EQ(cluster_gcs_block_mx_history_prepare_routed(&edge, &wire), CMXH_UNKNOWN);
		UT_ASSERT_EQ(memcmp(&wire, &zero, sizeof(wire)), 0);
	}
	graph_clean(g);
}
int
main(void)
{
	UT_PLAN(14);
	UT_RUN(graph_single_root_real_landing);
	UT_RUN(graph_global_nonpage_and_shared_prior);
	UT_RUN(graph_multiple_roots_share_done_node);
	UT_RUN(graph_control_and_v1_are_distinct);
	UT_RUN(graph_global_cycle_rejected);
	UT_RUN(graph_prior_cycle_rejected);
	UT_RUN(graph_conflicting_owner_shared_address_rejected);
	UT_RUN(graph_shared_address_new_head_constraint_rechecked);
	UT_RUN(graph_missing_predecessor_is_not_a_tail);
	UT_RUN(graph_no_fixed_depth_and_one_fetch_per_address);
	UT_RUN(graph_allocation_failure_cannot_complete_partial_graph);
	UT_RUN(graph_peer_claims_must_match_exact_record);
	UT_RUN(graph_pure_decode_or_consumed_result_cannot_enter);
	UT_RUN(graph_routes_actual_backend_and_capacity);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
