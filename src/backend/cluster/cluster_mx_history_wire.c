/*-------------------------------------------------------------------------
 * cluster_mx_history_wire.c
 *    Strict history carriage; decoded bytes remain private compare inputs.
 * Portions Copyright (c) 2026, pgrac contributors
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "cluster/cluster_mx_history_wire.h"
#include "cluster/cluster_semantic_activation.h"
#include "cluster/cluster_uba.h"
#include "port/pg_crc32c.h"

#define HREAD16(p) ClusterR4WireReadU16(p)
#define HREAD32(p) ClusterR4WireReadU32(p)
#define HREAD64(p) ClusterR4WireReadU64(p)
#define HWRITE16(p, v) ClusterR4WireWriteU16(p, v)
#define HWRITE32(p, v) ClusterR4WireWriteU32(p, v)
#define HWRITE64(p, v) ClusterR4WireWriteU64(p, v)

static bool
history_zero(const void *bytes, Size n)
{
	const uint8 *p = bytes;
	for (Size i = 0; i < n; i++)
		if (p[i] != 0)
			return false;
	return true;
}

static uint32
history_crc(const void *bytes, Size n)
{
	pg_crc32c crc;
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, bytes, n);
	FIN_CRC32C(crc);
	return crc;
}

static void
history_uba_write(uint8 *p, UBA uba)
{
	HWRITE64(p, uba.raw[0]);
	HWRITE64(p + 8, uba.raw[1]);
}

static UBA
history_uba_read(const uint8 *p)
{
	UBA uba = { { HREAD64(p), HREAD64(p + 8) } };
	return uba;
}

static bool
history_address(UBA uba)
{
	uint32 segment, block;
	uint16 slot, row;
	return uba_decode_record(uba, &segment, &block, &slot, &row);
}

static void
history_itl_write(uint8 *p, const ClusterItlSlotData *s)
{
	HWRITE32(p, s->xid);
	HWRITE16(p + 4, s->wrap);
	p[6] = s->flags;
	p[7] = s->lock_count;
	history_uba_write(p + 8, s->undo_segment_head);
	HWRITE64(p + 24, s->commit_scn);
	HWRITE64(p + 32, s->write_scn);
	HWRITE64(p + 40, s->first_change_lsn);
}

static void
history_itl_read(const uint8 *p, ClusterItlSlotData *s)
{
	s->xid = HREAD32(p);
	s->wrap = HREAD16(p + 4);
	s->flags = p[6];
	s->lock_count = p[7];
	s->undo_segment_head = history_uba_read(p + 8);
	s->commit_scn = HREAD64(p + 24);
	s->write_scn = HREAD64(p + 32);
	s->first_change_lsn = HREAD64(p + 40);
}

static bool
history_itl_valid(const ClusterItlSlotData *s)
{
	return TransactionIdIsNormal(s->xid) && s->wrap <= TT_WRAP_MAX
		   && ((s->flags >= ITL_FLAG_ACTIVE && s->flags <= ITL_FLAG_NEEDS_CLEANOUT)
			   || ITL_FLAG_IS_LOCK_ONLY(s->flags))
		   && history_address(s->undo_segment_head) && SCN_VALID(s->write_scn);
}

static bool
history_physical_valid(const ClusterTTSlotPhysicalLocator *p)
{
	return p->segment_id > 0 && p->segment_id <= UINT16_MAX
		   && (p->segment_id - 1) / CLUSTER_UNDO_SEGS_PER_INSTANCE < UNDO_OWNER_INSTANCE_MAX
		   && TransactionIdIsNormal(p->xid) && p->slot_offset < TT_SLOTS_PER_SEGMENT
		   && p->wrap <= TT_WRAP_MAX && p->reserved32 == 0;
}

static void
history_physical_write(uint8 *p, const ClusterTTSlotPhysicalLocator *s)
{
	HWRITE32(p, s->segment_id);
	HWRITE32(p + 4, s->xid);
	HWRITE16(p + 8, s->slot_offset);
	HWRITE16(p + 10, s->wrap);
	/* The final four bytes stay zero; they are not an extension slot. */
}

static void
history_physical_read(const uint8 *p, ClusterTTSlotPhysicalLocator *s)
{
	s->segment_id = HREAD32(p);
	s->xid = HREAD32(p + 4);
	s->slot_offset = HREAD16(p + 8);
	s->wrap = HREAD16(p + 10);
	s->reserved32 = HREAD32(p + 12);
}

static bool
history_request_valid(const ClusterMxHistoryRequest *r, int32 source, int32 origin, uint64 epoch)
{
	uint32 segment, block;
	uint16 slot, row;
	BufferTag expected;
	bool owner, generation;
	bool creator = r != NULL && r->edge_kind == CMXH_CREATOR_ROOT;

	if (r == NULL || source < 0 || source >= RESOURCE_X_PROTOCOL_NODE_LIMIT || origin < 0
		|| origin >= RESOURCE_X_PROTOCOL_NODE_LIMIT || r->forward.original_requester_node != source
		|| r->forward.master_node != origin || r->forward.requester_backend_id <= 0
		|| r->forward.request_id == 0 || r->forward.epoch != epoch
		|| r->forward.transition_id != PCM_TRANS_N_TO_S
		|| !history_zero(r->forward.expected_pi_watermark_scn_bytes, 8)
		|| !history_zero(r->forward.reserved_0, 7)
		|| !uba_decode_record(r->locator.uba, &segment, &block, &slot, &row)
		|| uba_origin_node_id(r->locator.uba) != origin || !TransactionIdIsNormal(r->locator.xid)
		|| (r->locator.tt_wrap != TT_WRAP_INVALID && r->locator.tt_wrap > TT_WRAP_MAX)
		|| r->edge_kind < CMXH_ROOT || r->edge_kind > CMXH_CREATOR_ROOT || r->root_ordinal >= 8
		|| (r->flags
			& ~(CLUSTER_MX_HISTORY_EXPECT_OWNER | CLUSTER_MX_HISTORY_EXPECT_DATA_GENERATION
				| CLUSTER_MX_HISTORY_CANCEL))
			   != 0
		|| r->collector_id == 0 || r->collector_id == UINT64_MAX || r->capture_generation == 0
		|| r->capture_generation == UINT64_MAX || r->backend_incarnation == 0
		|| r->backend_incarnation == UINT64_MAX || r->requester_capability_generation == 0
		|| r->origin_capability_generation == 0
		|| r->feature_bit != CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1 || r->admission_generation == 0
		|| r->admission_generation == UINT64_MAX || !SCN_VALID(r->read_scn)
		|| r->read_epoch != epoch || r->snapshot_identity == 0
		|| (creator ? (r->retained_floor != 0 || r->descriptor_hash != 0 || r->multixact_id != 0
					   || r->member_count != 0 || r->requester_owner.flags != 0
					   || r->read_scn == UINT64_MAX)
					: (!SCN_VALID(r->retained_floor)
					   || scn_local(r->retained_floor) > scn_local(r->read_scn)
					   || r->multixact_id == 0 || r->member_count == 0 || r->member_count > 256))
		|| !RelFileNumberIsValid(r->target_locator.relNumber) || r->target_fork != MAIN_FORKNUM
		|| r->target_block == InvalidBlockNumber || !OffsetNumberIsValid(r->target_offset)
		|| r->target_offset > MaxHeapTuplesPerPage
		|| !ClusterR4CrOwnerForRequester(&r->requester_owner, source)
		|| !history_itl_valid(&r->root))
		return false;
	expected = GcsBlockUndoFetchTagMake(segment, block);
	if (!BufferTagsEqual(&expected, &r->forward.tag))
		return false;
	owner = (r->flags & CLUSTER_MX_HISTORY_EXPECT_OWNER) != 0;
	generation = (r->flags & CLUSTER_MX_HISTORY_EXPECT_DATA_GENERATION) != 0;
	if (owner) {
		if (!history_physical_valid(&r->expected_owner) || r->expected_owner.slot_offset != slot
			|| (r->expected_owner.segment_id - 1) / CLUSTER_UNDO_SEGS_PER_INSTANCE != origin
			|| r->expected_owner_generation == UINT32_MAX
			|| !FullTransactionIdIsNormal(r->expected_top_fxid)
			|| U64FromFullTransactionId(r->expected_top_fxid) == UINT64_MAX
			|| XidFromFullTransactionId(r->expected_top_fxid) != r->expected_owner.xid
			|| r->locator.xid != r->expected_owner.xid
			|| (r->locator.tt_wrap != TT_WRAP_INVALID
				&& r->locator.tt_wrap != r->expected_owner.wrap))
			return false;
	} else if (!history_zero(&r->expected_owner, sizeof(r->expected_owner))
			   || r->expected_owner_generation != 0
			   || U64FromFullTransactionId(r->expected_top_fxid) != 0)
		return false;
	if (generation ? r->expected_data_generation == UINT32_MAX : r->expected_data_generation != 0)
		return false;
	if (r->edge_kind == CMXH_GLOBAL_PREV) {
		if (!owner || r->locator.tt_wrap == TT_WRAP_INVALID || r->edge_itl_slot != 255
			|| r->locator.itl_slot_index != 255 || r->locator.itl_kind != 0
			|| !history_zero(&r->edge, sizeof(r->edge)))
			return false;
	} else if (r->edge_itl_slot >= 8 || r->locator.itl_slot_index != r->edge_itl_slot
			   || !history_itl_valid(&r->edge) || r->edge.xid != r->locator.xid
			   || r->edge.flags != r->locator.itl_kind
			   || !cluster_undo_record_uba_equal(r->edge.undo_segment_head, r->locator.uba))
		return false;
	if (r->edge_kind == CMXH_ROOT || creator)
		return UBA_is_invalid(r->parent) && r->parent_data_generation == 0
			   && r->parent_record_crc == 0 && r->root_ordinal == r->edge_itl_slot
			   && memcmp(&r->root, &r->edge, sizeof(r->root)) == 0;
	return history_address(r->parent) && !cluster_undo_record_uba_equal(r->parent, r->locator.uba)
		   && r->parent_data_generation != UINT32_MAX;
}

/* Explicit field maps: storage order and width are independent of native
 * struct padding. The same map is used only for mechanical LE conversion. */
/* clang-format off */
#define HISTORY_REQUEST_FIELDS(X) \
	X(8, 0, forward.request_id) \
	X(8, 8, forward.epoch) \
	X(4, 16, forward.tag.spcOid) \
	X(4, 20, forward.tag.dbOid) \
	X(4, 24, forward.tag.relNumber) \
	X(4, 28, forward.tag.forkNum) \
	X(4, 32, forward.tag.blockNum) \
	X(4, 36, forward.original_requester_node) \
	X(4, 40, forward.requester_backend_id) \
	X(4, 44, forward.master_node) \
	X(1, 48, forward.transition_id) \
	X(4, 84, locator.xid) \
	X(2, 88, locator.tt_wrap) \
	X(1, 90, locator.itl_kind) \
	X(1, 91, locator.itl_slot_index) \
	X(1, 100, edge_kind) \
	X(1, 101, root_ordinal) \
	X(2, 102, flags) \
	X(8, 104, collector_id) \
	X(8, 112, capture_generation) \
	X(8, 120, backend_incarnation) \
	X(4, 128, requester_capability_generation) \
	X(4, 132, origin_capability_generation) \
	X(8, 136, feature_bit) \
	X(8, 144, admission_generation) \
	X(8, 152, read_scn) \
	X(8, 160, read_epoch) \
	X(8, 168, retained_floor) \
	X(8, 176, snapshot_identity) \
	X(8, 184, descriptor_hash) \
	X(4, 192, multixact_id) \
	X(2, 196, member_count) \
	X(1, 199, edge_itl_slot) \
	X(4, 200, target_locator.spcOid) \
	X(4, 204, target_locator.dbOid) \
	X(4, 208, target_locator.relNumber) \
	X(4, 212, target_fork) \
	X(4, 216, target_block) \
	X(2, 220, target_offset) \
	X(4, 392, expected_owner_generation) \
	X(4, 396, expected_data_generation) \
	X(4, 408, parent_data_generation) \
	X(4, 412, parent_record_crc)
/* clang-format on */

static uint64
history_read(const uint8 *p, unsigned n)
{
	uint64 value = 0;
	for (unsigned i = 0; i < n; i++)
		value |= ((uint64)p[i]) << (8 * i);
	return value;
}

static void
history_write(uint8 *p, unsigned n, uint64 value)
{
	for (unsigned i = 0; i < n; i++)
		p[i] = (uint8)(value >> (8 * i));
}

bool
cluster_mx_history_request_encode(const ClusterMxHistoryRequest *r,
								  ClusterMxHistoryRequestWire *out)
{
	uint8 *p;
	ClusterR4CrOwnerWire owner;
	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	if (r == NULL
		|| !history_request_valid(r, r->forward.original_requester_node, r->forward.master_node,
								  r->forward.epoch)
		|| !ClusterR4CrOwnerWireSet(&owner, &r->requester_owner))
		return false;
	p = out->bytes;
#define WRITE_FIELD(n, off, field) history_write(p + off, n, r->field);
	HISTORY_REQUEST_FIELDS(WRITE_FIELD)
#undef WRITE_FIELD
	p[64] = CLUSTER_R4_WIRE_VERSION;
	p[65] = CLUSTER_MX_HISTORY_KIND;
	history_uba_write(p + 68, r->locator.uba);
	HWRITE32(p + 92, r->expected_data_generation);
	HWRITE16(p + 96, r->edge_kind == CMXH_CREATOR_ROOT ? CLUSTER_FOREIGN_CREATOR_HISTORY_VERSION
													   : CLUSTER_MX_HISTORY_VERSION);
	HWRITE16(p + 98, 328);
	p[198] = r->edge_kind == CMXH_CREATOR_ROOT ? 1 : 8;
	memcpy(p + 224, owner.bytes, 40);
	history_itl_write(p + 264, &r->root);
	history_itl_write(p + 312, &r->edge);
	history_uba_write(p + 360, r->parent);
	history_physical_write(p + 376, &r->expected_owner);
	HWRITE64(p + 400, U64FromFullTransactionId(r->expected_top_fxid));
	HWRITE32(p + 416, history_crc(p, 416));
	return true;
}

bool
cluster_mx_history_request_decode(const void *payload, uint32 length, int32 source, int32 origin,
								  uint64 epoch, ClusterMxHistoryRequest *out)
{
	ClusterMxHistoryRequest r = { 0 };
	ClusterR4CrOwnerWire owner;
	const uint8 *p = payload;
	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	if (p == NULL || length != 424 || p[64] != CLUSTER_R4_WIRE_VERSION
		|| p[65] != CLUSTER_MX_HISTORY_KIND || HREAD16(p + 66) != 0 || !history_zero(p + 49, 15)
		|| HREAD16(p + 96)
			   != (p[100] == CMXH_CREATOR_ROOT ? CLUSTER_FOREIGN_CREATOR_HISTORY_VERSION
											   : CLUSTER_MX_HISTORY_VERSION)
		|| HREAD16(p + 98) != 328 || p[198] != (p[100] == CMXH_CREATOR_ROOT ? 1 : 8)
		|| HREAD16(p + 222) != 0 || HREAD32(p + 420) != 0
		|| HREAD32(p + 416) != history_crc(p, 416))
		return false;
#define READ_FIELD(n, off, field) r.field = history_read(p + off, n);
	HISTORY_REQUEST_FIELDS(READ_FIELD)
#undef READ_FIELD
	r.locator.uba = history_uba_read(p + 68);
	memcpy(owner.bytes, p + 224, 40);
	if (!ClusterR4CrOwnerWireGet(&owner, &r.requester_owner))
		return false;
	history_itl_read(p + 264, &r.root);
	history_itl_read(p + 312, &r.edge);
	r.parent = history_uba_read(p + 360);
	history_physical_read(p + 376, &r.expected_owner);
	r.expected_top_fxid = FullTransactionIdFromU64(HREAD64(p + 400));
	if (HREAD32(p + 92) != r.expected_data_generation
		|| !history_request_valid(&r, source, origin, epoch))
		return false;
	*out = r;
	return true;
}

static bool
history_request_wire_valid(const ClusterMxHistoryRequestWire *w)
{
	ClusterMxHistoryRequest r;
	uint32 segment, block;
	uint16 slot, row;
	UBA uba;
	if (w == NULL)
		return false;
	uba = history_uba_read(w->bytes + 68);
	if (!uba_decode_record(uba, &segment, &block, &slot, &row))
		return false;
	return cluster_mx_history_request_decode(w, 424, HREAD32(w->bytes + 36),
											 uba_origin_node_id(uba), HREAD64(w->bytes + 8), &r);
}

bool
cluster_mx_history_request_cancel(const ClusterMxHistoryRequestWire *request,
								  ClusterMxHistoryRequestWire *out)
{
	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	if (!history_request_wire_valid(request))
		return false;
	*out = *request;
	HWRITE16(out->bytes + 102, HREAD16(out->bytes + 102) | CLUSTER_MX_HISTORY_CANCEL);
	HWRITE32(out->bytes + 416, history_crc(out->bytes, 416));
	return true;
}

bool
cluster_mx_history_request_same(const ClusterMxHistoryRequestWire *left,
								const ClusterMxHistoryRequestWire *right, bool allow_cancel)
{
	ClusterMxHistoryRequestWire a, b;
	if (!history_request_wire_valid(left) || !history_request_wire_valid(right))
		return false;
	if (!allow_cancel)
		return memcmp(left, right, sizeof(*left)) == 0;
	a = *left;
	b = *right;
	HWRITE16(a.bytes + 102, HREAD16(a.bytes + 102) & ~CLUSTER_MX_HISTORY_CANCEL);
	HWRITE16(b.bytes + 102, HREAD16(b.bytes + 102) & ~CLUSTER_MX_HISTORY_CANCEL);
	return memcmp(a.bytes, b.bytes, 416) == 0;
}

static bool
history_peer_valid(const ClusterMxHistoryPeer *p)
{
	bool child;
	if (p == NULL
		|| (p->flags != CLUSTER_MX_HISTORY_ORDERED
			&& p->flags != (CLUSTER_MX_HISTORY_ORDERED | CLUSTER_MX_HISTORY_HAS_GRANT))
		|| p->record_class < CMXH_V1_TOP || p->record_class > CMXH_FOREIGN_CREATOR
		|| !history_physical_valid(&p->canonical) || p->canonical_generation == UINT32_MAX
		|| !FullTransactionIdIsNormal(p->top_fxid)
		|| U64FromFullTransactionId(p->top_fxid) == UINT64_MAX
		|| XidFromFullTransactionId(p->top_fxid) != p->canonical.xid
		|| !FullTransactionIdIsNormal(p->operation_fxid)
		|| U64FromFullTransactionId(p->operation_fxid) == UINT64_MAX
		|| p->outcome == CLUSTER_TX_UNKNOWN
		|| !cluster_tx_outcome_proof_is_valid(p->outcome, p->proof_kind)
		|| (p->proof_kind != CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG
			&& p->proof_kind != CLUSTER_TX_PROOF_ORIGIN_SUBTRANS_TOP
			&& p->proof_kind != CLUSTER_TX_PROOF_ORIGIN_TWOPHASE
			&& p->proof_kind != CLUSTER_TX_PROOF_ORIGIN_MULTIXACT)
		|| (p->outcome == CLUSTER_TX_COMMITTED ? !SCN_VALID(p->commit_scn) : p->commit_scn != 0)
		|| !history_address(p->record)
		|| (!UBA_is_invalid(p->previous)
			&& (!history_address(p->previous)
				|| cluster_undo_record_uba_equal(p->record, p->previous)))
		|| p->data_generation == UINT32_MAX || p->data_lsn == InvalidXLogRecPtr
		|| p->admission_generation == 0 || p->admission_generation == UINT64_MAX
		|| ((p->flags & CLUSTER_MX_HISTORY_HAS_GRANT) != 0) != (p->ctrc_grant != 0))
		return false;
	if (p->record_class == CMXH_FOREIGN_CREATOR)
		return p->flags == CLUSTER_MX_HISTORY_ORDERED && p->ctrc_grant == 0
			   && p->commit_scn != UINT64_MAX && p->outcome == CLUSTER_TX_COMMITTED
			   && p->effect == CLUSTER_SAVEPOINT_EFFECT_LIVE
			   && p->proof_kind == CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG
			   && FullTransactionIdEquals(p->operation_fxid, p->top_fxid);
	child = p->record_class != CMXH_V1_TOP;
	if (child ? (p->operation_sequence == 0
				 || U64FromFullTransactionId(p->operation_fxid)
						<= U64FromFullTransactionId(p->top_fxid))
			  : (p->operation_sequence != 0
				 || !FullTransactionIdEquals(p->operation_fxid, p->top_fxid)))
		return false;
	if (p->record_class == CMXH_ROLLBACK_CONTROL)
		return p->effect == 0 && p->ctrc_grant == 0;
	return (p->effect == CLUSTER_SAVEPOINT_EFFECT_LIVE
			|| p->effect == CLUSTER_SAVEPOINT_EFFECT_DISCARDED)
		   && (p->effect == CLUSTER_SAVEPOINT_EFFECT_DISCARDED
				   ? p->ctrc_grant == 0
				   : (p->outcome != CLUSTER_TX_IN_PROGRESS || p->ctrc_grant != 0));
}

/* clang-format off */
#define HISTORY_PEER_FIELDS(X) \
	X(4, 8, flags) \
	X(1, 12, record_class) \
	X(1, 13, outcome) \
	X(1, 14, effect) \
	X(1, 15, proof_kind) \
	X(4, 16, canonical.segment_id) \
	X(4, 20, canonical.xid) \
	X(2, 24, canonical.slot_offset) \
	X(2, 26, canonical.wrap) \
	X(4, 28, canonical_generation) \
	X(8, 48, operation_sequence) \
	X(4, 88, data_generation) \
	X(4, 92, data_crc) \
	X(8, 96, data_lsn) \
	X(8, 104, commit_scn) \
	X(8, 112, formation_epoch) \
	X(8, 120, admission_generation) \
	X(4, 128, ctrc_grant)
/* clang-format on */

bool
cluster_mx_history_peer_encode(const ClusterMxHistoryPeer *peer, ClusterMxHistoryPeerWire *out)
{
	uint8 *p;
	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	if (!history_peer_valid(peer))
		return false;
	p = out->bytes;
	HWRITE32(p, UINT32_C(0x3158484d));
	HWRITE16(p + 4, peer->record_class == CMXH_FOREIGN_CREATOR
						? CLUSTER_FOREIGN_CREATOR_HISTORY_VERSION
						: CLUSTER_MX_HISTORY_VERSION);
	HWRITE16(p + 6, 136);
#define WRITE_FIELD(n, off, field) history_write(p + off, n, peer->field);
	HISTORY_PEER_FIELDS(WRITE_FIELD)
#undef WRITE_FIELD
	HWRITE64(p + 32, U64FromFullTransactionId(peer->top_fxid));
	HWRITE64(p + 40, U64FromFullTransactionId(peer->operation_fxid));
	history_uba_write(p + 56, peer->record);
	history_uba_write(p + 72, peer->previous);
	return true;
}

bool
cluster_mx_history_peer_decode(const ClusterMxHistoryPeerWire *wire, ClusterMxHistoryPeer *out)
{
	ClusterMxHistoryPeer peer = { 0 };
	const uint8 *p;
	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	if (wire == NULL)
		return false;
	p = wire->bytes;
	if (HREAD32(p) != UINT32_C(0x3158484d)
		|| HREAD16(p + 4)
			   != (p[12] == CMXH_FOREIGN_CREATOR ? CLUSTER_FOREIGN_CREATOR_HISTORY_VERSION
												 : CLUSTER_MX_HISTORY_VERSION)
		|| HREAD16(p + 6) != 136 || HREAD32(p + 132) != 0)
		return false;
#define READ_FIELD(n, off, field) peer.field = history_read(p + off, n);
	HISTORY_PEER_FIELDS(READ_FIELD)
#undef READ_FIELD
	peer.top_fxid = FullTransactionIdFromU64(HREAD64(p + 32));
	peer.operation_fxid = FullTransactionIdFromU64(HREAD64(p + 40));
	peer.record = history_uba_read(p + 56);
	peer.previous = history_uba_read(p + 72);
	if (!history_peer_valid(&peer))
		return false;
	*out = peer;
	return true;
}

static bool
history_reply_binding(const ClusterMxHistoryRequest *r, const ClusterMxHistoryPeer *p,
					  const uint8 *data, const uint8 *auth)
{
	uint32 segment, block;
	uint16 slot, row;

	if ((r->edge_kind == CMXH_CREATOR_ROOT) != (p->record_class == CMXH_FOREIGN_CREATOR)
		|| (r->flags & CLUSTER_MX_HISTORY_CANCEL) != 0
		|| !uba_decode_record(r->locator.uba, &segment, &block, &slot, &row)
		|| p->canonical.slot_offset != slot
		|| (p->canonical.segment_id - 1) / CLUSTER_UNDO_SEGS_PER_INSTANCE != r->forward.master_node
		|| !cluster_undo_record_uba_equal(r->locator.uba, p->record)
		|| r->locator.xid != p->canonical.xid
		|| (r->locator.tt_wrap != TT_WRAP_INVALID && r->locator.tt_wrap != p->canonical.wrap)
		|| r->read_epoch != p->formation_epoch || r->admission_generation != p->admission_generation
		|| HREAD64(auth) != p->canonical_generation || !SCN_VALID(HREAD64(auth + 8))
		|| scn_local(HREAD64(auth + 8)) < scn_local(r->read_scn)
		|| p->data_crc != history_crc(data, 8192))
		return false;
	if ((r->flags & CLUSTER_MX_HISTORY_EXPECT_DATA_GENERATION) != 0
		&& r->expected_data_generation != p->data_generation)
		return false;
	if ((r->flags & CLUSTER_MX_HISTORY_EXPECT_OWNER) != 0
		&& (memcmp(&r->expected_owner, &p->canonical, sizeof(p->canonical)) != 0
			|| r->expected_owner_generation != p->canonical_generation
			|| !FullTransactionIdEquals(r->expected_top_fxid, p->top_fxid)))
		return false;
	return true;
}

bool
cluster_mx_history_reply_encode(const ClusterMxHistoryRequestWire *request, int32 origin,
								XLogRecPtr authority_lsn, const uint8 data[8192],
								const uint8 auth[16], const ClusterMxHistoryPeerWire *peer,
								ClusterMxHistoryReplyWire *out)
{
	ClusterMxHistoryRequest r;
	ClusterMxHistoryPeer p;
	uint8 *b;
	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	if (request == NULL || data == NULL || auth == NULL || peer == NULL || authority_lsn == 0
		|| !cluster_mx_history_request_decode(request, 424, HREAD32(request->bytes + 36), origin,
											  HREAD64(request->bytes + 8), &r)
		|| !cluster_mx_history_peer_decode(peer, &p) || !history_reply_binding(&r, &p, data, auth))
		return false;
	b = out->bytes;
	HWRITE64(b, r.forward.request_id);
	HWRITE64(b + 8, authority_lsn);
	HWRITE64(b + 16, r.forward.epoch);
	HWRITE32(b + 28, origin);
	HWRITE32(b + 32, r.forward.requester_backend_id);
	b[36] = PCM_TRANS_N_TO_S;
	b[37] = CLUSTER_MX_HISTORY_REPLY_STATUS;
	HWRITE32(b + 38, GCS_BLOCK_REPLY_NO_FORWARDING_MASTER);
	memcpy(b + 48, request, 424);
	memcpy(b + 472, data, 8192);
	memcpy(b + 8664, auth, 16);
	memcpy(b + 8680, peer, 136);
	HWRITE32(b + 24, history_crc(b + 48, 8768));
	return true;
}

bool
cluster_mx_history_reply_decode(const void *payload, uint32 length, int32 source,
								const ClusterMxHistoryRequestWire *expected, uint8 *status,
								ClusterMxHistoryPeerResult *out)
{
	const uint8 *b = payload;
	ClusterMxHistoryRequest r;
	ClusterMxHistoryPeer p;
	ClusterMxHistoryPeerWire wire;
	uint8 code;
	if (status != NULL)
		*status = 0;
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (b == NULL || expected == NULL || out == NULL || status == NULL || length < 48
		|| !cluster_mx_history_request_decode(expected, 424, HREAD32(expected->bytes + 36), source,
											  HREAD64(expected->bytes + 8), &r)
		|| (r.flags & CLUSTER_MX_HISTORY_CANCEL) != 0 || HREAD64(b) != r.forward.request_id
		|| HREAD64(b + 16) != r.forward.epoch || HREAD32(b + 28) != (uint32)source
		|| HREAD32(b + 32) != (uint32)r.forward.requester_backend_id || b[36] != PCM_TRANS_N_TO_S
		|| HREAD32(b + 38) != (uint32)GCS_BLOCK_REPLY_NO_FORWARDING_MASTER
		|| !history_zero(b + 42, 6))
		return false;
	code = b[37];
	if (code == GCS_BLOCK_REPLY_R4_DENIED || code == GCS_BLOCK_REPLY_R4_RETRYABLE_HOLDER_MOVED) {
		if (length != 48 || HREAD64(b + 8) != 0 || HREAD32(b + 24) != 0)
			return false;
		*status = code;
		return true;
	}
	if (code != CLUSTER_MX_HISTORY_REPLY_STATUS || length != 8816 || HREAD64(b + 8) == 0
		|| HREAD32(b + 24) != history_crc(b + 48, 8768) || memcmp(b + 48, expected, 424) != 0)
		return false;
	memcpy(&wire, b + 8680, 136);
	if (!cluster_mx_history_peer_decode(&wire, &p)
		|| !history_reply_binding(&r, &p, b + 472, b + 8664))
		return false;
	out->request = *expected;
	memcpy(out->data, b + 472, 8192);
	memcpy(out->auth, b + 8664, 16);
	out->peer = wire;
	/* Only the original slot/admission owner can publish admitted=1. */
	*status = code;
	return true;
}
