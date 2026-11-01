/* Foreign-MX metadata codec. No transport, retained scope or visibility grant. */
#include "postgres.h"
#include "cluster/cluster_multixact_range_wire.h"
#include "cluster/cluster_multixact_range.h"
#include "cluster/cluster_gcs_block.h"
#include "port/pg_crc32c.h"

static bool
range_context_valid(const ClusterMxRangeWireContext *ctx)
{
	return ctx != NULL && ctx->request_id != 0 && ctx->epoch > 0 && ctx->epoch <= UINT32_MAX
		&& ctx->key.origin_node_id < CLUSTER_MAX_NODES && ctx->key.reserved16 == 0
		&& ctx->key.reserved32 == 0 && MultiXactIdIsValid(ctx->key.multixact_id)
		&& ctx->key.cluster_epoch == ctx->epoch
		&& cluster_foreign_mx_format_registered_v1(ctx->format_id, ctx->version, ctx->loaded_catalog)
		&& (ctx->peer_capabilities & CLUSTER_FOREIGN_MX_REQUIRED_CAPABILITIES)
			== CLUSTER_FOREIGN_MX_REQUIRED_CAPABILITIES
		&& ctx->peer_generation != 0 && ctx->peer_generation != UINT32_MAX
		&& ctx->requested_cap > 0 && ctx->requested_cap <= CLUSTER_MX_COMPOSITION_PAGE_MEMBERS
		&& ctx->source != NULL && ctx->source_bytes > 0 && ctx->source_bytes <= BLCKSZ;
}

static bool
range_frame_size(Size source_bytes, uint32 count, Size entry_bytes,
				 Size *body, uint32 *bytes)
{
	uint64 prefix = (uint64)sizeof(ClusterMxRangeWireHeader) + source_bytes;
	uint64 length;

	if (prefix > BLCKSZ)
		return false;
	prefix = MAXALIGN(prefix);
	length = prefix + (uint64)count * entry_bytes;
	if (length > BLCKSZ)
		return false;
	*body = (Size)prefix;
	*bytes = (uint32)length;
	return true;
}

static bool
range_member_valid(const ClusterCurrentMxMemberDesc *member)
{
	return TransactionIdIsNormal(member->xid) && member->member_status <= MaxMultiXactStatus
		&& member->reserved8[0] == 0 && member->reserved8[1] == 0 && member->reserved8[2] == 0;
}

static bool
range_members_valid(const void *members, uint32 count)
{
	uint32 i, updaters = 0;

	if (count != 0 && members == NULL)
		return false;
	for (i = 0; i < count; i++) {
		ClusterCurrentMxMemberDesc member;

		memcpy(&member, (const char *)members + (Size)i * sizeof(member), sizeof(member));
		if (!range_member_valid(&member)
			|| (ISUPDATE_from_mxstatus(member.member_status) && ++updaters > 1))
			return false;
	}
	return true;
}

static bool
range_positions_valid(uint32 total, uint32 first, uint32 count, uint32 next,
					  bool last, bool skipped, uint32 cap)
{
	return total > 0 && (uint64)total <= (uint64)INT32_MAX + 1
		&& (uint64)count + (skipped ? 1 : 0) <= cap && next > first && next <= total
		&& (uint64)first + count + (skipped ? 1 : 0) == next
		&& last == (next == total) && (count > 0 || (skipped && last));
}

static void
range_header_init(ClusterMxRangeWireHeader *h, const ClusterMxRangeWireContext *ctx,
				  uint8 kind, uint32 bytes)
{
	memset(h, 0, sizeof(*h));
	h->format_id = ctx->format_id;
	h->version = ctx->version;
	h->wire_length = bytes;
	h->source_bytes = (uint32)ctx->source_bytes;
	h->request_id = ctx->request_id;
	h->epoch = ctx->epoch;
	h->key = ctx->key;
	h->peer_generation = ctx->peer_generation;
	h->kind = kind;
}

static void
range_frame_prefix(void *frame, Size body, const ClusterMxRangeWireHeader *h,
				   const ClusterMxRangeWireContext *ctx)
{
	memset(frame, 0, body);
	memcpy(frame, h, sizeof(*h));
	memcpy((char *)frame + sizeof(*h), ctx->source, ctx->source_bytes);
}

static bool
range_header_read(const void *wire, uint32 bytes, const ClusterMxRangeWireContext *ctx,
				 ClusterMxRangeWireHeader *h, uint8 kind, Size entry_bytes, Size *body)
{
	uint32 expected;
	Size i;

	if (!range_context_valid(ctx) || wire == NULL || bytes < sizeof(*h) || bytes > BLCKSZ
		|| ((uintptr_t)wire % MAXIMUM_ALIGNOF) != 0)
		return false;
	memcpy(h, wire, sizeof(*h));
	if (h->format_id != ctx->format_id || h->version != ctx->version || h->wire_length != bytes
		|| h->source_bytes != ctx->source_bytes || h->request_id != ctx->request_id
		|| h->epoch != ctx->epoch || memcmp(&h->key, &ctx->key, sizeof(h->key)) != 0
		|| h->peer_generation != ctx->peer_generation || h->kind != kind
		|| h->reserved8 != 0 || h->reserved32[0] != 0 || h->reserved32[1] != 0
		|| h->count > CLUSTER_MX_COMPOSITION_PAGE_MEMBERS
		|| !range_frame_size(h->source_bytes, h->count, entry_bytes, body, &expected)
		|| expected != bytes)
		return false;
	if (memcmp((const char *)wire + sizeof(*h), ctx->source, ctx->source_bytes) != 0)
		return false;
	for (i = sizeof(*h) + ctx->source_bytes; i < *body; i++)
		if (((const unsigned char *)wire)[i] != 0)
			return false;
	return true;
}

bool
cluster_mx_range_wire_encode_reply(const ClusterMxRangeWireContext *ctx,
	ClusterMxCompositionReadStatus status, const ClusterMxCompositionReply *reply,
	void *wire, uint32 capacity, uint32 *bytes)
{
	ClusterMxRangeWireHeader h;
	PGAlignedBlock frame;
	Size body;
	uint32 length, count = 0;

	if (bytes != NULL)
		*bytes = 0;
	if (!range_context_valid(ctx) || wire == NULL || bytes == NULL
		|| status < MXCOMP_READ_OK || status > MXCOMP_READ_UNKNOWN)
		return false;
	if (status == MXCOMP_READ_OK) {
		if (reply == NULL || reply->source == NULL || reply->source_bytes != ctx->source_bytes
			|| memcmp(reply->source, ctx->source, ctx->source_bytes) != 0
			|| reply->cap != ctx->requested_cap
			|| !range_positions_valid(reply->total, reply->first, reply->count, reply->next,
				reply->last, reply->skipped_zero, ctx->requested_cap)
			|| reply->member_bytes != (uint64)reply->count * sizeof(*reply->members)
			|| !range_members_valid(reply->members, reply->count))
			return false;
		count = reply->count;
	} else if (reply != NULL)
		return false;
	if (!range_frame_size(ctx->source_bytes, count, sizeof(ClusterCurrentMxMemberDesc), &body, &length)
		|| length > capacity)
		return false;
	range_header_init(&h, ctx, CLUSTER_MX_RANGE_REPLY, length);
	h.result = status;
	if (status == MXCOMP_READ_OK) {
		h.total = reply->total;
		h.first = reply->first;
		h.count = count;
		h.next = reply->next;
		h.flags = (reply->last ? CLUSTER_MX_RANGE_LAST : 0)
			| (reply->skipped_zero ? CLUSTER_MX_RANGE_SKIPPED_ZERO : 0);
	}
	range_frame_prefix(frame.data, body, &h, ctx);
	if (count != 0)
		memcpy(frame.data + body, reply->members, (Size)count * sizeof(*reply->members));
	memcpy(wire, frame.data, length);
	*bytes = length;
	return true;
}

ClusterMxCompositionReadStatus
cluster_mx_range_wire_decode_reply(const void *wire, uint32 bytes,
	const ClusterMxRangeWireContext *ctx, ClusterMxCompositionReply *reply)
{
	ClusterMxRangeWireHeader h;
	Size body;
	bool last, skipped;

	if (reply == NULL)
		return MXCOMP_READ_UNKNOWN;
	memset(reply, 0, sizeof(*reply));
	if (!range_header_read(wire, bytes, ctx, &h, CLUSTER_MX_RANGE_REPLY,
		sizeof(ClusterCurrentMxMemberDesc), &body) || h.result > MXCOMP_READ_UNKNOWN
		|| (h.flags & ~(CLUSTER_MX_RANGE_LAST | CLUSTER_MX_RANGE_SKIPPED_ZERO)) != 0)
		return MXCOMP_READ_UNKNOWN;
	if (h.result != MXCOMP_READ_OK) {
		if (h.total != 0 || h.first != 0 || h.count != 0 || h.next != 0 || h.flags != 0)
			return MXCOMP_READ_UNKNOWN;
		return (ClusterMxCompositionReadStatus)h.result;
	}
	last = (h.flags & CLUSTER_MX_RANGE_LAST) != 0;
	skipped = (h.flags & CLUSTER_MX_RANGE_SKIPPED_ZERO) != 0;
	if (!range_positions_valid(h.total, h.first, h.count, h.next, last, skipped, ctx->requested_cap)
		|| !range_members_valid((const char *)wire + body, h.count))
		return MXCOMP_READ_UNKNOWN;
	reply->source = (const char *)wire + sizeof(h);
	reply->source_bytes = ctx->source_bytes;
	reply->total = h.total;
	reply->first = h.first;
	reply->count = h.count;
	reply->next = h.next;
	reply->cap = ctx->requested_cap;
	reply->last = last;
	reply->skipped_zero = skipped;
	reply->members = h.count == 0 ? NULL : (const ClusterCurrentMxMemberDesc *)((const char *)wire + body);
	reply->member_bytes = (Size)h.count * sizeof(*reply->members);
	reply->frame_bytes = bytes;
	return MXCOMP_READ_OK;
}

bool
cluster_mx_range_wire_encode_proof_asks(const ClusterMxRangeWireContext *ctx,
	uint32 total, uint32 first, const ClusterCurrentMxMemberDesc *members, uint32 count,
	void *wire, uint32 capacity, uint32 *bytes)
{
	PGAlignedBlock frame;
	ClusterMxRangeWireHeader h;
	Size body;
	uint32 length, i, j;

	if (bytes != NULL)
		*bytes = 0;
	if (!range_context_valid(ctx) || wire == NULL || bytes == NULL || count == 0
		|| count > CLUSTER_MX_RANGE_PROOF_ASKS_PER_FRAME || total == 0 || total > INT32_MAX
		|| (uint64)first + count > total || !range_members_valid(members, count)
		|| !range_frame_size(ctx->source_bytes, count, sizeof(ClusterMxRangeProofAsk), &body, &length)
		|| length > capacity)
		return false;
	range_header_init(&h, ctx, CLUSTER_MX_RANGE_PROOF_ASKS, length);
	h.total = total;
	h.first = first;
	h.count = count;
	h.next = first + count;
	range_frame_prefix(frame.data, body, &h, ctx);
	for (i = 0; i < count; i++) {
		ClusterMxRangeProofAsk ask;
		ClusterCurrentMxMemberDesc member;

		memcpy(&member, (const char *)members + (Size)i * sizeof(member), sizeof(member));
		for (j = 0; j < i; j++) {
			ClusterCurrentMxMemberDesc previous;

			memcpy(&previous, (const char *)members + (Size)j * sizeof(previous), sizeof(previous));
			if (previous.xid == member.xid)
				return false;
		}
		memset(&ask, 0, sizeof(ask));
		ask.xid = member.xid;
		ask.member_ordinal = first + i;
		ask.member_status = member.member_status;
		memcpy(frame.data + body + (Size)i * sizeof(ask), &ask, sizeof(ask));
	}
	memcpy(wire, frame.data, length);
	*bytes = length;
	return true;
}

bool
cluster_mx_range_wire_decode_proof_asks(const void *wire, uint32 bytes,
	const ClusterMxRangeWireContext *ctx, ClusterMxRangeProofAsk *asks, uint32 asks_cap,
	uint32 *count, uint32 *total)
{
	ClusterMxRangeWireHeader h;
	ClusterMxRangeProofAsk decoded[CLUSTER_MX_RANGE_PROOF_ASKS_PER_FRAME];
	Size body;
	uint32 i, j, updaters = 0;

	if (count != NULL)
		*count = 0;
	if (total != NULL)
		*total = 0;
	if (asks_cap > CLUSTER_MX_RANGE_PROOF_ASKS_PER_FRAME)
		return false;
	if (asks != NULL)
		memset(asks, 0, (Size)asks_cap * sizeof(*asks));
	if (asks == NULL || asks_cap == 0 || count == NULL || total == NULL
		|| !range_header_read(wire, bytes, ctx, &h, CLUSTER_MX_RANGE_PROOF_ASKS,
			sizeof(*asks), &body) || h.result != MXCOMP_READ_OK || h.flags != 0
		|| h.total == 0 || h.total > INT32_MAX || h.count == 0 || h.count > asks_cap
		|| (uint64)h.first + h.count != h.next || h.next > h.total)
		return false;
	memcpy(decoded, (const char *)wire + body, (Size)h.count * sizeof(*asks));
	for (i = 0; i < h.count; i++) {
		ClusterMxRangeProofAsk *ask = &decoded[i];

		if (!TransactionIdIsNormal(ask->xid) || ask->member_status > MaxMultiXactStatus
			|| ask->member_ordinal != h.first + i
			|| ask->reserved[0] != 0 || ask->reserved[1] != 0 || ask->reserved[2] != 0
			|| (ISUPDATE_from_mxstatus(ask->member_status) && ++updaters > 1))
			return false;
		for (j = 0; j < i; j++)
			if (decoded[j].xid == ask->xid)
				return false;
	}
	memcpy(asks, decoded, (Size)h.count * sizeof(*asks));
	*count = h.count;
	*total = h.total;
	return true;
}

static bool
range_bytes_zero(const void *data, Size bytes)
{
	const uint8 *p = data;
	for (Size i = 0; i < bytes; i++)
		if (p[i] != 0)
			return false;
	return true;
}

bool
cluster_mx_range_proof_entry_valid(const ClusterMxFullMemberProof *entry, uint32 ordinal,
								   uint32 local, const ClusterCurrentMxMemberDesc *member)
{
	const ClusterCurrentMemberProof *p;
	static const ClusterCurrentMemberProofKey zero_key;
	ClusterTTStatusKey key;
	uint32 generation;
	uint16 wrap;
	if (entry == NULL || member == NULL || local >= CLUSTER_MX_PROOF_PAGE_MEMBERS
		|| entry->member_ordinal != ordinal || entry->origin_node_id >= CLUSTER_MAX_NODES
		|| entry->reserved16 != 0 || !range_member_valid(member))
		return false;
	p = &entry->proof;
	if (p->member_xid != member->xid || p->member_status != member->member_status
		|| p->member_ordinal != local
		|| !range_bytes_zero((const char *)p
								 + offsetof(ClusterCurrentMemberProof, origin_fullxid_v2)
								 + sizeof(p->origin_fullxid_v2),
							 sizeof(*p) - offsetof(ClusterCurrentMemberProof, origin_fullxid_v2)
								 - sizeof(p->origin_fullxid_v2)))
		return false;
	if (p->state == CCM_COMMITTED || p->state == CCM_ABORTED)
		return memcmp(&p->key, &zero_key, sizeof(zero_key)) == 0
			   && ClusterCurrentMemberProofGetCtrcGrant(p) == 0
			   && range_bytes_zero(p->origin_fullxid_v2, sizeof(p->origin_fullxid_v2))
			   && (p->state == CCM_COMMITTED ? SCN_VALID(p->commit_scn)
											 : p->commit_scn == InvalidScn);
	if (p->state != CCM_ACTIVE || p->commit_scn != InvalidScn
		|| ClusterCurrentMemberProofGetCtrcGrant(p) == 0
		|| !ClusterCurrentMemberProofGetStatusKey(p, &key, &generation, &wrap))
		return false;
	return key.origin_node_id == entry->origin_node_id && key.undo_segment_id != 0
		   && key.tt_slot_id != 0 && key.cluster_epoch > 0 && TransactionIdIsNormal(key.local_xid)
		   && (key.local_xid == member->xid
			   || NormalTransactionIdPrecedes(key.local_xid, member->xid))
		   && p->origin_fullxid_v2[0] == CLUSTER_CTRC_ORIGIN_FULLXID_V2_VERSION
		   && p->origin_fullxid_v2[1] == 0
		   && p->origin_fullxid_v2[2] == CLUSTER_CTRC_ORIGIN_FULLXID_V2_BYTES
		   && p->origin_fullxid_v2[3] == 0;
}

static bool
range_proof_window_valid(uint32 total, uint32 first, uint32 count, uint32 next, bool last)
{
	return total > 0 && total <= INT32_MAX && count > 0 && count <= CLUSTER_MX_PROOF_PAGE_MEMBERS
		   && (uint64)first + count == next && next <= total && last == (next == total);
}

bool
cluster_mx_range_wire_encode_proofs(const ClusterMxRangeWireContext *ctx,
									ClusterMxCompositionReadStatus status,
									const ClusterMxProofReply *reply, void *wire, uint32 capacity,
									uint32 *bytes)
{
	ClusterMxRangeWireHeader h;
	PGAlignedBlock frame;
	Size body;
	uint32 length, count = 0;
	if (bytes != NULL)
		*bytes = 0;
	if (!range_context_valid(ctx) || wire == NULL || bytes == NULL || status < MXCOMP_READ_OK
		|| status > MXCOMP_READ_UNKNOWN)
		return false;
	if (status == MXCOMP_READ_OK) {
		if (reply == NULL || reply->source == NULL || reply->source_bytes != ctx->source_bytes
			|| memcmp(reply->source, ctx->source, ctx->source_bytes) != 0
			|| !range_proof_window_valid(reply->total, reply->first, reply->count, reply->next,
										 reply->last)
			|| reply->count > ctx->requested_cap || reply->proofs == NULL
			|| reply->proof_bytes != (uint64)reply->count * sizeof(*reply->proofs))
			return false;
		count = reply->count;
		for (uint32 i = 0; i < count; i++) {
			const ClusterMxFullMemberProof *e = &reply->proofs[i];
			ClusterCurrentMxMemberDesc member = { 0 };
			member.xid = e->proof.member_xid;
			member.member_status = e->proof.member_status;
			if (!cluster_mx_range_proof_entry_valid(e, reply->first + i, i, &member)
				|| (e->proof.state == CCM_ACTIVE && e->proof.key.cluster_epoch != ctx->epoch))
				return false;
		}
	} else if (reply != NULL)
		return false;
	if (!range_frame_size(ctx->source_bytes, count, sizeof(ClusterMxFullMemberProof), &body,
						  &length)
		|| length > capacity)
		return false;
	range_header_init(&h, ctx, CLUSTER_MX_RANGE_PROOFS, length);
	h.result = status;
	if (count != 0) {
		h.total = reply->total;
		h.first = reply->first;
		h.count = count;
		h.next = reply->next;
		h.flags = reply->last ? CLUSTER_MX_RANGE_LAST : 0;
	}
	range_frame_prefix(frame.data, body, &h, ctx);
	if (count != 0)
		memcpy(frame.data + body, reply->proofs, (Size)count * sizeof(*reply->proofs));
	memcpy(wire, frame.data, length);
	*bytes = length;
	return true;
}

ClusterMxCompositionReadStatus
cluster_mx_range_wire_decode_proofs(const void *wire, uint32 bytes,
									const ClusterMxRangeWireContext *ctx, uint16 expected_origin,
									uint32 total, uint32 first,
									const ClusterCurrentMxMemberDesc *members, uint32 count,
									ClusterMxProofReply *reply)
{
	ClusterMxRangeWireHeader h;
	Size body;
	if (reply == NULL)
		return MXCOMP_READ_UNKNOWN;
	memset(reply, 0, sizeof(*reply));
	if (ctx == NULL || expected_origin >= CLUSTER_MAX_NODES || members == NULL || count == 0
		|| count > CLUSTER_MX_PROOF_PAGE_MEMBERS || count > ctx->requested_cap
		|| !range_header_read(wire, bytes, ctx, &h, CLUSTER_MX_RANGE_PROOFS,
							  sizeof(ClusterMxFullMemberProof), &body)
		|| h.result > MXCOMP_READ_UNKNOWN)
		return MXCOMP_READ_UNKNOWN;
	if (h.result != MXCOMP_READ_OK) {
		if (h.total || h.first || h.count || h.next || h.flags)
			return MXCOMP_READ_UNKNOWN;
		return (ClusterMxCompositionReadStatus)h.result;
	}
	if (h.total != total || h.first != first || h.count != count
		|| (h.flags & ~CLUSTER_MX_RANGE_LAST) != 0
		|| !range_proof_window_valid(total, first, count, h.next,
									 (h.flags & CLUSTER_MX_RANGE_LAST) != 0)
		|| !range_members_valid(members, count))
		return MXCOMP_READ_UNKNOWN;
	for (uint32 i = 0; i < count; i++) {
		ClusterMxFullMemberProof e;
		memcpy(&e, (const char *)wire + body + (Size)i * sizeof(e), sizeof(e));
		if (e.origin_node_id != expected_origin
			|| !cluster_mx_range_proof_entry_valid(&e, first + i, i, &members[i])
			|| (e.proof.state == CCM_ACTIVE && e.proof.key.cluster_epoch != ctx->epoch))
			return MXCOMP_READ_UNKNOWN;
	}
	reply->source = (const char *)wire + sizeof(h);
	reply->source_bytes = ctx->source_bytes;
	reply->total = total;
	reply->first = first;
	reply->count = count;
	reply->next = h.next;
	reply->last = (h.flags & CLUSTER_MX_RANGE_LAST) != 0;
	reply->proofs = (const ClusterMxFullMemberProof *)((const char *)wire + body);
	reply->proof_bytes = (Size)count * sizeof(*reply->proofs);
	reply->frame_bytes = bytes;
	return MXCOMP_READ_OK;
}

static uint32
range_checksum(const void *wire, uint32 bytes)
{
	ClusterMxRangeRoute route;
	pg_crc32c crc;

	memcpy(&route, wire, sizeof(route));
	route.checksum = 0;
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, &route, sizeof(route));
	COMP_CRC32C(crc, (const char *)wire + sizeof(route), bytes - sizeof(route));
	FIN_CRC32C(crc);
	return (uint32)crc;
}

static bool
range_target_valid(const BufferTag *tag)
{
	return tag->spcOid != 0 && tag->relNumber != 0 && tag->forkNum == MAIN_FORKNUM
		&& tag->blockNum != InvalidBlockNumber;
}

static bool
range_snapshot_valid(const ClusterMxRangeSnapshot *snapshot, uint64 epoch)
{
	return snapshot->snapshot_type <= SNAPSHOT_NON_VACUUMABLE
		&& (snapshot->read_epoch == 0 || snapshot->read_epoch == epoch);
}

static bool
range_request_valid(const ClusterMxRangeRequest *r)
{
	static const ClusterMxRangeSource zero;

	return r->key.reserved16 == 0 && r->key.reserved32 == 0
		&& r->key.origin_node_id < CLUSTER_MAX_NODES
		&& r->key.cluster_epoch == r->route.epoch && MultiXactIdIsValid(r->key.multixact_id)
		&& range_target_valid(&r->target) && range_snapshot_valid(&r->snapshot, r->route.epoch)
		&& r->cap > 0 && r->cap <= CLUSTER_MX_COMPOSITION_PAGE_MEMBERS
		&& r->peer_generation > 0 && r->peer_generation != UINT32_MAX
		&& r->reserved == 0 && r->has_source <= 1
		&& (r->has_source || (r->first == 0 && memcmp(&r->source, &zero, sizeof(zero)) == 0));
}

void
cluster_mx_range_request_seal(ClusterMxRangeRequest *request)
{
	request->route.format_id = CLUSTER_FOREIGN_MX_FORMAT_ID_V1;
	request->route.version = CLUSTER_FOREIGN_MX_FORMAT_VERSION_V1;
	request->route.kind = CLUSTER_MX_RANGE_REQUEST_KIND;
	request->route.wire_bytes = sizeof(*request);
	request->route.checksum = range_checksum(request, sizeof(*request));
}

/* Legacy frames begin with a legal request_id, whose low word may equal our
 * format id. First separate message type and length from those old shapes.
 * Resource-X's variable reply domain has its own distinct fixed prefix. */
bool
cluster_mx_range_transport_candidate(const void *wire, uint32 bytes, uint8 message_type)
{
	uint32 format;
	const uint32 minimum = sizeof(ClusterMxRangeRoute) + sizeof(ClusterMxRangeWireHeader)
		+ sizeof(ClusterMxRangeSource);
	const uint32 maximum = minimum + CLUSTER_MX_COMPOSITION_PAGE_MEMBERS * sizeof(ClusterCurrentMxMemberDesc);

	if (wire == NULL)
		return false;
	if (message_type == PGRAC_IC_MSG_GCS_BLOCK_FORWARD) {
		if (bytes != sizeof(ClusterMxRangeRequest))
			return false;
	} else if (message_type == PGRAC_IC_MSG_GCS_BLOCK_REPLY) {
		if (bytes < minimum || bytes > maximum
			|| (bytes - minimum) % sizeof(ClusterCurrentMxMemberDesc) != 0)
			return false;
	} else
		return false;
	memcpy(&format, wire, sizeof(format));
	return format == CLUSTER_FOREIGN_MX_FORMAT_ID_V1;
}

bool
cluster_mx_range_transport_route(const void *wire, uint32 bytes, uint8 message_type,
	ClusterMxRangeRoute *route, BufferTag *tag)
{
	ClusterMxRangeRoute h;

	if (route != NULL) memset(route, 0, sizeof(*route));
	if (tag != NULL) memset(tag, 0, sizeof(*tag));
	if (route == NULL || tag == NULL
		|| !cluster_mx_range_transport_candidate(wire, bytes, message_type))
		return false;
	memcpy(&h, wire, sizeof(h));
	if (h.format_id != CLUSTER_FOREIGN_MX_FORMAT_ID_V1
		|| h.version != CLUSTER_FOREIGN_MX_FORMAT_VERSION_V1 || h.wire_bytes != bytes
		|| h.request_id == 0 || h.epoch == 0 || h.epoch > UINT32_MAX
		|| h.requester_node < 0 || h.requester_node >= CLUSTER_MAX_NODES
		|| h.requester_backend <= 0 || h.reserved != 0
		|| h.checksum != range_checksum(wire, bytes))
		return false;
	if (message_type == PGRAC_IC_MSG_GCS_BLOCK_FORWARD) {
		ClusterMxRangeRequest request;
		if (h.kind != CLUSTER_MX_RANGE_REQUEST_KIND || bytes != sizeof(request))
			return false;
		memcpy(&request, wire, sizeof(request));
		if (!range_request_valid(&request))
			return false;
	} else if (message_type == PGRAC_IC_MSG_GCS_BLOCK_REPLY) {
		if (h.kind != CLUSTER_MX_RANGE_RESPONSE_KIND
			|| bytes < sizeof(h) + sizeof(ClusterMxRangeWireHeader))
			return false;
	} else
		return false;
	*route = h;
	*tag = GcsBlockCurrentMxRouteTagMake(h.request_id, h.epoch, h.requester_node, h.requester_backend);
	return true;
}


void
cluster_mx_range_response_seal(ClusterMxRangeResponse *response, uint32 body_bytes)
{
	response->route.kind = CLUSTER_MX_RANGE_RESPONSE_KIND;
	response->route.wire_bytes = sizeof(response->route) + body_bytes;
	response->route.checksum = range_checksum(response, response->route.wire_bytes);
}

bool
cluster_mx_range_refuse(const ClusterMxRangeRequest *request, uint32 loaded_catalog,
	ClusterMxRangeResponse *response, uint32 capacity, uint32 *bytes)
{
	ClusterMxRangeSource source = {0};
	ClusterMxRangeWireContext context = {0};
	uint32 body_bytes = 0;
	if (bytes != NULL) *bytes = 0;
	if (request == NULL || response == NULL || bytes == NULL || capacity < sizeof(*response)) return false;
	memset(response, 0, sizeof(*response));
	context.request_id = request->route.request_id; context.epoch = request->route.epoch;
	context.key = request->key; context.format_id = CLUSTER_FOREIGN_MX_FORMAT_ID_V1;
	context.version = CLUSTER_FOREIGN_MX_FORMAT_VERSION_V1; context.loaded_catalog = loaded_catalog;
	context.peer_capabilities = CLUSTER_FOREIGN_MX_REQUIRED_CAPABILITIES;
	context.peer_generation = request->peer_generation; context.requested_cap = request->cap;
	context.source = &source; context.source_bytes = sizeof(source);
	if (!cluster_mx_range_wire_encode_reply(&context, MXCOMP_READ_UNKNOWN, NULL,
		response->data.data, BLCKSZ, &body_bytes)) return false;
	response->route = request->route;
	cluster_mx_range_response_seal(response, body_bytes);
	*bytes = response->route.wire_bytes;
	return true;
}
