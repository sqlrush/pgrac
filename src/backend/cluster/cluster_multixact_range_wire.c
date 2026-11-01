/* Foreign-MX metadata codec. No transport, retained scope or visibility grant. */
#include "postgres.h"
#include "cluster/cluster_multixact_range_wire.h"

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
