/*-------------------------------------------------------------------------
 * cluster_mx_history_member.c
 *    Complete member selection and independent peer-only proof assembly.
 * Portions Copyright (c) 2026, pgrac contributors
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "cluster/cluster_mx_history_member.h"
#ifdef USE_PGRAC_CLUSTER
#include "utils/memutils.h"

static void
history_members_reset(ClusterSavepointOperationKey *keys,
					  ClusterCurrentMemberOperationProof *proofs, uint16 n)
{
	if (keys != NULL)
		memset(keys, 0, sizeof(*keys) * n);
	if (proofs != NULL) {
		memset(proofs, 0, sizeof(*proofs) * n);
		for (uint16 i = 0; i < n; ++i) {
			proofs[i].base.state = CCM_UNKNOWN;
			proofs[i].format_version = CLUSTER_CURRENT_OPERATION_PROOF_VERSION;
		}
	}
}

static bool
history_members_capture_valid(const ClusterMxHistoryCapture *c)
{
	return c != NULL && c->read_epoch > 0 && c->read_epoch <= UINT32_MAX
		   && c->read_epoch == c->admission.formation_epoch && c->admission.entered
		   && c->admission.side == CLUSTER_SEMANTIC_TARGET_SIDE
		   && c->admission.feature_bit == CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1
		   && cluster_multixact_current_validate_descriptor(&c->mxkey, c->mxkey.origin_node_id,
															c->read_epoch, c->members, c->nmembers,
															c->nmembers)
				  == CMX_DESC_OK
		   && cluster_multixact_current_descriptor_hash(&c->mxkey, c->members, c->nmembers)
				  == c->descriptor_hash;
}

static bool
history_member_echo(const ClusterMxHistoryCapture *c, const ClusterMxHistoryRequest *r)
{
	ClusterMxHistoryRequestWire checked;
	return r != NULL && cluster_mx_history_request_encode(r, &checked)
		   && r->edge_kind != CMXH_CREATOR_ROOT && !(r->flags & CLUSTER_MX_HISTORY_CANCEL)
		   && r->collector_id == c->collector_id && r->capture_generation == c->capture_generation
		   && r->backend_incarnation == c->backend_incarnation
		   && r->feature_bit == c->admission.feature_bit
		   && r->admission_generation == c->admission.record_generation
		   && r->read_scn == c->read_scn && r->retained_floor == c->retained_floor
		   && r->read_epoch == c->read_epoch && r->snapshot_identity == c->snapshot_identity
		   && r->descriptor_hash == c->descriptor_hash && r->multixact_id == c->mxkey.multixact_id
		   && r->member_count == c->nmembers && r->root_ordinal < CLUSTER_ITL_INITRANS_DEFAULT
		   && memcmp(&r->root, &c->roots[r->root_ordinal], sizeof(r->root)) == 0
		   && r->target_locator.spcOid == c->pcm.tag.spcOid
		   && r->target_locator.dbOid == c->pcm.tag.dbOid
		   && r->target_locator.relNumber == c->pcm.tag.relNumber
		   && r->target_fork == c->pcm.tag.forkNum
		   && r->target_block == ItemPointerGetBlockNumber(&c->tid)
		   && r->target_offset == ItemPointerGetOffsetNumber(&c->tid)
		   && ClusterR4CrOwnerEqual(&r->requester_owner, &c->requester_owner);
}

/* A compare input from the actual retained entry, including GLOBAL records
 * whose incoming edge deliberately has no ITL head/slot. No head is invented. */
static bool
history_member_key_target(const ClusterUndoRecordTarget *target, const ClusterMxHistoryNodeView *v,
				   ClusterSavepointOperationKey *key)
{
	const UndoItlHistoryEntry *entry = NULL;
	memset(key, 0, sizeof(*key));
	if (v->record->record_class == CMXH_V1_TOP)
		return true;
	if (v->record->record_class != CMXH_OPERATION)
		return false;
	for (unsigned i = 0; i < v->record->payload.history.count; ++i) {
		const UndoItlHistoryEntry *e = &v->record->payload.history.entries[i];
		if (e->block != target->blockno)
			continue;
		if (entry != NULL)
			return false;
		entry = e;
	}
	if (entry == NULL || entry->slot_index >= CLUSTER_ITL_INITRANS_DEFAULT)
		return false;
	key->locator = (ClusterTxLocator){ .uba = v->peer->record,
									   .xid = v->peer->canonical.xid,
									   .tt_wrap = v->peer->canonical.wrap,
									   .itl_kind = entry->after_kind,
									   .itl_slot_index = entry->slot_index };
	key->target = *target;
	key->top_fxid = v->peer->top_fxid;
	key->operation_fxid = v->peer->operation_fxid;
	return true;
}

static ClusterUndoRecordTarget
history_capture_target(const ClusterMxHistoryCapture *c)
{
	return (ClusterUndoRecordTarget){ .locator = { c->pcm.tag.spcOid, c->pcm.tag.dbOid,
		c->pcm.tag.relNumber }, .forknum = c->pcm.tag.forkNum,
		.blockno = ItemPointerGetBlockNumber(&c->tid), .offnum = ItemPointerGetOffsetNumber(&c->tid) };
}

static bool
history_member_key(const ClusterMxHistoryCapture *c, const ClusterMxHistoryNodeView *v,
	ClusterSavepointOperationKey *key)
{
	ClusterUndoRecordTarget target = history_capture_target(c);
	return history_member_key_target(&target, v, key);
}

static bool
history_member_assemble_peer_target(const ClusterMxHistoryCapture *c, const ClusterMxHistoryNodeView *v,
							 uint16 ordinal, const ClusterSavepointOperationKey *key,
							 ClusterCurrentMemberOperationProof *out,
							 const ClusterUndoRecordTarget *target, bool require_root)
{
	ClusterMxHistoryRecord record;
	ClusterSavepointReaderIdentity identity;
	ClusterSavepointOperationKey exact;
	ClusterMxHistoryNodeView decoded;
	ClusterCurrentMemberOperationProof proof = { 0 };
	const ClusterMxHistoryPeer *peer;
	bool v1;

	history_members_reset(NULL, out, 1);
	if (out == NULL || key == NULL || ordinal >= c->nmembers || v == NULL || v->record == NULL
		|| v->peer == NULL || v->bytes == NULL || !history_member_echo(c, v->request)
		|| !v->record->canonical_generation_known
		|| !cluster_mx_history_record_from_bytes(v->request, v->bytes, v->record->record_length,
												 v->record->record_offset, &record)
		|| (require_root && !record.matches_target) || record.record_class > CMXH_OPERATION
		|| !cluster_mx_history_record_bind_peer(v->request, &record, v->peer, &identity))
		return false;
	peer = v->peer;
	v1 = record.record_class == CMXH_V1_TOP;
	if (peer->formation_epoch != c->read_epoch
		|| peer->admission_generation != c->admission.record_generation
		|| record.header.origin_node_id != v->request->forward.master_node
		|| record.header.xid != c->members[ordinal].xid
		|| XidFromFullTransactionId(identity.operation_fxid) != c->members[ordinal].xid
		|| (v1
			&& (!FullTransactionIdEquals(identity.top_fxid, identity.operation_fxid)
				|| record.header.xid != identity.canonical.xid)))
		return false;
	decoded = *v;
	decoded.record = &record;
	if (!history_member_key_target(target, &decoded, &exact) || memcmp(key, &exact, sizeof(exact)) != 0)
		return false;
	proof.base.member_xid = c->members[ordinal].xid;
	proof.base.member_ordinal = ordinal;
	proof.base.member_status = c->members[ordinal].member_status;
	proof.format_version = CLUSTER_CURRENT_OPERATION_PROOF_VERSION;
	switch (peer->outcome) {
	case CLUSTER_TX_IN_PROGRESS:
		proof.base.state = CCM_ACTIVE;
		break;
	case CLUSTER_TX_COMMITTED:
		proof.base.state = CCM_COMMITTED;
		proof.base.commit_scn = peer->commit_scn;
		break;
	case CLUSTER_TX_ABORTED:
		proof.base.state = CCM_ABORTED;
		break;
	default:
		return false;
	}
	/* Never reinterpret peer state as SELF or a discarded operation as a
	 * top abort. Only a LIVE ACTIVE peer carries the original live grant. */
	if ((proof.base.state == CCM_ACTIVE && record.effect == CLUSTER_SAVEPOINT_EFFECT_LIVE)
		!= (peer->ctrc_grant != 0))
		return false;
	if (!v1 || proof.base.state == CCM_ACTIVE) {
		proof.base.key.origin_node_id = record.header.origin_node_id;
		proof.base.key.undo_segment_id = identity.canonical.segment_id;
		proof.base.key.tt_slot_id = (uint32)identity.canonical.slot_offset + 1;
		proof.base.key.cluster_epoch = c->read_epoch;
		proof.base.key.local_xid = identity.canonical.xid;
		ClusterCurrentMemberProofSetCtrcBinding(&proof.base, identity.canonical_generation,
												identity.canonical.wrap);
	}
	ClusterCurrentMemberProofSetCtrcGrant(&proof.base, peer->ctrc_grant);
	if (!v1) {
		proof.flags = CLUSTER_CURRENT_OPERATION_PROOF_EXACT;
		proof.operation_uba = peer->record;
		proof.top_fxid = identity.top_fxid;
		proof.operation_fxid = identity.operation_fxid;
		proof.effect_state = record.effect;
	}
	*out = proof;
	return true;
}

static bool
history_member_assemble_peer(const ClusterMxHistoryCapture *c, const ClusterMxHistoryNodeView *v,
	uint16 ordinal, const ClusterSavepointOperationKey *key, ClusterCurrentMemberOperationProof *out)
{
	ClusterUndoRecordTarget target = history_capture_target(c);
	return history_member_assemble_peer_target(c, v, ordinal, key, out, &target, true);
}

/* Standalone peer assembly checks the complete immutable descriptor. The
 * complete-set assembler does this once, not once for each member. */
bool
cluster_multixact_current_operation_proof_from_history_peer(const ClusterMxHistoryCapture *c,
															const ClusterMxHistoryNodeView *v,
															uint16 ordinal,
															const ClusterSavepointOperationKey *key,
															ClusterCurrentMemberOperationProof *out)
{
	history_members_reset(NULL, out, 1);
	return history_members_capture_valid(c)
		   && history_member_assemble_peer(c, v, ordinal, key, out);
}

typedef struct HistoryPageOrder {
	size_t child, sibling, enter, end;
} HistoryPageOrder;

/* Reverse only actual page-prior edges, then assign ancestry intervals in
 * one iterative forest walk. GLOBAL_PREV never makes candidates comparable. */
static bool
history_member_order(const ClusterMxHistoryGraph *g, HistoryPageOrder *order, size_t n)
{
	size_t clock = 0;
	for (size_t i = 0; i < n; ++i)
		order[i] = (HistoryPageOrder){ CMXH_NO_NODE, CMXH_NO_NODE, 0, 0 };
	for (size_t i = 0; i < n; ++i) {
		ClusterMxHistoryNodeView v;
		if (!cluster_mx_history_graph_view(g, i, &v))
			return false;
		if (v.page_prior == CMXH_NO_NODE)
			continue;
		if (v.page_prior >= n || v.page_prior == i)
			return false;
		order[i].sibling = order[v.page_prior].child;
		order[v.page_prior].child = i;
	}
	for (size_t i = 0; i < n; ++i) {
		ClusterMxHistoryNodeView v;
		size_t at = i;
		if (!cluster_mx_history_graph_view(g, i, &v))
			return false;
		if (v.page_prior != CMXH_NO_NODE)
			continue;
		for (;;) {
			size_t child = order[at].child;
			if (order[at].enter == 0)
				order[at].enter = ++clock;
			if (child != CMXH_NO_NODE) {
				order[at].child = order[child].sibling;
				if (order[child].enter != 0)
					return false;
				at = child;
				continue;
			}
			order[at].end = clock;
			if (!cluster_mx_history_graph_view(g, at, &v))
				return false;
			if (v.page_prior == CMXH_NO_NODE)
				break;
			at = v.page_prior;
		}
	}
	return clock == n;
}

static bool
history_member_ancestor(const HistoryPageOrder *order, size_t older, size_t newer)
{
	return order[older].enter <= order[newer].enter && order[newer].enter <= order[older].end;
}

static bool
history_member_same_identity(const ClusterMxHistoryNodeView *a, const ClusterMxHistoryNodeView *b)
{
	return FullTransactionIdEquals(a->peer->top_fxid, b->peer->top_fxid)
		   && FullTransactionIdEquals(a->peer->operation_fxid, b->peer->operation_fxid)
		   && memcmp(&a->peer->canonical, &b->peer->canonical, sizeof(a->peer->canonical)) == 0
		   && a->peer->canonical_generation == b->peer->canonical_generation;
}

static bool
history_assemble_members(const ClusterMxHistoryCapture *c,
	const ClusterMxHistoryGraph *g, ClusterSavepointOperationKey *keys,
	ClusterCurrentMemberOperationProof *proofs, uint32 *capabilities, uint16 capacity)
{
	/* Fixed hash/selection storage is bounded by the immutable 256-member
	 * descriptor; record count remains dynamic with no depth truncation. */
	uint16 member_hash[512] = { 0 };
	size_t latest[CLUSTER_CURRENT_MX_MAX_MEMBERS], live[CLUSTER_CURRENT_MX_MAX_MEMBERS];
	size_t n = cluster_mx_history_graph_count(g);
	HistoryPageOrder *order = NULL;
	bool ok = false;
	if (capacity > CLUSTER_CURRENT_MX_MAX_MEMBERS)
		return false;
	history_members_reset(keys, proofs, capacity);
	if (capabilities != NULL)
		memset(capabilities, 0, capacity * sizeof(*capabilities));
	if (keys == NULL || proofs == NULL || !history_members_capture_valid(c)
		|| capacity < c->nmembers || n == 0 || n > MaxAllocSize / sizeof(*order))
		return false;
	order = MemoryContextAllocExtended(CurrentMemoryContext, n * sizeof(*order), MCXT_ALLOC_NO_OOM);
	if (order == NULL || !history_member_order(g, order, n))
		goto done;
	for (uint16 i = 0; i < c->nmembers; ++i) {
		unsigned slot = (c->members[i].xid * UINT32_C(2654435761)) & 511;
		while (member_hash[slot])
			slot = (slot + 1) & 511;
		member_hash[slot] = i + 1;
		latest[i] = live[i] = CMXH_NO_NODE;
	}
	for (size_t i = 0; i < n; ++i) {
		ClusterMxHistoryNodeView v, previous;
		unsigned slot;
		uint16 member;
		if (!cluster_mx_history_graph_view(g, i, &v))
			goto done;
		if (!v.record->matches_target || v.record->record_class > CMXH_OPERATION)
			continue;
		slot = (v.record->header.xid * UINT32_C(2654435761)) & 511;
		while (member_hash[slot] && c->members[member_hash[slot] - 1].xid != v.record->header.xid)
			slot = (slot + 1) & 511;
		if (member_hash[slot] == 0)
			continue;
		member = member_hash[slot] - 1;
		if (latest[member] != CMXH_NO_NODE) {
			if (!cluster_mx_history_graph_view(g, latest[member], &previous)
				|| !history_member_same_identity(&v, &previous)
				|| (!history_member_ancestor(order, i, latest[member])
					&& !history_member_ancestor(order, latest[member], i)))
				goto done;
			if (history_member_ancestor(order, latest[member], i))
				latest[member] = i;
		} else
			latest[member] = i;
		if (v.record->effect == CLUSTER_SAVEPOINT_EFFECT_LIVE
			&& (live[member] == CMXH_NO_NODE || history_member_ancestor(order, live[member], i)))
			live[member] = i;
	}
	for (uint16 i = 0; i < c->nmembers; ++i) {
		ClusterMxHistoryNodeView v;
		size_t chosen = live[i] != CMXH_NO_NODE ? live[i] : latest[i];
		if (chosen == CMXH_NO_NODE || !cluster_mx_history_graph_view(g, chosen, &v)
			|| !history_member_key(c, &v, &keys[i])
			|| !history_member_assemble_peer(c, &v, i, &keys[i], &proofs[i]))
			goto done;
		/* This is the selected record's original qualified request,
		 * not the peer/origin generation or an unrelated B request. */
		if (capabilities != NULL)
			capabilities[i] = v.request->requester_capability_generation;
	}
	ok = true;
done:
	if (order != NULL)
		pfree(order);
	if (!ok) {
		history_members_reset(keys, proofs, capacity);
		if (capabilities != NULL)
			memset(capabilities, 0, capacity * sizeof(*capabilities));
	}
	return ok;
}

bool
cluster_mx_history_assemble_members(const ClusterMxHistoryCapture *c,
	const ClusterMxHistoryGraph *g, ClusterSavepointOperationKey *keys,
	ClusterCurrentMemberOperationProof *proofs, uint16 capacity)
{
	return history_assemble_members(c, g, keys, proofs, NULL, capacity);
}

bool
cluster_mx_history_assemble_members_with_capabilities(const ClusterMxHistoryCapture *c,
	const ClusterMxHistoryGraph *g, ClusterSavepointOperationKey *keys,
	ClusterCurrentMemberOperationProof *proofs, uint32 *capabilities, uint16 capacity)
{
	if (capabilities == NULL) {
		if (capacity <= CLUSTER_CURRENT_MX_MAX_MEMBERS)
			history_members_reset(keys, proofs, capacity);
		return false;
	}
	return history_assemble_members(c, g, keys, proofs, capabilities, capacity);
}

/* Reuse an original ordered peer proof, not a raw writer/echo. The successor
 * may be on another heap page, so B need not match A's target tuple. */
ClusterMxHistoryStep
cluster_mx_history_assemble_updater(const ClusterMxHistoryCapture *c,
									const ClusterMxHistoryGraph *g,
									const ClusterCurrentMemberOperationProof *proofs,
									const ClusterCurrentUpdaterChallenge *challenge,
									ClusterCurrentUpdaterProof *out)
{
	const ClusterCurrentMemberOperationProof *a;
	uint16 ordinal;
	if (out == NULL)
		return CMXH_UNKNOWN;
	memset(out, 0, sizeof(*out));
	out->verdict = CUCP_UNKNOWN;
	if (!history_members_capture_valid(c) || proofs == NULL || challenge == NULL
		|| challenge->reserved16 != 0 || challenge->member_ordinal >= c->nmembers)
		return CMXH_UNKNOWN;
	ordinal = challenge->member_ordinal;
	a = &proofs[ordinal];
	if (!ISUPDATE_from_mxstatus(c->members[ordinal].member_status)
		|| challenge->updater_xid != c->members[ordinal].xid || a->base.state >= CCM_UNKNOWN
		|| a->base.member_xid != challenge->updater_xid || a->base.member_ordinal != ordinal
		|| !cluster_multixact_current_successor_provenance_well_formed(
			&challenge->candidate_next_xmin_alias, &challenge->candidate_next_xmin_locator,
			challenge->updater_xid, challenge->candidate_next_xmin_alias.origin_node_id,
			c->read_epoch))
		return CMXH_UNKNOWN;
	for (size_t i = 0; i < cluster_mx_history_graph_count(g); ++i) {
		ClusterMxHistoryNodeView v;
		ClusterMxHistoryRecord record;
		ClusterSavepointReaderIdentity identity;
		if (!cluster_mx_history_graph_view(g, i, &v))
			return CMXH_UNKNOWN;
		if (!cluster_undo_record_uba_equal(v.peer->record,
										   challenge->candidate_next_xmin_locator.uba))
			continue;
		if (!history_member_echo(c, v.request)
			|| !cluster_mx_history_record_from_bytes(v.request, v.bytes, v.record->record_length,
													 v.record->record_offset, &record)
			|| record.record_class > CMXH_OPERATION || record.header.xid != challenge->updater_xid
			|| record.header.origin_node_id != challenge->candidate_next_xmin_alias.origin_node_id
			|| !cluster_mx_history_record_bind_peer(v.request, &record, v.peer, &identity)
			|| v.peer->formation_epoch != c->read_epoch
			|| v.peer->admission_generation != c->admission.record_generation
			|| XidFromFullTransactionId(identity.operation_fxid) != challenge->updater_xid)
			return CMXH_UNKNOWN;
		if (a->flags & CLUSTER_CURRENT_OPERATION_PROOF_EXACT) {
			if (!FullTransactionIdEquals(a->top_fxid, identity.top_fxid)
				|| !FullTransactionIdEquals(a->operation_fxid, identity.operation_fxid)
				|| a->base.key.undo_segment_id != identity.canonical.segment_id
				|| a->base.key.local_xid != identity.canonical.xid
				|| a->base.key.tt_slot_id != (uint32)identity.canonical.slot_offset + 1
				|| a->base.key.segment_generation != identity.canonical_generation
				|| a->base.key.slot_wrap != identity.canonical.wrap)
				return CMXH_UNKNOWN;
		} else {
			/* V1 A has no typed extension. Compare the already qualified A
			 * record itself, including its independent original FullXid. */
			bool same = false;
			for (size_t j = 0; j < cluster_mx_history_graph_count(g); ++j) {
				ClusterMxHistoryNodeView av;
				if (!cluster_mx_history_graph_view(g, j, &av))
					return CMXH_UNKNOWN;
				if (av.record->matches_target && av.record->record_class == CMXH_V1_TOP
					&& av.record->header.xid == challenge->updater_xid
					&& history_member_same_identity(&av, &v))
					same = true;
			}
			if (!same)
				return CMXH_UNKNOWN;
		}
		out->mxkey = c->mxkey;
		out->candidate_next_xmin_alias = challenge->candidate_next_xmin_alias;
		out->candidate_next_xmin_locator = challenge->candidate_next_xmin_locator;
		out->candidate_next_xmin_locator.tt_wrap = identity.canonical.wrap;
		out->updater_xid = challenge->updater_xid;
		out->member_ordinal = ordinal;
		out->verdict = CUCP_MATCH;
		return CMXH_COMPLETE;
	}
	return CMXH_PENDING;
}
/* The target role is separate from a row merely touched by the operation.
 * Decoded bodies include the structural checks before reaching this helper. */
static bool
history_successor_target(const ClusterUndoRecordTarget *target,
	const ClusterMxHistoryRecord *record, const char *body, bool *creation)
{
	const UndoRecordHeader *h = &record->header;
	bool old_side, new_side = false;
	*creation = false;
	if (h->target_locator.spcOid != target->locator.spcOid
		|| h->target_locator.dbOid != target->locator.dbOid
		|| h->target_locator.relNumber != target->locator.relNumber
		|| h->target_fork != target->forknum)
		return false;
	old_side = h->target_block == target->blockno && h->target_offset == target->offnum;
	if (h->record_type == UNDO_RECORD_INSERT) {
		UndoInsertPayload insert;
		memcpy(&insert, body, sizeof(insert));
		*creation = old_side && insert.inserted_tuple_len >= SizeofHeapTupleHeader
			&& insert.inserted_tuple_len <= BLCKSZ - SizeOfPageHeaderData;
	} else if (h->record_type == UNDO_RECORD_UPDATE) {
		UndoUpdatePayload update;
		memcpy(&update, body, sizeof(update));
		new_side = update.new_block == target->blockno && update.new_offset == target->offnum;
		*creation = new_side;
	}
	return old_side || new_side;
}

static bool
history_successor_identity(const ClusterCurrentMemberOperationProof *a,
	const ClusterSavepointReaderIdentity *b)
{
	return FullTransactionIdEquals(a->top_fxid, b->top_fxid)
		&& FullTransactionIdEquals(a->operation_fxid, b->operation_fxid)
		&& a->base.key.undo_segment_id == b->canonical.segment_id
		&& a->base.key.local_xid == b->canonical.xid
		&& a->base.key.tt_slot_id == (uint32)b->canonical.slot_offset + 1
		&& a->base.key.segment_generation == b->canonical_generation
		&& a->base.key.slot_wrap == b->canonical.wrap;
}

ClusterMxHistoryStep
cluster_mx_history_assemble_successor(const ClusterMxHistoryCapture *c,
	const ClusterMxHistoryGraph *g, const ClusterSavepointOperationKey *keys,
	const ClusterCurrentMemberOperationProof *proofs,
	const ClusterMxHistorySuccessorChallenge *challenge, ClusterMxHistorySuccessorProof *out)
{
	const ClusterCurrentMemberOperationProof *a;
	ClusterMxHistorySuccessorProof chosen = { 0 };
	HistoryPageOrder *order = NULL;
	ItemPointerData next;
	size_t n, selected = CMXH_NO_NODE;
	uint16 ordinal;
	bool found_a = false, saw_target = false;
	ClusterMxHistoryStep result = CMXH_UNKNOWN;

	if (out == NULL)
		return CMXH_UNKNOWN;
	memset(out, 0, sizeof(*out));
	history_members_reset(NULL, &out->member, 1);
	if (!history_members_capture_valid(c) || challenge == NULL || keys == NULL || proofs == NULL
		|| challenge->reserved16 != 0 || challenge->member_ordinal >= c->nmembers
		|| challenge->target._pad != 0 || challenge->target.forknum != c->pcm.tag.forkNum
		|| challenge->target.locator.spcOid != c->pcm.tag.spcOid
		|| challenge->target.locator.dbOid != c->pcm.tag.dbOid
		|| challenge->target.locator.relNumber != c->pcm.tag.relNumber
		|| cluster_multixact_current_validate_operation_proof_set(&c->mxkey, c->members,
			keys, proofs, c->nmembers) != CMX_RESOLVE_OK)
		return CMXH_UNKNOWN;
	memcpy(&next, c->tuple_header + offsetof(HeapTupleHeaderData, t_ctid), sizeof(next));
	if (challenge->target.blockno != ItemPointerGetBlockNumber(&c->tid)
		|| challenge->target.blockno != ItemPointerGetBlockNumber(&next)
		|| challenge->target.offnum != ItemPointerGetOffsetNumber(&next)
		|| !OffsetNumberIsValid(challenge->target.offnum)
		|| challenge->target.offnum > MaxHeapTuplesPerPage
		|| (ItemPointerGetBlockNumber(&next) == ItemPointerGetBlockNumber(&c->tid)
			&& ItemPointerGetOffsetNumber(&next) == ItemPointerGetOffsetNumber(&c->tid)))
		return CMXH_UNKNOWN;
	ordinal = challenge->member_ordinal;
	a = &proofs[ordinal];
	if (!ISUPDATE_from_mxstatus(c->members[ordinal].member_status)
		|| challenge->xmin != c->members[ordinal].xid
		|| a->flags != CLUSTER_CURRENT_OPERATION_PROOF_EXACT
		|| a->effect_state != CLUSTER_SAVEPOINT_EFFECT_LIVE)
		return CMXH_UNKNOWN;
	n = cluster_mx_history_graph_count(g);
	if (n == 0 || n > MaxAllocSize / sizeof(*order))
		return CMXH_UNKNOWN;
	/* Reject an otherwise valid A from another graph, including a changed
	 * committed SCN/grant/binding. No A read or authority is repeated. */
	for (size_t i = 0; i < n; ++i) {
		ClusterMxHistoryNodeView v;
		ClusterCurrentMemberOperationProof actual;
		if (!cluster_mx_history_graph_view(g, i, &v))
			return CMXH_UNKNOWN;
		if (!cluster_undo_record_uba_equal(v.peer->record, a->operation_uba))
			continue;
		if (!history_member_assemble_peer(c, &v, ordinal, &keys[ordinal], &actual)
			|| memcmp(&actual, a, sizeof(actual)) != 0)
			return CMXH_UNKNOWN;
		found_a = true;
	}
	if (!found_a)
		return CMXH_UNKNOWN;
	order = MemoryContextAllocExtended(CurrentMemoryContext, n * sizeof(*order), MCXT_ALLOC_NO_OOM);
	if (order == NULL || !history_member_order(g, order, n))
		goto done;
	for (size_t i = 0; i < n; ++i) {
		ClusterMxHistoryNodeView v, decoded;
		ClusterMxHistoryRecord record;
		ClusterSavepointReaderIdentity identity;
		ClusterMxHistorySuccessorProof b = { 0 };
		bool creation;
		if (!cluster_mx_history_graph_view(g, i, &v))
			goto done;
		if (v.record->record_class != CMXH_OPERATION || v.record->header.xid != challenge->xmin)
			continue;
		if (!history_member_echo(c, v.request)
			|| !cluster_mx_history_record_from_bytes(v.request, v.bytes, v.record->record_length,
				v.record->record_offset, &record)
			|| !cluster_mx_history_record_bind_peer(v.request, &record, v.peer, &identity))
			goto done;
		if (!history_successor_target(&challenge->target, &record,
			v.bytes + sizeof(UndoRecordHeader), &creation))
			continue;
		saw_target = true;
		if (!history_successor_identity(a, &identity))
			goto done;
		if (!creation || record.effect != CLUSTER_SAVEPOINT_EFFECT_LIVE)
			continue;
		decoded = v;
		decoded.record = &record;
		if (!history_member_key_target(&challenge->target, &decoded, &b.key)
			|| !history_member_assemble_peer_target(c, &decoded, ordinal, &b.key, &b.member,
				&challenge->target, false)
			|| b.member.base.state != a->base.state
			|| b.member.base.commit_scn != a->base.commit_scn)
			goto done;
		b.requester_capability_generation = v.request->requester_capability_generation;
		if (selected != CMXH_NO_NODE) {
			if (!history_member_ancestor(order, selected, i)
				&& !history_member_ancestor(order, i, selected))
				goto done;
			if (history_member_ancestor(order, i, selected))
				continue;
		}
		selected = i;
		chosen = b;
	}
	if (selected == CMXH_NO_NODE) {
		result = saw_target ? CMXH_UNKNOWN : CMXH_PENDING;
		goto done;
	}
	*out = chosen;
	result = CMXH_COMPLETE;
done:
	if (order != NULL)
		pfree(order);
	return result;
}
#endif
