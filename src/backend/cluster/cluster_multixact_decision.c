/* Pure whole-operation decisions over complete immutable G2 pages. */
#include "postgres.h"

#include "cluster/cluster_multixact_composition.h"
#include "cluster/cluster_multixact_range_wire.h"

static ClusterCurrentMxDecision
decision_unknown(ClusterCurrentMxDecisionTrace *trace, ClusterCurrentMxUnknownReason reason,
				 int32 ordinal)
{
	if (trace != NULL) {
		trace->unknown_reason = reason;
		trace->member_ordinal = ordinal;
	}
	return CMDL_UNKNOWN;
}

static bool
decision_conflict(ClusterCurrentMxDecision result)
{
	return result == CMDL_WAIT_MEMBER || result == CMDL_BEING_MODIFIED
		|| result == CMDL_WOULD_BLOCK || result == CMDL_LOCK_NOT_AVAILABLE;
}

static bool
decision_key_precedes(const ClusterTTStatusKey *a, const ClusterTTStatusKey *b)
{
	if (a->origin_node_id != b->origin_node_id)
		return a->origin_node_id < b->origin_node_id;
	if (a->local_xid != b->local_xid)
		return a->local_xid < b->local_xid;
	if (a->tt_slot_id != b->tt_slot_id)
		return a->tt_slot_id < b->tt_slot_id;
	return a->undo_segment_id < b->undo_segment_id;
}

static void
decision_merge(ClusterCurrentMxDecision result, const ClusterTTStatusKey *key,
			   ClusterCurrentMxDecision *whole, ClusterTTStatusKey *selected)
{
	if (result == CMDL_WAIT_MEMBER
		&& (*whole != CMDL_WAIT_MEMBER || decision_key_precedes(key, selected)))
		*selected = *key;
	if (result != CMDL_CONTINUE)
		*whole = result;
}

static ClusterCurrentMxDecision
decision_member(const ClusterCurrentMxMemberDesc *member, const ClusterMxFullMemberProof *entry,
	uint32 local_ordinal, const ClusterCurrentMxRequestContext *ctx, bool other_conflict,
	const ClusterMxFullUpdaterChallenge *challenge, const ClusterMxFullUpdaterProof *proof,
	ClusterTTStatusKey *wait_key, ClusterCurrentMxDecisionTrace *trace)
{
	ClusterCurrentMxRequestContext local_ctx = *ctx;
	ClusterCurrentMemberProof local = entry->proof;
	ClusterCurrentUpdaterChallenge local_challenge;
	ClusterCurrentUpdaterProof local_proof;
	ClusterCurrentMxDecisionTrace local_trace;
	ClusterCurrentMxDecision result;

	local.member_ordinal = 0;
	if (!ISUPDATE_from_mxstatus(member->member_status))
		local_ctx.tuple_shape = CCM_SHAPE_LOCK_ONLY;
	/* In the original whole-list rule an ACTIVE compatible updater follows
	 * only after every other member is known not to conflict. */
	if (other_conflict && local.state == CCM_ACTIVE)
		local_ctx.follow_updates = 0;
	if (challenge != NULL || proof != NULL) {
		if (challenge == NULL || proof == NULL
			|| challenge->member_ordinal != entry->member_ordinal
			|| proof->member_ordinal != entry->member_ordinal
			|| challenge->challenge.member_ordinal != local_ordinal
			|| proof->proof.member_ordinal != local_ordinal)
			return decision_unknown(trace, CMX_UNKNOWN_PROOF_ENTRY, entry->member_ordinal);
		local_challenge = challenge->challenge;
		local_proof = proof->proof;
		local_challenge.member_ordinal = 0;
		local_proof.member_ordinal = 0;
	}
	result = cluster_multixact_current_decide_observed(member, &local, 1, &local_ctx,
		challenge == NULL ? NULL : &local_challenge,
		proof == NULL ? NULL : &local_proof, wait_key, &local_trace);
	if (result == CMDL_UNKNOWN)
		return decision_unknown(trace, local_trace.unknown_reason,
			local_trace.member_ordinal < 0 ? -1 : (int32)entry->member_ordinal);
	return result;
}

ClusterCurrentMxDecision
cluster_mx_composition_decide(const ClusterMxCompositionReceive *op,
	const ClusterCurrentMxRequestContext *ctx, const ClusterMxFullUpdaterChallenge *challenge,
	const ClusterMxFullUpdaterProof *updater_proof, ClusterTTStatusKey *wait_key,
	ClusterCurrentMxDecisionTrace *trace)
{
	const ClusterMxCompositionPage *members;
	const ClusterMxCompositionProofPage *proofs;
	ClusterCurrentMxMemberDesc updater_member;
	ClusterMxFullMemberProof updater_entry;
	uint32 total, proof_total, seen = 0, index = 0, updater_local = 0;
	bool have_updater = false;
	ClusterCurrentMxDecision whole = CMDL_CONTINUE, result;
	ClusterTTStatusKey selected, candidate;

	if (wait_key != NULL)
		memset(wait_key, 0, sizeof(*wait_key));
	if (trace != NULL) {
		trace->unknown_reason = CMX_UNKNOWN_NONE;
		trace->member_ordinal = -1;
	}
	memset(&selected, 0, sizeof(selected));
	if (ctx == NULL || ctx->tuple_shape > CCM_SHAPE_DELETED)
		return decision_unknown(trace, CMX_UNKNOWN_REQUEST_CONTEXT, -1);
	members = cluster_mx_composition_pages(op, &total);
	proofs = cluster_mx_composition_proofs(op, &proof_total);
	if (members == NULL || total == 0 || total > INT32_MAX)
		return decision_unknown(trace, CMX_UNKNOWN_DESCRIPTOR, -1);
	if (proofs == NULL || proof_total != total)
		return decision_unknown(trace, CMX_UNKNOWN_PROOFS_NULL, -1);
	for (; proofs != NULL; proofs = proofs->next) {
		if (proofs->first != seen || proofs->count == 0
			|| proofs->count > CLUSTER_MX_PROOF_PAGE_MEMBERS
			|| (uint64)seen + proofs->count > total)
			return decision_unknown(trace, CMX_UNKNOWN_PROOF_ENTRY, seen);
		for (uint32 i = 0; i < proofs->count; i++, seen++) {
			const ClusterCurrentMxMemberDesc *member;
			const ClusterMxFullMemberProof *entry = &proofs->proofs[i];

			if (members == NULL || members->count == 0
				|| members->count > CLUSTER_MX_COMPOSITION_PAGE_MEMBERS
				|| index >= members->count || (uint64)members->first + index != seen)
				return decision_unknown(trace, CMX_UNKNOWN_DESCRIPTOR, seen);
			member = &members->members[index];
			if (!cluster_mx_range_proof_entry_valid(entry, seen, i, member))
				return decision_unknown(trace, CMX_UNKNOWN_PROOF_ENTRY, seen);
			if (ISUPDATE_from_mxstatus(member->member_status)) {
				if (have_updater)
					return decision_unknown(trace, CMX_UNKNOWN_DESCRIPTOR, seen);
				updater_member = *member;
				updater_entry = *entry;
				updater_local = i;
				have_updater = true;
			} else {
				result = decision_member(member, entry, i, ctx, false,
					NULL, NULL, &candidate, trace);
				if (result == CMDL_UNKNOWN)
					return result;
				decision_merge(result, &candidate, &whole, &selected);
			}
			if (++index == members->count) {
				members = members->next;
				index = 0;
			}
		}
	}
	if (seen != total || members != NULL)
		return decision_unknown(trace, CMX_UNKNOWN_DESCRIPTOR, -1);
	if ((ctx->tuple_shape == CCM_SHAPE_LOCK_ONLY) != !have_updater)
		return decision_unknown(trace, CMX_UNKNOWN_TUPLE_SHAPE, -1);
	if (have_updater) {
		result = decision_member(&updater_member, &updater_entry, updater_local, ctx,
			decision_conflict(whole), challenge, updater_proof, &candidate, trace);
		if (result == CMDL_UNKNOWN)
			return result;
		if (!decision_conflict(whole) || (result != CMDL_CONTINUE && result != CMDL_FOLLOW_UPDATED))
			decision_merge(result, &candidate, &whole, &selected);
	} else if (challenge != NULL || updater_proof != NULL)
		return decision_unknown(trace, CMX_UNKNOWN_PROOF_ENTRY, -1);
	if (whole == CMDL_WAIT_MEMBER && wait_key != NULL)
		*wait_key = selected;
	return whole;
}
