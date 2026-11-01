/* Pure full-width normalization over one complete composition operation. */
#include "postgres.h"

#include "cluster/cluster_multixact_composition.h"
#include "cluster/cluster_multixact_range_wire.h"

static bool
composition_recompose_clear(MultiXactMember **pages, uint32 cap)
{
	bool valid = true;

	if (pages == NULL || cap == 0 || cap > INT32_MAX)
		return false;
	for (uint32 first = 0; first < cap; first += CLUSTER_MX_COMPOSITION_PAGE_MEMBERS) {
		uint32 count = Min(cap - first, CLUSTER_MX_COMPOSITION_PAGE_MEMBERS);
		MultiXactMember *page = pages[first / CLUSTER_MX_COMPOSITION_PAGE_MEMBERS];

		if (page == NULL)
			valid = false;
		else
			memset(page, 0, (Size)count * sizeof(*page));
	}
	return valid;
}

/* Validate/count first, then replay the same immutable input into already
 * supplied output. Reuse the original pure member normalization rules; the
 * local ordinal copy is never sent or stored back in the canonical carrier. */
static ClusterMxRecomposeResult
composition_recompose_pass(const ClusterMxCompositionReceive *op,
	TransactionId requester_xid, MultiXactStatus requester_status,
	MultiXactMember **pages, uint32 cap, uint32 *produced)
{
	const ClusterMxCompositionPage *members;
	const ClusterMxCompositionProofPage *proofs;
	uint32 total, proof_total, seen = 0, member_index = 0, count = 0;
	bool requester_kept = false;

	*produced = 0;
	members = cluster_mx_composition_pages(op, &total);
	proofs = cluster_mx_composition_proofs(op, &proof_total);
	if (members == NULL || proofs == NULL || total == 0 || total > INT32_MAX
		|| proof_total != total)
		return CMX_RECOMPOSE_UNKNOWN;
	for (; proofs != NULL; proofs = proofs->next) {
		if (proofs->first != seen || proofs->count == 0
			|| proofs->count > CLUSTER_MX_PROOF_PAGE_MEMBERS
			|| (uint64)seen + proofs->count > total)
			return CMX_RECOMPOSE_UNKNOWN;
		for (uint32 i = 0; i < proofs->count; i++, seen++) {
			const ClusterCurrentMxMemberDesc *member;
			ClusterCurrentMemberProof local;
			MultiXactMember normalized[2];
			uint16 nlocal = 0;
			ClusterMxRecomposeResult result;

			if (members == NULL || members->count == 0
				|| members->count > CLUSTER_MX_COMPOSITION_PAGE_MEMBERS
				|| member_index >= members->count
				|| (uint64)members->first + member_index != seen)
				return CMX_RECOMPOSE_UNKNOWN;
			member = &members->members[member_index];
			if (!cluster_mx_range_proof_entry_valid(&proofs->proofs[i], seen, i, member))
				return CMX_RECOMPOSE_UNKNOWN;
			local = proofs->proofs[i].proof;
			local.member_ordinal = 0;
			result = cluster_multixact_current_recompose(member, &local, 1,
				requester_xid, requester_status, normalized, lengthof(normalized), &nlocal);
			if (result != CMX_RECOMPOSE_OK)
				return result;
			if (local.state == CCM_ACTIVE || local.state == CCM_SELF) {
				if (count == INT32_MAX || (pages != NULL && count >= cap))
					return CMX_RECOMPOSE_SUPPORTED_LIMIT;
				if (pages != NULL)
					pages[count / CLUSTER_MX_COMPOSITION_PAGE_MEMBERS]
						 [count % CLUSTER_MX_COMPOSITION_PAGE_MEMBERS] = normalized[0];
				count++;
				requester_kept |= member->xid == requester_xid;
			}
			if (++member_index == members->count) {
				members = members->next;
				member_index = 0;
			}
		}
	}
	if (seen != total || members != NULL)
		return CMX_RECOMPOSE_UNKNOWN;
	if (!requester_kept) {
		if (count == INT32_MAX || (pages != NULL && count >= cap))
			return CMX_RECOMPOSE_SUPPORTED_LIMIT;
		if (pages != NULL) {
			MultiXactMember *out =
				&pages[count / CLUSTER_MX_COMPOSITION_PAGE_MEMBERS]
					  [count % CLUSTER_MX_COMPOSITION_PAGE_MEMBERS];
			out->xid = requester_xid;
			out->status = requester_status;
		}
		count++;
	}
	*produced = count;
	return CMX_RECOMPOSE_OK;
}

ClusterMxRecomposeResult
cluster_mx_composition_recompose(const ClusterMxCompositionReceive *op,
	TransactionId requester_xid, MultiXactStatus requester_status,
	MultiXactMember **normalized_pages, uint32 normalized_cap, uint32 *normalized_count)
{
	ClusterMxRecomposeResult result;
	uint32 required = 0, produced = 0;
	bool output_valid;

	if (normalized_count != NULL)
		*normalized_count = 0;
	output_valid = composition_recompose_clear(normalized_pages, normalized_cap);
	if (!output_valid || normalized_count == NULL)
		return CMX_RECOMPOSE_UNKNOWN;
	result = composition_recompose_pass(op, requester_xid, requester_status,
		NULL, 0, &required);
	if (result != CMX_RECOMPOSE_OK)
		return result;
	if (required > normalized_cap)
		return CMX_RECOMPOSE_SUPPORTED_LIMIT;
	result = composition_recompose_pass(op, requester_xid, requester_status,
		normalized_pages, normalized_cap, &produced);
	if (result != CMX_RECOMPOSE_OK || produced != required) {
		(void)composition_recompose_clear(normalized_pages, normalized_cap);
		return result == CMX_RECOMPOSE_OK ? CMX_RECOMPOSE_UNKNOWN : result;
	}
	*normalized_count = produced;
	return CMX_RECOMPOSE_OK;
}
