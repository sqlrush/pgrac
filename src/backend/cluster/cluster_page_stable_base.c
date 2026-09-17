/*-------------------------------------------------------------------------
 *
 * cluster_page_stable_base.c
 *    STOP-06 exact PageVersion graph and stable-base selector.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/xlogreader.h"
#include "cluster/cluster_external_fence.h"
#include "cluster/cluster_page_stable_base.h"
#include "cluster/cluster_recovery_duty.h"
#include "cluster/cluster_wal_retention.h"

#ifdef USE_CLUSTER_UNIT
#define stable_alloc0(count_, size_) calloc((count_), (size_))
#define stable_free(pointer_) free((pointer_))
#else
#define stable_alloc0(count_, size_) palloc0((Size)(count_) * (size_))
#define stable_free(pointer_) pfree((pointer_))
#endif

#define RF_PAGE_STABLE_PROOF_MAGIC UINT32_C(0x52505350)

struct RfPageStableBaseProofV1 {
	uint32 magic;
	uint32 participant_count;
	RfPageIdentityV1 page_identity;
	RfPageVersionV1 expected_result;
	RfPageStableSelectionV1 selection;
	const ClusterRecoveryDutyKey *duties;
	const ClusterControlRootReadToken *root_tokens;
	const ClusterFormationWitnessV1 *formation;
	const PgracExternalFenceNeedSetV1 *fence_need_set;
	const PgracExternalFenceAdmissionSetV1 *fence_admission_set;
	ClusterWalRetentionPin *retention_pin;
	const RfPagePinnedSourceV1 *source;
	const RfContributorVectorV1 *contributors;
};

static bool
bytes_nonzero(const uint8 *bytes, size_t size)
{
	uint8 value = 0;
	size_t i;

	for (i = 0; i < size; i++)
		value |= bytes[i];
	return value != 0;
}

bool
rf_page_identity_valid_v1(const RfPageIdentityV1 *identity)
{
	return identity != NULL && identity->system_identifier != 0
		   && bytes_nonzero(identity->storage_uuid, sizeof(identity->storage_uuid))
		   && identity->locator.spcOid != InvalidOid && identity->locator.dbOid != InvalidOid
		   && identity->locator.relNumber != InvalidRelFileNumber
		   && identity->blockno != InvalidBlockNumber && identity->reserved_zero == 0;
}

bool
rf_page_identity_equal_v1(const RfPageIdentityV1 *left, const RfPageIdentityV1 *right)
{
	return left != NULL && right != NULL && left->system_identifier == right->system_identifier
		   && memcmp(left->storage_uuid, right->storage_uuid, 16) == 0
		   && RelFileLocatorEquals(left->locator, right->locator) && left->forknum == right->forknum
		   && left->blockno == right->blockno && left->reserved_zero == 0
		   && right->reserved_zero == 0;
}

bool
rf_page_version_present_v1(const RfPageVersionV1 *version)
{
	return version != NULL
		   && bytes_nonzero(version->segment_incarnation, sizeof(version->segment_incarnation))
		   && version->mutation_token != 0;
}

static void
edge_result_version(const RfPageStableEdgeInputV1 *edge, RfPageVersionV1 *result)
{
	memcpy(result->segment_incarnation, edge->edge.result_incarnation, 16);
	result->mutation_token = edge->result_token;
}

static bool
edge_before_vertex_equal(const RfPageStableEdgeInputV1 *left, const RfPageStableEdgeInputV1 *right)
{
	if (left->edge.before_kind != right->edge.before_kind)
		return false;
	return rf_page_version_equal_v1(&left->edge.before, &right->edge.before);
}

static bool
edge_result_equal(const RfPageStableEdgeInputV1 *left, const RfPageStableEdgeInputV1 *right)
{
	RfPageVersionV1 left_result;
	RfPageVersionV1 right_result;

	edge_result_version(left, &left_result);
	edge_result_version(right, &right_result);
	return rf_page_version_equal_v1(&left_result, &right_result);
}

static bool
record_identity_equal(const RfPageReplayRecordIdentityV1 *left,
					  const RfPageReplayRecordIdentityV1 *right)
{
	return left->system_identifier == right->system_identifier
		   && memcmp(left->storage_uuid, right->storage_uuid, 16) == 0
		   && left->origin_thread == right->origin_thread
		   && left->reserved_zero == right->reserved_zero && left->timeline_id == right->timeline_id
		   && left->read_rec_ptr == right->read_rec_ptr && left->end_rec_ptr == right->end_rec_ptr
		   && left->record_crc == right->record_crc && left->rmid == right->rmid
		   && left->info == right->info && left->reserved_zero2 == right->reserved_zero2;
}

static bool
edge_exact_equal(const RfPageStableEdgeInputV1 *left, const RfPageStableEdgeInputV1 *right)
{
	return record_identity_equal(&left->record_identity, &right->record_identity)
		   && left->participant_index == right->participant_index
		   && left->component_count == right->component_count
		   && rf_page_identity_equal_v1(&left->page_identity, &right->page_identity)
		   && memcmp(&left->edge, &right->edge, sizeof(left->edge)) == 0
		   && left->result_token == right->result_token
		   && memcmp(left->anchor_digest, right->anchor_digest, 32) == 0
		   && left->record_complete == right->record_complete
		   && left->opcode_supported == right->opcode_supported
		   && left->side_complete == right->side_complete
		   && left->image_integrity_ok == right->image_integrity_ok;
}

static bool
edge_is_duplicate_of_prior(const RfContributorVectorV1 *vector, uint32 index)
{
	uint32 i;

	for (i = 0; i < index; i++)
		if (edge_exact_equal(&vector->edges[i], &vector->edges[index]))
			return true;
	return false;
}

static bool
edge_anchor_flags_valid(uint16 flags)
{
	return flags == (RF_PAGE_EDGE_FULL_IMAGE_APPLY | RF_PAGE_EDGE_FULL_COVERAGE)
		   || flags == (RF_PAGE_EDGE_WILL_INIT | RF_PAGE_EDGE_FULL_COVERAGE)
		   || flags
				  == (RF_PAGE_EDGE_FULL_IMAGE_APPLY | RF_PAGE_EDGE_WILL_INIT
					  | RF_PAGE_EDGE_FULL_COVERAGE);
}

static RfPageProofDetailV1
validate_edge(const RfPageStableGraphRequestV1 *request, const RfPageStableEdgeInputV1 *edge)
{
	const RfContributorVectorV1 *vector = request->contributors;
	const RfContributorStreamCutV1 *cut;
	const RfPageReplayRecordIdentityV1 *record = &edge->record_identity;
	RfPageVersionV1 result;
	bool before_uuid;

	if (!rf_page_identity_equal_v1(&request->page_identity, &edge->page_identity))
		return RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH;
	if (!edge->record_complete || edge->component_count == 0
		|| edge->component_count > RF_PAGE_STABLE_MAX_COMPONENTS)
		return RF_PAGE_PROOF_DETAIL_COMPONENT_INCOMPLETE;
	if (!edge->opcode_supported)
		return RF_PAGE_PROOF_DETAIL_OPCODE_UNSUPPORTED;
	if (!edge->side_complete)
		return RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
	if (edge->participant_index >= vector->participant_count)
		return RF_PAGE_PROOF_DETAIL_PARTICIPANT_MISSING;
	cut = &vector->cuts[edge->participant_index];
	if (record->reserved_zero != 0 || record->reserved_zero2 != 0
		|| record->system_identifier != vector->system_identifier
		|| memcmp(record->storage_uuid, vector->storage_uuid, 16) != 0
		|| record->origin_thread != cut->failed_thread || record->timeline_id != cut->timeline_id
		|| record->read_rec_ptr == InvalidXLogRecPtr || record->end_rec_ptr == InvalidXLogRecPtr
		|| record->read_rec_ptr >= record->end_rec_ptr
		|| record->read_rec_ptr < cut->scan_begin_inclusive
		|| record->end_rec_ptr > cut->scan_end_exclusive)
		return RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH;
	if (edge->edge.page_class != RF_PAGE_CLASS_ORDINARY
		|| edge->edge.result_kind != RF_PAGE_STATE_PRESENT)
		return RF_PAGE_PROOF_DETAIL_CLASS_UNKNOWN;

	edge_result_version(edge, &result);
	if (!rf_page_version_present_v1(&result))
		return RF_PAGE_PROOF_DETAIL_VERSION_MISMATCH;
	before_uuid = bytes_nonzero(edge->edge.before.segment_incarnation, 16);
	switch (edge->edge.before_kind) {
	case RF_PAGE_STATE_PRESENT:
		if (!rf_page_version_present_v1(&edge->edge.before)
			|| memcmp(edge->edge.before.segment_incarnation, result.segment_incarnation, 16) != 0)
			return RF_PAGE_PROOF_DETAIL_INCARNATION_MISMATCH;
		break;
	case RF_PAGE_STATE_UNFORMATTED:
		if (!before_uuid || edge->edge.before.mutation_token != 0
			|| memcmp(edge->edge.before.segment_incarnation, result.segment_incarnation, 16) != 0)
			return RF_PAGE_PROOF_DETAIL_INCARNATION_MISMATCH;
		break;
	case RF_PAGE_STATE_ABSENT:
		if (before_uuid || edge->edge.before.mutation_token != 0)
			return RF_PAGE_PROOF_DETAIL_INCARNATION_MISMATCH;
		break;
	default:
		return RF_PAGE_PROOF_DETAIL_CLASS_UNKNOWN;
	}

	if (edge->edge.edge_flags == 0) {
		if (edge->edge.before_kind != RF_PAGE_STATE_PRESENT)
			return RF_PAGE_PROOF_DETAIL_ANCHOR_MISSING;
	} else if (!edge_anchor_flags_valid(edge->edge.edge_flags))
		return RF_PAGE_PROOF_DETAIL_IMAGE_DECODE_FAILED;
	else if (!edge->image_integrity_ok
			 || !bytes_nonzero(edge->anchor_digest, sizeof(edge->anchor_digest)))
		return RF_PAGE_PROOF_DETAIL_IMAGE_INTEGRITY_FAILED;

	return RF_PAGE_PROOF_DETAIL_OK;
}

static RfPageProofDetailV1
validate_request(const RfPageStableGraphRequestV1 *request)
{
	const RfContributorVectorV1 *vector;
	uint64 contributor_total = 0;
	uint64 component_total = 0;
	uint32 *per_participant;
	uint32 i;
	RfPageProofDetailV1 detail = RF_PAGE_PROOF_DETAIL_OK;

	if (request == NULL || request->contributors == NULL || request->source == NULL
		|| request->flags != 0 || !rf_page_identity_valid_v1(&request->page_identity)
		|| !rf_page_version_present_v1(&request->expected_result) || request->participant_count == 0
		|| request->participant_count > RF_PAGE_STABLE_MAX_PARTICIPANTS)
		return RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT;
	if (!request->root_current)
		return RF_PAGE_PROOF_DETAIL_ROOT_STALE;
	if (!request->duty_current)
		return RF_PAGE_PROOF_DETAIL_DUTY_STALE;
	if (!request->fence_current)
		return RF_PAGE_PROOF_DETAIL_FENCE_STALE;
	if (!request->retention_current || request->retention_binding_cookie == 0
		|| request->retention_binding_cookie != request->current_retention_binding_cookie)
		return RF_PAGE_PROOF_DETAIL_RETENTION_STALE;
	if (!request->source->identity_verified || !request->source->integrity_verified
		|| request->source->binding_cookie == 0
		|| request->source->binding_cookie != request->source->current_binding_cookie)
		return RF_PAGE_PROOF_DETAIL_SOURCE_GAP;
	if (!rf_page_identity_equal_v1(&request->page_identity, &request->source->page_identity))
		return RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH;

	vector = request->contributors;
	if (vector->participant_count != request->participant_count || vector->cuts == NULL
		|| (vector->edge_count > 0 && vector->edges == NULL)
		|| vector->system_identifier != request->page_identity.system_identifier
		|| memcmp(vector->storage_uuid, request->page_identity.storage_uuid, 16) != 0)
		return RF_PAGE_PROOF_DETAIL_PARTICIPANT_MISSING;
	if (vector->edge_count > RF_PAGE_STABLE_MAX_EDGES)
		return RF_PAGE_PROOF_DETAIL_CAPACITY;

	per_participant = (uint32 *)stable_alloc0(request->participant_count, sizeof(uint32));
	if (per_participant == NULL)
		return RF_PAGE_PROOF_DETAIL_OOM;

	for (i = 0; i < vector->participant_count; i++) {
		const RfContributorStreamCutV1 *cut = &vector->cuts[i];
		bool empty = (cut->flags & RF_CONTRIBUTOR_CUT_EXPLICIT_EMPTY) != 0;

		if (cut->failed_thread == 0 || cut->timeline_id == 0
			|| (cut->flags & ~RF_CONTRIBUTOR_CUT_KNOWN_MASK) != 0
			|| (cut->flags & RF_CONTRIBUTOR_CUT_COMPLETE) == 0
			|| (i > 0
				&& (vector->cuts[i - 1].failed_thread > cut->failed_thread
					|| (vector->cuts[i - 1].failed_thread == cut->failed_thread
						&& vector->cuts[i - 1].timeline_id >= cut->timeline_id)))) {
			detail = RF_PAGE_PROOF_DETAIL_PARTICIPANT_MISSING;
			goto done;
		}
		if ((empty
			 && (cut->flags != RF_CONTRIBUTOR_CUT_KNOWN_MASK
				 || cut->scan_begin_inclusive != cut->scan_end_exclusive
				 || cut->contributor_count != 0 || cut->component_count != 0))
			|| (!empty
				&& (cut->flags != RF_CONTRIBUTOR_CUT_COMPLETE
					|| cut->scan_begin_inclusive >= cut->scan_end_exclusive
					|| cut->contributor_count == 0))) {
			detail = RF_PAGE_PROOF_DETAIL_PARTICIPANT_MISSING;
			goto done;
		}
		contributor_total += cut->contributor_count;
		component_total += cut->component_count;
	}

	if (contributor_total != vector->edge_count) {
		detail = RF_PAGE_PROOF_DETAIL_PARTICIPANT_MISSING;
		goto done;
	}

	for (i = 0; i < vector->edge_count; i++) {
		const RfPageStableEdgeInputV1 *edge = &vector->edges[i];

		if (edge->participant_index >= request->participant_count) {
			detail = RF_PAGE_PROOF_DETAIL_PARTICIPANT_MISSING;
			goto done;
		}
		per_participant[edge->participant_index]++;
		detail = validate_edge(request, edge);
		if (detail != RF_PAGE_PROOF_DETAIL_OK)
			goto done;
	}
	for (i = 0; i < request->participant_count; i++) {
		if (per_participant[i] != vector->cuts[i].contributor_count) {
			detail = RF_PAGE_PROOF_DETAIL_PARTICIPANT_MISSING;
			goto done;
		}
	}
	if (component_total == 0 && vector->edge_count != 0)
		detail = RF_PAGE_PROOF_DETAIL_COMPONENT_INCOMPLETE;

done:
	stable_free(per_participant);
	return detail;
}

static bool
edge_selected_or_duplicate(const RfContributorVectorV1 *vector, uint32 index,
						   const uint32 *reverse_chain, uint32 chain_length)
{
	uint32 i;

	for (i = 0; i < chain_length; i++) {
		if (reverse_chain[i] == index
			|| edge_exact_equal(&vector->edges[reverse_chain[i]], &vector->edges[index]))
			return true;
	}
	return false;
}

RfPageProofDetailV1
rf_page_stable_base_select_v1(const RfPageStableGraphRequestV1 *request, uint32 *chain_indices,
							  uint32 chain_capacity, RfPageStableSelectionV1 *selection)
{
	const RfContributorVectorV1 *vector;
	RfPageStableSelectionV1 selected;
	RfPageVersionV1 current;
	uint32 *reverse_chain;
	uint32 reverse_length = 0;
	uint32 nearest_anchor_reverse_pos = UINT32_MAX;
	uint32 terminal_count = 0;
	uint32 terminal_index = UINT32_MAX;
	uint32 i;
	uint32 j;
	RfPageProofDetailV1 detail;

	if (chain_indices == NULL || selection == NULL || chain_capacity == 0)
		return RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT;
	detail = validate_request(request);
	if (detail != RF_PAGE_PROOF_DETAIL_OK)
		return detail;
	vector = request->contributors;

	if (vector->edge_count == 0) {
		if (!rf_page_version_equal_v1(&request->source->source_version, &request->expected_result))
			return RF_PAGE_PROOF_DETAIL_ANCHOR_MISSING;
		memset(&selected, 0, sizeof(selected));
		selected.terminal_version = request->expected_result;
		selected.anchor_edge_index = UINT32_MAX;
		selected.result_already_present = true;
		*selection = selected;
		return RF_PAGE_PROOF_DETAIL_OK;
	}
	if (chain_capacity < vector->edge_count)
		return RF_PAGE_PROOF_DETAIL_CAPACITY;

	reverse_chain = (uint32 *)stable_alloc0(vector->edge_count, sizeof(uint32));
	if (reverse_chain == NULL)
		return RF_PAGE_PROOF_DETAIL_OOM;

	for (i = 0; i < vector->edge_count; i++) {
		const RfPageStableEdgeInputV1 *left = &vector->edges[i];
		RfPageVersionV1 left_result;
		bool has_outgoing = false;

		if (edge_is_duplicate_of_prior(vector, i))
			continue;
		edge_result_version(left, &left_result);
		if (left->edge.before_kind == RF_PAGE_STATE_PRESENT
			&& rf_page_version_equal_v1(&left->edge.before, &left_result)) {
			detail = RF_PAGE_PROOF_DETAIL_EDGE_CYCLE;
			goto fail;
		}
		for (j = i + 1; j < vector->edge_count; j++) {
			const RfPageStableEdgeInputV1 *right = &vector->edges[j];

			if (edge_is_duplicate_of_prior(vector, j))
				continue;
			if (record_identity_equal(&left->record_identity, &right->record_identity)
				&& !edge_exact_equal(left, right)) {
				detail = RF_PAGE_PROOF_DETAIL_ANCHOR_AMBIGUOUS;
				goto fail;
			}
			if (edge_before_vertex_equal(left, right)) {
				if (!edge_result_equal(left, right))
					detail = RF_PAGE_PROOF_DETAIL_EDGE_BRANCH;
				else
					detail = RF_PAGE_PROOF_DETAIL_ANCHOR_AMBIGUOUS;
				goto fail;
			}
			if (edge_result_equal(left, right)) {
				detail = RF_PAGE_PROOF_DETAIL_TERMINAL_AMBIGUOUS;
				goto fail;
			}
		}
		for (j = 0; j < vector->edge_count; j++) {
			if (edge_is_duplicate_of_prior(vector, j))
				continue;
			if (vector->edges[j].edge.before_kind == RF_PAGE_STATE_PRESENT
				&& rf_page_version_equal_v1(&vector->edges[j].edge.before, &left_result)) {
				has_outgoing = true;
				break;
			}
		}
		if (!has_outgoing) {
			terminal_count++;
			terminal_index = i;
		}
	}
	if (terminal_count == 0) {
		detail = RF_PAGE_PROOF_DETAIL_EDGE_CYCLE;
		goto fail;
	}
	if (terminal_count != 1) {
		detail = RF_PAGE_PROOF_DETAIL_TERMINAL_AMBIGUOUS;
		goto fail;
	}
	{
		RfPageVersionV1 terminal;

		edge_result_version(&vector->edges[terminal_index], &terminal);
		if (!rf_page_version_equal_v1(&terminal, &request->expected_result)) {
			detail = RF_PAGE_PROOF_DETAIL_VERSION_MISMATCH;
			goto fail;
		}
	}

	current = request->expected_result;
	for (;;) {
		uint32 predecessor = UINT32_MAX;
		uint32 predecessor_count = 0;
		const RfPageStableEdgeInputV1 *edge;

		for (i = 0; i < vector->edge_count; i++) {
			RfPageVersionV1 result;

			if (edge_is_duplicate_of_prior(vector, i))
				continue;
			edge_result_version(&vector->edges[i], &result);
			if (rf_page_version_equal_v1(&result, &current)) {
				predecessor = i;
				predecessor_count++;
			}
		}
		if (predecessor_count == 0) {
			if (nearest_anchor_reverse_pos != UINT32_MAX)
				break;
			detail = RF_PAGE_PROOF_DETAIL_EDGE_GAP;
			goto fail;
		}
		if (predecessor_count != 1 || reverse_length >= chain_capacity) {
			detail = predecessor_count != 1 ? RF_PAGE_PROOF_DETAIL_TERMINAL_AMBIGUOUS
											: RF_PAGE_PROOF_DETAIL_CAPACITY;
			goto fail;
		}
		for (i = 0; i < reverse_length; i++) {
			if (reverse_chain[i] == predecessor) {
				detail = RF_PAGE_PROOF_DETAIL_EDGE_CYCLE;
				goto fail;
			}
		}
		reverse_chain[reverse_length++] = predecessor;
		edge = &vector->edges[predecessor];
		if (edge_anchor_flags_valid(edge->edge.edge_flags)
			&& nearest_anchor_reverse_pos == UINT32_MAX)
			nearest_anchor_reverse_pos = reverse_length - 1;
		if (edge->edge.before_kind != RF_PAGE_STATE_PRESENT) {
			if (nearest_anchor_reverse_pos == UINT32_MAX) {
				detail = RF_PAGE_PROOF_DETAIL_ANCHOR_MISSING;
				goto fail;
			}
			break;
		}
		current = edge->edge.before;
	}

	for (i = 0; i < vector->edge_count; i++) {
		if (!edge_selected_or_duplicate(vector, i, reverse_chain, reverse_length)) {
			detail = RF_PAGE_PROOF_DETAIL_TERMINAL_AMBIGUOUS;
			goto fail;
		}
	}
	{
		uint32 selected_length = nearest_anchor_reverse_pos + 1;

		for (i = 0; i < selected_length; i++)
			chain_indices[i] = reverse_chain[nearest_anchor_reverse_pos - i];
		memset(&selected, 0, sizeof(selected));
		selected.terminal_version = request->expected_result;
		selected.anchor_edge_index = chain_indices[0];
		selected.chain_length = selected_length;
		selected.result_already_present = false;
	}
	*selection = selected;
	stable_free(reverse_chain);
	return RF_PAGE_PROOF_DETAIL_OK;

fail:
	stable_free(reverse_chain);
	return detail;
}

static RfPageProofDetailV1
stable_owners_revalidate(const RfPageStableBaseProofRequestV1 *request, bool bound_pin)
{
	const RfPageStableGraphRequestV1 *graph = request->graph;
	const RfContributorVectorV1 *contributors = graph->contributors;
	PgracExternalFenceDenyReason fence_reason = PGRAC_EXTERNAL_FENCE_DENY_BAD_ARGUMENT;
	uint32 i;

	if (request->duties == NULL || request->root_tokens == NULL || contributors == NULL
		|| contributors->cuts == NULL || graph->participant_count == 0
		|| graph->participant_count > RF_PAGE_STABLE_MAX_PARTICIPANTS
		|| contributors->participant_count != graph->participant_count)
		return RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT;
	for (i = 0; i < graph->participant_count; i++) {
		const ClusterControlRootReadToken *token = &request->root_tokens[i];
		const ClusterRecoveryDutyKey *duty = &request->duties[i];
		ClusterControlRootSnapshot snapshot;
		ClusterControlRootResult result;

		if (!bytes_nonzero(token->authority_uuid, sizeof(token->authority_uuid))
			|| token->origin_thread_id != contributors->cuts[i].failed_thread
			|| token->origin_thread_id != duty->origin_thread_id
			|| token->root_lineage_seq != duty->root_lineage_seq
			|| memcmp(token->authority_uuid, duty->authority_uuid, 16) != 0
			|| token->root_lineage_seq == 0 || token->reserved20 != 0 || token->reserved32 != 0)
			return RF_PAGE_PROOF_DETAIL_ROOT_STALE;
		result = cluster_control_root_revalidate(token, duty, &snapshot);
		if ((result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
			 && result != CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED)
			|| snapshot.lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED
			|| (snapshot.root_flags & CLUSTER_CONTROL_ROOT_FLAG_TAIL_VALID) == 0)
			return RF_PAGE_PROOF_DETAIL_ROOT_STALE;
	}
	if (cluster_formation_witness_revalidate_nowait(request->formation)
			!= CLUSTER_FORMATION_WITNESS_READY
		|| !cluster_external_fence_need_set_revalidate_nowait(request->fence_need_set,
															  request->formation, &fence_reason)
		|| !cluster_external_fence_revalidate_set_nowait(request->fence_admission_set,
														 request->fence_need_set,
														 request->formation, &fence_reason))
		return RF_PAGE_PROOF_DETAIL_FENCE_STALE;
	if ((bound_pin ? cluster_wal_retention_pin_revalidate(request->retention_pin)
				   : cluster_wal_retention_pin_preflight_revalidate_wait_v1(request->retention_pin))
		!= CLUSTER_WAL_PIN_OK)
		return RF_PAGE_PROOF_DETAIL_RETENTION_STALE;
	return RF_PAGE_PROOF_DETAIL_OK;
}

static RfPageProofDetailV1
stable_proof_build(const RfPageStableBaseProofRequestV1 *request, uint32 *chain_indices,
				   uint32 chain_capacity, RfPageStableBaseProofV1 **out_proof, bool bound_pin)
{
	RfPageStableBaseProofV1 *proof;
	RfPageStableSelectionV1 selection;
	RfPageStableGraphRequestV1 verified_graph;
	RfPageProofDetailV1 detail;

	if (out_proof == NULL)
		return RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT;
	*out_proof = NULL;
	if (request == NULL || request->graph == NULL || request->duties == NULL
		|| request->root_tokens == NULL || request->formation == NULL
		|| request->fence_need_set == NULL || request->fence_admission_set == NULL
		|| request->retention_pin == NULL || request->flags != 0)
		return RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT;
	detail = stable_owners_revalidate(request, bound_pin);
	if (detail != RF_PAGE_PROOF_DETAIL_OK)
		return detail;
	verified_graph = *request->graph;
	verified_graph.root_current = true;
	verified_graph.duty_current = true;
	verified_graph.fence_current = true;
	verified_graph.retention_current = true;
	verified_graph.retention_binding_cookie = 1;
	verified_graph.current_retention_binding_cookie = 1;
	detail
		= rf_page_stable_base_select_v1(&verified_graph, chain_indices, chain_capacity, &selection);
	if (detail != RF_PAGE_PROOF_DETAIL_OK)
		return detail;
	proof = (RfPageStableBaseProofV1 *)stable_alloc0(1, sizeof(*proof));
	if (proof == NULL)
		return RF_PAGE_PROOF_DETAIL_OOM;
	proof->magic = RF_PAGE_STABLE_PROOF_MAGIC;
	proof->participant_count = request->graph->participant_count;
	proof->page_identity = request->graph->page_identity;
	proof->expected_result = request->graph->expected_result;
	proof->selection = selection;
	proof->duties = request->duties;
	proof->root_tokens = request->root_tokens;
	proof->formation = request->formation;
	proof->fence_need_set = request->fence_need_set;
	proof->fence_admission_set = request->fence_admission_set;
	proof->retention_pin = request->retention_pin;
	proof->source = request->graph->source;
	proof->contributors = request->graph->contributors;
	*out_proof = proof;
	return RF_PAGE_PROOF_DETAIL_OK;
}

RfPageProofDetailV1
rf_page_stable_base_proof_build_wait_v1(const RfPageStableBaseProofRequestV1 *request,
										uint32 *chain_indices, uint32 chain_capacity,
										int timeout_ms, RfPageStableBaseProofV1 **out_proof)
{
	if (out_proof == NULL)
		return RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT;
	*out_proof = NULL;
	if (timeout_ms < 0)
		return RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT;
	return stable_proof_build(request, chain_indices, chain_capacity, out_proof, false);
}

RfPageProofDetailV1
rf_page_stable_base_proof_build_bound_v1(const RfPageStableBaseProofRequestV1 *request,
										 uint32 *chain_indices, uint32 chain_capacity,
										 RfPageStableBaseProofV1 **out_proof)
{
	return stable_proof_build(request, chain_indices, chain_capacity, out_proof, true);
}

bool
rf_page_stable_base_proof_matches_v1(
	const RfPageStableBaseProofV1 *proof, const RfPageIdentityV1 *page_identity,
	const RfPageVersionV1 *expected_result, const ClusterRecoveryDutyKey *duties,
	const ClusterControlRootReadToken *root_tokens, const ClusterFormationWitnessV1 *formation,
	const PgracExternalFenceNeedSetV1 *fence_need_set,
	const PgracExternalFenceAdmissionSetV1 *fence_admission_set,
	ClusterWalRetentionPin *retention_pin, const RfPagePinnedSourceV1 *source,
	const RfContributorVectorV1 *contributors, uint32 participant_count)
{
	return proof != NULL && proof->magic == RF_PAGE_STABLE_PROOF_MAGIC && participant_count != 0
		   && proof->participant_count == participant_count
		   && rf_page_identity_equal_v1(&proof->page_identity, page_identity)
		   && rf_page_version_equal_v1(&proof->expected_result, expected_result)
		   && rf_page_version_equal_v1(&proof->selection.terminal_version, expected_result)
		   && proof->duties == duties && proof->root_tokens == root_tokens
		   && proof->formation == formation && proof->fence_need_set == fence_need_set
		   && proof->fence_admission_set == fence_admission_set
		   && proof->retention_pin == retention_pin && proof->source == source
		   && proof->contributors == contributors;
}

void
rf_page_stable_base_proof_destroy_v1(RfPageStableBaseProofV1 **proof_pointer)
{
	RfPageStableBaseProofV1 *proof;

	if (proof_pointer == NULL || *proof_pointer == NULL)
		return;
	proof = *proof_pointer;
	if (proof->magic != RF_PAGE_STABLE_PROOF_MAGIC)
		return;
	proof->magic = 0;
	stable_free(proof);
	*proof_pointer = NULL;
}

#ifdef USE_CLUSTER_UNIT

static bool
page_all_zero(const char page[BLCKSZ])
{
	const uint8 *bytes = (const uint8 *)page;
	uint8 value = 0;
	int i;

	for (i = 0; i < BLCKSZ; i++)
		value |= bytes[i];
	return value == 0;
}

RfPageProofDetailV1
rf_page_stable_install_test_v1(const RfPageInstallRequestV1 *request, RfPageInstallProofV1 *proof)
{
	RfPageInstallProofV1 completed;
	char postread[BLCKSZ];
	uint32 i;
	bool promoted = false;
	RfPageProofDetailV1 failure = RF_PAGE_PROOF_DETAIL_OK;

	if (request == NULL || proof == NULL || request->components == NULL || request->ops == NULL
		|| request->prepared_pages == NULL || request->component_count == 0
		|| request->component_count > RF_PAGE_STABLE_MAX_COMPONENTS
		|| request->prepared_capacity < (Size)request->component_count * BLCKSZ
		|| request->ops->canonicalize == NULL || request->ops->promote == NULL
		|| request->ops->write == NULL || request->ops->sync == NULL
		|| request->ops->postread == NULL || request->ops->publish == NULL
		|| request->ops->release == NULL)
		return RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT;
	if (!request->global_preflight_ok)
		return RF_PAGE_PROOF_DETAIL_COMPONENT_INCOMPLETE;

	for (i = 0; i < request->component_count; i++) {
		const RfPageInstallComponentV1 *component = &request->components[i];
		char *prepared = request->prepared_pages + (Size)i * BLCKSZ;

		if (!rf_page_identity_valid_v1(&component->page_identity)
			|| !rf_page_version_present_v1(&component->expected_before)
			|| !rf_page_version_present_v1(&component->expected_result)
			|| component->canonical_page == NULL || !component->route_preflight_ok
			|| !component->side_preflight_ok || !component->scratch_ready
			|| !component->identity_authority_ok || !component->canonical_layout_ok)
			return RF_PAGE_PROOF_DETAIL_COMPONENT_INCOMPLETE;
		if (component->target_state == RF_PAGE_INSTALL_TARGET_UNRELATED)
			return RF_PAGE_PROOF_DETAIL_VERSION_MISMATCH;
		if (component->target_state != RF_PAGE_INSTALL_TARGET_EXPECTED
			&& component->target_state != RF_PAGE_INSTALL_TARGET_RESULT
			&& component->target_state != RF_PAGE_INSTALL_TARGET_TORN)
			return RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT;
		memcpy(prepared, component->canonical_page, BLCKSZ);
		if (page_all_zero(prepared)
			|| !request->ops->canonicalize(request->ops->arg, i, component->checksums_enabled,
										   prepared)
			|| page_all_zero(prepared))
			return RF_PAGE_PROOF_DETAIL_IMAGE_INTEGRITY_FAILED;
	}

	if (!request->ops->promote(request->ops->arg))
		return RF_PAGE_PROOF_DETAIL_WOULD_BLOCK;
	promoted = true;
	for (i = 0; i < request->component_count; i++) {
		const RfPageInstallComponentV1 *component = &request->components[i];
		const char *prepared = request->prepared_pages + (Size)i * BLCKSZ;

		if (component->target_state != RF_PAGE_INSTALL_TARGET_RESULT
			&& !request->ops->write(request->ops->arg, i, prepared)) {
			failure = RF_PAGE_PROOF_DETAIL_INTERNAL;
			goto cleanup;
		}
	}
	for (i = 0; i < request->component_count; i++) {
		if (!request->ops->sync(request->ops->arg, i)) {
			failure = RF_PAGE_PROOF_DETAIL_POSTREAD_FAILED;
			goto cleanup;
		}
	}
	for (i = 0; i < request->component_count; i++) {
		const char *prepared = request->prepared_pages + (Size)i * BLCKSZ;

		if (!request->ops->postread(request->ops->arg, i, postread)
			|| memcmp(postread, prepared, BLCKSZ) != 0) {
			failure = RF_PAGE_PROOF_DETAIL_POSTREAD_FAILED;
			goto cleanup;
		}
	}
	if (!request->ops->publish(request->ops->arg)) {
		failure = RF_PAGE_PROOF_DETAIL_INTERNAL;
		goto cleanup;
	}
	if (!request->ops->release(request->ops->arg))
		return RF_PAGE_PROOF_DETAIL_ORDER_VIOLATION;
	promoted = false;

	memset(&completed, 0, sizeof(completed));
	completed.component_count = request->component_count;
	completed.durability_complete = true;
	completed.postread_complete = true;
	completed.proof_published = true;
	completed.authority_released = true;
	*proof = completed;
	return RF_PAGE_PROOF_DETAIL_OK;

cleanup:
	if (promoted && !request->ops->release(request->ops->arg))
		return RF_PAGE_PROOF_DETAIL_ORDER_VIOLATION;
	return failure;
}

#endif /* USE_CLUSTER_UNIT */
