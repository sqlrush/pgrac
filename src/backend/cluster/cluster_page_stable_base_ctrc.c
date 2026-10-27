/*-------------------------------------------------------------------------
 *
 * cluster_page_stable_base_ctrc.c
 *     Bind the original stable graph to native CTRC subject authorities.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *     src/backend/cluster/cluster_page_stable_base_ctrc.c
 *
 * NOTES
 *     ROOT I/O is preflight only, before PAGE guards. Private snapshots own
 *     neither the original scopes nor their IR/WALR grants. Native CTRC and
 *     ordinary RR proofs have separate consumers; neither grants PAGE itself.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_wal_inputs.h"
#include "cluster_page_stable_base_private.h"

#ifdef USE_CLUSTER_UNIT
#define ctrc_alloc0(bytes_) calloc(1, (bytes_))
#define ctrc_free(pointer_) free((pointer_))
#else
#define ctrc_alloc0(bytes_) palloc0(bytes_)
#define ctrc_free(pointer_) pfree(pointer_)
#endif

typedef struct RfPageStableCtrcStorageV1 {
	const RfPagePinnedSourceV1 *original_source;
	const RfContributorVectorV1 *original_vector;
	RfContributorVectorV1 original_header;
	RfPagePinnedSourceV1 source;
	RfContributorVectorV1 vector;
	/* Aligned cut and edge arrays follow this fixed header. */
} RfPageStableCtrcStorageV1;

static RfPageProofDetailV1
ctrc_owners_current(const RfPageIdentityV1 *identity,
					const RfContributorVectorV1 *vector,
					const ClusterRecoveryCtrcSubjectAuthorityV1 *owners, uint32 count)
{
	if (!rf_page_identity_valid_v1(identity) || owners == NULL || count == 0
		|| count > RF_PAGE_STABLE_MAX_PARTICIPANTS || vector == NULL
		|| vector->cuts == NULL || vector->participant_count != count)
		return RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT;
	for (uint32 i = 0; i < count; i++) {
		const ClusterRecoveryCtrcSubjectAuthorityV1 *a = &owners[i];
		const ClusterRecoveryTxRetentionV1 *retention = &a->source.retention;
		const ClusterWalInputV1 *selected = &a->source.selected;
		const ClusterControlRootSnapshot *checkpoint = &selected->checkpoint;
		const ClusterRecoveryDutyKey *duty = &selected->source.claim.identity;
		const RfContributorStreamCutV1 *cut = &vector->cuts[i];

		/* The native locator validates the scope/pin incarnation before it
		 * dereferences borrowed pointers, and checks the actual held IR. */
		if (!cluster_wal_inputs_ctrc_subject_current_v1(a))
			return RF_PAGE_PROOF_DETAIL_ROOT_STALE;
		if (retention->scope != owners[0].source.retention.scope
			|| memcmp(&retention->input_owner, &owners[0].source.retention.input_owner,
					  sizeof(retention->input_owner)) != 0
			|| a->work_generation != owners[0].work_generation
			|| memcmp(&a->source.root, &owners[0].source.root, sizeof(a->source.root)) != 0)
			return RF_PAGE_PROOF_DETAIL_RETENTION_STALE;
		if (duty->system_identifier != identity->system_identifier
			|| memcmp(duty->storage_uuid, identity->storage_uuid, 16) != 0
			|| cut->failed_thread != duty->origin_thread_id
			|| cut->origin_owner_incarnation == 0
			|| cut->origin_owner_incarnation != duty->origin_owner_incarnation)
			return RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH;
		if (checkpoint->lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE
			|| selected->source.timeline == 0
			|| checkpoint->checkpoint_tli != selected->source.timeline
			|| checkpoint->tail_tli != checkpoint->checkpoint_tli
			|| cut->timeline_id != checkpoint->checkpoint_tli
			|| checkpoint->checkpoint_lower_lsn == InvalidXLogRecPtr
			|| checkpoint->validated_tail_lsn_exclusive <= checkpoint->checkpoint_lower_lsn
			|| cut->scan_begin_inclusive < checkpoint->checkpoint_lower_lsn
			|| cut->scan_end_exclusive > checkpoint->validated_tail_lsn_exclusive
			|| cut->scan_end_exclusive < cut->scan_begin_inclusive)
			return RF_PAGE_PROOF_DETAIL_SOURCE_GAP;
	}
	return RF_PAGE_PROOF_DETAIL_OK;
}

/* Set membership is independent of TT subjects. Reject a proper subset even
 * when the native scope still holds every omitted origin's IR. */
static RfPageProofDetailV1
ctrc_replay_owners_current(const RfPageIdentityV1 *identity,
	const RfContributorVectorV1 *vector,
	const ClusterRecoveryCtrcReplaySetSourceV1 *owners, uint32 count)
{
	bool seen[RF_PAGE_STABLE_MAX_PARTICIPANTS] = {false};

	if (!rf_page_identity_valid_v1(identity) || owners == NULL || count == 0
		|| count > RF_PAGE_STABLE_MAX_PARTICIPANTS || vector == NULL
		|| vector->cuts == NULL || vector->participant_count != count)
		return RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT;
	for (uint32 i = 0; i < count; i++) {
		const ClusterRecoveryCtrcReplaySetSourceV1 *a = &owners[i];
		const ClusterWalInputV1 *selected = a->generation_count != 0 ? &a->generation : &a->selected;
		const ClusterControlRootSnapshot *checkpoint = &selected->checkpoint;
		const ClusterRecoveryDutyKey *duty = &selected->source.claim.identity;
		const RfContributorStreamCutV1 *cut = &vector->cuts[i];

		/* Locate the native incarnation before reading any borrowed scope. */
		if (!cluster_wal_inputs_ctrc_replays_source_current_v1(a))
			return RF_PAGE_PROOF_DETAIL_ROOT_STALE;
		if ((a->generation_count != 0) != (owners[0].generation_count != 0))
			return RF_PAGE_PROOF_DETAIL_PARTICIPANT_MISSING;
		if (a->generation_count != 0) {
			if (a->generation_count != count || a->generation_index >= count
				|| seen[a->generation_index])
				return RF_PAGE_PROOF_DETAIL_PARTICIPANT_MISSING;
			seen[a->generation_index] = true;
		} else {
			if (a->replay_count != count || a->source_index >= count || seen[a->source_index])
				return RF_PAGE_PROOF_DETAIL_PARTICIPANT_MISSING;
			seen[a->source_index] = true;
		}
		if (a->scope != owners[0].scope
			|| memcmp(&a->input_owner, &owners[0].input_owner, sizeof(a->input_owner)) != 0
			|| a->work_generation != owners[0].work_generation
			|| memcmp(&a->root, &owners[0].root, sizeof(a->root)) != 0)
			return RF_PAGE_PROOF_DETAIL_RETENTION_STALE;
		if (duty->system_identifier != identity->system_identifier
			|| memcmp(duty->storage_uuid, identity->storage_uuid, 16) != 0
			|| cut->failed_thread != duty->origin_thread_id
			|| cut->origin_owner_incarnation == 0
			|| cut->origin_owner_incarnation != duty->origin_owner_incarnation)
			return RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH;
		/* Exact original full-input cuts, including empty terminals and the
		 * once-fixed local OPEN end. The original COMPLETE replay input still
		 * names the actual IR holder; it never replaces this generation. */
		if (a->generation_count != 0) {
			if (cut->timeline_id != selected->source.timeline
				|| cut->flags != a->generation_flags
				|| cut->scan_begin_inclusive != a->generation_begin
				|| cut->scan_end_exclusive != a->generation_end)
				return RF_PAGE_PROOF_DETAIL_SOURCE_GAP;
			continue;
		}
		if (checkpoint->lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE
			|| selected->source.timeline == 0
			|| checkpoint->checkpoint_tli != selected->source.timeline
			|| checkpoint->tail_tli != checkpoint->checkpoint_tli
			|| cut->timeline_id != checkpoint->checkpoint_tli
			|| checkpoint->checkpoint_lower_lsn == InvalidXLogRecPtr
			|| checkpoint->validated_tail_lsn_exclusive <= checkpoint->checkpoint_lower_lsn
			|| cut->scan_begin_inclusive < checkpoint->checkpoint_lower_lsn
			|| cut->scan_end_exclusive > checkpoint->validated_tail_lsn_exclusive
			|| cut->scan_end_exclusive < cut->scan_begin_inclusive)
			return RF_PAGE_PROOF_DETAIL_SOURCE_GAP;
	}
	return RF_PAGE_PROOF_DETAIL_OK;
}

static RfPageProofDetailV1
ctrc_proof_owners_current(const RfPageStableBaseProofV1 *proof)
{
	if (proof->ctrc_replay_owners != NULL)
		return ctrc_replay_owners_current(&proof->page_identity, proof->contributors,
			proof->ctrc_replay_owners, proof->participant_count);
	return ctrc_owners_current(&proof->page_identity, proof->contributors,
		proof->ctrc_owners, proof->participant_count);
}

/* Check pointer-bearing headers first: changed pointers never become reads of
 * a replacement cut/edge allocation. The original caller buffers stay alive
 * until destruction, just like the original borrowed owners. */
static bool
ctrc_original_bytes_current(const RfPageStableBaseProofV1 *proof)
{
	const RfPageStableCtrcStorageV1 *s = proof->ctrc_storage;

	return memcmp(s->original_vector, &s->original_header, sizeof(s->original_header)) == 0
		   && memcmp(s->original_source, &s->source, sizeof(s->source)) == 0
		   && memcmp(s->original_header.cuts, s->vector.cuts,
					 (Size)proof->participant_count * sizeof(*s->vector.cuts)) == 0
		   && (s->vector.edge_count == 0
			   || memcmp(s->original_header.edges, s->vector.edges,
						 (Size)s->vector.edge_count * sizeof(*s->vector.edges)) == 0)
		   && (proof->ctrc_replay_owners != NULL
			   ? memcmp(proof->ctrc_replay_authorities, proof->ctrc_replay_owners,
						(Size)proof->participant_count * sizeof(*proof->ctrc_replay_owners)) == 0
			   : memcmp(proof->ctrc_authorities, proof->ctrc_owners,
						(Size)proof->participant_count * sizeof(*proof->ctrc_owners)) == 0);
}

/* Run under the caller's unchanged original owners. No PAGE grant is made by
 * selecting an exact chain, and no caller boolean can supply native custody. */
static RfPageProofDetailV1
ctrc_build_preflight(RfPageStableBaseProofV1 *proof,
					 const RfPageStableGraphRequestV1 *original_graph,
					 const RfPageStableGraphRequestV1 *frozen_graph)
{
	RfPageStableCtrcStorageV1 *s = proof->ctrc_storage;
	RfPageStableGraphRequestV1 verified;
	RfPageProofDetailV1 detail;
	ClusterControlRootResult root;

	detail = ctrc_proof_owners_current(proof);
	if (detail != RF_PAGE_PROOF_DETAIL_OK)
		return detail;
	/* The same complete ROOT scope, including every retained source, is
	 * checked outside CF/PAGE locks. Existing CF WAIT remains retryable. */
	root = cluster_wal_inputs_revalidate_v1(proof->ctrc_replay_owners != NULL
		? proof->ctrc_replay_owners[0].scope : proof->ctrc_owners[0].source.retention.scope);
	if (root == CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE)
		return RF_PAGE_PROOF_DETAIL_WOULD_BLOCK;
	if (root != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		&& root != CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED)
		return RF_PAGE_PROOF_DETAIL_ROOT_STALE;
	if (memcmp(original_graph, frozen_graph, sizeof(*frozen_graph)) != 0
		|| !ctrc_original_bytes_current(proof))
		return RF_PAGE_PROOF_DETAIL_ROOT_STALE;
	detail = ctrc_proof_owners_current(proof);
	if (detail != RF_PAGE_PROOF_DETAIL_OK)
		return detail;
	memcpy(&verified, frozen_graph, sizeof(verified));
	verified.source = &s->source;
	verified.contributors = &s->vector;
	verified.root_current = verified.duty_current = verified.fence_current = true;
	verified.retention_current = true;
	verified.retention_binding_cookie = verified.current_retention_binding_cookie = 1;
	detail = rf_page_stable_base_select_v1(&verified, proof->ctrc_scratch,
										 Max(s->vector.edge_count, 1), &proof->selection);
	if (detail != RF_PAGE_PROOF_DETAIL_OK)
		return detail;
	if (memcmp(original_graph, frozen_graph, sizeof(*frozen_graph)) != 0
		|| !ctrc_original_bytes_current(proof))
		return RF_PAGE_PROOF_DETAIL_ROOT_STALE;
	return ctrc_proof_owners_current(proof);
}

static bool
ctrc_private_allocate(RfPageStableBaseProofV1 *proof,
	Size storage_bytes, Size owner_bytes, Size scratch_bytes)
{
	proof->ctrc_storage = ctrc_alloc0(storage_bytes);
	if (proof->ctrc_replay_authorities != NULL)
		proof->ctrc_replay_owners = ctrc_alloc0(owner_bytes);
	else
		proof->ctrc_owners = ctrc_alloc0(owner_bytes);
	proof->ctrc_scratch = ctrc_alloc0(scratch_bytes);
	return proof->ctrc_storage != NULL && proof->ctrc_scratch != NULL
		&& (proof->ctrc_replay_authorities != NULL
			? proof->ctrc_replay_owners != NULL : proof->ctrc_owners != NULL);
}

static void
ctrc_snapshot_graph(RfPageStableBaseProofV1 *proof,
	const RfPageStableGraphRequestV1 *frozen, const RfContributorVectorV1 *header)
{
	RfPageStableCtrcStorageV1 *s = proof->ctrc_storage;
	Size cut_bytes = (Size)proof->participant_count * sizeof(*header->cuts);
	char *cuts = (char *)s + MAXALIGN(sizeof(*s));
	char *edges = cuts + MAXALIGN(cut_bytes);

	if (proof->ctrc_replay_owners != NULL)
		memcpy(proof->ctrc_replay_owners, proof->ctrc_replay_authorities,
			(Size)proof->participant_count * sizeof(*proof->ctrc_replay_owners));
	else
		memcpy(proof->ctrc_owners, proof->ctrc_authorities,
			(Size)proof->participant_count * sizeof(*proof->ctrc_owners));
	s->original_source = frozen->source;
	s->original_vector = frozen->contributors;
	memcpy(&s->original_header, header, sizeof(*header));
	memcpy(&s->source, frozen->source, sizeof(s->source));
	memcpy(&s->vector, header, sizeof(*header));
	memcpy(cuts, header->cuts, cut_bytes);
	if (header->edge_count != 0)
		memcpy(edges, header->edges, (Size)header->edge_count * sizeof(*header->edges));
	s->vector.cuts = (const RfContributorStreamCutV1 *)cuts;
	s->vector.edges = (const RfPageStableEdgeInputV1 *)edges;
	proof->source = &s->source;
	proof->contributors = &s->vector;
}

static RfPageStableCtrcBuildV1
ctrc_proof_build(const RfPageStableGraphRequestV1 *graph,
	const ClusterRecoveryCtrcSubjectAuthorityV1 *authorities,
	const ClusterRecoveryCtrcReplaySetSourceV1 *replay_authorities,
	uint32 count, Size memory_budget)
{
	RfPageStableCtrcBuildV1 result = {NULL, RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT};
	RfPageStableBaseProofV1 *proof;
	RfPageStableGraphRequestV1 frozen;
	RfContributorVectorV1 header;
	Size cut_bytes, edge_bytes, storage_bytes, owner_bytes, scratch_bytes, budget;
	uint32 edges;

	if (graph == NULL || (authorities == NULL) == (replay_authorities == NULL) || count == 0
		|| count > RF_PAGE_STABLE_MAX_PARTICIPANTS || graph->participant_count != count
		|| graph->source == NULL || graph->contributors == NULL
		|| graph->contributors->participant_count != count || graph->contributors->cuts == NULL
		|| graph->contributors->edge_count > RF_PAGE_STABLE_MAX_EDGES
		|| (graph->contributors->edge_count != 0 && graph->contributors->edges == NULL))
		return result;
	/* Count bounds above make every size and this sum bounded even at Size32.
	 * Include both selector temporary arrays, conservatively at the same time. */
	memcpy(&frozen, graph, sizeof(frozen));
	memcpy(&header, graph->contributors, sizeof(header));
	edges = header.edge_count;
	cut_bytes = (Size)count * sizeof(*header.cuts);
	edge_bytes = (Size)edges * sizeof(*header.edges);
	owner_bytes = (Size)count * (replay_authorities != NULL
		? sizeof(*replay_authorities) : sizeof(*authorities));
	scratch_bytes = (Size)Max(edges, 1) * sizeof(uint32);
	storage_bytes = MAXALIGN(sizeof(RfPageStableCtrcStorageV1)) + MAXALIGN(cut_bytes) + edge_bytes;
	budget = sizeof(*proof) + owner_bytes + storage_bytes + scratch_bytes
		+ (Size)count * sizeof(uint32) + (Size)edges * sizeof(uint32);
	if (memory_budget < budget) {
		result.detail = RF_PAGE_PROOF_DETAIL_CAPACITY;
		return result;
	}
	proof = ctrc_alloc0(sizeof(*proof));
	if (proof == NULL) {
		result.detail = RF_PAGE_PROOF_DETAIL_OOM;
		return result;
	}
	proof->magic = RF_PAGE_STABLE_PROOF_MAGIC;
	proof->participant_count = count;
	memcpy(&proof->page_identity, &frozen.page_identity, sizeof(proof->page_identity));
	memcpy(&proof->expected_result, &frozen.expected_result, sizeof(proof->expected_result));
	proof->ctrc_authorities = authorities;
	proof->ctrc_replay_authorities = replay_authorities;
	/* All allocations after this point are reachable from proof on ERROR.
	 * Only its private copies are destroyed; original owners remain held. */
	PG_TRY();
	{
		if (!ctrc_private_allocate(proof, storage_bytes, owner_bytes, scratch_bytes))
			result.detail = RF_PAGE_PROOF_DETAIL_OOM;
		else {
			ctrc_snapshot_graph(proof, &frozen, &header);
			result.detail = ctrc_build_preflight(proof, graph, &frozen);
		}
	}
	PG_CATCH();
	{
		rf_page_stable_base_proof_destroy_v1(&proof);
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (result.detail == RF_PAGE_PROOF_DETAIL_OK) {
		ctrc_free(proof->ctrc_scratch);
		proof->ctrc_scratch = NULL;
		result.proof = proof;
	} else
		rf_page_stable_base_proof_destroy_v1(&proof);
	return result;
}

RfPageStableCtrcBuildV1
rf_page_stable_base_proof_build_ctrc_v1(const RfPageStableGraphRequestV1 *graph,
	const ClusterRecoveryCtrcSubjectAuthorityV1 *authorities, uint32 count, Size memory_budget)
{
	return ctrc_proof_build(graph, authorities, NULL, count, memory_budget);
}

RfPageStableCtrcBuildV1
rf_page_stable_base_proof_build_ctrc_replays_v1(const RfPageStableGraphRequestV1 *graph,
	const ClusterRecoveryCtrcReplaySetSourceV1 *authorities, uint32 count, Size memory_budget)
{
	return ctrc_proof_build(graph, NULL, authorities, count, memory_budget);
}

bool
rf_page_stable_base_proof_matches_ctrc_v1(const RfPageStableBaseProofV1 *proof,
	const RfPageIdentityV1 *page_identity, const RfPageVersionV1 *expected_result,
	const ClusterRecoveryCtrcSubjectAuthorityV1 *authorities, uint32 count,
	const RfPagePinnedSourceV1 *source, const RfContributorVectorV1 *contributors)
{
	if (proof == NULL || proof->magic != RF_PAGE_STABLE_PROOF_MAGIC
		|| proof->ctrc_owners == NULL || proof->ctrc_storage == NULL
		|| authorities == NULL || proof->ctrc_authorities != authorities
		|| count == 0 || count != proof->participant_count
		|| !rf_page_identity_equal_v1(&proof->page_identity, page_identity)
		|| !rf_page_version_equal_v1(&proof->expected_result, expected_result)
		|| !rf_page_version_equal_v1(&proof->selection.terminal_version, expected_result)
		|| proof->ctrc_storage->original_source != source
		|| proof->ctrc_storage->original_vector != contributors
		|| !ctrc_original_bytes_current(proof))
		return false;
	return ctrc_owners_current(&proof->page_identity, proof->contributors,
							  proof->ctrc_owners, count) == RF_PAGE_PROOF_DETAIL_OK;
}

bool
rf_page_stable_base_proof_matches_ctrc_replays_v1(const RfPageStableBaseProofV1 *proof,
	const RfPageIdentityV1 *page_identity, const RfPageVersionV1 *expected_result,
	const ClusterRecoveryCtrcReplaySetSourceV1 *authorities, uint32 count,
	const RfPagePinnedSourceV1 *source, const RfContributorVectorV1 *contributors)
{
	if (proof == NULL || proof->magic != RF_PAGE_STABLE_PROOF_MAGIC
		|| proof->ctrc_replay_owners == NULL || proof->ctrc_storage == NULL
		|| authorities == NULL || proof->ctrc_replay_authorities != authorities
		|| count == 0 || count != proof->participant_count
		|| !rf_page_identity_equal_v1(&proof->page_identity, page_identity)
		|| !rf_page_version_equal_v1(&proof->expected_result, expected_result)
		|| !rf_page_version_equal_v1(&proof->selection.terminal_version, expected_result)
		|| proof->ctrc_storage->original_source != source
		|| proof->ctrc_storage->original_vector != contributors
		|| !ctrc_original_bytes_current(proof))
		return false;
	return ctrc_replay_owners_current(&proof->page_identity, proof->contributors,
		proof->ctrc_replay_owners, count) == RF_PAGE_PROOF_DETAIL_OK;
}
