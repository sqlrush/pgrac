/*-------------------------------------------------------------------------
 * cluster_pi_write.h
 *    Exact master/current-holder DATA completion.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_PI_WRITE_H
#define CLUSTER_PI_WRITE_H

#include "cluster/cluster_pcm_lock.h"
#include "cluster/cluster_page_data.h"

#ifdef USE_PGRAC_CLUSTER
/* Process-local projection, never a grant or a persistent record. */
typedef struct ClusterPcmPiWriteCutV1 {
	ResourceXMasterSnapshot holder;
	uint64 binding_generation;
	uint64 transition_count;
	uint64 master_generation;
	int32 master_node;
	uint32 pi_holders_bitmap;
} ClusterPcmPiWriteCutV1;

StaticAssertDecl(sizeof(ClusterPcmPiWriteCutV1) == 128,
				 "PI write cut must remain a bounded value projection");

static inline bool
cluster_pcm_pi_write_cut_valid_v1(const ClusterPcmPiWriteCutV1 *cut)
{
	const ResourceXMasterSnapshot *h;
	if (cut == NULL)
		return false;
	h = &cut->holder;
	return cut->binding_generation != 0 && cut->binding_generation != UINT64_MAX
		   && cut->transition_count != 0 && cut->transition_count != UINT64_MAX
		   && cut->master_generation != 0 && cut->master_generation != UINT64_MAX
		   && cut->master_node >= 0 && cut->master_node < RESOURCE_X_PROTOCOL_NODE_LIMIT
		   && h->assertion.requester_node >= 0
		   && h->assertion.requester_node < RESOURCE_X_PROTOCOL_NODE_LIMIT
		   && (h->assertion.resource.forkNum == MAIN_FORKNUM
			   || h->assertion.resource.forkNum == VISIBILITYMAP_FORKNUM)
		   && h->phase == RESOURCE_X_MASTER_SETTLED && h->is_head == 0 && h->resource_formation != 0
		   && h->resource_formation != UINT64_MAX && h->master_session_incarnation != 0
		   && h->master_session_incarnation != UINT64_MAX && h->assertion_sequence != 0
		   && h->assertion_sequence != UINT64_MAX && h->base_authority_generation != 0
		   && h->final_authority_generation > h->base_authority_generation
		   && h->final_authority_generation != UINT64_MAX && h->requester_target_generation != 0
		   && h->requester_target_generation != UINT64_MAX
		   && h->blocked_holders_bitmap == h->incompatible_holders_bitmap;
}

/* Read only; requires the local master and an exactly settled current X.
 * A zero PI set has no work. False leaves out unchanged and creates no entry. */
extern bool cluster_pcm_lock_pi_write_snapshot_v1(BufferTag tag, ClusterPcmPiWriteCutV1 *out);

/* Actual holder DATA write. The cut is captured before this call; it must
 * match the installed current-X buffer generation. No ownership acquisition.
 * A receipt from the ordinary endpoint cannot retire master PI obligations. */
extern bool cluster_bufmgr_write_page_data_at_cut_v1(const ClusterPageDataTargetV1 *target,
													 const ClusterPcmPiWriteCutV1 *cut,
													 ClusterPageDataReceiptV1 **out);

/* Background current-block variant: the requested incarnation must match,
 * while the actual version is sampled under the installed holder's content
 * lock. Later local mutations do not turn owned checkpoint work into errors. */
extern bool cluster_bufmgr_write_current_data_at_cut_v1(const ClusterPageDataTargetV1 *target,
														const ClusterPcmPiWriteCutV1 *cut,
														ClusterPageDataReceiptV1 **out);

/* Validate ancestry through the actual receipt in a sealed full-source PAGE
 * plan. Later page versions remain uncovered in its contribution prefix.
 * This projection grants neither checkpoint publication nor WAL reuse. */
extern bool cluster_page_data_pi_proof_v1(const ClusterPageDataReceiptV1 *receipt,
										  const RfPageOnlinePlanV1 *plan,
										  const ClusterWalSourceRef *sources, uint32 source_count,
										  ClusterPcmPiWriteCutV1 *out);

/* Clear only the unchanged master cut after exact DATA/ancestry proof.
 * The caller retains the returned holders until their background discard
 * notification is delivered. An unchanged already-cleared cut is idempotent.
 * False clears holders_out but no shared state. */
extern bool cluster_pcm_lock_pi_write_complete_v1(const ClusterPageDataReceiptV1 *receipt,
												  const RfPageOnlinePlanV1 *plan,
												  const ClusterWalSourceRef *sources,
												  uint32 source_count, uint32 *holders_out);
#endif
#endif /* CLUSTER_PI_WRITE_H */
