/*-------------------------------------------------------------------------
 *
 * cluster_pcm_direct_init.c
 *	Process-local exact proof for PCM direct initialization.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_pcm_direct_init.h"

static bool
cluster_pcm_direct_init_kind_valid(ClusterPcmDirectInitKind kind)
{
	return kind >= CLUSTER_PCM_DIRECT_INIT_READ_MISS && kind <= CLUSTER_PCM_DIRECT_INIT_FSM;
}

static ClusterPcmOwnResult
cluster_pcm_direct_init_snapshot_validate(ClusterPcmDirectInitKind kind,
										  const ClusterPcmDirectInitSnapshot *snapshot)
{
	ClusterPcmOwnResult live_result;
	bool valid_page_kind;

	if (!cluster_pcm_direct_init_kind_valid(kind) || snapshot == NULL)
		return CLUSTER_PCM_OWN_INVALID;

	live_result = cluster_pcm_own_classify_live_flags(snapshot->flags, snapshot->reservation_token);
	if (live_result != CLUSTER_PCM_OWN_OK)
		return live_result;

	if (snapshot->generation == UINT64_MAX)
		return CLUSTER_PCM_OWN_EXHAUSTED;
	if (snapshot->buf_id < 0 || snapshot->private_refcount <= 0
		|| BUF_STATE_GET_REFCOUNT(snapshot->buf_state) == 0)
		return CLUSTER_PCM_OWN_STALE;
	if ((snapshot->buf_state & BM_TAG_VALID) == 0
		|| (snapshot->buf_state & (BM_DIRTY | BM_JUST_DIRTIED | BM_IO_ERROR)) != 0
		|| snapshot->buffer_type != (uint8)BUF_TYPE_CURRENT
		|| snapshot->pcm_state != (uint8)PCM_STATE_N || snapshot->writer_activation_token != 0
		|| snapshot->resource_x_activation_generation != 0 || !snapshot->page_is_new)
		return CLUSTER_PCM_OWN_STALE;

	valid_page_kind = kind == CLUSTER_PCM_DIRECT_INIT_VM || kind == CLUSTER_PCM_DIRECT_INIT_FSM;
	if (valid_page_kind) {
		if ((snapshot->buf_state & BM_VALID) == 0 || (snapshot->buf_state & BM_IO_IN_PROGRESS) != 0)
			return CLUSTER_PCM_OWN_STALE;
	} else if ((snapshot->buf_state & BM_VALID) != 0
			   || (snapshot->buf_state & BM_IO_IN_PROGRESS) == 0)
		return CLUSTER_PCM_OWN_STALE;

	if ((kind == CLUSTER_PCM_DIRECT_INIT_VM && snapshot->tag.forkNum != VISIBILITYMAP_FORKNUM)
		|| (kind == CLUSTER_PCM_DIRECT_INIT_FSM && snapshot->tag.forkNum != FSM_FORKNUM))
		return CLUSTER_PCM_OWN_STALE;

	return CLUSTER_PCM_OWN_OK;
}

ClusterPcmOwnResult
cluster_pcm_direct_init_proof_arm(ClusterPcmDirectInitKind kind,
								  const ClusterPcmDirectInitSnapshot *snapshot,
								  ClusterPcmDirectInitProof *out_proof)
{
	ClusterPcmOwnResult result;

	if (out_proof == NULL)
		return CLUSTER_PCM_OWN_INVALID;
	memset(out_proof, 0, sizeof(*out_proof));

	result = cluster_pcm_direct_init_snapshot_validate(kind, snapshot);
	if (result != CLUSTER_PCM_OWN_OK)
		return result;

	out_proof->identity = *snapshot;
	out_proof->kind = kind;
	out_proof->armed = true;
	return CLUSTER_PCM_OWN_OK;
}

ClusterPcmOwnResult
cluster_pcm_direct_init_proof_consume(ClusterPcmDirectInitKind kind,
									  const ClusterPcmDirectInitSnapshot *snapshot,
									  ClusterPcmDirectInitProof *proof)
{
	ClusterPcmOwnResult result;
	ClusterPcmDirectInitSnapshot *expected;

	if (proof == NULL || !proof->armed)
		return CLUSTER_PCM_OWN_STALE;

	/* Failure consumes too: callers cannot refresh a rejected operation by
	 * retrying an old proof after the buffer identity has changed. */
	proof->armed = false;
	if (kind != proof->kind)
		return CLUSTER_PCM_OWN_STALE;

	result = cluster_pcm_direct_init_snapshot_validate(kind, snapshot);
	if (result != CLUSTER_PCM_OWN_OK)
		return result;

	expected = &proof->identity;
	if (snapshot->buf_id != expected->buf_id || !BufferTagsEqual(&snapshot->tag, &expected->tag)
		|| snapshot->generation != expected->generation
		|| snapshot->reservation_token != expected->reservation_token
		|| snapshot->writer_activation_token != expected->writer_activation_token
		|| snapshot->resource_x_activation_generation != expected->resource_x_activation_generation
		|| snapshot->private_refcount != expected->private_refcount)
		return CLUSTER_PCM_OWN_STALE;

	return CLUSTER_PCM_OWN_OK;
}

static ClusterPcmOwnResult
cluster_pcm_direct_init_target_shape_validate(ClusterPcmDirectInitKind kind,
											  const ClusterPcmDirectInitSnapshot *snapshot,
											  uint8 expected_buffer_type, uint8 expected_pcm_state,
											  uint32 expected_flags)
{
	bool valid_page_kind;

	if (!cluster_pcm_direct_init_kind_valid(kind) || snapshot == NULL)
		return CLUSTER_PCM_OWN_INVALID;
	if (snapshot->buf_id < 0 || snapshot->private_refcount <= 0
		|| BUF_STATE_GET_REFCOUNT(snapshot->buf_state) == 0
		|| (snapshot->buf_state & BM_TAG_VALID) == 0
		|| (snapshot->buf_state & (BM_DIRTY | BM_JUST_DIRTIED | BM_IO_ERROR)) != 0
		|| snapshot->buffer_type != expected_buffer_type
		|| snapshot->pcm_state != expected_pcm_state || snapshot->flags != expected_flags
		|| snapshot->writer_activation_token != 0 || snapshot->resource_x_activation_generation != 0
		|| !snapshot->page_is_new)
		return CLUSTER_PCM_OWN_STALE;

	valid_page_kind = kind == CLUSTER_PCM_DIRECT_INIT_VM || kind == CLUSTER_PCM_DIRECT_INIT_FSM;
	if (valid_page_kind) {
		if ((snapshot->buf_state & BM_VALID) == 0 || (snapshot->buf_state & BM_IO_IN_PROGRESS) != 0)
			return CLUSTER_PCM_OWN_STALE;
	} else if ((snapshot->buf_state & BM_VALID) != 0
			   || (snapshot->buf_state & BM_IO_IN_PROGRESS) == 0)
		return CLUSTER_PCM_OWN_STALE;
	if ((kind == CLUSTER_PCM_DIRECT_INIT_VM && snapshot->tag.forkNum != VISIBILITYMAP_FORKNUM)
		|| (kind == CLUSTER_PCM_DIRECT_INIT_FSM && snapshot->tag.forkNum != FSM_FORKNUM))
		return CLUSTER_PCM_OWN_STALE;
	return CLUSTER_PCM_OWN_OK;
}

/* A second VM/FSM initializer may observe the exact GRANT_PENDING sidecar
 * published by the process-local proof owner.  This predicate grants no
 * initialization authority and cannot arm a proof: it only validates the
 * immutable known-new shape needed to join that existing Resource-X round. */
ClusterPcmOwnResult
cluster_pcm_direct_init_aux_pending_observer_validate(ClusterPcmDirectInitKind kind,
													  const ClusterPcmDirectInitSnapshot *snapshot)
{
	ClusterPcmOwnResult live_result;

	if ((kind != CLUSTER_PCM_DIRECT_INIT_VM && kind != CLUSTER_PCM_DIRECT_INIT_FSM)
		|| snapshot == NULL)
		return CLUSTER_PCM_OWN_INVALID;
	if (snapshot->generation == UINT64_MAX)
		return CLUSTER_PCM_OWN_EXHAUSTED;
	if (snapshot->reservation_token == 0 || snapshot->reservation_token == UINT64_MAX)
		return CLUSTER_PCM_OWN_STALE;
	if (snapshot->flags != PCM_OWN_FLAG_GRANT_PENDING) {
		live_result
			= cluster_pcm_own_classify_live_flags(snapshot->flags, snapshot->reservation_token);
		return live_result == CLUSTER_PCM_OWN_OK ? CLUSTER_PCM_OWN_STALE : live_result;
	}
	return cluster_pcm_direct_init_target_shape_validate(
		kind, snapshot, (uint8)BUF_TYPE_CURRENT, (uint8)PCM_STATE_N, PCM_OWN_FLAG_GRANT_PENDING);
}

static bool
cluster_pcm_direct_init_target_identity_matches(const ClusterPcmDirectInitSnapshot *snapshot,
												const ClusterPcmDirectInitSnapshot *identity)
{
	return snapshot->buf_id == identity->buf_id && BufferTagsEqual(&snapshot->tag, &identity->tag)
		   && snapshot->private_refcount == identity->private_refcount
		   && snapshot->writer_activation_token == 0
		   && snapshot->resource_x_activation_generation == 0;
}

ClusterPcmOwnResult
cluster_pcm_direct_init_target_pending_validate(ClusterPcmDirectInitKind kind,
												const ClusterPcmDirectInitSnapshot *pending,
												const ClusterPcmDirectInitProof *consumed_proof)
{
	const ClusterPcmDirectInitSnapshot *identity;
	ClusterPcmOwnResult result;

	if (pending == NULL || consumed_proof == NULL)
		return CLUSTER_PCM_OWN_INVALID;
	if (consumed_proof->armed || consumed_proof->kind != kind)
		return CLUSTER_PCM_OWN_STALE;
	identity = &consumed_proof->identity;
	result = cluster_pcm_direct_init_snapshot_validate(kind, identity);
	if (result != CLUSTER_PCM_OWN_OK)
		return result;
	result = cluster_pcm_direct_init_target_shape_validate(
		kind, pending, (uint8)BUF_TYPE_CURRENT, (uint8)PCM_STATE_N, PCM_OWN_FLAG_GRANT_PENDING);
	if (result != CLUSTER_PCM_OWN_OK)
		return result;
	if (!cluster_pcm_direct_init_target_identity_matches(pending, identity)
		|| pending->generation != identity->generation || identity->reservation_token == UINT64_MAX
		|| pending->reservation_token != identity->reservation_token + 1)
		return CLUSTER_PCM_OWN_STALE;
	return CLUSTER_PCM_OWN_OK;
}

ClusterPcmOwnResult
cluster_pcm_direct_init_target_commit_validate(ClusterPcmDirectInitKind kind,
											   const ClusterPcmDirectInitSnapshot *committed,
											   const ClusterPcmDirectInitProof *consumed_proof)
{
	const ClusterPcmDirectInitSnapshot *identity;
	ClusterPcmOwnResult result;

	if (committed == NULL || consumed_proof == NULL)
		return CLUSTER_PCM_OWN_INVALID;
	if (consumed_proof->armed || consumed_proof->kind != kind)
		return CLUSTER_PCM_OWN_STALE;
	identity = &consumed_proof->identity;
	result = cluster_pcm_direct_init_snapshot_validate(kind, identity);
	if (result != CLUSTER_PCM_OWN_OK)
		return result;
	result = cluster_pcm_direct_init_target_shape_validate(kind, committed, (uint8)BUF_TYPE_XCUR,
														   (uint8)PCM_STATE_X, 0);
	if (result != CLUSTER_PCM_OWN_OK)
		return result;
	if (!cluster_pcm_direct_init_target_identity_matches(committed, identity)
		|| identity->generation == UINT64_MAX || committed->generation != identity->generation + 1
		|| identity->reservation_token == UINT64_MAX
		|| committed->reservation_token != identity->reservation_token + 1)
		return CLUSTER_PCM_OWN_STALE;
	return CLUSTER_PCM_OWN_OK;
}
