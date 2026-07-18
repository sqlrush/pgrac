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
		|| (snapshot->buf_state & (BM_DIRTY | BM_JUST_DIRTIED)) != 0
		|| snapshot->buffer_type != (uint8)BUF_TYPE_CURRENT
		|| snapshot->pcm_state != (uint8)PCM_STATE_N || !snapshot->page_is_new)
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
		|| snapshot->private_refcount != expected->private_refcount)
		return CLUSTER_PCM_OWN_STALE;

	return CLUSTER_PCM_OWN_OK;
}
