/*-------------------------------------------------------------------------
 *
 * cluster_r4_observe.c
 *	Thin observation-only adapter for Stage 8 R4 events.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "cluster/cluster_cr.h"
#include "cluster/cluster_conf.h"
#include "cluster/cluster_r4_observe.h"

void
cluster_r4_observe(ClusterR4Event event, ClusterTxResolveReason tx_reason,
				   ClusterCrBuildReason cr_reason)
{
	/* Reasons remain typed at the producer boundary.  The current R1 carrier
	 * is the monotonic event counter surface, never a decision input. */
	(void)tx_reason;
	(void)cr_reason;
	if ((uint32)event >= CLUSTER_R4_OBSERVATION_EVENT_COUNT)
		return;
	cluster_cr_r4_event_bump((uint32)event);
}

const char *
cluster_r4_refusal_stage_name(ClusterR4RefusalStage stage)
{
	switch (stage) {
	case CLUSTER_R4_REFUSAL_MASTER:
		return "master";
	case CLUSTER_R4_REFUSAL_HOLDER_ADMISSION:
		return "holder_admission";
	case CLUSTER_R4_REFUSAL_HOLDER_SHIP:
		return "holder_ship";
	}
	return "unknown";
}

/* Count at the real refusal boundary, not by decoding multiplexed status25.
 * Local powers-of-two logging preserves the first exact context without
 * letting a repeatedly refused request flood the server log.  These values
 * are observations only and never consulted by admission or transport. */
void
cluster_r4_observe_refusal(ClusterR4RefusalStage stage, ClusterCrBuildReason reason,
						   const BufferTag *tag, uint64 request_id, uint64 epoch, int32 requester,
						   int32 master, SCN read_scn)
{
	static uint64 seen[CLUSTER_R4_REFUSAL_STAGE_COUNT][CLUSTER_R4_REFUSAL_REASON_COUNT];
	uint64 count;

	if ((uint32)stage >= CLUSTER_R4_REFUSAL_STAGE_COUNT || tag == NULL
		|| reason <= CLUSTER_CR_BUILD_NONE || reason > CLUSTER_CR_BUILD_PROTOCOL)
		return;
	cluster_cr_r4_event_bump(CLUSTER_R4_REFUSAL_EVENT(stage, reason));
	if (seen[stage][reason] < UINT64_MAX)
		seen[stage][reason]++;
	count = seen[stage][reason];
	if ((count & (count - 1)) != 0)
		return;
	ereport(LOG, (errmsg_internal("R4 CR producer refusal"),
				  errdetail("PGRAC_FAMILY=R4_CR_DIAGNOSTIC PGRAC_REASON=%s "
							"stage=%s node=%d requester=%d master=%d tag=%u/%u/%u/%u/%u "
							"request=" UINT64_FORMAT " epoch=" UINT64_FORMAT
							" read_scn=" UINT64_FORMAT " process_reason_count=" UINT64_FORMAT,
							cluster_cr_build_reason_name(reason),
							cluster_r4_refusal_stage_name(stage), cluster_node_id, requester,
							master, tag->spcOid, tag->dbOid, tag->relNumber, (unsigned)tag->forkNum,
							tag->blockNum, request_id, epoch, (uint64)read_scn, count)));
}

#endif /* USE_PGRAC_CLUSTER */
