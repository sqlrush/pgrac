/*-------------------------------------------------------------------------
 * cluster_tx_identity_trace.c
 *   Finite process-local identity logs. No authority lookups or shared state.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 * Portions Copyright (c) 2026, pgrac contributors
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_tx_identity_trace.h"
#include "miscadmin.h"
#include "storage/proc.h"

#ifdef USE_PGRAC_CLUSTER
#define ORIGIN_DEDUP_SIZE 128
static uint32 counts[CLTXID_NKINDS];
static uint64 sequence;
static ClusterTxIdentityTrace origin_seen[ORIGIN_DEDUP_SIZE];
static uint32 origin_next;
static uint32 origin_count;

void
cluster_tx_identity_trace_emit(ClusterTxIdentityTraceKind kind, const ClusterTxIdentityTrace *row)
{
	static const char *const names[CLTXID_NKINDS] = {
		"RESERVE", "ACTIVE", "RETRY", "ALIAS", "LOCAL_TERMINAL", "TERMINAL",
		"WAIT_SOURCE", "WAIT_TARGET", "WAIT_END", "ORIGIN_MAP", "WAKE", "WAKE_SUMMARY"
	};
	uint32 *count;

	if (!cluster_trace_tx_identity || row == NULL || kind < 0 || kind >= CLTXID_NKINDS)
		return;
	count = &counts[kind];
	if (*count > CLUSTER_TX_IDENTITY_TRACE_LIMIT)
		return;
	if (*count == CLUSTER_TX_IDENTITY_TRACE_LIMIT) {
		(*count)++;
		ereport(LOG, (errmsg_internal("TX_IDENTITY event=LIMIT family=%s node=%d pid=%d "
			"limit=%u evidence_complete=0", names[kind], cluster_node_id, MyProcPid,
			CLUSTER_TX_IDENTITY_TRACE_LIMIT)));
		return;
	}
	/* Diagnostic-only deduplication. A changed physical key or outcome is
	 * retained. Eviction may repeat a map, never supplies an authority hit. */
	if (kind == CLTXID_ORIGIN_MAP) {
		for (uint32 i = 0; i < origin_count; i++)
			if (memcmp(&origin_seen[i], row, sizeof(*row)) == 0)
				return;
		origin_seen[origin_next] = *row;
		origin_next = (origin_next + 1) % ORIGIN_DEDUP_SIZE;
		if (origin_count < ORIGIN_DEDUP_SIZE)
			origin_count++;
	}
	(*count)++;
	sequence++;
	ereport(LOG, (errmsg_internal(
		"TX_IDENTITY event=%s seq=" UINT64_FORMAT " actor_node=%d pid=%d proc=%u "
		"node=%d xid=%u epoch=" UINT64_FORMAT " "
		"tt_known=%d tt_segment=%u tt_slot=%u tt_wrap=%u generation_known=%d tt_generation=%u "
		"source_known=%d source_segment=%u source_slot_id=%u target_known=%d uba_known=%d "
		"uba0=%016" INT64_MODIFIER "x uba1=%016" INT64_MODIFIER "x "
		"itl_wrap=%u itl_kind=%u itl_slot=%u wait_seq=" UINT64_FORMAT " "
		"status=%d peer_proc=%u count=%u skipped_targets=%u omitted=%u",
		names[kind], sequence, cluster_node_id, MyProcPid,
		MyProc == NULL ? UINT32_MAX : (uint32)MyProc->pgprocno,
		row->node, row->xid, row->epoch, row->tt_known, row->tt_segment, row->tt_slot,
		row->tt_wrap, row->generation_known, row->tt_generation, row->source_known,
		row->source_segment, row->source_slot_id, row->target_known, row->uba_known, row->uba0, row->uba1,
		row->itl_wrap, row->itl_kind, row->itl_slot, row->wait_seq, row->status,
		row->peer_procno, row->count, row->skipped_targets, row->omitted)));
}
#endif
