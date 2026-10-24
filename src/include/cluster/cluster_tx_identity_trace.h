/*-------------------------------------------------------------------------
 * cluster_tx_identity_trace.h
 *   Opt-in identity observations, never a transaction authority carrier.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 * Portions Copyright (c) 2026, pgrac contributors
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_TX_IDENTITY_TRACE_H
#define CLUSTER_TX_IDENTITY_TRACE_H

#include "cluster/cluster_tt_status.h"
#include "cluster/cluster_tx_resolve.h"

#ifdef USE_PGRAC_CLUSTER

#define CLUSTER_TX_IDENTITY_TRACE_LIMIT 32768U
typedef enum ClusterTxIdentityTraceKind {
	CLTXID_RESERVE, CLTXID_ACTIVE, CLTXID_RETRY, CLTXID_ALIAS,
	CLTXID_LOCAL_TERMINAL, CLTXID_TERMINAL, CLTXID_WAIT_SOURCE,
	CLTXID_WAIT_TARGET, CLTXID_WAIT_END, CLTXID_ORIGIN_MAP,
	CLTXID_WAKE, CLTXID_WAKE_SUMMARY, CLTXID_NKINDS
} ClusterTxIdentityTraceKind;

typedef struct ClusterTxIdentityTrace {
	uint64 epoch;
	uint64 wait_seq;
	uint64 uba0;
	uint64 uba1;
	uint32 xid;
	uint32 tt_segment;
	uint32 tt_slot;
	uint32 tt_wrap;
	uint32 tt_generation;
	uint32 source_segment;
	uint32 source_slot_id;
	uint32 peer_procno;
	uint32 count;
	uint32 skipped_targets;
	uint32 omitted;
	int32 node;
	int32 status;
	uint16 itl_wrap;
	uint8 itl_kind;
	uint8 itl_slot;
	bool tt_known;
	bool generation_known;
	bool source_known;
	bool target_known;
	bool uba_known;
} ClusterTxIdentityTrace;

extern PGDLLIMPORT bool cluster_trace_tx_identity;
extern void cluster_tx_identity_trace_emit(ClusterTxIdentityTraceKind kind,
										 const ClusterTxIdentityTrace *row);

static inline void
cluster_tx_identity_trace_canonical(ClusterTxIdentityTrace *row, int32 node, uint32 xid,
	uint32 segment, uint32 slot, uint32 wrap, uint32 generation, bool generation_known,
	uint64 epoch)
{
	memset(row, 0, sizeof(*row));
	row->peer_procno = UINT32_MAX;
	row->node = node;
	row->xid = xid;
	row->epoch = epoch;
	row->tt_known = true;
	row->tt_segment = segment;
	row->tt_slot = slot;
	row->tt_wrap = wrap;
	row->generation_known = generation_known;
	row->tt_generation = generation;
}

static inline void
cluster_tx_identity_trace_source(ClusterTxIdentityTrace *row, const ClusterTTStatusKey *key)
{
	memset(row, 0, sizeof(*row));
	row->peer_procno = UINT32_MAX;
	row->node = key->origin_node_id;
	row->xid = key->local_xid;
	row->epoch = key->cluster_epoch;
	row->source_known = true;
	row->source_segment = key->undo_segment_id;
	row->source_slot_id = key->tt_slot_id;
}

/* The requester's ITL wrap must never be labelled canonical TT wrap. */
static inline void
cluster_tx_identity_trace_target(ClusterTxIdentityTrace *row, const ClusterTxLocator *locator,
	int32 origin, uint64 epoch)
{
	memset(row, 0, sizeof(*row));
	row->peer_procno = UINT32_MAX;
	row->node = origin;
	row->xid = locator->xid;
	row->epoch = epoch;
	row->target_known = true;
	row->uba_known = true;
	row->uba0 = locator->uba.raw[0];
	row->uba1 = locator->uba.raw[1];
	row->itl_wrap = locator->tt_wrap;
	row->itl_kind = locator->itl_kind;
	row->itl_slot = locator->itl_slot_index;
}

static inline void
cluster_tx_identity_trace_origin(const ClusterTxLocator *locator,
	const ClusterTxResolution *resolution, int32 origin, uint64 epoch)
{
	if (unlikely(cluster_trace_tx_identity) && resolution->local_canonical_valid) {
		ClusterTxIdentityTrace row;

		cluster_tx_identity_trace_target(&row, locator, origin, epoch);
		row.tt_known = true;
		row.tt_segment = resolution->local_canonical.segment_id;
		row.tt_slot = resolution->local_canonical.slot_offset;
		row.tt_wrap = resolution->local_canonical.wrap;
		row.generation_known = true;
		row.tt_generation = resolution->local_canonical_generation;
		row.status = resolution->outcome;
		cluster_tx_identity_trace_emit(CLTXID_ORIGIN_MAP, &row);
	}
}
#endif
#endif
