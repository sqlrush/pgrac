/* Read-only native diagnostics. Author: SqlRush <sqlrush@gmail.com> */
#ifndef CLUSTER_WAIT_GRAPH_H
#define CLUSTER_WAIT_GRAPH_H

#include "cluster/cluster_grd.h"
#include "cluster/cluster_pcm_lock.h"
#include "cluster/cluster_wait_trace.h"

#ifdef USE_PGRAC_CLUSTER
typedef enum ClusterWaitGraphStatus {
	CLWG_OBSERVED, CLWG_BUSY, CLWG_ABSENT, CLWG_UNAVAILABLE, CLWG_INVALID
} ClusterWaitGraphStatus;

#define CLWG_PCM_QUEUE 64
#define CLWG_PCM_BLOCKS 32
#define CLWG_KO_ROWS 192

/* Entirely caller-owned. EMPTY slot can retain an immutable logical body;
 * neither that body nor a STAGED slot proves physical transport delivery. */
typedef struct ClusterWaitGraphPcmBlock {
	int32 holder;
	ResourceXIntentSlot slot;
	uint8 payload[RESOURCE_X_CONTROL_V1_BYTES];
	ClusterWaitTrace request;
	ResourceXWireReject reject;
	bool decoded, slot_matches, neutral_sender;
} ClusterWaitGraphPcmBlock;

/* Captured only at an original successful native claim, never reconstructed
 * from a proc slot at snapshot time. Zero means no observation, not no owner. */
typedef struct ClusterWaitGraphClaimIdentity {
	uint64 owner_generation, proc_generation;
	int32 pid;
} ClusterWaitGraphClaimIdentity;

/* Original holder handle and retained successor use different request spaces. */
typedef struct ClusterWaitGraphPcmOwner {
	ResourceXLocalOwnerHandle handle;
	ClusterWaitGraphClaimIdentity claim;
	ClusterWaitTrace successor;
	uint64 highest_generation;
	uint32 held_buffer_id_plus_one;
	uint8 state;
	bool valid, successor_known;
} ClusterWaitGraphPcmOwner;

typedef struct ClusterWaitGraphPcmSnapshot {
	ClusterWaitGraphStatus status;
	ClusterWaitTrace view;
	ClusterWaitGraphPcmOwner local_owner;
	uint32 nqueued;
	ClusterWaitTrace queue[CLWG_PCM_QUEUE];
	uint32 nblocks, omitted_blocks;
	ClusterWaitGraphPcmBlock blocks[CLWG_PCM_BLOCKS];
} ClusterWaitGraphPcmSnapshot;

typedef struct ClusterWaitGraphKoRow {
	uint32 kind, slot;
	int32 pid, executor;
	uint64 serial, epoch;
	RelFileLocator locator;
	int32 fork;
	bool failed, raw_copy, buffer_copy, complete, observed, reserved, invalid;
	uint32 cursor, first, count;
	uint64 lsn;
	uint32 sent, done, uncertain, failed_peers;
} ClusterWaitGraphKoRow;

typedef struct ClusterWaitGraphKoSnapshot {
	ClusterWaitGraphStatus status;
	uint32 nrows, omitted;
	ClusterWaitGraphKoRow rows[CLWG_KO_ROWS];
} ClusterWaitGraphKoSnapshot;

extern void cluster_pcm_wait_graph_snapshot(const BufferTag *tag,
	ClusterWaitGraphPcmSnapshot *out);
/* Decode only an owned copy, after all original source locks are released. */
extern void cluster_pcm_wait_graph_decode_block(ClusterWaitGraphPcmBlock *block);
extern void cluster_ko_wait_graph_snapshot(ClusterWaitGraphKoSnapshot *out);
#endif
#endif
