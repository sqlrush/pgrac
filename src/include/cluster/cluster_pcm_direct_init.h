/*-------------------------------------------------------------------------
 *
 * cluster_pcm_direct_init.h
 *	Process-local exact proof for PCM direct initialization.
 *
 * This is not a wire, shared-memory, or persistent ABI.  A proof lives on a
 * backend stack, binds one buffer mapping/ownership generation to one known
 * initialization operation, and is consumed exactly once.  bufmgr captures
 * and revalidates snapshots under the BufferDesc header spinlock; this module
 * owns the dependency-light decision table used by unit tests.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_PCM_DIRECT_INIT_H
#define CLUSTER_PCM_DIRECT_INIT_H

#include "cluster/cluster_pcm_own.h"
#include "storage/buf_internals.h"

typedef enum ClusterPcmDirectInitKind {
	CLUSTER_PCM_DIRECT_INIT_INVALID = 0,
	CLUSTER_PCM_DIRECT_INIT_READ_MISS,
	CLUSTER_PCM_DIRECT_INIT_EXTEND,
	CLUSTER_PCM_DIRECT_INIT_VM,
	CLUSTER_PCM_DIRECT_INIT_FSM
} ClusterPcmDirectInitKind;

typedef struct ClusterPcmDirectInitSnapshot {
	BufferTag tag;
	uint64 generation;
	uint64 reservation_token;
	uint32 flags;
	uint32 buf_state;
	int32 buf_id;
	int32 private_refcount;
	uint8 buffer_type;
	uint8 pcm_state;
	bool page_is_new;
} ClusterPcmDirectInitSnapshot;

typedef struct ClusterPcmDirectInitProof {
	ClusterPcmDirectInitSnapshot identity;
	ClusterPcmDirectInitKind kind;
	bool armed;
} ClusterPcmDirectInitProof;

extern ClusterPcmOwnResult
cluster_pcm_direct_init_proof_arm(ClusterPcmDirectInitKind kind,
								  const ClusterPcmDirectInitSnapshot *snapshot,
								  ClusterPcmDirectInitProof *out_proof);
extern ClusterPcmOwnResult
cluster_pcm_direct_init_proof_consume(ClusterPcmDirectInitKind kind,
									  const ClusterPcmDirectInitSnapshot *snapshot,
									  ClusterPcmDirectInitProof *proof);

#endif /* CLUSTER_PCM_DIRECT_INIT_H */
