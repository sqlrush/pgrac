/* Backend-private complete member assembly; no acquisition or local TT authority. */
#ifndef CLUSTER_MX_HISTORY_MEMBER_H
#define CLUSTER_MX_HISTORY_MEMBER_H
#include "c.h"
#if defined(USE_PGRAC_CLUSTER) && !defined(FRONTEND)
#include "cluster/cluster_mx_history_graph.h"
/* Inputs come from this completed graph, never a fabricated local TT result.
 * key is the exact compare input selected from actual record history; V1
 * uses an all-zero key and top-only extension. Failure resets UNKNOWN. */
extern bool cluster_multixact_current_operation_proof_from_history_peer(
	const ClusterMxHistoryCapture *capture, const ClusterMxHistoryNodeView *view, uint16 ordinal,
	const ClusterSavepointOperationKey *key, ClusterCurrentMemberOperationProof *out);
/* Outputs have capacity entries (nmembers..256), must not overlap inputs,
 * and stay private until the original full heap/scope relock succeeds. For
 * capacity<=256 failure clears keys and makes every output UNKNOWN; larger
 * capacity is rejected without touching memory. A complete-set operation:
 * no per-member graph or DATA re-read, cache or retirement authority. */
extern bool cluster_mx_history_assemble_members(const ClusterMxHistoryCapture *capture,
												const ClusterMxHistoryGraph *graph,
												ClusterSavepointOperationKey *keys,
												ClusterCurrentMemberOperationProof *proofs,
												uint16 capacity);
#endif
#endif
