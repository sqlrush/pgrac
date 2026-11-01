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
/* Same selection, plus the original qualified request capability of each
 * selected record. It is not a new grant or origin capability. The required
 * capabilities array has capacity entries, reset with keys/proofs on failure;
 * all outputs must be distinct and cannot overlap graph/capture inputs. */
extern bool cluster_mx_history_assemble_members_with_capabilities(
	const ClusterMxHistoryCapture *capture, const ClusterMxHistoryGraph *graph,
	ClusterSavepointOperationKey *keys, ClusterCurrentMemberOperationProof *proofs,
	uint32 *capabilities, uint16 capacity);
/* proofs must be the complete assemble_members output from this same graph.
 * COMPLETE reuses independently qualified B bytes from the completed graph;
 * PENDING means that exact address is absent and requires the original B
 * provenance reader. A found-but-unprovable record is UNKNOWN, never retried. */
extern ClusterMxHistoryStep cluster_mx_history_assemble_updater(
	const ClusterMxHistoryCapture *capture, const ClusterMxHistoryGraph *graph,
	const ClusterCurrentMemberOperationProof *proofs,
	const ClusterCurrentUpdaterChallenge *challenge, ClusterCurrentUpdaterProof *out);
extern bool cluster_mx_history_successor_challenge_valid(const ClusterMxHistoryCapture *capture,
	const ClusterMxHistorySuccessorChallenge *challenge);
extern bool cluster_mx_history_validate_successor(const ClusterMxHistoryCapture *capture,
	const ClusterSavepointOperationKey *keys, const ClusterCurrentMemberOperationProof *proofs,
	const ClusterMxHistorySuccessorChallenge *challenge, const ClusterMxHistorySuccessorProof *b);
/* keys/proofs are the complete A output of this same finished graph. Borrowed
 * inputs require the original capture/relock scope; this pure assembly does
 * not acquire or retain authority. COMPLETE returns independently qualified
 * LIVE creation B, not a legacy raw updater proof. PENDING means no creation
 * record for the target, never permission for a raw child lookup. UNKNOWN
 * includes found-but-unprovable/ambiguous B. Every non-success resets out.
 * No output may overlap inputs; graph views expire when its collector ends. */
extern ClusterMxHistoryStep cluster_mx_history_assemble_successor(
	const ClusterMxHistoryCapture *capture, const ClusterMxHistoryGraph *graph,
	const ClusterSavepointOperationKey *keys,
	const ClusterCurrentMemberOperationProof *proofs,
	const ClusterMxHistorySuccessorChallenge *challenge,
	ClusterMxHistorySuccessorProof *out);
#endif
#endif
