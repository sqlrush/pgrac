/* PGRAC: bounded, provisional input accounting for live PI proofs.
 * Author: SqlRush <sqlrush@gmail.com> */
#ifndef CLUSTER_PI_CONTRIBUTION_STREAM_H
#define CLUSTER_PI_CONTRIBUTION_STREAM_H

#include "cluster/cluster_page_stable_base.h"
#include "cluster/cluster_wal_tail.h"

typedef struct ClusterPiContributionStreamV1 ClusterPiContributionStreamV1;

/* Copies the exact, ordered original source vector. No source discovery,
 * WAL ownership or physical validation is performed by this object. */
extern RfPageProofDetailV1
cluster_pi_contribution_stream_create_v1(const ClusterWalSourceRef *sources,
										 const RfContributorStreamCutV1 *cuts, uint32 count,
										 ClusterPiContributionStreamV1 **out);
/* Feed every physically decoded record, including SIDE and native control.
 * A decoder/owner refusal must destroy the whole enclosing proof. This
 * ledger cannot turn such a refusal into an empty or successful record. */
extern RfPageProofDetailV1
cluster_pi_contribution_stream_record_v1(ClusterPiContributionStreamV1 *stream, uint32 index,
										 const ClusterWalSourceRef *source,
										 const struct XLogReaderState *record);
/* The original physical reader supplies its final observation. Even an
 * explicitly empty source must be physically visited and finished. */
extern RfPageProofDetailV1
cluster_pi_contribution_stream_finish_v1(ClusterPiContributionStreamV1 *stream, uint32 index,
										 const ClusterWalTailObservation *observed);
/* This certifies only accounting, never complete PAGE/SIDE/SPACE ownership,
 * ancestry, input liveness, recovery, DATA, PI retirement or WAL reclamation.
 * No output on refusal; failed objects can only be destroyed. */
extern RfPageProofDetailV1
cluster_pi_contribution_stream_seal_v1(ClusterPiContributionStreamV1 *stream, uint64 *out_records);
extern void cluster_pi_contribution_stream_destroy_v1(ClusterPiContributionStreamV1 **stream);

#endif
