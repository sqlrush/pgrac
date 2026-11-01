/*-------------------------------------------------------------------------
 * cluster_relmap_recovery_internal.h
 *     Unpublished debt for the original cold recovery coordinator only.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/include/cluster/cluster_relmap_recovery_internal.h
 * NOTES
 *     Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_RELMAP_RECOVERY_INTERNAL_H
#define CLUSTER_RELMAP_RECOVERY_INTERNAL_H
#include "cluster/cluster_relmap_retention.h"

/* Private plan-owned storage, never registered as an ordinary directory.
 * Count is conservative debt even after refusal/ERROR. It remains BUILDING
 * forever; restored only means this entire declared batch has been copied.
 * The original pass-1/SIDE seal must still prove no undeclared record exists. */
typedef struct ClusterRelmapRecoveryPins {
	ClusterRelmapRetentionState state;
	uint32 expected, count;
	bool restored, transferred;
	bool attempted[CLUSTER_RELMAP_RETENTION_CAPACITY];
	bool staged[CLUSTER_RELMAP_RETENTION_CAPACITY];
	ClusterRelmapV2Image images[CLUSTER_RELMAP_RETENTION_CAPACITY];
} ClusterRelmapRecoveryPins;

extern bool cluster_relmap_recovery_check(const ClusterRelmapRecoveryPins *pins,
	const ClusterRelmapV2Image *image);
extern bool cluster_relmap_recovery_stage_start(ClusterRelmapRecoveryPins *pins,
	const ClusterRelmapV2Image *image);
extern bool cluster_relmap_recovery_stage_finish(ClusterRelmapRecoveryPins *pins,
	const ClusterRelmapV2Image *image);
/* Original joint publisher ONLY, after all participants have qualified and
 * all fallible work, under its one publication lock/critical section. Targets
 * are qualified but still unpublished COMPLETE directories. Whole preflight
 * then nofail copies precede retirement; this does NOT publish or change
 * directory state. No I/O/allocation/provider calls. An unsuccessful joint
 * owner must never invoke this hook; failed targets leave ALL custody intact.
 * Stored source images remain until the original context is destroyed. */
extern bool cluster_relmap_recovery_transfer(ClusterRelmapRecoveryPins *pins,
	ClusterRelmapRetentionDirectory *const *targets, uint32 count);
extern bool cluster_relmap_authority_v2_write_recovery_pending(
	const ClusterRelmapV2Image *image, ClusterRelmapRecoveryPins *pins);
#endif
