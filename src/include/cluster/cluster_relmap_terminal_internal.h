/* Original terminal reader custody, private to the cold participant.
 * Author: SqlRush <sqlrush@gmail.com>
 * Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md */
#ifndef CLUSTER_RELMAP_TERMINAL_INTERNAL_H
#define CLUSTER_RELMAP_TERMINAL_INTERNAL_H
#include "cluster/cluster_relmap_recovery.h"
#include "cluster/cluster_wal_inputs.h"
#include "cluster/cluster_undo_recovery.h"
#ifdef USE_PGRAC_CLUSTER
/* Stack-only adapter of the original pre-publication episode. These reads
 * neither acquire nor prolong a native scope. permitted is identity-only
 * (called under native locks); current brackets I/O with the full owner. */
typedef struct ClusterRelmapTerminalEpisodeV1 {
	void *arg;
	bool (*current)(void *, const ClusterWalInputV1 *);
	bool (*permitted)(void *, int);
	bool (*visit)(void *, ClusterUndoRecoveryCheckpointVisitorV1, void *);
	const struct RfSideOnlinePlanV1 *side;
	uint64 serial; /* original IR request, not an expired per-record scope */
} ClusterRelmapTerminalEpisodeV1;
struct ClusterColdTypedV1;
struct ClusterRecoveryFencePlan;
struct ClusterThreadRecoveryAuthorityV1;
extern bool cluster_cold_typed_terminal_authority_v1(struct ClusterColdTypedV1 *,
	struct ClusterRecoveryFencePlan *, uint32, struct ClusterThreadRecoveryAuthorityV1 *);
extern bool cluster_cold_typed_terminal_visit_v1(struct ClusterColdTypedV1 *,
	struct ClusterRecoveryFencePlan *, uint32, ClusterUndoRecoveryCheckpointVisitorV1, void *);
extern bool cluster_relmap_recovery_prepare_episode_v1(const ClusterRelmapV2Namespace *,
	const ClusterRelmapV2View *, const ClusterRelmapTerminalEpisodeV1 *, ClusterRelmapRecoveryHandle **);
extern bool cluster_clog_native_episode_read_v1(int, TransactionId, ClusterClogNativeBaseV1 *,
	ClusterClogNativeBaseHandleV1 **, const ClusterRelmapTerminalEpisodeV1 *);
extern bool cluster_clog_native_episode_seal_v1(ClusterClogNativeBaseHandleV1 *,
	const ClusterRelmapTerminalEpisodeV1 *);
typedef struct ClusterRelmapTerminalCustodyV1 {
	ClusterWalInputV1 selected;
	uint64 scope_serial;
	ClusterRelmapRecoveryTerminalV1 terminal;
	ClusterClogNativeFileV1 file;
	ClusterClogNativeBaseHandleV1 *held;
} ClusterRelmapTerminalCustodyV1;
/* One-way move only after the original reader succeeded. No scope survives. */
extern bool cluster_relmap_recovery_terminal_detach_v1(ClusterRelmapRecoveryHandle *handle,
	ClusterRelmapTerminalCustodyV1 *out);
extern bool cluster_clog_native_terminal_seal_v1(ClusterClogNativeBaseHandleV1 *held);
/* Identity only: no cold permission, contents read, sync or file open. */
extern bool cluster_clog_native_terminal_current_v1(ClusterClogNativeBaseHandleV1 *held);
#endif
#endif
