/* Original retained native terminal observations, not release authority.
 * Author: SqlRush <sqlrush@gmail.com>
 * Copyright (c) 2026, pgrac contributors. */
#ifndef CLUSTER_RELMAP_TERMINAL_SCAN_H
#define CLUSTER_RELMAP_TERMINAL_SCAN_H
#ifdef USE_PGRAC_CLUSTER
#include "access/clog.h"
#include "access/transam.h"
#include "cluster/cluster_relmap_checkpoint.h"
#include "cluster/cluster_wal_inputs.h"
typedef struct ClusterRelmapTerminalScanV1 ClusterRelmapTerminalScanV1;
typedef struct ClusterRelmapTerminalObservationV1 {
	ClusterWalSourceRef source;
	ClusterRelmapCheckpointWindow window;
	uint64 requested_top_full_xid; /* Query identity, NOT certified terminal era. */
	XidStatus status;
	XLogRecPtr terminal_start, terminal_end, cut;
	/* Nonzero only for a v2 prefix scan; never a bound on the later suffix. */
	uint64 post_flush_issued;
	XLogRecPtr prefix_end;
	uint64 records;
	uint8 fingerprint[32];
} ClusterRelmapTerminalObservationV1;
/* Dormant full original retained scan; one physical input for this origin
 * only. Complete native checkpoint and bounded COMMIT/ABORT carrier required.
 * Every native XACT carrier must be bounded COMMIT/ABORT; unsupported XACT
 * and query-as-child refuse. Missing/duplicate/contradictory target or damaged
 * suffix refuses. The query
 * must lie in the persisted observed window; raw32 match does NOT prove its
 * era/base/retention lifetime or actual effect durability. Not terminal_read.
 * Borrowed inputs and its original ResourceOwner must outlive this object.
 * No source pin is acquired/released here. ERROR propagates with local cleanup.
 * out is a distinct caller-owned pointer variable; refusal sets it NULL. */
extern bool cluster_relmap_recovery_terminal_scan_v1(ClusterWalInputsV1 *inputs, uint32 index,
													 FullTransactionId requested_top,
													 ClusterRelmapTerminalScanV1 **out);
/* Dormant bounded prefix variant. Only a target terminal at/after the
 * selected native redo and BEFORE the exact checkpoint record is accepted.
 * Requires inner v2 actual post-flush issuance bound; legacy windows refuse.
 * The complete original source, including suffix, is still validated.
 * No native base/retention-lifetime/full-history/release qualification. */
extern bool cluster_relmap_recovery_terminal_prefix_scan_v2(ClusterWalInputsV1 *inputs,
															uint32 index,
															FullTransactionId requested_top,
															ClusterRelmapTerminalScanV1 **out);
/* Original scope/current ROOT/pin recheck before observation publish.
 * Nonoverlap refusal clears the full output; input overlap is unchanged.
 * A failed original recheck permanently invalidates this local scan. */
extern bool cluster_relmap_recovery_terminal_scan_get_v1(ClusterRelmapTerminalScanV1 *scan,
														 ClusterRelmapTerminalObservationV1 *out);
/* Caller frees only this private object, never the original owner/pins. */
extern void cluster_relmap_recovery_terminal_scan_release_v1(ClusterRelmapTerminalScanV1 **scan);
#endif
#endif
