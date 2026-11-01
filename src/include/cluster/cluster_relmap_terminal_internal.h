/* Original terminal reader custody, private to the cold participant.
 * Author: SqlRush <sqlrush@gmail.com>
 * Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md */
#ifndef CLUSTER_RELMAP_TERMINAL_INTERNAL_H
#define CLUSTER_RELMAP_TERMINAL_INTERNAL_H
#include "cluster/cluster_relmap_recovery.h"
#include "cluster/cluster_wal_inputs.h"
#ifdef USE_PGRAC_CLUSTER
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
