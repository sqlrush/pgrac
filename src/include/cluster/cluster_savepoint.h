/*-------------------------------------------------------------------------
 * cluster_savepoint.h -- backend-local savepoint frame/name lifecycle.
 * Author: SqlRush <sqlrush@gmail.com>
 * Portions Copyright (c) 2026, pgrac contributors
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_SAVEPOINT_H
#define CLUSTER_SAVEPOINT_H

#include "c.h"
#include "access/transam.h"
#include "cluster/cluster_tt_local.h"

#define CLUSTER_SAVEPOINT_OPERATION_PRESENT 1
#define CLUSTER_SAVEPOINT_FRAME_OPERATION_PRESENT 1

/* Backend-local capture, never a substitute for canonical TT authority.
 * Raw tuple/index writers retain operation_fxid; top alone owns the TT slot. */
typedef struct ClusterSavepointOperation {
	ClusterCanonicalTxnBinding top;
	FullTransactionId top_fxid;
	FullTransactionId operation_fxid;
	uint64 operation_sequence;
	CommandId command_id;
	SubTransactionId subid;
} ClusterSavepointOperation;

/* Call before content/entry locks and receipt preparation, after the original
 * canonical preflight. Native xid assignment proves the current parent chain;
 * only top's already published binding is read, never a child TT allocation.
 * false clears out. A read-only frame never calls this operation producer. */
extern bool cluster_savepoint_capture_operation(ClusterSavepointOperation *out);

/* Allocation-free local recheck before APPLY/consume. A captured command id
 * stays fixed even if nested work advanced the current command counter.
 * Cleanup ownership must not depend on the current native scope surviving. */
extern bool cluster_savepoint_operation_matches_current(const ClusterSavepointOperation *operation);

/* Explicit before-effect identity for a live child frame or its ancestor.
 * Native parent-first assignment is allowed during capture; only the top's
 * existing canonical binding is read. Does not reserve control storage.
 * The ordinary DML capture/recheck remains current-scope only. */
extern bool cluster_savepoint_capture_frame_operation(SubTransactionId subid,
													  ClusterSavepointOperation *out);

/* Allocation-free recheck while that native frame is still active, including
 * from a readonly descendant. Not a cleanup or historical authority API. */
extern bool cluster_savepoint_operation_matches_frame(const ClusterSavepointOperation *operation);

/* Called after the native frame is published; does not allocate an xid. */
extern void cluster_savepoint_start(SubTransactionId subid);

/* Publish a successfully allocated user name, or rebind a restarted point.
 * name_slot belongs to the native frame until cluster_savepoint_forget().
 * Older names at this savepoint level are retired; their frames stay live. */
extern void cluster_savepoint_define(SubTransactionId subid, char **name_slot,
									 int savepoint_level);

/* Native PopTransaction calls this for RELEASE and abort cleanup, including
 * an incomplete start. This only retires memory, not transaction effects. */
extern void cluster_savepoint_forget(SubTransactionId subid);

#endif
