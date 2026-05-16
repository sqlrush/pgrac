/*-------------------------------------------------------------------------
 *
 * cluster_native_lock_probe.c
 *	  pgrac per-node native-lock probe wrapper — spec-2.25 D3.
 *
 *	  Thin wrapper around cluster_native_lock_probe_pg_state (D8 — lives
 *	  in src/backend/storage/lmgr/lock.c so it can reach PG static lock
 *	  manager state).  Callable from both:
 *	    - origin self-probe path (LMS short-circuits its own fan-out by
 *	      invoking this directly per HC32a)
 *	    - peer node probe path (cluster_ges_handle_native_lock_probe_request
 *	      Step 6 — D5 wires the call site)
 *
 *	  All meaningful work happens in D8 helper;  this wrapper exists
 *	  to keep the public API surface in src/backend/cluster/ and to host
 *	  any future enhancement that should not bleed into PG core lock.c
 *	  (e.g., per-shard scan budgeting, incremental scan, telemetry).
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_native_lock_probe.c
 *
 * NOTES
 *	  spec-2.25 D3 — Step 4 ship.  Step 6 wires the GES request handler
 *	  to call cluster_native_lock_probe_local and emit reply via
 *	  cluster_grd_outbound_enqueue_lms_native_probe (D7).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_native_lock_probe.h"

ClusterNativeLockProbeReply
cluster_native_lock_probe_local(const LOCKTAG *locktag, LOCKMODE lockmode,
								const ClusterGrdHolderId *exclude_holder)
{
	Assert(locktag != NULL);
	Assert(exclude_holder != NULL);
	if (locktag == NULL || exclude_holder == NULL)
		return CLUSTER_NATIVE_LOCK_PROBE_HOLDER_CONFLICT; /* HC32a fail-closed */

	return cluster_native_lock_probe_pg_state(locktag, lockmode, exclude_holder);
}
