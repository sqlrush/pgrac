/*-------------------------------------------------------------------------
 *
 * cluster_elog.c
 *	  Storage for the pgrac cluster logging context (CLUSTER_LOG macro).
 *
 *	  Defines the lifecycle phase global that the CLUSTER_LOG macro
 *	  reads on every log line:
 *	      cluster_phase = "init"
 *
 *	  Callers are expected to update cluster_phase when crossing
 *	  lifecycle boundaries (e.g. set "running" after cluster_init,
 *	  "shutdown" before cluster_shutdown).  The variable is shared
 *	  process-wide; in the long run it will become per-process state
 *	  populated from the postmaster startup phase machine.
 *
 *	  cluster_node_id was originally defined here in stage 0.9 as a
 *	  placeholder global.  Stage 0.13 promoted it to a real GUC, with
 *	  storage now owned by src/backend/cluster/cluster_guc.c.  The
 *	  CLUSTER_LOG macro continues to read it via the same extern
 *	  declaration in cluster_elog.h, so call sites do not change.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_elog.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  cluster_phase is intentionally NOT a GUC: the value is set by
 *	  the cluster lifecycle (cluster_init / cluster_shutdown / future
 *	  reconfig handlers), not by users.  Exposing it via SHOW/SET
 *	  would be misleading.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_elog.h"


/*
 * Lifecycle phase tag.
 *
 *	HC2 (spec-1.10 §1.5): this is a read-only DERIVED MIRROR of the
 *	authoritative ClusterStartupPhase enum maintained in
 *	cluster_startup_phase.c.  The ONLY writer is cluster_advance_phase()
 *	(spec-1.10 D2).  External callers MUST NOT assign cluster_phase
 *	directly -- it would diverge from the enum SSOT.
 *
 *	Initial value "pre_init" matches CLUSTER_PHASE_PRE_INIT (the enum
 *	default at process start, before cluster_run_startup_sequence()
 *	advances into CLUSTER_PHASE_0_BASE in PostmasterMain).
 *
 *	CLUSTER_LOG dereferences this string on every log line; the
 *	cluster_advance_phase() updater simply swaps the pointer to point
 *	at one of the static const strings owned by cluster_startup_phase.c.
 *	No locking is required: postmaster startup is single-threaded, and
 *	child backends inherit the snapshot at fork() time without
 *	mutating it.
 *
 *	NULL is tolerated by the macro and rendered as "(unset)" for
 *	backward compatibility, even though we now always keep a non-NULL
 *	value.
 */
const char *cluster_phase = "pre_init";
