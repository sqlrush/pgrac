/*-------------------------------------------------------------------------
 *
 * cluster_undo_retention.h
 *	  pgrac own-instance undo / TT-slot retention horizon (spec-3.12).
 *
 *	  The retention horizon is the min CLUSTER-source read_scn over all
 *	  backends' live snapshots (each backend publishes its per-backend min into
 *	  PGPROC->cluster_read_scn_atomic via snapmgr; spec-3.12 D1).  TT slots and
 *	  undo segments whose commit_scn is strictly below the horizon are needed by
 *	  no live reader and may be recycled; commit_scn at or newer than the
 *	  horizon is retained.
 *	  This retires the spec-3.11 L4 watermark fail-closed + D5 old-slot
 *	  fail-closed (durable TT slot kept alive while a reader needs it).
 *
 *	  Implementation note: cluster_undo_retention_horizon() lives in
 *	  procarray.c (it scans the ProcArray under ProcArrayLock, mirroring
 *	  GetOldestXmin); the recyclable predicates (spec-3.12 D2/D3) are pure and
 *	  cluster_unit-tested.  Callers must compute the horizon BEFORE taking
 *	  seg->lock / undo lifecycle_lock (spec-3.12 C17 lock ordering).
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-3.12-retention-horizon.md (§2, D1)
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_undo_retention.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_UNDO_RETENTION_H
#define CLUSTER_UNDO_RETENTION_H

#include "cluster/cluster_scn.h" /* SCN */

/*
 * cluster_undo_retention_horizon -- own-instance retention lower bound:
 *	min(live CLUSTER read_scn over backends); cluster_scn_current() when no
 *	live cluster reader (everything recyclable); InvalidScn when cluster is
 *	disabled.  Scans the ProcArray under a SHARED ProcArrayLock.  spec-3.12
 *	C17: do NOT hold seg->lock / undo lifecycle_lock when calling.
 */
extern SCN cluster_undo_retention_horizon(void);


/*
 * Pure retention predicates (spec-3.12 D2/D3).  No shmem / no LWLock / no I/O;
 * implemented in cluster_undo_retention.c and cluster_unit-tested
 * (test_cluster_retention).  SCN comparisons go through scn_time_cmp().
 *
 *	cluster_tt_slot_recyclable:
 *	  Given a TT-slot allocator status (ClusterTTSlotAllocStatus value), its
 *	  commit_scn, and the current horizon, decide whether the slot may be
 *	  recycled.  ABORTED -> always (C7); COMMITTED -> only when
 *	  commit_scn is strictly older than the horizon (a reader at the same SCN
 *	  as commit_scn still needs the pre-image).  InvalidScn horizon (cluster
 *	  disabled) -> the gate carries no constraint, so COMMITTED is recyclable.
 *	  A COMMITTED slot with an unresolved (InvalidScn) commit_scn is retained
 *	  (rule 8.A fail-closed).
 *
 *	cluster_undo_segment_recyclable:
 *	  An undo segment may be recycled only when it is SEGMENT_COMMITTED AND its
 *	  retention watermark (= max commit_scn over the header's COMMITTED TT
 *	  slots) is strictly below the horizon.  ACTIVE / ALLOCATED / FULL-but-
 *	  ACTIVE are never recyclable (C5), even when they carry no COMMITTED slot.
 *	  A SEGMENT_COMMITTED segment with no live COMMITTED slot is recyclable; one
 *	  carrying a COMMITTED slot with an unresolved commit_scn is retained
 *	  (rule 8.A fail-closed).
 */
struct UndoSegmentHeaderData;

extern bool cluster_tt_slot_recyclable(uint8 cts_status, SCN commit_scn, SCN horizon);
extern bool cluster_undo_segment_recyclable(const struct UndoSegmentHeaderData *hdr, SCN horizon);

#endif /* CLUSTER_UNDO_RETENTION_H */
