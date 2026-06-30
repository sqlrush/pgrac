/*-------------------------------------------------------------------------
 *
 * cluster_itl_xlog.h
 *	  pgrac ITL-finish WAL resource manager (RM_CLUSTER_ITL).
 *
 *	  spec-3.26 replaces the GenericXLog whole-page byte-diff used by the
 *	  ITL xact-finish path (cluster_itl_touch.c) with a bespoke, block-local
 *	  delta record.  The payload reuses the spec-3.4a/3.4b xl_heap_itl_delta_block
 *	  (no new struct): each registered heap buffer carries one delta block whose
 *	  v1 deltas stamp the touched ITL slots ACTIVE -> COMMITTED/ABORTED.  Because
 *	  the records carry heap SHARED-storage block refs, cluster recovery
 *	  classifies them SHARED (cluster_recovery_merge.h) and replays them through
 *	  the shared-buffer redo path here -- unlike RM_CLUSTER_UNDO, which is
 *	  path-based MATERIALIZE_LOCAL.
 *
 *	  The redo handler lives in cluster_itl_xlog.c (backend-only: XLogReadBuffer-
 *	  ForRedo, bufmgr).  desc/identify live in
 *	  src/backend/access/rmgrdesc/clusteritldesc.c so they link into both the
 *	  backend and frontend pg_waldump.
 *
 *	  Spec: spec-3.26-single-node-write-tax-cpu-closure.md
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/storage/cluster_itl_xlog.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_ITL_XLOG_H
#define CLUSTER_ITL_XLOG_H

#include "access/xlogreader.h"
#include "lib/stringinfo.h"

/*
 * RM info bit values for RM_CLUSTER_ITL records.  This is the rmgr's OWN
 * opcode namespace (NOT bound by heap's XLOG_HEAP_OPMASK 0x70).
 */
#define XLOG_CLUSTER_ITL_FINISH 0x10 /* spec-3.26: xact-finish ITL slot stamp */

/*
 * cluster_itl_redo
 *	  RM_CLUSTER_ITL rm_redo (backend-only).  Defined in cluster_itl_xlog.c.
 */
extern void cluster_itl_redo(XLogReaderState *record);

/*
 * cluster_itl_desc / cluster_itl_identify
 *	  rmgr descriptor routines.  Defined in
 *	  src/backend/access/rmgrdesc/clusteritldesc.c (frontend-linkable).
 */
extern void cluster_itl_desc(StringInfo buf, XLogReaderState *record);
extern const char *cluster_itl_identify(uint8 info);

#endif /* CLUSTER_ITL_XLOG_H */
