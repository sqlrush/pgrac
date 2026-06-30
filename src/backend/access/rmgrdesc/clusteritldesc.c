/*-------------------------------------------------------------------------
 *
 * clusteritldesc.c
 *	  rmgr descriptor routines for src/backend/cluster/storage/
 *	  cluster_itl_xlog.c (RM_CLUSTER_ITL).
 *
 *	  Lives in src/backend/access/rmgrdesc/ so the rmgrdesc files are
 *	  compiled into both the backend (linked by xlog.o) and frontend
 *	  pg_waldump (which collects all *desc.c via wildcard).
 *
 *	  The redo handler stays in cluster_itl_xlog.c because it touches
 *	  backend-only APIs (XLogReadBufferForRedo, bufmgr).
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
 *	  src/backend/access/rmgrdesc/clusteritldesc.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER
#include "cluster/storage/cluster_itl_xlog.h"


void
cluster_itl_desc(StringInfo buf, XLogReaderState *record)
{
	uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	switch (info) {
	case XLOG_CLUSTER_ITL_FINISH:
		appendStringInfoString(buf, "ITL xact-finish (block-local slot deltas)");
		break;
	default:
		appendStringInfo(buf, "unknown op %u", info);
		break;
	}
}


const char *
cluster_itl_identify(uint8 info)
{
	switch (info & ~XLR_INFO_MASK) {
	case XLOG_CLUSTER_ITL_FINISH:
		return "ITL_FINISH";
	default:
		return NULL;
	}
}

#endif /* USE_PGRAC_CLUSTER */
