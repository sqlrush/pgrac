/*-------------------------------------------------------------------------
 *
 * clusterrawdesc.c
 *	  rmgr descriptor for RM_CLUSTER_RAW_LAYOUT.
 *
 *	  Human-readable WAL descriptor/identifier for the spec-6.0a raw
 *	  block-device layout metadata resource manager.  pg_waldump and
 *	  backend rmgrdesc callers use this file to decode raw layout metadata
 *	  page-image records without needing the block-device provider itself.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/access/rmgrdesc/clusterrawdesc.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-6.0a-production-shared-storage-backend-matrix.md
 *	  (FROZEN, RM_CLUSTER_RAW_LAYOUT descriptor surface).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER
#include "cluster/storage/cluster_raw_xlog.h"

void
cluster_raw_layout_desc(StringInfo buf, XLogReaderState *record)
{
	char *payload = XLogRecGetData(record);
	uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	switch (info) {
	case XLOG_CLUSTER_RAW_LAYOUT_WRITE: {
		xl_cluster_raw_layout_write *rec = (xl_cluster_raw_layout_write *)payload;

		appendStringInfo(buf, "offset " UINT64_FORMAT " nbytes %u (metadata page image)",
						 rec->offset, rec->nbytes);
		break;
	}
	default:
		appendStringInfo(buf, "unknown op %u", info);
		break;
	}
}

const char *
cluster_raw_layout_identify(uint8 info)
{
	switch (info & ~XLR_INFO_MASK) {
	case XLOG_CLUSTER_RAW_LAYOUT_WRITE:
		return "RAW_LAYOUT_WRITE";
	default:
		return NULL;
	}
}

#endif /* USE_PGRAC_CLUSTER */
