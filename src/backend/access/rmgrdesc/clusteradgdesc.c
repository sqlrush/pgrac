/*-------------------------------------------------------------------------
 *
 * clusteradgdesc.c
 *	  rmgr descriptor for RM_CLUSTER_ADG.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *	  src/backend/access/rmgrdesc/clusteradgdesc.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER
#include "cluster/cluster_adg_xlog.h"

void
cluster_adg_desc(StringInfo buf, XLogReaderState *record)
{
	char	   *payload = XLogRecGetData(record);
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	switch (info) {
	case XLOG_CLUSTER_ADG_THREAD_BARRIER: {
		xl_cluster_adg_thread_barrier *rec = (xl_cluster_adg_thread_barrier *)payload;

		if (XLogRecGetDataLen(record) != sizeof(*rec)) {
			appendStringInfo(buf, "invalid thread barrier record length %u",
							 XLogRecGetDataLen(record));
			break;
		}

		appendStringInfo(buf, "thread_id %u thread_safe_scn " UINT64_FORMAT,
						 (unsigned)rec->thread_id, (uint64)rec->thread_safe_scn);
		break;
	}
	default:
		appendStringInfo(buf, "unknown op %u", info);
		break;
	}
}

const char *
cluster_adg_identify(uint8 info)
{
	switch (info & ~XLR_INFO_MASK) {
	case XLOG_CLUSTER_ADG_THREAD_BARRIER:
		return "THREAD_BARRIER";
	default:
		return NULL;
	}
}

#endif /* USE_PGRAC_CLUSTER */
