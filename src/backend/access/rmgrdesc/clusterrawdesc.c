/*-------------------------------------------------------------------------
 *
 * clusterrawdesc.c
 *    rmgr descriptor for RM_CLUSTER_RAW_LAYOUT.
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
