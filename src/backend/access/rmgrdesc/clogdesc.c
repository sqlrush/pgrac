/*-------------------------------------------------------------------------
 *
 * clogdesc.c
 *	  rmgr descriptor routines for access/transam/clog.c
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/rmgrdesc/clogdesc.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/clog.h"
#include "access/clog_wal.h"


void
clog_desc(StringInfo buf, XLogReaderState *record)
{
	char	   *rec = XLogRecGetData(record);
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	if (info == CLUSTER_RECOVERY_CLOG_SEGMENT_WAL_V2 ||
		info == CLUSTER_RECOVERY_CLOG_PAGE_IMAGE_WAL_V2 ||
		info == CLUSTER_RECOVERY_CLOG_TRUNCATE_WAL_V2)
	{
		ClusterClogWalObservationV2 observed;

		if (!cluster_clog_wal_decode_v2(info, rec, XLogRecGetDataLen(record), &observed))
		{
			appendStringInfoString(buf, "invalid CLOG v2 record");
			return;
		}
		appendStringInfo(buf, "origin %u; segment %u; creation " UINT64_FORMAT
					 "; registration " UINT64_FORMAT,
			observed.binding.identity.origin, observed.binding.segment,
			observed.binding.identity.creation_generation,
			observed.binding.identity.registration_generation);
		if (info == CLUSTER_RECOVERY_CLOG_PAGE_IMAGE_WAL_V2)
			appendStringInfo(buf, "; page " UINT64_FORMAT "; generation " UINT64_FORMAT
				"; birth " UINT64_FORMAT "; page_lsn %X/%X; image_lsn %X/%X",
				observed.page.identity.logical_page, observed.page.identity.page_generation,
				U64FromFullTransactionId(observed.page.identity.birth_full_xid),
				LSN_FORMAT_ARGS(observed.page.identity.page_lsn),
				LSN_FORMAT_ARGS(observed.page.image_lsn));
		else if (info == CLUSTER_RECOVERY_CLOG_TRUNCATE_WAL_V2)
			appendStringInfo(buf, "; page %u; oldestXact %u; oldestFullXid " UINT64_FORMAT
				"; database %u", observed.cutoff_page, observed.oldest_xid,
				U64FromFullTransactionId(observed.oldest_full_xid), observed.oldest_database);
	}
	else if (info == CLOG_ZEROPAGE)
	{
		int			pageno;

		memcpy(&pageno, rec, sizeof(int));
		appendStringInfo(buf, "page %d", pageno);
	}
	else if (info == CLOG_TRUNCATE)
	{
		xl_clog_truncate xlrec;

		memcpy(&xlrec, rec, sizeof(xl_clog_truncate));
		appendStringInfo(buf, "page %d; oldestXact %u",
						 xlrec.pageno, xlrec.oldestXact);
	}
}

const char *
clog_identify(uint8 info)
{
	const char *id = NULL;

	switch (info & ~XLR_INFO_MASK)
	{
		case CLUSTER_RECOVERY_CLOG_SEGMENT_WAL_V2:
			id = "SEGMENT_V2";
			break;
		case CLUSTER_RECOVERY_CLOG_PAGE_IMAGE_WAL_V2:
			id = "PAGE_IMAGE_V2";
			break;
		case CLUSTER_RECOVERY_CLOG_TRUNCATE_WAL_V2:
			id = "TRUNCATE_V2";
			break;
		case CLOG_ZEROPAGE:
			id = "ZEROPAGE";
			break;
		case CLOG_TRUNCATE:
			id = "TRUNCATE";
			break;
	}

	return id;
}
