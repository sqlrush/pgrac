/*-------------------------------------------------------------------------
 *
 * relmapdesc.c
 *	  rmgr descriptor routines for utils/cache/relmapper.c
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/rmgrdesc/relmapdesc.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "utils/relmapper.h"
#include "common/cluster_relmap_file.h"

void
relmap_desc(StringInfo buf, XLogReaderState *record)
{
	char	   *rec = XLogRecGetData(record);
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	if (info == XLOG_RELMAP_UPDATE)
	{
		xl_relmap_update *xlrec = (xl_relmap_update *) rec;

		appendStringInfo(buf, "database %u tablespace %u size %d",
						 xlrec->dbid, xlrec->tsid, xlrec->nbytes);
	}
	else if (info == XLOG_RELMAP_UPDATE_V2)
	{
		ClusterRecoveryFileIdentityV2 identity;
		ClusterRelmapV2Image image;

		if (XLogRecHasAnyBlockRefs(record) || XLogRecHasPageVersionEdge(record)
			|| !cluster_relmap_wal_observe_intent_v2(rec, XLogRecGetDataLen(record), &identity, &image))
		{
			appendStringInfoString(buf, "invalid managed relmap record");
			return;
		}
		appendStringInfo(buf, "WAL_INTENT database %u tablespace %u origin %u creation " UINT64_FORMAT
						 " registration " UINT64_FORMAT " committed " UINT64_FORMAT
						 " pending " UINT64_FORMAT,
						 identity.database_oid, identity.tablespace_oid, identity.origin,
						 identity.creation_generation, identity.registration_generation,
						 image.committed_generation, image.pending_generation);
	}
}

const char *
relmap_identify(uint8 info)
{
	const char *id = NULL;

	switch (info & ~XLR_INFO_MASK)
	{
		case XLOG_RELMAP_UPDATE:
			id = "UPDATE";
			break;
		case XLOG_RELMAP_UPDATE_V2:
			id = "UPDATE_V2";
			break;
	}

	return id;
}
