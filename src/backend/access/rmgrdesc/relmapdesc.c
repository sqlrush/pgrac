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
/*-------------------------------------------------------------------------
 * PGRAC MODIFICATIONS
 *     Modified by: SqlRush <sqlrush@gmail.com>
 *     Describe canonical creation records and reject malformed MAIN data.
 *     Spec: 2026-11-01-pre2-e1-format-closure-v1.2-path-c.md
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "common/cluster_relmap_create.h"
#include "utils/relmapper.h"
#include "common/cluster_relmap_file.h"

void
relmap_desc(StringInfo buf, XLogReaderState *record)
{
	char	   *rec = XLogRecGetData(record);
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	if (info == XLOG_RELMAP_UPDATE)
	{
		xl_relmap_update xlrec;
		uint32 length = XLogRecGetDataLen(record);

		if (rec == NULL || length < MinSizeOfRelmapUpdate)
		{
			appendStringInfoString(buf, "invalid update record");
			return;
		}
		memcpy(&xlrec, rec, MinSizeOfRelmapUpdate);
		if (xlrec.nbytes < 0 || (uint32) xlrec.nbytes != length - MinSizeOfRelmapUpdate)
		{
			appendStringInfoString(buf, "invalid update record");
			return;
		}
		appendStringInfo(buf, "database %u tablespace %u size %d",
						 xlrec.dbid, xlrec.tsid, xlrec.nbytes);
	}
	else if (info == XLOG_RELMAP_CREATE_V1)
	{
		ClusterRelmapCreateWalV1 creation;
		char batch[33];
		static const char hex[] = "0123456789abcdef";

		if (XLogRecHasAnyBlockRefs(record) || XLogRecHasPageVersionEdge(record)
			|| !cluster_relmap_create_decode_v1(rec, XLogRecGetDataLen(record), &creation))
		{
			appendStringInfoString(buf, "invalid creation record");
			return;
		}
		for (unsigned i = 0; i < sizeof(creation.batch_uuid); ++i)
		{
			batch[2 * i] = hex[creation.batch_uuid[i] >> 4];
			batch[2 * i + 1] = hex[creation.batch_uuid[i] & 15];
		}
		batch[32] = '\0';
		appendStringInfo(buf, "database %u tablespace %u origin %u creation " UINT64_FORMAT
						 " registration " UINT64_FORMAT " batch %s generation 1 mappings %d",
						 creation.identity.database_oid, creation.identity.tablespace_oid,
						 creation.identity.origin, creation.identity.creation_generation,
						 creation.identity.registration_generation, batch,
						 creation.committed.num_mappings);
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
		case XLOG_RELMAP_CREATE_V1:
			id = "CREATE_V1";
			break;
		case XLOG_RELMAP_UPDATE_V2:
			id = "UPDATE_V2";
			break;
	}

	return id;
}
