/*-------------------------------------------------------------------------
 *
 * smgrdesc.c
 *	  rmgr descriptor routines for catalog/storage.c
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/rmgrdesc/smgrdesc.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/storage_xlog.h"
#ifdef USE_PGRAC_CLUSTER
#include "cluster/cluster_space_identity.h"
#include "cluster/cluster_space_reservation.h"
#endif


void
smgr_desc(StringInfo buf, XLogReaderState *record)
{
	char	   *rec = XLogRecGetData(record);
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

#ifdef USE_PGRAC_CLUSTER
	/* PGRAC: describe the typed payload only after exact structural decode. */
	if (info == XLOG_SMGR_SPACE_IDENTITY)
	{
		ClusterSpaceStructureChange change;

		if (cluster_space_structure_wal_decode(rec, XLogRecGetDataLen(record), &change))
			appendStringInfo(buf, "%u/%u/%u space action %u sequence " UINT64_FORMAT " reserved %u",
				change.identity.result.key.locator.spcOid, change.identity.result.key.locator.dbOid,
				change.identity.result.key.locator.relNumber, (unsigned)change.identity.action,
				change.identity.result.sequence, change.reservation.result.next_block);
		else
			appendStringInfoString(buf, "invalid SPACE identity");
		return;
	}
	if (info == XLOG_SMGR_SPACE_RESERVATION)
	{
		ClusterSpaceReservationChange change;

		if (cluster_space_reservation_wal_decode(rec, XLogRecGetDataLen(record), &change)
			&& change.action == CLUSTER_SPACE_RESERVATION_ADVANCE)
			appendStringInfo(buf, "%u/%u/%u reserve %u blocks from %u",
				change.result.identity.key.locator.spcOid, change.result.identity.key.locator.dbOid,
				change.result.identity.key.locator.relNumber, change.granted, change.first_block);
		else
			appendStringInfoString(buf, "invalid SPACE reservation");
		return;
	}
#endif
	if (info == XLOG_SMGR_CREATE)
	{
		xl_smgr_create *xlrec = (xl_smgr_create *) rec;
		char	   *path = relpathperm(xlrec->rlocator, xlrec->forkNum);

		appendStringInfoString(buf, path);
		pfree(path);
	}
	else if (info == XLOG_SMGR_TRUNCATE)
	{
		xl_smgr_truncate *xlrec = (xl_smgr_truncate *) rec;
		char	   *path = relpathperm(xlrec->rlocator, MAIN_FORKNUM);

		appendStringInfo(buf, "%s to %u blocks flags %d", path,
						 xlrec->blkno, xlrec->flags);
		pfree(path);
	}
}

const char *
smgr_identify(uint8 info)
{
	const char *id = NULL;

	switch (info & ~XLR_INFO_MASK)
	{
#ifdef USE_PGRAC_CLUSTER
		case XLOG_SMGR_SPACE_IDENTITY:
			id = "SPACE_IDENTITY";
			break;
		case XLOG_SMGR_SPACE_RESERVATION:
			id = "SPACE_RESERVATION";
			break;
#endif
		case XLOG_SMGR_CREATE:
			id = "CREATE";
			break;
		case XLOG_SMGR_TRUNCATE:
			id = "TRUNCATE";
			break;
	}

	return id;
}
