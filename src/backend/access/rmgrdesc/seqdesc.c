/*-------------------------------------------------------------------------
 *
 * seqdesc.c
 *	  rmgr descriptor routines for commands/sequence.c
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/rmgrdesc/seqdesc.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "commands/sequence.h"


void
seq_desc(StringInfo buf, XLogReaderState *record)
{
	char	   *rec = XLogRecGetData(record);
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
	xl_seq_rec *xlrec = (xl_seq_rec *) rec;

	if (info == XLOG_SEQ_LOG)
		/* PGRAC: spec-2.41 D4 — emit write_scn so pg_waldump can cross-check
		 * the recorded pd_block_scn against the page after crash recovery. */
		appendStringInfo(buf, "rel %u/%u/%u write_scn " UINT64_FORMAT,
						 xlrec->locator.spcOid, xlrec->locator.dbOid,
						 xlrec->locator.relNumber, xlrec->write_scn);
}

const char *
seq_identify(uint8 info)
{
	const char *id = NULL;

	switch (info & ~XLR_INFO_MASK)
	{
		case XLOG_SEQ_LOG:
			id = "LOG";
			break;
	}

	return id;
}
