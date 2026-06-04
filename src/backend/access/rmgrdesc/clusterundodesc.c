/*-------------------------------------------------------------------------
 *
 * clusterundodesc.c
 *	  rmgr descriptor routines for src/backend/cluster/storage/
 *	  cluster_undo_xlog.c (RM_CLUSTER_UNDO).
 *
 *	  Lives in src/backend/access/rmgrdesc/ so the rmgrdesc files are
 *	  compiled into both the backend (linked by xlog.o) and frontend
 *	  pg_waldump (which collects all *desc.c via wildcard).
 *
 *	  The redo handler + emit API stay in cluster_undo_xlog.c because
 *	  they touch backend-only APIs (XLogInsert, BasicOpenFile, etc.).
 *
 *	  Spec: spec-1.22-undo-tablespace-bootstrap.md §D14a.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/access/rmgrdesc/clusterundodesc.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER
#include "cluster/storage/cluster_undo_xlog.h"


void
cluster_undo_desc(StringInfo buf, XLogReaderState *record)
{
	char *payload = XLogRecGetData(record);
	uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	switch (info) {
	case XLOG_UNDO_SEGMENT_INIT: {
		xl_cluster_undo_segment_init *hdr = (xl_cluster_undo_segment_init *)payload;

		appendStringInfo(buf, "instance %u segment %u (page image %u bytes)",
						 (unsigned)hdr->instance, (unsigned)hdr->segment_id, BLCKSZ);
		break;
	}
	case XLOG_UNDO_TT_SLOT_COMMIT: {
		xl_undo_tt_slot_commit *rec = (xl_undo_tt_slot_commit *)payload;

		appendStringInfo(
			buf, "instance %u segment %u slot %u wrap %u xid %u commit_scn " UINT64_FORMAT,
			(unsigned)rec->instance, (unsigned)rec->segment_id, (unsigned)rec->slot_offset,
			(unsigned)rec->wrap, (unsigned)rec->xid, (uint64)rec->commit_scn);
		break;
	case XLOG_UNDO_SEGMENT_RECYCLE: {
		xl_undo_segment_recycle *xlrec = (xl_undo_segment_recycle *)rec;

		appendStringInfo(buf, "instance %u seg %u gen %u state %u->%u", xlrec->instance,
						 xlrec->segment_id, xlrec->expected_generation, xlrec->old_state,
						 xlrec->new_state);
		break;
	}
	}
	default:
		appendStringInfo(buf, "unknown op %u", info);
		break;
	}
}


const char *
cluster_undo_identify(uint8 info)
{
	switch (info & ~XLR_INFO_MASK) {
	case XLOG_UNDO_SEGMENT_INIT:
		return "UNDO_SEGMENT_INIT";
	case XLOG_UNDO_TT_SLOT_COMMIT:
		return "UNDO_TT_SLOT_COMMIT";
	case XLOG_UNDO_SEGMENT_RECYCLE:
		return "UNDO_SEGMENT_RECYCLE";
	default:
		return NULL;
	}
}

#endif /* USE_PGRAC_CLUSTER */
