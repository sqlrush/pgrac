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
	case XLOG_UNDO_TT_SLOT_BIND: {
		xl_undo_tt_slot_bind rec;
		xl_undo_tt_identity_v2 v2;

		if (XLogRecGetDataLen(record) == sizeof(v2)) {
			if (!cluster_undo_tt_bind_v2_decode(payload, XLogRecGetDataLen(record), &v2))
				appendStringInfoString(buf, "malformed TT BIND payload");
			else
				appendStringInfo(buf,
					"instance %u segment %u generation %u slot %u wrap %u xid %u original_full_xid " UINT64_FORMAT " (active bind v%u)",
					(unsigned)v2.instance, (unsigned)v2.segment_id,
					(unsigned)v2.segment_generation, (unsigned)v2.slot_offset,
					(unsigned)v2.wrap, (unsigned)v2.xid, v2.original_full_xid,
					(unsigned)v2.format_version);
			break;
		}
		if (XLogRecGetDataLen(record) != sizeof(rec)) {
			appendStringInfoString(buf, "malformed TT BIND payload");
			break;
		}
		memcpy(&rec, payload, sizeof(rec));
		if (rec.format_version != CLUSTER_UNDO_TT_BIND_VERSION || rec.flags != 0
			|| rec.reserved8 != 0 || rec.reserved32 != 0) {
			appendStringInfoString(buf, "malformed TT BIND payload");
			break;
		}

		appendStringInfo(buf,
			"instance %u segment %u generation %u slot %u wrap %u xid %u (active bind v%u)",
			(unsigned)rec.instance, (unsigned)rec.segment_id,
			(unsigned)rec.segment_generation, (unsigned)rec.slot_offset,
			(unsigned)rec.wrap, (unsigned)rec.xid,
			(unsigned)rec.format_version);
		break;
	}
	case XLOG_UNDO_TT_SLOT_COMMIT: {
		xl_undo_tt_slot_commit *rec = (xl_undo_tt_slot_commit *)payload;

		appendStringInfo(
			buf, "instance %u segment %u slot %u wrap %u xid %u commit_scn " UINT64_FORMAT,
			(unsigned)rec->instance, (unsigned)rec->segment_id, (unsigned)rec->slot_offset,
			(unsigned)rec->wrap, (unsigned)rec->xid, (uint64)rec->commit_scn);
		break;
	}
	case XLOG_UNDO_TT_SLOT_ABORT: {
		if (XLogRecGetDataLen(record) == sizeof(xl_undo_tt_slot_abort_exact))
		{
			xl_undo_tt_slot_abort_exact rec;

			memcpy(&rec, payload, sizeof(rec));
			appendStringInfo(buf,
				"instance %u seg %u generation %u slot %u wrap %u xid %u (exact abort v%u)",
				(unsigned) rec.instance, (unsigned) rec.segment_id,
				(unsigned) rec.segment_generation, (unsigned) rec.slot_offset,
				(unsigned) rec.wrap, (unsigned) rec.xid,
				(unsigned) rec.format_version);
		}
		else if (XLogRecGetDataLen(record) == sizeof(xl_undo_tt_slot_abort))
		{
			xl_undo_tt_slot_abort rec;

			memcpy(&rec, payload, sizeof(rec));
			appendStringInfo(buf,
				"instance %u seg %u slot %u wrap %u xid %u (prepared abort)",
				(unsigned) rec.instance, (unsigned) rec.segment_id,
				(unsigned) rec.slot_offset, (unsigned) rec.wrap,
				(unsigned) rec.xid);
		}
		else
			appendStringInfo(buf, "malformed abort payload length %u",
				(unsigned) XLogRecGetDataLen(record));
		break;
	}
	case XLOG_UNDO_SEGMENT_RECYCLE: {
		xl_undo_segment_recycle *xlrec = (xl_undo_segment_recycle *)payload;

		appendStringInfo(buf, "instance %u seg %u gen %u state %u->%u", xlrec->instance,
						 xlrec->segment_id, xlrec->expected_generation, xlrec->old_state,
						 xlrec->new_state);
		break;
	}
	case XLOG_UNDO_SEGMENT_REUSE: {
		xl_undo_segment_reuse *xlrec = (xl_undo_segment_reuse *)payload;

		appendStringInfo(buf, "instance %u seg %u gen %u->%u (fresh header image)", xlrec->instance,
						 xlrec->segment_id, xlrec->old_generation, xlrec->new_generation);
		break;
	}
	case XLOG_UNDO_BLOCK_WRITE: {
		xl_undo_block_write *xlrec = (xl_undo_block_write *)payload;

		appendStringInfo(buf, "instance %u seg %u block %u %s", (unsigned)xlrec->instance,
						 (unsigned)xlrec->segment_id, (unsigned)xlrec->block_no,
						 xlrec->has_fpi ? "(full image)" : "(3-range delta)");
		break;
	}
	case XLOG_UNDO_BLOCK_WRITE_MULTI: {
		xl_undo_block_write_multi *xlrec = (xl_undo_block_write_multi *)payload;

		appendStringInfo(buf, "instance %u seg %u block %u %s rec_len %u slot_len %u",
						 (unsigned)xlrec->instance, (unsigned)xlrec->segment_id,
						 (unsigned)xlrec->block_no,
						 xlrec->has_fpi ? "(full image)" : "(3-span multi delta)",
						 (unsigned)xlrec->rec_len, (unsigned)xlrec->slot_len);
		break;
	}
	case XLOG_UNDO_TT_SLOT_SET_HEAD: {
		xl_undo_tt_slot_set_head *xlrec = (xl_undo_tt_slot_set_head *)payload;

		appendStringInfo(buf, "instance %u seg %u slot %u wrap %u xid %u (set undo head)",
						 xlrec->instance, xlrec->segment_id, xlrec->slot_offset, xlrec->wrap,
						 xlrec->xid);
		break;
	}
	case XLOG_UNDO_TT_SLOT_CTRC_RELEASE: {
		xl_undo_tt_slot_ctrc_release_v1 rec;

		if (XLogRecGetDataLen(record)
			!= CLUSTER_UNDO_TT_CTRC_RELEASE_BYTES
			|| !cluster_undo_tt_ctrc_release_decode((const uint8 *)payload,
				&rec))
			appendStringInfo(buf, "malformed TT CTRC release payload");
		else
			appendStringInfo(buf,
				"instance %u seg %u generation %u slot %u wrap %u xid %u terminal %u seal " UINT64_FORMAT " touched " UINT64_FORMAT,
				(unsigned)rec.owner_instance, (unsigned)rec.segment_id,
				(unsigned)rec.segment_generation, (unsigned)rec.slot_offset,
				(unsigned)rec.slot_wrap, (unsigned)rec.xid,
				(unsigned)rec.terminal_status, rec.seal_generation,
				rec.touched_nodes_low);
		break;
	}
	case XLOG_HW_RESERVE: {
		xl_hw_reserve *xlrec = (xl_hw_reserve *)payload;

		appendStringInfo(buf, "db %u rel %u spc %u fork %u new_hwm %u granted %u", xlrec->dbOid,
						 xlrec->relNumber, xlrec->spcOid, xlrec->fork, xlrec->new_hwm,
						 xlrec->granted);
		break;
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
	case XLOG_UNDO_TT_SLOT_BIND:
		return "UNDO_TT_SLOT_BIND";
	case XLOG_UNDO_TT_SLOT_COMMIT:
		return "UNDO_TT_SLOT_COMMIT";
	case XLOG_UNDO_TT_SLOT_ABORT:
		return "UNDO_TT_SLOT_ABORT";
	case XLOG_UNDO_SEGMENT_RECYCLE:
		return "UNDO_SEGMENT_RECYCLE";
	case XLOG_UNDO_SEGMENT_REUSE:
		return "UNDO_SEGMENT_REUSE";
	case XLOG_UNDO_BLOCK_WRITE:
		return "UNDO_BLOCK_WRITE";
	case XLOG_UNDO_BLOCK_WRITE_MULTI:
		return "UNDO_BLOCK_WRITE_MULTI";
	case XLOG_UNDO_TT_SLOT_SET_HEAD:
		return "UNDO_TT_SLOT_SET_HEAD";
	case XLOG_UNDO_TT_SLOT_CTRC_RELEASE:
		return "UNDO_TT_SLOT_CTRC_RELEASE";
	case XLOG_HW_RESERVE:
		return "HW_RESERVE";
	default:
		return NULL;
	}
}

#endif /* USE_PGRAC_CLUSTER */
