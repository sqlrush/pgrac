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
#include "cluster/cluster_recovery_ctrc_record.h"
#include "cluster/cluster_recovery_tt_record.h"
#include "cluster/storage/cluster_undo_xlog.h"


static void
cluster_recovery_ctrc_desc(StringInfo buf, const uint8 *payload, size_t length)
{
	/* Diagnostic layout only: pg_waldump has no ROOT-selected system identity
	 * for the codec's hash validation. Replay still requires the original codec,
	 * graph and owner checks. Never read a scalar before bounding the payload. */
	if (payload == NULL || length != CLUSTER_RECOVERY_CTRC_BYTES
		|| cluster_undo_tt_ctrc_get_u16_le(payload) != CLUSTER_RECOVERY_CTRC_VERSION
		|| cluster_undo_tt_ctrc_get_u16_le(payload + 2) != CLUSTER_RECOVERY_CTRC_BYTES)
	{
		appendStringInfo(buf, "malformed recovery CTRC release payload");
		return;
	}
	appendStringInfo(buf,
		"v2 flags 0x%08x root " UINT64_FORMAT " root generation " UINT64_FORMAT
		" formation " UINT64_FORMAT " serial " UINT64_FORMAT
		" database incarnation " UINT64_FORMAT " writer boot " UINT64_FORMAT
		" history " UINT64_FORMAT " highest FPI end " UINT64_FORMAT " cohort " UINT64_FORMAT
		" origin %u writer thread %u target owner %u segment %u generation %u"
		" slot %u wrap %u xid %u terminal %u",
		(unsigned) cluster_undo_tt_ctrc_get_u32_le(payload + 4),
		cluster_undo_tt_ctrc_get_u64_le(payload + 8),
		cluster_undo_tt_ctrc_get_u64_le(payload + 16),
		cluster_undo_tt_ctrc_get_u64_le(payload + 24),
		cluster_undo_tt_ctrc_get_u64_le(payload + 32),
		cluster_undo_tt_ctrc_get_u64_le(payload + 40),
		cluster_undo_tt_ctrc_get_u64_le(payload + 48),
		cluster_undo_tt_ctrc_get_u64_le(payload + 72),
		cluster_undo_tt_ctrc_get_u64_le(payload + 80),
		cluster_undo_tt_ctrc_get_u64_le(payload + 88),
		(unsigned) cluster_undo_tt_ctrc_get_u16_le(payload + 96),
		(unsigned) cluster_undo_tt_ctrc_get_u16_le(payload + 98),
		(unsigned) cluster_undo_tt_ctrc_get_u16_le(payload + 100),
		(unsigned) cluster_undo_tt_ctrc_get_u32_le(payload + 108),
		(unsigned) cluster_undo_tt_ctrc_get_u32_le(payload + 112),
		(unsigned) cluster_undo_tt_ctrc_get_u16_le(payload + 102),
		(unsigned) cluster_undo_tt_ctrc_get_u16_le(payload + 104),
		(unsigned) cluster_undo_tt_ctrc_get_u32_le(payload + 116),
		(unsigned) payload[106]);
}


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
		xl_undo_tt_slot_bind *rec = (xl_undo_tt_slot_bind *)payload;

		appendStringInfo(buf,
			"instance %u segment %u generation %u slot %u wrap %u xid %u (active bind v%u)",
			(unsigned)rec->instance, (unsigned)rec->segment_id,
			(unsigned)rec->segment_generation, (unsigned)rec->slot_offset,
			(unsigned)rec->wrap, (unsigned)rec->xid,
			(unsigned)rec->format_version);
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
	case XLOG_UNDO_RECOVERY_TT_ABORT: {
		ClusterRecoveryTTAbortV1 rec;

		if (!cluster_recovery_tt_abort_decode_v1((const uint8 *)payload,
				XLogRecGetDataLen(record), &rec))
			appendStringInfo(buf, "malformed recovery TT abort payload");
		else
			appendStringInfo(buf,
				"target thread %u incarnation " UINT64_FORMAT
				" writer thread %u incarnation " UINT64_FORMAT
				" serial " UINT64_FORMAT " epoch " UINT64_FORMAT
				" full xid " UINT64_FORMAT " segment %u generation %u slot %u wrap %u",
				(unsigned)rec.target_thread, rec.target_incarnation,
				(unsigned)rec.writer_thread, rec.writer_incarnation,
				rec.recovery_serial, rec.target_epoch, rec.target_full_xid,
				rec.exact.segment_id, rec.exact.segment_generation,
				(unsigned)rec.exact.slot_offset, (unsigned)rec.exact.wrap);
		break;
	}
	case CLUSTER_RECOVERY_CTRC_WAL_INFO:
		cluster_recovery_ctrc_desc(buf, (const uint8 *)payload, XLogRecGetDataLen(record));
		break;
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
	case XLOG_UNDO_RECOVERY_TT_ABORT:
		return "UNDO_RECOVERY_TT_ABORT";
	case CLUSTER_RECOVERY_CTRC_WAL_INFO:
		return "UNDO_RECOVERY_CTRC_RELEASE";
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
