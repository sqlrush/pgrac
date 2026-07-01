/*-------------------------------------------------------------------------
 *
 * cluster_adg_xlog.c
 *	  WAL emit/redo for spec-6.4 ADG consistency barriers.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_adg_xlog.c
 *
 * NOTES
 *	  This is a pgrac-original file for spec-6.4 ADG WAL records.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "access/xlog.h"
#include "access/xloginsert.h"
#include "cluster/cluster_adg_xlog.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_mrp.h"
#include "cluster/cluster_scn.h"
#include "cluster/cluster_wal_thread.h"

XLogRecPtr
cluster_adg_emit_thread_barrier(void)
{
	xl_cluster_adg_thread_barrier rec;
	SCN thread_safe_scn;
	uint16 thread_id;

	if (!cluster_enabled || !cluster_enable_adg || cluster_dg_role != CLUSTER_DG_ROLE_PRIMARY)
		return InvalidXLogRecPtr;

	if (!XLogInsertAllowed())
		return InvalidXLogRecPtr;

	thread_id = cluster_wal_thread_id();
	if (thread_id < XLP_THREAD_ID_FIRST_REAL || thread_id > CLUSTER_WAL_THREAD_MAX)
		return InvalidXLogRecPtr;

	thread_safe_scn = cluster_scn_adg_thread_safe_scn();
	if (!SCN_VALID(thread_safe_scn))
		return InvalidXLogRecPtr;

	memset(&rec, 0, sizeof(rec));
	rec.thread_id = thread_id;
	rec.thread_safe_scn = thread_safe_scn;

	XLogBeginInsert();
	XLogRegisterData((char *)&rec, sizeof(rec));

	return XLogInsert(RM_CLUSTER_ADG_ID, XLOG_CLUSTER_ADG_THREAD_BARRIER);
}

void
cluster_adg_redo(XLogReaderState *record)
{
	char *payload = XLogRecGetData(record);
	uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
	xl_cluster_adg_thread_barrier *rec;

	if (info != XLOG_CLUSTER_ADG_THREAD_BARRIER)
		ereport(PANIC, (errmsg("cluster_adg_redo: unknown op %u", info)));

	if (XLogRecGetDataLen(record) != sizeof(*rec))
		ereport(PANIC, (errmsg("cluster_adg_redo: invalid thread barrier record length %u",
							   XLogRecGetDataLen(record))));

	rec = (xl_cluster_adg_thread_barrier *)payload;
	cluster_mrp_apply_thread_barrier(rec->thread_id, record->EndRecPtr, rec->thread_safe_scn);
}

#endif /* USE_PGRAC_CLUSTER */
