/*-------------------------------------------------------------------------
 *
 * cluster_adg_xlog.h
 *	  WAL records for spec-6.4 ADG consistency barriers.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_adg_xlog.h
 *
 * NOTES
 *	  This is a pgrac-original file for spec-6.4 ADG WAL records.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_ADG_XLOG_H
#define CLUSTER_ADG_XLOG_H

#include "access/xlogdefs.h"
#include "access/xlogreader.h"
#include "cluster/cluster_scn.h"
#include "lib/stringinfo.h"

#define XLOG_CLUSTER_ADG_THREAD_BARRIER 0x10

StaticAssertDecl((XLOG_CLUSTER_ADG_THREAD_BARRIER & XLR_INFO_MASK) == 0,
				 "cluster ADG WAL opcodes must leave XLR_INFO_MASK bits clear");

typedef struct xl_cluster_adg_thread_barrier {
	uint16 thread_id;
	uint16 _pad16;
	uint32 _pad32;
	SCN thread_safe_scn;
} xl_cluster_adg_thread_barrier;

StaticAssertDecl(sizeof(xl_cluster_adg_thread_barrier) == 16,
				 "xl_cluster_adg_thread_barrier WAL ABI lock");
StaticAssertDecl(offsetof(xl_cluster_adg_thread_barrier, thread_safe_scn) == 8,
				 "xl_cluster_adg_thread_barrier.thread_safe_scn offset changed");

extern XLogRecPtr cluster_adg_emit_thread_barrier(void);
extern void cluster_adg_redo(XLogReaderState *record);
extern void cluster_adg_desc(StringInfo buf, XLogReaderState *record);
extern const char *cluster_adg_identify(uint8 info);

#endif /* CLUSTER_ADG_XLOG_H */
