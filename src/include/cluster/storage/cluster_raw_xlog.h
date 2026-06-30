/*-------------------------------------------------------------------------
 *
 * cluster_raw_xlog.h
 *	  WAL records for the spec-6.0a raw block-device layout metadata.
 *
 *	  Defines the RM_CLUSTER_RAW_LAYOUT record ABI shared by the raw
 *	  block-device provider, backend redo, and pg_waldump descriptor code.
 *	  The record currently carries one BLCKSZ metadata page image plus its
 *	  raw-device byte offset.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/storage/cluster_raw_xlog.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-6.0a-production-shared-storage-backend-matrix.md
 *	  (FROZEN, raw layout WAL ABI).
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_RAW_XLOG_H
#define CLUSTER_RAW_XLOG_H

#include "access/xlogreader.h"
#include "lib/stringinfo.h"
#include "storage/block.h"

#define XLOG_CLUSTER_RAW_LAYOUT_WRITE 0x10

StaticAssertDecl((XLOG_CLUSTER_RAW_LAYOUT_WRITE & XLR_INFO_MASK) == 0,
				 "cluster raw layout WAL opcodes must leave XLR_INFO_MASK bits clear");

typedef struct xl_cluster_raw_layout_write {
	uint64 offset; /* raw device byte offset, BLCKSZ-aligned */
	uint32 nbytes; /* currently always BLCKSZ */
	uint32 _pad;
	/* Followed by char image[BLCKSZ]. */
} xl_cluster_raw_layout_write;

StaticAssertDecl(sizeof(xl_cluster_raw_layout_write) == 16,
				 "xl_cluster_raw_layout_write WAL ABI lock");
StaticAssertDecl(offsetof(xl_cluster_raw_layout_write, nbytes) == 8,
				 "xl_cluster_raw_layout_write.nbytes offset changed");

extern XLogRecPtr cluster_raw_layout_emit_write(uint64 offset, const char *image);
extern void cluster_raw_layout_redo(XLogReaderState *record);
extern void cluster_raw_layout_desc(StringInfo buf, XLogReaderState *record);
extern const char *cluster_raw_layout_identify(uint8 info);

#endif /* CLUSTER_RAW_XLOG_H */
