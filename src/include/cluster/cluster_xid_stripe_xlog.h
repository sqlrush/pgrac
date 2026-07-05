/*-------------------------------------------------------------------------
 *
 * cluster_xid_stripe_xlog.h
 *	  pgrac xid stripe WAL resource manager (spec-6.15 D5d, appendix B.4).
 *
 *	  Carries the stripe activation knowledge to WAL consumers (hot
 *	  standby / ADG replay): a JOIN record {activation floor, epoch,
 *	  slot} is emitted by every node at write-gate open, BEFORE the
 *	  node's first xid-bearing record; a RETIRE record is emitted by
 *	  the spec-5.18 removal coordinator after the durable slot retire.
 *
 *	  Ordering invariant (the F1(a) skip-fill soundness): a node's
 *	  striped xids appear only in its own WAL thread, and its JOIN
 *	  record precedes its first allocation IN THAT SAME THREAD, so any
 *	  order-preserving consumer (single-stream standby replay today,
 *	  the k-way SCN merge later — per-thread order is merge-stable)
 *	  learns "slot k is active above floor F" before it sees the first
 *	  class-k xid.  No cross-thread SCN reasoning is required.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_xid_stripe_xlog.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-6.15-xid-space-segmentation.md
 *	  Frontend-safe: consumed by the rmgrdesc surface (pg_waldump).
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_XID_STRIPE_XLOG_H
#define CLUSTER_XID_STRIPE_XLOG_H

#include "access/xlogreader.h"
#include "lib/stringinfo.h"

#ifdef USE_PGRAC_CLUSTER

/* record types (info bits) */
#define XLOG_CLUSTER_XID_STRIPE_JOIN 0x00
#define XLOG_CLUSTER_XID_STRIPE_RETIRE 0x10

typedef struct xl_cluster_xid_stripe_join {
	uint64 activated_floor_full; /* cluster activation floor (full-xid U64) */
	uint64 stride_mode_epoch;	 /* activation epoch */
	int32 slot;					 /* the joining node's stripe slot */
} xl_cluster_xid_stripe_join;

typedef struct xl_cluster_xid_stripe_retire {
	int32 slot; /* the permanently removed stripe slot */
} xl_cluster_xid_stripe_retire;

/* emission (backend only; no-ops when WAL insertion is not allowed) */
extern bool cluster_xid_stripe_emit_join_wal(void);
extern void cluster_xid_stripe_emit_retire_wal(int32 slot);
extern void cluster_xid_stripe_checkpoint_reemit(void);

/* rmgr callbacks */
extern void cluster_xid_stripe_redo(XLogReaderState *record);
extern void cluster_xid_stripe_desc(StringInfo buf, XLogReaderState *record);
extern const char *cluster_xid_stripe_identify(uint8 info);

#endif /* USE_PGRAC_CLUSTER */

#endif /* CLUSTER_XID_STRIPE_XLOG_H */
