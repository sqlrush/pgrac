/*-------------------------------------------------------------------------
 *
 * cluster_xid_wrap_barrier.h
 *	  pgrac xid epoch-rollover barrier (GCS-race round-3 P0-1).
 *
 *	  The native-prehistory coverage latch (cluster_xid_authority.h) lets a
 *	  node answer "is this raw 32-bit xid a native-era transaction?" from
 *	  its adopted local CLOG.  That judgement is alias-free only while NO
 *	  epoch>=1 xid exists anywhere in the cluster: after the first 2^32
 *	  carry, a wrapped peer's raw values re-occupy the 32-bit positions
 *	  below the native high-water, and a node whose own counter still reads
 *	  epoch 0 (herding lag, long idle) would mis-widen them into the native
 *	  era -- a wrong LOCAL verdict (rule 8.A false-visible channel).
 *
 *	  The barrier closes this ordering hole cluster-wide, BEFORE the first
 *	  epoch>=1 xid is issued:
 *
 *	   (1) durable one-way NATIVE_RAW_REUSED stamp in the shared xid
 *	       authority (no future boot may latch the prehistory coverage);
 *	   (2) NATIVE_DISABLE broadcast to every alive member -- each receiver
 *	       clears its own coverage latch (one-way) and ACKs;
 *	   (3) once every member ACKed, the local allocation gate opens
 *	       (cluster_xid_wrap_barrier_passed); until then GetNewTransactionId
 *	       refuses any epoch>=1 candidate fail-closed (53RB5, retryable).
 *
 *	  Each node runs its own round when its view of the cluster max xid
 *	  (own nextFullXid or the herding-mirrored peer hwm) comes within
 *	  MARGIN of the boundary; rounds are idempotent (stamp both-copies
 *	  re-assert, DISABLE/ACK re-sent every tick until the bitmap fills).  A
 *	  member that reboots after the stamp never latches again (boot gate in
 *	  the coverage verify), so a completed round is a permanent global
 *	  fact and later boots on a wrapped counter take the shortcut.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_xid_wrap_barrier.h
 *
 * NOTES
 *	  This is a pgrac-original file.  Compiled only in --enable-cluster
 *	  builds.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_XID_WRAP_BARRIER_H
#define CLUSTER_XID_WRAP_BARRIER_H

#ifndef FRONTEND
#ifdef USE_PGRAC_CLUSTER

#include "cluster/cluster_ic_envelope.h"

/*
 * Margin below 2^32 at which the LMON tick starts the barrier.  Correctness
 * does not depend on the value (the allocation gate blocks at the boundary
 * regardless); it only buys the round enough time to complete before
 * allocators start refusing.  16M xids = seconds-to-minutes of headroom at
 * any realistic allocation rate, and far above the herding window
 * (slack x 64) that bounds how far apart the nodes' counters can drift.
 */
#define CLUSTER_XID_WRAP_BARRIER_MARGIN UINT64CONST(16777216)

/* Wire payload (both NATIVE_DISABLE and its ACK; rule 15 integrity). */
#define CLUSTER_XID_WRAP_BARRIER_IC_MAGIC 0x50475742 /* "PGWB" */
#define CLUSTER_XID_WRAP_BARRIER_IC_VERSION 1

typedef struct ClusterXidNativeDisablePayload {
	uint32 magic;	 /* CLUSTER_XID_WRAP_BARRIER_IC_MAGIC */
	uint16 version;	 /* CLUSTER_XID_WRAP_BARRIER_IC_VERSION */
	uint16 reserved; /* zero */
	int32 node_id;	 /* sender: barrier coordinator (DISABLE) / acker (ACK) */
	uint32 crc;		 /* CRC32C over the fields above */
} ClusterXidNativeDisablePayload;

StaticAssertDecl(sizeof(ClusterXidNativeDisablePayload) == 16,
				 "xid wrap barrier payload is wire ABI");

/* msg_type registration (wired into cluster_lmon_shmem_init phase 1). */
extern void cluster_xid_wrap_barrier_register_ic_msg_types(void);

/*
 * LMON tick: margin check -> durable stamp -> self disable -> DISABLE
 * fanout -> ack sweep -> gate open.  No-op before the margin and after the
 * gate opens (zero steady-state cost).
 */
extern void cluster_xid_wrap_barrier_lmon_tick(void);

/*
 * StartupXLOG-tail init (runs right before the coverage verify): mirror a
 * durably stamped NATIVE_RAW_REUSED into shmem, and on an already-wrapped
 * counter (epoch>0) take the boot shortcut -- the first carry was gated, so
 * "every latch off" is already a permanent global fact.
 */
extern void cluster_xid_wrap_barrier_startup_init(void);

#endif /* USE_PGRAC_CLUSTER */
#endif /* FRONTEND */

#endif /* CLUSTER_XID_WRAP_BARRIER_H */
