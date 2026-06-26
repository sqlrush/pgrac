/*-------------------------------------------------------------------------
 *
 * cluster_lmd_wait_state.c
 *	  pgrac per-proc cluster wait-state publish / clear / read (spec-5.8 D1d).
 *
 *	  See cluster/cluster_lmd_wait_state.h for the contract.  The owning
 *	  backend publishes before a cross-node wait and clears on every exit;
 *	  the LMD resolver reads the record cross-process to revalidate a victim.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_lmd_wait_state.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Compiled only in --enable-cluster
 *	  builds.
 *	  Spec: spec-5.8-full-cross-node-deadlock-detector.md.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/transam.h"
#include "cluster/cluster_lmd_wait_state.h"

void
cluster_lmd_wait_state_init(ClusterLmdProcWaitState *ws)
{
	pg_atomic_init_u32(&ws->active, 0);
	pg_atomic_init_u64(&ws->wait_seq, 0);
	ws->kind = CLUSTER_LMD_WAIT_NONE;
	ws->request_id = 0;
	ws->cluster_epoch = 0;
	ws->xid = InvalidTransactionId;
}

void
cluster_lmd_wait_state_reset(ClusterLmdProcWaitState *ws)
{
	/*
	 * Backstop for a backend that reused this proc slot whose predecessor
	 * died mid-wait without clearing.  Clear active only — wait_seq is left
	 * monotonically climbing so a stale victim tuple recorded for the
	 * predecessor can never re-match (procno reuse ABA guard).
	 */
	pg_atomic_write_u32(&ws->active, 0);
}

uint64
cluster_lmd_wait_state_publish(ClusterLmdProcWaitState *ws, uint8 kind, uint64 request_id,
							   uint64 cluster_epoch, TransactionId xid)
{
	uint64 seq;

	/* Write the data fields and advance wait_seq first, then publish via the
	 * active flag with a write barrier between.  A reader that observes
	 * active == 1 (with a read barrier) sees this record, not a half-written
	 * one. */
	ws->kind = kind;
	ws->request_id = request_id;
	ws->cluster_epoch = cluster_epoch;
	ws->xid = xid;
	seq = pg_atomic_fetch_add_u64(&ws->wait_seq, 1) + 1;

	pg_write_barrier();
	pg_atomic_write_u32(&ws->active, 1);

	return seq;
}

void
cluster_lmd_wait_state_clear(ClusterLmdProcWaitState *ws)
{
	/* The single funnel every wait-exit path (grant / timeout / ERROR /
	 * cancel) calls.  Leaves the data fields and wait_seq in place; active
	 * is the authoritative "still waiting" signal. */
	pg_atomic_write_u32(&ws->active, 0);
}

bool
cluster_lmd_wait_state_read(ClusterLmdProcWaitState *ws, ClusterLmdWaitStateSnapshot *out)
{
	memset(out, 0, sizeof(*out));

	if (pg_atomic_read_u32(&ws->active) == 0)
		return false;

	pg_read_barrier();
	out->active = true;
	out->kind = ws->kind;
	out->request_id = ws->request_id;
	out->cluster_epoch = ws->cluster_epoch;
	out->xid = ws->xid;
	out->wait_seq = pg_atomic_read_u64(&ws->wait_seq);
	return true;
}
