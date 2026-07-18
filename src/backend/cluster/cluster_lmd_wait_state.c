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

#define CLUSTER_LMD_WAIT_STATE_READ_RETRIES 16

static uint32
cluster_lmd_wait_state_write_begin(ClusterLmdProcWaitState *ws)
{
	uint32 current;
	uint32 odd;

	current = pg_atomic_read_u32(&ws->change_seq);
	odd = current | 1U;
	pg_atomic_write_u32(&ws->change_seq, odd);
	pg_write_barrier();
	return odd;
}

static void
cluster_lmd_wait_state_write_end(ClusterLmdProcWaitState *ws, uint32 odd)
{
	pg_write_barrier();
	pg_atomic_write_u32(&ws->change_seq, odd + 1U);
}

void
cluster_lmd_wait_state_init(ClusterLmdProcWaitState *ws)
{
	pg_atomic_init_u32(&ws->change_seq, 0);
	pg_atomic_init_u64(&ws->wait_seq, 0);
	ws->active = 0;
	ws->kind = CLUSTER_LMD_WAIT_NONE;
	ws->request_id = 0;
	ws->cluster_epoch = 0;
	ws->xid = InvalidTransactionId;
}

void
cluster_lmd_wait_state_reset(ClusterLmdProcWaitState *ws)
{
	uint32 odd;

	/*
	 * Backstop for a backend that reused this proc slot whose predecessor
	 * died mid-wait without clearing.  Beginning from an already-odd sequence
	 * is deliberate: it closes a predecessor that died midway through a
	 * publish.  wait_seq is left monotonically climbing so a stale victim
	 * tuple recorded for the predecessor can never re-match.
	 */
	odd = cluster_lmd_wait_state_write_begin(ws);
	ws->active = 0;
	cluster_lmd_wait_state_write_end(ws, odd);
}

uint64
cluster_lmd_wait_state_publish(ClusterLmdProcWaitState *ws, uint8 kind, uint64 request_id,
							   uint64 cluster_epoch, TransactionId xid)
{
	uint64 seq;
	uint32 odd;

	odd = cluster_lmd_wait_state_write_begin(ws);
	ws->kind = kind;
	ws->request_id = request_id;
	ws->cluster_epoch = cluster_epoch;
	ws->xid = xid;
	seq = pg_atomic_fetch_add_u64(&ws->wait_seq, 1) + 1;
	ws->active = 1;
	cluster_lmd_wait_state_write_end(ws, odd);

	return seq;
}

void
cluster_lmd_wait_state_clear(ClusterLmdProcWaitState *ws)
{
	uint32 odd;

	/* The single funnel every wait-exit path (grant / timeout / ERROR /
	 * cancel) calls.  Leaves the data fields and wait_seq in place; active
	 * is the authoritative "still waiting" signal. */
	odd = cluster_lmd_wait_state_write_begin(ws);
	ws->active = 0;
	cluster_lmd_wait_state_write_end(ws, odd);
}

ClusterLmdWaitStateReadResult
cluster_lmd_wait_state_read_exact(ClusterLmdProcWaitState *ws, ClusterLmdWaitStateSnapshot *out)
{
	ClusterLmdWaitStateSnapshot snapshot;
	int retry;

	memset(out, 0, sizeof(*out));

	for (retry = 0; retry < CLUSTER_LMD_WAIT_STATE_READ_RETRIES; retry++) {
		uint32 before;
		uint32 after;

		before = pg_atomic_read_u32(&ws->change_seq);
		if ((before & 1U) != 0)
			continue;

		pg_read_barrier();
		snapshot.active = ws->active != 0;
		snapshot.kind = ws->kind;
		snapshot.request_id = ws->request_id;
		snapshot.cluster_epoch = ws->cluster_epoch;
		snapshot.xid = ws->xid;
		snapshot.wait_seq = pg_atomic_read_u64(&ws->wait_seq);
		pg_read_barrier();

		after = pg_atomic_read_u32(&ws->change_seq);
		if (before != after || (after & 1U) != 0)
			continue;
		if (!snapshot.active)
			return CLUSTER_LMD_WAIT_STATE_READ_INACTIVE;

		*out = snapshot;
		return CLUSTER_LMD_WAIT_STATE_READ_ACTIVE;
	}

	return CLUSTER_LMD_WAIT_STATE_READ_BUSY;
}

bool
cluster_lmd_wait_state_read(ClusterLmdProcWaitState *ws, ClusterLmdWaitStateSnapshot *out)
{
	return cluster_lmd_wait_state_read_exact(ws, out) == CLUSTER_LMD_WAIT_STATE_READ_ACTIVE;
}
