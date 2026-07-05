/*-------------------------------------------------------------------------
 *
 * cluster_xid_stripe_xlog.c
 *	  pgrac xid stripe WAL resource manager (spec-6.15 D5d) — emission
 *	  and redo.  See cluster_xid_stripe_xlog.h for the per-thread
 *	  ordering invariant that makes the standby skip-fill sound.
 *
 *	  Redo publishes the replay-learned stripe knowledge (activation
 *	  floor + active slot set) through cluster_xid_stripe_boot.c; the
 *	  hot-standby KnownAssignedXids gap-fill consumes it.  The standby
 *	  NEVER learns the stripe state from local configuration or from
 *	  the voting disks — WAL order is the only trusted source (user
 *	  constraint, appendix B.4).
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_xid_stripe_xlog.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-6.15-xid-space-segmentation.md
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "access/xlog.h"
#include "access/xloginsert.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_xid_stripe.h"
#include "cluster/cluster_xid_stripe_boot.h"
#include "cluster/cluster_xid_stripe_xlog.h"
#include "utils/elog.h"

/*
 * Emit this node's JOIN record: the activation floor/epoch plus our
 * slot.  Called at write-gate open, BEFORE the gate is published, so
 * the record precedes every xid-bearing record this node ever emits
 * (WAL is prefix-flushed: no explicit flush is needed for ordering).
 * Once per process lifetime (each boot re-emits — replay is
 * idempotent).  Returns false when WAL insertion is not possible yet
 * (still in recovery): the caller HOLDS the gate and retries.
 */
bool
cluster_xid_stripe_emit_join_wal(void)
{
	static bool emitted = false;
	xl_cluster_xid_stripe_join rec;
	uint64 floor = 0;
	uint64 epoch = 0;

	if (emitted)
		return true;

	if (RecoveryInProgress() || !XLogInsertAllowed())
		return false;

	if (!cluster_xid_stripe_get_activation(&floor, &epoch, NULL))
		return false; /* activation not published; the gate holds anyway */

	memset(&rec, 0, sizeof(rec));
	rec.activated_floor_full = floor;
	rec.stride_mode_epoch = epoch;
	rec.slot = cluster_node_id;

	XLogBeginInsert();
	XLogRegisterData((char *)&rec, sizeof(rec));
	(void)XLogInsert(RM_CLUSTER_XID_STRIPE_ID, XLOG_CLUSTER_XID_STRIPE_JOIN);

	emitted = true;
	ereport(LOG, (errmsg("cluster xid stripe: JOIN record emitted (slot %d, floor " UINT64_FORMAT
						 ", epoch " UINT64_FORMAT ")",
						 rec.slot, floor, epoch)));
	return true;
}

/*
 * Emit the RETIRE record for a permanently removed slot (spec-5.18
 * coordinator, after the durable voting-disk retire).  The voting
 * disk is the correctness anchor; this record only carries the
 * knowledge to WAL consumers, so an impossible-in-practice emission
 * failure is logged and tolerated (the standby keeps filling the
 * retired class — a capacity pessimism, never a visibility error).
 */
void
cluster_xid_stripe_emit_retire_wal(int32 slot)
{
	xl_cluster_xid_stripe_retire rec;

	if (RecoveryInProgress() || !XLogInsertAllowed()) {
		ereport(LOG, (errmsg("cluster xid stripe: RETIRE record for slot %d not emitted "
							 "(WAL insertion not allowed here)",
							 slot)));
		return;
	}

	memset(&rec, 0, sizeof(rec));
	rec.slot = slot;

	XLogBeginInsert();
	XLogRegisterData((char *)&rec, sizeof(rec));
	(void)XLogInsert(RM_CLUSTER_XID_STRIPE_ID, XLOG_CLUSTER_XID_STRIPE_RETIRE);
}

/*
 * Checkpoint re-emission: a standby (or PITR restore) replays from an
 * arbitrary checkpoint redo point, so the one-shot gate-open JOIN
 * record may lie before its window.  Re-emitting at every checkpoint
 * bounds the knowledge gap to one checkpoint distance; the short
 * window between the redo point and this record falls back to the
 * dense (vanilla) gap fill — a capacity pessimism, never a
 * visibility hazard.  Called from CreateCheckPoint outside the
 * critical section (spec-5.7 HW-snapshot hook precedent).
 */
void
cluster_xid_stripe_checkpoint_reemit(void)
{
	xl_cluster_xid_stripe_join rec;
	uint64 floor = 0;
	uint64 epoch = 0;

	if (!cluster_enabled || !cluster_xid_striping)
		return;
	if (RecoveryInProgress() || !XLogInsertAllowed())
		return;
	if (!cluster_xid_stripe_get_activation(&floor, &epoch, NULL))
		return;

	memset(&rec, 0, sizeof(rec));
	rec.activated_floor_full = floor;
	rec.stride_mode_epoch = epoch;
	rec.slot = cluster_node_id;

	XLogBeginInsert();
	XLogRegisterData((char *)&rec, sizeof(rec));
	(void)XLogInsert(RM_CLUSTER_XID_STRIPE_ID, XLOG_CLUSTER_XID_STRIPE_JOIN);
}

void
cluster_xid_stripe_redo(XLogReaderState *record)
{
	char *payload = XLogRecGetData(record);
	uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	switch (info) {
	case XLOG_CLUSTER_XID_STRIPE_JOIN: {
		xl_cluster_xid_stripe_join *rec = (xl_cluster_xid_stripe_join *)payload;

		if (rec->slot < 0 || rec->slot >= CLUSTER_XID_STRIDE || rec->activated_floor_full == 0
			|| rec->stride_mode_epoch == 0)
			ereport(PANIC,
					(errmsg("cluster xid stripe JOIN record is invalid"),
					 errdetail("slot=%d floor=" UINT64_FORMAT " epoch=" UINT64_FORMAT, rec->slot,
							   rec->activated_floor_full, rec->stride_mode_epoch)));
		cluster_xid_stripe_replay_note_join(rec->activated_floor_full, rec->stride_mode_epoch,
											rec->slot);
		break;
	}
	case XLOG_CLUSTER_XID_STRIPE_RETIRE: {
		xl_cluster_xid_stripe_retire *rec = (xl_cluster_xid_stripe_retire *)payload;

		if (rec->slot < 0 || rec->slot >= CLUSTER_XID_STRIDE)
			ereport(PANIC, (errmsg("cluster xid stripe RETIRE record is invalid"),
							errdetail("slot=%d", rec->slot)));
		cluster_xid_stripe_replay_note_retire(rec->slot);
		break;
	}
	default:
		ereport(PANIC, (errmsg("cluster_xid_stripe_redo: unknown op %u", info)));
	}
}

#endif /* USE_PGRAC_CLUSTER */
