/* Controlled service leaves around the original HOT traversal, not MX proof.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
#include "utils/snapmgr.h"

static OffsetNumber bitmap_visible_offset;
static bool bitmap_peer, bitmap_mx_restart, bitmap_all_sure_dead;
static unsigned bitmap_mx_calls, bitmap_native_updaters, bitmap_finishes;

static bool
bitmap_hot_visible(HeapTuple tuple, Snapshot snapshot, Buffer buffer)
{
	UT_ASSERT(pd_locked && buffer == 1 && snapshot == &ios_snapshot);
	pd_checks++;
	return ItemPointerGetOffsetNumber(&tuple->t_self) == bitmap_visible_offset;
}

static bool
bitmap_surely_dead(HeapTuple tuple, GlobalVisState *state)
{
	(void)tuple;
	(void)state;
	return bitmap_all_sure_dead;
}

static TransactionId
bitmap_update_xid(HeapTupleHeader tuple)
{
	UT_ASSERT(!(tuple->t_infomask & HEAP_XMAX_IS_MULTI));
	bitmap_native_updaters++;
	return HeapTupleHeaderGetRawXmax(tuple);
}

static bool
cluster_current_mx_hot_updater_for_chain(Relation rel, Buffer buffer, HeapTuple tuple,
	TransactionId *updater_xid, bool *restart, ClusterHeapSuccessorProof *successor,
	ClusterCurrentMxOperationState *operation)
{
	UT_ASSERT(pd_locked && rel == &relation && buffer == 1);
	bitmap_mx_calls++;
	(void)tuple;
	(void)successor;
	(void)operation;
	if (bitmap_mx_restart)
	{
		bitmap_mx_restart = false;
		/* An unqualified old observation must not become permission after
		 * the original proof path drops S and restarts on a newer image. */
		pd_lock(buffer, BUFFER_LOCK_UNLOCK);
		PageSetAllVisible(pd_current.data);
		ios_space_generation++;
		pd_lock(buffer, BUFFER_LOCK_SHARE);
		*restart = true;
		return false;
	}
	*updater_xid = 600;
	*restart = false;
	return true;
}

static bool
cluster_current_mx_successor_key_matches(Buffer buffer, HeapTuple tuple,
	const ClusterHeapSuccessorProof *proof)
{
	UT_ASSERT(false); /* This fixture never fabricates an authority proof. */
	(void)buffer;
	(void)tuple;
	(void)proof;
	return false;
}

static void
cluster_current_mx_operation_finish(ClusterCurrentMxOperationState *operation)
{
	UT_ASSERT(pd_locked && !operation->active);
	bitmap_finishes++;
}

static void
cluster_current_mx_failclosed(ClusterCurrentMxOperationState *operation,
	ClusterCurrentMxFailurePhase phase, const char *detail)
{
	(void)operation;
	(void)phase;
	(void)detail;
	elog(ERROR, "controlled HOT authority failure");
}

#define HeapHotLiveVisibility bitmap_hot_visible
#define HeapHotConflictOut pd_conflict
#define HeapHotSurelyDead bitmap_surely_dead
#define GlobalVisTestFor(rel) ((void)(rel), (GlobalVisState *)&bitmap_all_sure_dead)
#undef HeapTupleHeaderGetUpdateXid
#define HeapTupleHeaderGetUpdateXid bitmap_update_xid
#define BufferGetBlockNumber(buffer) ((void)(buffer), 0)
#define RecentXmin ((TransactionId)1)
#define cluster_peer_mode_enabled() bitmap_peer
