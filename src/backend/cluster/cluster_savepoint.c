/*-------------------------------------------------------------------------
 * cluster_savepoint.c -- native-frame metadata and Oracle savepoint names.
 *
 * Native TransactionState owns nesting and resources. This stack retains
 * only native name-slot references, never an xid, TT slot or undo authority.
 * All nodes live in TopTransactionContext; forgetting precedes native free.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 * Portions Copyright (c) 2026, pgrac contributors
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xact.h"
#include "cluster/cluster_mode.h"
#include "cluster/cluster_savepoint.h"
#include "utils/memutils.h"

#ifdef USE_PGRAC_CLUSTER

typedef struct ClusterSavepointFrame
{
	SubTransactionId subid;
	char **name_slot;
	int savepoint_level;
	struct ClusterSavepointFrame *parent;
} ClusterSavepointFrame;

static ClusterSavepointFrame *savepoint_frames;
static SubTransactionId pending_start = InvalidSubTransactionId;
static bool callback_registered;

static void
savepoint_xact_end(XactEvent event, void *arg)
{
	(void) arg;
	switch (event)
	{
		case XACT_EVENT_COMMIT:
		case XACT_EVENT_PARALLEL_COMMIT:
		case XACT_EVENT_ABORT:
		case XACT_EVENT_PARALLEL_ABORT:
		case XACT_EVENT_PREPARE:
			/* TopTransactionContext owns storage; never inspect stale slots. */
			savepoint_frames = NULL;
			pending_start = InvalidSubTransactionId;
			break;
		default:
			break;
	}
}

void
cluster_savepoint_start(SubTransactionId subid)
{
	ClusterSavepointFrame *frame;

	if (!cluster_storage_mode_enabled())
		return;
	if (subid == InvalidSubTransactionId || subid == TopSubTransactionId
		|| pending_start != InvalidSubTransactionId
		|| (savepoint_frames != NULL && savepoint_frames->subid >= subid))
		elog(PANIC, "invalid savepoint frame start");

	/* An ERROR here leaves the native frame published but metadata absent.
	 * Its eventual PopTransaction must not retire the parent's metadata. */
	pending_start = subid;
	if (!callback_registered)
	{
		RegisterXactCallback(savepoint_xact_end, NULL);
		callback_registered = true;
	}
	frame = MemoryContextAllocZero(TopTransactionContext, sizeof(*frame));
	frame->subid = subid;
	frame->parent = savepoint_frames;
	savepoint_frames = frame;
	pending_start = InvalidSubTransactionId;
}

void
cluster_savepoint_define(SubTransactionId subid, char **name_slot, int savepoint_level)
{
	ClusterSavepointFrame *frame = savepoint_frames;
	ClusterSavepointFrame *older;

	if (!cluster_storage_mode_enabled())
		return;
	if (frame == NULL || frame->subid != subid || name_slot == NULL)
		elog(PANIC, "invalid savepoint name publication");
	frame->name_slot = name_slot;
	frame->savepoint_level = savepoint_level;
	if (*name_slot == NULL)
		return;

	/* Publication allocates nothing: a failed new name cannot erase the old
	 * point. Anonymous/internal frames do not replace a user's name. */
	for (older = frame->parent; older != NULL; older = older->parent)
	{
		if (older->savepoint_level == savepoint_level && older->name_slot != NULL
			&& *older->name_slot != NULL && strcmp(*older->name_slot, *name_slot) == 0)
		{
			char *old_name = *older->name_slot;

			*older->name_slot = NULL;
			older->name_slot = NULL;
			pfree(old_name);
		}
	}
}

void
cluster_savepoint_forget(SubTransactionId subid)
{
	ClusterSavepointFrame *frame = savepoint_frames;

	if (pending_start == subid)
	{
		pending_start = InvalidSubTransactionId;
		return;
	}
	if (pending_start != InvalidSubTransactionId)
		elog(PANIC, "savepoint frame cleanup crossed an incomplete start");
	/* No metadata is created in disabled storage mode. */
	if (frame == NULL)
		return;
	if (frame->subid != subid)
		elog(PANIC, "savepoint frame cleanup is out of order");
	savepoint_frames = frame->parent;
	pfree(frame);
}

#endif
