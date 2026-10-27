/*-------------------------------------------------------------------------
 * cluster_savepoint.c -- native-frame metadata and Oracle savepoint names.
 *
 * Native TransactionState owns nesting and resources. This stack retains
 * native name-slot references. Operation captures name an existing top binding;
 * they never allocate a child TT slot or supply undo authority.
 * All nodes live in TopTransactionContext; forgetting precedes native free.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 * Portions Copyright (c) 2026, pgrac contributors
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xact.h"
#include "access/parallel.h"
#include "cluster/cluster_mode.h"
#include "cluster/cluster_savepoint.h"
#include "cluster/cluster_undo_record_api.h"
#include "cluster/cluster_tt_slot.h"
#include "utils/memutils.h"

#ifdef USE_PGRAC_CLUSTER

typedef struct ClusterSavepointFrame
{
	SubTransactionId subid;
	char **name_slot;
	int savepoint_level;
	ClusterUndoSavepointControl *control;
	struct ClusterSavepointFrame *parent;
} ClusterSavepointFrame;

static ClusterSavepointFrame *savepoint_frames;
static SubTransactionId pending_start = InvalidSubTransactionId;
static bool callback_registered;
static uint64 operation_sequence_floor;

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
			/* Original undo commit/reset bookkeeping runs after some native
			 * memory teardown. Retire frame references here, while nodes live. */
			cluster_undo_record_release_savepoint_controls_at_xact_end();
			/* TopTransactionContext owns storage; never inspect stale slots. */
			savepoint_frames = NULL;
			pending_start = InvalidSubTransactionId;
			operation_sequence_floor = 0;
			break;
		default:
			break;
	}
}

static void
savepoint_ensure_callback(void)
{
	if (!callback_registered)
	{
		RegisterXactCallback(savepoint_xact_end, NULL);
		callback_registered = true;
	}
}

static bool
savepoint_operation_identity_valid(const ClusterSavepointOperation *operation)
{
	const ClusterCanonicalTxnBinding *top = &operation->top;
	bool top_scope = operation->subid == TopSubTransactionId;

	return TransactionIdIsNormal(XidFromFullTransactionId(operation->top_fxid))
		&& TransactionIdIsNormal(XidFromFullTransactionId(operation->operation_fxid))
		&& top->xid == XidFromFullTransactionId(operation->top_fxid)
		&& top->segment_id > 0 && top->segment_id <= PG_UINT16_MAX
		&& top->segment_generation != PG_UINT32_MAX
		&& top->slot_offset < TT_SLOTS_PER_SEGMENT && top->slot_wrap != TT_WRAP_INVALID
		&& top->origin_instance == (uint8)(cluster_node_id + 1)
		&& top->publish_state == CLUSTER_CANONICAL_TXN_PUBLISHED && top->reserved16 == 0
		&& !XLogRecPtrIsInvalid(top->active_lsn)
		&& operation->command_id != InvalidCommandId
		&& operation->subid != InvalidSubTransactionId
		&& (top_scope ? FullTransactionIdEquals(operation->operation_fxid, operation->top_fxid)
			: U64FromFullTransactionId(operation->operation_fxid)
				> U64FromFullTransactionId(operation->top_fxid));
}

static ClusterSavepointFrame *
savepoint_find_frame(SubTransactionId subid)
{
	ClusterSavepointFrame *frame;

	for (frame = savepoint_frames; frame != NULL; frame = frame->parent)
		if (frame->subid == subid)
			return frame;
	return NULL;
}

static bool
savepoint_capture_scoped_operation(SubTransactionId requested, ClusterSavepointOperation *out)
{
	ClusterSavepointOperation operation = { 0 };
	SubTransactionId current;

	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	if (!cluster_storage_mode_enabled() || !IsTransactionState()
		|| IsInParallelMode() || IsParallelWorker()
		|| operation_sequence_floor == PG_UINT64_MAX)
		return false;
	current = GetCurrentSubTransactionId();
	if (current == InvalidSubTransactionId || pending_start != InvalidSubTransactionId
		|| (current != TopSubTransactionId
			&& (savepoint_frames == NULL || savepoint_frames->subid != current)))
		return false;
	operation.subid = requested == InvalidSubTransactionId ? current : requested;
	if (operation.subid != current
		&& (operation.subid <= TopSubTransactionId
			|| savepoint_find_frame(operation.subid) == NULL))
		return false;

	/* Register even for top-only producers: they need counter reset without
	 * ever creating a frame. Assignment uses native parent-first xid rules. */
	savepoint_ensure_callback();
	operation.operation_fxid = GetCurrentFullTransactionId();
	if (operation.subid != TopSubTransactionId)
		operation.operation_fxid = GetFullTransactionIdForSubTransactionIfAny(operation.subid);
	operation.top_fxid = GetTopFullTransactionIdIfAny();
	operation.command_id = GetCurrentCommandId(false);
	if (!cluster_tt_local_get_published_binding(XidFromFullTransactionId(operation.top_fxid),
			&operation.top) || !savepoint_operation_identity_valid(&operation))
		return false;
	operation.operation_sequence = ++operation_sequence_floor;
	*out = operation;
	return true;
}

bool
cluster_savepoint_capture_operation(ClusterSavepointOperation *out)
{
	return savepoint_capture_scoped_operation(InvalidSubTransactionId, out);
}

bool
cluster_savepoint_capture_frame_operation(SubTransactionId subid, ClusterSavepointOperation *out)
{
	if (subid <= TopSubTransactionId) {
		if (out != NULL)
			memset(out, 0, sizeof(*out));
		return false;
	}
	return savepoint_capture_scoped_operation(subid, out);
}

bool
cluster_savepoint_operation_matches_current(const ClusterSavepointOperation *operation)
{
	ClusterCanonicalTxnBinding top;

	if (operation == NULL || !cluster_storage_mode_enabled() || !IsTransactionState()
		|| IsInParallelMode() || IsParallelWorker()
		|| operation->operation_sequence == 0
		|| operation->operation_sequence > operation_sequence_floor
		|| !savepoint_operation_identity_valid(operation)
		|| operation->subid != GetCurrentSubTransactionId()
		|| !FullTransactionIdEquals(operation->top_fxid, GetTopFullTransactionIdIfAny())
		|| !FullTransactionIdEquals(operation->operation_fxid, GetCurrentFullTransactionIdIfAny()))
		return false;
	/* Published-binding lookup is backend-local, with no assignment or I/O. */
	return cluster_tt_local_get_published_binding(operation->top.xid, &top)
		&& memcmp(&top, &operation->top, sizeof(top)) == 0;
}

bool
cluster_savepoint_operation_matches_frame(const ClusterSavepointOperation *operation)
{
	ClusterCanonicalTxnBinding top;

	if (operation == NULL || !cluster_storage_mode_enabled() || !IsTransactionState()
		|| IsInParallelMode() || IsParallelWorker() || pending_start != InvalidSubTransactionId
		|| operation->subid <= TopSubTransactionId || operation->operation_sequence == 0
		|| operation->operation_sequence > operation_sequence_floor
		|| !savepoint_operation_identity_valid(operation) || savepoint_frames == NULL
		|| savepoint_frames->subid != GetCurrentSubTransactionId()
		|| savepoint_find_frame(operation->subid) == NULL
		|| !FullTransactionIdEquals(operation->top_fxid, GetTopFullTransactionIdIfAny())
		|| !FullTransactionIdEquals(operation->operation_fxid,
									GetFullTransactionIdForSubTransactionIfAny(operation->subid)))
		return false;
	return cluster_tt_local_get_published_binding(operation->top.xid, &top)
		   && memcmp(&top, &operation->top, sizeof(top)) == 0;
}

bool
cluster_savepoint_reserve_current_frames(uint64 absolute_deadline_us)
{
	ClusterSavepointFrame *frame;
	ClusterSavepointOperation *operations;
	ClusterUndoSavepointControl **controls;
	uint32 count = 0;
	uint32 index = 0;

	if (!cluster_storage_mode_enabled() || !IsTransactionState() || IsInParallelMode()
		|| IsParallelWorker() || absolute_deadline_us == 0
		|| pending_start != InvalidSubTransactionId)
		return false;
	if (GetCurrentSubTransactionId() == TopSubTransactionId)
		return savepoint_frames == NULL;
	if (savepoint_frames == NULL || savepoint_frames->subid != GetCurrentSubTransactionId())
		return false;
	for (frame = savepoint_frames; frame != NULL; frame = frame->parent) {
		if (frame->control != NULL) {
			if (!cluster_undo_record_savepoint_control_owned(frame->control))
				return false;
		} else
			count++;
	}
	if (count == 0)
		return true;
	if (count > MaxAllocSize / sizeof(*operations))
		return false;
	/* Both scratch arrays and the storage owner's handles precede effects.
	 * TopTransactionContext also owns them if native ERROR interrupts capture. */
	operations = MemoryContextAllocZero(TopTransactionContext, count * sizeof(*operations));
	controls = MemoryContextAllocZero(TopTransactionContext, count * sizeof(*controls));
	for (frame = savepoint_frames; frame != NULL; frame = frame->parent)
		if (frame->control == NULL
			&& !cluster_savepoint_capture_frame_operation(frame->subid, &operations[index++])) {
			pfree(controls);
			pfree(operations);
			return false;
		}
	if (!cluster_undo_record_reserve_savepoint_controls(operations, count, absolute_deadline_us,
														controls)) {
		pfree(controls);
		pfree(operations);
		return false;
	}
	for (index = 0; index < count; index++)
		if (controls[index] == NULL
			|| !cluster_undo_record_savepoint_control_owned(controls[index])) {
			for (uint32 i = 0; i < count; i++)
				cluster_undo_record_release_savepoint_control(controls[i]);
			pfree(controls);
			pfree(operations);
			return false;
		}
	index = 0;
	for (frame = savepoint_frames; frame != NULL; frame = frame->parent)
		if (frame->control == NULL)
			frame->control = controls[index++];
	pfree(controls);
	pfree(operations);
	return true;
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
	savepoint_ensure_callback();
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

/* Spec: 2026-10-26-pre2-p013-full-savepoint-design.md.
 * Native identity remains live through the original durable rollback driver. */
void
cluster_savepoint_subabort(SubTransactionId subid)
{
	ClusterSavepointFrame *frame = savepoint_frames;
	XLogRecPtr complete_cut;
	bool incomplete_empty;

	if (!cluster_storage_mode_enabled())
		return;
	/* Failed metadata allocation precedes any first-effect producer. Native
	 * PopTransaction will clear pending_start after its ordinary cleanup. */
	incomplete_empty = pending_start == subid && (frame == NULL || frame->subid != subid)
					   && !FullTransactionIdIsValid(GetCurrentFullTransactionIdIfAny());
	if (subid > TopSubTransactionId && GetCurrentSubTransactionId() == subid
		&& (incomplete_empty
			|| (pending_start == InvalidSubTransactionId && frame != NULL
				&& frame->subid == subid))) {
		if (incomplete_empty || frame->control == NULL)
			return;
		/* The driver proves the live frame, quiescence, exact canonical head,
		 * all marks/cursors and actual COMPLETE before native cleanup. */
		if (cluster_undo_record_rollback_savepoint_current(
				frame->control, cluster_undo_record_prepare_deadline_us(), &complete_cut))
			return;
	}
	ereport(PANIC, (errcode(ERRCODE_DATA_CORRUPTED),
					errmsg("cannot finish savepoint rollback before native child cleanup"),
					errhint("Retain the transaction's undo and recover its canonical owner before "
							"continuing.")));
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
	cluster_undo_record_release_savepoint_control(frame->control);
	savepoint_frames = frame->parent;
	pfree(frame);
}

#endif
