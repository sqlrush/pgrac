/*-------------------------------------------------------------------------
 *
 * test_pgrac_undo_slot.c
 *    Read-only observations for the native transaction-slot wait tests.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/modules/test_pgrac_shared_config/test_pgrac_undo_slot.c
 *
 * NOTES
 *    This pgrac-original file exports only test_pgrac_ test functions.
 *    The opt-in test module observes existing readers only. It does not
 *    assign an XID, initialize an allocator, or publish a binding.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xact.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#ifdef USE_PGRAC_CLUSTER
#include "cluster/cluster_guc.h"
#include "cluster/cluster_tt_local.h"
#include "cluster/cluster_tt_slot.h"
#include "cluster/cluster_undo_cleaner.h"
#endif

PG_FUNCTION_INFO_V1(test_pgrac_undo_slot_snapshot);
PG_FUNCTION_INFO_V1(test_pgrac_undo_slot_binding);

/*
 * check_slot_reader -- restrict the opt-in observations to superusers.
 * Inputs: current SQL role. Returns: void. Side effects: native 42501 on denial.
 */
static void
check_slot_reader(void)
{
	if (!superuser())
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("undo slot observation requires superuser"),
						errhint("This function is for native test fixtures only.")));
}

/*
 * test_pgrac_undo_slot_snapshot -- copy the existing local CURRENT census.
 * Inputs: none. Returns: JSON text, explicitly unavailable without a census.
 * Side effects: local read lock and output allocation; no allocator mutation.
 */
Datum
test_pgrac_undo_slot_snapshot(PG_FUNCTION_ARGS)
{
#ifdef USE_PGRAC_CLUSTER
	ClusterTTSlotCurrentSnapshot snapshot;
	StringInfoData out;
#endif
	check_slot_reader();
#ifdef USE_PGRAC_CLUSTER
	if (!cluster_enabled || cluster_node_id < 0
		|| !cluster_tt_slot_current_snapshot(cluster_node_id, &snapshot))
		PG_RETURN_TEXT_P(cstring_to_text("{\"available\":false}"));
	initStringInfo(&out);
	appendStringInfo(&out,
					 "{\"available\":true,\"origin\":%d,\"segment_id\":%u,"
					 "\"binding_generation\":\"" UINT64_FORMAT "\",\"slots\":[",
					 cluster_node_id, snapshot.segment_id, snapshot.binding_generation);
	for (unsigned i = 0; i < TT_SLOTS_PER_SEGMENT; i++) {
		const ClusterTTSlotCurrentOwner *slot = &snapshot.slots[i];

		if (i != 0)
			appendStringInfoChar(&out, ',');
		appendStringInfo(&out,
						 "{\"offset\":%u,\"xid\":%u,\"wrap\":%u,\"status\":%u,"
						 "\"commit_scn\":\"" UINT64_FORMAT "\"}",
						 slot->slot_offset, slot->xid, (unsigned)slot->wrap, (unsigned)slot->status,
						 (uint64)slot->commit_scn);
	}
	appendStringInfoString(&out, "]}");
	PG_RETURN_TEXT_P(cstring_to_text(out.data));
#else
	PG_RETURN_TEXT_P(cstring_to_text("{\"available\":false}"));
#endif
}

/*
 * test_pgrac_undo_slot_binding -- observe this backend's published top only.
 * Inputs: none. Returns: JSON text, explicitly unavailable without publication.
 * Side effects: backend-local reads and output allocation; never assigns an XID.
 */
Datum
test_pgrac_undo_slot_binding(PG_FUNCTION_ARGS)
{
#ifdef USE_PGRAC_CLUSTER
	ClusterCanonicalTxnBinding binding;
	bool waiting;
	StringInfoData out;
#endif
	check_slot_reader();
#ifdef USE_PGRAC_CLUSTER
	waiting = cluster_undo_cleaner_tx_slot_waiting();
	if (!cluster_tt_local_get_published_binding(GetTopTransactionIdIfAny(), &binding))
		PG_RETURN_TEXT_P(cstring_to_text(
			psprintf("{\"available\":false,\"wait_scope\":%s}", waiting ? "true" : "false")));
	initStringInfo(&out);
	appendStringInfo(&out,
					 "{\"available\":true,\"wait_scope\":%s,\"pid\":%d,\"xid\":%u,"
					 "\"origin\":%u,\"segment_id\":%u,\"segment_generation\":%u,"
					 "\"slot_offset\":%u,\"slot_wrap\":%u,\"active_lsn\":\"" UINT64_FORMAT "\"}",
					 waiting ? "true" : "false", MyProcPid, binding.xid,
					 (unsigned)(binding.origin_instance - 1), binding.segment_id,
					 binding.segment_generation, (unsigned)binding.slot_offset,
					 (unsigned)binding.slot_wrap, (uint64)binding.active_lsn);
	PG_RETURN_TEXT_P(cstring_to_text(out.data));
#else
	PG_RETURN_TEXT_P(cstring_to_text("{\"available\":false,\"wait_scope\":false}"));
#endif
}
