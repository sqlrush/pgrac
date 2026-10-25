/* Original allocator + local ACTIVE owner + durable TT producer bridge.
 * Existing fixture providers supply controlled native/current/physical leaves.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "test_cluster_relmap_native_tt_namespace.h"
#define TT_ACTIVE_OWNER_FIXTURE_MAIN d3_tt_existing_main
#include "test_cluster_tt_active_owner.c"
#include "test_cluster_relmap_native_tt.h"

void
test_relmap_native_tt_reset(void)
{
	ut_current_failed = 0;
	cluster_node_id = 0;
	reset_active_owner();
}

bool
test_relmap_native_tt_publish(TransactionId xid)
{
	ClusterCanonicalTxnBinding receipt;
	volatile bool caught = false;
	volatile bool published = false;

	PG_TRY();
	{
		published = cluster_tt_local_prepare_canonical_active(xid, &receipt);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	return !caught && published && !ut_current_failed;
}

bool
test_relmap_native_tt_lookup(TransactionId xid, ClusterCanonicalTxnBinding *out)
{
	return cluster_tt_local_get_published_binding(xid, out);
}

TestRelmapNativeTTStats
test_relmap_native_tt_stats(void)
{
	return (TestRelmapNativeTTStats){
		cluster_tt_local_binding_count, g_bind_emit_calls, g_ctrc_open_calls,
		lock_depth == 0 && modifier_enters == modifier_leaves
			&& g_current_pin_calls == g_current_unpin_calls
			&& g_current_acquire_calls == g_current_cancel_calls,
		ut_current_failed != 0
	};
}

/* Match the original status/hint unit providers: no positive admission. */
ClusterSemanticAdmissionResult
cluster_tt_status_source_dispatch(ClusterTTStatusSourceOp op pg_attribute_unused(),
	const ClusterTTStatusSourceRequest *request pg_attribute_unused(),
	ClusterTTStatusSourceResult *result pg_attribute_unused())
{
	return CLUSTER_SEMANTIC_ADMISSION_CLOSED;
}

ClusterSemanticAdmissionResult
cluster_tt_status_hint_source_dispatch(ClusterTTStatusHintSourceOp op pg_attribute_unused(),
	const ClusterTTStatusHintSourceRequest *request pg_attribute_unused())
{
	return CLUSTER_SEMANTIC_ADMISSION_CLOSED;
}

bool
test_relmap_native_tt_terminal(const ClusterCanonicalTxnBinding *binding,
	bool commit, bool fail_write, TestRelmapNativeTTTerminal *out)
{
	ClusterCanonicalTxnBinding current;
	void *old_bindings = cluster_tt_local_bindings;
	void (*saved_hook)(void) = g_modifier_recheck_hook;
	int writes = g_write_hdr_calls, aborts = g_abort_exact_emit_calls;
	int saved_critical = CritSectionCount;
	int saved_flags = MyProc->delayChkptFlags;
	volatile bool finished = false;

	memset(out, 0, sizeof(*out));
	if (!cluster_tt_local_get_published_binding(binding->xid, &current)
		|| memcmp(binding, &current, sizeof(current)) != 0)
		return false;
	/* Carry the completed mock ACTIVE write into its next canned disk read.
	 * Terminal admission must not run the allocation-only page reseed hook. */
	memcpy(g_canned_block, g_current_resident, BLCKSZ);
	g_modifier_recheck_hook = NULL;
	g_write_hdr_ok = !fail_write;
	g_require_abort_flush_before_write = !commit;
	g_abort_flush_seen = false;
	g_write_requires_flush = commit ? (XLogRecPtr)0x5000 : InvalidXLogRecPtr;
	g_flush_ptr = g_flushed_lsn = InvalidXLogRecPtr;
	last_ereport_elevel = 0;
	PG_TRY();
	{
		if (commit)
		{
			if (cluster_tt_local_precommit_durable_finish(binding->xid, scn_encode(1, 42),
				&out->fold))
			{
				/* Controlled native commit flush, VXID and checkpoint window. */
				g_flush_ptr = (XLogRecPtr)0x5000;
				CritSectionCount = 1;
				MyProc->delayChkptFlags |= DELAY_CHKPT_START;
				cluster_tt_local_commit_durable_apply((XLogRecPtr)0x5000);
				CritSectionCount = saved_critical;
				MyProc->delayChkptFlags = saved_flags;
				cluster_tt_local_record_commit(binding->xid, scn_encode(1, 42));
				finished = true;
			}
		}
		else if (cluster_tt_local_preabort_durable_finish(binding->xid))
		{
			cluster_tt_local_record_abort(binding->xid);
			finished = true;
		}
	}
	PG_CATCH();
	{
		out->error_level = last_ereport_elevel;
	}
	PG_END_TRY();
	CritSectionCount = saved_critical;
	MyProc->delayChkptFlags = saved_flags;
	g_modifier_recheck_hook = saved_hook;
	g_write_hdr_ok = true;
	g_require_abort_flush_before_write = false;
	out->resident = ((UndoSegmentHeaderData *)g_current_resident)->tt_slots[binding->slot_offset];
	out->written = g_last_written_slot;
	out->writes = g_write_hdr_calls - writes;
	out->abort_records = g_abort_exact_emit_calls - aborts;
	out->flushed = commit ? g_flush_ptr == (XLogRecPtr)0x5000
		: g_abort_flush_seen && g_flushed_lsn == g_abort_exact_lsn;
	out->write_before_flush = g_write_before_flush;
	out->allocator_present = cluster_tt_slot_current_owner_by_xid(0, binding->xid, &out->allocator);
	/* The fixture's calloc-backed transaction context is not an arena. */
	if (cluster_tt_local_bindings != old_bindings)
		free(old_bindings);
	return finished && !ut_current_failed;
}
