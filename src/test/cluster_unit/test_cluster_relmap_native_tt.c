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
