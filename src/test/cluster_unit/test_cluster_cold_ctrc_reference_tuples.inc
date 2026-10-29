/*-------------------------------------------------------------------------
 * test_cluster_cold_ctrc_reference_tuples.inc
 *     Original Startup consumes complete census tuple/version bindings.
 *     Census transport and the separately tested native tuple leaf are controlled.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#include "access/htup_details.h"

static unsigned reference_tuple_calls, reference_tuple_fault;
static char reference_tuple_bytes[2][64];

/* The declaration also permits the genuine untouched-parent consumer RED. */
bool cluster_ctrc_recovery_heap_tuple_itl_references_v1(
	const ClusterItlSlotData *slots, const char *bytes, Size length);
bool
cluster_ctrc_recovery_heap_tuple_itl_references_v1(
	const ClusterItlSlotData *slots, const char *bytes, Size length)
{
	reference_tuple_calls++;
	UT_ASSERT(slots != NULL && bytes != NULL && length == 64);
	UT_ASSERT_EQ(ctrc_driver_lwlocks, 0);
	UT_ASSERT_EQ(prefix_destroyed, 2 * prefix_pages);
	UT_ASSERT_EQ(slots[0].xid, native_ctrc_work.entries[0].key.xid);
	UT_ASSERT_EQ(slots[1].flags, ITL_FLAG_FREE);
	if (reference_tuple_calls == 2) {
		if (reference_tuple_fault == 1) return false;
		if (reference_tuple_fault == 2) owner_boot++;
		if (reference_tuple_fault == 3) pg_re_throw();
	}
	return true;
}

static ClusterRecoveryCensusReferencesV1 *
reference_tuples_ready(void)
{
	ClusterRecoveryCensusReferencesV1 *references = reference_terminals_ready();
	ClusterRecoveryCensusReferenceV1 roots[2];

	memcpy(roots, references->entries, sizeof(roots));
	memset(references->entries, 0, 18 * sizeof(references->entries[0]));
	references->count = 18;
	for (unsigned version = 0; version < 2; version++) {
		for (unsigned slot = 0; slot < CLUSTER_ITL_INITRANS_DEFAULT; slot++) {
			ClusterRecoveryCensusReferenceV1 *value = &references->entries[9 * version + slot];

			*value = roots[version]; value->entry.index = slot;
			if (slot != 0) {
				memset(&value->entry.itl, 0, sizeof(value->entry.itl));
				memset(&value->transaction, 0, sizeof(value->transaction));
			}
		}
		{
			ClusterRecoveryCensusReferenceV1 *value = &references->entries[9 * version + 8];
			HeapTupleHeaderData tuple;

			memset(&tuple, 0, sizeof(tuple));
			tuple.t_hoff = MAXALIGN(SizeofHeapTupleHeader);
			HeapTupleHeaderSetXmin(&tuple, native_ctrc_work.entries[0].key.xid);
			tuple.t_infomask = HEAP_XMAX_INVALID;
			memset(reference_tuple_bytes[version], 0, 64);
			memcpy(reference_tuple_bytes[version], &tuple, SizeofHeapTupleHeader);
			value->page_index = 1; value->version = roots[version].version;
			value->entry.kind = CLUSTER_RECOVERY_HEAP_ITEM; value->entry.index = 1;
			ItemIdSetNormal(&value->entry.item, 1024, 64);
			value->entry.tuple_bytes = reference_tuple_bytes[version];
			value->entry.tuple_length = 64;
		}
	}
	reference_tuple_calls = reference_tuple_fault = 0;
	return references;
}

UT_TEST(test_startup_reference_tuples_consume_untouched_current_and_inverse_versions)
{
	ClusterRecoveryCensusReferencesV1 *references = reference_tuples_ready();
	ClusterRecoveryCensusReferenceV1 saved[18];

	memcpy(saved, references->entries, sizeof(saved));
	retirement_run_startup();
	UT_ASSERT(retirement_service_reached);
	UT_ASSERT_EQ(reference_tuple_calls, 2);
	UT_ASSERT(memcmp(saved, references->entries, sizeof(saved)) == 0);
	UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
	retirement_local_cleanup();
}

UT_TEST(test_startup_reference_tuples_require_all_itls_from_the_same_page_version)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		ClusterRecoveryCensusReferencesV1 *references = reference_tuples_ready();
		ClusterRecoveryCensusReferenceV1 *last = &references->entries[16];

		if (fault == 0) last->version++;
		if (fault == 1) last->page_index = 0;
		if (fault == 2) last->entry.index = 6;
		if (fault == 3) last->entry.kind = CLUSTER_RECOVERY_HEAP_ITEM;
		retirement_run_startup();
		UT_ASSERT(!retirement_service_reached && info.cluster_tx_start->revoked);
		UT_ASSERT(pending_storage_live && observation_scope_live);
		UT_ASSERT_EQ(pending_storage_closes, 0);
		retirement_local_cleanup();
	}
}

UT_TEST(test_startup_reference_tuples_preserve_stored_item_bounds_and_unsupported_storage)
{
	for (unsigned fault = 0; fault < 5; fault++) {
		ClusterRecoveryCensusReferencesV1 *references = reference_tuples_ready();
		ClusterRecoveryHeapEntry *last = &references->entries[17].entry;

		if (fault == 0) last->tuple_length--;
		if (fault == 1) last->tuple_bytes = NULL;
		if (fault == 2) last->item.lp_flags = LP_DEAD;
		if (fault == 3) last->index = 0;
		if (fault == 4) { last->tuple_length = 0; last->tuple_bytes = NULL; }
		retirement_run_startup();
		UT_ASSERT(!retirement_service_reached && info.cluster_tx_start->revoked);
		UT_ASSERT(pending_storage_live && observation_scope_live);
		retirement_local_cleanup();
	}
}

UT_TEST(test_startup_reference_tuples_late_native_refusal_drift_and_error_keep_debt)
{
	for (unsigned fault = 1; fault <= 3; fault++) {
		(void)reference_tuples_ready(); reference_tuple_fault = fault;
		retirement_run_startup();
		UT_ASSERT(!retirement_service_reached && info.cluster_tx_start->revoked);
		UT_ASSERT_EQ(reference_tuple_calls, 2);
		UT_ASSERT(pending_storage_live && observation_scope_live && observation_cursor_live);
		UT_ASSERT_EQ(pending_storage_closes, 0);
		retirement_local_cleanup();
	}
}

UT_TEST(test_startup_reference_tuples_repeat_rechecks_without_new_page_or_wal)
{
	unsigned steps, opens;

	(void)reference_tuples_ready(); retirement_run_startup();
	UT_ASSERT(retirement_service_reached);
	steps = prefix_steps; opens = pending_storage_created;
	reference_tuple_calls = 0; reference_tuple_fault = 1;
	UT_ASSERT(!ClusterColdRecoveryCtrcPageDrain(info.cluster_tx_start, 0));
	UT_ASSERT_EQ(reference_tuple_calls, 2);
	UT_ASSERT_EQ(prefix_steps, steps); UT_ASSERT_EQ(pending_storage_created, opens);
	UT_ASSERT(pending_storage_live && observation_scope_live);
	retirement_local_cleanup();
}

UT_TEST(test_startup_reference_tuples_zero_page_wal_still_checks_complete_view)
{
	(void)reference_tuples_ready(); prefix_count = 0;
	ctrc_wait_step = reference_terminals_empty_probe; reference_tuple_fault = 1;
	retirement_run_startup();
	UT_ASSERT(!retirement_service_reached && info.cluster_tx_start->revoked);
	UT_ASSERT_EQ(reference_tuple_calls, 2); UT_ASSERT_EQ(prefix_steps, 0);
	UT_ASSERT(observation_scope_live && observation_cursor_live);
	retirement_local_cleanup();
}
