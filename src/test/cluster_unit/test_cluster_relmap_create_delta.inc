/*-------------------------------------------------------------------------
 * test_cluster_relmap_create_delta.inc
 *    Preserve the original receipt before accepting its exact owner delta.
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_relmap_create_delta.inc
 * NOTES
 *    The new typed-owner acceptance leaf is controlled; plans/pairs are real.
 *-------------------------------------------------------------------------
 */
bool cluster_cold_typed_relmap_create_accept_confirmation_v2(ClusterColdTypedV1 *,
	struct ClusterRecoveryFencePlan *, uint64, const ClusterRecoveryFileIdentityV2 *,
	const uint8[16], const ClusterControlRelmapRecoveryCreateV2 *);
bool cluster_cold_typed_relmap_create_accept_confirmation_v2(ClusterColdTypedV1 *typed,
	struct ClusterRecoveryFencePlan *fence, uint64 id, const ClusterRecoveryFileIdentityV2 *expected,
	const uint8 batch[16], const ClusterControlRelmapRecoveryCreateV2 *confirmation)
{
	const ColdRelmapCreateInput *entry=cold_relmap_create_at(redo_typed.plan,0);
	++barrier_delta_calls;
	/* The owner accepts its own completed confirmation, not a caller's token.
	 * Storage must already be attached before this fallible boundary is entered. */
	if (typed!=&redo_typed || fence!=redo_fence || id!=barrier_id || !expected || !batch
		|| memcmp(expected,&owner_declaration.creation.identity,sizeof(*expected))
		|| memcmp(batch,owner_declaration.creation.batch_uuid,16)
		|| confirmation!=barrier_confirmation || !entry || !entry->attempted || entry->applied
		|| !entry->receipt || !entry->receipt_release || !entry->receipt_output_distinct
		|| held_family_lock || !cf_held || !copy_x || !target_available()) return false;
	if (barrier_delta_fault==2) { InterruptHoldoffCount=0; pg_re_throw(); }
	if (barrier_delta_fault==1) return false;
	barrier_delta_accepted=true;
	if (barrier_delta_fault==3) owner_source_ok=false;
	return true;
}

UT_TEST(creation_delta_accepts_before_first_postconfirm_source_check)
{
	ClusterControlNativeRelmapCreateV2 *creator=NULL;
	XLogReaderState reader; DecodedXLogRecord decoded;
	bool applied;
	UT_ASSERT(barrier_setup(&creator,&reader,&decoded));
	barrier_delta_required=true;
	applied=barrier_apply(&reader,barrier_id);
	UT_ASSERT(applied && barrier_delta_calls==1 && barrier_delta_accepted);
	UT_ASSERT(target_available() && cold_relmap_create_at(redo_typed.plan,0)->receipt);
	if (applied) UT_ASSERT(barrier_ready(barrier_id) && !redo_failed);
	barrier_done(&creator);
}

UT_TEST(creation_delta_refusal_and_error_retain_attached_receipt)
{
	for (unsigned fault=1;fault<=2;++fault) {
		ClusterControlNativeRelmapCreateV2 *creator=NULL;
		XLogReaderState reader; DecodedXLogRecord decoded;
		volatile bool caught=false, applied=false;
		unsigned syncs,renames;
		UT_ASSERT(barrier_setup(&creator,&reader,&decoded));
		barrier_delta_required=true; barrier_delta_fault=fault;
		PG_TRY(); { applied=barrier_apply(&reader,barrier_id); }
		PG_CATCH(); { caught=true; }
		PG_END_TRY();
		UT_ASSERT(!applied && caught==(fault==2) && barrier_delta_calls==1);
		UT_ASSERT(target_available() && cold_relmap_create_at(redo_typed.plan,0)->receipt
			&& !cold_relmap_create_at(redo_typed.plan,0)->applied && redo_failed
			&& redo_typed.plan->relmap_failed);
		syncs=copy_syncs; renames=root_renames; barrier_delta_fault=0;
		UT_ASSERT(!barrier_apply(&reader,barrier_id) && !barrier_ready(barrier_id)
			&& barrier_delta_calls==1 && copy_syncs==syncs && root_renames==renames);
		UT_ASSERT(!held_family_lock && !InterruptHoldoffCount && cf_held && copy_x);
		barrier_done(&creator);
	}
}

UT_TEST(creation_delta_repeat_and_ready_do_not_accept_again)
{
	ClusterControlNativeRelmapCreateV2 *creator=NULL;
	XLogReaderState reader; DecodedXLogRecord decoded;
	bool applied; unsigned syncs,renames;
	UT_ASSERT(barrier_setup(&creator,&reader,&decoded));
	barrier_delta_required=true;
	applied=barrier_apply(&reader,barrier_id); UT_ASSERT(applied);
	if (applied) {
		syncs=copy_syncs; renames=root_renames;
		cf_held=false; ++cf_sequence; cf_held=true;
		UT_ASSERT(barrier_apply(&reader,barrier_id) && barrier_ready(barrier_id)
			&& barrier_delta_calls==1 && barrier_repeats==2
			&& copy_syncs==syncs && root_renames==renames);
	}
	barrier_done(&creator);
}

UT_TEST(creation_delta_does_not_hide_postaccept_source_change)
{
	ClusterControlNativeRelmapCreateV2 *creator=NULL;
	XLogReaderState reader; DecodedXLogRecord decoded;
	unsigned syncs;
	UT_ASSERT(barrier_setup(&creator,&reader,&decoded));
	barrier_delta_required=true; barrier_delta_fault=3;
	UT_ASSERT(!barrier_apply(&reader,barrier_id) && barrier_delta_calls==1
		&& barrier_delta_accepted && redo_failed && target_available()
		&& cold_relmap_create_at(redo_typed.plan,0)->receipt
		&& !cold_relmap_create_at(redo_typed.plan,0)->applied);
	syncs=copy_syncs; owner_source_ok=true; barrier_delta_fault=0;
	UT_ASSERT(!barrier_apply(&reader,barrier_id) && !barrier_ready(barrier_id)
		&& barrier_delta_calls==1 && copy_syncs==syncs);
	barrier_done(&creator);
}
