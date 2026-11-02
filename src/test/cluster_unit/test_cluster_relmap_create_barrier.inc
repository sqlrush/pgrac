/*-------------------------------------------------------------------------
 * test_cluster_relmap_create_barrier.inc
 *    Original creation consumer without serial custody and with saved receipts.
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_relmap_create_barrier.inc
 * NOTES
 *    New A barrier/current authority is controlled; ROOT, plans and pairs are real.
 *-------------------------------------------------------------------------
 */
static const uint64 barrier_id = 73;
static bool barrier_enabled, barrier_error;
static unsigned barrier_calls, barrier_fail_call, barrier_repeats;
static ClusterControlRelmapRecoveryCreateV2 *barrier_confirmation;
static bool barrier_output_ok=true;

bool cluster_control_relmap_recovery_create_output_distinct_v2(
	const ClusterControlRelmapRecoveryCreateV2 *c, const void *out, Size bytes)
{
	/* The opaque carrier/selection extent is checked by A in production. This
	 * leaf checks delegation and the base-address alias without knowing layout. */
	return barrier_output_ok && c==barrier_confirmation && out && bytes && out!=(const void *)c;
}
/* Only the new A boundary is controlled here. Its actual Startup/H and
 * independent confirmed-control implementation is verified by its owner. */
bool cluster_cold_typed_relmap_create_barrier_current_v2(ClusterColdTypedV1 *typed,
	struct ClusterRecoveryFencePlan *fence, uint64 id)
{
	if (held_family_lock) abort();
	++barrier_calls;
	if (barrier_error) { InterruptHoldoffCount=0; pg_re_throw(); }
	return barrier_enabled && id==barrier_id && typed==&redo_typed && fence==redo_fence
		&& owner_source_ok && !redo_failed && barrier_calls!=barrier_fail_call;
}
ClusterControlNativeResultV2 cluster_control_relmap_recovery_create_begin_barrier_locked_v2(
	ClusterColdTypedV1 *typed, struct ClusterRecoveryFencePlan *fence, uint64 id,
	uint32 participant, const ClusterWalSourceRef *source, XLogRecPtr read, XLogRecPtr end,
	const ClusterRecoveryFileIdentityV2 *expected, const uint8 batch[16],
	ClusterControlRelmapRecoveryCreateV2 **out)
{
	ClusterControlNativeResultV2 result;
	if (!cluster_cold_typed_relmap_create_barrier_current_v2(typed,fence,id))
		return CLUSTER_NATIVE_FILES_NOT_AVAILABLE_V2;
	/* Real first authorization/confirmation, with its unchanged SAME-CF rule.
	 * The original Startup authority leaf above is already controlled. */
	result=cluster_control_relmap_recovery_create_begin_locked_v2(typed,fence,participant,
		source,read,end,expected,batch,out);
	if (!result) barrier_confirmation=*out;
	return result;
}
bool cluster_control_relmap_recovery_create_confirmed_barrier_current_v2(
	ClusterControlRelmapRecoveryCreateV2 *confirmation, uint64 id,
	const ClusterRecoveryCheckpointFileV2 *files, uint64 count)
{
	if (held_family_lock) abort();
	++barrier_repeats;
	return cluster_cold_typed_relmap_create_barrier_current_v2(&redo_typed,redo_fence,id)
		&& confirmation==barrier_confirmation && target_available() && count==2 && files
		&& files[0].copy_index==0 && files[1].copy_index==1
		&& files[0].copy_count==2 && files[1].copy_count==2
		&& files[0].source_count==2 && files[1].source_count==2
		&& files[0].sources && files[1].sources
		&& !memcmp(&files[0].identity,&owner_expected.expected,sizeof(files[0].identity))
		&& !memcmp(&files[1].identity,&owner_expected.expected,sizeof(files[1].identity));
}
static bool barrier_setup(ClusterControlNativeRelmapCreateV2 **creator,
	XLogReaderState *reader, DecodedXLogRecord *decoded)
{
	if (!owner_setup(creator,reader,decoded)) return false;
	redo_typed.tx_fence=NULL; /* H has not handed over the serial fence. */
	barrier_enabled=true; barrier_error=false; barrier_calls=barrier_fail_call=barrier_repeats=0;
	barrier_confirmation=NULL; barrier_output_ok=true;
	return true;
}
static bool barrier_apply(XLogReaderState *reader, uint64 id)
{
#ifdef TEST_CREATE_BARRIER_BASELINE
	(void)id;
	return owner_apply(reader,65536);
#else
	return cluster_cold_typed_relmap_create_apply_barrier_v2(
		&redo_typed,redo_fence,id,&owner_step,reader,65536);
#endif
}
static bool barrier_ready(uint64 id)
{
#ifdef TEST_CREATE_BARRIER_BASELINE
	(void)id;
	return cluster_cold_typed_relmap_create_ready_v2(&redo_typed,redo_fence);
#else
	return cluster_cold_typed_relmap_create_ready_barrier_v2(&redo_typed,redo_fence,id);
#endif
}
static void barrier_done(ClusterControlNativeRelmapCreateV2 **creator)
{
	barrier_error=false; barrier_enabled=false; owner_done(creator);
}
UT_TEST(creation_barrier_without_serial_executes_once_and_exports_original_receipts)
{
	ClusterControlNativeRelmapCreateV2 *creator=NULL;
	XLogReaderState reader; DecodedXLogRecord decoded;
	const ClusterRelmapRecoveryCreateReceiptV2 *receipt=NULL;
	const ClusterControlRelmapRecoveryCreateV2 *confirmation=NULL;
	ClusterRelmapRecoveryCreateObservationV2 observation;
	bool applied;
	UT_ASSERT(barrier_setup(&creator,&reader,&decoded));
	applied=barrier_apply(&reader,barrier_id); UT_ASSERT(applied);
	if (applied) {
		UT_ASSERT(!redo_typed.tx_fence && !owner_source_calls && target_available());
#ifndef TEST_CREATE_BARRIER_BASELINE
		UT_ASSERT(cluster_cold_typed_relmap_create_receipt_at_barrier_v2(
			&redo_typed,barrier_id,0,&receipt,&confirmation));
		UT_ASSERT(receipt==cold_relmap_create_at(redo_typed.plan,0)->receipt
			&& confirmation==barrier_confirmation);
		UT_ASSERT(cluster_relmap_recovery_create_observation_v2(receipt,&observation)
			&& !memcmp(&observation.creation.record,&owner_declaration.creation,sizeof(owner_declaration.creation)));
#else
		(void)receipt; (void)confirmation; (void)observation;
#endif
		UT_ASSERT(barrier_ready(barrier_id) && !redo_failed && !owner_source_calls);
	}
	barrier_done(&creator);
}
UT_TEST(creation_barrier_fresh_cf_rechecks_without_execution_or_confirmation)
{
	ClusterControlNativeRelmapCreateV2 *creator=NULL;
	XLogReaderState reader; DecodedXLogRecord decoded;
	bool applied; unsigned syncs,renames;
	UT_ASSERT(barrier_setup(&creator,&reader,&decoded));
	applied=barrier_apply(&reader,barrier_id); UT_ASSERT(applied);
	if (applied) {
		syncs=copy_syncs; renames=root_renames;
		cf_held=false; ++cf_sequence; cf_held=true;
		UT_ASSERT(barrier_apply(&reader,barrier_id) && barrier_ready(barrier_id));
		UT_ASSERT(barrier_repeats==2 && copy_syncs==syncs && root_renames==renames
			&& !owner_source_calls && !redo_failed && !redo_typed.tx_fence);
	}
	barrier_done(&creator);
}
UT_TEST(creation_barrier_wrong_episode_source_frame_or_fence_refuses)
{
	for (unsigned fault=0;fault<5;++fault) {
		ClusterControlNativeRelmapCreateV2 *creator=NULL;
		XLogReaderState reader; DecodedXLogRecord decoded;
		uint64 id=barrier_id;
		UT_ASSERT(barrier_setup(&creator,&reader,&decoded));
		if (fault==0) id=0;
		if (fault==1) ++id;
		if (fault==2) ++redo_typed.sources[0].claim.identity.origin_owner_incarnation;
		if (fault==3) ++decoded.header.xl_crc;
#ifndef TEST_CREATE_BARRIER_BASELINE
		if (fault==4) {
			UT_ASSERT(!cluster_cold_typed_relmap_create_apply_barrier_v2(&redo_typed,
				(void *)0x9999,id,&owner_step,&reader,65536) && !copy_syncs && !target_available());
		} else
#endif
			UT_ASSERT(!barrier_apply(&reader,id) && !copy_syncs && !target_available());
		if (fault!=0) UT_ASSERT(!barrier_apply(&reader,barrier_id));
		barrier_done(&creator);
	}
}
UT_TEST(creation_barrier_late_failure_keeps_attached_receipt_and_debt)
{
	ClusterControlNativeRelmapCreateV2 *creator=NULL;
	XLogReaderState reader; DecodedXLogRecord decoded;
	UT_ASSERT(barrier_setup(&creator,&reader,&decoded));
	/* Outer precheck, first authorization, then post-capture current. */
	barrier_fail_call=3;
	UT_ASSERT(!barrier_apply(&reader,barrier_id));
	UT_ASSERT(target_available() && cold_relmap_create_at(redo_typed.plan,0)->receipt
		&& !cold_relmap_create_at(redo_typed.plan,0)->applied && redo_failed);
	barrier_fail_call=0;
	UT_ASSERT(!barrier_apply(&reader,barrier_id) && !barrier_ready(barrier_id));
	barrier_done(&creator);
}
UT_TEST(creation_barrier_changed_pair_rejects_repeat_and_final_gate)
{
	ClusterControlNativeRelmapCreateV2 *creator=NULL;
	XLogReaderState reader; DecodedXLogRecord decoded;
	bool applied; unsigned syncs;
	UT_ASSERT(barrier_setup(&creator,&reader,&decoded));
	applied=barrier_apply(&reader,barrier_id); UT_ASSERT(applied);
	if (applied) {
		syncs=copy_syncs; physical_target_fault(creator,3); ++cf_sequence;
		UT_ASSERT(!barrier_apply(&reader,barrier_id) && !barrier_ready(barrier_id));
		UT_ASSERT(redo_failed && cold_relmap_create_at(redo_typed.plan,0)->receipt
			&& !barrier_repeats && copy_syncs==syncs);
	}
	barrier_done(&creator);
}
UT_TEST(creation_barrier_receipt_outputs_reject_all_aliases_before_write)
{
	ClusterControlNativeRelmapCreateV2 *creator=NULL;
	XLogReaderState reader; DecodedXLogRecord decoded;
	bool applied;
	UT_ASSERT(barrier_setup(&creator,&reader,&decoded));
	applied=barrier_apply(&reader,barrier_id); UT_ASSERT(applied);
#ifndef TEST_CREATE_BARRIER_BASELINE
	if (applied) {
		const ClusterRelmapRecoveryCreateReceiptV2 *r=NULL,*again=NULL;
		const ClusterControlRelmapRecoveryCreateV2 *c=NULL,*saved;
		ClusterRecoveryCheckpointFileV2 file;
		UT_ASSERT(cluster_cold_typed_relmap_create_receipt_at_barrier_v2(&redo_typed,barrier_id,0,&r,&c));
		saved=c;
		UT_ASSERT(!cluster_cold_typed_relmap_create_receipt_at_barrier_v2(&redo_typed,barrier_id+1,0,&again,&c)
			&& !again && c==saved);
		UT_ASSERT(!cluster_cold_typed_relmap_create_receipt_at_barrier_v2(&redo_typed,barrier_id,1,&again,&c)
			&& !again && c==saved);
		UT_ASSERT(cluster_relmap_recovery_create_file_at_v2(r,0,&file));
		UT_ASSERT(!cluster_cold_typed_relmap_create_receipt_at_barrier_v2(&redo_typed,barrier_id,0,
			(void *)file.sources,&c) && c==saved);
		UT_ASSERT(!cluster_cold_typed_relmap_create_receipt_at_barrier_v2(&redo_typed,barrier_id,0,
			&again,(void *)&again) && !again);
		UT_ASSERT(!cluster_cold_typed_relmap_create_receipt_at_barrier_v2(&redo_typed,barrier_id,0,
			(void *)&redo_typed,&c) && c==saved);
		{
			unsigned char before[sizeof(void *)];
			bool accepted;
			memcpy(before,c,sizeof(before));
			accepted=cluster_cold_typed_relmap_create_receipt_at_barrier_v2(&redo_typed,barrier_id,0,
				&again,(void *)c);
			UT_ASSERT(!accepted && !memcmp(before,c,sizeof(before)));
			memcpy((void *)c,before,sizeof(before));
		}
		barrier_output_ok=false;
		UT_ASSERT(!cluster_cold_typed_relmap_create_receipt_at_barrier_v2(&redo_typed,barrier_id,0,&again,&c));
		barrier_output_ok=true;
		UT_ASSERT(cluster_cold_typed_relmap_create_receipt_at_barrier_v2(&redo_typed,barrier_id,0,&again,&c)
			&& again==r && c==saved && barrier_ready(barrier_id));
	}
#else
	(void)applied;
#endif
	barrier_done(&creator);
}
UT_TEST(creation_barrier_owner_error_is_sticky_and_never_retries)
{
	ClusterControlNativeRelmapCreateV2 *creator=NULL;
	XLogReaderState reader; DecodedXLogRecord decoded;
	volatile bool caught=false;
	UT_ASSERT(barrier_setup(&creator,&reader,&decoded)); barrier_error=true;
	PG_TRY(); { (void)barrier_apply(&reader,barrier_id); }
	PG_CATCH(); { caught=true; }
	PG_END_TRY();
	UT_ASSERT(caught && redo_failed && redo_typed.plan->relmap_failed && !copy_syncs);
	barrier_error=false;
	UT_ASSERT(!barrier_apply(&reader,barrier_id) && !barrier_ready(barrier_id));
	UT_ASSERT(!held_family_lock && !InterruptHoldoffCount && cf_held && copy_x);
	barrier_done(&creator);
}
