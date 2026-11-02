/*-------------------------------------------------------------------------
 * test_cluster_relmap_create_owner.inc
 *    Original merged-plan creation attempt, saved receipt and local cleanup.
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_relmap_create_owner.inc
 * NOTES
 *    Plan/ROOT/registry/pairs are real; original source/SIDE leaves controlled.
 *-------------------------------------------------------------------------
 */
#include "cluster/cluster_side_online_plan.h"
#include "test_cluster_relmap_create_side_original.h"
static ClusterColdRelmapCreateInputV1 owner_declaration;
static ClusterWalSourceRef owner_source_snapshot;
static ClusterColdStepV1 owner_step;
static bool owner_side_ok, owner_source_ok, owner_side_error, owner_reentry;
static unsigned owner_source_calls, owner_fail_call;
static bool owner_source_error, owner_physical_reentry;
static ClusterRelmapRecoveryCreateReceiptV2 *owner_physical_receipt;
static XLogReaderState *owner_reader;
static RfSideOnlinePlanV1 *const owner_side=(void *)0x5678;

/* These are outer source/SIDE leaf refusals, not replacement file execution. */
bool cluster_cold_typed_sources_current_v1(ClusterColdTypedV1 *typed,
	struct ClusterRecoveryFencePlan *fence)
{
	++owner_source_calls;
	if(owner_source_error) { InterruptHoldoffCount=0; pg_re_throw(); }
	if(owner_physical_reentry) {
		owner_physical_reentry=false;
		(void)cluster_relmap_recovery_create_physical_recheck_v2(owner_physical_receipt,typed,fence);
	}
	return typed==&redo_typed && fence==redo_fence && owner_source_ok && !redo_failed
		&& owner_source_calls!=owner_fail_call;
}
bool rf_side_online_plan_recovery_sources_match_v1(const RfSideOnlinePlanV1 *side,
	const ClusterWalSourceRef *selected, uint32 count)
{
	if(owner_side_error) { InterruptHoldoffCount=0; pg_re_throw(); }
	if(owner_reentry) {
		owner_reentry=false;
		(void)cluster_cold_typed_relmap_create_apply_v2(&redo_typed,redo_fence,&owner_step,owner_reader,65536);
	}
	if(side!=owner_side) return owner_side_ok && create_side_original_sources(side,selected,count);
	return owner_side_ok && side==owner_side && count==1
		&& !memcmp(selected,&owner_source_snapshot,sizeof(*selected));
}
uint32 rf_side_online_plan_relmap_create_count_v1(const RfSideOnlinePlanV1 *side,
	const ClusterWalSourceRef *source)
{
	if(side!=owner_side) return owner_side_ok ? create_side_original_count(side,source) : UINT32_MAX;
	return side==owner_side && owner_side_ok && !memcmp(source,&owner_source_snapshot,sizeof(*source)) ? 1 : UINT32_MAX;
}
bool rf_side_online_plan_relmap_create_input_matches_v1(const RfSideOnlinePlanV1 *side,
	const ClusterColdRelmapCreateInputV1 *input)
{
	if(side!=owner_side) return owner_side_ok && create_side_original_input(side,input);
	return side==owner_side && owner_side_ok && !memcmp(input,&owner_declaration,sizeof(*input));
}
bool rf_side_online_plan_relmap_create_matches_v1(const RfSideOnlinePlanV1 *side,
	const ClusterWalSourceRef *source,const XLogReaderState *reader)
{
	ClusterRelmapCreateReplayV1 value;
	if(side!=owner_side) return owner_side_ok && create_side_original_record(side,source,reader);
	return side==owner_side && owner_side_ok && source
		&& !memcmp(source,&owner_source_snapshot,sizeof(*source))
		&& cluster_relmap_control_create_redo_input_v1((XLogReaderState *)reader,
			&owner_declaration.creation.identity,owner_declaration.creation.batch_uuid,&value);
}
static bool owner_no_pages(void *arg,const RfPageIdentityV1 *page,ClusterColdDataV1 *out)
{
	(void)arg; (void)page; (void)out; abort();
}
static bool owner_setup(ClusterControlNativeRelmapCreateV2 **creator,
	XLogReaderState *reader,DecodedXLogRecord *decoded)
{
	ClusterColdPlanV1 *plan=NULL;
	ClusterColdRecordV1 record={0};
	copy_begin();
	if(!reserve_copy(creator) || !creation_replay_record(*creator,reader,decoded)) return false;
	recovery_copy_enter();
	memset(&owner_declaration,0,sizeof(owner_declaration));
	owner_declaration.source=redo_source;
	owner_declaration.cut=(ClusterColdParticipantV1){1,0,1,
		redo_source.claim.identity.origin_owner_incarnation,reader->ReadRecPtr,
		reader->ReadRecPtr,reader->EndRecPtr};
	owner_declaration.frame=redo_frame;
	if(!cluster_relmap_create_decode_v1(wal_bytes,sizeof(wal_bytes),&owner_declaration.creation)
		|| cluster_cold_plan_create_v1(&owner_declaration.cut,1,4*1024*1024,&plan)!=0
		|| cluster_cold_plan_relmap_create_declare_v1(plan,&owner_declaration,1)!=0) return false;
	record.read_rec_ptr=reader->ReadRecPtr; record.end_rec_ptr=reader->EndRecPtr;
	record.record_crc=redo_frame.record_crc; record.rmid=RM_RELMAP_ID; record.info=CLUSTER_RELMAP_CREATE_WAL_INFO_V1;
	record.record_flags=CLUSTER_COLD_RECORD_STRUCTURAL | CLUSTER_COLD_RECORD_SIDE_UNOWNED;
	record.relmap_dbid=owner_declaration.creation.identity.database_oid;
	record.relmap_tsid=DEFAULTTABLESPACE_OID; record.relmap_create=&owner_declaration.creation;
	if(cluster_cold_plan_feed_v1(plan,0,&record)!=0
		|| cluster_cold_plan_seal_v1(plan,owner_no_pages,NULL,NULL)!=0
		|| !cluster_cold_plan_step_v1(plan,0,&owner_step)) return false;
	redo_typed.plan=plan; redo_typed.participant_count=redo_typed.replay_count=1;
	redo_typed.system_identifier=redo_source.claim.identity.system_identifier;
	redo_typed.sources[0]=redo_source; redo_typed.participants[0]=owner_declaration.cut;
	redo_typed.side_plans[0]=owner_side; redo_typed.tx_fence=redo_fence;
	owner_source_snapshot=redo_source; owner_side_ok=owner_source_ok=true;
	owner_source_error=owner_physical_reentry=false; owner_physical_receipt=NULL;
	owner_side_error=owner_reentry=false; owner_source_calls=owner_fail_call=0; owner_reader=reader;
	return true;
}
static void owner_done(ClusterControlNativeRelmapCreateV2 **creator)
{
	cluster_cold_plan_destroy_v1(&redo_typed.plan);
	if(redo_typed.plan) abort();
	recovery_copy_leave(); copy_end(creator);
}
static bool owner_apply(XLogReaderState *reader,Size budget)
{
	return cluster_cold_typed_relmap_create_apply_v2(&redo_typed,redo_fence,&owner_step,reader,budget);
}
UT_TEST(creation_owner_first_step_keeps_receipt_until_original_plan_destroy)
{
	ClusterControlNativeRelmapCreateV2 *creator=NULL;
	XLogReaderState reader; DecodedXLogRecord decoded;
	ColdRelmapCreateInput *entry; Size before; unsigned syncs,renames;
	UT_ASSERT(owner_setup(&creator,&reader,&decoded));
	before=redo_typed.plan->memory_used;
	UT_ASSERT(owner_apply(&reader,65536));
	entry=cold_relmap_create_at(redo_typed.plan,0);
	UT_ASSERT(entry && entry->attempted && entry->applied && entry->receipt && entry->receipt_release);
	UT_ASSERT(entry->receipt_bytes>0 && redo_typed.plan->memory_used==before+entry->receipt_bytes);
	UT_ASSERT(!redo_typed.plan->relmap_failed && target_available() && !redo_failed);
	syncs=copy_syncs; renames=root_renames;
	owner_done(&creator);
	UT_ASSERT(copy_syncs==syncs && root_renames==renames && !redo_failed && !InterruptHoldoffCount);
}
UT_TEST(creation_owner_duplicate_rechecks_without_resync_or_confirmation)
{
	ClusterControlNativeRelmapCreateV2 *creator=NULL;
	XLogReaderState reader; DecodedXLogRecord decoded;
	unsigned syncs,renames;
	UT_ASSERT(owner_setup(&creator,&reader,&decoded)); UT_ASSERT(owner_apply(&reader,65536));
	syncs=copy_syncs; renames=root_renames;
	UT_ASSERT(owner_apply(&reader,65536));
	UT_ASSERT(!redo_typed.plan->relmap_failed && !redo_failed && copy_syncs==syncs && root_renames==renames);
	UT_ASSERT(cold_relmap_create_at(redo_typed.plan,0)->receipt);
	owner_done(&creator);
}
UT_TEST(creation_owner_whole_batch_and_original_step_precede_mutation)
{
	for(unsigned fault=0;fault<7;fault++) {
		ClusterControlNativeRelmapCreateV2 *creator=NULL;
		XLogReaderState reader; DecodedXLogRecord decoded;
		UT_ASSERT(owner_setup(&creator,&reader,&decoded));
		if(fault==0) owner_side_ok=false;
		if(fault==1) owner_source_ok=false;
		if(fault==2) ++redo_typed.sources[0].claim.identity.root_lineage_seq;
		if(fault==3) ++owner_step.record_crc;
		if(fault==4) ++cold_relmap_create_at(redo_typed.plan,0)->input.creation.identity.registration_generation;
		if(fault==5) cold_relmap_create_at(redo_typed.plan,0)->captured=false;
		if(fault==6) ++redo_typed.participants[0].tail_end;
		UT_ASSERT(!owner_apply(&reader,65536));
		UT_ASSERT(!copy_syncs && root_renames==2 && !target_available() && redo_typed.plan->relmap_failed);
		owner_done(&creator);
	}
}
UT_TEST(creation_owner_false_error_and_provider_reentry_lock_the_attempt)
{
	for(unsigned fault=0;fault<4;fault++) {
		ClusterControlNativeRelmapCreateV2 *creator=NULL;
		XLogReaderState reader; DecodedXLogRecord decoded; volatile bool caught=false;
		unsigned syncs;
		UT_ASSERT(owner_setup(&creator,&reader,&decoded));
		if(fault==0) fail_sync=2;
		if(fault==1) owner_side_error=true;
		if(fault==2) recovery_sync_error=true;
		if(fault==3) owner_reentry=true;
		PG_TRY(); { UT_ASSERT(!owner_apply(&reader,65536)); }
		PG_CATCH(); { caught=true; }
		PG_END_TRY();
		UT_ASSERT(caught==(fault==1 || fault==2));
		UT_ASSERT(cold_relmap_create_at(redo_typed.plan,0)->attempted && redo_typed.plan->relmap_failed && redo_failed);
		owner_side_error=recovery_sync_error=false; fail_sync=0; syncs=copy_syncs;
		UT_ASSERT(!owner_apply(&reader,65536) && copy_syncs==syncs && !held_family_lock && !InterruptHoldoffCount);
		owner_done(&creator);
	}
}
UT_TEST(creation_owner_late_refusal_keeps_the_confirmed_receipt_and_debt)
{
	ClusterControlNativeRelmapCreateV2 *creator=NULL;
	XLogReaderState reader; DecodedXLogRecord decoded; ColdRelmapCreateInput *entry;
	UT_ASSERT(owner_setup(&creator,&reader,&decoded));
	owner_fail_call=2; /* First current precedes native create; second follows confirm. */
	UT_ASSERT(!owner_apply(&reader,65536)); entry=cold_relmap_create_at(redo_typed.plan,0);
	UT_ASSERT(entry->attempted && !entry->applied && entry->receipt && target_available());
	UT_ASSERT(redo_typed.plan->relmap_failed && redo_failed && root_renames==4 && cf_held && copy_x);
	owner_done(&creator);
}
UT_TEST(creation_owner_capacity_refuses_before_authority_or_native_io)
{
	ClusterControlNativeRelmapCreateV2 *creator=NULL;
	XLogReaderState reader; DecodedXLogRecord decoded;
	UT_ASSERT(owner_setup(&creator,&reader,&decoded));
	redo_typed.plan->memory_budget=redo_typed.plan->memory_used+16;
	UT_ASSERT(!owner_apply(&reader,65536));
	UT_ASSERT(cold_relmap_create_at(redo_typed.plan,0)->attempted && redo_typed.plan->relmap_failed
		&& !copy_syncs && root_renames==2 && !owner_source_calls);
	owner_done(&creator);
}
UT_TEST(creation_owner_saved_storage_rejects_plan_output_aliases)
{
	ClusterControlNativeRelmapCreateV2 *creator=NULL;
	XLogReaderState reader; DecodedXLogRecord decoded; ColdRelmapCreateInput *entry;
	ClusterRecoveryCheckpointFileV2 file;
	UT_ASSERT(owner_setup(&creator,&reader,&decoded)); UT_ASSERT(owner_apply(&reader,65536));
	entry=cold_relmap_create_at(redo_typed.plan,0);
	UT_ASSERT(cluster_relmap_recovery_create_file_at_v2(entry->receipt,0,&file));
	UT_ASSERT(!cold_relmap_output_distinct(&redo_typed,file.sources,sizeof(*file.sources)));
	UT_ASSERT(!cluster_relmap_recovery_create_source_at_v2(entry->receipt,0,(void *)file.sources));
	UT_ASSERT(cf_held && copy_x && !redo_failed);
	owner_done(&creator);
}
