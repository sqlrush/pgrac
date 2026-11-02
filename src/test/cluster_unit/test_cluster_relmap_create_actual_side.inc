/*-------------------------------------------------------------------------
 * test_cluster_relmap_create_actual_side.inc
 *    Sealed original SIDE through the actual creation and receipt path.
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_relmap_create_actual_side.inc
 * NOTES
 *    Outer source, authority, WAL and locks remain controlled.
 *-------------------------------------------------------------------------
 */
static RfSideOnlinePlanV1 *actual_creation_side(const ClusterColdRelmapCreateInputV1 *input,
 XLogReaderState *reader,bool seal)
{
 RfSideOnlinePlanV1 *side=NULL;
 RfContributorStreamCutV1 cut={0};
 RfSideOnlinePlanRequestV1 request={0};
 RfPageOnlineRecordIdentityV1 identity={0};
 RfDetachedRecordPlanV1 detached={0};
 cut.failed_thread=input->cut.thread_id; cut.timeline_id=input->cut.timeline;
 cut.origin_owner_incarnation=input->cut.owner_incarnation;
 cut.scan_begin_inclusive=input->cut.physical_lower; cut.scan_end_exclusive=input->cut.tail_end;
 cut.flags=RF_CONTRIBUTOR_CUT_COMPLETE;
 request.system_identifier=input->source.claim.identity.system_identifier;
 memcpy(request.storage_uuid,input->source.claim.identity.storage_uuid,16);
 request.physical_cuts=&cut; request.participant_count=1;
 request.memory_budget=1048576; request.redo_starts=&input->cut.native_redo;
 request.purpose=RF_SIDE_PLAN_RECOVERY;
 if(rf_side_online_plan_create_v1(&request,&side)!=0
  || !rf_side_online_plan_bind_database_v1(side,input->source.claim.database_incarnation)
  || !rf_side_online_plan_bind_sources_v1(side,&input->source,1)) goto failed;
 identity.record.system_identifier=reader->system_identifier;
 memcpy(identity.record.storage_uuid,input->source.claim.identity.storage_uuid,16);
 identity.record.origin_thread=input->cut.thread_id; identity.record.timeline_id=input->cut.timeline;
 identity.record.read_rec_ptr=reader->ReadRecPtr; identity.record.end_rec_ptr=reader->EndRecPtr;
 identity.record.record_crc=reader->record->header.xl_crc;
 identity.record.rmid=RM_RELMAP_ID; identity.record.info=CLUSTER_RELMAP_CREATE_WAL_INFO_V1;
 detached.source_record=reader; detached.preflight_complete=true;
 detached.route.record_owner=RF_ROUTE_OWNER_SIDE_TYPED;
 detached.route.rmid=RM_RELMAP_ID; detached.route.normalized_info=CLUSTER_RELMAP_CREATE_WAL_INFO_V1;
 detached.route.codec_id=RF_ROUTE_CODEC_SIDE_STANDARD;
 if(rf_side_online_plan_feed_relmap_create_v1(side,&detached,&identity,input)!=0
  || (seal && rf_side_online_plan_seal_v1(side)!=0)) goto failed;
 return side;
failed:
 rf_side_online_plan_destroy_v1(&side); return NULL;
}
UT_TEST(creation_actual_side_executes_once_and_rechecks_original_pair)
{
 ClusterControlNativeRelmapCreateV2 *creator=NULL; XLogReaderState reader; DecodedXLogRecord decoded;
 RfSideOnlinePlanV1 *side; unsigned syncs,renames;
 UT_ASSERT(owner_setup(&creator,&reader,&decoded));
 side=actual_creation_side(&owner_declaration,&reader,true); UT_ASSERT(side);
 redo_typed.side_plans[0]=side;
 UT_ASSERT(!cluster_cold_typed_relmap_create_ready_v2(&redo_typed,redo_fence));
 UT_ASSERT(owner_apply(&reader,65536)); syncs=copy_syncs; renames=root_renames;
 UT_ASSERT(owner_apply(&reader,65536)
  && cluster_cold_typed_relmap_create_ready_v2(&redo_typed,redo_fence));
 UT_ASSERT(copy_syncs==syncs && root_renames==renames && !redo_failed
  && cold_relmap_create_at(redo_typed.plan,0)->receipt);
 owner_done(&creator); rf_side_online_plan_destroy_v1(&side); UT_ASSERT(!side);
}
UT_TEST(creation_actual_side_unsealed_or_other_declaration_refuses_before_write)
{
 for(unsigned fault=0;fault<2;fault++) {
  ClusterControlNativeRelmapCreateV2 *creator=NULL; XLogReaderState reader; DecodedXLogRecord decoded;
  RfSideOnlinePlanV1 *side; unsigned syncs,renames;
  UT_ASSERT(owner_setup(&creator,&reader,&decoded));
  side=actual_creation_side(&owner_declaration,&reader,fault!=0); UT_ASSERT(side);
  redo_typed.side_plans[0]=side;
  if(fault==1) cold_relmap_create_at(redo_typed.plan,0)->input.creation.batch_uuid[15]^=1;
  syncs=copy_syncs; renames=root_renames;
  UT_ASSERT(!owner_apply(&reader,65536) && redo_failed && redo_typed.plan->relmap_failed
   && copy_syncs==syncs && root_renames==renames && !target_available());
  owner_done(&creator); rf_side_online_plan_destroy_v1(&side);
 }
}
UT_TEST(creation_actual_side_native_byte_drift_preserves_attempt_and_debt)
{
 ClusterControlNativeRelmapCreateV2 *creator=NULL; XLogReaderState reader; DecodedXLogRecord decoded;
 RfSideOnlinePlanV1 *side; unsigned syncs,renames;
 UT_ASSERT(owner_setup(&creator,&reader,&decoded));
 side=actual_creation_side(&owner_declaration,&reader,true); UT_ASSERT(side);
 redo_typed.side_plans[0]=side; wal_bytes[64+128]^=1;
 syncs=copy_syncs; renames=root_renames;
 UT_ASSERT(!owner_apply(&reader,65536) && redo_failed && redo_typed.plan->relmap_failed
  && copy_syncs==syncs && root_renames==renames);
 wal_bytes[64+128]^=1;
 UT_ASSERT(!owner_apply(&reader,65536) && copy_syncs==syncs && root_renames==renames);
 owner_done(&creator); rf_side_online_plan_destroy_v1(&side);
}
UT_TEST(creation_actual_side_ready_refuses_valid_but_changed_files)
{
 ClusterControlNativeRelmapCreateV2 *creator=NULL; XLogReaderState reader; DecodedXLogRecord decoded;
 RfSideOnlinePlanV1 *side; unsigned syncs,renames;
 UT_ASSERT(owner_setup(&creator,&reader,&decoded));
 side=actual_creation_side(&owner_declaration,&reader,true); UT_ASSERT(side);
 redo_typed.side_plans[0]=side;
 if(!owner_apply(&reader,65536)) {
  UT_ASSERT(false); owner_done(&creator); rf_side_online_plan_destroy_v1(&side); return;
 }
 syncs=copy_syncs; renames=root_renames; physical_target_fault(creator,3);
 UT_ASSERT(!cluster_cold_typed_relmap_create_ready_v2(&redo_typed,redo_fence)
  && redo_failed && redo_typed.plan->relmap_failed
  && cold_relmap_create_at(redo_typed.plan,0)->receipt);
 UT_ASSERT(!owner_apply(&reader,65536) && copy_syncs==syncs && root_renames==renames
  && cf_held && copy_x && !held_family_lock && !InterruptHoldoffCount);
 owner_done(&creator); rf_side_online_plan_destroy_v1(&side);
}
