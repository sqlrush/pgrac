/*-------------------------------------------------------------------------
 * test_cluster_relmap_create_scan.inc
 *    Registered creation through original scanning, decoding and sealed SIDE.
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_relmap_create_scan.inc
 * NOTES
 *    ROOT visitation is controlled; scanner, decoder, SIDE and pair I/O are real.
 *-------------------------------------------------------------------------
 */
#include "cluster/cluster_wal_tail.h"
#include "catalog/pg_control.h"
#include "cluster/cluster_block_apply.h"

/* This component has no PAGE effects or pending cancellation. Any attempt
 * to borrow a PAGE executor/version predicate is an unexpected route. */
extern volatile sig_atomic_t create_scan_interrupt_pending;
void create_scan_interrupts(void);
volatile sig_atomic_t create_scan_interrupt_pending;
void create_scan_interrupts(void) { abort(); }
ClusterBlkApplyResult cluster_block_apply_one(XLogReaderState *reader, uint8 block_id, char *page)
{ (void)reader; (void)block_id; (void)page; abort(); }
bool rf_page_version_present_v1(const RfPageVersionV1 *version)
{ (void)version; abort(); }

ClusterControlRootResult create_scan_root_checkpoint(const ClusterControlRootSnapshot *,
 const ClusterControlRootReadToken *, ClusterWalRecoveryCheckpointV1 *);
ClusterControlRootResult create_scan_root_checkpoint_deadline(const ClusterControlRootSnapshot *,
 const ClusterControlRootReadToken *, ClusterWalRecoveryCheckpointV1 *, uint64);
ClusterControlRootResult create_scan_root_visit(const ClusterControlRootSnapshot *,
 const ClusterControlRootReadToken *, ClusterWalRecordVisitor, void *, ClusterWalTailObservation *);
ClusterControlRootResult create_scan_root_visit_deadline(const ClusterControlRootSnapshot *,
 const ClusterControlRootReadToken *, ClusterWalRecordVisitor, void *, ClusterWalTailObservation *, uint64);

static ClusterControlRootSnapshot scan_root;
static ClusterControlRootReadToken scan_token;
static ClusterWalRecoveryCheckpointV1 scan_checkpoint;
static XLogReaderState scan_checkpoint_reader, scan_duplicate_reader;
static DecodedXLogRecord scan_checkpoint_record, scan_duplicate_record;
static unsigned scan_mode;

ClusterControlRootResult create_scan_root_checkpoint(const ClusterControlRootSnapshot *expected,
 const ClusterControlRootReadToken *token, ClusterWalRecoveryCheckpointV1 *out)
{
 if(expected!=&scan_root || token!=&scan_token || held_family_lock) abort();
 *out=scan_checkpoint; return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}
ClusterControlRootResult create_scan_root_checkpoint_deadline(const ClusterControlRootSnapshot *expected,
 const ClusterControlRootReadToken *token, ClusterWalRecoveryCheckpointV1 *out, uint64 deadline)
{
 (void)deadline; return create_scan_root_checkpoint(expected,token,out);
}
ClusterControlRootResult create_scan_root_visit(const ClusterControlRootSnapshot *expected,
 const ClusterControlRootReadToken *token, ClusterWalRecordVisitor visit, void *arg,
 ClusterWalTailObservation *out)
{
 XLogReaderState *records[3]={&scan_checkpoint_reader,owner_reader,&scan_duplicate_reader};
 unsigned count=scan_mode==2 ? 3 : 2;
 if(expected!=&scan_root || token!=&scan_token || held_family_lock) abort();
 memset(out,0,sizeof(*out));
 for(unsigned i=0;i<count;i++) {
  if(!visit(records[i],arg)) return CLUSTER_CONTROL_ROOT_POSTREAD_FAILED;
  ++out->records; out->complete_end=records[i]->EndRecPtr;
 }
 return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}
ClusterControlRootResult create_scan_root_visit_deadline(const ClusterControlRootSnapshot *expected,
 const ClusterControlRootReadToken *token, ClusterWalRecordVisitor visit, void *arg,
 ClusterWalTailObservation *out, uint64 deadline)
{
 (void)deadline; return create_scan_root_visit(expected,token,visit,arg,out);
}

static bool creation_scan_setup(ClusterControlNativeRelmapCreateV2 **creator,
 XLogReaderState *reader, DecodedXLogRecord *decoded, RfSideOnlinePlanV1 **side,
 unsigned mode, bool wrong_batch)
{
 RfContributorStreamCutV1 cut={0}; RfSideOnlinePlanRequestV1 request={0};
 if(!owner_setup(creator,reader,decoded)) return false;
 /* Drop the older manually fed plan. The plan under test receives no record
  * except through the original ROOT visitor below. Expected is still copied
  * from the independent control reservation, never from decoded WAL. */
 cluster_cold_plan_destroy_v1(&redo_typed.plan);
 scan_mode=mode;
 memset(&scan_root,0,sizeof(scan_root)); memset(&scan_token,0,sizeof(scan_token));
 memset(&scan_checkpoint,0,sizeof(scan_checkpoint));
 scan_root.identity=redo_source.claim.identity;
 scan_root.checkpoint_tli=redo_source.timeline;
 scan_root.checkpoint_lower_lsn=reader->ReadRecPtr-0x100;
 scan_root.validated_tail_lsn_exclusive=reader->EndRecPtr;
 scan_checkpoint.source=redo_source;
 scan_checkpoint.checkpoint_start=scan_root.checkpoint_lower_lsn;
 scan_checkpoint.checkpoint.redo=scan_root.checkpoint_lower_lsn;
 scan_checkpoint.checkpoint.ThisTimeLineID=redo_source.timeline;
 scan_checkpoint.checkpoint.PrevTimeLineID=redo_source.timeline;
 scan_checkpoint.checkpoint.nextXid=FullTransactionIdFromU64(100);
 scan_checkpoint_reader=*reader;
 memset(&scan_checkpoint_record,0,sizeof(scan_checkpoint_record));
 scan_checkpoint_reader.record=&scan_checkpoint_record;
 scan_checkpoint_reader.ReadRecPtr=scan_checkpoint.checkpoint_start;
 scan_checkpoint_reader.EndRecPtr=reader->ReadRecPtr;
 scan_checkpoint_record.size=sizeof(scan_checkpoint_record);
 scan_checkpoint_record.lsn=scan_checkpoint_reader.ReadRecPtr;
 scan_checkpoint_record.next_lsn=scan_checkpoint_reader.EndRecPtr;
 scan_checkpoint_record.max_block_id=-1;
 scan_checkpoint_record.header.xl_rmid=RM_XLOG_ID;
 scan_checkpoint_record.header.xl_info=XLOG_CHECKPOINT_ONLINE;
 scan_checkpoint_record.header.xl_crc=0x12345678;
 scan_checkpoint_record.main_data=(char *)&scan_checkpoint.checkpoint;
 scan_checkpoint_record.main_data_len=sizeof(CheckPoint);
 decoded->header.xl_prev=scan_checkpoint_reader.ReadRecPtr;
 decoded->header.xl_scn=10;
 if(mode==1) { /* Same complete cut, but the declared RMC1 is absent. */
  decoded->header.xl_rmid=RM_XLOG_ID; decoded->header.xl_info=XLOG_NOOP;
  decoded->main_data=NULL; decoded->main_data_len=0;
 }
 if(mode==2) {
  scan_duplicate_reader=*reader; scan_duplicate_record=*decoded;
  scan_duplicate_reader.record=&scan_duplicate_record;
  scan_duplicate_reader.ReadRecPtr=reader->EndRecPtr;
  scan_duplicate_reader.EndRecPtr=reader->EndRecPtr+0xa8;
  scan_duplicate_record.lsn=scan_duplicate_reader.ReadRecPtr;
  scan_duplicate_record.next_lsn=scan_duplicate_reader.EndRecPtr;
  scan_duplicate_record.header.xl_prev=reader->ReadRecPtr;
  scan_duplicate_record.header.xl_crc++;
  scan_root.validated_tail_lsn_exclusive=scan_duplicate_reader.EndRecPtr;
 }
 owner_declaration.cut.physical_lower=scan_root.checkpoint_lower_lsn;
 owner_declaration.cut.native_redo=scan_checkpoint.checkpoint.redo;
 owner_declaration.cut.tail_end=scan_root.validated_tail_lsn_exclusive;
 redo_typed.participants[0]=owner_declaration.cut;
 if(wrong_batch) owner_expected.batch_uuid[15]^=1;
 if(cluster_cold_plan_create_v1(&owner_declaration.cut,1,4*1024*1024,&redo_typed.plan)!=0
  || cluster_cold_plan_relmap_create_predeclare_v1(redo_typed.plan,&owner_expected,1)!=0) return false;
 cut.failed_thread=owner_declaration.cut.thread_id;
 cut.timeline_id=owner_declaration.cut.timeline;
 cut.origin_owner_incarnation=owner_declaration.cut.owner_incarnation;
 cut.scan_begin_inclusive=owner_declaration.cut.physical_lower;
 cut.scan_end_exclusive=owner_declaration.cut.tail_end;
 cut.flags=RF_CONTRIBUTOR_CUT_COMPLETE;
 request.system_identifier=redo_source.claim.identity.system_identifier;
 memcpy(request.storage_uuid,redo_source.claim.identity.storage_uuid,16);
 request.physical_cuts=&cut; request.participant_count=1; request.memory_budget=1048576;
 request.redo_starts=&owner_declaration.cut.native_redo; request.purpose=RF_SIDE_PLAN_RECOVERY;
 if(rf_side_online_plan_create_v1(&request,side)!=0
  || !rf_side_online_plan_bind_database_v1(*side,redo_source.claim.database_incarnation)
  || !rf_side_online_plan_bind_sources_v1(*side,&redo_source,1)) return false;
 redo_typed.side_plans[0]=*side;
 return true;
}
static ClusterColdDetailV1 creation_scan_run(RfSideOnlinePlanV1 *side, ClusterColdScanResultV1 *result)
{
 return cluster_cold_scan_root_v1(redo_typed.plan,0,&scan_root,&scan_token,true,false,side,result);
}
static bool creation_scan_seal(RfSideOnlinePlanV1 *side)
{
 return rf_side_online_plan_seal_v1(side)==0
  && cluster_cold_plan_seal_v1(redo_typed.plan,owner_no_pages,NULL,NULL)==0
  && cluster_cold_plan_step_count_v1(redo_typed.plan)==1
  && cluster_cold_plan_step_v1(redo_typed.plan,0,&owner_step);
}
UT_TEST(creation_scan_real_decoder_and_side_execute_once_then_read_only)
{
 ClusterControlNativeRelmapCreateV2 *creator=NULL; XLogReaderState reader; DecodedXLogRecord decoded;
 RfSideOnlinePlanV1 *side=NULL; ClusterColdScanResultV1 result; unsigned syncs,renames;
 UT_ASSERT(creation_scan_setup(&creator,&reader,&decoded,&side,0,false));
 UT_ASSERT_EQ(creation_scan_run(side,&result),CLUSTER_COLD_OK);
 UT_ASSERT_EQ(result.records,2);
 if(ut_current_failed) { owner_done(&creator); rf_side_online_plan_destroy_v1(&side); return; }
 UT_ASSERT(creation_scan_seal(side));
 UT_ASSERT(!copy_syncs && !target_available() && root_renames==2);
 UT_ASSERT(owner_apply(&reader,65536));
 syncs=copy_syncs; renames=root_renames;
 UT_ASSERT(target_available() && syncs>0 && renames==4);
 UT_ASSERT(owner_apply(&reader,65536));
 UT_ASSERT(cluster_cold_typed_relmap_create_ready_v2(&redo_typed,redo_fence));
 UT_ASSERT(copy_syncs==syncs && root_renames==renames && !redo_failed);
 owner_done(&creator); rf_side_online_plan_destroy_v1(&side);
}
UT_TEST(creation_scan_wrong_registration_or_native_bytes_never_write_target)
{
 for(unsigned fault=0;fault<2;fault++) {
  ClusterControlNativeRelmapCreateV2 *creator=NULL; XLogReaderState reader; DecodedXLogRecord decoded;
  RfSideOnlinePlanV1 *side=NULL; ClusterColdScanResultV1 result;
  UT_ASSERT(creation_scan_setup(&creator,&reader,&decoded,&side,0,fault==0));
  if(fault==1) wal_bytes[0]^=1;
  UT_ASSERT_NE(creation_scan_run(side,&result),CLUSTER_COLD_OK);
  UT_ASSERT(!copy_syncs && root_renames==2 && !target_available());
  UT_ASSERT_NE(cluster_cold_plan_seal_v1(redo_typed.plan,owner_no_pages,NULL,NULL),CLUSTER_COLD_OK);
  UT_ASSERT(cf_held && copy_x && !held_family_lock && !InterruptHoldoffCount);
  owner_done(&creator); rf_side_online_plan_destroy_v1(&side);
 }
}
UT_TEST(creation_scan_missing_and_duplicate_declarations_cannot_seal)
{
 for(unsigned mode=1;mode<=2;mode++) {
  ClusterControlNativeRelmapCreateV2 *creator=NULL; XLogReaderState reader; DecodedXLogRecord decoded;
  RfSideOnlinePlanV1 *side=NULL; ClusterColdScanResultV1 result; ClusterColdDetailV1 detail;
  UT_ASSERT(creation_scan_setup(&creator,&reader,&decoded,&side,mode,false));
  detail=creation_scan_run(side,&result);
  if(mode==1) UT_ASSERT_EQ(detail,CLUSTER_COLD_OK);
  else UT_ASSERT_NE(detail,CLUSTER_COLD_OK);
  UT_ASSERT_NE(cluster_cold_plan_seal_v1(redo_typed.plan,owner_no_pages,NULL,NULL),CLUSTER_COLD_OK);
  UT_ASSERT(!copy_syncs && root_renames==2 && !target_available() && cf_held && copy_x);
  owner_done(&creator); rf_side_online_plan_destroy_v1(&side);
 }
}
UT_TEST(creation_scan_late_pair_change_keeps_original_receipt_and_debt)
{
 ClusterControlNativeRelmapCreateV2 *creator=NULL; XLogReaderState reader; DecodedXLogRecord decoded;
 RfSideOnlinePlanV1 *side=NULL; ClusterColdScanResultV1 result; unsigned syncs,renames;
 UT_ASSERT(creation_scan_setup(&creator,&reader,&decoded,&side,0,false));
 UT_ASSERT_EQ(creation_scan_run(side,&result),CLUSTER_COLD_OK);
 UT_ASSERT(creation_scan_seal(side));
 if(ut_current_failed) { owner_done(&creator); rf_side_online_plan_destroy_v1(&side); return; }
 UT_ASSERT(owner_apply(&reader,65536));
 if(ut_current_failed) { owner_done(&creator); rf_side_online_plan_destroy_v1(&side); return; }
 syncs=copy_syncs; renames=root_renames;
 physical_target_fault(creator,3);
 UT_ASSERT(!cluster_cold_typed_relmap_create_ready_v2(&redo_typed,redo_fence));
 UT_ASSERT(redo_failed && redo_typed.plan->relmap_failed
  && cold_relmap_create_at(redo_typed.plan,0)->receipt && cf_held && copy_x);
 UT_ASSERT(!owner_apply(&reader,65536) && copy_syncs==syncs && root_renames==renames);
 owner_done(&creator); rf_side_online_plan_destroy_v1(&side);
}
