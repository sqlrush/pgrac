/* Actual original heap/history emissions joined to original TX custody. */
static unsigned census_ref_fault,census_ref_calls;
static ClusterRecoveryGraphResult
census_ref_test_find(const ClusterRecoveryReferenceGraph *g,const ClusterRecoveryTxnKey *k,UBA a,ClusterRecoveryGraphDependency *out)
{
 ClusterRecoveryGraphResult r=cluster_recovery_reference_graph_dependency_find(g,k,a,out);
 census_ref_calls++;
 if(census_ref_fault==1) inputs_pin_current=false;
 if(census_ref_fault==2) pg_re_throw();
 if(census_ref_fault==3) cluster_recovery_reference_graph_invalidate((ClusterRecoveryReferenceGraph *)g);
 if(census_ref_fault==4 && r==CLUSTER_RECOVERY_GRAPH_RECORD) out->value.record_segment_generation++;
 return r;
}
UT_TEST(test_census_tx_references_join_all_original_current_and_history_items)
{
 ClusterWalInputsV1 *in; char path[MAXPGPATH]; int fd;
 ClusterRecoveryCensus *c=reference_census_fixture(&in,&fd,path);
 const ClusterRecoveryCensusReferencesV1 *refs; unsigned tx=0,history=0,items=0,free_itl=0;
 census_ref_fault=census_ref_calls=0;
 UT_ASSERT_EQ(owned_census_until(c,0),CLUSTER_RECOVERY_CENSUS_DRAINED);
 refs=cluster_recovery_census_references_v1(c); UT_ASSERT_NOT_NULL(refs);
 if(refs!=NULL) for(uint32 i=0;i<=refs->count;i++) {
  ClusterRecoveryCensusTxReferenceV1 got=cluster_recovery_census_tx_reference_v1(c,i);
  UT_ASSERT(got.observed); UT_ASSERT_EQ(got.index,i); UT_ASSERT_EQ(got.count,refs->count);
  if(i==refs->count) { UT_ASSERT(got.reference==NULL && got.transaction.value.record==NULL); break; }
  UT_ASSERT(got.reference==&refs->entries[i]);
  if(!got.observed || got.reference==NULL) break;
  history+=got.reference->version!=0;
  if(got.reference->entry.kind==CLUSTER_RECOVERY_HEAP_ITEM) items++;
  if(got.reference->entry.kind==CLUSTER_RECOVERY_HEAP_ITL && got.reference->entry.itl.flags==ITL_FLAG_FREE) free_itl++;
  if(got.reference->transaction.record!=NULL) {
   tx++; UT_ASSERT(got.transaction.value.record==got.reference->transaction.record);
   UT_ASSERT_EQ(got.transaction.value.record_segment_generation,got.reference->transaction.record_segment_generation);
   UT_ASSERT_EQ(got.transaction.value.transaction.xid,got.reference->entry.itl.xid);
   UT_ASSERT(cluster_undo_record_uba_equal(got.transaction.value.record->address,got.reference->entry.itl.undo_segment_head));
  } else UT_ASSERT(got.transaction.value.record==NULL);
 }
 UT_ASSERT(tx!=0 && history!=0 && items!=0 && free_itl!=0);
 UT_ASSERT_EQ(census_ref_calls,tx); UT_ASSERT(inputs_pin_held);
 owned_census_done(c,&in,fd,path);
}
UT_TEST(test_census_tx_references_prefix_empty_lock_and_range_are_distinct)
{
 ClusterWalInputsV1 *in; char path[MAXPGPATH]; int fd;
 ClusterRecoveryCensus *c=reference_census_fixture(&in,&fd,path);
 ClusterRecoveryCensusTxReferenceV1 got,z={0}; census_ref_calls=census_ref_fault=0;
 got=cluster_recovery_census_tx_reference_v1(c,0); UT_ASSERT(memcmp(&got,&z,sizeof(got))==0);
 UT_ASSERT_EQ(owned_census_until(c,0),CLUSTER_RECOVERY_CENSUS_DRAINED);
 for(unsigned mode=0;mode<4;mode++) {
  if(mode==0) inventory_graph_caller_lock=true;
  if(mode==1) CritSectionCount=1;
  if(mode==2) ShutdownRequestPending=true;
  got=cluster_recovery_census_tx_reference_v1(c,mode==3?c->reference_count+1:0);
  inventory_graph_caller_lock=false; CritSectionCount=0; ShutdownRequestPending=false;
  UT_ASSERT(memcmp(&got,&z,sizeof(got))==0); UT_ASSERT_EQ(census_ref_calls,0);
 }
 UT_ASSERT_EQ(c->status,CLUSTER_RECOVERY_CENSUS_DRAINED); owned_census_done(c,&in,fd,path);
 c=owned_census_fixture(&in,&fd,path,false);
 UT_ASSERT_EQ(owned_census_until(c,0),CLUSTER_RECOVERY_CENSUS_DRAINED);
 got=cluster_recovery_census_tx_reference_v1(c,0);
 UT_ASSERT(got.observed && got.count==0 && got.index==0 && got.reference==NULL && got.transaction.value.record==NULL);
 owned_census_done(c,&in,fd,path);
}
UT_TEST(test_census_tx_references_late_source_graph_generation_and_error_are_sticky)
{
 for(unsigned fault=1;fault<=4;fault++) {
  ClusterWalInputsV1 *in; char path[MAXPGPATH]; int fd; uint32 at=UINT32_MAX;
  ClusterRecoveryCensus *c=reference_census_fixture(&in,&fd,path);
  ClusterRecoveryCensusTxReferenceV1 got={0},z={0}; volatile bool caught=false;
  census_ref_fault=census_ref_calls=0; UT_ASSERT_EQ(owned_census_until(c,0),CLUSTER_RECOVERY_CENSUS_DRAINED);
  for(uint32 i=0;i<c->reference_count;i++) if(c->references->entries[i].transaction.record!=NULL) {at=i;break;}
  UT_ASSERT(at!=UINT32_MAX); census_ref_fault=fault;
  PG_TRY(); { got=cluster_recovery_census_tx_reference_v1(c,at); }
  PG_CATCH(); { caught=true; } PG_END_TRY();
  UT_ASSERT(caught==(fault==2)); UT_ASSERT(memcmp(&got,&z,sizeof(got))==0);
  UT_ASSERT_EQ(c->status,CLUSTER_RECOVERY_CENSUS_REFUSED); UT_ASSERT(inputs_pin_held);
  census_ref_fault=0; inputs_pin_current=true;
  got=cluster_recovery_census_tx_reference_v1(c,at); UT_ASSERT(!got.observed);
  UT_ASSERT_EQ(census_ref_calls,1); owned_census_done(c,&in,fd,path);
 }
}
