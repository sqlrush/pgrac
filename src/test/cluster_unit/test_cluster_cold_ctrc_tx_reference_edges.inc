/* Actual Start with controlled original lower adjacency view only. */
static unsigned start_ref_fault,start_ref_calls;
static bool start_ref_complete;
/* NULL preserves every original fixture's synthetic first transaction. */
static bool (*start_ref_first_transaction)(const ClusterRecoveryCensusReferenceV1 *);
ClusterRecoveryCensusTxReferenceV1
cluster_recovery_census_tx_reference_v1(ClusterRecoveryCensus *c,uint32 i)
{
 ClusterRecoveryCensusTxReferenceV1 d={0};
 ClusterRecoveryCensusReferencesV1 *refs=(ClusterRecoveryCensusReferencesV1 *)observation_references_storage[0].bytes;
 if(c!=(ClusterRecoveryCensus *)&observation_cursor_object || !observation_cursor_live || !observation_scope_live) abort();
 start_ref_calls++; if(start_ref_fault==1) return d; if(start_ref_fault==2) pg_re_throw();
 d.observed=true;d.count=refs->count;d.index=i;
 if(i<refs->count) {
  d.reference=&refs->entries[i];
  if((i==0 && (start_ref_first_transaction==NULL || start_ref_first_transaction(&refs->entries[i])))
      || (start_ref_complete && refs->entries[i].entry.kind==CLUSTER_RECOVERY_HEAP_ITL
      && refs->entries[i].entry.itl.flags!=ITL_FLAG_FREE)) {
   d.transaction.count=3;d.transaction.root_count=2;d.transaction.index=0;d.transaction.root=true;
   d.transaction.value=refs->entries[i].transaction;
   d.transaction.predecessor_index=2;d.transaction.predecessor.record=&start_tx_record_bytes;
   if(start_ref_complete) {
    d.transaction.count=observation_manifest.tx_record_count;
    d.transaction.root_count=Min(2,d.transaction.count);d.transaction.root=d.transaction.root_count!=0;
    d.transaction.predecessor_index=0;d.transaction.predecessor.record=NULL;
   }
  }
 }
 if(start_ref_fault==3) d.count++;
 if(start_ref_fault==4) owner_boot++;
 if(start_ref_fault==5) d.reference=NULL;
 if(start_ref_fault==6) observation_manifest.inputs_sha256[31]++;
 if(start_ref_fault==7) d.index++;
 if(start_ref_fault==8) d.reference=(const ClusterRecoveryCensusReferenceV1 *)(uintptr_t)1;
 if(start_ref_fault==9) d.transaction.value.record=NULL;
 if(start_ref_fault==10) d.transaction.index=d.transaction.count;
 return d;
}
static void start_ref_ready(uint32 count)
{
 ClusterRecoveryCensusReferencesV1 *refs;
 start_tx_records_ready(3);start_ref_calls=start_ref_fault=0;start_ref_complete=false;
 refs=(ClusterRecoveryCensusReferencesV1 *)observation_references_storage[0].bytes;
 refs->count=count;
 if(count!=0) {
  refs->entries[0].entry.kind=CLUSTER_RECOVERY_HEAP_ITL;
  refs->entries[0].entry.itl.flags=ITL_FLAG_ACTIVE;
  refs->entries[0].transaction.record=&start_tx_record_bytes;
  refs->entries[0].transaction.record_segment_generation=19;
  refs->entries[1].entry.kind=CLUSTER_RECOVERY_HEAP_ITL;refs->entries[1].entry.itl.flags=ITL_FLAG_FREE;
  refs->entries[2].entry.kind=CLUSTER_RECOVERY_HEAP_ITEM;refs->entries[2].version=1;
 }
}
UT_TEST(test_start_tx_references_preserve_exact_member_itl_free_tuple_and_end)
{
 for(uint32 n=0;n<=3;n+=3) {
  start_ref_ready(n);
  for(uint32 i=0;i<=n;i++) {
   ClusterRecoveryCensusTxReferenceV1 d=ClusterColdRecoveryCtrcTxReference(info.cluster_tx_start,&native_ctrc_work,i);
   UT_ASSERT(d.observed);UT_ASSERT_EQ(d.count,n);UT_ASSERT_EQ(d.index,i);
   if(i==0 && n!=0) {UT_ASSERT(d.transaction.root);UT_ASSERT_EQ(d.transaction.value.record_segment_generation,19);}
   if(i==n) UT_ASSERT(d.reference==NULL);
   else UT_ASSERT(d.reference==&info.cluster_tx_start->ctrc_references->entries[i]);
  }
  UT_ASSERT_EQ(observation_census_begins,1);UT_ASSERT_EQ(observation_scope_begins,1);
  UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation,0);
  ClusterReleaseColdRecoveryInputs(&info);
 }
}
UT_TEST(test_start_tx_references_preconditions_do_not_enter)
{
 ClusterRecoveryCensusTxReferenceV1 d,z={0};ClusterColdCtrcWorkV1 other;
 start_ref_ready(3);other=native_ctrc_work;
 d=ClusterColdRecoveryCtrcTxReference(NULL,&native_ctrc_work,0);UT_ASSERT(memcmp(&d,&z,sizeof(d))==0);
 d=ClusterColdRecoveryCtrcTxReference(info.cluster_tx_start,&other,0);UT_ASSERT(memcmp(&d,&z,sizeof(d))==0);
 CritSectionCount=1;d=ClusterColdRecoveryCtrcTxReference(info.cluster_tx_start,&native_ctrc_work,0);CritSectionCount=0;
 UT_ASSERT(memcmp(&d,&z,sizeof(d))==0);
 d=ClusterColdRecoveryCtrcTxReference(info.cluster_tx_start,&native_ctrc_work,4);UT_ASSERT(memcmp(&d,&z,sizeof(d))==0);
 UT_ASSERT_EQ(start_ref_calls,0);UT_ASSERT(observation_cursor_live && observation_scope_live);
 ClusterReleaseColdRecoveryInputs(&info);
}
UT_TEST(test_start_tx_references_late_owner_source_shape_and_pointer_are_sticky)
{
 const unsigned faults[]={1,3,4,5,6,7,8,9,10};
 for(unsigned k=0;k<lengthof(faults);k++) {
  ClusterRecoveryCensusTxReferenceV1 d,z={0};start_ref_ready(3);start_ref_fault=faults[k];
  d=ClusterColdRecoveryCtrcTxReference(info.cluster_tx_start,&native_ctrc_work,0);
  UT_ASSERT(memcmp(&d,&z,sizeof(d))==0);UT_ASSERT_EQ(start_ref_calls,1);
  UT_ASSERT(!observation_cursor_live && !observation_scope_live);
  start_ref_fault=0;d=ClusterColdRecoveryCtrcTxReference(info.cluster_tx_start,&native_ctrc_work,0);
  UT_ASSERT(!d.observed);UT_ASSERT_EQ(start_ref_calls,1);
  if(faults[k]==4) {owner_boot--;release_callback(RESOURCE_RELEASE_BEFORE_LOCKS,false,true,NULL);}
  ClusterReleaseColdRecoveryInputs(&info);
 }
}
UT_TEST(test_start_tx_references_error_retains_original_debt_and_reverse_cleanup)
{
 volatile bool caught=false;start_ref_ready(3);start_ref_fault=2;
 PG_TRY();{(void)ClusterColdRecoveryCtrcTxReference(info.cluster_tx_start,&native_ctrc_work,0);}
 PG_CATCH();{caught=true;}PG_END_TRY();
 UT_ASSERT(caught);UT_ASSERT(!observation_cursor_live && !observation_scope_live);
 UT_ASSERT(strstr(observation_events,"DR")!=NULL);UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation,0);
 start_ref_fault=0;ClusterReleaseColdRecoveryInputs(&info);
}
