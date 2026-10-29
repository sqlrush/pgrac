/* Same original Start/work and full ALL IR-WALR owner boundary. */
static unsigned start_dependency_fault,start_dependency_calls;
ClusterRecoveryCensusTxDependencyV1
cluster_recovery_census_tx_dependency_v1(ClusterRecoveryCensus *c,uint32 i)
{
 ClusterRecoveryCensusTxDependencyV1 d={0};
 if(c!=(ClusterRecoveryCensus *)&observation_cursor_object || !observation_cursor_live || !observation_scope_live) abort();
 start_dependency_calls++;
 if(start_dependency_fault==1) return d;
 if(start_dependency_fault==2) pg_re_throw();
 d.observed=true; d.value.count=observation_manifest.tx_record_count;
 d.value.root_count=d.value.count==0?0:2; d.value.index=i;
 d.value.root=i<d.value.root_count;
 if(i<d.value.count) d.value.value.record=&start_tx_record_bytes;
 if(i==0 && d.value.count!=0) {
  d.value.predecessor_index=2; d.value.predecessor.record=&start_tx_record_bytes;
  d.value.value.record_segment_generation=17; d.value.predecessor.record_segment_generation=41;
 }
 if(start_dependency_fault==3) d.value.count++;
 if(start_dependency_fault==4) owner_boot++;
 if(start_dependency_fault==5) d.value.value.record=NULL;
 if(start_dependency_fault==6) observation_manifest.inputs_sha256[31]++;
 if(start_dependency_fault==7) d.value.index++;
 if(start_dependency_fault==8) d.value.root_count=d.value.count+1;
 if(start_dependency_fault==9) d.value.root=!d.value.root;
 if(start_dependency_fault==10) d.value.predecessor_index=d.value.count;
 return d;
}
static void start_dependencies_ready(uint32 n)
{
 start_tx_records_ready(n); start_dependency_fault=start_dependency_calls=0;
}
UT_TEST(test_start_tx_dependencies_keep_exact_original_roots_edge_and_empty_end)
{
 for(uint32 count=0;count<=3;count+=3) {
  start_dependencies_ready(count);
  for(uint32 i=0;i<=count;i++) {
   ClusterRecoveryCensusTxDependencyV1 d=ClusterColdRecoveryCtrcTxDependency(info.cluster_tx_start,&native_ctrc_work,i);
   UT_ASSERT(d.observed); UT_ASSERT_EQ(d.value.count,count); UT_ASSERT_EQ(d.value.index,i);
   if(i<count) UT_ASSERT(d.value.value.record==&start_tx_record_bytes);
   else UT_ASSERT(d.value.value.record==NULL && d.value.predecessor.record==NULL);
   if(i==0 && count!=0) {
    UT_ASSERT(d.value.root); UT_ASSERT_EQ(d.value.root_count,2);
    UT_ASSERT_EQ(d.value.predecessor_index,2); UT_ASSERT_EQ(d.value.predecessor.record_segment_generation,41);
   }
  }
  UT_ASSERT_EQ(observation_scope_begins,1); UT_ASSERT_EQ(observation_census_begins,1);
  UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation,0);
  ClusterReleaseColdRecoveryInputs(&info);
 }
}
UT_TEST(test_start_tx_dependencies_do_not_enter_with_other_work_lock_or_range)
{
 ClusterRecoveryCensusTxDependencyV1 d,z={0}; ClusterColdCtrcWorkV1 other;
 start_dependencies_ready(3); other=native_ctrc_work;
 d=ClusterColdRecoveryCtrcTxDependency(NULL,&native_ctrc_work,0); UT_ASSERT(memcmp(&d,&z,sizeof(d))==0);
 d=ClusterColdRecoveryCtrcTxDependency(info.cluster_tx_start,&other,0); UT_ASSERT(memcmp(&d,&z,sizeof(d))==0);
 CritSectionCount=1; d=ClusterColdRecoveryCtrcTxDependency(info.cluster_tx_start,&native_ctrc_work,0); CritSectionCount=0;
 UT_ASSERT(memcmp(&d,&z,sizeof(d))==0);
 d=ClusterColdRecoveryCtrcTxDependency(info.cluster_tx_start,&native_ctrc_work,4); UT_ASSERT(memcmp(&d,&z,sizeof(d))==0);
 UT_ASSERT_EQ(start_dependency_calls,0); UT_ASSERT(observation_cursor_live && observation_scope_live);
 ClusterReleaseColdRecoveryInputs(&info);
}
UT_TEST(test_start_tx_dependencies_bad_shape_late_owner_and_source_are_sticky)
{
 const unsigned faults[]={1,3,4,5,6,7,8,9,10};
 for(unsigned k=0;k<lengthof(faults);k++) {
  ClusterRecoveryCensusTxDependencyV1 d,z={0};
  start_dependencies_ready(3); start_dependency_fault=faults[k];
  d=ClusterColdRecoveryCtrcTxDependency(info.cluster_tx_start,&native_ctrc_work,0);
  UT_ASSERT(memcmp(&d,&z,sizeof(d))==0); UT_ASSERT_EQ(start_dependency_calls,1);
  UT_ASSERT(!observation_cursor_live && !observation_scope_live);
  UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation,0);
  start_dependency_fault=0; d=ClusterColdRecoveryCtrcTxDependency(info.cluster_tx_start,&native_ctrc_work,0);
  UT_ASSERT(!d.observed); UT_ASSERT_EQ(start_dependency_calls,1);
  if(faults[k]==4) { owner_boot--; release_callback(RESOURCE_RELEASE_BEFORE_LOCKS,false,true,NULL); }
  ClusterReleaseColdRecoveryInputs(&info);
 }
}
UT_TEST(test_start_tx_dependencies_error_retires_original_children_in_reverse)
{
 volatile bool caught=false; start_dependencies_ready(3); start_dependency_fault=2;
 PG_TRY(); { (void)ClusterColdRecoveryCtrcTxDependency(info.cluster_tx_start,&native_ctrc_work,0); }
 PG_CATCH(); { caught=true; } PG_END_TRY();
 UT_ASSERT(caught); UT_ASSERT(!observation_cursor_live && !observation_scope_live);
 UT_ASSERT(strstr(observation_events,"DR")!=NULL);
 UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation,0);
 start_dependency_fault=0; ClusterReleaseColdRecoveryInputs(&info);
}
