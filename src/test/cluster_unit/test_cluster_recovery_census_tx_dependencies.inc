/* Same original native census and physically reread TX forest. */
static unsigned census_dependency_fault,census_dependency_calls;
static ClusterRecoveryGraphResult
census_dependency_test_at(const ClusterRecoveryReferenceGraph *g,uint32 i,ClusterRecoveryGraphDependency *d)
{
 ClusterRecoveryGraphResult r=cluster_recovery_reference_graph_dependency_at(g,i,d);
 census_dependency_calls++;
 if(census_dependency_fault==1) inputs_pin_current=false;
 if(census_dependency_fault==2) pg_re_throw();
 if(census_dependency_fault==3) cluster_recovery_reference_graph_invalidate(census_tx_owner->graph);
 if(census_dependency_fault==4) census_tx_owner->manifest.tx_record_count++;
 return r;
}
UT_TEST(test_census_tx_dependencies_construct_original_roots_and_cross_relation_edge)
{
 ClusterWalInputsV1 *in; char path[MAXPGPATH]; int fd;
 ClusterRecoveryCensus *c=census_tx_setup(&in,&fd,path,false);
 ClusterRecoveryCensusTxDependencyV1 d; unsigned roots=0,edges=0,children=0;
 census_dependency_fault=census_dependency_calls=0;
 census_tx_add_predecessor(fd,&cluster_wal_inputs_at_v1(in,0)->source);
 UT_ASSERT_EQ(owned_census_until(c,0),CLUSTER_RECOVERY_CENSUS_DRAINED);
 for(uint32 i=0;i<3;i++) {
  d=cluster_recovery_census_tx_dependency_v1(c,i); UT_ASSERT(d.observed);
  if(!d.observed) continue;
  UT_ASSERT_EQ(d.value.count,3); UT_ASSERT_EQ(d.value.root_count,2); UT_ASSERT_EQ(d.value.index,i);
  if(d.value.root) roots++;
  if(d.value.predecessor.record!=NULL) {
   edges++; UT_ASSERT_EQ(d.value.value.transaction.segment_id,1);
   UT_ASSERT_EQ(d.value.predecessor.record_segment_generation,17);
   UT_ASSERT_EQ(d.value.predecessor.record->header.target_locator.relNumber,20001);
   UT_ASSERT(cluster_undo_record_uba_equal(d.value.value.record->header.prev_uba,d.value.predecessor.record->address));
  } else if(!d.value.root) children++;
 }
 UT_ASSERT_EQ(roots,2); UT_ASSERT_EQ(edges,1); UT_ASSERT_EQ(children,1);
 d=cluster_recovery_census_tx_dependency_v1(c,3);
 UT_ASSERT(d.observed && d.value.value.record==NULL); UT_ASSERT_EQ(d.value.count,3);
 UT_ASSERT(inputs_pin_held); census_tx_done(c,&in,fd,path);
 c=census_tx_setup(&in,&fd,path,true);
 UT_ASSERT_EQ(owned_census_until(c,0),CLUSTER_RECOVERY_CENSUS_DRAINED);
 d=cluster_recovery_census_tx_dependency_v1(c,0);
 UT_ASSERT(d.observed && d.value.count==0 && d.value.root_count==0);
 census_tx_done(c,&in,fd,path);
}
UT_TEST(test_census_tx_dependencies_prefix_lock_and_range_do_not_enter)
{
 ClusterWalInputsV1 *in; char path[MAXPGPATH]; int fd;
 ClusterRecoveryCensus *c=census_tx_setup(&in,&fd,path,false);
 ClusterRecoveryCensusTxDependencyV1 d,z={0};
 census_dependency_fault=census_dependency_calls=0;
 d=cluster_recovery_census_tx_dependency_v1(c,0); UT_ASSERT(memcmp(&d,&z,sizeof(d))==0);
 UT_ASSERT_EQ(owned_census_until(c,0),CLUSTER_RECOVERY_CENSUS_DRAINED);
 for(unsigned mode=0;mode<4;mode++) {
  if(mode==0) inventory_graph_caller_lock=true;
  if(mode==1) CritSectionCount=1;
  if(mode==2) ShutdownRequestPending=true;
  d=cluster_recovery_census_tx_dependency_v1(c,mode==3?3:0);
  inventory_graph_caller_lock=false; CritSectionCount=0; ShutdownRequestPending=false;
  UT_ASSERT(memcmp(&d,&z,sizeof(d))==0); UT_ASSERT_EQ(census_dependency_calls,0);
 }
 UT_ASSERT_EQ(c->status,CLUSTER_RECOVERY_CENSUS_DRAINED); census_tx_done(c,&in,fd,path);
}
UT_TEST(test_census_tx_dependencies_late_source_count_error_revoke_whole_forest)
{
 for(unsigned fault=1;fault<=4;fault++) {
  ClusterWalInputsV1 *in; char path[MAXPGPATH]; int fd;
  ClusterRecoveryCensus *c=census_tx_setup(&in,&fd,path,false);
  ClusterRecoveryCensusTxDependencyV1 d={0},z={0}; volatile bool caught=false;
  UT_ASSERT_EQ(owned_census_until(c,0),CLUSTER_RECOVERY_CENSUS_DRAINED);
  census_dependency_fault=fault; census_dependency_calls=0;
  PG_TRY(); { d=cluster_recovery_census_tx_dependency_v1(c,0); }
  PG_CATCH(); { caught=true; } PG_END_TRY();
  UT_ASSERT(caught==(fault==2)); UT_ASSERT(memcmp(&d,&z,sizeof(d))==0);
  UT_ASSERT_EQ(c->status,CLUSTER_RECOVERY_CENSUS_REFUSED); UT_ASSERT(inputs_pin_held);
  census_dependency_fault=0; inputs_pin_current=true;
  d=cluster_recovery_census_tx_dependency_v1(c,0); UT_ASSERT(!d.observed);
  UT_ASSERT_EQ(census_dependency_calls,1); census_tx_done(c,&in,fd,path);
 }
}
