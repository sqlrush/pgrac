/* Native exact-key resolution into the original immutable forest. */
UT_TEST(test_tx_reference_find_preserves_shared_tail_indexes_and_native_uba)
{
 ClusterRecoveryReferenceGraph *g=dependency_fixture();
 for(uint32 i=0;i<3;i++) {
  ClusterRecoveryGraphDependency want,got;
  UT_ASSERT_EQ(cluster_recovery_reference_graph_dependency_at(g,i,&want),CLUSTER_RECOVERY_GRAPH_RECORD);
  UT_ASSERT_EQ(cluster_recovery_reference_graph_dependency_find(g,&want.value.transaction,want.value.record->address,&got),CLUSTER_RECOVERY_GRAPH_RECORD);
  UT_ASSERT(memcmp(&got,&want,sizeof(got))==0);
 }
 UT_ASSERT_EQ(input.calls,6); cluster_recovery_reference_graph_destroy(g);
}
UT_TEST(test_tx_reference_find_refuses_every_full_identity_and_address_difference)
{
 ClusterRecoveryReferenceGraph *g=dependency_fixture(); ClusterRecoveryGraphDependency want,got,z={0};
 UT_ASSERT_EQ(cluster_recovery_reference_graph_dependency_at(g,0,&want),CLUSTER_RECOVERY_GRAPH_RECORD);
 for(unsigned fault=0;fault<11;fault++) {
  ClusterRecoveryTxnKey k=want.value.transaction; UBA a=want.value.record->address;
  if(fault==0) k.system_identifier++;
  if(fault==1) k.database_incarnation++;
  if(fault==2) k.segment_id++;
  if(fault==3) k.segment_generation++;
  if(fault==4) k.xid++;
  if(fault==5) k.origin_node_id++;
  if(fault==6) k.slot_offset++;
  if(fault==7) k.slot_wrap++;
  if(fault==8) a.raw[0]^=1;
  if(fault==9) a.raw[1]^=UINT64CONST(1)<<32;
  if(fault==10) memset(&a,0,sizeof(a));
  memset(&got,0x31,sizeof(got));
  UT_ASSERT_EQ(cluster_recovery_reference_graph_dependency_find(g,&k,a,&got),CLUSTER_RECOVERY_GRAPH_INVALID);
  UT_ASSERT(memcmp(&got,&z,sizeof(got))==0);
 }
 UT_ASSERT_EQ(input.calls,6); cluster_recovery_reference_graph_destroy(g);
}
UT_TEST(test_tx_reference_find_prefix_empty_invalidated_and_inconsistent_bucket)
{
 ClusterRecoveryReferenceGraph *g; ClusterRecoveryGraphDependency got,z={0};
 ClusterRecoveryTxnKey k=transaction(); unsigned n=0; uint64 sum=0; UBA a=uba_encode(257,1,0,0);
 memset(&input,0,sizeof(input)); g=begin_graph(1024*1024);
 UT_ASSERT_EQ(cluster_recovery_reference_graph_dependency_find(g,&k,a,&got),CLUSTER_RECOVERY_GRAPH_INVALID);
 UT_ASSERT(memcmp(&got,&z,sizeof(got))==0);
 UT_ASSERT_EQ(drain(g,&n,&sum),CLUSTER_RECOVERY_GRAPH_DRAINED);
 UT_ASSERT_EQ(cluster_recovery_reference_graph_dependency_find(g,&k,a,&got),CLUSTER_RECOVERY_GRAPH_INVALID);
 UT_ASSERT_EQ(cluster_recovery_reference_graph_dependency_find(g,NULL,a,&got),CLUSTER_RECOVERY_GRAPH_INVALID);
 UT_ASSERT_EQ(cluster_recovery_reference_graph_dependency_find(g,&k,a,NULL),CLUSTER_RECOVERY_GRAPH_INVALID);
 cluster_recovery_reference_graph_destroy(g);
 for(unsigned fault=0;fault<3;fault++) {
  g=dependency_fixture(); k=g->nodes[0].transaction; a=g->nodes[0].address;
  if(fault==0) cluster_recovery_reference_graph_invalidate(g);
  if(fault==1) g->buckets[recovery_graph_bucket(&k,a,g->bucket_count-1)]=g->count+1;
  if(fault==2) g->nodes[0].predecessor=g->count;
  UT_ASSERT_EQ(cluster_recovery_reference_graph_dependency_find(g,&k,a,&got),CLUSTER_RECOVERY_GRAPH_INVALID);
  UT_ASSERT(memcmp(&got,&z,sizeof(got))==0); UT_ASSERT_EQ(input.calls,6);
  cluster_recovery_reference_graph_destroy(g);
 }
}
