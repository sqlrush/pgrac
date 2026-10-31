/* Original native forest root/predecessor adjacency, never owner completion.
 * Author: SqlRush <sqlrush@gmail.com> */
static ClusterRecoveryReferenceGraph *
dependency_fixture(void)
{
 ClusterRecoveryTxnKey key = transaction();
 ClusterRecoveryReferenceGraph *g; unsigned records = 0; uint64 sum = 0;
 memset(&input,0,sizeof(input));
 add_record(257,1,16385,(UBA){{0,0}});
 add_record(258,2,20001,input.records[0].address);
 add_record(258,3,20002,input.records[0].address);
 g=begin_graph(1024*1024);
 UT_ASSERT_EQ(cluster_recovery_reference_graph_add(g,&key,input.records[1].address),0);
 UT_ASSERT_EQ(cluster_recovery_reference_graph_add(g,&key,input.records[2].address),0);
 UT_ASSERT_EQ(cluster_recovery_reference_graph_add(g,&key,input.records[1].address),0);
 UT_ASSERT_EQ(drain(g,&records,&sum),CLUSTER_RECOVERY_GRAPH_DRAINED);
 UT_ASSERT_EQ(records,3); UT_ASSERT_EQ(input.calls,6);
 return g;
}
UT_TEST(test_tx_dependencies_preserve_roots_shared_tail_and_physical_generation)
{
 ClusterRecoveryReferenceGraph *g=dependency_fixture();
 for(uint32 i=0;i<3;i++) {
  ClusterRecoveryGraphDependency d; ClusterRecoveryGraphRecord r;
  UT_ASSERT_EQ(cluster_recovery_reference_graph_dependency_at(g,i,&d),CLUSTER_RECOVERY_GRAPH_RECORD);
  UT_ASSERT_EQ(cluster_recovery_reference_graph_at(g,i,&r),CLUSTER_RECOVERY_GRAPH_RECORD);
  UT_ASSERT_EQ(d.count,3); UT_ASSERT_EQ(d.root_count,2); UT_ASSERT_EQ(d.index,i);
  UT_ASSERT(d.root==(i<2)); UT_ASSERT(d.value.record==r.record);
  UT_ASSERT_EQ(d.value.transaction.segment_id,257);
  if(i<2) {
   UT_ASSERT_EQ(d.predecessor_index,2); UT_ASSERT(d.predecessor.record!=NULL);
   UT_ASSERT(cluster_undo_record_uba_equal(d.predecessor.record->address,d.value.record->header.prev_uba));
   UT_ASSERT_EQ(d.value.record_segment_generation,7);
   UT_ASSERT_EQ(d.predecessor.record_segment_generation,0);
   UT_ASSERT_EQ(d.predecessor.record->header.target_locator.relNumber,16385);
  } else UT_ASSERT(d.predecessor.record==NULL);
 }
 UT_ASSERT_EQ(input.calls,6); cluster_recovery_reference_graph_destroy(g);
}
UT_TEST(test_tx_dependencies_keep_an_already_root_predecessor_and_native_uba)
{
 ClusterRecoveryTxnKey k=transaction(); ClusterRecoveryReferenceGraph *g;
 ClusterRecoveryGraphDependency d; unsigned n=0; uint64 sum=0;
 memset(&input,0,sizeof(input)); add_record(257,1,16385,(UBA){{0,0}});
 add_record(258,2,20001,input.records[0].address); g=begin_graph(1024*1024);
 UT_ASSERT_EQ(cluster_recovery_reference_graph_add(g,&k,input.records[0].address),0);
 UT_ASSERT_EQ(cluster_recovery_reference_graph_add(g,&k,input.records[1].address),0);
 UT_ASSERT_EQ(drain(g,&n,&sum),CLUSTER_RECOVERY_GRAPH_DRAINED);
 UT_ASSERT_EQ(cluster_recovery_reference_graph_dependency_at(g,1,&d),CLUSTER_RECOVERY_GRAPH_RECORD);
 UT_ASSERT(d.root); UT_ASSERT_EQ(d.root_count,2); UT_ASSERT_EQ(d.predecessor_index,0);
 UT_ASSERT(d.predecessor.record!=NULL);
 UT_ASSERT_EQ(d.predecessor.record->address.raw[0],input.records[0].address.raw[0]);
 UT_ASSERT_EQ(d.predecessor.record->address.raw[1],input.records[0].address.raw[1]);
 UT_ASSERT_EQ(input.calls,4); cluster_recovery_reference_graph_destroy(g);
}
UT_TEST(test_tx_dependencies_prefix_empty_end_range_and_invalidation)
{
 ClusterRecoveryGraphDependency d,z={0}; ClusterRecoveryReferenceGraph *g;
 unsigned n=0; uint64 sum=0; memset(&input,0,sizeof(input)); g=begin_graph(1024*1024);
 memset(&d,0x71,sizeof(d));
 UT_ASSERT_EQ(cluster_recovery_reference_graph_dependency_at(g,0,&d),CLUSTER_RECOVERY_GRAPH_INVALID);
 UT_ASSERT(memcmp(&d,&z,sizeof(d))==0);
 UT_ASSERT_EQ(drain(g,&n,&sum),CLUSTER_RECOVERY_GRAPH_DRAINED);
 UT_ASSERT_EQ(cluster_recovery_reference_graph_dependency_at(g,0,&d),CLUSTER_RECOVERY_GRAPH_DRAINED);
 UT_ASSERT(memcmp(&d,&z,sizeof(d))==0);
 UT_ASSERT_EQ(cluster_recovery_reference_graph_dependency_at(g,1,&d),CLUSTER_RECOVERY_GRAPH_INVALID);
 UT_ASSERT(memcmp(&d,&z,sizeof(d))==0);
 UT_ASSERT_EQ(cluster_recovery_reference_graph_dependency_at(g,0,NULL),CLUSTER_RECOVERY_GRAPH_INVALID);
 cluster_recovery_reference_graph_destroy(g); g=dependency_fixture();
 UT_ASSERT_EQ(cluster_recovery_reference_graph_dependency_at(g,3,&d),CLUSTER_RECOVERY_GRAPH_DRAINED);
 UT_ASSERT_EQ(d.count,3); UT_ASSERT_EQ(d.root_count,2); UT_ASSERT_EQ(d.index,3);
 UT_ASSERT(!d.root && d.value.record==NULL && d.predecessor.record==NULL);
 cluster_recovery_reference_graph_invalidate(g);
 UT_ASSERT_EQ(cluster_recovery_reference_graph_dependency_at(g,0,&d),CLUSTER_RECOVERY_GRAPH_INVALID);
 UT_ASSERT(memcmp(&d,&z,sizeof(d))==0); UT_ASSERT_EQ(input.calls,6);
 cluster_recovery_reference_graph_destroy(g);
}
UT_TEST(test_tx_dependencies_refuse_inconsistent_original_edge_without_partial_view)
{
 for(unsigned fault=0;fault<6;fault++) {
  ClusterRecoveryReferenceGraph *g=dependency_fixture(); ClusterRecoveryGraphDependency d,z={0};
  if(fault==0) g->nodes[0].predecessor=g->count;
  if(fault==1) g->nodes[2].transaction.segment_generation++;
  if(fault==2) g->nodes[2].address.raw[1]^=UINT64CONST(1)<<32;
  if(fault==3) g->nodes[2].state=RECOVERY_NODE_DISCOVERED;
  if(fault==4) g->nodes[0].predecessor=RECOVERY_GRAPH_NONE;
  if(fault==5) g->nodes[2].predecessor=0;
  UT_ASSERT_EQ(cluster_recovery_reference_graph_dependency_at(g,fault==5?2:0,&d),CLUSTER_RECOVERY_GRAPH_INVALID);
  UT_ASSERT(memcmp(&d,&z,sizeof(d))==0); UT_ASSERT_EQ(input.calls,6);
  cluster_recovery_reference_graph_destroy(g);
 }
}
