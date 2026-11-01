/* Original G1/G2/ResourceOwner and GCS sender/landing bodies. Only proof DATA
 * delivery supplies literal provisional terminal frames: no TT/grant/touch
 * authority, local service or qualification is manufactured by this fixture. */
int k_window_component_main(void);
#define K_MX_NATIVE_CALLER_MAIN k_window_component_main
#include "test_k_mx_g5_native_caller.c"

static unsigned origin_mode, window_fault, window_frames;
static uint32 window_fault_first, highest_first;

int cluster_xid_origin_slot(TransactionId xid)
{
	if (xid < 100) return -1;
	if (origin_mode == 2) return xid < 103 ? 1 : 2;
	if (origin_mode == 3 && xid == 107) return -1;
	return ((xid - 100) / 5) % 2 == 0 ? 0 : 2;
}

static bool window_enqueue(int worker,uint8 type,uint32 destination,const void *payload,
	uint16 bytes,uint32 capabilities,uint32 generation)
{
	const ClusterMxProofRequest *packet=payload;ClusterMxProofRequestView v;
	ClusterMxRangeWireContext c={0};ClusterMxProofReply reply={0};ClusterMxFullMemberProof entries[7]={{0}};
	ClusterMxRangeResponse response;ClusterICEnvelope env={0};uint32 n=0;
	UT_ASSERT_EQ(type,PGRAC_IC_MSG_GCS_BLOCK_FORWARD);UT_ASSERT_EQ(gcs_locks,0);
	UT_ASSERT(cluster_mx_proof_request_decode(packet,bytes,202611010,capabilities,&v));
	UT_ASSERT_EQ(worker,cluster_gcs_block_payload_shard(type,payload,bytes,cluster_lms_workers));
	UT_ASSERT_EQ(generation,destination==2?18:11);UT_ASSERT_EQ(v.peer_generation,generation);
	UT_ASSERT_EQ(v.source.capability_generation,111);UT_ASSERT_EQ(v.key.origin_node_id,0);
	UT_ASSERT(destination!=1);window_frames++;highest_first=v.first;
	for(uint32 i=0;i<v.count;i++){
		UT_ASSERT_EQ(cluster_xid_origin_slot(v.asks[i].xid),destination);
		entries[i].member_ordinal=v.first+i;entries[i].origin_node_id=destination;
		entries[i].proof.member_xid=v.asks[i].xid;entries[i].proof.member_status=v.asks[i].member_status;
		entries[i].proof.member_ordinal=i;entries[i].proof.state=CCM_ABORTED;
	}
	c.request_id=v.route.request_id;c.epoch=v.route.epoch;c.key=v.key;
	c.format_id=v.route.format_id;c.version=v.route.version;c.loaded_catalog=202611010;
	c.peer_capabilities=capabilities;c.peer_generation=generation;c.requested_cap=v.count;
	c.source=&v.source;c.source_bytes=sizeof(v.source);
	reply.source=&v.source;reply.source_bytes=sizeof(v.source);reply.total=v.total;reply.first=v.first;
	reply.count=v.count;reply.next=v.next;reply.last=v.next==v.total;reply.proofs=entries;
	reply.proof_bytes=v.count*sizeof(*entries);reply.frame_bytes=BLCKSZ;
	if(window_fault && v.first==window_fault_first && window_fault<=2){
		UT_ASSERT(cluster_mx_proof_response_pack(&v,&c,window_fault==1?MXCOMP_READ_UNKNOWN:MXCOMP_READ_RETRY,
			NULL,&response,sizeof(response),&n));
	}else{
		UT_ASSERT(cluster_mx_proof_response_pack(&v,&c,MXCOMP_READ_OK,&reply,&response,sizeof(response),&n));
	}
	if(window_fault==3 && v.first==window_fault_first){InterruptPending=true;return true;}
	env.msg_type=PGRAC_IC_MSG_GCS_BLOCK_REPLY;env.source_node_id=destination;env.dest_node_id=1;env.payload_length=n;
	UT_ASSERT(gcs_block_try_land_full_mx_reply(&env,&response));
	if(window_fault==4 && v.first==window_fault_first)native_member_generation_delta++;
	return true;
}

static ClusterMxCompositionReceive *window_start(uint32 total,ClusterMxCompositionReceiveStats *stats)
{
	ClusterCurrentMxKey key={0};ClusterMxCompositionReceive *op=NULL;
	chain_seed(total,MULTIXACT_MEMBERS_PER_PAGE-1);key.multixact_id=1000;key.cluster_epoch=7;
	UT_ASSERT(cluster_multixact_current_describe_full(&key,&request_tag,&request_snapshot,&op,stats));
	origin_mode=1;window_fault=window_fault_first=window_frames=highest_first=0;
	native_member_generation_delta=7;native_proof_enqueue=window_enqueue;
	return op;
}

static void window_finish(ClusterMxCompositionReceive **op)
{
	cluster_multixact_current_describe_finish(op);
	range_release(RESOURCE_RELEASE_BEFORE_LOCKS,false,false,range_release_arg);
	chain_clean();
}

UT_TEST(window_partitions_origins_without_advancing_on_peek)
{
	ClusterMxCompositionReceiveStats stats={0};ClusterMxProofTransportStats transport={0};
	ClusterMxCompositionReceive *op=window_start(13,&stats);
	const uint32 firsts[]={0,5,10},counts[]={5,5,3};const int32 origins[]={0,2,0};
	for(unsigned i=0;i<3;i++){
		ClusterMxProofWindow w,again;uint32 total=99;
		UT_ASSERT(cluster_multixact_current_proofs_window(&op,&w));
		UT_ASSERT(cluster_multixact_current_proofs_window(&op,&again));
		UT_ASSERT_EQ(memcmp(&w,&again,sizeof(w)),0);UT_ASSERT_EQ(window_frames,i);
		UT_ASSERT_EQ(w.total,13);UT_ASSERT_EQ(w.first,firsts[i]);UT_ASSERT_EQ(w.count,counts[i]);UT_ASSERT_EQ(w.origin,origins[i]);
		UT_ASSERT_NULL(cluster_mx_composition_proofs(op,&total));UT_ASSERT_EQ(total,0);
		UT_ASSERT_EQ(cluster_multixact_current_proofs_fetch_window(&op,&w,&transport),i==2?MXCOMP_COMPLETE:MXCOMP_PARTIAL);
	}
	UT_ASSERT_EQ(transport.attempts,3);UT_ASSERT_EQ(transport.enqueued,3);UT_ASSERT_EQ(transport.replies,3);
	UT_ASSERT_EQ(transport.request_bytes,924);UT_ASSERT_EQ(transport.reply_bytes,3368);
	UT_ASSERT_EQ(stats.accepted_proof_frames,3);UT_ASSERT_EQ(stats.unknown_results,0);window_finish(&op);
}

UT_TEST(window_full_width_assembly_keeps_global_tail)
{
	const uint32 sizes[]={2,256,257,65537};
	for(unsigned s=0;s<lengthof(sizes);s++){
		ClusterMxCompositionReceiveStats stats={0};ClusterMxProofTransportStats transport={0};
		ClusterMxCompositionReceive *op=window_start(sizes[s],&stats);uint32 seen=0,total=0,steps=0;
		const ClusterMxCompositionProofPage *page;ClusterMxCompositionReceiveResult result=MXCOMP_UNKNOWN;
		while(seen<sizes[s] && steps++<sizes[s]){
			ClusterMxProofWindow w;UT_ASSERT(cluster_multixact_current_proofs_window(&op,&w));
			UT_ASSERT_EQ(w.first,seen);UT_ASSERT(w.count>0 && w.count<=7);seen+=w.count;
			result=cluster_multixact_current_proofs_fetch_window(&op,&w,&transport);
			UT_ASSERT_EQ(result,seen==sizes[s]?MXCOMP_COMPLETE:MXCOMP_PARTIAL);
			if(result==MXCOMP_UNKNOWN)break;
		}
		UT_ASSERT_EQ(seen,sizes[s]);UT_ASSERT_EQ(result,MXCOMP_COMPLETE);
		page=cluster_mx_composition_proofs(op,&total);UT_ASSERT_EQ(total,sizes[s]);seen=0;
		for(;page;page=page->next)for(uint32 i=0;i<page->count;i++,seen++){
			UT_ASSERT_EQ(page->proofs[i].member_ordinal,seen);UT_ASSERT_EQ(page->proofs[i].proof.member_xid,100+seen);
			UT_ASSERT_EQ(page->proofs[i].origin_node_id,((seen/5)%2)==0?0:2);
		}
		UT_ASSERT_EQ(seen,sizes[s]);if(s==3)UT_ASSERT_EQ(highest_first,65535);
		UT_ASSERT_EQ(transport.attempts,window_frames);UT_ASSERT_EQ(transport.replies,window_frames);
		UT_ASSERT_EQ(stats.accepted_proof_frames,window_frames);window_finish(&op);
	}
}

UT_TEST(window_local_members_are_handed_back_without_network_or_touch)
{
	ClusterMxCompositionReceiveStats stats={0};ClusterMxProofTransportStats transport={0};
	ClusterMxCompositionReceive *op=window_start(5,&stats);ClusterMxProofWindow w;
	ClusterMxFullMemberProof entries[7];ClusterMxProofReply reply;
	origin_mode=2;UT_ASSERT(cluster_multixact_current_proofs_window(&op,&w));
	UT_ASSERT_EQ(w.origin,1);UT_ASSERT_EQ(w.first,0);UT_ASSERT_EQ(w.count,3);UT_ASSERT_EQ(window_frames,0);
	/* Provisional fixture append tests the existing owner continuation only.
	 * It does not invoke, replace or certify the parked local TT/touch owner. */
	reply=proof_window(5,0,entries);reply.count=3;reply.next=3;reply.last=false;
	reply.proof_bytes=3*sizeof(*entries);reply.frame_bytes=208+reply.proof_bytes;
	for(unsigned i=0;i<3;i++)entries[i].origin_node_id=1;
	UT_ASSERT_EQ(cluster_multixact_current_proofs_accept(&op,MXCOMP_READ_OK,&reply),MXCOMP_PARTIAL);
	UT_ASSERT(cluster_multixact_current_proofs_window(&op,&w));
	UT_ASSERT_EQ(w.origin,2);UT_ASSERT_EQ(w.first,3);UT_ASSERT_EQ(w.count,2);
	UT_ASSERT_EQ(cluster_multixact_current_proofs_fetch_window(&op,&w,&transport),MXCOMP_COMPLETE);
	UT_ASSERT_EQ(window_frames,1);UT_ASSERT_EQ(transport.attempts,1);window_finish(&op);
}

UT_TEST(window_nonok_late_reply_discards_all_without_retry)
{
	for(unsigned kind=1;kind<=2;kind++){
		ClusterMxCompositionReceiveStats stats={0};ClusterMxProofTransportStats transport={0};
		ClusterMxCompositionReceive *op=window_start(10,&stats);ClusterMxProofWindow w;
		UT_ASSERT(cluster_multixact_current_proofs_window(&op,&w));
		UT_ASSERT_EQ(cluster_multixact_current_proofs_fetch_window(&op,&w,&transport),MXCOMP_PARTIAL);
		window_fault=kind;window_fault_first=5;
		UT_ASSERT(cluster_multixact_current_proofs_window(&op,&w));
		UT_ASSERT_EQ(cluster_multixact_current_proofs_fetch_window(&op,&w,&transport),MXCOMP_UNKNOWN);
		UT_ASSERT_NULL(op);UT_ASSERT_EQ(window_frames,2);UT_ASSERT_EQ(transport.attempts,2);
		UT_ASSERT_EQ(stats.accepted_proof_frames,1);UT_ASSERT_EQ(stats.unknown_results,1);
		UT_ASSERT_EQ(stats.restart_requests,kind==2);window_finish(&op);
	}
}

UT_TEST(window_cancel_cleans_original_operation_and_request_slot)
{
	static ClusterMxCompositionReceiveStats stats;static ClusterMxProofTransportStats transport;
	static ClusterMxCompositionReceive *op;ClusterMxProofWindow w;volatile bool caught=false;
	memset(&stats,0,sizeof(stats));memset(&transport,0,sizeof(transport));op=window_start(10,&stats);
	UT_ASSERT(cluster_multixact_current_proofs_window(&op,&w));
	UT_ASSERT_EQ(cluster_multixact_current_proofs_fetch_window(&op,&w,&transport),MXCOMP_PARTIAL);
	window_fault=3;window_fault_first=5;UT_ASSERT(cluster_multixact_current_proofs_window(&op,&w));
	PG_TRY();{(void)cluster_multixact_current_proofs_fetch_window(&op,&w,&transport);}
	PG_CATCH();{caught=true;}PG_END_TRY();
	UT_ASSERT(caught);UT_ASSERT_NULL(op);UT_ASSERT_EQ(window_frames,2);UT_ASSERT_EQ(transport.attempts,2);
	InterruptPending=false;window_finish(&op);
}

UT_TEST(window_rechecks_member_generation_after_transport_and_append)
{
	for(unsigned phase=0;phase<2;phase++){
		ClusterMxCompositionReceiveStats stats={0};ClusterMxProofTransportStats transport={0};
		ClusterMxCompositionReceive *op=window_start(10,&stats);ClusterMxProofWindow w;
		UT_ASSERT(cluster_multixact_current_proofs_window(&op,&w));
		UT_ASSERT_EQ(cluster_multixact_current_proofs_fetch_window(&op,&w,&transport),MXCOMP_PARTIAL);
		UT_ASSERT(cluster_multixact_current_proofs_window(&op,&w));
		if(phase){proof_allocation_fault=7;}else{window_fault=4;window_fault_first=5;}
		UT_ASSERT_EQ(cluster_multixact_current_proofs_fetch_window(&op,&w,&transport),MXCOMP_UNKNOWN);
		UT_ASSERT_NULL(op);UT_ASSERT_EQ(stats.accepted_proof_frames,phase?2:1);
		UT_ASSERT_EQ(stats.unknown_results,1);UT_ASSERT_EQ(stats.restart_requests,1);window_finish(&op);
	}
}

UT_TEST(window_source_scope_drift_before_prepare_discards_operation)
{
	ClusterMxCompositionReceiveStats stats={0};ClusterMxCompositionReceive *op=window_start(13,&stats);
	ClusterMxProofWindow w,zero={0};connection_generation++;memset(&w,0xa5,sizeof(w));
	UT_ASSERT(!cluster_multixact_current_proofs_window(&op,&w));UT_ASSERT_NULL(op);
	UT_ASSERT_EQ(memcmp(&w,&zero,sizeof(w)),0);UT_ASSERT_EQ(window_frames,0);
	UT_ASSERT_EQ(stats.unknown_results,1);UT_ASSERT_EQ(stats.restart_requests,1);window_finish(&op);
}

UT_TEST(window_owner_release_invalidates_borrow_before_send)
{
	ClusterMxCompositionReceiveStats stats={0};ClusterMxProofTransportStats transport={0};
	ClusterMxCompositionReceive *op=window_start(13,&stats);ClusterMxProofWindow w;
	UT_ASSERT(cluster_multixact_current_proofs_window(&op,&w));
	range_release(RESOURCE_RELEASE_BEFORE_LOCKS,false,false,range_release_arg);
	UT_ASSERT_EQ(cluster_multixact_current_proofs_fetch_window(&op,&w,&transport),MXCOMP_UNKNOWN);
	UT_ASSERT_NULL(op);UT_ASSERT_EQ(window_frames,0);UT_ASSERT_EQ(transport.attempts,0);window_finish(&op);
}

UT_TEST(window_unprovable_member_origin_discards_prior_windows)
{
	ClusterMxCompositionReceiveStats stats={0};ClusterMxProofTransportStats transport={0};
	ClusterMxCompositionReceive *op=window_start(13,&stats);ClusterMxProofWindow w;
	origin_mode=3;UT_ASSERT(cluster_multixact_current_proofs_window(&op,&w));
	UT_ASSERT_EQ(cluster_multixact_current_proofs_fetch_window(&op,&w,&transport),MXCOMP_PARTIAL);
	UT_ASSERT(!cluster_multixact_current_proofs_window(&op,&w));UT_ASSERT_NULL(op);
	UT_ASSERT_EQ(window_frames,1);UT_ASSERT_EQ(stats.unknown_results,1);UT_ASSERT_EQ(stats.restart_requests,1);window_finish(&op);
}

UT_TEST(window_forged_or_stale_plan_never_sends)
{
	for(unsigned mode=0;mode<6;mode++){
		ClusterMxCompositionReceiveStats stats={0};ClusterMxProofTransportStats transport={0};
		ClusterMxCompositionReceive *op=window_start(13,&stats);ClusterMxProofWindow w;
		UT_ASSERT(cluster_multixact_current_proofs_window(&op,&w));
		if(mode==0)w.first++;if(mode==1)w.count--;if(mode==2)w.origin=2;
		if(mode==3)w.members[0].xid++;if(mode==4)w.source.boot_incarnation++;if(mode==5)w.total++;
		UT_ASSERT_EQ(cluster_multixact_current_proofs_fetch_window(&op,&w,&transport),MXCOMP_UNKNOWN);
		UT_ASSERT_NULL(op);UT_ASSERT_EQ(window_frames,0);UT_ASSERT_EQ(transport.attempts,0);
		UT_ASSERT_EQ(stats.unknown_results,1);UT_ASSERT_EQ(stats.restart_requests,1);window_finish(&op);
	}
}

UT_TEST(window_scope_provider_error_rethrows_after_owner_cleanup)
{
	static ClusterMxCompositionReceiveStats stats;static ClusterMxCompositionReceive *op;
	static ClusterMxProofWindow w;
	ClusterMxProofWindow zero={0};volatile bool caught=false;
	memset(&stats,0,sizeof(stats));op=window_start(13,&stats);proof_catalog_error=true;
	memset(&w,0xa5,sizeof(w));
	PG_TRY();{(void)cluster_multixact_current_proofs_window(&op,&w);}
	PG_CATCH();{caught=true;}PG_END_TRY();proof_catalog_error=false;
	UT_ASSERT(caught);UT_ASSERT_NULL(op);UT_ASSERT_EQ(memcmp(&w,&zero,sizeof(w)),0);window_finish(&op);
}

int main(void)
{
	TopMemoryContext=AllocSetContextCreate(CurrentMemoryContext,"original proof window",ALLOCSET_DEFAULT_SIZES);
	CurrentMemoryContext=TopMemoryContext;
	UT_PLAN(11);
	UT_RUN(window_partitions_origins_without_advancing_on_peek);UT_RUN(window_full_width_assembly_keeps_global_tail);
	UT_RUN(window_local_members_are_handed_back_without_network_or_touch);UT_RUN(window_nonok_late_reply_discards_all_without_retry);
	UT_RUN(window_cancel_cleans_original_operation_and_request_slot);UT_RUN(window_rechecks_member_generation_after_transport_and_append);
	UT_RUN(window_source_scope_drift_before_prepare_discards_operation);UT_RUN(window_owner_release_invalidates_borrow_before_send);
	UT_RUN(window_unprovable_member_origin_discards_prior_windows);UT_RUN(window_forged_or_stale_plan_never_sends);
	UT_RUN(window_scope_provider_error_rethrows_after_owner_cleanup);
	MemoryContextDelete(TopMemoryContext);UT_DONE();return ut_failed_count==0?0:1;
}
