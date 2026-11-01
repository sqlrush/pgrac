/* One compiled caller -> real GCS slots/router -> real inline/native/G2
 * chain. DATA delivery and backend/storage primitives remain controlled. */
int k_service_component_main(void);
#define main k_service_component_main
#define LWLockAcquire k_native_lock_acquire
#define LWLockRelease k_native_lock_release
#define cluster_gcs_current_mx_range_fetch_and_wait k_controlled_range_transport
#include "test_k_mx_g5_service.c"
#undef cluster_gcs_current_mx_range_fetch_and_wait
#undef LWLockAcquire
#undef LWLockRelease
#undef main
bool LWLockAcquire(LWLock *lock, LWLockMode mode);
void LWLockRelease(LWLock *lock);
bool cluster_gcs_current_mx_range_fetch_and_wait(ClusterMxRangeRequest *request,
	ClusterMxRangeResponse *response,uint32 *bytes);
#include "../../backend/cluster/cluster_gcs_block.c"

ProcessingMode Mode=NormalProcessing;
int cluster_lms_workers=4;
static ClusterGcsBlockBackendBlock native_blocks[2];
static PGAlignedBlock stats_memory;
static unsigned gcs_locks, requests_sent, replies_sent, wakes, fault, data_worker;
static uint64 previous_request_id;

void *ShmemInitStruct(const char *name,Size size,bool *found)
{
	UT_ASSERT_EQ(strcmp(name,"pgrac current multixact stats"),0);
	UT_ASSERT(size<=sizeof(stats_memory));*found=false;return stats_memory.data;
}
TimestampTz GetCurrentTimestamp(void) { return 123456; }
int errdetail(const char *fmt, ...) { (void)fmt; return 0; }
void *palloc0(Size size) { return MemoryContextAllocZero(CurrentMemoryContext,size); }
void MemoryContextReset(MemoryContext context)
{
	Arena *a=(Arena *)context;
	while(a->blocks)pfree(a->blocks->ptr);
}
void FlushErrorState(void) { }
bool LWLockAcquire(LWLock *lock,LWLockMode mode)
{
	if(lock==&native_blocks[0].lock.lock || lock==&native_blocks[1].lock.lock) {
		UT_ASSERT_EQ(gcs_locks,0);gcs_locks++;return true;
	}
	return k_native_lock_acquire(lock,mode);
}
void LWLockRelease(LWLock *lock)
{
	if(lock==&native_blocks[0].lock.lock || lock==&native_blocks[1].lock.lock) {
		UT_ASSERT_EQ(gcs_locks,1);gcs_locks--;return;
	}
	k_native_lock_release(lock);
}
const ClusterICMsgTypeInfo *cluster_ic_get_msg_type_info(uint8 type)
{
	static ClusterICMsgTypeInfo data={.plane=CLUSTER_IC_PLANE_DATA};
	UT_ASSERT_EQ(type,PGRAC_IC_MSG_GCS_BLOCK_REPLY);return &data;
}
void cluster_gcs_block_dedup_register_backend_exit_hook(void) { }
int cluster_ic_tier1_my_data_channel(void) { return data_worker; }
void ConditionVariablePrepareToSleep(ConditionVariable *cv) { (void)cv; }
bool ConditionVariableCancelSleep(void) { return false; }
void ConditionVariableBroadcast(ConditionVariable *cv) { (void)cv;wakes++; }
bool ConditionVariableTimedSleep(ConditionVariable *cv,long timeout,uint32 event)
{
	(void)cv;(void)event;UT_ASSERT_EQ(gcs_locks,0);UT_ASSERT_EQ(timeout,100);
	/* Controlled transport never silently loses a valid reply. Refused data
	 * is terminated by the real caller cancel path, never a timeout error. */
	InterruptPending=true;return true;
}

ClusterICSendResult cluster_ic_send_envelope(uint8 type,int32 destination,
	const void *payload,uint32 bytes)
{
	ClusterICEnvelope env={0};ClusterMxRangeResponse copy;
	UT_ASSERT_EQ(type,PGRAC_IC_MSG_GCS_BLOCK_REPLY);UT_ASSERT_EQ(destination,1);
	UT_ASSERT(bytes<=sizeof(copy));memcpy(&copy,payload,bytes);
	env.msg_type=type;env.source_node_id=0;env.dest_node_id=1;env.payload_length=bytes;
	cluster_node_id=1;MyBackendId=1;
	if(fault==6)copy.data.data[sizeof(ClusterMxRangeWireHeader)]^=1;
	UT_ASSERT(gcs_block_try_land_full_mx_reply(&env,&copy));
	replies_sent++;cluster_node_id=0;MyBackendId=InvalidBackendId;
	return CLUSTER_IC_SEND_DONE;
}

bool cluster_lms_outbound_enqueue_cap_bound(int worker,uint8 type,uint32 destination,
	const void *payload,uint16 bytes,uint32 capabilities,uint32 generation)
{
	ClusterICEnvelope env={0};const ClusterMxRangeRequest *request=payload;
	int saved_node=cluster_node_id;BackendId saved_backend=MyBackendId;
	UT_ASSERT_EQ(gcs_locks,0);UT_ASSERT_EQ(type,PGRAC_IC_MSG_GCS_BLOCK_FORWARD);
	UT_ASSERT_EQ(destination,0);UT_ASSERT_EQ(generation,11);
	UT_ASSERT_EQ(capabilities,CLUSTER_FOREIGN_MX_REQUIRED_CAPABILITIES);
	UT_ASSERT_EQ(worker,cluster_gcs_block_payload_shard(type,payload,bytes,cluster_lms_workers));
	UT_ASSERT(request->route.request_id!=previous_request_id);previous_request_id=request->route.request_id;
	requests_sent++;data_worker=worker;
	if(fault==1 && requests_sent==2)ko_revision++;
	if(fault==2 && requests_sent==2)service_boot++;
	if(fault==3 && requests_sent==2)native_epoch++;
	if(fault==4 && requests_sent==2)connection_generation++;
	if(fault==5 && requests_sent==2)native_fail_at=offset_reads+member_reads+1;
	env.msg_type=type;env.source_node_id=1;env.dest_node_id=0;env.payload_length=bytes;
	cluster_node_id=0;MyBackendId=InvalidBackendId;
	PG_TRY(); {
		cluster_gcs_current_mx_range_serve_inline(&env,payload);
	} PG_FINALLY(); {
		cluster_node_id=saved_node;MyBackendId=saved_backend;
	} PG_END_TRY();
	if(fault==7 && requests_sent==1)InterruptPending=true;
	return true;
}

static void chain_seed(uint32 count,uint32 start)
{
	service_seed(count,start);memset(native_blocks,0,sizeof(native_blocks));
	gcs_block_backend_blocks=native_blocks;
	native_blocks[0].next_request_id=1;native_blocks[1].next_request_id=1;
	gcs_locks=requests_sent=replies_sent=wakes=fault=0;previous_request_id=0;
	cluster_multixact_current_stats_shmem_init();
}
static void chain_clean(void)
{
	UT_ASSERT_EQ(io_live,0);UT_ASSERT_EQ(gcs_locks,0);UT_ASSERT_EQ(live_blocks,0);
	/* Only the backend top and original reusable CR scratch contexts live. */
	UT_ASSERT_EQ(live_contexts,2);UT_ASSERT(CurrentMemoryContext==TopMemoryContext);
	UT_ASSERT_EQ(cluster_node_id,1);UT_ASSERT_EQ(MyBackendId,1);
	for(unsigned i=0;i<4;i++)UT_ASSERT_EQ(held[i],0);
	for(unsigned i=0;i<lengthof(native_blocks);i++)
		for(unsigned j=0;j<lengthof(native_blocks[i].slots);j++)UT_ASSERT(!native_blocks[i].slots[j].in_use);
}
static void caller_native_transport_sizes(void)
{
	uint32 sizes[]={2,256,257,65537};
	for(unsigned i=0;i<lengthof(sizes);i++) {
		ClusterCurrentMxKey key={0};ClusterMxCompositionReceive *op=NULL;
		ClusterMxCompositionReceiveStats stats={0};uint32 total=0,seen=0;
		const ClusterMxCompositionPage *page;
		chain_seed(sizes[i],MULTIXACT_MEMBERS_PER_PAGE-1);key.multixact_id=1000;key.cluster_epoch=7;
		UT_ASSERT(cluster_multixact_current_describe_full(&key,&request_tag,&request_snapshot,&op,&stats));
		page=cluster_mx_composition_pages(op,&total);
		UT_ASSERT_EQ(total,sizes[i]);
		for(;page;page=page->next)for(unsigned j=0;j<page->count;j++,seen++)UT_ASSERT_EQ(page->members[j].xid,100+seen);
		UT_ASSERT_EQ(seen,total);UT_ASSERT_EQ(stats.accepted_frames,requests_sent);
		UT_ASSERT_EQ(requests_sent,replies_sent);UT_ASSERT_EQ(stats.unknown_results,0);
		cluster_multixact_current_describe_finish(&op);chain_clean();
	}
}
static void caller_native_transport_failure_cleanup(void)
{
	for(unsigned mode=1;mode<=7;mode++) {
		ClusterCurrentMxKey key={0};
		static ClusterMxCompositionReceive *op;
		ClusterMxCompositionReceiveStats stats={0};volatile bool caught=false;
		chain_seed(257,1);op=NULL;fault=mode;key.multixact_id=1000;key.cluster_epoch=7;
		PG_TRY(); {
			UT_ASSERT(!cluster_multixact_current_describe_full(&key,&request_tag,&request_snapshot,&op,&stats));
		} PG_CATCH(); { caught=true; } PG_END_TRY();
		UT_ASSERT_EQ(caught,mode==6 || mode==7);UT_ASSERT_NULL(op);UT_ASSERT(requests_sent<=2);
		InterruptPending=false;chain_clean();
	}
}
static void scoped_outcomes_in_original_snapshot(void)
{
	ClusterCurrentMxKey key={0};SnapshotData snapshot={0};ClusterCurrentMxMemberDesc members[256];
	uint16 count;uint32 total;ClusterCurrentMxStatsSnapshot observed;
	key.multixact_id=1000;key.cluster_epoch=7;snapshot.snapshot_type=SNAPSHOT_ANY;
	for(unsigned mode=0;mode<3;mode++) {
		ClusterMxDescribeResult result;
		chain_seed(mode==0?2:257,1);if(mode==2)fault=1;
		result=cluster_multixact_current_describe_scoped(&key,&request_tag,&snapshot,members,256,&count,&total);
		UT_ASSERT_EQ(result,mode==0?CMX_DESC_OK:mode==1?CMX_DESC_SUPPORTED_LIMIT:CMX_DESC_UNKNOWN);
		UT_ASSERT_EQ(count,mode==0?2:0);UT_ASSERT_EQ(total,mode==0?2:mode==1?257:0);
		for(unsigned i=0;i<lengthof(members);i++)
			UT_ASSERT_EQ(members[i].xid,mode==0 && i<2?100+i:0);
		UT_ASSERT(cluster_multixact_current_stats_snapshot(1,7,&observed));
		UT_ASSERT_EQ(observed.node_id,1);UT_ASSERT_EQ(observed.cluster_epoch,7);
		UT_ASSERT_EQ(observed.counters[CMX_STAT_DESCRIBE_REMOTE_ASK],1);
		UT_ASSERT_EQ(observed.counters[CMX_STAT_DESCRIBE_REMOTE_HIT],mode==0);
		UT_ASSERT_EQ(observed.counters[CMX_STAT_DESCRIBE_REMOTE_SUPPORTED_LIMIT],mode==1);
		UT_ASSERT_EQ(observed.counters[CMX_STAT_DESCRIBE_REMOTE_UNKNOWN],mode==2);
		UT_ASSERT_EQ(observed.counters[CMX_STAT_RESTART_MAX],0);chain_clean();
	}
}
static void scoped_cancel_has_no_returned_outcome(void)
{
	for(unsigned mode=6;mode<=7;mode++) {
		ClusterCurrentMxKey key={0};SnapshotData snapshot={0};
		ClusterCurrentMxMemberDesc members[256];ClusterCurrentMxStatsSnapshot observed;
		/* Outputs survive the caller's ERROR/cancel longjmp. */
		static uint16 count;
		static uint32 total;
		volatile bool caught=false;
		chain_seed(257,1);fault=mode;key.multixact_id=1000;key.cluster_epoch=7;
		snapshot.snapshot_type=SNAPSHOT_ANY;count=99;total=99;
		PG_TRY(); {
			(void)cluster_multixact_current_describe_scoped(&key,&request_tag,&snapshot,members,256,&count,&total);
		} PG_CATCH(); { caught=true; } PG_END_TRY();
		UT_ASSERT(caught);UT_ASSERT_EQ(count,0);UT_ASSERT_EQ(total,0);
		UT_ASSERT(cluster_multixact_current_stats_snapshot(1,7,&observed));
		UT_ASSERT_EQ(observed.counters[CMX_STAT_DESCRIBE_REMOTE_ASK],1);
		UT_ASSERT_EQ(observed.counters[CMX_STAT_DESCRIBE_REMOTE_HIT],0);
		UT_ASSERT_EQ(observed.counters[CMX_STAT_DESCRIBE_REMOTE_SUPPORTED_LIMIT],0);
		UT_ASSERT_EQ(observed.counters[CMX_STAT_DESCRIBE_REMOTE_UNKNOWN],0);
		UT_ASSERT_EQ(observed.counters[CMX_STAT_DESCRIBE_REMOTE_TIMEOUT],0);
		UT_ASSERT_EQ(observed.counters[CMX_STAT_RESTART_MAX],0);
		InterruptPending=false;chain_clean();
	}
}
int main(void)
{
	TopMemoryContext=AllocSetContextCreate(CurrentMemoryContext,"native caller",ALLOCSET_DEFAULT_SIZES);
	CurrentMemoryContext=TopMemoryContext;
	UT_PLAN(4);UT_RUN(caller_native_transport_sizes);UT_RUN(caller_native_transport_failure_cleanup);
	UT_RUN(scoped_outcomes_in_original_snapshot);UT_RUN(scoped_cancel_has_no_returned_outcome);UT_DONE();
	return ut_failed_count==0?0:1;
}
