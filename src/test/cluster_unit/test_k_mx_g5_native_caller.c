/* One compiled caller -> real GCS slots/router -> real inline/native/G2
 * chain. DATA delivery and backend/storage primitives remain controlled. */
int k_service_component_main(void);
#define main k_service_component_main
#define LWLockAcquire k_native_lock_acquire
#define LWLockRelease k_native_lock_release
#define MemoryContextAllocZero k_proof_original_alloc_zero
#define GetControlFileCatalogVersion k_proof_original_catalog
#define cluster_gcs_current_mx_range_fetch_and_wait k_controlled_range_transport
#include "test_k_mx_g5_service.c"
#undef cluster_gcs_current_mx_range_fetch_and_wait
#undef LWLockAcquire
#undef LWLockRelease
#undef MemoryContextAllocZero
#undef GetControlFileCatalogVersion
#undef main
void *MemoryContextAllocZero(MemoryContext context, Size size);
uint32 GetControlFileCatalogVersion(void);
bool LWLockAcquire(LWLock *lock, LWLockMode mode);
void LWLockRelease(LWLock *lock);
bool cluster_gcs_current_mx_range_fetch_and_wait(ClusterMxRangeRequest *request,
	ClusterMxRangeResponse *response,uint32 *bytes);
bool k_real_range_fetch_and_wait(ClusterMxRangeRequest *request,
	ClusterMxRangeResponse *response,uint32 *bytes);
#define cluster_gcs_current_mx_range_fetch_and_wait k_real_range_fetch_and_wait
#include "../../backend/cluster/cluster_gcs_block.c"
#undef cluster_gcs_current_mx_range_fetch_and_wait

ProcessingMode Mode=NormalProcessing;
int cluster_lms_workers=4;
static ClusterGcsBlockBackendBlock native_blocks[2];
static PGAlignedBlock stats_memory;
static unsigned gcs_locks, requests_sent, replies_sent, wakes, fault, data_worker;
static uint64 previous_request_id;
static ClusterMxRangeSource proof_source;
static unsigned late_fault, late_at;
static bool late_fired;
static uint32 first_history[8];
static uint64 request_history[8];
static unsigned proof_allocation_fault;
static bool proof_allocation_fired, proof_catalog_error;
static uint32 proof_catalog_delta;
static unsigned proof_request_checks, proof_request_drift_at;

static void
proof_drift(unsigned kind)
{
	switch (kind) {
	case 1: native_epoch++; break;
	case 2: service_system++; break;
	case 3: connection_generation++; break;
	case 4: capability_ok = false; break;
	case 5: proof_catalog_delta++; break;
	case 6: proof_catalog_error = true; break;
	default: abort();
	}
}

void *
MemoryContextAllocZero(MemoryContext context, Size size)
{
	void *result = k_proof_original_alloc_zero(context, size);
	/* Keep the real G2 allocation/append path. Only its slow allocator leaf
	 * schedules a change after allocation and before the owner returns. */
	if (proof_allocation_fault && size == sizeof(ClusterMxCompositionProofPage)) {
		unsigned kind = proof_allocation_fault;
		proof_allocation_fault = 0;
		proof_allocation_fired = true;
		proof_drift(kind);
	}
	return result;
}

uint32
GetControlFileCatalogVersion(void)
{
	if (proof_request_drift_at && ++proof_request_checks == proof_request_drift_at)
		proof_catalog_delta++;
	if (proof_catalog_error)
		pg_re_throw();
	return k_proof_original_catalog() + proof_catalog_delta;
}

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
	memcpy(&proof_source, copy.data.data + sizeof(ClusterMxRangeWireHeader), sizeof(proof_source));
	replies_sent++;cluster_node_id=0;MyBackendId=InvalidBackendId;
	return CLUSTER_IC_SEND_DONE;
}

bool cluster_lms_outbound_enqueue_cap_bound(int worker,uint8 type,uint32 destination,
	const void *payload,uint16 bytes,uint32 capabilities,uint32 generation)
{
	ClusterICEnvelope env={0};const ClusterMxRangeRequest *request=payload;
	int saved_node=cluster_node_id;BackendId saved_backend=MyBackendId;
	UT_ASSERT_EQ(gcs_locks,0);UT_ASSERT_EQ(type,PGRAC_IC_MSG_GCS_BLOCK_FORWARD);
	UT_ASSERT_EQ(destination,0);UT_ASSERT_EQ(generation,connection_generation);
	UT_ASSERT_EQ(capabilities,CLUSTER_FOREIGN_MX_REQUIRED_CAPABILITIES);
	UT_ASSERT_EQ(worker,cluster_gcs_block_payload_shard(type,payload,bytes,cluster_lms_workers));
	UT_ASSERT(request->route.request_id!=previous_request_id);previous_request_id=request->route.request_id;
	requests_sent++;data_worker=worker;
	if(requests_sent<=lengthof(first_history)) {
		first_history[requests_sent-1]=request->first;
		request_history[requests_sent-1]=request->route.request_id;
	}
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

bool cluster_gcs_current_mx_range_fetch_and_wait(ClusterMxRangeRequest *request,
	ClusterMxRangeResponse *response,uint32 *bytes)
{
	bool result=k_real_range_fetch_and_wait(request,response,bytes);
	/* The real transport has finished all its own checks and retired the
	 * slot. This scheduling boundary never changes its result or output. */
	if(result && late_fault && requests_sent==late_at) {
		UT_ASSERT_EQ(replies_sent,requests_sent);late_fired=true;
		if(late_fault==1)native_epoch++;
		if(late_fault==2)connection_generation++;
		if(late_fault==3)capability_ok=false;
	}
	return result;
}

static void chain_seed(uint32 count,uint32 start)
{
	service_seed(count,start);memset(native_blocks,0,sizeof(native_blocks));
	gcs_block_backend_blocks=native_blocks;
	native_blocks[0].next_request_id=1;native_blocks[1].next_request_id=1;
	gcs_locks=requests_sent=replies_sent=wakes=fault=0;previous_request_id=0;
	late_fault=late_at=0;late_fired=false;memset(first_history,0,sizeof(first_history));
	memset(request_history,0,sizeof(request_history));
	proof_allocation_fault = proof_catalog_delta = 0;
	proof_allocation_fired = proof_catalog_error = false;
	proof_request_checks = proof_request_drift_at = 0;
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
static void expect_last_reply_refusal(unsigned mode)
{
	uint32 sizes[]={2,257};
	for(unsigned i=0;i<lengthof(sizes);i++) {
		ClusterCurrentMxKey key={0};ClusterMxCompositionReceive *op=NULL;
		ClusterMxCompositionReceiveStats stats={0};
		chain_seed(sizes[i],1);key.multixact_id=1000;key.cluster_epoch=7;
		late_fault=mode;late_at=i+1;
		UT_ASSERT(!cluster_multixact_current_describe_full(&key,&request_tag,&request_snapshot,&op,&stats));
		UT_ASSERT(late_fired);UT_ASSERT_NULL(op);
		UT_ASSERT_EQ(requests_sent,i+1);UT_ASSERT_EQ(replies_sent,i+1);
		UT_ASSERT_EQ(stats.accepted_frames,i+1);
		UT_ASSERT_EQ(stats.unknown_results,1);UT_ASSERT_EQ(stats.restart_requests,1);
		UT_ASSERT_EQ(legacy_calls,0);
		/* Keep a faulty success from leaking into the next test case. */
		cluster_multixact_current_describe_finish(&op);chain_clean();
	}
}
static void caller_rejects_epoch_after_last_reply(void)
{
	expect_last_reply_refusal(1);
}
static void caller_rejects_control_change_after_last_reply(void)
{
	expect_last_reply_refusal(2);expect_last_reply_refusal(3);
}
static void scoped_last_reply_refusal_has_one_outcome(void)
{
	for(unsigned mode=1;mode<=3;mode++)for(unsigned i=0;i<2;i++) {
		ClusterCurrentMxKey key={0};SnapshotData snapshot={0};
		ClusterCurrentMxMemberDesc members[256];ClusterCurrentMxStatsSnapshot observed;
		uint16 count=99;uint32 total=99;
		chain_seed(i==0?2:257,1);key.multixact_id=1000;key.cluster_epoch=7;
		late_fault=mode;late_at=i+1;snapshot.snapshot_type=SNAPSHOT_ANY;
		memset(members,0xa5,sizeof(members));
		UT_ASSERT_EQ(cluster_multixact_current_describe_scoped(&key,&request_tag,&snapshot,
			members,256,&count,&total),CMX_DESC_UNKNOWN);
		UT_ASSERT(late_fired);UT_ASSERT_EQ(count,0);UT_ASSERT_EQ(total,0);
		for(unsigned j=0;j<sizeof(members);j++)UT_ASSERT_EQ(((uint8 *)members)[j],0);
		UT_ASSERT_EQ(requests_sent,i+1);UT_ASSERT_EQ(replies_sent,i+1);
		UT_ASSERT(cluster_multixact_current_stats_snapshot(1,7,&observed));
		UT_ASSERT_EQ(observed.counters[CMX_STAT_DESCRIBE_REMOTE_ASK],1);
		UT_ASSERT_EQ(observed.counters[CMX_STAT_DESCRIBE_REMOTE_UNKNOWN],1);
		UT_ASSERT_EQ(observed.counters[CMX_STAT_DESCRIBE_REMOTE_HIT],0);
		UT_ASSERT_EQ(observed.counters[CMX_STAT_DESCRIBE_REMOTE_SUPPORTED_LIMIT],0);
		UT_ASSERT_EQ(observed.counters[CMX_STAT_DESCRIBE_REMOTE_TIMEOUT],0);
		UT_ASSERT_EQ(legacy_calls,0);chain_clean();
	}
}
static void caller_fresh_retry_starts_at_zero(void)
{
	ClusterCurrentMxKey key={0};ClusterMxCompositionReceive *op=NULL;
	ClusterMxCompositionReceiveStats stats={0};
	const ClusterMxCompositionPage *page;uint32 total=0,seen=0;
	chain_seed(257,1);key.multixact_id=1000;key.cluster_epoch=7;
	late_fault=2;late_at=2;
	UT_ASSERT(!cluster_multixact_current_describe_full(&key,&request_tag,&request_snapshot,&op,&stats));
	UT_ASSERT(late_fired);UT_ASSERT_NULL(op);UT_ASSERT_EQ(connection_generation,12);
	cluster_multixact_current_describe_finish(&op);chain_clean();
	late_fault=0;
	UT_ASSERT(cluster_multixact_current_describe_full(&key,&request_tag,&request_snapshot,&op,&stats));
	UT_ASSERT_EQ(requests_sent,4);UT_ASSERT_EQ(replies_sent,4);
	UT_ASSERT_EQ(first_history[0],0);UT_ASSERT_EQ(first_history[1],256);
	UT_ASSERT_EQ(first_history[2],0);UT_ASSERT_EQ(first_history[3],256);
	for(unsigned i=0;i<4;i++) {
		UT_ASSERT(request_history[i]!=0);
		for(unsigned j=0;j<i;j++)UT_ASSERT(request_history[i]!=request_history[j]);
	}
	page=cluster_mx_composition_pages(op,&total);UT_ASSERT_EQ(total,257);
	for(;page;page=page->next)for(unsigned j=0;j<page->count;j++,seen++)UT_ASSERT_EQ(page->members[j].xid,100+seen);
	UT_ASSERT_EQ(seen,257);UT_ASSERT_EQ(stats.accepted_frames,4);
	UT_ASSERT_EQ(stats.unknown_results,1);UT_ASSERT_EQ(stats.restart_requests,1);
	UT_ASSERT_EQ(legacy_calls,0);cluster_multixact_current_describe_finish(&op);chain_clean();
}
static void
caller_proof_windows_keep_original_owner(void)
{
	static ClusterMxCompositionReceive *op;
	static ClusterMxCompositionReceiveStats stats;
	for (unsigned mode = 0; mode < 4; mode++) {
		ClusterCurrentMxKey key = { 0 };
		ClusterMxFullMemberProof entries[7];
		ClusterMxProofReply r = { 0 };
		bool caught = false;
		chain_seed(9, 1);
		key.multixact_id = 1000;
		key.cluster_epoch = 7;
		memset(&stats, 0, sizeof(stats));
		op = NULL;
		UT_ASSERT(cluster_multixact_current_describe_full(&key, &request_tag, &request_snapshot,
														  &op, &stats));
		r.source = &proof_source;
		r.source_bytes = sizeof(proof_source);
		r.total = 9;
		r.count = 7;
		r.next = 7;
		r.proofs = entries;
		r.proof_bytes = 7 * sizeof(*entries);
		r.frame_bytes = 208 + r.proof_bytes;
		for (unsigned i = 0; i < 7; i++) {
			memset(&entries[i], 0, sizeof(entries[i]));
			entries[i].member_ordinal = i;
			entries[i].proof.member_status = MultiXactStatusForShare;
			entries[i].proof.member_ordinal = i;
			entries[i].proof.member_xid = 100 + i;
			entries[i].proof.state = CCM_COMMITTED;
			entries[i].proof.commit_scn = 1000 + i;
		}
		UT_ASSERT_EQ(cluster_multixact_current_proofs_accept(&op, MXCOMP_READ_OK, &r),
					 MXCOMP_PARTIAL);
		if (mode == 2) {
			range_release(RESOURCE_RELEASE_BEFORE_LOCKS, false, false, range_release_arg);
			op = NULL;
		} else {
			r.first = 7;
			r.count = 2;
			r.next = 9;
			r.last = true;
			r.proof_bytes = 2 * sizeof(*entries);
			r.frame_bytes = 208 + r.proof_bytes;
			for (unsigned i = 0; i < 2; i++) {
				memset(&entries[i], 0, sizeof(entries[i]));
				entries[i].member_ordinal = 7 + i;
				entries[i].proof.member_status = MultiXactStatusForShare;
				entries[i].proof.member_ordinal = i;
				entries[i].proof.member_xid = 107 + i;
				entries[i].proof.state = CCM_COMMITTED;
				entries[i].proof.commit_scn = 1007 + i;
			}
			if (mode == 0)
				entries[1].proof.state = CCM_UNKNOWN;
			if (mode == 1)
				fail_at = allocations + 1;
			PG_TRY();
			{
				UT_ASSERT_EQ(cluster_multixact_current_proofs_accept(&op, MXCOMP_READ_OK, &r),
							 mode == 3 ? MXCOMP_COMPLETE : MXCOMP_UNKNOWN);
			}
			PG_CATCH();
			{
				caught = true;
			}
			PG_END_TRY();
			fail_at = 0;
			UT_ASSERT_EQ(caught, mode == 1);
			if (mode == 3) {
				uint32 n = 0;
				UT_ASSERT_NOT_NULL(cluster_mx_composition_proofs(op, &n));
				UT_ASSERT_EQ(n, 9);
			} else
				UT_ASSERT_NULL(op);
			cluster_multixact_current_describe_finish(&op);
		}
		range_release(RESOURCE_RELEASE_BEFORE_LOCKS, false, false, range_release_arg);
		chain_clean();
	}
}

static ClusterMxProofReply
proof_window(uint32 total, uint32 first, ClusterMxFullMemberProof *entries)
{
	ClusterMxProofReply reply = { 0 };
	uint32 count = Min(total - first, 7);
	reply.source = &proof_source;
	reply.source_bytes = sizeof(proof_source);
	reply.total = total; reply.first = first; reply.count = count;
	reply.next = first + count; reply.last = reply.next == total;
	reply.proofs = entries; reply.proof_bytes = count * sizeof(*entries);
	reply.frame_bytes = 208 + reply.proof_bytes;
	for (uint32 i = 0; i < count; i++) {
		memset(&entries[i], 0, sizeof(entries[i]));
		entries[i].member_ordinal = first + i;
		entries[i].proof.member_xid = 100 + first + i;
		entries[i].proof.member_status = MultiXactStatusForShare;
		entries[i].proof.member_ordinal = i;
		entries[i].proof.state = CCM_COMMITTED;
		entries[i].proof.commit_scn = 1000 + first + i;
	}
	return reply;
}

static void
proof_scope_drift(bool during, bool error)
{
	static ClusterMxCompositionReceive *op;
	static ClusterMxCompositionReceiveStats stats;
	const uint32 sizes[] = { 2, 257 };
	for (unsigned s = 0; s < lengthof(sizes); s++)
		for (unsigned tail = 0; tail < 2; tail++)
			for (unsigned kind = error ? 6 : 1; kind <= (error ? 6 : 5); kind++) {
				ClusterCurrentMxKey key = { 0 };
				uint32 total = sizes[s];
				uint64 accepted = 0;
				unsigned network;
				bool caught = false;
				chain_seed(total, 1); op = NULL; memset(&stats, 0, sizeof(stats));
				key.multixact_id = 1000; key.cluster_epoch = 7;
				UT_ASSERT(cluster_multixact_current_describe_full(&key, &request_tag,
					&request_snapshot, &op, &stats));
				network = requests_sent;
				for (uint32 first = 0; first < total;) {
					ClusterMxFullMemberProof entries[7];
					ClusterMxProofReply reply = proof_window(total, first, entries);
					bool inject_now = tail ? reply.last : first == 0;
					ClusterMxCompositionReceiveResult result = MXCOMP_UNKNOWN;
					if (inject_now) {
						if (during) proof_allocation_fault = kind;
						else proof_drift(kind);
					}
					PG_TRY();
					{
						result = cluster_multixact_current_proofs_accept(&op, MXCOMP_READ_OK, &reply);
					}
					PG_CATCH();
					{
						caught = true;
					}
					PG_END_TRY();
					if (inject_now) {
						UT_ASSERT_EQ(caught, error);
						UT_ASSERT_EQ(result, MXCOMP_UNKNOWN);
						UT_ASSERT_NULL(op);
						UT_ASSERT_EQ(proof_allocation_fired, during);
						if (!error) {
							UT_ASSERT_EQ(stats.accepted_proof_frames, accepted + (during ? 1 : 0));
							UT_ASSERT_EQ(stats.unknown_results, 1);
							UT_ASSERT_EQ(stats.restart_requests, 1);
						}
						break;
					}
					UT_ASSERT(!caught); UT_ASSERT_EQ(result, MXCOMP_PARTIAL);
					accepted++; first = reply.next;
				}
				proof_catalog_error = false;
				UT_ASSERT_EQ(requests_sent, network);
				cluster_multixact_current_describe_finish(&op);
				range_release(RESOURCE_RELEASE_BEFORE_LOCKS, false, false, range_release_arg);
				chain_clean();
			}
}

static void caller_proof_scope_drift_before_window(void) { proof_scope_drift(false, false); }
static void caller_proof_scope_drift_after_window(void) { proof_scope_drift(true, false); }
static void caller_proof_scope_provider_error_cleanup(void)
{
	proof_scope_drift(false, true);
	proof_scope_drift(true, true);
}

static void
caller_proof_scope_stable_and_explicit_retry(void)
{
	const uint32 sizes[] = { 2, 257 };
	for (unsigned s = 0; s < lengthof(sizes); s++) {
		ClusterCurrentMxKey key = { 0 };
		ClusterMxCompositionReceive *op = NULL;
		ClusterMxCompositionReceiveStats stats = { 0 };
		uint32 total = sizes[s], n = 0, seen = 0;
		const ClusterMxCompositionProofPage *page;
		chain_seed(total, 1); key.multixact_id = 1000; key.cluster_epoch = 7;
		UT_ASSERT(cluster_multixact_current_describe_full(&key, &request_tag,
			&request_snapshot, &op, &stats));
		connection_generation++;
		{
			ClusterMxFullMemberProof entries[7];
			ClusterMxProofReply reply = proof_window(total, 0, entries);
			UT_ASSERT_EQ(cluster_multixact_current_proofs_accept(&op, MXCOMP_READ_OK, &reply), MXCOMP_UNKNOWN);
			UT_ASSERT_NULL(op);
		}
		UT_ASSERT(cluster_multixact_current_describe_full(&key, &request_tag,
			&request_snapshot, &op, &stats));
		UT_ASSERT_EQ(first_history[0], 0);
		UT_ASSERT_EQ(first_history[total > 256 ? 2 : 1], 0);
		UT_ASSERT_EQ(requests_sent, total > 256 ? 4 : 2);
		for (uint32 first = 0; first < total;) {
			ClusterMxFullMemberProof entries[7];
			ClusterMxProofReply reply = proof_window(total, first, entries);
			UT_ASSERT_EQ(cluster_multixact_current_proofs_accept(&op, MXCOMP_READ_OK, &reply),
				reply.last ? MXCOMP_COMPLETE : MXCOMP_PARTIAL);
			first = reply.next;
		}
		page = cluster_mx_composition_proofs(op, &n); UT_ASSERT_EQ(n, total);
		for (; page != NULL; page = page->next)
			for (uint32 j = 0; j < page->count; j++, seen++)
				UT_ASSERT_EQ(page->proofs[j].member_ordinal, seen);
		UT_ASSERT_EQ(seen, total);
		UT_ASSERT_EQ(stats.unknown_results, 1); UT_ASSERT_EQ(stats.restart_requests, 1);
		cluster_multixact_current_describe_finish(&op); chain_clean();
	}
}

/* The existing native/GCS/inline/G2 chain supplies the source and operation.
 * ASK generation itself sends no DATA and performs no CTRC proof-touch. */
static ClusterMxRangeWireContext
proof_request_context(void)
{
	ClusterMxRangeWireContext c = {0};
	c.request_id = 901; c.epoch = 7; c.key.multixact_id = 1000; c.key.cluster_epoch = 7;
	c.format_id = CLUSTER_FOREIGN_MX_FORMAT_ID_V1;
	c.version = CLUSTER_FOREIGN_MX_FORMAT_VERSION_V1;
	c.loaded_catalog = 202611010; c.peer_generation = connection_generation;
	c.peer_capabilities = CLUSTER_FOREIGN_MX_REQUIRED_CAPABILITIES;
	c.requested_cap = 7; c.source = &proof_source; c.source_bytes = sizeof(proof_source);
	return c;
}

static void
caller_full_proof_asks_follow_accepted_windows(void)
{
	const uint32 sizes[] = {2, 256, 257, 65537};
	for (unsigned k = 0; k < lengthof(sizes); k++) {
		ClusterCurrentMxKey key = {0}; ClusterMxCompositionReceive *op = NULL;
		ClusterMxCompositionReceiveStats stats = {0}; ClusterMxRangeWireContext c;
		unsigned network, allocated; uint32 first = 0;
		chain_seed(sizes[k], MULTIXACT_MEMBERS_PER_PAGE - 1);
		key.multixact_id = 1000; key.cluster_epoch = 7;
		UT_ASSERT(cluster_multixact_current_describe_full(&key, &request_tag, &request_snapshot, &op, &stats));
		c = proof_request_context(); network = requests_sent;
		while (first < sizes[k]) {
			PGAlignedBlock frame, duplicate;
			ClusterMxRangeProofAsk asks[7]; ClusterMxRangeWireHeader header;
			ClusterMxFullMemberProof entries[7]; ClusterMxProofReply r;
			uint32 bytes = 99, copied = 99, count = 99, total = 99;
			bool encoded;
			allocated = allocations;
			encoded = cluster_multixact_current_proofs_request(&op, &c, frame.data, BLCKSZ, &bytes);
			UT_ASSERT(encoded);
			if (!encoded) break;
			UT_ASSERT_EQ(allocations, allocated);
			encoded = cluster_mx_range_wire_decode_proof_asks(frame.data, bytes, &c, asks, 7, &count, &total);
			UT_ASSERT(encoded);
			if (!encoded) break;
			UT_ASSERT_EQ(total, sizes[k]); UT_ASSERT_EQ(count, Min(sizes[k] - first, 7));
			memcpy(&header, frame.data, sizeof(header));
			UT_ASSERT_EQ(header.first, first); UT_ASSERT_EQ(header.next, first + count);
			for (uint32 i = 0; i < count; i++) {
				UT_ASSERT_EQ(asks[i].member_ordinal, first + i);
				UT_ASSERT_EQ(asks[i].xid, 100 + first + i);
				UT_ASSERT_EQ(asks[i].member_status, MultiXactStatusForShare);
			}
			/* Encoding is not progress. Only complete proof acceptance advances. */
			UT_ASSERT(cluster_multixact_current_proofs_request(&op, &c, duplicate.data, BLCKSZ, &copied));
			UT_ASSERT_EQ(copied, bytes); UT_ASSERT_EQ(memcmp(frame.data, duplicate.data, bytes), 0);
			UT_ASSERT_EQ(stats.accepted_proof_frames, first / 7);
			r = proof_window(sizes[k], first, entries);
			UT_ASSERT_EQ(cluster_multixact_current_proofs_accept(&op, MXCOMP_READ_OK, &r),
				r.last ? MXCOMP_COMPLETE : MXCOMP_PARTIAL);
			first += count; c.request_id++;
		}
		UT_ASSERT_EQ(requests_sent, network); UT_ASSERT_EQ(replies_sent, network);
		UT_ASSERT_EQ(first, sizes[k]);
		UT_ASSERT_EQ(stats.accepted_proof_frames, (sizes[k] + 6) / 7);
		UT_ASSERT_EQ(stats.unknown_results, 0);
		cluster_multixact_current_describe_finish(&op); chain_clean();
	}
}

static void
caller_proof_ask_context_is_bound_to_original_operation(void)
{
	for (unsigned mode = 0; mode < 12; mode++) {
		ClusterCurrentMxKey key = {0}; ClusterMxCompositionReceive *op = NULL;
		ClusterMxCompositionReceiveStats stats = {0}; ClusterMxRangeWireContext c;
		ClusterMxRangeSource changed; PGAlignedBlock out, unchanged; uint32 bytes = 99;
		chain_seed(257, 1); key.multixact_id = 1000; key.cluster_epoch = 7;
		UT_ASSERT(cluster_multixact_current_describe_full(&key, &request_tag, &request_snapshot, &op, &stats));
		c = proof_request_context(); changed = proof_source;
		switch (mode) {
		case 0: changed.admission_revision++; c.source = &changed; break;
		case 1: changed.snapshot.curcid++; c.source = &changed; break;
		case 2: changed.native.oldest_multi++; c.source = &changed; break;
		case 3: c.source_bytes--; break;
		case 4: c.key.multixact_id++; break;
		case 5: c.loaded_catalog++; break;
		case 6: c.peer_generation++; break;
		case 7: c.peer_capabilities &= ~CLUSTER_FOREIGN_MX_REQUIRED_CAPABILITIES; break;
		case 8: c.epoch++; c.key.cluster_epoch++; break;
		case 9: c.requested_cap = 0; break;
		case 10: c.requested_cap = 8; break;
		case 11: c.request_id = 0; break;
		}
		memset(out.data, 0xa5, BLCKSZ); unchanged = out;
		UT_ASSERT(!cluster_multixact_current_proofs_request(&op, &c, out.data, BLCKSZ, &bytes));
		UT_ASSERT_NULL(op); UT_ASSERT_EQ(bytes, 0); UT_ASSERT_EQ(memcmp(out.data, unchanged.data, BLCKSZ), 0);
		UT_ASSERT_EQ(stats.unknown_results, 1); UT_ASSERT_EQ(stats.restart_requests, 1);
		UT_ASSERT_EQ(requests_sent, 2); UT_ASSERT_EQ(replies_sent, 2);
		cluster_multixact_current_describe_finish(&op); chain_clean();
	}
}

static void
caller_proof_ask_scope_drift_discards_without_sending(void)
{
	for (unsigned kind = 1; kind <= 5; kind++) {
		ClusterCurrentMxKey key = {0}; ClusterMxCompositionReceive *op = NULL;
		ClusterMxCompositionReceiveStats stats = {0}; ClusterMxRangeWireContext c;
		PGAlignedBlock frame; uint32 bytes = 99;
		chain_seed(2, 1); key.multixact_id = 1000; key.cluster_epoch = 7;
		UT_ASSERT(cluster_multixact_current_describe_full(&key, &request_tag, &request_snapshot, &op, &stats));
		c = proof_request_context(); proof_drift(kind); memset(frame.data, 0xa5, BLCKSZ);
		UT_ASSERT(!cluster_multixact_current_proofs_request(&op, &c, frame.data, BLCKSZ, &bytes));
		UT_ASSERT_NULL(op); UT_ASSERT_EQ(bytes, 0); UT_ASSERT_EQ((uint8)frame.data[0], 0xa5);
		UT_ASSERT_EQ(requests_sent, 1); UT_ASSERT_EQ(stats.unknown_results, 1);
		UT_ASSERT_EQ(stats.restart_requests, 1);
		cluster_multixact_current_describe_finish(&op); chain_clean();
	}
}

static void
caller_proof_ask_late_scope_check_keeps_output_atomic(void)
{
	ClusterCurrentMxKey key = {0}; ClusterMxCompositionReceive *op = NULL;
	ClusterMxCompositionReceiveStats stats = {0}; ClusterMxRangeWireContext c;
	PGAlignedBlock out, unchanged; uint32 bytes = 99;
	chain_seed(2, 1); key.multixact_id = 1000; key.cluster_epoch = 7;
	UT_ASSERT(cluster_multixact_current_describe_full(&key, &request_tag, &request_snapshot, &op, &stats));
	c = proof_request_context(); proof_request_drift_at = 2;
	memset(out.data, 0xa5, BLCKSZ); unchanged = out;
	UT_ASSERT(!cluster_multixact_current_proofs_request(&op, &c, out.data, BLCKSZ, &bytes));
	UT_ASSERT_EQ(proof_request_checks, 2); UT_ASSERT_NULL(op); UT_ASSERT_EQ(bytes, 0);
	UT_ASSERT_EQ(memcmp(out.data, unchanged.data, BLCKSZ), 0);
	UT_ASSERT_EQ(stats.unknown_results, 1); UT_ASSERT_EQ(stats.restart_requests, 1);
	UT_ASSERT_EQ(requests_sent, 1);
	cluster_multixact_current_describe_finish(&op); chain_clean();
}

static void
caller_proof_ask_error_and_owner_release_cleanup(void)
{
	static ClusterMxCompositionReceive *op;
	static ClusterMxCompositionReceiveStats stats;
	static uint32 bytes;
	for (unsigned mode = 0; mode < 2; mode++) {
		ClusterCurrentMxKey key = {0}; ClusterMxRangeWireContext c;
		PGAlignedBlock frame; bool caught = false;
		chain_seed(2, 1); key.multixact_id = 1000; key.cluster_epoch = 7;
		bytes = 99;
		op = NULL; memset(&stats, 0, sizeof(stats));
		UT_ASSERT(cluster_multixact_current_describe_full(&key, &request_tag, &request_snapshot, &op, &stats));
		c = proof_request_context(); memset(frame.data, 0xa5, BLCKSZ);
		if (mode == 0) proof_catalog_error = true;
		else {
			UT_ASSERT(cluster_multixact_current_proofs_request(&op, &c, frame.data, BLCKSZ, &bytes));
			range_release(RESOURCE_RELEASE_BEFORE_LOCKS, false, false, range_release_arg);
			op = NULL; /* The released borrowed pointer must never be dereferenced. */
		}
		PG_TRY(); {
			UT_ASSERT(!cluster_multixact_current_proofs_request(&op, &c, frame.data, BLCKSZ, &bytes));
		} PG_CATCH(); { caught = true; } PG_END_TRY();
		proof_catalog_error = false;
		UT_ASSERT_EQ(caught, mode == 0); UT_ASSERT_NULL(op); UT_ASSERT_EQ(bytes, 0);
		range_release(RESOURCE_RELEASE_BEFORE_LOCKS, false, false, range_release_arg);
		UT_ASSERT_EQ(requests_sent, 1); chain_clean();
	}
}

static void
raw_proof_ask_requires_complete_set_and_positive_capacity(void)
{
	ClusterMxCompositionReceiveStats stats = {0}; ClusterMxCompositionReceive *op;
	ClusterCurrentMxMemberDesc members[256]; ClusterMxCompositionReply r;
	ClusterMxRangeWireContext c; PGAlignedBlock out, unchanged; uint32 bytes = 99;
	chain_seed(2, 1); op = begin(2, &stats); c = proof_request_context();
	c.source = source; c.source_bytes = sizeof(source);
	memset(out.data, 0xa5, BLCKSZ); unchanged = out;
	UT_ASSERT(!cluster_mx_composition_encode_proof_asks(op, &c, out.data, BLCKSZ, &bytes));
	UT_ASSERT_EQ(bytes, 0); UT_ASSERT_EQ(memcmp(out.data, unchanged.data, BLCKSZ), 0);
	r = reply(2, 0, 2, members);
	UT_ASSERT_EQ(cluster_mx_composition_accept(&op, MXCOMP_READ_OK, &r), MXCOMP_COMPLETE);
	UT_ASSERT(!cluster_mx_composition_encode_proof_asks(op, &c, out.data, 1, &bytes));
	UT_ASSERT_EQ(bytes, 0); UT_ASSERT_EQ(memcmp(out.data, unchanged.data, BLCKSZ), 0);
	UT_ASSERT(cluster_mx_composition_encode_proof_asks(op, &c, out.data, BLCKSZ, &bytes));
	UT_ASSERT_EQ(bytes, 168); /* 80 header +64 exact source +2*12 asks. */
	cluster_mx_composition_finish(&op); chain_clean();
}

static void
caller_proof_asks_skip_native_zero_slot_and_stop_after_complete(void)
{
	ClusterCurrentMxKey key = {0}; ClusterMxCompositionReceive *op = NULL;
	ClusterMxCompositionReceiveStats stats = {0}; ClusterMxRangeWireContext c;
	ClusterMxRangeProofAsk asks[7]; ClusterMxFullMemberProof entries[7];
	PGAlignedBlock frame; uint32 bytes = 99, count = 99, total = 99;
	ClusterMxProofReply r;
	chain_seed(2, UINT32_MAX - 1); key.multixact_id = 1000; key.cluster_epoch = 7;
	UT_ASSERT(cluster_multixact_current_describe_full(&key, &request_tag, &request_snapshot, &op, &stats));
	UT_ASSERT_EQ(proof_source.native.span, 3); c = proof_request_context(); c.requested_cap = 1;
	UT_ASSERT(cluster_multixact_current_proofs_request(&op, &c, frame.data, BLCKSZ, &bytes));
	UT_ASSERT(cluster_mx_range_wire_decode_proof_asks(frame.data, bytes, &c, asks, 7, &count, &total));
	UT_ASSERT_EQ(total, 2); UT_ASSERT_EQ(count, 1); UT_ASSERT_EQ(asks[0].member_ordinal, 0);
	UT_ASSERT_EQ(asks[0].xid, 100);
	/* Accept both real members in one original proof window. */
	r = proof_window(2, 0, entries);
	UT_ASSERT_EQ(cluster_multixact_current_proofs_accept(&op, MXCOMP_READ_OK, &r), MXCOMP_COMPLETE);
	memset(frame.data, 0xa5, BLCKSZ);
	UT_ASSERT(!cluster_multixact_current_proofs_request(&op, &c, frame.data, BLCKSZ, &bytes));
	UT_ASSERT_NULL(op); UT_ASSERT_EQ(bytes, 0); UT_ASSERT_EQ((uint8)frame.data[0], 0xa5);
	cluster_multixact_current_describe_finish(&op); chain_clean();
}

#ifndef K_MX_NATIVE_CALLER_MAIN
#define K_MX_NATIVE_CALLER_MAIN main
#endif
int K_MX_NATIVE_CALLER_MAIN(void)
{
	TopMemoryContext=AllocSetContextCreate(CurrentMemoryContext,"native caller",ALLOCSET_DEFAULT_SIZES);
	CurrentMemoryContext=TopMemoryContext;
	UT_PLAN(20);
	UT_RUN(caller_native_transport_sizes);
	UT_RUN(caller_native_transport_failure_cleanup);
	UT_RUN(scoped_outcomes_in_original_snapshot);UT_RUN(scoped_cancel_has_no_returned_outcome);
	UT_RUN(caller_rejects_epoch_after_last_reply);UT_RUN(caller_rejects_control_change_after_last_reply);
	UT_RUN(scoped_last_reply_refusal_has_one_outcome);
	UT_RUN(caller_fresh_retry_starts_at_zero);
	UT_RUN(caller_proof_windows_keep_original_owner);
	UT_RUN(caller_full_proof_asks_follow_accepted_windows);
	UT_RUN(caller_proof_ask_context_is_bound_to_original_operation);
	UT_RUN(caller_proof_ask_scope_drift_discards_without_sending);
	UT_RUN(caller_proof_ask_late_scope_check_keeps_output_atomic);
	UT_RUN(caller_proof_ask_error_and_owner_release_cleanup);
	UT_RUN(raw_proof_ask_requires_complete_set_and_positive_capacity);
	UT_RUN(caller_proof_asks_skip_native_zero_slot_and_stop_after_complete);
	UT_RUN(caller_proof_scope_drift_before_window);
	UT_RUN(caller_proof_scope_drift_after_window);
	UT_RUN(caller_proof_scope_provider_error_cleanup);
	UT_RUN(caller_proof_scope_stable_and_explicit_retry);
	UT_DONE();
	return ut_failed_count==0?0:1;
}
