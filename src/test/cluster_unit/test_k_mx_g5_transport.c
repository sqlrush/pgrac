/* The original GCS slot layout, reservation, wait and reply landing. Only
 * CONTROL, DATA enqueue, locks/CVs and cancellation are controlled leaves. */
#include "../../backend/cluster/cluster_gcs_block.c"
#include "unit_test.h"

UT_DEFINE_GLOBALS();
int cluster_node_id = 1, MaxBackends = 1, cluster_lms_workers = 2;
BackendId MyBackendId = 1;
volatile sig_atomic_t InterruptPending;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
static ClusterGcsBlockBackendBlock block;
static ClusterMxRangeRequest request;
static ClusterMxRangeResponse outbound_reply;
static ClusterICEnvelope envelope;
static uint64 epoch = 7;
static uint32 generation = 11;
static unsigned locks, sends, sleeps, wakes, cancels, injection;
static bool proof_mode;
static int32 proof_origin;
static unsigned proof_fault, routing_fault;
static bool proof_enqueue(int worker, uint8 type, uint32 peer, const void *data,
	uint16 bytes, uint32 caps, uint32 bound);

uint64 cluster_epoch_get_current(void) { return epoch; }
uint32 GetControlFileCatalogVersion(void) { return 202611010; }
const ClusterICMsgTypeInfo *cluster_ic_get_msg_type_info(uint8 type)
{
	static ClusterICMsgTypeInfo data = {.plane=CLUSTER_IC_PLANE_DATA};
	UT_ASSERT_EQ(type,PGRAC_IC_MSG_GCS_BLOCK_REPLY); return &data;
}
bool cluster_sf_peer_foreign_mx_capability_sample_v1(int32 peer,uint32 format,
	uint32 version,uint32 catalog,uint32 *out)
{
	UT_ASSERT_EQ(peer,proof_mode ? proof_origin : 0);UT_ASSERT(cluster_foreign_mx_format_registered_v1(format,version,catalog));
	*out=generation;return generation!=0;
}
bool cluster_sf_peer_foreign_mx_capability_matches_v1(int32 peer,uint32 format,
	uint32 version,uint32 catalog,uint32 expected)
{
	uint32 actual;return cluster_sf_peer_foreign_mx_capability_sample_v1(peer,format,version,catalog,&actual)
		&& actual==expected;
}
void cluster_gcs_block_dedup_register_backend_exit_hook(void) { }
bool LWLockAcquire(LWLock *l,LWLockMode m) { (void)l;(void)m;UT_ASSERT_EQ(locks,0);locks++;return true; }
void LWLockRelease(LWLock *l) { (void)l;UT_ASSERT_EQ(locks,1);locks--; }
void ConditionVariablePrepareToSleep(ConditionVariable *cv) { (void)cv; }
bool ConditionVariableCancelSleep(void) { cancels++;return false; }
void ConditionVariableBroadcast(ConditionVariable *cv) { (void)cv;wakes++; }
int cluster_lms_shard_for_tag(const BufferTag *tag,int workers)
{ UT_ASSERT_EQ(workers,2);return tag->blockNum%workers; }
void ProcessInterrupts(void) { UT_ASSERT_EQ(locks,0);siglongjmp(*PG_exception_stack,1); }
void pg_re_throw(void) { if(PG_exception_stack)siglongjmp(*PG_exception_stack,1);abort(); }
void ExceptionalCondition(const char *c,const char *f,int l) { (void)c;(void)f;(void)l;abort(); }
bool errstart(int level,const char *domain) { (void)level;(void)domain;return false; }
bool errstart_cold(int level,const char *domain) { return errstart(level,domain); }
int errmsg_internal(const char *fmt,...) { (void)fmt;return 0; }
int errmsg(const char *fmt,...) { (void)fmt;return 0; }
int errcode(int code) { return code; }
int errdetail(const char *fmt,...) { (void)fmt;return 0; }
void errfinish(const char *f,int l,const char *fn) { (void)f;(void)l;(void)fn;abort(); }

bool cluster_lms_outbound_enqueue_cap_bound(int worker,uint8 type,uint32 peer,
	const void *data,uint16 bytes,uint32 caps,uint32 bound)
{
	const ClusterMxRangeRequest *r=data;
	BufferTag tag=GcsBlockCurrentMxRouteTagMake(r->route.request_id,r->route.epoch,
		r->route.requester_node,r->route.requester_backend);
	uint32 reply_bytes=0;
	if (proof_mode) return proof_enqueue(worker,type,peer,data,bytes,caps,bound);
	UT_ASSERT_EQ(locks,0);UT_ASSERT_EQ(worker,cluster_lms_shard_for_tag(&tag,2));
	UT_ASSERT_EQ(type,PGRAC_IC_MSG_GCS_BLOCK_FORWARD);UT_ASSERT_EQ(peer,0);
	UT_ASSERT_EQ(bytes,sizeof(*r));UT_ASSERT_EQ(caps,CLUSTER_FOREIGN_MX_REQUIRED_CAPABILITIES);
	UT_ASSERT_EQ(bound,11);sends++;
	UT_ASSERT(cluster_mx_range_refuse(r,GetControlFileCatalogVersion(),&outbound_reply,
		sizeof(outbound_reply),&reply_bytes));
	memset(&envelope,0,sizeof(envelope));envelope.msg_type=PGRAC_IC_MSG_GCS_BLOCK_REPLY;
	envelope.source_node_id=0;envelope.dest_node_id=1;envelope.payload_length=reply_bytes;
	if(injection==1)return false;
	if(injection==2)generation++;
	if(injection==3)epoch++;
	if(injection==4) { InterruptPending=true;return true; }
	if(injection==5)return true; /* many diagnostic wakes before a real reply */
	UT_ASSERT(gcs_block_try_land_full_mx_reply(&envelope,&outbound_reply));
	if(injection==6)block.slots[0].stale=true;
	return true;
}

bool ConditionVariableTimedSleep(ConditionVariable *cv,long timeout,uint32 event)
{
	(void)cv;(void)event;UT_ASSERT_EQ(timeout,100);UT_ASSERT_EQ(locks,0);sleeps++;
	UT_ASSERT_EQ(injection,5);
	if(sleeps==20)UT_ASSERT(gcs_block_try_land_full_mx_reply(&envelope,&outbound_reply));
	UT_ASSERT(sleeps<=20);return true;
}

static void seed(void)
{
	memset(&block,0,sizeof(block));gcs_block_backend_blocks=&block;block.next_request_id=1;
	memset(&request,0,sizeof(request));request.route.epoch=7;
	request.route.requester_node=1;request.route.requester_backend=1;
	request.key.multixact_id=1000;request.key.cluster_epoch=7;
	request.target.spcOid=1663;request.target.dbOid=5;request.target.relNumber=8;
	request.cap=256;request.peer_generation=11;
	epoch=7;generation=11;locks=sends=sleeps=wakes=cancels=injection=0;InterruptPending=false;
	proof_mode=false;proof_origin=0;proof_fault=routing_fault=0;
}
static void slots_empty(void)
{
	for(unsigned i=0;i<lengthof(block.slots);i++)UT_ASSERT(!block.slots[i].in_use);
	UT_ASSERT_EQ(locks,0);
}
static void actual_fetch(void)
{
	ClusterMxRangeResponse response;uint32 bytes=0;
	seed();UT_ASSERT(cluster_gcs_current_mx_range_fetch_and_wait(&request,&response,&bytes));
	UT_ASSERT_EQ(bytes,response.route.wire_bytes);UT_ASSERT_EQ(sends,1);UT_ASSERT_EQ(sleeps,0);
	UT_ASSERT_EQ(response.route.kind,CLUSTER_MX_RANGE_RESPONSE_KIND);slots_empty();
}
static void wait_is_not_timeout(void)
{
	ClusterMxRangeResponse response;uint32 bytes=0;
	seed();injection=5;
	UT_ASSERT(cluster_gcs_current_mx_range_fetch_and_wait(&request,&response,&bytes));
	UT_ASSERT_EQ(sends,1);UT_ASSERT_EQ(sleeps,20);slots_empty();
}
static void failed_attempts_release_slot(void)
{
	unsigned modes[]={1,2,3,4,6};
	for(unsigned i=0;i<lengthof(modes);i++) {
		unsigned mode=modes[i];
		ClusterMxRangeResponse response;uint32 bytes=99;volatile bool caught=false;
		seed();injection=mode;
		PG_TRY(); {
			UT_ASSERT(!cluster_gcs_current_mx_range_fetch_and_wait(&request,&response,&bytes));
		} PG_CATCH(); { caught=true; } PG_END_TRY();
		UT_ASSERT_EQ(caught,mode==4);UT_ASSERT_EQ(bytes,0);UT_ASSERT_EQ(sends,1);slots_empty();
	}
}
static void reply_domain_and_identity(void)
{
	for(unsigned mode=0;mode<12;mode++) {
		uint64 id;uint32 bytes=0;ClusterGcsBlockOutstandingSlot *slot;
		seed();slot=gcs_block_try_reserve_current_mx_slot(&request.key,7,0,CLUSTER_MX_RANGE_RESPONSE_KIND,&id);
		UT_ASSERT_NOT_NULL(slot);request.route.request_id=id;cluster_mx_range_request_seal(&request);
		/* Mirror the original sender's published request, not a naked slot. */
		memcpy(slot->reply_block_data,&request,sizeof(request));
		UT_ASSERT(cluster_mx_range_refuse(&request,GetControlFileCatalogVersion(),&outbound_reply,sizeof(outbound_reply),&bytes));
		memset(&envelope,0,sizeof(envelope));envelope.msg_type=PGRAC_IC_MSG_GCS_BLOCK_REPLY;
		envelope.source_node_id=0;envelope.dest_node_id=1;envelope.payload_length=bytes;
		if(mode==1)envelope.source_node_id=2;
		if(mode==2)envelope.dest_node_id=0;
		if(mode==3)outbound_reply.route.requester_backend=2;
		if(mode==4)outbound_reply.route.request_id++;
		if(mode==5)slot->reply_domain=CLUSTER_GCS_BLOCK_REPLY_DOMAIN_CURRENT_MX;
		if(mode==6)slot->reply_domain=CLUSTER_GCS_BLOCK_REPLY_DOMAIN_LEGACY_ACQUIRE;
		if(mode==7)slot->reply_domain=CLUSTER_GCS_BLOCK_REPLY_DOMAIN_R4_CR;
		if(mode==8)slot->stale=true;
		if(mode==9)slot->direct_target_prepared=true;
		if(mode==10)slot->direct_state=GCS_BLOCK_DIRECT_ARMED;
		if(mode==11)outbound_reply.route.epoch++;
		cluster_mx_range_response_seal(&outbound_reply,bytes-sizeof(outbound_reply.route));
		UT_ASSERT(gcs_block_try_land_full_mx_reply(&envelope,&outbound_reply));
		UT_ASSERT_EQ(slot->reply_received,mode==0);
		slot->direct_target_prepared=false;gcs_block_release_slot(slot);slots_empty();
	}
}
static void legacy_reply_magic_is_not_new_domain(void)
{
	struct { GcsBlockReplyHeader header; PGAlignedBlock data; } legacy={0};
	ClusterICEnvelope env={0};
	seed();legacy.header.request_id=UINT64_C(0x014D584731);
	env.msg_type=PGRAC_IC_MSG_GCS_BLOCK_REPLY;env.payload_length=sizeof(legacy);
	UT_ASSERT(!gcs_block_try_land_full_mx_reply(&env,&legacy));
}

/* Native stripe routing is a hint here, never a FullXid ownership proof. */
int cluster_xid_origin_slot(TransactionId xid)
{
	if (routing_fault && xid==101) return routing_fault==1 ? -1 : 2;
	return xid >= 100 && xid < 200 ? proof_origin : -1;
}

static ClusterMxProofRequest proof_request;
static ClusterMxProofTransportStats proof_stats;
static uint32 proof_request_bytes;
static uint64 last_proof_id;

static void proof_seed(uint32 total,uint32 first,int32 origin)
{
	ClusterMxRangeSource source={0};ClusterMxRangeWireContext c={0};
	ClusterCurrentMxMemberDesc members[7]={{0}};PGAlignedBlock body;uint32 bytes=0;
	seed();proof_mode=true;proof_origin=origin;memset(&proof_stats,0,sizeof(proof_stats));last_proof_id=0;
	source.native.multi=1000;source.native.epoch=7;source.native.start=1;source.native.span=total;
	source.system_identifier=91;source.formation_epoch=7;source.boot_incarnation=71;
	source.admission_revision=3;source.capability_generation=111;
	source.target.spcOid=1663;source.target.dbOid=5;source.target.relNumber=8;
	source.snapshot.snapshot_type=SNAPSHOT_MVCC;source.snapshot.read_scn=17;source.snapshot.read_epoch=7;
	source.snapshot.xmin=40;source.snapshot.xmax=400;source.snapshot.curcid=9;
	c.request_id=99;c.epoch=7;c.key=request.key;c.format_id=CLUSTER_FOREIGN_MX_FORMAT_ID_V1;
	c.version=CLUSTER_FOREIGN_MX_FORMAT_VERSION_V1;c.loaded_catalog=202611010;
	c.peer_capabilities=CLUSTER_FOREIGN_MX_REQUIRED_CAPABILITIES;c.peer_generation=11;
	c.source=&source;c.source_bytes=sizeof(source);c.requested_cap=7;
	for(unsigned i=0;i<7;i++){members[i].xid=100+i;members[i].member_status=MultiXactStatusForShare;}
	UT_ASSERT(cluster_mx_range_wire_encode_proof_asks(&c,total,first,members,Min(7,total-first),body.data,BLCKSZ,&bytes));
	UT_ASSERT(cluster_mx_proof_request_pack(&c,1,1,body.data,bytes,&proof_request,sizeof(proof_request),&proof_request_bytes));
}

static bool proof_enqueue(int worker,uint8 type,uint32 peer,const void *data,
	uint16 bytes,uint32 caps,uint32 bound)
{
	const ClusterMxProofRequest *r=data;ClusterMxProofRequestView v;
	ClusterMxRangeWireContext c={0};ClusterMxFullMemberProof entries[7]={{0}};
	ClusterMxProofReply reply={0};uint32 n=0;BufferTag tag;
	UT_ASSERT_EQ(locks,0);UT_ASSERT_EQ(type,PGRAC_IC_MSG_GCS_BLOCK_FORWARD);UT_ASSERT_EQ(peer,proof_origin);
	UT_ASSERT_EQ(caps,CLUSTER_FOREIGN_MX_REQUIRED_CAPABILITIES);UT_ASSERT_EQ(bound,11);sends++;
	UT_ASSERT(cluster_mx_proof_request_decode(r,bytes,202611010,caps,&v));
	tag=GcsBlockCurrentMxRouteTagMake(v.route.request_id,7,1,1);
	UT_ASSERT_EQ(worker,cluster_lms_shard_for_tag(&tag,2));UT_ASSERT(v.route.request_id!=last_proof_id);
	last_proof_id=v.route.request_id;
	/* The actual sender must publish the full ASK before enqueue. */
	UT_ASSERT_EQ(memcmp(block.slots[0].reply_block_data,r,bytes),0);
	UT_ASSERT_EQ(block.slots[0].reply_domain,CLUSTER_GCS_BLOCK_REPLY_DOMAIN_FULL_MX);
	c.request_id=v.route.request_id;c.epoch=7;c.key=v.key;c.format_id=v.route.format_id;c.version=v.route.version;
	c.loaded_catalog=202611010;c.peer_capabilities=caps;c.peer_generation=v.peer_generation;c.requested_cap=v.count;
	c.source=&v.source;c.source_bytes=sizeof(v.source);
	for(unsigned i=0;i<v.count;i++){
		entries[i].member_ordinal=v.first+i;entries[i].origin_node_id=peer;
		entries[i].proof.member_xid=v.asks[i].xid;entries[i].proof.member_status=v.asks[i].member_status;
		entries[i].proof.member_ordinal=i;entries[i].proof.state=CCM_ABORTED;
	}
	reply.source=&v.source;reply.source_bytes=sizeof(v.source);reply.total=v.total;reply.first=v.first;
	reply.count=v.count;reply.next=v.next;reply.last=v.next==v.total;reply.proofs=entries;
	reply.proof_bytes=v.count*sizeof(*entries);reply.frame_bytes=BLCKSZ;
	UT_ASSERT(cluster_mx_proof_response_pack(&v,&c,proof_fault==12?MXCOMP_READ_UNKNOWN:MXCOMP_READ_OK,
		proof_fault==12?NULL:&reply,&outbound_reply,sizeof(outbound_reply),&n));
	memset(&envelope,0,sizeof(envelope));envelope.msg_type=PGRAC_IC_MSG_GCS_BLOCK_REPLY;
	envelope.source_node_id=peer;envelope.dest_node_id=1;envelope.payload_length=n;
	if(proof_fault==1)return false;
	if(proof_fault==2)generation++;
	if(proof_fault==3)epoch++;
	if(proof_fault==4){InterruptPending=true;return true;}
	if(proof_fault==5){injection=5;return true;}
	if(proof_fault>=6 && proof_fault<=11){
		ClusterMxRangeResponse bad=outbound_reply;
		ClusterMxRangeWireHeader *h=(ClusterMxRangeWireHeader *)bad.data.data;
		if(proof_fault==6)h->kind=CLUSTER_MX_RANGE_REPLY;
		if(proof_fault==7)h->peer_generation++;
		if(proof_fault==8)((ClusterMxRangeSource *)(bad.data.data+sizeof(*h)))->boot_incarnation++;
		if(proof_fault==9)((ClusterMxFullMemberProof *)(bad.data.data+sizeof(*h)+sizeof(ClusterMxRangeSource)))->proof.member_xid++;
		if(proof_fault==10)((ClusterMxFullMemberProof *)(bad.data.data+sizeof(*h)+sizeof(ClusterMxRangeSource)))->origin_node_id++;
		if(proof_fault==11)h->first++;
		cluster_mx_range_response_seal(&bad,n-sizeof(bad.route));
		UT_ASSERT(gcs_block_try_land_full_mx_reply(&envelope,&bad));
		UT_ASSERT(!block.slots[0].reply_received);
	}
	UT_ASSERT(gcs_block_try_land_full_mx_reply(&envelope,&outbound_reply));
	return true;
}

static void proof_full_width_and_member_origin(void)
{
	const uint32 totals[]={2,257,65537},firsts[]={0,0,65534};
	for(unsigned i=0;i<3;i++)for(int32 peer=0;peer<=2;peer+=2){
		ClusterMxRangeResponse response;uint32 bytes=0;
		proof_seed(totals[i],firsts[i],peer);
		UT_ASSERT(cluster_gcs_current_mx_proofs_fetch_and_wait(peer,&proof_request,proof_request_bytes,&response,&bytes,&proof_stats));
		UT_ASSERT_EQ(bytes,i==0?656:i==1?1656:856);UT_ASSERT_EQ(sends,1);UT_ASSERT_EQ(proof_stats.attempts,1);
		UT_ASSERT_EQ(proof_stats.enqueued,1);UT_ASSERT_EQ(proof_stats.replies,1);
		UT_ASSERT_EQ(proof_stats.request_bytes,i==0?280:i==1?340:292);UT_ASSERT_EQ(proof_stats.reply_bytes,bytes);
		UT_ASSERT_EQ(((ClusterMxRangeWireHeader *)response.data.data)->first,firsts[i]);
		UT_ASSERT_EQ(proof_request.route.request_id,last_proof_id);slots_empty();
	}
}
static void proof_bad_reply_does_not_win_slot(void)
{
	for(unsigned i=6;i<=11;i++){
		ClusterMxRangeResponse response;uint32 bytes=0;
		proof_seed(257,0,2);proof_fault=i;
		UT_ASSERT(cluster_gcs_current_mx_proofs_fetch_and_wait(2,&proof_request,proof_request_bytes,&response,&bytes,&proof_stats));
		UT_ASSERT_EQ(sends,1);UT_ASSERT_EQ(proof_stats.replies,1);slots_empty();
	}
}
static void proof_attempt_failure_and_cancel_cleanup(void)
{
	for(unsigned i=1;i<=4;i++){
		static ClusterMxRangeResponse response;static uint32 bytes;volatile bool caught=false;
		proof_seed(257,0,0);proof_fault=i;bytes=99;memset(&response,0xa5,sizeof(response));
		PG_TRY();{
			UT_ASSERT(!cluster_gcs_current_mx_proofs_fetch_and_wait(0,&proof_request,proof_request_bytes,&response,&bytes,&proof_stats));
		}PG_CATCH();{caught=true;}PG_END_TRY();
		UT_ASSERT_EQ(caught,i==4);UT_ASSERT_EQ(bytes,0);UT_ASSERT_EQ(sends,1);UT_ASSERT_EQ(proof_stats.attempts,1);
		UT_ASSERT_EQ(proof_stats.enqueued,i!=1);
		for(unsigned j=0;j<sizeof(response);j++)UT_ASSERT_EQ(((uint8 *)&response)[j],0);
		InterruptPending=false;slots_empty();
	}
}
static void proof_wait_and_unknown_are_not_success(void)
{
	for(unsigned i=0;i<2;i++){
		ClusterMxRangeResponse response;uint32 bytes=0;
		proof_seed(257,0,0);proof_fault=i?12:5;
		UT_ASSERT(cluster_gcs_current_mx_proofs_fetch_and_wait(0,&proof_request,proof_request_bytes,&response,&bytes,&proof_stats));
		UT_ASSERT_EQ(sends,1);UT_ASSERT_EQ(sleeps,i?0:20);
		UT_ASSERT_EQ(((ClusterMxRangeWireHeader *)response.data.data)->result,i?MXCOMP_READ_UNKNOWN:MXCOMP_READ_OK);
		UT_ASSERT_EQ(bytes,i?256:1656);slots_empty();
	}
}
static void proof_input_and_counter_overflow_no_send(void)
{
	for(unsigned i=0;i<7;i++){
		ClusterMxRangeResponse response;uint32 bytes=99;
		proof_seed(257,0,0);
		if(i==0)proof_request.route.checksum++;
		if(i==1)generation++;
		if(i==2)proof_stats.attempts=UINT64_MAX;
		if(i==3)proof_origin=1;
		if(i==4)proof_origin=-1;
		if(i>=5)routing_fault=i-4;
		UT_ASSERT(!cluster_gcs_current_mx_proofs_fetch_and_wait(proof_origin,&proof_request,proof_request_bytes,&response,&bytes,&proof_stats));
		UT_ASSERT_EQ(sends,0);UT_ASSERT_EQ(bytes,0);slots_empty();
	}
}
static void proof_fresh_attempt_ids_and_counters(void)
{
	ClusterMxRangeResponse response;uint32 bytes=0;uint64 id;
	proof_seed(257,0,0);
	UT_ASSERT(cluster_gcs_current_mx_proofs_fetch_and_wait(0,&proof_request,proof_request_bytes,&response,&bytes,&proof_stats));id=last_proof_id;
	UT_ASSERT(cluster_gcs_current_mx_proofs_fetch_and_wait(0,&proof_request,proof_request_bytes,&response,&bytes,&proof_stats));
	UT_ASSERT(id!=last_proof_id);UT_ASSERT_EQ(sends,2);UT_ASSERT_EQ(proof_stats.attempts,2);
	UT_ASSERT_EQ(proof_stats.enqueued,2);UT_ASSERT_EQ(proof_stats.replies,2);UT_ASSERT_EQ(proof_stats.request_bytes,680);
	UT_ASSERT_EQ(proof_stats.reply_bytes,3312);slots_empty();
}

int main(void)
{
	UT_PLAN(11);UT_RUN(actual_fetch);UT_RUN(wait_is_not_timeout);
	UT_RUN(failed_attempts_release_slot);UT_RUN(reply_domain_and_identity);
	UT_RUN(legacy_reply_magic_is_not_new_domain);
	UT_RUN(proof_full_width_and_member_origin);UT_RUN(proof_bad_reply_does_not_win_slot);
	UT_RUN(proof_attempt_failure_and_cancel_cleanup);UT_RUN(proof_wait_and_unknown_are_not_success);
	UT_RUN(proof_input_and_counter_overflow_no_send);UT_RUN(proof_fresh_attempt_ids_and_counters);UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
