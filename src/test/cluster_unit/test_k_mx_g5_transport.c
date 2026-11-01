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

uint64 cluster_epoch_get_current(void) { return epoch; }
uint32 GetControlFileCatalogVersion(void) { return CATALOG_VERSION_NO; }
const ClusterICMsgTypeInfo *cluster_ic_get_msg_type_info(uint8 type)
{
	static ClusterICMsgTypeInfo data = {.plane=CLUSTER_IC_PLANE_DATA};
	UT_ASSERT_EQ(type,PGRAC_IC_MSG_GCS_BLOCK_REPLY); return &data;
}
bool cluster_sf_peer_foreign_mx_capability_sample_v1(int32 peer,uint32 format,
	uint32 version,uint32 catalog,uint32 *out)
{
	UT_ASSERT_EQ(peer,0);UT_ASSERT(cluster_foreign_mx_format_registered_v1(format,version,catalog));
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
int main(void)
{
	UT_PLAN(5);UT_RUN(actual_fetch);UT_RUN(wait_is_not_timeout);
	UT_RUN(failed_attempts_release_slot);UT_RUN(reply_domain_and_identity);
	UT_RUN(legacy_reply_magic_is_not_new_domain);UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
