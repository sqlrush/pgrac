/* Actual native range -> holder service -> caller -> G2. The transport,
 * control/KO providers and ResourceOwner dispatcher are controlled leaves. */
int k_native_component_main(void);
#pragma push_macro("main")
#undef main
#define main k_native_component_main
#include "test_cluster_mx_native_range.c"
#undef main
#pragma pop_macro("main")
#include "cluster/cluster_multixact_range.h"
#include "cluster/cluster_ko.h"
#include "utils/resowner.h"
#include "miscadmin.h"

MemoryContext TopMemoryContext;
ResourceOwner CurrentResourceOwner = (ResourceOwner)1;
ResourceOwner AuxProcessResourceOwner;
volatile sig_atomic_t InterruptPending;
void ProcessInterrupts(void) { pg_re_throw(); }
static ResourceReleaseCallback range_release;
static void *range_release_arg;
static uint64 service_system = 91, service_boot = 71, ko_revision = 3;
static uint32 connection_generation = 11;
static bool admission_ok = true, capability_ok = true, serving_ok = true;
static unsigned io_live, frame_calls, inject;
static unsigned legacy_calls;
static ClusterMxRangeSnapshot request_snapshot;
static BufferTag request_tag;

uint64 GetSystemIdentifier(void) { return service_system; }
uint32 GetControlFileCatalogVersion(void) { return CATALOG_VERSION_NO; }
uint64 cluster_qvotec_get_self_incarnation(void) { return service_boot; }
bool cluster_normal_stop_service_new_work(bool writes) { UT_ASSERT(!writes); return serving_ok; }
bool cluster_write_fence_enforcing(void) { return true; }
bool cluster_write_fence_allowed(void) { return serving_ok; }
int cluster_mxid_origin_slot(MultiXactId multi) { (void)multi; return 0; }
bool cluster_sf_peer_foreign_mx_capability_sample_v1(int32 peer, uint32 format,
	uint32 version, uint32 catalog, uint32 *generation)
{
	(void)peer; *generation = 0;
	if (!capability_ok || !cluster_foreign_mx_format_registered_v1(format,version,catalog)) return false;
	*generation = connection_generation + (cluster_node_id==0 ? 100 : 0); return true;
}
bool cluster_sf_peer_foreign_mx_capability_matches_v1(int32 peer, uint32 format,
	uint32 version, uint32 catalog, uint32 generation)
{
	uint32 actual;
	return cluster_sf_peer_foreign_mx_capability_sample_v1(peer,format,version,catalog,&actual)
		&& actual == generation;
}
bool cluster_ko_peer_admission_begin_v3(RelFileLocator locator, ClusterKoPeerAdmissionV3 *out)
{
	memset(out,0,sizeof(*out)); if (!admission_ok) return false;
	out->locator=locator; out->revision=ko_revision; return true;
}
bool cluster_ko_peer_admission_current_v3(const ClusterKoPeerAdmissionV3 *a, RelFileLocator l)
{
	if (inject==11 && held[0] && held[1] && member_reads>0) return false;
	return admission_ok && a->revision==ko_revision && RelFileLocatorEquals(a->locator,l);
}
bool cluster_ko_directory_io_begin_v3(RelFileLocator l, ClusterKoDirectoryIoV3 *out)
{
	(void)l; memset(out,0,sizeof(*out)); if (!admission_ok) return false;
	out->serial=++io_live; return true;
}
bool cluster_ko_directory_io_end_v3(const ClusterKoDirectoryIoV3 *io)
{
	if(io->serial) { UT_ASSERT(io_live>0); io_live--; } return true;
}
void RegisterResourceReleaseCallback(ResourceReleaseCallback cb, void *arg)
{ range_release=cb; range_release_arg=arg; }
ClusterMxDescribeResult cluster_multixact_current_describe(const ClusterCurrentMxKey *key,
	ClusterCurrentMxMemberDesc *members,uint16 cap,uint16 *count,uint32 *total)
{
	(void)key;UT_ASSERT(cap>0);legacy_calls++;
	memset(members,0,sizeof(*members));members->xid=55;*count=1;*total=1;
	return CMX_DESC_OK;
}

/* One original transport attempt. Product callers still have to reject its
 * late/corrupt output. It invokes the real holder service, never fakes OK. */
bool cluster_gcs_current_mx_range_fetch_and_wait(ClusterMxRangeRequest *request,
	ClusterMxRangeResponse *response, uint32 *bytes)
{
	int saved_node = cluster_node_id;
	BackendId saved_backend = MyBackendId;
	bool ok;
	frame_calls++;
	request->route.request_id=1000+frame_calls;
	cluster_mx_range_request_seal(request);
	if (inject==1 && frame_calls==2) ko_revision++;
	if (inject==2 && frame_calls==2) service_boot++;
	if (inject==3 && frame_calls==2) service_system++;
	if (inject==4 && frame_calls==2) native_epoch++;
	if (inject==5 && frame_calls==2) connection_generation++;
	if (inject==6 && frame_calls==2) return false;
	cluster_node_id=0;
	MyBackendId=InvalidBackendId;
	PG_TRY();
	{
		ok=cluster_mx_range_serve(request,response,sizeof(*response),bytes);
	}
	PG_FINALLY();
	{
		cluster_node_id=saved_node;
		MyBackendId=saved_backend;
	}
	PG_END_TRY();
	if (inject==7 && frame_calls==2 && ok) response->data.data[sizeof(ClusterMxRangeWireHeader)]^=1;
	if (inject==8 && ok) response->route.request_id++;
	if (inject==9 && ok) response->route.requester_backend++;
	if (inject==10 && ok) capability_ok=false;
	if (inject==12 && frame_calls==1 && ok) InterruptPending=true;
	return ok;
}

static void service_seed(uint32 n, uint32 start)
{
	native_seed(n,start,1000); cluster_node_id=1;
	MyBackendId=1;
	InterruptPending=false;
	service_system=91;service_boot=71;ko_revision=3;connection_generation=11;
	admission_ok=capability_ok=serving_ok=true;io_live=frame_calls=inject=legacy_calls=0;
	memset(&request_tag,0,sizeof(request_tag));
	request_tag.spcOid=1663;request_tag.dbOid=5;request_tag.relNumber=8;
	request_tag.forkNum=MAIN_FORKNUM;request_tag.blockNum=2;
	memset(&request_snapshot,0,sizeof(request_snapshot));
	request_snapshot.snapshot_type=SNAPSHOT_MVCC;
	request_snapshot.read_scn=17;request_snapshot.read_epoch=7;
	request_snapshot.xmin=40;request_snapshot.xmax=400;request_snapshot.curcid=9;
}

static ClusterMxCompositionReceive *fetch(uint32 n)
{
	ClusterCurrentMxKey key={0}; ClusterMxCompositionReceive *op=NULL;
	ClusterMxCompositionReceiveStats stats={0};
	key.multixact_id=1000;key.cluster_epoch=7;
	UT_ASSERT(cluster_multixact_current_describe_full(&key,&request_tag,&request_snapshot,&op,&stats));
	UT_ASSERT_NOT_NULL(op);
	{
		uint32 total=0,seen=0;const ClusterMxCompositionPage *page=cluster_mx_composition_pages(op,&total);
		UT_ASSERT_EQ(total,n);
		for(;page;page=page->next) for(uint32 i=0;i<page->count;i++,seen++)
			UT_ASSERT_EQ(page->members[i].xid,100+seen);
		UT_ASSERT_EQ(seen,n);
	}
	UT_ASSERT_EQ(io_live,0);return op;
}

static void service_sizes(void)
{
	uint32 sizes[]={2,256,257,65537};
	for(unsigned i=0;i<lengthof(sizes);i++) {
		ClusterMxCompositionReceive *op;
		service_seed(sizes[i],MULTIXACT_MEMBERS_PER_PAGE-1);op=fetch(sizes[i]);
		cluster_multixact_current_describe_finish(&op);UT_ASSERT_NULL(op);
		UT_ASSERT_EQ(live_contexts,1);UT_ASSERT_EQ(live_blocks,0);
	}
}

static void service_later_failures(void)
{
	for(unsigned failure=1;failure<=11;failure++) {
		ClusterCurrentMxKey key={0};ClusterMxCompositionReceive *op=NULL;
		ClusterMxCompositionReceiveStats stats={0};
		service_seed(failure==11?2:257,1);inject=failure;key.multixact_id=1000;key.cluster_epoch=7;
		UT_ASSERT(!cluster_multixact_current_describe_full(&key,&request_tag,&request_snapshot,&op,&stats));
		UT_ASSERT_NULL(op);UT_ASSERT(frame_calls<=2);UT_ASSERT_EQ(io_live,0);
		UT_ASSERT_EQ(live_contexts,1);UT_ASSERT_EQ(live_blocks,0);
		UT_ASSERT_EQ(stats.unknown_results,1);UT_ASSERT_EQ(stats.restart_requests,1);
	}
}

static void service_refusals(void)
{
	for(unsigned failure=0;failure<3;failure++) {
		ClusterCurrentMxKey key={0};ClusterMxCompositionReceive *op=NULL;ClusterMxCompositionReceiveStats stats={0};
		service_seed(2,1);key.multixact_id=1000;key.cluster_epoch=7;
		if(failure==0) admission_ok=false;if(failure==1) capability_ok=false;if(failure==2) serving_ok=false;
		UT_ASSERT(!cluster_multixact_current_describe_full(&key,&request_tag,&request_snapshot,&op,&stats));
		UT_ASSERT_NULL(op);UT_ASSERT_EQ(io_live,0);UT_ASSERT_EQ(live_contexts,1);UT_ASSERT_EQ(live_blocks,0);
	}
}

static void service_resource_owner(void)
{
	ClusterMxCompositionReceive *op;
	service_seed(257,1);op=fetch(257);
	UT_ASSERT_NOT_NULL(range_release);
	range_release(RESOURCE_RELEASE_BEFORE_LOCKS,false,false,range_release_arg);
	/* Output ownership is revoked even when the caller's stack address is no
	 * longer registered. No dereference of the old borrowed pointer here. */
	UT_ASSERT_EQ(live_contexts,1);UT_ASSERT_EQ(live_blocks,0);
	op=NULL;
	UT_ASSERT_NULL(op);
}

static void service_wire_and_snapshot(void)
{
	ClusterMxRangeRequest r={0};ClusterMxRangeResponse response;
	ClusterMxRangeSource source;
	ClusterMxRangeRoute route;BufferTag tag;uint32 bytes=0;
	service_seed(2,1);cluster_node_id=0;
	r.route.request_id=91;r.route.epoch=7;r.route.requester_node=1;r.route.requester_backend=1;
	r.key.multixact_id=1000;r.key.cluster_epoch=7;r.target=request_tag;r.snapshot=request_snapshot;
	r.cap=256;r.peer_generation=11;cluster_mx_range_request_seal(&r);
	UT_ASSERT(cluster_mx_range_serve(&r,&response,sizeof(response),&bytes));
	UT_ASSERT(cluster_mx_range_transport_route(&response,bytes,PGRAC_IC_MSG_GCS_BLOCK_REPLY,&route,&tag));
	memcpy(&source,response.data.data+sizeof(ClusterMxRangeWireHeader),sizeof(source));
	UT_ASSERT_EQ(source.system_identifier,91);UT_ASSERT_EQ(source.boot_incarnation,71);
	UT_ASSERT_EQ(source.formation_epoch,7);UT_ASSERT_EQ(source.admission_revision,3);
	UT_ASSERT_EQ(memcmp(&source.snapshot,&request_snapshot,sizeof(request_snapshot)),0);
	UT_ASSERT_EQ(memcmp(&source.target,&request_tag,sizeof(request_tag)),0);
	UT_ASSERT(!cluster_mx_range_transport_route(&r,sizeof(r)-1,PGRAC_IC_MSG_GCS_BLOCK_FORWARD,&route,&tag));
	r.snapshot.curcid++;UT_ASSERT(!cluster_mx_range_serve(&r,&response,sizeof(response),&bytes));
	UT_ASSERT_EQ(bytes,0);UT_ASSERT_EQ(io_live,0);
}

static void service_error_cleanup(void)
{
	ClusterCurrentMxKey key={0};ClusterMxCompositionReceive *op=NULL;
	ClusterMxCompositionReceiveStats stats={0};volatile bool caught=false;
	service_seed(2,1);native_fail_at=2;key.multixact_id=1000;key.cluster_epoch=7;
	PG_TRY();
	{
		(void)cluster_multixact_current_describe_full(&key,&request_tag,&request_snapshot,&op,&stats);
	}
	PG_CATCH();
	{
		caught=true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);UT_ASSERT_NULL(op);UT_ASSERT_EQ(io_live,0);
	for(unsigned i=0;i<4;i++)UT_ASSERT_EQ(held[i],0);
	UT_ASSERT_EQ(live_contexts,1);UT_ASSERT_EQ(live_blocks,0);
}

static void service_scoped_adapter(void)
{
	ClusterCurrentMxKey key={0};SnapshotData snapshot={0};
	ClusterCurrentMxMemberDesc members[256];uint16 count;uint32 total;
	key.multixact_id=1000;key.cluster_epoch=7;
	snapshot.snapshot_type=SNAPSHOT_MVCC;snapshot.read_scn=17;snapshot.read_epoch=7;
	snapshot.xmin=40;snapshot.xmax=400;snapshot.curcid=9;
	service_seed(2,1);capability_ok=false;
	UT_ASSERT_EQ(cluster_multixact_current_describe_scoped(&key,&request_tag,&snapshot,members,256,&count,&total),CMX_DESC_OK);
	UT_ASSERT_EQ(legacy_calls,1);UT_ASSERT_EQ(count,1);UT_ASSERT_EQ(members[0].xid,55);UT_ASSERT_EQ(frame_calls,0);
	service_seed(2,1);
	UT_ASSERT_EQ(cluster_multixact_current_describe_scoped(&key,&request_tag,&snapshot,members,256,&count,&total),CMX_DESC_OK);
	UT_ASSERT_EQ(legacy_calls,0);UT_ASSERT_EQ(count,2);UT_ASSERT_EQ(total,2);UT_ASSERT_EQ(members[1].xid,101);
	service_seed(257,1);memset(members,0xa5,sizeof(members));count=99;total=99;
	UT_ASSERT_EQ(cluster_multixact_current_describe_scoped(&key,&request_tag,&snapshot,members,256,&count,&total),CMX_DESC_SUPPORTED_LIMIT);
	UT_ASSERT_EQ(count,0);UT_ASSERT_EQ(total,257);UT_ASSERT_EQ(legacy_calls,0);
	for(unsigned i=0;i<sizeof(members);i++)UT_ASSERT_EQ(((char *)members)[i],0);
	service_seed(257,1);inject=6;
	UT_ASSERT_EQ(cluster_multixact_current_describe_scoped(&key,&request_tag,&snapshot,members,256,&count,&total),CMX_DESC_UNKNOWN);
	UT_ASSERT_EQ(count,0);UT_ASSERT_EQ(total,0);UT_ASSERT_EQ(legacy_calls,0);
	UT_ASSERT_EQ(io_live,0);UT_ASSERT_EQ(live_contexts,1);UT_ASSERT_EQ(live_blocks,0);
}

static void service_cancel_between_frames(void)
{
	ClusterCurrentMxKey key={0};ClusterMxCompositionReceive *op=NULL;
	ClusterMxCompositionReceiveStats stats={0};volatile bool caught=false;
	service_seed(257,1);inject=12;key.multixact_id=1000;key.cluster_epoch=7;
	PG_TRY(); {
		(void)cluster_multixact_current_describe_full(&key,&request_tag,&request_snapshot,&op,&stats);
	} PG_CATCH(); { caught=true; } PG_END_TRY();
	UT_ASSERT(caught);UT_ASSERT_NULL(op);UT_ASSERT_EQ(frame_calls,1);UT_ASSERT_EQ(io_live,0);
	UT_ASSERT_EQ(live_contexts,1);UT_ASSERT_EQ(live_blocks,0);
	cluster_multixact_current_describe_finish(&op);
	InterruptPending=false;
}

int main(void)
{
	TopMemoryContext=AllocSetContextCreate(CurrentMemoryContext,"range test top",ALLOCSET_DEFAULT_SIZES);
	CurrentMemoryContext=TopMemoryContext;
	UT_PLAN(8);UT_RUN(service_sizes);UT_RUN(service_later_failures);
	UT_RUN(service_refusals);UT_RUN(service_resource_owner);
	UT_RUN(service_wire_and_snapshot);UT_RUN(service_error_cleanup);
	UT_RUN(service_scoped_adapter);
	UT_RUN(service_cancel_between_frames);
	MemoryContextDelete(TopMemoryContext);UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
