/* Independent range-domain cases against the original production router. */
int k_original_shard_main(void);
#define main k_original_shard_main
#include "test_cluster_gcs_block_shard.c"
#undef main
#include "cluster/cluster_multixact_range.h"

static ClusterMxRangeRequest range_request(void)
{
	ClusterMxRangeRequest r={0};
	r.route.request_id=91;r.route.epoch=7;r.route.requester_node=1;r.route.requester_backend=2;
	r.key.multixact_id=1000;r.key.cluster_epoch=7;
	r.target=make_tag(1663,5,8,MAIN_FORKNUM,2);r.cap=256;r.peer_generation=11;
	cluster_mx_range_request_seal(&r);return r;
}
static void range_route_affinity(void)
{
	ClusterMxRangeRequest r=range_request();ClusterMxRangeResponse reply;
	BufferTag tag=GcsBlockCurrentMxRouteTagMake(91,7,1,2);uint32 bytes=0;
	UT_ASSERT(cluster_mx_range_refuse(&r,202611010,&reply,sizeof(reply),&bytes));
	for(int workers=1;workers<=8;workers*=2) {
		int expected=cluster_lms_shard_for_tag(&tag,workers);
		UT_ASSERT_EQ(cluster_gcs_block_payload_shard(PGRAC_IC_MSG_GCS_BLOCK_FORWARD,&r,sizeof(r),workers),expected);
		UT_ASSERT_EQ(cluster_gcs_block_payload_shard(PGRAC_IC_MSG_GCS_BLOCK_REPLY,&reply,bytes,workers),expected);
	}
}
static void range_malformed_never_legacy(void)
{
	ClusterMxRangeRequest good=range_request(),r;
	for(unsigned fault=0;fault<8;fault++) {
		uint16 bytes=sizeof(r);r=good;
		if(fault==0)bytes--;
		if(fault==1)r.route.kind=GCS_BLOCK_FORWARD_KIND_CURRENT_MX_DESCRIBE;
		if(fault==2)r.route.version++;
		if(fault==3)r.route.reserved=1;
		if(fault==4)r.reserved=1;
		if(fault==5)r.peer_generation=0;
		if(fault==6)r.cap=257;
		if(fault==7)r.route.checksum++;
		UT_ASSERT_EQ(cluster_gcs_block_payload_shard(PGRAC_IC_MSG_GCS_BLOCK_FORWARD,&r,bytes,4),-1);
		UT_ASSERT_EQ(cluster_gcs_block_payload_shard(PGRAC_IC_MSG_GCS_BLOCK_REQUEST,&r,bytes,4),-1);
	}
}
static void legacy_request_id_magic_is_legal(void)
{
	GcsBlockRequestPayload request={0};GcsBlockForwardPayload forward={0};
	BufferTag tag=make_tag(1663,5,8,MAIN_FORKNUM,2);
	request.request_id=UINT64_C(0x014D584731);request.tag=tag;
	forward.request_id=request.request_id;forward.tag=tag;
	UT_ASSERT_EQ(cluster_gcs_block_payload_shard(PGRAC_IC_MSG_GCS_BLOCK_REQUEST,&request,sizeof(request),4),cluster_lms_shard_for_tag(&tag,4));
	UT_ASSERT_EQ(cluster_gcs_block_payload_shard(PGRAC_IC_MSG_GCS_BLOCK_FORWARD,&forward,sizeof(forward),4),cluster_lms_shard_for_tag(&tag,4));
}
int main(void)
{
	UT_PLAN(3);UT_RUN(range_route_affinity);UT_RUN(range_malformed_never_legacy);
	UT_RUN(legacy_request_id_magic_is_legal);UT_DONE();
	return ut_failed_count==0?0:1;
}
