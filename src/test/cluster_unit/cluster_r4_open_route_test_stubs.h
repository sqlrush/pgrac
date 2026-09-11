/* Services outside the real OPEN -> master route boundary.  No semantic
 * admission, token recheck or peer OPEN matcher is replaced in this test. */
#include "cluster/cluster_cssd.h"
#include "cluster/cluster_gcs.h"
#include "cluster/cluster_grd_outbound.h"
#include "cluster/cluster_ic_router.h"
#include "cluster/cluster_ic_tier1.h"
#include "cluster/cluster_lms_shard.h"
#include "cluster/cluster_pcm_lock.h"
#include "cluster/cluster_r4_observe.h"
#include "cluster/cluster_recovery_merge.h"

int MaxBackends = 32;
int cluster_lms_workers = 4;
int cluster_pcm_grd_max_entries = 32;
bool cluster_enabled = true;
bool cluster_online_join = true;
bool cluster_recmerge_window_active = false;
bool cluster_ic_suppress_gcs_done_cap = false;
ClusterConf *ClusterConfShmem = NULL;
sigjmp_buf *PG_exception_stack = NULL;
ErrorContextCallback *error_context_stack = NULL;

void
pg_re_throw(void)
{
	abort();
}

int
cluster_conf_node_count(void)
{
	return 4;
}

bool
cluster_grd_recovery_in_progress(void)
{
	return false;
}

bool
cluster_merged_instance_is_materialized(int node pg_attribute_unused())
{
	return false;
}

uint64
cluster_merged_instance_recovered_through(int node pg_attribute_unused())
{
	return 0;
}

uint64
cluster_pcm_lock_pi_watermark_lsn_query(BufferTag tag pg_attribute_unused())
{
	return 0;
}

static int test_open_route_forwards;
static int test_open_route_refusals;
static ClusterR4CrForwardPayload test_open_route_forward;

int
cluster_ic_tier1_my_data_channel(void)
{
	return 0;
}

int
cluster_lms_shard_for_tag(const BufferTag *tag pg_attribute_unused(), int workers)
{
	return workers > 0 ? 0 : -1;
}

int
cluster_gcs_lookup_master(BufferTag tag pg_attribute_unused())
{
	return 0;
}

int
cluster_gcs_lookup_master_static(BufferTag tag pg_attribute_unused())
{
	return 0;
}

bool
cluster_grd_offpath_boot_decided(void)
{
	return true;
}

void
cluster_grd_inc_join_block_failclosed(void)
{}

bool
cluster_grd_join_remaster_active_for_shard(BufferTag tag pg_attribute_unused())
{
	return false;
}

bool
cluster_grd_block_view_rebuilt(BufferTag tag pg_attribute_unused())
{
	return true;
}

ClusterCssdPeerState
cluster_cssd_get_peer_state(int32 node pg_attribute_unused())
{
	return CLUSTER_CSSD_PEER_ALIVE;
}

bool
cluster_sf_peer_capability_family_sample(int32 node, uint32 required, uint32 optional,
										 bool *optional_out, uint32 *generation_out)
{
	uint32 word = 0;
	bool result = cluster_sf_peer_capability_word_sample(node, required, &word, generation_out);

	*optional_out = (word & optional) == optional;
	return result;
}

bool
cluster_pcm_lock_r4_route_snapshot(BufferTag tag pg_attribute_unused(), PcmAuthoritySnapshot *out,
								   uint64 *generation, SCN *page_scn)
{
	memset(out, 0, sizeof(*out));
	out->state = PCM_STATE_S;
	out->x_holder_node = -1;
	out->pending_x_requester_node = -1;
	out->transition_count = 7;
	out->master_holder.node_id = 3;
	out->s_holders_bitmap = UINT32_C(1) << 3;
	*generation = (test_current_epoch << 32) | UINT64_C(4);
	*page_scn = (SCN)55;
	return true;
}

GcsBlockR4RouteArmResult
cluster_gcs_block_dedup_r4_route_arm_or_match(
	int worker pg_attribute_unused(), const GcsBlockR4RouteIdentity *identity pg_attribute_unused(),
	uint8 transition pg_attribute_unused(), const ClusterR4CrRouteProof *proof,
	uint32 lifetime pg_attribute_unused(), bool trusted pg_attribute_unused(),
	GcsBlockR4RouteRecord *out)
{
	memset(out, 0, sizeof(*out));
	out->proof = *proof;
	out->state = GCS_BLOCK_R4_ROUTE_ROUTING;
	return GCS_BLOCK_R4_ROUTE_ARM_NEW;
}

GcsBlockR4RouteSendResult
cluster_gcs_block_dedup_r4_route_finish_send(
	int worker pg_attribute_unused(), const GcsBlockR4RouteIdentity *identity pg_attribute_unused(),
	uint8 transition pg_attribute_unused(),
	const ClusterR4CrRouteProof *proof pg_attribute_unused(), bool admitted)
{
	return admitted ? GCS_BLOCK_R4_ROUTE_SEND_FORWARDED : GCS_BLOCK_R4_ROUTE_SEND_INVALID;
}

bool
cluster_lms_outbound_enqueue_cap_bound(int worker, uint8 type, uint32 dest, const void *payload,
									   uint16 length, uint32 required, uint32 generation)
{
	UT_ASSERT_EQ(worker, 0);
	UT_ASSERT_EQ(type, PGRAC_IC_MSG_GCS_BLOCK_FORWARD);
	UT_ASSERT_EQ(dest, 3);
	UT_ASSERT_EQ(length, sizeof(test_open_route_forward));
	UT_ASSERT((test_peer_capability_word & required) == required);
	UT_ASSERT_EQ(generation, test_peer_capability_generation);
	test_open_route_forwards++;
	memcpy(&test_open_route_forward, payload, sizeof(test_open_route_forward));
	return true;
}

bool
cluster_lms_outbound_enqueue_zero_block_reply_cap_bound(
	int worker pg_attribute_unused(), uint32 dest pg_attribute_unused(),
	const GcsBlockReplyHeader *header pg_attribute_unused(), uint32 required pg_attribute_unused(),
	uint32 generation pg_attribute_unused())
{
	test_open_route_refusals++;
	return true;
}

bool
cluster_ic_envelope_build(ClusterICEnvelope *out pg_attribute_unused(),
						  uint8 type pg_attribute_unused(), uint32 source pg_attribute_unused(),
						  uint32 dest pg_attribute_unused(),
						  const void *payload pg_attribute_unused(),
						  uint32 length pg_attribute_unused())
{
	UT_ASSERT(false); /* The fixture always routes to a remote holder. */
	return false;
}

bool
cluster_ic_dispatch_envelope(const ClusterICEnvelope *env pg_attribute_unused(),
							 const void *payload pg_attribute_unused(),
							 int32 peer pg_attribute_unused())
{
	UT_ASSERT(false);
	return false;
}

void
cluster_r4_observe(ClusterR4Event event pg_attribute_unused(),
				   ClusterTxResolveReason tx_reason pg_attribute_unused(),
				   ClusterCrBuildReason reason pg_attribute_unused())
{}

void
cluster_r4_observe_refusal(ClusterR4RefusalStage stage pg_attribute_unused(),
						   ClusterCrBuildReason reason pg_attribute_unused(),
						   const BufferTag *tag pg_attribute_unused(),
						   uint64 request pg_attribute_unused(), uint64 epoch pg_attribute_unused(),
						   int32 requester pg_attribute_unused(),
						   int32 master pg_attribute_unused(), SCN scn pg_attribute_unused())
{}
