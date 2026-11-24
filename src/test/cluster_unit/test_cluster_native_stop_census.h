/* Original stop predicates over the native fixture's one-buffer pool.
 * No debt, native flag or protocol state is cleared by these observers.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "cluster/cluster_pi_shadow.h"

static SCN ns_shadow[1];
/* The inherited PCM directory has several entries; its native buffer pool
 * has exactly one descriptor. Keep those two capacities independent. */
#define NBuffers 1
#define ClusterPiShadow ns_shadow
#define LockBufHdr request_header_lock
#include "test_cluster_native_stop_observer.inc"
#undef LockBufHdr
#undef ClusterPiShadow
#undef NBuffers

static ClusterNormalStopPollResult
ns_buffer(ClusterBufferStopObservation *out, const char **reason)
{
	bool under = IsUnderPostmaster, enabled = cluster_enabled;
	ClusterNormalStopPollResult result;
	BufferTag tag; int id;
	IsUnderPostmaster = cluster_enabled = true;
	result = cluster_bufmgr_stop_poll(false, false, &tag, &id, reason, out);
	IsUnderPostmaster = under; cluster_enabled = enabled;
	return result;
}

static ClusterNormalStopPollResult
ns_pcm_tag(const char *cut, const BufferTag *resource)
{
	BufferTag tag; uint32 slot; const char *reason;
	PcmGrdProtocolDebtStats debt;
	ClusterNormalStopPollResult result;
	struct StopPcmEntryLayout *entry = hash_search((HTAB *)&fake_pcm_htab_token,
		resource, HASH_FIND, NULL);
	ClusterPcmResourceXBootstrapRound before;
	ClusterPcmOwnEntry own = request_own;
	uint32 header = pg_atomic_read_u32(&request_buf.state);

	/* A master observes its received resource, without a requester buffer. */
	UT_ASSERT(entry != NULL);
	if (entry == NULL) return CLUSTER_NORMAL_STOP_INVALID;
	before = entry->resource_x_bootstrap_round;
	UT_ASSERT_EQ(fake_lwlock_depth, 0);
	result = stop_pcm_poll(false, &tag, &slot, &reason);
	cluster_pcm_grd_protocol_debt_snapshot(&debt);
	UT_ASSERT(memcmp(&before, &entry->resource_x_bootstrap_round, sizeof(before)) == 0);
	UT_ASSERT(memcmp(&own, &request_own, sizeof(own)) == 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&request_buf.state), header);
	UT_ASSERT_EQ(fake_lwlock_depth, 0);
	printf("# stop-census node=%d cut=%s result=%d reason=%s slot=%u active=" UINT64_FORMAT
		" retained=" UINT64_FORMAT " owners=" UINT64_FORMAT " invalid=" UINT64_FORMAT
		" phase=%u resident-S=%u acquire=%u request-send=%u native=%u flags=%u IO=%u\n",
		cluster_node_id, cut, result, reason, slot, debt.active_resource_x_entry_count,
		debt.retained_entry_count, debt.local_owner_entry_count, debt.invalid_entry_count,
		before.phase, before.resident_s_authorized, before.has_acquire_intent,
		before.send.pending, request_buf.pcm_state, cluster_pcm_own_flags_get(0),
		(header & BM_IO_IN_PROGRESS) != 0);
	UT_ASSERT_EQ(debt.invalid_entry_count, 0);
	return result;
}

static ClusterNormalStopPollResult
ns_pcm(const char *cut)
{
	return ns_pcm_tag(cut, &request_prepared.common.logical_assertion.resource);
}

static void
ns_buffer_pending(const char *cut)
{
	ClusterBufferStopObservation observed;
	const char *reason;
	uint32 header = pg_atomic_read_u32(&request_buf.state);
	ClusterPcmOwnEntry own = request_own;
	UT_ASSERT_EQ(ns_buffer(&observed, &reason), CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT(strcmp(reason, "BUFMGR_OWNED") == 0);
	UT_ASSERT_EQ(observed.state, header);
	UT_ASSERT_EQ(observed.own.generation, cluster_pcm_own_gen_get(0));
	UT_ASSERT(memcmp(&own, &request_own, sizeof(own)) == 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&request_buf.state), header);
	printf("# stop-census node=%d cut=%s buffer=%s pins=%u IO=%u delivery=" UINT64_FORMAT "\n",
		cluster_node_id, cut, reason, BUF_STATE_GET_REFCOUNT(observed.state),
		(observed.state & BM_IO_IN_PROGRESS) != 0, observed.delivery);
}
