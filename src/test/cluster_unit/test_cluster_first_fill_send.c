/*-------------------------------------------------------------------------
 * test_cluster_first_fill_send.c
 *    Real holder-forward handler and transport frame-ownership boundary.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_first_fill_send.c
 *-------------------------------------------------------------------------
 */
int first_fill_send_own_fixture_main(void);
#define main first_fill_send_own_fixture_main
#include "test_cluster_pcm_own.c"
#undef main

#include "cluster/cluster_lms.h"
#include "cluster/cluster_membership.h"
#include "cluster/cluster_ic_rdma.h"
#include "cluster/cluster_block_io_trace.h"
#include "cluster/cluster_gcs_block_dedup.h"
#include "cluster/cluster_xnode_profile.h"

/* Keep the native wait witness and the whole production forward handler.
 * Only peer transport and unrelated request families are fixture boundaries. */
static uint64 send_epoch = 1, send_master_incarnation = 9;
static TimestampTz send_now = 10;
static bool send_stop, send_session = true;
static int send_master = 2;
static int send_route_unknown_after = -1, send_auth_unknown_after = -1;
static bool send_auth_zero_after;
static unsigned send_calls, send_admitted, send_nacks, send_returns, send_deferred, send_copies, send_releases;
static bool send_notice_admit = true;
static unsigned send_holder_offers, send_holder_handed;
static ClusterMembershipState send_master_state = CLUSTER_MEMBER_MEMBER;
static ClusterPcmReadGrant send_last_holder;
static ClusterICSendResult send_result;
static char send_first[sizeof(GcsBlockReplyHeaderV2) + GCS_BLOCK_DATA_SIZE];
static size_t send_length;
static bool send_borrowed, send_downgrade, send_copy_failure, send_cancel_check = true;
static void send_wakeup(int worker pg_attribute_unused()) { }
#define LockBufHdr transition_lock_header
#define LWLockAcquire transition_lock_acquire
#define LWLockRelease transition_lock_release
#define BufTableHashCode(tag) 0
#define BufMappingPartitionLock(hash) (&transition_mapping_lock)
#define BufTableLookup(tag, hash) 0
#define GetBufferDescriptor(id) transition_buf
#define cluster_lms_wakeup send_wakeup
#include "test_cluster_first_fill_bufmgr.inc"
#undef GetBufferDescriptor
#undef BufTableLookup
#undef BufMappingPartitionLock
#undef BufTableHashCode
#undef LWLockRelease
#undef LWLockAcquire
#undef LockBufHdr

static void gcs_first_fill_master_tick(int worker pg_attribute_unused()) { }
static bool
gcs_block_pending_s_notify_service(const ClusterPcmReadGrant *service, int master, uint8 action)
{
	const ClusterPcmReadGrantKey *key = &service->key;
	UT_ASSERT_EQ(service->binding_generation, 7);
	UT_ASSERT_EQ(service->service_generation, 1);
	if (send_cancel_check) UT_ASSERT_EQ(key->request_id, 71);
	UT_ASSERT_EQ(key->requester_node, 1);
	UT_ASSERT_EQ(master, 2);
	UT_ASSERT(action == GCS_BLOCK_DONE_PENDING_S_NACK || action == GCS_BLOCK_DONE_PENDING_S_RETURN
		|| action == GCS_BLOCK_DONE_PENDING_S_HOLDER);
	if (action == GCS_BLOCK_DONE_PENDING_S_HOLDER) {
		send_holder_offers++;
		send_last_holder = *service;
		if (send_notice_admit) send_holder_handed++;
	} else if (action == GCS_BLOCK_DONE_PENDING_S_RETURN) send_returns++; else send_nacks++;
	return send_notice_admit;
}
static void gcs_block_forward_reply_immediate_deny(const GcsBlockForwardPayload *fwd)
{
	if (send_cancel_check) UT_ASSERT_EQ(fwd->request_id, 71);
}
static bool
send_auth(int master, uint64 epoch, uint64 *session)
{
	*session = send_master_incarnation;
	if (send_auth_unknown_after == 0) {
		if (send_auth_zero_after) *session = 0;
		return send_auth_zero_after;
	}
	if (send_auth_unknown_after > 0) send_auth_unknown_after--;
	return send_session && master == send_master && epoch == send_epoch;
}
static int
send_lookup_master(BufferTag tag pg_attribute_unused())
{
	if (send_route_unknown_after == 0) return -1;
	if (send_route_unknown_after > 0) send_route_unknown_after--;
	return send_master;
}
static void
send_release(void *arg)
{
	UT_ASSERT(arg == transition_page.data);
	memset(arg, 0xee, GCS_BLOCK_DATA_SIZE);
	send_releases++;
}
static void
gcs_block_release_ship_image(ClusterICSgeReleaseCallback cb, void *arg)
{
	if (cb != NULL) cb(arg);
}
static void gcs_block_release_live_sge(void *arg) { send_release(arg); }
static bool
send_image(BufferTag tag, int requester, bool forwarded, XLogRecPtr *lsn,
		   char *scratch, const char **bytes, uint32 *lkey,
		   ClusterICSgeReleaseCallback *cb, void **arg,
		   ClusterSfDepVec *dep, bool *dep_valid, ClusterBufmgrGcsCopyRefusal *refusal)
{
	UT_ASSERT(BufferTagsEqual(&tag, &transition_buf->tag));
	UT_ASSERT_EQ(requester, 1);
	UT_ASSERT(forwarded);
	UT_ASSERT(!(pg_atomic_read_u32(&transition_buf->state) & BM_IO_IN_PROGRESS));
	send_copies++;
	if (send_copy_failure) {
		pg_atomic_fetch_and_u32(&transition_buf->state, ~BM_VALID);
		*refusal = CLUSTER_BUFMGR_GCS_COPY_REFUSAL_NOT_RESIDENT;
		return false;
	}
	*lsn = 42;
	if (send_borrowed) {
		*bytes = transition_page.data;
		*cb = send_release;
		*arg = transition_page.data;
	} else {
		memcpy(scratch, transition_page.data, GCS_BLOCK_DATA_SIZE);
		*bytes = scratch;
		*cb = NULL;
		*arg = NULL;
	}
	*lkey = 0;
	memset(dep, 0, sizeof(*dep));
	*dep_valid = false;
	*refusal = CLUSTER_BUFMGR_GCS_COPY_REFUSAL_NONE;
	return true;
}
static ClusterBufmgrGcsDowngradeOutcome
send_prepare(const ClusterPcmReadGrant *service, int master, XLogRecPtr *lsn,
			 char *bytes, ClusterBufmgrGcsCopyRefusal *refusal)
{
	UT_ASSERT(send_downgrade);
	UT_ASSERT_EQ(service->key.request_id, 71);
	UT_ASSERT_EQ(service->binding_generation, 7);
	UT_ASSERT_EQ(service->service_generation, 1);
	UT_ASSERT_EQ(service->holder_node, cluster_node_id);
	UT_ASSERT_EQ(master, 2);
	UT_ASSERT(!(pg_atomic_read_u32(&transition_buf->state) & BM_IO_IN_PROGRESS));
	if (!cluster_gcs_block_read_grant_holder_notify(service, master)) {
		*refusal = CLUSTER_BUFMGR_GCS_COPY_REFUSAL_NONE;
		return CLUSTER_BUFMGR_GCS_DOWNGRADE_REFUSED_PRE_NOTIFY;
	}
	memcpy(bytes, transition_page.data, GCS_BLOCK_DATA_SIZE);
	*lsn = 42;
	*refusal = CLUSTER_BUFMGR_GCS_COPY_REFUSAL_NONE;
	transition_buf->pcm_state = PCM_STATE_S;
	pg_atomic_fetch_add_u64(&ClusterPcmOwnArray[0].generation, 1);
	send_copies++;
	return CLUSTER_BUFMGR_GCS_DOWNGRADE_COMMITTED;
}
static ClusterICSendResult
send_frame(uint32 type, int target, const void *bytes, uint32 length)
{
	GcsBlockReplyHeader header;
	const GcsBlockReplyHeader *hdr = &header;
	memcpy(&header, bytes, sizeof(header));
	UT_ASSERT_EQ(type, PGRAC_IC_MSG_GCS_BLOCK_REPLY);
	UT_ASSERT_EQ(target, 1);
	UT_ASSERT_EQ(hdr->request_id, 71);
	UT_ASSERT_EQ(hdr->epoch, 1);
	UT_ASSERT_EQ(hdr->requester_backend_id, 3);
	UT_ASSERT(length <= sizeof(send_first));
	if (send_calls == 0) {
		memcpy(send_first, bytes, length);
		send_length = length;
	} else {
		UT_ASSERT_EQ(length, send_length);
		UT_ASSERT(memcmp(send_first, bytes, length) == 0);
	}
	send_calls++;
	if (send_result == CLUSTER_IC_SEND_DONE || send_result == CLUSTER_IC_SEND_WOULD_BLOCK)
		send_admitted++;
	return send_result;
}
static ClusterICSendResult send_seal_result;
static unsigned send_seal_calls;
static GcsBlockDonePayload send_sealed;
static ClusterICSendResult
send_seal_frame(uint8 type, int target, const void *bytes, uint32 length)
{
	ClusterPcmReadGrant service;
	UT_ASSERT_EQ(type, PGRAC_IC_MSG_GCS_BLOCK_DONE);
	UT_ASSERT_EQ(target, 2);
	UT_ASSERT_EQ(length, sizeof(GcsBlockDonePayload));
	send_sealed = *(const GcsBlockDonePayload *)bytes;
	UT_ASSERT_EQ(send_sealed.reserved_0[0], GCS_BLOCK_DONE_PENDING_S_SEALED);
	UT_ASSERT(GcsBlockDonePayloadPendingSService(&send_sealed, &service));
	UT_ASSERT_EQ(service.holder_node, cluster_node_id);
	send_seal_calls++;
	return send_seal_result;
}
static ClusterICSendResult
send_sge(uint32 type, int target, const ClusterICSge *sge, int n, uint32 length)
{
	char bytes[sizeof(send_first)];
	size_t pos = 0;
	ClusterICSendResult result;
	for (int i = 0; i < n; i++) {
		UT_ASSERT(pos + sge[i].len <= sizeof(bytes));
		memcpy(bytes + pos, sge[i].addr, sge[i].len);
		pos += sge[i].len;
	}
	UT_ASSERT_EQ(pos, length);
	result = send_frame(type, target, bytes, length);
	/* The real SGE API releases borrowed payload even on NOT_ADMITTED. */
	for (int i = 0; i < n; i++)
		if (sge[i].release_cb != NULL) sge[i].release_cb(sge[i].release_arg);
	return result;
}

static struct {
	pg_atomic_uint64 block_forward_holder_evicted_count;
	pg_atomic_uint64 block_forward_received_count;
	pg_atomic_uint64 block_from_holder_ship_count;
	pg_atomic_uint64 block_ship_bytes_total;
	pg_atomic_uint64 block_x_transfer_ship_count;
	pg_atomic_uint64 cf_xheld_read_ship_count;
	pg_atomic_uint64 clean_page_xfer_count;
	pg_atomic_uint64 drop_pinned_deny_count;
	pg_atomic_uint64 lost_write_detected_count;
	pg_atomic_uint64 lost_write_invalidscn_failclosed_count;
	pg_atomic_uint64 lost_write_not_scn_tracked_skip_count;
	pg_atomic_uint64 xfer_stale_deny_count;
} send_stats;

#define GCS_FIRST_FILL_PARK_MAX 64
#define ClusterGcsBlock (&send_stats)
#define MaxBackends 32
#define cluster_lms_workers 2
#define gcs_block_send_envelope_or_loopback send_seal_frame
#define palloc0(n) calloc(1, (n))
#define pfree(p) free(p)
#define cluster_ic_tier1_my_data_channel() 0
#define cluster_ic_dispatch_defer(env) ((void)(env), ++send_deferred)
#define GetCurrentTimestamp() send_now
#define cluster_epoch_get_current() send_epoch
#define cluster_normal_stop_requested() send_stop
#define cluster_gcs_lookup_master send_lookup_master
#define cluster_membership_get_state(node) send_master_state
#define gcs_block_pcm_x_authenticated_session send_auth
#define cluster_block_io_trace_note(...) ((void)0)
#define gcs_block_try_ctrc_forward136(env, payload) false
#define gcs_block_try_current_mx_forward128(env, payload) false
#define gcs_block_try_r4_forward96(env, payload) false
#define cluster_cr_server_freshref_c1b_pair_request_decode(...) false
#define cluster_smart_fusion false
#define cluster_sf_peer_supports_reply_v2(node) false
#define cluster_sf_dep_vec_reset(vec) memset((vec), 0, sizeof(*(vec)))
#define cluster_gcs_block_family_on_data_plane() true
#define cluster_gcs_block_forward_serve_inline(...) ((void)0)
#define cluster_lms_cr_submit(fwd) false
#define cluster_lms_undo_fetch_submit(fwd) false
#define cluster_lms_undo_verdict_submit(fwd) false
#define cluster_lms_undo_multi_verdict_submit(fwd) false
#define cluster_pcm_legacy_transition_allowed(...) true
#define cluster_ges_bast false
#define cluster_injection_should_skip(name) false
#undef CLUSTER_INJECTION_POINT
#define CLUSTER_INJECTION_POINT(name) ((void)0)
#define cluster_bufmgr_downgrade_x_to_s_remote_for_gcs(...) false
#define cluster_bufmgr_renotify_s_for_gcs(...) false
#define cluster_lever_e2_note_nudge_result(result) ((void)(result))
#define cluster_read_scache false
#define cluster_bufmgr_first_fill_s_prepare_image send_prepare
#define cluster_bufmgr_downgrade_x_to_s_remote_for_gcs_prepare_image(...) CLUSTER_BUFMGR_GCS_DOWNGRADE_REFUSED_PRE_NOTIFY
#define cluster_lever_a_note_remote_downgrade(result) ((void)(result))
#define cluster_bufmgr_block_pcm_state(tag) \
	(transition_buf->pcm_state == PCM_STATE_S ? PCM_LOCK_MODE_S : PCM_LOCK_MODE_X)
#define cluster_bufmgr_block_grant_pending(tag) false
#define gcs_block_get_ship_image send_image
#define gcs_block_lost_write_verdict(expected, shipped) ((void)(expected), (void)(shipped), GCS_LOST_WRITE_SKIP)
#define gcs_block_compute_checksum cluster_gcs_block_compute_checksum
#define cluster_itl_page_has_active_slot(page) false
#define cluster_xp_begin(scope, op) ((void)0)
#define cluster_xp_end(scope) ((void)(scope))
#define cluster_bufmgr_drop_block_for_gcs_no_wire(tag, lsn, out) ((void)(out), CLUSTER_BUFMGR_GCS_DROP_NOT_RESIDENT)
#define GCS_BLOCK_LOG_MASTER_NOT_HOLDER_FORWARD(...) ((void)0)
#define cluster_bufmgr_gcs_copy_refusal_name(refusal) "fixture-refusal"
#define cluster_bufmgr_block_is_pi(tag) false
#define gcs_block_pi_kept_note_send(...) ((void)0)
#define gcs_block_try_send_direct_reply(...) false
#define cluster_ic_rdma_send_envelope_sge send_sge
#define cluster_ic_send_envelope send_frame
#define cluster_gcs_block_note_send_outcome(...) ((void)0)
#define gcs_block_note_live_sge_send() ((void)0)
/* This send-ownership fixture exercises the ordinary observation-OFF path. */
bool cluster_trace_normal_stop = false;
static void
send_trace_disabled(void)
{
	UT_ASSERT(!cluster_trace_normal_stop);
}
#define gcs_block_read_image_ingress_reject_trace(...) send_trace_disabled()
#define gcs_block_read_image_hop_trace(...) send_trace_disabled()
#undef ereport
#define ereport(level, rest) do { if ((level) >= ERROR) pg_re_throw(); } while (0)
#include "test_cluster_first_fill_send.inc"

static GcsBlockFirstFillForwardPayload
send_setup(BufferDesc *buf, ClusterPcmOwnEntry *entry)
{
	GcsBlockFirstFillForwardPayload fwd = {0};
	n_predecessor_fixture(buf, entry, BUF_TYPE_XCUR);
	buf->pcm_state = PCM_STATE_S;
	pg_atomic_write_u32(&buf->state, BM_TAG_VALID | BM_VALID | BM_DIRTY);
	transition_buf = buf;
	transition_mapping_held = transition_content_held = false;
	memset(gcs_first_fill_holder_parks, 0, sizeof(gcs_first_fill_holder_parks));
	memset(gcs_first_fill_holder_seals, 0, sizeof(gcs_first_fill_holder_seals));
	gcs_first_fill_holder_seal_count = 0;
	send_seal_calls = 0;
	send_seal_result = CLUSTER_IC_SEND_NOT_ADMITTED;
	memset(transition_page.data, 0x5a, GCS_BLOCK_DATA_SIZE);
	memset(send_first, 0, sizeof(send_first));
	send_length = send_calls = send_admitted = send_nacks = send_returns = send_deferred = send_copies = send_releases = 0;
	send_notice_admit = true;
	send_holder_offers = send_holder_handed = 0;
	memset(&send_last_holder, 0, sizeof(send_last_holder));
	send_master_state = CLUSTER_MEMBER_MEMBER;
	send_epoch = 1;
	send_master_incarnation = 9;
	send_now = 10;
	send_stop = false;
	send_session = true;
	send_master = 2;
	send_route_unknown_after = send_auth_unknown_after = -1;
	send_auth_zero_after = false;
	send_borrowed = send_downgrade = send_copy_failure = false;
	send_cancel_check = true;
	send_result = CLUSTER_IC_SEND_NOT_ADMITTED;
	fwd.forward.tag = buf->tag;
	fwd.forward.epoch = 1;
	fwd.forward.request_id = 71;
	fwd.forward.original_requester_node = 1;
	fwd.forward.requester_backend_id = 3;
	fwd.forward.master_node = 2;
	fwd.forward.transition_id = PCM_TRANS_N_TO_S;
	fwd.forward.reserved_0[6] = GCS_BLOCK_FORWARD_KIND_FIRST_FILL_S;
	fwd.binding_generation = 7;
	fwd.service_generation = 1;
	return fwd;
}
static void
send_forward(const GcsBlockFirstFillForwardPayload *fwd)
{
	ClusterICEnvelope env = {0};
	env.source_node_id = 2;
	env.dest_node_id = cluster_node_id;
	env.payload_length = sizeof(*fwd);
	cluster_gcs_handle_block_forward_envelope(&env, fwd);
}
static unsigned
send_owned(void)
{
	unsigned n = 0;
	for (unsigned i = 0; i < GCS_FIRST_FILL_PARK_MAX; i++)
		if (gcs_first_fill_holder_parks[i].in_use) n++;
	return n;
}
static void send_tick(void)
{
	send_now += 10000;
	cluster_gcs_block_first_fill_tick();
}

/* Clearing a park on a void handler return must fail this case. The real
 * handler builds and offers the reply; only the transport refusal is faked. */
UT_TEST(actual_handler_retains_rejected_reply_until_transport_admission)
{
	BufferDesc buf;
	ClusterPcmOwnEntry entry;
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	GcsBlockFirstFillForwardPayload fwd = send_setup(&buf, &entry);
	pg_atomic_write_u32(&buf.state, pg_atomic_read_u32(&buf.state) | BM_IO_IN_PROGRESS);
	pg_atomic_write_u64(&entry.write_io_generation, 1);
	send_forward(&fwd);
	UT_ASSERT_EQ(send_owned(), 1);
	UT_ASSERT_EQ(send_calls, 0);
	pg_atomic_write_u32(&buf.state, pg_atomic_read_u32(&buf.state) & ~BM_IO_IN_PROGRESS);
	pg_atomic_write_u64(&entry.write_io_generation, 2);
	send_tick();
	UT_ASSERT_EQ(send_calls, 1);
	UT_ASSERT_EQ(send_admitted, 0);
	UT_ASSERT_EQ(send_owned(), 1);
	send_tick();
	UT_ASSERT_EQ(send_calls, 2);
	UT_ASSERT_EQ(send_owned(), 1);
	send_result = CLUSTER_IC_SEND_DONE;
	send_tick();
	UT_ASSERT_EQ(send_calls, 3);
	UT_ASSERT_EQ(send_admitted, 1);
	UT_ASSERT_EQ(send_owned(), 0);
	UT_ASSERT_EQ(send_copies, 1);
	send_tick();
	UT_ASSERT_EQ(send_calls, 3);
	ClusterPcmOwnArray = saved;
}

UT_TEST(immediate_reply_keeps_owned_bytes_after_sge_release)
{
	BufferDesc buf;
	ClusterPcmOwnEntry entry;
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	GcsBlockFirstFillForwardPayload fwd = send_setup(&buf, &entry);
	send_borrowed = true;
	send_forward(&fwd);
	UT_ASSERT_EQ(send_calls, 1);
	UT_ASSERT_EQ(send_releases, 1);
	UT_ASSERT_EQ(send_owned(), 1);
	UT_ASSERT_EQ((unsigned char)transition_page.data[128], 0xee);
	UT_ASSERT_EQ((unsigned char)send_first[sizeof(GcsBlockReplyHeader) + 128], 0x5a);
	send_forward(&fwd); /* a duplicate cannot overwrite/reproduce the reply */
	UT_ASSERT_EQ(send_calls, 1);
	UT_ASSERT_EQ(send_copies, 1);
	send_result = CLUSTER_IC_SEND_DONE;
	send_tick();
	UT_ASSERT_EQ(send_calls, 2);
	UT_ASSERT_EQ(send_admitted, 1);
	UT_ASSERT_EQ(send_owned(), 0);
	UT_ASSERT_EQ(send_releases, 1);
	UT_ASSERT_EQ(send_copies, 1);
	ClusterPcmOwnArray = saved;
}

UT_TEST(admitted_would_block_is_never_resubmitted)
{
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	for (int delayed = 0; delayed <= 1; delayed++) {
		BufferDesc buf;
		ClusterPcmOwnEntry entry;
		GcsBlockFirstFillForwardPayload fwd = send_setup(&buf, &entry);
		if (!delayed) send_result = CLUSTER_IC_SEND_WOULD_BLOCK;
		send_forward(&fwd);
		if (delayed) {
			UT_ASSERT_EQ(send_owned(), 1);
			send_result = CLUSTER_IC_SEND_WOULD_BLOCK;
			send_tick();
		}
		UT_ASSERT_EQ(send_owned(), 0);
		UT_ASSERT_EQ(send_admitted, 1);
		send_tick();
		UT_ASSERT_EQ(send_calls, delayed ? 2 : 1);
		UT_ASSERT_EQ(send_admitted, 1);
	}
	ClusterPcmOwnArray = saved;
}

UT_TEST(downgraded_reply_outlives_the_preproduction_generation)
{
	BufferDesc buf;
	ClusterPcmOwnEntry entry;
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	GcsBlockFirstFillForwardPayload fwd = send_setup(&buf, &entry);
	buf.pcm_state = PCM_STATE_X;
	fwd.forward.reserved_0[0] = fwd.forward.reserved_0[3] = 1;
	send_downgrade = true;
	pg_atomic_write_u32(&buf.state, pg_atomic_read_u32(&buf.state) | BM_IO_IN_PROGRESS);
	pg_atomic_write_u64(&entry.write_io_generation, 1);
	send_forward(&fwd);
	UT_ASSERT_EQ(send_owned(), 1);
	pg_atomic_write_u32(&buf.state, pg_atomic_read_u32(&buf.state) & ~BM_IO_IN_PROGRESS);
	pg_atomic_write_u64(&entry.write_io_generation, 2);
	send_tick();
	UT_ASSERT_EQ(send_calls, 1);
	UT_ASSERT_EQ(send_owned(), 1);
	UT_ASSERT_EQ(send_copies, 1);
	UT_ASSERT_EQ(((GcsBlockReplyHeader *)send_first)->status,
				 GCS_BLOCK_REPLY_S_GRANTED_XHOLDER_DOWNGRADE);
	send_result = CLUSTER_IC_SEND_DONE;
	send_tick();
	UT_ASSERT_EQ(send_owned(), 0);
	UT_ASSERT_EQ(send_admitted, 1);
	UT_ASSERT_EQ(send_copies, 1);
	UT_ASSERT_EQ(send_nacks, 0);
	ClusterPcmOwnArray = saved;
}

UT_TEST(retained_reply_checks_terminal_identity_and_stop_before_reoffer)
{
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	for (int reason = 0; reason < 4; reason++) {
		BufferDesc buf;
		ClusterPcmOwnEntry entry;
		GcsBlockFirstFillForwardPayload fwd = send_setup(&buf, &entry);
		send_forward(&fwd);
		UT_ASSERT_EQ(send_owned(), 1);
		if (reason == 0) send_epoch++;
		if (reason == 1) send_stop = true;
		if (reason == 2) send_master = 3;
		if (reason == 3) send_session = false;
		send_result = CLUSTER_IC_SEND_DONE;
		send_tick();
		UT_ASSERT_EQ(send_calls, 1);
		UT_ASSERT_EQ(send_admitted, 0);
		UT_ASSERT_EQ(send_owned(), reason == 3);
		UT_ASSERT_EQ(send_nacks, reason != 3);
		if (reason == 3) {
			send_session = true;
			send_tick();
			UT_ASSERT_EQ(send_owned(), 0);
			UT_ASSERT_EQ(send_admitted, 1);
			UT_ASSERT_EQ(send_nacks, 0);
		}
	}
	ClusterPcmOwnArray = saved;
}

UT_TEST(hard_error_terminates_with_exact_cancellation)
{
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	for (int delayed = 0; delayed <= 1; delayed++) {
		BufferDesc buf;
		ClusterPcmOwnEntry entry;
		GcsBlockFirstFillForwardPayload fwd = send_setup(&buf, &entry);
		if (!delayed) send_result = CLUSTER_IC_SEND_HARD_ERROR;
		send_forward(&fwd);
		if (delayed) {
			UT_ASSERT_EQ(send_owned(), 1);
			send_result = CLUSTER_IC_SEND_HARD_ERROR;
			send_tick();
		}
		UT_ASSERT_EQ(send_nacks, 1);
		UT_ASSERT_EQ(send_owned(), 0);
		UT_ASSERT_EQ(send_admitted, 0);
		send_tick();
		UT_ASSERT_EQ(send_calls, delayed ? 2 : 1);
	}
	ClusterPcmOwnArray = saved;
}

UT_TEST(local_backoff_does_not_reoffer_before_the_next_service_tick)
{
	BufferDesc buf;
	ClusterPcmOwnEntry entry;
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	GcsBlockFirstFillForwardPayload fwd = send_setup(&buf, &entry);
	send_forward(&fwd);
	UT_ASSERT_EQ(send_owned(), 1);
	for (int i = 0; i < 100; i++) cluster_gcs_block_first_fill_tick();
	UT_ASSERT_EQ(send_calls, 1);
	send_tick();
	UT_ASSERT_EQ(send_calls, 2);
	UT_ASSERT_EQ(send_owned(), 1);
	UT_ASSERT_EQ(send_copies, 1);
	ClusterPcmOwnArray = saved;
}

UT_TEST(reply_capacity_defers_new_work_before_production_and_preserves_owned_frames)
{
	BufferDesc buf;
	ClusterPcmOwnEntry entry;
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	GcsBlockFirstFillForwardPayload fwd = send_setup(&buf, &entry);
	send_forward(&fwd);
	UT_ASSERT_EQ(send_owned(), 1);
	for (int i = 1; i < 64; i++) {
		gcs_first_fill_holder_parks[i] = gcs_first_fill_holder_parks[0];
		gcs_first_fill_holder_parks[i].forward.forward.request_id += i;
	}
	send_cancel_check = false;
	fwd.forward.request_id = 900;
	send_forward(&fwd);
	UT_ASSERT_EQ(send_owned(), 64);
	UT_ASSERT_EQ(send_calls, 1);
	UT_ASSERT_EQ(send_nacks + send_returns, 0);
	UT_ASSERT_EQ(send_deferred, 1);
	UT_ASSERT_EQ(send_copies, 1);
	UT_ASSERT_EQ(gcs_first_fill_holder_parks[0].forward.forward.request_id, 71);
	UT_ASSERT_EQ(gcs_first_fill_holder_parks[63].forward.forward.request_id, 134);
	for (int i = 1; i < 64; i++) gcs_first_fill_holder_parks[i].in_use = false;
	send_result = CLUSTER_IC_SEND_DONE;
	send_tick();
	UT_ASSERT_EQ(send_admitted, 1);
	UT_ASSERT_EQ(send_owned(), 0);
	ClusterPcmOwnArray = saved;
}

UT_TEST(actual_handler_rejects_unbound_and_malformed_first_fill_before_io)
{
	for (int bad = 0; bad < 16; bad++) {
		BufferDesc buf;
		ClusterPcmOwnEntry entry;
		ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
		GcsBlockFirstFillForwardPayload fwd = send_setup(&buf, &entry);
		ClusterICEnvelope env = {0};
		env.source_node_id = 2; env.dest_node_id = cluster_node_id;
		env.payload_length = sizeof(fwd);
		switch (bad) {
		case 0: env.payload_length = sizeof(GcsBlockForwardPayload); break;
		case 1: env.payload_length--; break;
		case 2: fwd.binding_generation = 0; break;
		case 3: fwd.binding_generation = UINT64_MAX; break;
		case 4: fwd.service_generation = 0; break;
		case 5: fwd.service_generation = UINT32_MAX; break;
		case 6: fwd.reserved = 1; break;
		case 7: fwd.forward.reserved_0[6] = 0; break;
		case 8: env.source_node_id = 3; break;
		case 9: env.dest_node_id++; break;
		case 10: fwd.forward.epoch++; break;
		case 11: fwd.forward.requester_backend_id = MaxBackends + 1; break;
		case 12: fwd.forward.request_id = 0; break;
		case 13: fwd.forward.reserved_0[4] = 1; break;
		case 14: fwd.forward.reserved_0[1] = 1; break;
		case 15: fwd.forward.reserved_0[3] = 1; break;
		}
		cluster_gcs_handle_block_forward_envelope(&env, &fwd);
		UT_ASSERT_EQ(send_calls + send_copies + send_nacks + send_owned(), 0);
		ClusterPcmOwnArray = saved;
	}
}

UT_TEST(actual_holder_refusal_never_tells_requester_to_retry)
{
	BufferDesc buf;
	ClusterPcmOwnEntry entry;
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	GcsBlockFirstFillForwardPayload fwd = send_setup(&buf, &entry);
	send_copy_failure = true;
	send_forward(&fwd);
	UT_ASSERT_EQ(send_copies, 1);
	UT_ASSERT_EQ(send_calls, 0);
	UT_ASSERT_EQ(send_owned(), 1);
	ClusterPcmOwnArray = saved;
}

UT_TEST(quiet_holder_return_waits_for_exact_master_consumption)
{
	BufferDesc buf;
	ClusterPcmOwnEntry entry;
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	GcsBlockFirstFillForwardPayload fwd = send_setup(&buf, &entry);
	ClusterPcmReadGrant service;
	GcsBlockDonePayload done = {0};
	send_copy_failure = true;
	send_notice_admit = false;
	send_forward(&fwd);
	UT_ASSERT_EQ(send_returns, 1);
	UT_ASSERT(gcs_first_fill_holder_parks[0].returned);
	UT_ASSERT(!gcs_first_fill_holder_parks[0].notice_admitted);
	for (int i = 0; i < 10; i++) send_tick();
	UT_ASSERT_EQ(send_owned(), 1);
	UT_ASSERT_EQ(send_calls + send_nacks, 0);
	UT_ASSERT_EQ(send_copies, 1);
	send_notice_admit = true;
	send_tick();
	UT_ASSERT(gcs_first_fill_holder_parks[0].notice_admitted);
	UT_ASSERT_EQ(send_owned(), 1); /* enqueue admission is not consumption */
	send_forward(&fwd);
	for (int i = 0; i < 10; i++) send_tick();
	UT_ASSERT_EQ(send_copies, 1);
	UT_ASSERT_EQ(send_returns, 12);
	UT_ASSERT(GcsBlockFirstFillForwardPayloadGrant(&fwd, cluster_node_id, &service));
	done.request_id = service.key.request_id; done.epoch = service.key.epoch;
	done.tag = service.key.tag; done.sender_node = 2;
	done.requester_backend_id = service.key.backend_id;
	done.transition_id = PCM_TRANS_N_TO_S;
	done.reserved_0[0] = GCS_BLOCK_DONE_PENDING_S_RETURNED;
	done.reserved_0[1] = 1;
	GcsBlockDonePayloadSetPendingSService(&done, &service);
	for (int bad = 0; bad < 11; bad++) {
		GcsBlockDonePayload wrong = done;
		uint64 session = 9;
		switch (bad) {
		case 0: wrong.reserved_0[2]++; break;
		case 1: wrong.reserved_0[10]++; break;
		case 2: wrong.reserved_0[15] = 4; break;
		case 3: wrong.reserved_0[14] = 1; break;
		case 4: wrong.request_id++; break;
		case 5: wrong.requester_backend_id++; break;
		case 6: wrong.epoch++; break;
		case 7: wrong.tag.blockNum++; break;
		case 8: wrong.sender_node++; break;
		case 9: session++; break;
		case 10: wrong.reserved_0[16] = 1; break;
		}
		UT_ASSERT(!gcs_first_fill_holder_returned(&wrong, session));
		UT_ASSERT_EQ(send_owned(), 1);
	}
	UT_ASSERT(gcs_first_fill_holder_returned(&done, 9));
	UT_ASSERT_EQ(send_owned(), 0);
	UT_ASSERT(!gcs_first_fill_holder_returned(&done, 9));
	send_tick();
	UT_ASSERT_EQ(send_calls, 0);
	ClusterPcmOwnArray = saved;
}

UT_TEST(terminal_holder_nack_retains_ownership_without_reselecting)
{
	BufferDesc buf;
	ClusterPcmOwnEntry entry;
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	GcsBlockFirstFillForwardPayload fwd = send_setup(&buf, &entry);
	send_result = CLUSTER_IC_SEND_HARD_ERROR;
	send_notice_admit = false;
	send_forward(&fwd);
	UT_ASSERT_EQ(send_owned(), 1);
	UT_ASSERT(gcs_first_fill_holder_parks[0].terminal);
	UT_ASSERT_EQ(send_calls, 1);
	for (int i = 0; i < 10; i++) send_tick();
	UT_ASSERT_EQ(send_owned(), 1);
	UT_ASSERT_EQ(send_returns, 0);
	UT_ASSERT_EQ(send_calls, 1);
	send_notice_admit = true;
	send_tick();
	UT_ASSERT_EQ(send_owned(), 0);
	UT_ASSERT_EQ(send_calls, 1);
	UT_ASSERT_EQ(send_returns, 0);
	ClusterPcmOwnArray = saved;
}

UT_TEST(quiet_return_ends_on_proved_master_reconfiguration_but_not_unknown_session)
{
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	for (int why = 0; why < 4; why++) {
		BufferDesc buf;
		ClusterPcmOwnEntry entry;
		GcsBlockFirstFillForwardPayload fwd = send_setup(&buf, &entry);
		send_copy_failure = true;
		send_forward(&fwd);
		UT_ASSERT_EQ(send_owned(), 1);
		if (why == 0) send_master_incarnation++;
		if (why == 1) send_epoch++;
		if (why == 2) send_master++;
		if (why == 3) send_session = false;
		send_tick();
		UT_ASSERT_EQ(send_owned(), why == 3);
		UT_ASSERT_EQ(send_calls + send_nacks, 0);
	}
	ClusterPcmOwnArray = saved;
}

/* The callback, park owner and send continuation below are production code.
 * Only physical downgrade/image copying and peer/ring transport are controlled. */
static GcsBlockFirstFillForwardPayload
send_x_fixture(BufferDesc *buf, ClusterPcmOwnEntry *entry)
{
	GcsBlockFirstFillForwardPayload fwd = send_setup(buf, entry);
	buf->pcm_state = PCM_STATE_X;
	fwd.forward.reserved_0[0] = fwd.forward.reserved_0[3] = 1;
	send_downgrade = true;
	send_notice_admit = false;
	return fwd;
}

UT_TEST(holder_notice_refusal_preserves_image_and_exact_notification_independently)
{
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	for (int delayed = 0; delayed <= 1; delayed++) {
		BufferDesc buf;
		ClusterPcmOwnEntry entry;
		GcsBlockFirstFillForwardPayload fwd = send_x_fixture(&buf, &entry);
		unsigned offers;
		send_result = delayed ? CLUSTER_IC_SEND_WOULD_BLOCK : CLUSTER_IC_SEND_DONE;
		send_forward(&fwd);
		UT_ASSERT_EQ(send_admitted, 1);
		UT_ASSERT_EQ(buf.pcm_state, PCM_STATE_S);
		UT_ASSERT_EQ(send_owned(), 1);
		UT_ASSERT_EQ(send_holder_handed, 0);
		/* A successor buffer and elapsed time cannot provide old notice proof. */
		buf.tag.blockNum++;
		pg_atomic_fetch_add_u64(&entry.generation, 10);
		send_now += 10000000;
		send_tick();
		UT_ASSERT_EQ(send_owned(), 1);
		UT_ASSERT_EQ(send_calls, 1);
		UT_ASSERT_EQ(send_copies, 1);
		UT_ASSERT_EQ(send_last_holder.key.tag.blockNum, fwd.forward.tag.blockNum);
		send_notice_admit = true;
		send_tick();
		UT_ASSERT_EQ(send_holder_handed, 1);
		UT_ASSERT_EQ(send_owned(), 0);
		offers = send_holder_offers;
		for (int i = 0; i < 10; i++) send_tick();
		UT_ASSERT_EQ(send_holder_offers, offers);
		UT_ASSERT_EQ(send_calls, 1);
		UT_ASSERT_EQ(send_nacks + send_returns, 0);
	}
	ClusterPcmOwnArray = saved;
}

UT_TEST(holder_notice_unknown_session_waits_without_cancel_or_image_replay)
{
	BufferDesc buf;
	ClusterPcmOwnEntry entry;
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	GcsBlockFirstFillForwardPayload fwd = send_x_fixture(&buf, &entry);
	unsigned offers;
	send_result = CLUSTER_IC_SEND_DONE;
	send_forward(&fwd);
	UT_ASSERT_EQ(send_owned(), 1);
	offers = send_holder_offers;
	send_session = false;
	send_notice_admit = true;
	for (int i = 0; i < 10; i++) send_tick();
	UT_ASSERT_EQ(send_owned(), 1);
	UT_ASSERT_EQ(send_holder_offers, offers);
	UT_ASSERT_EQ(send_calls, 1);
	UT_ASSERT_EQ(send_nacks + send_returns, 0);
	send_session = true;
	send_tick();
	UT_ASSERT_EQ(send_holder_handed, 1);
	UT_ASSERT_EQ(send_owned(), 0);
	ClusterPcmOwnArray = saved;
}

UT_TEST(holder_notice_ends_on_proved_master_reconfiguration_only)
{
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	for (int reason = 0; reason < 5; reason++) {
		BufferDesc buf;
		ClusterPcmOwnEntry entry;
		GcsBlockFirstFillForwardPayload fwd = send_x_fixture(&buf, &entry);
		unsigned offers;
		send_result = CLUSTER_IC_SEND_DONE;
		send_forward(&fwd);
		UT_ASSERT_EQ(send_owned(), 1);
		offers = send_holder_offers;
		if (reason == 0) send_epoch++;
		if (reason == 1) send_master++;
		if (reason == 2) send_master_incarnation++;
		if (reason == 3) send_master_state = CLUSTER_MEMBER_DEAD;
		if (reason == 4) send_master_state = CLUSTER_MEMBER_REMOVED;
		send_notice_admit = true;
		send_tick();
		UT_ASSERT_EQ(send_owned(), 0);
		UT_ASSERT_EQ(send_holder_offers, offers);
		UT_ASSERT_EQ(send_calls, 1);
		UT_ASSERT_EQ(send_nacks + send_returns, 0);
	}
	ClusterPcmOwnArray = saved;
}

UT_TEST(normal_stop_drains_admitted_image_holder_notice_without_revoke)
{
	BufferDesc buf;
	ClusterPcmOwnEntry entry;
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	GcsBlockFirstFillForwardPayload fwd = send_x_fixture(&buf, &entry);
	send_result = CLUSTER_IC_SEND_DONE;
	send_forward(&fwd);
	UT_ASSERT_EQ(send_owned(), 1);
	send_stop = true;
	send_tick();
	UT_ASSERT_EQ(send_owned(), 1);
	send_notice_admit = true;
	send_tick();
	UT_ASSERT_EQ(send_holder_handed, 1);
	UT_ASSERT_EQ(send_owned(), 0);
	UT_ASSERT_EQ(send_calls, 1);
	UT_ASSERT_EQ(send_nacks + send_returns, 0);
	ClusterPcmOwnArray = saved;
}

UT_TEST(holder_notice_and_unadmitted_image_transfer_separately_once)
{
	BufferDesc buf;
	ClusterPcmOwnEntry entry;
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	GcsBlockFirstFillForwardPayload fwd = send_x_fixture(&buf, &entry);
	unsigned offers;
	send_forward(&fwd);
	UT_ASSERT_EQ(send_owned(), 1);
	UT_ASSERT_EQ(send_calls, 1);
	UT_ASSERT_EQ(send_admitted, 0);
	UT_ASSERT_EQ(send_copies, 1);
	send_notice_admit = true;
	send_tick();
	UT_ASSERT_EQ(send_holder_handed, 1);
	UT_ASSERT_EQ(send_owned(), 1);
	offers = send_holder_offers;
	send_result = CLUSTER_IC_SEND_DONE;
	send_tick();
	UT_ASSERT_EQ(send_owned(), 0);
	UT_ASSERT_EQ(send_holder_offers, offers);
	UT_ASSERT_EQ(send_admitted, 1);
	UT_ASSERT_EQ(send_copies, 1);
	ClusterPcmOwnArray = saved;
}

UT_TEST(wrong_holder_service_cannot_steal_original_notification_owner)
{
	BufferDesc buf;
	ClusterPcmOwnEntry entry;
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	GcsBlockFirstFillForwardPayload fwd = send_x_fixture(&buf, &entry);
	ClusterPcmReadGrant original;
	unsigned offers;
	send_forward(&fwd);
	UT_ASSERT(GcsBlockFirstFillForwardPayloadGrant(&fwd, cluster_node_id, &original));
	offers = send_holder_offers;
	for (int bad = 0; bad < 10; bad++) {
		ClusterPcmReadGrant wrong = original;
		int master = 2;
		if (bad == 0) wrong.binding_generation++;
		if (bad == 1) wrong.service_generation++;
		if (bad == 2) wrong.key.request_id++;
		if (bad == 3) wrong.key.epoch++;
		if (bad == 4) wrong.key.backend_id++;
		if (bad == 5) wrong.key.requester_node++;
		if (bad == 6) wrong.key.tag.blockNum++;
		if (bad == 7) wrong.holder_node++;
		if (bad == 8) wrong.holder_was_x = false;
		if (bad == 9) master++;
		UT_ASSERT(!cluster_gcs_block_read_grant_holder_notify(&wrong, master));
		UT_ASSERT_EQ(send_holder_offers, offers);
		UT_ASSERT_EQ(send_owned(), 1);
	}
	ClusterPcmOwnArray = saved;
}

UT_TEST(terminal_cancel_keeps_holder_notice_until_both_notices_transfer)
{
	BufferDesc buf;
	ClusterPcmOwnEntry entry;
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	GcsBlockFirstFillForwardPayload fwd = send_x_fixture(&buf, &entry);
	send_forward(&fwd);
	UT_ASSERT_EQ(send_calls, 1);
	send_stop = true;
	send_tick();
	UT_ASSERT_EQ(send_owned(), 1);
	UT_ASSERT_EQ(send_nacks, 0);
	send_notice_admit = true;
	send_tick();
	UT_ASSERT_EQ(send_holder_handed, 1);
	UT_ASSERT_EQ(send_nacks, 1);
	UT_ASSERT_EQ(send_owned(), 0);
	UT_ASSERT_EQ(send_admitted, 0);
	UT_ASSERT_EQ(send_copies, 1);
	ClusterPcmOwnArray = saved;
}

UT_TEST(holder_callback_cannot_mint_unreserved_service_or_repeat_after_handoff)
{
	BufferDesc buf;
	ClusterPcmOwnEntry entry;
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	GcsBlockFirstFillForwardPayload fwd = send_x_fixture(&buf, &entry);
	ClusterPcmReadGrant service;
	UT_ASSERT(GcsBlockFirstFillForwardPayloadGrant(&fwd, cluster_node_id, &service));
	send_notice_admit = true;
	UT_ASSERT(!cluster_gcs_block_read_grant_holder_notify(&service, 2));
	UT_ASSERT_EQ(send_holder_offers, 0);
	send_result = CLUSTER_IC_SEND_DONE;
	send_forward(&fwd);
	UT_ASSERT_EQ(send_holder_handed, 1);
	UT_ASSERT_EQ(send_owned(), 0);
	UT_ASSERT(!cluster_gcs_block_read_grant_holder_notify(&service, 2));
	UT_ASSERT_EQ(send_holder_handed, 1);
	ClusterPcmOwnArray = saved;
}

static void
send_unknown_identity(int reason)
{
	if (reason == 0) send_master = -1;
	if (reason == 1) send_session = false;
	if (reason == 2) send_master_incarnation = 0;
}

static void
send_restore_identity(void)
{
	send_master = 2;
	send_session = true;
	send_master_incarnation = 9;
	send_route_unknown_after = send_auth_unknown_after = -1;
}

/* A lookup/authentication gap must not clear bytes or manufacture a NACK.
 * Only the external identity query and actual transport outcome are faked. */
UT_TEST(saved_reply_survives_unknown_master_identity_until_original_session_returns)
{
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	for (int reason = 0; reason < 3; reason++) {
		BufferDesc buf;
		ClusterPcmOwnEntry entry;
		GcsBlockFirstFillForwardPayload fwd = send_setup(&buf, &entry);
		GcsFirstFillHolderPark original;
		send_forward(&fwd);
		original = gcs_first_fill_holder_parks[0];
		buf.tag.blockNum++;
		pg_atomic_fetch_add_u64(&entry.generation, 10);
		send_unknown_identity(reason);
		send_now += 10000000;
		send_result = CLUSTER_IC_SEND_DONE;
		for (int i = 0; i < 10; i++) send_tick();
		UT_ASSERT_EQ(send_owned(), 1);
		UT_ASSERT_EQ(send_calls, 1);
		UT_ASSERT_EQ(send_nacks + send_returns, 0);
		UT_ASSERT_EQ(gcs_first_fill_holder_parks[0].reply_size, original.reply_size);
		UT_ASSERT(memcmp(gcs_first_fill_holder_parks[0].reply, original.reply, original.reply_size) == 0);
		UT_ASSERT(memcmp(&gcs_first_fill_holder_parks[0].forward, &fwd, sizeof(fwd)) == 0);
		send_restore_identity();
		send_tick();
		UT_ASSERT_EQ(send_owned(), 0);
		UT_ASSERT_EQ(send_admitted, 1);
		UT_ASSERT_EQ(send_calls, 2);
		UT_ASSERT_EQ(send_copies, 1);
	}
	ClusterPcmOwnArray = saved;
}

UT_TEST(completed_write_keeps_original_wait_during_unknown_master_identity)
{
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	for (int reason = 0; reason < 3; reason++) {
		BufferDesc buf;
		ClusterPcmOwnEntry entry;
		GcsBlockFirstFillForwardPayload fwd = send_setup(&buf, &entry);
		ClusterPcmFirstFillWait original;
		pg_atomic_fetch_or_u32(&buf.state, BM_IO_IN_PROGRESS);
		pg_atomic_write_u64(&entry.write_io_generation, 1);
		send_forward(&fwd);
		original = gcs_first_fill_holder_parks[0].wait;
		pg_atomic_fetch_and_u32(&buf.state, ~BM_IO_IN_PROGRESS);
		pg_atomic_write_u64(&entry.write_io_generation, 2);
		send_unknown_identity(reason);
		for (int i = 0; i < 10; i++) send_tick();
		UT_ASSERT_EQ(send_owned(), 1);
		UT_ASSERT_EQ(send_calls + send_copies + send_nacks + send_returns, 0);
		UT_ASSERT(memcmp(&gcs_first_fill_holder_parks[0].wait, &original, sizeof(original)) == 0);
		send_restore_identity();
		send_result = CLUSTER_IC_SEND_DONE;
		send_tick();
		UT_ASSERT_EQ(send_admitted, 1);
		UT_ASSERT_EQ(send_copies, 1);
		UT_ASSERT_EQ(send_owned(), 0);
	}
	ClusterPcmOwnArray = saved;
}

UT_TEST(admitted_image_keeps_unsent_holder_notice_during_unknown_identity)
{
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	for (int reason = 0; reason < 3; reason++) {
		BufferDesc buf;
		ClusterPcmOwnEntry entry;
		GcsBlockFirstFillForwardPayload fwd = send_x_fixture(&buf, &entry);
		unsigned offers;
		send_result = CLUSTER_IC_SEND_DONE;
		send_forward(&fwd);
		offers = send_holder_offers;
		send_unknown_identity(reason);
		send_notice_admit = true;
		for (int i = 0; i < 10; i++) send_tick();
		UT_ASSERT_EQ(send_owned(), 1);
		UT_ASSERT_EQ(send_holder_offers, offers);
		UT_ASSERT_EQ(send_calls, 1);
		UT_ASSERT_EQ(send_nacks + send_returns, 0);
		send_restore_identity();
		send_tick();
		UT_ASSERT_EQ(send_holder_handed, 1);
		UT_ASSERT_EQ(send_owned(), 0);
		UT_ASSERT_EQ(send_calls, 1);
	}
	ClusterPcmOwnArray = saved;
}

UT_TEST(quiet_return_keeps_exact_consumption_receipt_during_unknown_identity)
{
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	for (int reason = 0; reason < 3; reason++) {
		BufferDesc buf;
		ClusterPcmOwnEntry entry;
		GcsBlockFirstFillForwardPayload fwd = send_setup(&buf, &entry);
		ClusterPcmReadGrant service;
		GcsBlockDonePayload done = {0};
		send_copy_failure = true;
		send_forward(&fwd);
		UT_ASSERT_EQ(send_returns, 1);
		send_unknown_identity(reason);
		send_now += 10000000;
		for (int i = 0; i < 10; i++) send_tick();
		UT_ASSERT_EQ(send_owned(), 1);
		UT_ASSERT_EQ(send_returns, 1);
		UT_ASSERT_EQ(send_nacks + send_calls, 0);
		send_restore_identity();
		UT_ASSERT(GcsBlockFirstFillForwardPayloadGrant(&fwd, cluster_node_id, &service));
		done.request_id = 71; done.epoch = 1; done.tag = fwd.forward.tag;
		done.sender_node = 2; done.requester_backend_id = 3;
		done.transition_id = PCM_TRANS_N_TO_S;
		done.reserved_0[0] = GCS_BLOCK_DONE_PENDING_S_RETURNED;
		done.reserved_0[1] = 1;
		GcsBlockDonePayloadSetPendingSService(&done, &service);
		UT_ASSERT(gcs_first_fill_holder_returned(&done, 9));
		UT_ASSERT_EQ(send_owned(), 0);
	}
	ClusterPcmOwnArray = saved;
}

UT_TEST(terminal_nack_waits_for_known_original_master_before_handoff)
{
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	for (int reason = 0; reason < 3; reason++) {
		BufferDesc buf;
		ClusterPcmOwnEntry entry;
		GcsBlockFirstFillForwardPayload fwd = send_setup(&buf, &entry);
		send_result = CLUSTER_IC_SEND_HARD_ERROR;
		send_notice_admit = false;
		send_forward(&fwd);
		UT_ASSERT_EQ(send_nacks, 1);
		send_unknown_identity(reason);
		send_notice_admit = true;
		for (int i = 0; i < 10; i++) send_tick();
		UT_ASSERT_EQ(send_owned(), 1);
		UT_ASSERT_EQ(send_nacks, 1);
		send_restore_identity();
		send_tick();
		UT_ASSERT_EQ(send_owned(), 0);
		UT_ASSERT_EQ(send_nacks, 2);
		UT_ASSERT_EQ(send_calls, 1);
	}
	ClusterPcmOwnArray = saved;
}

UT_TEST(quiet_return_retires_on_committed_master_death_without_session_recovery)
{
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	for (int reason = 0; reason < 2; reason++) {
		BufferDesc buf;
		ClusterPcmOwnEntry entry;
		GcsBlockFirstFillForwardPayload fwd = send_setup(&buf, &entry);
		send_copy_failure = true;
		send_forward(&fwd);
		send_session = false;
		send_master_state = reason == 0 ? CLUSTER_MEMBER_DEAD : CLUSTER_MEMBER_REMOVED;
		send_tick();
		UT_ASSERT_EQ(send_owned(), 0);
		UT_ASSERT_EQ(send_returns, 1);
		UT_ASSERT_EQ(send_nacks + send_calls, 0);
	}
	ClusterPcmOwnArray = saved;
}

UT_TEST(unproven_ingress_master_identity_keeps_the_original_carrier)
{
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	for (int reason = 0; reason < 3; reason++) {
		BufferDesc buf;
		ClusterPcmOwnEntry entry;
		GcsBlockFirstFillForwardPayload fwd = send_setup(&buf, &entry);
		send_unknown_identity(reason);
		send_forward(&fwd);
		UT_ASSERT_EQ(send_deferred, 1);
		UT_ASSERT_EQ(send_owned() + send_copies + send_calls + send_nacks, 0);
		send_restore_identity();
		send_result = CLUSTER_IC_SEND_DONE;
		send_forward(&fwd); /* Redispatch the same retained carrier, no new REQUEST. */
		UT_ASSERT_EQ(send_deferred, 1);
		UT_ASSERT_EQ(send_admitted, 1);
		UT_ASSERT_EQ(send_owned(), 0);
	}
	ClusterPcmOwnArray = saved;
}

UT_TEST(resumption_identity_race_keeps_local_owner_without_deferring_a_fake_carrier)
{
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	for (int reason = 0; reason < 3; reason++) {
		BufferDesc buf;
		ClusterPcmOwnEntry entry;
		GcsBlockFirstFillForwardPayload fwd = send_setup(&buf, &entry);
		pg_atomic_fetch_or_u32(&buf.state, BM_IO_IN_PROGRESS);
		pg_atomic_write_u64(&entry.write_io_generation, 1);
		send_forward(&fwd);
		pg_atomic_fetch_and_u32(&buf.state, ~BM_IO_IN_PROGRESS);
		pg_atomic_write_u64(&entry.write_io_generation, 2);
		if (reason == 0) send_route_unknown_after = 1;
		else {
			send_auth_unknown_after = 1;
			send_auth_zero_after = reason == 2;
		}
		send_tick(); /* Identity becomes unknown between tick proof and real handler. */
		UT_ASSERT_EQ(send_owned(), 1);
		UT_ASSERT_EQ(send_deferred + send_copies + send_calls + send_nacks + send_returns, 0);
		send_restore_identity();
		send_result = CLUSTER_IC_SEND_DONE;
		send_tick();
		UT_ASSERT_EQ(send_admitted, 1);
		UT_ASSERT_EQ(send_owned(), 0);
	}
	ClusterPcmOwnArray = saved;
}

#ifndef FIRST_FILL_SEND_FIXTURE_ONLY
int
main(void)
{
	UT_PLAN(29);
	UT_RUN(saved_reply_survives_unknown_master_identity_until_original_session_returns);
	UT_RUN(completed_write_keeps_original_wait_during_unknown_master_identity);
	UT_RUN(admitted_image_keeps_unsent_holder_notice_during_unknown_identity);
	UT_RUN(quiet_return_keeps_exact_consumption_receipt_during_unknown_identity);
	UT_RUN(terminal_nack_waits_for_known_original_master_before_handoff);
	UT_RUN(quiet_return_retires_on_committed_master_death_without_session_recovery);
	UT_RUN(unproven_ingress_master_identity_keeps_the_original_carrier);
	UT_RUN(resumption_identity_race_keeps_local_owner_without_deferring_a_fake_carrier);
	UT_RUN(holder_notice_refusal_preserves_image_and_exact_notification_independently);
	UT_RUN(holder_notice_unknown_session_waits_without_cancel_or_image_replay);
	UT_RUN(holder_notice_ends_on_proved_master_reconfiguration_only);
	UT_RUN(normal_stop_drains_admitted_image_holder_notice_without_revoke);
	UT_RUN(holder_notice_and_unadmitted_image_transfer_separately_once);
	UT_RUN(wrong_holder_service_cannot_steal_original_notification_owner);
	UT_RUN(terminal_cancel_keeps_holder_notice_until_both_notices_transfer);
	UT_RUN(holder_callback_cannot_mint_unreserved_service_or_repeat_after_handoff);
	UT_RUN(quiet_return_ends_on_proved_master_reconfiguration_but_not_unknown_session);
	UT_RUN(quiet_holder_return_waits_for_exact_master_consumption);
	UT_RUN(terminal_holder_nack_retains_ownership_without_reselecting);
	UT_RUN(actual_holder_refusal_never_tells_requester_to_retry);
	UT_RUN(actual_handler_rejects_unbound_and_malformed_first_fill_before_io);
	UT_RUN(actual_handler_retains_rejected_reply_until_transport_admission);
	UT_RUN(immediate_reply_keeps_owned_bytes_after_sge_release);
	UT_RUN(admitted_would_block_is_never_resubmitted);
	UT_RUN(downgraded_reply_outlives_the_preproduction_generation);
	UT_RUN(retained_reply_checks_terminal_identity_and_stop_before_reoffer);
	UT_RUN(hard_error_terminates_with_exact_cancellation);
	UT_RUN(local_backoff_does_not_reoffer_before_the_next_service_tick);
	UT_RUN(reply_capacity_defers_new_work_before_production_and_preserves_owned_frames);
	UT_DONE();
	return ut_failed_count != 0;
}
#endif
