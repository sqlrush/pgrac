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
#include "cluster/cluster_ic_rdma.h"
#include "cluster/cluster_block_io_trace.h"
#include "cluster/cluster_gcs_block_dedup.h"
#include "cluster/cluster_xnode_profile.h"

/* Keep the native wait witness and the whole production forward handler.
 * Only peer transport and unrelated request families are fixture boundaries. */
static uint64 send_epoch = 1;
static TimestampTz send_now = 10;
static bool send_stop, send_session = true;
static int send_master = 2;
static unsigned send_calls, send_admitted, send_nacks, send_copies, send_releases;
static ClusterICSendResult send_result;
static char send_first[sizeof(GcsBlockReplyHeaderV2) + GCS_BLOCK_DATA_SIZE];
static size_t send_length;
static bool send_borrowed, send_downgrade, send_cancel_check = true;
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
	UT_ASSERT_EQ(action, GCS_BLOCK_DONE_PENDING_S_NACK);
	send_nacks++;
	return true;
}
static void gcs_block_forward_reply_immediate_deny(const GcsBlockForwardPayload *fwd)
{
	if (send_cancel_check) UT_ASSERT_EQ(fwd->request_id, 71);
}
static bool
send_auth(int master, uint64 epoch, uint64 *session)
{
	*session = 9;
	return send_session && master == send_master && epoch == send_epoch;
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
#define palloc0(n) calloc(1, (n))
#define pfree(p) free(p)
#define cluster_ic_tier1_my_data_channel() 0
#define GetCurrentTimestamp() send_now
#define cluster_epoch_get_current() send_epoch
#define cluster_normal_stop_requested() send_stop
#define cluster_gcs_lookup_master(tag) send_master
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
#define cluster_bufmgr_block_pcm_state(tag) PCM_LOCK_MODE_S
#define cluster_bufmgr_block_grant_pending(tag) false
#define gcs_block_get_ship_image send_image
#define gcs_block_lost_write_verdict(expected, shipped) ((void)(expected), (void)(shipped), GCS_LOST_WRITE_SKIP)
#define gcs_block_compute_checksum cluster_gcs_block_compute_checksum
#define cluster_itl_page_has_active_slot(page) false
#define cluster_xp_begin(scope, op) ((void)0)
#define cluster_xp_end(scope) ((void)(scope))
#define cluster_bufmgr_drop_block_for_gcs_no_wire(tag, lsn, out) ((void)(out), CLUSTER_BUFMGR_GCS_DROP_NOT_RESIDENT)
#define GCS_BLOCK_LOG_MASTER_NOT_HOLDER_FORWARD(...) ((void)0)
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
	memset(transition_page.data, 0x5a, GCS_BLOCK_DATA_SIZE);
	memset(send_first, 0, sizeof(send_first));
	send_length = send_calls = send_admitted = send_nacks = send_copies = send_releases = 0;
	send_epoch = 1;
	send_now = 10;
	send_stop = false;
	send_session = true;
	send_master = 2;
	send_borrowed = send_downgrade = false;
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
		UT_ASSERT_EQ(send_owned(), 0);
		UT_ASSERT_EQ(send_nacks, 1);
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

UT_TEST(reply_capacity_cancels_only_new_work_and_preserves_owned_frames)
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
	UT_ASSERT_EQ(send_nacks, 1);
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

int
main(void)
{
	UT_PLAN(9);
	UT_RUN(actual_handler_rejects_unbound_and_malformed_first_fill_before_io);
	UT_RUN(actual_handler_retains_rejected_reply_until_transport_admission);
	UT_RUN(immediate_reply_keeps_owned_bytes_after_sge_release);
	UT_RUN(admitted_would_block_is_never_resubmitted);
	UT_RUN(downgraded_reply_outlives_the_preproduction_generation);
	UT_RUN(retained_reply_checks_terminal_identity_and_stop_before_reoffer);
	UT_RUN(hard_error_terminates_with_exact_cancellation);
	UT_RUN(local_backoff_does_not_reoffer_before_the_next_service_tick);
	UT_RUN(reply_capacity_cancels_only_new_work_and_preserves_owned_frames);
	UT_DONE();
	return ut_failed_count != 0;
}
