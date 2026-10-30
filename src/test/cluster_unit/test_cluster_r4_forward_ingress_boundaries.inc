/*-------------------------------------------------------------------------
 *
 * test_cluster_r4_forward_ingress_boundaries.inc
 *    Unentered native/legacy boundaries for the registered CR ingress test.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_r4_forward_ingress_boundaries.inc
 *
 * NOTES
 *    This is a pgrac-original file.
 *
 *-------------------------------------------------------------------------
 */

#include "cluster/cluster_itl.h"
#include "cluster/cluster_write_fence.h"

/* Linking the registered production handler also retains its legacy holder,
 * first-fill, RDMA and CTRC callees. These paths must never execute for the
 * selected CR frames. Abort rather than manufacture a successful authority,
 * image, transport or retention result. This fixture proves dispatch only. */
void
cluster_block_io_trace_note(BufferTag tag, const char *event, uint64 version, uint64 sequence,
							uint32 holders)
{
	abort();
}

bool
cluster_bufmgr_block_grant_pending(BufferTag tag)
{
	abort();
}

bool
cluster_bufmgr_block_is_pi(BufferTag tag)
{
	abort();
}

PcmLockMode
cluster_bufmgr_block_pcm_state(BufferTag tag)
{
	abort();
}

bool
cluster_bufmgr_borrow_block_for_gcs_live_sge(BufferTag tag, XLogRecPtr *out_page_lsn,
											 void **out_page_addr, BufferDesc **out_buf)
{
	abort();
}

bool
cluster_bufmgr_copy_block_for_gcs(BufferTag tag, XLogRecPtr *out_page_lsn, char *dst,
								  ClusterBufmgrGcsCopyRefusal *out_refusal,
								  ClusterPageWalBindingV1 *out_wal, ClusterPageWalRefV1 *out_first,
								  uint32 peer_capabilities)
{
	abort();
}

bool
cluster_bufmgr_copy_block_for_gcs_smart_fusion(BufferTag tag, XLogRecPtr *out_page_lsn, char *dst,
											   ClusterSfDepVec *out_dep_vec)
{
	abort();
}

bool
cluster_bufmgr_downgrade_x_to_s_remote_for_gcs(BufferTag tag, int32 master_node)
{
	abort();
}

ClusterBufmgrGcsDowngradeOutcome
cluster_bufmgr_downgrade_x_to_s_remote_for_gcs_prepare_image(
	BufferTag tag, int32 master_node, XLogRecPtr *out_page_lsn, char *dst,
	ClusterBufmgrGcsCopyRefusal *out_refusal)
{
	abort();
}

ClusterBufmgrGcsDropResult
cluster_bufmgr_drop_block_for_gcs_no_wire(BufferTag tag, XLogRecPtr expected_lsn,
										  XLogRecPtr *out_page_lsn)
{
	abort();
}

ClusterBufmgrGcsDowngradeOutcome
cluster_bufmgr_first_fill_s_prepare_image(const ClusterPcmReadGrant *service, int32 master_node,
										  XLogRecPtr *out_page_lsn, char *dst,
										  ClusterBufmgrGcsCopyRefusal *out_refusal)
{
	abort();
}

bool
cluster_bufmgr_first_fill_wait_begin(BufferTag tag, int worker, ClusterPcmFirstFillWait *wait)
{
	abort();
}

const char *
cluster_bufmgr_gcs_copy_refusal_name(ClusterBufmgrGcsCopyRefusal refusal)
{
	abort();
}

void
cluster_bufmgr_release_block_for_gcs_live_sge(BufferDesc *buf)
{
	abort();
}

bool
cluster_bufmgr_renotify_s_for_gcs(BufferTag tag, int32 master_node)
{
	abort();
}

bool
cluster_cr_server_freshref_c1b_pair_request_decode(const GcsBlockForwardPayload *fwd,
												   int32 authenticated_source_node,
												   int32 local_node, uint64 current_epoch,
												   int max_backends, uint32 *segment_id,
												   TransactionId *xid, uint32 *expected_tt_slot_id,
												   SCN *proposed_scn)
{
	abort();
}

ClusterCtrcSealReplyResult
cluster_ctrc_participant_request_shared(const ClusterCtrcTxnKeyV1 *key,
										const ClusterCtrcParticipantIdentity *identity,
										uint32 grant_generation, uint64 seal_generation,
										ClusterCtrcSealSuboperation suboperation,
										uint16 *first_reason, ClusterCtrcLocalReleaseAckV1 *ack_out)
{
	abort();
}

bool
cluster_ctrc_seal_reply_encode(const uint8 *request_bytes, Size request_length, int32 source_node,
							   int32 destination_node, ClusterCtrcSealReplyResult result,
							   uint16 first_reason, const ClusterCtrcLocalReleaseAckV1 *ack,
							   uint8 *page, Size page_length)
{
	abort();
}

void
cluster_gcs_block_forward_serve_inline(const GcsBlockForwardPayload *fwd, ClusterLmsCrSlotKind kind)
{
	abort();
}

void
cluster_ic_dispatch_defer(const ClusterICEnvelope *env)
{
	abort();
}

uint32
cluster_ic_local_capability_word(void)
{
	abort();
}

ClusterICPeerTransport
cluster_ic_mux_peer_transport(int32 peer_id)
{
	abort();
}

bool
cluster_ic_rdma_block_sge_supported(const char **reason)
{
	abort();
}

bool
cluster_ic_rdma_borrow_block_scratch(int32 peer_id, size_t len, void **out_addr, uint32 *out_lkey,
									 ClusterICSgeReleaseCallback *out_release_cb,
									 void **out_release_arg)
{
	abort();
}

ClusterICSendResult
cluster_ic_rdma_send_block_reply_direct(int32 dest_node_id, const ClusterICSge *payload_sge,
										int n_sge, uint32 payload_len)
{
	abort();
}

ClusterICSendResult
cluster_ic_rdma_send_envelope_sge(uint8 msg_type, int32 dest_node_id,
								  const ClusterICSge *payload_sge, int n_sge, uint32 payload_len)
{
	abort();
}

bool
cluster_ic_rdma_shared_buffers_sge(void *addr, size_t len, uint32 *out_lkey)
{
	abort();
}

bool
cluster_itl_page_has_active_slot(Page page)
{
	abort();
}

void
cluster_lever_a_note_remote_downgrade(bool downgraded)
{
	abort();
}

void
cluster_lever_e2_note_nudge_result(bool yielded)
{
	abort();
}

bool
cluster_lms_cr_submit(const GcsBlockForwardPayload *fwd)
{
	abort();
}

bool
cluster_lms_undo_fetch_submit(const GcsBlockForwardPayload *fwd)
{
	abort();
}

bool
cluster_lms_undo_multi_verdict_submit(const GcsBlockForwardPayload *fwd)
{
	abort();
}

bool
cluster_lms_undo_verdict_submit(const GcsBlockForwardPayload *fwd)
{
	abort();
}

void
cluster_lms_wakeup(int worker_id)
{
	abort();
}

bool
cluster_pcm_lock_authority_snapshot(BufferTag tag, PcmAuthoritySnapshot *out)
{
	abort();
}

void
cluster_pcm_lock_pi_holder_note(BufferTag tag, int32 holder_node)
{
	abort();
}

bool
cluster_sf_peer_supports_reply_v2(int32 peer_id)
{
	abort();
}

bool
cluster_write_fence_allowed(void)
{
	abort();
}

bool
cluster_write_fence_enforcing(void)
{
	abort();
}

int
errdetail_internal(const char *fmt, ...)
{
	abort();
}

void *
palloc0(Size size)
{
	abort();
}

void
pfree(void *pointer)
{
	abort();
}

static int route_ctrc_decode_calls;

/* The foreign-domain negative reaches this decoder boundary, which refuses
 * without producing a key, reply or participant authority. */
bool
cluster_ctrc_seal_request_decode(const uint8 *bytes, Size length,
								 uint64 authenticated_system_identifier, int32 envelope_source_node,
								 int32 local_node, uint64 current_epoch,
								 ClusterCtrcSealRequestV1 *request_out,
								 ClusterCtrcTxnKeyV1 *key_out)
{
	route_ctrc_decode_calls++;
	memset(request_out, 0, sizeof(*request_out));
	memset(key_out, 0, sizeof(*key_out));
	return false;
}
