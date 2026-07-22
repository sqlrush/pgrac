/*-------------------------------------------------------------------------
 *
 * test_cluster_pcm_x_image_fetch.c
 *	  Pure identity and byte-validation tests for the PCM-X requester image
 *	  adapter.  The transport remains the established GCS BLOCK request/reply
 *	  family; these tests prove that its canonical image-id subdomain cannot
 *	  be confused with an ordinary block request or a different queue round.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_pcm_x_image_fetch.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_pcm_x_image_fetch.h"
#include "port/pg_crc32c.h"
#include "storage/bufpage.h"

#undef printf
#undef fprintf
#undef snprintf
#undef sprintf
#undef vsnprintf
#undef vfprintf
#undef vprintf
#undef vsprintf
#undef strerror
#undef strerror_r

#include "unit_test.h"

UT_DEFINE_GLOBALS();


void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}


uint32
cluster_gcs_block_compute_checksum(const char *block_data)
{
	pg_crc32c crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, block_data, GCS_BLOCK_DATA_SIZE);
	FIN_CRC32C(crc);
	return (uint32)crc;
}


static BufferTag
make_tag(uint32 blockno)
{
	BufferTag tag;

	memset(&tag, 0, sizeof(tag));
	tag.spcOid = 1663;
	tag.dbOid = 1;
	tag.relNumber = 910;
	tag.forkNum = MAIN_FORKNUM;
	tag.blockNum = blockno;
	return tag;
}


static PcmXLocalProgress
make_requester_progress(void)
{
	PcmXLocalProgress progress;
	uint64 image_id;

	memset(&progress, 0, sizeof(progress));
	progress.identity.tag = make_tag(17);
	progress.identity.node_id = 1;
	progress.identity.procno = 9;
	progress.identity.xid = (TransactionId)23;
	progress.identity.cluster_epoch = 0;
	progress.identity.request_id = gcs_reqid_requester(1, 2, 41);
	progress.identity.wait_seq = UINT64_C(43);
	progress.identity.base_own_generation = UINT64_C(47);
	progress.ref.identity = progress.identity;
	progress.ref.handle.ticket_id = UINT64_C(53);
	progress.ref.handle.queue_generation = UINT64_C(59);
	progress.ref.grant_generation = UINT64_C(61);
	UT_ASSERT(cluster_pcm_x_image_id_encode(2, UINT64_C(67), &image_id));
	progress.image.image_id = image_id;
	/* Generation zero is a legal first ownership generation. */
	progress.image.source_own_generation = 0;
	progress.image.page_scn = UINT64_C(71);
	progress.image.page_lsn = UINT64_C(73);
	progress.image.source_node = 3;
	progress.local_sequence = UINT64_C(79);
	progress.reliable_state_sequence = UINT64_C(83);
	progress.local_round = 1;
	progress.member_state = PCM_XL_REMOTE_WAIT;
	progress.role = PCM_X_LOCAL_ROLE_NODE_LEADER;
	progress.pending_opcode = 0;
	progress.last_response_opcode = PGRAC_IC_MSG_PCM_X_PREPARE_GRANT;
	progress.phase = 0;
	progress.master_session_incarnation = UINT64_C(89);
	progress.master_node = 2;
	return progress;
}


static void
prepare_page(char *page, PcmXLocalProgress *progress)
{
	PageHeader header;

	memset(page, 0x5a, GCS_BLOCK_DATA_SIZE);
	header = (PageHeader)page;
	PageXLogRecPtrSet(header->pd_lsn, (XLogRecPtr)progress->image.page_lsn);
	header->pd_block_scn = (SCN)progress->image.page_scn;
	progress->image.page_checksum = cluster_gcs_block_compute_checksum(page);
}


UT_TEST(test_fetch_request_uses_exact_image_handle_without_reserved_overlay)
{
	PcmXLocalProgress progress = make_requester_progress();
	GcsBlockRequestPayload request;
	static const uint8 zero_reserved[sizeof(request.reserved_0)] = { 0 };

	memset(&request, 0xa5, sizeof(request));
	UT_ASSERT(cluster_pcm_x_image_fetch_build_request(&progress, 1, 3, &request));
	UT_ASSERT_EQ(request.request_id, progress.image.image_id);
	UT_ASSERT_EQ(request.epoch, progress.identity.cluster_epoch);
	UT_ASSERT(memcmp(&request.tag, &progress.identity.tag, sizeof(request.tag)) == 0);
	UT_ASSERT_EQ(request.sender_node, 1);
	UT_ASSERT_EQ(request.requester_backend_id, 3);
	UT_ASSERT_EQ(request.transition_id, PCM_TRANS_N_TO_S);
	UT_ASSERT(memcmp(request.reserved_0, zero_reserved, sizeof(zero_reserved)) == 0);

	progress.ref.identity.wait_seq++;
	UT_ASSERT(!cluster_pcm_x_image_fetch_build_request(&progress, 1, 3, &request));
}


UT_TEST(test_fetch_holder_authenticates_requester_master_and_record_generation)
{
	PcmXLocalProgress requester = make_requester_progress();
	PcmXLocalHolderProgress holder;
	GcsBlockRequestPayload request;
	ClusterICEnvelope env;
	PcmXImageFetchRequestRefusal refusal;

	UT_ASSERT(cluster_pcm_x_image_fetch_build_request(&requester, 1, 3, &request));
	memset(&holder, 0, sizeof(holder));
	holder.ref = requester.ref;
	holder.image = requester.image;
	holder.reliable_state_sequence = UINT64_C(97);
	holder.pending_opcode = PGRAC_IC_MSG_PCM_X_IMAGE_READY;
	holder.last_response_opcode = PGRAC_IC_MSG_PCM_X_REVOKE;
	holder.phase = PGRAC_IC_MSG_PCM_X_IMAGE_READY;
	holder.master_session_incarnation = requester.master_session_incarnation;
	holder.master_node = requester.master_node;
	memset(&env, 0, sizeof(env));
	env.source_node_id = 1;
	env.dest_node_id = 3;
	env.payload_length = sizeof(request);

	UT_ASSERT(cluster_pcm_x_image_fetch_request_exact_diagnosed(&env, &request, &holder, 3, 2, 0,
																&refusal));
	UT_ASSERT_EQ((int)refusal, (int)PCM_X_IMAGE_FETCH_REQUEST_REFUSAL_NONE);
	env.source_node_id = 0;
	UT_ASSERT(!cluster_pcm_x_image_fetch_request_exact_diagnosed(&env, &request, &holder, 3, 2, 0,
																 &refusal));
	UT_ASSERT_EQ((int)refusal, (int)PCM_X_IMAGE_FETCH_REQUEST_REFUSAL_ENVELOPE);
	env.source_node_id = 1;
	holder.image.image_id++;
	UT_ASSERT(!cluster_pcm_x_image_fetch_request_exact_diagnosed(&env, &request, &holder, 3, 2, 0,
																 &refusal));
	UT_ASSERT_EQ((int)refusal, (int)PCM_X_IMAGE_FETCH_REQUEST_REFUSAL_HOLDER_IMAGE);
	holder.image = requester.image;
	holder.master_node = 0;
	UT_ASSERT(!cluster_pcm_x_image_fetch_request_exact_diagnosed(&env, &request, &holder, 3, 2, 0,
																 &refusal));
	UT_ASSERT_EQ((int)refusal, (int)PCM_X_IMAGE_FETCH_REQUEST_REFUSAL_HOLDER_MASTER);
}


UT_TEST(test_fetch_reply_reuses_gcs_checksum_and_rejects_old_or_torn_bytes)
{
	PcmXLocalProgress progress = make_requester_progress();
	GcsBlockReplyHeader reply;
	char page[GCS_BLOCK_DATA_SIZE];

	prepare_page(page, &progress);
	memset(&reply, 0, sizeof(reply));
	reply.request_id = progress.image.image_id;
	reply.page_lsn = progress.image.page_lsn;
	reply.epoch = progress.identity.cluster_epoch;
	reply.checksum = progress.image.page_checksum;
	reply.sender_node = (int32)progress.image.source_node;
	reply.requester_backend_id = 3;
	reply.transition_id = PCM_TRANS_N_TO_S;
	reply.status = GCS_BLOCK_REPLY_READ_IMAGE_FROM_XHOLDER;
	GcsBlockReplyHeaderSetForwardingMasterNode(&reply, GCS_BLOCK_REPLY_NO_FORWARDING_MASTER);

	UT_ASSERT(cluster_pcm_x_image_fetch_reply_exact(&reply, page, &progress, 1, 3));
	page[GCS_BLOCK_DATA_SIZE - 1] ^= 1;
	UT_ASSERT(!cluster_pcm_x_image_fetch_reply_exact(&reply, page, &progress, 1, 3));
	page[GCS_BLOCK_DATA_SIZE - 1] ^= 1;
	reply.request_id++;
	UT_ASSERT(!cluster_pcm_x_image_fetch_reply_exact(&reply, page, &progress, 1, 3));
	reply.request_id = progress.image.image_id;
	GcsBlockReplyHeaderSetForwardingMasterNode(&reply, 2);
	UT_ASSERT(!cluster_pcm_x_image_fetch_reply_exact(&reply, page, &progress, 1, 3));
}


UT_TEST(test_fetch_install_requires_the_exact_live_grant_reservation)
{
	ClusterPcmOwnSnapshot base;
	ClusterPcmOwnSnapshot live;

	memset(&base, 0, sizeof(base));
	base.tag = make_tag(17);
	base.generation = UINT64_C(47);
	base.reservation_token = UINT64_C(5);
	base.pcm_state = PCM_STATE_N;
	live = base;
	live.reservation_token = UINT64_C(6);
	live.flags = PCM_OWN_FLAG_GRANT_PENDING;
	UT_ASSERT(cluster_pcm_x_image_fetch_reservation_exact(&live, &base, UINT64_C(6)));
	live.generation++;
	UT_ASSERT(!cluster_pcm_x_image_fetch_reservation_exact(&live, &base, UINT64_C(6)));
	live = base;
	live.reservation_token = UINT64_C(6);
	live.flags = PCM_OWN_FLAG_REVOKING;
	UT_ASSERT(!cluster_pcm_x_image_fetch_reservation_exact(&live, &base, UINT64_C(6)));

	base.flags = PCM_OWN_FLAG_REVOKING;
	base.pcm_state = PCM_STATE_S;
	live = base;
	live.flags = PCM_OWN_FLAG_GRANT_PENDING;
	UT_ASSERT_EQ(cluster_pcm_x_grant_reservation_kind(&live, &base, UINT64_C(5)),
				 CLUSTER_PCM_X_GRANT_RESERVATION_S_REVOKE_HANDOFF);
	UT_ASSERT(!cluster_pcm_x_image_fetch_reservation_exact(&live, &base, UINT64_C(5)));
	live.reservation_token++;
	UT_ASSERT_EQ(cluster_pcm_x_grant_reservation_kind(&live, &base, UINT64_C(5)),
				 CLUSTER_PCM_X_GRANT_RESERVATION_INVALID);
}


int
main(void)
{
	UT_PLAN(4);
	UT_RUN(test_fetch_request_uses_exact_image_handle_without_reserved_overlay);
	UT_RUN(test_fetch_holder_authenticates_requester_master_and_record_generation);
	UT_RUN(test_fetch_reply_reuses_gcs_checksum_and_rejects_old_or_torn_bytes);
	UT_RUN(test_fetch_install_requires_the_exact_live_grant_reservation);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
