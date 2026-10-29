/*-------------------------------------------------------------------------
 * test_cluster_native_image_episode.c
 *    Original current-image installation, IO and cleanup handoff.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_native_image_episode.c
 * NOTES
 *    Native copy/C/IO, PCM, codec, cleanup and send callbacks are real.
 *    Master/source inputs and process/content events are controlled leaves;
 *    this does not qualify master source settlement or a native cluster.
 *-------------------------------------------------------------------------
 */
#include "test_cluster_native_disk_episode_fixture.inc"
#include "cluster/cluster_space_reservation.h"

/* The product WAL sidecar uses the same actual descriptor as the native
 * episode; only this fixture's one-buffer address is supplied here. */
BufferDescPadded *BufferDescriptors = (BufferDescPadded *)&request_buf;
uint32
LockBufHdr(BufferDesc *buf)
{
	return request_header_lock(buf);
}

#ifndef IMAGE_EPISODE_RED
#define LockBufHdr stage_header_lock
#define GetPrivateRefCount(buffer) request_private_pin(buffer)
#define LWLockAcquire stage_content_acquire
#define LWLockRelease stage_content_release
#define cluster_bufmgr_pcm_own_acquire_execute_begin_exact dg_execute
#define cluster_pcm_lock_resource_x_requester_acquire_read_arm_exact dg_arm
#define cluster_bufmgr_pcm_own_acquire_continue_disk_exact de_continue
#define ereport(level, rest) dg_native_report(level)
#include "test_cluster_native_image_episode.inc"
#undef ereport
#undef cluster_bufmgr_pcm_own_acquire_continue_disk_exact
#undef cluster_pcm_lock_resource_x_requester_acquire_read_arm_exact
#undef cluster_bufmgr_pcm_own_acquire_execute_begin_exact
#undef LWLockRelease
#undef LWLockAcquire
#undef GetPrivateRefCount
#undef LockBufHdr
#endif

static PGIOAlignedBlock im_expected;
static ClusterPageWalBindingV1 im_wal;

static bool
im_start_full(EvidenceFixture *f, int profile, unsigned shape, bool with_wal)
{
	ResourceXDecodedImageEnvelope *i;
	ResourceXDecodedBlockedToN *proof;
	ResourceXWireReject reject;
	uint32 component;
	PageHeader header = (PageHeader)im_expected.data;
	if (!de_start(f) || !input_begin(f, profile, -1)) return false;
	memset(&im_expected, 0x53, sizeof(im_expected));
	memset(im_expected.data, 0, SizeOfPageHeaderData);
	header->pd_lower = shape == 1 ? 1 : SizeOfPageHeaderData;
	header->pd_upper = header->pd_special = BLCKSZ;
	header->pd_pagesize_version = BLCKSZ | PG_PAGE_LAYOUT_VERSION;
	PageSetLSNPreserveOrigin((Page)im_expected.data, 12345);
	header->pd_block_scn = 93;
	if (shape == 2) header->pd_upper = BLCKSZ + 1;
	if (shape == 3) header->pd_special--;
	if (shape == 4) header->pd_pagesize_version = 0;
	if (shape == 5) header->pd_flags = PD_SPACE_METADATA;
	if (shape == 6) header->pd_flags = PD_UNDO_SEG_HEADER;
	if (shape == 7) header->pd_flags = (uint16)~PD_VALID_FLAG_BITS;
	if (shape == 8) memset(&im_expected, 0, sizeof(im_expected));
	memset(&im_wal, 0, sizeof(im_wal));
	if (with_wal) {
		im_wal.source.claim.identity.system_identifier = 123;
		im_wal.source.claim.identity.origin_node_id = 2;
		im_wal.source.claim.identity.origin_thread_id = 3;
		im_wal.source.claim.identity.origin_owner_incarnation = 71;
		im_wal.source.claim.identity.thread_claim_created_at = 8;
		im_wal.source.claim.identity.root_lineage_seq = 9;
		im_wal.source.claim.identity.storage_uuid[0] = 10;
		im_wal.source.claim.identity.authority_uuid[0] = 11;
		im_wal.source.claim.database_incarnation = 12;
		im_wal.source.claim.max_config_generation = 13;
		im_wal.source.claim.claim_sha256[0] = 14;
		im_wal.source.timeline = 1;
		im_wal.identity.system_identifier = 123;
		memcpy(im_wal.identity.storage_uuid, im_wal.source.claim.identity.storage_uuid, 16);
		im_wal.identity.locator = BufTagGetRelFileLocator(&request_base.tag);
		im_wal.identity.forknum = request_base.tag.forkNum;
		im_wal.identity.blockno = request_base.tag.blockNum;
		im_wal.version.segment_incarnation[0] = 15;
		im_wal.version.mutation_token = header->pd_block_scn;
		im_wal.record_start = 100;
		im_wal.record_end = PageGetLSN(im_expected.data);
		im_wal.record_crc = 30;
		im_wal.rmid = RM_HEAP_ID;
		im_wal.flags = CLUSTER_PAGE_WAL_NATIVE_FLUSHED;
		UT_ASSERT(PageSetLSNOrigin(im_expected.data, 2));
		local_pi_writer = im_wal.source;
		local_pi_writer.claim.identity.origin_node_id = 1;
		local_pi_writer.claim.identity.origin_thread_id = 2;
		local_pi_writer_ready = true;
		pg_atomic_write_u32(&request_buf.state, pg_atomic_read_u32(&request_buf.state) | BM_PERMANENT);
	}
	/* A current memory image legitimately has its previous disk checksum. */
	if (shape != 8)
		header->pd_checksum = pg_checksum_page(im_expected.data, request_base.tag.blockNum) ^ UINT16_MAX;
	i = &f->image.body.acquire_image.original_image;
	proof = &f->image.body.acquire_image.source.original_proof;
	i->image_flags = with_wal ? RESOURCE_X_IMAGE_HAS_WAL : 0;
	i->page_wal = im_wal;
	memcpy(i->page_bytes, im_expected.data, BLCKSZ);
	i->page_scn_lsn = proof->page_scn_lsn = header->pd_block_scn;
	i->page_checksum = proof->page_checksum = cluster_gcs_block_compute_checksum(im_expected.data);
	if (!cluster_resource_x_wire_acquire_image_crc(&f->image.body.acquire_image, &component, &reject)) return false;
	proof->source_proof_crc32c = component;
	if (!evidence_wire(&f->image)) return false;
	input_complete(f, profile);
	if (claim_get() != RESOURCE_X_APPLY_APPLIED) return false;
	prepared_episode = f->decision.body.acquire_decision.episode;
	if (stage_enter() != RESOURCE_X_APPLY_APPLIED) return false;
	CurrentResourceOwner = ResourceOwnerCreate(NULL, "native-image");
	original_io_owner = CurrentResourceOwner;
	ResourceOwnerEnlargeBufferIOs(CurrentResourceOwner);
	ResourceOwnerRememberBufferIO(CurrentResourceOwner, 1);
	input_grant = f->decision;
	input_grant.common.sender_connection_generation = PGRAC_RESOURCE_X_RETAINED_SENDER_GENERATION;
	if (!evidence_wire(&input_grant) || !dg_locate_settlement()) return false;
	armed_capability = prepared_claim;
	cluster_enabled = cluster_shared_config = true;
	cluster_page_wal_shmem_init();
	BufferBlocks = disk_shared.data;
	memset(&disk_shared, 0x7B, sizeof(disk_shared));
	auth_peer_boot = 31;
	request_rechecks = 0; request_hook = NULL;
	stage_on_content = stage_after_content = NULL;
	complete_before_io_error = complete_after_io_error = complete_publish_error = false;
	complete_commits = complete_io_calls = complete_broadcasts = 0;
	quiet_aborts = ua_aborts = 0;
	dg_boundary = dg_fault = dg_panics = de_boundary = de_fault = 0;
	dg_stack_base = disk_exit_count;
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose, 3);
	return true;
}

static bool
im_start(EvidenceFixture *f, int profile, unsigned shape)
{
	return im_start_full(f, profile, shape, false);
}

static ResourceXApplyResult
im_run(void)
{
#ifdef IMAGE_EPISODE_RED
	return cluster_bufmgr_pcm_own_acquire_fill_disk_exact(&request_buf, &request_base,
		&prepared_episode, &request_admission, &prepared_claim);
#else
	return cluster_bufmgr_pcm_own_acquire_fill_image_exact(&request_buf, &request_base,
		&prepared_episode, &request_admission, &prepared_claim);
#endif
}

static bool
im_error(void)
{
	volatile bool caught = false;
	PG_TRY(); { (void)im_run(); } PG_CATCH(); { caught = true; } PG_END_TRY();
	return caught;
}

static void
im_exit(void)
{
	sigjmp_buf *saved = PG_exception_stack;
	ErrorContextCallback *context = error_context_stack;
	if (sigsetjmp(dg_fatal_jump, 0) == 0) { (void)im_run(); UT_ASSERT(false); }
	PG_exception_stack = saved;
	error_context_stack = context;
}

static void
im_unpublished(unsigned phase)
{
	UT_ASSERT_EQ(complete_commits + complete_io_calls + complete_broadcasts + disk_reads, 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&request_buf.state) & (BM_VALID | BM_IO_IN_PROGRESS | BM_IO_ERROR), BM_IO_ERROR);
	UT_ASSERT(!ResourceOwnerHasBufferIO(original_io_owner, 1));
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), request_base.generation);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 0);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose, phase);
	dg_stack_empty();
}

static ResourceXDecodedFrame
im_notice(void)
{
	ResourceXDecodedFrame frame = {0}; ResourceXWireReject reject;
	UT_ASSERT(cluster_resource_x_wire_decode(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE,
		qn_intent->payload, qn_intent->slot.payload_bytes, &frame, &reject));
	return frame;
}

static ResourceXApplyResult
im_ack(ResourceXDecodedFrame frame)
{
	uint8 bytes[RESOURCE_X_SOURCE_COMPLETE_V11_BYTES]; uint16 len; ResourceXWireReject reject;
	frame.common.action_node = 0;
	frame.common.outcome = frame.common.outcome == RESOURCE_X_OUTCOME_QUIET ? RESOURCE_X_OUTCOME_QUIET_RETIRED
		: frame.common.outcome == RESOURCE_X_OUTCOME_QUIET_NO_IMAGE ? RESOURCE_X_OUTCOME_QUIET_NO_IMAGE_RETIRED
		: RESOURCE_X_OUTCOME_DUPLICATE;
	if (!cluster_resource_x_wire_encode(RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, &frame,
		bytes, sizeof(bytes), &len, &reject) || !cluster_resource_x_wire_decode(
		RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE, bytes, len, &frame, &reject)) return RESOURCE_X_APPLY_INVALID;
	return cluster_pcm_lock_resource_x_requester_acquire_read_consumed_exact(&frame, 0, 61, 0, 61, 77);
}

static void
im_installed(unsigned io_calls)
{
	UT_ASSERT_EQ(complete_commits, 1); UT_ASSERT_EQ(complete_io_calls, io_calls);
	UT_ASSERT_EQ(complete_broadcasts, 1); UT_ASSERT_EQ(disk_reads, 0);
	UT_ASSERT_EQ(disk_enters + disk_opens + disk_bad_pages, 0);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), request_base.generation + 1);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0); UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 0);
	UT_ASSERT_EQ(request_buf.pcm_state, PCM_STATE_S); UT_ASSERT_EQ(request_buf.buffer_type, BUF_TYPE_SCUR);
	UT_ASSERT_EQ(pg_atomic_read_u32(&request_buf.state) & CLUSTER_PCM_OWN_SEMANTIC_BUF_STATE_MASK, BM_TAG_VALID | BM_VALID);
	UT_ASSERT(!ResourceOwnerHasBufferIO(original_io_owner, 1));
	UT_ASSERT(memcmp(disk_shared.data, im_expected.data, BLCKSZ) == 0);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose, RESOURCE_X_DELIVERY_ACQUIRE_READ_NOTICE);
	UT_ASSERT_EQ(qn_intent->slot.payload_bytes, RESOURCE_X_SOURCE_COMPLETE_V11_BYTES);
	UT_ASSERT_EQ(quiet_aborts + ua_aborts, 0); dg_stack_empty();
}

UT_TEST(test_image_episode_S_and_X_sources_copy_commit_and_handoff_once)
{
	for (int p = 2; p <= 3; p++) {
		EvidenceFixture f; UT_ASSERT(im_start(&f, p, false));
		UT_ASSERT_EQ(im_run(), RESOURCE_X_APPLY_APPLIED); im_installed(1);
	}
}
UT_TEST(test_image_episode_memory_checksum_is_not_a_disk_checksum)
{
	EvidenceFixture f; UT_ASSERT(im_start(&f, 3, false));
	UT_ASSERT(((PageHeader)im_expected.data)->pd_checksum != pg_checksum_page(im_expected.data, request_base.tag.blockNum));
	UT_ASSERT_EQ(im_run(), RESOURCE_X_APPLY_APPLIED); im_installed(1);
}
UT_TEST(test_image_episode_ACK_and_real_callback_end_only_original_notice)
{
	EvidenceFixture f; ResourceXIntentSlot slot; ResourceXDecodedFrame notice;
	UT_ASSERT(im_start(&f, 2, false)); UT_ASSERT_EQ(im_run(), RESOURCE_X_APPLY_APPLIED);
	if (qn_intent->slot.state == RESOURCE_X_INTENT_SLOT_EMPTY) return;
	notice = im_notice(); slot = qn_stage();
	UT_ASSERT_EQ(im_ack(notice), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&slot, true, ++fake_pcm_clock_us));
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose, RESOURCE_X_DELIVERY_ACQUIRE_READ_SETTLED);
	UT_ASSERT_EQ(complete_commits, 1); UT_ASSERT_EQ(disk_reads, 0);
}
UT_TEST(test_image_episode_bad_shape_cannot_publish_or_copy)
{
	EvidenceFixture f; PGIOAlignedBlock before;
	UT_ASSERT(im_start(&f, 2, true)); before = disk_shared;
	UT_ASSERT(im_run() != RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(complete_commits, 0); UT_ASSERT_EQ(disk_reads, 0);
	UT_ASSERT(memcmp(&before, &disk_shared, sizeof(before)) == 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&request_buf.state) & BM_VALID, 0);
	dg_stack_empty();
}
UT_TEST(test_image_episode_ERROR_after_arming_owns_verified_quiet)
{
	EvidenceFixture f; ResourceXIntentSlot slot;
	UT_ASSERT(im_start(&f, 3, false)); dg_boundary = 4; dg_fault = 1;
	UT_ASSERT(im_error()); UT_ASSERT_EQ(complete_commits, 0);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose, RESOURCE_X_DELIVERY_ACQUIRE_READ_QUIET);
	UT_ASSERT_EQ(quiet_aborts, 1); UT_ASSERT_EQ(disk_reads, 0); UT_ASSERT(qn_probe(&slot));
	UT_ASSERT_EQ(im_notice().common.outcome, RESOURCE_X_OUTCOME_QUIET); dg_stack_empty();
}
UT_TEST(test_image_episode_ERROR_after_native_C_finishes_original_IO)
{
	EvidenceFixture f; UT_ASSERT(im_start(&f, 2, false)); complete_before_io_error = true;
	UT_ASSERT(im_error()); im_installed(2);
}

UT_TEST(test_image_episode_all_header_refusals_precede_shared_copy)
{
	for (unsigned shape = 1; shape <= 7; shape++) {
		EvidenceFixture f; PGIOAlignedBlock before;
		UT_ASSERT(im_start(&f, 2, shape)); before = disk_shared;
		UT_ASSERT(im_run() != RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT(memcmp(&before, &disk_shared, sizeof(before)) == 0);
		im_unpublished(RESOURCE_X_DELIVERY_ACQUIRE_UNARMED_QUIET);
	}
}
UT_TEST(test_image_episode_empty_current_image_is_all_zero_only)
{
	EvidenceFixture f; UT_ASSERT(im_start(&f, 2, 8));
	UT_ASSERT_EQ(im_run(), RESOURCE_X_APPLY_APPLIED); im_installed(1);
}
UT_TEST(test_image_episode_ERROR_before_and_after_execution_seals_no_image)
{
	for (unsigned n = 1; n <= 2; n++) {
		EvidenceFixture f; UT_ASSERT(im_start(&f, 2, 0)); dg_boundary = n; dg_fault = 1;
		UT_ASSERT(im_error()); im_unpublished(RESOURCE_X_DELIVERY_ACQUIRE_UNARMED_QUIET);
	}
}
UT_TEST(test_image_episode_ERROR_during_copy_releases_content_before_outer_quiet)
{
	EvidenceFixture f; UT_ASSERT(im_start(&f, 3, 0));
	stage_on_content = stage_header_error;
	UT_ASSERT(im_error()); im_unpublished(RESOURCE_X_DELIVERY_ACQUIRE_UNARMED_QUIET);
}
UT_TEST(test_image_episode_ERROR_after_IO_and_publication_never_repeats_C)
{
	for (unsigned n = 0; n < 3; n++) {
		EvidenceFixture f; UT_ASSERT(im_start(&f, 2, 0));
		if (n == 0) complete_after_io_error = true;
		else if (n == 1) complete_publish_error = true;
		else { de_boundary = 3; de_fault = 1; }
		UT_ASSERT(im_error()); im_installed(1);
	}
}
UT_TEST(test_image_episode_FATAL_before_C_uses_original_LIFO_quiet)
{
	static const unsigned cuts[] = {1, 2, 4};
	for (unsigned i = 0; i < lengthof(cuts); i++) {
		unsigned n = cuts[i]; EvidenceFixture f;
		UT_ASSERT(im_start(&f, 3, 0)); dg_boundary = n; dg_fault = 2;
		im_exit(); im_unpublished(n == 4 ? RESOURCE_X_DELIVERY_ACQUIRE_READ_QUIET
			: RESOURCE_X_DELIVERY_ACQUIRE_UNARMED_QUIET);
	}
}
UT_TEST(test_image_episode_FATAL_after_C_hands_original_notice_to_LMS)
{
	EvidenceFixture f; UT_ASSERT(im_start(&f, 2, 0)); de_boundary = 3; de_fault = 2;
	im_exit(); im_installed(1);
}
UT_TEST(test_image_episode_replay_cannot_copy_commit_or_wake_twice)
{
	EvidenceFixture f; UT_ASSERT(im_start(&f, 3, 0)); UT_ASSERT_EQ(im_run(), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(im_run(), RESOURCE_X_APPLY_DUPLICATE); im_installed(1);
	UT_ASSERT_EQ(request_shared.rings[1].count, 1);
}
UT_TEST(test_image_episode_wrong_original_process_pin_and_handle_preserve_responsibility)
{
	for (unsigned n = 0; n < 7; n++) {
		EvidenceFixture f; ClusterPcmOwnEntry own; ClusterPcmResourceXBootstrapRound round; uint32 state;
		UT_ASSERT(im_start(&f, 2, 0));
		if (n == 0) request_proc.cluster_grd_generation++;
		if (n == 1) request_proc.pid++;
		if (n == 2) request_pins = 0;
		if (n == 3) prepared_episode.reservation_token++;
		if (n == 4) prepared_claim.executor_sequence++;
		if (n == 5) request_base.tag.blockNum++;
		if (n == 6) prepared_episode.backend_id++;
		own = request_own; round = request_entry()->resource_x_bootstrap_round;
		state = pg_atomic_read_u32(&request_buf.state);
		UT_ASSERT(im_run() != RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT(memcmp(&own, &request_own, sizeof(own)) == 0);
		UT_ASSERT(memcmp(&round, &request_entry()->resource_x_bootstrap_round, sizeof(round)) == 0);
		UT_ASSERT_EQ(pg_atomic_read_u32(&request_buf.state), state);
		UT_ASSERT_EQ(complete_commits + disk_reads, 0); dg_stack_empty();
	}
}
UT_TEST(test_image_episode_callback_failure_and_wrong_ACK_keep_original_debt)
{
	EvidenceFixture f; ResourceXIntentSlot slot; ResourceXDecodedFrame notice, wrong;
	UT_ASSERT(im_start(&f, 3, 0)); UT_ASSERT_EQ(im_run(), RESOURCE_X_APPLY_APPLIED);
	notice = im_notice(); wrong = notice; wrong.body.acquire_completion.decision.holders[2].boot_incarnation++;
	UT_ASSERT(im_ack(wrong) != RESOURCE_X_APPLY_APPLIED);
	slot = qn_stage();
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&slot, false, ++fake_pcm_clock_us));
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose, RESOURCE_X_DELIVERY_ACQUIRE_READ_NOTICE);
	slot = qn_stage(); UT_ASSERT_EQ(im_ack(notice), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&slot, true, ++fake_pcm_clock_us));
	UT_ASSERT_EQ(im_ack(notice), RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT_EQ(complete_commits, 1);
}
UT_TEST(test_image_episode_foreign_WAL_is_installed_with_memory_bytes)
{
	EvidenceFixture f; UT_ASSERT(im_start_full(&f, 3, 0, true));
	UT_ASSERT_EQ(im_run(), RESOURCE_X_APPLY_APPLIED); im_installed(1);
	LWLockAcquire(BufferDescriptorGetContentLock(&request_buf), LW_EXCLUSIVE);
	UT_ASSERT(cluster_page_wal_install_matches_v1(1, &im_wal));
	LWLockRelease(BufferDescriptorGetContentLock(&request_buf));
}
UT_TEST(test_image_episode_input_WAL_fence_does_not_reuse_resident_API)
{
	EvidenceFixture f; ClusterPcmOwnSnapshot pending; ClusterPageWalInstallV1 prep = {0};
	UT_ASSERT(im_start(&f, 2, 0));
	pending = request_base; pending.flags = PCM_OWN_FLAG_GRANT_PENDING;
	pending.reservation_token = prepared_episode.reservation_token;
	LWLockAcquire(BufferDescriptorGetContentLock(&request_buf), LW_EXCLUSIVE);
	UT_ASSERT(!cluster_page_wal_prepare_install_v1(1, &im_wal, im_expected.data, &prep));
#ifdef IMAGE_WAL_RED
	UT_ASSERT(cluster_page_wal_prepare_install_v1(1, &im_wal, im_expected.data, &prep));
#else
	UT_ASSERT(cluster_page_wal_prepare_input_install_v1(&request_buf, &pending, 1,
		&im_wal, im_expected.data, &prep));
#endif
	cluster_page_wal_release_install_v1(&prep);
	LWLockRelease(BufferDescriptorGetContentLock(&request_buf));
	UT_ASSERT_EQ(complete_commits + disk_reads, 0);
}
UT_TEST(test_image_episode_input_WAL_rejects_changed_fence_and_odd_write)
{
	for (unsigned n = 0; n < 7; n++) {
		EvidenceFixture f; ClusterPcmOwnSnapshot pending; ClusterPageWalInstallV1 prep, before;
		UT_ASSERT(im_start(&f, 2, 0)); pending = request_base;
		pending.flags = PCM_OWN_FLAG_GRANT_PENDING; pending.reservation_token = prepared_episode.reservation_token;
		if (n == 0) pending.generation++;
		if (n == 1) pending.reservation_token++;
		if (n == 2) pending.tag.blockNum++;
		if (n == 3) pg_atomic_write_u64(&request_own.write_io_generation, 1);
		if (n == 4) pg_atomic_write_u64(&request_own.delivery_attempt, 2);
		if (n == 5) pending.flags = PCM_OWN_FLAG_REVOKING;
		if (n == 6) pg_atomic_write_u32(&request_buf.state, pg_atomic_read_u32(&request_buf.state) | BM_IO_ERROR);
		memset(&prep, 0xA5, sizeof(prep)); before = prep;
		LWLockAcquire(BufferDescriptorGetContentLock(&request_buf), LW_EXCLUSIVE);
		UT_ASSERT(!cluster_page_wal_prepare_input_install_v1(&request_buf, &pending, 1,
			&im_wal, im_expected.data, &prep));
		UT_ASSERT(memcmp(&prep, &before, sizeof(prep)) == 0);
		LWLockRelease(BufferDescriptorGetContentLock(&request_buf));
	}
}

static void
im_WAL_pool_has_no_preparation_debt(void)
{
	ClusterPageWalRefV1 refs[256] = {{0}};
	/* A leaked preparation consumes one of the original 256 source slots.
	 * Exercise the real bounded pool, then return every explicit reference. */
	for (unsigned n = 0; n < lengthof(refs); n++) {
		ClusterPageWalBindingV1 binding = im_wal;
		binding.source.claim.max_config_generation += n + 1;
		UT_ASSERT(cluster_page_wal_ref_retain_v1(&binding, &refs[n]));
	}
	for (unsigned n = 0; n < lengthof(refs); n++)
		UT_ASSERT(cluster_page_wal_ref_release_v1(&refs[n]));
}

UT_TEST(test_image_episode_ERROR_after_WAL_preparation_returns_original_pool_reference)
{
	EvidenceFixture f; UT_ASSERT(im_start_full(&f, 3, 0, true));
	stage_on_content = stage_header_error;
	UT_ASSERT(im_error()); im_unpublished(RESOURCE_X_DELIVERY_ACQUIRE_UNARMED_QUIET);
	im_WAL_pool_has_no_preparation_debt();
}

static void
im_FATAL_with_original_content(void)
{
	/* Unlike the off-lock fault leaf, this cut deliberately holds content
	 * X. Actual temporary callbacks must release WAL/content before outer IO. */
	UT_ASSERT(LWLockHeldByMeInMode(BufferDescriptorGetContentLock(&request_buf), LW_EXCLUSIVE));
	while (disk_exit_count > dg_stack_base) {
		unsigned n = --disk_exit_count;
		disk_exit_callbacks[n](1, disk_exit_arguments[n]);
	}
	siglongjmp(dg_fatal_jump, 1);
}
static void
im_prepare_FATAL_cut(void)
{
	stage_header_hook = im_FATAL_with_original_content;
}
UT_TEST(test_image_episode_FATAL_after_WAL_preparation_releases_inner_scope_first)
{
	EvidenceFixture f; UT_ASSERT(im_start_full(&f, 3, 0, true));
	stage_on_content = im_prepare_FATAL_cut;
	im_exit(); im_unpublished(RESOURCE_X_DELIVERY_ACQUIRE_UNARMED_QUIET);
	im_WAL_pool_has_no_preparation_debt();
}

int
main(void)
{
	UT_PLAN(21);
	UT_RUN(test_image_episode_S_and_X_sources_copy_commit_and_handoff_once);
	UT_RUN(test_image_episode_memory_checksum_is_not_a_disk_checksum);
	UT_RUN(test_image_episode_ACK_and_real_callback_end_only_original_notice);
	UT_RUN(test_image_episode_bad_shape_cannot_publish_or_copy);
	UT_RUN(test_image_episode_ERROR_after_arming_owns_verified_quiet);
	UT_RUN(test_image_episode_ERROR_after_native_C_finishes_original_IO);
	UT_RUN(test_image_episode_all_header_refusals_precede_shared_copy);
	UT_RUN(test_image_episode_empty_current_image_is_all_zero_only);
	UT_RUN(test_image_episode_ERROR_before_and_after_execution_seals_no_image);
	UT_RUN(test_image_episode_ERROR_during_copy_releases_content_before_outer_quiet);
	UT_RUN(test_image_episode_ERROR_after_IO_and_publication_never_repeats_C);
	UT_RUN(test_image_episode_FATAL_before_C_uses_original_LIFO_quiet);
	UT_RUN(test_image_episode_FATAL_after_C_hands_original_notice_to_LMS);
	UT_RUN(test_image_episode_replay_cannot_copy_commit_or_wake_twice);
	UT_RUN(test_image_episode_wrong_original_process_pin_and_handle_preserve_responsibility);
	UT_RUN(test_image_episode_callback_failure_and_wrong_ACK_keep_original_debt);
	UT_RUN(test_image_episode_foreign_WAL_is_installed_with_memory_bytes);
	UT_RUN(test_image_episode_input_WAL_fence_does_not_reuse_resident_API);
	UT_RUN(test_image_episode_input_WAL_rejects_changed_fence_and_odd_write);
	UT_RUN(test_image_episode_ERROR_after_WAL_preparation_returns_original_pool_reference);
	UT_RUN(test_image_episode_FATAL_after_WAL_preparation_releases_inner_scope_first);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
