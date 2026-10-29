/*-------------------------------------------------------------------------
 * test_cluster_native_disk_guard.c
 *   Original pre-C producer cleanup before ResourceOwner IO/pin release.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *   src/test/cluster_unit/test_cluster_native_disk_guard.c
 * NOTES
 *   Actual execution, disk/stage, native, PCM, IO and PG cleanup macros run.
 *   Storage/process/exit scheduling and boundary faults are explicit leaves.
 *   Controlled FATAL stack execution is not a full native process-exit test.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
static void dg_native_report(int level);
#undef ereport
#define ereport(level, rest) dg_native_report(level)
#include "test_cluster_native_unarmed_quiet_fixture.inc"
#undef ereport
#include "storage/ipc.h"

static unsigned dg_boundary, dg_fault, dg_panics;
static int dg_stack_base;
static sigjmp_buf dg_fatal_jump, dg_panic_jump;
static bool dg_panic_expected;

static void
dg_fatal(void)
{
	/* Native shmem_exit decrements before calling each temporary callback. */
	UT_ASSERT_EQ(fake_lwlock_depth, 0);
	while (disk_exit_count > dg_stack_base) {
		unsigned n = --disk_exit_count;
		disk_exit_callbacks[n](1, disk_exit_arguments[n]);
	}
	siglongjmp(dg_fatal_jump, 1);
}
static void
dg_event(unsigned boundary)
{
	if (dg_boundary != boundary) return;
	if (dg_fault == 1) pg_re_throw();
	if (dg_fault == 2) dg_fatal();
}
static void
dg_native_report(int level)
{
	if (level < ERROR) return;
	UT_ASSERT_EQ(level, PANIC);
	UT_ASSERT(dg_panic_expected);
	dg_panics++;
	siglongjmp(dg_panic_jump, 1);
}
static ResourceXApplyResult
dg_execute(BufferDesc *buf, const ClusterPcmOwnSnapshot *base,
	const ResourceXDecodedAcquireIntent *episode, const ClusterSemanticAdmissionToken *admission,
	const ResourceXDeliveryClaim *original, ResourceXDecodedFrame *grant,
	ResourceXDecodedFrame *image, ResourceXRequesterJoinSnapshot *join, ResourceXDeliveryClaim *out)
{
	ResourceXApplyResult result;
	dg_event(1);
	result = cluster_bufmgr_pcm_own_acquire_execute_begin_exact(buf, base, episode, admission,
		original, grant, image, join, out);
	dg_event(2);
	return dg_boundary == 2 && dg_fault == 3 ? RESOURCE_X_APPLY_RECOVERY_BLOCKED : result;
}
static ResourceXApplyResult
dg_stage(BufferDesc *buf, const ClusterPcmOwnSnapshot *base,
	const ResourceXDecodedAcquireIntent *episode, const ClusterSemanticAdmissionToken *admission,
	const ResourceXDeliveryClaim *execution, ResourceXDiskStageObservation *out)
{
	ResourceXApplyResult result = cluster_bufmgr_pcm_own_acquire_stage_disk_exact(
		buf, base, episode, admission, execution, out);
	dg_event(3);
	return dg_boundary == 3 && dg_fault == 3 ? RESOURCE_X_APPLY_RECOVERY_BLOCKED : result;
}
static ResourceXApplyResult
dg_arm(const ResourceXDeliveryClaim *execution, const ResourceXDecodedAcquireIntent *episode,
	const ResourceXAcquireReadReceipt *receipt)
{
	ResourceXApplyResult result;
	if (dg_boundary == 4 && dg_fault == 3) return RESOURCE_X_APPLY_RECOVERY_BLOCKED;
	result = cluster_pcm_lock_resource_x_requester_acquire_read_arm_exact(execution, episode, receipt);
	dg_event(4);
	return result;
}
#ifndef DISK_GUARD_RED
#define GetPrivateRefCount(buffer) request_private_pin(buffer)
#define cluster_bufmgr_pcm_own_acquire_execute_begin_exact dg_execute
#define cluster_bufmgr_pcm_own_acquire_stage_disk_exact dg_stage
#define cluster_pcm_lock_resource_x_requester_acquire_read_arm_exact dg_arm
#define ereport(level, rest) dg_native_report(level)
#include "test_cluster_native_disk_guard.inc"
#undef ereport
#undef cluster_pcm_lock_resource_x_requester_acquire_read_arm_exact
#undef cluster_bufmgr_pcm_own_acquire_stage_disk_exact
#undef cluster_bufmgr_pcm_own_acquire_execute_begin_exact
#undef GetPrivateRefCount
#endif

static bool
dg_start(EvidenceFixture *f)
{
	bool ok;
	/* Initialize unchanged storage/IO leaves, then create a fresh PREPARED E
	 * with the real begin functions. Never relabel an executing round. */
	if (!ua_start(f, 0)) return false;
	evidence_disk_watermark_scn = 90;
	ok = execution_start(f, 0, -1);
	evidence_disk_watermark_scn = 0;
	if (!ok) return false;
	input_grant = f->decision;
	/* Match the actual retained owner representation, not ingress transport. */
	input_grant.common.sender_connection_generation = PGRAC_RESOURCE_X_RETAINED_SENDER_GENERATION;
	input_grant.common.semantic_crc32c = 0;
	if (!evidence_wire(&input_grant)) return false;
	armed_capability = prepared_claim;
	quiet_aborts = quiet_abort_calls = quiet_broadcasts = 0;
	quiet_before_abort_error = quiet_after_abort_error = quiet_publish_error = false;
	dg_boundary = dg_fault = dg_panics = 0;
	dg_panic_expected = false;
	dg_stack_base = disk_exit_count;
	fake_error_use_pg_exception_stack = true;
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose, 3);
	return true;
}
static ResourceXApplyResult
dg_run(void)
{
#ifdef DISK_GUARD_RED
	/* Exact parent sequence, with no missing API or fabricated handler. */
	ResourceXApplyResult result = dg_execute(&request_buf, &request_base, &prepared_episode,
		&request_admission, &prepared_claim, &input_grant, &input_image, &input_join, &execution_claim);
	if (result == RESOURCE_X_APPLY_APPLIED) {
		result = dg_stage(&request_buf, &request_base, &prepared_episode, &request_admission,
			&execution_claim, &staged);
		if (result == RESOURCE_X_APPLY_APPLIED) {
			ResourceXAcquireReadReceipt receipt;
			cluster_bufmgr_acquire_read_receipt(&staged, &receipt);
			result = dg_arm(&execution_claim, &prepared_episode, &receipt);
		}
	}
	return result;
#else
	return cluster_bufmgr_pcm_own_acquire_prepare_disk_exact(&request_buf, &request_base,
		&prepared_episode, &request_admission, &prepared_claim);
#endif
}
static bool
dg_error(void)
{
	volatile bool caught = false;
	PG_TRY(); { (void)dg_run(); } PG_CATCH(); { caught = true; } PG_END_TRY();
	return caught;
}
static void
dg_fatal_run(void)
{
	sigjmp_buf *saved = PG_exception_stack;
	ErrorContextCallback *context = error_context_stack;
	if (sigsetjmp(dg_fatal_jump, 0) == 0) {
		(void)dg_run();
		UT_ASSERT(false);
	}
	PG_exception_stack = saved;
	error_context_stack = context;
}
static bool
dg_panic_run(void)
{
	sigjmp_buf *saved = PG_exception_stack;
	ErrorContextCallback *context = error_context_stack;
	bool caught = false;
	if (sigsetjmp(dg_panic_jump, 0) == 0) {
		PG_TRY(); { (void)dg_run(); } PG_CATCH(); { /* Not a PANIC cut. */ } PG_END_TRY();
	}
	else caught = true;
	PG_exception_stack = saved;
	error_context_stack = context;
	return caught;
}
static void
dg_stack_empty(void)
{
	UT_ASSERT_EQ(disk_exit_count, dg_stack_base);
	UT_ASSERT_EQ(disk_exit_lifo_errors, 0);
	complete_idle();
}
static void
dg_quiet(unsigned reads, bool verified)
{
	const ResourceXAcquireReadReceipt *r = &request_entry()->resource_x_bootstrap_round.acquire_read_receipt;
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose, verified ? 11 : 14);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), request_base.generation);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&request_buf.state) & (BM_VALID | BM_IO_IN_PROGRESS | BM_IO_ERROR), BM_IO_ERROR);
	UT_ASSERT(!ResourceOwnerHasBufferIO(original_io_owner, 1));
	UT_ASSERT_EQ(disk_reads, reads);
	UT_ASSERT_EQ(complete_commits, 0);
	UT_ASSERT_EQ(complete_io_calls, 0);
	UT_ASSERT_EQ(verified ? quiet_aborts : ua_aborts, 1);
	UT_ASSERT_EQ(r->authority_generation, input_grant.common.authority_generation);
	if (!verified) { UT_ASSERT_EQ(r->page_lsn, 0); UT_ASSERT_EQ(r->page_scn, 0); UT_ASSERT_EQ(r->page_crc32c, 0); }
	dg_stack_empty();
}
static void
dg_prepared_refused(void)
{
	ClusterPcmOwnEntry own = request_own;
	ClusterPcmResourceXBootstrapRound round = request_entry()->resource_x_bootstrap_round;
	uint32 state = pg_atomic_read_u32(&request_buf.state);
	UT_ASSERT(ua_run() != RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(memcmp(&own, &request_own, sizeof(own)) == 0);
	UT_ASSERT(memcmp(&round, &request_entry()->resource_x_bootstrap_round, sizeof(round)) == 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&request_buf.state), state);
	UT_ASSERT_EQ(ua_aborts, 0);
}
static void dg_open_fatal(void) { dg_fatal(); }
static void dg_open_error(void) { pg_re_throw(); }
static void dg_seal_refusal(void) { ua_seal_refused = true; pg_re_throw(); }
static void dg_end_refusal(void) { disk_end_ok = false; }
static void dg_outer(int code, Datum arg) { (void)code; (void)arg; UT_ASSERT(false); }

static bool
dg_locate_settlement(void)
{
	uint8 bytes[RESOURCE_X_DECISION_V4_BYTES];
	uint16 len;
	ResourceXWireReject reject;
	unsigned char *memory = (unsigned char *)fake_pcm_header.data;
	Size offset = offsetof(ClusterPcmResourceXMasterState, requester_join.grant_payload), found = 0;

	if (!cluster_resource_x_wire_encode(RESOURCE_X_MSG_IMAGE_OR_GRANT, &input_grant,
		bytes, sizeof(bytes), &len, &reject)) return false;
	for (Size n = offset % sizeof(uint64); n + len <= fake_pcm_header_requested_size; n += sizeof(uint64))
		if (memcmp(memory + n, bytes, len) == 0) {
			qn_state = (void *)(memory + n - offset);
			found++;
		}
	if (found != 1) return false;
	qn_intent = &qn_state->requester_settlement_intent;
	return true;
}

UT_TEST(test_prepared_cleanup_seals_without_claiming_execution)
{
	EvidenceFixture f; UT_ASSERT(dg_start(&f));
	UT_ASSERT_EQ(ua_run(), RESOURCE_X_APPLY_APPLIED); dg_quiet(0, false);
}
UT_TEST(test_prepared_cleanup_replay_never_repeats_abort)
{
	EvidenceFixture f; UT_ASSERT(dg_start(&f)); UT_ASSERT_EQ(ua_run(), RESOURCE_X_APPLY_APPLIED);
	for (int n = 0; n < 4; n++) UT_ASSERT_EQ(ua_run(), RESOURCE_X_APPLY_DUPLICATE);
	dg_quiet(0, false);
}
UT_TEST(test_prepared_full_episode_and_sequence_mismatch_preserve_owner)
{
	for (int n = 0; n < 6; n++) {
		EvidenceFixture f; UT_ASSERT(dg_start(&f));
		if (n == 0) armed_capability.executor_sequence++;
		if (n == 1) prepared_episode.reservation_token++;
		if (n == 2) prepared_episode.backend_generation++;
		if (n == 3) armed_capability.observation.entry_binding_generation++;
		if (n == 4) armed_capability.observation.request.assertion_sequence++;
		if (n == 5) armed_capability.observation.request.master_session_incarnation++;
		dg_prepared_refused();
	}
}
UT_TEST(test_prepared_native_generation_token_hold_or_write_mismatch_refuses)
{
	for (int n = 0; n < 4; n++) {
		EvidenceFixture f; UT_ASSERT(dg_start(&f));
		if (n == 0) pg_atomic_write_u64(&request_own.generation, request_base.generation + 1);
		if (n == 1) pg_atomic_write_u64(&request_own.reservation_token, prepared_episode.reservation_token + 1);
		if (n == 2) pg_atomic_write_u64(&request_own.delivery_attempt, prepared_claim.executor_sequence + 1);
		if (n == 3) pg_atomic_write_u64(&request_own.write_io_generation, 1);
		dg_prepared_refused();
	}
}
UT_TEST(test_prepared_process_pin_and_IO_owner_are_not_replaceable)
{
	for (int n = 0; n < 4; n++) {
		EvidenceFixture f; UT_ASSERT(dg_start(&f));
		if (n == 0) request_proc.cluster_grd_generation++;
		if (n == 1) request_pins = 0;
		if (n == 2) CurrentResourceOwner = ResourceOwnerCreate(NULL, "other");
		if (n == 3) ResourceOwnerForgetBufferIO(original_io_owner, 1);
		dg_prepared_refused();
	}
}
UT_TEST(test_prepared_cleanup_keeps_same_cancel_floor_when_gate_closes)
{
	EvidenceFixture f; ResourceXGateSnapshot gate; UT_ASSERT(dg_start(&f));
	request_entry()->resource_x_bootstrap_round.cancelled_attempt_floor = prepared_claim.observation.request.assertion_sequence;
	UT_ASSERT(cluster_pcm_lock_resource_x_gate_snapshot(&gate));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_fail_closed_exact(&gate), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(ua_run(), RESOURCE_X_APPLY_APPLIED); dg_quiet(0, false);
}
UT_TEST(test_prepared_future_cancel_floor_does_not_retire_original)
{
	EvidenceFixture f; UT_ASSERT(dg_start(&f));
	request_entry()->resource_x_bootstrap_round.cancelled_attempt_floor = prepared_claim.observation.request.assertion_sequence + 1;
	dg_prepared_refused();
}
UT_TEST(test_guard_success_returns_one_original_ARMED_without_C)
{
	EvidenceFixture f; UT_ASSERT(dg_start(&f)); UT_ASSERT_EQ(dg_run(), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose, 5);
	UT_ASSERT_EQ(disk_reads, 1); UT_ASSERT_EQ(disk_enters, 1); UT_ASSERT_EQ(disk_ends, 1);
	UT_ASSERT_EQ(complete_commits, 0); UT_ASSERT_EQ(complete_io_calls, 0); UT_ASSERT_EQ(ua_aborts, 0);
	UT_ASSERT(ResourceOwnerHasBufferIO(original_io_owner, 1)); dg_stack_empty();
}
UT_TEST(test_guard_success_continues_original_C_once_without_reread)
{
	EvidenceFixture f; UT_ASSERT(dg_start(&f)); UT_ASSERT_EQ(dg_run(), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(armed_advance(), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(armed_advance(), RESOURCE_X_APPLY_DUPLICATE);
	UT_ASSERT_EQ(disk_reads, 1); UT_ASSERT_EQ(complete_commits, 1); UT_ASSERT_EQ(complete_io_calls, 1); dg_stack_empty();
}
UT_TEST(test_guard_never_reexecutes_or_reloads_an_ARMED_episode)
{
	EvidenceFixture f; UT_ASSERT(dg_start(&f)); UT_ASSERT_EQ(dg_run(), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(dg_run() != RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(disk_reads, 1); UT_ASSERT_EQ(ua_aborts, 0); UT_ASSERT_EQ(quiet_aborts, 0); dg_stack_empty();
}
UT_TEST(test_guard_ERROR_before_execute_cleans_PREPARED_before_RO)
{
	EvidenceFixture f; UT_ASSERT(dg_start(&f)); dg_boundary = 1; dg_fault = 1;
	UT_ASSERT(dg_error()); dg_quiet(0, false);
}
UT_TEST(test_guard_ERROR_after_execute_cleans_original_shared_owner)
{
	EvidenceFixture f; UT_ASSERT(dg_start(&f)); dg_boundary = 2; dg_fault = 1;
	UT_ASSERT(dg_error()); dg_quiet(0, false);
}
UT_TEST(test_guard_storage_open_ERROR_ends_KO_before_native_quiet)
{
	EvidenceFixture f; UT_ASSERT(dg_start(&f)); disk_open_error = true;
	UT_ASSERT(dg_error()); UT_ASSERT_EQ(disk_ends, 1); dg_quiet(0, false);
}
UT_TEST(test_guard_storage_read_ERROR_ends_KO_before_native_quiet)
{
	EvidenceFixture f; UT_ASSERT(dg_start(&f)); disk_read_error = true;
	UT_ASSERT(dg_error()); UT_ASSERT_EQ(disk_ends, 1); dg_quiet(1, false);
}
UT_TEST(test_guard_ERROR_after_shared_staging_never_publishes_image)
{
	EvidenceFixture f; UT_ASSERT(dg_start(&f)); dg_boundary = 3; dg_fault = 1;
	UT_ASSERT(dg_error()); dg_quiet(1, false);
}
UT_TEST(test_guard_content_header_ERROR_releases_content_before_cleanup)
{
	EvidenceFixture f; UT_ASSERT(dg_start(&f)); stage_on_content = stage_header_error;
	UT_ASSERT(dg_error()); dg_quiet(1, false);
}
UT_TEST(test_guard_ERROR_after_arm_uses_verified_quiet_receipt)
{
	EvidenceFixture f; UT_ASSERT(dg_start(&f)); dg_boundary = 4; dg_fault = 1;
	UT_ASSERT(dg_error()); dg_quiet(1, true);
}
UT_TEST(test_guard_execute_refusal_cannot_drop_original_responsibility)
{
	EvidenceFixture f; UT_ASSERT(dg_start(&f)); dg_boundary = 2; dg_fault = 3;
	UT_ASSERT_EQ(dg_run(), RESOURCE_X_APPLY_RECOVERY_BLOCKED); dg_quiet(0, false);
}
UT_TEST(test_guard_staging_refusal_cleans_before_returning_original_status)
{
	EvidenceFixture f; UT_ASSERT(dg_start(&f)); dg_boundary = 3; dg_fault = 3;
	UT_ASSERT_EQ(dg_run(), RESOURCE_X_APPLY_RECOVERY_BLOCKED); dg_quiet(1, false);
}
UT_TEST(test_guard_arm_refusal_cannot_upgrade_unpublished_bytes_to_I)
{
	EvidenceFixture f; UT_ASSERT(dg_start(&f)); dg_boundary = 4; dg_fault = 3;
	UT_ASSERT_EQ(dg_run(), RESOURCE_X_APPLY_RECOVERY_BLOCKED); dg_quiet(1, false);
}
UT_TEST(test_guard_FATAL_before_execute_runs_original_stack_cleanup)
{
	EvidenceFixture f; UT_ASSERT(dg_start(&f)); dg_boundary = 1; dg_fault = 2;
	dg_fatal_run(); dg_quiet(0, false);
}
UT_TEST(test_guard_FATAL_after_execute_keeps_original_identity)
{
	EvidenceFixture f; UT_ASSERT(dg_start(&f)); dg_boundary = 2; dg_fault = 2;
	dg_fatal_run(); dg_quiet(0, false);
}
UT_TEST(test_guard_FATAL_open_pops_exact_KO_scope_first)
{
	EvidenceFixture f; UT_ASSERT(dg_start(&f)); disk_on_open = dg_open_fatal;
	dg_fatal_run(); UT_ASSERT_EQ(disk_ends, 1); dg_quiet(0, false);
}
UT_TEST(test_guard_FATAL_read_pops_exact_KO_scope_first)
{
	EvidenceFixture f; UT_ASSERT(dg_start(&f)); disk_on_read = dg_open_fatal;
	dg_fatal_run(); UT_ASSERT_EQ(disk_ends, 1); dg_quiet(1, false);
}
UT_TEST(test_guard_FATAL_after_staging_retains_absent_image_proof)
{
	EvidenceFixture f; UT_ASSERT(dg_start(&f)); dg_boundary = 3; dg_fault = 2;
	dg_fatal_run(); dg_quiet(1, false);
}
UT_TEST(test_guard_FATAL_after_arm_uses_original_verified_receipt)
{
	EvidenceFixture f; UT_ASSERT(dg_start(&f)); dg_boundary = 4; dg_fault = 2;
	dg_fatal_run(); dg_quiet(1, true);
}
UT_TEST(test_guard_KO_end_refusal_preserves_debt_and_fails_closed)
{
	EvidenceFixture f; UT_ASSERT(dg_start(&f)); disk_on_read = dg_end_refusal; dg_panic_expected = true;
	UT_ASSERT(dg_panic_run()); UT_ASSERT_EQ(dg_panics, 1);
	UT_ASSERT_EQ(ua_aborts, 0); UT_ASSERT_EQ(quiet_aborts, 0);
	UT_ASSERT(ResourceOwnerHasBufferIO(original_io_owner, 1));
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), PCM_OWN_FLAG_GRANT_PENDING);
	/* PANIC does not unwind into generic cleanup. Discard only the expired
	 * controlled test stack after recording its outstanding outer guard. */
	UT_ASSERT_EQ(disk_exit_count, dg_stack_base + 1);
	disk_exit_count = dg_stack_base; complete_idle();
}
UT_TEST(test_guard_unprovable_quiet_fails_closed_without_generic_abort)
{
	EvidenceFixture f; UT_ASSERT(dg_start(&f)); disk_on_open = dg_seal_refusal; dg_panic_expected = true;
	UT_ASSERT(dg_panic_run()); UT_ASSERT_EQ(dg_panics, 1); UT_ASSERT_EQ(ua_aborts, 0);
	UT_ASSERT(ResourceOwnerHasBufferIO(original_io_owner, 1)); dg_stack_empty();
}
UT_TEST(test_guard_nested_cleanup_preserves_original_outer_LIFO)
{
	for (int n = 0; n < 2; n++) {
		EvidenceFixture f; UT_ASSERT(dg_start(&f)); before_shmem_exit(dg_outer, (Datum)47); dg_stack_base++;
		if (n == 0) UT_ASSERT_EQ(dg_run(), RESOURCE_X_APPLY_APPLIED);
		else { disk_on_open = dg_open_error; UT_ASSERT(dg_error()); }
		dg_stack_empty(); cancel_before_shmem_exit(dg_outer, (Datum)47); dg_stack_base--;
		UT_ASSERT_EQ(disk_exit_lifo_errors, 0);
	}
}
UT_TEST(test_prepared_cleanup_never_retires_installed_C_as_unarmed)
{
	EvidenceFixture f; ClusterPcmOwnEntry own; UT_ASSERT(ua_start(&f, 0));
	UT_ASSERT_EQ(complete_run(), RESOURCE_X_APPLY_APPLIED);
	own = request_own;
	UT_ASSERT(ua_run() != RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(memcmp(&own, &request_own, sizeof(own)) == 0); UT_ASSERT_EQ(ua_aborts, 0);
}
UT_TEST(test_guard_wrong_episode_or_mode_has_no_callback_or_physical_effect)
{
	for (int n = 0; n < 3; n++) {
		EvidenceFixture f; ClusterPcmOwnEntry own; UT_ASSERT(dg_start(&f));
		if (n == 0) prepared_episode.backend_generation++;
		if (n == 1) prepared_claim.executor_sequence++;
		if (n == 2) prepared_claim.observation.request.target_mode = PCM_STATE_X;
		own = request_own;
		UT_ASSERT(dg_run() != RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT(memcmp(&own, &request_own, sizeof(own)) == 0);
		UT_ASSERT_EQ(ua_aborts, 0); UT_ASSERT_EQ(disk_reads, 0); dg_stack_empty();
	}
}
UT_TEST(test_guard_checksum_refusal_keeps_original_fail_closed_verdict)
{
	EvidenceFixture f; UT_ASSERT(dg_start(&f)); disk_bytes.data[512] ^= 1; disk_store();
	UT_ASSERT_EQ(dg_run(), RESOURCE_X_APPLY_RECOVERY_BLOCKED);
	UT_ASSERT_EQ(disk_traces, 1); dg_quiet(1, false);
}

UT_TEST(test_prepared_quiet_LMS_ACK_waits_for_exact_transport_then_retires)
{
	EvidenceFixture f; ResourceXIntentSlot slot;
	UT_ASSERT(dg_start(&f)); UT_ASSERT_EQ(ua_run(), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(dg_locate_settlement()); slot = qn_stage();
	UT_ASSERT_EQ(ua_ack(), RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose, UA_CONSUMED);
	UT_ASSERT_EQ(qn_intent->slot.state, RESOURCE_X_INTENT_SLOT_STAGED);
	UT_ASSERT(cluster_pcm_lock_resource_x_outbound_transport_complete_exact(&slot, true, ++fake_pcm_clock_us));
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.has_acquire_intent, 0);
	UT_ASSERT_EQ(ua_aborts, 1); UT_ASSERT_EQ(disk_reads, 0); dg_stack_empty();
}
UT_TEST(test_guard_ERROR_quiet_survives_original_backend_loss_for_LMS)
{
	EvidenceFixture f; ResourceXIntentSlot slot; PGPROC *proc; ResourceOwner owner; int pid;
	UT_ASSERT(dg_start(&f)); disk_open_error = true; UT_ASSERT(dg_error());
	UT_ASSERT(dg_locate_settlement()); proc = MyProc; pid = MyProcPid; owner = CurrentResourceOwner;
	MyProc = NULL; MyProcPid = 0; CurrentResourceOwner = NULL;
	UT_ASSERT(qn_global_probe(&slot));
	MyProc = proc; MyProcPid = pid; CurrentResourceOwner = owner;
	UT_ASSERT_EQ(slot.body.owner_kind, RESOURCE_X_INTENT_OWNER_REQUESTER_SETTLEMENT);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose, UA_QUIET);
	UT_ASSERT_EQ(ua_aborts, 1); UT_ASSERT_EQ(disk_ends, 1); dg_stack_empty();
}

UT_TEST(test_quiet_publication_requires_prior_sealing_not_PREPARED_execution_or_ARMED)
{
	for (int n = 0; n < 3; n++) {
		EvidenceFixture f;
		ResourceXDeliveryClaim current;
		ResourceXAcquireReadReceipt receipt;
		ClusterPcmOwnSnapshot terminal;
		ClusterPcmResourceXBootstrapRound round;
		ClusterPcmOwnEntry own;

		UT_ASSERT(dg_start(&f));
		if (n == 1) UT_ASSERT_EQ(execution_enter(), RESOURCE_X_APPLY_APPLIED);
		if (n == 2) UT_ASSERT_EQ(dg_run(), RESOURCE_X_APPLY_APPLIED);
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_acquire_read_close_observe_exact(
			&prepared_claim, &prepared_episode, &current, &receipt), RESOURCE_X_APPLY_APPLIED);
		/* Deliberately shape-valid caller input is not a prior shared seal.
		 * It must not manufacture a native-quiet or installed-byte proof. */
		terminal = request_base;
		terminal.reservation_token = prepared_episode.reservation_token;
		terminal.flags = 0;
		terminal.pcm_state = PCM_STATE_N;
		terminal.buffer_type = BUF_TYPE_CURRENT;
		terminal.semantic_buf_state = BM_TAG_VALID | BM_IO_ERROR;
		round = request_entry()->resource_x_bootstrap_round;
		own = request_own;
		UT_ASSERT_EQ(cluster_pcm_lock_resource_x_requester_acquire_read_quiet_exact(
			&prepared_claim, &prepared_episode, &receipt, &terminal), RESOURCE_X_APPLY_STALE);
		UT_ASSERT(memcmp(&round, &request_entry()->resource_x_bootstrap_round, sizeof(round)) == 0);
		UT_ASSERT(memcmp(&own, &request_own, sizeof(own)) == 0);
		UT_ASSERT(ResourceOwnerHasBufferIO(original_io_owner, 1));
		UT_ASSERT_EQ(ua_aborts, 0); UT_ASSERT_EQ(quiet_aborts, 0); dg_stack_empty();
	}
}

int
main(void)
{
	UT_PLAN(35);
	UT_RUN(test_prepared_cleanup_seals_without_claiming_execution);
	UT_RUN(test_prepared_cleanup_replay_never_repeats_abort);
	UT_RUN(test_prepared_full_episode_and_sequence_mismatch_preserve_owner);
	UT_RUN(test_prepared_native_generation_token_hold_or_write_mismatch_refuses);
	UT_RUN(test_prepared_process_pin_and_IO_owner_are_not_replaceable);
	UT_RUN(test_prepared_cleanup_keeps_same_cancel_floor_when_gate_closes);
	UT_RUN(test_prepared_future_cancel_floor_does_not_retire_original);
	UT_RUN(test_guard_success_returns_one_original_ARMED_without_C);
	UT_RUN(test_guard_success_continues_original_C_once_without_reread);
	UT_RUN(test_guard_never_reexecutes_or_reloads_an_ARMED_episode);
	UT_RUN(test_guard_ERROR_before_execute_cleans_PREPARED_before_RO);
	UT_RUN(test_guard_ERROR_after_execute_cleans_original_shared_owner);
	UT_RUN(test_guard_storage_open_ERROR_ends_KO_before_native_quiet);
	UT_RUN(test_guard_storage_read_ERROR_ends_KO_before_native_quiet);
	UT_RUN(test_guard_ERROR_after_shared_staging_never_publishes_image);
	UT_RUN(test_guard_content_header_ERROR_releases_content_before_cleanup);
	UT_RUN(test_guard_ERROR_after_arm_uses_verified_quiet_receipt);
	UT_RUN(test_guard_execute_refusal_cannot_drop_original_responsibility);
	UT_RUN(test_guard_staging_refusal_cleans_before_returning_original_status);
	UT_RUN(test_guard_arm_refusal_cannot_upgrade_unpublished_bytes_to_I);
	UT_RUN(test_guard_FATAL_before_execute_runs_original_stack_cleanup);
	UT_RUN(test_guard_FATAL_after_execute_keeps_original_identity);
	UT_RUN(test_guard_FATAL_open_pops_exact_KO_scope_first);
	UT_RUN(test_guard_FATAL_read_pops_exact_KO_scope_first);
	UT_RUN(test_guard_FATAL_after_staging_retains_absent_image_proof);
	UT_RUN(test_guard_FATAL_after_arm_uses_original_verified_receipt);
	UT_RUN(test_guard_KO_end_refusal_preserves_debt_and_fails_closed);
	UT_RUN(test_guard_unprovable_quiet_fails_closed_without_generic_abort);
	UT_RUN(test_guard_nested_cleanup_preserves_original_outer_LIFO);
	UT_RUN(test_prepared_cleanup_never_retires_installed_C_as_unarmed);
	UT_RUN(test_guard_wrong_episode_or_mode_has_no_callback_or_physical_effect);
	UT_RUN(test_guard_checksum_refusal_keeps_original_fail_closed_verdict);
	UT_RUN(test_prepared_quiet_LMS_ACK_waits_for_exact_transport_then_retires);
	UT_RUN(test_guard_ERROR_quiet_survives_original_backend_loss_for_LMS);
	UT_RUN(test_quiet_publication_requires_prior_sealing_not_PREPARED_execution_or_ARMED);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
