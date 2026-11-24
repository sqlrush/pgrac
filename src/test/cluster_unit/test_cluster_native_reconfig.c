/* Independent ordinary requester/master/holder preserve their old acquisition
 * across the real Resource-X freeze and sweep. Authentication, sockets and
 * OS/storage scheduling remain explicit leaves; no recovery proof is forged.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "test_cluster_native_remote_x_fixture.inc"
#ifndef NATIVE_RECONFIG_WRITE
#define NATIVE_RESIDENT_LOCK_REUSE_FIXTURE
#include "test_cluster_native_resident_lock_fixture.inc"
#undef NATIVE_RESIDENT_LOCK_REUSE_FIXTURE
#endif

static unsigned nr_cut, nr_native;
static bool nr_late, nr_reached;
enum { NR_FREEZE, NR_SESSION, NR_ADMISSION, NR_CONNECTION, NR_UNAVAILABLE };
static unsigned nr_domain;
static bool nr_cancel_wait;
static int nr_control[2][2];
static ResourceXReconfigToken nr_token;
static ResourceXIntentSlot nr_unsent_callback;
#ifdef NATIVE_RECONFIG_WRITE
static sigjmp_buf nr_panic_jump;
static bool nr_panic_caught;
static void nr_panic(void)
{
	/* Inspect the live arguments before abandoning their stack frames. The
	 * two enclosing native guards must still belong to this exact caller. */
	UT_ASSERT_EQ(disk_exit_count, 2);
	if (disk_exit_count == 2) {
		ClusterBufmgrFirstFill *read = (void *)DatumGetPointer(disk_exit_arguments[0]);
		ClusterAcquireResidentGuard *guard = (void *)DatumGetPointer(disk_exit_arguments[1]);
		UT_ASSERT(disk_exit_callbacks[0] == cluster_bufmgr_first_fill_error);
		UT_ASSERT(disk_exit_callbacks[1] == cluster_bufmgr_acquire_resident_cleanup);
		UT_ASSERT(read != NULL && guard != NULL);
		if (read != NULL && guard != NULL) {
			UT_ASSERT(read->buf == &request_buf && guard->original.buf == &request_buf);
			UT_ASSERT(read->owner == CurrentResourceOwner && guard->original.owner == read->owner);
			UT_ASSERT(read->pending && read->delegated);
			UT_ASSERT(memcmp(&read->base, &guard->original.base, sizeof(read->base)) == 0);
			UT_ASSERT(memcmp(&read->request.episode, &guard->original.episode,
				sizeof(read->request.episode)) == 0);
			UT_ASSERT(memcmp(&read->request.episode, &prepared_episode, sizeof(prepared_episode)) == 0);
			UT_ASSERT(memcmp(&read->claim, &guard->original.original, sizeof(read->claim)) == 0);
		}
	}
	nr_panic_caught = true;
	siglongjmp(nr_panic_jump, 1);
}
#endif

/* These separate descriptors carry test scheduling only, never wire frames. */
static bool nr_signal(int fd, bool send)
{
	uint8 marker = 0x6d;
	NM_REQUIRE(nm_transfer(fd, &marker, sizeof(marker), send));
	NM_REQUIRE(marker == 0x6d);
	return true;
}
static void nr_freeze(void)
{
	UT_ASSERT(!nr_reached);
	UT_ASSERT(cluster_resource_x_reconfig_freeze(17, 18, &nr_token));
	UT_ASSERT_EQ(nr_token.old_formation, 17);
	UT_ASSERT_EQ(nr_token.new_formation, 18);
	UT_ASSERT_EQ(nr_token.dead_requester_bitmap, 0);
	nr_reached = true;
}
static void nr_change(void)
{
	/* Model another actor's publication, including while the caller holds
	 * content-X at IO end. Actual GCS sampling still asserts lock freedom. */
	UT_ASSERT(!nr_reached);
	if (nr_domain == NR_FREEZE) { nr_freeze(); return; }
	if (nr_domain == NR_SESSION) {
		ordinary_master_session++; wc_session_id++;
	} else if (nr_domain == NR_ADMISSION) {
		master_durable_admission = false; wc_r4++;
	} else if (nr_domain == NR_CONNECTION) ordinary_connection++;
	else {
		UT_ASSERT_EQ(nr_domain, NR_UNAVAILABLE);
		ordinary_unavailable = nr_cancel_wait ? 64 : 3;
	}
	nr_reached = true;
}
static void nr_retained(const ResourceXReconfigToken *token, bool native_namespace)
{
	ResourceXReconfigBatch batch;
	ResourceXZeroResidualProof zero;
	ResourceXGateSnapshot gate;
	PcmRxStats before, after;
	ResourceXReconfigResult result = RESOURCE_X_RECONFIG_MORE;
	UT_ASSERT(cluster_pcm_rx_stats_snapshot(&before));
	for (unsigned n = 0; n < 8 && (result == RESOURCE_X_RECONFIG_MORE
		|| result == RESOURCE_X_RECONFIG_RETRY); n++) {
		result = cluster_resource_x_reconfig_sweep(token, 4, &batch);
		printf("# Freeze role=%d sweep=%u result=%d examined=%u residual=%u orphan=%u\n",
			cluster_node_id, n, result, batch.examined_count, batch.residual_count, batch.orphan_count);
	}
	/* R8 cannot certify delivery abandonment from a namespace change. The
	 * native holder/requester therefore blocks recovery; the master keeps
	 * its decided canonical request as an orphan. Neither is completion. */
	UT_ASSERT_EQ(result, native_namespace ? RESOURCE_X_RECONFIG_CORRUPT : RESOURCE_X_RECONFIG_ORPHAN);
	UT_ASSERT_EQ(batch.zero_residual, 0);
	UT_ASSERT(cluster_pcm_rx_stats_snapshot(&after));
	UT_ASSERT_EQ(after.count[PCM_RX_DELIVERY_NAMESPACE_BLOCKED],
		before.count[PCM_RX_DELIVERY_NAMESPACE_BLOCKED] + native_namespace);
	UT_ASSERT(!cluster_resource_x_reconfig_zero_proof_exact(token, &zero));
	UT_ASSERT(!cluster_resource_x_reconfig_thaw_exact(token));
	UT_ASSERT(cluster_pcm_lock_resource_x_gate_snapshot(&gate));
	UT_ASSERT_EQ(gate.phase, native_namespace ? RESOURCE_X_GATE_RECOVERY_BLOCKED : RESOURCE_X_GATE_FROZEN);
	UT_ASSERT_EQ(gate.formation, 17);
}
static bool nr_master(int requester, int holder, int mode)
{
	PublishFixture f;
	ResourceXDecodedFrame request, block, ack;
	ResourceXIntentSlot forwarded;
	ResourceXMasterSnapshot snapshot;
	ResourceXReconfigToken token;
	ClusterPcmResourceXMasterRequest retained;
	uint8 decision[sizeof(f.state->block_intents[2].payload)];
	uint32 pending;
	(void)mode;
	close(nr_control[0][0]); close(nr_control[1][0]); close(nr_control[1][1]);
	NM_REQUIRE(nt_seed_master(holder, 0));
	NM_REQUIRE(nm_receive(requester, RESOURCE_X_MSG_ASSERT_X, &request));
	if (nr_cut == 4) {
		ClusterPcmResourceXMasterRequest empty = {0};
		NM_REQUIRE(nm_master_enqueue(&f, &request));
		retained = f.state->requests[1];
		UT_ASSERT_EQ(retained.phase, RESOURCE_X_MASTER_QUEUED);
		NM_REQUIRE(nr_signal(nr_control[0][1], true));
		NM_REQUIRE(nr_signal(nr_control[0][1], false));
		if (nr_domain == NR_FREEZE) {
			ResourceXReconfigBatch batch;
			NM_REQUIRE(cluster_resource_x_reconfig_freeze(17, 18, &token));
			UT_ASSERT_EQ(cluster_resource_x_reconfig_sweep(&token, 4, &batch), RESOURCE_X_RECONFIG_MORE);
			UT_ASSERT(memcmp(&empty, &f.state->requests[1], sizeof(empty)) == 0);
		} else UT_ASSERT(memcmp(&retained, &f.state->requests[1], sizeof(retained)) == 0);
		UT_ASSERT_EQ(f.state->bootstrap_receipts[1].highest_attempt_floor, request.common.assertion_sequence);
		close(nr_control[0][1]); complete_idle(); return true;
	}
	NM_REQUIRE(nm_master_admit(&f, &request));
	block = publish_decode(f.state->block_intents[2].payload, true);
	UT_ASSERT_EQ(block.body.acquire_decision.source_node, 2);
	UT_ASSERT_EQ(block.body.acquire_decision.directory_mode, nt_source_mode);
	NM_REQUIRE(nc_stage(&f.state->block_intents[2].slot, &forwarded));
	NM_REQUIRE(nm_send(holder, RESOURCE_X_MSG_BLOCK_TO_N, &block));
	NM_REQUIRE(nc_callback(&forwarded));
	NM_REQUIRE(nm_receive(holder, RESOURCE_X_MSG_BLOCKED_TO_N, &ack));
	auth_peer_boot = 101;
	NM_REQUIRE(cluster_pcm_lock_resource_x_blocked_to_n_exact(&ack, 2, &snapshot)
		== RESOURCE_X_APPLY_APPLIED);
	NM_REQUIRE(nr_signal(nr_control[0][1], false));
	retained = f.state->requests[1]; pending = f.state->pending_s_bitmap;
	memcpy(decision, f.state->block_intents[2].payload, sizeof(decision));
	UT_ASSERT(retained.phase != RESOURCE_X_MASTER_NONE);
	if (nr_domain == NR_FREEZE) {
		NM_REQUIRE(cluster_resource_x_reconfig_freeze(17, 18, &token));
		nr_retained(&token, false);
	}
	UT_ASSERT(memcmp(&retained, &f.state->requests[1], sizeof(retained)) == 0);
	UT_ASSERT_EQ(f.state->pending_s_bitmap, pending);
	UT_ASSERT(memcmp(decision, f.state->block_intents[2].payload, sizeof(decision)) == 0);
	/* Neither repeated old proof nor a frozen scan may complete the request. */
	UT_ASSERT(cluster_pcm_lock_resource_x_blocked_to_n_exact(&ack, 2, &snapshot)
		!= RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(memcmp(&retained, &f.state->requests[1], sizeof(retained)) == 0);
	close(nr_control[0][1]); complete_idle(); return true;
}
static bool nr_holder(int master, int requester, int mode)
{
	HolderFixture f = {0};
	ResourceXDecodedFrame block, image, ack;
	ResourceXIntentSlot image_send, proof_send;
	ClusterPcmResourceXMasterState *state = NULL;
	ClusterPcmResourceXMasterState *retained;
	ClusterPcmOwnEntry native;
	ResourceXReconfigToken token;
	ResourceXSourceDrainWork stale_work;
	PGAlignedBlock page;
	uint32 header, bufstate;
	(void)mode;
	close(nr_control[1][0]); close(nr_control[0][0]); close(nr_control[0][1]);
	NM_REQUIRE(nt_seed_holder(master, 0, 2));
	if (nr_cut == 4) {
		NM_REQUIRE(nm_requester_notice(&state));
		retained = malloc(sizeof(*retained)); NM_REQUIRE(retained != NULL);
		memcpy(retained, state, sizeof(*state)); native = request_own;
		bufstate = pg_atomic_read_u32(&request_buf.state); memcpy(page.data, disk_shared.data, BLCKSZ);
		NM_REQUIRE(nr_signal(nr_control[1][1], false));
		if (nr_domain == NR_FREEZE) {
			NM_REQUIRE(cluster_resource_x_reconfig_freeze(17, 18, &token));
			nr_retained(&token, true);
		}
		UT_ASSERT(memcmp(retained, state, sizeof(*state)) == 0);
		request_native_unchanged(&native, bufstate);
		UT_ASSERT(memcmp(page.data, disk_shared.data, BLCKSZ) == 0);
		free(retained); close(nr_control[1][1]); complete_idle(); return true;
	}
	holder_pins = holder_fuses = 0; holder_content_busy = false;
	holder_after_prepare = NULL; holder_peer_current = true; holder_write_error = false;
	holder_io_enters = holder_io_ends = 0;
	holder_interrupt_repairs = holder_interrupt_releases = 0;
	holder_requester_connection = 61; holder_requester_node = 1; MyBackendType = B_LMS;
	NM_REQUIRE(nm_receive(master, RESOURCE_X_MSG_BLOCK_TO_N, &block)); f.block = block;
	NM_REQUIRE(cluster_pcm_lock_resource_x_gate_snapshot(&f.gate));
	NM_REQUIRE(cluster_pcm_lock_resource_x_acquire_s_source_forward_exact(&block, 0, 77, 61)
		== RESOURCE_X_APPLY_APPLIED);
	NM_REQUIRE(holder_probe(&f.work));
	header = request_header_lock(&request_buf);
	cluster_pcm_own_snapshot_locked(&request_buf, &request_base); UnlockBufHdr(&request_buf, header);
	NM_REQUIRE(holder_capture(&f) == RESOURCE_X_APPLY_APPLIED);
	NM_REQUIRE(holder_finish(&f) == RESOURCE_X_APPLY_APPLIED);
	NM_REQUIRE(cluster_pcm_lock_resource_x_source_drain_complete_exact(&f.work, RESOURCE_X_APPLY_APPLIED)
		== RESOURCE_X_APPLY_APPLIED);
	NM_REQUIRE(cluster_pcm_lock_resource_x_holder_image_exact(&block.common.logical_assertion, &image)
		== RESOURCE_X_APPLY_APPLIED);
	NM_REQUIRE(cluster_pcm_lock_resource_x_holder_status_exact(&block.common.logical_assertion, &ack)
		== RESOURCE_X_APPLY_APPLIED);
	NM_REQUIRE(nt_holder_state(&image, &state));
	if (nt_source_finished != NULL) nt_source_finished();
	NM_REQUIRE(nc_stage(&state->holder_image_intent, &image_send));
	NM_REQUIRE(nm_send(requester, RESOURCE_X_MSG_IMAGE_OR_GRANT, &image));
	if (!nr_late) NM_REQUIRE(nc_callback(&image_send));
	NM_REQUIRE(nc_stage(&state->holder_status_intent.slot, &proof_send));
	NM_REQUIRE(nm_send(master, RESOURCE_X_MSG_BLOCKED_TO_N, &ack));
	NM_REQUIRE(nc_callback(&proof_send));
	NM_REQUIRE(nr_signal(nr_control[1][1], false));
	retained = malloc(sizeof(*retained)); NM_REQUIRE(retained != NULL);
	memcpy(retained, state, sizeof(*state)); native = request_own;
	bufstate = pg_atomic_read_u32(&request_buf.state); memcpy(page.data, disk_shared.data, BLCKSZ);
	if (nr_domain == NR_FREEZE) {
		NM_REQUIRE(cluster_resource_x_reconfig_freeze(17, 18, &token));
		nr_retained(&token, true);
	}
	UT_ASSERT(memcmp(retained, state, sizeof(*state)) == 0);
	request_native_unchanged(&native, bufstate);
	UT_ASSERT(memcmp(page.data, disk_shared.data, BLCKSZ) == 0);
	if (nr_late) {
		UT_ASSERT_EQ(state->holder_image_intent.state, RESOURCE_X_INTENT_SLOT_STAGED);
		NM_REQUIRE(nc_callback(&image_send));
		UT_ASSERT_EQ(state->holder_image_intent.state, RESOURCE_X_INTENT_SLOT_EMPTY);
	}
	UT_ASSERT(memcmp(&retained->holder_image, &state->holder_image, sizeof(state->holder_image)) == 0);
	UT_ASSERT(memcmp(&retained->holder_status, &state->holder_status, sizeof(state->holder_status)) == 0);
	UT_ASSERT_EQ(state->pi_carrier.transport_refs, retained->pi_carrier.transport_refs);
	UT_ASSERT_EQ(state->pi_carrier.pi_refs, retained->pi_carrier.pi_refs);
	if (nr_domain == NR_FREEZE) NM_REQUIRE(!holder_probe(&stale_work));
	request_native_unchanged(&native, bufstate);
	UT_ASSERT(memcmp(page.data, disk_shared.data, BLCKSZ) == 0);
	free(retained); close(nr_control[1][1]); complete_idle(); return true;
}
static void nr_deliver(void)
{
	if (nr_cut == 4) {
		ResourceXDecodedFrame request;
		ResourceXIntentSlot ticket = request_ticket(&request);
		nr_unsent_callback = ticket;
		UT_ASSERT(nm_send(nc_fd, RESOURCE_X_MSG_ASSERT_X, &request));
		cluster_lms_outbound_resource_x_send_complete(&ticket, true);
		prepared_episode = request.body.acquire_intent;
		input_observation = ordinary_active->observation;
		UT_ASSERT(nr_signal(nr_control[0][0], false)); nr_change(); return;
	}
	nc_deliver(); input_observation = ordinary_active->observation;
	if (nr_cut == 0) nr_change();
}
static void nr_after_copy(void)
{
	if (request_entry()->resource_x_bootstrap_round.delivery_executor_purpose
		== RESOURCE_X_DELIVERY_ACQUIRE_EXECUTING
		&& memcmp(disk_shared.data, nc_image.body.acquire_image.original_image.page_bytes, BLCKSZ) == 0) {
		stage_after_content = NULL; nr_change();
	}
}
static void nr_io_boundary(bool completed)
{
	if ((nr_cut == 2 && !completed) || (nr_cut == 3 && completed)) {
		complete_io_boundary = NULL; nr_change();
	}
}
static bool nr_requester(int master, int holder, int mode, int source, uint64 boot)
{
#ifdef NATIVE_RECONFIG_WRITE
	ClusterPcmXWriterLedgerEntry *entry = NULL;
#else
	ClusterBufmgrFirstFill read;
#endif
	ClusterPcmOwnEntry native;
	ClusterPcmResourceXBootstrapRound retained;
	ClusterICEnvelope env = {0}; ResourceXRequesterJoinSnapshot joined;
	ResourceXApplyResult replay;
	PGAlignedBlock before, after;
	uint32 header;
	volatile bool completed = false;
	bool panic_expected = false;
	bool native_completed, copied;
	(void)mode;
	close(nr_control[0][1]); close(nr_control[1][1]);
#ifdef NATIVE_RECONFIG_WRITE
	rx_writer_start(nr_native, 4);
#else
	if (nr_native == 0) ordinary_start(&read, 2); else rl_start(nr_native, 2);
#endif
	fake_gcs_master_node = 0;
	nc_fd = master; nc_image_fd = holder; nc_mode = 0;
	nc_source_node = source; nc_source_boot = boot;
	auth_observation_hook = nc_auth_peer; ordinary_delivery_transport = nr_deliver;
	nr_reached = false; memcpy(before.data, disk_shared.data, BLCKSZ);
	if (nr_cut == 1) stage_after_content = nr_after_copy;
	if (nr_cut >= 2 && nr_cut < 4) complete_io_boundary = nr_io_boundary;
#ifdef NATIVE_RECONFIG_WRITE
	panic_expected = nr_domain == NR_FREEZE && nr_cut >= 2 && nr_cut < 4;
	if (panic_expected) {
		sigjmp_buf *saved = PG_exception_stack;
		ErrorContextCallback *context = error_context_stack;
		/* Preserve the first native PANIC cut. It must not turn into
		 * catchable ERROR, run generic exit cleanup, or report recovery. */
		nr_panic_caught = false; ordinary_panic_hook = nr_panic;
		if (sigsetjmp(nr_panic_jump, 0) == 0) {
			completed = rx_prepare(NULL) != NULL;
			UT_ASSERT(false);
		}
		PG_exception_stack = saved; error_context_stack = context;
		ordinary_panic_hook = NULL;
		UT_ASSERT(nr_panic_caught);
		UT_ASSERT(!completed);
		UT_ASSERT_EQ(disk_exit_count, 2);
		UT_ASSERT_EQ(fake_lwlock_depth, 0);
	} else {
		entry = rx_prepare(NULL); completed = entry != NULL;
		if (entry != NULL) NM_REQUIRE(xt_use_writer(entry, false));
	}
#else
	completed = nr_native == 0 ? ordinary_run(&read) : rl_run(NULL);
#endif
	UT_ASSERT(nr_reached);
	native_completed = (nr_cut >= 2 && nr_cut < 4)
		|| (nr_domain == NR_UNAVAILABLE && !nr_cancel_wait);
	copied = nr_cut >= 1 && nr_cut < 4;
	if (nr_domain == NR_UNAVAILABLE) {
		UT_ASSERT_EQ(ordinary_pauses, nr_cancel_wait ? ordinary_pause_limit : 3);
		UT_ASSERT_EQ(completed, !nr_cancel_wait);
	} else UT_ASSERT(!completed);
	printf("# Native domain terminal: domain=%u cut=%u native=%u panic=%u gen=%llu purpose=%llu io=%u\n",
		nr_domain, nr_cut, nr_native, ordinary_panics, (unsigned long long)cluster_pcm_own_gen_get(0),
		(unsigned long long)request_entry()->resource_x_bootstrap_round.delivery_executor_purpose,
		complete_io_calls);
	UT_ASSERT_EQ(ordinary_sends, 1); UT_ASSERT_EQ(disk_reads, 0);
	UT_ASSERT_EQ(ordinary_panics, panic_expected ? 1 : 0);
	UT_ASSERT_EQ(ordinary_leaves, panic_expected ? 0 : 1);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), request_base.generation + native_completed);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), panic_expected ? 1 : 0);
	UT_ASSERT_EQ(complete_broadcasts, native_completed ? 1 : 0);
	UT_ASSERT_EQ(complete_io_calls, native_completed ? 1 : 0);
	UT_ASSERT(!ResourceOwnerHasBufferIO(original_io_owner, 1));
	UT_ASSERT(!fake_cv_prepared);
	UT_ASSERT_EQ(pg_atomic_read_u32(&request_buf.state) & (BM_VALID | BM_IO_ERROR | BM_IO_IN_PROGRESS),
		native_completed || (!copied && nr_native != 0) ? BM_VALID : BM_IO_ERROR);
	if (!copied) UT_ASSERT(memcmp(before.data, disk_shared.data, BLCKSZ) == 0);
	else UT_ASSERT(memcmp(disk_shared.data, nc_image.body.acquire_image.original_image.page_bytes, BLCKSZ) == 0);
	if (LWLockHeldByMe(BufferDescriptorGetContentLock(&request_buf)))
		LWLockRelease(BufferDescriptorGetContentLock(&request_buf));
#ifdef NATIVE_RECONFIG_WRITE
	if (!panic_expected) cluster_bufmgr_pcm_x_writer_exception_cleanup_all();
#endif
	NM_REQUIRE(nr_signal(nr_control[0][0], true)); NM_REQUIRE(nr_signal(nr_control[1][0], true));
	retained = request_entry()->resource_x_bootstrap_round; native = request_own;
	header = pg_atomic_read_u32(&request_buf.state); memcpy(after.data, disk_shared.data, BLCKSZ);
	if (nr_domain == NR_FREEZE) nr_retained(&nr_token, true);
	UT_ASSERT(memcmp(&retained, &request_entry()->resource_x_bootstrap_round, sizeof(retained)) == 0);
	request_native_unchanged(&native, header);
	if (nr_cut == 4) {
		UT_ASSERT(!cluster_pcm_lock_resource_x_outbound_transport_complete_exact(
			&nr_unsent_callback, true, ++fake_pcm_clock_us));
	} else {
		env.msg_type = RESOURCE_X_MSG_IMAGE_OR_GRANT; env.source_node_id = source; env.dest_node_id = 1;
		env.epoch = 17; env.payload_length = nc_image.payload_bytes; auth_peer_boot = boot;
		replay = gcs_block_resource_x_requester_join_ingress(&env, &nc_image, 61, &joined);
		UT_ASSERT(replay != RESOURCE_X_APPLY_APPLIED);
	}
	UT_ASSERT(memcmp(&retained, &request_entry()->resource_x_bootstrap_round, sizeof(retained)) == 0);
	request_native_unchanged(&native, header);
	UT_ASSERT(memcmp(after.data, disk_shared.data, BLCKSZ) == 0);
	UT_ASSERT_EQ(ordinary_sends, 1); complete_idle(); return true;
}
static void nr_run_domain(unsigned domain, unsigned cut, bool late, bool cancel)
{
	nr_domain = domain; nr_cut = cut; nr_late = late; nr_cancel_wait = cancel;
	nt_requester = nr_requester; nt_master_role = nr_master; nt_holder_role = nr_holder;
#ifdef NATIVE_RECONFIG_WRITE
	nt_target_mode = PCM_STATE_X;
#else
	nt_target_mode = PCM_STATE_S;
#endif
	for (unsigned source = 0; source < 2; source++) {
		nt_source_mode = source == 0 ? PCM_STATE_S : PCM_STATE_X;
		nt_seed_holder = source == 0 ? nm_parent_as : xt_seed;
		nt_source_finished = source == 0 ? NULL : xt_source_finished;
#ifdef NATIVE_RECONFIG_WRITE
		nr_native = 1;
#else
		nr_native = 0;
#endif
		for (; nr_native <= 2; nr_native++) {
			/* Preserve the original eight methods' evidence labels. */
			if (nr_domain == NR_FREEZE && nr_cut < 4)
				printf("# Native freeze: source=%s target=%s native=%u cut=%u late=%u\n",
					source == 0 ? "S" : "dirty-X", nt_target_mode == PCM_STATE_S ? "S" : "X",
					nr_native, nr_cut, nr_late);
			printf("# Native domain: source=%s target=%s native=%u domain=%u cut=%u late=%u cancel=%u\n",
				source == 0 ? "S" : "dirty-X", nt_target_mode == PCM_STATE_S ? "S" : "X",
				nr_native, nr_domain, nr_cut, nr_late, nr_cancel_wait);
			if (socketpair(AF_UNIX, SOCK_STREAM, 0, nr_control[0]) != 0) { UT_ASSERT(false); return; }
			if (socketpair(AF_UNIX, SOCK_STREAM, 0, nr_control[1]) != 0) {
				close(nr_control[0][0]); close(nr_control[0][1]); UT_ASSERT(false); return;
			}
			/* Every requester is a fresh native process. A PANIC abandons its
			 * callback arguments, so no later episode may inherit that stack. */
			fflush(NULL);
			pid_t requester = fork(); UT_ASSERT(requester >= 0);
			if (requester == 0) {
				UT_ASSERT_EQ(disk_exit_count, 0);
				nt_run(0);
				close(nr_control[0][0]); close(nr_control[1][0]);
				fflush(NULL); _exit(ut_current_failed ? 1 : 0);
			}
			for (unsigned n = 0; n < 2; n++) {
				close(nr_control[n][0]); close(nr_control[n][1]);
			}
			if (requester >= 0) nm_wait_child(requester);
		}
	}
}
static void nr_run(unsigned cut, bool late) { nr_run_domain(NR_FREEZE, cut, late, false); }
static void nr_all_cuts(unsigned domain)
{
	for (unsigned cut = 0; cut < 5; cut++)
		for (unsigned late = 0; late < (cut == 4 ? 1u : 2u); late++)
			nr_run_domain(domain, cut, late, false);
}
static void nr_waits(bool cancel)
{
	for (unsigned cut = 1; cut < 4; cut++)
		for (unsigned late = 0; late < 2; late++)
			nr_run_domain(NR_UNAVAILABLE, cut, late, cancel);
}
UT_TEST(test_freeze_after_SHIP_before_copy) { nr_run(0, false); }
UT_TEST(test_freeze_after_copy_before_C) { nr_run(1, false); }
UT_TEST(test_freeze_after_C_before_IO_end) { nr_run(2, false); }
UT_TEST(test_freeze_after_IO) { nr_run(3, false); }
UT_TEST(test_frozen_SHIP_waits_original_physical_callback) { nr_run(0, true); }
UT_TEST(test_frozen_copy_waits_original_physical_callback) { nr_run(1, true); }
UT_TEST(test_frozen_C_waits_original_physical_callback) { nr_run(2, true); }
UT_TEST(test_frozen_IO_waits_original_physical_callback) { nr_run(3, true); }
UT_TEST(test_freeze_before_master_decision) { nr_run_domain(NR_FREEZE, 4, false, false); }
UT_TEST(test_master_session_change_across_ordinary_cuts) { nr_all_cuts(NR_SESSION); }
UT_TEST(test_admission_change_across_ordinary_cuts) { nr_all_cuts(NR_ADMISSION); }
UT_TEST(test_connection_change_across_ordinary_cuts) { nr_all_cuts(NR_CONNECTION); }
UT_TEST(test_unavailable_observations_resume_without_REQUEST) { nr_waits(false); }
UT_TEST(test_cancel_during_physical_observation_wait) { nr_waits(true); }
int main(void)
{
	UT_PLAN(14);
	UT_RUN(test_freeze_after_SHIP_before_copy);
	UT_RUN(test_freeze_after_copy_before_C);
	UT_RUN(test_freeze_after_C_before_IO_end);
	UT_RUN(test_freeze_after_IO);
	UT_RUN(test_frozen_SHIP_waits_original_physical_callback);
	UT_RUN(test_frozen_copy_waits_original_physical_callback);
	UT_RUN(test_frozen_C_waits_original_physical_callback);
	UT_RUN(test_frozen_IO_waits_original_physical_callback);
	UT_RUN(test_freeze_before_master_decision);
	UT_RUN(test_master_session_change_across_ordinary_cuts);
	UT_RUN(test_admission_change_across_ordinary_cuts);
	UT_RUN(test_connection_change_across_ordinary_cuts);
	UT_RUN(test_unavailable_observations_resume_without_REQUEST);
	UT_RUN(test_cancel_during_physical_observation_wait);
	UT_DONE(); return ut_failed_count == 0 ? 0 : 1;
}
