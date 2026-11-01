/* Ordinary S-to-X across domain loss, with an independent real master.
 * Only authentication, sockets and scheduling are fixture leaves.
 * Author: SqlRush <sqlrush@gmail.com> */
#define NATIVE_MASTER_RESIDENT_WRITE_COMPOSITION
#include "test_cluster_native_master_fixture.inc"

enum { SD_FREEZE, SD_SESSION, SD_ADMISSION, SD_CONNECTION, SD_UNAVAILABLE };
static unsigned sd_domain, sd_cut;
static bool sd_dirty, sd_late, sd_reached, sd_panicked, sd_cancel_wait;
static int sd_control[2];
static sigjmp_buf sd_panic_jump;
static ResourceXReconfigToken sd_token;
static ResourceXDecodedFrame sd_seed_done;
static ResourceXIntentSlot sd_seed_send;

/* Control bytes schedule independent roles; they are not protocol frames. */
static bool sd_signal(int fd, bool send)
{
	uint8 byte = 0x6d;
	NM_REQUIRE(nm_transfer(fd, &byte, sizeof(byte), send));
	NM_REQUIRE(byte == 0x6d);
	return true;
}
static void sd_seed(const ResourceXIntentSlot *slot, const ResourceXDecodedFrame *done)
{
	sd_seed_send = *slot; sd_seed_done = *done;
}
static void sd_change(void)
{
	UT_ASSERT(!sd_reached); UT_ASSERT_EQ(fake_lwlock_depth, 0);
	if (sd_domain == SD_FREEZE) {
		UT_ASSERT(cluster_resource_x_reconfig_freeze(17, 18, &sd_token));
		UT_ASSERT_EQ(sd_token.old_formation, 17); UT_ASSERT_EQ(sd_token.new_formation, 18);
	} else if (sd_domain == SD_SESSION) {
		ordinary_master_session++; wc_session_id++;
	} else if (sd_domain == SD_ADMISSION) {
		master_durable_admission = false; wc_r4++;
	} else if (sd_domain == SD_CONNECTION) {
		ordinary_connection++;
	} else {
		UT_ASSERT_EQ(sd_domain, SD_UNAVAILABLE);
		ordinary_unavailable = sd_cancel_wait ? 64 : 3;
	}
	sd_reached = true;
}
static void sd_native_cut(void)
{
	ClusterPcmResourceXBootstrapRound *round = &request_entry()->resource_x_bootstrap_round;
	if ((sd_cut == 2 && round->delivery_executor_purpose == RESOURCE_X_DELIVERY_ACQUIRE_READ_ARMED
			&& request_buf.pcm_state == PCM_STATE_S)
		|| (sd_cut == 3 && request_buf.pcm_state == PCM_STATE_X
			&& cluster_pcm_own_gen_get(0) == prepared_episode.base_generation + 1)) {
		rx_native_boundary = NULL; sd_change();
	}
}
static void sd_panic(void)
{
	/* A terminal PANIC cannot unwind through the abandoned PG_TRY frames.
	 * Inspect both live owner arguments, then end this fresh requester. */
	/* The inner cleanup may already have returned to the outer scope. */
	UT_ASSERT(disk_exit_count == 1 || disk_exit_count == 2);
	if (disk_exit_count >= 1) {
		ClusterBufmgrFirstFill *read = (void *)DatumGetPointer(disk_exit_arguments[0]);
		UT_ASSERT(disk_exit_callbacks[0] == cluster_bufmgr_first_fill_error);
		UT_ASSERT(read != NULL);
		if (read != NULL) {
			UT_ASSERT(read->buf == &request_buf && read->owner == CurrentResourceOwner);
			UT_ASSERT(read->pending && read->delegated);
			UT_ASSERT(memcmp(&read->request.episode, &prepared_episode, sizeof(prepared_episode)) == 0);
			if (disk_exit_count == 2) {
				ClusterAcquireResidentGuard *guard = (void *)DatumGetPointer(disk_exit_arguments[1]);
				UT_ASSERT(disk_exit_callbacks[1] == cluster_bufmgr_acquire_resident_cleanup);
				UT_ASSERT(guard != NULL);
				if (guard != NULL) {
					UT_ASSERT(guard->original.buf == read->buf && guard->original.owner == read->owner);
					UT_ASSERT(memcmp(&read->base, &guard->original.base, sizeof(read->base)) == 0);
					UT_ASSERT(memcmp(&read->request.episode, &guard->original.episode, sizeof(prepared_episode)) == 0);
					UT_ASSERT(memcmp(&read->claim, &guard->original.original, sizeof(read->claim)) == 0);
				}
			}
		}
	}
	sd_panicked = true; siglongjmp(sd_panic_jump, 1);
}
static bool sd_master(int fd, int mode)
{
	PublishFixture f;
	ResourceXDecodedFrame request, grant;
	ResourceXIntentSlot staged = {0};
	ClusterPcmResourceXMasterRequest canonical;
	ResourceXReconfigToken token;
	ResourceXReconfigBatch batch;
	ResourceXZeroResidualProof zero;
	uint64 old_grant;
	(void)mode;
	close(sd_control[0]);
	NM_REQUIRE(nm_master(fd, 0)); /* preceding ordinary S, fully consumed */
	NM_REQUIRE(nm_receive(fd, RESOURCE_X_MSG_ASSERT_X, &request));
	NM_REQUIRE(nm_master_enqueue(&f, &request));
	UT_ASSERT_EQ(f.state->requests[1].phase, RESOURCE_X_MASTER_QUEUED);
	old_grant = f.state->holder_grants[1].grant_generation;
	if (sd_cut != 0) {
		publish_apply(&f);
		grant = publish_decode(f.state->grant_intent.payload, false);
		UT_ASSERT_EQ(grant.body.acquire_decision.source_kind, RESOURCE_X_ACQUIRE_SOURCE_LOCAL_CURRENT);
		UT_ASSERT_EQ(grant.body.acquire_decision.required_holders_bitmap, 0);
		UT_ASSERT_EQ(grant.body.acquire_decision.source_node, 1);
		UT_ASSERT_EQ(grant.body.acquire_decision.directory_mode, PCM_STATE_S);
		UT_ASSERT_EQ(grant.body.acquire_decision.episode.path, RESOURCE_X_ACQUIRE_RESIDENT);
		UT_ASSERT_EQ(grant.common.target_mode, PCM_STATE_X);
		NM_REQUIRE(cluster_pcm_lock_resource_x_outbound_intent_stage_capture_exact(
			&f.state->grant_intent.slot, ++fake_pcm_clock_us, &staged) == RESOURCE_X_INTENT_STAGED);
		if (!sd_late) NM_REQUIRE(nm_master_callback(&staged, true));
		NM_REQUIRE(nm_send(fd, RESOURCE_X_MSG_IMAGE_OR_GRANT, &grant));
	} else NM_REQUIRE(sd_signal(sd_control[1], true));
	canonical = f.state->requests[1];
	NM_REQUIRE(sd_signal(sd_control[1], false));
	if (sd_domain == SD_FREEZE) {
		NM_REQUIRE(cluster_resource_x_reconfig_freeze(17, 18, &token));
		if (sd_cut == 0) {
			ResourceXReconfigResult result = cluster_resource_x_reconfig_sweep(&token, 4, &batch);
			UT_ASSERT_EQ(result, RESOURCE_X_RECONFIG_MORE);
			UT_ASSERT_EQ(f.state->requests[1].phase, RESOURCE_X_MASTER_NONE);
		} else UT_ASSERT_EQ(cluster_resource_x_reconfig_sweep(&token, 4, &batch), RESOURCE_X_RECONFIG_ORPHAN);
		UT_ASSERT_EQ(batch.zero_residual, 0);
		UT_ASSERT(!cluster_resource_x_reconfig_zero_proof_exact(&token, &zero));
		UT_ASSERT(!cluster_resource_x_reconfig_thaw_exact(&token));
	}
	if (sd_domain != SD_FREEZE || sd_cut != 0)
		UT_ASSERT(memcmp(&canonical, &f.state->requests[1], sizeof(canonical)) == 0);
	UT_ASSERT_EQ(f.state->holder_grants[1].grant_generation, sd_domain == SD_FREEZE ? 0 : old_grant);
	UT_ASSERT_EQ(((struct StopPcmEntryLayout *)fake_pcm_entries.data[0])->x_holder_node, -1);
	UT_ASSERT_EQ(pg_atomic_read_u32(&((struct StopPcmEntryLayout *)fake_pcm_entries.data[0])->s_holders_bitmap), 2);
	if (sd_late && sd_cut != 0) {
		UT_ASSERT_EQ(f.state->grant_intent.slot.state, RESOURCE_X_INTENT_SLOT_STAGED);
		NM_REQUIRE(nm_master_callback(&staged, true));
		UT_ASSERT_EQ(f.state->grant_intent.slot.state, RESOURCE_X_INTENT_SLOT_EMPTY);
		UT_ASSERT(memcmp(&canonical, &f.state->requests[1], sizeof(canonical)) == 0);
	}
	close(sd_control[1]); complete_idle(); return true;
}
static void sd_deliver(void)
{
	if (sd_cut == 0) {
		ResourceXDecodedFrame request;
		ResourceXIntentSlot ticket = request_ticket(&request);
		UT_ASSERT(nm_send(nm_fd, RESOURCE_X_MSG_ASSERT_X, &request));
		cluster_lms_outbound_resource_x_send_complete(&ticket, true);
		prepared_episode = request.body.acquire_intent;
		input_observation = ordinary_active->observation;
		UT_ASSERT(sd_signal(sd_control[0], false)); sd_change();
	} else {
		nm_deliver();
		if (sd_cut == 1) sd_change();
	}
}
static bool sd_replay(void)
{
	ClusterPcmResourceXBootstrapRound round = request_entry()->resource_x_bootstrap_round;
	ClusterPcmOwnEntry native = request_own;
	ClusterICEnvelope env = {0}; ResourceXRequesterJoinSnapshot join;
	uint32 header = pg_atomic_read_u32(&request_buf.state);
	env.msg_type = RESOURCE_X_MSG_SETTLEMENT_OR_RELEASE; env.source_node_id = 0;
	env.dest_node_id = 1; env.epoch = 17; env.payload_length = sd_seed_done.payload_bytes;
	auth_peer_boot = 31;
	NM_REQUIRE(gcs_block_resource_x_requester_join_ingress(&env, &sd_seed_done, 61, &join) == RESOURCE_X_APPLY_STALE);
	NM_REQUIRE(!cluster_pcm_lock_resource_x_outbound_transport_complete_exact(
		&sd_seed_send, true, ++fake_pcm_clock_us));
	if (sd_cut != 0) {
		ResourceXApplyResult result;
		env.msg_type = RESOURCE_X_MSG_IMAGE_OR_GRANT; env.payload_length = nm_grant.payload_bytes;
		result = gcs_block_resource_x_requester_join_ingress(&env, &nm_grant, 61, &join);
		NM_REQUIRE(result != RESOURCE_X_APPLY_APPLIED);
	}
	UT_ASSERT(memcmp(&round, &request_entry()->resource_x_bootstrap_round, sizeof(round)) == 0);
	UT_ASSERT(memcmp(&native, &request_own, sizeof(native)) == 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&request_buf.state), header);
	return true;
}
static bool sd_parent(int fd, int mode)
{
	PGAlignedBlock page;
	ClusterPcmResourceXBootstrapRound retained;
	ClusterPcmXWriterLedgerEntry *volatile entry = NULL;
	sigjmp_buf *saved;
	ErrorContextCallback *context;
	uint32 semantic;
	(void)mode; close(sd_control[1]);
	rx_native_boundary = NULL; nm_settled_observer = sd_seed;
	NM_REQUIRE(nm_parent_as(fd, 0, 1)); nm_settled_observer = NULL;
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.resident_s_authorized, 1);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.has_acquire_intent, 0);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.send.pending, 0);
	memset(&request_shared, 0, sizeof(request_shared));
	pg_atomic_init_u64(&request_shared.resource_x_transport_mutation_sequence, 1);
	nm_fd = fd; nm_mode = 0; ordinary_delivery_transport = sd_deliver;
	cluster_enabled = cluster_shared_config = cluster_gcs_block_local_cache = true;
	wc_object_enabled = false; wc_object_valid = true;
	wc_r4 = 77; wc_session_id = 31; wc_master = 0; wc_path = RESOURCE_X_WRITER_TARGET;
	wc_legacy = wc_failures = wc_waits = wc_object_checks = wc_unavailable = wc_pauses = 0;
	wc_cancel_pause = rx_barrier = rx_cancel_wait = false;
	if (sd_dirty) pg_atomic_fetch_or_u32(&request_buf.state, BM_DIRTY | BM_JUST_DIRTIED | BM_CHECKPOINT_NEEDED);
	semantic = pg_atomic_read_u32(&request_buf.state) & CLUSTER_PCM_OWN_SEMANTIC_BUF_STATE_MASK;
	memcpy(page.data, disk_shared.data, BLCKSZ);
	ordinary_active = NULL; ordinary_sends = ordinary_pauses = ordinary_leaves = ordinary_panics = 0;
	ordinary_current_calls = ordinary_peer_samples = 0;
	disk_reads = complete_commits = complete_io_calls = complete_broadcasts = 0;
	sd_reached = sd_panicked = false; MyBackendType = B_BACKEND;
	rx_native_boundary = sd_cut >= 2 ? sd_native_cut : NULL;
	saved = PG_exception_stack; context = error_context_stack;
	ordinary_panic_hook = sd_panic;
	if (sigsetjmp(sd_panic_jump, 0) == 0) entry = rx_prepare(NULL);
	PG_exception_stack = saved; error_context_stack = context; ordinary_panic_hook = NULL;
	UT_ASSERT(sd_reached);
	if (sd_domain == SD_UNAVAILABLE && !sd_cancel_wait) {
		NM_REQUIRE(entry != NULL);
		LWLockAcquire(entry->content_lock, LW_EXCLUSIVE);
		NM_REQUIRE(cluster_bufmgr_pcm_x_writer_activate(entry, true));
		cluster_bufmgr_pcm_x_writer_mark_releasing(entry); LWLockRelease(entry->content_lock);
		cluster_bufmgr_pcm_x_writer_release(entry);
	} else UT_ASSERT(entry == NULL);
	UT_ASSERT_EQ(sd_panicked, sd_domain == SD_FREEZE && sd_cut == 3);
	UT_ASSERT_EQ(ordinary_panics, sd_panicked ? 1 : 0);
	UT_ASSERT_EQ(ordinary_leaves, sd_panicked ? 0 : 1);
	UT_ASSERT_EQ(disk_exit_count, sd_panicked ? 1 : 0);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), sd_panicked ? 1 : 0);
	if (sd_domain == SD_UNAVAILABLE) UT_ASSERT_EQ(ordinary_pauses, sd_cancel_wait ? ordinary_pause_limit : 3);
	printf("# S-X domain terminal: domain=%u cut=%u panic=%u generation=%llu purpose=%llu flags=%u delivery=%llu leaves=%u\n",
		sd_domain, sd_cut, ordinary_panics, (unsigned long long)cluster_pcm_own_gen_get(0),
		(unsigned long long)request_entry()->resource_x_bootstrap_round.delivery_executor_purpose,
		cluster_pcm_own_flags_get(0), (unsigned long long)cluster_pcm_own_delivery_attempt_get(0), ordinary_leaves);
	UT_ASSERT_EQ(ordinary_sends, 1); UT_ASSERT_EQ(wc_legacy, 0); UT_ASSERT_EQ(disk_reads, 0);
	UT_ASSERT_EQ(complete_commits, 0); UT_ASSERT_EQ(complete_io_calls, 0); UT_ASSERT_EQ(complete_broadcasts, 0);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), request_base.generation + (sd_cut == 3 || (sd_domain == SD_UNAVAILABLE && !sd_cancel_wait)));
	UT_ASSERT_EQ(request_buf.pcm_state, sd_cut == 3 || (sd_domain == SD_UNAVAILABLE && !sd_cancel_wait) ? PCM_STATE_X : PCM_STATE_S);
	UT_ASSERT_EQ(request_buf.buffer_type, sd_cut == 3 || (sd_domain == SD_UNAVAILABLE && !sd_cancel_wait) ? BUF_TYPE_XCUR : BUF_TYPE_SCUR);
	UT_ASSERT_EQ(pg_atomic_read_u32(&request_buf.state) & CLUSTER_PCM_OWN_SEMANTIC_BUF_STATE_MASK, semantic);
	UT_ASSERT(memcmp(page.data, disk_shared.data, BLCKSZ) == 0);
	UT_ASSERT(!ResourceOwnerHasBufferIO(original_io_owner, 1)); UT_ASSERT(!fake_cv_prepared);
	UT_ASSERT_EQ(fake_lwlock_depth, 0);
	NM_REQUIRE(sd_replay());
	retained = request_entry()->resource_x_bootstrap_round;
	UT_ASSERT(retained.has_acquire_intent != 0);
	UT_ASSERT_EQ(retained.highest_attempt_floor, input_observation.request.assertion_sequence);
	NM_REQUIRE(sd_signal(sd_control[0], true));
	close(sd_control[0]); complete_idle(); return true;
}
static void sd_run(unsigned domain, unsigned cut)
{
	sd_domain = domain; sd_cut = cut;
	if (domain != SD_UNAVAILABLE) sd_cancel_wait = false;
	for (unsigned dirty = 0; dirty < 2; dirty++)
		for (unsigned late = 0; late < (cut == 0 ? 1u : 2u); late++) {
			pid_t requester;
			sd_dirty = dirty; sd_late = late;
			printf("# S-X domain: domain=%u cut=%u dirty=%u late=%u cancel=%u\n", domain, cut, dirty, late, sd_cancel_wait);
			if (socketpair(AF_UNIX, SOCK_STREAM, 0, sd_control) != 0) { UT_ASSERT(false); return; }
			fflush(NULL); requester = fork(); UT_ASSERT(requester >= 0);
			if (requester == 0) {
				UT_ASSERT_EQ(disk_exit_count, 0);
				nm_run_roles(0, sd_master, sd_parent);
				fflush(NULL); _exit(ut_current_failed ? 1 : 0);
			}
			close(sd_control[0]); close(sd_control[1]);
			if (requester < 0) return;
			nm_wait_child(requester);
		}
}
#define SD_CASE(name, domain, cut) UT_TEST(name) { sd_run(domain, cut); }
SD_CASE(test_S_X_freeze_before_decision, SD_FREEZE, 0)
SD_CASE(test_S_X_freeze_after_decision, SD_FREEZE, 1)
SD_CASE(test_S_X_freeze_armed_before_C, SD_FREEZE, 2)
SD_CASE(test_S_X_freeze_after_native_C, SD_FREEZE, 3)
SD_CASE(test_S_X_session_before_decision, SD_SESSION, 0)
SD_CASE(test_S_X_session_after_decision, SD_SESSION, 1)
SD_CASE(test_S_X_session_armed_before_C, SD_SESSION, 2)
SD_CASE(test_S_X_session_after_native_C, SD_SESSION, 3)
SD_CASE(test_S_X_admission_before_decision, SD_ADMISSION, 0)
SD_CASE(test_S_X_admission_after_decision, SD_ADMISSION, 1)
SD_CASE(test_S_X_admission_armed_before_C, SD_ADMISSION, 2)
SD_CASE(test_S_X_admission_after_native_C, SD_ADMISSION, 3)
SD_CASE(test_S_X_connection_before_decision, SD_CONNECTION, 0)
SD_CASE(test_S_X_connection_after_decision, SD_CONNECTION, 1)
SD_CASE(test_S_X_connection_armed_before_C, SD_CONNECTION, 2)
SD_CASE(test_S_X_connection_after_native_C, SD_CONNECTION, 3)
UT_TEST(test_S_X_unavailable_domain_observation_waits_without_request) { sd_cancel_wait = false; sd_run(SD_UNAVAILABLE, 2); }
UT_TEST(test_S_X_cancel_during_domain_observation_wait) { sd_cancel_wait = true; sd_run(SD_UNAVAILABLE, 2); }
UT_TEST(test_S_X_post_C_unavailable_observation_waits_without_request) { sd_cancel_wait = false; sd_run(SD_UNAVAILABLE, 3); }
UT_TEST(test_S_X_post_C_cancel_during_domain_observation_wait) { sd_cancel_wait = true; sd_run(SD_UNAVAILABLE, 3); }
int main(void)
{
	UT_PLAN(20);
	UT_RUN(test_S_X_freeze_before_decision); UT_RUN(test_S_X_freeze_after_decision);
	UT_RUN(test_S_X_freeze_armed_before_C); UT_RUN(test_S_X_freeze_after_native_C);
	UT_RUN(test_S_X_session_before_decision); UT_RUN(test_S_X_session_after_decision);
	UT_RUN(test_S_X_session_armed_before_C); UT_RUN(test_S_X_session_after_native_C);
	UT_RUN(test_S_X_admission_before_decision); UT_RUN(test_S_X_admission_after_decision);
	UT_RUN(test_S_X_admission_armed_before_C); UT_RUN(test_S_X_admission_after_native_C);
	UT_RUN(test_S_X_connection_before_decision); UT_RUN(test_S_X_connection_after_decision);
	UT_RUN(test_S_X_connection_armed_before_C); UT_RUN(test_S_X_connection_after_native_C);
	UT_RUN(test_S_X_unavailable_domain_observation_waits_without_request);
	UT_RUN(test_S_X_cancel_during_domain_observation_wait);
	UT_RUN(test_S_X_post_C_unavailable_observation_waits_without_request);
	UT_RUN(test_S_X_post_C_cancel_during_domain_observation_wait);
	UT_DONE(); return ut_failed_count == 0 ? 0 : 1;
}
