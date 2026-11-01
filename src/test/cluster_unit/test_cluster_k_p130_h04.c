/* Resident PI preservation through original native/source owners. */
#include "test_cluster_k_p130_h04_current.inc"
#include "test_cluster_k_p130_h02_wal.inc"

/* Only the private GCS statistics cell is a leaf. The barrier predicate
 * calls both original PCM owners rather than the inherited test stub. */
static struct { pg_atomic_uint64 starvation_denied_pending_x_count; } h04_counters;
#define ClusterGcsBlock (&h04_counters)
#define cluster_gcs_block_resource_x_local_s_barrier_active h04_barrier
#include "test_cluster_k_p130_h04_barrier.inc"
#undef cluster_gcs_block_resource_x_local_s_barrier_active
#undef ClusterGcsBlock

static unsigned h04_wait_cancels, h04_delivery_cancels;
static unsigned h04_lowers;
static ClusterPageWalBindingV1 h04_first, h04_last;
static ClusterPcmLocalPiSnapshotV1 h04_local;

static bool
h04_record(ClusterPageWalBindingV1 last)
{
	last.identity.locator = BufTagGetRelFileLocator(&request_buf.tag);
	last.identity.forknum = request_buf.tag.forkNum;
	last.identity.blockno = request_buf.tag.blockNum;
	last.source.claim.identity.origin_node_id = cluster_node_id;
	last.source.claim.identity.origin_thread_id = cluster_node_id + 1;
	last.source.claim.identity.origin_owner_incarnation = cluster_qvotec_get_self_incarnation();
	h04_last = last; h04_first = last;
	h04_first.record_start = 80; h04_first.record_end = 90;
	h04_first.version.mutation_token = 70;
	local_pi_writer = last.source; local_pi_writer_ready = true;
	return cluster_pcm_local_pi_record_first_v1(request_buf.tag, &h04_first, &last)
		&& cluster_pcm_local_pi_snapshot_v1(request_buf.tag, &h04_local);
}

static void
h04_debt_unchanged(void)
{
	ClusterPcmLocalPiSnapshotV1 local;
	ClusterPcmLocalPiFloorV1 floor;
	UT_ASSERT(cluster_pcm_local_pi_snapshot_v1(request_buf.tag, &local));
	UT_ASSERT(memcmp(&local, &h04_local, sizeof(local)) == 0);
	UT_ASSERT(cluster_pcm_local_pi_floor_v1(&h04_last.source, &floor));
	UT_ASSERT_EQ(floor.bounded, h04_lowers); UT_ASSERT_EQ(floor.floor, 80);
	UT_ASSERT(!cluster_pcm_local_pi_retire_v1(&local, NULL, NULL, NULL, 0, NULL));
	UT_ASSERT(cluster_pcm_local_pi_snapshot_v1(request_buf.tag, &local));
	UT_ASSERT(memcmp(&local, &h04_local, sizeof(local)) == 0);
}

static void
h04_cancel_wait(void)
{
	UT_ASSERT_EQ(fake_lwlock_depth, 0); h04_wait_cancels++;
	UT_ASSERT_EQ(complete_commits, 0); UT_ASSERT_EQ(complete_io_calls, 0);
	printf("# H04 cancel=caller local_wait=%u flags=%u gen=" UINT64_FORMAT " C=0 IO=0\n",
		rl_waits, cluster_pcm_own_flags_get(0), cluster_pcm_own_gen_get(0));
	pg_re_throw();
}
static void
h04_cancel_delivery(void)
{
	/* Explicit caller cancellation at CV delivery, before any peer/image.
	 * This is not a successful protocol response or a wait timeout. */
	h04_delivery_cancels++;
	UT_ASSERT_EQ(ordinary_sends, 1); UT_ASSERT_EQ(complete_commits, 0);
	UT_ASSERT_EQ(complete_io_calls, 0); UT_ASSERT_EQ(disk_reads, 0);
	printf("# H04 cancel=caller issued=1 flags=%u gen=" UINT64_FORMAT " C=0 IO=0\n",
		cluster_pcm_own_flags_get(0), cluster_pcm_own_gen_get(0));
	pg_re_throw();
}

static bool
h04_start(bool body, bool valid, bool revoking, HolderFixture *f)
{
	h04_lowers = body ? 2 : 1;
	if (body) {
		/* Real old source producer: capture -> native N/PI -> publication,
		 * with the same actual holder handoff used by production finish. */
		if (!holder_start_target(f, PCM_STATE_X, PCM_STATE_X)
			|| !h04_record(writeback_install_wal())
			|| holder_capture(f) != RESOURCE_X_APPLY_APPLIED
			|| holder_publish_retained(f) != RESOURCE_X_APPLY_APPLIED)
			return false;
		if (!revoking && (holder_finish(f) != RESOURCE_X_APPLY_APPLIED
			|| cluster_pcm_lock_resource_x_source_drain_complete_exact(&f->work,
				RESOURCE_X_APPLY_APPLIED) != RESOURCE_X_APPLY_APPLIED)) return false;
		UT_ASSERT_EQ(f->master.state->pi_carrier.pi_refs, 1);
		UT_ASSERT_EQ(f->master.state->pi_carrier.transport_refs, 1);
		UT_ASSERT_EQ(f->master.state->holder_image.valid, RESOURCE_X_HOLDER_PAIR_PUBLISHED);
		printf("# H04 old E=" UINT64_FORMAT " A=" UINT64_FORMAT " holder=%d boot=" UINT64_FORMAT
			" native=" UINT64_FORMAT " native_flags=%u head=%u pair=%u transport=1 PI=1\n",
			f->block.common.assertion_sequence, f->block.common.authority_generation, cluster_node_id,
			cluster_qvotec_get_self_incarnation(), f->master.state->pi_carrier.completed.generation,
			f->master.state->pi_carrier.completed.flags, f->master.state->requests[1].phase,
			f->master.state->holder_image.valid);
	} else {
		/* Legacy/unknown predecessor descriptor is an explicit initial input,
		 * not a fabricated completed source handoff or carrier receipt. */
		ClusterPageWalBindingV1 seed = local_pi_setup(make_tag(850));
		rl_start(2, 2); fake_gcs_master_node = 0;
		if (!h04_record(seed)) return false;
		pg_atomic_write_u32(&request_own.flags, revoking ? PCM_OWN_FLAG_REVOKING : 0);
	}
	if (!valid) pg_atomic_fetch_and_u32(&request_buf.state, ~BM_VALID);
	rl_waits = rl_share_locks = 0; rl_cancel_wait = false;
	rl_on_wait = h04_cancel_wait; rl_on_share = NULL; rl_after_deliver = NULL;
	h04_wait_cancels = h04_delivery_cancels = 0;
	ordinary_delivery_transport = h04_cancel_delivery;
	MyBackendType = B_BACKEND;
	return true;
}

static void
h04_matrix(bool body, bool valid, bool revoking)
{
	HolderFixture f;
	ClusterPcmOwnSnapshot base;
	ClusterPcmResourceXPiCarrier carrier = {0};
	ClusterPcmResourceXHolderImage retained = {0};
	PGIOAlignedBlock before;
	ResourceXApplyResult observed;
	uint64 generation, token;
	uint32 state;
	if (!h04_start(body, valid, revoking, &f)) { UT_ASSERT(false); complete_idle(); return; }
	state = request_header_lock(&request_buf);
	cluster_pcm_own_snapshot_locked(&request_buf, &base); UnlockBufHdr(&request_buf, state);
	generation = base.generation; token = base.reservation_token;
	memcpy(before.data, disk_shared.data, BLCKSZ);
	if (body) { carrier = f.master.state->pi_carrier; retained = f.master.state->holder_image; }
	observed = cluster_pcm_lock_resource_x_resident_image_preserved_exact(&base, before.data);
	UT_ASSERT_EQ(observed, revoking ? RESOURCE_X_APPLY_INVALID
		: body && valid ? RESOURCE_X_APPLY_APPLIED : RESOURCE_X_APPLY_LOCAL_WAIT);
	printf("# H04 before body=%d VALID=%d REVOKING=%d guard=%d gen=" UINT64_FORMAT
		" token=" UINT64_FORMAT " crc=%u pair=%u transport=%u PI=%u lowers=%u floor=80\n",
		body, valid, revoking, observed, generation, token,
		cluster_gcs_block_compute_checksum(before.data), retained.valid,
		carrier.transport_refs, carrier.pi_refs, h04_lowers);
	/* The old source head is deliberately not completed by a fabricated X
	 * requester. The actual ordinary caller may wait; cancel it explicitly. */
	UT_ASSERT(!rl_run(NULL));
	UT_ASSERT_EQ(ordinary_sends, !body && valid && !revoking ? 1 : 0);
	UT_ASSERT_EQ(h04_delivery_cancels, !body && valid && !revoking ? 1 : 0);
	UT_ASSERT_EQ(h04_wait_cancels, body || revoking ? 1 : 0);
	UT_ASSERT_EQ(rl_share_locks, 0); UT_ASSERT_EQ(ordinary_panics, 0);
	UT_ASSERT_EQ(complete_commits, 0); UT_ASSERT_EQ(complete_io_calls, 0);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), generation);
	UT_ASSERT(cluster_pcm_own_reservation_token_get(0) >= token);
	UT_ASSERT_EQ(request_buf.pcm_state, PCM_STATE_N); UT_ASSERT_EQ(request_buf.buffer_type, BUF_TYPE_PI);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), revoking ? PCM_OWN_FLAG_REVOKING : 0);
	UT_ASSERT_EQ((pg_atomic_read_u32(&request_buf.state) & BM_VALID) != 0, valid);
	UT_ASSERT(memcmp(before.data, disk_shared.data, BLCKSZ) == 0);
	UT_ASSERT(!ResourceOwnerHasBufferIO(CurrentResourceOwner, 1));
	if (body) {
		UT_ASSERT(memcmp(&carrier, &f.master.state->pi_carrier, sizeof(carrier)) == 0);
		UT_ASSERT(memcmp(&retained, &f.master.state->holder_image, sizeof(retained)) == 0);
	}
	h04_debt_unchanged();
	printf("# H04 after body=%d VALID=%d REVOKING=%d REQUEST=%u waits=%u C=0 IO=0"
		" pair=%u transport=%u PI=%u lowers=%u floor=80 flags=%u PANIC=%u"
		" gen=" UINT64_FORMAT " token=" UINT64_FORMAT " ring=%u retained=1\n", body, valid, revoking,
		ordinary_sends, rl_waits, retained.valid, carrier.transport_refs, carrier.pi_refs, h04_lowers,
		cluster_pcm_own_flags_get(0), ordinary_panics, cluster_pcm_own_gen_get(0),
		cluster_pcm_own_reservation_token_get(0), request_shared.rings[1].count);
	complete_idle();
}

static void
h04_prepared_without_body(void)
{
	ClusterGcsFirstFill request;
	ClusterPageWalBindingV1 seed = local_pi_setup(make_tag(850));
	ClusterPcmOwnEntry own;
	uint32 state;
	PGIOAlignedBlock before;
	h04_lowers = 1;
	/* The peer fixture resets shared state while preparing E/A/I. Install
	 * uncovered debt before native execution; pre-REQUEST capacity is not
	 * qualified by this case. */
	if (!rf_start(&request, 2, 2) || !h04_record(seed)) { UT_ASSERT(false); complete_idle(); return; }
	own = request_own; state = pg_atomic_read_u32(&request_buf.state);
	memcpy(before.data, disk_shared.data, BLCKSZ);
	UT_ASSERT_EQ(rf_run(), RESOURCE_X_APPLY_LOCAL_WAIT);
	UT_ASSERT_EQ(rf_run(), RESOURCE_X_APPLY_LOCAL_WAIT);
	request_native_unchanged(&own, state);
	UT_ASSERT(memcmp(before.data, disk_shared.data, BLCKSZ) == 0);
	UT_ASSERT_EQ(request_entry()->resource_x_bootstrap_round.delivery_executor_purpose,
		RESOURCE_X_DELIVERY_ACQUIRE_PREPARED);
	UT_ASSERT_EQ(cluster_pcm_own_delivery_attempt_get(0), prepared_claim.executor_sequence);
	UT_ASSERT_EQ(ordinary_sends, 1); UT_ASSERT_EQ(complete_commits, 0);
	UT_ASSERT_EQ(complete_io_calls, 0); UT_ASSERT_EQ(disk_reads, 0);
	h04_debt_unchanged();
	printf("# H04 prepared E=" UINT64_FORMAT " A=" UINT64_FORMAT " base=" UINT64_FORMAT
		" token=" UINT64_FORMAT " attempts=2 result=LOCAL_WAIT REQUEST=1 C=0 IO=0 lowers=1 floor=80\n",
		input_observation.request.assertion_sequence, input_grant.common.authority_generation,
		request_base.generation, request_token);
	/* PREPARED and its debt intentionally remain owned at this boundary. */
	complete_idle();
}

UT_TEST(test_k_h04_valid_quiet_with_body) { h04_matrix(true, true, false); }
UT_TEST(test_k_h04_valid_revoking_with_body) { h04_matrix(true, true, true); }
UT_TEST(test_k_h04_nonvalid_quiet_with_body) { h04_matrix(true, false, false); }
UT_TEST(test_k_h04_nonvalid_revoking_with_body) { h04_matrix(true, false, true); }
UT_TEST(test_k_h04_valid_quiet_without_body) { h04_matrix(false, true, false); }
UT_TEST(test_k_h04_valid_revoking_without_body) { h04_matrix(false, true, true); }
UT_TEST(test_k_h04_nonvalid_quiet_without_body) { h04_matrix(false, false, false); }
UT_TEST(test_k_h04_nonvalid_revoking_without_body) { h04_matrix(false, false, true); }
UT_TEST(test_k_h04_prepared_without_body_preserves_native_and_WAL) { h04_prepared_without_body(); }
UT_TEST(test_k_h04_clean_PI_independent_S_installs_current) { h03_case(true, 0, 999); }
UT_TEST(test_k_h04_clean_PI_colocated_X_installs_current) { h03_case(false, 1, 7); }
int main(void)
{
	setvbuf(stdout, NULL, _IONBF, 0);
	pg_atomic_init_u64(&h04_counters.starvation_denied_pending_x_count, 0);
#ifdef USE_ASSERT_CHECKING
	printf("# H04 USE_ASSERT_CHECKING=1\n");
#else
	printf("# H04 USE_ASSERT_CHECKING=0\n");
#endif
	UT_PLAN(11);
	UT_RUN(test_k_h04_valid_quiet_with_body);
	UT_RUN(test_k_h04_valid_revoking_with_body);
	UT_RUN(test_k_h04_nonvalid_quiet_with_body);
	UT_RUN(test_k_h04_nonvalid_revoking_with_body);
	UT_RUN(test_k_h04_valid_quiet_without_body);
	UT_RUN(test_k_h04_valid_revoking_without_body);
	UT_RUN(test_k_h04_nonvalid_quiet_without_body);
	UT_RUN(test_k_h04_nonvalid_revoking_without_body);
	UT_RUN(test_k_h04_prepared_without_body_preserves_native_and_WAL);
	UT_RUN(test_k_h04_clean_PI_independent_S_installs_current);
	UT_RUN(test_k_h04_clean_PI_colocated_X_installs_current);
	UT_DONE(); return ut_failed_count == 0 ? 0 : 1;
}
