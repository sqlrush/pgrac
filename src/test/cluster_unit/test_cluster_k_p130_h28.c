/*
 * H28: real cancellation/final-pin and visibility lifetime owners.
 * Transport, lock scheduling, admission policy and storage are fixture leaves;
 * these tests grant no native authority and do not certify whole-node stop.
 */
#ifdef K_P130_H28_VISIBILITY
#include "test_cluster_k_p130_h28_visibility.inc"

int MyProcPid = 4281;
static int semantic_activation_exit_hook_pid;

static ClusterTxOutcome
h28_borrowed_sample(void)
{
	raise_at(AT_COPY);
	return CLUSTER_TX_UNKNOWN;
}

#define cluster_runtime_visibility_resolve_exact_origin_held(...) h28_borrowed_sample()
#define cluster_gcs_block_r4_tx_resolve_fetch_and_wait(...) CLUSTER_TX_UNKNOWN
#define cluster_tx_identity_trace_origin(...) ((void)0)
#include "test_cluster_k_p130_h28_borrowed.inc"
#undef cluster_tx_identity_trace_origin
#undef cluster_gcs_block_r4_tx_resolve_fetch_and_wait
#undef cluster_runtime_visibility_resolve_exact_origin_held
#include "test_cluster_k_p130_h28_exit.inc"

static ClusterTxOutcome
h28_invoke_borrowed(const ClusterSemanticAdmissionToken *outer)
{
	ClusterTxLocator locator = { 0 };
	ClusterTxResolution out, zero = { 0 };
	ClusterTxResolveReason reason;
	ClusterTxOutcome outcome;

	memset(&out, 0xa5, sizeof(out));
	outcome = cluster_runtime_visibility_resolve_exact_origin_admitted(
		&locator, CLUSTER_TX_RESOLVE_VISIBILITY, outer, &out, &reason);
	UT_ASSERT_EQ(outcome, CLUSTER_TX_UNKNOWN);
	UT_ASSERT_EQ(memcmp(&out, &zero, sizeof(out)), 0);
	return outcome;
}

/* Each fresh case runs fault -> unwind -> successor without debt reset.
 * Re-entered outer tokens must survive every owned-caller error boundary. */
static void
h28_owned_faults(unsigned caller, const int *points, unsigned npoints)
{
	for (unsigned i = 0; i < npoints; i++) {
		ClusterSemanticAdmissionToken outer;
		ClusterSemanticAdmissionToken before;
		unsigned expected_cancels = points[i] == AT_ACQUIRE || points[i] == AT_COPY
			|| points[i] == AT_RELEASE;
		volatile bool caught = false;

		reset_fixture(); /* New scenario, no live owner from the previous one. */
		UT_ASSERT_EQ(fixture_enter(CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
			CLUSTER_SEMANTIC_TARGET_SIDE, &outer), CLUSTER_SEMANTIC_ADMISSION_OK);
		before = outer;
		fault = points[i];
		PG_TRY();
		{
			(void)invoke(caller);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(caught);
		assert_debt(1);
		UT_ASSERT_EQ(leaves, 1);
		UT_ASSERT_EQ(cancels, expected_cancels);
		UT_ASSERT_EQ(memcmp(&outer, &before, sizeof(outer)), 0);
		fault = AT_NONE;
		(void)invoke(caller);
		assert_debt(1);
		UT_ASSERT_EQ(leaves, 2);
		UT_ASSERT_EQ(cancels, expected_cancels);
		fixture_leave(&outer);
		fixture_leave(&outer);
		assert_debt(0);
		UT_ASSERT_EQ(leaves, 3);
		printf("# owned caller=%u fault=%d successor/debt=0\n", caller, points[i]);
	}
}

UT_TEST(test_h28_owner_callback_errors_preserve_outer_and_successor)
{
	const int points[] = { AT_OWNER, AT_ROOT, AT_ACQUIRE, AT_COPY, AT_RELEASE, AT_RECHECK };
	h28_owned_faults(0, points, lengthof(points));
}

UT_TEST(test_h28_terminal_callback_errors_preserve_outer_and_successor)
{
	const int points[] = { AT_ROOT, AT_ACQUIRE, AT_COPY, AT_RELEASE, AT_RECHECK };
	h28_owned_faults(1, points, lengthof(points));
}

UT_TEST(test_h28_mx_data_callback_errors_preserve_outer_and_successor)
{
	/* This fixture stops at the DATA freeze, before the second TT phase. */
	const int points[] = { AT_ROOT, AT_ACQUIRE, AT_COPY, AT_RELEASE };
	h28_owned_faults(2, points, lengthof(points));
}

UT_TEST(test_h28_borrowed_callback_errors_leave_caller_token_untouched)
{
	const int points[] = { AT_RECHECK, AT_ROOT, AT_ACQUIRE, AT_COPY, AT_RELEASE };
	for (unsigned i = 0; i < lengthof(points); i++) {
		ClusterSemanticAdmissionToken outer, before;
		volatile bool caught = false;
		unsigned expected_cancels = points[i] == AT_ACQUIRE || points[i] == AT_COPY
			|| points[i] == AT_RELEASE;
		reset_fixture();
		UT_ASSERT_EQ(fixture_enter(CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
			CLUSTER_SEMANTIC_TARGET_SIDE, &outer), CLUSTER_SEMANTIC_ADMISSION_OK);
		before = outer;
		fault = points[i];
		PG_TRY();
		{
			(void)h28_invoke_borrowed(&outer);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(caught);
		assert_debt(1);
		UT_ASSERT_EQ(leaves, 0);
		UT_ASSERT_EQ(cancels, expected_cancels);
		UT_ASSERT_EQ(memcmp(&outer, &before, sizeof(outer)), 0);
		fault = AT_NONE;
		(void)h28_invoke_borrowed(&outer);
		assert_debt(1);
		UT_ASSERT_EQ(leaves, 0);
		UT_ASSERT_EQ(cancels, expected_cancels);
		fixture_leave(&outer);
		assert_debt(0);
		printf("# borrowed fault=%d successor/debt=0\n", points[i]);
	}
}

UT_TEST(test_h28_borrowed_guard_cleanup_is_idempotent)
{
	ClusterSemanticAdmissionToken outer;
	ClusterUndoBlock0CurrentGuard guard = { 0 };
	ClusterRuntimeCandidateCleanup cleanup = { &guard, true };
	reset_fixture();
	(void)fixture_enter(CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
		CLUSTER_SEMANTIC_TARGET_SIDE, &outer);
	guards = 1; /* Provider supplied one guard; cleanup owns only this guard. */
	for (int i = 0; i < 3; i++)
		cluster_runtime_visibility_candidate_cleanup(0, PointerGetDatum(&cleanup));
	cluster_runtime_visibility_candidate_cleanup(0, PointerGetDatum(NULL));
	assert_debt(1);
	UT_ASSERT_EQ(cancels, 1);
	UT_ASSERT_EQ(leaves, 0);
	UT_ASSERT(!cleanup.active && outer.entered);
	fixture_leave(&outer);
	fixture_leave(&outer);
	assert_debt(0);
	UT_ASSERT_EQ(leaves, 1);
}

UT_TEST(test_h28_exit_cleanup_retires_only_local_debt_once)
{
	ClusterSemanticAdmissionToken abandoned[2], successor;
	int old_pid = MyProcPid;
	reset_fixture();
	semantic_activation_exit_hook_pid = MyProcPid;
	for (unsigned i = 0; i < lengthof(abandoned); i++)
		(void)fixture_enter(CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
			CLUSTER_SEMANTIC_TARGET_SIDE, &abandoned[i]);
	/* Three shared units belong to another process, not this exit owner. */
	for (int i = 0; i < 3; i++)
		UT_ASSERT(semantic_activation_counter_increment(
			&state.inflight[CLUSTER_SEMANTIC_TARGET_SIDE][0]));
	semantic_activation_exit_cleanup(0, Int32GetDatum(old_pid + 1));
	UT_ASSERT_EQ(pg_atomic_read_u32(&state.inflight[CLUSTER_SEMANTIC_TARGET_SIDE][0]), 5);
	UT_ASSERT_EQ(semantic_activation_local_inflight[CLUSTER_SEMANTIC_TARGET_SIDE][0], 2);
	semantic_activation_exit_cleanup(0, Int32GetDatum(old_pid));
	semantic_activation_exit_cleanup(0, Int32GetDatum(old_pid));
	UT_ASSERT_EQ(pg_atomic_read_u32(&state.inflight[CLUSTER_SEMANTIC_TARGET_SIDE][0]), 3);
	UT_ASSERT_EQ(semantic_activation_local_inflight[CLUSTER_SEMANTIC_TARGET_SIDE][0], 0);
	UT_ASSERT_EQ(semantic_activation_exit_hook_pid, 0);
	UT_ASSERT_EQ(InterruptHoldoffCount, 0);
	/* A successor PID is a controlled process leaf, never reused old tokens. */
	MyProcPid++;
	semantic_activation_exit_hook_pid = MyProcPid;
	(void)fixture_enter(CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
		CLUSTER_SEMANTIC_TARGET_SIDE, &successor);
	semantic_activation_exit_cleanup(0, Int32GetDatum(old_pid));
	UT_ASSERT_EQ(pg_atomic_read_u32(&state.inflight[CLUSTER_SEMANTIC_TARGET_SIDE][0]), 4);
	UT_ASSERT_EQ(semantic_activation_local_inflight[CLUSTER_SEMANTIC_TARGET_SIDE][0], 1);
	fixture_leave(&successor);
	semantic_activation_exit_cleanup(0, Int32GetDatum(MyProcPid));
	UT_ASSERT_EQ(pg_atomic_read_u32(&state.inflight[CLUSTER_SEMANTIC_TARGET_SIDE][0]), 3);
	UT_ASSERT_EQ(semantic_activation_local_inflight[CLUSTER_SEMANTIC_TARGET_SIDE][0], 0);
	UT_ASSERT_EQ(callback_depth + guards + cancels, 0);
	MyProcPid = old_pid;
}

UT_TEST(test_h28_locator_error_precedes_owned_admission)
{
	ClusterSemanticAdmissionToken outer;
	volatile bool caught = false;
	reset_fixture();
	(void)fixture_enter(CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
		CLUSTER_SEMANTIC_TARGET_SIDE, &outer);
	fault = AT_LOCATOR;
	PG_TRY(); { (void)invoke(1); }
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	UT_ASSERT(caught);
	assert_debt(1);
	UT_ASSERT_EQ(leaves + cancels, 0);
	fault = AT_NONE;
	(void)invoke(1);
	assert_debt(1);
	UT_ASSERT_EQ(leaves, 1);
	fixture_leave(&outer);
	assert_debt(0);
}

#else
#include "test_cluster_k_p130_h28_read_source.inc"

typedef struct H28Read
{
	BufferDesc buf;
	ClusterPcmOwnEntry entry;
	ClusterBufmgrReadSource read;
	ClusterPcmOwnEntry *saved;
	bool saved_enabled, saved_shared;
} H28Read;

static void
h28_start(H28Read *c, unsigned shared_pins, unsigned private_pins)
{
	memset(c, 0, sizeof(*c));
	c->saved = ClusterPcmOwnArray;
	c->saved_enabled = cluster_enabled;
	c->saved_shared = cluster_shared_config;
	n_predecessor_fixture(&c->buf, &c->entry, BUF_TYPE_CURRENT);
	transition_buf = &c->buf;
	transition_content_held = transition_mapping_held = false;
	read_reclaim_drift = 0;
	CritSectionCount = 0;
	cluster_enabled = cluster_shared_config = true;
	/* Fresh first-fill input, before any I/O error exists. */
	pg_atomic_write_u32(&c->buf.state, BM_TAG_VALID | shared_pins * BUF_REFCOUNT_ONE);
	abort_read_image_pin.refcount = private_pins;
	abort_read_image_forgets = abort_read_image_freed = abort_read_image_signals = 0;
	failed_read_deletes = failed_read_frees = failed_read_lock_acquires = 0;
	failed_read_lock_error = failed_read_expect_mapping = false;
	failed_read_abort_on_forget = failed_read_other_pin_deferred = false;
	failed_read_lock_drift = 0;
	transition_owned_io = transition_io_wakes = 0;
	fixture_calls = fixture_io_aborts = fixture_releases = fixture_nacks = 0;
	fixture_native_io = fixture_cancel = fixture_quiet = true;
	fixture_shutdown = fixture_cancel_wait = fixture_release_error = false;
	fixture_shutdown_checked = false;
	c->read.buf = &c->buf;
	UT_ASSERT(StartBufferIO(&c->buf, true));
	UT_ASSERT_EQ(transition_owned_io, 1);
}

static void
h28_end(H28Read *c)
{
	fixture_native_io = false;
	cluster_enabled = c->saved_enabled;
	cluster_shared_config = c->saved_shared;
	ClusterPcmOwnArray = c->saved;
}

static void
h28_cancel(H28Read *c, bool cleanup)
{
	volatile bool caught = false;
	PG_TRY(); { cluster_bufmgr_pcm_read_source_begin(&c->buf, &c->read); }
	PG_CATCH(); {
		caught = true;
		if (cleanup)
			cluster_bufmgr_pcm_read_source_error(0, PointerGetDatum(&c->read));
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(fixture_calls, 1);
	UT_ASSERT_EQ(c->read.base.generation, 48);
	UT_ASSERT_EQ(c->read.token, 49);
	UT_ASSERT_EQ(fixture_io_aborts, cleanup ? 1 : 0);
	UT_ASSERT_EQ(transition_owned_io, cleanup ? 0 : 1);
	UT_ASSERT_EQ(transition_io_wakes, cleanup ? 1 : 0);
}

static void
h28_stop_refuses(H28Read *c)
{
	ClusterBufferStopObservation observation;
	BufferTag tag;
	const char *reason = NULL;
	int id = -1;
	UT_ASSERT_EQ(cluster_bufmgr_stop_poll(true, true, &tag, &id, &reason, &observation),
		CLUSTER_NORMAL_STOP_INVALID);
	UT_ASSERT_STR_EQ(reason, "BUFMGR_SHAPE_INVALID");
	UT_ASSERT_EQ(id, 0);
	UT_ASSERT(BufferTagsEqual(&tag, &c->buf.tag));
	UT_ASSERT_EQ(observation.state, pg_atomic_read_u32(&c->buf.state));
	UT_ASSERT_EQ(observation.own.generation, cluster_pcm_own_gen_get(0));
	UT_ASSERT_EQ(observation.own.reservation_token, cluster_pcm_own_reservation_token_get(0));
	UT_ASSERT_EQ(observation.delivery, cluster_pcm_own_delivery_attempt_get(0));
}

static void
h28_ready(H28Read *c, uint64 expected_token)
{
	UT_ASSERT_EQ(cluster_bufmgr_stop_poll(true, true, NULL, NULL, NULL, NULL),
		CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_EQ(pg_atomic_read_u32(&c->buf.state) & (BUF_FLAG_MASK | BUF_REFCOUNT_MASK), 0);
	UT_ASSERT_EQ(failed_read_deletes, 1);
	UT_ASSERT_EQ(failed_read_frees, 1);
	UT_ASSERT_EQ(cluster_pcm_own_gen_get(0), 49);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_token_get(0), expected_token);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
	UT_ASSERT_EQ(transition_owned_io, 0);
	UT_ASSERT_EQ(transition_io_wakes, 1);
	UT_ASSERT(!transition_mapping_held && !transition_content_held);
}

UT_TEST(test_h28_cancel_then_original_final_pin_reaches_ready_once)
{
	H28Read c;
	h28_start(&c, 1, 1);
	h28_cancel(&c, true);
	UT_ASSERT(!c.read.pending);
	UT_ASSERT(pg_atomic_read_u32(&c.buf.state) & BM_IO_ERROR);
	h28_stop_refuses(&c);
	for (int i = 0; i < 3; i++)
		cluster_bufmgr_pcm_read_source_error(0, PointerGetDatum(&c.read));
	UT_ASSERT_EQ(fixture_io_aborts, 1);
	abort_read_image_unpin(&c.buf);
	UT_ASSERT_EQ(abort_read_image_forgets, 1);
	UT_ASSERT_EQ(abort_read_image_freed, 1);
	h28_ready(&c, 49);
	cluster_bufmgr_pcm_read_source_error(0, PointerGetDatum(&c.read));
	h28_ready(&c, 49);
	h28_end(&c);
}

UT_TEST(test_h28_cancel_keeps_shared_and_private_nonfinal_pins)
{
	for (int private_pins = 0; private_pins < 2; private_pins++) {
		H28Read c;
		BufferTag tag;
		h28_start(&c, private_pins ? 1 : 2, private_pins ? 2 : 1);
		h28_cancel(&c, true);
		tag = c.buf.tag;
		failed_read_expect_mapping = !private_pins;
		abort_read_image_unpin(&c.buf);
		UT_ASSERT_EQ(failed_read_deletes + failed_read_frees, 0);
		UT_ASSERT_EQ(BUF_STATE_GET_REFCOUNT(pg_atomic_read_u32(&c.buf.state)), 1);
		UT_ASSERT(BufferTagsEqual(&tag, &c.buf.tag));
		UT_ASSERT(pg_atomic_read_u32(&c.buf.state) & BM_IO_ERROR);
		h28_stop_refuses(&c);
		/* Select the other process's private pin; shared count is untouched. */
		if (!private_pins) abort_read_image_pin.refcount = 1;
		failed_read_expect_mapping = true;
		abort_read_image_unpin(&c.buf);
		UT_ASSERT_EQ(abort_read_image_forgets, 2);
		UT_ASSERT_EQ(abort_read_image_freed, private_pins ? 1 : 2);
		UT_ASSERT_EQ(failed_read_lock_acquires, private_pins ? 1 : 2);
		h28_ready(&c, 49);
		h28_end(&c);
	}
}

UT_TEST(test_h28_cancel_preserves_unretired_responsibilities)
{
	for (int scenario = 0; scenario < 12; scenario++) {
		H28Read c;
		ClusterPcmOwnEntry before;
		BufferTag tag;
		uint32 state;
		h28_start(&c, 1, 1);
		h28_cancel(&c, true);
		state = pg_atomic_read_u32(&c.buf.state);
		/* Controlled competing-actor inputs. No claimed retirement below. */
		switch (scenario) {
			case 0: state |= BM_DIRTY; break;
			case 1: state |= BM_JUST_DIRTIED; break;
			case 2: state |= BM_CHECKPOINT_NEEDED; break;
			case 3: state |= BM_IO_IN_PROGRESS; break;
			case 4: state |= BM_PIN_COUNT_WAITER; break;
			case 5: pg_atomic_write_u32(&c.entry.flags, PCM_OWN_FLAG_GRANT_PENDING); break;
			case 6: pg_atomic_write_u32(&c.entry.flags, PCM_OWN_FLAG_REVOKING); break;
			case 7: pg_atomic_write_u64(&c.entry.writer_activation_token, 49); break;
			case 8: pg_atomic_write_u64(&c.entry.resource_x_activation_generation, 48); break;
			case 9: pg_atomic_write_u64(&c.entry.delivery_attempt, 17); break;
			case 10: pg_atomic_write_u64(&c.entry.generation, UINT64_MAX - 1); break;
			case 11: pg_atomic_write_u64(&c.entry.reservation_token, UINT64_MAX); break;
		}
		pg_atomic_write_u32(&c.buf.state, state);
		tag = c.buf.tag;
		memcpy(&before, &c.entry, sizeof(before));
		abort_read_image_unpin(&c.buf);
		UT_ASSERT_EQ(failed_read_deletes + failed_read_frees, 0);
		UT_ASSERT_EQ(pg_atomic_read_u32(&c.buf.state), state - BUF_REFCOUNT_ONE);
		UT_ASSERT(BufferTagsEqual(&tag, &c.buf.tag));
		UT_ASSERT_EQ(memcmp(&before, &c.entry, sizeof(before)), 0);
		h28_stop_refuses(&c);
		printf("# retained obligation=%d state=%u generation=%llu token=%llu\n",
			scenario, pg_atomic_read_u32(&c.buf.state),
			(unsigned long long)cluster_pcm_own_gen_get(0),
			(unsigned long long)cluster_pcm_own_reservation_token_get(0));
		h28_end(&c); /* The captured obligation is deliberately not made READY. */
	}
}

UT_TEST(test_h28_mapping_lock_error_keeps_original_pin_for_retry)
{
	H28Read c;
	uint32 state;
	volatile bool caught = false;
	h28_start(&c, 1, 1);
	h28_cancel(&c, true);
	state = pg_atomic_read_u32(&c.buf.state);
	failed_read_lock_error = true;
	PG_TRY(); { abort_read_image_unpin(&c.buf); }
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(abort_read_image_forgets + abort_read_image_freed, 0);
	UT_ASSERT_EQ(abort_read_image_pin.refcount, 1);
	UT_ASSERT_EQ(pg_atomic_read_u32(&c.buf.state), state);
	UT_ASSERT(!transition_mapping_held);
	h28_stop_refuses(&c);
	abort_read_image_unpin(&c.buf);
	h28_ready(&c, 49);
	h28_end(&c);
}

UT_TEST(test_h28_mapping_recheck_retains_drifted_cancelled_frame)
{
	for (int drift = 1; drift <= 4; drift++) {
		H28Read c;
		h28_start(&c, 1, 1);
		h28_cancel(&c, true);
		failed_read_lock_drift = drift;
		abort_read_image_unpin(&c.buf);
		UT_ASSERT_EQ(failed_read_lock_acquires, 1);
		UT_ASSERT_EQ(failed_read_deletes + failed_read_frees, 0);
		UT_ASSERT(pg_atomic_read_u32(&c.buf.state) & BM_IO_ERROR);
		h28_stop_refuses(&c);
		UT_ASSERT(!transition_mapping_held);
		h28_end(&c);
	}
}

UT_TEST(test_h28_late_error_hook_cannot_abort_successor_generation)
{
	H28Read c;
	ClusterPcmOwnEntry before;
	uint64 generation, token;
	uint32 state, locked;
	h28_start(&c, 1, 1);
	h28_cancel(&c, false);
	UT_ASSERT(c.read.pending);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), 0);
	/* Real sidecar owners publish a different generation and reservation.
	 * This is an ABA guard input, not a complete native successor install. */
	locked = transition_lock_header(&c.buf);
	UT_ASSERT(cluster_pcm_own_gen_bump_checked(0, &generation));
	UT_ASSERT_EQ(cluster_pcm_own_reservation_begin_exact(0, generation,
		PCM_OWN_FLAG_GRANT_PENDING, &token), CLUSTER_PCM_OWN_OK);
	UnlockBufHdr(&c.buf, locked);
	state = pg_atomic_read_u32(&c.buf.state);
	memcpy(&before, &c.entry, sizeof(before));
	for (int i = 0; i < 3; i++)
		cluster_bufmgr_pcm_read_source_error(0, PointerGetDatum(&c.read));
	UT_ASSERT_EQ(memcmp(&before, &c.entry, sizeof(before)), 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&c.buf.state), state);
	UT_ASSERT_EQ(fixture_io_aborts, 0);
	UT_ASSERT_EQ(transition_owned_io, 1);
	UT_ASSERT_EQ(transition_io_wakes, 0);
	UT_ASSERT_EQ(generation, 49);
	UT_ASSERT_EQ(token, 50);
	UT_ASSERT(c.read.pending);
	abort_read_image_unpin(&c.buf);
	UT_ASSERT_EQ(failed_read_deletes + failed_read_frees, 0);
	UT_ASSERT_NE(cluster_bufmgr_stop_poll(true, true, NULL, NULL, NULL, NULL),
		CLUSTER_NORMAL_STOP_READY);
	h28_end(&c);
}

UT_TEST(test_h28_real_io_abort_during_final_pin_forget_reaches_ready)
{
	H28Read c;
	h28_start(&c, 2, 1);
	UT_ASSERT_EQ(cluster_bufmgr_stop_poll(false, false, NULL, NULL, NULL, NULL),
		CLUSTER_NORMAL_STOP_PENDING);
	failed_read_abort_on_forget = true;
	abort_read_image_unpin(&c.buf);
	if (failed_read_other_pin_deferred) {
		UT_ASSERT_EQ(BUF_STATE_GET_REFCOUNT(pg_atomic_read_u32(&c.buf.state)), 1);
		abort_read_image_pin.refcount = 1;
		abort_read_image_unpin(&c.buf);
	}
	UT_ASSERT_EQ(abort_read_image_forgets, 2);
	UT_ASSERT_EQ(abort_read_image_freed, 2);
	UT_ASSERT_EQ(failed_read_lock_acquires, 2);
	/* This interleaving began I/O only; it never reserved a GCS source. */
	h28_ready(&c, 48);
	h28_end(&c);
}
#endif

int
main(void)
{
#ifdef K_P130_EXPECT_ASSERT_OFF
#ifdef USE_ASSERT_CHECKING
#error H28 expected an actual configure with assertions disabled
#endif
#else
#ifndef USE_ASSERT_CHECKING
#error H28 expected an actual configure with assertions enabled
#endif
#endif
	UT_PLAN(7);
#ifdef K_P130_H28_VISIBILITY
	UT_RUN(test_h28_owner_callback_errors_preserve_outer_and_successor);
	UT_RUN(test_h28_terminal_callback_errors_preserve_outer_and_successor);
	UT_RUN(test_h28_mx_data_callback_errors_preserve_outer_and_successor);
	UT_RUN(test_h28_borrowed_callback_errors_leave_caller_token_untouched);
	UT_RUN(test_h28_borrowed_guard_cleanup_is_idempotent);
	UT_RUN(test_h28_exit_cleanup_retires_only_local_debt_once);
	UT_RUN(test_h28_locator_error_precedes_owned_admission);
#else
	UT_RUN(test_h28_cancel_then_original_final_pin_reaches_ready_once);
	UT_RUN(test_h28_cancel_keeps_shared_and_private_nonfinal_pins);
	UT_RUN(test_h28_cancel_preserves_unretired_responsibilities);
	UT_RUN(test_h28_mapping_lock_error_keeps_original_pin_for_retry);
	UT_RUN(test_h28_mapping_recheck_retains_drifted_cancelled_frame);
	UT_RUN(test_h28_late_error_hook_cannot_abort_successor_generation);
	UT_RUN(test_h28_real_io_abort_during_final_pin_forget_reaches_ready);
#endif
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
