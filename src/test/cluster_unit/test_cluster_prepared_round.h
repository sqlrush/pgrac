/* Original LMON bit22 round with explicit ROOT/CF completion leaves.
 * The actual ROOT/CF producer has separate joined owner/registry tests.
 * Author: SqlRush <sqlrush@gmail.com> */
static bool ut_root_job_pending, ut_root_job_ack;
static unsigned ut_root_job_polls, ut_root_job_deliveries;
static ClusterControlRootPreparedWorkV1 *ut_root_job_address;
static bool ut_root_job_observation_gap, ut_root_job_poll_invalid, ut_root_job_throw;
static bool ut_root_job_throw_before_bind;
static ClusterControlRootResult ut_root_job_operation_result;
static bool ut_root_job_identity_current;
static void (*ut_root_job_finish_hook)(void);
static unsigned ut_root_job_return_drift;

static void
ut_prepared_job_reset(void)
{
	ut_root_job_pending = ut_root_job_ack = false;
	ut_root_job_polls = ut_root_job_deliveries = 0;
	ut_root_job_address = NULL;
	ut_root_job_observation_gap = ut_root_job_poll_invalid = ut_root_job_throw = false;
	ut_root_job_throw_before_bind = false;
	ut_root_image_calls = 0;
	ut_root_job_operation_result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	ut_root_job_identity_current = true;
	ut_root_job_finish_hook = NULL;
	memset(&semantic_activation_root_publication, 0, sizeof(semantic_activation_root_publication));
}

static ClusterControlRootResult
ut_root_job_start(bool activate, const ClusterControlRootMigrationImage *image,
				  const ClusterControlRootFileToken *expected, const uint8 *sha,
				  const ClusterControlRootMigrationRoundV1 *round,
				  ClusterControlRootPreparedWorkV1 *work, ClusterControlRootFileToken *out)
{
	ClusterControlRootResult result;
	ClusterControlRootResult saved = activate ? ut_activate_result : ut_create_result;

	memset(out, 0, sizeof(*out));
	UT_ASSERT(work != NULL && work->release.job_generation != 0);
	UT_ASSERT_EQ(work->phase, CONTROL_ROOT_PREPARED_WORK_EMPTY);
	if (ut_root_job_throw_before_bind)
		siglongjmp(*PG_exception_stack, 1); /* Explicit prebinding ERROR leaf. */
	if (ut_root_job_pending) {
		if (activate)
			ut_activate_result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
		else
			ut_create_result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	}
	result = activate ? cluster_control_root_activate_prepared(expected, sha, round, out)
					  : cluster_control_root_create_prepared(image, round, out);
	if (activate)
		ut_activate_result = saved;
	else
		ut_create_result = saved;
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result; /* Explicit prepublication refusal leaf. */
	work->address = work;
	work->generation = work->release.job_generation;
	work->round = *round;
	work->result = ut_root_job_operation_result;
	work->token.file_txn_seq = activate ? 2 : 1;
	work->token.activation_state = activate ? CLUSTER_CONTROL_ROOT_ACTIVATION_ACTIVE
											: CLUSTER_CONTROL_ROOT_ACTIVATION_PREPARED;
	ut_root_job_address = work;
	if (ut_root_job_pending) {
		work->phase = CONTROL_ROOT_PREPARED_WORK_RETIRING;
		work->release.state = CF_JOB_RECEIPT_RETIRING;
		memset(out, 0, sizeof(*out));
		if (ut_root_job_throw)
			siglongjmp(*PG_exception_stack, 1); /* Explicit post-publication ERROR leaf. */
		return CLUSTER_CONTROL_ROOT_RELEASE_UNCERTAIN;
	}
	*out = work->token;
	memset(&work->token, 0, sizeof(work->token));
	work->phase = CONTROL_ROOT_PREPARED_WORK_CONSUMED;
	work->release.state = CF_JOB_RECEIPT_CONFIRMED;
	++ut_root_job_deliveries;
	if (ut_root_job_finish_hook != NULL)
		ut_root_job_finish_hook();
	return work->result;
}

bool
cluster_control_root_prepared_job_identity_current_v1(const ClusterControlRootPreparedWorkV1 *work)
{
	/* The actual producer checks its saved proc/context/GRD identity in the
	 * separate joined CF tests. This is that explicit identity leaf. */
	return ut_root_job_identity_current && work == ut_root_job_address && !work->rejected;
}

ClusterControlRootResult
cluster_control_root_create_prepared_job_v1(const ClusterControlRootMigrationImage *image,
											const ClusterControlRootMigrationRoundV1 *round,
											ClusterControlRootPreparedWorkV1 *work,
											ClusterControlRootFileToken *out)
{
	return ut_root_job_start(false, image, NULL, NULL, round, work, out);
}

ClusterControlRootResult
cluster_control_root_activate_prepared_job_v1(const ClusterControlRootFileToken *expected,
											  const uint8 sha[32],
											  const ClusterControlRootMigrationRoundV1 *round,
											  ClusterControlRootPreparedWorkV1 *work,
											  ClusterControlRootFileToken *out)
{
	return ut_root_job_start(true, NULL, expected, sha, round, work, out);
}

ClusterControlRootResult
cluster_control_root_prepared_job_poll_v1(ClusterControlRootPreparedWorkV1 *work,
										  ClusterControlRootFileToken *out)
{
	++ut_root_job_polls;
	memset(out, 0, sizeof(*out));
	UT_ASSERT(work == ut_root_job_address && work->address == work);
	UT_ASSERT_EQ(work->phase, CONTROL_ROOT_PREPARED_WORK_RETIRING);
	if (ut_root_job_poll_invalid)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	if (!ut_root_job_ack)
		return CLUSTER_CONTROL_ROOT_RELEASE_UNCERTAIN;
	work->release.state = CF_JOB_RECEIPT_CONFIRMED;
	work->phase = CONTROL_ROOT_PREPARED_WORK_CONSUMED;
	if (work->rejected)
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	*out = work->token;
	memset(&work->token, 0, sizeof(work->token));
	++ut_root_job_deliveries;
	if (ut_root_job_observation_gap)
		pg_atomic_write_u64(&SemanticActivationAckTable->publication_seq,
							pg_atomic_read_u64(&SemanticActivationAckTable->publication_seq)
								| UINT64_C(1));
	return work->result;
}

static void
ut_prepared_round_start(bool activate)
{
	ut_prepared_round_cf_setup(activate);
	ut_root_job_pending = true;
	(void)semantic_activation_ack_lmon_bit22_advance();
	UT_ASSERT_EQ(ut_create_calls + ut_activate_calls, 1);
	UT_ASSERT_EQ(semantic_activation_root_publication.work.phase,
				 CONTROL_ROOT_PREPARED_WORK_RETIRING);
}

UT_TEST(test_prepared_round_published_result_waits_for_original_release_without_replay)
{
	for (int activate = 0; activate < 2; activate++) {
		ClusterSemanticActivationAckTableV1 before;
		uint64 seq;

		ut_prepared_round_cf_setup(activate);
		before = *SemanticActivationAckTable;
		seq = pg_atomic_read_u64(&SemanticActivationShmem->record_cas_request_seq);
		ut_root_job_pending = true;
		ut_create_result = ut_activate_result = CLUSTER_CONTROL_ROOT_RELEASE_UNCERTAIN;
		for (unsigned tick = 0; tick < 4; tick++) {
			(void)semantic_activation_ack_lmon_bit22_advance();
			UT_ASSERT_EQ(ut_create_calls + ut_activate_calls, 1);
			UT_ASSERT_EQ(ut_root_job_deliveries, 0);
			UT_ASSERT_EQ(memcmp(&before, SemanticActivationAckTable, sizeof(before)), 0);
			UT_ASSERT_EQ(pg_atomic_read_u64(&SemanticActivationShmem->record_cas_request_seq), seq);
			UT_ASSERT(!cluster_r4_bit22_cutover_active());
		}
		ut_root_job_ack = true;
		UT_ASSERT(semantic_activation_ack_lmon_bit22_advance());
		UT_ASSERT_EQ(ut_create_calls + ut_activate_calls, 1);
		UT_ASSERT_EQ(ut_root_job_deliveries, 1);
		UT_ASSERT_EQ(SemanticActivationAckTable->stage,
					 activate ? CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_COMMIT_APPLIED
							  : CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED);
		UT_ASSERT(!cluster_r4_bit22_cutover_active());
	}
	test_gate_reset();
}

UT_TEST(test_prepared_round_completed_result_survives_table_observation_gap)
{
	for (int activate = 0; activate < 2; activate++) {
		uint64 seq;

		ut_prepared_round_start(activate);
		seq = pg_atomic_read_u64(&SemanticActivationAckTable->publication_seq);
		ut_root_job_ack = ut_root_job_observation_gap = true;
		UT_ASSERT(!semantic_activation_ack_lmon_bit22_advance());
		UT_ASSERT_EQ(ut_root_job_deliveries, 1);
		UT_ASSERT(semantic_activation_root_publication.completed);
		test_membership_snapshot_valid = false; /* An observation gap is not drift. */
		for (int tick = 0; tick < 3; tick++) {
			UT_ASSERT(!semantic_activation_ack_lmon_bit22_advance());
			UT_ASSERT_EQ(ut_create_calls + ut_activate_calls, 1);
			UT_ASSERT_EQ(ut_root_image_calls, activate ? 0 : 1);
			UT_ASSERT_EQ(ut_root_job_deliveries, 1);
			UT_ASSERT(semantic_activation_root_publication.active);
		}
		test_membership_snapshot_valid = true;
		ut_root_job_observation_gap = false;
		pg_atomic_write_u64(&SemanticActivationAckTable->publication_seq, seq + 2);
		UT_ASSERT(semantic_activation_ack_lmon_bit22_advance());
		UT_ASSERT_EQ(ut_create_calls + ut_activate_calls, 1);
		UT_ASSERT_EQ(ut_root_image_calls, activate ? 0 : 1);
		UT_ASSERT_EQ(ut_root_job_deliveries, 1);
		UT_ASSERT_EQ(SemanticActivationAckTable->stage,
					 activate ? CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_COMMIT_APPLIED
							  : CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED);
		UT_ASSERT(!semantic_activation_root_publication.active);
		(void)semantic_activation_ack_lmon_bit22_advance();
		UT_ASSERT_EQ(ut_create_calls + ut_activate_calls, 1);
	}
	test_gate_reset();
}

UT_TEST(test_prepared_round_identity_loss_retires_original_debt_without_delivery)
{
	for (int activate = 0; activate < 2; activate++) {
		for (int drift = 0; drift < 8; drift++) {
			ClusterControlRootPreparedWorkV1 *address;
			uint64 generation;

			ut_prepared_round_start(activate);
			address = ut_root_job_address;
			generation = address->generation;
			switch (drift) {
			case 0:
				test_current_epoch++;
				break;
			case 1:
				test_membership_snapshot_lo ^= UINT64_C(2);
				break;
			case 2:
				test_membership_snapshot_epoch++;
				break;
			case 3:
				cluster_node_id = 1;
				break;
			case 4:
				test_qvotec_self_incarnation++;
				break;
			case 5:
				test_remote_admitted_incarnations[1]++;
				break;
			case 6:
				test_observed_slot_valid[1] = true;
				test_observed_slot_incarnation[1] = UINT64_C(0x101);
				test_observed_slot_generation[1] = 1;
				test_observed_slot_epoch[1] = test_current_epoch + 1;
				break;
			case 7:
				SemanticActivationAckTable->expected[1].capability_generation++;
				break;
			}
			(void)semantic_activation_ack_lmon_bit22_advance();
			UT_ASSERT_EQ(ut_create_calls + ut_activate_calls, 1);
			UT_ASSERT_EQ(ut_root_job_deliveries, 0);
			UT_ASSERT(address == ut_root_job_address && address->generation == generation);
			UT_ASSERT(address->rejected);
			UT_ASSERT_EQ(SemanticActivationAckTable->expected_members_lo, 0);
			UT_ASSERT(!cluster_r4_bit22_cutover_active());
			ut_root_job_ack = true;
			(void)semantic_activation_ack_lmon_bit22_advance();
			UT_ASSERT_EQ(ut_root_job_deliveries, 0);
			UT_ASSERT(!semantic_activation_root_publication.active);
		}
	}
	test_gate_reset();
}

UT_TEST(test_prepared_round_activate_seam_identity_loss_keeps_original_cleanup)
{
	for (int drift = 0; drift < 6; drift++) {
		ut_prepared_round_start(true);
		switch (drift) {
		case 0:
			pg_atomic_write_u32(&SemanticActivationBit22Seam->valid, 0);
			break;
		case 1:
			SemanticActivationBit22Seam->transition_epoch++;
			break;
		case 2:
			SemanticActivationBit22Seam->prepare_generation++;
			break;
		case 3:
			SemanticActivationBit22Seam->file_token.file_txn_seq++;
			break;
		case 4:
			SemanticActivationBit22Seam->round_sha[0] ^= 1;
			break;
		case 5:
			SemanticActivationBit22Seam->round.coordinator_incarnation++;
			break;
		}
		(void)semantic_activation_ack_lmon_bit22_advance();
		UT_ASSERT_EQ(ut_activate_calls, 1);
		UT_ASSERT(ut_root_job_address->rejected);
		UT_ASSERT_EQ(SemanticActivationAckTable->expected_members_lo, 0);
		UT_ASSERT(semantic_activation_root_publication.active);
		ut_root_job_ack = true;
		(void)semantic_activation_ack_lmon_bit22_advance();
		UT_ASSERT_EQ(ut_root_job_deliveries, 0);
		UT_ASSERT(!semantic_activation_root_publication.active);
	}
	test_gate_reset();
}

UT_TEST(test_prepared_round_failed_published_result_waits_then_refuses)
{
	for (int activate = 0; activate < 2; activate++) {
		ut_prepared_round_start(activate);
		ut_root_job_address->result = CLUSTER_CONTROL_ROOT_IO_ERROR;
		(void)semantic_activation_ack_lmon_bit22_advance();
		UT_ASSERT(semantic_activation_root_publication.active);
		UT_ASSERT_EQ(ut_root_job_address->phase, CONTROL_ROOT_PREPARED_WORK_RETIRING);
		ut_root_job_ack = true;
		(void)semantic_activation_ack_lmon_bit22_advance();
		UT_ASSERT_EQ(ut_create_calls + ut_activate_calls, 1);
		UT_ASSERT(!semantic_activation_root_publication.active);
		UT_ASSERT_EQ(SemanticActivationAckTable->expected_members_lo, 0);
		UT_ASSERT(!cluster_r4_bit22_cutover_active());
	}
	test_gate_reset();
}

UT_TEST(test_prepared_round_missing_receipt_proof_never_replays_or_completes)
{
	for (int activate = 0; activate < 2; activate++) {
		ut_prepared_round_start(activate);
		ut_root_job_ack = ut_root_job_poll_invalid = true;
		for (int tick = 0; tick < 3; tick++) {
			(void)semantic_activation_ack_lmon_bit22_advance();
			UT_ASSERT_EQ(ut_create_calls + ut_activate_calls, 1);
			UT_ASSERT_EQ(ut_root_job_deliveries, 0);
			UT_ASSERT(semantic_activation_root_publication.active);
			UT_ASSERT_EQ(ut_root_job_address->phase, CONTROL_ROOT_PREPARED_WORK_RETIRING);
			UT_ASSERT(ut_root_job_address->rejected);
			UT_ASSERT_EQ(SemanticActivationAckTable->expected_members_lo, 0);
			UT_ASSERT(!cluster_r4_bit22_cutover_active());
		}
		ut_root_job_poll_invalid = false;
		(void)semantic_activation_ack_lmon_bit22_advance();
		UT_ASSERT_EQ(ut_root_job_deliveries, 0);
		UT_ASSERT(!semantic_activation_root_publication.active);
	}
	test_gate_reset();
}

UT_TEST(test_prepared_round_error_keeps_work_at_original_address_for_next_tick)
{
	for (int activate = 0; activate < 2; activate++) {
		volatile bool caught = false;

		ut_prepared_round_cf_setup(activate);
		ut_root_job_pending = ut_root_job_throw = true;
		PG_TRY();
		{
			(void)semantic_activation_ack_lmon_bit22_advance();
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(caught);
		UT_ASSERT(semantic_activation_root_publication.active);
		UT_ASSERT(ut_root_job_address == &semantic_activation_root_publication.work);
		UT_ASSERT_EQ(ut_root_job_address->phase, CONTROL_ROOT_PREPARED_WORK_RETIRING);
		ut_root_job_throw = false;
		ut_root_job_ack = true;
		(void)semantic_activation_ack_lmon_bit22_advance();
		UT_ASSERT_EQ(ut_create_calls + ut_activate_calls, 1);
		UT_ASSERT_EQ(ut_root_job_deliveries, 1);
		UT_ASSERT(!semantic_activation_root_publication.active);
	}
	test_gate_reset();
}

UT_TEST(test_prepared_round_cleanup_precedes_original_lmon_early_return)
{
	for (int activate = 0; activate < 2; activate++) {
		unsigned polls;

		ut_prepared_round_start(activate);
		/* Explicit original stop-read ownership blocks the later advance,
		 * not cleanup of the existing publication. No new ROOT acquisition. */
		semantic_activation_lmon_stop_read_seq = 91;
		polls = ut_root_job_polls;
		cluster_semantic_activation_lmon_tick();
		UT_ASSERT(ut_root_job_polls > polls);
		UT_ASSERT_EQ(ut_root_job_deliveries, 0);
		ut_root_job_ack = true;
		cluster_semantic_activation_lmon_tick();
		UT_ASSERT_EQ(ut_root_job_deliveries, 1);
		UT_ASSERT(semantic_activation_root_publication.completed);
		UT_ASSERT_EQ(ut_create_calls + ut_activate_calls, 1);
		semantic_activation_lmon_stop_read_seq
			= 0; /* The stop-read leaf consumes its own request. */
		(void)semantic_activation_ack_lmon_bit22_advance();
		UT_ASSERT(!semantic_activation_root_publication.active);
	}
	test_gate_reset();
}

UT_TEST(test_prepared_round_stop_census_keeps_debt_until_exact_cleanup)
{
	for (int activate = 0; activate < 2; activate++) {
		const char *domain, *reason;
		uint64 generation, key;
		unsigned polls;

		ut_prepared_round_start(activate);
		IsUnderPostmaster = true;
		MyAuxProcType = LmonProcess;
		/* Quiescent inputs for the other original census owners. This is
		 * the ROOT owner's census boundary, not a full stop/seal proof.
		 * QVOTEC and origin retirement have their own existing tests. */
		semantic_activation_lmon_bit22_prepare_cas_seq = 0;
		semantic_activation_lmon_commit_cas_seq = 0;
		memset(&semantic_activation_ack_local_request_origin, 0,
			   sizeof(semantic_activation_ack_local_request_origin));
		generation = ut_root_job_address->generation;
		polls = ut_root_job_polls;
		for (int sample = 0; sample < 3; sample++) {
			UT_ASSERT_EQ(cluster_semantic_normal_stop_poll(&domain, &key, &reason),
						 CLUSTER_NORMAL_STOP_PENDING);
			UT_ASSERT_STR_EQ(domain, "R4_ROOT_PUBLICATION");
			UT_ASSERT_STR_EQ(reason, "SEMANTIC_STOP_ROOT_PUBLICATION_PENDING");
			UT_ASSERT_EQ(key, generation);
			UT_ASSERT_EQ(ut_root_job_polls, polls); /* Census is read-only. */
		}
		/* The existing epoch contradiction terminates delivery; the original
		 * receipt must still confirm cleanup before its census can be READY. */
		test_current_epoch++;
		semantic_activation_root_publication_poll();
		UT_ASSERT_EQ(cluster_semantic_normal_stop_poll(NULL, NULL, NULL),
					 CLUSTER_NORMAL_STOP_PENDING);
		ut_root_job_ack = true;
		semantic_activation_root_publication_poll();
		UT_ASSERT_EQ(ut_root_job_deliveries, 0);
		UT_ASSERT(!semantic_activation_root_publication.active);
		UT_ASSERT_EQ(cluster_semantic_normal_stop_poll(NULL, NULL, NULL),
					 CLUSTER_NORMAL_STOP_READY);
	}
	test_gate_reset();
}

UT_TEST(test_prepared_round_confirmed_operation_failure_terminates_original_round)
{
	static const ClusterControlRootResult errors[]
		= { CLUSTER_CONTROL_ROOT_IO_ERROR, CLUSTER_CONTROL_ROOT_STALE_TOKEN,
			CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH };

	for (int activate = 0; activate < 2; activate++) {
		for (unsigned error = 0; error < lengthof(errors); error++) {
			ut_prepared_round_cf_setup(activate);
			ut_create_result = ut_activate_result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
			ut_root_job_operation_result = errors[error];
			(void)semantic_activation_ack_lmon_bit22_advance();
			(void)semantic_activation_ack_lmon_bit22_advance();
			UT_ASSERT_EQ(ut_create_calls + ut_activate_calls, 1);
			UT_ASSERT(!semantic_activation_root_publication.active);
			UT_ASSERT_EQ(SemanticActivationAckTable->expected_members_lo, 0);
			UT_ASSERT(!cluster_r4_bit22_cutover_active());
		}
	}
	test_gate_reset();
}

UT_TEST(test_prepared_round_error_before_binding_keeps_original_refusal_semantics)
{
	for (int activate = 0; activate < 2; activate++) {
		ClusterSemanticActivationAckTableV1 before;
		volatile bool caught = false;

		ut_prepared_round_cf_setup(activate);
		before = *SemanticActivationAckTable;
		ut_root_job_throw_before_bind = true;
		PG_TRY();
		{
			(void)semantic_activation_ack_lmon_bit22_advance();
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(caught);
		UT_ASSERT_EQ(ut_create_calls + ut_activate_calls, 0);
		semantic_activation_root_publication_poll();
		UT_ASSERT(!semantic_activation_root_publication.active);
		UT_ASSERT_EQ(memcmp(&before, SemanticActivationAckTable, sizeof(before)), 0);
		ut_root_job_throw_before_bind = false;
		ut_create_result = ut_activate_result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
		UT_ASSERT(semantic_activation_ack_lmon_bit22_advance());
		UT_ASSERT_EQ(ut_create_calls + ut_activate_calls, 1);
	}
	test_gate_reset();
}

static void
ut_prepared_round_success_drift(void)
{
	switch (ut_root_job_return_drift) {
	case 0:
		test_membership_snapshot_lo ^= UINT64_C(2);
		break;
	case 1:
		if (semantic_activation_root_publication.activate)
			SemanticActivationBit22Seam->file_token.file_txn_seq++;
		else
			SemanticActivationAckTable->expected[1].capability_generation++;
		break;
	case 2:
		ut_root_job_identity_current = false;
		break;
	case 3:
		test_membership_snapshot_valid = false;
		break;
	case 4:
		pg_atomic_write_u64(&SemanticActivationAckTable->publication_seq,
							pg_atomic_read_u64(&SemanticActivationAckTable->publication_seq)
								| UINT64_C(1));
		break;
	}
	ut_root_job_finish_hook = NULL;
}

UT_TEST(test_prepared_round_synchronous_success_rechecks_round_before_stage)
{
	for (int activate = 0; activate < 2; activate++) {
		for (unsigned drift = 0; drift < 5; drift++) {
			uint64 seq;
			uint32 stage;

			ut_prepared_round_cf_setup(activate);
			stage = SemanticActivationAckTable->stage;
			seq = pg_atomic_read_u64(&SemanticActivationAckTable->publication_seq);
			ut_create_result = ut_activate_result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
			ut_root_job_return_drift = drift;
			ut_root_job_finish_hook = ut_prepared_round_success_drift;
			(void)semantic_activation_ack_lmon_bit22_advance();
			UT_ASSERT_EQ(ut_create_calls + ut_activate_calls, 1);
			UT_ASSERT_EQ(SemanticActivationAckTable->stage, stage);
			UT_ASSERT(!cluster_r4_bit22_cutover_active());
			if (drift < 3) {
				UT_ASSERT_EQ(SemanticActivationAckTable->expected_members_lo, 0);
				UT_ASSERT(!semantic_activation_root_publication.active);
			} else {
				UT_ASSERT(semantic_activation_root_publication.completed);
				UT_ASSERT(semantic_activation_root_publication.active);
				test_membership_snapshot_valid = true;
				pg_atomic_write_u64(&SemanticActivationAckTable->publication_seq, seq + 2);
				UT_ASSERT(semantic_activation_ack_lmon_bit22_advance());
				UT_ASSERT_EQ(ut_create_calls + ut_activate_calls, 1);
				UT_ASSERT(!semantic_activation_root_publication.active);
			}
		}
	}
	test_gate_reset();
}

UT_TEST(test_prepared_round_cached_success_rejects_later_producer_identity_loss)
{
	for (int activate = 0; activate < 2; activate++) {
		uint64 seq;

		ut_prepared_round_start(activate);
		seq = pg_atomic_read_u64(&SemanticActivationAckTable->publication_seq);
		ut_root_job_ack = ut_root_job_observation_gap = true;
		(void)semantic_activation_ack_lmon_bit22_advance();
		UT_ASSERT(semantic_activation_root_publication.completed);
		ut_root_job_identity_current = false; /* Saved context/GRD binding changed. */
		ut_root_job_observation_gap = false;
		pg_atomic_write_u64(&SemanticActivationAckTable->publication_seq, seq + 2);
		(void)semantic_activation_ack_lmon_bit22_advance();
		UT_ASSERT_EQ(ut_create_calls + ut_activate_calls, 1);
		UT_ASSERT_EQ(SemanticActivationAckTable->expected_members_lo, 0);
		UT_ASSERT(!semantic_activation_root_publication.active);
		UT_ASSERT(!cluster_r4_bit22_cutover_active());
	}
	test_gate_reset();
}

UT_TEST(test_prepared_round_old_cleanup_cannot_invalidate_replacement_round)
{
	for (int activate = 0; activate < 2; activate++) {
		ClusterSemanticActivationAckTableV1 replacement;
		uint64 generation;

		ut_prepared_round_start(activate);
		generation = ut_root_job_address->generation;
		replacement = *SemanticActivationAckTable;
		replacement.round_nonce++;
		UT_ASSERT(semantic_activation_ack_table_publish(&replacement));
		replacement = *SemanticActivationAckTable;
		for (int tick = 0; tick < 3; tick++) {
			(void)semantic_activation_ack_lmon_bit22_advance();
			UT_ASSERT(ut_root_job_address->rejected);
			UT_ASSERT_EQ(ut_root_job_address->generation, generation);
			UT_ASSERT_EQ(memcmp(&replacement, SemanticActivationAckTable, sizeof(replacement)), 0);
			UT_ASSERT_EQ(ut_create_calls + ut_activate_calls, 1);
		}
		ut_root_job_ack = true;
		semantic_activation_root_publication_poll();
		UT_ASSERT(!semantic_activation_root_publication.active);
		UT_ASSERT_EQ(memcmp(&replacement, SemanticActivationAckTable, sizeof(replacement)), 0);
		UT_ASSERT_EQ(ut_root_job_deliveries, 0);
	}
	test_gate_reset();
}
