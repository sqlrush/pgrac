/* Real original authority composition; only lower providers are controlled.
 * These aligned opaque addresses are compared, never dereferenced or passed
 * to real providers. They certify no fence, native base or global cohort.
 * Author: SqlRush <sqlrush@gmail.com> */
static PGAlignedBlock cold_authority_tokens[4];
static unsigned cold_authority_fault;
static bool cold_authority_after_native;

static bool
cold_authority_fault_now(unsigned fault)
{
	return cold_authority_fault == fault
		   && (!cold_authority_after_native || prefix_base_reads >= 2);
}

ClusterRecoverySerialRevalidateResult
test_relmap_authority_serial(ClusterRecoverySerialGuard *guard)
{
	cold_original_checks++;
	if (cold_authority_fault_now(6))
		ereport(ERROR, (errmsg("injected original serial provider ERROR")));
	return guard == &cold_original_serial && guard->held && !guard->release_uncertain
				   && cold_original_fault == 0 && !cold_authority_fault_now(1)
			   ? CLUSTER_RECOVERY_SERIAL_CURRENT
			   : CLUSTER_RECOVERY_SERIAL_FENCE_STALE;
}

ClusterFormationWitnessResult
test_relmap_authority_formation(const ClusterFormationWitnessV1 *formation)
{
	return formation == (const ClusterFormationWitnessV1 *)&cold_authority_tokens[0]
				   && !cold_authority_fault_now(2)
			   ? CLUSTER_FORMATION_WITNESS_READY
			   : CLUSTER_FORMATION_WITNESS_ORIGIN_NOT_EXCLUDED;
}

bool
test_relmap_authority_needs(const PgracExternalFenceNeedSetV1 *needs,
							const ClusterFormationWitnessV1 *formation,
							PgracExternalFenceDenyReason *reason)
{
	UT_ASSERT(reason != NULL);
	return needs == (const PgracExternalFenceNeedSetV1 *)&cold_authority_tokens[1]
		   && formation == (const ClusterFormationWitnessV1 *)&cold_authority_tokens[0]
		   && !cold_authority_fault_now(3);
}

bool
test_relmap_authority_admissions(const PgracExternalFenceAdmissionSetV1 *admissions,
								 const PgracExternalFenceNeedSetV1 *needs,
								 const ClusterFormationWitnessV1 *formation,
								 PgracExternalFenceDenyReason *reason)
{
	UT_ASSERT(reason != NULL);
	return admissions == (const PgracExternalFenceAdmissionSetV1 *)&cold_authority_tokens[2]
		   && needs == (const PgracExternalFenceNeedSetV1 *)&cold_authority_tokens[1]
		   && formation == (const ClusterFormationWitnessV1 *)&cold_authority_tokens[0]
		   && !cold_authority_fault_now(4);
}

ClusterWalPinResult
test_relmap_authority_pin(ClusterWalRetentionPin *pin)
{
	return pin == (ClusterWalRetentionPin *)&cold_authority_tokens[3]
				   && !cold_authority_fault_now(5)
			   ? CLUSTER_WAL_PIN_OK
			   : CLUSTER_WAL_PIN_STALE;
}

static void
cold_original_protection_setup(void)
{
	cold_authority_fault = 0;
	cold_authority_after_native = false;
	cold_original_authority.formation
		= (const ClusterFormationWitnessV1 *)&cold_authority_tokens[0];
	cold_original_authority.fence_need_set
		= (const PgracExternalFenceNeedSetV1 *)&cold_authority_tokens[1];
	cold_original_authority.fence_admission_set
		= (const PgracExternalFenceAdmissionSetV1 *)&cold_authority_tokens[2];
	cold_original_authority.retention_pin = (ClusterWalRetentionPin *)&cold_authority_tokens[3];
	cold_original_serial.formation = cold_original_authority.formation;
	cold_original_serial.fence_need_set = cold_original_authority.fence_need_set;
	cold_original_serial.fence_admission_set = cold_original_authority.fence_admission_set;
}

static void
cold_original_authority_postwrite(void)
{
	if (cold_native_post_fault == 3)
		cold_original_serial.fence_admission_set = NULL;
	if (cold_native_post_fault == 4)
		cold_authority_fault = 6;
}

UT_TEST(test_cold_original_authority_rejects_broken_provenance_before_storage)
{
	cold_native_real_scope = cold_native_real_clog = true;
	for (unsigned sample = 0; sample < 8; sample++) {
		ClusterRelmapRetentionCensus *census = NULL;
		ClusterThreadRecoveryAuthorityResultV1 expected;
		cold_native_setup(false, false, false);
		UT_ASSERT_EQ(test_relmap_scope_authority(&cold_original_authority),
					 CLUSTER_THREAD_AUTHORITY_OK);
		switch (sample) {
		case 0:
			cold_original_authority.formation = NULL;
			expected = CLUSTER_THREAD_AUTHORITY_INVALID;
			break;
		case 1:
			cold_original_serial.root_read_token.file_txn_seq++;
			expected = CLUSTER_THREAD_AUTHORITY_ROOT_STALE;
			break;
		case 2:
			cold_original_serial.fence_admission_set = NULL;
			expected = CLUSTER_THREAD_AUTHORITY_SERIAL_STALE;
			break;
		case 3:
			cold_authority_fault = 1;
			expected = CLUSTER_THREAD_AUTHORITY_SERIAL_STALE;
			break;
		case 4:
			cold_authority_fault = 2;
			expected = CLUSTER_THREAD_AUTHORITY_FENCE_STALE;
			break;
		case 5:
			cold_authority_fault = 3;
			expected = CLUSTER_THREAD_AUTHORITY_FENCE_STALE;
			break;
		case 6:
			cold_authority_fault = 4;
			expected = CLUSTER_THREAD_AUTHORITY_FENCE_STALE;
			break;
		default:
			cold_authority_fault = 5;
			expected = CLUSTER_THREAD_AUTHORITY_PIN_STALE;
			break;
		}
		UT_ASSERT_EQ(test_relmap_scope_authority(&cold_original_authority), expected);
		UT_ASSERT(!cold_native_execute(&census));
		UT_ASSERT(census == NULL && cold_original_plan != NULL && cold_original_serial.held);
		UT_ASSERT_EQ(execution_root_opens + prefix_base_reads + cold_native_syncs
						 + cold_native_invals + cold_native_acks,
					 0);
		cold_native_done(census);
		if (ut_current_failed)
			break;
	}
	cold_native_real_scope = cold_native_real_clog = false;
}

UT_TEST(test_cold_original_authority_postwrite_loss_keeps_original_debt)
{
	cold_native_real_scope = cold_native_real_clog = true;
	for (unsigned sample = 0; sample < 2; sample++) {
		ClusterRelmapRetentionCensus *census = NULL;
		ClusterRelmapV2View view;
		volatile bool accepted = false, threw = false;
		cold_native_setup(sample != 0, false, false);
		cold_native_post_fault = sample == 0 ? 3 : 4;
		test_capture_error_level = true;
		PG_TRY();
		{
			accepted = cold_native_execute(&census);
		}
		PG_CATCH();
		{
			threw = true;
		}
		PG_END_TRY();
		test_capture_error_level = false;
		UT_ASSERT(!accepted && census == NULL);
		UT_ASSERT_EQ(threw, sample != 0);
		UT_ASSERT_EQ(cold_native_syncs, 4);
		UT_ASSERT_EQ(cold_native_invals + cold_native_acks, 0);
		/* Original authority refuses before the post-write native re-read. */
		UT_ASSERT_EQ(prefix_base_reads, 2);
		UT_ASSERT(cold_original_plan != NULL && cold_original_serial.held);
		UT_ASSERT(cold_original_authority.retention_pin
				  == (ClusterWalRetentionPin *)&cold_authority_tokens[3]);
		UT_ASSERT_EQ(cluster_relmap_authority_v2_read(&custody_image.identity, &view),
					 CLUSTER_RELMAP_V2_PAIR_VALID);
		UT_ASSERT(view.pending_complete && !view.has_pending && view.committed_generation == 8);
		cold_native_done(census);
		if (ut_current_failed)
			break;
	}
	cold_native_real_scope = cold_native_real_clog = false;
}

UT_TEST(test_cold_original_authority_late_provider_refusal_or_error_keeps_pending)
{
	cold_native_real_scope = cold_native_real_clog = true;
	for (unsigned fault = 1; fault <= 6; fault++) {
		ClusterRelmapRetentionCensus *census = NULL;
		ClusterRelmapV2View view;
		volatile bool accepted = false, threw = false;
		cold_native_setup(false, fault % 2 == 0, false);
		cold_authority_fault = fault;
		cold_authority_after_native = true;
		test_capture_error_level = true;
		PG_TRY();
		{
			accepted = cold_native_execute(&census);
		}
		PG_CATCH();
		{
			threw = true;
		}
		PG_END_TRY();
		test_capture_error_level = false;
		UT_ASSERT(!accepted && census == NULL);
		UT_ASSERT_EQ(threw, fault == 6);
		UT_ASSERT_EQ(prefix_base_reads, 2);
		UT_ASSERT_EQ(cold_native_syncs + cold_native_invals + cold_native_acks, 0);
		UT_ASSERT(cold_original_plan != NULL && cold_original_serial.held);
		UT_ASSERT_EQ(cluster_relmap_authority_v2_read(&custody_image.identity, &view),
					 CLUSTER_RELMAP_V2_PAIR_VALID);
		UT_ASSERT(view.pending_complete && view.has_pending);
		cold_native_done(census);
		UT_ASSERT_EQ(cold_native_clog_last.calls, 2);
		UT_ASSERT(cold_native_clog_last.file_syncs >= 1);
		if (ut_current_failed)
			break;
	}
	cold_native_real_scope = cold_native_real_clog = false;
}
