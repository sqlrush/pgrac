/* Shared A/F fixture contract: PI-only builds reject native recovery input;
 * a build with the cold completion API may retain it, but cannot lend its
 * execution owner to PI or to an ordinary unbound production owner. */
static void
pi_assert_recovery_completion_owner(FakeXactRecord *fake,
									const RfPageOnlineRecordIdentityV1 *identity,
									const RfDetachedRecordPlanV1 *record,
									bool supported_native_completion)
{
	RfSideOnlinePlanV1 *recovery = pi_side_plan(identity, false);
	RfPageProofDetailV1 expected = RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
	RfPageProofDetailV1 detail;

#ifdef CLUSTER_SIDE_XACT_COLD_COMPLETION_V1
	if (supported_native_completion)
		expected = RF_PAGE_PROOF_DETAIL_OK;
#else
	(void)fake;
	(void)supported_native_completion;
#endif
	detail = rf_side_online_plan_feed_record_v1(recovery, record, identity);
	UT_ASSERT_EQ(detail, expected);
#ifdef CLUSTER_SIDE_XACT_COLD_COMPLETION_V1
	if (supported_native_completion && detail == RF_PAGE_PROOF_DETAIL_OK) {
		RfSideOnlineOperationV1 operation;
		RfSideContributionOwnersV1 owners;
		RfSideOnlineProductionOwnerV1 *owner = calloc(1, sizeof(*owner));
		int authority = 1;
		uint32 writes_before = canonical_writes, stats_before = cold_stats_calls;
		uint32 cache_before = cold_cache_calls;
		uint32 terminal_before = prepare_apply.terminal_projection_stores;
		bool readable;

		detail = rf_side_online_plan_seal_v1(recovery);
		UT_ASSERT_EQ(detail, RF_PAGE_PROOF_DETAIL_OK);
		readable = detail == RF_PAGE_PROOF_DETAIL_OK
				   && rf_side_online_plan_operation_v1(recovery, 0, &operation);
		UT_ASSERT(readable);
		if (!readable) {
			free(owner);
			rf_side_online_plan_destroy_v1(&recovery);
			return;
		}
		UT_ASSERT(operation.xact.cold_completion_effects && !operation.xact.pi_read_only);
		UT_ASSERT_EQ(operation.owned_payload_length, fake->u.decoded.main_data_len);
		UT_ASSERT(operation.owned_payload != NULL
				  && operation.owned_payload_length == fake->u.decoded.main_data_len
				  && memcmp(operation.owned_payload, fake->data, operation.owned_payload_length) == 0);
		UT_ASSERT(rf_side_online_plan_contribution_owners_v1(recovery, 0, &owners));
		UT_ASSERT((owners.owners & RF_SIDE_CONTRIBUTION_TERMINAL) != 0);
		/* Cache-only records retain the invalidation owner; FORCE_SYNC and
		 * stats retain completion. Neither can become an ownerless PI input. */
		UT_ASSERT((owners.owners
				  & (RF_SIDE_CONTRIBUTION_COMPLETION | RF_SIDE_CONTRIBUTION_INVALIDATION)) != 0);
		UT_ASSERT_EQ(rf_side_xact_apply_owned_v1(&operation.xact, operation.owned_payload,
												   operation.owned_payload_length),
					 RF_SIDE_XACT_APPLY_BLOCKED);
		UT_ASSERT_EQ(rf_side_xact_apply_cold_completion_v1(&operation.xact,
							 operation.owned_payload, operation.owned_payload_length, NULL, NULL),
					 RF_SIDE_XACT_APPLY_BLOCKED);
		UT_ASSERT(owner != NULL);
		if (owner != NULL) {
			/* Even a freshly initialized original owner cannot substitute for
			 * the missing cold scope/undo binding and native target proof. */
			UT_ASSERT(rf_side_online_production_owner_init_v1(owner, &authority,
																canonical_authority_fresh, 19, true));
			UT_ASSERT_NE(rf_side_online_production_preflight_v1(recovery, owner),
						 RF_PAGE_PROOF_DETAIL_OK);
			UT_ASSERT_NE(rf_side_online_production_apply_v1(recovery, owner),
						 RF_PAGE_PROOF_DETAIL_OK);
			UT_ASSERT(!owner->protected_set_active && !owner->protected_set_complete);
			free(owner);
		}
		UT_ASSERT_EQ(canonical_writes, writes_before);
		UT_ASSERT_EQ(cold_stats_calls, stats_before);
		UT_ASSERT_EQ(cold_cache_calls, cache_before);
		UT_ASSERT_EQ(prepare_apply.terminal_projection_stores, terminal_before);
	}
#endif
	rf_side_online_plan_destroy_v1(&recovery);
}
