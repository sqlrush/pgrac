/* Original Start wiring. CF/control selection/publication remain controlled
 * leaves; the separate native-table suite executes real ROOT/registry files. */
static bool start_table_cf_held, start_table_cf_share, start_table_installed;
static unsigned start_table_acquires, start_table_releases, start_table_formats;
static unsigned start_table_publishes, start_table_version, start_table_fault;
static uint64 start_table_boot;
static bool start_table_managed_fixture;
static unsigned start_table_info_fault, start_table_binds, start_table_bind_fault;
static unsigned start_table_infos;
static ClusterControlNativeTableInfoV2 start_table_view, start_table_bound;

static void
start_native_table_reset(void)
{
	start_table_cf_held = start_table_cf_share = start_table_installed = false;
	start_table_acquires = start_table_releases = start_table_formats = 0;
	start_table_publishes = start_table_fault = 0;
	start_table_version = start_table_managed_fixture ? 4 : 3;
	start_table_info_fault = start_table_binds = start_table_bind_fault = start_table_infos = 0;
	memset(&start_table_view, 0, sizeof(start_table_view));
	memset(&start_table_bound, 0, sizeof(start_table_bound));
	start_table_boot = 0;
}

bool
cluster_cf_held(LOCKMODE mode)
{
	return mode == ExclusiveLock ? start_table_cf_held : start_table_cf_share;
}

bool
cluster_cf_held_is_clusterwide(LOCKMODE mode)
{
	return cluster_cf_held(mode);
}

bool
cluster_cf_lock(LOCKMODE mode)
{
	UT_ASSERT_EQ(mode, ExclusiveLock);
	UT_ASSERT(!start_table_cf_held && !start_table_cf_share);
	UT_ASSERT_EQ(ctrc_driver_lwlocks, 0);
	UT_ASSERT(!tx_ir_held && cluster_recovery_cold_start != NULL);
	UT_ASSERT(!window_region.checkpoint_owner.valid);
	start_table_acquires++;
	start_table_cf_held = start_table_fault != 1;
	return start_table_cf_held;
}

ClusterCfReleaseResult
cluster_cf_unlock_confirmed(LOCKMODE mode)
{
	UT_ASSERT_EQ(mode, ExclusiveLock);
	start_table_releases++;
	if (start_table_fault == 13) pg_re_throw();
	if (start_table_fault == 8) return CLUSTER_CF_RELEASE_UNCONFIRMED;
	if (!start_table_cf_held) return CLUSTER_CF_RELEASE_NOT_HELD;
	start_table_cf_held = false;
	if (start_table_fault == 7) observed_writer.epoch++;
	return CLUSTER_CF_RELEASE_CONFIRMED;
}

ClusterControlRootResult
cluster_control_root_relmap_format_locked_v2(Size budget, ClusterControlRelmapFormatV2 *out)
{
	ClusterControlRelmapFormatV2 value = {0};
	UT_ASSERT(start_table_cf_held);
	UT_ASSERT_EQ(budget, CLUSTER_NATIVE_FILES_REGISTRY_BUDGET_V2);
	UT_ASSERT(!tx_ir_held);
	start_table_formats++;
	if (start_table_fault == 12) pg_re_throw();
	if (start_table_fault == 2) return CLUSTER_CONTROL_ROOT_IO_ERROR;
	value.root_version = start_table_fault == 3 ? 0 : start_table_version;
	value.config.identity.system_identifier = observed_writer.ref.claim.identity.system_identifier;
	value.config.identity.database_incarnation = observed_writer.ref.claim.database_incarnation;
	value.config.identity.generation = observed_writer.ref.claim.max_config_generation;
	memcpy(value.config.identity.storage_uuid, observed_writer.ref.claim.identity.storage_uuid, 16);
	memcpy(value.config.identity.authority_uuid, observed_writer.ref.claim.identity.authority_uuid, 16);
	value.config.identity.configured[0] = 1;
	value.config.sha256[0] = 31;
	value.root_sha256[0] = 41;
	if (start_table_fault == 5 && start_table_formats > 1) value.root_sha256[0]++;
	if (start_table_fault == 9) value.config.identity.authority_uuid[0]++;
	if (start_table_fault == 10) value.config.identity.generation--;
	*out = value;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

ClusterControlNativeResultV2
cluster_control_native_files_publish_locked_v2(const ClusterSharedConfigRef *config,
	Size budget, uint64 startup)
{
	UT_ASSERT(start_table_cf_held && !tx_ir_held);
	UT_ASSERT_EQ(start_table_formats, 1);
	UT_ASSERT_EQ(config->identity.system_identifier, GetSystemIdentifier());
	UT_ASSERT_EQ(budget, CLUSTER_NATIVE_FILES_REGISTRY_BUDGET_V2);
	UT_ASSERT_EQ(startup, owner_boot);
	UT_ASSERT(!window_region.checkpoint_owner.valid);
	start_table_publishes++;
	if (start_table_fault == 11) pg_re_throw();
	if (start_table_fault == 4) return CLUSTER_NATIVE_FILES_BUSY_V2;
	start_table_installed = true;
	start_table_boot = startup;
	start_table_view.mode = CLUSTER_NATIVE_FILES_MANAGED_V2;
	start_table_view.table_generation = 11;
	start_table_view.startup_generation = startup;
	start_table_view.count = start_table_view.capacity = 2;
	start_table_view.control.config = *config;
	start_table_view.control.logical_count = 1;
	start_table_view.control.source_count = 1;
	start_table_view.control.root_pair_sha256[0] = 41;
	if (start_table_fault == 6) observed_writer.epoch++;
	return CLUSTER_NATIVE_FILES_OK_V2;
}

UT_TEST(test_start_native_table_managed_publish_precedes_original_work)
{
	reset(); retain(); start_table_version = 4;
	UT_ASSERT(ClusterBeginColdRecoveryTx(&info));
	UT_ASSERT(info.cluster_tx_start != NULL && ClusterColdRecoveryTxCurrent(info.cluster_tx_start));
	UT_ASSERT_EQ(start_table_acquires, 1);
	UT_ASSERT_EQ(start_table_formats, 2);
	UT_ASSERT_EQ(start_table_publishes, 1);
	UT_ASSERT_EQ(start_table_releases, 1);
	UT_ASSERT(start_table_installed && start_table_boot == owner_boot);
	UT_ASSERT(!start_table_cf_held && !start_table_cf_share && !tx_ir_held);
	UT_ASSERT_EQ(scope_begins, 0);
	UT_ASSERT(window_region.checkpoint_owner.valid);
	UT_ASSERT(!ClusterBeginColdRecoveryTx(&info));
	UT_ASSERT_EQ(start_table_publishes, 1);
	ClusterReleaseColdRecoveryInputs(&info);
}

UT_TEST(test_start_native_table_legacy_uses_explicit_control_format)
{
	reset(); retain();
	UT_ASSERT(ClusterBeginColdRecoveryTx(&info));
	UT_ASSERT_EQ(start_table_formats, 2);
	UT_ASSERT_EQ(start_table_publishes, 0);
	UT_ASSERT_EQ(start_table_acquires, 1);
	UT_ASSERT_EQ(start_table_releases, 1);
	UT_ASSERT(!start_table_cf_held && !start_table_installed);
	ClusterReleaseColdRecoveryInputs(&info);
}

UT_TEST(test_start_native_table_refusal_preserves_original_input_custody)
{
	for (unsigned fault = 1; fault <= 10; fault++) {
		unsigned destroyed;
		reset(); retain(); start_table_version = 4; start_table_fault = fault;
		destroyed = input_destroys;
		UT_ASSERT(!ClusterBeginColdRecoveryTx(&info));
		UT_ASSERT(info.cluster_tx_start == NULL && cluster_recovery_cold_start == NULL);
		UT_ASSERT(info.cluster_cold_inputs == &typed && cluster_recovery_cold_inputs == &typed);
		UT_ASSERT(!window_region.checkpoint_owner.valid);
		UT_ASSERT_EQ(input_destroys, destroyed);
		UT_ASSERT_EQ(scope_begins, 0);
		UT_ASSERT_EQ(start_table_acquires, 1);
		UT_ASSERT_EQ(start_table_releases, 1);
		UT_ASSERT_EQ(start_table_cf_held, fault == 8);
		UT_ASSERT_EQ(start_table_publishes, fault >= 4 && fault <= 8 ? 1 : 0);
		if (fault >= 5 && fault <= 8) UT_ASSERT(start_table_installed);
		UT_ASSERT(!ClusterBeginColdRecoveryTx(&info));
		UT_ASSERT_EQ(start_table_acquires, 1);
	}
}

UT_TEST(test_start_native_table_error_never_publishes_start_or_retries_cleanup)
{
	for (unsigned fault = 11; fault <= 13; fault++) {
		volatile bool threw = false;
		unsigned destroyed;
		reset(); retain(); start_table_version = 4; start_table_fault = fault;
		destroyed = input_destroys;
		PG_TRY();
		{
			(void)ClusterBeginColdRecoveryTx(&info);
		}
		PG_CATCH();
		{
			threw = true;
		}
		PG_END_TRY();
		UT_ASSERT(threw);
		UT_ASSERT(info.cluster_tx_start == NULL && cluster_recovery_cold_start == NULL);
		UT_ASSERT(info.cluster_cold_inputs == &typed && cluster_recovery_cold_inputs == &typed);
		UT_ASSERT(!window_region.checkpoint_owner.valid);
		UT_ASSERT_EQ(input_destroys, destroyed);
		UT_ASSERT_EQ(start_table_acquires, 1);
		UT_ASSERT_EQ(start_table_releases, 1);
		UT_ASSERT_EQ(start_table_cf_held, fault == 13);
		UT_ASSERT_EQ(start_table_publishes, fault == 12 ? 0 : 1);
		UT_ASSERT_EQ(start_table_installed, fault == 13);
		UT_ASSERT(!ClusterBeginColdRecoveryTx(&info));
		UT_ASSERT_EQ(start_table_acquires, 1);
	}
}

UT_TEST(test_start_native_table_rejects_held_locks_before_cf_or_io)
{
	for (unsigned fault = 0; fault < 5; fault++) {
		reset(); retain(); start_table_version = 4;
		switch (fault) {
		case 0: start_table_cf_held = true; break;
		case 1: start_table_cf_share = true; break;
		case 2: ctrc_driver_lwlocks = 1; break;
		case 3: CritSectionCount = 1; break;
		case 4: ShutdownRequestPending = true; break;
		}
		UT_ASSERT(!ClusterBeginColdRecoveryTx(&info));
		UT_ASSERT(info.cluster_tx_start == NULL && cluster_recovery_cold_start == NULL);
		UT_ASSERT_EQ(start_table_acquires, 0);
		UT_ASSERT_EQ(start_table_formats, 0);
		UT_ASSERT_EQ(start_table_publishes, 0);
		UT_ASSERT_EQ(start_table_releases, 0);
		UT_ASSERT_EQ(start_table_cf_held, fault == 0);
		UT_ASSERT_EQ(start_table_cf_share, fault == 1);
		ctrc_driver_lwlocks = CritSectionCount = 0;
		ShutdownRequestPending = false;
	}
}

static void
start_native_table_tests(void)
{
	UT_RUN(test_start_native_table_managed_publish_precedes_original_work);
	UT_RUN(test_start_native_table_legacy_uses_explicit_control_format);
	UT_RUN(test_start_native_table_refusal_preserves_original_input_custody);
	UT_RUN(test_start_native_table_error_never_publishes_start_or_retries_cleanup);
	UT_RUN(test_start_native_table_rejects_held_locks_before_cf_or_io);
	reset();
}


/* The Start/census boundary is real. The separate census suite executes the
 * original table/Borrow/scanner; this leaf checks its fixed snapshot contract. */
bool
cluster_control_native_files_info_v2(ClusterControlNativeTableInfoV2 *out)
{
	start_table_infos++;
	if (start_table_info_fault == 1) return false;
	if (start_table_info_fault == 2) pg_re_throw();
	*out = start_table_view;
	if (start_table_info_fault == 3) out->startup_generation++;
	if (start_table_info_fault == 4) out->control.config.identity.database_incarnation++;
	if (start_table_info_fault == 5) out->table_generation = 0;
	return start_table_installed;
}

bool
cluster_recovery_census_bind_managed_relmap_v2(ClusterRecoveryCensus *cursor,
	const ClusterControlNativeTableInfoV2 *table)
{
	ClusterControlNativeTableInfoV2 expected = *table, now = start_table_view;
	UT_ASSERT(cursor == (ClusterRecoveryCensus *)&observation_cursor_object);
	UT_ASSERT(observation_cursor_live && observation_scope_live);
	UT_ASSERT(!start_table_cf_held && !start_table_cf_share);
	UT_ASSERT_EQ(observation_steps, 0);
	start_table_binds++;
	if (start_table_bind_fault == 2) pg_re_throw();
	expected.borrowed = now.borrowed = 0;
	if (start_table_bind_fault == 1 || memcmp(&expected, &now, sizeof(now)) != 0)
		return false;
	start_table_bound = *table;
	return true;
}

static void
start_managed_observation_ready(void)
{
	start_table_managed_fixture = true;
	observation_ready();
	start_table_managed_fixture = false;
}

UT_TEST(test_start_managed_census_binds_published_snapshot_after_cf_release)
{
	start_managed_observation_ready();
	UT_ASSERT(ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
	UT_ASSERT_EQ(start_table_binds, 1);
	UT_ASSERT_EQ(start_table_bound.table_generation, 11);
	UT_ASSERT_EQ(start_table_bound.startup_generation, owner_boot);
	UT_ASSERT_EQ(observation_steps, 3);
	UT_ASSERT(!start_table_cf_held && !start_table_cf_share);
	/* Concurrent local borrows do not change the fixed selected identity. */
	start_table_view.borrowed++;
	UT_ASSERT(ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 0));
	UT_ASSERT_EQ(start_table_binds, 1);
	ClusterReleaseColdRecoveryInputs(&info);
}

UT_TEST(test_start_managed_census_cannot_select_a_replacement_table)
{
	for (unsigned fault = 0; fault < 6; fault++) {
		ClusterRecoveryCtrcResultValueV1 original;
		start_managed_observation_ready();
		original = window_region.checkpoint_owner.ctrc_result;
		switch (fault) {
		case 0: start_table_view.table_generation++; break;
		case 1: start_table_view.startup_generation++; break;
		case 2: start_table_view.control.config.identity.database_incarnation++; break;
		case 3: start_table_view.control.root_pair_sha256[0]++; break;
		case 4: start_table_view.mode = CLUSTER_NATIVE_FILES_UNKNOWN_V2; break;
		case 5: start_table_view.reservation_count++; break;
		}
		UT_ASSERT(!ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
		UT_ASSERT_EQ(observation_steps, 0);
		UT_ASSERT_EQ(start_table_binds, 1);
		UT_ASSERT(!observation_cursor_live && !observation_scope_live);
		UT_ASSERT(info.cluster_cold_inputs == &typed && info.cluster_tx_start != NULL);
		UT_ASSERT(memcmp(&original, &window_region.checkpoint_owner.ctrc_result, sizeof(original)) == 0);
		UT_ASSERT(!ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 0));
		UT_ASSERT_EQ(start_table_binds, 1);
		ClusterReleaseColdRecoveryInputs(&info);
	}
}

UT_TEST(test_start_managed_census_bind_refusal_and_error_preserve_input_custody)
{
	for (unsigned fault = 1; fault <= 2; fault++) {
		volatile bool threw = false;
		unsigned destroyed;
		start_managed_observation_ready(); destroyed = input_destroys;
		start_table_bind_fault = fault;
		PG_TRY();
		{
			UT_ASSERT(!ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
		}
		PG_CATCH(); { threw = true; } PG_END_TRY();
		UT_ASSERT_EQ(threw, fault == 2);
		UT_ASSERT_EQ(start_table_binds, 1);
		UT_ASSERT_EQ(observation_steps, 0);
		UT_ASSERT_EQ(input_destroys, destroyed);
		UT_ASSERT(info.cluster_cold_inputs == &typed && info.cluster_tx_start != NULL);
		UT_ASSERT(!observation_cursor_live && !observation_scope_live);
		start_table_bind_fault = 0;
		UT_ASSERT(!ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 0));
		UT_ASSERT_EQ(start_table_binds, 1);
		ClusterReleaseColdRecoveryInputs(&info);
	}
}

UT_TEST(test_start_managed_snapshot_failure_refuses_before_original_registration)
{
	for (unsigned fault = 1; fault <= 5; fault++) {
		volatile bool threw = false;
		unsigned destroyed;
		reset(); retain(); start_table_version = 4; start_table_info_fault = fault;
		destroyed = input_destroys;
		PG_TRY(); { UT_ASSERT(!ClusterBeginColdRecoveryTx(&info)); }
		PG_CATCH(); { threw = true; } PG_END_TRY();
		UT_ASSERT_EQ(threw, fault == 2);
		UT_ASSERT(info.cluster_tx_start == NULL && cluster_recovery_cold_start == NULL);
		UT_ASSERT(info.cluster_cold_inputs == &typed && cluster_recovery_cold_inputs == &typed);
		UT_ASSERT_EQ(input_destroys, destroyed);
		UT_ASSERT(!window_region.checkpoint_owner.valid && !start_table_cf_held);
		UT_ASSERT_EQ(start_table_publishes, 1);
		UT_ASSERT_EQ(start_table_releases, 1);
		UT_ASSERT(!ClusterBeginColdRecoveryTx(&info));
		UT_ASSERT_EQ(start_table_publishes, 1);
	}
}

UT_TEST(test_start_explicit_legacy_ignores_an_unrelated_managed_table)
{
	observation_ready();
	start_table_installed = true;
	start_table_view.mode = CLUSTER_NATIVE_FILES_MANAGED_V2;
	start_table_view.table_generation = 99;
	UT_ASSERT(ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
	UT_ASSERT_EQ(start_table_binds, 0);
	UT_ASSERT_EQ(start_table_infos, 0);
	UT_ASSERT_EQ(observation_steps, 3);
	ClusterReleaseColdRecoveryInputs(&info);
}

static void
start_managed_census_tests(void)
{
	UT_RUN(test_start_managed_census_binds_published_snapshot_after_cf_release);
	UT_RUN(test_start_managed_census_cannot_select_a_replacement_table);
	UT_RUN(test_start_managed_census_bind_refusal_and_error_preserve_input_custody);
	UT_RUN(test_start_managed_snapshot_failure_refuses_before_original_registration);
	UT_RUN(test_start_explicit_legacy_ignores_an_unrelated_managed_table);
	reset();
}

uint64
cluster_cf_attempt_sequence(void)
{
    return start_table_acquires;
}

/* Selection/storage are controlled leaves here; Start, its original effects,
 * scope, all-replay authorization and late owner checks are the product body. */
static int debt_selection_object;
static ClusterControlRelmapReferenceV2 debt_selection_reference;
static unsigned debt_selection_reads, debt_selection_rechecks, debt_selection_fault;

bool
cluster_control_root_relmap_selection_reference_v4(ClusterControlRelmapSelectionV2 *selection,
    ClusterControlRelmapReferenceV2 *out)
{
    UT_ASSERT(selection == (ClusterControlRelmapSelectionV2 *)&debt_selection_object);
    UT_ASSERT(start_table_cf_held && !start_table_cf_share);
    UT_ASSERT_EQ(ctrc_driver_lwlocks, 0);
    debt_selection_reads++;
    *out = debt_selection_reference;
    return true;
}

ClusterControlRootResult
cluster_control_root_relmap_selection_recheck_locked_v4(ClusterControlRelmapSelectionV2 *selection)
{
    UT_ASSERT(selection == (ClusterControlRelmapSelectionV2 *)&debt_selection_object);
    UT_ASSERT(start_table_cf_held && !start_table_cf_share);
    debt_selection_rechecks++;
    if (debt_selection_fault == 1) return CLUSTER_CONTROL_ROOT_IO_ERROR;
    if (debt_selection_fault == 2) observed_writer.epoch++;
    if (debt_selection_fault == 3) pg_re_throw();
    if (debt_selection_fault == 4) start_table_view.table_generation++;
    if (debt_selection_fault == 5) {
        page_source_request_delta[0]++;
        debt_selection_fault = 0;
    }
    return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

static void
start_debt_ready(void)
{
    start_managed_observation_ready();
    UT_ASSERT(ClusterColdRecoveryCtrcObserve(info.cluster_tx_start, 2 * 1024 * 1024));
    UT_ASSERT(ClusterColdRecoveryCtrcReplaysAcquire(info.cluster_tx_start));
    debt_selection_reference = info.cluster_tx_start->relmap_table.control;
    debt_selection_reads = debt_selection_rechecks = debt_selection_fault = 0;
    /* Only this original caller's existing CF-X is borrowed by the query. */
    start_table_cf_held = true;
}

UT_TEST(test_start_debt_query_requires_original_complete_ir_walr_and_same_cf)
{
    start_debt_ready();
    UT_ASSERT(ClusterColdRecoveryDebtControlCurrentV2(info.cluster_tx_start,
        (ClusterControlRelmapSelectionV2 *)&debt_selection_object));
    UT_ASSERT(debt_selection_reads >= 1 && debt_selection_rechecks >= 1);
    UT_ASSERT(start_table_cf_held && observation_scope_live && observation_cursor_live);
    UT_ASSERT(info.cluster_tx_start != NULL && info.cluster_cold_inputs == &typed);
    start_table_cf_held = false;
    UT_ASSERT(!ClusterColdRecoveryDebtControlCurrentV2(info.cluster_tx_start,
        (ClusterControlRelmapSelectionV2 *)&debt_selection_object));
    ClusterReleaseColdRecoveryInputs(&info);
}

UT_TEST(test_start_debt_query_refuses_wrong_phase_holder_and_identity)
{
    for (unsigned fault = 0; fault < 10; fault++) {
        start_debt_ready();
        switch (fault) {
        case 0: ctrc_driver_false_authorization = true; break;
        case 1: start_table_cf_held = false; start_table_cf_share = true; break;
        case 2: CritSectionCount = 1; break;
        case 3: ctrc_driver_lwlocks = 1; break;
        case 4: native_recovery = true; break;
        case 5: cluster_recovery_cold_start->ctrc_observation_complete = false; break;
        case 6: cluster_recovery_cold_start->managed_relmap_table = false; break;
        case 7: start_table_view.table_generation++; break;
        case 8: start_table_view.control.root_pair_sha256[0]++; break;
        case 9: observed_writer.epoch++; break;
        }
        UT_ASSERT(!ClusterColdRecoveryDebtControlCurrentV2(info.cluster_tx_start,
            (ClusterControlRelmapSelectionV2 *)&debt_selection_object));
        UT_ASSERT_EQ(debt_selection_reads, 0);
        UT_ASSERT(observation_scope_live && observation_cursor_live);
        CritSectionCount = ctrc_driver_lwlocks = 0;
        native_recovery = false;
        start_table_cf_held = start_table_cf_share = false;
        /* Failed identity is exit cleanup, never successful checkpoint handoff. */
        reset();
    }
}

UT_TEST(test_start_debt_query_compares_every_original_control_reference)
{
    for (unsigned fault = 0; fault < 5; fault++) {
        start_debt_ready();
        switch (fault) {
        case 0: debt_selection_reference.config.sha256[0]++; break;
        case 1: debt_selection_reference.root_pair_sha256[31]++; break;
        case 2: debt_selection_reference.registry_reference[127]++; break;
        case 3: debt_selection_reference.logical_count++; break;
        case 4: debt_selection_reference.source_count++; break;
        }
        UT_ASSERT(!ClusterColdRecoveryDebtControlCurrentV2(info.cluster_tx_start,
            (ClusterControlRelmapSelectionV2 *)&debt_selection_object));
        UT_ASSERT(observation_scope_live && observation_cursor_live);
        start_table_cf_held = false;
        ClusterReleaseColdRecoveryInputs(&info);
    }
}

UT_TEST(test_start_debt_query_late_refusal_and_error_preserve_original_protections)
{
    for (unsigned fault = 1; fault <= 4; fault++) {
        volatile bool threw = false, accepted = false;
        start_debt_ready(); debt_selection_fault = fault;
        PG_TRY();
        {
            accepted = ClusterColdRecoveryDebtControlCurrentV2(info.cluster_tx_start,
                (ClusterControlRelmapSelectionV2 *)&debt_selection_object);
        }
        PG_CATCH();
        {
            threw = true;
        }
        PG_END_TRY();
        UT_ASSERT(!accepted && threw == (fault == 3));
        UT_ASSERT(start_table_cf_held && observation_scope_live && observation_cursor_live);
        UT_ASSERT(info.cluster_cold_inputs == &typed && info.cluster_tx_start != NULL);
        start_table_cf_held = false;
        reset();
    }
}

bool
cluster_control_root_relmap_selection_output_distinct_v4(
    const ClusterControlRelmapSelectionV2 *selection, const void *out, Size n)
{
    uintptr_t a = (uintptr_t)out, b = (uintptr_t)&debt_selection_reference;
    UT_ASSERT(selection == (const ClusterControlRelmapSelectionV2 *)&debt_selection_object);
    return out != NULL && n != 0
        && (a <= b ? b - a >= n : a - b >= sizeof(debt_selection_reference));
}

UT_TEST(test_start_debt_begin_is_once_owned_and_never_rebases_to_later_cf)
{
    ClusterRecoveryDebtStartV2 *credential = NULL, *second = NULL;
    start_debt_ready();
    UT_ASSERT(ClusterColdRecoveryDebtBeginV2(info.cluster_tx_start,
        (ClusterControlRelmapSelectionV2 *)&debt_selection_object, &credential));
    UT_ASSERT(credential != NULL);
    UT_ASSERT(ClusterColdRecoveryDebtCurrentV2(info.cluster_tx_start, credential));
    UT_ASSERT(!ClusterColdRecoveryDebtBeginV2(info.cluster_tx_start,
        (ClusterControlRelmapSelectionV2 *)&debt_selection_object, &second));
    UT_ASSERT(second == NULL);
    start_table_acquires++;
    UT_ASSERT(!ClusterColdRecoveryDebtCurrentV2(info.cluster_tx_start, credential));
    start_table_cf_held = false;
    ClusterReleaseColdRecoveryInputs(&info);
    UT_ASSERT(!ClusterColdRecoveryDebtCurrentV2(NULL, credential));
}

UT_TEST(test_start_debt_begin_false_error_and_alias_cannot_publish_or_retry)
{
    for (unsigned fault = 0; fault < 3; fault++) {
        ClusterRecoveryDebtStartV2 *credential = NULL;
        volatile bool accepted = false, threw = false;
        start_debt_ready();
        if (fault == 0) debt_selection_reference.root_pair_sha256[0]++;
        if (fault == 1) debt_selection_fault = 3;
        if (fault == 2) {
            struct ClusterRecoveryStartV1 before = *info.cluster_tx_start;
            UT_ASSERT(!ClusterColdRecoveryDebtBeginV2(info.cluster_tx_start,
                (ClusterControlRelmapSelectionV2 *)&debt_selection_object,
                (ClusterRecoveryDebtStartV2 **)&cluster_recovery_cold_start->pid));
            UT_ASSERT(memcmp(&before, info.cluster_tx_start, sizeof(before)) == 0);
            start_table_cf_held = false;
            ClusterReleaseColdRecoveryInputs(&info);
            continue;
        }
        PG_TRY();
        {
            accepted = ClusterColdRecoveryDebtBeginV2(info.cluster_tx_start,
                (ClusterControlRelmapSelectionV2 *)&debt_selection_object, &credential);
        }
        PG_CATCH();
        {
            threw = true;
        }
        PG_END_TRY();
        UT_ASSERT(!accepted && threw == (fault == 1) && credential == NULL);
        debt_selection_reference = info.cluster_tx_start->relmap_table.control;
        debt_selection_fault = 0;
        UT_ASSERT(!ClusterColdRecoveryDebtBeginV2(info.cluster_tx_start,
            (ClusterControlRelmapSelectionV2 *)&debt_selection_object, &credential));
        UT_ASSERT(credential == NULL && start_table_cf_held);
        UT_ASSERT(observation_scope_live && observation_cursor_live);
        start_table_cf_held = false;
        ClusterReleaseColdRecoveryInputs(&info);
    }
}

static void
start_debt_many_ready(void)
{
    start_table_managed_fixture = true;
    page_sources_three_ready();
    start_table_managed_fixture = false;
    debt_selection_reference = info.cluster_tx_start->relmap_table.control;
    debt_selection_reads = debt_selection_rechecks = debt_selection_fault = 0;
    start_table_cf_held = true;
}

UT_TEST(test_start_debt_binds_first_and_last_original_input_holder)
{
    for (unsigned index = 0; index < 3; index++) {
        ClusterRecoveryDebtStartV2 *credential = NULL;
        start_debt_many_ready();
        UT_ASSERT(ClusterColdRecoveryDebtBeginV2(info.cluster_tx_start,
            (ClusterControlRelmapSelectionV2 *)&debt_selection_object, &credential));
        UT_ASSERT(ClusterColdRecoveryDebtCurrentV2(info.cluster_tx_start, credential));
        page_source_request_delta[index]++;
        /* A different still-authorized native holder is not the bound grant. */
        UT_ASSERT(!ClusterColdRecoveryDebtCurrentV2(info.cluster_tx_start, credential));
        page_source_request_delta[index]--;
        UT_ASSERT(!ClusterColdRecoveryDebtCurrentV2(info.cluster_tx_start, credential));
        UT_ASSERT(observation_scope_live && observation_cursor_live && start_table_cf_held);
        UT_ASSERT_EQ(observation_scope_releases + observation_census_destroys, 0);
        start_table_cf_held = false;
        ClusterReleaseColdRecoveryInputs(&info);
    }
}

UT_TEST(test_start_debt_binds_scope_and_continuous_retention_incarnation)
{
    for (unsigned fault = 0; fault < 3; fault++) {
        ClusterRecoveryDebtStartV2 *credential = NULL;
        start_debt_many_ready();
        UT_ASSERT(ClusterColdRecoveryDebtBeginV2(info.cluster_tx_start,
            (ClusterControlRelmapSelectionV2 *)&debt_selection_object, &credential));
        if (fault == 0) page_source_scope_delta++;
        if (fault == 1) page_source_retention_delta++;
        if (fault == 2) page_source_input_count--;
        UT_ASSERT(!ClusterColdRecoveryDebtCurrentV2(info.cluster_tx_start, credential));
        UT_ASSERT(observation_scope_live && observation_cursor_live && start_table_cf_held);
        UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
        start_table_cf_held = false;
        ClusterReleaseColdRecoveryInputs(&info);
    }
}

static unsigned debt_input_capture_steps;
static void
debt_input_replace_last(void)
{
    if (++debt_input_capture_steps == 3) page_source_request_delta[2]++;
}

UT_TEST(test_start_debt_input_capture_refusal_and_error_never_retry_or_release)
{
    for (unsigned fault = 0; fault < 3; fault++) {
        ClusterRecoveryDebtStartV2 *credential = NULL;
        volatile bool accepted = false, caught = false;
        start_debt_many_ready();
        debt_input_capture_steps = 0;
        if (fault == 0) page_source_step = debt_input_replace_last;
        if (fault == 1) page_source_fault = 1;
        if (fault == 2) page_source_current_error = true;
        PG_TRY(); {
            accepted = ClusterColdRecoveryDebtBeginV2(info.cluster_tx_start,
                (ClusterControlRelmapSelectionV2 *)&debt_selection_object, &credential);
        }
        PG_CATCH(); { caught = true; }
        PG_END_TRY();
        UT_ASSERT(!accepted && credential == NULL && caught == (fault == 2));
        page_source_step = NULL; page_source_fault = 0; page_source_current_error = false;
        UT_ASSERT(!ClusterColdRecoveryDebtBeginV2(info.cluster_tx_start,
            (ClusterControlRelmapSelectionV2 *)&debt_selection_object, &credential));
        UT_ASSERT(credential == NULL && start_table_cf_held);
        UT_ASSERT(observation_scope_live && observation_cursor_live);
        UT_ASSERT_EQ(observation_scope_releases + observation_census_destroys, 0);
        start_table_cf_held = false;
        ClusterReleaseColdRecoveryInputs(&info);
    }
}

UT_TEST(test_start_debt_current_input_error_is_sticky_without_mutation_or_cleanup)
{
    ClusterRecoveryDebtStartV2 *credential = NULL;
    volatile bool caught = false, accepted = false;
    start_debt_many_ready();
    UT_ASSERT(ClusterColdRecoveryDebtBeginV2(info.cluster_tx_start,
        (ClusterControlRelmapSelectionV2 *)&debt_selection_object, &credential));
    page_source_current_error = true;
    PG_TRY(); { accepted = ClusterColdRecoveryDebtCurrentV2(info.cluster_tx_start, credential); }
    PG_CATCH(); { caught = true; }
    PG_END_TRY();
    UT_ASSERT(caught && !accepted);
    page_source_current_error = false;
    UT_ASSERT(!ClusterColdRecoveryDebtCurrentV2(info.cluster_tx_start, credential));
    UT_ASSERT(observation_scope_live && observation_cursor_live && start_table_cf_held);
    UT_ASSERT_EQ(window_region.checkpoint_owner.ctrc_accepted_generation, 0);
    UT_ASSERT_EQ(observation_scope_releases + observation_census_destroys, 0);
    start_table_cf_held = false;
    ClusterReleaseColdRecoveryInputs(&info);
}

UT_TEST(test_start_debt_rechecks_input_holders_after_physical_control_validation)
{
    ClusterRecoveryDebtStartV2 *credential = NULL;
    start_debt_many_ready();
    UT_ASSERT(ClusterColdRecoveryDebtBeginV2(info.cluster_tx_start,
        (ClusterControlRelmapSelectionV2 *)&debt_selection_object, &credential));
    debt_selection_fault = 5;
    UT_ASSERT(!ClusterColdRecoveryDebtCurrentV2(info.cluster_tx_start, credential));
    debt_selection_fault = 0; page_source_request_delta[0] = 0;
    UT_ASSERT(!ClusterColdRecoveryDebtCurrentV2(info.cluster_tx_start, credential));
    UT_ASSERT(observation_scope_live && observation_cursor_live && start_table_cf_held);
    UT_ASSERT_EQ(observation_scope_releases + observation_census_destroys, 0);
    start_table_cf_held = false;
    ClusterReleaseColdRecoveryInputs(&info);
}

UT_TEST(test_start_debt_current_preconditions_do_not_touch_input_holders)
{
    for (unsigned fault = 0; fault < 4; fault++) {
        ClusterRecoveryDebtStartV2 *credential = NULL;
        unsigned factories;
        start_debt_many_ready();
        UT_ASSERT(ClusterColdRecoveryDebtBeginV2(info.cluster_tx_start,
            (ClusterControlRelmapSelectionV2 *)&debt_selection_object, &credential));
        factories = page_source_factories;
        if (fault == 0) CritSectionCount++;
        if (fault == 1) ctrc_driver_lwlocks++;
        if (fault == 2) MyBackendType = B_BACKEND;
        if (fault == 3) native_recovery = true;
        UT_ASSERT(!ClusterColdRecoveryDebtCurrentV2(info.cluster_tx_start, credential));
        UT_ASSERT_EQ(page_source_factories, factories);
        CritSectionCount = ctrc_driver_lwlocks = 0;
        MyBackendType = B_STARTUP; native_recovery = false;
        UT_ASSERT(ClusterColdRecoveryDebtCurrentV2(info.cluster_tx_start, credential));
        start_table_cf_held = false;
        ClusterReleaseColdRecoveryInputs(&info);
    }
}

UT_TEST(test_start_debt_captures_input_holders_before_first_control_read)
{
    ClusterRecoveryDebtStartV2 *credential = NULL;
    start_debt_many_ready();
    debt_selection_fault = 5;
    UT_ASSERT(!ClusterColdRecoveryDebtBeginV2(info.cluster_tx_start,
        (ClusterControlRelmapSelectionV2 *)&debt_selection_object, &credential));
    UT_ASSERT(credential == NULL && page_source_request_delta[0] == 1);
    UT_ASSERT(observation_scope_live && observation_cursor_live && start_table_cf_held);
    UT_ASSERT_EQ(observation_scope_releases + observation_census_destroys, 0);
    start_table_cf_held = false;
    ClusterReleaseColdRecoveryInputs(&info);
}
