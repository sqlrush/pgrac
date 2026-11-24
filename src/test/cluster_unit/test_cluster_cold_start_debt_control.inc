/*-------------------------------------------------------------------------
 * test_cluster_cold_start_debt_control.inc
 *     Original Start protection credentials, adopted without changing leaves.
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_cold_start_debt_control.inc
 * NOTES
 *     Existing Start body is real; selection/storage remain controlled leaves.
 *-------------------------------------------------------------------------
 */

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
