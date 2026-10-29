/*-------------------------------------------------------------------------
 * test_cluster_recovery_scope_native_census.c
 *     Original E1 scope and complete native relmap census composition.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md
 *
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_recovery_scope_native_census.c
 *
 * NOTES
 *     Keep the original input fixture and its lower Startup/GES providers.
 *     Privately name only its two unused controlled census providers. The
 *     new target links the original census product and original input scope.
 *-------------------------------------------------------------------------
 */
#define RECOVERY_SCOPE_NATIVE_CENSUS 1
#define cluster_relmap_retention_census_execute_native_episode scope_native_fixture_execute
#define cluster_relmap_retention_census_destroy scope_native_fixture_destroy
#include "test_cluster_recovery_native_inputs.c"
