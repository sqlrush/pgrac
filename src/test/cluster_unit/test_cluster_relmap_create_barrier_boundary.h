/*-------------------------------------------------------------------------
 * test_cluster_relmap_create_barrier_boundary.h
 *    Isolate only the original creation-consumer test's new authority leaves.
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_relmap_create_barrier_boundary.h
 * NOTES
 *    Applied only when compiling this consumer fixture and its inlined product.
 *-------------------------------------------------------------------------
 */
#ifndef TEST_CLUSTER_RELMAP_CREATE_BARRIER_BOUNDARY_H
#define TEST_CLUSTER_RELMAP_CREATE_BARRIER_BOUNDARY_H
#define cluster_cold_typed_relmap_create_barrier_current_v2 create_barrier_source_current
#define cluster_control_relmap_recovery_create_begin_barrier_locked_v2 create_barrier_control_begin
#define cluster_control_relmap_recovery_create_confirmed_barrier_current_v2 create_barrier_control_current
#define cluster_control_relmap_recovery_create_output_distinct_v2 create_barrier_control_output_distinct
#define cluster_cold_typed_relmap_create_accept_confirmation_v2 create_barrier_accept_confirmation
#endif
