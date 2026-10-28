/*-------------------------------------------------------------------------
 *
 * test_cluster_operation_worker_product.c
 *   Real holder builder in the original origin-reader boundary fixture.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *   src/test/cluster_unit/test_cluster_operation_worker_product.c
 *
 * NOTES
 *   Pgrac-original wrapper. Rename unrelated exported bodies whose explicit
 *   boundary doubles already belong to the origin fixture; no tested builder,
 *   history predicate or runtime reader body is replaced.
 *-------------------------------------------------------------------------
 */
#define cluster_cr_apply_operation_origin_admitted operation_worker_unused_head_cr
#define cluster_cr_r4_extract_resident_record operation_worker_real_top_extract
#define cluster_cr_native_prehistory_reader_lock operation_worker_unused_native_lock
#define cluster_cr_native_prehistory_reader_unlock operation_worker_unused_native_unlock
#define cluster_cr_native_origin_epoch0_provable operation_worker_unused_epoch0
#define cluster_rtvis_resolve_note_committed operation_worker_unused_note_commit
#define cluster_rtvis_resolve_note_aborted operation_worker_unused_note_abort
#define cluster_rtvis_resolve_note_failclosed operation_worker_unused_note_refuse
#ifndef CR_WALK_SOURCE
#define CR_WALK_SOURCE "../../backend/cluster/cluster_cr.c"
#endif
#include CR_WALK_SOURCE
