/*-------------------------------------------------------------------------
 * test_cluster_relmap_create_scan_io.c
 *    Original recovery scanner with target-local ROOT visitor providers.
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_relmap_create_scan_io.c
 * NOTES
 *    ROOT visitation is controlled; scanner, decoder, SIDE and pair I/O are real.
 *-------------------------------------------------------------------------
 */
#define cluster_control_root_recovery_checkpoint_v1 create_scan_root_checkpoint
#define cluster_control_root_recovery_checkpoint_deadline_v1 create_scan_root_checkpoint_deadline
#define cluster_control_root_recovery_visit create_scan_root_visit
#define cluster_control_root_recovery_visit_deadline_v1 create_scan_root_visit_deadline
#define InterruptPending create_scan_interrupt_pending
#define ProcessInterrupts create_scan_interrupts
#include "../../backend/cluster/cluster_cold_recovery_io.c"
