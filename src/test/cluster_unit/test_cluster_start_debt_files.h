/*-------------------------------------------------------------------------
 *
 * test_cluster_start_debt_files.h
 *    Test-only access to real protective debt files and their original reader.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_start_debt_files.h
 *
 * NOTES
 *    Borrow the linked original Start actor. No production admission changes.
 *
 *-------------------------------------------------------------------------
 */
#ifndef TEST_CLUSTER_START_DEBT_FILES_H
#define TEST_CLUSTER_START_DEBT_FILES_H
#include "cluster/cluster_control_debt_transition.h"
#define START_DEBT_FILES_BUDGET ((Size) 24 * 1024 * 1024)
extern int start_debt_files_before(uint8 *pair, Size bytes);
extern ClusterControlDebtTransitionV2 *start_debt_files_build(
	const struct ClusterRecoveryStartV1 *, const ClusterControlRelmapSelectionV2 *);
extern bool start_debt_files_publish(ClusterControlDebtTransitionV2 *);
extern bool start_debt_files_readback_v2(const ClusterControlDebtTransitionV2 *, int, Size);
extern void start_debt_files_damage(unsigned kind);
extern void start_debt_files_reset(void);
extern void start_debt_files_counts(unsigned *read_count, unsigned *write_count, unsigned *sync_count);
extern uint64 start_debt_files_digest(uint8 digest[32]);
extern void start_debt_files_end(ClusterControlDebtTransitionV2 **);
extern void start_debt_files_read_step(void);
#endif
