/*-------------------------------------------------------------------------
 * test_cluster_recovery_episode_clog.h
 *     Native CLOG fixture boundary for original recovery episode tests.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_recovery_episode_clog.h
 * NOTES
 *     The native fixture owns its private handle type. No native authority.
 *-------------------------------------------------------------------------
 */
#ifndef TEST_CLUSTER_RECOVERY_EPISODE_CLOG_H
#define TEST_CLUSTER_RECOVERY_EPISODE_CLOG_H
#include "access/clog.h"
#include "cluster/cluster_wal_inputs.h"
extern size_t test_episode_native_clog_bytes(void);
extern bool test_episode_native_clog_read(const ClusterRecoveryRelmapSourceV1 *source,
	uint64 full, ClusterRelmapClogBaseV1 *out, void **held);
extern void test_episode_native_clog_release(void **held);
extern void test_episode_native_clog_start(const char *root);
extern void test_episode_native_clog_stop(void);
extern void test_episode_native_clog_error(bool on);
extern int test_episode_native_clog_fd(void *held);
extern void test_episode_native_clog_redo(XLogReaderState *reader);
/* Fixture outcome through original CLOG storage, not XACT/TT authority. */
extern void test_episode_native_clog_set_committed(TransactionId xid);
extern void test_episode_native_clog_write_all(void);
extern void test_episode_native_clog_local_bounds(bool on, uint64 next, TransactionId oldest);
extern void test_episode_native_clog_advance_next(TransactionId xid);
extern void test_episode_native_clog_advance_oldest(TransactionId xid);
extern void test_episode_native_clog_discard_resident(void);
/* Actual cold native initialization; original episode authority is a fixture. */
extern bool test_episode_native_clog_full_apply(const ClusterRecoveryRelmapSourceV1 *source,
	const xl_clog_zeropage_full *birth);
#endif
