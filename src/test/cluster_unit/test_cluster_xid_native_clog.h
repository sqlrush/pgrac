/*-------------------------------------------------------------------------
 *
 * test_cluster_xid_native_clog.h
 *    Native CLOG test boundary for retained allocation.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_xid_native_clog.h
 * NOTES
 *    Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md
 *
 *-------------------------------------------------------------------------
 */
#ifndef TEST_CLUSTER_XID_NATIVE_CLOG_H
#define TEST_CLUSTER_XID_NATIVE_CLOG_H
#include "access/clog.h"

typedef struct TestXidNativeClogState {
	xl_clog_zeropage_full birth;
	uint32 length;
	uint8 info;
	unsigned calls, inserts, writes;
	bool page_zero, dirty;
} TestXidNativeClogState;

extern void test_xid_native_clog_start(int origin);
extern void test_xid_native_clog_extend(FullTransactionId full);
extern void test_xid_native_clog_state(int page, TestXidNativeClogState *out);
extern bool test_xid_native_clog_writeback(int page);
extern void test_xid_native_clog_stop(void);
#endif
