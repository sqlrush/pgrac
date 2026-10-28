/*-------------------------------------------------------------------------
 * test_cluster_xid_native_clog.h
 *     Observe the original native FULL CLOG producer.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_xid_native_clog.h
 * NOTES
 *     Original allocator and CLOG/SLRU bodies execute. Locks, lease and WAL
 *     insertion are controlled; no lifetime or completion authority.
 *-------------------------------------------------------------------------
 */
#ifndef TEST_CLUSTER_XID_NATIVE_CLOG_H
#define TEST_CLUSTER_XID_NATIVE_CLOG_H
#include "access/clog.h"
typedef struct XidNativeClogState
{
	unsigned calls, begins, inserts, dirty_pages;
	uint32 length;
	uint8 info;
	xl_clog_zeropage_full bytes;
	bool xid_held, slru_held, page_zero;
} XidNativeClogState;
extern void test_xid_native_clog_start(bool fail);
extern bool test_xid_native_clog_extend(FullTransactionId candidate);
extern void test_xid_native_clog_state(XidNativeClogState *out);
extern void test_xid_native_clog_stop(void);

typedef enum XidNativeClogReplayFault
{
	XID_CLOG_REPLAY_VALID,
	XID_CLOG_REPLAY_RESERVED,
	XID_CLOG_REPLAY_PAGE,
	XID_CLOG_REPLAY_SHORT,
	XID_CLOG_REPLAY_BLOCKREF
} XidNativeClogReplayFault;
typedef struct XidNativeClogReplayState
{
	unsigned writes, inserts;
	int64 file_size;
	bool io_ok, file_existed, caught, target_zero, target_sentinel, neighbor_sentinel;
} XidNativeClogReplayState;
extern void test_xid_native_clog_replay(XidNativeClogReplayFault fault,
									 XidNativeClogReplayState *out);
#endif
