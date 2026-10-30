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
extern void test_xid_native_clog_start(bool fail, uint16 origin);
extern bool test_xid_native_clog_extend(FullTransactionId candidate);
extern void test_xid_native_clog_state(XidNativeClogState *out);
extern void test_xid_native_clog_stop(void);

/* Optional local native directory beside the default cold-origin fixture. */
typedef enum XidNativeClogTerminalFault
{
	XID_CLOG_TERMINAL_VALID,
	XID_CLOG_TERMINAL_FILE_SYNC,
	XID_CLOG_TERMINAL_DIRECTORY_SYNC,
	XID_CLOG_TERMINAL_SHORT,
	XID_CLOG_TERMINAL_CORRUPT,
	XID_CLOG_TERMINAL_CLOSE,
	XID_CLOG_TERMINAL_OPEN_ERROR
} XidNativeClogTerminalFault;
typedef struct XidNativeClogTerminalState
{
	unsigned calls, writes, wal_flushes, file_syncs, directory_syncs;
	int transient_fds;
	XidStatus disk_status;
	bool file_present, dirty, truncation_held, slru_held, caught;
} XidNativeClogTerminalState;
extern void test_xid_native_clog_use_local_directory(void);
extern void test_xid_native_clog_set_status(TransactionId top, int nchildren,
	TransactionId *children, XidStatus status);
extern bool test_xid_native_clog_terminal(TransactionId xid, XidNativeClogTerminalFault fault,
	void (*sync_hook)(void), ClusterClogNativeTerminalV1 *out, bool *caught);
extern void test_xid_native_clog_terminal_state(TransactionId xid, XidNativeClogTerminalState *out);

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
