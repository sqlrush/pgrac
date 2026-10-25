/* Original canonical ACTIVE owner for the relmap producer composition.
 * Native WAL, admission, block authority and physical TT leaves are controlled.
 * Author: SqlRush <sqlrush@gmail.com> */
#ifndef TEST_CLUSTER_RELMAP_NATIVE_TT_H
#define TEST_CLUSTER_RELMAP_NATIVE_TT_H
#include "postgres.h"
#include "access/xact.h"
#include "cluster/cluster_tt_local.h"
#include "cluster/cluster_tt_slot.h"

typedef struct TestRelmapNativeTTStats
{
	unsigned bindings, bind_records, ctrc_opens;
	bool balanced, assertion_failed;
	bool commit_admission, commit_reservation;
} TestRelmapNativeTTStats;

typedef struct TestRelmapNativeTTTerminal
{
	TTSlot resident, written;
	ClusterTTSlotCurrentOwner allocator;
	xl_xact_tt_commit fold;
	unsigned writes, abort_records;
	int error_level;
	bool allocator_present, flushed, write_before_flush;
} TestRelmapNativeTTTerminal;

extern void test_relmap_native_tt_reset(void);
extern void test_relmap_native_tt_observe_active(bool (*observer)(TransactionId));
extern bool test_relmap_native_tt_publish(TransactionId xid);
extern bool test_relmap_native_tt_lookup(TransactionId xid, ClusterCanonicalTxnBinding *out);
extern TestRelmapNativeTTStats test_relmap_native_tt_stats(void);
extern bool test_relmap_native_tt_terminal(const ClusterCanonicalTxnBinding *binding,
	bool commit, bool fail_write, TestRelmapNativeTTTerminal *out);
extern bool test_relmap_native_tt_stage_commit(const ClusterCanonicalTxnBinding *binding,
	TestRelmapNativeTTTerminal *out);
#endif
