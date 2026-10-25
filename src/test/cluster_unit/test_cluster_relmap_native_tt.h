/* Original canonical ACTIVE owner for the relmap producer composition.
 * Native WAL, admission, block authority and physical TT leaves are controlled.
 * Author: SqlRush <sqlrush@gmail.com> */
#ifndef TEST_CLUSTER_RELMAP_NATIVE_TT_H
#define TEST_CLUSTER_RELMAP_NATIVE_TT_H
#include "postgres.h"
#include "cluster/cluster_tt_local.h"

typedef struct TestRelmapNativeTTStats
{
	unsigned bindings, bind_records, ctrc_opens;
	bool balanced, assertion_failed;
} TestRelmapNativeTTStats;

extern void test_relmap_native_tt_reset(void);
extern bool test_relmap_native_tt_publish(TransactionId xid);
extern bool test_relmap_native_tt_lookup(TransactionId xid, ClusterCanonicalTxnBinding *out);
extern TestRelmapNativeTTStats test_relmap_native_tt_stats(void);
#endif
