/* Native CLOG test bridge; no production interface.
 * Author: SqlRush <sqlrush@gmail.com> */
#ifndef TEST_CLUSTER_RELMAP_NATIVE_CLOG_H
#define TEST_CLUSTER_RELMAP_NATIVE_CLOG_H
#include "access/clog.h"

typedef struct TestRelmapNativeClogStats {
	unsigned calls, file_syncs, directory_syncs;
	int open_fds, assertion_failed;
} TestRelmapNativeClogStats;

extern void test_relmap_native_clog_setup(const char *root, int origin, TransactionId xid,
										  XidStatus status, uint8 last_byte);
extern bool test_relmap_native_clog_read(int origin, TransactionId xid,
										 ClusterClogNativeBaseV1 *out, sigjmp_buf *outer_stack);
/* Faults: 1 missing,2 short,3 file sync,4 directory sync,5 directory ERROR,
 * 6 different last page byte. Trigger at call0 before the physical census,
 * or immediately before the selected original native read. */
extern void test_relmap_native_clog_fault(unsigned fault, unsigned on_call);
extern TestRelmapNativeClogStats test_relmap_native_clog_stats(void);
extern TestRelmapNativeClogStats test_relmap_native_clog_done(void);
#endif
