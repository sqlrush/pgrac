/* Original Start fixture bridge; no production interface.
 * Author: SqlRush <sqlrush@gmail.com> */
#ifndef TEST_CLUSTER_RELMAP_ORIGINAL_START_H
#define TEST_CLUSTER_RELMAP_ORIGINAL_START_H
#include "cluster/cluster_cold_recovery.h"
#include "cluster/cluster_recovery_plan.h"
#include "cluster/cluster_wal_writer.h"
extern const struct ClusterRecoveryStartV1 *test_relmap_start_begin(
	ClusterColdTypedV1 *input, int pid);
extern bool test_relmap_start_current(const struct ClusterRecoveryStartV1 *start);
extern void test_relmap_start_fail(const struct ClusterRecoveryStartV1 *start);
extern const ClusterColdTypedV1 *test_relmap_start_inputs(const struct ClusterRecoveryStartV1 *start);
extern bool test_relmap_start_owner(const struct ClusterRecoveryStartV1 *start,
	ClusterRecoveryCheckpointOwnerV1 *owner);
extern bool test_relmap_start_writer(ClusterWalWriterToken *writer, uint8 operation[16], uint64 *generation);
extern bool test_relmap_start_distinct(const struct ClusterRecoveryStartV1 *start, const void *out, Size bytes);
extern bool test_relmap_start_pending(void);
extern void test_relmap_start_fault(unsigned fault);
extern bool test_relmap_start_done(void);
#endif
