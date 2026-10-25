/* Original dual-copy file I/O beside the original producer.
 * Author: SqlRush <sqlrush@gmail.com> */
#ifndef TEST_CLUSTER_RELMAP_PHYSICAL_PAIR_H
#define TEST_CLUSTER_RELMAP_PHYSICAL_PAIR_H
#include "cluster/cluster_relmap_authority.h"

enum TestRelmapPhysicalEvent
{
	RELMAP_PHYSICAL_READ, RELMAP_PHYSICAL_WRITE, RELMAP_PHYSICAL_FILE_SYNC,
	RELMAP_PHYSICAL_DIRECTORY_SYNC, RELMAP_PHYSICAL_RENAME, RELMAP_PHYSICAL_CLOSE
};
typedef struct TestRelmapPhysicalStats
{
	unsigned reads, writes, file_syncs, directory_syncs, renames;
} TestRelmapPhysicalStats;

extern void test_relmap_physical_reset(const char *root, void (*observe)(unsigned));
extern void test_relmap_physical_fail_sync(bool directory, unsigned call);
extern TestRelmapPhysicalStats test_relmap_physical_stats(void);
extern ClusterRelmapV2PairResult test_relmap_physical_v2_read(
	const ClusterRelmapV2Namespace *, ClusterRelmapV2View *);
extern bool test_relmap_physical_v2_check_staged(const ClusterRelmapV2Image *);
extern bool test_relmap_physical_v2_write_pending(const ClusterRelmapV2Namespace *, uint64,
	const ClusterRelmapV2Owner *, const RelMapFile *, ClusterRelmapRetentionDirectory *, LWLock *);
extern ClusterRelmapV2ClearResult test_relmap_physical_v2_publish(const ClusterRelmapV2Image *);
extern ClusterRelmapV2ClearResult test_relmap_physical_v2_discard_pending(const ClusterRelmapV2Image *);
#endif
