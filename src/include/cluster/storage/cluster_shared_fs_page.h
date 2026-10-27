/* Existing cluster_fs PAGE target custody; no recovery authority is granted.
 * Author: SqlRush <sqlrush@gmail.com> */
#ifndef CLUSTER_SHARED_FS_PAGE_H
#define CLUSTER_SHARED_FS_PAGE_H

#include "storage/relfilelocator.h"
#include "storage/block.h"
#include "common/relpath.h"

typedef struct ClusterSharedFsPageV1 ClusterSharedFsPageV1;

/* Reserve the complete preparation peak (handle and native path scratch) from the executor's original
 * budget before PAGE promotion. No memory is allocated after opening files.
 * The caller must close the handle on normal and ERROR exits, before its
 * ResourceOwner is retired. Descriptors are never exported or reopened.
 * Identity/extent loss returns refusal. Read/write errors retain ERROR and
 * sync errors retain the original data_sync_elevel(ERROR) policy. */
extern Size cluster_shared_fs_page_bytes_v1(void);
extern ClusterSharedFsPageV1 *cluster_shared_fs_page_open_v1(
	RelFileLocator locator, BlockNumber block, Size memory_budget);
extern bool cluster_shared_fs_page_current_v1(ClusterSharedFsPageV1 *target);
extern bool cluster_shared_fs_page_read_v1(ClusterSharedFsPageV1 *target,
	ForkNumber fork, BlockNumber block, char *out);
/* The original typed executor must still prove PAGE/source/writer custody
 * on both sides of each operation. Original shared write gates also apply. */
extern bool cluster_shared_fs_page_write_v1(ClusterSharedFsPageV1 *target,
	const char *image);
extern bool cluster_shared_fs_page_sync_v1(ClusterSharedFsPageV1 *target);
extern bool cluster_shared_fs_page_close_v1(ClusterSharedFsPageV1 *target);

#endif
