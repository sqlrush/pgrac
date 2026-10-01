/*-------------------------------------------------------------------------
 * cluster_page_data.h
 *    Exact DATA completion from the resident current holder.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_PAGE_DATA_H
#define CLUSTER_PAGE_DATA_H

#include "cluster/cluster_page_stable_base.h"

typedef struct ClusterPageDataTargetV1 {
	uint64 database_incarnation;
	RfPageIdentityV1 identity;
	RfPageVersionV1 version;
} ClusterPageDataTargetV1;

typedef struct ClusterPageDataReceiptV1 ClusterPageDataReceiptV1;

/* Background write endpoint, with no caller buffer locks. Borrows only a
 * resident, unfenced current-X; resident SPACE0 stays locked across write,
 * fsync and exact post-read. Busy/missing/stale returns false, never takes
 * ownership or creates storage. This initial endpoint requires the page's
 * explicit WAL origin to be the selected local writer. Foreign WAL requires
 * its original contribution owner, not a comparison with local LSNs.
 *
 * On success *out (NULL on entry) owns a receipt in CurrentMemoryContext.
 * Failure leaves *out unchanged. ERROR releases this call's locks/pins/I/O
 * and propagates. A receipt proves only the exact requested DATA version;
 * it never grants checkpoint publication or WAL retirement. */
extern bool cluster_bufmgr_write_page_data_v1(const ClusterPageDataTargetV1 *target,
											  ClusterPageDataReceiptV1 **out);
extern bool cluster_page_data_receipt_read_v1(const ClusterPageDataReceiptV1 *receipt,
											  ClusterPageDataTargetV1 *out);
extern void cluster_page_data_receipt_free_v1(ClusterPageDataReceiptV1 **receipt);

#endif /* CLUSTER_PAGE_DATA_H */
