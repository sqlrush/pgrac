/* Original native cache predecessor of a bufferless CTRC recovery FPI.
 * Author: SqlRush <sqlrush@gmail.com> */
#ifndef CLUSTER_PAGE_BUFFER_CTRC_H
#define CLUSTER_PAGE_BUFFER_CTRC_H

#include "cluster/cluster_page_authority_ctrc.h"
#include "cluster/cluster_space_identity.h"

struct ClusterRecoveryStartV1;
struct ClusterColdCtrcWorkV1;

typedef enum ClusterCtrcPageCacheResultV1 {
	CLUSTER_CTRC_PAGE_CACHE_REFUSED = 0,
	CLUSTER_CTRC_PAGE_CACHE_WAIT,
	CLUSTER_CTRC_PAGE_CACHE_ABSENT
} ClusterCtrcPageCacheResultV1;

/* Same accepted original Start/work/scope. A non-NULL
 * before image permits only the original native invalidation of an exact,
 * clean, unpinned CURRENT/N predecessor with no first/PI/CR/CF/ownership duty.
 * Every byte except its independently validated storage checksum must match.
 * The mutation requires the exact promoted native PAGE before/after. NULL is
 * an absence observation only; it never removes a mapping and may run in the
 * original off-PAGE preparation gap. It supplies no PAGE authority.
 * Mapping acquisition is conditional, without allocation, I/O, pin or remote
 * grant/release. Busy native owners retain all obligations. Both native locks
 * are released before the final original authority check. ABSENT is only a
 * local observation at this call, never a future cache/IO/discharge authority.
 * False/ERROR grants no window completion or borrowed-owner retirement. */
extern ClusterCtrcPageCacheResultV1 cluster_bufmgr_ctrc_page_cache_v1(
	const struct ClusterRecoveryStartV1 *start, const struct ClusterColdCtrcWorkV1 *work,
	const RfPageCtrcAuthorityV1 *authority, const ClusterSpaceIdentity *space,
	BlockNumber blockno, const RfPageVersionV1 *before, const char *before_image);

#endif
