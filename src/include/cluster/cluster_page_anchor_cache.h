/*-------------------------------------------------------------------------
 *
 * cluster_page_anchor_cache.h
 *    STOP-06 process-local anchor-known cache.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_PAGE_ANCHOR_CACHE_H
#define CLUSTER_PAGE_ANCHOR_CACHE_H

#include "cluster/cluster_page_stable_base.h"

#define CLUSTER_PAGE_ANCHOR_CACHE_INTERFACE_V1 1
#define RF_PAGE_ANCHOR_CACHE_CAPACITY 256

typedef struct RfPageAnchorCacheKeyV1 {
	RfPageIdentityV1 page_identity;
	uint8 segment_incarnation[16];
	uint8 root_lineage[16];
	uint64 checkpoint_epoch;
	uint64 fpw_epoch;
} RfPageAnchorCacheKeyV1;

/* true means the caller must add REGBUF_FORCE_IMAGE. */
extern bool rf_page_anchor_cache_key_valid_v1(const RfPageAnchorCacheKeyV1 *key);
extern bool rf_page_anchor_cache_block_matches_v1(const RfPageAnchorCacheKeyV1 *key,
												  const RelFileLocator *locator, ForkNumber forknum,
												  BlockNumber blockno);
extern bool rf_page_anchor_cache_incarnation_matches_v1(const RfPageAnchorCacheKeyV1 *key,
														const uint8 result_incarnation[16]);
extern bool rf_page_anchor_cache_should_force_v1(const RfPageAnchorCacheKeyV1 *key);
extern uint8 rf_page_anchor_cache_registration_flags_v1(const RfPageAnchorCacheKeyV1 *key,
														uint8 caller_flags);

/*
 * Cache credit is granted only after a successful WAL insertion that
 * actually emitted an APPLY image.  A full cache returns false and safely
 * leaves this key as a miss.
 */
extern bool rf_page_anchor_cache_record_v1(const RfPageAnchorCacheKeyV1 *key, bool insert_succeeded,
										   bool apply_image_emitted);
extern void rf_page_anchor_cache_forget_all_v1(void);

#endif /* CLUSTER_PAGE_ANCHOR_CACHE_H */
