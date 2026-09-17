/*-------------------------------------------------------------------------
 *
 * cluster_page_anchor_cache.c
 *    STOP-06 process-local anchor-known cache.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xloginsert.h"
#include "cluster/cluster_page_anchor_cache.h"

typedef struct RfPageAnchorCacheEntryV1 {
	RfPageAnchorCacheKeyV1 key;
	bool valid;
} RfPageAnchorCacheEntryV1;

static RfPageAnchorCacheEntryV1 anchor_cache[RF_PAGE_ANCHOR_CACHE_CAPACITY];

static bool
bytes_nonzero(const uint8 *bytes, size_t length)
{
	uint8 any = 0;
	size_t i;

	for (i = 0; i < length; i++)
		any |= bytes[i];
	return any != 0;
}

bool
rf_page_anchor_cache_key_valid_v1(const RfPageAnchorCacheKeyV1 *key)
{
	return key != NULL && rf_page_identity_valid_v1(&key->page_identity)
		   && bytes_nonzero(key->segment_incarnation, 16) && bytes_nonzero(key->root_lineage, 16)
		   && key->checkpoint_epoch != 0 && key->fpw_epoch != 0;
}

bool
rf_page_anchor_cache_block_matches_v1(const RfPageAnchorCacheKeyV1 *key,
									  const RelFileLocator *locator, ForkNumber forknum,
									  BlockNumber blockno)
{
	return rf_page_anchor_cache_key_valid_v1(key) && locator != NULL
		   && RelFileLocatorEquals(key->page_identity.locator, *locator)
		   && key->page_identity.forknum == (uint32)forknum
		   && key->page_identity.blockno == blockno;
}

bool
rf_page_anchor_cache_incarnation_matches_v1(const RfPageAnchorCacheKeyV1 *key,
											const uint8 result_incarnation[16])
{
	return rf_page_anchor_cache_key_valid_v1(key) && result_incarnation != NULL
		   && memcmp(key->segment_incarnation, result_incarnation, 16) == 0;
}

static bool
key_equal(const RfPageAnchorCacheKeyV1 *left, const RfPageAnchorCacheKeyV1 *right)
{
	return rf_page_identity_equal_v1(&left->page_identity, &right->page_identity)
		   && memcmp(left->segment_incarnation, right->segment_incarnation, 16) == 0
		   && memcmp(left->root_lineage, right->root_lineage, 16) == 0
		   && left->checkpoint_epoch == right->checkpoint_epoch
		   && left->fpw_epoch == right->fpw_epoch;
}

bool
rf_page_anchor_cache_should_force_v1(const RfPageAnchorCacheKeyV1 *key)
{
	int i;

	if (!rf_page_anchor_cache_key_valid_v1(key))
		return true;
	for (i = 0; i < RF_PAGE_ANCHOR_CACHE_CAPACITY; i++)
		if (anchor_cache[i].valid && key_equal(&anchor_cache[i].key, key))
			return false;
	return true;
}

uint8
rf_page_anchor_cache_registration_flags_v1(const RfPageAnchorCacheKeyV1 *key, uint8 caller_flags)
{
	if (rf_page_anchor_cache_should_force_v1(key))
		caller_flags |= REGBUF_FORCE_IMAGE;
	return caller_flags;
}

bool
rf_page_anchor_cache_record_v1(const RfPageAnchorCacheKeyV1 *key, bool insert_succeeded,
							   bool apply_image_emitted)
{
	int empty = -1;
	int i;

	if (!insert_succeeded || !apply_image_emitted || !rf_page_anchor_cache_key_valid_v1(key))
		return false;
	for (i = 0; i < RF_PAGE_ANCHOR_CACHE_CAPACITY; i++) {
		if (anchor_cache[i].valid) {
			if (key_equal(&anchor_cache[i].key, key))
				return true;
		} else if (empty < 0)
			empty = i;
	}
	if (empty < 0)
		return false;
	anchor_cache[empty].key = *key;
	anchor_cache[empty].valid = true;
	return true;
}

void
rf_page_anchor_cache_forget_all_v1(void)
{
	memset(anchor_cache, 0, sizeof(anchor_cache));
}
