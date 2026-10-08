/* Author: SqlRush <sqlrush@gmail.com>
 * Exercise the complete production cache router with the real legacy L1.
 * Only the constructor and L2 boundaries are scripted and counted.
 * Portions Copyright (c) 2026, pgrac contributors
 */
int legacy_cache_fixture_main(int argc, char **argv);
#define main legacy_cache_fixture_main
#include "test_cluster_cr_cache.c"
#undef main
#include "cluster/cluster_cr.h"
#include "cluster/cluster_cr_pool.h"
#include "cluster/cluster_cr_admit.h"
#include "utils/elog.h"

bool cluster_shared_config;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
static sigjmp_buf route_error;
static int key_reads, pool_reads, constructs;
static bool fail_construct;
static uint64 pool_epoch;
static char scratch[BLCKSZ];
static struct {
	pg_atomic_uint64 cr_cache_hit_count;
	pg_atomic_uint64 cr_cache_miss_count;
	pg_atomic_uint64 cr_cache_evict_count;
	pg_atomic_uint64 cr_cache_install_count;
} *CRShared;

void
pg_re_throw(void)
{
	if (PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	siglongjmp(route_error, 1);
}

static ClusterCRCacheKey
cr_build_cache_key(Buffer buf, SCN read)
{
	UT_ASSERT_EQ(buf, 1);
	key_reads++;
	return mk_key(100, 0, read, 10);
}

static const char *
cluster_cr_construct_block_into(Buffer buf, SCN read, char *dst)
{
	UT_ASSERT_EQ(buf, 1);
	UT_ASSERT_EQ(read, 50);
	constructs++;
	if (fail_construct)
		pg_re_throw();
	memset(dst, 'N', BLCKSZ);
	return dst;
}
const char *
cluster_cr_construct_block(Buffer buf, SCN read)
{
	return cluster_cr_construct_block_into(buf, read, scratch);
}
static void
cr_note_retention_if_advanced(SCN read)
{}
uint64
cluster_cr_pool_current_epoch(void)
{
	pool_reads++;
	return pool_epoch;
}
bool
cluster_cr_pool_rel_generation_enabled(void)
{
	pool_reads++;
	return false;
}
bool
cluster_cr_pool_rel_generation(RelFileLocator loc, uint64 *gen)
{
	UT_ASSERT(false);
	return false;
}
bool
cluster_cr_pool_register_locator(RelFileLocator loc, uint64 *gen)
{
	UT_ASSERT(false);
	return false;
}
bool
cluster_cr_pool_lookup_copy_gen(const ClusterCRCacheKey *key, char *dst, uint64 *gen)
{
	pool_reads++;
	return false;
}
bool
cluster_cr_pool_reserve_gen(const ClusterCRCacheKey *key, uint64 gen, ClusterCRPoolHandle *h)
{
	pool_reads++;
	return false;
}
void
cluster_cr_pool_publish(const ClusterCRPoolHandle *h, const char *page)
{
	UT_ASSERT(false);
}
void
cluster_cr_pool_abort(const ClusterCRPoolHandle *h)
{
	UT_ASSERT(false);
}
void
cluster_cr_pool_note_l1_epoch_mismatch(void)
{
	pool_reads++;
}
void
cluster_cr_pool_note_base_lsn_mismatch(void)
{
	pool_reads++;
}
void
cluster_cr_pool_note_key_mismatch(void)
{
	pool_reads++;
}
bool
cluster_cr_pool_admit(const ClusterCRCacheKey *key, const ClusterCRAdmitCtx *ctx)
{
	pool_reads++;
	return false;
}
ClusterCRScanKind
cluster_cr_admit_current_scan_kind(void)
{
	return 0;
}
ClusterCRAdmitReason
cluster_cr_admit_last_reason(void)
{
	return 0;
}
void
cluster_cr_admit_stat_bump(ClusterCRAdmitReason reason)
{}
void
cluster_cr_admit_note_published(const ClusterCRCacheKey *key)
{
	UT_ASSERT(false);
}
#include "test_cluster_cr_shared_route.inc"

static void
route_reset(bool shared, uint64 epoch)
{
	cluster_cr_cache_max_blocks = 8;
	cluster_cr_cache_reset();
	cluster_shared_config = shared;
	pool_epoch = epoch;
	key_reads = pool_reads = constructs = 0;
	fail_construct = false;
}

UT_TEST(test_shared_hot_legacy_entry_is_never_consumed)
{
	ClusterCRCacheKey key = mk_key(100, 0, 50, 10);
	route_reset(true, 0);
	install(&key, 'O');
	UT_ASSERT_EQ(cluster_cr_lookup_or_construct(1, 50)[0], 'N');
	UT_ASSERT_EQ(constructs, 1);
	UT_ASSERT_EQ(key_reads, 0);
	UT_ASSERT_EQ(pool_reads, 0);
	UT_ASSERT_EQ(cluster_cr_cache_lookup(&key, 0, NULL)[0], 'O');
}
UT_TEST(test_shared_never_installs_private_cache_with_l2_enabled)
{
	ClusterCRCacheKey key = mk_key(100, 0, 50, 10);
	route_reset(true, 7);
	for (int i = 0; i < 3; i++)
		UT_ASSERT_EQ(cluster_cr_lookup_or_construct(1, 50)[0], 'N');
	UT_ASSERT_EQ(constructs, 3);
	UT_ASSERT_EQ(key_reads, 0);
	UT_ASSERT_EQ(pool_reads, 0);
	UT_ASSERT(cluster_cr_cache_lookup(&key, 7, NULL) == NULL);
}
UT_TEST(test_shared_construction_error_keeps_failure_and_no_private_entry)
{
	ClusterCRCacheKey key = mk_key(100, 0, 50, 10);
	volatile bool caught = false;
	route_reset(true, 0);
	fail_construct = true;
	if (sigsetjmp(route_error, 0) == 0)
		(void)cluster_cr_lookup_or_construct(1, 50);
	else
		caught = true;
	UT_ASSERT(caught);
	UT_ASSERT_EQ(key_reads, 0);
	UT_ASSERT_EQ(pool_reads, 0);
	UT_ASSERT(cluster_cr_cache_lookup(&key, 0, NULL) == NULL);
	fail_construct = false;
	UT_ASSERT_EQ(cluster_cr_lookup_or_construct(1, 50)[0], 'N');
	UT_ASSERT_EQ(constructs, 2);
}
UT_TEST(test_nonshared_keeps_original_construct_then_cache_hit)
{
	route_reset(false, 0);
	for (int i = 0; i < 3; i++)
		UT_ASSERT_EQ(cluster_cr_lookup_or_construct(1, 50)[0], 'N');
	UT_ASSERT_EQ(constructs, 1);
	UT_ASSERT_EQ(key_reads, 3);
	UT_ASSERT(pool_reads > 0);
}
int
main(void)
{
	UT_PLAN(4);
	UT_RUN(test_shared_hot_legacy_entry_is_never_consumed);
	UT_RUN(test_shared_never_installs_private_cache_with_l2_enabled);
	UT_RUN(test_shared_construction_error_keeps_failure_and_no_private_entry);
	UT_RUN(test_nonshared_keeps_original_construct_then_cache_hit);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
