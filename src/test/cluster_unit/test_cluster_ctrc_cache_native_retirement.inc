/*-------------------------------------------------------------------------
 * test_cluster_ctrc_cache_native_retirement.inc
 *     Original native first/latest source custody at clean cache retirement.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_ctrc_cache_native_retirement.inc
 * NOTES
 *     Native source pool and invalidation tail are real. Pin decrement,
 *     mapping, ownership commit, write completion and authority are controlled.
 *-------------------------------------------------------------------------
 */
static void
native_retirement_ready(void)
{
	ClusterWalSourceRef source = {0};
	BufferDesc *buf;

	fixture();
	source.claim.identity.system_identifier = 17;
	source.claim.identity.storage_uuid[0] = 4;
	source.claim.identity.authority_uuid[0] = 10;
	source.claim.identity.origin_thread_id = 1;
	source.claim.identity.thread_claim_created_at = 99;
	source.claim.identity.origin_owner_incarnation = 7;
	source.claim.identity.root_lineage_seq = 1;
	source.claim.database_incarnation = 3;
	source.claim.max_config_generation = 4;
	source.claim.claim_sha256[0] = 13; source.timeline = 1;
	test_startup_first_native_begin(&source, 0);
	BufferDescriptors = test_startup_first_native_residency(&space, &before, image.data);
	memcpy(blocks[0].data, image.data, BLCKSZ);
	buf = &BufferDescriptors[0].bufferdesc;
	buf->buffer_type = BUF_TYPE_CURRENT; buf->pcm_state = PCM_STATE_N;
	buf->pi_buf_id = buf->cr_chain_head = buf->cr_chain_next = INVALID_BUFFER_ID;
	own.tag = buf->tag; native_retirement = true;
	UT_ASSERT_EQ(test_startup_first_native_references(), 2);
}

static void
native_retirement_end(void)
{
	UT_ASSERT(!locked && InterruptHoldoffCount == 0);
	UT_ASSERT(!(pg_atomic_read_u32(&BufferDescriptors[0].bufferdesc.state) & BM_LOCKED));
	test_startup_first_native_end();
	BufferDescriptors = descriptors; native_retirement = false;
}

UT_TEST(test_ctrc_native_pin_blocks_latest_source_retirement)
{
	BufferDesc *buf;

	native_retirement_ready(); buf = &BufferDescriptors[0].bufferdesc;
	UT_ASSERT(test_startup_first_native_clear(0));
	UT_ASSERT_EQ(test_startup_first_native_references(), 1);
	pg_atomic_fetch_or_u32(&buf->state, BUF_REFCOUNT_ONE);
	UT_ASSERT_EQ(probe(image.data), CLUSTER_CTRC_PAGE_CACHE_WAIT);
	UT_ASSERT(mapping); UT_ASSERT_EQ(evictions, 0);
	UT_ASSERT_EQ(test_startup_first_native_references(), 1);
	/* Controlled original pin owner exits; the recovery gate never unpins. */
	pg_atomic_fetch_sub_u32(&buf->state, BUF_REFCOUNT_ONE);
	UT_ASSERT_EQ(probe(image.data), CLUSTER_CTRC_PAGE_CACHE_ABSENT);
	UT_ASSERT(!mapping); UT_ASSERT_EQ(evictions, 1);
	UT_ASSERT_EQ(native_freelist, 1);
	UT_ASSERT_EQ(test_startup_first_native_references(), 0);
	UT_ASSERT_EQ(probe(NULL), CLUSTER_CTRC_PAGE_CACHE_ABSENT);
	UT_ASSERT_EQ(evictions, 1); /* No double source release on re-observation. */
	UT_ASSERT_EQ(native_freelist, 1);
	native_retirement_end();
}

UT_TEST(test_ctrc_native_first_must_clear_before_residency_can_retire)
{
	BufferDesc *buf;

	native_retirement_ready(); buf = &BufferDescriptors[0].bufferdesc;
	/* Even a controlled clean-state report cannot erase the real first. */
	pg_atomic_fetch_and_u32(&buf->state, ~(BM_DIRTY | BM_JUST_DIRTIED));
	UT_ASSERT_EQ(probe(image.data), CLUSTER_CTRC_PAGE_CACHE_WAIT);
	UT_ASSERT_EQ(first_observations, 1); UT_ASSERT_EQ(evictions, 0);
	UT_ASSERT_EQ(test_startup_first_native_references(), 2);
	UT_ASSERT(test_startup_first_native_clear(0));
	UT_ASSERT_EQ(test_startup_first_native_references(), 1);
	UT_ASSERT_EQ(probe(image.data), CLUSTER_CTRC_PAGE_CACHE_ABSENT);
	UT_ASSERT_EQ(test_startup_first_native_references(), 0);
	native_retirement_end();
}

UT_TEST(test_ctrc_native_refused_image_or_owner_preserves_source)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		native_retirement_ready();
		UT_ASSERT(test_startup_first_native_clear(0));
		if (fault == 0) blocks[0].data[BLCKSZ - 1]++;
		if (fault == 1) own.pcm_state = PCM_STATE_X;
		if (fault == 2) retire_success = false;
		UT_ASSERT_EQ(probe(image.data), fault == 0
			? CLUSTER_CTRC_PAGE_CACHE_REFUSED : CLUSTER_CTRC_PAGE_CACHE_WAIT);
		UT_ASSERT(mapping); UT_ASSERT_EQ(test_startup_first_native_references(), 1);
		UT_ASSERT_EQ(native_freelist, 0);
		UT_ASSERT_EQ(evictions, fault == 2);
		native_retirement_end();
	}
}

UT_TEST(test_ctrc_native_late_owner_refusal_cannot_restore_retired_source)
{
	native_retirement_ready();
	UT_ASSERT(test_startup_first_native_clear(0)); drift_after_release = true;
	UT_ASSERT_EQ(probe(image.data), CLUSTER_CTRC_PAGE_CACHE_REFUSED);
	UT_ASSERT(!mapping); UT_ASSERT_EQ(evictions, 1);
	UT_ASSERT_EQ(test_startup_first_native_references(), 0);
	UT_ASSERT_EQ(probe(image.data), CLUSTER_CTRC_PAGE_CACHE_REFUSED);
	UT_ASSERT_EQ(test_startup_first_native_references(), 0);
	native_retirement_end();
}
