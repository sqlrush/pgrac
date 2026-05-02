/*-------------------------------------------------------------------------
 *
 * test_cluster_buffer_desc.c
 *	  Compile-time + link-time invariants for the BufferDesc cluster
 *	  fields introduced at stage 1.6.
 *
 *	  Stage 1.6 ships only the cluster_buffer_desc.h enums + INVALID_*
 *	  sentinels + the inline ClusterInitBufferDescFields helper that
 *	  writes 17 placeholder values at InitBufferPool / InitLocalBuffers
 *	  time.  The actual PCM / CR / PI / Cache Fusion / GRD master cache
 *	  semantics land at Stage 2-3.  This binary covers only the layout /
 *	  sizeof / sentinel-value invariants.
 *
 *	  PIVOT B (2026-05-02): PG 16.13 sizeof(BufferTag) == 20 (not 16),
 *	  pushing PG-original fields to offset 52 and leaving 12B of cache
 *	  line 1 for cluster hot tail.  block_scn must stay in cache line 1
 *	  (Stage 2-3 visibility hot path); cr_chain_head moved to cache
 *	  line 2 boundary.  Tests assert the resulting layout via semantic
 *	  constraints (not magic offset numbers).
 *
 *	  Spec: spec-1.6-buffer-descriptor.md §1.2 Deliverable 6 + §4.1
 *	  Design: docs/buffer-pool-design.md v1.2 §4.3
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_buffer_desc.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Each test_*.c is a standalone executable; see unit_test.h.
 *	  cluster_buffer_desc.h is header-only at stage 1.6 (the inline
 *	  helper lives in buf_internals.h after the BufferDesc struct
 *	  definition); no .o file to link.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <stddef.h> /* offsetof */

#include "cluster/cluster_buffer_desc.h"
#include "storage/buf_internals.h"

/* Drop PG's port.h printf -> pg_printf override; unit_test.h uses
 * stdlib printf and we don't link libpgport in this test binary. */
#undef printf

#include "unit_test.h"


UT_DEFINE_GLOBALS();


UT_TEST(test_buffer_desc_size_within_padded_size)
{
	/*
	 * sizeof(BufferDesc) MUST fit in BUFFERDESC_PAD_TO_SIZE.  This is
	 * the cornerstone invariant of stage 1.6; PG 16.13 with cluster
	 * fields packs into 128B padded slot (BUFFERDESC_PAD_TO_SIZE
	 * 64 -> 128 in USE_PGRAC_CLUSTER mode).  Same invariant locked by
	 * the StaticAssertDecl in buf_internals.h; this runtime check is
	 * defense-in-depth.
	 */
	UT_ASSERT((int)sizeof(BufferDesc) <= BUFFERDESC_PAD_TO_SIZE);
	UT_ASSERT_EQ(BUFFERDESC_PAD_TO_SIZE, 128);
}


UT_TEST(test_block_scn_stays_in_cache_line_1)
{
	/*
	 * Semantic invariant (spec-1.6 PIVOT B): block_scn (8B SCN; visibility
	 * hot field, read on every Stage 2-3 buffer access) must end at or
	 * before offset 64 = cache line 1 boundary.  PG 16.13 实测: starts
	 * at 56, ends at 64.  Any future field reorder that pushes block_scn
	 * past 56 (its end past 64) breaks the hot path.
	 */
	UT_ASSERT((int)(offsetof(BufferDesc, block_scn) + sizeof(SCN)) <= 64);
}


UT_TEST(test_cr_chain_head_starts_cache_line_2)
{
	/*
	 * Semantic invariant (spec-1.6 PIVOT B): cr_chain_head (cold; CR
	 * construction path only) starts cache line 2 (offset >= 64).  This
	 * locks the boundary between hot tail and cold body.
	 */
	UT_ASSERT((int)offsetof(BufferDesc, cr_chain_head) >= 64);
}


UT_TEST(test_cluster_fields_follow_content_lock)
{
	/*
	 * Semantic invariant (spec-1.6 §8 Q2 user 修订): cluster fields
	 * must come *after* PG-original content_lock so bufmgr.c:3275
	 * AssertNotCatalogBufferLock reverse-deref keeps yielding a valid
	 * BufferDesc pointer.  The reverse-deref uses offsetof() so it
	 * adapts to whatever PG version's content_lock offset is; we just
	 * lock the relative ordering here.
	 */
	UT_ASSERT((int)offsetof(BufferDesc, content_lock) < (int)offsetof(BufferDesc, buffer_type));
}


UT_TEST(test_buffer_type_zero_init_is_current)
{
	/*
	 * BufferType enum MUST have BUF_TYPE_CURRENT = 0 so MemSet /
	 * calloc zero-fill puts every buffer into "current" state without
	 * explicit assignment.  ClusterInitBufferDescFields still writes
	 * explicitly per spec-1.6 §8 Q5 = B, but the zero-init invariant
	 * is what makes Stage 2/3 incremental work safe.
	 */
	UT_ASSERT_EQ((int)BUF_TYPE_CURRENT, 0);
}


UT_TEST(test_pcm_state_zero_init_is_n)
{
	/*
	 * PcmState enum MUST have PCM_STATE_N = 0.  Zero-init occupies
	 * "no PCM lock held" semantics matching PG-native behavior --
	 * Stage 2 PCM 真值激活 starts here.
	 */
	UT_ASSERT_EQ((int)PCM_STATE_N, 0);
}


UT_TEST(test_cf_state_zero_init_is_none)
{
	/*
	 * CacheFusionState enum MUST have CF_STATE_NONE = 0.  Zero-init
	 * occupies "static / no transfer in progress" semantics.
	 */
	UT_ASSERT_EQ((int)CF_STATE_NONE, 0);
}


UT_TEST(test_invalid_buffer_id_and_node_id_sentinels)
{
	/*
	 * INVALID_BUFFER_ID = -1 (matches PG's FREENEXT_END_OF_LIST).
	 * INVALID_NODE_ID = 0 (uint16 cannot hold -1 sentinel; pgrac
	 * caps node id 1..16 per AD-012 例外 10 so 0 is reserved for
	 * "未分配 master").
	 *
	 * These are CRITICAL: cr_chain_head / cr_chain_next / pi_buf_id
	 * occupy "absent" via -1 not 0 (0 is a valid buffer_id).
	 * grd_master_node uses 0 because uint16 cannot represent -1.
	 */
	UT_ASSERT_EQ((int)INVALID_BUFFER_ID, -1);
	UT_ASSERT_EQ((int)INVALID_NODE_ID, 0);
}


int
main(void)
{
	UT_PLAN(8);
	UT_RUN(test_buffer_desc_size_within_padded_size);
	UT_RUN(test_block_scn_stays_in_cache_line_1);
	UT_RUN(test_cr_chain_head_starts_cache_line_2);
	UT_RUN(test_cluster_fields_follow_content_lock);
	UT_RUN(test_buffer_type_zero_init_is_current);
	UT_RUN(test_pcm_state_zero_init_is_n);
	UT_RUN(test_cf_state_zero_init_is_none);
	UT_RUN(test_invalid_buffer_id_and_node_id_sentinels);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
