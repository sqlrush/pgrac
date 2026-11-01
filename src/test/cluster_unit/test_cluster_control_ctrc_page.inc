/*-------------------------------------------------------------------------
 *
 * test_cluster_control_ctrc_page.inc
 *     Protect exact CTRC targets under the original native replay set.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_control_ctrc_page.inc
 *
 * NOTES
 *     Native set, graph and PAGE primitives execute. Distributed GES/Start
 *     and decoded graph edges remain controlled original fixture leaves.
 *
 *-------------------------------------------------------------------------
 */
#include "cluster/cluster_page_authority_ctrc.h"

typedef struct CtrcPageFixture {
	CtrcReplayStableFixture replay;
	RfPageStableCtrcBuildV1 proof;
	RfPageAuthorityTargetV1 target;
	RfPageCtrcAuthorityBuildV1 page;
} CtrcPageFixture;

static void
ctrc_page_fixture(CtrcPageFixture *f)
{
	memset(f, 0, sizeof(*f)); rf_page_guard_shmem_init_v1();
	ctrc_replay_stable_fixture(&f->replay);
	f->proof = ctrc_replay_stable_build(&f->replay);
	UT_ASSERT_EQ(f->proof.detail, RF_PAGE_PROOF_DETAIL_OK);
	f->target.page_identity = f->replay.graph.page_identity;
	f->target.expected_result = f->replay.graph.expected_result;
	f->target.stable_base = f->proof.proof; f->target.source = &f->replay.source;
	f->target.contributors = &f->replay.vector;
}

static RfPageCtrcAuthorityBuildV1
ctrc_page_build(CtrcPageFixture *f)
{
	return rf_page_authority_ctrc_replays_preflight_v1(&f->target, 1,
		f->replay.owners, 2, 1024 * 1024);
}

static void
ctrc_page_finish(CtrcPageFixture *f)
{
	rf_page_authority_ctrc_replays_destroy_v1(&f->page.authority);
	rf_page_stable_base_proof_destroy_v1(&f->proof.proof);
	if (f->replay.inputs != NULL) ctrc_set_finish(&f->replay.inputs);
}

UT_TEST(test_ctrc_page_native_set_promotes_exact_target_without_rr)
{
	CtrcPageFixture f; RfPageGuardPreflightV1 pre; RfPageGuardV1 other = {0};
	RfPageVersionV1 wrong; unsigned cf, grants;
	ctrc_page_fixture(&f); cf = test_cf_lock_calls; grants = test_tx_ir_grants;
	f.page = ctrc_page_build(&f); UT_ASSERT_EQ(f.page.verdict, RF_PAGE_AUTHORITY_OK);
	UT_ASSERT(!rf_page_authority_ctrc_replays_covers_nowait_v1(f.page.authority,
		&f.target.page_identity, &f.target.expected_result));
	UT_ASSERT_EQ(rf_page_authority_ctrc_replays_promote_nowait_v1(f.page.authority), 0);
	UT_ASSERT_EQ(rf_page_authority_ctrc_replays_revalidate_nowait_v1(f.page.authority), 0);
	UT_ASSERT(rf_page_authority_ctrc_replays_covers_nowait_v1(f.page.authority,
		&f.target.page_identity, &f.target.expected_result));
	wrong = f.target.expected_result; wrong.mutation_token--;
	UT_ASSERT(!rf_page_authority_ctrc_replays_covers_nowait_v1(f.page.authority,
		&f.target.page_identity, &wrong));
	UT_ASSERT(rf_page_guard_preflight_v1(&f.target.page_identity, &pre));
	UT_ASSERT(!rf_page_guard_promote_nowait_v1(&pre, &other));
	UT_ASSERT_EQ(test_cf_lock_calls, cf); UT_ASSERT_EQ(test_tx_ir_grants, grants);
	rf_page_authority_ctrc_replays_destroy_v1(&f.page.authority);
	UT_ASSERT(rf_page_guard_promote_nowait_v1(&pre, &other)); rf_page_guard_release_v1(&other);
	UT_ASSERT_EQ(ctrc_set_held, 2); UT_ASSERT_EQ(test_tx_ir_releases, 0);
	ctrc_page_finish(&f);
}

UT_TEST(test_ctrc_page_refuses_budget_shape_and_omitted_native_origin)
{
	CtrcPageFixture f; RfPageCtrcAuthorityBuildV1 built;
	ctrc_page_fixture(&f);
	for (unsigned fault = 0; fault < 5; fault++) {
		built = rf_page_authority_ctrc_replays_preflight_v1(fault == 0 ? NULL : &f.target,
			fault == 1 ? 0 : 1, f.replay.owners, fault == 2 ? 0 : fault == 3 ? 1 : 2,
			fault == 4 ? 0 : 1024 * 1024);
		UT_ASSERT(built.verdict != RF_PAGE_AUTHORITY_OK && built.authority == NULL);
		UT_ASSERT_EQ(ctrc_set_held, 2); UT_ASSERT_EQ(test_tx_ir_releases, 0);
	}
	ctrc_page_finish(&f);
}

UT_TEST(test_ctrc_page_freezes_complete_target_and_native_owner_bytes)
{
	CtrcPageFixture f; void *buffers[2]; Size sizes[2];
	ctrc_page_fixture(&f); f.page = ctrc_page_build(&f);
	UT_ASSERT_EQ(f.page.verdict, RF_PAGE_AUTHORITY_OK);
	buffers[0] = &f.target; sizes[0] = sizeof(f.target);
	buffers[1] = f.replay.owners; sizes[1] = sizeof(f.replay.owners);
	for (unsigned b = 0; b < lengthof(buffers); b++)
		for (Size i = 0; i < sizes[b]; i++) {
			uint8 *byte = (uint8 *)buffers[b] + i;
			*byte ^= 1;
			UT_ASSERT(rf_page_authority_ctrc_replays_promote_nowait_v1(f.page.authority) != 0);
			*byte ^= 1;
		}
	UT_ASSERT_EQ(rf_page_authority_ctrc_replays_promote_nowait_v1(f.page.authority), 0);
	UT_ASSERT_EQ(test_tx_ir_releases, 0); ctrc_page_finish(&f);
}

UT_TEST(test_ctrc_page_native_loss_before_promotion_preserves_original_debt)
{
	CtrcPageFixture f; RfPageGuardPreflightV1 pre; RfPageGuardV1 other = {0};
	ctrc_page_fixture(&f); f.page = ctrc_page_build(&f); test_input_current = false;
	UT_ASSERT(rf_page_authority_ctrc_replays_promote_nowait_v1(f.page.authority) != 0);
	UT_ASSERT(rf_page_guard_preflight_v1(&f.target.page_identity, &pre));
	UT_ASSERT(rf_page_guard_promote_nowait_v1(&pre, &other)); rf_page_guard_release_v1(&other);
	UT_ASSERT(inputs_recovery_held && ctrc_set_held == 2 && test_tx_ir_releases == 0);
	ctrc_page_finish(&f);
}

UT_TEST(test_ctrc_page_wait_retries_same_original_set_without_deadline)
{
	CtrcPageFixture f; RfPageGuardPreflightV1 pre; RfPageGuardV1 blocker = {0};
	ctrc_page_fixture(&f); f.page = ctrc_page_build(&f);
	UT_ASSERT(rf_page_guard_preflight_v1(&f.target.page_identity, &pre));
	UT_ASSERT(rf_page_guard_promote_nowait_v1(&pre, &blocker));
	UT_ASSERT_EQ(rf_page_authority_ctrc_replays_promote_nowait_v1(f.page.authority),
		RF_PAGE_AUTHORITY_WOULD_BLOCK);
	UT_ASSERT(inputs_recovery_held && ctrc_set_held == 2 && test_tx_ir_releases == 0);
	rf_page_guard_release_v1(&blocker);
	UT_ASSERT_EQ(rf_page_authority_ctrc_replays_promote_nowait_v1(f.page.authority), 0);
	ctrc_page_finish(&f);
}

UT_TEST(test_ctrc_page_partial_page_wait_releases_only_new_page_locks)
{
	CtrcPageFixture f; CtrcReplayStableFixture second;
	RfPageStableCtrcBuildV1 proof; RfPageAuthorityTargetV1 targets[2];
	RfPageGuardPreflightV1 pre[2]; RfPageGuardV1 blocker = {0}, probe = {0};
	ctrc_page_fixture(&f); memcpy(&second, &f.replay, sizeof(second));
	second.graph.source = &second.source; second.graph.contributors = &second.vector;
	second.vector.cuts = second.cuts; second.vector.edges = second.edges;
	UT_ASSERT(rf_page_guard_preflight_v1(&f.target.page_identity, &pre[0]));
	do {
		second.graph.page_identity.blockno++;
		UT_ASSERT(rf_page_guard_preflight_v1(&second.graph.page_identity, &pre[1]));
	} while (pre[0].partition == pre[1].partition);
	second.source.page_identity = second.graph.page_identity;
	for (unsigned i = 0; i < 2; i++) second.edges[i].page_identity = second.graph.page_identity;
	proof = rf_page_stable_base_proof_build_ctrc_replays_v1(&second.graph,
		f.replay.owners, 2, 1024 * 1024); UT_ASSERT_EQ(proof.detail, 0);
	memset(targets, 0, sizeof(targets)); targets[0] = f.target;
	targets[1].page_identity = second.graph.page_identity;
	targets[1].expected_result = second.graph.expected_result;
	targets[1].stable_base = proof.proof; targets[1].source = &second.source;
	targets[1].contributors = &second.vector;
	f.page = rf_page_authority_ctrc_replays_preflight_v1(targets, 2, f.replay.owners, 2, 1024 * 1024);
	UT_ASSERT_EQ(f.page.verdict, 0); UT_ASSERT(rf_page_guard_promote_nowait_v1(&pre[1], &blocker));
	UT_ASSERT_EQ(rf_page_authority_ctrc_replays_promote_nowait_v1(f.page.authority),
		RF_PAGE_AUTHORITY_WOULD_BLOCK);
	UT_ASSERT(rf_page_guard_promote_nowait_v1(&pre[0], &probe)); rf_page_guard_release_v1(&probe);
	UT_ASSERT_EQ(ctrc_set_held, 2); rf_page_guard_release_v1(&blocker);
	UT_ASSERT_EQ(rf_page_authority_ctrc_replays_promote_nowait_v1(f.page.authority), 0);
	for (unsigned i = 0; i < 2; i++)
		UT_ASSERT(rf_page_authority_ctrc_replays_covers_nowait_v1(f.page.authority,
			&targets[i].page_identity, &targets[i].expected_result));
	ctrc_page_finish(&f); rf_page_stable_base_proof_destroy_v1(&proof.proof);
}

UT_TEST(test_ctrc_page_stale_proof_blocks_use_while_page_retirement_is_owned)
{
	CtrcPageFixture f; const RfContributorStreamCutV1 *cuts;
	RfPageGuardPreflightV1 pre; RfPageGuardV1 other = {0};
	ctrc_page_fixture(&f); f.page = ctrc_page_build(&f);
	UT_ASSERT_EQ(rf_page_authority_ctrc_replays_promote_nowait_v1(f.page.authority), 0);
	cuts = f.replay.vector.cuts; f.replay.vector.cuts = (const void *)(uintptr_t)1;
	UT_ASSERT(rf_page_authority_ctrc_replays_revalidate_nowait_v1(f.page.authority) != 0);
	UT_ASSERT(!rf_page_authority_ctrc_replays_covers_nowait_v1(f.page.authority,
		&f.target.page_identity, &f.target.expected_result));
	f.replay.vector.cuts = cuts; test_input_current = false;
	UT_ASSERT(rf_page_authority_ctrc_replays_revalidate_nowait_v1(f.page.authority) != 0);
	UT_ASSERT(rf_page_guard_preflight_v1(&f.target.page_identity, &pre));
	UT_ASSERT(!rf_page_guard_promote_nowait_v1(&pre, &other));
	rf_page_authority_ctrc_replays_destroy_v1(&f.page.authority);
	UT_ASSERT(rf_page_guard_promote_nowait_v1(&pre, &other)); rf_page_guard_release_v1(&other);
	UT_ASSERT_EQ(ctrc_set_held, 2); UT_ASSERT_EQ(test_tx_ir_releases, 0); ctrc_page_finish(&f);
}

UT_TEST(test_ctrc_page_destroy_after_scope_retirement_has_no_borrowed_access)
{
	CtrcPageFixture f;
	ctrc_page_fixture(&f);
	f.page = ctrc_page_build(&f);
	UT_ASSERT_EQ(rf_page_authority_ctrc_replays_promote_nowait_v1(f.page.authority), 0);
	ctrc_set_finish(&f.replay.inputs);
	UT_ASSERT(rf_page_authority_ctrc_replays_revalidate_nowait_v1(f.page.authority) != 0);
	rf_page_authority_ctrc_replays_destroy_v1(&f.page.authority);
	UT_ASSERT(f.page.authority == NULL); ctrc_page_finish(&f);
}
