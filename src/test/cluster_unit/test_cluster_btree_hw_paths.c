/*-------------------------------------------------------------------------
 *
 * test_cluster_btree_hw_paths.c
 *    Exercise real B-tree root/search/incomplete-split allocation boundaries.
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_btree_hw_paths.c
 *
 * NOTES
 *    Reuse the real-page/version fixture. Parent completion, comparison,
 *    snapshot and buffer I/O are controlled boundaries, not concurrency proof.
 *-------------------------------------------------------------------------
 */
int structure_fixture_main(void);
#define main structure_fixture_main
#include "test_cluster_btree_structure_version.c"
#undef main

static unsigned completed_parents;
static bool
all_unlocked(void)
{
	for (int i = 0; i < 6; i++)
		if (locked[i])
			return false;
	return true;
}
static void
path_parent(Relation rel, Relation heap, Buffer left, Buffer right, BTStack stack,
			bool root, bool only, const ClusterSpaceIdentity *identity, BTInsertAlloc *allocation)
{
	(void)heap; (void)stack; (void)root; (void)only;
	UT_ASSERT(identity == &space_identity && allocation && allocation->hw.held);
	BTPageGetOpaque(BufferGetPage(left))->btpo_flags &= ~BTP_INCOMPLETE_SPLIT;
	_bt_relbuf(rel, right);
	_bt_relbuf(rel, left);
	completed_parents++;
}
int32
_bt_compare(Relation rel, BTScanInsert key, Page page, OffsetNumber offset)
{
	(void)rel; (void)key; (void)page; (void)offset;
	return -1;
}
static OffsetNumber
_bt_binsrch(Relation rel, BTScanInsert key, Buffer buffer)
{
	(void)rel; (void)key; (void)buffer;
	return 1;
}
void
_bt_freestack(BTStack stack)
{
	while (stack) {
		BTStack parent = stack->bts_parent;
		pfree(stack);
		stack = parent;
	}
}
#define _bt_insert_parent path_parent
#include "test_cluster_btree_hw_insert_paths.inc"
#undef _bt_insert_parent
#undef TestForOldSnapshot
#define TestForOldSnapshot(snapshot, rel, page) ((void) 0)
#include "test_cluster_btree_hw_search_paths.inc"
#undef TestForOldSnapshot

static void
held_scope(BTInsertAlloc *allocation)
{
	memset(allocation, 0, sizeof(*allocation));
	allocation->required = true;
	allocation->hw.held = allocation->hw.coordinated = true;
	allocation->ko_serial = 42;
	expected_hw = &allocation->hw;
}
UT_TEST(test_first_root_defers_before_allocation_and_meta_mutation)
{
	BTInsertAlloc allocation = {0};
	reset();
	BTPageGetMeta(pages[3].data)->btm_root = P_NONE;
	save_before();
	UT_ASSERT_EQ(_bt_getroot(&relation_data, &relation_data, BT_WRITE, &space_identity,
							&allocation), InvalidBuffer);
	UT_ASSERT(allocation.required && all_unlocked());
	UT_ASSERT(memcmp(before, pages, sizeof(pages)) == 0);
	UT_ASSERT(wal_count == 0 && dirties == 0 && borrowed_extensions == 0);
}
UT_TEST(test_first_root_borrows_exact_owner)
{
	BTInsertAlloc allocation;
	reset(); held_scope(&allocation);
	BTPageGetMeta(pages[3].data)->btm_root = P_NONE;
	save_before(); expected_info = XLOG_BTREE_NEWROOT;
	UT_ASSERT_EQ(_bt_getroot(&relation_data, &relation_data, BT_WRITE, &space_identity,
							&allocation), 2);
	UT_ASSERT(borrowed_extensions == 1 && allocation.hw.held && allocation.ko_serial == 42);
	check_version(2, 5);
}
UT_TEST(test_search_unwinds_incomplete_split_with_owned_stack)
{
	BTInsertAlloc allocation = {0};
	BTScanInsertData key = {0};
	BTStack stack;
	Buffer buffer;
	reset();
	BTPageGetOpaque(pages[0].data)->btpo_flags = BTP_ROOT;
	BTPageGetOpaque(pages[0].data)->btpo_level = 1;
	BTPageGetMeta(pages[3].data)->btm_level = 1;
	BTPageGetMeta(pages[3].data)->btm_fastlevel = 1;
	add_downlink(pages[0].data, 1, 8, false);
	PageInit(pages[1].data, BLCKSZ, sizeof(BTPageOpaqueData));
	BTPageGetOpaque(pages[1].data)->btpo_flags = BTP_LEAF | BTP_INCOMPLETE_SPLIT;
	BTPageGetOpaque(pages[1].data)->btpo_next = 9;
	save_before();
	stack = _bt_search(&relation_data, &relation_data, &key, &buffer, BT_WRITE, NULL,
					   &space_identity, &allocation);
	UT_ASSERT(buffer == InvalidBuffer && stack != NULL && allocation.required);
	UT_ASSERT(stack->bts_blkno == 7 && stack->bts_parent == NULL && all_unlocked());
	UT_ASSERT(memcmp(before, pages, sizeof(pages)) == 0 && wal_count == 0);
	_bt_freestack(stack);
	pfree(relation_data.rd_amcache); relation_data.rd_amcache = NULL;
}
UT_TEST(test_step_right_releases_deferred_sibling_but_retains_callers_leaf)
{
	BTInsertAlloc allocation = {0};
	BTInsertStateData state = {0};
	reset();
	BTPageGetOpaque(pages[0].data)->btpo_next = 9;
	BTPageGetOpaque(pages[2].data)->btpo_flags |= BTP_INCOMPLETE_SPLIT;
	BTPageGetOpaque(pages[2].data)->btpo_next = 10;
	state.buf = 1; state.version_identity = space_identity; state.allocation = &allocation;
	locked[0] = true; save_before();
	UT_ASSERT(!_bt_stepright(&relation_data, &relation_data, &state, NULL));
	UT_ASSERT(allocation.required && state.buf == 1 && locked[0] && !locked[2]);
	UT_ASSERT(memcmp(before, pages, sizeof(pages)) == 0 && wal_count == 0);
	_bt_relbuf(&relation_data, state.buf);
	UT_ASSERT(all_unlocked());
}
UT_TEST(test_parent_refind_defers_before_repair)
{
	BTInsertAlloc allocation = {0};
	BTStackData stack = {0};
	reset();
	stack.bts_blkno = 7;
	BTPageGetOpaque(pages[0].data)->btpo_flags |= BTP_INCOMPLETE_SPLIT;
	BTPageGetOpaque(pages[0].data)->btpo_next = 9;
	save_before();
	UT_ASSERT_EQ(_bt_getstackbuf(&relation_data, &relation_data, &stack, 8,
								&space_identity, &allocation), InvalidBuffer);
	UT_ASSERT(allocation.required && all_unlocked() && wal_count == 0);
	UT_ASSERT(memcmp(before, pages, sizeof(pages)) == 0);
}
UT_TEST(test_owned_incomplete_split_does_not_request_another_restart)
{
	BTInsertAlloc allocation;
	BTStackData stack = {0};
	reset(); held_scope(&allocation); completed_parents = 0;
	locked[0] = true;
	BTPageGetOpaque(pages[0].data)->btpo_flags |= BTP_INCOMPLETE_SPLIT;
	BTPageGetOpaque(pages[0].data)->btpo_next = 9;
	UT_ASSERT(_bt_finish_split(&relation_data, &relation_data, 1, &stack,
							  &space_identity, &allocation));
	UT_ASSERT(completed_parents == 1 && all_unlocked() && allocation.hw.held);
}
UT_TEST(test_split_borrows_the_original_hw_and_ko_serial)
{
	BTInsertAlloc allocation;
	BTScanInsertData key = {0};
	IndexTuple tuple;
	reset(); held_scope(&allocation); locked[0] = true; key.heapkeyspace = true;
	for (int i = 1; i <= 3; i++) {
		tuple = new_tuple(i);
		UT_ASSERT_EQ(PageAddItem(pages[0].data, (Item)tuple, 16, i, false, false), i);
		pfree(tuple);
	}
	tuple = new_tuple(4); save_before(); expected_info = XLOG_BTREE_SPLIT_R;
	UT_ASSERT_EQ(run_split(&relation_data, &relation_data, &key, 1, InvalidBuffer,
						   4, 16, tuple, NULL, NULL, 0, &space_identity, &allocation), 2);
	UT_ASSERT(borrowed_extensions == 1 && allocation.hw.held && allocation.ko_serial == 42);
	check_version(2, 3); pfree(tuple);
}
UT_TEST(test_new_root_borrows_the_original_hw_and_ko_serial)
{
	BTInsertAlloc allocation;
	IndexTuple high;
	reset(); held_scope(&allocation); locked[0] = locked[2] = true;
	BTPageGetOpaque(pages[0].data)->btpo_flags |= BTP_INCOMPLETE_SPLIT;
	BTPageGetOpaque(pages[0].data)->btpo_next = 9;
	high = new_tuple(4); BTreeTupleSetNAtts(high, 1, false);
	UT_ASSERT_EQ(PageAddItem(pages[0].data, (Item)high, 16, 1, false, false), 1);
	pfree(high); save_before(); expected_info = XLOG_BTREE_NEWROOT;
	UT_ASSERT_EQ(run_newlevel(&relation_data, &relation_data, 1, 3, &space_identity,
							 &allocation), 2);
	UT_ASSERT(borrowed_extensions == 1 && allocation.hw.held && allocation.ko_serial == 42);
	check_version(3, 7);
}
int main(void)
{
	UT_PLAN(8);
	UT_RUN(test_first_root_defers_before_allocation_and_meta_mutation);
	UT_RUN(test_first_root_borrows_exact_owner);
	UT_RUN(test_search_unwinds_incomplete_split_with_owned_stack);
	UT_RUN(test_step_right_releases_deferred_sibling_but_retains_callers_leaf);
	UT_RUN(test_parent_refind_defers_before_repair);
	UT_RUN(test_owned_incomplete_split_does_not_request_another_restart);
	UT_RUN(test_split_borrows_the_original_hw_and_ko_serial);
	UT_RUN(test_new_root_borrows_the_original_hw_and_ko_serial);
	UT_DONE(); return ut_failed_count ? 1 : 0;
}
