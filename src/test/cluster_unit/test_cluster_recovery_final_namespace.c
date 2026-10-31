/* Actual complete namespace re-enumeration, with original root budget and
 * immutable observations. This establishes no recovery completion authority.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
#define RECOVERY_ROOT_SET_FIXTURE_ONLY
#include "test_cluster_recovery_root_set.c"

#ifdef USE_PGRAC_CLUSTER
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
void
pg_re_throw(void)
{
	if (PG_exception_stack != NULL) siglongjmp(*PG_exception_stack, 1);
	abort();
}

extern bool cluster_recovery_root_set_namespace_current_v1(ClusterRecoveryRootSet *roots,
	bool (*current)(void *), void *arg);
static unsigned namespace_current_calls, namespace_current_at, namespace_current_fault;
static bool
namespace_current(void *arg)
{
	UT_ASSERT(arg != NULL);
	if (++namespace_current_calls == namespace_current_at) {
		if (namespace_current_fault == 1) return false;
		if (namespace_current_fault == 2) pg_re_throw();
		if (namespace_current_fault == 3) inject_readdir_error = true;
		if (namespace_current_fault == 4)
			UT_ASSERT(!cluster_recovery_root_set_namespace_current_v1(arg, NULL, NULL));
		if (namespace_current_fault == 5)
			UT_ASSERT_EQ(cluster_recovery_root_set_recheck_begin(arg), CLUSTER_RECOVERY_ROOTS_INVALID);
	}
	return true;
}
static void
namespace_current_reset(unsigned fault, unsigned at)
{
	namespace_current_calls = 0; namespace_current_fault = fault; namespace_current_at = at;
}
UT_TEST(test_final_namespace_keeps_original_digest_after_authorized_relation_bytes)
{
	int fd = new_root(), file;
	ClusterRecoveryRootSet *roots;
	uint8 before[32], after[32];
	char changed = 'x';
	UT_ASSERT_EQ(mkdirat(fd, "base", 0700), 0);
	UT_ASSERT_EQ(mkdirat(fd, "base/5", 0700), 0); write_file(fd, "base/5/16385");
	roots = begin_roots(fd, 1024 * 1024);
	UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
	UT_ASSERT(cluster_recovery_root_set_digest(roots, before));
	file = openat(fd, "base/5/16385", O_WRONLY);
	UT_ASSERT(file >= 0); UT_ASSERT_EQ(pwrite(file, &changed, 1, 0), 1); UT_ASSERT_EQ(close(file), 0);
	namespace_current_reset(0, 0);
	for (unsigned repeat = 0; repeat < 2; repeat++) {
		UT_ASSERT(cluster_recovery_root_set_namespace_current_v1(roots, namespace_current, roots));
		UT_ASSERT(cluster_recovery_root_set_digest(roots, after));
		UT_ASSERT(memcmp(before, after, 32) == 0);
	}
	UT_ASSERT(namespace_current_calls > 3);
	cluster_recovery_root_set_destroy(roots); remove_root(fd);
}
UT_TEST(test_final_namespace_new_missing_replaced_or_unsafe_paths_refuse)
{
	for (unsigned fault = 0; fault < 5; fault++) {
		int fd = new_root();
		ClusterRecoveryRootSet *roots;
		uint8 digest[32];
		write_file(fd, "original"); roots = begin_roots(fd, 1024 * 1024);
		UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
		if (fault == 0) write_file(fd, "new-physical");
		if (fault == 1) UT_ASSERT_EQ(unlinkat(fd, "original", 0), 0);
		if (fault == 2) UT_ASSERT_EQ(renameat(fd, "original", fd, "same-count"), 0);
		if (fault == 3) {
			UT_ASSERT_EQ(renameat(fd, "original", fd, "old"), 0); write_file(fd, "original");
		}
		if (fault == 4) UT_ASSERT_EQ(symlinkat("original", fd, "alias"), 0);
		UT_ASSERT(!cluster_recovery_root_set_namespace_current_v1(roots, NULL, NULL));
		UT_ASSERT(!cluster_recovery_root_set_digest(roots, digest));
		UT_ASSERT(!cluster_recovery_root_set_namespace_current_v1(roots, NULL, NULL));
		cluster_recovery_root_set_destroy(roots); remove_root(fd);
	}
}
UT_TEST(test_final_namespace_empty_and_wal_only_do_not_adopt_materialized_objects)
{
	for (unsigned wal = 0; wal < 2; wal++) {
		int fd = new_root();
		ClusterRecoveryRootSet *roots = begin_roots(fd, 1024 * 1024);
		RfPageIdentityV1 page = target(16385, MAIN_FORKNUM, 0);
		if (wal) UT_ASSERT_EQ(cluster_recovery_root_set_add_target(roots, &page), 0);
		UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
		UT_ASSERT(cluster_recovery_root_set_namespace_current_v1(roots, NULL, NULL));
		UT_ASSERT_EQ(mkdirat(fd, "base", 0700), 0);
		UT_ASSERT_EQ(mkdirat(fd, "base/5", 0700), 0); write_file(fd, "base/5/16385");
		UT_ASSERT(!cluster_recovery_root_set_namespace_current_v1(roots, NULL, NULL));
		UT_ASSERT_EQ(cluster_recovery_root_set_count(roots), wal ? 1 : 0);
		cluster_recovery_root_set_destroy(roots); remove_root(fd);
	}
}
UT_TEST(test_final_namespace_late_current_loss_error_and_scan_io_are_sticky)
{
	for (unsigned fault = 1; fault <= 5; fault++) {
		int fd = new_root();
		ClusterRecoveryRootSet *roots;
		volatile bool caught = false;
		uint8 digest[32];
		for (unsigned i = 0; i < 140; i++) {
			char name[32]; snprintf(name, sizeof(name), "file%u", i); write_file(fd, name);
		}
		roots = begin_roots(fd, 2 * 1024 * 1024);
		UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
		namespace_current_reset(fault, 5);
		PG_TRY();
		{
			UT_ASSERT(!cluster_recovery_root_set_namespace_current_v1(roots, namespace_current, roots));
		}
		PG_CATCH(); { caught = true; }
		PG_END_TRY();
		inject_readdir_error = false;
		UT_ASSERT_EQ(caught, fault == 2); UT_ASSERT(namespace_current_calls >= 5);
		UT_ASSERT(!cluster_recovery_root_set_digest(roots, digest));
		UT_ASSERT(!cluster_recovery_root_set_namespace_current_v1(roots, NULL, NULL));
		cluster_recovery_root_set_destroy(roots); remove_root(fd);
	}
}
UT_TEST(test_final_namespace_unclassified_bytes_and_relation_extent_stay_exact)
{
	for (unsigned relation = 0; relation < 2; relation++) {
		int fd = new_root(), file;
		ClusterRecoveryRootSet *roots;
		const char *name = relation ? "base/5/16385" : "unknown";
		UT_ASSERT_EQ(mkdirat(fd, "base", 0700), 0); UT_ASSERT_EQ(mkdirat(fd, "base/5", 0700), 0);
		write_file(fd, name); roots = begin_roots(fd, 1024 * 1024);
		UT_ASSERT_EQ(finish_roots(roots, NULL), CLUSTER_RECOVERY_ROOTS_OBSERVED);
		file = openat(fd, name, O_WRONLY); UT_ASSERT(file >= 0);
		if (relation) UT_ASSERT_EQ(ftruncate(file, 5), 0);
		else UT_ASSERT_EQ(pwrite(file, "x", 1, 0), 1);
		UT_ASSERT_EQ(close(file), 0);
		UT_ASSERT(!cluster_recovery_root_set_namespace_current_v1(roots, NULL, NULL));
		cluster_recovery_root_set_destroy(roots); remove_root(fd);
	}
}
int main(void)
{
	UT_PLAN(23);
	UT_RUN(test_final_namespace_keeps_original_digest_after_authorized_relation_bytes);
	UT_RUN(test_final_namespace_new_missing_replaced_or_unsafe_paths_refuse);
	UT_RUN(test_final_namespace_empty_and_wal_only_do_not_adopt_materialized_objects);
	UT_RUN(test_final_namespace_late_current_loss_error_and_scan_io_are_sticky);
	UT_RUN(test_final_namespace_unclassified_bytes_and_relation_extent_stay_exact);
	UT_RUN(test_unreferenced_physical_files_and_missing_wal_targets_are_preserved);
	UT_RUN(test_native_slru_root_counts_are_exposed_only_after_observation);
	UT_RUN(test_all_file_bytes_are_read_twice_in_bounded_chunks);
	UT_RUN(test_digest_is_path_ordered_and_bound_to_the_namespace);
	UT_RUN(test_file_digest_uses_all_sha256_bits);
	UT_RUN(test_equal_count_rename_or_rewrite_between_passes_is_not_stable);
	UT_RUN(test_short_read_and_io_failure_cannot_finish_a_root_set);
	UT_RUN(test_capacity_and_late_targets_never_shrink_the_union);
	UT_RUN(test_malformed_and_foreign_wal_targets_refuse);
	UT_RUN(test_a_late_root_revokes_the_completed_candidate_digest);
	UT_RUN(test_wal_and_physical_forks_merge_without_losing_missing_blocks);
	UT_RUN(test_large_root_union_sorts_across_ticks_without_dropping_targets);
	UT_RUN(test_directory_buffers_share_the_root_memory_budget);
	UT_RUN(test_directory_budget_is_returned_after_each_subtree);
	UT_RUN(test_graph_reader_uses_the_observed_file_and_range);
	UT_RUN(test_graph_read_refuses_changed_parent_or_file);
	UT_RUN(test_graph_read_never_turns_io_or_short_read_into_an_empty_edge);
	UT_RUN(test_long_collision_chain_exhausts_budget_without_omitting_a_root);
	UT_DONE(); return ut_failed_count != 0;
}
#endif
