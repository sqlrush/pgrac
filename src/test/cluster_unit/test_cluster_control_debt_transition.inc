/*-------------------------------------------------------------------------
 * test_cluster_control_debt_transition.inc
 *     Original-owner transition observations do not grant publication.
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_control_debt_transition.inc
 * NOTES
 *     Uses the same private owner implementation; no live prepare caller.
 *-------------------------------------------------------------------------
 */
#include "cluster/cluster_control_debt_transition.h"
#include "utils/resowner.h"
#include <sys/wait.h>
ResourceOwner CurrentResourceOwner = (ResourceOwner)(uintptr_t)1;
int MyProcPid;
/* The original Start/selection remain opaque controlled storage leaves. */
static char transition_start[2048], transition_selection[2048];
bool transition_start_distinct(const struct ClusterRecoveryStartV1 *, const void *, Size);
bool transition_selection_distinct(const ClusterControlRelmapSelectionV2 *, const void *, Size);
#define cluster_control_root_debt_transition_get_v2 debt_fixture_transition_get_v2
#define cluster_recovery_plan_output_distinct_v1 transition_start_distinct
#define cluster_control_root_relmap_selection_output_distinct_v4 transition_selection_distinct
#include "../../backend/cluster/cluster_control_debt_transition_v2.inc"
#undef cluster_control_root_relmap_selection_output_distinct_v4
#undef cluster_recovery_plan_output_distinct_v1
#undef cluster_control_root_debt_transition_get_v2
bool transition_start_distinct(const struct ClusterRecoveryStartV1 *s, const void *out, Size n)
{
	return s == (const struct ClusterRecoveryStartV1 *)transition_start
		&& !debt_overlap(transition_start, sizeof(transition_start), out, n);
}
bool transition_selection_distinct(const ClusterControlRelmapSelectionV2 *s,
	const void *out, Size n)
{
	return s == (const ClusterControlRelmapSelectionV2 *)transition_selection
		&& !debt_overlap(transition_selection, sizeof(transition_selection), out, n);
}
static const struct ClusterRecoveryStartV1 *transition_start_ptr(void)
{ return (const struct ClusterRecoveryStartV1 *)&transition_start; }
static const ClusterControlRelmapSelectionV2 *transition_selection_ptr(void)
{ return (const ClusterControlRelmapSelectionV2 *)&transition_selection; }
static ClusterControlDebtTransitionV2 *transition_begin(void)
{
	prepared_begin(65);
	MyProcPid = (int)getpid(); selected_cf_sequence = 1;
	return control_debt_transition_build_v2(transition_start_ptr(), transition_selection_ptr(),
		prepared_before, sizeof(prepared_before), image, debt_batches, 1, DEBT_TEST_BUDGET);
}
UT_TEST(transition_gets_complete_original_and_prepared_bytes_without_io)
{
	ClusterControlDebtTransitionV2 *t = transition_begin();
	ClusterControlDebtTransitionViewV2 v;
	UT_ASSERT(t != NULL);
	reset_faults();
	UT_ASSERT(debt_fixture_transition_get_v2(t, &v));
	UT_ASSERT_EQ(reads, 0);
	UT_ASSERT(v.start == transition_start_ptr() && v.selection == transition_selection_ptr());
	UT_ASSERT_EQ(v.cf_sequence, 1); UT_ASSERT_EQ(v.root_pair_bytes, 133120);
	UT_ASSERT(!memcmp(v.before_root, prepared_before, 133120));
	UT_ASSERT(prepared_candidate(DEBT_TEST_BUDGET, prepared_after));
	UT_ASSERT(!memcmp(v.after_root, prepared_after, 133120));
	UT_ASSERT_EQ(v.basis_bytes, debt_original_bytes);
	UT_ASSERT(!memcmp(v.basis, debt_original, debt_original_bytes));
	UT_ASSERT(!memcmp(v.basis_reference, debt_original_ref, 128));
	UT_ASSERT_EQ(v.batch_count, 1); UT_ASSERT_EQ(v.batches[0].episode, debt_batches[0].episode);
	UT_ASSERT_EQ(v.batches[0].prepare, debt_batches[0].prepare);
	UT_ASSERT_EQ(v.batches[0].basis_bytes, debt_batches[0].basis_bytes);
	UT_ASSERT(!memcmp(v.batches[0].basis, debt_batches[0].basis, v.batches[0].basis_bytes));
	UT_ASSERT(!memcmp(v.batches[0].basis_reference, debt_batches[0].basis_ref, 128));
	UT_ASSERT_EQ(v.registry_bytes, image->registry_bytes);
	UT_ASSERT_EQ(v.page_bytes, image->page_bytes);
	UT_ASSERT(!memcmp(v.prepared_reference, image->reference, 128));
	UT_ASSERT(!memcmp(v.registry, image->registry, image->registry_bytes));
	UT_ASSERT(!memcmp(v.pages, image->pages, image->page_bytes));
	control_debt_transition_destroy_v2(&t); UT_ASSERT(t == NULL); store_end();
}
UT_TEST(transition_owns_snapshots_after_original_scratch_changes)
{
	ClusterControlDebtTransitionV2 *t = transition_begin();
	ClusterControlDebtTransitionViewV2 first, again;
	UT_ASSERT(t && debt_fixture_transition_get_v2(t, &first));
	uint8 before = first.before_root[1024], page = first.pages[500];
	uint8 basis = first.batches[0].basis[400];
	prepared_before[1024] ^= 1; image->pages[500] ^= 1;
	((uint8 *)debt_batches[0].basis)[400] ^= 1;
	UT_ASSERT(debt_fixture_transition_get_v2(t, &again));
	UT_ASSERT_EQ(again.before_root[1024], before); UT_ASSERT_EQ(again.pages[500], page);
	UT_ASSERT_EQ(again.batches[0].basis[400], basis);
	UT_ASSERT(!memcmp(&first, &again, sizeof(first)));
	control_debt_transition_destroy_v2(&t); store_end();
}
UT_TEST(transition_wrong_owner_process_or_cf_preserves_output)
{
	for (unsigned fault = 0; fault < 4; ++fault) {
		ClusterControlDebtTransitionV2 *t = transition_begin();
		ClusterControlDebtTransitionViewV2 v, saved;
		ResourceOwner owner = CurrentResourceOwner;
		int pid = MyProcPid;
		memset(&v, 0xa5, sizeof(v)); saved = v;
		UT_ASSERT(t != NULL);
		if (fault == 0) CurrentResourceOwner = (ResourceOwner)&saved;
		if (fault == 1) MyProcPid++;
		if (fault == 2) cf_held = false;
		if (fault == 3) selected_cf_sequence++;
		UT_ASSERT(!debt_fixture_transition_get_v2(t, &v));
		UT_ASSERT(!memcmp(&v, &saved, sizeof(v)));
		CurrentResourceOwner = owner; MyProcPid = pid; cf_held = true;
		control_debt_transition_destroy_v2(&t); store_end();
	}
	/* fork inherits the same MyProcPid/owner values; actual PID must differ. */
	ClusterControlDebtTransitionV2 *t = transition_begin();
	pid_t child = fork(); int status = 0;
	UT_ASSERT(child >= 0);
	if (child == 0) {
		ClusterControlDebtTransitionViewV2 v;
		_exit(debt_fixture_transition_get_v2(t, &v) ? 1 : 0);
	}
	if (child > 0) {
		UT_ASSERT(waitpid(child, &status, 0) == child);
		UT_ASSERT(WIFEXITED(status) && WEXITSTATUS(status) == 0);
	}
	control_debt_transition_destroy_v2(&t); store_end();
}
UT_TEST(transition_copy_and_output_alias_cannot_modify_custody)
{
	ClusterControlDebtTransitionV2 *t = transition_begin();
	ClusterControlDebtTransitionV2 copy;
	ClusterControlDebtTransitionViewV2 v;
	UT_ASSERT(t != NULL); copy = *t;
	UT_ASSERT(!debt_fixture_transition_get_v2(&copy, &v));
	UT_ASSERT(debt_fixture_transition_get_v2(t, &v));
	UT_ASSERT(!debt_fixture_transition_get_v2(t, (ClusterControlDebtTransitionViewV2 *)t));
	UT_ASSERT(!debt_fixture_transition_get_v2(t, (ClusterControlDebtTransitionViewV2 *)v.before_root));
	UT_ASSERT(!debt_fixture_transition_get_v2(t, (ClusterControlDebtTransitionViewV2 *)v.after_root));
	UT_ASSERT(!debt_fixture_transition_get_v2(t, (ClusterControlDebtTransitionViewV2 *)v.pages));
	UT_ASSERT(!debt_fixture_transition_get_v2(t, (ClusterControlDebtTransitionViewV2 *)v.batches));
	UT_ASSERT(!debt_fixture_transition_get_v2(t, (ClusterControlDebtTransitionViewV2 *)v.batches[0].basis));
	UT_ASSERT(debt_fixture_transition_get_v2(t, &v));
	char original_start[2048], original_selection[2048];
	memcpy(original_start, transition_start, sizeof(original_start));
	memcpy(original_selection, transition_selection, sizeof(original_selection));
	UT_ASSERT(!debt_fixture_transition_get_v2(t,
		(ClusterControlDebtTransitionViewV2 *)(transition_start + 64)));
	UT_ASSERT(!debt_fixture_transition_get_v2(t,
		(ClusterControlDebtTransitionViewV2 *)(transition_selection + 64)));
	UT_ASSERT(!memcmp(original_start, transition_start, sizeof(original_start)));
	UT_ASSERT(!memcmp(original_selection, transition_selection, sizeof(original_selection)));
	control_debt_transition_destroy_v2(&t); store_end();
}
UT_TEST(transition_build_refuses_invalid_candidate_lock_or_budget)
{
	prepared_begin(65); MyProcPid = (int)getpid(); selected_cf_sequence = 1;
	UT_ASSERT(control_debt_transition_build_v2(transition_start_ptr(), transition_selection_ptr(),
		prepared_before, sizeof(prepared_before), image, debt_batches, 1, 1024) == NULL);
	cf_held = false;
	UT_ASSERT(control_debt_transition_build_v2(transition_start_ptr(), transition_selection_ptr(),
		prepared_before, sizeof(prepared_before), image, debt_batches, 1, DEBT_TEST_BUDGET) == NULL);
	cf_held = true; image->pages[500] ^= 1;
	UT_ASSERT(control_debt_transition_build_v2(transition_start_ptr(), transition_selection_ptr(),
		prepared_before, sizeof(prepared_before), image, debt_batches, 1, DEBT_TEST_BUDGET) == NULL);
	ClusterControlDebtTransitionViewV2 v, saved;
	memset(&v, 0xa5, sizeof(v)); saved = v;
	UT_ASSERT(!debt_fixture_transition_get_v2(NULL, &v));
	UT_ASSERT(!memcmp(&v, &saved, sizeof(v))); store_end();
}

UT_TEST(transition_gets_every_batch_and_rejects_empty_images)
{
	ClusterControlDebtTransitionV2 *t;
	ClusterControlDebtTransitionViewV2 v;
	prepared_begin(130); MyProcPid = (int)getpid(); selected_cf_sequence = 1;
	debt_batches[1] = debt_batches[0];
	debt_batches[0].entry_count = 65;
	debt_batches[1].episode++; debt_batches[1].entries += 65;
	debt_batches[1].entry_count = 65;
	for (unsigned i = 65; i < 130; ++i) debt_entries[i].entry.ordinal -= 65;
	control_debt_wire_destroy_v1(&image);
	UT_ASSERT(control_debt_wire_encode_v1(debt_original, debt_original_bytes,
		debt_original_ref, 15, 17, debt_batches, 2, DEBT_TEST_BUDGET, &image));
	t = control_debt_transition_build_v2(transition_start_ptr(), transition_selection_ptr(),
		prepared_before, sizeof(prepared_before), image, debt_batches, 2, DEBT_TEST_BUDGET);
	UT_ASSERT(t && debt_fixture_transition_get_v2(t, &v));
	UT_ASSERT_EQ(v.batch_count, 2);
	UT_ASSERT_EQ(v.batches[1].episode, debt_batches[1].episode);
	UT_ASSERT(!memcmp(v.batches[1].basis, debt_original, debt_original_bytes));
	UT_ASSERT(v.batches[0].basis != v.batches[1].basis);
	UT_ASSERT_EQ(v.page_bytes, 4 * 33024);
	control_debt_transition_destroy_v2(&t);
	ControlDebtWireImageV1 empty = *image;
	for (unsigned fault = 0; fault < 3; ++fault) {
		if (fault == 0) empty.registry_bytes = 0;
		if (fault == 1) { empty = *image; empty.page_bytes = 0; }
		if (fault == 2) { empty = *image; debt_batches[0].basis_bytes = 0; }
		UT_ASSERT(control_debt_transition_build_v2(transition_start_ptr(), transition_selection_ptr(),
			prepared_before, sizeof(prepared_before), &empty, debt_batches, 2, DEBT_TEST_BUDGET) == NULL);
	}
	store_end();
}
