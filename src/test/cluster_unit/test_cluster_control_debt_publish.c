/*-------------------------------------------------------------------------
 * test_cluster_control_debt_publish.c
 *     Protective ROOT replacement and complete immutable debt readback.
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_control_debt_publish.c
 * NOTES
 *     Real files, codecs, store and writer body; owner/CF and backend syscall
 *     wrappers are controlled. No live Start acceptance or protection transfer.
 *     Spec: 2026-11-01-pre2-e1-format-closure-v1.2-path-c.md
 *-------------------------------------------------------------------------
 */
#define CONTROL_DEBT_RESTART_TEST_MAIN debt_publish_old_restart_main
int debt_publish_old_restart_main(void);
#include "test_cluster_control_debt_restart.c"
#include "utils/resowner.h"
#include "storage/lwlock.h"
#include "cluster/cluster_control_debt_transition.h"

ResourceOwner CurrentResourceOwner = (ResourceOwner)(uintptr_t)1;
int MyProcPid;
volatile uint32 CritSectionCount;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
void pg_re_throw(void);
void pg_re_throw(void) { siglongjmp(*PG_exception_stack, 1); }
static bool publisher_lwlock;
void ForEachLWLockHeldByMe(void (*callback)(LWLock *, LWLockMode, void *), void *p);
void ForEachLWLockHeldByMe(void (*callback)(LWLock *, LWLockMode, void *), void *p)
{ if (publisher_lwlock) callback(NULL, LW_EXCLUSIVE, p); }

static char publisher_start[2048], publisher_selection[2048];
static bool publisher_start_distinct(const struct ClusterRecoveryStartV1 *s, const void *p, Size n)
{ return s == (const void *)publisher_start && !debt_overlap(s, sizeof(publisher_start), p, n); }
static bool publisher_selection_distinct(const ClusterControlRelmapSelectionV2 *s, const void *p, Size n)
{ return s == (const void *)publisher_selection && !debt_overlap(s, sizeof(publisher_selection), p, n); }
#define cluster_control_root_debt_transition_get_v2 publisher_transition_get_v2
#define cluster_recovery_plan_output_distinct_v1 publisher_start_distinct
#define cluster_control_root_relmap_selection_output_distinct_v4 publisher_selection_distinct
#include "../../backend/cluster/cluster_control_debt_transition_v2.inc"
#undef cluster_control_root_relmap_selection_output_distinct_v4
#undef cluster_recovery_plan_output_distinct_v1

static unsigned publisher_renames, publisher_fault, publisher_fault_at;
static char publisher_primary[MAXPGPATH], publisher_backup[MAXPGPATH];
static bool publisher_regular(const char *path, bool absent)
{ struct stat st; return lstat(path, &st) ? absent && errno == ENOENT : S_ISREG(st.st_mode); }
static bool publisher_parent_sync(void) { return fsync(store_global) == 0; }
static ClusterControlRootResult publisher_legacy_read(const char *p, uint8 *b)
{ (void)p; (void)b; abort(); }
static int publisher_rename(const char *a, const char *b, int severity)
{
	(void)severity;
	++publisher_renames;
	if (publisher_fault_at == publisher_renames && publisher_fault == 1) return -1;
	if (rename(a, b) || fsync(store_global)) return -1;
	if (publisher_fault_at == publisher_renames) {
		if (publisher_fault == 2) cf_held = false;
		if (publisher_fault == 3) restart_cf_sequence++;
		if (publisher_fault == 4) {
			uint8 hash[32], byte; char file[96]; int dir, fd;
			debt_sha(image->pages + image->page_bytes - 33024, 33024, hash);
			debt_store_name(hash, false, file);
			dir = openat(store_global, "pgrac_recovery_registry", O_RDONLY | O_DIRECTORY);
			fd = dir < 0 ? -1 : openat(dir, file, O_RDWR);
			if (fd < 0 || pread(fd, &byte, 1, 257) != 1) abort();
			byte ^= 1;
			if (pwrite(fd, &byte, 1, 257) != 1) abort();
			close(fd); close(dir);
		}
		if (publisher_fault == 5) siglongjmp(*PG_exception_stack, 1);
		if (publisher_fault == 6) {
			char name[96]; int dir;
			debt_store_basis_name(debt_batches[restart_nb - 1].basis_ref, name);
			dir = openat(store_global, "pgrac_recovery_registry", O_RDONLY | O_DIRECTORY);
			if (dir < 0 || unlinkat(dir, name, 0)) abort();
			close(dir);
		}
	}
	return 0;
}

#ifndef DEBT_PUBLISH_BASELINE
#define regular_or_absent_nosymlink publisher_regular
#define fsync_parent_global publisher_parent_sync
#define read_exact_file publisher_legacy_read
#define BasicOpenFilePerm open
#define OpenTransientFile open
#define CloseTransientFile close
#define durable_rename publisher_rename
#define pg_fsync fsync
#define history_ranges_overlap debt_overlap
int cluster_node_id;
#include "test_cluster_control_debt_writer.inc"
#include "../../backend/cluster/cluster_control_debt_publish_v2.inc"
#undef history_ranges_overlap
#undef pg_fsync
#undef durable_rename
#undef CloseTransientFile
#undef OpenTransientFile
#undef BasicOpenFilePerm
#undef read_exact_file
#undef fsync_parent_global
#undef regular_or_absent_nosymlink
#else
/* Missing physical publisher: an unpublished candidate cannot alter ROOT. */
static bool control_debt_transition_publish_files_v2(ClusterControlDebtTransitionV2 *t,
	int fd, const char *p, const char *b, Size budget)
{ (void)t; (void)fd; (void)p; (void)b; (void)budget; return false; }
#endif
#undef cluster_control_root_debt_transition_get_v2

static ClusterControlDebtTransitionV2 *publisher_begin(uint64 n, unsigned nb)
{
	MyProcPid = (int)getpid(); CurrentResourceOwner = (ResourceOwner)(uintptr_t)1;
	publisher_fault = publisher_fault_at = publisher_renames = 0;
	publisher_lwlock = false; CritSectionCount = 0;
	restart_begin(n, nb);
	if (snprintf(publisher_primary, sizeof(publisher_primary), "%s/pgrac_control_root", store_path) >= sizeof(publisher_primary)
		|| snprintf(publisher_backup, sizeof(publisher_backup), "%s/pgrac_control_root.bak", store_path) >= sizeof(publisher_backup)) abort();
	return control_debt_transition_build_v2((const void *)publisher_start,
		(const void *)publisher_selection, restart_before, sizeof(restart_before),
		image, debt_batches, nb, RESTART_BUDGET);
}
static bool publisher_run(ClusterControlDebtTransitionV2 *t)
{ return control_debt_transition_publish_files_v2(t, store_global, publisher_primary, publisher_backup, RESTART_BUDGET); }
static void publisher_end(ClusterControlDebtTransitionV2 **t)
{ cf_held = true; CritSectionCount = 0; publisher_lwlock = false; control_debt_transition_destroy_v2(t); restart_end(); }
static bool publisher_pair_matches(const uint8 *pair)
{
	uint8 *actual = malloc(133120); ControlRootImage *meta = malloc(sizeof(*meta));
	bool ok = actual && meta && cluster_control_root_registry_capture_v4(store_global,
		restart_storage, restart_system, actual, meta) && !memcmp(actual, pair, 133120);
	free(meta); free(actual); return ok;
}
UT_TEST(publisher_replaces_only_primary_and_reloads_every_batch_page)
{
	for (unsigned i = 0; i < 3; i++) {
		restart_distinct_bases = i == 1;
		ClusterControlDebtTransitionV2 *t = publisher_begin(i == 0 ? 65 : i == 1 ? 130 : 4097, i == 1 ? 2 : 1);
		ControlDebtWireImageV1 *loaded = NULL;
		UT_ASSERT(t && publisher_run(t));
		UT_ASSERT_EQ(publisher_renames, 2);
		UT_ASSERT(publisher_pair_matches(restart_after));
		UT_ASSERT(control_debt_store_load_selected_v4(store_global, restart_after, 133120,
			restart_storage, restart_system, RESTART_BUDGET, &loaded));
		UT_ASSERT(loaded && !memcmp(loaded->pages, image->pages, image->page_bytes));
		if (loaded) UT_ASSERT(!memcmp(loaded->registry, image->registry, image->registry_bytes));
		unsigned before = writes;
		UT_ASSERT(!publisher_run(t)); UT_ASSERT_EQ(writes, before);
		control_debt_wire_destroy_v1(&loaded); publisher_end(&t);
	}
}
UT_TEST(publisher_complete_before_mismatch_writes_nothing_and_cannot_retry)
{
	ClusterControlDebtTransitionV2 *t = publisher_begin(65, 1);
	write_root(store_global, "pgrac_control_root.bak", restart_after);
	reset_faults(); UT_ASSERT(!publisher_run(t));
	UT_ASSERT_EQ(writes, 0); UT_ASSERT_EQ(publisher_renames, 0);
	write_root(store_global, "pgrac_control_root.bak", restart_before + 66560);
	UT_ASSERT(!publisher_run(t)); UT_ASSERT_EQ(writes, 0);
	publisher_end(&t);
}
UT_TEST(publisher_lock_or_owner_refusal_precedes_all_mutation)
{
	for (unsigned i = 0; i < 5; i++) {
		ClusterControlDebtTransitionV2 *t = publisher_begin(65, 1);
		reset_faults();
		if (i == 0) cf_held = false;
		if (i == 1) restart_cf_sequence++;
		if (i == 2) CurrentResourceOwner = (ResourceOwner)(uintptr_t)2;
		if (i == 3) publisher_lwlock = true;
		if (i == 4) CritSectionCount = 1;
		UT_ASSERT(!publisher_run(t)); UT_ASSERT_EQ(writes, 0); UT_ASSERT_EQ(publisher_renames, 0);
		CurrentResourceOwner = (ResourceOwner)(uintptr_t)1;
		publisher_end(&t);
	}
}
UT_TEST(publisher_failed_store_does_not_publish_or_erase_debt)
{
	for (unsigned i = 0; i < 3; i++) {
		ClusterControlDebtTransitionV2 *t = publisher_begin(65, 1);
		if (i == 0) fail_write = 2;
		if (i == 1) fail_sync = 2;
		if (i == 2) fail_read = 5;
		UT_ASSERT(!publisher_run(t)); UT_ASSERT_EQ(publisher_renames, 0);
		unsigned before = writes;
		reset_faults(); UT_ASSERT(publisher_pair_matches(restart_before));
		UT_ASSERT(!publisher_run(t)); UT_ASSERT_EQ(writes, 0);
		UT_ASSERT(before > 0 || i == 2); publisher_end(&t);
	}
}
UT_TEST(publisher_failure_after_each_root_step_is_sticky_and_preserves_objects)
{
	for (unsigned step = 1; step <= 2; step++) for (unsigned fault = 1; fault <= 3; fault++) {
		ClusterControlDebtTransitionV2 *t = publisher_begin(65, 1);
		publisher_fault = fault; publisher_fault_at = step;
		UT_ASSERT(!publisher_run(t));
		unsigned before = writes, renames = publisher_renames;
		cf_held = true; publisher_fault = 0;
		UT_ASSERT(!publisher_run(t)); UT_ASSERT_EQ(writes, before); UT_ASSERT_EQ(publisher_renames, renames);
		ControlDebtWireImageV1 *loaded = NULL;
		UT_ASSERT(control_debt_store_load_v1(store_global, image->reference, RESTART_BUDGET, &loaded));
		UT_ASSERT(loaded && !memcmp(loaded->pages, image->pages, image->page_bytes));
		control_debt_wire_destroy_v1(&loaded); publisher_end(&t);
	}
}
UT_TEST(publisher_final_readback_includes_every_page_and_old_basis)
{
	for (unsigned fault = 4; fault <= 6; fault += 2) {
		restart_distinct_bases = true;
		ClusterControlDebtTransitionV2 *t = publisher_begin(130, 2);
		publisher_fault = fault; publisher_fault_at = 2;
		UT_ASSERT(!publisher_run(t)); UT_ASSERT_EQ(publisher_renames, 2);
		UT_ASSERT(publisher_pair_matches(restart_after));
		unsigned before = writes;
		UT_ASSERT(!publisher_run(t)); UT_ASSERT_EQ(writes, before);
		publisher_end(&t);
	}
}
UT_TEST(publisher_error_after_primary_preserves_once_only_attempt_and_files)
{
	ClusterControlDebtTransitionV2 *t = publisher_begin(65, 1);
	volatile bool caught = false;
	publisher_fault = 5; publisher_fault_at = 2;
	PG_TRY(); { (void)publisher_run(t); }
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	UT_ASSERT(caught); UT_ASSERT(publisher_pair_matches(restart_after));
	unsigned before = writes;
	UT_ASSERT(!publisher_run(t)); UT_ASSERT_EQ(writes, before);
	publisher_end(&t);
}
UT_TEST(publisher_budget_and_wrong_directory_cannot_be_retried)
{
	for (unsigned i = 0; i < 2; i++) {
		ClusterControlDebtTransitionV2 *t = publisher_begin(65, 1);
		int other = openat(store_global, "pgrac_recovery_registry", O_RDONLY | O_DIRECTORY);
		UT_ASSERT(other >= 0);
		UT_ASSERT(!control_debt_transition_publish_files_v2(t, i ? other : store_global,
			publisher_primary, publisher_backup, i ? RESTART_BUDGET : 1024));
		UT_ASSERT_EQ(writes, 0); UT_ASSERT_EQ(publisher_renames, 0);
		if (other >= 0) close(other);
		UT_ASSERT(!publisher_run(t)); UT_ASSERT_EQ(writes, 0);
		publisher_end(&t);
	}
}
int main(void)
{
	UT_PLAN(8);
	UT_RUN(publisher_replaces_only_primary_and_reloads_every_batch_page);
	UT_RUN(publisher_complete_before_mismatch_writes_nothing_and_cannot_retry);
	UT_RUN(publisher_lock_or_owner_refusal_precedes_all_mutation);
	UT_RUN(publisher_failed_store_does_not_publish_or_erase_debt);
	UT_RUN(publisher_failure_after_each_root_step_is_sticky_and_preserves_objects);
	UT_RUN(publisher_final_readback_includes_every_page_and_old_basis);
	UT_RUN(publisher_error_after_primary_preserves_once_only_attempt_and_files);
	UT_RUN(publisher_budget_and_wrong_directory_cannot_be_retried);
	UT_DONE(); return ut_failed_count != 0;
}
