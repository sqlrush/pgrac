/* Actual SMGR staged writer/real files; finite ResourceOwner callbacks/table. */
#include "postgres.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include <unistd.h>
#include <sys/wait.h>

static bool duplicate_fail;
static int duplicate_fd = -1;
static int writer_test_dup(int fd);
#define dup writer_test_dup
#define UNDO_NATIVE_TT_EXIT_TEST
#define UNDO_NATIVE_TT_HEADER_IO_MAIN native_tt_header_original_main
int native_tt_header_original_main(void);
#include "test_cluster_undo_native_tt_header_io.c"
#undef dup

ResourceOwner CurrentResourceOwner = (ResourceOwner)(uintptr_t)1;
MemoryContext TopMemoryContext = (MemoryContext)(uintptr_t)1;
static ResourceReleaseCallback writer_release;
static void *writer_release_arg;
static pg_on_exit_callback exit_callbacks[8];
static Datum exit_args[8];
static unsigned exit_count, allocations, frees;

int errcode_for_file_access(void) { return 0; }

void *MemoryContextAlloc(MemoryContext context, Size size)
{
	void *p;
	if (context != TopMemoryContext) abort();
	p = malloc(size); if (p == NULL) abort(); allocations++; return p;
}
void pfree(void *p) { frees++; free(p); }
void RegisterResourceReleaseCallback(ResourceReleaseCallback callback, void *arg)
{ if (writer_release != NULL) abort(); writer_release = callback; writer_release_arg = arg; }
void before_shmem_exit(pg_on_exit_callback callback, Datum arg)
{
	if (exit_count >= lengthof(exit_callbacks)) abort();
	exit_callbacks[exit_count] = callback; exit_args[exit_count++] = arg;
}
static int writer_test_dup(int fd)
{
	if (duplicate_fail) { errno = EMFILE; return -1; }
	duplicate_fd = dup(fd); return duplicate_fd;
}
static void writer_reset(PGAlignedBlock *page, char path[MAXPGPATH])
{
	header_reset(page, path); CurrentResourceOwner = (ResourceOwner)(uintptr_t)1;
	duplicate_fail = false; duplicate_fd = -1;
	UT_ASSERT_EQ(allocations, frees);
}
static int writer_open(void)
{ return cluster_undo_smgr_header_writer_open(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1); }
static bool writer_write(int handle, const PGAlignedBlock *next)
{ return cluster_undo_smgr_header_writer_write(handle, CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, 0, next->data, BLCKSZ); }
static void writer_cleanup(ResourceOwner owner, ResourceReleasePhase phase)
{
	ResourceOwner save = CurrentResourceOwner;
	CurrentResourceOwner = owner;
	if (writer_release != NULL) writer_release(phase, false, true, writer_release_arg);
	else UT_ASSERT_NOT_NULL(writer_release);
	CurrentResourceOwner = save;
}

UT_TEST(staged_writer_keeps_identity_through_private_fd_and_close)
{
	PGAlignedBlock page, next;
	char path[MAXPGPATH];
	int h;
	unsigned before, acquired;

	writer_reset(&page, path); write_successor(&page, &next); h = writer_open();
	UT_ASSERT(h >= 0); UT_ASSERT_EQ(selected_borrows, 1); acquired = selected_acquires;
	cluster_undo_smgr_fd_cache_reset(); basic_open_forced_error = true;
	before = allocations; checkpoint_accept = true;
	UT_ASSERT(writer_write(h, &next)); UT_ASSERT(checkpoint_custody_ok);
	UT_ASSERT_EQ(allocations, before); UT_ASSERT_EQ(selected_acquires, acquired);
	UT_ASSERT_EQ(selected_borrows, 1); assert_disk(path, &next);
	cluster_undo_smgr_header_writer_close(h); UT_ASSERT_EQ(allocations, frees);
	cluster_undo_smgr_header_writer_close(h); finish_selected(path);
}

UT_TEST(staged_writer_open_refuses_without_matching_file)
{
	PGAlignedBlock page;
	char path[MAXPGPATH];
	unsigned variant;
	int h;

	for (variant = 0; variant < 8; variant++) {
		writer_reset(&page, path);
		if (variant == 0) selected_missing = true;
		if (variant == 1) selected.creation_generation++;
		if (variant == 2) selected_current = false;
		if (variant == 3) pread_forced_error = true;
		if (variant == 4) UT_ASSERT_EQ(truncate(path, BLCKSZ - 1), 0);
		if (variant == 5) basic_open_forced_error = true;
		if (variant == 6) duplicate_fail = true;
		if (variant == 7) selected_change_on = 3;
		h = writer_open(); UT_ASSERT(h < 0);
		cluster_undo_smgr_header_writer_close(h);
		UT_ASSERT_EQ(allocations, frees); finish_selected(path);
	}
}

UT_TEST(staged_writer_open_error_cleans_original_duty)
{
	PGAlignedBlock page;
	char path[MAXPGPATH];
	unsigned at;

	for (at = 1; at <= 3; at++) {
		volatile bool caught = false;
		volatile int h = -1;

		writer_reset(&page, path); selected_throw_on = at;
		PG_TRY(); { h = writer_open(); } PG_CATCH(); { caught = true; } PG_END_TRY();
		UT_ASSERT(caught); cluster_undo_smgr_header_writer_close(h);
		UT_ASSERT_EQ(allocations, frees); assert_disk(path, &page); finish_selected(path);
	}
}

UT_TEST(staged_writer_checks_handle_original_owner_and_key)
{
	PGAlignedBlock page, next;
	char path[MAXPGPATH];
	int h;

	writer_reset(&page, path); write_successor(&page, &next); h = writer_open();
	UT_ASSERT(h >= 0);
	UT_ASSERT(!cluster_undo_smgr_header_writer_write(h, CLUSTER_UNDO_PATH_RECOVERY_SHARED, 1, 1, 0, next.data, BLCKSZ));
	UT_ASSERT(!cluster_undo_smgr_header_writer_write(h, CLUSTER_UNDO_PATH_RUNTIME_SHARED, 2, 1, 0, next.data, BLCKSZ));
	UT_ASSERT(!cluster_undo_smgr_header_writer_write(h, CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 2, 0, next.data, BLCKSZ));
	CurrentResourceOwner = (ResourceOwner)(uintptr_t)2; UT_ASSERT(!writer_write(h, &next));
	CurrentResourceOwner = (ResourceOwner)(uintptr_t)1;
	UT_ASSERT(!writer_write(-1, &next)); UT_ASSERT_EQ(checkpoint_calls, 0);
	assert_disk(path, &page); cluster_undo_smgr_header_writer_close(h); finish_selected(path);
}

UT_TEST(staged_writer_rejects_partial_or_bad_successor)
{
	PGAlignedBlock page, next;
	char path[MAXPGPATH];
	int h;

	writer_reset(&page, path); write_successor(&page, &next); h = writer_open();
	UT_ASSERT(h >= 0);
	UT_ASSERT(!cluster_undo_smgr_header_writer_write(h, CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1, 1, 128, next.data, 40));
	next.data[3100] ^= 1; UT_ASSERT(!writer_write(h, &next));
	UT_ASSERT_EQ(checkpoint_calls, 0); assert_disk(path, &page);
	cluster_undo_smgr_header_writer_close(h); finish_selected(path);
}

UT_TEST(staged_writer_rechecks_control_and_disk_before_mutation)
{
	PGAlignedBlock page, next;
	char path[MAXPGPATH];
	unsigned variant;
	int h;

	for (variant = 0; variant < 3; variant++) {
		writer_reset(&page, path); write_successor(&page, &next); h = writer_open();
		UT_ASSERT(h >= 0); selected_reads = 0;
		if (variant == 0) selected_current = false;
		if (variant == 1) selected_change_on = 2;
		if (variant == 2) {
			int fd = open(path, O_WRONLY);
			UT_ASSERT(fd >= 0); page.data[400] ^= 1;
			UT_ASSERT_EQ(pwrite(fd, page.data, BLCKSZ, 0), BLCKSZ); UT_ASSERT_EQ(close(fd), 0);
		}
		UT_ASSERT(!writer_write(h, &next)); UT_ASSERT_EQ(selected_borrows, 1);
		UT_ASSERT_EQ(checkpoint_calls, 0); assert_disk(path, &page);
		cluster_undo_smgr_header_writer_close(h); finish_selected(path);
	}
}

UT_TEST(staged_writer_late_failure_retains_effects_until_close)
{
	PGAlignedBlock page, next;
	char path[MAXPGPATH];
	unsigned variant;
	int h;

	for (variant = 0; variant < 3; variant++) {
		writer_reset(&page, path); write_successor(&page, &next); h = writer_open();
		UT_ASSERT(h >= 0); selected_reads = 0;
		if (variant == 0) pwrite_forced_error = true;
		if (variant == 1) fsync_fail_on_call = fsync_calls + 1;
		if (variant == 2) { selected_change_on = 3; checkpoint_accept = true; }
		UT_ASSERT(!writer_write(h, &next)); UT_ASSERT_EQ(selected_borrows, 1);
		assert_disk(path, variant == 0 ? &page : &next);
		cluster_undo_smgr_header_writer_close(h); UT_ASSERT_EQ(allocations, frees); finish_selected(path);
	}
}

UT_TEST(staged_writer_error_waits_for_original_resource_cleanup)
{
	PGAlignedBlock page, next;
	char path[MAXPGPATH];
	int h;
	volatile bool caught = false;

	writer_reset(&page, path); write_successor(&page, &next); h = writer_open();
	UT_ASSERT(h >= 0); selected_throw_on = selected_reads + 1;
	PG_TRY(); { (void) writer_write(h, &next); } PG_CATCH(); { caught = true; } PG_END_TRY();
	UT_ASSERT(caught); UT_ASSERT_EQ(selected_borrows, 1);
	writer_cleanup(CurrentResourceOwner, RESOURCE_RELEASE_AFTER_LOCKS);
	UT_ASSERT_EQ(selected_borrows, 1);
	writer_cleanup(CurrentResourceOwner, RESOURCE_RELEASE_BEFORE_LOCKS);
	UT_ASSERT_EQ(selected_borrows, 0); UT_ASSERT_EQ(allocations, frees);
	cluster_undo_smgr_header_writer_close(h); assert_disk(path, &page); finish_selected(path);
}

UT_TEST(staged_writer_cleanup_preserves_other_owner_and_blocks_fd_aba)
{
	PGAlignedBlock page, next;
	char path[MAXPGPATH];
	int a, b, c, fd_a;

	writer_reset(&page, path); write_successor(&page, &next); a = writer_open(); fd_a = duplicate_fd;
	CurrentResourceOwner = (ResourceOwner)(uintptr_t)2; b = writer_open();
	UT_ASSERT(a >= 0 && b >= 0); UT_ASSERT_EQ(selected_borrows, 2);
	writer_cleanup((ResourceOwner)(uintptr_t)1, RESOURCE_RELEASE_BEFORE_LOCKS);
	UT_ASSERT_EQ(selected_borrows, 1); c = writer_open();
	UT_ASSERT(c >= 0 && c != a); UT_ASSERT_EQ(duplicate_fd, fd_a);
	cluster_undo_smgr_header_writer_close(a); UT_ASSERT_EQ(selected_borrows, 2);
	cluster_undo_smgr_header_writer_close(c); UT_ASSERT_EQ(selected_borrows, 1);
	checkpoint_accept = true; UT_ASSERT(writer_write(b, &next));
	writer_cleanup(CurrentResourceOwner, RESOURCE_RELEASE_BEFORE_LOCKS);
	UT_ASSERT_EQ(selected_borrows, 0); cluster_undo_smgr_header_writer_close(b);
	UT_ASSERT_EQ(allocations, frees); assert_disk(path, &next); finish_selected(path);
}

UT_TEST(staged_writer_requires_original_resource_owner_before_open)
{
	PGAlignedBlock page;
	char path[MAXPGPATH];
	int h;

	writer_reset(&page, path); CurrentResourceOwner = NULL;
	h = writer_open(); UT_ASSERT(h < 0); UT_ASSERT_EQ(cached_fd, -1);
	cluster_undo_smgr_header_writer_close(h); UT_ASSERT_EQ(allocations, frees); finish_selected(path);
}

UT_TEST(staged_writer_original_exit_callback_retires_all_private_fds)
{
	PGAlignedBlock page;
	char path[MAXPGPATH];
	unsigned i;
	int h;

	writer_reset(&page, path); h = writer_open(); UT_ASSERT(h >= 0);
	UT_ASSERT_EQ(selected_borrows, 1);
	for (i = exit_count; i > 0; i--) exit_callbacks[i - 1](0, exit_args[i - 1]);
	UT_ASSERT_EQ(selected_borrows, 0); UT_ASSERT_EQ(allocations, frees);
	cluster_undo_smgr_header_writer_close(h); finish_selected(path);
}

UT_TEST(staged_writer_release_failure_panics_instead_of_erasing_duty)
{
	PGAlignedBlock page;
	char path[MAXPGPATH];
	pid_t child;
	int status;

	writer_reset(&page, path); fflush(NULL); child = fork(); UT_ASSERT(child >= 0);
	if (child == 0) {
		int h = writer_open();
		selected_release_ok = false; cluster_undo_smgr_header_writer_close(h); _exit(0);
	}
	UT_ASSERT_EQ(waitpid(child, &status, 0), child);
	UT_ASSERT(WIFSIGNALED(status) && WTERMSIG(status) == SIGABRT); finish_selected(path);
}

UT_TEST(staged_writer_keeps_legacy_range_and_checkpoint_policy)
{
	PGAlignedBlock page;
	char path[MAXPGPATH];
	int h;

	writer_reset(&page, path); selected_mode = CLUSTER_NATIVE_FILES_UNKNOWN_V2;
	cluster_enabled = false; expect_identity = false; checkpoint_accept = true;
	h = writer_open(); UT_ASSERT(h >= 0);
	UT_ASSERT(cluster_undo_smgr_header_writer_write(h, CLUSTER_UNDO_PATH_RUNTIME_SHARED,
		1, 1, 128, page.data, 40));
	UT_ASSERT_EQ(checkpoint_calls, 1); UT_ASSERT_EQ(selected_acquires, 0);
	cluster_undo_smgr_header_writer_close(h); UT_ASSERT_EQ(allocations, frees); finish_selected(path);
}

int main(void)
{
	char path[] = "/tmp/pgrac-native-tt-staged-XXXXXX";

	UT_ASSERT_NOT_NULL(mkdtemp(path)); strlcpy(publication_dir, path, sizeof(publication_dir));
	UT_PLAN(13);
	UT_RUN(staged_writer_keeps_identity_through_private_fd_and_close);
	UT_RUN(staged_writer_open_refuses_without_matching_file);
	UT_RUN(staged_writer_open_error_cleans_original_duty);
	UT_RUN(staged_writer_checks_handle_original_owner_and_key);
	UT_RUN(staged_writer_rejects_partial_or_bad_successor);
	UT_RUN(staged_writer_rechecks_control_and_disk_before_mutation);
	UT_RUN(staged_writer_late_failure_retains_effects_until_close);
	UT_RUN(staged_writer_error_waits_for_original_resource_cleanup);
	UT_RUN(staged_writer_cleanup_preserves_other_owner_and_blocks_fd_aba);
	UT_RUN(staged_writer_requires_original_resource_owner_before_open);
	UT_RUN(staged_writer_original_exit_callback_retires_all_private_fds);
	UT_RUN(staged_writer_release_failure_panics_instead_of_erasing_duty);
	UT_RUN(staged_writer_keeps_legacy_range_and_checkpoint_policy);
	UT_ASSERT_EQ(rmdir(publication_dir), 0); UT_DONE(); return ut_failed_count ? 1 : 0;
}
