/* Actual bootstrap wrapper with original directory/file I/O; no WAL provider. */
#include "postgres.h"
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include "common/cluster_recovery_bootstrap.h"
#include "storage/fd.h"
#ifdef CLOG_BOOTSTRAP_TEST_SOURCE
#include CLOG_BOOTSTRAP_TEST_SOURCE
#else
#include "../../backend/access/transam/clog.c"
#endif
#undef printf
#undef fprintf
#undef snprintf
#include "unit_test.h"
UT_DEFINE_GLOBALS();
ProcessingMode Mode;
bool IsUnderPostmaster, InRecovery, enableFsync;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
static uint64 system_id;
static unsigned opens, closes, directory_syncs;
static bool throw_sync, fail_close;
static char root[MAXPGPATH];
static ClusterRecoveryFileIdentityV2 identity;
static ClusterRecoveryBootFileV2 out, sentinel;
static int opened_fd;

void ExceptionalCondition(const char *c, const char *f, int l)
{ fprintf(stderr, "%s at %s:%d\n", c, f, l); abort(); }
uint64 GetSystemIdentifier(void) { return system_id; }
int OpenTransientFile(const char *name, int flags)
{
	opens++; opened_fd = open(name, flags, 0600); return opened_fd;
}
int CloseTransientFile(int fd)
{
	int rc;

	UT_ASSERT_EQ(fd, opened_fd); closes++; rc = close(fd);
	return fail_close ? -1 : rc;
}
void fsync_fname(const char *name, bool isdir)
{
	int fd;

	UT_ASSERT(isdir); directory_syncs++;
	if (throw_sync) siglongjmp(*PG_exception_stack, 1);
	fd = open(name, O_RDONLY | O_DIRECTORY);
	if (fd < 0 || fsync(fd) != 0 || close(fd) != 0) abort();
}
void pg_re_throw(void)
{
	if (PG_exception_stack == NULL) abort();
	siglongjmp(*PG_exception_stack, 1);
}
static void reset(void)
{
	char path[MAXPGPATH];

	snprintf(path, sizeof(path), "%s/0000", root); unlink(path);
	memset(&identity, 0, sizeof(identity));
	identity.kind = CLUSTER_RECOVERY_FILE_CLOG_V2;
	identity.format_id = CLUSTER_RECOVERY_FORMAT_ID_V2;
	identity.format_version = CLUSTER_RECOVERY_FORMAT_VERSION_V2;
	identity.system_identifier = system_id = 101;
	identity.database_incarnation = 103;
	identity.storage_uuid[0] = 5; identity.redo_branch_uuid[0] = 6;
	identity.redo_branch_generation = 107; identity.redo_timeline = 2;
	identity.creation_generation = 109; identity.registration_generation = 113;
	memset(&sentinel, 0xA5, sizeof(sentinel)); out = sentinel;
	Mode = BootstrapProcessing; IsUnderPostmaster = InRecovery = false; enableFsync = true;
	opens = closes = directory_syncs = 0; throw_sync = fail_close = false;
	strlcpy(XactCtl->Dir, root, sizeof(XactCtl->Dir));
}
static bool create(void)
{ return BootStrapCLOGManagedV2(&identity, FullTransactionIdFromU64(3), &out); }
#ifdef USE_PGRAC_CLUSTER
UT_TEST(original_bootstrap_owner_writes_syncs_and_closes) {
	ClusterRecoveryBootFileV2 observed;
	char path[MAXPGPATH];
	int fd;

	reset(); UT_ASSERT(create()); UT_ASSERT_EQ(opens, 1); UT_ASSERT_EQ(closes, 1);
	UT_ASSERT_EQ(directory_syncs, 1); UT_ASSERT_EQ(out.synced_bytes, 16448);
	snprintf(path, sizeof(path), "%s/0000", root); fd = open(path, O_RDONLY);
	UT_ASSERT(fd >= 0);
	UT_ASSERT(cluster_clog_bootstrap_read_v2(fd, &identity, &observed));
	UT_ASSERT(memcmp(&observed, &out, sizeof(out)) == 0); close(fd);
	out = sentinel; UT_ASSERT(!create()); UT_ASSERT_EQ(closes, 1);
	UT_ASSERT(memcmp(&out, &sentinel, sizeof(out)) == 0);
}
UT_TEST(bootstrap_context_and_expected_guards_precede_create) {
	for (unsigned i = 0; i < 8; i++) {
		reset();
		if (i == 0) Mode = NormalProcessing;
		if (i == 1) Mode = InitProcessing;
		if (i == 2) IsUnderPostmaster = true;
		if (i == 3) InRecovery = true;
		if (i == 4) enableFsync = false;
		if (i == 5) system_id++;
		if (i == 6) identity.kind = CLUSTER_RECOVERY_FILE_UNDO_V2;
		if (i == 7) identity.object_id = 1;
		UT_ASSERT(!create()); UT_ASSERT_EQ(opens, 0);
		UT_ASSERT(memcmp(&out, &sentinel, sizeof(out)) == 0);
	}
}
UT_TEST(directory_sync_error_closes_and_preserves_partial_out) {
	bool caught = false;

	reset(); throw_sync = true;
	PG_TRY(); { (void)create(); } PG_CATCH(); { caught = true; } PG_END_TRY();
	UT_ASSERT(caught); UT_ASSERT_EQ(closes, 1); UT_ASSERT_EQ(directory_syncs, 1);
	UT_ASSERT(fcntl(opened_fd, F_GETFD) < 0);
	UT_ASSERT(memcmp(&out, &sentinel, sizeof(out)) == 0);
}
UT_TEST(close_failure_preserves_out_and_does_not_unlink) {
	char path[MAXPGPATH];
	struct stat st;

	reset(); fail_close = true;
	UT_ASSERT(!create()); UT_ASSERT_EQ(closes, 1);
	UT_ASSERT(memcmp(&out, &sentinel, sizeof(out)) == 0);
	snprintf(path, sizeof(path), "%s/0000", root);
	UT_ASSERT_EQ(stat(path, &st), 0); UT_ASSERT_EQ(st.st_size, 16448);
}
#else
UT_TEST(disabled_bootstrap_never_creates) {
	reset(); UT_ASSERT(!create()); UT_ASSERT_EQ(opens, 0);
	UT_ASSERT(memcmp(&out, &sentinel, sizeof(out)) == 0);
}
#endif
int main(void)
{
	char path[] = "/tmp/pgrac-clog-boot-XXXXXX";
	char file[MAXPGPATH];

	if (mkdtemp(path) == NULL) abort();
	strlcpy(root, path, sizeof(root));
#ifdef USE_PGRAC_CLUSTER
	UT_PLAN(4);
	UT_RUN(original_bootstrap_owner_writes_syncs_and_closes);
	UT_RUN(bootstrap_context_and_expected_guards_precede_create);
	UT_RUN(directory_sync_error_closes_and_preserves_partial_out);
	UT_RUN(close_failure_preserves_out_and_does_not_unlink);
#else
	UT_PLAN(1); UT_RUN(disabled_bootstrap_never_creates);
#endif
	snprintf(file, sizeof(file), "%s/0000", root); unlink(file); rmdir(root);
	UT_DONE(); return ut_failed_count != 0;
}
