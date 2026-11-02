/* Exact original Startup owner/dispatch and creation file checker. Process,
 * table and path selection are finite providers; BOOT TT file bytes, checksum,
 * complete readback and identity codecs are real. No recovery owner is minted. */
#include "postgres.h"
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include "access/xlogrecovery.h"
#include "access/xlog.h"
#include "access/clog_file.h"
#include "catalog/pg_tablespace_d.h"
#include "cluster/cluster_control_native_files.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_backup.h"
#include "cluster/cluster_tt_durable.h"
#include "cluster/cluster_semantic_activation.h"
#include "cluster/cluster_tt_startup.h"
#include "cluster/cluster_undo_smgr.h"
#include "cluster/cluster_undo_segment_init.h"
#include "cluster/cluster_wal_thread.h"
#include "common/cluster_recovery_bootstrap.h"
#include "miscadmin.h"
#include "storage/fd.h"
#undef printf
#undef fprintf
#undef snprintf
#include "unit_test.h"
UT_DEFINE_GLOBALS();

bool cluster_enabled, cluster_shared_config, cluster_tt_recovery_resolve_active;
int cluster_node_id;
bool IsUnderPostmaster, IsPostmasterEnvironment;
BackendType MyBackendType;
AuxProcType MyAuxProcType;
ProcessingMode Mode;
static int LocalXLogInsertAllowed = 1;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
static bool recovery, hot, request_present, info_present;
static ClusterControlNativeTableInfoV2 selected;
static PgracInitdbRecoveryContext request;
static unsigned acquires, releases, held, checks, infos, opens, boot_reads, jobs[4];
static unsigned drift_at, throw_at;
static bool missing, bad_borrow, bad_generation;
static char directory[MAXPGPATH], filename[MAXPGPATH];

void ExceptionalCondition(const char *c, const char *f, int l)
{ fprintf(stderr, "%s %s:%d\n", c, f, l); abort(); }
void pg_re_throw(void)
{ if (!PG_exception_stack) abort(); siglongjmp(*PG_exception_stack, 1); }
bool errstart(int level pg_attribute_unused(), const char *domain pg_attribute_unused())
{ return true; }
void errfinish(const char *f pg_attribute_unused(), int l pg_attribute_unused(),
			   const char *fn pg_attribute_unused()) { pg_re_throw(); }
int errcode(int code pg_attribute_unused()) { return 0; }
int errmsg(const char *fmt pg_attribute_unused(), ...) { return 0; }
bool RecoveryInProgress(void) { return recovery; }
bool HotStandbyActive(void) { return hot; }
int cluster_tt_recovery_resolve_active_slots(void) { jobs[0]++; return 0; }
int cluster_tt_recovery_observe_scn_highwater(void) { jobs[1]++; return 0; }
void cluster_backup_recovery_observe_highwater(void) { jobs[2]++; }
int cluster_tt_recovery_physical_rollback(void) { jobs[3]++; return 0; }
ClusterNormalStartState cluster_semantic_normal_start_state(void)
{ return CLUSTER_NORMAL_START_UNCLASSIFIED; }
ClusterControlNativeModeV2 cluster_control_native_files_mode_v2(void)
{ return selected.mode; }
const PgracInitdbRecoveryContext *cluster_wal_thread_initdb_recovery_context(void)
{ return request_present ? &request : NULL; }
bool cluster_control_native_files_info_v2(ClusterControlNativeTableInfoV2 *out)
{
	infos++;
	if (!info_present) return false;
	*out = selected;
	if (infos == drift_at) out->table_generation++;
	return true;
}
bool cluster_control_native_file_acquire_v2(const ClusterControlFileKeyV2 *key,
										  ClusterControlNativeFileBorrowV2 *out)
{
	acquires++;
	for (unsigned i = 0; i < request.boot_file_count; i++) {
		const ClusterRecoveryFileIdentityV2 *id = &request.boot_files[i];
		if (key->kind != id->kind || key->origin != id->origin ||
			key->object_id != id->object_id || key->database_oid || key->tablespace_oid) continue;
		if (missing) return false;
		out->expected = *id;
		if (bad_borrow) out->expected.creation_generation++;
		out->table_generation = selected.table_generation + bad_generation;
		out->startup_generation = selected.startup_generation;
		out->private_self = (uintptr_t)out;
		held++;
		return true;
	}
	abort(); /* Never select a key outside the registered batch. */
}
bool cluster_control_native_file_current_v2(const ClusterControlNativeFileBorrowV2 *b)
{
	if (!held || b->private_self != (uintptr_t)b) abort();
	checks++;
	if (checks == throw_at) pg_re_throw();
	return true;
}
bool cluster_control_native_file_release_v2(ClusterControlNativeFileBorrowV2 *b)
{
	if (!held || b->private_self != (uintptr_t)b) abort();
	held--; releases++; memset(b, 0, sizeof(*b)); return true;
}
int cluster_undo_path_resolve(ClusterUndoPathIntent intent, uint8 owner, uint32 segment,
							char *out, size_t size)
{
	if (intent != CLUSTER_UNDO_PATH_MATERIALIZED_LOCAL || owner != 1 || segment != 0) abort();
	return strlcpy(out, filename, size) < size ? 0 : -1;
}
static int pg_attribute_unused() creation_open(const char *path, int flags)
{
	if (!held || strcmp(path, filename) || flags & (O_CREAT | O_RDWR | O_WRONLY)) abort();
	opens++; return open(path, flags);
}
static bool pg_attribute_unused() creation_boot_read(int fd, const ClusterRecoveryFileIdentityV2 *id,
							   ClusterRecoveryBootFileV2 *out)
{
	if (!held) abort();
	boot_reads++;
	return cluster_undo_bootstrap_read_v2(fd, id, out);
}
#define BasicOpenFile creation_open
#define cluster_undo_bootstrap_read_v2 creation_boot_read
#include "test_cluster_native_creation_check.inc"
#undef BasicOpenFile
#undef cluster_undo_bootstrap_read_v2
#include "test_cluster_tt_startup_local_owner.inc"
#include "test_cluster_native_creation_dispatch.inc"

static void seed(bool tt, unsigned origin)
{
	ClusterRecoveryFileIdentityV2 id = {0};
	memset(&request, 0, sizeof(request)); memset(&selected, 0, sizeof(selected));
	id.kind = CLUSTER_RECOVERY_FILE_RELMAP_V2;
	id.format_id = CLUSTER_RECOVERY_FORMAT_ID_V2;
	id.format_version = CLUSTER_RECOVERY_FORMAT_VERSION_V2;
	id.system_identifier = 77; id.database_incarnation = 1;
	id.storage_uuid[0] = 1; id.redo_branch_uuid[0] = 2;
	id.redo_branch_generation = 1; id.redo_timeline = 1;
	id.creation_generation = 10; id.registration_generation = 4;
	id.tablespace_oid = GLOBALTABLESPACE_OID;
	request.first = id; request.creator_pid = 1234;
	request.magic = PGRAC_INITDB_RECOVERY_MAGIC;
	request.version = CLUSTER_RECOVERY_FORMAT_VERSION_V2;
	request.config.fd = 4; request.config.bytes = 100; request.config.sha256[0] = 1;
	request.boot_file_count = tt ? 2 : 1;
	id.kind = CLUSTER_RECOVERY_FILE_CLOG_V2; id.creation_generation = 20;
	id.tablespace_oid = 0;
	id.origin = origin; request.boot_files[0] = id;
	if (tt) {
		id.kind = CLUSTER_RECOVERY_FILE_UNDO_V2; id.creation_generation++;
		request.boot_files[1] = id;
	} else {
		request.cohort[0] = 1;
		request.cohort[origin / 64] |= UINT64CONST(1) << (origin % 64);
		request.cohort_origin = origin;
	}
	selected.mode = CLUSTER_NATIVE_FILES_CREATION_V2;
	selected.table_generation = 41; selected.startup_generation = request.creator_pid;
	selected.creation = request;
	selected.count = request.boot_file_count + (origin == 0 ? 4 : 0);
	request_present = info_present = true;
	cluster_enabled = cluster_tt_recovery_resolve_active = true;
	cluster_shared_config = recovery = hot = false;
	IsUnderPostmaster = IsPostmasterEnvironment = false;
	cluster_node_id = -1; MyBackendType = B_STANDALONE_BACKEND;
	MyAuxProcType = NotAnAuxProcess; Mode = InitProcessing;
	LocalXLogInsertAllowed = 1;
	acquires = releases = held = checks = infos = opens = boot_reads = 0;
	drift_at = throw_at = 0; missing = bad_borrow = bad_generation = false;
	memset(jobs, 0, sizeof(jobs));
}
static bool attempt(void)
{
	volatile bool error = false;
	PG_TRY(); { ClusterTTStartupPostJobs(); }
	PG_CATCH(); { error = true; }
	PG_END_TRY();
	UT_ASSERT_EQ(held, 0);
	UT_ASSERT_EQ(acquires, releases + (missing && acquires != 0));
	UT_ASSERT(StartupLocalTTContext == NULL);
	return error;
}
UT_TEST(creation_dispatch_keeps_disabled_node_and_never_mints_startup_owner)
{
	for (unsigned mode = 0; mode < 8; mode++) {
		seed(false, mode & 1 ? 127 : 0);
		cluster_enabled = (mode & 2) != 0;
		Mode = mode & 4 ? BootstrapProcessing : InitProcessing;
		UT_ASSERT(!attempt());
		UT_ASSERT_EQ(jobs[0] + jobs[1] + jobs[2] + jobs[3], 0);
		UT_ASSERT_EQ(acquires, 1); UT_ASSERT_EQ(opens, 0);
		UT_ASSERT_EQ(cluster_node_id, -1);
		UT_ASSERT(cluster_tt_startup_local_context_current() == NULL);
	}
}
UT_TEST(noncreation_dispatch_preserves_original_jobs_and_owner_refusals)
{
	seed(false, 0); request_present = false; selected.mode = CLUSTER_NATIVE_FILES_UNKNOWN_V2;
	cluster_node_id = 0;
	UT_ASSERT(!attempt());
	for (unsigned i = 0; i < 4; i++) UT_ASSERT_EQ(jobs[i], 1);
	UT_ASSERT_EQ(acquires + infos, 0);
	seed(false, 0); request_present = false; selected.mode = CLUSTER_NATIVE_FILES_UNKNOWN_V2;
	UT_ASSERT(attempt()); UT_ASSERT_EQ(jobs[0] + jobs[1] + jobs[2] + jobs[3], 0);
}
UT_TEST(creation_dispatch_refuses_unqualified_batch_before_recovery_or_file_access)
{
	for (unsigned fault = 0; fault < 18; fault++) {
		seed(false, 0);
		if (fault == 0) request_present = false;
		if (fault == 1) selected.mode = CLUSTER_NATIVE_FILES_UNKNOWN_V2;
		if (fault == 2) info_present = false;
		if (fault == 3) selected.startup_generation++;
		if (fault == 4) selected.creation.creator_pid++;
		if (fault == 5) cluster_node_id = 0;
		if (fault == 6) IsUnderPostmaster = true;
		if (fault == 7) IsPostmasterEnvironment = true;
		if (fault == 8) MyBackendType = B_BACKEND;
		if (fault == 9) Mode = NormalProcessing;
		if (fault == 10) recovery = true;
		if (fault == 11) hot = true;
		if (fault == 12) selected.count++;
		if (fault == 13) selected.reservation_count++;
		if (fault == 14) missing = true;
		if (fault == 15) bad_borrow = true;
		if (fault == 16) bad_generation = true;
		if (fault == 17) drift_at = 2;
		UT_ASSERT(attempt());
		UT_ASSERT_EQ(jobs[0] + jobs[1] + jobs[2] + jobs[3] + opens, 0);
	}
}
UT_TEST(registered_boot_tt_is_read_only_exact_initial_file_and_error_releases_borrow)
{
	for (unsigned fault = 0; fault < 5; fault++) {
		ClusterRecoveryBootFileV2 observed;
		int fd;
		seed(true, 0);
		unlink(filename);
		fd = open(filename, O_RDWR | O_CREAT | O_EXCL, 0600);
		UT_ASSERT(fd >= 0);
		UT_ASSERT(cluster_undo_bootstrap_v2(fd, &request.boot_files[1], &observed));
		if (fault == 1) UT_ASSERT_EQ(ftruncate(fd, BLCKSZ), 0);
		if (fault == 2) {
			uint8 bad = 1;
			UT_ASSERT_EQ(pwrite(fd, &bad, 1, BLCKSZ), 1);
		}
		if (fault == 3) request.boot_files[1].creation_generation++;
		if (fault == 4) throw_at = 3;
		UT_ASSERT_EQ(close(fd), 0);
		UT_ASSERT_EQ(attempt(), fault != 0);
		UT_ASSERT_EQ(jobs[0] + jobs[1] + jobs[2] + jobs[3], 0);
		if (fault == 0) UT_ASSERT_EQ(boot_reads, 1);
		unlink(filename);
	}
}
int main(void)
{
	strlcpy(directory, "/tmp/pgrac-creation-startup-XXXXXX", sizeof(directory));
	if (!mkdtemp(directory)) abort();
	snprintf(filename, sizeof(filename), "%s/seg_0.dat", directory);
	UT_PLAN(4);
	UT_RUN(creation_dispatch_keeps_disabled_node_and_never_mints_startup_owner);
	UT_RUN(noncreation_dispatch_preserves_original_jobs_and_owner_refusals);
	UT_RUN(creation_dispatch_refuses_unqualified_batch_before_recovery_or_file_access);
	UT_RUN(registered_boot_tt_is_read_only_exact_initial_file_and_error_releases_borrow);
	rmdir(directory); UT_DONE(); return ut_failed_count != 0;
}
