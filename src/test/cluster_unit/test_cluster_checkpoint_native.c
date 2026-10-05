/* Author: SqlRush <sqlrush@gmail.com> */
/* Actual native adapter; root/filesystem behavior is separately tested by
 * test_cluster_control_root. Only runtime/lock/root-return facts are replaced. */
#include "postgres.h"
#include <time.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xlogrecovery.h"
#include "catalog/pg_control.h"
#include "cluster/cluster_cf_authority.h"
#include "cluster/cluster_cf_enqueue.h"
#include "cluster/cluster_wal_thread.h"
#include "cluster/cluster_wal_source.h"
#include "cluster/cluster_wal_writer.h"
#include "cluster/cluster_write_fence.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_reconfig.h"
#include "cluster/cluster_startup_phase.h"
#include "cluster/cluster_external_fence.h"
#include "../../backend/cluster/cluster_control_root_private.h"
#include "miscadmin.h"
#include "postmaster/interrupt.h"
#include "postmaster/bgwriter.h"
#include "postmaster/startup.h"
#include "storage/latch.h"
#include "cluster/cluster_config_members.h"
#include "cluster/cluster_recovery_anchor.h"
#include "storage/fd.h"
#include "storage/lwlock.h"
#include "utils/wait_event.h"
#include "utils/timestamp.h"
#include "port/pg_crc32c.h"

#undef printf
#undef fprintf
#include "unit_test.h"
UT_DEFINE_GLOBALS();

bool cluster_shared_config = true, cluster_enabled = true;
bool enableFsync = true;
bool cluster_controlfile_shared_authority = true;
char *DataDir = "/unused-native-adapter";
AuxProcType MyAuxProcType = CheckpointerProcess;
volatile uint32 CritSectionCount;
volatile sig_atomic_t InterruptPending;
volatile sig_atomic_t ShutdownRequestPending;
volatile uint32 InterruptHoldoffCount, QueryCancelHoldoffCount;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
static sigjmp_buf error_boundary;
static ControlFileData current, selected, candidate;
static ControlFileData *ControlFile = &current;
static ClusterWalSourceRef ref;
static LWLockPadded locks[NUM_INDIVIDUAL_LWLOCKS];
LWLockPadded *MainLWLockArray = locks;
static Latch latch;
Latch *MyLatch = &latch;
static LOCKMODE cf_mode;
static bool local_lock, ref_ok, lock_ok, read_ok, release_ok, fence_ok, serving_ok;
static bool provider_ok, prebump, cancel_on_wait, change_epoch_on_wait, read_error;
static bool initialized_ok, clean_ok, lose_initialized_on_release;
static ClusterWalSourceRef initialized_ref;
static uint64 initialized_epoch;
static uint64 epoch;
static unsigned root_calls, waits, local_updates, reads, releases;
static unsigned native_writes, shutdown_calls;
static int native_error_level;
static ClusterControlRootResult returns[4];
static ClusterWalStartupImage clusterStartupWriter;
static bool clusterStartupWriterBound, clusterStartupWriterInstalled;
static bool clusterStartupWriterSelected;
static bool startup_sync_ok = true, startup_sync_change_epoch;
static unsigned startup_sync_calls;

ClusterFormationWitnessResult
cluster_authority_startup_refresh_recovery(int timeout_ms)
{
	UT_ASSERT_EQ(timeout_ms, 100);
	return CLUSTER_FORMATION_WITNESS_READY;
}
static unsigned startup_directory_calls;
static bool directory_before_selection;
BackendType MyBackendType = B_INVALID;
static bool startup_binding_ok, startup_view_ok;
static unsigned startup_calls, install_calls;
static ClusterControlRootResult install_returns[4];
bool InRecovery, ArchiveRecoveryRequested;
int wal_segment_size = 16 * 1024 * 1024;
static ClusterWalStartupImage offered;
static EndOfWalRecoveryInfo input;
static unsigned advance_calls, route_calls, bind_calls, writer_read_calls;
static bool restart_ok, route_ok, bind_ok, writer_read_ok, writer_read_changed;
static ClusterControlRootResult advance_returns[4];
static ClusterConfigMountResult mount_result;
static unsigned mount_waits;
int cluster_cssd_heartbeat_interval_ms = 1000, cluster_cssd_dead_deadband_factor = 5;
static unsigned self_seal_calls;
static ClusterControlRootResult self_seal_returns[4];

TimestampTz GetCurrentTimestamp(void) { return 10000000; }
bool TimestampDifferenceExceeds(TimestampTz start, TimestampTz end, int msec)
{
	return end - start >= (int64)msec * 1000;
}

/* The native adapter calls the real ROOT self-seal owner. Its quorum,
 * durable tail and refusal semantics have independent ROOT/QVOTEC tests. */
ClusterControlRootResult
cluster_control_root_v3_self_seal_v1(const ClusterWalSourceRef *restart, uint64 min_dead_us,
	ClusterControlRootSnapshot *out, ClusterControlRootReadToken *token)
{
	UT_ASSERT(cf_mode == NoLock && !local_lock && CritSectionCount == 0);
	UT_ASSERT_EQ(memcmp(restart, &ref, sizeof(ref)), 0);
	UT_ASSERT_EQ(min_dead_us, 5000000);
	UT_ASSERT(!clusterStartupWriterSelected && !clusterStartupWriterBound);
	UT_ASSERT_EQ(startup_directory_calls | advance_calls | route_calls | bind_calls, 0);
	UT_ASSERT(self_seal_calls < lengthof(self_seal_returns));
	memset(out, 0, sizeof(*out));
	memset(token, 0, sizeof(*token));
	return self_seal_returns[self_seal_calls++];
}
ClusterConfigMountResult
cluster_config_members_mount_status(void)
{
	if (mount_waits > 0) {
		--mount_waits;
		return CLUSTER_CONFIG_MOUNT_UNPROVEN;
	}
	return mount_result;
}

void *
palloc(Size n)
{
	void *p = malloc(n);
	if (p == NULL)
		abort();
	return p;
}
void
pfree(void *p)
{
	free(p);
}

void
ExceptionalCondition(const char *c, const char *f, int l)
{
	fprintf(stderr, "%s %s:%d\n", c, f, l);
	abort();
}
bool
errstart(int l, const char *d pg_attribute_unused())
{
	if (l >= ERROR)
		native_error_level = l;
	/* Native ARM CRC dispatch may emit DEBUG1 during its first invocation. */
	return l >= ERROR;
}
bool
errstart_cold(int l, const char *d)
{
	return errstart(l, d);
}
int
errmsg(const char *f pg_attribute_unused(), ...)
{
	return 0;
}
int
errdetail(const char *f pg_attribute_unused(), ...)
{
	return 0;
}
int
errhint(const char *f pg_attribute_unused(), ...)
{
	return 0;
}
int
errcode(int e pg_attribute_unused())
{
	return 0;
}
void
errfinish(const char *f pg_attribute_unused(), int l pg_attribute_unused(),
		  const char *fn pg_attribute_unused())
{
	if (PG_exception_stack)
		siglongjmp(*PG_exception_stack, 1);
	siglongjmp(error_boundary, 1);
}
void
pg_re_throw(void)
{
	siglongjmp(error_boundary, 1);
}
bool
LWLockHeldByMe(LWLock *l)
{
	UT_ASSERT(l == ControlFileLock);
	return local_lock;
}
bool
LWLockAcquire(LWLock *l, LWLockMode m)
{
	UT_ASSERT(l == ControlFileLock && m == LW_EXCLUSIVE && cf_mode == NoLock && !local_lock);
	local_lock = true;
	local_updates++;
	return true;
}
void
LWLockRelease(LWLock *l)
{
	UT_ASSERT(l == ControlFileLock && local_lock);
	local_lock = false;
}
bool
cluster_cf_held(LOCKMODE m)
{
	return cf_mode == m;
}
bool
cluster_cf_lock(LOCKMODE m)
{
	UT_ASSERT(m == ShareLock && cf_mode == NoLock && !local_lock && CritSectionCount == 0);
	if (lock_ok)
		cf_mode = m;
	return lock_ok;
}
bool
cluster_cf_held_is_clusterwide(LOCKMODE m)
{
	return cf_mode == m;
}
ClusterCfReleaseResult
cluster_cf_unlock_confirmed(LOCKMODE m)
{
	UT_ASSERT(cf_mode == m);
	cf_mode = NoLock;
	releases++;
	if (lose_initialized_on_release)
		initialized_ok = clean_ok = false;
	return release_ok ? CLUSTER_CF_RELEASE_CONFIRMED : CLUSTER_CF_RELEASE_UNCONFIRMED;
}
bool
cluster_cf_authority_read(ControlFileData *o)
{
	UT_ASSERT(cf_mode == ShareLock && !local_lock);
	reads++;
	if (read_error)
		ereport(ERROR, (errmsg("fixture read error")));
	*o = selected;
	return read_ok;
}
bool
cluster_wal_thread_current_v2_ref(ClusterWalSourceRef *o)
{
	*o = ref;
	return ref_ok;
}
bool
cluster_wal_thread_initialized_writer_matches(const ClusterWalSourceRef *expected,
											  uint64 observed_epoch)
{
	return initialized_ok && ref_ok && observed_epoch == initialized_epoch
		   && memcmp(expected, &initialized_ref, sizeof(*expected)) == 0;
}
bool
cluster_wal_thread_clean_writer_matches(const ClusterWalSourceRef *expected, uint64 observed_epoch)
{
	return clean_ok && ref_ok && observed_epoch == initialized_epoch
		   && memcmp(expected, &initialized_ref, sizeof(*expected)) == 0;
}
bool
cluster_write_fence_allowed(void)
{
	return fence_ok;
}
bool
cluster_serving_ready_is_current(void)
{
	return serving_ok;
}
bool
cluster_reconfig_has_pending_prebump_stage(void)
{
	return prebump;
}
bool
cluster_external_fence_runtime_active(void)
{
	return provider_ok;
}
uint64
cluster_epoch_get_current(void)
{
	return epoch;
}
void
ProcessInterrupts(void)
{
	siglongjmp(error_boundary, 1);
}
void
HandleStartupProcInterrupts(void)
{
	if (InterruptPending || ShutdownRequestPending)
		siglongjmp(error_boundary, 1);
}
void
ResetLatch(Latch *l)
{
	UT_ASSERT(l == MyLatch);
}
int
WaitLatch(Latch *l, int e, long t, uint32 event)
{
	UT_ASSERT(l == MyLatch && (e & WL_EXIT_ON_PM_DEATH) && t > 0);
	UT_ASSERT_EQ(event, MyBackendType == B_STARTUP ? WAIT_EVENT_CLUSTER_STARTUP_PHASE_3
												   : WAIT_EVENT_CHECKPOINTER_MAIN);
	UT_ASSERT_EQ(cf_mode, NoLock);
	UT_ASSERT(!local_lock && CritSectionCount == 0);
	UT_ASSERT_EQ(current.checkPoint, 100);
	waits++;
	if (change_epoch_on_wait)
		epoch++;
	if (cancel_on_wait)
		InterruptPending = true;
	return WL_TIMEOUT;
}
ClusterControlRootResult
cluster_control_root_v3_checkpoint_publish(const ClusterControlRootIdentity *self,
										   const ControlFileData *c, XLogRecPtr end,
										   ClusterControlRootSnapshot *out,
										   ClusterControlRootFileToken *token,
										   ControlFileData *control)
{
	UT_ASSERT(self->system_identifier == ref.claim.identity.system_identifier);
	UT_ASSERT(c == &candidate && end == 220);
	UT_ASSERT_EQ(cf_mode, NoLock);
	UT_ASSERT(!local_lock && CritSectionCount == 0);
	UT_ASSERT_EQ(current.checkPoint, 100);
	memset(out, 0, sizeof(*out));
	memset(token, 0, sizeof(*token));
	*control = *c;
	control->checkPointCopy.nextOid = 60001;
	UT_ASSERT(root_calls < lengthof(returns));
	return returns[root_calls++];
}

ClusterControlRootResult
cluster_control_root_v3_shutdown_checkpoint_publish(const ClusterControlRootIdentity *self,
													const ControlFileData *c, XLogRecPtr end,
													ClusterControlRootSnapshot *out,
													ClusterControlRootFileToken *token,
													ControlFileData *control)
{
	ClusterControlRootResult result;
	UT_ASSERT(ShutdownRequestPending && c->state == DB_SHUTDOWNED);
	shutdown_calls++;
	result = cluster_control_root_v3_checkpoint_publish(self, c, end, out, token, control);
	/* Real root keeps its OPEN lifecycle until the separate protocol close. */
	control->state = DB_IN_PRODUCTION;
	return result;
}

bool
cluster_wal_writer_startup_matches(const ClusterControlRootIdentity *self, const uint8 uuid[16],
									XLogRecPtr first)
{
	return startup_binding_ok && fence_ok && provider_ok && !prebump
		   && epoch == clusterStartupWriter.formation_epoch
		   && memcmp(self, &clusterStartupWriter.claim.identity, sizeof(*self)) == 0
		   && memcmp(uuid, clusterStartupWriter.operation_uuid, 16) == 0
		   && first == clusterStartupWriter.first_segment_lsn;
}

ClusterControlRootResult
cluster_wal_writer_begin(TimeLineID timeline, ClusterWalWriterToken *writer)
{
	memset(writer, 0, sizeof(*writer));
	writer->ref = ref;
	writer->epoch = epoch;
	return startup_binding_ok && fence_ok && timeline == clusterStartupWriter.timeline
		? CLUSTER_CONTROL_ROOT_OK_PRIMARY : CLUSTER_CONTROL_ROOT_STALE_TOKEN;
}

ClusterControlRootResult
cluster_wal_writer_check(const ClusterWalWriterToken *writer)
{
	return writer->epoch == epoch && startup_binding_ok && fence_ok
		? CLUSTER_CONTROL_ROOT_OK_PRIMARY : CLUSTER_CONTROL_ROOT_STALE_TOKEN;
}

bool RequestStartupSync(void)
{
	UT_ASSERT_EQ(cf_mode, NoLock);
	UT_ASSERT(!local_lock);
	startup_sync_calls++;
	if (startup_sync_change_epoch)
		epoch++;
	return startup_sync_ok;
}

ClusterControlRootResult
cluster_control_root_v3_startup_checkpoint(const ClusterControlRootIdentity *self,
										   const uint8 uuid[16], const ControlFileData *c,
										   XLogRecPtr end, ClusterWalStartupImage *out)
{
	UT_ASSERT_EQ(cf_mode, NoLock);
	UT_ASSERT(!local_lock && CritSectionCount == 0 && MyBackendType == B_STARTUP);
	UT_ASSERT(
		cluster_wal_writer_startup_matches(self, uuid, clusterStartupWriter.first_segment_lsn));
	UT_ASSERT(c == &candidate && c->state == DB_SHUTDOWNED && end == 220);
	UT_ASSERT_EQ(current.checkPoint, 100);
	*out = clusterStartupWriter;
	out->phase = CLUSTER_WAL_STARTUP_DURABLE;
	UT_ASSERT(startup_calls < lengthof(returns));
	return returns[startup_calls++];
}

ClusterControlRootResult
cluster_wal_thread_install_startup(const ClusterWalStartupImage *durable)
{
	UT_ASSERT_EQ(cf_mode, NoLock);
	UT_ASSERT(!local_lock && CritSectionCount == 0 && startup_calls > 0);
	UT_ASSERT_EQ(durable->phase, CLUSTER_WAL_STARTUP_DURABLE);
	UT_ASSERT_EQ(current.checkPoint, 100);
	UT_ASSERT(install_calls < lengthof(install_returns));
	return install_returns[install_calls++];
}

ClusterControlRootResult
cluster_control_root_v3_read_thread_locked(const ClusterControlRootIdentity *self,
										   ControlRootImage *root, ControlFileData *out,
										   ClusterControlRootFileToken *token)
{
	UT_ASSERT(install_calls > 0 && cf_mode == ShareLock && !local_lock);
	UT_ASSERT(memcmp(self, &clusterStartupWriter.claim.identity, sizeof(*self)) == 0);
	memset(root, 0, sizeof(*root));
	memset(token, 0, sizeof(*token));
	root->present[0] = true;
	root->records[0].identity = *self;
	root->records[0].lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN;
	root->header.activation_state = CLUSTER_CONTROL_ROOT_ACTIVATION_ACTIVE;
	root->header.v2.database_state = CLUSTER_CONTROL_ROOT_DATABASE_MOUNTED;
	*out = candidate;
	out->state = DB_IN_PRODUCTION;
	out->checkPointCopy.nextOid = 60002;
	if (!startup_view_ok)
		out->checkPoint++;
	reads++;
	if (read_error)
		ereport(ERROR, (errmsg("startup fixture read error")));
	return read_ok ? CLUSTER_CONTROL_ROOT_OK_PRIMARY : CLUSTER_CONTROL_ROOT_IO_ERROR;
}

bool
cluster_wal_thread_restart_v2_ref(ClusterWalSourceRef *out)
{
	*out = ref;
	return restart_ok;
}

ClusterControlRootResult
cluster_control_root_v3_startup_advance_clean(const ClusterWalSourceRef *restart,
											  ClusterWalStartupImage *out)
{
	UT_ASSERT(cf_mode == NoLock && !local_lock && CritSectionCount == 0);
	UT_ASSERT(memcmp(restart, &ref, sizeof(ref)) == 0);
	UT_ASSERT_EQ(current.checkPoint, 100);
	UT_ASSERT(advance_calls < lengthof(advance_returns));
	*out = offered;
	return advance_returns[advance_calls++];
}

ClusterControlRootResult
cluster_control_root_v3_startup_read_writer(const ClusterControlRootIdentity *self,
											const uint8 uuid[16], ClusterWalStartupImage *out)
{
	UT_ASSERT(cf_mode == NoLock && !local_lock && CritSectionCount == 0);
	UT_ASSERT(clusterStartupWriterSelected && !clusterStartupWriterBound);
	UT_ASSERT(memcmp(self, &clusterStartupWriter.claim.identity, sizeof(*self)) == 0);
	UT_ASSERT(memcmp(uuid, clusterStartupWriter.operation_uuid, 16) == 0);
	writer_read_calls++;
	*out = offered;
	if (writer_read_changed)
		out->formation_epoch++;
	return writer_read_ok ? CLUSTER_CONTROL_ROOT_OK_PRIMARY : CLUSTER_CONTROL_ROOT_STALE_TOKEN;
}

ClusterControlRootResult
cluster_control_root_v3_startup_route_writer(const ClusterControlRootIdentity *self,
											 const uint8 uuid[16], ClusterWalStartupImage *out)
{
	UT_ASSERT(cf_mode == NoLock && !local_lock && CritSectionCount == 0);
	UT_ASSERT(!clusterStartupWriterBound && bind_calls == 0 && advance_calls > 0);
	UT_ASSERT(memcmp(self, &offered.claim.identity, sizeof(*self)) == 0);
	UT_ASSERT(memcmp(uuid, offered.operation_uuid, 16) == 0);
	route_calls++;
	*out = offered;
	return route_ok ? CLUSTER_CONTROL_ROOT_OK_PRIMARY : CLUSTER_CONTROL_ROOT_IO_ERROR;
}

ClusterControlRootResult
cluster_wal_writer_startup_prepare(const ClusterControlRootIdentity *self, const uint8 uuid[16],
									XLogRecPtr *first)
{
	UT_ASSERT(cf_mode == NoLock && !local_lock && CritSectionCount == 0);
	UT_ASSERT(!clusterStartupWriterBound && route_calls == 1);
	UT_ASSERT(memcmp(self, &offered.claim.identity, sizeof(*self)) == 0);
	UT_ASSERT(memcmp(uuid, offered.operation_uuid, 16) == 0);
	bind_calls++;
	*first = offered.first_segment_lsn;
	return bind_ok ? CLUSTER_CONTROL_ROOT_OK_PRIMARY : CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
}

#include "test_cluster_startup_writer_native.inc"

#include "test_cluster_checkpoint_native.inc"

static void
UpdateControlFile(void)
{
	native_writes++;
	/* Preserve the actual chokepoint: shared_config cannot use this writer. */
	if (cluster_shared_config)
		ereport(PANIC, (errmsg("untyped native control write")));
}

static bool
native_shutdown_begin(bool shutdown)
{
	if (sigsetjmp(error_boundary, 1))
		return false;
#include "test_cluster_checkpoint_shutdown_begin.inc"
	END_CRIT_SECTION();
	return true;
}
/* Only the native lock boundary is substituted. Actual candidate assignments,
 * CRC and old-writer dispatch below come from CreateCheckPoint. */
#undef SpinLockAcquire
#undef SpinLockRelease
#define SpinLockAcquire(lock) ((void)(lock))
#define SpinLockRelease(lock) ((void)(lock))
static ControlFileData
native_candidate(bool shutdown)
{
	ControlFileData *checkpoint_control = ControlFile, v3_checkpoint = { 0 };
	CheckPoint checkPoint = candidate.checkPointCopy;
	XLogRecPtr ProcLastRecPtr = 200;
	struct {
		int ulsn_lck;
		XLogRecPtr unloggedLSN;
	} xlog_state = { 0, 444 };
	typeof(xlog_state) *XLogCtl = &xlog_state;
#include "test_cluster_checkpoint_candidate.inc"
	return *checkpoint_control;
}
#undef SpinLockAcquire
#undef SpinLockRelease

static void
reset_fixture(void)
{
	memset(&current, 0, sizeof(current));
	current.checkPoint = 100;
	current.system_identifier = 1234;
	current.state = DB_IN_PRODUCTION;
	selected = current;
	selected.checkPoint = 150;
	candidate = current;
	candidate.checkPoint = 200;
	memset(&ref, 0, sizeof(ref));
	ref.claim.identity.system_identifier = 1234;
	ref.timeline = 1;
	ref.claim.identity.origin_owner_incarnation = 99;
	candidate.checkPointCopy.ThisTimeLineID = selected.checkPointCopy.ThisTimeLineID = 1;
	ref_ok = lock_ok = read_ok = release_ok = fence_ok = serving_ok = provider_ok = true;
	prebump = local_lock = cancel_on_wait = change_epoch_on_wait = read_error = false;
	cf_mode = NoLock;
	epoch = 9;
	initialized_ok = clean_ok = lose_initialized_on_release = false;
	initialized_ref = ref;
	initialized_epoch = epoch;
	CritSectionCount = InterruptHoldoffCount = QueryCancelHoldoffCount = 0;
	InterruptPending = false;
	ShutdownRequestPending = false;
	PG_exception_stack = NULL;
	error_context_stack = NULL;
	MyAuxProcType = CheckpointerProcess;
	MyBackendType = B_CHECKPOINTER;
	root_calls = waits = local_updates = reads = releases = 0;
	native_writes = shutdown_calls = 0;
	native_error_level = 0;
	cluster_shared_config = cluster_enabled = cluster_controlfile_shared_authority = true;
	memset(returns, 0, sizeof(returns));
	memset(install_returns, 0, sizeof(install_returns));
	memset(&clusterStartupWriter, 0, sizeof(clusterStartupWriter));
	clusterStartupWriterBound = clusterStartupWriterInstalled = false;
	clusterStartupWriterSelected = false;
	startup_directory_calls = 0;
	directory_before_selection = false;
	startup_binding_ok = startup_view_ok = true;
	startup_calls = install_calls = 0;
	memset(&offered, 0, sizeof(offered));
	memset(&input, 0, sizeof(input));
	memset(advance_returns, 0, sizeof(advance_returns));
	advance_calls = route_calls = bind_calls = writer_read_calls = 0;
	restart_ok = route_ok = bind_ok = writer_read_ok = true;
	writer_read_changed = false;
	mount_result = CLUSTER_CONFIG_MOUNT_MATCH;
	mount_waits = 0;
	self_seal_calls = 0;
	for (unsigned i = 0; i < lengthof(self_seal_returns); i++)
		self_seal_returns[i] = CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID;
	InRecovery = ArchiveRecoveryRequested = false;
}
static bool
prepare(int flags)
{
	if (sigsetjmp(error_boundary, 1))
		return false;
	ClusterCheckpointV3Prepare(flags, &candidate);
	return true;
}
static bool
publish(void)
{
	if (sigsetjmp(error_boundary, 1))
		return false;
	ClusterCheckpointV3Publish(&candidate, 220);
	return true;
}

UT_TEST(prepare_uses_short_owned_read)
{
	reset_fixture();
	UT_ASSERT(prepare(CHECKPOINT_FORCE));
	UT_ASSERT_EQ(candidate.checkPoint, 150);
	UT_ASSERT_EQ(current.checkPoint, 100);
	UT_ASSERT_EQ(reads, 1);
	UT_ASSERT_EQ(releases, 1);
	UT_ASSERT_EQ(cf_mode, NoLock);
}
UT_TEST(prepare_refuses_unsupported_or_unproven_input)
{
	for (int f = 0; f < 9; ++f) {
		reset_fixture();
		if (f == 0)
			MyAuxProcType = StartupProcess;
		if (f == 1)
			ref_ok = false;
		if (f == 2)
			provider_ok = false;
		if (f == 3)
			lock_ok = false;
		if (f == 4)
			read_ok = false;
		if (f == 5)
			release_ok = false;
		if (f == 6)
			read_error = true;
		UT_ASSERT(!prepare(f == 7	? CHECKPOINT_IS_SHUTDOWN
						   : f == 8 ? CHECKPOINT_END_OF_RECOVERY
									: 0));
		UT_ASSERT_EQ(current.checkPoint, 100);
		UT_ASSERT_EQ(cf_mode, NoLock);
	}
}
UT_TEST(clean_writer_checkpoint_uses_exact_existing_fence_qualification)
{
	for (unsigned shutdown = 0; shutdown < 2; shutdown++) {
		reset_fixture();
		provider_ok = false;
		clean_ok = true;
		ShutdownRequestPending = shutdown != 0;
		UT_ASSERT(prepare(shutdown ? CHECKPOINT_IS_SHUTDOWN : CHECKPOINT_FORCE));
		UT_ASSERT_EQ(reads, 1);
		UT_ASSERT_EQ(cf_mode, NoLock);
		candidate.checkPoint = 200;
		candidate.state = shutdown ? DB_SHUTDOWNED : DB_IN_PRODUCTION;
		UT_ASSERT(publish());
		UT_ASSERT_EQ(root_calls, 1);
		UT_ASSERT_EQ(shutdown_calls, shutdown);
		UT_ASSERT_EQ(current.checkPoint, 200);
	}
}
UT_TEST(clean_checkpoint_cannot_borrow_other_input_epoch_or_writer)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		reset_fixture();
		provider_ok = false;
		clean_ok = fault != 0;
		if (fault == 1)
			initialized_epoch++;
		if (fault == 2)
			initialized_ref.claim.identity.origin_owner_incarnation++;
		if (fault == 3)
			lose_initialized_on_release = true;
		UT_ASSERT(!prepare(CHECKPOINT_FORCE));
		UT_ASSERT_EQ(reads, fault == 3 ? 1 : 0);
		UT_ASSERT_EQ(cf_mode, NoLock);
		UT_ASSERT(!publish());
		UT_ASSERT_EQ(root_calls, 0);
		UT_ASSERT_EQ(local_updates, 0);
		UT_ASSERT_EQ(current.checkPoint, 100);
	}
}
UT_TEST(initialized_writer_checkpoint_uses_exact_existing_fence_qualification)
{
	for (unsigned shutdown = 0; shutdown < 2; shutdown++) {
		reset_fixture();
		provider_ok = false;
		initialized_ok = true;
		ShutdownRequestPending = shutdown != 0;
		UT_ASSERT(prepare(shutdown ? CHECKPOINT_IS_SHUTDOWN : CHECKPOINT_FORCE));
		UT_ASSERT_EQ(reads, 1);
		UT_ASSERT_EQ(cf_mode, NoLock);
		candidate.checkPoint = 200;
		candidate.state = shutdown ? DB_SHUTDOWNED : DB_IN_PRODUCTION;
		UT_ASSERT(publish());
		UT_ASSERT_EQ(root_calls, 1);
		UT_ASSERT_EQ(shutdown_calls, shutdown);
		UT_ASSERT_EQ(current.checkPoint, 200);
	}
}
UT_TEST(initialized_checkpoint_cannot_borrow_other_input_epoch_or_writer)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		reset_fixture();
		provider_ok = false;
		initialized_ok = fault != 0;
		if (fault == 1)
			initialized_epoch++;
		if (fault == 2)
			initialized_ref.claim.identity.origin_owner_incarnation++;
		if (fault == 3)
			lose_initialized_on_release = true;
		UT_ASSERT(!prepare(CHECKPOINT_FORCE));
		UT_ASSERT_EQ(reads, fault == 3 ? 1 : 0);
		UT_ASSERT_EQ(cf_mode, NoLock);
		UT_ASSERT(!publish());
		UT_ASSERT_EQ(root_calls, 0);
		UT_ASSERT_EQ(local_updates, 0);
		UT_ASSERT_EQ(current.checkPoint, 100);
	}
}
UT_TEST(publish_retries_only_root_competition_before_projection)
{
	reset_fixture();
	returns[0] = CLUSTER_CONTROL_ROOT_CAS_CONFLICT;
	returns[1] = CLUSTER_CONTROL_ROOT_CAS_CONFLICT;
	UT_ASSERT(publish());
	UT_ASSERT_EQ(root_calls, 3);
	UT_ASSERT_EQ(waits, 2);
	UT_ASSERT_EQ(current.checkPoint, 200);
	UT_ASSERT_EQ(local_updates, 1);
}
UT_TEST(publish_does_not_retry_safety_or_io_refusal)
{
	ClusterControlRootResult failures[]
		= { CLUSTER_CONTROL_ROOT_IO_ERROR, CLUSTER_CONTROL_ROOT_STALE_TOKEN,
			CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE, CLUSTER_CONTROL_ROOT_RELEASE_UNCERTAIN,
			CLUSTER_CONTROL_ROOT_POSTREAD_FAILED };
	for (unsigned i = 0; i < lengthof(failures); i++) {
		reset_fixture();
		returns[0] = failures[i];
		UT_ASSERT(!publish());
		UT_ASSERT_EQ(root_calls, 1);
		UT_ASSERT_EQ(waits, 0);
		UT_ASSERT_EQ(current.checkPoint, 100);
		UT_ASSERT_EQ(local_updates, 0);
	}
}
UT_TEST(publish_cancel_and_changed_authority_stop_owned_retry)
{
	for (int f = 0; f < 2; f++) {
		reset_fixture();
		returns[0] = CLUSTER_CONTROL_ROOT_CAS_CONFLICT;
		cancel_on_wait = f == 0;
		change_epoch_on_wait = f == 1;
		UT_ASSERT(!publish());
		UT_ASSERT_EQ(root_calls, 1);
		UT_ASSERT_EQ(waits, 1);
		UT_ASSERT_EQ(current.checkPoint, 100);
		UT_ASSERT_EQ(cf_mode, NoLock);
	}
}
UT_TEST(publish_installs_root_selected_common_fields)
{
	reset_fixture();
	candidate.checkPointCopy.nextOid = 88;
	UT_ASSERT(publish());
	UT_ASSERT_EQ(current.checkPoint, 200);
	UT_ASSERT_EQ(current.checkPointCopy.nextOid, 60001);
}
UT_TEST(native_candidate_is_private_until_publication)
{
	ControlFileData built;
	pg_crc32c crc;
	reset_fixture();
	built = native_candidate(false);
	UT_ASSERT_EQ(current.checkPoint, 100);
	UT_ASSERT_EQ(native_writes, 0);
	UT_ASSERT_EQ(built.checkPoint, 200);
	UT_ASSERT_EQ(built.unloggedLSN, 444);
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, &built, offsetof(ControlFileData, crc));
	FIN_CRC32C(crc);
	UT_ASSERT_EQ(crc, built.crc);
	UT_ASSERT(!local_lock);
}
UT_TEST(native_legacy_candidate_keeps_control_write)
{
	for (int shutdown = 0; shutdown < 2; shutdown++) {
		ControlFileData built;
		reset_fixture();
		cluster_shared_config = false;
		built = native_candidate(shutdown);
		UT_ASSERT_EQ(current.checkPoint, 200);
		UT_ASSERT_EQ(native_writes, 1);
		UT_ASSERT_EQ(built.state, shutdown ? DB_SHUTDOWNED : DB_IN_PRODUCTION);
		UT_ASSERT(!local_lock);
	}
}
UT_TEST(shutdown_prepare_requires_native_request_not_eor)
{
	reset_fixture();
	ShutdownRequestPending = true;
	UT_ASSERT(prepare(CHECKPOINT_IS_SHUTDOWN | CHECKPOINT_IMMEDIATE));
	UT_ASSERT_EQ(candidate.checkPoint, 150);
	UT_ASSERT_EQ(current.checkPoint, 100);
	UT_ASSERT_EQ(cf_mode, NoLock);
	reset_fixture();
	ShutdownRequestPending = true;
	UT_ASSERT(!prepare(CHECKPOINT_IS_SHUTDOWN | CHECKPOINT_END_OF_RECOVERY));
	UT_ASSERT_EQ(reads, 0);
}
UT_TEST(shutdown_dispatch_preserves_open_view_and_retries)
{
	reset_fixture();
	ShutdownRequestPending = true;
	UT_ASSERT(native_shutdown_begin(true));
	if (ut_current_failed)
		return;
	candidate = native_candidate(true);
	UT_ASSERT_EQ(candidate.state, DB_SHUTDOWNED);
	UT_ASSERT_EQ(current.state, DB_IN_PRODUCTION);
	UT_ASSERT_EQ(native_writes, 0);
	local_updates = 0;
	returns[0] = CLUSTER_CONTROL_ROOT_CAS_CONFLICT;
	UT_ASSERT(publish());
	UT_ASSERT_EQ(shutdown_calls, 2);
	UT_ASSERT_EQ(waits, 1);
	UT_ASSERT_EQ(local_updates, 1);
	UT_ASSERT_EQ(current.state, DB_IN_PRODUCTION);
	UT_ASSERT_EQ(current.checkPoint, 200);
	UT_ASSERT_EQ(candidate.state, DB_SHUTDOWNED);
}
UT_TEST(shutdown_missing_owner_or_publication_never_installs_candidate)
{
	for (int f = 0; f < 4; f++) {
		reset_fixture();
		candidate.state = f == 3 ? DB_SHUTDOWNING : DB_SHUTDOWNED;
		ShutdownRequestPending = f != 0;
		if (f == 1)
			MyAuxProcType = StartupProcess;
		if (f == 2)
			returns[0] = CLUSTER_CONTROL_ROOT_IO_ERROR;
		UT_ASSERT(!publish());
		UT_ASSERT_EQ(root_calls, f == 2 ? 1 : 0);
		UT_ASSERT_EQ(shutdown_calls, f == 2 ? 1 : 0);
		UT_ASSERT_EQ(current.checkPoint, 100);
		UT_ASSERT_EQ(current.state, DB_IN_PRODUCTION);
		UT_ASSERT_EQ(local_updates, 0);
	}
}
UT_TEST(online_checkpoint_during_shutdown_signal_stays_online)
{
	reset_fixture();
	ShutdownRequestPending = true;
	UT_ASSERT(publish());
	UT_ASSERT_EQ(shutdown_calls, 0);
	UT_ASSERT_EQ(root_calls, 1);
	UT_ASSERT_EQ(current.state, DB_IN_PRODUCTION);
}
UT_TEST(early_shutdown_never_calls_untyped_writer_for_root_v3)
{
	for (int f = 0; f < 3; f++) {
		reset_fixture();
		cluster_shared_config = f != 2;
		UT_ASSERT(native_shutdown_begin(f != 0));
		UT_ASSERT_EQ(native_writes, f == 2 ? 1 : 0);
		UT_ASSERT_EQ(current.state, f == 2 ? DB_SHUTDOWNING : DB_IN_PRODUCTION);
		UT_ASSERT_EQ(CritSectionCount, 0);
		UT_ASSERT(!local_lock);
	}
}

static void
startup_fixture(void)
{
	reset_fixture();
	MyBackendType = B_STARTUP;
	MyAuxProcType = StartupProcess;
	serving_ok = ref_ok = false;
	current.state = candidate.state = DB_SHUTDOWNED;
	current.checkPointCopy.ThisTimeLineID = 1;
	clusterStartupWriterBound = true;
	clusterStartupWriter.phase = CLUSTER_WAL_STARTUP_INITIALIZING;
	clusterStartupWriter.claim.identity = ref.claim.identity;
	clusterStartupWriter.claim.identity.origin_node_id = 0;
	clusterStartupWriter.claim.identity.origin_thread_id = 1;
	clusterStartupWriter.operation_uuid[0] = 5;
	clusterStartupWriter.formation_epoch = epoch;
	clusterStartupWriter.timeline = 1;
	clusterStartupWriter.first_segment_lsn = 128;
	clusterStartupWriter.predecessor.snapshot.checkpoint_lower_lsn = 100;
	clusterStartupWriter.predecessor.snapshot.tail_last_record_lsn = 100;
}

static bool
startup_prepare(int flags)
{
	if (sigsetjmp(error_boundary, 1))
		return false;
	ClusterCheckpointV3Prepare(flags, &candidate);
	return true;
}

static bool
startup_publish(void)
{
	if (sigsetjmp(error_boundary, 1))
		return false;
	ClusterCheckpointV3Publish(&candidate, 220);
	return true;
}

UT_TEST(startup_prepare_uses_only_bound_initializer_without_serving)
{
	startup_fixture();
	UT_ASSERT(startup_prepare(CHECKPOINT_END_OF_RECOVERY | CHECKPOINT_IMMEDIATE));
	UT_ASSERT_EQ(candidate.checkPoint, 100);
	UT_ASSERT_EQ(candidate.state, DB_SHUTDOWNED);
	UT_ASSERT_EQ(reads | local_updates | native_writes, 0);
}

UT_TEST(startup_file_sync_rechecks_original_writer_and_keeps_native_owner)
{
	for (unsigned fault = 0; fault < 8; fault++) {
		startup_fixture();
		startup_sync_calls = 0;
		startup_sync_ok = true;
		startup_sync_change_epoch = false;
		switch (fault) {
		case 1: clusterStartupWriterBound = false; break;
		case 2: clusterStartupWriterInstalled = true; break;
		case 3: startup_binding_ok = false; break;
		case 4: startup_sync_ok = false; break;
		case 5: startup_sync_change_epoch = true; break;
		case 6: cf_mode = ExclusiveLock; break;
		case 7: local_lock = true; break;
		}
		UT_ASSERT_EQ(ClusterStartupFileSync(), fault == 0);
		UT_ASSERT_EQ(startup_sync_calls, fault == 0 || fault == 4 || fault == 5 ? 1 : 0);
		UT_ASSERT_EQ(root_calls | local_updates | native_writes, 0);
	}
	startup_sync_change_epoch = false;
}

UT_TEST(startup_prepare_rejects_other_purposes_and_lost_owner)
{
	for (unsigned f = 0; f < 10; ++f) {
		int flags = CHECKPOINT_END_OF_RECOVERY;
		startup_fixture();
		if (f == 0)
			flags = CHECKPOINT_FORCE;
		if (f == 1)
			flags |= CHECKPOINT_IS_SHUTDOWN;
		if (f == 2)
			MyBackendType = B_CHECKPOINTER;
		if (f == 3)
			clusterStartupWriterBound = false;
		if (f == 4)
			clusterStartupWriterInstalled = true;
		if (f == 5)
			startup_binding_ok = false;
		if (f == 6)
			ShutdownRequestPending = true;
		if (f == 7)
			current.checkPoint++;
		if (f == 8)
			provider_ok = false;
		if (f == 9)
			current.backupStartPoint = 1;
		UT_ASSERT(!startup_prepare(flags));
		UT_ASSERT_EQ(local_updates | native_writes, 0);
	}
}

UT_TEST(startup_publish_installs_only_actual_root_projection)
{
	startup_fixture();
	UT_ASSERT(startup_publish());
	UT_ASSERT_EQ(startup_calls, 1);
	UT_ASSERT_EQ(install_calls, 1);
	UT_ASSERT_EQ(current.checkPoint, 200);
	UT_ASSERT_EQ(current.state, DB_IN_PRODUCTION);
	UT_ASSERT_EQ(current.checkPointCopy.nextOid, 60002);
	UT_ASSERT(clusterStartupWriterInstalled);
	UT_ASSERT_EQ(reads, 1);
	UT_ASSERT_EQ(releases, 1);
	UT_ASSERT_EQ(local_updates, 1);
	UT_ASSERT_EQ(native_writes, 0);
	UT_ASSERT_EQ(cf_mode, NoLock);
}

UT_TEST(startup_publish_reobserves_cas_without_holding_locks)
{
	startup_fixture();
	returns[0] = install_returns[0] = CLUSTER_CONTROL_ROOT_CAS_CONFLICT;
	UT_ASSERT(startup_publish());
	UT_ASSERT_EQ(startup_calls, 2);
	UT_ASSERT_EQ(install_calls, 2);
	UT_ASSERT_EQ(waits, 2);
	UT_ASSERT_EQ(local_updates, 1);
}

UT_TEST(startup_publish_refusal_never_installs_in_memory_candidate)
{
	for (unsigned f = 0; f < 9; ++f) {
		startup_fixture();
		if (f == 0)
			returns[0] = CLUSTER_CONTROL_ROOT_IO_ERROR;
		if (f == 1)
			install_returns[0] = CLUSTER_CONTROL_ROOT_RELEASE_UNCERTAIN;
		if (f == 2)
			lock_ok = false;
		if (f == 3)
			release_ok = false;
		if (f == 4)
			read_ok = false;
		if (f == 5)
			startup_view_ok = false;
		if (f == 6)
			read_error = true;
		if (f == 7)
			provider_ok = false;
		if (f == 8) {
			returns[0] = CLUSTER_CONTROL_ROOT_CAS_CONFLICT;
			cancel_on_wait = true;
		}
		UT_ASSERT(!startup_publish());
		UT_ASSERT_EQ(current.checkPoint, 100);
		UT_ASSERT_EQ(current.state, DB_SHUTDOWNED);
		UT_ASSERT(!clusterStartupWriterInstalled);
		UT_ASSERT_EQ(local_updates | native_writes, 0);
		UT_ASSERT_EQ(cf_mode, NoLock);
	}
}

static void
writer_begin_fixture(void)
{
	startup_fixture();
	offered = clusterStartupWriter;
	offered.input_kind = CLUSTER_WAL_STARTUP_CLEAN;
	offered.segment_size = wal_segment_size;
	offered.first_segment_lsn = wal_segment_size;
	offered.input_record_start = 100;
	offered.input_record_end = offered.sealed_input_end = 220;
	offered.input_timeline = 1;
	input.lastRec = 100;
	input.endOfLog = 220;
	input.lastRecTLI = input.endOfLogTLI = 1;
	memset(&clusterStartupWriter, 0, sizeof(clusterStartupWriter));
	clusterStartupWriterBound = false;
}

/* Only the filesystem operation is replaced. The selected boundary and its
 * order relative to native startup execute the actual StartupXLOG body.
 * Author: SqlRush <sqlrush@gmail.com> */
static void
ValidateXLOGDirectoryStructure(void)
{
	startup_directory_calls++;
	directory_before_selection |= cluster_shared_config && !clusterStartupWriterSelected;
}

static bool
startup_first_native_site(void)
{
	if (sigsetjmp(error_boundary, 1))
		return false;
#include "test_cluster_startup_early_native.inc"
	return true;
}

static void
clean_input_fixture(void)
{
	writer_begin_fixture();
	offered.generation = 23;
	offered.predecessor_file_sequence = 19;
	offered.predecessor_file_sha256[0] = 71;
	offered.predecessor_evidence_sha256[0] = 83;
	offered.predecessor.snapshot.identity = ref.claim.identity;
	offered.predecessor.snapshot.identity.origin_owner_incarnation = 41;
	offered.predecessor.refs.claim_sha256[0] = 37;
	offered.config_generation = offered.claim.config_generation = 6;
	offered.claim.claim_generation = 22;
	offered.claim.database_incarnation = 12;
	offered.input_record_crc = 167;
	UT_ASSERT(startup_first_native_site());
}

UT_TEST(clean_input_observation_preserves_exact_old_and_new_owners)
{
	ClusterWalStartupCleanInputV1 got, expected = { 0 }, again;
	clean_input_fixture();
	provider_ok = false;
	expected.predecessor = offered.predecessor.snapshot.identity;
	memcpy(expected.predecessor_claim_sha256, offered.predecessor.refs.claim_sha256, 32);
	expected.successor = offered.claim;
	expected.formation_epoch = offered.formation_epoch;
	expected.config_generation = offered.config_generation;
	expected.predecessor_root_sequence = offered.predecessor_file_sequence;
	memcpy(expected.predecessor_root_sha256, offered.predecessor_file_sha256, 32);
	memcpy(expected.exit_evidence_sha256, offered.predecessor_evidence_sha256, 32);
	memcpy(expected.operation_uuid, offered.operation_uuid, 16);
	expected.operation_generation = offered.generation;
	expected.checkpoint_lsn = offered.input_record_start;
	expected.checkpoint_end = offered.input_record_end;
	expected.checkpoint_crc32c = offered.input_record_crc;
	expected.timeline = offered.timeline;
	UT_ASSERT_EQ(cluster_wal_startup_clean_input_v1(&got), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(memcmp(&got, &expected, sizeof(got)), 0);
	UT_ASSERT_EQ(cluster_wal_startup_clean_input_v1(&again), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(memcmp(&again, &got, sizeof(got)), 0);
	UT_ASSERT_EQ(writer_read_calls, 2);
	UT_ASSERT_EQ(route_calls | bind_calls | native_writes | install_calls, 0);
	UT_ASSERT(!clusterStartupWriterBound && !clusterStartupWriterInstalled);
	UT_ASSERT_EQ(cf_mode, NoLock);
}

UT_TEST(clean_input_observation_rejects_other_input_kinds)
{
	const uint32 kinds[]
		= { 0, CLUSTER_WAL_STARTUP_INITIALIZED, CLUSTER_WAL_STARTUP_RECOVERED, 3, UINT32_MAX };
	ClusterWalStartupCleanInputV1 got, zero = { 0 };
	for (unsigned i = 0; i < lengthof(kinds); ++i) {
		clean_input_fixture();
		clusterStartupWriter.input_kind = offered.input_kind = kinds[i];
		memset(&got, 0x55, sizeof(got));
		UT_ASSERT(cluster_wal_startup_clean_input_v1(&got) != CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		UT_ASSERT_EQ(memcmp(&got, &zero, sizeof(got)), 0);
		UT_ASSERT_EQ(writer_read_calls | route_calls | bind_calls | native_writes, 0);
	}
}

UT_TEST(clean_input_observation_rejects_wrong_owner_phase_and_lock_context)
{
	ClusterWalStartupCleanInputV1 got, zero = { 0 };
	for (unsigned fault = 0; fault < 16; ++fault) {
		clean_input_fixture();
		switch (fault) {
		case 0:
			clusterStartupWriterSelected = false;
			break;
		case 1:
			clusterStartupWriterBound = true;
			break;
		case 2:
			clusterStartupWriterInstalled = true;
			break;
		case 3:
			clusterStartupWriter.phase = CLUSTER_WAL_STARTUP_RESERVED;
			break;
		case 4:
			clusterStartupWriter.phase = CLUSTER_WAL_STARTUP_DURABLE;
			break;
		case 5:
			MyBackendType = B_CHECKPOINTER;
			break;
		case 6:
			cluster_shared_config = false;
			break;
		case 7:
			cluster_enabled = false;
			break;
		case 8:
			cluster_controlfile_shared_authority = false;
			break;
		case 9:
			CritSectionCount = 1;
			break;
		case 10:
			ShutdownRequestPending = true;
			break;
		case 11:
			InRecovery = true;
			break;
		case 12:
			ArchiveRecoveryRequested = true;
			break;
		case 13:
			current.state = DB_IN_PRODUCTION;
			break;
		case 14:
			local_lock = true;
			break;
		case 15:
			cf_mode = ShareLock;
			break;
		}
		memset(&got, 0x55, sizeof(got));
		UT_ASSERT(cluster_wal_startup_clean_input_v1(&got) != CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		UT_ASSERT_EQ(memcmp(&got, &zero, sizeof(got)), 0);
		UT_ASSERT_EQ(writer_read_calls | route_calls | bind_calls | native_writes, 0);
	}
	clean_input_fixture();
	UT_ASSERT_EQ(cluster_wal_startup_clean_input_v1(NULL), CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(writer_read_calls, 0);
}

UT_TEST(clean_input_observation_rechecks_root_and_native_checkpoint)
{
	ClusterWalStartupCleanInputV1 got, zero = { 0 };
	for (unsigned fault = 0; fault < 9; ++fault) {
		clean_input_fixture();
		switch (fault) {
		case 0:
			writer_read_ok = false;
			break;
		case 1:
			writer_read_changed = true;
			break;
		case 2:
			offered.claim.identity.origin_owner_incarnation++;
			break;
		case 3:
			offered.predecessor_evidence_sha256[0]++;
			break;
		case 4:
			offered.claim.claim_generation++;
			break;
		case 5:
			current.checkPoint++;
			break;
		case 6:
			current.checkPointCopy.ThisTimeLineID++;
			break;
		case 7:
			offered.predecessor_file_sha256[0]++;
			break;
		case 8:
			offered.operation_uuid[1]++;
			break;
		}
		memset(&got, 0x55, sizeof(got));
		UT_ASSERT(cluster_wal_startup_clean_input_v1(&got) != CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		UT_ASSERT_EQ(memcmp(&got, &zero, sizeof(got)), 0);
		UT_ASSERT_EQ(route_calls | bind_calls | native_writes | install_calls, 0);
		UT_ASSERT_EQ(cf_mode, NoLock);
	}
}

static unsigned input_checks, legacy_probes, legacy_windows, legacy_anchors;
static bool input_qualified;
static ClusterRecoveryAnchor legacy_anchor;
int cluster_node_id;
void
cluster_control_bootstrap_native_inputs_require(const char *pgdata pg_attribute_unused())
{
	input_checks++;
	if (!input_qualified)
		siglongjmp(error_boundary, 1);
}
static void
cluster_cf_phase2_verify_or_fail(const char *pgdata pg_attribute_unused())
{
	legacy_probes++;
}
static void
cluster_cf_enter_bootstrap_window_or_fail(void)
{
	legacy_windows++;
}
bool
cluster_recovery_anchor_load(uint64 sysid pg_attribute_unused(), bool *bak)
{
	legacy_anchors++;
	*bak = false;
	return true;
}
const ClusterRecoveryAnchor *
cluster_recovery_anchor_get(void)
{
	legacy_anchor.state = DB_SHUTDOWNED;
	return &legacy_anchor;
}
#define CLUSTER_INJECTION_POINT(name) ((void)0)
#define cluster_injection_should_skip(name) false
static bool
startup_control_site(void)
{
	if (sigsetjmp(error_boundary, 1))
		return false;
#include "test_cluster_startup_control_native.inc"
	return true;
}

UT_TEST(shared_startup_does_not_adopt_legacy_control_authority)
{
	cluster_shared_config = cluster_enabled = cluster_controlfile_shared_authority = true;
	input_qualified = true;
	input_checks = legacy_probes = legacy_windows = legacy_anchors = 0;
	UT_ASSERT(startup_control_site());
	UT_ASSERT_EQ(input_checks, 1);
	UT_ASSERT_EQ(legacy_probes, 0);
	UT_ASSERT_EQ(legacy_windows, 0);
	UT_ASSERT_EQ(legacy_anchors, 0);
	input_qualified = false;
	UT_ASSERT(!startup_control_site());
	UT_ASSERT_EQ(input_checks, 2);
	UT_ASSERT_EQ(legacy_probes + legacy_windows + legacy_anchors, 0);
}

UT_TEST(legacy_startup_keeps_its_control_authority_path)
{
	cluster_shared_config = false;
	cluster_enabled = cluster_controlfile_shared_authority = true;
	input_checks = legacy_probes = legacy_windows = legacy_anchors = 0;
	UT_ASSERT(startup_control_site());
	UT_ASSERT_EQ(input_checks, 0);
	UT_ASSERT_EQ(legacy_probes, 1);
	UT_ASSERT_EQ(legacy_windows, 1);
	UT_ASSERT_EQ(legacy_anchors, 1);
}

static bool
startup_recovery_control_site(void)
{
	if (sigsetjmp(error_boundary, 1))
		return false;
#include "test_cluster_startup_recovery_control.inc"
	return true;
}

UT_TEST(shared_crash_control_refuses_before_untyped_publication)
{
	reset_fixture();
	MyBackendType = B_STARTUP;
	InRecovery = true;
	current.state = DB_IN_CRASH_RECOVERY;
	selected = current;
	UT_ASSERT(!startup_recovery_control_site());
	UT_ASSERT_EQ(native_error_level, FATAL);
	UT_ASSERT_EQ(native_writes, 0);
	UT_ASSERT_EQ(root_calls + startup_calls + install_calls, 0);
	UT_ASSERT_EQ(memcmp(&selected, &current, sizeof(current)), 0);
	UT_ASSERT_EQ(CritSectionCount, 0);
}

UT_TEST(legacy_crash_control_keeps_native_publication)
{
	reset_fixture();
	cluster_shared_config = false;
	MyBackendType = B_STARTUP;
	InRecovery = true;
	current.state = DB_IN_CRASH_RECOVERY;
	UT_ASSERT(startup_recovery_control_site());
	UT_ASSERT_EQ(native_error_level, 0);
	UT_ASSERT_EQ(native_writes, 1);
}

/* PGRAC: actual directory validator on real scratch files, not a mocked
 * existence result. Only the ordinary mkdir wrapper is replaced.
 * Author: SqlRush <sqlrush@gmail.com> */
static unsigned directory_creates;
int
MakePGDirectory(const char *path)
{
	directory_creates++;
	return mkdir(path, 0700);
}
#define ValidateXLOGDirectoryStructure ActualValidateXLOGDirectoryStructure
#include "test_cluster_startup_directory_native.inc"
#undef ValidateXLOGDirectoryStructure

static bool
validate_native_directory(void)
{
	if (sigsetjmp(error_boundary, 1))
		return false;
	ActualValidateXLOGDirectoryStructure();
	return true;
}

UT_TEST(shared_startup_never_repairs_immutable_input_directory)
{
	char scratch[] = "/tmp/pgrac-native-dir-XXXXXX";
	int saved = open(".", O_RDONLY);
	struct stat st;

	UT_ASSERT(saved >= 0 && mkdtemp(scratch) != NULL);
	if (ut_current_failed)
		return;
	UT_ASSERT_EQ(chdir(scratch), 0);
	UT_ASSERT_EQ(mkdir("pg_wal", 0700), 0);
	reset_fixture();
	directory_creates = 0;
	UT_ASSERT(!validate_native_directory());
	UT_ASSERT_EQ(directory_creates, 0);
	UT_ASSERT(stat("pg_wal/archive_status", &st) != 0 && errno == ENOENT);
	/* Cleanup also accepts the old implementation's demonstrated mutation. */
	if (stat("pg_wal/archive_status", &st) == 0)
		UT_ASSERT_EQ(rmdir("pg_wal/archive_status"), 0);
	UT_ASSERT_EQ(mkdir("archive_target", 0700), 0);
	UT_ASSERT_EQ(symlink("../archive_target", "pg_wal/archive_status"), 0);
	UT_ASSERT(!validate_native_directory());
	UT_ASSERT_EQ(unlink("pg_wal/archive_status"), 0);
	UT_ASSERT_EQ(rmdir("archive_target"), 0);
	cluster_shared_config = false;
	UT_ASSERT(validate_native_directory());
	UT_ASSERT_EQ(directory_creates, 1);
	cluster_shared_config = true;
	UT_ASSERT(validate_native_directory());
	UT_ASSERT_EQ(directory_creates, 1);
	UT_ASSERT_EQ(rmdir("pg_wal/archive_status"), 0);
	UT_ASSERT_EQ(rmdir("pg_wal"), 0);
	UT_ASSERT_EQ(fchdir(saved), 0);
	UT_ASSERT_EQ(close(saved), 0);
	UT_ASSERT_EQ(rmdir(scratch), 0);
}

static XLogRecPtr
writer_bind(void)
{
	if (sigsetjmp(error_boundary, 1))
		return InvalidXLogRecPtr;
	return ClusterStartupWriterBegin(&input);
}

static XLogRecPtr
writer_begin(void)
{
	return startup_first_native_site() ? writer_bind() : InvalidXLogRecPtr;
}

UT_TEST(static_common_wait_precedes_initializer_and_native_directory)
{
	writer_begin_fixture();
	mount_waits = 2;
	UT_ASSERT_EQ(writer_begin(), wal_segment_size);
	UT_ASSERT_EQ(waits, 2);
	UT_ASSERT_EQ(advance_calls, 1);
	UT_ASSERT(!directory_before_selection);
}
UT_TEST(static_common_mismatch_or_cancel_never_writes)
{
	for (unsigned fault = 0; fault < 2; ++fault) {
		writer_begin_fixture();
		mount_result = fault == 0 ? CLUSTER_CONFIG_MOUNT_MISMATCH : CLUSTER_CONFIG_MOUNT_UNPROVEN;
		cancel_on_wait = fault == 1;
		UT_ASSERT(!startup_first_native_site());
		UT_ASSERT_EQ(advance_calls | route_calls | bind_calls | startup_directory_calls, 0);
		UT_ASSERT(!clusterStartupWriterSelected && !clusterStartupWriterBound);
		UT_ASSERT_EQ(cf_mode, NoLock);
	}
}
UT_TEST(static_common_is_rechecked_before_new_wal_binding)
{
	writer_begin_fixture();
	UT_ASSERT(startup_first_native_site());
	mount_result = CLUSTER_CONFIG_MOUNT_MISMATCH;
	UT_ASSERT_EQ(writer_bind(), InvalidXLogRecPtr);
	UT_ASSERT_EQ(route_calls | bind_calls | writer_read_calls, 0);
	UT_ASSERT(!clusterStartupWriterBound);
}

UT_TEST(initializer_selection_precedes_first_native_side_effect)
{
	writer_begin_fixture();
	advance_returns[0] = CLUSTER_CONTROL_ROOT_RECONFIG_WAIT;
	UT_ASSERT(startup_first_native_site());
	UT_ASSERT_EQ(startup_directory_calls, 1);
	UT_ASSERT(!directory_before_selection);
	UT_ASSERT(clusterStartupWriterSelected);
	UT_ASSERT(!clusterStartupWriterBound);
	UT_ASSERT_EQ(route_calls | bind_calls, 0);
	UT_ASSERT_EQ(advance_calls, 2);
	UT_ASSERT_EQ(waits, 1);
	UT_ASSERT_EQ(current.checkPoint, 100);
}

UT_TEST(initializer_selection_waits_for_original_lock_before_native_side_effect)
{
	writer_begin_fixture();
	advance_returns[0] = CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	advance_returns[1] = CLUSTER_CONTROL_ROOT_CAS_CONFLICT;
	UT_ASSERT(startup_first_native_site());
	UT_ASSERT_EQ(advance_calls, 3);
	UT_ASSERT_EQ(waits, 2);
	UT_ASSERT_EQ(startup_directory_calls, 1);
	UT_ASSERT(!directory_before_selection);
	UT_ASSERT_EQ(route_calls | bind_calls, 0);
	UT_ASSERT_EQ(current.checkPoint, 100);
}

UT_TEST(early_selection_refusal_or_cancel_precedes_native_mutation)
{
	for (unsigned f = 0; f < 7; ++f) {
		writer_begin_fixture();
		if (f == 0)
			restart_ok = false;
		if (f == 1)
			advance_returns[0] = CLUSTER_CONTROL_ROOT_IO_ERROR;
		if (f == 2) {
			advance_returns[0] = CLUSTER_CONTROL_ROOT_RECONFIG_WAIT;
			cancel_on_wait = true;
		}
		if (f == 3)
			offered.phase = CLUSTER_WAL_STARTUP_DURABLE;
		if (f == 4)
			current.state = DB_IN_PRODUCTION;
		if (f == 5)
			offered.input_record_end++;
		if (f == 6)
			offered.predecessor.snapshot.checkpoint_lower_lsn++;
		UT_ASSERT(!startup_first_native_site());
		UT_ASSERT_EQ(startup_directory_calls | route_calls | bind_calls, 0);
		UT_ASSERT(!clusterStartupWriterSelected && !clusterStartupWriterBound);
	}
}

UT_TEST(late_binding_requires_same_early_selection)
{
	for (unsigned f = 0; f < 3; ++f) {
		writer_begin_fixture();
		if (f != 0) {
			UT_ASSERT(startup_first_native_site());
			writer_read_ok = f != 1;
			writer_read_changed = f == 2;
		}
		UT_ASSERT_EQ(writer_bind(), InvalidXLogRecPtr);
		UT_ASSERT_EQ(route_calls | bind_calls, 0);
		UT_ASSERT(!clusterStartupWriterBound);
		UT_ASSERT_EQ(current.checkPoint, 100);
	}
}

UT_TEST(legacy_startup_does_not_select_shared_initializer)
{
	writer_begin_fixture();
	cluster_shared_config = false;
	UT_ASSERT(startup_first_native_site());
	UT_ASSERT_EQ(startup_directory_calls, 1);
	UT_ASSERT_EQ(advance_calls | writer_read_calls | route_calls | bind_calls, 0);
	UT_ASSERT(!clusterStartupWriterSelected);
}

UT_TEST(shared_crash_startup_uses_original_self_seal_before_native_directory)
{
	DBState crashed[] = {DB_IN_PRODUCTION, DB_SHUTDOWNING, DB_IN_CRASH_RECOVERY};
	for (unsigned i = 0; i < lengthof(crashed); i++) {
		writer_begin_fixture();
		current.state = crashed[i];
		self_seal_returns[0] = CLUSTER_CONTROL_ROOT_RECONFIG_WAIT;
		self_seal_returns[1] = CLUSTER_CONTROL_ROOT_CAS_CONFLICT;
		self_seal_returns[2] = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
		UT_ASSERT(startup_first_native_site());
		UT_ASSERT_EQ(self_seal_calls, 3);
		UT_ASSERT_EQ(waits, 2);
		UT_ASSERT_EQ(startup_directory_calls, 1);
		UT_ASSERT_EQ(advance_calls | route_calls | bind_calls | native_writes, 0);
		UT_ASSERT(!clusterStartupWriterSelected && !clusterStartupWriterBound);
		UT_ASSERT_EQ(current.state, crashed[i]);
	}
}

UT_TEST(shared_crash_refusal_or_cancel_never_reaches_native_mutation)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		writer_begin_fixture();
		current.state = DB_IN_PRODUCTION;
		if (fault == 1) {
			self_seal_returns[0] = CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
			cancel_on_wait = true;
		} else if (fault == 2)
			restart_ok = false;
		else if (fault == 3)
			mount_result = CLUSTER_CONFIG_MOUNT_MISMATCH;
		UT_ASSERT(!startup_first_native_site());
		UT_ASSERT_EQ(self_seal_calls, fault < 2 ? 1 : 0);
		UT_ASSERT_EQ(startup_directory_calls | advance_calls | route_calls | bind_calls | native_writes, 0);
		UT_ASSERT(!clusterStartupWriterSelected && !clusterStartupWriterBound);
		UT_ASSERT_EQ(cf_mode, NoLock);
	}
}

UT_TEST(clean_restart_uses_shutdown_checkpoint_above_retained_floor)
{
	writer_begin_fixture();
	offered.predecessor.snapshot.checkpoint_lower_lsn = 80;
	UT_ASSERT_EQ(writer_begin(), wal_segment_size);
	UT_ASSERT(clusterStartupWriterSelected && clusterStartupWriterBound);
	UT_ASSERT(startup_prepare(CHECKPOINT_END_OF_RECOVERY));
	UT_ASSERT_EQ(candidate.checkPoint, 100);
	UT_ASSERT_EQ(clusterStartupWriter.predecessor.snapshot.checkpoint_lower_lsn, 80);
	UT_ASSERT_EQ(clusterStartupWriter.predecessor.snapshot.tail_last_record_lsn, 100);
}

UT_TEST(clean_restart_rejects_wrong_shutdown_tail_at_each_native_boundary)
{
	writer_begin_fixture();
	offered.predecessor.snapshot.checkpoint_lower_lsn = 80;
	offered.predecessor.snapshot.tail_last_record_lsn++;
	UT_ASSERT(!startup_first_native_site());
	UT_ASSERT_EQ(startup_directory_calls | route_calls | bind_calls, 0);
	writer_begin_fixture();
	UT_ASSERT(startup_first_native_site());
	clusterStartupWriter.predecessor.snapshot.tail_last_record_lsn++;
	UT_ASSERT_EQ(writer_bind(), InvalidXLogRecPtr);
	UT_ASSERT_EQ(route_calls | bind_calls, 0);
	startup_fixture();
	clusterStartupWriter.predecessor.snapshot.tail_last_record_lsn++;
	UT_ASSERT(!startup_prepare(CHECKPOINT_END_OF_RECOVERY));
	UT_ASSERT_EQ(native_writes | local_updates, 0);
}

UT_TEST(writer_begin_routes_only_after_all_target_initializing)
{
	writer_begin_fixture();
	advance_returns[0] = CLUSTER_CONTROL_ROOT_RECONFIG_WAIT;
	advance_returns[1] = CLUSTER_CONTROL_ROOT_CAS_CONFLICT;
	UT_ASSERT_EQ(writer_begin(), wal_segment_size);
	UT_ASSERT_EQ(advance_calls, 3);
	UT_ASSERT_EQ(route_calls, 1);
	UT_ASSERT_EQ(bind_calls, 1);
	UT_ASSERT_EQ(writer_read_calls, 1);
	UT_ASSERT_EQ(waits, 2);
	UT_ASSERT(clusterStartupWriterBound && !clusterStartupWriterInstalled);
	UT_ASSERT(memcmp(&clusterStartupWriter, &offered, sizeof(offered)) == 0);
	UT_ASSERT_EQ(current.checkPoint, 100);
	UT_ASSERT_EQ(input.endOfLog, 220);
	UT_ASSERT_EQ(native_writes | local_updates, 0);
}

UT_TEST(writer_begin_rejects_wrong_input_or_failed_route_without_binding)
{
	for (unsigned f = 0; f < 13; ++f) {
		writer_begin_fixture();
		if (f == 0)
			restart_ok = false;
		if (f == 1)
			InRecovery = true;
		if (f == 2)
			ArchiveRecoveryRequested = true;
		if (f == 3)
			input.abortedRecPtr = 100;
		if (f == 4)
			input.endOfLog++;
		if (f == 5)
			input.lastRecTLI++;
		if (f == 6)
			input.lastRec++;
		if (f == 7)
			offered.first_segment_lsn++;
		if (f == 8)
			offered.phase = CLUSTER_WAL_STARTUP_DURABLE;
		if (f == 9)
			route_ok = false;
		if (f == 10)
			bind_ok = false;
		if (f == 11)
			advance_returns[0] = CLUSTER_CONTROL_ROOT_IO_ERROR;
		if (f == 12) {
			advance_returns[0] = CLUSTER_CONTROL_ROOT_RECONFIG_WAIT;
			cancel_on_wait = true;
		}
		UT_ASSERT_EQ(writer_begin(), InvalidXLogRecPtr);
		UT_ASSERT(!clusterStartupWriterBound);
		UT_ASSERT_EQ(current.checkPoint, 100);
		UT_ASSERT_EQ(native_writes | local_updates, 0);
		UT_ASSERT_EQ(cf_mode, NoLock);
	}
}

/* Execute the actual StartupXLOG buffer initialization and native address
 * conversions. Only the shared-memory allocation is a small local fixture. */
#define UsableBytesInPage (XLOG_BLCKSZ - SizeOfXLogShortPHD)
static int UsableBytesInSegment;
#include "test_cluster_startup_lsn_native.inc"

typedef struct {
	int insertpos_lck;
	uint64 CurrBytePos, PrevBytePos;
} XLogCtlInsert;
static struct {
	XLogCtlInsert Insert;
	int info_lck;
	TimeLineID InsertTimeLineID;
	struct { XLogRecPtr Write, Flush; } LogwrtResult;
} reserve_ctl, *XLogCtl = &reserve_ctl;
#define SpinLockAcquire(l) ((void)(l))
#define SpinLockRelease(l) ((void)(l))
#include "test_cluster_startup_reserve_native.inc"
#undef SpinLockAcquire
#undef SpinLockRelease

UT_TEST(native_startup_flush_requires_bound_owner_and_exact_timeline)
{
	for (unsigned fault = 0; fault < 14; fault++) {
		XLogRecPtr end = 256;
		TimeLineID timeline = 1;
		startup_fixture();
		enableFsync = true;
		reserve_ctl.InsertTimeLineID = timeline;
		reserve_ctl.LogwrtResult.Flush = end;
		switch (fault) {
		case 1: reserve_ctl.LogwrtResult.Flush--; break;
		case 2: clusterStartupWriterBound = false; break;
		case 3: clusterStartupWriterInstalled = true; break;
		case 4: MyBackendType = B_BACKEND; break;
		case 5: cluster_enabled = false; break;
		case 6: cluster_shared_config = false; break;
		case 7: enableFsync = false; break;
		case 8: ShutdownRequestPending = true; break;
		case 9: clusterStartupWriter.timeline++; break;
		case 10: reserve_ctl.InsertTimeLineID++; break;
		case 11: end = InvalidXLogRecPtr; break;
		case 12: timeline = 0; break;
		case 13: reserve_ctl.LogwrtResult.Flush++; break;
		}
		UT_ASSERT_EQ(ClusterXLogStartupFlushCovers(end, timeline), fault == 0 || fault == 13);
	}
	enableFsync = true;
}

UT_TEST(native_first_record_has_zero_xl_prev_only_for_bound_successor)
{
	for (unsigned f = 0; f < 4; ++f) {
		XLogRecPtr start, end, prev, initial;
		writer_begin_fixture();
		UT_ASSERT_EQ(writer_begin(), wal_segment_size);
		UsableBytesInSegment = (wal_segment_size / XLOG_BLCKSZ) * UsableBytesInPage
							   - (SizeOfXLogLongPHD - SizeOfXLogShortPHD);
		if (f == 1)
			cluster_shared_config = false;
		if (f == 2)
			clusterStartupWriterBound = false;
		if (f == 3)
			clusterStartupWriterInstalled = true;
		reserve_ctl.Insert.CurrBytePos = XLogRecPtrToBytePos(wal_segment_size);
		reserve_ctl.Insert.PrevBytePos = 0;
		ReserveXLogInsertLocation(64, &start, &end, &prev);
		UT_ASSERT_EQ(start, wal_segment_size + SizeOfXLogLongPHD);
		UT_ASSERT_EQ(end, start + 64);
		UT_ASSERT_EQ(prev, f == 0 ? InvalidXLogRecPtr : SizeOfXLogLongPHD);
		initial = start;
		ReserveXLogInsertLocation(64, &start, &end, &prev);
		UT_ASSERT_EQ(prev, initial);
	}
}

UT_TEST(native_startup_insert_has_no_link_or_page_from_predecessor)
{
	for (unsigned shared = 0; shared < 2; ++shared) {
		struct {
			uint64 PrevBytePos, CurrBytePos;
		} *Insert;
		struct {
			typeof(*Insert) Insert;
			XLogRecPtr InitializedUpTo, xlblocks[2];
			struct {
				XLogRecPtr Write, Flush;
			} LogwrtResult, LogwrtRqst;
			int XLogCacheBlck;
			char pages[2 * XLOG_BLCKSZ];
		} ctl = { 0 }, *XLogCtl = &ctl;
		typeof(ctl.LogwrtResult) LogwrtResult;
		EndOfWalRecoveryInfo *endOfRecoveryInfo = &input;
		XLogRecPtr EndOfLog;
		char old_page[XLOG_BLCKSZ];
		writer_begin_fixture();
		memset(old_page, 0x5a, sizeof(old_page));
		input.lastPage = old_page;
		input.lastPageBeginPtr = 0;
		ctl.XLogCacheBlck = 1;
		UsableBytesInSegment = (wal_segment_size / XLOG_BLCKSZ) * UsableBytesInPage
							   - (SizeOfXLogLongPHD - SizeOfXLogShortPHD);
		EndOfLog = shared ? writer_begin() : input.endOfLog;
		cluster_shared_config = shared;
#define XLogRecPtrToBufIdx(p) (((p) / XLOG_BLCKSZ) % (XLogCtl->XLogCacheBlck + 1))
#include "test_cluster_startup_insert_native.inc"
#undef XLogRecPtrToBufIdx
		UT_ASSERT_EQ(XLogBytePosToRecPtr(Insert->CurrBytePos),
					 shared ? wal_segment_size + SizeOfXLogLongPHD : input.endOfLog);
		if (shared) {
			UT_ASSERT_EQ(Insert->PrevBytePos, 0);
			UT_ASSERT_EQ(ctl.InitializedUpTo, wal_segment_size);
			UT_ASSERT_EQ(ctl.xlblocks[0] | ctl.xlblocks[1], 0);
			for (unsigned i = 0; i < sizeof(ctl.pages); ++i)
				UT_ASSERT_EQ(ctl.pages[i], 0);
		} else {
			UT_ASSERT_EQ(XLogBytePosToRecPtr(Insert->PrevBytePos), input.lastRec);
			UT_ASSERT_EQ(ctl.InitializedUpTo, XLOG_BLCKSZ);
			UT_ASSERT(memcmp(ctl.pages, old_page, input.endOfLog) == 0);
		}
		UT_ASSERT_EQ(ctl.LogwrtResult.Flush, EndOfLog);
		UT_ASSERT_EQ(ctl.LogwrtRqst.Flush, EndOfLog);
	}
}
int
main(void)
{
	UT_PLAN(49);
	UT_RUN(clean_input_observation_preserves_exact_old_and_new_owners);
	UT_RUN(clean_input_observation_rejects_other_input_kinds);
	UT_RUN(clean_input_observation_rejects_wrong_owner_phase_and_lock_context);
	UT_RUN(clean_input_observation_rechecks_root_and_native_checkpoint);
	UT_RUN(initializer_selection_waits_for_original_lock_before_native_side_effect);
	UT_RUN(clean_writer_checkpoint_uses_exact_existing_fence_qualification);
	UT_RUN(clean_checkpoint_cannot_borrow_other_input_epoch_or_writer);
	UT_RUN(shared_crash_control_refuses_before_untyped_publication);
	UT_RUN(legacy_crash_control_keeps_native_publication);
	UT_RUN(startup_file_sync_rechecks_original_writer_and_keeps_native_owner);
	UT_RUN(shared_crash_startup_uses_original_self_seal_before_native_directory);
	UT_RUN(shared_crash_refusal_or_cancel_never_reaches_native_mutation);
	UT_RUN(clean_restart_uses_shutdown_checkpoint_above_retained_floor);
	UT_RUN(shared_startup_does_not_adopt_legacy_control_authority);
	UT_RUN(legacy_startup_keeps_its_control_authority_path);
	UT_RUN(clean_restart_rejects_wrong_shutdown_tail_at_each_native_boundary);
	UT_RUN(static_common_wait_precedes_initializer_and_native_directory);
	UT_RUN(static_common_mismatch_or_cancel_never_writes);
	UT_RUN(static_common_is_rechecked_before_new_wal_binding);
	UT_RUN(prepare_uses_short_owned_read);
	UT_RUN(prepare_refuses_unsupported_or_unproven_input);
	UT_RUN(initialized_writer_checkpoint_uses_exact_existing_fence_qualification);
	UT_RUN(initialized_checkpoint_cannot_borrow_other_input_epoch_or_writer);
	UT_RUN(publish_retries_only_root_competition_before_projection);
	UT_RUN(publish_does_not_retry_safety_or_io_refusal);
	UT_RUN(publish_cancel_and_changed_authority_stop_owned_retry);
	UT_RUN(publish_installs_root_selected_common_fields);
	UT_RUN(native_candidate_is_private_until_publication);
	UT_RUN(native_legacy_candidate_keeps_control_write);
	UT_RUN(shutdown_prepare_requires_native_request_not_eor);
	UT_RUN(shutdown_dispatch_preserves_open_view_and_retries);
	UT_RUN(shutdown_missing_owner_or_publication_never_installs_candidate);
	UT_RUN(online_checkpoint_during_shutdown_signal_stays_online);
	UT_RUN(early_shutdown_never_calls_untyped_writer_for_root_v3);
	UT_RUN(startup_prepare_uses_only_bound_initializer_without_serving);
	UT_RUN(startup_prepare_rejects_other_purposes_and_lost_owner);
	UT_RUN(startup_publish_installs_only_actual_root_projection);
	UT_RUN(startup_publish_reobserves_cas_without_holding_locks);
	UT_RUN(startup_publish_refusal_never_installs_in_memory_candidate);
	UT_RUN(writer_begin_routes_only_after_all_target_initializing);
	UT_RUN(initializer_selection_precedes_first_native_side_effect);
	UT_RUN(early_selection_refusal_or_cancel_precedes_native_mutation);
	UT_RUN(late_binding_requires_same_early_selection);
	UT_RUN(legacy_startup_does_not_select_shared_initializer);
	UT_RUN(shared_startup_never_repairs_immutable_input_directory);
	UT_RUN(writer_begin_rejects_wrong_input_or_failed_route_without_binding);
	UT_RUN(native_startup_insert_has_no_link_or_page_from_predecessor);
	UT_RUN(native_first_record_has_zero_xl_prev_only_for_bound_successor);
	UT_RUN(native_startup_flush_requires_bound_owner_and_exact_timeline);
	UT_DONE();
	return ut_failed_count != 0;
}
