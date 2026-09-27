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
#include "cluster/cluster_wal_durable_prefix.h"
#include "cluster/cluster_write_fence.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_reconfig.h"
#include "cluster/cluster_startup_phase.h"
#include "cluster/cluster_external_fence.h"
#include "../../backend/cluster/cluster_control_root_private.h"
#include "miscadmin.h"
#include "postmaster/interrupt.h"
#include "postmaster/startup.h"
#include "storage/latch.h"
#include "storage/fd.h"
#include "storage/lwlock.h"
#include "utils/wait_event.h"
#include "port/pg_crc32c.h"

#undef printf
#undef fprintf
#include "unit_test.h"
UT_DEFINE_GLOBALS();

bool cluster_shared_config = true, cluster_enabled = true;
bool cluster_controlfile_shared_authority = true;
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
static ClusterWalDurablePrefixRef ref;
static LWLockPadded locks[NUM_INDIVIDUAL_LWLOCKS];
LWLockPadded *MainLWLockArray = locks;
static Latch latch;
Latch *MyLatch = &latch;
static LOCKMODE cf_mode;
static bool local_lock, ref_ok, lock_ok, read_ok, release_ok, fence_ok, serving_ok;
static bool provider_ok, prebump, cancel_on_wait, change_epoch_on_wait, read_error;
static uint64 epoch;
static unsigned root_calls, waits, local_updates, reads, releases;
static unsigned native_writes, shutdown_calls;
static ClusterControlRootResult returns[4];
static ClusterWalStartupImage clusterStartupWriter;
static bool clusterStartupWriterBound, clusterStartupWriterInstalled;
static bool clusterStartupWriterSelected;
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
cluster_wal_thread_current_v2_ref(ClusterWalDurablePrefixRef *o)
{
	*o = ref;
	return ref_ok;
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
cluster_wal_durable_startup_matches(const ClusterControlRootIdentity *self, const uint8 uuid[16],
									XLogRecPtr first)
{
	return startup_binding_ok && fence_ok && provider_ok && !prebump
		   && epoch == clusterStartupWriter.formation_epoch
		   && memcmp(self, &clusterStartupWriter.claim.identity, sizeof(*self)) == 0
		   && memcmp(uuid, clusterStartupWriter.operation_uuid, 16) == 0
		   && first == clusterStartupWriter.first_segment_lsn;
}

ClusterControlRootResult
cluster_control_root_v3_startup_checkpoint(const ClusterControlRootIdentity *self,
										   const uint8 uuid[16], const ControlFileData *c,
										   XLogRecPtr end, ClusterWalStartupImage *out)
{
	UT_ASSERT_EQ(cf_mode, NoLock);
	UT_ASSERT(!local_lock && CritSectionCount == 0 && MyBackendType == B_STARTUP);
	UT_ASSERT(
		cluster_wal_durable_startup_matches(self, uuid, clusterStartupWriter.first_segment_lsn));
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
cluster_wal_thread_restart_v2_ref(ClusterWalDurablePrefixRef *out)
{
	*out = ref;
	return restart_ok;
}

ClusterControlRootResult
cluster_control_root_v3_startup_advance_clean(const ClusterWalDurablePrefixRef *restart,
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
	UT_ASSERT(memcmp(self, &offered.claim.identity, sizeof(*self)) == 0);
	UT_ASSERT(memcmp(uuid, offered.operation_uuid, 16) == 0);
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
cluster_wal_durable_startup_prepare(const ClusterControlRootIdentity *self, const uint8 uuid[16],
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
	CritSectionCount = InterruptHoldoffCount = QueryCancelHoldoffCount = 0;
	InterruptPending = false;
	ShutdownRequestPending = false;
	PG_exception_stack = NULL;
	error_context_stack = NULL;
	MyAuxProcType = CheckpointerProcess;
	MyBackendType = B_CHECKPOINTER;
	root_calls = waits = local_updates = reads = releases = 0;
	native_writes = shutdown_calls = 0;
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
} reserve_ctl, *XLogCtl = &reserve_ctl;
#define SpinLockAcquire(l) ((void)(l))
#define SpinLockRelease(l) ((void)(l))
#include "test_cluster_startup_reserve_native.inc"
#undef SpinLockAcquire
#undef SpinLockRelease

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
	UT_PLAN(27);
	UT_RUN(prepare_uses_short_owned_read);
	UT_RUN(prepare_refuses_unsupported_or_unproven_input);
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
	UT_DONE();
	return ut_failed_count != 0;
}
