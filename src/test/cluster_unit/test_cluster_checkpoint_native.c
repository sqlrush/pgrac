/* Author: SqlRush <sqlrush@gmail.com> */
/* Actual native adapter; root/filesystem behavior is separately tested by
 * test_cluster_control_root. Only runtime/lock/root-return facts are replaced. */
#include "postgres.h"
#include "access/multixact.h"
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
#include "cluster/cluster_clean_leave.h"
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
#include "postmaster/checkpointer.h"
#include "postmaster/startup.h"
#include "storage/latch.h"
#include "cluster/cluster_config_members.h"
#include "cluster/cluster_recovery_anchor.h"
#include "cluster/cluster_recovery_plan.h"
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
bool cluster_shared_catalog = true, cluster_xid_striping = true;
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
static XLogRecPtr expected_control_checkpoint;
static ControlFileData *ControlFile = &current;
static ClusterWalSourceRef ref;
static LWLockPadded locks[NUM_INDIVIDUAL_LWLOCKS];
LWLockPadded *MainLWLockArray = locks;
static Latch latch;
Latch *MyLatch = &latch;
static LOCKMODE cf_mode;
static bool local_lock, ref_ok, lock_ok, read_ok, release_ok, fence_ok, serving_ok;
static bool provider_ok, prebump, cancel_on_wait, change_epoch_on_wait, read_error;
static bool initialized_ok, clean_ok, recovered_ok, lose_initialized_on_release;
static ClusterWalSourceRef initialized_ref;
static uint64 initialized_epoch;
static ClusterWalSourceRef recovered_ref;
static uint64 recovered_epoch;
static uint64 epoch;
/* The opaque owner and ROOT I/O are leaf fixture boundaries; all native
 * consumer control flow remains the production function body. */
static bool recovery_fixture, recovery_scope_valid;
static bool recovery_revoke_read, recovery_change_read;
static bool recovery_revoke_release, recovery_revoke_wait, recovery_change_wait;
static bool recovery_revoke_publish, recovery_change_publish;
static unsigned recovery_reads, recovery_publishes, recovery_begins, recovery_ends;
static ClusterWalWriterToken recovery_writer;
static char recovery_scope_identity;
#define FIXTURE_RECOVERY_SCOPE ((struct ClusterRecoveryCheckpointScopeV1 *) &recovery_scope_identity)

static unsigned root_calls, waits, local_updates, reads, releases;
static unsigned serving_pending_reads, serving_reads;
static unsigned read_pending;
static bool change_ref_on_wait;
static bool change_ref_on_release;
static unsigned native_writes, shutdown_calls;
static int native_error_level;
static ClusterControlRootResult returns[4];
static ClusterWalStartupImage clusterStartupWriter;
static bool clusterStartupWriterBound, clusterStartupWriterInstalled;
static bool clusterStartupWriterSelected;
static bool startup_sync_ok = true, startup_sync_change_epoch;
static unsigned startup_sync_calls;

/* PGRAC: ROOT and transport are fixture boundaries. The observation API and
 * all consumer control flow below are the unmodified production functions. */
static ClusterCfAttemptObservation cf_attempt;
#include "test_cluster_cf_observation.inc"
static unsigned retirement_polls, retire_remaining, required_retire_polls;
static unsigned direct_wait_slices, writer_wait_step;
static bool typed_wait, shutdown_on_wait, normal_stop_requested, shutdown_after_publish;
static unsigned wait_log_count, budget_charges, exhaust_after_charge;
static int native_sqlstate;
static int native_report_level;
static char wait_detail[768];
static LOCKMODE retiring_mode;

static void
fixture_wait_slice(LOCKMODE mode)
{
	UT_ASSERT_EQ(retiring_mode, NoLock);
	cf_attempt_begin(mode, false, NULL);
	cf_attempt.result = CLUSTER_LOCK_ACQUIRE_FAIL_TIMEOUT;
	cf_attempt.source = CLUSTER_GES_TSRC_CV_WAIT_TIMEOUT;
	cf_attempt.owner_state_at_return = CLUSTER_LOCK_OWNER_RETIRING;
	cf_attempt.request_id = 100 + cf_attempt.sequence;
	cf_attempt.request_epoch = 9;
	cf_attempt.control_owner_id = 200 + cf_attempt.sequence;
	cf_attempt.classified_wait = true;
	retiring_mode = mode;
	retire_remaining = required_retire_polls;
}

static ClusterControlRootResult
fixture_root_result(ClusterControlRootResult result)
{
	UT_ASSERT_EQ(retiring_mode, NoLock);
	if (typed_wait && result == CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE)
		fixture_wait_slice(ExclusiveLock);
	return result;
}

void
cluster_cf_retirement_poll(void)
{
	++retirement_polls;
	if (retiring_mode != NoLock && --retire_remaining == 0)
		retiring_mode = NoLock;
}

void
cluster_lock_owners_service_poll(void)
{}

uint64
cluster_cf_owner_cookie(LOCKMODE mode)
{
	return retiring_mode == mode ? cf_attempt.control_owner_id : 0;
}

bool
cluster_cf_release_completed(LOCKMODE mode, uint64 cookie)
{
	return cookie != 0 && mode != retiring_mode && cookie == cf_attempt.control_owner_id;
}

bool
cluster_normal_stop_requested(void)
{
	return normal_stop_requested;
}

uint64
cluster_checkpoint_wait_budget_now(void)
{
	return UINT64_C(1000000) + waits * UINT64_C(20000);
}

uint64 cluster_checkpoint_wait_budget_remaining_us(uint64 started_us);
uint64
cluster_checkpoint_wait_budget_remaining_us(uint64 started_us)
{
	UT_ASSERT(ShutdownRequestPending && started_us != 0);
	return UINT64_C(23999000);
}

long
cluster_checkpoint_wait_budget_charge(bool waiting, uint64 started_us,
									  const ClusterWalSourceRef *original, uint64 original_epoch)
{
	UT_ASSERT(ShutdownRequestPending && started_us != 0);
	UT_ASSERT_EQ(original_epoch, epoch);
	UT_ASSERT_EQ(memcmp(original, &ref, sizeof(ref)), 0);
	UT_ASSERT_EQ(cf_mode, NoLock);
	++budget_charges;
	if (exhaust_after_charge && budget_charges >= exhaust_after_charge)
		ereport(ERROR, (errcode(ERRCODE_CLUSTER_CONTROLFILE_AUTHORITY_UNAVAILABLE),
						errmsg("fixture original wait budget exhausted")));
	return 20;
}

bool
cluster_cf_held_is_usable(LOCKMODE mode)
{
	return cf_mode == mode;
}

/* PGRAC: readiness is diagnostic-only at this adapter boundary. */
ClusterAuthorityReadiness
cluster_authority_readiness_get(void)
{
	return CLUSTER_AUTHORITY_RECOVERY_READY;
}

ClusterFormationWitnessResult
cluster_authority_startup_refresh_recovery(int timeout_ms)
{
	UT_ASSERT_EQ(timeout_ms, 100);
	return retiring_mode == NoLock ? CLUSTER_FORMATION_WITNESS_READY
								   : CLUSTER_FORMATION_WITNESS_UNSTABLE;
}
static unsigned startup_directory_calls;
static bool directory_before_selection;
BackendType MyBackendType = B_INVALID;
static bool startup_binding_ok, startup_view_ok;
static bool startup_binding_present, fail_prepare_with_binding;
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

TimestampTz
GetCurrentTimestamp(void)
{
	return 10000000;
}
bool
TimestampDifferenceExceeds(TimestampTz start, TimestampTz end, int msec)
{
	return end - start >= (int64)msec * 1000;
}

/* The native adapter calls the real ROOT self-seal owner. Its quorum,
 * durable tail and refusal semantics have independent ROOT/QVOTEC tests. */
ClusterControlRootResult
cluster_control_root_v3_self_seal_v1(const ClusterWalSourceRef *restart, uint64 min_dead_us,
									 ClusterControlRootSnapshot *out,
									 ClusterControlRootReadToken *token)
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
	native_report_level = l;
	if (l >= ERROR)
		native_error_level = l;
	if (l == LOG)
		++wait_log_count;
	/* Native ARM CRC dispatch may emit DEBUG1 during its first invocation. */
	return l >= ERROR || l == LOG;
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
errdetail(const char *f, ...)
{
	if (native_report_level == LOG) {
		va_list args;
		va_start(args, f);
		vsnprintf(wait_detail, sizeof(wait_detail), f, args);
		va_end(args);
	}
	return 0;
}
int
errhint(const char *f pg_attribute_unused(), ...)
{
	return 0;
}
int
errcode(int e)
{
	native_sqlstate = e;
	return 0;
}
void
errfinish(const char *f pg_attribute_unused(), int l pg_attribute_unused(),
		  const char *fn pg_attribute_unused())
{
	if (native_report_level < ERROR)
		return;
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
	return cf_mode == m || retiring_mode == m;
}
bool
cluster_cf_lock(LOCKMODE m)
{
	UT_ASSERT(m == ShareLock && cf_mode == NoLock && !local_lock && CritSectionCount == 0);
	UT_ASSERT_EQ(retiring_mode, NoLock);
	if (direct_wait_slices > 0) {
		--direct_wait_slices;
		fixture_wait_slice(m);
		return false;
	}
	cf_attempt_begin(m, false, NULL);
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
	if (recovery_revoke_release)
		recovery_scope_valid = false;
	if (lose_initialized_on_release)
		initialized_ok = clean_ok = recovered_ok = false;
	if (change_ref_on_release)
		ref.claim.max_config_generation++;
	return release_ok ? CLUSTER_CF_RELEASE_CONFIRMED : CLUSTER_CF_RELEASE_UNCONFIRMED;
}
bool
cluster_cf_authority_read_check(ControlFileData *o, bool *pending)
{
	UT_ASSERT(cf_mode == ShareLock && !local_lock);
	reads++;
	if (recovery_fixture && !serving_ok)
		return false;
	if (pending)
		*pending = read_pending > 0;
	if (read_pending > 0) {
		read_pending--;
		return false;
	}
	if (read_error)
		ereport(ERROR, (errmsg("fixture read error")));
	*o = selected;
	return read_ok;
}
bool
cluster_cf_authority_read(ControlFileData *o)
{
	return cluster_cf_authority_read_check(o, NULL);
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
cluster_wal_thread_recovered_writer_matches(const ClusterWalSourceRef *expected,
													 uint64 observed_epoch)
{
	return recovered_ok && ref_ok && observed_epoch == recovered_epoch
		   && memcmp(expected, &recovered_ref, sizeof(*expected)) == 0;
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
cluster_serving_ready_check(bool *pending, const char **failed_predicate)
{
	bool unavailable = serving_pending_reads > 0;

	serving_reads++;
	if (unavailable)
		serving_pending_reads--;
	if (pending)
		*pending = unavailable;
	if (failed_predicate)
		*failed_predicate = unavailable ? "FORMATION_PENDING" : serving_ok ? NULL : "LOST";
	return !unavailable && serving_ok;
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
	UT_ASSERT_EQ(current.checkPoint, expected_control_checkpoint);
	waits++;
	if (waits > 32)
		siglongjmp(error_boundary, 1); /* Bound the fixture, never the product wait. */
	if (shutdown_on_wait)
		ShutdownRequestPending = true;
	if (change_epoch_on_wait)
		epoch++;
	if (change_ref_on_wait)
		ref.claim.identity.origin_owner_incarnation++;
	if (cancel_on_wait)
		InterruptPending = true;
	if (recovery_revoke_wait)
		recovery_scope_valid = false;
	if (recovery_change_wait)
		recovery_writer.startup_first_lsn++;
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
	UT_ASSERT_EQ(current.checkPoint, expected_control_checkpoint);
	memset(out, 0, sizeof(*out));
	memset(token, 0, sizeof(*token));
	*control = *c;
	control->checkPointCopy.nextOid = 60001;
	UT_ASSERT(root_calls < lengthof(returns));
	if (shutdown_after_publish)
		ShutdownRequestPending = true;
	return fixture_root_result(returns[root_calls++]);
}

bool
cluster_recovery_checkpoint_begin_v1(struct ClusterRecoveryCheckpointScopeV1 **out)
{
	recovery_begins++;
	if (!recovery_scope_valid)
		return false;
	*out = FIXTURE_RECOVERY_SCOPE;
	return true;
}
bool
cluster_recovery_checkpoint_current_v1(const struct ClusterRecoveryCheckpointScopeV1 *scope,
									 ClusterWalWriterToken *writer)
{
	memset(writer, 0, sizeof(*writer));
	if (scope != FIXTURE_RECOVERY_SCOPE || !recovery_scope_valid
		|| MyBackendType != B_CHECKPOINTER || CritSectionCount != 0 || ShutdownRequestPending)
		return false;
	*writer = recovery_writer;
	return true;
}
void
cluster_recovery_checkpoint_end_v1(struct ClusterRecoveryCheckpointScopeV1 **scope)
{
	if (*scope != NULL) {
		UT_ASSERT(*scope == FIXTURE_RECOVERY_SCOPE);
		recovery_ends++;
	}
	*scope = NULL;
}
ClusterControlRootResult
cluster_control_root_v3_read_recovery_checkpoint_locked(
	const struct ClusterRecoveryCheckpointScopeV1 *scope, ControlFileData *out)
{
	UT_ASSERT(scope == FIXTURE_RECOVERY_SCOPE && cf_mode == ShareLock && !local_lock);
	recovery_reads++;
	if (read_error)
		ereport(ERROR, (errmsg("fixture recovery read error")));
	*out = selected;
	if (recovery_revoke_read)
		recovery_scope_valid = false;
	if (recovery_change_read)
		recovery_writer.startup_first_lsn++;
	return read_ok ? CLUSTER_CONTROL_ROOT_OK_PRIMARY : CLUSTER_CONTROL_ROOT_IO_ERROR;
}
ClusterControlRootResult
cluster_control_root_v3_recovery_checkpoint_publish(
	const struct ClusterRecoveryCheckpointScopeV1 *scope, const ClusterControlRootIdentity *self,
	const ControlFileData *c, XLogRecPtr end, ClusterControlRootSnapshot *out,
	ClusterControlRootFileToken *token, ControlFileData *control)
{
	ClusterControlRootResult result;
	UT_ASSERT(scope == FIXTURE_RECOVERY_SCOPE);
	recovery_publishes++;
	result = cluster_control_root_v3_checkpoint_publish(self, c, end, out, token, control);
	if (recovery_revoke_publish)
		recovery_scope_valid = false;
	if (recovery_change_publish)
		recovery_writer.startup_first_lsn++;
	return result;
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

bool cluster_wal_writer_startup_binding_present(void);
bool
cluster_wal_writer_startup_binding_present(void)
{
	return startup_binding_present;
}

ClusterControlRootResult
cluster_wal_writer_begin(TimeLineID timeline, ClusterWalWriterToken *writer)
{
	memset(writer, 0, sizeof(*writer));
	writer->ref = ref;
	writer->epoch = epoch;
	return startup_binding_ok && fence_ok && timeline == clusterStartupWriter.timeline
			   ? CLUSTER_CONTROL_ROOT_OK_PRIMARY
			   : CLUSTER_CONTROL_ROOT_STALE_TOKEN;
}

ClusterControlRootResult
cluster_wal_writer_check(const ClusterWalWriterToken *writer)
{
	return writer->epoch == epoch && startup_binding_ok && fence_ok
			   ? CLUSTER_CONTROL_ROOT_OK_PRIMARY
			   : CLUSTER_CONTROL_ROOT_STALE_TOKEN;
}

bool
RequestStartupSync(void)
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
	UT_ASSERT_EQ(current.checkPoint, expected_control_checkpoint);
	*out = clusterStartupWriter;
	out->phase = CLUSTER_WAL_STARTUP_DURABLE;
	UT_ASSERT(startup_calls < lengthof(returns));
	return fixture_root_result(returns[startup_calls++]);
}

ClusterControlRootResult
cluster_wal_thread_install_startup(const ClusterWalStartupImage *durable)
{
	UT_ASSERT_EQ(cf_mode, NoLock);
	UT_ASSERT(!local_lock && CritSectionCount == 0 && startup_calls > 0);
	UT_ASSERT_EQ(durable->phase, CLUSTER_WAL_STARTUP_DURABLE);
	UT_ASSERT_EQ(current.checkPoint, expected_control_checkpoint);
	UT_ASSERT(install_calls < lengthof(install_returns));
	return fixture_root_result(install_returns[install_calls++]);
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
	UT_ASSERT_EQ(current.checkPoint, expected_control_checkpoint);
	UT_ASSERT(advance_calls < lengthof(advance_returns));
	*out = offered;
	return fixture_root_result(advance_returns[advance_calls++]);
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
	if (writer_wait_step == 1) {
		writer_wait_step = 0;
		return fixture_root_result(CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE);
	}
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
	if (writer_wait_step == 2) {
		writer_wait_step = 0;
		return fixture_root_result(CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE);
	}
	*out = offered;
	return route_ok ? CLUSTER_CONTROL_ROOT_OK_PRIMARY : CLUSTER_CONTROL_ROOT_IO_ERROR;
}

ClusterControlRootResult
cluster_wal_writer_startup_prepare(const ClusterControlRootIdentity *self, const uint8 uuid[16],
								   XLogRecPtr *first)
{
	UT_ASSERT(cf_mode == NoLock && !local_lock && CritSectionCount == 0);
	UT_ASSERT(!clusterStartupWriterBound && route_calls >= 1);
	UT_ASSERT(memcmp(self, &offered.claim.identity, sizeof(*self)) == 0);
	UT_ASSERT(memcmp(uuid, offered.operation_uuid, 16) == 0);
	bind_calls++;
	if (writer_wait_step >= 3) {
		startup_binding_present = fail_prepare_with_binding;
		if (writer_wait_step == 4)
			cf_attempt_begin(ExclusiveLock, false, NULL);
		writer_wait_step = 0;
		*first = 0;
		return fixture_root_result(CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE);
	}
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
	memset(&cf_attempt, 0, sizeof(cf_attempt));
	retirement_polls = retire_remaining = direct_wait_slices = writer_wait_step = 0;
	required_retire_polls = 3;
	typed_wait = shutdown_on_wait = normal_stop_requested = shutdown_after_publish = false;
	retiring_mode = NoLock;
	startup_binding_present = fail_prepare_with_binding = false;
	wait_log_count = budget_charges = exhaust_after_charge = 0;
	native_sqlstate = 0;
	native_report_level = 0;
	wait_detail[0] = '\0';
	memset(&current, 0, sizeof(current));
	current.checkPoint = expected_control_checkpoint = 100;
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
	recovery_fixture = false;
	recovery_scope_valid = true;
	recovery_revoke_read = recovery_change_read = recovery_revoke_release = false;
	recovery_revoke_wait = recovery_change_wait = false;
	recovery_revoke_publish = recovery_change_publish = false;
	recovery_reads = recovery_publishes = recovery_begins = recovery_ends = 0;
	memset(&recovery_writer, 0, sizeof(recovery_writer));
	recovery_writer.ref = ref;
	recovery_writer.epoch = epoch;
	recovery_writer.startup_first_lsn = 64;
	initialized_ok = clean_ok = recovered_ok = lose_initialized_on_release = false;
	initialized_ref = ref;
	initialized_epoch = epoch;
	recovered_ref = ref;
	recovered_epoch = epoch;
	CritSectionCount = InterruptHoldoffCount = QueryCancelHoldoffCount = 0;
	InterruptPending = false;
	ShutdownRequestPending = false;
	PG_exception_stack = NULL;
	error_context_stack = NULL;
	MyAuxProcType = CheckpointerProcess;
	MyBackendType = B_CHECKPOINTER;
	root_calls = waits = local_updates = reads = releases = 0;
	serving_pending_reads = serving_reads = 0;
	read_pending = 0;
	change_ref_on_wait = false;
	change_ref_on_release = false;
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
	ClusterCheckpointV3Prepare(flags, &candidate, NULL);
	return true;
}
static bool
publish(void)
{
	if (sigsetjmp(error_boundary, 1))
		return false;
	ClusterCheckpointV3Publish(&candidate, 220, NULL);
	return true;
}

/* Exercise the explicit scope separately from every ordinary native purpose. */
static bool
recovery_prepare(int flags)
{
	if (sigsetjmp(error_boundary, 1))
		return false;
	ClusterCheckpointV3Prepare(flags, &candidate, FIXTURE_RECOVERY_SCOPE);
	return true;
}
static bool
recovery_publish(void)
{
	if (sigsetjmp(error_boundary, 1))
		return false;
	ClusterCheckpointV3Publish(&candidate, 220, FIXTURE_RECOVERY_SCOPE);
	return true;
}
static void
recovery_setup(void)
{
	reset_fixture();
	recovery_fixture = true;
	serving_ok = false;
}
static bool
recovery_begin(int flags, struct ClusterRecoveryCheckpointScopeV1 **scope)
{
	if (sigsetjmp(error_boundary, 1))
		return false;
	ClusterCheckpointRecoveryBegin(flags, scope);
	return true;
}
UT_TEST(recovery_checkpoint_selects_scope_only_for_closed_online_owner)
{
	struct ClusterRecoveryCheckpointScopeV1 *scope = NULL;

	reset_fixture();
	UT_ASSERT(recovery_begin(CHECKPOINT_FORCE, &scope));
	UT_ASSERT(scope == NULL && recovery_begins == 0);
	recovery_setup();
	serving_pending_reads = 1;
	UT_ASSERT(recovery_begin(CHECKPOINT_FORCE, &scope));
	UT_ASSERT(scope == NULL && recovery_begins == 0);
	recovery_setup();
	UT_ASSERT(recovery_begin(CHECKPOINT_FORCE, &scope));
	UT_ASSERT(scope == FIXTURE_RECOVERY_SCOPE && recovery_begins == 1);
	cluster_recovery_checkpoint_end_v1(&scope);
	UT_ASSERT(scope == NULL && recovery_ends == 1);
	recovery_setup();
	recovery_scope_valid = false;
	UT_ASSERT(!recovery_begin(CHECKPOINT_FORCE, &scope));
	UT_ASSERT(scope == NULL && recovery_begins == 1);
	for (int purpose = 0; purpose < 3; purpose++) {
		recovery_setup();
		if (purpose == 2) {
			MyBackendType = B_STARTUP;
			MyAuxProcType = StartupProcess;
		}
		UT_ASSERT(recovery_begin(purpose == 0 ? CHECKPOINT_IS_SHUTDOWN
										: purpose == 1 ? CHECKPOINT_END_OF_RECOVERY : CHECKPOINT_FORCE,
								&scope));
		UT_ASSERT(scope == NULL && recovery_begins == 0);
	}
}

UT_TEST(recovery_checkpoint_closed_admission_uses_only_original_scope)
{
	recovery_setup();
	UT_ASSERT(recovery_prepare(CHECKPOINT_FORCE));
	UT_ASSERT_EQ(candidate.checkPoint, 150);
	UT_ASSERT_EQ(recovery_reads, 1);
	UT_ASSERT_EQ(reads, 0);
	UT_ASSERT_EQ(cf_mode, NoLock);
	UT_ASSERT_EQ(current.checkPoint, 100);
	candidate = selected;
	candidate.checkPoint = 200;
	UT_ASSERT(recovery_publish());
	UT_ASSERT_EQ(recovery_publishes, 1);
	UT_ASSERT_EQ(root_calls, 1);
	UT_ASSERT_EQ(current.checkPoint, 200);
	UT_ASSERT_EQ(current.checkPointCopy.nextOid, 60001);
	UT_ASSERT(!serving_ok);
}
UT_TEST(recovery_checkpoint_preserves_owner_and_native_guards)
{
	for (int stage = 0; stage < 2; stage++) {
		for (int fault = 0; fault < 10; fault++) {
			recovery_setup();
			switch (fault) {
				case 0: recovery_scope_valid = false; break;
				case 1: recovery_writer.ref.claim.identity.origin_owner_incarnation++; break;
				case 2: recovery_writer.epoch++; break;
				case 3: provider_ok = false; initialized_ok = clean_ok = true; break;
				case 4: fence_ok = false; break;
				case 5: prebump = true; break;
				case 6: ref_ok = false; break;
				case 7: CritSectionCount = 1; break;
				case 8: ShutdownRequestPending = true; break;
				case 9: MyBackendType = B_BACKEND; break;
			}
			UT_ASSERT(!(stage == 0 ? recovery_prepare(CHECKPOINT_FORCE) : recovery_publish()));
			UT_ASSERT_EQ(recovery_reads | recovery_publishes | root_calls, 0);
			UT_ASSERT_EQ(current.checkPoint, 100);
			UT_ASSERT_EQ(cf_mode, NoLock);
		}
	}
}
UT_TEST(recovery_checkpoint_rechecks_read_and_confirmed_release)
{
	for (int fault = 0; fault < 7; fault++) {
		recovery_setup();
		switch (fault) {
			case 0: recovery_revoke_read = true; break;
			case 1: recovery_change_read = true; break;
			case 2: recovery_revoke_release = true; break;
			case 3: change_ref_on_release = true; break;
			case 4: release_ok = false; break;
			case 5: read_ok = false; break;
			case 6: read_error = true; break;
		}
		UT_ASSERT(!recovery_prepare(CHECKPOINT_FORCE));
		UT_ASSERT_EQ(recovery_reads, 1);
		UT_ASSERT_EQ(candidate.checkPoint, 0);
		UT_ASSERT_EQ(cf_mode, NoLock);
		UT_ASSERT_EQ(releases, 1);
		UT_ASSERT_EQ(current.checkPoint, 100);
	}
}
UT_TEST(recovery_checkpoint_retries_without_reclassifying_its_owner)
{
	for (int fault = 0; fault < 5; fault++) {
		recovery_setup();
		returns[0] = CLUSTER_CONTROL_ROOT_CAS_CONFLICT;
		if (fault == 1) recovery_revoke_wait = true;
		if (fault == 2) recovery_change_wait = true;
		if (fault == 3) cancel_on_wait = true;
		if (fault == 4) change_epoch_on_wait = true;
		UT_ASSERT_EQ(recovery_publish(), fault == 0);
		UT_ASSERT_EQ(root_calls, fault == 0 ? 2 : 1);
		UT_ASSERT_EQ(recovery_publishes, root_calls);
		UT_ASSERT_EQ(waits, 1);
		UT_ASSERT_EQ(current.checkPoint, fault == 0 ? 200 : 100);
		UT_ASSERT(!serving_ok && cf_mode == NoLock && !local_lock);
	}
}
UT_TEST(recovery_checkpoint_rechecks_durable_result_before_projection)
{
	for (int fault = 0; fault < 2; fault++) {
		recovery_setup();
		if (fault == 0) recovery_revoke_publish = true;
		if (fault == 1) recovery_change_publish = true;
		UT_ASSERT(!recovery_publish());
		UT_ASSERT_EQ(root_calls, 1);
		UT_ASSERT_EQ(recovery_publishes, 1);
		UT_ASSERT_EQ(current.checkPoint, 100);
		UT_ASSERT_EQ(local_updates, 0);
	}
}
UT_TEST(recovery_checkpoint_does_not_replace_other_native_purposes)
{
	recovery_setup();
	UT_ASSERT(!prepare(CHECKPOINT_FORCE));
	UT_ASSERT(!publish());
	UT_ASSERT_EQ(recovery_reads | recovery_publishes | root_calls, 0);
	for (int startup = 0; startup < 2; startup++) {
		recovery_setup();
		if (startup) MyBackendType = B_STARTUP;
		else ShutdownRequestPending = true;
		UT_ASSERT(!recovery_prepare(startup ? CHECKPOINT_END_OF_RECOVERY : CHECKPOINT_IS_SHUTDOWN));
		UT_ASSERT_EQ(recovery_reads | startup_calls, 0);
		recovery_setup();
		if (startup) MyBackendType = B_STARTUP;
		else { ShutdownRequestPending = true; candidate.state = DB_SHUTDOWNED; }
		UT_ASSERT(!recovery_publish());
		UT_ASSERT_EQ(recovery_publishes | root_calls | startup_calls, 0);
	}
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
UT_TEST(prepare_pending_releases_before_normal_and_shutdown_retry)
{
	for (int shutdown = 0; shutdown < 2; shutdown++) {
		reset_fixture();
		read_pending = 2;
		ShutdownRequestPending = shutdown != 0;
		UT_ASSERT(prepare(shutdown ? CHECKPOINT_IS_SHUTDOWN : CHECKPOINT_FORCE));
		UT_ASSERT_EQ(reads, 3);
		UT_ASSERT_EQ(releases, 3);
		UT_ASSERT_EQ(waits, 2);
		UT_ASSERT_EQ(cf_mode, NoLock);
		UT_ASSERT_EQ(candidate.checkPoint, 150);
		UT_ASSERT_EQ(current.checkPoint, 100);
	}
}
UT_TEST(prepare_pending_cannot_hide_release_loss_cancel_or_changed_owner)
{
	for (int fault = 0; fault < 5; fault++) {
		reset_fixture();
		read_pending = 1;
		release_ok = fault != 0;
		cancel_on_wait = fault == 1;
		change_epoch_on_wait = fault == 2;
		change_ref_on_wait = fault == 3;
		read_ok = fault != 4;
		UT_ASSERT(!prepare(CHECKPOINT_FORCE));
		UT_ASSERT_EQ(waits, fault == 0 ? 0 : 1);
		UT_ASSERT_EQ(reads, fault == 4 ? 2 : 1);
		UT_ASSERT_EQ(reads, releases);
		UT_ASSERT_EQ(cf_mode, NoLock);
		UT_ASSERT_EQ(current.checkPoint, 100);
		UT_ASSERT_EQ(candidate.checkPoint, 0);
	}
	for (int pending = 0; pending < 2; pending++) {
		reset_fixture();
		read_pending = pending;
		change_ref_on_release = true;
		UT_ASSERT(!prepare(CHECKPOINT_FORCE));
		UT_ASSERT_EQ(waits, 0);
		UT_ASSERT_EQ(releases, 1);
		UT_ASSERT_EQ(candidate.checkPoint, 0);
		UT_ASSERT_EQ(cf_mode, NoLock);
	}
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
UT_TEST(recovered_writer_checkpoint_uses_exact_recovered_fence_qualification)
{
	for (unsigned shutdown = 0; shutdown < 2; shutdown++) {
		reset_fixture();
		provider_ok = false;
		recovered_ok = true;
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
	for (unsigned fault = 0; fault < 4; fault++) {
		reset_fixture();
		provider_ok = false;
		recovered_ok = fault != 0;
		if (fault == 1)
			recovered_epoch++;
		if (fault == 2)
			recovered_ref.claim.identity.origin_owner_incarnation++;
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
UT_TEST(publish_waits_for_pending_admission_outside_locks)
{
	for (unsigned shutdown = 0; shutdown < 2; shutdown++) {
		reset_fixture();
		ShutdownRequestPending = shutdown != 0;
		candidate.state = shutdown ? DB_SHUTDOWNED : DB_IN_PRODUCTION;
		serving_pending_reads = 2;
		returns[0] = CLUSTER_CONTROL_ROOT_ADMISSION_PENDING;
		UT_ASSERT(publish());
		UT_ASSERT_EQ(serving_reads, 4);
		UT_ASSERT_EQ(root_calls, 2);
		UT_ASSERT_EQ(shutdown_calls, shutdown ? 2 : 0);
		UT_ASSERT_EQ(waits, 3);
		UT_ASSERT_EQ(local_updates, 1);
		UT_ASSERT_EQ(current.checkPoint, 200);
		UT_ASSERT_EQ(cf_mode, NoLock);
	}
}
UT_TEST(publish_pending_still_refuses_loss_cancel_and_epoch_change)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		reset_fixture();
		serving_pending_reads = 1;
		serving_ok = fault != 0;
		cancel_on_wait = fault == 1;
		change_epoch_on_wait = fault == 2;
		UT_ASSERT(!publish());
		UT_ASSERT_EQ(waits, 1);
		UT_ASSERT_EQ(root_calls, 0);
		UT_ASSERT_EQ(local_updates, 0);
		UT_ASSERT_EQ(current.checkPoint, 100);
		UT_ASSERT_EQ(cf_mode, NoLock);
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
	UT_ASSERT(!publish());
	UT_ASSERT_EQ(root_calls, 0);
	UT_ASSERT_EQ(native_error_level, ERROR);
	reset_fixture();
	shutdown_after_publish = true;
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
	ClusterCheckpointV3Prepare(flags, &candidate, NULL);
	return true;
}

static bool
startup_publish(void)
{
	if (sigsetjmp(error_boundary, 1))
		return false;
	ClusterCheckpointV3Publish(&candidate, 220, NULL);
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

UT_TEST(startup_checkpoint_accepts_recovered_cohort)
{
	startup_fixture();
	clusterStartupWriter.input_kind = CLUSTER_WAL_STARTUP_RECOVERED;
	clusterStartupWriter.generation = 7;
	UT_ASSERT(startup_prepare(CHECKPOINT_END_OF_RECOVERY | CHECKPOINT_IMMEDIATE));
	UT_ASSERT_EQ(candidate.checkPoint, 100);
	candidate.checkPoint = 200;
	UT_ASSERT(startup_publish());
	UT_ASSERT_EQ(startup_calls, 1);
	UT_ASSERT_EQ(install_calls, 1);
	UT_ASSERT_EQ(reads, 1);
	UT_ASSERT_EQ(local_updates, 1);
	UT_ASSERT(clusterStartupWriterInstalled);
	UT_ASSERT_EQ(current.checkPoint, 200);
	UT_ASSERT_EQ(current.state, DB_IN_PRODUCTION);
	UT_ASSERT_EQ(root_calls, 0);
}

UT_TEST(startup_file_sync_rechecks_original_writer_and_keeps_native_owner)
{
	for (unsigned fault = 0; fault < 8; fault++) {
		startup_fixture();
		startup_sync_calls = 0;
		startup_sync_ok = true;
		startup_sync_change_epoch = false;
		switch (fault) {
		case 1:
			clusterStartupWriterBound = false;
			break;
		case 2:
			clusterStartupWriterInstalled = true;
			break;
		case 3:
			startup_binding_ok = false;
			break;
		case 4:
			startup_sync_ok = false;
			break;
		case 5:
			startup_sync_change_epoch = true;
			break;
		case 6:
			cf_mode = ExclusiveLock;
			break;
		case 7:
			local_lock = true;
			break;
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
/* PGRAC: the recorded-parameter check runs after the input check and
 * before any other startup control step; refused, nothing else runs. */
static unsigned parameter_checks;
static bool parameters_changed;
void
ClusterRequireRecordedParameters(void)
{
	UT_ASSERT_EQ(parameter_checks + 1, input_checks);
	parameter_checks++;
	if (parameters_changed)
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
	parameters_changed = false;
	input_checks = legacy_probes = legacy_windows = legacy_anchors = parameter_checks = 0;
	UT_ASSERT(startup_control_site());
	UT_ASSERT_EQ(input_checks, 1);
	UT_ASSERT_EQ(parameter_checks, 1);
	UT_ASSERT_EQ(legacy_probes, 0);
	UT_ASSERT_EQ(legacy_windows, 0);
	UT_ASSERT_EQ(legacy_anchors, 0);
	parameters_changed = true;
	UT_ASSERT(!startup_control_site());
	UT_ASSERT_EQ(input_checks, 2);
	UT_ASSERT_EQ(parameter_checks, 2);
	UT_ASSERT_EQ(legacy_probes + legacy_windows + legacy_anchors, 0);
	parameters_changed = false;
	input_qualified = false;
	input_checks = parameter_checks = 0;
	UT_ASSERT(!startup_control_site());
	UT_ASSERT_EQ(input_checks, 1);
	UT_ASSERT_EQ(parameter_checks, 0);
	UT_ASSERT_EQ(legacy_probes + legacy_windows + legacy_anchors, 0);
}

UT_TEST(legacy_startup_keeps_its_control_authority_path)
{
	cluster_shared_config = false;
	cluster_enabled = cluster_controlfile_shared_authority = true;
	input_checks = legacy_probes = legacy_windows = legacy_anchors = parameter_checks = 0;
	UT_ASSERT(startup_control_site());
	UT_ASSERT_EQ(input_checks, 0);
	UT_ASSERT_EQ(parameter_checks, 0);
	UT_ASSERT_EQ(legacy_probes, 1);
	UT_ASSERT_EQ(legacy_windows, 1);
	UT_ASSERT_EQ(legacy_anchors, 1);
}

static unsigned cold_prepare_calls;
static bool cold_prepare_ok;

void
ClusterPrepareColdWalRecovery(void)
{
	cold_prepare_calls++;
	if (!cold_prepare_ok)
		ereport(FATAL, (errmsg("typed cold preparation refused")));
}

static bool
startup_recovery_control_site(void)
{
	if (sigsetjmp(error_boundary, 1))
		return false;
#include "test_cluster_startup_recovery_control.inc"
	return true;
}

UT_TEST(shared_crash_control_prepares_without_untyped_publication)
{
	reset_fixture();
	cold_prepare_calls = 0;
	cold_prepare_ok = true;
	MyBackendType = B_STARTUP;
	InRecovery = true;
	current.state = DB_IN_CRASH_RECOVERY;
	selected = current;
	UT_ASSERT(startup_recovery_control_site());
	UT_ASSERT_EQ(cold_prepare_calls, 1);
	UT_ASSERT_EQ(native_error_level, 0);
	UT_ASSERT_EQ(native_writes, 0);
	UT_ASSERT_EQ(root_calls + startup_calls + install_calls, 0);
	UT_ASSERT_EQ(memcmp(&selected, &current, sizeof(current)), 0);
	UT_ASSERT_EQ(CritSectionCount, 0);
}

UT_TEST(shared_crash_control_prepare_refusal_never_publishes)
{
	reset_fixture();
	cold_prepare_calls = 0;
	cold_prepare_ok = false;
	MyBackendType = B_STARTUP;
	InRecovery = true;
	current.state = DB_IN_CRASH_RECOVERY;
	selected = current;
	UT_ASSERT(!startup_recovery_control_site());
	UT_ASSERT_EQ(cold_prepare_calls, 1);
	UT_ASSERT_EQ(native_error_level, FATAL);
	UT_ASSERT_EQ(native_writes, 0);
	UT_ASSERT_EQ(memcmp(&selected, &current, sizeof(current)), 0);
}

UT_TEST(legacy_crash_control_keeps_native_publication)
{
	reset_fixture();
	cold_prepare_calls = 0;
	cluster_shared_config = false;
	MyBackendType = B_STARTUP;
	InRecovery = true;
	current.state = DB_IN_CRASH_RECOVERY;
	UT_ASSERT(startup_recovery_control_site());
	UT_ASSERT_EQ(native_error_level, 0);
	UT_ASSERT_EQ(native_writes, 1);
	UT_ASSERT_EQ(cold_prepare_calls, 0);
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

UT_TEST(initializer_selection_accepts_recovered_cohort)
{
	writer_begin_fixture();
	offered.input_kind = CLUSTER_WAL_STARTUP_RECOVERED;
	UT_ASSERT(startup_first_native_site());
	UT_ASSERT_EQ(startup_directory_calls, 1);
	UT_ASSERT(!directory_before_selection);
	UT_ASSERT(clusterStartupWriterSelected);
	UT_ASSERT_EQ(clusterStartupWriter.input_kind, CLUSTER_WAL_STARTUP_RECOVERED);
	UT_ASSERT_EQ(route_calls | bind_calls, 0);
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
	DBState crashed[] = { DB_IN_PRODUCTION, DB_SHUTDOWNING, DB_IN_CRASH_RECOVERY };
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
		UT_ASSERT_EQ(
			startup_directory_calls | advance_calls | route_calls | bind_calls | native_writes, 0);
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

UT_TEST(writer_begin_accepts_recovered_cohort)
{
	writer_begin_fixture();
	offered.input_kind = CLUSTER_WAL_STARTUP_RECOVERED;
	UT_ASSERT_EQ(writer_begin(), wal_segment_size);
	UT_ASSERT(clusterStartupWriterSelected && clusterStartupWriterBound);
	UT_ASSERT_EQ(clusterStartupWriter.input_kind, CLUSTER_WAL_STARTUP_RECOVERED);
	UT_ASSERT_EQ(route_calls, 1);
	UT_ASSERT_EQ(bind_calls, 1);
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
	struct {
		XLogRecPtr Write, Flush;
	} LogwrtResult;
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
		case 1:
			reserve_ctl.LogwrtResult.Flush--;
			break;
		case 2:
			clusterStartupWriterBound = false;
			break;
		case 3:
			clusterStartupWriterInstalled = true;
			break;
		case 4:
			MyBackendType = B_BACKEND;
			break;
		case 5:
			cluster_enabled = false;
			break;
		case 6:
			cluster_shared_config = false;
			break;
		case 7:
			enableFsync = false;
			break;
		case 8:
			ShutdownRequestPending = true;
			break;
		case 9:
			clusterStartupWriter.timeline++;
			break;
		case 10:
			reserve_ctl.InsertTimeLineID++;
			break;
		case 11:
			end = InvalidXLogRecPtr;
			break;
		case 12:
			timeline = 0;
			break;
		case 13:
			reserve_ctl.LogwrtResult.Flush++;
			break;
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
UT_TEST(startup_configuration_and_selection_retire_before_reobserving)
{
	writer_begin_fixture();
	typed_wait = true;
	advance_returns[0] = CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	UT_ASSERT_EQ(writer_begin(), wal_segment_size);
	UT_ASSERT(retirement_polls >= 3);
	UT_ASSERT_EQ(advance_calls, 2);
	UT_ASSERT_EQ(retiring_mode, NoLock);
}

UT_TEST(writer_begin_retries_only_its_unfinished_step)
{
	for (unsigned step = 1; step <= 4; ++step) {
		writer_begin_fixture();
		typed_wait = true;
		writer_wait_step = step;
		UT_ASSERT_EQ(writer_begin(), wal_segment_size);
		UT_ASSERT(clusterStartupWriterBound);
		UT_ASSERT_EQ(writer_read_calls, step == 1 ? 2 : 1);
		UT_ASSERT_EQ(route_calls, step == 2 ? 2 : 1);
		UT_ASSERT_EQ(bind_calls, step >= 3 ? 2 : 1);
		UT_ASSERT_EQ(advance_calls, 1);
		UT_ASSERT(retirement_polls >= 3);
	}
}

UT_TEST(writer_prepare_nonok_with_binding_is_not_retried_or_cleared)
{
	writer_begin_fixture();
	typed_wait = true;
	writer_wait_step = 3;
	fail_prepare_with_binding = true;
	UT_ASSERT_EQ(writer_begin(), InvalidXLogRecPtr);
	UT_ASSERT_EQ(native_error_level, FATAL);
	UT_ASSERT_EQ(native_sqlstate, ERRCODE_CLUSTER_CONTROLFILE_AUTHORITY_UNAVAILABLE);
	UT_ASSERT_EQ(bind_calls, 1);
	UT_ASSERT_EQ(waits, 0);
	UT_ASSERT(startup_binding_present);
	UT_ASSERT(!clusterStartupWriterBound);
}

UT_TEST(startup_checkpoint_retries_durable_install_and_readback_separately)
{
	for (unsigned step = 0; step < 3; ++step) {
		startup_fixture();
		typed_wait = true;
		if (step == 0)
			returns[0] = CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
		else if (step == 1)
			install_returns[0] = CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
		else
			direct_wait_slices = 1;
		UT_ASSERT(startup_publish());
		UT_ASSERT(clusterStartupWriterInstalled);
		UT_ASSERT_EQ(startup_calls, step == 0 ? 2 : 1);
		UT_ASSERT_EQ(install_calls, step == 1 ? 2 : 1);
		UT_ASSERT_EQ(reads, 1);
		UT_ASSERT(retirement_polls >= 3);
	}
}

UT_TEST(checkpoint_prepare_publish_retire_before_new_attempt)
{
	reset_fixture();
	direct_wait_slices = 1;
	UT_ASSERT(prepare(CHECKPOINT_FORCE));
	UT_ASSERT_EQ(reads, 1);
	UT_ASSERT(retirement_polls >= 3);
	reset_fixture();
	typed_wait = true;
	returns[0] = CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	UT_ASSERT(publish());
	UT_ASSERT_EQ(root_calls, 2);
	UT_ASSERT(retirement_polls >= 3);
}

UT_TEST(online_checkpoint_wait_yields_to_normal_shutdown)
{
	for (unsigned branch = 0; branch < 5; ++branch) {
		reset_fixture();
		shutdown_on_wait = true;
		if (branch < 2) {
			if (branch == 0)
				read_pending = 1;
			else
				direct_wait_slices = 1;
			UT_ASSERT(!prepare(CHECKPOINT_FORCE));
			UT_ASSERT_EQ(reads, branch == 0 ? 1 : 0);
		} else {
			returns[0] = branch == 2   ? CLUSTER_CONTROL_ROOT_CAS_CONFLICT
						 : branch == 3 ? CLUSTER_CONTROL_ROOT_ADMISSION_PENDING
									   : CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
			typed_wait = branch == 4;
			UT_ASSERT(!publish());
			UT_ASSERT_EQ(root_calls, 1);
		}
		UT_ASSERT_EQ(current.checkPoint, 100);
		UT_ASSERT_EQ(native_error_level, ERROR);
		UT_ASSERT_EQ(native_sqlstate, ERRCODE_QUERY_CANCELED);
		UT_ASSERT_EQ(budget_charges, 0);
		UT_ASSERT(ShutdownRequestPending);
	}
}

UT_TEST(startup_wait_preserves_retirement_when_original_phase_terminates)
{
	writer_begin_fixture();
	typed_wait = true;
	advance_returns[0] = CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	required_retire_polls = 100;
	shutdown_on_wait = true;
	UT_ASSERT(!startup_first_native_site());
	UT_ASSERT_EQ(waits, 1);
	UT_ASSERT_EQ(advance_calls, 1);
	UT_ASSERT_EQ(retiring_mode, ExclusiveLock);
	UT_ASSERT(ShutdownRequestPending);
}

UT_TEST(shutdown_both_consumers_charge_while_original_retirement_remains)
{
	for (unsigned stage = 0; stage < 2; ++stage) {
		reset_fixture();
		ShutdownRequestPending = true;
		required_retire_polls = 100;
		exhaust_after_charge = 4;
		if (stage == 0) {
			direct_wait_slices = 1;
			UT_ASSERT(!prepare(CHECKPOINT_IS_SHUTDOWN));
			UT_ASSERT_EQ(reads, 0);
		} else {
			candidate.state = DB_SHUTDOWNED;
			typed_wait = true;
			returns[0] = CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
			UT_ASSERT(!publish());
			UT_ASSERT_EQ(root_calls, 1);
		}
		UT_ASSERT_EQ(budget_charges, 4);
		UT_ASSERT_EQ(waits, 3);
		UT_ASSERT_EQ(native_error_level, ERROR);
		UT_ASSERT_EQ(native_sqlstate, ERRCODE_CLUSTER_CONTROLFILE_AUTHORITY_UNAVAILABLE);
		UT_ASSERT_NE(retiring_mode, NoLock);
		UT_ASSERT_EQ(current.checkPoint, 100);
	}
}

UT_TEST(wait_diagnostic_once_per_original_operation_not_request)
{
	reset_fixture();
	typed_wait = true;
	returns[0] = returns[1] = CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	UT_ASSERT(publish());
	UT_ASSERT_EQ(root_calls, 3);
	UT_ASSERT_EQ(wait_log_count, 1);
	/* A new invocation is a new operation; transport requests alone were not. */
	root_calls = 0;
	expected_control_checkpoint = current.checkPoint;
	candidate.checkPoint = 300;
	UT_ASSERT(publish());
	UT_ASSERT_EQ(wait_log_count, 2);
}

UT_TEST(shutdown_wait_log_carries_the_remaining_wait_budget)
{
	for (unsigned stage = 0; stage < 2; ++stage) {
		reset_fixture();
		ShutdownRequestPending = true;
		if (stage == 0) {
			direct_wait_slices = 1;
			UT_ASSERT(prepare(CHECKPOINT_IS_SHUTDOWN));
		} else {
			candidate.state = DB_SHUTDOWNED;
			typed_wait = true;
			returns[0] = CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
			UT_ASSERT(publish());
		}
		UT_ASSERT_EQ(wait_log_count, 1);
		UT_ASSERT(strstr(wait_detail, "remaining_us=23999000") != NULL);
	}
}

UT_TEST(checkpoint_hard_results_and_untyped_lock_never_consume_wait_observation)
{
	const ClusterControlRootResult failures[]
		= { CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE, CLUSTER_CONTROL_ROOT_RELEASE_UNCERTAIN,
			CLUSTER_CONTROL_ROOT_IO_ERROR, CLUSTER_CONTROL_ROOT_STALE_TOKEN };
	for (unsigned i = 0; i < lengthof(failures); ++i) {
		reset_fixture();
		returns[0] = failures[i];
		UT_ASSERT(!publish());
		UT_ASSERT_EQ(root_calls, 1);
		UT_ASSERT_EQ(waits, 0);
		UT_ASSERT_EQ(wait_log_count, 0);
		UT_ASSERT_EQ(current.checkPoint, 100);
	}
	reset_fixture();
	lock_ok = false;
	UT_ASSERT(!prepare(CHECKPOINT_FORCE));
	UT_ASSERT_EQ(waits, 0);
}

UT_TEST(cf_observation_saturation_never_wraps_or_reuses_an_old_receipt)
{
	ClusterCfAttemptObservation observation;
	reset_fixture();
	cf_attempt.sequence = UINT64_MAX - 1;
	fixture_wait_slice(ShareLock);
	UT_ASSERT_EQ(cluster_cf_attempt_sequence(), UINT64_MAX);
	UT_ASSERT(!cluster_cf_last_attempt_since(UINT64_MAX - 1, &observation));
	observation = cf_attempt;
	UT_ASSERT(!cluster_cf_attempt_wait_slice(&observation));
	cf_attempt_begin(ExclusiveLock, false, NULL);
	UT_ASSERT_EQ(cluster_cf_attempt_sequence(), UINT64_MAX);
	UT_ASSERT(!cluster_cf_last_attempt_since(0, &observation));
}

UT_TEST(retirement_rejects_usable_holds_and_missing_original_cookie)
{
	for (unsigned fault = 0; fault < 3; ++fault) {
		ClusterCfAttemptObservation waiting = { 0 };
		bool caught = false;
		reset_fixture();
		if (fault < 2)
			cf_mode = fault == 0 ? ShareLock : ExclusiveLock;
		else {
			waiting.mode = ShareLock;
			waiting.owner_state_at_return = CLUSTER_LOCK_OWNER_RETIRING;
		}
		if (sigsetjmp(error_boundary, 1) == 0)
			(void)ClusterCfRetirementPending(&waiting);
		else
			caught = true;
		UT_ASSERT(caught);
		UT_ASSERT_EQ(native_error_level, ERROR);
		UT_ASSERT_EQ(native_sqlstate, ERRCODE_CLUSTER_CONTROLFILE_AUTHORITY_UNAVAILABLE);
		UT_ASSERT_EQ(waits | root_calls | native_writes, 0);
		if (fault < 2)
			UT_ASSERT_EQ(cf_mode, fault == 0 ? ShareLock : ExclusiveLock);
	}
}

UT_TEST(native_empty_input_requires_the_original_bound_initializer)
{
	for (unsigned fault = 0; fault < 22; ++fault) {
		ControlFileData before;
		ClusterWalStartupImage original;
		writer_begin_fixture();
		cluster_shared_catalog = cluster_xid_striping = true;
		offered.input_kind = CLUSTER_WAL_STARTUP_INITIALIZED;
		current.checkPointCopy.nextMulti = current.checkPointCopy.oldestMulti = FirstMultiXactId;
		current.checkPointCopy.nextMultiOffset = 0;
		UT_ASSERT_EQ(writer_begin(), wal_segment_size);
		switch (fault) {
		case 1:
			clusterStartupWriter.input_kind = CLUSTER_WAL_STARTUP_CLEAN;
			break;
		case 2:
			clusterStartupWriter.input_kind = CLUSTER_WAL_STARTUP_RECOVERED;
			break;
		case 3:
			clusterStartupWriterSelected = false;
			break;
		case 4:
			clusterStartupWriterBound = false;
			break;
		case 5:
			clusterStartupWriterInstalled = true;
			break;
		case 6:
			startup_binding_ok = false;
			break;
		case 7:
			current.system_identifier++;
			break;
		case 8:
			current.checkPoint++;
			break;
		case 9:
			current.checkPointCopy.ThisTimeLineID++;
			break;
		case 10:
			current.checkPointCopy.nextMulti++;
			break;
		case 11:
			current.checkPointCopy.oldestMulti++;
			break;
		case 12:
			current.checkPointCopy.nextMultiOffset++;
			break;
		case 13:
			ShutdownRequestPending = true;
			break;
		case 14:
			ArchiveRecoveryRequested = true;
			break;
		case 15:
			clusterStartupWriter.phase = CLUSTER_WAL_STARTUP_DURABLE;
			break;
		case 16:
			MyBackendType = B_BACKEND;
			break;
		case 17:
			current.state = DB_IN_PRODUCTION;
			break;
		case 18:
			current.backupStartPoint = 1;
			break;
		case 19:
			cluster_shared_catalog = false;
			break;
		case 20:
			cluster_xid_striping = false;
			break;
		case 21:
			provider_ok = false;
			break;
		}
		before = current;
		original = clusterStartupWriter;
		/* PGRAC: runtime caller/admission facts are separate from input bytes. */
		UT_ASSERT_EQ(ClusterXLogNativeEmptyMultiXactInputMatches(
						 &original, clusterStartupWriterSelected, clusterStartupWriterBound,
						 clusterStartupWriterInstalled, &before),
					 fault == 0 || fault == 6 || fault == 13 || fault == 14 || fault == 16
						 || fault == 19 || fault == 20 || fault == 21);
		UT_ASSERT_EQ(ClusterXLogStartupNativeEmptyMultiXactInput(), fault == 0);
		UT_ASSERT_EQ(memcmp(&before, &current, sizeof(current)), 0);
		UT_ASSERT_EQ(memcmp(&original, &clusterStartupWriter, sizeof(original)), 0);
		UT_ASSERT_EQ(native_writes | install_calls, 0);
	}
	cluster_shared_catalog = cluster_xid_striping = true;
}

int
main(void)
{
	if (getenv("PGRAC_RECOVERY_CHECKPOINT_ONLY") != NULL) {
		UT_PLAN(7);
		UT_RUN(recovery_checkpoint_selects_scope_only_for_closed_online_owner);
		UT_RUN(recovery_checkpoint_closed_admission_uses_only_original_scope);
		UT_RUN(recovery_checkpoint_preserves_owner_and_native_guards);
		UT_RUN(recovery_checkpoint_rechecks_read_and_confirmed_release);
		UT_RUN(recovery_checkpoint_retries_without_reclassifying_its_owner);
		UT_RUN(recovery_checkpoint_rechecks_durable_result_before_projection);
		UT_RUN(recovery_checkpoint_does_not_replace_other_native_purposes);
		UT_DONE();
		return ut_failed_count != 0;
	}
	if (getenv("PGRAC_CF_CONSUMERS_ONLY") != NULL) {
		UT_PLAN(6);
		UT_RUN(startup_configuration_and_selection_retire_before_reobserving);
		UT_RUN(writer_begin_retries_only_its_unfinished_step);
		UT_RUN(startup_checkpoint_retries_durable_install_and_readback_separately);
		UT_RUN(checkpoint_prepare_publish_retire_before_new_attempt);
		UT_RUN(online_checkpoint_wait_yields_to_normal_shutdown);
		UT_RUN(cf_observation_saturation_never_wraps_or_reuses_an_old_receipt);
		UT_DONE();
		return ut_failed_count != 0;
	}
	UT_PLAN(79);
	UT_RUN(recovery_checkpoint_selects_scope_only_for_closed_online_owner);
	UT_RUN(recovery_checkpoint_closed_admission_uses_only_original_scope);
	UT_RUN(recovery_checkpoint_preserves_owner_and_native_guards);
	UT_RUN(recovery_checkpoint_rechecks_read_and_confirmed_release);
	UT_RUN(recovery_checkpoint_retries_without_reclassifying_its_owner);
	UT_RUN(recovery_checkpoint_rechecks_durable_result_before_projection);
	UT_RUN(recovery_checkpoint_does_not_replace_other_native_purposes);

	UT_RUN(native_empty_input_requires_the_original_bound_initializer);
	UT_RUN(shutdown_wait_log_carries_the_remaining_wait_budget);
	UT_RUN(writer_prepare_nonok_with_binding_is_not_retried_or_cleared);
	UT_RUN(retirement_rejects_usable_holds_and_missing_original_cookie);
	UT_RUN(startup_wait_preserves_retirement_when_original_phase_terminates);
	UT_RUN(shutdown_both_consumers_charge_while_original_retirement_remains);
	UT_RUN(wait_diagnostic_once_per_original_operation_not_request);
	UT_RUN(checkpoint_hard_results_and_untyped_lock_never_consume_wait_observation);
	UT_RUN(startup_configuration_and_selection_retire_before_reobserving);
	UT_RUN(writer_begin_retries_only_its_unfinished_step);
	UT_RUN(startup_checkpoint_retries_durable_install_and_readback_separately);
	UT_RUN(checkpoint_prepare_publish_retire_before_new_attempt);
	UT_RUN(online_checkpoint_wait_yields_to_normal_shutdown);
	UT_RUN(cf_observation_saturation_never_wraps_or_reuses_an_old_receipt);
	UT_RUN(clean_input_observation_preserves_exact_old_and_new_owners);
	UT_RUN(clean_input_observation_rejects_other_input_kinds);
	UT_RUN(clean_input_observation_rejects_wrong_owner_phase_and_lock_context);
	UT_RUN(clean_input_observation_rechecks_root_and_native_checkpoint);
	UT_RUN(initializer_selection_waits_for_original_lock_before_native_side_effect);
	UT_RUN(initializer_selection_accepts_recovered_cohort);
	UT_RUN(clean_writer_checkpoint_uses_exact_existing_fence_qualification);
	UT_RUN(clean_checkpoint_cannot_borrow_other_input_epoch_or_writer);
	UT_RUN(shared_crash_control_prepares_without_untyped_publication);
	UT_RUN(shared_crash_control_prepare_refusal_never_publishes);
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
	UT_RUN(prepare_pending_releases_before_normal_and_shutdown_retry);
	UT_RUN(prepare_pending_cannot_hide_release_loss_cancel_or_changed_owner);
	UT_RUN(prepare_refuses_unsupported_or_unproven_input);
	UT_RUN(initialized_writer_checkpoint_uses_exact_existing_fence_qualification);
	UT_RUN(recovered_writer_checkpoint_uses_exact_recovered_fence_qualification);
	UT_RUN(initialized_checkpoint_cannot_borrow_other_input_epoch_or_writer);
	UT_RUN(publish_retries_only_root_competition_before_projection);
	UT_RUN(publish_does_not_retry_safety_or_io_refusal);
	UT_RUN(publish_waits_for_pending_admission_outside_locks);
	UT_RUN(publish_pending_still_refuses_loss_cancel_and_epoch_change);
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
	UT_RUN(startup_checkpoint_accepts_recovered_cohort);
	UT_RUN(startup_prepare_rejects_other_purposes_and_lost_owner);
	UT_RUN(startup_publish_installs_only_actual_root_projection);
	UT_RUN(startup_publish_reobserves_cas_without_holding_locks);
	UT_RUN(startup_publish_refusal_never_installs_in_memory_candidate);
	UT_RUN(writer_begin_routes_only_after_all_target_initializing);
	UT_RUN(writer_begin_accepts_recovered_cohort);
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
