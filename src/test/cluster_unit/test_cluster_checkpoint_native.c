/* Author: SqlRush <sqlrush@gmail.com> */
/* Actual native adapter; root/filesystem behavior is separately tested by
 * test_cluster_control_root. Only runtime/lock/root-return facts are replaced. */
#include "postgres.h"
#include <time.h>
#include "access/xlog.h"
#include "catalog/pg_control.h"
#include "cluster/cluster_cf_authority.h"
#include "cluster/cluster_cf_enqueue.h"
#include "cluster/cluster_wal_thread.h"
#include "cluster/cluster_write_fence.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_reconfig.h"
#include "cluster/cluster_startup_phase.h"
#include "cluster/cluster_external_fence.h"
#include "../../backend/cluster/cluster_control_root_private.h"
#include "miscadmin.h"
#include "storage/latch.h"
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
static unsigned native_writes;
static ClusterControlRootResult returns[4];

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
ResetLatch(Latch *l)
{
	UT_ASSERT(l == MyLatch);
}
int
WaitLatch(Latch *l, int e, long t, uint32 event)
{
	UT_ASSERT(l == MyLatch && (e & WL_EXIT_ON_PM_DEATH) && t > 0);
	UT_ASSERT_EQ(event, WAIT_EVENT_CHECKPOINTER_MAIN);
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
cluster_control_root_v2_checkpoint_publish(const ClusterControlRootIdentity *self,
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

#include "test_cluster_checkpoint_native.inc"

static void
UpdateControlFile(void)
{
	native_writes++;
}
/* Only the native lock boundary is substituted. Actual candidate assignments,
 * CRC and old-writer dispatch below come from CreateCheckPoint. */
#define SpinLockAcquire(lock) ((void)(lock))
#define SpinLockRelease(lock) ((void)(lock))
static ControlFileData
native_candidate(bool shutdown)
{
	ControlFileData *checkpoint_control = ControlFile, v2_checkpoint = { 0 };
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
	PG_exception_stack = NULL;
	error_context_stack = NULL;
	MyAuxProcType = CheckpointerProcess;
	root_calls = waits = local_updates = reads = releases = 0;
	native_writes = 0;
	cluster_shared_config = cluster_enabled = cluster_controlfile_shared_authority = true;
	memset(returns, 0, sizeof(returns));
}
static bool
prepare(int flags)
{
	if (sigsetjmp(error_boundary, 1))
		return false;
	ClusterCheckpointV2Prepare(flags, &candidate);
	return true;
}
static bool
publish(void)
{
	if (sigsetjmp(error_boundary, 1))
		return false;
	ClusterCheckpointV2Publish(&candidate, 220);
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
int
main(void)
{
	UT_PLAN(8);
	UT_RUN(prepare_uses_short_owned_read);
	UT_RUN(prepare_refuses_unsupported_or_unproven_input);
	UT_RUN(publish_retries_only_root_competition_before_projection);
	UT_RUN(publish_does_not_retry_safety_or_io_refusal);
	UT_RUN(publish_cancel_and_changed_authority_stop_owned_retry);
	UT_RUN(publish_installs_root_selected_common_fields);
	UT_RUN(native_candidate_is_private_until_publication);
	UT_RUN(native_legacy_candidate_keeps_control_write);
	UT_DONE();
}
