/* Opt-in cassert-only scheduling control, local to one test postmaster. */
#include "postgres.h"
#if defined(USE_PGRAC_CLUSTER) && defined(USE_ASSERT_CHECKING)
#include "fmgr.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_runtime_visibility.h"
#include "portability/instr_time.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/wait_event.h"
#include "test_pgrac_m2_pause.h"

PG_FUNCTION_INFO_V1(test_pgrac_m2_pause);
static TestM2PauseState *pause_state;
static shmem_request_hook_type previous_request;
static shmem_startup_hook_type previous_startup;
static uint64 local_lease;
static bool exit_registered;

static uint32
pause_caller(const char *caller)
{
	if (strcmp(caller, "freeze_data") == 0)
		return 1;
	if (strcmp(caller, "sample_canonical") == 0)
		return 2;
	return 0;
}

static void
pause_exit(int code pg_attribute_unused(), Datum arg pg_attribute_unused())
{
	bool consumed;

	if (pause_state != NULL && local_lease != 0)
		(void)test_m2_pause_clear(pause_state, local_lease, &consumed);
	local_lease = 0;
}

static void
pause_own(uint64 lease)
{
	if (!exit_registered) {
		before_shmem_exit(pause_exit, (Datum)0);
		exit_registered = true;
	}
	local_lease = lease;
}

static void
pause_copy(const char *caller, TransactionId xid)
{
	uint64 lease;
	uint32 duration;
	uint32 previous_wait;
	instr_time start;

	if (pause_state == NULL || (MyBackendType != B_LMS && MyBackendType != B_LMS_WORKER))
		return;
	duration = test_m2_pause_claim(pause_state, cluster_node_id, pause_caller(caller), xid, &lease);
	if (duration == 0)
		return;
	pause_own(lease);
	write_stderr("M2_TEST_PAUSE node=%d caller=%s xid=%u pid=%d lease=" UINT64_FORMAT "\n",
				 cluster_node_id, caller, xid, MyProcPid, lease);
	previous_wait = *my_wait_event_info;
	INSTR_TIME_SET_CURRENT(start);
	PG_TRY();
	{
		pgstat_report_wait_start(WAIT_EVENT_PG_SLEEP);
		for (;;) {
			instr_time now;
			uint64 elapsed;

			INSTR_TIME_SET_CURRENT(now);
			INSTR_TIME_SUBTRACT(now, start);
			elapsed = INSTR_TIME_GET_MICROSEC(now);
			if (elapsed >= duration)
				break;
			if (CritSectionCount == 0)
				CHECK_FOR_INTERRUPTS();
			pg_usleep((long)Min(duration - elapsed, UINT64CONST(10000)));
		}
	}
	PG_FINALLY();
	{
		pgstat_report_wait_start(previous_wait);
	}
	PG_END_TRY();
}

static void
pause_request(void)
{
	if (previous_request != NULL)
		previous_request();
	RequestAddinShmemSpace(MAXALIGN(sizeof(TestM2PauseState)));
}

static void
pause_startup(void)
{
	bool found;

	if (previous_startup != NULL)
		previous_startup();
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	pause_state = ShmemInitStruct("test_pgrac_m2_pause", sizeof(TestM2PauseState), &found);
	if (!found)
		test_m2_pause_state_init(pause_state);
	LWLockRelease(AddinShmemInitLock);
}

void
test_pgrac_m2_pause_init(void)
{
	previous_request = shmem_request_hook;
	shmem_request_hook = pause_request;
	previous_startup = shmem_startup_hook;
	shmem_startup_hook = pause_startup;
	cluster_runtime_visibility_test_copy_hook = pause_copy;
}

Datum
test_pgrac_m2_pause(PG_FUNCTION_ARGS)
{
	int node = PG_GETARG_INT32(0);
	char *caller = text_to_cstring(PG_GETARG_TEXT_PP(1));
	TransactionId xid = PG_GETARG_TRANSACTIONID(2);
	int duration = PG_GETARG_INT32(3);
	uint32 kind = pause_caller(caller);
	uint64 lease;
	bool consumed = false;
	bool cleared;

	if (!superuser())
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("superuser required")));
	if (pause_state == NULL)
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("test pause requires preloaded cassert test module")));
	if (node != cluster_node_id || node < 0 || node > 127 || kind == 0 || xid < 3 || duration < 0
		|| duration > 60000000)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid test pause target")));
	if (duration == 0) {
		lease = local_lease;
		cleared = test_m2_pause_clear(pause_state, lease, &consumed);
		local_lease = 0;
		PG_RETURN_TEXT_P(cstring_to_text(
			psprintf("{\"disarmed\":%s,\"consumed\":%s,\"lease\":\"" UINT64_FORMAT "\"}",
					 cleared ? "true" : "false", consumed ? "true" : "false", lease)));
	}
	if (local_lease != 0)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_IN_USE), errmsg("disarm previous test pause first")));
	/* Register exit cleanup before publishing; no interrupt point intervenes. */
	pause_own(0);
	lease = test_m2_pause_arm(pause_state, node, kind, xid, duration);
	if (lease == 0)
		ereport(ERROR, (errcode(ERRCODE_OBJECT_IN_USE), errmsg("test pause slot unavailable")));
	local_lease = lease;
	PG_RETURN_TEXT_P(cstring_to_text(psprintf(
		"{\"armed\":true,\"node\":%d,\"caller\":\"%s\",\"xid\":%u,\"lease\":\"" UINT64_FORMAT
		"\",\"owner_pid\":%d}",
		node, caller, xid, lease, MyProcPid)));
}
#endif
