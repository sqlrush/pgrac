/*-------------------------------------------------------------------------
 *
 * cluster_shared_config_delivery.c
 *    Ephemeral native postmaster-family configuration delivery.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/backend/cluster/cluster_shared_config_delivery.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#ifndef WIN32
#include <sys/mman.h>
#endif
#include "access/xact.h"
#include "miscadmin.h"
#include "postmaster/interrupt.h"
#include "portability/mem.h"
#include "storage/ipc.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "cluster/cluster_clean_leave.h"
#include "cluster/cluster_cr_server.h"
#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_semantic_activation.h"
#include "cluster/cluster_shared_config.h"
#include "cluster/cluster_terminal_ref_census.h"

/* Not main shmem: the native logger deliberately detaches that mapping. This
 * unnamed carrier dies with the last native family process. Neither its bytes
 * nor a signal establishes a root, membership or node-application authority.
 * No child-owned lock can block the postmaster.
 */
typedef struct ConfigDeliveryFamily {
	int32 postmaster_pid;
	pg_atomic_uint64 generation;
	pg_atomic_uint32 lmon_pid;
	pg_atomic_uint32 logger_pid;
	ClusterSharedConfigDeliverySlot incoming;
	ClusterSharedConfigDeliverySlot accepted;
	ClusterSharedConfigSlot logger;
} ConfigDeliveryFamily;

static ConfigDeliveryFamily *delivery_family;
static uint64 delivery_generation;
/* Scheduling only, never a retained target. Native fork starts from a parent
 * which owns no transaction and therefore never sets this indication. */
static bool delivery_waiting_for_idle;
/* A utility or maintenance command may own work across native transactions.
 * This local nesting count is not a target, node ACK or DATA permission. */
static uint32 delivery_work_depth;

static void
delivery_release_owner(ResourceOwner owner, ResourceOwner saved, bool success)
{
	/* PM/logger have no transaction owner. OpenSSL-backed validation still
	 * needs a real scoped owner, just as native startup/reload application does. */
	ResourceOwnerRelease(owner, RESOURCE_RELEASE_BEFORE_LOCKS, success, saved == NULL);
	ResourceOwnerRelease(owner, RESOURCE_RELEASE_LOCKS, success, saved == NULL);
	ResourceOwnerRelease(owner, RESOURCE_RELEASE_AFTER_LOCKS, success, saved == NULL);
	CurrentResourceOwner = saved;
	ResourceOwnerDelete(owner);
}

static bool
delivery_is_parent(void)
{
	return IsPostmasterEnvironment && !IsUnderPostmaster && MyProcPid > 0
		   && MyProcPid == PostmasterPid;
}

static bool
delivery_is_family(void)
{
	return delivery_family != NULL && IsPostmasterEnvironment
		   && delivery_family->postmaster_pid == PostmasterPid;
}

bool
cluster_shared_config_delivery_work_enter(void)
{
	if (!IsUnderPostmaster || !delivery_is_family())
		return false;
	if (delivery_work_depth == PG_UINT32_MAX)
		ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
						errmsg("configuration work nesting limit exceeded")));
	++delivery_work_depth;
	return true;
}

void
cluster_shared_config_delivery_work_leave(bool entered)
{
	if (entered) {
		Assert(delivery_work_depth > 0);
		--delivery_work_depth;
	}
	/* PG_FINALLY can run during ERROR cleanup. Do not assign, allocate, wait,
	 * or invoke hooks here. The next actual idle boundary owns the retry. */
}

/* Returning to a service loop is not completion of its asynchronous work.
 * Observe the original owners, without sealing, parking or cancelling them.
 * This is only a local deferral condition, not a cluster drain certificate:
 * shared producers and transport obligations require their separate cut.
 * Author: SqlRush <sqlrush@gmail.com>
 */
static bool
delivery_has_owned_work(void)
{
	int slot;
	const char *reason;

	if (delivery_work_depth != 0 || IsTransactionState() || IsTransactionOrTransactionBlock()
		|| cluster_semantic_activation_backend_has_admission())
		return true;
	if ((AmLmonProcess() || AmLmsProcess() || AmLmsWorkerProcess())
		&& cluster_gcs_block_normal_stop_local_poll(&slot, &reason) != CLUSTER_NORMAL_STOP_READY)
		return true;
	if (AmLmsProcess() && !cluster_cr_server_r4_worker0_drained())
		return true;
	if (AmUndoCleanerProcess() && !cluster_ctrc_cleaner_local_idle())
		return true;
	return false;
}

/* Main-shmem children cannot cross its native reset. The detached logger is
 * the exception: its actual lifetime spans that reset, but never proves DATA
 * convergence. A generation change during a copy is a refused observation.
 */
static bool
delivery_current(uint64 *generation)
{
	if (!delivery_is_family())
		return false;
	*generation = pg_atomic_read_u64(&delivery_family->generation);
	return *generation != 0 && (*generation == delivery_generation || MyBackendType == B_LOGGER);
}

void
cluster_shared_config_delivery_start(void)
{
	if (!delivery_is_parent())
		ereport(FATAL, (errmsg("configuration delivery requires the native postmaster")));
	if (delivery_family != NULL)
		return;
#ifdef EXEC_BACKEND
	ereport(FATAL, (errmsg("shared configuration delivery requires a fork-based platform")));
#else
	delivery_family
		= mmap(NULL, sizeof(*delivery_family), PROT_READ | PROT_WRITE, PG_MMAP_FLAGS, -1, 0);
	if (delivery_family == MAP_FAILED) {
		delivery_family = NULL;
		ereport(FATAL, (errmsg("could not map native configuration delivery: %m")));
	}
	delivery_family->postmaster_pid = MyProcPid;
	delivery_generation = 1;
	pg_atomic_init_u64(&delivery_family->generation, delivery_generation);
	pg_atomic_init_u32(&delivery_family->lmon_pid, 0);
	pg_atomic_init_u32(&delivery_family->logger_pid, 0);
	cluster_shared_config_delivery_slot_init(&delivery_family->incoming);
	cluster_shared_config_delivery_slot_init(&delivery_family->accepted);
	cluster_shared_config_registration_init(&delivery_family->logger);
	if (!cluster_shared_config_delivery_parent_publish())
		ereport(FATAL, (errmsg("configuration delivery lacks native startup defaults")));
#endif
}

void
cluster_shared_config_delivery_new_shmem(void)
{
	uint64 generation;
	if (delivery_family == NULL)
		return;
	if (!delivery_is_parent() || !delivery_is_family())
		ereport(FATAL, (errmsg("invalid configuration delivery reset owner")));
	generation = pg_atomic_read_u64(&delivery_family->generation);
	if (generation == PG_UINT64_MAX)
		ereport(FATAL, (errmsg("configuration delivery lifetime exhausted")));
	/* Native InitProcGlobal runs only after old main-shmem children have left.
	 * No old LMON can still write incoming. Do not reset the live logger or the
	 * parent's accepted sequence, which the detached logger can still read. */
	cluster_shared_config_delivery_slot_init(&delivery_family->incoming);
	pg_atomic_write_u32(&delivery_family->lmon_pid, 0);
	delivery_generation = generation + 1;
	pg_atomic_write_u64(&delivery_family->generation, delivery_generation);
	if (!cluster_shared_config_delivery_parent_publish())
		ereport(FATAL, (errmsg("configuration delivery cannot retain native defaults")));
}

void
cluster_shared_config_delivery_lmon_started(int32 pid)
{
	if (delivery_family == NULL)
		return;
	if (!delivery_is_parent() || !delivery_is_family() || pid <= 0
		|| pg_atomic_read_u32(&delivery_family->lmon_pid) != 0)
		ereport(FATAL, (errmsg("invalid configuration LMON fork registration")));
	/* A child running before this store simply retries at its next tick. */
	pg_atomic_write_u32(&delivery_family->lmon_pid, (uint32)pid);
}

void
cluster_shared_config_delivery_lmon_reaped(int32 pid)
{
	if (delivery_family == NULL)
		return;
	if (!delivery_is_parent() || !delivery_is_family() || pid <= 0
		|| pg_atomic_read_u32(&delivery_family->lmon_pid) != (uint32)pid)
		ereport(FATAL, (errmsg("invalid configuration LMON reap registration")));
	/* Actual waitpid, not mere SIGTERM or age, permits writer replacement. */
	pg_atomic_write_u32(&delivery_family->lmon_pid, 0);
	if (!cluster_shared_config_delivery_slot_retire(&delivery_family->incoming, pid))
		ereport(LOG, (errmsg("configuration incoming lifetime could not be retired")));
}

void
cluster_shared_config_delivery_logger_started(int32 pid)
{
	if (delivery_family == NULL)
		return;
	if (!delivery_is_parent() || !delivery_is_family() || pid <= 0
		|| pg_atomic_read_u32(&delivery_family->logger_pid) != 0)
		ereport(FATAL, (errmsg("invalid configuration logger fork registration")));
	/* The child may attach before this store. Observers require both sides. */
	pg_atomic_write_u32(&delivery_family->logger_pid, (uint32)pid);
}

void
cluster_shared_config_delivery_logger_reaped(int32 pid)
{
	ClusterSharedConfigSlot *slot;
	uint64 sequence;
	if (delivery_family == NULL)
		return;
	if (!delivery_is_parent() || !delivery_is_family() || pid <= 0
		|| pg_atomic_read_u32(&delivery_family->logger_pid) != (uint32)pid)
		ereport(FATAL, (errmsg("invalid configuration logger reap registration")));
	/* Actual waitpid has proven this writer dead. This alone permits retiring
	 * even an interrupted odd copy; age or main-shmem reset never does. Keep
	 * sequence monotonic so PID reuse cannot recycle an old registration. */
	pg_atomic_write_u32(&delivery_family->logger_pid, 0);
	slot = &delivery_family->logger;
	sequence = pg_atomic_read_u64(&slot->sequence);
	if (sequence > PG_UINT64_MAX - 2) {
		pg_atomic_write_u64(&slot->sequence, PG_UINT64_MAX);
		return;
	}
	pg_atomic_write_u64(&slot->sequence, sequence | UINT64CONST(1));
	pg_write_barrier();
	memset(&slot->value, 0, sizeof(slot->value));
	pg_write_barrier();
	pg_atomic_write_u64(&slot->sequence, (sequence | UINT64CONST(1)) + 1);
}

static void
delivery_logger_exit(int code, Datum arg)
{
	(void)code;
	(void)arg;
	cluster_shared_config_process_detach();
}

void
cluster_shared_config_delivery_logger_attach(void)
{
	if (delivery_family == NULL)
		return;
	if (!delivery_is_family() || !IsUnderPostmaster || MyBackendType != B_LOGGER
		|| !cluster_shared_config_process_attach(&delivery_family->logger))
		ereport(FATAL, (errmsg("could not enroll native configuration logger")));
	on_proc_exit(delivery_logger_exit, (Datum)0);
}

bool
cluster_shared_config_delivery_logger_observe(ClusterSharedConfigRegistration *out)
{
	uint64 sequence;
	return cluster_shared_config_delivery_logger_snapshot(out, &sequence);
}

bool
cluster_shared_config_delivery_logger_snapshot(ClusterSharedConfigRegistration *out,
											   uint64 *sequence)
{
	ClusterSharedConfigRegistration value;
	uint32 pid;
	uint64 before;
	if (out == NULL || sequence == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	*sequence = 0;
	if (!delivery_is_family())
		return false;
	pid = pg_atomic_read_u32(&delivery_family->logger_pid);
	before = pg_atomic_read_u64(&delivery_family->logger.sequence);
	if (pid == 0 || (before & 1)
		|| !cluster_shared_config_registration_read(&delivery_family->logger, &value)
		|| value.pid != (int32)pid || value.role != B_LOGGER || !value.observed
		|| pg_atomic_read_u64(&delivery_family->logger.sequence) != before
		|| pg_atomic_read_u32(&delivery_family->logger_pid) != pid)
		return false;
	*out = value;
	*sequence = before;
	return true;
}

bool
cluster_shared_config_delivery_publish(const ClusterSharedConfigRef *ref,
									   const ClusterSharedConfigImage *image)
{
	ResourceOwner saved = CurrentResourceOwner;
	ResourceOwner owner;
	MemoryContext caller = CurrentMemoryContext;
	uint64 generation;
	bool result;
	if (!IsUnderPostmaster || MyBackendType != B_LMON || image == NULL
		|| !delivery_current(&generation)
		|| pg_atomic_read_u32(&delivery_family->lmon_pid) != (uint32)MyProcPid)
		return false;
	owner = ResourceOwnerCreate(saved, "configuration incoming validation");
	CurrentResourceOwner = owner;
	PG_TRY();
	{
		result = cluster_shared_config_delivery_slot_write(
			&delivery_family->incoming, generation, MyProcPid, ref, image->bytes, image->len);
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(caller);
		delivery_release_owner(owner, saved, false);
		PG_RE_THROW();
	}
	PG_END_TRY();
	delivery_release_owner(owner, saved, result);
	return result;
}

bool
cluster_shared_config_delivery_parent_publish(void)
{
	ClusterSharedConfigProcess process;
	ClusterSharedConfigImage image;
	ResourceOwner saved = CurrentResourceOwner;
	ResourceOwner owner;
	MemoryContext caller, context;
	uint64 generation;
	bool result;
	if (!delivery_is_parent() || !delivery_current(&generation))
		return false;
	/* Obtain the parent's real retained bytes, not a caller-supplied target. */
	context = AllocSetContextCreate(CurrentMemoryContext, "native configuration accepted",
									ALLOCSET_DEFAULT_SIZES);
	caller = MemoryContextSwitchTo(context);
	owner = ResourceOwnerCreate(saved, "configuration accepted validation");
	CurrentResourceOwner = owner;
	PG_TRY();
	{
		result = cluster_shared_config_process_copy(&process, &image)
				 && cluster_shared_config_delivery_slot_write(&delivery_family->accepted,
															  generation, MyProcPid, &process.ref,
															  image.bytes, image.len);
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(caller);
		delivery_release_owner(owner, saved, false);
		MemoryContextDelete(context);
		PG_RE_THROW();
	}
	PG_END_TRY();
	MemoryContextSwitchTo(caller);
	delivery_release_owner(owner, saved, result);
	MemoryContextDelete(context);
	return result;
}

bool
cluster_shared_config_delivery_reload(void)
{
	MemoryContext caller, context;
	ResourceOwner saved = CurrentResourceOwner;
	ResourceOwner owner;
	ClusterSharedConfigDeliverySlot *slot;
	ClusterSharedConfigRef ref;
	ClusterSharedConfigImage image;
	ClusterSharedConfigProcess process;
	ClusterSharedConfigPolicyReport report;
	uint64 generation;
	volatile bool result = false;
	if (!delivery_current(&generation))
		return false;
	slot = delivery_is_parent() ? &delivery_family->incoming : &delivery_family->accepted;
	context = AllocSetContextCreate(CurrentMemoryContext, "native configuration delivery",
									ALLOCSET_DEFAULT_SIZES);
	caller = MemoryContextSwitchTo(context);
	owner = ResourceOwnerCreate(saved, "configuration delivery validation");
	CurrentResourceOwner = owner;
	PG_TRY();
	{
		if (cluster_shared_config_delivery_slot_read(slot, generation, &ref, &image)
			&& pg_atomic_read_u64(&delivery_family->generation) == generation) {
			bool can_apply = true;
			if (IsUnderPostmaster && delivery_has_owned_work()) {
				bool needs_idle;
				can_apply = cluster_shared_config_process_reload_needs_idle(
								image.bytes, image.len, &ref, &needs_idle, &report)
							== CLUSTER_CONTROL_ROOT_OK_PRIMARY;
				if (can_apply && needs_idle) {
					/* No waiting inside this callback, and no half application.
					 * COMMIT/ROLLBACK must run with their old native values. The
					 * next normal command/service loop retries the then-current
					 * accepted image, not a pointer to this delayed generation.
					 * Do not signal a latch here and spin an active transaction. */
					ConfigReloadPending = true;
					delivery_waiting_for_idle = true;
					can_apply = false;
				}
			}
			if (can_apply)
				result = cluster_shared_config_process_reload(image.bytes, image.len, &ref,
															  &process, &report)
						 == CLUSTER_CONTROL_ROOT_OK_PRIMARY;
			if (result && delivery_is_parent())
				result = cluster_shared_config_delivery_parent_publish();
		}
	}
	PG_CATCH();
	{
		/* A native hook may have changed process state before ERROR. The real
		 * consumer has already marked it failed; never fabricate rollback or
		 * publish the new parent target. Do not let an ERROR kill the PM. */
		MemoryContextSwitchTo(caller);
		FlushErrorState();
		result = false;
		ereport(LOG,
				(errmsg("native configuration delivery failed; application remains unproven")));
	}
	PG_END_TRY();
	MemoryContextSwitchTo(caller);
	delivery_release_owner(owner, saved, result);
	MemoryContextDelete(context);
	if (result)
		delivery_waiting_for_idle = false;
	return result;
}

/* Native COMMIT/ROLLBACK AND CHAIN and a multi-statement simple-query message
 * can start the next transaction without the frontend's message-level reload
 * check. Retry before TRANS_START, not from fallible transaction cleanup.
 * An unavailable family slot retains retry ownership. Unrelated native
 * reload indications are not consumed here. Author: SqlRush <sqlrush@gmail.com>
 */
void
cluster_shared_config_delivery_retry_idle(void)
{
	if (!delivery_waiting_for_idle || !IsUnderPostmaster || delivery_has_owned_work())
		return;
	/* Prevent recursive native hooks from entering this retry again. */
	delivery_waiting_for_idle = false;
	if (!cluster_shared_config_delivery_reload())
		delivery_waiting_for_idle = true;
}
