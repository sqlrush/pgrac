/* Original auxiliary caller job lifetime; cleanup never grants input use.
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "cluster/cluster_guc.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "utils/memutils.h"
#include "cluster_wal_inputs_job_private.h"

static ClusterWalInputsJobSlotV1 inputs_jobs[CLUSTER_WAL_INPUT_JOB_COUNT];
static ClusterWalInputsJobProcessV1 inputs_jobs_process;

static bool
inputs_job_caller(uint32 consumer, bool owner_cleanup)
{
	return cluster_shared_config && MyProc != NULL && MyProcPid > 0
		&& MyProc->pid == MyProcPid && MyProc->pgprocno >= 0
		&& MyProc->cluster_grd_generation != 0
		&& AuxProcessResourceOwner != NULL && CurrentResourceOwner == AuxProcessResourceOwner
		/* Aux owner cleanup runs after proc_exit marks the process exiting.
		 * Only that callback may validate retained debt during exit. */
		&& CritSectionCount == 0 && (!proc_exit_inprogress || owner_cleanup)
		&& ((MyBackendType == B_CHECKPOINTER
			 && (consumer == WAL_INPUT_JOB_RETAINED_CUT || consumer == WAL_INPUT_JOB_PI_BATCH))
			|| (MyBackendType == B_BG_WRITER
				&& (consumer == WAL_INPUT_JOB_PI_NOTICE || consumer == WAL_INPUT_JOB_PI_REBUILD)));
}

static bool
inputs_job_parent(MemoryContext parent)
{
	return parent != NULL && parent != TopMemoryContext && parent != ErrorContext
		&& parent != TopTransactionContext && parent != CurTransactionContext
		&& MemoryContextGetParent(parent) == TopMemoryContext;
}

static bool
inputs_job_empty(const ClusterWalInputsJobSlotV1 *slot)
{
	return slot->job == NULL && slot->parent == NULL && slot->context == NULL
		&& slot->generation == 0 && slot->state == 0 && slot->flags == 0 && slot->reserved == 0;
}

static ClusterWalInputsJobSlotV1 *
inputs_job_find(ClusterWalInputsJobV1 *job, bool owner_cleanup)
{
	if (job == NULL)
		return NULL;
	for (uint32 i = 0; i < CLUSTER_WAL_INPUT_JOB_COUNT; ++i) {
		ClusterWalInputsJobSlotV1 *slot = &inputs_jobs[i];

		if (slot->job == job) {
			ClusterCfJobReceiptV1 unbound;

			memset(&unbound, 0, sizeof(unbound));
			unbound.job_generation = slot->generation;

			if (!inputs_job_caller(i, owner_cleanup) || slot->generation == 0
				|| job->generation != slot->generation || job->consumer != i
				|| !inputs_job_parent(slot->parent) || job->context == NULL
				|| job->context != slot->context || slot->reserved != 0
				|| job->state != slot->state || job->flags != slot->flags
				|| MemoryContextGetParent(job->context) != slot->parent
				|| job->owner != CurrentResourceOwner || job->owner_pid != MyProcPid
				|| job->owner_procno != (uint32)MyProc->pgprocno
				|| job->owner_proc_generation != MyProc->cluster_grd_generation
				|| (job->flags & ~WAL_INPUT_JOB_FLAGS) != 0
				|| job->cf.job_generation != job->generation || job->cf.reserved != 0
				|| job->cf.state > CF_JOB_RECEIPT_CONFIRMED)
				return NULL;
			for (unsigned j = 0; j < sizeof(job->reserved); ++j)
				if (job->reserved[j] != 0)
					return NULL;
			/* Input/pin binding is a later slice. Never treat an unsupported
			 * debt as absent or advertise a usable/suspended input scope. */
			if (job->inputs != NULL || job->pin != NULL || job->pin_generation != 0
				|| job->walr_next != 0
				|| (job->state != WAL_INPUT_JOB_STATE_NEW
					&& job->state != WAL_INPUT_JOB_STATE_RETIRING_CF
					&& job->state != WAL_INPUT_JOB_STATE_DONE)
				|| (job->state == WAL_INPUT_JOB_STATE_NEW ? job->flags != 0
					: (job->flags & WAL_INPUT_JOB_FLAG_REVOKED) == 0)
				|| (job->state == WAL_INPUT_JOB_STATE_DONE && job->cf.state == CF_JOB_RECEIPT_RETIRING)
				|| (job->cf.state == CF_JOB_RECEIPT_UNBOUND
					&& memcmp(&job->cf, &unbound, sizeof(unbound)) != 0))
				return NULL;
			return slot;
		}
	}
	return NULL;
}

static void
inputs_job_transition(ClusterWalInputsJobSlotV1 *slot, uint32 state, uint16 flags)
{
	slot->job->state = slot->state = state;
	slot->job->flags = slot->flags = flags;
}

static void
inputs_jobs_resource_release(ResourceReleasePhase phase, bool isCommit,
							 bool isTopLevel, void *arg)
{
	(void)isTopLevel;
	(void)arg;
	if (phase != RESOURCE_RELEASE_BEFORE_LOCKS || AuxProcessResourceOwner == NULL
		|| CurrentResourceOwner != AuxProcessResourceOwner)
		return;
	for (uint32 i = 0; i < CLUSTER_WAL_INPUT_JOB_COUNT; ++i) {
		ClusterWalInputsJobV1 *job = inputs_jobs[i].job;

		if (inputs_job_empty(&inputs_jobs[i]))
			continue;
		if (inputs_job_find(job, true) == NULL)
			elog(FATAL, "invalid original WAL input job during owner cleanup");
		if (job->state == WAL_INPUT_JOB_STATE_DONE)
			continue;
		inputs_job_transition(&inputs_jobs[i], WAL_INPUT_JOB_STATE_RETIRING_CF,
			job->flags | WAL_INPUT_JOB_FLAG_REVOKED | (isCommit ? 0 : WAL_INPUT_JOB_FLAG_ERROR));
		/* Only revoke. The original service loop observes ACK completion;
		 * this callback neither waits nor releases the caller's context. */
	}
}

ClusterControlRootResult
cluster_wal_inputs_job_open_v1(ClusterWalInputsConsumerV1 consumer, MemoryContext caller_lifetime,
							   ClusterWalInputsJobV1 **out)
{
	ClusterWalInputsJobSlotV1 *slot;
	ClusterWalInputsJobV1 *volatile job;
	MemoryContext context;
	uint64 generation;

	if (out == NULL || (uint32)consumer >= CLUSTER_WAL_INPUT_JOB_COUNT
		|| !inputs_job_caller(consumer, false) || !inputs_job_parent(caller_lifetime))
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	/* An invalid other slot must not be hidden by a new allocation or role. */
	for (uint32 i = 0; i < CLUSTER_WAL_INPUT_JOB_COUNT; ++i)
		if (!inputs_job_empty(&inputs_jobs[i]) && inputs_job_find(inputs_jobs[i].job, false) == NULL)
			return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	slot = &inputs_jobs[consumer];
	if (slot->job != NULL) {
		if (slot->parent != caller_lifetime || (*out != NULL && *out != slot->job))
			return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
		*out = slot->job;
		return slot->job->state == WAL_INPUT_JOB_STATE_NEW
			&& slot->job->cf.state != CF_JOB_RECEIPT_RETIRING
			? CLUSTER_CONTROL_ROOT_OK_PRIMARY : CLUSTER_CONTROL_ROOT_RECONFIG_WAIT;
	}
	if (*out != NULL)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	if (inputs_jobs_process.generation == UINT64_MAX)
		return CLUSTER_CONTROL_ROOT_RECONFIG_WAIT;
	if (!inputs_jobs_process.callback_registered) {
		RegisterResourceReleaseCallback(inputs_jobs_resource_release, NULL);
		inputs_jobs_process.callback_registered = true;
	}
	generation = ++inputs_jobs_process.generation;
	context = AllocSetContextCreate(caller_lifetime, "WAL input caller job", ALLOCSET_DEFAULT_SIZES);
	PG_TRY();
	{
		job = MemoryContextAllocZero(context, sizeof(*job));
	}
	PG_CATCH();
	{
		MemoryContextDelete(context);
		PG_RE_THROW();
	}
	PG_END_TRY();
	job->context = context;
	job->owner = CurrentResourceOwner;
	job->consumer = consumer;
	job->generation = job->cf.job_generation = generation;
	job->owner_pid = MyProcPid;
	job->owner_procno = (uint32)MyProc->pgprocno;
	job->owner_proc_generation = MyProc->cluster_grd_generation;
	job->original_result = UINT32_MAX;
	slot->parent = caller_lifetime;
	slot->generation = generation;
	slot->context = context;
	slot->job = job;
	*out = job;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

MemoryContext
cluster_wal_inputs_job_context_v1(ClusterWalInputsJobV1 *job)
{
	return inputs_job_find(job, false) != NULL && job->state == WAL_INPUT_JOB_STATE_NEW
		&& job->cf.state != CF_JOB_RECEIPT_RETIRING ? job->context : NULL;
}

ClusterWalInputsJobResultV1
cluster_wal_inputs_job_poll_v1(ClusterWalInputsJobV1 *job)
{
	ClusterCfJobResultV1 result;
	ClusterWalInputsJobSlotV1 *slot = inputs_job_find(job, false);

	if (slot == NULL)
		return WAL_INPUT_JOB_INVALID;
	if (job->state == WAL_INPUT_JOB_STATE_DONE)
		return WAL_INPUT_JOB_DONE;
	if (job->state != WAL_INPUT_JOB_STATE_RETIRING_CF)
		return WAL_INPUT_JOB_INVALID;
	if (job->cf.state != CF_JOB_RECEIPT_UNBOUND) {
		result = cluster_cf_job_retirement_poll_v1(&job->cf);
		if (result != CF_JOB_CONFIRMED)
			return result == CF_JOB_PENDING ? WAL_INPUT_JOB_PENDING : WAL_INPUT_JOB_INVALID;
	}
	inputs_job_transition(slot, WAL_INPUT_JOB_STATE_DONE, job->flags);
	return WAL_INPUT_JOB_DONE;
}

ClusterWalInputsJobResultV1
cluster_wal_inputs_job_retire_v1(ClusterWalInputsJobV1 *job)
{
	ClusterWalInputsJobSlotV1 *slot = inputs_job_find(job, false);

	if (slot == NULL)
		return WAL_INPUT_JOB_INVALID;
	if (job->state == WAL_INPUT_JOB_STATE_NEW) {
		inputs_job_transition(slot, WAL_INPUT_JOB_STATE_RETIRING_CF,
			job->flags | WAL_INPUT_JOB_FLAG_REVOKED);
	}
	return cluster_wal_inputs_job_poll_v1(job);
}

ClusterWalInputsJobResultV1
cluster_wal_inputs_job_finish_v1(ClusterWalInputsJobV1 **out)
{
	ClusterWalInputsJobSlotV1 *slot;
	ClusterWalInputsJobV1 *job;
	MemoryContext context;

	if (out == NULL || (slot = inputs_job_find(*out, false)) == NULL)
		return WAL_INPUT_JOB_INVALID;
	job = *out;
	if (job->state != WAL_INPUT_JOB_STATE_DONE)
		return WAL_INPUT_JOB_PENDING;
	context = job->context;
	/* Clear both stable locators before deleting any borrowed allocation. */
	memset(slot, 0, sizeof(*slot));
	*out = NULL;
	explicit_bzero(job, sizeof(*job));
	MemoryContextDelete(context);
	return WAL_INPUT_JOB_DONE;
}

ClusterWalInputsJobResultV1
cluster_wal_inputs_jobs_stop_census_v1(void)
{
	bool pending = false;

	for (uint32 i = 0; i < CLUSTER_WAL_INPUT_JOB_COUNT; ++i) {
		ClusterWalInputsJobV1 *job = inputs_jobs[i].job;

		if (inputs_job_empty(&inputs_jobs[i]))
			continue;
		if (inputs_job_find(job, false) == NULL)
			return WAL_INPUT_JOB_INVALID;
		if (job->state == WAL_INPUT_JOB_STATE_RETIRING_CF || job->cf.state == CF_JOB_RECEIPT_RETIRING)
			pending = true;
	}
	return pending ? WAL_INPUT_JOB_PENDING : WAL_INPUT_JOB_DONE;
}

void
cluster_wal_inputs_jobs_service_poll_v1(void)
{
	if (cluster_wal_inputs_jobs_stop_census_v1() == WAL_INPUT_JOB_INVALID)
		elog(FATAL, "invalid original WAL input job during service cleanup");
	for (uint32 i = 0; i < CLUSTER_WAL_INPUT_JOB_COUNT; ++i)
		if (inputs_jobs[i].job != NULL
			&& inputs_jobs[i].job->state == WAL_INPUT_JOB_STATE_RETIRING_CF
			&& cluster_wal_inputs_job_poll_v1(inputs_jobs[i].job) == WAL_INPUT_JOB_INVALID)
			elog(FATAL, "could not confirm original WAL input job cleanup");
}

#endif
