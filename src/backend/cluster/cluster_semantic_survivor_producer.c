/*-------------------------------------------------------------------------
 * cluster_semantic_survivor_producer.c
 *    Read-only dispatch to the actual original caller's observations.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *------------------------------------------------------------------------- */
#include "postgres.h"
#include "miscadmin.h"
#include "storage/proc.h"
#include "storage/lwlock.h"
#include "storage/ipc.h"
#include "utils/resowner.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_semantic_survivor_producer.h"

static bool
survivor_producer_family(ClusterSurvivorProducerOwner owner)
{
 switch (owner) {
 case CLUSTER_SURVIVOR_PRODUCER_BACKEND:
  return MyBackendType == B_BACKEND;
 case CLUSTER_SURVIVOR_PRODUCER_NATIVE:
  return (MyBackendType == B_LMON && AmLmonProcess())
   || (MyBackendType == B_LMS && AmLmsProcess())
   || (MyBackendType == B_LMS_WORKER && AmLmsWorkerProcess())
   || (MyBackendType == B_LMD && AmLmdProcess())
   || (MyBackendType == B_SINVAL_BCAST && AmSinvalBcastProcess());
 case CLUSTER_SURVIVOR_PRODUCER_INPUT_JOBS:
  return (MyBackendType == B_CHECKPOINTER && AmCheckpointerProcess())
   || (MyBackendType == B_BG_WRITER && AmBackgroundWriterProcess());
 }
 return false;
}
static void
survivor_producer_lock(LWLock *lock, LWLockMode mode, void *held)
{
 (void)lock; (void)mode;
 *(bool *)held = true;
}
static bool
survivor_producer_context(ClusterSurvivorProducerOwner owner)
{
 bool held = false;
 if (!IsUnderPostmaster || !cluster_enabled || !cluster_shared_config || proc_exit_inprogress
  || MyProc == NULL || MyProcPid <= 0 || MyProc->pid != MyProcPid
  || MyProc->pgprocno < 0 || MyProc->cluster_grd_generation == 0
  || MyProc->cluster_grd_generation == UINT64_MAX || CritSectionCount != 0
  || !survivor_producer_family(owner))
  return false;
 if (owner == CLUSTER_SURVIVOR_PRODUCER_BACKEND
  && (MyBackendId == InvalidBackendId
   || MyProc->backendId != MyBackendId))
  return false;
 if (owner == CLUSTER_SURVIVOR_PRODUCER_INPUT_JOBS
  && (CurrentResourceOwner == NULL
   || CurrentResourceOwner != AuxProcessResourceOwner))
  return false;
 ForEachLWLockHeldByMe(survivor_producer_lock, &held);
 return !held;
}
static ClusterSemanticAdmissionObservationResult
survivor_producer_unknown(ClusterSurvivorProducerObservation *out,
 ClusterSemanticAdmissionObservationResult result, const char *reason)
{
 memset(out, 0, sizeof(*out));
 out->reason = reason;
 return result;
}

ClusterSemanticAdmissionObservationResult
cluster_semantic_survivor_producer_poll(ClusterSurvivorProducerOwner owner,
 ClusterSurvivorProducerScope scope, ClusterSurvivorProducerObservation *out)
{
 ClusterSemanticAdmissionObservationResult result;
 PGPROC *proc;
 BackendType backend;
 AuxProcType aux;
 ResourceOwner resource;
 int pid, pgprocno, backend_id;
 uint64 generation;
 const char *reason;

 if (out == NULL)
  return CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_REFUSED;
 memset(out, 0, sizeof(*out));
 out->reason = "SURVIVOR_PRODUCER_SCOPE_OR_OWNER_INVALID";
 if (scope.native_epoch == 0 || scope.native_epoch == UINT64_MAX
  || scope.gate_epoch == 0 || scope.gate_epoch > scope.native_epoch
  || scope.record_generation == 0 || scope.record_generation == UINT64_MAX
  || !survivor_producer_context(owner))
  return CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_REFUSED;
 if (cluster_epoch_get_current() != scope.native_epoch)
  return survivor_producer_unknown(out, CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_STALE,
   "SURVIVOR_PRODUCER_EPOCH_CHANGED");
 proc = MyProc; backend = MyBackendType; aux = MyAuxProcType;
 resource = owner == CLUSTER_SURVIVOR_PRODUCER_NATIVE ? NULL : CurrentResourceOwner;
 pid = MyProcPid; pgprocno = proc->pgprocno; backend_id = proc->backendId;
 generation = proc->cluster_grd_generation;
 switch (owner) {
 case CLUSTER_SURVIVOR_PRODUCER_BACKEND:
  result = cluster_service_backend_collect_return(scope.native_epoch, scope.gate_epoch,
   scope.record_generation, &out->sample.backend);
  reason = out->sample.backend.reason;
  break;
 case CLUSTER_SURVIVOR_PRODUCER_NATIVE:
  result = cluster_service_producer_collect_return(scope.native_epoch, scope.gate_epoch,
   scope.record_generation, &out->sample.native);
  reason = out->sample.native.reason;
  break;
 case CLUSTER_SURVIVOR_PRODUCER_INPUT_JOBS: {
  ClusterWalInputsJobsObservationResultV1 jobs
   = cluster_wal_inputs_jobs_service_observe_v1(scope.native_epoch, &out->sample.inputs);
  result = jobs == WAL_INPUT_JOBS_OBSERVATION_PENDING
   ? CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_PENDING
   : jobs == WAL_INPUT_JOBS_OBSERVATION_STALE
	? CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_STALE
	: CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_REFUSED;
  reason = out->sample.inputs.reason;
  break;
 }
 default:
  return CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_REFUSED;
 }
 if (result != CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_PENDING)
  return survivor_producer_unknown(out,
   result == CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_STALE
	? result : CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_REFUSED,
   reason != NULL ? reason : "SURVIVOR_PRODUCER_SAMPLE_INVALID");
 pg_read_barrier();
 if (!survivor_producer_context(owner) || MyProc != proc
  || MyBackendType != backend || MyAuxProcType != aux || MyProcPid != pid
  || proc->pgprocno != pgprocno || proc->backendId != backend_id
  || proc->cluster_grd_generation != generation
  || (owner == CLUSTER_SURVIVOR_PRODUCER_BACKEND && CurrentResourceOwner != resource)
  || (owner == CLUSTER_SURVIVOR_PRODUCER_INPUT_JOBS
   && (CurrentResourceOwner != resource || AuxProcessResourceOwner != resource)))
  return survivor_producer_unknown(out, CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_REFUSED,
   "SURVIVOR_PRODUCER_RETURN_IDENTITY_INVALID");
 if (cluster_epoch_get_current() != scope.native_epoch)
  return survivor_producer_unknown(out, CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_STALE,
   "SURVIVOR_PRODUCER_EPOCH_CHANGED");
 out->owner = owner;
 out->reason = reason != NULL ? reason : "SURVIVOR_PRODUCER_CLOSURE_UNPROVED";
 return CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_PENDING;
}
