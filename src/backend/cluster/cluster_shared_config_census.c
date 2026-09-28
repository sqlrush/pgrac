/*-------------------------------------------------------------------------
 *
 * cluster_shared_config_census.c
 *    PGRAC native configuration observations, not cluster admission.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/backend/cluster/cluster_shared_config_census.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "miscadmin.h"
#include "postmaster/syslogger.h"
#include "storage/proc.h"
#include "utils/memutils.h"
#include "cluster/cluster_shared_config.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_lms_shard.h"

/* Native AuxProcType is the unique worker ordinal; BackendType alone cannot
 * distinguish seven LMS workers or eight cleaners. This table describes
 * existing native producers, not a new role or process-spawning policy. */
static const BackendType census_aux_roles[NUM_AUXPROCTYPES]
	= { [StartupProcess] = B_STARTUP,
		[BgWriterProcess] = B_BG_WRITER,
		[ArchiverProcess] = B_ARCHIVER,
		[CheckpointerProcess] = B_CHECKPOINTER,
		[WalWriterProcess] = B_WAL_WRITER,
		[WalReceiverProcess] = B_WAL_RECEIVER,
		[LmonProcess] = B_LMON,
		[LckProcess] = B_LCK,
		[DiagProcess] = B_DIAG,
		[ClusterStatsProcess] = B_CLUSTER_STATS,
		[CssdProcess] = B_CSSD,
		[QvotecProcess] = B_QVOTEC,
		[LmsProcess] = B_LMS,
		[LmdProcess] = B_LMD,
		[SinvalBcastProcess] = B_SINVAL_BCAST,
		[UndoCleanerProcess] = B_UNDO_CLEANER,
		[MrpProcess] = B_MRP,
		[RfsProcess] = B_RFS,
		[LmsWorker1Process] = B_LMS_WORKER,
		[LmsWorker2Process] = B_LMS_WORKER,
		[LmsWorker3Process] = B_LMS_WORKER,
		[LmsWorker4Process] = B_LMS_WORKER,
		[LmsWorker5Process] = B_LMS_WORKER,
		[LmsWorker6Process] = B_LMS_WORKER,
		[LmsWorker7Process] = B_LMS_WORKER,
		[UndoCleanerWorker1Process] = B_UNDO_CLEANER,
		[UndoCleanerWorker2Process] = B_UNDO_CLEANER,
		[UndoCleanerWorker3Process] = B_UNDO_CLEANER,
		[UndoCleanerWorker4Process] = B_UNDO_CLEANER,
		[UndoCleanerWorker5Process] = B_UNDO_CLEANER,
		[UndoCleanerWorker6Process] = B_UNDO_CLEANER,
		[UndoCleanerWorker7Process] = B_UNDO_CLEANER };
StaticAssertDecl(NUM_AUXPROCTYPES <= 64, "native configuration service bitmap");
StaticAssertDecl(CLUSTER_LMS_MAX_WORKERS == 8 && CLUSTER_UNDO_CLEANER_WORKER_TYPES == 8,
				 "native configuration worker roster must follow producer ordinals");

static bool
census_required_services(uint64 *required)
{
	static const AuxProcType core[]
		= { BgWriterProcess, CheckpointerProcess, WalWriterProcess,	  LmonProcess, LckProcess,
			CssdProcess,	 QvotecProcess,		  SinvalBcastProcess, LmsProcess };
	*required = 0;
	if (!cluster_shared_config || !cluster_enabled)
		return true;
	if (!cluster_lms_enabled || cluster_lms_workers < 1
		|| cluster_lms_workers > CLUSTER_LMS_MAX_WORKERS)
		return false;
	for (unsigned i = 0; i < lengthof(core); ++i)
		*required |= UINT64CONST(1) << core[i];
	if (cluster_lmd_enabled)
		*required |= UINT64CONST(1) << LmdProcess;
	for (int i = 1; i < cluster_lms_workers; ++i)
		*required |= UINT64CONST(1) << (LmsWorker1Process + i - 1);
	for (int i = 0; i < CLUSTER_UNDO_CLEANER_WORKER_TYPES; ++i)
		*required |= UINT64CONST(1) << ClusterUndoCleanerTypeForWorker(i);
	return true;
}

static bool
census_service(const ClusterSharedConfigRegistration *value, uint64 required, uint64 *seen)
{
	int aux = value->aux_type;
	uint64 bit;
	if (required == 0)
		return true;
	if (aux == NotAnAuxProcess) {
		for (unsigned i = 0; i < lengthof(census_aux_roles); ++i)
			if (value->role == (int32)census_aux_roles[i])
				return false;
		return value->role > B_INVALID && value->role < BACKEND_NUM_TYPES;
	}
	if (aux < 0 || aux >= NUM_AUXPROCTYPES || census_aux_roles[aux] == B_INVALID
		|| value->role != (int32)census_aux_roles[aux])
		return false;
	bit = UINT64CONST(1) << aux;
	if ((*seen & bit) != 0 || (aux == LmdProcess && !(required & bit))
		|| (aux >= LmsWorker1Process && aux <= LmsWorker7Process && !(required & bit)))
		return false;
	*seen |= bit;
	return true;
}

typedef struct ConfigCensusSlot {
	uint64 sequence;
	int32 pid;
} ConfigCensusSlot;

static bool
census_overlap(const void *a, size_t alen, const void *b, size_t blen)
{
	uintptr_t av = (uintptr_t)a, bv = (uintptr_t)b;
	return a != NULL && b != NULL && (av <= bv ? bv - av < alen : av - bv < blen);
}

static void
census_count(const ClusterSharedConfigRegistration *value, ClusterSharedConfigCensus *out)
{
	const ClusterSharedConfigProcess *p = &value->process;
	out->participants++;
	if (!value->observed || value->registration == 0)
		out->waiting_processes++;
	else if (p->failed)
		out->failed_processes++;
	else if (p->parallel_snapshot)
		out->parallel_processes++;
	else if (p->node_id != out->node_id || p->applier_pid <= 0
			 || memcmp(&p->ref, &out->ref, sizeof(p->ref)) != 0)
		out->waiting_processes++;
	else {
		out->current_processes++;
		out->pending_processes += p->pending_restart_total != 0;
		out->deferred_processes += p->deferred_total != 0;
		out->pending_entries += (uint64)p->pending_restart_total;
		out->deferred_entries += (uint64)p->deferred_total;
		if (value->active.version != CLUSTER_SHARED_CONFIG_ACTIVE_VERSION
			|| value->active.static_entries == 0 || value->active.dynamic_entries == 0)
			out->active_missing_processes++;
		else if (out->active.version == CLUSTER_SHARED_CONFIG_ACTIVE_VERSION) {
			out->static_mismatch_processes
				+= value->active.static_entries != out->active.static_entries
				   || memcmp(value->active.static_sha256, out->active.static_sha256, 32) != 0;
			out->dynamic_mismatch_processes
				+= value->active.dynamic_entries != out->active.dynamic_entries
				   || memcmp(value->active.dynamic_sha256, out->active.dynamic_sha256, 32) != 0;
		}
	}
}

static bool
census_capture(ClusterSharedConfigSlot *slot, int32 pid, ConfigCensusSlot *captured,
			   ClusterSharedConfigCensus *result, bool parent, uint64 required, uint64 *seen)
{
	ClusterSharedConfigRegistration value;
	uint64 sequence = pg_atomic_read_u64(&slot->sequence);
	if ((sequence & 1) || !cluster_shared_config_registration_read(slot, &value))
		return false;
	pg_read_barrier();
	if (sequence != pg_atomic_read_u64(&slot->sequence) || value.pid != pid || pid < 0
		|| (parent
			&& (pid == 0 || value.role != B_INVALID
				|| (required != 0 && value.aux_type != NotAnAuxProcess)))
		|| (!parent && pid > 0 && !census_service(&value, required, seen)))
		return false;
	if (parent && value.observed && !value.process.failed && !value.process.parallel_snapshot
		&& value.process.node_id == result->node_id && value.process.applier_pid > 0
		&& memcmp(&value.process.ref, &result->ref, sizeof(result->ref)) == 0
		&& value.active.version == CLUSTER_SHARED_CONFIG_ACTIVE_VERSION
		&& value.active.static_entries != 0 && value.active.dynamic_entries != 0)
		result->active = value.active;
	if (pid > 0)
		census_count(&value, result);
	else if (value.observed)
		return false;
	captured->sequence = sequence;
	captured->pid = pid;
	return true;
}

bool
cluster_shared_config_node_census(const ClusterSharedConfigRef *target, int node_id,
								  ClusterSharedConfigCensus *out)
{
	ClusterSharedConfigCensus result = { 0 };
	ClusterSharedConfigRegistration logger, logger_recheck;
	ConfigCensusSlot parent, *slots;
	int32 *pids, *pids_after;
	uint64 logger_sequence = 0, logger_after = 0;
	uint64 required, seen = 0;
	uint32 count;
	bool valid = false;

	if (out == NULL || census_overlap(target, sizeof(*target), out, sizeof(*out))
		|| census_overlap(ProcGlobal, sizeof(*ProcGlobal), out, sizeof(*out))
		|| (ProcGlobal != NULL
			&& census_overlap(ProcGlobal->allProcs, (Size)ProcGlobal->allProcCount * sizeof(PGPROC),
							  out, sizeof(*out))))
		return false;
	memset(out, 0, sizeof(*out));
	if (target == NULL || target->identity.generation == 0 || node_id < 0 || node_id >= 128
		|| (target->identity.configured[node_id / 64] & (UINT64CONST(1) << (node_id % 64))) == 0
		|| ProcGlobal == NULL || ProcGlobal->allProcs == NULL || PostmasterPid <= 0)
		return false;
	count = ProcGlobal->allProcCount;
	if (count == 0 || count > MaxAllocSize / (sizeof(*slots) + 2 * sizeof(*pids)))
		return false;
	result.ref = *target;
	result.node_id = node_id;
	if (!census_required_services(&required))
		return false;
	/* Allocate before any capture; no fallible work/wait while scanning. The
	 * real registration readers are bounded, nonthrowing value observations. */
	slots = palloc((Size)count * (sizeof(*slots) + 2 * sizeof(*pids)));
	pids = (int32 *)(slots + count);
	pids_after = pids + count;
	if (!ProcConfigSnapshotPids(pids, count))
		goto done;
	if (!census_capture(&ProcGlobal->cluster_config_postmaster, PostmasterPid, &parent, &result,
						true, required, &seen))
		goto done;
	if (Logging_collector) {
		if (!cluster_shared_config_delivery_logger_snapshot(&logger, &logger_sequence)
			|| logger.pid <= 0 || logger.role != B_LOGGER
			|| (required != 0 && logger.aux_type != NotAnAuxProcess))
			goto done;
		census_count(&logger, &result);
	}
	for (uint32 i = 0; i < count; ++i) {
		PGPROC *proc = &ProcGlobal->allProcs[i];
		if (!census_capture(&proc->cluster_config, pids[i], &slots[i], &result, false, required,
							&seen))
			goto done;
	}
	/* Missing configured services are not an empty live subset. Their native
	 * owner may respawn them; this observer cannot declare them unnecessary. */
	if ((seen & required) != required)
		goto done;
	/* Every captured interval must span this boundary. A future native birth
	 * is not prevented: its consumer must still prove its own permission. */
	pg_read_barrier();
	if (!ProcConfigSnapshotPids(pids_after, count) || count != ProcGlobal->allProcCount
		|| parent.pid != PostmasterPid
		|| parent.sequence != pg_atomic_read_u64(&ProcGlobal->cluster_config_postmaster.sequence))
		goto done;
	for (uint32 i = 0; i < count; ++i) {
		PGPROC *proc = &ProcGlobal->allProcs[i];
		if (slots[i].pid != pids_after[i]
			|| slots[i].sequence != pg_atomic_read_u64(&proc->cluster_config.sequence))
			goto done;
	}
	if (Logging_collector
		&& (!cluster_shared_config_delivery_logger_snapshot(&logger_recheck, &logger_after)
			|| logger_after != logger_sequence || logger_recheck.pid != logger.pid))
		goto done;
	*out = result;
	valid = true;
done:
	pfree(slots);
	return valid;
}
