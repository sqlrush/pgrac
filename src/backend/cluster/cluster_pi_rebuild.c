/* PGRAC: retained-WAL contributor census before local GCS master service.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"

#include "cluster/cluster_gcs.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_clean_leave.h"
#include "cluster/cluster_lmon.h"
#include "cluster/cluster_pi_rebuild.h"
#include "cluster/cluster_semantic_activation.h"
#include "cluster/cluster_wal_inputs.h"
#include "cluster/cluster_wal_thread.h"
#include "miscadmin.h"
#include "postmaster/interrupt.h"
#include "storage/buffile.h"
#include "utils/resowner.h"
#include "utils/timestamp.h"
#include "cluster_wal_inputs_job_payload_private.h"

#define PI_REBUILD_BATCH 64

typedef struct PiRebuildContribution {
	BufferTag tag;
	uint32 holders;
	XLogRecPtr lsn;
	SCN scn;
} PiRebuildContribution;

typedef struct PiRebuildJob {
	ResourceOwner owner;
	ClusterGrdPiRebuildCutV1 cut;
	ClusterWalSourceRef local;
	ClusterWalSourceRef source;
	ClusterWalInputsV1 *inputs;
	ClusterWalInputsJobV1 *inputs_job;
	BufFile *spool;
	PiRebuildContribution batch[PI_REBUILD_BATCH];
	PiRebuildContribution pending;
	uint32 batch_count;
	uint64 total;
	uint64 cursor;
	uint64 records;
	int source_node;
	XLogRecPtr source_lsn;
	XLogRecPtr recovered_end;
	bool source_valid;
	bool pending_valid;
	bool scanned;
	bool plan_blocked;
	/* PGRAC: when this cut's census began, for its completion reading. */
	TimestampTz started;
} PiRebuildJob;

StaticAssertDecl(sizeof(PiRebuildJob)
				 <= CLUSTER_WAL_INPUT_JOB_REBUILD_BYTES,
				 "WAL input rebuild exceeds its payload envelope");

static PiRebuildJob *pi_rebuild_job;
static bool pi_rebuild_callback_registered;
static bool pi_rebuild_logged;
static ClusterGrdPiRebuildCutV1 pi_rebuild_logged_cut;
static bool pi_rebuild_apply_logged;
static ClusterGrdPiRebuildCutV1 pi_rebuild_apply_logged_cut;
static bool pi_rebuild_plan_logged;
static ClusterGrdPiRebuildCutV1 pi_rebuild_plan_logged_cut;

static bool
pi_rebuild_release(void)
{
	ClusterWalInputsJobV1 *job;
	ClusterWalInputsJobResultV1 result;
	if (pi_rebuild_job == NULL)
		return true;
	job = pi_rebuild_job->inputs_job;
	if (pi_rebuild_job->owner != CurrentResourceOwner
		|| !cluster_wal_inputs_job_consumer_matches_v1(job, WAL_INPUT_JOB_PI_REBUILD))
		elog(FATAL, "invalid original PI rebuild release identity");
	if (pi_rebuild_job->spool != NULL) {
		BufFileClose(pi_rebuild_job->spool);
		pi_rebuild_job->spool = NULL;
	}
	result = cluster_wal_inputs_job_retire_v1(job);
	if (result == WAL_INPUT_JOB_INVALID)
		elog(FATAL, "invalid original PI rebuild retirement");
	if (result != WAL_INPUT_JOB_DONE)
		return false;
	pi_rebuild_job = NULL;
	if (cluster_wal_inputs_job_finish_v1(&job) != WAL_INPUT_JOB_DONE)
		elog(FATAL, "invalid original PI rebuild completion");
	return true;
}

/* Failure waiting belongs to this original business header, not input
 * authority. DONE retains it until the immutable ROOT baseline changes. */
static bool
pi_rebuild_failed_wait(void)
{
	ClusterWalInputsJobResultV1 retired = cluster_wal_inputs_job_wait_failed_v1(pi_rebuild_job->inputs_job);
	ClusterControlRootResult result;

	if (retired == WAL_INPUT_JOB_INVALID)
		elog(FATAL, "invalid failed PI rebuild input cleanup");
	if (retired == WAL_INPUT_JOB_PENDING)
		return true;
	result = cluster_wal_inputs_job_failed_root_v1(pi_rebuild_job->inputs_job);
	return result == CLUSTER_CONTROL_ROOT_OK_PRIMARY || result == CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE
		|| result == CLUSTER_CONTROL_ROOT_RECONFIG_WAIT;
}

static void
pi_rebuild_owner_release(ResourceReleasePhase phase, bool is_commit pg_attribute_unused(),
						 bool is_top_level pg_attribute_unused(), void *arg pg_attribute_unused())
{
	if (phase == RESOURCE_RELEASE_BEFORE_LOCKS && pi_rebuild_job != NULL
		&& pi_rebuild_job->owner == CurrentResourceOwner) {
		if (!cluster_wal_inputs_job_owner_revoke_v1(pi_rebuild_job->inputs_job,
				WAL_INPUT_JOB_PI_REBUILD, !is_commit))
			elog(FATAL, "invalid original PI rebuild input cleanup");
		/* Native file cleanup/reset owns this borrow. Never close it again. */
		pi_rebuild_job->spool = NULL;
		pi_rebuild_job->inputs = NULL;
	}
}

static void
pi_rebuild_spill(PiRebuildJob *job)
{
	if (job->batch_count == 0)
		return;
	if (job->spool == NULL)
		job->spool = BufFileCreateTemp(false);
	BufFileWrite(job->spool, job->batch, sizeof(job->batch[0]) * job->batch_count);
	job->total += job->batch_count;
	job->batch_count = 0;
}

static bool
pi_rebuild_add(void *arg, const RelFileLocator *locator, ForkNumber forknum, BlockNumber blockno,
			   uint64 token)
{
	PiRebuildJob *job = arg;
	BufferTag tag;
	PiRebuildContribution *item;
	int home;

	/* PGRAC: an exact RECOVERY_COMPLETE prefix qualifies only the ROOT chain.
	 * A recovered writer's responsibility is rebuilt like any other; only
	 * the recovered acknowledgement (actual DATA, its exact DEAD boot and the
	 * complete master cut) discharges it, never this single ROOT value. */
	if (token == 0 || job->source_node < 0 || job->source_node >= 32
		|| XLogRecPtrIsInvalid(job->source_lsn))
		return false;
	InitBufferTag(&tag, locator, forknum, blockno);
	home = cluster_gcs_lookup_master_static(tag);
	if (home < 0 || home >= 32)
		return false;
	if (!(job->cut.affected[home / 8] & (1u << (home % 8)))
		|| cluster_gcs_lookup_master(tag) != cluster_node_id)
		return true;
	for (uint32 i = 0; i < job->batch_count; i++)
		if (BufferTagsEqual(&job->batch[i].tag, &tag)) {
			item = &job->batch[i];
			goto merge;
		}
	if (job->total > UINT64_MAX - PI_REBUILD_BATCH)
		return false;
	if (job->batch_count == PI_REBUILD_BATCH)
		pi_rebuild_spill(job);
	item = &job->batch[job->batch_count++];
	memset(item, 0, sizeof(*item));
	item->tag = tag;
merge:
	item->holders |= (uint32)1u << job->source_node;
	item->lsn = Max(item->lsn, job->source_lsn);
	if (scn_local(token) > scn_local(item->scn))
		item->scn = token;
	return true;
}

static RfPageProofDetailV1
pi_rebuild_record(XLogReaderState *record, const ClusterWalSourceRef *source,
				  const RfContributorStreamCutV1 *cut, void *arg)
{
	PiRebuildJob *job = arg;

	if ((job->records++ % PI_REBUILD_BATCH) == 0 && !cluster_grd_pi_rebuild_current_v1(&job->cut))
		return RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH;
	if (source->claim.identity.system_identifier != job->local.claim.identity.system_identifier
		|| source->claim.database_incarnation != job->local.claim.database_incarnation
		|| memcmp(source->claim.identity.storage_uuid, job->local.claim.identity.storage_uuid, 16))
		return RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH;
	if (!job->source_valid || memcmp(&job->source, source, sizeof(*source)) != 0) {
		job->source = *source;
		job->source_valid = true;
		(void)cluster_wal_inputs_recovered_prefix_v1(job->inputs, source, &job->recovered_end);
	}
	/* A recovered source still ends exactly at its recovered prefix. */
	if (job->recovered_end != InvalidXLogRecPtr
		&& (record->EndRecPtr <= record->ReadRecPtr || record->EndRecPtr > job->recovered_end))
		return RF_PAGE_PROOF_DETAIL_SOURCE_GAP;
	job->source_node = source->claim.identity.origin_node_id;
	job->source_lsn = record->ReadRecPtr;
	return cluster_thread_recovery_record_census_v1(record, source, cut, pi_rebuild_add, job);
}

static void
pi_rebuild_report_apply_blocked(PiRebuildJob *job)
{
	if (pi_rebuild_apply_logged
		&& memcmp(&pi_rebuild_apply_logged_cut, &job->cut, sizeof(job->cut)) == 0)
		return;
	pi_rebuild_apply_logged = true;
	pi_rebuild_apply_logged_cut = job->cut;
	cluster_grd_inc_pi_rebuild_apply_blocked();
	ereport(
		LOG,
		(errmsg("cluster PI rebuild could not register a retained contribution"),
		 errdetail("Epoch " UINT64_FORMAT ", direction %u, contribution " UINT64_FORMAT ". "
				   "Service remains fenced; retrying the same target with WAL read pins released.",
				   job->cut.epoch, job->cut.direction, job->cursor)));
}

static void
pi_rebuild_report_plan_blocked(PiRebuildJob *job, ClusterControlRootResult result,
							   RfPageProofDetailV1 detail)
{
	if (result == CLUSTER_CONTROL_ROOT_STALE_TOKEN)
		return;
	if (detail == RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE) {
		if (pi_rebuild_logged && memcmp(&pi_rebuild_logged_cut, &job->cut, sizeof(job->cut)) == 0)
			return;
		pi_rebuild_logged = true;
		pi_rebuild_logged_cut = job->cut;
		cluster_grd_inc_pi_rebuild_side_blocked();
	} else {
		if (pi_rebuild_plan_logged
			&& memcmp(&pi_rebuild_plan_logged_cut, &job->cut, sizeof(job->cut)) == 0)
			return;
		pi_rebuild_plan_logged = true;
		pi_rebuild_plan_logged_cut = job->cut;
		cluster_grd_inc_pi_rebuild_plan_blocked();
	}
	ereport(LOG,
			(errmsg("cluster PI rebuild could not complete its retained contribution census"),
			 errdetail("Epoch " UINT64_FORMAT ", direction %u, input result %u, proof detail %u. "
					   "Service remains fenced; failed proofs are never used to complete the cut.",
					   job->cut.epoch, job->cut.direction, result, detail)));
}

ClusterPiRebuildProgressV1
cluster_pi_rebuild_bgwriter_tick_v1(void)
{
	ClusterGrdPiRebuildCutV1 cut;
	ClusterWalSourceRef local;
	ClusterControlRootResult result;
	uint64 records;
	RfPageProofDetailV1 detail;
	uint32 budget = PI_REBUILD_BATCH;
	ClusterPiRebuildProgressV1 progress = CLUSTER_PI_REBUILD_WAIT;
	int state;

	if (pi_rebuild_job != NULL
		&& !cluster_wal_inputs_job_consumer_matches_v1(pi_rebuild_job->inputs_job, WAL_INPUT_JOB_PI_REBUILD))
		elog(FATAL, "invalid original PI rebuild caller binding");
	if (pi_rebuild_job != NULL
		&& cluster_wal_inputs_job_context_v1(pi_rebuild_job->inputs_job) == NULL
		&& (!pi_rebuild_job->plan_blocked
			|| (pi_rebuild_job->inputs_job->flags & (WAL_INPUT_JOB_FLAG_ERROR | WAL_INPUT_JOB_FLAG_CANCELLED)) != 0))
		return pi_rebuild_release() ? CLUSTER_PI_REBUILD_IDLE : CLUSTER_PI_REBUILD_WAIT;
	if (MyBackendType != B_BG_WRITER || !cluster_enabled || !cluster_shared_config
		|| CurrentResourceOwner == NULL || CritSectionCount != 0)
		return CLUSTER_PI_REBUILD_IDLE;
	if (cluster_semantic_normal_start_state() == CLUSTER_NORMAL_START_EXISTING_OTHER
		&& !cluster_semantic_activation_recovered_open_current())
		return CLUSTER_PI_REBUILD_IDLE;
	if (!pi_rebuild_callback_registered) {
		RegisterResourceReleaseCallback(pi_rebuild_owner_release, NULL);
		pi_rebuild_callback_registered = true;
	}
	state = cluster_grd_pi_rebuild_snapshot_v1(&cut);
	if (ShutdownRequestPending || cluster_normal_stop_requested()
		|| state != 1 || !cluster_grd_pi_rebuild_gate_v1()
		|| !cluster_wal_thread_current_v2_ref(&local)
		|| local.claim.identity.origin_owner_incarnation != cut.self_boot
		|| local.claim.identity.origin_node_id != cluster_node_id)
		goto done;
	if (pi_rebuild_job != NULL
		&& (pi_rebuild_job->owner != CurrentResourceOwner
			|| memcmp(&pi_rebuild_job->cut, &cut, sizeof(cut)) != 0
			|| memcmp(&pi_rebuild_job->local, &local, sizeof(local)) != 0))
		if (!pi_rebuild_release())
			return CLUSTER_PI_REBUILD_WAIT;
	if (pi_rebuild_job == NULL) {
		ClusterWalInputsJobV1 *inputs_job = NULL;
		PiRebuildJob *job;
		result = cluster_wal_inputs_job_open_aux_v1(WAL_INPUT_JOB_PI_REBUILD, &inputs_job);
		if (result == CLUSTER_CONTROL_ROOT_RECONFIG_WAIT && inputs_job != NULL) {
			ClusterWalInputsJobResultV1 retired = cluster_wal_inputs_job_retire_v1(inputs_job);
			if (retired == WAL_INPUT_JOB_INVALID
				|| (retired == WAL_INPUT_JOB_DONE
					&& cluster_wal_inputs_job_finish_v1(&inputs_job) != WAL_INPUT_JOB_DONE))
				elog(FATAL, "invalid unbound PI rebuild cleanup");
			return CLUSTER_PI_REBUILD_WAIT;
		}
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			elog(FATAL, "invalid original PI rebuild auxiliary caller");
		job = MemoryContextAllocZero(cluster_wal_inputs_job_context_v1(inputs_job), sizeof(*job));
		job->inputs_job = inputs_job;
		job->owner = CurrentResourceOwner;
		job->started = GetCurrentTimestamp();
		job->cut = cut;
		job->local = local;
		pi_rebuild_job = job;
	}
	if (pi_rebuild_job->plan_blocked)
		goto failed;
	if (pi_rebuild_job->inputs_job->state == WAL_INPUT_JOB_STATE_NEW) {
		result = cluster_wal_inputs_job_begin_v1(pi_rebuild_job->inputs_job,
											   local.claim.identity.storage_uuid,
											 local.claim.identity.system_identifier,
											 &pi_rebuild_job->inputs);
		if (result == CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE
			|| result == CLUSTER_CONTROL_ROOT_RECONFIG_WAIT)
			return CLUSTER_PI_REBUILD_WAIT;
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
			pi_rebuild_report_plan_blocked(pi_rebuild_job, result, RF_PAGE_PROOF_DETAIL_OK);
			goto done;
		}
	}
	result = cluster_wal_inputs_job_resume_v1(pi_rebuild_job->inputs_job, &pi_rebuild_job->inputs);
	if (result == CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE)
		goto wait;
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
		pi_rebuild_report_plan_blocked(pi_rebuild_job, result, RF_PAGE_PROOF_DETAIL_OK);
		goto done;
	}
	if (!pi_rebuild_job->scanned) {
		result = cluster_wal_inputs_census_v1(pi_rebuild_job->inputs, pi_rebuild_record,
											  pi_rebuild_job, &records, &detail);
		if (result == CLUSTER_CONTROL_ROOT_RECONFIG_WAIT)
			goto wait;
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
			pi_rebuild_report_plan_blocked(pi_rebuild_job, result, detail);
			if (detail == RF_PAGE_PROOF_DETAIL_OK)
				goto done;
			pi_rebuild_job->plan_blocked = true;
			pi_rebuild_job->inputs_job->original_result = result;
			goto failed;
		}
		/* A failed physical suffix has made no directory additions. Spill
		 * only contributions, never full replay operations or payloads. */
		if (pi_rebuild_job->spool != NULL) {
			pi_rebuild_spill(pi_rebuild_job);
			if (BufFileSeek(pi_rebuild_job->spool, 0, 0, SEEK_SET) != 0)
				ereport(ERROR, (errmsg("could not rewind cluster PI census")));
		} else
			pi_rebuild_job->total = pi_rebuild_job->batch_count;
		pi_rebuild_job->scanned = true;
	}
	if (!cluster_grd_pi_rebuild_current_v1(&cut)
		|| cluster_wal_inputs_job_revalidate_v1(pi_rebuild_job->inputs_job)
			   != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		goto done;
	for (; budget > 0 && pi_rebuild_job->cursor < pi_rebuild_job->total; budget--) {
		PiRebuildJob *job = pi_rebuild_job;
		if (!job->pending_valid) {
			if (job->spool != NULL)
				BufFileReadExact(job->spool, &job->pending, sizeof(job->pending));
			else
				job->pending = job->batch[job->cursor];
			job->pending_valid = true;
		}
		if (!cluster_pcm_rebuild_pi_contributors_v1(&cut, job->pending.tag, job->pending.holders,
													job->pending.lsn, job->pending.scn)) {
			pi_rebuild_report_apply_blocked(job);
			goto blocked;
		}
		job->pending_valid = false;
		job->cursor++;
	}
	if (pi_rebuild_job->cursor != pi_rebuild_job->total) {
		progress = CLUSTER_PI_REBUILD_MORE;
		goto wait;
	}
	if (cluster_wal_inputs_job_revalidate_v1(pi_rebuild_job->inputs_job) == CLUSTER_CONTROL_ROOT_OK_PRIMARY
		&& cluster_grd_pi_rebuild_complete_v1(&cut)) {
		/* The fault-cut reading: census and registration of this cut. */
		ereport(DEBUG1,
				(errmsg("cluster PI rebuild completed for epoch " UINT64_FORMAT,
						pi_rebuild_job->cut.epoch),
				 errdetail("%llu records, %llu contributions, spool %llu bytes, %lld ms.",
						   (unsigned long long)pi_rebuild_job->records,
						   (unsigned long long)pi_rebuild_job->total,
						   (unsigned long long)(pi_rebuild_job->spool != NULL
													? pi_rebuild_job->total
														  * sizeof(PiRebuildContribution)
													: 0),
						   (long long)((GetCurrentTimestamp() - pi_rebuild_job->started) / 1000))));
		cluster_lmon_wakeup();
	}
done:
	return pi_rebuild_release() ? CLUSTER_PI_REBUILD_IDLE : CLUSTER_PI_REBUILD_WAIT;
failed:
	if (pi_rebuild_failed_wait())
		return CLUSTER_PI_REBUILD_WAIT;
	goto done;
blocked:
	if (!cluster_grd_pi_rebuild_current_v1(&cut)
		|| cluster_wal_inputs_job_revalidate_v1(pi_rebuild_job->inputs_job)
			   != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		goto done;
wait:
	if (cluster_wal_inputs_job_suspend_v1(pi_rebuild_job->inputs_job) != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		goto done;
	return progress;
}
