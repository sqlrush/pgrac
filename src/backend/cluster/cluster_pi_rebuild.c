/* PGRAC: retained-WAL contributor census before local GCS master service.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"

#include "cluster/cluster_gcs.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_lmon.h"
#include "cluster/cluster_pi_rebuild.h"
#include "cluster/cluster_wal_inputs.h"
#include "cluster/cluster_wal_thread.h"
#include "miscadmin.h"
#include "postmaster/interrupt.h"
#include "storage/buffile.h"
#include "utils/resowner.h"

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
} PiRebuildJob;

static PiRebuildJob *pi_rebuild_job;
static bool pi_rebuild_callback_registered;
static bool pi_rebuild_logged;
static ClusterGrdPiRebuildCutV1 pi_rebuild_logged_cut;
static bool pi_rebuild_apply_logged;
static ClusterGrdPiRebuildCutV1 pi_rebuild_apply_logged_cut;
static bool pi_rebuild_plan_logged;
static ClusterGrdPiRebuildCutV1 pi_rebuild_plan_logged_cut;

static void
pi_rebuild_release(void)
{
	if (pi_rebuild_job == NULL)
		return;
	if (pi_rebuild_job->spool != NULL)
		BufFileClose(pi_rebuild_job->spool);
	cluster_wal_inputs_release_v1(&pi_rebuild_job->inputs);
	pfree(pi_rebuild_job);
	pi_rebuild_job = NULL;
}

static void
pi_rebuild_owner_release(ResourceReleasePhase phase, bool is_commit pg_attribute_unused(),
						 bool is_top_level pg_attribute_unused(), void *arg pg_attribute_unused())
{
	/* Native file/input owners release resources before this memory context
	 * is reset. No retry may retain any pointer from a failed owner. */
	if (phase == RESOURCE_RELEASE_BEFORE_LOCKS && pi_rebuild_job != NULL
		&& pi_rebuild_job->owner == CurrentResourceOwner)
		pi_rebuild_job = NULL;
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

	if (MyBackendType != B_BG_WRITER || !cluster_enabled || !cluster_shared_config
		|| CurrentResourceOwner == NULL || CritSectionCount != 0)
		return CLUSTER_PI_REBUILD_IDLE;
	if (!pi_rebuild_callback_registered) {
		RegisterResourceReleaseCallback(pi_rebuild_owner_release, NULL);
		pi_rebuild_callback_registered = true;
	}
	state = cluster_grd_pi_rebuild_snapshot_v1(&cut);
	if (ShutdownRequestPending || state != 1 || !cluster_grd_pi_rebuild_gate_v1()
		|| !cluster_wal_thread_current_v2_ref(&local)
		|| local.claim.identity.origin_owner_incarnation != cut.self_boot
		|| local.claim.identity.origin_node_id != cluster_node_id)
		goto done;
	if (pi_rebuild_job != NULL
		&& (pi_rebuild_job->owner != CurrentResourceOwner
			|| memcmp(&pi_rebuild_job->cut, &cut, sizeof(cut)) != 0
			|| memcmp(&pi_rebuild_job->local, &local, sizeof(local)) != 0))
		pi_rebuild_release();
	if (pi_rebuild_job == NULL) {
		PiRebuildJob *job = palloc0(sizeof(*job));
		job->owner = CurrentResourceOwner;
		job->cut = cut;
		job->local = local;
		pi_rebuild_job = job;
	}
	if (pi_rebuild_job->plan_blocked)
		goto failed;
	if (pi_rebuild_job->inputs == NULL) {
		result = cluster_wal_inputs_begin_v1(local.claim.identity.storage_uuid,
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
	result = cluster_wal_inputs_resume_v1(pi_rebuild_job->inputs);
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
		|| cluster_wal_inputs_revalidate_v1(pi_rebuild_job->inputs)
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
	if (cluster_wal_inputs_revalidate_v1(pi_rebuild_job->inputs) == CLUSTER_CONTROL_ROOT_OK_PRIMARY
		&& cluster_grd_pi_rebuild_complete_v1(&cut))
		cluster_lmon_wakeup();
done:
	pi_rebuild_release();
	return CLUSTER_PI_REBUILD_IDLE;
failed:
	result = cluster_wal_inputs_wait_failed_v1(pi_rebuild_job->inputs);
	if (result == CLUSTER_CONTROL_ROOT_STALE_TOKEN
		|| result == CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT)
		goto done;
	return CLUSTER_PI_REBUILD_WAIT;
blocked:
	if (!cluster_grd_pi_rebuild_current_v1(&cut)
		|| cluster_wal_inputs_revalidate_v1(pi_rebuild_job->inputs)
			   != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		goto done;
wait:
	if (cluster_wal_inputs_suspend_v1(pi_rebuild_job->inputs) != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		goto done;
	return progress;
}
