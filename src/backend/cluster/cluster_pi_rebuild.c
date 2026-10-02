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
#include "utils/resowner.h"

typedef struct PiRebuildJob {
	ResourceOwner owner;
	ClusterGrdPiRebuildCutV1 cut;
	ClusterWalSourceRef local;
	ClusterWalInputsV1 *inputs;
	ClusterThreadRecoveryFabricPlanV1 *plan;
	uint32 cursor;
	uint32 side_cursor;
	uint8 side_block;
	bool side_blocked;
	bool side_checked;
} PiRebuildJob;

static PiRebuildJob *pi_rebuild_job;
static bool pi_rebuild_callback_registered;
static bool pi_rebuild_logged;
static ClusterGrdPiRebuildCutV1 pi_rebuild_logged_cut;

static void
pi_rebuild_release(void)
{
	if (pi_rebuild_job == NULL)
		return;
	cluster_thread_recovery_fabric_plan_destroy_v1(&pi_rebuild_job->plan);
	cluster_wal_inputs_release_v1(&pi_rebuild_job->inputs);
	pfree(pi_rebuild_job);
	pi_rebuild_job = NULL;
}

static void
pi_rebuild_owner_release(ResourceReleasePhase phase, bool is_commit pg_attribute_unused(),
						 bool is_top_level pg_attribute_unused(), void *arg pg_attribute_unused())
{
	/* The native input/pin owners perform error cleanup. Their memory context
	 * is about to be reset, so no retry may retain these process pointers. */
	if (phase == RESOURCE_RELEASE_BEFORE_LOCKS && pi_rebuild_job != NULL
		&& pi_rebuild_job->owner == CurrentResourceOwner)
		pi_rebuild_job = NULL;
}

static bool
pi_rebuild_target(PiRebuildJob *job, const RfPageOnlinePlanV1 *plan, uint32 index)
{
	RfPageOnlineTargetViewV1 target;
	const RfContributorVectorV1 *contributors;
	BufferTag tag;
	uint32 holders = 0;
	XLogRecPtr watermark_lsn = InvalidXLogRecPtr;
	SCN watermark_scn = 0;
	int home;

	if (!rf_page_online_plan_target_v1(plan, index, &target)
		|| (contributors = target.contributors) == NULL || contributors->edge_count == 0)
		return false;
	InitBufferTag(&tag, &target.page_identity.locator, target.page_identity.forknum,
				  target.page_identity.blockno);
	home = cluster_gcs_lookup_master_static(tag);
	if (home < 0 || home >= 32)
		return false;
	if (!(job->cut.affected[home / 8] & (1u << (home % 8)))
		|| cluster_gcs_lookup_master(tag) != cluster_node_id)
		return true;
	for (uint32 i = 0; i < contributors->edge_count; i++) {
		const RfPageStableEdgeInputV1 *edge = &contributors->edges[i];
		ClusterWalSourceRef source;
		int node;
		if (edge->participant_index >= contributors->participant_count
			|| !rf_page_online_plan_source_v1(plan, edge->participant_index, &source)
			|| !cluster_wal_claim_v2_ref_valid(&source.claim)
			|| source.claim.identity.origin_owner_incarnation == 0
			|| source.claim.identity.system_identifier
				   != job->local.claim.identity.system_identifier
			|| source.claim.database_incarnation != job->local.claim.database_incarnation
			|| memcmp(source.claim.identity.storage_uuid, job->local.claim.identity.storage_uuid,
					  16)
				   != 0
			|| (node = source.claim.identity.origin_node_id) < 0 || node >= 32
			|| source.claim.identity.origin_thread_id != node + 1
			|| source.timeline != edge->record_identity.timeline_id
			|| source.claim.identity.origin_thread_id != edge->record_identity.origin_thread
			|| source.claim.identity.origin_owner_incarnation
				   != contributors->cuts[edge->participant_index].origin_owner_incarnation
			|| edge->edge.page_class != RF_PAGE_CLASS_ORDINARY || edge->result_token == 0
			|| XLogRecPtrIsInvalid(edge->record_identity.read_rec_ptr))
			return false;
		holders |= (uint32)1u << node;
		watermark_lsn = Max(watermark_lsn, edge->record_identity.read_rec_ptr);
		if (scn_local(edge->result_token) > scn_local(watermark_scn))
			watermark_scn = edge->result_token;
	}
	return cluster_pcm_rebuild_pi_contributors_v1(&job->cut, tag, holders, watermark_lsn,
												  watermark_scn);
}

static bool
pi_rebuild_side_covered(const ClusterThreadRecoveryFabricPlanV1 *plan, uint32 *blocked)
{
	const RfSideOnlinePlanV1 *side = cluster_thread_recovery_fabric_side_plan_v1(plan);
	uint32 count = rf_side_online_plan_operation_count_v1(side);
	if (side == NULL || count == UINT32_MAX)
		return false;
	for (uint32 i = 0; i < count; i++) {
		RfSideOnlineOperationV1 operation;
		RfSideSpaceContributionV1 space;
		*blocked = i;
		if (!rf_side_online_plan_operation_v1(side, i, &operation))
			return false;
		if (operation.kind == RF_SIDE_ONLINE_OPERATION_NATIVE_CONTROL)
			continue;
		if (operation.kind == RF_SIDE_ONLINE_OPERATION_SPACE
			&& rf_side_online_plan_space_contribution_count_v1(side, i) == 1
			&& rf_side_online_plan_space_contribution_v1(side, i, 0, &space))
			continue;
		/* Other owners, including a COMMIT's TT and structural components,
		 * must all be mapped before the directory can complete its census. */
		return false;
	}
	return true;
}

static bool
pi_rebuild_space(PiRebuildJob *job, const RfPageOnlinePlanV1 *page, const RfSideOnlinePlanV1 *side)
{
	RfSideOnlineOperationV1 operation;
	RfSideSpaceContributionV1 contribution;
	ClusterWalSourceRef source;
	RfContributorStreamCutV1 physical;
	BufferTag tag;
	int node, home;
	uint32 index = job->side_cursor;
	uint8 block = job->side_block++;

	if (!rf_side_online_plan_operation_v1(side, index, &operation))
		return false;
	if (operation.kind == RF_SIDE_ONLINE_OPERATION_NATIVE_CONTROL) {
		job->side_cursor++;
		job->side_block = 0;
		return true;
	}
	if (operation.kind != RF_SIDE_ONLINE_OPERATION_SPACE || block > 1
		|| !rf_side_online_plan_space_contribution_v1(side, index, 0, &contribution)
		|| !rf_page_online_plan_source_v1(page, operation.identity.participant_index, &source)
		|| !cluster_thread_recovery_fabric_cut_v1(job->plan, operation.identity.participant_index,
												  &physical)
		|| !cluster_wal_claim_v2_ref_valid(&source.claim)
		|| (node = source.claim.identity.origin_node_id) < 0 || node >= 32
		|| source.claim.identity.origin_thread_id != node + 1
		|| source.claim.identity.origin_owner_incarnation == 0
		|| physical.origin_owner_incarnation != source.claim.identity.origin_owner_incarnation
		|| source.claim.identity.system_identifier != job->local.claim.identity.system_identifier
		|| source.claim.database_incarnation != job->local.claim.database_incarnation
		|| memcmp(source.claim.identity.storage_uuid, job->local.claim.identity.storage_uuid, 16)
			   != 0
		|| physical.failed_thread != source.claim.identity.origin_thread_id
		|| physical.timeline_id != source.timeline
		|| operation.identity.record.origin_thread != physical.failed_thread
		|| operation.identity.record.timeline_id != physical.timeline_id
		|| operation.identity.record.read_rec_ptr < physical.scan_begin_inclusive
		|| operation.identity.record.end_rec_ptr > physical.scan_end_exclusive
		|| operation.identity.record.end_rec_ptr <= operation.identity.record.read_rec_ptr
		|| contribution.result.key.system_identifier != source.claim.identity.system_identifier
		|| contribution.result.key.database_incarnation != source.claim.database_incarnation
		|| memcmp(contribution.result.key.storage_uuid, source.claim.identity.storage_uuid, 16)
			   != 0)
		return false;
	if (block == 1) {
		job->side_cursor++;
		job->side_block = 0;
	}
	if (!(contribution.page_mask & (1u << block)))
		return true;
	InitBufferTag(&tag, &contribution.result.key.locator, SPACE_FORKNUM, block);
	home = cluster_gcs_lookup_master_static(tag);
	if (home < 0 || home >= 32)
		return false;
	if (!(job->cut.affected[home / 8] & (1u << (home % 8)))
		|| cluster_gcs_lookup_master(tag) != cluster_node_id)
		return true;
	return cluster_pcm_rebuild_pi_contributors_v1(&job->cut, tag, (uint32)1u << node,
												  operation.identity.record.read_rec_ptr,
												  contribution.result_token[block]);
}

static void
pi_rebuild_report_side_blocked(PiRebuildJob *job, uint32 index)
{
	RfSideOnlineOperationV1 operation = { 0 };

	job->side_blocked = true;
	if (pi_rebuild_logged && memcmp(&pi_rebuild_logged_cut, &job->cut, sizeof(job->cut)) == 0)
		return;
	pi_rebuild_logged = true;
	pi_rebuild_logged_cut = job->cut;
	cluster_grd_inc_pi_rebuild_side_blocked();
	(void)rf_side_online_plan_operation_v1(cluster_thread_recovery_fabric_side_plan_v1(job->plan),
										   index, &operation);
	ereport(LOG, (errmsg("cluster PI rebuild is waiting for a typed SIDE contribution owner"),
				  errdetail("Epoch " UINT64_FORMAT
							", direction %u, operation %u, kind %u, WAL thread %u, record %X/%X. "
							"The retained plan remains blocked with WAL read pins released.",
							job->cut.epoch, job->cut.direction, index, operation.kind,
							operation.identity.record.origin_thread,
							LSN_FORMAT_ARGS(operation.identity.record.read_rec_ptr))));
}

bool
cluster_pi_rebuild_bgwriter_tick_v1(void)
{
	ClusterGrdPiRebuildCutV1 cut;
	ClusterWalSourceRef local;
	const RfPageOnlinePlanV1 *page;
	const RfSideOnlinePlanV1 *side;
	ClusterControlRootResult result;
	uint64 records;
	RfPageProofDetailV1 detail;
	uint32 blocked = UINT32_MAX;
	uint32 budget = 64;
	int state;

	if (MyBackendType != B_BG_WRITER || !cluster_enabled || !cluster_shared_config
		|| CurrentResourceOwner == NULL || CritSectionCount != 0)
		return false;
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
	if (pi_rebuild_job->inputs == NULL) {
		result = cluster_wal_inputs_begin_v1(local.claim.identity.storage_uuid,
											 local.claim.identity.system_identifier,
											 &pi_rebuild_job->inputs);
		if (result == CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE
			|| result == CLUSTER_CONTROL_ROOT_RECONFIG_WAIT)
			return true;
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			goto done;
	}
	result = cluster_wal_inputs_resume_v1(pi_rebuild_job->inputs);
	if (result == CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE)
		goto wait;
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		goto done;
	if (pi_rebuild_job->plan == NULL) {
		result = cluster_wal_inputs_contributions_v1(pi_rebuild_job->inputs, true,
													 &pi_rebuild_job->plan, &records, &detail);
		if (result == CLUSTER_CONTROL_ROOT_RECONFIG_WAIT)
			goto wait;
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			goto done;
	}
	page = cluster_thread_recovery_fabric_page_plan_v1(pi_rebuild_job->plan);
	if (page == NULL || !cluster_grd_pi_rebuild_current_v1(&cut)
		|| cluster_wal_inputs_revalidate_v1(pi_rebuild_job->inputs)
			   != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		goto done;
	if (pi_rebuild_job->side_blocked)
		goto wait;
	if (!pi_rebuild_job->side_checked && !pi_rebuild_side_covered(pi_rebuild_job->plan, &blocked)) {
		pi_rebuild_report_side_blocked(pi_rebuild_job, blocked);
		goto wait;
	}
	pi_rebuild_job->side_checked = true;
	/* Directory additions are bounded per tick and remain invisible to
	 * grants until the complete graph and original ROOT have been checked. */
	for (; budget > 0 && pi_rebuild_job->cursor < rf_page_online_plan_target_count_v1(page);
		 budget--) {
		if (!pi_rebuild_target(pi_rebuild_job, page, pi_rebuild_job->cursor++))
			goto done;
	}
	if (pi_rebuild_job->cursor != rf_page_online_plan_target_count_v1(page))
		goto wait;
	side = cluster_thread_recovery_fabric_side_plan_v1(pi_rebuild_job->plan);
	for (; budget > 0 && pi_rebuild_job->side_cursor < rf_side_online_plan_operation_count_v1(side);
		 budget--)
		if (!pi_rebuild_space(pi_rebuild_job, page, side))
			goto done;
	if (pi_rebuild_job->side_cursor != rf_side_online_plan_operation_count_v1(side))
		goto wait;
	if (cluster_wal_inputs_revalidate_v1(pi_rebuild_job->inputs) == CLUSTER_CONTROL_ROOT_OK_PRIMARY
		&& cluster_grd_pi_rebuild_complete_v1(&cut))
		cluster_lmon_wakeup();
done:
	pi_rebuild_release();
	return false;
wait:
	if (cluster_wal_inputs_suspend_v1(pi_rebuild_job->inputs) != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		goto done;
	return true;
}
