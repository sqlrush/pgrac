/*-------------------------------------------------------------------------
 * cluster_thread_recovery_fabric_scan.c
 *    Decode one exact ROOT-owned WAL cut into an immutable recovery fabric.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "access/xlogreader.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_thread_recovery.h"
#include "cluster/cluster_thread_recovery_authority.h"
#include "cluster/cluster_thread_recovery_fabric.h"
#include "cluster/cluster_wal_tail.h"

/* PGRAC: exact-generation records remain provisional until the complete
 * physical scan and owner revalidation succeed. Author: SqlRush <sqlrush@gmail.com> */
typedef struct FabricVisitWork {
	const ClusterThreadRecoveryAuthorityV1 *authorities;
	uint32 authority_count;
	uint16 participant_index;
	ClusterThreadRecoveryFabricPlanV1 *plan;
	RfPageProofDetailV1 detail;
	uint64 records;
} FabricVisitWork;

static bool
fabric_scan_authorities_current(const ClusterThreadRecoveryAuthorityV1 *authorities, uint32 count)
{
	uint32 i;

	for (i = 0; i < count; i++)
		if (cluster_thread_recovery_authority_revalidate_nowait_v1(&authorities[i])
			!= CLUSTER_THREAD_AUTHORITY_OK)
			return false;
	return true;
}

static bool
fabric_visit_record(XLogReaderState *reader, void *arg)
{
	FabricVisitWork *work = arg;
	if (!fabric_scan_authorities_current(work->authorities, work->authority_count)) {
		work->detail = RF_PAGE_PROOF_DETAIL_ROOT_STALE;
		return false;
	}
	if (work->records == UINT64_MAX) {
		work->detail = RF_PAGE_PROOF_DETAIL_CAPACITY;
		return false;
	}
	work->detail = cluster_thread_recovery_fabric_plan_feed_record_v1(work->plan, reader,
																	  work->participant_index);
	if (work->detail != RF_PAGE_PROOF_DETAIL_OK)
		return false;
	work->records++;
	return true;
}

static bool
fabric_scan_root_exact(uint16 dead_thread, XLogRecPtr scan_begin, XLogRecPtr scan_end,
					   const ClusterThreadRecoveryAuthorityV1 *authority)
{
	const ClusterControlRootSnapshot *root;

	if (authority == NULL || authority->duty == NULL || authority->root_snapshot == NULL
		|| authority->retention_pin == NULL)
		return false;
	root = authority->root_snapshot;
	return dead_thread != 0 && dead_thread == authority->duty->origin_thread_id
		   && dead_thread == root->identity.origin_thread_id
		   && memcmp(&root->identity, authority->duty, sizeof(*authority->duty)) == 0
		   && root->checkpoint_tli != 0 && root->checkpoint_tli == root->tail_tli
		   && scan_begin != InvalidXLogRecPtr && scan_end > scan_begin
		   && scan_begin == root->checkpoint_lower_lsn
		   && scan_end == root->validated_tail_lsn_exclusive;
}

RfPageProofDetailV1
cluster_thread_recovery_fabric_scan_roots_v1(const ClusterThreadRecoveryAuthorityV1 *authorities,
											 uint32 count, bool space_active,
											 ClusterThreadRecoveryFabricPlanV1 **out_plan,
											 uint64 *out_record_count)
{
	ClusterThreadRecoveryFabricPlanRequestV1 request = { 0 };
	ClusterThreadRecoveryFabricPlanV1 *plan = NULL;
	RfContributorStreamCutV1 cuts[RF_PAGE_STABLE_MAX_PARTICIPANTS];
	ClusterWalSourceRef sources[RF_PAGE_STABLE_MAX_PARTICIPANTS];
	XLogRecPtr redo_starts[RF_PAGE_STABLE_MAX_PARTICIPANTS];
	RfPageProofDetailV1 detail;
	uint64 database_incarnation = 0;
	uint64 record_count = 0;
	uint32 i;

	if (out_plan == NULL || out_record_count == NULL)
		return RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT;
	*out_plan = NULL;
	*out_record_count = 0;
	if (!cluster_shared_config || authorities == NULL || count == 0
		|| count > RF_PAGE_STABLE_MAX_PARTICIPANTS)
		return RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT;
	memset(cuts, 0, sizeof(cuts));
	for (i = 0; i < count; i++) {
		const ClusterThreadRecoveryAuthorityV1 *authority = &authorities[i];
		const ClusterControlRootSnapshot *root = authority->root_snapshot;

		if (authority->duty == NULL || root == NULL || authority->root_token == NULL
			|| !fabric_scan_root_exact(authority->duty->origin_thread_id,
									   root->checkpoint_lower_lsn,
									   root->validated_tail_lsn_exclusive, authority)
			|| cluster_thread_recovery_authority_revalidate_nowait_v1(authority)
				   != CLUSTER_THREAD_AUTHORITY_OK)
			return RF_PAGE_PROOF_DETAIL_ROOT_STALE;
		if (i > 0
			&& (authority->duty->origin_thread_id <= authorities[i - 1].duty->origin_thread_id
				|| authority->duty->system_identifier != authorities[0].duty->system_identifier
				|| memcmp(authority->duty->storage_uuid, authorities[0].duty->storage_uuid, 16)
					   != 0))
			return RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH;
		if (authority->retention_pin != authorities[0].retention_pin
			|| !cluster_thread_recovery_authority_covers_window_v1(
				authority, authority->duty->origin_thread_id, root->checkpoint_lower_lsn,
				root->validated_tail_lsn_exclusive))
			return RF_PAGE_PROOF_DETAIL_RETENTION_STALE;
		cuts[i].failed_thread = authority->duty->origin_thread_id;
		cuts[i].origin_owner_incarnation = authority->duty->origin_owner_incarnation;
		cuts[i].timeline_id = root->checkpoint_tli;
		cuts[i].flags = RF_CONTRIBUTOR_CUT_COMPLETE;
		cuts[i].scan_begin_inclusive = root->checkpoint_lower_lsn;
		cuts[i].scan_end_exclusive = root->validated_tail_lsn_exclusive;
		if (cluster_control_root_recovery_source_v1(root, authority->root_token, &sources[i],
													&redo_starts[i])
			!= CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			return RF_PAGE_PROOF_DETAIL_ROOT_STALE;
		if (sources[i].claim.database_incarnation == 0
			|| (i > 0
				&& sources[i].claim.database_incarnation != sources[0].claim.database_incarnation))
			return RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH;
	}
	if (!fabric_scan_authorities_current(authorities, count))
		return RF_PAGE_PROOF_DETAIL_ROOT_STALE;
	database_incarnation = sources[0].claim.database_incarnation;
	request.system_identifier = authorities[0].duty->system_identifier;
	memcpy(request.storage_uuid, authorities[0].duty->storage_uuid, 16);
	request.physical_cuts = cuts;
	request.sources = sources;
	request.redo_starts = redo_starts;
	request.participant_count = count;
	request.retention_binding_cookie = (uint64)(uintptr_t)authorities[0].retention_pin;
	request.space_active = space_active;
	detail = cluster_thread_recovery_fabric_plan_create_v1(&request, &plan);
	if (detail != RF_PAGE_PROOF_DETAIL_OK)
		return detail;
	PG_TRY();
	{
		for (i = 0; i < count; i++) {
			const ClusterThreadRecoveryAuthorityV1 *authority = &authorities[i];
			FabricVisitWork visit
				= { authorities, count, (uint16)i, plan, RF_PAGE_PROOF_DETAIL_OK, 0 };
			ClusterWalTailObservation observed;
			ClusterControlRootResult source;

			source = cluster_control_root_recovery_visit(authority->root_snapshot,
														 authority->root_token, fabric_visit_record,
														 &visit, &observed);
			if (source != CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
				detail = visit.detail == RF_PAGE_PROOF_DETAIL_OK ? RF_PAGE_PROOF_DETAIL_SOURCE_GAP
																 : visit.detail;
				break;
			}
			if (visit.records == 0 || visit.records != observed.records
				|| observed.complete_end != cuts[i].scan_end_exclusive) {
				detail = RF_PAGE_PROOF_DETAIL_SOURCE_GAP;
				break;
			}
			if (observed.database_incarnation == 0
				|| (database_incarnation != 0
					&& database_incarnation != observed.database_incarnation)) {
				detail = RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH;
				break;
			}
			database_incarnation = observed.database_incarnation;
			if (record_count > UINT64_MAX - visit.records) {
				detail = RF_PAGE_PROOF_DETAIL_CAPACITY;
				break;
			}
			record_count += visit.records;
		}
		if (detail == RF_PAGE_PROOF_DETAIL_OK
			&& !fabric_scan_authorities_current(authorities, count))
			detail = RF_PAGE_PROOF_DETAIL_ROOT_STALE;
		if (detail == RF_PAGE_PROOF_DETAIL_OK
			&& !cluster_thread_recovery_fabric_bind_database_v1(plan, database_incarnation))
			detail = RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH;
		if (detail == RF_PAGE_PROOF_DETAIL_OK)
			detail = cluster_thread_recovery_fabric_plan_seal_v1(plan);
		if (detail == RF_PAGE_PROOF_DETAIL_OK
			&& !fabric_scan_authorities_current(authorities, count))
			detail = RF_PAGE_PROOF_DETAIL_ROOT_STALE;
	}
	PG_CATCH();
	{
		cluster_thread_recovery_fabric_plan_destroy_v1(&plan);
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (detail != RF_PAGE_PROOF_DETAIL_OK) {
		cluster_thread_recovery_fabric_plan_destroy_v1(&plan);
		return detail;
	}
	*out_plan = plan;
	*out_record_count = record_count;
	return RF_PAGE_PROOF_DETAIL_OK;
}

RfPageProofDetailV1
cluster_thread_recovery_fabric_scan_root_v1(uint16 dead_thread, XLogRecPtr scan_begin_inclusive,
											XLogRecPtr scan_end_exclusive,
											const ClusterThreadRecoveryAuthorityV1 *authority,
											bool space_active,
											ClusterThreadRecoveryFabricPlanV1 **out_plan,
											uint64 *out_record_count)
{
	ClusterThreadRecoveryFabricPlanRequestV1 request;
	ClusterThreadRecoveryFabricPlanV1 *plan = NULL;
	RfContributorStreamCutV1 cut;
	RfPageProofDetailV1 detail;
	XLogReaderState *reader = NULL;
	void *reader_private = NULL;
	char *error_message = NULL;
	uint64 record_count = 0;
	bool reached_upper = false;

	if (out_plan == NULL || out_record_count == NULL)
		return RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT;
	*out_plan = NULL;
	*out_record_count = 0;
	if (cluster_thread_recovery_authority_revalidate_nowait_v1(authority)
			!= CLUSTER_THREAD_AUTHORITY_OK
		|| !fabric_scan_root_exact(dead_thread, scan_begin_inclusive, scan_end_exclusive,
								   authority))
		return RF_PAGE_PROOF_DETAIL_ROOT_STALE;
	if (!cluster_thread_recovery_authority_covers_window_v1(
			authority, dead_thread, scan_begin_inclusive, scan_end_exclusive))
		return RF_PAGE_PROOF_DETAIL_RETENTION_STALE;
	if (cluster_shared_config)
		return cluster_thread_recovery_fabric_scan_roots_v1(authority, 1, space_active, out_plan,
															out_record_count);

	memset(&cut, 0, sizeof(cut));
	cut.failed_thread = dead_thread;
	cut.flags = RF_CONTRIBUTOR_CUT_COMPLETE;
	cut.timeline_id = authority->root_snapshot->tail_tli;
	cut.scan_begin_inclusive = scan_begin_inclusive;
	cut.scan_end_exclusive = scan_end_exclusive;
	memset(&request, 0, sizeof(request));
	request.system_identifier = authority->duty->system_identifier;
	memcpy(request.storage_uuid, authority->duty->storage_uuid, 16);
	request.physical_cuts = &cut;
	request.participant_count = 1;
	request.retention_binding_cookie = (uint64)(uintptr_t)authority->retention_pin;
	request.space_active = space_active;
	detail = cluster_thread_recovery_fabric_plan_create_v1(&request, &plan);
	if (detail != RF_PAGE_PROOF_DETAIL_OK)
		return detail;
	reader = cluster_thread_wal_reader_make(dead_thread, &reader_private);
	if (reader == NULL || reader_private == NULL) {
		detail = RF_PAGE_PROOF_DETAIL_SOURCE_GAP;
		goto done;
	}
	reader->seg.ws_tli = cut.timeline_id;
	XLogBeginRead(reader, scan_begin_inclusive);
	for (;;) {
		XLogRecord *record = XLogReadRecord(reader, &error_message);

		if (record == NULL) {
			detail = RF_PAGE_PROOF_DETAIL_SOURCE_GAP;
			break;
		}
		if ((record_count == 0 && reader->ReadRecPtr != scan_begin_inclusive)
			|| reader->ReadRecPtr < scan_begin_inclusive || reader->EndRecPtr <= reader->ReadRecPtr
			|| reader->EndRecPtr > scan_end_exclusive) {
			detail = RF_PAGE_PROOF_DETAIL_SOURCE_GAP;
			break;
		}
		detail = cluster_thread_recovery_fabric_plan_feed_record_v1(plan, reader, 0);
		if (detail != RF_PAGE_PROOF_DETAIL_OK)
			break;
		if (record_count == UINT64_MAX) {
			detail = RF_PAGE_PROOF_DETAIL_CAPACITY;
			break;
		}
		record_count++;
		if (reader->EndRecPtr == scan_end_exclusive) {
			reached_upper = true;
			break;
		}
	}
	if (!reached_upper)
		goto done;
	if (cluster_thread_recovery_authority_revalidate_nowait_v1(authority)
			!= CLUSTER_THREAD_AUTHORITY_OK
		|| !cluster_thread_recovery_authority_covers_window_v1(
			authority, dead_thread, scan_begin_inclusive, scan_end_exclusive)) {
		detail = RF_PAGE_PROOF_DETAIL_RETENTION_STALE;
		goto done;
	}
	detail = cluster_thread_recovery_fabric_plan_seal_v1(plan);
	if (detail != RF_PAGE_PROOF_DETAIL_OK)
		goto done;
	*out_plan = plan;
	*out_record_count = record_count;
	plan = NULL;

done:
	if (reader != NULL)
		cluster_thread_wal_reader_free(reader, reader_private);
	if (plan != NULL)
		cluster_thread_recovery_fabric_plan_destroy_v1(&plan);
	return detail;
}

#endif
