/*-------------------------------------------------------------------------
 * cluster_thread_recovery_fabric_scan.c
 *    Decode one exact ROOT-owned WAL cut into an immutable recovery fabric.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "access/xlogreader.h"
#include "cluster/cluster_thread_recovery.h"
#include "cluster/cluster_thread_recovery_authority.h"
#include "cluster/cluster_thread_recovery_fabric.h"

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
