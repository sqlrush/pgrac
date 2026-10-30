/*-------------------------------------------------------------------------
 * test_cluster_cold_native_side.c
 *     Original immutable SIDE plan through Startup's native storage tests.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_cold_native_side.c
 * NOTES
 *     Only names and allocation boundary differ from the original plan.
 *     The caller's fence/ROOT capture remains controlled, not a native grant.
 *-------------------------------------------------------------------------
 */
#include "test_cluster_cold_native_side_namespace.h"
#define RF_SIDE_ONLINE_TESTING 1
#include "../../backend/cluster/cluster_side_online_plan.c"
#include "test_cluster_cold_native_side.h"

struct TestColdNativeSide {
	RfSideOnlinePlanV1 *plan;
};

TestColdNativeSide *
test_cold_native_side_create(const ClusterWalSourceRef *sources, uint32 count,
	const RfContributorStreamCutV1 *cut, XLogRecPtr redo, XLogReaderState *reader)
{
	TestColdNativeSide *side;
	RfSideOnlinePlanRequestV1 request = {0};
	RfPageOnlineRecordIdentityV1 identity = {0};
	RfDetachedRecordPlanV1 record = {0};

	if (sources == NULL || count == 0 || cut == NULL || reader == NULL || reader->record == NULL)
		return NULL;
	side = calloc(1, sizeof(*side));
	if (side == NULL)
		return NULL;
	request.system_identifier = sources[0].claim.identity.system_identifier;
	memcpy(request.storage_uuid, sources[0].claim.identity.storage_uuid, 16);
	request.physical_cuts = cut;
	request.participant_count = 1;
	request.redo_starts = &redo;
	request.purpose = RF_SIDE_PLAN_RECOVERY;
	identity.record.system_identifier = reader->system_identifier;
	memcpy(identity.record.storage_uuid, sources[0].claim.identity.storage_uuid, 16);
	identity.record.origin_thread = reader->cluster_expected_thread_id;
	identity.record.timeline_id = reader->seg.ws_tli;
	identity.record.read_rec_ptr = reader->ReadRecPtr;
	identity.record.end_rec_ptr = reader->EndRecPtr;
	identity.record.record_crc = (uint32)reader->record->header.xl_crc;
	identity.record.rmid = XLogRecGetRmid(reader);
	identity.record.info = XLogRecGetInfo(reader);
	record.source_record = reader;
	record.preflight_complete = true;
	if (rf_opcode_route_lookup_v1(XLogRecGetRmid(reader), XLogRecGetInfo(reader),
		XLogRecHasAnyBlockRefs(reader), false, &record.route) != RF_OPCODE_ROUTE_OK
		|| rf_side_online_plan_create_v1(&request, &side->plan) != RF_PAGE_PROOF_DETAIL_OK
		|| !rf_side_online_plan_bind_database_v1(side->plan, sources[0].claim.database_incarnation)
		|| !rf_side_online_plan_bind_sources_v1(side->plan, sources, count)
		|| rf_side_online_plan_feed_record_v1(side->plan, &record, &identity) != RF_PAGE_PROOF_DETAIL_OK
		|| rf_side_online_plan_seal_v1(side->plan) != RF_PAGE_PROOF_DETAIL_OK
		|| rf_side_online_plan_native_clog_count_v1(side->plan, &sources[0]) != 1) {
		test_cold_native_side_destroy(&side);
		return NULL;
	}
	return side;
}

void
test_cold_native_side_destroy(TestColdNativeSide **side)
{
	if (side == NULL || *side == NULL)
		return;
	rf_side_online_plan_destroy_v1(&(*side)->plan);
	free(*side);
	*side = NULL;
}

bool
test_cold_native_side_matches(const TestColdNativeSide *side,
	const ClusterWalSourceRef *source, const XLogReaderState *reader)
{
	return side != NULL && rf_side_online_plan_clog_matches_v1(side->plan, source, reader);
}

bool
test_cold_native_side_replay(const TestColdNativeSide *side,
	const ClusterWalSourceRef *source, XLogReaderState *reader,
	void (*redo)(XLogReaderState *, void *), bool (*current)(void *), void *arg)
{
	return side != NULL && side_plan_clog_native_replay(side->plan, source, reader, redo, current, arg);
}

bool
test_cold_native_side_consumed(const TestColdNativeSide *side,
	const ClusterWalSourceRef *source, const XLogReaderState *reader)
{
	return side != NULL && rf_side_online_plan_clog_native_consumed_v1(side->plan, source, reader);
}

bool
test_cold_native_side_applied(const TestColdNativeSide *side,
	const ClusterWalSourceRef *source, const XLogReaderState *reader)
{
	return side != NULL && rf_side_online_plan_clog_applied_v1(side->plan, source, reader);
}
