/*-------------------------------------------------------------------------
 * cluster_side_online_plan.c
 *    RF-SIDE immutable online operation plan implementation.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "access/clog.h"
#include "access/commit_ts.h"
#include "access/multixact.h"
#include "catalog/storage_xlog.h"
#include "cluster/cluster_side_online_plan.h"
#include "cluster/storage/cluster_undo_alloc.h"

#ifdef RF_SIDE_ONLINE_TESTING
#include <stdlib.h>
#define side_alloc0(bytes_) calloc(1, (bytes_))
#define side_realloc(ptr_, bytes_) realloc((ptr_), (bytes_))
#define side_free(ptr_) free((ptr_))
#else
#define side_alloc0(bytes_) palloc0(bytes_)
#define side_realloc(ptr_, bytes_) repalloc((ptr_), (bytes_))
#define side_free(ptr_) pfree(ptr_)
#endif

#define RF_SIDE_ONLINE_PLAN_MAGIC UINT32_C(0x53495031)

struct RfSideOnlinePlanV1 {
	uint32 magic;
	bool sealed;
	uint8 reserved5[3];
	uint64 system_identifier;
	uint64 database_incarnation;
	uint8 storage_uuid[16];
	Size memory_budget;
	Size memory_used;
	uint32 participant_count;
	RfContributorStreamCutV1 *physical_cuts;
	XLogRecPtr *last_record_end;
	bool *participant_seen;
	RfSideOnlineOperationV1 *operations;
	uint32 operation_count;
	uint32 operation_capacity;
	uint8 *owned_payload;
	uint32 owned_payload_bytes;
	uint32 owned_payload_capacity;
};

static bool
side_bytes_nonzero(const uint8 *bytes, Size length)
{
	uint8 seen = 0;
	Size i;

	for (i = 0; i < length; i++)
		seen |= bytes[i];
	return seen != 0;
}

static bool
side_plan_reserve(RfSideOnlinePlanV1 *plan, Size bytes)
{
	if (plan->memory_used > plan->memory_budget - Min(plan->memory_budget, bytes))
		return false;
	plan->memory_used += bytes;
	return true;
}

static RfPageProofDetailV1
side_record_identity_validate(RfSideOnlinePlanV1 *plan, const RfDetachedRecordPlanV1 *record_plan,
							  const RfPageOnlineRecordIdentityV1 *identity)
{
	const RfContributorStreamCutV1 *cut;
	const RfPageReplayRecordIdentityV1 *record;
	const XLogReaderState *reader;
	const DecodedXLogRecord *decoded;

	if (record_plan == NULL || !record_plan->preflight_complete
		|| record_plan->source_record == NULL || record_plan->source_record->record == NULL
		|| identity == NULL)
		return RF_PAGE_PROOF_DETAIL_SOURCE_GAP;
	reader = record_plan->source_record;
	decoded = reader->record;
	record = &identity->record;
	if (identity->reserved_zero != 0 || record->reserved_zero != 0 || record->reserved_zero2 != 0
		|| identity->participant_index >= plan->participant_count
		|| record->system_identifier != plan->system_identifier
		|| memcmp(record->storage_uuid, plan->storage_uuid, 16) != 0 || record->origin_thread == 0
		|| record->timeline_id == 0 || record->read_rec_ptr == InvalidXLogRecPtr
		|| record->end_rec_ptr <= record->read_rec_ptr
		|| reader->system_identifier != record->system_identifier
		|| reader->ReadRecPtr != record->read_rec_ptr || reader->EndRecPtr != record->end_rec_ptr
		|| decoded->lsn != record->read_rec_ptr || decoded->next_lsn != record->end_rec_ptr
		|| (uint32)decoded->header.xl_crc != record->record_crc
		|| decoded->header.xl_rmid != record->rmid || decoded->header.xl_info != record->info)
		return RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH;
	cut = &plan->physical_cuts[identity->participant_index];
	if (record->origin_thread != cut->failed_thread || record->timeline_id != cut->timeline_id
		|| (cut->flags & RF_CONTRIBUTOR_CUT_EXPLICIT_EMPTY) != 0
		|| record->read_rec_ptr < cut->scan_begin_inclusive
		|| record->end_rec_ptr > cut->scan_end_exclusive
		|| (plan->participant_seen[identity->participant_index]
			&& record->read_rec_ptr < plan->last_record_end[identity->participant_index]))
		return RF_PAGE_PROOF_DETAIL_SOURCE_GAP;
	return RF_PAGE_PROOF_DETAIL_OK;
}

static bool
side_ensure_operation_capacity(RfSideOnlinePlanV1 *plan)
{
	RfSideOnlineOperationV1 *operations;
	uint32 capacity;
	Size delta;

	if (plan->operation_count < plan->operation_capacity)
		return true;
	capacity = plan->operation_capacity == 0 ? 16 : plan->operation_capacity * 2;
	if (capacity < plan->operation_capacity)
		return false;
	delta = (Size)(capacity - plan->operation_capacity) * sizeof(*operations);
	if (!side_plan_reserve(plan, delta))
		return false;
	if (plan->operations == NULL)
		operations = (RfSideOnlineOperationV1 *)side_alloc0((Size)capacity * sizeof(*operations));
	else {
		operations = (RfSideOnlineOperationV1 *)side_realloc(plan->operations,
															 (Size)capacity * sizeof(*operations));
		if (operations != NULL)
			memset(operations + plan->operation_capacity, 0, delta);
	}
	if (operations == NULL) {
		plan->memory_used -= delta;
		return false;
	}
	plan->operations = operations;
	plan->operation_capacity = capacity;
	return true;
}

static bool
side_ensure_payload_capacity(RfSideOnlinePlanV1 *plan, uint32 additional)
{
	uint8 *payload;
	uint32 needed;
	uint32 capacity;
	Size delta;

	if (additional == 0)
		return true;
	if (additional > UINT32_MAX - plan->owned_payload_bytes)
		return false;
	needed = plan->owned_payload_bytes + additional;
	if (needed <= plan->owned_payload_capacity)
		return true;
	capacity = plan->owned_payload_capacity == 0 ? BLCKSZ : plan->owned_payload_capacity;
	while (capacity < needed) {
		if (capacity > UINT32_MAX / 2) {
			capacity = needed;
			break;
		}
		capacity *= 2;
	}
	delta = (Size)capacity - plan->owned_payload_capacity;
	if (!side_plan_reserve(plan, delta))
		return false;
	if (plan->owned_payload == NULL)
		payload = (uint8 *)side_alloc0(capacity);
	else
		payload = (uint8 *)side_realloc(plan->owned_payload, capacity);
	if (payload == NULL) {
		plan->memory_used -= delta;
		return false;
	}
	plan->owned_payload = payload;
	plan->owned_payload_capacity = capacity;
	return true;
}

static bool
side_space_decode(const RfSideOnlinePlanV1 *plan, const RfDetachedRecordPlanV1 *record_plan,
				  RfSideOnlineOperationV1 *candidate)
{
	XLogReaderState *record = record_plan->source_record;
	uint8 info = record_plan->route.normalized_info;
	ClusterSpaceIdentityKey key;

	if (XLogRecHasAnyBlockRefs(record))
		return false;
	if (info == XLOG_SMGR_SPACE_IDENTITY) {
		ClusterSpaceStructureChange change;

		if (!cluster_space_structure_wal_decode(XLogRecGetData(record), XLogRecGetDataLen(record),
			&change) || change.identity.action == CLUSTER_SPACE_WAL_TOMBSTONE)
			return false;
		key = change.identity.result.key;
	} else if (info == XLOG_SMGR_SPACE_RESERVATION) {
		ClusterSpaceReservationChange change;

		if (!cluster_space_reservation_wal_decode(XLogRecGetData(record), XLogRecGetDataLen(record),
			&change) || change.action != CLUSTER_SPACE_RESERVATION_ADVANCE)
			return false;
		key = change.result.identity.key;
	} else
		return false;
	if (key.system_identifier != plan->system_identifier
		|| memcmp(key.storage_uuid, plan->storage_uuid, 16) != 0
		|| (plan->database_incarnation != 0 && key.database_incarnation != plan->database_incarnation))
		return false;
	candidate->kind = RF_SIDE_ONLINE_OPERATION_SPACE;
	candidate->space_key = key;
	candidate->owned_payload_length = XLogRecGetDataLen(record);
	return true;
}

static bool
side_projection_decode(const RfDetachedRecordPlanV1 *record_plan,
					   RfSideOnlineOperationV1 *candidate, uint32 *payload_offset,
					   uint32 *payload_length)
{
	const XLogReaderState *record = record_plan->source_record;
	const char *data = XLogRecGetData(record);
	uint32 data_length = XLogRecGetDataLen(record);
	uint8 info = record_plan->route.normalized_info;
	ClusterSideProjectionOperationV1 *projection = &candidate->projection;

	*payload_offset = 0;
	*payload_length = 0;
	projection->normalized_info = info;
	projection->page_number = -1;
	if (record_plan->route.rmid == RM_CLOG_ID) {
		projection->kind = CLUSTER_SIDE_PROJECTION_CLOG;
		if (info == CLOG_ZEROPAGE) {
			if (data_length != sizeof(int))
				return false;
			memcpy(&projection->page_number, data, sizeof(int));
			projection->action = CLUSTER_SIDE_PROJECTION_ACTION_ZERO_PAGE;
			return projection->page_number >= 0;
		}
		if (info == CLOG_TRUNCATE) {
			xl_clog_truncate trunc;

			if (data_length != sizeof(trunc))
				return false;
			memcpy(&trunc, data, sizeof(trunc));
			projection->action = CLUSTER_SIDE_PROJECTION_ACTION_TRUNCATE;
			projection->page_number = trunc.pageno;
			projection->oldest_xid = trunc.oldestXact;
			projection->oldest_database = trunc.oldestXactDb;
			return trunc.pageno >= 0 && TransactionIdIsNormal(trunc.oldestXact)
				   && OidIsValid(trunc.oldestXactDb);
		}
		return false;
	}
	if (record_plan->route.rmid == RM_COMMIT_TS_ID) {
		projection->kind = CLUSTER_SIDE_PROJECTION_COMMIT_TS;
		if (info == COMMIT_TS_ZEROPAGE) {
			if (data_length != sizeof(int))
				return false;
			memcpy(&projection->page_number, data, sizeof(int));
			projection->action = CLUSTER_SIDE_PROJECTION_ACTION_ZERO_PAGE;
			return projection->page_number >= 0;
		}
		if (info == COMMIT_TS_TRUNCATE) {
			xl_commit_ts_truncate trunc;

			if (data_length != SizeOfCommitTsTruncate)
				return false;
			memset(&trunc, 0, sizeof(trunc));
			memcpy(&trunc, data, SizeOfCommitTsTruncate);
			projection->action = CLUSTER_SIDE_PROJECTION_ACTION_TRUNCATE;
			projection->page_number = trunc.pageno;
			projection->oldest_xid = trunc.oldestXid;
			return trunc.pageno >= 0 && TransactionIdIsNormal(trunc.oldestXid);
		}
		return false;
	}
	if (record_plan->route.rmid == RM_MULTIXACT_ID) {
		projection->kind = CLUSTER_SIDE_PROJECTION_MULTIXACT;
		if (info == XLOG_MULTIXACT_ZERO_OFF_PAGE || info == XLOG_MULTIXACT_ZERO_MEM_PAGE) {
			if (data_length != sizeof(int))
				return false;
			memcpy(&projection->page_number, data, sizeof(int));
			projection->action = CLUSTER_SIDE_PROJECTION_ACTION_ZERO_PAGE;
			return projection->page_number >= 0;
		}
		if (info == XLOG_MULTIXACT_CREATE_ID) {
			xl_multixact_create header;
			Size expected;
			int i;

			if (data_length < SizeOfMultiXactCreate)
				return false;
			memset(&header, 0, sizeof(header));
			memcpy(&header, data, SizeOfMultiXactCreate);
			if (header.nmembers <= 0 || header.nmembers > 256 || !MultiXactIdIsValid(header.mid)
				|| header.moff == 0)
				return false;
			expected = SizeOfMultiXactCreate + (Size)header.nmembers * sizeof(MultiXactMember);
			if (expected != data_length)
				return false;
			for (i = 0; i < header.nmembers; i++) {
				MultiXactMember member;

				memcpy(&member, data + SizeOfMultiXactCreate + (Size)i * sizeof(member),
					   sizeof(member));
				if (!TransactionIdIsNormal(member.xid) || member.status < MultiXactStatusForKeyShare
					|| member.status > MaxMultiXactStatus)
					return false;
			}
			projection->action = CLUSTER_SIDE_PROJECTION_ACTION_CREATE;
			projection->multixact_id = header.mid;
			projection->member_offset = header.moff;
			projection->member_count = (uint32)header.nmembers;
			*payload_offset = SizeOfMultiXactCreate;
			*payload_length = data_length - SizeOfMultiXactCreate;
			return true;
		}
		if (info == XLOG_MULTIXACT_TRUNCATE_ID) {
			xl_multixact_truncate trunc;

			if (data_length != SizeOfMultiXactTruncate)
				return false;
			memcpy(&trunc, data, sizeof(trunc));
			projection->action = CLUSTER_SIDE_PROJECTION_ACTION_TRUNCATE;
			projection->oldest_database = trunc.oldestMultiDB;
			projection->truncate_start_multixact = trunc.startTruncOff;
			projection->truncate_end_multixact = trunc.endTruncOff;
			projection->truncate_start_member = trunc.startTruncMemb;
			projection->truncate_end_member = trunc.endTruncMemb;
			return OidIsValid(trunc.oldestMultiDB) && MultiXactIdIsValid(trunc.startTruncOff)
				   && MultiXactIdIsValid(trunc.endTruncOff);
		}
	}
	return false;
}

static bool
side_plan_commit_prepared_dependencies_closed(const RfSideOnlinePlanV1 *plan, uint32 terminal_index)
{
	const RfSideOnlineOperationV1 *terminal = &plan->operations[terminal_index];
	RfSideXactCommitPreparedRequirementsV1 requirements;
	uint16 i;

	if (rf_side_xact_commit_prepared_requirements_v1(&terminal->xact, &requirements)
		!= RF_SIDE_XACT_APPLY_OK)
		return false;
	for (i = 0; i < requirements.count; i++) {
		const RfSideXactTTCommitRequirementV1 *required = &requirements.bindings[i];
		uint32 matches = 0;
		uint32 j;

		if (!required->requires_apply)
			continue;
		for (j = 0; j < terminal_index; j++) {
			const RfSideOnlineOperationV1 *candidate = &plan->operations[j];
			const ClusterUndoDecoded *undo = &candidate->undo;

			if (candidate->kind == RF_SIDE_ONLINE_OPERATION_UNDO
				&& candidate->identity.participant_index == terminal->identity.participant_index
				&& undo->kind == CLUSTER_UNDO_KIND_TT_COMMIT && undo->instance == required->instance
				&& undo->segment_id == required->segment_id
				&& undo->slot_offset == required->slot_offset && undo->wrap == required->wrap
				&& undo->xid == required->xid && undo->commit_scn == required->commit_scn)
				matches++;
		}
		if (matches != 1)
			return false;
	}
	return true;
}

static bool
side_plan_abort_prepared_dependencies_closed(const RfSideOnlinePlanV1 *plan, uint32 terminal_index)
{
	const RfSideOnlineOperationV1 *terminal = &plan->operations[terminal_index];
	RfSideXactAbortPreparedRequirementsV1 requirements;
	uint16 i;

	if (rf_side_xact_abort_prepared_requirements_v1(&terminal->xact, &requirements)
		!= RF_SIDE_XACT_APPLY_OK)
		return false;
	for (i = 0; i < requirements.count; i++) {
		const RfSideXactTTAbortRequirementV1 *required = &requirements.bindings[i];
		uint32 matches = 0;
		uint32 j;

		if (!required->requires_apply)
			continue;
		for (j = 0; j < terminal_index; j++) {
			const RfSideOnlineOperationV1 *candidate = &plan->operations[j];
			const ClusterUndoDecoded *undo = &candidate->undo;
			if (candidate->kind != RF_SIDE_ONLINE_OPERATION_UNDO
				|| candidate->identity.participant_index != terminal->identity.participant_index)
				continue;
			if (undo->kind == CLUSTER_UNDO_KIND_TT_ABORT && undo->instance == required->instance
				&& undo->segment_id == required->segment_id
				&& undo->slot_offset == required->slot_offset && undo->wrap == required->wrap
				&& undo->xid == required->xid)
				matches++;
		}
		if (matches != 1)
			return false;
	}
	return true;
}

RfPageProofDetailV1
rf_side_online_plan_create_v1(const RfSideOnlinePlanRequestV1 *request,
							  RfSideOnlinePlanV1 **out_plan)
{
	RfSideOnlinePlanV1 *plan;
	Size budget;
	Size arrays_size;
	uint32 i;

	if (out_plan == NULL)
		return RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT;
	*out_plan = NULL;
	if (request == NULL || request->system_identifier == 0
		|| !side_bytes_nonzero(request->storage_uuid, 16) || request->physical_cuts == NULL
		|| request->participant_count == 0
		|| request->participant_count > RF_PAGE_STABLE_MAX_PARTICIPANTS)
		return RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT;
	budget = request->memory_budget == 0 ? RF_SIDE_ONLINE_PLAN_MAX_BYTES : request->memory_budget;
	if (budget > RF_SIDE_ONLINE_PLAN_MAX_BYTES || budget < sizeof(*plan))
		return RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT;
	for (i = 0; i < request->participant_count; i++) {
		const RfContributorStreamCutV1 *cut = &request->physical_cuts[i];
		bool empty = (cut->flags & RF_CONTRIBUTOR_CUT_EXPLICIT_EMPTY) != 0;

		if (cut->failed_thread == 0 || cut->timeline_id == 0
			|| (cut->flags & RF_CONTRIBUTOR_CUT_COMPLETE) == 0
			|| (cut->flags & ~RF_CONTRIBUTOR_CUT_KNOWN_MASK) != 0
			|| (empty && cut->scan_begin_inclusive != cut->scan_end_exclusive)
			|| (!empty && cut->scan_begin_inclusive >= cut->scan_end_exclusive)
			|| (i > 0 && request->physical_cuts[i - 1].failed_thread >= cut->failed_thread))
			return RF_PAGE_PROOF_DETAIL_PARTICIPANT_MISSING;
	}
	arrays_size = (Size)request->participant_count
				  * (sizeof(*plan->physical_cuts) + sizeof(*plan->last_record_end)
					 + sizeof(*plan->participant_seen));
	if (sizeof(*plan) > budget - Min(budget, arrays_size))
		return RF_PAGE_PROOF_DETAIL_CAPACITY;
	plan = (RfSideOnlinePlanV1 *)side_alloc0(sizeof(*plan));
	if (plan == NULL)
		return RF_PAGE_PROOF_DETAIL_OOM;
	plan->memory_budget = budget;
	plan->memory_used = sizeof(*plan) + arrays_size;
	plan->physical_cuts = (RfContributorStreamCutV1 *)side_alloc0((Size)request->participant_count
																  * sizeof(*plan->physical_cuts));
	plan->last_record_end = (XLogRecPtr *)side_alloc0((Size)request->participant_count
													  * sizeof(*plan->last_record_end));
	plan->participant_seen
		= (bool *)side_alloc0((Size)request->participant_count * sizeof(*plan->participant_seen));
	if (plan->physical_cuts == NULL || plan->last_record_end == NULL
		|| plan->participant_seen == NULL) {
		rf_side_online_plan_destroy_v1(&plan);
		return RF_PAGE_PROOF_DETAIL_OOM;
	}
	memcpy(plan->physical_cuts, request->physical_cuts,
		   (Size)request->participant_count * sizeof(*plan->physical_cuts));
	plan->magic = RF_SIDE_ONLINE_PLAN_MAGIC;
	plan->system_identifier = request->system_identifier;
	memcpy(plan->storage_uuid, request->storage_uuid, 16);
	plan->participant_count = request->participant_count;
	*out_plan = plan;
	return RF_PAGE_PROOF_DETAIL_OK;
}

RfPageProofDetailV1
rf_side_online_plan_feed_record_v1(RfSideOnlinePlanV1 *plan,
								   const RfDetachedRecordPlanV1 *record_plan,
								   const RfPageOnlineRecordIdentityV1 *identity)
{
	RfSideOnlineOperationV1 candidate;
	RfPageProofDetailV1 detail;
	uint16 participant;

	if (plan == NULL || plan->magic != RF_SIDE_ONLINE_PLAN_MAGIC || plan->sealed)
		return RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT;
	detail = side_record_identity_validate(plan, record_plan, identity);
	if (detail != RF_PAGE_PROOF_DETAIL_OK)
		return detail;
	memset(&candidate, 0, sizeof(candidate));
	if (record_plan->route.record_owner == RF_ROUTE_OWNER_SIDE_TYPED) {
		if (record_plan->route.rmid == RM_XACT_ID
			&& record_plan->route.codec_id == RF_ROUTE_CODEC_SIDE_STANDARD) {
			if (!rf_side_xact_decode_v1(record_plan->source_record, plan->system_identifier,
										identity->record.origin_thread, &candidate.xact))
				return RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
			candidate.kind = RF_SIDE_ONLINE_OPERATION_XACT;
			if (candidate.xact.kind == RF_SIDE_XACT_PREPARE) {
				uint32 record_length = XLogRecGetDataLen(record_plan->source_record);

				if (record_length == 0 || record_length != candidate.xact.prepare_payload_length
					|| !side_ensure_operation_capacity(plan)
					|| !side_ensure_payload_capacity(plan, record_length))
					return RF_PAGE_PROOF_DETAIL_CAPACITY;
				candidate.owned_payload_offset = plan->owned_payload_bytes;
				candidate.owned_payload_length = record_length;
				memcpy(plan->owned_payload + plan->owned_payload_bytes,
					   XLogRecGetData(record_plan->source_record), record_length);
				plan->owned_payload_bytes += record_length;
			}
		} else if (record_plan->route.rmid == RM_CLUSTER_UNDO_ID
				   && record_plan->route.codec_id == RF_ROUTE_CODEC_SIDE_CLUSTER_UNDO) {
			const char *record_data = XLogRecGetData(record_plan->source_record);
			uint32 record_length = XLogRecGetDataLen(record_plan->source_record);

			if (!cluster_undo_decode(record_plan->source_record, &candidate.undo)
				|| !cluster_undo_preflight(&candidate.undo)
				|| candidate.undo.instance != identity->record.origin_thread
				|| candidate.undo.payload_offset > record_length
				|| candidate.undo.payload_length > record_length - candidate.undo.payload_offset)
				return RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
			candidate.kind = RF_SIDE_ONLINE_OPERATION_UNDO;
			candidate.owned_payload_length = candidate.undo.payload_length;
			if (!side_ensure_operation_capacity(plan))
				return RF_PAGE_PROOF_DETAIL_CAPACITY;
			if (candidate.owned_payload_length > 0) {
				if (!side_ensure_payload_capacity(plan, candidate.owned_payload_length))
					return RF_PAGE_PROOF_DETAIL_CAPACITY;
				candidate.owned_payload_offset = plan->owned_payload_bytes;
				memcpy(plan->owned_payload + plan->owned_payload_bytes,
					   record_data + candidate.undo.payload_offset, candidate.owned_payload_length);
				plan->owned_payload_bytes += candidate.owned_payload_length;
			}
		} else if (record_plan->route.rmid == RM_SMGR_ID
				   && record_plan->route.codec_id == RF_ROUTE_CODEC_SIDE_STANDARD) {
			if (!side_space_decode(plan, record_plan, &candidate))
				return RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
			if (!side_ensure_operation_capacity(plan)
				|| !side_ensure_payload_capacity(plan, candidate.owned_payload_length))
				return RF_PAGE_PROOF_DETAIL_CAPACITY;
			candidate.owned_payload_offset = plan->owned_payload_bytes;
			memcpy(plan->owned_payload + plan->owned_payload_bytes,
				XLogRecGetData(record_plan->source_record), candidate.owned_payload_length);
			plan->owned_payload_bytes += candidate.owned_payload_length;
		} else if ((record_plan->route.rmid == RM_CLOG_ID
					|| record_plan->route.rmid == RM_MULTIXACT_ID
					|| record_plan->route.rmid == RM_COMMIT_TS_ID)
				   && record_plan->route.codec_id == RF_ROUTE_CODEC_SIDE_STANDARD) {
			uint32 payload_offset;
			uint32 payload_length;

			if (!side_projection_decode(record_plan, &candidate, &payload_offset, &payload_length))
				return RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
			if (!side_ensure_operation_capacity(plan))
				return RF_PAGE_PROOF_DETAIL_CAPACITY;
			candidate.kind = RF_SIDE_ONLINE_OPERATION_PROJECTION;
			candidate.owned_payload_length = payload_length;
			if (payload_length > 0) {
				if (!side_ensure_payload_capacity(plan, payload_length))
					return RF_PAGE_PROOF_DETAIL_CAPACITY;
				candidate.owned_payload_offset = plan->owned_payload_bytes;
				memcpy(plan->owned_payload + plan->owned_payload_bytes,
					   XLogRecGetData(record_plan->source_record) + payload_offset, payload_length);
				plan->owned_payload_bytes += payload_length;
			}
		} else
			return RF_PAGE_PROOF_DETAIL_OPCODE_UNSUPPORTED;
		candidate.identity = *identity;
		candidate.route = record_plan->route;
		if (!side_ensure_operation_capacity(plan))
			return RF_PAGE_PROOF_DETAIL_CAPACITY;
		plan->operations[plan->operation_count++] = candidate;
	} else if (record_plan->route.record_owner != RF_ROUTE_OWNER_PAGE_CODEC
			   && record_plan->route.record_owner != RF_ROUTE_OWNER_LOGICAL_NOOP)
		return RF_PAGE_PROOF_DETAIL_OPCODE_UNSUPPORTED;
	participant = identity->participant_index;
	plan->participant_seen[participant] = true;
	plan->last_record_end[participant] = identity->record.end_rec_ptr;
	return RF_PAGE_PROOF_DETAIL_OK;
}

bool
rf_side_online_plan_bind_database_v1(RfSideOnlinePlanV1 *plan, uint64 database_incarnation)
{
	if (plan == NULL || plan->magic != RF_SIDE_ONLINE_PLAN_MAGIC || plan->sealed
		|| database_incarnation == 0
		|| (plan->database_incarnation != 0 && plan->database_incarnation != database_incarnation))
		return false;
	for (uint32 i = 0; i < plan->operation_count; i++)
		if (plan->operations[i].kind == RF_SIDE_ONLINE_OPERATION_SPACE
			&& plan->operations[i].space_key.database_incarnation != database_incarnation)
			return false;
	plan->database_incarnation = database_incarnation;
	return true;
}

bool
rf_side_online_plan_source_matches_v1(const RfSideOnlinePlanV1 *plan,
	uint64 system_identifier, const uint8 storage_uuid[16], const RfContributorStreamCutV1 *cut)
{
	const RfContributorStreamCutV1 *source;

	if (plan == NULL || plan->magic != RF_SIDE_ONLINE_PLAN_MAGIC || !plan->sealed
		|| plan->database_incarnation == 0 || plan->participant_count != 1
		|| system_identifier != plan->system_identifier || storage_uuid == NULL || cut == NULL
		|| memcmp(storage_uuid, plan->storage_uuid, 16) != 0)
		return false;
	source = &plan->physical_cuts[0];
	return cut->flags == RF_CONTRIBUTOR_CUT_COMPLETE && cut->flags == source->flags
		&& cut->failed_thread == source->failed_thread && cut->timeline_id == source->timeline_id
		&& cut->scan_begin_inclusive == source->scan_begin_inclusive
		&& cut->scan_end_exclusive == source->scan_end_exclusive;
}

RfPageProofDetailV1
rf_side_online_plan_prepare_space_v1(const RfSideOnlinePlanV1 *plan,
	const ClusterSpaceIdentityKey *expected, const void *identity_page, const void *reservation_page,
	uint32 *order, uint32 capacity, uint32 *out_count, ClusterSpaceRecoveryImage *out)
{
	ClusterSpaceRecoveryInput *inputs = NULL;
	uint32 *indices = NULL, *sorted = NULL;
	ClusterSpaceRecoveryImage prepared;
	uint32 count = 0, next = 0;
	Size bytes;
	size_t scratch;
	RfPageProofDetailV1 detail = RF_PAGE_PROOF_DETAIL_VERSION_MISMATCH;

	if (plan == NULL || plan->magic != RF_SIDE_ONLINE_PLAN_MAGIC || !plan->sealed
		|| expected == NULL || identity_page == NULL || reservation_page == NULL
		|| order == NULL || out_count == NULL || out == NULL)
		return RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT;
	if (plan->database_incarnation == 0 || expected->database_incarnation != plan->database_incarnation
		|| expected->system_identifier != plan->system_identifier
		|| memcmp(expected->storage_uuid, plan->storage_uuid, 16) != 0)
		return RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH;
	for (uint32 i = 0; i < plan->operation_count; i++)
		if (plan->operations[i].kind == RF_SIDE_ONLINE_OPERATION_SPACE
			&& RelFileLocatorEquals(plan->operations[i].space_key.locator, expected->locator))
			count++;
	if (count == 0)
		return RF_PAGE_PROOF_DETAIL_COMPONENT_INCOMPLETE;
	bytes = (Size)count * (sizeof(*inputs) + sizeof(*indices) + sizeof(*sorted));
	scratch = cluster_space_recovery_scratch_bytes(count);
	if (scratch == 0 || scratch > SIZE_MAX - bytes)
		return RF_PAGE_PROOF_DETAIL_CAPACITY;
	bytes += scratch;
	if (count > capacity || bytes > plan->memory_budget
		|| plan->memory_used > plan->memory_budget - bytes)
		return RF_PAGE_PROOF_DETAIL_CAPACITY;
	inputs = side_alloc0((Size)count * sizeof(*inputs));
	indices = side_alloc0((Size)count * sizeof(*indices));
	sorted = side_alloc0((Size)count * sizeof(*sorted));
	if (inputs == NULL || indices == NULL || sorted == NULL) {
		detail = RF_PAGE_PROOF_DETAIL_OOM;
		goto done;
	}
	for (uint32 i = 0; i < plan->operation_count; i++) {
		const RfSideOnlineOperationV1 *op = &plan->operations[i];

		if (op->kind != RF_SIDE_ONLINE_OPERATION_SPACE
			|| !RelFileLocatorEquals(op->space_key.locator, expected->locator))
			continue;
		inputs[next] = (ClusterSpaceRecoveryInput){plan->owned_payload + op->owned_payload_offset,
			op->owned_payload_length};
		indices[next++] = i;
	}
	if (!cluster_space_recovery_prepare(inputs, count, expected, identity_page,
		reservation_page, sorted, &prepared))
		goto done;
	for (uint32 i = 0; i < count; i++)
		order[i] = indices[sorted[i]];
	for (uint32 i = 0; i < 2; i++)
		if (prepared.source_index[i] != UINT32_MAX)
			prepared.source_index[i] = indices[prepared.source_index[i]];
	*out = prepared;
	*out_count = count;
	detail = RF_PAGE_PROOF_DETAIL_OK;
done:
	if (sorted != NULL) side_free(sorted);
	if (indices != NULL) side_free(indices);
	if (inputs != NULL) side_free(inputs);
	return detail;
}

/* Return only TT slot operations on this exact segment. */
static bool
side_plan_slot_operation(const RfSideOnlineOperationV1 *op, uint8 instance,
	uint32 segment, uint16 *slot)
{
	if (op->kind == RF_SIDE_ONLINE_OPERATION_XACT
		&& op->xact.kind == RF_SIDE_XACT_COMMIT && op->xact.has_tt_delta
		&& op->xact.tt_delta.instance == instance && op->xact.tt_delta.segment_id == segment) {
		*slot = op->xact.tt_delta.slot_offset;
		return true;
	}
	if (op->kind != RF_SIDE_ONLINE_OPERATION_UNDO || op->undo.instance != instance
		|| op->undo.segment_id != segment
		|| (op->undo.kind != CLUSTER_UNDO_KIND_TT_BIND
			&& op->undo.kind != CLUSTER_UNDO_KIND_TT_COMMIT
			&& op->undo.kind != CLUSTER_UNDO_KIND_TT_ABORT
			&& op->undo.kind != CLUSTER_UNDO_KIND_TT_SET_HEAD
			&& op->undo.kind != CLUSTER_UNDO_KIND_TT_CTRC_RELEASE))
		return false;
	*slot = op->undo.slot_offset;
	return true;
}

bool
rf_side_online_plan_contains_commit_v1(const RfSideOnlinePlanV1 *plan,
	const RfSideXactOperationV1 *operation)
{
	if (plan == NULL || plan->magic != RF_SIDE_ONLINE_PLAN_MAGIC || !plan->sealed
		|| operation == NULL || operation->kind != RF_SIDE_XACT_COMMIT)
		return false;
	for (uint32 i = 0; i < plan->operation_count; i++)
		if (plan->operations[i].kind == RF_SIDE_ONLINE_OPERATION_XACT
			&& memcmp(&plan->operations[i].xact, operation, sizeof(*operation)) == 0)
			return true;
	return false;
}

static ClusterUndoHeaderPrepareResultV1
side_plan_slot_step(const RfSideOnlineOperationV1 *op, const char *base, char *out)
{
	if (op->owned_payload_length != 0)
		return CLUSTER_UNDO_HEADER_BLOCKED;
	if (op->kind == RF_SIDE_ONLINE_OPERATION_XACT) {
		const xl_xact_tt_commit *d = &op->xact.tt_delta;

		return cluster_undo_prepare_commit_v1(d->instance, d->segment_id,
			d->segment_generation, d->slot_offset, d->wrap, d->xid, d->commit_scn, base, out);
	} else {
		const ClusterUndoDecoded *d = &op->undo;
		const UndoSegmentHeaderData *h = (const UndoSegmentHeaderData *)base;

		if (d->slot_offset >= TT_SLOTS_PER_SEGMENT)
			return CLUSTER_UNDO_HEADER_BLOCKED;
		if ((d->kind == CLUSTER_UNDO_KIND_TT_COMMIT
			|| d->kind == CLUSTER_UNDO_KIND_TT_SET_HEAD
			|| (d->kind == CLUSTER_UNDO_KIND_TT_ABORT && d->format_version == 0))
			&& cluster_undo_preflight_legacy_slot_v1(d, &h->tt_slots[d->slot_offset])
				== CLUSTER_UNDO_TARGET_BLOCKED)
			return CLUSTER_UNDO_HEADER_BLOCKED;
		return cluster_undo_prepare_header_v1(d, NULL, 0, base, out);
	}
}

/* A slot anchor describes source result bytes, never a target predecessor.
 * The original DATA must separately be an admitted first predecessor or an
 * exact member of the complete source evolution below. */
static void
side_plan_slot_anchor(const RfSideOnlineOperationV1 *op, TTSlot *slot)
{
	static const UBA invalid_head = InvalidUba_init;

	if (op->kind == RF_SIDE_ONLINE_OPERATION_UNDO
		&& op->undo.kind == CLUSTER_UNDO_KIND_TT_BIND)
		memset(slot, 0, sizeof(*slot));
	else if (op->kind == RF_SIDE_ONLINE_OPERATION_XACT
		|| op->undo.kind == CLUSTER_UNDO_KIND_TT_COMMIT
		|| op->undo.kind == CLUSTER_UNDO_KIND_TT_ABORT) {
		memset(slot, 0, sizeof(*slot));
		slot->xid = op->kind == RF_SIDE_ONLINE_OPERATION_XACT ? op->xact.tt_delta.xid : op->undo.xid;
		slot->wrap = op->kind == RF_SIDE_ONLINE_OPERATION_XACT ? op->xact.tt_delta.wrap : op->undo.wrap;
		slot->status = TT_SLOT_ACTIVE;
		slot->first_undo_block = invalid_head;
	}
}

static RfPageProofDetailV1
side_plan_prepare_slots(const RfSideOnlinePlanV1 *plan, uint8 instance,
	uint32 segment, const char *base, RfSideUndoHeaderImageV1 *prepared)
{
	PGAlignedBlock scratch;
	const UndoSegmentHeaderData *original = (const UndoSegmentHeaderData *)base;
	UndoSegmentHeaderData *candidate = (UndoSegmentHeaderData *)scratch.data;
	UndoSegmentHeaderData *final = (UndoSegmentHeaderData *)prepared->page.data;

	if (base == NULL || !UndoSegmentHeader_identity_matches(base, segment, instance))
		return RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
	for (uint16 s = 0; s < TT_SLOTS_PER_SEGMENT; s++) {
		bool started = false, proved = false;
		uint32 last_source = UINT32_MAX;

		memcpy(scratch.data, base, BLCKSZ);
		for (uint32 i = 0; i < plan->operation_count; i++) {
			const RfSideOnlineOperationV1 *op = &plan->operations[i];
			ClusterUndoHeaderPrepareResultV1 result;
			uint16 slot;

			if (!side_plan_slot_operation(op, instance, segment, &slot) || slot != s)
				continue;
			if (!started) {
				result = side_plan_slot_step(op, base, scratch.data);
				proved = result == CLUSTER_UNDO_HEADER_APPLY || result == CLUSTER_UNDO_HEADER_ALREADY;
				memcpy(scratch.data, base, BLCKSZ);
				side_plan_slot_anchor(op, &candidate->tt_slots[s]);
				started = true;
			}
			result = side_plan_slot_step(op, scratch.data, scratch.data);
			/* Stale source is not evidence of a target's membership. */
			if (result == CLUSTER_UNDO_HEADER_BLOCKED
				|| result == CLUSTER_UNDO_HEADER_SKIP_STALE)
				return RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
			if (memcmp(&candidate->tt_slots[s], &original->tt_slots[s], sizeof(TTSlot)) == 0)
				proved = true;
			prepared->operation_count++;
			last_source = i;
		}
		if (!started)
			continue;
		if (!proved)
			return RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
		if (memcmp(&candidate->tt_slots[s], &original->tt_slots[s], sizeof(TTSlot)) != 0) {
			final->tt_slots[s] = candidate->tt_slots[s];
			if (prepared->source_index == UINT32_MAX || last_source > prepared->source_index)
				prepared->source_index = last_source;
		}
	}
	return prepared->operation_count == 0 ? RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE : RF_PAGE_PROOF_DETAIL_OK;
}

RfPageProofDetailV1
rf_side_online_plan_prepare_undo_header_v1(const RfSideOnlinePlanV1 *plan,
	uint8 instance, uint32 segment_id, const char *base, RfSideUndoHeaderImageV1 *out)
{
	RfSideUndoHeaderImageV1 prepared;
	const char *current = base;
	Size scratch = sizeof(prepared) + 3 * BLCKSZ;
	bool lifecycle = false;

	if (plan == NULL || plan->magic != RF_SIDE_ONLINE_PLAN_MAGIC || !plan->sealed
		|| out == NULL || instance == 0 || instance > 128 || segment_id == 0
		|| ((segment_id - 1) / CLUSTER_UNDO_SEGS_PER_INSTANCE) + 1 != instance)
		return RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT;
	if (scratch > plan->memory_budget || plan->memory_used > plan->memory_budget - scratch)
		return RF_PAGE_PROOF_DETAIL_CAPACITY;
	memset(&prepared, 0, sizeof(prepared));
	prepared.source_index = UINT32_MAX;
	if (base != NULL)
		memcpy(prepared.page.data, base, BLCKSZ);
	for (uint32 i = 0; i < plan->operation_count; i++) {
		const RfSideOnlineOperationV1 *op = &plan->operations[i];

		if (op->kind == RF_SIDE_ONLINE_OPERATION_UNDO && op->undo.instance == instance
			&& op->undo.segment_id == segment_id
			&& (op->undo.kind == CLUSTER_UNDO_KIND_SEGMENT_INIT
				|| op->undo.kind == CLUSTER_UNDO_KIND_SEGMENT_REUSE
				|| op->undo.kind == CLUSTER_UNDO_KIND_SEGMENT_RECYCLE))
			lifecycle = true;
	}
	if (!lifecycle) {
		RfPageProofDetailV1 detail = side_plan_prepare_slots(plan, instance, segment_id, base, &prepared);

		if (detail == RF_PAGE_PROOF_DETAIL_OK)
			*out = prepared;
		return detail;
	}
	for (uint32 i = 0; i < plan->operation_count; i++) {
		const RfSideOnlineOperationV1 *op = &plan->operations[i];
		ClusterUndoHeaderPrepareResultV1 result;

		if (op->kind == RF_SIDE_ONLINE_OPERATION_UNDO) {
			const ClusterUndoDecoded *undo = &op->undo;
			const uint8 *payload = op->owned_payload_length == 0 ? NULL
				: plan->owned_payload + op->owned_payload_offset;

			if (undo->instance != instance || undo->segment_id != segment_id
				|| undo->kind == CLUSTER_UNDO_KIND_BLOCK_WRITE
				|| undo->kind == CLUSTER_UNDO_KIND_BLOCK_WRITE_MULTI)
				continue;
			result = cluster_undo_prepare_header_v1(undo, payload, op->owned_payload_length,
				current, prepared.page.data);
		} else if (op->kind == RF_SIDE_ONLINE_OPERATION_XACT
			&& op->xact.kind == RF_SIDE_XACT_COMMIT && op->xact.has_tt_delta) {
			const xl_xact_tt_commit *delta = &op->xact.tt_delta;

			if (delta->instance != instance || delta->segment_id != segment_id)
				continue;
			result = cluster_undo_prepare_commit_v1(delta->instance, delta->segment_id,
				delta->segment_generation, delta->slot_offset, delta->wrap, delta->xid,
				delta->commit_scn, current, prepared.page.data);
		} else
			continue;
		if (result == CLUSTER_UNDO_HEADER_BLOCKED)
			return RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
		if (result == CLUSTER_UNDO_HEADER_APPLY) {
			current = prepared.page.data;
			prepared.source_index = i;
		}
		prepared.operation_count++;
	}
	if (current == NULL || prepared.operation_count == 0)
		return RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
	*out = prepared;
	return RF_PAGE_PROOF_DETAIL_OK;
}

RfPageProofDetailV1
rf_side_online_plan_seal_v1(RfSideOnlinePlanV1 *plan)
{
	uint32 i;

	if (plan == NULL || plan->magic != RF_SIDE_ONLINE_PLAN_MAGIC || plan->sealed)
		return RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT;
	for (i = 0; i < plan->operation_count; i++)
		if (plan->operations[i].kind == RF_SIDE_ONLINE_OPERATION_SPACE
			&& (plan->database_incarnation == 0
				|| plan->operations[i].space_key.database_incarnation != plan->database_incarnation))
			return RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH;
	for (i = 0; i < plan->participant_count; i++) {
		const RfContributorStreamCutV1 *cut = &plan->physical_cuts[i];
		bool empty = (cut->flags & RF_CONTRIBUTOR_CUT_EXPLICIT_EMPTY) != 0;

		if ((empty && plan->participant_seen[i])
			|| (!empty
				&& (!plan->participant_seen[i]
					|| plan->last_record_end[i] != cut->scan_end_exclusive)))
			return RF_PAGE_PROOF_DETAIL_SOURCE_GAP;
	}
	plan->sealed = true;
	return RF_PAGE_PROOF_DETAIL_OK;
}

uint32
rf_side_online_plan_operation_count_v1(const RfSideOnlinePlanV1 *plan)
{
	return plan != NULL && plan->magic == RF_SIDE_ONLINE_PLAN_MAGIC && plan->sealed
			   ? plan->operation_count
			   : 0;
}

Size
rf_side_online_plan_scratch_available_v1(const RfSideOnlinePlanV1 *plan)
{
	return plan != NULL && plan->magic == RF_SIDE_ONLINE_PLAN_MAGIC && plan->sealed
		&& plan->memory_used <= plan->memory_budget ? plan->memory_budget - plan->memory_used : 0;
}

bool
rf_side_online_plan_operation_v1(const RfSideOnlinePlanV1 *plan, uint32 index,
								 RfSideOnlineOperationV1 *out_operation)
{
	if (plan == NULL || plan->magic != RF_SIDE_ONLINE_PLAN_MAGIC || !plan->sealed
		|| index >= plan->operation_count || out_operation == NULL)
		return false;
	*out_operation = plan->operations[index];
	if (out_operation->owned_payload_length > 0)
		out_operation->owned_payload = plan->owned_payload + out_operation->owned_payload_offset;
	return true;
}

static bool
side_plan_apply_ops_valid(const RfSideOnlinePlanV1 *plan, const RfSideOnlineApplyOpsV1 *ops)
{
	uint32 i;

	if (plan == NULL || plan->magic != RF_SIDE_ONLINE_PLAN_MAGIC || !plan->sealed || ops == NULL)
		return false;
	if (ops->begin_protected_set == NULL || ops->end_protected_set == NULL)
		return false;
	for (i = 0; i < plan->operation_count; i++)
		if ((plan->operations[i].kind == RF_SIDE_ONLINE_OPERATION_XACT
			 && (ops->preflight_xact == NULL || ops->apply_xact == NULL))
			|| (plan->operations[i].kind == RF_SIDE_ONLINE_OPERATION_UNDO
				&& (ops->preflight_undo == NULL || ops->apply_undo == NULL))
			|| (plan->operations[i].kind == RF_SIDE_ONLINE_OPERATION_PROJECTION
				&& (ops->preflight_projection == NULL || ops->apply_projection == NULL))
			|| (plan->operations[i].kind == RF_SIDE_ONLINE_OPERATION_SPACE
				&& (ops->preflight_space == NULL || ops->apply_space == NULL))
			|| plan->operations[i].kind == RF_SIDE_ONLINE_OPERATION_INVALID)
			return false;
	return true;
}

static RfPageProofDetailV1
side_plan_preflight_active(const RfSideOnlinePlanV1 *plan, const RfSideOnlineApplyOpsV1 *ops)
{
	uint32 i;

	/*
	 * STOP-06 section 9.2: classify every target before the first target
	 * byte changes.  The caller keeps its protected-set certification stable
	 * across this pass and the mutation pass below.
	 */
	for (i = 0; i < plan->operation_count; i++) {
		RfSideOnlineOperationV1 operation = plan->operations[i];
		bool accepted;

		if (operation.kind == RF_SIDE_ONLINE_OPERATION_XACT
			&& operation.xact.kind == RF_SIDE_XACT_COMMIT_PREPARED
			&& !side_plan_commit_prepared_dependencies_closed(plan, i))
			return RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
		if (operation.kind == RF_SIDE_ONLINE_OPERATION_XACT
			&& operation.xact.kind == RF_SIDE_XACT_ABORT_PREPARED
			&& !side_plan_abort_prepared_dependencies_closed(plan, i))
			return RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
		if (operation.owned_payload_length > 0)
			operation.owned_payload = plan->owned_payload + operation.owned_payload_offset;
		if (operation.kind == RF_SIDE_ONLINE_OPERATION_XACT)
			accepted = ops->preflight_xact(ops->arg, &operation);
		else if (operation.kind == RF_SIDE_ONLINE_OPERATION_UNDO)
			accepted = ops->preflight_undo(ops->arg, &operation);
		else if (operation.kind == RF_SIDE_ONLINE_OPERATION_SPACE)
			accepted = ops->preflight_space(ops->arg, &operation);
		else
			accepted = ops->preflight_projection(ops->arg, &operation);
		if (!accepted)
			return RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
	}
	return RF_PAGE_PROOF_DETAIL_OK;
}

RfPageProofDetailV1
rf_side_online_plan_preflight_v1(const RfSideOnlinePlanV1 *plan, const RfSideOnlineApplyOpsV1 *ops)
{
	RfPageProofDetailV1 detail;

	if (!side_plan_apply_ops_valid(plan, ops))
		return RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT;
	if (plan->operation_count == 0)
		return RF_PAGE_PROOF_DETAIL_OK;
	if (!ops->begin_protected_set(ops->arg))
		return RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
	PG_TRY();
	{
		detail = side_plan_preflight_active(plan, ops);
	}
	PG_FINALLY();
	{
		/* A preflight-only pass never publishes a complete protected set. */
		ops->end_protected_set(ops->arg, false);
	}
	PG_END_TRY();
	return detail;
}

static RfPageProofDetailV1
side_plan_apply_active(const RfSideOnlinePlanV1 *plan, const RfSideOnlineApplyOpsV1 *ops)
{
	RfPageProofDetailV1 detail;
	uint32 i;

	detail = side_plan_preflight_active(plan, ops);
	if (detail != RF_PAGE_PROOF_DETAIL_OK)
		return detail;
	for (i = 0; i < plan->operation_count; i++) {
		RfSideOnlineOperationV1 operation = plan->operations[i];
		bool applied;

		if (operation.owned_payload_length > 0)
			operation.owned_payload = plan->owned_payload + operation.owned_payload_offset;
		if (operation.kind == RF_SIDE_ONLINE_OPERATION_XACT)
			applied = ops->apply_xact(ops->arg, &operation);
		else if (operation.kind == RF_SIDE_ONLINE_OPERATION_UNDO)
			applied = ops->apply_undo(ops->arg, &operation);
		else if (operation.kind == RF_SIDE_ONLINE_OPERATION_SPACE)
			applied = ops->apply_space(ops->arg, &operation);
		else
			applied = ops->apply_projection(ops->arg, &operation);
		if (!applied)
			return RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
	}
	return RF_PAGE_PROOF_DETAIL_OK;
}

RfPageProofDetailV1
rf_side_online_plan_apply_v1(const RfSideOnlinePlanV1 *plan, const RfSideOnlineApplyOpsV1 *ops)
{
	RfPageProofDetailV1 detail;
	volatile bool complete = false;

	if (!side_plan_apply_ops_valid(plan, ops))
		return RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT;
	if (plan->operation_count == 0)
		return RF_PAGE_PROOF_DETAIL_OK;
	if (!ops->begin_protected_set(ops->arg))
		return RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
	PG_TRY();
	{
		detail = side_plan_apply_active(plan, ops);
		complete = detail == RF_PAGE_PROOF_DETAIL_OK;
	}
	PG_FINALLY();
	{
		ops->end_protected_set(ops->arg, complete);
	}
	PG_END_TRY();
	return detail;
}
void
rf_side_online_plan_destroy_v1(RfSideOnlinePlanV1 **plan_address)
{
	RfSideOnlinePlanV1 *plan;

	if (plan_address == NULL || *plan_address == NULL)
		return;
	plan = *plan_address;
	if (plan->physical_cuts != NULL)
		side_free(plan->physical_cuts);
	if (plan->last_record_end != NULL)
		side_free(plan->last_record_end);
	if (plan->participant_seen != NULL)
		side_free(plan->participant_seen);
	if (plan->operations != NULL)
		side_free(plan->operations);
	if (plan->owned_payload != NULL)
		side_free(plan->owned_payload);
	memset(plan, 0, sizeof(*plan));
	side_free(plan);
	*plan_address = NULL;
}

#endif
