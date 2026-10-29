/* PGRAC: original relmap producer identity.
 * Author: SqlRush <sqlrush@gmail.com>
 * Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md */
#include "postgres.h"

#include "access/xact.h"
#include "access/xlog.h"
#include "access/xlogreader.h"
#include "catalog/pg_tablespace_d.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_membership.h"
#include "cluster/cluster_qvotec.h"
#include "cluster/cluster_relmap_authority.h"
#include "cluster/cluster_relmap_lock.h"
#include "cluster/cluster_relmap_owner.h"
#include "cluster/cluster_relmap_pending_side.h"
#include "cluster/cluster_wal_inputs.h"
#include "miscadmin.h"
#include "postmaster/interrupt.h"
#include "storage/lwlock.h"
#include "utils/relmapper.h"

static bool
relmap_owner_context(bool require_active)
{
	return cluster_enabled && cluster_shared_config
		   && cluster_node_id >= 0 && cluster_node_id < CLUSTER_MAX_NODES
		   && MyBackendType == B_BACKEND && MyProcPid > 0 && CritSectionCount == 0
		   && !ShutdownRequestPending && (!require_active || IsTransactionState())
		   && cluster_relmap_authority_x_held() && !RecoveryInProgress();
}

bool
cluster_relmap_owner_capture_v2(ClusterRelmapOwnerSampleV2 *out)
{
	ClusterRelmapOwnerSampleV2 work = { 0 };
	FullTransactionId top;

	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	if (!relmap_owner_context(true) || LWLockHeldByMe(RelationMappingLock))
		return false;

	/* Assignment can wait and write transaction metadata. It must precede the
	 * local mapping lock and WAL window. Never reconstruct from a raw XID. */
	top = GetTopFullTransactionId();
	if (!FullTransactionIdIsValid(top) || !TransactionIdIsNormal(XidFromFullTransactionId(top))
		|| cluster_wal_writer_begin(GetWALInsertionTimeLine(), &work.writer)
			   != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return false;
	work.owner.node = cluster_node_id;
	work.owner.top_full_xid = U64FromFullTransactionId(top);
	work.owner.membership_epoch = work.writer.epoch;
	/* These are separate observations, even when native writer qualification
	 * currently requires both to match its admitted claim incarnation. */
	work.owner.boot_incarnation = cluster_qvotec_get_self_incarnation();
	work.owner.admitted_incarnation =
		cluster_membership_get_last_admitted_incarnation(cluster_node_id);
	work.producer_pid = MyProcPid;
	if (!cluster_relmap_owner_matches_v2(&work))
		return false;
	*out = work;
	return true;
}

bool
cluster_relmap_owner_capture_retained_v2(ClusterRelmapOwnerSampleV2 *out)
{
#ifdef USE_PGRAC_CLUSTER
	ClusterRelmapOwnerSampleV2 work = { 0 };
	FullTransactionId top;
#endif

	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
#ifdef USE_PGRAC_CLUSTER
	if (!relmap_owner_context(true) || LWLockHeldByMe(RelationMappingLock)
		|| LWLockHeldByMe(XidGenLock)
		|| cluster_wal_writer_begin(GetWALInsertionTimeLine(), &work.writer)
			!= CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return false;
	/* Keep this original writer across native allocation and its waits. The
	 * final matcher must reject drift, never sample a newer writer to adopt. */
	work.owner.node = cluster_node_id;
	work.owner.membership_epoch = work.writer.epoch;
	work.owner.boot_incarnation = cluster_qvotec_get_self_incarnation();
	work.owner.admitted_incarnation =
		cluster_membership_get_last_admitted_incarnation(cluster_node_id);
	work.producer_pid = MyProcPid;
	top = GetTopFullTransactionIdRelmap(work.writer.ref.claim.identity.system_identifier,
		work.writer.ref.claim.database_incarnation, work.writer.ref.claim.identity.origin_node_id);
	if (!FullTransactionIdIsValid(top) || !TransactionIdIsNormal(XidFromFullTransactionId(top)))
		return false;
	work.owner.top_full_xid = U64FromFullTransactionId(top);
	if (!cluster_relmap_owner_matches_v2(&work))
		return false;
	*out = work;
	return true;
#else
	return false;
#endif
}

static bool
relmap_owner_matches_v2(const ClusterRelmapOwnerSampleV2 *sample, bool require_active)
{
	FullTransactionId top;

	if (sample == NULL || !relmap_owner_context(require_active)
		|| sample->producer_pid != MyProcPid || sample->owner.node != cluster_node_id
		|| sample->owner.wal_end != InvalidXLogRecPtr || sample->writer.startup_first_lsn != 0)
		return false;
	/* IfAny never assigns an XID, including when RelationMappingLock is held.
	 * A later epoch with the same raw XID is a different original owner. */
	top = GetTopFullTransactionIdIfAny();
	if (!FullTransactionIdIsValid(top) || !TransactionIdIsNormal(XidFromFullTransactionId(top))
		|| sample->owner.top_full_xid != U64FromFullTransactionId(top)
		|| sample->owner.membership_epoch != sample->writer.epoch
		|| sample->owner.boot_incarnation == 0
		|| sample->owner.boot_incarnation != cluster_qvotec_get_self_incarnation()
		|| sample->owner.admitted_incarnation == 0
		|| sample->owner.admitted_incarnation
			   != sample->writer.ref.claim.identity.origin_owner_incarnation
		|| sample->owner.admitted_incarnation
			   != cluster_membership_get_last_admitted_incarnation(cluster_node_id)
		|| sample->writer.ref.timeline != GetWALInsertionTimeLine())
		return false;
	return cluster_wal_writer_check(&sample->writer) == CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

bool
cluster_relmap_owner_matches_v2(const ClusterRelmapOwnerSampleV2 *sample)
{
	return relmap_owner_matches_v2(sample, true);
}

bool
cluster_relmap_owner_convert_clean_v2(const ClusterRelmapOwnerSampleV2 *sample, bool shared_map)
{
	ClusterRelmapV2Namespace identity = { 0 };

	if (LWLockHeldByMe(RelationMappingLock) || !cluster_relmap_owner_matches_v2(sample)
		|| (!shared_map && !OidIsValid(MyDatabaseId)))
		return false;
	identity.system_identifier = sample->writer.ref.claim.identity.system_identifier;
	identity.database_incarnation = sample->writer.ref.claim.database_incarnation;
	identity.dbid = shared_map ? InvalidOid : MyDatabaseId;
	identity.shared = shared_map;
	if (!cluster_relmap_authority_v2_convert_clean(&identity))
		return false;
	/* I/O may have durably converted both files before an owner change is
	 * observed. Refusal never proves rollback or enables either publisher. */
	return !LWLockHeldByMe(RelationMappingLock) && cluster_relmap_owner_matches_v2(sample)
		&& (shared_map || MyDatabaseId == identity.dbid);
}

bool
cluster_relmap_owner_terminal_matches_v2(const ClusterRelmapOwnerSampleV2 *sample)
{
	/* Original postcommit publish is TRANS_COMMIT, with its top FullXid still
	 * retained. This rechecks provenance, never selects commit/abort polarity. */
	return relmap_owner_matches_v2(sample, false);
}

bool
cluster_relmap_owner_wal_matches_v2(const ClusterRelmapV2Image *image,
	const ClusterWalSourceRef *source, const XLogReaderState *reader)
{
	uint8 wire[CLUSTER_RELMAP_V2_FILE_SIZE];
	xl_relmap_update update;
	TransactionId raw;
	const char *data;

	if (image == NULL || source == NULL || reader == NULL || reader->record == NULL
		|| image->pending_generation == 0
		|| cluster_relmap_v2_encode(image, wire, sizeof(wire)) != CLUSTER_RELMAP_V2_VALID
		|| !cluster_wal_claim_v2_ref_valid(&source->claim) || source->timeline == 0)
		return false;
	if (image->identity.system_identifier != source->claim.identity.system_identifier
		|| image->identity.database_incarnation != source->claim.database_incarnation
		|| image->pending_owner.node != source->claim.identity.origin_node_id
		|| image->pending_owner.admitted_incarnation
			   != source->claim.identity.origin_owner_incarnation
		|| reader->system_identifier != image->identity.system_identifier
		|| reader->cluster_expected_thread_id != source->claim.identity.origin_thread_id
		|| reader->seg.ws_tli != source->timeline)
		return false;
	if (reader->ReadRecPtr == InvalidXLogRecPtr || reader->EndRecPtr <= reader->ReadRecPtr
		|| reader->record->lsn != reader->ReadRecPtr
		|| reader->record->next_lsn != reader->EndRecPtr
		|| reader->EndRecPtr != image->pending_owner.wal_end
		|| XLogRecGetTotalLen(reader) > reader->EndRecPtr - reader->ReadRecPtr
		|| XLogRecGetTotalLen(reader)
			   < SizeOfXLogRecord + MinSizeOfRelmapUpdate + sizeof(RelMapFile)
		|| XLogRecGetRmid(reader) != RM_RELMAP_ID
		|| (XLogRecGetInfo(reader) & ~XLR_INFO_MASK) != XLOG_RELMAP_UPDATE
		|| XLogRecHasAnyBlockRefs(reader) || XLogRecHasPageVersionEdge(reader)
		|| XLogRecGetData(reader) == NULL
		|| XLogRecGetDataLen(reader) != MinSizeOfRelmapUpdate + sizeof(RelMapFile))
		return false;

	/* This checks only the original raw XID. The era, membership epoch and
	 * boot incarnation have no independent representation in these inputs. */
	raw = XidFromFullTransactionId(FullTransactionIdFromU64(image->pending_owner.top_full_xid));
	if (XLogRecGetXid(reader) != raw
		|| (TransactionIdIsValid(XLogRecGetTopXid(reader)) && XLogRecGetTopXid(reader) != raw))
		return false;
	data = XLogRecGetData(reader);
	memcpy(&update, data, MinSizeOfRelmapUpdate);
	if (update.nbytes != sizeof(RelMapFile) || update.dbid != image->identity.dbid
		|| (image->identity.shared ? update.tsid != GLOBALTABLESPACE_OID
								 : !OidIsValid(update.tsid) || update.tsid == GLOBALTABLESPACE_OID))
		return false;
	return memcmp(data + MinSizeOfRelmapUpdate, &image->pending, sizeof(RelMapFile)) == 0;
}

typedef struct RelmapRetainedWalWork
{
	ClusterRelmapV2Image image;
	bool found;
	const ClusterRelmapPendingSideSource *sides;
	size_t side_count;
	uint32 participant[CLUSTER_WAL_INPUTS_MAX];
	uint32 next_operation[CLUSTER_WAL_INPUTS_MAX];
	bool completed[CLUSTER_WAL_INPUTS_MAX];
	size_t completed_count;
} RelmapRetainedWalWork;

static bool
relmap_retained_side_sources(RelmapRetainedWalWork *work)
{
	const ClusterRelmapV2Image *image = &work->image;
	const ClusterRelmapPendingSideSource *sources = work->sides;
	size_t count = work->side_count;

	/* Validate every plan's complete physical membership before scanning.
	 * Binding a claim to a plan alone does not bind it to the held ROOT. */
	for (size_t i = 0; i < count; i++) {
		uint32 participant = 0;

		const ClusterRelmapPendingSideSource *input = &sources[i];
		const ClusterControlRootIdentity *identity = &input->source.claim.identity;
		uint32 participants = rf_side_online_plan_participant_count_v1(input->plan);
		size_t supplied = 0;

		if (participants == UINT32_MAX || participants == 0
			|| participants > RF_PAGE_STABLE_MAX_PARTICIPANTS
			|| identity->system_identifier != image->identity.system_identifier
			|| input->source.claim.database_incarnation != image->identity.database_incarnation
			|| input->cut.failed_thread != identity->origin_thread_id
			|| input->cut.origin_owner_incarnation != identity->origin_owner_incarnation
			|| input->cut.timeline_id != input->source.timeline
			|| !rf_side_online_plan_recovery_source_matches_v1(input->plan,
				image->identity.system_identifier, identity->storage_uuid, &input->cut)
			|| !rf_side_online_plan_recovery_claim_matches_v1(input->plan, &input->source))
			return false;
		for (size_t j = 0; j < i; j++)
			if (sources[j].cut.failed_thread == input->cut.failed_thread
				&& sources[j].cut.origin_owner_incarnation == input->cut.origin_owner_incarnation)
				return false;
		for (size_t j = 0; j < count; j++)
			if (sources[j].plan == input->plan)
				supplied++;
		if (supplied != participants)
			return false;
		/* The sealed participant index is the physical identity. Thread alone
		 * is insufficient when one thread has retained incarnations. */
		for (size_t j = 0; j < count; j++)
			if (sources[j].plan == input->plan
				&& rf_contributor_cut_precedes_v1(&sources[j].cut, &input->cut))
				participant++;
		if (participant >= participants || work->participant[i] != UINT32_MAX)
			return false;
		work->participant[i] = participant;
		for (size_t j = 0; j < i; j++)
			if (sources[j].plan == input->plan && work->participant[j] == participant)
				return false;
	}
	return true;
}

static RfPageProofDetailV1
relmap_retained_next_structure(RelmapRetainedWalWork *work, size_t source,
	RfSideOnlineOperationV1 *operation, bool *found)
{
	const ClusterRelmapPendingSideSource *input = &work->sides[source];
	uint32 count = rf_side_online_plan_operation_count_v1(input->plan);

	*found = false;
	/* Each physical source visits its plan at most once, including history.
	 * A shared plan may interleave other sources and unrelated SIDE records. */
	while (work->next_operation[source] < count) {
		if (!rf_side_online_plan_operation_v1(input->plan,
				work->next_operation[source]++, operation))
			return RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
		if (operation->identity.participant_index != work->participant[source]
			|| (operation->identity.record.rmid != RM_SMGR_ID
				&& operation->identity.record.rmid != RM_XACT_ID))
			continue;
		*found = true;
		break;
	}
	return RF_PAGE_PROOF_DETAIL_OK;
}

static RfPageProofDetailV1
relmap_retained_structure(RelmapRetainedWalWork *work, XLogReaderState *reader,
	const ClusterWalSourceRef *source, const RfContributorStreamCutV1 *cut)
{
	if (work->sides == NULL
		|| (XLogRecGetRmid(reader) != RM_SMGR_ID && XLogRecGetRmid(reader) != RM_XACT_ID))
		return RF_PAGE_PROOF_DETAIL_OK;
	for (size_t i = 0; i < work->side_count; i++) {
		RfSideOnlineOperationV1 operation;
		const RfPageReplayRecordIdentityV1 *record = &operation.identity.record;
		RfPageProofDetailV1 detail;
		bool found;

		if (work->sides[i].cut.failed_thread != cut->failed_thread
			|| work->sides[i].cut.origin_owner_incarnation != cut->origin_owner_incarnation)
			continue;
		detail = relmap_retained_next_structure(work, i, &operation, &found);
		if (detail != RF_PAGE_PROOF_DETAIL_OK)
			return detail;
		if (!found)
			return RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
		if (record->system_identifier != reader->system_identifier
			|| memcmp(record->storage_uuid, source->claim.identity.storage_uuid, 16) != 0
			|| record->origin_thread != cut->failed_thread
			|| record->timeline_id != source->timeline
			|| record->read_rec_ptr != reader->ReadRecPtr || record->end_rec_ptr != reader->EndRecPtr
			|| record->record_crc != reader->record->header.xl_crc
			|| record->rmid != XLogRecGetRmid(reader) || record->info != XLogRecGetInfo(reader)
			|| operation.source_xid != XLogRecGetXid(reader)
			|| operation.source_top_xid != XLogRecGetTopXid(reader))
			return RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH;
		/* Structural payloads are whole native main data. Simple XACT may
		 * own none; its original identity, CRC and frame still must match. */
		if (operation.owned_payload_length > 0
			&& (operation.owned_payload_length != XLogRecGetDataLen(reader)
				|| operation.owned_payload == NULL || XLogRecGetData(reader) == NULL
				|| memcmp(operation.owned_payload, XLogRecGetData(reader),
					   operation.owned_payload_length) != 0))
			return RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH;
		return RF_PAGE_PROOF_DETAIL_OK;
	}
	return RF_PAGE_PROOF_DETAIL_SOURCE_GAP;
}

static RfPageProofDetailV1
relmap_retained_source(const ClusterWalSourceRef *source,
	const RfContributorStreamCutV1 *cut, const ClusterWalTailObservation *completed, void *arg)
{
	RelmapRetainedWalWork *work = arg;

	if (source->claim.identity.system_identifier != work->image.identity.system_identifier
		|| source->claim.database_incarnation != work->image.identity.database_incarnation)
		return RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH;
	if (work->sides == NULL)
		return RF_PAGE_PROOF_DETAIL_OK;
	for (size_t i = 0; i < work->side_count; i++) {
		const ClusterRelmapPendingSideSource *input = &work->sides[i];

		if (input->cut.failed_thread != cut->failed_thread
			|| input->cut.origin_owner_incarnation != cut->origin_owner_incarnation)
			continue;
		/* Compare with this scan's original source and physical cut, not the
		 * caller's self-consistent copies. Graph counts are not source proof. */
		if (work->completed[i]
			|| !rf_side_online_plan_recovery_source_matches_v1(input->plan,
				work->image.identity.system_identifier, source->claim.identity.storage_uuid, cut)
			|| !rf_side_online_plan_recovery_claim_matches_v1(input->plan, source))
			return RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH;
		if (completed != NULL) {
			RfSideOnlineOperationV1 operation;
			RfPageProofDetailV1 detail;
			bool found;

			/* Empty native sources must also have no extra structural input. */
			detail = relmap_retained_next_structure(work, i, &operation, &found);
			if (detail != RF_PAGE_PROOF_DETAIL_OK)
				return detail;
			if (found)
				return RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
			work->completed[i] = true;
			work->completed_count++;
		}
		return RF_PAGE_PROOF_DETAIL_OK;
	}
	return RF_PAGE_PROOF_DETAIL_SOURCE_GAP;
}

static RfPageProofDetailV1
relmap_retained_record(XLogReaderState *reader, const ClusterWalSourceRef *source,
	const RfContributorStreamCutV1 *cut, void *arg)
{
	RelmapRetainedWalWork *work = arg;
	const ClusterRelmapV2Owner *owner = &work->image.pending_owner;
	RfPageProofDetailV1 detail = relmap_retained_structure(work, reader, source, cut);

	if (detail != RF_PAGE_PROOF_DETAIL_OK)
		return detail;
	if (work->image.pending_generation == 0)
		return RF_PAGE_PROOF_DETAIL_OK;
	if (source->claim.identity.origin_node_id != owner->node
		|| source->claim.identity.origin_owner_incarnation != owner->admitted_incarnation
		|| reader->EndRecPtr != owner->wal_end)
		return RF_PAGE_PROOF_DETAIL_OK;
	if (!cluster_relmap_owner_wal_matches_v2(&work->image, source, reader))
		return RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH;
	if (work->found)
		return RF_PAGE_PROOF_DETAIL_TERMINAL_AMBIGUOUS;
	work->found = true;
	/* Keep reading: neither this source's suffix nor other sources are proven. */
	return RF_PAGE_PROOF_DETAIL_OK;
}

static ClusterControlRootResult
relmap_retained_wal(const ClusterRelmapV2Image *image, ClusterWalInputsV1 *inputs,
	const ClusterRelmapPendingSideSource *sides, size_t side_count)
{
	RelmapRetainedWalWork work = { 0 };
	uint8 wire[CLUSTER_RELMAP_V2_FILE_SIZE];
	uint64 records;
	RfPageProofDetailV1 detail;
	ClusterControlRootResult result;

	for (size_t i = 0; i < CLUSTER_WAL_INPUTS_MAX; i++)
		work.participant[i] = UINT32_MAX;

	if (image == NULL || inputs == NULL
		|| (image->pending_generation == 0 && sides == NULL)
		|| cluster_relmap_v2_encode(image, wire, sizeof(wire)) != CLUSTER_RELMAP_V2_VALID)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	work.image = *image;
	work.sides = sides;
	work.side_count = side_count;
	if (sides != NULL
		&& (side_count != cluster_wal_inputs_count_v1(inputs)
			|| !relmap_retained_side_sources(&work)))
		return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
	result = cluster_wal_inputs_cold_census_v1(inputs, relmap_retained_record,
		relmap_retained_source, &work, &records, &detail);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if ((image->pending_generation != 0 && !work.found)
		|| work.completed_count != side_count)
		return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
	/* Consume provisional callbacks only under the caller's original scope. */
	return cluster_wal_inputs_revalidate_v1(inputs);
}

ClusterControlRootResult
cluster_relmap_owner_retained_wal_v2(const ClusterRelmapV2Image *image,
	struct ClusterWalInputsV1 *inputs)
{
	return relmap_retained_wal(image, inputs, NULL, 0);
}

ClusterControlRootResult
cluster_relmap_owner_retained_side_wal_v2(const ClusterRelmapV2Image *image,
	struct ClusterWalInputsV1 *inputs,
	const ClusterRelmapPendingSideSource *sources, size_t source_count)
{
	if (sources == NULL || source_count == 0 || source_count > CLUSTER_WAL_INPUTS_MAX
		|| source_count > RF_PAGE_STABLE_MAX_PARTICIPANTS)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	return relmap_retained_wal(image, inputs, sources, source_count);
}

ClusterRelmapRetentionResult
cluster_relmap_owner_retention_begin_v2(const ClusterRelmapOwnerSampleV2 *sample,
	const ClusterRelmapV2Namespace *identity, uint64 generation,
	ClusterRelmapRetentionDirectory *directory, LWLock *retention_lock)
{
	ClusterRelmapRetentionKey key = { 0 };
	ClusterCanonicalTxnBinding binding;
	ClusterRelmapRetentionResult result;
	bool has_binding;

	if (sample == NULL || identity == NULL || directory == NULL || retention_lock == NULL
		|| retention_lock == RelationMappingLock)
		return CLUSTER_RELMAP_RETENTION_INVALID;
	if (LWLockHeldByMe(RelationMappingLock) || LWLockHeldByMe(retention_lock)
		|| !cluster_relmap_owner_matches_v2(sample))
		return CLUSTER_RELMAP_RETENTION_UNAVAILABLE;
	if (identity->system_identifier != sample->writer.ref.claim.identity.system_identifier
		|| identity->database_incarnation != sample->writer.ref.claim.database_incarnation
		|| (identity->shared ? identity->dbid != InvalidOid
							: !OidIsValid(MyDatabaseId) || identity->dbid != MyDatabaseId))
		return CLUSTER_RELMAP_RETENTION_INVALID;

	/* Read only: neither reserve a new slot nor reconstruct the original era.
	 * A later ACTIVE owner must enroll any pin which is still unbound. */
	has_binding = cluster_tt_local_get_published_binding(
		XidFromFullTransactionId(FullTransactionIdFromU64(sample->owner.top_full_xid)), &binding);
	if (!cluster_relmap_owner_matches_v2(sample))
		return CLUSTER_RELMAP_RETENTION_UNAVAILABLE;
	key.identity = *identity;
	key.generation = generation;
	key.origin = sample->owner.node;
	key.top_full_xid = sample->owner.top_full_xid;
	key.membership_epoch = sample->owner.membership_epoch;
	key.admitted_incarnation = sample->owner.admitted_incarnation;
	key.boot_incarnation = sample->owner.boot_incarnation;

	/* XID/native authority providers and TT publication stay outside this lock.
	 * The existing core checks COMPLETE, native retention and issuance bounds. */
	LWLockAcquire(retention_lock, LW_EXCLUSIVE);
	result = cluster_relmap_retention_begin_bound(directory, &key, has_binding ? &binding : NULL);
	LWLockRelease(retention_lock);
	return result;
}
