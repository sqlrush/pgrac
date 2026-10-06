/* PGRAC: provisional retained-input continuity for bounded live PI proofs.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"
#include "access/xlogreader.h"
#include "cluster/cluster_pi_contribution_stream.h"

#ifdef USE_CLUSTER_UNIT
#define stream_alloc0(size_) calloc(1, (size_))
#define stream_release(pointer_) free(pointer_)
#else
#define stream_alloc0(size_) palloc0(size_)
#define stream_release(pointer_) pfree(pointer_)
#endif

#define PI_STREAM_MAGIC UINT32_C(0x50495331)

typedef struct PiContributionInputV1 {
	ClusterWalSourceRef source;
	RfContributorStreamCutV1 cut;
	ClusterWalTailObservation seen;
	bool finished;
} PiContributionInputV1;

struct ClusterPiContributionStreamV1 {
	uint32 magic;
	uint32 count;
	uint64 records;
	RfPageProofDetailV1 failure;
	bool sealed;
	PiContributionInputV1 inputs[FLEXIBLE_ARRAY_MEMBER];
};

static RfPageProofDetailV1
stream_refuse(ClusterPiContributionStreamV1 *stream, RfPageProofDetailV1 detail)
{
	if (stream != NULL && stream->magic == PI_STREAM_MAGIC) {
		if (stream->failure == RF_PAGE_PROOF_DETAIL_OK)
			stream->failure = detail;
		return stream->failure;
	}
	return RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT;
}

static RfPageProofDetailV1
stream_ready(ClusterPiContributionStreamV1 *stream)
{
	if (stream == NULL || stream->magic != PI_STREAM_MAGIC)
		return RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT;
	if (stream->failure != RF_PAGE_PROOF_DETAIL_OK)
		return stream->failure;
	if (stream->sealed)
		return stream_refuse(stream, RF_PAGE_PROOF_DETAIL_ORDER_VIOLATION);
	return RF_PAGE_PROOF_DETAIL_OK;
}

RfPageProofDetailV1
cluster_pi_contribution_stream_create_v1(const ClusterWalSourceRef *sources,
										 const RfContributorStreamCutV1 *cuts, uint32 count,
										 ClusterPiContributionStreamV1 **out)
{
	ClusterPiContributionStreamV1 *stream;
	Size bytes;

	if (sources == NULL || cuts == NULL || out == NULL || *out != NULL || count == 0
		|| count > RF_PAGE_STABLE_MAX_PARTICIPANTS)
		return RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT;
	for (uint32 i = 0; i < count; i++) {
		const ClusterWalSourceRef *source = &sources[i];
		const RfContributorStreamCutV1 *cut = &cuts[i];
		bool empty = cut->flags == RF_CONTRIBUTOR_CUT_KNOWN_MASK;

		if (!cluster_wal_claim_v2_ref_valid(&source->claim) || source->timeline == 0
			|| source->timeline != cut->timeline_id
			|| source->claim.identity.origin_thread_id != cut->failed_thread
			|| source->claim.identity.origin_owner_incarnation != cut->origin_owner_incarnation
			|| cut->origin_owner_incarnation == 0 || cut->origin_owner_incarnation == UINT64_MAX
			|| source->claim.identity.system_identifier
				   != sources[0].claim.identity.system_identifier
			|| source->claim.database_incarnation != sources[0].claim.database_incarnation
			|| memcmp(source->claim.identity.storage_uuid, sources[0].claim.identity.storage_uuid,
					  16)
				   != 0)
			return RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH;
		if ((!empty && cut->flags != RF_CONTRIBUTOR_CUT_COMPLETE)
			|| (empty && (cut->contributor_count != 0 || cut->component_count != 0))
			|| cut->scan_begin_inclusive == InvalidXLogRecPtr
			|| (empty ? cut->scan_end_exclusive != cut->scan_begin_inclusive
					  : cut->scan_end_exclusive <= cut->scan_begin_inclusive)
			|| (i > 0 && !rf_contributor_cut_precedes_v1(&cuts[i - 1], cut)))
			return RF_PAGE_PROOF_DETAIL_SOURCE_GAP;
	}
	bytes = offsetof(ClusterPiContributionStreamV1, inputs) + count * sizeof(stream->inputs[0]);
	stream = stream_alloc0(bytes);
	if (stream == NULL)
		return RF_PAGE_PROOF_DETAIL_OOM;
	stream->magic = PI_STREAM_MAGIC;
	stream->count = count;
	for (uint32 i = 0; i < count; i++) {
		stream->inputs[i].source = sources[i];
		stream->inputs[i].cut = cuts[i];
		stream->inputs[i].seen.database_incarnation = sources[i].claim.database_incarnation;
	}
	*out = stream;
	return RF_PAGE_PROOF_DETAIL_OK;
}

RfPageProofDetailV1
cluster_pi_contribution_stream_record_v1(ClusterPiContributionStreamV1 *stream, uint32 index,
										 const ClusterWalSourceRef *source,
										 const XLogReaderState *record)
{
	PiContributionInputV1 *input;
	RfPageProofDetailV1 detail = stream_ready(stream);
	XLogRecPtr start, end, prev;

	if (detail != RF_PAGE_PROOF_DETAIL_OK)
		return detail;
	if (index >= stream->count || source == NULL || record == NULL || record->record == NULL)
		return stream_refuse(stream, RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT);
	input = &stream->inputs[index];
	if (input->finished)
		return stream_refuse(stream, RF_PAGE_PROOF_DETAIL_ORDER_VIOLATION);
	/* The source is the scope's original value, not a thread/timeline alias
	 * or a later claim with a matching record address. */
	if (source->timeline != input->source.timeline
		|| memcmp(&source->claim, &input->source.claim, sizeof(source->claim)) != 0)
		return stream_refuse(stream, RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH);
	start = record->ReadRecPtr;
	end = record->EndRecPtr;
	prev = XLogRecGetPrev(record);
	if ((input->cut.flags & RF_CONTRIBUTOR_CUT_EXPLICIT_EMPTY) != 0
		|| record->record->header.xl_tot_len < SizeOfXLogRecord
		|| start < input->cut.scan_begin_inclusive || end <= start
		|| end - start < record->record->header.xl_tot_len || end > input->cut.scan_end_exclusive
		|| (input->seen.records == 0
				? start != input->cut.scan_begin_inclusive || prev >= start
				: start < input->seen.complete_end || prev != input->seen.last_record_start))
		return stream_refuse(stream, RF_PAGE_PROOF_DETAIL_SOURCE_GAP);
	if (stream->records == UINT64_MAX || input->seen.records == UINT64_MAX)
		return stream_refuse(stream, RF_PAGE_PROOF_DETAIL_CAPACITY);
	input->seen.records++;
	input->seen.last_record_start = start;
	input->seen.complete_end = end;
	input->seen.last_record_crc = record->record->header.xl_crc;
	stream->records++;
	return RF_PAGE_PROOF_DETAIL_OK;
}

RfPageProofDetailV1
cluster_pi_contribution_stream_finish_v1(ClusterPiContributionStreamV1 *stream, uint32 index,
										 const ClusterWalTailObservation *observed)
{
	PiContributionInputV1 *input;
	bool empty;
	RfPageProofDetailV1 detail = stream_ready(stream);

	if (detail != RF_PAGE_PROOF_DETAIL_OK)
		return detail;
	if (index >= stream->count || observed == NULL)
		return stream_refuse(stream, RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT);
	input = &stream->inputs[index];
	if (input->finished)
		return stream_refuse(stream, RF_PAGE_PROOF_DETAIL_ORDER_VIOLATION);
	empty = (input->cut.flags & RF_CONTRIBUTOR_CUT_EXPLICIT_EMPTY) != 0;
	if (observed->records != input->seen.records
		|| observed->database_incarnation != input->seen.database_incarnation
		|| observed->last_record_start != input->seen.last_record_start
		|| observed->last_record_crc != input->seen.last_record_crc
		|| observed->complete_end != input->seen.complete_end
		|| (empty ? input->seen.records != 0
				  : input->seen.records == 0
						|| input->seen.complete_end != input->cut.scan_end_exclusive))
		return stream_refuse(stream, RF_PAGE_PROOF_DETAIL_SOURCE_GAP);
	input->finished = true;
	return RF_PAGE_PROOF_DETAIL_OK;
}

RfPageProofDetailV1
cluster_pi_contribution_stream_seal_v1(ClusterPiContributionStreamV1 *stream, uint64 *out_records)
{
	RfPageProofDetailV1 detail = stream_ready(stream);
	if (detail != RF_PAGE_PROOF_DETAIL_OK)
		return detail;
	if (out_records == NULL)
		return stream_refuse(stream, RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT);
	for (uint32 i = 0; i < stream->count; i++)
		if (!stream->inputs[i].finished)
			return stream_refuse(stream, RF_PAGE_PROOF_DETAIL_SOURCE_GAP);
	stream->sealed = true;
	*out_records = stream->records;
	return RF_PAGE_PROOF_DETAIL_OK;
}

void
cluster_pi_contribution_stream_destroy_v1(ClusterPiContributionStreamV1 **stream)
{
	if (stream != NULL && *stream != NULL) {
		(*stream)->magic = 0;
		stream_release(*stream);
		*stream = NULL;
	}
}
