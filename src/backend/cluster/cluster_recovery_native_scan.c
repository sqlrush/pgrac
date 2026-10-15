/*-------------------------------------------------------------------------
 * cluster_recovery_native_scan.c
 *    Native WAL effect candidates for cold recovery.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/backend/cluster/cluster_recovery_native_scan.c
 * NOTES
 *    PGRAC-original. Spec: spec-s9p2-05-instance-and-cluster-recovery.md
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#ifdef USE_PGRAC_CLUSTER
#include "catalog/storage_xlog.h"
#include "cluster/cluster_recovery_native_scan.h"
#include "cluster/cluster_side_xact.h"
#include "storage/standbydefs.h"

typedef struct NativeRegion {
	ClusterRecoveryNativeKind kind;
	const char *bytes;
	uint32 count;
	size_t item_size;
} NativeRegion;

struct ClusterRecoveryNativeScan {
	ClusterRecoveryNativeRecord record;
	ClusterRecoveryNativeResult status;
	ClusterRecoveryNativeKind primary;
	NativeRegion regions[6];
	xl_xact_tt_commit tt;
	unsigned region;
	uint32 ordinal;
	bool primary_returned;
	char *payload; /* MAXALIGNed bytes in the same allocation */
};

/* Range membership only. The original input owner must authenticate this
 * complete cut and keep its ROOT/retention scope valid. A record cannot
 * belong to an empty cut or to a legacy, unidentified writer generation. */
static bool
native_reader_in_cut(const XLogReaderState *reader, const ClusterWalSourceRef *source,
					 const RfContributorStreamCutV1 *cut)
{
	return reader != NULL && reader->record != NULL && source != NULL && cut != NULL
		   && cluster_wal_claim_v2_ref_valid(&source->claim) && source->timeline != 0
		   && reader->system_identifier == source->claim.identity.system_identifier
		   && reader->ReadRecPtr != InvalidXLogRecPtr && reader->EndRecPtr > reader->ReadRecPtr
		   && reader->record->lsn == reader->ReadRecPtr
		   && reader->record->next_lsn == reader->EndRecPtr
		   && reader->record->header.xl_prev < reader->ReadRecPtr
		   && cut->flags == RF_CONTRIBUTOR_CUT_COMPLETE
		   && cut->failed_thread == source->claim.identity.origin_thread_id
		   && cut->timeline_id == source->timeline && cut->origin_owner_incarnation != 0
		   && cut->origin_owner_incarnation == source->claim.identity.origin_owner_incarnation
		   && cut->scan_begin_inclusive != InvalidXLogRecPtr
		   && cut->scan_begin_inclusive <= reader->ReadRecPtr
		   && cut->scan_end_exclusive >= reader->EndRecPtr;
}

static bool
native_source_equal(const ClusterWalSourceRef *a, const ClusterWalSourceRef *b)
{
	const ClusterControlRootIdentity *x = &a->claim.identity;
	const ClusterControlRootIdentity *y = &b->claim.identity;

	/* Compare logical fields, not compiler padding or a truncated digest. */
	return x->system_identifier == y->system_identifier
		   && memcmp(x->storage_uuid, y->storage_uuid, sizeof(x->storage_uuid)) == 0
		   && memcmp(x->authority_uuid, y->authority_uuid, sizeof(x->authority_uuid)) == 0
		   && x->origin_thread_id == y->origin_thread_id && x->reserved42 == y->reserved42
		   && x->origin_node_id == y->origin_node_id
		   && x->thread_claim_created_at == y->thread_claim_created_at
		   && x->thread_claim_crc32c == y->thread_claim_crc32c && x->reserved60 == y->reserved60
		   && x->origin_owner_incarnation == y->origin_owner_incarnation
		   && x->root_lineage_seq == y->root_lineage_seq
		   && a->claim.database_incarnation == b->claim.database_incarnation
		   && a->claim.max_config_generation == b->claim.max_config_generation
		   && memcmp(a->claim.claim_sha256, b->claim.claim_sha256, sizeof(a->claim.claim_sha256))
				  == 0
		   && a->timeline == b->timeline;
}

static bool
native_cut_equal(const RfContributorStreamCutV1 *a, const RfContributorStreamCutV1 *b)
{
	return a->failed_thread == b->failed_thread && a->flags == b->flags
		   && a->timeline_id == b->timeline_id && a->scan_begin_inclusive == b->scan_begin_inclusive
		   && a->scan_end_exclusive == b->scan_end_exclusive
		   && a->contributor_count == b->contributor_count
		   && a->component_count == b->component_count
		   && a->origin_owner_incarnation == b->origin_owner_incarnation;
}

static bool
native_header_equal(const XLogRecord *a, const XLogRecord *b)
{
	return a->xl_tot_len == b->xl_tot_len && a->xl_xid == b->xl_xid && a->xl_prev == b->xl_prev
		   && a->xl_scn == b->xl_scn && a->xl_info == b->xl_info && a->xl_rmid == b->xl_rmid
		   && a->xl_crc == b->xl_crc;
}

/* Each parsed count/region was first bounded against the complete payload.
 * No per-effect allocation or unbounded list walk occurs here. */
static void
native_regions(ClusterRecoveryNativeScan *scan, int nsubxacts, const TransactionId *subxacts,
			   int nrels, const RelFileLocator *relations, int nstats,
			   const xl_xact_stats_item *stats, int nmsgs, const SharedInvalidationMessage *msgs)
{
	scan->regions[0] = (NativeRegion){ CLUSTER_RECOVERY_NATIVE_SUBXACT, (const char *)subxacts,
									   nsubxacts, sizeof(*subxacts) };
	scan->regions[1] = (NativeRegion){ CLUSTER_RECOVERY_NATIVE_RELATION_DROP,
									   (const char *)relations, nrels, sizeof(*relations) };
	scan->regions[2] = (NativeRegion){ CLUSTER_RECOVERY_NATIVE_STATS_DROP, (const char *)stats,
									   nstats, sizeof(*stats) };
	scan->regions[3] = (NativeRegion){ CLUSTER_RECOVERY_NATIVE_INVALIDATION, (const char *)msgs,
									   nmsgs, sizeof(*msgs) };
}

static bool
native_array_size(uint32 length, size_t base, int count, size_t item_size)
{
	return length >= base && count >= 0 && (size_t)count <= (length - base) / item_size
		   && base + (size_t)count * item_size == length;
}

static ClusterRecoveryNativeResult
native_commit(ClusterRecoveryNativeScan *scan)
{
	xl_xact_parsed_commit parsed;
	ClusterRecoveryNativeRecord *record = &scan->record;

	if (!ParseCommitRecord(record->header.xl_info, (xl_xact_commit *)scan->payload,
						   record->payload_length, &parsed))
		return CLUSTER_RECOVERY_NATIVE_SHAPE;
	if (parsed.xinfo & (XACT_XINFO_HAS_TWOPHASE | XACT_XINFO_HAS_GID))
		return CLUSTER_RECOVERY_NATIVE_UNSUPPORTED;
	scan->primary = CLUSTER_RECOVERY_NATIVE_TERMINAL;
	record->xinfo = parsed.xinfo;
	record->top_xid = record->header.xl_xid;
	record->database = parsed.dbId;
	record->tablespace = parsed.tsId;
	record->xact_time = parsed.xact_time;
	record->terminal_scn = parsed.scn;
	record->origin = (xl_xact_origin){ parsed.origin_lsn, parsed.origin_timestamp };
	record->relcache_init_file = (parsed.xinfo & XACT_COMPLETION_UPDATE_RELCACHE_FILE) != 0;
	native_regions(scan, parsed.nsubxacts, parsed.subxacts, parsed.nrels, parsed.xlocators,
				   parsed.nstats, parsed.stats, parsed.nmsgs, parsed.msgs);
	if (parsed.has_tt_commit) {
		scan->tt = parsed.tt_commit;
		scan->regions[4] = (NativeRegion){ CLUSTER_RECOVERY_NATIVE_TT_COMMIT,
										   (const char *)&scan->tt, 1, sizeof(scan->tt) };
	}
	scan->regions[5] = (NativeRegion){ CLUSTER_RECOVERY_NATIVE_SPACE_DROP, parsed.space_drops,
									   parsed.nspace_drops, XACT_SPACE_DROP_RECORD_BYTES };
	return CLUSTER_RECOVERY_NATIVE_MORE;
}

static ClusterRecoveryNativeResult
native_abort(ClusterRecoveryNativeScan *scan, XLogReaderState *reader)
{
	xl_xact_parsed_abort parsed;
	ClusterRecoveryNativeRecord *record = &scan->record;

	/* The original abort parser trusts lengths. Its original bounded SIDE
	 * shape check runs first; this does not invoke SIDE preflight or apply. */
	if (!rf_side_xact_completion_shape_v1(reader, false))
		return CLUSTER_RECOVERY_NATIVE_SHAPE;
	ParseAbortRecord(record->header.xl_info, (xl_xact_abort *)scan->payload, &parsed);
	if (parsed.xinfo & XACT_XINFO_HAS_INVALS)
		return CLUSTER_RECOVERY_NATIVE_SHAPE; /* abort has no such array */
	if (parsed.xinfo & (XACT_XINFO_HAS_TWOPHASE | XACT_XINFO_HAS_GID))
		return CLUSTER_RECOVERY_NATIVE_UNSUPPORTED;
	scan->primary = CLUSTER_RECOVERY_NATIVE_TERMINAL;
	record->xinfo = parsed.xinfo;
	record->top_xid = record->header.xl_xid;
	record->database = parsed.dbId;
	record->tablespace = parsed.tsId;
	record->xact_time = parsed.xact_time;
	record->terminal_scn = parsed.scn;
	record->origin = (xl_xact_origin){ parsed.origin_lsn, parsed.origin_timestamp };
	native_regions(scan, parsed.nsubxacts, parsed.subxacts, parsed.nrels, parsed.xlocators,
				   parsed.nstats, parsed.stats, 0, NULL);
	return CLUSTER_RECOVERY_NATIVE_MORE;
}

static ClusterRecoveryNativeResult
native_parse(ClusterRecoveryNativeScan *scan, XLogReaderState *reader)
{
	ClusterRecoveryNativeRecord *record = &scan->record;
	uint8 opcode = record->header.xl_info & ~XLR_INFO_MASK;

	if (record->header.xl_rmid == RM_SMGR_ID) {
		if (opcode != XLOG_SMGR_CREATE)
			return CLUSTER_RECOVERY_NATIVE_UNSUPPORTED;
		if (record->payload_length != sizeof(xl_smgr_create))
			return CLUSTER_RECOVERY_NATIVE_SHAPE;
		scan->primary = CLUSTER_RECOVERY_NATIVE_CREATE;
		return CLUSTER_RECOVERY_NATIVE_MORE;
	}
	if (record->header.xl_rmid != RM_XACT_ID)
		return CLUSTER_RECOVERY_NATIVE_UNSUPPORTED;
	switch (opcode) {
	case XLOG_XACT_COMMIT:
	case XLOG_XACT_COMMIT | XLOG_XACT_HAS_INFO:
		return native_commit(scan);
	case XLOG_XACT_ABORT:
	case XLOG_XACT_ABORT | XLOG_XACT_HAS_INFO:
		return native_abort(scan, reader);
	case XLOG_XACT_ASSIGNMENT: {
		xl_xact_assignment assignment;

		if (record->payload_length < MinSizeOfXactAssignment)
			return CLUSTER_RECOVERY_NATIVE_SHAPE;
		memcpy(&assignment, scan->payload, MinSizeOfXactAssignment);
		if (!native_array_size(record->payload_length, MinSizeOfXactAssignment,
							   assignment.nsubxacts, sizeof(TransactionId)))
			return CLUSTER_RECOVERY_NATIVE_SHAPE;
		scan->primary = CLUSTER_RECOVERY_NATIVE_ASSIGNMENT;
		record->top_xid = assignment.xtop;
		scan->regions[0] = (NativeRegion){ CLUSTER_RECOVERY_NATIVE_SUBXACT,
										   scan->payload + MinSizeOfXactAssignment,
										   assignment.nsubxacts, sizeof(TransactionId) };
		return CLUSTER_RECOVERY_NATIVE_MORE;
	}
	case XLOG_XACT_INVALIDATIONS: {
		xl_invalidations invalidations;

		if (record->payload_length < MinSizeOfInvalidations
			|| (uint8)scan->payload[offsetof(xl_invalidations, relcacheInitFileInval)] > 1)
			return CLUSTER_RECOVERY_NATIVE_SHAPE;
		memcpy(&invalidations, scan->payload, MinSizeOfInvalidations);
		if (!native_array_size(record->payload_length, MinSizeOfInvalidations, invalidations.nmsgs,
							   sizeof(SharedInvalidationMessage)))
			return CLUSTER_RECOVERY_NATIVE_SHAPE;
		scan->primary = CLUSTER_RECOVERY_NATIVE_INVALIDATION_BATCH;
		record->database = invalidations.dbId;
		record->tablespace = invalidations.tsId;
		record->relcache_init_file = invalidations.relcacheInitFileInval;
		scan->regions[3] = (NativeRegion){ CLUSTER_RECOVERY_NATIVE_INVALIDATION,
										   scan->payload + MinSizeOfInvalidations,
										   invalidations.nmsgs, sizeof(SharedInvalidationMessage) };
		return CLUSTER_RECOVERY_NATIVE_MORE;
	}
	default:
		return CLUSTER_RECOVERY_NATIVE_UNSUPPORTED;
	}
}

ClusterRecoveryNativeResult
cluster_recovery_native_scan_begin(XLogReaderState *reader, const ClusterWalSourceRef *source,
								   const RfContributorStreamCutV1 *cut, size_t memory_budget,
								   ClusterRecoveryNativeScan **out)
{
	const DecodedXLogRecord *decoded;
	ClusterRecoveryNativeScan *scan;
	ClusterRecoveryNativeResult result;
	size_t prefix = MAXALIGN(sizeof(*scan));
	uint32 length;

	if (out == NULL)
		return CLUSTER_RECOVERY_NATIVE_INVALID;
	*out = NULL;
	if (!native_reader_in_cut(reader, source, cut))
		return CLUSTER_RECOVERY_NATIVE_INVALID;
	decoded = reader->record;
	if (decoded->max_block_id >= 0 || decoded->has_page_version_edge)
		return CLUSTER_RECOVERY_NATIVE_UNSUPPORTED;
	length = decoded->main_data_len;
	if (decoded->main_data == NULL || length == 0)
		return CLUSTER_RECOVERY_NATIVE_SHAPE;
	if (memory_budget < prefix || length > memory_budget - prefix || length > SIZE_MAX - prefix)
		return CLUSTER_RECOVERY_NATIVE_CAPACITY;
	scan = calloc(1, prefix + length);
	if (scan == NULL)
		return CLUSTER_RECOVERY_NATIVE_CAPACITY;
	scan->payload = (char *)scan + prefix;
	scan->record.source = *source;
	scan->record.cut = *cut;
	scan->record.read_lsn = reader->ReadRecPtr;
	scan->record.end_lsn = reader->EndRecPtr;
	scan->record.header = decoded->header;
	scan->record.record_origin = decoded->record_origin;
	scan->record.toplevel_xid = decoded->toplevel_xid;
	scan->record.payload = scan->payload;
	scan->record.payload_length = length;
	memcpy(scan->payload, decoded->main_data, length);
	result = native_parse(scan, reader);
	if (result != CLUSTER_RECOVERY_NATIVE_MORE) {
		free(scan);
		return result;
	}
	*out = scan;
	return CLUSTER_RECOVERY_NATIVE_MORE;
}

ClusterRecoveryNativeResult
cluster_recovery_native_scan_recheck(ClusterRecoveryNativeScan *scan, XLogReaderState *reader,
									 const ClusterWalSourceRef *source,
									 const RfContributorStreamCutV1 *cut)
{
	const ClusterRecoveryNativeRecord *record;
	const DecodedXLogRecord *decoded;

	if (scan == NULL)
		return CLUSTER_RECOVERY_NATIVE_INVALID;
	if (scan->status != CLUSTER_RECOVERY_NATIVE_MORE)
		return scan->status;
	if (!native_reader_in_cut(reader, source, cut))
		return scan->status = CLUSTER_RECOVERY_NATIVE_CHANGED;
	record = &scan->record;
	decoded = reader->record;
	if (!native_source_equal(&record->source, source) || !native_cut_equal(&record->cut, cut)
		|| record->read_lsn != reader->ReadRecPtr || record->end_lsn != reader->EndRecPtr
		|| !native_header_equal(&record->header, &decoded->header)
		|| record->record_origin != decoded->record_origin
		|| record->toplevel_xid != decoded->toplevel_xid || decoded->max_block_id >= 0
		|| decoded->has_page_version_edge || decoded->main_data == NULL
		|| decoded->main_data_len != record->payload_length
		|| memcmp(record->payload, decoded->main_data, record->payload_length) != 0)
		return scan->status = CLUSTER_RECOVERY_NATIVE_CHANGED;
	return CLUSTER_RECOVERY_NATIVE_UNCHANGED;
}

ClusterRecoveryNativeResult
cluster_recovery_native_scan_next(ClusterRecoveryNativeScan *scan, ClusterRecoveryNativeEffect *out)
{
	if (out == NULL)
		return CLUSTER_RECOVERY_NATIVE_INVALID;
	memset(out, 0, sizeof(*out));
	if (scan == NULL)
		return CLUSTER_RECOVERY_NATIVE_INVALID;
	if (scan->status != CLUSTER_RECOVERY_NATIVE_MORE)
		return scan->status;
	if (!scan->primary_returned) {
		scan->primary_returned = true;
		out->kind = scan->primary;
		out->record = &scan->record;
		out->bytes = scan->payload;
		out->length = scan->record.payload_length;
		return CLUSTER_RECOVERY_NATIVE_EFFECT;
	}
	while (scan->region < lengthof(scan->regions)) {
		const NativeRegion *region = &scan->regions[scan->region];

		if (scan->ordinal < region->count) {
			out->kind = region->kind;
			out->ordinal = scan->ordinal;
			out->record = &scan->record;
			out->bytes = region->bytes + (size_t)scan->ordinal * region->item_size;
			out->length = region->item_size;
			scan->ordinal++;
			return CLUSTER_RECOVERY_NATIVE_EFFECT;
		}
		scan->region++;
		scan->ordinal = 0;
	}
	return CLUSTER_RECOVERY_NATIVE_END;
}

void
cluster_recovery_native_scan_destroy(ClusterRecoveryNativeScan *scan)
{
	free(scan);
}

size_t
cluster_recovery_native_scan_bytes(const ClusterRecoveryNativeScan *scan)
{
	return scan == NULL ? 0 : MAXALIGN(sizeof(*scan)) + scan->record.payload_length;
}
#endif
