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
#include "access/clog.h"
#include "access/commit_ts.h"
#include "access/multixact.h"
#include "access/twophase_rmgr.h"
#include "catalog/storage_xlog.h"
#include "catalog/pg_tablespace_d.h"
#include "common/relmap.h"
#include "cluster/cluster_native_startup.h"
#include "cluster/cluster_recovery_native_scan.h"
#include "cluster/cluster_side_undo.h"
#include "cluster/cluster_side_xact.h"
#include "cluster/cluster_undo_segment.h"
#include "storage/standbydefs.h"
#include "utils/relmapper.h"

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
	ClusterUndoDecoded undo;
	unsigned region;
	uint32 ordinal;
	uint32 prepared_cursor, prepared_ordinal;
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
	scan->record.nsubxacts = nsubxacts;
	if (nsubxacts != 0)
		scan->record.subxacts_offset = (const char *)subxacts - scan->payload;
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

/* The native image includes unused slots in its CRC and in the owned WAL
 * bytes. Only active entries are emitted separately. This validates the
 * original representation, not authority generations, publish or RESET. */
static ClusterRecoveryNativeResult
native_relmap(ClusterRecoveryNativeScan *scan, uint8 opcode)
{
	xl_relmap_update update;
	RelMapFile map;
	pg_crc32c crc;

	if (opcode != XLOG_RELMAP_UPDATE)
		return CLUSTER_RECOVERY_NATIVE_UNSUPPORTED;
	if (scan->record.payload_length != MinSizeOfRelmapUpdate + sizeof(map))
		return CLUSTER_RECOVERY_NATIVE_SHAPE;
	memcpy(&update, scan->payload, MinSizeOfRelmapUpdate);
	memcpy(&map, scan->payload + MinSizeOfRelmapUpdate, sizeof(map));
	if (update.nbytes != sizeof(map) || map.magic != RELMAPPER_FILEMAGIC || map.num_mappings < 0
		|| map.num_mappings > MAX_MAPPINGS
		|| (update.tsid == GLOBALTABLESPACE_OID
				? update.dbid != InvalidOid
				: update.tsid != DEFAULTTABLESPACE_OID || update.dbid == InvalidOid))
		return CLUSTER_RECOVERY_NATIVE_SHAPE;
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, &map, offsetof(RelMapFile, crc));
	FIN_CRC32C(crc);
	if (!EQ_CRC32C(crc, map.crc))
		return CLUSTER_RECOVERY_NATIVE_SHAPE;
	scan->primary = CLUSTER_RECOVERY_NATIVE_RELMAP_UPDATE;
	scan->record.database = update.dbid;
	scan->record.tablespace = update.tsid;
	scan->record.mappings_offset = MinSizeOfRelmapUpdate + offsetof(RelMapFile, mappings);
	scan->record.nmappings = map.num_mappings;
	scan->regions[0] = (NativeRegion){ CLUSTER_RECOVERY_NATIVE_RELMAP_ENTRY,
									   scan->payload + scan->record.mappings_offset,
									   map.num_mappings, sizeof(RelMapping) };
	return CLUSTER_RECOVERY_NATIVE_MORE;
}

/* The original native producers log a page number for ZERO, and the page
 * containing oldest xid for TRUNCATE. Retain the raw horizon/database too;
 * neither the page geometry nor its presence proves an era or a completed
 * truncation. This is observation only, including disabled commit_ts history. */
static ClusterRecoveryNativeResult
native_slru(ClusterRecoveryNativeScan *scan, uint8 opcode, bool commit_ts)
{
	uint32 length = scan->record.payload_length;
	uint32 per_page = commit_ts ? CLUSTER_NATIVE_COMMIT_TS_PER_PAGE : CLUSTER_NATIVE_CLOG_PER_PAGE;
	int page;
	TransactionId oldest;
	uint8 zero = commit_ts ? COMMIT_TS_ZEROPAGE : CLOG_ZEROPAGE;
	uint8 truncate = commit_ts ? COMMIT_TS_TRUNCATE : CLOG_TRUNCATE;

	/* FULL retains the producer's native page-generation value verbatim.
	 * It does not certify a queried transaction or the page's later lifetime. */
	if (!commit_ts && opcode == CLOG_ZEROPAGE_FULL) {
		xl_clog_zeropage_full birth;
		TransactionId raw;

		if (length != sizeof(birth))
			return CLUSTER_RECOVERY_NATIVE_SHAPE;
		memcpy(&birth, scan->payload, sizeof(birth));
		raw = (TransactionId)birth.first_full_xid;
		if (birth.reserved_zero != 0 || birth.pageno < 0 || !TransactionIdIsNormal(raw)
			|| (raw % per_page != 0 && raw != FirstNormalTransactionId)
			|| raw / per_page != (uint32)birth.pageno)
			return CLUSTER_RECOVERY_NATIVE_SHAPE;
		scan->primary = CLUSTER_RECOVERY_NATIVE_CLOG_ZERO;
		return CLUSTER_RECOVERY_NATIVE_MORE;
	}
	if (opcode == zero) {
		if (length != sizeof(page))
			return CLUSTER_RECOVERY_NATIVE_SHAPE;
		memcpy(&page, scan->payload, sizeof(page));
		if (page < 0 || (uint64)page > UINT32_MAX / per_page)
			return CLUSTER_RECOVERY_NATIVE_SHAPE;
		scan->primary = commit_ts ? CLUSTER_RECOVERY_NATIVE_COMMIT_TS_ZERO
								  : CLUSTER_RECOVERY_NATIVE_CLOG_ZERO;
		return CLUSTER_RECOVERY_NATIVE_MORE;
	}
	if (opcode != truncate)
		return CLUSTER_RECOVERY_NATIVE_UNSUPPORTED;
	if (commit_ts) {
		xl_commit_ts_truncate record;
		if (length != SizeOfCommitTsTruncate)
			return CLUSTER_RECOVERY_NATIVE_SHAPE;
		memcpy(&record, scan->payload, SizeOfCommitTsTruncate);
		page = record.pageno;
		oldest = record.oldestXid;
	} else {
		xl_clog_truncate record;
		if (length != sizeof(record))
			return CLUSTER_RECOVERY_NATIVE_SHAPE;
		memcpy(&record, scan->payload, sizeof(record));
		page = record.pageno;
		oldest = record.oldestXact;
	}
	if (page < 0 || !TransactionIdIsNormal(oldest) || (uint64)page != oldest / per_page)
		return CLUSTER_RECOVERY_NATIVE_SHAPE;
	scan->primary = commit_ts ? CLUSTER_RECOVERY_NATIVE_COMMIT_TS_TRUNCATE
							  : CLUSTER_RECOVERY_NATIVE_CLOG_TRUNCATE;
	return CLUSTER_RECOVERY_NATIVE_MORE;
}

static ClusterRecoveryNativeResult
native_multixact(ClusterRecoveryNativeScan *scan, uint8 opcode)
{
	uint32 length = scan->record.payload_length;

	if (opcode == XLOG_MULTIXACT_ZERO_OFF_PAGE || opcode == XLOG_MULTIXACT_ZERO_MEM_PAGE) {
		int page;
		uint32 per_page = opcode == XLOG_MULTIXACT_ZERO_OFF_PAGE
							  ? CLUSTER_NATIVE_MX_OFFSETS_PER_PAGE
							  : CLUSTER_NATIVE_MX_MEMBERS_PER_PAGE;

		if (length != sizeof(page))
			return CLUSTER_RECOVERY_NATIVE_SHAPE;
		memcpy(&page, scan->payload, sizeof(page));
		if (page < 0 || (uint64)(uint32)page * per_page > UINT32_MAX)
			return CLUSTER_RECOVERY_NATIVE_SHAPE;
		scan->primary = opcode == XLOG_MULTIXACT_ZERO_OFF_PAGE
							? CLUSTER_RECOVERY_NATIVE_MULTIXACT_ZERO_OFFSETS
							: CLUSTER_RECOVERY_NATIVE_MULTIXACT_ZERO_MEMBERS;
		return CLUSTER_RECOVERY_NATIVE_MORE;
	}
	if (opcode == XLOG_MULTIXACT_CREATE_ID) {
		xl_multixact_create create = { 0 };

		if (length < SizeOfMultiXactCreate)
			return CLUSTER_RECOVERY_NATIVE_SHAPE;
		memcpy(&create, scan->payload, SizeOfMultiXactCreate);
		if (!MultiXactIdIsValid(create.mid) || create.moff == 0 || create.nmembers <= 0
			|| !native_array_size(length, SizeOfMultiXactCreate, create.nmembers,
								  sizeof(MultiXactMember)))
			return CLUSTER_RECOVERY_NATIVE_SHAPE;
		/* Observe the complete native array, not the bounded online projection.
		 * Offset wrap and member ownership remain raw recovery obligations. */
		for (int i = 0; i < create.nmembers; i++) {
			MultiXactMember member;

			memcpy(&member, scan->payload + SizeOfMultiXactCreate + (size_t)i * sizeof(member),
				   sizeof(member));
			if (!TransactionIdIsNormal(member.xid) || member.status < MultiXactStatusForKeyShare
				|| member.status > MaxMultiXactStatus)
				return CLUSTER_RECOVERY_NATIVE_SHAPE;
		}
		scan->primary = CLUSTER_RECOVERY_NATIVE_MULTIXACT_CREATE;
		scan->regions[0] = (NativeRegion){ CLUSTER_RECOVERY_NATIVE_MULTIXACT_MEMBER,
										   scan->payload + SizeOfMultiXactCreate,
										   (uint32)create.nmembers, sizeof(MultiXactMember) };
		return CLUSTER_RECOVERY_NATIVE_MORE;
	}
	if (opcode == XLOG_MULTIXACT_TRUNCATE_ID) {
		xl_multixact_truncate trunc;

		if (length != SizeOfMultiXactTruncate)
			return CLUSTER_RECOVERY_NATIVE_SHAPE;
		memcpy(&trunc, scan->payload, sizeof(trunc));
		if (!OidIsValid(trunc.oldestMultiDB) || !MultiXactIdIsValid(trunc.startTruncOff)
			|| !MultiXactIdIsValid(trunc.endTruncOff))
			return CLUSTER_RECOVERY_NATIVE_SHAPE;
		/* The native ranges can wrap. Preserve both complete ranges; their
		 * presence does not authorize truncation or prove retained members. */
		scan->primary = CLUSTER_RECOVERY_NATIVE_MULTIXACT_TRUNCATE;
		return CLUSTER_RECOVERY_NATIVE_MORE;
	}
	return CLUSTER_RECOVERY_NATIVE_UNSUPPORTED;
}

/* Bound each original StartPrepare segment before deriving its pointer.
 * MAXALIGN padding is owned too, but is not interpreted as an array item. */
static bool
native_prepare_region(ClusterRecoveryNativeScan *scan, size_t *at, unsigned region,
					  ClusterRecoveryNativeKind kind, int32 count, size_t width)
{
	size_t remaining = scan->record.payload_length - *at;
	size_t bytes, aligned;

	if (count < 0 || (size_t)count > remaining / width)
		return false;
	bytes = (size_t)count * width;
	aligned = MAXALIGN(bytes);
	if (aligned < bytes || aligned > remaining)
		return false;
	scan->regions[region] = (NativeRegion){ kind, scan->payload + *at, count, width };
	*at += aligned;
	return true;
}

/* Observe the complete native envelope, never run two-phase callbacks or
 * infer a terminal from PREPARE. Resource-manager payloads (including TT)
 * remain opaque until their own complete semantic consumer validates them. */
static ClusterRecoveryNativeResult
native_prepare(ClusterRecoveryNativeScan *scan)
{
	ClusterRecoveryNativeRecord *record = &scan->record;
	xl_xact_prepare header;
	size_t at = MAXALIGN(sizeof(header));
	size_t length = record->payload_length;
	const char *gid;

	if (length < at || (uint8)scan->payload[offsetof(xl_xact_prepare, initfileinval)] > 1)
		return CLUSTER_RECOVERY_NATIVE_SHAPE;
	memcpy(&header, scan->payload, sizeof(header));
	if (header.magic != TWOPHASE_MAGIC
		|| (uint64)header.total_len != (uint64)length + sizeof(pg_crc32c)
		|| !TransactionIdIsNormal(header.xid) || header.xid != record->header.xl_xid
		|| !OidIsValid(header.database) || !OidIsValid(header.owner) || header.gidlen == 0
		|| header.gidlen > GIDSIZE || MAXALIGN((size_t)header.gidlen) > length - at)
		return CLUSTER_RECOVERY_NATIVE_SHAPE;
	gid = scan->payload + at;
	if (gid[header.gidlen - 1] != '\0' || memchr(gid, '\0', header.gidlen - 1) != NULL)
		return CLUSTER_RECOVERY_NATIVE_SHAPE;
	record->prepared_gid_offset = at;
	record->prepared_gid_length = header.gidlen;
	at += MAXALIGN((size_t)header.gidlen);
	record->subxacts_offset = header.nsubxacts > 0 ? at : 0;
	if (!native_prepare_region(scan, &at, 0, CLUSTER_RECOVERY_NATIVE_PREPARE_SUBXACT,
							   header.nsubxacts, sizeof(TransactionId))
		|| !native_prepare_region(scan, &at, 1, CLUSTER_RECOVERY_NATIVE_PREPARE_COMMIT_RELATION,
								  header.ncommitrels, sizeof(RelFileLocator))
		|| !native_prepare_region(scan, &at, 2, CLUSTER_RECOVERY_NATIVE_PREPARE_ABORT_RELATION,
								  header.nabortrels, sizeof(RelFileLocator))
		|| !native_prepare_region(scan, &at, 3, CLUSTER_RECOVERY_NATIVE_PREPARE_COMMIT_STATS,
								  header.ncommitstats, sizeof(xl_xact_stats_item))
		|| !native_prepare_region(scan, &at, 4, CLUSTER_RECOVERY_NATIVE_PREPARE_ABORT_STATS,
								  header.nabortstats, sizeof(xl_xact_stats_item))
		|| !native_prepare_region(scan, &at, 5, CLUSTER_RECOVERY_NATIVE_PREPARE_INVALIDATION,
								  header.ninvalmsgs, sizeof(SharedInvalidationMessage)))
		return CLUSTER_RECOVERY_NATIVE_SHAPE;
	record->prepared_resources_offset = at;
	scan->prepared_cursor = at;
	while (at < length) {
		TwoPhaseRecordOnDisk resource;
		size_t aligned;
		if (MAXALIGN(sizeof(resource)) > length - at)
			return CLUSTER_RECOVERY_NATIVE_SHAPE;
		memcpy(&resource, scan->payload + at, sizeof(resource));
		at += MAXALIGN(sizeof(resource));
		if (resource.rmid == TWOPHASE_RM_END_ID) {
			if (resource.len != 0 || resource.info != 0 || at != length)
				return CLUSTER_RECOVERY_NATIVE_SHAPE;
			scan->primary = CLUSTER_RECOVERY_NATIVE_PREPARE;
			record->top_xid = header.xid;
			record->database = header.database;
			record->nsubxacts = header.nsubxacts;
			record->xact_time = header.prepared_at;
			record->relcache_init_file = header.initfileinval;
			record->origin = (xl_xact_origin){ header.origin_lsn, header.origin_timestamp };
			return CLUSTER_RECOVERY_NATIVE_MORE;
		}
		if (resource.rmid > TWOPHASE_RM_MAX_ID)
			return CLUSTER_RECOVERY_NATIVE_UNSUPPORTED;
		aligned = MAXALIGN((size_t)resource.len);
		if (aligned < resource.len || aligned > length - at)
			return CLUSTER_RECOVERY_NATIVE_SHAPE;
		at += aligned;
		record->prepared_resource_count++;
	}
	return CLUSTER_RECOVERY_NATIVE_SHAPE; /* missing END, never an empty plan */
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
		scan->record.tt_commit = parsed.tt_commit;
		scan->regions[4] = (NativeRegion){ CLUSTER_RECOVERY_NATIVE_TT_COMMIT,
										   (const char *)&scan->record.tt_commit, 1,
										   sizeof(scan->record.tt_commit) };
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

	if (record->header.xl_rmid == RM_CLOG_ID || record->header.xl_rmid == RM_COMMIT_TS_ID)
		return native_slru(scan, opcode, record->header.xl_rmid == RM_COMMIT_TS_ID);
	if (record->header.xl_rmid == RM_RELMAP_ID)
		return native_relmap(scan, opcode);
	if (record->header.xl_rmid == RM_MULTIXACT_ID)
		return native_multixact(scan, opcode);
	if (record->header.xl_rmid == RM_CLUSTER_UNDO_ID) {
		/* Historical shape only. In particular, a valid release certificate
		 * does not authorize release here; its complete bytes remain owned.
		 * Legacy kinds without a generation require a separate binding. */
		if (!cluster_undo_decode(reader, &scan->undo) || !cluster_undo_inspect_pi(&scan->undo)
			|| scan->undo.instance != record->source.claim.identity.origin_thread_id
			|| (scan->undo.kind == CLUSTER_UNDO_KIND_TT_BIND
				&& scan->undo.expected_generation == UINT32_MAX)
			|| ((scan->undo.kind == CLUSTER_UNDO_KIND_BLOCK_WRITE
				 || scan->undo.kind == CLUSTER_UNDO_KIND_BLOCK_WRITE_MULTI)
				&& scan->undo.block_no >= UNDO_BLOCKS_PER_SEGMENT))
			return CLUSTER_RECOVERY_NATIVE_SHAPE;
		scan->primary = CLUSTER_RECOVERY_NATIVE_UNDO;
		return CLUSTER_RECOVERY_NATIVE_MORE;
	}
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
	case XLOG_XACT_PREPARE:
		return native_prepare(scan);
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
		record->nsubxacts = assignment.nsubxacts;
		if (assignment.nsubxacts != 0)
			record->subxacts_offset = MinSizeOfXactAssignment;
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
cluster_recovery_native_scan_primary(const ClusterRecoveryNativeScan *scan,
									 ClusterRecoveryNativeEffect *out)
{
	if (out == NULL)
		return CLUSTER_RECOVERY_NATIVE_INVALID;
	memset(out, 0, sizeof(*out));
	if (scan == NULL)
		return CLUSTER_RECOVERY_NATIVE_INVALID;
	if (scan->status != CLUSTER_RECOVERY_NATIVE_MORE)
		return scan->status;
	out->kind = scan->primary;
	out->record = &scan->record;
	out->bytes = scan->payload;
	out->length = scan->record.payload_length;
	if (scan->primary == CLUSTER_RECOVERY_NATIVE_UNDO) {
		out->bytes = &scan->undo;
		out->length = sizeof(scan->undo);
	}
	return CLUSTER_RECOVERY_NATIVE_EFFECT;
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
		return cluster_recovery_native_scan_primary(scan, out);
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
	if (scan->primary == CLUSTER_RECOVERY_NATIVE_PREPARE
		&& scan->prepared_cursor < scan->record.payload_length) {
		TwoPhaseRecordOnDisk resource;
		/* Every header/body, aligned length and the final sentinel were
		 * bounded before begin exported this immutable owned payload. */
		memcpy(&resource, scan->payload + scan->prepared_cursor, sizeof(resource));
		out->kind = resource.rmid == TWOPHASE_RM_END_ID ? CLUSTER_RECOVERY_NATIVE_PREPARE_END
														: CLUSTER_RECOVERY_NATIVE_PREPARE_RESOURCE;
		out->ordinal = scan->prepared_ordinal++;
		out->record = &scan->record;
		out->bytes = scan->payload + scan->prepared_cursor;
		out->length = MAXALIGN(sizeof(resource)) + MAXALIGN((size_t)resource.len);
		scan->prepared_cursor += out->length;
		return CLUSTER_RECOVERY_NATIVE_EFFECT;
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
