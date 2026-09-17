/*-------------------------------------------------------------------------
 *
 * cluster_side_xact.c
 *    RF-SIDE immutable XACT decode implementation.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "access/rmgr.h"
#include "access/twophase.h"
#include "access/twophase_rmgr.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "cluster/cluster_side_xact.h"
#include "cluster/cluster_scn.h"
#include "cluster/cluster_tt_durable.h"
#include "cluster/cluster_tt_slot.h"
#include "cluster/cluster_uba.h"
#include "cluster/cluster_xid_stripe.h"
#include "cluster/storage/cluster_undo_alloc.h"
#include "cluster/storage/cluster_undo_xlog.h"

#define RF_SIDE_XACT_KNOWN_XINFO                                                                   \
	(XACT_XINFO_HAS_DBINFO | XACT_XINFO_HAS_SUBXACTS | XACT_XINFO_HAS_RELFILELOCATORS              \
	 | XACT_XINFO_HAS_INVALS | XACT_XINFO_HAS_TWOPHASE | XACT_XINFO_HAS_ORIGIN                     \
	 | XACT_XINFO_HAS_AE_LOCKS | XACT_XINFO_HAS_GID | XACT_XINFO_HAS_DROPPED_STATS                 \
	 | XACT_XINFO_HAS_SCN | XACT_XINFO_HAS_TT_COMMIT)
#define RF_SIDE_XACT_TWOPHASE_MAGIC UINT32_C(0x57F94534)

typedef struct RfSideXactCursorV1 {
	const char *position;
	const char *end;
} RfSideXactCursorV1;

typedef struct RfSideTwoPhaseRecordOnDiskV1 {
	uint32 len;
	TwoPhaseRmgrId rmid;
	uint16 info;
} RfSideTwoPhaseRecordOnDiskV1;

StaticAssertDecl(sizeof(RfSideTwoPhaseRecordOnDiskV1) == 8,
				 "RF-SIDE two-phase record header must match PostgreSQL's 8-byte layout");

static void
side_xact_payload_free(void *payload)
{
#ifdef RF_SIDE_XACT_TESTING
	free(payload);
#else
	pfree(payload);
#endif
}

static bool
side_xact_take(RfSideXactCursorV1 *cursor, Size bytes, const char **start)
{
	if (cursor == NULL || bytes > (Size)(cursor->end - cursor->position))
		return false;
	if (start != NULL)
		*start = cursor->position;
	cursor->position += bytes;
	return true;
}

static bool
side_xact_take_counted(RfSideXactCursorV1 *cursor, Size element_size)
{
	const char *count_bytes;
	int count;

	if (!side_xact_take(cursor, sizeof(count), &count_bytes))
		return false;
	memcpy(&count, count_bytes, sizeof(count));
	return count >= 0 && (Size)count <= (Size)(cursor->end - cursor->position) / element_size
		   && side_xact_take(cursor, (Size)count * element_size, NULL);
}

static bool
side_xact_take_aligned_array(RfSideXactCursorV1 *cursor, int32 count, Size element_size)
{
	Size bytes;

	if (count < 0 || (Size)count > (Size)(cursor->end - cursor->position) / element_size)
		return false;
	bytes = (Size)count * element_size;
	return side_xact_take(cursor, MAXALIGN(bytes), NULL);
}

static bool
side_xact_prepare_payload_valid(const char *data, uint32 data_len, RfSideXactOperationV1 *candidate)
{
	RfSideXactCursorV1 cursor;
	xl_xact_prepare header;
	ClusterTT2PCParsed parsed_tt;
	const char *gid;
	bool found_cluster_tt = false;
	bool found_end = false;
	uint16 i;

	if (candidate == NULL || data == NULL || data_len < MAXALIGN(sizeof(header)))
		return false;
	memcpy(&header, data, sizeof(header));
	if (header.magic != RF_SIDE_XACT_TWOPHASE_MAGIC
		|| (uint64)header.total_len != (uint64)data_len + sizeof(pg_crc32c) || header.gidlen <= 1
		|| header.gidlen > GIDSIZE)
		return false;
	cursor.position = data + MAXALIGN(sizeof(header));
	cursor.end = data + data_len;
	if (!side_xact_take(&cursor, MAXALIGN(header.gidlen), &gid) || gid[header.gidlen - 1] != '\0'
		|| memchr(gid, '\0', header.gidlen - 1) != NULL
		|| !side_xact_take_aligned_array(&cursor, header.nsubxacts, sizeof(TransactionId))
		|| !side_xact_take_aligned_array(&cursor, header.ncommitrels, sizeof(RelFileLocator))
		|| !side_xact_take_aligned_array(&cursor, header.nabortrels, sizeof(RelFileLocator))
		|| !side_xact_take_aligned_array(&cursor, header.ncommitstats, sizeof(xl_xact_stats_item))
		|| !side_xact_take_aligned_array(&cursor, header.nabortstats, sizeof(xl_xact_stats_item))
		|| !side_xact_take_aligned_array(&cursor, header.ninvalmsgs,
										 sizeof(SharedInvalidationMessage)))
		return false;
	while (cursor.position < cursor.end) {
		RfSideTwoPhaseRecordOnDiskV1 disk_record;
		const char *record_header;
		const char *record_data;
		Size aligned_len;

		if (!side_xact_take(&cursor, MAXALIGN(sizeof(disk_record)), &record_header))
			return false;
		memcpy(&disk_record, record_header, sizeof(disk_record));
		if (disk_record.rmid == TWOPHASE_RM_END_ID) {
			if (disk_record.len != 0 || disk_record.info != 0 || cursor.position != cursor.end)
				return false;
			found_end = true;
			break;
		}
		if (disk_record.rmid > TWOPHASE_RM_MAX_ID
			|| disk_record.len > (uint32)(cursor.end - cursor.position))
			return false;
		aligned_len = MAXALIGN((Size)disk_record.len);
		if (aligned_len < disk_record.len || !side_xact_take(&cursor, aligned_len, &record_data))
			return false;
		if (disk_record.rmid != TWOPHASE_RM_CLUSTER_TT_ID)
			continue;
		if (found_cluster_tt || disk_record.info != 0
			|| !cluster_tt_2pc_parse_record(record_data, disk_record.len, &parsed_tt)
			|| parsed_tt.nbindings == 0)
			return false;
		found_cluster_tt = true;
		candidate->prepared_record_version = parsed_tt.version;
		candidate->prepared_binding_count = parsed_tt.nbindings;
		candidate->prepared_sublink_count = parsed_tt.nsublinks;
		memcpy(candidate->prepared_bindings, parsed_tt.bindings,
			   (Size)parsed_tt.nbindings * sizeof(*parsed_tt.bindings));
		if (parsed_tt.nsublinks > 0)
			memcpy(candidate->prepared_sublinks, parsed_tt.sublinks,
				   (Size)parsed_tt.nsublinks * sizeof(*parsed_tt.sublinks));
		if (parsed_tt.heads != NULL)
			memcpy(candidate->prepared_heads, parsed_tt.heads,
				   (Size)parsed_tt.nbindings * sizeof(*parsed_tt.heads));
	}
	if (!found_end || !found_cluster_tt)
		return false;
	for (i = 0; i < candidate->prepared_binding_count; i++) {
		const ClusterTT2PCBinding *binding = &candidate->prepared_bindings[i];

		if (binding->undo_segment_id == 0
			|| ((binding->undo_segment_id - 1) / CLUSTER_UNDO_SEGS_PER_INSTANCE) + 1
				   != candidate->origin_thread
			|| binding->slot_offset >= TT_SLOTS_PER_SEGMENT || binding->wrap == 0
			|| binding->cluster_epoch == 0 || !TransactionIdIsNormal(binding->xid))
			return false;
	}
	if (!OidIsValid(header.owner))
		return false;
	candidate->xid = header.xid;
	candidate->database = header.database;
	candidate->prepared_owner = header.owner;
	candidate->prepared_at = header.prepared_at;
	candidate->prepare_payload_length = data_len;
	memcpy(candidate->prepare_gid, gid, header.gidlen);
	return true;
}

static bool
side_xact_prepare_shape_valid(XLogReaderState *record, RfSideXactOperationV1 *candidate)
{
	return record != NULL
		   && side_xact_prepare_payload_valid(XLogRecGetData(record), XLogRecGetDataLen(record),
											  candidate);
}

static bool
side_xact_completion_shape_valid(XLogReaderState *record, bool commit)
{
	RfSideXactCursorV1 cursor;
	const char *xinfo_bytes;
	uint32 xinfo = 0;
	Size base_size = commit ? MinSizeOfXactCommit : MinSizeOfXactAbort;
	uint32 data_len;
	const char *data;
	const char *terminator;

	data_len = XLogRecGetDataLen(record);
	data = XLogRecGetData(record);
	if (data == NULL || data_len < base_size)
		return false;
	cursor.position = data + base_size;
	cursor.end = data + data_len;
	if ((XLogRecGetInfo(record) & XLOG_XACT_HAS_INFO) != 0) {
		if (!side_xact_take(&cursor, sizeof(xl_xact_xinfo), &xinfo_bytes))
			return false;
		memcpy(&xinfo, xinfo_bytes, sizeof(xinfo));
	}
	if ((xinfo & ~RF_SIDE_XACT_KNOWN_XINFO) != 0
		|| ((xinfo & XACT_XINFO_HAS_GID) != 0 && (xinfo & XACT_XINFO_HAS_TWOPHASE) == 0)
		|| (!commit && (xinfo & XACT_XINFO_HAS_TT_COMMIT) != 0))
		return false;
	if ((xinfo & XACT_XINFO_HAS_DBINFO) != 0
		&& !side_xact_take(&cursor, sizeof(xl_xact_dbinfo), NULL))
		return false;
	if ((xinfo & XACT_XINFO_HAS_SUBXACTS) != 0
		&& !side_xact_take_counted(&cursor, sizeof(TransactionId)))
		return false;
	if ((xinfo & XACT_XINFO_HAS_RELFILELOCATORS) != 0
		&& !side_xact_take_counted(&cursor, sizeof(RelFileLocator)))
		return false;
	if ((xinfo & XACT_XINFO_HAS_DROPPED_STATS) != 0
		&& !side_xact_take_counted(&cursor, sizeof(xl_xact_stats_item)))
		return false;
	if (commit && (xinfo & XACT_XINFO_HAS_INVALS) != 0
		&& !side_xact_take_counted(&cursor, sizeof(SharedInvalidationMessage)))
		return false;
	if ((xinfo & XACT_XINFO_HAS_TWOPHASE) != 0) {
		if (!side_xact_take(&cursor, sizeof(xl_xact_twophase), NULL))
			return false;
		if ((xinfo & XACT_XINFO_HAS_GID) != 0) {
			terminator = memchr(cursor.position, '\0',
								Min((Size)GIDSIZE, (Size)(cursor.end - cursor.position)));
			if (terminator == NULL || terminator == cursor.position
				|| !side_xact_take(&cursor, (Size)(terminator - cursor.position) + 1, NULL))
				return false;
		}
	}
	if ((xinfo & XACT_XINFO_HAS_ORIGIN) != 0
		&& !side_xact_take(&cursor, sizeof(xl_xact_origin), NULL))
		return false;
	if ((xinfo & XACT_XINFO_HAS_SCN) != 0 && !side_xact_take(&cursor, sizeof(xl_xact_scn), NULL))
		return false;
	if (commit && (xinfo & XACT_XINFO_HAS_TT_COMMIT) != 0
		&& !side_xact_take(&cursor, sizeof(xl_xact_tt_commit), NULL))
		return false;
	return cursor.position == cursor.end;
}

static bool
side_xact_no_commit_effects(const xl_xact_parsed_commit *parsed)
{
	return parsed->nsubxacts == 0 && parsed->nrels == 0 && parsed->nstats == 0
		   && parsed->nmsgs == 0;
}

static bool
side_xact_no_abort_effects(const xl_xact_parsed_abort *parsed)
{
	return parsed->nsubxacts == 0 && parsed->nrels == 0 && parsed->nstats == 0;
}

static bool
side_xact_tt_delta_valid(const xl_xact_tt_commit *delta, uint16 origin_thread, TransactionId xid,
						 SCN scn)
{
	return delta != NULL && delta->instance == origin_thread && delta->segment_id != 0
		   && delta->segment_generation != UINT32_MAX && delta->slot_offset < TT_SLOTS_PER_SEGMENT
		   && delta->wrap != TT_WRAP_INVALID && delta->xid == xid
		   && delta->format_version == CLUSTER_XACT_TT_COMMIT_VERSION && delta->flags == 0
		   && delta->reserved == 0 && delta->commit_scn == scn;
}

static bool
side_xact_key_matches_binding(const ClusterTTStatusKey *key, const ClusterTT2PCBinding *binding,
							  uint16 origin_thread)
{
	return key->origin_node_id == origin_thread - 1
		   && key->undo_segment_id == binding->undo_segment_id
		   && key->tt_slot_id == cluster_tt_slot_offset_to_id(binding->slot_offset)
		   && key->cluster_epoch == binding->cluster_epoch && key->local_xid == binding->xid
		   && key->_reserved == 0 && key->_reserved2 == 0;
}

static int
side_xact_binding_for_key(const RfSideXactOperationV1 *operation, const ClusterTTStatusKey *key)
{
	uint16 i;

	for (i = 0; i < operation->prepared_binding_count; i++)
		if (side_xact_key_matches_binding(key, &operation->prepared_bindings[i],
										  operation->origin_thread))
			return (int)i;
	return -1;
}

static bool
side_xact_prepared_material_valid(const RfSideXactOperationV1 *operation)
{
	bool top_seen = false;
	uint16 i;
	uint32 j;

	if ((operation->prepared_record_version != CLUSTER_TT_2PC_VERSION_NO_HEADS
		 && operation->prepared_record_version != CLUSTER_TT_2PC_VERSION)
		|| operation->prepared_binding_count == 0
		|| operation->prepared_binding_count > CLUSTER_TT_2PC_MAX_BINDINGS
		|| operation->prepared_sublink_count > CLUSTER_TT_2PC_MAX_SUBLINKS)
		return false;
	for (i = 0; i < operation->prepared_binding_count; i++) {
		const ClusterTT2PCBinding *binding = &operation->prepared_bindings[i];
		uint32 owner;
		uint32 head_segment;
		uint32 head_block;
		uint16 head_slot;
		uint16 head_row;
		uint16 k;

		if (binding->undo_segment_id == 0)
			return false;
		owner = ((binding->undo_segment_id - 1) / CLUSTER_UNDO_SEGS_PER_INSTANCE) + 1;
		if (owner != operation->origin_thread || binding->slot_offset >= TT_SLOTS_PER_SEGMENT
			|| binding->wrap == 0 || binding->cluster_epoch == 0
			|| !TransactionIdIsNormal(binding->xid))
			return false;
		for (k = 0; k < i; k++)
			if (operation->prepared_bindings[k].undo_segment_id == binding->undo_segment_id
				&& operation->prepared_bindings[k].slot_offset == binding->slot_offset)
				return false;
		if (binding->xid == operation->xid)
			top_seen = true;
		if (operation->prepared_record_version == CLUSTER_TT_2PC_VERSION_NO_HEADS) {
			if (!UBA_is_invalid(operation->prepared_heads[i]))
				return false;
		} else if (!UBA_is_invalid(operation->prepared_heads[i])
				   && (!uba_decode_record(operation->prepared_heads[i], &head_segment, &head_block,
										  &head_slot, &head_row)
					   || head_segment != binding->undo_segment_id
					   || head_slot != binding->slot_offset))
			return false;
	}
	if (!top_seen)
		return false;
	for (j = 0; j < operation->prepared_sublink_count; j++) {
		const ClusterTT2PCSubLink *link = &operation->prepared_sublinks[j];
		uint32 k;

		if (side_xact_binding_for_key(operation, &link->child_key) < 0
			|| side_xact_binding_for_key(operation, &link->parent_key) < 0
			|| link->child_key.local_xid == link->parent_key.local_xid)
			return false;
		for (k = 0; k < j; k++)
			if (memcmp(&operation->prepared_sublinks[k].child_key, &link->child_key,
					   sizeof(link->child_key))
				== 0)
				return false;
	}
	for (i = 0; i < operation->prepared_binding_count; i++) {
		ClusterTTStatusKey key;
		uint32 depth;

		if (operation->prepared_bindings[i].xid == operation->xid)
			continue;
		memset(&key, 0, sizeof(key));
		key.origin_node_id = operation->origin_thread - 1;
		key.undo_segment_id = (uint16)operation->prepared_bindings[i].undo_segment_id;
		key.tt_slot_id = cluster_tt_slot_offset_to_id(operation->prepared_bindings[i].slot_offset);
		key.cluster_epoch = operation->prepared_bindings[i].cluster_epoch;
		key.local_xid = operation->prepared_bindings[i].xid;
		for (depth = 0; depth <= operation->prepared_sublink_count; depth++) {
			const ClusterTT2PCSubLink *link = NULL;

			for (j = 0; j < operation->prepared_sublink_count; j++)
				if (memcmp(&operation->prepared_sublinks[j].child_key, &key, sizeof(key)) == 0) {
					link = &operation->prepared_sublinks[j];
					break;
				}
			if (link == NULL)
				return false;
			key = link->parent_key;
			if (key.local_xid == operation->xid)
				break;
		}
		if (key.local_xid != operation->xid)
			return false;
	}
	return true;
}

static TimestampTz
side_xact_commit_timestamp(const xl_xact_parsed_commit *parsed)
{
	return (parsed->xinfo & XACT_XINFO_HAS_ORIGIN) != 0 ? parsed->origin_timestamp
														: parsed->xact_time;
}

bool
rf_side_xact_structural_preflight_v1(const RfSideXactOperationV1 *operation)
{
	uint8 binding_seen = 0;
	const char *gid_end;
	Size i;

	if (operation == NULL || operation->system_identifier == 0
		|| operation->kind == RF_SIDE_XACT_INVALID || operation->origin_thread == 0
		|| operation->origin_thread > 128 || operation->reserved_zero != 0
		|| !TransactionIdIsNormal(operation->xid))
		return false;
	for (i = 0; i < sizeof(operation->reserved49); i++)
		if (operation->reserved49[i] != 0)
			return false;
	for (i = 0; i < sizeof(operation->prepare_binding); i++)
		binding_seen |= operation->prepare_binding[i];
	gid_end = memchr(operation->prepare_gid, '\0', sizeof(operation->prepare_gid));

	switch (operation->kind) {
	case RF_SIDE_XACT_COMMIT:
		return SCN_VALID(operation->terminal_scn) && operation->terminal_timestamp != 0
			   && operation->has_tt_delta
			   && side_xact_tt_delta_valid(&operation->tt_delta, operation->origin_thread,
										   operation->xid, operation->terminal_scn)
			   && binding_seen == 0 && operation->prepared_owner == InvalidOid
			   && operation->prepare_payload_length == 0 && operation->prepared_at == 0
			   && operation->prepare_gid[0] == '\0';
	case RF_SIDE_XACT_ABORT:
		return SCN_VALID(operation->terminal_scn) && operation->terminal_timestamp == 0
			   && !operation->has_tt_delta && binding_seen == 0
			   && operation->prepared_owner == InvalidOid && operation->prepare_payload_length == 0
			   && operation->prepared_at == 0 && operation->prepare_gid[0] == '\0';
	case RF_SIDE_XACT_PREPARE:
		return OidIsValid(operation->database) && OidIsValid(operation->prepared_owner)
			   && operation->prepare_payload_length > 0 && gid_end != NULL
			   && gid_end != operation->prepare_gid && operation->terminal_scn == InvalidScn
			   && operation->terminal_timestamp == 0 && !operation->has_tt_delta
			   && binding_seen != 0 && side_xact_prepared_material_valid(operation);
	case RF_SIDE_XACT_COMMIT_PREPARED:
		return OidIsValid(operation->database) && operation->prepared_owner == InvalidOid
			   && operation->prepare_payload_length == 0 && operation->prepared_at == 0
			   && gid_end != NULL && gid_end != operation->prepare_gid
			   && SCN_VALID(operation->terminal_scn) && operation->terminal_timestamp != 0
			   && !operation->has_tt_delta && binding_seen != 0;
	case RF_SIDE_XACT_ABORT_PREPARED:
		return OidIsValid(operation->database) && operation->prepared_owner == InvalidOid
			   && operation->prepare_payload_length == 0 && operation->prepared_at == 0
			   && gid_end != NULL && gid_end != operation->prepare_gid
			   && SCN_VALID(operation->terminal_scn) && operation->terminal_timestamp == 0
			   && !operation->has_tt_delta && binding_seen != 0;
	default:
		return false;
	}
}

bool
rf_side_xact_decode_v1(XLogReaderState *record, uint64 system_identifier, uint16 origin_thread,
					   RfSideXactOperationV1 *out)
{
	RfSideXactOperationV1 candidate;
	uint8 opcode;
	TransactionId xid;

	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	if (record == NULL || record->record == NULL || system_identifier == 0 || origin_thread == 0
		|| origin_thread > 128 || XLogRecGetRmid(record) != RM_XACT_ID)
		return false;
	memset(&candidate, 0, sizeof(candidate));
	candidate.system_identifier = system_identifier;
	candidate.origin_thread = origin_thread;
	opcode = XLogRecGetInfo(record) & XLOG_XACT_OPMASK;
	xid = XLogRecGetXid(record);

	switch (opcode) {
	case XLOG_XACT_COMMIT: {
		xl_xact_parsed_commit parsed;

		if (!side_xact_completion_shape_valid(record, true) || !TransactionIdIsNormal(xid))
			return false;
		ParseCommitRecord(XLogRecGetInfo(record), (xl_xact_commit *)XLogRecGetData(record),
						  &parsed);
		if (!side_xact_no_commit_effects(&parsed) || (parsed.xinfo & XACT_XINFO_HAS_TWOPHASE) != 0
			|| !SCN_VALID(parsed.scn) || side_xact_commit_timestamp(&parsed) == 0
			|| !parsed.has_tt_commit
			|| !side_xact_tt_delta_valid(&parsed.tt_commit, origin_thread, xid, parsed.scn))
			return false;
		candidate.kind = RF_SIDE_XACT_COMMIT;
		candidate.xid = xid;
		candidate.database = parsed.dbId;
		candidate.xinfo = parsed.xinfo;
		candidate.terminal_scn = parsed.scn;
		candidate.terminal_timestamp = side_xact_commit_timestamp(&parsed);
		candidate.has_tt_delta = true;
		candidate.tt_delta = parsed.tt_commit;
		break;
	}
	case XLOG_XACT_ABORT: {
		xl_xact_parsed_abort parsed;

		if (!side_xact_completion_shape_valid(record, false) || !TransactionIdIsNormal(xid))
			return false;
		ParseAbortRecord(XLogRecGetInfo(record), (xl_xact_abort *)XLogRecGetData(record), &parsed);
		if (!side_xact_no_abort_effects(&parsed) || (parsed.xinfo & XACT_XINFO_HAS_TWOPHASE) != 0
			|| !SCN_VALID(parsed.scn))
			return false;
		candidate.kind = RF_SIDE_XACT_ABORT;
		candidate.xid = xid;
		candidate.database = parsed.dbId;
		candidate.xinfo = parsed.xinfo;
		candidate.terminal_scn = parsed.scn;
		break;
	}
	case XLOG_XACT_PREPARE: {
		xl_xact_prepare *xlrec;
		xl_xact_parsed_prepare parsed;

		if (!side_xact_prepare_shape_valid(record, &candidate))
			return false;
		xlrec = (xl_xact_prepare *)XLogRecGetData(record);
		ParsePrepareRecord(XLogRecGetInfo(record), xlrec, &parsed);
		xid = parsed.twophase_xid;
		if (!TransactionIdIsNormal(xid) || !OidIsValid(parsed.dbId) || parsed.nsubxacts != 0
			|| parsed.nrels != 0 || parsed.nabortrels != 0 || parsed.nstats != 0
			|| parsed.nabortstats != 0 || parsed.nmsgs != 0 || xlrec->initfileinval
			|| !cluster_remote_xact_prepare_digest_v2(system_identifier, origin_thread - 1, xid,
													  parsed.dbId, parsed.twophase_gid,
													  candidate.prepare_binding))
			return false;
		candidate.kind = RF_SIDE_XACT_PREPARE;
		candidate.xid = xid;
		candidate.database = parsed.dbId;
		break;
	}
	case XLOG_XACT_COMMIT_PREPARED: {
		xl_xact_parsed_commit parsed;

		if (!side_xact_completion_shape_valid(record, true))
			return false;
		ParseCommitRecord(XLogRecGetInfo(record), (xl_xact_commit *)XLogRecGetData(record),
						  &parsed);
		xid = parsed.twophase_xid;
		if (!TransactionIdIsNormal(xid) || !side_xact_no_commit_effects(&parsed)
			|| (parsed.xinfo
				& (XACT_XINFO_HAS_TWOPHASE | XACT_XINFO_HAS_GID | XACT_XINFO_HAS_DBINFO))
				   != (XACT_XINFO_HAS_TWOPHASE | XACT_XINFO_HAS_GID | XACT_XINFO_HAS_DBINFO)
			|| !SCN_VALID(parsed.scn) || side_xact_commit_timestamp(&parsed) == 0
			|| parsed.has_tt_commit
			|| !cluster_remote_xact_prepare_digest_v2(system_identifier, origin_thread - 1, xid,
													  parsed.dbId, parsed.twophase_gid,
													  candidate.prepare_binding))
			return false;
		candidate.kind = RF_SIDE_XACT_COMMIT_PREPARED;
		candidate.xid = xid;
		candidate.database = parsed.dbId;
		candidate.xinfo = parsed.xinfo;
		candidate.terminal_scn = parsed.scn;
		candidate.terminal_timestamp = side_xact_commit_timestamp(&parsed);
		strlcpy(candidate.prepare_gid, parsed.twophase_gid, sizeof(candidate.prepare_gid));
		break;
	}
	case XLOG_XACT_ABORT_PREPARED: {
		xl_xact_parsed_abort parsed;

		if (!side_xact_completion_shape_valid(record, false))
			return false;
		ParseAbortRecord(XLogRecGetInfo(record), (xl_xact_abort *)XLogRecGetData(record), &parsed);
		xid = parsed.twophase_xid;
		if (!TransactionIdIsNormal(xid) || !side_xact_no_abort_effects(&parsed)
			|| (parsed.xinfo
				& (XACT_XINFO_HAS_TWOPHASE | XACT_XINFO_HAS_GID | XACT_XINFO_HAS_DBINFO))
				   != (XACT_XINFO_HAS_TWOPHASE | XACT_XINFO_HAS_GID | XACT_XINFO_HAS_DBINFO)
			|| !SCN_VALID(parsed.scn)
			|| !cluster_remote_xact_prepare_digest_v2(system_identifier, origin_thread - 1, xid,
													  parsed.dbId, parsed.twophase_gid,
													  candidate.prepare_binding))
			return false;
		candidate.kind = RF_SIDE_XACT_ABORT_PREPARED;
		candidate.xid = xid;
		candidate.database = parsed.dbId;
		candidate.xinfo = parsed.xinfo;
		candidate.terminal_scn = parsed.scn;
		strlcpy(candidate.prepare_gid, parsed.twophase_gid, sizeof(candidate.prepare_gid));
		break;
	}
	default:
		return false;
	}
	if (!rf_side_xact_structural_preflight_v1(&candidate))
		return false;
	*out = candidate;
	return true;
}

static RfSideXactApplyResultV1
side_xact_apply_commit_v1(const RfSideXactOperationV1 *operation)
{
	ClusterRemoteXactMutationV2 mutation;
	ClusterRemoteXactOutcome outcome;
	TimestampTz timestamp;
	SCN post_scn;
	SCN durable_scn;
	uint16 post_wrap;
	uint16 durable_segment;
	uint16 durable_slot;
	uint16 durable_wrap;
	bool post_wrap_valid;

	/* Canonical TT truth first, using only the frozen typed delta. */
	cluster_tt_durable_redo_stamp_slot_exact(
		operation->tt_delta.instance, operation->tt_delta.segment_id,
		operation->tt_delta.segment_generation, operation->tt_delta.slot_offset,
		operation->tt_delta.wrap, operation->tt_delta.xid, operation->tt_delta.commit_scn);
	if (cluster_tt_slot_durable_resolve_by_xid_origin(
			operation->origin_thread - 1, operation->xid, operation->tt_delta.wrap, &durable_scn,
			&durable_segment, &durable_slot, &durable_wrap)
			!= CLUSTER_TT_DURABLE_RESOLVED_SCN
		|| durable_scn != operation->terminal_scn
		|| durable_segment != operation->tt_delta.segment_id
		|| durable_slot != operation->tt_delta.slot_offset
		|| durable_wrap != operation->tt_delta.wrap)
		return RF_SIDE_XACT_APPLY_POST_READ_FAILED;

	cluster_scn_recovery_replay_observe(operation->terminal_scn);
	mutation = cluster_remote_xact_store_terminal_v2(
		operation->origin_thread - 1, operation->xid, false, NULL, CLUSTER_REMOTE_XACT_COMMITTED,
		operation->terminal_scn, operation->terminal_timestamp, true, operation->tt_delta.wrap);
	if (mutation == CLUSTER_REMOTE_XACT_MUTATION_CONFLICT)
		return RF_SIDE_XACT_APPLY_CONFLICT;
	if (mutation != CLUSTER_REMOTE_XACT_MUTATION_STORED
		&& mutation != CLUSTER_REMOTE_XACT_MUTATION_UNCHANGED)
		return RF_SIDE_XACT_APPLY_BLOCKED;

	outcome = cluster_remote_commit_outcome_ex(operation->origin_thread - 1, operation->xid,
											   &post_scn, &post_wrap, &post_wrap_valid);
	if (outcome != CLUSTER_REMOTE_XACT_COMMITTED || post_scn != operation->terminal_scn
		|| !post_wrap_valid || post_wrap != operation->tt_delta.wrap
		|| !cluster_remote_commit_timestamp(operation->origin_thread - 1, operation->xid,
											&timestamp)
		|| timestamp != operation->terminal_timestamp)
		return RF_SIDE_XACT_APPLY_POST_READ_FAILED;
	return RF_SIDE_XACT_APPLY_OK;
}

static RfSideXactApplyResultV1
side_xact_map_pending_result(TwoPhaseRecoveryPendingResult result)
{
	switch (result) {
	case TWOPHASE_RECOVERY_PENDING_OK:
		return RF_SIDE_XACT_APPLY_OK;
	case TWOPHASE_RECOVERY_PENDING_CONFLICT:
		return RF_SIDE_XACT_APPLY_CONFLICT;
	case TWOPHASE_RECOVERY_PENDING_POST_READ_FAILED:
		return RF_SIDE_XACT_APPLY_POST_READ_FAILED;
	case TWOPHASE_RECOVERY_PENDING_BLOCKED:
	default:
		return RF_SIDE_XACT_APPLY_BLOCKED;
	}
}

static RfSideXactApplyResultV1
side_xact_read_prepared_material(const RfSideXactOperationV1 *operation,
								 RfSideXactOperationV1 *prepared, uint8 **payload_out,
								 uint32 *payload_length_out)
{
	TwoPhaseRecoveryPendingResult pending;
	void *payload = NULL;
	uint32 payload_length = 0;
	uint8 digest[CLUSTER_REMOTE_XACT_PREPARE_DIGEST_BYTES];

	if (prepared == NULL || payload_out == NULL || payload_length_out == NULL)
		return RF_SIDE_XACT_APPLY_BLOCKED;
	memset(prepared, 0, sizeof(*prepared));
	*payload_out = NULL;
	*payload_length_out = 0;
	pending = TwoPhaseRecoveryPendingReadExact(operation->xid, operation->database,
											   operation->prepare_gid, &payload, &payload_length);
	if (pending != TWOPHASE_RECOVERY_PENDING_OK)
		return side_xact_map_pending_result(pending);
	prepared->system_identifier = operation->system_identifier;
	prepared->origin_thread = operation->origin_thread;
	prepared->kind = RF_SIDE_XACT_PREPARE;
	if (!side_xact_prepare_payload_valid(payload, payload_length, prepared)
		|| !TransactionIdEquals(prepared->xid, operation->xid)
		|| prepared->database != operation->database
		|| strcmp(prepared->prepare_gid, operation->prepare_gid) != 0
		|| !cluster_remote_xact_prepare_digest_v2(
			operation->system_identifier, operation->origin_thread - 1, operation->xid,
			operation->database, operation->prepare_gid, digest)
		|| memcmp(digest, operation->prepare_binding, sizeof(digest)) != 0
		|| !side_xact_prepared_material_valid(prepared)) {
		side_xact_payload_free(payload);
		return RF_SIDE_XACT_APPLY_CONFLICT;
	}
	memcpy(prepared->prepare_binding, digest, sizeof(digest));
	*payload_out = payload;
	*payload_length_out = payload_length;
	return RF_SIDE_XACT_APPLY_OK;
}

static bool
side_xact_prepared_commit_tt_requirements(const RfSideXactOperationV1 *prepared, SCN terminal_scn,
										  RfSideXactCommitPreparedRequirementsV1 *requirements)
{
	uint16 i;

	if (requirements == NULL)
		return false;
	memset(requirements, 0, sizeof(*requirements));
	for (i = 0; i < prepared->prepared_binding_count; i++)
		if (cluster_xid_origin_slot(prepared->prepared_bindings[i].xid)
			!= (int)prepared->origin_thread - 1)
			return false;
	requirements->count = prepared->prepared_binding_count;
	for (i = 0; i < prepared->prepared_binding_count; i++) {
		const ClusterTT2PCBinding *binding = &prepared->prepared_bindings[i];
		RfSideXactTTCommitRequirementV1 *requirement = &requirements->bindings[i];
		TTSlot slot;

		requirement->instance = prepared->origin_thread;
		requirement->segment_id = binding->undo_segment_id;
		requirement->slot_offset = binding->slot_offset;
		requirement->wrap = binding->wrap;
		requirement->xid = binding->xid;
		requirement->commit_scn = terminal_scn;
		if (!cluster_tt_slot_durable_read_exact_stable(
				binding->undo_segment_id, binding->slot_offset, binding->xid, binding->wrap, &slot))
			return false;
		if (slot.status == TT_SLOT_COMMITTED && slot.commit_scn == terminal_scn
			&& UBA_is_invalid(slot.first_undo_block))
			continue;
		if (slot.status == TT_SLOT_ACTIVE && !SCN_VALID(slot.commit_scn)
			&& memcmp(&slot.first_undo_block, &prepared->prepared_heads[i],
					  sizeof(slot.first_undo_block))
				   == 0) {
			requirement->requires_apply = true;
			continue;
		}
		return false;
	}
	return true;
}

RfSideXactApplyResultV1
rf_side_xact_commit_prepared_requirements_v1(
	const RfSideXactOperationV1 *operation,
	RfSideXactCommitPreparedRequirementsV1 *out_requirements)
{
	RfSideXactOperationV1 prepared;
	RfSideXactApplyResultV1 result;
	uint8 *payload = NULL;
	uint32 payload_length = 0;

	if (out_requirements == NULL)
		return RF_SIDE_XACT_APPLY_BLOCKED;
	memset(out_requirements, 0, sizeof(*out_requirements));
	if (!rf_side_xact_structural_preflight_v1(operation)
		|| operation->kind != RF_SIDE_XACT_COMMIT_PREPARED
		|| operation->system_identifier != GetSystemIdentifier()
		|| cluster_xid_origin_slot(operation->xid) != (int)operation->origin_thread - 1)
		return RF_SIDE_XACT_APPLY_BLOCKED;
	result = side_xact_read_prepared_material(operation, &prepared, &payload, &payload_length);
	if (result != RF_SIDE_XACT_APPLY_OK)
		return result;
	result = side_xact_prepared_commit_tt_requirements(&prepared, operation->terminal_scn,
													   out_requirements)
				 ? RF_SIDE_XACT_APPLY_OK
				 : RF_SIDE_XACT_APPLY_BLOCKED;
	side_xact_payload_free(payload);
	return result;
}

static RfSideXactApplyResultV1
side_xact_preflight_commit_prepared(const RfSideXactOperationV1 *operation)
{
	RfSideXactCommitPreparedRequirementsV1 requirements;

	return rf_side_xact_commit_prepared_requirements_v1(operation, &requirements);
}

static RfSideXactApplyResultV1
side_xact_apply_commit_prepared(const RfSideXactOperationV1 *operation)
{
	RfSideXactOperationV1 prepared;
	RfSideXactApplyResultV1 result;
	TwoPhaseRecoveryPendingResult pending;
	ClusterRemoteXactMutationV2 mutation;
	ClusterRemoteXactOutcome outcome;
	uint8 *payload = NULL;
	uint32 payload_length = 0;
	SCN post_scn;
	TimestampTz post_timestamp;
	uint16 post_wrap;
	uint16 top_wrap = 0;
	bool post_wrap_valid;
	uint16 i;
	RfSideXactCommitPreparedRequirementsV1 requirements;

	result = side_xact_read_prepared_material(operation, &prepared, &payload, &payload_length);
	if (result != RF_SIDE_XACT_APPLY_OK)
		return result;
	if (!side_xact_prepared_commit_tt_requirements(&prepared, operation->terminal_scn,
												   &requirements)) {
		side_xact_payload_free(payload);
		return RF_SIDE_XACT_APPLY_BLOCKED;
	}
	for (i = 0; i < requirements.count; i++)
		if (requirements.bindings[i].requires_apply) {
			side_xact_payload_free(payload);
			return RF_SIDE_XACT_APPLY_BLOCKED;
		}
	for (i = 0; i < prepared.prepared_binding_count; i++)
		if (TransactionIdEquals(prepared.prepared_bindings[i].xid, operation->xid)) {
			top_wrap = prepared.prepared_bindings[i].wrap;
			break;
		}
	if (top_wrap == 0) {
		side_xact_payload_free(payload);
		return RF_SIDE_XACT_APPLY_CONFLICT;
	}

	cluster_scn_recovery_replay_observe(operation->terminal_scn);
	mutation = cluster_remote_xact_store_terminal_v2(
		operation->origin_thread - 1, operation->xid, true, operation->prepare_binding,
		CLUSTER_REMOTE_XACT_COMMITTED, operation->terminal_scn, operation->terminal_timestamp, true,
		top_wrap);
	if (mutation == CLUSTER_REMOTE_XACT_MUTATION_CONFLICT) {
		side_xact_payload_free(payload);
		return RF_SIDE_XACT_APPLY_CONFLICT;
	}
	if (mutation != CLUSTER_REMOTE_XACT_MUTATION_STORED
		&& mutation != CLUSTER_REMOTE_XACT_MUTATION_UNCHANGED) {
		side_xact_payload_free(payload);
		return RF_SIDE_XACT_APPLY_BLOCKED;
	}
	outcome = cluster_remote_commit_outcome_ex(operation->origin_thread - 1, operation->xid,
											   &post_scn, &post_wrap, &post_wrap_valid);
	if (outcome != CLUSTER_REMOTE_XACT_COMMITTED || post_scn != operation->terminal_scn
		|| !post_wrap_valid || post_wrap != top_wrap
		|| !cluster_remote_commit_timestamp(operation->origin_thread - 1, operation->xid,
											&post_timestamp)
		|| post_timestamp != operation->terminal_timestamp) {
		side_xact_payload_free(payload);
		return RF_SIDE_XACT_APPLY_POST_READ_FAILED;
	}

	pending = TwoPhaseRecoveryPendingResolveExact(
		operation->xid, operation->database, operation->prepare_gid, payload, payload_length, true);
	side_xact_payload_free(payload);
	return side_xact_map_pending_result(pending);
}

static bool
side_xact_prepared_abort_tt_requirements(const RfSideXactOperationV1 *prepared,
										 RfSideXactAbortPreparedRequirementsV1 *requirements)
{
	uint16 i;

	if (requirements == NULL)
		return false;
	memset(requirements, 0, sizeof(*requirements));
	for (i = 0; i < prepared->prepared_binding_count; i++)
		if (cluster_xid_origin_slot(prepared->prepared_bindings[i].xid)
			!= (int)prepared->origin_thread - 1)
			return false;
	requirements->count = prepared->prepared_binding_count;
	for (i = 0; i < prepared->prepared_binding_count; i++) {
		const ClusterTT2PCBinding *binding = &prepared->prepared_bindings[i];
		RfSideXactTTAbortRequirementV1 *requirement = &requirements->bindings[i];
		TTSlot slot;

		/*
		 * RF-SIDE §2.3/§4: a nonempty prepared undo chain needs a
		 * positive physical rollback-completion proof.  TT_ABORT and
		 * SET_HEAD only preserve the work to do; neither is completion.
		 * Until the rollback owner supplies that proof, retain the native
		 * pending owner and fail closed before projection or lock release.
		 * In particular, a temporarily empty TT head is not a guessed
		 * completion because the immutable prepare payload still names work.
		 */
		if (!UBA_is_invalid(prepared->prepared_heads[i]))
			return false;

		requirement->instance = prepared->origin_thread;
		requirement->segment_id = binding->undo_segment_id;
		requirement->slot_offset = binding->slot_offset;
		requirement->wrap = binding->wrap;
		requirement->xid = binding->xid;
		if (!cluster_tt_slot_durable_read_exact_stable(
				binding->undo_segment_id, binding->slot_offset, binding->xid, binding->wrap, &slot))
			return false;
		if (slot.status == TT_SLOT_ACTIVE && !SCN_VALID(slot.commit_scn)
			&& UBA_is_invalid(slot.first_undo_block)) {
			requirement->requires_apply = true;
			continue;
		}
		if (slot.status == TT_SLOT_ABORTED && !SCN_VALID(slot.commit_scn)
			&& UBA_is_invalid(slot.first_undo_block))
			continue;
		return false;
	}
	return true;
}

RfSideXactApplyResultV1
rf_side_xact_abort_prepared_requirements_v1(const RfSideXactOperationV1 *operation,
											RfSideXactAbortPreparedRequirementsV1 *out_requirements)
{
	RfSideXactOperationV1 prepared;
	RfSideXactApplyResultV1 result;
	uint8 *payload = NULL;
	uint32 payload_length = 0;

	if (out_requirements == NULL)
		return RF_SIDE_XACT_APPLY_BLOCKED;
	memset(out_requirements, 0, sizeof(*out_requirements));
	if (!rf_side_xact_structural_preflight_v1(operation)
		|| operation->kind != RF_SIDE_XACT_ABORT_PREPARED
		|| operation->system_identifier != GetSystemIdentifier()
		|| cluster_xid_origin_slot(operation->xid) != (int)operation->origin_thread - 1)
		return RF_SIDE_XACT_APPLY_BLOCKED;
	result = side_xact_read_prepared_material(operation, &prepared, &payload, &payload_length);
	if (result != RF_SIDE_XACT_APPLY_OK)
		return result;
	result = side_xact_prepared_abort_tt_requirements(&prepared, out_requirements)
				 ? RF_SIDE_XACT_APPLY_OK
				 : RF_SIDE_XACT_APPLY_BLOCKED;
	side_xact_payload_free(payload);
	return result;
}

static RfSideXactApplyResultV1
side_xact_preflight_abort_prepared(const RfSideXactOperationV1 *operation)
{
	RfSideXactAbortPreparedRequirementsV1 requirements;

	return rf_side_xact_abort_prepared_requirements_v1(operation, &requirements);
}

static RfSideXactApplyResultV1
side_xact_apply_abort_prepared(const RfSideXactOperationV1 *operation)
{
	RfSideXactOperationV1 prepared;
	RfSideXactApplyResultV1 result;
	TwoPhaseRecoveryPendingResult pending;
	ClusterRemoteXactMutationV2 mutation;
	ClusterRemoteXactOutcome outcome;
	uint8 *payload = NULL;
	uint32 payload_length = 0;
	SCN post_scn;
	uint16 post_wrap;
	bool post_wrap_valid;
	RfSideXactAbortPreparedRequirementsV1 requirements;
	uint16 i;

	result = side_xact_read_prepared_material(operation, &prepared, &payload, &payload_length);
	if (result != RF_SIDE_XACT_APPLY_OK)
		return result;
	if (!side_xact_prepared_abort_tt_requirements(&prepared, &requirements)) {
		side_xact_payload_free(payload);
		return RF_SIDE_XACT_APPLY_BLOCKED;
	}
	for (i = 0; i < requirements.count; i++)
		if (requirements.bindings[i].requires_apply) {
			side_xact_payload_free(payload);
			return RF_SIDE_XACT_APPLY_BLOCKED;
		}

	cluster_scn_recovery_replay_observe(operation->terminal_scn);
	mutation = cluster_remote_xact_store_terminal_v2(
		operation->origin_thread - 1, operation->xid, true, operation->prepare_binding,
		CLUSTER_REMOTE_XACT_ABORTED, InvalidScn, 0, false, 0);
	if (mutation == CLUSTER_REMOTE_XACT_MUTATION_CONFLICT) {
		side_xact_payload_free(payload);
		return RF_SIDE_XACT_APPLY_CONFLICT;
	}
	if (mutation != CLUSTER_REMOTE_XACT_MUTATION_STORED
		&& mutation != CLUSTER_REMOTE_XACT_MUTATION_UNCHANGED) {
		side_xact_payload_free(payload);
		return RF_SIDE_XACT_APPLY_BLOCKED;
	}
	outcome = cluster_remote_commit_outcome_ex(operation->origin_thread - 1, operation->xid,
											   &post_scn, &post_wrap, &post_wrap_valid);
	if (outcome != CLUSTER_REMOTE_XACT_ABORTED || SCN_VALID(post_scn) || post_wrap_valid) {
		side_xact_payload_free(payload);
		return RF_SIDE_XACT_APPLY_POST_READ_FAILED;
	}

	pending = TwoPhaseRecoveryPendingResolveExact(operation->xid, operation->database,
												  operation->prepare_gid, payload, payload_length,
												  false);
	side_xact_payload_free(payload);
	return side_xact_map_pending_result(pending);
}

static bool
side_xact_prepared_tt_undo_exact(const RfSideXactOperationV1 *operation)
{
	uint16 i;

	/* Classify every top/subtransaction origin before the first target read. */
	for (i = 0; i < operation->prepared_binding_count; i++)
		if (cluster_xid_origin_slot(operation->prepared_bindings[i].xid)
			!= (int)operation->origin_thread - 1)
			return false;

	for (i = 0; i < operation->prepared_binding_count; i++) {
		const ClusterTT2PCBinding *binding = &operation->prepared_bindings[i];
		TTSlot slot;

		if (!cluster_tt_slot_durable_read_exact_stable(
				binding->undo_segment_id, binding->slot_offset, binding->xid, binding->wrap, &slot)
			|| slot.status != TT_SLOT_ACTIVE || SCN_VALID(slot.commit_scn)
			|| memcmp(&slot.first_undo_block, &operation->prepared_heads[i],
					  sizeof(slot.first_undo_block))
				   != 0)
			return false;
	}
	return true;
}

RfSideXactApplyResultV1
rf_side_xact_target_preflight_owned_v1(const RfSideXactOperationV1 *operation,
									   const uint8 *owned_payload, uint32 owned_payload_length)
{
	TwoPhaseRecoveryPendingResult pending;

	if (!rf_side_xact_structural_preflight_v1(operation)
		|| operation->system_identifier != GetSystemIdentifier()
		|| cluster_xid_origin_slot(operation->xid) != (int)operation->origin_thread - 1)
		return RF_SIDE_XACT_APPLY_BLOCKED;

	if (operation->kind == RF_SIDE_XACT_COMMIT)
		return owned_payload == NULL && owned_payload_length == 0 ? RF_SIDE_XACT_APPLY_OK
																  : RF_SIDE_XACT_APPLY_BLOCKED;
	if (operation->kind == RF_SIDE_XACT_COMMIT_PREPARED)
		return owned_payload == NULL && owned_payload_length == 0
				   ? side_xact_preflight_commit_prepared(operation)
				   : RF_SIDE_XACT_APPLY_BLOCKED;
	if (operation->kind == RF_SIDE_XACT_ABORT_PREPARED)
		return owned_payload == NULL && owned_payload_length == 0
				   ? side_xact_preflight_abort_prepared(operation)
				   : RF_SIDE_XACT_APPLY_BLOCKED;
	if (operation->kind != RF_SIDE_XACT_PREPARE || owned_payload == NULL
		|| owned_payload_length != operation->prepare_payload_length
		|| !side_xact_prepared_tt_undo_exact(operation))
		return RF_SIDE_XACT_APPLY_BLOCKED;

	pending = TwoPhaseRecoveryPendingPreflight(
		operation->xid, operation->database, operation->prepared_owner, operation->prepared_at,
		operation->prepare_gid, owned_payload, owned_payload_length);
	return side_xact_map_pending_result(pending);
}

RfSideXactApplyResultV1
rf_side_xact_apply_owned_v1(const RfSideXactOperationV1 *operation, const uint8 *owned_payload,
							uint32 owned_payload_length)
{
	RfSideXactApplyResultV1 preflight;
	TwoPhaseRecoveryPendingResult pending;
	ClusterRemoteXactMutationV2 mutation;

	preflight
		= rf_side_xact_target_preflight_owned_v1(operation, owned_payload, owned_payload_length);
	if (preflight != RF_SIDE_XACT_APPLY_OK)
		return preflight;
	if (operation->kind == RF_SIDE_XACT_COMMIT)
		return side_xact_apply_commit_v1(operation);
	if (operation->kind == RF_SIDE_XACT_COMMIT_PREPARED)
		return side_xact_apply_commit_prepared(operation);
	if (operation->kind == RF_SIDE_XACT_ABORT_PREPARED)
		return side_xact_apply_abort_prepared(operation);

	/*
	 * The native database-scoped pending record is the authority-bearing
	 * state.  Only after its exact durable post-read succeeds may the v2 SLRU
	 * projection be materialized; that projection never grants PREPARED,
	 * terminal state, readiness, or OPEN by itself.
	 */
	pending = TwoPhaseRecoveryPendingInstall(
		operation->xid, operation->database, operation->prepared_owner, operation->prepared_at,
		operation->prepare_gid, owned_payload, owned_payload_length);
	preflight = side_xact_map_pending_result(pending);
	if (preflight != RF_SIDE_XACT_APPLY_OK)
		return preflight;

	mutation = cluster_remote_xact_store_prepared_v2(operation->origin_thread - 1, operation->xid,
													 operation->prepare_binding);
	if (mutation == CLUSTER_REMOTE_XACT_MUTATION_CONFLICT)
		return RF_SIDE_XACT_APPLY_CONFLICT;
	if (mutation != CLUSTER_REMOTE_XACT_MUTATION_STORED
		&& mutation != CLUSTER_REMOTE_XACT_MUTATION_UNCHANGED)
		return RF_SIDE_XACT_APPLY_BLOCKED;
	if (!cluster_remote_xact_pending_matches_v2(operation->origin_thread - 1, operation->xid,
												operation->prepare_binding))
		return RF_SIDE_XACT_APPLY_POST_READ_FAILED;
	return RF_SIDE_XACT_APPLY_OK;
}

RfSideXactApplyResultV1
rf_side_xact_apply_v1(const RfSideXactOperationV1 *operation)
{
	return rf_side_xact_apply_owned_v1(operation, NULL, 0);
}

#endif
