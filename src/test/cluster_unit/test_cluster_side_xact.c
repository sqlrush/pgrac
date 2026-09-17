/*-------------------------------------------------------------------------
 * test_cluster_side_xact.c
 *    RF-SIDE immutable XACT decode tests.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/clog.h"
#include "access/commit_ts.h"
#include "access/multixact.h"
#include "access/rmgr.h"
#include "access/twophase.h"
#include "access/twophase_rmgr.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "cluster/cluster_side_xact.h"
#include "cluster/cluster_side_online_plan.h"
#include "cluster/cluster_side_undo.h"
#include "cluster/cluster_tt_durable.h"
#include "cluster/cluster_tt_2pc.h"
#include "cluster/cluster_xid_stripe.h"
#include "cluster/storage/cluster_undo_xlog.h"

#include "unit_test.h"

UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

#include <stdio.h>

typedef struct PrepareApplyCapture {
	uint64 system_identifier;
	int origin_slot;
	TransactionId mismatched_origin_xid;
	TTSlot slot;
	TwoPhaseRecoveryPendingResult pending_preflight;
	TwoPhaseRecoveryPendingResult pending_install;
	TwoPhaseRecoveryPendingResult pending_read;
	TwoPhaseRecoveryPendingResult pending_resolve;
	ClusterRemoteXactMutationV2 projection_store;
	ClusterRemoteXactOutcome terminal_outcome;
	bool projection_postread;
	bool resolved_is_commit;
	uint8 native_payload[512];
	uint32 native_payload_length;
	uint32 tt_reads;
	uint32 pending_preflights;
	uint32 pending_installs;
	uint32 pending_reads;
	uint32 pending_resolves;
	uint32 projection_stores;
	uint32 terminal_projection_stores;
	uint32 projection_postreads;
	uint32 next_order;
	uint32 install_order;
	uint32 projection_order;
	uint32 terminal_projection_order;
	uint32 resolve_order;
} PrepareApplyCapture;

static PrepareApplyCapture prepare_apply;

static void
reset_prepare_apply(void)
{
	memset(&prepare_apply, 0, sizeof(prepare_apply));
	prepare_apply.system_identifier = UINT64_C(0x11223344);
	prepare_apply.origin_slot = 2;
	prepare_apply.slot.xid = 802;
	prepare_apply.slot.wrap = 7;
	prepare_apply.slot.status = TT_SLOT_ACTIVE;
	prepare_apply.slot.commit_scn = InvalidScn;
	prepare_apply.pending_preflight = TWOPHASE_RECOVERY_PENDING_OK;
	prepare_apply.pending_install = TWOPHASE_RECOVERY_PENDING_OK;
	prepare_apply.pending_read = TWOPHASE_RECOVERY_PENDING_OK;
	prepare_apply.pending_resolve = TWOPHASE_RECOVERY_PENDING_OK;
	prepare_apply.projection_store = CLUSTER_REMOTE_XACT_MUTATION_STORED;
	prepare_apply.terminal_outcome = CLUSTER_REMOTE_XACT_COMMITTED;
	prepare_apply.projection_postread = true;
}

uint64
GetSystemIdentifier(void)
{
	return prepare_apply.system_identifier;
}

int
cluster_xid_origin_slot(TransactionId xid pg_attribute_unused())
{
	if (TransactionIdIsValid(prepare_apply.mismatched_origin_xid)
		&& TransactionIdEquals(xid, prepare_apply.mismatched_origin_xid))
		return prepare_apply.origin_slot + 1;
	return prepare_apply.origin_slot;
}

bool
cluster_tt_slot_durable_read_exact_stable(uint32 segment_id pg_attribute_unused(),
										  uint16 slot_offset pg_attribute_unused(),
										  TransactionId xid pg_attribute_unused(),
										  uint16 expected_wrap pg_attribute_unused(),
										  TTSlot *slot_out)
{
	prepare_apply.tt_reads++;
	*slot_out = prepare_apply.slot;
	return true;
}

TwoPhaseRecoveryPendingResult
TwoPhaseRecoveryPendingPreflight(TransactionId xid pg_attribute_unused(),
								 Oid database pg_attribute_unused(),
								 Oid owner pg_attribute_unused(),
								 TimestampTz prepared_at pg_attribute_unused(),
								 const char *gid pg_attribute_unused(),
								 const void *content pg_attribute_unused(),
								 uint32 len pg_attribute_unused())
{
	prepare_apply.pending_preflights++;
	return prepare_apply.pending_preflight;
}

TwoPhaseRecoveryPendingResult
TwoPhaseRecoveryPendingInstall(TransactionId xid pg_attribute_unused(),
							   Oid database pg_attribute_unused(), Oid owner pg_attribute_unused(),
							   TimestampTz prepared_at pg_attribute_unused(),
							   const char *gid pg_attribute_unused(),
							   const void *content pg_attribute_unused(),
							   uint32 len pg_attribute_unused())
{
	prepare_apply.pending_installs++;
	prepare_apply.install_order = ++prepare_apply.next_order;
	return prepare_apply.pending_install;
}

TwoPhaseRecoveryPendingResult
TwoPhaseRecoveryPendingReadExact(TransactionId xid pg_attribute_unused(),
								 Oid database pg_attribute_unused(),
								 const char *gid pg_attribute_unused(), void **content_out,
								 uint32 *len_out)
{
	prepare_apply.pending_reads++;
	if (prepare_apply.pending_read != TWOPHASE_RECOVERY_PENDING_OK)
		return prepare_apply.pending_read;
	if (content_out == NULL || len_out == NULL || prepare_apply.native_payload_length == 0)
		return TWOPHASE_RECOVERY_PENDING_BLOCKED;
	*content_out = malloc(prepare_apply.native_payload_length);
	memcpy(*content_out, prepare_apply.native_payload, prepare_apply.native_payload_length);
	*len_out = prepare_apply.native_payload_length;
	return TWOPHASE_RECOVERY_PENDING_OK;
}

TwoPhaseRecoveryPendingResult
TwoPhaseRecoveryPendingResolveExact(TransactionId xid pg_attribute_unused(),
									Oid database pg_attribute_unused(),
									const char *gid pg_attribute_unused(), const void *content,
									uint32 len, bool isCommit)
{
	prepare_apply.pending_resolves++;
	prepare_apply.resolved_is_commit = isCommit;
	prepare_apply.resolve_order = ++prepare_apply.next_order;
	if (content == NULL || len != prepare_apply.native_payload_length
		|| memcmp(content, prepare_apply.native_payload, len) != 0)
		return TWOPHASE_RECOVERY_PENDING_CONFLICT;
	return prepare_apply.pending_resolve;
}

ClusterRemoteXactMutationV2
cluster_remote_xact_store_prepared_v2(
	int origin_node pg_attribute_unused(), TransactionId xid pg_attribute_unused(),
	const uint8 digest[CLUSTER_REMOTE_XACT_PREPARE_DIGEST_BYTES] pg_attribute_unused())
{
	prepare_apply.projection_stores++;
	prepare_apply.projection_order = ++prepare_apply.next_order;
	return prepare_apply.projection_store;
}

bool
cluster_remote_xact_pending_matches_v2(
	int origin_node pg_attribute_unused(), TransactionId xid pg_attribute_unused(),
	const uint8 digest[CLUSTER_REMOTE_XACT_PREPARE_DIGEST_BYTES] pg_attribute_unused())
{
	prepare_apply.projection_postreads++;
	return prepare_apply.projection_postread;
}

void
cluster_tt_durable_redo_stamp_slot(uint8 instance pg_attribute_unused(),
								   uint32 segment_id pg_attribute_unused(),
								   uint16 slot_offset pg_attribute_unused(),
								   uint16 wrap pg_attribute_unused(),
								   TransactionId xid pg_attribute_unused(),
								   SCN commit_scn pg_attribute_unused())
{}

void
cluster_tt_durable_redo_stamp_slot_exact(uint8 instance pg_attribute_unused(),
										 uint32 segment_id pg_attribute_unused(),
										 uint32 segment_generation pg_attribute_unused(),
										 uint16 slot_offset pg_attribute_unused(),
										 uint16 wrap pg_attribute_unused(),
										 TransactionId xid pg_attribute_unused(),
										 SCN commit_scn pg_attribute_unused())
{}

ClusterTTDurableResolve
cluster_tt_slot_durable_resolve_by_xid_origin(int origin_node pg_attribute_unused(),
											  TransactionId xid pg_attribute_unused(),
											  uint32 expected_wrap pg_attribute_unused(),
											  SCN *commit_scn, uint16 *out_seg, uint16 *out_slot,
											  uint16 *out_wrap)
{
	*commit_scn = UINT64_C(901);
	*out_seg = 9;
	*out_slot = 4;
	*out_wrap = 7;
	return CLUSTER_TT_DURABLE_RESOLVED_SCN;
}

void
cluster_scn_recovery_replay_observe(SCN scn pg_attribute_unused())
{}

ClusterRemoteXactMutationV2
cluster_remote_xact_store_terminal_v2(
	int origin_node pg_attribute_unused(), TransactionId xid pg_attribute_unused(),
	bool require_prepared pg_attribute_unused(),
	const uint8
		expected_prepare_digest[CLUSTER_REMOTE_XACT_PREPARE_DIGEST_BYTES] pg_attribute_unused(),
	ClusterRemoteXactOutcome outcome, SCN commit_scn pg_attribute_unused(),
	TimestampTz commit_timestamp pg_attribute_unused(), bool wrap_valid pg_attribute_unused(),
	uint16 wrap pg_attribute_unused())
{
	prepare_apply.terminal_projection_stores++;
	prepare_apply.terminal_outcome = outcome;
	prepare_apply.terminal_projection_order = ++prepare_apply.next_order;
	return CLUSTER_REMOTE_XACT_MUTATION_STORED;
}

ClusterRemoteXactOutcome
cluster_remote_commit_outcome_ex(int origin_node pg_attribute_unused(),
								 TransactionId xid pg_attribute_unused(), SCN *commit_scn,
								 uint16 *out_wrap, bool *out_wrap_valid)
{
	if (prepare_apply.terminal_outcome == CLUSTER_REMOTE_XACT_COMMITTED) {
		*commit_scn = UINT64_C(901);
		*out_wrap = 7;
		*out_wrap_valid = true;
	} else {
		*commit_scn = InvalidScn;
		*out_wrap = 0;
		*out_wrap_valid = false;
	}
	return prepare_apply.terminal_outcome;
}

bool
cluster_remote_commit_timestamp(int origin_node pg_attribute_unused(),
								TransactionId xid pg_attribute_unused(),
								TimestampTz *commit_timestamp)
{
	if (prepare_apply.terminal_outcome != CLUSTER_REMOTE_XACT_COMMITTED)
		return false;
	*commit_timestamp = INT64_C(123456);
	return true;
}

typedef struct FakeXactRecord {
	XLogReaderState reader;
	uint8 data[512];
	union {
		DecodedXLogRecord decoded;
		char pad[sizeof(DecodedXLogRecord) + 2 * sizeof(DecodedBkpBlock)];
	} u;
} FakeXactRecord;

static XLogReaderState *
make_commit(FakeXactRecord *fake, TransactionId xid, SCN scn, TimestampTz timestamp,
			bool conflicting_tt)
{
	xl_xact_commit commit;
	xl_xact_xinfo xinfo;
	xl_xact_scn wal_scn;
	xl_xact_tt_commit tt;
	uint8 *cursor;

	memset(fake, 0, sizeof(*fake));
	memset(&commit, 0, sizeof(commit));
	memset(&xinfo, 0, sizeof(xinfo));
	memset(&wal_scn, 0, sizeof(wal_scn));
	memset(&tt, 0, sizeof(tt));
	commit.xact_time = timestamp;
	xinfo.xinfo = XACT_XINFO_HAS_SCN | XACT_XINFO_HAS_TT_COMMIT;
	wal_scn.scn = scn;
	tt.instance = 3;
	tt.segment_id = 9;
	tt.segment_generation = 11;
	tt.slot_offset = 4;
	tt.wrap = 7;
	tt.xid = conflicting_tt ? xid + 1 : xid;
	tt.format_version = CLUSTER_XACT_TT_COMMIT_VERSION;
	tt.commit_scn = scn;
	cursor = fake->data;
	memcpy(cursor, &commit, sizeof(commit));
	cursor += sizeof(commit);
	memcpy(cursor, &xinfo, sizeof(xinfo));
	cursor += sizeof(xinfo);
	memcpy(cursor, &wal_scn, sizeof(wal_scn));
	cursor += sizeof(wal_scn);
	memcpy(cursor, &tt, sizeof(tt));
	cursor += sizeof(tt);
	fake->u.decoded.header.xl_rmid = RM_XACT_ID;
	fake->u.decoded.header.xl_info = XLOG_XACT_COMMIT | XLOG_XACT_HAS_INFO;
	fake->u.decoded.header.xl_xid = xid;
	fake->u.decoded.main_data = (char *)fake->data;
	fake->u.decoded.main_data_len = (uint32)(cursor - fake->data);
	fake->reader.record = &fake->u.decoded;
	return &fake->reader;
}

static XLogReaderState *
make_prepare_with_head(FakeXactRecord *fake, TransactionId xid, bool with_tt, bool truncated_gid,
					   const UBA *head)
{
	typedef struct FakeTwoPhaseRecordOnDisk {
		uint32 len;
		TwoPhaseRmgrId rmid;
		uint16 info;
	} FakeTwoPhaseRecordOnDisk;
	xl_xact_prepare prepare;
	ClusterTT2PCBinding binding;
	FakeTwoPhaseRecordOnDisk disk_record;
	Size header_size = MAXALIGN(sizeof(prepare));
	Size payload_size;
	uint32 tt_size;
	char *cursor;

	memset(fake, 0, sizeof(*fake));
	memset(&prepare, 0, sizeof(prepare));
	memset(&binding, 0, sizeof(binding));
	memset(&disk_record, 0, sizeof(disk_record));
	prepare.magic = UINT32_C(0x57F94534);
	prepare.xid = xid;
	prepare.database = 16384;
	prepare.owner = 10;
	prepare.prepared_at = INT64_C(123456);
	prepare.gidlen = 4;
	cursor = (char *)fake->data + header_size;
	memcpy(cursor, "gid", 4);
	cursor += MAXALIGN(4);
	if (with_tt) {
		binding.undo_segment_id = 513;
		binding.slot_offset = 4;
		binding.wrap = 7;
		binding.cluster_epoch = 11;
		binding.xid = xid;
		tt_size = cluster_tt_2pc_record_size(CLUSTER_TT_2PC_VERSION, 1, 0);
		disk_record.len = tt_size;
		disk_record.rmid = TWOPHASE_RM_CLUSTER_TT_ID;
		memcpy(cursor, &disk_record, sizeof(disk_record));
		cursor += MAXALIGN(sizeof(disk_record));
		UT_ASSERT_EQ(cluster_tt_2pc_serialize(&binding, head, 1, NULL, 0, cursor, tt_size),
					 tt_size);
		cursor += MAXALIGN(tt_size);
	}
	memset(&disk_record, 0, sizeof(disk_record));
	disk_record.rmid = TWOPHASE_RM_END_ID;
	memcpy(cursor, &disk_record, sizeof(disk_record));
	cursor += MAXALIGN(sizeof(disk_record));
	payload_size = (Size)(cursor - (char *)fake->data);
	prepare.total_len = (uint32)payload_size + sizeof(uint32);
	memcpy(fake->data, &prepare, sizeof(prepare));
	fake->u.decoded.header.xl_rmid = RM_XACT_ID;
	fake->u.decoded.header.xl_info = XLOG_XACT_PREPARE;
	fake->u.decoded.main_data = (char *)fake->data;
	fake->u.decoded.main_data_len = truncated_gid ? (uint32)header_size : (uint32)payload_size;
	fake->reader.record = &fake->u.decoded;
	return &fake->reader;
}

static XLogReaderState *
make_prepare(FakeXactRecord *fake, TransactionId xid, bool with_tt, bool truncated_gid)
{
	return make_prepare_with_head(fake, xid, with_tt, truncated_gid, NULL);
}

static XLogReaderState *
make_commit_prepared(FakeXactRecord *fake, TransactionId xid, Oid database, SCN scn,
					 TimestampTz timestamp)
{
	xl_xact_commit commit;
	xl_xact_xinfo xinfo;
	xl_xact_dbinfo dbinfo;
	xl_xact_twophase twophase;
	xl_xact_scn wal_scn;
	char *cursor;

	memset(fake, 0, sizeof(*fake));
	memset(&commit, 0, sizeof(commit));
	memset(&xinfo, 0, sizeof(xinfo));
	memset(&dbinfo, 0, sizeof(dbinfo));
	memset(&twophase, 0, sizeof(twophase));
	memset(&wal_scn, 0, sizeof(wal_scn));
	commit.xact_time = timestamp;
	xinfo.xinfo
		= XACT_XINFO_HAS_DBINFO | XACT_XINFO_HAS_TWOPHASE | XACT_XINFO_HAS_GID | XACT_XINFO_HAS_SCN;
	dbinfo.dbId = database;
	twophase.xid = xid;
	wal_scn.scn = scn;
	cursor = (char *)fake->data;
	memcpy(cursor, &commit, sizeof(commit));
	cursor += sizeof(commit);
	memcpy(cursor, &xinfo, sizeof(xinfo));
	cursor += sizeof(xinfo);
	memcpy(cursor, &dbinfo, sizeof(dbinfo));
	cursor += sizeof(dbinfo);
	memcpy(cursor, &twophase, sizeof(twophase));
	cursor += sizeof(twophase);
	memcpy(cursor, "gid", 4);
	cursor += 4;
	memcpy(cursor, &wal_scn, sizeof(wal_scn));
	cursor += sizeof(wal_scn);
	fake->u.decoded.header.xl_rmid = RM_XACT_ID;
	fake->u.decoded.header.xl_info = XLOG_XACT_COMMIT_PREPARED | XLOG_XACT_HAS_INFO;
	fake->u.decoded.main_data = (char *)fake->data;
	fake->u.decoded.main_data_len = (uint32)(cursor - (char *)fake->data);
	fake->reader.record = &fake->u.decoded;
	return &fake->reader;
}

static XLogReaderState *
make_abort_prepared(FakeXactRecord *fake, TransactionId xid, Oid database, SCN scn)
{
	xl_xact_abort abort;
	xl_xact_xinfo xinfo;
	xl_xact_dbinfo dbinfo;
	xl_xact_twophase twophase;
	xl_xact_scn wal_scn;
	char *cursor;

	memset(fake, 0, sizeof(*fake));
	memset(&abort, 0, sizeof(abort));
	memset(&xinfo, 0, sizeof(xinfo));
	memset(&dbinfo, 0, sizeof(dbinfo));
	memset(&twophase, 0, sizeof(twophase));
	memset(&wal_scn, 0, sizeof(wal_scn));
	abort.xact_time = INT64_C(123456);
	xinfo.xinfo
		= XACT_XINFO_HAS_DBINFO | XACT_XINFO_HAS_TWOPHASE | XACT_XINFO_HAS_GID | XACT_XINFO_HAS_SCN;
	dbinfo.dbId = database;
	twophase.xid = xid;
	wal_scn.scn = scn;
	cursor = (char *)fake->data;
	memcpy(cursor, &abort, sizeof(abort));
	cursor += sizeof(abort);
	memcpy(cursor, &xinfo, sizeof(xinfo));
	cursor += sizeof(xinfo);
	memcpy(cursor, &dbinfo, sizeof(dbinfo));
	cursor += sizeof(dbinfo);
	memcpy(cursor, &twophase, sizeof(twophase));
	cursor += sizeof(twophase);
	memcpy(cursor, "gid", 4);
	cursor += 4;
	memcpy(cursor, &wal_scn, sizeof(wal_scn));
	cursor += sizeof(wal_scn);
	fake->u.decoded.header.xl_rmid = RM_XACT_ID;
	fake->u.decoded.header.xl_info = XLOG_XACT_ABORT_PREPARED | XLOG_XACT_HAS_INFO;
	fake->u.decoded.main_data = (char *)fake->data;
	fake->u.decoded.main_data_len = (uint32)(cursor - (char *)fake->data);
	fake->reader.record = &fake->u.decoded;
	return &fake->reader;
}

static RfPageOnlineRecordIdentityV1
make_identity(FakeXactRecord *fake, uint8 storage_uuid[16])
{
	RfPageOnlineRecordIdentityV1 identity;

	memset(&identity, 0, sizeof(identity));
	identity.record.system_identifier = UINT64_C(0x11223344);
	memcpy(identity.record.storage_uuid, storage_uuid, 16);
	identity.record.origin_thread = 3;
	identity.record.timeline_id = 7;
	identity.record.read_rec_ptr = UINT64_C(100);
	identity.record.end_rec_ptr = UINT64_C(200);
	identity.record.record_crc = UINT32_C(0xabc123);
	identity.record.rmid = fake->u.decoded.header.xl_rmid;
	identity.record.info = fake->u.decoded.header.xl_info;
	fake->reader.system_identifier = identity.record.system_identifier;
	fake->reader.ReadRecPtr = identity.record.read_rec_ptr;
	fake->reader.EndRecPtr = identity.record.end_rec_ptr;
	fake->u.decoded.lsn = identity.record.read_rec_ptr;
	fake->u.decoded.next_lsn = identity.record.end_rec_ptr;
	fake->u.decoded.header.xl_crc = identity.record.record_crc;
	return identity;
}

static void
set_identity_range(FakeXactRecord *fake, RfPageOnlineRecordIdentityV1 *identity, XLogRecPtr begin,
				   XLogRecPtr end)
{
	identity->record.read_rec_ptr = begin;
	identity->record.end_rec_ptr = end;
	fake->reader.ReadRecPtr = begin;
	fake->reader.EndRecPtr = end;
	fake->u.decoded.lsn = begin;
	fake->u.decoded.next_lsn = end;
}

static XLogReaderState *
make_undo_delta(FakeXactRecord *fake)
{
	xl_undo_block_write undo;
	uint32 body_len = UNDO_BLOCK_HDR_PREFIX_LEN + 24 + sizeof(UndoSlotDirEntry);

	memset(fake, 0, sizeof(*fake));
	memset(&undo, 0, sizeof(undo));
	undo.instance = 3;
	undo.segment_id = 513;
	undo.block_no = 9;
	undo.rec_off = sizeof(UndoBlockHeader);
	undo.rec_len = 24;
	undo.slot_off = BLCKSZ - sizeof(UndoSlotDirEntry);
	memcpy(fake->data, &undo, sizeof(undo));
	memset(fake->data + sizeof(undo), 0x6b, body_len);
	fake->u.decoded.header.xl_rmid = RM_CLUSTER_UNDO_ID;
	fake->u.decoded.header.xl_info = XLOG_UNDO_BLOCK_WRITE;
	fake->u.decoded.main_data = (char *)fake->data;
	fake->u.decoded.main_data_len = sizeof(undo) + body_len;
	fake->reader.record = &fake->u.decoded;
	return &fake->reader;
}

static XLogReaderState *
make_tt_commit_delta(FakeXactRecord *fake, TransactionId xid, SCN commit_scn)
{
	xl_undo_tt_slot_commit commit;

	memset(fake, 0, sizeof(*fake));
	memset(&commit, 0, sizeof(commit));
	commit.instance = 3;
	commit.segment_id = 513;
	commit.slot_offset = 4;
	commit.wrap = 7;
	commit.xid = xid;
	commit.commit_scn = commit_scn;
	memcpy(fake->data, &commit, sizeof(commit));
	fake->u.decoded.header.xl_rmid = RM_CLUSTER_UNDO_ID;
	fake->u.decoded.header.xl_info = XLOG_UNDO_TT_SLOT_COMMIT;
	fake->u.decoded.main_data = (char *)fake->data;
	fake->u.decoded.main_data_len = sizeof(commit);
	fake->reader.record = &fake->u.decoded;
	return &fake->reader;
}

static XLogReaderState *
make_tt_abort_delta(FakeXactRecord *fake, TransactionId xid)
{
	xl_undo_tt_slot_abort abort;

	memset(fake, 0, sizeof(*fake));
	memset(&abort, 0, sizeof(abort));
	abort.instance = 3;
	abort.segment_id = 513;
	abort.slot_offset = 4;
	abort.wrap = 7;
	abort.xid = xid;
	memcpy(fake->data, &abort, sizeof(abort));
	fake->u.decoded.header.xl_rmid = RM_CLUSTER_UNDO_ID;
	fake->u.decoded.header.xl_info = XLOG_UNDO_TT_SLOT_ABORT;
	fake->u.decoded.main_data = (char *)fake->data;
	fake->u.decoded.main_data_len = sizeof(abort);
	fake->reader.record = &fake->u.decoded;
	return &fake->reader;
}

static XLogReaderState *
make_tt_set_head_delta(FakeXactRecord *fake, TransactionId xid, const UBA *head)
{
	xl_undo_tt_slot_set_head set_head;

	memset(fake, 0, sizeof(*fake));
	memset(&set_head, 0, sizeof(set_head));
	set_head.instance = 3;
	set_head.segment_id = 513;
	set_head.slot_offset = 4;
	set_head.wrap = 7;
	set_head.xid = xid;
	set_head.first_undo_block = *head;
	memcpy(fake->data, &set_head, sizeof(set_head));
	fake->u.decoded.header.xl_rmid = RM_CLUSTER_UNDO_ID;
	fake->u.decoded.header.xl_info = XLOG_UNDO_TT_SLOT_SET_HEAD;
	fake->u.decoded.main_data = (char *)fake->data;
	fake->u.decoded.main_data_len = sizeof(set_head);
	fake->reader.record = &fake->u.decoded;
	return &fake->reader;
}

static RfDetachedRecordPlanV1
make_undo_record_plan(FakeXactRecord *fake)
{
	RfDetachedRecordPlanV1 record_plan;

	memset(&record_plan, 0, sizeof(record_plan));
	record_plan.source_record = &fake->reader;
	record_plan.route.rmid = RM_CLUSTER_UNDO_ID;
	record_plan.route.normalized_info = fake->u.decoded.header.xl_info & XLR_RMGR_INFO_MASK;
	record_plan.route.record_owner = RF_ROUTE_OWNER_SIDE_TYPED;
	record_plan.route.block_policy = RF_ROUTE_BLOCKS_FORBIDDEN;
	record_plan.route.codec_id = RF_ROUTE_CODEC_SIDE_CLUSTER_UNDO;
	record_plan.preflight_complete = true;
	return record_plan;
}

static RfDetachedRecordPlanV1
make_record_plan(FakeXactRecord *fake)
{
	RfDetachedRecordPlanV1 record_plan;

	memset(&record_plan, 0, sizeof(record_plan));
	record_plan.source_record = &fake->reader;
	record_plan.route.rmid = RM_XACT_ID;
	record_plan.route.normalized_info = fake->u.decoded.header.xl_info & XLOG_XACT_OPMASK;
	record_plan.route.legal_info_flags = XLOG_XACT_HAS_INFO;
	record_plan.route.record_owner = RF_ROUTE_OWNER_SIDE_TYPED;
	record_plan.route.block_policy = RF_ROUTE_BLOCKS_FORBIDDEN;
	record_plan.route.codec_id = RF_ROUTE_CODEC_SIDE_STANDARD;
	record_plan.preflight_complete = true;
	return record_plan;
}

static XLogReaderState *
make_projection_record(FakeXactRecord *fake, RmgrId rmid, uint8 info, const void *payload,
					   uint32 payload_length)
{
	memset(fake, 0, sizeof(*fake));
	UT_ASSERT(payload_length <= sizeof(fake->data));
	memcpy(fake->data, payload, payload_length);
	fake->u.decoded.header.xl_rmid = rmid;
	fake->u.decoded.header.xl_info = info;
	fake->u.decoded.main_data = (char *)fake->data;
	fake->u.decoded.main_data_len = payload_length;
	fake->reader.record = &fake->u.decoded;
	return &fake->reader;
}

static RfDetachedRecordPlanV1
make_projection_record_plan(FakeXactRecord *fake)
{
	RfDetachedRecordPlanV1 record_plan;

	memset(&record_plan, 0, sizeof(record_plan));
	record_plan.source_record = &fake->reader;
	record_plan.route.rmid = fake->u.decoded.header.xl_rmid;
	record_plan.route.normalized_info = fake->u.decoded.header.xl_info & ~XLR_INFO_MASK;
	record_plan.route.record_owner = RF_ROUTE_OWNER_SIDE_TYPED;
	record_plan.route.block_policy = RF_ROUTE_BLOCKS_FORBIDDEN;
	record_plan.route.codec_id = RF_ROUTE_CODEC_SIDE_STANDARD;
	record_plan.preflight_complete = true;
	return record_plan;
}

typedef struct ApplyCapture {
	uint32 count;
	uint32 undo_count;
	uint32 projection_count;
	uint32 begin_count;
	uint32 end_count;
	TransactionId xid;
	SCN scn;
	uint8 undo_first_byte;
	uint8 projection_first_byte;
	bool end_complete;
} ApplyCapture;

static bool
capture_begin(void *arg)
{
	ApplyCapture *capture = (ApplyCapture *)arg;

	capture->begin_count++;
	return true;
}

static void
capture_end(void *arg, bool complete)
{
	ApplyCapture *capture = (ApplyCapture *)arg;

	capture->end_count++;
	capture->end_complete = complete;
}

static bool
capture_apply(void *arg, const RfSideOnlineOperationV1 *operation)
{
	ApplyCapture *capture = (ApplyCapture *)arg;

	capture->count++;
	capture->xid = operation->xact.xid;
	capture->scn = operation->xact.terminal_scn;
	return true;
}

static bool
capture_apply_undo(void *arg, const RfSideOnlineOperationV1 *operation)
{
	ApplyCapture *capture = (ApplyCapture *)arg;

	capture->undo_count++;
	if (operation->owned_payload_length > 0)
		capture->undo_first_byte = operation->owned_payload[0];
	return operation->kind == RF_SIDE_ONLINE_OPERATION_UNDO;
}

static bool
capture_apply_projection(void *arg, const RfSideOnlineOperationV1 *operation)
{
	ApplyCapture *capture = (ApplyCapture *)arg;

	capture->projection_count++;
	if (operation->owned_payload_length > 0)
		capture->projection_first_byte = operation->owned_payload[0];
	return operation->kind == RF_SIDE_ONLINE_OPERATION_PROJECTION;
}

static bool
accept_preflight(void *arg pg_attribute_unused(),
				 const RfSideOnlineOperationV1 *operation pg_attribute_unused())
{
	return true;
}

static bool
reject_undo_preflight(void *arg pg_attribute_unused(), const RfSideOnlineOperationV1 *operation)
{
	return operation->kind != RF_SIDE_ONLINE_OPERATION_UNDO;
}

UT_TEST(test_commit_decodes_to_immutable_truth_operation)
{
	FakeXactRecord fake;
	RfSideXactOperationV1 operation;

	UT_ASSERT(rf_side_xact_decode_v1(make_commit(&fake, 800, UINT64_C(901), INT64_C(123456), false),
									 UINT64_C(0x11223344), 3, &operation));
	UT_ASSERT_EQ(operation.kind, RF_SIDE_XACT_COMMIT);
	UT_ASSERT_EQ(operation.system_identifier, UINT64_C(0x11223344));
	UT_ASSERT_EQ(operation.origin_thread, 3);
	UT_ASSERT_EQ(operation.xid, 800);
	UT_ASSERT_EQ(operation.terminal_scn, UINT64_C(901));
	UT_ASSERT_EQ(operation.terminal_timestamp, INT64_C(123456));
	UT_ASSERT(operation.has_tt_delta);
	UT_ASSERT_EQ(operation.tt_delta.xid, 800);
	UT_ASSERT_EQ(operation.tt_delta.segment_generation, 11);
	UT_ASSERT_EQ(operation.tt_delta.commit_scn, UINT64_C(901));
	UT_ASSERT(rf_side_xact_structural_preflight_v1(&operation));
}

UT_TEST(test_commit_tt_conflict_is_blocked_before_apply)
{
	FakeXactRecord fake;
	RfSideXactOperationV1 operation;

	UT_ASSERT(!rf_side_xact_decode_v1(make_commit(&fake, 800, UINT64_C(901), INT64_C(123456), true),
									  UINT64_C(0x11223344), 3, &operation));
	UT_ASSERT_EQ(operation.kind, RF_SIDE_XACT_INVALID);
}

UT_TEST(test_non_xact_and_missing_tt_are_blocked)
{
	FakeXactRecord fake;
	RfSideXactOperationV1 operation;
	XLogReaderState *record = make_commit(&fake, 800, UINT64_C(901), INT64_C(123456), false);

	fake.u.decoded.header.xl_rmid = RM_HEAP_ID;
	UT_ASSERT(!rf_side_xact_decode_v1(record, UINT64_C(0x11223344), 3, &operation));
	record = make_commit(&fake, 800, UINT64_C(901), INT64_C(123456), false);
	fake.u.decoded.main_data_len -= sizeof(xl_xact_tt_commit);
	UT_ASSERT(!rf_side_xact_decode_v1(record, UINT64_C(0x11223344), 3, &operation));
	UT_ASSERT(!rf_side_xact_structural_preflight_v1(NULL));
}

UT_TEST(test_prepare_requires_bounded_aligned_gid)
{
	FakeXactRecord fake;
	RfSideXactOperationV1 operation;

	UT_ASSERT(rf_side_xact_decode_v1(make_prepare(&fake, 801, true, false), UINT64_C(0x11223344), 3,
									 &operation));
	UT_ASSERT_EQ(operation.kind, RF_SIDE_XACT_PREPARE);
	UT_ASSERT_EQ(operation.xid, 801);
	UT_ASSERT_EQ(operation.database, 16384);
	UT_ASSERT_EQ(operation.prepared_owner, 10);
	UT_ASSERT_EQ(operation.prepared_at, INT64_C(123456));
	UT_ASSERT(strcmp(operation.prepare_gid, "gid") == 0);
	UT_ASSERT(operation.prepare_payload_length > 0);
	UT_ASSERT_EQ(operation.prepared_binding_count, 1);
	UT_ASSERT_EQ(operation.prepared_bindings[0].undo_segment_id, 513);
	UT_ASSERT_EQ(operation.prepared_bindings[0].slot_offset, 4);
	UT_ASSERT_EQ(operation.prepared_bindings[0].wrap, 7);
	UT_ASSERT_EQ(operation.prepared_bindings[0].xid, 801);
	UT_ASSERT(!rf_side_xact_decode_v1(make_prepare(&fake, 801, false, false), UINT64_C(0x11223344),
									  3, &operation));
	UT_ASSERT(!rf_side_xact_decode_v1(make_prepare(&fake, 801, true, true), UINT64_C(0x11223344), 3,
									  &operation));
	UT_ASSERT_EQ(operation.kind, RF_SIDE_XACT_INVALID);
}

UT_TEST(test_prepare_apply_installs_authoritative_pending_before_projection)
{
	FakeXactRecord fake;
	RfSideXactOperationV1 operation;
	XLogReaderState *record;

	reset_prepare_apply();
	record = make_prepare(&fake, 802, true, false);
	UT_ASSERT(rf_side_xact_decode_v1(record, UINT64_C(0x11223344), 3, &operation));
	UT_ASSERT_EQ(rf_side_xact_target_preflight_owned_v1(&operation, fake.data,
														operation.prepare_payload_length),
				 RF_SIDE_XACT_APPLY_OK);
	UT_ASSERT_EQ(prepare_apply.tt_reads, 1);
	UT_ASSERT_EQ(prepare_apply.pending_preflights, 1);
	UT_ASSERT_EQ(prepare_apply.pending_installs, 0);
	UT_ASSERT_EQ(prepare_apply.projection_stores, 0);

	UT_ASSERT_EQ(
		rf_side_xact_apply_owned_v1(&operation, fake.data, operation.prepare_payload_length),
		RF_SIDE_XACT_APPLY_OK);
	UT_ASSERT_EQ(prepare_apply.pending_installs, 1);
	UT_ASSERT_EQ(prepare_apply.projection_stores, 1);
	UT_ASSERT_EQ(prepare_apply.projection_postreads, 1);
	UT_ASSERT(prepare_apply.install_order < prepare_apply.projection_order);
}

UT_TEST(test_prepare_apply_blocks_underivable_or_wrong_system_before_pending)
{
	FakeXactRecord fake;
	RfSideXactOperationV1 operation;

	reset_prepare_apply();
	UT_ASSERT(rf_side_xact_decode_v1(make_prepare(&fake, 802, true, false), UINT64_C(0x11223344), 3,
									 &operation));
	prepare_apply.origin_slot = -1;
	UT_ASSERT_EQ(rf_side_xact_target_preflight_owned_v1(&operation, fake.data,
														operation.prepare_payload_length),
				 RF_SIDE_XACT_APPLY_BLOCKED);
	UT_ASSERT_EQ(prepare_apply.tt_reads, 0);
	UT_ASSERT_EQ(prepare_apply.pending_preflights, 0);

	prepare_apply.origin_slot = 2;
	prepare_apply.system_identifier++;
	UT_ASSERT_EQ(
		rf_side_xact_apply_owned_v1(&operation, fake.data, operation.prepare_payload_length),
		RF_SIDE_XACT_APPLY_BLOCKED);
	UT_ASSERT_EQ(prepare_apply.pending_installs, 0);
	UT_ASSERT_EQ(prepare_apply.projection_stores, 0);
}

UT_TEST(test_prepare_apply_blocks_wrong_origin_subxid_before_target_reads)
{
	FakeXactRecord fake;
	RfSideXactOperationV1 operation;
	ClusterTT2PCBinding *child;
	ClusterTT2PCSubLink *link;

	reset_prepare_apply();
	UT_ASSERT(rf_side_xact_decode_v1(make_prepare(&fake, 802, true, false), UINT64_C(0x11223344), 3,
									 &operation));
	child = &operation.prepared_bindings[1];
	memset(child, 0, sizeof(*child));
	child->undo_segment_id = 514;
	child->slot_offset = 5;
	child->wrap = 7;
	child->cluster_epoch = 11;
	child->xid = 803;
	operation.prepared_binding_count = 2;
	link = &operation.prepared_sublinks[0];
	memset(link, 0, sizeof(*link));
	link->child_key.origin_node_id = 2;
	link->child_key.undo_segment_id = 514;
	link->child_key.tt_slot_id = cluster_tt_slot_offset_to_id(5);
	link->child_key.cluster_epoch = 11;
	link->child_key.local_xid = 803;
	link->parent_key.origin_node_id = 2;
	link->parent_key.undo_segment_id = 513;
	link->parent_key.tt_slot_id = cluster_tt_slot_offset_to_id(4);
	link->parent_key.cluster_epoch = 11;
	link->parent_key.local_xid = 802;
	operation.prepared_sublink_count = 1;
	UT_ASSERT(rf_side_xact_structural_preflight_v1(&operation));
	prepare_apply.mismatched_origin_xid = 803;

	UT_ASSERT_EQ(rf_side_xact_target_preflight_owned_v1(&operation, fake.data,
														operation.prepare_payload_length),
				 RF_SIDE_XACT_APPLY_BLOCKED);
	UT_ASSERT_EQ(prepare_apply.tt_reads, 0);
	UT_ASSERT_EQ(prepare_apply.pending_preflights, 0);
	UT_ASSERT_EQ(prepare_apply.pending_installs, 0);
	UT_ASSERT_EQ(prepare_apply.projection_stores, 0);
}

UT_TEST(test_prepare_apply_never_projects_unverified_pending)
{
	FakeXactRecord fake;
	RfSideXactOperationV1 operation;

	reset_prepare_apply();
	UT_ASSERT(rf_side_xact_decode_v1(make_prepare(&fake, 802, true, false), UINT64_C(0x11223344), 3,
									 &operation));
	prepare_apply.pending_install = TWOPHASE_RECOVERY_PENDING_POST_READ_FAILED;
	UT_ASSERT_EQ(
		rf_side_xact_apply_owned_v1(&operation, fake.data, operation.prepare_payload_length),
		RF_SIDE_XACT_APPLY_POST_READ_FAILED);
	UT_ASSERT_EQ(prepare_apply.pending_installs, 1);
	UT_ASSERT_EQ(prepare_apply.projection_stores, 0);

	reset_prepare_apply();
	prepare_apply.projection_store = CLUSTER_REMOTE_XACT_MUTATION_CONFLICT;
	UT_ASSERT_EQ(
		rf_side_xact_apply_owned_v1(&operation, fake.data, operation.prepare_payload_length),
		RF_SIDE_XACT_APPLY_CONFLICT);
	UT_ASSERT_EQ(prepare_apply.pending_installs, 1);
	UT_ASSERT_EQ(prepare_apply.projection_stores, 1);
	UT_ASSERT_EQ(prepare_apply.projection_postreads, 0);
}

UT_TEST(test_commit_prepared_resolves_exact_native_owner_after_tt_truth)
{
	FakeXactRecord prepare_fake;
	FakeXactRecord terminal_fake;
	RfSideXactOperationV1 operation;
	XLogReaderState *prepare_record;
	UBA head = InvalidUba_init;

	reset_prepare_apply();
	head.raw[0] = UINT64_C(513) | (UINT64_C(9) << 32);
	head.raw[1] = UINT64_C(4);
	prepare_record = make_prepare_with_head(&prepare_fake, 802, true, false, &head);
	prepare_apply.native_payload_length = XLogRecGetDataLen(prepare_record);
	memcpy(prepare_apply.native_payload, XLogRecGetData(prepare_record),
		   prepare_apply.native_payload_length);
	prepare_apply.slot.status = TT_SLOT_COMMITTED;
	prepare_apply.slot.commit_scn = UINT64_C(901);
	prepare_apply.slot.first_undo_block = (UBA)InvalidUba_init;
	UT_ASSERT(rf_side_xact_decode_v1(
		make_commit_prepared(&terminal_fake, 802, 16384, UINT64_C(901), INT64_C(123456)),
		UINT64_C(0x11223344), 3, &operation));
	UT_ASSERT_EQ(operation.kind, RF_SIDE_XACT_COMMIT_PREPARED);
	UT_ASSERT_EQ(rf_side_xact_target_preflight_owned_v1(&operation, NULL, 0),
				 RF_SIDE_XACT_APPLY_OK);
	UT_ASSERT_EQ(rf_side_xact_apply_owned_v1(&operation, NULL, 0), RF_SIDE_XACT_APPLY_OK);
	UT_ASSERT(prepare_apply.pending_reads > 0);
	UT_ASSERT_EQ(prepare_apply.terminal_projection_stores, 1);
	UT_ASSERT_EQ(prepare_apply.pending_resolves, 1);
	UT_ASSERT(prepare_apply.terminal_projection_order < prepare_apply.resolve_order);
}

UT_TEST(test_abort_prepared_resolves_exact_native_owner_after_tt_truth)
{
	FakeXactRecord prepare_fake;
	FakeXactRecord terminal_fake;
	RfSideXactOperationV1 operation;
	XLogReaderState *prepare_record;

	reset_prepare_apply();
	prepare_record = make_prepare(&prepare_fake, 802, true, false);
	prepare_apply.native_payload_length = XLogRecGetDataLen(prepare_record);
	memcpy(prepare_apply.native_payload, XLogRecGetData(prepare_record),
		   prepare_apply.native_payload_length);
	prepare_apply.slot.status = TT_SLOT_ABORTED;
	prepare_apply.slot.commit_scn = InvalidScn;
	prepare_apply.slot.first_undo_block = (UBA)InvalidUba_init;
	UT_ASSERT(rf_side_xact_decode_v1(make_abort_prepared(&terminal_fake, 802, 16384, UINT64_C(902)),
									 UINT64_C(0x11223344), 3, &operation));
	UT_ASSERT_EQ(operation.kind, RF_SIDE_XACT_ABORT_PREPARED);
	UT_ASSERT_EQ(rf_side_xact_target_preflight_owned_v1(&operation, NULL, 0),
				 RF_SIDE_XACT_APPLY_OK);
	UT_ASSERT_EQ(rf_side_xact_apply_owned_v1(&operation, NULL, 0), RF_SIDE_XACT_APPLY_OK);
	UT_ASSERT(prepare_apply.pending_reads > 0);
	UT_ASSERT_EQ(prepare_apply.terminal_projection_stores, 1);
	UT_ASSERT_EQ(prepare_apply.terminal_outcome, CLUSTER_REMOTE_XACT_ABORTED);
	UT_ASSERT_EQ(prepare_apply.pending_resolves, 1);
	UT_ASSERT(!prepare_apply.resolved_is_commit);
	UT_ASSERT(prepare_apply.terminal_projection_order < prepare_apply.resolve_order);
}

UT_TEST(test_abort_prepared_nonempty_undo_retains_native_owner)
{
	FakeXactRecord prepare_fake;
	FakeXactRecord terminal_fake;
	RfSideXactOperationV1 operation;
	XLogReaderState *prepare_record;
	UBA head = InvalidUba_init;

	head.raw[0] = UINT64_C(513) | (UINT64_C(9) << 32);
	head.raw[1] = UINT64_C(4);
	reset_prepare_apply();
	prepare_record = make_prepare_with_head(&prepare_fake, 802, true, false, &head);
	prepare_apply.native_payload_length = XLogRecGetDataLen(prepare_record);
	memcpy(prepare_apply.native_payload, XLogRecGetData(prepare_record),
		   prepare_apply.native_payload_length);
	prepare_apply.slot.status = TT_SLOT_ABORTED;
	prepare_apply.slot.commit_scn = InvalidScn;
	prepare_apply.slot.first_undo_block = (UBA)InvalidUba_init;
	UT_ASSERT(rf_side_xact_decode_v1(make_abort_prepared(&terminal_fake, 802, 16384, UINT64_C(902)),
									 UINT64_C(0x11223344), 3, &operation));
	UT_ASSERT_EQ(rf_side_xact_target_preflight_owned_v1(&operation, NULL, 0),
				 RF_SIDE_XACT_APPLY_BLOCKED);
	UT_ASSERT_EQ(rf_side_xact_apply_owned_v1(&operation, NULL, 0), RF_SIDE_XACT_APPLY_BLOCKED);
	UT_ASSERT_EQ(prepare_apply.terminal_projection_stores, 0);
	UT_ASSERT_EQ(prepare_apply.pending_resolves, 0);
}

UT_TEST(test_online_plan_owns_decoded_operation_not_raw_record)
{
	FakeXactRecord fake;
	RfDetachedRecordPlanV1 record_plan;
	RfPageOnlineRecordIdentityV1 identity;
	RfSideOnlinePlanRequestV1 request;
	RfSideOnlinePlanV1 *plan = NULL;
	RfContributorStreamCutV1 cut;
	RfSideOnlineOperationV1 operation;
	RfSideOnlineApplyOpsV1 apply_ops;
	ApplyCapture capture;
	uint8 storage_uuid[16];

	memset(storage_uuid, 0x42, sizeof(storage_uuid));
	(void)make_commit(&fake, 800, UINT64_C(901), INT64_C(123456), false);
	identity = make_identity(&fake, storage_uuid);
	record_plan = make_record_plan(&fake);
	memset(&cut, 0, sizeof(cut));
	cut.failed_thread = 3;
	cut.timeline_id = 7;
	cut.scan_begin_inclusive = 100;
	cut.scan_end_exclusive = 200;
	cut.flags = RF_CONTRIBUTOR_CUT_COMPLETE;
	memset(&request, 0, sizeof(request));
	request.system_identifier = identity.record.system_identifier;
	memcpy(request.storage_uuid, storage_uuid, sizeof(storage_uuid));
	request.physical_cuts = &cut;
	request.participant_count = 1;
	UT_ASSERT_EQ(rf_side_online_plan_create_v1(&request, &plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &record_plan, &identity),
				 RF_PAGE_PROOF_DETAIL_OK);
	memset(fake.data, 0xee, sizeof(fake.data));
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_operation_count_v1(plan), 1);
	UT_ASSERT(rf_side_online_plan_operation_v1(plan, 0, &operation));
	UT_ASSERT_EQ(operation.xact.xid, 800);
	UT_ASSERT_EQ(operation.xact.terminal_scn, UINT64_C(901));
	memset(&capture, 0, sizeof(capture));
	memset(&apply_ops, 0, sizeof(apply_ops));
	apply_ops.arg = &capture;
	apply_ops.begin_protected_set = capture_begin;
	apply_ops.end_protected_set = capture_end;
	apply_ops.preflight_xact = accept_preflight;
	apply_ops.apply_xact = capture_apply;
	UT_ASSERT_EQ(rf_side_online_plan_apply_v1(plan, &apply_ops), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(capture.count, 1);
	UT_ASSERT_EQ(capture.xid, 800);
	UT_ASSERT_EQ(capture.scn, UINT64_C(901));
	UT_ASSERT_EQ(capture.begin_count, 1);
	UT_ASSERT_EQ(capture.end_count, 1);
	UT_ASSERT(capture.end_complete);
	rf_side_online_plan_destroy_v1(&plan);
	UT_ASSERT(plan == NULL);
}

UT_TEST(test_online_plan_owns_typed_projection_records)
{
	FakeXactRecord clog_fake;
	FakeXactRecord multi_fake;
	FakeXactRecord commit_ts_fake;
	RfDetachedRecordPlanV1 clog_plan;
	RfDetachedRecordPlanV1 multi_plan;
	RfDetachedRecordPlanV1 commit_ts_plan;
	RfPageOnlineRecordIdentityV1 clog_identity;
	RfPageOnlineRecordIdentityV1 multi_identity;
	RfPageOnlineRecordIdentityV1 commit_ts_identity;
	RfSideOnlinePlanRequestV1 request;
	RfSideOnlinePlanV1 *plan = NULL;
	RfContributorStreamCutV1 cut;
	RfSideOnlineOperationV1 operation;
	RfSideOnlineApplyOpsV1 apply_ops;
	ApplyCapture capture;
	xl_multixact_create *create;
	xl_commit_ts_truncate commit_ts;
	uint8 create_payload[SizeOfMultiXactCreate + 2 * sizeof(MultiXactMember)];
	uint8 storage_uuid[16];
	int clog_page = 17;

	memset(storage_uuid, 0x45, sizeof(storage_uuid));
	(void)make_projection_record(&clog_fake, RM_CLOG_ID, CLOG_ZEROPAGE, &clog_page,
								 sizeof(clog_page));
	memset(create_payload, 0, sizeof(create_payload));
	create = (xl_multixact_create *)create_payload;
	create->mid = 33;
	create->moff = 71;
	create->nmembers = 2;
	create->members[0].xid = 800;
	create->members[0].status = MultiXactStatusForKeyShare;
	create->members[1].xid = 816;
	create->members[1].status = MultiXactStatusUpdate;
	(void)make_projection_record(&multi_fake, RM_MULTIXACT_ID, XLOG_MULTIXACT_CREATE_ID,
								 create_payload, sizeof(create_payload));
	memset(&commit_ts, 0, sizeof(commit_ts));
	commit_ts.pageno = 19;
	commit_ts.oldestXid = 800;
	(void)make_projection_record(&commit_ts_fake, RM_COMMIT_TS_ID, COMMIT_TS_TRUNCATE, &commit_ts,
								 SizeOfCommitTsTruncate);

	clog_identity = make_identity(&clog_fake, storage_uuid);
	multi_identity = make_identity(&multi_fake, storage_uuid);
	commit_ts_identity = make_identity(&commit_ts_fake, storage_uuid);
	set_identity_range(&multi_fake, &multi_identity, 200, 300);
	set_identity_range(&commit_ts_fake, &commit_ts_identity, 300, 400);
	clog_plan = make_projection_record_plan(&clog_fake);
	multi_plan = make_projection_record_plan(&multi_fake);
	commit_ts_plan = make_projection_record_plan(&commit_ts_fake);
	memset(&cut, 0, sizeof(cut));
	cut.failed_thread = 3;
	cut.timeline_id = 7;
	cut.scan_begin_inclusive = 100;
	cut.scan_end_exclusive = 400;
	cut.flags = RF_CONTRIBUTOR_CUT_COMPLETE;
	memset(&request, 0, sizeof(request));
	request.system_identifier = clog_identity.record.system_identifier;
	memcpy(request.storage_uuid, storage_uuid, sizeof(storage_uuid));
	request.physical_cuts = &cut;
	request.participant_count = 1;
	UT_ASSERT_EQ(rf_side_online_plan_create_v1(&request, &plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &clog_plan, &clog_identity),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &multi_plan, &multi_identity),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &commit_ts_plan, &commit_ts_identity),
				 RF_PAGE_PROOF_DETAIL_OK);
	memset(create_payload, 0xee, sizeof(create_payload));
	memset(multi_fake.data, 0xee, sizeof(multi_fake.data));
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_operation_count_v1(plan), 3);
	UT_ASSERT(rf_side_online_plan_operation_v1(plan, 0, &operation));
	UT_ASSERT_EQ(operation.kind, RF_SIDE_ONLINE_OPERATION_PROJECTION);
	UT_ASSERT_EQ(operation.projection.kind, CLUSTER_SIDE_PROJECTION_CLOG);
	UT_ASSERT_EQ(operation.projection.action, CLUSTER_SIDE_PROJECTION_ACTION_ZERO_PAGE);
	UT_ASSERT_EQ(operation.projection.page_number, 17);
	UT_ASSERT(rf_side_online_plan_operation_v1(plan, 1, &operation));
	UT_ASSERT_EQ(operation.projection.kind, CLUSTER_SIDE_PROJECTION_MULTIXACT);
	UT_ASSERT_EQ(operation.projection.action, CLUSTER_SIDE_PROJECTION_ACTION_CREATE);
	UT_ASSERT_EQ(operation.projection.multixact_id, 33);
	UT_ASSERT_EQ(operation.projection.member_offset, 71);
	UT_ASSERT_EQ(operation.projection.member_count, 2);
	UT_ASSERT_EQ(operation.owned_payload_length, 2 * sizeof(MultiXactMember));
	UT_ASSERT_EQ(((const MultiXactMember *)operation.owned_payload)[1].xid, 816);
	UT_ASSERT(rf_side_online_plan_operation_v1(plan, 2, &operation));
	UT_ASSERT_EQ(operation.projection.kind, CLUSTER_SIDE_PROJECTION_COMMIT_TS);
	UT_ASSERT_EQ(operation.projection.action, CLUSTER_SIDE_PROJECTION_ACTION_TRUNCATE);
	UT_ASSERT_EQ(operation.projection.page_number, 19);
	UT_ASSERT_EQ(operation.projection.oldest_xid, 800);

	memset(&capture, 0, sizeof(capture));
	memset(&apply_ops, 0, sizeof(apply_ops));
	apply_ops.arg = &capture;
	apply_ops.begin_protected_set = capture_begin;
	apply_ops.end_protected_set = capture_end;
	apply_ops.preflight_projection = accept_preflight;
	apply_ops.apply_projection = capture_apply_projection;
	UT_ASSERT_EQ(rf_side_online_plan_apply_v1(plan, &apply_ops), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(capture.projection_count, 3);
	UT_ASSERT_EQ(capture.projection_first_byte, 0x20);
	UT_ASSERT(capture.end_complete);
	rf_side_online_plan_destroy_v1(&plan);
}

UT_TEST(test_online_plan_rejects_malformed_multixact_projection)
{
	FakeXactRecord fake;
	RfDetachedRecordPlanV1 record_plan;
	RfPageOnlineRecordIdentityV1 identity;
	RfSideOnlinePlanRequestV1 request;
	RfSideOnlinePlanV1 *plan = NULL;
	RfContributorStreamCutV1 cut;
	xl_multixact_create create;
	uint8 storage_uuid[16];

	memset(storage_uuid, 0x46, sizeof(storage_uuid));
	memset(&create, 0, sizeof(create));
	create.mid = 33;
	create.moff = 71;
	create.nmembers = 2;
	(void)make_projection_record(&fake, RM_MULTIXACT_ID, XLOG_MULTIXACT_CREATE_ID, &create,
								 SizeOfMultiXactCreate);
	identity = make_identity(&fake, storage_uuid);
	record_plan = make_projection_record_plan(&fake);
	memset(&cut, 0, sizeof(cut));
	cut.failed_thread = 3;
	cut.timeline_id = 7;
	cut.scan_begin_inclusive = 100;
	cut.scan_end_exclusive = 200;
	cut.flags = RF_CONTRIBUTOR_CUT_COMPLETE;
	memset(&request, 0, sizeof(request));
	request.system_identifier = identity.record.system_identifier;
	memcpy(request.storage_uuid, storage_uuid, sizeof(storage_uuid));
	request.physical_cuts = &cut;
	request.participant_count = 1;
	UT_ASSERT_EQ(rf_side_online_plan_create_v1(&request, &plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &record_plan, &identity),
				 RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE);
	UT_ASSERT_EQ(rf_side_online_plan_operation_count_v1(plan), 0);
	rf_side_online_plan_destroy_v1(&plan);
}

UT_TEST(test_online_plan_denies_incomplete_physical_cut)
{
	RfSideOnlinePlanRequestV1 request;
	RfSideOnlinePlanV1 *plan = NULL;
	RfContributorStreamCutV1 cut;
	uint8 storage_uuid[16];

	memset(storage_uuid, 0x41, sizeof(storage_uuid));
	memset(&cut, 0, sizeof(cut));
	cut.failed_thread = 3;
	cut.timeline_id = 7;
	cut.scan_begin_inclusive = 100;
	cut.scan_end_exclusive = 200;
	cut.flags = RF_CONTRIBUTOR_CUT_COMPLETE;
	memset(&request, 0, sizeof(request));
	request.system_identifier = UINT64_C(0x11223344);
	memcpy(request.storage_uuid, storage_uuid, sizeof(storage_uuid));
	request.physical_cuts = &cut;
	request.participant_count = 1;
	UT_ASSERT_EQ(rf_side_online_plan_create_v1(&request, &plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_SOURCE_GAP);
	UT_ASSERT_EQ(rf_side_online_plan_operation_count_v1(plan), 0);
	rf_side_online_plan_destroy_v1(&plan);
}

UT_TEST(test_online_plan_owns_undo_payload_not_raw_record)
{
	FakeXactRecord fake;
	RfDetachedRecordPlanV1 record_plan;
	RfPageOnlineRecordIdentityV1 identity;
	RfSideOnlinePlanRequestV1 request;
	RfSideOnlinePlanV1 *plan = NULL;
	RfContributorStreamCutV1 cut;
	RfSideOnlineOperationV1 operation;
	RfSideOnlineApplyOpsV1 apply_ops;
	ApplyCapture capture;
	uint8 storage_uuid[16];

	memset(storage_uuid, 0x44, sizeof(storage_uuid));
	(void)make_undo_delta(&fake);
	identity = make_identity(&fake, storage_uuid);
	record_plan = make_undo_record_plan(&fake);
	memset(&cut, 0, sizeof(cut));
	cut.failed_thread = 3;
	cut.timeline_id = 7;
	cut.scan_begin_inclusive = 100;
	cut.scan_end_exclusive = 200;
	cut.flags = RF_CONTRIBUTOR_CUT_COMPLETE;
	memset(&request, 0, sizeof(request));
	request.system_identifier = identity.record.system_identifier;
	memcpy(request.storage_uuid, storage_uuid, sizeof(storage_uuid));
	request.physical_cuts = &cut;
	request.participant_count = 1;
	UT_ASSERT_EQ(rf_side_online_plan_create_v1(&request, &plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &record_plan, &identity),
				 RF_PAGE_PROOF_DETAIL_OK);
	memset(fake.data, 0xee, sizeof(fake.data));
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(rf_side_online_plan_operation_v1(plan, 0, &operation));
	UT_ASSERT_EQ(operation.kind, RF_SIDE_ONLINE_OPERATION_UNDO);
	UT_ASSERT_EQ((int)operation.undo.kind, (int)CLUSTER_UNDO_KIND_BLOCK_WRITE);
	UT_ASSERT_EQ(operation.owned_payload_length,
				 UNDO_BLOCK_HDR_PREFIX_LEN + 24 + sizeof(UndoSlotDirEntry));
	UT_ASSERT_EQ(operation.owned_payload[0], 0x6b);
	memset(&capture, 0, sizeof(capture));
	memset(&apply_ops, 0, sizeof(apply_ops));
	apply_ops.arg = &capture;
	apply_ops.begin_protected_set = capture_begin;
	apply_ops.end_protected_set = capture_end;
	apply_ops.preflight_undo = accept_preflight;
	apply_ops.apply_xact = capture_apply;
	apply_ops.apply_undo = capture_apply_undo;
	UT_ASSERT_EQ(rf_side_online_plan_apply_v1(plan, &apply_ops), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(capture.undo_count, 1);
	UT_ASSERT_EQ(capture.undo_first_byte, 0x6b);
	rf_side_online_plan_destroy_v1(&plan);
}

UT_TEST(test_online_plan_owns_prepare_state_not_raw_record)
{
	FakeXactRecord fake;
	RfDetachedRecordPlanV1 record_plan;
	RfPageOnlineRecordIdentityV1 identity;
	RfSideOnlinePlanRequestV1 request;
	RfSideOnlinePlanV1 *plan = NULL;
	RfContributorStreamCutV1 cut;
	RfSideOnlineOperationV1 operation;
	uint8 storage_uuid[16];
	uint32 magic;

	memset(storage_uuid, 0x46, sizeof(storage_uuid));
	(void)make_prepare(&fake, 801, true, false);
	identity = make_identity(&fake, storage_uuid);
	record_plan = make_record_plan(&fake);
	memset(&cut, 0, sizeof(cut));
	cut.failed_thread = 3;
	cut.timeline_id = 7;
	cut.scan_begin_inclusive = 100;
	cut.scan_end_exclusive = 200;
	cut.flags = RF_CONTRIBUTOR_CUT_COMPLETE;
	memset(&request, 0, sizeof(request));
	request.system_identifier = identity.record.system_identifier;
	memcpy(request.storage_uuid, storage_uuid, sizeof(storage_uuid));
	request.physical_cuts = &cut;
	request.participant_count = 1;
	UT_ASSERT_EQ(rf_side_online_plan_create_v1(&request, &plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &record_plan, &identity),
				 RF_PAGE_PROOF_DETAIL_OK);
	memset(fake.data, 0xee, sizeof(fake.data));
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(rf_side_online_plan_operation_v1(plan, 0, &operation));
	UT_ASSERT_EQ(operation.kind, RF_SIDE_ONLINE_OPERATION_XACT);
	UT_ASSERT_EQ(operation.xact.kind, RF_SIDE_XACT_PREPARE);
	UT_ASSERT_EQ(operation.xact.prepared_owner, 10);
	UT_ASSERT(strcmp(operation.xact.prepare_gid, "gid") == 0);
	UT_ASSERT_EQ(operation.owned_payload_length, operation.xact.prepare_payload_length);
	UT_ASSERT(operation.owned_payload != NULL);
	magic = 0;
	if (operation.owned_payload != NULL)
		memcpy(&magic, operation.owned_payload, sizeof(magic));
	UT_ASSERT_EQ(magic, UINT32_C(0x57F94534));
	rf_side_online_plan_destroy_v1(&plan);
}

UT_TEST(test_online_plan_preflights_all_targets_before_first_mutation)
{
	FakeXactRecord xact_fake;
	FakeXactRecord undo_fake;
	RfDetachedRecordPlanV1 xact_plan;
	RfDetachedRecordPlanV1 undo_plan;
	RfPageOnlineRecordIdentityV1 xact_identity;
	RfPageOnlineRecordIdentityV1 undo_identity;
	RfSideOnlinePlanRequestV1 request;
	RfSideOnlinePlanV1 *plan = NULL;
	RfContributorStreamCutV1 cut;
	RfSideOnlineApplyOpsV1 apply_ops;
	ApplyCapture capture;
	uint8 storage_uuid[16];

	memset(storage_uuid, 0x45, sizeof(storage_uuid));
	(void)make_commit(&xact_fake, 800, UINT64_C(901), INT64_C(123456), false);
	xact_identity = make_identity(&xact_fake, storage_uuid);
	xact_identity.record.end_rec_ptr = 150;
	xact_fake.reader.EndRecPtr = 150;
	xact_fake.u.decoded.next_lsn = 150;
	xact_plan = make_record_plan(&xact_fake);
	(void)make_undo_delta(&undo_fake);
	undo_identity = make_identity(&undo_fake, storage_uuid);
	undo_identity.record.read_rec_ptr = 150;
	undo_fake.reader.ReadRecPtr = 150;
	undo_fake.u.decoded.lsn = 150;
	undo_plan = make_undo_record_plan(&undo_fake);
	memset(&cut, 0, sizeof(cut));
	cut.failed_thread = 3;
	cut.timeline_id = 7;
	cut.scan_begin_inclusive = 100;
	cut.scan_end_exclusive = 200;
	cut.flags = RF_CONTRIBUTOR_CUT_COMPLETE;
	memset(&request, 0, sizeof(request));
	request.system_identifier = xact_identity.record.system_identifier;
	memcpy(request.storage_uuid, storage_uuid, sizeof(storage_uuid));
	request.physical_cuts = &cut;
	request.participant_count = 1;
	UT_ASSERT_EQ(rf_side_online_plan_create_v1(&request, &plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &xact_plan, &xact_identity),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &undo_plan, &undo_identity),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	memset(&capture, 0, sizeof(capture));
	memset(&apply_ops, 0, sizeof(apply_ops));
	apply_ops.arg = &capture;
	apply_ops.begin_protected_set = capture_begin;
	apply_ops.end_protected_set = capture_end;
	apply_ops.preflight_xact = accept_preflight;
	apply_ops.preflight_undo = reject_undo_preflight;
	apply_ops.apply_xact = capture_apply;
	apply_ops.apply_undo = capture_apply_undo;
	UT_ASSERT_EQ(rf_side_online_plan_preflight_v1(plan, &apply_ops),
				 RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE);
	UT_ASSERT_EQ(capture.count, 0);
	UT_ASSERT_EQ(capture.undo_count, 0);
	UT_ASSERT_EQ(capture.begin_count, 1);
	UT_ASSERT_EQ(capture.end_count, 1);
	UT_ASSERT(!capture.end_complete);
	memset(&capture, 0, sizeof(capture));
	UT_ASSERT_EQ(rf_side_online_plan_apply_v1(plan, &apply_ops),
				 RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE);
	UT_ASSERT_EQ(capture.count, 0);
	UT_ASSERT_EQ(capture.undo_count, 0);
	UT_ASSERT_EQ(capture.begin_count, 1);
	UT_ASSERT_EQ(capture.end_count, 1);
	UT_ASSERT(!capture.end_complete);
	rf_side_online_plan_destroy_v1(&plan);
}

UT_TEST(test_online_plan_denies_terminal_missing_required_tt_commit)
{
	FakeXactRecord prepare_fake;
	FakeXactRecord terminal_fake;
	RfDetachedRecordPlanV1 terminal_plan;
	RfPageOnlineRecordIdentityV1 terminal_identity;
	RfSideOnlinePlanRequestV1 request;
	RfSideOnlinePlanV1 *plan = NULL;
	RfContributorStreamCutV1 cut;
	RfSideOnlineApplyOpsV1 apply_ops;
	ApplyCapture capture;
	XLogReaderState *prepare_record;
	uint8 storage_uuid[16];

	reset_prepare_apply();
	prepare_record = make_prepare(&prepare_fake, 802, true, false);
	prepare_apply.native_payload_length = XLogRecGetDataLen(prepare_record);
	memcpy(prepare_apply.native_payload, XLogRecGetData(prepare_record),
		   prepare_apply.native_payload_length);
	(void)make_commit_prepared(&terminal_fake, 802, 16384, UINT64_C(901), INT64_C(123456));
	memset(storage_uuid, 0x42, sizeof(storage_uuid));
	terminal_identity = make_identity(&terminal_fake, storage_uuid);
	terminal_plan = make_record_plan(&terminal_fake);
	memset(&cut, 0, sizeof(cut));
	cut.failed_thread = 3;
	cut.timeline_id = 7;
	cut.scan_begin_inclusive = 100;
	cut.scan_end_exclusive = 200;
	cut.flags = RF_CONTRIBUTOR_CUT_COMPLETE;
	memset(&request, 0, sizeof(request));
	request.system_identifier = terminal_identity.record.system_identifier;
	memcpy(request.storage_uuid, storage_uuid, sizeof(storage_uuid));
	request.physical_cuts = &cut;
	request.participant_count = 1;
	UT_ASSERT_EQ(rf_side_online_plan_create_v1(&request, &plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &terminal_plan, &terminal_identity),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	memset(&capture, 0, sizeof(capture));
	memset(&apply_ops, 0, sizeof(apply_ops));
	apply_ops.arg = &capture;
	apply_ops.begin_protected_set = capture_begin;
	apply_ops.end_protected_set = capture_end;
	apply_ops.preflight_xact = accept_preflight;
	apply_ops.apply_xact = capture_apply;
	UT_ASSERT_EQ(rf_side_online_plan_apply_v1(plan, &apply_ops),
				 RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE);
	UT_ASSERT_EQ(capture.count, 0);
	UT_ASSERT_EQ(capture.begin_count, 1);
	UT_ASSERT_EQ(capture.end_count, 1);
	UT_ASSERT(!capture.end_complete);
	rf_side_online_plan_destroy_v1(&plan);
}

UT_TEST(test_online_plan_accepts_exact_preceding_tt_commit_dependency)
{
	FakeXactRecord prepare_fake;
	FakeXactRecord undo_fake;
	FakeXactRecord terminal_fake;
	RfDetachedRecordPlanV1 undo_plan;
	RfDetachedRecordPlanV1 terminal_plan;
	RfPageOnlineRecordIdentityV1 undo_identity;
	RfPageOnlineRecordIdentityV1 terminal_identity;
	RfSideOnlinePlanRequestV1 request;
	RfSideOnlinePlanV1 *plan = NULL;
	RfContributorStreamCutV1 cut;
	RfSideOnlineApplyOpsV1 apply_ops;
	ApplyCapture capture;
	XLogReaderState *prepare_record;
	uint8 storage_uuid[16];

	reset_prepare_apply();
	prepare_record = make_prepare(&prepare_fake, 802, true, false);
	prepare_apply.native_payload_length = XLogRecGetDataLen(prepare_record);
	memcpy(prepare_apply.native_payload, XLogRecGetData(prepare_record),
		   prepare_apply.native_payload_length);
	(void)make_tt_commit_delta(&undo_fake, 802, UINT64_C(901));
	(void)make_commit_prepared(&terminal_fake, 802, 16384, UINT64_C(901), INT64_C(123456));
	memset(storage_uuid, 0x42, sizeof(storage_uuid));
	undo_identity = make_identity(&undo_fake, storage_uuid);
	terminal_identity = make_identity(&terminal_fake, storage_uuid);
	terminal_identity.record.read_rec_ptr = 200;
	terminal_identity.record.end_rec_ptr = 300;
	terminal_fake.reader.ReadRecPtr = 200;
	terminal_fake.reader.EndRecPtr = 300;
	terminal_fake.u.decoded.lsn = 200;
	terminal_fake.u.decoded.next_lsn = 300;
	undo_plan = make_undo_record_plan(&undo_fake);
	terminal_plan = make_record_plan(&terminal_fake);
	memset(&cut, 0, sizeof(cut));
	cut.failed_thread = 3;
	cut.timeline_id = 7;
	cut.scan_begin_inclusive = 100;
	cut.scan_end_exclusive = 300;
	cut.flags = RF_CONTRIBUTOR_CUT_COMPLETE;
	memset(&request, 0, sizeof(request));
	request.system_identifier = undo_identity.record.system_identifier;
	memcpy(request.storage_uuid, storage_uuid, sizeof(storage_uuid));
	request.physical_cuts = &cut;
	request.participant_count = 1;
	UT_ASSERT_EQ(rf_side_online_plan_create_v1(&request, &plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &undo_plan, &undo_identity),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &terminal_plan, &terminal_identity),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	memset(&capture, 0, sizeof(capture));
	memset(&apply_ops, 0, sizeof(apply_ops));
	apply_ops.arg = &capture;
	apply_ops.begin_protected_set = capture_begin;
	apply_ops.end_protected_set = capture_end;
	apply_ops.preflight_xact = accept_preflight;
	apply_ops.preflight_undo = accept_preflight;
	apply_ops.apply_xact = capture_apply;
	apply_ops.apply_undo = capture_apply_undo;
	UT_ASSERT_EQ(rf_side_online_plan_apply_v1(plan, &apply_ops), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(capture.count, 1);
	UT_ASSERT_EQ(capture.undo_count, 1);
	UT_ASSERT(capture.end_complete);
	rf_side_online_plan_destroy_v1(&plan);
}

UT_TEST(test_online_plan_denies_abort_terminal_missing_tt_abort)
{
	FakeXactRecord prepare_fake;
	FakeXactRecord terminal_fake;
	RfDetachedRecordPlanV1 terminal_plan;
	RfPageOnlineRecordIdentityV1 terminal_identity;
	RfSideOnlinePlanRequestV1 request;
	RfSideOnlinePlanV1 *plan = NULL;
	RfContributorStreamCutV1 cut;
	RfSideOnlineApplyOpsV1 apply_ops;
	ApplyCapture capture;
	XLogReaderState *prepare_record;
	uint8 storage_uuid[16];

	reset_prepare_apply();
	prepare_record = make_prepare(&prepare_fake, 802, true, false);
	prepare_apply.native_payload_length = XLogRecGetDataLen(prepare_record);
	memcpy(prepare_apply.native_payload, XLogRecGetData(prepare_record),
		   prepare_apply.native_payload_length);
	(void)make_abort_prepared(&terminal_fake, 802, 16384, UINT64_C(902));
	memset(storage_uuid, 0x42, sizeof(storage_uuid));
	terminal_identity = make_identity(&terminal_fake, storage_uuid);
	terminal_plan = make_record_plan(&terminal_fake);
	memset(&cut, 0, sizeof(cut));
	cut.failed_thread = 3;
	cut.timeline_id = 7;
	cut.scan_begin_inclusive = 100;
	cut.scan_end_exclusive = 200;
	cut.flags = RF_CONTRIBUTOR_CUT_COMPLETE;
	memset(&request, 0, sizeof(request));
	request.system_identifier = terminal_identity.record.system_identifier;
	memcpy(request.storage_uuid, storage_uuid, sizeof(storage_uuid));
	request.physical_cuts = &cut;
	request.participant_count = 1;
	UT_ASSERT_EQ(rf_side_online_plan_create_v1(&request, &plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &terminal_plan, &terminal_identity),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	memset(&capture, 0, sizeof(capture));
	memset(&apply_ops, 0, sizeof(apply_ops));
	apply_ops.arg = &capture;
	apply_ops.begin_protected_set = capture_begin;
	apply_ops.end_protected_set = capture_end;
	apply_ops.preflight_xact = accept_preflight;
	apply_ops.apply_xact = capture_apply;
	UT_ASSERT_EQ(rf_side_online_plan_apply_v1(plan, &apply_ops),
				 RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE);
	UT_ASSERT_EQ(capture.count, 0);
	UT_ASSERT(!capture.end_complete);
	rf_side_online_plan_destroy_v1(&plan);
}

UT_TEST(test_online_plan_accepts_exact_preceding_tt_abort_dependency)
{
	FakeXactRecord prepare_fake;
	FakeXactRecord abort_fake;
	FakeXactRecord terminal_fake;
	RfDetachedRecordPlanV1 abort_plan;
	RfDetachedRecordPlanV1 terminal_plan;
	RfPageOnlineRecordIdentityV1 abort_identity;
	RfPageOnlineRecordIdentityV1 terminal_identity;
	RfSideOnlinePlanRequestV1 request;
	RfSideOnlinePlanV1 *plan = NULL;
	RfContributorStreamCutV1 cut;
	RfSideOnlineApplyOpsV1 apply_ops;
	ApplyCapture capture;
	XLogReaderState *prepare_record;
	uint8 storage_uuid[16];

	reset_prepare_apply();
	prepare_record = make_prepare(&prepare_fake, 802, true, false);
	prepare_apply.native_payload_length = XLogRecGetDataLen(prepare_record);
	memcpy(prepare_apply.native_payload, XLogRecGetData(prepare_record),
		   prepare_apply.native_payload_length);
	prepare_apply.slot.first_undo_block = (UBA)InvalidUba_init;
	(void)make_tt_abort_delta(&abort_fake, 802);
	(void)make_abort_prepared(&terminal_fake, 802, 16384, UINT64_C(902));
	memset(storage_uuid, 0x42, sizeof(storage_uuid));
	abort_identity = make_identity(&abort_fake, storage_uuid);
	terminal_identity = make_identity(&terminal_fake, storage_uuid);
	set_identity_range(&terminal_fake, &terminal_identity, 200, 300);
	abort_plan = make_undo_record_plan(&abort_fake);
	terminal_plan = make_record_plan(&terminal_fake);
	memset(&cut, 0, sizeof(cut));
	cut.failed_thread = 3;
	cut.timeline_id = 7;
	cut.scan_begin_inclusive = 100;
	cut.scan_end_exclusive = 300;
	cut.flags = RF_CONTRIBUTOR_CUT_COMPLETE;
	memset(&request, 0, sizeof(request));
	request.system_identifier = abort_identity.record.system_identifier;
	memcpy(request.storage_uuid, storage_uuid, sizeof(storage_uuid));
	request.physical_cuts = &cut;
	request.participant_count = 1;
	UT_ASSERT_EQ(rf_side_online_plan_create_v1(&request, &plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &abort_plan, &abort_identity),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &terminal_plan, &terminal_identity),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	memset(&capture, 0, sizeof(capture));
	memset(&apply_ops, 0, sizeof(apply_ops));
	apply_ops.arg = &capture;
	apply_ops.begin_protected_set = capture_begin;
	apply_ops.end_protected_set = capture_end;
	apply_ops.preflight_xact = accept_preflight;
	apply_ops.preflight_undo = accept_preflight;
	apply_ops.apply_xact = capture_apply;
	apply_ops.apply_undo = capture_apply_undo;
	UT_ASSERT_EQ(rf_side_online_plan_apply_v1(plan, &apply_ops), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(capture.count, 1);
	UT_ASSERT_EQ(capture.undo_count, 1);
	UT_ASSERT(capture.end_complete);
	rf_side_online_plan_destroy_v1(&plan);
}

UT_TEST(test_online_plan_denies_nonempty_abort_without_undo_completion)
{
	FakeXactRecord prepare_fake;
	FakeXactRecord abort_fake;
	FakeXactRecord head_fake;
	FakeXactRecord terminal_fake;
	RfDetachedRecordPlanV1 abort_plan;
	RfDetachedRecordPlanV1 head_plan;
	RfDetachedRecordPlanV1 terminal_plan;
	RfPageOnlineRecordIdentityV1 abort_identity;
	RfPageOnlineRecordIdentityV1 head_identity;
	RfPageOnlineRecordIdentityV1 terminal_identity;
	RfSideOnlinePlanRequestV1 request;
	RfSideOnlinePlanV1 *plan = NULL;
	RfContributorStreamCutV1 cut;
	RfSideOnlineApplyOpsV1 apply_ops;
	ApplyCapture capture;
	XLogReaderState *prepare_record;
	UBA head = InvalidUba_init;
	uint8 storage_uuid[16];

	head.raw[0] = UINT64_C(513) | (UINT64_C(9) << 32);
	head.raw[1] = UINT64_C(4);
	reset_prepare_apply();
	prepare_record = make_prepare_with_head(&prepare_fake, 802, true, false, &head);
	prepare_apply.native_payload_length = XLogRecGetDataLen(prepare_record);
	memcpy(prepare_apply.native_payload, XLogRecGetData(prepare_record),
		   prepare_apply.native_payload_length);
	prepare_apply.slot.first_undo_block = head;
	(void)make_tt_abort_delta(&abort_fake, 802);
	(void)make_tt_set_head_delta(&head_fake, 802, &head);
	(void)make_abort_prepared(&terminal_fake, 802, 16384, UINT64_C(902));
	memset(storage_uuid, 0x42, sizeof(storage_uuid));
	abort_identity = make_identity(&abort_fake, storage_uuid);
	head_identity = make_identity(&head_fake, storage_uuid);
	terminal_identity = make_identity(&terminal_fake, storage_uuid);
	set_identity_range(&head_fake, &head_identity, 200, 300);
	set_identity_range(&terminal_fake, &terminal_identity, 200, 300);
	abort_plan = make_undo_record_plan(&abort_fake);
	head_plan = make_undo_record_plan(&head_fake);
	terminal_plan = make_record_plan(&terminal_fake);
	memset(&cut, 0, sizeof(cut));
	cut.failed_thread = 3;
	cut.timeline_id = 7;
	cut.scan_begin_inclusive = 100;
	cut.scan_end_exclusive = 300;
	cut.flags = RF_CONTRIBUTOR_CUT_COMPLETE;
	memset(&request, 0, sizeof(request));
	request.system_identifier = abort_identity.record.system_identifier;
	memcpy(request.storage_uuid, storage_uuid, sizeof(storage_uuid));
	request.physical_cuts = &cut;
	request.participant_count = 1;
	UT_ASSERT_EQ(rf_side_online_plan_create_v1(&request, &plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &abort_plan, &abort_identity),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &terminal_plan, &terminal_identity),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	memset(&capture, 0, sizeof(capture));
	memset(&apply_ops, 0, sizeof(apply_ops));
	apply_ops.arg = &capture;
	apply_ops.begin_protected_set = capture_begin;
	apply_ops.end_protected_set = capture_end;
	apply_ops.preflight_xact = accept_preflight;
	apply_ops.preflight_undo = accept_preflight;
	apply_ops.apply_xact = capture_apply;
	apply_ops.apply_undo = capture_apply_undo;
	UT_ASSERT_EQ(rf_side_online_plan_apply_v1(plan, &apply_ops),
				 RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE);
	UT_ASSERT_EQ(capture.count, 0);
	UT_ASSERT_EQ(capture.undo_count, 0);
	rf_side_online_plan_destroy_v1(&plan);

	cut.scan_end_exclusive = 400;
	set_identity_range(&terminal_fake, &terminal_identity, 300, 400);
	UT_ASSERT_EQ(rf_side_online_plan_create_v1(&request, &plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &abort_plan, &abort_identity),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &head_plan, &head_identity),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &terminal_plan, &terminal_identity),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	memset(&capture, 0, sizeof(capture));
	memset(&apply_ops, 0, sizeof(apply_ops));
	apply_ops.arg = &capture;
	apply_ops.begin_protected_set = capture_begin;
	apply_ops.end_protected_set = capture_end;
	apply_ops.preflight_xact = accept_preflight;
	apply_ops.preflight_undo = accept_preflight;
	apply_ops.apply_xact = capture_apply;
	apply_ops.apply_undo = capture_apply_undo;
	UT_ASSERT_EQ(rf_side_online_plan_apply_v1(plan, &apply_ops),
				 RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE);
	UT_ASSERT_EQ(capture.count, 0);
	UT_ASSERT_EQ(capture.undo_count, 0);
	UT_ASSERT(!capture.end_complete);
	rf_side_online_plan_destroy_v1(&plan);
}

int
main(void)
{
	UT_PLAN(23);
	UT_RUN(test_commit_decodes_to_immutable_truth_operation);
	UT_RUN(test_commit_tt_conflict_is_blocked_before_apply);
	UT_RUN(test_non_xact_and_missing_tt_are_blocked);
	UT_RUN(test_prepare_requires_bounded_aligned_gid);
	UT_RUN(test_prepare_apply_installs_authoritative_pending_before_projection);
	UT_RUN(test_prepare_apply_blocks_underivable_or_wrong_system_before_pending);
	UT_RUN(test_prepare_apply_blocks_wrong_origin_subxid_before_target_reads);
	UT_RUN(test_prepare_apply_never_projects_unverified_pending);
	UT_RUN(test_commit_prepared_resolves_exact_native_owner_after_tt_truth);
	UT_RUN(test_abort_prepared_resolves_exact_native_owner_after_tt_truth);
	UT_RUN(test_abort_prepared_nonempty_undo_retains_native_owner);
	UT_RUN(test_online_plan_owns_decoded_operation_not_raw_record);
	UT_RUN(test_online_plan_owns_typed_projection_records);
	UT_RUN(test_online_plan_rejects_malformed_multixact_projection);
	UT_RUN(test_online_plan_denies_incomplete_physical_cut);
	UT_RUN(test_online_plan_owns_undo_payload_not_raw_record);
	UT_RUN(test_online_plan_owns_prepare_state_not_raw_record);
	UT_RUN(test_online_plan_preflights_all_targets_before_first_mutation);
	UT_RUN(test_online_plan_denies_terminal_missing_required_tt_commit);
	UT_RUN(test_online_plan_accepts_exact_preceding_tt_commit_dependency);
	UT_RUN(test_online_plan_denies_abort_terminal_missing_tt_abort);
	UT_RUN(test_online_plan_accepts_exact_preceding_tt_abort_dependency);
	UT_RUN(test_online_plan_denies_nonempty_abort_without_undo_completion);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
