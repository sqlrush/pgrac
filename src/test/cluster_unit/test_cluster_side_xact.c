/*-------------------------------------------------------------------------
 * test_cluster_side_xact.c
 *    RF-SIDE immutable XACT decode tests.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include <unistd.h>
#include <sys/stat.h>

#include "access/clog.h"
#include "access/commit_ts.h"
#include "access/multixact.h"
#include "access/rmgr.h"
#include "access/twophase.h"
#include "access/twophase_rmgr.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/storage_xlog.h"
#include "catalog/pg_control.h"
#include "catalog/pg_tablespace_d.h"
#include "cluster/cluster_side_xact.h"
#include "cluster/cluster_side_online_plan.h"
#include "cluster/cluster_side_online_owner.h"
#include "cluster/cluster_undo_smgr.h"
#include "cluster/cluster_side_undo.h"
#include "cluster/cluster_tt_durable.h"
#include "cluster/cluster_undo_segment_init.h"
#include "cluster/cluster_tt_2pc.h"
#include "cluster/cluster_xid_stripe.h"
#include "cluster/storage/cluster_undo_xlog.h"
#include "storage/standbydefs.h"

#include "unit_test.h"

UT_DEFINE_GLOBALS();

#include "test_cluster_undo_header_identity.inc"

sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;

void
pg_re_throw(void)
{
	if (PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}

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
	uint32 tt_stamps;
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
static FILE *canonical_file;
static const ClusterRecoveryDutyKey canonical_duty = { .origin_thread_id = 3 };
static ClusterUndoRecoveryScopeV1 *canonical_scope;
static uint32 canonical_writes, canonical_syncs, canonical_writer_depth;
static SCN canonical_projection_scn;
static TimestampTz canonical_projection_time;
static uint16 canonical_projection_wrap;
static TransactionId canonical_expected_xid = 818;
static uint16 canonical_expected_wrap = 8;
static bool canonical_check_block, canonical_fail_data_write, canonical_corrupt_data_write;
static bool canonical_file_io_error, canonical_materialize_failure;
static XLogRecPtr canonical_expected_block_lsn = 400;
ClusterUndoTargetPreflightV1 fixture_preflight_tt_target(const ClusterUndoDecoded *d);
ClusterUndoApplyResultV1 fixture_apply_tt(const ClusterUndoDecoded *d);
static bool canonical_authority_fresh(void *arg);

bool
cluster_undo_recovery_scope_enter_v1(ClusterUndoRecoveryScopeV1 *scope,
	const ClusterThreadRecoveryAuthorityV1 *authority, const RfSideOnlinePlanV1 *plan)
{
	UT_ASSERT(canonical_file != NULL && canonical_scope == NULL && plan != NULL && authority != NULL);
	canonical_scope = scope;
	return true;
}

void cluster_undo_recovery_scope_leave_v1(ClusterUndoRecoveryScopeV1 *scope)
{
	if (canonical_scope == scope) canonical_scope = NULL;
}
void cluster_remote_xact_online_writer_push(void) { canonical_writer_depth++; }
void cluster_remote_xact_online_writer_pop(void) { canonical_writer_depth--; }

bool
cluster_undo_smgr_read_block(ClusterUndoPathIntent intent, uint32 segment,
	uint8 instance, uint32 block, char *out)
{
	UT_ASSERT(canonical_scope != NULL && intent == CLUSTER_UNDO_PATH_RECOVERY_SHARED);
	UT_ASSERT(segment == 513 && instance == 3 && (block == 0 || block == 9));
	return pread(fileno(canonical_file), out, BLCKSZ, (off_t)block * BLCKSZ) == BLCKSZ;
}

bool
cluster_undo_smgr_write_block(ClusterUndoPathIntent intent, uint32 segment,
	uint8 instance, uint32 block, const char *data, bool sync)
{
	UT_ASSERT(canonical_scope != NULL && intent == CLUSTER_UNDO_PATH_RECOVERY_SHARED);
	UT_ASSERT(segment == 513 && instance == 3 && (block == 0 || block == 9) && sync);
	canonical_writes++;
	if (block != 0 && canonical_fail_data_write) return false;
	if (block == 0 && canonical_check_block) {
		PGAlignedBlock physical;

		UT_ASSERT(canonical_syncs != 0);
		UT_ASSERT(pread(fileno(canonical_file), physical.data, BLCKSZ, 9 * BLCKSZ) == BLCKSZ);
		UT_ASSERT_EQ(((UndoBlockHeader *)physical.data)->block_lsn, canonical_expected_block_lsn);
		UT_ASSERT_EQ((uint8)physical.data[128], 0x31);
		UT_ASSERT_EQ((uint8)physical.data[sizeof(UndoBlockHeader)], 0x6b);
	}
	if (pwrite(fileno(canonical_file), data, BLCKSZ, (off_t)block * BLCKSZ) != BLCKSZ) return false;
	if (block != 0 && canonical_corrupt_data_write) {
		uint8 wrong = 0x99;
		UT_ASSERT(pwrite(fileno(canonical_file), &wrong, 1, (off_t)block * BLCKSZ + 128) == 1);
	}
	canonical_syncs++;
	return fsync(fileno(canonical_file)) == 0;
}

bool
cluster_undo_smgr_fsync_segment_file(uint32 segment, uint8 instance)
{
	UT_ASSERT(canonical_scope != NULL && segment == 513 && instance == 3);
	canonical_syncs++;
	return fsync(fileno(canonical_file)) == 0;
}

bool
cluster_undo_smgr_recovery_probe_v1(uint32 segment, uint8 instance,
	ClusterUndoSmgrRecoveryFileV1 *file, char block0[BLCKSZ])
{
	struct stat st;
	Size length;

	UT_ASSERT(canonical_scope != NULL && segment == 513 && instance == 3);
	if (canonical_file_io_error || fstat(fileno(canonical_file), &st) != 0) return false;
	memset(file, 0, sizeof(*file));
	file->exists = true;
	file->device = st.st_dev; file->inode = st.st_ino; file->size = st.st_size;
	length = Min(file->size, BLCKSZ);
	memset(block0, 0, BLCKSZ);
	return pread(fileno(canonical_file), block0, length, 0) == (ssize_t)length;
}

bool
cluster_undo_smgr_recovery_read_block_v1(uint32 segment, uint8 instance, uint32 block,
	const ClusterUndoSmgrRecoveryFileV1 *file, char out[BLCKSZ])
{
	uint64 offset = (uint64)block * BLCKSZ;
	Size length = file->size <= offset ? 0 : Min(file->size - offset, BLCKSZ);

	UT_ASSERT(canonical_scope != NULL && segment == 513 && instance == 3);
	if (canonical_file_io_error) return false;
	memset(out, 0, BLCKSZ);
	return pread(fileno(canonical_file), out, length, offset) == (ssize_t)length;
}

bool
cluster_undo_smgr_recovery_materialize_v1(uint32 segment, uint8 instance,
	const ClusterUndoSmgrRecoveryFileV1 *expected, const char base[BLCKSZ], const char final[BLCKSZ])
{
	ClusterUndoSmgrRecoveryFileV1 observed;
	PGAlignedBlock current;

	if (canonical_materialize_failure || !cluster_undo_smgr_recovery_probe_v1(segment,
		instance, &observed, current.data)) return false;
	UT_ASSERT(UndoSegmentHeader_identity_matches(final, segment, instance));
	if (observed.size != expected->size || observed.inode != expected->inode
		|| memcmp(current.data, base, BLCKSZ) != 0) return false;
	canonical_syncs++;
	return ftruncate(fileno(canonical_file), UNDO_SEGMENT_SIZE_BYTES) == 0
		&& fsync(fileno(canonical_file)) == 0;
}

bool
rf_side_online_projection_owner_init_v1(RfSideOnlineProjectionOwnerV1 *owner,
	uint32 epoch, bool retained)
{
	memset(owner, 0, sizeof(*owner));
	return epoch != 0 && retained;
}
bool rf_side_online_projection_preflight_owned_v1(void *arg, const RfSideOnlineOperationV1 *op)
{ UT_ASSERT(false); return false; }
bool rf_side_online_projection_apply_owned_v1(void *arg, const RfSideOnlineOperationV1 *op)
{ UT_ASSERT(false); return false; }
ClusterUndoTargetPreflightV1 fixture_preflight_tt_target(const ClusterUndoDecoded *d)
{ UT_ASSERT(false); return CLUSTER_UNDO_TARGET_BLOCKED; }
ClusterUndoApplyResultV1 fixture_apply_tt(const ClusterUndoDecoded *d)
{ UT_ASSERT(false); return CLUSTER_UNDO_APPLY_BLOCKED; }

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
{ prepare_apply.tt_stamps++; }

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
	if (canonical_file != NULL) {
		PGAlignedBlock disk;
		TTSlot *slot;

		UT_ASSERT(canonical_scope != NULL && canonical_syncs != 0);
		UT_ASSERT(pread(fileno(canonical_file), disk.data, BLCKSZ, 0) == BLCKSZ);
		slot = &((UndoSegmentHeaderData *)disk.data)->tt_slots[4];
		UT_ASSERT(slot->status == TT_SLOT_COMMITTED && slot->xid == canonical_expected_xid
			&& slot->wrap == canonical_expected_wrap);
		canonical_projection_scn = commit_scn;
		canonical_projection_time = commit_timestamp;
		canonical_projection_wrap = wrap;
	}
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
		if (canonical_file != NULL) {
			*commit_scn = canonical_projection_scn;
			*out_wrap = canonical_projection_wrap;
		}
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
	if (canonical_file != NULL) *commit_timestamp = canonical_projection_time;
	return true;
}

typedef struct FakeXactRecord {
	XLogReaderState reader;
	uint8 data[BLCKSZ + 1024];
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
	tt.segment_id = 513;
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
	uint16 projection_origin;
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
	capture->projection_origin = operation->identity.record.origin_thread;
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

static bool
verify_test_commit_coverage(void *arg, const RfSideXactOperationV1 *operation)
{
	UT_ASSERT_EQ(prepare_apply.terminal_projection_stores, 0);
	UT_ASSERT_EQ(operation->tt_delta.segment_id, 513);
	return *(bool *)arg;
}

UT_TEST(test_covered_commit_requires_owner_proof_before_projection_without_tt_rewind)
{
	FakeXactRecord fake;
	RfSideXactOperationV1 operation;
	bool covered;

	for (int variant = 0; variant < 3; variant++) {
		reset_prepare_apply();
		UT_ASSERT(rf_side_xact_decode_v1(make_commit(&fake, 802, 901, 123456, false),
			UINT64_C(0x11223344), 3, &operation));
		covered = variant != 0;
		if (variant == 2) operation.system_identifier++;
		UT_ASSERT_EQ(rf_side_xact_apply_covered_commit_v1(&operation, &covered, verify_test_commit_coverage),
			variant == 1 ? RF_SIDE_XACT_APPLY_OK : RF_SIDE_XACT_APPLY_BLOCKED);
		UT_ASSERT_EQ(prepare_apply.tt_stamps, 0);
		UT_ASSERT_EQ(prepare_apply.terminal_projection_stores, variant == 1 ? 1 : 0);
	}
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

static void
assert_contribution_owners(const RfSideOnlinePlanV1 *plan, uint32 index, uint32 owners,
						   uint32 space_count)
{
	RfSideContributionOwnersV1 result = { 0 };
	UT_ASSERT(rf_side_online_plan_contribution_owners_v1(plan, index, &result));
	UT_ASSERT_EQ(result.owners, owners);
	UT_ASSERT_EQ(result.space_locator_count, space_count);
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
	assert_contribution_owners(plan, 0,
							   RF_SIDE_CONTRIBUTION_UNDO_HEADER | RF_SIDE_CONTRIBUTION_TERMINAL, 0);
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
	assert_contribution_owners(plan, 0, RF_SIDE_CONTRIBUTION_CLOG, 0);
	assert_contribution_owners(plan, 1, RF_SIDE_CONTRIBUTION_MULTIXACT, 0);
	assert_contribution_owners(plan, 2, RF_SIDE_CONTRIBUTION_COMMIT_TS, 0);

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
	assert_contribution_owners(plan, 0, RF_SIDE_CONTRIBUTION_UNDO_BLOCK, 0);
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
	assert_contribution_owners(plan, 0,
							   RF_SIDE_CONTRIBUTION_PREPARED | RF_SIDE_CONTRIBUTION_UNDO_HEADER
								   | RF_SIDE_CONTRIBUTION_TERMINAL,
							   0);
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

static void
check_preceding_tt_commit_dependency(bool historical)
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
	XLogRecPtr redo = historical ? 200 : 100;

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
	request.redo_starts = &redo;
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
	UT_ASSERT_EQ(capture.undo_count, historical ? 0 : 1);
	UT_ASSERT(capture.end_complete);
	rf_side_online_plan_destroy_v1(&plan);
}

UT_TEST(test_online_plan_accepts_exact_preceding_tt_commit_dependency)
{
	check_preceding_tt_commit_dependency(false);
	check_preceding_tt_commit_dependency(true);
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

static bool
raise_owner_error(void *arg pg_attribute_unused(),
				  const RfSideOnlineOperationV1 *operation pg_attribute_unused())
{
	pg_re_throw();
}

UT_TEST(test_protected_set_released_on_preflight_or_apply_error)
{
	FakeXactRecord fake;
	RfDetachedRecordPlanV1 record_plan;
	RfPageOnlineRecordIdentityV1 identity;
	RfSideOnlinePlanRequestV1 request = {0};
	RfSideOnlinePlanV1 *plan = NULL;
	RfContributorStreamCutV1 cut = {0};
	RfSideOnlineApplyOpsV1 ops = {0};
	ApplyCapture *capture = calloc(1, sizeof(*capture));
	uint8 storage_uuid[16];

	memset(storage_uuid, 0x42, sizeof(storage_uuid));
	(void)make_commit(&fake, 800, UINT64_C(901), INT64_C(123456), false);
	identity = make_identity(&fake, storage_uuid);
	record_plan = make_record_plan(&fake);
	cut.failed_thread = 3;
	cut.timeline_id = 7;
	cut.scan_begin_inclusive = 100;
	cut.scan_end_exclusive = 200;
	cut.flags = RF_CONTRIBUTOR_CUT_COMPLETE;
	request.system_identifier = identity.record.system_identifier;
	memcpy(request.storage_uuid, storage_uuid, sizeof(storage_uuid));
	request.physical_cuts = &cut;
	request.participant_count = 1;
	UT_ASSERT_EQ(rf_side_online_plan_create_v1(&request, &plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &record_plan, &identity), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	ops.arg = capture;
	ops.begin_protected_set = capture_begin;
	ops.end_protected_set = capture_end;
	ops.source_thread = 3;
	for (int cutpoint = 0; cutpoint < 3; cutpoint++) {
		volatile bool caught = false;

		memset(capture, 0, sizeof(*capture));
		ops.preflight_xact = cutpoint == 2 ? accept_preflight : raise_owner_error;
		ops.apply_xact = cutpoint == 2 ? raise_owner_error : capture_apply;
		PG_TRY();
		{
			if (cutpoint == 0)
				(void)rf_side_online_plan_preflight_v1(plan, &ops);
			else
				(void)rf_side_online_plan_apply_v1(plan, &ops);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(caught);
		UT_ASSERT_EQ(capture->begin_count, 1);
		UT_ASSERT_EQ(capture->end_count, 1);
		UT_ASSERT(!capture->end_complete);
		UT_ASSERT_EQ(capture->count, 0);
		UT_ASSERT(PG_exception_stack == NULL);
		/* A clean retry must acquire/release its own scope normally. */
		ops.preflight_xact = accept_preflight;
		ops.apply_xact = capture_apply;
		UT_ASSERT_EQ(rf_side_online_plan_apply_v1(plan, &ops), RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(capture->begin_count, 2);
		UT_ASSERT_EQ(capture->end_count, 2);
		UT_ASSERT(capture->end_complete);
	}
	rf_side_online_plan_destroy_v1(&plan);
	free(capture);
}

static ClusterSpaceReservationChange
space_advance_fixture(void)
{
	ClusterSpaceReservationChange c = {0};

	c.action = CLUSTER_SPACE_RESERVATION_ADVANCE;
	c.before.identity.key.system_identifier = UINT64_C(0x11223344);
	c.before.identity.key.database_incarnation = 42;
	memset(c.before.identity.key.storage_uuid, 0x44, 16);
	c.before.identity.key.locator = (RelFileLocator){DEFAULTTABLESPACE_OID, 5, 16384};
	memset(c.before.identity.incarnation, 0x17, 16);
	c.before.identity.sequence = c.before.identity.operation = 1;
	c.before.identity.state = CLUSTER_SPACE_IDENTITY_LIVE;
	c.before.next_block = c.first_block = 3;
	c.granted = 4;
	c.result = c.before;
	c.result.next_block += c.granted;
	c.before_token = 100;
	c.result_token = 80;
	return c;
}

static RfSideOnlinePlanV1 *
space_online_plan_redo(uint64 end, const XLogRecPtr *redo)
{
	RfSideOnlinePlanV1 *plan = NULL;
	RfContributorStreamCutV1 cut = {0};
	RfSideOnlinePlanRequestV1 request = {0};

	cut.failed_thread = 3;
	cut.timeline_id = 7;
	cut.flags = RF_CONTRIBUTOR_CUT_COMPLETE;
	cut.scan_begin_inclusive = 100;
	cut.scan_end_exclusive = end;
	request.system_identifier = UINT64_C(0x11223344);
	memset(request.storage_uuid, 0x44, 16);
	request.physical_cuts = &cut;
	request.participant_count = 1;
	request.redo_starts = redo;
	UT_ASSERT_EQ(rf_side_online_plan_create_v1(&request, &plan), RF_PAGE_PROOF_DETAIL_OK);
	return plan;
}

static RfSideOnlinePlanV1 *
space_online_plan(uint64 end)
{
	return space_online_plan_redo(end, NULL);
}

static ClusterSpaceStructureChange
space_drop_fixture(uint32 relnumber)
{
	ClusterSpaceReservationChange advance = space_advance_fixture();
	ClusterSpaceStructureChange drop = {0};

	drop.identity.action = CLUSTER_SPACE_WAL_TOMBSTONE;
	drop.identity.nblocks = InvalidBlockNumber;
	drop.identity.expected = advance.result.identity;
	drop.identity.expected.key.locator.relNumber = relnumber;
	drop.identity.result = drop.identity.expected;
	drop.identity.result.state = CLUSTER_SPACE_IDENTITY_TOMBSTONED;
	drop.identity.result.sequence++;
	drop.identity.result.operation++;
	drop.identity.before_token = 123;
	drop.identity.result_token = 70;
	drop.reservation.action = CLUSTER_SPACE_RESERVATION_TOMBSTONE;
	drop.reservation.before = advance.result;
	drop.reservation.before.identity = drop.identity.expected;
	drop.reservation.result = drop.reservation.before;
	drop.reservation.result.identity = drop.identity.result;
	drop.reservation.before_token = advance.result_token;
	drop.reservation.result_token = drop.identity.result_token;
	return drop;
}

static void
make_space_commit(FakeXactRecord *fake, const ClusterSpaceStructureChange *drops, uint32 count)
{
	uint32 prefix = MinSizeOfXactCommit + sizeof(xl_xact_xinfo);
	uint32 locator_bytes = sizeof(int) + count * sizeof(RelFileLocator);
	uint32 old_len;
	uint32 xinfo;
	int nrels = (int)count;

	make_commit(fake, 802, 901, 123456, false);
	fake->u.decoded.max_block_id = -1;
	old_len = fake->u.decoded.main_data_len;
	UT_ASSERT(old_len + locator_bytes + sizeof(count)
		+ count * CLUSTER_SPACE_STRUCTURE_WAL_BYTES <= sizeof(fake->data));
	memmove(fake->data + prefix + locator_bytes, fake->data + prefix, old_len - prefix);
	memcpy(&xinfo, fake->data + MinSizeOfXactCommit, sizeof(xinfo));
	xinfo |= XACT_XINFO_HAS_RELFILELOCATORS | XACT_XINFO_HAS_SPACE_DROP;
	memcpy(fake->data + MinSizeOfXactCommit, &xinfo, sizeof(xinfo));
	memcpy(fake->data + prefix, &nrels, sizeof(nrels));
	for (uint32 i = 0; i < count; i++)
		memcpy(fake->data + prefix + sizeof(nrels) + i * sizeof(RelFileLocator),
			&drops[i].identity.result.key.locator, sizeof(RelFileLocator));
	fake->u.decoded.main_data_len += locator_bytes;
	memcpy(fake->data + fake->u.decoded.main_data_len, &count, sizeof(count));
	fake->u.decoded.main_data_len += sizeof(count);
	for (uint32 i = 0; i < count; i++) {
		UT_ASSERT(cluster_space_structure_wal_encode(&drops[i],
			fake->data + fake->u.decoded.main_data_len, CLUSTER_SPACE_STRUCTURE_WAL_BYTES));
		fake->u.decoded.main_data_len += CLUSTER_SPACE_STRUCTURE_WAL_BYTES;
	}
}

static RfPageProofDetailV1
space_commit_feed(RfSideOnlinePlanV1 *plan, FakeXactRecord *fake, XLogRecPtr begin)
{
	uint8 uuid[16];
	RfPageOnlineRecordIdentityV1 identity;
	RfDetachedRecordPlanV1 record;

	memset(uuid, 0x44, sizeof(uuid));
	identity = make_identity(fake, uuid);
	set_identity_range(fake, &identity, begin, begin + 100);
	record = make_record_plan(fake);
	return rf_side_online_plan_feed_record_v1(plan, &record, &identity);
}

static RfPageProofDetailV1
space_online_feed(RfSideOnlinePlanV1 *plan, uint8 info, const void *wal, uint32 length,
	XLogRecPtr begin, bool block_ref)
{
	FakeXactRecord fake;
	RfDetachedRecordPlanV1 record;
	RfPageOnlineRecordIdentityV1 identity;
	uint8 storage_uuid[16];

	memset(storage_uuid, 0x44, sizeof(storage_uuid));
	make_projection_record(&fake, RM_SMGR_ID, info | XLR_SPECIAL_REL_UPDATE, wal, length);
	fake.u.decoded.max_block_id = block_ref ? 0 : -1;
	identity = make_identity(&fake, storage_uuid);
	set_identity_range(&fake, &identity, begin, begin + 100);
	record = make_projection_record_plan(&fake);
	return rf_side_online_plan_feed_record_v1(plan, &record, &identity);
}

UT_TEST(test_space_commit_owned_multiple_targets_close_exact_source_chains)
{
	ClusterSpaceReservationChange advance = space_advance_fixture();
	ClusterSpaceStructureChange drops[2] = {space_drop_fixture(16384), space_drop_fixture(16385)};
	FakeXactRecord fake;
	RfSideOnlinePlanV1 *plan = space_online_plan(300);
	RfSideOnlineOperationV1 operation;
	uint8 wal[CLUSTER_SPACE_RESERVATION_WAL_BYTES];
	PGAlignedBlock pages[2];
	ClusterSpaceRecoveryImage out;
	uint32 order[2], count;
	RfPageProofDetailV1 detail;

	reset_prepare_apply();
	UT_ASSERT(cluster_space_reservation_wal_encode(&advance, wal, sizeof(wal)));
	UT_ASSERT_EQ(space_online_feed(plan, XLOG_SMGR_SPACE_RESERVATION, wal, sizeof(wal), 100, false),
		RF_PAGE_PROOF_DETAIL_OK);
	make_space_commit(&fake, drops, 2);
	detail = space_commit_feed(plan, &fake, 200);
	UT_ASSERT_EQ(detail, RF_PAGE_PROOF_DETAIL_OK);
	if (detail != RF_PAGE_PROOF_DETAIL_OK)
		goto done;
	/* The native decoder buffer is reused immediately after feeding. */
	memset(fake.data, 0xee, sizeof(fake.data));
	UT_ASSERT(!rf_side_online_plan_bind_database_v1(plan, 43));
	UT_ASSERT(rf_side_online_plan_bind_database_v1(plan, 42));
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_operation_count_v1(plan), 2);
	for (int i = 0; i < 2; i++) {
		ClusterSpaceReservation result;
		uint64 token;

		UT_ASSERT(cluster_space_identity_page_encode(&drops[i].identity.expected, 123,
			pages[0].data, BLCKSZ));
		UT_ASSERT(cluster_space_reservation_page_encode(i == 0 ? &advance.before : &drops[i].reservation.before,
			i == 0 ? advance.before_token : drops[i].reservation.before_token, pages[1].data, BLCKSZ));
		detail = rf_side_online_plan_prepare_space_v1(plan, &drops[i].identity.result.key,
			pages[0].data, pages[1].data, order, 2, &count, &out);
		UT_ASSERT_EQ(detail, RF_PAGE_PROOF_DETAIL_OK);
		if (detail != RF_PAGE_PROOF_DETAIL_OK)
			goto done;
		UT_ASSERT_EQ(count, i == 0 ? 2 : 1);
		UT_ASSERT_EQ(order[count - 1], 1);
		UT_ASSERT_EQ(out.source_index[0], 1);
		UT_ASSERT_EQ(out.source_index[1], 1);
		UT_ASSERT(cluster_space_reservation_page_decode(out.pages[1].data, BLCKSZ,
			SPACE_FORKNUM, 1, &drops[i].identity.result.key, &result, &token));
		UT_ASSERT_EQ(result.identity.state, CLUSTER_SPACE_IDENTITY_TOMBSTONED);
		UT_ASSERT_EQ(result.next_block, 7);
		UT_ASSERT_EQ(token, 70);
	}
	UT_ASSERT(rf_side_online_plan_operation_v1(plan, 1, &operation));
	/* A source plan cannot silently apply TT/projection and drop its structural effects. */
	UT_ASSERT_EQ(rf_side_xact_apply_owned_v1(&operation.xact, operation.owned_payload,
		operation.owned_payload_length), RF_SIDE_XACT_APPLY_BLOCKED);
	UT_ASSERT_EQ(rf_side_xact_apply_v1(&operation.xact), RF_SIDE_XACT_APPLY_BLOCKED);
	UT_ASSERT_EQ(prepare_apply.tt_stamps, 0);
	UT_ASSERT_EQ(prepare_apply.terminal_projection_stores, 0);
done:
	rf_side_online_plan_destroy_v1(&plan);
}

UT_TEST(test_space_commit_rejects_unowned_duplicate_or_malformed_drop_inputs)
{
	for (int fault = 0; fault < 10; fault++) {
		ClusterSpaceStructureChange drops[2] = {space_drop_fixture(16384), space_drop_fixture(16385)};
		RfSideOnlinePlanV1 *plan = space_online_plan(200);
		FakeXactRecord fake;
		uint32 prefix = MinSizeOfXactCommit + sizeof(xl_xact_xinfo) + sizeof(int);

		if (fault == 1) drops[1] = drops[0];
		if (fault == 2) {
			ClusterSpaceStructureChange tmp = drops[0];

			drops[0] = drops[1];
			drops[1] = tmp;
		}
		if (fault == 6 || fault == 7 || fault == 8) {
			ClusterSpaceIdentityKey *key = &drops[1].identity.expected.key;

			if (fault == 6) key->system_identifier++;
			if (fault == 7) key->storage_uuid[0]++;
			if (fault == 8) key->database_incarnation++;
			drops[1].identity.result.key = *key;
			drops[1].reservation.before.identity.key = *key;
			drops[1].reservation.result.identity.key = *key;
		}
		make_space_commit(&fake, drops, 2);
		if (fault == 0) {
			RelFileLocator wrong = {DEFAULTTABLESPACE_OID, 5, 99999};

			memcpy(fake.data + prefix, &wrong, sizeof(wrong));
		}
		if (fault == 3) fake.u.decoded.main_data_len--;
		if (fault == 4) {
			uint32 count = UINT32_MAX;

			memcpy(fake.data + fake.u.decoded.main_data_len
				- 2 * CLUSTER_SPACE_STRUCTURE_WAL_BYTES - sizeof(count), &count, sizeof(count));
		}
		if (fault == 5) fake.u.decoded.max_block_id = 0;
		if (fault == 9) fake.u.decoded.header.xl_info = XLOG_XACT_COMMIT_PREPARED | XLOG_XACT_HAS_INFO;
		if (fault == 8) {
			UT_ASSERT_EQ(space_commit_feed(plan, &fake, 100), RF_PAGE_PROOF_DETAIL_OK);
			UT_ASSERT(!rf_side_online_plan_bind_database_v1(plan, 42));
			UT_ASSERT(!rf_side_online_plan_bind_database_v1(plan, 43));
			UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH);
		} else {
			UT_ASSERT(space_commit_feed(plan, &fake, 100) != RF_PAGE_PROOF_DETAIL_OK);
			UT_ASSERT_EQ(rf_side_online_plan_operation_count_v1(plan), 0);
		}
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_space_commit_retains_all_native_side_effects_and_refuses_tt_only_owner)
{
	ClusterSpaceStructureChange drop = space_drop_fixture(16384);
	RfSideOnlinePlanV1 *plan = space_online_plan(200);
	FakeXactRecord fake;
	RfSideOnlineOperationV1 operation;
	xl_xact_parsed_commit parsed;
	uint8 saved[sizeof(fake.data)];
	TransactionId subxid = 818;
	uint32 prefix = MinSizeOfXactCommit + sizeof(xl_xact_xinfo);
	uint32 xinfo, added = sizeof(int) + sizeof(subxid), len;
	int count = 1;
	RfSideOnlineProductionOwnerV1 owner;
	bool fresh = true;

	reset_prepare_apply();
	make_space_commit(&fake, &drop, 1);
	memmove(fake.data + prefix + added, fake.data + prefix, fake.u.decoded.main_data_len - prefix);
	memcpy(fake.data + prefix, &count, sizeof(count));
	memcpy(fake.data + prefix + sizeof(count), &subxid, sizeof(subxid));
	memcpy(&xinfo, fake.data + MinSizeOfXactCommit, sizeof(xinfo));
	xinfo |= XACT_XINFO_HAS_SUBXACTS;
	memcpy(fake.data + MinSizeOfXactCommit, &xinfo, sizeof(xinfo));
	fake.u.decoded.main_data_len += added;
	len = fake.u.decoded.main_data_len;
	memcpy(saved, fake.data, len);
	UT_ASSERT_EQ(space_commit_feed(plan, &fake, 100), RF_PAGE_PROOF_DETAIL_OK);
	memset(fake.data, 0xee, sizeof(fake.data));
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH);
	UT_ASSERT(rf_side_online_plan_bind_database_v1(plan, 42));
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(rf_side_online_plan_operation_v1(plan, 0, &operation));
	UT_ASSERT_EQ(operation.owned_payload_length, len);
	UT_ASSERT(memcmp(operation.owned_payload, saved, len) == 0);
	UT_ASSERT(ParseCommitRecord(operation.identity.record.info,
		(xl_xact_commit *)operation.owned_payload, operation.owned_payload_length, &parsed));
	UT_ASSERT_EQ(parsed.nsubxacts, 1);
	UT_ASSERT_EQ(parsed.nrels, 1);
	UT_ASSERT_EQ(parsed.nspace_drops, 1);
	UT_ASSERT_EQ(parsed.subxacts[0], subxid);
	UT_ASSERT_EQ(parsed.tt_commit.xid, 802);
	UT_ASSERT_EQ(parsed.scn, 901);
	UT_ASSERT(rf_side_online_production_owner_init_v1(&owner, &fresh,
		canonical_authority_fresh, 9, true));
	UT_ASSERT_EQ(rf_side_online_production_apply_v1(plan, &owner), RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE);
	UT_ASSERT_EQ(prepare_apply.tt_stamps, 0);
	UT_ASSERT_EQ(prepare_apply.terminal_projection_stores, 0);
	rf_side_online_plan_destroy_v1(&plan);
}

UT_TEST(test_space_plan_owns_exact_chain_and_requires_observed_namespace)
{
	ClusterSpaceReservationChange c = space_advance_fixture();
	ClusterSpaceIdentityKey key = c.result.identity.key;
	RfSideOnlinePlanV1 *plan = space_online_plan(300);
	RfSideOnlineApplyOpsV1 ops = {0};
	ApplyCapture capture = {0};
	ClusterSpaceRecoveryImage out = {0};
	PGAlignedBlock pages[2] = {0};
	uint8 wal[CLUSTER_SPACE_RESERVATION_WAL_BYTES];
	uint32 order[2] = {99, 99}, count = 99;
	RfOpcodeRouteV1 route;
	ClusterSpaceReservation final;
	uint64 token;

	UT_ASSERT_EQ(rf_opcode_route_lookup_v1(RM_SMGR_ID, XLOG_SMGR_SPACE_RESERVATION,
		false, false, &route), RF_OPCODE_ROUTE_OK);
	UT_ASSERT_EQ(route.record_owner, RF_ROUTE_OWNER_SIDE_TYPED);

	UT_ASSERT(cluster_space_identity_page_encode(&c.before.identity, 123, pages[0].data, BLCKSZ));
	UT_ASSERT(cluster_space_reservation_page_encode(&c.before, c.before_token, pages[1].data, BLCKSZ));
	for (int i = 0; i < 2; i++) {
		UT_ASSERT(cluster_space_reservation_wal_encode(&c, wal, sizeof(wal)));
		UT_ASSERT_EQ(space_online_feed(plan, XLOG_SMGR_SPACE_RESERVATION, wal, sizeof(wal),
			100 + i * 100, false), RF_PAGE_PROOF_DETAIL_OK);
		c.before = c.result;
		c.before_token = c.result_token;
		c.first_block = c.before.next_block;
		c.result.next_block += c.granted;
		c.result_token -= 20;
	}
	memset(wal, 0xee, sizeof(wal));
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH);
	UT_ASSERT(!rf_side_online_plan_bind_database_v1(plan, 0));
	UT_ASSERT(rf_side_online_plan_bind_database_v1(plan, 42));
	UT_ASSERT(rf_side_online_plan_bind_database_v1(plan, 42));
	UT_ASSERT(!rf_side_online_plan_bind_database_v1(plan, 43));
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_operation_count_v1(plan), 2);
	UT_ASSERT_EQ(rf_side_online_plan_prepare_space_v1(plan, &key, pages[0].data, pages[1].data,
		order, 2, &count, &out), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(count, 2);
	UT_ASSERT_EQ(order[0], 0);
	UT_ASSERT_EQ(order[1], 1);
	UT_ASSERT_EQ(out.apply_mask, 2);
	UT_ASSERT(memcmp(out.pages[0].data, pages[0].data, BLCKSZ) == 0);
	UT_ASSERT(cluster_space_reservation_page_decode(out.pages[1].data, BLCKSZ,
		SPACE_FORKNUM, 1, &key, &final, &token));
	UT_ASSERT_EQ(final.next_block, 11);
	UT_ASSERT_EQ(token, 60);
	/* Collecting source bytes cannot supply the missing protected writer. */
	ops.arg = &capture;
	ops.begin_protected_set = capture_begin;
	ops.end_protected_set = capture_end;
	UT_ASSERT_EQ(rf_side_online_plan_apply_v1(plan, &ops), RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT);
	UT_ASSERT_EQ(capture.begin_count, 0);
	if (count == 2) {
		ClusterSpaceRecoveryImage saved = out;

		count = 99;
		UT_ASSERT_EQ(rf_side_online_plan_prepare_space_v1(plan, &key, pages[0].data,
			pages[1].data, order, 1, &count, &out), RF_PAGE_PROOF_DETAIL_CAPACITY);
		UT_ASSERT_EQ(count, 99);
		UT_ASSERT(memcmp(&out, &saved, sizeof(out)) == 0);
		key.database_incarnation++;
		UT_ASSERT_EQ(rf_side_online_plan_prepare_space_v1(plan, &key, pages[0].data,
			pages[1].data, order, 2, &count, &out), RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH);
		UT_ASSERT_EQ(count, 99);
		UT_ASSERT(memcmp(&out, &saved, sizeof(out)) == 0);
	}
	rf_side_online_plan_destroy_v1(&plan);
}

UT_TEST(test_space_plan_structural_pair_and_standalone_tombstone)
{
	ClusterSpaceStructureChange change = {0};
	ClusterSpaceReservationChange advance = space_advance_fixture();
	RfSideOnlinePlanV1 *plan = space_online_plan(200);
	uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES];
	PGAlignedBlock zero = {0};
	ClusterSpaceRecoveryImage out;
	uint32 order, count = 0;
	RfSideSpaceContributionV1 contribution;

	change.identity.action = CLUSTER_SPACE_WAL_CREATE;
	change.identity.result = advance.result.identity;
	change.identity.result_token = 50;
	change.identity.nblocks = InvalidBlockNumber;
	change.reservation.action = CLUSTER_SPACE_RESERVATION_INIT;
	change.reservation.result.identity = change.identity.result;
	change.reservation.result_token = 50;
	UT_ASSERT(cluster_space_structure_wal_encode(&change, wal, sizeof(wal)));
	UT_ASSERT_EQ(space_online_feed(plan, XLOG_SMGR_SPACE_IDENTITY, wal, sizeof(wal), 100, false),
		RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(!rf_side_online_plan_bind_database_v1(plan, 43));
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_IDENTITY_MISMATCH);
	UT_ASSERT(rf_side_online_plan_bind_database_v1(plan, 42));
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_prepare_space_v1(plan, &change.identity.result.key,
		zero.data, zero.data, &order, 1, &count, &out), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(count, 1);
	UT_ASSERT_EQ(out.apply_mask, 3);
	UT_ASSERT_EQ(rf_side_online_plan_space_contribution_count_v1(plan, 0), 1);
	UT_ASSERT(rf_side_online_plan_space_contribution_v1(plan, 0, 0, &contribution));
	UT_ASSERT_EQ(contribution.page_mask, 3);
	UT_ASSERT_EQ(contribution.result_token[0], change.identity.result_token);
	UT_ASSERT_EQ(contribution.result_token[1], change.reservation.result_token);
	UT_ASSERT(!rf_side_online_plan_bind_database_v1(plan, 42));
	rf_side_online_plan_destroy_v1(&plan);
	change.identity.action = CLUSTER_SPACE_WAL_TOMBSTONE;
	change.identity.expected = change.identity.result;
	change.identity.before_token = change.identity.result_token;
	change.identity.result.sequence++;
	change.identity.result.operation++;
	change.identity.result.state = CLUSTER_SPACE_IDENTITY_TOMBSTONED;
	change.identity.result_token++;
	change.reservation.action = CLUSTER_SPACE_RESERVATION_TOMBSTONE;
	change.reservation.before = change.reservation.result;
	change.reservation.before_token = change.identity.before_token;
	change.reservation.result.identity = change.identity.result;
	change.reservation.result_token = change.identity.result_token;
	UT_ASSERT(cluster_space_structure_wal_encode(&change, wal, sizeof(wal)));
	plan = space_online_plan(200);
	UT_ASSERT(space_online_feed(plan, XLOG_SMGR_SPACE_IDENTITY, wal, sizeof(wal), 100, false)
		!= RF_PAGE_PROOF_DETAIL_OK);
	rf_side_online_plan_destroy_v1(&plan);
}

UT_TEST(test_space_seal_rejects_incomplete_source_chains_before_target_io)
{
	for (int fault = 0; fault < 4; fault++) {
		ClusterSpaceReservationChange c = space_advance_fixture();
		RfSideOnlinePlanV1 *plan = space_online_plan(300);
		uint8 wal[CLUSTER_SPACE_RESERVATION_WAL_BYTES];

		UT_ASSERT(cluster_space_reservation_wal_encode(&c, wal, sizeof(wal)));
		UT_ASSERT_EQ(
			space_online_feed(plan, XLOG_SMGR_SPACE_RESERVATION, wal, sizeof(wal), 100, false),
			RF_PAGE_PROOF_DETAIL_OK);
		c.before = c.result;
		c.before_token = c.result_token;
		c.result_token = 60;
		if (fault == 0)
			c.before_token = 900;
		if (fault == 1)
			c.before_token = 100;
		if (fault == 2)
			c.before.next_block++;
		if (fault == 3)
			c.before.identity.incarnation[0]++;
		c.result = c.before;
		c.first_block = c.before.next_block;
		c.result.next_block += c.granted;
		UT_ASSERT(cluster_space_reservation_wal_encode(&c, wal, sizeof(wal)));
		UT_ASSERT_EQ(
			space_online_feed(plan, XLOG_SMGR_SPACE_RESERVATION, wal, sizeof(wal), 200, false),
			RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT(rf_side_online_plan_bind_database_v1(plan, 42));
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_VERSION_MISMATCH);
		UT_ASSERT_EQ(rf_side_online_plan_operation_count_v1(plan), 0);
		UT_ASSERT_EQ(rf_side_online_plan_space_target_count_v1(plan), UINT32_MAX);
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_space_seal_enumerates_every_unique_commit_and_smgr_target)
{
	ClusterSpaceReservationChange c = space_advance_fixture();
	ClusterSpaceStructureChange drops[2] = { space_drop_fixture(16384), space_drop_fixture(16385) };
	RfSideOnlinePlanV1 *plan = space_online_plan(400);
	uint8 wal[CLUSTER_SPACE_RESERVATION_WAL_BYTES];
	FakeXactRecord fake;
	ClusterSpaceIdentityKey key, saved;

	UT_ASSERT_EQ(rf_side_online_plan_space_target_count_v1(plan), UINT32_MAX);
	c.before.identity.key.locator.relNumber = c.result.identity.key.locator.relNumber = 16385;
	UT_ASSERT(cluster_space_reservation_wal_encode(&c, wal, sizeof(wal)));
	UT_ASSERT_EQ(space_online_feed(plan, XLOG_SMGR_SPACE_RESERVATION, wal, sizeof(wal), 100, false),
				 RF_PAGE_PROOF_DETAIL_OK);
	make_space_commit(&fake, drops, 2);
	UT_ASSERT_EQ(space_commit_feed(plan, &fake, 200), RF_PAGE_PROOF_DETAIL_OK);
	c.before.identity.key.locator.relNumber = c.result.identity.key.locator.relNumber = 16383;
	UT_ASSERT(cluster_space_reservation_wal_encode(&c, wal, sizeof(wal)));
	UT_ASSERT_EQ(space_online_feed(plan, XLOG_SMGR_SPACE_RESERVATION, wal, sizeof(wal), 300, false),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(rf_side_online_plan_bind_database_v1(plan, 42));
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_space_target_count_v1(plan), 3);
	UT_ASSERT_EQ(rf_side_online_plan_operation_count_v1(plan), 3);
	for (uint32 i = 0; i < 3; i++) {
		memset(&key, 0, sizeof(key));
		UT_ASSERT(rf_side_online_plan_space_target_v1(plan, i, &key));
		UT_ASSERT_EQ(key.locator.relNumber, 16383 + i);
		UT_ASSERT_EQ(key.system_identifier, c.result.identity.key.system_identifier);
		UT_ASSERT_EQ(key.database_incarnation, 42);
		UT_ASSERT(memcmp(key.storage_uuid, c.result.identity.key.storage_uuid, 16) == 0);
	}
	saved = key;
	UT_ASSERT(!rf_side_online_plan_space_target_v1(plan, 3, &key));
	UT_ASSERT(memcmp(&key, &saved, sizeof(key)) == 0);
	rf_side_online_plan_destroy_v1(&plan);
}

UT_TEST(test_space_plan_rejects_unbound_native_shapes)
{
	for (int variant = 0; variant < 5; variant++) {
		ClusterSpaceReservationChange c = space_advance_fixture();
		RfSideOnlinePlanV1 *plan = space_online_plan(200);
		uint8 wal[CLUSTER_SPACE_RESERVATION_WAL_BYTES];
		uint8 opcode = XLOG_SMGR_SPACE_RESERVATION;
		uint32 len = sizeof(wal);

		if (variant == 0) c.result.identity.key.system_identifier++;
		if (variant == 1) c.result.identity.key.storage_uuid[0]++;
		if (variant <= 1) c.before.identity.key = c.result.identity.key;
		UT_ASSERT(cluster_space_reservation_wal_encode(&c, wal, sizeof(wal)));
		if (variant == 2) len--;
		if (variant == 3) opcode = XLOG_SMGR_SPACE_IDENTITY;
		UT_ASSERT(space_online_feed(plan, opcode, wal, len, 100, variant == 4)
			!= RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(rf_side_online_plan_operation_count_v1(plan), 0);
		rf_side_online_plan_destroy_v1(&plan);
	}
}

static void
undo_header_feed(RfSideOnlinePlanV1 *plan, FakeXactRecord *fake, XLogRecPtr begin)
{
	uint8 uuid[16];
	RfPageOnlineRecordIdentityV1 identity;
	RfDetachedRecordPlanV1 record;

	memset(uuid, 0x44, sizeof(uuid));
	identity = make_identity(fake, uuid);
	set_identity_range(fake, &identity, begin, begin + 100);
	record = fake->u.decoded.header.xl_rmid == RM_CLUSTER_UNDO_ID
		? make_undo_record_plan(fake) : make_record_plan(fake);
	UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &record, &identity),
		RF_PAGE_PROOF_DETAIL_OK);
	memset(fake->data, 0xee, sizeof(fake->data));
}

static void
undo_header_bind(FakeXactRecord *fake, uint32 generation)
{
	xl_undo_tt_slot_bind bind = {0};

	bind.instance = 3;
	bind.segment_id = 513;
	bind.segment_generation = generation;
	bind.slot_offset = 4;
	bind.wrap = 7;
	bind.xid = 802;
	bind.format_version = CLUSTER_UNDO_TT_BIND_VERSION;
	make_projection_record(fake, RM_CLUSTER_UNDO_ID, XLOG_UNDO_TT_SLOT_BIND,
		&bind, sizeof(bind));
}

static void
undo_header_commit(FakeXactRecord *fake, uint32 generation, TransactionId xid)
{
	xl_xact_tt_commit delta;
	uint32 offset;

	make_commit(fake, xid, 999, 12345, false);
	offset = fake->u.decoded.main_data_len - sizeof(delta);
	memcpy(&delta, fake->data + offset, sizeof(delta));
	delta.segment_id = 513;
	delta.segment_generation = generation;
	memcpy(fake->data + offset, &delta, sizeof(delta));
}

UT_TEST(test_online_undo_header_evolves_init_bind_and_folded_commit)
{
	RfSideOnlinePlanV1 *plan = space_online_plan(500);
	FakeXactRecord fake;
	RfSideUndoHeaderImageV1 image, repeated;
	PGAlignedBlock base;
	struct { xl_cluster_undo_segment_init record; uint8 page[BLCKSZ]; } init = {0};

	init.record.instance = 3;
	init.record.segment_id = 513;
	cluster_undo_segment_make_header_bytes(513, 3, (char *)init.page);
	make_projection_record(&fake, RM_CLUSTER_UNDO_ID, XLOG_UNDO_SEGMENT_INIT,
		&init, sizeof(init));
	undo_header_feed(plan, &fake, 100);
	undo_header_bind(&fake, 0);
	undo_header_feed(plan, &fake, 200);
	make_undo_delta(&fake);
	undo_header_feed(plan, &fake, 300);
	undo_header_commit(&fake, 0, 802);
	undo_header_feed(plan, &fake, 400);
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	assert_contribution_owners(
		plan, 0, RF_SIDE_CONTRIBUTION_UNDO_HEADER | RF_SIDE_CONTRIBUTION_UNDO_BLOCK, 0);
	assert_contribution_owners(plan, 1, RF_SIDE_CONTRIBUTION_UNDO_HEADER, 0);
	assert_contribution_owners(plan, 2, RF_SIDE_CONTRIBUTION_UNDO_BLOCK, 0);
	assert_contribution_owners(plan, 3,
							   RF_SIDE_CONTRIBUTION_UNDO_HEADER | RF_SIDE_CONTRIBUTION_TERMINAL, 0);
	memset(&image, 0x7a, sizeof(image));
	UT_ASSERT_EQ(rf_side_online_plan_prepare_undo_header_v1(plan, 3, 513, NULL, &image),
		RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(image.operation_count, 3);
	UT_ASSERT_EQ(image.source_index, 3);
	UT_ASSERT_EQ(((UndoSegmentHeaderData *)image.page.data)->tt_slots[4].status,
		TT_SLOT_COMMITTED);
	UT_ASSERT_EQ(((UndoSegmentHeaderData *)image.page.data)->tt_slots[4].commit_scn, 999);
	base = image.page;
	UT_ASSERT_EQ(rf_side_online_plan_prepare_undo_header_v1(plan, 3, 513, base.data, &repeated),
		RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(memcmp(repeated.page.data, image.page.data, BLCKSZ) == 0);
	UT_ASSERT(memcmp(base.data, image.page.data, BLCKSZ) == 0);
	rf_side_online_plan_destroy_v1(&plan);
}

UT_TEST(test_online_undo_header_private_base_and_late_failure)
{
	for (int fault = 0; fault < 3; fault++) {
		RfSideOnlinePlanV1 *plan = space_online_plan(300);
		FakeXactRecord fake;
		PGAlignedBlock base, saved;
		RfSideUndoHeaderImageV1 image, unchanged;

		cluster_undo_segment_make_header_bytes(513, 3, base.data);
		saved = base;
		undo_header_bind(&fake, 0);
		undo_header_feed(plan, &fake, 100);
		undo_header_commit(&fake, fault == 1 ? 1 : 0, fault == 2 ? 803 : 802);
		undo_header_feed(plan, &fake, 200);
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
		memset(&unchanged, 0xa9, sizeof(unchanged));
		image = unchanged;
		UT_ASSERT_EQ(rf_side_online_plan_prepare_undo_header_v1(plan, 3, 513, base.data, &image),
			fault == 0 ? RF_PAGE_PROOF_DETAIL_OK : RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE);
		UT_ASSERT(memcmp(base.data, saved.data, BLCKSZ) == 0);
		if (fault != 0) UT_ASSERT(memcmp(&image, &unchanged, sizeof(image)) == 0);
		else {
			UT_ASSERT_EQ(image.operation_count, 2);
			UT_ASSERT_EQ(image.source_index, 1);
		}
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_lifecycle_header_requires_known_generation_and_complete_source_chain)
{
	for (int scenario = 0; scenario < 5; scenario++) {
		RfSideOnlinePlanV1 *plan = space_online_plan(500);
		FakeXactRecord fake;
		PGAlignedBlock base;
		RfSideUndoHeaderImageV1 image, saved;
		struct { xl_undo_segment_reuse record; uint8 page[BLCKSZ]; } reuse = {0};

		cluster_undo_segment_make_header_bytes(513, 3, base.data);
		reuse.record.instance = 3;
		reuse.record.segment_id = 513;
		reuse.record.new_generation = 1;
		cluster_undo_segment_make_header_bytes(513, 3, (char *)reuse.page);
		((UndoSegmentHeaderData *)reuse.page)->wrap_count = 1;
		undo_header_bind(&fake, 0);
		undo_header_feed(plan, &fake, 100);
		undo_header_commit(&fake, 0, scenario == 3 ? 803 : 802);
		undo_header_feed(plan, &fake, 200);
		make_projection_record(&fake, RM_CLUSTER_UNDO_ID, XLOG_UNDO_SEGMENT_REUSE,
			&reuse, sizeof(reuse));
		undo_header_feed(plan, &fake, 300);
		undo_header_bind(&fake, 1);
		undo_header_feed(plan, &fake, 400);
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
		if (scenario != 0) {
			memcpy(base.data, reuse.page, BLCKSZ);
			if (scenario == 1 || scenario == 3) {
				TTSlot *slot = &((UndoSegmentHeaderData *)base.data)->tt_slots[4];
				slot->xid = 802;
				slot->wrap = 7;
				slot->status = TT_SLOT_ACTIVE;
				slot->first_undo_block = (UBA)InvalidUba_init;
			}
			if (scenario == 2) ((UndoSegmentHeaderData *)base.data)->wrap_count = 2;
		}
		memset(&saved, 0x8c, sizeof(saved));
		image = saved;
		UT_ASSERT_EQ(rf_side_online_plan_prepare_undo_header_v1(plan, 3, 513,
			scenario == 4 ? NULL : base.data, &image), scenario == 2 || scenario == 3
			? RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE : RF_PAGE_PROOF_DETAIL_OK);
		if (scenario == 2 || scenario == 3)
			UT_ASSERT(memcmp(&image, &saved, sizeof(image)) == 0);
		else {
			UT_ASSERT_EQ(((UndoSegmentHeaderData *)image.page.data)->wrap_count, 1);
			UT_ASSERT_EQ(((UndoSegmentHeaderData *)image.page.data)->tt_slots[4].status, TT_SLOT_ACTIVE);
			UT_ASSERT_EQ(image.operation_count, 4);
		}
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_recycle_window_repeated_terminal_and_init_unknown_generation)
{
	for (int init = 0; init < 2; init++) {
		RfSideOnlinePlanV1 *plan = space_online_plan(init ? 200 : 400);
		FakeXactRecord fake;
		PGAlignedBlock base;
		RfSideUndoHeaderImageV1 image, saved;

		cluster_undo_segment_make_header_bytes(513, 3, base.data);
		if (init) {
			struct { xl_cluster_undo_segment_init record; uint8 page[BLCKSZ]; } record = {0};
			record.record.instance = 3;
			record.record.segment_id = 513;
			memcpy(record.page, base.data, BLCKSZ);
			make_projection_record(&fake, RM_CLUSTER_UNDO_ID, XLOG_UNDO_SEGMENT_INIT,
				&record, sizeof(record));
			undo_header_feed(plan, &fake, 100);
			((UndoSegmentHeaderData *)base.data)->wrap_count = 1;
		} else {
			xl_undo_segment_recycle record = {0};
			TTSlot *slot = &((UndoSegmentHeaderData *)base.data)->tt_slots[4];
			record.instance = 3;
			record.segment_id = 513;
			record.old_state = SEGMENT_COMMITTED;
			record.new_state = SEGMENT_RECYCLABLE;
			undo_header_bind(&fake, 0);
			undo_header_feed(plan, &fake, 100);
			undo_header_commit(&fake, 0, 802);
			undo_header_feed(plan, &fake, 200);
			make_projection_record(&fake, RM_CLUSTER_UNDO_ID, XLOG_UNDO_SEGMENT_RECYCLE,
				&record, sizeof(record));
			undo_header_feed(plan, &fake, 300);
			slot->xid = 802;
			slot->wrap = 7;
			slot->status = TT_SLOT_COMMITTED;
			slot->commit_scn = 999;
			slot->first_undo_block = (UBA)InvalidUba_init;
		}
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
		memset(&saved, 0x1a, sizeof(saved)); image = saved;
		UT_ASSERT_EQ(rf_side_online_plan_prepare_undo_header_v1(plan, 3, 513, base.data, &image), init
			? RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE : RF_PAGE_PROOF_DETAIL_OK);
		if (init) UT_ASSERT(memcmp(&image, &saved, sizeof(image)) == 0);
		else UT_ASSERT_EQ(((UndoSegmentHeaderData *)image.page.data)->segment_state, SEGMENT_RECYCLABLE);
		rf_side_online_plan_destroy_v1(&plan);
	}
}

static RfSideOnlinePlanV1 *
undo_repeated_slot_plan_sources(bool wrong_second_xid, bool with_peer)
{
	RfSideOnlinePlanV1 *plan = NULL;
	FakeXactRecord fake;
	xl_undo_tt_slot_bind bind;
	xl_xact_tt_commit delta;
	uint32 offset;

	if (with_peer) {
		RfContributorStreamCutV1 cuts[2] = { { 0 } };
		RfSideOnlinePlanRequestV1 request = { 0 };

		for (uint32 i = 0; i < 2; i++) {
			cuts[i].failed_thread = i + 3;
			cuts[i].timeline_id = 7;
			cuts[i].flags = RF_CONTRIBUTOR_CUT_COMPLETE;
			cuts[i].scan_begin_inclusive = 100;
			cuts[i].scan_end_exclusive = i == 0 ? 500 : 200;
		}
		request.system_identifier = UINT64_C(0x11223344);
		memset(request.storage_uuid, 0x44, 16);
		request.physical_cuts = cuts;
		request.participant_count = 2;
		UT_ASSERT_EQ(rf_side_online_plan_create_v1(&request, &plan), RF_PAGE_PROOF_DETAIL_OK);
	} else
		plan = space_online_plan(500);
	undo_header_bind(&fake, 0);
	undo_header_feed(plan, &fake, 100);
	undo_header_commit(&fake, 0, 802);
	undo_header_feed(plan, &fake, 200);
	undo_header_bind(&fake, 0);
	memcpy(&bind, fake.data, sizeof(bind));
	bind.wrap = 8;
	bind.xid = 818;
	memcpy(fake.data, &bind, sizeof(bind));
	undo_header_feed(plan, &fake, 300);
	undo_header_commit(&fake, 0, wrong_second_xid ? 834 : 818);
	offset = fake.u.decoded.main_data_len - sizeof(delta);
	memcpy(&delta, fake.data + offset, sizeof(delta));
	delta.wrap = 8;
	memcpy(fake.data + offset, &delta, sizeof(delta));
	undo_header_feed(plan, &fake, 400);
	if (with_peer) {
		RfPageOnlineRecordIdentityV1 identity;
		RfDetachedRecordPlanV1 record;
		uint8 uuid[16];
		int page = 17;

		memset(uuid, 0x44, 16);
		make_projection_record(&fake, RM_MULTIXACT_ID, XLOG_MULTIXACT_ZERO_OFF_PAGE, &page,
							   sizeof(page));
		identity = make_identity(&fake, uuid);
		identity.participant_index = 1;
		identity.record.origin_thread = 4;
		record = make_projection_record_plan(&fake);
		UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &record, &identity),
					 RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT(rf_side_online_plan_bind_database_v1(plan, 42));
	}
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	return plan;
}

static RfSideOnlinePlanV1 *
undo_repeated_slot_plan(bool wrong_second_xid)
{
	return undo_repeated_slot_plan_sources(wrong_second_xid, false);
}

UT_TEST(test_undo_header_repeated_slot_exact_source_membership_without_init)
{
	RfSideOnlinePlanV1 *plan = undo_repeated_slot_plan(false);
	static const UBA invalid_head = InvalidUba_init;
	RfSideOnlineOperationV1 commit;

	UT_ASSERT(rf_side_online_plan_operation_v1(plan, 1, &commit));
	UT_ASSERT(rf_side_online_plan_contains_commit_v1(plan, &commit.xact));
	commit.xact.terminal_timestamp++;
	UT_ASSERT(!rf_side_online_plan_contains_commit_v1(plan, &commit.xact));

	for (int state = 0; state < 9; state++) {
		PGAlignedBlock base, saved, expected;
		RfSideUndoHeaderImageV1 out, unchanged;
		UndoSegmentHeaderData *header;
		TTSlot *slot;

		cluster_undo_segment_make_header_bytes(513, 3, base.data);
		header = (UndoSegmentHeaderData *)base.data;
		/* Untouched native metadata and slots must survive private replay. */
		header->last_used_at = 4321;
		header->tt_slots[5].xid = 777;
		slot = &header->tt_slots[4];
		if (state != 0) {
			slot->xid = state < 3 ? 802 : 818;
			slot->wrap = state < 3 ? 7 : 8;
			slot->status = state == 1 || state == 3 ? TT_SLOT_ACTIVE : TT_SLOT_COMMITTED;
			slot->commit_scn = slot->status == TT_SLOT_COMMITTED ? 999 : InvalidScn;
			slot->first_undo_block = invalid_head;
		}
		if (state == 5) slot->xid = 834;
		if (state == 6) slot->wrap = 9;
		if (state == 7) slot->commit_scn = 1000;
		if (state == 8) header->wrap_count = 1;
		saved = base;
		expected = base;
		slot = &((UndoSegmentHeaderData *)expected.data)->tt_slots[4];
		memset(slot, 0, sizeof(*slot));
		slot->xid = 818;
		slot->wrap = 8;
		slot->status = TT_SLOT_COMMITTED;
		slot->commit_scn = 999;
		slot->first_undo_block = invalid_head;
		memset(&unchanged, 0xc5, sizeof(unchanged));
		out = unchanged;
		UT_ASSERT_EQ(rf_side_online_plan_prepare_undo_header_v1(plan, 3, 513, base.data, &out),
			state < 5 ? RF_PAGE_PROOF_DETAIL_OK : RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE);
		UT_ASSERT(memcmp(base.data, saved.data, BLCKSZ) == 0);
		if (state < 5) {
			UT_ASSERT(memcmp(out.page.data, expected.data, BLCKSZ) == 0);
			UT_ASSERT_EQ(out.operation_count, 4);
			UT_ASSERT_EQ(out.source_index, state == 4 ? UINT32_MAX : 3);
		} else UT_ASSERT(memcmp(&out, &unchanged, sizeof(out)) == 0);
	}
	rf_side_online_plan_destroy_v1(&plan);
}

UT_TEST(test_undo_header_later_disk_slot_does_not_hide_conflicting_source)
{
	RfSideOnlinePlanV1 *plan = undo_repeated_slot_plan(true);
	PGAlignedBlock base;
	RfSideUndoHeaderImageV1 out, saved;
	TTSlot *slot;

	cluster_undo_segment_make_header_bytes(513, 3, base.data);
	slot = &((UndoSegmentHeaderData *)base.data)->tt_slots[4];
	slot->xid = 818;
	slot->wrap = 8;
	slot->status = TT_SLOT_COMMITTED;
	slot->commit_scn = 999;
	memset(&saved, 0x7d, sizeof(saved));
	out = saved;
	UT_ASSERT_EQ(rf_side_online_plan_prepare_undo_header_v1(plan, 3, 513, base.data, &out),
		RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE);
	UT_ASSERT(memcmp(&out, &saved, sizeof(out)) == 0);
	rf_side_online_plan_destroy_v1(&plan);
}

static bool
canonical_authority_fresh(void *arg)
{
	return arg != NULL;
}

UT_TEST(test_real_plan_owner_and_xact_install_final_header_before_older_commit_projection)
{
	for (int scenario = 0; scenario < 4; scenario++) {
		bool conflict = (scenario & 1) != 0;
		RfSideOnlinePlanV1 *plan = undo_repeated_slot_plan_sources(conflict, scenario >= 2);
		ClusterThreadRecoveryAuthorityV1 authority = { .duty = &canonical_duty };
		RfSideOnlineProductionOwnerV1 owner;
		PGAlignedBlock initial, after;

		reset_prepare_apply();
		canonical_file = tmpfile();
		UT_ASSERT(canonical_file != NULL);
		UT_ASSERT_EQ(ftruncate(fileno(canonical_file), UNDO_SEGMENT_SIZE_BYTES), 0);
		cluster_undo_segment_make_header_bytes(513, 3, initial.data);
		UT_ASSERT(pwrite(fileno(canonical_file), initial.data, BLCKSZ, 0) == BLCKSZ);
		UT_ASSERT_EQ(fsync(fileno(canonical_file)), 0);
		canonical_writes = canonical_syncs = 0;
		UT_ASSERT(rf_side_online_production_owner_init_v1(&owner, &authority,
			canonical_authority_fresh, 19, true));
		UT_ASSERT(rf_side_online_production_bind_undo_v1(&owner, &authority));
		UT_ASSERT_EQ(rf_side_online_production_preflight_v1(plan, &owner), conflict
			? RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE : RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(canonical_writes, 0);
		UT_ASSERT_EQ(prepare_apply.terminal_projection_stores, 0);
		UT_ASSERT_EQ(rf_side_online_production_apply_v1(plan, &owner), conflict
			? RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE : RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(canonical_writes, conflict ? 0 : 1);
		UT_ASSERT_EQ(canonical_syncs, conflict ? 0 : 1);
		UT_ASSERT_EQ(prepare_apply.terminal_projection_stores, conflict ? 0 : 2);
		UT_ASSERT_EQ(prepare_apply.tt_stamps, 0);
		UT_ASSERT(pread(fileno(canonical_file), after.data, BLCKSZ, 0) == BLCKSZ);
		if (conflict) UT_ASSERT(memcmp(after.data, initial.data, BLCKSZ) == 0);
		else {
			UT_ASSERT_EQ(rf_side_online_production_apply_v1(plan, &owner), RF_PAGE_PROOF_DETAIL_OK);
			UT_ASSERT_EQ(canonical_writes, 1);
			UT_ASSERT_EQ(canonical_syncs, 2);
			UT_ASSERT_EQ(prepare_apply.terminal_projection_stores, 4);
		}
		UT_ASSERT(canonical_scope == NULL && canonical_writer_depth == 0 && owner.undo_headers == NULL);
		UT_ASSERT_EQ(fclose(canonical_file), 0);
		canonical_file = NULL;
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_physical_undo_is_durable_before_canonical_tt_publication)
{
	for (int fault = 0; fault < 4; fault++) {
		RfSideOnlinePlanV1 *plan = space_online_plan(fault == 1 ? 400 : 500);
		ClusterThreadRecoveryAuthorityV1 authority = { .duty = &canonical_duty };
		RfSideOnlineProductionOwnerV1 owner;
		FakeXactRecord fake;
		PGAlignedBlock initial, after;
		XLogRecPtr begin = 100;
		struct { xl_undo_block_write record; uint8 image[BLCKSZ]; } fpi = {0};

		undo_header_bind(&fake, 0);
		undo_header_feed(plan, &fake, begin); begin += 100;
		if (fault != 1) {
			fpi.record.instance = 3;
			fpi.record.segment_id = 513;
			fpi.record.block_no = 9;
			fpi.record.has_fpi = 1;
			memset(fpi.image, 0x31, sizeof(fpi.image));
			make_projection_record(&fake, RM_CLUSTER_UNDO_ID, XLOG_UNDO_BLOCK_WRITE, &fpi, sizeof(fpi));
			undo_header_feed(plan, &fake, begin); begin += 100;
		}
		make_undo_delta(&fake);
		undo_header_feed(plan, &fake, begin); begin += 100;
		undo_header_commit(&fake, 0, 802);
		undo_header_feed(plan, &fake, begin);
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
		reset_prepare_apply();
		canonical_file = tmpfile();
		UT_ASSERT(canonical_file != NULL);
		UT_ASSERT_EQ(ftruncate(fileno(canonical_file), UNDO_SEGMENT_SIZE_BYTES), 0);
		cluster_undo_segment_make_header_bytes(513, 3, initial.data);
		UT_ASSERT(pwrite(fileno(canonical_file), initial.data, BLCKSZ, 0) == BLCKSZ);
		UT_ASSERT_EQ(fsync(fileno(canonical_file)), 0);
		canonical_writes = canonical_syncs = 0;
		canonical_expected_xid = 802;
		canonical_expected_wrap = 7;
		canonical_check_block = true;
		canonical_fail_data_write = fault == 2;
		canonical_corrupt_data_write = fault == 3;
		UT_ASSERT(rf_side_online_production_owner_init_v1(&owner, &authority,
			canonical_authority_fresh, 19, true));
		UT_ASSERT(rf_side_online_production_bind_undo_v1(&owner, &authority));
		UT_ASSERT_EQ(rf_side_online_production_preflight_v1(plan, &owner), fault == 1
			? RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE : RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(canonical_writes, 0);
		UT_ASSERT_EQ(rf_side_online_production_apply_v1(plan, &owner), fault == 0
			? RF_PAGE_PROOF_DETAIL_OK : RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE);
		UT_ASSERT_EQ(canonical_writes, fault == 0 ? 2 : fault >= 2 ? 1 : 0);
		UT_ASSERT_EQ(canonical_syncs, fault == 0 ? 2 : fault == 3 ? 1 : 0);
		UT_ASSERT_EQ(prepare_apply.terminal_projection_stores, fault == 0 ? 1 : 0);
		UT_ASSERT(pread(fileno(canonical_file), after.data, BLCKSZ, 0) == BLCKSZ);
		if (fault != 0) UT_ASSERT(memcmp(after.data, initial.data, BLCKSZ) == 0);
		UT_ASSERT(canonical_scope == NULL && canonical_writer_depth == 0 && owner.undo_headers == NULL);
		UT_ASSERT_EQ(fclose(canonical_file), 0);
		canonical_file = NULL;
		canonical_check_block = canonical_fail_data_write = canonical_corrupt_data_write = false;
		canonical_expected_xid = 818;
		canonical_expected_wrap = 8;
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_init_owner_repairs_short_segment_before_header_and_commit)
{
	for (int partial = 0; partial < 5; partial++) {
		RfSideOnlinePlanV1 *plan = space_online_plan(500);
		ClusterThreadRecoveryAuthorityV1 authority = { .duty = &canonical_duty };
		RfSideOnlineProductionOwnerV1 owner;
		FakeXactRecord fake;
		PGAlignedBlock after;
		struct { xl_cluster_undo_segment_init record; uint8 image[BLCKSZ]; } init = {0};
		struct { xl_undo_block_write record; uint8 image[BLCKSZ]; } fpi = {0};

		init.record.instance = fpi.record.instance = 3;
		init.record.segment_id = fpi.record.segment_id = 513;
		cluster_undo_segment_make_header_bytes(513, 3, (char *)init.image);
		make_projection_record(&fake, RM_CLUSTER_UNDO_ID, XLOG_UNDO_SEGMENT_INIT, &init, sizeof(init));
		undo_header_feed(plan, &fake, 100);
		undo_header_bind(&fake, 0);
		undo_header_feed(plan, &fake, 200);
		fpi.record.block_no = 9;
		fpi.record.has_fpi = 1;
		memset(fpi.image, 0x31, BLCKSZ);
		make_projection_record(&fake, RM_CLUSTER_UNDO_ID, XLOG_UNDO_BLOCK_WRITE, &fpi, sizeof(fpi));
		undo_header_feed(plan, &fake, 300);
		undo_header_commit(&fake, 0, 802);
		undo_header_feed(plan, &fake, 400);
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
		reset_prepare_apply();
		canonical_file = tmpfile();
		UT_ASSERT(canonical_file != NULL);
		if (partial) {
			UT_ASSERT_EQ(ftruncate(fileno(canonical_file), partial == 1 ? 17 : UNDO_SEGMENT_SIZE_BYTES), 0);
			UT_ASSERT(pwrite(fileno(canonical_file), init.image, partial == 1 ? 17 : BLCKSZ, 0)
				== (partial == 1 ? 17 : BLCKSZ));
		}
		canonical_writes = canonical_syncs = 0;
		canonical_file_io_error = partial == 3;
		canonical_materialize_failure = partial == 4;
		canonical_expected_xid = 802;
		canonical_expected_wrap = 7;
		UT_ASSERT(rf_side_online_production_owner_init_v1(&owner, &authority, canonical_authority_fresh, 19, true));
		UT_ASSERT(rf_side_online_production_bind_undo_v1(&owner, &authority));
		UT_ASSERT_EQ(rf_side_online_production_preflight_v1(plan, &owner), partial == 3
			? RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE : RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(canonical_writes, 0);
		UT_ASSERT_EQ(rf_side_online_production_apply_v1(plan, &owner), partial >= 3
			? RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE : RF_PAGE_PROOF_DETAIL_OK);
		if (partial >= 3) {
			UT_ASSERT_EQ(canonical_writes, 0);
			UT_ASSERT_EQ(prepare_apply.terminal_projection_stores, 0);
			canonical_file_io_error = canonical_materialize_failure = false;
			UT_ASSERT_EQ(rf_side_online_production_apply_v1(plan, &owner), RF_PAGE_PROOF_DETAIL_OK);
		}
		UT_ASSERT_EQ(canonical_writes, 2);
		UT_ASSERT_EQ(prepare_apply.terminal_projection_stores, 1);
		UT_ASSERT(pread(fileno(canonical_file), after.data, BLCKSZ, 9 * BLCKSZ) == BLCKSZ);
		UT_ASSERT_EQ(((UndoBlockHeader *)after.data)->block_lsn, 400);
		UT_ASSERT_EQ((uint8)after.data[128], 0x31);
		UT_ASSERT_EQ(rf_side_online_production_apply_v1(plan, &owner), RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(canonical_writes, 2);
		UT_ASSERT_EQ(prepare_apply.terminal_projection_stores, 2);
		UT_ASSERT(canonical_scope == NULL && canonical_writer_depth == 0 && owner.undo_headers == NULL);
		UT_ASSERT_EQ(fclose(canonical_file), 0);
		canonical_file = NULL;
		canonical_expected_xid = 818;
		canonical_expected_wrap = 8;
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_reuse_owner_covers_retired_commit_only_after_new_physical_and_tt)
{
	RfSideOnlinePlanV1 *plan = space_online_plan(800);
	ClusterThreadRecoveryAuthorityV1 authority = { .duty = &canonical_duty };
	RfSideOnlineProductionOwnerV1 owner;
	FakeXactRecord fake;
	PGAlignedBlock initial, after;
	xl_undo_tt_slot_bind bind;
	xl_xact_tt_commit commit;
	uint32 offset;
	struct { xl_undo_segment_reuse record; uint8 page[BLCKSZ]; } reuse = {0};
	struct { xl_undo_block_write record; uint8 page[BLCKSZ]; } fpi = {0};

	undo_header_bind(&fake, 0); undo_header_feed(plan, &fake, 100);
	undo_header_commit(&fake, 0, 802); undo_header_feed(plan, &fake, 200);
	reuse.record.instance = 3; reuse.record.segment_id = 513; reuse.record.new_generation = 1;
	cluster_undo_segment_make_header_bytes(513, 3, (char *)reuse.page);
	((UndoSegmentHeaderData *)reuse.page)->wrap_count = 1;
	make_projection_record(&fake, RM_CLUSTER_UNDO_ID, XLOG_UNDO_SEGMENT_REUSE, &reuse, sizeof(reuse));
	undo_header_feed(plan, &fake, 300);
	undo_header_bind(&fake, 1);
	memcpy(&bind, fake.data, sizeof(bind)); bind.xid = 818; bind.wrap = 8;
	memcpy(fake.data, &bind, sizeof(bind)); undo_header_feed(plan, &fake, 400);
	fpi.record.instance = 3; fpi.record.segment_id = 513;
	fpi.record.block_no = 9; fpi.record.has_fpi = 1;
	memset(fpi.page, 0x31, BLCKSZ);
	make_projection_record(&fake, RM_CLUSTER_UNDO_ID, XLOG_UNDO_BLOCK_WRITE, &fpi, sizeof(fpi));
	undo_header_feed(plan, &fake, 500);
	make_undo_delta(&fake); undo_header_feed(plan, &fake, 600);
	undo_header_commit(&fake, 1, 818);
	offset = fake.u.decoded.main_data_len - sizeof(commit);
	memcpy(&commit, fake.data + offset, sizeof(commit)); commit.wrap = 8;
	memcpy(fake.data + offset, &commit, sizeof(commit)); undo_header_feed(plan, &fake, 700);
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	reset_prepare_apply();
	canonical_file = tmpfile(); UT_ASSERT(canonical_file != NULL);
	UT_ASSERT_EQ(ftruncate(fileno(canonical_file), UNDO_SEGMENT_SIZE_BYTES), 0);
	cluster_undo_segment_make_header_bytes(513, 3, initial.data);
	UT_ASSERT(pwrite(fileno(canonical_file), initial.data, BLCKSZ, 0) == BLCKSZ);
	canonical_writes = canonical_syncs = 0;
	canonical_check_block = true; canonical_expected_block_lsn = 700;
	UT_ASSERT(rf_side_online_production_owner_init_v1(&owner, &authority, canonical_authority_fresh, 19, true));
	UT_ASSERT(rf_side_online_production_bind_undo_v1(&owner, &authority));
	UT_ASSERT_EQ(rf_side_online_production_apply_v1(plan, &owner), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(canonical_writes, 2);
	UT_ASSERT_EQ(prepare_apply.terminal_projection_stores, 2);
	UT_ASSERT(pread(fileno(canonical_file), after.data, BLCKSZ, 0) == BLCKSZ);
	UT_ASSERT_EQ(((UndoSegmentHeaderData *)after.data)->wrap_count, 1);
	UT_ASSERT_EQ(((UndoSegmentHeaderData *)after.data)->tt_slots[4].xid, 818);
	UT_ASSERT_EQ(rf_side_online_production_apply_v1(plan, &owner), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(canonical_writes, 2);
	UT_ASSERT_EQ(prepare_apply.terminal_projection_stores, 4);
	UT_ASSERT(canonical_scope == NULL && canonical_writer_depth == 0 && owner.undo_headers == NULL);
	UT_ASSERT_EQ(fclose(canonical_file), 0); canonical_file = NULL;
	canonical_check_block = false; canonical_expected_block_lsn = 400;
	rf_side_online_plan_destroy_v1(&plan);
}

UT_TEST(test_physical_only_cut_and_reuse_anchor_boundary)
{
	for (int reuse = 0; reuse < 2; reuse++) {
		RfSideOnlinePlanV1 *plan = space_online_plan(reuse ? 400 : 300);
		FakeXactRecord fake;
		RfSideUndoBlockImageV1 image, saved;
		struct { xl_undo_block_write record; uint8 image[BLCKSZ]; } fpi = {0};
		XLogRecPtr begin = 200;

		fpi.record.instance = 3;
		fpi.record.segment_id = 513;
		fpi.record.block_no = 9;
		fpi.record.has_fpi = 1;
		memset(fpi.image, 0x31, BLCKSZ);
		make_projection_record(&fake, RM_CLUSTER_UNDO_ID, XLOG_UNDO_BLOCK_WRITE, &fpi, sizeof(fpi));
		undo_header_feed(plan, &fake, 100);
		if (reuse) {
			struct { xl_undo_segment_reuse record; uint8 image[BLCKSZ]; } record = {0};

			record.record.instance = 3;
			record.record.segment_id = 513;
			record.record.new_generation = 1;
			cluster_undo_segment_make_header_bytes(513, 3, (char *)record.image);
			((UndoSegmentHeaderData *)record.image)->wrap_count = 1;
			make_projection_record(&fake, RM_CLUSTER_UNDO_ID, XLOG_UNDO_SEGMENT_REUSE, &record, sizeof(record));
			undo_header_feed(plan, &fake, begin); begin += 100;
		}
		make_undo_delta(&fake);
		undo_header_feed(plan, &fake, begin);
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
		memset(&saved, 0x91, sizeof(saved));
		image = saved;
		UT_ASSERT_EQ(rf_side_online_plan_prepare_undo_block_v1(plan, 3, 513, 9, &image), reuse
			? RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE : RF_PAGE_PROOF_DETAIL_OK);
		if (reuse) UT_ASSERT(memcmp(&image, &saved, sizeof(image)) == 0);
		else {
			RfSideOnlineProductionOwnerV1 owner;
			ClusterThreadRecoveryAuthorityV1 authority = { .duty = &canonical_duty };
			PGAlignedBlock initial, actual;

			canonical_file = tmpfile();
			UT_ASSERT(canonical_file != NULL);
			UT_ASSERT_EQ(ftruncate(fileno(canonical_file), UNDO_SEGMENT_SIZE_BYTES), 0);
			cluster_undo_segment_make_header_bytes(513, 3, initial.data);
			UT_ASSERT(pwrite(fileno(canonical_file), initial.data, BLCKSZ, 0) == BLCKSZ);
			canonical_writes = canonical_syncs = 0;
			UT_ASSERT(rf_side_online_production_owner_init_v1(&owner, &authority, canonical_authority_fresh, 19, true));
			UT_ASSERT(rf_side_online_production_bind_undo_v1(&owner, &authority));
			UT_ASSERT_EQ(rf_side_online_production_apply_v1(plan, &owner), RF_PAGE_PROOF_DETAIL_OK);
			UT_ASSERT_EQ(canonical_writes, 1);
			UT_ASSERT_EQ(canonical_syncs, 1);
			UT_ASSERT(pread(fileno(canonical_file), actual.data, BLCKSZ, 0) == BLCKSZ);
			UT_ASSERT(memcmp(actual.data, initial.data, BLCKSZ) == 0);
			UT_ASSERT(pread(fileno(canonical_file), actual.data, BLCKSZ, 9 * BLCKSZ) == BLCKSZ);
			UT_ASSERT(memcmp(actual.data, image.page.data, BLCKSZ) == 0);
			UT_ASSERT_EQ(rf_side_online_production_apply_v1(plan, &owner), RF_PAGE_PROOF_DETAIL_OK);
			UT_ASSERT_EQ(canonical_writes, 1);
			UT_ASSERT_EQ(canonical_syncs, 2);
			UT_ASSERT(canonical_scope == NULL && canonical_writer_depth == 0 && owner.undo_headers == NULL);
			UT_ASSERT_EQ(fclose(canonical_file), 0);
			canonical_file = NULL;
		}
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_folded_commit_segment_must_belong_to_source_origin)
{
	FakeXactRecord fake;
	xl_xact_tt_commit delta;
	RfSideXactOperationV1 operation;
	uint32 offset;

	make_commit(&fake, 802, 999, 12345, false);
	offset = fake.u.decoded.main_data_len - sizeof(delta);
	memcpy(&delta, fake.data + offset, sizeof(delta));
	delta.segment_id = 9;
	memcpy(fake.data + offset, &delta, sizeof(delta));
	UT_ASSERT(!rf_side_xact_decode_v1(&fake.reader, UINT64_C(0x11223344), 3, &operation));
}

UT_TEST(test_sealed_source_match_requires_observed_namespace_and_exact_cut)
{
	RfSideOnlinePlanV1 *p = space_online_plan(200);
	FakeXactRecord fake;
	RfContributorStreamCutV1 cut = {0};
	uint8 uuid[16];

	memset(uuid, 0x44, sizeof(uuid));
	cut.failed_thread = 3;
	cut.timeline_id = 7;
	cut.flags = RF_CONTRIBUTOR_CUT_COMPLETE;
	cut.scan_begin_inclusive = 100;
	cut.scan_end_exclusive = 200;
	make_undo_delta(&fake);
	undo_header_feed(p, &fake, 100);
	UT_ASSERT(!rf_side_online_plan_source_matches_v1(p, UINT64_C(0x11223344), uuid, &cut));
	UT_ASSERT(rf_side_online_plan_bind_database_v1(p, 42));
	UT_ASSERT(!rf_side_online_plan_source_matches_v1(p, UINT64_C(0x11223344), uuid, &cut));
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(p), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(rf_side_online_plan_source_matches_v1(p, UINT64_C(0x11223344), uuid, &cut));
	for (int fault = 0; fault < 6; fault++) {
		RfContributorStreamCutV1 bad = cut;
		if (fault == 0) bad.failed_thread++;
		if (fault == 1) bad.timeline_id++;
		if (fault == 2) bad.scan_begin_inclusive++;
		if (fault == 3) bad.scan_end_exclusive++;
		if (fault == 4) bad.flags = RF_CONTRIBUTOR_CUT_EXPLICIT_EMPTY;
		if (fault == 5) uuid[0] ^= 1;
		UT_ASSERT(!rf_side_online_plan_source_matches_v1(p, UINT64_C(0x11223344), uuid, &bad));
	}
	rf_side_online_plan_destroy_v1(&p);
	p = space_online_plan(200);
	make_undo_delta(&fake);
	undo_header_feed(p, &fake, 100);
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(p), RF_PAGE_PROOF_DETAIL_OK);
	memset(uuid, 0x44, sizeof(uuid));
	UT_ASSERT(!rf_side_online_plan_source_matches_v1(p, UINT64_C(0x11223344), uuid, &cut));
	rf_side_online_plan_destroy_v1(&p);
}

UT_TEST(test_multixact_retired_page_requires_exact_later_sealed_truncate)
{
	for (int scenario = 0; scenario < 4; scenario++) {
		bool earlier = scenario == 1;
		bool crossing = scenario == 2;
		FakeXactRecord create_fake, truncate_fake;
		RfDetachedRecordPlanV1 create_plan, truncate_plan;
		RfPageOnlineRecordIdentityV1 create_identity, truncate_identity;
		RfSideOnlinePlanRequestV1 request = {0};
		RfContributorStreamCutV1 cut = {0};
		RfSideOnlinePlanV1 *plan = NULL;
		PGAlignedBlock payload;
		xl_multixact_create *create = (xl_multixact_create *)payload.data;
		xl_multixact_truncate trunc = {0};
		uint8 uuid[16];
		XLogRecPtr begin = earlier ? 200 : 100;

		memset(uuid, 0x45, sizeof(uuid));
		memset(&payload, 0, sizeof(payload));
		create->mid = crossing ? (BLCKSZ / sizeof(MultiXactOffset)) * 32 - 1 : 17;
		create->moff = crossing ? (BLCKSZ / 20) * 4 * 32 - 1 : 71;
		create->nmembers = 2;
		create->members[0].xid = 800;
		create->members[0].status = MultiXactStatusForShare;
		create->members[1].xid = 816;
		create->members[1].status = MultiXactStatusForShare;
		make_projection_record(&create_fake, RM_MULTIXACT_ID, XLOG_MULTIXACT_CREATE_ID,
			payload.data, SizeOfMultiXactCreate + 2 * sizeof(MultiXactMember));
		trunc.oldestMultiDB = 1;
		trunc.startTruncOff = 1;
		trunc.endTruncOff = (BLCKSZ / sizeof(MultiXactOffset)) * 32 + (scenario == 3 ? 0 : 1);
		trunc.endTruncMemb = (BLCKSZ / 20) * 4 * 32;
		make_projection_record(&truncate_fake, RM_MULTIXACT_ID, XLOG_MULTIXACT_TRUNCATE_ID,
			&trunc, SizeOfMultiXactTruncate);
		create_identity = make_identity(&create_fake, uuid);
		truncate_identity = make_identity(&truncate_fake, uuid);
		set_identity_range(&create_fake, &create_identity, begin, begin + 100);
		set_identity_range(&truncate_fake, &truncate_identity, earlier ? 100 : 200,
			earlier ? 200 : 300);
		create_plan = make_projection_record_plan(&create_fake);
		truncate_plan = make_projection_record_plan(&truncate_fake);
		cut.failed_thread = 3;
		cut.timeline_id = 7;
		cut.scan_begin_inclusive = 100;
		cut.scan_end_exclusive = 300;
		cut.flags = RF_CONTRIBUTOR_CUT_COMPLETE;
		request.system_identifier = create_identity.record.system_identifier;
		memcpy(request.storage_uuid, uuid, sizeof(uuid));
		request.physical_cuts = &cut;
		request.participant_count = 1;
		UT_ASSERT_EQ(rf_side_online_plan_create_v1(&request, &plan), RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan,
			earlier ? &truncate_plan : &create_plan,
			earlier ? &truncate_identity : &create_identity), RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan,
			earlier ? &create_plan : &truncate_plan,
			earlier ? &create_identity : &truncate_identity), RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT(!rf_side_online_plan_multixact_page_retired_v1(plan, 3, begin, begin + 100, false, 0));
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(rf_side_online_plan_multixact_page_retired_v1(plan, 3, begin, begin + 100, false,
			crossing ? 31 : 0), !earlier && scenario != 3);
		UT_ASSERT_EQ(rf_side_online_plan_multixact_page_retired_v1(plan, 3, begin, begin + 100, true,
			crossing ? 31 : 0), !earlier);
		UT_ASSERT(!rf_side_online_plan_multixact_page_retired_v1(plan, 3, begin, begin + 100, false, 32));
		UT_ASSERT(!rf_side_online_plan_multixact_page_retired_v1(plan, 3, begin, begin + 100, true, 32));
		UT_ASSERT(!rf_side_online_plan_multixact_page_retired_v1(plan, 2, begin, begin + 100, false, 0));
		UT_ASSERT(!rf_side_online_plan_multixact_page_retired_v1(plan, 3, begin, begin + 101, false, 0));
		UT_ASSERT(!rf_side_online_plan_multixact_page_retired_v1(plan, 3, begin, begin + 100, false, 1));
		UT_ASSERT(!rf_side_online_plan_multixact_page_retired_v1(plan, 3, begin, begin + 100, true, UINT32_MAX));
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_multi_origin_side_selection_keeps_exact_source_and_original_plan)
{
	RfSideOnlinePlanRequestV1 request = { 0 };
	RfContributorStreamCutV1 cuts[3] = { { 0 } };
	RfSideOnlinePlanV1 *plan = NULL;
	RfSideOnlineApplyOpsV1 ops = { 0 };
	ApplyCapture capture = { 0 };
	FakeXactRecord fake;
	RfPageOnlineRecordIdentityV1 identity;
	RfDetachedRecordPlanV1 record_plan;
	int page = 17;

	request.system_identifier = UINT64_C(0x11223344);
	memset(request.storage_uuid, 0x45, 16);
	request.participant_count = 3;
	request.physical_cuts = cuts;
	for (uint32 i = 0; i < 3; i++) {
		cuts[i].failed_thread = i + 3;
		cuts[i].timeline_id = 7;
		cuts[i].flags = RF_CONTRIBUTOR_CUT_COMPLETE;
		cuts[i].scan_begin_inclusive = 100;
		cuts[i].scan_end_exclusive = 200;
	}
	/* Third origin has a physically empty input, not a missing origin. */
	cuts[2].scan_end_exclusive = 100;
	cuts[2].flags |= RF_CONTRIBUTOR_CUT_EXPLICIT_EMPTY;
	UT_ASSERT_EQ(rf_side_online_plan_create_v1(&request, &plan), RF_PAGE_PROOF_DETAIL_OK);
	if (plan == NULL)
		return;
	for (uint32 i = 0; i < 2; i++) {
		make_projection_record(&fake, RM_MULTIXACT_ID, XLOG_MULTIXACT_ZERO_OFF_PAGE, &page,
							   sizeof(page));
		identity = make_identity(&fake, request.storage_uuid);
		identity.participant_index = i;
		identity.record.origin_thread = i + 3;
		record_plan = make_projection_record_plan(&fake);
		UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &record_plan, &identity),
					 RF_PAGE_PROOF_DETAIL_OK);
	}
	UT_ASSERT(rf_side_online_plan_bind_database_v1(plan, 42));
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(rf_side_online_plan_source_matches_v1(plan, request.system_identifier,
													request.storage_uuid, &cuts[0]));
	UT_ASSERT(rf_side_online_plan_source_matches_v1(plan, request.system_identifier,
													request.storage_uuid, &cuts[1]));
	UT_ASSERT_EQ(rf_side_online_plan_origin_operation_count_v1(plan, 0), 2);
	UT_ASSERT_EQ(rf_side_online_plan_origin_operation_count_v1(plan, 3), 1);
	UT_ASSERT_EQ(rf_side_online_plan_origin_operation_count_v1(plan, 4), 1);
	UT_ASSERT_EQ(rf_side_online_plan_origin_operation_count_v1(plan, 5), 0);
	UT_ASSERT_EQ(rf_side_online_plan_origin_operation_count_v1(plan, 6), UINT32_MAX);
	ops.arg = &capture;
	ops.begin_protected_set = capture_begin;
	ops.end_protected_set = capture_end;
	ops.preflight_projection = accept_preflight;
	ops.apply_projection = capture_apply_projection;
	for (uint16 origin = 3; origin <= 6; origin++) {
		memset(&capture, 0, sizeof(capture));
		ops.source_thread = origin;
		UT_ASSERT_EQ(rf_side_online_plan_apply_v1(plan, &ops),
					 origin == 6 ? RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT : RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(capture.projection_count, origin < 5 ? 1 : 0);
		UT_ASSERT_EQ(capture.projection_origin, origin < 5 ? origin : 0);
		UT_ASSERT_EQ(capture.begin_count, origin < 5 ? 1 : 0);
		UT_ASSERT_EQ(capture.end_count, capture.begin_count);
	}
	/* The selector leaves the complete immutable input available. */
	UT_ASSERT_EQ(rf_side_online_plan_operation_count_v1(plan), 2);
	UT_ASSERT(rf_side_online_plan_source_matches_v1(plan, request.system_identifier,
													request.storage_uuid, &cuts[0]));
	rf_side_online_plan_destroy_v1(&plan);
}

UT_TEST(test_space_contribution_census_includes_history_and_every_drop_page)
{
	ClusterSpaceReservationChange advance = space_advance_fixture();
	ClusterSpaceStructureChange drops[2] = { space_drop_fixture(16384), space_drop_fixture(16385) };
	XLogRecPtr redo = 300;
	RfSideOnlinePlanV1 *plan = space_online_plan_redo(300, &redo);
	RfSideSpaceContributionV1 contribution, saved;
	RfSideContributionOwnersV1 owners = { UINT32_MAX, UINT32_MAX }, saved_owners = owners;
	RfSideOnlineOperationV1 op;
	FakeXactRecord fake;
	uint8 wal[CLUSTER_SPACE_RESERVATION_WAL_BYTES];

	memset(&contribution, 0x5a, sizeof(contribution));
	saved = contribution;
	UT_ASSERT_EQ(rf_side_online_plan_space_contribution_count_v1(plan, 0), UINT32_MAX);
	UT_ASSERT(!rf_side_online_plan_space_contribution_v1(plan, 0, 0, &contribution));
	UT_ASSERT(!rf_side_online_plan_contribution_owners_v1(plan, 0, &owners));
	UT_ASSERT(memcmp(&owners, &saved_owners, sizeof(owners)) == 0);
	UT_ASSERT(memcmp(&contribution, &saved, sizeof(saved)) == 0);
	UT_ASSERT(cluster_space_reservation_wal_encode(&advance, wal, sizeof(wal)));
	UT_ASSERT_EQ(space_online_feed(plan, XLOG_SMGR_SPACE_RESERVATION, wal, sizeof(wal), 100, false),
				 RF_PAGE_PROOF_DETAIL_OK);
	make_space_commit(&fake, drops, 2);
	UT_ASSERT_EQ(space_commit_feed(plan, &fake, 200), RF_PAGE_PROOF_DETAIL_OK);
	memset(fake.data, 0xee, sizeof(fake.data));
	UT_ASSERT(rf_side_online_plan_bind_database_v1(plan, 42));
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_space_target_count_v1(plan), 0);
	UT_ASSERT(rf_side_online_plan_operation_v1(plan, 0, &op));
	UT_ASSERT(op.history_only);
	UT_ASSERT_EQ(rf_side_online_plan_space_contribution_count_v1(plan, 0), 1);
	UT_ASSERT_EQ(rf_side_online_plan_space_contribution_count_v1(plan, 1), 2);
	assert_contribution_owners(plan, 0, RF_SIDE_CONTRIBUTION_SPACE, 1);
	assert_contribution_owners(plan, 1,
							   RF_SIDE_CONTRIBUTION_SPACE | RF_SIDE_CONTRIBUTION_UNDO_HEADER
								   | RF_SIDE_CONTRIBUTION_TERMINAL,
							   2);
	if (rf_side_online_plan_space_contribution_v1(plan, 0, 0, &contribution)) {
		UT_ASSERT_EQ(contribution.page_mask, 2);
		UT_ASSERT_EQ(contribution.result_token[0], 0);
		UT_ASSERT_EQ(contribution.result_token[1], advance.result_token);
		UT_ASSERT_EQ(contribution.result.key.database_incarnation, 42);
		UT_ASSERT(RelFileLocatorEquals(contribution.result.key.locator,
									   advance.result.identity.key.locator));
	} else
		UT_ASSERT(false);
	for (uint32 i = 0; i < 2; i++) {
		UT_ASSERT(rf_side_online_plan_operation_v1(plan, 1, &op));
		UT_ASSERT(op.history_only);
		if (rf_side_online_plan_space_contribution_v1(plan, 1, i, &contribution)) {
			UT_ASSERT_EQ(contribution.page_mask, 3);
			UT_ASSERT_EQ(contribution.result_token[0], drops[i].identity.result_token);
			UT_ASSERT_EQ(contribution.result_token[1], drops[i].reservation.result_token);
			UT_ASSERT_EQ(contribution.result.state, CLUSTER_SPACE_IDENTITY_TOMBSTONED);
			UT_ASSERT(RelFileLocatorEquals(contribution.result.key.locator,
										   drops[i].identity.result.key.locator));
		} else
			UT_ASSERT(false);
	}
	saved = contribution;
	UT_ASSERT(!rf_side_online_plan_space_contribution_v1(plan, 1, 2, &contribution));
	UT_ASSERT(!rf_side_online_plan_space_contribution_v1(plan, 2, 0, &contribution));
	UT_ASSERT_EQ(rf_side_online_plan_space_contribution_count_v1(plan, 2), UINT32_MAX);
	UT_ASSERT(!rf_side_online_plan_contribution_owners_v1(plan, 2, &owners));
	UT_ASSERT(!rf_side_online_plan_contribution_owners_v1(NULL, 0, &owners));
	UT_ASSERT(!rf_side_online_plan_contribution_owners_v1(plan, 0, NULL));
	UT_ASSERT(memcmp(&owners, &saved_owners, sizeof(owners)) == 0);
	UT_ASSERT(memcmp(&contribution, &saved, sizeof(saved)) == 0);
	rf_side_online_plan_destroy_v1(&plan);
}

UT_TEST(test_retained_commit_drop_does_not_create_space_mutation_target)
{
	ClusterSpaceStructureChange drop = space_drop_fixture(16384);
	XLogRecPtr redo = 200;
	RfSideOnlinePlanV1 *plan = space_online_plan_redo(200, &redo);
	FakeXactRecord fake;
	RfSideOnlineOperationV1 op;
	RfSideOnlineApplyOpsV1 ops = { 0 };
	ApplyCapture capture = { 0 };
	make_space_commit(&fake, &drop, 1);
	UT_ASSERT_EQ(space_commit_feed(plan, &fake, 100), RF_PAGE_PROOF_DETAIL_OK);
	/* History still belongs to the exact database; skipping redo is not
	 * permission to accept an unrelated namespace. */
	UT_ASSERT(!rf_side_online_plan_bind_database_v1(plan, 43));
	UT_ASSERT(rf_side_online_plan_bind_database_v1(plan, 42));
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_space_target_count_v1(plan), 0);
	UT_ASSERT_EQ(rf_side_online_plan_origin_operation_count_v1(plan, 3), 0);
	UT_ASSERT(rf_side_online_plan_operation_v1(plan, 0, &op));
	UT_ASSERT(op.history_only);
	UT_ASSERT_EQ(op.xact.space_drop_count, 1);
	ops.arg = &capture;
	ops.begin_protected_set = capture_begin;
	ops.end_protected_set = capture_end;
	UT_ASSERT_EQ(rf_side_online_plan_apply_v1(plan, &ops), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(capture.begin_count, 0);
	rf_side_online_plan_destroy_v1(&plan);
}

UT_TEST(test_retained_side_history_is_validated_but_never_dispatched)
{
	for (unsigned mode = 0; mode < 4; mode++) {
		RfSideOnlinePlanRequestV1 request = { 0 };
		RfContributorStreamCutV1 cut = { 0 };
		RfSideOnlinePlanV1 *plan = NULL;
		RfSideOnlineOperationV1 op;
		RfSideOnlineApplyOpsV1 ops = { 0 };
		ApplyCapture capture = { 0 };
		FakeXactRecord fake;
		RfPageOnlineRecordIdentityV1 identity;
		RfDetachedRecordPlanV1 record;
		XLogRecPtr redo = mode == 0 ? 100 : mode == 1 ? 200 : mode == 2 ? 300 : 150;
		request.system_identifier = UINT64_C(0x11223344);
		memset(request.storage_uuid, 0x42, 16);
		cut.failed_thread = 3;
		cut.timeline_id = 7;
		cut.flags = RF_CONTRIBUTOR_CUT_COMPLETE;
		cut.scan_begin_inclusive = 100;
		cut.scan_end_exclusive = 300;
		request.physical_cuts = &cut;
		request.participant_count = 1;
		request.redo_starts = &redo;
		UT_ASSERT_EQ(rf_side_online_plan_create_v1(&request, &plan), RF_PAGE_PROOF_DETAIL_OK);
		redo = 0;
		for (unsigned i = 0; i < 2; i++) {
			(void)make_commit(&fake, 800 + i, 901 + i, 123456, false);
			identity = make_identity(&fake, request.storage_uuid);
			set_identity_range(&fake, &identity, 100 + i * 100, 200 + i * 100);
			record = make_record_plan(&fake);
			UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &record, &identity),
						 mode == 3 && i == 0 ? RF_PAGE_PROOF_DETAIL_SOURCE_GAP
											 : RF_PAGE_PROOF_DETAIL_OK);
			if (mode == 3)
				break;
		}
		if (mode == 3) {
			UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_SOURCE_GAP);
			rf_side_online_plan_destroy_v1(&plan);
			continue;
		}
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(rf_side_online_plan_operation_count_v1(plan), 2);
		UT_ASSERT_EQ(rf_side_online_plan_origin_operation_count_v1(plan, 0), 2 - mode);
		UT_ASSERT_EQ(rf_side_online_plan_origin_operation_count_v1(plan, 3), 2 - mode);
		for (unsigned i = 0; i < 2; i++) {
			UT_ASSERT(rf_side_online_plan_operation_v1(plan, i, &op));
			UT_ASSERT_EQ(op.history_only, i < mode);
		}
		ops.arg = &capture;
		ops.begin_protected_set = capture_begin;
		ops.end_protected_set = capture_end;
		if (mode < 2) {
			ops.preflight_xact = accept_preflight;
			ops.apply_xact = capture_apply;
		}
		UT_ASSERT_EQ(rf_side_online_plan_apply_v1(plan, &ops), RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(capture.count, 2 - mode);
		UT_ASSERT_EQ(capture.begin_count, mode < 2 ? 1 : 0);
		UT_ASSERT_EQ(capture.end_count, capture.begin_count);
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_side_plan_does_not_drop_unowned_page_components)
{
	const uint8 classes[]
		= { RF_PAGE_CLASS_ROUTED_HEADER, RF_PAGE_CLASS_ROUTED_SIDE, RF_PAGE_CLASS_ROUTED_SPACE };
	uint8 uuid[16];

	memset(uuid, 0x44, sizeof(uuid));
	for (unsigned i = 0; i < lengthof(classes); i++) {
		RfSideOnlinePlanV1 *plan = space_online_plan(200);
		FakeXactRecord fake = { 0 };
		RfDetachedRecordPlanV1 record = { 0 };
		RfPageOnlineRecordIdentityV1 identity;

		fake.reader.record = &fake.u.decoded;
		fake.u.decoded.header.xl_rmid = RM_XLOG_ID;
		fake.u.decoded.header.xl_info = XLOG_FPI;
		fake.u.decoded.max_block_id = 0;
		fake.u.decoded.blocks[0].in_use = true;
		fake.u.decoded.blocks[0].forknum = i == 2 ? SPACE_FORKNUM : MAIN_FORKNUM;
		identity = make_identity(&fake, uuid);
		record.source_record = &fake.reader;
		record.route.record_owner = RF_ROUTE_OWNER_PAGE_CODEC;
		record.route.rmid = RM_XLOG_ID;
		record.route.codec_id = RF_ROUTE_CODEC_XLOG_FPI;
		record.component_count = 1;
		record.components[0].page_class = classes[i];
		record.components[0].owner = RF_DETACHED_COMPONENT_SIDE_TYPED;
		record.components[0].before_kind = record.components[0].result_kind = RF_PAGE_STATE_ROUTED;
		record.preflight_complete = true;
		UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &record, &identity),
					 RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE);
		/* A refusal did not consume the source position or add any operation.
		 * The same cut can still contain a PAGE-owned ordinary component. */
		record.components[0].page_class
			= i == 1 ? RF_PAGE_CLASS_REBUILDABLE_FSM : RF_PAGE_CLASS_ORDINARY;
		record.components[0].owner
			= i == 1 ? RF_DETACHED_COMPONENT_REBUILDABLE : RF_DETACHED_COMPONENT_PAGE_CODEC;
		record.components[0].before_kind = record.components[0].result_kind
			= i == 1 ? RF_PAGE_STATE_REBUILDABLE : RF_PAGE_STATE_PRESENT;
		fake.u.decoded.blocks[0].forknum = i == 1 ? FSM_FORKNUM : MAIN_FORKNUM;
		UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &record, &identity),
					 RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(rf_side_online_plan_operation_count_v1(plan), 0);
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_retained_generations_refuse_ambiguous_thread_selectors)
{
	RfSideOnlinePlanRequestV1 request = { 0 };
	RfContributorStreamCutV1 cuts[2] = { { 0 } }, changed;
	XLogRecPtr redo[2] = { 200, 100 };
	RfSideOnlinePlanV1 *plan = NULL;
	RfSideOnlineApplyOpsV1 ops = { 0 };
	ApplyCapture capture = { 0 };
	FakeXactRecord fake;
	int page = 17;
	request.system_identifier = UINT64_C(0x11223344);
	memset(request.storage_uuid, 0x45, 16);
	request.participant_count = 2;
	request.physical_cuts = cuts;
	request.redo_starts = redo;
	for (uint32 i = 0; i < 2; i++) {
		cuts[i].failed_thread = 3;
		cuts[i].timeline_id = 7;
		cuts[i].flags = RF_CONTRIBUTOR_CUT_COMPLETE;
		cuts[i].scan_begin_inclusive = 100;
		cuts[i].scan_end_exclusive = 200;
		cuts[i].origin_owner_incarnation = 41 + i;
	}
	UT_ASSERT_EQ(rf_side_online_plan_create_v1(&request, &plan), RF_PAGE_PROOF_DETAIL_OK);
	if (plan == NULL)
		return;
	for (uint32 i = 0; i < 2; i++) {
		RfPageOnlineRecordIdentityV1 identity;
		RfDetachedRecordPlanV1 record;
		make_projection_record(&fake, RM_MULTIXACT_ID, XLOG_MULTIXACT_ZERO_OFF_PAGE, &page,
							   sizeof(page));
		identity = make_identity(&fake, request.storage_uuid);
		identity.participant_index = i;
		record = make_projection_record_plan(&fake);
		UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &record, &identity),
					 RF_PAGE_PROOF_DETAIL_OK);
	}
	UT_ASSERT(rf_side_online_plan_bind_database_v1(plan, 42));
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	for (uint32 i = 0; i < 2; i++)
		UT_ASSERT(rf_side_online_plan_source_matches_v1(plan, request.system_identifier,
														request.storage_uuid, &cuts[i]));
	changed = cuts[1];
	changed.origin_owner_incarnation = 0;
	UT_ASSERT(!rf_side_online_plan_source_matches_v1(plan, request.system_identifier,
													 request.storage_uuid, &changed));
	changed.origin_owner_incarnation = 43;
	UT_ASSERT(!rf_side_online_plan_source_matches_v1(plan, request.system_identifier,
													 request.storage_uuid, &changed));
	UT_ASSERT_EQ(rf_side_online_plan_origin_operation_count_v1(plan, 0), 1);
	UT_ASSERT_EQ(rf_side_online_plan_origin_operation_count_v1(plan, 3), UINT32_MAX);
	ops.arg = &capture;
	ops.begin_protected_set = capture_begin;
	ops.end_protected_set = capture_end;
	ops.preflight_projection = accept_preflight;
	ops.apply_projection = capture_apply_projection;
	ops.source_thread = 3;
	UT_ASSERT_EQ(rf_side_online_plan_apply_v1(plan, &ops), RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT);
	UT_ASSERT_EQ(capture.begin_count, 0);
	UT_ASSERT_EQ(capture.projection_count, 0);
	rf_side_online_plan_destroy_v1(&plan);
}

static uint32
native_control_payload(uint8 info, uint8 *payload)
{
	CheckPoint checkpoint = { 0 };
	xl_parameter_change parameters = { 0 };
	xl_restore_point restore = { 0 };
	xl_end_of_recovery end = { 0 };
	xl_overwrite_contrecord overwrite = { 0 };
	Oid oid = 5;
	XLogRecPtr backup = 50;
	checkpoint.ThisTimeLineID = checkpoint.PrevTimeLineID = 7;
	checkpoint.redo = 100;
	checkpoint.nextXid = FullTransactionIdFromU64(20);
	checkpoint.fullPageWrites = true;
	parameters.MaxConnections = parameters.max_locks_per_xact = 20;
	parameters.wal_level = WAL_LEVEL_REPLICA;
	end.ThisTimeLineID = end.PrevTimeLineID = 7;
	overwrite.overwritten_lsn = 50;
#define CONTROL_COPY(value_)                                                                       \
	do {                                                                                           \
		memcpy(payload, &(value_), sizeof(value_));                                                \
		return sizeof(value_);                                                                     \
	} while (0)
	switch (info) {
	case XLOG_CHECKPOINT_SHUTDOWN:
	case XLOG_CHECKPOINT_ONLINE:
		CONTROL_COPY(checkpoint);
	case XLOG_PARAMETER_CHANGE:
		CONTROL_COPY(parameters);
	case XLOG_FPW_CHANGE:
		payload[0] = 1;
		return sizeof(bool);
	case XLOG_NEXTOID:
		CONTROL_COPY(oid);
	case XLOG_BACKUP_END:
		CONTROL_COPY(backup);
	case XLOG_RESTORE_POINT:
		CONTROL_COPY(restore);
	case XLOG_END_OF_RECOVERY:
		CONTROL_COPY(end);
	case XLOG_OVERWRITE_CONTRECORD:
		CONTROL_COPY(overwrite);
	case XLOG_SWITCH:
		return 0;
	default:
		payload[0] = 0x52;
		return 1;
	}
#undef CONTROL_COPY
}

UT_TEST(test_native_control_is_owned_input_and_not_replay_permission)
{
	static const uint8 infos[] = { XLOG_CHECKPOINT_SHUTDOWN,
								   XLOG_CHECKPOINT_ONLINE,
								   XLOG_PARAMETER_CHANGE,
								   XLOG_FPW_CHANGE,
								   XLOG_NEXTOID,
								   XLOG_NOOP,
								   XLOG_SWITCH,
								   XLOG_BACKUP_END,
								   XLOG_RESTORE_POINT,
								   XLOG_END_OF_RECOVERY,
								   XLOG_OVERWRITE_CONTRECORD };
	for (unsigned i = 0; i < lengthof(infos); i++) {
		RfSideOnlinePlanV1 *plan = space_online_plan(200);
		FakeXactRecord fake;
		uint8 payload[512] = { 0 }, uuid[16];
		uint32 length = native_control_payload(infos[i], payload);
		RfPageOnlineRecordIdentityV1 identity;
		RfDetachedRecordPlanV1 record;
		RfSideOnlineOperationV1 operation;
		RfSideOnlineApplyOpsV1 ops = { 0 };
		ApplyCapture capture = { 0 };
		memset(uuid, 0x44, sizeof(uuid));
		make_projection_record(&fake, RM_XLOG_ID, infos[i], payload, length);
		identity = make_identity(&fake, uuid);
		fake.u.decoded.max_block_id = -1;
		record = make_projection_record_plan(&fake);
		UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &record, &identity),
					 RF_PAGE_PROOF_DETAIL_OK);
		memset(fake.data, 0xa5, length);
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(rf_side_online_plan_operation_count_v1(plan), 1);
		assert_contribution_owners(plan, 0, RF_SIDE_CONTRIBUTION_NATIVE_CONTROL, 0);
		if (rf_side_online_plan_operation_v1(plan, 0, &operation)) {
			UT_ASSERT_EQ(operation.kind, RF_SIDE_ONLINE_OPERATION_NATIVE_CONTROL);
			UT_ASSERT_EQ(operation.owned_payload_length, length);
			if (length > 0)
				UT_ASSERT_EQ(memcmp(operation.owned_payload, payload, length), 0);
			UT_ASSERT(!operation.history_only);
		} else
			UT_ASSERT(false);
		ops.arg = &capture;
		ops.begin_protected_set = capture_begin;
		ops.end_protected_set = capture_end;
		ops.preflight_projection = accept_preflight;
		ops.apply_projection = capture_apply_projection;
		UT_ASSERT_EQ(rf_side_online_plan_apply_v1(plan, &ops),
					 RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT);
		UT_ASSERT_EQ(capture.begin_count + capture.projection_count, 0);
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_native_control_rejects_bad_shape_and_identity)
{
	for (unsigned fault = 0; fault < 9; fault++) {
		RfSideOnlinePlanV1 *plan = space_online_plan(200);
		FakeXactRecord fake;
		uint8 payload[512] = { 0 }, uuid[16];
		uint8 info = fault < 5	 ? XLOG_CHECKPOINT_SHUTDOWN
					 : fault < 8 ? XLOG_PARAMETER_CHANGE
								 : 0xc0;
		uint32 length = native_control_payload(info, payload);
		RfPageOnlineRecordIdentityV1 identity;
		RfDetachedRecordPlanV1 record;
		if (fault == 0)
			length--;
		if (fault == 1)
			payload[offsetof(CheckPoint, fullPageWrites)] = 2;
		if (fault == 2) {
			TimeLineID tli = 9;
			memcpy(payload + offsetof(CheckPoint, ThisTimeLineID), &tli, sizeof(tli));
		}
		if (fault == 3) {
			XLogRecPtr redo = 101;
			memcpy(payload + offsetof(CheckPoint, redo), &redo, sizeof(redo));
		}
		if (fault == 5)
			payload[offsetof(xl_parameter_change, track_commit_timestamp)] = 2;
		if (fault == 6)
			memset(payload + offsetof(xl_parameter_change, MaxConnections), 0, sizeof(int));
		if (fault == 7)
			memset(payload + offsetof(xl_parameter_change, wal_level), 0xff, sizeof(int));
		memset(uuid, 0x44, sizeof(uuid));
		make_projection_record(&fake, RM_XLOG_ID, info, payload, length);
		identity = make_identity(&fake, uuid);
		fake.u.decoded.max_block_id = fault == 4 ? 0 : -1;
		record = make_projection_record_plan(&fake);
		UT_ASSERT_NE(rf_side_online_plan_feed_record_v1(plan, &record, &identity),
					 RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_SOURCE_GAP);
		rf_side_online_plan_destroy_v1(&plan);
	}
}

static uint32
standby_payload(uint8 info, char *payload)
{
	if (info == XLOG_STANDBY_LOCK) {
		xl_standby_locks *locks = (xl_standby_locks *)payload;
		locks->nlocks = 1;
		locks->locks[0] = (xl_standby_lock){ 42, 0, 1259 };
		return offsetof(xl_standby_locks, locks) + sizeof(xl_standby_lock);
	}
	if (info == XLOG_RUNNING_XACTS) {
		xl_running_xacts *running = (xl_running_xacts *)payload;
		running->xcnt = running->subxcnt = 1;
		running->nextXid = 5;
		running->oldestRunningXid = MaxTransactionId;
		running->latestCompletedXid = 3;
		running->xids[0] = MaxTransactionId;
		running->xids[1] = 4;
		return offsetof(xl_running_xacts, xids) + 2 * sizeof(TransactionId);
	}
	{
		xl_invalidations *invals = (xl_invalidations *)payload;
		invals->dbId = 1;
		invals->tsId = 1663;
		invals->relcacheInitFileInval = true;
		invals->nmsgs = 1;
		invals->msgs[0].rc.id = SHAREDINVALRELCACHE_ID;
		invals->msgs[0].rc.dbId = 0;
		invals->msgs[0].rc.relId = 0;
		return MinSizeOfInvalidations + sizeof(SharedInvalidationMessage);
	}
}

UT_TEST(test_standby_retains_exact_payload_and_independent_control_obligation)
{
	const uint8 infos[] = { XLOG_STANDBY_LOCK, XLOG_RUNNING_XACTS, XLOG_INVALIDATIONS };
	for (unsigned i = 0; i < lengthof(infos); i++) {
		RfSideOnlinePlanV1 *plan = space_online_plan(200);
		FakeXactRecord fake;
		PGAlignedBlock payload = { 0 };
		uint8 uuid[16];
		uint32 length = standby_payload(infos[i], payload.data);
		RfPageOnlineRecordIdentityV1 identity;
		RfDetachedRecordPlanV1 record;
		RfSideOnlineOperationV1 operation;
		RfSideOnlineApplyOpsV1 ops = { 0 };
		ApplyCapture capture = { 0 };

		memset(uuid, 0x44, sizeof(uuid));
		make_projection_record(&fake, RM_STANDBY_ID, infos[i], payload.data, length);
		identity = make_identity(&fake, uuid);
		fake.u.decoded.max_block_id = -1;
		record = make_projection_record_plan(&fake);
		UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &record, &identity),
					 RF_PAGE_PROOF_DETAIL_OK);
		memset(fake.data, 0xa5, length);
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(rf_side_online_plan_operation_count_v1(plan), 1);
		assert_contribution_owners(plan, 0, RF_SIDE_CONTRIBUTION_NATIVE_CONTROL, 0);
		if (rf_side_online_plan_operation_v1(plan, 0, &operation)) {
			UT_ASSERT_EQ(operation.kind, RF_SIDE_ONLINE_OPERATION_NATIVE_CONTROL);
			UT_ASSERT_EQ(operation.identity.record.rmid, RM_STANDBY_ID);
			UT_ASSERT_EQ(operation.owned_payload_length, length);
			UT_ASSERT_EQ(memcmp(operation.owned_payload, payload.data, length), 0);
		} else
			UT_ASSERT(false);
		ops.arg = &capture;
		ops.begin_protected_set = capture_begin;
		ops.end_protected_set = capture_end;
		ops.preflight_projection = accept_preflight;
		ops.apply_projection = capture_apply_projection;
		UT_ASSERT_EQ(rf_side_online_plan_apply_v1(plan, &ops),
					 RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT);
		UT_ASSERT_EQ(capture.begin_count + capture.projection_count, 0);
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_standby_empty_snapshot_and_initial_completed_xid_are_valid)
{
	RfSideOnlinePlanV1 *plan = space_online_plan(200);
	FakeXactRecord fake;
	xl_running_xacts running = { 0 };
	uint8 uuid[16];
	RfPageOnlineRecordIdentityV1 identity;
	RfDetachedRecordPlanV1 record;

	running.nextXid = running.oldestRunningXid = FirstNormalTransactionId;
	running.latestCompletedXid = FrozenTransactionId;
	memset(uuid, 0x44, sizeof(uuid));
	make_projection_record(&fake, RM_STANDBY_ID, XLOG_RUNNING_XACTS, &running,
						   offsetof(xl_running_xacts, xids));
	identity = make_identity(&fake, uuid);
	fake.u.decoded.max_block_id = -1;
	record = make_projection_record_plan(&fake);
	UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &record, &identity),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	rf_side_online_plan_destroy_v1(&plan);
}

UT_TEST(test_standby_invalid_counts_shapes_and_sources_never_seal)
{
	const uint8 infos[] = { XLOG_STANDBY_LOCK, XLOG_RUNNING_XACTS, XLOG_INVALIDATIONS };
	for (unsigned i = 0; i < lengthof(infos); i++)
		for (unsigned fault = 0; fault < 8; fault++) {
			RfSideOnlinePlanV1 *plan = space_online_plan(200);
			FakeXactRecord fake;
			PGAlignedBlock payload = { 0 };
			uint8 uuid[16];
			uint8 info = infos[i];
			uint32 length = standby_payload(info, payload.data);
			RfPageOnlineRecordIdentityV1 identity;
			RfDetachedRecordPlanV1 record;
			int count = fault == 2 ? -1 : INT_MAX;
			Size offset = i == 0   ? offsetof(xl_standby_locks, nlocks)
						  : i == 1 ? offsetof(xl_running_xacts, subxcnt)
								   : offsetof(xl_invalidations, nmsgs);

			if (fault == 0)
				length--;
			if (fault == 1)
				length++;
			if (fault == 2 || fault == 3)
				memcpy(payload.data + offset, &count, sizeof(count));
			if (fault == 4) {
				if (i == 0)
					((xl_standby_locks *)payload.data)->locks[0].xid = InvalidTransactionId;
				else
					payload.data[i == 1 ? offsetof(xl_running_xacts, subxid_overflow)
										: offsetof(xl_invalidations, relcacheInitFileInval)] = 2;
			}
			if (fault == 5)
				info |= XLR_SPECIAL_REL_UPDATE;
			if (fault == 7)
				info = 0xf0;
			memset(uuid, 0x44, sizeof(uuid));
			make_projection_record(&fake, RM_STANDBY_ID, info, payload.data, length);
			identity = make_identity(&fake, uuid);
			fake.u.decoded.max_block_id = fault == 6 ? 0 : -1;
			record = make_projection_record_plan(&fake);
			UT_ASSERT_NE(rf_side_online_plan_feed_record_v1(plan, &record, &identity),
						 RF_PAGE_PROOF_DETAIL_OK);
			UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_SOURCE_GAP);
			rf_side_online_plan_destroy_v1(&plan);
		}
}

static bool
census_space(void *arg, const RfSideSpaceContributionV1 *space)
{
	unsigned *calls = arg;
	(*calls)++;
	UT_ASSERT_EQ(space->page_mask, 2);
	UT_ASSERT_EQ(space->result.key.database_incarnation, 42);
	UT_ASSERT_EQ(space->result_token[1], 80);
	return true;
}

UT_TEST(test_stream_census_does_not_accumulate_a_million_side_operations)
{
	FakeXactRecord fake;
	RfPageOnlineRecordIdentityV1 identity;
	RfDetachedRecordPlanV1 record;
	RfContributorStreamCutV1 cut = { 0 };
	RfSideContributionOwnersV1 owners;
	uint8 uuid[16];
	unsigned calls = 0;
	memset(uuid, 0x44, sizeof(uuid));
	make_commit(&fake, 802, 999, 12345, false);
	identity = make_identity(&fake, uuid);
	record = make_record_plan(&fake);
	cut.failed_thread = 3;
	cut.timeline_id = 7;
	cut.origin_owner_incarnation = 9;
	cut.flags = RF_CONTRIBUTOR_CUT_COMPLETE;
	cut.scan_begin_inclusive = 100;
	cut.scan_end_exclusive = 100000100;
	for (uint32 i = 0; i < 1000000; i++) {
		set_identity_range(&fake, &identity, 100 + (uint64)i * 100, 200 + (uint64)i * 100);
		UT_ASSERT_EQ(
			rf_side_record_census_v1(&record, &identity, &cut, 42, census_space, &calls, &owners),
			RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(owners.owners,
					 RF_SIDE_CONTRIBUTION_UNDO_HEADER | RF_SIDE_CONTRIBUTION_TERMINAL);
		UT_ASSERT_EQ(owners.space_locator_count, 0);
	}
	UT_ASSERT_EQ(calls, 0);
	/* The census cannot make a malformed final record disappear. */
	fake.u.decoded.main_data_len--;
	UT_ASSERT_NE(
		rf_side_record_census_v1(&record, &identity, &cut, 42, census_space, &calls, &owners),
		RF_PAGE_PROOF_DETAIL_OK);
}

UT_TEST(test_stream_census_checks_native_and_space_payloads_without_replay)
{
	FakeXactRecord fake;
	RfPageOnlineRecordIdentityV1 identity;
	RfDetachedRecordPlanV1 record;
	RfContributorStreamCutV1 cut = { 0 };
	RfSideContributionOwnersV1 owners;
	ClusterSpaceReservationChange change = space_advance_fixture();
	PGAlignedBlock payload = { 0 };
	uint8 uuid[16];
	unsigned calls = 0;
	memset(uuid, 0x44, sizeof(uuid));
	cut.failed_thread = 3;
	cut.timeline_id = 7;
	cut.origin_owner_incarnation = 9;
	cut.flags = RF_CONTRIBUTOR_CUT_COMPLETE;
	cut.scan_begin_inclusive = 100;
	cut.scan_end_exclusive = 200;
	for (uint8 info = 0; info <= XLOG_INVALIDATIONS; info += 0x10) {
		uint32 length = standby_payload(info, payload.data);
		make_projection_record(&fake, RM_STANDBY_ID, info, payload.data, length);
		identity = make_identity(&fake, uuid);
		record = make_projection_record_plan(&fake);
		fake.u.decoded.max_block_id = -1;
		UT_ASSERT_EQ(
			rf_side_record_census_v1(&record, &identity, &cut, 42, census_space, &calls, &owners),
			RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(owners.owners, RF_SIDE_CONTRIBUTION_NATIVE_CONTROL);
		fake.u.decoded.main_data_len--;
		UT_ASSERT_NE(
			rf_side_record_census_v1(&record, &identity, &cut, 42, census_space, &calls, &owners),
			RF_PAGE_PROOF_DETAIL_OK);
	}
	UT_ASSERT(cluster_space_reservation_wal_encode(&change, payload.data,
												   CLUSTER_SPACE_RESERVATION_WAL_BYTES));
	make_projection_record(&fake, RM_SMGR_ID, XLOG_SMGR_SPACE_RESERVATION, payload.data,
						   CLUSTER_SPACE_RESERVATION_WAL_BYTES);
	identity = make_identity(&fake, uuid);
	record = make_projection_record_plan(&fake);
	fake.u.decoded.max_block_id = -1;
	UT_ASSERT_EQ(
		rf_side_record_census_v1(&record, &identity, &cut, 42, census_space, &calls, &owners),
		RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(calls, 1);
	UT_ASSERT_EQ(owners.space_locator_count, 1);
	UT_ASSERT_NE(
		rf_side_record_census_v1(&record, &identity, &cut, 43, census_space, &calls, &owners),
		RF_PAGE_PROOF_DETAIL_OK);
	cut.origin_owner_incarnation = 0;
	UT_ASSERT_NE(
		rf_side_record_census_v1(&record, &identity, &cut, 42, census_space, &calls, &owners),
		RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(calls, 1);
}

UT_TEST(test_space_retained_ancestry_covers_history_and_each_committed_drop)
{
	for (int history_only = 0; history_only < 2; history_only++) {
		ClusterSpaceReservationChange advance = space_advance_fixture();
		ClusterSpaceStructureChange drops[2]
			= { space_drop_fixture(16384), space_drop_fixture(16385) };
		XLogRecPtr redo = history_only ? 300 : 200;
		RfSideOnlinePlanV1 *plan = space_online_plan_redo(300, &redo);
		FakeXactRecord fake;
		uint8 wal[CLUSTER_SPACE_RESERVATION_WAL_BYTES];
		bool terminal = false;

		UT_ASSERT(cluster_space_reservation_wal_encode(&advance, wal, sizeof(wal)));
		UT_ASSERT_EQ(
			space_online_feed(plan, XLOG_SMGR_SPACE_RESERVATION, wal, sizeof(wal), 100, false),
			RF_PAGE_PROOF_DETAIL_OK);
		make_space_commit(&fake, drops, 2);
		UT_ASSERT_EQ(space_commit_feed(plan, &fake, 200), RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT(!rf_side_online_plan_space_covers_v1(plan, &advance.result.identity.key, 1, 0, 1,
													   &terminal));
		UT_ASSERT(rf_side_online_plan_bind_database_v1(plan, 42));
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT_EQ(rf_side_online_plan_space_target_count_v1(plan), history_only ? 0 : 2);
		UT_ASSERT(rf_side_online_plan_space_covers_v1(plan, &advance.result.identity.key, 1, 0, 1,
													  &terminal));
		UT_ASSERT(terminal);
		UT_ASSERT(rf_side_online_plan_space_covers_v1(plan, &advance.result.identity.key, 1, 0, 0,
													  &terminal));
		UT_ASSERT(!terminal);
		for (int block = 0; block < 2; block++) {
			UT_ASSERT(rf_side_online_plan_space_covers_v1(plan, &drops[1].identity.result.key,
														  block, 1, 1, &terminal));
			UT_ASSERT(terminal);
		}
		/* The other COMMIT locator has no contribution in operation zero. */
		UT_ASSERT(!rf_side_online_plan_space_covers_v1(plan, &drops[1].identity.result.key, 1, 0, 1,
													   &terminal));
		UT_ASSERT(!rf_side_online_plan_space_covers_v1(plan, &advance.result.identity.key, 0, 0, 1,
													   &terminal));
		UT_ASSERT(!rf_side_online_plan_space_covers_v1(plan, &advance.result.identity.key, 1, 1, 0,
													   &terminal));
		UT_ASSERT(!rf_side_online_plan_space_covers_v1(plan, &advance.result.identity.key, 2, 1, 1,
													   &terminal));
		UT_ASSERT(!rf_side_online_plan_space_covers_v1(plan, &advance.result.identity.key, 1, 0, 2,
													   &terminal));
		advance.result.identity.key.database_incarnation++;
		UT_ASSERT(!rf_side_online_plan_space_covers_v1(plan, &advance.result.identity.key, 1, 1, 1,
													   &terminal));
		UT_ASSERT(terminal); /* Every refusal preserves output. */
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_space_retained_ancestry_rejects_broken_history_before_live_suffix)
{
	for (int fault = 0; fault < 4; fault++) {
		ClusterSpaceReservationChange advance = space_advance_fixture();
		ClusterSpaceIdentityKey key = advance.result.identity.key;
		XLogRecPtr redo = 200;
		RfSideOnlinePlanV1 *plan = space_online_plan_redo(300, &redo);
		uint8 wal[CLUSTER_SPACE_RESERVATION_WAL_BYTES];
		bool terminal = true;
		RfSideSpaceTerminalV1 selected, untouched;
		memset(&untouched, 0xa5, sizeof(untouched));
		selected = untouched;
		UT_ASSERT(cluster_space_reservation_wal_encode(&advance, wal, sizeof(wal)));
		UT_ASSERT_EQ(
			space_online_feed(plan, XLOG_SMGR_SPACE_RESERVATION, wal, sizeof(wal), 100, false),
			RF_PAGE_PROOF_DETAIL_OK);
		advance.before = advance.result;
		advance.before_token = advance.result_token;
		if (fault == 0)
			advance.before_token++;
		if (fault == 1)
			advance.before.next_block--;
		if (fault == 2)
			advance.before.identity.incarnation[0]++;
		if (fault == 3) {
			/* Replay permits survivor gaps; PI ancestry must not infer them. */
			advance.before.next_block += 4;
			advance.before_token = 70;
		}
		advance.first_block = advance.before.next_block;
		advance.result = advance.before;
		advance.result.next_block += advance.granted;
		advance.result_token = 60;
		UT_ASSERT(cluster_space_reservation_wal_encode(&advance, wal, sizeof(wal)));
		UT_ASSERT_EQ(
			space_online_feed(plan, XLOG_SMGR_SPACE_RESERVATION, wal, sizeof(wal), 200, false),
			RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT(rf_side_online_plan_bind_database_v1(plan, 42));
		/* A valid standalone redo suffix must not bless its broken retained history. */
		UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
		UT_ASSERT(!rf_side_online_plan_space_covers_v1(plan, &key, 1, 0, 1, &terminal));
		UT_ASSERT(!rf_side_online_plan_space_covers_v1(plan, &key, 1, 1, 1, &terminal));
		UT_ASSERT(terminal);
		UT_ASSERT(!rf_side_online_plan_space_terminal_v1(plan, &key, 1, &selected));
		UT_ASSERT_EQ(memcmp(&selected, &untouched, sizeof(selected)), 0);
		rf_side_online_plan_destroy_v1(&plan);
	}
}

UT_TEST(test_space_retained_ancestry_orders_interleaved_writers_not_feed_or_token)
{
	RfContributorStreamCutV1 cuts[3] = { { 0 } };
	RfSideOnlinePlanRequestV1 request = { 0 };
	RfSideOnlinePlanV1 *plan = NULL;
	ClusterSpaceReservationChange chain[3];
	uint8 wal[CLUSTER_SPACE_RESERVATION_WAL_BYTES];
	const int feed_order[] = { 1, 2, 0 };
	bool terminal = false;
	RfSideSpaceTerminalV1 selected = { 0 };
	chain[0] = space_advance_fixture();
	for (int i = 0; i < 3; i++) {
		cuts[i].failed_thread = i + 1;
		cuts[i].origin_owner_incarnation = i + 11;
		cuts[i].timeline_id = 7;
		cuts[i].flags = RF_CONTRIBUTOR_CUT_COMPLETE;
		cuts[i].scan_begin_inclusive = 100;
		cuts[i].scan_end_exclusive = 200;
		if (i > 0) {
			chain[i] = chain[i - 1];
			chain[i].before = chain[i - 1].result;
			chain[i].before_token = chain[i - 1].result_token;
			chain[i].first_block = chain[i].before.next_block;
			chain[i].result = chain[i].before;
			chain[i].result.next_block += chain[i].granted;
			chain[i].result_token -= 20;
		}
	}
	request.system_identifier = chain[0].before.identity.key.system_identifier;
	memcpy(request.storage_uuid, chain[0].before.identity.key.storage_uuid, 16);
	request.physical_cuts = cuts;
	request.participant_count = 3;
	UT_ASSERT_EQ(rf_side_online_plan_create_v1(&request, &plan), RF_PAGE_PROOF_DETAIL_OK);
	for (int i = 0; i < 3; i++) {
		int origin = feed_order[i];
		FakeXactRecord fake;
		RfDetachedRecordPlanV1 record;
		RfPageOnlineRecordIdentityV1 identity;
		UT_ASSERT(cluster_space_reservation_wal_encode(&chain[origin], wal, sizeof(wal)));
		make_projection_record(&fake, RM_SMGR_ID,
							   XLOG_SMGR_SPACE_RESERVATION | XLR_SPECIAL_REL_UPDATE, wal,
							   sizeof(wal));
		fake.u.decoded.max_block_id = -1;
		identity = make_identity(&fake, request.storage_uuid);
		identity.participant_index = origin;
		identity.record.origin_thread = cuts[origin].failed_thread;
		set_identity_range(&fake, &identity, 100, 200);
		record = make_projection_record_plan(&fake);
		UT_ASSERT_EQ(rf_side_online_plan_feed_record_v1(plan, &record, &identity),
					 RF_PAGE_PROOF_DETAIL_OK);
	}
	UT_ASSERT(rf_side_online_plan_bind_database_v1(plan, 42));
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	/* Operation order is B,C,A; dependency order is A,B,C (tokens 80,60,40). */
	UT_ASSERT(rf_side_online_plan_space_terminal_v1(plan, &chain[0].before.identity.key, 1, &selected));
	UT_ASSERT_EQ(selected.operation, 1);
	UT_ASSERT_EQ(selected.contribution.result_token[1], 40);
	UT_ASSERT(!rf_side_online_plan_space_terminal_v1(plan, &chain[0].before.identity.key, 0, &selected));
	UT_ASSERT(rf_side_online_plan_space_covers_v1(plan, &chain[0].before.identity.key, 1, 2, 1,
												  &terminal));
	UT_ASSERT(terminal);
	UT_ASSERT(rf_side_online_plan_space_covers_v1(plan, &chain[0].before.identity.key, 1, 2, 0,
												  &terminal));
	UT_ASSERT(!terminal);
	UT_ASSERT(!rf_side_online_plan_space_covers_v1(plan, &chain[0].before.identity.key, 1, 1, 2,
												   &terminal));
	UT_ASSERT(!terminal);
	rf_side_online_plan_destroy_v1(&plan);
}

UT_TEST(test_space_retained_ancestry_does_not_retire_prior_truncate_incarnation)
{
	ClusterSpaceReservationChange advance = space_advance_fixture();
	ClusterSpaceStructureChange truncate = { 0 };
	RfSideOnlinePlanV1 *plan = space_online_plan(300);
	uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES];
	bool terminal = false;
	UT_ASSERT(
		cluster_space_reservation_wal_encode(&advance, wal, CLUSTER_SPACE_RESERVATION_WAL_BYTES));
	UT_ASSERT_EQ(space_online_feed(plan, XLOG_SMGR_SPACE_RESERVATION, wal,
								   CLUSTER_SPACE_RESERVATION_WAL_BYTES, 100, false),
				 RF_PAGE_PROOF_DETAIL_OK);
	truncate.identity.action = CLUSTER_SPACE_WAL_TRUNCATE;
	truncate.identity.nblocks = 2;
	truncate.identity.expected = advance.result.identity;
	truncate.identity.result = truncate.identity.expected;
	truncate.identity.result.incarnation[0]++;
	truncate.identity.result.sequence++;
	truncate.identity.result.operation++;
	truncate.identity.before_token = 123;
	truncate.identity.result_token = 60;
	truncate.reservation.action = CLUSTER_SPACE_RESERVATION_RESET;
	truncate.reservation.before = advance.result;
	truncate.reservation.result = truncate.reservation.before;
	truncate.reservation.result.identity = truncate.identity.result;
	truncate.reservation.result.next_block = 2;
	truncate.reservation.first_block = 2;
	truncate.reservation.before_token = advance.result_token;
	truncate.reservation.result_token = 60;
	UT_ASSERT(cluster_space_structure_wal_encode(&truncate, wal, sizeof(wal)));
	UT_ASSERT_EQ(space_online_feed(plan, XLOG_SMGR_SPACE_IDENTITY, wal, sizeof(wal), 200, false),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(rf_side_online_plan_bind_database_v1(plan, 42));
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(!rf_side_online_plan_space_covers_v1(plan, &truncate.identity.result.key, 1, 0, 1,
												   &terminal));
	UT_ASSERT(!terminal);
	for (int block = 0; block < 2; block++) {
		UT_ASSERT(rf_side_online_plan_space_covers_v1(plan, &truncate.identity.result.key, block, 1,
													  1, &terminal));
		UT_ASSERT(terminal);
	}
	rf_side_online_plan_destroy_v1(&plan);
}

UT_TEST(test_space_retained_ancestry_create_and_independent_page_terminal)
{
	ClusterSpaceReservationChange advance = space_advance_fixture();
	ClusterSpaceStructureChange create = { 0 };
	XLogRecPtr redo = 300;
	RfSideOnlinePlanV1 *plan = space_online_plan_redo(300, &redo);
	uint8 wal[CLUSTER_SPACE_STRUCTURE_WAL_BYTES];
	bool terminal = false;
	RfSideSpaceTerminalV1 selected = { 0 };
	create.identity.action = CLUSTER_SPACE_WAL_CREATE;
	create.identity.nblocks = InvalidBlockNumber;
	create.identity.result = advance.result.identity;
	create.identity.result_token = 50;
	create.reservation.action = CLUSTER_SPACE_RESERVATION_INIT;
	create.reservation.result.identity = create.identity.result;
	create.reservation.result_token = 50;
	UT_ASSERT(cluster_space_structure_wal_encode(&create, wal, sizeof(wal)));
	UT_ASSERT_EQ(space_online_feed(plan, XLOG_SMGR_SPACE_IDENTITY, wal, sizeof(wal), 100, false),
				 RF_PAGE_PROOF_DETAIL_OK);
	advance.before = create.reservation.result;
	advance.before_token = 50;
	advance.first_block = 0;
	advance.result = advance.before;
	advance.result.next_block = advance.granted;
	UT_ASSERT(
		cluster_space_reservation_wal_encode(&advance, wal, CLUSTER_SPACE_RESERVATION_WAL_BYTES));
	UT_ASSERT_EQ(space_online_feed(plan, XLOG_SMGR_SPACE_RESERVATION, wal,
								   CLUSTER_SPACE_RESERVATION_WAL_BYTES, 200, false),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(rf_side_online_plan_bind_database_v1(plan, 42));
	UT_ASSERT_EQ(rf_side_online_plan_seal_v1(plan), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(rf_side_online_plan_space_terminal_v1(plan, &create.identity.result.key, 0, &selected));
	UT_ASSERT_EQ(selected.operation, 0);
	UT_ASSERT_EQ(selected.contribution.result_token[0], 50);
	UT_ASSERT(rf_side_online_plan_space_terminal_v1(plan, &create.identity.result.key, 1, &selected));
	UT_ASSERT_EQ(selected.operation, 1);
	UT_ASSERT_EQ(selected.contribution.result_token[1], 80);
	UT_ASSERT(
		rf_side_online_plan_space_covers_v1(plan, &create.identity.result.key, 0, 0, 0, &terminal));
	UT_ASSERT(terminal); /* ADVANCE changes only block one. */
	UT_ASSERT(
		rf_side_online_plan_space_covers_v1(plan, &create.identity.result.key, 1, 0, 0, &terminal));
	UT_ASSERT(!terminal);
	UT_ASSERT(
		rf_side_online_plan_space_covers_v1(plan, &create.identity.result.key, 1, 0, 1, &terminal));
	UT_ASSERT(terminal);
	UT_ASSERT_EQ(rf_side_online_plan_space_target_count_v1(plan), 0);
	rf_side_online_plan_destroy_v1(&plan);
}

int
main(void)
{
	UT_PLAN(65);
	UT_RUN(test_space_retained_ancestry_create_and_independent_page_terminal);
	UT_RUN(test_space_retained_ancestry_covers_history_and_each_committed_drop);
	UT_RUN(test_space_retained_ancestry_rejects_broken_history_before_live_suffix);
	UT_RUN(test_space_retained_ancestry_orders_interleaved_writers_not_feed_or_token);
	UT_RUN(test_space_retained_ancestry_does_not_retire_prior_truncate_incarnation);
	UT_RUN(test_stream_census_does_not_accumulate_a_million_side_operations);
	UT_RUN(test_stream_census_checks_native_and_space_payloads_without_replay);
	UT_RUN(test_standby_retains_exact_payload_and_independent_control_obligation);
	UT_RUN(test_standby_empty_snapshot_and_initial_completed_xid_are_valid);
	UT_RUN(test_standby_invalid_counts_shapes_and_sources_never_seal);
	UT_RUN(test_space_contribution_census_includes_history_and_every_drop_page);
	UT_RUN(test_native_control_is_owned_input_and_not_replay_permission);
	UT_RUN(test_native_control_rejects_bad_shape_and_identity);
	UT_RUN(test_retained_generations_refuse_ambiguous_thread_selectors);
	UT_RUN(test_side_plan_does_not_drop_unowned_page_components);
	UT_RUN(test_retained_side_history_is_validated_but_never_dispatched);
	UT_RUN(test_retained_commit_drop_does_not_create_space_mutation_target);
	UT_RUN(test_multi_origin_side_selection_keeps_exact_source_and_original_plan);
	UT_RUN(test_reuse_owner_covers_retired_commit_only_after_new_physical_and_tt);
	UT_RUN(test_init_owner_repairs_short_segment_before_header_and_commit);
	UT_RUN(test_lifecycle_header_requires_known_generation_and_complete_source_chain);
	UT_RUN(test_recycle_window_repeated_terminal_and_init_unknown_generation);
	UT_RUN(test_physical_only_cut_and_reuse_anchor_boundary);
	UT_RUN(test_physical_undo_is_durable_before_canonical_tt_publication);
	UT_RUN(test_real_plan_owner_and_xact_install_final_header_before_older_commit_projection);
	UT_RUN(test_covered_commit_requires_owner_proof_before_projection_without_tt_rewind);
	UT_RUN(test_undo_header_repeated_slot_exact_source_membership_without_init);
	UT_RUN(test_undo_header_later_disk_slot_does_not_hide_conflicting_source);
	UT_RUN(test_sealed_source_match_requires_observed_namespace_and_exact_cut);
	UT_RUN(test_folded_commit_segment_must_belong_to_source_origin);
	UT_RUN(test_online_undo_header_evolves_init_bind_and_folded_commit);
	UT_RUN(test_online_undo_header_private_base_and_late_failure);
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
	UT_RUN(test_protected_set_released_on_preflight_or_apply_error);
	UT_RUN(test_space_plan_owns_exact_chain_and_requires_observed_namespace);
	UT_RUN(test_space_plan_rejects_unbound_native_shapes);
	UT_RUN(test_space_plan_structural_pair_and_standalone_tombstone);
	UT_RUN(test_space_seal_rejects_incomplete_source_chains_before_target_io);
	UT_RUN(test_space_seal_enumerates_every_unique_commit_and_smgr_target);
	UT_RUN(test_space_commit_owned_multiple_targets_close_exact_source_chains);
	UT_RUN(test_space_commit_rejects_unowned_duplicate_or_malformed_drop_inputs);
	UT_RUN(test_space_commit_retains_all_native_side_effects_and_refuses_tt_only_owner);
	UT_RUN(test_multixact_retired_page_requires_exact_later_sealed_truncate);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
