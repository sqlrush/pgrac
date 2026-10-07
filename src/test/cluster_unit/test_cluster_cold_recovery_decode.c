/*-------------------------------------------------------------------------
 *
 * test_cluster_cold_recovery_decode.c
 *	  Mapping of decoded native records to typed cold replay plan input.
 *
 *	  The closed route registry preflight and transaction record parsing are
 *	  fixtures here; this binary checks the cold owner policy, component
 *	  mapping, lifecycle classification and the typed SPACE effects decoded
 *	  with the SPACE owner's real WAL codec.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_cold_recovery_decode.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-s9p2-05-instance-and-cluster-recovery.md
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include "access/xact.h"
#include "access/xlog_internal.h"
#include "access/xlogreader.h"
#include "catalog/pg_control.h"
#include "catalog/storage_xlog.h"
#include "access/heapam_xlog.h"
#include "catalog/pg_tablespace_d.h"
#include "cluster/cluster_cold_recovery.h"
#include "cluster/cluster_page_detached.h"
#include "cluster/cluster_side_xact.h"
#include "cluster/cluster_space_reservation.h"
#include "replication/message.h"
#include "storage/standbydefs.h"

#include "unit_test.h"

UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *condition_name, const char *file_name, int line_number)
{
	printf("# unexpected Assert: %s at %s:%d\n", condition_name, file_name, line_number);
	abort();
}

static RfPageProofDetailV1 preflight_result;
static RfDetachedRecordPlanV1 preflight_plan;
static RfDetachedOwnerOpsV1 seen_ops;
static int commit_nrels;
static bool commit_parse_ok = true;
static int abort_nrels;
static uint32 commit_nspace_drops;
static const char *commit_space_drops;

RfPageProofDetailV1
rf_page_detached_preflight_v1(XLogReaderState *record, bool space_active,
							  const RfDetachedOwnerOpsV1 *owner_ops, RfDetachedRecordPlanV1 *plan)
{
	(void)space_active;
	seen_ops = *owner_ops;
	if (preflight_result != RF_PAGE_PROOF_DETAIL_OK)
		return preflight_result;
	*plan = preflight_plan;
	plan->source_record = record;
	return RF_PAGE_PROOF_DETAIL_OK;
}

bool
ParseCommitRecord(uint8 info, xl_xact_commit *xlrec, Size len, xl_xact_parsed_commit *parsed)
{
	(void)info;
	(void)xlrec;
	(void)len;
	if (!commit_parse_ok)
		return false;
	memset(parsed, 0, sizeof(*parsed));
	parsed->nrels = commit_nrels;
	parsed->nspace_drops = commit_nspace_drops;
	parsed->space_drops = commit_space_drops;
	return true;
}

/* The bounded completion check itself is exercised with real records in
 * test_cluster_wal_retained_cut_records; here only its verdict is used. */
static bool abort_shape_ok;

bool
rf_side_xact_completion_shape_v1(XLogReaderState *record, bool commit)
{
	UT_ASSERT(record != NULL && !commit);
	return abort_shape_ok;
}

void
ParseAbortRecord(uint8 info, xl_xact_abort *xlrec, xl_xact_parsed_abort *parsed)
{
	(void)info;
	(void)xlrec;
	memset(parsed, 0, sizeof(*parsed));
	parsed->nrels = abort_nrels;
}

typedef struct FakeRecord {
	XLogReaderState reader;
	char data[2 * CLUSTER_SPACE_STRUCTURE_WAL_BYTES];
	union {
		DecodedXLogRecord decoded;
		/* Reserves the trailing block array addressed through decoded. */
		/* cppcheck-suppress unusedStructMember */
		char padding[sizeof(DecodedXLogRecord) + 4 * sizeof(DecodedBkpBlock)];
	} storage;
} FakeRecord;

static const uint8 UUID[16] = { 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3 };

static void
fake_record(FakeRecord *record, uint8 rmid, uint8 info, int blocks)
{
	int i;

	memset(record, 0, sizeof(*record));
	record->reader.ReadRecPtr = 0x1000;
	record->reader.EndRecPtr = 0x1080;
	record->reader.record = &record->storage.decoded;
	record->storage.decoded.header.xl_rmid = rmid;
	record->storage.decoded.header.xl_info = info;
	record->storage.decoded.header.xl_crc = 0xabcd;
	record->storage.decoded.header.xl_scn = 77;
	record->storage.decoded.header.xl_prev = 0xf80;
	record->storage.decoded.main_data = record->data;
	record->storage.decoded.main_data_len = sizeof(record->data);
	record->storage.decoded.max_block_id = blocks - 1;
	for (i = 0; i < blocks; i++) {
		DecodedBkpBlock *block = &record->storage.decoded.blocks[i];

		block->in_use = true;
		block->rlocator.spcOid = 1663;
		block->rlocator.dbOid = 5;
		block->rlocator.relNumber = 100 + i;
		block->forknum = i == 2 ? FSM_FORKNUM : MAIN_FORKNUM;
		block->blkno = 10 + i;
	}
	memset(&preflight_plan, 0, sizeof(preflight_plan));
	preflight_result = RF_PAGE_PROOF_DETAIL_OK;
	preflight_plan.route.rmid = rmid;
	preflight_plan.route.record_owner
		= blocks > 0 ? RF_ROUTE_OWNER_PAGE_CODEC : RF_ROUTE_OWNER_SIDE_TYPED;
	preflight_plan.preflight_complete = true;
	commit_nrels = 0;
	abort_nrels = 0;
	abort_shape_ok = true;
	commit_parse_ok = true;
	commit_nspace_drops = 0;
	commit_space_drops = NULL;
}

static void
plan_component(uint32 index, uint8 owner, uint8 page_class, uint64 before, uint64 result,
			   uint16 flags)
{
	RfDetachedComponentPlanV1 *component = &preflight_plan.components[index];

	component->block_id = (uint8)index;
	component->component_ordinal = (uint16)index;
	component->owner = owner;
	component->page_class = page_class;
	component->edge_flags = flags;
	component->before_kind = RF_PAGE_STATE_PRESENT;
	component->result_kind = RF_PAGE_STATE_PRESENT;
	memset(component->before.segment_incarnation, 7, 16);
	component->before.mutation_token = before;
	memset(component->result.segment_incarnation, 7, 16);
	component->result.mutation_token = result;
	preflight_plan.component_count = index + 1;
	preflight_plan.result_token = result;
}

UT_TEST(test_page_record_maps_ordinary_components)
{
	FakeRecord record;
	ClusterColdDecodedV1 out = { 0 };

	fake_record(&record, RM_HEAP_ID, 0x20, 3);
	plan_component(0, RF_DETACHED_COMPONENT_PAGE_CODEC, RF_PAGE_CLASS_ORDINARY, 4, 9,
				   RF_PAGE_EDGE_FULL_IMAGE_APPLY | RF_PAGE_EDGE_FULL_COVERAGE);
	plan_component(1, RF_DETACHED_COMPONENT_PAGE_CODEC, RF_PAGE_CLASS_ORDINARY, 5, 9, 0);
	plan_component(2, RF_DETACHED_COMPONENT_REBUILDABLE, RF_PAGE_CLASS_REBUILDABLE_FSM, 0, 9, 0);
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&record.reader, 99, UUID, true, false, &out),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(out.record.read_rec_ptr, 0x1000);
	UT_ASSERT_EQ(out.record.end_rec_ptr, 0x1080);
	UT_ASSERT_EQ(out.record.scn, 77);
	UT_ASSERT_EQ(out.record.prev_rec_ptr, 0xf80);
	UT_ASSERT_EQ(out.record.record_crc, 0xabcd);
	UT_ASSERT_EQ(out.record.rmid, RM_HEAP_ID);
	UT_ASSERT_EQ(out.record.info, 0x20);
	UT_ASSERT_EQ(out.record.record_flags, 0);
	UT_ASSERT_EQ(out.record.component_count, 2);
	UT_ASSERT(out.record.components == out.components);
	UT_ASSERT_EQ(out.route_owner, RF_ROUTE_OWNER_PAGE_CODEC);
	UT_ASSERT_EQ(out.components[0].page.system_identifier, 99);
	UT_ASSERT(memcmp(out.components[0].page.storage_uuid, UUID, 16) == 0);
	UT_ASSERT_EQ(out.components[0].page.locator.relNumber, 100);
	UT_ASSERT_EQ(out.components[0].page.blockno, 10);
	UT_ASSERT_EQ(out.components[0].page.forknum, MAIN_FORKNUM);
	UT_ASSERT_EQ(out.components[0].edge_flags,
				 RF_PAGE_EDGE_FULL_IMAGE_APPLY | RF_PAGE_EDGE_FULL_COVERAGE);
	UT_ASSERT_EQ(out.components[0].before.mutation_token, 4);
	UT_ASSERT_EQ(out.components[1].block_id, 1);
	UT_ASSERT_EQ(out.components[1].page.locator.relNumber, 101);
	UT_ASSERT_EQ(out.components[1].result.mutation_token, 9);
	UT_ASSERT_EQ(out.components[1].page_class, RF_PAGE_CLASS_ORDINARY);
}

UT_TEST(test_registry_refusal_is_opcode_unsupported)
{
	FakeRecord record;
	ClusterColdDecodedV1 out = { 0 };

	fake_record(&record, RM_HEAP_ID, 0x20, 1);
	preflight_result = RF_PAGE_PROOF_DETAIL_OPCODE_UNSUPPORTED;
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&record.reader, 99, UUID, true, false, &out),
				 CLUSTER_COLD_OPCODE_UNSUPPORTED);
	UT_ASSERT_EQ(out.route_detail, RF_PAGE_PROOF_DETAIL_OPCODE_UNSUPPORTED);
	preflight_result = RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&record.reader, 99, UUID, true, false, &out),
				 CLUSTER_COLD_OPCODE_UNSUPPORTED);
	UT_ASSERT_EQ(out.route_detail, RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE);
}

/* Cold owner policy handed to the registry preflight. */
UT_TEST(test_cold_owner_policy)
{
	FakeRecord record;
	ClusterColdDecodedV1 out = { 0 };
	RfOpcodeRouteV1 side = { 0 };
	RfOpcodeRouteV1 noop = { 0 };
	RfOpcodeRouteV1 page = { 0 };
	RfPageVersionEdgeEntryV1 edge = { 0 };
	DecodedBkpBlock block = { 0 };

	fake_record(&record, RM_XACT_ID, XLOG_XACT_COMMIT, 0);
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&record.reader, 99, UUID, true, false, &out),
				 CLUSTER_COLD_OK);
	side.record_owner = RF_ROUTE_OWNER_SIDE_TYPED;
	noop.record_owner = RF_ROUTE_OWNER_LOGICAL_NOOP;
	page.record_owner = RF_ROUTE_OWNER_PAGE_CODEC;
	UT_ASSERT_EQ(seen_ops.preflight_side_record(seen_ops.arg, &side, NULL, NULL),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(seen_ops.preflight_side_record(seen_ops.arg, &noop, NULL, NULL),
				 RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT(seen_ops.preflight_side_record(seen_ops.arg, &page, NULL, NULL)
			  != RF_PAGE_PROOF_DETAIL_OK);
	edge.page_class = RF_PAGE_CLASS_ROUTED_SPACE;
	UT_ASSERT(seen_ops.preflight_side_component(seen_ops.arg, &page, &edge, &block)
			  != RF_PAGE_PROOF_DETAIL_OK);
	edge.page_class = RF_PAGE_CLASS_REBUILDABLE_FSM;
	UT_ASSERT_EQ(seen_ops.preflight_rebuildable_component(seen_ops.arg, &page, &edge, &block),
				 RF_PAGE_PROOF_DETAIL_OK);
	edge.page_class = RF_PAGE_CLASS_ORDINARY;
	UT_ASSERT(seen_ops.preflight_rebuildable_component(seen_ops.arg, &page, &edge, &block)
			  != RF_PAGE_PROOF_DETAIL_OK);
}

UT_TEST(test_transaction_lifecycle_classification)
{
	FakeRecord record;
	ClusterColdDecodedV1 out = { 0 };
	static const uint8 prepared[3]
		= { XLOG_XACT_PREPARE, XLOG_XACT_COMMIT_PREPARED, XLOG_XACT_ABORT_PREPARED };
	int i;

	fake_record(&record, RM_XACT_ID, XLOG_XACT_COMMIT, 0);
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&record.reader, 99, UUID, true, false, &out),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(out.record.record_flags, 0);
	/* the native commit redo deletes the files; SPACE drops are decoded below */
	commit_nrels = 1;
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&record.reader, 99, UUID, true, false, &out),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(out.record.record_flags, 0);
	commit_parse_ok = false;
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&record.reader, 99, UUID, true, false, &out),
				 CLUSTER_COLD_COMPONENT_INVALID);

	fake_record(&record, RM_XACT_ID, XLOG_XACT_ABORT, 0);
	abort_nrels = 2;
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&record.reader, 99, UUID, true, false, &out),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(out.record.record_flags, CLUSTER_COLD_RECORD_STRUCTURAL);
	/* An ABORT whose sections do not lie within its data is damaged. */
	abort_shape_ok = false;
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&record.reader, 99, UUID, true, false, &out),
				 CLUSTER_COLD_COMPONENT_INVALID);

	for (i = 0; i < 3; i++) {
		fake_record(&record, RM_XACT_ID, prepared[i], 0);
		UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&record.reader, 99, UUID, true, false, &out),
					 CLUSTER_COLD_OK);
		UT_ASSERT_EQ(out.record.record_flags, CLUSTER_COLD_RECORD_UNSUPPORTED);
	}
}

UT_TEST(test_storage_lifecycle_classification)
{
	FakeRecord record;
	ClusterColdDecodedV1 out = { 0 };
	static const struct {
		uint8 rmid;
		uint8 info;
		uint8 flags;
	} cases[] = { { RM_SMGR_ID, XLOG_SMGR_CREATE, 0 },
				  { RM_SMGR_ID, XLOG_SMGR_TRUNCATE, CLUSTER_COLD_RECORD_STRUCTURAL },
				  { RM_DBASE_ID, 0x00, CLUSTER_COLD_RECORD_STRUCTURAL },
				  { RM_TBLSPC_ID, 0x00, CLUSTER_COLD_RECORD_STRUCTURAL },
				  { RM_RELMAP_ID, 0x00, CLUSTER_COLD_RECORD_STRUCTURAL },
				  { RM_REPLORIGIN_ID, 0x00, CLUSTER_COLD_RECORD_STRUCTURAL },
				  { RM_HEAP2_ID, XLOG_HEAP2_REWRITE, CLUSTER_COLD_RECORD_STRUCTURAL },
				  { RM_CLUSTER_XID_STRIPE_ID, 0x00, CLUSTER_COLD_RECORD_STRUCTURAL },
				  { RM_CLUSTER_XID_STRIPE_ID, 0x10, CLUSTER_COLD_RECORD_STRUCTURAL },
				  { RM_XLOG_ID, 0x30, 0 },
				  { RM_STANDBY_ID, 0x10, 0 } };
	Size i;

	for (i = 0; i < lengthof(cases); i++) {
		fake_record(&record, cases[i].rmid, cases[i].info, 0);
		UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&record.reader, 99, UUID, true, false, &out),
					 CLUSTER_COLD_OK);
		if (out.record.record_flags != cases[i].flags)
			printf("# case %zu rmid %u info 0x%02x flags %u\n", i, cases[i].rmid, cases[i].info,
				   out.record.record_flags);
		UT_ASSERT_EQ(out.record.record_flags, cases[i].flags);
	}
}

UT_TEST(test_malformed_plan_refused)
{
	FakeRecord record;
	ClusterColdDecodedV1 out = { 0 };

	fake_record(&record, RM_HEAP_ID, 0x20, 1);
	plan_component(0, RF_DETACHED_COMPONENT_PAGE_CODEC, RF_PAGE_CLASS_ORDINARY, 4, 9, 0);
	preflight_plan.components[0].block_id = 3;
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&record.reader, 99, UUID, true, false, &out),
				 CLUSTER_COLD_COMPONENT_INVALID);
	fake_record(&record, RM_HEAP_ID, 0x20, 1);
	plan_component(0, RF_DETACHED_COMPONENT_SIDE_TYPED, RF_PAGE_CLASS_ROUTED_SIDE, 0, 9, 0);
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&record.reader, 99, UUID, true, false, &out),
				 CLUSTER_COLD_OPCODE_UNSUPPORTED);
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(NULL, 99, UUID, true, false, &out),
				 CLUSTER_COLD_INVALID_ARGUMENT);
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&record.reader, 99, UUID, true, false, NULL),
				 CLUSTER_COLD_INVALID_ARGUMENT);
}

/* A foreign side record has no cold owner yet; it is flagged so the plan
 * refuses it after the native redo start.  The founder's own records keep
 * their native owner. */
UT_TEST(test_foreign_side_records_have_no_cold_owner)
{
	FakeRecord record;
	ClusterColdDecodedV1 out = { 0 };

	fake_record(&record, RM_XLOG_ID, XLOG_CHECKPOINT_ONLINE, 0);
	record.storage.decoded.main_data_len = sizeof(CheckPoint) + 1; /* not a checkpoint */
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&record.reader, 99, UUID, true, false, &out),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(out.record.record_flags, 0);
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&record.reader, 99, UUID, true, true, &out),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(out.record.record_flags, CLUSTER_COLD_RECORD_SIDE_UNOWNED);

	fake_record(&record, RM_XACT_ID, XLOG_XACT_COMMIT, 0);
	commit_nrels = 1;
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&record.reader, 99, UUID, true, true, &out),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(out.record.record_flags, CLUSTER_COLD_RECORD_SIDE_UNOWNED);

	/* An XLOG record outside the native control set (or with blocks) is no
	 * control record. */
	fake_record(&record, RM_XLOG_ID, 0xE0, 0);
	record.storage.decoded.main_data_len = 0;
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&record.reader, 99, UUID, true, true, &out),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(out.record.record_flags, CLUSTER_COLD_RECORD_SIDE_UNOWNED);
	fake_record(&record, RM_STANDBY_ID, 0x30, 0);
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&record.reader, 99, UUID, true, true, &out),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(out.record.record_flags, CLUSTER_COLD_RECORD_SIDE_UNOWNED);
	fake_record(&record, RM_XLOG_ID, XLOG_NOOP, 0);
	record.storage.decoded.header.xl_info |= XLR_SPECIAL_REL_UPDATE;
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&record.reader, 99, UUID, true, true, &out),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(out.record.record_flags, CLUSTER_COLD_RECORD_SIDE_UNOWNED);
	/* a side record routed with block references, or not routed as a side
	 * record at all, is no control record */
	fake_record(&record, RM_XLOG_ID, XLOG_NOOP, 1);
	preflight_plan.route.record_owner = RF_ROUTE_OWNER_SIDE_TYPED;
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&record.reader, 99, UUID, true, true, &out),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(out.record.record_flags, CLUSTER_COLD_RECORD_SIDE_UNOWNED);
	fake_record(&record, RM_XLOG_ID, XLOG_NOOP, 0);
	preflight_plan.route.record_owner = 0;
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&record.reader, 99, UUID, true, true, &out),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(out.record.record_flags, CLUSTER_COLD_RECORD_SIDE_UNOWNED);
	/* an outcome record whose info value matches a control record's */
	fake_record(&record, RM_XACT_ID, XLOG_XACT_ABORT, 0);
	UT_ASSERT_EQ(XLOG_XACT_ABORT, XLOG_NOOP);
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&record.reader, 99, UUID, true, true, &out),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(out.record.record_flags, CLUSTER_COLD_RECORD_SIDE_UNOWNED);

	/* Creating a relation file is idempotent: its native redo is the owner. */
	fake_record(&record, RM_SMGR_ID, XLOG_SMGR_CREATE, 0);
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&record.reader, 99, UUID, true, true, &out),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(out.record.record_flags, 0);

	fake_record(&record, RM_HEAP_ID, 0x20, 1);
	plan_component(0, RF_DETACHED_COMPONENT_PAGE_CODEC, RF_PAGE_CLASS_ORDINARY, 4, 9, 0);
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&record.reader, 99, UUID, true, true, &out),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(out.record.record_flags, 0);
	UT_ASSERT_EQ(out.record.component_count, 1);
}

/*
 * Another generation's native control records -- checkpoints, NEXTOID,
 * parameter, FPW and timeline records, segment switches, restore points,
 * standby records and logical messages -- do on the founder exactly what
 * crash recovery does with them: nothing.  Only their native shape is
 * accepted; the founder's own keep their native owner.
 */
UT_TEST(test_foreign_native_control_is_typed_noop)
{
	FakeRecord record;
	ClusterColdDecodedV1 out = { 0 };
	static const struct {
		uint8 rmid;
		uint8 info;
		uint32 length;
	} cases[] = { { RM_XLOG_ID, XLOG_CHECKPOINT_SHUTDOWN, sizeof(CheckPoint) },
				  { RM_XLOG_ID, XLOG_CHECKPOINT_ONLINE, sizeof(CheckPoint) },
				  { RM_XLOG_ID, XLOG_NOOP, 0 },
				  { RM_XLOG_ID, XLOG_NEXTOID, sizeof(Oid) },
				  { RM_XLOG_ID, XLOG_SWITCH, 0 },
				  { RM_XLOG_ID, XLOG_BACKUP_END, sizeof(XLogRecPtr) },
				  { RM_XLOG_ID, XLOG_PARAMETER_CHANGE, sizeof(xl_parameter_change) },
				  { RM_XLOG_ID, XLOG_RESTORE_POINT, sizeof(xl_restore_point) },
				  { RM_XLOG_ID, XLOG_FPW_CHANGE, sizeof(bool) },
				  { RM_XLOG_ID, XLOG_END_OF_RECOVERY, sizeof(xl_end_of_recovery) },
				  { RM_XLOG_ID, XLOG_OVERWRITE_CONTRECORD, sizeof(xl_overwrite_contrecord) },
				  { RM_STANDBY_ID, XLOG_STANDBY_LOCK, 0 },
				  { RM_STANDBY_ID, XLOG_RUNNING_XACTS, 0 },
				  { RM_STANDBY_ID, XLOG_INVALIDATIONS, 0 } };
	Size i;

	for (i = 0; i < lengthof(cases); i++) {
		fake_record(&record, cases[i].rmid, cases[i].info, 0);
		record.storage.decoded.main_data_len = cases[i].length;
		UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&record.reader, 99, UUID, true, true, &out),
					 CLUSTER_COLD_OK);
		if (out.record.record_flags != CLUSTER_COLD_RECORD_FOREIGN_CONTROL)
			printf("# case %zu rmid %u info 0x%02x flags %u\n", i, cases[i].rmid, cases[i].info,
				   out.record.record_flags);
		UT_ASSERT_EQ(out.record.record_flags, CLUSTER_COLD_RECORD_FOREIGN_CONTROL);
		UT_ASSERT_EQ(out.record.component_count, 0);
		UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&record.reader, 99, UUID, true, false, &out),
					 CLUSTER_COLD_OK);
		UT_ASSERT_EQ(out.record.record_flags, 0);
		/* a wrong payload length is no such record */
		if (cases[i].rmid == RM_XLOG_ID && cases[i].info != XLOG_NOOP) {
			record.storage.decoded.main_data_len = cases[i].length + 1;
			UT_ASSERT_EQ(
				cluster_cold_recovery_decode_v1(&record.reader, 99, UUID, true, true, &out),
				CLUSTER_COLD_OK);
			UT_ASSERT_EQ(out.record.record_flags, CLUSTER_COLD_RECORD_SIDE_UNOWNED);
		}
	}

	fake_record(&record, RM_LOGICALMSG_ID, XLOG_LOGICAL_MESSAGE, 0);
	preflight_plan.route.record_owner = RF_ROUTE_OWNER_LOGICAL_NOOP;
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&record.reader, 99, UUID, true, true, &out),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(out.record.record_flags, CLUSTER_COLD_RECORD_FOREIGN_CONTROL);
	/* the logical no-op route is only a logical message */
	fake_record(&record, RM_XLOG_ID, XLOG_NOOP, 0);
	preflight_plan.route.record_owner = RF_ROUTE_OWNER_LOGICAL_NOOP;
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&record.reader, 99, UUID, true, true, &out),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(out.record.record_flags, CLUSTER_COLD_RECORD_SIDE_UNOWNED);
}

/* SPACE payloads built with the SPACE owner's own WAL codec. */
static ClusterSpaceReservation
space_state(uint8 incarnation)
{
	ClusterSpaceReservation state;

	memset(&state, 0, sizeof(state));
	state.identity.key.system_identifier = 99;
	state.identity.key.database_incarnation = 4;
	memcpy(state.identity.key.storage_uuid, UUID, 16);
	state.identity.key.locator.spcOid = DEFAULTTABLESPACE_OID;
	state.identity.key.locator.dbOid = 5;
	state.identity.key.locator.relNumber = 16384;
	memset(state.identity.incarnation, incarnation, 16);
	state.identity.sequence = 1;
	state.identity.operation = 7;
	state.identity.state = CLUSTER_SPACE_IDENTITY_LIVE;
	return state;
}

static ClusterSpaceStructureChange
structure_change(ClusterSpaceWalAction action)
{
	ClusterSpaceStructureChange c;

	memset(&c, 0, sizeof(c));
	c.identity.action = action;
	c.identity.result = space_state(0x31).identity;
	c.identity.result_token = 211;
	c.reservation.result_token = 211;
	c.reservation.result.identity = c.identity.result;
	if (action == CLUSTER_SPACE_WAL_CREATE) {
		c.identity.nblocks = InvalidBlockNumber;
		c.reservation.action = CLUSTER_SPACE_RESERVATION_INIT;
		return c;
	}
	c.identity.expected = c.identity.result;
	c.identity.before_token = 17;
	c.reservation.before.identity = c.identity.expected;
	c.reservation.before.next_block = 10;
	c.reservation.before_token = 99;
	c.identity.result.sequence++;
	c.identity.result.operation++;
	if (action == CLUSTER_SPACE_WAL_TRUNCATE) {
		c.identity.nblocks = 4;
		c.identity.result.incarnation[0]++;
		c.reservation.action = CLUSTER_SPACE_RESERVATION_RESET;
		c.reservation.first_block = c.reservation.result.next_block = 4;
	} else {
		c.identity.nblocks = InvalidBlockNumber;
		c.identity.result.state = CLUSTER_SPACE_IDENTITY_TOMBSTONED;
		c.reservation.action = CLUSTER_SPACE_RESERVATION_TOMBSTONE;
		c.reservation.result.next_block = 10;
	}
	c.reservation.result.identity = c.identity.result;
	return c;
}

static void
space_record(FakeRecord *record, uint8 info, const ClusterSpaceStructureChange *change)
{
	fake_record(record, RM_SMGR_ID, info, 0);
	if (info == XLOG_SMGR_SPACE_IDENTITY) {
		UT_ASSERT(cluster_space_structure_wal_encode(change, record->data,
													 CLUSTER_SPACE_STRUCTURE_WAL_BYTES));
		record->storage.decoded.main_data_len = CLUSTER_SPACE_STRUCTURE_WAL_BYTES;
	} else {
		UT_ASSERT(cluster_space_reservation_wal_encode(&change->reservation, record->data,
													   CLUSTER_SPACE_RESERVATION_WAL_BYTES));
		record->storage.decoded.main_data_len = CLUSTER_SPACE_RESERVATION_WAL_BYTES;
	}
}

static bool
incarnation_is(const uint8 *incarnation, uint8 first, uint8 rest)
{
	int i;

	if (incarnation[0] != first)
		return false;
	for (i = 1; i < 16; i++)
		if (incarnation[i] != rest)
			return false;
	return true;
}

UT_TEST(test_space_identity_changes_are_typed)
{
	FakeRecord record;
	ClusterColdDecodedV1 out = { 0 };
	ClusterSpaceStructureChange change = structure_change(CLUSTER_SPACE_WAL_CREATE);

	space_record(&record, XLOG_SMGR_SPACE_IDENTITY, &change);
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&record.reader, 99, UUID, true, false, &out),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(out.record.record_flags, 0);
	UT_ASSERT_EQ(out.record.space_count, 1);
	UT_ASSERT(out.record.space_ops == out.space_ops);
	UT_ASSERT_EQ(out.space_ops[0].kind, CLUSTER_COLD_SPACE_CREATE);
	UT_ASSERT_EQ(out.space_ops[0].locator.relNumber, 16384);
	UT_ASSERT(incarnation_is(out.space_ops[0].result_incarnation, 0x31, 0x31));
	UT_ASSERT(incarnation_is(out.space_ops[0].before_incarnation, 0, 0));
	UT_ASSERT(out.space_ops[0].payload == record.data);
	UT_ASSERT_EQ(out.space_ops[0].payload_length, CLUSTER_SPACE_STRUCTURE_WAL_BYTES);

	change = structure_change(CLUSTER_SPACE_WAL_TRUNCATE);
	space_record(&record, XLOG_SMGR_SPACE_IDENTITY, &change);
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&record.reader, 99, UUID, true, true, &out),
				 CLUSTER_COLD_OK);
	/* another generation's SPACE change has the SPACE owner */
	UT_ASSERT_EQ(out.record.record_flags, 0);
	UT_ASSERT_EQ(out.space_ops[0].kind, CLUSTER_COLD_SPACE_TRUNCATE);
	UT_ASSERT_EQ(out.space_ops[0].nblocks, 4);
	UT_ASSERT(incarnation_is(out.space_ops[0].before_incarnation, 0x31, 0x31));
	UT_ASSERT(incarnation_is(out.space_ops[0].result_incarnation, 0x32, 0x31));

	/* A standalone tombstone, another namespace, or a damaged payload. */
	change = structure_change(CLUSTER_SPACE_WAL_TOMBSTONE);
	space_record(&record, XLOG_SMGR_SPACE_IDENTITY, &change);
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&record.reader, 99, UUID, true, false, &out),
				 CLUSTER_COLD_SPACE_INVALID);
	change = structure_change(CLUSTER_SPACE_WAL_CREATE);
	space_record(&record, XLOG_SMGR_SPACE_IDENTITY, &change);
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&record.reader, 98, UUID, true, false, &out),
				 CLUSTER_COLD_SPACE_INVALID);
	{
		uint8 other[16];

		memcpy(other, UUID, 16);
		other[15] ^= 1;
		UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&record.reader, 99, other, true, false, &out),
					 CLUSTER_COLD_SPACE_INVALID);
	}
	record.storage.decoded.main_data_len--;
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&record.reader, 99, UUID, true, false, &out),
				 CLUSTER_COLD_SPACE_INVALID);
	cluster_cold_decoded_release_v1(&out);
	UT_ASSERT(out.space_ops == NULL);
}

UT_TEST(test_space_reservation_advance_is_typed)
{
	FakeRecord record;
	ClusterColdDecodedV1 out = { 0 };
	ClusterSpaceStructureChange change = structure_change(CLUSTER_SPACE_WAL_CREATE);

	change.reservation.action = CLUSTER_SPACE_RESERVATION_ADVANCE;
	change.reservation.before = space_state(0x31);
	change.reservation.before.next_block = 3;
	change.reservation.before_token = 5;
	change.reservation.result = change.reservation.before;
	change.reservation.result.next_block = 9;
	change.reservation.first_block = 3;
	change.reservation.granted = 6;
	change.reservation.result_token = 6;
	space_record(&record, XLOG_SMGR_SPACE_RESERVATION, &change);
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&record.reader, 99, UUID, true, true, &out),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(out.record.record_flags, 0);
	UT_ASSERT_EQ(out.record.space_count, 1);
	UT_ASSERT_EQ(out.space_ops[0].kind, CLUSTER_COLD_SPACE_ADVANCE);
	UT_ASSERT(incarnation_is(out.space_ops[0].result_incarnation, 0x31, 0x31));
	UT_ASSERT_EQ(out.space_ops[0].payload_length, CLUSTER_SPACE_RESERVATION_WAL_BYTES);

	/* Only an advance stands alone. */
	change = structure_change(CLUSTER_SPACE_WAL_CREATE);
	space_record(&record, XLOG_SMGR_SPACE_RESERVATION, &change);
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&record.reader, 99, UUID, true, false, &out),
				 CLUSTER_COLD_SPACE_INVALID);
	cluster_cold_decoded_release_v1(&out);
}

/* A commit's drops: one SPACE tombstone each; the commit stays native. */
UT_TEST(test_commit_space_drops_are_typed)
{
	FakeRecord record;
	ClusterColdDecodedV1 out = { 0 };
	ClusterSpaceStructureChange drop = structure_change(CLUSTER_SPACE_WAL_TOMBSTONE);
	char *items;

	fake_record(&record, RM_XACT_ID, XLOG_XACT_COMMIT, 0);
	items = record.data;
	UT_ASSERT(cluster_space_structure_wal_encode(&drop, items, CLUSTER_SPACE_STRUCTURE_WAL_BYTES));
	drop.identity.expected.key.locator.relNumber = 16385;
	drop.identity.result.key.locator.relNumber = 16385;
	drop.reservation.before.identity.key.locator.relNumber = 16385;
	drop.reservation.result.identity.key.locator.relNumber = 16385;
	UT_ASSERT(cluster_space_structure_wal_encode(&drop, items + CLUSTER_SPACE_STRUCTURE_WAL_BYTES,
												 CLUSTER_SPACE_STRUCTURE_WAL_BYTES));
	commit_nrels = 3;
	commit_nspace_drops = 2;
	commit_space_drops = items;
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&record.reader, 99, UUID, true, false, &out),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(out.record.record_flags, 0);
	UT_ASSERT_EQ(out.record.space_count, 2);
	UT_ASSERT_EQ(out.space_ops[0].kind, CLUSTER_COLD_SPACE_DROP);
	UT_ASSERT_EQ(out.space_ops[0].locator.relNumber, 16384);
	UT_ASSERT_EQ(out.space_ops[1].locator.relNumber, 16385);
	UT_ASSERT(incarnation_is(out.space_ops[1].before_incarnation, 0x31, 0x31));
	UT_ASSERT(incarnation_is(out.space_ops[1].result_incarnation, 0, 0));
	UT_ASSERT(out.space_ops[1].payload == items + CLUSTER_SPACE_STRUCTURE_WAL_BYTES);

	/* Another generation's commit still has no cold owner. */
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&record.reader, 99, UUID, true, true, &out),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(out.record.record_flags, CLUSTER_COLD_RECORD_SIDE_UNOWNED);

	/* More drops than relations, or a drop that is not a tombstone. */
	commit_nrels = 1;
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&record.reader, 99, UUID, true, false, &out),
				 CLUSTER_COLD_SPACE_INVALID);
	commit_nrels = 3;
	drop = structure_change(CLUSTER_SPACE_WAL_TRUNCATE);
	UT_ASSERT(cluster_space_structure_wal_encode(&drop, items, CLUSTER_SPACE_STRUCTURE_WAL_BYTES));
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&record.reader, 99, UUID, true, false, &out),
				 CLUSTER_COLD_SPACE_INVALID);

	/* A plain record afterwards keeps the buffer but carries no effects. */
	fake_record(&record, RM_HEAP_ID, 0x20, 1);
	plan_component(0, RF_DETACHED_COMPONENT_PAGE_CODEC, RF_PAGE_CLASS_ORDINARY, 4, 9, 0);
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&record.reader, 99, UUID, true, false, &out),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(out.record.space_count, 0);
	UT_ASSERT(out.record.space_ops == NULL);
	UT_ASSERT(out.space_capacity >= 2);
	cluster_cold_decoded_release_v1(&out);
	UT_ASSERT_EQ(out.space_capacity, 0);
}

int
main(void)
{
	UT_PLAN(11);
	UT_RUN(test_page_record_maps_ordinary_components);
	UT_RUN(test_registry_refusal_is_opcode_unsupported);
	UT_RUN(test_cold_owner_policy);
	UT_RUN(test_transaction_lifecycle_classification);
	UT_RUN(test_storage_lifecycle_classification);
	UT_RUN(test_malformed_plan_refused);
	UT_RUN(test_foreign_side_records_have_no_cold_owner);
	UT_RUN(test_foreign_native_control_is_typed_noop);
	UT_RUN(test_space_identity_changes_are_typed);
	UT_RUN(test_space_reservation_advance_is_typed);
	UT_RUN(test_commit_space_drops_are_typed);
	UT_DONE();
	return ut_failed_count != 0;
}
