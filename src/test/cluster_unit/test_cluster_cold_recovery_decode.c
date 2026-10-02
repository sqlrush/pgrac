/*-------------------------------------------------------------------------
 *
 * test_cluster_cold_recovery_decode.c
 *	  Mapping of decoded native records to typed cold replay plan input.
 *
 *	  The closed route registry preflight and transaction record parsing are
 *	  fixtures here; this binary checks the cold owner policy, component
 *	  mapping and lifecycle classification only.
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
#include "access/xlogreader.h"
#include "catalog/storage_xlog.h"
#include "access/heapam_xlog.h"
#include "cluster/cluster_cold_recovery.h"
#include "cluster/cluster_page_detached.h"

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
	return true;
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
	char data[64];
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
	commit_parse_ok = true;
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
	ClusterColdDecodedV1 out;

	fake_record(&record, RM_HEAP_ID, 0x20, 3);
	plan_component(0, RF_DETACHED_COMPONENT_PAGE_CODEC, RF_PAGE_CLASS_ORDINARY, 4, 9,
				   RF_PAGE_EDGE_FULL_IMAGE_APPLY | RF_PAGE_EDGE_FULL_COVERAGE);
	plan_component(1, RF_DETACHED_COMPONENT_PAGE_CODEC, RF_PAGE_CLASS_ORDINARY, 5, 9, 0);
	plan_component(2, RF_DETACHED_COMPONENT_REBUILDABLE, RF_PAGE_CLASS_REBUILDABLE_FSM, 0, 9, 0);
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&record.reader, 99, UUID, true, &out),
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
	ClusterColdDecodedV1 out;

	fake_record(&record, RM_HEAP_ID, 0x20, 1);
	preflight_result = RF_PAGE_PROOF_DETAIL_OPCODE_UNSUPPORTED;
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&record.reader, 99, UUID, true, &out),
				 CLUSTER_COLD_OPCODE_UNSUPPORTED);
	UT_ASSERT_EQ(out.route_detail, RF_PAGE_PROOF_DETAIL_OPCODE_UNSUPPORTED);
	preflight_result = RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&record.reader, 99, UUID, true, &out),
				 CLUSTER_COLD_OPCODE_UNSUPPORTED);
	UT_ASSERT_EQ(out.route_detail, RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE);
}

/* Cold owner policy handed to the registry preflight. */
UT_TEST(test_cold_owner_policy)
{
	FakeRecord record;
	ClusterColdDecodedV1 out;
	RfOpcodeRouteV1 side = { 0 };
	RfOpcodeRouteV1 noop = { 0 };
	RfOpcodeRouteV1 page = { 0 };
	RfPageVersionEdgeEntryV1 edge = { 0 };
	DecodedBkpBlock block = { 0 };

	fake_record(&record, RM_XACT_ID, XLOG_XACT_COMMIT, 0);
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&record.reader, 99, UUID, true, &out),
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
	ClusterColdDecodedV1 out;
	static const uint8 prepared[3]
		= { XLOG_XACT_PREPARE, XLOG_XACT_COMMIT_PREPARED, XLOG_XACT_ABORT_PREPARED };
	int i;

	fake_record(&record, RM_XACT_ID, XLOG_XACT_COMMIT, 0);
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&record.reader, 99, UUID, true, &out),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(out.record.record_flags, 0);
	commit_nrels = 1;
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&record.reader, 99, UUID, true, &out),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(out.record.record_flags, CLUSTER_COLD_RECORD_STRUCTURAL);
	commit_parse_ok = false;
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&record.reader, 99, UUID, true, &out),
				 CLUSTER_COLD_COMPONENT_INVALID);

	fake_record(&record, RM_XACT_ID, XLOG_XACT_ABORT, 0);
	abort_nrels = 2;
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&record.reader, 99, UUID, true, &out),
				 CLUSTER_COLD_OK);
	UT_ASSERT_EQ(out.record.record_flags, CLUSTER_COLD_RECORD_STRUCTURAL);

	for (i = 0; i < 3; i++) {
		fake_record(&record, RM_XACT_ID, prepared[i], 0);
		UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&record.reader, 99, UUID, true, &out),
					 CLUSTER_COLD_OK);
		UT_ASSERT_EQ(out.record.record_flags, CLUSTER_COLD_RECORD_UNSUPPORTED);
	}
}

UT_TEST(test_storage_lifecycle_classification)
{
	FakeRecord record;
	ClusterColdDecodedV1 out;
	static const struct {
		uint8 rmid;
		uint8 info;
		uint8 flags;
	} cases[] = { { RM_SMGR_ID, XLOG_SMGR_CREATE, 0 },
				  { RM_SMGR_ID, XLOG_SMGR_SPACE_RESERVATION, CLUSTER_COLD_RECORD_STRUCTURAL },
				  { RM_SMGR_ID, XLOG_SMGR_TRUNCATE, CLUSTER_COLD_RECORD_STRUCTURAL },
				  { RM_SMGR_ID, XLOG_SMGR_SPACE_IDENTITY, CLUSTER_COLD_RECORD_STRUCTURAL },
				  { RM_DBASE_ID, 0x00, CLUSTER_COLD_RECORD_STRUCTURAL },
				  { RM_TBLSPC_ID, 0x00, CLUSTER_COLD_RECORD_STRUCTURAL },
				  { RM_RELMAP_ID, 0x00, CLUSTER_COLD_RECORD_STRUCTURAL },
				  { RM_REPLORIGIN_ID, 0x00, CLUSTER_COLD_RECORD_STRUCTURAL },
				  { RM_HEAP2_ID, XLOG_HEAP2_REWRITE, CLUSTER_COLD_RECORD_STRUCTURAL },
				  { RM_XLOG_ID, 0x30, 0 },
				  { RM_STANDBY_ID, 0x10, 0 } };
	Size i;

	for (i = 0; i < lengthof(cases); i++) {
		fake_record(&record, cases[i].rmid, cases[i].info, 0);
		UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&record.reader, 99, UUID, true, &out),
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
	ClusterColdDecodedV1 out;

	fake_record(&record, RM_HEAP_ID, 0x20, 1);
	plan_component(0, RF_DETACHED_COMPONENT_PAGE_CODEC, RF_PAGE_CLASS_ORDINARY, 4, 9, 0);
	preflight_plan.components[0].block_id = 3;
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&record.reader, 99, UUID, true, &out),
				 CLUSTER_COLD_COMPONENT_INVALID);
	fake_record(&record, RM_HEAP_ID, 0x20, 1);
	plan_component(0, RF_DETACHED_COMPONENT_SIDE_TYPED, RF_PAGE_CLASS_ROUTED_SIDE, 0, 9, 0);
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&record.reader, 99, UUID, true, &out),
				 CLUSTER_COLD_OPCODE_UNSUPPORTED);
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(NULL, 99, UUID, true, &out),
				 CLUSTER_COLD_INVALID_ARGUMENT);
	UT_ASSERT_EQ(cluster_cold_recovery_decode_v1(&record.reader, 99, UUID, true, NULL),
				 CLUSTER_COLD_INVALID_ARGUMENT);
}

int
main(void)
{
	UT_PLAN(6);
	UT_RUN(test_page_record_maps_ordinary_components);
	UT_RUN(test_registry_refusal_is_opcode_unsupported);
	UT_RUN(test_cold_owner_policy);
	UT_RUN(test_transaction_lifecycle_classification);
	UT_RUN(test_storage_lifecycle_classification);
	UT_RUN(test_malformed_plan_refused);
	UT_DONE();
	return ut_failed_count != 0;
}
