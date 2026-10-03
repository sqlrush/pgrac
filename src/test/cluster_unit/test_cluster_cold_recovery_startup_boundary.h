/*-------------------------------------------------------------------------
 *
 * test_cluster_cold_recovery_startup_boundary.h
 *	  Fixtures for test_cluster_cold_recovery_startup.c: memory contexts,
 *	  ROOT lookup and source selection, fence-plan origins, the pass-1
 *	  visitor scan, A's page consumer bracket, DATA observation and the
 *	  SPACE owner's pass-1 check.
 *
 *	  Included once by test_cluster_cold_recovery_startup.c; defines the
 *	  external symbols cluster_cold_recovery_startup.c links against.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_cold_recovery_startup_boundary.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-s9p2-05-instance-and-cluster-recovery.md
 *
 *-------------------------------------------------------------------------
 */
#ifndef TEST_CLUSTER_COLD_RECOVERY_STARTUP_BOUNDARY_H
#define TEST_CLUSTER_COLD_RECOVERY_STARTUP_BOUNDARY_H

/* Minimal memory-context fixtures: one context, plain heap allocations. */
static MemoryContextData fixture_context;
MemoryContext TopMemoryContext = &fixture_context;
MemoryContext CurrentMemoryContext = &fixture_context;
static int contexts_alive;

MemoryContext
AllocSetContextCreateInternal(MemoryContext parent, const char *name, Size min_context_size,
							  Size init_block_size, Size max_block_size)
{
	(void)parent;
	(void)name;
	(void)min_context_size;
	(void)init_block_size;
	(void)max_block_size;
	contexts_alive++;
	return &fixture_context;
}

void
MemoryContextDelete(MemoryContext context)
{
	(void)context;
	contexts_alive--;
}

void *
palloc0(Size size)
{
	return calloc(1, size);
}

void *
palloc_extended(Size size, int flags)
{
	return (flags & MCXT_ALLOC_ZERO) != 0 ? calloc(1, size) : malloc(size);
}

void *
repalloc_extended(void *pointer, Size size, int flags)
{
	(void)flags;
	return realloc(pointer, size);
}

void
pfree(void *pointer)
{
	free(pointer);
}

#define NROOTS 3
static ClusterControlRootSnapshot roots[NROOTS + 1];
static ClusterControlRootReadToken tokens[NROOTS + 1];
static XLogRecPtr native_redo[NROOTS + 1];
static ClusterControlRootResult source_result[NROOTS + 1];
static uint16 fence_count;
static ClusterColdDetailV1 scan_result[NROOTS + 1];
static uint64 data_token;
static uint64 database_incarnation[NROOTS + 1];
static bool scan_space; /* thread 2 extends relation 300 before its page record */

ClusterRecoveryDutyCompare
cluster_recovery_duty_key_compare(const ClusterRecoveryDutyKey *a, const ClusterRecoveryDutyKey *b)
{
	return memcmp(a, b, sizeof(*a)) == 0 ? CLUSTER_RECOVERY_DUTY_COMPARE_EXACT
										 : CLUSTER_RECOVERY_DUTY_COMPARE_DIFFERENT;
}

ClusterControlRootResult
cluster_control_root_lookup_owner_by_node_runtime(int32 node, ClusterControlRootIdentity *identity,
												  ClusterControlRootSnapshot *snapshot,
												  ClusterControlRootReadToken *token)
{
	if (node < 0 || node >= NROOTS)
		return CLUSTER_CONTROL_ROOT_ABSENT;
	*snapshot = roots[node + 1];
	*identity = snapshot->identity;
	*token = tokens[node + 1];
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

ClusterControlRootResult
cluster_control_root_recovery_source_v1(const ClusterControlRootSnapshot *root,
										const ClusterControlRootReadToken *token,
										ClusterWalSourceRef *source, XLogRecPtr *redo)
{
	uint16 thread = root->identity.origin_thread_id;

	UT_ASSERT_EQ(memcmp(token, &tokens[thread], sizeof(*token)), 0);
	memset(source, 0, sizeof(*source));
	source->claim.identity = root->identity;
	source->claim.database_incarnation = database_incarnation[thread];
	source->timeline = root->checkpoint_tli;
	*redo = native_redo[thread];
	return source_result[thread];
}

uint16
cluster_recovery_merge_fence_plan_origin_count(const ClusterRecoveryFencePlan *plan)
{
	(void)plan;
	return fence_count;
}

bool
cluster_recovery_merge_fence_plan_origin(const ClusterRecoveryFencePlan *plan, uint16 index,
										 uint16 *origin_thread,
										 struct ClusterControlRootSnapshot *root,
										 struct ClusterControlRootReadToken *token)
{
	uint16 thread = (uint16)(index + 2); /* own thread is 1 */

	(void)plan;
	if (index >= fence_count || thread > NROOTS)
		return false;
	*origin_thread = thread;
	*root = roots[thread];
	*token = tokens[thread];
	return true;
}

/* Each participant contributes its native-redo checkpoint record and one
 * page record on page (100, thread). */
static bool scan_foreign[NROOTS + 1];

ClusterColdDetailV1
cluster_cold_scan_root_v1(ClusterColdPlanV1 *plan, uint32 participant,
						  const ClusterControlRootSnapshot *root,
						  const ClusterControlRootReadToken *token, bool space_active, bool foreign,
						  ClusterColdScanResultV1 *result)
{
	uint16 thread = root->identity.origin_thread_id;
	ClusterColdComponentV1 component;
	ClusterColdRecordV1 record;
	ClusterColdDetailV1 detail;

	(void)token;
	(void)space_active;
	scan_foreign[thread] = foreign;
	memset(result, 0, sizeof(*result));
	if (scan_result[thread] != CLUSTER_COLD_OK) {
		result->failed_read_rec_ptr = 0x1234;
		return scan_result[thread];
	}
	memset(&record, 0, sizeof(record));
	record.read_rec_ptr = root->checkpoint_lower_lsn;
	record.end_rec_ptr = native_redo[thread];
	record.prev_rec_ptr = 0x10;
	detail = cluster_cold_plan_feed_v1(plan, participant, &record);
	if (detail != CLUSTER_COLD_OK)
		return detail;
	if (scan_space && thread == 2) {
		static const char payload[4] = { 'A', 'D', 'V', '1' };
		ClusterColdSpaceOpV1 advance;

		memset(&advance, 0, sizeof(advance));
		advance.kind = CLUSTER_COLD_SPACE_ADVANCE;
		advance.locator.spcOid = 1663;
		advance.locator.dbOid = 5;
		advance.locator.relNumber = 300;
		memset(advance.result_incarnation, 7, 16);
		advance.payload = payload;
		advance.payload_length = sizeof(payload);
		memset(&record, 0, sizeof(record));
		record.read_rec_ptr = native_redo[thread];
		record.end_rec_ptr = native_redo[thread] + 0x100;
		record.prev_rec_ptr = root->checkpoint_lower_lsn;
		record.space_count = 1;
		record.space_ops = &advance;
		detail = cluster_cold_plan_feed_v1(plan, participant, &record);
		if (detail != CLUSTER_COLD_OK)
			return detail;
		record.space_count = 0;
		record.space_ops = NULL;
	}
	memset(&component, 0, sizeof(component));
	component.page.system_identifier = 9;
	memset(component.page.storage_uuid, 1, 16);
	component.page.locator.spcOid = 1663;
	component.page.locator.dbOid = 5;
	component.page.locator.relNumber = 100;
	component.page.forknum = MAIN_FORKNUM;
	component.page.blockno = thread;
	component.page_class = RF_PAGE_CLASS_ORDINARY;
	component.before_kind = RF_PAGE_STATE_PRESENT;
	component.result_kind = RF_PAGE_STATE_PRESENT;
	memset(component.before.segment_incarnation, 7, 16);
	component.before.mutation_token = 5;
	memset(component.result.segment_incarnation, 7, 16);
	component.result.mutation_token = 6;
	record.read_rec_ptr = native_redo[thread] + (scan_space && thread == 2 ? 0x100 : 0);
	record.end_rec_ptr = root->validated_tail_lsn_exclusive;
	record.prev_rec_ptr
		= scan_space && thread == 2 ? native_redo[thread] : root->checkpoint_lower_lsn;
	record.component_count = 1;
	record.components = &component;
	detail = cluster_cold_plan_feed_v1(plan, participant, &record);
	result->records = 2;
	return detail;
}

/* A's native block consumer: record the bracket and what it can see. */
static char bracket_order[8];
static int bracket_calls;
static bool bracket_saw_step[3];

/* slot: 0 begin, 1 apply, 2 end. */
static void
bracket_note(char kind, int slot, XLogReaderState *record)
{
	ClusterColdRedoBlockV1 decision;

	if (bracket_calls < (int)sizeof(bracket_order) - 1)
		bracket_order[bracket_calls] = kind;
	bracket_saw_step[slot] = cluster_cold_redo_block_decision_v1(record, 1, &decision)
							 && decision.action == CLUSTER_COLD_REDO_APPLY;
	bracket_calls++;
}

void
cluster_page_cold_redo_begin_v1(XLogReaderState *record)
{
	bracket_note('b', 0, record);
}

void
cluster_page_cold_redo_end_v1(XLogReaderState *record)
{
	bracket_note('e', 2, record);
}

bool
cluster_cold_observe_data_v1(void *arg, const RfPageIdentityV1 *page, ClusterColdDataV1 *out)
{
	ClusterColdObserverV1 *observer = (ClusterColdObserverV1 *)arg;

	(void)page;
	observer->pages_observed++;
	memset(out, 0, sizeof(*out));
	out->kind = CLUSTER_COLD_DATA_PRESENT;
	out->flags = CLUSTER_COLD_DATA_FLAG_CONTENT_VERIFIED; /* data checksums on */
	memset(out->version.segment_incarnation, 7, 16);
	out->version.mutation_token = data_token;
	return true;
}

/* The SPACE owner's pass-1 check: what it was given, and its answer. */
static int space_checks;
static ClusterColdObserverV1 space_check_namespace;
static uint32 space_check_count;

bool
cluster_cold_space_check_v1(void *arg, const RelFileLocator *locator,
							const ClusterColdSpaceInputV1 *inputs, uint32 count, uint32 *order)
{
	uint32 i;

	(void)locator;
	(void)inputs;
	space_checks++;
	space_check_namespace = *(ClusterColdObserverV1 *)arg;
	space_check_count = count;
	for (i = 0; i < count; i++)
		order[i] = i;
	return true;
}

static void
fixture(void)
{
	uint16 thread;

	memset(roots, 0, sizeof(roots));
	memset(tokens, 0, sizeof(tokens));
	for (thread = 1; thread <= NROOTS; thread++) {
		ClusterControlRootIdentity *id = &roots[thread].identity;
		ClusterWalThreadClaim claim;

		id->system_identifier = 9;
		memset(id->storage_uuid, 1, 16);
		id->authority_uuid[6] = 0x40;
		id->authority_uuid[8] = 0x80;
		id->origin_thread_id = thread;
		id->origin_node_id = thread - 1;
		id->thread_claim_created_at = 42;
		id->origin_owner_incarnation = 10 + thread;
		id->root_lineage_seq = 3;
		cluster_wal_thread_claim_fill(&claim, thread, thread - 1, 42);
		id->thread_claim_crc32c = claim.crc;
		roots[thread].lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED;
		roots[thread].root_flags
			= CLUSTER_CONTROL_ROOT_FLAG_CLAIM_VALID | CLUSTER_CONTROL_ROOT_FLAG_CHECKPOINT_VALID
			  | CLUSTER_CONTROL_ROOT_FLAG_TAIL_VALID | CLUSTER_CONTROL_ROOT_FLAG_RECOVERED_VALID;
		roots[thread].checkpoint_tli = roots[thread].tail_tli = 1;
		roots[thread].checkpoint_lower_lsn = 0x100;
		roots[thread].validated_tail_lsn_exclusive = 0x2000;
		native_redo[thread] = 0x800;
		source_result[thread] = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
		scan_result[thread] = CLUSTER_COLD_OK;
		database_incarnation[thread] = 5;
		tokens[thread].origin_thread_id = thread;
		tokens[thread].file_txn_seq = 77;
	}
	fence_count = 2;
	data_token = 5;
	contexts_alive = 0;
}

static ClusterColdTypedV1 *
prepare(XLogRecPtr own_redo)
{
	return cluster_cold_typed_prepare_v1((struct ClusterRecoveryFencePlan *)&fixture_context, 1,
										 own_redo);
}

#endif /* TEST_CLUSTER_COLD_RECOVERY_STARTUP_BOUNDARY_H */
