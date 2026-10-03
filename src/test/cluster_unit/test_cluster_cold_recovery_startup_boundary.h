/*-------------------------------------------------------------------------
 *
 * test_cluster_cold_recovery_startup_boundary.h
 *	  Fixtures for test_cluster_cold_recovery_startup.c: memory contexts,
 *	  ROOT lookup and source selection, fence-plan origins, the pass-1
 *	  visitor scan, the participant census and its cold read scope, A's page
 *	  consumer bracket, DATA observation and the SPACE owner's pass-1 check.
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

/* The ROOT module's identity equality, field by field. */
bool
cluster_control_root_identity_equal(const ClusterControlRootIdentity *left,
									const ClusterControlRootIdentity *right)
{
	return left != NULL && right != NULL && memcmp(left, right, sizeof(*left)) == 0;
}

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

/*
 * The participant census and its cold read scope.  By default the census
 * finds exactly the crashed roots the fence plan names, with their cuts;
 * census_history adds history-only generations.  The census selection
 * itself is tested with the I/O module.
 */
#define MAX_HISTORY 2
static int census_scope;
static ClusterControlRootResult census_begin_result;
static ClusterControlRootResult census_revalidate_result;
static ClusterColdCensusDetailV1 census_select_result;
static ClusterColdCensusDetailV1 census_cover_result;
static uint16 census_bad_thread;
static XLogRecPtr census_redo_skew; /* added to thread 2's census native redo */
static ClusterColdCensusEntryV1 census_history[MAX_HISTORY];
static uint32 census_history_count;
static int census_begins;
static int census_releases;
static int history_scans;
static uint32 history_scan_index;

ClusterControlRootResult
cluster_wal_inputs_cold_begin_v1(const uint8 storage_uuid[16], uint64 system_identifier,
								 ClusterWalInputsV1 **out)
{
	census_begins++;
	UT_ASSERT_EQ(system_identifier, 9);
	UT_ASSERT_EQ(storage_uuid[0], 1);
	*out = census_begin_result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
			   ? (ClusterWalInputsV1 *)&census_scope
			   : NULL;
	return census_begin_result;
}

uint32
cluster_wal_inputs_count_v1(ClusterWalInputsV1 *inputs)
{
	UT_ASSERT(inputs == (ClusterWalInputsV1 *)&census_scope);
	return 0;
}

const ClusterWalInputV1 *
cluster_wal_inputs_at_v1(ClusterWalInputsV1 *inputs, uint32 index)
{
	(void)inputs;
	(void)index;
	return NULL;
}

ClusterControlRootResult
cluster_wal_inputs_revalidate_v1(ClusterWalInputsV1 *inputs)
{
	UT_ASSERT(inputs == (ClusterWalInputsV1 *)&census_scope);
	return census_revalidate_result;
}

void
cluster_wal_inputs_release_v1(ClusterWalInputsV1 **inputs)
{
	UT_ASSERT(*inputs == (ClusterWalInputsV1 *)&census_scope);
	census_releases++;
	*inputs = NULL;
}

ClusterColdCensusDetailV1
cluster_cold_census_select_v1(const ClusterWalInputV1 *const *inputs, uint32 count,
							  ClusterColdCensusEntryV1 *entries, uint32 capacity, uint32 *out_count,
							  uint32 *bad_index)
{
	uint32 n = 0;
	uint16 thread;
	uint32 i;

	(void)inputs;
	(void)count;
	*out_count = 0;
	*bad_index = 3;
	if (census_select_result != CLUSTER_COLD_CENSUS_OK)
		return census_select_result;
	for (thread = 1; thread <= 1 + fence_count && thread <= NROOTS && n < capacity; thread++) {
		ClusterColdCensusEntryV1 *e = &entries[n++];

		memset(e, 0, sizeof(*e));
		e->input_index = thread;
		e->role = CLUSTER_COLD_CENSUS_REPLAY;
		e->identity = roots[thread].identity;
		e->source.claim.identity = roots[thread].identity;
		e->cut.thread_id = thread;
		e->cut.timeline = roots[thread].checkpoint_tli;
		e->cut.owner_incarnation = roots[thread].identity.origin_owner_incarnation;
		e->cut.physical_lower = roots[thread].checkpoint_lower_lsn;
		e->cut.native_redo = native_redo[thread] + (thread == 2 ? census_redo_skew : 0);
		e->cut.tail_end = roots[thread].validated_tail_lsn_exclusive;
	}
	for (i = 0; i < census_history_count && n < capacity; i++)
		entries[n++] = census_history[i];
	*out_count = n;
	*bad_index = 0;
	return CLUSTER_COLD_CENSUS_OK;
}

ClusterColdCensusDetailV1
cluster_cold_census_cover_v1(const ClusterColdCensusEntryV1 *entries, uint32 count,
							 const ClusterControlRootSnapshot *crashed, uint32 crashed_count,
							 uint16 *bad_thread)
{
	(void)entries;
	(void)count;
	UT_ASSERT_EQ(crashed_count, (uint32)fence_count + 1);
	UT_ASSERT_EQ(crashed[0].identity.origin_thread_id, 1);
	*bad_thread = census_bad_thread;
	return census_cover_result;
}

/* A history-only generation: one retained record covering its range. */
ClusterColdDetailV1
cluster_cold_scan_input_v1(ClusterColdPlanV1 *plan, uint32 participant, ClusterWalInputsV1 *inputs,
						   uint32 index, bool space_active, bool foreign,
						   ClusterColdScanResultV1 *result)
{
	const ClusterColdCensusEntryV1 *entry = NULL;
	ClusterColdRecordV1 record;
	uint32 i;

	UT_ASSERT(inputs == (ClusterWalInputsV1 *)&census_scope);
	UT_ASSERT(!space_active && foreign);
	history_scans++;
	history_scan_index = index;
	memset(result, 0, sizeof(*result));
	for (i = 0; i < census_history_count; i++)
		if (census_history[i].input_index == index)
			entry = &census_history[i];
	UT_ASSERT(entry != NULL);
	memset(&record, 0, sizeof(record));
	record.read_rec_ptr = entry->cut.physical_lower;
	record.end_rec_ptr = entry->cut.tail_end;
	record.prev_rec_ptr = 0x10;
	result->records = 1;
	return cluster_cold_plan_feed_v1(plan, participant, &record);
}

static void
history_generation(uint16 thread, uint64 incarnation, XLogRecPtr lower, XLogRecPtr tail)
{
	ClusterColdCensusEntryV1 *e = &census_history[census_history_count++];

	memset(e, 0, sizeof(*e));
	e->input_index = 10 + census_history_count;
	e->role = CLUSTER_COLD_CENSUS_HISTORY;
	e->identity = roots[thread].identity;
	e->identity.origin_owner_incarnation = incarnation;
	e->source.claim.identity = e->identity;
	e->source.claim.database_incarnation = 5;
	e->cut.thread_id = thread;
	e->cut.timeline = 1;
	e->cut.owner_incarnation = incarnation;
	e->cut.physical_lower = lower;
	e->cut.native_redo = tail;
	e->cut.tail_end = tail;
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
	census_begin_result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	census_revalidate_result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	census_select_result = CLUSTER_COLD_CENSUS_OK;
	census_cover_result = CLUSTER_COLD_CENSUS_OK;
	census_bad_thread = 0;
	census_redo_skew = 0;
	census_history_count = 0;
	census_begins = census_releases = history_scans = 0;
	history_scan_index = 0;
}

static ClusterColdTypedV1 *
prepare(XLogRecPtr own_redo)
{
	return cluster_cold_typed_prepare_v1((struct ClusterRecoveryFencePlan *)&fixture_context, 1,
										 own_redo);
}

#endif /* TEST_CLUSTER_COLD_RECOVERY_STARTUP_BOUNDARY_H */
