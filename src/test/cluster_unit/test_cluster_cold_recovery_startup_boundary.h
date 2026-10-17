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
static uint32 scan_space_offset;
static bool scan_space_drop;
static bool scan_space;					/* thread 2 extends relation 300 before its page record */
static ClusterWalSourceRef restart_ref; /* this node's bootstrap-validated restart input */
static bool restart_ref_present;
static int source_sync_calls;
static uint16 source_sync_order[NROOTS];
static uint16 source_sync_fail_thread;
static int source_sync_fault;
static bool fence_current;
static sigjmp_buf source_sync_exception;

bool
cluster_wal_thread_restart_v2_ref(ClusterWalSourceRef *out)
{
	memset(out, 0, sizeof(*out));
	if (!restart_ref_present)
		return false;
	*out = restart_ref;
	return true;
}

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

	memset(source, 0, sizeof(*source));
	if (memcmp(token, &tokens[thread], sizeof(*token)) != 0
		|| memcmp(root, &roots[thread], sizeof(*root)) != 0)
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	source->claim.identity = root->identity;
	source->claim.database_incarnation = database_incarnation[thread];
	memset(source->claim.claim_sha256, thread, sizeof(source->claim.claim_sha256));
	source->timeline = root->checkpoint_tli;
	*redo = native_redo[thread];
	return source_result[thread];
}

bool
cluster_recovery_merge_fence_plan_revalidate_nowait(ClusterRecoveryFencePlan *plan)
{
	/* Real revalidation also requires the pass-2 serial set. Pass 1
	 * may compare exact ROOT inputs, but cannot use execution authority. */
	return plan != NULL && fence_current && cluster_cold_replay_window_active_v1();
}

ClusterControlRootResult
cluster_control_root_recovery_sync_v1(const ClusterControlRootSnapshot *root,
									  const ClusterControlRootReadToken *token,
									  ClusterWalTailObservation *out)
{
	uint16 thread = root->identity.origin_thread_id;

	UT_ASSERT_EQ(memcmp(root, &roots[thread], sizeof(*root)), 0);
	UT_ASSERT_EQ(memcmp(token, &tokens[thread], sizeof(*token)), 0);
	if (source_sync_calls < NROOTS)
		source_sync_order[source_sync_calls] = thread;
	source_sync_calls++;
	memset(out, 0, sizeof(*out));
	out->complete_end = root->validated_tail_lsn_exclusive;
	out->last_record_start = root->tail_last_record_lsn;
	out->last_record_crc = root->tail_last_record_crc32c;
	out->database_incarnation = database_incarnation[thread];
	if (thread != source_sync_fail_thread)
		return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	switch (source_sync_fault) {
	case 1:
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	case 2:
		out->complete_end++;
		break;
	case 3:
		out->last_record_start++;
		break;
	case 4:
		out->last_record_crc++;
		break;
	case 5:
		out->database_incarnation++;
		break;
	case 6:
		database_incarnation[1]++;
		break;
	case 7:
		fence_current = false;
		break;
	case 8:
		siglongjmp(source_sync_exception, 1);
	default:
		break;
	}
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
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

static int census_releases;
/* SIDE storage is a lifecycle boundary here; original decoder/payload
 * ownership is tested separately. No mutation authority is manufactured. */
struct RfSideOnlinePlanV1 {
	RfContributorStreamCutV1 cut;
	ClusterWalSourceRef sources[CLUSTER_COLD_MAX_PARTICIPANTS];
	uint32 source_count;
	XLogRecPtr redo;
	uint32 captured;
	bool bound;
	bool sealed;
	uint32 operation_count;
	RfSideOnlineOperationV1 operations[4];
};
static RfSideOnlinePlanV1 side_fixture[NROOTS];
static int side_creates, side_seals, side_destroys, side_fault;
static Size side_budget_total;

RfPageProofDetailV1
rf_side_online_plan_create_v1(const RfSideOnlinePlanRequestV1 *request, RfSideOnlinePlanV1 **out)
{
	*out = NULL;
	UT_ASSERT_EQ(request->participant_count, 1);
	UT_ASSERT_EQ(request->purpose, RF_SIDE_PLAN_RECOVERY);
	UT_ASSERT_EQ(request->system_identifier, 9);
	UT_ASSERT_EQ(request->storage_uuid[0], 1);
	UT_ASSERT_NOT_NULL(request->redo_starts);
	UT_ASSERT(request->memory_budget > 0
			  && request->memory_budget <= RF_SIDE_ONLINE_PLAN_MAX_BYTES);
	if (side_fault == 1 && side_creates == 1)
		return RF_PAGE_PROOF_DETAIL_CAPACITY;
	UT_ASSERT(side_creates < NROOTS);
	*out = &side_fixture[side_creates++];
	(*out)->cut = request->physical_cuts[0];
	(*out)->redo = request->redo_starts[0];
	side_budget_total += request->memory_budget;
	return RF_PAGE_PROOF_DETAIL_OK;
}

bool
rf_side_online_plan_bind_sources_v1(RfSideOnlinePlanV1 *plan, const ClusterWalSourceRef *sources,
									uint32 count)
{
	UT_ASSERT_EQ(plan->captured, 0);
	UT_ASSERT_EQ(census_releases, 0);
	if (side_fault == 8 && plan == &side_fixture[1])
		return false;
	if (sources == NULL || count == 0 || count > CLUSTER_COLD_MAX_PARTICIPANTS)
		return false;
	memcpy(plan->sources, sources, count * sizeof(*sources));
	plan->source_count = count;
	return true;
}

bool
rf_side_online_plan_bind_database_v1(RfSideOnlinePlanV1 *plan, uint64 database)
{
	UT_ASSERT_EQ(database, 5);
	UT_ASSERT_EQ(side_creates, NROOTS);
	UT_ASSERT_EQ(census_releases, 1);
	if (side_fault == 2 && plan == &side_fixture[1])
		return false;
	plan->bound = true;
	return true;
}

RfPageProofDetailV1
rf_side_online_plan_seal_v1(RfSideOnlinePlanV1 *plan)
{
	UT_ASSERT(plan->bound && plan->captured == 2);
	if (side_fault == 3 && plan == &side_fixture[1])
		return RF_PAGE_PROOF_DETAIL_SOURCE_GAP;
	if (side_fault == 7 && plan == &side_fixture[0])
		return RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
	plan->sealed = true;
	side_seals++;
	/* A later source cannot hide drift of the first original ROOT. */
	if (side_fault == 5 && plan == &side_fixture[2])
		tokens[1].file_txn_seq++;
	return RF_PAGE_PROOF_DETAIL_OK;
}

void
rf_side_online_plan_destroy_v1(RfSideOnlinePlanV1 **plan)
{
	if (plan != NULL && *plan != NULL) {
		side_destroys++;
		*plan = NULL;
	}
}
/* Each participant contributes its native-redo checkpoint record and one
 * page record on page (100, thread). */
static bool scan_foreign[NROOTS + 1];

ClusterColdDetailV1
cluster_cold_scan_root_v1(ClusterColdPlanV1 *plan, uint32 participant,
						  const ClusterControlRootSnapshot *root,
						  const ClusterControlRootReadToken *token, bool space_active, bool foreign,
						  struct RfSideOnlinePlanV1 *side_plan, ClusterColdScanResultV1 *result)
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
		static const char drop_payload[CLUSTER_SPACE_STRUCTURE_WAL_BYTES] = { 'D' };
		ClusterColdSpaceOpV1 advance;

		memset(&advance, 0, sizeof(advance));
		advance.kind = CLUSTER_COLD_SPACE_ADVANCE;
		advance.locator.spcOid = 1663;
		advance.locator.dbOid = 5;
		advance.locator.relNumber = 300;
		memset(advance.result_incarnation, 7, 16);
		advance.payload = payload;
		advance.payload_length = sizeof(payload);
		if (scan_space_drop) {
			advance.kind = CLUSTER_COLD_SPACE_DROP;
			memset(advance.result_incarnation, 0, 16);
			memset(advance.before_incarnation, 7, 16);
			advance.payload = drop_payload;
			advance.payload_length = sizeof(drop_payload);
		}
		memset(&record, 0, sizeof(record));
		record.read_rec_ptr = native_redo[thread] + scan_space_offset;
		record.end_rec_ptr = record.read_rec_ptr + 0x100;
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
	record.read_rec_ptr
		= native_redo[thread] + (scan_space && thread == 2 ? scan_space_offset + 0x100 : 0);
	record.end_rec_ptr = root->validated_tail_lsn_exclusive;
	record.prev_rec_ptr = scan_space && thread == 2 ? native_redo[thread] + scan_space_offset
													: root->checkpoint_lower_lsn;
	record.component_count = 1;
	record.components = &component;
	detail = cluster_cold_plan_feed_v1(plan, participant, &record);
	result->records = 2;
	if (side_plan != NULL) {
		side_plan->captured = 2;
		if (side_fault == 6 && thread == 1) {
			result->side_capture_detail = RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
			result->side_capture_read_rec_ptr = 0x100;
		}
		if (side_fault == 4 && thread == 3)
			tokens[1].file_txn_seq++;
	}
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


/* Exact-record dispatch boundary: real Startup selection and revalidation,
 * with the original SIDE consumer and authority IO represented separately. */
static uint64 control_epoch;
static int control_calls, control_init_calls, control_fault;
static uint32 control_selected;
static bool control_authority_ok, control_borrow_ok, control_cover_ok;
static bool control_native_pair_ok;
static ClusterRecoveryDutyKey control_duty;
static ClusterRecoverySerialGuard control_serial;
static ClusterControlRootSnapshot control_root;
static ClusterControlRootReadToken control_token;

uint64
cluster_epoch_get_current(void)
{
	return control_epoch;
}

bool
cluster_recovery_merge_fence_plan_authority(ClusterRecoveryFencePlan *plan, uint16 thread,
											ClusterThreadRecoveryAuthorityV1 *out)
{
	memset(out, 0, sizeof(*out));
	if (!control_borrow_ok || !cluster_recovery_merge_fence_plan_revalidate_nowait(plan)
		|| thread != 2)
		return false;
	out->duty = &control_duty;
	out->root_snapshot = &control_root;
	out->root_token = &control_token;
	out->serial_guard = &control_serial;
	return true;
}

ClusterThreadRecoveryAuthorityResultV1
cluster_thread_recovery_authority_revalidate_nowait_v1(
	const ClusterThreadRecoveryAuthorityV1 *authority)
{
	return control_authority_ok && authority != NULL && authority->duty == &control_duty
				   && authority->root_snapshot == &control_root
				   && authority->root_token == &control_token
				   && authority->serial_guard == &control_serial
				   && control_serial.mode == CLUSTER_RECOVERY_SERIAL_COLD_FORMED
			   ? CLUSTER_THREAD_AUTHORITY_OK
			   : CLUSTER_THREAD_AUTHORITY_ROOT_STALE;
}

bool
cluster_thread_recovery_authority_covers_window_v1(
	const ClusterThreadRecoveryAuthorityV1 *authority, uint16 thread, XLogRecPtr lower,
	XLogRecPtr upper)
{
	return control_cover_ok && authority->duty == &control_duty && thread == 2 && lower == 0x100
		   && upper == 0x2000;
}

bool
rf_side_online_plan_source_matches_v1(const RfSideOnlinePlanV1 *plan, uint64 system,
									  const uint8 uuid[16], const RfContributorStreamCutV1 *cut)
{
	const uint8 expected[16] = { 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 };
	return plan != NULL && plan->sealed && plan->bound && system == 9
		   && memcmp(uuid, expected, 16) == 0 && memcmp(cut, &plan->cut, sizeof(*cut)) == 0;
}

bool
rf_side_online_plan_replay_start_matches_v1(const RfSideOnlinePlanV1 *plan, uint32 thread,
											uint64 incarnation, uint64 database, XLogRecPtr redo)
{
	return plan != NULL && plan->sealed && database == 5 && plan->cut.failed_thread == thread
		   && plan->cut.origin_owner_incarnation == incarnation && plan->redo == redo;
}

uint32
rf_side_online_plan_operation_count_v1(const RfSideOnlinePlanV1 *plan)
{
	return plan != NULL && plan->sealed ? plan->operation_count : 0;
}

bool
rf_side_online_plan_operation_v1(const RfSideOnlinePlanV1 *plan, uint32 index,
								 RfSideOnlineOperationV1 *out)
{
	if (plan == NULL || !plan->sealed || index >= plan->operation_count)
		return false;
	*out = plan->operations[index];
	return true;
}

bool
rf_side_online_plan_native_creation_v1(const RfSideOnlinePlanV1 *plan, uint32 index,
									   ClusterSpaceStructureChange *creation)
{
	/*
	 * Decoder boundary only: actual native/typed payload pairing also has
	 * the original real SIDE plan regression.
	 */
	if (!control_native_pair_ok || plan != &side_fixture[1] || index != 0
		|| plan->operations[index].source_xid != plan->operations[2].source_xid
		|| plan->operations[index].identity.record.end_rec_ptr
			   > plan->operations[2].identity.record.read_rec_ptr)
		return false;
	memset(creation, 0, sizeof(*creation));
	creation->identity.action = CLUSTER_SPACE_WAL_CREATE;
	creation->identity.result.key = plan->operations[2].space_key;
	return true;
}

bool
rf_side_online_production_owner_init_v1(RfSideOnlineProductionOwnerV1 *owner, void *arg,
										RfSideOnlineFreshAuthorityV1 check, uint32 epoch,
										bool retained)
{
	control_init_calls++;
	UT_ASSERT_EQ(epoch, 7);
	UT_ASSERT(retained);
	memset(owner, 0, sizeof(*owner));
	owner->authority_arg = arg;
	owner->revalidate_authority = check;
	return control_fault != 6;
}

bool
rf_side_online_production_bind_undo_v1(RfSideOnlineProductionOwnerV1 *owner,
									   const ClusterThreadRecoveryAuthorityV1 *authority)
{
	UT_ASSERT(owner->authority_arg == authority);
	owner->undo_authority = authority;
	return control_fault != 7;
}

RfPageProofDetailV1
rf_side_online_production_apply_record_v1(const RfSideOnlinePlanV1 *plan, uint32 index,
										  const RfPageOnlineRecordIdentityV1 *identity,
										  RfSideOnlineProductionOwnerV1 *owner)
{
	control_calls++;
	control_selected = index;
	UT_ASSERT(plan == &side_fixture[1]);
	UT_ASSERT_EQ(index, 1);
	UT_ASSERT_EQ(memcmp(identity, &plan->operations[index].identity, sizeof(*identity)), 0);
	UT_ASSERT(owner->undo_authority != NULL);
	UT_ASSERT(owner->revalidate_authority(owner->authority_arg));
	/* Preserve the real record-local owner's whole-source callback contract. */
	for (uint32 i = 0; i < plan->operation_count; i++) {
		const RfSideOnlineOperationV1 *op = &plan->operations[i];
		if (!op->history_only
			&& (op->kind == RF_SIDE_ONLINE_OPERATION_SPACE
				|| op->kind == RF_SIDE_ONLINE_OPERATION_NATIVE_STRUCTURE)) {
			if (owner->preflight_space == NULL || owner->apply_space == NULL)
				return RF_PAGE_PROOF_DETAIL_INVALID_ARGUMENT;
			if (!owner->preflight_space(owner->space_arg, op))
				return RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
			/* Record selection must not execute SPACE early. */
		}
	}
	if (control_fault == 1)
		return RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
	if (control_fault == 2)
		tokens[3].file_txn_seq++;
	if (control_fault == 3)
		control_authority_ok = false;
	if (control_fault == 4)
		control_epoch++;
	if (control_fault == 5)
		siglongjmp(source_sync_exception, 1);
	return RF_PAGE_PROOF_DETAIL_OK;
}

static void
fixture(void)
{
	uint16 thread;
	memset(side_fixture, 0, sizeof(side_fixture));
	side_creates = side_seals = side_destroys = side_fault = 0;
	side_budget_total = 0;

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
	memset(&restart_ref, 0, sizeof(restart_ref));
	restart_ref.claim.identity = roots[1].identity;
	memset(restart_ref.claim.claim_sha256, 1, sizeof(restart_ref.claim.claim_sha256));
	restart_ref.timeline = 1;
	restart_ref_present = true;
	source_sync_calls = source_sync_fail_thread = source_sync_fault = 0;
	memset(source_sync_order, 0, sizeof(source_sync_order));
	fence_current = true;
	scan_space_offset = 0;
	scan_space_drop = false;
	cluster_cold_replay_window_leave_v1();
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
